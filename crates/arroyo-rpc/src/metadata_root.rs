//! The authoritative, fence-scoped metadata root of one scheduling generation
//! (M11.T26b/M11.T26g, design M11.D39d, plan M11.P54a).
//!
//! M11.D39d splits publishing a generation's metadata into two steps that cannot be collapsed:
//! the object is written first, under an **immutable, fence-scoped candidate name**, and it
//! becomes authoritative only when a conditional `job_statuses` update — matched on job id,
//! `lifecycle_fence` and `controller_epoch` — installs its reference. A controller that loses
//! the fence duel between the two steps has written an object nobody points at; it cannot
//! replace the root the winner installed, because its update matches no row.
//!
//! [`MetadataRoot`] is that reference, as `job_statuses.state_context` carries it.
//!
//! # The name is the identity, so the two cannot disagree
//!
//! A record that carried both an identity and a free-form object key would be a record whose
//! two halves could describe different objects — a hand-edited row, a partially applied write,
//! or a build with a different layout would all produce one. So there is no key field:
//! [`MetadataRoot::object`] *derives* the key from the identity the record carries, through
//! [`candidate_object_key`], which is also what the controller writes the candidate at. There
//! is one spelling of the name and nothing to reconcile.
//!
//! # Why every part of the identity is bounded and checked
//!
//! This is persisted state, read back by a process that did not write it, and the value it
//! produces is an *object-store key*. A record whose job id contained a path separator would
//! name an object outside the job's namespace; one whose epoch was unbounded would name a key
//! no store accepts. So the identity is validated on both paths into existence —
//! [`MetadataRoot::mint`] and `Deserialize`, which goes through `#[serde(try_from)]` — exactly
//! as [`Fencing`](crate::fencing::Fencing) is. There is no third path.
//!
//! # What is not here
//!
//! Writing the candidate object and running the conditional update are the controller's:
//! `arroyo_controller::states::lifecycle::root`. This module owns the *name* and the *record*,
//! because both ends of the protocol — the controller that installs a root and the controller
//! that recovers one — have to agree about them, and a durable value's rules belong with the
//! value.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::fencing::MAX_CANDIDATE_ROOT_BYTES;

/// The metadata-root record version this build writes and is the only one it accepts.
///
/// A record carrying any other version was written by a build whose layout for the candidate
/// name this one does not know, and deriving a key from it would name an object that build
/// never wrote. It is refused rather than partially interpreted, which is the same fail-closed
/// choice [`FENCING_RECORD_VERSION`](crate::fencing::FENCING_RECORD_VERSION) makes.
pub const METADATA_ROOT_VERSION: u32 = 1;

/// The longest controller epoch a metadata root may name, in characters.
///
/// The controller mints an epoch as two `u64`s in zero-padded lowercase hexadecimal, so this
/// is `2 × (64 / 4)`. Like [`MAX_ATTEMPT_ID_CHARS`](crate::fencing::MAX_ATTEMPT_ID_CHARS) it is
/// an upper bound on what may be persisted rather than a restatement of the minting format;
/// the controller-side test `the_minted_controller_epoch_fits_the_metadata_root_bound` is what
/// pins the two together.
pub const MAX_CONTROLLER_EPOCH_CHARS: usize = 2 * (u64::BITS as usize / 4);

/// The longest pipeline or job identifier a metadata root may name, in characters.
///
/// Both are `pub_id`-shaped values a few dozen characters long, and both become path segments
/// of the candidate key. The bound is generous rather than exact — its job is to keep the
/// derived key inside [`MAX_CANDIDATE_ROOT_BYTES`] whatever an operator has named a pipeline,
/// not to re-specify how identifiers are generated.
pub const MAX_ROOT_IDENTIFIER_CHARS: usize = 128;

/// Why a metadata-root record was refused.
///
/// Every variant is reachable from a decode as well as from [`MetadataRoot::mint`], because
/// this value arrives from the database as often as from a constructor. None is recoverable by
/// guessing: a record that breaks one of these rules cannot be turned into an object-store key
/// that means what its writer meant.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum MetadataRootError {
    /// The record was written by a build with a different candidate layout.
    #[error(
        "metadata root record is version {found}, and this build reads only version \
         {METADATA_ROOT_VERSION}"
    )]
    UnknownVersion {
        /// The version the record carried.
        found: u32,
    },
    /// A pipeline or job identifier that is empty, oversized, or not a single path segment.
    #[error(
        "metadata root names a {field} of {found:?}, which is not a single non-empty path \
         segment of at most {MAX_ROOT_IDENTIFIER_CHARS} characters"
    )]
    MalformedIdentifier {
        /// Which identifier it was, for the operator reading the message.
        field: &'static str,
        /// The value the record carried.
        found: String,
    },
    /// A generation of zero, which no launched worker generation runs under.
    ///
    /// `job_statuses.run_id` defaults to 0 and the scheduling preamble raises it before the
    /// generation's workers are launched, so zero names a generation that never existed.
    #[error("metadata root names scheduling generation 0, which no launched generation runs under")]
    UnlaunchedGeneration,
    /// A fence of zero, which is the column default no adoption can install.
    ///
    /// Adoption stores `lifecycle_fence + 1`, so a root scoped to fence zero was scoped to an
    /// authority no controller ever held.
    #[error("metadata root names lifecycle fence 0, which no adoption installs")]
    UnadoptedFence,
    /// A controller epoch that is empty, oversized, or not the shape an adoption mints.
    #[error(
        "metadata root names controller epoch {found:?}, which is not between 1 and \
         {MAX_CONTROLLER_EPOCH_CHARS} lowercase hexadecimal characters"
    )]
    MalformedEpoch {
        /// The value the record carried.
        found: String,
    },
    /// The key this identity derives is longer than any object-store key that can exist.
    #[error(
        "metadata root derives a {found}-byte candidate key, more than the \
         {MAX_CANDIDATE_ROOT_BYTES} bytes an object-store key can hold"
    )]
    CandidateKeyTooLong {
        /// The derived key's length in bytes.
        found: usize,
    },
}

/// The object-store key a generation's candidate metadata is written at.
///
/// One function, used by the controller that writes the object and by [`MetadataRoot::object`]
/// that names it, so a root can never point somewhere a candidate was not written.
///
/// The name is **immutable and fence-scoped**: every component of the identity appears in it,
/// so two controllers scheduling the same generation of the same job write different objects,
/// and one controller re-running the same attempt writes the same one. Nothing overwrites
/// anything.
///
/// It sits under the job's own `generations/{generation}/` prefix — inside the namespace the
/// job's checkpoint artifacts live in, so a collector that reclaims a generation reclaims its
/// unrooted candidates with it, and outside
/// [`ProtocolPaths::contains_deletable_object`](../../arroyo_state_protocol/struct.ProtocolPaths.html)'s
/// table-data shape, so the landed history cleanup can never mistake a candidate for a data
/// file.
///
/// The fence is zero-padded to twenty digits — `u64::MAX` is twenty digits — so the keys of one
/// generation sort in fence order, which is the order a collector wants to read them in.
fn candidate_object_key(
    pipeline_id: &str,
    job_id: &str,
    generation: u64,
    fence: u64,
    epoch: &str,
) -> String {
    format!(
        "{pipeline_id}/{job_id}/generations/{generation}/candidates/\
         fence-{fence:020}-epoch-{epoch}.json"
    )
}

/// The shape a metadata-root record is decoded through.
///
/// Private, and unchecked, so that [`MetadataRoot`]'s own `Deserialize` can be
/// `#[serde(try_from)]` — which is what makes [`MetadataRoot::validate`] unskippable rather
/// than something a reader has to remember to call.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataRootRepr {
    version: u32,
    pipeline_id: String,
    job_id: String,
    generation: u64,
    fence: u64,
    epoch: String,
}

/// The authoritative metadata root one controller installed for one scheduling generation.
///
/// Its fields are the identity of a candidate object, and nothing else: the key is
/// [`Self::object`], derived rather than stored, so the record cannot name an object that
/// disagrees with the identity it claims.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "MetadataRootRepr")]
pub struct MetadataRoot {
    version: u32,
    pipeline_id: String,
    job_id: String,
    generation: u64,
    fence: u64,
    epoch: String,
}

impl MetadataRoot {
    /// The root a candidate with this identity would become.
    ///
    /// The version is this build's rather than a parameter: a caller that could choose one
    /// could write a record its own build cannot read back.
    ///
    /// # Errors
    ///
    /// Every variant of [`MetadataRootError`] except [`MetadataRootError::UnknownVersion`],
    /// which no caller of this function can provoke.
    pub fn mint(
        pipeline_id: &str,
        job_id: &str,
        generation: u64,
        fence: u64,
        epoch: &str,
    ) -> Result<Self, MetadataRootError> {
        Self::validate(
            METADATA_ROOT_VERSION,
            pipeline_id,
            job_id,
            generation,
            fence,
            epoch,
        )?;
        Ok(Self {
            version: METADATA_ROOT_VERSION,
            pipeline_id: pipeline_id.to_string(),
            job_id: job_id.to_string(),
            generation,
            fence,
            epoch: epoch.to_string(),
        })
    }

    /// The version this record was written at, which is always [`METADATA_ROOT_VERSION`].
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The pipeline whose namespace the rooted object lives in.
    pub fn pipeline_id(&self) -> &str {
        &self.pipeline_id
    }

    /// The job this root belongs to.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// The scheduling generation this root is for.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The lifecycle fence the controller that installed this root held.
    pub fn fence(&self) -> u64 {
        self.fence
    }

    /// The controller epoch the controller that installed this root held.
    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    /// The object-store key of the candidate this root makes authoritative.
    ///
    /// Derived from the identity above, so it is the same key the controller published the
    /// candidate at and there is no second field that could disagree with it.
    pub fn object(&self) -> String {
        candidate_object_key(
            &self.pipeline_id,
            &self.job_id,
            self.generation,
            self.fence,
            &self.epoch,
        )
    }

    /// Whether `candidate` is the object this root makes authoritative.
    ///
    /// The question a collector asks of a candidate it found: an object the job's row does not
    /// name is **unrooted**, and an unrooted candidate is reclaimable however recently it was
    /// written. `None` — no root installed at all — leaves every candidate unrooted, which is
    /// the state a job has before its first conditional install.
    pub fn roots(&self, candidate: &str) -> bool {
        self.object() == candidate
    }

    /// Checks every rule a metadata-root record is under, before any of it is adopted.
    ///
    /// Ordered cheapest-first, and the derived-length check last, because it is the only one
    /// that has to build the key.
    fn validate(
        version: u32,
        pipeline_id: &str,
        job_id: &str,
        generation: u64,
        fence: u64,
        epoch: &str,
    ) -> Result<(), MetadataRootError> {
        if version != METADATA_ROOT_VERSION {
            return Err(MetadataRootError::UnknownVersion { found: version });
        }
        for (field, value) in [("pipeline id", pipeline_id), ("job id", job_id)] {
            let length = value.chars().count();
            // A single path segment: no separator, no `.`/`..` traversal, nothing empty. The
            // value becomes a directory name in the job's own namespace, and a record that
            // could name a segment outside it could aim a root at another job's objects.
            if length == 0
                || length > MAX_ROOT_IDENTIFIER_CHARS
                || value.contains('/')
                || value.contains('\\')
                || value == "."
                || value == ".."
            {
                return Err(MetadataRootError::MalformedIdentifier {
                    field,
                    found: value.to_string(),
                });
            }
        }
        if generation == 0 {
            return Err(MetadataRootError::UnlaunchedGeneration);
        }
        if fence == 0 {
            return Err(MetadataRootError::UnadoptedFence);
        }
        let epoch_length = epoch.chars().count();
        if epoch_length == 0
            || epoch_length > MAX_CONTROLLER_EPOCH_CHARS
            || !epoch.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(MetadataRootError::MalformedEpoch {
                found: epoch.to_string(),
            });
        }
        let derived = candidate_object_key(pipeline_id, job_id, generation, fence, epoch).len();
        if derived > MAX_CANDIDATE_ROOT_BYTES {
            return Err(MetadataRootError::CandidateKeyTooLong { found: derived });
        }
        Ok(())
    }
}

impl TryFrom<MetadataRootRepr> for MetadataRoot {
    type Error = MetadataRootError;

    fn try_from(repr: MetadataRootRepr) -> Result<Self, Self::Error> {
        MetadataRoot::validate(
            repr.version,
            &repr.pipeline_id,
            &repr.job_id,
            repr.generation,
            repr.fence,
            &repr.epoch,
        )?;
        Ok(Self {
            version: repr.version,
            pipeline_id: repr.pipeline_id,
            job_id: repr.job_id,
            generation: repr.generation,
            fence: repr.fence,
            epoch: repr.epoch,
        })
    }
}

#[cfg(test)]
mod tests;
