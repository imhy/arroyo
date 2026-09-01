//! The durable half of a job's fencing obligation (M11.T26b, design M11.D39d).
//!
//! A controller that is interrupted mid-scheduling owes an acknowledgement to every worker
//! generation a request it issued could still reach. M11.T25 holds that obligation in memory,
//! in `Fencing`/`FenceTargets`; a controller that dies holds it nowhere. [`Fencing`] is the
//! record that survives the process — the target generations, the identifier each was issued,
//! and the fence-scoped candidate root the attempt published but never made authoritative —
//! written into the same `job_statuses.state_context` column as the execution selector, under
//! the conditional update that the fence and controller epoch columns make possible.
//!
//! # Why the invariants are the type's and not a caller's
//!
//! This is persisted state read back by a process that did not write it, so every rule about
//! it has to hold for a value that arrives from the database rather than from a constructor.
//! There are exactly two ways to obtain a [`Fencing`] — [`Fencing::record`] and deserializing
//! one — and both run [`Fencing::validate`]: the deserializer goes through
//! `#[serde(try_from)]`, so a record that breaks a rule is a *decode* failure, and the
//! controller's one fail-closed decode path
//! (`states::lifecycle::classification::decode_execution_record`) rejects it exactly as it
//! rejects any other unusable execution record, skipping that job and no other. There is no
//! third path and no `Fencing` that has not been checked.
//!
//! # What "bounded" means here
//!
//! [`MAX_FENCE_TARGETS`] is derived rather than chosen; see its documentation for the two
//! quantities it comes from and the one it does not. Overflow is refused with
//! [`FencingRecordError::TooManyTargets`] at both ends — writing and decoding — because a
//! durable collection that grows with the cluster is durable state nobody bounded.
//! [`MAX_TARGET_ADDRESS_CHARS`] is derived outright, from the standards that cap a URI's three
//! parts.
//!
//! # What M11.T26f added, and why it is not decoration
//!
//! Two facts joined the record when it acquired a writer: a target's [`FenceTarget::rpc_address`]
//! and the obligation's [`Fencing::fencing_since_millis`]. Both exist for the same reason the
//! record does — a controller that did not write it has to be able to *act* on it — and neither
//! could be reconstructed by the reader. Without the address a recovered obligation can only be
//! discharged by teardown, because a replacement controller has no channel to a generation it
//! never started; without the origin, a job's fencing age restarts every time its controller
//! does, and the metric an operator pages on would hide exactly the wedged job it exists to
//! surface. Both are optional and skipped when absent, so a record written without them decodes
//! and re-serializes unchanged.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The largest number of workers one scheduling attempt addresses in a deployment that has
/// not raised its own ceiling.
///
/// Every worker holds at least one task slot, the controller sizes a job at
/// `slots_for_job(program)` — the program's maximum node parallelism — and `arroyo-api`
/// refuses to create a pipeline whose parallelism exceeds the organization's
/// `max_parallelism`, whose shipped default (`arroyo_api::default_max_parallelism`) is this
/// value.
const DEFAULT_ADMITTED_WORKERS_PER_JOB: usize = 32;

/// The worker generations one job can owe an acknowledgement to at the same moment.
///
/// Two: the generation an interrupted scheduling attempt addressed, and the replacement
/// generation a controller takeover addresses while the first is still unacknowledged —
/// M11.D39d's "controller takeover and refusal actively advance the fence at every worker
/// generation addressable by the old scheduling generation". Their worker ids are disjoint,
/// because a replacement generation is issued fresh ids by the scheduler's own counter, which
/// is why they occupy separate entries rather than colliding.
const ADDRESSABLE_GENERATIONS_PER_JOB: usize = 2;

/// The factor by which a deployment may raise its own parallelism ceiling before this record
/// refuses to describe its jobs.
///
/// This is the one quantity in [`MAX_FENCE_TARGETS`] that is stated rather than derived, and
/// it is stated because `max_parallelism` is per-organization configuration — arroyo's own
/// cloud profile sets it to `u32::MAX` — so there is no compile-time worker count to derive
/// from. The capacity has to fail closed on a corrupt record without failing closed on a job
/// an operator legitimately configured, and this factor is where that trade is made
/// explicitly rather than inside a round number.
const RAISED_CEILING_HEADROOM: usize = 32;

/// How many target worker generations one durable fencing record may name.
///
/// The count is also the record's capacity for issued identifiers, and deliberately so: the
/// controller's fan-out ledger (`IssuedAttempts`) keys its records by `WorkerId` and
/// *overwrites* on replay, so the `START_EXECUTION_RECONCILE_ATTEMPTS` ambiguous-transport
/// retries an attempt may spend on one target all carry the identifier that target was minted
/// and cost no further entries. One target is therefore one identifier, and there is no second
/// capacity to state.
///
/// The value is [`DEFAULT_ADMITTED_WORKERS_PER_JOB`] × [`ADDRESSABLE_GENERATIONS_PER_JOB`] ×
/// [`RAISED_CEILING_HEADROOM`]; the first two are derived from the controller and the design,
/// and the third is stated.
pub const MAX_FENCE_TARGETS: usize =
    DEFAULT_ADMITTED_WORKERS_PER_JOB * ADDRESSABLE_GENERATIONS_PER_JOB * RAISED_CEILING_HEADROOM;

/// The width of a `start_execution_id`, in characters.
///
/// The controller mints one as two `u64`s in zero-padded lowercase hexadecimal, so this is
/// `2 × (64 / 4)`. It is an upper bound on what may be persisted rather than an exact match,
/// because the record's job is to stay bounded, not to re-specify the minting format; the
/// controller-side test `the_ledger_holds_one_identifier_per_target_however_much_budget_is_spent`
/// is what pins the two together.
pub const MAX_ATTEMPT_ID_CHARS: usize = 2 * (u64::BITS as usize / 4);

/// How long a candidate-root reference may be, in bytes.
///
/// A candidate root names an object in the job's store, and an object-store key is at most
/// 1024 bytes (the S3 limit, which is the smallest of the stores arroyo writes to). A
/// reference longer than any key that can exist does not name a candidate.
pub const MAX_CANDIDATE_ROOT_BYTES: usize = 1024;

/// The longest host name a target's address can carry, in characters.
///
/// RFC 1035 §2.3.4 caps a domain name at 255 octets of wire form, which is 253 characters of
/// the presentation form a URI carries. Every deployment target arroyo dials — a pod's
/// cluster-local name, a node's host name, an IPv4 or bracketed IPv6 literal — is inside it.
const MAX_TARGET_HOST_CHARS: usize = 253;

/// `https://`, the longer of the two schemes the controller's channel builder produces.
const MAX_TARGET_SCHEME_CHARS: usize = "https://".len();

/// `:` and a 16-bit port at its widest.
const MAX_TARGET_PORT_CHARS: usize = ":65535".len();

/// How long the address of a target worker generation may be, in characters.
///
/// Fully derived, unlike [`MAX_FENCE_TARGETS`]: a target's address is a URI the controller
/// hands to its own channel builder, and each of its three parts has a ceiling that comes from
/// a standard rather than from a deployment's configuration. An address longer than this names
/// no endpoint that could be dialled, so refusing it costs no job an operator could have
/// configured.
pub const MAX_TARGET_ADDRESS_CHARS: usize =
    MAX_TARGET_SCHEME_CHARS + MAX_TARGET_HOST_CHARS + MAX_TARGET_PORT_CHARS;

/// The fencing-record version this build writes and is the only one it accepts.
///
/// A record carrying any other version was written by a build whose rules for the fields
/// below this one does not know. It is refused rather than partially interpreted, which is
/// the same fail-closed choice the execution selector makes for a backend name nobody
/// recognizes.
pub const FENCING_RECORD_VERSION: u32 = 1;

/// Why a durable fencing record was refused.
///
/// Every variant is a refusal of *persisted* input, so each is reachable from a decode as well
/// as from [`Fencing::record`]. None of them is recoverable by guessing: a record that breaks
/// one of these rules describes an obligation this build cannot reason about, and the job it
/// belongs to is skipped rather than administered under a repaired version of its own state.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum FencingRecordError {
    /// The record was written by a build with different rules.
    #[error(
        "fencing record is version {found}, and this build reads only version {FENCING_RECORD_VERSION}"
    )]
    UnknownVersion {
        /// The version the record carried.
        found: u32,
    },
    /// More target generations than the record's derived capacity.
    #[error(
        "fencing record names {found} target worker generations, more than the {MAX_FENCE_TARGETS} \
         one job can owe"
    )]
    TooManyTargets {
        /// How many targets the record named.
        found: usize,
    },
    /// One worker id appears twice, so the record gives two answers about one generation.
    #[error("fencing record names worker {worker_id} more than once")]
    DuplicateTarget {
        /// The worker id that appeared more than once.
        worker_id: u64,
    },
    /// An issued identifier that is empty or longer than one the controller can mint.
    #[error(
        "fencing record carries a {found}-character attempt identifier for worker {worker_id}, \
         which is not between 1 and {MAX_ATTEMPT_ID_CHARS}"
    )]
    MalformedAttemptId {
        /// The worker whose entry carried it.
        worker_id: u64,
        /// The identifier's length in characters.
        found: usize,
    },
    /// A candidate-root reference that is empty or longer than any object-store key.
    #[error(
        "fencing record carries a {found}-byte candidate root reference, which is not between 1 \
         and {MAX_CANDIDATE_ROOT_BYTES}"
    )]
    MalformedCandidateRoot {
        /// The reference's length in bytes.
        found: usize,
    },
    /// A target address that is empty or longer than any endpoint a controller can dial.
    #[error(
        "fencing record carries a {found}-character address for worker {worker_id}, which is \
         not between 1 and {MAX_TARGET_ADDRESS_CHARS}"
    )]
    MalformedTargetAddress {
        /// The worker whose entry carried it.
        worker_id: u64,
        /// The address's length in characters.
        found: usize,
    },
    /// A fencing origin at the Unix epoch, which is not an instant any controller fenced at.
    ///
    /// Zero is refused rather than accepted because [`Fencing::fencing_since_millis`] is an
    /// `Option`: absence already has a spelling, and a second one would let "this obligation
    /// has no recorded origin" and "this obligation began in 1970" be the same durable value
    /// while producing wildly different ages.
    #[error("fencing record carries a fencing origin of 0, which names no instant")]
    MalformedFencingSince,
}

/// What a target worker generation has done about the fence it was addressed under.
///
/// The three values are the three sets M11.T25's in-memory `FenceTargets` keeps, so a durable
/// record is an image of the live obligation rather than a summary of it. Only the last two
/// settle a target (M11.D39e): a pending target has neither acknowledged nor been observed to
/// have gone away, and nothing else — no timeout, no read-through — moves it.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FenceTargetState {
    /// Neither acknowledged nor observed terminated.
    Pending,
    /// Acknowledged the newer fence and its revokes.
    Acknowledged,
    /// Observed to have gone away.
    Terminated,
}

/// One worker generation an interrupted scheduling attempt owes an acknowledgement to.
///
/// `generation` is carried beside `worker_id` because an endpoint can be reused: a restarted
/// worker at the same address is a different generation and must not answer for its
/// predecessor's requests (M11.D39d). `attempt_id` is the `start_execution_id` this target was
/// issued, or `None` for a target that was addressed by the fence without having been issued a
/// start.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FenceTarget {
    /// The worker this obligation is owed by.
    pub worker_id: u64,
    /// The worker generation, which is what makes a reused endpoint a different target.
    pub generation: u64,
    /// The `start_execution_id` this target was issued, if it was issued one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
    /// The address a controller reaches this generation at, if the attempt that recorded the
    /// obligation knew one.
    ///
    /// Without it a recovered obligation can only be discharged by tearing the generation down
    /// and observing its termination: a controller that did not start these workers has never
    /// received their registration and so has no channel to any of them. With it, M11.D39d's
    /// *"controller takeover and refusal actively advance the fence at every worker generation
    /// addressable by the old scheduling generation"* is reachable after the controller that
    /// addressed them is gone, which is the whole point of persisting the obligation at all.
    ///
    /// Addressing a *reused* endpoint is safe rather than merely unlikely: every directive sent
    /// here carries [`Self::generation`], and a worker generation that is not the one addressed
    /// refuses the directive definitively (M11.D39d, M11.T26d's guard). So a successor at the
    /// same address cannot acknowledge its predecessor's fence — the target simply stays
    /// unsettled until it is observed terminated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_address: Option<String>,
    /// What the target has done about the fence.
    pub state: FenceTargetState,
}

/// The shape a durable fencing record is decoded through.
///
/// Private, and the only `Deserialize` in this module that is not checked: it exists so that
/// [`Fencing`]'s own deserialization can be `#[serde(try_from)]`, which is what makes
/// [`Fencing::validate`] unskippable rather than something a reader has to remember to call.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FencingRepr {
    version: u32,
    #[serde(default)]
    targets: Vec<FenceTarget>,
    #[serde(default)]
    candidate_root: Option<String>,
    #[serde(default)]
    fencing_since_millis: Option<u64>,
}

/// A job's durable fencing obligation, as `job_statuses.state_context` carries it.
///
/// It sits beside the execution selector rather than replacing anything: the selector is a
/// property of the job's execution and the fencing record is a property of one interrupted
/// attempt on it, and a job that has never been interrupted has no record at all. Absence is
/// therefore meaningful and is not the same as an empty record — see
/// [`StateContext::fencing`](crate::StateContext::fencing).
///
/// The fields are private and the constructor is fallible because the invariants in
/// [`FencingRecordError`] are what make this durable state bounded and interpretable. There is
/// no way to build one that breaks them and no way to decode one that does.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "FencingRepr")]
pub struct Fencing {
    version: u32,
    targets: Vec<FenceTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fencing_since_millis: Option<u64>,
}

impl Fencing {
    /// Records an obligation over `targets`, with `candidate_root` if the attempt published a
    /// candidate that never became authoritative, and `fencing_since_millis` naming the instant
    /// the obligation began.
    ///
    /// The version is this build's rather than a parameter: a caller that could choose one
    /// could write a record its own build cannot read back.
    ///
    /// The origin is a parameter rather than being stamped here, and that is what makes the age
    /// this record reports survive a controller restart: a controller republishing an obligation
    /// it recovered carries the origin it read forward, so the age is measured from when the job
    /// started fencing rather than from when the current process noticed.
    ///
    /// # Errors
    ///
    /// Every variant of [`FencingRecordError`] except [`FencingRecordError::UnknownVersion`],
    /// which no caller of this function can provoke.
    pub fn record(
        targets: Vec<FenceTarget>,
        candidate_root: Option<String>,
        fencing_since_millis: Option<u64>,
    ) -> Result<Self, FencingRecordError> {
        Self::validate(
            FENCING_RECORD_VERSION,
            &targets,
            candidate_root.as_deref(),
            fencing_since_millis,
        )?;
        Ok(Self {
            version: FENCING_RECORD_VERSION,
            targets,
            candidate_root,
            fencing_since_millis,
        })
    }

    /// The version this record was written at, which is always [`FENCING_RECORD_VERSION`].
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The worker generations this obligation is owed by.
    pub fn targets(&self) -> &[FenceTarget] {
        &self.targets
    }

    /// The fence-scoped candidate root this attempt published, if it published one.
    ///
    /// A candidate is not authoritative: M11.D39d makes it so only through the conditional row
    /// update, and a losing controller's candidate stays unrooted for the grace collector.
    pub fn candidate_root(&self) -> Option<&str> {
        self.candidate_root.as_deref()
    }

    /// When this obligation began, in milliseconds since the Unix epoch, if it was recorded.
    ///
    /// `None` for a record written before the origin existed, and for one a caller had no clock
    /// for. A reader that cannot say when the obligation began reports no age rather than an
    /// age of zero — the second would say "this job has just started fencing" about a job that
    /// may have been fencing for a week.
    pub fn fencing_since_millis(&self) -> Option<u64> {
        self.fencing_since_millis
    }

    /// Checks every rule a durable fencing record is under, before any of it is adopted.
    ///
    /// Ordered so that the cheap bounds run first: the capacity check precedes the duplicate
    /// scan, so an oversized record is refused without this function building an index over
    /// it. Nothing here mutates or takes anything — the value is still the caller's, and is
    /// adopted only after every rule has passed.
    fn validate(
        version: u32,
        targets: &[FenceTarget],
        candidate_root: Option<&str>,
        fencing_since_millis: Option<u64>,
    ) -> Result<(), FencingRecordError> {
        if version != FENCING_RECORD_VERSION {
            return Err(FencingRecordError::UnknownVersion { found: version });
        }
        if targets.len() > MAX_FENCE_TARGETS {
            return Err(FencingRecordError::TooManyTargets {
                found: targets.len(),
            });
        }
        for target in targets {
            if let Some(attempt_id) = &target.attempt_id {
                let found = attempt_id.chars().count();
                if found == 0 || found > MAX_ATTEMPT_ID_CHARS {
                    return Err(FencingRecordError::MalformedAttemptId {
                        worker_id: target.worker_id,
                        found,
                    });
                }
            }
            if let Some(rpc_address) = &target.rpc_address {
                let found = rpc_address.chars().count();
                if found == 0 || found > MAX_TARGET_ADDRESS_CHARS {
                    return Err(FencingRecordError::MalformedTargetAddress {
                        worker_id: target.worker_id,
                        found,
                    });
                }
            }
        }
        let mut seen = std::collections::BTreeSet::new();
        for target in targets {
            if !seen.insert(target.worker_id) {
                return Err(FencingRecordError::DuplicateTarget {
                    worker_id: target.worker_id,
                });
            }
        }
        if let Some(candidate_root) = candidate_root {
            let found = candidate_root.len();
            if found == 0 || found > MAX_CANDIDATE_ROOT_BYTES {
                return Err(FencingRecordError::MalformedCandidateRoot { found });
            }
        }
        if fencing_since_millis == Some(0) {
            return Err(FencingRecordError::MalformedFencingSince);
        }
        Ok(())
    }
}

impl TryFrom<FencingRepr> for Fencing {
    type Error = FencingRecordError;

    fn try_from(repr: FencingRepr) -> Result<Self, Self::Error> {
        Fencing::validate(
            repr.version,
            &repr.targets,
            repr.candidate_root.as_deref(),
            repr.fencing_since_millis,
        )?;
        Ok(Self {
            version: repr.version,
            targets: repr.targets,
            candidate_root: repr.candidate_root,
            fencing_since_millis: repr.fencing_since_millis,
        })
    }
}

#[cfg(test)]
mod tests;
