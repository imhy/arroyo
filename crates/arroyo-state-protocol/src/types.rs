use crate::ProtocolPaths;
/// Protobuf checkpoint manifest used as the publication point for checkpoint data.
pub use arroyo_rpc::grpc::rpc::CheckpointManifest;
use arroyo_rpc::state_backend::validated::identity::CheckpointIdentity;
use arroyo_types::{JobId, PipelineId, to_micros};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::time::SystemTime;
use thiserror::Error;

/// Current version for JSON protocol records written by this crate.
pub const PROTOCOL_VERSION: u32 = 1;

/// Errors returned when observed protocol objects violate protocol invariants.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("invalid checkpoint ref `{path}`: {reason}")]
    InvalidCheckpointRef { path: String, reason: &'static str },
    #[error("invalid path component `{name}` with value `{value}`: {reason}")]
    InvalidPathComponent {
        name: &'static str,
        value: String,
        reason: &'static str,
    },
    #[error(
        "epoch record for epoch {record_epoch} cannot describe checkpoint at epoch {checkpoint_epoch}"
    )]
    EpochMismatch {
        checkpoint_epoch: Epoch,
        record_epoch: Epoch,
    },
    #[error("epoch record parent does not match checkpoint manifest parent")]
    ParentMismatch,
    #[error("committed marker does not match checkpoint")]
    CommittedMarkerMismatch,
    #[error("checkpoint manifest does not match protocol record")]
    CheckpointManifestMismatch,
    #[error("recovery checkpoint manifest `{checkpoint_ref}` is missing")]
    MissingCheckpointManifest { checkpoint_ref: CheckpointRef },
    #[error("update to current generation would have caused a non-monotonic generation update")]
    NonMonotonicGenerationUpdate,
    #[error("checkpoint parent chain contains a cycle at generation {generation}, epoch {epoch}")]
    CheckpointCycle {
        generation: Generation,
        epoch: Epoch,
    },
    #[error("checkpoint GC minimum epoch {new_min_epoch} is newer than head epoch {head_epoch}")]
    CheckpointGcMinEpochBeyondHead {
        head_epoch: Epoch,
        new_min_epoch: Epoch,
    },
    /// A checkpoint manifest read back from storage is not the checkpoint the reference it was
    /// read from names.
    ///
    /// Every path that acts on a manifest — the generation manifest that records it as a
    /// recovery point, the traversal that follows its parent link, the deletes that are built
    /// from its generation and epoch — is built out of the identity it claims, so a misplaced
    /// or corrupt object aims those paths rather than merely describing itself wrongly. See
    /// [`identify_checkpoint_manifest`].
    #[error(
        "the checkpoint manifest at `{checkpoint_ref}` claims to be {claimed}, which is not \
         the checkpoint that reference names"
    )]
    CheckpointManifestMisplaced {
        checkpoint_ref: CheckpointRef,
        claimed: String,
    },
    /// A garbage-collection plan would delete a checkpoint the reachable-history traversal
    /// never read, so nothing validated the manifest that named its files.
    #[error(
        "checkpoint GC would delete generation {generation}, epoch {epoch}, which the \
         reachable history traversal never reached"
    )]
    CheckpointGcUnreached {
        generation: Generation,
        epoch: Epoch,
    },
}

/// Monotonic identifier for a worker cluster generation of a job.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct Generation(pub u64);

impl Display for Generation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Deref for Generation {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Monotonic identifier for a checkpoint epoch.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct Epoch(pub u64);

impl Epoch {
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl Deref for Epoch {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for Epoch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Relative object-store path to a checkpoint/protocol object.
///
/// Use [`CheckpointRef::new`] for externally supplied paths. It rejects absolute
/// paths and parent-directory traversal so protocol records can be safely
/// relocated under a checkpoint URI.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CheckpointRef(String);

impl CheckpointRef {
    /// Validates and constructs a checkpoint reference from a relative path.
    pub fn new(path: impl Into<String>) -> Result<Self, ProtocolError> {
        let path = path.into();
        validate_ref(&path)?;
        Ok(Self(path))
    }

    pub(crate) fn from_validated(path: String) -> Self {
        debug_assert!(validate_ref(&path).is_ok());
        Self(path)
    }

    /// Returns the underlying relative path string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for CheckpointRef {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for CheckpointRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

fn validate_ref(path: &str) -> Result<(), ProtocolError> {
    if path.is_empty() {
        return Err(invalid_ref(path, "path is empty"));
    }

    if path.starts_with('/') {
        return Err(invalid_ref(path, "path must be relative"));
    }

    if path.ends_with('/') {
        return Err(invalid_ref(path, "path must identify an object"));
    }

    if path.contains('\\') {
        return Err(invalid_ref(path, "path must use `/` separators"));
    }

    if path
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(invalid_ref(path, "path contains an invalid segment"));
    }

    if path.len() > 1024 {
        return Err(invalid_ref(path, "path length must be <= 1024"));
    }

    Ok(())
}

fn invalid_ref(path: &str, reason: &'static str) -> ProtocolError {
    ProtocolError::InvalidCheckpointRef {
        path: path.to_string(),
        reason,
    }
}

/// Controller-written fence naming the current generation for a job.
///
/// Workers should read this before generation initialization, publication, and
/// other ownership-sensitive operations. It is advisory; canonical ownership is
/// still determined by epoch records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentGeneration {
    pub version: u32,
    pub pipeline_id: PipelineId,
    pub job_id: JobId,
    pub generation: Generation,
    pub generation_manifest_ref: CheckpointRef,
    pub updated_at_micros: u64,
}

impl CurrentGeneration {
    /// Builds a current-generation record with the current protocol version.
    pub fn new(
        pipeline_id: PipelineId,
        job_id: JobId,
        generation: Generation,
        updated_at: SystemTime,
    ) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            generation_manifest_ref: ProtocolPaths::new(pipeline_id.clone(), job_id.clone())
                .generation_manifest(generation),
            pipeline_id,
            job_id,
            generation,
            updated_at_micros: to_micros(updated_at),
        }
    }
}

/// Per-generation candidate recovery manifest.
///
/// `latest_checkpoint_ref` is only a candidate pointer. Callers must resolve it
/// through `workflow::resolve_generation_manifest` before restoring from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationManifest {
    pub version: u32,
    pub pipeline_id: PipelineId,
    pub job_id: JobId,
    pub generation: Generation,
    pub base_checkpoint_ref: Option<CheckpointRef>,
    pub latest_checkpoint_ref: Option<CheckpointRef>,
    pub updated_at_micros: u64,
}

impl GenerationManifest {
    /// Creates a new generation manifest with `latest_checkpoint_ref` unset.
    ///
    /// New generations should set `base_checkpoint_ref` to the checkpoint
    /// returned by `workflow::initialize_generation`, if any.
    pub fn new(
        pipeline_id: PipelineId,
        job_id: JobId,
        generation: Generation,
        base_checkpoint_ref: Option<CheckpointRef>,
        updated_at_micros: u64,
    ) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            pipeline_id,
            job_id,
            generation,
            base_checkpoint_ref,
            latest_checkpoint_ref: None,
            updated_at_micros,
        }
    }

    /// Returns `latest_checkpoint_ref` if present, otherwise `base_checkpoint_ref`.
    ///
    /// This value is a candidate only; do not restore from it without resolving
    /// it against epoch records.
    pub fn candidate_checkpoint_ref(&self) -> Option<&CheckpointRef> {
        self.latest_checkpoint_ref
            .as_ref()
            .or(self.base_checkpoint_ref.as_ref())
    }
}

/// Marker written after all external commit work for a checkpoint completes.
///
/// This object is immutable and should be created conditionally. If creation
/// races with a retry, an existing marker for the same checkpoint is success.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedMarker {
    pub version: u32,
    pub pipeline_id: PipelineId,
    pub job_id: JobId,
    pub epoch: Epoch,
    pub checkpoint_generation: Generation,
    pub writer_generation: Generation,
    pub checkpoint_ref: CheckpointRef,
}

impl CommittedMarker {
    /// Builds a commit-completion marker with the current protocol version.
    pub fn new(
        pipeline_id: PipelineId,
        job_id: JobId,
        epoch: Epoch,
        checkpoint_generation: Generation,
        writer_generation: Generation,
        checkpoint_ref: CheckpointRef,
    ) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            pipeline_id,
            job_id,
            epoch,
            checkpoint_generation,
            writer_generation,
            checkpoint_ref,
        }
    }
}

/// Canonical ownership record for an epoch.
///
/// Exactly one checkpoint may own an epoch record. For non-committing
/// checkpoints, this makes the checkpoint recoverable. For committing
/// checkpoints, it also authorizes sending external commit requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochRecord {
    pub version: u32,
    pub pipeline_id: PipelineId,
    pub job_id: JobId,
    pub epoch: Epoch,
    pub generation: Generation,
    pub parent_checkpoint_ref: Option<CheckpointRef>,
    pub checkpoint_ref: CheckpointRef,
    pub created_at_micros: u64,
}

impl EpochRecord {
    /// Builds the epoch record that canonically assigns `checkpoint_ref` to the
    /// checkpoint's epoch.
    ///
    /// Callers should write this with conditional-create semantics, normally via
    /// `workflow::claim_epoch_record` rather than writing it directly.
    pub fn for_checkpoint(
        pipeline_id: PipelineId,
        generation: Generation,
        checkpoint_ref: CheckpointRef,
        checkpoint: &CheckpointManifest,
        created_at: SystemTime,
    ) -> Result<Self, ProtocolError> {
        Ok(Self {
            version: PROTOCOL_VERSION,
            pipeline_id,
            job_id: JobId::new(&checkpoint.job_id),
            epoch: Epoch(checkpoint.epoch),
            generation,
            parent_checkpoint_ref: checkpoint_parent_checkpoint_ref(checkpoint)?,
            checkpoint_ref,
            created_at_micros: to_micros(created_at),
        })
    }
}

pub(crate) fn checkpoint_parent_checkpoint_ref(
    checkpoint: &CheckpointManifest,
) -> Result<Option<CheckpointRef>, ProtocolError> {
    checkpoint
        .parent_checkpoint_ref
        .as_ref()
        .map(|checkpoint_ref| CheckpointRef::new(checkpoint_ref.clone()))
        .transpose()
}

/// Establishes which checkpoint a manifest is, by requiring it to agree with the reference it
/// was read from.
///
/// A `CheckpointManifest` read back out of storage carries its own `pipeline_id`, `job_id`,
/// `generation` and `epoch`, and until review round 7 of PR #160 nothing on either leader-mode
/// path compared any of the four with the `CheckpointRef` the bytes came from. That is not a
/// description problem. `resolve_generation_manifest` rebuilds its [`ProtocolPaths`] out of
/// the first two, and leader GC builds every object it deletes —
/// `paths.checkpoint_manifest`, `paths.committed_marker`, `paths.epoch_record` — out of the
/// last two. A misplaced or corrupt object therefore *aims* later reads and deletes at a
/// checkpoint nobody asked about.
///
/// The check is by reconstruction rather than by parsing: `paths` is built from the job's own
/// pipeline and job ids, so rebuilding the manifest path from the identity the object claims
/// and comparing it with the reference the object was read from binds all four fields at once,
/// without taking a path apart into components an id could contain.
///
/// It returns the [`CheckpointIdentity`] rather than `()` on purpose. That identity is what
/// [`arroyo_rpc::state_backend::validate_manifest_covers_program`] compares the manifest's
/// entries against, and producing it here is what makes "the entries belong to the checkpoint
/// the reference names" unbypassable: there is no other constructor of the value that check
/// takes, so a caller cannot reach the entry check without having passed this one.
///
/// # What is deliberately *not* required
///
/// Nothing here says the manifest's generation or epoch is the current one. A recovery
/// checkpoint is by construction from an earlier generation and an earlier epoch — that is what
/// recovering means — and a history traversal spans a range of both. What is required is that
/// each manifest is the checkpoint *its own reference* names, which is a different quantity
/// from the generation being published, and confusing the two would refuse every legitimate
/// publication that has any history to recover from.
///
/// # Errors
///
/// Returns [`ProtocolError::CheckpointManifestMisplaced`] if the manifest names another
/// pipeline or job, if rebuilding its path from its own generation and epoch does not produce
/// `checkpoint_ref`, or if its epoch is wider than the epoch a checkpoint object can carry.
pub(crate) fn identify_checkpoint_manifest(
    paths: &ProtocolPaths,
    checkpoint_ref: &CheckpointRef,
    manifest: &CheckpointManifest,
) -> Result<CheckpointIdentity, ProtocolError> {
    let misplaced = || ProtocolError::CheckpointManifestMisplaced {
        checkpoint_ref: checkpoint_ref.clone(),
        claimed: format!(
            "pipeline {}, job {}, generation {}, epoch {}",
            manifest.pipeline_id, manifest.job_id, manifest.generation, manifest.epoch
        ),
    };

    if manifest.pipeline_id != **paths.pipeline_id() || manifest.job_id != **paths.job_id() {
        return Err(misplaced());
    }

    if paths.checkpoint_manifest(Generation(manifest.generation), Epoch(manifest.epoch))
        != *checkpoint_ref
    {
        return Err(misplaced());
    }

    CheckpointIdentity::at_wide_epoch(manifest.job_id.as_str(), manifest.epoch, |_| misplaced())
}

pub(crate) fn validate_epoch_record_matches_checkpoint(
    checkpoint_ref: &CheckpointRef,
    checkpoint: &CheckpointManifest,
    record: &EpochRecord,
) -> Result<(), ProtocolError> {
    let checkpoint_epoch = Epoch(checkpoint.epoch);
    if checkpoint_epoch != record.epoch {
        return Err(ProtocolError::EpochMismatch {
            checkpoint_epoch,
            record_epoch: record.epoch,
        });
    }

    if checkpoint_parent_checkpoint_ref(checkpoint)? != record.parent_checkpoint_ref {
        return Err(ProtocolError::ParentMismatch);
    }

    debug_assert_eq!(checkpoint_ref, &record.checkpoint_ref);
    Ok(())
}

pub(crate) fn validate_committed_marker_matches_checkpoint(
    checkpoint_ref: &CheckpointRef,
    checkpoint: &CheckpointManifest,
    marker: &CommittedMarker,
) -> Result<(), ProtocolError> {
    if *marker.job_id != checkpoint.job_id
        || marker.epoch != Epoch(checkpoint.epoch)
        || &marker.checkpoint_ref != checkpoint_ref
    {
        return Err(ProtocolError::CommittedMarkerMismatch);
    }

    Ok(())
}
