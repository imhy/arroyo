//! Whole-object validation for the protocol's publishing and collecting operations
//! (design item M11.D39c).
//!
//! Two of the five call-site families D39c names live in this crate. Publishing a generation
//! commits a job to the checkpoint it will restore from, and garbage-collecting the leader's
//! history deletes the objects a restart would otherwise read; both act on a whole object —
//! a manifest that has to describe exactly the program's operators, and a reachable chain
//! every link of which names the files that are about to go — and both have a first
//! irreversible effect after which a later disagreement is useless.
//!
//! The types here are those whole objects. Their [`WholeObject`] impls are the complete
//! statement about each, and [`crate::workflow`] and [`crate::gc`] hand the token to the
//! functions that write and delete, so a new caller cannot reach either effect without
//! having produced one. See [`arroyo_rpc::state_backend::validated`] for what makes a token
//! unforgeable.

use crate::gc::CheckpointOwner;
use crate::store::StoreError;
use crate::types::{
    CheckpointRef, CurrentGeneration, Generation, GenerationManifest, ProtocolError,
};
use arroyo_rpc::grpc::rpc::{CheckpointManifest, OperatorCheckpointMetadata};
use arroyo_rpc::state_backend::validated::{Validated, WholeObject};
use arroyo_rpc::state_backend::{
    StateBackendSelector, validate_manifest_covers_program, validate_restored_manifest,
};
use arroyo_types::{JobId, PipelineId, to_micros};
use std::collections::HashSet;
use std::time::SystemTime;

/// The job a generation is being published for.
#[derive(Debug, Clone, Copy)]
pub struct PublishingJob<'a> {
    /// The state backend the job selects.
    pub state_backend: StateBackendSelector,
    /// Every operator id the job's workers will construct — the key set of
    /// `LogicalProgram::tasks_per_operator` for the *current* program.
    pub program_operators: &'a HashSet<&'a str>,
}

/// Everything a generation publication commits the job to.
///
/// Publishing writes two persistent objects: the current-generation fence and the generation
/// manifest, which records the link to the checkpoint this generation will restore from.
/// Both are protocol state a restart reads, so the checkpoint they point at has to be
/// resolved, read, and checked before either is written — validating afterwards reports the
/// problem only once the job has already advanced.
#[derive(Debug, Clone)]
pub struct GenerationPublication {
    pipeline_id: PipelineId,
    job_id: JobId,
    generation: Generation,
    updated_at: SystemTime,
    base_checkpoint_ref: Option<CheckpointRef>,
    recovery_checkpoint: Option<CheckpointManifest>,
}

impl GenerationPublication {
    /// Collects the publication, before it is checked.
    ///
    /// `base_checkpoint_ref` and `recovery_checkpoint` are the reference the generation
    /// manifest would record and the manifest that was read from it; a generation with no
    /// history to recover from passes `None` for both.
    pub fn new(
        pipeline_id: PipelineId,
        job_id: JobId,
        generation: Generation,
        updated_at: SystemTime,
        base_checkpoint_ref: Option<CheckpointRef>,
        recovery_checkpoint: Option<CheckpointManifest>,
    ) -> Self {
        Self {
            pipeline_id,
            job_id,
            generation,
            updated_at,
            base_checkpoint_ref,
            recovery_checkpoint,
        }
    }

    /// The generation being published.
    pub fn generation(&self) -> Generation {
        self.generation
    }

    /// The current-generation fence this publication would write.
    pub fn current_generation(&self, now: SystemTime) -> CurrentGeneration {
        CurrentGeneration::new(
            self.pipeline_id.clone(),
            self.job_id.clone(),
            self.generation,
            now,
        )
    }

    /// The generation manifest this publication would write.
    pub fn generation_manifest(&self) -> GenerationManifest {
        GenerationManifest::new(
            self.pipeline_id.clone(),
            self.job_id.clone(),
            self.generation,
            self.base_checkpoint_ref.clone(),
            to_micros(self.updated_at),
        )
    }

    /// The recovery checkpoint's manifest, for a caller that acts on the object that was
    /// checked rather than reading it again.
    pub fn into_recovery_checkpoint(self) -> Option<CheckpointManifest> {
        self.recovery_checkpoint
    }
}

impl WholeObject for GenerationPublication {
    type Context<'a> = PublishingJob<'a>;
    type Error = StoreError;

    /// A generation may only be published against a checkpoint that was read and that this
    /// job's workers can actually restore.
    ///
    /// Both halves are whole-set claims rather than per-entry ones. The manifest must
    /// describe exactly the program's operators, one entry each, carrying an operator header
    /// — every one of those operators looks itself up in it as it builds its state, so an
    /// entry the manifest merely happens to contain proves nothing. And every table config
    /// in it must agree with the job's selector, because a manifest another backend wrote
    /// describes state this one cannot read.
    ///
    /// The structural half — that a recorded checkpoint reference has a manifest beside it,
    /// and vice versa — is what stops a publication naming a checkpoint whose manifest was
    /// never read at all, which is the state the two checks above would otherwise have
    /// nothing to say about.
    fn check_whole(&self, job: PublishingJob<'_>) -> Result<(), StoreError> {
        match (&self.base_checkpoint_ref, &self.recovery_checkpoint) {
            (Some(checkpoint_ref), None) => {
                return Err(StoreError::Protocol(
                    ProtocolError::MissingCheckpointManifest {
                        checkpoint_ref: checkpoint_ref.clone(),
                    },
                ));
            }
            (None, Some(_)) => {
                return Err(StoreError::Protocol(
                    ProtocolError::CheckpointManifestMismatch,
                ));
            }
            (None, None) => return Ok(()),
            (Some(_), Some(_)) => {}
        }

        let checkpoint = self
            .recovery_checkpoint
            .as_ref()
            .expect("matched as present above");
        validate_manifest_covers_program(checkpoint, job.program_operators)?;
        validate_restored_manifest(job.state_backend, checkpoint)?;
        Ok(())
    }
}

/// One checkpoint the history traversal reached, reduced to what says who wrote it.
///
/// The manifest itself is deliberately not kept: its bulk is the per-table file lists, and
/// buffering those for the whole reachable chain is the memory cost leader GC exists to
/// avoid. What is kept is the operator headers and table configs — the part the selector
/// check reads — so the whole-set check below costs a few configs per checkpoint rather than
/// the chain's file names twice over.
#[derive(Debug, Clone)]
struct ReachedCheckpoint {
    owner: CheckpointOwner,
    selectors: CheckpointManifest,
}

/// The reachable checkpoint history a garbage-collection pass is about to delete from.
///
/// Traversal is the only thing that names the objects this deletes, so the claim the
/// deletion depends on is about the whole chain — retained links included, because a
/// retained checkpoint's files are what protect them from an older link that also
/// references them.
#[derive(Debug, Clone, Default)]
pub struct CheckpointHistory {
    reached: Vec<ReachedCheckpoint>,
    old_checkpoints: Vec<CheckpointOwner>,
    data_files: Vec<CheckpointRef>,
}

impl CheckpointHistory {
    /// Records one checkpoint the traversal read, keeping only what says who wrote it.
    ///
    /// Called as each manifest is read rather than afterwards, so the traversal never holds
    /// more than one full manifest at a time.
    pub(crate) fn reached(&mut self, owner: CheckpointOwner, manifest: &CheckpointManifest) {
        self.reached.push(ReachedCheckpoint {
            owner,
            selectors: selector_evidence(manifest),
        });
    }

    /// Records the classification the traversal produced: the checkpoints below the
    /// retention boundary, newest to oldest, and the deduplicated files only they reference.
    pub(crate) fn classified(
        &mut self,
        old_checkpoints: Vec<CheckpointOwner>,
        data_files: Vec<CheckpointRef>,
    ) {
        self.old_checkpoints = old_checkpoints;
        self.data_files = data_files;
    }

    /// The checkpoints below the retention boundary, newest to oldest.
    pub(crate) fn old_checkpoints(&self) -> &[CheckpointOwner] {
        &self.old_checkpoints
    }

    /// The data files only those checkpoints reference.
    pub(crate) fn data_files(&self) -> &[CheckpointRef] {
        &self.data_files
    }
}

impl WholeObject for CheckpointHistory {
    type Context<'a> = StateBackendSelector;
    type Error = StoreError;

    /// Every checkpoint the traversal reached agrees with the job, and every checkpoint the
    /// pass would delete is one the traversal reached.
    ///
    /// The second half is what makes the first one worth anything: without it a plan could
    /// name a checkpoint for deletion that no manifest in the chain ever described, and the
    /// selector check over the chain would have nothing to say about it.
    ///
    /// What this does *not* re-derive is which files belong to which checkpoint — that is
    /// the traversal's own job, and re-checking it would mean buffering the file lists the
    /// traversal exists to stream past.
    fn check_whole(&self, job: StateBackendSelector) -> Result<(), StoreError> {
        for checkpoint in &self.reached {
            validate_restored_manifest(job, &checkpoint.selectors)?;
        }

        let reached: HashSet<CheckpointOwner> =
            self.reached.iter().map(|reached| reached.owner).collect();
        if let Some(unreached) = self
            .old_checkpoints
            .iter()
            .find(|owner| !reached.contains(owner))
        {
            return Err(StoreError::Protocol(ProtocolError::CheckpointGcUnreached {
                generation: unreached.generation,
                epoch: unreached.epoch,
            }));
        }

        Ok(())
    }
}

/// Reduces a manifest to the operator headers and table configs the selector check reads.
fn selector_evidence(manifest: &CheckpointManifest) -> CheckpointManifest {
    CheckpointManifest {
        epoch: manifest.epoch,
        generation: manifest.generation,
        operators: manifest
            .operators
            .iter()
            .map(|operator| OperatorCheckpointMetadata {
                operator_metadata: operator.operator_metadata.clone(),
                table_configs: operator.table_configs.clone(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// Checks a classified history as a whole, yielding the token the deletion takes.
///
/// # Errors
///
/// Returns [`StoreError::StateBackend`] if any reachable manifest disagrees with `job`, or
/// [`StoreError::Protocol`] if the plan would delete a checkpoint the traversal never
/// reached. In either case nothing has been deleted.
pub(crate) fn validate_history(
    history: CheckpointHistory,
    job: StateBackendSelector,
) -> Result<Validated<CheckpointHistory>, StoreError> {
    Validated::validate(history, job)
}
