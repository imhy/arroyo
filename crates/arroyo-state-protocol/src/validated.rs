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
    ///
    /// The identity half is the leader-mode row of the matrix in
    /// [`arroyo_state::validated`](../../arroyo_state/validated/index.html): the recovery
    /// manifest has to belong to the pipeline and job that is publishing. It is the one
    /// identity of the four a manifest carries that must be *equal*; the generation and the
    /// epoch legitimately differ, because a recovery checkpoint is always an earlier one.
    ///
    /// One relationship inside the manifest is **not** checked here and is disclosed rather
    /// than assumed away: each entry's own `OperatorMetadata` header carries a `job_id` and an
    /// `epoch`, and nothing compares them with the manifest's. Closing that means tightening
    /// [`validate_manifest_covers_program`], which is landed M11.T08 code whose regression
    /// fixtures deliberately build entries whose header job and epoch differ from the
    /// manifest's — so it cannot be closed without editing carried tests, which M11.T25's plan
    /// forbids. See PR #160 review round 6.
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

        // Which job's checkpoint this is. The manifest was read from a reference under this
        // job's own prefix, and it names the pipeline and job it belongs to; until review round
        // 6 of PR #160 nothing compared the two, so a manifest belonging to another job, stored
        // where this one's belongs, was published as this generation's recovery point — and
        // `resolve_generation_manifest` rebuilds its `ProtocolPaths` out of exactly those two
        // fields, so the mis-identification would then aim later reads and deletes.
        //
        // The *generation* and the *epoch* are deliberately not compared. A recovery checkpoint
        // is by construction from an earlier generation and an earlier epoch — that is what
        // recovering means — so demanding equality there would refuse every legitimate
        // publication that has any history to recover from.
        if checkpoint.pipeline_id != *self.pipeline_id || checkpoint.job_id != *self.job_id {
            return Err(StoreError::Protocol(
                ProtocolError::CheckpointManifestMismatch,
            ));
        }

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

#[cfg(test)]
mod tests {
    use super::{GenerationPublication, PublishingJob};
    use crate::store::StoreError;
    use crate::types::{CheckpointRef, Generation, ProtocolError};
    use arroyo_rpc::grpc::rpc::CheckpointManifest;
    use arroyo_rpc::state_backend::StateBackendSelector;
    use arroyo_rpc::state_backend::validated::Validated;
    use arroyo_types::{JobId, PipelineId};
    use std::collections::HashSet;
    use std::time::SystemTime;

    /// A recovery manifest claiming to belong to `pipeline_id`/`job_id`, at `generation` and
    /// `epoch`, describing no operators.
    fn recovery(
        pipeline_id: &str,
        job_id: &str,
        generation: u64,
        epoch: u64,
    ) -> CheckpointManifest {
        CheckpointManifest {
            pipeline_id: pipeline_id.to_string(),
            job_id: job_id.to_string(),
            generation,
            epoch,
            min_epoch: epoch,
            operators: vec![],
            ..Default::default()
        }
    }

    /// A publication of generation 4 of pipeline `P`, job `J`, recovering from `manifest`.
    fn publish(manifest: CheckpointManifest) -> Result<(), StoreError> {
        let program: HashSet<&str> = HashSet::new();
        Validated::validate(
            GenerationPublication::new(
                PipelineId::new("P"),
                JobId::new("J"),
                Generation(4),
                SystemTime::UNIX_EPOCH,
                Some(CheckpointRef::new("P/J/checkpoints/g-1/e-2").unwrap()),
                Some(manifest),
            ),
            PublishingJob {
                state_backend: StateBackendSelector::Parquet,
                program_operators: &program,
            },
        )
        .map(|_| ())
    }

    /// A generation may only be published against a recovery checkpoint belonging to the job
    /// that is publishing — and its generation and epoch legitimately differ (PR #160 review
    /// round 6).
    ///
    /// The leader-mode row of the identity matrix. `resolve_generation_manifest` rebuilds its
    /// `ProtocolPaths` out of the manifest's own `pipeline_id` and `job_id`, so a manifest
    /// belonging to another job — stored where this one's belongs — would aim later reads and
    /// deletes. The positive case has to come first, because a rule of "the manifest names this
    /// generation and this epoch" would refuse every publication that has any history at all:
    /// recovering *means* reaching back past the current generation.
    #[test]
    fn a_generation_publishes_only_against_its_own_jobs_recovery_checkpoint() {
        publish(recovery("P", "J", 1, 2))
            .expect("an earlier generation and epoch of this job is what recovery is");

        // Each identity varied on its own, from a manifest that agrees in the other.
        let other_job = publish(recovery("P", "J2", 1, 2)).unwrap_err();
        assert!(
            matches!(
                other_job,
                StoreError::Protocol(ProtocolError::CheckpointManifestMismatch)
            ),
            "{other_job:?}"
        );

        let other_pipeline = publish(recovery("P2", "J", 1, 2)).unwrap_err();
        assert!(
            matches!(
                other_pipeline,
                StoreError::Protocol(ProtocolError::CheckpointManifestMismatch)
            ),
            "{other_pipeline:?}"
        );

        // ...and the two that must *not* be compared, stated as a positive so the exemption is
        // pinned rather than merely absent: the generation being published is 4, and a recovery
        // checkpoint from generation 3 at a much older epoch is ordinary.
        publish(recovery("P", "J", 3, 0))
            .expect("a recovery checkpoint is always from an earlier generation and epoch");
    }
}
