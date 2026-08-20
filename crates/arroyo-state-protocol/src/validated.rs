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

use crate::ProtocolPaths;
use crate::gc::CheckpointOwner;
use crate::store::StoreError;
use crate::types::{
    CheckpointRef, CurrentGeneration, Epoch, Generation, GenerationManifest, ProtocolError,
    identify_checkpoint_manifest,
};
use arroyo_rpc::grpc::rpc::{CheckpointManifest, OperatorCheckpointMetadata};
use arroyo_rpc::state_backend::validated::{Validated, WholeObject};
use arroyo_rpc::state_backend::{
    StateBackendSelector, validate_manifest_covers_program, validate_manifest_identity,
    validate_restored_manifest,
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

    /// A generation may only be published against a checkpoint that was read, that is the
    /// checkpoint the reference it was read from names, and that this job's workers can
    /// actually restore.
    ///
    /// The claims are whole-set ones rather than per-entry ones. The manifest must describe
    /// exactly the program's operators, one entry each, carrying an operator header — every one
    /// of those operators looks itself up in it as it builds its state, so an entry the manifest
    /// merely happens to contain proves nothing. And every table config in it must agree with
    /// the job's selector, because a manifest another backend wrote describes state this one
    /// cannot read.
    ///
    /// The structural half — that a recorded checkpoint reference has a manifest beside it,
    /// and vice versa — is what stops a publication naming a checkpoint whose manifest was
    /// never read at all, which is the state the two checks above would otherwise have
    /// nothing to say about.
    ///
    /// # The identity half
    ///
    /// [`identify_checkpoint_manifest`] binds the manifest's own `pipeline_id`, `job_id`,
    /// `generation` and `epoch` to the [`CheckpointRef`] the recovery search resolved and read
    /// it from, and hands back the [`CheckpointIdentity`] the entries are then checked against.
    /// Review round 6 of PR #160 compared only the pipeline and the job, and review round 7
    /// found the rest of the row open: a misplaced or corrupt object whose selector and outer
    /// job still matched was published as this generation's recovery point, and
    /// `resolve_generation_manifest` rebuilds its paths out of exactly those fields.
    ///
    /// The generation and the epoch are compared **against the reference**, and deliberately
    /// not against the generation being published. A recovery checkpoint is by construction
    /// from an earlier generation and an earlier epoch — that is what recovering means — so
    /// demanding equality with the publishing generation would refuse every legitimate
    /// publication that has any history at all. Those are two different quantities, and
    /// `a_recovery_manifest_must_be_the_checkpoint_its_reference_names` pins both directions.
    ///
    /// [`CheckpointIdentity`]: arroyo_rpc::state_backend::validated::identity::CheckpointIdentity
    fn check_whole(&self, job: PublishingJob<'_>) -> Result<(), StoreError> {
        let checkpoint_ref = match (&self.base_checkpoint_ref, &self.recovery_checkpoint) {
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
            (Some(checkpoint_ref), Some(_)) => checkpoint_ref,
        };

        let checkpoint = self
            .recovery_checkpoint
            .as_ref()
            .expect("matched as present above");

        // Which checkpoint this is, established against the reference the manifest was read
        // from rather than taken from the manifest's own word for it. Everything below depends
        // on the answer: the entry headers are compared with it, and the generation manifest
        // this publication writes records the reference itself.
        let paths = ProtocolPaths::new(self.pipeline_id.clone(), self.job_id.clone());
        let identity = identify_checkpoint_manifest(&paths, checkpoint_ref, checkpoint)?;

        validate_manifest_covers_program(checkpoint, &identity, job.program_operators)?;
        validate_restored_manifest(job.state_backend, checkpoint)?;
        Ok(())
    }
}

/// The job a garbage-collection pass is collecting for.
///
/// `paths` is not decoration: the check below asks whether each reached manifest is the
/// checkpoint the reference it was read from names, and the only way to ask that is against
/// the path builder the job's own pipeline and job ids produce. Handing it in is also what
/// keeps the answer out of the manifest's own hands.
#[derive(Debug, Clone, Copy)]
pub struct CollectingJob<'a> {
    /// The state backend the job selected.
    pub state_backend: StateBackendSelector,
    /// The path builder for this job's protocol objects.
    pub paths: &'a ProtocolPaths,
}

/// One checkpoint the history traversal reached: where it was read from, and what it says
/// about itself.
///
/// The manifest itself is deliberately not kept in full: its bulk is the per-table file lists,
/// and buffering those for the whole reachable chain is the memory cost leader GC exists to
/// avoid. What is kept is the identity fields and the operator headers and table configs — the
/// part the identity and selector checks read — so the whole-set check below costs a few
/// strings and configs per checkpoint rather than the chain's file names twice over.
///
/// `checkpoint_ref` is the reference the traversal read the bytes from, and `owner` is the
/// generation and epoch the bytes claim for themselves. Review round 7 of PR #160 is the round
/// that made those two different things: `owner` is what every delete path is built from, so
/// until something compares it with the reference it came from, the manifest's own word for
/// which checkpoint it is aims the deletion.
#[derive(Debug, Clone)]
struct ReachedCheckpoint {
    checkpoint_ref: CheckpointRef,
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
    /// Records one checkpoint the traversal read: the reference it came from, and what it says
    /// about itself.
    ///
    /// Called as each manifest is read rather than afterwards, so the traversal never holds
    /// more than one full manifest at a time.
    ///
    /// The owner is derived here rather than passed in, so that the generation and epoch this
    /// records are the manifest's own claim and nothing else — the caller cannot hand over a
    /// claim and a correction to it. Binding that claim to `checkpoint_ref` is
    /// [`Self::check_whole`]'s job, and the reference is not an `Option`: a caller cannot
    /// record a reached checkpoint without saying where it read it.
    pub(crate) fn reached(&mut self, checkpoint_ref: CheckpointRef, manifest: &CheckpointManifest) {
        self.reached.push(ReachedCheckpoint {
            checkpoint_ref,
            owner: CheckpointOwner {
                generation: Generation(manifest.generation),
                epoch: Epoch(manifest.epoch),
            },
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
    type Context<'a> = CollectingJob<'a>;
    type Error = StoreError;

    /// Every checkpoint the traversal reached is the checkpoint its own reference names,
    /// carries entries headed for that checkpoint, and agrees with the job — and every
    /// checkpoint the pass would delete is one the traversal reached.
    ///
    /// The last half is what makes the others worth anything: without it a plan could
    /// name a checkpoint for deletion that no manifest in the chain ever described, and the
    /// checks over the chain would have nothing to say about it.
    ///
    /// # Why the identity check is here and not only in the traversal
    ///
    /// The traversal already refuses a manifest another backend wrote, because the parent link
    /// and the file refs it interprets come out of those bytes. Review round 7 of PR #160 found
    /// the other half of that argument missing: the *generation and epoch* also come out of
    /// those bytes, and they are what [`crate::gc::delete_classified_history`] builds every
    /// object it removes from — the checkpoint manifest, the committed marker, the epoch
    /// record, the checkpoint directory. A misplaced or corrupt manifest whose selector still
    /// agreed therefore aimed the deletes. Each reached manifest is now required to be the
    /// checkpoint the reference it was read from names.
    ///
    /// # The difference this row must admit
    ///
    /// A history traversal spans a *range* of epochs and may span generations — collecting a
    /// range of older checkpoints is what it is for — so the rule cannot be "every manifest
    /// carries one identity". It is per object: each manifest is the checkpoint *its own*
    /// reference names.
    /// `cleanup_collects_a_history_spanning_generations_and_epochs` is the positive row for
    /// that, and it is deliberately a chain no single identity could describe.
    ///
    /// What this does *not* re-derive is which files belong to which checkpoint — that is
    /// the traversal's own job, and re-checking it would mean buffering the file lists the
    /// traversal exists to stream past.
    fn check_whole(&self, job: CollectingJob<'_>) -> Result<(), StoreError> {
        for checkpoint in &self.reached {
            let identity = identify_checkpoint_manifest(
                job.paths,
                &checkpoint.checkpoint_ref,
                &checkpoint.selectors,
            )?;
            validate_manifest_identity(&checkpoint.selectors, &identity)?;
            validate_restored_manifest(job.state_backend, &checkpoint.selectors)?;
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

/// Reduces a manifest to the identity fields, operator headers and table configs the identity
/// and selector checks read.
///
/// The four identity fields are kept because the checks are about *relationships* between them
/// and the reference the manifest was read from; dropping any one of them would leave a check
/// that cannot run. They cost four short strings and two integers per reached checkpoint, next
/// to the per-table file lists this exists to avoid buffering.
fn selector_evidence(manifest: &CheckpointManifest) -> CheckpointManifest {
    CheckpointManifest {
        pipeline_id: manifest.pipeline_id.clone(),
        job_id: manifest.job_id.clone(),
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
/// Returns [`StoreError::Protocol`] if any reachable manifest is not the checkpoint the
/// reference it was read from names, or if the plan would delete a checkpoint the traversal
/// never reached; [`StoreError::IncompleteManifest`] if an entry of one is headed for another
/// checkpoint; and [`StoreError::StateBackend`] if any reachable manifest disagrees with the
/// job's selector. In every case nothing has been deleted.
pub(crate) fn validate_history(
    history: CheckpointHistory,
    job: CollectingJob<'_>,
) -> Result<Validated<CheckpointHistory>, StoreError> {
    Validated::validate(history, job)
}

#[cfg(test)]
mod tests {
    use super::{GenerationPublication, PublishingJob};
    use crate::ProtocolPaths;
    use crate::store::StoreError;
    use crate::types::{Epoch, Generation, ProtocolError};
    use arroyo_rpc::grpc::rpc::{CheckpointManifest, OperatorCheckpointMetadata, OperatorMetadata};
    use arroyo_rpc::state_backend::StateBackendSelector;
    use arroyo_rpc::state_backend::validated::Validated;
    use arroyo_types::{JobId, PipelineId};
    use std::collections::HashSet;
    use std::time::SystemTime;

    /// The job that is publishing: pipeline `P`, job `J`, generation 4.
    fn paths() -> ProtocolPaths {
        ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"))
    }

    /// A recovery manifest claiming to belong to `pipeline_id`/`job_id`, at `generation` and
    /// `epoch`, describing operator `op` with a header that agrees with the manifest.
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
            operators: vec![headed(job_id, "op", epoch as u32)],
            ..Default::default()
        }
    }

    /// One manifest entry, headed exactly as stated.
    fn headed(job_id: &str, operator_id: &str, epoch: u32) -> OperatorCheckpointMetadata {
        OperatorCheckpointMetadata {
            operator_metadata: Some(OperatorMetadata {
                job_id: job_id.to_string(),
                operator_id: operator_id.to_string(),
                epoch,
                min_watermark: None,
                max_watermark: None,
                parallelism: 1,
            }),
            ..Default::default()
        }
    }

    /// A publication of generation 4 of pipeline `P`, job `J`, recovering from `manifest` read
    /// from the reference `at` names.
    fn publish_from(at: (u64, u64), manifest: CheckpointManifest) -> Result<(), StoreError> {
        let program: HashSet<&str> = HashSet::from(["op"]);
        Validated::validate(
            GenerationPublication::new(
                PipelineId::new("P"),
                JobId::new("J"),
                Generation(4),
                SystemTime::UNIX_EPOCH,
                Some(paths().checkpoint_manifest(Generation(at.0), Epoch(at.1))),
                Some(manifest),
            ),
            PublishingJob {
                state_backend: StateBackendSelector::Parquet,
                program_operators: &program,
            },
        )
        .map(|_| ())
    }

    /// The same, read from the reference the manifest's own generation and epoch name — the
    /// ordinary case, where the object is where it says it is.
    fn publish(manifest: CheckpointManifest) -> Result<(), StoreError> {
        publish_from((manifest.generation, manifest.epoch), manifest)
    }

    fn misplaced(err: &StoreError) -> bool {
        matches!(
            err,
            StoreError::Protocol(ProtocolError::CheckpointManifestMisplaced { .. })
        )
    }

    /// A generation may only be published against a recovery manifest that is the checkpoint
    /// the reference it was loaded from names — every one of the four identities it carries
    /// (PR #160 review round 7, finding 1).
    ///
    /// Round 6 compared the pipeline and the job and left the generation and the epoch out,
    /// with the reasoning that a recovery checkpoint is *always* from an earlier generation and
    /// an earlier epoch. That reasoning is right against the generation being **published** and
    /// wrong against the **reference**: those are different quantities, and the reference is
    /// where `resolve_generation_manifest` and leader GC rebuild their paths from. A misplaced
    /// or corrupt object whose selector and outer job still matched was therefore published as
    /// this generation's recovery point.
    ///
    /// The positive case comes first and is repeated at the end, because a rule of "the
    /// manifest names this generation and this epoch" would refuse every publication that has
    /// any history at all — recovering *means* reaching back past the current generation. Each
    /// negative varies exactly one identity against a reference that agrees in the other three.
    #[test]
    fn a_recovery_manifest_must_be_the_checkpoint_its_reference_names() {
        // Generation 4 is being published, and it recovers from generation 1 epoch 2, read
        // from generation 1 epoch 2's own reference. This is what recovery is.
        publish(recovery("P", "J", 1, 2))
            .expect("an earlier generation and epoch of this job is what recovery is");

        // Each identity varied on its own, from a manifest that agrees in the other three.
        let other_job = publish(recovery("P", "J2", 1, 2)).unwrap_err();
        assert!(misplaced(&other_job), "{other_job:?}");
        assert!(other_job.to_string().contains("job J2"), "{other_job}");

        let other_pipeline = publish(recovery("P2", "J", 1, 2)).unwrap_err();
        assert!(misplaced(&other_pipeline), "{other_pipeline:?}");

        // The two round 6 left out. Read from generation 1 epoch 2's reference, but claiming
        // to be a different checkpoint — which is what a misplaced or corrupt object is.
        let other_generation = publish_from((1, 2), recovery("P", "J", 3, 2)).unwrap_err();
        assert!(misplaced(&other_generation), "{other_generation:?}");
        assert!(
            other_generation.to_string().contains("generation 3"),
            "{other_generation}"
        );

        let other_epoch = publish_from((1, 2), recovery("P", "J", 1, 9)).unwrap_err();
        assert!(misplaced(&other_epoch), "{other_epoch:?}");
        assert!(other_epoch.to_string().contains("epoch 9"), "{other_epoch}");

        // ...and the difference that must *not* be refused, stated as a positive so the
        // exemption is pinned rather than merely absent: the generation being published is 4,
        // and a recovery checkpoint from generation 3 at a much older epoch is ordinary — as is
        // one from generation 0 at epoch 0.
        publish(recovery("P", "J", 3, 0))
            .expect("a recovery checkpoint is always from an earlier generation and epoch");
        publish(recovery("P", "J", 0, 0))
            .expect("the first generation's first checkpoint is a legitimate recovery point");
    }

    /// A recovery manifest whose entry is headed for another checkpoint is refused, even though
    /// the manifest itself is where it says it is (PR #160 review round 7, finding 2).
    ///
    /// The manifest, the reference and the operator set all agree here; only the entry's own
    /// `OperatorMetadata` header moves. That is the shape the finding describes — "the correct
    /// outer job and operator IDs while containing an `OperatorMetadata` header from another job
    /// or epoch" — and it is the one a check that read `operator_id` alone admitted. The job and
    /// the epoch are varied independently.
    #[test]
    fn a_recovery_manifest_entry_must_be_headed_for_the_manifests_own_checkpoint() {
        let mut wrong_job = recovery("P", "J", 1, 2);
        wrong_job.operators = vec![headed("J2", "op", 2)];
        let err = publish(wrong_job).unwrap_err();
        assert!(
            matches!(err, StoreError::IncompleteManifest(ref m) if m.detail.contains("job \"J2\"")),
            "{err:?}"
        );

        let mut wrong_epoch = recovery("P", "J", 1, 2);
        wrong_epoch.operators = vec![headed("J", "op", 5)];
        let err = publish(wrong_epoch).unwrap_err();
        assert!(
            matches!(err, StoreError::IncompleteManifest(ref m) if m.detail.contains("epoch 5")),
            "{err:?}"
        );

        // The entry that agrees is the one that publishes, and it is the same manifest in
        // every other respect.
        publish(recovery("P", "J", 1, 2))
            .expect("an entry headed for its own checkpoint is what a manifest is made of");
    }
}
