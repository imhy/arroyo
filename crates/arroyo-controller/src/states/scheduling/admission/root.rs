//! What the M11.D39b preamble publishes and roots (M11.T26g, design M11.D39c/M11.D39d).
//!
//! A child of [`super`] for the same reason [`super::execution`] and [`super::observation`] are:
//! it needs [`PhaseContext`]'s own fields — the job it is scheduling, the checkpoint the
//! recovery resolved, the topology it is running in, and the slot a lost duel records its
//! unrooted candidate in — and a sibling would have had to open those to the whole of
//! `states::scheduling`, including the phase graph whose entire claim is that it reaches the job
//! only through the methods a phase exposes.
//!
//! Every production scheduling attempt reaches this, since M11.T26h's activation change: a
//! [`PhaseContext`] exists only for a job whose lifecycle mechanism is M11.D39a's single writer,
//! and [`LifecycleMode::SELECTED`](crate::states::lifecycle::LifecycleMode::SELECTED) is
//! `FencedV2`. A job built in the pre-flag-day peer mode writes no candidate and installs no
//! root — see `only_the_fenced_preamble_publishes_a_candidate_and_roots_it`.

use anyhow::anyhow;
use arroyo_rpc::state_backend::validated::Validated;
use arroyo_state::{StorageProviderFor, get_storage_provider};

use super::PhaseContext;
use crate::AuthorityOutcome;
use crate::states::lifecycle::{GenerationRoot, RecoveryReference, RootCandidate, RootContext};
use crate::states::{Admission, StateError, fatal};

impl PhaseContext<'_, '_> {
    /// Publishes this generation's metadata as an immutable, fence-scoped candidate object,
    /// installs it as the job's authoritative root, conditionally, and only then makes this
    /// generation canonically current (M11.D39c/M11.D39d).
    ///
    /// Three steps, in this order, and the order is the guarantee:
    ///
    /// 1. the object is written under a name that embeds the whole identity — job, pipeline,
    ///    generation, fence, epoch — so nothing else can write it and nothing is overwritten;
    /// 2. the reference is installed by the conditional row update, which either matches this
    ///    controller's authority or matches nothing;
    /// 3. the canonical `current-generation.json` that
    ///    [`prepare_recovery_checkpoint`](super::PhaseContext::prepare_recovery_checkpoint)
    ///    deferred is written, if there is one.
    ///
    /// The third step is here rather than inside the registration because it is *authoritative*
    /// protocol state and not an unrooted candidate: `publish_checkpoint` refuses a checkpoint
    /// whose generation is not the current one and `resolve_generation_manifest` reads a
    /// candidate differently depending on whether its generation is current, so a controller
    /// that wrote it before step 2 would leave a generation it has just lost named as the
    /// job's current one — and losing the CAS undoes only the root (PR #167 round 6, finding
    /// 5). Everything the loser wrote before that point is a candidate the grace collector
    /// takes; this is the one object that would not have been.
    ///
    /// A controller that loses the duel between the two has published an object nobody points
    /// at. That is the intended outcome, not a failure to clean up: an **unrooted candidate**
    /// is what M11.D39d leaves for the grace collector, and it is recorded on the durable
    /// fencing obligation so the controller that holds the job knows it is out there. The same
    /// is true of a cancelled attempt, for the same reason — the object is complete or absent,
    /// and only one statement can root it.
    ///
    /// Runs after [`Self::prepare_recovery_checkpoint`] because the metadata it publishes
    /// includes what that step resolved, and before the preamble releases its admission because
    /// installing a root is an irreversible effect.
    ///
    /// # Errors
    ///
    /// A fatal reason when the metadata does not describe this attempt, when no candidate can
    /// be named for it, or when another controller holds the job; retryable when the store or
    /// the database could not be reached.
    pub(crate) async fn publish_metadata_root(&mut self, a: &Admission) -> Result<(), StateError> {
        let metadata = GenerationRoot::describing(
            self.ctx.pipeline_info.pipeline_id.to_string(),
            (*self.ctx.config.id).clone(),
            self.ctx.status.generation,
            self.ctx.execution_selector,
            self.recovery_reference(),
        );
        // The whole-object check (M11.D39c), against what the *job* says rather than against
        // the metadata's own fields. Nothing has been written at this point and nothing will be
        // unless the token exists.
        let validated = Validated::validate(
            metadata,
            RootContext {
                job_id: &self.ctx.config.id,
                pipeline_id: &self.ctx.pipeline_info.pipeline_id,
                generation: self.ctx.status.generation,
                execution_selector: self.ctx.execution_selector,
                leader_mode: self.leader_mode,
            },
        )
        .map_err(|refusal| {
            fatal(
                "this scheduling attempt's generation metadata does not describe it",
                anyhow!("{}", refusal),
            )
        })?;
        let candidate =
            RootCandidate::mint(self.ctx.status.authority(), &validated).map_err(|refusal| {
                fatal(
                    "no candidate metadata object can be named for this scheduling attempt",
                    anyhow!("{}", refusal),
                )
            })?;

        let storage = get_storage_provider(&StorageProviderFor::Controller {
            storage_url: self.ctx.pipeline_info.state_url.clone(),
        })
        .await
        .map_err(|e| {
            self.retryable(
                "failed to reach the job's state store",
                anyhow!("{}", e),
                20,
            )
        })?;
        if let Err(e) = a
            .effect(
                "publish the generation's candidate metadata object",
                candidate.publish(storage.as_ref()),
            )
            .await
        {
            return Err(self.retryable(
                "failed to publish the generation's candidate metadata object",
                anyhow!("{}", e),
                20,
            ));
        }

        let db = self.ctx.db.clone();
        let installed = a
            .effect(
                "install the generation's authoritative metadata root",
                self.ctx.status.install_metadata_root(&db, &candidate),
            )
            .await;
        match installed {
            Ok(Ok(AuthorityOutcome::Applied(()))) => {
                // Won. Only now is this generation the job's current one, and only now may the
                // pointer that says so be written.
                let Some(deferred) = self.deferred_current_generation.take() else {
                    return Ok(());
                };
                match a
                    .effect(
                        "publish the generation's canonical current-generation pointer",
                        deferred.publish(storage.as_ref()),
                    )
                    .await
                {
                    Ok(()) => Ok(()),
                    // Retryable, and the root stays installed: this controller holds the job,
                    // and a later attempt of its own re-registers the generation and writes the
                    // pointer. Nothing has been started against a generation that is not yet
                    // current — the fan-out is past the end of this region.
                    Err(e) => Err(self.retryable(
                        "failed to publish the generation's canonical current-generation pointer",
                        anyhow!("{}", e),
                        20,
                    )),
                }
            }
            // The duel, lost between publishing and rooting. The candidate stays exactly where
            // it was written, unrooted, and is recorded so the fencing obligation names it.
            Ok(Ok(AuthorityOutcome::Stale(stale))) => {
                self.unrooted_candidate = Some(candidate.key());
                Err(self.stand_down_from(stale))
            }
            Ok(Err(e)) => Err(self.retryable(
                "failed to install the generation's authoritative metadata root",
                anyhow!("{}", e),
                20,
            )),
            Err(refusal) => Err(fatal(
                "this generation's candidate cannot be rooted under this controller's authority",
                anyhow!("{}", refusal),
            )),
        }
    }

    /// How this attempt names the checkpoint its generation restores from, if it restores from
    /// one.
    ///
    /// The two topologies name it differently — `CheckpointInfo::id` is an object-store
    /// reference for a worker-leader execution and a `checkpoints` row id for a controller-mode
    /// one — and the difference is stated here, once, from the same `leader_mode` the rest of
    /// the phase reads.
    fn recovery_reference(&self) -> Option<RecoveryReference> {
        let info = self.checkpoint_info.as_ref()?;
        Some(match self.leader_mode {
            true => RecoveryReference::LeaderObject(info.id.clone()),
            false => RecoveryReference::ControllerCheckpointRow(info.id.clone()),
        })
    }
}
