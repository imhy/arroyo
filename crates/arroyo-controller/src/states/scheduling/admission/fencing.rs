//! The preamble's recovery step: discharging an obligation an earlier attempt left durably
//! (M11.T26f, design M11.D39d/M11.D39g).
//!
//! A **child** of [`super`] rather than a sibling, for the reason the other children are: it
//! needs [`PhaseContext`]'s own fields — the job, its database handle, its scheduler — and a
//! sibling would have had to be handed them, which would open them to the whole of
//! `states::scheduling`.
//!
//! # Where it sits, and why exactly there
//!
//! Between [`adopt_lifecycle_authority`](PhaseContext::adopt_lifecycle_authority) and
//! [`persist_generation`](PhaseContext::persist_generation), and neither neighbour is
//! negotiable:
//!
//! * **After adoption**, because discharging the obligation means advancing *this controller's*
//!   fence at the recorded targets, and a controller that has not adopted the job holds no fence
//!   to advance. Adoption is also what makes the recovered obligation's bound derivable: it
//!   stores `lifecycle_fence + 1` over what it read, so the fence it installed is one above
//!   anything a previous attempt could have issued under.
//! * **Before the generation is persisted**, because persisting it is M11.D39d's *"admission of
//!   a replacement generation"* — the very effect that may not precede settlement. Everything
//!   the preamble does after this point is an effect on a job whose old worker generations have
//!   answered.
//!
//! # What an unsettled obligation does to the attempt
//!
//! It ends it, retryably, with no worker touched and no generation persisted. The job's state
//! task goes `Scheduling → Recovering → Scheduling` on the landed backoff and runs this pass
//! again, which is safe because the pass is idempotent; and while a target stays unobservable it
//! never gets past this line. That is M11.D39g's declared liveness result — *"`Fencing` remains
//! pending while other jobs progress"* — expressed in this state machine's own vocabulary.
//!
//! It is **retryable and not fatal** deliberately. Failing the job would publish this
//! controller's opinion about a job whose previous generation may still be applying a
//! `StartExecution`, which is the publication the fence exists to hold back. And nothing here
//! counts passes towards giving up: the retry budget decides how long the *state machine* waits
//! between attempts, never whether the obligation is discharged.

use std::sync::Arc;

use anyhow::anyhow;
use arroyo_types::WorkerId;
use tracing::warn;

use super::PhaseContext;
use crate::states::lifecycle::recovery::observe_terminations;
use crate::states::lifecycle::{Discharge, DischargeReason, discharge_recorded_obligation};
use crate::states::{Admission, StateError};

impl PhaseContext<'_, '_> {
    /// Discharges whatever fencing obligation this job's row carries, before this attempt
    /// admits a replacement generation.
    ///
    /// Inside the admitted region because advancing a worker generation's fence is irreversible:
    /// the generation is in strict mode afterwards and refuses everything older, which is
    /// exactly the effect a refusal published concurrently must not race. It is the same
    /// argument [`address_every_worker`](super::super::fanout) makes for the handshake it runs.
    ///
    /// # Errors
    ///
    /// Retryable when the obligation is still unsettled, or when this pass could not be
    /// completed at all — both leave the durable record exactly as it was, so the next attempt
    /// repeats this one. Losing the job's authority is recorded as a stand-down rather than
    /// reported as a failure, exactly as adoption's own loss is.
    pub(crate) async fn discharge_recovered_fencing(
        &mut self,
        a: &Admission,
    ) -> Result<(), StateError> {
        let db = self.ctx.db.clone();
        let scheduler = Arc::clone(&self.ctx.scheduler);
        let mode = self.ctx.lifecycle_mode();
        let discharge = a
            .effect(
                "discharge the job's recovered durable fencing obligation",
                discharge_recorded_obligation(
                    self.ctx.status,
                    &db,
                    &scheduler,
                    mode,
                    // A preamble exists to admit a **replacement** generation, so every target
                    // the record still names is asked again under the fence this attempt has
                    // just adopted. Reading the acknowledgements an *earlier* and lower fence
                    // left behind would let the preamble persist a new generation and tear the
                    // old cluster down while its workers still admit their old owner's
                    // directives (PR #167 round 6, finding 1).
                    DischargeReason::SupersedingTheGenerationsItNames,
                ),
            )
            .await;
        match discharge {
            // Nothing recorded, nothing recovered, or a mechanism that records none: the
            // attempt continues exactly as it would have before this step existed.
            Discharge::Inactive | Discharge::NothingRecorded | Discharge::Settled => Ok(()),
            Discharge::Superseded(stale) => Err(self.stand_down_from(stale)),
            Discharge::StillPending {
                pending,
                outstanding_attempts,
            } => Err(self.retryable(
                "this job still owes a worker generation an acknowledged fence",
                anyhow!(
                    "{pending} target worker generation(s) and {outstanding_attempts} issued \
                     StartExecution identifier(s) of an earlier scheduling attempt have neither \
                     acknowledged a superseding lifecycle fence nor been observed terminated; \
                     this job stays in token-free fencing and admits no replacement generation \
                     until one of those is observed"
                ),
                10,
            )),
            Discharge::Unusable(failure) => Err(self.retryable(
                "this job's recovered fencing obligation could not be discharged",
                anyhow!("{failure}"),
                10,
            )),
        }
    }
}

impl PhaseContext<'_, '_> {
    /// Asks the scheduler which of *this* attempt's own worker generations are already gone,
    /// and records what it says (M11.T26f, M11.D39e(v)).
    ///
    /// The second of the two real observation sources, and the one that answers a generation
    /// which never acknowledged anything. It is asked once, as the attempt ends, because that
    /// is the moment its obligation is about to become durable: a target the scheduler has
    /// already reclaimed is a target the record should name as terminated rather than as one a
    /// later controller must go and fence.
    ///
    /// Nothing here settles anything by itself. It fills the inbox
    /// [`observed_generation_terminations`](PhaseContext::observed_generation_terminations)
    /// drains, and the reconciliation applies each observation against the target set — where a
    /// termination naming another generation accounts for nothing. A scheduler that cannot
    /// answer, or that does not track its worker generations at all, leaves the inbox empty and
    /// every target as pending as it was: **not knowing is not knowing they are gone**, and this
    /// is the one place in the fencing path where those could be confused.
    pub(crate) async fn observe_generation_teardown(&mut self) {
        let addressed: Vec<WorkerId> = self.workers().keys().copied().collect();
        if addressed.is_empty() {
            return;
        }
        let job_id = self.job().config.id.clone();
        let generation = self.addressed_generation();
        let scheduler = Arc::clone(&self.job().scheduler);
        match observe_terminations(&scheduler, &job_id, generation, &addressed).await {
            Ok(terminations) => self.record_observed_terminations(terminations),
            Err(e) => warn!(
                job_id = %job_id,
                generation,
                error = %e,
                "this controller cannot observe whether this attempt's worker generations have \
                 terminated, so every target it addressed stays pending"
            ),
        }
    }
}
