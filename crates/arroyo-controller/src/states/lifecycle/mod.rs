//! The D39 single-writer job lifecycle (M11.T25a/M11.T25f).
//!
//! Two mechanisms are named by [`LifecycleMode`], and [`LifecycleMode::SELECTED`] says which
//! one a build runs. Since M11.T26h's activation change that is M11.D39a's: a per-job
//! [`IntentMailbox`] the configuration poll writes, and a [`LifecycleActor`] in the job's own
//! state task that is the only thing which decides and publishes. M11.T08's cross-task refusal
//! gate — its admission mutex and its per-task `acted` watermark — was removed in the same
//! change, because a superseded mechanism that is still compiled is a second thing that could
//! decide a job.
//!
//! [`waiting`] holds D39a's second consumption point: the one wait a job's state task parks
//! on, which reads the writer beside the job's message channel so that a stop nobody sent a
//! message for still ends the wait.
//!
//! [`leaving`] holds the other half of a decision: what the state that runs next does about a
//! stop the writer decided while the previous one was running. The boundary consumes such an
//! intent, so the state can no longer observe it for itself, and routing it is what keeps a
//! consumed stop from being a lost one. A state that answers "not here" does not thereby
//! discard it: the stop is left standing on the job's writer and re-offered at that state's own
//! consumption points, which are [`waiting`]'s.
//!
//! [`fence`] holds M11.T26's durable half: the `job_statuses.lifecycle_fence` /
//! `controller_epoch` authority a controller must hold to write a job's row, the adoption
//! that installs it, and the outcome taxonomy that makes losing it an answer rather than a
//! silent success. Every production status write presents one since M11.T26h's activation
//! change, which removed the unconditional write in the same edit that selected the fence.
//!
//! [`protocol`] and [`handshake`] hold M11.T26c's controller half of the worker protocol: which
//! directive this controller's start and commit requests carry, how a gRPC status about an
//! issued attempt is classified as definitive or ambiguous, and the active advance that makes a
//! worker generation acknowledge this controller's fence before it is asked to start anything.
//! Both answer `Legacy` — the shape a controller predating the fields sends — for a job built in
//! the pre-flag-day peer mode [`LifecycleMode::LegacyT08`], and `Fenced` for every production
//! job.
//!
//! [`publication`] is the one place a job's lifecycle status reaches its row, and [`root`] is
//! the candidate-then-conditional-root protocol M11.D39d makes generation metadata publish
//! through. Since M11.T26h [`publication`] has one write form — the conditional one — and every
//! production generation publishes a fence-scoped candidate before its root becomes
//! authoritative.
//!
//! [`recovery`] holds M11.T26f's other half: reading that obligation back after the controller
//! that owed it is gone, re-adopting the job conditionally, actively advancing this controller's
//! fence at every recorded target it can reach, observing the terminations of those it cannot,
//! and writing the advanced obligation back through the same funnel. A target that answers
//! neither leaves the job in `Fencing` — for as long as it takes, with no timeout and no value
//! in that module able to express one.
//!
//! [`settlement`] holds M11.T26e's cancellation-resistant per-job settlement owner: the party an
//! interrupted fan-out hands its whole obligation to when the phase that raised it cannot settle
//! it, and the one that decides — from the identifiers it is holding, never from a timeout or a
//! dropped future — when the job's lifecycle authority may be released. A job has one exactly
//! when it has the [`LifecycleActor`] above, because [`JobLifecycle`] builds both or neither.
//!
//! [`classification`] holds the rules that run before either of them: a job's selector is
//! fixed at its first execution, a row that disagrees with it earns a typed refusal, and a
//! durable record that will not decode skips the job rather than being defaulted (M11.D39f).
//! Those are decisions about a *value*, so both mechanisms above reach the same ones.
//!
//! **This path carries a rollout constraint**, and it is one-way: the durable fence, the worker
//! acknowledgement protocol and the flag day are M11.T26's, and M11.T26h selected them and
//! removed the M11.T08 guards they supersede in one change. Worker images go first; after any
//! worker generation enters strict mode a controller rollback is only to a fence-capable build
//! or through a coordinated stop. See the rollout section of [`LifecycleMode`], and
//! `docs/lifecycle-fence-rollout.md` for the whole of it.

pub(crate) mod actor;
pub(crate) mod classification;
pub(crate) mod fence;
pub(crate) mod handshake;
pub(crate) mod intent;
pub(crate) mod leaving;
pub(crate) mod mode;
pub(crate) mod protocol;
pub(crate) mod publication;
pub(crate) mod recovery;
pub(crate) mod root;
pub(crate) mod settlement;
pub(crate) mod waiting;

#[cfg(test)]
mod fault_model_tests;
/// M11.D39g's declared fault model, as named reusable injections (M11.T26g).
#[cfg(test)]
mod faults;
#[cfg(test)]
mod faults_tests;
#[cfg(test)]
pub(super) mod fence_tests;
#[cfg(test)]
mod handshake_tests;
#[cfg(test)]
mod protocol_tests;
#[cfg(test)]
mod publication_tests;
#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod root_tests;
#[cfg(test)]
mod settlement_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod wiring_tests;

use std::sync::Arc;

use arroyo_rpc::state_backend::StateBackendSelector;

pub(crate) use actor::{ConsumptionPoint, LifecycleActor, ObservedIntent};
pub(crate) use handshake::{StartTargets, advance_fence};
pub(crate) use intent::{IntentMailbox, IntentWakeup, LifecycleIntent};
pub(crate) use mode::LifecycleMode;
pub(crate) use protocol::{FenceProtocol, TransportSettlement, UnfencedAuthority};
pub(crate) use publication::{StatusPublication, publish_status, stand_down};
pub(crate) use recovery::{Discharge, discharge_recorded_obligation};
pub(crate) use root::{GenerationRoot, RecoveryReference, RootCandidate, RootContext};
pub(crate) use settlement::JobSettlementOwner;
pub(crate) use waiting::{JobWait, Waited};

/// One job's lifecycle mechanism, as its state machine holds it.
///
/// This is the seam M11.T26 flips, and it is deliberately a *per job* value rather than a
/// process-wide switch consulted at each decision point: a job's mechanism is fixed when
/// its state machine is created, so a job cannot change hands halfway through its own
/// lifecycle. It is built from [`LifecycleMode::SELECTED`] on every production path, which has
/// made [`Self::FencedV2`] the only production result since M11.T26h — see
/// `every_production_path_selects_the_fenced_v2_lifecycle`, which pins that there is exactly one
/// such construction site and what it passes.
pub(crate) enum JobLifecycle {
    /// M11.T08's cross-task mechanism, retained as the pre-flag-day peer. There is no intent
    /// mailbox and no actor: the configuration-update thread decides and publishes into the
    /// job's queue. No production job is built in this mode since M11.T26h.
    LegacyT08,
    /// M11.D39a's single-writer mechanism, holding the job's one intent slot and the job's
    /// cancellation-resistant settlement owner.
    ///
    /// The two are one variant because they are one decision. A job whose transitions the D39a
    /// writer decides and whose interrupted fan-outs have nobody to hand their obligation to
    /// would be half of the mechanism, and so would the reverse; making them fields of one arm
    /// is what stops either from being wired without the other. See
    /// `the_fenced_mechanism_supplies_a_writer_and_a_settlement_owner_together`.
    FencedV2 {
        /// The job's one intent slot, written by the configuration poll.
        intents: Arc<IntentMailbox>,
        /// The job's settlement owner, per job rather than per state task: a task that is
        /// restarted must not get a fresh owner that has forgotten what the previous one was
        /// answerable for.
        settlement: Arc<settlement::JobSettlementOwner>,
    },
}

impl JobLifecycle {
    /// The lifecycle mechanism for one job.
    ///
    /// Takes the mode rather than reading [`LifecycleMode::SELECTED`] itself so that the
    /// selection has exactly one production site to audit, and so that the D39a path can be
    /// constructed directly by the tests that exercise it. Production passes
    /// `LifecycleMode::SELECTED`; nothing else in the crate outside a test module passes
    /// anything at all.
    pub(crate) fn for_mode(mode: LifecycleMode, job_id: Arc<String>) -> Self {
        match mode {
            LifecycleMode::LegacyT08 => JobLifecycle::LegacyT08,
            LifecycleMode::FencedV2 => JobLifecycle::FencedV2 {
                intents: Arc::new(IntentMailbox::new(Arc::clone(&job_id))),
                settlement: settlement::JobSettlementOwner::for_job(job_id),
            },
        }
    }

    /// The mechanism this job runs under.
    ///
    /// One derivation from the variant, so a caller outside a [`JobContext`] — the state-machine
    /// boundary, which has no context yet — reads the same fact
    /// [`JobContext::lifecycle_mode`](crate::states::JobContext::lifecycle_mode) reads rather
    /// than a second copy of `LifecycleMode::SELECTED`.
    pub(crate) fn mode(&self) -> LifecycleMode {
        LifecycleMode::of_job(matches!(self, JobLifecycle::FencedV2 { .. }))
    }

    /// The job's intent slot, or `None` when the configuration poll is itself the decider.
    ///
    /// `Some` is the whole test the configuration-update thread makes: it either has
    /// somewhere to leave a classified intent, in which case that is all it does, or it
    /// does not, in which case the M11.T08 path runs unchanged.
    pub(crate) fn intents(&self) -> Option<&Arc<IntentMailbox>> {
        match self {
            JobLifecycle::LegacyT08 => None,
            JobLifecycle::FencedV2 { intents, .. } => Some(intents),
        }
    }

    /// The job's settlement owner, or `None` under [`LifecycleMode::LegacyT08`].
    ///
    /// `Some` for every production job since M11.T26h: `PhaseContext::settlement_owner` answers
    /// with this, so an interrupted fan-out has a party to hand its whole obligation to. `None`
    /// only in the pre-flag-day peer mode, where such a fan-out settles in place.
    ///
    /// Cloned rather than borrowed for the reason the owner exists: it has to be reachable from
    /// the region rescue that runs when the job's state task, and everything borrowed from it,
    /// is already gone.
    pub(crate) fn settlement(&self) -> Option<Arc<settlement::JobSettlementOwner>> {
        match self {
            JobLifecycle::LegacyT08 => None,
            JobLifecycle::FencedV2 { settlement, .. } => Some(Arc::clone(settlement)),
        }
    }

    /// The actor for a state task that is starting, or `None` under
    /// [`LifecycleMode::LegacyT08`].
    ///
    /// One actor per task, not per job: the watermark of what has already been decided
    /// belongs to the task that decided it, so a job whose task is restarted re-decides
    /// whatever the poll still stands behind. This is the same rule the T08 gate follows
    /// with its per-task `acted` counter, for the same reason.
    pub(crate) fn actor(
        &self,
        job_id: Arc<String>,
        execution_selector: StateBackendSelector,
    ) -> Option<LifecycleActor> {
        let mailbox = Arc::clone(self.intents()?);
        Some(LifecycleActor::new(job_id, execution_selector, mailbox))
    }
}
