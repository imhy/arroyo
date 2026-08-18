//! The D39 single-writer job lifecycle (M11.T25a/M11.T25f).
//!
//! Two mechanisms decide a job's lifecycle transitions in this crate, and
//! [`LifecycleMode::SELECTED`] says which one a build runs. M11.T08's landed mechanism —
//! the cross-task [`RefusalGate`](crate::states::RefusalGate), its admission mutex and its
//! per-task `acted` watermark — is the selected one for the whole of M11.T25 and is not
//! changed by anything in this module. What is built here is the alternative M11.D39a
//! specifies: a per-job [`IntentMailbox`] the configuration poll writes, and a
//! [`LifecycleActor`] in the job's own state task that is the only thing which decides and
//! publishes.
//!
//! [`classification`] holds the rules that run before either of them: a job's selector is
//! fixed at its first execution, a row that disagrees with it earns a typed refusal, and a
//! durable record that will not decode skips the job rather than being defaulted (M11.D39f).
//! Those are decisions about a *value*, so both mechanisms above reach the same ones.
//!
//! Nothing here adds a wire field, a schema change, or a rollout constraint. The only
//! ordering requirement a deployment of M11.T25 is under is the worker-first one M11.T08
//! already landed, and the durable fence, the worker acknowledgement protocol, the flag day
//! and the removal of the T08 guards this path supersedes are all M11.T26's. See the rollout
//! section of [`LifecycleMode`] for the whole of it.

pub(crate) mod actor;
pub(crate) mod classification;
pub(crate) mod intent;
pub(crate) mod mode;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arroyo_rpc::state_backend::StateBackendSelector;

pub(crate) use actor::{ConsumptionPoint, LifecycleActor, ObservedIntent};
pub(crate) use intent::{IntentMailbox, IntentWakeup, LifecycleIntent};
pub(crate) use mode::LifecycleMode;

/// One job's lifecycle mechanism, as its state machine holds it.
///
/// This is the seam M11.T26 flips, and it is deliberately a *per job* value rather than a
/// process-wide switch consulted at each decision point: a job's mechanism is fixed when
/// its state machine is created, so a job cannot change hands halfway through its own
/// lifecycle. It is built from [`LifecycleMode::SELECTED`] on every production path, which
/// makes [`Self::LegacyT08`] the only production result — see
/// `no_production_path_selects_the_fenced_v2_lifecycle`, which pins that there is exactly
/// one such construction site and what it passes.
pub(crate) enum JobLifecycle {
    /// M11.T08's cross-task mechanism. There is no intent mailbox and no actor: the
    /// configuration-update thread decides and publishes through the
    /// [`RefusalGate`](crate::states::RefusalGate), exactly as it has since T08 landed.
    LegacyT08,
    /// M11.D39a's single-writer mechanism, holding the job's one intent slot.
    FencedV2(Arc<IntentMailbox>),
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
            LifecycleMode::FencedV2 => JobLifecycle::FencedV2(Arc::new(IntentMailbox::new(job_id))),
        }
    }

    /// The job's intent slot, or `None` when the configuration poll is itself the decider.
    ///
    /// `Some` is the whole test the configuration-update thread makes: it either has
    /// somewhere to leave a classified intent, in which case that is all it does, or it
    /// does not, in which case the M11.T08 path runs unchanged.
    pub(crate) fn intents(&self) -> Option<&Arc<IntentMailbox>> {
        match self {
            JobLifecycle::LegacyT08 => None,
            JobLifecycle::FencedV2(mailbox) => Some(mailbox),
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
