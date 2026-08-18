//! The single-writer lifecycle actor (M11.T25a, design M11.D39a).
//!
//! One job, one decider. The actor lives in the job's own state task, reads the
//! [`IntentMailbox`] the configuration poll writes, and is the only thing that turns an
//! intent into a lifecycle transition.

use std::sync::Arc;

use arroyo_rpc::state_backend::{
    StateBackendError, StateBackendSelector, validate_unchanged_job_selector,
};
use tracing::{error, info};

use super::intent::{IntentMailbox, IntentVersion, IntentWakeup, LifecycleIntent};
use crate::JobConfig;
use crate::states::{JobContext, StateError, fatal};
use crate::types::public::StopMode;

/// A point at which the job's single writer reads its intent mailbox.
///
/// The two points are what makes D39a's linearizability claim structural rather than
/// hopeful, and they are chosen for the two ways a decision can be missed:
///
/// * an intent published while the task was doing something it cannot undo would be read
///   only *after* that work, so it is read again immediately before every stretch of
///   irreversible work instead; and
/// * an intent published while the task was waiting on its own message channel can be
///   sitting behind the messages that ended the wait — a wait ends on the message that
///   made its count, not on the last message in the queue — so it is read again on every
///   turn of every wait rather than being left to whatever the channel happens to deliver.
///
/// The variant is carried only for the log: what is read, and what is decided, are the
/// same at both points. Naming them is what lets a reader of the log say which of the two
/// windows a decision closed.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConsumptionPoint {
    /// Immediately before a stretch of work the controller cannot take back.
    BeforeIrreversiblePhase,
    /// On a turn of a wait that does nothing but receive.
    InsideInterruptibleWait,
}

impl ConsumptionPoint {
    /// The name this point appears under in the job's log.
    fn as_str(self) -> &'static str {
        match self {
            ConsumptionPoint::BeforeIrreversiblePhase => "before an irreversible phase",
            ConsumptionPoint::InsideInterruptibleWait => "inside an interruptible wait",
        }
    }
}

/// What observing the job's single writer leaves its caller to do.
///
/// # Why observation has three outcomes and not two
///
/// A decision is *published* by [`LifecycleDecision::apply`] and *acted on* by the caller,
/// and the two are different steps because only the caller knows what leaving looks like from
/// where it stands: `Scheduling` leaves for [`Stopping`](crate::states::stopping::Stopping),
/// `Running` leaves for a checkpoint stop or an immediate one, a leader-mode state leaves for
/// [`LeaderStopping`](crate::states::leader_stopping::LeaderStopping).
///
/// Publishing alone is not enough. A stop written into the job's configuration and then
/// reported as `Ok(())` reads, at every call site, as permission to continue — and the phases
/// that follow a consumption point are the fan-out that starts an execution and the
/// publication of a restored checkpoint's commits, neither of which can be taken back. So a
/// consumed stop is returned as a value the caller cannot spend without answering it, exactly
/// as a consumed refusal is returned as an `Err` the caller cannot `?` past.
///
/// # The rule, in one place
///
/// [`Self::Stop`] means "the job's configuration now asks it to stop" — literally
/// `stop_mode != StopMode::none`, which is the same field, read by the same rule, that every
/// `stop_if_desired*` macro reads. The caller then invokes its own macro rather than
/// reimplementing the mapping from stop mode to state, so a stop that arrives through the
/// mailbox and a stop that arrives as a `ConfigUpdate` cannot come to mean different things.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum ObservedIntent {
    /// Nothing was decided, or what was decided leaves the job doing what it was doing.
    ///
    /// This is the *only* outcome under
    /// [`LifecycleMode::LegacyT08`](super::LifecycleMode::LegacyT08), because a job on the
    /// landed mechanism has no actor to decide anything — which is what makes every guard
    /// built on [`Self::Stop`] unreachable in production and the selected path unchanged.
    Continue,
    /// The job's writer decided it stops, and has published the stop into the job's
    /// configuration. The caller must leave for its own stop state before its next
    /// irreversible effect.
    Stop,
}

impl ObservedIntent {
    /// What the job's configuration now says, as an outcome.
    ///
    /// Derived from the published configuration rather than from which decision produced it,
    /// because a stop can arrive either way: [`LifecycleDecision::StopUnderRunningConfig`] is
    /// a refused row keeping its stop, and [`LifecycleDecision::Adopt`] is an accepted row
    /// that may itself be asking the job to stop. Reading the result closes both.
    fn of(config: &JobConfig) -> Self {
        match config.stop_mode {
            StopMode::none => ObservedIntent::Continue,
            _ => ObservedIntent::Stop,
        }
    }

    /// Whether the caller must leave for its stop state.
    pub(crate) fn stops(self) -> bool {
        matches!(self, ObservedIntent::Stop)
    }
}

/// What the job's single writer has decided to do about the poll's latest intent.
///
/// A decision is separated from its application so that the decision itself is a value a
/// test can look at, and so that the only code that mutates a job's baseline is
/// [`Self::apply`] — one function, at the points [`LifecycleActor::observe`] is called
/// from, rather than one per caller.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum LifecycleDecision {
    /// Run under this configuration from now on.
    Adopt(JobConfig),
    /// Stop the job, under the configuration it is already running with.
    ///
    /// Carries a stop mode and nothing else, which is what makes "a refused row keeps its
    /// stop and loses everything else" a property of the type rather than of the care
    /// taken at each call site.
    StopUnderRunningConfig(StopMode),
    /// Fail the job: its persisted configuration was refused and nothing about it is
    /// adopted.
    Refuse(StateBackendError),
}

impl LifecycleDecision {
    /// Publishes the decision into the job's running context.
    ///
    /// This is the *only* place the D39a path replaces a job's configuration, and it runs
    /// on the job's own state task. Nothing on the configuration-update thread writes a
    /// baseline or a lifecycle status under [`LifecycleMode::FencedV2`](super::LifecycleMode::FencedV2),
    /// which is the whole of "decision and delivery have one owner".
    ///
    /// Returning [`ObservedIntent`] rather than `()` is the other half of publication: what is
    /// written here has to be *acted on* by whatever called the consumption point, and a stop
    /// reported as an unremarkable success is a stop every caller reads as permission to carry
    /// on into the next irreversible phase.
    ///
    /// # Errors
    ///
    /// Returns a fatal [`StateError`] for [`Self::Refuse`], carrying the typed
    /// [`StateBackendError`] the classification produced. The caller is a state boundary or
    /// an interruptible wait, so the job fails from whatever state it is in — exactly as
    /// the M11.T08 path fails it through
    /// [`handle_unhandled_message`](crate::states::handle_unhandled_message).
    pub(crate) fn apply(self, ctx: &mut JobContext<'_>) -> Result<ObservedIntent, StateError> {
        match self {
            LifecycleDecision::Adopt(config) => {
                ctx.config = config;
                // The adopted row may itself be the row that asks the job to stop, which is
                // the ordinary way an operator stops a job whose configuration is fine. So
                // the answer is read off what was just published, not off which arm reached
                // it.
                Ok(ObservedIntent::of(&ctx.config))
            }
            LifecycleDecision::StopUnderRunningConfig(stop_mode) => {
                // Only the stop mode. The configuration this is written onto is the one the
                // job's workers, table configs and checkpoints were built from, so the
                // refused selector and the refused restart nonce are in neither.
                ctx.config.stop_mode = stop_mode;
                Ok(ObservedIntent::of(&ctx.config))
            }
            LifecycleDecision::Refuse(error) => {
                error!(
                    job_id = %ctx.config.id,
                    pipeline_id = *ctx.pipeline_info.pipeline_id,
                    error = %error,
                    "failing job whose persisted configuration was refused"
                );
                Err(fatal(
                    "the job's persisted configuration was refused",
                    error.into(),
                ))
            }
        }
    }
}

/// The one component that decides and publishes a job's lifecycle transitions.
///
/// # What makes it the single writer
///
/// Under [`LifecycleMode::FencedV2`](super::LifecycleMode::FencedV2) the configuration-
/// update thread's entire contribution is
/// [`IntentMailbox::submit`]: it validates, classifies, and leaves a value. It replaces no
/// baseline, writes no status, sends no message, and publishes no refusal. Everything that
/// *decides* is here, and this lives in the job's own state task — so there is no second
/// task to linearize against, and therefore no cross-task gate, admission mutex or `acted`
/// counter of the kind M11.T08 needed. Publication-versus-entry linearizability holds
/// because there is only ever one publisher, and it is the same task that enters the
/// phases.
///
/// M11.T08's [`RefusalGate`](crate::states::RefusalGate) remains the selected production
/// mechanism through M11.T25 and is not weakened by anything here; the two are
/// alternatives, and which one a job runs under is [`LifecycleMode::SELECTED`](super::LifecycleMode::SELECTED).
///
/// # Deciding each intent once
///
/// [`Self::decided`] is the watermark that does for this path what `RefusalGate::acted`
/// does for the T08 one. A row that stays bad is re-polled every 500ms and re-submitted at
/// the *same* version, so the actor decides it once; a row that changes gets a new version
/// and is decided afresh. The watermark is per actor, so a task started later re-decides
/// an intent an earlier task already did — which is what keeps a job restarting out of
/// `Failed` from restarting into a configuration that is still refused.
///
/// # What is deliberately not claimed
///
/// The interval between an intent being submitted and being observed is bounded by the
/// longest stretch of work that consults neither consumption point. It is *not* a claim
/// about final settlement: what a worker does with a request already in flight is M11.T26's
/// durable fence and worker acknowledgement protocol (D39d/D39e), and no part of this type
/// asserts anything about it.
pub(crate) struct LifecycleActor {
    /// The job whose transitions this actor decides, for the log.
    job_id: Arc<String>,
    /// The state backend this execution of the job runs with — the job's authority, fixed
    /// when the execution began and never reassigned (M11.D39f).
    ///
    /// Held so that adoption re-checks it. The poll validates before it enqueues, and the
    /// row it enqueues carries this same value, so a disagreement found here means one of
    /// those has stopped being true; the answer is to refuse, never to adopt.
    execution_selector: StateBackendSelector,
    /// The job's single intent slot, shared with the configuration-update thread.
    mailbox: Arc<IntentMailbox>,
    /// The newest version this actor has already decided on.
    decided: IntentVersion,
}

impl LifecycleActor {
    /// The actor for a state task that is starting.
    ///
    /// Constructed only through [`JobLifecycle::actor`](super::JobLifecycle::actor), whose
    /// `LegacyT08` arm returns `None` — so the existence of an actor *is* the fact that the
    /// job runs the D39a path.
    pub(crate) fn new(
        job_id: Arc<String>,
        execution_selector: StateBackendSelector,
        mailbox: Arc<IntentMailbox>,
    ) -> Self {
        Self {
            job_id,
            execution_selector,
            // A task that has just come up has decided nothing, so it observes whatever the
            // poll left — including an intent submitted before this task existed, which is
            // the case a job whose program could not be loaded spends its retries in.
            decided: IntentVersion::NONE,
            mailbox,
        }
    }

    /// What an interruptible wait parks on so that a submission ends it.
    ///
    /// A handle rather than a borrow, because the waits select on it beside a mutable borrow
    /// of the job's own context. See [`IntentWakeup`].
    pub(crate) fn wakeup(&self) -> IntentWakeup {
        IntentWakeup::watching(Arc::clone(&self.mailbox))
    }

    /// Reads the job's intent mailbox and decides, if there is anything new to decide.
    ///
    /// Returns `None` both when the poll has said nothing and when what it has said has
    /// already been decided on, so calling this on every turn of a wait costs a lock and a
    /// comparison rather than a decision.
    pub(crate) fn observe(&mut self, at: ConsumptionPoint) -> Option<LifecycleDecision> {
        let intent = self.mailbox.newer_than(self.decided)?;
        self.decided = intent.version();

        let decision = self.decide(intent.into_intent());
        info!(
            job_id = %self.job_id,
            version = self.decided.as_u64(),
            at = at.as_str(),
            decision = ?decision,
            "the job's lifecycle actor decided on a configuration intent"
        );
        Some(decision)
    }

    /// Turns one classified intent into the transition this job is to make.
    fn decide(&self, intent: LifecycleIntent) -> LifecycleDecision {
        match intent {
            LifecycleIntent::Adopt(config) => {
                // Fail closed. `adopt_refreshed_config` makes the same check on the
                // M11.T08 path for the same reason: keeping the configuration this
                // execution was validated with is safe, and adopting one that disagrees is
                // not, so a disagreement can only ever become a refusal.
                match validate_unchanged_job_selector(
                    &config.id,
                    self.execution_selector,
                    config.state_backend,
                ) {
                    Ok(()) => LifecycleDecision::Adopt(config),
                    Err(error) => LifecycleDecision::Refuse(error),
                }
            }
            LifecycleIntent::RefusedButStopping { error, stop_mode } => {
                error!(
                    job_id = %self.job_id,
                    error = %error,
                    "refusing the job's persisted state backend; executing the stop the same \
                     row requests, under the state backend the job is running with"
                );
                LifecycleDecision::StopUnderRunningConfig(stop_mode)
            }
            LifecycleIntent::Refused(error) => LifecycleDecision::Refuse(error),
        }
    }
}
