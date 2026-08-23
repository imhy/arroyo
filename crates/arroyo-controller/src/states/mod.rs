use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::{fmt::Debug, sync::Arc};

use arroyo_rpc::grpc::api::ArrowProgram;

use thiserror::Error;
use time::OffsetDateTime;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender, channel};

use tracing::{debug, error, info, warn};

use anyhow::{Result, anyhow};
use cornucopia_async::DatabaseSource;

use self::checkpoint_stopping::CheckpointStopping;
use self::compiling::Compiling;
use self::failing::Failing;
use self::finishing::Finishing;
use self::leader_checkpoint_stopping::LeaderCheckpointStopping;
use self::leader_finishing::LeaderFinishing;
use self::leader_rescaling::LeaderRescaling;
use self::leader_restarting::LeaderRestarting;
use self::leader_running::LeaderRunning;
use self::leader_stopping::LeaderStopping;
use self::lifecycle::{
    ConsumptionPoint, IntentWakeup, JobLifecycle, JobWait, LifecycleActor, LifecycleIntent,
    ObservedIntent,
};
use self::recovering::Recovering;
use self::rescaling::Rescaling;
use self::running::Running;
use self::scheduling::Scheduling;
use self::stopping::Stopping;
use crate::job_controller::JobController;
use crate::queries::controller_queries;
use crate::types::public::{LogLevel, RestartMode, StopMode};
use crate::{
    JobConfig, JobMessage, JobStatus, PipelineInfo, PolledJob, RefusedConfig, queries,
    schedulers::Scheduler,
};
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::config::{JobControllerMode, config};
use arroyo_rpc::errors::ErrorDomain;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::grpc::rpc::JobFailure;
use arroyo_rpc::public_ids::{IdTypes, generate_id};
use arroyo_rpc::state_backend::{
    StateBackendError, StateBackendSelector, validate_unchanged_job_selector,
};
use arroyo_rpc::worker_types::{RunningMessage, TaskFailedEvent};
use arroyo_rpc::{errors, log_event};
use arroyo_server_common::shutdown::ShutdownGuard;
use arroyo_types::{JobId, PipelineId};
use arroyo_worker::job_controller::job_metrics::JobMetrics;
use prost::Message;

pub(crate) mod checkpoint_stopping;
pub(crate) mod compiling;
pub(crate) mod failing;
pub(crate) mod finishing;
pub(crate) mod leader_checkpoint_stopping;
pub(crate) mod leader_finishing;
pub(crate) mod leader_rescaling;
pub(crate) mod leader_restarting;
pub(crate) mod leader_running;
pub(crate) mod leader_stopping;
pub(crate) mod lifecycle;
pub(crate) mod recovering;
pub(crate) mod rescaling;
pub(crate) mod restarting;
pub(crate) mod running;
pub(crate) mod scheduling;
pub(crate) mod stopping;

pub enum Transition {
    Stop,
    Advance(StateHolder),
}

/// Whether a fatal reason is fatal *because of the configuration the job was carrying when it
/// was raised*.
///
/// Every fatal error says the job cannot go on. This says whether a later decision about the
/// job's configuration is entitled to withdraw it, and that is not a question the message or
/// the [`errors::ErrorDomain`] can answer: "cannot restore a checkpoint written with a
/// different state backend" and "the job's persisted configuration was refused" are both
/// `Internal` fatals carrying a
/// [`StateBackendError`](arroyo_rpc::state_backend::StateBackendError), and only one of them
/// stops being true when an operator repairs the row.
///
/// The distinction is used by
/// [`Fencing::coalesce_intent`](crate::states::scheduling::fencing::Fencing::coalesce_intent),
/// which is the one place a standing reason is reconsidered in the light of something the
/// job's writer said afterwards.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FatalProvenance {
    /// The job is being failed *because its persisted configuration was refused*: the reason
    /// describes the row, and a writer that has since adopted a different row has replaced the
    /// thing it describes.
    RefusedConfig,
    /// The job is being failed for something a configuration cannot answer — its durable
    /// state, its checkpoints, its cluster, its program. A newer row says nothing about it, so
    /// nothing may downgrade it on the strength of one.
    ///
    /// The default, and deliberately so: [`fatal`] produces this, so a fatal reason nobody has
    /// classified is one nothing is allowed to withdraw.
    Unrelated,
}

#[derive(Error, Debug)]
pub enum StateError {
    #[error("fatal error: {message:?}")]
    FatalError {
        message: String,
        domain: errors::ErrorDomain,
        source: anyhow::Error,
        /// Why this is fatal, for the one decision that is allowed to reconsider it.
        provenance: FatalProvenance,
    },
    #[error("retryable error: {message:?} ")]
    RetryableError {
        state: Box<dyn State>,
        message: String,
        domain: errors::ErrorDomain,
        source: anyhow::Error,
        retries: usize,
    },
}

/// A fatal reason that is *not* the job's configuration being refused.
///
/// The fail-closed default. Use [`fatal_refused_config`] for a refusal, and nothing else: a
/// reason marked as a refusal is a reason a later adoption may withdraw, so mis-marking one
/// loses a failure the job should have had.
pub fn fatal(message: impl Into<String>, source: anyhow::Error) -> StateError {
    StateError::FatalError {
        message: message.into(),
        domain: errors::ErrorDomain::Internal,
        source,
        provenance: FatalProvenance::Unrelated,
    }
}

/// The fatal reason of a job whose persisted configuration was refused.
///
/// Identical to [`fatal`] except for what it records about *why*. That record is what lets a
/// scheduling attempt that has already been interrupted tell the two apart when the job's
/// writer adopts a newer configuration while it fences: a refusal of the replaced row is no
/// longer a reason to fail the job, and a reason that was never about the row is unaffected.
pub(crate) fn fatal_refused_config(
    message: impl Into<String>,
    source: anyhow::Error,
) -> StateError {
    StateError::FatalError {
        message: message.into(),
        domain: errors::ErrorDomain::Internal,
        source,
        provenance: FatalProvenance::RefusedConfig,
    }
}

#[derive(Debug)]
pub struct Created;

#[async_trait::async_trait]
impl State for Created {
    fn name(&self) -> &'static str {
        "Created"
    }

    /// Stays. `Created` writes nothing, starts nothing and waits on nothing; the whole of its
    /// body is the transition to `Compiling`, which answers the same stop before anything
    /// reaches `Scheduling`.
    fn leave_for_stop(self: Box<Self>, _config: &JobConfig) -> LeavingForStop {
        LeavingForStop::Stays(self)
    }

    async fn next(self: Box<Self>, _: &mut JobContext) -> Result<Transition, StateError> {
        Ok(Transition::next(*self, Compiling))
    }
}

async fn handle_terminal<'a>(ctx: &mut JobContext<'a>) {
    if let Err(e) = ctx
        .scheduler
        .stop_workers(&ctx.config.id, Some(ctx.status.generation), true)
        .await
    {
        warn!(
            message = "Failed to clean up cluster",
            error = format!("{:?}", e),
            job_id = %ctx.config.id,
            pipeline_id = *ctx.pipeline_info.pipeline_id
        );
    }
}

#[derive(Debug)]
pub struct Failed;
#[async_trait::async_trait]
impl State for Failed {
    fn name(&self) -> &'static str {
        "Failed"
    }

    /// Stays. The job has already failed; `handle_terminal` tears its workers down, which is
    /// everything a stop would do to a job in this state, and what `Failed` reads the
    /// configuration for is the *restart* nonce, not the stop mode.
    fn leave_for_stop(self: Box<Self>, _config: &JobConfig) -> LeavingForStop {
        LeavingForStop::Stays(self)
    }

    async fn next(self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        handle_terminal(ctx).await;
        if ctx.config.restart_nonce != ctx.status.restart_nonce {
            // the user has requested a restart
            Ok(Transition::next(*self, Compiling {}))
        } else {
            Ok(Transition::Stop)
        }
    }

    fn is_terminal(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct Finished;

#[async_trait::async_trait]
impl State for Finished {
    fn name(&self) -> &'static str {
        "Finished"
    }

    /// Stays. The job's sources are exhausted and it has ended; `handle_terminal` tears its
    /// workers down and the state machine stops. There is nothing left for a stop to stop.
    fn leave_for_stop(self: Box<Self>, _config: &JobConfig) -> LeavingForStop {
        LeavingForStop::Stays(self)
    }

    async fn next(self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        handle_terminal(ctx).await;
        Ok(Transition::Stop)
    }

    fn is_terminal(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct Stopped {}

#[async_trait::async_trait]
impl State for Stopped {
    fn name(&self) -> &'static str {
        "Stopped"
    }

    /// Stays, and is the one terminal state for which that is a *read* rather than an
    /// absence of one: `Stopped` already consults `stop_mode` unconditionally, and restarts
    /// the job only when it is `none`. A stop published at the boundary is therefore honoured
    /// by the body itself — the job stays stopped instead of being compiled again.
    fn leave_for_stop(self: Box<Self>, _config: &JobConfig) -> LeavingForStop {
        LeavingForStop::Stays(self)
    }

    async fn next(self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        handle_terminal(ctx).await;

        if ctx.config.stop_mode == StopMode::none && ctx.config.ttl.is_none() {
            Ok(Transition::next(*self, Compiling {}))
        } else {
            Ok(Transition::Stop)
        }
    }

    fn is_terminal(&self) -> bool {
        true
    }
}

// State transitions
impl TransitionTo<Compiling> for Created {}

impl TransitionTo<Compiling> for Stopped {}

impl TransitionTo<Compiling> for Scheduling {}

impl TransitionTo<Scheduling> for Compiling {}

impl TransitionTo<Running> for Scheduling {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            // set the start time and clear the finish time, but only if this is an initial start
            // and not a recovery
            if ctx.status.start_time.is_none() || ctx.status.finish_time.is_some() {
                ctx.status.start_time = Some(OffsetDateTime::now_utc());
                ctx.status.finish_time = None;
            }
        })
    }
}

impl TransitionTo<LeaderRunning> for Scheduling {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            if ctx.status.start_time.is_none() || ctx.status.finish_time.is_some() {
                ctx.status.start_time = Some(OffsetDateTime::now_utc());
                ctx.status.finish_time = None;
            }
        })
    }
}

impl TransitionTo<CheckpointStopping> for Running {}
impl TransitionTo<Stopping> for Running {}
impl TransitionTo<Stopping> for LeaderRunning {}
impl TransitionTo<Stopping> for Scheduling {}
impl TransitionTo<Stopping> for Compiling {}
impl TransitionTo<Stopping> for Rescaling {}
impl TransitionTo<Finishing> for Running {}
impl TransitionTo<Recovering> for Running {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.restarts += 1;
        })
    }
}
impl TransitionTo<Recovering> for LeaderRunning {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.restarts += 1;
        })
    }
}
impl TransitionTo<Recovering> for Scheduling {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.restarts += 1;
        })
    }
}

impl TransitionTo<Rescaling> for Running {}

impl TransitionTo<Scheduling> for Rescaling {}

impl TransitionTo<Compiling> for Recovering {}
impl TransitionTo<Compiling> for Failed {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.restart_nonce = ctx.config.restart_nonce;
            ctx.status.restarts = 0;
            ctx.status.failure_message = None;
        })
    }
}

impl TransitionTo<Failing> for Running {}
impl TransitionTo<Failing> for Scheduling {}
impl TransitionTo<Failed> for Failing {}

fn done_transition(ctx: &mut JobContext) {
    ctx.status.finish_time = Some(OffsetDateTime::now_utc());
    ctx.job_controller = None;
    ctx.leader_manager = None;
    ctx.status.state_context.leader = None;
}

impl TransitionTo<Stopped> for Stopping {
    fn update_status(&self) -> TransitionFn {
        Box::new(done_transition)
    }
}

impl TransitionTo<LeaderCheckpointStopping> for LeaderRunning {}
impl TransitionTo<LeaderStopping> for LeaderRunning {}
impl TransitionTo<LeaderFinishing> for LeaderRunning {}
impl TransitionTo<LeaderRestarting> for LeaderRunning {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.restart_nonce = ctx.config.restart_nonce;
        })
    }
}
impl TransitionTo<LeaderRescaling> for LeaderRunning {}

impl TransitionTo<Stopped> for LeaderStopping {
    fn update_status(&self) -> TransitionFn {
        Box::new(done_transition)
    }
}

impl TransitionTo<LeaderStopping> for LeaderStopping {}

impl TransitionTo<Stopping> for LeaderStopping {}
impl TransitionTo<Recovering> for LeaderStopping {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.restarts += 1;
        })
    }
}

impl TransitionTo<LeaderStopping> for LeaderCheckpointStopping {}
impl TransitionTo<Stopping> for LeaderCheckpointStopping {}
impl TransitionTo<Stopped> for LeaderCheckpointStopping {
    fn update_status(&self) -> TransitionFn {
        Box::new(done_transition)
    }
}
impl TransitionTo<Recovering> for LeaderCheckpointStopping {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.restarts += 1;
        })
    }
}

impl TransitionTo<Finished> for LeaderFinishing {
    fn update_status(&self) -> TransitionFn {
        Box::new(done_transition)
    }
}
impl TransitionTo<Recovering> for LeaderFinishing {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.restarts += 1;
        })
    }
}
impl TransitionTo<LeaderStopping> for LeaderFinishing {}
impl TransitionTo<Stopping> for LeaderFinishing {}

impl TransitionTo<LeaderCheckpointStopping> for LeaderRestarting {}
impl TransitionTo<LeaderRestarting> for LeaderRestarting {}
impl TransitionTo<Scheduling> for LeaderRestarting {}

impl TransitionTo<Recovering> for LeaderRestarting {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.restarts += 1;
        })
    }
}
impl TransitionTo<LeaderStopping> for LeaderRestarting {}

impl TransitionTo<Scheduling> for LeaderRescaling {}

impl TransitionTo<Recovering> for LeaderRescaling {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.restarts += 1;
        })
    }
}
impl TransitionTo<LeaderStopping> for LeaderRescaling {}
// A checkpoint-stop decided while the job was still running reaches `LeaderRescaling` at the
// state boundary, and a rescale that has not started yet has no reason to throw the final
// checkpoint away: the edge exists so `leave_for_stop` can honour the mode the operator asked
// for rather than downgrading it.
impl TransitionTo<LeaderCheckpointStopping> for LeaderRescaling {}

impl TransitionTo<Stopping> for CheckpointStopping {}
impl TransitionTo<Stopped> for CheckpointStopping {
    fn update_status(&self) -> TransitionFn {
        Box::new(done_transition)
    }
}

impl TransitionTo<Finished> for Finishing {
    fn update_status(&self) -> TransitionFn {
        Box::new(done_transition)
    }
}
// A job whose sources are exhausted still has workers, and a stop is the operator's answer to
// workers that are not ending on their own. `Finishing` reaches this both from the state
// boundary and from inside its own wait, through one mapping — `stop_if_desired_non_running!`.
// `LeaderFinishing` has had the same edge since M11.T08.
impl TransitionTo<Stopping> for Finishing {}

impl TransitionTo<Restarting> for Running {
    fn update_status(&self) -> TransitionFn {
        Box::new(|ctx| {
            ctx.status.restart_nonce = ctx.config.restart_nonce;
        })
    }
}
impl TransitionTo<Restarting> for Restarting {}
impl TransitionTo<Scheduling> for Restarting {}
impl TransitionTo<Stopping> for Restarting {}
impl TransitionTo<CheckpointStopping> for Restarting {}

// Macro to handle stopping behavior from a running state, where we want to
// support checkpoint stopping
macro_rules! stop_if_desired_running {
    ($self: ident, $config: expr) => {
        use crate::states::checkpoint_stopping::CheckpointStopping;
        use crate::states::stopping::StopBehavior;
        use crate::states::stopping::Stopping;
        use crate::types::public::StopMode;
        use arroyo_rpc::grpc::rpc;
        match $config.stop_mode {
            StopMode::checkpoint => {
                return Ok(Transition::next(*$self, CheckpointStopping {}));
            }
            StopMode::graceful => {
                return Ok(Transition::next(
                    *$self,
                    Stopping {
                        stop_mode: StopBehavior::StopJob(rpc::StopMode::Graceful),
                    },
                ));
            }
            StopMode::immediate => {
                return Ok(Transition::next(
                    *$self,
                    Stopping {
                        stop_mode: StopBehavior::StopJob(rpc::StopMode::Immediate),
                    },
                ));
            }
            StopMode::force => {
                return Ok(Transition::next(
                    *$self,
                    Stopping {
                        stop_mode: StopBehavior::StopWorkers,
                    },
                ));
            }
            StopMode::none => {
                // do nothing
            }
        }
    };
}

// macro to handle stopping behavior from a state where the job is not current running
// (like compiling / scheduling / etc.). in this case, there's nothing active to checkpoint
// so we just move to stopping all cases
macro_rules! stop_if_desired_non_running {
    ($self: ident, $config: expr) => {
        use crate::states::stopping::StopBehavior;
        use crate::states::stopping::Stopping;
        use crate::types::public::StopMode;
        use arroyo_rpc::grpc;
        match $config.stop_mode {
            StopMode::checkpoint | StopMode::graceful | StopMode::immediate => {
                return Ok(Transition::next(
                    *$self,
                    Stopping {
                        stop_mode: StopBehavior::StopJob(grpc::rpc::StopMode::Immediate),
                    },
                ));
            }
            StopMode::force => {
                return Ok(Transition::next(
                    *$self,
                    Stopping {
                        stop_mode: StopBehavior::StopWorkers,
                    },
                ));
            }
            StopMode::none => {
                // do nothing
            }
        }
    };
}

macro_rules! leader_stop_if_desired_running {
    ($self:ident, $config:expr) => {
        use crate::states::leader_checkpoint_stopping::LeaderCheckpointStopping;
        use crate::states::leader_stopping::{LeaderStopBehavior, LeaderStopping};
        use crate::types::public::StopMode;
        use arroyo_rpc::grpc::rpc::JobStopMode;

        match $config.stop_mode {
            StopMode::force => {
                return Ok(Transition::next(
                    *$self,
                    LeaderStopping {
                        stop_behavior: LeaderStopBehavior::StopWorkers,
                    },
                ));
            }
            StopMode::checkpoint => {
                return Ok(Transition::next(*$self, LeaderCheckpointStopping {}));
            }
            StopMode::graceful => {
                return Ok(Transition::next(
                    *$self,
                    LeaderStopping {
                        stop_behavior: LeaderStopBehavior::StopJob(JobStopMode::JobStopGraceful),
                    },
                ));
            }
            StopMode::immediate => {
                return Ok(Transition::next(
                    *$self,
                    LeaderStopping {
                        stop_behavior: LeaderStopBehavior::StopJob(JobStopMode::JobStopImmediate),
                    },
                ));
            }
            StopMode::none => {
                // do nothing
            }
        }
    };
}

/// What a configuration update means for a job whose workers are already running.
///
/// Produced by [`classify_running_config_update`], which is the one place both running
/// modes decide that question.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunningConfigUpdate {
    /// Nothing in the update requires the workers to be rescheduled; the state may apply
    /// it in place.
    Apply,
    /// The update changes something that is only read when workers are (re)scheduled, so
    /// it takes effect only after a restart in this mode's restarting state.
    Restart(RestartMode),
}

/// Decides what a running job must do about a new configuration, before it applies any of
/// it.
///
/// Legacy and worker-leader running states share this so the two modes cannot drift: both
/// restart for a restart-nonce, env-var, or scheduler-config change, and both refuse a
/// state-backend change outright.
///
/// `execution_selector` — not `current.state_backend` — is what the update is checked
/// against. `current` is the state machine's shared configuration as of the last
/// transition, so using it would compare the update against whatever the previous refresh
/// installed; the execution's selector is fixed for the life of the job's workers.
///
/// The state backend is deliberately *not* a restartable change. A restart restores from
/// the job's last checkpoint, and that checkpoint was written by the backend the job is
/// running with, so restarting under a different selector would only move the failure to
/// the restore path. The alternative — discarding the state so the new backend can start
/// clean — destroys data on the strength of a configuration edit, and M11.T08 ships no
/// user-facing way to make that edit deliberately. Refusing it keeps the job's state
/// exactly as it is and leaves an operator both remedies: restore the previous value, or
/// stop this job and create a new one under the new backend.
///
/// The stop half of that is reachable with the bad value still in the row: the row is
/// checked *after* the state's stop decision here, and [`StateMachine::apply_refused_row`]
/// carries a refused row's stop request through under the running selector rather than
/// discarding it with the rest of the row.
///
/// The parallelism overrides are not considered here: the two modes look up a running
/// operator's actual parallelism differently, so each checks those itself, after this.
///
/// # Errors
///
/// Returns a fatal [`StateError`] carrying
/// [`StateBackendError::JobSelectorChanged`](arroyo_rpc::state_backend::StateBackendError::JobSelectorChanged)
/// if `updated` names a different state backend than the job is running with.
pub(crate) fn classify_running_config_update(
    execution_selector: StateBackendSelector,
    current: &JobConfig,
    updated: &JobConfig,
    restart_nonce: i32,
) -> Result<RunningConfigUpdate, StateError> {
    // Checked before anything else is acted on: a job whose selector has changed must not
    // be restarted into the new one, and must not go on running while the database claims
    // a backend its workers are not using.
    validate_unchanged_job_selector(&current.id, execution_selector, updated.state_backend)
        .map_err(|e| {
            fatal_refused_config(
                "the state backend of a running job cannot be changed",
                e.into(),
            )
        })?;

    if updated.restart_nonce != restart_nonce {
        return Ok(RunningConfigUpdate::Restart(updated.restart_mode));
    }

    // env_vars and scheduler_config are only applied when workers are (re)scheduled, so a
    // change to either while the job is running requires a restart to take effect.
    if updated.scheduler_config != current.scheduler_config || updated.env_vars != current.env_vars
    {
        return Ok(RunningConfigUpdate::Restart(RestartMode::safe));
    }

    Ok(RunningConfigUpdate::Apply)
}

/// What a running job does about a configuration it has just been given.
///
/// [`RunningConfigUpdate`] answers the half of that question that does not depend on how the
/// job's workers are laid out; this is the whole of it, and it is what both running modes and
/// both delivery routes decide with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunningConfigAction {
    /// Nothing in the configuration requires the workers to be rescheduled: it is applied to
    /// the running job in place.
    Apply,
    /// The configuration changes something that is only read when workers are (re)scheduled,
    /// so it takes effect only after a restart in this mode's restarting state.
    Restart(RestartMode),
    /// An operator's parallelism override no longer matches the parallelism its running
    /// operator has, so the job is rescheduled at the new width in this mode's rescaling
    /// state.
    Rescale,
}

/// Decides what a running job must do about a configuration it has just been given, in either
/// running mode and by either delivery route.
///
/// # Why this exists rather than three steps written out at each site
///
/// A configuration reaches a running job two ways: as a [`JobMessage::ConfigUpdate`] on the
/// landed M11.T08 path — which is what production runs — and, for a job whose lifecycle the
/// M11.D39a single writer decides, as an [`ObservedIntent::Adopted`] that writer published.
/// The two must not come to mean different things, and "must not" is not a property of anyone
/// remembering: it is a property of there being one function. PR #160 review comment
/// `5365261487` is what the other arrangement produced — the adopted route reported only
/// whether the job stopped, so a `restart_nonce`, scheduler, environment or parallelism change
/// published through the mailbox reached neither the restart classification nor the rescale
/// comparison, and left the job's workers on the configuration it replaced.
///
/// `operator_parallelism` is the one thing the two modes genuinely differ on: controller mode
/// asks the [`JobController`] what an operator is actually running at, and leader mode reads
/// the program's own task counts. Everything else — including the selector refusal that
/// [`classify_running_config_update`] makes first — is shared.
///
/// `current` is the configuration the job was running under and `updated` the one it has been
/// given; on the adopted route those are [`ObservedIntent::Adopted`]'s payload and
/// [`JobContext::config`] respectively, because publication has already happened by the time a
/// consumption point sees it.
///
/// # Errors
///
/// The fatal [`StateError`] [`classify_running_config_update`] produces for a configuration
/// that changes the job's state backend. Unreachable on the adopted route —
/// [`LifecycleActor::decide`](lifecycle::LifecycleActor) refuses such a row rather than
/// adopting it — and reached here anyway, because a check that is skipped where it is believed
/// to be redundant is a check that is missing when the belief stops being true.
pub(crate) fn decide_running_config(
    execution_selector: StateBackendSelector,
    current: &JobConfig,
    updated: &JobConfig,
    restart_nonce: i32,
    operator_parallelism: impl Fn(u32) -> Option<usize>,
) -> Result<RunningConfigAction, StateError> {
    match classify_running_config_update(execution_selector, current, updated, restart_nonce)? {
        RunningConfigUpdate::Restart(mode) => return Ok(RunningConfigAction::Restart(mode)),
        RunningConfigUpdate::Apply => {}
    }

    // Deliberately after the restart classification: a job that is being rescheduled anyway
    // picks the new width up from the configuration it is rescheduled with.
    for (node_id, requested) in &updated.parallelism_overrides {
        if let Some(actual) = operator_parallelism(*node_id)
            && actual != *requested
        {
            return Ok(RunningConfigAction::Rescale);
        }
    }

    Ok(RunningConfigAction::Apply)
}

/// What a running state did with a configuration it was given.
///
/// The state comes back out when it is applied in place, because applying is not leaving and
/// the state has more running to do. One type for both modes: `Running` and `LeaderRunning`
/// differ in *which* states they name, not in the shape of the answer.
pub(crate) enum ConfigApplied<S> {
    /// Applied to the running job; carry on in this state.
    Applied(Box<S>),
    /// The configuration reschedules the job, in this mode's restarting or rescaling state.
    Leaves(Transition),
}

pub fn controller_job_failure(
    message: impl Into<String>,
    error_domain: rpc::ErrorDomain,
    retry_hint: rpc::RetryHint,
) -> JobFailure {
    JobFailure {
        operator_id: None,
        task_id: None,
        subtask_index: None,
        message: message.into(),
        error_domain: error_domain as i32,
        retry_hint: retry_hint as i32,
    }
}

use crate::job_controller::leader_manager::LeaderManager;
use crate::states::restarting::Restarting;
pub(crate) use leader_stop_if_desired_running;
pub(crate) use stop_if_desired_non_running;
pub(crate) use stop_if_desired_running;

pub struct JobContext<'a> {
    pub config: JobConfig,
    /// The state backend this execution of the job is running with.
    ///
    /// Captured once, from the configuration the state machine was started with, and
    /// never reassigned: the workers' `TaskInfo`, every `TableConfig` they stamp, every
    /// checkpoint they write and every cleanup that deletes one all carry this value, so
    /// it is the job's authority for as long as the job runs. [`config`](Self::config) is
    /// refreshed from the state machine's shared configuration after every transition and
    /// is therefore *not* that authority — which is the whole reason this field exists
    /// separately from `config.state_backend`.
    ///
    /// It is per-job state, threaded explicitly from the job's own [`StateMachine`]; there
    /// is deliberately no process-global, thread-local, or environment fallback (M11.T08d).
    pub execution_selector: StateBackendSelector,
    /// The identity of the cluster this controller belongs to, stamped into every worker
    /// this job starts.
    ///
    /// Threaded from the controller that owns the job, for the same reason
    /// [`execution_selector`](Self::execution_selector) is: a state should be given what it
    /// acts on rather than reach for a process-wide cell. It matters beyond tidiness here,
    /// because the only way to populate that cell is a setter that resolves the identity
    /// against `~/.config/arroyo/cluster-info` and writes it there — so a `Scheduling` that
    /// read the global made every test of it a reason to create or overwrite a developer's
    /// real cluster identity.
    pub cluster_id: Arc<String>,
    pub pipeline_info: Arc<PipelineInfo>,
    pub status: &'a mut JobStatus,
    pub program: &'a mut LogicalProgram,
    pub db: DatabaseSource,
    pub scheduler: Arc<dyn Scheduler>,
    pub rx: &'a mut Receiver<JobMessage>,
    /// The refusal this task must apply before its next state runs, if any.
    ///
    /// Not `pub`, unlike the rest: no state reads it. [`execute_state`] consults it on every
    /// state's behalf precisely because a state that had to remember to consult it would be
    /// a state that could forget — which is the shape of the bug this exists for.
    pub(crate) refusal_gate: RefusalGate,
    /// This job's D39a single writer, or `None` under
    /// [`LifecycleMode::LegacyT08`](lifecycle::LifecycleMode::LegacyT08) — which is what
    /// production runs through M11.T25, so in production this is always `None` and
    /// [`refusal_gate`](Self::refusal_gate) above is the mechanism.
    ///
    /// Not `pub`, for the same reason the gate is not: no state reaches for it. It is read
    /// by [`Self::observe_lifecycle_intent`], which the state boundary and the
    /// interruptible waits call, so a state that had to remember to consult it cannot
    /// forget to.
    pub(crate) lifecycle_actor: Option<LifecycleActor>,
    pub retries_attempted: usize,
    pub job_controller: Option<JobController>,
    pub leader_manager: Option<LeaderManager>,
    pub last_transitioned_at: Instant,
    pub metrics: Arc<tokio::sync::RwLock<HashMap<Arc<String>, JobMetrics>>>,
}

/// Handles a job message no state has a use of its own for.
///
/// This is the one place a [`JobMessage::ConfigRefused`] is acted on: every state that
/// reads the job's message channel routes what it does not recognize here, so the policy
/// for a refused configuration — fail the job, in whatever state it is in — is written
/// once instead of once per state.
///
/// It is a free function rather than only a [`JobContext`] method because the states that
/// hold a live borrow of [`JobContext::job_controller`] across their message loop cannot
/// also borrow the whole context; they pass the two fields it reports on.
///
/// # Errors
///
/// Returns a fatal [`StateError`] for a [`JobMessage::ConfigRefused`] that still describes
/// the job's configuration. A refusal that has been superseded since it was queued — the
/// operator repaired the row, a different refusal replaced it, or a stop is answering it —
/// is discarded, because failing the job would fail it for a configuration that no longer
/// exists. Every other message is logged and ignored.
pub(crate) fn handle_unhandled_message(
    job_id: &str,
    pipeline_id: &str,
    msg: JobMessage,
) -> Result<(), StateError> {
    match msg {
        JobMessage::ConfigRefused(refusal) => {
            let Some(e) = refusal.into_current_error() else {
                // The queue is FIFO and a message in it cannot be retracted, so this is
                // the only place the race can be resolved: the row this refusal describes
                // has already been repaired or replaced.
                info!(
                    %job_id,
                    pipeline_id,
                    "discarding a configuration refusal that no longer describes the job's \
                     configuration"
                );
                return Ok(());
            };
            error!(
                %job_id,
                pipeline_id,
                error = %e,
                "failing job whose persisted configuration was refused"
            );
            Err(fatal_refused_config(
                "the job's persisted configuration was refused",
                e.into(),
            ))
        }
        JobMessage::RunningMessage(RunningMessage::WorkerHeartbeat { .. }) => Ok(()),
        msg => {
            warn!(%job_id, pipeline_id, "unhandled job message {:?}", msg);
            Ok(())
        }
    }
}

/// Refuses a configuration update that names a different state backend than the one this
/// execution is running with.
///
/// [`StateMachine::update`] already refuses such a row before it replaces the state
/// machine's authoritative configuration, so in a running controller no state should ever
/// see a change. Every state that acts on a [`JobMessage::ConfigUpdate`] calls this
/// anyway, because "the selector is immutable" is a property of the whole lifecycle and
/// not of the one writer that happens to enforce it today: a state that reschedules,
/// restarts, or rescales the job must validate against the selector the job is actually
/// running with, and never against whatever the last refresh of [`JobContext::config`]
/// put there.
///
/// # Errors
///
/// Returns a fatal [`StateError`] carrying
/// [`StateBackendError::JobSelectorChanged`](arroyo_rpc::state_backend::StateBackendError::JobSelectorChanged)
/// if `updated` names a different state backend than `execution_selector`.
pub(crate) fn check_config_update(
    execution_selector: StateBackendSelector,
    updated: &JobConfig,
) -> Result<(), StateError> {
    validate_unchanged_job_selector(&updated.id, execution_selector, updated.state_backend).map_err(
        |e| {
            fatal_refused_config(
                "the state backend of an existing job cannot be changed",
                e.into(),
            )
        },
    )
}

impl JobContext<'_> {
    /// [`handle_unhandled_message`] for a context that can be borrowed whole.
    pub fn handle(&self, msg: JobMessage) -> Result<(), StateError> {
        handle_unhandled_message(&self.config.id, &self.pipeline_info.pipeline_id, msg)
    }

    /// Admits this state to one region of irreversible scheduling work, or fails the job
    /// because its configuration has been refused.
    ///
    /// [`Scheduling`] is not one long region. It alternates: a stretch of irreversible work
    /// with no `recv` in it, then an interruptible phase that waits on the job's channel,
    /// then the next irreversible stretch. This is called at every *crossing* — every point
    /// where an interruptible phase gives way to irreversible work — and it does two things
    /// there that only work together:
    ///
    /// * it re-reads the gate, which is the authoritative record of what the state machine
    ///   has refused, rather than trusting that a refusal has reached the front of the job's
    ///   queue. The interruptible phases end on a *queued* message — enough worker connects,
    ///   enough task starts — and a refusal published while they were waiting can be sitting
    ///   behind those messages, unread, when they break. Channel order is delivery; the gate
    ///   is the decision.
    /// * it takes the job's publication lock, so for as long as the caller holds the returned
    ///   [`Admission`] no refusal can be published at all. Without that, the re-read would be
    ///   just another snapshot, and there would again be a last check and a first effect after
    ///   it.
    ///
    /// So hold it across the irreversible work and drop it before the next `recv`. Dropping it
    /// early reopens the window; holding it across a `recv` would leave a job that waits
    /// minutes for its workers unable to be refused for exactly as long. Each region must also
    /// drop before the next is entered — the guard does not re-enter, so a shadowed live one
    /// would wedge the job on its own lock.
    ///
    /// # Errors
    ///
    /// Returns the same fatal [`StateError`] the queued [`JobMessage::ConfigRefused`] would
    /// produce, through the same [`handle_unhandled_message`] policy — including its
    /// superseded-version check, so a row repaired since the refusal was published lets the
    /// job schedule instead of failing it.
    pub(crate) async fn admit_irreversible_scheduling(&mut self) -> Result<Admission, StateError> {
        let (admission, refusal) = self.refusal_gate.admit_scheduling().await;
        if let Some(refusal) = refusal {
            self.handle(JobMessage::ConfigRefused(refusal))?;
        }
        Ok(admission)
    }

    /// Whether this job's lifecycle transitions are decided by the M11.D39a single writer.
    ///
    /// The existence of an actor *is* that fact: `JobLifecycle::actor` returns `None` for the
    /// landed M11.T08 mechanism, so there is nothing to consult and nothing that could
    /// disagree with the selection. Asked here rather than by matching on a mode so that the
    /// mode itself stays named in exactly one production place —
    /// `no_production_path_selects_the_fenced_v2_lifecycle` counts those, and a second one
    /// would be a second thing that could choose differently.
    pub(crate) fn runs_fenced_lifecycle(&self) -> bool {
        self.lifecycle_actor.is_some()
    }

    /// Reads the job's lifecycle intent and publishes whatever it decides, if this job runs
    /// the D39a path.
    ///
    /// A no-op under [`LifecycleMode::LegacyT08`](lifecycle::LifecycleMode::LegacyT08) —
    /// production through M11.T25 — where there is no actor and the cross-task
    /// [`RefusalGate`] is the mechanism.
    ///
    /// It is a plain `fn` and not `async`, which is not an accident: the interruptible
    /// waits call it on every turn, and the source-level pin
    /// `the_source_of_scheduling_next_keeps_every_irreversible_effect_inside_an_admitted_region`
    /// enumerates everything those stretches are allowed to await. Consuming an intent is a
    /// lock and a comparison; nothing about it should ever become something to wait on.
    ///
    /// # What the caller must do with the answer
    ///
    /// [`ObservedIntent::Stop`] is not advice. A stop the writer has decided on is published
    /// into [`Self::config`] here, and the caller has to leave for its own stop state before
    /// its next irreversible effect — the `StartExecution` fan-out and the publication of a
    /// restored checkpoint's commits both sit immediately after a consumption point, and
    /// neither can be withdrawn. Which state "leaving" means differs by caller, which is why
    /// this returns the fact rather than a transition: every caller answers it with the same
    /// `stop_if_desired*` macro the landed path uses, so a stop consumed from the mailbox and
    /// a stop delivered as a [`JobMessage::ConfigUpdate`] cannot come to mean different
    /// things.
    ///
    /// Under [`LifecycleMode::LegacyT08`](lifecycle::LifecycleMode::LegacyT08) — production
    /// through M11.T25 — there is no actor, so the answer is always
    /// [`ObservedIntent::Continue`] and every guard built on it is unreachable.
    ///
    /// # Errors
    ///
    /// Returns the fatal [`StateError`] a refused configuration produces, from whatever
    /// state the job is in — the same outcome the T08 path reaches through
    /// [`handle_unhandled_message`].
    pub(crate) fn observe_lifecycle_intent(
        &mut self,
        at: ConsumptionPoint,
    ) -> Result<ObservedIntent, StateError> {
        Ok(self
            .observe_lifecycle_decision(at)?
            .unwrap_or(ObservedIntent::Continue))
    }

    /// The same read, keeping the one thing [`Self::observe_lifecycle_intent`] discards:
    /// whether the writer decided anything at all.
    ///
    /// `None` is "the writer has said nothing new since the last look"; `Some` is a decision
    /// that has been applied, and what the job's configuration says as a result. The two are
    /// the same answer for a phase — a job that carries on carries on either way — and a
    /// different one for a job that is *fencing*, whose standing reason for ending may be one
    /// the writer has since superseded. Reporting a superseded reason fails a job for a
    /// configuration that no longer exists, which is the same defect
    /// [`RefusedConfig::into_current_error`] closes on the M11.T08 path.
    ///
    /// # Errors
    ///
    /// The fatal [`StateError`] of a refused configuration, exactly as above.
    pub(crate) fn observe_lifecycle_decision(
        &mut self,
        at: ConsumptionPoint,
    ) -> Result<Option<ObservedIntent>, StateError> {
        let Some(decision) = self
            .lifecycle_actor
            .as_mut()
            .and_then(|actor| actor.observe(at))
        else {
            return Ok(None);
        };
        decision
            .apply(&mut self.config, &self.pipeline_info.pipeline_id)
            .map(Some)
    }

    /// The job's controller and the job's wait, as two borrows that can be held at once.
    ///
    /// This is the **only** place a [`JobWait`] is assembled, which is what makes "every
    /// interruptible wait is a consumption point" a property of the type rather than of each
    /// wait's author: `JobWait::new` is visible only inside this module tree, and
    /// [`JobController::wait_for_finish`] takes the wait rather than a bare
    /// [`Receiver`]. A caller cannot hand it half the sources a stop can arrive on, because a
    /// caller cannot build a wait at all.
    ///
    /// Two borrows rather than one call, because a wait for a job's workers to finish needs the
    /// controller *and* the job's channel *and* the job's configuration at the same time. They
    /// are disjoint fields of this context, so the split is what the borrow checker already
    /// permits — written once here so that no state has to write it.
    ///
    /// `None` when the job has no controller: a leader-mode job never gets one, and a job whose
    /// last transition ran `done_transition` has had it taken away.
    pub(crate) fn controller_and_wait(&mut self) -> Option<(&mut JobController, JobWait<'_>)> {
        let controller = self.job_controller.as_mut()?;
        Some((
            controller,
            JobWait::new(
                self.rx,
                self.lifecycle_actor.as_mut(),
                &mut self.config,
                &self.pipeline_info.pipeline_id,
            ),
        ))
    }

    /// Answers a stop this job's writer decided before the given state ran, and keeps it
    /// standing if the state declines to leave for it.
    ///
    /// [`State::leave_for_stop`]'s two answers are not "act" and "ignore". A state that stays
    /// says the stop is not answered *here* — because its own body is what answers it — and the
    /// stop is therefore still the job's standing instruction when its body starts. Recording
    /// that is what stops "stays" from meaning "discarded": the intent has been consumed from
    /// the mailbox and the state's own [`Self::observe_lifecycle_intent`] can never report it
    /// again, so a state that waits without it waits out a stop that was already decided.
    ///
    /// PR #160 review comment `5362488017` is that gap: `Finishing` waits with no deadline for
    /// workers that may never finish, and a stop taken at its boundary reached nothing that
    /// could end the wait.
    fn hand_stop_to(&mut self, state: Box<dyn State>) -> LeavingForStop {
        match state.leave_for_stop(&self.config) {
            LeavingForStop::Leaves(transition) => LeavingForStop::Leaves(transition),
            LeavingForStop::Stays(state) => {
                if let Some(actor) = self.lifecycle_actor.as_mut() {
                    // `ObservedIntent::Stop` is `stop_mode != none`, so this is never `none`.
                    actor.leave_stop_standing(self.config.stop_mode);
                }
                LeavingForStop::Stays(state)
            }
        }
    }

    /// Hands a configuration adopted before a state ran to that state's own consumption
    /// points.
    ///
    /// The counterpart of [`Self::hand_stop_to`], for the other decision a state has to
    /// answer, and for the same reason: an intent is decided once, so an adoption read at the
    /// state boundary is one the state's own reads can never report. The boundary holds a
    /// `Box<dyn State>` and has no job controller, so it cannot classify the change itself —
    /// what a new configuration means for a running job is decided by
    /// [`decide_running_config`], against the workers the job actually has. So it publishes,
    /// records, and hands on.
    ///
    /// PR #160 review comment `5365261487` is the finding one level down: the same adoption,
    /// read inside `Running`'s own wait, reached neither the restart classification nor
    /// [`JobController::update_config`].
    fn hand_adoption_to(&mut self, superseded: Box<JobConfig>) {
        if let Some(actor) = self.lifecycle_actor.as_mut() {
            actor.leave_adoption_standing(superseded);
        }
    }

    /// What an interruptible wait parks on so that a submitted intent ends it.
    ///
    /// Submission is not delivery. A wait turns only when something wakes it, and for a job
    /// that is simply running well nothing does: the mailbox has no channel behind it and
    /// [`Running`] has no reason to look at it again. So every wait selects on this beside
    /// whatever it was already waiting for, and a stop or a refusal is observed on the turn it
    /// causes rather than at the next timeout — or, for a healthy running job, never.
    ///
    /// Taken as an owned handle before the wait, because the other arms of the same `select!`
    /// borrow this context mutably.
    ///
    /// Never completes for a job whose lifecycle the configuration poll decides, which is
    /// every production job through M11.T25: there is no mailbox, the poll publishes to the
    /// [`RefusalGate`] and the job's queue, and the wait already selects on that queue.
    pub(crate) fn lifecycle_wakeup(&self) -> IntentWakeup {
        match &self.lifecycle_actor {
            Some(actor) => actor.wakeup(),
            None => IntentWakeup::none(),
        }
    }

    pub fn retryable(
        &self,
        state: Box<dyn State>,
        message: impl Into<String>,
        source: anyhow::Error,
        retries: usize,
    ) -> StateError {
        StateError::RetryableError {
            state,
            message: message.into(),
            domain: ErrorDomain::Internal,
            source,
            retries: retries.saturating_sub(self.retries_attempted),
        }
    }

    pub async fn handle_task_error<T: State + TransitionTo<Recovering>>(
        &self,
        state: Box<T>,
        event: TaskFailedEvent,
    ) -> Result<Transition, StateError> {
        error!(
            job_id = %self.config.id.as_str(),
            pipeline_id = *self.pipeline_info.pipeline_id,
            task_id = event.task_id,
            operator_subtask = event.subtask_idx,
            operator_id = event.operator_id,
            error_domain = event.error_domain.as_str(),
            retry_hint = event.retry_hint.as_str(),
            message = "task failed",
            reason = event.reason,
        );

        let client = self.db.client().await.unwrap();
        if let Err(db_err) = queries::controller_queries::execute_create_job_log_message(
            &client,
            &generate_id(IdTypes::JobLogMessage),
            &self.config.id.as_str(),
            &event.operator_id,
            &(event.subtask_idx as i64),
            &LogLevel::error,
            &"task failed",
            &event.reason,
            &event.error_domain.as_str(),
            &event.retry_hint.as_str(),
        )
        .await
        {
            warn!(
                job_id = %self.config.id,
                pipeline_id = *self.pipeline_info.pipeline_id,
                "Failed to log task failure to database: {:?}",
                db_err
            );
        }

        match event.retry_hint {
            errors::RetryHint::NoRetry => Err(StateError::FatalError {
                source: anyhow!("task failed: {}", event.reason),
                message: event.reason,
                domain: event.error_domain,
                // A task that failed is not a row that was refused: repairing the job's
                // configuration does not un-fail it.
                provenance: FatalProvenance::Unrelated,
            }),
            errors::RetryHint::WithBackoff => Ok(Transition::next(
                *state,
                Recovering {
                    source: anyhow!("task failed: {}", event.reason),
                    reason: event.reason,
                    domain: event.error_domain,
                },
            )),
        }
    }

    pub async fn handle_job_failure<T: State + TransitionTo<Recovering>>(
        &self,
        state: T,
        failure: arroyo_rpc::grpc::rpc::JobFailure,
    ) -> Result<Transition, StateError> {
        let error_domain = errors::ErrorDomain::from(failure.error_domain());
        let retry_hint = errors::RetryHint::from(failure.retry_hint());
        let operator_id = failure
            .operator_id
            .clone()
            .unwrap_or_else(|| "<unknown>".to_string());
        let subtask_index = failure.subtask_index.unwrap_or_default();
        let reason = failure.message.clone();

        error!(
            job_id = %self.config.id.as_str(),
            pipeline_id = *self.pipeline_info.pipeline_id,
            task_id = format!("{:?}", failure.task_id),
            operator_subtask = subtask_index,
            operator_id,
            error_domain = error_domain.as_str(),
            retry_hint = retry_hint.as_str(),
            message = "job failed",
            reason,
        );

        if let (Some(operator_id), Some(subtask_index)) =
            (failure.operator_id.as_ref(), failure.subtask_index)
        {
            let client = self.db.client().await.unwrap();
            if let Err(db_err) = queries::controller_queries::execute_create_job_log_message(
                &client,
                &generate_id(IdTypes::JobLogMessage),
                &self.config.id.as_str(),
                operator_id,
                &(subtask_index as i64),
                &LogLevel::error,
                &"job failed",
                &failure.message,
                &error_domain.as_str(),
                &retry_hint.as_str(),
            )
            .await
            {
                warn!(
                    job_id = %self.config.id,
                    pipeline_id = *self.pipeline_info.pipeline_id,
                    "Failed to log job failure to database: {:?}",
                    db_err
                );
            }
        }

        match retry_hint {
            errors::RetryHint::NoRetry => Err(StateError::FatalError {
                source: anyhow!("job failed: {}", failure.message),
                message: failure.message,
                domain: error_domain,
                // The job's own workers reported this; the job's configuration did not cause
                // it and a newer one does not answer it.
                provenance: FatalProvenance::Unrelated,
            }),
            errors::RetryHint::WithBackoff => Ok(Transition::next(
                state,
                Recovering {
                    source: anyhow!("job failed: {}", failure.message),
                    reason: failure.message,
                    domain: error_domain,
                },
            )),
        }
    }

    pub fn leader_manager(&mut self) -> &mut LeaderManager {
        self.leader_manager
            .as_mut()
            .expect("requested leader_manager but was not initialized")
    }
}

#[async_trait::async_trait]
pub trait State: Sync + Send + 'static + Debug {
    fn name(&self) -> &'static str;

    #[allow(unused)]
    fn is_terminal(&self) -> bool {
        false
    }

    /// What this state does about a stop that was decided **before it ran**.
    ///
    /// # Why every state has to answer this, and why the answer cannot be defaulted
    ///
    /// A lifecycle intent is consumed once. [`execute_state`] is M11.D39a's first consumption
    /// point, so a stop the job's writer decided while the *previous* state was running is
    /// consumed at the boundary, published into [`JobContext::config`], and is no longer
    /// there for this state to observe: its own
    /// [`observe_lifecycle_intent`](JobContext::observe_lifecycle_intent) would answer
    /// [`ObservedIntent::Continue`], because nothing new has been decided since. A state that
    /// learned about a stop only that way would therefore start a final checkpoint, a
    /// replacement cluster or a leader stop that the job had already been told not to start.
    ///
    /// So the boundary asks instead, and this is the question. There is deliberately **no
    /// default body**: a default would be an answer given on behalf of a state by something
    /// that cannot see what the state is about to do, and a state added later would inherit
    /// it silently. Answering is what makes "a consumed stop reaches the state that has to
    /// act on it" a property of the loop rather than of each state remembering to look —
    /// the same argument the refusal gate and the choice of state body are already made on.
    ///
    /// # What the two answers mean
    ///
    /// [`LeavingForStop::Leaves`] is this state's own stop transition, built by invoking the
    /// landed `stop_if_desired*` macro for its family through
    /// [`lifecycle::leaving`](lifecycle::leaving) — never by restating the mapping, so a stop
    /// that arrives as an intent and a stop that arrives as a
    /// [`JobMessage::ConfigUpdate`] cannot come to mean different things.
    ///
    /// [`LeavingForStop::Stays`] hands the state back and lets its body run, and it is a
    /// claim the implementation site has to justify: that nothing this state goes on to do
    /// outruns the stop, either because its body *is* the stop, or because it does nothing
    /// irreversible before handing to a state that answers the same question.
    ///
    /// Staying is **not** discarding. The stop the boundary consumed is left standing on the
    /// job's writer ([`JobContext::hand_stop_to`]) and offered again at this state's own
    /// consumption points — every [`JobWait::recv`], and the boundary of whatever state this
    /// one hands to. So a body that waits, waits on a source that already carries the stop,
    /// and a body that hands on hands to a state that is asked afresh. Without that, a state
    /// whose only content is an unbounded wait would wait out a stop that had already been
    /// decided, which is PR #160 review comment `5362488017`.
    ///
    /// Called only for a job whose lifecycle is M11.D39a's single writer, and only when that
    /// writer has decided the job stops. Under
    /// [`LifecycleMode::LegacyT08`](lifecycle::LifecycleMode::LegacyT08) — production through
    /// M11.T25 — there is no writer, the boundary observes
    /// [`ObservedIntent::Continue`] always, and this is never called at all.
    fn leave_for_stop(self: Box<Self>, config: &JobConfig) -> LeavingForStop;

    async fn next(self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError>;
}

type TransitionFn = Box<dyn Fn(&mut JobContext) + Send>;

pub trait TransitionTo<S: State> {
    fn update_status(&self) -> TransitionFn {
        Box::new(|_| {})
    }
}

pub struct StateHolder {
    state: Box<dyn State>,
    update_fn: TransitionFn,
}

/// What a state does about a stop that was decided before it ran (M11.T25, M11.D39a).
///
/// Returned by [`State::leave_for_stop`] and consumed by [`execute_state`], which either takes
/// the transition or runs the body it was handed back. The state is carried *inside* the
/// "stays" answer rather than kept by the caller, so answering the question is the only way to
/// get the state back: running it without answering is not something a caller can express.
///
/// The three mappings that produce [`Self::Leaves`] live in [`lifecycle::leaving`], one per
/// family of states, and each invokes the landed `stop_if_desired*` macro rather than restating
/// it.
#[must_use = "a stop decided before this state ran is answered by leaving or by a stated \
              reason for staying; dropping the answer is how the stop gets lost"]
pub enum LeavingForStop {
    /// The state leaves now, for the transition it named. Its body does not run.
    Leaves(Transition),
    /// The state's body runs anyway. The implementation site says why the stop is not lost by
    /// letting it.
    Stays(Box<dyn State>),
}

impl LeavingForStop {
    /// Folds one of the landed stop macros' two outcomes into this answer.
    ///
    /// The macros `return` a [`Transition`] from the body they are invoked in and fall through
    /// when the configuration asks for no stop, which is why each helper in
    /// [`lifecycle::leaving`] wraps one in a function whose `Err` is the untouched state.
    fn of<S: State>(answered: Result<Transition, Box<S>>) -> Self {
        match answered {
            Ok(transition) => LeavingForStop::Leaves(transition),
            Err(state) => LeavingForStop::Stays(state),
        }
    }
}

impl Transition {
    #[allow(clippy::multiple_bound_locations)]
    pub fn next<ThisState: State, NextState: State>(t: ThisState, n: NextState) -> Transition
    where
        ThisState: TransitionTo<NextState>,
    {
        Transition::Advance(StateHolder {
            state: Box::new(n),
            update_fn: t.update_status(),
        })
    }
}

#[allow(clippy::needless_lifetimes)]
async fn execute_state<'a>(
    state: Box<dyn State>,
    mut ctx: JobContext<'a>,
) -> (Option<Box<dyn State>>, JobContext<'a>) {
    let state_name = state.name();

    debug!(
        message = "executing state",
        job_id = %ctx.config.id,
        pipeline_id = *ctx.pipeline_info.pipeline_id,
        state = state_name,
        config = format!("{:?}", ctx.config)
    );

    // A refusal the job is *already* under is applied here, before the state body, and not
    // somewhere inside it. Queueing it ahead of the state is not enough: `Compiling` never
    // reads the job's channel, and `Scheduling` increments and persists the generation,
    // stops the job's workers, starts replacements and prepares checkpoint recovery before
    // its first `recv`. Doing this once, for every state, is what makes "a refused
    // configuration is adopted nowhere" a property of the loop rather than of each state
    // remembering to receive before it acts.
    //
    // The refusal is applied through the same [`handle_unhandled_message`] policy the queued
    // message goes through, including its superseded-version check, so a row repaired while
    // the previous state was running still saves the job.
    let known_refusal = ctx.refusal_gate.take();
    let gated = match known_refusal {
        Some(refusal) => ctx.handle(JobMessage::ConfigRefused(refusal)),
        None => Ok(()),
    };

    // The same boundary is D39a's first consumption point, for exactly the reason above:
    // `Compiling` never receives, and `Scheduling` does its generation write, worker
    // teardown, worker start and checkpoint recovery before its first `recv`, so a state
    // boundary is "before an irreversible phase". Under `LifecycleMode::LegacyT08` —
    // production through M11.T25 — there is no actor, this is a no-op, and the gate above
    // is the mechanism, unchanged.
    //
    // What is consumed here is consumed *from* the state that runs next: an intent is decided
    // once, so a stop read at this boundary is one the state's own consumption points will
    // never report. It is therefore not enough to publish it into `ctx.config` and hope the
    // state looks — `Restarting` starts its final checkpoint before its first look,
    // `LeaderRescaling` and `LeaderCheckpointStopping` stop the leader without ever looking,
    // and `CheckpointStopping` would carry on taking a checkpoint an operator had asked it to
    // abandon. So the boundary hands the outcome to the state, through `leave_for_stop`, which
    // every state must implement and none can default. `execute_state` holds a
    // `Box<dyn State>` and could not name the transition itself: what leaving means is the
    // state's own, which is why it is the state that answers — but *whether* it is asked is
    // the loop's, which is why the question is here.
    //
    // And a state that answers "not here" has not answered it away: `hand_stop_to` leaves the
    // stop standing on the job's writer, so the state's own waits and the next state's boundary
    // are offered it again. That is what makes `Stays` a routing decision rather than a
    // discard — PR #160 review comment `5362488017`.
    let observed = gated
        .and_then(|()| ctx.observe_lifecycle_intent(ConsumptionPoint::BeforeIrreversiblePhase));

    let leaving = match observed {
        Err(refused) => Err(refused),
        // Nothing was decided, or what was decided leaves the job doing what it was doing.
        Ok(ObservedIntent::Continue) => Ok(LeavingForStop::Stays(state)),
        // A configuration adopted between states. Publishing it is not the whole of adopting
        // it — a running job's workers, its restart nonce and its parallelism are decided by
        // comparing it with the one it replaced — and this boundary is not where that
        // comparison can be made. So it is left standing for the state's own consumption
        // points, exactly as a stop a state declines to leave for is: PR #160 review comment
        // `5365261487`.
        Ok(ObservedIntent::Adopted(superseded)) => {
            ctx.hand_adoption_to(superseded);
            Ok(LeavingForStop::Stays(state))
        }
        Ok(ObservedIntent::Stop) => Ok(ctx.hand_stop_to(state)),
    };

    // One place a state body is entered from, and everything that stands before a job's next
    // irreversible work stands before it: the gate, the writer, and the state's own answer to
    // what the writer decided. Which body it is, is the job's lifecycle mechanism's to choose,
    // and choosing it here rather than inside each state is the same argument as the reads
    // above — a state that had to remember to ask would be a state that could forget.
    // Production takes the legacy branch for every state of every job:
    // `runs_fenced_lifecycle` is false unless the job was built with the D39a single writer,
    // which no production construction site does.
    let outcome = match leaving {
        Err(refused) => Err(refused),
        Ok(LeavingForStop::Leaves(transition)) => Ok(transition),
        Ok(LeavingForStop::Stays(state)) => scheduling::run_state_body(state, &mut ctx).await,
    };

    let next: Option<Box<dyn State>> = match outcome {
        Ok(Transition::Advance(s)) => {
            info!(
                message = "state transition",
                job_id = %ctx.config.id,
                pipeline_id = *ctx.pipeline_info.pipeline_id,
                from = state_name,
                to = s.state.name(),
                duration_ms = ctx.last_transitioned_at.elapsed().as_millis()
            );

            log_event!(
                "state_transition",
                {
                    "service": "controller",
                    "job_id": ctx.config.id,
                    "from": state_name,
                    "to": s.state.name(),
                    "scheduler": &config().controller.scheduler,
                },
                ["duration_ms" => ctx.last_transitioned_at.elapsed().as_millis() as f64]
            );

            (s.update_fn)(&mut ctx);
            ctx.retries_attempted = 0;
            ctx.last_transitioned_at = Instant::now();

            Some(s.state)
        }
        Ok(Transition::Stop) => None,
        Err(StateError::FatalError {
            message,
            source,
            domain,
            // Read where it is acted on, not here: by this point the job is failing, and every
            // fatal reason fails it the same way.
            provenance: _,
        }) => {
            error!(
                message = "fatal state error",
                job_id = %ctx.config.id,
                pipeline_id = *ctx.pipeline_info.pipeline_id,
                state = state_name,
                error_message = message,
                error = format!("{:?}", source)
            );
            log_event!(
                "fatal_state_error",
                {
                    "service": "controller",
                    "job_id": ctx.config.id,
                    "state": state_name,
                    "error_message": message,
                    "domain": domain.as_str(),
                    "error": format!("{:?}", source),
                    "retries": 0,
                }
            );
            // The job is failing; there is nothing left for the gate to protect, and a
            // refusal a state has just turned fatal through its own channel must not be
            // turned fatal a second time before `Failing` gets to run.
            ctx.refusal_gate.disarm();
            ctx.status.failure_message = Some(message);
            ctx.status.failure_domain = Some(domain.as_str().to_string());
            ctx.status.finish_time = Some(OffsetDateTime::now_utc());
            // Transition to Failing state for graceful shutdown before Failed
            let s: Box<dyn State> = Box::new(Failing {});
            Some(s)
        }
        Err(StateError::RetryableError {
            message,
            source,
            domain,
            retries: 0 | 1,
            ..
        }) => {
            error!(
                message = "exhausted in-state retries, moving to recovering",
                job_id = %ctx.config.id,
                pipeline_id = *ctx.pipeline_info.pipeline_id,
                state = state_name,
                error_message = message,
                error = format!("{:?}", source)
            );
            ctx.status.restarts += 1;
            ctx.retries_attempted = 0;
            let s: Box<dyn State> = Box::new(Recovering {
                source,
                reason: message,
                domain,
            });
            Some(s)
        }
        Err(StateError::RetryableError {
            state,
            message,
            source,
            retries,
            domain,
        }) => {
            error!(
                message = "retryable state error",
                job_id = %ctx.config.id,
                pipeline_id = *ctx.pipeline_info.pipeline_id,
                state = state_name,
                error_message = message,
                error = format!("{:?}", source),
                retries,
            );
            log_event!(
                "state_error",
                {
                    "service": "controller",
                    "job_id": ctx.config.id,
                    "state": state_name,
                    "error_message": message,
                    "domain": domain.as_str(),
                    "error": format!("{:?}", source),
                },
                ["retries" => retries]
            );

            state_backoff(
                ctx.retries_attempted,
                &ctx.config.id,
                &ctx.pipeline_info.pipeline_id,
            )
            .await;

            ctx.retries_attempted += 1;
            Some(state)
        }
    };

    if let Some(s) = &next {
        ctx.status.state = s.name().to_string();

        ctx.status
            .update_db(&ctx.db)
            .await
            .expect("Failed to update status");
    }

    (next, ctx)
}

pub(crate) async fn state_backoff(retries_attempted: usize, job_id: &str, pipeline_id: &str) {
    let pipeline_config = &config().pipeline;
    let base = *pipeline_config.state_initial_backoff;
    let max = *pipeline_config.state_max_backoff;
    let exp_backoff = max.min(base * 2u32.pow(retries_attempted as u32));

    let backoff = exp_backoff / 2
        + Duration::from_micros(rand::Rng::random_range(
            &mut rand::rng(),
            0..exp_backoff.as_micros() as u64 / 2,
        ));

    debug!(
        %job_id,
        pipeline_id,
        "waiting {}ms to retry",
        backoff.as_millis()
    );
    tokio::time::sleep(backoff).await;
}

#[allow(clippy::too_many_arguments)]
async fn run_to_completion(
    job_config_and_status: Arc<RwLock<(JobConfig, AppliedStatus)>>,
    execution_selector: StateBackendSelector,
    cluster_id: Arc<String>,
    refusal_gate: RefusalGate,
    // The job's D39a single writer, or `None` under `LifecycleMode::LegacyT08`. Built by
    // `StateMachine::start` from the job's own `JobLifecycle`, so a task and its actor have
    // the same lifetime: the watermark of what has been decided belongs to the task that
    // decided it.
    lifecycle_actor: Option<LifecycleActor>,
    pipeline_info: Arc<PipelineInfo>,
    mut program: LogicalProgram,
    mut status: JobStatus,
    mut state: Box<dyn State>,
    db: DatabaseSource,
    mut rx: Receiver<JobMessage>,
    scheduler: Arc<dyn Scheduler>,
    metrics: Arc<tokio::sync::RwLock<HashMap<Arc<String>, JobMetrics>>>,
) {
    let job_config = job_config_and_status.read().unwrap().0.clone();

    let leader_manager = if let Some(ctx) = &status.state_context.leader {
        LeaderManager::connect(
            JobId(job_config.id.clone()),
            pipeline_info.pipeline_id.clone(),
            ctx.generation,
            ctx.worker_id,
            ctx.rpc_address.clone(),
            config().controller.connect_timeout.as_deref().copied(),
            // The reconnect this finding is about: a controller that has just been
            // rebuilt asks the live leader what backend it is running before it attaches
            // to it, so a reconstruction that disagrees cannot start administering a job
            // that is still running under something else.
            execution_selector,
        )
        .await
        .map(Some)
        .unwrap_or_else(|e| {
            warn!(job_id = %job_config.id,
                    pipeline_id = *pipeline_info.pipeline_id,
                    leader_ctx =? ctx,
                    error =? e,
                    "failed to connect to leader worker on start");
            None
        })
    } else {
        None
    };

    let mut ctx = JobContext {
        config: job_config,
        execution_selector,
        cluster_id,
        pipeline_info,
        status: &mut status,
        program: &mut program,
        db: db.clone(),
        scheduler,
        rx: &mut rx,
        refusal_gate,
        lifecycle_actor,
        retries_attempted: 0,
        job_controller: None,
        leader_manager,
        last_transitioned_at: Instant::now(),
        metrics,
    };

    loop {
        job_config_and_status.write().unwrap().1 = AppliedStatus::Applied;
        match execute_state(state, ctx).await {
            (Some(new_state), new_ctx) => {
                state = new_state;
                ctx = new_ctx;
            }
            (None, _) => break,
        }

        // D39a: under `FencedV2` this task is the only writer of the job's configuration.
        // The shared cell is the *other* writer's — the configuration poll's — and under
        // that mode the poll deliberately never writes it, so refreshing from it here would
        // undo whatever the actor has just published. Under `LegacyT08`, which is what
        // production runs, the refresh is exactly as M11.T08 landed it.
        if ctx.lifecycle_actor.is_none() {
            let refreshed = job_config_and_status.read().unwrap().0.clone();
            if let Some(adopted) = adopt_refreshed_config(
                refreshed,
                execution_selector,
                &ctx.pipeline_info.pipeline_id,
            ) {
                ctx.config = adopted;
            }
        }
    }
}

/// Decides whether the *next* state adopts the configuration the state machine's shared
/// cell now holds, returning `Some(config)` to adopt it and `None` to keep the one the
/// last state ran with.
///
/// This is the one place a transition's view of the job's configuration is replaced.
/// [`StateMachine::update`] refuses a row that names a different backend before it ever
/// reaches that cell, so `refreshed` should never disagree; keeping the configuration this
/// execution was validated with — rather than adopting the new one and letting the next
/// state treat it as its baseline — is what makes the refusal a property of the loop and
/// not of the one writer that enforces it today.
fn adopt_refreshed_config(
    refreshed: JobConfig,
    execution_selector: StateBackendSelector,
    pipeline_id: &str,
) -> Option<JobConfig> {
    if refreshed.state_backend == execution_selector {
        return Some(refreshed);
    }

    error!(
        job_id = %refreshed.id,
        pipeline_id,
        running = %execution_selector,
        requested = %refreshed.state_backend,
        "refusing to adopt a job configuration that changes the state backend"
    );
    None
}

/// Whether the job's state task has taken up the poll's latest word about the job.
///
/// [`AppliedStatus::NotApplied`] is what makes [`StateMachine::restart_if_needed`] start a job
/// whose status does not otherwise say it should be running: there is something for a state
/// task to take up and no task to take it. [`run_to_completion`] clears it at the head of
/// every state, so a task that has come up has by definition taken up whatever was there —
/// which is also why a start that could not load the job's program leaves this untouched and
/// is retried by the next poll.
///
/// The two lifecycle mechanisms differ in *what* the poll's word is, and therefore in what
/// records it untaken, but not in what this means. Under
/// [`LifecycleMode::LegacyT08`](lifecycle::LifecycleMode::LegacyT08) the word is the
/// [`JobConfig`] in the cell beside this flag, and storing a changed one records it. Under the
/// D39a mechanism the poll writes no configuration at all — its word is the
/// [`LifecycleIntent`] it leaves in the job's [`IntentMailbox`](lifecycle::IntentMailbox) — so
/// a submission that needs a state task records it here instead. On neither path is
/// this a configuration write: the flag says whether a task has the poll's word, never what
/// the word is.
#[derive(Copy, Clone)]
enum AppliedStatus {
    Applied,
    NotApplied,
}

/// What has been done about a refusal, on the sending side.
///
/// This is what makes repeated refusal delivery idempotent: the row stays bad until an
/// operator fixes it, so it is polled again every 500ms and must not produce a message
/// each time.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum RefusalDelivery {
    /// A [`JobMessage::ConfigRefused`] carrying this refusal is with the job's queue.
    /// Nothing more to send.
    ///
    /// Only [`Delivery::Delivered`] reaches this. Having no live queue to give the refusal
    /// to used to count as well, which is how a job could be recorded as told about a
    /// refusal that nothing had ever received.
    Sent,
    /// Nothing holds this refusal yet: the job's queue was full, or the job has no state
    /// task to give it to at all. Nothing is lost — the same refusal, at the same version,
    /// is offered again on the next poll, and a repair, a different refusal, or a stop
    /// supersedes it exactly as it supersedes a [`Self::Sent`] one.
    Pending,
    /// No refusal message exists for this one and none will be sent: the job is being
    /// stopped instead, which is what the refused row itself asked for
    /// (see [`StateMachine::apply_refused_row`]).
    AnsweredByStop,
}

/// The refusal a job's state task must apply *before* it runs a state.
///
/// A refusal reaches the state task two ways, and only one of them is prompt. The queued
/// [`JobMessage::ConfigRefused`] wakes a state that is already blocked on the job's channel
/// — which is what fails a long-running [`Running`] job — but a state that acts before it
/// receives never sees it. [`Compiling`] does not receive at all, and [`Scheduling`]
/// increments and persists the job's generation, stops its workers, starts replacements and
/// prepares checkpoint recovery before its first `ctx.rx.recv`. Queueing a refusal ahead of
/// such a state is therefore not the same as applying it: "the refusal is applied before the
/// job is rescheduled" is a claim about the operation, not about a queue position.
///
/// So every refusal the state machine intends to *fail* the job with is published here as
/// well, and [`execute_state`] applies it ahead of every state body it runs — including the
/// very first state of a task that the refused row's own poll started.
///
/// Only refusals that are to be delivered as failures are published. One a stop is
/// answering ([`RefusalDelivery::AnsweredByStop`]) is withdrawn instead, because failing the
/// job would destroy exactly the final-checkpoint semantics that stop exists for.
///
/// Held per job and cloned into that job's state task. There is no registry, no static and
/// no global, for the same reason [`StateMachine::execution_selector`] has none.
///
/// # Publishing a refusal and doing irreversible scheduling work are one another's alternatives
///
/// Reading the gate once, at the state boundary, is a *snapshot*: [`Self::take`] clones what
/// it finds and releases the lock, and [`Scheduling`] then runs a long, irreversible preamble
/// — persist the incremented generation, tear the live cluster down, start replacements,
/// prepare checkpoint recovery — with several awaits in it and no further reference to the
/// gate. A refusal published anywhere in that interval would be seen only by the *next* state
/// boundary, which is after all of it. Adding a second read somewhere inside the preamble
/// only moves the interval; there is always a last check and a first effect after it.
///
/// So the two are made mutually exclusive instead of merely ordered. [`Self::admission`] is
/// the job's serialization point: [`JobContext::admit_irreversible_scheduling`] holds it
/// across each region of irreversible work, and [`StateMachine::refuse_config`] must take it
/// — without waiting — to publish. Whichever gets it first decides the outcome for that whole
/// region:
///
/// * the publisher first, and the gate is read *under the admission* the region then
///   acquires, so the region is refused before its first effect; or
/// * the region first, and publication finds the admission taken, changes nothing at all,
///   and is offered again by the next poll — reaching the job strictly after the region it
///   could no longer have stopped, where the *next* crossing reads it from the gate.
///
/// There is no third interleaving, which is what makes this a property of the operation
/// rather than of how many places remembered to re-check.
///
/// # The gate is the decision; the channel is only delivery
///
/// [`Scheduling`] has more than one such region, separated by phases that wait on the job's
/// message channel. Those phases end as soon as *enough* messages have arrived — enough
/// worker connects for the slots, enough task starts for the tasks — and they end on the
/// message that made the count, not on the last message in the queue. A refusal published
/// while such a phase was waiting can therefore be sitting behind the messages it just
/// consumed, still unread, when it breaks.
///
/// Draining or scanning the queue at that point would be the weaker answer, and not only
/// because it is more work: a refusal that could not be *delivered* at all — the job's queue
/// was full, so [`RefusalDelivery::Pending`] — is not in the queue to be found, and the same
/// poll that could not deliver it did publish it here. `refuse_config` writes the gate before
/// it offers the message and regardless of what becomes of it, so the gate is the record that
/// exists in every case. Every crossing reads *that*, which is what makes the outcome
/// independent of queue capacity and queue order alike.
#[derive(Clone, Default)]
pub(crate) struct RefusalGate {
    /// The refusal the state machine is under now, shared with the job's state task so a
    /// refusal raised after the task started is seen at its next state boundary.
    current: Arc<RwLock<Option<RefusedConfig>>>,
    /// Exclusive access to the job's destructive scheduling work, shared with the job's
    /// state task. See the type's documentation: this is what makes publishing a refusal
    /// and entering [`Scheduling`]'s preamble alternatives rather than a race.
    ///
    /// A `tokio` mutex rather than a `std` one for two reasons that both matter here: the
    /// guard is held across the preamble's awaits and so must be `Send`, and it does not
    /// poison, so a state body that panics under it leaves the next attempt able to acquire
    /// it rather than wedging the job for the life of the controller.
    admission: Arc<tokio::sync::Mutex<()>>,
    /// The highest refusal version this task has already turned fatal.
    ///
    /// Per task rather than shared. It is what stops the gate from failing the job a second
    /// time on its way through [`Failing`] to [`Failed`], while a task started later still
    /// applies a refusal an earlier one already did.
    acted: u64,
}

/// Exclusive access to one region of a job's irreversible scheduling work.
///
/// Held by [`Scheduling`] across each such region, and taken — without ever waiting — by
/// [`StateMachine::refuse_config`] for the instant it publishes. See [`RefusalGate`] for why
/// those two are the same lock.
///
/// The guard is what makes the region rather than any individual statement the unit: every
/// effect between the acquisition and the drop is inside it, including effects added later,
/// so the guarantee does not depend on a future author remembering to re-check anything
/// *within* a region. What it cannot speak for is an effect written outside every region;
/// that is what
/// `the_source_of_scheduling_next_keeps_every_irreversible_effect_inside_an_admitted_region`
/// pins, and why that pin enumerates the effects by name.
pub(crate) struct Admission {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

impl Admission {
    /// Performs one irreversible scheduling effect inside the admitted region.
    ///
    /// Every irreversible effect of [`Scheduling`] goes through here, and needing an
    /// `&Admission` to call it is the point: an effect can only be written where a guard is
    /// in scope, and a guard is only in scope inside a region no refusal can be published
    /// into. `what` names the effect for the log and for
    /// `the_source_of_scheduling_next_keeps_every_irreversible_effect_inside_an_admitted_region`,
    /// which pins the set of them and the boundaries they sit between.
    pub(crate) async fn effect<F: std::future::Future>(
        &self,
        what: &'static str,
        effect: F,
    ) -> F::Output {
        debug!(
            effect = what,
            "performing an irreversible scheduling effect under admission"
        );
        effect.await
    }
}

/// Runs a region that has issued work the controller cannot revoke, and does not release its
/// [`Admission`] until that work has settled — including when this future is dropped.
///
/// # Why a region cannot simply be cancelled
///
/// Round 11 made the `StartExecution` fan-out's requests children of the state task rather than
/// spawned tasks, so that every exit from the region took the requests with it. That is true of
/// the *client* futures and only of them. Dropping a `tonic` client future resets its stream; it
/// does not revoke work the server has already begun. The production worker's handler takes a
/// `std::sync::Mutex` as its first statement and, on the `Idle` branch, sets the phase to
/// `Initializing` and spawns initialization without reaching a single `.await`
/// (`arroyo-worker/src/lib.rs`, `WorkerGrpc::start_execution`). A future blocked *inside* `poll`
/// cannot be dropped, so a handler that was waiting for that lock when the controller gave up on
/// it still runs to completion, and still starts the worker, after the reset.
///
/// So "the request was cancelled" is not an observation the controller can make, and the safety
/// claim must not rest on it. What the controller *can* know is whether an RPC it issued has
/// come back with an outcome. This type turns that into the invariant: **the admission is not
/// released while any work the region issued is still unsettled**, so at the first instant a
/// refusal can be published every worker's answer is already in hand and there is nothing left
/// in flight for the refusal to race.
///
/// # What happens when the region is dropped
///
/// The job's state task is dropped as a whole when the controller's shutdown token fires — see
/// `ShutdownGuard::into_spawn_task`, whose `select!` drops `run_to_completion` mid-flight — and
/// then the admission would go at the same instant as the requests. The region is therefore
/// owned by this future, and on drop it is moved into a task that finishes it. The task is
/// detached, but the round-10 hole it might look like is precisely the one it cannot reopen:
/// what round 10 detached was a *request*, which then outlived the admission that authorised it;
/// what is detached here is the *admission itself*, wrapped around those requests. A refusal
/// cannot be published while that task lives, because the task is holding the lock a publication
/// needs — and [`RefusalGate::admit_publication`] never waits, so contention with it defers a
/// refusal to the next poll rather than blocking anything (round 6's rule).
///
/// # Where the rescued authority goes
///
/// Holding the admission until the requests settle is only half of what a dropped region owes.
/// The other half is *who ends up with it*: the phase that raised the region has gone with the
/// task, so if this rescue simply dropped what it rescued, a controller that has a per-job
/// settlement owner would be handed nothing on the one path where the phase cannot hand
/// anything over itself. `rescue` is that seam. It is `None` for a caller with no owner —
/// every caller through M11.T25, including the selected M11.T08 path, for which this behaves
/// exactly as it did before the parameter existed — and otherwise it is given the settled
/// [`Admission`] to pass to the job's owner together with the inventory of what was issued.
/// See `scheduling::fanout::AttemptLedger::settlement_rescue`.
///
/// What a rescue must *not* do is release that admission on behalf of a phase that no longer
/// exists. It is handed the authority precisely because there is nobody else left holding it,
/// so an owner that declines leaves the obligation with nobody — see
/// `scheduling::fanout::settlement::retain_without_a_phase`, which is why the rescue answers
/// every arm of what the hand-over returns rather than dropping it.
///
/// # What bounds the region
///
/// Not the RPC deadline alone. The worker channels are built with `Endpoint::timeout` (90s in
/// `handle_worker_connect`), but the fan-out treats an ambiguous transport outcome as a reason
/// to *retry* the same attempt ID, so one deadline expiring only started another wait: this
/// paragraph used to claim a 90s bound that the retry loop had already made false, and a worker
/// that stayed unreachable held the region — and with it the deferral of any refusal for that
/// job — indefinitely.
///
/// The bound is now the fan-out's own terminal path: at most
/// `START_EXECUTION_RECONCILE_ATTEMPTS` further attempts, each bounded by that same deadline,
/// after which the attempt is given up and the region ends. See `start_execution_on_workers`
/// for why ending it is a statement about the peer's handler rather than about elapsed time.
///
/// # Two cases it does not cover, both deliberate
///
/// * **A panic inside the region.** Its future is poisoned and cannot be resumed, so unwinding
///   drops the requests with the admission, exactly as before this type existed. A `Scheduling`
///   that panics is failing the job anyway, and the fan-out's own path has no panicking
///   operation left on it.
/// * **No Tokio runtime at drop time.** Then the controller is already gone and nothing is left
///   that could publish a refusal.
pub(crate) async fn settle_under_admission<T, F>(
    admission: Admission,
    region: impl FnOnce(Admission) -> F,
    rescue: Option<SettlementRescue>,
) -> (Admission, T)
where
    F: std::future::Future<Output = (Admission, T)> + Send + 'static,
    T: Send + 'static,
{
    SettlingUnderAdmission {
        region: Some(Box::pin(region(admission))),
        rescue,
    }
    .await
}

/// What a rescued region hands the job's lifecycle authority to, once its requests have
/// settled and the phase that issued them is gone.
///
/// Owned and `'static` on purpose: the rescue runs in a detached task, so anything borrowed
/// from the phase — which is what was dropped — could not be named here. That is the whole
/// reason `PhaseContext::settlement_owner` answers with an `Arc` rather than a borrow.
///
/// Taking the [`Admission`] by value makes the rescue the last party that can decide anything
/// about it: there is no path on which this returns and the authority is still somewhere a
/// phase could reach. What the rescue owes in exchange is a decision for *every* way the
/// hand-over can end, including the one where nobody takes the obligation.
pub(crate) type SettlementRescue = Box<dyn FnOnce(Admission) + Send>;

/// The owner [`settle_under_admission`] wraps a region in. Constructed only there.
///
/// The region hands the [`Admission`] back as part of its output rather than merely borrowing
/// it, which is what makes "the admission is inside the thing that gets rescued" a fact of the
/// type rather than a convention: a future that did not own an admission could not produce one.
struct SettlingUnderAdmission<T: Send + 'static> {
    /// `None` once the region has settled, which is what tells [`Drop`] there is nothing left
    /// to rescue.
    region: Option<
        std::pin::Pin<Box<dyn std::future::Future<Output = (Admission, T)> + Send + 'static>>,
    >,
    /// Where a rescued admission goes. `None` releases it, which is what every caller through
    /// M11.T25 asks for.
    rescue: Option<SettlementRescue>,
}

impl<T: Send + 'static> std::future::Future for SettlingUnderAdmission<T> {
    type Output = (Admission, T);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = &mut *self;
        let region = this
            .region
            .as_mut()
            .expect("a region that has settled is not polled again");
        match region.as_mut().poll(cx) {
            std::task::Poll::Ready(settled) => {
                this.region = None;
                std::task::Poll::Ready(settled)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl<T: Send + 'static> Drop for SettlingUnderAdmission<T> {
    fn drop(&mut self) {
        let Some(region) = self.region.take() else {
            // Settled: the admission has already been handed back to the caller, which drops it
            // where it means to.
            return;
        };

        if std::thread::panicking() {
            // A poisoned future cannot be resumed. See the type's documentation.
            return;
        }

        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            error!(
                "a region of irreversible scheduling work was dropped with no Tokio runtime to \
                 finish it in; its admission is released with it"
            );
            return;
        };

        warn!(
            "a region of irreversible scheduling work was dropped while the requests it issued \
             were still unsettled; holding the job's admission until they are, so that no \
             refusal can be published behind them"
        );
        let rescue = self.rescue.take();
        runtime.spawn(async move {
            // The admission is inside `region`, so this is the first moment it exists as a
            // value again — and the last moment anything can be done with it.
            let (admission, _outcome) = region.await;
            match rescue {
                // The obligation reaches the job's settlement owner even here. This is the
                // only path on which it can: the phase that issued the requests went with the
                // state task, so nothing else is left that could hand its authority over.
                Some(rescue) => rescue(admission),
                None => drop(admission),
            }
        });
    }
}

impl RefusalGate {
    /// Takes the job's scheduling admission for a publication, if the job is not in the
    /// middle of a region of irreversible scheduling work.
    ///
    /// Never waits, and must never be made to: the update thread calls
    /// [`StateMachine::refuse_config`] while it holds the global job map, so blocking here
    /// would block every other job's poll — the failure mode round 4 removed from this path
    /// and the reason `refuse_config` is not `async`. `None` means such a region is in flight
    /// and the caller must leave the refusal [`RefusalDelivery::Pending`], changing *nothing*
    /// — not the refusal version, not the recorded delivery — so the next poll offers exactly
    /// the same refusal again.
    ///
    /// How long `None` can persist is bounded by the region in flight, and the longest of them
    /// is the preamble's wait for slots (`pipeline.worker-startup-time`). The deferral is the
    /// price of the interlock and is paid by one job's refusal latency only: nothing here
    /// blocks the caller, no other job is affected, and the refusal itself is untouched.
    fn admit_publication(&self) -> Option<Admission> {
        Arc::clone(&self.admission)
            .try_lock_owned()
            .ok()
            .map(|guard| Admission { _guard: guard })
    }

    /// Admits the caller to one region of the job's irreversible scheduling work, and reports
    /// the refusal that must stop it.
    ///
    /// Waiting here is bounded and safe: the only other holder is a publication, which is
    /// synchronous and does not await, or a region held by this same task, which must have
    /// dropped before the next is entered. The refusal is read *after* the admission is held,
    /// so it is the last thing any publisher can have said before this region became closed to
    /// them — and it is read from the gate rather than from the job's queue, so a refusal a
    /// preceding receive phase left unread, or that no queue ever took, stops this region all
    /// the same.
    async fn admit_scheduling(&mut self) -> (Admission, Option<RefusedConfig>) {
        let guard = Arc::clone(&self.admission).lock_owned().await;
        let refusal = self.take();
        (Admission { _guard: guard }, refusal)
    }

    /// Publishes a refusal for the job's state task to apply before its next state.
    ///
    /// Only callable with the job's scheduling admission in hand, which is the whole of the
    /// interlock: a refusal cannot appear while a region of irreversible work is running, so
    /// a region that started with the gate clear can never be overtaken by one.
    fn publish(&self, _admission: &Admission, refusal: RefusedConfig) {
        *self.current.write().unwrap() = Some(refusal);
    }

    /// Withdraws whatever was published, because it no longer describes the job or is being
    /// answered by a stop rather than by a failure.
    ///
    /// Deliberately not admitted. Withdrawal only ever *removes* a reason to stop the job, so
    /// it cannot make a preamble run for a configuration that is refused; and it has to work
    /// while a preamble is in flight, because the row the operator has just repaired is
    /// exactly the one that must stop gating the states after it.
    fn withdraw(&self) {
        *self.current.write().unwrap() = None;
    }

    /// The refusal this task must apply before it runs another state, if any.
    ///
    /// Each refusal is returned at most once. `is_current` is checked here as well as in
    /// [`RefusedConfig::into_current_error`], so a row the operator repaired while the
    /// previous state was running is never failed for a refusal that no longer exists.
    fn take(&mut self) -> Option<RefusedConfig> {
        let refusal = self.current.read().unwrap().clone()?;
        if !refusal.is_current() || refusal.version() <= self.acted {
            return None;
        }
        self.acted = refusal.version();
        Some(refusal)
    }

    /// Records that the job is already failing, so a refusal one of its own states has just
    /// turned fatal is not turned fatal again before [`Failing`] runs.
    ///
    /// The gate and the job's message channel are two routes to one policy, and the job may
    /// only be failed once per refusal whichever route reached it first. A refusal raised
    /// *after* this — at a higher version — still gates, which is what keeps a job that
    /// restarts out of [`Failed`] from restarting into a configuration that is refused now.
    fn disarm(&mut self) {
        if let Some(refusal) = self.current.read().unwrap().as_ref() {
            self.acted = self.acted.max(refusal.version());
        }
    }
}

/// A refused configuration, its version, and what has been done about it.
///
/// `version` is the sending half of [`crate::RefusedConfig`]: a queued refusal cannot be
/// retracted, so the only way a repaired row can stop an already-queued refusal from
/// failing the job is for the receiver to be able to tell that the refusal it is holding
/// is not the one the state machine is now under. Every change of refusal state — raising
/// a different one, clearing one because the row was repaired, or handing one over to a
/// stop — advances [`StateMachine::refusal_version`], and a message stamped with an older
/// version is discarded on receipt.
struct Refusal {
    error: StateBackendError,
    version: u64,
    delivery: RefusalDelivery,
}

/// What became of a message offered to a job's own queue, without ever waiting for it.
///
/// Waiting is the thing being avoided: the update thread offers these while it holds the
/// global job map, so anything that blocks here blocks every other job's poll and every
/// RPC that needs the map.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Delivery {
    /// The job's state machine has the message.
    Delivered,
    /// The job's queue is full. Nothing is lost — the caller keeps the request and offers
    /// it again on the next poll — but nothing has been applied either.
    Full,
    /// There is no running state machine to give it to.
    Inactive,
}

pub struct StateMachine {
    tx: Option<Sender<JobMessage>>,
    config: Arc<RwLock<(JobConfig, AppliedStatus)>>,
    /// The state backend this job is running with, read once from the row that created
    /// the state machine and never reassigned.
    ///
    /// This is the job's authoritative selector. [`Self::config`] is replaced whenever the
    /// update thread polls a changed row, and the running state machine adopts it after
    /// every transition, so it can never be the authority for a value that is fixed for
    /// the life of the job's state. Holding the selector separately is what lets
    /// [`Self::update`] classify a polled row *before* the shared configuration is
    /// overwritten.
    ///
    /// Per job, and owned by that job's state machine: no static, thread-local,
    /// environment, or configuration fallback exists (M11.T08d).
    execution_selector: StateBackendSelector,
    /// The identity of the controller's cluster, handed to every state task this machine
    /// starts. See [`JobContext::cluster_id`].
    cluster_id: Arc<String>,
    /// The refusal the job's configuration is currently under, if any.
    ///
    /// One refusal, not one per poll. The row stays bad until an operator fixes it, so a
    /// message per 500ms poll would repeat forever; and because each of those sends
    /// awaited a bounded per-job channel while the global job map was locked, a job whose
    /// consumer was slow could stop every other job on the cluster from being polled and
    /// block the RPC paths that need the map. Holding the refusal here instead makes
    /// delivery idempotent: it is offered until it is taken, then never again unless it
    /// changes.
    refusal: Option<Refusal>,
    /// The version of the refusal the job is currently under.
    ///
    /// Shared with every [`crate::RefusedConfig`] this state machine has ever queued, so a
    /// refusal that has not been read yet can still be told apart from the one that
    /// describes the job now. Advanced whenever the refusal state changes; never reset.
    ///
    /// It is per job and threaded through the job's own messages — no static, thread-local
    /// or global registry — for the same reason [`Self::execution_selector`] is.
    refusal_version: Arc<AtomicU64>,
    /// The same refusal, on the receiving side, for the job's state task to apply *before*
    /// it runs a state rather than only when a state happens to read its channel.
    ///
    /// The job's queue and this cell are two routes to one policy. The queue is the prompt
    /// one — it wakes a state already blocked on `recv` — and this one is the safe one: it
    /// is consulted before every state body, so a refusal that is known when a state is
    /// about to run stops it, whether or not that state ever receives. See [`RefusalGate`].
    refusal_gate: RefusalGate,
    /// Which mechanism decides this job's lifecycle transitions (M11.T25f).
    ///
    /// Fixed when the state machine is created, from
    /// [`LifecycleMode::SELECTED`](lifecycle::LifecycleMode::SELECTED), so a job cannot
    /// change hands halfway through its own lifecycle. Production is
    /// [`JobLifecycle::LegacyT08`] for the whole of M11.T25 and every field above stays
    /// exactly as M11.T08 landed it; the alternative is reachable only by constructing a
    /// state machine with [`JobLifecycle::for_mode`] directly, which nothing outside a test
    /// module does.
    lifecycle: JobLifecycle,
    pub(crate) state: Arc<RwLock<String>>,
    metrics: Arc<tokio::sync::RwLock<HashMap<Arc<String>, JobMetrics>>>,
    db: DatabaseSource,
    scheduler: Arc<dyn Scheduler>,
}

impl StateMachine {
    /// Creates the state machine for a job the controller has just picked up.
    ///
    /// The selector comes from [`PolledJob::execution_selector`], which is recovered from
    /// the job's own execution record rather than read out of the configuration row. That
    /// is what stops a controller that has just been restarted from re-baselining a
    /// running job's backend from a row an operator edited while it was down.
    ///
    /// A job can also be picked up *while already refused* — the edit and the controller
    /// restart happen in either order — so a refusal that arrived with the row is applied
    /// to the new state machine exactly as it would be to one that had been there all
    /// along. Before this, a refused row with no state machine was skipped, which meant a
    /// still-running job was neither adopted nor failed.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        polled: PolledJob,
        status: JobStatus,
        db: DatabaseSource,
        scheduler: Arc<dyn Scheduler>,
        cluster_id: Arc<String>,
        shutdown_guard: ShutdownGuard,
        metrics: Arc<tokio::sync::RwLock<HashMap<Arc<String>, JobMetrics>>>,
    ) -> Self {
        let job_id = Arc::clone(&status.id);
        // The one production site at which a job's lifecycle mechanism is chosen. It is
        // `LifecycleMode::SELECTED`, which is derived exhaustively from the enum and is
        // `LegacyT08` for the whole of M11.T25, so no production job runs the D39a path.
        // `no_production_path_selects_the_fenced_v2_lifecycle` pins that this is the only
        // such site and what it passes.
        let lifecycle = JobLifecycle::for_mode(lifecycle::LifecycleMode::SELECTED, job_id);

        let PolledJob {
            execution_selector,
            config,
            refusal,
        } = polled;

        let mut this = Self {
            tx: None,
            config: Arc::new(RwLock::new((config, AppliedStatus::NotApplied))),
            execution_selector,
            cluster_id,
            refusal: None,
            refusal_version: Arc::new(AtomicU64::new(0)),
            refusal_gate: RefusalGate::default(),
            lifecycle,
            state: Arc::new(RwLock::new(status.state.clone())),
            metrics,
            db,
            scheduler,
        };

        // Which of the two mechanisms this job runs under, decided once, above, and read
        // here rather than at each of the three sites below — so the poll cannot record
        // through one mechanism and act through the other.
        let fenced = this.lifecycle.intents().map(Arc::clone);

        // Recorded *before* the adoption starts the job's state task, and separately from
        // acting on it below.
        //
        // A cold controller adopting a controller-mode `Running` job starts it in
        // [`Compiling`], which advances to [`Scheduling`] without ever reading the job's
        // channel — and `Scheduling` increments and persists the generation, stops the live
        // workers, starts replacements and prepares checkpoint recovery before its first
        // `recv`. So a refusal recorded after the start could not stop any of it. Recording
        // it here publishes it to [`RefusalGate`], which the task consults before its first
        // state body.
        //
        // D39a says the same thing with one owner instead of two: under `FencedV2` the poll
        // decides nothing at all, and its whole contribution is the classified intent left
        // here — before the state task exists — for that task's actor to read at its first
        // consumption point. The ordering requirement is identical, and
        // `both_paths_that_start_a_job_are_written_to_record_the_refusal_first` pins it for
        // the selected mechanism.
        if let Some(intents) = &fenced {
            // The submission's answer to "does this job need a state task" is not read here,
            // and does not have to be: the start below is unconditional. `StateMachine::update`
            // is where it is read, because that is where a job may already have had its task
            // and lost it. `every_mailbox_submission_path_can_get_the_job_a_state_task` pins
            // both halves of that.
            intents.submit(LifecycleIntent::classify(
                execution_selector,
                PolledJob {
                    execution_selector,
                    config: this.config.read().unwrap().0.clone(),
                    refusal: refusal.clone(),
                },
            ));
        } else if let Some(error) = &refusal {
            let refused = this.config.read().unwrap().0.clone();
            this.note_refused_row(error.clone(), &refused);
        }

        this.start(status.clone(), shutdown_guard.clone_temporary())
            .await;

        // Under `FencedV2` there is nothing left for this thread to do: applying the row is
        // the state task's, and it has the intent.
        if let Some(error) = refusal
            && fenced.is_none()
        {
            let refused = this.config.read().unwrap().0.clone();
            this.apply_refused_row(error, &refused, status, &shutdown_guard)
                .await;
        }

        this
    }

    fn decode_program(bs: &[u8]) -> anyhow::Result<LogicalProgram> {
        ArrowProgram::decode(bs)
            .map_err(|e| anyhow!("Failed to decode program: {:?}", e))?
            .try_into()
            .map_err(|e| anyhow!("Failed to construct graph from program: {:?}", e))
    }

    async fn get_program(
        db: &DatabaseSource,
        job_id: &str,
        id: i64,
    ) -> anyhow::Result<Option<(LogicalProgram, PipelineInfo)>> {
        let res = controller_queries::fetch_get_program(&db.client().await?, &id)
            .await
            .map_err(|e| anyhow!("Failed to fetch program from database: {:?}", e))?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Could not find program for job_id {job_id}"))?;

        let tags = serde_json::from_value::<HashMap<String, String>>(res.tags).map_err(|e| {
            anyhow!(
                "malformed JSON in pipelines.tags for pipeline {}: {e}",
                res.pipeline_id
            )
        })?;

        let info = PipelineInfo {
            pipeline_id: PipelineId(res.pipeline_id.into()),
            state_url: res.state_url,
            tags,
        };

        Ok(if res.proto_version == 2 {
            match Self::decode_program(&res.program) {
                Ok(p) => Some((p, info)),
                Err(e) => {
                    warn!(
                        %job_id,
                        pipeline_id = *info.pipeline_id,
                        "Failed to start job: {}",
                        e
                    );
                    None
                }
            }
        } else {
            None
        })
    }

    async fn start(&mut self, mut status: JobStatus, shutdown_guard: ShutdownGuard) {
        if !self.done() {
            // we're already running, don't do anything
            return;
        }

        let leader_mode = matches!(config().job_controller, JobControllerMode::Worker);

        // TODO: This seems pretty error-prone and easy to miss adding when we add states
        let initial_state: Option<Box<dyn State>> = match status.state.as_str() {
            "Created" => Some(Box::new(Created {})),
            "Stopped" => Some(Box::new(Stopped {})),
            "Finished" => Some(Box::new(Finished {})),
            "Failed" => Some(Box::new(Failed {})),
            "Running" if leader_mode => Some(Box::new(LeaderRunning {
                started: Instant::now(),
            })),
            "Compiling" | "Scheduling" | "Running" | "Recovering" | "Rescaling" | "Restarting" => {
                Some(Box::new(Compiling {}))
            }
            "Failing" => {
                // If we crashed during Failing, the job was already failing.
                // Transition directly to Failed which will clean up any remaining workers.
                Some(Box::new(Failed {}))
            }
            "Stopping" | "CheckpointStopping" => {
                // TODO: do we need to handle a failure in CheckpointStopping specially?
                if status.finish_time.is_none() {
                    status.finish_time = Some(OffsetDateTime::now_utc());
                }
                Some(Box::new(Stopped {}))
            }
            "Finishing" => {
                if status.finish_time.is_none() {
                    status.finish_time = Some(OffsetDateTime::now_utc());
                }
                Some(Box::new(Finished {}))
            }
            s => {
                panic!("Unhandled state {s} in recovery");
            }
        };

        if let Some(initial_state) = initial_state {
            status.state = initial_state.name().to_string();
            let (tx, rx) = channel(1024);
            let started;
            {
                let config = self.config.clone();
                // The job's selector, fixed for the life of the state machine and handed
                // to the execution rather than re-read from the shared config it is the
                // authority for.
                let execution_selector = self.execution_selector;
                // The controller's own cluster identity, handed to the task for the same
                // reason: a state is given what it stamps into its workers.
                let cluster_id = self.cluster_id.clone();
                // The task's half of the refusal gate. Cloned rather than re-derived, so a
                // refusal already published when this task is created gates its very first
                // state, and one raised later reaches it at the next state boundary.
                let refusal_gate = self.refusal_gate.clone();
                // The task's D39a single writer, or `None` under `LegacyT08` — which is
                // production through M11.T25. One actor per task rather than per job: it
                // starts having decided nothing, so it reads whatever the poll left,
                // including an intent submitted while this job had no state task at all.
                let lifecycle_actor = self
                    .lifecycle
                    .actor(Arc::clone(&status.id), self.execution_selector);
                let db = self.db.clone();
                let scheduler = self.scheduler.clone();
                let metrics = self.metrics.clone();
                let pipeline_id = config.read().unwrap().0.pipeline_id;
                // Loaded before anything durable is written, because whether this call
                // succeeds is the difference between a job that has an execution and a job
                // that does not — and the execution selector must only be recorded for one
                // that does.
                let program = Self::get_program(&db, &status.id, pipeline_id).await;

                if matches!(program, Ok(Some(_))) {
                    // Recorded at the one moment the execution actually begins: the
                    // program is loaded and the state task is about to run under this
                    // selector. Recording it any earlier wrote a selector for a job that
                    // never started — and because a recorded selector is authority that
                    // outranks the configuration row, that guess could not be corrected by
                    // any later poll, so a job the controller could not start was pinned
                    // to a backend it had never used.
                    //
                    // Carried by every later status write, so a controller that restarts
                    // recovers the backend this job is running with from the job's own
                    // execution record instead of from the configuration row — which an
                    // operator can edit while the controller is down.
                    status.state_context.execution_selector =
                        Some(self.execution_selector.as_str().to_string());
                }

                // The state itself is written either way, exactly as before: a recovered
                // job's status must reflect the state it was recovered into whether or not
                // its program could be loaded.
                status.update_db(&self.db).await.unwrap();

                started = match program {
                    Ok(Some((program, pipeline_info))) => {
                        let pipeline_info = Arc::new(pipeline_info);
                        let pipeline_id = pipeline_info.pipeline_id.clone();
                        shutdown_guard.into_spawn_task(async move {
                            let id = { config.read().unwrap().0.id.clone() };
                            info!(
                                message = "starting state machine",
                                job_id = %id,
                                pipeline_id = *pipeline_id
                            );
                            run_to_completion(
                                config,
                                execution_selector,
                                cluster_id,
                                refusal_gate,
                                lifecycle_actor,
                                pipeline_info,
                                program,
                                status,
                                initial_state,
                                db,
                                rx,
                                scheduler,
                                metrics,
                            )
                            .await;
                            info!(
                                message = "finished state machine",
                                job_id = %id,
                                pipeline_id = *pipeline_id
                            );
                            Ok(())
                        });
                        true
                    }
                    Ok(None) => {
                        // this is a bad/old pipeline, skip it
                        false
                    }
                    Err(e) => {
                        // something went wrong, we'll retry on the next go around
                        warn!(job_id = %status.id, "Failed to start job: {:?}", e);
                        false
                    }
                };
            }

            // Only a state task that actually took the receiving end gets to be the job's
            // queue. Installing the sender unconditionally advertised a channel whose
            // receiver had already been dropped, so every later `offer` reported
            // `Inactive` for a state machine that had explicitly promised to retry — and a
            // caller that treats `Inactive` as delivery loses the message. `done()` is
            // true either way, so the retry itself is unaffected.
            self.tx = started.then_some(tx);
        }
    }

    /// Applies a freshly polled `job_configs` row to this job.
    ///
    /// [`crate::classify_polled_row`] has already resolved the row against the job's own
    /// execution record, so `polled.config` carries the selector this job is running with
    /// whatever the row now says, and `polled.refusal` says whether the row's own value
    /// was refused. This is still the one place the state machine's shared configuration
    /// is replaced — everything downstream reads that cell — so a refused row is stopped
    /// here rather than at each of those readers.
    ///
    /// A refused row replaces nothing, and specifically not the selector or the restart
    /// nonce, so no state's baseline changes and [`Failed`] never sees a restart request:
    /// the update the refusal promised not to restart cannot restart. What a refused row
    /// *can* still do is stop the job — see [`Self::apply_refused_row`].
    pub async fn update(
        &mut self,
        polled: PolledJob,
        status: JobStatus,
        shutdown_guard: &ShutdownGuard,
    ) {
        *self.state.write().unwrap() = status.state.clone();

        // D39a: under `FencedV2` this thread is not a decider. It validates and classifies
        // the polled row and leaves one versioned intent; the job's own state task — the
        // single writer — consumes it and publishes the transition. Nothing below runs, so
        // no in-memory execution baseline is replaced and no lifecycle status is written
        // from the poll thread, which is the whole of "classify before adopting".
        //
        // The job's *task* is still supervised from here, because a job with no task has no
        // writer at all: an intent left for a job whose state task has ended — or was never
        // started, because the program could not be loaded — is decided by nobody until one
        // exists, and this thread is the only thing that can start one. It used to be said
        // here that such an intent "is decided by the actor of whichever poll finally gets a
        // task up", which asserted a later poll that nothing arranged: a job that reached
        // `Failed` or `Stopped` is not one `restart_if_needed` starts on its own account, so
        // an accepted restart of one was stranded in the mailbox forever — PR #160 review
        // comment `5368132947`.
        //
        // `restart_if_needed` starts a job whose status says it should be running, and a job
        // that has not taken up the poll's latest word about it. What that word *is* differs
        // between the two mechanisms and what recording it untaken means does not: under
        // `LegacyT08` the word is the configuration in the shared cell and storing it is what
        // records it, and under `FencedV2` the word is the intent submitted above, so a
        // submission that needs a state task records the same thing on the same flag.
        //
        // Which submissions those are is `LifecycleIntent::needs_a_state_task`, and its three
        // answers are this branch's own three, read off the code below rather than invented:
        // an accepted row is stored and started for, a refused row that also asks the job to
        // stop is stored and started for by `request_stop`, and a refused row that asks for
        // nothing else is left to `restart_if_needed` — which does not wake a job that has
        // legitimately reached a terminal state.
        //
        // Only that flag is written, never the configuration beside it. The actor remains the
        // only thing that adopts a row — `AppliedStatus` is a delivery watermark and not a
        // baseline, and asking whether an intent needs a task is a question about the mailbox
        // rather than a decision about the job. What a started task then does is the state's:
        // a stop cannot make a job run, so a terminal job given a task for one re-terminates
        // rather than resurrects, exactly as it does on the landed path.
        //
        // And what the started task may do is otherwise unchanged: `start` runs it under
        // `Self::execution_selector` and the shared configuration a refused row was never
        // allowed into, so this cannot restart the job under a value that is being refused.
        //
        // The state mirror above is not a lifecycle publication: it is this controller's
        // cached view of what the database already says, read by the API, and it is
        // deliberately still refreshed on both paths.
        if let Some(intents) = self.lifecycle.intents().map(Arc::clone) {
            let submitted =
                intents.submit(LifecycleIntent::classify(self.execution_selector, polled));

            if submitted.needs_a_state_task() {
                debug!(
                    job_id = %self.config.read().unwrap().0.id,
                    version = submitted.version().as_u64(),
                    "the job's configuration poll left an intent that needs a state task; \
                     recording it as one no state task has taken up"
                );
                self.config.write().unwrap().1 = AppliedStatus::NotApplied;
            }

            let applied = self.config.read().unwrap().1;
            self.restart_if_needed(applied, status, shutdown_guard)
                .await;
            return;
        }

        let PolledJob {
            config, refusal, ..
        } = polled;

        // Defence in depth. The poll substitutes this execution's own selector into every
        // configuration it hands on, so this can only fire if that ever stops being true.
        if let Err(e) = validate_unchanged_job_selector(
            &config.id,
            self.execution_selector,
            config.state_backend,
        ) {
            self.refuse_config(e);
            return;
        }

        if let Some(error) = refusal {
            self.apply_refused_row(error, &config, status, shutdown_guard)
                .await;
            return;
        }

        // The row is good again. Clearing the refusal also supersedes any `ConfigRefused`
        // still unread in the job's queue: without that, a row repaired between the poll
        // that raised the refusal and the state that reads it would still fail the job,
        // for a configuration that no longer exists.
        self.clear_refusal();

        if self.config.read().unwrap().0 != config {
            match self.offer(JobMessage::ConfigUpdate(config.clone())) {
                Delivery::Delivered => {
                    *self.config.write().unwrap() = (config, AppliedStatus::NotApplied);
                }
                Delivery::Full => {
                    // Nothing stored, so the same update is offered again on the next
                    // poll. Waiting for this one job's consumer instead would hold the
                    // global job map while it drained.
                    debug!(
                        job_id = %config.id,
                        "job queue is full; deferring a configuration update to the next poll"
                    );
                }
                Delivery::Inactive => {
                    // Stored without a roll-back, unlike [`Self::request_stop`], and the
                    // difference is a guard rather than a judgement call. `request_stop`
                    // returns at its head when the stored stop mode already equals the
                    // requested one, so a stop stored against a state task that never came
                    // up would short-circuit every later poll and strand the job. There is
                    // no such guard here: an update stored and not started leaves
                    // `self.config` equal to the polled row, which sends the next poll down
                    // the unchanged-row branch below and into `restart_if_needed`, and the
                    // `AppliedStatus::NotApplied` stored with it is exactly what makes that
                    // call start the job. Rolling back instead would only re-run this same
                    // branch. Covered by
                    // `an_accepted_update_that_could_not_start_is_retried_until_it_can`.
                    *self.config.write().unwrap() = (config, AppliedStatus::NotApplied);
                    self.start(status, shutdown_guard.clone_temporary()).await;
                }
            }
        } else {
            let applied = self.config.read().unwrap().1;
            self.restart_if_needed(applied, status, shutdown_guard)
                .await;
        }
    }

    /// Applies a polled row whose own `state_backend` was refused.
    ///
    /// Two things reach this: a row whose `state_backend` names a different backend than
    /// the job is running with, and a row whose `state_backend` cannot be interpreted at
    /// all (see [`crate::classify_polled_row`]).
    ///
    /// Refusing the *selector* must not also discard the row's **lifecycle control**. The
    /// refusal tells an operator to stop the job and create a new one under the new
    /// backend; if the stop request that arrives with the bad selector is thrown away
    /// with it, that remedy does not exist, and worse, the job is failed instead — losing
    /// the final-checkpoint semantics a `checkpoint` or `graceful` stop asked for and
    /// ending in `Failed` rather than `Stopped`. So a refused row that also asks the job
    /// to stop is executed as a stop, under the selector the job is running with.
    ///
    /// Everything else about the row is still discarded: the stop is issued on top of the
    /// configuration the job's workers, table configs, and checkpoints were built from,
    /// with the refused selector and the refused restart nonce nowhere in it.
    ///
    /// Either way the job must first *exist* as a state task, because neither a stop nor a
    /// refusal can be given to a job that has none: the stop branch restarts through
    /// [`Self::request_stop`], the refusal branch through [`Self::restart_if_needed`]. Both
    /// restart the job under its own immutable selector and its own unrefused
    /// configuration, which is why refusing a row still cannot restart the job into the
    /// value being refused.
    ///
    /// And either way the refusal is *recorded* first, by [`Self::note_refused_row`], before
    /// anything below can start a state task. Round 6 ordered the restart first so that the
    /// poll which finally got a task up would also deliver the refusal to it; the restart is
    /// kept, but delivery is not application. [`Compiling`] never reads the job's channel,
    /// and [`Scheduling`] increments and persists the generation, stops the job's workers,
    /// starts replacements and prepares checkpoint recovery before its first `recv` — so a
    /// task started before the refusal was recorded could reschedule a live execution for a
    /// configuration that must be adopted nowhere. Recording it first publishes it to
    /// [`RefusalGate`], which stops the restarted task at its very first state.
    async fn apply_refused_row(
        &mut self,
        error: StateBackendError,
        refused: &JobConfig,
        status: JobStatus,
        shutdown_guard: &ShutdownGuard,
    ) {
        self.note_refused_row(error, refused);

        if refused.stop_mode != StopMode::none {
            self.request_stop(refused.stop_mode, &refused.id, status, shutdown_guard)
                .await;
            return;
        }

        // A refusal has to reach the job, and a job with no state task cannot be told
        // anything. [`Self::start`] leaves exactly that behind when it cannot load the
        // program — a transient `get_program` or database failure while a cold controller
        // adopts a still-running job — and explicitly promises to retry on the next poll.
        // This branch used to return before the ordinary inactive retry, so for a refused
        // row that promise was never kept: the job was neither adopted nor failed, its
        // workers kept running unmanaged, and program loading was never retried even after
        // the dependency recovered.
        //
        // The retry is the ordinary one, on the same terms as any unchanged row.
        // `restart_if_needed` starts the job only if it should be running or has never
        // applied its configuration, so a job that legitimately reached a terminal state
        // is not woken up; and `start` runs it under [`Self::execution_selector`], which is
        // immutable, and the shared configuration the refused row was deliberately kept out
        // of. The refused selector and the refused restart nonce are in neither, so this
        // restarts the job as itself and never under the value being refused.
        //
        // What the task it starts may then *do* is settled above rather than here: the
        // refusal is already published, so the task is failed at its first state instead of
        // being allowed to reschedule the job on its way to reading its channel.
        let applied = self.config.read().unwrap().1;
        self.restart_if_needed(applied, status, shutdown_guard)
            .await;
    }

    /// Records what the controller has decided about a refused row, without yet doing
    /// anything to the job.
    ///
    /// Split out of [`Self::apply_refused_row`] because the decision has to be published
    /// strictly before anything can start the job's state task, and two callers start one:
    /// `apply_refused_row` itself, through `request_stop` or `restart_if_needed`, and
    /// [`Self::new`], which starts a cold-adopted job before it has looked at the row's
    /// refusal at all. Recording is idempotent — a refusal already raised at a version is
    /// re-offered at that same version, and one a stop is already answering is left alone —
    /// so calling it twice around a start costs nothing.
    ///
    /// A row that also asks for a stop records the refusal as answered by that stop
    /// ([`Self::note_refusal`]) and publishes *nothing* to [`RefusalGate`], because failing
    /// the job would lose exactly the final-checkpoint semantics the stop asked for.
    /// Everything else raises the refusal for delivery ([`Self::refuse_config`]).
    fn note_refused_row(&mut self, error: StateBackendError, refused: &JobConfig) {
        if refused.stop_mode != StopMode::none {
            self.note_refusal(error);
        } else {
            self.refuse_config(error);
        }
    }

    /// Asks the job to stop, under the configuration it is already running with.
    ///
    /// Only the stop mode is taken from the refused row. The rest — and in particular the
    /// state backend and the restart nonce — is copied from the shared configuration, so
    /// the job stops as itself rather than restarting or rescheduling as something else.
    ///
    /// Idempotent by construction: the request is recorded in the shared configuration once
    /// something is actually going to execute it, so a row that keeps asking for the same
    /// stop on every poll produces one message, and a full queue simply means the same
    /// request is offered again 500ms later.
    ///
    /// "Actually going to execute it" is the whole of the `Inactive` case. A stopped state
    /// machine is not delivery: recording the mode against one would make every later poll
    /// short-circuit on "the job is already stopping" while nothing ever stopped it, and
    /// the job would sit in that state until an operator noticed. `Inactive` is reachable
    /// with a refused row — [`Self::start`] leaves the job with no queue when its program
    /// cannot be loaded, and explicitly promises to retry — so the stop restarts the state
    /// machine exactly as an accepted update does, and if that restart does not take, the
    /// request is put back so the next poll offers it again.
    async fn request_stop(
        &mut self,
        stop_mode: StopMode,
        job_id: &str,
        status: JobStatus,
        shutdown_guard: &ShutdownGuard,
    ) {
        let stop = {
            let current = self.config.read().unwrap();
            if current.0.stop_mode == stop_mode {
                return;
            }
            JobConfig {
                stop_mode,
                ..current.0.clone()
            }
        };

        match self.offer(JobMessage::ConfigUpdate(stop.clone())) {
            Delivery::Delivered => {
                *self.config.write().unwrap() = (stop, AppliedStatus::NotApplied);
            }
            Delivery::Full => {
                debug!(
                    %job_id,
                    "job queue is full; deferring a refused row's stop request to the next poll"
                );
            }
            Delivery::Inactive => {
                // Stored before the restart, so the state task reads the stop from the
                // shared configuration the moment it comes up.
                let previous = std::mem::replace(
                    &mut *self.config.write().unwrap(),
                    (stop, AppliedStatus::NotApplied),
                );

                self.start(status, shutdown_guard.clone_temporary()).await;

                if self.done() {
                    // Nothing came up to execute it — a program that still cannot be
                    // loaded, most likely. Put the configuration back rather than leave a
                    // stop recorded that no one is acting on, so the next poll offers it
                    // again instead of short-circuiting on it.
                    *self.config.write().unwrap() = previous;
                    warn!(
                        %job_id,
                        "could not restart the job's state machine to execute a refused \
                         row's stop request; it will be offered again on the next poll"
                    );
                }
            }
        }
    }

    /// Records a refusal that is being answered by stopping the job rather than by failing
    /// it, so it is reported once and no [`JobMessage::ConfigRefused`] is ever sent for it.
    ///
    /// Taking a refusal over also *supersedes* any refusal message still unread in the
    /// job's queue. Without that, a row edited from "bad selector" to "bad selector plus a
    /// stop" would have the stop queued behind a refusal already on its way, and the job
    /// would be failed by the older message before the stop it asked for was executed —
    /// losing exactly the final-checkpoint semantics that stop existed for.
    fn note_refusal(&mut self, error: StateBackendError) {
        if self
            .refusal
            .as_ref()
            .is_some_and(|r| r.error == error && r.delivery == RefusalDelivery::AnsweredByStop)
        {
            return;
        }

        error!(
            job_id = %self.config.read().unwrap().0.id,
            error = %error,
            "refusing the job's persisted state backend; executing the stop the same row \
             requests, under the state backend the job is running with"
        );
        let version = self.next_refusal_version();
        self.refusal = Some(Refusal {
            error,
            version,
            delivery: RefusalDelivery::AnsweredByStop,
        });
        // Nothing is published to the gate, and anything already there is withdrawn: this
        // refusal is being answered by a stop, and a gate that failed the job first would
        // destroy the final-checkpoint semantics the stop exists for. The version bump above
        // already supersedes any queued message; this is its receiving-side counterpart.
        self.refusal_gate.withdraw();
    }

    /// Supersedes whatever refusal the job was under, and returns the version that
    /// describes it now.
    ///
    /// Any [`JobMessage::ConfigRefused`] still sitting unread in the job's queue was
    /// stamped with an older version and will be discarded when a state reads it.
    fn next_refusal_version(&mut self) -> u64 {
        self.refusal_version.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Forgets the refusal the job's configuration was under, because the row is good
    /// again.
    ///
    /// The bookkeeping alone is not enough: the queue is FIFO and a message already in it
    /// cannot be retracted, so the version is advanced too, which is what lets the state
    /// task tell that the refusal it eventually reads no longer describes the job.
    fn clear_refusal(&mut self) {
        if self.refusal.take().is_some() {
            self.next_refusal_version();
            // The gate is checked before every state, so a repaired row has to clear it as
            // well as the queue. The version bump alone would already make the gate discard
            // what it holds; withdrawing means it never holds a refusal the job is not under.
            self.refusal_gate.withdraw();
        }
    }

    /// Refuses the job's persisted configuration and fails the job.
    ///
    /// Nothing about the refused row is adopted — the shared configuration is deliberately
    /// left holding the value the job's workers, table configs, and checkpoints were built
    /// from, so the refused selector never becomes any state's baseline. The job is failed
    /// instead, by the state it is in, through [`JobContext::handle`].
    ///
    /// The refusal is coalesced rather than re-sent: the row stays bad until an operator
    /// fixes it, so one message per 500ms poll would repeat forever, and each of those
    /// sends used to *await* a bounded per-job channel while the global job map was
    /// locked — which is how one unusable row could stop every other job on the cluster
    /// from being polled. It is offered without waiting; a refusal the job's queue could
    /// not take is offered again on the next poll rather than dropped.
    ///
    /// A job with no state task has not been told anything, so this does not pretend it
    /// has: an offer that finds no live queue leaves the refusal [`RefusalDelivery::Pending`]
    /// and the next poll offers it again. Recording it as sent was the third way
    /// [`Delivery::Inactive`] got mistaken for delivery — every later poll then
    /// short-circuited on "already sent" while nothing had ever received it.
    ///
    /// Restoring a state task that *can* receive it is deliberately not done here.
    /// [`Self::apply_refused_row`] does it, because only the caller holds the job's status
    /// and shutdown guard, and because this function must stay non-`async`: the update
    /// thread calls it while it holds the global job map.
    ///
    /// Delivery is not the same as application, so the refusal is also published to the job's
    /// [`RefusalGate`], which every state is checked against before it runs. A queued message
    /// only reaches a state that receives, and [`Compiling`] never does while [`Scheduling`]
    /// does its generation write, worker teardown, worker start and checkpoint restore first.
    /// Publishing here, before the offer, is also what makes it safe for a caller to record a
    /// refusal and only then restart the job.
    ///
    /// Coalescing alone does not make the refusal safe to deliver late. Between the poll
    /// that queues it and the state that reads it, the operator can repair the row — the
    /// remedy the refusal itself asks for — and the queued message cannot be retracted. So
    /// every refusal is stamped with [`Self::refusal_version`], and a version the state
    /// machine has since moved past is discarded on receipt instead of failing the job.
    ///
    /// # A refusal is never published into irreversible scheduling work that has already started
    ///
    /// Publishing takes the job's scheduling admission ([`RefusalGate::admit_publication`]),
    /// which [`Scheduling`] holds across each region of irreversible work — its destructive
    /// preamble, the `StartExecution` fan-out, and the publication of a restored checkpoint's
    /// commits. Nothing waits for it — this function runs under the global job map — so a
    /// refusal raised while one of those regions is in flight simply does not happen yet:
    /// *nothing at all* is recorded, the refusal version is not advanced, and the next 500ms
    /// poll offers the same refusal again, by which time the region has either finished or is
    /// finishing. That is round 6's rule for an undeliverable refusal applied to an
    /// unpublishable one, and it is why contention defers a refusal rather than losing one.
    ///
    /// Leaving the recorded state untouched is load-bearing rather than tidy. Advancing
    /// [`Self::refusal_version`] on a poll that publishes nothing would supersede a refusal
    /// already on the gate or in the queue, and a state reading it would discard it as stale
    /// — a refusal silently dropped by the very contention that was supposed to defer it.
    pub(crate) fn refuse_config(&mut self, error: StateBackendError) {
        let job_id = self.config.read().unwrap().0.id.clone();

        // Taken before anything is decided, so a poll that cannot publish leaves no trace.
        let Some(admission) = self.refusal_gate.admit_publication() else {
            debug!(
                job_id = %job_id,
                "the job is in the middle of its scheduling work; keeping the refusal pending \
                 so the next poll offers it again"
            );
            return;
        };

        let version = match &self.refusal {
            // Already with the state machine, and unchanged. Nothing to say and nothing
            // to send.
            Some(refusal)
                if refusal.error == error && refusal.delivery == RefusalDelivery::Sent =>
            {
                return;
            }
            // Known, but the job's queue was full last time. Retry quietly, at the same
            // version: this is the same refusal, not a new one.
            Some(refusal)
                if refusal.error == error && refusal.delivery == RefusalDelivery::Pending =>
            {
                refusal.version
            }
            // A different refusal, none at all, or one a stop was answering: whatever the
            // job's queue may still hold is superseded and this one is raised afresh.
            _ => {
                error!(job_id = %job_id, error = %error, "refusing the job's persisted configuration");
                self.next_refusal_version()
            }
        };

        let refused = RefusedConfig::new(error.clone(), version, Arc::clone(&self.refusal_version));

        // Published before the message is offered, and regardless of what becomes of it.
        // Whether the job's queue took the refusal decides how *promptly* a state blocked on
        // `recv` learns of it; it does not decide whether the next state may run. A refusal
        // the queue was full for, or that has no queue at all, must still stop the next state
        // — and this is also what lets a caller record the refusal and only then restart the
        // job, so the task it starts is gated at its first state instead of after it has
        // rescheduled a live execution.
        //
        // Under the admission taken at the head of this function, so a preamble that has not
        // started yet reads this refusal before its first effect and one that is already
        // running could not have reached here at all.
        self.refusal_gate.publish(&admission, refused.clone());

        let delivery = match self.offer(JobMessage::ConfigRefused(refused)) {
            Delivery::Delivered => RefusalDelivery::Sent,
            Delivery::Inactive => {
                debug!(
                    job_id = %job_id,
                    "the job has no state task to receive its refusal; keeping it pending so \
                     the next poll offers it again"
                );
                RefusalDelivery::Pending
            }
            Delivery::Full => RefusalDelivery::Pending,
        };

        self.refusal = Some(Refusal {
            error,
            version,
            delivery,
        });
    }

    /// Offers a message to the job's own queue without ever waiting for capacity.
    ///
    /// The update thread calls this while it holds the global job map, so it must not
    /// block: see [`Delivery`].
    fn offer(&self, msg: JobMessage) -> Delivery {
        let Some(tx) = &self.tx else {
            return Delivery::Inactive;
        };

        match tx.try_send(msg) {
            Ok(()) => Delivery::Delivered,
            Err(TrySendError::Full(_)) => Delivery::Full,
            Err(TrySendError::Closed(_)) => Delivery::Inactive,
        }
    }

    pub(crate) fn sender(&self) -> Option<Sender<JobMessage>> {
        self.tx.clone()
    }

    pub fn done(&self) -> bool {
        if let Some(tx) = &self.tx {
            tx.is_closed()
        } else {
            true
        }
    }

    // for states that should be running, check them and restart if needed
    async fn restart_if_needed(
        &mut self,
        applied: AppliedStatus,
        status: JobStatus,
        shutdown_guard: &ShutdownGuard,
    ) {
        match (applied, status.state.as_str()) {
            (_, "Running" | "Recovering" | "Rescaling" | "Restarting")
            | (AppliedStatus::NotApplied, _)
                // done() means there isn't a task running, but these states
                // need to be advanced.
                if self.done() => {
                    self.start(status, shutdown_guard.clone_temporary()).await;
                }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::leader_stopping::LeaderStopBehavior;
    use super::lifecycle::classification::{
        SelectorClassification, UndecidableSelector, classify_selector,
    };
    use super::lifecycle::intent::{IntentVersion, VersionedIntent};
    use super::lifecycle::{
        ConsumptionPoint, JobLifecycle, LifecycleActor, LifecycleIntent, LifecycleMode,
        ObservedIntent,
    };
    use super::stopping::StopBehavior;
    use super::{
        Admission, AppliedStatus, CheckpointStopping, Compiling, Created, Failed, Failing,
        FatalProvenance, Finished, Finishing, JobContext, LeaderCheckpointStopping,
        LeaderFinishing, LeaderRescaling, LeaderRestarting, LeaderRunning, LeaderStopping,
        LeavingForStop, Recovering, RefusalGate, Rescaling, Restarting, Running,
        RunningConfigUpdate, State, StateMachine, Stopped, Stopping, Transition,
        adopt_refreshed_config, check_config_update, classify_running_config_update,
        controller_job_failure, errors, execute_state, fatal, fatal_refused_config,
        handle_unhandled_message, lifecycle,
    };
    use crate::job_controller::JobController;
    use crate::job_controller::checkpoint_store::DbCheckpointMetadataStore;
    use crate::job_controller::leader_manager::LeaderManager;
    use crate::schedulers::{Scheduler, SchedulerError, StartPipelineReq};
    use crate::states::scheduling::admission::PhaseContext;
    use crate::states::scheduling::fanout::IssuedAttempts;
    use crate::states::scheduling::{START_EXECUTION_RECONCILE_ATTEMPTS, Scheduling};
    use crate::types::public::{RestartMode, StopMode};
    use crate::{
        JobConfig, JobMessage, JobStatus, PipelineInfo, PolledJob, RefusedConfig, StateContext,
        states::StateError,
    };
    use arroyo_datastream::logical::{LogicalNode, LogicalProgram, OperatorName};
    use arroyo_rpc::grpc::api::ArrowProgram;
    use arroyo_rpc::grpc::rpc;
    use arroyo_rpc::grpc::rpc::job_status_grpc_server::{JobStatusGrpc, JobStatusGrpcServer};
    use arroyo_rpc::grpc::rpc::worker_grpc_server::{WorkerGrpc, WorkerGrpcServer};
    use arroyo_rpc::grpc::rpc::{
        CheckpointMetadata, CheckpointReq, CheckpointResp, CommitReq, CommitResp,
        GetCheckpointDetailsReq, GetCheckpointDetailsResp, GetJobCheckpointsReq,
        GetJobCheckpointsResp, GetWorkerPhaseReq, GetWorkerPhaseResp, GlobalKeyedTableConfig,
        GlobalKeyedTableTaskCheckpointMetadata, HeartbeatNodeReq, JobControllerInitReq,
        JobControllerInitResp, JobFailure, JobFinishedReq, JobFinishedResp, JobState,
        JobStatus as LeaderJobStatus, JobStatusReq, JobStatusResp, JobStopMode,
        LoadCompactedDataReq, LoadCompactedDataRes, MetricsReq, MetricsResp,
        OperatorCheckpointMetadata, OperatorMetadata, RegisterNodeReq, StartExecutionReq,
        StartExecutionResp, StopExecutionReq, StopExecutionResp, StopJobReq, StopJobResp,
        TableCheckpointMetadata, TableConfig, TableEnum, WorkerFinishedReq,
    };
    use arroyo_rpc::identity::worker_client;
    use arroyo_rpc::state_backend::validated::Validated;
    use arroyo_rpc::state_backend::{StateBackendError, StateBackendSelector};
    use arroyo_rpc::worker_types::RunningMessage;
    use arroyo_server_common::shutdown::{Shutdown, SignalBehavior};
    use arroyo_state::validated::{
        CheckpointMetadataWrite, CompletedCheckpoint, CompletedOperator,
    };
    use arroyo_state::{BackingStore, StateBackend, StorageProviderFor};
    use arroyo_types::{JobId, MachineId, PipelineId, WorkerId};
    use cornucopia_async::DatabaseSource;
    use futures::FutureExt as _;
    use prost::Message as _;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::{Duration, Instant, SystemTime};
    use tokio::sync::mpsc::{Receiver, Sender, channel};

    /// A running job's config. Only the fields the classifier reads vary between the
    /// cases below; everything else is fixed so a difference in the result can only come
    /// from the field under test.
    fn running_config(state_backend: StateBackendSelector) -> JobConfig {
        JobConfig {
            id: Arc::new("job_abc".to_string()),
            organization_id: "org".to_string(),
            pipeline_name: "pipeline".to_string(),
            pipeline_id: 1,
            stop_mode: StopMode::none,
            checkpoint_interval: Duration::from_secs(10),
            ttl: None,
            parallelism_overrides: HashMap::new(),
            restart_nonce: 3,
            restart_mode: RestartMode::safe,
            ignore_state_before_epoch: None,
            env_vars: serde_json::json!({}),
            scheduler_config: serde_json::json!({}),
            state_backend,
        }
    }

    /// The job's scheduling admission, for a test that publishes to the gate directly.
    ///
    /// A publication only ever happens under this, so a test that stands in for one takes it
    /// too. It is never contended in the tests that use this helper, which is why they can
    /// insist on getting it.
    fn admitted(gate: &RefusalGate) -> Admission {
        gate.admit_publication()
            .expect("nothing is scheduling in this test, so the admission must be free")
    }

    fn selector_error(err: &StateError) -> &StateBackendError {
        let StateError::FatalError { source, .. } = err else {
            panic!("expected a fatal error, got {err:?}");
        };
        source
            .downcast_ref::<StateBackendError>()
            .unwrap_or_else(|| panic!("expected a typed selector error, got {source:?}"))
    }

    /// A scheduler that does nothing and records what was asked of a job's cluster.
    ///
    /// The generation is recorded with every teardown because it is what tells the two kinds
    /// apart: a terminal state tears down the generation it knows (`Some(g)`), while
    /// [`Scheduling`] clears whatever is there before it schedules (`None`). Only the second
    /// is destructive to a running execution, so a test that must prove nothing was
    /// rescheduled has to be able to see the difference.
    #[derive(Default)]
    struct RecordingScheduler {
        stopped: Mutex<Vec<(String, Option<u64>)>>,
        started: Mutex<Vec<(String, u64)>>,
        /// Panics instead of starting the cluster, after recording the request.
        ///
        /// This is how the tests that need a panic *inside* the admitted region get one they
        /// own, and it stays that way now that the cluster identity is threaded through
        /// [`JobContext::cluster_id`]. They used to rely on `Scheduling::start_workers`
        /// panicking on a process-wide cell no test had populated: a panic that any test which
        /// *did* populate it would take away from every test that ran after it in the same
        /// binary. A panic the scheduler raises is under the same admission, is what the test
        /// asked for, and depends on nothing outside the test.
        panic_on_start: bool,
        /// Announces that the cluster has been asked for. See [`SchedulingBarriers`].
        barriers: Option<Arc<SchedulingBarriers>>,
    }

    impl RecordingScheduler {
        fn panicking() -> Self {
            Self {
                panic_on_start: true,
                ..Default::default()
            }
        }

        fn watching(barriers: Arc<SchedulingBarriers>) -> Self {
            Self {
                barriers: Some(barriers),
                ..Default::default()
            }
        }
    }

    #[async_trait::async_trait]
    impl Scheduler for RecordingScheduler {
        async fn start_workers(&self, req: StartPipelineReq) -> Result<(), SchedulerError> {
            self.started
                .lock()
                .unwrap()
                .push(((*req.job_id.0).clone(), req.generation));
            if let Some(barriers) = &self.barriers {
                barriers.workers_started.notify_one();
            }
            assert!(
                !self.panic_on_start,
                "the test asked for a panic while the job's scheduling admission is held"
            );
            Ok(())
        }
        async fn register_node(&self, _: RegisterNodeReq) {}
        async fn heartbeat_node(&self, _: HeartbeatNodeReq) -> Result<(), tonic::Status> {
            Ok(())
        }
        async fn worker_finished(&self, _: WorkerFinishedReq) {}
        async fn stop_workers(
            &self,
            job_id: &str,
            generation: Option<u64>,
            _: bool,
        ) -> anyhow::Result<()> {
            self.stopped
                .lock()
                .unwrap()
                .push((job_id.to_string(), generation));
            Ok(())
        }
        async fn workers_for_job(&self, _: &str, _: Option<u64>) -> anyhow::Result<Vec<WorkerId>> {
            Ok(vec![])
        }
    }

    /// A database handle the tests below never query. `Failed`/`Failing` write status
    /// through `execute_state`, which these tests do not call; they drive `next` directly.
    fn unused_db() -> DatabaseSource {
        DatabaseSource::Sqlite(Arc::new(Mutex::new(
            cornucopia_async::rusqlite::Connection::open_in_memory().unwrap(),
        )))
    }

    /// The two rows [`StateMachine::start`] actually reads and writes, in a schema that
    /// mirrors the migrated one for the columns it touches.
    ///
    /// `proto_version` decides whether the job's program can be loaded: the controller only
    /// runs `proto_version = 2` pipelines, so `1` is the "this is a bad/old pipeline" path
    /// — the one that leaves `start` with a job it cannot run while it explicitly promises
    /// to retry on the next poll.
    ///
    /// A trigger records every status write into `state_writes`. Reading the final row only
    /// says where a job ended up; a test that has to prove a job never *reached* a state, and
    /// never advanced the generation on the way, needs the whole sequence — see
    /// [`state_writes`].
    fn sqlite_startable_job(state: &str, proto_version: i32) -> DatabaseSource {
        let connection = cornucopia_async::rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE job_statuses (
                    id TEXT PRIMARY KEY,
                    state TEXT,
                    start_time TIMESTAMP,
                    finish_time TIMESTAMP,
                    tasks INTEGER,
                    failure_message TEXT,
                    failure_domain TEXT,
                    restarts INTEGER DEFAULT 0 NOT NULL,
                    pipeline_path TEXT,
                    wasm_path TEXT,
                    run_id INTEGER DEFAULT 0 NOT NULL,
                    restart_nonce INTEGER DEFAULT 0 NOT NULL,
                    state_context TEXT DEFAULT '{\"version\": 1}' NOT NULL
                );
                CREATE TABLE pipelines (
                    id INTEGER PRIMARY KEY,
                    pub_id TEXT NOT NULL,
                    program BLOB NOT NULL,
                    proto_version INTEGER NOT NULL,
                    state_url TEXT,
                    tags TEXT NOT NULL
                );
                CREATE TABLE checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pub_id TEXT NOT NULL,
                    organization_id TEXT NOT NULL DEFAULT '',
                    job_id TEXT NOT NULL,
                    epoch INTEGER NOT NULL,
                    min_epoch INTEGER NOT NULL,
                    state TEXT NOT NULL DEFAULT 'inprogress',
                    state_backend TEXT NOT NULL DEFAULT '',
                    start_time TIMESTAMP,
                    finish_time TIMESTAMP,
                    is_stopping BOOLEAN NOT NULL DEFAULT 0,
                    operators TEXT,
                    event_spans TEXT
                );
                CREATE TABLE state_writes (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    state TEXT NOT NULL,
                    run_id INTEGER NOT NULL
                );
                CREATE TRIGGER record_state_writes AFTER UPDATE ON job_statuses
                BEGIN
                    INSERT INTO state_writes (state, run_id) VALUES (NEW.state, NEW.run_id);
                END;",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO job_statuses (id, state) VALUES ('job_abc', ?1)",
                [state],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO pipelines (id, pub_id, program, proto_version, state_url, tags)
                 VALUES (1, 'pl_1', ?1, ?2, NULL, '{}')",
                cornucopia_async::rusqlite::params![
                    ArrowProgram::default().encode_to_vec(),
                    proto_version
                ],
            )
            .unwrap();
        DatabaseSource::Sqlite(Arc::new(Mutex::new(connection)))
    }

    /// Makes the job's program loadable or not, the way a database that is briefly
    /// unavailable and then recovers does.
    ///
    /// With the pipeline row gone, `fetch_get_program` returns nothing and
    /// [`StateMachine::get_program`] is an `Err` — the "something went wrong, we'll retry
    /// on the next go around" arm of [`StateMachine::start`], which is the arm a transient
    /// database failure during a cold controller restart takes. Putting the row back is the
    /// dependency recovering.
    fn program_loadable(db: &DatabaseSource, loadable: bool) {
        let DatabaseSource::Sqlite(connection) = db else {
            unreachable!("the fixture is always sqlite")
        };
        let connection = connection.lock().unwrap();
        connection.execute("DELETE FROM pipelines", []).unwrap();
        if loadable {
            connection
                .execute(
                    "INSERT INTO pipelines (id, pub_id, program, proto_version, state_url, tags)
                     VALUES (1, 'pl_1', ?1, 2, NULL, '{}')",
                    cornucopia_async::rusqlite::params![ArrowProgram::default().encode_to_vec()],
                )
                .unwrap();
        }
    }

    /// What the job's durable execution record says now, read straight out of the fixture.
    fn recorded_status(db: &DatabaseSource) -> (String, Option<String>) {
        let DatabaseSource::Sqlite(connection) = db else {
            unreachable!("the fixture is always sqlite")
        };
        let (state, raw): (String, String) = connection
            .lock()
            .unwrap()
            .query_row(
                "SELECT state, state_context FROM job_statuses WHERE id = 'job_abc'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let context: StateContext = serde_json::from_str(&raw).unwrap();
        (state, context.execution_selector)
    }

    /// Every status write the job made, in order, as `(state, generation)`.
    ///
    /// `generation` is `job_statuses.run_id`, which [`JobStatus::update_db`] is the only
    /// writer of, and [`Scheduling`] advances it as the first thing it does. So this
    /// sequence answers both halves of "did the job schedule anything": whether it ever
    /// entered `Scheduling` at all, and whether the generation it would have rescheduled
    /// under was ever persisted.
    fn state_writes(db: &DatabaseSource) -> Vec<(String, u64)> {
        let DatabaseSource::Sqlite(connection) = db else {
            unreachable!("the fixture is always sqlite")
        };
        let connection = connection.lock().unwrap();
        let mut stmt = connection
            .prepare("SELECT state, run_id FROM state_writes ORDER BY seq")
            .unwrap();
        stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }

    /// The failure the job's durable record now reports, if any.
    fn recorded_failure(db: &DatabaseSource) -> Option<String> {
        let DatabaseSource::Sqlite(connection) = db else {
            unreachable!("the fixture is always sqlite")
        };
        connection
            .lock()
            .unwrap()
            .query_row(
                "SELECT failure_message FROM job_statuses WHERE id = 'job_abc'",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }

    /// Lets the job's spawned state task run to the end, and reports every status write it
    /// made on the way.
    ///
    /// The point of the tests that use this is what the task *does*, so they have to let it
    /// actually run: a test that returns as soon as the state machine reports that a task
    /// exists cannot see the state that task goes on to enter, which is how a job that
    /// rescheduled itself before it was failed went unnoticed.
    ///
    /// The wait is on the task releasing the job's channel — [`StateMachine::done`] — rather
    /// than on the writes going quiet, because "nothing has been written for a while" is
    /// also what a task that has simply not been scheduled yet looks like.
    async fn drive_to_completion(sm: &StateMachine, db: &DatabaseSource) -> Vec<(String, u64)> {
        for _ in 0..2000 {
            if sm.done() {
                return state_writes(db);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!(
            "the job's state task never finished; wrote {:?}",
            state_writes(db)
        );
    }

    fn job_status(restart_nonce: i32) -> JobStatus {
        JobStatus {
            id: Arc::new("job_abc".to_string()),
            generation: 1,
            state: "Running".to_string(),
            start_time: None,
            finish_time: None,
            tasks: None,
            failure_message: None,
            failure_domain: None,
            restarts: 0,
            pipeline_path: None,
            wasm_path: None,
            restart_nonce,
            state_context: StateContext {
                version: 1,
                leader: None,
                execution_selector: None,
            },
        }
    }

    /// The cluster identity the tests hand to the code under test.
    ///
    /// A plain value, injected, and that is the point. The only way to populate the
    /// process-wide cell this used to come from is `arroyo_server_common`'s setter, and that
    /// setter resolves the identity against `~/.config/arroyo/cluster-info` and *writes* it
    /// there whenever the directory exists and holds nothing valid. Running `cargo test` is
    /// not a reason to give a developer's machine a cluster identity, still less to give it
    /// this one — so no test in this crate calls it, which
    /// `no_state_and_no_state_test_reaches_for_a_process_wide_cluster_identity` keeps true.
    fn test_cluster_id() -> Arc<String> {
        Arc::new("2f2a2f3c-0000-4000-8000-000000000001".to_string())
    }

    /// Owns everything a [`JobContext`] borrows, so a test can hand real states a real
    /// context and run their `next`.
    struct Harness {
        status: JobStatus,
        program: LogicalProgram,
        /// Kept alive, and handed out by [`Self::queue`]: a state that reads its channel has
        /// to have one, and a test that queues messages ahead of a refusal has to be the
        /// thing that queued them.
        tx: Sender<JobMessage>,
        rx: Receiver<JobMessage>,
        scheduler: Arc<RecordingScheduler>,
        /// Unused by the tests that drive `next` directly, and the difference between a
        /// context and a whole state machine for the ones that drive [`execute_state`],
        /// which writes the job's status after every transition.
        db: DatabaseSource,
        /// Where the job's checkpoints live, for the tests that restore from one.
        state_url: Option<String>,
        /// Owned here rather than made inside [`Self::ctx`], so a test can publish to the
        /// same gate the state runs under and can ask, afterwards, whether the job's
        /// scheduling admission was left free.
        refusal_gate: RefusalGate,
        /// The D39a actor the context runs with, for the tests of the `FencedV2` path.
        /// `None` — the production selection — for every other test in this module, whose
        /// contexts therefore behave exactly as they did before M11.T25a.
        lifecycle_actor: Option<LifecycleActor>,
        /// A live controller for the rows that run a state's *body* rather than its answer to
        /// the boundary. `None` everywhere else, which is what makes a body that dereferences
        /// it panic rather than run — the second half of every `leave_for_stop` row.
        job_controller: Option<JobController>,
        /// The same, for the leader-mode bodies.
        leader_manager: Option<LeaderManager>,
    }

    impl Harness {
        fn new(restart_nonce: i32) -> Self {
            let (tx, rx) = channel(16);
            Self {
                status: job_status(restart_nonce),
                program: LogicalProgram::default(),
                tx,
                rx,
                scheduler: Arc::new(RecordingScheduler::default()),
                db: unused_db(),
                state_url: None,
                refusal_gate: RefusalGate::default(),
                lifecycle_actor: None,
                job_controller: None,
                leader_manager: None,
            }
        }

        /// A harness whose status writes go somewhere, for tests that run `execute_state`
        /// rather than a state's `next`.
        fn with_db(mut self, db: DatabaseSource) -> Self {
            self.db = db;
            self
        }

        fn with_program(mut self, program: LogicalProgram) -> Self {
            self.program = program;
            self
        }

        fn with_state_url(mut self, state_url: String) -> Self {
            self.state_url = Some(state_url);
            self
        }

        fn with_scheduler(mut self, scheduler: RecordingScheduler) -> Self {
            self.scheduler = Arc::new(scheduler);
            self
        }

        /// A harness whose context has a live job controller, talking to a real worker.
        fn with_job_controller(mut self, job_controller: JobController) -> Self {
            self.job_controller = Some(job_controller);
            self
        }

        /// A harness whose context has a live leader manager, attached to a real leader.
        fn with_leader_manager(mut self, leader_manager: LeaderManager) -> Self {
            self.leader_manager = Some(leader_manager);
            self
        }

        /// A harness whose context carries the D39a single writer for `job_abc`.
        ///
        /// The actor is built from a mailbox the test also holds, so the test can play the
        /// configuration poll — `submit` — and then ask what the job's writer decided.
        fn with_actor(mut self, mailbox: &Arc<lifecycle::IntentMailbox>) -> Self {
            self.install_actor(mailbox);
            self
        }

        /// A harness whose lifecycle mechanism is derived the way a production job's is.
        ///
        /// Every other fixture leaves [`Self::lifecycle_actor`] at its `None` default, which
        /// is the same answer arrived at by assertion. This one *derives* it: it asks
        /// `JobLifecycle::for_mode(LifecycleMode::SELECTED, ..)` for the job's actor and
        /// installs whatever comes back, so a change that made production's mechanism the
        /// D39a single writer would arrive here as an actor rather than as nothing — and the
        /// row that uses this would see the phase graph run instead of the landed body.
        fn install_production_lifecycle(&mut self) {
            let job_id = Arc::new("job_abc".to_string());
            self.lifecycle_actor =
                JobLifecycle::for_mode(lifecycle::LifecycleMode::SELECTED, Arc::clone(&job_id))
                    .actor(job_id, StateBackendSelector::Parquet);
        }

        /// The same, for a harness a fixture has already built.
        fn install_actor(&mut self, mailbox: &Arc<lifecycle::IntentMailbox>) {
            self.lifecycle_actor = Some(LifecycleActor::new(
                Arc::new("job_abc".to_string()),
                StateBackendSelector::Parquet,
                Arc::clone(mailbox),
            ));
        }

        /// The job's own queue, for a test that has to put messages in it in a known order.
        fn queue(&self) -> Sender<JobMessage> {
            self.tx.clone()
        }

        fn ctx(
            &mut self,
            config: JobConfig,
            execution_selector: StateBackendSelector,
        ) -> JobContext<'_> {
            JobContext {
                config,
                execution_selector,
                cluster_id: test_cluster_id(),
                pipeline_info: Arc::new(PipelineInfo {
                    pipeline_id: PipelineId("pipeline_1".to_string().into()),
                    state_url: self.state_url.clone(),
                    tags: HashMap::new(),
                }),
                status: &mut self.status,
                program: &mut self.program,
                db: self.db.clone(),
                scheduler: self.scheduler.clone(),
                rx: &mut self.rx,
                refusal_gate: self.refusal_gate.clone(),
                lifecycle_actor: self.lifecycle_actor.take(),
                retries_attempted: 0,
                job_controller: self.job_controller.take(),
                leader_manager: self.leader_manager.take(),
                last_transitioned_at: Instant::now(),
                metrics: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            }
        }
    }

    /// A state machine whose channel a test can read, without a database or a spawned
    /// task. `update` and `refuse_config` are the two entry points exercised here and
    /// neither touches the database while the job's queue stays live.
    fn state_machine(
        config: JobConfig,
        execution_selector: StateBackendSelector,
    ) -> (StateMachine, Receiver<JobMessage>) {
        let (tx, rx) = channel(16);
        (
            state_machine_with(config, execution_selector, Some(tx), unused_db()),
            rx,
        )
    }

    fn state_machine_with(
        config: JobConfig,
        execution_selector: StateBackendSelector,
        tx: Option<Sender<JobMessage>>,
        db: DatabaseSource,
    ) -> StateMachine {
        state_machine_in_mode(
            LifecycleMode::SELECTED,
            config,
            execution_selector,
            tx,
            db,
            Arc::new(RecordingScheduler::default()),
        )
    }

    /// The same, in a named lifecycle mode and with a scheduler the caller can inspect.
    ///
    /// Every test that predates M11.T25a goes through [`state_machine_with`] and therefore
    /// runs `LifecycleMode::SELECTED`, which is what production runs. The `FencedV2` rows
    /// name the mode here, which is the only way to reach that path: no production
    /// construction site takes anything but `SELECTED` — see
    /// [`no_production_path_selects_the_fenced_v2_lifecycle`].
    fn state_machine_in_mode(
        mode: LifecycleMode,
        config: JobConfig,
        execution_selector: StateBackendSelector,
        tx: Option<Sender<JobMessage>>,
        db: DatabaseSource,
        scheduler: Arc<RecordingScheduler>,
    ) -> StateMachine {
        let job_id = Arc::clone(&config.id);
        StateMachine {
            tx,
            config: Arc::new(RwLock::new((config, AppliedStatus::Applied))),
            execution_selector,
            cluster_id: test_cluster_id(),
            refusal: None,
            refusal_version: Arc::new(AtomicU64::new(0)),
            refusal_gate: RefusalGate::default(),
            lifecycle: JobLifecycle::for_mode(mode, job_id),
            state: Arc::new(RwLock::new("Running".to_string())),
            metrics: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            db,
            scheduler,
        }
    }

    /// The job's intent mailbox, for a test that plays the configuration poll and then asks
    /// what the job's single writer decided.
    fn mailbox_of(sm: &StateMachine) -> Arc<lifecycle::IntentMailbox> {
        Arc::clone(
            sm.lifecycle
                .intents()
                .expect("this state machine runs the FencedV2 lifecycle"),
        )
    }

    /// What the job's configuration poll currently stands behind, if anything.
    fn standing_intent(mailbox: &Arc<lifecycle::IntentMailbox>) -> Option<VersionedIntent> {
        mailbox.newer_than(IntentVersion::NONE)
    }

    /// The error a queued [`JobMessage::ConfigRefused`] would fail the job with, or `None`
    /// if it has been superseded and a state would discard it.
    fn refusal_if_current(msg: JobMessage) -> Option<StateBackendError> {
        match msg {
            JobMessage::ConfigRefused(refusal) => refusal.into_current_error(),
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    fn shutdown_guard() -> arroyo_server_common::shutdown::ShutdownGuard {
        Shutdown::new("test", SignalBehavior::None).guard("test")
    }

    /// A shutdown whose owner outlives the tasks it spawns.
    ///
    /// [`shutdown_guard`] hands out a guard whose `Shutdown` has already been dropped, so
    /// its cancellation token is cancelled before the guard is ever used. Nothing notices
    /// while a test only asks whether a task was *spawned* — the task is cancelled at its
    /// first poll, which still leaves the job's channel open long enough to observe. A test
    /// that needs the task to actually *run* has to hold the owner, and this is that owner.
    struct LiveShutdown {
        _shutdown: Shutdown,
        guard: arroyo_server_common::shutdown::ShutdownGuard,
    }

    impl LiveShutdown {
        fn new() -> Self {
            let shutdown = Shutdown::new("test", SignalBehavior::None);
            let guard = shutdown.guard("test");
            Self {
                _shutdown: shutdown,
                guard,
            }
        }

        /// The one guard, borrowed. Handing out fresh ones would not help: a non-temporary
        /// guard cancels the token when *it* drops, which is what
        /// `ShutdownGuard::clone_temporary` — the thing `start` spawns under — exists to
        /// avoid.
        fn guard(&self) -> &arroyo_server_common::shutdown::ShutdownGuard {
            &self.guard
        }
    }

    /// A polled row as [`crate::classify_polled_row`] hands it on.
    ///
    /// The poll has already resolved the row against the job's execution record, so
    /// `config` carries the execution's own selector whatever the row said, and `refusal`
    /// carries why the row's value was rejected. `classify_polled_row`'s own tests, in
    /// `crate::tests`, prove it produces exactly this shape.
    fn polled(
        execution_selector: StateBackendSelector,
        config: JobConfig,
        refusal: Option<StateBackendError>,
    ) -> PolledJob {
        PolledJob {
            execution_selector,
            config,
            refusal,
        }
    }

    /// The refusal a row that asks for another backend produces.
    fn selector_changed() -> StateBackendError {
        StateBackendError::JobSelectorChanged {
            label: "job \"job_abc\"".to_string(),
            running: StateBackendSelector::Parquet,
            requested: StateBackendSelector::StateEngine,
        }
    }

    /// A configuration that changes the state backend of a job whose workers are already
    /// running is refused outright, in both running modes — they share this classifier,
    /// so there is one rule rather than two.
    ///
    /// The database can only get into this state through a direct edit: M11.T08 adds no
    /// API that sets the column on an existing job.
    #[test]
    fn a_state_backend_change_on_a_running_job_is_fatal() {
        let current = running_config(StateBackendSelector::Parquet);
        let updated = running_config(StateBackendSelector::StateEngine);

        let err = classify_running_config_update(
            current.state_backend,
            &current,
            &updated,
            current.restart_nonce,
        )
        .expect_err("a selector change must not be accepted");
        assert_eq!(
            selector_error(&err),
            &StateBackendError::JobSelectorChanged {
                label: "job \"job_abc\"".to_string(),
                running: StateBackendSelector::Parquet,
                requested: StateBackendSelector::StateEngine,
            }
        );

        // and in the other direction, so a stateengine job cannot be demoted either
        let err = classify_running_config_update(
            updated.state_backend,
            &updated,
            &current,
            updated.restart_nonce,
        )
        .expect_err("a selector change must not be accepted");
        assert!(
            matches!(
                selector_error(&err),
                StateBackendError::JobSelectorChanged {
                    running: StateBackendSelector::StateEngine,
                    requested: StateBackendSelector::Parquet,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    /// The selector is refused *before* anything else in the update is acted on. A
    /// restart-nonce bump arriving in the same update must not turn into a restart: a
    /// restart restores from a checkpoint the old backend wrote, so it would only move
    /// the failure to the restore path.
    #[test]
    fn a_state_backend_change_is_refused_even_when_the_update_also_restarts() {
        let current = running_config(StateBackendSelector::Parquet);
        let mut updated = running_config(StateBackendSelector::StateEngine);
        updated.restart_nonce = current.restart_nonce + 1;
        updated.env_vars = serde_json::json!({ "A": "1" });
        updated.scheduler_config = serde_json::json!({ "slots": 2 });

        let err = classify_running_config_update(
            current.state_backend,
            &current,
            &updated,
            current.restart_nonce,
        )
        .expect_err("a selector change must not be accepted");
        assert!(
            matches!(
                selector_error(&err),
                StateBackendError::JobSelectorChanged { .. }
            ),
            "{err:?}"
        );
    }

    /// The compatibility direction: everything a running job's configuration was allowed
    /// to change before still changes, and an unchanged selector — including the
    /// pre-selector empty string, which was normalized to `parquet` when the row was read
    /// — is not a change.
    #[test]
    fn updates_that_do_not_change_the_state_backend_are_classified_as_before() {
        for selector in [
            StateBackendSelector::Parquet,
            StateBackendSelector::StateEngine,
        ] {
            let current = running_config(selector);

            assert_eq!(
                classify_running_config_update(selector, &current, &current, current.restart_nonce)
                    .unwrap(),
                RunningConfigUpdate::Apply
            );

            let mut restarted = running_config(selector);
            restarted.restart_nonce = current.restart_nonce + 1;
            restarted.restart_mode = RestartMode::force;
            assert_eq!(
                classify_running_config_update(
                    selector,
                    &current,
                    &restarted,
                    current.restart_nonce
                )
                .unwrap(),
                RunningConfigUpdate::Restart(RestartMode::force)
            );

            let mut env = running_config(selector);
            env.env_vars = serde_json::json!({ "A": "1" });
            assert_eq!(
                classify_running_config_update(selector, &current, &env, current.restart_nonce)
                    .unwrap(),
                RunningConfigUpdate::Restart(RestartMode::safe)
            );

            let mut scheduler = running_config(selector);
            scheduler.scheduler_config = serde_json::json!({ "slots": 2 });
            assert_eq!(
                classify_running_config_update(
                    selector,
                    &current,
                    &scheduler,
                    current.restart_nonce
                )
                .unwrap(),
                RunningConfigUpdate::Restart(RestartMode::safe)
            );
        }

        // "" and "parquet" are the same selector by the time a JobConfig exists, so a row
        // edited from one to the other is not a selector change.
        let empty = running_config(StateBackendSelector::normalize("", "job").unwrap());
        let explicit = running_config(StateBackendSelector::normalize("parquet", "job").unwrap());
        assert_eq!(
            classify_running_config_update(
                empty.state_backend,
                &empty,
                &explicit,
                empty.restart_nonce
            )
            .unwrap(),
            RunningConfigUpdate::Apply
        );
    }

    /// Both running modes must route every configuration they are given through the one
    /// decision function, and must hand it the execution's own selector rather than the
    /// refreshable `ctx.config`; neither may keep a private copy of the restart rules that
    /// would then not carry the selector guard. This is a structural pin rather than a
    /// behavioural one — driving either state's `next` needs a live scheduler, database, and
    /// worker set.
    ///
    /// Its domain grew with PR #160 review comment `5365261487`. "One rule" was a claim about
    /// two sites, one per mode; a configuration reaches a running job by **two** routes — as a
    /// `JobMessage::ConfigUpdate`, and, for a job whose lifecycle the M11.D39a single writer
    /// decides, as an `ObservedIntent::Adopted` — so it is a claim about four. Each file
    /// therefore consults the rules in exactly one place and reaches it from exactly two.
    #[test]
    fn both_running_modes_classify_config_updates_through_one_rule() {
        for (name, source) in [
            ("running.rs", include_str!("running.rs")),
            ("leader_running.rs", include_str!("leader_running.rs")),
        ] {
            assert_eq!(
                source.matches("decide_running_config(").count(),
                1,
                "{name} must decide what a configuration means for a running job in exactly \
                 one place; a second is a second thing that can be changed alone"
            );
            assert!(
                source.contains("ctx.execution_selector,"),
                "{name} must decide against the execution's own selector, not against \
                 `ctx.config`, which is refreshed from shared state after every transition"
            );
            assert_eq!(
                source.matches("apply_new_config(").count(),
                3,
                "{name} must reach that one place from exactly two routes — the \
                 `JobMessage::ConfigUpdate` arm and the adopted-intent arm — plus the \
                 declaration itself. A route that grew its own copy, or one that stopped \
                 routing at all, changes this count"
            );
            assert!(
                source.contains("ObservedIntent::Adopted("),
                "{name} must answer an adopted configuration at all: reading only whether the \
                 job stopped is the finding this pin was widened for"
            );
            assert!(
                !source.contains("classify_running_config_update("),
                "{name} must not reach past `decide_running_config` to the half of the rules \
                 that omits the parallelism comparison — that half answers `Apply` for a \
                 rescale"
            );
            assert!(
                !source.contains("c.restart_nonce != ctx.status.restart_nonce"),
                "{name} must not keep its own copy of the restart-nonce rule"
            );
        }
    }

    /// What each consumption point does with a configuration the job's writer adopted.
    ///
    /// Recorded per file rather than per call, because a file is what a reviewer opens and
    /// because the answer is a property of what that file's landed `JobMessage::ConfigUpdate`
    /// arm does — an adopted configuration and a delivered one have to mean the same thing.
    #[derive(Debug, Clone, Copy)]
    enum AdoptedAnswer {
        /// Decides what it means for a running job, through `decide_running_config`.
        Routes,
        /// Acts on it with this state's own rule, whose source marker is named here.
        Acts(&'static str),
        /// Nothing further, for one of two reasons. Usually: besides the stop it answers, all
        /// that file's `ConfigUpdate` arm does with an update is `check_config_update`, and the
        /// job's writer refuses a configuration that changes the state backend rather than
        /// adopting it — the new configuration is published into `JobContext::config`, which is
        /// the field those states already read. For `leader_stopping.rs` the reason is one step
        /// shorter: it has no `ConfigUpdate` arm to agree with, because it is ending the job and
        /// starts nothing a configuration could change.
        NothingFurther,
        /// Carries the outcome to its caller without reading it. `JobWait` is the wait, not a
        /// decider; what a decision means belongs to the state that owns the wait.
        Reports,
    }

    /// Every consumption point in this crate, and what it does with an adopted configuration.
    ///
    /// The registry half of `every_consumption_point_answers_an_adopted_configuration`. A
    /// hard-coded list of sites is what let PR #160 review comments `5358055190`,
    /// `5362488017` and `5365261487` through in succession, so this list is not the domain —
    /// the source is, and this is checked against it.
    fn every_consumption_point_and_its_answer() -> Vec<(&'static str, AdoptedAnswer)> {
        vec![
            // The state boundary. It holds a `Box<dyn State>` and no job controller, so it
            // cannot classify; it publishes and leaves the adoption standing for the state's
            // own consumption points, exactly as it does a stop a state declines to leave for.
            ("states/mod.rs", AdoptedAnswer::Acts("hand_adoption_to")),
            ("states/running.rs", AdoptedAnswer::Routes),
            ("states/leader_running.rs", AdoptedAnswer::Routes),
            // A safe restart escalates to a force one, which is what its `ConfigUpdate` arm
            // does with a configuration that changes `restart_mode`.
            (
                "states/restarting.rs",
                AdoptedAnswer::Acts("ctx.config.restart_mode == RestartMode::force"),
            ),
            ("states/leader_restarting.rs", AdoptedAnswer::NothingFurther),
            // Ending the job: it schedules nothing, restarts nothing and rescales nothing, so
            // there is no later work an adopted configuration could change. Added with the
            // consumption point itself — PR #160 review comment `5384225297`.
            ("states/leader_stopping.rs", AdoptedAnswer::NothingFurther),
            ("states/rescaling.rs", AdoptedAnswer::NothingFurther),
            (
                "states/checkpoint_stopping.rs",
                AdoptedAnswer::NothingFurther,
            ),
            ("states/scheduling.rs", AdoptedAnswer::NothingFurther),
            (
                "states/scheduling/admission/observation.rs",
                AdoptedAnswer::NothingFurther,
            ),
            ("job_controller/mod.rs", AdoptedAnswer::NothingFurther),
            (
                "job_controller/leader_manager.rs",
                AdoptedAnswer::NothingFurther,
            ),
            ("states/lifecycle/waiting.rs", AdoptedAnswer::Reports),
        ]
    }

    /// No consumption point can silently drop a configuration the job's writer adopted.
    ///
    /// The quantified row, and the one that would have found PR #160 review comment
    /// `5365261487` before a reviewer did. Its domain is every file in this crate that reads
    /// the job's single writer — taken from the source, not from a list — and each has to
    /// appear in `every_consumption_point_and_its_answer` with an answer the source then has
    /// to bear out. A file that starts observing, or one that stops answering, fails here.
    ///
    /// It is a source pin because most of these sites cannot be driven: `Scheduling`'s two
    /// loops need a live worker set, `handle_leader_stopping` a live leader. The behavioural
    /// rows for the sites that *can* be driven are below.
    #[test]
    fn every_consumption_point_answers_an_adopted_configuration() {
        /// Everything in a file before its test module: a test that observes answers for
        /// itself.
        fn production_half(source: &str) -> &str {
            match source.find("\n#[cfg(test)]") {
                Some(at) => &source[..at],
                None => source,
            }
        }

        /// What makes a file a consumption point: it reads the writer, or it consumes what a
        /// read produced.
        const OBSERVES: [&str; 3] = [
            "observe_lifecycle_intent(",
            "observe_lifecycle_decision(",
            "Waited::Decided(",
        ];

        fn walk(
            dir: &std::path::Path,
            root: &std::path::Path,
            found: &mut std::collections::BTreeSet<String>,
        ) {
            for entry in std::fs::read_dir(dir).expect("this crate's own source") {
                let path = entry.expect("a readable directory entry").path();
                if path.is_dir() {
                    walk(&path, root, found);
                    continue;
                }
                if path.extension().is_none_or(|e| e != "rs") {
                    continue;
                }
                let source = std::fs::read_to_string(&path).expect("a readable source file");
                if OBSERVES
                    .iter()
                    .any(|marker| production_half(&source).contains(marker))
                {
                    found.insert(
                        path.strip_prefix(root)
                            .expect("a path under the crate's source root")
                            .to_string_lossy()
                            .replace('\\', "/"),
                    );
                }
            }
        }

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut found = std::collections::BTreeSet::new();
        walk(&root, &root, &mut found);

        let declared: std::collections::BTreeSet<String> = every_consumption_point_and_its_answer()
            .into_iter()
            .map(|(file, _)| file.to_string())
            .collect();
        assert_eq!(
            found, declared,
            "every file that reads the job's single writer has to say what it does with a \
             configuration that writer adopted. The left side is the source; the right side \
             is `every_consumption_point_and_its_answer`"
        );

        for (file, answer) in every_consumption_point_and_its_answer() {
            let source = std::fs::read_to_string(root.join(file)).expect("a declared file");
            let source = production_half(&source);

            match answer {
                AdoptedAnswer::Reports => {
                    assert!(
                        !source.contains("ObservedIntent::Adopted"),
                        "{file} is declared as carrying the outcome to its caller, so it must \
                         not decide anything about an adopted configuration itself"
                    );
                    continue;
                }
                AdoptedAnswer::Routes => assert!(
                    source.contains("decide_running_config("),
                    "{file} is declared as routing an adopted configuration through the one \
                     rule a running job decides with"
                ),
                AdoptedAnswer::Acts(marker) => assert!(
                    source.contains(marker),
                    "{file} is declared as acting on an adopted configuration with `{marker}`, \
                     and that is no longer in its source"
                ),
                AdoptedAnswer::NothingFurther => assert!(
                    !source.contains("decide_running_config("),
                    "{file} is declared as having nothing further to do with an adopted \
                     configuration, but it now decides one — the declaration is stale"
                ),
            }

            assert!(
                source.contains("ObservedIntent::Adopted"),
                "{file} must name `ObservedIntent::Adopted` — an adopted configuration folded \
                 into a wildcard or read only through a stop test is one this file drops \
                 without saying so, which is PR #160 review comment `5365261487`"
            );
        }
    }

    /// Every state that acts on a `ConfigUpdate` must validate it against the execution's
    /// selector, and every state that reads the job's message channel must route what it
    /// does not recognize to `handle_unhandled_message`, which is where a refusal is
    /// turned into a job failure.
    ///
    /// A structural pin, because none of these states' `next` can be driven without a live
    /// scheduler, a Postgres schema, and a worker set: `Scheduling` blocks on real worker
    /// connections and `Restarting`/`Rescaling`/`CheckpointStopping` dereference a
    /// `JobController`. What each of them does once the check fires *is* covered
    /// behaviourally, by the tests of `check_config_update` and `handle_unhandled_message`
    /// below.
    #[test]
    fn every_config_update_consumer_validates_against_the_execution_selector() {
        for (name, source) in [
            ("scheduling.rs", include_str!("scheduling.rs")),
            ("restarting.rs", include_str!("restarting.rs")),
            ("rescaling.rs", include_str!("rescaling.rs")),
            (
                "checkpoint_stopping.rs",
                include_str!("checkpoint_stopping.rs"),
            ),
            ("leader_restarting.rs", include_str!("leader_restarting.rs")),
            (
                "leader_manager.rs",
                include_str!("../job_controller/leader_manager.rs"),
            ),
        ] {
            let updates = source.matches("JobMessage::ConfigUpdate(c)").count();
            assert!(updates > 0, "{name} should still consume config updates");
            assert_eq!(
                source.matches("check_config_update(").count(),
                updates,
                "{name} must validate every config update against the execution's selector"
            );
        }

        // scheduling.rs routes unknown messages through `handle_worker_connect`, which
        // ends in `ctx.handle`; running.rs does so directly.
        for (name, source) in [
            ("scheduling.rs", include_str!("scheduling.rs")),
            ("running.rs", include_str!("running.rs")),
            ("leader_running.rs", include_str!("leader_running.rs")),
            ("leader_restarting.rs", include_str!("leader_restarting.rs")),
            (
                "leader_manager.rs",
                include_str!("../job_controller/leader_manager.rs"),
            ),
        ] {
            assert!(
                source.contains("ctx.handle("),
                "{name} must route unrecognized job messages to JobContext::handle"
            );
        }
        for (name, source) in [
            ("restarting.rs", include_str!("restarting.rs")),
            ("rescaling.rs", include_str!("rescaling.rs")),
            (
                "checkpoint_stopping.rs",
                include_str!("checkpoint_stopping.rs"),
            ),
        ] {
            assert!(
                source.contains("handle_unhandled_message(&job_id, &pipeline_id, msg)?"),
                "{name} must route unrecognized job messages to handle_unhandled_message"
            );
        }
    }

    /// The second half of finding 1: persisted state is not the only thing a restarted
    /// controller can be wrong about, so it also asks the live leader before attaching to
    /// it. A structural pin — driving it needs a running worker leader to answer.
    ///
    /// What is pinned is that the check is in the poll rather than only in `connect`: a
    /// leader that is replaced under a manager the controller already holds is exactly
    /// what a one-off check at connect time would miss.
    #[test]
    fn attaching_to_a_worker_leader_requires_it_to_agree_on_the_backend() {
        let source = include_str!("../job_controller/leader_manager.rs");
        assert!(
            source.contains(&format!("        {}(\n", "validate_leader_selector")),
            "every leader status poll must check the reported backend against the one the \
             controller is administering the job with"
        );
        assert!(
            source.contains("        this.poll_leader_status().await?;"),
            "and connecting must poll once, so no caller can hold a manager for a leader \
             that has never agreed"
        );

        // ...and every connect site hands in the execution's own selector rather than
        // whatever the configuration cell currently holds.
        for (name, source) in [
            ("states/mod.rs", include_str!("mod.rs")),
            ("scheduling.rs", include_str!("scheduling.rs")),
        ] {
            let connects = source.matches("LeaderManager::connect(").count();
            assert!(connects > 0, "{name} should still connect to leaders");
            assert!(
                source.matches("execution_selector,\n").count() >= connects,
                "{name} must hand every leader connect the execution's own selector"
            );
        }
    }

    /// The central refusal, on the real writer. A polled row that changes the selector
    /// must not replace the state machine's authoritative config — not the selector, and
    /// not the restart nonce that arrived with it — and must not be delivered as a
    /// `ConfigUpdate` any state could act on.
    ///
    /// Without this, `update` stored the row first: scheduling's worker- and task-startup
    /// consumers, and the `ctx.config` refresh after every transition, would then read the
    /// new selector as their baseline.
    #[tokio::test]
    async fn a_polled_row_that_changes_the_selector_never_reaches_the_shared_config() {
        let current = running_config(StateBackendSelector::Parquet);
        let (mut sm, mut rx) = state_machine(current.clone(), StateBackendSelector::Parquet);

        // The rest of the refused row is deliberately different from what the job is
        // running with, so "nothing was replaced" is an assertion about the whole row and
        // not only about the selector.
        let mut refused = running_config(StateBackendSelector::Parquet);
        refused.env_vars = serde_json::json!({ "A": "1" });
        refused.scheduler_config = serde_json::json!({ "slots": 2 });

        sm.update(
            polled(
                StateBackendSelector::Parquet,
                refused,
                Some(selector_changed()),
            ),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;

        assert_eq!(
            sm.config.read().unwrap().0,
            current,
            "a refused row must replace nothing in the job's authoritative config"
        );

        assert_eq!(
            refusal_if_current(
                rx.try_recv()
                    .expect("the refusal should have been delivered")
            ),
            Some(selector_changed()),
        );
        assert!(
            rx.try_recv().is_err(),
            "nothing else may be delivered for a refused row"
        );
    }

    /// Round 3 refused a changed-selector row by discarding **all** of it, which threw
    /// away the stop request an operator sends to apply the remedy the refusal itself
    /// documents. A row that carries both must still stop the job, under the selector the
    /// job is running with, in every stop mode — and, crucially, must *not* be delivered
    /// as a refusal, because that would fail the job instead of stopping it.
    #[tokio::test]
    async fn a_refused_row_that_also_asks_for_a_stop_stops_the_job_under_the_execution_selector() {
        for stop_mode in [
            StopMode::checkpoint,
            StopMode::graceful,
            StopMode::immediate,
            StopMode::force,
        ] {
            let current = running_config(StateBackendSelector::Parquet);
            let (mut sm, mut rx) = state_machine(current.clone(), StateBackendSelector::Parquet);

            // The row the operator wrote: a bad selector *and* the stop that undoes it.
            // The poll pins the nonce and substitutes the execution's selector; the stop
            // mode is the operator's, and is the one thing that must survive.
            let mut refused = running_config(StateBackendSelector::Parquet);
            refused.stop_mode = stop_mode;
            refused.env_vars = serde_json::json!({ "A": "1" });

            sm.update(
                polled(
                    StateBackendSelector::Parquet,
                    refused,
                    Some(selector_changed()),
                ),
                job_status(current.restart_nonce),
                &shutdown_guard(),
            )
            .await;

            let stored = sm.config.read().unwrap().0.clone();
            assert_eq!(
                stored.stop_mode, stop_mode,
                "the stop the row asked for must reach the job's configuration"
            );
            assert_eq!(
                stored.state_backend,
                StateBackendSelector::Parquet,
                "and must be executed under the selector the job is running with"
            );
            assert_eq!(
                stored.restart_nonce, current.restart_nonce,
                "a refused row still must not advance the restart nonce"
            );
            assert_eq!(
                stored.env_vars, current.env_vars,
                "and nothing else from the refused row may be adopted either"
            );

            match rx.try_recv().expect("the stop should have been delivered") {
                JobMessage::ConfigUpdate(c) => {
                    assert_eq!(c.stop_mode, stop_mode);
                    assert_eq!(c.state_backend, StateBackendSelector::Parquet);
                }
                other => panic!(
                    "a refused row that asks for a stop must be delivered as that stop, \
                     not as {other:?}"
                ),
            }
            assert!(
                rx.try_recv().is_err(),
                "and must not also be delivered as a refusal, which would fail the job \
                 instead of stopping it"
            );

            // Driven through the real states: both running modes take the stop from the
            // configuration the refusal left them with, and a `checkpoint` stop still
            // gets its final checkpoint rather than becoming fatal cleanup.
            let expected = match stop_mode {
                StopMode::checkpoint => ("CheckpointStopping", "CheckpointStopping"),
                _ => ("Stopping", "Stopping"),
            };

            let mut harness = Harness::new(current.restart_nonce);
            let mut ctx = harness.ctx(stored.clone(), StateBackendSelector::Parquet);
            let Ok(Transition::Advance(next)) = Box::new(Running {}).next(&mut ctx).await else {
                panic!("a stop request must move a running job out of Running");
            };
            assert_eq!(next.state.name(), expected.0, "legacy mode, {stop_mode:?}");

            let mut harness = Harness::new(current.restart_nonce);
            let mut ctx = harness.ctx(stored, StateBackendSelector::Parquet);
            let Ok(Transition::Advance(next)) = Box::new(LeaderRunning {
                started: Instant::now(),
            })
            .next(&mut ctx)
            .await
            else {
                panic!("a stop request must move a leader-mode job out of Running");
            };
            assert_eq!(next.state.name(), expected.1, "leader mode, {stop_mode:?}");
        }
    }

    /// The same row, polled again and again while the operator leaves it in place, must
    /// produce one stop request rather than one per poll.
    #[tokio::test]
    async fn a_refused_row_that_asks_for_a_stop_asks_once() {
        let current = running_config(StateBackendSelector::Parquet);
        let (mut sm, mut rx) = state_machine(current.clone(), StateBackendSelector::Parquet);

        let mut refused = running_config(StateBackendSelector::Parquet);
        refused.stop_mode = StopMode::checkpoint;

        for _ in 0..5 {
            sm.update(
                polled(
                    StateBackendSelector::Parquet,
                    refused.clone(),
                    Some(selector_changed()),
                ),
                job_status(current.restart_nonce),
                &shutdown_guard(),
            )
            .await;
        }

        assert!(matches!(
            rx.try_recv(),
            Ok(JobMessage::ConfigUpdate(c)) if c.stop_mode == StopMode::checkpoint
        ));
        assert!(
            rx.try_recv().is_err(),
            "a row that keeps asking for the same stop must produce one request"
        );
    }

    /// Finding 4. The row stays bad until an operator fixes it, so the refusal is polled
    /// again every 500ms; it must reach the job once and then stop being sent.
    #[tokio::test]
    async fn a_refusal_is_delivered_once_however_often_the_bad_row_is_polled() {
        let current = running_config(StateBackendSelector::Parquet);
        let (mut sm, mut rx) = state_machine(current, StateBackendSelector::Parquet);

        for _ in 0..10 {
            sm.refuse_config(selector_changed());
        }

        assert_eq!(
            refusal_if_current(rx.try_recv().expect("the refusal must be delivered")),
            Some(selector_changed())
        );
        assert!(
            rx.try_recv().is_err(),
            "one unusable row must produce one refusal, not one per poll"
        );
    }

    /// Finding 4, the part that made it a cluster problem: the refusal is offered to the
    /// job's own bounded queue and never waited for. The update thread holds the global
    /// job map while it does this, so a job whose consumer is slow must not be able to
    /// stall it.
    #[tokio::test]
    async fn refusing_a_row_never_waits_for_the_jobs_own_queue() {
        let current = running_config(StateBackendSelector::Parquet);
        let (tx, mut rx) = channel(1);
        let mut sm = state_machine_with(
            current,
            StateBackendSelector::Parquet,
            Some(tx),
            unused_db(),
        );

        // Fill the job's queue, as a delayed teardown or retry consumer would.
        sm.offer(JobMessage::TaskStarted {
            worker_id: WorkerId(1),
            task_id: 1,
            subtask_idx: 0,
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            for _ in 0..10 {
                sm.refuse_config(selector_changed());
            }
        })
        .await
        .expect(
            "refusing a row must never wait for the job's own queue: the update thread \
             holds the global job map while it does this",
        );

        // Nothing was lost either: once the consumer catches up, the same refusal is
        // offered again rather than dropped.
        assert!(matches!(rx.try_recv(), Ok(JobMessage::TaskStarted { .. })));
        sm.refuse_config(selector_changed());
        assert_eq!(
            refusal_if_current(rx.try_recv().expect("the retry must be delivered")),
            Some(selector_changed()),
            "a refusal the queue could not take is offered again at the same version, so \
             the retry is still the refusal that describes the job"
        );
    }

    /// Coalescing must not swallow a *different* refusal: a row edited from one bad value
    /// to another is a new fact about the job and has to reach it.
    #[tokio::test]
    async fn a_changed_refusal_is_delivered_again() {
        let current = running_config(StateBackendSelector::Parquet);
        let (mut sm, mut rx) = state_machine(current, StateBackendSelector::Parquet);

        let unknown = StateBackendError::UnknownValue {
            label: "job job_abc".to_string(),
            value: "rocksdb".to_string(),
        };
        sm.refuse_config(selector_changed());
        sm.refuse_config(unknown.clone());

        assert_eq!(
            refusal_if_current(rx.try_recv().expect("the first refusal was queued")),
            None,
            "the first refusal no longer describes the job, so a state that reads it now \
             must discard it rather than fail the job for a value that has been replaced"
        );
        assert_eq!(
            refusal_if_current(rx.try_recv().expect("the second refusal must be delivered")),
            Some(unknown),
            "and the refusal that does describe the job still fails it"
        );
    }

    /// The compatibility direction of the same writer: an update that does not change the
    /// selector is stored and delivered exactly as before, including the restart nonce.
    #[tokio::test]
    async fn a_polled_row_that_keeps_the_selector_is_applied_as_before() {
        let current = running_config(StateBackendSelector::Parquet);
        let (mut sm, mut rx) = state_machine(current.clone(), StateBackendSelector::Parquet);

        let mut updated = running_config(StateBackendSelector::Parquet);
        updated.restart_nonce = current.restart_nonce + 1;

        sm.update(
            polled(StateBackendSelector::Parquet, updated.clone(), None),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;

        assert_eq!(sm.config.read().unwrap().0, updated);
        match rx
            .try_recv()
            .expect("the update should have been delivered")
        {
            JobMessage::ConfigUpdate(c) => assert_eq!(c, updated),
            other => panic!("expected a config update, got {other:?}"),
        }
    }

    /// Finding 3's route: a row the controller cannot interpret at all is refused to an
    /// existing job through the same path, rather than skipped while the job keeps running
    /// under a selector the database no longer agrees with.
    #[tokio::test]
    async fn an_uninterpretable_row_is_refused_to_the_running_job() {
        let current = running_config(StateBackendSelector::Parquet);
        let (mut sm, mut rx) = state_machine(current.clone(), StateBackendSelector::Parquet);

        sm.refuse_config(StateBackendError::UnknownValue {
            label: "job job_abc".to_string(),
            value: "rocksdb".to_string(),
        });

        assert_eq!(
            sm.config.read().unwrap().0,
            current,
            "an unusable row must not disturb the config the job is running with"
        );
        match refusal_if_current(
            rx.try_recv()
                .expect("the refusal should have been delivered"),
        ) {
            Some(StateBackendError::UnknownValue { value, .. }) => assert_eq!(value, "rocksdb"),
            other => panic!("expected a current refusal naming the bad value, got {other:?}"),
        }
    }

    /// The refusal reaches the job as a failure, wherever it is. This is the one place the
    /// policy lives, so every state that routes an unrecognized message here fails the
    /// same way.
    #[test]
    fn a_current_refusal_fails_the_job_and_a_superseded_one_is_discarded() {
        let version = Arc::new(AtomicU64::new(4));
        let err = handle_unhandled_message(
            "job_abc",
            "pipeline_1",
            JobMessage::ConfigRefused(RefusedConfig::new(
                selector_changed(),
                4,
                Arc::clone(&version),
            )),
        )
        .expect_err("a refused configuration must fail the job");
        assert!(
            matches!(
                selector_error(&err),
                StateBackendError::JobSelectorChanged { .. }
            ),
            "{err:?}"
        );

        // The same refusal, read after the state machine has moved past it: the row it
        // describes no longer exists, so the job must not be failed for it.
        handle_unhandled_message(
            "job_abc",
            "pipeline_1",
            JobMessage::ConfigRefused(RefusedConfig::new(selector_changed(), 3, version)),
        )
        .expect("a superseded refusal must be discarded, not turned into a job failure");

        // and every other message is still ignored, so routing them here changes nothing
        handle_unhandled_message(
            "job_abc",
            "pipeline_1",
            JobMessage::TaskStarted {
                worker_id: WorkerId(1),
                task_id: 1,
                subtask_idx: 0,
            },
        )
        .expect("an ordinary unhandled message must still be ignored");
    }

    /// Finding 2, the exact race the previous review asked for.
    ///
    /// A refusal is queued, not applied in place, so between the poll that raises it and
    /// the state that reads it the operator can repair the row — which is the remedy the
    /// refusal itself asks for. The queue is FIFO and a message already in it cannot be
    /// retracted, so the only thing that can save the repaired job is the receiver being
    /// able to tell that the refusal it is holding no longer describes the job.
    ///
    /// Round 4 coalesced repeated refusals, which fixed repeated-send backpressure but not
    /// this: the valid poll cleared the sender's bookkeeping and left the older message in
    /// the queue, and every state turned a `ConfigRefused` into an unconditional fatal
    /// error. The repaired job still failed.
    #[tokio::test]
    async fn a_row_repaired_before_the_refusal_is_read_does_not_fail_the_job() {
        let current = running_config(StateBackendSelector::Parquet);
        let (mut sm, mut rx) = state_machine(current.clone(), StateBackendSelector::Parquet);

        // The bad poll. Nothing has read the job's queue yet.
        sm.update(
            polled(
                StateBackendSelector::Parquet,
                current.clone(),
                Some(selector_changed()),
            ),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;

        // The operator repairs the row and the next poll accepts it — still before the
        // state task has drained anything.
        let mut repaired = running_config(StateBackendSelector::Parquet);
        repaired.env_vars = serde_json::json!({ "A": "1" });
        sm.update(
            polled(StateBackendSelector::Parquet, repaired.clone(), None),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;

        // Only now does the state task read its queue, oldest first.
        let queued = rx
            .try_recv()
            .expect("the refusal was queued before the repair");
        handle_unhandled_message("job_abc", "pipeline_1", queued)
            .expect("a repaired row must not be failed by the refusal that preceded it");

        match rx
            .try_recv()
            .expect("and the repaired configuration must still be delivered")
        {
            JobMessage::ConfigUpdate(c) => assert_eq!(c, repaired),
            other => panic!("expected the repaired config update, got {other:?}"),
        }
        assert_eq!(sm.config.read().unwrap().0, repaired);
    }

    /// The same race in the shape the refusal's own remedy produces.
    ///
    /// The operator reads "stop the job and create a new one under the new backend" and
    /// adds a stop to the row that is still bad. The stop is queued behind a refusal raised
    /// on an earlier poll; if that older message still fails the job, the stop it was
    /// answering never runs — the job ends in `Failed` rather than `Stopped` and loses the
    /// final checkpoint a `checkpoint` stop asked for.
    #[tokio::test]
    async fn a_stop_answering_a_refusal_supersedes_the_refusal_already_queued() {
        let current = running_config(StateBackendSelector::Parquet);
        let (mut sm, mut rx) = state_machine(current.clone(), StateBackendSelector::Parquet);

        sm.update(
            polled(
                StateBackendSelector::Parquet,
                current.clone(),
                Some(selector_changed()),
            ),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;

        let mut with_stop = running_config(StateBackendSelector::Parquet);
        with_stop.stop_mode = StopMode::checkpoint;
        sm.update(
            polled(
                StateBackendSelector::Parquet,
                with_stop,
                Some(selector_changed()),
            ),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;

        let queued = rx.try_recv().expect("the refusal was queued first");
        handle_unhandled_message("job_abc", "pipeline_1", queued).expect(
            "a refusal that is being answered by a stop must not fail the job before the \
             stop is executed",
        );

        match rx.try_recv().expect("the stop must still be delivered") {
            JobMessage::ConfigUpdate(c) => {
                assert_eq!(c.stop_mode, StopMode::checkpoint);
                assert_eq!(c.state_backend, StateBackendSelector::Parquet);
            }
            other => panic!("expected the stop, got {other:?}"),
        }
    }

    /// Finding 1's second half, on the real writer.
    ///
    /// `start` recorded the execution selector into `job_statuses.state_context` before it
    /// had even tried to load the job's program, so a job the controller could not start
    /// was left with a recorded execution it never had. A recorded selector outranks the
    /// configuration row by design, so that guess could not be corrected by any later poll:
    /// the row was pinned to a backend the job had never used, and nothing downstream —
    /// checkpoint validation, worker startup — ever got the chance to reject it.
    #[tokio::test]
    async fn no_execution_selector_is_recorded_for_a_job_that_could_not_be_started() {
        let db = sqlite_startable_job("Running", 1);
        let mut sm = state_machine_with(
            running_config(StateBackendSelector::StateEngine),
            StateBackendSelector::StateEngine,
            None,
            db.clone(),
        );

        sm.start(job_status(3), shutdown_guard()).await;

        assert!(
            sm.done(),
            "the program cannot be loaded, so there is no state task and start must retry"
        );
        assert!(
            sm.sender().is_none(),
            "and no queue is advertised for a state task that never took the receiving end"
        );
        assert_eq!(
            recorded_status(&db),
            ("Compiling".to_string(), None),
            "the recovered state is still written exactly as before, but a job with no \
             execution must have no recorded execution selector"
        );
    }

    /// The other direction: a job that really does start records its selector, which is
    /// what a restarted controller recovers instead of re-reading the editable row.
    #[tokio::test]
    async fn a_job_that_starts_records_the_selector_its_execution_runs_with() {
        for selector in [
            StateBackendSelector::Parquet,
            StateBackendSelector::StateEngine,
        ] {
            // `Stopped` is terminal, so the state task this spawns has nothing to do but
            // tear a cluster down through `RecordingScheduler`.
            let db = sqlite_startable_job("Stopped", 2);
            let mut sm = state_machine_with(running_config(selector), selector, None, db.clone());

            let mut status = job_status(3);
            status.state = "Stopped".to_string();
            sm.start(status, shutdown_guard()).await;

            assert!(
                !sm.done(),
                "the program loads, so a state task must be running"
            );
            assert_eq!(
                recorded_status(&db),
                ("Stopped".to_string(), Some(selector.as_str().to_string()))
            );
        }
    }

    /// Finding 3. `Delivery::Inactive` is not delivery.
    ///
    /// `start` creates the job's channel and then finds it cannot load the program, so
    /// nothing takes the receiving end: the job has no state task, while `start` explicitly
    /// promises to retry on the next poll. A refused row that also asks the job to stop
    /// reaches `request_stop` in exactly that state.
    ///
    /// Recording the stop mode against a state machine that cannot execute it made every
    /// later poll return early on "that mode is already stored" while nothing ever stopped
    /// the job — a `checkpoint`, `graceful`, `immediate` or `force` stop that disappeared
    /// permanently. The accepted-update branch already restarted the state task on
    /// `Inactive`; this is the refused row's equivalent.
    #[tokio::test]
    async fn a_refused_rows_stop_is_not_swallowed_by_a_state_machine_that_cannot_run_it() {
        for stop_mode in [
            StopMode::checkpoint,
            StopMode::graceful,
            StopMode::immediate,
            StopMode::force,
        ] {
            let db = sqlite_startable_job("Running", 1);
            let current = running_config(StateBackendSelector::Parquet);
            let mut sm = state_machine_with(
                current.clone(),
                StateBackendSelector::Parquet,
                None,
                db.clone(),
            );

            let mut refused = running_config(StateBackendSelector::Parquet);
            refused.stop_mode = stop_mode;

            for poll in 0..3 {
                sm.update(
                    polled(
                        StateBackendSelector::Parquet,
                        refused.clone(),
                        Some(selector_changed()),
                    ),
                    job_status(current.restart_nonce),
                    &shutdown_guard(),
                )
                .await;

                assert!(
                    sm.done(),
                    "{stop_mode:?} poll {poll}: the program still cannot be loaded, so no \
                     state task can exist to execute the stop"
                );
                assert_eq!(
                    sm.config.read().unwrap().0.stop_mode,
                    StopMode::none,
                    "{stop_mode:?} poll {poll}: a stop nothing can execute must not be \
                     recorded as issued — recording it is what makes the next poll return \
                     early on it and strand the job"
                );
            }

            // The job's state machine comes back up, as `start` promised it would. The
            // same row is still asking for the same stop, and now it is executed — under
            // the configuration the job is running with, not the refused row's.
            let (tx, mut rx) = channel(16);
            sm.tx = Some(tx);
            sm.update(
                polled(
                    StateBackendSelector::Parquet,
                    refused.clone(),
                    Some(selector_changed()),
                ),
                job_status(current.restart_nonce),
                &shutdown_guard(),
            )
            .await;

            let stored = sm.config.read().unwrap().0.clone();
            assert_eq!(stored.stop_mode, stop_mode);
            assert_eq!(stored.state_backend, StateBackendSelector::Parquet);
            assert_eq!(stored.restart_nonce, current.restart_nonce);

            match rx.try_recv().expect("the stop must reach the job") {
                JobMessage::ConfigUpdate(c) => {
                    assert_eq!(c.stop_mode, stop_mode);
                    assert_eq!(c.state_backend, StateBackendSelector::Parquet);
                }
                other => panic!("expected the stop, got {other:?}"),
            }
            assert!(
                rx.try_recv().is_err(),
                "and it must not also arrive as a refusal, which would fail the job \
                 instead of stopping it"
            );
        }
    }

    /// Round 6's finding: the third route on which `Delivery::Inactive` was mistaken for
    /// delivery, and the only one that could strand a job for good.
    ///
    /// A cold controller adopts a still-running job whose row now names a different
    /// backend. `start` cannot load the program — a transient `get_program` or database
    /// failure — so it leaves the job with no state task and explicitly promises to retry
    /// on the next poll. The refused row asks for no stop, so `request_stop`'s own restart
    /// (round 5) never runs; instead the refusal branch returned before the ordinary
    /// inactive `restart_if_needed` path and recorded the refusal as sent to a queue that
    /// did not exist. Every later poll then short-circuited on "already sent": program
    /// loading was never retried, the job was never adopted, and its workers kept running
    /// with nothing administering them — even after the database recovered.
    ///
    /// Round 6 stopped at "a state task now exists", which is why it could not see round 7's
    /// finding: what the adopted task went on to do. The task is driven to a standstill here,
    /// so the retry is proved by the job actually running and being failed by the refusal it
    /// was restarted to receive — and proved to have rescheduled nothing on the way.
    #[tokio::test]
    async fn cold_adoption_is_retried_for_a_refused_row_and_the_adopted_job_never_schedules() {
        let db = sqlite_startable_job("Running", 2);
        let scheduler = Arc::new(RecordingScheduler::default());
        // Held for the whole test: the adopted task has to run, not just exist.
        let shutdown = LiveShutdown::new();
        program_loadable(&db, false);

        let current = running_config(StateBackendSelector::Parquet);
        // The shape round 5's fix never sees: refused, and asking for no stop.
        let refused = running_config(StateBackendSelector::Parquet);
        assert_eq!(refused.stop_mode, StopMode::none);

        let refused_poll = || {
            polled(
                StateBackendSelector::Parquet,
                refused.clone(),
                Some(selector_changed()),
            )
        };

        // Cold adoption itself, through the real constructor.
        let mut sm = StateMachine::new(
            refused_poll(),
            job_status(current.restart_nonce),
            db.clone(),
            scheduler.clone(),
            test_cluster_id(),
            shutdown.guard().clone_temporary(),
            Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        )
        .await;

        for poll in 0..3 {
            assert!(
                sm.done(),
                "poll {poll}: the program still cannot be loaded, so there is no state task"
            );
            assert_eq!(
                recorded_status(&db).1,
                None,
                "poll {poll}: and no execution has begun, so none is recorded"
            );

            sm.update(
                refused_poll(),
                job_status(current.restart_nonce),
                shutdown.guard(),
            )
            .await;
        }

        // The dependency recovers. The row is unchanged and still refused, and nothing
        // else about the job has changed, so the only thing that can adopt it now is the
        // retry `start` promised.
        program_loadable(&db, true);
        sm.update(
            refused_poll(),
            job_status(current.restart_nonce),
            shutdown.guard(),
        )
        .await;

        assert!(
            !sm.done(),
            "once the program loads, the still-live job must finally be adopted: a refusal \
             it cannot be told about is not a reason to stop trying to reach it"
        );
        assert_eq!(
            recorded_status(&db).1,
            Some("parquet".to_string()),
            "and the execution that has now begun is recorded under the job's own \
             immutable selector, never the refused row's"
        );

        // What the adopted task then does. Round 6 asserted only that it existed.
        let writes = drive_to_completion(&sm, &db).await;
        assert!(
            writes
                .iter()
                .all(|(state, generation)| state != "Scheduling" && *generation == 1),
            "the job is adopted so the refusal can be applied to it, not so the refused row \
             can reschedule it: it may never reach `Scheduling`, and the generation it would \
             reschedule under may never advance; wrote {writes:?}"
        );
        assert!(
            writes.ends_with(&[("Failing".to_string(), 1), ("Failed".to_string(), 1)]),
            "and the adoption ends in the failure the refusal asked for; wrote {writes:?}"
        );
        assert_eq!(
            scheduler.started.lock().unwrap().as_slice(),
            [],
            "no replacement workers for a configuration that must be adopted nowhere"
        );
        assert_eq!(
            scheduler.stopped.lock().unwrap().as_slice(),
            [("job_abc".to_string(), Some(1))],
            "and the only teardown is the terminal one, under the generation the job already \
             had — never `Scheduling`'s pre-scheduling `stop_workers(_, None, _)`"
        );
    }

    /// The delivery half of the same route. A refusal offered to a job with no state task
    /// is not delivered, so it must not be recorded as delivered: it stays pending and is
    /// offered again until something can receive it.
    ///
    /// The version matters as much as the message. The refusal that finally arrives has to
    /// be one the state machine still holds, or `handle_unhandled_message` discards it and
    /// the job keeps running under a row the controller has already rejected.
    ///
    /// This drives no state task by construction: it injects a sender so it can read the
    /// message off the queue and inspect it, which a real task would consume instead. That
    /// is why it also checks the refusal is on the job's [`RefusalGate`] the whole time —
    /// the queue is what a state blocked on `recv` gets, and the gate is what a state that
    /// does not receive gets, and a pending refusal has to be on both. What the gate then
    /// does to a running task is covered behaviourally by
    /// `a_known_refusal_fails_the_restarted_task_before_it_can_reschedule_the_job`.
    #[tokio::test]
    async fn a_refusal_with_no_state_task_gates_it_and_is_delivered_once_it_has_one() {
        let current = running_config(StateBackendSelector::Parquet);
        let mut sm = state_machine_with(
            current.clone(),
            StateBackendSelector::Parquet,
            None,
            sqlite_startable_job("Running", 1),
        );

        for poll in 0..3 {
            sm.update(
                polled(
                    StateBackendSelector::Parquet,
                    current.clone(),
                    Some(selector_changed()),
                ),
                job_status(current.restart_nonce),
                &shutdown_guard(),
            )
            .await;
            assert!(
                sm.done(),
                "poll {poll}: the program still cannot be loaded, so nothing can receive \
                 the refusal"
            );
            assert_eq!(
                sm.refusal_gate
                    .clone()
                    .take()
                    .and_then(RefusedConfig::into_current_error),
                Some(selector_changed()),
                "poll {poll}: a refusal nothing can receive must still stop the first state \
                 of whatever task comes up next"
            );
        }

        // The job's state machine comes back up, as `start` promised it would, and the
        // same unchanged row is polled again.
        let (tx, mut rx) = channel(16);
        sm.tx = Some(tx);
        sm.update(
            polled(
                StateBackendSelector::Parquet,
                current.clone(),
                Some(selector_changed()),
            ),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;

        assert_eq!(
            refusal_if_current(
                rx.try_recv()
                    .expect("the refusal must reach the state task that can finally act on it")
            ),
            Some(selector_changed()),
            "and at a version the state machine still holds, so the job is failed rather \
             than left running under a row the controller rejected"
        );
        assert!(
            rx.try_recv().is_err(),
            "one unusable row still produces one refusal, not one per poll it was pending"
        );
        assert_eq!(
            sm.config.read().unwrap().0,
            current,
            "and the refused row is still adopted nowhere"
        );
    }

    /// Pending must not mean immortal. Keeping an undelivered refusal alive across polls is
    /// only safe because a repair supersedes it exactly as it supersedes a delivered one —
    /// otherwise round 6's fix would reintroduce round 5's: a job that comes back up after
    /// the operator fixed the row would be failed for a configuration that no longer
    /// exists.
    #[tokio::test]
    async fn a_pending_refusal_is_superseded_by_the_repair_that_answers_it() {
        let current = running_config(StateBackendSelector::Parquet);
        let mut sm = state_machine_with(
            current.clone(),
            StateBackendSelector::Parquet,
            None,
            sqlite_startable_job("Running", 1),
        );

        // Refused while the job has no state task: nothing receives this one.
        sm.update(
            polled(
                StateBackendSelector::Parquet,
                current.clone(),
                Some(selector_changed()),
            ),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;
        assert!(sm.done(), "the program cannot be loaded");

        // The operator repairs the row, and only then does a state task exist.
        let (tx, mut rx) = channel(16);
        sm.tx = Some(tx);
        sm.update(
            polled(StateBackendSelector::Parquet, current.clone(), None),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;

        assert!(
            rx.try_recv().is_err(),
            "a refusal that was still pending when the row was repaired must not be \
             delivered to the job the repair saved"
        );

        // The control: the mechanism is still live, so a row that goes bad again is
        // refused afresh and does reach the job.
        sm.update(
            polled(
                StateBackendSelector::Parquet,
                current.clone(),
                Some(selector_changed()),
            ),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;
        assert_eq!(
            refusal_if_current(rx.try_recv().expect("the new refusal must be delivered")),
            Some(selector_changed()),
            "the test would not detect the supersession if no refusal ever arrived"
        );
    }

    /// Round 7's finding, on the route the reviewer traced: a job the controller has already
    /// rejected must not be able to reschedule its own live execution on the way to being
    /// failed.
    ///
    /// Round 6 made the refused-row branch restart the job's state task so the refusal could
    /// be delivered at all. But a controller-mode `Running` job restarts into [`Compiling`],
    /// which never reads the job's channel and advances straight to [`Scheduling`] — and
    /// `Scheduling` increments and persists the generation, stops the live workers, starts
    /// replacements and prepares checkpoint recovery *before* its first `ctx.rx.recv` can
    /// turn the refusal fatal. Queueing the refusal ahead of that task did not help; it had
    /// to be applied before the task's first state body ran.
    ///
    /// So this drives the spawned task rather than observing that one exists, and asserts
    /// the four things it must not have done: no generation advance, no `Scheduling`
    /// teardown, no worker start, and no checkpoint restore — the last because the job never
    /// enters the state that prepares one.
    #[tokio::test]
    async fn a_known_refusal_fails_the_restarted_task_before_it_can_reschedule_the_job() {
        let db = sqlite_startable_job("Running", 2);
        let scheduler = Arc::new(RecordingScheduler::default());
        let current = running_config(StateBackendSelector::Parquet);
        // Held for the whole test: the spawned task has to run, not just exist.
        let shutdown = LiveShutdown::new();

        // An inactive state machine for a job whose status says it is still running: what a
        // controller is left with after `start` could not load the program, and what round
        // 6's `restart_if_needed` exists to bring back up.
        let mut sm = state_machine_with(
            current.clone(),
            StateBackendSelector::Parquet,
            None,
            db.clone(),
        );
        sm.scheduler = scheduler.clone();
        assert!(sm.done(), "the job starts with no state task");

        // The refused row asks for no stop, so this is round 6's branch exactly.
        sm.update(
            polled(
                StateBackendSelector::Parquet,
                current.clone(),
                Some(selector_changed()),
            ),
            job_status(current.restart_nonce),
            shutdown.guard(),
        )
        .await;

        assert!(
            !sm.done(),
            "round 6's liveness property: the job is still adopted, because a refusal it \
             cannot be told about is not a reason to stop trying to reach it"
        );

        let writes = drive_to_completion(&sm, &db).await;

        assert_eq!(
            writes,
            [
                ("Compiling".to_string(), 1),
                ("Failing".to_string(), 1),
                ("Failed".to_string(), 1),
            ],
            "the adopted task must be failed by the refusal at its very first state: it may \
             never reach `Scheduling`, and the generation it would reschedule under may never \
             advance"
        );
        assert_eq!(
            scheduler.started.lock().unwrap().as_slice(),
            [],
            "and no replacement workers may be started for a configuration that must be \
             adopted nowhere"
        );
        assert_eq!(
            scheduler.stopped.lock().unwrap().as_slice(),
            [("job_abc".to_string(), Some(1))],
            "the only teardown is the terminal one, under the generation the job already had \
             — `Scheduling`'s pre-scheduling `stop_workers(_, None, _)` must not happen"
        );
        assert_eq!(
            recorded_status(&db),
            ("Failed".to_string(), Some("parquet".to_string())),
            "and the execution is recorded under the job's own immutable selector, never the \
             refused row's"
        );
        assert!(
            recorded_failure(&db)
                .expect("the job must be failed")
                .contains("refused"),
            "and failed for the refusal, not for something the rescheduling attempt hit"
        );
    }

    /// The same guarantee on the constructor route, where nothing had recorded the refusal
    /// before the job's state task existed at all.
    ///
    /// [`StateMachine::new`] adopts a job and *then* looks at the row's refusal, so a cold
    /// controller picking up a still-running job whose row already names another backend
    /// started a task into `Compiling` before anything about the refusal had been recorded.
    /// The refusal now reaches the job's gate before the adoption starts anything.
    #[tokio::test]
    async fn a_cold_adopted_job_is_failed_by_its_refusal_before_it_schedules_anything() {
        let db = sqlite_startable_job("Running", 2);
        let scheduler = Arc::new(RecordingScheduler::default());
        let current = running_config(StateBackendSelector::Parquet);
        // Held for the whole test: the spawned task has to run, not just exist.
        let shutdown = LiveShutdown::new();

        let sm = StateMachine::new(
            polled(
                StateBackendSelector::Parquet,
                current.clone(),
                Some(selector_changed()),
            ),
            job_status(current.restart_nonce),
            db.clone(),
            scheduler.clone(),
            test_cluster_id(),
            shutdown.guard().clone_temporary(),
            Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        )
        .await;

        assert!(
            !sm.done(),
            "the still-running job is adopted — the refusal has to reach it, and a job with \
             no state task can be told nothing"
        );

        assert_eq!(
            drive_to_completion(&sm, &db).await,
            [
                ("Compiling".to_string(), 1),
                ("Failing".to_string(), 1),
                ("Failed".to_string(), 1),
            ],
            "but the adopted task is failed at its first state, so a row the controller has \
             already rejected never reschedules the execution it was adopted to administer"
        );
        assert_eq!(scheduler.started.lock().unwrap().as_slice(), []);
        assert_eq!(
            scheduler.stopped.lock().unwrap().as_slice(),
            [("job_abc".to_string(), Some(1))]
        );
    }

    /// The other direction, and the control that makes the two tests above mean something.
    ///
    /// The gate must fire for a refused job and for nothing else. An ordinary job — same
    /// fixture, same scheduler, same states — must still reach [`Scheduling`], advance its
    /// generation there and clear whatever cluster was there before it. If it did not, "no
    /// generation advance and no teardown" above would be a statement about the harness
    /// rather than about the refusal.
    ///
    /// The task then panics inside the scheduler's `start_workers`, which this test asks for:
    /// the panic is expected, is printed by the runtime, and is what lets the spawned task
    /// finish so the writes can be read back. It happens *after* both things asserted here,
    /// and it is why they are the generation and the teardown rather than the `start_workers`
    /// call itself.
    #[tokio::test]
    async fn an_unrefused_job_still_reaches_scheduling_and_advances_its_generation() {
        let db = sqlite_startable_job("Running", 2);
        let scheduler = Arc::new(RecordingScheduler::panicking());
        let current = running_config(StateBackendSelector::Parquet);
        // Held for the whole test: the spawned task has to run, not just exist.
        let shutdown = LiveShutdown::new();

        let sm = StateMachine::new(
            polled(StateBackendSelector::Parquet, current.clone(), None),
            job_status(current.restart_nonce),
            db.clone(),
            scheduler.clone(),
            test_cluster_id(),
            shutdown.guard().clone_temporary(),
            Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        )
        .await;
        assert!(!sm.done(), "an unrefused running job is adopted");

        let writes = drive_to_completion(&sm, &db).await;
        assert!(
            writes.starts_with(&[
                ("Compiling".to_string(), 1),
                ("Scheduling".to_string(), 1),
                ("Scheduling".to_string(), 2),
            ]),
            "a job with nothing wrong with it must reach `Scheduling` and advance its \
             generation there, which is exactly what the refused job above must not do; \
             wrote {writes:?}"
        );
        assert_eq!(
            scheduler.stopped.lock().unwrap().as_slice(),
            [("job_abc".to_string(), None)],
            "and it must clear the existing cluster before scheduling — the destructive \
             teardown the refused job must never reach"
        );
    }

    /// The poll thread reaching the job in the one instant round 7's gate did not cover: the
    /// snapshot [`execute_state`] takes before the state body has already been read, and
    /// `Scheduling`'s preamble has not yet done anything.
    ///
    /// This is a barrier, not a race. The publication happens at a point this state controls,
    /// strictly after the gate snapshot and strictly before the first statement of the real
    /// [`Scheduling::next`] it then delegates to, so the interleaving under test is the one
    /// that runs, every time, on any runtime. It is also the *latest* such point: a
    /// publication that lands here has beaten the preamble by nothing at all, and must still
    /// stop it.
    #[derive(Debug)]
    struct PublishesAfterTheGateSnapshot(RefusedConfig);

    #[async_trait::async_trait]
    impl State for PublishesAfterTheGateSnapshot {
        /// Stays: this fixture exists to publish a refusal at a chosen instant, and the rows
        /// that use it build no lifecycle actor, so the boundary never asks.
        fn leave_for_stop(self: Box<Self>, _config: &JobConfig) -> LeavingForStop {
            LeavingForStop::Stays(self)
        }

        fn name(&self) -> &'static str {
            "PublishesAfterTheGateSnapshot"
        }

        async fn next(self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
            assert!(
                ctx.refusal_gate.clone().take().is_none(),
                "the state body starts with the gate clear: `execute_state`'s snapshot is \
                 spent, which is the whole premise of this test"
            );
            ctx.refusal_gate
                .publish(&admitted(&ctx.refusal_gate), self.0.clone());
            Box::new(Scheduling {}).next(ctx).await
        }
    }

    /// A refusal published after the gate snapshot must still stop the scheduling preamble.
    ///
    /// Round 7 made [`execute_state`] apply a known refusal before every state body, and
    /// argued that covered a job whose state task was already live. It did not: `take`
    /// clones under its lock and releases it, and `Scheduling` then persists an incremented
    /// generation, tears down the live cluster, starts replacements and prepares checkpoint
    /// recovery — awaiting throughout — without looking at the gate again. A refusal raised
    /// anywhere in there was not read until the next state, which is after all of it.
    ///
    /// The four things the reviewer named are asserted here, and all four are `zero`: no
    /// generation was persisted, no unscoped teardown (`stop_workers(_, None, _)`) was asked
    /// for, no replacement workers were started, and no recovery was prepared — the last
    /// because preparing it is strictly after starting workers, which never happened.
    ///
    /// Without the interlock this test does not merely mis-assert: the preamble runs on
    /// through `start_workers` into the checkpoint recovery this fixture has laid nothing
    /// down for, and can die there. The unwind is caught so that the failure reports what was
    /// done to the job rather than wherever the preamble happened to stop.
    #[tokio::test]
    async fn a_refusal_published_after_the_gate_snapshot_still_stops_the_scheduling_preamble() {
        let db = sqlite_startable_job("Scheduling", 2);
        let refusal = RefusedConfig::new(selector_changed(), 1, Arc::new(AtomicU64::new(1)));

        let mut harness = Harness::new(3).with_db(db.clone());
        harness.status.state = "Scheduling".to_string();
        let scheduler = harness.scheduler.clone();
        let ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );

        let outcome = std::panic::AssertUnwindSafe(async {
            let (next, ctx) =
                execute_state(Box::new(PublishesAfterTheGateSnapshot(refusal)), ctx).await;
            (next.map(|s| s.name().to_string()), ctx.status.generation)
        })
        .catch_unwind()
        .await;

        let Ok((next, generation)) = outcome else {
            panic!(
                "the preamble ran for a refused configuration and got as far as \
                 `start_workers`; it persisted {:?} and asked for the teardowns {:?}",
                state_writes(&db),
                scheduler.stopped.lock().unwrap()
            );
        };

        assert_eq!(
            next.as_deref(),
            Some("Failing"),
            "a refusal published after the gate snapshot must still fail the job"
        );
        assert_eq!(
            generation, 1,
            "and the generation this scheduling attempt would have run under must not even \
             be incremented in memory"
        );
        assert_eq!(
            state_writes(&db),
            [("Failing".to_string(), 1)],
            "the only status write is the failure itself: no generation was ever persisted"
        );
        assert_eq!(
            scheduler.stopped.lock().unwrap().as_slice(),
            [],
            "no unscoped teardown — `Scheduling`'s `stop_workers(_, None, _)` is what \
             destroys a live execution"
        );
        assert_eq!(
            scheduler.started.lock().unwrap().as_slice(),
            [],
            "no replacement workers, and so no checkpoint recovery either: preparing it is \
             strictly after starting them"
        );
    }

    /// The control for the interlock, and for the two properties an interlock can break.
    ///
    /// An ordinary job must still run its scheduling preamble — the admission must not stall
    /// or deadlock it — and the assertions above must be about the refusal rather than about
    /// a harness in which nothing schedules anyway. So the same path is run with the gate
    /// clear, and it must reach the effects the refused job reached none of.
    ///
    /// It gets as far as the scheduler's `start_workers`, which this test asks to panic; that
    /// panic is the proof the preamble ran, and is also the second thing checked here. A
    /// `tokio` mutex does not poison, so a state body that panics under the admission leaves
    /// the job refusable rather than wedged for the life of the controller — which a `std`
    /// mutex would not have.
    ///
    /// The panic is asked for explicitly, from the scheduler, rather than being inherited from
    /// whatever the preamble happened to trip over. It used to come from `start_workers`
    /// reading a process-wide cluster identity no test had populated — a premise that any
    /// test which populated it would silently remove, depending on the order the binary ran
    /// them in. That identity is now handed to the state ([`JobContext::cluster_id`]) and no
    /// test populates anything process-wide, but the explicit panic stays: what this test
    /// asserts is that a panic *under the admission* releases it, so it has to own the panic
    /// rather than depend on one.
    #[tokio::test]
    async fn an_unrefused_job_still_schedules_and_a_panic_under_the_admission_releases_it() {
        let db = sqlite_startable_job("Scheduling", 2);

        let mut harness = Harness::new(3)
            .with_db(db.clone())
            .with_scheduler(RecordingScheduler::panicking());
        harness.status.state = "Scheduling".to_string();
        let scheduler = harness.scheduler.clone();
        let gate = harness.refusal_gate.clone();
        let ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );

        let outcome = std::panic::AssertUnwindSafe(async {
            execute_state(Box::new(Scheduling {}), ctx).await;
        })
        .catch_unwind()
        .await;

        assert!(
            outcome.is_err(),
            "the control only means anything if the unrefused job really ran the preamble \
             through to `start_workers`; it wrote {:?}",
            state_writes(&db)
        );
        assert_eq!(
            state_writes(&db),
            [("Scheduling".to_string(), 2)],
            "an unrefused job advances and persists its generation exactly as before: the \
             admission is an interlock, not a stall"
        );
        assert_eq!(
            scheduler.stopped.lock().unwrap().as_slice(),
            [("job_abc".to_string(), None)],
            "and clears the cluster it is replacing"
        );
        assert!(
            gate.admit_publication().is_some(),
            "and a state body that panics under the admission releases it: a refusal raised \
             after this must still be publishable"
        );
    }

    /// The other half of the interlock: a refusal that arrives *during* a preamble.
    ///
    /// Publication takes the same admission `Scheduling` holds across its preamble, and takes
    /// it without ever waiting — the update thread calls `refuse_config` under the global job
    /// map. So a refusal raised mid-preamble is not published, not queued, and above all not
    /// recorded: round 6's rule for a refusal nothing can receive, applied to one nothing can
    /// publish. The next 500ms poll offers the same refusal, at the same version, and by then
    /// the preamble is over and the job's own `recv` is what acts on it.
    ///
    /// The version is the part that has to be left alone rather than merely re-derived.
    /// Advancing it on a poll that publishes nothing would supersede whatever refusal is
    /// already on the gate or in the queue, and a state reading that one would discard it as
    /// stale — losing the refusal to the very contention that was meant to defer it.
    #[tokio::test]
    async fn a_refusal_raised_during_a_scheduling_preamble_is_deferred_rather_than_lost() {
        let current = running_config(StateBackendSelector::Parquet);
        let (mut sm, mut rx) = state_machine(current.clone(), StateBackendSelector::Parquet);
        let mut gate = sm.refusal_gate.clone();

        // `Scheduling`, in its preamble: it holds the job's admission from before its first
        // effect until after its last.
        let (preamble, refusal) = gate.admit_scheduling().await;
        assert!(
            refusal.is_none(),
            "the preamble was admitted with the gate clear, which is the interleaving \
             this test is about"
        );

        sm.refuse_config(selector_changed());

        assert!(
            sm.refusal_gate.clone().take().is_none(),
            "nothing may be published into a preamble that has already started: the states \
             after it read the gate, and the preamble itself is past reading anything"
        );
        assert!(
            rx.try_recv().is_err(),
            "and nothing is queued for it either, so the poll leaves no trace at all"
        );
        assert!(
            sm.refusal.is_none(),
            "and nothing is recorded as delivered, so the next poll does not short-circuit \
             on a refusal that was never raised"
        );
        assert_eq!(
            sm.refusal_version.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "and the refusal version is untouched: advancing it here would supersede a \
             refusal already on the gate or in the queue"
        );

        // The preamble finishes; the next poll finds the same row still bad.
        drop(preamble);
        sm.refuse_config(selector_changed());

        assert_eq!(
            sm.refusal_gate
                .clone()
                .take()
                .and_then(RefusedConfig::into_current_error),
            Some(selector_changed()),
            "the deferred refusal is published the moment the preamble is over, so it gates \
             every state after it"
        );
        assert_eq!(
            refusal_if_current(rx.try_recv().expect("the refusal must be delivered too")),
            Some(selector_changed()),
            "and is delivered to the state that is now reading its channel"
        );
        assert_eq!(
            sm.refusal_version.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "at the first version: this is the same refusal deferred, not a second one"
        );
    }

    // ---------------------------------------------------------------------------------------
    // Round 9: the crossings after the preamble.
    //
    // `Scheduling` does not stop being destructive when its preamble ends. It still has to send
    // every worker its `StartExecution`, and — for a checkpoint that died mid-commit — publish
    // that checkpoint's commits. Both are externally visible and neither can be taken back.
    //
    // The tests below are end-to-end on purpose: a real gRPC worker on a real socket, the real
    // `Scheduling::next`, and the reviewer's own scenario — enough queued messages ahead of the
    // refusal to satisfy each receive loop, so the loop breaks without ever reading it. What is
    // asserted is what arrived at the worker.
    // ---------------------------------------------------------------------------------------

    /// The points inside one run of `Scheduling::next` that a test can wait for.
    ///
    /// These are what make the tests below deterministic rather than timed. Each is announced
    /// from inside a piece of work the state does, so "publish the refusal after the preamble
    /// and before the fan-out" is an ordering the test enforces rather than one it hopes for.
    #[derive(Default)]
    struct SchedulingBarriers {
        /// The scheduler has been asked for the replacement cluster: the last effect of the
        /// destructive preamble, and so a point from which the preamble's admission is about
        /// to be released.
        workers_started: tokio::sync::Notify,
        /// A worker has been asked to start executing: inside the second region, and so after
        /// its gate read.
        execution_started: tokio::sync::Notify,
    }

    /// What a worker was actually asked to do.
    #[derive(Default)]
    struct WorkerCalls {
        /// One entry per `StartExecution`, carrying the selector it was stamped with.
        start_execution: Mutex<Vec<String>>,
        /// One entry per `Commit`, carrying its epoch.
        commit: Mutex<Vec<u64>>,
        /// One entry per `StopExecution`, carrying the mode it asked for. This is what "the
        /// job was actually stopped" means for a controller-mode job: a request that arrived
        /// at a worker, not a function that returned.
        stop_execution: Mutex<Vec<i32>>,
    }

    impl WorkerCalls {
        fn started(&self) -> Vec<String> {
            self.start_execution.lock().unwrap().clone()
        }

        fn committed(&self) -> Vec<u64> {
            self.commit.lock().unwrap().clone()
        }

        fn stopped(&self) -> Vec<rpc::StopMode> {
            self.stop_execution
                .lock()
                .unwrap()
                .iter()
                .map(|mode| rpc::StopMode::try_from(*mode).expect("a stop mode a worker was sent"))
                .collect()
        }
    }

    /// How a [`FakeWorker`] answers `StartExecution`.
    #[derive(Clone, Default)]
    enum StartsExecution {
        /// Records the call and accepts, which is what every round-9 test wants.
        #[default]
        Accepting,
        /// Fails the RPC — but not before the paused worker is inside its own handler, so the
        /// fan-out's failure always lands on a job that has another request outstanding.
        FailingOnce(Arc<tokio::sync::Notify>),
        /// Announces that it has been asked and then waits, so the controller has a
        /// `StartExecution` in flight for as long as the test wants one.
        Pausing(Arc<PausedWorker>),
        /// Blocks the thread *inside a single poll* until the test lets it go. This preserves
        /// the legacy/adversarial server shape that no client cancellation can reach; current
        /// production workers use `try_lock` and never create this window. See [`BlockedWorker`].
        Blocking(Arc<BlockedWorker>),
        /// Returns an ambiguous deadline once while server-side work carrying the request
        /// remains live, then acknowledges an idempotent retry of the same execution ID.
        AmbiguousOnce(Arc<AmbiguousStart>),
        /// Returns the production worker's definitive lock-contention response once, then
        /// accepts the retry. Both calls must carry the same execution ID.
        BusyOnce(Arc<BusyStart>),
        /// Answers `Unavailable` to every attempt, forever, and never applies anything.
        ///
        /// A worker that is reachable but permanently unable to settle — the shape a partition
        /// or a half-dead peer presents to the controller, and the one the retry loop used to
        /// spin on at 250ms for the life of the process.
        NeverSettling(Arc<NeverSettles>),
    }

    /// Counts the attempts a [`StartsExecution::NeverSettling`] worker refuses, and checks that
    /// every one of them replays the same attempt ID rather than inventing a new one.
    struct NeverSettles {
        calls: AtomicU64,
        expected_id: Mutex<Option<String>>,
    }

    impl NeverSettles {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicU64::new(0),
                expected_id: Mutex::new(None),
            })
        }

        fn remember_or_check(&self, id: &str) {
            assert!(!id.is_empty(), "every new controller attempt has an ID");
            let mut expected = self.expected_id.lock().unwrap();
            match expected.as_deref() {
                Some(expected) => assert_eq!(expected, id, "a retry must keep its attempt ID"),
                None => *expected = Some(id.to_string()),
            }
        }
    }

    struct BusyStart {
        calls: AtomicU64,
        expected_id: Mutex<Option<String>>,
    }

    impl BusyStart {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicU64::new(0),
                expected_id: Mutex::new(None),
            })
        }

        fn remember_or_check(&self, id: &str) {
            assert!(!id.is_empty(), "every new controller attempt has an ID");
            let mut expected = self.expected_id.lock().unwrap();
            match expected.as_deref() {
                Some(expected) => assert_eq!(expected, id, "a retry must keep its attempt ID"),
                None => *expected = Some(id.to_string()),
            }
        }
    }

    /// Models the ordering a client-side timeout creates: the client has an `Err`, but work
    /// carrying that request is still alive on the server. The original work and the retry
    /// rendezvous before applying, and the stable execution ID makes exactly one of them the
    /// application while the other is an acknowledgement.
    struct AmbiguousStart {
        calls: AtomicU64,
        expected_id: Mutex<Option<String>>,
        applied: std::sync::atomic::AtomicBool,
        retry_entered: tokio::sync::Notify,
        rendezvous: tokio::sync::Barrier,
    }

    impl AmbiguousStart {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicU64::new(0),
                expected_id: Mutex::new(None),
                applied: std::sync::atomic::AtomicBool::new(false),
                retry_entered: tokio::sync::Notify::new(),
                // The escaped first handler, its retry, and the test that releases both.
                rendezvous: tokio::sync::Barrier::new(3),
            })
        }

        fn remember_or_check(&self, id: &str) {
            assert!(!id.is_empty(), "every new controller attempt has an ID");
            let mut expected = self.expected_id.lock().unwrap();
            match expected.as_deref() {
                Some(expected) => assert_eq!(expected, id, "a retry must keep its attempt ID"),
                None => *expected = Some(id.to_string()),
            }
        }

        fn apply_once(&self) -> bool {
            !self.applied.swap(true, Ordering::SeqCst)
        }
    }

    /// A worker that has been asked to start executing and has not answered yet.
    ///
    /// This is the instrument for round 11: the question is whether a request the controller
    /// has stopped waiting for can still reach its worker, and the only way to ask it is to
    /// hold one open across the moment the controller gives up.
    struct PausedWorker {
        /// Fired from inside the handler, so no test has to guess when the request arrived.
        asked: tokio::sync::Notify,
        /// The same announcement, for the worker whose failure must land only once *this*
        /// request is outstanding. Separate from [`Self::asked`] because a `Notify` permit
        /// has one taker and these are two.
        asked_relay: Arc<tokio::sync::Notify>,
        /// Fired by a test to let the handler finish, if it still exists.
        released: tokio::sync::Notify,
        outcome: tokio::sync::watch::Sender<Option<PausedOutcome>>,
    }

    /// What became of a paused `StartExecution` handler.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum PausedOutcome {
        /// Its stream was cut off before it could apply the request: the controller took the
        /// call back.
        CutOff,
        /// It ran to completion, and the worker was told to start executing.
        Applied,
    }

    impl PausedWorker {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                asked: tokio::sync::Notify::new(),
                asked_relay: Arc::new(tokio::sync::Notify::new()),
                released: tokio::sync::Notify::new(),
                outcome: tokio::sync::watch::Sender::new(None),
            })
        }

        /// The signal a [`StartsExecution::FailingOnce`] worker waits on before it fails.
        fn asked_relay(&self) -> Arc<tokio::sync::Notify> {
            self.asked_relay.clone()
        }

        fn announce(&self) {
            self.asked.notify_one();
            self.asked_relay.notify_one();
        }

        fn settle(&self, outcome: PausedOutcome) {
            self.outcome.send_replace(Some(outcome));
        }

        /// Waits for the handler to have finished one way or the other.
        ///
        /// Both endings announce themselves — completion from the handler, cancellation from
        /// [`CutOffOnDrop`] — and a `watch` rather than a one-shot signal so that asking twice,
        /// or asking after the answer arrived, is the same question.
        async fn outcome(&self) -> PausedOutcome {
            let mut settled = self.outcome.subscribe();
            loop {
                if let Some(outcome) = *settled.borrow_and_update() {
                    return outcome;
                }
                settled.changed().await.unwrap();
            }
        }
    }

    /// A worker whose `StartExecution` handler blocks the thread inside one poll.
    ///
    /// Round 11's [`PausedWorker`] holds its request open at `Notify::notified().await`, which
    /// is a *cooperative* suspension point: the handler is parked between polls, so when the
    /// controller drops the client future `tonic` can drop the handler and the request really
    /// is taken back. That is why round 11's tests passed and the hole stayed open.
    ///
    /// The production handler used to take `self.state.phase.lock()` as its first statement.
    /// A future blocked *inside* `poll` cannot be dropped; the poll runs to its end whatever
    /// happened to the stream, and the worker could start later. Production now uses
    /// `try_lock`, but this instrument remains as defense-in-depth coverage for a legacy or
    /// independently implemented worker that still has the hostile shape.
    ///
    /// This reproduces exactly that: from the moment the handler is entered to the moment it
    /// returns there is no `.await`, and it blocks on a `std::sync::Mutex` in between. Nothing
    /// the controller does can take the request back, so the only question a test can ask is
    /// the one the invariant is actually about — what had the controller already done by the
    /// time the handler got through?
    struct BlockedWorker {
        /// The blocking point, and the handshake with the test in one lock: the handler
        /// records that it is inside before it waits, so "the request is unstoppable now" is a
        /// fact the test observes rather than a delay it hopes is long enough.
        state: Mutex<BlockedState>,
        changed: std::sync::Condvar,
        /// The signal a [`StartsExecution::FailingOnce`] sibling waits on. Fired by the test
        /// once the handler is provably inside, not by the handler itself.
        asked_relay: Arc<tokio::sync::Notify>,
        /// The job's gate, so the handler can answer "had a refusal been published by the time
        /// I started this worker?" from inside the worker. Set once the harness exists.
        gate: std::sync::OnceLock<RefusalGate>,
        saw_refusal: std::sync::atomic::AtomicBool,
        started: std::sync::atomic::AtomicBool,
    }

    #[derive(Default)]
    struct BlockedState {
        /// The handler is inside its blocking wait and can no longer be cancelled.
        inside: bool,
        /// The test has let it go.
        released: bool,
    }

    impl BlockedWorker {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                state: Mutex::new(BlockedState::default()),
                changed: std::sync::Condvar::new(),
                asked_relay: Arc::new(tokio::sync::Notify::new()),
                gate: std::sync::OnceLock::new(),
                saw_refusal: std::sync::atomic::AtomicBool::new(false),
                started: std::sync::atomic::AtomicBool::new(false),
            })
        }

        /// Gives the handler the gate it reports against. The harness owns the gate, and the
        /// worker has to exist before the harness does.
        fn watch(&self, gate: RefusalGate) {
            self.gate.set(gate).ok().expect("the gate is set once");
        }

        fn asked_relay(&self) -> Arc<tokio::sync::Notify> {
            self.asked_relay.clone()
        }

        /// Waits until the handler is inside its blocking wait.
        ///
        /// On a blocking thread, because that is what waiting on a `Condvar` is; the point of
        /// the whole instrument is that this handler does not participate in async
        /// cancellation.
        async fn wait_until_inside(self: &Arc<Self>) {
            let worker = Arc::clone(self);
            tokio::task::spawn_blocking(move || {
                let mut state = worker.state.lock().unwrap();
                while !state.inside {
                    state = worker.changed.wait(state).unwrap();
                }
            })
            .await
            .unwrap();
        }

        /// Lets the handler out. Whatever the controller has done by now, it is about to
        /// start executing.
        fn release(&self) {
            let mut state = self.state.lock().unwrap();
            state.released = true;
            self.changed.notify_all();
        }

        fn saw_refusal(&self) -> bool {
            self.saw_refusal.load(Ordering::SeqCst)
        }

        fn started(&self) -> bool {
            self.started.load(Ordering::SeqCst)
        }
    }

    /// Lets a [`BlockedWorker`] out however its test ends.
    ///
    /// A handler blocked inside its own poll owns a runtime worker thread until it is released,
    /// and a `tokio` runtime cannot finish shutting down while one of its threads is inside a
    /// task. Without this, an assertion that fired *before* the test reached its own release
    /// would hang the suite instead of reporting — which is the one thing a regression must
    /// never do, and is exactly what the round-13 cancellation test does on unfixed code.
    struct ReleasedOnDrop(Arc<BlockedWorker>);

    impl Drop for ReleasedOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    /// How long a test waits to establish that a refusal *cannot* be published.
    ///
    /// A negative is the one thing no handshake can observe, so this is the only deadline in
    /// the suite that the *fixed* code is expected to reach — and reaching it is what then lets
    /// the blocked worker out so the assertions can run. It cannot turn a violation into a
    /// pass in any realistic case: the unfixed fan-out releases the admission the instant a
    /// sibling's error lands, so the publication it permits succeeds over loopback in
    /// microseconds, and the assertions are about a recorded order rather than about a time.
    const SETTLEMENT_GRACE: Duration = Duration::from_secs(3);

    /// Records that a paused handler was dropped rather than resumed.
    ///
    /// `tonic` drops a request handler when its stream is reset or its connection closes, which
    /// is what the controller dropping an in-flight request does to a worker that suspends
    /// cooperatively. Round 11 asked for this to happen; since round 13 nothing may cut a
    /// request off before it has answered, so this guard firing is a regression report rather
    /// than the expected outcome.
    struct CutOffOnDrop(Option<Arc<PausedWorker>>);

    impl CutOffOnDrop {
        fn disarm(mut self) {
            self.0 = None;
        }
    }

    impl Drop for CutOffOnDrop {
        fn drop(&mut self) {
            if let Some(worker) = self.0.take() {
                worker.settle(PausedOutcome::CutOff);
            }
        }
    }

    /// A worker, as far as the controller can tell.
    ///
    /// A real server on a real socket, because the claim under test is about real RPCs: the
    /// controller connects to whatever address a `WorkerConnect` carries and sends
    /// `StartExecution` and `Commit` over that channel, so "no execution RPC and no commit was
    /// issued" is a question about what arrived here and nowhere else.
    struct FakeWorker {
        calls: Arc<WorkerCalls>,
        barriers: Arc<SchedulingBarriers>,
        starts_execution: StartsExecution,
    }

    #[tonic::async_trait]
    impl WorkerGrpc for FakeWorker {
        async fn start_execution(
            &self,
            request: tonic::Request<StartExecutionReq>,
        ) -> Result<tonic::Response<StartExecutionResp>, tonic::Status> {
            let request = request.into_inner();
            let selector = request.state_backend.clone();
            match &self.starts_execution {
                StartsExecution::Accepting => {}
                StartsExecution::FailingOnce(after) => {
                    after.notified().await;
                    return Err(tonic::Status::internal(
                        "this worker cannot start executing",
                    ));
                }
                StartsExecution::Blocking(blocked) => {
                    // No `.await` from here to the return. The thread is blocked inside this
                    // poll, so `tonic` cannot drop this future however the client's stream
                    // ends — the legacy worker shape that `try_lock` removed from production.
                    let mut state = blocked.state.lock().unwrap();
                    state.inside = true;
                    blocked.changed.notify_all();
                    while !state.released {
                        state = blocked.changed.wait(state).unwrap();
                    }
                    drop(state);

                    // What the controller had already decided by the time this worker started.
                    let refused = blocked
                        .gate
                        .get()
                        .expect("the blocked worker is given the job's gate")
                        .current
                        .read()
                        .unwrap()
                        .is_some();
                    blocked.saw_refusal.store(refused, Ordering::SeqCst);
                    blocked.started.store(true, Ordering::SeqCst);
                    self.calls.start_execution.lock().unwrap().push(selector);
                    self.barriers.execution_started.notify_one();
                    return Ok(tonic::Response::new(StartExecutionResp {}));
                }
                StartsExecution::Pausing(paused) => {
                    paused.announce();
                    let cut_off = CutOffOnDrop(Some(paused.clone()));
                    paused.released.notified().await;
                    // Only reached if the handler was not dropped while it waited.
                    cut_off.disarm();
                    self.calls.start_execution.lock().unwrap().push(selector);
                    paused.settle(PausedOutcome::Applied);
                    self.barriers.execution_started.notify_one();
                    return Ok(tonic::Response::new(StartExecutionResp {}));
                }
                StartsExecution::AmbiguousOnce(ambiguous) => {
                    ambiguous.remember_or_check(&request.start_execution_id);
                    if ambiguous.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        // This is the server work the client-side deadline cannot revoke.
                        let ambiguous = ambiguous.clone();
                        let calls = self.calls.clone();
                        let barriers = self.barriers.clone();
                        tokio::spawn(async move {
                            ambiguous.rendezvous.wait().await;
                            if ambiguous.apply_once() {
                                calls.start_execution.lock().unwrap().push(selector);
                                barriers.execution_started.notify_one();
                            }
                        });
                        return Err(tonic::Status::deadline_exceeded(
                            "the client stopped waiting while the handler remained live",
                        ));
                    }

                    ambiguous.retry_entered.notify_one();
                    ambiguous.rendezvous.wait().await;
                    if ambiguous.apply_once() {
                        self.calls.start_execution.lock().unwrap().push(selector);
                        self.barriers.execution_started.notify_one();
                    }
                    return Ok(tonic::Response::new(StartExecutionResp {}));
                }
                StartsExecution::BusyOnce(busy) => {
                    busy.remember_or_check(&request.start_execution_id);
                    if busy.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(tonic::Status::aborted(
                            "the worker phase lock is busy; nothing was applied",
                        ));
                    }
                }
                StartsExecution::NeverSettling(never) => {
                    never.remember_or_check(&request.start_execution_id);
                    never.calls.fetch_add(1, Ordering::SeqCst);
                    // Nothing is ever recorded in `calls.start_execution`: this worker does
                    // not apply, it only fails to settle.
                    return Err(tonic::Status::unavailable(
                        "this worker can never settle the request",
                    ));
                }
            }
            self.calls.start_execution.lock().unwrap().push(selector);
            self.barriers.execution_started.notify_one();
            Ok(tonic::Response::new(StartExecutionResp {}))
        }

        async fn commit(
            &self,
            request: tonic::Request<CommitReq>,
        ) -> Result<tonic::Response<CommitResp>, tonic::Status> {
            self.calls
                .commit
                .lock()
                .unwrap()
                .push(request.into_inner().epoch);
            Ok(tonic::Response::new(CommitResp {}))
        }

        async fn get_worker_phase(
            &self,
            _: tonic::Request<GetWorkerPhaseReq>,
        ) -> Result<tonic::Response<GetWorkerPhaseResp>, tonic::Status> {
            Ok(tonic::Response::new(GetWorkerPhaseResp::default()))
        }

        async fn checkpoint(
            &self,
            _: tonic::Request<CheckpointReq>,
        ) -> Result<tonic::Response<CheckpointResp>, tonic::Status> {
            Ok(tonic::Response::new(CheckpointResp {}))
        }

        async fn load_compacted_data(
            &self,
            _: tonic::Request<LoadCompactedDataReq>,
        ) -> Result<tonic::Response<LoadCompactedDataRes>, tonic::Status> {
            Ok(tonic::Response::new(LoadCompactedDataRes {}))
        }

        async fn stop_execution(
            &self,
            request: tonic::Request<StopExecutionReq>,
        ) -> Result<tonic::Response<StopExecutionResp>, tonic::Status> {
            self.calls
                .stop_execution
                .lock()
                .unwrap()
                .push(request.into_inner().stop_mode);
            Ok(tonic::Response::new(StopExecutionResp {}))
        }

        async fn job_finished(
            &self,
            _: tonic::Request<JobFinishedReq>,
        ) -> Result<tonic::Response<JobFinishedResp>, tonic::Status> {
            Ok(tonic::Response::new(JobFinishedResp {}))
        }

        async fn get_metrics(
            &self,
            _: tonic::Request<MetricsReq>,
        ) -> Result<tonic::Response<MetricsResp>, tonic::Status> {
            Ok(tonic::Response::new(MetricsResp::default()))
        }

        async fn job_controller_init(
            &self,
            _: tonic::Request<JobControllerInitReq>,
        ) -> Result<tonic::Response<JobControllerInitResp>, tonic::Status> {
            Ok(tonic::Response::new(JobControllerInitResp {}))
        }
    }

    /// Serves a [`FakeWorker`] on a loopback port and returns the address a `WorkerConnect`
    /// would carry.
    async fn fake_worker(
        calls: Arc<WorkerCalls>,
        barriers: Arc<SchedulingBarriers>,
        starts_execution: StartsExecution,
    ) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(WorkerGrpcServer::new(FakeWorker {
                    calls,
                    barriers,
                    starts_execution,
                }))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        format!("http://{addr}")
    }

    /// The `WorkerConnect` a fake worker would send, at the generation the preamble has just
    /// advanced to, carrying one slot.
    ///
    /// One slot per worker throughout, so "how many workers" and "how many slots the program
    /// needs" are the same number and a test states it once.
    ///
    /// Advertises the `StartExecution` reconciliation contract, which is what a worker of this
    /// version does. [`legacy_worker_connect_from`] is the other generation.
    fn worker_connect_from(worker_id: WorkerId, rpc_address: &str) -> JobMessage {
        worker_connect_advertising(worker_id, rpc_address, true)
    }

    /// The `WorkerConnect` a worker predating the reconciliation contract would send.
    ///
    /// `reconciles_start_execution` is a proto3 `bool`, so a registration from such a worker
    /// decodes with it `false` — this is that registration, not a synthetic marker.
    fn legacy_worker_connect_from(worker_id: WorkerId, rpc_address: &str) -> JobMessage {
        worker_connect_advertising(worker_id, rpc_address, false)
    }

    fn worker_connect_advertising(
        worker_id: WorkerId,
        rpc_address: &str,
        reconciles_start_execution: bool,
    ) -> JobMessage {
        JobMessage::WorkerConnect {
            worker_id,
            machine_id: MachineId(Arc::new(format!("machine_{}", worker_id.0))),
            generation: 2,
            rpc_address: rpc_address.to_string(),
            data_address: "127.0.0.1:1".to_string(),
            slots: 1,
            reconciles_start_execution,
        }
    }

    /// The single-worker case, which is what the round-9 tests use.
    fn worker_connect(rpc_address: &str) -> JobMessage {
        worker_connect_from(WorkerId(7), rpc_address)
    }

    /// The `TaskStarted` that satisfies the second loop for a single-worker run.
    fn task_started() -> JobMessage {
        JobMessage::TaskStarted {
            worker_id: WorkerId(7),
            task_id: 1,
            subtask_idx: 0,
        }
    }

    /// One operator at the given parallelism: that many slots to fill and that many tasks to
    /// start, over one operator for the restored checkpoint to cover.
    fn one_operator_program_at(parallelism: usize) -> LogicalProgram {
        let mut program = LogicalProgram::default();
        program.graph.add_node(LogicalNode::single(
            1,
            OPERATOR_ID.to_string(),
            OperatorName::ArrowValue,
            vec![],
            "the only operator".to_string(),
            parallelism,
        ));
        program
    }

    const OPERATOR_ID: &str = "node_1";
    /// The epoch of the restored checkpoint, and the min epoch it is rewritten to. They differ
    /// so the commit the job replays can be told apart from anything else.
    const RESTORED_EPOCH: u32 = 4;
    const RESTORED_MIN_EPOCH: u32 = 2;

    /// A directory holding one job's checkpoints, removed with the test.
    struct CheckpointDir(String);

    impl CheckpointDir {
        fn new(name: &str) -> Self {
            let directory = std::env::temp_dir()
                .join(format!(
                    "arroyo-states-{name}-{}-{:?}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                ))
                .to_string_lossy()
                .into_owned();
            std::fs::create_dir_all(&directory).unwrap();
            Self(directory)
        }

        fn url(&self) -> String {
            format!("file://{}", self.0)
        }

        fn role(&self) -> StorageProviderFor {
            StorageProviderFor::Controller {
                storage_url: Some(self.url()),
            }
        }
    }

    impl Drop for CheckpointDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Lays down a checkpoint that died in its committing phase, so restoring from it makes the
    /// job replay commits — the effect the third crossing guards.
    async fn committing_checkpoint(dir: &CheckpointDir, db: &DatabaseSource) {
        StateBackend::write_operator_checkpoint_metadata(
            &dir.role(),
            OperatorCheckpointMetadata {
                operator_metadata: Some(OperatorMetadata {
                    job_id: "job_abc".to_string(),
                    operator_id: OPERATOR_ID.to_string(),
                    epoch: RESTORED_EPOCH,
                    min_watermark: None,
                    max_watermark: None,
                    parallelism: 1,
                }),
                start_time: 0,
                finish_time: 0,
                table_checkpoint_metadata: HashMap::from([(
                    "g".to_string(),
                    TableCheckpointMetadata {
                        table_type: TableEnum::GlobalKeyValue as i32,
                        data: GlobalKeyedTableTaskCheckpointMetadata {
                            files: vec![],
                            commit_data_by_subtask: HashMap::from([(0, b"commit".to_vec())]),
                        }
                        .encode_to_vec(),
                    },
                )]),
                table_configs: HashMap::from([(
                    "g".to_string(),
                    TableConfig {
                        table_type: TableEnum::GlobalKeyValue as i32,
                        config: GlobalKeyedTableConfig {
                            table_name: "g".to_string(),
                            description: "global".to_string(),
                            uses_two_phase_commit: true,
                        }
                        .encode_to_vec(),
                        state_version: 0,
                        state_backend: "parquet".to_string(),
                    },
                )]),
            },
        )
        .await
        .unwrap();

        // The write takes a token, so the fixture stands behind its operator the same way the
        // worker that took the checkpoint does — with what that operator's subtasks reported.
        let completed = Validated::validate(
            CompletedCheckpoint::new(
                "job_abc".to_string(),
                RESTORED_EPOCH,
                vec![CompletedOperator::reported(OPERATOR_ID.to_string(), 1, [0])],
            ),
            &std::collections::HashSet::from([OPERATOR_ID]),
        )
        .unwrap();
        StateBackend::write_checkpoint_metadata(
            &dir.role(),
            Validated::validate(
                CheckpointMetadataWrite::for_completed_checkpoint(
                    CheckpointMetadata {
                        job_id: "job_abc".to_string(),
                        epoch: RESTORED_EPOCH,
                        min_epoch: 0,
                        start_time: 0,
                        finish_time: 0,
                        operator_ids: vec![OPERATOR_ID.to_string()],
                    },
                    &completed,
                ),
                (),
            )
            .unwrap(),
        )
        .await
        .unwrap();

        let DatabaseSource::Sqlite(connection) = db else {
            unreachable!("the fixture is always sqlite")
        };
        connection
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO checkpoints (pub_id, job_id, epoch, min_epoch, state, state_backend)
                 VALUES ('cp_1', 'job_abc', ?1, ?2, 'committing', 'parquet')",
                cornucopia_async::rusqlite::params![RESTORED_EPOCH, RESTORED_MIN_EPOCH],
            )
            .unwrap();
    }

    /// Everything a run of the real `Scheduling::next` against real workers needs.
    struct SchedulingRun {
        db: DatabaseSource,
        calls: Arc<WorkerCalls>,
        barriers: Arc<SchedulingBarriers>,
        worker_addresses: Vec<String>,
        harness: Harness,
        /// Held for the test: dropping it takes the checkpoint away.
        _checkpoints: CheckpointDir,
    }

    impl SchedulingRun {
        /// One worker that accepts, which is what the round-9 tests want.
        async fn new(name: &str) -> Self {
            Self::with_workers(name, vec![StartsExecution::Accepting]).await
        }

        /// One worker per behaviour, each with one slot, and a program that needs exactly as
        /// many slots as there are workers.
        async fn with_workers(name: &str, workers: Vec<StartsExecution>) -> Self {
            let db = sqlite_startable_job("Scheduling", 2);
            let checkpoints = CheckpointDir::new(name);
            committing_checkpoint(&checkpoints, &db).await;

            let calls = Arc::new(WorkerCalls::default());
            let barriers = Arc::new(SchedulingBarriers::default());
            let mut worker_addresses = Vec::new();
            for starts_execution in &workers {
                worker_addresses.push(
                    fake_worker(calls.clone(), barriers.clone(), starts_execution.clone()).await,
                );
            }

            let mut harness = Harness::new(3)
                .with_db(db.clone())
                .with_program(one_operator_program_at(workers.len()))
                .with_state_url(checkpoints.url())
                .with_scheduler(RecordingScheduler::watching(barriers.clone()));
            harness.status.state = "Scheduling".to_string();

            Self {
                db,
                calls,
                barriers,
                worker_addresses,
                harness,
                _checkpoints: checkpoints,
            }
        }

        /// The address of the nth worker, in the order its behaviour was given.
        fn address(&self, n: usize) -> String {
            self.worker_addresses[n].clone()
        }

        /// Runs the real `Scheduling::next` against the real worker.
        async fn schedule(&mut self) -> Result<Transition, StateError> {
            let mut ctx = self.harness.ctx(
                running_config(StateBackendSelector::Parquet),
                StateBackendSelector::Parquet,
            );
            Box::new(Scheduling {}).next(&mut ctx).await
        }

        /// The same run, entered exactly as [`execute_state`] enters it, for a job whose
        /// lifecycle mechanism was derived the way production derives one.
        ///
        /// The assertion inside is the point of the fixture rather than a precondition of it:
        /// `run_state_body`'s only question is `runs_fenced_lifecycle()`, and this is where a
        /// harness that has been given production's own `JobLifecycle` answers it.
        async fn schedule_through_the_production_route(
            &mut self,
        ) -> Result<Transition, StateError> {
            let mut ctx = self.harness.ctx(
                running_config(StateBackendSelector::Parquet),
                StateBackendSelector::Parquet,
            );
            assert!(
                !ctx.runs_fenced_lifecycle(),
                "a job built from `LifecycleMode::SELECTED` has no D39a writer, so the seam \
                 must send it to the landed `Scheduling::next`"
            );
            super::scheduling::run_state_body(Box::new(Scheduling {}), &mut ctx).await
        }

        /// The same run, through the M11.D39b phase graph, entered exactly as
        /// [`execute_state`] enters it.
        ///
        /// The harness must have been given a lifecycle actor first: that is what makes the
        /// job's mechanism the D39a single writer, and it is the only thing that decides which
        /// body runs.
        async fn schedule_through_the_phase_graph(&mut self) -> Result<Transition, StateError> {
            let mut ctx = self.harness.ctx(
                running_config(StateBackendSelector::Parquet),
                StateBackendSelector::Parquet,
            );
            assert!(
                ctx.runs_fenced_lifecycle(),
                "this fixture is about the phase graph, and without an actor it would silently \
                 run the landed body instead"
            );
            super::scheduling::run_state_body(Box::new(Scheduling {}), &mut ctx).await
        }
    }

    /// The name of the state a transition advances to.
    fn advanced_to(outcome: &Result<Transition, StateError>) -> Option<&'static str> {
        match outcome {
            Ok(Transition::Advance(holder)) => Some(holder.state.name()),
            _ => None,
        }
    }

    /// A job's intent mailbox, for a fixture that drives the D39a path without a state machine.
    fn intent_mailbox() -> Arc<lifecycle::IntentMailbox> {
        Arc::new(lifecycle::IntentMailbox::new(Arc::new(
            "job_abc".to_string(),
        )))
    }

    /// Publishes `refusal` to `gate` at the first instant a publication is possible, exactly as
    /// a poll that found the admission taken would when it came round again.
    ///
    /// Spinning on `admit_publication` is not a timing hack: it is the only outcome
    /// `refuse_config` has while a region is in flight — it changes nothing and the next poll
    /// tries again — so the first success is by construction the first moment the region ended.
    async fn publish_when_admitted(gate: &RefusalGate, refusal: RefusedConfig) {
        loop {
            if let Some(admission) = gate.admit_publication() {
                gate.publish(&admission, refusal);
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    /// A refusal that reaches the gate while the job waits for its workers, with the connect
    /// that ends that wait queued *ahead* of it, must stop the `StartExecution` RPCs.
    ///
    /// This is the reviewer's scenario for the first receive loop, and it is deterministic
    /// rather than raced. The refusal is published the instant the preamble's admission is
    /// released — the earliest a poll could have published it at all — and only then is the
    /// `WorkerConnect` queued, with the refusal message behind it. The loop breaks on the
    /// connect the moment the slots add up, so the refusal is still sitting unread in the job's
    /// queue when the crossing is reached; nothing about the outcome may depend on that.
    ///
    /// Before the fix the fan-out ran here and a worker was told to start executing a
    /// configuration the controller had already refused. It is a real gRPC call to a real
    /// server, so what is asserted is what the worker received.
    #[tokio::test]
    async fn a_refusal_queued_behind_the_worker_connects_stops_the_execution_rpcs() {
        let mut run = SchedulingRun::new("connects-refusal").await;
        let gate = run.harness.refusal_gate.clone();
        let queue = run.harness.queue();
        let barriers = run.barriers.clone();
        let address = run.address(0);

        let version = Arc::new(AtomicU64::new(1));
        let refusal = RefusedConfig::new(selector_changed(), 1, version);

        let poll = tokio::spawn(async move {
            // Strictly inside the preamble: the cluster has been asked for, so the preamble
            // still holds the admission and nothing can be published yet.
            barriers.workers_started.notified().await;
            publish_when_admitted(&gate, refusal.clone()).await;
            // Only now the messages, in the order that hides the refusal from the loop.
            queue.send(worker_connect(&address)).await.unwrap();
            queue
                .send(JobMessage::ConfigRefused(refusal))
                .await
                .unwrap();
        });

        let outcome = run.schedule().await;
        poll.await.unwrap();

        assert_eq!(
            run.calls.started(),
            Vec::<String>::new(),
            "no worker may be told to start executing a configuration the controller has \
             refused — the refusal was on the gate before this crossing, whatever position it \
             had reached in the job's queue"
        );
        assert_eq!(
            run.calls.committed(),
            Vec::<u64>::new(),
            "and so nothing is committed either: the commits come after the execution that \
             never started"
        );

        let Err(err) = outcome else {
            panic!("a refused job must not be scheduled into an execution");
        };
        assert!(
            matches!(
                selector_error(&err),
                StateBackendError::JobSelectorChanged { .. }
            ),
            "and must fail for the refusal itself, not for something the attempt hit: {err:?}"
        );
        assert_eq!(
            state_writes(&run.db),
            [("Scheduling".to_string(), 2)],
            "the preamble did run — it was admitted with the gate clear — so this test is \
             about the crossing after it and not about a job that never got started"
        );
    }

    /// The same, one phase later: a refusal that reaches the gate while the job waits for its
    /// tasks, with the `TaskStarted` that ends that wait queued ahead of it, must stop the
    /// recovered commits.
    ///
    /// The barrier here is the worker's own `StartExecution` handler, which fires from inside
    /// the second region — so the refusal is published strictly after that region's gate read
    /// (execution is allowed to start, and does) and strictly before the `TaskStarted` that
    /// makes the second loop exit. The refusal message is queued behind that `TaskStarted` and
    /// is never read.
    ///
    /// Before the fix the job then published the restored checkpoint's commits: a two-phase
    /// commit finished against the job's sinks, for a configuration the controller had refused,
    /// and not something a later failure can take back.
    #[tokio::test]
    async fn a_refusal_queued_behind_the_task_starts_stops_the_recovered_commits() {
        let mut run = SchedulingRun::new("task-starts-refusal").await;
        let gate = run.harness.refusal_gate.clone();
        let queue = run.harness.queue();
        let barriers = run.barriers.clone();

        // The connect is already in the queue, so the first loop is satisfied without the
        // poll thread doing anything: this test is about the second one.
        queue.send(worker_connect(&run.address(0))).await.unwrap();

        let version = Arc::new(AtomicU64::new(1));
        let refusal = RefusedConfig::new(selector_changed(), 1, version);

        let poll = tokio::spawn(async move {
            barriers.execution_started.notified().await;
            publish_when_admitted(&gate, refusal.clone()).await;
            queue.send(task_started()).await.unwrap();
            queue
                .send(JobMessage::ConfigRefused(refusal))
                .await
                .unwrap();
        });

        let outcome = run.schedule().await;
        poll.await.unwrap();

        assert_eq!(
            run.calls.started(),
            ["parquet".to_string()],
            "execution did start, because nothing was refused when that crossing read the \
             gate — which is what makes this test about the crossing after it"
        );
        assert_eq!(
            run.calls.committed(),
            Vec::<u64>::new(),
            "but the restored checkpoint's commits are externally visible and must not be \
             published for a refused configuration, however far behind the `TaskStarted` the \
             refusal was queued"
        );

        let Err(err) = outcome else {
            panic!("a refused job must not publish a checkpoint's commits");
        };
        assert!(
            matches!(
                selector_error(&err),
                StateBackendError::JobSelectorChanged { .. }
            ),
            "and must fail for the refusal itself: {err:?}"
        );
    }

    /// The control for both, and for every property an interlock at these crossings could
    /// break.
    ///
    /// An ordinary job must still schedule, start its workers, start executing, wait for its
    /// tasks, publish the restored checkpoint's commits and run. Two extra admissions on the
    /// path are two extra chances to stall a job that nothing is wrong with, and the gate is
    /// read twice more, so a false positive would fail a job that was never refused. Neither
    /// happens here, and the assertions above are about the refusal rather than about a harness
    /// in which nothing ever reaches a worker.
    #[tokio::test]
    async fn an_unrefused_job_starts_executing_publishes_its_commits_and_runs() {
        let mut run = SchedulingRun::new("unrefused-control").await;
        let queue = run.harness.queue();
        queue.send(worker_connect(&run.address(0))).await.unwrap();
        queue.send(task_started()).await.unwrap();

        let outcome = run.schedule().await;

        let Ok(Transition::Advance(next)) = outcome else {
            panic!("an unrefused job must schedule and advance to `Running`");
        };
        assert_eq!(
            next.state.name(),
            "Running",
            "and reach `Running`, which is the state the two tests above must never get to"
        );
        assert_eq!(
            run.calls.started(),
            ["parquet".to_string()],
            "the worker was told to start executing, under the job's own selector"
        );
        assert_eq!(
            run.calls.committed(),
            [RESTORED_EPOCH as u64],
            "and the restored checkpoint's commits were published: the effect the third \
             crossing guards, still happening when nothing is refused"
        );
        assert_eq!(
            run.harness.scheduler.started.lock().unwrap().as_slice(),
            [("job_abc".to_string(), 2)],
            "with the replacement cluster started at the new generation"
        );
        assert_eq!(
            state_writes(&run.db),
            [("Scheduling".to_string(), 2)],
            "and the generation persisted exactly once"
        );
    }

    // ---------------------------------------------------------------------------------------
    // Round 11: the fan-out's own lifetime.
    //
    // Holding the admission across the fan-out only says something if the fan-out is over when
    // the admission is released. It was not: every request was a `tokio::spawn`, and dropping a
    // `JoinHandle` detaches its task rather than cancelling it. So the first worker to fail
    // ended the helper and left its siblings running, and cancelling the job's state task left
    // all of them running — in both cases past the point where a refusal could be published,
    // and a detached request can still make a worker execute the configuration just refused.
    //
    // The two tests below are the two ways out that leaked: a sibling failing, and the parent
    // being dropped. Both hold one worker inside its own `StartExecution` handler across that
    // moment and ask what happened to it. A real server on a real socket is what makes the
    // question answerable at all.
    //
    // What they *ask for* changed in round 13. Round 11 asked that the request be cut off, and
    // a cut-off is only a client-side event: see the round 13 block below, and `BlockedWorker`.
    // These two now ask for the same thing that block does, from a worker that could have been
    // cancelled — so that the answer does not depend on the handler's shape.
    // ---------------------------------------------------------------------------------------

    /// A worker still inside its `StartExecution` must be *waited for* when a sibling request
    /// fails, so that no refusal can be published while its answer is still outstanding.
    ///
    /// Round 11 asserted the opposite here — that the request was cut off — and round 13
    /// replaced that claim rather than strengthening it. Cutting a request off is the most a
    /// client can do and it is not enough: a stream reset stops nobody, it only stops anyone
    /// listening. The scenario is unchanged (two workers, one that fails and one held inside its
    /// handler when the failure lands, the failure held back until the second is provably
    /// inside); what is asked of the controller is now settlement instead of cancellation.
    ///
    /// This worker suspends cooperatively, so the fan-out *could* still take its request away.
    /// The point of keeping it alongside
    /// `a_sibling_failure_cannot_release_the_admission_while_a_blocked_worker_is_unsettled` is
    /// that both now get the same answer: the guarantee no longer depends on which kind of
    /// handler the worker happens to have.
    #[tokio::test]
    async fn a_sibling_failure_waits_for_the_request_it_used_to_cut_off() {
        let paused = PausedWorker::new();
        let mut run = SchedulingRun::with_workers(
            "sibling-failure",
            vec![
                StartsExecution::FailingOnce(paused.asked_relay()),
                StartsExecution::Pausing(paused.clone()),
            ],
        )
        .await;

        let gate = run.harness.refusal_gate.clone();
        let queue = run.harness.queue();
        for (n, worker_id) in [WorkerId(7), WorkerId(8)].into_iter().enumerate() {
            queue
                .send(worker_connect_from(worker_id, &run.address(n)))
                .await
                .unwrap();
        }

        let version = Arc::new(AtomicU64::new(1));
        let refusal = RefusedConfig::new(selector_changed(), 1, version);

        let watching = paused.clone();
        let poll = tokio::spawn(async move {
            // Not before the fan-out has begun. A publication that succeeded during the
            // receive phase ahead of it would stop the fan-out at its own gate read — round
            // 9's scenario, covered by round 9's tests — and this test is about what is left
            // running once the fan-out has already been entered and has failed.
            watching.asked.notified().await;

            // Publishing is impossible for as long as a region is in flight, so this asks
            // directly whether the fan-out kept its admission across a sibling's failure. See
            // `SETTLEMENT_GRACE` for why establishing that it did needs a deadline.
            let published_while_unsettled =
                tokio::time::timeout(SETTLEMENT_GRACE, publish_when_admitted(&gate, refusal))
                    .await
                    .is_ok();

            // However that went, let the worker out so the assertions can run.
            watching.released.notify_one();
            published_while_unsettled
        });

        let outcome = run.schedule().await;
        let published_while_unsettled = poll.await.unwrap();

        assert!(
            !published_while_unsettled,
            "a refusal was published while a `StartExecution` this job had issued was still \
             unsettled: the fan-out gave its admission up on a sibling's failure, and a request \
             it has stopped waiting for is not a request the worker has stopped serving"
        );
        assert_eq!(
            paused.outcome().await,
            PausedOutcome::Applied,
            "and the request must have been waited for rather than taken back: round 11 cut \
             this one off, which is all a client can do to a handler and says nothing about \
             what the handler did"
        );
        assert_eq!(
            run.calls.started(),
            ["parquet".to_string()],
            "so the surviving worker did start executing — under a configuration that was \
             unrefused when it was admitted, which is the order this test is about"
        );
        assert_eq!(
            run.calls.committed(),
            Vec::<u64>::new(),
            "and nothing is committed: the commits come after a fan-out that succeeded"
        );

        let Err(err) = outcome else {
            panic!("a fan-out in which a worker refused the request cannot have succeeded");
        };
        assert!(
            format!("{err:?}").contains("failed to initialize workers"),
            "and draining the siblings must still report the worker that refused: {err:?}"
        );
    }

    /// Cancelling the job's state task must not release its admission while an outstanding
    /// `StartExecution` is unanswered, because nothing else can speak for that request now.
    ///
    /// The other ordering, and the one no error path reaches: the state task is simply dropped —
    /// `ShutdownGuard::into_spawn_task` drops `run_to_completion` when the shutdown token fires
    /// — while the fan-out is in flight. Round 10 left the requests detached and running past
    /// the admission; round 11 made them die with the task, which is the right thing to do with
    /// a client future and still no answer at all about the handler behind it.
    ///
    /// So round 13 sends the admission with them instead: the region owns it, and a region
    /// dropped with work outstanding is finished in a task that carries the admission inside it.
    /// A publication is therefore impossible the instant after the state task is gone, and
    /// becomes possible only once the worker has answered.
    #[tokio::test]
    async fn a_cancelled_fan_out_holds_its_admission_until_the_request_it_issued_settles() {
        let paused = PausedWorker::new();
        let mut run = SchedulingRun::with_workers(
            "cancelled-fan-out",
            vec![StartsExecution::Pausing(paused.clone())],
        )
        .await;

        let gate = run.harness.refusal_gate.clone();
        let queue = run.harness.queue();
        queue
            .send(worker_connect_from(WorkerId(7), &run.address(0)))
            .await
            .unwrap();

        {
            let mut scheduling = std::pin::pin!(run.schedule());
            tokio::select! {
                _ = &mut scheduling => {
                    panic!("the state cannot have finished: a worker is still holding its \
                            `StartExecution` open")
                }
                _ = paused.asked.notified() => {}
            }
            // The job's state task, cancelled with a request in flight.
        }

        // Deterministic, and the assertion round 11 had backwards: the admission is inside the
        // rescued region, so it is still held the instant the task that opened it is gone.
        assert!(
            gate.admit_publication().is_none(),
            "the admission must have outlived the cancelled state task, because the request it \
             authorised has: a refusal published now would be published behind a worker that is \
             still deciding what to do with its `StartExecution`"
        );

        paused.released.notify_one();
        assert_eq!(
            paused.outcome().await,
            PausedOutcome::Applied,
            "the rescued region must carry its request through to an answer, not merely hold a \
             lock for a while"
        );

        tokio::time::timeout(
            SETTLEMENT_GRACE,
            publish_when_admitted(
                &gate,
                RefusedConfig::new(selector_changed(), 1, Arc::new(AtomicU64::new(1))),
            ),
        )
        .await
        .expect(
            "and it must be released once that answer is in — holding it any longer would wedge \
             the job's next scheduling attempt on a task nobody is waiting for",
        );

        assert_eq!(
            run.calls.started(),
            ["parquet".to_string()],
            "the worker did start executing: it was asked under an admitted, unrefused \
             configuration, and the guarantee is that no refusal was published while that was \
             still in doubt"
        );
    }

    // ---------------------------------------------------------------------------------------
    // Round 13: settlement, not cancellation.
    //
    // Round 11 stopped the fan-out from *detaching* its requests, and the tests above prove it
    // for a handler that yields. At the time, production `WorkerGrpc::start_execution` blocked
    // on a `std::sync::Mutex` inside one poll and then started without an `.await`, so a reset
    // only meant nobody was listening. Production now uses `try_lock`, but these tests retain
    // the hostile handler as defense in depth and for mixed/legacy peers.
    //
    // What replaces it is a claim about what the controller knows rather than about what it can
    // stop: **no refusal is published while any `StartExecution` this job issued is unsettled**.
    // The two orderings that broke it are a sibling's failure and the state task being dropped,
    // and the two tests below are those, with a handler that cannot be cancelled at all.
    //
    // Note what is deliberately *not* asserted: that the worker does not start. It does — it
    // accepted a configuration that was unrefused when it was admitted, and revoking that would
    // need the worker to acknowledge an admission back. What is asserted is that it started
    // before any refusal existed, which is the whole of what the controller can enforce from
    // this side.
    // ---------------------------------------------------------------------------------------

    /// A sibling request failing must not release the admission while a worker that cannot be
    /// cancelled is still inside its `StartExecution`.
    ///
    /// The reviewer's counterexample, made deterministic. Two workers: one fails, and one is
    /// blocked inside its handler in a way no stream reset can reach. The failure is held back
    /// until the blocked handler is provably inside, so "a request was outstanding" is enforced
    /// rather than hoped for; a refusal is then published at the first instant a publication is
    /// possible.
    ///
    /// Before the fix the fan-out left on the first error, dropped the sibling's client future,
    /// and released the admission. The publication then succeeded — and the worker, which had
    /// never stopped, went on to start executing the configuration that had just been refused.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_sibling_failure_cannot_release_the_admission_while_a_blocked_worker_is_unsettled() {
        let blocked = BlockedWorker::new();
        // Declared first, so it is dropped last: whatever fails below, the worker is let go
        // before the runtime is torn down. See `ReleasedOnDrop`.
        let _released = ReleasedOnDrop(blocked.clone());
        let mut run = SchedulingRun::with_workers(
            "blocked-sibling-failure",
            vec![
                StartsExecution::FailingOnce(blocked.asked_relay()),
                StartsExecution::Blocking(blocked.clone()),
            ],
        )
        .await;
        blocked.watch(run.harness.refusal_gate.clone());

        let gate = run.harness.refusal_gate.clone();
        let queue = run.harness.queue();
        for (n, worker_id) in [WorkerId(7), WorkerId(8)].into_iter().enumerate() {
            queue
                .send(worker_connect_from(worker_id, &run.address(n)))
                .await
                .unwrap();
        }

        let refusal = RefusedConfig::new(selector_changed(), 1, Arc::new(AtomicU64::new(1)));
        let watching = blocked.clone();
        let relay = blocked.asked_relay();
        let probe = tokio::spawn(async move {
            // Only once the blocked handler is past the point of no return: from here on the
            // controller cannot stop it, whatever it does with the request.
            watching.wait_until_inside().await;
            relay.notify_one();

            // A publication succeeds at the first instant a region is not in flight, so this
            // asks directly whether the admission survived the sibling's failure. See
            // `SETTLEMENT_GRACE` for why establishing a negative needs a deadline.
            let published_while_unsettled =
                tokio::time::timeout(SETTLEMENT_GRACE, publish_when_admitted(&gate, refusal))
                    .await
                    .is_ok();

            // However that went, let the worker out so the assertions can run.
            watching.release();
            published_while_unsettled
        });

        let outcome = run.schedule().await;
        let published_while_unsettled = probe.await.unwrap();

        assert!(
            !published_while_unsettled,
            "a refusal was published while a `StartExecution` this job had issued was still \
             unsettled: the fan-out gave the admission up on a sibling's failure, and the \
             request it stopped waiting for was one no cancellation could reach"
        );
        assert!(
            blocked.started(),
            "the blocked handler must have run to completion — past the point at which the \
             unfixed fan-out had already given up on it — or this test proves nothing"
        );
        assert!(
            !blocked.saw_refusal(),
            "and it must have started the worker before any refusal existed: a worker that \
             cannot be cancelled must have its answer in hand before a refusal can be published"
        );
        assert_eq!(
            run.calls.started(),
            ["parquet".to_string()],
            "the blocked worker did start executing, which is the point: it accepted a \
             configuration that was unrefused when it was admitted, and the guarantee is about \
             the order of that against the refusal, not about revoking it"
        );

        let Err(err) = outcome else {
            panic!("a fan-out in which a worker refused the request cannot have succeeded");
        };
        assert!(
            format!("{err:?}").contains("failed to initialize workers"),
            "and draining the siblings must still report the worker that refused: {err:?}"
        );
    }

    /// Cancelling the job's state task must not release the admission while a worker that
    /// cannot be cancelled is still inside its `StartExecution`.
    ///
    /// The other ordering, and the one no error path reaches: the state task is dropped whole —
    /// `ShutdownGuard::into_spawn_task` drops `run_to_completion` when the shutdown token fires
    /// — while the fan-out is in flight. Round 11 made the requests die with it, which is the
    /// right thing to do with a client future and no answer at all about the handler behind it.
    ///
    /// So the admission goes with the requests instead of before them: the region owns it, and
    /// a region dropped with work outstanding is finished in a task that carries the admission
    /// inside it. This asserts that directly — a publication is impossible the instant after
    /// the task is gone, and becomes possible only once the worker has been let out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropping_the_scheduling_task_keeps_the_admission_until_its_blocked_request_settles() {
        let blocked = BlockedWorker::new();
        // Declared first, so it is dropped last. The assertion below fires on unfixed code
        // while the worker is still blocked, and without this the suite would hang there
        // rather than report. See `ReleasedOnDrop`.
        let _released = ReleasedOnDrop(blocked.clone());
        let mut run = SchedulingRun::with_workers(
            "blocked-cancelled-fan-out",
            vec![StartsExecution::Blocking(blocked.clone())],
        )
        .await;
        blocked.watch(run.harness.refusal_gate.clone());

        let gate = run.harness.refusal_gate.clone();
        let queue = run.harness.queue();
        queue
            .send(worker_connect_from(WorkerId(7), &run.address(0)))
            .await
            .unwrap();

        {
            let mut scheduling = std::pin::pin!(run.schedule());
            let mut inside = std::pin::pin!(blocked.wait_until_inside());
            tokio::select! {
                _ = &mut scheduling => {
                    panic!("the state cannot have finished: the worker is still blocked inside \
                            its `StartExecution`")
                }
                _ = &mut inside => {}
            }
            // The job's state task, cancelled with a request in flight that nothing can recall.
        }

        assert!(
            gate.admit_publication().is_none(),
            "the admission must have outlived the cancelled state task, because the request it \
             authorised has: the worker is inside a handler that no stream reset can drop, and \
             a refusal published now would be published behind it"
        );

        blocked.release();

        tokio::time::timeout(
            SETTLEMENT_GRACE,
            publish_when_admitted(
                &gate,
                RefusedConfig::new(selector_changed(), 1, Arc::new(AtomicU64::new(1))),
            ),
        )
        .await
        .expect(
            "and it must be released once the request settles — holding it past that would \
             wedge the job's next scheduling attempt on a task nobody is waiting for",
        );

        assert!(
            blocked.started(),
            "the blocked handler must have run to completion, or this test proves nothing"
        );
        assert!(
            !blocked.saw_refusal(),
            "and it started the worker before any refusal existed"
        );
        assert_eq!(
            run.calls.started(),
            ["parquet".to_string()],
            "the worker did start: a request the controller could not recall was answered \
             before the refusal, which is the order the invariant is about"
        );
    }

    /// A client-side deadline is not a worker answer. The first handler here escapes the
    /// failed RPC and remains able to apply the request, as a legacy handler blocked inside
    /// `phase.lock()` could. The controller must retain the admission, retry the same execution
    /// ID, and receive an idempotent acknowledgement before a refusal can be published.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_transport_error_is_reconciled_before_the_start_admission_is_released() {
        let ambiguous = AmbiguousStart::new();
        let mut run = SchedulingRun::with_workers(
            "ambiguous-start-execution",
            vec![StartsExecution::AmbiguousOnce(ambiguous.clone())],
        )
        .await;

        let gate = run.harness.refusal_gate.clone();
        let queue = run.harness.queue();
        let execution_started = run.barriers.clone();
        let calls = run.calls.clone();
        queue
            .send(worker_connect_from(WorkerId(7), &run.address(0)))
            .await
            .unwrap();

        let mut scheduling = std::pin::pin!(run.schedule());
        tokio::select! {
            _ = &mut scheduling => {
                panic!("an ambiguous transport result must be retried under admission")
            }
            retry = tokio::time::timeout(
                SETTLEMENT_GRACE,
                ambiguous.retry_entered.notified(),
            ) => {
                retry.expect("the ambiguous result must cause an idempotent retry");
            }
        }

        assert!(
            gate.admit_publication().is_none(),
            "the admission must remain held after the client deadline while the original \
             server work can still apply the request"
        );

        // Release the escaped handler and its idempotent retry together. Exactly one applies;
        // the other acknowledges the same stable execution ID.
        ambiguous.rendezvous.wait().await;
        execution_started.execution_started.notified().await;
        queue.send(task_started()).await.unwrap();

        let outcome = scheduling.await;
        let Ok(Transition::Advance(next)) = outcome else {
            panic!("the reconciled execution must continue scheduling");
        };
        assert_eq!(next.state.name(), "Running");
        assert_eq!(
            ambiguous.calls.load(Ordering::SeqCst),
            2,
            "the ambiguous call must be retried"
        );
        assert_eq!(
            calls.started(),
            ["parquet".to_string()],
            "the stable execution ID makes the retry idempotent"
        );
    }

    /// A busy worker phase is a definitive non-application, but it is transient. The worker
    /// returns `Aborted` without parking a handler inside its mutex, and the controller retries
    /// the same execution ID while the admission remains held.
    #[tokio::test]
    async fn a_busy_worker_phase_is_retried_under_the_same_start_admission() {
        let busy = BusyStart::new();
        let mut run = SchedulingRun::with_workers(
            "busy-start-execution",
            vec![StartsExecution::BusyOnce(busy.clone())],
        )
        .await;
        let queue = run.harness.queue();
        queue
            .send(worker_connect_from(WorkerId(7), &run.address(0)))
            .await
            .unwrap();
        queue.send(task_started()).await.unwrap();

        let outcome = run.schedule().await;
        let Ok(Transition::Advance(next)) = outcome else {
            panic!("a transient busy phase must be retried rather than fail scheduling");
        };
        assert_eq!(next.state.name(), "Running");
        assert_eq!(
            busy.calls.load(Ordering::SeqCst),
            2,
            "the Aborted response must be retried exactly once in this fixture"
        );
        assert_eq!(
            run.calls.started(),
            ["parquet".to_string()],
            "only the accepted retry starts the worker"
        );
    }

    // ---------------------------------------------------------------------------------------
    // Round 15: a peer that never answers, and a peer that cannot be reasoned with.
    //
    // Rounds 13 and 14 made the admission survive every way the *controller* could let go of a
    // request early. What neither addressed is a request that never settles at all: the retry
    // loop had no terminal path, so a worker that died or stayed partitioned during
    // `StartExecution` held the admission — and with it the job's ability to reschedule or to
    // accept a refusal — for the life of the controller process.
    //
    // That had a deterministic mixed-version form. A worker predating the reconciliation
    // contract answers its `Initializing` phase with `Unavailable`, which this loop reads as
    // "transport, retry"; every authoritative answer such a worker can give was therefore
    // classified as ambiguous and retried forever. The same worker takes its phase lock
    // *blocking*, so one of its handlers can still be inside `poll` when the controller that
    // issued it exits, where no refusal a replacement controller publishes can reach it.
    //
    // Both are answered by the same two-part fix, and each part is load-bearing for the other:
    //
    //   * `Scheduling::next` will not fan out to a worker that has not advertised the contract,
    //     so every peer the loop can be talking to is one whose handler cannot park; and
    //   * because of that, the loop can end. Ceasing to offer the attempt is the terminal
    //     event — not the passage of time — since a peer that received it decided within one
    //     synchronous poll.
    // ---------------------------------------------------------------------------------------

    /// How long these tests allow for an outcome that the fixed code reaches in about two
    /// seconds of bounded retries, and that the unfixed code never reaches at all.
    ///
    /// Generous on purpose: it is not measuring anything. It exists so that "the fan-out never
    /// terminates" is reported as a failure instead of hanging the suite.
    const TERMINAL_PATH_GRACE: Duration = Duration::from_secs(60);

    /// A `StartExecution` that never settles must end the fan-out, not hold the admission for
    /// the life of the controller.
    ///
    /// The worker here is reachable and capable — it advertised the contract — and answers
    /// `Unavailable` to every attempt, which is what a partition or a half-dead peer looks
    /// like from the controller. Before the fix the loop retried that at 250ms with no exit:
    /// the admission was never released, so the job could neither be rescheduled nor be
    /// refused, and the "bounded by the RPC deadline" claim on `settle_under_admission` was
    /// false because each expiry started another attempt.
    ///
    /// What is asserted is the pair: the attempt is given up after a bounded number of
    /// *replays of the same ID* (not a new attempt each time, which would be a second way to
    /// start a worker twice), and a refusal can be published the moment it is.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_permanently_unsettled_start_execution_ends_the_fan_out_and_releases_the_admission() {
        let never = NeverSettles::new();
        let mut run = SchedulingRun::with_workers(
            "never-settling-start-execution",
            vec![StartsExecution::NeverSettling(never.clone())],
        )
        .await;
        let gate = run.harness.refusal_gate.clone();
        run.harness
            .queue()
            .send(worker_connect_from(WorkerId(7), &run.address(0)))
            .await
            .unwrap();

        let outcome = tokio::time::timeout(TERMINAL_PATH_GRACE, run.schedule())
            .await
            .expect(
                "the fan-out must give an unsettleable attempt up: with no terminal path the \
                 loop retries at 250ms forever and the job can never reschedule or be refused",
            );

        let Err(err) = outcome else {
            panic!("a fan-out no worker ever accepted cannot have succeeded");
        };
        assert!(
            format!("{err:?}").contains("failed to initialize workers"),
            "and it must fail as a retryable scheduling error, so the next attempt raises the \
             generation and tears the old one down: {err:?}"
        );
        assert_eq!(
            never.calls.load(Ordering::SeqCst) as usize,
            START_EXECUTION_RECONCILE_ATTEMPTS + 1,
            "the first request plus a bounded number of reconciliation attempts, every one of \
             them a replay of the same attempt ID"
        );
        assert!(
            gate.admit_publication().is_some(),
            "and the admission must be free once the attempt is over: holding it past the last \
             request the controller will ever issue defers every refusal for this job forever"
        );
        assert_eq!(
            run.calls.started(),
            Vec::<String>::new(),
            "nothing started: this worker never applied anything, which is why giving the \
             attempt up loses knowledge rather than safety"
        );
    }

    /// A worker predating the reconciliation contract must never be sent a `StartExecution` at
    /// all.
    ///
    /// This is the mixed-version case of both round-15 findings, and it is removed rather than
    /// reasoned about. The worker here has the legacy shape end to end: it registers with
    /// `reconciles_start_execution` at its proto3 default of `false`, and its handler blocks
    /// the thread *inside one poll* on a `std::sync::Mutex`, which no stream reset and no
    /// controller exit can reach.
    ///
    /// Before the fix the controller sent it a request, that handler parked, and the fan-out
    /// spun on `Unavailable` forever. The assertion is that the handler is never entered — a
    /// request that was never issued is the only kind that certainly cannot be parked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_worker_predating_the_reconciliation_contract_is_never_sent_a_start_execution() {
        let blocked = BlockedWorker::new();
        // Declared first, so it is dropped last: on unfixed code the handler is inside its
        // wait when the assertions fire, and without this the suite hangs there instead of
        // reporting. See `ReleasedOnDrop`.
        let _released = ReleasedOnDrop(blocked.clone());
        let mut run = SchedulingRun::with_workers(
            "legacy-worker-not-scheduled",
            vec![StartsExecution::Blocking(blocked.clone())],
        )
        .await;
        blocked.watch(run.harness.refusal_gate.clone());
        run.harness
            .queue()
            .send(legacy_worker_connect_from(WorkerId(7), &run.address(0)))
            .await
            .unwrap();

        let outcome = tokio::time::timeout(TERMINAL_PATH_GRACE, run.schedule())
            .await
            .expect(
                "scheduling must fail on the legacy worker rather than issue it a request that \
                 parks a handler nothing can reach",
            );

        let Err(err) = outcome else {
            panic!("a job whose workers cannot reconcile a StartExecution must not be scheduled");
        };
        assert!(
            format!("{err:?}").contains("reconciles_start_execution"),
            "and it must say which worker property is missing, because the remedy is an \
             operator action — upgrade the worker image: {err:?}"
        );
        assert!(
            !blocked.started(),
            "the legacy handler must never have been entered: this is the whole guarantee, \
             since a handler blocked inside its own poll cannot be taken back once it has been"
        );
        assert_eq!(
            run.calls.started(),
            Vec::<String>::new(),
            "and nothing was started under any selector"
        );
    }

    /// Replacing the controller must not leave a legacy handler behind a fresh refusal gate.
    ///
    /// The reviewer's scenario, and the one the in-memory admission cannot answer on its own: a
    /// `RefusalGate` lives in the `StateMachine`, so a replacement controller builds a new one
    /// and its `admit_publication` succeeds immediately, however many handlers the previous
    /// controller left parked in a worker.
    ///
    /// The fix does not make that gate durable. It removes what the durability would have been
    /// for: a controller of this version never issues a `StartExecution` to a worker that could
    /// park one, so after any number of controller replacements there is no parked legacy
    /// handler for a fresh gate to be raced by. This runs that end to end — schedule under the
    /// first controller, drop it whole, publish a refusal on the replacement's brand-new gate,
    /// and only then let the handler out — and asserts the handler was never entered by either.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_controller_replacement_leaves_no_legacy_start_execution_handler_behind_its_gate() {
        let blocked = BlockedWorker::new();
        let _released = ReleasedOnDrop(blocked.clone());

        // The first controller, with its own gate, which goes away with it.
        let mut first = SchedulingRun::with_workers(
            "legacy-across-controller-replacement",
            vec![StartsExecution::Blocking(blocked.clone())],
        )
        .await;
        let worker_address = first.address(0);
        first
            .harness
            .queue()
            .send(legacy_worker_connect_from(WorkerId(7), &worker_address))
            .await
            .unwrap();
        let first_outcome = tokio::time::timeout(TERMINAL_PATH_GRACE, first.schedule())
            .await
            .expect("the first controller must not park a handler in the legacy worker");
        assert!(
            first_outcome.is_err(),
            "the first controller refuses to schedule onto a legacy worker"
        );

        // The controller process is replaced: a different `StateMachine`, and with it a
        // different `RefusalGate` and a different admission mutex. Nothing in memory survives.
        drop(first);
        let mut replacement = SchedulingRun::with_workers(
            "legacy-across-controller-replacement-2",
            vec![StartsExecution::Blocking(blocked.clone())],
        )
        .await;
        let fresh_gate = replacement.harness.refusal_gate.clone();
        blocked.watch(fresh_gate.clone());
        replacement
            .harness
            .queue()
            .send(legacy_worker_connect_from(
                WorkerId(7),
                &replacement.address(0),
            ))
            .await
            .unwrap();
        let replacement_outcome = tokio::time::timeout(TERMINAL_PATH_GRACE, replacement.schedule())
            .await
            .expect("nor may the replacement");
        assert!(replacement_outcome.is_err());

        // The refusal the old handler was supposed to be able to start behind.
        let admission = fresh_gate
            .admit_publication()
            .expect("the replacement's gate is free: nothing was ever issued under it");
        fresh_gate.publish(
            &admission,
            RefusedConfig::new(selector_changed(), 1, Arc::new(AtomicU64::new(1))),
        );
        drop(admission);

        // And now let the handler out, which is where the hazard would materialise.
        blocked.release();
        tokio::task::yield_now().await;

        assert!(
            !blocked.started(),
            "a legacy handler started behind a refusal published by a replacement controller: \
             the only defence against that is never to have issued it a request, and one was \
             issued"
        );
        assert!(
            !blocked.saw_refusal(),
            "and it cannot have observed the replacement's refusal, because it never ran"
        );
        assert_eq!(
            replacement.calls.started(),
            Vec::<String>::new(),
            "nothing started under the replacement either"
        );
    }

    /// The production half of `scheduling.rs` — everything before its own `#[cfg(test)]` — as
    /// raw source.
    ///
    /// The audit below is about the code that runs, and the file's test module contains both
    /// `file://` URLs and copies of the words being searched for.
    fn scheduling_source() -> &'static str {
        let source = include_str!("scheduling.rs");
        &source[..source
            .find("\n#[cfg(test)]")
            .expect("scheduling.rs has a test module")]
    }

    /// The same, with line comments removed, so a needle found in it is code.
    fn scheduling_source_without_comments() -> String {
        scheduling_source()
            .lines()
            .map(|line| match line.find("//") {
                // Sound only while no string literal in this half of the file contains `//`,
                // which `the_region_audits_comment_stripper_is_sound_for_scheduling_rs` checks.
                Some(at) => &line[..at],
                None => line,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The body of a function in `scheduling.rs`, by its signature, comments removed.
    fn scheduling_body(source: &str, signature: &str) -> String {
        let start = source
            .find(signature)
            .unwrap_or_else(|| panic!("{signature} has been renamed"));
        let rest = &source[start..];
        let end = rest
            .find("\n    }\n")
            .or_else(|| rest.find("\n}\n"))
            .expect("unterminated function body");
        rest[..end].to_string()
    }

    /// `Scheduling::next`, split into the stretches an [`Admission`] is held across and the
    /// stretches it is not.
    ///
    /// A region opens at `ctx.admit_irreversible_scheduling(` and closes at `drop(admission)`.
    /// They must strictly alternate, which is itself checked: two opens in a row would be the
    /// state taking a lock it already holds, and `tokio`'s mutex does not re-enter — the job
    /// would wedge on itself rather than fail.
    fn admitted_and_interruptible(body: &str) -> (Vec<String>, Vec<String>) {
        const OPEN: &str = "ctx.admit_irreversible_scheduling(";
        const CLOSE: &str = "drop(admission)";

        let mut marks: Vec<(usize, bool)> = body
            .match_indices(OPEN)
            .map(|(i, _)| (i, true))
            .chain(body.match_indices(CLOSE).map(|(i, _)| (i, false)))
            .collect();
        marks.sort_unstable();
        assert!(
            marks
                .iter()
                .enumerate()
                .all(|(n, (_, is_open))| *is_open == (n % 2 == 0)),
            "every admitted region must be opened and then closed before the next is opened; \
             found {:?}",
            marks.iter().map(|(_, o)| *o).collect::<Vec<_>>()
        );
        assert!(
            marks.len().is_multiple_of(2),
            "an admitted region was opened and never closed"
        );

        let mut admitted = vec![];
        let mut interruptible = vec![];
        let mut prev = 0;
        for pair in marks.chunks(2) {
            let (open, close) = (pair[0].0, pair[1].0);
            interruptible.push(body[prev..open].to_string());
            admitted.push(body[open..close].to_string());
            prev = close + CLOSE.len();
        }
        interruptible.push(body[prev..].to_string());
        (admitted, interruptible)
    }

    /// The regions, and everything in them, pinned on source text rather than on behaviour.
    ///
    /// **This is a structural source pin, and the name says so.** What it asserts is where the
    /// words are in `scheduling.rs`, not what the job does; the behaviour is covered by
    /// `a_refusal_published_after_the_gate_snapshot_still_stops_the_scheduling_preamble`,
    /// `a_refusal_queued_behind_the_worker_connects_stops_the_execution_rpcs`,
    /// `a_refusal_queued_behind_the_task_starts_stops_the_recovered_commits` and their control.
    ///
    /// It exists because the guarantee is a property of a *region*: every irreversible effect
    /// between an `admit_irreversible_scheduling` and its `drop` is covered without anyone
    /// remembering anything. What no test of behaviour can notice is an effect added *outside*
    /// every region — which is exactly how rounds 7, 8 and 9 each found the next unguarded
    /// phase — so the boundaries are pinned here together with the whole inventory of effects
    /// they contain, and with the inventory of what the *interruptible* stretches are allowed
    /// to await. A new `await` outside a region fails this test, and its author then has to say
    /// whether what it waits on is an effect. That is the intended reading of a failure here:
    /// not "the test is stale", but "decide which side of the boundary this belongs on".
    #[test]
    fn the_source_of_scheduling_next_keeps_every_irreversible_effect_inside_an_admitted_region() {
        let source = scheduling_source_without_comments();
        let body = scheduling_body(
            &source,
            "    async fn next(mut self: Box<Self>, ctx: &mut JobContext)",
        );
        let (admitted, interruptible) = admitted_and_interruptible(&body);

        assert_eq!(
            admitted.len(),
            3,
            "three regions: the destructive preamble, the `StartExecution` fan-out, and the \
             publication of a restored checkpoint's commits"
        );

        let count = |regions: &[String], needle: &str| -> usize {
            regions.iter().map(|r| r.matches(needle).count()).sum()
        };

        // Every irreversible effect of this state, by the call that performs it. Each must
        // appear, and every occurrence of it must be inside a region.
        for effect in [
            "ctx.status.generation += 1",
            "ctx.status.update_db(",
            "stop_workers(",
            "self.start_workers(",
            "get_and_register_checkpoint_info_leader(",
            "get_checkpoint_info_legacy(",
            "start_execution_on_workers(",
            "send_commit_messages(",
        ] {
            assert_eq!(
                count(&admitted, effect),
                1,
                "`{effect}` is irreversible and must appear exactly once, inside an admitted \
                 region: a refusal published before it would otherwise not be read until the \
                 next state, or not at all"
            );
            assert_eq!(
                count(&interruptible, effect),
                0,
                "`{effect}` must not also appear outside every region"
            );
        }

        // And the converse: the interruptible stretches wait, and do nothing else. Anything
        // they await is listed here.
        let interruptible_awaits = [
            ("handle_worker_connect(", 1),
            ("h.await", 1),
            ("ctx.handle_task_error(", 1),
            ("LeaderManager::connect(", 2),
            ("poll_leader_status(", 1),
            ("ctx.handle_job_failure(", 1),
            ("ctx.metrics", 1),
        ];
        for (waited_on, times) in interruptible_awaits {
            assert_eq!(
                count(&interruptible, waited_on),
                times,
                "the interruptible phases are pinned to what they wait on, and `{waited_on}` \
                 is no longer awaited {times} time(s) outside a region"
            );
        }
        assert_eq!(
            count(&interruptible, ".await"),
            interruptible_awaits.iter().map(|(_, n)| n).sum::<usize>(),
            "an `.await` appeared in `Scheduling` outside every admitted region. If it waits \
             on something irreversible or externally visible it belongs inside one — that is \
             the failure this pin exists to force a decision about — and if it does not, add \
             it to the list above"
        );

        assert_eq!(
            count(&admitted, "ctx.rx.recv()"),
            0,
            "and no admission is ever held across a wait on the job's channel: that would make \
             the job unrefusable for as long as it waited, and could not terminate if what it \
             waited for was the refusal"
        );

        // The names, which are the enumeration itself. Two of them are performed by helpers
        // that take an `&Admission`, so they are named there rather than in `next`; the whole
        // file's inventory is what is pinned.
        let mut effects: Vec<&str> = source
            .match_indices(".effect(")
            .map(|(i, _)| {
                let rest = &source[i + ".effect(".len()..];
                let name = &rest[rest.find('"').expect("an effect is named") + 1..];
                &name[..name.find('"').expect("an unterminated effect name")]
            })
            .collect();
        effects.sort_unstable();
        assert_eq!(
            effects,
            [
                "persist the incremented scheduling generation",
                "prepare the legacy recovery checkpoint",
                "publish the restored checkpoint's commits",
                "register the generation and prepare its recovery checkpoint",
                "send every worker its StartExecution",
                "start the job's replacement workers",
                "tear down the job's existing cluster",
            ],
            "these are the irreversible effects of `Scheduling`; adding one means deciding, \
             here, which region it belongs in"
        );

        // The one effect whose call is a helper rather than a statement: the RPCs must be
        // inside the helper's own `effect`, not merely inside the function that owns it.
        let fan_out = scheduling_body(&source, "async fn start_execution_on_workers(");
        let effect_at = fan_out
            .find(".effect(")
            .expect("the fan-out must go through `Admission::effect`");
        assert!(
            fan_out
                .match_indices(".start_execution(")
                .all(|(i, _)| i > effect_at),
            "the `StartExecution` RPCs must be issued inside the effect, not before it"
        );

        // And the lifetime half of the same guarantee, which no region boundary can express.
        //
        // Round 11 read this as "the requests must not outlive the region", and made them
        // children of the state task so that every exit took them with it. That is all a
        // *client* future can be made to do. Dropping one resets its stream; it does not stop a
        // server handler that has already been entered. The current worker uses `try_lock` so
        // it never parks there, but the fan-out still has to tolerate legacy or independently
        // implemented handlers that block inside one poll and ignore the reset.
        //
        // So the fan-out no longer ends its requests: it waits for them, and owns the admission
        // while it does. `settle_under_admission` is what makes that survive the state task
        // being dropped — the region and its admission are rescued together, so the one thing
        // that outlives cancellation is the interlock itself.
        assert!(
            fan_out.contains("settle_under_admission("),
            "the `StartExecution` fan-out must own its admission through \
             `settle_under_admission`, so that neither a sibling's failure nor the job's state \
             task being dropped can release the admission while a request it issued is still \
             unsettled. Taking an `&Admission` again would put the guarantee back on the \
             caller's drop order, which says nothing about a worker that is already running"
        );

        // A region covers the work *this task* does between its boundaries. `tokio::spawn`
        // hands its child an independent lifetime — dropping a `JoinHandle` detaches the task
        // rather than cancelling it — so a spawned effect is precisely an effect the region
        // cannot end. The one spawn left in this file is `handle_worker_connect`'s channel
        // setup, which is outside every region, does nothing irreversible, and is awaited to
        // completion by the phase that started it. A second one means someone has given a
        // piece of `Scheduling` a life of its own, and has to say which region ends it.
        //
        // `settle_under_admission` does spawn, on the cancellation path, and it lives in
        // `states/mod.rs` rather than here for exactly that reason: what it detaches is the
        // *admission*, wrapped around the unsettled requests, which is the opposite of what
        // round 10 detached and is why this count can stay honest at one.
        assert_eq!(
            source.matches("tokio::spawn").count(),
            1,
            "`Scheduling` may spawn exactly one thing — the worker channel setup in \
             `handle_worker_connect`. Anything else spawned here can outlive the admission \
             that authorised it, because dropping its handle detaches it rather than \
             cancelling it, and can then reach a worker after a refusal has been published"
        );
    }

    /// The assumption [`scheduling_source_without_comments`] rests on.
    ///
    /// **A source pin, like the test above, and it asserts about source text only.** Stripping
    /// `//` to end-of-line is sound only while no string literal in the audited half of the
    /// file contains `//`; a URL in a log message would silently truncate a line and could
    /// hide an effect from the region audit.
    #[test]
    fn the_region_audits_comment_stripper_is_sound_for_scheduling_rs() {
        for (n, line) in scheduling_source().lines().enumerate() {
            let Some(at) = line.find("//") else { continue };
            assert_eq!(
                line[..at].matches('"').count() % 2,
                0,
                "line {} of scheduling.rs has `//` inside a string literal, which would make \
                 the region audit strip real code: {line}",
                n + 1
            );
        }
    }

    /// Neither the states nor their tests may reach for the process's cluster identity.
    ///
    /// **A source pin: it asserts about the text of `states/mod.rs` and `states/scheduling.rs`,
    /// not about what the controller does.** The behaviour it protects is not the controller's
    /// either — it is the developer's machine.
    ///
    /// `arroyo_server_common`'s cluster-id setter is not a setter. It resolves the identity
    /// against `~/.config/arroyo/cluster-info` and, whenever that directory exists and holds
    /// nothing valid, *writes* the value there; every later real Arroyo process on that machine
    /// then inherits it. A test fixture that called it to satisfy `Scheduling::start_workers`
    /// therefore gave `cargo test -p arroyo-controller` the power to create or overwrite a
    /// developer's cluster identity with a fixed test uuid — on any machine where a real
    /// Arroyo had ever run, which is exactly the machines that would notice.
    ///
    /// The states now take the identity ([`JobContext::cluster_id`]); the controller reads the
    /// process-wide cell once, in `start_updater`, where a test never goes. What is pinned here
    /// is that neither half of either file goes back — including the tests, which is why the
    /// needles are assembled rather than written out, so this test does not match itself.
    #[test]
    fn no_state_and_no_state_test_reaches_for_a_process_wide_cluster_identity() {
        for (file, source) in [
            ("states/mod.rs", include_str!("mod.rs")),
            ("states/scheduling.rs", include_str!("scheduling.rs")),
        ] {
            for needle in [concat!("set_", "cluster_id"), concat!("get_", "cluster_id")] {
                assert!(
                    !source.contains(needle),
                    "{file} calls `{needle}`. The identity a job stamps into its workers is \
                     handed to the state, not fetched from a process-wide cell — and the cell's \
                     setter persists what it is given to the machine's own configuration, so a \
                     test that populates it changes the machine rather than the test"
                );
            }
        }
    }

    /// The ordering half of the fix, pinned on statement order rather than on behaviour.
    ///
    /// The gate can only stop a state task that was handed a refusal before that task ran,
    /// so every path that starts one has to record the refusal first. On the multi-threaded
    /// runtime the controller actually runs on, a task spawned before the refusal is recorded
    /// can reach `Compiling` — and from there `Scheduling` — on another worker thread while
    /// the poll that spawned it is still on its way to recording it. There are exactly two
    /// such paths, and both are covered here.
    ///
    /// **This is a structural pin, not a behavioural test, and deliberately so.** The window
    /// it closes is a thread interleaving of a few instructions: a test that raced it would
    /// pass or fail by scheduling luck, and every `#[tokio::test]` here runs on a
    /// current-thread runtime where the window does not exist at all. What the gate does once
    /// the refusal *is* recorded is covered behaviourally, by
    /// `a_known_refusal_fails_the_restarted_task_before_it_can_reschedule_the_job` and
    /// `a_cold_adopted_job_is_failed_by_its_refusal_before_it_schedules_anything`.
    #[test]
    fn both_paths_that_start_a_job_are_written_to_record_the_refusal_first() {
        let source = include_str!("mod.rs");

        let body_of = |signature: &str| -> &str {
            let start = source
                .find(signature)
                .unwrap_or_else(|| panic!("{signature} has been renamed"));
            let rest = &source[start..];
            &rest[..rest.find("\n    }\n").expect("unterminated function body")]
        };

        for (name, signature, starters) in [
            (
                "apply_refused_row",
                "    async fn apply_refused_row(",
                ["self.request_stop(", "self.restart_if_needed("].as_slice(),
            ),
            (
                "StateMachine::new",
                "    pub async fn new(",
                ["this.start(", "this.apply_refused_row("].as_slice(),
            ),
        ] {
            let body = body_of(signature);
            let recorded = body.find("note_refused_row(").unwrap_or_else(|| {
                panic!("{name} must record the refused row before it does anything else")
            });
            for starter in starters {
                let started = body
                    .find(starter)
                    .unwrap_or_else(|| panic!("{name} must still call {starter}"));
                assert!(
                    recorded < started,
                    "{name} must record the refusal before {starter}: a state task started \
                     first can reach `Scheduling` on another thread before anything has \
                     applied the refusal to it"
                );
            }
        }
    }

    /// A refused row that asks for a stop must still be answered by the stop, not by the
    /// gate.
    ///
    /// The gate fails the job wherever it is, which is the right policy for a refusal that
    /// has no other answer and the wrong one for a refusal that does: a `checkpoint` or
    /// `graceful` stop exists to take a final checkpoint, and a job failed on its way into
    /// [`Stopping`] would end in `Failed` without one. So a refusal recorded as answered by
    /// a stop publishes nothing to the gate, and supersedes anything already published.
    #[tokio::test]
    async fn a_refusal_a_stop_is_answering_never_reaches_the_gate() {
        for stop_mode in [
            StopMode::checkpoint,
            StopMode::graceful,
            StopMode::immediate,
            StopMode::force,
        ] {
            let current = running_config(StateBackendSelector::Parquet);
            let (mut sm, _rx) = state_machine(current.clone(), StateBackendSelector::Parquet);

            // First the row is only bad, so a refusal is published for the gate to apply.
            sm.update(
                polled(
                    StateBackendSelector::Parquet,
                    current.clone(),
                    Some(selector_changed()),
                ),
                job_status(current.restart_nonce),
                &shutdown_guard(),
            )
            .await;
            assert!(
                sm.refusal_gate.clone().take().is_some(),
                "{stop_mode:?}: a refusal with no other answer must gate the next state"
            );

            // Then the operator applies the remedy the refusal itself documents.
            let mut stopping = running_config(StateBackendSelector::Parquet);
            stopping.stop_mode = stop_mode;
            sm.update(
                polled(
                    StateBackendSelector::Parquet,
                    stopping,
                    Some(selector_changed()),
                ),
                job_status(current.restart_nonce),
                &shutdown_guard(),
            )
            .await;

            assert!(
                sm.refusal_gate.clone().take().is_none(),
                "{stop_mode:?}: once a stop is answering the refusal, the gate must let the \
                 job reach `Stopping` — failing it first is what loses the final checkpoint"
            );
            assert_eq!(
                sm.config.read().unwrap().0.stop_mode,
                stop_mode,
                "{stop_mode:?}: and the stop is the thing that was actually issued"
            );
        }
    }

    /// A repaired row clears the gate as well as the queue.
    ///
    /// Round 5's versioning saves a repaired job from a refusal already sitting in its queue.
    /// The gate is a second holder of the same refusal and must be superseded by the same
    /// repair — otherwise the gate would fail a job for a configuration that no longer
    /// exists, which is round 5's bug reintroduced on a new route.
    #[tokio::test]
    async fn a_repair_supersedes_the_refusal_the_gate_is_holding() {
        let current = running_config(StateBackendSelector::Parquet);
        let (mut sm, _rx) = state_machine(current.clone(), StateBackendSelector::Parquet);

        sm.update(
            polled(
                StateBackendSelector::Parquet,
                current.clone(),
                Some(selector_changed()),
            ),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;
        assert!(
            sm.refusal_gate.clone().take().is_some(),
            "the refusal must gate the next state while the row is bad"
        );

        sm.update(
            polled(StateBackendSelector::Parquet, current.clone(), None),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;
        assert!(
            sm.refusal_gate.clone().take().is_none(),
            "and must stop gating the moment the operator repairs the row"
        );

        // The control: the gate is still live, so a row that goes bad again gates afresh.
        sm.update(
            polled(
                StateBackendSelector::Parquet,
                current.clone(),
                Some(selector_changed()),
            ),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;
        assert!(
            sm.refusal_gate.clone().take().is_some(),
            "the test would not detect the supersession if the gate never fired again"
        );
    }

    /// A state that stands in for one already blocked on the job's channel when a refusal
    /// arrives: the poll thread reaches the job while the state is running, so the refusal is
    /// published to the gate *and* queued, and the state reads it off the channel itself.
    ///
    /// That is the prompt route the gate does not replace, and the only way to reach
    /// [`execute_state`]'s fatal handling with a refusal the gate has not already applied.
    /// The real states that take it — `Running`, `Scheduling`'s worker loop, `LeaderRunning`
    /// — all need a live scheduler, worker set and `JobController` to run.
    #[derive(Debug)]
    struct ReadsItsRefusal(RefusedConfig);

    #[async_trait::async_trait]
    impl State for ReadsItsRefusal {
        /// Stays, for the same reason: the row that uses it is about a refusal a state reads
        /// off its own channel, and its context carries no lifecycle actor.
        fn leave_for_stop(self: Box<Self>, _config: &JobConfig) -> LeavingForStop {
            LeavingForStop::Stays(self)
        }

        fn name(&self) -> &'static str {
            "ReadsItsRefusal"
        }

        async fn next(self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
            ctx.refusal_gate
                .publish(&admitted(&ctx.refusal_gate), self.0.clone());
            ctx.handle(JobMessage::ConfigRefused(self.0))?;
            unreachable!("a current refusal fails the job")
        }
    }

    /// One refusal fails the job once, whichever of its two routes reached the job first.
    ///
    /// The gate is consulted before every state, so a refusal a state has already read off
    /// its own channel would otherwise be applied a second time — failing the job again
    /// before [`Failing`] could tear the cluster down, and reporting the same fatal error
    /// twice on the way to `Failed`.
    #[tokio::test]
    async fn a_refusal_a_state_read_itself_does_not_gate_the_failure_that_follows_it() {
        let db = sqlite_startable_job("Running", 2);
        let refusal = RefusedConfig::new(selector_changed(), 1, Arc::new(AtomicU64::new(1)));

        let mut harness = Harness::new(3).with_db(db.clone());
        let ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );

        let (next, ctx) = execute_state(Box::new(ReadsItsRefusal(refusal)), ctx).await;
        assert_eq!(
            next.as_ref().map(|s| s.name()),
            Some("Failing"),
            "a refusal a state reads for itself still fails the job"
        );

        let (next, _ctx) = execute_state(next.unwrap(), ctx).await;
        assert_eq!(
            next.as_ref().map(|s| s.name()),
            Some("Failed"),
            "and the gate must then let the failure run: one refusal is one failure, \
             whichever of its two routes reached the job first"
        );
        assert_eq!(
            state_writes(&db),
            [("Failing".to_string(), 1), ("Failed".to_string(), 1)],
            "so the job goes to `Failed` through one `Failing`, not two"
        );
    }

    /// The same rule at the level of the gate itself, including that a *later* refusal is a
    /// different fact about the job and still applies.
    #[test]
    fn the_gate_applies_each_refusal_once_and_a_later_one_afresh() {
        let mut gate = RefusalGate::default();
        let version = Arc::new(AtomicU64::new(7));
        gate.publish(
            &admitted(&gate),
            RefusedConfig::new(selector_changed(), 7, Arc::clone(&version)),
        );

        // What a state that read the refusal off its own channel leaves behind.
        gate.disarm();
        assert!(
            gate.take().is_none(),
            "a refusal a state has already turned fatal must not be turned fatal again"
        );

        // A *later* refusal is a different fact about the job and still gates, which is what
        // keeps a job restarting out of `Failed` from restarting into a refused row.
        version.store(8, std::sync::atomic::Ordering::SeqCst);
        gate.publish(
            &admitted(&gate),
            RefusedConfig::new(selector_changed(), 8, version),
        );
        assert!(gate.take().is_some());
        assert!(
            gate.take().is_none(),
            "and each refusal is applied at most once"
        );
    }

    /// Round 5 left the accepted-update `Inactive` branch without a roll-back and argued it
    /// was unnecessary because the next poll reaches `restart_if_needed` anyway. Round 6's
    /// finding is a case where exactly that argument failed, because a different branch
    /// returned first — so the argument is checked here instead of repeated.
    ///
    /// An accepted update handed to a job with no state task is stored, and storing it is
    /// precisely what sends every later poll down the unchanged-row branch and into
    /// `restart_if_needed`, which retries `start` for as long as the configuration is
    /// `NotApplied`. When the program finally loads, the job starts under it.
    #[tokio::test]
    async fn an_accepted_update_that_could_not_start_is_retried_until_it_can() {
        let db = sqlite_startable_job("Running", 2);
        program_loadable(&db, false);

        let current = running_config(StateBackendSelector::Parquet);
        let mut sm = state_machine_with(
            current.clone(),
            StateBackendSelector::Parquet,
            None,
            db.clone(),
        );

        let mut updated = running_config(StateBackendSelector::Parquet);
        updated.restart_nonce = current.restart_nonce + 1;

        for poll in 0..3 {
            sm.update(
                polled(StateBackendSelector::Parquet, updated.clone(), None),
                job_status(current.restart_nonce),
                &shutdown_guard(),
            )
            .await;

            assert!(
                sm.done(),
                "poll {poll}: the program cannot be loaded, so nothing took the update"
            );
            assert_eq!(
                sm.config.read().unwrap().0,
                updated,
                "poll {poll}: the update stays stored — there is no head guard here to \
                 short-circuit on it, so storing it is what makes the next poll retry"
            );
            assert_eq!(
                recorded_status(&db).1,
                None,
                "poll {poll}: and no execution has begun, so none is recorded"
            );
        }

        program_loadable(&db, true);
        sm.update(
            polled(StateBackendSelector::Parquet, updated.clone(), None),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;

        assert!(
            !sm.done(),
            "the retry the roll-back-free branch relies on has to actually happen"
        );
        assert_eq!(
            recorded_status(&db),
            ("Compiling".to_string(), Some("parquet".to_string()))
        );
    }

    /// The check every non-running consumer applies, in both directions: a changed
    /// selector is fatal, and an unchanged one — including a row edited from `""` to
    /// `"parquet"` — passes.
    #[test]
    fn a_config_update_is_checked_against_the_execution_selector() {
        let updated = running_config(StateBackendSelector::StateEngine);
        let err = check_config_update(StateBackendSelector::Parquet, &updated)
            .expect_err("a selector change must not be accepted mid-transition");
        assert!(
            matches!(
                selector_error(&err),
                StateBackendError::JobSelectorChanged { .. }
            ),
            "{err:?}"
        );

        check_config_update(
            StateBackendSelector::Parquet,
            &running_config(StateBackendSelector::normalize("", "job").unwrap()),
        )
        .expect("an unchanged selector must still be accepted");
        check_config_update(
            StateBackendSelector::StateEngine,
            &running_config(StateBackendSelector::StateEngine),
        )
        .expect("an unchanged selector must still be accepted");
    }

    /// The refusal is durable, driven through the real cleanup and restart states.
    ///
    /// A selector change arriving with a new restart nonce is classified fatal by both
    /// running modes, and a fatal error goes to `Failing` and then `Failed`. `Failed`
    /// restarts the job whenever the config's nonce is ahead of the status's — so if the
    /// refused row had been stored, the update the classifier promised not to restart
    /// would restart, under the refused selector. Because the row is refused before the
    /// shared config is replaced, `Failed` sees the nonce it started with and stops.
    ///
    /// The second half of the test is the control: the same states, with the new nonce
    /// actually adopted, do restart. That is what makes the first half a statement about
    /// the refusal rather than about `Failed`.
    #[tokio::test]
    async fn a_refused_selector_and_nonce_do_not_restart_the_job_through_the_fatal_path() {
        let current = running_config(StateBackendSelector::Parquet);
        let mut updated = running_config(StateBackendSelector::StateEngine);
        updated.restart_nonce = current.restart_nonce + 1;

        // both modes classify it the same way, through the one shared rule
        let err = classify_running_config_update(
            StateBackendSelector::Parquet,
            &current,
            &updated,
            current.restart_nonce,
        )
        .expect_err("a selector change must be fatal");
        assert!(matches!(err, StateError::FatalError { .. }), "{err:?}");

        // the fatal path, with the refused row never stored
        let (mut sm, _rx) = state_machine(current.clone(), StateBackendSelector::Parquet);
        let mut refused = running_config(StateBackendSelector::Parquet);
        refused.restart_nonce = current.restart_nonce;
        sm.update(
            polled(
                StateBackendSelector::Parquet,
                refused,
                Some(selector_changed()),
            ),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;
        let after_refusal = sm.config.read().unwrap().0.clone();

        let mut harness = Harness::new(current.restart_nonce);
        let mut ctx = harness.ctx(after_refusal, StateBackendSelector::Parquet);

        let Ok(Transition::Advance(next)) = Box::new(Failing {}).next(&mut ctx).await else {
            panic!("Failing must advance");
        };
        assert_eq!(next.state.name(), "Failed");

        let stopped = matches!(
            Box::new(Failed {}).next(&mut ctx).await,
            Ok(Transition::Stop)
        );
        assert!(
            stopped,
            "a job failed by a refused selector must not restart into it"
        );
        assert_eq!(
            harness.scheduler.stopped.lock().unwrap().as_slice(),
            [("job_abc".to_string(), Some(1))],
            "`Failed` still tears the cluster down, under the generation it knows; refusing \
             the row does not skip cleanup"
        );

        // the control: had the new nonce been adopted, Failed would have restarted
        let mut harness = Harness::new(current.restart_nonce);
        let mut ctx = harness.ctx(updated, StateBackendSelector::StateEngine);
        let restarted = matches!(
            Box::new(Failed {}).next(&mut ctx).await,
            Ok(Transition::Advance(_))
        );
        assert!(
            restarted,
            "the test would not detect the refusal if Failed never restarted"
        );
    }

    /// The transition funnel refuses to adopt a configuration with a different selector,
    /// so even a writer that got past `StateMachine::update` could not make the next state
    /// treat the new value as its baseline.
    #[test]
    fn a_refreshed_config_with_a_different_selector_is_not_adopted() {
        let mut refreshed = running_config(StateBackendSelector::StateEngine);
        refreshed.restart_nonce = 99;
        assert!(
            adopt_refreshed_config(refreshed, StateBackendSelector::Parquet, "pipeline_1")
                .is_none()
        );

        // and the ordinary refresh still happens
        let mut refreshed = running_config(StateBackendSelector::Parquet);
        refreshed.restart_nonce = 99;
        let adopted =
            adopt_refreshed_config(refreshed, StateBackendSelector::Parquet, "pipeline_1")
                .expect("an unchanged selector must still be adopted");
        assert_eq!(adopted.restart_nonce, 99);
    }

    // ---------------------------------------------------------------------------------------
    // M11.T25a / M11.T25f — the D39a single writer, and the seam that leaves it unselected.
    //
    // The rows below need a whole `StateMachine`: a job with no state task, a program that
    // will not load, a database that records every status write. The rows that are about the
    // intent and the decision alone live beside their modules, in `states/lifecycle/tests.rs`.
    // ---------------------------------------------------------------------------------------

    /// No production path can select the D39a lifecycle (M11.T25f, DoD M11.T25l).
    ///
    /// **A structural source pin, and the name says so.** Its companion
    /// `production_selects_only_the_legacy_t08_lifecycle` proves that the *selection* names
    /// `LegacyT08` exhaustively over the enum; what no test of that selection can notice is
    /// a second construction site that never consults it. `JobLifecycle::for_mode` takes the
    /// mode as an argument — which is what lets a test build the new path directly — so the
    /// claim "production is `LegacyT08`" is a claim about call sites, and this is where they
    /// are counted.
    ///
    /// The intended reading of a failure here is not "the test is stale" but "say why a
    /// second production path is choosing a lifecycle mechanism". M11.T26 is the owner that
    /// changes the answer, together with the durable fence and worker protocol that make the
    /// new path's settlement claim true.
    #[test]
    fn no_production_path_selects_the_fenced_v2_lifecycle() {
        /// Everything in a file before its test module, so a mention inside a test does not
        /// count as a production one.
        fn production_half(source: &'static str) -> &'static str {
            match source.find("\n#[cfg(test)]") {
                Some(at) => &source[..at],
                None => source,
            }
        }

        let states = production_half(include_str!("mod.rs"));
        let scheduling = production_half(include_str!("scheduling.rs"));

        assert_eq!(
            states.matches("JobLifecycle::for_mode(").count(),
            1,
            "a job's lifecycle mechanism is chosen in exactly one production place. A second \
             one is a second thing that could choose differently"
        );
        assert!(
            states.contains("JobLifecycle::for_mode(lifecycle::LifecycleMode::SELECTED"),
            "and it passes the selection rather than a literal, so it inherits the exhaustive \
             `LegacyT08` result that `production_selects_only_the_legacy_t08_lifecycle` pins"
        );

        for (file, source) in [
            ("states/mod.rs", states),
            ("states/scheduling.rs", scheduling),
        ] {
            assert_eq!(
                source.matches("LifecycleMode::FencedV2").count(),
                0,
                "{file}: the D39a mode is never named on a production path outside the module \
                 that defines it — naming it is how it would come to be selected"
            );
        }

        assert_eq!(
            include_str!("lifecycle/mod.rs")
                .matches("LifecycleMode::FencedV2")
                .count(),
            1,
            "the only production code that names the D39a mode is `JobLifecycle::for_mode`'s \
             own match arm, which is what makes the mode a seam rather than a switch"
        );
        assert!(
            include_str!("lifecycle/mode.rs").contains("LifecycleMode::FencedV2 => false,"),
            "and the selection's exhaustive answer for it is `false`: M11.T25 builds the \
             substrate and M11.T26 activates it"
        );
    }

    /// D96 row 6 (R3): a polled configuration is classified before it becomes anybody's
    /// baseline.
    ///
    /// The finding is that the configuration was adopted as the execution baseline and
    /// *then* classified, which is an order that cannot be repaired afterwards: whatever
    /// re-read the baseline in between has already run.
    ///
    /// Under D39a the poll thread has no baseline to replace. It classifies, leaves one
    /// intent, and stops; the job's own state task is the only thing that adopts anything,
    /// and it adopts the classification's *result* rather than the row. So the two halves
    /// below assert the same property from both sides: a refused row is adopted nowhere, and
    /// an accepted row is adopted only by the writer, only after classification.
    #[tokio::test]
    async fn classify_then_adopt_execution_config() {
        // The row an operator has edited to name another backend, which the poll has already
        // resolved against the job's execution record and refused.
        let current = running_config(StateBackendSelector::Parquet);
        let (tx, mut rx) = channel(16);
        let mut sm = state_machine_in_mode(
            LifecycleMode::FencedV2,
            current.clone(),
            StateBackendSelector::Parquet,
            Some(tx),
            unused_db(),
            Arc::new(RecordingScheduler::default()),
        );
        let mailbox = mailbox_of(&sm);

        let mut refused = running_config(StateBackendSelector::Parquet);
        refused.restart_nonce = 99;
        sm.update(
            polled(
                StateBackendSelector::Parquet,
                refused,
                Some(selector_changed()),
            ),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;

        assert_eq!(
            sm.config.read().unwrap().0,
            current,
            "the poll replaced no baseline at all: under D39a it is not a writer, so there \
             is no window in which a row that turns out to be refused has already been adopted"
        );
        assert!(
            rx.try_recv().is_err(),
            "and it published nothing to the job either — a decision is the state task's"
        );
        assert!(
            sm.refusal.is_none()
                && sm.refusal_version.load(Ordering::SeqCst) == 0
                && sm.refusal_gate.clone().take().is_none(),
            "and the M11.T08 cross-task machinery is untouched: the two mechanisms are \
             alternatives, not layers"
        );
        assert_eq!(
            standing_intent(&mailbox).map(VersionedIntent::into_intent),
            Some(LifecycleIntent::Refused(selector_changed())),
            "what the poll left is the classification, and nothing of the refused row \
             travels with it"
        );

        // The job's single writer, at its first consumption point.
        let mut harness = Harness::new(current.restart_nonce).with_actor(&mailbox);
        let mut ctx = harness.ctx(current.clone(), StateBackendSelector::Parquet);
        let refused_by_the_writer = ctx
            .observe_lifecycle_intent(ConsumptionPoint::BeforeIrreversiblePhase)
            .expect_err("the writer must fail the job its configuration was refused for");
        assert_eq!(selector_error(&refused_by_the_writer), &selector_changed());
        assert_eq!(
            ctx.config, current,
            "and the refused row is still adopted nowhere, including by the writer that \
             read it"
        );

        // The control, and the half that makes the assertions above about classification
        // rather than about a harness in which nothing is ever adopted.
        let (tx, _rx) = channel(16);
        let mut sm = state_machine_in_mode(
            LifecycleMode::FencedV2,
            current.clone(),
            StateBackendSelector::Parquet,
            Some(tx),
            unused_db(),
            Arc::new(RecordingScheduler::default()),
        );
        let mailbox = mailbox_of(&sm);

        let mut accepted = running_config(StateBackendSelector::Parquet);
        accepted.checkpoint_interval = Duration::from_secs(45);
        sm.update(
            polled(StateBackendSelector::Parquet, accepted.clone(), None),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;

        assert_eq!(
            sm.config.read().unwrap().0,
            current,
            "even an accepted row is not adopted by the poll: adoption is a lifecycle \
             decision, and D39a gives every decision one owner"
        );
        assert_eq!(
            standing_intent(&mailbox).map(VersionedIntent::into_intent),
            Some(LifecycleIntent::Adopt(Box::new(accepted.clone()))),
        );

        let mut harness = Harness::new(current.restart_nonce).with_actor(&mailbox);
        let mut ctx = harness.ctx(current.clone(), StateBackendSelector::Parquet);
        assert_eq!(
            ctx.observe_lifecycle_intent(ConsumptionPoint::BeforeIrreversiblePhase)
                .expect("an accepted row is adopted, not refused"),
            ObservedIntent::Adopted(Box::new(current.clone())),
            "the row asks for no stop, so the boundary does not leave — and it reports the \
             adoption carrying the configuration it replaced, which is what a running state \
             classifies the change against (PR #160 review comment `5365261487`)"
        );
        assert_eq!(
            ctx.config, accepted,
            "and the writer is what adopts it, at a point strictly after the classification \
             that let it through"
        );
    }

    /// D96 row 10 (R5): a refused row's stop survives a job that has no state task to
    /// execute it.
    ///
    /// The finding is that the stop disappeared. A cold controller cannot load the job's
    /// program — a transient `get_program` or database failure — so there is no state task,
    /// and the stop the refused row asks for has nowhere to go; recording it as issued made
    /// every later poll short-circuit on "already stopping" while nothing ever stopped the
    /// job.
    ///
    /// Under D39a there is nothing to record and nothing to short-circuit on. The poll
    /// classifies and leaves an intent, which simply stands in the job's mailbox until a
    /// writer exists; the writer that finally comes up reads it and stops the job. The job
    /// therefore ends in `Stopped`, with the final-checkpoint semantics the stop asked for,
    /// rather than in `Failed` — and it reaches neither by rescheduling anything on the way.
    #[tokio::test]
    async fn inactive_refused_row_honors_stop() {
        for stop_mode in [
            StopMode::checkpoint,
            StopMode::graceful,
            StopMode::immediate,
            StopMode::force,
        ] {
            let db = sqlite_startable_job("Running", 2);
            program_loadable(&db, false);
            // Held for the whole test: the task that finally comes up has to run, not just
            // exist.
            let shutdown = LiveShutdown::new();
            let scheduler = Arc::new(RecordingScheduler::default());

            let current = running_config(StateBackendSelector::Parquet);
            let mut sm = state_machine_in_mode(
                LifecycleMode::FencedV2,
                current.clone(),
                StateBackendSelector::Parquet,
                None,
                db.clone(),
                scheduler.clone(),
            );
            let mailbox = mailbox_of(&sm);

            let mut refused = running_config(StateBackendSelector::Parquet);
            refused.stop_mode = stop_mode;
            let refused_poll = || {
                polled(
                    StateBackendSelector::Parquet,
                    refused.clone(),
                    Some(selector_changed()),
                )
            };

            for poll in 0..3 {
                sm.update(
                    refused_poll(),
                    job_status(current.restart_nonce),
                    shutdown.guard(),
                )
                .await;

                assert!(
                    sm.done(),
                    "{stop_mode:?} poll {poll}: the program still cannot be loaded, so there \
                     is no writer to execute the stop"
                );
                assert_eq!(
                    sm.config.read().unwrap().0.stop_mode,
                    StopMode::none,
                    "{stop_mode:?} poll {poll}: and the poll records nothing, so it cannot \
                     short-circuit a later one on a stop nobody executed"
                );
                let standing = standing_intent(&mailbox).unwrap_or_else(|| {
                    panic!(
                        "{stop_mode:?} poll {poll}: the stop must not be lost with the \
                                refusal — a job with no state task is exactly the case in \
                                which it used to disappear"
                    )
                });
                assert_eq!(
                    standing.version().as_u64(),
                    1,
                    "{stop_mode:?} poll {poll}: the same row polled again is the same intent, \
                     so the job stands where it stood"
                );
                assert_eq!(
                    standing.into_intent(),
                    LifecycleIntent::RefusedButStopping {
                        error: selector_changed(),
                        stop_mode,
                    },
                    "{stop_mode:?} poll {poll}: and what stands is the stop, carried through \
                     the refusal rather than discarded with it"
                );
            }

            // The dependency recovers, so the retry finally brings a writer up.
            program_loadable(&db, true);
            sm.update(
                refused_poll(),
                job_status(current.restart_nonce),
                shutdown.guard(),
            )
            .await;
            assert!(
                !sm.done(),
                "{stop_mode:?}: once the program loads the job must finally be adopted — a \
                 stop nothing could execute is not a reason to stop trying to reach it"
            );

            let writes = drive_to_completion(&sm, &db).await;
            assert!(
                writes.iter().all(|(state, generation)| state != "Failing"
                    && state != "Failed"
                    && *generation == 1),
                "{stop_mode:?}: the row asked for a stop, so the job stops. Failing it would \
                 destroy exactly the final checkpoint the stop exists for, and rescheduling \
                 it would run the refused configuration; wrote {writes:?}"
            );
            assert!(
                writes.ends_with(&[("Stopping".to_string(), 1), ("Stopped".to_string(), 1)]),
                "{stop_mode:?}: and it ends stopped; wrote {writes:?}"
            );
            assert!(
                scheduler
                    .stopped
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|(job, generation)| job == "job_abc" && *generation == Some(1)),
                "{stop_mode:?}: every teardown is scoped to the generation the job already \
                 had — never `Scheduling`'s destructive `stop_workers(_, None, _)`; asked \
                 for {:?}",
                scheduler.stopped.lock().unwrap()
            );
            assert_eq!(
                scheduler.started.lock().unwrap().as_slice(),
                [],
                "{stop_mode:?}: and no replacement workers for a configuration that must be \
                 adopted nowhere"
            );
        }
    }

    /// D96 row 11 (R6): a job whose program will not load keeps its refusal across every
    /// retry.
    ///
    /// The finding is a cold adoption that orphaned itself. `start` cannot load the program,
    /// leaves the job with no state task, and explicitly promises to retry — but the refused
    /// row's branch recorded the refusal as delivered to a queue that did not exist, so every
    /// later poll short-circuited on "already sent". Program loading was never retried, the
    /// job was never adopted, and its workers kept running with nothing administering them
    /// even after the dependency recovered.
    ///
    /// Under D39a "delivered" is not a thing the poll can get wrong, because the poll does
    /// not deliver: the classification stands in the job's mailbox, at one version, until a
    /// writer reads it. So the retry is the ordinary one, the refusal is still there when it
    /// succeeds, and the job is failed by the refusal it was restarted to receive — having
    /// rescheduled nothing on the way.
    ///
    /// `StateMachine::new` is deliberately not the entry point here: it is the one
    /// production site that chooses a lifecycle mechanism, and it chooses
    /// `LifecycleMode::SELECTED`. The retry loop this row is about is `update`'s, which is
    /// where the poll reaches a job it has already picked up.
    #[tokio::test]
    async fn program_load_failure_retries_keeping_refusal() {
        let db = sqlite_startable_job("Running", 2);
        program_loadable(&db, false);
        let shutdown = LiveShutdown::new();
        let scheduler = Arc::new(RecordingScheduler::default());

        let current = running_config(StateBackendSelector::Parquet);
        let mut sm = state_machine_in_mode(
            LifecycleMode::FencedV2,
            current.clone(),
            StateBackendSelector::Parquet,
            None,
            db.clone(),
            scheduler.clone(),
        );
        let mailbox = mailbox_of(&sm);

        // The shape the stop branch never sees: refused, and asking for no stop.
        let refused = running_config(StateBackendSelector::Parquet);
        assert_eq!(refused.stop_mode, StopMode::none);
        let refused_poll = || {
            polled(
                StateBackendSelector::Parquet,
                refused.clone(),
                Some(selector_changed()),
            )
        };

        for poll in 0..3 {
            sm.update(
                refused_poll(),
                job_status(current.restart_nonce),
                shutdown.guard(),
            )
            .await;

            assert!(
                sm.done(),
                "poll {poll}: the program still cannot be loaded, so there is no writer"
            );
            assert_eq!(
                recorded_status(&db).1,
                None,
                "poll {poll}: and no execution has begun, so none is recorded"
            );
            let standing = standing_intent(&mailbox).unwrap_or_else(|| {
                panic!(
                    "poll {poll}: the refusal must survive a job with no writer, which is \
                         the whole of the retry this row is about"
                )
            });
            assert_eq!(
                standing.version().as_u64(),
                1,
                "poll {poll}: the row has not changed, so nothing about the job's standing \
                 intent has either — this is the coalescing that makes a retry free"
            );
            assert_eq!(
                standing.into_intent(),
                LifecycleIntent::Refused(selector_changed()),
                "poll {poll}: and it is still the refusal, not something a failed start \
                 consumed"
            );
        }

        // The dependency recovers. The row is unchanged and still refused, so the only thing
        // that can adopt the job now is the retry `start` promised.
        program_loadable(&db, true);
        sm.update(
            refused_poll(),
            job_status(current.restart_nonce),
            shutdown.guard(),
        )
        .await;

        assert!(
            !sm.done(),
            "once the program loads, the still-live job must finally be adopted: a refusal \
             it cannot be told about is not a reason to stop trying to reach it"
        );
        assert_eq!(
            recorded_status(&db).1,
            Some("parquet".to_string()),
            "and the execution that has now begun is recorded under the job's own immutable \
             selector, never the refused row's"
        );

        let writes = drive_to_completion(&sm, &db).await;
        assert!(
            writes
                .iter()
                .all(|(state, generation)| state != "Scheduling" && *generation == 1),
            "the job is adopted so the refusal can be applied to it, not so the refused row \
             can reschedule it; wrote {writes:?}"
        );
        assert!(
            writes.ends_with(&[("Failing".to_string(), 1), ("Failed".to_string(), 1)]),
            "and the adoption ends in the failure the refusal asked for; wrote {writes:?}"
        );
        assert_eq!(
            recorded_failure(&db).as_deref(),
            Some("the job's persisted configuration was refused"),
            "failed by the refusal itself, through the job's single writer"
        );
        assert_eq!(
            scheduler.started.lock().unwrap().as_slice(),
            [],
            "no replacement workers for a configuration that must be adopted nowhere"
        );
    }

    // ---------------------------------------------------------------------------------------
    // M11.T25e — selector immutability and fail-closed classification (design M11.D39f).
    // ---------------------------------------------------------------------------------------

    /// D96 row 5 (R2): a configuration update carrying a different state backend is refused
    /// before it replaces any execution baseline and before any status write.
    ///
    /// The claim is an *ordering* one, and the reason it has to be is that the two possible
    /// orders are not equally repairable. A configuration adopted as the execution baseline
    /// and classified afterwards cannot be un-adopted: whatever re-read that baseline in
    /// between — a state that reschedules, a status write that persists the job's execution
    /// record — has already run under the value being refused. Refusing by *not mutating* is
    /// the only version of the rule that survives the fatal path it ends in.
    ///
    /// So the assertions below are about what did not move, not about an error coming back.
    /// After a poll carrying a different selector:
    ///
    /// * the state machine's shared configuration is exactly what it was, so nothing
    ///   downstream of it can read the refused row;
    /// * the job's durable status row is untouched and the trigger recorded **no status
    ///   write at all** — in particular the execution record was never re-stamped, which is
    ///   the write that would have made the refused value the job's authority;
    /// * nothing was published: not to the job's queue, and not to M11.T08's cross-task
    ///   refusal machinery, which is a different mechanism and not a second layer of this one;
    /// * and what stands is a *typed* [`StateBackendError::JobSelectorChanged`] naming both
    ///   backends, rather than a string an operator has to interpret.
    ///
    /// Then the job's single writer reads it and fails the job — still adopting nothing, so
    /// the refused row is a baseline nowhere, including in the task that was told about it.
    ///
    /// # Both shapes the boundary can be handed
    ///
    /// The loop runs the same row twice. Once as [`crate::classify_polled_row`] really hands
    /// it on — resolved against the job's execution record, so the configuration already
    /// carries the job's own selector and the refusal travels beside it — and once as if that
    /// resolution had not happened at all, with the row's own foreign selector still on the
    /// configuration and no refusal attached. The second shape is the defence in depth: the
    /// lifecycle boundary validates against the job's immutable
    /// [`StateMachine::execution_selector`] rather than trusting what it was handed, so the
    /// refusal does not depend on one earlier caller having got it right.
    ///
    /// # The control
    ///
    /// The final section polls the *identical* edit with the job's own selector on it. It is
    /// adopted, and the writer's configuration changes — which is what makes everything above
    /// a statement about the selector rather than about a harness in which nothing is ever
    /// adopted anyway.
    #[tokio::test]
    async fn selector_change_refused_before_execution_adoption() {
        // The rule itself, in the one place it now lives (M11.D39f). A job's selector is
        // fixed at its first execution: the recorded value wins, the row's differing value
        // becomes a typed refusal rather than a change, and an unrecognized persisted value
        // is never guessed at.
        let unknown = StateBackendError::UnknownValue {
            label: "job \"job_abc\" execution".to_string(),
            value: "rocksdb".to_string(),
        };
        for (recorded, requested, expected, why) in [
            (
                Ok(Some(StateBackendSelector::Parquet)),
                Ok(StateBackendSelector::StateEngine),
                SelectorClassification::Fixed {
                    execution_selector: StateBackendSelector::Parquet,
                    refusal: Some(selector_changed()),
                },
                "an execution on record is the job's authority, so the row's differing value \
                 is refused and the job goes on being administered under its own backend",
            ),
            (
                Ok(None),
                Ok(StateBackendSelector::StateEngine),
                SelectorClassification::Fixed {
                    execution_selector: StateBackendSelector::StateEngine,
                    refusal: None,
                },
                "and a job with no execution takes the row's value: starting is the one \
                 moment a job chooses its backend",
            ),
            (
                Err(unknown.clone()),
                Ok(StateBackendSelector::Parquet),
                SelectorClassification::Undecidable(UndecidableSelector::ExecutionRecord(
                    unknown.clone(),
                )),
                "a recorded value nobody recognizes leaves the controller unable to say what \
                 the job is running with, and picking one would pick it for a live execution",
            ),
            (
                Ok(None),
                Err(unknown.clone()),
                SelectorClassification::Undecidable(UndecidableSelector::FirstDeclaration(
                    unknown.clone(),
                )),
                "a declaration that cannot be interpreted is never downgraded to a default, \
                 so the job simply never starts",
            ),
            (
                Ok(Some(StateBackendSelector::Parquet)),
                Err(unknown.clone()),
                SelectorClassification::Fixed {
                    execution_selector: StateBackendSelector::Parquet,
                    refusal: Some(unknown.clone()),
                },
                "but an uninterpretable row on a job that *has* an execution is a refusal \
                 rather than a skip — there is a selector to go on running under",
            ),
        ] {
            assert_eq!(
                classify_selector("job_abc", recorded, requested),
                expected,
                "{why}"
            );
        }

        let current = running_config(StateBackendSelector::Parquet);

        for (shape, row_selector, refusal_from_the_poll) in [
            (
                "resolved by the poll",
                StateBackendSelector::Parquet,
                Some(selector_changed()),
            ),
            (
                "not resolved by the poll",
                StateBackendSelector::StateEngine,
                None,
            ),
        ] {
            // A real database, so a status write would leave a record. Nothing here should
            // produce one: the job already has a state task, so the poll's own supervision
            // has nothing to start, and under D39a the poll publishes nothing itself.
            let db = sqlite_startable_job("Running", 2);
            let (tx, mut rx) = channel(16);
            let mut sm = state_machine_in_mode(
                LifecycleMode::FencedV2,
                current.clone(),
                StateBackendSelector::Parquet,
                Some(tx),
                db.clone(),
                Arc::new(RecordingScheduler::default()),
            );
            let mailbox = mailbox_of(&sm);

            // The row an operator has edited. A real edit moves more than one column, so the
            // restart nonce and the checkpoint interval move too: adoption anywhere would be
            // visible, and "nothing was mutated" is then a claim with something to catch.
            let mut edited = running_config(row_selector);
            edited.restart_nonce = 99;
            edited.checkpoint_interval = Duration::from_secs(45);

            sm.update(
                polled(StateBackendSelector::Parquet, edited, refusal_from_the_poll),
                job_status(current.restart_nonce),
                &shutdown_guard(),
            )
            .await;

            assert_eq!(
                sm.config.read().unwrap().0,
                current,
                "{shape}: the execution baseline is untouched — not the selector, not the \
                 restart nonce, not the checkpoint interval. A refusal that had already \
                 replaced it could not take it back"
            );
            assert!(
                state_writes(&db).is_empty(),
                "{shape}: and no lifecycle status was written; wrote {:?}",
                state_writes(&db)
            );
            assert_eq!(
                recorded_status(&db),
                ("Running".to_string(), None),
                "{shape}: in particular the job's durable execution record was never \
                 re-stamped, which is the write that would have made the refused value the \
                 job's own authority"
            );
            assert!(
                rx.try_recv().is_err(),
                "{shape}: nothing was published to the job either — under D39a a decision \
                 belongs to the job's state task"
            );
            assert!(
                sm.refusal.is_none()
                    && sm.refusal_version.load(Ordering::SeqCst) == 0
                    && sm.refusal_gate.clone().take().is_none(),
                "{shape}: and M11.T08's cross-task machinery is untouched: the two \
                 mechanisms are alternatives, not layers"
            );
            assert_eq!(
                standing_intent(&mailbox).map(VersionedIntent::into_intent),
                Some(LifecycleIntent::Refused(selector_changed())),
                "{shape}: what stands is the typed refusal, naming the backend the job runs \
                 with and the one the row asked for — and nothing of the refused row travels \
                 with it"
            );

            // The job's single writer, at its first consumption point.
            let mut harness = Harness::new(current.restart_nonce).with_actor(&mailbox);
            let mut ctx = harness.ctx(current.clone(), StateBackendSelector::Parquet);
            let Err(refused_by_the_writer) =
                ctx.observe_lifecycle_intent(ConsumptionPoint::BeforeIrreversiblePhase)
            else {
                panic!("{shape}: the writer must fail a job whose configuration was refused")
            };
            assert_eq!(
                selector_error(&refused_by_the_writer),
                &selector_changed(),
                "{shape}: with the typed error the classification produced, carried through \
                 rather than rewritten"
            );
            assert_eq!(
                ctx.config, current,
                "{shape}: and the refused row is adopted nowhere, including by the writer \
                 that was told about it"
            );
        }

        // The control. The same edit, with the job's own selector on it, is adopted — so the
        // sections above are about the selector and not about a state machine that never
        // adopts anything.
        let db = sqlite_startable_job("Running", 2);
        let (tx, _rx) = channel(16);
        let mut sm = state_machine_in_mode(
            LifecycleMode::FencedV2,
            current.clone(),
            StateBackendSelector::Parquet,
            Some(tx),
            db.clone(),
            Arc::new(RecordingScheduler::default()),
        );
        let mailbox = mailbox_of(&sm);

        let mut unchanged_selector = running_config(StateBackendSelector::Parquet);
        unchanged_selector.restart_nonce = 99;
        unchanged_selector.checkpoint_interval = Duration::from_secs(45);

        sm.update(
            polled(
                StateBackendSelector::Parquet,
                unchanged_selector.clone(),
                None,
            ),
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;

        assert_eq!(
            standing_intent(&mailbox).map(VersionedIntent::into_intent),
            Some(LifecycleIntent::Adopt(Box::new(unchanged_selector.clone()))),
            "an edit that leaves the selector alone is classified as an adoption"
        );

        let mut harness = Harness::new(current.restart_nonce).with_actor(&mailbox);
        let mut ctx = harness.ctx(current.clone(), StateBackendSelector::Parquet);
        assert_eq!(
            ctx.observe_lifecycle_intent(ConsumptionPoint::BeforeIrreversiblePhase)
                .expect("an unchanged selector is adopted, not refused"),
            ObservedIntent::Adopted(Box::new(current.clone())),
            "an edit that only changes the restart nonce and the checkpoint interval is not a \
             stop — it is an adoption, reported with the configuration it replaced so that a \
             running state can tell a restart from an in-place update"
        );
        assert_eq!(
            ctx.config, unchanged_selector,
            "and the writer adopts it, at a point strictly after the classification that let \
             it through: the same edit, differing only in the selector, does move the job"
        );
    }
    // ---------------------------------------------------------------------------------------
    // M11.T25b — the D39b phase graph, run against the same workers the landed body is run
    // against. The rows that need no job at all — the issued-attempt inventory, the fence
    // target set, the transfer interface, and the two source pins over `Fencing` — live beside
    // their modules in `states/scheduling/phase_tests.rs`, and the two compile restrictions are
    // in `states/scheduling/compile_fail.rs`.
    // ---------------------------------------------------------------------------------------

    /// A job whose lifecycle is the D39a single writer schedules through the phase graph, and
    /// reaches the same place, by the same effects, as the landed body.
    ///
    /// The two halves run the identical fixture — one restored checkpoint in its committing
    /// phase, one worker, one operator — so every difference between them is the body that ran.
    /// What is compared is what the *worker* saw and what the job's status row records, because
    /// those are the things a job's owner can observe: the `StartExecution` it was sent and the
    /// selector stamped into it, the commits replayed into its sinks, and the generation
    /// persisted before any of it.
    #[tokio::test]
    async fn the_fenced_lifecycle_schedules_a_job_through_the_phase_graph() {
        async fn prime(run: &SchedulingRun) {
            let queue = run.harness.queue();
            queue
                .send(worker_connect_from(WorkerId(7), &run.address(0)))
                .await
                .unwrap();
            queue.send(task_started()).await.unwrap();
        }

        let mut legacy = SchedulingRun::new("phase-parity-legacy").await;
        prime(&legacy).await;
        let legacy_outcome = legacy.schedule().await;

        let mailbox = intent_mailbox();
        let mut fenced = SchedulingRun::new("phase-parity-fenced").await;
        fenced.harness.install_actor(&mailbox);
        prime(&fenced).await;
        let fenced_outcome = fenced.schedule_through_the_phase_graph().await;

        assert_eq!(
            advanced_to(&legacy_outcome),
            Some("Running"),
            "the control: the landed body schedules this fixture into a running execution"
        );
        assert_eq!(
            advanced_to(&fenced_outcome),
            Some("Running"),
            "and so does the phase graph"
        );
        assert_eq!(
            fenced.calls.started(),
            legacy.calls.started(),
            "the worker is sent the same `StartExecution`, stamped with the same selector"
        );
        assert_eq!(
            fenced.calls.committed(),
            legacy.calls.committed(),
            "and the restored checkpoint's commits are published exactly as before — the third \
             admitted region does the same externally visible thing"
        );
        assert_eq!(
            state_writes(&fenced.db),
            state_writes(&legacy.db),
            "and the job's status row records the same scheduling generation, written in the \
             same preamble"
        );
    }

    /// A decision the job's writer has already taken stops the phase graph before its preamble,
    /// and the attempt ends in token-free fencing.
    ///
    /// The intent is standing before the graph is entered, which is the case the first crossing
    /// exists for: `Preamble::enter` reads the job's writer and takes the admission, in that
    /// order, so a refusal that was decided while the job was elsewhere stops it before the
    /// generation is persisted rather than after. Nothing reaches a worker and nothing is
    /// written, which is the difference between refusing and failing partway.
    #[tokio::test]
    async fn the_phase_graph_fences_a_refused_job_before_its_preamble() {
        let mailbox = intent_mailbox();
        mailbox.submit(LifecycleIntent::Refused(selector_changed()));

        let mut run = SchedulingRun::new("phase-fencing").await;
        run.harness.install_actor(&mailbox);
        let outcome = run.schedule_through_the_phase_graph().await;

        let Err(err) = outcome else {
            panic!("a refused job must not be scheduled into an execution");
        };
        assert!(
            matches!(
                selector_error(&err),
                StateBackendError::JobSelectorChanged { .. }
            ),
            "and it fails for the refusal itself, reported out of token-free fencing: {err:?}"
        );
        assert_eq!(
            run.calls.started(),
            Vec::<String>::new(),
            "no worker is told to start executing a configuration the job's writer has refused"
        );
        assert_eq!(
            run.calls.committed(),
            Vec::<u64>::new(),
            "and nothing is committed either"
        );
        assert_eq!(
            state_writes(&run.db),
            Vec::<(String, u64)>::new(),
            "nor is a generation persisted: the crossing is before the preamble's first effect, \
             not after it"
        );
        assert_eq!(
            run.harness.scheduler.stopped.lock().unwrap().as_slice(),
            Vec::<(String, Option<u64>)>::new(),
            "and the cluster the job is running on is left alone"
        );
    }

    // ---------------------------------------------------------------------------------------
    // Review round 1 — a consumed stop leaves, and a submitted intent is observed.
    //
    // Two defects, one shape. Consuming an intent used to answer `Ok(())`, which every caller
    // reads as permission to continue: a stop the job's writer had decided on mutated
    // `ctx.config` and then let the phase graph walk into the `StartExecution` fan-out and the
    // publication of a restored checkpoint's commits, both irreversible. And submitting an
    // intent used to notify nothing at all, so a wait parked on the job's channel — which under
    // M11.D39a carries nothing when the poll decides something — only looked again when its
    // startup budget expired, ten minutes for workers and two for tasks.
    //
    // The rows below are end-to-end against the same real gRPC workers the landed body is run
    // against, because what has to be asserted is what a worker was *sent*.
    // ---------------------------------------------------------------------------------------

    /// The configuration a poll carries once an operator has asked the job to stop.
    fn config_requesting_a_stop() -> JobConfig {
        let mut config = running_config(StateBackendSelector::Parquet);
        config.stop_mode = StopMode::immediate;
        config
    }

    /// How long a row waits for a decision it expects to be acted on.
    ///
    /// A liveness bound, not a performance assertion, and deliberately far below the budgets
    /// that are the only other way out of these waits: ten minutes for worker startup and two
    /// for task startup. A row that fell back on one of those would not finish inside this.
    const DECIDED: Duration = Duration::from_secs(30);

    /// An adopted configuration that asks the job to stop leaves `Scheduling` at its first
    /// crossing, before the preamble's first effect.
    ///
    /// The shape this covers is the ordinary one: an operator stops a job whose configuration is
    /// perfectly valid, so the intent is an `Adopt` and the stop is a field of what it carries.
    /// Nothing in the decision itself says "stop" — only the configuration it publishes does —
    /// which is why the answer is read off the published configuration rather than off which
    /// arm of the writer produced it.
    ///
    /// Before the fix `LifecycleDecision::apply` wrote the stop into `ctx.config` and returned
    /// `Ok(())`, and the crossing took its admission and scheduled the job anyway.
    #[tokio::test]
    async fn an_adopted_configuration_that_asks_the_job_to_stop_leaves_scheduling() {
        let mailbox = intent_mailbox();
        mailbox.submit(LifecycleIntent::Adopt(Box::new(config_requesting_a_stop())));

        let mut run = SchedulingRun::new("stop-before-preamble").await;
        run.harness.install_actor(&mailbox);
        let outcome = run.schedule_through_the_phase_graph().await;

        assert_eq!(
            advanced_to(&outcome),
            Some("Stopping"),
            "a job whose writer has decided it stops leaves for the state that stops it, \
             through the same macro the landed body uses — not for `Running`, and not as an \
             error"
        );
        assert_eq!(
            run.calls.started(),
            Vec::<String>::new(),
            "and no worker is told to start executing a job that has been asked to stop"
        );
        assert_eq!(
            run.calls.committed(),
            Vec::<u64>::new(),
            "nor is the restored checkpoint's two-phase commit finished against its sinks"
        );
        assert_eq!(
            state_writes(&run.db),
            Vec::<(String, u64)>::new(),
            "and the crossing is before the preamble's first effect, so not even the \
             scheduling generation was advanced"
        );
        assert_eq!(
            run.harness.scheduler.started.lock().unwrap().as_slice(),
            Vec::<(String, u64)>::new(),
            "and no replacement cluster was started for a job that is stopping"
        );
    }

    /// A refused row that keeps its stop leaves `Scheduling` rather than starting an execution.
    ///
    /// The other shape a stop arrives in: the row was refused for its state backend, and the
    /// one thing that survives the refusal is the stop the same row asks for — because the
    /// refusal's own remedy is "stop this job and create a new one under the other backend".
    /// `StopUnderRunningConfig` writes only the stop mode, onto the configuration the job's
    /// workers and checkpoints were built from.
    ///
    /// Before the fix that write was the whole of it, and the job was scheduled onto workers
    /// under a configuration the controller had already decided to stop.
    #[tokio::test]
    async fn a_refused_row_keeping_its_stop_leaves_scheduling_rather_than_starting_an_execution() {
        let mailbox = intent_mailbox();
        mailbox.submit(LifecycleIntent::RefusedButStopping {
            error: selector_changed(),
            stop_mode: StopMode::immediate,
        });

        let mut run = SchedulingRun::new("refused-but-stopping").await;
        run.harness.install_actor(&mailbox);
        let outcome = run.schedule_through_the_phase_graph().await;

        assert_eq!(
            advanced_to(&outcome),
            Some("Stopping"),
            "the stop wins over the refusal, and winning means the job stops — in `Stopped`, \
             with the final-checkpoint semantics its stop mode asked for, rather than in \
             `Failed`"
        );
        assert_eq!(
            run.calls.started(),
            Vec::<String>::new(),
            "and nothing was started under the configuration the row was refused for"
        );
        assert_eq!(
            state_writes(&run.db),
            Vec::<(String, u64)>::new(),
            "nor was a generation persisted on the way to not starting it"
        );
    }

    /// A stop decided while the job waits for its workers leaves before the `StartExecution`
    /// fan-out.
    ///
    /// This is the wait half, and it needs both fixes to pass. The decision is submitted after
    /// the preamble has asked for the cluster — so the job is inside `AwaitingWorkers`, with no
    /// worker connect ever queued — and the only other way out of that wait is the ten-minute
    /// worker-startup budget. So the mailbox's wake is what ends the turn, the read at the top
    /// of the next turn is what sees the stop, and the transition it returns is what stops the
    /// loop from going on to admit the fan-out.
    ///
    /// The sleep decides only *which* of the two reads catches it, not the outcome: without the
    /// wake the wait never turns again, and without the transition the loop reads the stop,
    /// writes it into the job's configuration and waits for workers anyway.
    #[tokio::test]
    async fn a_stop_decided_while_the_job_waits_for_workers_leaves_before_the_fan_out() {
        let mailbox = intent_mailbox();
        let mut run = SchedulingRun::new("stop-in-worker-wait").await;
        run.harness.install_actor(&mailbox);

        let barriers = run.barriers.clone();
        let poll = async move {
            // The last effect of the destructive preamble: from here the job is about to be
            // waiting, and the sleep gives it time to actually park.
            barriers.workers_started.notified().await;
            tokio::time::sleep(Duration::from_millis(250)).await;
            mailbox.submit(LifecycleIntent::Adopt(Box::new(config_requesting_a_stop())));
        };
        let scheduling = tokio::time::timeout(DECIDED, run.schedule_through_the_phase_graph());
        let (outcome, ()) = tokio::join!(scheduling, poll);
        let outcome = outcome.expect(
            "the stop has to end the wait. The only other thing that would is the worker \
             startup budget, which is ten minutes",
        );

        assert_eq!(
            advanced_to(&outcome),
            Some("Stopping"),
            "the wait is left for the state that stops the job"
        );
        assert_eq!(
            run.calls.started(),
            Vec::<String>::new(),
            "and the fan-out immediately after the wait never ran, so no worker was sent a \
             `StartExecution` for a job the controller had decided to stop"
        );
        assert_eq!(
            run.calls.committed(),
            Vec::<u64>::new(),
            "and nothing was committed either"
        );
        assert_eq!(
            run.harness.scheduler.started.lock().unwrap().len(),
            1,
            "the control: the preamble did run, so this row really is about the wait after it \
             and not about a job that never got that far"
        );
    }

    /// A stop decided while the job waits for its tasks leaves before the restored checkpoint's
    /// commits are published.
    ///
    /// The third admitted region is the one that is visible outside the cluster: it finishes a
    /// two-phase commit against the job's sinks, and it cannot be withdrawn. The fixture's
    /// restored checkpoint died in its committing phase, so a run that reaches that region
    /// publishes — which is what `the_fenced_lifecycle_schedules_a_job_through_the_phase_graph`
    /// asserts happens on the ordinary path, and what must not happen here.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_stop_decided_while_the_job_waits_for_tasks_leaves_before_the_restored_commits() {
        let mailbox = intent_mailbox();
        let mut run = SchedulingRun::new("stop-in-task-wait").await;
        run.harness.install_actor(&mailbox);
        // Enough to finish the first wait and the fan-out, and deliberately no `TaskStarted`:
        // the second wait has nothing to end it but its own two-minute budget.
        run.harness
            .queue()
            .send(worker_connect_from(WorkerId(7), &run.address(0)))
            .await
            .unwrap();

        let barriers = run.barriers.clone();
        let poll = async move {
            // Raised inside the worker's own `StartExecution` handler, so the job is past the
            // fan-out and into the wait for its tasks.
            barriers.execution_started.notified().await;
            tokio::time::sleep(Duration::from_millis(250)).await;
            mailbox.submit(LifecycleIntent::Adopt(Box::new(config_requesting_a_stop())));
        };
        let scheduling = tokio::time::timeout(DECIDED, run.schedule_through_the_phase_graph());
        let (outcome, ()) = tokio::join!(scheduling, poll);
        let outcome = outcome.expect(
            "the stop has to end the wait. The only other thing that would is the task startup \
             budget, which is two minutes",
        );

        assert_eq!(
            advanced_to(&outcome),
            Some("Stopping"),
            "the wait is left for the state that stops the job"
        );
        assert_eq!(
            run.calls.committed(),
            Vec::<u64>::new(),
            "and the restored checkpoint's commits — externally visible, and unwithdrawable — \
             were not published for a job the controller had decided to stop"
        );
        assert_eq!(
            run.calls.started(),
            ["parquet".to_string()],
            "the control: the fan-out did run, so this row really is about the wait after it"
        );
    }

    /// A healthy running job observes a stop its writer decided on.
    ///
    /// The sharp half of the delivery finding. `Running` is where a job that is working spends
    /// its life, and under M11.D39a nothing is put in its channel when the poll decides
    /// something — so before the fix a stop or a refusal left for a job that never had another
    /// configuration update was observed at no point at all, for as long as the job kept
    /// running well.
    ///
    /// Driven through the real `Running::next`, with nothing sent to the job's channel: the
    /// mailbox is the only thing that could have carried this.
    #[tokio::test]
    async fn a_healthy_running_job_observes_a_stop_left_in_its_mailbox() {
        let mailbox = intent_mailbox();
        mailbox.submit(LifecycleIntent::Adopt(Box::new(config_requesting_a_stop())));

        let mut harness = Harness::new(3).with_actor(&mailbox);
        let mut ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );
        let outcome = tokio::time::timeout(DECIDED, Box::new(Running {}).next(&mut ctx))
            .await
            .expect("a running job has to read its mailbox; nothing else here ever would");

        assert_eq!(
            advanced_to(&outcome),
            Some("Stopping"),
            "and it acts on what it read, through the same macro its `ConfigUpdate` arm uses"
        );
        assert_eq!(
            ctx.config.stop_mode,
            StopMode::immediate,
            "the decision was published into the job's configuration by the one thing allowed \
             to publish it"
        );
    }

    /// Every long-running wait a job can sit in reads its lifecycle mailbox, and can be woken by
    /// it (M11.D39a's second consumption point).
    ///
    /// **A structural source pin, and the name says so.** The behavioural rows above drive the
    /// two waits inside `Scheduling` and the one inside `Running`; the leader-mode waits cannot
    /// be driven without a live worker leader to answer them, and a wait that quietly lost
    /// either half would go unnoticed exactly where it is least testable.
    ///
    /// **Its domain is walked, not listed — PR #160 review comment `5384225297`.** This row used
    /// to iterate seven `include_str!`s, and `leader_stopping.rs` was not one of them: the one
    /// wait in the module that watched neither the mailbox nor the job's channel was never
    /// exempted, it was simply never asked. A list of files is the same maintenance hazard as a
    /// list of states, one level up — the argument `every_state_in_the_module` already makes
    /// against a list of states. So every production file under `states/`, plus the module the
    /// leader-mode wait lives in, is read, and each one that contains a wait must fall into one
    /// of three buckets: it reads the writer itself, it delegates to a primitive that does, or
    /// it is named in `EXEMPT` with the reason. `EXEMPT` is asserted to be exactly the set that
    /// was used, so an exemption cannot go stale and a new file cannot land in it by accident.
    ///
    /// Its gap, stated: the buckets are per file, so the three exempt entries are held by the
    /// paired counts at the end rather than by this partition, and a new file added *under*
    /// `scheduling/` with an unobserved wait would be answered by those counts and not by the
    /// walk.
    ///
    /// The intended reading of a failure here is "this wait can no longer be interrupted", not
    /// "the test is stale".
    #[test]
    fn every_long_running_wait_reads_the_jobs_writer_and_can_be_woken_by_it() {
        /// What makes a file one this row has anything to say about.
        const WAITS: [&str; 4] = [
            "tokio::select!",
            "wait_for_state(",
            "wait_for_finish(",
            "handle_leader_stopping(",
        ];
        /// Reading the job's single writer on every turn, in the file's own body.
        const READS: &str = "ConsumptionPoint::InsideInterruptibleWait";
        /// And being woken by a submission rather than by whatever ended the previous turn.
        const WOKEN: &str = "wake.notified()";
        /// Handing the wait to a primitive that does both. `JobWait::recv` reads the writer
        /// before it parks and again on the turn a submission ends; `handle_leader_stopping`
        /// is the leader-mode wait three states share.
        const DELEGATES: [&str; 2] = ["wait_for_finish(", "handle_leader_stopping("];

        /// A wait held by something other than the markers above, and the argument for it.
        const EXEMPT: [(&str, &str); 3] = [
            (
                "lifecycle/waiting.rs",
                "is the primitive the delegating files reach: it is where reading the writer                  before parking is implemented, so requiring it to delegate to itself is                  circular",
            ),
            (
                "scheduling.rs",
                "the phase graph splits the two halves across its modules on purpose — the loop                  that reads lives with the typestate that can see it — and the paired counts                  below are what hold it",
            ),
            (
                "scheduling/admission/execution.rs",
                "the other half of that split: the `select!` that is woken lives with the                  context that owns the job's channel, and is held by the same paired counts",
            ),
        ];

        /// Everything in a file before its test module. The same rule
        /// `every_state_in_the_module` applies, kept separate so that neither row's domain
        /// moves when the other is edited.
        fn production_half(source: &str) -> &str {
            match source.find("\n#[cfg(test)]") {
                Some(at) => &source[..at],
                None => source,
            }
        }

        fn walk(dir: &std::path::Path, found: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(dir).expect("the states module's own source") {
                let path = entry.expect("a readable directory entry").path();
                if path.is_dir() {
                    walk(&path, found);
                    continue;
                }
                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .expect("a readable file name")
                    .to_string();
                // Production files only: a test module's own fixtures wait on things no job
                // ever sits in, and `production_half` cannot see a file that is test code all
                // the way down.
                if !name.ends_with(".rs") || name == "tests.rs" || name.ends_with("_tests.rs") {
                    continue;
                }
                found.push(path);
            }
        }

        let states = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/states");
        let mut sources = Vec::new();
        walk(&states, &mut sources);
        sources.push(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/job_controller/leader_manager.rs"),
        );
        sources.sort();

        let mut in_domain = 0;
        let mut exemptions_used = std::collections::BTreeSet::new();
        for path in &sources {
            let source = std::fs::read_to_string(path).expect("a readable source file");
            let source = production_half(&source);
            if !WAITS.iter().any(|wait| source.contains(wait)) {
                continue;
            }
            in_domain += 1;

            let name = path
                .strip_prefix(&states)
                .unwrap_or(path)
                .to_str()
                .expect("a printable path")
                .to_string();
            if source.contains(READS) && source.contains(WOKEN) {
                continue;
            }
            if DELEGATES.iter().any(|to| source.contains(to)) {
                continue;
            }
            let excused = EXEMPT.iter().find(|(file, _)| *file == name);
            assert!(
                excused.is_some(),
                "{name}: every turn of a long-running wait must read the job's single writer                  and be woken by a submission to it. Under M11.D39a nothing is sent to the                  job's channel when the poll decides something, so a wait that does neither                  does not learn — and a wait that only reads is only reached when something                  else ended the previous turn. Fix the wait, or name this file in `EXEMPT`                  with the reason it is held elsewhere"
            );
            exemptions_used.insert(name);
        }

        assert!(
            in_domain >= 15,
            "this row asked {in_domain} files, which is fewer than `states/` held when it was              written — the walk has stopped reading the source it quantifies over"
        );
        assert_eq!(
            exemptions_used,
            EXEMPT
                .iter()
                .map(|(file, _)| (*file).to_string())
                .collect::<std::collections::BTreeSet<_>>(),
            "`EXEMPT` must name exactly the files that needed excusing: an entry no file used              is a stale exemption waiting to excuse the next wait that lands on it"
        );

        // The phase graph splits the two halves across its modules on purpose: the loop that
        // reads lives with the typestate that can see it, and the `select!` that is woken lives
        // with the context that owns the job's channel. This is what the three `EXEMPT` entries
        // above are held by instead of the partition.
        //
        // **It is a rule per wait, not a total — PR #160 review comment `5384611151`.** It used
        // to assert that `execution.rs` contained `wake.notified()` exactly twice. That is a
        // statement about how many of the interruptible waits are interruptible, not about
        // whether all of them are: the module held a *third* wait, `await_worker_channels`, a
        // bare `for h in handles { h.await }` with no `select!` at all, and a count pinned at
        // two was perfectly consistent with it. Every wait is now asked individually, and the
        // count that remains is derived from the waits rather than written down beside them.
        let execution = include_str!("scheduling/admission/execution.rs");
        const WAIT: &str = "pub(crate) async fn await_";
        let mut waits = 0;
        let mut rest = execution;
        while let Some(at) = rest.find(WAIT) {
            let from = &rest[at..];
            let name: String = from[WAIT.len()..]
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            // A function body at the module's own indentation ends at the first `\n    }`;
            // everything nested inside it closes deeper than that.
            let end = from
                .find("\n    }")
                .expect("a wait that ends at the module's indentation");
            let body = &from[..end];
            assert!(
                body.contains("tokio::select!") && body.contains("wake.notified()"),
                "await_{name} waits without a `select!` that a submission can wake. Every wait \
                 in this module is a consumption point under M11.D39a; a wait that parks on \
                 what it was already waiting for cannot be told the job has been stopped"
            );
            waits += 1;
            rest = &from[end..];
        }
        assert!(
            waits >= 3,
            "this row asked {waits} of the phase graph's waits, fewer than the module held \
             when it was written"
        );
        // Both halves of the phase graph: the driver moved into a child of `phases` when
        // review comment `5384870087` took that file past the 500-line bar, and the loops this
        // counts moved with it. Counting only the parent would have silently reached zero.
        let driver_loops = include_str!("scheduling/phases.rs")
            .matches("awaiting.observe_intent()")
            .count()
            + include_str!("scheduling/phases/driver.rs")
                .matches("awaiting.observe_intent()")
                .count();
        assert_eq!(
            driver_loops, waits,
            "every wait in `execution.rs` must be driven by a loop in `phases.rs` that reads \
             the job's writer before it — the two halves of the split, tied to each other so \
             that adding one without the other fails here"
        );
    }

    // ---------------------------------------------------------------------------------------
    // M11.T25f — legacy-path parity. What these rows are for, and what they deliberately are
    // not.
    //
    // DoD M11.T25l is one claim in two halves: production selects `LegacyT08`, and the guards
    // M11.T08 landed under it are still there and still doing their work. The first half is
    // already pinned twice — `production_selects_only_the_legacy_t08_lifecycle` quantifies the
    // selection over the whole enum, and `no_production_path_selects_the_fenced_v2_lifecycle`
    // counts the production construction sites — and neither is restated here.
    //
    // What was left unpinned is the *middle* of the chain those two do not meet in: a mode is
    // selected, and then a job is built from it, and then a body is chosen for that job. M11.T25b
    // added the last link — `run_state_body`, a fork taken on every state of every job — and its
    // question is not "which mode?" but "does this context have a D39a writer?". The rows below
    // are about that: what a job built from the selection actually has, which body it therefore
    // runs, and that the landed guards are still present and still called on the way there.
    //
    // Deliberately not restated, because a pin that only repeats another adds a place to
    // update rather than a thing that can fail:
    //
    // * the admitted regions of `Scheduling::next` and its inventory of irreversible effects —
    //   `the_source_of_scheduling_next_keeps_every_irreversible_effect_inside_an_admitted_region`;
    // * `settle_under_admission` owning the `StartExecution` fan-out, structurally in that same
    //   pin and behaviourally in `dropping_the_scheduling_task_keeps_the_admission_until_its_blocked_request_settles`,
    //   `a_cancelled_fan_out_holds_its_admission_until_the_request_it_issued_settles` and
    //   `a_sibling_failure_cannot_release_the_admission_while_a_blocked_worker_is_unsettled`,
    //   all of which drive `Scheduling::next` itself;
    // * the `reconciles_start_execution` capability gate, in
    //   `a_worker_predating_the_reconciliation_contract_is_never_sent_a_start_execution` and
    //   `a_controller_replacement_leaves_no_legacy_start_execution_handler_behind_its_gate`,
    //   which drive the same body against a worker that never advertised it.
    // ---------------------------------------------------------------------------------------

    /// A job built the way production builds one runs the landed `Scheduling` body, and reaches
    /// the same place by the same effects (M11.T25f, DoD M11.T25l).
    ///
    /// The chain M11.T25 has to keep true has four links, and the two pins named above cover
    /// only the first two:
    ///
    /// 1. the production construction site passes `LifecycleMode::SELECTED`;
    /// 2. `SELECTED` is `LegacyT08`;
    /// 3. **a `JobLifecycle` built from `LegacyT08` has no mailbox and no actor**;
    /// 4. **a context with no actor runs `Scheduling::next`, not the phase graph.**
    ///
    /// Links 3 and 4 are what this asserts, and they are the ones M11.T25 introduced: before
    /// this todo there was no second body for a state to run and no seam to choose between
    /// them. Link 3 is checked on the real `JobLifecycle::for_mode` rather than on a literal,
    /// so a future arm that handed `LegacyT08` a mailbox "for symmetry" fails here; link 4 is
    /// checked by running the fixture through the seam with production's own lifecycle
    /// installed, which is the only thing `run_state_body` consults.
    ///
    /// The parity half is then the ordinary one: the same fixture — one restored checkpoint in
    /// its committing phase, one worker, one operator — run once through the seam and once by
    /// calling `Scheduling::next` directly, compared on what a job's owner can observe. The
    /// landed body is unchanged by M11.T25 down to its source text, so "the same body ran" and
    /// "nothing about the job's lifecycle changed" are the same statement.
    #[tokio::test]
    async fn the_production_route_runs_the_landed_scheduling_body() {
        // Link 3, on the production constructor itself.
        let job_id = Arc::new("job_abc".to_string());
        let production =
            JobLifecycle::for_mode(lifecycle::LifecycleMode::SELECTED, Arc::clone(&job_id));
        assert!(
            production.intents().is_none(),
            "a job on the selected mechanism has no intent mailbox, so the configuration poll \
             has nowhere to leave a decision and keeps deciding for itself exactly as M11.T08 \
             landed it"
        );
        assert!(
            production
                .actor(Arc::clone(&job_id), StateBackendSelector::Parquet)
                .is_none(),
            "and its state task has no single writer, which is the fact the seam reads: the \
             D39a machinery is absent from a production job rather than present and bypassed"
        );

        async fn prime(run: &SchedulingRun) {
            let queue = run.harness.queue();
            queue
                .send(worker_connect_from(WorkerId(7), &run.address(0)))
                .await
                .unwrap();
            queue.send(task_started()).await.unwrap();
        }

        // The control: the landed body, called the way every round-1-to-16 row calls it.
        let mut landed = SchedulingRun::new("route-parity-landed").await;
        prime(&landed).await;
        let landed_outcome = landed.schedule().await;

        // And link 4: the same fixture, entered through the seam `execute_state` enters, by a
        // job whose lifecycle came out of `LifecycleMode::SELECTED`.
        let mut production_route = SchedulingRun::new("route-parity-production").await;
        production_route.harness.install_production_lifecycle();
        prime(&production_route).await;
        let production_outcome = production_route
            .schedule_through_the_production_route()
            .await;

        assert_eq!(
            advanced_to(&landed_outcome),
            Some("Running"),
            "the control: `Scheduling::next` schedules this fixture into a running execution"
        );
        assert_eq!(
            advanced_to(&production_outcome),
            advanced_to(&landed_outcome),
            "and a job that reached the same body through the seam leaves `Scheduling` for the \
             same state"
        );
        assert_eq!(
            production_route.calls.started(),
            landed.calls.started(),
            "the worker is sent the same `StartExecution`, stamped with the same selector"
        );
        assert_eq!(
            production_route.calls.committed(),
            landed.calls.committed(),
            "the restored checkpoint's commits are published identically — the third \
             irreversible region does the same externally visible thing"
        );
        assert_eq!(
            state_writes(&production_route.db),
            state_writes(&landed.db),
            "and the job's status row records the same scheduling generation, written in the \
             same preamble"
        );
    }

    /// Every guard M11.T08 landed on the production route is still there, and still on it
    /// (M11.T25f, DoD M11.T25l).
    ///
    /// **A structural source pin, and the name says so.** Two things no behavioural row can
    /// notice:
    ///
    /// * **The gate's inventory, and that every member of it is still reached from production.**
    ///   `RefusalGate` is six methods that only make sense together — take the admission
    ///   without waiting, take it and read what was published, publish under it, withdraw,
    ///   apply once, and stop applying — so the set is pinned in the same idiom as
    ///   `the_source_of_fencing_exposes_no_admission_and_no_irreversible_effect`: adding or
    ///   removing one is a decision, and this is where it has to be made. The *caller* half is
    ///   the part no other mechanism covers at all: `#[warn(dead_code)]` is silenced by a test
    ///   that calls the method, and every one of these has such a test, so a guard that quietly
    ///   lost its production call site would compile with no warning and keep passing its own
    ///   unit row while guarding nothing. M11.T26 is the owner allowed to remove them, in the
    ///   change that activates the durable fence; the intended reading of a failure here is
    ///   "T26 has arrived", not "the test is stale".
    /// * **Where the body is chosen.** M11.T25b moved the state body's call site: `execute_state`
    ///   used to call `state.next(ctx)` and now calls `scheduling::run_state_body`. That fork is
    ///   the only new thing between a job and its landed behaviour, and it cannot be reached
    ///   behaviourally — the two bodies are built to be observably identical, which is what
    ///   `the_fenced_lifecycle_schedules_a_job_through_the_phase_graph` asserts, so no run can
    ///   tell you which one it took. What it is guarded by, and that the gate is still read
    ///   *before* it, are therefore pinned here. A seam installed ahead of the gate would be a
    ///   state body that runs before the refusal it is under is applied — the exact defect
    ///   rounds 7 to 9 kept finding, reintroduced one level up.
    #[test]
    fn the_production_route_retains_every_t08_guard() {
        /// Everything in a file before its test module, so a mention inside a test does not
        /// count as a production one.
        fn production_half(source: &'static str) -> &'static str {
            match source.find("\n#[cfg(test)]") {
                Some(at) => &source[..at],
                None => source,
            }
        }
        let states = production_half(include_str!("mod.rs"));

        // The gate quartet, plus the two per-task methods that make "applied once" true.
        let impl_at = states
            .find("impl RefusalGate {")
            .expect("the refusal gate's impl has been renamed");
        let body =
            &states[impl_at..impl_at + states[impl_at..].find("\n}\n").expect("unterminated impl")];
        let mut methods: Vec<&str> = body
            .match_indices("    fn ")
            .chain(body.match_indices("    async fn "))
            .map(|(i, m)| {
                let rest = &body[i + m.len()..];
                &rest[..rest.find('(').expect("a method has arguments")]
            })
            .collect();
        methods.sort_unstable();
        assert_eq!(
            methods,
            [
                "admit_publication",
                "admit_scheduling",
                "disarm",
                "publish",
                "take",
                "withdraw",
            ],
            "these six are the landed M11.T08 interlock, and M11.T25 keeps every one of them \
             selected. Removing one is M11.T26's to do, in the change that makes the durable \
             fence the mechanism instead"
        );

        // And none of them is merely compiled: each is still reached from a production path.
        for method in methods {
            let calls = states.matches(&format!("refusal_gate.{method}(")).count();
            assert!(
                calls > 0,
                "`RefusalGate::{method}` has no production caller left. A guard nothing calls \
                 guards nothing, and its removal would then look like tidying"
            );
        }

        // Where a state's body is chosen, and what stands before that choice.
        let execute = states
            .find("async fn execute_state<'a>(")
            .expect("`execute_state` has been renamed");
        let execute = &states[execute
            ..execute
                + states[execute..]
                    .find("\n}\n")
                    .expect("unterminated function")];
        let read_gate = execute
            .find("refusal_gate.take()")
            .expect("`execute_state` must read the gate on every state's behalf");
        let run_body = execute
            .find("scheduling::run_state_body(")
            .expect("`execute_state` runs a state's body through the M11.T25b seam");
        assert!(
            read_gate < run_body,
            "the gate is read before the body runs, not after it: a refusal the job is already \
             under has to stop `Compiling`, which never receives, and `Scheduling`, which \
             persists a generation and tears the job's cluster down before its first `recv`"
        );
        assert_eq!(
            execute.matches("run_state_body(").count(),
            1,
            "and there is one place a state body is entered from. A second would be a second \
             thing that could choose a body without reading the gate first"
        );

        // The seam itself: which job takes the new branch, and what every other job gets.
        //
        // Cut here rather than with `scheduling_body`, whose "first `\n    }\n`" rule ends a
        // function at the close of its first indented block — which for this one is the
        // `return` arm, leaving the fallthrough this row exists to read outside the slice.
        let scheduling = scheduling_source_without_comments();
        let seam_at = scheduling
            .find("pub(crate) async fn run_state_body(")
            .expect("`run_state_body` has been renamed");
        let seam = &scheduling[seam_at
            ..seam_at
                + scheduling[seam_at..]
                    .find("\n}\n")
                    .expect("unterminated function")];
        assert_eq!(
            seam.matches("phases::schedule(").count(),
            1,
            "the M11.D39b phase graph is entered from exactly one place"
        );
        assert!(
            seam.contains("ctx.runs_fenced_lifecycle()"),
            "and only for a job that has a D39a single writer, which is what \
             `the_production_route_runs_the_landed_scheduling_body` shows a production job \
             does not have"
        );
        assert!(
            seam.contains("state.next(ctx).await"),
            "every other state of every other job runs its own landed body, unchanged. If this \
             fell through to anything else, `LegacyT08` would no longer mean M11.T08's path"
        );
    }

    // ---------------------------------------------------------------------------------------
    // M11.T25f — the leader-mode tail of the M11.D39b phase graph.
    //
    // M11.T25b reproduced this tail from the landed body but disclosed it as uncovered: the
    // `SchedulingRun` fixture runs controller mode, because which mode a scheduling attempt is
    // in comes from the process-wide `config().job_controller` and every test in this binary
    // reads that same cell. The rows below cover it without touching the cell — see
    // `PhaseContext::run_as_leader_on` for why that matters at `--test-threads` 16 — by driving
    // the tail directly against a real worker leader on a real socket.
    // ---------------------------------------------------------------------------------------

    /// A worker leader, as far as [`LeaderManager`] can tell.
    ///
    /// `GetJobStatus` and `StopJob` are answered: connecting polls once, and that poll is the
    /// whole handshake; a state that is stopping the job sends the stop before it waits. The
    /// other two are the rest of the service and answer as a leader that has been asked
    /// something it has no business being asked at this point in a job's life.
    struct FakeLeader {
        job_id: String,
        generation: u64,
        /// What this leader reports it is running the job with, in the persisted spelling.
        state_backend: String,
        /// What it reports about the job itself. Default — `JOB_UNKNOWN`, no failure — for
        /// every row that only needs the handshake.
        job_status: LeaderJobStatus,
        /// One per status poll, so a row can say the handshake happened rather than assume it.
        polls: Arc<AtomicU64>,
        /// One entry per `StopJob`, carrying the mode it asked for — the leader-mode
        /// counterpart of `WorkerCalls::stop_execution`. What a state *sent* is the half a
        /// transition cannot show: an escalation that re-sent the weaker stop first, or sent
        /// the harder one twice, reaches the same next state as one that did neither.
        stops: Arc<Mutex<Vec<i32>>>,
    }

    #[tonic::async_trait]
    impl JobStatusGrpc for FakeLeader {
        async fn get_job_status(
            &self,
            _: tonic::Request<JobStatusReq>,
        ) -> Result<tonic::Response<JobStatusResp>, tonic::Status> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Ok(tonic::Response::new(JobStatusResp {
                job_id: self.job_id.clone(),
                generation: self.generation,
                job_status: Some(self.job_status.clone()),
                state_backend: self.state_backend.clone(),
            }))
        }

        async fn stop_job(
            &self,
            req: tonic::Request<StopJobReq>,
        ) -> Result<tonic::Response<StopJobResp>, tonic::Status> {
            self.stops.lock().unwrap().push(req.into_inner().stop_mode);
            Ok(tonic::Response::new(StopJobResp {}))
        }

        async fn get_job_checkpoints(
            &self,
            _: tonic::Request<GetJobCheckpointsReq>,
        ) -> Result<tonic::Response<GetJobCheckpointsResp>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "this leader is only ever asked for its status",
            ))
        }

        async fn get_checkpoint_details(
            &self,
            _: tonic::Request<GetCheckpointDetailsReq>,
        ) -> Result<tonic::Response<GetCheckpointDetailsResp>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "this leader is only ever asked for its status",
            ))
        }
    }

    /// Serves a [`FakeLeader`] on a loopback port; returns its poll counter and its address.
    async fn fake_leader(generation: u64, state_backend: &str) -> (Arc<AtomicU64>, String) {
        fake_leader_reporting(generation, state_backend, LeaderJobStatus::default()).await
    }

    /// The same, for a leader that has something to say about the job it is running.
    async fn fake_leader_reporting(
        generation: u64,
        state_backend: &str,
        job_status: LeaderJobStatus,
    ) -> (Arc<AtomicU64>, String) {
        let (polls, _stops, address) =
            fake_leader_recording(generation, state_backend, job_status).await;
        (polls, address)
    }

    /// The same leader, keeping the stops it was sent.
    async fn fake_leader_recording(
        generation: u64,
        state_backend: &str,
        job_status: LeaderJobStatus,
    ) -> (Arc<AtomicU64>, Arc<Mutex<Vec<i32>>>, String) {
        let polls = Arc::new(AtomicU64::new(0));
        let stops = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(JobStatusGrpcServer::new(FakeLeader {
                    job_id: "job_abc".to_string(),
                    generation,
                    state_backend: state_backend.to_string(),
                    job_status,
                    polls: polls.clone(),
                    stops: stops.clone(),
                }))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        (polls, stops, format!("http://{addr}"))
    }

    /// The leader-mode tail attaches to the job's leader and records where it is
    /// (M11.T25b's disclosed gap, closed).
    ///
    /// A job whose controller runs on a worker never gets a `JobController` of its own: the
    /// controller connects to the leader, writes down which worker it is and at which
    /// generation, and hands the job to `LeaderRunning`. That record is the only thing a
    /// restarted controller has to find the leader again with — `run_to_completion` reconnects
    /// from `status.state_context.leader` before the job's first state — so writing it, and
    /// writing it only once the leader has agreed about the backend, is the whole of this tail.
    ///
    /// Two of the three assertions are leader-specific by construction: `epochs` reads
    /// `ignore_state_before_epoch` as an epoch threshold in controller mode and must ignore it
    /// in leader mode, where the same column carries a generation number, and the same context
    /// is checked both ways here so the difference is the mode and nothing else.
    #[tokio::test]
    async fn the_phase_graph_hands_a_leader_mode_execution_to_its_worker_leader() {
        const GENERATION: u64 = 2;
        let (polls, address) = fake_leader(GENERATION, "parquet").await;

        // The column the two modes read differently, set to something a controller-mode
        // execution would have started from epoch 4 with.
        let mut config = running_config(StateBackendSelector::Parquet);
        config.ignore_state_before_epoch = Some(5);

        let mut harness = Harness::new(3);
        harness.status.generation = GENERATION;
        let mut ctx = harness.ctx(config, StateBackendSelector::Parquet);

        let control = PhaseContext::new(&mut ctx);
        assert!(
            !control.leader_mode(),
            "the control half of this row needs the process's own configuration to be the \
             ordinary controller-mode one, which is what `PhaseContext::new` reads"
        );
        assert_eq!(
            control.epochs(),
            (4, 4),
            "the control: in controller mode `ignore_state_before_epoch` is an epoch threshold"
        );
        drop(control);

        let mut phase = PhaseContext::new(&mut ctx);
        phase.run_as_leader_on(WorkerId(7), address.clone());
        assert!(phase.leader_mode());
        assert_eq!(
            phase.epochs(),
            (0, 0),
            "and in leader mode the same column is a generation number, which must not be read \
             as an epoch: a job whose controller runs on a worker would otherwise start from a \
             checkpoint epoch nobody asked for"
        );

        phase.prepare_handover().await;
        assert!(
            !phase.needs_restored_commits(),
            "a leader-mode generation registers its recovery checkpoint through the leader and \
             carries no commit replay of its own"
        );

        let transition = match phase.into_transition().await {
            Ok(transition) => transition,
            Err((_, reason)) => panic!("the leader answered, so the job must leave: {reason:?}"),
        };
        let Transition::Advance(holder) = transition else {
            panic!("a started execution advances rather than stopping");
        };
        assert!(
            format!("{:?}", holder.state).starts_with("LeaderRunning"),
            "the job is handed to the state that administers it through its leader, not to the \
             one that administers it directly — both of which report the name `Running` in the \
             status row, which is why this asks the type: {:?}",
            holder.state
        );

        assert!(
            polls.load(Ordering::SeqCst) >= 1,
            "and it was attached to only after the leader had answered once: `connect` polls \
             before it returns a manager, which is what validates the backend the leader says \
             it is running"
        );
        assert!(
            ctx.leader_manager.is_some(),
            "the manager is installed on the job's context, so the states after this one poll \
             the same leader rather than reconnecting to whatever the row now says"
        );
        assert!(
            ctx.job_controller.is_none(),
            "and no job controller is carried into the state that runs it: in leader mode the \
             leader owns the job's checkpointing, which is what the landed body says by \
             clearing the field in its own `Some((id, addr))` arm"
        );
        assert_eq!(
            ctx.status.tasks,
            Some(ctx.program.task_count() as i32),
            "the task count is recorded in both modes, exactly as the landed body records it \
             before its own `match leader_info`"
        );
        let leader = ctx
            .status
            .state_context
            .leader
            .as_ref()
            .expect("the durable record of where this job's leader is");
        assert_eq!(leader.worker_id, WorkerId(7));
        assert_eq!(leader.rpc_address, address);
        assert_eq!(
            leader.generation, GENERATION,
            "recorded at the generation this scheduling attempt started, which is what a \
             reconnecting controller checks the leader against"
        );
    }

    /// A leader that reports a different backend is not attached to, and the phase is handed
    /// back able to fence (M11.T25b's disclosed gap, second half).
    ///
    /// This is the failure arm of the same tail, and it is where the handback contract earns
    /// its keep: `into_transition` returns the `PhaseContext` *with* the error rather than
    /// dropping it, because a phase that cannot leave `Scheduling` still has to be able to
    /// enter token-free `Fencing`, and fencing needs the context the phase was holding.
    ///
    /// Nothing durable may be written on the way out. A `LeaderContext` recorded for a leader
    /// the controller refused to attach to would be read back on the next controller start as
    /// the place to reconnect to.
    #[tokio::test]
    async fn a_leader_that_disagrees_about_the_backend_is_not_attached_to() {
        let (polls, address) = fake_leader(2, "stateengine").await;

        let mut harness = Harness::new(3);
        harness.status.generation = 2;
        let mut ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );

        let mut phase = PhaseContext::new(&mut ctx);
        phase.run_as_leader_on(WorkerId(7), address);
        phase.prepare_handover().await;

        let Err((phase, reason)) = phase.into_transition().await else {
            panic!(
                "a controller administering this job under parquet must not attach to a leader \
                 running it under something else"
            );
        };
        assert!(
            matches!(reason, StateError::RetryableError { .. }),
            "the disagreement is reported as retryable: the row may be repaired, or this may be \
             a leader that is about to be replaced — {reason:?}"
        );
        assert!(
            polls.load(Ordering::SeqCst) >= 1,
            "and it is the leader's own answer that produced it, not a guess from persisted \
             state"
        );

        // The handback, used for the one thing it exists for.
        let Err(reported) = phase
            .into_fencing(reason, IssuedAttempts::default())
            .reconcile_and_report()
        else {
            panic!(
                "nothing asked this job to stop, so fencing ends it on the reason it was \
                 interrupted with rather than on a transition"
            )
        };
        assert!(
            matches!(reported, StateError::RetryableError { .. }),
            "the reason survives token-free fencing unchanged: fencing is where an interrupted \
             phase releases its authority, not where the reason is rewritten — {reported:?}"
        );

        assert!(
            ctx.status.state_context.leader.is_none(),
            "and nothing was recorded about a leader this controller never attached to: the \
             next controller to start reads this column to find the job's leader"
        );
        assert!(ctx.leader_manager.is_none());
    }
    // ---------------------------------------------------------------------------------------
    // Review round 2 — the three defects the M11.T25 substrate carried into review.
    //
    // None of these is reachable in production: `LifecycleMode::SELECTED` is `LegacyT08`, so no
    // job has a `PhaseContext` at all. They are rows about what M11.T26 would activate.
    // ---------------------------------------------------------------------------------------

    /// A fatal reason for fencing has been superseded once the job's writer adopts a newer
    /// configuration, and must not be what the job is failed for.
    ///
    /// The named contract rows `repaired_row_not_failed_by_stale_intent` and
    /// `stop_wins_over_refusal` (D96 rows 9 and 7) live in `lifecycle/tests.rs`, and both are
    /// about the *classifier* and the *actor*: which decision a polled row produces, and that a
    /// superseded one is never decided twice. Neither reaches the fencing path, which is where
    /// a scheduling attempt that has already been interrupted decides what to report — and that
    /// is where the standing reason was being kept regardless of what the writer had since
    /// said.
    ///
    /// A repaired row cannot undo the interruption, so the attempt still ends; what it removes
    /// is the job being *failed* for a configuration it no longer has. The attempt becomes
    /// retryable, and the next one runs under the repaired configuration.
    #[tokio::test]
    async fn a_repaired_configuration_stops_fencing_reporting_the_refusal_it_superseded() {
        // The control first, so the two halves differ by one thing: the newer intent.
        let mailbox = intent_mailbox();
        let mut harness = Harness::new(3).with_actor(&mailbox);
        let mut ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );
        let phase = PhaseContext::new(&mut ctx);
        let Err(reported) = phase
            .into_fencing(refusal_reason(), IssuedAttempts::default())
            .reconcile_and_report()
        else {
            panic!("a job nobody has asked to stop does not end by stopping")
        };
        assert!(
            matches!(reported, StateError::FatalError { .. }),
            "the control: with nothing newer said, the refusal this attempt was interrupted by \
             is what it reports — {reported:?}"
        );

        // And now the same interruption, with the operator having repaired the row while the
        // attempt was fencing.
        let mailbox = intent_mailbox();
        let mut harness = Harness::new(3).with_actor(&mailbox);
        let mut ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );
        let phase = PhaseContext::new(&mut ctx);
        let mut repaired = running_config(StateBackendSelector::Parquet);
        repaired.restart_nonce = 4;
        mailbox.submit(LifecycleIntent::Adopt(Box::new(repaired)));

        let Err(reported) = phase
            .into_fencing(refusal_reason(), IssuedAttempts::default())
            .reconcile_and_report()
        else {
            panic!("an adoption that does not ask the job to stop is not a stop")
        };
        assert!(
            matches!(reported, StateError::RetryableError { .. }),
            "a job may not be failed for a configuration its own writer has since replaced: the \
             attempt ends, and the next one runs under the repaired row — {reported:?}"
        );
        assert_eq!(
            ctx.config.restart_nonce, 4,
            "and the newer configuration was actually published, which is what makes the \
             standing reason stale rather than merely old"
        );
    }

    /// A fatal reason that is *not* the job's configuration being refused survives a newer
    /// configuration (review round 3, finding 1).
    ///
    /// The defect: `Fencing::supersede` rewrote every `FatalError` the moment the job's writer
    /// adopted anything, reading only "the writer decided something" and never why the standing
    /// reason was fatal. The reviewer's own example is the one used here — the fatal
    /// `prepare_recovery_checkpoint` raises when the manifest a job would restore from was
    /// written by another state backend. That is a fact about what is on disk. It is still true
    /// after any number of adoptions, and turning it into ten retries both hides a permanent
    /// condition behind a retry budget and reports the wrong cause when the budget runs out.
    ///
    /// Both halves run against the same fixture and the same adopted configuration, so the only
    /// thing that differs between them is the provenance of the reason they start from.
    #[tokio::test]
    async fn a_newer_configuration_does_not_withdraw_a_fatal_reason_it_did_not_cause() {
        async fn fence_under_a_newer_configuration(standing: StateError) -> StateError {
            let mailbox = intent_mailbox();
            let mut harness = Harness::new(3).with_actor(&mailbox);
            let mut ctx = harness.ctx(
                running_config(StateBackendSelector::Parquet),
                StateBackendSelector::Parquet,
            );
            let phase = PhaseContext::new(&mut ctx);
            let mut repaired = running_config(StateBackendSelector::Parquet);
            repaired.restart_nonce = 9;
            mailbox.submit(LifecycleIntent::Adopt(Box::new(repaired)));

            let Err(reported) = phase
                .into_fencing(standing, IssuedAttempts::default())
                .reconcile_and_report()
            else {
                panic!("an adoption that does not ask the job to stop is not a stop")
            };
            assert_eq!(
                ctx.config.restart_nonce, 9,
                "the fixture's precondition: the newer configuration really was adopted, so \
                 both halves are asked the same question"
            );
            reported
        }

        // The half that must keep working: a refusal of the row the writer has just replaced.
        let reported = fence_under_a_newer_configuration(refusal_reason()).await;
        assert!(
            matches!(reported, StateError::RetryableError { .. }),
            "the control, and D96 row 9: a job may not be failed for a configuration its own \
             writer has since replaced — {reported:?}"
        );

        // The half this row exists for.
        let reported = fence_under_a_newer_configuration(recovery_backend_mismatch_reason()).await;
        let StateError::FatalError {
            message,
            provenance,
            ..
        } = &reported
        else {
            panic!(
                "a checkpoint written by another backend is not a row an operator can repair, \
                 and the job may not be told to retry ten times for it: {reported:?}"
            )
        };
        assert_eq!(
            (message.as_str(), *provenance),
            (
                "cannot restore a checkpoint written with a different state backend",
                FatalProvenance::Unrelated
            ),
            "and it survives with its own message, so the operator is told what is actually \
             wrong rather than that some configuration was superseded"
        );
    }

    /// A retryable reason for fencing is left exactly as it was when a newer configuration is
    /// adopted.
    ///
    /// The other side of the rule, and the reason it is not "the newest decision always wins":
    /// an adoption cannot repair what already went wrong. The workers still failed to start, the
    /// job retries either way, and replacing the reason would cost the operator the message and
    /// the retry budget that say what happened.
    #[tokio::test]
    async fn a_newer_configuration_does_not_rewrite_a_retryable_reason_for_fencing() {
        let mailbox = intent_mailbox();
        let mut harness = Harness::new(3).with_actor(&mailbox);
        let mut ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );
        let standing = ctx_retryable(&ctx);
        let phase = PhaseContext::new(&mut ctx);
        mailbox.submit(LifecycleIntent::Adopt(Box::new(running_config(
            StateBackendSelector::Parquet,
        ))));

        let Err(reported) = phase
            .into_fencing(standing, IssuedAttempts::default())
            .reconcile_and_report()
        else {
            panic!("an adoption that does not ask the job to stop is not a stop")
        };
        let StateError::RetryableError {
            message, retries, ..
        } = &reported
        else {
            panic!("a retryable reason stays retryable: {reported:?}")
        };
        assert_eq!(
            (message.as_str(), *retries),
            ("failed to start the job's workers", 7),
            "with its own message and its own budget: the workers still failed to start, and a \
             newer configuration does not make that untrue"
        );
    }

    /// Attempts a settlement owner was offered and then lost are still outstanding (review
    /// round 3, finding 4).
    ///
    /// The accounting half of the transfer fix. A fencing job reports two numbers, and
    /// `is_settled` is what M11.T26 will read before it may publish `Refused`: an attempt an
    /// owner took has somebody waiting for its outcome and is deliberately not counted here, so
    /// an attempt an owner *lost* must be — otherwise a reconciliation answers "settled" for a
    /// job whose issued `StartExecution` requests nothing at all is accounting for.
    #[tokio::test]
    async fn attempts_an_owner_lost_keep_a_reconciliation_unsettled() {
        let mut harness = Harness::new(3);
        let mut ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );
        let standing = ctx_retryable(&ctx);
        let phase = PhaseContext::new(&mut ctx);
        let mut interrupted = phase.into_fencing(standing, IssuedAttempts::default());

        let fencing = interrupted.fencing_mut();
        assert!(
            fencing.reconcile().is_settled(),
            "the control: an interruption that issued nothing and reached no worker owes \
             nothing"
        );

        // What the phase graph records when `hand_over` reports an owner that dropped the
        // obligation — built through the same conversion the phase uses, so the two cannot
        // disagree about what an abandoned attempt is.
        let (_, lost) =
            super::scheduling::fanout::settlement::SettlementOutcome::Abandoned { outstanding: 2 }
                .into_fencing_record();
        fencing.note_handover(lost);

        let reconciliation = fencing.reconcile();
        assert_eq!(
            reconciliation.outstanding_attempts, 2,
            "the attempts the owner was handed and did not keep are back on this job's account, \
             because nobody else has them"
        );
        assert!(
            !reconciliation.is_settled(),
            "so the job is not settled, and M11.T26 may not publish `Refused` behind requests a \
             worker could still apply"
        );
    }

    /// A stop decided while the attempt is fencing ends it as a stop, not as the refusal the
    /// stop was answering.
    ///
    /// `stop_wins_over_refusal` at the far end of the mechanism. The classifier already answers
    /// "refused *and* stopping" as one case; what this covers is the attempt that had already
    /// been interrupted by the refusal before the stop was decided. Reporting the refusal there
    /// ends the job in `Failed`, which throws away the stop the operator asked for — and with a
    /// `checkpoint` or `graceful` stop, the final checkpoint that stop exists for.
    #[tokio::test]
    async fn a_stop_decided_while_fencing_ends_the_attempt_as_a_stop() {
        for stop_mode in [
            StopMode::checkpoint,
            StopMode::graceful,
            StopMode::immediate,
            StopMode::force,
        ] {
            let mailbox = intent_mailbox();
            let mut harness = Harness::new(3).with_actor(&mailbox);
            let mut ctx = harness.ctx(
                running_config(StateBackendSelector::Parquet),
                StateBackendSelector::Parquet,
            );
            let phase = PhaseContext::new(&mut ctx);
            // The operator's answer to the refusal: stop this job.
            mailbox.submit(LifecycleIntent::RefusedButStopping {
                error: selector_changed(),
                stop_mode,
            });

            let outcome = phase
                .into_fencing(refusal_reason(), IssuedAttempts::default())
                .reconcile_and_report();
            assert_eq!(
                advanced_to(&outcome),
                Some("Stopping"),
                "{stop_mode:?}: the job ends where a stop ends. Failing it for the refusal the \
                 stop was answering would end it in `Failed` and discard the final checkpoint"
            );
        }
    }

    /// The fatal refusal a fencing attempt was interrupted by, exactly as
    /// `LifecycleDecision::Refuse` builds one — including its provenance, which is the whole
    /// of what makes it withdrawable.
    fn refusal_reason() -> StateError {
        fatal_refused_config(
            "the job's persisted configuration was refused",
            selector_changed().into(),
        )
    }

    /// The fatal reason `PhaseContext::prepare_recovery_checkpoint` builds when the manifest a
    /// job would restore from was written by a different state backend.
    ///
    /// A *non*-configuration fatal, reachable from the same phase graph and through the same
    /// `into_fencing`, built here exactly as `scheduling/admission.rs` builds it — which
    /// `the_recovery_backend_mismatch_is_not_a_configuration_refusal` pins so this fixture
    /// cannot quietly stop matching the site it stands for.
    fn recovery_backend_mismatch_reason() -> StateError {
        fatal(
            "cannot restore a checkpoint written with a different state backend",
            selector_changed().into(),
        )
    }

    /// A retryable reason of the kind the preamble produces, built through the same helper the
    /// phases use so its shape cannot drift from theirs.
    fn ctx_retryable(ctx: &JobContext<'_>) -> StateError {
        ctx.retryable(
            Box::new(Scheduling {}),
            "failed to start the job's workers",
            anyhow::anyhow!("the scheduler refused"),
            7,
        )
    }

    /// A leader that reports the job failing answers the task-startup timeout, and the failure
    /// it reports is what the job leaves on.
    ///
    /// The landed body extracts `job_failure` and calls `handle_job_failure`, which reads the
    /// failure's error domain and its retry hint and decides between recovering the job and
    /// failing it outright. The phase graph reproduced the RPC and threw the answer away: it
    /// logged the state and returned a generic retryable timeout, on a budget of 3 rather than
    /// the 10 the landed body gives even the case where the payload is missing.
    ///
    /// Parity is asserted rather than described. Both halves of this row are driven against the
    /// same leader and the same payload, and the expected outcome is computed by calling the
    /// very function the landed body calls — so the two routes cannot come to disagree without
    /// this failing.
    #[tokio::test]
    async fn a_leader_reported_startup_failure_is_what_the_phase_graph_leaves_on() {
        // A recoverable failure: the landed body transitions the job to `Recovering`.
        let failure = controller_job_failure(
            "operator blew up on startup",
            arroyo_rpc::grpc::rpc::ErrorDomain::External,
            arroyo_rpc::grpc::rpc::RetryHint::WithBackoff,
        );
        let (polls, address) = fake_leader_reporting(4, "parquet", failing_status(&failure)).await;

        let mut harness = Harness::new(3);
        harness.status.generation = 4;
        let mut ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );

        // What the landed `Scheduling::next` does with this exact status, computed here rather
        // than asserted from memory.
        let legacy = ctx.handle_job_failure(Scheduling {}, failure.clone()).await;
        assert_eq!(
            advanced_to(&legacy),
            Some("Recovering"),
            "the fixture's precondition: a `WithBackoff` failure recovers the job"
        );

        let mut phase = PhaseContext::new(&mut ctx);
        phase.run_as_leader_on(WorkerId(7), address);
        let outcome = phase.task_startup_timeout().await;

        let Ok(super::scheduling::admission::PhaseWait::Leave(transition)) = outcome else {
            panic!(
                "a leader that says why the job never started is answering the timeout, and the \
                 answer is a transition the timeout itself cannot express"
            )
        };
        let Transition::Advance(holder) = transition else {
            panic!("a failing job advances to the state that recovers it")
        };
        assert_eq!(
            holder.state.name(),
            "Recovering",
            "the phase graph reaches the same state the landed body reaches for the same \
             leader status, because it calls the same `handle_job_failure`"
        );
        assert!(
            polls.load(Ordering::SeqCst) >= 1,
            "and it is the leader's own answer that produced it"
        );
    }

    /// The same, for a failure the job may not be retried for.
    ///
    /// The half that shows the retry *hint* survives and not merely the transition: a
    /// `NoRetry` failure fails the job outright, with the domain the leader reported. The
    /// discarded-answer path could only ever produce a retryable internal error, so the two
    /// differ in the outcome as well as in the message.
    #[tokio::test]
    async fn a_leader_reported_unretryable_startup_failure_fails_the_job_in_its_own_domain() {
        let failure = controller_job_failure(
            "the pipeline's SQL cannot run",
            arroyo_rpc::grpc::rpc::ErrorDomain::User,
            arroyo_rpc::grpc::rpc::RetryHint::NoRetry,
        );
        let (_polls, address) = fake_leader_reporting(4, "parquet", failing_status(&failure)).await;

        let mut harness = Harness::new(3);
        harness.status.generation = 4;
        let mut ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );
        let mut phase = PhaseContext::new(&mut ctx);
        phase.run_as_leader_on(WorkerId(7), address);

        let Err(StateError::FatalError {
            domain, message, ..
        }) = phase.task_startup_timeout().await
        else {
            panic!("a failure the leader says must not be retried is not a retryable timeout")
        };
        assert_eq!(
            (domain, message.as_str()),
            (errors::ErrorDomain::User, "the pipeline's SQL cannot run"),
            "the failure's own domain and its own message reach the job's status row; a generic \
             timeout would have reported `internal` and hidden the cause"
        );
    }

    /// A leader that reports failing without saying why is retried on the landed budget.
    ///
    /// The distinction the phase graph had lost entirely: the landed body gives this case 10
    /// retries — the payload may simply not have been written yet — while an ordinary startup
    /// timeout gets 3. Merging them makes a job that failed on startup give up sooner than the
    /// landed path lets it.
    #[tokio::test]
    async fn a_leader_failing_without_a_payload_keeps_the_landed_retry_budget() {
        let mut status = LeaderJobStatus {
            job_state: JobState::JobFailed as i32,
            ..Default::default()
        };
        status.job_failure = None;
        let (_polls, address) = fake_leader_reporting(4, "parquet", status).await;

        let mut harness = Harness::new(3);
        harness.status.generation = 4;
        let mut ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );
        let mut phase = PhaseContext::new(&mut ctx);
        phase.run_as_leader_on(WorkerId(7), address);

        let Err(StateError::RetryableError {
            message, retries, ..
        }) = phase.task_startup_timeout().await
        else {
            panic!("a leader that says nothing useful leaves the attempt retryable")
        };
        assert_eq!(
            (message.as_str(), retries),
            ("leader reported failing status without failure payload", 10),
            "the landed body's own message and its own budget"
        );
    }

    /// A leader that is not failing leaves the timeout exactly as it was.
    ///
    /// The control for the three rows above, and the guarantee that nothing about a healthy —
    /// or merely slow — leader has changed: the generic startup timeout, on its budget of 3, is
    /// still what a job whose tasks did not report in gets.
    #[tokio::test]
    async fn a_leader_that_is_not_failing_still_reports_the_startup_timeout() {
        let (_polls, address) = fake_leader_reporting(
            4,
            "parquet",
            LeaderJobStatus {
                job_state: JobState::JobInitializing as i32,
                ..Default::default()
            },
        )
        .await;

        let mut harness = Harness::new(3);
        harness.status.generation = 4;
        let mut ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );
        let mut phase = PhaseContext::new(&mut ctx);
        phase.run_as_leader_on(WorkerId(7), address);

        let Err(StateError::RetryableError {
            message, retries, ..
        }) = phase.task_startup_timeout().await
        else {
            panic!("a leader with nothing wrong to report leaves the timeout as a timeout")
        };
        assert_eq!(
            (message.as_str(), retries),
            ("timed out while waiting for tasks to start", 3),
            "unchanged from the landed body, which reports the same thing for the same status"
        );
    }

    /// A leader status that reports the job failing for `failure`.
    fn failing_status(failure: &JobFailure) -> LeaderJobStatus {
        LeaderJobStatus {
            job_state: JobState::JobFailing as i32,
            job_failure: Some(failure.clone()),
            ..Default::default()
        }
    }

    // ------------------------------------------------------------------------------------
    // A stop decided *between* states — PR #160 review comment 5358055190.
    //
    // The state boundary is M11.D39a's first consumption point, and an intent is consumed
    // once. So a stop the writer decided while the previous state was running is taken at the
    // boundary and is no longer there for the state that runs next to observe: publishing it
    // into `ctx.config` and returning is not enough, because `Restarting` starts its final
    // checkpoint before its first look, `Rescaling` and `CheckpointStopping` look only after
    // theirs has begun, and `LeaderRescaling` and `LeaderCheckpointStopping` stop the leader
    // without ever looking. The boundary therefore hands the outcome to the state, through
    // `State::leave_for_stop`.
    // ------------------------------------------------------------------------------------

    /// Runs one state through the state boundary with a stop standing in the job's mailbox.
    ///
    /// The configuration the state is *handed* asks for no stop — only the mailbox carries
    /// one — which is exactly the shape of the finding: the stop arrives between two states,
    /// so nothing the state can read on its own account mentions it until the boundary
    /// publishes it.
    ///
    /// The context is given no `JobController` and no `LeaderManager`, and that is the second
    /// half of every row below. `Restarting`, `Rescaling` and `CheckpointStopping` dereference
    /// the first as their opening effect, and `LeaderRestarting`, `LeaderRescaling` and
    /// `LeaderCheckpointStopping` dereference the second: a state whose body ran at all would
    /// panic here rather than reach a transition, so "it transitioned" and "its body never
    /// started the effect" are the same assertion.
    async fn a_stop_arriving_between_states(
        state: Box<dyn State>,
        stop_mode: StopMode,
    ) -> Box<dyn State> {
        let mut stopped = running_config(StateBackendSelector::Parquet);
        stopped.stop_mode = stop_mode;
        let mailbox = intent_mailbox();
        mailbox.submit(LifecycleIntent::Adopt(Box::new(stopped)));

        let running = running_config(StateBackendSelector::Parquet);
        assert_eq!(
            running.stop_mode,
            StopMode::none,
            "the control on the fixture: the state is handed a configuration that asks for \
             nothing, so a state that leaves can only have been told to by the boundary"
        );
        let mut harness = Harness::new(running.restart_nonce)
            .with_db(sqlite_startable_job("Running", 2))
            .with_actor(&mailbox);
        let ctx = harness.ctx(running, StateBackendSelector::Parquet);

        let (next, _ctx) = execute_state(state, ctx).await;
        next.expect("a job that is stopping transitions; it does not end its own state machine")
    }

    /// The mechanism itself: the boundary *consumes* the intent it acts on, so the state that
    /// runs next cannot observe it for itself — and a state that answers "not here" therefore
    /// has the stop left standing for it.
    ///
    /// This is the row that would have found the previous finding, stated as the property
    /// rather than as any state's behaviour. Publication is not enough on its own, which is
    /// what the second assertion says.
    ///
    /// **Amended for PR #160 review comment `5362488017`.** Its third assertion used to read
    /// that the state's own consumption point reports `Continue` — that the boundary had taken
    /// the stop away from the state entirely. That is exactly what left `Finishing` and
    /// `LeaderFinishing` waiting out a stop already decided, so the property is now the
    /// stronger one: the *intent* is consumed once, and what a staying state's own consumption
    /// point reports is the standing stop, once. The first two assertions are unchanged.
    #[tokio::test]
    async fn the_state_boundary_consumes_the_stop_it_publishes() {
        let mut stopped = running_config(StateBackendSelector::Parquet);
        stopped.stop_mode = StopMode::immediate;
        let mailbox = intent_mailbox();
        mailbox.submit(LifecycleIntent::Adopt(Box::new(stopped)));

        let running = running_config(StateBackendSelector::Parquet);
        let mut harness = Harness::new(running.restart_nonce)
            .with_db(sqlite_startable_job("Running", 2))
            .with_actor(&mailbox);
        let ctx = harness.ctx(running, StateBackendSelector::Parquet);

        // `Created` is the one state whose body cannot mask the mechanism: it reads nothing,
        // starts nothing, and always advances to `Compiling`.
        let (next, mut ctx) = execute_state(Box::new(Created), ctx).await;

        assert_eq!(
            next.as_ref().map(|s| s.name()),
            Some("Compiling"),
            "`Created` stays, so the body ran — this row is about what the boundary left \
             behind, not about a state that left"
        );
        assert_eq!(
            ctx.config.stop_mode,
            StopMode::immediate,
            "the boundary published the stop into the job's configuration"
        );
        assert_eq!(
            ctx.observe_lifecycle_intent(ConsumptionPoint::InsideInterruptibleWait)
                .expect("an adopted configuration is not a refusal"),
            ObservedIntent::Stop,
            "`Created` answered `Stays`, which is `not here` and not `not at all`: the stop is \
             left standing on the job's writer, so the state's own consumption points are \
             offered it. Without this a body whose whole content is a wait waits the stop out"
        );
        assert_eq!(
            ctx.observe_lifecycle_intent(ConsumptionPoint::InsideInterruptibleWait)
                .expect("an adopted configuration is not a refusal"),
            ObservedIntent::Continue,
            "and offered it once. The intent itself was consumed at the boundary — the \
             writer's watermark moved past it — so this is the standing stop being spent, not \
             the mailbox being re-read"
        );
    }

    /// The other half: a state that *leaves* leaves nothing standing behind it.
    ///
    /// A stop answered by a transition is answered. If it also stayed standing, the state it
    /// transitioned into would be asked about a stop that had already been acted on — and for
    /// `Compiling`, whose answer is `Stopping`, a job would be routed to its stop state twice.
    #[tokio::test]
    async fn a_state_that_leaves_for_a_stop_leaves_none_standing() {
        let mut stopped = running_config(StateBackendSelector::Parquet);
        stopped.stop_mode = StopMode::immediate;
        let mailbox = intent_mailbox();
        mailbox.submit(LifecycleIntent::Adopt(Box::new(stopped)));

        let running = running_config(StateBackendSelector::Parquet);
        let mut harness = Harness::new(running.restart_nonce)
            .with_db(sqlite_startable_job("Running", 2))
            .with_actor(&mailbox);
        let ctx = harness.ctx(running, StateBackendSelector::Parquet);

        let (next, mut ctx) = execute_state(Box::new(Compiling), ctx).await;

        assert_eq!(
            next.as_ref().map(|s| s.name()),
            Some("Stopping"),
            "`Compiling` leaves, which is what `every_state_and_its_answer` records"
        );
        assert_eq!(
            ctx.observe_lifecycle_intent(ConsumptionPoint::InsideInterruptibleWait)
                .expect("an adopted configuration is not a refusal"),
            ObservedIntent::Continue,
            "and having left for it, there is nothing left standing: `Stopping` is not asked \
             again about the stop that created it"
        );
    }

    /// `Restarting` honours a stop that arrived between states instead of taking a final
    /// checkpoint and rescheduling the job.
    ///
    /// `RestartMode::safe` initiates the job's final checkpoint as its first statement, above
    /// the loop that holds its consumption point, and then ends in `Scheduling` — a
    /// replacement cluster for a job an operator has stopped.
    #[tokio::test]
    async fn restarting_honors_a_stop_that_arrived_between_states() {
        let left = a_stop_arriving_between_states(
            Box::new(Restarting {
                mode: RestartMode::safe,
            }),
            StopMode::immediate,
        )
        .await;

        assert_eq!(
            format!("{left:?}"),
            "Stopping { stop_mode: StopJob(Immediate) }",
            "the transition `stop_if_desired_non_running!` names for an immediate stop, \
             reached before `checkpoint(true)` — which, with no `JobController` in this \
             context, would have panicked"
        );
    }

    /// The same for `RestartMode::force`, whose body tears the cluster down and reschedules.
    #[tokio::test]
    async fn a_force_restart_honors_a_stop_that_arrived_between_states() {
        let left = a_stop_arriving_between_states(
            Box::new(Restarting {
                mode: RestartMode::force,
            }),
            StopMode::force,
        )
        .await;

        assert_eq!(
            format!("{left:?}"),
            "Stopping { stop_mode: StopWorkers }",
            "and a `force` stop reaches the workers directly, which is the same mapping the \
             landed macro makes for a job that is not running"
        );
    }

    /// `LeaderRestarting` honours a stop that arrived between states rather than sending the
    /// leader a checkpoint-stop and rescheduling.
    #[tokio::test]
    async fn leader_restarting_honors_a_stop_that_arrived_between_states() {
        let left = a_stop_arriving_between_states(
            Box::new(LeaderRestarting {
                mode: RestartMode::safe,
            }),
            StopMode::graceful,
        )
        .await;

        assert_eq!(
            format!("{left:?}"),
            "LeaderStopping { stop_behavior: StopJob(JobStopGraceful) }",
            "leader mode's own stop state, by leader mode's own mapping — reached before \
             `stop_leader(JobStopCheckpoint)`, which with no `LeaderManager` in this context \
             would have panicked"
        );
    }

    /// `Rescaling` honours a stop that arrived between states rather than taking a final
    /// checkpoint and scheduling a resized cluster.
    #[tokio::test]
    async fn rescaling_honors_a_stop_that_arrived_between_states() {
        let left =
            a_stop_arriving_between_states(Box::new(Rescaling {}), StopMode::checkpoint).await;

        assert_eq!(
            format!("{left:?}"),
            "Stopping { stop_mode: StopJob(Immediate) }",
            "a rescale that has not begun has nothing running to checkpoint, so every stop \
             mode ends it now — the landed `stop_if_desired_non_running!` mapping, unchanged"
        );
    }

    /// `LeaderRescaling` — the leader-mode twin of `Rescaling`, and a state with no
    /// consumption point of its own at all.
    ///
    /// It sends the leader a checkpoint-stop and waits inside `handle_leader_stopping`, so the
    /// state boundary is the only place it can learn that the job it is about to rescale has
    /// been stopped. Unlike its twin it can still honour the *mode*: nothing has been sent
    /// yet, so a `checkpoint` stop keeps its final checkpoint instead of being downgraded.
    #[tokio::test]
    async fn leader_rescaling_honors_a_stop_that_arrived_between_states() {
        let left =
            a_stop_arriving_between_states(Box::new(LeaderRescaling {}), StopMode::checkpoint)
                .await;

        assert_eq!(
            format!("{left:?}"),
            "LeaderCheckpointStopping",
            "the stop an operator asked for, not the rescale it cancelled"
        );
    }

    /// `CheckpointStopping` escalates a stop that arrived between states instead of going on
    /// taking a final checkpoint.
    ///
    /// The narrow case, and the one that costs an operator the most: the job is already
    /// stopping the careful way, and the only thing an `immediate` stop means is "stop waiting
    /// for that checkpoint". Its loop escalates only when its own consumption point reports a
    /// stop, and the boundary has already taken the one that matters.
    #[tokio::test]
    async fn checkpoint_stopping_escalates_a_stop_that_arrived_between_states() {
        let left =
            a_stop_arriving_between_states(Box::new(CheckpointStopping {}), StopMode::immediate)
                .await;

        assert_eq!(
            format!("{left:?}"),
            "Stopping { stop_mode: StopJob(Immediate) }",
            "the same escalation its message loop makes, at the one point the loop can no \
             longer make it"
        );
    }

    /// `LeaderCheckpointStopping` — the leader-mode twin, which sends its checkpoint-stop
    /// before it reaches the shared wait that holds its consumption point.
    #[tokio::test]
    async fn leader_checkpoint_stopping_escalates_a_stop_that_arrived_between_states() {
        let left = a_stop_arriving_between_states(
            Box::new(LeaderCheckpointStopping {}),
            StopMode::immediate,
        )
        .await;

        assert_eq!(
            format!("{left:?}"),
            "LeaderStopping { stop_behavior: StopJob(JobStopImmediate) }",
            "by `leader_stop_escalation`, which is the rule the wait below it applies — one \
             mapping, read at both points, rather than a second copy at this one"
        );
    }

    /// `LeaderStopping` leaves for a stop that arrived between states only when it goes
    /// further than the stop it is already making.
    ///
    /// The escalation row for the state the earlier rounds left alone, and its own control —
    /// PR #160 review comment `5384225297`. This state was recorded as staying because "this
    /// state is the stop", which is true of the stop it holds and false of a harder one an
    /// operator asked for afterwards. Leaving is what keeps `stop_leader(JobStopGraceful)`
    /// from being sent to a leader the operator has already given up on.
    ///
    /// The order matters in both directions, so the domain is every pair rather than the two
    /// cells that would have passed a weaker rule. A rule that left for *every* stop would
    /// answer the stop that created the state with a transition back to itself and send the
    /// same `stop_leader` again on each turn; a rule with no order would let a `graceful`
    /// arriving after an `immediate` walk the job back to the gentler stop already abandoned.
    /// `Stopped` is what a cell that stays reaches: with no `LeaderManager` in this context the
    /// body takes its worker-stopping arm, so "stayed" and "ran its body" are one answer.
    #[tokio::test]
    async fn leader_stopping_leaves_only_for_a_stop_that_goes_further_than_its_own() {
        const GRACEFUL: LeaderStopBehavior =
            LeaderStopBehavior::StopJob(JobStopMode::JobStopGraceful);
        const IMMEDIATE: LeaderStopBehavior =
            LeaderStopBehavior::StopJob(JobStopMode::JobStopImmediate);
        const WORKERS: LeaderStopBehavior = LeaderStopBehavior::StopWorkers;

        for (holding, asked, expected) in [
            // Strictly harder than the stop in flight: the escalation.
            (
                GRACEFUL,
                StopMode::immediate,
                "LeaderStopping { stop_behavior: StopJob(JobStopImmediate) }",
            ),
            (
                GRACEFUL,
                StopMode::force,
                "LeaderStopping { stop_behavior: StopWorkers }",
            ),
            (
                IMMEDIATE,
                StopMode::force,
                "LeaderStopping { stop_behavior: StopWorkers }",
            ),
            // The stop already in flight: answering it would send it a second time.
            (GRACEFUL, StopMode::graceful, "Stopped"),
            (IMMEDIATE, StopMode::immediate, "Stopped"),
            (WORKERS, StopMode::force, "Stopped"),
            // Weaker: an operator does not get a gentler ending by asking for one later.
            (IMMEDIATE, StopMode::graceful, "Stopped"),
            (WORKERS, StopMode::immediate, "Stopped"),
            (WORKERS, StopMode::graceful, "Stopped"),
            // Not a stop this family acts on: `leader_stop_escalation` answers `None` for
            // both, and a checkpoint stop is `LeaderCheckpointStopping`'s to make.
            (GRACEFUL, StopMode::checkpoint, "Stopped"),
            (GRACEFUL, StopMode::none, "Stopped"),
        ] {
            let left = a_stop_arriving_between_states(
                Box::new(LeaderStopping {
                    stop_behavior: holding,
                }),
                asked,
            )
            .await;

            assert_eq!(
                format!("{left:?}"),
                expected,
                "holding {holding:?} and asked for a {asked:?} stop"
            );
        }
    }

    /// A `CheckpointStopping` asked to stop the way it is already stopping stays and finishes
    /// its checkpoint.
    ///
    /// The control for the escalation rows, and the reason "leaves" is not the right answer
    /// for every state: turning a `checkpoint` stop into a transition here would throw away
    /// the final checkpoint the operator asked for.
    #[test]
    fn checkpoint_stopping_stays_for_the_stop_it_is_already_making() {
        for stop_mode in [StopMode::checkpoint, StopMode::graceful] {
            let mut config = running_config(StateBackendSelector::Parquet);
            config.stop_mode = stop_mode;
            let LeavingForStop::Stays(stayed) =
                Box::new(CheckpointStopping {}).leave_for_stop(&config)
            else {
                panic!("a {stop_mode:?} stop is what `CheckpointStopping` is already doing");
            };
            assert_eq!(stayed.name(), "CheckpointStopping");
        }
    }

    /// What this suite expects a state to do about a stop that was decided before it ran.
    #[derive(Debug)]
    enum ExpectedAnswer {
        /// The boundary hands the state this transition and the state's body never runs. The
        /// string is the whole `Debug` of the state transitioned to, not its name: `Stopping`
        /// and `LeaderStopping` both call themselves "Stopping", and the stop *behaviour* is
        /// the part an operator would notice being wrong.
        Leaves(&'static str),
        /// The state's body runs anyway. The reason is at the implementation site, and the
        /// sweep below records that one was given rather than that the question was skipped.
        Stays,
    }

    /// Every state in this module, and what each does about a stop it did not read itself.
    ///
    /// The reviewer named four states. The defect is not four states: it is any state that
    /// learns about a stop only from an observation the boundary has already consumed. So the
    /// domain here is *every* `impl State for` in `states/`, checked against the source by
    /// `every_state_answers_a_stop_that_was_decided_before_it_ran` rather than against a list
    /// someone has to remember to extend.
    fn every_state_and_its_answer() -> Vec<(&'static str, StopMode, Box<dyn State>, ExpectedAnswer)>
    {
        vec![
            // Nothing irreversible, and hands to a state that answers the same stop.
            (
                "Created",
                StopMode::immediate,
                Box::new(Created),
                ExpectedAnswer::Stays,
            ),
            // Hands to `Scheduling`, which persists a generation and starts a cluster.
            (
                "Compiling",
                StopMode::immediate,
                Box::new(Compiling),
                ExpectedAnswer::Leaves("Stopping { stop_mode: StopJob(Immediate) }"),
            ),
            (
                "Scheduling",
                StopMode::graceful,
                Box::new(Scheduling {}),
                ExpectedAnswer::Leaves("Stopping { stop_mode: StopJob(Immediate) }"),
            ),
            (
                "Running",
                StopMode::checkpoint,
                Box::new(Running {}),
                ExpectedAnswer::Leaves("CheckpointStopping"),
            ),
            (
                "LeaderRunning",
                StopMode::checkpoint,
                Box::new(LeaderRunning {
                    started: Instant::now(),
                }),
                ExpectedAnswer::Leaves("LeaderCheckpointStopping"),
            ),
            (
                "Restarting",
                StopMode::immediate,
                Box::new(Restarting {
                    mode: RestartMode::safe,
                }),
                ExpectedAnswer::Leaves("Stopping { stop_mode: StopJob(Immediate) }"),
            ),
            (
                "LeaderRestarting",
                StopMode::immediate,
                Box::new(LeaderRestarting {
                    mode: RestartMode::safe,
                }),
                ExpectedAnswer::Leaves(
                    "LeaderStopping { stop_behavior: StopJob(JobStopImmediate) }",
                ),
            ),
            (
                "Rescaling",
                StopMode::immediate,
                Box::new(Rescaling {}),
                ExpectedAnswer::Leaves("Stopping { stop_mode: StopJob(Immediate) }"),
            ),
            (
                "LeaderRescaling",
                StopMode::immediate,
                Box::new(LeaderRescaling {}),
                ExpectedAnswer::Leaves(
                    "LeaderStopping { stop_behavior: StopJob(JobStopImmediate) }",
                ),
            ),
            (
                "CheckpointStopping",
                StopMode::immediate,
                Box::new(CheckpointStopping {}),
                ExpectedAnswer::Leaves("Stopping { stop_mode: StopJob(Immediate) }"),
            ),
            (
                "LeaderCheckpointStopping",
                StopMode::immediate,
                Box::new(LeaderCheckpointStopping {}),
                ExpectedAnswer::Leaves(
                    "LeaderStopping { stop_behavior: StopJob(JobStopImmediate) }",
                ),
            ),
            // The states that are already the stop, or already ending, or already failing.
            // Each says why at its implementation site; the sweep records that it said so, and
            // `no_state_that_stays_waits_out_the_stop_it_was_handed` runs each of their bodies
            // under one.
            (
                "Stopping",
                StopMode::force,
                Box::new(Stopping {
                    stop_mode: StopBehavior::StopWorkers,
                }),
                ExpectedAnswer::Stays,
            ),
            (
                "LeaderStopping",
                StopMode::force,
                Box::new(LeaderStopping {
                    stop_behavior: LeaderStopBehavior::StopWorkers,
                }),
                ExpectedAnswer::Stays,
            ),
            // Was `Stays` until PR #160 review comment `5362488017`: its body is an unbounded
            // wait for workers that a stop is the operator's way of saying are not ending.
            (
                "Finishing",
                StopMode::immediate,
                Box::new(Finishing {}),
                ExpectedAnswer::Leaves("Stopping { stop_mode: StopJob(Immediate) }"),
            ),
            (
                "LeaderFinishing",
                StopMode::immediate,
                Box::new(LeaderFinishing {}),
                ExpectedAnswer::Stays,
            ),
            (
                "Failing",
                StopMode::immediate,
                Box::new(Failing {}),
                ExpectedAnswer::Stays,
            ),
            (
                "Recovering",
                StopMode::immediate,
                Box::new(Recovering {
                    source: anyhow::anyhow!("a worker died"),
                    reason: "a worker died".to_string(),
                    domain: errors::ErrorDomain::Internal,
                }),
                ExpectedAnswer::Stays,
            ),
            (
                "Failed",
                StopMode::immediate,
                Box::new(Failed),
                ExpectedAnswer::Stays,
            ),
            (
                "Finished",
                StopMode::immediate,
                Box::new(Finished),
                ExpectedAnswer::Stays,
            ),
            (
                "Stopped",
                StopMode::immediate,
                Box::new(Stopped {}),
                ExpectedAnswer::Stays,
            ),
        ]
    }

    /// Every state in `states/`, taken from the source tree rather than from a list.
    ///
    /// The directory is walked rather than a set of `include_str!`s enumerated, so that a state
    /// added in a *new file* is covered too: a list of files is the same maintenance hazard as
    /// a list of states, one level up.
    fn every_state_in_the_module() -> std::collections::BTreeSet<String> {
        /// Everything in a file before its test module, so a state that stands in for one in a
        /// test does not have to be answered for.
        fn production_half(source: &str) -> &str {
            match source.find("\n#[cfg(test)]") {
                Some(at) => &source[..at],
                None => source,
            }
        }

        fn walk(dir: &std::path::Path, found: &mut std::collections::BTreeSet<String>) {
            const MARKER: &str = "impl State for ";
            for entry in std::fs::read_dir(dir).expect("the states module's own source") {
                let path = entry.expect("a readable directory entry").path();
                if path.is_dir() {
                    walk(&path, found);
                    continue;
                }
                if path.extension().is_none_or(|e| e != "rs") {
                    continue;
                }
                let source = std::fs::read_to_string(&path).expect("a readable source file");
                let source = production_half(&source);
                found.extend(source.match_indices(MARKER).map(|(at, _)| {
                    source[at + MARKER.len()..]
                        .chars()
                        .take_while(|c| c.is_alphanumeric() || *c == '_')
                        .collect::<String>()
                }));
            }
        }

        let mut found = std::collections::BTreeSet::new();
        walk(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/states"),
            &mut found,
        );
        assert!(
            found.len() >= 20,
            "the walk found {} states, which is fewer than `states/` had when this was \
             written — it has stopped reading the source it quantifies over",
            found.len()
        );
        found
    }

    /// Every state answers a stop that was decided before it ran, and the answer is the one
    /// this suite records.
    ///
    /// The row the finding asked for, quantified over the states rather than over the four
    /// names the review listed. Each `Leaves` entry is driven through `execute_state`, so it
    /// proves the wiring — the boundary asks, and the answer is what the loop acts on — and
    /// not merely that a method exists. Each `Stays` entry is asserted at the method, because
    /// running those bodies needs a live `JobController` or `LeaderManager`; what makes them
    /// safe is written beside each implementation and summarised in the commit message.
    #[tokio::test]
    async fn every_state_answers_a_stop_that_was_decided_before_it_ran() {
        for (name, stop_mode, state, expected) in every_state_and_its_answer() {
            match expected {
                ExpectedAnswer::Leaves(next) => {
                    let left = a_stop_arriving_between_states(state, stop_mode).await;
                    assert_eq!(
                        format!("{left:?}"),
                        next,
                        "{name}: a stop consumed at the state boundary must reach this state, \
                         and it leaves for the transition its own family's macro names"
                    );
                }
                ExpectedAnswer::Stays => {
                    let mut config = running_config(StateBackendSelector::Parquet);
                    config.stop_mode = stop_mode;
                    let LeavingForStop::Stays(stayed) = state.leave_for_stop(&config) else {
                        panic!(
                            "{name}: this state is recorded as staying, and a state that \
                             leaves has changed what the job does about a stop"
                        );
                    };
                    assert!(
                        format!("{stayed:?}").starts_with(name),
                        "{name}: staying hands the same state back to be run"
                    );
                }
            }
        }

        let answered: std::collections::BTreeSet<String> = every_state_and_its_answer()
            .into_iter()
            .map(|(name, ..)| name.to_string())
            .collect();
        assert_eq!(
            answered,
            every_state_in_the_module(),
            "the domain this row quantifies over is every state in `states/`, read from the \
             source. A state added without an entry here is a state whose answer to a stop \
             nothing has looked at"
        );
    }

    /// The forcing function itself: `State::leave_for_stop` is declared without a body.
    ///
    /// A default would be an answer given on a state's behalf by something that cannot see
    /// what the state is about to do, and a state added later would inherit it in silence.
    /// Declaring it without one is what makes `every_state_and_its_answer` above possible to
    /// keep complete: the compiler refuses the state that has not answered.
    #[test]
    fn the_state_trait_admits_no_default_answer_to_a_stop() {
        assert!(
            include_str!("mod.rs").contains(
                "fn leave_for_stop(self: Box<Self>, config: &JobConfig) -> LeavingForStop;\n"
            ),
            "declared and not defined: a state that could inherit an answer is a state that \
             could forget to give one, which is the defect this closes"
        );
    }

    // ------------------------------------------------------------------------------------
    // A stop that only the job's mailbox carries, inside a state's own wait — PR #160 review
    // comment `5362488017`.
    //
    // The state boundary is M11.D39a's *first* consumption point, and `leave_for_stop` makes
    // it impossible for a state to skip. The second — "inside every interruptible wait" — had
    // no such forcing function: a wait was a `recv` on the job's message channel, and under
    // `FencedV2` nothing is ever sent to that channel for a lifecycle decision. `Finishing`
    // waits there with no deadline, so a job whose workers never finish on their own could not
    // be stopped at all; `LeaderFinishing`'s shared wait had a consumption point but read it
    // only after the boundary had taken the stop away from it.
    //
    // Every row below selects `FencedV2` by building its context with
    // `Harness::with_actor(&mailbox)`, which is what gives the context a `LifecycleActor` —
    // `runs_fenced_lifecycle()` is that actor's existence. Under `LegacyT08` there is no actor
    // and no mailbox, so a row that forgot would pass while proving nothing. The one row that
    // deliberately does not is the legacy control, which asserts the absence.
    // ------------------------------------------------------------------------------------

    /// How long a row waits before calling an unbounded wait unbounded.
    ///
    /// Every wait these rows drive is expected to end *because of the stop*, over loopback, in
    /// microseconds. Reaching this deadline is the failure and never the pass, which is why it
    /// is generous: no amount of slowness can turn the unfixed code's "never" into a pass, and
    /// a slow machine cannot turn a pass into a failure.
    const STOP_REACHES_THE_WAIT: Duration = Duration::from_secs(20);

    /// A live [`JobController`] whose single worker is a real server on a real socket.
    ///
    /// The rows here are about what a state's *body* does, and every body that waits for a
    /// job's workers dereferences this. `program` decides whether the job can finish on its
    /// own: [`one_operator_program_at`] declares a task that stays `Running` until something
    /// says otherwise, which is the job the finding is about.
    async fn controller_over_a_worker(
        program: LogicalProgram,
    ) -> (Arc<WorkerCalls>, JobController) {
        controller_over_a_worker_storing_checkpoints_in(program, unused_db()).await
    }

    /// The same, with the database its checkpoint metadata store writes to.
    ///
    /// [`controller_over_a_worker`] hands it one with no schema, which is enough for every row
    /// that never takes a checkpoint. A row that drives a state whose *first* statement is
    /// `checkpoint(true)` — `Restarting` in `RestartMode::safe`, `Rescaling` — needs the
    /// `checkpoints` table [`sqlite_startable_job`] creates, or the state retries on the
    /// missing table before it ever reaches its consumption point.
    async fn controller_over_a_worker_storing_checkpoints_in(
        program: LogicalProgram,
        db: DatabaseSource,
    ) -> (Arc<WorkerCalls>, JobController) {
        let calls = Arc::new(WorkerCalls::default());
        let address = fake_worker(
            calls.clone(),
            Arc::new(SchedulingBarriers::default()),
            StartsExecution::Accepting,
        )
        .await;
        let channel = tonic::transport::Endpoint::from_shared(address)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let controller = JobController::new(
            Arc::new(DbCheckpointMetadataStore {
                organization_id: "org".to_string(),
                job_id: Arc::new("job_abc".to_string()),
                db,
                state_backend: StateBackendSelector::Parquet,
            }),
            running_config(StateBackendSelector::Parquet),
            PipelineId("pipeline_1".to_string().into()),
            None,
            LEADER_GENERATION,
            Arc::new(program),
            1,
            1,
            HashMap::from([(WorkerId(7), worker_client(channel, WorkerId(7)))]),
            None,
            None,
        );
        (calls, controller)
    }

    /// The generation the fake leader and the fake controller both run the job at.
    const LEADER_GENERATION: u64 = 2;

    /// A live [`LeaderManager`] attached to a leader that reports `job_state` forever.
    ///
    /// `JobFinishing` is what makes the leader-mode wait unbounded in the same way the
    /// controller-mode one is: `wait_for_state(JobFinished)` polls a non-terminal state that
    /// never changes, so nothing but the job's writer can end the wait.
    async fn leader_manager_reporting(job_state: JobState) -> LeaderManager {
        let (_polls, address) = fake_leader_reporting(
            LEADER_GENERATION,
            "parquet",
            LeaderJobStatus {
                job_state: job_state as i32,
                ..Default::default()
            },
        )
        .await;
        LeaderManager::connect(
            JobId(Arc::new("job_abc".to_string())),
            PipelineId("pipeline_1".to_string().into()),
            LEADER_GENERATION,
            WorkerId(7),
            address,
            Some(Duration::from_secs(5)),
            StateBackendSelector::Parquet,
        )
        .await
        .expect("the fake leader agrees about the job, its generation and its backend")
    }

    /// The same leader, and the stops it was sent.
    async fn leader_manager_recording_stops(
        job_state: JobState,
    ) -> (Arc<Mutex<Vec<i32>>>, LeaderManager) {
        let (_polls, stops, address) = fake_leader_recording(
            LEADER_GENERATION,
            "parquet",
            LeaderJobStatus {
                job_state: job_state as i32,
                ..Default::default()
            },
        )
        .await;
        let manager = LeaderManager::connect(
            JobId(Arc::new("job_abc".to_string())),
            PipelineId("pipeline_1".to_string().into()),
            LEADER_GENERATION,
            WorkerId(7),
            address,
            Some(Duration::from_secs(5)),
            StateBackendSelector::Parquet,
        )
        .await
        .expect("the fake leader agrees about the job, its generation and its backend");
        (stops, manager)
    }

    /// The `TaskFinished` the single task of [`one_operator_program_at`] reports.
    fn task_finished() -> JobMessage {
        JobMessage::RunningMessage(RunningMessage::TaskFinished {
            worker_id: WorkerId(7),
            time: SystemTime::now(),
            task_id: 1,
            subtask_idx: 0,
        })
    }

    /// A job that is finishing is stopped by a stop nothing sent it a message about.
    ///
    /// The finding, driven end to end. The mailbox is empty when the wait starts and nothing is
    /// ever put on the job's channel, so the submission below is the only thing in the process
    /// that could end the wait — and before this change the wait was not watching it.
    ///
    /// It runs on to `Stopping`'s body so that "the job stops" is a `StopExecution` that
    /// arrived at a real worker and a job that reached `Stopped`, rather than a function that
    /// returned.
    #[tokio::test]
    async fn a_finishing_job_stops_when_the_stop_arrives_only_in_the_mailbox() {
        let mailbox = intent_mailbox();
        let (calls, job_controller) = controller_over_a_worker(one_operator_program_at(1)).await;

        let mut harness = Harness::new(3)
            .with_db(sqlite_startable_job("Running", 2))
            .with_program(one_operator_program_at(1))
            .with_job_controller(job_controller)
            .with_actor(&mailbox);
        let queue = harness.queue();
        let ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );

        let mut stopped = running_config(StateBackendSelector::Parquet);
        stopped.stop_mode = StopMode::immediate;
        let submit = async {
            // Long enough that the wait is parked. Not load-bearing: submitting earlier only
            // makes the wait read the stop before it parks, which is the same consumption
            // point on the same turn.
            tokio::time::sleep(Duration::from_millis(150)).await;
            mailbox.submit(LifecycleIntent::Adopt(Box::new(stopped)));
        };

        let (finishing, ()) = tokio::time::timeout(STOP_REACHES_THE_WAIT, async {
            tokio::join!(execute_state(Box::new(Finishing {}), ctx), submit)
        })
        .await
        .expect(
            "`Finishing`'s wait has no deadline, so reaching this one is the finding: the stop \
             was submitted to the job's writer and the wait was not watching it",
        );
        let (next, ctx) = finishing;

        let left = next.expect("a job that is stopping transitions; it does not end here");
        assert_eq!(
            format!("{left:?}"),
            "Stopping { stop_mode: StopJob(Immediate) }",
            "the transition `stop_if_desired_non_running!` names for an immediate stop, which \
             is the same mapping the state boundary above it answers with"
        );
        assert!(
            calls.stopped().is_empty(),
            "and the wait reported the decision rather than acting on it: what a stop means \
             differs by state, and `Stopping` is what asks the workers"
        );

        // The task the program declares finishes while `Stopping` waits, which is how a real
        // job reaches `Stopped`. Queued now rather than earlier so that `Finishing`'s own wait
        // cannot consume it and finish the job before the stop lands.
        queue.send(task_finished()).await.unwrap();

        let (stopped_next, _ctx) =
            tokio::time::timeout(STOP_REACHES_THE_WAIT, execute_state(left, ctx))
                .await
                .expect(
                    "`Stopping` waits for workers that have been told to stop and have finished",
                );

        assert_eq!(
            calls.stopped(),
            vec![rpc::StopMode::Immediate],
            "the job actually stops: one `StopExecution` arrived at the worker, asking for the \
             mode the operator asked for"
        );
        assert_eq!(
            format!(
                "{:?}",
                stopped_next.expect("a stopped job reaches its terminal state")
            ),
            "Stopped",
            "and the job ends where an operator who stopped it would look for it"
        );
    }

    /// The legacy control: a `ConfigUpdate` still stops a finishing job exactly as M11.T08
    /// landed it.
    ///
    /// No `with_actor`, so this context is `LifecycleMode::LegacyT08` — the selected production
    /// path — and the assertion below says so rather than assuming it. The outcome is the
    /// landed one and deliberately *not* the fenced one: the wait tells the workers to stop and
    /// goes on waiting, so the job ends in `Finished`.
    #[tokio::test]
    async fn a_config_update_still_stops_a_finishing_job_on_the_landed_path() {
        let (calls, job_controller) = controller_over_a_worker(one_operator_program_at(1)).await;

        let mut harness = Harness::new(3)
            .with_db(sqlite_startable_job("Running", 2))
            .with_program(one_operator_program_at(1))
            .with_job_controller(job_controller);
        let queue = harness.queue();
        let ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );
        assert!(
            !ctx.runs_fenced_lifecycle(),
            "the control on this row: no actor, which is what `LegacyT08` *is* — so the \
             mailbox path below is structurally absent and what runs is the landed one"
        );

        let mut stopping = running_config(StateBackendSelector::Parquet);
        stopping.stop_mode = StopMode::immediate;
        queue
            .send(JobMessage::ConfigUpdate(stopping))
            .await
            .unwrap();
        queue.send(task_finished()).await.unwrap();

        let (next, _ctx) = tokio::time::timeout(
            STOP_REACHES_THE_WAIT,
            execute_state(Box::new(Finishing {}), ctx),
        )
        .await
        .expect("the landed wait ends when its tasks report finished");

        assert_eq!(
            calls.stopped(),
            vec![rpc::StopMode::Immediate],
            "the landed `ConfigUpdate` arm, unchanged: an immediate stop tells the workers to \
             stop now"
        );
        assert_eq!(
            format!(
                "{:?}",
                next.expect("a finishing job that finished transitions")
            ),
            "Finished",
            "and unchanged in where it leaves the job: the legacy wait carries on after \
             stopping the workers, so the job ends `Finished` rather than `Stopping`"
        );
    }

    /// `LeaderFinishing` answers a stop that was taken at its own boundary.
    ///
    /// Its body is `handle_leader_stopping`, whose wait has read the job's writer on every turn
    /// since M11.T25a — but the boundary had already consumed the stop, so the first turn read
    /// nothing and every turn after it read nothing either. The wait is passed `None` for its
    /// timeout, so what that reached was a job an operator could not stop.
    #[tokio::test]
    async fn leader_finishing_answers_a_stop_taken_at_its_own_boundary() {
        let mut stopped = running_config(StateBackendSelector::Parquet);
        stopped.stop_mode = StopMode::immediate;
        let mailbox = intent_mailbox();
        mailbox.submit(LifecycleIntent::Adopt(Box::new(stopped)));

        let mut harness = Harness::new(3)
            .with_db(sqlite_startable_job("Running", 2))
            .with_leader_manager(leader_manager_reporting(JobState::JobFinishing).await)
            .with_actor(&mailbox);
        let ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );

        let (next, _ctx) = tokio::time::timeout(
            STOP_REACHES_THE_WAIT,
            execute_state(Box::new(LeaderFinishing {}), ctx),
        )
        .await
        .expect(
            "`handle_leader_stopping(.., None)` has no deadline and this leader reports \
             `JobFinishing` forever, so reaching this deadline is the finding",
        );

        assert_eq!(
            format!(
                "{:?}",
                next.expect("a job that is stopping transitions; it does not end here")
            ),
            "LeaderStopping { stop_behavior: StopJob(JobStopImmediate) }",
            "by `leader_stop_escalation`, on the first turn of the wait — the stop the \
             boundary took is standing, so the turn that reads the writer reads it"
        );
    }

    /// `Recovering` hands the stop it was given to the state that answers it, before anything
    /// irreversible.
    ///
    /// Its justification for staying is that what it hands to answers the same stop before
    /// `Scheduling` starts anything, and that was argued rather than executed. It is executed
    /// here, and the chain is one state longer than the doc used to claim: the cleanup's own
    /// wait is a consumption point, so it takes the standing stop — which is what ends that
    /// wait early — and `Compiling`, which writes nothing and starts nothing, passes the job to
    /// `Scheduling`, whose first statement reads `ctx.config.stop_mode`. The doc has been
    /// corrected to say so.
    #[tokio::test]
    async fn recovering_hands_a_stop_it_was_given_to_the_state_that_answers_it() {
        let mut stopped = running_config(StateBackendSelector::Parquet);
        stopped.stop_mode = StopMode::immediate;
        let mailbox = intent_mailbox();
        mailbox.submit(LifecycleIntent::Adopt(Box::new(stopped)));

        let (calls, job_controller) = controller_over_a_worker(one_operator_program_at(1)).await;
        let mut harness = Harness::new(3)
            .with_db(sqlite_startable_job("Running", 2))
            .with_program(one_operator_program_at(1))
            .with_job_controller(job_controller)
            .with_actor(&mailbox);
        let ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );

        let (next, ctx) = tokio::time::timeout(
            STOP_REACHES_THE_WAIT,
            execute_state(
                Box::new(Recovering {
                    source: anyhow::anyhow!("a worker died"),
                    reason: "a worker died".to_string(),
                    domain: errors::ErrorDomain::Internal,
                }),
                ctx,
            ),
        )
        .await
        .expect("the cleanup's wait for workers is bounded at five seconds even unfixed");

        let handed_to = next.expect("recovering hands the job on");
        assert_eq!(
            format!("{handed_to:?}"),
            "Compiling",
            "`Recovering` stays: everything it does is the tear-down a stop performs"
        );
        assert_eq!(
            calls.stopped(),
            vec![rpc::StopMode::Immediate],
            "and the tear-down actually stopped the job's workers"
        );
        assert_eq!(
            ctx.config.stop_mode,
            StopMode::immediate,
            "with the stop published into the job's configuration, where the states after it \
             read one"
        );

        let (compiled, ctx) =
            tokio::time::timeout(STOP_REACHES_THE_WAIT, execute_state(handed_to, ctx))
                .await
                .expect("`Compiling` waits on nothing");
        let scheduling = compiled.expect("compiling hands the job on");
        assert_eq!(
            format!("{scheduling:?}"),
            "Scheduling",
            "`Compiling` writes nothing and starts nothing, so passing the stop through it \
             costs the job one transition and nothing else"
        );

        let (after, _ctx) =
            tokio::time::timeout(STOP_REACHES_THE_WAIT, execute_state(scheduling, ctx))
                .await
                .expect(
                    "a scheduling attempt that reads a stop before its preamble waits on \
                         nothing",
                );
        assert_eq!(
            format!(
                "{:?}",
                after.expect("a job that is stopping transitions; it does not end here")
            ),
            "Stopping { stop_mode: StopJob(Immediate) }",
            "and the replacement cluster is never started: `phases::schedule` reads \
             `ctx.config.stop_mode` as its first statement, which is where a stop decided \
             while the job was recovering is answered"
        );
    }

    /// A leader-mode stop already in flight is overtaken by a harder one that arrives only in
    /// the job's mailbox.
    ///
    /// The finding driven end to end — PR #160 review comment `5384225297`, the wait half. The
    /// mailbox is empty when the state starts, so the boundary consumes nothing and the body
    /// runs; the leader reports `JobFinishing` for ever, so `wait_for_state(JobStopped)` never
    /// returns and the submission below is the only thing in the process that can end the wait.
    /// Before this change that wait watched neither the mailbox nor the job's channel, so the
    /// force stop sat behind `FINISH_TIMEOUT` — sixty seconds, three times this row's deadline.
    ///
    /// This is also the variant `no_state_that_stays_waits_out_the_stop_it_was_handed` cannot
    /// reach: that row runs one instance per state, and the instance recorded for this one is
    /// `StopWorkers`, whose body stops the workers and returns without waiting at all.
    #[tokio::test]
    async fn a_stopping_leader_is_overtaken_by_a_harder_stop_from_the_mailbox() {
        let mailbox = intent_mailbox();
        let (stops, leader_manager) = leader_manager_recording_stops(JobState::JobFinishing).await;

        let mut harness = Harness::new(3)
            .with_db(sqlite_startable_job("Running", 2))
            .with_program(one_operator_program_at(1))
            .with_leader_manager(leader_manager)
            .with_actor(&mailbox);
        let ctx = harness.ctx(
            running_config(StateBackendSelector::Parquet),
            StateBackendSelector::Parquet,
        );

        let mut forced = running_config(StateBackendSelector::Parquet);
        forced.stop_mode = StopMode::force;
        let submit = async {
            // Long enough that the wait is parked. Not load-bearing: submitting earlier only
            // makes the wait read the stop before it parks, which is the same consumption
            // point on the same turn.
            tokio::time::sleep(Duration::from_millis(150)).await;
            mailbox.submit(LifecycleIntent::Adopt(Box::new(forced)));
        };

        let (stopping, ()) = tokio::time::timeout(STOP_REACHES_THE_WAIT, async {
            tokio::join!(
                execute_state(
                    Box::new(LeaderStopping {
                        stop_behavior: LeaderStopBehavior::StopJob(JobStopMode::JobStopGraceful),
                    }),
                    ctx,
                ),
                submit
            )
        })
        .await
        .expect(
            "reaching this deadline is the finding: the escalation was submitted to the job's \
             writer, and a wait watching neither of the sources a stop arrives on holds the job \
             on its graceful stop until `FINISH_TIMEOUT`",
        );
        let (next, _ctx) = stopping;

        assert_eq!(
            format!(
                "{:?}",
                next.expect("a job that is stopping transitions; it does not end here")
            ),
            "LeaderStopping { stop_behavior: StopWorkers }",
            "the behaviour `leader_stop_escalation` names for a force stop — the same mapping \
             the state boundary answers with, read here at the wait's own consumption point"
        );
        assert_eq!(
            stops.lock().unwrap().as_slice(),
            [JobStopMode::JobStopGraceful as i32],
            "and the leader was sent the graceful stop once and nothing after it: an escalation \
             is a transition, and asking the workers directly is `StopWorkers`'s own body"
        );
    }

    /// No state that stays can wait out the stop it was handed.
    ///
    /// The quantified row, and the one that would have found this finding. Its domain is
    /// `every_state_and_its_answer`'s own `Stays` entries rather than the three states the
    /// review named — a hard-coded list of three is what let the previous change through, and
    /// the previous change's own report said as much: no staying state's *body* had ever been
    /// run under a stop, so the claim was argued and not tested.
    ///
    /// Each body runs through `execute_state`, so the stop arrives the way the finding
    /// describes: decided by the job's writer, consumed at the boundary, and never sent to the
    /// job's channel at all. Reaching the deadline is the failure this row exists to report.
    ///
    /// **Its own gap, and PR #160 review comment `5384225297` is what that cost.** The table
    /// holds one instance per state, so a state whose body branches on how it was constructed
    /// is asked about one branch. The instance recorded here for `LeaderStopping` is
    /// `StopWorkers`, whose body stops the workers and returns without waiting at all — so this
    /// row passed while the same state's other branch waited out every stop that was not a
    /// message. `a_stopping_leader_is_overtaken_by_a_harder_stop_from_the_mailbox` is that
    /// branch, driven separately because it needs a live leader to wait on.
    #[tokio::test]
    async fn no_state_that_stays_waits_out_the_stop_it_was_handed() {
        let mut ran = 0;
        for (name, stop_mode, state, expected) in every_state_and_its_answer() {
            let ExpectedAnswer::Stays = expected else {
                continue;
            };
            ran += 1;

            let mut stopped = running_config(StateBackendSelector::Parquet);
            stopped.stop_mode = stop_mode;
            let mailbox = intent_mailbox();
            mailbox.submit(LifecycleIntent::Adopt(Box::new(stopped)));

            let harness = Harness::new(3)
                .with_db(sqlite_startable_job("Running", 2))
                .with_program(one_operator_program_at(1))
                .with_actor(&mailbox);
            // A leader-mode body dereferences a leader manager and a controller-mode body a job
            // controller, and `Recovering::cleanup` calls a context holding both `unreachable!`.
            // The state's own name is the rule, so a state added later is placed by it rather
            // than by being remembered.
            let mut harness = if name.starts_with("Leader") {
                harness.with_leader_manager(leader_manager_reporting(JobState::JobFinishing).await)
            } else {
                harness.with_job_controller(
                    controller_over_a_worker(one_operator_program_at(1)).await.1,
                )
            };
            let ctx = harness.ctx(
                running_config(StateBackendSelector::Parquet),
                StateBackendSelector::Parquet,
            );

            tokio::time::timeout(STOP_REACHES_THE_WAIT, execute_state(state, ctx))
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "{name}: its body did not finish under a stop that only the job's \
                         mailbox carried. A state that stays is a state whose own body answers \
                         the stop, and a body that blocks on a source the job's writer never \
                         writes to answers nothing"
                    )
                });
        }

        assert!(
            ran >= 9,
            "this row ran {ran} of the states recorded as staying, which is fewer than \
             `every_state_and_its_answer` held when it was written — it has stopped \
             quantifying over the set it exists to quantify over"
        );
    }

    // ------------------------------------------------------------------------------------
    // A configuration a running job's own writer adopted — PR #160 review comment
    // `5365261487`.
    //
    // Under `FencedV2` a configuration change is not a message. The poll's whole contribution
    // is `IntentMailbox::submit`, and what reaches a running state is an adoption its own
    // writer published into `ctx.config`. That adoption used to report one bit — whether the
    // job stopped — so a `restart_nonce`, scheduler, environment or parallelism change
    // reached neither the restart classification nor the rescale comparison, and a
    // `checkpoint_interval` change never reached `JobController::update_config`: the
    // controller went on making progress under its own private copy of the configuration the
    // job had replaced.
    //
    // The rows below are parity rows rather than fenced-only rows, deliberately. The claim is
    // not "the adopted route does something" but "the two routes cannot come to mean
    // different things", so each change is delivered both ways and the answers are compared
    // against the same closed-form expectation. Every fenced row selects `FencedV2` by
    // building its context with `Harness::with_actor(&mailbox)` — the actor's existence *is*
    // `runs_fenced_lifecycle()` — and `running_given`/`leader_running_given` assert that
    // selection on every run rather than assuming it.
    // ------------------------------------------------------------------------------------

    /// How a configuration reaches a running job.
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    enum ConfigRoute {
        /// `JobMessage::ConfigUpdate` on the job's channel: the landed M11.T08 delivery, and
        /// the only one production uses.
        Message,
        /// An intent the job's own single writer adopted and published — reached only by a
        /// context built with `Harness::with_actor`.
        AdoptedIntent,
    }

    /// What a running job does about one configuration change.
    #[derive(Copy, Clone, Debug)]
    enum RunningConfigOutcome {
        /// It leaves `Running` for this state, named by the whole `Debug` of the state.
        Leaves(&'static str),
        /// It applies the change and carries on. The row then stops the job so its body ends,
        /// and asserts the state it leaves for — plus, in controller mode, that the live
        /// `JobController` is running under the new configuration.
        AppliedInPlace(&'static str),
    }

    /// Every dimension of a job's configuration a *running* job has to do something about,
    /// and what each of the two running modes does about it.
    ///
    /// This is the domain both routes are quantified over, and it is the whole of what the
    /// landed `JobMessage::ConfigUpdate` arms decide: the restart nonce and the restart mode
    /// it carries, the two fields that are only read when workers are scheduled, the
    /// parallelism override that is compared against a running operator, and a field that is
    /// simply applied. A change that is none of these is applied by the configuration having
    /// been replaced, which every state reads through `ctx.config`.
    fn every_running_config_change() -> Vec<(
        &'static str,
        fn(&mut JobConfig),
        RunningConfigOutcome,
        RunningConfigOutcome,
    )> {
        vec![
            (
                "a restart nonce bump",
                |c| c.restart_nonce = 4,
                RunningConfigOutcome::Leaves("Restarting { mode: safe }"),
                RunningConfigOutcome::Leaves("LeaderRestarting { mode: safe }"),
            ),
            (
                "a restart nonce bump that asks for a force restart",
                |c| {
                    c.restart_nonce = 4;
                    c.restart_mode = RestartMode::force;
                },
                RunningConfigOutcome::Leaves("Restarting { mode: force }"),
                RunningConfigOutcome::Leaves("LeaderRestarting { mode: force }"),
            ),
            (
                "a scheduler config change",
                |c| c.scheduler_config = serde_json::json!({ "slots": 4 }),
                RunningConfigOutcome::Leaves("Restarting { mode: safe }"),
                RunningConfigOutcome::Leaves("LeaderRestarting { mode: safe }"),
            ),
            (
                "an environment variable change",
                |c| c.env_vars = serde_json::json!({ "RUST_LOG": "debug" }),
                RunningConfigOutcome::Leaves("Restarting { mode: safe }"),
                RunningConfigOutcome::Leaves("LeaderRestarting { mode: safe }"),
            ),
            (
                "a parallelism override the running operator does not match",
                |c| c.parallelism_overrides = HashMap::from([(1, 2)]),
                RunningConfigOutcome::Leaves("Rescaling"),
                RunningConfigOutcome::Leaves("LeaderRescaling"),
            ),
            (
                "a checkpoint interval change",
                |c| c.checkpoint_interval = Duration::from_secs(45),
                RunningConfigOutcome::AppliedInPlace("Stopping { stop_mode: StopJob(Immediate) }"),
                RunningConfigOutcome::AppliedInPlace(
                    "LeaderStopping { stop_behavior: StopJob(JobStopImmediate) }",
                ),
            ),
        ]
    }

    /// The state a transition leaves the job in, by its whole `Debug`.
    ///
    /// Read off the transition rather than out of `execute_state`, so the job's controller is
    /// still in the context afterwards to be asked what configuration it is running under.
    fn left_for(transition: Transition) -> String {
        match transition {
            Transition::Advance(holder) => format!("{:?}", holder.state),
            Transition::Stop => "Stop".to_string(),
        }
    }

    /// The stop each row ends an in-place change with, so the state's body finishes.
    ///
    /// The same configuration the row just delivered, plus the stop, so that what ends the
    /// state is a stop and never a second unrelated change.
    fn stopping(updated: &JobConfig) -> JobConfig {
        let mut stopped = updated.clone();
        stopped.stop_mode = StopMode::immediate;
        stopped
    }

    /// Runs `Running`'s body under one configuration change delivered by one route, and
    /// returns the state it left for and what its job controller ended up running under.
    async fn running_given(
        route: ConfigRoute,
        change: fn(&mut JobConfig),
        stop_after: bool,
    ) -> (String, JobConfig) {
        let current = running_config(StateBackendSelector::Parquet);
        let mut updated = current.clone();
        change(&mut updated);

        let mailbox = intent_mailbox();
        let (_calls, job_controller) = controller_over_a_worker(one_operator_program_at(1)).await;
        let mut harness = Harness::new(current.restart_nonce)
            .with_db(sqlite_startable_job("Running", 2))
            .with_program(one_operator_program_at(1))
            .with_job_controller(job_controller);
        if route == ConfigRoute::AdoptedIntent {
            harness = harness.with_actor(&mailbox);
        }
        let queue = harness.queue();
        let mut ctx = harness.ctx(current, StateBackendSelector::Parquet);
        assert_eq!(
            ctx.runs_fenced_lifecycle(),
            route == ConfigRoute::AdoptedIntent,
            "{route:?}: the route is the context's lifecycle mechanism and nothing else. \
             Under `LegacyT08` there is no actor, so the adopted path is structurally absent \
             and a row that forgot `with_actor` would pass while proving nothing"
        );

        let stopped = stopping(&updated);
        match route {
            ConfigRoute::Message => {
                queue.send(JobMessage::ConfigUpdate(updated)).await.unwrap();
            }
            ConfigRoute::AdoptedIntent => {
                mailbox.submit(LifecycleIntent::Adopt(Box::new(updated)));
            }
        }

        // Nothing else is ever put on the job's channel for the adopted route: the mailbox
        // holds one intent, so the stop can only be submitted once the change has been
        // observed, which is the first turn of the loop.
        let deliver_stop = {
            let mailbox = Arc::clone(&mailbox);
            async move {
                if !stop_after {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
                match route {
                    ConfigRoute::Message => {
                        queue.send(JobMessage::ConfigUpdate(stopped)).await.unwrap();
                    }
                    ConfigRoute::AdoptedIntent => {
                        mailbox.submit(LifecycleIntent::Adopt(Box::new(stopped)));
                    }
                }
            }
        };

        let (transition, ()) = tokio::time::timeout(STOP_REACHES_THE_WAIT, async {
            tokio::join!(Box::new(Running {}).next(&mut ctx), deliver_stop)
        })
        .await
        .expect("`Running` answers a configuration change on the turn it reads it");

        let left = left_for(transition.expect("a configuration change is not a job failure"));
        let controller_config = ctx
            .job_controller
            .as_ref()
            .expect("the controller is still the context's; no transition was applied")
            .config()
            .clone();
        (left, controller_config)
    }

    /// A running job answers a configuration its writer adopted exactly as it answers one
    /// delivered as a message.
    ///
    /// The finding, quantified over every dimension a running job has to act on. Before this
    /// change the adopted column of the table read `Stopping`/never-returns for every row but
    /// the last, because the adopted route replaced `ctx.config` and reported `Continue`.
    #[tokio::test]
    async fn a_running_job_answers_an_adopted_configuration_as_it_answers_a_config_update() {
        for (name, change, outcome, _) in every_running_config_change() {
            let (expected, stop_after) = match outcome {
                RunningConfigOutcome::Leaves(state) => (state, false),
                RunningConfigOutcome::AppliedInPlace(state) => (state, true),
            };

            for route in [ConfigRoute::Message, ConfigRoute::AdoptedIntent] {
                let (left, controller_config) = running_given(route, change, stop_after).await;
                assert_eq!(
                    left, expected,
                    "{name}, delivered as {route:?}: both routes decide what a configuration \
                     means for a running job through `decide_running_config`, so both name \
                     the same state"
                );

                if let RunningConfigOutcome::AppliedInPlace(_) = outcome {
                    let mut applied = running_config(StateBackendSelector::Parquet);
                    change(&mut applied);
                    assert_eq!(
                        controller_config.checkpoint_interval, applied.checkpoint_interval,
                        "{name}, delivered as {route:?}: an in-place change is only applied \
                         once `JobController::update_config` has run. This asserts the \
                         controller's own configuration — the one `progress` reads its \
                         checkpoint interval out of — and not that a function returned"
                    );
                }
            }
        }
    }

    /// The same, in leader mode.
    async fn leader_running_given(
        route: ConfigRoute,
        change: fn(&mut JobConfig),
        stop_after: bool,
    ) -> String {
        let current = running_config(StateBackendSelector::Parquet);
        let mut updated = current.clone();
        change(&mut updated);

        let mailbox = intent_mailbox();
        let mut harness = Harness::new(current.restart_nonce)
            .with_db(sqlite_startable_job("Running", 2))
            .with_program(one_operator_program_at(1))
            .with_leader_manager(leader_manager_reporting(JobState::JobRunning).await);
        if route == ConfigRoute::AdoptedIntent {
            harness = harness.with_actor(&mailbox);
        }
        // The leader this job has, and the generation the job's status records, have to agree
        // or `LeaderRunning` fails the job before it reaches its loop.
        harness.status.generation = LEADER_GENERATION;
        let queue = harness.queue();
        let mut ctx = harness.ctx(current, StateBackendSelector::Parquet);
        assert_eq!(
            ctx.runs_fenced_lifecycle(),
            route == ConfigRoute::AdoptedIntent,
            "{route:?}: as in controller mode, the route is the context's lifecycle mechanism"
        );

        let stopped = stopping(&updated);
        match route {
            ConfigRoute::Message => {
                queue.send(JobMessage::ConfigUpdate(updated)).await.unwrap();
            }
            ConfigRoute::AdoptedIntent => {
                mailbox.submit(LifecycleIntent::Adopt(Box::new(updated)));
            }
        }

        let deliver_stop = {
            let mailbox = Arc::clone(&mailbox);
            async move {
                if !stop_after {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
                match route {
                    ConfigRoute::Message => {
                        queue.send(JobMessage::ConfigUpdate(stopped)).await.unwrap();
                    }
                    ConfigRoute::AdoptedIntent => {
                        mailbox.submit(LifecycleIntent::Adopt(Box::new(stopped)));
                    }
                }
            }
        };

        let (transition, ()) = tokio::time::timeout(STOP_REACHES_THE_WAIT, async {
            tokio::join!(
                Box::new(LeaderRunning {
                    started: Instant::now()
                })
                .next(&mut ctx),
                deliver_stop
            )
        })
        .await
        .expect("`LeaderRunning` answers a configuration change on the turn it reads it");

        left_for(transition.expect("a configuration change is not a job failure"))
    }

    /// A leader-mode running job answers an adopted configuration exactly as it answers a
    /// message.
    ///
    /// The sibling path. The finding named controller mode, but leader mode's
    /// `JobMessage::ConfigUpdate` arm makes the same two decisions from the same classifier,
    /// and its adopted route dropped them the same way.
    #[tokio::test]
    async fn a_leader_mode_job_answers_an_adopted_configuration_as_it_answers_a_config_update() {
        for (name, change, _, outcome) in every_running_config_change() {
            let (expected, stop_after) = match outcome {
                RunningConfigOutcome::Leaves(state) => (state, false),
                RunningConfigOutcome::AppliedInPlace(state) => (state, true),
            };

            for route in [ConfigRoute::Message, ConfigRoute::AdoptedIntent] {
                assert_eq!(
                    leader_running_given(route, change, stop_after).await,
                    expected,
                    "{name}, delivered as {route:?}: leader mode decides through the same \
                     `decide_running_config` as controller mode, and names its own states"
                );
            }
        }
    }

    /// A configuration adopted at the state boundary reaches the state that has to act on it.
    ///
    /// `execute_state` reads the job's writer before every state body, so an adoption made
    /// there is one the state's own consumption points can never report — the same shape as
    /// the stop of review comment `5362488017`, one decision along. Without
    /// `leave_adoption_standing` the restart below is never classified at all: `Running` starts
    /// with the new configuration already in `ctx.config` and nothing left to compare it with,
    /// and its workers go on running the one it replaced.
    #[tokio::test]
    async fn a_configuration_adopted_at_the_state_boundary_reaches_the_running_state() {
        let current = running_config(StateBackendSelector::Parquet);
        let mut restarted = current.clone();
        restarted.restart_nonce = 4;

        let mailbox = intent_mailbox();
        // Submitted before the state runs, so the boundary is what consumes it.
        mailbox.submit(LifecycleIntent::Adopt(Box::new(restarted)));

        let (_calls, job_controller) = controller_over_a_worker(one_operator_program_at(1)).await;
        let mut harness = Harness::new(current.restart_nonce)
            .with_db(sqlite_startable_job("Running", 2))
            .with_program(one_operator_program_at(1))
            .with_job_controller(job_controller)
            .with_actor(&mailbox);
        let mut ctx = harness.ctx(current, StateBackendSelector::Parquet);

        let observed = ctx
            .observe_lifecycle_intent(ConsumptionPoint::BeforeIrreversiblePhase)
            .expect("an accepted row is adopted, not refused");
        let ObservedIntent::Adopted(superseded) = observed else {
            panic!("the boundary reads an adoption, not {observed:?}");
        };
        assert_eq!(
            ctx.config.restart_nonce, 4,
            "the boundary publishes it: `ctx.config` is the adopted configuration from here on"
        );
        ctx.hand_adoption_to(superseded);

        let transition =
            tokio::time::timeout(STOP_REACHES_THE_WAIT, Box::new(Running {}).next(&mut ctx))
                .await
                .expect("`Running` reads its writer on the first turn of its loop")
                .expect("a restart nonce bump is not a job failure");

        assert_eq!(
            left_for(transition),
            "Restarting { mode: safe }",
            "the adoption the boundary took is offered again at the state's own consumption \
             point, so the restart is classified there — against the configuration it \
             replaced, which is the only place that configuration still exists"
        );
    }

    /// A safe restart escalates to a force restart from a configuration its writer adopted.
    ///
    /// The third site of the same defect, which the review did not name. `Restarting`'s
    /// `JobMessage::ConfigUpdate` arm escalates a safe restart to a force one when the update
    /// asks for it — an operator saying they will not wait for the final checkpoint — and its
    /// adopted route dropped that for the same reason `Running`'s dropped the restart
    /// classification.
    #[tokio::test]
    async fn a_safe_restart_escalates_to_a_force_restart_from_an_adopted_configuration() {
        let current = running_config(StateBackendSelector::Parquet);
        let mut forced = current.clone();
        forced.restart_mode = RestartMode::force;

        let mailbox = intent_mailbox();
        mailbox.submit(LifecycleIntent::Adopt(Box::new(forced)));

        let db = sqlite_startable_job("Running", 2);
        let (_calls, job_controller) =
            controller_over_a_worker_storing_checkpoints_in(one_operator_program_at(1), db.clone())
                .await;
        let mut harness = Harness::new(current.restart_nonce)
            .with_db(db)
            .with_program(one_operator_program_at(1))
            .with_job_controller(job_controller)
            .with_actor(&mailbox);
        let mut ctx = harness.ctx(current, StateBackendSelector::Parquet);

        let transition = tokio::time::timeout(
            STOP_REACHES_THE_WAIT,
            Box::new(Restarting {
                mode: RestartMode::safe,
            })
            .next(&mut ctx),
        )
        .await
        .expect("`Restarting` reads its writer on every turn of its final-checkpoint wait")
        .expect("an escalation is not a job failure");

        assert_eq!(
            left_for(transition),
            "Restarting { mode: force }",
            "the same escalation the `ConfigUpdate` arm makes, from the same rule, reached \
             through the mailbox instead of the channel"
        );
    }

    /// The job's wait is assembled in one place, out of every source a stop can arrive on.
    ///
    /// This is the forcing function, and the reason the fix is one wait rather than three local
    /// repairs. A wait is a `JobWait`; `JobWait::new` is visible only inside `crate::states`;
    /// `JobController::wait_for_finish` takes the wait and can no longer take a bare channel.
    /// So a wait written later cannot watch half the sources, because it cannot build a
    /// half-blind wait — the same argument `leave_for_stop` makes one level up.
    #[test]
    fn the_jobs_wait_is_assembled_in_one_place() {
        let states = include_str!("mod.rs");
        let production = &states[..states
            .find("\n#[cfg(test)]")
            .expect("this module has a test module")];
        assert_eq!(
            production.matches("JobWait::new(").count(),
            1,
            "one place assembles the job's wait, and it takes every field a decision can \
             arrive on. A second would be a second thing that could be given a subset"
        );
        assert!(
            production.contains("pub(crate) fn controller_and_wait("),
            "and it is the split that hands a state its controller and its wait together, \
             which is what makes assembling one by hand unnecessary"
        );

        let controller = include_str!("../job_controller/mod.rs");
        assert!(
            controller.contains("wait: &mut JobWait<'_>,"),
            "`wait_for_finish` takes the job's wait. Taking a `&mut Receiver<JobMessage>` is \
             what let it watch a source no lifecycle decision is ever sent to"
        );
        assert_eq!(
            controller.matches("JobWait::new(").count(),
            0,
            "and cannot build one: `JobWait::new` is `pub(in crate::states)`, so the module \
             that waits is not the module that decides what a wait observes"
        );
        assert_eq!(
            include_str!("lifecycle/waiting.rs")
                .matches("pub(in crate::states) fn new(")
                .count(),
            1,
            "which is the visibility this rests on"
        );
    }

    // ---------------------------------------------------------------------------------------
    // An inactive job whose pending intent needs a state task — PR #160 review comment
    // `5368132947`.
    //
    // Under the D39a mechanism the configuration poll decides nothing: it classifies the row,
    // leaves an intent, and the job's own state task decides it. A job with no state task has
    // no actor, so an intent left for one is decided by nobody — and `restart_if_needed` starts
    // a job that has reached a terminal state only when the shared cell says the poll's latest
    // word about it has not been taken up. The poll left that cell alone, so an accepted
    // restart of a `Failed` or `Stopped` job was stranded in the mailbox forever.
    //
    // The fix asks the mailbox one question — is this a new intent that needs a state task —
    // and records the answer on the same delivery watermark the landed mechanism already uses.
    // The three answers are the landed path's own three, one per classification, so the two
    // mechanisms start a task for the same rows. It decides nothing further: which field of an
    // adopted configuration matters, and whether the job actually restarts, stay the state's,
    // which is what the rows below separate.
    // ---------------------------------------------------------------------------------------

    /// The status of a job that has reached a terminal state.
    fn terminal_status(state: &str, restart_nonce: i32) -> JobStatus {
        JobStatus {
            state: state.to_string(),
            ..job_status(restart_nonce)
        }
    }

    /// The configuration a job an operator stopped is left running under.
    fn a_stopped_jobs_config() -> JobConfig {
        let mut config = running_config(StateBackendSelector::Parquet);
        config.stop_mode = StopMode::checkpoint;
        config
    }

    /// A job this controller is already administering, whose state task has ended.
    ///
    /// `tx: None` is the task having gone, and [`state_machine_in_mode`] leaves the shared cell
    /// `AppliedStatus::Applied` — which is exactly what a task that ran leaves behind, because
    /// [`run_to_completion`] clears the flag at the head of every state. So this is the job the
    /// finding is about: one `restart_if_needed` will not start on its own account, whose only
    /// remaining prompt is the poll's own word about it.
    ///
    /// **Selects `FencedV2` by name**, here rather than in each row. Under `LegacyT08` there is
    /// no mailbox at all and every path below is inert, so a row that forgot to select the mode
    /// would pass while proving nothing.
    ///
    /// The scheduler panics instead of starting a cluster. That is what lets a job which really
    /// does restart end its own state task, so [`drive_to_completion`] can read back everything
    /// it wrote on the way; the panic is expected, is printed by the runtime, and happens
    /// strictly after the generation advance every positive row asserts.
    fn an_inactive_fenced_job(
        state: &str,
        current: JobConfig,
    ) -> (StateMachine, DatabaseSource, Arc<RecordingScheduler>) {
        let db = sqlite_startable_job(state, 2);
        let scheduler = Arc::new(RecordingScheduler::panicking());
        let sm = state_machine_in_mode(
            LifecycleMode::FencedV2,
            current,
            StateBackendSelector::Parquet,
            None,
            db.clone(),
            scheduler.clone(),
        );
        (sm, db, scheduler)
    }

    /// What a job that was really brought back up writes, and what it asks of its cluster.
    ///
    /// The generation advance and the `None` teardown are the two things only a job that
    /// reached [`Scheduling`] can produce: `run_id` is written by nothing but
    /// [`JobStatus::update_db`] and advanced by nothing but `Scheduling`, and a teardown under
    /// no generation is `Scheduling`'s destructive pre-scheduling one rather than
    /// `handle_terminal`'s. So "a task was started" and "the job restarted" are different
    /// assertions here, which is what the negative rows below rest on.
    fn assert_the_job_was_brought_back_up(
        from: &str,
        writes: &[(String, u64)],
        scheduler: &RecordingScheduler,
    ) {
        assert_eq!(
            writes,
            [
                (from.to_string(), 1),
                ("Compiling".to_string(), 1),
                ("Scheduling".to_string(), 1),
                ("Scheduling".to_string(), 2),
            ],
            "the job left its terminal state, compiled, and advanced the generation it \
             reschedules under — none of which happens without a state task to decide the \
             intent the poll left"
        );
        assert_eq!(
            scheduler.stopped.lock().unwrap().as_slice(),
            [
                ("job_abc".to_string(), Some(1)),
                ("job_abc".to_string(), None)
            ],
            "the terminal state's teardown of the generation it knew, and then `Scheduling`'s \
             own teardown of whatever cluster was there — the second is the one only a job \
             that is really being scheduled makes"
        );
    }

    /// A `Stopped` job whose stop an operator cleared is started, and runs.
    ///
    /// The first of the two cases the finding names. The shared cell is `Applied` and the
    /// job's status says `Stopped`, so neither arm of `restart_if_needed` fires on its own
    /// account; the accepted intent is the only thing that says this job has somewhere to be.
    #[tokio::test]
    async fn an_inactive_stopped_job_is_started_for_a_cleared_stop_mode() {
        // Held for the whole test: the started task has to run, not merely exist.
        let shutdown = LiveShutdown::new();
        let (mut sm, db, scheduler) = an_inactive_fenced_job("Stopped", a_stopped_jobs_config());
        assert!(sm.done(), "the job's state task ended when the job stopped");

        // The operator's restart: the same row with the stop taken off it.
        let restarted = running_config(StateBackendSelector::Parquet);
        assert_eq!(
            restarted.stop_mode,
            StopMode::none,
            "the fixture's precondition: what changed is the stop, and only the stop"
        );

        sm.update(
            polled(StateBackendSelector::Parquet, restarted, None),
            terminal_status("Stopped", 3),
            shutdown.guard(),
        )
        .await;

        assert!(
            !sm.done(),
            "a job whose pending intent can make it runnable must be given a state task: \
             without one there is no actor, and an intent no actor ever reads is a restart \
             that never happens"
        );
        let writes = drive_to_completion(&sm, &db).await;
        assert_the_job_was_brought_back_up("Stopped", &writes, &scheduler);
    }

    /// A `Failed` job whose restart nonce the poll accepted is started, and runs.
    ///
    /// The other case, and the reason the condition is not "the stop mode was cleared": what a
    /// terminal state reads to decide whether it restarts differs per state — `Stopped` reads
    /// `stop_mode` and `ttl`, `Failed` reads `restart_nonce` against the one its own status
    /// records — and the poll asks about none of them.
    #[tokio::test]
    async fn an_inactive_failed_job_is_started_for_a_bumped_restart_nonce() {
        let shutdown = LiveShutdown::new();
        let failed = running_config(StateBackendSelector::Parquet);
        let (mut sm, db, scheduler) = an_inactive_fenced_job("Failed", failed.clone());
        assert!(sm.done(), "the job's state task ended when the job failed");

        let mut restarted = failed.clone();
        restarted.restart_nonce = failed.restart_nonce + 1;
        assert_eq!(
            restarted.stop_mode,
            StopMode::none,
            "and this row's stop mode is what it always was, so the two rows above cannot both \
             be passing for the same reason"
        );

        sm.update(
            polled(StateBackendSelector::Parquet, restarted, None),
            terminal_status("Failed", failed.restart_nonce),
            shutdown.guard(),
        )
        .await;

        assert!(
            !sm.done(),
            "the same liveness property, for the other terminal state"
        );
        let writes = drive_to_completion(&sm, &db).await;
        assert_the_job_was_brought_back_up("Failed", &writes, &scheduler);
    }

    /// Being started for an intent is not being restarted by it, and being started once is not
    /// being started every 500ms.
    ///
    /// A row an operator changed while the job stays stopped — a bumped restart nonce with the
    /// stop still on it — is an accepted configuration, so the job is given a task to decide
    /// it. `Stopped` then reads the published configuration and stays stopped, which is the
    /// division this fix rests on: the poll asks whether an intent needs a state task, and the
    /// state decides what the intent means. If the poll answered the second question it would
    /// have to reimplement `Stopped::next`, and would be wrong the moment a state read one more
    /// field.
    ///
    /// The second half is what stops that being a 2Hz resurrection loop. The row is polled
    /// nine more times; each is the same intent, so it is coalesced, so it asks for nothing —
    /// and the job's cluster is torn down once, not once per poll.
    #[tokio::test]
    async fn a_stopped_job_started_for_an_intent_that_leaves_its_stop_standing_stays_stopped() {
        let shutdown = LiveShutdown::new();
        let stopped = a_stopped_jobs_config();
        let (mut sm, db, scheduler) = an_inactive_fenced_job("Stopped", stopped.clone());

        let mut edited = stopped.clone();
        edited.restart_nonce = stopped.restart_nonce + 1;
        let poll = || polled(StateBackendSelector::Parquet, edited.clone(), None);

        sm.update(poll(), terminal_status("Stopped", 3), shutdown.guard())
            .await;
        assert!(
            !sm.done(),
            "the row was accepted, so a task exists to decide it — deciding is not the poll's"
        );
        let writes = drive_to_completion(&sm, &db).await;
        assert_eq!(
            writes,
            [("Stopped".to_string(), 1)],
            "and the state decided it stays: the job never left `Stopped`, never compiled, and \
             never advanced the generation it would reschedule under"
        );

        for repoll in 1..10 {
            sm.update(poll(), terminal_status("Stopped", 3), shutdown.guard())
                .await;
            assert!(
                sm.done(),
                "poll {repoll}: the row has not changed since the poll that was already acted \
                 on, so it is the same intent and asks for no second task. A start per poll \
                 would be a stopped job's cluster torn down every 500ms forever"
            );
        }
        assert_eq!(
            state_writes(&db),
            [("Stopped".to_string(), 1)],
            "ten polls, one state task"
        );
        assert_eq!(
            scheduler.stopped.lock().unwrap().as_slice(),
            [("job_abc".to_string(), Some(1))],
            "and one terminal teardown, under the generation the job already had"
        );
    }

    /// A terminal job is not woken up by a refusal that asks for nothing else.
    ///
    /// The anti-resurrection half, and the reason the condition is a property of the intent
    /// rather than "there is something in the mailbox". A refused row that asks for nothing but
    /// the refusal fails the job, and a task started for one would take a job that had
    /// legitimately reached `Stopped` and fail it, or one that had reached `Failed` and fail it
    /// again. The landed mechanism does not do that — `apply_refused_row` reaches
    /// `restart_if_needed` for exactly this row, and it starts nothing for a terminal job — so
    /// neither may this.
    #[tokio::test]
    async fn a_terminal_job_is_not_started_for_a_refusal_that_asks_for_nothing_else() {
        for job_state in ["Stopped", "Failed"] {
            let shutdown = LiveShutdown::new();
            let (mut sm, db, scheduler) =
                an_inactive_fenced_job(job_state, a_stopped_jobs_config());

            let refused = running_config(StateBackendSelector::Parquet);
            assert_eq!(
                refused.stop_mode,
                StopMode::none,
                "the row asks for nothing but the refusal, which is what separates this row \
                 from the one below"
            );

            sm.update(
                polled(
                    StateBackendSelector::Parquet,
                    refused,
                    Some(selector_changed()),
                ),
                terminal_status(job_state, 3),
                shutdown.guard(),
            )
            .await;

            assert!(
                sm.done(),
                "{job_state}: no state task. Starting one would resurrect a job that has \
                 legitimately ended, only to fail it"
            );
            assert_eq!(
                state_writes(&db),
                [],
                "{job_state}: and nothing was written about it at all — not the status write \
                 `start` makes before it spawns, and not a second failure"
            );
            assert_eq!(
                scheduler.stopped.lock().unwrap().as_slice(),
                [],
                "{job_state}: and its cluster was not touched"
            );
            assert_eq!(scheduler.started.lock().unwrap().as_slice(), []);
            assert!(
                standing_intent(&mailbox_of(&sm)).is_some(),
                "{job_state}: the intent is still there, for the actor of a task started for \
                 some later reason — not started for is not discarded"
            );
        }
    }

    /// A refused row's stop reaches a job with no state task, and does not resurrect it.
    ///
    /// The other stranding this sweep found, and the reason the condition is "needs a state
    /// task" rather than "could make the job runnable". Refusing a row's selector must not
    /// discard the row's lifecycle control: the refusal's whole remedy is "stop this job and
    /// create a new one under the other backend", and the landed path starts a task for
    /// precisely this row — `apply_refused_row` reaches `request_stop`, whose `Inactive` arm
    /// stores the stop and restarts the job's state machine. A fenced path that started nothing
    /// here would have quietly dropped that.
    ///
    /// The second half is what makes it a delivery rather than a resurrection: a stop cannot
    /// make a job run, so the task starts, is handed the stop by the boundary, and the terminal
    /// state ends it again. The job never compiles and never advances the generation it would
    /// reschedule under.
    #[tokio::test]
    async fn a_refused_rows_stop_reaches_a_terminal_job_without_restarting_it() {
        for job_state in ["Stopped", "Failed"] {
            let shutdown = LiveShutdown::new();
            let (mut sm, db, scheduler) =
                an_inactive_fenced_job(job_state, a_stopped_jobs_config());

            let mut refused = running_config(StateBackendSelector::Parquet);
            refused.stop_mode = StopMode::immediate;

            sm.update(
                polled(
                    StateBackendSelector::Parquet,
                    refused,
                    Some(selector_changed()),
                ),
                terminal_status(job_state, 3),
                shutdown.guard(),
            )
            .await;

            assert!(
                !sm.done(),
                "{job_state}: a job with no state task can be told nothing, and the stop is the \
                 refusal's remedy"
            );
            let writes = drive_to_completion(&sm, &db).await;
            assert_eq!(
                writes,
                [(job_state.to_string(), 1)],
                "{job_state}: and the task it was given ends it again where it was. A stop \
                 cannot make a job run, so this is delivery, not resurrection: no `Compiling`, \
                 and no generation advance"
            );
            assert_eq!(
                scheduler.started.lock().unwrap().as_slice(),
                [],
                "{job_state}: and no cluster was started for a row the controller refused"
            );
            assert_eq!(
                scheduler.stopped.lock().unwrap().as_slice(),
                [("job_abc".to_string(), Some(1))],
                "{job_state}: the terminal teardown, under the generation the job already had — \
                 never `Scheduling`'s destructive one"
            );
        }
    }

    /// The control: the landed mechanism still starts an inactive job for an accepted update,
    /// exactly as it did.
    ///
    /// Selects `LifecycleMode::SELECTED`, which is `LegacyT08` and is what production runs.
    /// There is no mailbox on this path — the accepted row is stored into the shared cell and
    /// `AppliedStatus::NotApplied` is stored with it — and nothing above may change that. The
    /// row is the same operator action as
    /// `an_inactive_stopped_job_is_started_for_a_cleared_stop_mode`, against the same fixture,
    /// so the two together say the fix added a path rather than altering one.
    #[tokio::test]
    async fn the_legacy_lifecycle_still_starts_an_inactive_job_for_an_accepted_update() {
        let shutdown = LiveShutdown::new();
        let db = sqlite_startable_job("Stopped", 2);
        let scheduler = Arc::new(RecordingScheduler::panicking());
        let mut sm = state_machine_in_mode(
            LifecycleMode::SELECTED,
            a_stopped_jobs_config(),
            StateBackendSelector::Parquet,
            None,
            db.clone(),
            scheduler.clone(),
        );
        assert!(
            sm.lifecycle.intents().is_none(),
            "the control's own precondition: the selected mechanism has no intent mailbox, so \
             this row cannot be passing through the path the rows above exercise"
        );

        sm.update(
            polled(
                StateBackendSelector::Parquet,
                running_config(StateBackendSelector::Parquet),
                None,
            ),
            terminal_status("Stopped", 3),
            shutdown.guard(),
        )
        .await;

        assert!(
            !sm.done(),
            "unchanged: the accepted update starts the job's task"
        );
        let writes = drive_to_completion(&sm, &db).await;
        assert_the_job_was_brought_back_up("Stopped", &writes, &scheduler);
    }

    /// Every shape [`LifecycleIntent::classify`] can produce, as the polled row that produces
    /// it.
    ///
    /// The list is the fixture, not the domain: the domain is the enum, and
    /// `every_mailbox_submission_path_can_get_the_job_a_state_task` checks this against it.
    fn every_classified_intent() -> Vec<(&'static str, JobConfig, Option<StateBackendError>)> {
        let mut accepted = running_config(StateBackendSelector::Parquet);
        accepted.restart_nonce += 1;

        let mut refused_and_stopping = running_config(StateBackendSelector::Parquet);
        refused_and_stopping.stop_mode = StopMode::immediate;

        vec![
            ("Adopt", accepted, None),
            (
                "RefusedButStopping",
                refused_and_stopping,
                Some(selector_changed()),
            ),
            (
                "Refused",
                running_config(StateBackendSelector::Parquet),
                Some(selector_changed()),
            ),
        ]
    }

    /// No production path can leave an intent for a job that will never have a task to decide
    /// it.
    ///
    /// The quantified row, and the one that would have found this finding before a reviewer
    /// did. Four findings on this PR have had the same shape — the fenced path records a
    /// decision and the machinery that acts on it does not run — and every one of them got
    /// through because the sites were enumerated by hand. So both domains here are taken from
    /// the source: the submission sites are counted off `states/mod.rs`, and the intent shapes
    /// off `LifecycleIntent`'s own declaration. Quantifying over the second is what found the
    /// refused-row stop that `a_refused_rows_stop_reaches_a_terminal_job_without_restarting_it`
    /// covers, which the finding did not name.
    ///
    /// The two submission paths do not have the same answer, and saying so is the point.
    /// `StateMachine::new` is a job the controller has just picked up: it has to be adopted
    /// before anything about it can be decided, so its task is started unconditionally, on
    /// both mechanisms. `StateMachine::update` is a job the controller already administers,
    /// whose task may have ended for a perfectly good reason, so it starts one only for an
    /// intent that could make the job runnable.
    ///
    /// `new`'s half is a source pin rather than a driven row because `new` is the *single*
    /// production site that chooses a lifecycle mechanism — `no_production_path_selects_the_
    /// fenced_v2_lifecycle` pins that — so it cannot be driven in the fenced mode at all. What
    /// is pinned instead is the thing that makes its answer `Always`: the start is a statement
    /// of the function's own body, not of either mechanism's branch.
    #[tokio::test]
    async fn every_mailbox_submission_path_can_get_the_job_a_state_task() {
        /// Everything in a file before its test module: a submission in a test is not a
        /// production path.
        fn production_half(source: &str) -> &str {
            match source.find("\n#[cfg(test)]") {
                Some(at) => &source[..at],
                None => source,
            }
        }

        // ---- the domain of submission sites, from the source -------------------------------
        let states = include_str!("mod.rs");
        let production = production_half(states);
        let (in_new, in_update) = production
            .split_once("    pub async fn update(")
            .expect("`StateMachine::update` is declared in this file");

        assert_eq!(
            in_new.matches("intents.submit(").count(),
            1,
            "`StateMachine::new` leaves exactly one intent, and nothing before it does"
        );
        assert_eq!(
            in_update.matches("intents.submit(").count(),
            1,
            "and `StateMachine::update` exactly one. A third submission site is a third thing \
             that would have to answer this question, and it would not be found by a list"
        );
        assert!(
            in_new.contains(
                "\n        this.start(status.clone(), shutdown_guard.clone_temporary())\n"
            ),
            "`new`'s answer is `always`, and this is what makes it one: the start is a \
             statement of the constructor's own body, at its top level, so it happens whichever \
             mechanism the job was built with"
        );

        // A whole file that is a `#[cfg(test)]` module has no production half at all, and
        // `production_half` cannot see that from inside it — the marker is on the `mod`
        // declaration in its parent. So the set is read off the declarations, not guessed
        // from file names.
        let mut test_modules = std::collections::BTreeSet::new();
        walk_crate_sources(&mut |_, source| {
            for declaration in source.split("#[cfg(test)]\n").skip(1) {
                let declaration = declaration.trim_start_matches("pub(crate) ");
                if let Some(rest) = declaration.strip_prefix("mod ")
                    && let Some(name) = rest.split(';').next()
                    && !name.contains(char::is_whitespace)
                {
                    test_modules.insert(name.to_string());
                }
            }
        });
        assert!(
            test_modules.contains("tests"),
            "the sweep below rests on finding these; `states/lifecycle/tests.rs` alone would \
             otherwise be read as a production submission path"
        );

        let mut elsewhere = std::collections::BTreeSet::new();
        walk_crate_sources(&mut |path, source| {
            let under_test = path
                .trim_end_matches(".rs")
                .split('/')
                .any(|part| test_modules.contains(part));
            if !under_test && production_half(source).contains(".submit(") {
                elsewhere.insert(path.to_string());
            }
        });
        assert_eq!(
            elsewhere,
            ["states/mod.rs".to_string()].into_iter().collect(),
            "and `states/mod.rs` is the only file that submits at all: a mailbox handed to \
             anything else would be a submission path this row never asked about"
        );

        // ---- the domain of intent shapes, from the source ----------------------------------
        let intent_source = include_str!("lifecycle/intent.rs");
        let declaration = {
            let at = intent_source
                .find("pub(crate) enum LifecycleIntent {")
                .expect("the enum this row quantifies over");
            let rest = &intent_source[at..];
            &rest[..rest.find("\n}\n").expect("a closed declaration")]
        };
        let declared = declaration
            .lines()
            .filter(|line| {
                line.starts_with("    ")
                    && !line.starts_with("     ")
                    && line[4..].starts_with(|c: char| c.is_ascii_uppercase())
            })
            .count();
        assert_eq!(
            declared,
            every_classified_intent().len(),
            "every variant of `LifecycleIntent` is a shape the poll can leave behind, so every \
             one of them needs a row in `every_classified_intent`"
        );

        // ---- `update`'s answer, driven, for every shape ------------------------------------
        for (shape, config, refusal) in every_classified_intent() {
            let row = || {
                polled(
                    StateBackendSelector::Parquet,
                    config.clone(),
                    refusal.clone(),
                )
            };
            let intent = LifecycleIntent::classify(StateBackendSelector::Parquet, row());
            let produced = match &intent {
                LifecycleIntent::Adopt(_) => "Adopt",
                LifecycleIntent::RefusedButStopping { .. } => "RefusedButStopping",
                LifecycleIntent::Refused(_) => "Refused",
            };
            assert_eq!(
                produced, shape,
                "the fixture really produces the shape it is filed under; the match above is \
                 exhaustive, so a new variant does not compile until it is answered here too"
            );

            // Read off the production predicate rather than restated. What that predicate
            // *should* say is settled by the closed-form rows above; what this row settles is
            // that the submission path is wired to it.
            let needs_a_task = intent.needs_a_state_task();

            for job_state in ["Stopped", "Failed"] {
                let shutdown = LiveShutdown::new();
                let (mut sm, db, _scheduler) =
                    an_inactive_fenced_job(job_state, a_stopped_jobs_config());

                sm.update(row(), terminal_status(job_state, 3), shutdown.guard())
                    .await;

                assert_eq!(
                    !sm.done(),
                    needs_a_task,
                    "{job_state}/{shape}: a job with no state task gets one exactly when its \
                     pending intent needs one"
                );
                if needs_a_task {
                    // Let it end, so the panicking scheduler's task does not outlive the row.
                    drive_to_completion(&sm, &db).await;
                } else {
                    assert_eq!(
                        state_writes(&db),
                        [],
                        "{job_state}/{shape}: and a job that was not started wrote nothing"
                    );
                }
            }
        }
    }

    /// Every `.rs` file under this crate's `src`, as `(path relative to src, contents)`.
    fn walk_crate_sources(visit: &mut dyn FnMut(&str, &str)) {
        fn walk(dir: &std::path::Path, root: &std::path::Path, visit: &mut dyn FnMut(&str, &str)) {
            for entry in std::fs::read_dir(dir).expect("this crate's own source") {
                let path = entry.expect("a readable directory entry").path();
                if path.is_dir() {
                    walk(&path, root, visit);
                    continue;
                }
                if path.extension().is_none_or(|e| e != "rs") {
                    continue;
                }
                let source = std::fs::read_to_string(&path).expect("a readable source file");
                let relative = path
                    .strip_prefix(root)
                    .expect("a path under the crate's source root")
                    .to_string_lossy()
                    .replace('\\', "/");
                visit(&relative, &source);
            }
        }

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        walk(&root, &root, visit);
    }
}
