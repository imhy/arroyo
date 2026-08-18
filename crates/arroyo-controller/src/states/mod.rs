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
    ConsumptionPoint, IntentWakeup, JobLifecycle, LifecycleActor, LifecycleIntent, ObservedIntent,
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

#[derive(Error, Debug)]
pub enum StateError {
    #[error("fatal error: {message:?}")]
    FatalError {
        message: String,
        domain: errors::ErrorDomain,
        source: anyhow::Error,
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

pub fn fatal(message: impl Into<String>, source: anyhow::Error) -> StateError {
    StateError::FatalError {
        message: message.into(),
        domain: errors::ErrorDomain::Internal,
        source,
    }
}

#[derive(Debug)]
pub struct Created;

#[async_trait::async_trait]
impl State for Created {
    fn name(&self) -> &'static str {
        "Created"
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
    ($self:ident, $config:expr, $ctx:expr) => {
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
            fatal(
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
            Err(fatal(
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
            fatal(
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
        let Some(decision) = self
            .lifecycle_actor
            .as_mut()
            .and_then(|actor| actor.observe(at))
        else {
            return Ok(ObservedIntent::Continue);
        };
        decision.apply(self)
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
    // A stop decided here is *published* rather than acted on, and that is not a gap: the
    // states that go on to do something irreversible — `Scheduling` in either of its bodies,
    // `Running`, `LeaderRunning` — all open with their own `stop_if_desired*` on the very
    // configuration this writes, and the states that loop instead read it on their first turn.
    // `execute_state` holds a `Box<dyn State>` and could not name the transition anyway: what
    // leaving means is the state's own, which is why it is the state that answers.
    let gated = gated.and_then(|()| {
        ctx.observe_lifecycle_intent(ConsumptionPoint::BeforeIrreversiblePhase)
            .map(|_| ())
    });

    // Which body a state runs is the job's lifecycle mechanism's to choose, and choosing it
    // here rather than inside each state is the same argument as the two reads above: a state
    // that had to remember to ask would be a state that could forget. Production takes the
    // legacy branch for every state of every job — `runs_fenced_lifecycle` is false unless the
    // job was built with the D39a single writer, which no production construction site does.
    let outcome = match gated {
        Ok(()) => scheduling::run_state_body(state, &mut ctx).await,
        Err(refused) => Err(refused),
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
) -> (Admission, T)
where
    F: std::future::Future<Output = (Admission, T)> + Send + 'static,
    T: Send + 'static,
{
    SettlingUnderAdmission {
        region: Some(Box::pin(region(admission))),
    }
    .await
}

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
        runtime.spawn(async move {
            // The admission is inside `region`, and is released only by this drop.
            drop(region.await);
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
        // writer at all: an intent left while the program could not be loaded stays in the
        // mailbox, and is decided by the actor of whichever poll finally gets a task up.
        // `restart_if_needed` starts the job only if it should be running or has never
        // applied a configuration, and it starts it under `Self::execution_selector` and
        // the shared configuration a refused row was never allowed into — so this cannot
        // restart the job under a value that is being refused.
        //
        // The state mirror above is not a lifecycle publication: it is this controller's
        // cached view of what the database already says, read by the API, and it is
        // deliberately still refreshed on both paths.
        if let Some(intents) = self.lifecycle.intents().map(Arc::clone) {
            intents.submit(LifecycleIntent::classify(self.execution_selector, polled));
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
    use super::lifecycle::classification::{
        SelectorClassification, UndecidableSelector, classify_selector,
    };
    use super::lifecycle::intent::{IntentVersion, VersionedIntent};
    use super::lifecycle::{
        ConsumptionPoint, JobLifecycle, LifecycleActor, LifecycleIntent, LifecycleMode,
    };
    use super::{
        Admission, AppliedStatus, Failed, Failing, JobContext, LeaderRunning, RefusalGate, Running,
        RunningConfigUpdate, State, StateMachine, Transition, adopt_refreshed_config,
        check_config_update, classify_running_config_update, execute_state,
        handle_unhandled_message, lifecycle,
    };
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
    use arroyo_rpc::grpc::rpc::job_status_grpc_server::{JobStatusGrpc, JobStatusGrpcServer};
    use arroyo_rpc::grpc::rpc::worker_grpc_server::{WorkerGrpc, WorkerGrpcServer};
    use arroyo_rpc::grpc::rpc::{
        CheckpointMetadata, CheckpointReq, CheckpointResp, CommitReq, CommitResp,
        GetCheckpointDetailsReq, GetCheckpointDetailsResp, GetJobCheckpointsReq,
        GetJobCheckpointsResp, GetWorkerPhaseReq, GetWorkerPhaseResp, GlobalKeyedTableConfig,
        GlobalKeyedTableTaskCheckpointMetadata, HeartbeatNodeReq, JobControllerInitReq,
        JobControllerInitResp, JobFinishedReq, JobFinishedResp, JobStatus as LeaderJobStatus,
        JobStatusReq, JobStatusResp, LoadCompactedDataReq, LoadCompactedDataRes, MetricsReq,
        MetricsResp, OperatorCheckpointMetadata, OperatorMetadata, RegisterNodeReq,
        StartExecutionReq, StartExecutionResp, StopExecutionReq, StopExecutionResp, StopJobReq,
        StopJobResp, TableCheckpointMetadata, TableConfig, TableEnum, WorkerFinishedReq,
    };
    use arroyo_rpc::state_backend::validated::Validated;
    use arroyo_rpc::state_backend::{StateBackendError, StateBackendSelector};
    use arroyo_server_common::shutdown::{Shutdown, SignalBehavior};
    use arroyo_state::validated::CheckpointMetadataWrite;
    use arroyo_state::{BackingStore, StateBackend, StorageProviderFor};
    use arroyo_types::{MachineId, PipelineId, WorkerId};
    use cornucopia_async::DatabaseSource;
    use futures::FutureExt as _;
    use prost::Message as _;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::{Duration, Instant};
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
                    job_id TEXT NOT NULL,
                    epoch INTEGER NOT NULL,
                    min_epoch INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    state_backend TEXT NOT NULL DEFAULT ''
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
                job_controller: None,
                leader_manager: None,
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

    /// Both running modes must route their config updates through the one classifier, and
    /// must hand it the execution's own selector rather than the refreshable `ctx.config`;
    /// neither may keep a private copy of the restart rules that would then not carry the
    /// selector guard. This is a structural pin rather than a behavioural one — driving
    /// either state's `next` needs a live scheduler, database, and worker set.
    #[test]
    fn both_running_modes_classify_config_updates_through_one_rule() {
        for (name, source) in [
            ("running.rs", include_str!("running.rs")),
            ("leader_running.rs", include_str!("leader_running.rs")),
        ] {
            assert!(
                source.contains(
                    "classify_running_config_update(ctx.execution_selector, &ctx.config, &c, \
                     ctx.status.restart_nonce)?"
                ),
                "{name} must classify config updates through classify_running_config_update, \
                 against the execution's selector"
            );
            assert!(
                !source.contains("c.restart_nonce != ctx.status.restart_nonce"),
                "{name} must not keep its own copy of the restart-nonce rule"
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
    }

    impl WorkerCalls {
        fn started(&self) -> Vec<String> {
            self.start_execution.lock().unwrap().clone()
        }

        fn committed(&self) -> Vec<u64> {
            self.commit.lock().unwrap().clone()
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
            _: tonic::Request<StopExecutionReq>,
        ) -> Result<tonic::Response<StopExecutionResp>, tonic::Status> {
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

        // The write takes a token, so the fixture states which operators it stands behind
        // the same way the worker that took the checkpoint does — with the set it just
        // wrote.
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
                    vec![OPERATOR_ID.to_string()],
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
        ctx.observe_lifecycle_intent(ConsumptionPoint::BeforeIrreversiblePhase)
            .expect("an accepted row is adopted, not refused");
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
        ctx.observe_lifecycle_intent(ConsumptionPoint::BeforeIrreversiblePhase)
            .expect("an unchanged selector is adopted, not refused");
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
    /// The intended reading of a failure here is "this wait can no longer be interrupted", not
    /// "the test is stale". A wait that genuinely has nothing an intent could decide — `Stopping`
    /// and `Finishing`, which are already ending the job and have no transition an intent could
    /// produce — is deliberately absent rather than listed and excused.
    #[test]
    fn every_long_running_wait_reads_the_jobs_writer_and_can_be_woken_by_it() {
        for (name, source) in [
            ("running.rs", include_str!("running.rs")),
            ("leader_running.rs", include_str!("leader_running.rs")),
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
            assert!(
                source.contains("ConsumptionPoint::InsideInterruptibleWait"),
                "{name}: every turn of a long-running wait must read the job's single writer. \
                 Under M11.D39a nothing is sent to the job's channel when the poll decides \
                 something, so a wait that does not read the mailbox does not learn"
            );
            assert!(
                source.contains("ctx.lifecycle_wakeup()") && source.contains("wake.notified()"),
                "{name}: and it must be woken by a submission, or the read at the top of the \
                 turn is only reached when something else — a message, a timeout — ended the \
                 previous one"
            );
        }

        // The phase graph splits the two halves across its modules on purpose: the loop that
        // reads lives with the typestate that can see it, and the `select!` that is woken lives
        // with the context that owns the job's channel.
        let phases = include_str!("scheduling/phases.rs");
        assert_eq!(
            phases.matches("awaiting.observe_intent()").count(),
            2,
            "both interruptible waits of the phase graph read the job's single writer"
        );
        assert_eq!(
            include_str!("scheduling/admission/execution.rs")
                .matches("wake.notified()")
                .count(),
            2,
            "and both of their `select!`s can be woken by a submission"
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
    /// Only `GetJobStatus` is ever called: connecting polls once, and that poll is the whole
    /// handshake. The other three are the rest of the service and answer as a leader that has
    /// been asked something it has no business being asked at this point in a job's life.
    struct FakeLeader {
        job_id: String,
        generation: u64,
        /// What this leader reports it is running the job with, in the persisted spelling.
        state_backend: String,
        /// One per status poll, so a row can say the handshake happened rather than assume it.
        polls: Arc<AtomicU64>,
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
                job_status: Some(LeaderJobStatus::default()),
                state_backend: self.state_backend.clone(),
            }))
        }

        async fn stop_job(
            &self,
            _: tonic::Request<StopJobReq>,
        ) -> Result<tonic::Response<StopJobResp>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "this leader is only ever asked for its status",
            ))
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
        let polls = Arc::new(AtomicU64::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(JobStatusGrpcServer::new(FakeLeader {
                    job_id: "job_abc".to_string(),
                    generation,
                    state_backend: state_backend.to_string(),
                    polls: polls.clone(),
                }))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        (polls, format!("http://{addr}"))
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
        let reported = phase
            .into_fencing(reason, IssuedAttempts::default())
            .reconcile_and_report();
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
}
