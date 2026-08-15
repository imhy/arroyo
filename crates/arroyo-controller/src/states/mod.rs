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
    pub pipeline_info: Arc<PipelineInfo>,
    pub status: &'a mut JobStatus,
    pub program: &'a mut LogicalProgram,
    pub db: DatabaseSource,
    pub scheduler: Arc<dyn Scheduler>,
    pub rx: &'a mut Receiver<JobMessage>,
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

    let next: Option<Box<dyn State>> = match state.next(&mut ctx).await {
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
        pipeline_info,
        status: &mut status,
        program: &mut program,
        db: db.clone(),
        scheduler,
        rx: &mut rx,
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
    pub async fn new(
        polled: PolledJob,
        status: JobStatus,
        db: DatabaseSource,
        scheduler: Arc<dyn Scheduler>,
        shutdown_guard: ShutdownGuard,
        metrics: Arc<tokio::sync::RwLock<HashMap<Arc<String>, JobMetrics>>>,
    ) -> Self {
        let PolledJob {
            execution_selector,
            config,
            refusal,
        } = polled;

        let mut this = Self {
            tx: None,
            config: Arc::new(RwLock::new((config, AppliedStatus::NotApplied))),
            execution_selector,
            refusal: None,
            refusal_version: Arc::new(AtomicU64::new(0)),
            state: Arc::new(RwLock::new(status.state.clone())),
            metrics,
            db,
            scheduler,
        };

        this.start(status.clone(), shutdown_guard.clone_temporary())
            .await;

        if let Some(error) = refusal {
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
    async fn apply_refused_row(
        &mut self,
        error: StateBackendError,
        refused: &JobConfig,
        status: JobStatus,
        shutdown_guard: &ShutdownGuard,
    ) {
        if refused.stop_mode != StopMode::none {
            self.note_refusal(error);
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
        // Ordered before the refusal so that the poll which finally gets a state task up
        // also delivers the refusal to it; if nothing came up, the refusal stays pending
        // and both are tried again 500ms later.
        let applied = self.config.read().unwrap().1;
        self.restart_if_needed(applied, status, shutdown_guard)
            .await;

        self.refuse_config(error);
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
    /// Coalescing alone does not make the refusal safe to deliver late. Between the poll
    /// that queues it and the state that reads it, the operator can repair the row — the
    /// remedy the refusal itself asks for — and the queued message cannot be retracted. So
    /// every refusal is stamped with [`Self::refusal_version`], and a version the state
    /// machine has since moved past is discarded on receipt instead of failing the job.
    pub(crate) fn refuse_config(&mut self, error: StateBackendError) {
        let job_id = self.config.read().unwrap().0.id.clone();

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

        let delivery = match self.offer(JobMessage::ConfigRefused(RefusedConfig::new(
            error.clone(),
            version,
            Arc::clone(&self.refusal_version),
        ))) {
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
    use super::{
        AppliedStatus, Failed, Failing, JobContext, LeaderRunning, Running, RunningConfigUpdate,
        State, StateMachine, Transition, adopt_refreshed_config, check_config_update,
        classify_running_config_update, handle_unhandled_message,
    };
    use crate::schedulers::{Scheduler, SchedulerError, StartPipelineReq};
    use crate::types::public::{RestartMode, StopMode};
    use crate::{
        JobConfig, JobMessage, JobStatus, PipelineInfo, PolledJob, RefusedConfig, StateContext,
        states::StateError,
    };
    use arroyo_datastream::logical::LogicalProgram;
    use arroyo_rpc::grpc::api::ArrowProgram;
    use arroyo_rpc::grpc::rpc::{HeartbeatNodeReq, RegisterNodeReq, WorkerFinishedReq};
    use arroyo_rpc::state_backend::{StateBackendError, StateBackendSelector};
    use arroyo_server_common::shutdown::{Shutdown, SignalBehavior};
    use arroyo_types::{PipelineId, WorkerId};
    use cornucopia_async::DatabaseSource;
    use prost::Message as _;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;
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

    fn selector_error(err: &StateError) -> &StateBackendError {
        let StateError::FatalError { source, .. } = err else {
            panic!("expected a fatal error, got {err:?}");
        };
        source
            .downcast_ref::<StateBackendError>()
            .unwrap_or_else(|| panic!("expected a typed selector error, got {source:?}"))
    }

    /// A scheduler that does nothing and records the teardowns the terminal states ask
    /// for. Enough to run the states that only tear a cluster down.
    #[derive(Default)]
    struct RecordingScheduler {
        stopped: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Scheduler for RecordingScheduler {
        async fn start_workers(&self, _: StartPipelineReq) -> Result<(), SchedulerError> {
            Ok(())
        }
        async fn register_node(&self, _: RegisterNodeReq) {}
        async fn heartbeat_node(&self, _: HeartbeatNodeReq) -> Result<(), tonic::Status> {
            Ok(())
        }
        async fn worker_finished(&self, _: WorkerFinishedReq) {}
        async fn stop_workers(&self, job_id: &str, _: Option<u64>, _: bool) -> anyhow::Result<()> {
            self.stopped.lock().unwrap().push(job_id.to_string());
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
                );",
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

    /// Owns everything a [`JobContext`] borrows, so a test can hand real states a real
    /// context and run their `next`.
    struct Harness {
        status: JobStatus,
        program: LogicalProgram,
        rx: Receiver<JobMessage>,
        scheduler: Arc<RecordingScheduler>,
    }

    impl Harness {
        fn new(restart_nonce: i32) -> Self {
            let (_tx, rx) = channel(16);
            Self {
                status: job_status(restart_nonce),
                program: LogicalProgram::default(),
                rx,
                scheduler: Arc::new(RecordingScheduler::default()),
            }
        }

        fn ctx(
            &mut self,
            config: JobConfig,
            execution_selector: StateBackendSelector,
        ) -> JobContext<'_> {
            JobContext {
                config,
                execution_selector,
                pipeline_info: Arc::new(PipelineInfo {
                    pipeline_id: PipelineId("pipeline_1".to_string().into()),
                    state_url: None,
                    tags: HashMap::new(),
                }),
                status: &mut self.status,
                program: &mut self.program,
                db: unused_db(),
                scheduler: self.scheduler.clone(),
                rx: &mut self.rx,
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
        StateMachine {
            tx,
            config: Arc::new(RwLock::new((config, AppliedStatus::Applied))),
            execution_selector,
            refusal: None,
            refusal_version: Arc::new(AtomicU64::new(0)),
            state: Arc::new(RwLock::new("Running".to_string())),
            metrics: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            db,
            scheduler: Arc::new(RecordingScheduler::default()),
        }
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
    #[tokio::test]
    async fn cold_adoption_is_retried_for_a_refused_row_that_asks_for_no_stop() {
        let db = sqlite_startable_job("Running", 2);
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
            Arc::new(RecordingScheduler::default()),
            shutdown_guard(),
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
                &shutdown_guard(),
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
            &shutdown_guard(),
        )
        .await;

        assert!(
            !sm.done(),
            "once the program loads, the still-live job must finally be adopted: a refusal \
             it cannot be told about is not a reason to stop trying to reach it"
        );
        assert_eq!(
            recorded_status(&db),
            ("Compiling".to_string(), Some("parquet".to_string())),
            "and the execution that has now begun is recorded under the job's own \
             immutable selector, never the refused row's"
        );
    }

    /// The delivery half of the same route. A refusal offered to a job with no state task
    /// is not delivered, so it must not be recorded as delivered: it stays pending and is
    /// offered again until something can receive it.
    ///
    /// The version matters as much as the message. The refusal that finally arrives has to
    /// be one the state machine still holds, or `handle_unhandled_message` discards it and
    /// the job keeps running under a row the controller has already rejected.
    #[tokio::test]
    async fn a_refusal_with_no_state_task_is_delivered_once_the_job_has_one() {
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
            ["job_abc"],
            "`Failed` still tears the cluster down; refusing the row does not skip cleanup"
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
}
