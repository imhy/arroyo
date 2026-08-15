use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use std::{fmt::Debug, sync::Arc};

use arroyo_rpc::grpc::api::ArrowProgram;

use thiserror::Error;
use time::OffsetDateTime;
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
use crate::{JobConfig, JobMessage, JobStatus, PipelineInfo, queries, schedulers::Scheduler};
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
/// Returns a fatal [`StateError`] for [`JobMessage::ConfigRefused`]. Every other message
/// is logged and ignored.
pub(crate) fn handle_unhandled_message(
    job_id: &str,
    pipeline_id: &str,
    msg: JobMessage,
) -> Result<(), StateError> {
    match msg {
        JobMessage::ConfigRefused(e) => {
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
    /// Whether the last polled row was refused, so the refusal is logged once rather than
    /// on every 500ms poll for as long as the row stays bad.
    refused: bool,
    pub(crate) state: Arc<RwLock<String>>,
    metrics: Arc<tokio::sync::RwLock<HashMap<Arc<String>, JobMetrics>>>,
    db: DatabaseSource,
    scheduler: Arc<dyn Scheduler>,
}

impl StateMachine {
    pub async fn new(
        config: JobConfig,
        status: JobStatus,
        db: DatabaseSource,
        scheduler: Arc<dyn Scheduler>,
        shutdown_guard: ShutdownGuard,
        metrics: Arc<tokio::sync::RwLock<HashMap<Arc<String>, JobMetrics>>>,
    ) -> Self {
        let execution_selector = config.state_backend;
        let mut this = Self {
            tx: None,
            config: Arc::new(RwLock::new((config, AppliedStatus::NotApplied))),
            execution_selector,
            refused: false,
            state: Arc::new(RwLock::new(status.state.clone())),
            metrics,
            db,
            scheduler,
        };

        this.start(status, shutdown_guard).await;

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
            status.update_db(&self.db).await.unwrap();
            let (tx, rx) = channel(1024);
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
                match Self::get_program(&db, &status.id, pipeline_id).await {
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
                    }
                    Ok(None) => {
                        // this is a bad/old pipeline, skip it
                    }
                    Err(e) => {
                        // something went wrong, we'll retry on the next go around
                        warn!(job_id = %status.id, "Failed to start job: {:?}", e);
                    }
                }
            }

            self.tx = Some(tx);
        }
    }

    /// Applies a freshly polled `job_configs` row to this job.
    ///
    /// The selector is classified **first**, before the shared configuration is replaced.
    /// Everything downstream reads that cell — the states' `ConfigUpdate` message, the
    /// `ctx.config` refresh after every transition, and therefore the worker and task
    /// startup requests scheduling builds from it — so a row that changes the backend has
    /// to be stopped here, at the one writer, rather than at each of those readers. It
    /// also makes the refusal durable: because the new `restart_nonce` is refused along
    /// with the rest of the row, `Failed` does not see a restart request and the update
    /// the refusal promised not to restart cannot restart.
    pub async fn update(
        &mut self,
        config: JobConfig,
        status: JobStatus,
        shutdown_guard: &ShutdownGuard,
    ) {
        *self.state.write().unwrap() = status.state.clone();

        if let Err(e) = validate_unchanged_job_selector(
            &config.id,
            self.execution_selector,
            config.state_backend,
        ) {
            self.refuse_config(e).await;
            return;
        }
        self.refused = false;

        if self.config.read().unwrap().0 != config {
            let update = JobMessage::ConfigUpdate(config.clone());
            {
                let mut c = self.config.write().unwrap();
                *c = (config, AppliedStatus::NotApplied);
            }
            if self.send(update).await.is_err() {
                self.start(status, shutdown_guard.clone_temporary()).await;
            }
        } else {
            let applied = self.config.read().unwrap().1;
            self.restart_if_needed(applied, status, shutdown_guard)
                .await;
        }
    }

    /// Refuses the job's persisted configuration and fails the job.
    ///
    /// Two things reach this: a row whose `state_backend` names a different backend than
    /// the job is running with, and a row whose `state_backend` cannot be interpreted at
    /// all (see [`crate::job_config_or_refusal`]).
    ///
    /// Nothing about the refused row is adopted — the shared configuration is deliberately
    /// left holding the value the job's workers, table configs, and checkpoints were built
    /// from, so the refused selector never becomes any state's baseline, and the refused
    /// `restart_nonce` never reaches [`Failed`], which is what stops the job restarting
    /// under a selector that was just refused. The job is failed instead, by the state it
    /// is in, through [`JobContext::handle`].
    ///
    /// If the state machine has already stopped there is nothing to fail and nothing to
    /// restart: the send fails and — unlike an accepted update — no restart is attempted,
    /// because restarting is exactly what a refused row must not cause.
    pub(crate) async fn refuse_config(&mut self, error: StateBackendError) {
        let job_id = self.config.read().unwrap().0.id.clone();
        if !self.refused {
            self.refused = true;
            error!(job_id = %job_id, error = %error, "refusing the job's persisted configuration");
        }

        if self.send(JobMessage::ConfigRefused(error)).await.is_err() {
            debug!(
                job_id = %job_id,
                "state machine is already stopped; not restarting it for a refused config"
            );
        }
    }

    pub async fn send(&mut self, msg: JobMessage) -> Result<(), &'static str> {
        if let Some(tx) = &self.tx {
            tx.send(msg).await.map_err(|_| "State machine is inactive")
        } else {
            Err("State machine is inactive")
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
        AppliedStatus, Failed, Failing, JobContext, RunningConfigUpdate, State, StateMachine,
        Transition, adopt_refreshed_config, check_config_update, classify_running_config_update,
        handle_unhandled_message,
    };
    use crate::schedulers::{Scheduler, SchedulerError, StartPipelineReq};
    use crate::types::public::{RestartMode, StopMode};
    use crate::{JobConfig, JobMessage, JobStatus, PipelineInfo, StateContext, states::StateError};
    use arroyo_datastream::logical::LogicalProgram;
    use arroyo_rpc::grpc::rpc::{HeartbeatNodeReq, RegisterNodeReq, WorkerFinishedReq};
    use arroyo_rpc::state_backend::{StateBackendError, StateBackendSelector};
    use arroyo_server_common::shutdown::{Shutdown, SignalBehavior};
    use arroyo_types::{PipelineId, WorkerId};
    use cornucopia_async::DatabaseSource;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc::{Receiver, channel};

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
    /// neither touches the database.
    fn state_machine(
        config: JobConfig,
        execution_selector: StateBackendSelector,
    ) -> (StateMachine, Receiver<JobMessage>) {
        let (tx, rx) = channel(16);
        (
            StateMachine {
                tx: Some(tx),
                config: Arc::new(RwLock::new((config, AppliedStatus::Applied))),
                execution_selector,
                refused: false,
                state: Arc::new(RwLock::new("Running".to_string())),
                metrics: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
                db: unused_db(),
                scheduler: Arc::new(RecordingScheduler::default()),
            },
            rx,
        )
    }

    fn shutdown_guard() -> arroyo_server_common::shutdown::ShutdownGuard {
        Shutdown::new("test", SignalBehavior::None).guard("test")
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

        let mut updated = running_config(StateBackendSelector::StateEngine);
        updated.restart_nonce = current.restart_nonce + 1;

        sm.update(
            updated,
            job_status(current.restart_nonce),
            &shutdown_guard(),
        )
        .await;

        let stored = sm.config.read().unwrap().0.clone();
        assert_eq!(
            stored.state_backend,
            StateBackendSelector::Parquet,
            "the refused selector must not have become the job's authoritative config"
        );
        assert_eq!(
            stored.restart_nonce, current.restart_nonce,
            "the restart nonce that arrived with the refused selector must not have been \
             stored either, or Failed would restart the job under it"
        );

        match rx
            .try_recv()
            .expect("the refusal should have been delivered")
        {
            JobMessage::ConfigRefused(e) => assert!(
                matches!(e, StateBackendError::JobSelectorChanged { .. }),
                "{e:?}"
            ),
            other => panic!("a refused row must not be delivered as a config update: {other:?}"),
        }
        assert!(
            rx.try_recv().is_err(),
            "nothing else may be delivered for a refused row"
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
            updated.clone(),
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
        })
        .await;

        assert_eq!(
            sm.config.read().unwrap().0,
            current,
            "an unusable row must not disturb the config the job is running with"
        );
        match rx
            .try_recv()
            .expect("the refusal should have been delivered")
        {
            JobMessage::ConfigRefused(StateBackendError::UnknownValue { value, .. }) => {
                assert_eq!(value, "rocksdb")
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    /// The refusal reaches the job as a failure, wherever it is. This is the one place the
    /// policy lives, so every state that routes an unrecognized message here fails the
    /// same way.
    #[test]
    fn a_delivered_refusal_fails_the_job() {
        let err = handle_unhandled_message(
            "job_abc",
            "pipeline_1",
            JobMessage::ConfigRefused(StateBackendError::JobSelectorChanged {
                label: "job \"job_abc\"".to_string(),
                running: StateBackendSelector::Parquet,
                requested: StateBackendSelector::StateEngine,
            }),
        )
        .expect_err("a refused configuration must fail the job");
        assert!(
            matches!(
                selector_error(&err),
                StateBackendError::JobSelectorChanged { .. }
            ),
            "{err:?}"
        );

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
        sm.update(
            updated.clone(),
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
