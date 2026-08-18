//! Bringing an execution up, and handing it over (M11.T25b, design M11.D39b).
//!
//! The two interruptible waits and the handover that follows them. They live in a child module
//! of [`super`] rather than beside it because they need [`PhaseContext`]'s own fields, and
//! those stay private to the token API: a sibling module would have forced them open, and the
//! whole of "the job's channel is reachable only through the methods a phase chooses to
//! expose" rests on their being shut.
//!
//! # Why this is one module and not two
//!
//! Waiting for the workers, waiting for their tasks, and handing the running execution to the
//! state that owns it are three steps of one thing: bringing an execution up. What separates
//! them from [`super`] is not subject matter but the token — nothing here holds an
//! [`Admission`](crate::states::Admission) except the commit publication, which is the one
//! irreversible thing the handover contains and is therefore written to take one.
//!
//! Both waits are `ctx.rx.recv` wrappers, and this is the only place in the phase graph that
//! has any: [`super::super::phases`]'s token-free types are the only ones that expose a route
//! to them.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use arroyo_rpc::LeaderContext;
use arroyo_rpc::config::config;
use arroyo_rpc::worker_types::RunningMessage;
use arroyo_types::{JobId, WorkerId};
use arroyo_worker::job_controller::job_metrics::JobMetrics;
use tracing::{error, info, warn};

use super::super::{Scheduling, WorkerState, handle_worker_connect};
use super::{PhaseContext, PhaseWait, stop_transition};
use crate::JobMessage;
use crate::job_controller::JobController;
use crate::job_controller::checkpoint_store::DbCheckpointMetadataStore;
use crate::job_controller::leader_manager::LeaderManager;
use crate::states::leader_running::LeaderRunning;
use crate::states::running::Running as RunningState;
use crate::states::{Admission, StateError, Transition, check_config_update};

/// The two interruptible waits: everything the phase graph does while it holds no token.
impl PhaseContext<'_, '_> {
    /// One turn of the wait for workers to register and open their channels.
    ///
    /// # Errors
    ///
    /// Retryable on the startup timeout, and fatal for a configuration update that changes the
    /// job's selector — the workers this waits for were started from it, and the
    /// `StartExecutionReq` the fan-out sends stamps it into every one of them.
    pub(crate) async fn await_message_from_workers(&mut self) -> Result<PhaseWait, StateError> {
        let timeout = self.remaining(*config().pipeline.worker_startup_time);
        tokio::select! {
            val = self.ctx.rx.recv() => self.handle_worker_wait_message(val).await,
            _ = tokio::time::sleep(timeout) => Err(self.retryable(
                "timed out while waiting for workers to start",
                anyhow!(
                    "timed out after {:?} while waiting for worker startup",
                    *config().pipeline.worker_startup_time
                ),
                3,
            )),
        }
    }

    /// Whether the workers that have registered supply the slots the program needs.
    pub(crate) fn workers_are_sufficient(&self) -> bool {
        self.workers.values().map(|w| w.slots).sum::<usize>() >= self.slots_needed
    }

    /// Waits for every registered worker's outbound channel to be open.
    pub(crate) async fn await_worker_channels(&mut self) -> Result<(), StateError> {
        for h in std::mem::take(&mut self.handles) {
            if let Err(e) = h.await {
                return Err(self.retryable("Failed to start cluster for pipeline", e.into(), 10));
            }
        }
        Ok(())
    }

    /// One turn of the wait for the started execution's tasks to report in.
    pub(crate) async fn await_message_from_tasks(&mut self) -> Result<PhaseWait, StateError> {
        let timeout = self.remaining(*config().pipeline.task_startup_time);
        tokio::select! {
            v = self.ctx.rx.recv() => self.handle_task_wait_message(v).await,
            _ = tokio::time::sleep(timeout) => Err(self.task_startup_timeout().await),
        }
    }

    /// Whether every task of the program has reported started.
    pub(crate) fn tasks_are_all_started(&self) -> bool {
        self.started_tasks.len() >= self.ctx.program.task_count()
    }

    /// How much of `budget` this wait has left, also bounded by the job's TTL.
    fn remaining(&self, budget: Duration) -> Duration {
        budget
            .min(self.ctx.config.ttl.unwrap_or(budget))
            .checked_sub(self.wait_started.elapsed())
            .unwrap_or(Duration::ZERO)
    }

    async fn handle_worker_wait_message(
        &mut self,
        val: Option<JobMessage>,
    ) -> Result<PhaseWait, StateError> {
        match val {
            Some(JobMessage::ConfigUpdate(c)) => {
                if let Some(stop) = stop_transition(&c) {
                    return Ok(PhaseWait::Leave(stop));
                }
                check_config_update(self.ctx.execution_selector, &c)?;
                Ok(PhaseWait::Continue)
            }
            Some(msg) => {
                let connects = Arc::clone(&self.worker_connects);
                handle_worker_connect(
                    msg,
                    &mut self.workers,
                    connects,
                    &mut self.handles,
                    self.ctx,
                )
                .await?;
                Ok(PhaseWait::Continue)
            }
            None => panic!("Job message channel closed: {}", self.ctx.config.id),
        }
    }

    async fn handle_task_wait_message(
        &mut self,
        v: Option<JobMessage>,
    ) -> Result<PhaseWait, StateError> {
        match v {
            Some(JobMessage::WorkerInitializationComplete {
                worker_id,
                success,
                error_message,
            }) => self.record_worker_initialization(worker_id, success, error_message),
            Some(JobMessage::TaskStarted {
                task_id,
                subtask_idx,
                ..
            }) => {
                self.started_tasks.insert((task_id, subtask_idx));
                Ok(PhaseWait::Continue)
            }
            Some(JobMessage::RunningMessage(RunningMessage::TaskFailed(event))) => self
                .ctx
                .handle_task_error(Box::new(Scheduling {}), event)
                .await
                .map(PhaseWait::Leave),
            Some(JobMessage::ConfigUpdate(c)) => {
                if let Some(stop) = stop_transition(&c) {
                    return Ok(PhaseWait::Leave(stop));
                }
                check_config_update(self.ctx.execution_selector, &c)?;
                Ok(PhaseWait::Continue)
            }
            Some(msg) => {
                self.ctx.handle(msg)?;
                Ok(PhaseWait::Continue)
            }
            None => panic!("Job queue shutdown"),
        }
    }

    fn record_worker_initialization(
        &mut self,
        worker_id: WorkerId,
        success: bool,
        error_message: Option<String>,
    ) -> Result<PhaseWait, StateError> {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            return Ok(PhaseWait::Continue);
        };
        if success {
            worker.state = WorkerState::Ready;
            info!(
                message = "worker initialization completed successfully",
                job_id = %self.ctx.config.id,
                pipeline_id = *self.ctx.pipeline_info.pipeline_id,
                worker_id = worker_id.0,
            );
            return Ok(PhaseWait::Continue);
        }
        let error = error_message.unwrap_or_else(|| "Unknown error".to_string());
        worker.state = WorkerState::Failed;
        error!(
            message = "worker initialization failed",
            job_id = %self.ctx.config.id,
            pipeline_id = *self.ctx.pipeline_info.pipeline_id,
            worker_id = worker_id.0,
            error = error
        );
        Err(self.retryable(
            "worker initialization failed",
            anyhow!("worker {} initialization failed: {}", worker_id.0, error),
            5,
        ))
    }

    /// What a task-startup timeout means, once the leader has been asked.
    ///
    /// In leader mode a timeout is often a job that failed on startup, and the failure is only
    /// readable from the leader; asking costs one RPC on a path that is already giving up.
    async fn task_startup_timeout(&mut self) -> StateError {
        if let Some((id, addr)) = self.leader_endpoint()
            && let Ok(mut leader_manager) = LeaderManager::connect(
                JobId(self.ctx.config.id.clone()),
                self.ctx.pipeline_info.pipeline_id.clone(),
                self.ctx.status.generation,
                id,
                addr,
                config().controller.connect_timeout.as_deref().copied(),
                self.ctx.execution_selector,
            )
            .await
            && let Ok(status) = leader_manager.poll_leader_status().await
        {
            warn!(
                message = "leader status at task startup timeout",
                job_id = %self.ctx.config.id,
                job_state = status.job_state,
            );
        }
        self.retryable(
            "timed out while waiting for tasks to start",
            anyhow!(
                "timed out after {:?} while waiting for worker startup",
                *config().pipeline.task_startup_time
            ),
            3,
        )
    }

    /// The worker that runs this job's controller, in leader mode.
    fn leader_endpoint(&self) -> Option<(WorkerId, String)> {
        if !self.leader_mode {
            return None;
        }
        self.workers
            .iter()
            .min_by_key(|w| w.0.0)
            .map(|(id, status)| (*id, status.rpc_address.clone()))
    }
}

/// The handover from a started execution to the state that runs it.
impl PhaseContext<'_, '_> {
    /// Whether the restored checkpoint left a two-phase commit to finish.
    ///
    /// Answered from the flag [`Self::prepare_handover`] records rather than from
    /// `committing_state`, which that same handover has by then moved into the job controller.
    pub(crate) fn needs_restored_commits(&self) -> bool {
        self.restored_commits_pending
    }

    /// Records the task count and builds the job controller a non-leader execution runs under.
    ///
    /// Nothing here is irreversible, which is why it happens before the third admission is
    /// taken rather than inside it.
    pub(crate) async fn prepare_handover(&mut self) {
        self.ctx.status.tasks = Some(self.ctx.program.task_count() as i32);
        // Before the controller is built, because building it takes the commits with it.
        self.restored_commits_pending = self.committing_state.is_some();
        if self.leader_mode {
            return;
        }

        let program = Arc::new(self.ctx.program.clone());
        let metrics = if config().controller.metrics.enabled {
            let metrics = JobMetrics::new(program.clone());
            self.ctx
                .metrics
                .write()
                .await
                .insert(self.ctx.config.id.clone(), metrics.clone());
            Some(metrics)
        } else {
            None
        };

        let checkpoint_store = Arc::new(DbCheckpointMetadataStore {
            organization_id: self.ctx.config.organization_id.clone(),
            job_id: self.ctx.config.id.clone(),
            state_backend: self.ctx.execution_selector,
            db: self.ctx.db.clone(),
        });
        let (start_epoch, min_epoch) = self.epochs();
        self.job_controller = Some(JobController::new(
            checkpoint_store,
            self.ctx.config.clone(),
            self.ctx.pipeline_info.pipeline_id.clone(),
            self.ctx.pipeline_info.state_url.clone(),
            self.ctx.status.generation,
            program,
            start_epoch,
            min_epoch,
            std::mem::take(&mut self.started_connects),
            self.committing_state.take(),
            metrics,
        ));
    }

    /// Publishes the restored checkpoint's commits.
    ///
    /// These finish a two-phase commit against the job's sinks: they are visible outside the
    /// cluster and cannot be withdrawn, which is the whole reason this is a region of its own.
    pub(crate) async fn publish_restored_commits(&mut self, a: &Admission) {
        info!(
            job_id = %self.ctx.config.id,
            pipeline_id = *self.ctx.pipeline_info.pipeline_id,
            "restored checkpoint was in committing phase, sending commits"
        );
        let controller = self
            .job_controller
            .as_mut()
            .expect("the handover built a job controller before admitting a commit publication");
        a.effect(
            "publish the restored checkpoint's commits",
            controller.send_commit_messages(),
        )
        .await
        .expect("failed to send commit messages");
    }

    /// The transition out of `Scheduling` for an execution that is up.
    ///
    /// Hands the context back with the error, because a phase that cannot leave has to be able
    /// to fence, and fencing needs the context it was holding.
    pub(crate) async fn into_transition(mut self) -> Result<Transition, (Self, StateError)> {
        let Some((id, addr)) = self.leader_endpoint() else {
            self.ctx.job_controller = self.job_controller.take();
            return Ok(Transition::next(Scheduling {}, RunningState {}));
        };

        self.ctx.job_controller = None;
        let connected = LeaderManager::connect(
            JobId(self.ctx.config.id.clone()),
            self.ctx.pipeline_info.pipeline_id.clone(),
            self.ctx.status.generation,
            id,
            addr.clone(),
            config().controller.connect_timeout.as_deref().copied(),
            self.ctx.execution_selector,
        )
        .await;
        let leader_manager = match connected {
            Ok(m) => m,
            Err(e) => {
                let reason = self.retryable("failed to connect to worker leader", e, 10);
                return Err((self, reason));
            }
        };

        self.ctx.leader_manager = Some(leader_manager);
        self.ctx.status.state_context.leader = Some(LeaderContext {
            worker_id: id,
            rpc_address: addr,
            generation: self.ctx.status.generation,
        });
        Ok(Transition::next(
            Scheduling {},
            LeaderRunning {
                started: Instant::now(),
            },
        ))
    }

    /// The epochs the execution starts from.
    ///
    /// The two controller modes read `ignore_state_before_epoch` differently: in controller
    /// mode it is an epoch threshold, and in leader mode the same field is a generation number
    /// that must not affect the checkpoint epoch.
    pub(crate) fn epochs(&self) -> (u64, u64) {
        let default_epoch = if self.leader_mode {
            0
        } else {
            self.ctx
                .config
                .ignore_state_before_epoch
                .filter(|&t| t > 0)
                .map(|t| (t - 1) as u64)
                .unwrap_or(0)
        };
        let start = self
            .checkpoint_info
            .as_ref()
            .map(|i| i.epoch)
            .unwrap_or(default_epoch);
        let min = self
            .checkpoint_info
            .as_ref()
            .map(|i| i.min_epoch)
            .unwrap_or(default_epoch);
        (start, min)
    }
}
