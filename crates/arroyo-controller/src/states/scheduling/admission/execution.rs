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
//!
//! # Why each wait also selects on the job's intent mailbox
//!
//! Under M11.D39a the configuration poll *submits* rather than sends: it leaves a versioned
//! intent and returns, and nothing is put in the job's channel. A wait that selected only on
//! that channel would therefore not turn when a stop or a refusal was decided — it would turn
//! when a worker registered, or when the startup budget ran out. Reading the mailbox at the
//! top of each turn is necessary but not sufficient, because "the top of each turn" is only
//! reached when something ended the previous one. So the mailbox's own wake is an arm of both
//! selects, and the deadline for observing a decision becomes the submission rather than the
//! timeout.
//!
//! For a job built in the pre-flag-day peer mode that arm is a future that never completes:
//! there is no mailbox, the poll publishes into the job's channel, and this wait already
//! selects on the channel.

use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc::{JobFailure, JobState};
use arroyo_rpc::worker_types::RunningMessage;
use arroyo_types::{JobId, WorkerId};
use tracing::{error, info, warn};

use super::super::{Scheduling, WorkerState, handle_worker_connect};
use super::{PhaseContext, PhaseWait, stop_transition};
use crate::JobMessage;
use crate::job_controller::leader_manager::LeaderManager;
use crate::states::{StateError, check_config_update};

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
        // Taken before the select rather than inside it: the other arm borrows the job's
        // context mutably, and this handle owns what it watches instead of borrowing it.
        let wake = self.ctx.lifecycle_wakeup();
        tokio::select! {
            val = self.ctx.rx.recv() => self.handle_worker_wait_message(val).await,
            // The loop reads the mailbox at the top of every turn, so ending the turn is the
            // whole of what this arm has to do.
            _ = wake.notified() => Ok(PhaseWait::Continue),
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

    /// One turn of the wait for the registered workers' outbound channels to open.
    ///
    /// **PR #160 review comment `5384611151`.** This was a bare
    /// `for h in take(&mut self.handles) { h.await }` — the one wait in the module whose
    /// subject *is* the interruptible waits that could not be interrupted.
    /// [`wait_for_workers`](super::super::phases::wait_for_workers) enters it as soon as the
    /// last registration makes capacity sufficient, and each setup task makes up to three
    /// 90-second connection attempts, so a stop or a refusal decided while it ran went unseen
    /// for as long as 270 seconds.
    ///
    /// A *turn* rather than the whole wait, so the caller's loop reads the job's writer
    /// between channels exactly as it does between worker messages and task messages.
    /// `&mut JoinHandle` is a cancel-safe future and `JoinHandle` is `Unpin`: a turn the wake
    /// ends leaves the handle in place, and the next turn polls the same task again rather
    /// than restarting or detaching it.
    ///
    /// The channels are awaited in the order they were spawned, which is the order the
    /// previous `for` loop used — so which failure is reported first is unchanged.
    pub(crate) async fn await_worker_channels(&mut self) -> Result<PhaseWait, StateError> {
        // Taken before the borrow below, for the reason `await_message_from_workers` gives.
        let wake = self.ctx.lifecycle_wakeup();
        let joined = {
            let Some(handle) = self.handles.first_mut() else {
                return Ok(PhaseWait::Continue);
            };
            tokio::select! {
                joined = handle => Some(joined),
                // The caller reads the job's writer at the top of every turn, so ending the
                // turn is the whole of what this arm has to do. The handle is left untouched.
                _ = wake.notified() => None,
            }
        };
        let Some(joined) = joined else {
            return Ok(PhaseWait::Continue);
        };
        self.handles.remove(0);
        match joined {
            Ok(()) => Ok(PhaseWait::Continue),
            Err(e) => Err(self.retryable("Failed to start cluster for pipeline", e.into(), 10)),
        }
    }

    /// Whether every registered worker's outbound channel is open.
    pub(crate) fn worker_channels_are_open(&self) -> bool {
        self.handles.is_empty()
    }

    /// One turn of the wait for the started execution's tasks to report in.
    pub(crate) async fn await_message_from_tasks(&mut self) -> Result<PhaseWait, StateError> {
        let timeout = self.remaining(*config().pipeline.task_startup_time);
        let wake = self.ctx.lifecycle_wakeup();
        tokio::select! {
            v = self.ctx.rx.recv() => self.handle_task_wait_message(v).await,
            _ = wake.notified() => Ok(PhaseWait::Continue),
            _ = tokio::time::sleep(timeout) => self.task_startup_timeout().await,
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
    ///
    /// The answer decides the outcome, and it has to: a leader that reports the job failing is
    /// giving the *cause*, of which the timeout is only the symptom. Reporting the symptom
    /// discards the failure's error domain, its retry hint, and with the hint the difference
    /// between a job that should be recovered and a job that must be failed — and it does so on
    /// a shorter retry budget than the missing-payload case the landed body gives. So the
    /// failure is handed to [`JobContext::handle_job_failure`](crate::states::JobContext::handle_job_failure),
    /// the same function the landed `Scheduling::next` calls for the same status, which is what
    /// makes the two routes' answers the same answer rather than two that happen to agree.
    ///
    /// The transition it can produce travels as [`PhaseWait::Leave`]: a job recovering from a
    /// startup failure leaves `Scheduling` for `Recovering`, which is not something a
    /// [`StateError`] can express and is why this returns a wait outcome rather than an error.
    pub(crate) async fn task_startup_timeout(&mut self) -> Result<PhaseWait, StateError> {
        match self.leader_startup_verdict().await {
            LeaderVerdict::Failed(failure) => self
                .ctx
                .handle_job_failure(Scheduling {}, failure)
                .await
                .map(PhaseWait::Leave),
            LeaderVerdict::FailedWithoutReason => Err(self.retryable(
                "leader reported failing status without failure payload",
                anyhow!("missing job failure"),
                10,
            )),
            LeaderVerdict::NoFailure => Err(self.retryable(
                "timed out while waiting for tasks to start",
                anyhow!(
                    "timed out after {:?} while waiting for worker startup",
                    *config().pipeline.task_startup_time
                ),
                3,
            )),
        }
    }

    /// Asks the job's leader why its tasks never started.
    ///
    /// [`LeaderVerdict::NoFailure`] covers every way the question goes unanswered as well as a
    /// leader that answers with a job that is not failing: an unreachable leader, a controller
    /// that is not in leader mode at all, and a status whose `job_state` is not a value this
    /// controller knows. All of them mean the same thing here — nothing better than the
    /// timeout is known — and treating an unreadable status as a failure would fail jobs for a
    /// leader the controller could not talk to.
    async fn leader_startup_verdict(&mut self) -> LeaderVerdict {
        let Some((id, addr)) = self.leader_endpoint() else {
            return LeaderVerdict::NoFailure;
        };
        let connected = LeaderManager::connect(
            JobId(self.ctx.config.id.clone()),
            self.ctx.pipeline_info.pipeline_id.clone(),
            self.ctx.status.generation,
            id,
            addr,
            config().controller.connect_timeout.as_deref().copied(),
            self.ctx.execution_selector,
        )
        .await;
        let Ok(mut leader_manager) = connected else {
            return LeaderVerdict::NoFailure;
        };
        let Ok(status) = leader_manager.poll_leader_status().await else {
            return LeaderVerdict::NoFailure;
        };
        warn!(
            message = "leader status at task startup timeout",
            job_id = %self.ctx.config.id,
            job_state = status.job_state,
        );
        match JobState::try_from(status.job_state) {
            Ok(JobState::JobFailing | JobState::JobFailed) => match status.job_failure {
                Some(failure) => LeaderVerdict::Failed(failure),
                None => LeaderVerdict::FailedWithoutReason,
            },
            Ok(_) => LeaderVerdict::NoFailure,
            Err(e) => {
                warn!(
                    message = "leader returned invalid job state before task startup timeout",
                    error = format!("{:?}", e),
                    job_id = %self.ctx.config.id,
                    pipeline_id = *self.ctx.pipeline_info.pipeline_id,
                );
                LeaderVerdict::NoFailure
            }
        }
    }

    /// The worker that runs this job's controller, in leader mode.
    pub(super) fn leader_endpoint(&self) -> Option<(WorkerId, String)> {
        if !self.leader_mode {
            return None;
        }
        self.workers
            .iter()
            .min_by_key(|w| w.0.0)
            .map(|(id, status)| (*id, status.rpc_address.clone()))
    }
}

/// What the job's worker leader says about an execution whose tasks never started.
///
/// Three outcomes rather than two, because "the leader says the job failed" and "the leader
/// says the job failed and here is why" lead to different retry budgets on the landed path,
/// and this half must not quietly merge them.
enum LeaderVerdict {
    /// Nothing better than the timeout is known.
    NoFailure,
    /// The leader reports the job failing or failed, and this is the failure.
    Failed(JobFailure),
    /// The leader reports the job failing or failed and carried no failure to say why. The
    /// status is worth another attempt: the payload may simply not have been written yet.
    FailedWithoutReason,
}
