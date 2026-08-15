use crate::JobMessage;
use crate::states::leader_stopping::{LeaderStopBehavior, LeaderStopping};
use crate::states::recovering::Recovering;
use crate::states::{
    JobContext, State, StateError, Transition, TransitionTo, check_config_update,
    controller_job_failure,
};
use crate::types::public::StopMode as SqlStopMode;
use anyhow::{anyhow, bail};
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::grpc::rpc::job_status_grpc_client::JobStatusGrpcClient;
use arroyo_rpc::grpc::rpc::{JobState, JobStatusReq, JobStopMode, StopJobReq};
use arroyo_rpc::identity::InjectWorkerId;
use arroyo_rpc::state_backend::{StateBackendSelector, validate_leader_selector};
use arroyo_rpc::{job_status_client, retry};
use arroyo_types::{JobId, PipelineId, WorkerId};
use std::time::{Duration, Instant};
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;
use tracing::{info, warn};

pub struct LeaderManager {
    leader_client: JobStatusGrpcClient<InterceptedService<Channel, InjectWorkerId>>,
    pub job_id: JobId,
    pub pipeline_id: PipelineId,
    pub generation: u64,
    /// The selector the controller is administering this job with, checked against the
    /// one the live leader reports on every status poll.
    execution_selector: StateBackendSelector,
    pub last_heartbeat: Instant,
}

impl LeaderManager {
    /// Connects to a job's live worker leader, and refuses to attach to one that is
    /// running the job on a different state backend.
    ///
    /// `execution_selector` is what this controller believes the job's backend is. A
    /// controller that has just restarted rebuilt that belief from persisted state, while
    /// the leader has been running with the value it was handed in its own
    /// `StartExecutionReq` — so the agreement is checked here, before the manager is
    /// handed to any state, rather than left to be discovered by a checkpoint, a cleanup,
    /// or a restore that acts under the wrong one.
    ///
    /// # Errors
    ///
    /// Returns an error if the leader cannot be reached, if it is running a different job
    /// or generation, or if it reports a different state backend.
    pub async fn connect(
        job_id: JobId,
        pipeline_id: PipelineId,
        generation: u64,
        worker_id: WorkerId,
        address: String,
        connect_timeout: Option<Duration>,
        execution_selector: StateBackendSelector,
    ) -> anyhow::Result<Self> {
        let leader_client = retry!(
            job_status_client(
                "controller",
                &config().worker.tls,
                worker_id,
                address.clone(),
                connect_timeout,
            )
            .await,
            5,
            Duration::from_millis(100),
            Duration::from_secs(2),
            |e| warn!(
                job_id = %job_id,
                pipeline_id = *pipeline_id.0,
                message = "failed to connect to worker leader",
                error = ?e
            )
        )?;

        let mut this = Self {
            job_id,
            pipeline_id,
            generation,
            execution_selector,
            leader_client,
            last_heartbeat: Instant::now(),
        };

        // The handshake. `poll_leader_status` is what validates the reported selector, so
        // one poll here means no caller can ever hold a manager for a leader that has not
        // agreed at least once.
        this.poll_leader_status().await?;

        Ok(this)
    }

    pub async fn poll_leader_status(&mut self) -> anyhow::Result<rpc::JobStatus> {
        let response = retry!(
            self.leader_client
                .get_job_status(JobStatusReq {
                    job_id: self.job_id.to_string(),
                    generation: self.generation,
                })
                .await,
            5,
            Duration::from_millis(100),
            Duration::from_secs(2),
            |e| warn!(
                job_id = %self.job_id,
                pipeline_id = *self.pipeline_id.0,
                message = "failed to poll for job status",
                error = ?e
            )
        )?
        .into_inner();

        if response.job_id != *self.job_id.0 {
            bail!(
                "leader returned job status for wrong job: expected {}, got {}",
                self.job_id,
                response.job_id
            );
        }

        if response.generation != self.generation {
            bail!(
                "leader returned job status for wrong run: expected {}, got {}",
                self.generation,
                response.generation
            );
        }

        // The live leader owns the selector it was started with; this controller's value
        // was recovered from persisted state and may have been rebuilt after a restart.
        // Checked on every poll rather than only at connect, because a leader that has
        // been replaced under a running manager is exactly the case a one-off check
        // would miss.
        validate_leader_selector(
            &self.job_id,
            self.execution_selector,
            &response.state_backend,
        )?;

        let status = response
            .job_status
            .ok_or_else(|| anyhow!("leader returned empty job status"))?;

        self.last_heartbeat = Instant::now();

        Ok(status)
    }

    pub async fn stop_leader(&mut self, stop_mode: JobStopMode) -> anyhow::Result<()> {
        info!(
            message = "sending stop request to leader",
            job_id = %self.job_id,
            pipeline_id = *self.pipeline_id.0,
            stop_mode = ?stop_mode,
        );

        self.leader_client
            .stop_job(StopJobReq {
                stop_mode: stop_mode as i32,
            })
            .await?;

        Ok(())
    }

    pub async fn wait_for_state(&mut self, expected: rpc::JobState) -> anyhow::Result<()> {
        let mut timer = tokio::time::interval(Duration::from_millis(200));
        loop {
            let status = self.poll_leader_status().await?;

            let state = JobState::try_from(status.job_state)
                .map_err(|e| anyhow!("received invalid job state from leader: {e}"))?;

            if state == expected {
                return Ok(());
            }

            match state {
                JobState::JobUnknown => bail!("received unknown job status"),
                JobState::JobInitializing
                | JobState::JobRunning
                | JobState::JobStopping
                | JobState::JobFinishing
                | JobState::JobFailing => {
                    // non-terminal states, continue waiting
                }
                JobState::JobStopped | JobState::JobFinished | JobState::JobFailed => {
                    bail!(
                        "reached unexpected terminal state {:?} while waiting for {:?}",
                        state,
                        expected
                    );
                }
            }

            timer.tick().await;
        }
    }
}

pub async fn handle_leader_stopping<'a, S, T>(
    state: S,
    ctx: &mut JobContext<'a>,
    expected_state: rpc::JobState,
    next: T,
    timeout: Option<Duration>,
) -> Result<Transition, StateError>
where
    S: State,
    T: State,
    S: TransitionTo<T>,
    S: TransitionTo<LeaderStopping>,
    S: TransitionTo<Recovering>,
{
    let started = Instant::now();

    loop {
        let timeout = timeout
            .map(|t| (started + t).saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::MAX);

        tokio::select! {
            msg = ctx.rx.recv() => {
                match msg {
                    Some(JobMessage::ConfigUpdate(c)) => {
                        let next = match c.stop_mode {
                            SqlStopMode::force => {
                                Some(LeaderStopBehavior::StopWorkers)
                            }
                            SqlStopMode::immediate => {
                                Some(LeaderStopBehavior::StopJob(JobStopMode::JobStopImmediate))
                            }
                            SqlStopMode::graceful => {
                                Some(LeaderStopBehavior::StopJob(JobStopMode::JobStopGraceful))
                            }
                            SqlStopMode::none | SqlStopMode::checkpoint => {
                                // do nothing
                                None
                            }
                        };

                        if let Some(stop_behavior) = next {
                            return Ok(Transition::next(state, LeaderStopping {
                                stop_behavior
                            }));
                        }

                        // After the stop decision above, deliberately: stopping is how an
                        // operator undoes a refused selector. Nothing else in an update
                        // that changes the backend may be taken.
                        check_config_update(ctx.execution_selector, &c)?;
                    }
                    Some(msg) => {
                        // Routed rather than logged here so a refused configuration
                        // reaches the one place that acts on it.
                        ctx.handle(msg)?;
                    }
                    None => {
                        panic!("job queue shut down");
                    }
                }
            }
            resp = ctx.leader_manager.as_mut().expect("leader manager not initialized").wait_for_state(expected_state) => {
                if let Err(e) = resp {
                return ctx.handle_job_failure(state, controller_job_failure(
                    format!("failed while taking final checkpoint: {:?}", e),
                    rpc::ErrorDomain::Internal,
                    rpc::RetryHint::WithBackoff,
                )).await;
                }
                return Ok(Transition::next(
                    state,
                    next,
                ));
            }
            _ = tokio::time::sleep(timeout) => {
                return ctx.handle_job_failure(state, controller_job_failure(
                    "timed out while taking final checkpoint".to_string(),
                    rpc::ErrorDomain::Internal,
                    rpc::RetryHint::WithBackoff,
                )).await;
            }
        }
    }
}
