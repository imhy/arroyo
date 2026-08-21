use super::{
    FatalProvenance, JobContext, State, StateError, Transition, compiling::Compiling, fatal,
    state_backoff,
};
use crate::JobConfig;
use crate::job_controller::leader_manager::LeaderManager;
use crate::job_controller::{FinishOutcome, JobController, WaitError};
use crate::states::LeavingForStop;
use crate::states::lifecycle::JobWait;
use arroyo_rpc::config::config;
use arroyo_rpc::errors::ErrorDomain;
use arroyo_rpc::grpc::rpc::{JobState, JobStopMode, StopMode};
use arroyo_rpc::retry;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{info, warn};

/// Why tearing a job's cluster down did not complete.
///
/// Two outcomes rather than one `anyhow::Error`, because they are answered differently: the
/// teardown failing is what the calling state retries, and a refusal is fatal from wherever the
/// job is. A refusal that arrived here as a retryable error would be a job that kept recovering
/// under a configuration that had been refused.
#[derive(Debug)]
pub enum CleanupFailure {
    /// The teardown itself failed after its retries.
    Failed(anyhow::Error),
    /// The job's persisted configuration was refused while the cleanup waited for its workers.
    ///
    /// The wait below is M11.D39a's second consumption point, so a refusal decided while it
    /// runs is consumed *here* and can be reported at no later point: `Recovering` hands to
    /// `Compiling`, whose own boundary read would find the writer had already been asked. Only
    /// logging it would therefore reschedule the job under the row that was refused.
    Refused(StateError),
}

#[derive(Debug)]
pub struct Recovering {
    pub source: anyhow::Error,
    pub reason: String,
    pub domain: ErrorDomain,
}

impl Recovering {
    /// Tries, with increasing levels of force, to tear down the existing cluster.
    ///
    /// # Errors
    ///
    /// The fatal [`StateError`] of a refused configuration, if the job's writer decided one
    /// while this waited. Everything else this can run into is logged and left to the
    /// unconditional teardown in [`Self::cleanup`].
    pub(crate) async fn cleanup_job_controller(
        job_controller: &mut JobController,
        mut wait: JobWait<'_>,
    ) -> Result<(), StateError> {
        // first try to stop it gracefully
        if job_controller.finished() {
            return Ok(());
        }

        // stop the job
        info!(
            message = "stopping job",
            job_id = %wait.config().id,
            pipeline_id = wait.pipeline_id()
        );
        let start = Instant::now();
        match job_controller.stop_job(StopMode::Immediate).await {
            Ok(_) => {
                match timeout(
                    Duration::from_secs(5),
                    job_controller.wait_for_finish(&mut wait),
                )
                .await
                {
                    Ok(Ok(FinishOutcome::Finished)) => {
                        info!(
                            message = "job stopped",
                            job_id = %wait.config().id,
                            pipeline_id = wait.pipeline_id(),
                            duration = start.elapsed().as_secs_f32()
                        );
                    }
                    // A stop the job's writer decided while this cleanup was carrying one out.
                    // It has been published into the job's configuration, so `Compiling` —
                    // which `Recovering` hands to — answers it before anything is rescheduled.
                    // The teardown below runs either way.
                    Ok(Ok(FinishOutcome::StopDecided)) => {
                        info!(
                            message = "the job's lifecycle writer decided a stop while the job \
                                       was recovering; tearing the cluster down",
                            job_id = %wait.config().id,
                            pipeline_id = wait.pipeline_id()
                        );
                    }
                    Ok(Err(WaitError::Refused(refusal))) => return Err(refusal),
                    Ok(Err(WaitError::Failed(e))) => {
                        warn!(
                            message = "failed while waiting for the job to stop",
                            error = format!("{:?}", e),
                            job_id = %wait.config().id,
                            pipeline_id = wait.pipeline_id(),
                        );
                    }
                    Err(_) => {
                        // Timed out. The teardown below is what stops it.
                    }
                }
            }
            Err(e) => {
                warn!(
                    message = "failed to stop job",
                    error = format!("{:?}", e),
                    job_id = %wait.config().id,
                    pipeline_id = wait.pipeline_id(),
                );
            }
        }

        Ok(())
    }

    pub async fn cleanup_leader(
        leader_manager: &mut LeaderManager,
        job_id: &str,
        pipeline_id: &str,
    ) {
        let status =
            match timeout(Duration::from_secs(30), leader_manager.poll_leader_status()).await {
                Ok(Ok(status)) => status,
                Ok(Err(e)) => {
                    warn!(
                        %job_id,
                        pipeline_id,
                        error =? e,
                        "failed to get leader status while recovering"
                    );
                    return;
                }
                Err(e) => {
                    warn!(
                        %job_id,
                        pipeline_id,
                        error =? e,
                        "timed out polling leader status while recovering"
                    );
                    return;
                }
            };

        let expected_state = match JobState::try_from(status.job_state) {
            Ok(JobState::JobFailed) => {
                return;
            }
            Ok(JobState::JobUnknown) | Err(_) => {
                warn!(
                    %job_id,
                    pipeline_id,
                    "received unknown job state {} while cleaning job",
                    status.job_state
                );
                return;
            }
            Ok(JobState::JobInitializing) => {
                warn!(%job_id, pipeline_id, "job is in initializing while cleaning");
                return;
            }
            Ok(JobState::JobRunning) => {
                // shutdown
                info!(
                    %job_id,
                    pipeline_id, "job is still running in recovering, shutting down"
                );
                if let Err(e) = leader_manager
                    .stop_leader(JobStopMode::JobStopImmediate)
                    .await
                {
                    warn!(%job_id, pipeline_id, error =? e, "failed to stop leader");
                    return;
                }
                JobState::JobStopped
            }
            Ok(JobState::JobStopping) => {
                // wait for job to be stopped
                JobState::JobStopped
            }
            Ok(JobState::JobStopped) => {
                return;
            }
            Ok(JobState::JobFinishing) => {
                // wait for job to be finished
                JobState::JobFinished
            }
            Ok(JobState::JobFinished) => {
                return;
            }
            Ok(JobState::JobFailing) => {
                info!(
                    %job_id,
                    pipeline_id, "job is failing in recovering, shutting down"
                );
                if let Err(e) = leader_manager
                    .stop_leader(JobStopMode::JobStopImmediate)
                    .await
                {
                    warn!(%job_id, pipeline_id, error =? e, "failed to stop leader");
                    return;
                }
                JobState::JobFailed
            }
        };

        if let Err(e) = timeout(
            Duration::from_secs(60),
            leader_manager.wait_for_state(expected_state),
        )
        .await
        {
            warn!(
                %job_id,
                pipeline_id,
                error = ?e,
                ?expected_state,
                "timed out waiting for state during cleanup"
            );
        }
    }

    async fn tear_down_workers<'a>(ctx: &mut JobContext<'a>) -> anyhow::Result<()> {
        if ctx
            .scheduler
            .workers_for_job(&ctx.config.id, Some(ctx.status.generation))
            .await?
            .is_empty()
        {
            return Ok(());
        }

        info!(
            message = "tearing down workers",
            job_id = %ctx.config.id,
            pipeline_id = *ctx.pipeline_info.pipeline_id
        );

        ctx.scheduler
            .stop_workers(&ctx.config.id, Some(ctx.status.generation), true)
            .await
    }

    pub async fn cleanup<'a>(ctx: &mut JobContext<'a>) -> Result<(), CleanupFailure> {
        // attempt to shutdown the job cleanly
        match (ctx.job_controller.is_some(), ctx.leader_manager.is_some()) {
            (true, false) => {
                let Some((job_controller, wait)) = ctx.controller_and_wait() else {
                    unreachable!("the arm this is in tested for a job controller")
                };
                Self::cleanup_job_controller(job_controller, wait)
                    .await
                    .map_err(CleanupFailure::Refused)?;
            }
            (false, true) => {
                Self::cleanup_leader(
                    ctx.leader_manager
                        .as_mut()
                        .expect("the arm this is in tested for a leader manager"),
                    &ctx.config.id,
                    &ctx.pipeline_info.pipeline_id,
                )
                .await
            }
            (true, true) => unreachable!("both job controller and leader manager are set!"),
            (false, false) => {
                // somehow we got here before scheduling set the job controller / leader manager
            }
        };

        // clear workers
        ctx.leader_manager = None;
        ctx.status.state_context.leader = None;
        ctx.job_controller = None;

        // then tear down the workers
        let torn_down = retry!(
            Self::tear_down_workers(ctx).await,
            10,
            Duration::from_millis(200),
            Duration::from_secs(10),
            |e| warn!(
                job_id = %ctx.config.id,
                pipeline_id = *ctx.pipeline_info.pipeline_id,
                error =? e,
                "failed to tear down cluster"
            )
        );
        torn_down.map_err(CleanupFailure::Failed)?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl State for Recovering {
    fn name(&self) -> &'static str {
        "Recovering"
    }

    /// Stays. Everything `Recovering` does is the tear-down a stop performs — it backs off,
    /// cleans up the job controller or the leader, and stops the workers — and it is entered
    /// only from a failure, where no final checkpoint is possible and every stop mode would
    /// map to an immediate one anyway. What it hands to is `Compiling`, which answers the same
    /// stop before `Scheduling` starts anything.
    ///
    /// Precisely: the stop is *published* into `ctx.config` and stands until a state reads it.
    /// The cleanup's own wait is a consumption point and takes it — which is what ends that wait
    /// early rather than five seconds later — so `Compiling` is not asked, and the state that
    /// answers is `Scheduling`, whose first statement in either lifecycle mode is a read of
    /// `ctx.config.stop_mode` (`stop_if_desired_non_running!` on the landed path,
    /// `PhaseContext::stop_if_desired` on the D39a one). Nothing between here and there is
    /// irreversible: `Compiling` writes nothing and starts nothing.
    /// `recovering_hands_a_stop_it_was_given_to_the_state_that_answers_it` runs that chain.
    ///
    /// It is therefore not a state that *misses* a stop; it is a state that reaches the same
    /// place by its own route. The one thing a stop changes is how long that takes: the restart
    /// backoff runs first, and a job told to stop waits it out. Both waits inside the cleanup
    /// are bounded — five seconds for the job's workers, sixty for a leader's state — so a stop
    /// decided during one is a delay and never a hang.
    fn leave_for_stop(self: Box<Self>, _config: &JobConfig) -> LeavingForStop {
        LeavingForStop::Stays(self)
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        let pipeline_config = &config().pipeline;

        // only allow one restart for preview pipelines
        if ctx.config.ttl.is_some() {
            return Err(fatal(
                "Job encountered a fatal error; see worker logs for details",
                self.source,
            ));
        }

        if pipeline_config.allowed_restarts != -1
            && ctx.status.restarts >= pipeline_config.allowed_restarts
        {
            return Err(StateError::FatalError {
                message: format!("Exhausted retries: {}", self.reason),
                domain: self.domain,
                source: self.source,
                // Exhausting the restart budget is a fact about how often the job has failed,
                // not about the row it is configured by.
                provenance: FatalProvenance::Unrelated,
            });
        }

        // backoff
        state_backoff(
            ctx.status.restarts as usize,
            &ctx.config.id,
            &ctx.pipeline_info.pipeline_id,
        )
        .await;

        info!(
            job_id = %ctx.config.id,
            pipeline_id = *ctx.pipeline_info.pipeline_id,
            retries_remaining = pipeline_config.allowed_restarts - ctx.status.restarts,
            "recovering pipeline"
        );

        match Self::cleanup(ctx).await {
            Ok(()) => Ok(Transition::next(*self, Compiling)),
            Err(CleanupFailure::Refused(refusal)) => Err(refusal),
            Err(CleanupFailure::Failed(e)) => {
                Err(ctx.retryable(self, "failed to tear down existing cluster", e, 20))
            }
        }
    }
}
