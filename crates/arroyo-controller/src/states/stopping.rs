use std::time::{Duration, Instant};

use crate::JobConfig;
use crate::job_controller::{FinishOutcome, WaitError};
use crate::states::LeavingForStop;
use crate::states::StateError;
use arroyo_rpc::grpc::rpc::StopMode;
use tokio::time::timeout;
use tracing::{error, info};

use super::{JobContext, State, Stopped, Transition};

const FINISH_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Copy, Clone, Debug)]
pub enum StopBehavior {
    StopJob(StopMode),
    StopWorkers,
}

#[derive(Debug)]
pub struct Stopping {
    pub stop_mode: StopBehavior,
}

#[async_trait::async_trait]
impl State for Stopping {
    fn name(&self) -> &'static str {
        "Stopping"
    }

    /// Stays. This state *is* the stop: leaving it for a stop would be leaving it for itself,
    /// and the behaviour it was constructed with — graceful, immediate, or straight to the
    /// workers — is the one the decision that created it asked for.
    ///
    /// Staying does not discard the stop. It is left standing on the job's writer and offered
    /// again inside the wait below, which is where a job that is being stopped repeatedly is
    /// bounded: `FINISH_TIMEOUT` is one deadline for the whole wait, and reaching it force-stops
    /// the workers.
    fn leave_for_stop(self: Box<Self>, _config: &JobConfig) -> LeavingForStop {
        LeavingForStop::Stays(self)
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        match (ctx.job_controller.is_some(), self.stop_mode) {
            (true, StopBehavior::StopJob(stop_mode)) => {
                if let Err(e) = ctx
                    .job_controller
                    .as_mut()
                    .expect("the arm this is in tested for a job controller")
                    .stop_job(stop_mode)
                    .await
                {
                    return Err(ctx.retryable(self, "failed while stopping job", e, 10));
                }

                info!(
                    msg = "waiting for workers to terminate",
                    job_id = %ctx.config.id,
                    pipeline_id = *ctx.pipeline_info.pipeline_id
                );

                // One deadline for the whole wait rather than one per turn. A stop decided
                // while this one is being carried out sends the wait round again, and a job
                // whose clock restarted every time it was told to stop would never reach the
                // force stop below.
                let deadline = Instant::now() + FINISH_TIMEOUT;
                loop {
                    let waited = {
                        let Some((job_controller, mut wait)) = ctx.controller_and_wait() else {
                            unreachable!("the arm this is in tested for a job controller")
                        };
                        timeout(
                            deadline.saturating_duration_since(Instant::now()),
                            job_controller.wait_for_finish(&mut wait),
                        )
                        .await
                    };

                    match waited {
                        Ok(Ok(FinishOutcome::Finished)) => break,
                        // `Stopping` *is* the stop, and the behaviour it holds is the one the
                        // decision that created it asked for — so a stop decided while it runs
                        // is the stop already in progress. The workers have been told; what
                        // remains is to wait for them, under the same deadline. Leaving here
                        // would record the job `Stopped` while its workers were still running.
                        Ok(Ok(FinishOutcome::StopDecided)) => {}
                        Ok(Err(WaitError::Refused(refusal))) => return Err(refusal),
                        Ok(Err(WaitError::Failed(e))) => {
                            error!(
                                msg = "encountered error while waiting for job to stop gracefully; will try force-stopping",
                                job_id = %ctx.config.id,
                                pipeline_id = *ctx.pipeline_info.pipeline_id,
                                error = e.to_string(),
                            );
                            self.stop_mode = StopBehavior::StopWorkers;
                            return Self::next(self, ctx).await;
                        }
                        Err(_) => {
                            error!(
                                msg = "timed out while waiting for job to stop; will try force-stopping",
                                job_id = %ctx.config.id,
                                pipeline_id = *ctx.pipeline_info.pipeline_id
                            );
                            self.stop_mode = StopBehavior::StopWorkers;
                            return Self::next(self, ctx).await;
                        }
                    }
                }
            }
            (_, StopBehavior::StopWorkers) | (false, _) => {
                if let Err(e) = ctx
                    .scheduler
                    .stop_workers(&ctx.config.id, Some(ctx.status.generation), true)
                    .await
                {
                    return Err(ctx.retryable(self, "failed while stopping workers", e, 20));
                }
            }
        }

        Ok(Transition::next(*self, Stopped {}))
    }
}
