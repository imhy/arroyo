use std::time::Duration;

use crate::JobConfig;
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
    fn leave_for_stop(self: Box<Self>, _config: &JobConfig) -> LeavingForStop {
        LeavingForStop::Stays(self)
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        match (ctx.job_controller.as_mut(), self.stop_mode) {
            (Some(job_controller), StopBehavior::StopJob(stop_mode)) => {
                if let Err(e) = job_controller.stop_job(stop_mode).await {
                    return Err(ctx.retryable(self, "failed while stopping job", e, 10));
                }

                info!(
                    msg = "waiting for workers to terminate",
                    job_id = %ctx.config.id,
                    pipeline_id = *ctx.pipeline_info.pipeline_id
                );
                match timeout(FINISH_TIMEOUT, job_controller.wait_for_finish(ctx.rx)).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
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
                            msg =
                                "timed out while waiting for job to stop; will try force-stopping",
                            job_id = %ctx.config.id,
                            pipeline_id = *ctx.pipeline_info.pipeline_id
                        );
                        self.stop_mode = StopBehavior::StopWorkers;
                        return Self::next(self, ctx).await;
                    }
                }
            }
            (_, StopBehavior::StopWorkers) | (None, _) => {
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
