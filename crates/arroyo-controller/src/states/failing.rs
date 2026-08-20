use tracing::warn;

use super::recovering::Recovering;
use super::{Failed, JobContext, State, StateError, Transition};
use crate::JobConfig;
use crate::states::LeavingForStop;

/// Intermediate state that attempts to cleanly shut down the pipeline before transitioning to Failed.
#[derive(Debug)]
pub struct Failing {}

#[async_trait::async_trait]
impl State for Failing {
    fn name(&self) -> &'static str {
        "Failing"
    }

    /// Stays. The job is already failing; this tears the cluster down — which is what a stop
    /// would do to it — and hands to `Failed`, which is terminal. A stop cannot pre-empt a
    /// failure, and turning this into a `Stopping` would record the job as stopped when it
    /// had in fact failed.
    fn leave_for_stop(self: Box<Self>, _config: &JobConfig) -> LeavingForStop {
        LeavingForStop::Stays(self)
    }

    async fn next(self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        if let Err(e) = Recovering::cleanup(ctx).await {
            warn!(
                message = "failed to gracefully tear down cluster during failure",
                error = format!("{:?}", e),
                job_id = %ctx.config.id,
                pipeline_id = *ctx.pipeline_info.pipeline_id
            );
        }

        Ok(Transition::next(*self, Failed {}))
    }
}
