use super::{JobContext, State, StateError, Transition, scheduling::Scheduling};
use crate::JobConfig;
use crate::job_controller::leader_manager::handle_leader_stopping;
use crate::states::LeavingForStop;
use crate::states::lifecycle::leaving;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc::{JobState, JobStopMode};

#[derive(Debug)]
pub struct LeaderRescaling {}

#[async_trait::async_trait]
impl State for LeaderRescaling {
    fn name(&self) -> &'static str {
        "Rescaling"
    }

    /// Leaves. `LeaderRescaling` has no consumption point and no message loop at all: it sends
    /// the leader a checkpoint-stop and waits for the workers to stop, then goes to
    /// `Scheduling`. So this is the only point at which it can learn that the job it is about
    /// to rescale has been told to stop, and without it a cancelled rescale would take a final
    /// checkpoint and start a replacement cluster.
    fn leave_for_stop(self: Box<Self>, config: &JobConfig) -> LeavingForStop {
        leaving::leaves_running_under_leader(self, config)
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        if let Err(e) = ctx
            .leader_manager()
            .stop_leader(JobStopMode::JobStopCheckpoint)
            .await
        {
            return Err(ctx.retryable(
                self,
                "failed to send checkpoint-stop to leader for rescaling",
                e,
                10,
            ));
        }

        let timeout = config().pipeline.checkpoint.timeout.as_ref().map(|t| **t);
        handle_leader_stopping(*self, ctx, JobState::JobStopped, Scheduling {}, timeout).await
    }
}
