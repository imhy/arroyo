use super::leader_stopping::LeaderStopping;
use super::{JobContext, State, Stopped, Transition};
use crate::JobConfig;
use crate::job_controller::leader_manager::{handle_leader_stopping, leader_stop_escalation};
use crate::states::LeavingForStop;
use crate::states::StateError;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc::{JobState, JobStopMode};

#[derive(Debug)]
pub struct LeaderCheckpointStopping {}

#[async_trait::async_trait]
impl State for LeaderCheckpointStopping {
    fn name(&self) -> &'static str {
        "CheckpointStopping"
    }

    /// Leaves only for a stop that overtakes this one, by the rule
    /// [`leader_stop_escalation`] applies in the wait below.
    ///
    /// `LeaderCheckpointStopping` sends the leader its checkpoint-stop *before* it reaches
    /// that wait, so this is the only point at which an escalation decided while the previous
    /// state was running can be acted on at all: the wait's own consumption point cannot
    /// report an intent the state boundary has already consumed.
    fn leave_for_stop(self: Box<Self>, config: &JobConfig) -> LeavingForStop {
        match leader_stop_escalation(config) {
            Some(stop_behavior) => {
                LeavingForStop::Leaves(Transition::next(*self, LeaderStopping { stop_behavior }))
            }
            None => LeavingForStop::Stays(self),
        }
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        if let Err(e) = ctx
            .leader_manager()
            .stop_leader(JobStopMode::JobStopCheckpoint)
            .await
        {
            return Err(ctx.retryable(self, "failed to send stop message to leader", e, 10));
        }

        let timeout = config().pipeline.checkpoint.timeout.as_ref().map(|t| **t);

        handle_leader_stopping(*self, ctx, JobState::JobStopped, Stopped {}, timeout).await
    }
}
