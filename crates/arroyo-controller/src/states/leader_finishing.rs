use super::{Finished, JobContext, State, Transition};
use crate::JobConfig;
use crate::job_controller::leader_manager::handle_leader_stopping;
use crate::states::LeavingForStop;
use crate::states::StateError;
use arroyo_rpc::grpc::rpc::JobState;

#[derive(Debug)]
pub struct LeaderFinishing {}

#[async_trait::async_trait]
impl State for LeaderFinishing {
    fn name(&self) -> &'static str {
        "Finishing"
    }

    /// Stays. The job's sources are exhausted and its leader is shutting the workers down;
    /// `Finished` is where that ends, and a stop does not make an ended job end differently.
    /// The wait this delegates to still escalates a stop decided *while* it waits.
    fn leave_for_stop(self: Box<Self>, _config: &JobConfig) -> LeavingForStop {
        LeavingForStop::Stays(self)
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        handle_leader_stopping(*self, ctx, JobState::JobFinished, Finished {}, None).await
    }
}
