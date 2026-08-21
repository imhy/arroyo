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

    /// Stays. The job's sources are exhausted and its leader is shutting the workers down, and
    /// the wait this delegates to is where a stop is answered: `handle_leader_stopping` reads
    /// the job's writer at the top of every turn and escalates through `leader_stop_escalation`,
    /// which is the one mapping leader mode has. Answering here as well would be a second copy
    /// of it, and the two would come to disagree.
    ///
    /// Staying is what routes the stop there, not what discards it. The stop the boundary
    /// consumed is left standing on the job's writer, so the wait's *first* turn reports it —
    /// before that it reported only stops decided after the wait had begun, and a stop decided
    /// between two states reached nothing at all. That wait has no deadline
    /// (`handle_leader_stopping(.., None)`), so what it reached was a job an operator could not
    /// stop: PR #160 review comment `5362488017`.
    ///
    /// The mode survives the trip, which is why this is better than leaving here: a `checkpoint`
    /// stop is what a finishing job is already doing, and `leader_stop_escalation` keeps it
    /// finishing instead of cutting it short.
    fn leave_for_stop(self: Box<Self>, _config: &JobConfig) -> LeavingForStop {
        LeavingForStop::Stays(self)
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        handle_leader_stopping(*self, ctx, JobState::JobFinished, Finished {}, None).await
    }
}
