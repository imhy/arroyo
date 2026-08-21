use crate::JobConfig;
use crate::job_controller::{FinishOutcome, WaitError};
use crate::states::LeavingForStop;
use crate::states::StateError;
use crate::states::lifecycle::leaving;
use crate::states::stop_if_desired_non_running;

use super::{Finished, JobContext, State, Transition};

#[derive(Debug)]
pub struct Finishing {}

#[async_trait::async_trait]
impl State for Finishing {
    fn name(&self) -> &'static str {
        "Finishing"
    }

    /// Leaves. The job's sources are exhausted and its workers are supposed to be ending on
    /// their own — but "supposed to" is the whole of it: the body below waits for that with **no
    /// deadline**, so a job whose workers are wedged stays here until something ends the wait.
    /// A stop is an operator saying the workers are not ending, and the only state that can act
    /// on that is `Stopping`, which asks them again and then tears them down.
    ///
    /// This state used to stay, on the argument that `Finished` is where finishing ends anyway.
    /// It is not: `Finished` is where finishing ends when it *ends*. PR #160 review comment
    /// `5362488017`.
    ///
    /// The mapping is `stop_if_desired_non_running!`'s, through [`leaving::leaves_not_running`],
    /// and it is the same one the wait below reaches for a stop decided while it waits — so a
    /// stop taken at this boundary and a stop taken one turn later mean the same thing.
    fn leave_for_stop(self: Box<Self>, config: &JobConfig) -> LeavingForStop {
        leaving::leaves_not_running(self, config)
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        loop {
            let waited = {
                let Some((job_controller, mut wait)) = ctx.controller_and_wait() else {
                    unreachable!("a job reaches `Finishing` only from a controller-mode `Running`")
                };
                job_controller.wait_for_finish(&mut wait).await
            };

            match waited {
                Ok(FinishOutcome::Finished) => return Ok(Transition::next(*self, Finished {})),
                // M11.D39a's second consumption point, answered by the same macro the boundary
                // above answers with. The wait ends on the decision rather than on a message,
                // because under `FencedV2` no message is sent for one — and this wait has no
                // deadline to fall back on.
                //
                // The macro returns for every stop mode; a decision that turns out not to ask
                // for one falls through and the job goes back to finishing under it.
                Ok(FinishOutcome::StopDecided) => {
                    stop_if_desired_non_running!(self, ctx.config);
                }
                Err(WaitError::Refused(refusal)) => return Err(refusal),
                Err(WaitError::Failed(e)) => {
                    return Err(ctx.retryable(
                        self,
                        "failed while waiting for job to finish",
                        e,
                        10,
                    ));
                }
            }
        }
    }
}
