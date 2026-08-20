use crate::JobConfig;
use crate::states::LeavingForStop;
use crate::states::StateError;
use crate::states::lifecycle::leaving;

use super::{JobContext, State, Transition, scheduling::Scheduling};

#[derive(Debug)]
pub struct Compiling;

#[async_trait::async_trait]
impl State for Compiling {
    fn name(&self) -> &'static str {
        "Compiling"
    }

    /// Leaves. `Compiling` does nothing irreversible, but the `Scheduling` it hands to
    /// increments and persists the job's generation and starts a cluster, so answering the
    /// stop here rather than one state later is what keeps a job that was told to stop from
    /// being scheduled first.
    fn leave_for_stop(self: Box<Self>, config: &JobConfig) -> LeavingForStop {
        leaving::leaves_not_running(self, config)
    }

    async fn next(self: Box<Self>, _ctx: &mut JobContext) -> Result<Transition, StateError> {
        return Ok(Transition::next(*self, Scheduling {}));
    }
}
