use crate::{JobMessage, states::stop_if_desired_non_running};

use super::{
    JobContext, State, StateError, Transition, check_config_update, handle_unhandled_message,
    scheduling::Scheduling,
};

#[derive(Debug)]
pub struct Rescaling {}

#[async_trait::async_trait]
impl State for Rescaling {
    fn name(&self) -> &'static str {
        "Rescaling"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        let job_id = ctx.config.id.clone();
        let pipeline_id = ctx.pipeline_info.pipeline_id.clone();
        let execution_selector = ctx.execution_selector;
        let job_controller = ctx.job_controller.as_mut().unwrap();

        let mut final_checkpoint_started = false;

        loop {
            match job_controller.checkpoint_finished().await {
                Ok(done) => {
                    if done && job_controller.finished() && final_checkpoint_started {
                        return Ok(Transition::next(*self, Scheduling {}));
                    }
                }
                Err(e) => {
                    return Err(ctx.retryable(
                        self,
                        "failed while monitoring final checkpoint",
                        e,
                        10,
                    ));
                }
            }

            if !final_checkpoint_started {
                match job_controller.checkpoint(true).await {
                    Ok(started) => final_checkpoint_started = started,
                    Err(e) => {
                        return Err(ctx.retryable(
                            self,
                            "failed to initiate final checkpoint",
                            e,
                            10,
                        ));
                    }
                }
            }

            match ctx.rx.recv().await.expect("channel closed while receiving") {
                JobMessage::RunningMessage(msg) => {
                    if let Err(e) = job_controller.handle_message(msg).await {
                        return Err(ctx.retryable(
                            self,
                            "failed while waiting for job finish",
                            e,
                            10,
                        ));
                    }
                }
                JobMessage::ConfigUpdate(c) => {
                    stop_if_desired_non_running!(self, &c);
                    // A rescale ends in `Scheduling`, which starts workers from the job's
                    // selector; nothing but a stop may be taken from an update that
                    // changes it.
                    check_config_update(execution_selector, &c)?;
                }
                msg => {
                    handle_unhandled_message(&job_id, &pipeline_id, msg)?;
                }
            }
        }
    }
}
