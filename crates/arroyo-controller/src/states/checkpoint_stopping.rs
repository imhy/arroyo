use arroyo_rpc::grpc;
use tracing::debug;

use crate::{JobMessage, states::StateError};

use super::{
    JobContext, State, Stopped, Transition, check_config_update, handle_unhandled_message,
    stopping::{StopBehavior, Stopping},
};

#[derive(Debug)]
pub struct CheckpointStopping {}

#[async_trait::async_trait]
impl State for CheckpointStopping {
    fn name(&self) -> &'static str {
        "CheckpointStopping"
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
                    debug!(
                        job_id = %job_id,
                        pipeline_id = *pipeline_id,
                        "checked checkpoint, got {}, job_controller.finished(): {}, final_checkpoint_started: {}",
                        done,
                        job_controller.finished(),
                        final_checkpoint_started
                    );

                    if done && job_controller.finished() && final_checkpoint_started {
                        return Ok(Transition::next(*self, Stopped {}));
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
                    match c.stop_mode {
                        crate::types::public::StopMode::immediate => {
                            return Ok(Transition::next(
                                *self,
                                Stopping {
                                    stop_mode: StopBehavior::StopJob(
                                        grpc::rpc::StopMode::Immediate,
                                    ),
                                },
                            ));
                        }
                        crate::types::public::StopMode::force => {
                            todo!("implement force stop mode");
                        }
                        _ => {
                            // do nothing
                        }
                    }

                    // After the stop escalation above, deliberately: stopping is how an
                    // operator undoes a refused selector, so a stop is still honoured.
                    // Anything else in an update that changes the backend is not.
                    check_config_update(execution_selector, &c)?;
                }
                msg => {
                    handle_unhandled_message(&job_id, &pipeline_id, msg)?;
                }
            }
        }
    }
}
