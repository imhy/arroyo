use crate::JobMessage;
use crate::states::recovering::Recovering;
use crate::states::scheduling::Scheduling;
use crate::states::stop_if_desired_non_running;
use crate::types::public::RestartMode;

use super::{
    JobContext, State, StateError, Transition, check_config_update, handle_unhandled_message,
};

#[derive(Debug)]
pub struct Restarting {
    pub mode: RestartMode,
}

#[async_trait::async_trait]
impl State for Restarting {
    fn name(&self) -> &'static str {
        "Restarting"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        let job_id = ctx.config.id.clone();
        let pipeline_id = ctx.pipeline_info.pipeline_id.clone();
        let execution_selector = ctx.execution_selector;
        let job_controller = ctx.job_controller.as_mut().unwrap();

        match self.mode {
            RestartMode::safe => {
                if let Err(e) = job_controller.checkpoint(true).await {
                    return Err(ctx.retryable(self, "failed to initiate final checkpoint", e, 10));
                }

                loop {
                    match job_controller.checkpoint_finished().await {
                        Ok(done) => {
                            if done && job_controller.finished() {
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
                            // Before the force-restart branch below: a restart reschedules
                            // the job, and it must not be rescheduled from a configuration
                            // that changes the backend its state was written with.
                            check_config_update(execution_selector, &c)?;
                            if c.restart_mode == RestartMode::force {
                                return Ok(Transition::next(
                                    *self,
                                    Restarting {
                                        mode: RestartMode::force,
                                    },
                                ));
                            }
                            stop_if_desired_non_running!(self, &c);
                        }
                        msg => {
                            handle_unhandled_message(&job_id, &pipeline_id, msg)?;
                        }
                    }
                }
            }
            RestartMode::force => {
                if let Err(e) = Recovering::cleanup(ctx).await {
                    return Err(ctx.retryable(self, "failed to tear down existing cluster", e, 20));
                }

                Ok(Transition::next(*self, Scheduling {}))
            }
        }
    }
}
