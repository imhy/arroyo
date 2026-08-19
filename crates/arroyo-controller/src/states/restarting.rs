use crate::JobMessage;
use crate::states::lifecycle::ConsumptionPoint;
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

        match self.mode {
            RestartMode::safe => {
                // What the select below parks on so that a lifecycle intent submitted while the
                // final checkpoint is being taken ends the wait. Never ready on the landed
                // M11.T08 mechanism — see `JobContext::lifecycle_wakeup`.
                let wake = ctx.lifecycle_wakeup();

                if let Err(e) = ctx.job_controller.as_mut().unwrap().checkpoint(true).await {
                    return Err(ctx.retryable(self, "failed to initiate final checkpoint", e, 10));
                }

                loop {
                    // M11.D39a's second consumption point. The job controller is borrowed one
                    // turn at a time rather than across the loop so that this read — which
                    // needs the whole context — can happen at all.
                    if ctx
                        .observe_lifecycle_intent(ConsumptionPoint::InsideInterruptibleWait)?
                        .stops()
                    {
                        // A restart ends in `Scheduling`, which starts a replacement cluster.
                        // A stop decided while the final checkpoint is in flight must not be
                        // read on the far side of that.
                        stop_if_desired_non_running!(self, &ctx.config);
                    }

                    match ctx
                        .job_controller
                        .as_mut()
                        .unwrap()
                        .checkpoint_finished()
                        .await
                    {
                        Ok(done) => {
                            if done && ctx.job_controller.as_mut().unwrap().finished() {
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

                    tokio::select! {
                        // The loop reads the job's writer at the top of every turn, so ending
                        // the turn is the whole of what this arm has to do.
                        _ = wake.notified() => {}
                        msg = ctx.rx.recv() => {
                            match msg.expect("channel closed while receiving") {
                                JobMessage::RunningMessage(msg) => {
                                    if let Err(e) =
                                        ctx.job_controller.as_mut().unwrap().handle_message(msg).await
                                    {
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
