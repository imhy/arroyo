use crate::states::LeavingForStop;
use crate::states::lifecycle::{ConsumptionPoint, ObservedIntent, leaving};
use crate::states::recovering::{CleanupFailure, Recovering};
use crate::states::scheduling::Scheduling;
use crate::states::stop_if_desired_non_running;
use crate::types::public::RestartMode;
use crate::{JobConfig, JobMessage};

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

    /// Leaves. `RestartMode::safe` initiates the job's final checkpoint as its very first
    /// statement — before its loop's first consumption point — and `RestartMode::force` tears
    /// the cluster down; both then end in `Scheduling`, which starts a replacement cluster. A
    /// stop consumed at the state boundary is one this state can no longer observe for itself,
    /// so without this the restart would run to completion and the job would be rescheduled.
    fn leave_for_stop(self: Box<Self>, config: &JobConfig) -> LeavingForStop {
        leaving::leaves_not_running(self, config)
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
                    match ctx.observe_lifecycle_intent(ConsumptionPoint::InsideInterruptibleWait)? {
                        // A restart ends in `Scheduling`, which starts a replacement cluster.
                        // A stop decided while the final checkpoint is in flight must not be
                        // read on the far side of that.
                        ObservedIntent::Stop => {
                            stop_if_desired_non_running!(self, &ctx.config);
                        }
                        // The escalation the `ConfigUpdate` arm below makes, by the same rule
                        // and against the configuration the writer has just published: an
                        // operator who asks for a force restart while the safe one's final
                        // checkpoint is in flight is asking not to wait for it, and an adopted
                        // configuration used to reach this wait carrying nothing but whether
                        // the job stopped (PR #160 review comment `5365261487`). The selector
                        // guard that arm makes first is already made here — a configuration
                        // that changes the backend is refused rather than adopted.
                        ObservedIntent::Adopted(_) => {
                            if ctx.config.restart_mode == RestartMode::force {
                                return Ok(Transition::next(
                                    *self,
                                    Restarting {
                                        mode: RestartMode::force,
                                    },
                                ));
                            }
                        }
                        ObservedIntent::Continue => {}
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
                match Recovering::cleanup(ctx).await {
                    Ok(()) => {}
                    // A refusal decided while the teardown waited fails the job from here, as
                    // it would from any other consumption point; the teardown failing is what
                    // this state retries.
                    Err(CleanupFailure::Refused(refusal)) => return Err(refusal),
                    Err(CleanupFailure::Failed(e)) => {
                        return Err(ctx.retryable(
                            self,
                            "failed to tear down existing cluster",
                            e,
                            20,
                        ));
                    }
                }

                Ok(Transition::next(*self, Scheduling {}))
            }
        }
    }
}
