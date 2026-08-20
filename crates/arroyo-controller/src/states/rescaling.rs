use crate::states::LeavingForStop;
use crate::states::lifecycle::{ConsumptionPoint, leaving};
use crate::{JobConfig, JobMessage, states::stop_if_desired_non_running};

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

    /// Leaves. `Rescaling` initiates the job's final checkpoint on the first turn of its loop,
    /// *after* the consumption point that a stop taken at the state boundary has already been
    /// removed from, and then ends in `Scheduling`. Answering here is what stops a rescale
    /// that an operator has cancelled from taking a checkpoint and starting a new cluster.
    fn leave_for_stop(self: Box<Self>, config: &JobConfig) -> LeavingForStop {
        leaving::leaves_not_running(self, config)
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        let job_id = ctx.config.id.clone();
        let pipeline_id = ctx.pipeline_info.pipeline_id.clone();
        let execution_selector = ctx.execution_selector;

        let mut final_checkpoint_started = false;

        // What the select below parks on so that a lifecycle intent submitted while the final
        // checkpoint is being taken ends the wait. Never ready on the landed M11.T08 mechanism
        // — see `JobContext::lifecycle_wakeup`.
        let wake = ctx.lifecycle_wakeup();

        loop {
            // M11.D39a's second consumption point. A rescale ends in `Scheduling`, which starts
            // a replacement cluster, so a stop decided while this waits must be read here
            // rather than after that. The job controller is borrowed one turn at a time so that
            // this read — which needs the whole context — can happen at all.
            if ctx
                .observe_lifecycle_intent(ConsumptionPoint::InsideInterruptibleWait)?
                .stops()
            {
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
                    if done
                        && ctx.job_controller.as_mut().unwrap().finished()
                        && final_checkpoint_started
                    {
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
                match ctx.job_controller.as_mut().unwrap().checkpoint(true).await {
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

            tokio::select! {
                // The loop reads the job's writer at the top of every turn, so ending the turn
                // is the whole of what this arm has to do.
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
    }
}
