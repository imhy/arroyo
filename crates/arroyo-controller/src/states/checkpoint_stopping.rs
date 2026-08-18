use arroyo_rpc::grpc;
use tracing::debug;

use crate::states::lifecycle::ConsumptionPoint;
use crate::types::public::StopMode;
use crate::{JobConfig, JobMessage, states::StateError};

use super::{
    JobContext, State, Stopped, Transition, check_config_update, handle_unhandled_message,
    stopping::{StopBehavior, Stopping},
};

#[derive(Debug)]
pub struct CheckpointStopping {}

/// The stop that overtakes the one this state is already making, if the job's configuration
/// asks for one.
///
/// `CheckpointStopping` is stopping the job the slowest and most careful way there is: it is
/// taking a final checkpoint first. So a configuration asking for a `checkpoint` or `graceful`
/// stop is asking for what is already happening and changes nothing, and only an `immediate` or
/// `force` stop overtakes it. That is why this is deliberately *not*
/// `stop_if_desired_non_running!`, whose mapping would turn the careful stop an operator asked
/// for into an immediate one and throw the final checkpoint away.
///
/// One rule with two readers: the configuration updates this state consumes from the job's
/// channel, and the lifecycle intents M11.D39a's single writer publishes into its
/// configuration. Written once so the two cannot come to disagree.
fn escalation(config: &JobConfig) -> Option<Transition> {
    match config.stop_mode {
        StopMode::immediate => Some(Transition::next(
            CheckpointStopping {},
            Stopping {
                stop_mode: StopBehavior::StopJob(grpc::rpc::StopMode::Immediate),
            },
        )),
        StopMode::force => {
            todo!("implement force stop mode");
        }
        StopMode::none | StopMode::checkpoint | StopMode::graceful => None,
    }
}

#[async_trait::async_trait]
impl State for CheckpointStopping {
    fn name(&self) -> &'static str {
        "CheckpointStopping"
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
            // M11.D39a's second consumption point. What an intent can decide here is narrow —
            // the job is already stopping — but it is not nothing: an operator who asked for an
            // immediate stop while the final checkpoint was running is asking to stop waiting
            // for it, and under the single-writer mechanism that arrives here and nowhere else.
            // The job controller is borrowed one turn at a time so that this read, which needs
            // the whole context, can happen at all.
            if ctx
                .observe_lifecycle_intent(ConsumptionPoint::InsideInterruptibleWait)?
                .stops()
                && let Some(escalated) = escalation(&ctx.config)
            {
                return Ok(escalated);
            }

            match ctx
                .job_controller
                .as_mut()
                .unwrap()
                .checkpoint_finished()
                .await
            {
                Ok(done) => {
                    debug!(
                        job_id = %job_id,
                        pipeline_id = *pipeline_id,
                        "checked checkpoint, got {}, job_controller.finished(): {}, final_checkpoint_started: {}",
                        done,
                        ctx.job_controller.as_ref().unwrap().finished(),
                        final_checkpoint_started
                    );

                    if done
                        && ctx.job_controller.as_mut().unwrap().finished()
                        && final_checkpoint_started
                    {
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
                            if let Some(escalated) = escalation(&c) {
                                return Ok(escalated);
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
    }
}
