use super::{
    JobContext, State, StateError, Transition, check_config_update, fatal,
    leader_stop_if_desired_running, scheduling::Scheduling,
};
use crate::JobConfig;
use crate::JobMessage;
use crate::states::LeavingForStop;
use crate::states::lifecycle::{ConsumptionPoint, leaving};
use crate::states::recovering::Recovering;
use crate::types::public::RestartMode;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::grpc::rpc::{JobFailure, JobState, JobStopMode};
use std::time::{Duration, Instant};
use tracing::info;

#[derive(Debug)]
pub struct LeaderRestarting {
    pub mode: RestartMode,
}

#[async_trait::async_trait]
impl State for LeaderRestarting {
    fn name(&self) -> &'static str {
        "Restarting"
    }

    /// Leaves. `RestartMode::safe` sends the leader a checkpoint-stop as its first statement,
    /// before the loop that holds its consumption point, and `RestartMode::force` tears the
    /// cluster down; both end in `Scheduling`. A stop consumed at the state boundary is
    /// invisible to this state's own observation, so answering it here is what keeps a
    /// cancelled restart from checkpointing and rescheduling anyway.
    fn leave_for_stop(self: Box<Self>, config: &JobConfig) -> LeavingForStop {
        leaving::leaves_running_under_leader(self, config)
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        match self.mode {
            RestartMode::safe => {
                if let Err(e) = ctx
                    .leader_manager()
                    .stop_leader(JobStopMode::JobStopCheckpoint)
                    .await
                {
                    return Err(ctx.retryable(
                        self,
                        "failed to send checkpoint-stop to leader",
                        e,
                        10,
                    ));
                }

                let started = Instant::now();

                // What the select below parks on so that a lifecycle intent submitted while the
                // final checkpoint is being taken ends the wait. Never ready on the landed
                // M11.T08 mechanism — see `JobContext::lifecycle_wakeup`.
                let wake = ctx.lifecycle_wakeup();

                loop {
                    // M11.D39a's second consumption point. This restart ends in `Scheduling`,
                    // which starts a replacement cluster, so a stop decided while the final
                    // checkpoint is in flight has to be read here rather than after it.
                    if ctx
                        .observe_lifecycle_intent(ConsumptionPoint::InsideInterruptibleWait)?
                        .stops()
                    {
                        leader_stop_if_desired_running!(self, ctx.config);
                    }

                    let timeout = config()
                        .pipeline
                        .checkpoint
                        .timeout
                        .as_ref()
                        .map(|t| (started + **t).saturating_duration_since(Instant::now()))
                        .unwrap_or(Duration::MAX);

                    tokio::select! {
                        // The loop reads the job's writer at the top of every turn, so ending
                        // the turn is the whole of what this arm has to do.
                        _ = wake.notified() => {}
                        msg = ctx.rx.recv() => {
                            match msg {
                                Some(JobMessage::ConfigUpdate(c)) => {
                                    leader_stop_if_desired_running!(self, c);

                                    // This restart ends in `Scheduling`, which starts
                                    // workers from the job's selector; a stop is honoured
                                    // above, but the job must not be rescheduled from a
                                    // configuration that changes the backend.
                                    check_config_update(ctx.execution_selector, &c)?;
                                }
                                Some(msg) => {
                                    // Routed rather than logged here so a refused
                                    // configuration reaches the one place that acts on it.
                                    ctx.handle(msg)?;
                                }
                                None => {
                                    panic!("job queue shut down");
                                }
                            }
                        }
                        resp = ctx.leader_manager.as_mut().expect("leader manager not initialized").wait_for_state(JobState::JobStopped) => {
                            return if let Err(e) = resp {
                                Err(fatal("failed while waiting for checkpoint-stop during restart",e))
                            } else {
                                Ok(Transition::next(*self, Scheduling {}))
                            };
                        }
                        _ = tokio::time::sleep(timeout) => {
                            return ctx.handle_job_failure(*self, JobFailure {
                                operator_id: None,
                                task_id: None,
                                subtask_index: None,
                                message: "timed out while taking final checkpoint".to_string(),
                                error_domain: rpc::ErrorDomain::Internal as i32,
                                retry_hint: rpc::RetryHint::WithBackoff as i32,
                            }).await;
                        }
                    }
                }
            }
            RestartMode::force => {
                info!(
                    job_id = %ctx.config.id,
                    pipeline_id = *ctx.pipeline_info.pipeline_id,
                    "force restarting job, tearing down cluster"
                );

                if let Err(e) = Recovering::cleanup(ctx).await {
                    return Err(ctx.retryable(self, "failed to tear down existing cluster", e, 20));
                }

                Ok(Transition::next(*self, Scheduling {}))
            }
        }
    }
}
