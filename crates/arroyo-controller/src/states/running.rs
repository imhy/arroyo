use std::time::{Duration, Instant};

use time::OffsetDateTime;
use tokio::time::MissedTickBehavior;

use tracing::error;

use crate::JobMessage;
use crate::states::finishing::Finishing;
use crate::states::lifecycle::ConsumptionPoint;
use crate::states::recovering::Recovering;
use crate::states::rescaling::Rescaling;
use crate::states::restarting::Restarting;
use crate::states::stop_if_desired_running;
use crate::states::{RunningConfigUpdate, classify_running_config_update};
use crate::{job_controller::ControllerProgress, states::StateError};
use arroyo_rpc::config::config;
use arroyo_rpc::errors::ErrorDomain;
use arroyo_rpc::log_event;

use super::{JobContext, State, Transition};

#[derive(Debug)]
pub struct Running {}

#[async_trait::async_trait]
impl State for Running {
    fn name(&self) -> &'static str {
        "Running"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        stop_if_desired_running!(self, ctx.config);

        let pipeline_config = &config().clone().pipeline;

        let running_start = Instant::now();

        let mut log_interval = tokio::time::interval(Duration::from_secs(60));
        log_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // What the select below parks on so that a lifecycle intent submitted while the job is
        // healthy ends the wait. Never ready for a job on the landed M11.T08 mechanism, which
        // is every production job through M11.T25: there the poll publishes into the channel
        // the first arm already reads.
        let wake = ctx.lifecycle_wakeup();

        loop {
            // M11.D39a's second consumption point. `Running` is the state a healthy job spends
            // its life in, and under the single-writer mechanism nothing is sent to the job's
            // channel when the poll decides something — so without this read a stop or a
            // refusal decided here would be observed at no point at all, for as long as the
            // job kept running well.
            if ctx
                .observe_lifecycle_intent(ConsumptionPoint::InsideInterruptibleWait)?
                .stops()
            {
                // Through the same macro the `ConfigUpdate` arm below uses, on the
                // configuration the writer has just published into: a stop that arrives as an
                // intent and a stop that arrives as a message reach the same state.
                stop_if_desired_running!(self, ctx.config);
            }

            let ttl_end: Option<Duration> = ctx.config.ttl.map(|t| {
                let elapsed = Duration::from_micros(
                    (OffsetDateTime::now_utc() - ctx.status.start_time.unwrap())
                        .whole_microseconds() as u64,
                );

                t.checked_sub(elapsed).unwrap_or(Duration::ZERO)
            });

            tokio::select! {
                // The loop reads the job's writer at the top of every turn, so ending the turn
                // is the whole of what this arm has to do.
                _ = wake.notified() => {}
                msg = ctx.rx.recv() => {
                    match msg {
                        Some(JobMessage::ConfigUpdate(c)) => {
                            stop_if_desired_running!(self, &c);

                            // Shared with leader mode: refuses a state-backend change and
                            // decides whether the rest of the update needs a restart. The
                            // comparison is against the execution's own selector, not
                            // against `ctx.config`, which is refreshed from shared state
                            // after every transition.
                            match classify_running_config_update(ctx.execution_selector, &ctx.config, &c, ctx.status.restart_nonce)? {
                                RunningConfigUpdate::Restart(mode) => {
                                    return Ok(Transition::next(*self, Restarting { mode }));
                                }
                                RunningConfigUpdate::Apply => {}
                            }

                            let job_controller = ctx.job_controller.as_mut().unwrap();

                            for (node_id, p) in &c.parallelism_overrides {
                                if let Some(actual) = job_controller.operator_parallelism(*node_id)
                                    && actual != *p {
                                        return Ok(Transition::next(
                                            *self,
                                            Rescaling {}
                                        ));
                                    }
                            }

                            job_controller.update_config(c);
                        }
                        Some(JobMessage::RunningMessage(msg)) => {
                            if let Err(e) = ctx.job_controller.as_mut().unwrap().handle_message(msg).await {
                                return Err(ctx.retryable(self, "job encountered an error", e, 10));
                            }
                        }
                        Some(msg) => {
                            ctx.handle(msg)?;
                        }
                        None => {
                            panic!("job queue shut down");
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(200)) => {
                    if ctx.status.restarts > 0 && running_start.elapsed() > *pipeline_config.healthy_duration {
                        let restarts = ctx.status.restarts;
                        ctx.status.restarts = 0;
                        if let Err(e) = ctx.status.update_db(&ctx.db).await {
                            error!(
                                message = "Failed to update status",
                                error = format!("{:?}", e),
                                job_id = %ctx.config.id,
                                pipeline_id = *ctx.pipeline_info.pipeline_id
                            );
                            ctx.status.restarts = restarts;
                            // we'll try again on the next round
                        }
                    }

                    match ctx.job_controller.as_mut().unwrap().progress().await {
                        Ok(ControllerProgress::Continue) => {
                            // do nothing
                        },
                        Ok(ControllerProgress::Finishing) => {
                            return Ok(Transition::next(
                                *self,
                                Finishing {}
                            ))
                        },
                        Ok(ControllerProgress::TaskFailed(event)) => {
                            log_event!("task_error", {
                                "service": "controller",
                                "job_id": ctx.config.id,
                                "operator_id": event.operator_id,
                                "subtask_idx": event.subtask_idx,
                                "error": event.reason,
                                "domain": event.error_domain.as_str(),
                                "is_preview": ctx.config.ttl.is_some(),
                            });

                            return ctx.handle_task_error(self, event).await;
                        }
                        Err(err) => {
                            error!(
                                message = "error while running",
                                error = format!("{:?}", err),
                                job_id = %ctx.config.id,
                                pipeline_id = *ctx.pipeline_info.pipeline_id
                            );
                            log_event!("running_error", {
                                "service": "controller",
                                "job_id": ctx.config.id,
                                "error": format!("{:?}", err),
                                "is_preview": ctx.config.ttl.is_some(),
                            });

                            return Ok(Transition::next(
                                *self,
                                Recovering {
                                    source: err,
                                    reason: "error while running".to_string(),
                                    domain: ErrorDomain::Internal,
                                }
                            ))
                        }
                    }
                }
                _ = log_interval.tick() => {
                    log_event!(
                        "job_running",
                        {
                            "service": "controller",
                            "job_id": ctx.config.id,
                            "scheduler": &config().controller.scheduler,
                        },
                        [
                            "duration_ms" => ctx.last_transitioned_at.elapsed().as_millis() as f64,
                        ]
                    );
                }
                _ = tokio::time::sleep(ttl_end.unwrap_or(Duration::MAX)) => {
                    // TTL has expired, stop the job
                    return Ok(Transition::next(
                        *self,
                        Stopping {
                            stop_mode: StopBehavior::StopJob(rpc::StopMode::Immediate),
                        },
                    ));
                }
            }
        }
    }
}
