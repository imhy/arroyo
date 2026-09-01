use std::time::{Duration, Instant};

use time::OffsetDateTime;
use tokio::time::MissedTickBehavior;

use tracing::error;

use crate::JobConfig;
use crate::JobMessage;
use crate::states::LeavingForStop;
use crate::states::finishing::Finishing;
use crate::states::lifecycle::{
    ConsumptionPoint, ObservedIntent, StatusPublication, leaving, stand_down,
};
use crate::states::recovering::Recovering;
use crate::states::rescaling::Rescaling;
use crate::states::restarting::Restarting;
use crate::states::stop_if_desired_running;
use crate::states::{ConfigApplied, RunningConfigAction, decide_running_config};
use crate::{job_controller::ControllerProgress, states::StateError};
use arroyo_rpc::config::config;
use arroyo_rpc::errors::ErrorDomain;
use arroyo_rpc::log_event;

use super::{JobContext, State, Transition};

#[derive(Debug)]
pub struct Running {}

/// Everything `Running` does about a configuration it has just been given, whichever route
/// delivered it.
///
/// The two routes are [`JobMessage::ConfigUpdate`] on the landed M11.T08 path and
/// [`ObservedIntent::Adopted`] under
/// [`LifecycleMode::FencedV2`](crate::states::lifecycle::LifecycleMode::FencedV2), and they
/// meet here — not in two places that happen to agree today. PR #160 review comment
/// `5365261487` is what the other arrangement produced: the adopted route replaced
/// `ctx.config` and returned, so a `restart_nonce`, scheduler, environment or parallelism
/// change reached neither [`Restarting`] nor [`Rescaling`], and a `checkpoint_interval` change
/// never reached the job controller, which went on making progress under its own private copy
/// of the configuration the job had replaced.
///
/// `superseded` is what the job was running under and `updated` what it has been given. The
/// stop is *not* decided here: what a stop means differs by state and by mode, and both
/// callers answer it with the landed `stop_if_desired_running!` before they get here.
///
/// # Errors
///
/// The fatal [`StateError`] a configuration that changes the job's state backend produces.
fn apply_new_config(
    state: Box<Running>,
    ctx: &mut JobContext<'_>,
    superseded: &JobConfig,
    updated: JobConfig,
) -> Result<ConfigApplied<Running>, StateError> {
    let action = {
        let job_controller = ctx
            .job_controller
            .as_ref()
            .expect("a running job has a job controller");
        decide_running_config(
            ctx.execution_selector,
            superseded,
            &updated,
            ctx.status.restart_nonce,
            |node_id| job_controller.operator_parallelism(node_id),
        )?
    };

    Ok(match action {
        RunningConfigAction::Restart(mode) => {
            ConfigApplied::Leaves(Transition::next(*state, Restarting { mode }))
        }
        RunningConfigAction::Rescale => {
            ConfigApplied::Leaves(Transition::next(*state, Rescaling {}))
        }
        RunningConfigAction::Apply => {
            // The job controller keeps its own copy of the configuration —
            // `JobController::progress` reads the checkpoint interval out of it on every turn
            // — so an update classified as applicable is only applied once this has run.
            ctx.job_controller
                .as_mut()
                .expect("a running job has a job controller")
                .update_config(updated);
            ConfigApplied::Applied(state)
        }
    })
}

#[async_trait::async_trait]
impl State for Running {
    fn name(&self) -> &'static str {
        "Running"
    }

    /// Leaves, by the same macro the body's own first statement uses. `Running` already reads
    /// the published configuration before it does anything, so this changes no outcome for it
    /// — it is here because the trait admits no default, and a state whose answer happens to
    /// be redundant today is exactly the state that stops being redundant when its body grows
    /// a statement above that read.
    fn leave_for_stop(self: Box<Self>, config: &JobConfig) -> LeavingForStop {
        leaving::leaves_running(self, config)
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        stop_if_desired_running!(self, ctx.config);

        let pipeline_config = &config().clone().pipeline;

        let running_start = Instant::now();

        let mut log_interval = tokio::time::interval(Duration::from_secs(60));
        log_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // What the select below parks on so that a lifecycle intent submitted while the job is
        // healthy ends the wait. Never ready for a job built in the pre-flag-day peer mode,
        // which no production job is: there the poll publishes into the channel the first arm
        // already reads.
        let wake = ctx.lifecycle_wakeup();

        loop {
            // M11.D39a's second consumption point. `Running` is the state a healthy job spends
            // its life in, and under the single-writer mechanism nothing is sent to the job's
            // channel when the poll decides something — so without this read a stop or a
            // refusal decided here would be observed at no point at all, for as long as the
            // job kept running well.
            match ctx.observe_lifecycle_intent(ConsumptionPoint::InsideInterruptibleWait)? {
                // Through the same macro the `ConfigUpdate` arm below uses, on the
                // configuration the writer has just published into: a stop that arrives as an
                // intent and a stop that arrives as a message reach the same state.
                ObservedIntent::Stop => {
                    stop_if_desired_running!(self, ctx.config);
                }
                // And through the same function the `ConfigUpdate` arm below uses, for the
                // same reason: the writer has published the new configuration into
                // `ctx.config`, and what a running job *does* about it is the difference
                // between that and the one it replaced.
                ObservedIntent::Adopted(superseded) => {
                    let updated = ctx.config.clone();
                    match apply_new_config(self, ctx, &superseded, updated)? {
                        ConfigApplied::Leaves(transition) => return Ok(transition),
                        ConfigApplied::Applied(running) => self = running,
                    }
                }
                ObservedIntent::Continue => {}
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

                            // Shared with leader mode and with the adopted route above:
                            // refuses a state-backend change, decides whether the rest of
                            // the update needs a restart or a rescale, and otherwise applies
                            // it to the running job. The comparison is against the
                            // execution's own selector, not against `ctx.config`, which is
                            // refreshed from shared state after every transition.
                            let superseded = ctx.config.clone();
                            match apply_new_config(self, ctx, &superseded, c)? {
                                ConfigApplied::Leaves(transition) => return Ok(transition),
                                ConfigApplied::Applied(running) => self = running,
                            }
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
                        match ctx.publish_status().await {
                            Ok(StatusPublication::Published) => {}
                            // Another controller holds this job. Restoring the counter and
                            // trying again next round is what a *failure* deserves; a lost
                            // authority is not one, and retrying it is a superseded controller
                            // trying to overwrite a live one on a two-hundred-millisecond
                            // timer. This task ends instead.
                            Ok(StatusPublication::Superseded(stale)) => {
                                stand_down(stale);
                                return Ok(Transition::Stop);
                            }
                            Err(e) => {
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
