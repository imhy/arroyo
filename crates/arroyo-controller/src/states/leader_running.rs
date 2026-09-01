use super::{JobContext, State, Transition, controller_job_failure};
use crate::JobConfig;
use crate::JobMessage;
use crate::states::LeavingForStop;
use crate::states::StateError;
use crate::states::leader_finishing::LeaderFinishing;
use crate::states::leader_rescaling::LeaderRescaling;
use crate::states::leader_restarting::LeaderRestarting;
use crate::states::leader_stop_if_desired_running;
use crate::states::lifecycle::{
    ConsumptionPoint, ObservedIntent, StatusPublication, leaving, stand_down,
};
use crate::states::{ConfigApplied, RunningConfigAction, decide_running_config};
use anyhow::anyhow;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::grpc::rpc::{ErrorDomain, JobFailure, RetryHint};
use arroyo_rpc::log_event;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::MissedTickBehavior;
use tracing::{error, warn};

#[derive(Debug)]
pub struct LeaderRunning {
    pub started: Instant,
}

/// Everything `LeaderRunning` does about a configuration it has just been given, whichever
/// route delivered it.
///
/// The leader-mode half of [`running::apply_new_config`](crate::states::running), and it
/// exists for the same reason: a configuration reaches a running job as a
/// [`JobMessage::ConfigUpdate`] on the landed M11.T08 path and as an
/// [`ObservedIntent::Adopted`] under
/// [`LifecycleMode::FencedV2`](crate::states::lifecycle::LifecycleMode::FencedV2), and the two
/// routes decide with one function rather than with two that agree today. PR #160 review
/// comment `5365261487`.
///
/// `operator_parallelism` is the job's own program rather than a running controller's model:
/// that is the one place the two modes differ, and [`decide_running_config`] takes it as an
/// argument so that everything else is shared.
///
/// # Errors
///
/// The fatal [`StateError`] a configuration that changes the job's state backend produces.
fn apply_new_config(
    state: Box<LeaderRunning>,
    ctx: &JobContext<'_>,
    superseded: &JobConfig,
    updated: &JobConfig,
    operator_parallelism: &HashMap<u32, usize>,
) -> Result<ConfigApplied<LeaderRunning>, StateError> {
    let action = decide_running_config(
        ctx.execution_selector,
        superseded,
        updated,
        ctx.status.restart_nonce,
        |node_id| operator_parallelism.get(&node_id).copied(),
    )?;

    Ok(match action {
        RunningConfigAction::Restart(mode) => {
            ConfigApplied::Leaves(Transition::next(*state, LeaderRestarting { mode }))
        }
        RunningConfigAction::Rescale => {
            ConfigApplied::Leaves(Transition::next(*state, LeaderRescaling {}))
        }
        // Nothing further. Leader mode holds no second copy of the job's configuration to
        // update — there is no `JobController` here — so an update that needs no rescheduling
        // is applied by the job's configuration having been replaced, which is the landed
        // `ConfigUpdate` arm's behaviour unchanged.
        RunningConfigAction::Apply => ConfigApplied::Applied(state),
    })
}

#[async_trait::async_trait]
impl State for LeaderRunning {
    fn name(&self) -> &'static str {
        "Running"
    }

    /// Leaves, by the same macro the body's own entry check uses, and for the same reason
    /// [`Running`](crate::states::running::Running) does: the answer is redundant with that
    /// check today and the trait admits no default, so it cannot stop being made.
    fn leave_for_stop(self: Box<Self>, config: &JobConfig) -> LeavingForStop {
        leaving::leaves_running_under_leader(self, config)
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        let pipeline_config = &config().clone().pipeline;

        let mut log_interval = tokio::time::interval(Duration::from_secs(60));
        log_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut poll_interval = tokio::time::interval(*config().controller.leader_poll_interval);
        poll_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        leader_stop_if_desired_running!(self, ctx.config);

        if ctx.leader_manager.is_none() {
            return ctx
                .handle_job_failure(
                    *self,
                    controller_job_failure(
                        "leader_manager was None entering Running state",
                        ErrorDomain::Internal,
                        RetryHint::WithBackoff,
                    ),
                )
                .await;
        }

        if ctx.leader_manager().generation != ctx.status.generation {
            let generation = ctx.status.generation;
            let msg = format!(
                "leader_manager has incorrect generation (expected {}, found {})",
                generation,
                ctx.leader_manager().generation
            );
            return ctx
                .handle_job_failure(
                    *self,
                    controller_job_failure(msg, ErrorDomain::Internal, RetryHint::WithBackoff),
                )
                .await;
        }

        let operator_parallelism = ctx.program.tasks_per_node();

        // What the select below parks on so that a lifecycle intent submitted while the job is
        // healthy ends the wait. Never ready on the landed M11.T08 mechanism — see
        // `JobContext::lifecycle_wakeup`.
        let wake = ctx.lifecycle_wakeup();

        loop {
            // M11.D39a's second consumption point, for the same reason as in controller mode:
            // a job whose leader is healthy has nothing else that would make this loop look.
            match ctx.observe_lifecycle_intent(ConsumptionPoint::InsideInterruptibleWait)? {
                ObservedIntent::Stop => {
                    leader_stop_if_desired_running!(self, ctx.config);
                }
                // Through the same function the `ConfigUpdate` arm below uses: the writer has
                // published the new configuration into `ctx.config`, and what a running job
                // does about it is the difference between that and the one it replaced.
                ObservedIntent::Adopted(superseded) => {
                    match apply_new_config(
                        self,
                        ctx,
                        &superseded,
                        &ctx.config,
                        &operator_parallelism,
                    )? {
                        ConfigApplied::Leaves(transition) => return Ok(transition),
                        ConfigApplied::Applied(running) => self = running,
                    }
                }
                ObservedIntent::Continue => {}
            }

            if ctx.leader_manager().last_heartbeat.elapsed()
                > *pipeline_config.worker_heartbeat_timeout
            {
                return ctx
                    .handle_job_failure(
                        *self,
                        controller_job_failure(
                            format!(
                                "no response from job controller after {} seconds",
                                pipeline_config.worker_heartbeat_timeout.as_secs()
                            ),
                            ErrorDomain::Internal,
                            RetryHint::WithBackoff,
                        ),
                    )
                    .await;
            }

            if let Some(ttl) = ctx.config.ttl
                && self.started.elapsed() > ttl
            {
                return Ok(Transition::next(
                    *self,
                    LeaderStopping {
                        stop_behavior: LeaderStopBehavior::StopWorkers,
                    },
                ));
            }

            tokio::select! {
                // The loop reads the job's writer at the top of every turn, so ending the turn
                // is the whole of what this arm has to do.
                _ = wake.notified() => {}
                msg = ctx.rx.recv() => {
                    match msg {
                        Some(JobMessage::ConfigUpdate(c)) => {
                            leader_stop_if_desired_running!(self, c);

                            // Shared with controller mode and with the adopted route above:
                            // refuses a state-backend change and decides whether the rest of
                            // the update needs a restart or a rescale. The comparison is
                            // against the execution's own selector, not against
                            // `ctx.config`, which is refreshed from shared state after every
                            // transition.
                            match apply_new_config(
                                self,
                                ctx,
                                &ctx.config,
                                &c,
                                &operator_parallelism,
                            )? {
                                ConfigApplied::Leaves(transition) => return Ok(transition),
                                ConfigApplied::Applied(running) => self = running,
                            }
                        }
                        Some(msg) => {
                            // Routed rather than logged here so a refused configuration
                            // reaches the one place that acts on it.
                            ctx.handle(msg)?;
                        }
                        None => {
                            panic!("job queue shut down");
                        }
                    }
                }
                _ = poll_interval.tick() => {
                    if ctx.status.restarts > 0 && self.started.elapsed() > *pipeline_config.healthy_duration {
                        let restarts = ctx.status.restarts;
                        ctx.status.restarts = 0;
                        match ctx.publish_status().await {
                            Ok(StatusPublication::Published) => {}
                            // The same decision as `Running`'s, for the same reason: a lost
                            // authority is not a failure to retry, so this task ends.
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
                            }
                        }
                    }

                    match ctx.leader_manager().poll_leader_status().await {
                        Ok(status) => {
                            let state = match rpc::JobState::try_from(status.job_state) {
                                Ok(state) => state,
                                Err(e) => {
                                    return Err(ctx.retryable(
                                        self,
                                        "leader returned invalid job state",
                                        e.into(),
                                        10,
                                    ));
                                }
                            };
                            match state {
                                rpc::JobState::JobInitializing => {
                                    return ctx.handle_job_failure(*self, JobFailure {
                                        operator_id: None,
                                        task_id: None,
                                        subtask_index: None,
                                        message: "job unexpectedly in Initializing state, should be running".to_string(),
                                        error_domain: ErrorDomain::Internal as i32,
                                        retry_hint: RetryHint::WithBackoff as i32,
                                    }).await;
                                }
                                rpc::JobState::JobRunning => {
                                    // in progress
                                }
                                rpc::JobState::JobStopping | rpc::JobState::JobStopped => {
                                    return ctx.handle_job_failure(*self, controller_job_failure(
                                        format!("job unexpectedly in {:?} state, should be running", state),
                                        rpc::ErrorDomain::Internal,
                                        rpc::RetryHint::WithBackoff,
                                    )).await;
                                }
                                rpc::JobState::JobFinishing | rpc::JobState::JobFinished => {
                                    // finishing is initiated by the workers themselves (via the
                                    // sources consuming all of their input) so we just respond
                                    // to it
                                    return Ok(Transition::next(
                                        *self,
                                        LeaderFinishing {},
                                    ));
                                }
                                rpc::JobState::JobFailing | rpc::JobState::JobFailed => {
                                    let Some(failure) = status.job_failure else {
                                        return Err(ctx.retryable(
                                            self,
                                            "leader reported failing status without failure payload",
                                            anyhow!("missing job failure"),
                                            10,
                                        ));
                                    };
                                    return ctx.handle_job_failure(*self, failure).await;
                                }
                                rpc::JobState::JobUnknown => {
                                    return Err(ctx.retryable(
                                        self,
                                        "leader returned unknown job state",
                                        anyhow!("unknown leader job state"),
                                        10,
                                    ));
                                }
                            }
                        }
                        Err(err) => {
                            warn!(
                                message = "error while polling leader status",
                                error = format!("{:?}", err),
                                job_id = %ctx.config.id,
                                pipeline_id = *ctx.pipeline_info.pipeline_id
                            );
                            tokio::time::sleep(Duration::from_secs(2)).await;
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
            }
        }
    }
}
