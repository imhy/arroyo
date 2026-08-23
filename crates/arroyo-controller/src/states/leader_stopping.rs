use super::{JobContext, State, Stopped, Transition};
use crate::JobConfig;
use crate::job_controller::leader_manager::leader_stop_escalation;
use crate::states::LeavingForStop;
use crate::states::StateError;
use crate::states::lifecycle::{ConsumptionPoint, ObservedIntent};
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::grpc::rpc::JobStopMode;
use std::time::{Duration, Instant};
use tracing::{error, info};
const FINISH_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Copy, Clone, Debug)]
pub enum LeaderStopBehavior {
    StopJob(JobStopMode),
    StopWorkers,
}

impl LeaderStopBehavior {
    /// How far a stop goes, as an order: a higher number ends the job harder and sooner.
    ///
    /// The order exists so that "escalates" can be said once, in
    /// [`Self::escalation`]. `LeaderStopping` *is* a stop, so a second stop reaching it is
    /// only news when it goes further than the one already in flight — and a rule that
    /// answered every stop would send the leader the same message again on every turn of the
    /// wait, or replace an `immediate` stop with the `graceful` one an operator asked for
    /// before it.
    ///
    /// `JobStopCheckpoint` is the weakest, and is unreachable *here*: a checkpoint stop is
    /// [`LeaderCheckpointStopping`](crate::states::leader_checkpoint_stopping::LeaderCheckpointStopping)'s,
    /// nothing constructs this state with it, and `leader_stop_escalation` never answers it.
    /// It is ordered rather than excluded because the enum admits it, and a match that
    /// pretended otherwise would be the catch-all that turns a later variant into a silent
    /// no-op.
    fn severity(self) -> u8 {
        match self {
            LeaderStopBehavior::StopJob(JobStopMode::JobStopCheckpoint) => 0,
            LeaderStopBehavior::StopJob(JobStopMode::JobStopGraceful) => 1,
            LeaderStopBehavior::StopJob(JobStopMode::JobStopImmediate) => 2,
            LeaderStopBehavior::StopWorkers => 3,
        }
    }

    /// The stop that overtakes this one, if the job's configuration asks for a harder one.
    ///
    /// [`leader_stop_escalation`] is the mapping from stop mode to behaviour — one rule, of
    /// which this is the third reader — and the comparison is the part that belongs to a state
    /// that is already stopping. `None` therefore means two things that are the same thing
    /// here: the configuration asks for no stop (`none`, `checkpoint`), or it asks for one
    /// this state is already making or has already gone past.
    fn escalation(self, config: &JobConfig) -> Option<Self> {
        leader_stop_escalation(config).filter(|harder| harder.severity() > self.severity())
    }
}

#[derive(Debug)]
pub struct LeaderStopping {
    pub stop_behavior: LeaderStopBehavior,
}

#[async_trait::async_trait]
impl State for LeaderStopping {
    fn name(&self) -> &'static str {
        "Stopping"
    }

    /// Leaves only for a stop that goes further than the one this state is already making.
    ///
    /// An unconditional `Stays` until PR #160 review comment `5384225297`. The reason recorded
    /// then — this state is the stop, and the behaviour it holds is the one decided when it
    /// was constructed — is why it still stays for an equal or weaker stop, and it was never a
    /// reason to go on sending the leader a graceful stop after an operator asked for a force.
    ///
    /// Staying is not discarding: the stop is left standing on the job's writer and re-offered
    /// at this state's own consumption point, which the wait in [`Self::next`] now has.
    /// Leaving *here* is what keeps the weaker `stop_leader` from being sent at all, exactly as
    /// it does for
    /// [`LeaderCheckpointStopping`](crate::states::leader_checkpoint_stopping::LeaderCheckpointStopping),
    /// whose send is also ahead of the wait that holds its consumption point.
    fn leave_for_stop(self: Box<Self>, config: &JobConfig) -> LeavingForStop {
        match self.stop_behavior.escalation(config) {
            Some(stop_behavior) => {
                LeavingForStop::Leaves(Transition::next(*self, LeaderStopping { stop_behavior }))
            }
            None => LeavingForStop::Stays(self),
        }
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        // `is_some` rather than `as_mut`, because the wait below reads the job's writer between
        // turns and cannot hold a borrow of the context across them. The pairs this matches are
        // the landed ones: a leader stop needs a leader manager, and everything else — including
        // a job stop with no leader manager to send it to — stops the workers directly.
        match (ctx.leader_manager.is_some(), self.stop_behavior) {
            (true, LeaderStopBehavior::StopJob(mode)) => {
                if let Err(e) = ctx.leader_manager().stop_leader(mode).await {
                    return Err(ctx.retryable(
                        self,
                        "failed to send stop message to leader",
                        e,
                        10,
                    ));
                }

                let timeout = match mode {
                    JobStopMode::JobStopCheckpoint => {
                        config().pipeline.checkpoint.timeout.as_ref().map(|t| **t)
                    }
                    JobStopMode::JobStopGraceful | JobStopMode::JobStopImmediate => {
                        Some(FINISH_TIMEOUT)
                    }
                };
                let started = Instant::now();

                info!(
                    msg = "waiting for workers to terminate",
                    job_id = %ctx.config.id,
                    pipeline_id = *ctx.pipeline_info.pipeline_id
                );

                // What the select below parks on so that a lifecycle intent submitted while
                // this state waits for the leader ends the wait. Never ready on the landed
                // M11.T08 mechanism — see `JobContext::lifecycle_wakeup`.
                //
                // The job's channel is deliberately not read here, and the landed `TODO` it
                // carried says why it should be: on the M11.T08 path a force stop still
                // arrives as a `ConfigUpdate` this wait does not consume. That half is
                // unchanged legacy behaviour and is not this round's.
                let wake = ctx.lifecycle_wakeup();

                loop {
                    // M11.D39a's second consumption point, and PR #160 review comment
                    // `5384225297`: this wait watched neither of the sources a stop arrives
                    // on, so under `FencedV2` an operator who escalated to `immediate` or
                    // `force` while a graceful stop was in flight waited out the whole
                    // deadline below first.
                    match ctx.observe_lifecycle_intent(ConsumptionPoint::InsideInterruptibleWait)? {
                        ObservedIntent::Stop => {
                            if let Some(stop_behavior) = self.stop_behavior.escalation(&ctx.config)
                            {
                                return Ok(Transition::next(
                                    *self,
                                    LeaderStopping { stop_behavior },
                                ));
                            }
                        }
                        // Nothing further. This state is ending the job — it schedules
                        // nothing, restarts nothing and rescales nothing — so a configuration
                        // it could adopt changes nothing it is about to do. The stop half is
                        // above, by the rule the state boundary applies.
                        ObservedIntent::Adopted(_) | ObservedIntent::Continue => {}
                    }

                    // One deadline for the whole wait rather than one per turn, as `Stopping`
                    // does: a job whose clock restarted every time it was told to stop again
                    // would never reach the force stop below.
                    let remaining = timeout
                        .map(|t| (started + t).saturating_duration_since(Instant::now()))
                        .unwrap_or(Duration::MAX);

                    tokio::select! {
                        // The loop reads the job's writer at the top of every turn, so ending
                        // the turn is the whole of what this arm has to do.
                        _ = wake.notified() => {}
                        resp = ctx.leader_manager.as_mut().expect("the arm this is in tested for a leader manager").wait_for_state(rpc::JobState::JobStopped) => {
                            match resp {
                                Ok(_) => break,
                                Err(e) => {
                                    error!(
                                        msg = "encountered error while waiting for job to stop gracefully; will try force-stopping",
                                        job_id = %ctx.config.id,
                                        pipeline_id = *ctx.pipeline_info.pipeline_id,
                                        error = e.to_string(),
                                    );

                                    return Ok(Transition::next(
                                        *self,
                                        LeaderStopping {
                                            stop_behavior: LeaderStopBehavior::StopWorkers,
                                        },
                                    ));
                                }
                            }
                        }
                        _ = tokio::time::sleep(remaining) => {
                            error!(
                                msg = "timed out waiting for job to stop",
                                job_id = %ctx.config.id,
                                pipeline_id = *ctx.pipeline_info.pipeline_id,
                            );

                            return Ok(Transition::next(
                                *self,
                                LeaderStopping {
                                    stop_behavior: LeaderStopBehavior::StopWorkers,
                                },
                            ));
                        }
                    }
                }
            }
            (_, LeaderStopBehavior::StopWorkers) | (false, _) => {
                if let Err(e) = ctx
                    .scheduler
                    .stop_workers(&ctx.config.id, Some(ctx.status.generation), true)
                    .await
                {
                    return Err(ctx.retryable(self, "failed while stopping workers", e, 20));
                }
            }
        }

        Ok(Transition::next(*self, Stopped {}))
    }
}
