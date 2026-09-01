//! What a state does about a stop its job's single writer has already decided (M11.T25,
//! M11.D39a).
//!
//! # The question this module exists to make unforgettable
//!
//! A lifecycle intent is consumed exactly once: [`LifecycleActor::observe`] advances its
//! version watermark, so the *second* look at the same intent reports nothing. The state
//! boundary in [`execute_state`](crate::states::execute_state) is a consumption point, which
//! means a stop decided while the previous state was running is consumed **there** — before
//! the state that has to act on it has run a single line. A state that learned about a stop
//! only by observing for itself would therefore never learn about that one, and would go on
//! to start a final checkpoint, a replacement cluster, or a leader stop that the operator had
//! already asked it not to start.
//!
//! So the boundary does not discard what it consumed: it asks the state it is about to run
//! what that stop means, through [`State::leave_for_stop`](crate::states::State::leave_for_stop).
//! The method has **no default body**, so every state answers and a state added later cannot
//! fail to — which is the same argument the boundary already makes for the refusal gate and
//! for the choice of state body: a state that had to remember to ask would be a state that
//! could forget.
//!
//! # What is in here, and what deliberately is not
//!
//! Only the three answers that map a stop mode to a transition, one per family of states, and
//! each of them expressed by **invoking the landed macro** rather than by restating its
//! mapping. `Stopping`, `CheckpointStopping`, `LeaderStopping` and `LeaderCheckpointStopping`
//! are what a stop means, and they must mean the same thing whether the stop arrived as a
//! [`JobMessage::ConfigUpdate`](crate::JobMessage::ConfigUpdate) that a state read for itself
//! or as an intent its writer published. One mapping, three callers of it.
//!
//! [`LeavingForStop`] itself lives in [`crate::states`], beside [`Transition`] and `StateError`:
//! it is the third type in [`State::leave_for_stop`](crate::states::State::leave_for_stop)'s
//! signature, and a return type of a public trait method belongs where the trait is.
//!
//! The states whose answer is "keep running" are not here. Their answer belongs beside the
//! body it is a claim about — a state that stays has to say *why* nothing it goes on to do
//! outruns the stop, and that reason is about that state.
//!
//! The boundary only asks when the job's writer decided something, so nothing here runs for a
//! job built in the pre-flag-day peer mode
//! [`LifecycleMode::LegacyT08`](super::LifecycleMode::LegacyT08), which has no writer to decide.
//! Every production job has had one since M11.T26h's activation change.

use crate::JobConfig;
use crate::states::checkpoint_stopping::CheckpointStopping;
use crate::states::leader_checkpoint_stopping::LeaderCheckpointStopping;
use crate::states::leader_stopping::LeaderStopping;
use crate::states::stopping::Stopping;
use crate::states::{
    LeavingForStop, State, Transition, TransitionTo, leader_stop_if_desired_running,
    stop_if_desired_non_running, stop_if_desired_running,
};

/// The answer of a state whose job's workers are not running the pipeline.
///
/// `Compiling`, `Scheduling`, `Restarting` and `Rescaling` all end in a *started* execution —
/// a replacement cluster, or the one they are checkpointing towards — so there is nothing to
/// take a final checkpoint of that the stop has not already been answered by, and every stop
/// mode ends the job now. That is `stop_if_desired_non_running!`'s mapping, invoked here
/// rather than restated.
pub(crate) fn leaves_not_running<S>(state: Box<S>, config: &JobConfig) -> LeavingForStop
where
    S: State + TransitionTo<Stopping>,
{
    fn answered<S>(state: Box<S>, config: &JobConfig) -> Result<Transition, Box<S>>
    where
        S: State + TransitionTo<Stopping>,
    {
        stop_if_desired_non_running!(state, config);
        Err(state)
    }

    LeavingForStop::of(answered(state, config))
}

/// The answer of a state whose job's workers are running the pipeline.
///
/// A running job can still be stopped the careful way, so `checkpoint` here means
/// `CheckpointStopping` and a final checkpoint rather than an immediate teardown. That is
/// `stop_if_desired_running!`'s mapping.
pub(crate) fn leaves_running<S>(state: Box<S>, config: &JobConfig) -> LeavingForStop
where
    S: State + TransitionTo<Stopping> + TransitionTo<CheckpointStopping>,
{
    fn answered<S>(state: Box<S>, config: &JobConfig) -> Result<Transition, Box<S>>
    where
        S: State + TransitionTo<Stopping> + TransitionTo<CheckpointStopping>,
    {
        stop_if_desired_running!(state, config);
        Err(state)
    }

    LeavingForStop::of(answered(state, config))
}

/// The answer of a worker-leader-mode state whose job's workers are running the pipeline.
///
/// The same shape as [`leaves_running`] against the leader-mode stop states, through
/// `leader_stop_if_desired_running!`. Leader mode is where the states that had *no* way at all
/// to learn about a stop live — `LeaderRescaling` and `LeaderCheckpointStopping` never read
/// the job's configuration again once they are entered — so this is what gives them one.
pub(crate) fn leaves_running_under_leader<S>(state: Box<S>, config: &JobConfig) -> LeavingForStop
where
    S: State + TransitionTo<LeaderStopping> + TransitionTo<LeaderCheckpointStopping>,
{
    fn answered<S>(state: Box<S>, config: &JobConfig) -> Result<Transition, Box<S>>
    where
        S: State + TransitionTo<LeaderStopping> + TransitionTo<LeaderCheckpointStopping>,
    {
        leader_stop_if_desired_running!(state, config);
        Err(state)
    }

    LeavingForStop::of(answered(state, config))
}
