//! Reading the job's single writer, at the points M11.D39a defines (M11.T25b, M11.D39a).
//!
//! A child module of [`super`] rather than a sibling, for the same reason [`super::execution`]
//! is: every read here needs [`PhaseContext`]'s own `ctx`, and those fields stay private to the
//! token API. What separates this from its parent is not subject matter but *direction* — the
//! parent says what a phase may **do** to a job, and this says what the job's writer has
//! **said** to the phase.
//!
//! There are three shapes of answer, and they differ only in what the caller is able to act on:
//!
//! * [`PhaseWait`], for a token-free wait, which may end early;
//! * [`Admitted`], for a crossing into irreversible work, which may not happen at all;
//! * [`FencedIntent`], for a job that has already been interrupted, whose only remaining
//!   question is what it should *report*.
//!
//! Nothing on the selected M11.T08 path reaches any of them: a [`PhaseContext`] exists only for
//! a job whose lifecycle is M11.D39a's single writer, and no production job has one through
//! M11.T25.

use super::{PhaseContext, Scheduling};
use crate::JobConfig;
use crate::states::lifecycle::{ConsumptionPoint, ObservedIntent};
use crate::states::{Admission, StateError, Transition, stop_if_desired_non_running};

/// What one turn of a token-free wait produced.
///
/// A wait is a loop, and the two things that can end it early are a stop request and a
/// message that decides the job's fate; everything else is another turn. Returning the
/// decision rather than performing it keeps the loop in [`super::super::phases`], where the
/// typestate can see it, instead of hidden in a helper that could quietly do something
/// irreversible.
pub(crate) enum PhaseWait {
    /// Nothing decisive happened; the phase should look at its own counters and, if they are
    /// not yet satisfied, wait again.
    Continue,
    /// The job leaves `Scheduling` for this state without reaching the next region.
    Leave(Transition),
}

/// What crossing into a region of irreversible work produced.
///
/// A crossing has three outcomes, not two, and the third is the one M11.D39a's consumption
/// points exist to make reachable: the job's single writer may have decided, since the last
/// look, that the job *stops*. A stop is not a failure — nothing is wrong, and the job must
/// end where a stop ends rather than where an error does — so it cannot be an `Err`; and it is
/// emphatically not a success to be carried into the region, because the regions are the
/// `StartExecution` fan-out and the publication of a restored checkpoint's commits.
///
/// So the admission itself is what a crossing has to *not* return when the job is stopping.
/// Leaving carries no token, which is the structural half of the claim: there is no value in
/// this enum from which a stopping job could perform an irreversible effect.
pub(crate) enum Admitted {
    /// The region was entered. Its authority is the job's publication lock, held until this
    /// value is dropped.
    Region(Admission),
    /// The job's writer decided it stops. Nothing was admitted, and the region's first effect
    /// has not run.
    Leave(Transition),
}

/// What the job's single writer has said since a scheduling attempt was interrupted.
///
/// The third shape, and the one that needs a distinction the other two deliberately discard:
/// whether the writer decided *anything at all*. A phase that is still running does the same
/// thing whether the writer adopted a new configuration or said nothing — it carries on — so
/// [`PhaseWait`] folds both into `Continue`. A job that is **fencing** does not: it is about to
/// report the reason its attempt ended, and a reason the writer has since superseded is a
/// reason that describes a configuration the job no longer has.
///
/// See [`Fencing::coalesce_intent`](super::super::fencing::Fencing::coalesce_intent) for what
/// each of these means for what gets reported.
pub(crate) enum FencedIntent {
    /// The writer has said nothing since the last look. The standing reason stands.
    Unchanged,
    /// The writer replaced the job's configuration, and the new one does not ask it to stop.
    /// Whatever this attempt was going to be reported as predates that.
    Superseded,
    /// The job's configuration now asks it to stop. The attempt ends as that stop.
    Leave(Transition),
}

/// The consumption points, and the stop test every one of them ends with.
impl PhaseContext<'_, '_> {
    /// The transition a job whose configuration asks it to stop makes instead of scheduling.
    pub(crate) fn stop_if_desired(&self) -> Option<Transition> {
        stop_transition(&self.ctx.config)
    }

    /// Crosses from a token-free phase into a region of irreversible work.
    ///
    /// Both halves of M11.D39a's boundary happen here and in this order: the job's single
    /// writer is read first, because a wait ends on the message that made its count and a
    /// decision published while it waited can be sitting behind that message unread; then the
    /// admission is taken, which under the landed M11.T08 gate re-reads the refusal that is
    /// the same decision reached by the other mechanism. A job that runs this path has an
    /// actor, so the first read is the operative one; taking the gate as well costs one
    /// uncontended lock and keeps the two mechanisms from diverging while both exist.
    ///
    /// A writer that has decided the job stops leaves through [`Admitted::Leave`] instead, and
    /// leaves *before* the admission is taken — not after. Taking it first and then noticing
    /// would put the decision on the far side of the lock whose whole purpose is to fence the
    /// region's effects, and the phase holding it would already be one method call from the
    /// fan-out.
    ///
    /// # Errors
    ///
    /// The fatal [`StateError`] of a refused configuration, from either mechanism.
    pub(crate) async fn admit(&mut self) -> Result<Admitted, StateError> {
        if let PhaseWait::Leave(stop) = self.observe(ConsumptionPoint::BeforeIrreversiblePhase)? {
            return Ok(Admitted::Leave(stop));
        }
        // Reclaimed before the lock is awaited, and that ordering is the whole of PR #167
        // round 3. A predecessor whose fan-out gave up on an unsettled request handed its
        // inventory *and* this job's authority to the settlement owner, which retains the
        // authority until every identifier is accounted for — and cannot account for one by
        // itself: an issued identifier is superseded only by an acknowledgement of a fence above
        // the one it was issued under, raising the fence is an adoption, and an adoption is this
        // attempt's first effect under the very authority being held. Awaiting the lock there is
        // a job that never moves again, however the world heals.
        //
        // Taking the obligation back is not taking a shortcut past it: the authority and the
        // inventory move together, exactly as they did on the way out, so this attempt is
        // answerable for both — and its preamble is what settles them, by advancing the fence it
        // adopts at every target the durable record names.
        if let Some(admission) = self.reclaim_transferred_obligation() {
            return Ok(Admitted::Region(admission));
        }
        Ok(Admitted::Region(
            self.ctx.admit_irreversible_scheduling().await,
        ))
    }

    /// Reads the job's single writer on a turn of a wait (M11.D39a's second consumption
    /// point).
    pub(crate) fn observe_intent_in_wait(&mut self) -> Result<PhaseWait, StateError> {
        self.observe(ConsumptionPoint::InsideInterruptibleWait)
    }

    /// Reads the job's single writer immediately before a stretch of work that is not
    /// irreversible in itself but *prepares* one (M11.D39a's first consumption point).
    ///
    /// [`AwaitingTasks::admit_commit_publish`](super::super::phases::AwaitingTasks::admit_commit_publish)
    /// is the caller: the handover it runs first moves the restored checkpoint's commits into
    /// the job controller, and a stop read on the far side of that move would be a stop read
    /// after the thing it exists to prevent had been assembled.
    pub(crate) fn observe_before_phase(&mut self) -> Result<PhaseWait, StateError> {
        self.observe(ConsumptionPoint::BeforeIrreversiblePhase)
    }

    /// Reads the job's single writer while the attempt is already fencing.
    ///
    /// The same consumption point as [`Self::observe_intent_in_wait`] — a fencing job is
    /// waiting, and the M11.D39a rule is that every interruptible wait consumes — but it keeps
    /// the fact that a decision *was made*, which the wait has no use for and fencing does.
    ///
    /// The stop test is unconditional and made against the job's configuration, exactly as in
    /// [`Self::observe`] and for the same reason: publication writes the stop into
    /// `ctx.config`, so a stop that arrived by any route is caught rather than only the one
    /// this call happened to observe. It is checked *before* supersession is reported, because
    /// a decision that both replaces the configuration and asks the job to stop is a stop —
    /// that is what `stop_wins_over_refusal` means, read at this end of the mechanism.
    ///
    /// # Errors
    ///
    /// The fatal [`StateError`] of a refused configuration — a *newer* refusal than the one
    /// that may already be standing.
    pub(crate) fn observe_intent_in_fencing(&mut self) -> Result<FencedIntent, StateError> {
        let decided = self
            .ctx
            .observe_lifecycle_decision(ConsumptionPoint::InsideInterruptibleWait)?;
        if let Some(stop) = self.stop_if_desired() {
            return Ok(FencedIntent::Leave(stop));
        }
        Ok(match decided {
            // A decision that leaves the job running is still a decision: it is the writer
            // saying that *this* is the job's configuration now, which is exactly what a
            // standing refusal claims it is not.
            Some(ObservedIntent::Continue | ObservedIntent::Adopted(_) | ObservedIntent::Stop) => {
                FencedIntent::Superseded
            }
            None => FencedIntent::Unchanged,
        })
    }

    /// Reads the job's single writer, and says whether the job is now leaving `Scheduling`.
    ///
    /// The stop test is made against the job's *configuration* rather than against which
    /// decision the writer reached, and unconditionally rather than only when something was
    /// decided. Both are deliberate. Publication writes the stop into `ctx.config`, so the
    /// configuration is where the answer is; and asking the same question
    /// [`schedule`](super::super::phases::schedule) asks on entry — through the same
    /// `stop_if_desired_non_running!` macro, so the two cannot disagree about what a stop mode
    /// means — catches a stop that reached the configuration by any route, not only the one
    /// this call just observed.
    fn observe(&mut self, at: ConsumptionPoint) -> Result<PhaseWait, StateError> {
        let observed = self.ctx.observe_lifecycle_intent(at)?;
        Ok(match (observed, self.stop_if_desired()) {
            // Either reading is enough to leave, and the configuration is the one that names
            // the transition: that mapping lives in one macro and this is not a second copy of
            // it.
            (_, Some(stop)) => PhaseWait::Leave(stop),
            // `ObservedIntent::Stop` *is* `stop_mode != none`, and every mode but `none` names
            // a transition, so reaching here means both readings say the job carries on. The
            // observation is matched rather than dropped because what a phase does with
            // `Continue` is enter the fan-out.
            //
            // `Adopted` carries nothing further for a phase, and that is a property of what a
            // phase does rather than a convenience: `Scheduling` is where a cluster is
            // *started*, from `ctx.config` — which is what the writer published the adopted
            // configuration into — so a phase reads the new value by reading the field it was
            // always going to read. The restart and rescale classification an adopted
            // configuration also needs belongs to a job whose workers are already running, and
            // that is `Running`/`LeaderRunning`'s (PR #160 review comment `5365261487`). The
            // selector guard the landed loops make with `check_config_update` is made by the
            // writer, which refuses a selector change rather than adopting it.
            (
                ObservedIntent::Continue | ObservedIntent::Adopted(_) | ObservedIntent::Stop,
                None,
            ) => PhaseWait::Continue,
        })
    }
}

/// The transition a stop-requesting configuration produces, if it requests one.
///
/// The landed path spells this `stop_if_desired_non_running!`, which `return`s from the state
/// body; the macro is used here verbatim rather than reimplemented so that the two routes
/// cannot come to disagree about what each stop mode means.
pub(crate) fn stop_transition(config: &JobConfig) -> Option<Transition> {
    fn wanted(config: &JobConfig) -> Result<Transition, ()> {
        let state = Box::new(Scheduling {});
        stop_if_desired_non_running!(state, config);
        Err(())
    }
    wanted(config).ok()
}
