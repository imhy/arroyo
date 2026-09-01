//! The cancellation-resistant per-job settlement owner (M11.T26e, design M11.D39b, M11.D39e(v)).
//!
//! M11.T25 defined [`SettlementOwner`] and implemented it nowhere, because an owner is only safe
//! once the obligation it holds can be recovered after the process holding it is gone. This is
//! that owner. It accepts the whole [`SettlementBundle`] — the identifiers an interrupted
//! fan-out issued *and* the lifecycle authority it issued them under — keeps it somewhere that
//! outlives the phase, and releases the authority only when every one of those identifiers has
//! been accounted for by one of M11.D39e(v)'s three facts.
//!
//! # Why it lives here and not in the phase graph
//!
//! The path an owner exists for is an *interrupted* phase: a fan-out that ends without every
//! attempt settled reaches `Interrupted`, which hands the inventory and the authority over as
//! one unit rather than releasing either. (The other way a fan-out can end — the job's state
//! task being dropped whole by the controller's shutdown token — takes the requests and this
//! owner with it, and is answered durably instead: what the attempt owed is in the job's row,
//! and its successor re-adopts and raises the fence before any effect. M11.T08 answered that
//! case with an in-process region rescue, which M11.T26h removed with the rest of the
//! mechanism the fence supersedes.) So it is a per-job value held beside the job's lifecycle
//! mechanism, it is handed out as an `Arc`, and the ledger holds one — which is what keeps
//! it alive after everything else about the job's task has gone.
//!
//! # Abandonment: one half impossible, one half loud
//!
//! `Drop` is skippable. `mem::forget`, an abort and a leak all skip it, so a guarantee that must
//! hold on *every* path cannot rest on it. The two halves are therefore carried by two different
//! mechanisms, and each is only asked to do the thing it can do:
//!
//! * **Impossible** — by the compiler, for this owner. [`Kept`] is the only value
//!   [`Self::keep`] can answer `Ok` with; its field is private to this module and its only
//!   constructor is [`Kept::store`], which *performs* the write it is proof of and takes the
//!   bundle by value to do it. There is one bundle, and it is moved either into the `Err` that
//!   gives it back or into the slot that keeps it — so an implementation of `take_over` that
//!   dropped what it was handed and answered `Ok(())` is not a bug this module could contain:
//!   there would be nothing left to build the `Ok` out of.
//! * **Loud** — by `Drop`, for everybody else. [`SettlementBundle`]'s destructor raises the flag
//!   `SettlementBundle::transfer_to` is holding across the call, which turns a dropped
//!   obligation into `SettlementRefusal::Abandoned` rather than a receipt, and logs it. That is
//!   what covers an owner this module did not write.
//!
//! And when *this* owner is dropped still holding an obligation, which is its own drop path and
//! not a transfer that never happened: it discharges what is fully accounted for and **retains**
//! the rest. Releasing the job's publication lock behind an identifier nobody accounted for is
//! the one thing the mechanism exists to prevent, and a destructor is not a place to decide
//! otherwise.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tracing::{error, info, warn};

use crate::states::scheduling::fanout::{
    Accounted, Discharged, Observed, SettlementBundle, SettlementOwner,
};

/// The job's owner of an interrupted fan-out's obligation.
///
/// One per job, created with the job's lifecycle mechanism — see
/// [`JobLifecycle`](super::JobLifecycle), which builds one exactly when it builds the D39a single
/// writer, so a job cannot have a settlement owner and a legacy decider or the reverse.
///
/// It holds at most one obligation. A job has one scheduling attempt at a time and an attempt's
/// authority is the job's publication lock, so a second obligation offered while the first is
/// still held is a state this owner declines rather than merges: whoever offered it keeps it,
/// whole, which is the fail-closed answer and the only correct way to say no.
pub(crate) struct JobSettlementOwner {
    job_id: Arc<String>,
    /// The obligation, whole, or nothing. `None` after it has been discharged, which is what
    /// lets the same owner speak for the job's next attempt.
    held: Mutex<Option<SettlementBundle>>,
}

/// Proof that an obligation was stored where it outlives the phase that raised it.
///
/// The field is private to this module and [`Self::store`] is the only constructor, so there is
/// no expression of this type that does not correspond to a bundle now sitting in the owner's
/// slot. That is what makes "`take_over` answered `Ok` and kept what it was handed" a fact the
/// compiler establishes rather than one the implementation remembered — see the module
/// documentation on which half of the abandonment guarantee this carries.
struct Kept(());

impl Kept {
    /// Stores `bundle` in `slot`, and is the proof that it did.
    ///
    /// Taking the bundle **by value** is the load-bearing part. A version of this that took a
    /// reference, or that took nothing, could be called on a path that had already dropped what
    /// it was given; this one cannot exist without the obligation having been moved into the
    /// slot, because moving it in is what building it consists of.
    fn store(slot: &mut Option<SettlementBundle>, bundle: SettlementBundle) -> Self {
        *slot = Some(bundle);
        Kept(())
    }

    /// What the trait's `Ok` is, once the store has happened.
    fn into_acceptance(self) {}
}

/// What one observation did to the obligation this owner holds.
///
/// `#[must_use]` because each arm is a different situation and three of them are quiet: an
/// observation about another generation, an observation about an obligation this owner does not
/// have, and an observation that left identifiers outstanding all look like "nothing happened"
/// unless the caller reads which one it was.
#[must_use = "an observation may have accounted for nothing, or may have discharged the job's \
              lifecycle authority; the caller has to be able to tell those apart"]
#[derive(Debug)]
pub(crate) enum Progress {
    /// This owner holds no obligation, so there was nothing for the observation to account for.
    ///
    /// Not an error and not a refusal. It is what an observation about a job whose fan-out
    /// settled normally looks like, and the honest answer is that nobody here is waiting for it.
    NothingHeld,
    /// The observation is not about the obligation this owner holds, so it accounted for
    /// nothing. See [`Disagreement`](crate::states::scheduling::fanout::Disagreement).
    NotThisObligation,
    /// Recorded, and identifiers this obligation issued are still unaccounted for.
    StillOwed {
        /// How many.
        outstanding: usize,
    },
    /// Recorded, it was the last identifier owed, and the job's lifecycle authority has been
    /// released.
    Discharged(Discharged),
}

impl JobSettlementOwner {
    /// The settlement owner for one job.
    ///
    /// An `Arc` because that is the only shape in which it is useful: the ledger of a fan-out
    /// captures one so the region rescue can reach it after the phase, the context that issued
    /// the fan-out holds one, and neither may be the sole owner of the value.
    pub(crate) fn for_job(job_id: Arc<String>) -> Arc<Self> {
        Arc::new(Self {
            job_id,
            held: Mutex::new(None),
        })
    }

    /// Records one observed fact, and discharges the obligation if it was the last one owed.
    ///
    /// Discharge is attempted rather than predicted: this asks
    /// [`SettlementBundle::discharge`](crate::states::scheduling::fanout::SettlementBundle) to
    /// release the authority and takes its answer, instead of counting what is outstanding and
    /// deciding for itself. The bundle either returns a proof that every identifier is accounted
    /// for — in which case the authority is already gone — or hands the whole obligation back,
    /// which is what goes into the slot again. There is no arm on which this owner has released
    /// the authority and still holds identifiers, and none on which it holds an obligation it
    /// has already discharged.
    pub(crate) fn observe(&self, observed: &Observed) -> Progress {
        let mut held = self.held();
        if let Some(bundle) = held.as_mut() {
            match bundle.observe(observed) {
                Accounted::NotThisObligation(disagreement) => {
                    warn!(
                        job_id = %self.job_id,
                        worker_id = observed.worker().0,
                        generation = observed.generation(),
                        disagreement = ?disagreement,
                        "an observation offered to this job's settlement owner is not about the \
                         obligation it holds, and accounts for nothing in it"
                    );
                    return Progress::NotThisObligation;
                }
                // Recorded now, or recorded by something earlier. Either way this identifier is
                // accounted for.
                Accounted::Settled(_) | Accounted::Already(_) => {}
            }
        }
        // Holding nothing, or holding an obligation one of whose identifiers has just been
        // accounted for: the question that follows is the same one either way, and it is asked
        // in one place rather than answered here for one of the two.
        self.settle(&mut held)
    }

    /// Discharges what the slot holds, if the obligation says every identifier is accounted for.
    ///
    /// **The one place the authority may leave this owner**, and it is the obligation rather
    /// than this method that decides: the bundle either answers with the proof that its whole
    /// inventory is accounted for — in which case it has already released the authority itself —
    /// or hands the obligation back whole, which goes straight into the slot again. Nothing here
    /// counts anything, and there is no arm on which the slot ends up empty and the authority
    /// still somewhere.
    ///
    /// [`Progress::NotThisObligation`] is not reachable from here: this asks nothing about an
    /// observation.
    fn settle(&self, held: &mut Option<SettlementBundle>) -> Progress {
        let Some(bundle) = held.take() else {
            // An owner holding nothing. Not an error and not a refusal: it is what an
            // observation about a job whose fan-out settled normally looks like, and the honest
            // answer is that nobody here was waiting for it.
            return Progress::NothingHeld;
        };
        match bundle.discharge() {
            Ok(discharged) => {
                info!(
                    job_id = %self.job_id,
                    identifiers = discharged.count(),
                    "every identifier an interrupted fan-out of this job issued is accounted \
                     for; its settlement owner has released the lifecycle authority"
                );
                Progress::Discharged(discharged)
            }
            Err(bundle) => {
                let outstanding = bundle.issued().outstanding_count();
                *held = Some(bundle);
                Progress::StillOwed { outstanding }
            }
        }
    }

    /// Stores the obligation, or gives it back.
    ///
    /// The whole of [`SettlementOwner::take_over`]'s decision, written so that its `Ok` cannot be
    /// produced except by the store: see [`Kept`]. The one reason to decline is that this owner
    /// already holds an obligation for this job — a second one would have to be merged with the
    /// first, and merging two attempts' inventories is how an identifier from the older one
    /// comes to be accounted for by an answer about the newer.
    ///
    /// # Errors
    ///
    /// The bundle, whole and unreleased, which is what a decline *is*. Dropping it would be
    /// neither settlement nor a decline, and
    /// [`SettlementBundle::transfer_to`](crate::states::scheduling::fanout::SettlementBundle)
    /// reports that as an abandonment rather than issuing a receipt for it.
    fn keep(&self, bundle: SettlementBundle) -> Result<Kept, SettlementBundle> {
        let mut held = self.held();
        if let Some(standing) = held.as_ref() {
            warn!(
                job_id = %self.job_id,
                standing_outstanding = standing.issued().outstanding_count(),
                standing_generation = standing.issued().generation(),
                offered_outstanding = bundle.issued().outstanding_count(),
                offered_generation = bundle.issued().generation(),
                "this job's settlement owner already holds an interrupted fan-out's obligation, \
                 so it declines the one offered: whoever offered it keeps it, whole and \
                 unreleased"
            );
            return Err(bundle);
        }
        let kept = Kept::store(&mut held, bundle);
        // An obligation offered with nothing outstanding — every request answered, and the
        // fan-out interrupted for some other reason — is discharged here rather than held. There
        // is nothing left for this owner to wait for, and keeping the job's publication lock for
        // it would stop the job's next scheduling attempt from ever taking one. It happens
        // *after* the store so that the acceptance above is still the only way to reach this
        // line at all.
        let _settled = self.settle(&mut held);
        Ok(kept)
    }

    /// The obligation slot.
    ///
    /// Poisoning is recovered from rather than propagated, for the reason
    /// `AttemptLedger::attempts` gives: nothing under this lock can leave the slot inconsistent
    /// — the operations are a move in, a move out and a record — so a panic elsewhere must not
    /// also cost the controller the ability to answer for what a fan-out issued. Propagating it
    /// would panic every later observation, and the obligation would then be released by the
    /// owner's own destructor with identifiers still outstanding.
    fn held(&self) -> MutexGuard<'_, Option<SettlementBundle>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl SettlementOwner for JobSettlementOwner {
    fn take_over(&self, bundle: SettlementBundle) -> Result<(), SettlementBundle> {
        self.keep(bundle).map(Kept::into_acceptance)
    }
}

/// Losing an obligation is a failure to report, never a release (M11.D39e(v)).
///
/// This runs when the last handle to the owner goes — the job's state machine and every rescue
/// that captured it are gone — and it is a *drop path*, which is exactly why it decides as
/// little as possible. It does not "clean up": it asks the obligation whether every identifier
/// it issued is accounted for, and releases the authority only if the answer is yes, which is
/// the same question every other release is gated on.
///
/// If the answer is no, the authority is retained. The job's admission is then held for the
/// remaining life of the controller process. That is affordable here: by the time this runs
/// there is no live phase of this job waiting on it, and the alternative is a refusal becoming
/// publishable behind a `StartExecution` a worker may still apply. What makes the retention
/// recoverable rather than permanent is M11.D39d's durable record, which M11.T26f owns.
impl Drop for JobSettlementOwner {
    fn drop(&mut self) {
        let held = self
            .held
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let Some(bundle) = held else {
            return;
        };
        error!(
            job_id = %self.job_id,
            outstanding = bundle.issued().outstanding_count(),
            issued = bundle.issued().issued_count(),
            "this job's settlement owner is being dropped while it still holds an interrupted \
             fan-out's obligation"
        );
        match bundle.discharge() {
            // **Unreachable in this build, and handled rather than asserted away.** The slot is
            // private to this module and exactly two expressions write a bundle into it:
            // [`Kept::store`], which [`Self::keep`] follows with [`Self::settle`] under the same
            // guard, and `settle`'s own `Err` arm, which by construction holds a bundle
            // `discharge` refused. So every bundle observable here has at least one unaccounted
            // identifier and this arm cannot be the one taken.
            //
            // It stays because the alternative on a drop path is an `expect`, which would turn
            // an argument about today's two writers into a panic in a destructor; and because a
            // third writer added later would make it live rather than make it wrong.
            // `the_only_ok_this_owner_can_answer_is_the_one_the_store_produces` counts the
            // writers, and
            // `dropping_the_owner_after_everything_is_accounted_for_releases_the_authority`
            // shows the discharge happening where it does happen — before the drop.
            Ok(discharged) => {
                info!(
                    job_id = %self.job_id,
                    identifiers = discharged.count(),
                    "every identifier it listed was accounted for, so the lifecycle authority is \
                     released here rather than retained"
                );
            }
            Err(bundle) => bundle.retain_unsettled(
                "this job's settlement owner was dropped while identifiers it was answerable for \
                 were unaccounted for",
            ),
        }
    }
}

// ---------------------------------------------------------------------------------------
// Test-only reach into what this owner is holding.
//
// Declared below the whole production half, for the reason `scheduling/fanout.rs` records: a
// `#[cfg(test)]` placed higher truncates any source pin that cuts a file at its first one.
// ---------------------------------------------------------------------------------------

#[cfg(test)]
impl JobSettlementOwner {
    /// How many identifiers this owner is still answerable for.
    ///
    /// `None` when it holds no obligation, which is a different answer from `Some(0)`: the
    /// second would mean it holds one that everything has accounted for, and there is no moment
    /// at which that is true — [`Self::observe`] discharges such an obligation rather than
    /// keeping it, so a row that saw `Some(0)` would have found a released authority still in
    /// somebody's hands.
    pub(crate) fn outstanding(&self) -> Option<usize> {
        self.held()
            .as_ref()
            .map(|bundle| bundle.issued().outstanding_count())
    }

    /// The identifiers it is holding, by target, whatever has accounted for them.
    pub(crate) fn holding(
        &self,
    ) -> Option<
        Vec<(
            u64,
            String,
            Option<crate::states::scheduling::fanout::Accounting>,
        )>,
    > {
        self.held().as_ref().map(|bundle| {
            bundle
                .issued()
                .records()
                .map(|(worker, record)| (worker.0, record.attempt_id.clone(), record.accounted))
                .collect()
        })
    }
}
