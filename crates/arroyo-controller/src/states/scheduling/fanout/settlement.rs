//! The typed transfer interface (M11.T25c, design M11.D39b).
//!
//! An interrupted fan-out owes two things at once: the inventory of `StartExecution` requests
//! it issued, and the lifecycle authority under which it issued them. M11.D39b requires them
//! to move **as one unit**, to whatever cancellation-resistant owner the controller has, and
//! this module is that unit and that move.
//!
//! It is a child of [`super`] rather than a sibling because the inventory it carries is the
//! fan-out's own — [`AttemptLedger::settlement_rescue`](super::AttemptLedger::settlement_rescue)
//! builds the bundle from the live ledger — and because splitting "what a fan-out owes" from
//! "how it hands it over" would put the two halves of one obligation in two files.
//!
//! # The two ways a phase can be interrupted, and why both come here
//!
//! * The fan-out **returned** and the attempt cannot continue: [`super::super::phases::StartFanOut::issue`]
//!   builds a bundle and calls [`hand_over`] itself.
//! * The fan-out's **future was dropped** — the job's state task was cancelled — so no line
//!   after the `await` runs at all. Then the rescue inside
//!   [`settle_under_admission`](crate::states::settle_under_admission) is the only thing left
//!   holding the authority, and it builds the same bundle and calls the same function once the
//!   requests have settled. Routing only the first through this seam would leave an owner
//!   receiving nothing on the path it exists for.
//!
//! # Acceptance is observed, not reported
//!
//! An owner is code M11.T26 supplies, and the seam has to be safe against the ways it can be
//! wrong. So `take_over` can *decline* — it returns the obligation, and the phase settles in
//! place exactly as a controller with no owner does — and a
//! [`SettlementReceipt`] is issued only after [`SettlementBundle::transfer_to`] has checked
//! that the bundle did not die inside the call. An owner that drops what it was handed
//! releases the job's publication lock on the spot, and that is reported as
//! [`SettlementRefusal::Abandoned`] rather than as a transfer.
//!
//! Nor can an owner be *half* wrong about it: "kept the authority, dropped the inventory" —
//! or the reverse — is not a state a `take_over` can leave the world in. See
//! [`SettlementBundle::into_parts`], which is private, and review comment `5369004357`, which
//! is what happened while it was not.
//!
//! # A decline is not the same answer on both paths
//!
//! "The phase settles in place" is an answer only where there *is* a phase. On the rescued
//! path there is not, and review round 4 found the consequence: the rescue discarded the
//! [`SettlementOutcome`], so a declining owner released the job's lifecycle authority the
//! instant it said no, with whatever the reconcile budget had given up on still unaccounted
//! for. [`SettlementOutcome`] is therefore `#[must_use]`, and a declined obligation with
//! nothing left to answer for it goes to [`retain_without_a_phase`] rather than out of scope.
//!
//! **M11.T25 implements no [`SettlementOwner`]**, which is the point rather than an omission:
//! an owner is only safe once there is a durable record it can be recovered from after a
//! controller restart, and that record — the M11.D39d fence — is M11.T26's. Until then
//! [`PhaseContext::settlement_owner`](super::super::admission::PhaseContext::settlement_owner)
//! answers `None`, both paths settle in place, and the landed
//! [`settle_under_admission`](crate::states::settle_under_admission) rescue is what ends them.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{error, info, warn};

use super::IssuedAttempts;
use crate::states::Admission;

/// An interrupted fan-out's whole obligation: what it issued, and the authority that may not
/// be released until those attempts settle.
///
/// The two travel together deliberately. Handing over the inventory without the
/// [`Admission`] would leave a refusal publishable while the attempts were still live; handing
/// over the authority without the inventory would leave the new owner unable to say what it
/// was waiting for. M11.D39b requires them to move as one unit, and this type *is* that unit:
/// the fields and [`Self::into_parts`] are private, so an owner keeps the bundle whole or
/// parts with all of it, and "it kept both halves" is not something an implementation can
/// forget to do.
pub(crate) struct SettlementBundle {
    /// `None` once the bundle has been taken apart, which is what tells [`Drop`] whether the
    /// authority left through the seam or merely fell out of scope.
    admission: Option<Admission>,
    issued: IssuedAttempts,
    /// Raised by [`Drop`] when this bundle died with its authority still inside.
    ///
    /// Shared with [`Self::transfer_to`], which keeps a handle on it across the call into the
    /// owner. That is what makes acceptance something the transfer point *observes* rather than
    /// something the owner reports about itself: the flag is written by the one operation the
    /// owner cannot fake — the bundle ceasing to exist while it still held the job's
    /// publication lock — and it is read after `take_over` has returned.
    released_unsettled: Arc<AtomicBool>,
}

/// A proof that an obligation was handed over, and to how many attempts it applied.
///
/// Returned by [`SettlementBundle::transfer_to`] rather than by the owner, so that "the
/// transfer happened" is something this module observes rather than something an
/// implementation asserts about itself — and issued only when the observation says so. An
/// owner that declined, or that dropped what it was handed, produces a
/// [`SettlementRefusal`] instead; there is no path on which a receipt exists and the
/// obligation does not.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SettlementReceipt {
    outstanding: usize,
}

impl SettlementReceipt {
    /// How many issued attempts the new owner became responsible for.
    pub(crate) fn outstanding(&self) -> usize {
        self.outstanding
    }
}

/// Why an offered obligation was not taken over.
///
/// The two cases are not the same failure and must not be reported as one. A decline is
/// orderly — the owner said no and gave the obligation back intact, so the phase is still the
/// party that settles. An abandonment is a loss: the job's publication lock was released
/// inside `take_over` with attempts still outstanding, and by the time this is returned there
/// is nothing left to give back.
pub(crate) enum SettlementRefusal {
    /// The owner declined and returned the obligation, whole and unreleased.
    Declined(SettlementBundle),
    /// The owner neither kept the obligation nor returned it. `outstanding` is what the
    /// inventory said when it was offered, which is the last thing anybody knew about it.
    Abandoned { outstanding: usize },
}

/// The cancellation-resistant per-job owner an interrupted fan-out hands its obligation to.
///
/// **M11.T25 defines this and implements it nowhere** — see the module documentation.
///
/// An implementor takes the bundle by value: it receives the issued-attempt inventory and
/// the lifecycle authority together, and there is no way to receive one without the other —
/// nor to *keep* one without the other. See [`SettlementBundle`]: the halves are separable
/// only inside the module that observes the separation.
///
/// `Send + Sync` because an owner has to be reachable from the rescue that runs when the job's
/// state task has already been dropped, which is a detached task of its own. An owner that
/// could only be borrowed from the phase would be exactly the owner that is unreachable on the
/// path it exists for.
pub(crate) trait SettlementOwner: Send + Sync {
    /// Takes over an interrupted fan-out's obligation, or gives it back.
    ///
    /// The implementation must not release the [`Admission`] inside the bundle until every
    /// outstanding attempt has an authoritative outcome, an acknowledged fence or revoke that
    /// makes its identifier permanently inapplicable, or an observed termination of the
    /// worker generation it addressed.
    ///
    /// # Accepting
    ///
    /// An owner that accepts stores the bundle — all of it — somewhere that outlives this
    /// call, and reads what it owes through [`SettlementBundle::issued`], which takes nothing
    /// out of it. There is no half to store instead. The operations M11.T26 needs beyond
    /// reading — recording an attempt's outcome, and discharging the obligation once every
    /// attempt has one — belong beside [`SettlementBundle::transfer_to`], where releasing the
    /// authority is observed, and not in a widening of what an owner may take apart.
    ///
    /// # Declining
    ///
    /// An owner that cannot take responsibility — it is shutting down, it already holds an
    /// obligation for a newer generation of this job, its durable record is unavailable —
    /// returns `Err(bundle)`. The obligation goes back to the phase untouched, which settles
    /// it in place exactly as a controller with no owner at all does. That is the fail-closed
    /// answer, and it is the *only* correct way to say no: **dropping the bundle is never
    /// settlement and is never a decline.** A dropped bundle releases the job's publication
    /// lock on the spot, and [`SettlementBundle::transfer_to`] reports it as
    /// [`SettlementRefusal::Abandoned`] rather than issuing a receipt for it.
    fn take_over(&self, bundle: SettlementBundle) -> Result<(), SettlementBundle>;
}

/// What became of an interrupted fan-out's obligation.
///
/// `#[must_use]` because dropping this value is a decision and not a formality: the
/// [`SettledInPlace`](Self::SettledInPlace) arm carries the job's lifecycle authority, so a
/// caller that lets the outcome fall out of scope has released it. That is exactly what review
/// round 4 found at the region rescue, where the value was discarded as a statement — the
/// compiler now refuses it, and every site has to say which arm it is answering.
#[must_use = "this outcome can carry the job's lifecycle authority; dropping it releases the \
              authority behind attempts that may still be unaccounted for"]
pub(crate) enum SettlementOutcome {
    /// It was handed to an owner that outlives the phase, and the owner took it. Unreachable
    /// in M11.T25, which implements no [`SettlementOwner`].
    Transferred(SettlementReceipt),
    /// It stayed with the phase, which settled it before releasing anything — the landed
    /// M11.T08 behaviour, and the only outcome M11.T25 has.
    ///
    /// Reached both when there is no owner to offer it to and when the owner declined, because
    /// those are the same situation from the phase's side: nobody else is going to settle
    /// these attempts.
    ///
    /// **Only a phase can act on it.** The name is the returned path's answer, and the rescued
    /// path has no phase to give it back to — see [`retain_without_a_phase`], which is what
    /// that path does with this arm instead.
    SettledInPlace(Admission, IssuedAttempts),
    /// An owner was offered it, released the job's publication lock, and took responsibility
    /// for nothing. Unreachable in M11.T25 for the same reason as `Transferred`, and reported
    /// rather than papered over because the alternative is a fencing state that believes an
    /// obligation is somebody else's when it is nobody's.
    Abandoned { outstanding: usize },
}

/// What an interrupted phase carries into fencing after the obligation has been disposed of.
///
/// Counted rather than summed into one number because the two mean opposite things to whoever
/// reads the reconciliation: `transferred` attempts have an owner speaking for them, and
/// `abandoned` attempts have nobody.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HandoverRecord {
    /// Attempts an owner became responsible for.
    transferred: usize,
    /// Attempts an owner was offered and then lost.
    abandoned: usize,
}

impl HandoverRecord {
    /// Attempts an owner became responsible for.
    pub(crate) fn transferred(&self) -> usize {
        self.transferred
    }

    /// Attempts nobody is speaking for.
    pub(crate) fn abandoned(&self) -> usize {
        self.abandoned
    }
}

impl SettlementOutcome {
    /// What the interrupted phase takes into token-free fencing: the inventory it is still
    /// answerable for, and the record of what an owner took or lost.
    ///
    /// Releasing the authority is this method's job and not the caller's, deliberately. A
    /// phase that has been interrupted has no business holding an [`Admission`] again, and
    /// there is no arm of this that hands one back — so "the token is released into fencing
    /// and nowhere else" stays a property of the types rather than of the line the caller
    /// remembered to write.
    pub(crate) fn into_fencing_record(self) -> (IssuedAttempts, HandoverRecord) {
        match self {
            SettlementOutcome::SettledInPlace(admission, issued) => {
                drop(admission);
                (issued, HandoverRecord::default())
            }
            SettlementOutcome::Transferred(receipt) => (
                IssuedAttempts::default(),
                HandoverRecord {
                    transferred: receipt.outstanding(),
                    abandoned: 0,
                },
            ),
            SettlementOutcome::Abandoned { outstanding } => (
                IssuedAttempts::default(),
                HandoverRecord {
                    transferred: 0,
                    abandoned: outstanding,
                },
            ),
        }
    }
}

impl SettlementBundle {
    /// The obligation of a fan-out that is being interrupted.
    pub(crate) fn new(admission: Admission, issued: IssuedAttempts) -> Self {
        Self {
            admission: Some(admission),
            issued,
            released_unsettled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// What this bundle still owes.
    pub(crate) fn issued(&self) -> &IssuedAttempts {
        &self.issued
    }

    /// Takes the obligation apart: the authority, and the inventory it is answerable for.
    ///
    /// **Private, and the privacy is the invariant** (review comment `5369004357`). It is the
    /// one operation that separates the halves, and it also clears the field [`Drop`] reads —
    /// so an owner able to call it could drop the returned [`Admission`], return `Ok(())`, and
    /// have [`Self::transfer_to`] see an unraised flag and issue a [`SettlementReceipt`] for a
    /// job whose publication lock it had just opened. Dropping the returned [`IssuedAttempts`]
    /// is the same defect from the other side: the receipt says how many attempts moved and
    /// nobody holds a record of them.
    ///
    /// Its two callers are the two situations in which nothing was transferred at all — no
    /// owner, and an owner that gave the obligation back — so no receipt exists for a partial
    /// release to contradict, and no owner can reach either.
    fn into_parts(mut self) -> (Admission, IssuedAttempts) {
        let admission = self
            .admission
            .take()
            .expect("a settlement bundle is taken apart exactly once");
        (admission, std::mem::take(&mut self.issued))
    }

    /// Offers the whole obligation to `owner`, and says what the owner did with it.
    ///
    /// Consuming `self` is what makes the hand-over exclusive: the phase that transferred can
    /// no longer publish, reschedule or commit under the authority it gave away, because it
    /// no longer has it.
    ///
    /// # Why the answer is not the owner's to give
    ///
    /// An earlier revision returned a [`SettlementReceipt`] unconditionally, which made the
    /// receipt a statement about the *call* rather than about the obligation: an owner that
    /// dropped the bundle released the job's publication lock inside `take_over`, and the
    /// caller was still told the attempts had been transferred. The fencing state then
    /// recorded them as somebody's when they were nobody's.
    ///
    /// So acceptance is observed, at the one place it can be. A handle on
    /// [`Self::released_unsettled`] is kept across the call, and [`Drop`] raises it if the
    /// bundle dies with the authority still inside. After `take_over` returns there are
    /// exactly three states the world can be in, and each has its own answer:
    ///
    /// * the owner returned the bundle — it declined, nothing was released, and the phase
    ///   still owes the attempts;
    /// * the owner kept it — necessarily whole, since [`Self::into_parts`] is private, so it
    ///   is now the party that decides when the admission goes and a receipt is issued;
    /// * the owner dropped it — the flag is up, the lock is already gone, and there is no
    ///   receipt to issue for an obligation nobody holds.
    ///
    /// There is no fourth state in which the owner kept one half. That was the fourth state
    /// until review comment `5369004357`, and it produced a receipt.
    ///
    /// A `Drop` that happens *later*, after this returned, is the owner losing something it
    /// had accepted; that is its own failure and its own log line, not a transfer that never
    /// happened. An owner that `mem::forget`s the bundle is issued a receipt and never
    /// releases the authority — the conservative direction, and the only reason `Drop` is
    /// enough here: what it cannot observe is retention, not release.
    pub(crate) fn transfer_to<O: SettlementOwner + ?Sized>(
        self,
        owner: &O,
    ) -> Result<SettlementReceipt, SettlementRefusal> {
        let outstanding = self.issued().outstanding_count();
        let released_unsettled = Arc::clone(&self.released_unsettled);
        match owner.take_over(self) {
            Err(returned) => Err(SettlementRefusal::Declined(returned)),
            Ok(()) if released_unsettled.load(Ordering::SeqCst) => {
                Err(SettlementRefusal::Abandoned { outstanding })
            }
            Ok(()) => Ok(SettlementReceipt { outstanding }),
        }
    }

    /// Releases the obligation back to the phase that raised it, for a controller with no
    /// owner to transfer to.
    ///
    /// This is not a transfer and does not go through [`SettlementOwner`]: it is the
    /// statement that nothing was handed over, and the caller is still the one that must
    /// settle. M11.T25 always takes this branch.
    ///
    /// Private for the reason [`Self::into_parts`] is: an owner that could call it would have
    /// a second name for the same partial release.
    fn keep(self) -> (Admission, IssuedAttempts) {
        self.into_parts()
    }
}

/// Dropping an obligation is never settling it (M11.R59b).
///
/// A bundle that goes out of scope with its authority still inside has released the job's
/// publication lock without anybody having decided that the attempts it lists are accounted
/// for — a refusal becomes publishable behind requests a worker may still apply. Nothing in
/// M11.T25 can reach this, because both seams end in [`SettlementBundle::into_parts`]; it is
/// here because M11.T26's owner is the first code that could, and because a rule enforced only
/// by the two call sites that exist today is a rule the third one will break.
impl Drop for SettlementBundle {
    fn drop(&mut self) {
        if self.admission.is_none() {
            return;
        }
        // Raised before the log line, because this is the half a caller can act on:
        // `transfer_to` is holding a handle on it and turns it into a refusal rather than a
        // receipt. The log stays, for the drop that happens after any transfer point.
        self.released_unsettled.store(true, Ordering::SeqCst);
        error!(
            outstanding = self.issued.outstanding_count(),
            issued = self.issued.issued_count(),
            "an interrupted fan-out's obligation was dropped rather than taken apart: the job's \
             lifecycle authority is released here, and merely dropping the obligation is never \
             settlement of the attempts it lists"
        );
    }
}

/// Hands an interrupted fan-out's obligation to whatever owner the controller has.
///
/// One function rather than a branch at each call site, so that "there is no owner, therefore
/// the fan-out settles in place" is written once and is the same statement on both the
/// returned and the cancelled path.
pub(crate) fn hand_over(
    bundle: SettlementBundle,
    owner: Option<&dyn SettlementOwner>,
) -> SettlementOutcome {
    let Some(owner) = owner else {
        let (admission, issued) = bundle.keep();
        return SettlementOutcome::SettledInPlace(admission, issued);
    };
    match bundle.transfer_to(owner) {
        Ok(receipt) => {
            info!(
                outstanding = receipt.outstanding(),
                "transferred an interrupted fan-out's issued attempts and its lifecycle \
                 authority to the job's settlement owner"
            );
            SettlementOutcome::Transferred(receipt)
        }
        // Declining is orderly, and the answer to it is the answer to having no owner at all:
        // whoever offered the obligation still has it, whole and unreleased. *Who* that is
        // differs by path, which is why this function returns the obligation rather than
        // disposing of it — the returned path is a phase and settles in place, and the rescued
        // path is `retain_without_a_phase`.
        Err(SettlementRefusal::Declined(bundle)) => {
            let (admission, issued) = bundle.keep();
            warn!(
                outstanding = issued.outstanding_count(),
                "the job's settlement owner declined an interrupted fan-out's obligation; \
                 nothing was released, and whoever offered it still owes the attempts it lists"
            );
            SettlementOutcome::SettledInPlace(admission, issued)
        }
        Err(SettlementRefusal::Abandoned { outstanding }) => {
            error!(
                outstanding,
                "the job's settlement owner was handed an interrupted fan-out's obligation and \
                 dropped it: the job's lifecycle authority has been released with these \
                 attempts unaccounted for, and no receipt is issued for them"
            );
            SettlementOutcome::Abandoned { outstanding }
        }
    }
}

/// Disposes of an obligation nobody took, where there is no phase left to keep it.
///
/// [`SettlementOutcome::SettledInPlace`] is the *returned* path's answer: the phase keeps what
/// it was always able to keep, releases the authority into token-free
/// [`Fencing`](super::super::fencing::Fencing) through
/// [`SettlementOutcome::into_fencing_record`], and carries the inventory there with it, where
/// the fence reconciliation goes on accounting for the attempts. The rescued path has none of
/// that — the phase went with the job's state task, which is what being rescued means — so
/// "settles in place" is not an answer it can give.
///
/// Review round 4 found what happens when it is treated as one: the rescue discarded the
/// outcome, so a declining owner released the job's publication lock on the spot and the
/// inventory went with it, unread.
///
/// The disposal here is therefore by what is still owed:
///
/// * **Nothing outstanding.** Every attempt the region issued was answered before it ended, so
///   the obligation is discharged. The authority is released exactly where a controller with no
///   owner at all releases it, which is the landed M11.T08 behaviour of
///   [`settle_under_admission`](crate::states::settle_under_admission).
/// * **Something outstanding.** An attempt the fan-out stopped offering after spending its
///   reconcile budget, whose outcome nobody ever learned. Releasing the authority now is the one
///   thing this mechanism exists to prevent: a refusal would become publishable behind a
///   `StartExecution` a worker may still apply. Nobody took the obligation and nobody is left
///   who could, so the authority is **retained** — not released here, and not releasable
///   afterwards by anything in M11.T25.
///
/// # What retaining costs, and why this is the only place it is affordable
///
/// The job's publication lock is held for the remaining life of the controller process, and one
/// `Arc<Mutex<()>>` is never freed. That is affordable *here* and nowhere else, because of when
/// this runs: the rescue exists precisely because the job's state task has already been dropped,
/// so the lock being held is one no live phase of that job is waiting on. It is not a general
/// answer, and it is not offered as one — the general answer is M11.D39d's, which M11.T26 owns:
/// an acknowledged durable fence makes the outstanding identifiers permanently inapplicable, and
/// *that*, rather than the passage of time or a lack of anywhere else to put the token, is what
/// entitles anybody to release the authority standing behind them.
///
/// M11.T25 cannot reach this at all: [`PhaseContext::settlement_owner`](super::super::admission::PhaseContext::settlement_owner)
/// answers `None`, so [`AttemptLedger::settlement_rescue`](super::AttemptLedger::settlement_rescue)
/// answers `None` and `settle_under_admission` releases the admission itself, exactly as it did
/// before the seam existed.
pub(crate) fn retain_without_a_phase(admission: Admission, issued: IssuedAttempts) {
    let outstanding = issued.outstanding_count();
    if outstanding == 0 {
        info!(
            issued = issued.issued_count(),
            "the job's settlement owner declined a rescued fan-out's obligation, and every \
             attempt it issued had already been answered; the lifecycle authority is released \
             here, exactly as it is for a controller with no settlement owner"
        );
        drop(admission);
        return;
    }

    error!(
        outstanding,
        issued = issued.issued_count(),
        "the job's settlement owner declined a rescued fan-out's obligation while attempts it \
         issued are still unaccounted for, and the phase that would have settled them no longer \
         exists; the job's lifecycle authority is retained rather than released, so that no \
         refusal can be published behind a StartExecution a worker may still apply"
    );
    // Retaining *is* this: the authority's destructor never runs, so the job's publication lock
    // is never handed back. Releasing it is what would be unsafe, and there is no other party
    // left to hand it to.
    std::mem::forget(admission);
}
