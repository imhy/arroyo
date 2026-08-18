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
//! **M11.T25 implements no [`SettlementOwner`]**, which is the point rather than an omission:
//! an owner is only safe once there is a durable record it can be recovered from after a
//! controller restart, and that record — the M11.D39d fence — is M11.T26's. Until then
//! [`PhaseContext::settlement_owner`](super::super::admission::PhaseContext::settlement_owner)
//! answers `None`, both paths settle in place, and the landed
//! [`settle_under_admission`](crate::states::settle_under_admission) rescue is what ends them.

use tracing::{error, info};

use super::IssuedAttempts;
use crate::states::Admission;

/// An interrupted fan-out's whole obligation: what it issued, and the authority that may not
/// be released until those attempts settle.
///
/// The two travel together deliberately. Handing over the inventory without the
/// [`Admission`] would leave a refusal publishable while the attempts were still live; handing
/// over the authority without the inventory would leave the new owner unable to say what it
/// was waiting for. M11.D39b requires them to move as one unit, and the only way to part with
/// either is [`Self::into_parts`], which yields both.
pub(crate) struct SettlementBundle {
    /// `None` once the bundle has been taken apart, which is what tells [`Drop`] whether the
    /// authority left through the seam or merely fell out of scope.
    admission: Option<Admission>,
    issued: IssuedAttempts,
}

/// A proof that an obligation was handed over, and to how many attempts it applied.
///
/// Returned by [`SettlementBundle::transfer_to`] rather than by the owner, so that "the
/// transfer happened" is something this module observes rather than something an
/// implementation asserts about itself.
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

/// The cancellation-resistant per-job owner an interrupted fan-out hands its obligation to.
///
/// **M11.T25 defines this and implements it nowhere** — see the module documentation.
///
/// An implementor takes the bundle by value: it receives the issued-attempt inventory and
/// the lifecycle authority together, and there is no way to receive one without the other.
///
/// `Send + Sync` because an owner has to be reachable from the rescue that runs when the job's
/// state task has already been dropped, which is a detached task of its own. An owner that
/// could only be borrowed from the phase would be exactly the owner that is unreachable on the
/// path it exists for.
pub(crate) trait SettlementOwner: Send + Sync {
    /// Takes over an interrupted fan-out's obligation.
    ///
    /// The implementation must not release the [`Admission`] inside the bundle until every
    /// outstanding attempt has an authoritative outcome, an acknowledged fence or revoke that
    /// makes its identifier permanently inapplicable, or an observed termination of the
    /// worker generation it addressed. Dropping the bundle is never settlement, and
    /// [`SettlementBundle`]'s own `Drop` says so.
    fn take_over(&self, bundle: SettlementBundle);
}

/// What became of an interrupted fan-out's obligation.
pub(crate) enum SettlementOutcome {
    /// It was handed to an owner that outlives the phase. Unreachable in M11.T25, which
    /// implements no [`SettlementOwner`].
    Transferred(SettlementReceipt),
    /// It stayed with the phase, which settled it before releasing anything — the landed
    /// M11.T08 behaviour, and the only outcome M11.T25 has.
    SettledInPlace(Admission, IssuedAttempts),
}

impl SettlementBundle {
    /// The obligation of a fan-out that is being interrupted.
    pub(crate) fn new(admission: Admission, issued: IssuedAttempts) -> Self {
        Self {
            admission: Some(admission),
            issued,
        }
    }

    /// What this bundle still owes.
    pub(crate) fn issued(&self) -> &IssuedAttempts {
        &self.issued
    }

    /// Takes the obligation apart: the authority, and the inventory it is answerable for.
    ///
    /// The one way to part with either, and therefore the one way a bundle is *settled*
    /// rather than merely gone. Whoever calls this is stating that it is now the party that
    /// decides when the admission is released.
    pub(crate) fn into_parts(mut self) -> (Admission, IssuedAttempts) {
        let admission = self
            .admission
            .take()
            .expect("a settlement bundle is taken apart exactly once");
        (admission, std::mem::take(&mut self.issued))
    }

    /// Hands the whole obligation to `owner`.
    ///
    /// Consuming `self` is what makes the hand-over exclusive: the phase that transferred can
    /// no longer publish, reschedule or commit under the authority it gave away, because it
    /// no longer has it.
    pub(crate) fn transfer_to<O: SettlementOwner + ?Sized>(self, owner: &O) -> SettlementReceipt {
        let outstanding = self.issued().outstanding_count();
        owner.take_over(self);
        SettlementReceipt { outstanding }
    }

    /// Releases the obligation back to the phase that raised it, for a controller with no
    /// owner to transfer to.
    ///
    /// This is not a transfer and does not go through [`SettlementOwner`]: it is the
    /// statement that nothing was handed over, and the caller is still the one that must
    /// settle. M11.T25 always takes this branch.
    pub(crate) fn keep(self) -> (Admission, IssuedAttempts) {
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
    match owner {
        Some(owner) => {
            let receipt = bundle.transfer_to(owner);
            info!(
                outstanding = receipt.outstanding(),
                "transferred an interrupted fan-out's issued attempts and its lifecycle \
                 authority to the job's settlement owner"
            );
            SettlementOutcome::Transferred(receipt)
        }
        None => {
            let (admission, issued) = bundle.keep();
            SettlementOutcome::SettledInPlace(admission, issued)
        }
    }
}
