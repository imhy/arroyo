//! What accounts for an identifier an interrupted fan-out issued, and what entitles the holder
//! of that obligation to release the authority behind it (M11.T26e, design M11.D39e(v)).
//!
//! M11.T25 gave [`SettlementBundle`] the two operations a *seam* needs — build one, offer the
//! whole of it to an owner — and deliberately gave it nothing an owner could decide anything
//! with, because the party that decides did not exist yet. This module is that party's half,
//! and it is a **child** of [`super`] rather than a widening of it for the reason M11.T25's
//! module documentation gives: the operations belong beside
//! [`SettlementBundle::transfer_to`](super::SettlementBundle::transfer_to), *"where releasing
//! the authority is observed, and not in a widening of what an owner may take apart"*. Being a
//! child is what lets them reach the private halves without anything else being able to.
//!
//! # The three facts, and nothing else
//!
//! M11.D39e(v): *every issued request is accounted for by an authoritative response, a
//! worker-acknowledged fence/revoke that makes the ID permanently non-applicable, or observed
//! target worker-generation termination.* [`Accounting`] is that list, closed, and [`Observed`]
//! is the only thing in this crate that produces one. So "the deadline expired", "the client
//! future was dropped", "the fence CAS went through" and "the row said so" are not readings
//! this module refuses — they are readings it has nothing to write with.
//!
//! # Why an observation is checked against the inventory rather than believed
//!
//! An issued identifier, the worker generation it addressed and the outcome observed for it are
//! **one fact**. Arriving separately they are three untrusted inputs, and the failure they
//! produce together is silent: an answer about the previous generation's attempt would account
//! for this generation's identifier, and the authority standing behind a request a worker may
//! still apply would be released on the strength of it. So [`SettlementBundle::observe`]
//! compares all three against the record the fan-out wrote when it issued the request, and a
//! disagreement in any of them accounts for nothing and says which one — see [`Disagreement`].
//!
//! M11.T26f added the fourth half of that identity, and it is the one a caller was previously
//! trusted with: the **height** of an acknowledged fence. An acknowledgement settles an issued
//! identifier only if it is of a fence *above* the one that identifier was issued under, because
//! a worker revokes what is below the fence it takes. So the acknowledgement arrives as
//! [`FenceAcknowledgement`] — built only by the module that observed the response, carrying the
//! height — and the inventory records the fence it issued under, and the comparison is made
//! here rather than remembered there.
//!
//! # Discharge is established, not counted
//!
//! [`SettlementBundle::discharge`] does not ask how many identifiers are outstanding. It folds
//! the whole inventory into a [`Discharged`], one record at a time, and the fold has no arm
//! that skips a record: a record with no [`Accounting`] ends it. The witness therefore cannot
//! exist while an identifier is unaccounted for, and the authority is released only on the arm
//! that holds one. There is no counter for a caller to get wrong, because there is no counter.

use arroyo_types::WorkerId;
use tracing::{error, info};

use super::super::{Accounting, IssuedAttempts};
use super::SettlementBundle;
use crate::states::lifecycle::handshake::FenceAcknowledgement;
use crate::states::lifecycle::recovery::ObservedTermination;

/// One observed fact about one target of an interrupted fan-out.
///
/// The three constructors are the three ways M11.D39e(v) allows an issued identifier to stop
/// being applicable, and each takes the target it is about — a worker, *and* the worker
/// generation the observation concerns — because an observation without a generation is an
/// observation that cannot be checked against what was issued.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Observed {
    worker: WorkerId,
    generation: u64,
    fact: Fact,
}

/// Which of the three facts this is. Private: the taxonomy is [`Accounting`], and this exists
/// only to carry the identifier the first of them names.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Fact {
    /// The worker's own answer about the identifier it was issued.
    AuthoritativeResponse { attempt_id: String },
    /// The generation acknowledged a fence, at the height it reported.
    ///
    /// The height is carried because an acknowledgement settles an identifier only if the fence
    /// acknowledged is above the one that identifier was issued under — see
    /// [`FenceAcknowledgement`] and [`Disagreement::Fence`].
    AcknowledgedFence { observed_fence: u64 },
    /// The generation was observed to have gone away.
    TerminatedGeneration,
}

impl Observed {
    /// The target generation's own authoritative answer about `attempt_id`.
    ///
    /// The identifier is carried because this is the one fact that is *about* an identifier
    /// rather than about a generation: a response is an answer to the request that carried it,
    /// and an answer about some other request accounts for nothing here.
    pub(crate) fn authoritative_response(
        worker: WorkerId,
        generation: u64,
        attempt_id: impl Into<String>,
    ) -> Self {
        Self {
            worker,
            generation,
            fact: Fact::AuthoritativeResponse {
                attempt_id: attempt_id.into(),
            },
        }
    }

    /// The target generation acknowledged a fence, and the revokes that came with it.
    ///
    /// No identifier: an acknowledged fence supersedes *every* identifier below it at that
    /// generation, which is what makes it settlement of the whole target rather than of one
    /// request (M11.D39d, M11.D39e(v)).
    ///
    /// **It takes the witness, not a worker and a generation.** M11.T26e's version took the
    /// two scalars, so an acknowledgement of a fence too low to revoke anything — the very fence
    /// this attempt's own starts carry, say — produced an `Observed` indistinguishable from one
    /// that had settled the target, and the caller was the only thing standing between that and
    /// a released authority. [`FenceAcknowledgement`] is built only by
    /// [`handshake`](crate::states::lifecycle::handshake) observing a `FENCE_ACKNOWLEDGED`
    /// response, and it carries the height, so the comparison below has something to make and a
    /// caller has nothing to remember.
    pub(crate) fn acknowledged_fence(acknowledgement: &FenceAcknowledgement) -> Self {
        Self {
            worker: acknowledgement.worker(),
            generation: acknowledgement.generation(),
            fact: Fact::AcknowledgedFence {
                observed_fence: acknowledgement.observed_fence(),
            },
        }
    }

    /// The target worker generation was observed to have terminated.
    ///
    /// The other identifier-free fact, and the one that answers a generation which never
    /// acknowledged anything: a generation that no longer exists cannot apply what was
    /// addressed to it.
    ///
    /// It takes [`ObservedTermination`] for the same reason the acknowledgement takes its
    /// witness: that type is built only where a scheduler's *successful* listing of a job's live
    /// generations was read, so "the deadline expired", "the channel broke" and "the listing
    /// failed" cannot be spelled as a termination here.
    pub(crate) fn terminated_generation(termination: &ObservedTermination) -> Self {
        Self {
            worker: termination.worker(),
            generation: termination.generation(),
            fact: Fact::TerminatedGeneration,
        }
    }

    /// The worker this observation is about.
    pub(crate) fn worker(&self) -> WorkerId {
        self.worker
    }

    /// The worker generation this observation is about.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Which of M11.D39e(v)'s three facts this is.
    pub(crate) fn accounting(&self) -> Accounting {
        match &self.fact {
            Fact::AuthoritativeResponse { .. } => Accounting::AuthoritativeResponse,
            Fact::AcknowledgedFence { .. } => Accounting::AcknowledgedFence,
            Fact::TerminatedGeneration => Accounting::TerminatedGeneration,
        }
    }
}

/// What an observation did to an obligation.
///
/// `#[must_use]` because the only way to get this wrong is to observe something and carry on as
/// though it had settled what it named: a disagreement is *not* settlement, and a caller that
/// drops this value has stopped being able to tell the two apart.
#[must_use = "an observation that named nothing this obligation issued has settled nothing, and \
              the caller has to be able to tell that from one that did"]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Accounted {
    /// The observation named an identifier this obligation issued, which is now accounted for.
    Settled(Accounting),
    /// It named one that something had already accounted for.
    ///
    /// Idempotent rather than an error: a reconciliation that is safe to run repeatedly is one
    /// that says the same thing the second time, and the fact that accounted for the identifier
    /// first is the one that stands.
    Already(Accounting),
    /// It is not about anything this obligation issued, so it accounts for nothing.
    NotThisObligation(Disagreement),
}

/// Which of the three halves of one identity the observation and the inventory disagreed about.
///
/// Named rather than collapsed into a bare "no", because the three are different failures with
/// different causes: a stale generation is a message from a superseded attempt, an unknown
/// target is an observation about a worker this fan-out never addressed, and a mismatched
/// identifier is an answer to some other request from the right worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Disagreement {
    /// The observation is about a worker generation this obligation did not address.
    Generation {
        /// The generation the observation names.
        observed: u64,
        /// The generation this obligation's identifiers were issued into.
        addressed: u64,
    },
    /// The observation is about a worker this obligation issued nothing to.
    NotIssuedTo {
        /// The worker the observation names.
        worker: u64,
    },
    /// The observation answers an identifier that is not the one this target was issued.
    Identifier {
        /// The worker the observation names.
        worker: u64,
        /// The identifier it answers.
        observed: String,
        /// The identifier this obligation issued to that worker.
        issued: String,
    },
    /// The acknowledgement is of a fence too low to have revoked what this obligation issued.
    ///
    /// A worker admits a directive carrying the fence it holds and refuses one below it, so a
    /// generation acknowledging fence *f* has made everything issued under *f - 1* permanently
    /// inapplicable and has changed nothing about what was issued under *f*. This is the
    /// disagreement M11.T26e could not express, and the one that would have released the job's
    /// lifecycle authority behind a `StartExecution` a worker may still apply.
    Fence {
        /// The worker the observation names.
        worker: u64,
        /// The height that generation reported.
        observed: u64,
        /// The fence this obligation's identifiers were issued under.
        issued_under: u64,
    },
}

/// Every identifier an obligation issued, each with the fact that accounted for it.
///
/// The proof [`SettlementBundle::discharge`] releases the authority on, and it is a proof rather
/// than a report because of how it is built: [`SettlementBundle::fully_accounted`] visits every
/// record in the inventory and ends at the first one nothing has accounted for, so no value of
/// this type describes an obligation with an outstanding identifier. Its field is private and
/// that fold is its only constructor.
#[must_use = "this is the proof that every issued identifier was accounted for; it is what the \
              release of the job's lifecycle authority was justified by"]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Discharged {
    accounted: Vec<(WorkerId, String, Accounting)>,
}

impl Discharged {
    /// Every identifier, the target it was issued to, and what accounted for it.
    pub(crate) fn accounted(&self) -> &[(WorkerId, String, Accounting)] {
        &self.accounted
    }

    /// How many identifiers the discharged obligation listed.
    pub(crate) fn count(&self) -> usize {
        self.accounted.len()
    }
}

impl IssuedAttempts {
    /// Records one observed fact against this inventory, if it is about this inventory.
    ///
    /// **The one validated recording operation.** Every route by which an identifier stops being
    /// outstanding goes through here — the fan-out's own answers through
    /// [`AttemptLedger::answered`](super::super::AttemptLedger::answered), a fencing job's
    /// acknowledgements and terminations, and an owner's through
    /// [`SettlementBundle::observe`] — so the check below is not a thing one of them could be
    /// written without.
    ///
    /// The three checks are one check of one identity, in the order that makes each meaningful:
    /// the generation says whether the observation is about this attempt at all, the target says
    /// whether this attempt addressed that worker, and — for the one fact that names an
    /// identifier — the identifier says whether the answer is to the request this inventory is
    /// waiting on. Any of them disagreeing accounts for nothing, and says which.
    ///
    /// Recording is idempotent: an identifier something has already accounted for keeps the fact
    /// that accounted for it first, so a reconciliation may be run on every turn of whatever
    /// loop it is in without tracking whether it already has.
    pub(crate) fn observe(&mut self, observed: &Observed) -> Accounted {
        let addressed = self.generation();
        if observed.generation != addressed {
            // A message from, or about, some other scheduling generation. Reading it as
            // settlement here would account for this generation's identifier with an answer
            // that was never about it.
            return Accounted::NotThisObligation(Disagreement::Generation {
                observed: observed.generation,
                addressed,
            });
        }
        let Some((issued_id, already)) = self
            .record(observed.worker)
            .map(|record| (record.attempt_id.clone(), record.accounted))
        else {
            return Accounted::NotThisObligation(Disagreement::NotIssuedTo {
                worker: observed.worker.0,
            });
        };
        if let Fact::AuthoritativeResponse { attempt_id } = &observed.fact
            && attempt_id != &issued_id
        {
            return Accounted::NotThisObligation(Disagreement::Identifier {
                worker: observed.worker.0,
                observed: attempt_id.clone(),
                issued: issued_id,
            });
        }
        if let Fact::AcknowledgedFence { observed_fence } = &observed.fact
            && *observed_fence <= self.fence()
        {
            // The right worker, in the right generation, acknowledging a fence that revoked
            // nothing this inventory is waiting on. It is the acknowledgement that reaches here
            // on the ordinary path — the fan-out's own handshake acknowledges exactly the fence
            // its starts then carry — so this arm is not a defence against a hypothetical.
            return Accounted::NotThisObligation(Disagreement::Fence {
                worker: observed.worker.0,
                observed: *observed_fence,
                issued_under: self.fence(),
            });
        }
        if let Some(by) = already {
            return Accounted::Already(by);
        }
        let accounting = observed.accounting();
        self.account(observed.worker, accounting);
        Accounted::Settled(accounting)
    }
}

impl SettlementBundle {
    /// Records one observed fact against this obligation, if it is about this obligation.
    ///
    /// The obligation's inventory *is* the record — see [`IssuedAttempts::observe`], which is
    /// what this delegates to. Delegating rather than repeating is the point: the fan-out's own
    /// answers and an owner's later observations are checked against the same identity by the
    /// same code, so an obligation cannot be settled by something the ledger that built it would
    /// have refused.
    pub(crate) fn observe(&mut self, observed: &Observed) -> Accounted {
        self.issued.observe(observed)
    }

    /// Releases the authority, if and only if every identifier this obligation issued has been
    /// accounted for.
    ///
    /// The release happens *here*, inside the module that owns the coupling, and nothing is
    /// handed back: this consumes the whole obligation and returns a proof, never an
    /// [`Admission`](crate::states::Admission). So it is not a third way to part with half of a
    /// bundle — see the inventory pinned by
    /// `the_source_of_a_settlement_bundle_exposes_no_way_to_part_with_half_of_it`.
    ///
    /// # Errors
    ///
    /// The obligation, whole and still holding the authority, when something it issued is still
    /// unaccounted for. Giving it back rather than reporting a count is what keeps the caller
    /// from being the party that decides: there is no arm of this on which an unsettled
    /// obligation and a released authority coexist.
    pub(crate) fn discharge(self) -> Result<Discharged, Self> {
        let Some(discharged) = self.fully_accounted() else {
            return Err(self);
        };
        let (admission, issued) = self.into_parts();
        for (worker, attempt_id, accounting) in discharged.accounted() {
            info!(
                worker_id = worker.0,
                attempt_id = attempt_id,
                accounted_by = accounting.as_str(),
                "an issued StartExecution is accounted for"
            );
        }
        info!(
            identifiers = issued.issued_count(),
            generation = issued.generation(),
            "every identifier an interrupted fan-out issued is accounted for; releasing the \
             job's lifecycle authority"
        );
        drop(admission);
        Ok(discharged)
    }

    /// Keeps the authority rather than releasing it, for an obligation nothing left can settle.
    ///
    /// An identifier nobody accounted for is one a worker may still be applying, so releasing
    /// the job's admission behind it is the one thing this whole mechanism exists to prevent. `why` names the situation for the
    /// operator, because the two that reach it — an owner going away with an obligation, and an
    /// obligation nothing is left to observe — read very differently in a log.
    ///
    /// Only reachable after [`Self::discharge`] has refused, so something here is always
    /// outstanding.
    pub(crate) fn retain_unsettled(self, why: &'static str) {
        for (worker, attempt) in self.issued.outstanding() {
            error!(
                worker_id = worker.0,
                attempt_id = attempt.attempt_id,
                generation = self.issued.generation(),
                "an issued StartExecution has no authoritative response, no acknowledged fence \
                 or revoke, and no observed generation termination"
            );
        }
        error!(
            why,
            outstanding = self.issued.outstanding_count(),
            issued = self.issued.issued_count(),
            generation = self.issued.generation(),
            "the job's lifecycle authority is retained rather than released, so that no refusal \
             can be published behind a StartExecution a worker may still apply"
        );
        let (admission, _inventory) = self.into_parts();
        // Retaining *is* this: the authority's destructor never runs, so the job's admission
        // is never handed back. Releasing it is what would be unsafe, and there is nobody left
        // to hand it to.
        std::mem::forget(admission);
    }

    /// The proof that every identifier is accounted for, or `None`.
    ///
    /// The fold that establishes "every". It visits the whole inventory and has no arm that
    /// skips a record: a record whose [`Accounting`] is `None` ends it, so the value it returns
    /// cannot describe an obligation with an outstanding identifier. This is why
    /// [`Self::discharge`] asks no question about counts.
    ///
    /// An inventory of no identifiers folds to an empty proof and discharges, which is not a
    /// special case: a fan-out that issued nothing owes nothing.
    fn fully_accounted(&self) -> Option<Discharged> {
        let mut accounted = Vec::with_capacity(self.issued.issued_count());
        for (worker, record) in self.issued.records() {
            accounted.push((worker, record.attempt_id.clone(), record.accounted?));
        }
        Some(Discharged { accounted })
    }
}
