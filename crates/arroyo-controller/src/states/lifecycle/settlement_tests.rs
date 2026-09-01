//! The cancellation-resistant per-job settlement owner, and what accounts for an issued
//! identifier (M11.T26e, design M11.D39b, M11.D39e(v)).
//!
//! These rows are about the *mechanism*: what an observation has to agree with before it settles
//! anything, what discharge is gated on, and what this owner does on the two paths it can lose an
//! obligation by. The rows that drive the same owner through the fan-out's own seams — the phase
//! returning, and the phase's future being dropped — are in `states/mod.rs` and
//! `states/scheduling/fanout_tests.rs`, because that is where the fan-out and the phase graph
//! are.

use std::sync::Arc;

use arroyo_types::WorkerId;

use super::handshake::FenceAcknowledgement;
use super::recovery::ObservedTermination;
use super::settlement::{JobSettlementOwner, Progress};
use crate::states::scheduling::fanout::settlement::SettlementOutcome;
use crate::states::scheduling::fanout::settlement::observed::Disagreement;
use crate::states::scheduling::fanout::{
    Accounted, Accounting, IssuedAttempts, Observed, SettlementBundle, SettlementOwner, hand_over,
};
use crate::states::{Admission, AdmissionLock};

/// The worker generation these rows' obligations address.
///
/// Non-zero, because zero is the wire's sentinel for "addresses no generation" and an inventory
/// carrying it would be claiming that rather than naming a generation.
const GENERATION: u64 = 7;

/// The lifecycle fence these rows' obligations issued their identifiers under.
///
/// Non-zero for the same reason [`GENERATION`] is: zero is the wire's sentinel for "carries no
/// fence", and an inventory holding it would be describing a pre-flag-day attempt rather than a
/// fenced one.
const FENCE: u64 = 11;

/// An acknowledgement of a fence that **supersedes** [`FENCE`], and therefore revokes what these
/// obligations issued.
///
/// The height is what makes it settlement (M11.T26f): a worker revokes what is below the fence
/// it takes, so an acknowledgement of [`FENCE`] itself would leave every identifier here exactly
/// as applicable as it was. `an_acknowledgement_that_does_not_supersede_the_issuing_fence_\
/// accounts_for_nothing` is that negative.
fn ack(worker: u64, generation: u64) -> FenceAcknowledgement {
    FenceAcknowledgement::reported(WorkerId(worker), generation, FENCE + 1)
}

/// An observed termination of one worker generation.
fn gone(worker: u64, generation: u64) -> ObservedTermination {
    ObservedTermination::observed(WorkerId(worker), generation)
}

/// An [`Admission`] taken from an [`AdmissionLock`] the row also holds, so it can ask
/// afterwards whether the job's lifecycle authority is free.
async fn admitted() -> (AdmissionLock, Admission) {
    let lock = AdmissionLock::default();
    let admission = lock.admit().await;
    (lock, admission)
}

/// The obligation of a fan-out that issued one request per `(worker, attempt_id)` pair.
fn obligation(admission: Admission, issued: &[(u64, &str)]) -> SettlementBundle {
    let mut inventory = IssuedAttempts::issued_under(GENERATION, FENCE);
    for (worker, attempt_id) in issued {
        inventory.issued(WorkerId(*worker), (*attempt_id).to_string());
    }
    SettlementBundle::new(admission, inventory)
}

/// An owner holding the obligation of a fan-out that issued `issued`, and the lock its
/// admission came from.
async fn owning(issued: &[(u64, &str)]) -> (AdmissionLock, Arc<JobSettlementOwner>) {
    let (authority, admission) = admitted().await;
    let owner = JobSettlementOwner::for_job(Arc::new("job_abc".to_string()));
    let SettlementOutcome::Transferred(receipt) =
        hand_over(obligation(admission, issued), Some(owner.as_ref()))
    else {
        panic!("this owner takes an obligation offered to it");
    };
    assert_eq!(
        receipt.outstanding(),
        issued.len(),
        "the receipt says how many identifiers the owner became answerable for"
    );
    (authority, owner)
}

// ---------------------------------------------------------------------------------------------
// M11.D39e(v): what accounts for an identifier, and what does not
// ---------------------------------------------------------------------------------------------

/// Each of the three facts M11.D39e(v) allows accounts for the identifier it names, and the last
/// one discharges the obligation.
///
/// Quantified over the whole taxonomy rather than sampled, because the claim is that these are
/// *the* three: a fourth way to settle an identifier would have to be added to
/// [`Accounting`] and would fail this row's exhaustive expectation rather than slipping past a
/// test that happened to check two of them.
#[tokio::test]
async fn each_of_the_three_observations_accounts_for_the_identifier_it_names() {
    for (observed, expected) in [
        (
            Observed::authoritative_response(WorkerId(4), GENERATION, "id-4") as Observed,
            Accounting::AuthoritativeResponse,
        ),
        (
            Observed::acknowledged_fence(&ack(4, GENERATION)),
            Accounting::AcknowledgedFence,
        ),
        (
            Observed::terminated_generation(&gone(4, GENERATION)),
            Accounting::TerminatedGeneration,
        ),
    ] {
        let (authority, owner) = owning(&[(4, "id-4")]).await;
        assert!(
            !authority.is_free(),
            "{expected:?}: the owner is holding the job's publication lock before the \
             observation, which is what makes the release below the observation's doing"
        );

        let Progress::Discharged(discharged) = owner.observe(&observed) else {
            panic!(
                "{expected:?}: this is the only identifier owed, so accounting for it \
                    discharges the obligation"
            );
        };
        assert_eq!(
            discharged.accounted(),
            [(WorkerId(4), "id-4".to_string(), expected)],
            "{expected:?}: the proof names every identifier and what accounted for it"
        );
        assert_eq!(
            owner.outstanding(),
            None,
            "{expected:?}: and the owner is answerable for nothing afterwards"
        );
        assert!(
            authority.is_free(),
            "{expected:?}: the authority is released, because every identifier this obligation \
             issued is accounted for"
        );
    }
}

/// Mutating the agreement between the identifier, the generation it addressed and the outcome
/// observed for it accounts for nothing — and the authority stays where it is.
///
/// The three are one fact (M11.D39e(v)). Arriving separately they are three untrusted inputs, and
/// the failure they make possible is silent: an answer about another generation's attempt, or
/// about another request of this one, would account for an identifier a worker may still be
/// applying and the authority standing behind it would be released.
///
/// Each dimension is varied independently and against each of the observations it can be varied
/// for, so a check that covered only the response — the one fact that names an identifier — would
/// fail here rather than pass.
#[tokio::test]
async fn mutating_the_agreement_between_identifier_generation_and_outcome_accounts_for_nothing() {
    let cases: Vec<(&str, Observed, Disagreement)> = vec![
        (
            "a response from another generation",
            Observed::authoritative_response(WorkerId(4), GENERATION + 1, "id-4"),
            Disagreement::Generation {
                observed: GENERATION + 1,
                addressed: GENERATION,
            },
        ),
        (
            "an acknowledged fence from another generation",
            Observed::acknowledged_fence(&ack(4, GENERATION - 1)),
            Disagreement::Generation {
                observed: GENERATION - 1,
                addressed: GENERATION,
            },
        ),
        (
            "a termination of another generation",
            Observed::terminated_generation(&gone(4, GENERATION + 2)),
            Disagreement::Generation {
                observed: GENERATION + 2,
                addressed: GENERATION,
            },
        ),
        (
            "a response from a worker this fan-out issued nothing to",
            Observed::authoritative_response(WorkerId(5), GENERATION, "id-4"),
            Disagreement::NotIssuedTo { worker: 5 },
        ),
        (
            "an acknowledged fence from a worker this fan-out issued nothing to",
            Observed::acknowledged_fence(&ack(5, GENERATION)),
            Disagreement::NotIssuedTo { worker: 5 },
        ),
        (
            "a termination of a worker this fan-out issued nothing to",
            Observed::terminated_generation(&gone(5, GENERATION)),
            Disagreement::NotIssuedTo { worker: 5 },
        ),
        (
            "a response naming another request of this worker",
            Observed::authoritative_response(WorkerId(4), GENERATION, "id-4-previous"),
            Disagreement::Identifier {
                worker: 4,
                observed: "id-4-previous".to_string(),
                issued: "id-4".to_string(),
            },
        ),
        // The fourth dimension, and the one M11.T26e could not express (M11.T26f). The right
        // worker, in the right generation, acknowledging the very fence this obligation's
        // identifiers were issued under: a worker revokes what is *below* the fence it takes,
        // so this has made nothing here inapplicable. It is also the acknowledgement that
        // arrives on the ordinary path — a fan-out's own handshake acknowledges exactly the
        // fence its starts then carry — so this arm is not a defence against a hypothetical.
        (
            "an acknowledged fence that does not supersede the one this obligation issued under",
            Observed::acknowledged_fence(&FenceAcknowledgement::reported(
                WorkerId(4),
                GENERATION,
                FENCE,
            )),
            Disagreement::Fence {
                worker: 4,
                observed: FENCE,
                issued_under: FENCE,
            },
        ),
        (
            "an acknowledged fence below the one this obligation issued under",
            Observed::acknowledged_fence(&FenceAcknowledgement::reported(
                WorkerId(4),
                GENERATION,
                FENCE - 1,
            )),
            Disagreement::Fence {
                worker: 4,
                observed: FENCE - 1,
                issued_under: FENCE,
            },
        ),
    ];

    for (what, observed, expected) in cases {
        let (authority, owner) = owning(&[(4, "id-4")]).await;
        assert!(
            matches!(owner.observe(&observed), Progress::NotThisObligation),
            "{what}: accounts for nothing"
        );
        assert_eq!(
            owner.outstanding(),
            Some(1),
            "{what}: the identifier is still outstanding"
        );
        assert!(
            !authority.is_free(),
            "{what}: and the authority behind it has not been released"
        );

        // The same disagreement, read at the seam that produced it, so the row says *which* half
        // of the identity was wrong rather than only that something was.
        let (_authority, admission) = admitted().await;
        let mut bundle = obligation(admission, &[(4, "id-4")]);
        assert_eq!(
            bundle.observe(&observed),
            Accounted::NotThisObligation(expected),
            "{what}: and it says which half of the identity disagreed"
        );
        let SettlementOutcome::SettledInPlace(admission, _) = hand_over(bundle, None) else {
            panic!("{what}: with no owner the obligation stays with the caller");
        };
        drop(admission);
    }
}

/// An obligation is discharged when every identifier it issued is accounted for, and not before.
///
/// The domain the "every" quantifies over is the whole inventory, so this uses three targets and
/// accounts for them one at a time: a fix that released on the *first* observation, on a majority,
/// or on a counter a caller supplied would pass a one-identifier row and fail here.
#[tokio::test]
async fn an_obligation_is_discharged_only_when_every_identifier_it_issued_is_accounted_for() {
    let (authority, owner) = owning(&[(1, "id-1"), (2, "id-2"), (3, "id-3")]).await;

    // A different fact for each, so the row also says that the three are interchangeable as
    // *accountings* while being distinguishable as records.
    assert!(
        matches!(
            owner.observe(&Observed::authoritative_response(
                WorkerId(1),
                GENERATION,
                "id-1"
            )),
            Progress::StillOwed { outstanding: 2 }
        ),
        "one of three accounted for"
    );
    assert!(
        !authority.is_free(),
        "and nothing is released while two identifiers are unaccounted for"
    );
    assert!(
        matches!(
            owner.observe(&Observed::acknowledged_fence(&ack(2, GENERATION))),
            Progress::StillOwed { outstanding: 1 }
        ),
        "two of three accounted for"
    );
    assert!(
        !authority.is_free(),
        "and still nothing, with one identifier left"
    );

    let Progress::Discharged(discharged) =
        owner.observe(&Observed::terminated_generation(&gone(3, GENERATION)))
    else {
        panic!("the third and last identifier discharges the obligation");
    };
    assert_eq!(
        discharged.accounted(),
        [
            (
                WorkerId(1),
                "id-1".to_string(),
                Accounting::AuthoritativeResponse
            ),
            (
                WorkerId(2),
                "id-2".to_string(),
                Accounting::AcknowledgedFence
            ),
            (
                WorkerId(3),
                "id-3".to_string(),
                Accounting::TerminatedGeneration
            ),
        ],
        "the proof carries every identifier and the fact that accounted for it, and the three \
         facts are told apart rather than collapsed into a count"
    );
    assert!(
        authority.is_free(),
        "and only now is the job's lifecycle authority released"
    );
}

/// Observing the same fact twice says what observing it once said, and the first fact stands.
///
/// The property a reconciliation that may run on every turn of a loop needs. The direction
/// matters as much as the idempotence: an acknowledged fence arriving after a worker's own answer
/// must not rewrite the record of what actually answered, because "no worker ever answered this,
/// a fence made it inapplicable" and "the worker answered it" are different facts about the same
/// job and an operator reads both.
#[tokio::test]
async fn observing_the_same_identifier_twice_is_not_two_outcomes() {
    let (authority, owner) = owning(&[(1, "id-1"), (2, "id-2")]).await;
    for _ in 0..3 {
        assert!(
            matches!(
                owner.observe(&Observed::authoritative_response(
                    WorkerId(1),
                    GENERATION,
                    "id-1"
                )),
                Progress::StillOwed { outstanding: 1 }
            ),
            "however many times it is observed, one identifier is accounted for"
        );
    }

    // A *different* fact about the same, already accounted for, identifier.
    assert!(
        matches!(
            owner.observe(&Observed::acknowledged_fence(&ack(1, GENERATION))),
            Progress::StillOwed { outstanding: 1 }
        ),
        "and it is not a second settlement: the obligation still owes the identifier nothing has \
         accounted for"
    );
    assert_eq!(
        owner
            .holding()
            .expect("the obligation is still held")
            .into_iter()
            .map(|(worker, _, accounted)| (worker, accounted))
            .collect::<Vec<_>>(),
        vec![(1, Some(Accounting::AuthoritativeResponse)), (2, None)],
        "with the fact that accounted for it first, not the last one to arrive"
    );

    assert!(
        matches!(
            owner.observe(&Observed::acknowledged_fence(&ack(2, GENERATION))),
            Progress::Discharged(_)
        ),
        "and the identifier that was actually outstanding is what discharges it"
    );
    assert!(authority.is_free());

    // An observation offered to an owner that is answerable for nothing accounts for nothing,
    // and says so rather than pretending an obligation into existence to hold it.
    assert!(
        matches!(
            owner.observe(&Observed::acknowledged_fence(&ack(1, GENERATION))),
            Progress::NothingHeld
        ),
        "an owner holding no obligation has nothing for an observation to account for"
    );
    assert_eq!(owner.outstanding(), None);
}

// ---------------------------------------------------------------------------------------------
// Taking over, declining, and the two ways an obligation can be lost
// ---------------------------------------------------------------------------------------------

/// A second obligation is declined, and comes back whole.
///
/// The one reason this owner says no, and the only correct way to say it: the obligation is
/// returned, unreleased, so whoever offered it still has all of it and settles it exactly as a
/// controller with no owner at all would. Merging the two would be the alternative, and it is
/// the defect the generation check above exists for — one attempt's identifier accounted for by
/// an answer about another's.
#[tokio::test]
async fn a_second_obligation_is_declined_and_comes_back_whole() {
    let (first_authority, owner) = owning(&[(1, "id-1")]).await;
    let (second_authority, admission) = admitted().await;

    let outcome = hand_over(
        obligation(admission, &[(2, "id-2"), (3, "id-3")]),
        Some(owner.as_ref()),
    );
    let SettlementOutcome::SettledInPlace(admission, issued) = outcome else {
        panic!("an owner that already holds an obligation declines the next one");
    };
    assert_eq!(
        issued
            .records()
            .map(|(w, r)| (w.0, r.attempt_id.clone()))
            .collect::<Vec<_>>(),
        vec![(2, "id-2".to_string()), (3, "id-3".to_string())],
        "the inventory comes back intact — every identifier, under the same identifier"
    );
    assert_eq!(
        issued.generation(),
        GENERATION,
        "and still addressed to the generation it was issued to"
    );
    assert_eq!(
        owner.outstanding(),
        Some(1),
        "the obligation the owner already had is untouched by the offer it refused"
    );
    assert!(
        !first_authority.is_free(),
        "and it is still holding that one's authority"
    );
    assert!(
        !second_authority.is_free(),
        "while the declined obligation's authority was not released either: it came back inside \
         the bundle, which is what a decline is"
    );

    // And the caller settles it in place, exactly as it does with no owner at all.
    drop(admission);
    assert!(
        second_authority.is_free(),
        "released by the caller that still had it, at the moment it chose"
    );
}

/// An owner that drops what it was handed is issued no receipt, and the loss is reported as one.
///
/// The half of the guarantee `Drop` carries, stated here because it is about an owner this module
/// did not write: `SettlementBundle`'s destructor raises a flag `transfer_to` is holding across
/// the call, so an implementation that released the job's publication lock inside `take_over`
/// cannot be answered with a receipt. The half the *compiler* carries — that this owner has no
/// such path — is pinned by
/// [`the_only_ok_this_owner_can_answer_is_the_one_the_store_produces`].
#[tokio::test]
async fn an_owner_that_drops_the_obligation_is_reported_as_a_loss_and_issued_no_receipt() {
    struct Abandoning;
    impl SettlementOwner for Abandoning {
        fn take_over(&self, bundle: SettlementBundle) -> Result<(), SettlementBundle> {
            drop(bundle);
            Ok(())
        }
    }

    let (authority, admission) = admitted().await;
    let outcome = hand_over(
        obligation(admission, &[(1, "id-1"), (2, "id-2")]),
        Some(&Abandoning),
    );
    let SettlementOutcome::Abandoned { outstanding } = outcome else {
        panic!("an owner that dropped the obligation did not take it over");
    };
    assert_eq!(
        outstanding, 2,
        "and what is reported is what the inventory said when it was offered, which is the last \
         thing anybody knew about it"
    );
    assert!(
        authority.is_free(),
        "the authority went with the bundle it was inside — which is the loss, and why it is \
         reported as one rather than as a transfer"
    );
}

/// The `Ok` this owner answers with is the one its store produced (M11.T26e).
///
/// **A structural source pin, and the name says so.** The behavioural rows above can show that
/// *this* implementation keeps what it is handed; what no behavioural row can show is that no
/// future edit of it introduces a path that drops the obligation and answers `Ok(())` anyway —
/// because `()` is constructible anywhere, and `Drop` is skippable.
///
/// So the shape is pinned. `take_over` is a delegation and nothing else; `keep` answers with a
/// `Kept`; and `Kept` has one constructor, which takes the bundle **by value** and performs the
/// store. There is one bundle in that function and it is moved either into the `Err` that gives
/// it back or into the slot — so a path that dropped it would have nothing left to build the
/// `Ok` out of, and would not compile.
///
/// The intended reading of a failure here is "say how the new shape makes abandonment
/// impossible", not "the test is stale".
#[test]
fn the_only_ok_this_owner_can_answer_is_the_one_the_store_produces() {
    let source = include_str!("settlement.rs");
    let production = match source.find("\n#[cfg(test)]") {
        Some(at) => &source[..at],
        None => source,
    };

    assert_eq!(
        production
            .matches("        self.keep(bundle).map(Kept::into_acceptance)")
            .count(),
        1,
        "`take_over` is a delegation to `keep` and nothing else: every `Ok` it answers is a \
         `Kept`, and a `Kept` is a store that happened"
    );
    assert_eq!(
        production
            .matches("fn keep(&self, bundle: SettlementBundle) -> Result<Kept, SettlementBundle>")
            .count(),
        1,
        "and `keep` answers with the proof rather than with a bare unit"
    );
    assert_eq!(
        production
            .matches(
                "fn store(slot: &mut Option<SettlementBundle>, bundle: SettlementBundle) -> Self"
            )
            .count(),
        1,
        "`Kept::store` is the only constructor and it takes the obligation by value: there is no \
         way to build the proof without having moved the bundle into the slot"
    );
    assert_eq!(
        production.matches("struct Kept(());").count(),
        1,
        "and its field is private to this module, so nothing outside can mint one"
    );

    // The two expressions that put a bundle in the slot, counted. This is what makes the
    // destructor's `Ok` arm unreachable rather than merely unobserved: `keep` stores and then
    // settles under the same guard, and `settle` only puts back a bundle `discharge` refused, so
    // a bundle in the slot always has an identifier outstanding. A third writer added later
    // makes this count wrong, which is the point.
    assert_eq!(
        production
            .matches("let kept = Kept::store(&mut held, bundle);\n        // An obligation")
            .count(),
        1,
        "`keep` stores the obligation exactly once"
    );
    assert_eq!(
        production
            .matches("        let _settled = self.settle(&mut held);\n        Ok(kept)")
            .count(),
        1,
        "and settles it immediately afterwards, under the same guard, so an obligation that owes \
         nothing on arrival is never left in the slot"
    );
    assert_eq!(
        production.matches("*held = Some(bundle);").count(),
        1,
        "and the only other write into the slot is `settle` putting back a bundle `discharge` \
         refused, which by construction still owes an identifier"
    );
    assert_eq!(
        production.matches("Kept::store(").count(),
        1,
        "there is exactly one store, so `Ok` has exactly one origin"
    );
    for forbidden in ["drop(bundle)", "std::mem::forget(bundle)"] {
        assert_eq!(
            production.matches(forbidden).count(),
            0,
            "`{forbidden}` in this module would be an obligation released inside `take_over`, \
             which is the abandonment the seam reports and this owner must not be able to reach"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// The drop paths
// ---------------------------------------------------------------------------------------------

/// Dropping the owner while an identifier is unaccounted for **retains** the authority.
///
/// A destructor is a failure path, and this one is asked to decide something important, so it is
/// tested. What it must not do is the tidy thing: releasing the job's publication lock here would
/// make a refusal publishable behind a `StartExecution` a worker may still be applying, which is
/// the whole of what the mechanism prevents. It retains instead — the disposal
/// `retain_without_a_phase` performs, for the same reason and at the same cost — and says so
/// loudly.
#[tokio::test]
async fn dropping_the_owner_with_an_identifier_unaccounted_for_retains_the_authority() {
    let (authority, owner) = owning(&[(1, "id-1"), (2, "id-2")]).await;
    assert!(
        matches!(
            owner.observe(&Observed::authoritative_response(
                WorkerId(1),
                GENERATION,
                "id-1"
            )),
            Progress::StillOwed { outstanding: 1 }
        ),
        "one of the two answered, so the obligation is partly accounted for and not discharged"
    );

    drop(owner);
    assert!(
        !authority.is_free(),
        "the authority is retained rather than released: nobody accounted for `id-2`, and \
         releasing here is exactly the publication the mechanism exists to prevent"
    );
}

/// Dropping the owner once everything is accounted for releases the authority.
///
/// The control, and what makes the row above about the *obligation* rather than about dropping.
/// A destructor that simply never released would pass that row and fail this one — and would
/// leak the job's publication lock on every job that ever interrupted a fan-out.
#[tokio::test]
async fn dropping_the_owner_after_everything_is_accounted_for_releases_the_authority() {
    let (authority, admission) = admitted().await;
    let owner = JobSettlementOwner::for_job(Arc::new("job_abc".to_string()));

    // Handed over with one identifier outstanding, which is then accounted for by an observation
    // that arrives while nothing is looking at the result.
    let mut inventory = IssuedAttempts::issued_under(GENERATION, FENCE);
    inventory.issued(WorkerId(1), "id-1".to_string());
    let mut bundle = SettlementBundle::new(admission, inventory);
    assert_eq!(
        bundle.observe(&Observed::acknowledged_fence(&ack(1, GENERATION))),
        Accounted::Settled(Accounting::AcknowledgedFence),
        "the identifier is accounted for before the obligation is offered"
    );
    let SettlementOutcome::Transferred(_) = hand_over(bundle, Some(owner.as_ref())) else {
        panic!("this owner takes an obligation offered to it");
    };
    assert_eq!(
        owner.outstanding(),
        None,
        "an obligation that owes nothing when it arrives is discharged on the spot rather than \
         held: keeping the lock for it would stop the job's next attempt taking one"
    );
    assert!(
        authority.is_free(),
        "so the authority is already released before the owner is dropped"
    );
    drop(owner);
    assert!(
        authority.is_free(),
        "and dropping an owner that holds nothing changes nothing"
    );
}

// ---------------------------------------------------------------------------------------------
// The mechanism seam
// ---------------------------------------------------------------------------------------------

/// Exactly one production implementation of the transfer interface exists, and it is this one.
///
/// **A structural source pin, and the name says so.** M11.T25's version of this claim was that
/// nothing implemented `SettlementOwner` at all, pinned over the phase graph's file list; the
/// half of it that still holds — that the phase graph implements none — is
/// `crate::states::scheduling::phase_tests::the_phase_graph_implements_no_settlement_owner`, and
/// this is the other half, walked over the whole crate so that an implementation added anywhere
/// is found wherever it is put.
///
/// One is the number that matters. Two owners for one job is not a redundancy: an interrupted
/// fan-out offers its obligation once, and the party that answers has to be the one a later
/// observation reaches. The phase-side seam and the fencing reconciliation read the same field of
/// the same `JobContext` for exactly that reason, and a second implementation would be a second
/// thing they could disagree about.
#[test]
fn exactly_one_production_settlement_owner_exists() {
    let implemented_in: Vec<String> =
        super::fence_tests::production_call_sites("impl SettlementOwner for")
            .into_iter()
            // The compile-fail fixtures are not compiled into this crate at all —
            // `scheduling::compile_fail` `include_str!`s them into standalone `rustc`
            // invocations — and the owners in them are the deliberately-wrong ones the negative
            // rows exist to reject.
            .filter(|path| !path.starts_with("src/states/scheduling/compile_fail/"))
            .collect();
    assert_eq!(
        implemented_in,
        vec!["src/states/lifecycle/settlement.rs"],
        "the cancellation-resistant per-job owner is implemented once, outside the phase graph \
         that is dropped with the job's state task"
    );
}

/// The fenced mechanism supplies a single writer and a settlement owner together, or neither.
///
/// Quantified over every mode rather than over the one production selects, because the claim is
/// about the enum: a job whose transitions the D39a writer decides and whose interrupted fan-outs
/// have nobody to hand their obligation to would be half the mechanism, and so would the reverse.
/// `JobLifecycle::FencedV2` carries both as fields of one arm, which is what makes this an
/// assertion about a shape rather than about two call sites that happen to agree today.
#[test]
fn the_fenced_mechanism_supplies_a_writer_and_a_settlement_owner_together() {
    for mode in super::LifecycleMode::ALL {
        let job_id = Arc::new("job_abc".to_string());
        let lifecycle = super::JobLifecycle::for_mode(mode, Arc::clone(&job_id));
        let has_writer = lifecycle
            .actor(
                job_id,
                arroyo_rpc::state_backend::StateBackendSelector::Parquet,
            )
            .is_some();
        assert_eq!(
            has_writer,
            lifecycle.settlement().is_some(),
            "{mode:?}: a job has the D39a single writer and the M11.T26e settlement owner \
             together, or it has neither"
        );
        assert_eq!(
            has_writer,
            mode == super::LifecycleMode::FencedV2,
            "{mode:?}: and it has them exactly under the fenced mechanism"
        );
    }
}

/// The selected mechanism offers an interrupted fan-out's obligation to this owner, and the
/// ownerless disposal M11.T08 used is gone.
///
/// The rewrite of `the_selected_mechanism_offers_no_settlement_owner_and_keeps_the_landed_rescue`,
/// which asserted the opposite while M11.T26e was default-inactive: `LifecycleMode::SELECTED` was
/// `LegacyT08`, a job built from it had no settlement owner, and an interrupted fan-out settled
/// in place under the landed `settle_under_admission` rescue. M11.T26h's activation change made
/// the selection `FencedV2` and removed that rescue, so both halves of this row moved together
/// and the requirement — "what the *selected* mechanism does with an interrupted fan-out's
/// obligation is one derivation, not a literal" — is carried by a row of the same shape.
///
/// A derivation rather than an assertion about a literal: it asks the job's own mechanism, so it
/// is the activation that decides this row's answer.
#[tokio::test]
async fn the_selected_mechanism_transfers_an_interrupted_obligation_to_its_settlement_owner() {
    let job_id = Arc::new("job_abc".to_string());
    let lifecycle = super::JobLifecycle::for_mode(super::LifecycleMode::SELECTED, job_id);
    let owner = lifecycle.settlement();
    assert!(
        owner.is_some(),
        "the selected mechanism is `FencedV2`, and a job built from it has a settlement owner"
    );

    let (authority, admission) = admitted().await;
    let outcome = hand_over(
        obligation(admission, &[(1, "id-1")]),
        owner.as_deref().map(|o| o as &dyn SettlementOwner),
    );
    let SettlementOutcome::Transferred(receipt) = outcome else {
        panic!("with an owner, an interrupted fan-out transfers rather than settling in place");
    };
    assert_eq!(
        receipt.outstanding(),
        1,
        "and the owner is answerable for exactly what the interrupted fan-out issued"
    );
    assert!(
        !authority.is_free(),
        "the job's lifecycle authority went with it, so nothing can be published behind the \
         attempt the owner is now holding"
    );

    // And it comes back only when the identifier is accounted for — never by dropping the
    // party that took it, which is what the removed ownerless disposal did.
    let owner = owner.expect("checked above");
    let accounted = owner.observe(&Observed::authoritative_response(
        WorkerId(1),
        GENERATION,
        "id-1",
    ));
    assert!(matches!(accounted, Progress::Discharged(_)));
    assert!(
        authority.is_free(),
        "and once every identifier is accounted for the authority is released"
    );
}

/// Each of the three facts names itself, carries the generation it is about, and no two share a
/// name.
///
/// The name is what a discharge log line says accounted for each identifier, and the generation
/// is what an observation is measured against before anything is accounted at all. Two facts
/// sharing a name would make "the worker answered" and "the worker is gone" the same entry in
/// the record of *why* a job's lifecycle authority was released — and those are the difference
/// between a settled attempt and an abandoned one.
#[test]
fn each_accounting_fact_names_itself_and_carries_the_generation_it_is_about() {
    let facts = [
        (
            Observed::authoritative_response(WorkerId(1), GENERATION, "id-1"),
            Accounting::AuthoritativeResponse,
            "an authoritative response",
        ),
        (
            Observed::acknowledged_fence(&ack(1, GENERATION)),
            Accounting::AcknowledgedFence,
            "an acknowledged fence and its revokes",
        ),
        (
            Observed::terminated_generation(&gone(1, GENERATION)),
            Accounting::TerminatedGeneration,
            "an observed generation termination",
        ),
    ];
    for (observed, accounting, name) in &facts {
        assert_eq!(observed.accounting(), *accounting);
        assert_eq!(observed.generation(), GENERATION);
        assert_eq!(accounting.as_str(), *name);
    }
    let mut named: Vec<&str> = facts.iter().map(|(_, _, name)| *name).collect();
    named.sort_unstable();
    named.dedup();
    assert_eq!(named.len(), 3, "no two facts share a name");
}
