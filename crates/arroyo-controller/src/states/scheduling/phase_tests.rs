//! Focused tests for the M11.D39b phase graph's own machinery (M11.T25b).
//!
//! The rows that need a whole job — a database, workers, a gRPC server — live beside the
//! scheduling integration rows in `states/mod.rs`, because that is where the harness that owns
//! those things is. What is here is everything that can be decided from the types alone: the
//! issued-attempt inventory, the fence target set, the transfer interface, and two source pins
//! over what `Fencing` and the transfer interface are *allowed* to be.

use std::sync::Mutex;

use arroyo_types::WorkerId;

use super::fanout::{
    IssuedAttempts, SettlementBundle, SettlementOutcome, SettlementOwner, hand_over,
};
use super::fencing::FenceTargets;
use crate::states::{Admission, RefusalGate};

/// An [`Admission`] taken from a gate the test also holds, so it can ask afterwards whether the
/// job's publication lock is free.
async fn admitted() -> (RefusalGate, Admission) {
    let mut gate = RefusalGate::default();
    let (admission, refusal) = gate.admit_scheduling().await;
    assert!(refusal.is_none(), "a fresh gate has refused nothing");
    (gate, admission)
}

/// The inventory accounts for every target and settles idempotently.
///
/// Idempotence is the property M11.D39b needs and the reason the inventory is explicit rather
/// than derived: a reconciliation that may run on every turn of a loop has to be able to be run
/// twice without meaning something different the second time.
#[test]
fn issued_attempts_account_for_every_target_and_settle_idempotently() {
    let mut issued = IssuedAttempts::default();
    issued.issued(WorkerId(1), "aa".to_string());
    issued.issued(WorkerId(2), "bb".to_string());
    assert_eq!(issued.issued_count(), 2);
    assert_eq!(issued.outstanding_count(), 2);

    issued.settled(WorkerId(1));
    assert_eq!(issued.outstanding_count(), 1);
    issued.settled(WorkerId(1));
    assert_eq!(
        issued.outstanding_count(),
        1,
        "settling an attempt twice is not two outcomes"
    );

    issued.settled(WorkerId(99));
    assert_eq!(
        issued.issued_count(),
        2,
        "and an outcome for an attempt that was never issued invents no attempt to hold it"
    );

    let outstanding: Vec<WorkerId> = issued.outstanding().map(|(id, _)| id).collect();
    assert_eq!(
        outstanding,
        vec![WorkerId(2)],
        "what is left is exactly what has not been accounted for"
    );
}

/// A fence target answers once, whichever way it answers.
#[test]
fn fence_targets_reconcile_idempotently() {
    let mut targets = FenceTargets::for_workers([WorkerId(1), WorkerId(2), WorkerId(3)]);
    assert_eq!(targets.pending(), 3);

    assert!(targets.acknowledge(WorkerId(1)));
    assert_eq!(targets.pending(), 2);
    assert!(
        !targets.acknowledge(WorkerId(1)),
        "a duplicate acknowledgement from the same generation is not a second event"
    );
    assert_eq!(targets.pending(), 2);

    assert!(targets.terminate(WorkerId(2)));
    assert_eq!(targets.pending(), 1);
    assert!(
        !targets.terminate(WorkerId(2)),
        "and neither is a termination observed twice"
    );

    assert!(
        targets.terminate(WorkerId(1)),
        "a generation that acknowledged and then went away is a new observation about it"
    );
    assert_eq!(
        targets.pending(),
        1,
        "but it does not make an already-answered target pending again"
    );
}

/// An owner that keeps whatever it is handed, so a test can ask what moved.
#[derive(Default)]
struct RecordingOwner {
    /// The authority, held exactly as a real settlement owner would hold it.
    held: Mutex<Option<Admission>>,
    /// What the inventory said when it arrived.
    outstanding: Mutex<Option<usize>>,
}

impl SettlementOwner for RecordingOwner {
    fn take_over(&self, bundle: SettlementBundle) {
        // Taken apart rather than dropped: an owner that merely dropped what it was handed
        // would release the job's publication lock without settling anything, which is what
        // `SettlementBundle`'s own `Drop` exists to report.
        let (admission, issued) = bundle.into_parts();
        *self.outstanding.lock().unwrap() = Some(issued.outstanding_count());
        *self.held.lock().unwrap() = Some(admission);
    }
}

/// The transfer interface moves the issued-attempt inventory and the lifecycle authority as one
/// unit.
///
/// The authority half is asserted the only way it can be: by asking the gate whether a
/// publication is possible. While the owner holds the admission it is not, which is what "the
/// obligation moved" has to mean — an owner that had received the inventory without the
/// authority would leave a refusal publishable behind attempts it had just taken charge of.
#[tokio::test]
async fn an_interrupted_fan_out_hands_over_inventory_and_authority_together() {
    let (gate, admission) = admitted().await;
    let mut issued = IssuedAttempts::default();
    issued.issued(WorkerId(4), "cc".to_string());
    issued.issued(WorkerId(5), "dd".to_string());
    issued.settled(WorkerId(4));

    let owner = RecordingOwner::default();
    let outcome = hand_over(SettlementBundle::new(admission, issued), Some(&owner));

    let SettlementOutcome::Transferred(receipt) = outcome else {
        panic!("an owner that exists takes the obligation");
    };
    assert_eq!(
        receipt.outstanding(),
        1,
        "the receipt says how much of the obligation the new owner became responsible for"
    );
    assert_eq!(*owner.outstanding.lock().unwrap(), Some(1));
    assert!(
        gate.admit_publication().is_none(),
        "and the authority went with it: nothing can be published while the owner holds the \
         admission it was handed"
    );

    drop(owner.held.lock().unwrap().take());
    assert!(
        gate.admit_publication().is_some(),
        "the control — the gate is only closed because the owner was holding it"
    );
}

/// M11.T25 has no settlement owner, so an interrupted fan-out keeps its own obligation.
///
/// This is the whole of "T25 cannot release or activate through the transfer interface": with no
/// owner there is nothing to transfer to, the admission comes back to the phase, and the landed
/// `settle_under_admission` rescue is what settles the attempts — unchanged, and still selected
/// in production.
#[tokio::test]
async fn without_a_settlement_owner_an_interrupted_fan_out_keeps_its_obligation() {
    let (gate, admission) = admitted().await;
    let mut issued = IssuedAttempts::default();
    issued.issued(WorkerId(6), "ee".to_string());

    let outcome = hand_over(SettlementBundle::new(admission, issued), None);
    let SettlementOutcome::SettledInPlace(admission, issued) = outcome else {
        panic!("with no owner the obligation cannot have been transferred");
    };
    assert_eq!(issued.outstanding_count(), 1);
    assert!(
        gate.admit_publication().is_none(),
        "and the phase still holds the authority, so nothing is publishable behind the attempts \
         it is still waiting on"
    );
    drop(admission);
    assert!(gate.admit_publication().is_some());
}

/// The production halves of the phase-graph modules, and of the two files they were cut out of.
///
/// The scheduling modules have no test module of their own — their tests are these — so their
/// whole source is production; `scheduling.rs` and `mod.rs` are cut at their first `#[cfg(test)]`
/// exactly as the landed source pins cut them.
fn phase_graph_production_sources() -> Vec<(&'static str, &'static str)> {
    fn production_half(source: &'static str) -> &'static str {
        match source.find("\n#[cfg(test)]") {
            Some(at) => &source[..at],
            None => source,
        }
    }
    vec![
        (
            "scheduling/phases.rs",
            production_half(include_str!("phases.rs")),
        ),
        (
            "scheduling/admission.rs",
            production_half(include_str!("admission.rs")),
        ),
        (
            "scheduling/admission/execution.rs",
            production_half(include_str!("admission/execution.rs")),
        ),
        (
            "scheduling/admission/observation.rs",
            production_half(include_str!("admission/observation.rs")),
        ),
        (
            "scheduling/fanout.rs",
            production_half(include_str!("fanout.rs")),
        ),
        (
            "scheduling/fanout/settlement.rs",
            production_half(include_str!("fanout/settlement.rs")),
        ),
        (
            "scheduling/fencing.rs",
            production_half(include_str!("fencing.rs")),
        ),
        (
            "scheduling.rs",
            production_half(include_str!("../scheduling.rs")),
        ),
        ("states/mod.rs", production_half(include_str!("../mod.rs"))),
    ]
}

/// Nothing in M11.T25 implements the transfer interface (M11.R59b).
///
/// **A structural source pin, and the name says so.** The claim it protects is not that
/// transferring is discouraged but that this half *cannot* transfer: `SettlementOwner` has no
/// implementation outside a test, so `PhaseContext::settlement_owner` has nothing to answer with
/// and the only outcome an interrupted fan-out has is settling in place under the landed rescue.
///
/// The intended reading of a failure here is "M11.T26 has arrived", not "the test is stale" — and
/// T26's owner must come with the durable fence that lets a controller restart recover the same
/// obligation, because an owner without one is a way to lose it.
#[test]
fn no_production_code_implements_the_settlement_owner() {
    for (file, source) in phase_graph_production_sources() {
        assert_eq!(
            source.matches("impl SettlementOwner").count()
                + source.matches("SettlementOwner for").count(),
            0,
            "{file}: M11.T25 defines the transfer interface and implements it nowhere. An \
             implementation here would let an interrupted phase release its lifecycle authority \
             to something that cannot be recovered after a controller restart"
        );
    }
}

/// `Fencing` is token-free and has no irreversible effect (M11.D39b, DoD M11.T25h).
///
/// **A structural source pin, and the name says so.** The compile-fail fixtures next door prove
/// what the *phase* types do and do not expose; what no fixture can notice is a method added to
/// `Fencing` later, because `Fencing` is where an interruption ends up and therefore the one
/// place where "it is safe, we are already fencing" is a tempting thing to write.
///
/// So the inventory is pinned: the type holds no admission, takes none, and exposes exactly the
/// three kinds of operation M11.D39b allows it — idempotent fence/revoke reconciliation,
/// generation teardown/termination observation, and intent coalescing.
#[test]
fn the_source_of_fencing_exposes_no_admission_and_no_irreversible_effect() {
    let source = include_str!("fencing.rs");
    let struct_at = source
        .find("pub(crate) struct Fencing<'a, 'ctx> {")
        .expect("the fencing state has been renamed");
    let fields = &source[struct_at
        ..struct_at
            + source[struct_at..]
                .find("\n}")
                .expect("unterminated struct")];
    assert_eq!(
        fields.matches("Admission").count(),
        0,
        "`Fencing` holds no admission. It is what an interrupted phase releases *into*, so a \
         token here would mean the release had not happened"
    );

    let impl_at = source
        .find("impl<'a, 'ctx> Fencing<'a, 'ctx> {")
        .expect("the fencing impl has been renamed");
    let body = &source[impl_at
        ..source[impl_at..]
            .find("\n}\n")
            .map(|at| impl_at + at)
            .expect("unterminated impl")];
    let mut methods: Vec<&str> = body
        .match_indices("pub(crate) fn ")
        .chain(body.match_indices("pub(crate) async fn "))
        .map(|(i, m)| {
            let rest = &body[i + m.len()..];
            &rest[..rest.find('(').expect("a method has arguments")]
        })
        .collect();
    methods.sort_unstable();
    assert_eq!(
        methods,
        [
            "coalesce_intent",
            "new",
            "note_transferred",
            "observe_fence_acknowledged",
            "observe_generation_terminated",
            "outstanding",
            "reconcile",
            "targets",
        ],
        "`Fencing` may reconcile fences and revokes, observe a generation's teardown or \
         termination, and coalesce the job's intents. Adding a start, generation, recovery or \
         commit effect to it — or a publication of `Refused`, which needs the durable fence \
         M11.T26 owns — is the change this pin exists to force a decision about"
    );
    assert_eq!(
        body.matches("&Admission").count() + body.matches(": Admission").count(),
        0,
        "and no method of it takes one either, so a caller cannot lend it the authority it does \
         not hold"
    );
}
