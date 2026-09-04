//! M11.D39g's declared fault model, as named reusable injections (M11.T26g).
//!
//! # What this module is
//!
//! M11.D39g declares the faults the fenced lifecycle is answerable for and requires that *"each
//! has a named fault-injection test in both controller modes"*. This module is the controller's
//! half of that: [`Fault`] is the declared model as an enumeration, [`Fault::injection`] names
//! the operation that injects each one, and [`Fault::injected_in`] names the file that operation
//! lives in — because three of the faults are answered on the *worker* side and two of them are
//! only observable end to end, against a real scheduling row.
//!
//! The table is not documentation. `every_declared_fault_has_a_live_injection` reads it back and
//! checks each named operation still exists in the file the table names, so an injection renamed
//! or deleted fails the build rather than leaving a fault silently uncovered. Within this file
//! the same guarantee comes from the compiler: an injection nothing calls is dead code, and the
//! workspace builds under `-D warnings`.
//!
//! # The layer this harness injects at, and the two it deliberately does not
//!
//! A fault is a statement about **what the controller gets to observe**, and the controller's
//! whole safety rule (M11.D39, M11.T26r) is that settlement is never *inferred*: only a
//! definitive worker response, an acknowledged fence above the issuing one, or an authoritatively
//! observed generation termination accounts for an issued identifier. So the injections here
//! transform the *observation stream* an interrupted fan-out's obligation is reconciled against
//! — messages lost, duplicated, reordered, arbitrarily delayed, or arriving from a generation
//! that is not the one the obligation addressed — and the assertions are about what the job's
//! [`JobSettlementOwner`] then does and does not release.
//!
//! The two layers below it already have harnesses and are not duplicated here:
//!
//! * the **network** layer — a fence directive that is paused, refused, misreported or never
//!   answered, against real worker servers on real sockets — is `recovery_tests.rs`'s
//!   `Answers`/`Lists` pair, driving `discharge_recorded_obligation`; and
//! * the **scheduling row** layer — a whole attempt interrupted at a phase boundary, against a
//!   real database and real workers — is `states/mod.rs`'s `SchedulingRun`/`StartsExecution`
//!   pair.
//!
//! [`Fault::injected_in`] points at whichever of the three answers each fault, so the model has
//! one index rather than three partial ones.

use std::sync::Arc;

use arroyo_types::WorkerId;
use tonic::Code;

use super::handshake::FenceAcknowledgement;
use super::mode::LifecycleMode;
use super::protocol::{FenceProtocol, TransportSettlement};
use super::recovery::ObservedTermination;
use super::settlement::{JobSettlementOwner, Progress};
use crate::states::AdmissionLock;
use crate::states::scheduling::fanout::settlement::SettlementOutcome;
use crate::states::scheduling::fanout::{IssuedAttempts, Observed, SettlementBundle, hand_over};

/// One fault from M11.D39g's declared model.
///
/// The list is D39g's own, in D39g's order: *"message loss/duplication/reorder and arbitrary
/// in-transit delay; worker crash/restart and partition; controller crash/restart at any point;
/// endpoint reuse by a new worker generation; and post-flag-day version skew"*, with the
/// controller's crash split at the three points §3C's fault-model table names — preamble,
/// fan-out and commit — and the incapable/unregistered peer that table also carries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Fault {
    /// A message that was sent and never arrived.
    MessageLoss,
    /// A message that arrived more than once.
    Duplication,
    /// Messages that arrived in an order other than the one they were sent in.
    Reorder,
    /// A message held arbitrarily long in transit and delivered later.
    InTransitDelay,
    /// A worker generation that died and came back with none of its predecessor's state.
    WorkerCrashRestart,
    /// A target generation that can neither be reached nor observed to have terminated.
    Partition,
    /// A *new* worker generation answering at its predecessor's endpoint.
    EndpointReuse,
    /// The controller died before its scheduling preamble finished.
    ControllerCrashAtPreamble,
    /// The controller died with start requests issued and unsettled.
    ControllerCrashMidFanOut,
    /// The controller died with a two-phase commit outstanding.
    ControllerCrashMidCommit,
    /// A worker that never advertised the reconciliation contract.
    IncapableWorker,
    /// A worker generation that has not announced itself to any controller.
    UnregisteredWorker,
    /// A controller and a worker on opposite sides of the flag day.
    PostFlagDaySkew,
}

/// Where a fault's injection lives.
///
/// Three files, because three parties answer for these faults and a harness that pretended
/// otherwise would be mocking one of them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InjectedIn {
    /// This module: the controller's observation stream and its protocol values.
    Controller,
    /// `arroyo-worker`'s `lifecycle_fence/faults.rs`: what arrives at a worker generation.
    Worker,
    /// `lifecycle/recovery_tests.rs`: fence directives against real worker servers.
    RecoveryNetwork,
    /// `states/mod.rs`: a whole scheduling attempt against a real row and real workers.
    SchedulingRow,
}

impl InjectedIn {
    /// The source this injection has to be findable in.
    fn source(self) -> &'static str {
        match self {
            InjectedIn::Controller => include_str!("faults.rs"),
            InjectedIn::Worker => {
                include_str!("../../../../arroyo-worker/src/lifecycle_fence/faults.rs")
            }
            InjectedIn::RecoveryNetwork => include_str!("recovery_tests.rs"),
            InjectedIn::SchedulingRow => include_str!("../mod.rs"),
        }
    }

    /// The path a failure reports, so a reader is told where to look.
    fn path(self) -> &'static str {
        match self {
            InjectedIn::Controller => "crates/arroyo-controller/src/states/lifecycle/faults.rs",
            InjectedIn::Worker => "crates/arroyo-worker/src/lifecycle_fence/faults.rs",
            InjectedIn::RecoveryNetwork => {
                "crates/arroyo-controller/src/states/lifecycle/recovery_tests.rs"
            }
            InjectedIn::SchedulingRow => "crates/arroyo-controller/src/states/mod.rs",
        }
    }
}

impl Fault {
    /// Every fault M11.D39g declares.
    pub(crate) const ALL: [Fault; 13] = [
        Fault::MessageLoss,
        Fault::Duplication,
        Fault::Reorder,
        Fault::InTransitDelay,
        Fault::WorkerCrashRestart,
        Fault::Partition,
        Fault::EndpointReuse,
        Fault::ControllerCrashAtPreamble,
        Fault::ControllerCrashMidFanOut,
        Fault::ControllerCrashMidCommit,
        Fault::IncapableWorker,
        Fault::UnregisteredWorker,
        Fault::PostFlagDaySkew,
    ];

    /// The operation that injects this fault, spelled as it appears in its source.
    ///
    /// Exhaustive: a fault added to the enum without an injection does not compile.
    pub(crate) fn injection(self) -> &'static str {
        match self {
            Fault::MessageLoss => "fn lose",
            Fault::Duplication => "fn duplicate",
            Fault::Reorder => "fn deliver_held_in_reverse",
            Fault::InTransitDelay => "fn deliver_held",
            Fault::WorkerCrashRestart => "fn restart_generation",
            Fault::Partition => "NeverSettling",
            Fault::EndpointReuse => "fn endpoint_reused_by",
            Fault::ControllerCrashAtPreamble => "fn crashed_at",
            Fault::ControllerCrashMidFanOut => "fn crashed_at",
            Fault::ControllerCrashMidCommit => "fn crashed_at",
            Fault::IncapableWorker => "a_worker_predating_the_reconciliation_contract",
            Fault::UnregisteredWorker => "fn to_unregistered_generation",
            Fault::PostFlagDaySkew => "fn directive_from_a_controller_in",
        }
    }

    /// The harness the injection lives in.
    ///
    /// Exhaustive for the same reason [`Self::injection`] is.
    pub(crate) fn injected_in(self) -> InjectedIn {
        match self {
            Fault::MessageLoss
            | Fault::Duplication
            | Fault::Reorder
            | Fault::InTransitDelay
            | Fault::ControllerCrashAtPreamble
            | Fault::ControllerCrashMidFanOut
            | Fault::ControllerCrashMidCommit
            | Fault::PostFlagDaySkew => InjectedIn::Controller,
            Fault::WorkerCrashRestart | Fault::EndpointReuse | Fault::UnregisteredWorker => {
                InjectedIn::Worker
            }
            Fault::Partition => InjectedIn::RecoveryNetwork,
            Fault::IncapableWorker => InjectedIn::SchedulingRow,
        }
    }

    /// Whether the named injection is findable where the table says it is.
    ///
    /// Returns the path so a failure names it. This is the anti-drift check: a renamed operation
    /// makes the fault it injects unfindable, and the model then says it covers something it
    /// does not.
    pub(crate) fn resolves(self) -> Result<(), &'static str> {
        let site = self.injected_in();
        if site.source().contains(self.injection()) {
            Ok(())
        } else {
            Err(site.path())
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The observation stream of one interrupted fan-out
// ---------------------------------------------------------------------------------------------

/// The worker generation this harness's obligations address.
///
/// Non-zero: zero is the wire's sentinel for "addresses no generation".
pub(crate) const GENERATION: u64 = 7;

/// The lifecycle fence this harness's obligations issued their identifiers under.
pub(crate) const FENCE: u64 = 11;

/// The worker this harness's obligations issue to.
pub(crate) const WORKER: u64 = 3;

/// The identifier that worker is issued, at M11.T26d's exact bounded width.
pub(crate) const ATTEMPT: &str = "0123456789abcdef0123456789abcdef";

/// The point a controller died at, as the obligation it leaves behind.
///
/// M11.D39g requires controller crash/restart *"at any point"*, and §3C's fault-model table names
/// the three that leave different obligations. They are distinguished by exactly one thing —
/// what had been issued when the process went away — which is why this is one function and not
/// three fixtures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CrashPoint {
    /// Before the fan-out: nothing was issued, so nothing is owed.
    Preamble,
    /// Mid-fan-out: identifiers were issued to these workers and none was settled.
    FanOut,
    /// Mid-commit: the same, plus the commit the execution was publishing when it died.
    Commit,
}

/// An interrupted fan-out's obligation, held by the job's real settlement owner.
///
/// The job's [`AdmissionLock`] is kept because the obligation carries its [`Admission`]:
/// releasing the lifecycle authority is what discharge *does*, so a row that could not ask
/// whether the authority is free could not tell a discharge from a leak.
pub(crate) struct InterruptedFanOut {
    admission: AdmissionLock,
    owner: Arc<JobSettlementOwner>,
    /// Observations that have been produced and not yet applied, oldest first.
    in_transit: Vec<(&'static str, Observed)>,
    /// Observations the link swallowed, in the order they were sent.
    lost: Vec<&'static str>,
    /// What each applied observation did, in application order.
    applied: Vec<(&'static str, Progress)>,
}

impl InterruptedFanOut {
    /// **Injects [`Fault::ControllerCrashAtPreamble`], [`Fault::ControllerCrashMidFanOut`] and
    /// [`Fault::ControllerCrashMidCommit`].**
    ///
    /// The controller died at `point`, having issued `issued`. What survives it is exactly the
    /// obligation: the identifiers, the generation they addressed and the fence they were issued
    /// under. Nothing in this value is a token, a future or a deadline, which is the property
    /// M11.D39d's durable half rests on.
    pub(crate) async fn crashed_at(point: CrashPoint, issued: &[(u64, &str)]) -> Self {
        let issued: &[(u64, &str)] = match point {
            // Nothing was issued before the fan-out, whatever the caller offered: a preamble
            // that never reached `start_execution_on_workers` addressed no worker at all.
            CrashPoint::Preamble => &[],
            CrashPoint::FanOut | CrashPoint::Commit => issued,
        };

        let lock = AdmissionLock::default();
        let admission = lock.admit().await;

        let mut inventory = IssuedAttempts::issued_under(GENERATION, FENCE);
        for (worker, attempt_id) in issued {
            inventory.issued(WorkerId(*worker), (*attempt_id).to_string());
        }

        let owner = JobSettlementOwner::for_job(Arc::new("job_abc".to_string()));
        match hand_over(
            SettlementBundle::new(admission, inventory),
            Some(owner.as_ref()),
        ) {
            SettlementOutcome::Transferred(receipt) => {
                assert_eq!(
                    receipt.outstanding(),
                    issued.len(),
                    "the owner became answerable for exactly what the crashed attempt issued"
                );
            }
            _ => panic!("this owner takes an obligation offered to it rather than refusing it"),
        }

        Self {
            admission: lock,
            owner,
            in_transit: Vec::new(),
            lost: Vec::new(),
            applied: Vec::new(),
        }
    }

    /// Applies an observation immediately: the fault-free control.
    pub(crate) fn observe(&mut self, label: &'static str, observed: Observed) -> &Progress {
        let progress = self.owner.observe(&observed);
        self.applied.push((label, progress));
        &self.applied.last().expect("just pushed").1
    }

    /// **Injects [`Fault::MessageLoss`].** The observation was produced and never applied.
    ///
    /// Recorded rather than dropped, so a row can name what the link swallowed. The point of the
    /// injection is the *negative*: a settlement owner that released the job's authority for a
    /// message it never received would be inferring settlement.
    pub(crate) fn lose(&mut self, label: &'static str, observed: Observed) {
        let _ = observed;
        self.lost.push(label);
    }

    /// The observations this link swallowed, in send order.
    pub(crate) fn lost(&self) -> &[&'static str] {
        &self.lost
    }

    /// **Injects [`Fault::Duplication`].** The same observation is applied twice.
    pub(crate) fn duplicate(&mut self, label: &'static str, observed: Observed) {
        let _ = self.observe(label, observed.clone());
        let _ = self.observe(label, observed);
    }

    /// **Injects the send half of [`Fault::InTransitDelay`].** The observation is in transit.
    pub(crate) fn hold(&mut self, label: &'static str, observed: Observed) {
        self.in_transit.push((label, observed));
    }

    /// **Injects the delivery half of [`Fault::InTransitDelay`].** The held observation arrives.
    ///
    /// # Panics
    ///
    /// If nothing was sent under `label`.
    pub(crate) fn deliver_held(&mut self, label: &'static str) -> &Progress {
        let at = self
            .in_transit
            .iter()
            .position(|(held, _)| *held == label)
            .unwrap_or_else(|| panic!("nothing named {label} is in transit"));
        let (label, observed) = self.in_transit.remove(at);
        self.observe(label, observed)
    }

    /// **Injects [`Fault::Reorder`].** Everything in transit arrives newest-first.
    pub(crate) fn deliver_held_in_reverse(&mut self) {
        let mut reversed: Vec<(&'static str, Observed)> = self.in_transit.drain(..).collect();
        reversed.reverse();
        for (label, observed) in reversed {
            let _ = self.observe(label, observed);
        }
    }

    /// What each applied observation did, in application order.
    pub(crate) fn progress(&self) -> Vec<(&'static str, &Progress)> {
        self.applied
            .iter()
            .map(|(label, progress)| (*label, progress))
            .collect()
    }

    /// How many identifiers this owner is still answerable for, or `None` once discharged.
    pub(crate) fn outstanding(&self) -> Option<usize> {
        self.owner.outstanding()
    }

    /// Whether the job's lifecycle authority has been released.
    ///
    /// Asked of the job's authority rather than of the owner, because that is what a
    /// *replacement* controller's publication would have to get past: an authority nobody
    /// released is one nothing else can take.
    pub(crate) fn authority_released(&self) -> bool {
        self.admission.is_free()
    }
}

// ---------------------------------------------------------------------------------------------
// The witnesses an observation is built from
// ---------------------------------------------------------------------------------------------

/// A generation acknowledging a fence *above* the one this obligation issued under.
///
/// Settlement, under M11.D39e(v): a worker revokes what is below the fence it takes.
pub(crate) fn superseding_acknowledgement(worker: u64, generation: u64) -> Observed {
    Observed::acknowledged_fence(&FenceAcknowledgement::reported(
        WorkerId(worker),
        generation,
        FENCE + 1,
    ))
}

/// A generation acknowledging a fence that does **not** supersede the issuing one.
pub(crate) fn acknowledgement_at(worker: u64, generation: u64, height: u64) -> Observed {
    Observed::acknowledged_fence(&FenceAcknowledgement::reported(
        WorkerId(worker),
        generation,
        height,
    ))
}

/// An authoritatively observed termination of one worker generation.
pub(crate) fn observed_termination(worker: u64, generation: u64) -> Observed {
    Observed::terminated_generation(&ObservedTermination::observed(WorkerId(worker), generation))
}

/// A target generation's own definitive answer about the identifier it was issued.
pub(crate) fn authoritative_response(worker: u64, attempt_id: &str) -> Observed {
    Observed::authoritative_response(WorkerId(worker), GENERATION, attempt_id)
}

// ---------------------------------------------------------------------------------------------
// Version skew, at the protocol values it changes
// ---------------------------------------------------------------------------------------------

/// **Injects [`Fault::PostFlagDaySkew`].** The directive a controller running `mode` sends.
///
/// Skew is not a message that gets mangled: it is two peers reading the *same* message under
/// different rules. The controller's half of it is which directive it stamps at all — a
/// controller on the legacy side of the flag day sends the shape that predates the fields, and
/// one on the fenced side sends a fence and a target — and how it classifies a worker's answer.
/// The worker's half is `arroyo-worker`'s `Link::deliver_from_a_predecessor_controller`.
pub(crate) fn directive_from_a_controller_in(mode: LifecycleMode) -> FenceProtocol {
    let authority = crate::LifecycleAuthority::from_parts("job_abc", FENCE, "epoch-1");
    FenceProtocol::for_job(mode, &authority, GENERATION)
        .expect("an adopted controller can address its own generation")
}

/// How a controller running `mode` classifies a worker's `code`.
///
/// The other half of skew. It reads the classification *through* the directive that mode
/// produces, which is what makes this a statement about a controller of that version rather
/// than about a free function: before M11.T26h `Aborted` was ambiguous to a legacy controller —
/// the M11.T08 busy-worker retry — and definitive to a fenced one, and the activation change
/// removed the difference along with the mode parameter. Keeping the mode here is what lets
/// `post_flag_day_skew_moves_exactly_one_transport_code` go on asking the question and get the
/// post-flag-day answer.
pub(crate) fn settlement_under(mode: LifecycleMode, code: Code) -> TransportSettlement {
    directive_from_a_controller_in(mode).transport_settlement(code)
}
