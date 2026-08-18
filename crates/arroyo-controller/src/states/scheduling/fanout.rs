//! The fan-out region: what it issues, what it still owes, and who it may owe it to
//! (M11.T25b and M11.T25c, design M11.D39b).
//!
//! Three separate things live here because M11.D39b makes them one obligation:
//!
//! * the [`ExecutionPlan`] the fan-out sends and the capability gate that decides whether it
//!   may be sent at all;
//! * [`IssuedAttempts`] and the live [`AttemptLedger`] the fan-out writes it through — the
//!   explicit inventory of what was issued, the fan-out's own record of what it is waiting to
//!   hear about, rather than an inference from which futures are still alive; and
//! * [`SettlementBundle`] and [`SettlementOwner`], the typed means by which an interrupted
//!   fan-out hands that inventory **and** the lifecycle authority to an owner that outlives
//!   it, as one unit.
//!
//! # What M11.T25 does not claim
//!
//! Dropping a `tonic` client future resets its stream. It does not revoke work a server has
//! begun: the production worker's `start_execution` takes a `std::sync::Mutex` as its first
//! statement and can be parked inside `poll`, where dropping cannot reach it. Owning the
//! request futures as children of the fan-out is therefore **client-task ownership** — it
//! stops a client request task from silently outliving its phase — and nothing here calls it
//! cancellation.
//!
//! For the same reason M11.T25 implements no [`SettlementOwner`]:
//! [`PhaseContext::settlement_owner`] is always `None`, so the only outcome a transfer can
//! have in this half is [`SettlementOutcome::SettledInPlace`] — the landed
//! [`settle_under_admission`](crate::states::settle_under_admission) rescue, which is
//! retained unchanged on the production path. M11.T26 supplies the cancellation-resistant
//! per-job owner and the durable recovery that make a transfer meaningful.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use arroyo_rpc::grpc::api;
use arroyo_types::{MachineId, WorkerId};
use tracing::info;

use super::ExecutionPlan;
use super::admission::PhaseContext;
use crate::states::{Admission, StateError};

/// One `StartExecution` the fan-out issued, and whether it has been accounted for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AttemptRecord {
    /// The stable `start_execution_id` carried by the request.
    ///
    /// The identifier the worker actually receives, threaded out of the request loop that
    /// mints it rather than reconstructed here — a fabricated one would make the inventory
    /// claim knowledge it does not have, and the whole value of the inventory is that what it
    /// says is what a worker could be holding. It is not an `Option`: an attempt without an
    /// identifier is one nothing could ever reconcile, and there is no way to record one.
    pub(crate) attempt_id: String,
    /// Whether an authoritative outcome for this attempt has been observed.
    ///
    /// "Settled" here means the same thing it means in
    /// [`settle_under_admission`](crate::states::settle_under_admission): a response was
    /// received, not that a client future was dropped or a deadline expired.
    pub(crate) settled: bool,
}

/// Every `StartExecution` a fan-out has issued, by target worker.
///
/// The inventory is explicit rather than derived because the two differ exactly where it
/// matters: a request whose client future has gone away is not a request the worker has
/// stopped considering. What is recorded here is what the controller *issued*, and it is
/// removed from the outstanding set only when an outcome comes back.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct IssuedAttempts {
    attempts: BTreeMap<u64, AttemptRecord>,
}

impl IssuedAttempts {
    /// Records that a request carrying `attempt_id` has been issued to `worker`.
    ///
    /// One record per target worker: re-offering the same identifier after an ambiguous
    /// transport failure is the same attempt, so a replay overwrites rather than accumulates
    /// and the inventory stays bounded by the number of workers however many times the
    /// fan-out's reconcile budget is spent.
    pub(crate) fn issued(&mut self, worker: WorkerId, attempt_id: String) {
        self.attempts.insert(
            worker.0,
            AttemptRecord {
                attempt_id,
                settled: false,
            },
        );
    }

    /// Records an authoritative outcome for `worker`'s attempt.
    ///
    /// Idempotent: settling an attempt that has already settled, or one that was never
    /// issued, changes nothing. That is what lets a reconciliation be run repeatedly without
    /// having to remember whether it already has.
    pub(crate) fn settled(&mut self, worker: WorkerId) {
        if let Some(attempt) = self.attempts.get_mut(&worker.0) {
            attempt.settled = true;
        }
    }

    /// The workers whose attempts have not been accounted for.
    pub(crate) fn outstanding(&self) -> impl Iterator<Item = (WorkerId, &AttemptRecord)> {
        self.attempts
            .iter()
            .filter(|(_, a)| !a.settled)
            .map(|(id, a)| (WorkerId(*id), a))
    }

    /// How many attempts have not been accounted for.
    pub(crate) fn outstanding_count(&self) -> usize {
        self.attempts.values().filter(|a| !a.settled).count()
    }

    /// How many attempts were issued at all.
    pub(crate) fn issued_count(&self) -> usize {
        self.attempts.len()
    }
}

/// The inventory a fan-out writes to while it is running.
///
/// [`IssuedAttempts`] is a value; this is the fan-out's live copy of one. The fan-out's request
/// futures are children of a single future — no request is a task of its own — so the two
/// recording points below happen on one task, and the lock is a formality that lets the phase
/// that started the fan-out hold a handle to the same record. Reading it is therefore an
/// answer about *now*: what has been issued, and what has come back, at the moment of asking,
/// rather than a summary composed after the fan-out has ended.
///
/// # What each recording point means
///
/// * **Issued** is recorded where the identifier is minted, before the first request carrying
///   it is sent. An inventory that under-reports is one that could let a phase release its
///   authority while a request it had forgotten was still live; one that over-reports merely
///   keeps the obligation longer than it had to. Only the first of those is unsafe, so the
///   record is made early.
/// * **Settled** is recorded only when a worker answers — an acknowledgement, or an explicit
///   status that is the worker's own decision. It is *not* recorded when a client future is
///   dropped, when a deadline expires, or when the fan-out gives an unsettleable attempt up
///   after spending its reconcile budget. Owning the request futures as children is
///   **client-task ownership**: it stops a client request task from silently outliving the
///   phase that issued it, and it says nothing about what a worker did with a request that
///   reached it. So an attempt the controller stopped offering stays outstanding here, which
///   is the honest record of what happened — the controller lost knowledge, not exposure.
#[derive(Debug, Default)]
pub(crate) struct AttemptLedger {
    attempts: Mutex<IssuedAttempts>,
}

impl AttemptLedger {
    /// Records that a request carrying `attempt_id` has been issued to `worker`.
    pub(crate) fn issued(&self, worker: WorkerId, attempt_id: &str) {
        self.attempts().issued(worker, attempt_id.to_string());
    }

    /// Records that `worker` answered the attempt addressed to it.
    pub(crate) fn settled(&self, worker: WorkerId) {
        self.attempts().settled(worker);
    }

    /// What the ledger says at this moment.
    pub(crate) fn snapshot(&self) -> IssuedAttempts {
        self.attempts().clone()
    }

    /// The record itself.
    ///
    /// Poisoning is recovered from rather than propagated: nothing under this lock can leave
    /// the inventory inconsistent — the operations are a map insert and a flag — so a panic
    /// elsewhere in the fan-out must not also cost the controller its record of what that
    /// fan-out issued.
    fn attempts(&self) -> MutexGuard<'_, IssuedAttempts> {
        self.attempts.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// An interrupted fan-out's whole obligation: what it issued, and the authority that may not
/// be released until those attempts settle.
///
/// The two travel together deliberately. Handing over the inventory without the
/// [`Admission`] would leave a refusal publishable while the attempts were still live; handing
/// over the authority without the inventory would leave the new owner unable to say what it
/// was waiting for. M11.D39b requires them to move as one unit, and the only way to part with
/// this value is [`Self::transfer_to`], which moves both.
pub(crate) struct SettlementBundle {
    admission: Admission,
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
/// **M11.T25 defines this and implements it nowhere**, which is the point rather than an
/// omission: an owner is only safe once there is a durable record it can be recovered from
/// after a controller restart, and that record — the M11.D39d fence — is M11.T26's. Until
/// then [`PhaseContext::settlement_owner`] answers `None` and the fan-out settles in place,
/// exactly as the landed M11.T08 path does.
///
/// An implementor takes the bundle by value: it receives the issued-attempt inventory and
/// the lifecycle authority together, and there is no way to receive one without the other.
pub(crate) trait SettlementOwner {
    /// Takes over an interrupted fan-out's obligation.
    ///
    /// The implementation must not release the [`Admission`] inside the bundle until every
    /// outstanding attempt has an authoritative outcome, an acknowledged fence or revoke that
    /// makes its identifier permanently inapplicable, or an observed termination of the
    /// worker generation it addressed. Dropping the bundle is never settlement.
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
        Self { admission, issued }
    }

    /// What this bundle still owes.
    pub(crate) fn issued(&self) -> &IssuedAttempts {
        &self.issued
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
        (self.admission, self.issued)
    }
}

/// Hands an interrupted fan-out's obligation to whatever owner the controller has.
///
/// One function rather than a branch at each call site, so that "there is no owner, therefore
/// the fan-out settles in place" is written once and is the same statement everywhere.
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

/// The fan-out's half of the phase graph's access to the job.
impl PhaseContext<'_, '_> {
    /// The job's settlement owner, if this controller has one.
    ///
    /// Always `None` in M11.T25: see [`SettlementOwner`]. It is a method rather than a
    /// constant so that M11.T26 has one place to answer differently, and so that the
    /// always-`None` answer is visibly the seam rather than an oversight.
    pub(crate) fn settlement_owner(&self) -> Option<&dyn SettlementOwner> {
        None
    }

    /// Refuses to schedule onto any worker that did not advertise the `StartExecution`
    /// reconciliation contract.
    ///
    /// Deliberately checked *before* the fan-out's admission is taken: refusing to schedule
    /// is not an irreversible effect and awaits nothing, so putting it inside the region
    /// would hold the job's publication lock for a decision that cannot need it. Everything
    /// the fan-out does with an unsettled request assumes all three clauses of that contract
    /// — an idempotent replayed identifier, `Unavailable` as transport rather than as an
    /// answer, and a handler that cannot be parked — and a worker predating it satisfies
    /// none of them.
    ///
    /// # Errors
    ///
    /// Retryable: the operator's answer is to upgrade the worker image, and the next attempt
    /// sees the new registration.
    pub(crate) fn require_reconciling_workers(&self) -> Result<(), StateError> {
        let unreconciled: Vec<u64> = self
            .workers()
            .values()
            .filter(|w| !w.reconciles_start_execution)
            .map(|w| w.id.0)
            .collect();
        if unreconciled.is_empty() {
            return Ok(());
        }
        Err(self.retryable(
            "workers predating the StartExecution reconciliation contract",
            anyhow::anyhow!(
                "workers {unreconciled:?} did not advertise `reconciles_start_execution`; \
                 upgrade the worker image to at least this controller's version before \
                 scheduling this job"
            ),
            10,
        ))
    }

    /// The plan every worker of this execution is started with.
    fn execution_plan(&self) -> ExecutionPlan {
        let (start_epoch, min_epoch) = self.epochs();
        let leader = if self.leader_mode() {
            self.workers()
                .iter()
                .min_by_key(|w| w.0.0)
                .map(|(id, status)| (*id, status.rpc_address.clone()))
        } else {
            None
        };
        let checkpoint_manifest_ref = leader
            .as_ref()
            .and(self.checkpoint_info())
            .map(|ci| ci.id.clone());
        let program = self.program().clone();
        let assignments = super::compute_assignments(self.workers().values().collect(), &program);

        ExecutionPlan {
            assignments,
            program: api::ArrowProgram::from(program),
            restore_epoch: self.checkpoint_info().map(|i| i.epoch),
            start_epoch,
            min_epoch,
            leader,
            checkpoint_manifest_ref,
            checkpoint_interval_micros: self.job().config.checkpoint_interval.as_micros() as u64,
            // The job's own selector, fixed for the life of this execution and read from the
            // execution rather than from the configuration cell, which is refreshed from the
            // database after every transition.
            state_backend: self.job().execution_selector,
        }
    }

    /// Sends every connected worker its `StartExecution` and waits for all of them to settle.
    ///
    /// Takes the [`Admission`] by value and gives it back, because the region owns it for as
    /// long as anything it issued is unsettled — the landed
    /// [`start_execution_on_workers`](super::start_execution_on_workers) does exactly that
    /// through [`settle_under_admission`](crate::states::settle_under_admission), and this
    /// delegates to it rather than reimplementing the retry, budget and settlement rules that
    /// sixteen review rounds shaped.
    ///
    /// The inventory comes from the fan-out itself. A shared [`AttemptLedger`] is handed to it,
    /// each request records the identifier it was minted with before it is sent, and each
    /// records its own outcome as that outcome arrives — so what comes back here is what the
    /// workers actually answered rather than a summary this method composed from the fan-out's
    /// return value. The difference is visible in exactly the case that matters: an attempt the
    /// fan-out stopped offering after spending its reconcile budget never answered, and stays
    /// outstanding.
    pub(crate) async fn fan_out_start_execution(
        &mut self,
        admission: Admission,
    ) -> (Admission, IssuedAttempts, Result<(), StateError>) {
        let plan = self.execution_plan();
        let machine_ids: HashMap<WorkerId, MachineId> = self
            .workers()
            .iter()
            .map(|(id, status)| (*id, status.machine_id.clone()))
            .collect();
        let connects = self.take_worker_connects();

        let attempts = Arc::new(AttemptLedger::default());
        let job_id = self.job().config.id.clone();
        let pipeline_id = self.job().pipeline_info.pipeline_id.clone();
        let (admission, started) = super::start_execution_on_workers(
            admission,
            job_id,
            pipeline_id,
            plan,
            machine_ids,
            connects,
            Arc::clone(&attempts),
        )
        .await;

        let issued = attempts.snapshot();
        let outcome = match started {
            Ok(connects) => {
                self.record_started_connects(connects);
                Ok(())
            }
            Err(e) => Err(self.retryable("failed to initialize workers", e, 10)),
        };
        (admission, issued, outcome)
    }
}
