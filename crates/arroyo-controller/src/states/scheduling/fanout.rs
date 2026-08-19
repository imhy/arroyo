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
//! * [`SettlementBundle`] and [`SettlementOwner`] — in the [`settlement`] child module — the
//!   typed means by which an interrupted fan-out hands that inventory **and** the lifecycle
//!   authority to an owner that outlives it, as one unit.
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
//! retained on the production path and whose behaviour with no owner is what it always was.
//! M11.T26 supplies the cancellation-resistant per-job owner and the durable recovery that
//! make a transfer meaningful.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use arroyo_rpc::grpc::api;
use arroyo_types::{MachineId, WorkerId};

use super::ExecutionPlan;
use super::admission::PhaseContext;
use crate::states::{Admission, SettlementRescue, StateError};

pub(crate) mod settlement;

pub(crate) use settlement::{
    HandoverRecord, SettlementBundle, SettlementOutcome, SettlementOwner, hand_over,
    retain_without_a_phase,
};
// [`SettlementOutcome`] is re-exported unconditionally, and must stay that way. Review round 4
// is what put it in the production half at all: [`AttemptLedger::settlement_rescue`] now
// answers every arm of it instead of discarding the value, where before nothing here named the
// type — [`super::phases::StartFanOut::issue`] resolves whatever [`hand_over`] returns through
// `SettlementOutcome::into_fencing_record` without ever binding it — so a re-export for the
// tests alone would have had to be `#[cfg(test)]`. A line-start `#[cfg(test)]` this high in the
// file truncates the production half that `super::phase_tests::phase_graph_production_sources`
// cuts at the first one, which would make every source pin over this file vacuous **while still
// passing**. Whatever else changes here, no `#[cfg(test)]` may go above the production code.

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
///
/// # Why it also knows who the obligation belongs to
///
/// A fan-out can end in two ways, and only one of them runs the line after the `await`. If the
/// job's state task is cancelled mid-fan-out, the phase that would have handed the obligation
/// over is gone before it can; what survives is the region rescue inside
/// [`settle_under_admission`](crate::states::settle_under_admission), which holds the
/// admission until the requests settle. The ledger is the one value that is live on *both*
/// paths and carries the inventory on both, so it is also where the job's
/// [`SettlementOwner`] is recorded — see [`Self::settlement_rescue`]. `None` for every caller
/// in M11.T25, and for the landed M11.T08 path, whose rescue therefore behaves exactly as it
/// did before.
///
/// An owner that *declines* there leaves the obligation with nobody at all, because the phase
/// that would otherwise have kept it no longer exists. [`retain_without_a_phase`] is what
/// answers that, and it does not release the authority behind an unaccounted attempt.
#[derive(Default)]
pub(crate) struct AttemptLedger {
    attempts: Mutex<IssuedAttempts>,
    /// The job's settlement owner, if this controller has one. Always `None` in M11.T25.
    owner: Option<Arc<dyn SettlementOwner>>,
}

impl std::fmt::Debug for AttemptLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttemptLedger")
            .field("attempts", &self.snapshot())
            .field("owned", &self.owner.is_some())
            .finish()
    }
}

impl AttemptLedger {
    /// The ledger of a fan-out whose obligation belongs to `owner` if it is interrupted.
    pub(crate) fn owned_by(owner: Option<Arc<dyn SettlementOwner>>) -> Self {
        Self {
            attempts: Mutex::new(IssuedAttempts::default()),
            owner,
        }
    }

    /// What a region rescue does with the authority it recovered, for this fan-out.
    ///
    /// `None` when the controller has no settlement owner: then there is nobody to hand the
    /// obligation to, the rescue releases the admission once the requests have settled, and
    /// that is the landed M11.T08 behaviour unchanged.
    ///
    /// Otherwise it is the same hand-over [`super::super::phases::StartFanOut::issue`]
    /// performs, built at the only other moment the two halves of the obligation exist
    /// together: the inventory is read from this ledger — which the request futures have gone
    /// on writing to inside the rescued region, so it is what the workers actually answered —
    /// and the authority is the admission the rescue is holding.
    ///
    /// # Why every arm is answered here
    ///
    /// The offer is the same on both paths; the *disposal* is not, because this one has no
    /// phase to give a declined obligation back to. Review round 4 found this outcome being
    /// discarded as a statement, which released the job's lifecycle authority the instant an
    /// owner declined — and this is the half a declining owner is most likely to be reached
    /// from, since declining is what an owner does while it is shutting down and cancellation
    /// is what happens to a job's state task when a controller shuts down. So every arm is
    /// named, and [`retain_without_a_phase`] stands in for the phase that is gone.
    pub(crate) fn settlement_rescue(self: &Arc<Self>) -> Option<SettlementRescue> {
        let owner = Arc::clone(self.owner.as_ref()?);
        let ledger = Arc::clone(self);
        Some(Box::new(move |admission| {
            let bundle = SettlementBundle::new(admission, ledger.snapshot());
            match hand_over(bundle, Some(owner.as_ref())) {
                // The obligation left through the seam, or was lost inside it. `hand_over` has
                // already said which, and logged it: an owner that took it holds the authority
                // now, and one that dropped it released the authority itself. Neither leaves
                // this closure anything to hold.
                SettlementOutcome::Transferred(_) | SettlementOutcome::Abandoned { .. } => {}
                // Nobody took it. On the phase's own path this is the phase keeping what it was
                // always able to keep; here the phase is gone, so the obligation is disposed of
                // by what is still owed on it.
                SettlementOutcome::SettledInPlace(admission, issued) => {
                    retain_without_a_phase(admission, issued);
                }
            }
        }))
    }

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

/// The fan-out's half of the phase graph's access to the job.
impl PhaseContext<'_, '_> {
    /// The job's settlement owner, if this controller has one.
    ///
    /// Always `None` in M11.T25: see [`SettlementOwner`]. It is a method rather than a
    /// constant so that M11.T26 has one place to answer differently, and so that the
    /// always-`None` answer is visibly the seam rather than an oversight.
    ///
    /// An owned handle rather than a borrow, because the owner has to be reachable from the
    /// rescue that runs after the phase — and the whole `PhaseContext` — has been dropped.
    /// A borrow would have made the seam work on every path except the one it is for.
    pub(crate) fn settlement_owner(&self) -> Option<Arc<dyn SettlementOwner>> {
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

        // The owner is read here, while the phase still exists, precisely because the path
        // that needs it is the one on which the phase does not: a cancelled state task drops
        // this context, and the ledger — captured by the region — is what carries the answer
        // into the rescue.
        let attempts = Arc::new(AttemptLedger::owned_by(self.settlement_owner()));
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
