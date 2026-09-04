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
//! For the same reason M11.T25 implemented no [`SettlementOwner`]: a transfer is only safe once
//! there is a durable record the same obligation can be recovered from after a controller
//! restart. M11.T26e supplies the owner —
//! [`JobSettlementOwner`](crate::states::lifecycle::settlement::JobSettlementOwner) — and
//! [`PhaseContext::settlement_owner`] answers with it for every production job since M11.T26h's
//! activation change. It answers `None` only for a context built in the pre-flag-day peer mode
//! [`LifecycleMode::LegacyT08`](crate::states::lifecycle::LifecycleMode), where the only outcome
//! a transfer can have is `SettlementOutcome::SettledInPlace`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use arroyo_rpc::grpc::api;
use arroyo_rpc::identity::WorkerClient;
use arroyo_types::{MachineId, WorkerId};
use tracing::warn;

use super::ExecutionPlan;
use super::admission::PhaseContext;
use crate::states::lifecycle::{FenceProtocol, StartTargets, advance_fence};
use crate::states::{Admission, StateError};

pub(crate) mod settlement;

pub(crate) use settlement::observed::{Accounted, Discharged, Observed};
pub(crate) use settlement::{HandoverRecord, SettlementBundle, SettlementOwner, hand_over};
// [`SettlementOutcome`] is re-exported unconditionally, and must stay that way. Review round 4
// is what put it in the production half at all, and M11.T26h's removal of the region rescue did
// not take it out again: [`super::phases::StartFanOut::issue`] resolves whatever [`hand_over`]
// returns through
// `SettlementOutcome::into_fencing_record` without ever binding it — so a re-export for the
// tests alone would have had to be `#[cfg(test)]`. A line-start `#[cfg(test)]` this high in the
// file truncates the production half that `super::phase_tests::phase_graph_production_sources`
// cuts at the first one, which would make every source pin over this file vacuous **while still
// passing**. Whatever else changes here, no `#[cfg(test)]` may go above the production code.

/// What accounted for an issued identifier (M11.D39e(v)).
///
/// The three, and the only three: *"every issued request is accounted for by an authoritative
/// response, a worker-acknowledged fence/revoke that makes the ID permanently non-applicable, or
/// observed target worker-generation termination."* A closed enum rather than a `bool`, because
/// the record has to say **which** — an identifier accounted for by an acknowledged fence is one
/// no worker ever answered, and reading the two as the same fact is how a controller comes to
/// believe a request was answered because a fence moved.
///
/// Nothing here can be produced by a timeout, a dropped client future, a fence CAS or a database
/// read-through. Not because those are refused, but because
/// [`Observed`](settlement::observed::Observed) is what produces these and it has no constructor
/// for any of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Accounting {
    /// The target generation's own answer about this identifier — a response, or an explicit
    /// status that is the worker's own decision.
    AuthoritativeResponse,
    /// The target generation acknowledged a newer fence and the revokes it carried, which makes
    /// this identifier permanently inapplicable there.
    AcknowledgedFence,
    /// The target worker generation was observed to have terminated, so nothing addressed to it
    /// can be applied.
    TerminatedGeneration,
}

impl Accounting {
    /// The word for a log line. A word rather than a `Debug` because an operator reading why a
    /// job released its authority is reading prose.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Accounting::AuthoritativeResponse => "an authoritative response",
            Accounting::AcknowledgedFence => "an acknowledged fence and its revokes",
            Accounting::TerminatedGeneration => "an observed generation termination",
        }
    }
}

/// One `StartExecution` the fan-out issued, and what has accounted for it.
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
    /// What accounted for this attempt, or `None` while nothing has.
    ///
    /// The `None` is the outstanding set: there is one place an unaccounted identifier can be,
    /// and the only operations that move one out of it are the three that observe a fact
    /// M11.D39e(v) allows. A client future being dropped, a deadline expiring or the fan-out
    /// giving the attempt up after spending its reconcile budget are none of them, and leave the
    /// record exactly as it is.
    pub(crate) accounted: Option<Accounting>,
}

/// Every `StartExecution` a fan-out has issued, by target worker.
///
/// The inventory is explicit rather than derived because the two differ exactly where it
/// matters: a request whose client future has gone away is not a request the worker has
/// stopped considering. What is recorded here is what the controller *issued*, and it is
/// removed from the outstanding set only when something accounts for it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct IssuedAttempts {
    /// The worker generation every identifier here was addressed to.
    ///
    /// Carried by the inventory rather than by each record because one fan-out addresses one
    /// generation: it is a fact about the attempt, and putting a copy on every record would be a
    /// second place for it to disagree with itself. It is what makes an observation checkable —
    /// see [`SettlementBundle::observe`](settlement::SettlementBundle::observe) — because an
    /// answer about a *different* generation's attempt must account for nothing here.
    ///
    /// Zero on a [`Default`] inventory, which is the same sentinel M11.T26c gave the wire:
    /// generation zero addresses nothing, and an empty inventory addressed nothing.
    generation: u64,
    /// The lifecycle fence every identifier here was issued under (M11.T26f, M11.D39e(v)).
    ///
    /// Carried for the same reason [`Self::generation`] is, and it answers the other half of the
    /// same question. The generation says whether an observation is about this attempt; the
    /// fence says whether a *fence acknowledgement* about this attempt has made its identifiers
    /// inapplicable. A generation acknowledging the very fence this attempt's starts carry has
    /// revoked nothing of this attempt's — a worker revokes what is below the fence it takes —
    /// so without this the settlement check could only be "the right worker answered", and an
    /// acknowledgement of a fence too low to revoke anything would settle an identifier a worker
    /// may still apply. See
    /// [`FenceAcknowledgement`](crate::states::lifecycle::handshake::FenceAcknowledgement).
    ///
    /// Zero on a [`Default`] inventory and under the pre-flag-day protocol, where no fence is
    /// carried at all: any real acknowledgement is above zero, which is correct — a generation in
    /// strict mode refuses the fence-less starts such an inventory holds.
    fence: u64,
    attempts: BTreeMap<u64, AttemptRecord>,
}

impl IssuedAttempts {
    /// The inventory of a fan-out addressing `generation` under `fence`.
    ///
    /// Both halves of the identity, in one constructor, because there is no correct inventory
    /// that names one and not the other: an observation is checked against the pair.
    pub(crate) fn issued_under(generation: u64, fence: u64) -> Self {
        Self {
            generation,
            fence,
            attempts: BTreeMap::new(),
        }
    }

    /// The worker generation these identifiers were issued to.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// The lifecycle fence these identifiers were issued under.
    pub(crate) fn fence(&self) -> u64 {
        self.fence
    }

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
                accounted: None,
            },
        );
    }

    /// Records that `accounting` accounts for `worker`'s attempt.
    ///
    /// **Private, and the privacy is the point.** The only caller is
    /// [`IssuedAttempts::observe`](settlement::observed), in the child module that checks the
    /// observation against what was issued, so there is no route by which an identifier stops
    /// being outstanding without the identity behind it having been compared. A `pub` version of
    /// this would be a second route, and the thing it would let through is exactly what
    /// M11.D39e(v) is about: an identifier accounted for by an answer that was never about it.
    ///
    /// Idempotent, and the first fact wins: accounting for an identifier something has already
    /// accounted for, or for one that was never issued, changes nothing.
    fn account(&mut self, worker: WorkerId, accounting: Accounting) {
        if let Some(attempt) = self.attempts.get_mut(&worker.0)
            && attempt.accounted.is_none()
        {
            attempt.accounted = Some(accounting);
        }
    }

    /// What was issued to `worker`, if anything was.
    pub(crate) fn record(&self, worker: WorkerId) -> Option<&AttemptRecord> {
        self.attempts.get(&worker.0)
    }

    /// Every attempt this fan-out issued, accounted for or not.
    pub(crate) fn records(&self) -> impl Iterator<Item = (WorkerId, &AttemptRecord)> {
        self.attempts.iter().map(|(id, a)| (WorkerId(*id), a))
    }

    /// The workers whose attempts have not been accounted for.
    pub(crate) fn outstanding(&self) -> impl Iterator<Item = (WorkerId, &AttemptRecord)> {
        self.records().filter(|(_, a)| a.accounted.is_none())
    }

    /// How many attempts have not been accounted for.
    pub(crate) fn outstanding_count(&self) -> usize {
        self.outstanding().count()
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
/// The ledger is the one value that is live for the whole fan-out and carries the inventory
/// throughout, so it is also where the job's [`SettlementOwner`] is recorded — which is what
/// lets [`super::phases::StartFanOut::issue`] hand the inventory and the authority over as one
/// unit rather than reading the owner from somewhere the phase might no longer have.
///
/// An owner that *declines* leaves the obligation with the phase, which is what
/// `SettlementOutcome::SettledInPlace` means: the phase settles it before releasing anything.
pub(crate) struct AttemptLedger {
    attempts: Mutex<IssuedAttempts>,
    /// The job's settlement owner. `Some` for every production job since M11.T26h.
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
    /// The ledger of a fan-out addressing `generation`, whose obligation belongs to `owner` if
    /// it is interrupted.
    ///
    /// The generation and the fence are seeded here rather than stamped on later because they
    /// are what makes the inventory checkable: an obligation built from this ledger carries the
    /// generation its identifiers were issued to and the fence they were issued under, so an
    /// observation about some other generation — or an acknowledgement of a fence too low to
    /// revoke them — accounts for nothing in it. A ledger that learned either after the fact
    /// would have a window in which it addressed everything.
    pub(crate) fn owned_by(
        owner: Option<Arc<dyn SettlementOwner>>,
        generation: u64,
        fence: u64,
    ) -> Self {
        Self {
            attempts: Mutex::new(IssuedAttempts::issued_under(generation, fence)),
            owner,
        }
    }

    /// Records that a request carrying `attempt_id` has been issued to `worker`.
    pub(crate) fn issued(&self, worker: WorkerId, attempt_id: &str) {
        self.attempts().issued(worker, attempt_id.to_string());
    }

    /// Records that `worker` authoritatively answered `attempt_id`.
    ///
    /// The identifier is passed rather than assumed, and it is checked: the fan-out calls this
    /// from inside the loop that owns one request, so the identifier it names is the one that
    /// request carries, and an inventory that recorded an answer for anything else would be
    /// claiming knowledge about a request this fan-out did not issue. The check is the same one
    /// an owner's later observations go through — see
    /// [`IssuedAttempts::observe`](settlement::observed) — which is what makes "the fan-out
    /// answered it" and "the owner observed an answer" one fact rather than two records.
    pub(crate) fn answered(&self, worker: WorkerId, attempt_id: &str) {
        let mut attempts = self.attempts();
        let observed = Observed::authoritative_response(worker, attempts.generation(), attempt_id);
        if let Accounted::NotThisObligation(disagreement) = attempts.observe(&observed) {
            warn!(
                worker_id = worker.0,
                disagreement = ?disagreement,
                "a StartExecution response does not name what this fan-out issued to that                  worker, so nothing is accounted for by it"
            );
        }
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
    /// `Some` for a job running the M11.D39a/D39e fenced mechanism and `None` under
    /// [`LifecycleMode::LegacyT08`](crate::states::lifecycle::LifecycleMode::LegacyT08) — the
    /// pre-flag-day peer, which no production job is since M11.T26h, and where an interrupted
    /// fan-out settles in place instead.
    ///
    /// One reading, from the job, and the *same* value on both calls of one fan-out: the ledger
    /// takes it for the cancelled path and [`super::super::phases::StartFanOut::issue`] takes it
    /// for the returned one, and two owners built here would mean the rescue and the phase were
    /// speaking to different parties about one obligation. See
    /// [`JobContext::settlement_owner`](crate::states::JobContext::settlement_owner), which
    /// holds it.
    ///
    /// An owned handle rather than a borrow, because the owner has to be reachable from the
    /// rescue that runs after the phase — and the whole `PhaseContext` — has been dropped.
    /// A borrow would have made the seam work on every path except the one it is for.
    pub(crate) fn settlement_owner(&self) -> Option<Arc<dyn SettlementOwner>> {
        self.job().settlement_owner()
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

    /// The lifecycle-fence protocol this attempt's directives are issued under.
    ///
    /// One reading, from the job's own mechanism and its durable authority; see
    /// [`JobContext::fence_protocol`](crate::states::JobContext::fence_protocol).
    ///
    /// # Errors
    ///
    /// Retryable. A controller that must fence and holds no adopted fence cannot address its
    /// worker generations at all, and the answer is to adopt the job — which the next attempt's
    /// preamble does — rather than to fall back to fence-less requests.
    pub(super) fn fence_protocol(&self) -> Result<FenceProtocol, StateError> {
        self.job().fence_protocol().map_err(|e| {
            self.retryable(
                "the job's lifecycle fence cannot address its worker generations",
                anyhow::anyhow!("{e}"),
                10,
            )
        })
    }

    /// Turns this attempt's open worker channels into the targets its fan-out may address.
    ///
    /// Under the legacy protocol that is all of them, unchanged: no fence is advanced and no
    /// request is sent here, so a pre-flag-day attempt does exactly what it did before this
    /// existed.
    ///
    /// Under the fenced protocol it is the **active replacement handshake** (M11.D39e(i)): every
    /// generation is sent a `FENCE_ONLY` directive under this job's fence and must acknowledge
    /// it before any `StartExecution` is issued to any of them. Two things make that a property
    /// of the code rather than of its ordering:
    ///
    /// * it happens *here*, where the channels are turned into targets, so there is no ordering
    ///   in which a start could precede it — the value the fan-out needs does not exist until
    ///   the handshake has produced it; and
    /// * an [`AcknowledgedTarget`](crate::states::lifecycle::handshake::AcknowledgedTarget) is
    ///   built only by observing an acknowledgement, so a fenced start to a generation that did
    ///   not answer is not a check that was skipped but a value that cannot be built.
    ///
    /// Every generation reached here has announced itself — the only way a worker enters
    /// [`workers`](Self::workers) and gets a channel is the `WorkerConnect` its registration
    /// *request* produced, and the connects were taken from that same set — so "registration and
    /// the handshake precede start admission" is one statement about this method. It is the
    /// request and not the answer: `register_worker` enqueues that message before it replies, so
    /// the handshake below may reach a generation whose answer is still in flight, which is the
    /// window `WorkerLifecycle::announce` exists to make admissible.
    ///
    /// It is inside the admitted region because advancing a worker's fence is irreversible: the
    /// generation is in strict mode afterwards and refuses everything older, which is exactly
    /// the effect a refusal published concurrently must not race.
    ///
    /// # Errors
    ///
    /// Retryable when a generation did not acknowledge. Nothing is started when one does not:
    /// see [`advance_fence`]'s module documentation on why this is all or nothing.
    async fn address_every_worker(
        &mut self,
        admission: &Admission,
        connects: HashMap<WorkerId, WorkerClient>,
    ) -> Result<StartTargets, StateError> {
        let protocol = self.fence_protocol()?;
        let FenceProtocol::Fenced(generation) = protocol else {
            // The pre-flag-day arm. The constructor is *asked* whether this protocol may skip
            // the handshake rather than being told that it may, so this arm and the landed
            // `Scheduling::next` body consult one rule; a protocol shape added later that is
            // neither of today's two makes both of them refuse rather than making one of them
            // quietly send fence-less requests under it.
            return StartTargets::without_a_handshake(protocol, connects).ok_or_else(|| {
                self.retryable(
                    "the fenced protocol reached the unfenced fan-out",
                    anyhow::anyhow!(
                        "a fenced attempt cannot address its workers without a fence handshake"
                    ),
                    10,
                )
            });
        };
        admission
            .effect(
                "advance the lifecycle fence on every worker generation",
                advance_fence(generation, connects),
            )
            .await
            .map_err(|refusal| {
                self.retryable(
                    "worker generations did not acknowledge the job's lifecycle fence",
                    anyhow::anyhow!("{refusal}"),
                    10,
                )
            })
    }

    /// Sends every connected worker its `StartExecution` and waits for all of them to settle.
    ///
    /// Takes the [`Admission`] by value and gives it back, because the region owns it for as
    /// long as anything it issued is unsettled — which is exactly what
    /// [`start_execution_on_workers`](super::start_execution_on_workers) does, and this
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

        let generation = self.addressed_generation();
        let fence = self.addressed_fence();
        let targets = match self.address_every_worker(&admission, connects).await {
            Ok(targets) => targets,
            Err(reason) => {
                return (
                    admission,
                    IssuedAttempts::issued_under(generation, fence),
                    Err(reason),
                );
            }
        };
        // Every acknowledgement the handshake observed is an observation this attempt made, so
        // it is recorded where observations go rather than being consumed by the fan-out alone.
        // None of them settles anything of *this* attempt's — the fence they acknowledge is the
        // one its own starts carry, and a worker revokes what is below the fence it takes — and
        // that refusal is the reconciliation's to make rather than this method's. See
        // `an_acknowledgement_of_the_fence_the_attempt_issued_under_settles_nothing`.
        self.record_observed_acknowledgements(targets.acknowledgements());

        // The owner is read here, while the phase still exists, precisely because the path
        // that needs it is the one on which the phase does not: a cancelled state task drops
        // this context, and the ledger — captured by the region — is what carries the answer
        // into the rescue.
        let attempts = Arc::new(AttemptLedger::owned_by(
            self.settlement_owner(),
            generation,
            fence,
        ));
        let job_id = self.job().config.id.clone();
        let pipeline_id = self.job().pipeline_info.pipeline_id.clone();
        let (admission, started) = super::start_execution_on_workers(
            admission,
            job_id,
            pipeline_id,
            plan,
            machine_ids,
            targets,
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
