//! The token-free fencing substrate (M11.T25b, design M11.D39b).
//!
//! [`Fencing`] is where a scheduling attempt goes when it cannot continue. It holds **no**
//! [`Admission`](crate::states::Admission) — that is the whole of its safety property — and
//! it offers exactly three kinds of operation:
//!
//! * idempotent fence/revoke reconciliation over the worker generations a stale request could
//!   still reach ([`Fencing::reconcile`], [`Fencing::observe_fence_acknowledged`]);
//! * observation of a generation having been torn down or terminated
//!   ([`Fencing::observe_generation_terminated`]); and
//! * coalescing of the job's lifecycle intents while it fences
//!   ([`Fencing::coalesce_intent`]).
//!
//! It exposes **no** start, generation, recovery or commit effect, and it cannot obtain one:
//! it has no token to consume and no method that takes one. That is what makes "the admission
//! may be released only into token-free `Fencing`" a fact about the type rather than a rule
//! someone has to remember at each interruption.
//!
//! # What this is not, in M11.T25
//!
//! It is a substrate, not the protocol. There is no durable fence here, no wire field, no
//! worker acknowledgement, and no publication of `Refused`: M11.D39d and M11.D39e are
//! M11.T26's, and `Refused` may be published only once every target generation has
//! acknowledged the newer fence or has been observed terminated. What this half provides is
//! the shape T26 fills: a per-job set of addressable targets, an inventory of attempts that
//! were issued into them, and a reconciliation that is safe to run repeatedly because running
//! it twice says the same thing as running it once.
//!
//! An interruption in M11.T25 therefore ends the same way the landed M11.T08 path ends one —
//! by returning the [`StateError`] that caused it, after reconciling what can be reconciled
//! in memory. Nothing here claims that a partitioned worker has been fenced.

use std::collections::BTreeSet;

use arroyo_types::WorkerId;
use tracing::info;

use super::admission::PhaseContext;
use super::fanout::IssuedAttempts;
use crate::states::StateError;

/// The worker generations a stale request issued by this scheduling attempt could still be
/// delivered to.
///
/// A `BTreeSet` rather than a `HashSet` so that the reconciliation log is in a stable order:
/// the only consumer of this today is a human reading why a job is fencing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FenceTargets {
    /// Targets that have neither acknowledged a fence nor been observed terminated.
    pending: BTreeSet<u64>,
    /// Targets that have acknowledged the newer fence and its revokes.
    acknowledged: BTreeSet<u64>,
    /// Targets whose generation has been observed to have gone away.
    terminated: BTreeSet<u64>,
}

impl FenceTargets {
    /// The targets an attempt that reached these workers must reconcile with.
    pub(crate) fn for_workers(workers: impl IntoIterator<Item = WorkerId>) -> Self {
        Self {
            pending: workers.into_iter().map(|w| w.0).collect(),
            acknowledged: BTreeSet::new(),
            terminated: BTreeSet::new(),
        }
    }

    /// How many targets are still unaccounted for.
    pub(crate) fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Records a target's acknowledgement of the newer fence and its revokes.
    ///
    /// Returns whether this changed anything, which is what makes the operation idempotent
    /// rather than merely repeatable: a second acknowledgement from the same generation is
    /// not a second event.
    pub(super) fn acknowledge(&mut self, worker: WorkerId) -> bool {
        if !self.pending.remove(&worker.0) {
            return false;
        }
        self.acknowledged.insert(worker.0);
        true
    }

    /// Records that a target's generation has been observed terminated.
    pub(super) fn terminate(&mut self, worker: WorkerId) -> bool {
        let was_pending = self.pending.remove(&worker.0);
        let was_acknowledged = self.acknowledged.remove(&worker.0);
        if !was_pending && !was_acknowledged && self.terminated.contains(&worker.0) {
            return false;
        }
        self.terminated.insert(worker.0)
    }
}

/// What one reconciliation pass found.
///
/// Returned rather than logged-and-forgotten so that a test can assert the pass is
/// idempotent by comparing two of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FenceReconciliation {
    /// Target generations that have neither acknowledged nor been observed terminated.
    pub(crate) pending_targets: usize,
    /// Issued attempts with no authoritative outcome.
    pub(crate) outstanding_attempts: usize,
}

impl FenceReconciliation {
    /// Whether every target has answered and every attempt has an outcome.
    ///
    /// M11.T25 records this; it does not act on it. Publishing `Refused` on the strength of
    /// it needs the durable fence and the worker acknowledgement protocol M11.T26 owns.
    pub(crate) fn is_settled(&self) -> bool {
        self.pending_targets == 0 && self.outstanding_attempts == 0
    }
}

/// Whether coalescing found a newer lifecycle intent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IntentCoalescing {
    /// The job's writer has said nothing new since the last look.
    Unchanged,
    /// A newer decision was folded into the standing reason for fencing.
    Coalesced,
}

/// A job that is fencing: reconciling what it issued, and publishing nothing.
pub(crate) struct Fencing<'a, 'ctx> {
    ctx: PhaseContext<'a, 'ctx>,
    targets: FenceTargets,
    outstanding: IssuedAttempts,
    /// Attempts a settlement owner took responsibility for, if the controller had one.
    ///
    /// Always zero in M11.T25, which implements no
    /// [`SettlementOwner`](super::fanout::SettlementOwner); it is recorded rather than
    /// assumed so that a reconciliation says how much of the obligation it is still speaking
    /// for.
    transferred: usize,
}

impl<'a, 'ctx> Fencing<'a, 'ctx> {
    /// The fencing state an interrupted phase releases into.
    ///
    /// Note what the signature does not take: there is no [`Admission`](crate::states::Admission)
    /// parameter and no field to put one in, so a caller cannot carry a token in here even by
    /// accident.
    pub(crate) fn new(
        ctx: PhaseContext<'a, 'ctx>,
        targets: FenceTargets,
        outstanding: IssuedAttempts,
    ) -> Self {
        Self {
            ctx,
            targets,
            outstanding,
            transferred: 0,
        }
    }

    /// Records that an owner outside this phase took `attempts` of the obligation over.
    pub(crate) fn note_transferred(&mut self, attempts: usize) {
        self.transferred = attempts;
    }

    /// Reconciles what this job still owes, and reports what is left.
    ///
    /// Idempotent by construction: it reads the target set and the issued-attempt inventory
    /// and derives a count from them. Running it again after nothing has been observed
    /// produces the same answer, which is what lets a caller run it on every turn of whatever
    /// loop it is in without tracking whether it already has.
    pub(crate) fn reconcile(&mut self) -> FenceReconciliation {
        for worker in self.ctx.observed_fence_acknowledgements() {
            self.observe_fence_acknowledged(worker);
        }
        for worker in self.ctx.observed_generation_terminations() {
            self.observe_generation_terminated(worker);
        }
        let reconciliation = FenceReconciliation {
            pending_targets: self.targets().pending(),
            outstanding_attempts: self.outstanding().outstanding_count(),
        };
        for (worker, attempt) in self.outstanding().outstanding() {
            info!(
                worker_id = worker.0,
                attempt_id = attempt.attempt_id,
                "an issued StartExecution has no authoritative outcome"
            );
        }
        info!(
            job_id = %self.ctx.job().config.id,
            pending_targets = reconciliation.pending_targets,
            issued_attempts = self.outstanding().issued_count(),
            outstanding_attempts = reconciliation.outstanding_attempts,
            transferred_attempts = self.transferred,
            "reconciling a fencing job's outstanding scheduling work"
        );
        reconciliation
    }

    /// Records that a worker generation has acknowledged the newer fence and its revokes.
    ///
    /// Returns whether anything changed. Every attempt this generation was issued is settled
    /// by the acknowledgement, because an acknowledged revoke makes those identifiers
    /// permanently inapplicable — that is the M11.D39e rule this substrate is shaped for, and
    /// M11.T26 supplies the protocol that makes an acknowledgement observable at all.
    pub(crate) fn observe_fence_acknowledged(&mut self, worker: WorkerId) -> bool {
        let changed = self.targets.acknowledge(worker);
        if changed {
            self.outstanding.settled(worker);
        }
        changed
    }

    /// Records that a worker generation has been observed torn down.
    ///
    /// The other way an issued attempt stops being applicable: a generation that no longer
    /// exists cannot apply anything addressed to it.
    pub(crate) fn observe_generation_terminated(&mut self, worker: WorkerId) -> bool {
        let changed = self.targets.terminate(worker);
        if changed {
            self.outstanding.settled(worker);
        }
        changed
    }

    /// Folds whatever the job's single writer has decided since the last look into the
    /// standing reason for fencing.
    ///
    /// While a job fences, a further decision is not a further transition: there is nothing
    /// left to stop and nothing that may be published. So an intent read here is *coalesced*
    /// — recorded as the reason this attempt ends — rather than applied.
    /// A stop decided here is deliberately *not* turned into a transition. There is nothing
    /// left to leave for: the attempt has already ended, and this is the reason it ended being
    /// recorded rather than a phase deciding what to do next. Only a refusal — which changes
    /// what the job is reported as failing for — is folded in.
    pub(crate) fn coalesce_intent(&mut self, standing: &mut StateError) -> IntentCoalescing {
        match self.ctx.observe_intent_in_wait() {
            Ok(_) => IntentCoalescing::Unchanged,
            Err(newer) => {
                *standing = newer;
                IntentCoalescing::Coalesced
            }
        }
    }

    /// The targets this job is fencing against.
    pub(crate) fn targets(&self) -> &FenceTargets {
        &self.targets
    }

    /// The attempts this job has not accounted for.
    pub(crate) fn outstanding(&self) -> &IssuedAttempts {
        &self.outstanding
    }
}

/// A phase that could not continue, and the fencing state it released its authority into.
///
/// The two are one value because M11.D39b makes them one step: a phase does not "fail and
/// then fence", it *becomes* token-free fencing carrying the reason. There is no constructor
/// that produces an [`Interrupted`] without a [`Fencing`], so no interruption can leave the
/// job holding a token.
pub(crate) struct Interrupted<'a, 'ctx> {
    fencing: Fencing<'a, 'ctx>,
    reason: StateError,
}

impl<'a, 'ctx> Interrupted<'a, 'ctx> {
    /// The interruption a phase releasing into fencing produces.
    pub(crate) fn new(fencing: Fencing<'a, 'ctx>, reason: StateError) -> Self {
        Self { fencing, reason }
    }

    /// The fencing substrate, for a caller that wants to observe acknowledgements before it
    /// reports.
    pub(crate) fn fencing_mut(&mut self) -> &mut Fencing<'a, 'ctx> {
        &mut self.fencing
    }

    /// Reconciles once, coalesces whatever the job's writer has since decided, and reports.
    ///
    /// M11.T25 stops here. The reported [`StateError`] is what the landed path would have
    /// produced for the same interruption, so an inactive substrate ends a scheduling attempt
    /// exactly as the selected one does. Turning a settled reconciliation into a published
    /// `Refused` is M11.T26's, and needs the durable fence this half deliberately does not
    /// have.
    pub(crate) fn reconcile_and_report(mut self) -> StateError {
        let reconciliation = self.fencing.reconcile();
        let coalescing = self.fencing.coalesce_intent(&mut self.reason);
        info!(
            settled = reconciliation.is_settled(),
            coalesced = matches!(coalescing, IntentCoalescing::Coalesced),
            "a scheduling attempt ended in token-free fencing"
        );
        self.reason
    }
}
