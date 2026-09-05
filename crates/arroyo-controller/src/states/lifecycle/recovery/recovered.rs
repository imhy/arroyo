//! The obligation a previous attempt left, as a recovering controller reads and advances it
//! (M11.T26f, design M11.D39d/M11.D39e(v)).
//!
//! A **child** of [`super`] rather than a sibling, because what is here is the value that pass
//! works on: the record read back from the row, the two witnesses that may advance it, and the
//! rules under which each does. The pass itself — the adoption it runs under, the fence it
//! advances, the write it ends with — is next door.
//!
//! # The two witnesses
//!
//! [`ObservedTermination`] is minted here, by [`observe_terminations`], and nowhere else;
//! [`FenceAcknowledgement`](super::super::handshake::FenceAcknowledgement) is minted by the
//! module that reads a `FENCE_ACKNOWLEDGED` response. Neither has a public constructor, so a
//! failed listing, a broken channel and an expired deadline are not readings this module
//! refuses — they are readings nothing in the crate can express.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arroyo_rpc::fence_wire::WorkerIncarnation;
use arroyo_rpc::fencing::{FenceTarget, FenceTargetState, Fencing, FencingRecordError};
use arroyo_types::WorkerId;
use thiserror::Error;
use tracing::info;

use super::super::fence::metrics;
use super::super::handshake::FenceAcknowledgement;
use crate::schedulers::{GenerationObservation, Scheduler};

/// A worker generation observed to have terminated (M11.D39e(v)).
///
/// The fields are private and the only constructor is [`observe_terminations`]'s own, so this is
/// a witness in the same sense
/// [`FenceAcknowledgement`](super::handshake::FenceAcknowledgement) is: holding one means a
/// scheduler that tracks its own worker generations answered a listing **successfully** and
/// that generation was not in it. A listing that failed, a connection that broke and a deadline
/// that expired produce no value of this type, so none of them can be spelled as a termination
/// anywhere in the crate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ObservedTermination {
    worker: WorkerId,
    generation: u64,
}

impl ObservedTermination {
    /// The worker whose generation is gone.
    pub(crate) fn worker(&self) -> WorkerId {
        self.worker
    }

    /// The generation that is gone.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
}

/// Why nothing could be observed about which of a job's worker generations are still live.
///
/// Not a failure of the job and not settlement of anything: it is this controller being unable
/// to see, which leaves every target exactly as pending as it was.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum TerminationUnobservable {
    /// The scheduler does not track the worker generations it starts, so its listing says
    /// nothing about whether one has terminated.
    ///
    /// `ManualScheduler` is one case — its workers are started by a person and it keeps no
    /// registry, so an empty listing from it is "I do not know" and not "they are gone" — and
    /// `KubernetesScheduler` is the other, for a reason its own answer records. Reading either
    /// as the second would settle every target of every job the moment it was asked.
    #[error("the {scheduler} scheduler cannot report a worker generation as terminated: {why}")]
    NotTracked {
        /// The scheduler that was asked.
        scheduler: &'static str,
        /// Why it cannot say.
        why: &'static str,
    },
    /// The scheduler was asked and could not answer.
    #[error("the scheduler could not list job {job_id}'s live worker generations: {report}")]
    ListingFailed {
        /// The job the listing was for.
        job_id: String,
        /// The scheduler's own report, preserved rather than replaced.
        report: String,
    },
}

/// Asks the scheduler which of `targets` are gone, and produces a witness for each that is.
///
/// The one production route to an [`ObservedTermination`]. It asks for `generation`
/// specifically rather than for the job as a whole, because a job whose replacement generation
/// is live and whose predecessor is gone must produce terminations for the predecessor and none
/// for the successor — a job-wide listing would answer about both at once.
///
/// # Errors
///
/// [`TerminationUnobservable`] when the scheduler cannot say. Nothing is settled then, and the
/// caller leaves every target pending: not knowing is not the same as knowing they are gone,
/// and this is the one place in the fencing path where those could be confused.
pub(crate) async fn observe_terminations(
    scheduler: &Arc<dyn Scheduler>,
    job_id: &str,
    generation: u64,
    targets: &[WorkerId],
) -> Result<Vec<ObservedTermination>, TerminationUnobservable> {
    // One question, so the authority and the listing cannot come from different states of the
    // world: a scheduler answers `Live` only for a generation it can vouch for, and the listing
    // it vouches for arrives inside that answer (PR #167 round 2).
    let observation = scheduler
        .observe_generation(job_id, generation)
        .await
        .map_err(|e| TerminationUnobservable::ListingFailed {
            job_id: job_id.to_string(),
            report: format!("{e:?}"),
        })?;
    let live: BTreeSet<u64> = match observation {
        GenerationObservation::Live(workers) => {
            workers.into_iter().map(|worker| worker.0).collect()
        }
        GenerationObservation::Untracked { scheduler, why } => {
            return Err(TerminationUnobservable::NotTracked { scheduler, why });
        }
    };
    Ok(targets
        .iter()
        .filter(|worker| !live.contains(&worker.0))
        .map(|worker| ObservedTermination {
            worker: *worker,
            generation,
        })
        .collect())
}

/// One pending target, taken out of the record so the pass can advance the record while it
/// works through them.
///
/// Owned rather than borrowed for a plain borrow-checker reason and one design reason: the pass
/// records each acknowledgement against the obligation as it arrives, so it cannot be holding a
/// reference into it, and a target's identity — the worker and the address it is reached at —
/// is exactly what the advance needs and all of it.
pub(super) struct PendingTarget {
    pub(super) worker: WorkerId,
    pub(super) rpc_address: Option<String>,
    /// The process the obligation is owed by, as the record names it.
    ///
    /// Carried beside the address because an advance has to be addressed to a *process*: a
    /// worker id and a generation name a slot, and the successor of a restart holds none of its
    /// predecessor's fence state (M11.D39d, PR #167 round 6). `None` is a record written before
    /// the field, whose targets can be observed terminated but not fenced.
    pub(super) incarnation: Option<WorkerIncarnation>,
}

/// A job's fencing obligation, as read back from its row and advanced by one recovery pass.
///
/// It is a separate value from M11.T25's in-memory `Fencing` on purpose. That one is *this*
/// attempt's obligation, measured against the fence this attempt addressed its workers under;
/// this one is a **previous** attempt's, measured against the fence the row carried before this
/// controller adopted it. Collapsing them would make one of the two comparisons wrong, and the
/// wrong one settles a target nothing has answered for.
pub(crate) struct RecoveredObligation {
    targets: Vec<FenceTarget>,
    candidate_root: Option<String>,
    /// Carried forward on every republication, which is what makes the age this obligation
    /// reports the *job's* age rather than this process's.
    since_millis: Option<u64>,
    /// The highest fence anything in this record could have been issued under.
    ///
    /// Derived rather than persisted: adoption stores `lifecycle_fence + 1` over the value it
    /// read, so the fence the row carried *before* this controller's adoption is an upper bound
    /// on the fence any previous controller issued under. An acknowledgement above it therefore
    /// supersedes everything here, and one at or below it supersedes nothing — the same rule
    /// M11.T25's `Fencing` applies to its own attempt, applied to the fence this one belongs to.
    issued_under_at_most: u64,
}

impl RecoveredObligation {
    /// The obligation `record` describes, to be discharged under `adopted_fence`.
    ///
    /// `adopted_fence` is the fence this controller's adoption *installed*; the bound this
    /// obligation is measured against is one below it. Taking the installed fence rather than
    /// the bound is deliberate: the caller has the first from its own adoption and would have
    /// to compute the second, and a caller that computed it wrongly would settle targets on the
    /// strength of an acknowledgement that revoked nothing.
    pub(crate) fn of(record: &Fencing, adopted_fence: u64) -> Self {
        Self {
            targets: record.targets().to_vec(),
            candidate_root: record.candidate_root().map(str::to_string),
            since_millis: record.fencing_since_millis(),
            issued_under_at_most: adopted_fence.saturating_sub(1),
        }
    }

    /// How many targets have neither acknowledged nor been observed terminated.
    pub(crate) fn pending(&self) -> usize {
        self.pending_targets().count()
    }

    /// How many issued identifiers belong to a target that is still pending.
    ///
    /// A pending target that was never issued a start owes an acknowledgement and no
    /// identifier; both are counted, separately, because they are what an operator needs to
    /// tell "a generation we must fence" from "a request a worker may still apply".
    pub(crate) fn outstanding_attempts(&self) -> usize {
        self.pending_targets()
            .filter(|target| target.attempt_id.is_some())
            .count()
    }

    /// How long this obligation has been standing, if it recorded when it began.
    pub(crate) fn age(&self) -> Option<std::time::Duration> {
        metrics::age_of(self.since_millis)
    }

    /// The pending targets of each generation this obligation names.
    ///
    /// Grouped because a fence is advanced *per generation*: M11.D39d allows a job to owe two
    /// at once — the interrupted attempt's and a takeover's — and a directive addressed to one
    /// is refused by the other.
    pub(super) fn pending_by_generation(&self) -> BTreeMap<u64, Vec<PendingTarget>> {
        let mut by_generation: BTreeMap<u64, Vec<PendingTarget>> = BTreeMap::new();
        for target in self.pending_targets() {
            by_generation
                .entry(target.generation)
                .or_default()
                .push(PendingTarget {
                    worker: WorkerId(target.worker_id),
                    rpc_address: target.rpc_address.clone(),
                    incarnation: target.incarnation.map(WorkerIncarnation::from),
                });
        }
        by_generation
    }

    fn pending_targets(&self) -> impl Iterator<Item = &FenceTarget> {
        self.targets
            .iter()
            .filter(|target| target.state == FenceTargetState::Pending)
    }

    /// Records an acknowledgement against this obligation, if it supersedes what was issued.
    ///
    /// Returns whether anything changed, so a pass can be run twice and say the same thing the
    /// second time. Monotone: a target only ever leaves `Pending`, so replaying an
    /// acknowledgement is not a second event.
    pub(super) fn acknowledge(&mut self, acknowledgement: &FenceAcknowledgement) -> bool {
        if !acknowledgement.supersedes(self.issued_under_at_most) {
            info!(
                worker_id = acknowledgement.worker().0,
                observed_fence = acknowledgement.observed_fence(),
                issued_under_at_most = self.issued_under_at_most,
                "a worker generation acknowledged a fence that does not supersede what this \
                 recovered obligation issued, so it settles nothing"
            );
            return false;
        }
        self.advance(
            acknowledgement.worker(),
            acknowledgement.generation(),
            FenceTargetState::Acknowledged,
        )
    }

    /// Records an observed termination against this obligation.
    ///
    /// A generation that no longer exists cannot apply what was addressed to it, whatever fence
    /// it last held — so unlike an acknowledgement there is no height to compare. It supersedes
    /// an acknowledgement, which is the same precedence M11.T25's `FenceTargets::terminate`
    /// has: a target that acknowledged and then went away is gone.
    pub(super) fn terminate(&mut self, termination: &ObservedTermination) -> bool {
        self.advance(
            termination.worker(),
            termination.generation(),
            FenceTargetState::Terminated,
        )
    }

    /// Moves one target to `state`, if this obligation names it and it is not already there.
    ///
    /// The worker **and** the generation must both match: a reused endpoint is a different
    /// target, and an answer from the successor must not settle the predecessor's obligation
    /// (M11.D39d).
    fn advance(&mut self, worker: WorkerId, generation: u64, state: FenceTargetState) -> bool {
        let Some(target) = self
            .targets
            .iter_mut()
            .find(|target| target.worker_id == worker.0 && target.generation == generation)
        else {
            return false;
        };
        if target.state == state || target.state == FenceTargetState::Terminated {
            return false;
        }
        target.state = state;
        true
    }

    /// The record this obligation would be republished as, or `None` if it names nothing.
    ///
    /// # Why a settled record is kept and not deleted
    ///
    /// A record says two things, and they stop being true at different moments (PR #167
    /// rounds 5 and 6). The **identifiers** stop being owed the instant every target settles.
    /// The **targets** — which worker generations can act, and where they answer — stay true for
    /// as long as those workers run, and that is what a controller about to publish a refusal, or
    /// to admit a replacement generation, has to fence.
    ///
    /// So settling advances the states and keeps the record. Deleting it here was how round 5's
    /// retained live-generation inventory was lost: a recovery pass — the cold `LeaderRunning`
    /// adoption and the scheduling preamble both run this one — converted an all-acknowledged
    /// record to `None` and persisted the deletion, leaving nothing for a later refusal to fence
    /// *at*. Keeping it holds the job in `Fencing` for nothing, because a job is held by
    /// **pending** targets and this record has none.
    ///
    /// `None` is now only "this obligation names no target at all", which is a record with
    /// nothing in it whatever its candidate says: a candidate is not an obligation to a worker,
    /// it is an object the grace collector reclaims from the job's own `generations/` prefix, and
    /// no acknowledgement can settle one. The caller logs the key on the way out so it is not
    /// lost from the record silently.
    ///
    /// # Errors
    ///
    /// [`FencingRecordError`] if the advanced obligation no longer describes a writable record.
    /// It cannot, in this build — advancing changes only a target's state — and it is a
    /// `Result` rather than an `expect` because "it cannot" is an argument about today's
    /// operations rather than a property of the type.
    pub(super) fn into_record(self) -> Result<Option<Fencing>, FencingRecordError> {
        if self.targets.is_empty() {
            return Ok(None);
        }
        // Settled targets keep the record; only the identifiers they were issued go, because an
        // identifier a later pass could revoke is one this pass has already had answered.
        let targets = self
            .targets
            .into_iter()
            .map(|target| match target.state {
                FenceTargetState::Pending => target,
                _ => FenceTarget {
                    attempt_id: None,
                    ..target
                },
            })
            .collect();
        Ok(Some(Fencing::record(
            targets,
            self.candidate_root,
            self.since_millis,
        )?))
    }

    /// The candidate object the interrupted attempt left unrooted, if it left one.
    pub(super) fn candidate_root(&self) -> Option<&str> {
        self.candidate_root.as_deref()
    }
}

// ---------------------------------------------------------------------------------------
// Test-only construction of the witnesses this module mints.
//
// Declared below the whole production half, for the reason `scheduling/fanout.rs` records: a
// `#[cfg(test)]` placed higher truncates any source pin that cuts a file at its first one.
// ---------------------------------------------------------------------------------------

#[cfg(test)]
impl ObservedTermination {
    /// A termination assembled from loose values, for a row that needs to state one.
    ///
    /// Test-only for the reason the type exists: the production route is a scheduler listing,
    /// and a build that could name a termination for an arbitrary generation could settle a
    /// target nothing observed. The same allowance `LifecycleAuthority::from_parts` and
    /// `FenceAcknowledgement::reported` carry, for the same reason.
    pub(crate) fn observed(worker: WorkerId, generation: u64) -> Self {
        Self { worker, generation }
    }
}

#[cfg(test)]
impl RecoveredObligation {
    /// What each target of this obligation has done about the fence, for a row that asserts the
    /// advance in closed form.
    pub(crate) fn states(&self) -> Vec<(u64, u64, FenceTargetState)> {
        self.targets
            .iter()
            .map(|target| (target.worker_id, target.generation, target.state))
            .collect()
    }

    /// The record this obligation would be republished as.
    pub(crate) fn record(self) -> Option<Fencing> {
        self.into_record()
            .expect("advancing a target's state cannot break a rule the record is under")
    }

    /// Records an acknowledgement, for a row that drives the advance without a worker.
    pub(crate) fn observe_acknowledgement(&mut self, ack: &FenceAcknowledgement) -> bool {
        self.acknowledge(ack)
    }

    /// Records a termination, for the same reason.
    pub(crate) fn observe_termination(&mut self, termination: &ObservedTermination) -> bool {
        self.terminate(termination)
    }
}
