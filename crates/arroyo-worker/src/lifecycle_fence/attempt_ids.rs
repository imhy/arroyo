//! The bounded record of which start attempts one worker generation applied and which were
//! revoked (M11.T26d, design M11.D39e(v)).
//!
//! M11.D39e(v) requires the worker to keep "a bounded per-generation set of applied/revoked IDs
//! plus the highest acknowledged fence", with a capacity derived from what the controller may
//! issue, overflow that fails closed, and no eviction of a live generation's identifier. This
//! module is the first half of that; [`super::guard`] holds the fence.
//!
//! # Why there is no eviction, rather than a rule against evicting one
//!
//! A worker process *is* one generation: its identity is fixed when the process is built and
//! nothing here is reachable from a request addressed to another generation, because the guard
//! refuses those before it gets this far. Every identifier this record holds therefore belongs
//! to the live generation, so "never evict an ID belonging to a live generation" is not a rule
//! a caller has to observe — it is the absence of any operation that removes one. The record
//! only grows, and it refuses to grow past [`MAX_TRACKED_ATTEMPT_IDS`].

use arroyo_rpc::fencing::{MAX_ATTEMPT_ID_CHARS, MAX_FENCE_TARGETS};
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};

/// How many `StartExecution` requests one worker generation can have applied: one.
///
/// The execution phase admits a start only from `Idle`, and the identifier this record keeps is
/// what refuses the next attempt once the phase has returned to `Idle` after `JobFinished`. So a
/// generation carries at most one applied identifier for its whole life, which is why
/// [`AttemptIds`] holds it in an `Option` instead of a second set: "at most one" is then the
/// shape of the field rather than an invariant something has to check.
const APPLIED_ATTEMPTS_PER_GENERATION: usize = 1;

/// How many attempt identifiers one worker generation's record may hold.
///
/// # Where the number bottoms out
///
/// In [`MAX_FENCE_TARGETS`], which is the controller's ceiling on how many `start_execution_id`s
/// it can have outstanding for one job at one moment. The controller's fan-out ledger
/// (`IssuedAttempts`) keys its records by `WorkerId` and *overwrites* on replay, so the
/// `START_EXECUTION_RECONCILE_ATTEMPTS` ambiguous-transport retries an attempt may spend on one
/// target all carry the identifier that target was minted and cost no further entries: one
/// target is one identifier, and the fan-out's issued-attempt bound is therefore the same number
/// as its target bound. Every identifier a directive can name for revocation is one of those,
/// and `arroyo_rpc::fence_wire` already refuses a directive naming more than
/// [`MAX_FENCE_TARGETS`] of them — so this capacity admits every well-formed directive whole.
/// [`APPLIED_ATTEMPTS_PER_GENERATION`] is added for the identifier this generation may itself
/// have applied, which the controller need not have named for revocation.
///
/// `MAX_FENCE_TARGETS` is 32 default-admitted workers per job × 2 addressable worker generations
/// × 32 headroom, and **its third factor is stated rather than derived**: `max_parallelism` is
/// per-organization configuration and arroyo's own cloud profile sets it to `u32::MAX`, so there
/// is no compile-time worker count to derive from. This capacity inherits that residual instead
/// of hiding it inside a round number of its own.
///
/// Overflow is a refusal, not an eviction and not a panic: a job whose controller genuinely
/// addresses more identifiers than this fails closed at the worker, which is the same trade the
/// durable record makes.
pub(crate) const MAX_TRACKED_ATTEMPT_IDS: usize =
    MAX_FENCE_TARGETS + APPLIED_ATTEMPTS_PER_GENERATION;

/// The capacity must admit one whole well-formed directive plus this generation's own applied
/// identifier. A capacity below that would fail closed on input the controller is entitled to
/// send, so lowering either constant has to break the build rather than a deployment.
const _: () = const {
    assert!(MAX_TRACKED_ATTEMPT_IDS >= MAX_FENCE_TARGETS + APPLIED_ATTEMPTS_PER_GENERATION)
};

/// What this generation has done about one `start_execution_id`.
///
/// The three values are exclusive by construction: an identifier is recorded applied only when
/// it is not revoked, and [`AttemptIds::record`] refuses to revoke the applied one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AttemptDisposition {
    /// This generation has neither applied nor revoked it.
    Unknown,
    /// This generation applied it; a delayed duplicate is acknowledged, not replayed.
    Applied,
    /// It is permanently non-applicable for this generation.
    Revoked,
}

/// Why a record refused to take an identifier.
///
/// Each variant is a refusal of input this process did not write — a directive decoded off the
/// wire — so none of them is a programming error to be asserted away, and none is recoverable by
/// guessing which half the sender meant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AttemptIdRefusal {
    /// An identifier that is longer than one the controller can mint.
    ///
    /// The record is bounded in bytes as well as in count; an identifier it cannot bound is one
    /// it must not store. `arroyo_rpc::fence_wire` applies the same bound to a revocation list,
    /// but nothing bounds `start_execution_id` before it arrives here.
    MalformedId {
        /// The identifier's length in characters.
        found: usize,
    },
    /// Recording these identifiers would take the record past its capacity.
    Overflow {
        /// How many identifiers the record already holds.
        held: usize,
        /// How many more this call would add.
        added: usize,
    },
    /// A second, different execution would be applied by a generation that already applied one.
    AlreadyApplied {
        /// The identifier this generation applied.
        held: String,
    },
    /// A revocation names the identifier this generation has already applied.
    ///
    /// It cannot be made non-applicable: it was applied. Saying otherwise would report
    /// "nothing applied" for an execution that is running, so the whole directive is refused and
    /// the controller's remaining route to settlement is observed generation termination
    /// (design §3C, "teardown if applied or unacknowledged").
    RevokesApplied {
        /// The identifier named.
        id: String,
    },
}

impl Display for AttemptIdRefusal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AttemptIdRefusal::MalformedId { found } => write!(
                f,
                "execution identifier is {found} characters, which is not between 1 and \
                 {MAX_ATTEMPT_ID_CHARS}"
            ),
            AttemptIdRefusal::Overflow { held, added } => write!(
                f,
                "this worker generation already tracks {held} execution identifiers and cannot \
                 take {added} more without exceeding {MAX_TRACKED_ATTEMPT_IDS}"
            ),
            AttemptIdRefusal::AlreadyApplied { held } => {
                write!(f, "this worker generation already applied execution {held}")
            }
            AttemptIdRefusal::RevokesApplied { id } => write!(
                f,
                "execution {id} was applied by this worker generation and cannot be revoked"
            ),
        }
    }
}

/// One worker generation's applied and revoked `start_execution_id`s.
///
/// The fields are private and there is exactly one operation that changes them, so the rules in
/// [`AttemptIdRefusal`] hold for every value that exists rather than for the callers that
/// remembered them.
#[derive(Debug, Default)]
pub(crate) struct AttemptIds {
    applied: Option<String>,
    revoked: BTreeSet<String>,
}

impl AttemptIds {
    /// What this generation has done about `id`.
    pub(crate) fn disposition(&self, id: &str) -> AttemptDisposition {
        if self.revoked.contains(id) {
            AttemptDisposition::Revoked
        } else if self.applied.as_deref() == Some(id) {
            AttemptDisposition::Applied
        } else {
            AttemptDisposition::Unknown
        }
    }

    /// How many identifiers the record holds.
    pub(crate) fn len(&self) -> usize {
        self.revoked.len() + usize::from(self.applied.is_some())
    }

    /// Revokes every identifier in `revoke` and, if `apply` is `Some`, records it applied.
    ///
    /// This is the only operation that changes the record, and it is all-or-nothing: every rule
    /// is checked against the identifiers the call names before any of them is stored, so a
    /// refusal leaves the record exactly as it found it. That is what lets the caller run it as
    /// the first step of a commit and treat everything after it as infallible.
    ///
    /// Idempotent in both arguments: re-revoking a revoked identifier and re-applying the
    /// applied one change nothing and consume no capacity, which is what makes a duplicated or
    /// re-delivered directive safe to answer twice.
    ///
    /// # Errors
    ///
    /// Every variant of [`AttemptIdRefusal`].
    pub(crate) fn record(
        &mut self,
        revoke: &[String],
        apply: Option<&str>,
    ) -> Result<(), AttemptIdRefusal> {
        for id in revoke.iter().map(String::as_str).chain(apply) {
            let found = id.chars().count();
            if found == 0 || found > MAX_ATTEMPT_ID_CHARS {
                return Err(AttemptIdRefusal::MalformedId { found });
            }
        }

        if let Some(id) = apply
            && let Some(held) = self.applied.as_deref()
            && held != id
        {
            return Err(AttemptIdRefusal::AlreadyApplied {
                held: held.to_string(),
            });
        }

        // A revocation may not name an identifier this generation applied, nor the one this very
        // directive would apply: both would report a running execution as non-applicable.
        let applied_after = apply.or(self.applied.as_deref());
        if let Some(held) = applied_after
            && let Some(named) = revoke.iter().find(|id| id.as_str() == held)
        {
            return Err(AttemptIdRefusal::RevokesApplied { id: named.clone() });
        }

        let additions: BTreeSet<&str> = revoke
            .iter()
            .map(String::as_str)
            .filter(|id| !self.revoked.contains(*id))
            .collect();
        let added = additions.len() + usize::from(apply.is_some() && self.applied.is_none());
        let held = self.len();
        if held + added > MAX_TRACKED_ATTEMPT_IDS {
            return Err(AttemptIdRefusal::Overflow { held, added });
        }

        // Every rule has passed; nothing below can fail.
        for id in additions {
            self.revoked.insert(id.to_string());
        }
        if let Some(id) = apply {
            self.applied = Some(id.to_string());
        }
        Ok(())
    }

    /// The identifier this generation applied, if it applied one.
    #[cfg(test)]
    pub(crate) fn applied(&self) -> Option<&str> {
        self.applied.as_deref()
    }
}
