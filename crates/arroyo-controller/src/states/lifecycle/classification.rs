//! Selector immutability and fail-closed classification (M11.T25e, design M11.D39f).
//!
//! A job's state backend is fixed at its first execution and never changes afterwards.
//! Everything that follows from that — which of two disagreeing values is the job's
//! authority, what a value nobody recognizes means, and what a durable record that will not
//! decode means — is decided here, as pure functions over values that have already been
//! read.
//!
//! # Why the rules live in the lifecycle boundary
//!
//! M11.T08 landed these rules inside the configuration poll, beside the database row they
//! interpret. Reading the row there is right; *deciding* there is what M11.D39a takes away.
//! Under that design the job's own state task is the only component that decides and
//! publishes a lifecycle transition, so a rule expressed only on the poll thread is a rule
//! the single writer cannot reach — and a second copy of it beside the writer would be two
//! rules that can drift apart. Relocating them here leaves one expression of D39f which both
//! mechanisms reach: [`crate::classify_polled_row`] resolves each polled row through
//! [`classify_selector`] on the poll thread, and the job's writer decides on the *result*
//! rather than on the row.
//!
//! # Fail closed for the job, fail open for the cluster
//!
//! Two of the answers below are "nothing about this job can be decided". That is deliberately
//! not an error the caller propagates: the configuration poll reads every job on the cluster
//! in one pass, so returning an error for one unusable row would stop every other job from
//! being polled. The job is skipped instead — on every poll, so the condition stays visible
//! until an operator repairs it — and nothing about it is guessed at. Guessing is the failure
//! these functions exist to prevent: a default picked for a job that is still running picks
//! it for that job's workers, table configs, and checkpoints too.
//!
//! Nothing here writes, publishes, or decides anything about a job. It is called before any
//! execution baseline is replaced and before any lifecycle or status write, which is the
//! ordering D39f is about; [`LifecycleActor`](super::LifecycleActor) is what acts on the
//! answer.

use arroyo_rpc::StateContext;
use arroyo_rpc::state_backend::{
    StateBackendError, StateBackendSelector, validate_unchanged_job_selector,
};
use tracing::error;

/// What one polled configuration row means for the state backend a job runs with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SelectorClassification {
    /// The job's selector is settled, and this is it.
    ///
    /// The two fields are deliberately separate rather than a `Result`. A refused row is
    /// still *carried*: the job goes on being administered under the backend it is actually
    /// running with while the refusal travels beside it, which is what lets a stop the same
    /// row asks for still be executed — under the job's own backend, with the
    /// final-checkpoint semantics that stop asked for. Collapsing the two into one error
    /// would throw the row away and with it the operator's documented remedy.
    Fixed {
        /// The state backend this execution of the job runs with, and the only value any
        /// consumer of the row may use.
        execution_selector: StateBackendSelector,
        /// Why the row's own `state_backend` was refused, if it was: either it names a
        /// different backend than the job is running with, or it cannot be interpreted at
        /// all.
        refusal: Option<StateBackendError>,
    },
    /// Nothing about this job can be decided, so it is skipped — see
    /// [`UndecidableSelector`].
    Undecidable(UndecidableSelector),
}

/// Why a job's state backend could not be decided at all.
///
/// Both cases are unrecognized *persisted* values, and both are refused rather than
/// defaulted. They are kept apart because they say different things to an operator: one is a
/// job that is running under a backend this controller cannot name, and the other is a job
/// that has never run and cannot be started.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum UndecidableSelector {
    /// The job's durable execution record names a backend this build does not recognize.
    ///
    /// The job has an execution, so something is running or has run under a value this
    /// controller cannot interpret. Choosing one for it would be choosing for that
    /// execution's workers, table configs, and checkpoints.
    ExecutionRecord(StateBackendError),
    /// The job has never executed, and the row that would choose its backend names one this
    /// build does not recognize.
    ///
    /// A declaration that cannot be interpreted is never downgraded to a default, so the job
    /// simply never starts. There is no execution to administer and nothing to fail.
    FirstDeclaration(StateBackendError),
}

impl UndecidableSelector {
    /// Reports the condition against the job it belongs to.
    ///
    /// Logged on every poll rather than once, because the row stays unusable until an
    /// operator repairs it and a condition reported once is a condition nobody sees.
    pub(crate) fn log(&self, job_id: &str) {
        match self {
            UndecidableSelector::ExecutionRecord(e) => {
                error!(job_id = %job_id, error = %e,
                    "refusing job whose recorded execution state backend is unusable");
            }
            UndecidableSelector::FirstDeclaration(e) => {
                error!(job_id = %job_id, error = %e, "refusing job with an unusable config");
            }
        }
    }
}

/// Resolves the state backend a polled row asks for against the one the job's own execution
/// recorded.
///
/// `recorded` is the job's durable execution record, as
/// [`JobStatus::recorded_execution_selector`](crate::JobStatus::recorded_execution_selector)
/// reports it: `Ok(None)` for a job with no execution, `Ok(Some(_))` for one that has an
/// execution and therefore an authority, and `Err(_)` for a recorded value this build cannot
/// interpret. `requested` is the row's own `state_backend` column, already normalized.
///
/// # The rule, in one place
///
/// * **A recorded selector wins.** An execution exists, so it — and not the editable
///   configuration row — is what the job's workers, table configs, and checkpoints were built
///   with. A controller that re-baselined from the row after a restart would go on to
///   administer, and reconnect to, a job that is still running under something else.
/// * **A job with no execution takes the row's value.** Starting is the one moment a job
///   chooses its backend, so it is the one moment the row is the authority.
/// * **A difference is a typed refusal, not a change.** The job keeps its selector and the
///   caller is told, in a [`StateBackendError::JobSelectorChanged`] that names both values,
///   why the row was rejected.
/// * **An unrecognized persisted value is never defaulted.** It becomes
///   [`SelectorClassification::Undecidable`], which skips the job.
///
/// Note the asymmetry between the last two: a row whose value cannot be *interpreted* still
/// leaves a job that has an execution running under that execution's own backend, refusal in
/// hand. Only a job with nothing on record is skipped by it, because only then is there no
/// selector to fall back to.
pub(crate) fn classify_selector(
    job_id: &str,
    recorded: Result<Option<StateBackendSelector>, StateBackendError>,
    requested: Result<StateBackendSelector, StateBackendError>,
) -> SelectorClassification {
    // Persisted, and therefore untrusted: a recorded value nobody recognizes is refused,
    // never guessed at, because guessing is what would pick a backend for a live job.
    let recorded = match recorded {
        Ok(recorded) => recorded,
        Err(e) => {
            return SelectorClassification::Undecidable(UndecidableSelector::ExecutionRecord(e));
        }
    };

    match (recorded, requested) {
        // No execution on record: this row is the job's declaration, and starting is the
        // only moment a job chooses its backend.
        (None, Ok(requested)) => SelectorClassification::Fixed {
            execution_selector: requested,
            refusal: None,
        },
        (None, Err(e)) => {
            SelectorClassification::Undecidable(UndecidableSelector::FirstDeclaration(e))
        }
        // An execution exists, so it is the authority. The job goes on being administered
        // under the backend it is running with, and the row's value is refused.
        (Some(recorded), Ok(requested)) => SelectorClassification::Fixed {
            execution_selector: recorded,
            refusal: validate_unchanged_job_selector(job_id, recorded, requested).err(),
        },
        (Some(recorded), Err(e)) => SelectorClassification::Fixed {
            execution_selector: recorded,
            refusal: Some(e),
        },
    }
}

/// Decodes the controller's own durable record of a job's execution, failing closed.
///
/// `job_statuses.state_context` is persisted state, and therefore untrusted input. A blob
/// that cannot be decoded is *not* turned into "this job has no execution": for a job that
/// does have one, that would erase its only selector authority, and the editable
/// configuration row would be adopted in its place — the very substitution
/// [`classify_selector`] exists to prevent.
///
/// `None` therefore means *skip this job*. It is reported on every poll, so the condition
/// stays visible until an operator repairs the row, and it is scoped to the one job: the rest
/// of the cluster goes on being polled.
pub(crate) fn decode_execution_record(
    job_id: &str,
    raw: &serde_json::Value,
) -> Option<StateContext> {
    match serde_json::from_value::<StateContext>(raw.clone()) {
        Ok(state_context) => Some(state_context),
        Err(e) => {
            error!(job_id = %job_id, original =? raw, error =? e,
                "skipping job whose execution record cannot be decoded");
            None
        }
    }
}
