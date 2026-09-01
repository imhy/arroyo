//! The one place a job's lifecycle status reaches its row (M11.T26b/M11.T26h, design M11.D39d).
//!
//! M11.D39d makes every lifecycle, status and generation write conditional on the job's
//! durable authority — its id, its `lifecycle_fence` and its `controller_epoch` — so that two
//! controller processes cannot both publish one job's status.
//!
//! M11.T26b landed both write forms on [`JobStatus`]: the conditional
//! [`update_db_under_authority`](JobStatus::update_db_under_authority) and the landed
//! unconditional `update_db` beside it, and this module chose between them by mode. **M11.T26h
//! removed the choice along with the unconditional write**: there is one write form now, and
//! this module is what every publishing state reaches it through.
//!
//! # Why one funnel and not five call sites
//!
//! Five states publish a status: the state-machine boundary in `states/mod.rs`, the landed
//! `Scheduling::next`, the M11.D39b scheduling preamble, `Running` and `LeaderRunning`. If each
//! wrote its own statement, activating the fence would have been five changes that could land
//! apart, and a state added later would inherit whichever form its author copied. With one
//! funnel there was exactly one thing to change at M11.T26h, and a new publishing state has no
//! other way to write a status —
//! `the_production_status_write_is_conditional_since_the_activation_change` is what counts
//! that.
//!
//! # Three answers, because a caller must be able to tell them apart
//!
//! A write that touched no row means two different things. Unconditionally, it meant the job's
//! row was gone. Conditionally, it means **another controller holds this job** — and those
//! need opposite responses: the first is a failure to report, and the second is a signal to
//! stop administering the job at once, before the next effect. Collapsing them into one
//! `Result` would make the second reachable only by reading an error message. So
//! [`StatusPublication::Superseded`] is a value of its own, and [`stand_down`] is what every
//! caller does with it.

use cornucopia_async::DatabaseSource;
use tracing::error;

use super::fence::{AuthorityOutcome, AuthorityWriteError, StaleAuthority};
use crate::JobStatus;

/// What publishing a job's status did.
///
/// `#[must_use]` for the same reason [`AuthorityOutcome`] is: the one way to get this wrong is
/// to ignore it and carry on as though the row had been written.
#[derive(Debug)]
#[must_use = "a status publication may have been refused by another controller's authority; \
              handle the superseded outcome"]
pub(crate) enum StatusPublication {
    /// The row accepted the write.
    Published,
    /// The conditional write matched no row: another controller holds this job's durable
    /// lifecycle authority.
    Superseded(StaleAuthority),
}

/// Publishes `status` to its row, under the job's durable lifecycle authority.
///
/// There is one write form and this is the only place it is performed, which is what makes
/// "the status write is conditional" a property of the crate rather than of five states that
/// each remembered.
///
/// # Errors
///
/// [`AuthorityWriteError`] when the write could not be performed at all. Zero updated rows is
/// *not* an error: it is [`StatusPublication::Superseded`], and it means another controller
/// holds this job.
pub(crate) async fn publish_status(
    status: &JobStatus,
    database: &DatabaseSource,
) -> Result<StatusPublication, AuthorityWriteError> {
    match status.update_db_under_authority(database).await {
        Ok(AuthorityOutcome::Applied(())) => Ok(StatusPublication::Published),
        Ok(AuthorityOutcome::Stale(stale)) => Ok(StatusPublication::Superseded(stale)),
        Err(error) => Err(error),
    }
}

/// What a controller does when it learns it no longer holds a job: it stops administering it.
///
/// One function, because every publishing state must do the same thing and because "the same
/// thing" is not obvious. It is deliberately **not** an error:
///
/// * Retrying is the unsafe answer — a superseded controller that retries is a controller
///   trying to overwrite a live one, forever.
/// * Failing the job is worse: the job is not failing, it is being run by somebody else, and
///   this process would be publishing that opinion about a job it has already lost. (It would
///   also be publishing it through the same conditional write, which would refuse it — so the
///   only thing a fatal error achieves is a log line claiming a failure that never reached the
///   row.)
///
/// So the job's state task ends. Every caller ends it the way its own position ends things — a
/// state body returns `Transition::Stop`, and the state-machine boundary answers `None` —
/// and both are the same decision: `execute_state` maps one to the other, the loop breaks, and
/// this process writes nothing further about the job. The controller that holds it carries on;
/// a durable obligation this attempt left behind is in the job's row, not in this process.
///
/// What this function itself does is say so, once, where an operator will read it.
/// `the_production_status_write_is_conditional_since_the_activation_change` is what keeps
/// every publishing state doing the same thing.
pub(crate) fn stand_down(stale: StaleAuthority) {
    error!(
        job_id = %stale.job_id,
        operation = stale.operation,
        presented_fence = %stale.presented_fence,
        presented_epoch = %stale.presented_epoch,
        "standing down: another controller holds this job's durable lifecycle authority, so \
         this controller publishes nothing further for it"
    );
}
