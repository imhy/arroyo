//! What an operator sees while a job is fencing (M11.T26f, design M11.D39g, plan M11.T26n).
//!
//! M11.D39g chooses safety over per-job availability: a job whose target worker generation is
//! partitioned stays in token-free `Fencing` and `Refused` is never published for it. That is a
//! deliberate, unbounded wait, and the price of choosing it is that the wait must be **visible**
//! — otherwise the design's answer to a partition is indistinguishable from a controller that
//! has silently stopped working on a job.
//!
//! So this module publishes four numbers and one alert, per job:
//!
//! * how long the job has been fencing, measured from the durable origin so it survives the
//!   controller restarts that a crash-looping deployment produces;
//! * how many target generations are still pending;
//! * how many issued identifiers are still unaccounted for;
//! * how many settlements each of M11.D39e(v)'s facts has produced, and how many errors of each
//!   kind the fencing path has hit; and
//! * whether the operator-visible alert is raised.
//!
//! # The gauge is the alert's state
//!
//! There is no second registry of "which jobs are alerting". [`alert_pending`] reads the gauge
//! it is about to write, so raising, sustaining and clearing are decided by the same value an
//! operator's dashboard is reading. A private `HashSet` beside it could disagree with what was
//! published, and the disagreement would be invisible in exactly the situation the alert exists
//! for.
//!
//! # What this module never does
//!
//! It never settles anything, and it has nothing to settle with. A metric is an observation
//! *about* an obligation; M11.D39e(v)'s three facts are the only things that discharge one, and
//! none of them is expressible here. In particular, nothing here reads the age and concludes
//! anything: [`AlertTransition`] is a description of what was published, not a decision about
//! the job.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lazy_static::lazy_static;
use prometheus::{IntCounterVec, IntGaugeVec, register_int_counter_vec, register_int_gauge_vec};
use tracing::{error, info, warn};

use crate::states::scheduling::fanout::Accounting;

lazy_static! {
    /// How long each fencing job has been fencing, in seconds.
    static ref FENCING_AGE: IntGaugeVec = register_int_gauge_vec!(
        "arroyo_controller_job_fencing_age_seconds",
        "How long a job has been in token-free Fencing, from its durable fencing origin",
        &["job_id"]
    )
    .unwrap();
    /// Target worker generations that have neither acknowledged nor been observed terminated.
    static ref FENCING_PENDING_TARGETS: IntGaugeVec = register_int_gauge_vec!(
        "arroyo_controller_job_fencing_pending_targets",
        "Target worker generations a fencing job still owes an acknowledgement from",
        &["job_id"]
    )
    .unwrap();
    /// Issued `StartExecution` identifiers with no authoritative outcome.
    static ref FENCING_OUTSTANDING_ATTEMPTS: IntGaugeVec = register_int_gauge_vec!(
        "arroyo_controller_job_fencing_outstanding_attempts",
        "Issued StartExecution identifiers a fencing job cannot yet account for",
        &["job_id"]
    )
    .unwrap();
    /// Settlements, by which of M11.D39e(v)'s facts produced them.
    static ref FENCING_SETTLEMENTS: IntCounterVec = register_int_counter_vec!(
        "arroyo_controller_job_fencing_settlements_total",
        "Fencing targets settled, by the observed fact that settled them",
        &["job_id", "accounted_by"]
    )
    .unwrap();
    /// Errors the fencing path hit, by kind.
    static ref FENCING_ERRORS: IntCounterVec = register_int_counter_vec!(
        "arroyo_controller_job_fencing_errors_total",
        "Errors encountered while discharging a job's fencing obligation, by kind",
        &["job_id", "kind"]
    )
    .unwrap();
    /// 1 while a job's fencing obligation is unsettled after an active advance, 0 otherwise.
    static ref FENCING_ALERT: IntGaugeVec = register_int_gauge_vec!(
        "arroyo_controller_job_fencing_alert",
        "1 while a job is held in Fencing by a target generation that cannot be observed",
        &["job_id"]
    )
    .unwrap();
}

/// Which error a fencing pass hit.
///
/// A closed list with no catch-all, so an error kind added later is a decision rather than an
/// entry in an `"other"` bucket an operator cannot act on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FencingError {
    /// The obligation could not be described durably at all: it broke a rule the record is
    /// under, or its parts disagreed about which generation they belonged to.
    Unrecordable,
    /// A target generation did not acknowledge the fence this controller advanced to it.
    NotAcknowledged,
    /// The scheduler could not say which of the job's worker generations are still live, so no
    /// termination could be observed.
    TerminationUnobservable,
    /// The obligation was described but could not be written to the job's row.
    PublicationFailed,
}

impl FencingError {
    /// The label an operator groups by.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            FencingError::Unrecordable => "unrecordable",
            FencingError::NotAcknowledged => "not_acknowledged",
            FencingError::TerminationUnobservable => "termination_unobservable",
            FencingError::PublicationFailed => "publication_failed",
        }
    }
}

/// What one pass did to a job's operator-visible fencing alert.
///
/// Returned rather than only logged so that the alert's whole lifecycle — raised, sustained,
/// cleared, and quiet for a job that was never alerting — is a value a test can assert. An
/// alert nobody can observe the transitions of is an alert whose clearing nobody can prove.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "the alert transition is what says whether an operator was told anything"]
pub(crate) enum AlertTransition {
    /// The job was not alerting and now is.
    Raised,
    /// The job was already alerting and still is.
    Sustained,
    /// The job was alerting and is not any more.
    Cleared,
    /// The job was not alerting and still is not.
    Quiet,
}

/// The state of one job's fencing obligation, as the metrics report it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FencingReport {
    /// Target generations that have neither acknowledged nor been observed terminated.
    pub(crate) pending_targets: usize,
    /// Issued identifiers with no authoritative outcome.
    pub(crate) outstanding_attempts: usize,
    /// How long the job has been fencing, or `None` when the record carries no origin.
    pub(crate) age: Option<Duration>,
}

/// Publishes an obligation that is **still pending**, and raises or sustains the alert.
///
/// This is the M11.D39g liveness result made visible: the job stays in `Fencing`, and an
/// operator is told which job, how many targets, how many identifiers and for how long. It
/// makes no decision about the job — see the module documentation on why an age here can never
/// become a timeout.
pub(crate) fn alert_pending(job_id: &str, report: FencingReport) -> AlertTransition {
    publish(job_id, report);
    let transition = match FENCING_ALERT.with_label_values(&[job_id]).get() {
        0 => AlertTransition::Raised,
        _ => AlertTransition::Sustained,
    };
    FENCING_ALERT.with_label_values(&[job_id]).set(1);
    match transition {
        AlertTransition::Raised => error!(
            job_id,
            pending_targets = report.pending_targets,
            outstanding_attempts = report.outstanding_attempts,
            age_seconds = age_seconds(report.age),
            "this job is held in token-free Fencing by a target worker generation that has \
             neither acknowledged this controller's lifecycle fence nor been observed \
             terminated. It publishes no refusal and admits no replacement generation until one \
             of those is observed; other jobs are unaffected. This is M11.D39g's declared \
             choice of safety over this job's availability, and it will not time out"
        ),
        _ => warn!(
            job_id,
            pending_targets = report.pending_targets,
            outstanding_attempts = report.outstanding_attempts,
            age_seconds = age_seconds(report.age),
            "this job is still held in token-free Fencing"
        ),
    }
    transition
}

/// Publishes an obligation that is **settled**, and clears the alert if one was raised.
pub(crate) fn alert_settled(job_id: &str) -> AlertTransition {
    publish(
        job_id,
        FencingReport {
            pending_targets: 0,
            outstanding_attempts: 0,
            age: None,
        },
    );
    let transition = match FENCING_ALERT.with_label_values(&[job_id]).get() {
        0 => AlertTransition::Quiet,
        _ => AlertTransition::Cleared,
    };
    FENCING_ALERT.with_label_values(&[job_id]).set(0);
    if transition == AlertTransition::Cleared {
        info!(
            job_id,
            "every target worker generation this job owed an acknowledgement to has \
             acknowledged a superseding fence or been observed terminated; the job is no longer \
             held in Fencing"
        );
    }
    transition
}

/// Records that one of M11.D39e(v)'s facts settled a target.
pub(crate) fn record_settlement(job_id: &str, accounting: Accounting) {
    FENCING_SETTLEMENTS
        .with_label_values(&[job_id, accounting.as_str()])
        .inc();
}

/// Records an error the fencing path hit.
pub(crate) fn record_error(job_id: &str, kind: FencingError) {
    FENCING_ERRORS
        .with_label_values(&[job_id, kind.as_str()])
        .inc();
}

/// The three gauges, written together so a dashboard never shows two of them from one pass and
/// the third from the pass before.
fn publish(job_id: &str, report: FencingReport) {
    FENCING_PENDING_TARGETS
        .with_label_values(&[job_id])
        .set(saturating(report.pending_targets));
    FENCING_OUTSTANDING_ATTEMPTS
        .with_label_values(&[job_id])
        .set(saturating(report.outstanding_attempts));
    FENCING_AGE
        .with_label_values(&[job_id])
        .set(age_seconds(report.age));
}

/// A count as a gauge value, saturating rather than wrapping.
///
/// The counts are bounded by `MAX_FENCE_TARGETS` long before this matters; saturating is what
/// keeps a nonsense input from being published as a negative number, which reads as a different
/// nonsense.
fn saturating(count: usize) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

/// An age in whole seconds, and 0 for an obligation with no recorded origin.
///
/// Zero is also what a clock that has gone backwards produces, because
/// [`SystemTime::duration_since`] is checked and the caller hands in `None` when it fails. A
/// job whose age reads zero while its pending-target gauge reads one is a job that is fencing
/// with no usable origin — which is the honest report, and is why the two are separate series.
fn age_seconds(age: Option<Duration>) -> i64 {
    age.map(|age| saturating(age.as_secs() as usize))
        .unwrap_or(0)
}

/// Now, in milliseconds since the Unix epoch, for a record that is beginning its obligation.
///
/// `None` when the host's clock is before the epoch, which is not a case to guess at: a record
/// with no origin reports no age, and reporting an age of zero for a job that has been fencing
/// for a week is the failure worth avoiding.
pub(crate) fn now_millis() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since| u64::try_from(since.as_millis()).ok())
        .filter(|millis| *millis != 0)
}

/// How long an obligation whose origin is `since_millis` has been standing.
///
/// `None` when there is no origin, and when the origin is in the future — a clock that
/// disagrees with the one that wrote the record produces the second, and a negative age is not
/// a number to publish.
pub(crate) fn age_of(since_millis: Option<u64>) -> Option<Duration> {
    let since = since_millis?;
    let now = now_millis()?;
    now.checked_sub(since).map(Duration::from_millis)
}

// ---------------------------------------------------------------------------------------
// Test-only reads of what was published.
//
// Declared below the whole production half, for the reason `scheduling/fanout.rs` records: a
// `#[cfg(test)]` placed higher truncates any source pin that cuts a file at its first one.
// ---------------------------------------------------------------------------------------

/// What the six series say about one job, for a row that has to assert them.
#[cfg(test)]
pub(crate) fn published(job_id: &str) -> (i64, i64, i64, i64) {
    (
        FENCING_PENDING_TARGETS.with_label_values(&[job_id]).get(),
        FENCING_OUTSTANDING_ATTEMPTS
            .with_label_values(&[job_id])
            .get(),
        FENCING_AGE.with_label_values(&[job_id]).get(),
        FENCING_ALERT.with_label_values(&[job_id]).get(),
    )
}

/// How many settlements of `accounting` were recorded for `job_id`.
#[cfg(test)]
pub(crate) fn settlements(job_id: &str, accounting: Accounting) -> u64 {
    FENCING_SETTLEMENTS
        .with_label_values(&[job_id, accounting.as_str()])
        .get()
}

/// How many errors of `kind` were recorded for `job_id`.
#[cfg(test)]
pub(crate) fn errors(job_id: &str, kind: FencingError) -> u64 {
    FENCING_ERRORS
        .with_label_values(&[job_id, kind.as_str()])
        .get()
}
