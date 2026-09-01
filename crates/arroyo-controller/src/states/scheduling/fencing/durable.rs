//! Making an interrupted attempt's fencing obligation durable (M11.T26f, design M11.D39d).
//!
//! A **child** of [`super`] rather than a sibling, and the split is the point rather than a size
//! measure. [`Fencing`](super::Fencing)'s methods record and never act — its inventory of
//! `pub(crate)` operations is pinned by
//! `the_source_of_fencing_exposes_no_admission_and_no_irreversible_effect`, and a method of it
//! that published anything is the change that pin exists to force a decision about. What that
//! type supplies is [`Fencing::durable_obligation`](super::Fencing::durable_obligation), a
//! read. The **write** is here, on [`Interrupted`] — the value that represents the attempt
//! *ending* — and it goes through
//! [`JobContext::publish_status`](crate::states::JobContext::publish_status) like every other
//! lifecycle write in the crate.
//!
//! Being a child is what lets it reach `Interrupted`'s private half without opening it to the
//! rest of `states::scheduling`.

use tracing::{error, info};

use super::Interrupted;
use crate::states::lifecycle::fence::metrics;

impl Interrupted<'_, '_> {
    /// Makes this attempt's obligation durable, through the one publication funnel
    /// (M11.T26f, design M11.D39d).
    ///
    /// **This is what earns the word "durable"** in M11.T26e's *"budget exhaustion transfers
    /// into durable token-free `Fencing`"*. M11.T26e deliberately did not claim it: an owner
    /// that retains the job's authority in memory is answerable only for as long as the process
    /// lives, and the record M11.T26b defined had no writer. This is the writer.
    ///
    /// It is on [`Interrupted`] and not on [`Fencing`], and the placement is the point.
    /// `Fencing`'s methods record and never act — its inventory of `pub(crate)` operations is
    /// pinned by `the_source_of_fencing_exposes_no_admission_and_no_irreversible_effect`, and a
    /// method of it that published anything is the change that pin exists to force a decision
    /// about. What `Fencing` supplies is
    /// [`durable_obligation`](Fencing::durable_obligation), a read; the write is here, on the
    /// value that represents the attempt *ending*, and it goes through
    /// [`JobContext::publish_status`](crate::states::JobContext::publish_status) like every
    /// other lifecycle write in the crate.
    ///
    /// # What it does not do
    ///
    /// It does not write when this attempt owes nothing. That is not an optimization: an
    /// attempt interrupted by a *recovered* obligation it could not discharge has itself
    /// addressed no worker, and writing its empty obligation would erase the record it was
    /// interrupted by — the one thing a later controller has to work from. So at most one
    /// obligation is live for a job at a time, and the attempt that may record one is the
    /// attempt that got past recovery.
    ///
    /// A write that fails is logged and counted and does not change how the attempt is
    /// reported: the attempt is already ending, and its reason is the one that ended it. The
    /// record stays staged on the status, so the next publication carries it.
    pub(super) async fn persist_obligation(&mut self) {
        let job_id = self.fencing.ctx.job().config.id.clone();
        if !self
            .fencing
            .ctx
            .job()
            .lifecycle_mode()
            .recovers_a_durable_fencing_obligation()
        {
            return;
        }
        // The origin the row already carries, if it carries one, so that a job which has been
        // fencing across several attempts reports the age of the *obligation* rather than of the
        // most recent attempt at it. `now` only when there is nothing to carry forward.
        let since = self
            .fencing
            .ctx
            .job()
            .status
            .recorded_fencing()
            .and_then(|record| record.fencing_since_millis())
            .or_else(metrics::now_millis);

        let obligation = match self.fencing.durable_obligation(since) {
            Ok(Some(obligation)) => obligation,
            // Nothing owed by this attempt. Whatever the row carries is left exactly as it is.
            Ok(None) => return,
            Err(refusal) => {
                metrics::record_error(&job_id, metrics::FencingError::Unrecordable);
                error!(
                    job_id = %job_id,
                    error = %refusal,
                    "this attempt's fencing obligation cannot be recorded durably, so it is not                      recorded at all: a truncated obligation would name fewer worker                      generations than the attempt addressed and would read as settled"
                );
                return;
            }
        };

        let pending = self.fencing.targets().pending();
        let outstanding = self.fencing.outstanding().outstanding_count();
        let age = metrics::age_of(since);
        self.fencing
            .ctx
            .job_mut()
            .status
            .record_fencing_obligation(Some(obligation));
        match self.fencing.ctx.job().publish_status().await {
            Ok(crate::states::lifecycle::StatusPublication::Published) => {
                info!(
                    job_id = %job_id,
                    pending_targets = pending,
                    outstanding_attempts = outstanding,
                    "this attempt's fencing obligation is now durable: a controller that reads                      this row will advance its own fence at every target named in it before the                      job admits a replacement generation"
                );
                if pending == 0 {
                    let _quiet = metrics::alert_settled(&job_id);
                } else {
                    let _raised = metrics::alert_pending(
                        &job_id,
                        metrics::FencingReport {
                            pending_targets: pending,
                            outstanding_attempts: outstanding,
                            age,
                        },
                    );
                }
            }
            Ok(crate::states::lifecycle::StatusPublication::Superseded(stale)) => {
                // The obligation belongs to whoever holds the row now, and it is already in the
                // row this controller could not write: the attempt that superseded this one
                // adopted the job, which is what makes it answerable for the targets. Recording
                // the stand-down is what turns the report into a stop.
                self.fencing.note_superseded(stale);
            }
            Err(e) => {
                metrics::record_error(&job_id, metrics::FencingError::PublicationFailed);
                error!(
                    job_id = %job_id,
                    error = ?e,
                    "this attempt's fencing obligation could not be written to the job's row;                      it stays staged on the status and the next publication carries it"
                );
            }
        }
    }
}
