//! Recovering a job's fencing obligation after the controller that owed it is gone
//! (M11.T26f, design M11.D39d/M11.D39g).
//!
//! M11.T26e's settlement owner keeps an interrupted fan-out's obligation alive across a
//! *cancelled phase*. It cannot survive a **dead process**, and its own documentation says so:
//! *"what makes the retention recoverable rather than permanent is M11.D39d's durable record,
//! which M11.T26f owns."* This module is the other end of that sentence.
//!
//! # What is recovered, and what is not
//!
//! Recovered from the row: which worker generations were addressed, what each has done about
//! the fence, the `start_execution_id` each was issued, the address each was reached at, the
//! candidate object the dead attempt left unrooted, and when the obligation began.
//!
//! **Not** recovered, because it was never written: the in-process token. M11.T26f's brief
//! forbids persisting one, and the reason is not economy — an
//! [`Admission`](crate::states::Admission) is this process's exclusive right to publish about
//! this job, and a serialized one is a right two processes could present. What a replacement
//! controller re-acquires instead is the *authority*, by re-adopting the row: adoption is a CAS
//! on `(job id, lifecycle_fence, controller_epoch)` that exactly one controller wins, and the
//! loser is told so rather than proceeding. So the obligation is recovered from durable
//! identity, and the entitlement to act on it is re-won rather than restored.
//!
//! # Why this is idempotent by construction rather than by retry count
//!
//! Every step is a function of the durable record and of what was observed in *this* pass:
//!
//! * a target moves `Pending → Acknowledged` or `… → Terminated` and never back, so replaying a
//!   pass over an already-advanced record changes nothing;
//! * the fence advance is itself idempotent at the worker — a generation that already holds
//!   this fence acknowledges it again, because M11.T26d records the *highest* fence and admits
//!   a directive carrying it;
//! * the write is a conditional update of the whole record, not an increment of anything, so
//!   the row after two passes is the row after one; and
//! * a controller killed part-way through leaves the row exactly as its last write left it —
//!   the record is readable, its targets are in one of three states, and the next pass starts
//!   from that.
//!
//! There is no attempt counter, no "already recovered" flag, and nothing that has to be true
//! only once.
//!
//! # What never settles a target
//!
//! A deadline, a dropped future, a failed connection, a failed listing, a fence CAS, a
//! read-through of the row, or the absence of an answer. A permanent unobservable partition
//! leaves the job in `Fencing` for as long as it lasts — M11.D39g's declared liveness result —
//! and what this module does about that is publish it: see
//! [`metrics::alert_pending`](super::fence::metrics::alert_pending).
//!
//! There **are** deadlines here, and it is worth being exact about what they are: a connect
//! timeout and a per-request timeout on the gRPC calls this pass makes. Neither settles
//! anything. A request that times out is an ambiguous transport outcome — M11.T26c's
//! `transport_settlement` says so — which is retried within the same budget the fan-out uses and
//! then reported as [`NotAcknowledged::Unsettled`](super::handshake::NotAcknowledged), and a
//! target that did not acknowledge stays `Pending`. There is no elapsed time anywhere in this
//! module that moves a target, and
//! `nothing_but_the_two_witnesses_can_advance_a_recovered_target` is the pin that keeps it that
//! way.
//!
//! # What is here and what is beside it
//!
//! This module is the **pass**: the adoption it runs under, the fence it advances, the
//! terminations it asks about, the write it ends with, and the metrics it publishes. The value
//! it works on — the record read back from the row, and the two witnesses that may advance it —
//! is [`recovered`].

pub(crate) mod recovered;

use std::collections::HashMap;
use std::sync::Arc;

use arroyo_rpc::fencing::{Fencing, FencingRecordError};
use arroyo_rpc::identity::{WorkerChannel, WorkerClient, worker_client};
use arroyo_rpc::{config::config, grpc_channel_builder};
use arroyo_types::WorkerId;
use cornucopia_async::DatabaseSource;
use thiserror::Error;
use tracing::{error, info, warn};

pub(crate) use recovered::{ObservedTermination, RecoveredObligation, observe_terminations};

use super::fence::AuthorityWriteError;
use super::fence::metrics::{self, FencingError, FencingReport};
use super::handshake::advance_fence_each;
use super::protocol::{FenceProtocol, UnfencedAuthority};
use super::{LifecycleMode, StatusPublication, publish_status};
use crate::JobStatus;
use crate::schedulers::Scheduler;
use crate::states::scheduling::fanout::Accounting;

/// What one recovery pass did.
///
/// `#[must_use]` because three of the five arms mean the attempt must not continue, and the
/// difference between them is what the caller reports.
#[must_use = "a recovered obligation may still be unsettled, in which case this attempt may not \
              admit a replacement generation"]
#[derive(Debug)]
pub(crate) enum Discharge {
    /// This job's lifecycle mechanism does not recover a durable obligation.
    ///
    /// The whole of the pre-activation answer: under
    /// [`LifecycleMode::LegacyT08`](super::LifecycleMode::LegacyT08) nothing here runs, no
    /// record is read, and no row is written.
    Inactive,
    /// The job's row carries no fencing obligation, so there was nothing to discharge.
    NothingRecorded,
    /// Every target settled. The record has been cleared from the row under this controller's
    /// authority, and the attempt may continue.
    Settled,
    /// Targets remain unsettled. The updated record has been written and the alert raised, and
    /// the attempt may not admit a replacement generation.
    StillPending {
        /// How many target generations have not answered.
        pending: usize,
        /// How many issued identifiers belong to them.
        outstanding_attempts: usize,
    },
    /// Another controller holds this job. Nothing further may be published about it.
    Superseded(crate::StaleAuthority),
    /// The obligation could not be discharged at all, and the row was left as it was.
    Unusable(RecoveryFailure),
}

/// Why a recovery pass could not complete.
///
/// None of these settles anything: each leaves the durable record exactly as the last write
/// left it, which is what makes the next pass a repeat rather than a repair.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum RecoveryFailure {
    /// This controller holds no fence it could advance at the recorded generations.
    #[error(
        "job {job_id} cannot advance a lifecycle fence over its recovered obligation: {source}"
    )]
    Unfenced {
        /// The job whose obligation could not be advanced.
        job_id: String,
        /// Why the authority names no fence.
        source: UnfencedAuthority,
    },
    /// The advanced obligation could not be described as a durable record.
    #[error("job {job_id}'s recovered obligation cannot be rewritten: {source}")]
    Unrecordable {
        /// The job whose obligation could not be rewritten.
        job_id: String,
        /// Which rule the record broke.
        source: FencingRecordError,
    },
    /// The row would not take the updated record.
    #[error("job {job_id}'s recovered obligation could not be written to its row: {report}")]
    NotWritten {
        /// The job whose row refused the write.
        job_id: String,
        /// What went wrong, as the publication funnel reported it.
        report: String,
    },
}

/// Discharges whatever fencing obligation this job's row carries, under this controller's
/// freshly adopted authority.
///
/// Why a controller is discharging a recovered obligation.
///
/// The record survives the generation it names (PR #167 round 5), which makes a *settled* target
/// mean two different things depending on who is reading it. `Acknowledged` says that generation
/// took some **earlier** fence; whether that settles anything depends entirely on what the
/// reader is about to do, and there is no way to answer it from the record alone. So the caller
/// says which it is, and the two answers are different code paths rather than a flag one of them
/// might forget to set (PR #167 round 6, finding 1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DischargeReason {
    /// This controller is about to **supersede** the generations the record names — by admitting
    /// a replacement generation, or by publishing a refusal.
    ///
    /// Every target that is still running is re-opened and asked again under the fence this
    /// controller has just adopted. An acknowledgement of a lower fence acknowledges nothing
    /// about this one: those workers still admit their old owner's directives, and D39d requires
    /// them fenced *before* a replacement generation is admitted or a refusal is published.
    /// Observed terminations survive the re-opening — see [`Fencing::reopened`].
    SupersedingTheGenerationsItNames,
    /// This controller is **adopting** the execution the record names and will keep it running.
    ///
    /// The documented exception (PR #167 round 3): the cold `Running` → `LeaderRunning` recovery
    /// admits no generation and issues no start, and in worker-leader mode holds no worker set to
    /// address — it reconnects to the leader the row names, and what makes it exclusive is the
    /// adoption CAS. Re-opening here would demand a fresh acknowledgement from workers this
    /// controller is not superseding, and a partition would then wedge a job that is running
    /// perfectly well. Those workers learn this controller's fence at its first fenced directive.
    AdoptingTheGenerationItNames,
}

/// The whole of M11.T26f's recovery, in the order M11.D39d requires it:
///
/// 1. read the durable record — the caller has already re-adopted, conditionally, so this
///    controller either holds the job or has already been told it does not;
/// 2. **advance** this controller's fence at every pending target it can reach, and take the
///    acknowledgements as values carrying the height each generation reported;
/// 3. ask the scheduler, per generation, which targets are gone, and take those as witnesses;
/// 4. write the advanced obligation back through the one publication funnel — cleared if every
///    target settled, updated if any did not — under id, fence and epoch; and
/// 5. publish the metrics and the operator-visible alert either way.
///
/// It never publishes `Refused` and never admits anything. What it decides is whether the
/// attempt that called it may go on to do those things, which is M11.D39d's *"`Refused` (or a
/// new scheduling generation) becomes reachable only after every target generation has acked
/// the fence/revokes or has been observed terminated."*
///
/// `reason` decides whether the record's *settled* targets are asked again; see
/// [`DischargeReason`], which is the whole of PR #167 round 6's finding 1.
pub(crate) async fn discharge_recorded_obligation(
    status: &mut JobStatus,
    db: &DatabaseSource,
    scheduler: &Arc<dyn Scheduler>,
    mode: LifecycleMode,
    reason: DischargeReason,
) -> Discharge {
    if !mode.recovers_a_durable_fencing_obligation() {
        return Discharge::Inactive;
    }
    let job_id = (**status.authority().job_id()).clone();
    let Some(record) = status.recorded_fencing().cloned() else {
        return Discharge::NothingRecorded;
    };
    // The one place the settled states of a recovered record are re-opened, and the reason
    // decides it (PR #167 round 6, finding 1). Doing it here rather than at each caller is what
    // stops a third discharge path being added that reads a *previous* fence's acknowledgements
    // as though they said something about this controller's.
    let record = match reason {
        DischargeReason::SupersedingTheGenerationsItNames => {
            let reopened = record.reopened();
            status.record_fencing_obligation(Some(reopened.clone()));
            reopened
        }
        DischargeReason::AdoptingTheGenerationItNames => record,
    };
    let adopted_fence = status.authority().fence().get();
    let mut obligation = RecoveredObligation::of(&record, adopted_fence);
    info!(
        job_id,
        adopted_fence,
        pending = obligation.pending(),
        outstanding_attempts = obligation.outstanding_attempts(),
        "recovered a durable fencing obligation left by an earlier scheduling attempt"
    );

    if let Err(failure) = advance_and_observe(&mut obligation, status, scheduler, &job_id).await {
        // Nothing was settled and nothing was written. The record stands exactly as it was, so
        // the next pass repeats this one rather than continuing it.
        metrics::record_error(&job_id, FencingError::Unrecordable);
        error!(job_id, error = %failure, "a recovery pass could not advance this job's fencing obligation");
        return Discharge::Unusable(failure);
    }

    let pending = obligation.pending();
    let outstanding_attempts = obligation.outstanding_attempts();
    let age = obligation.age();
    if let Some(candidate) = obligation.candidate_root()
        && pending == 0
    {
        // Compared against what the row says is authoritative *now*, rather than reported as
        // orphaned on the strength of the interrupted attempt's own belief. A controller that
        // wrote a candidate and was interrupted between the object and the row update recorded
        // it as unrooted; the very next thing that happened may have been its own retry
        // installing it. The row is the only thing that decides (M11.D39d), so it is read.
        match status.metadata_root().map(|root| root.object()) {
            Some(rooted) if rooted == candidate => info!(
                job_id,
                candidate, "the candidate this obligation named is the job's authoritative root"
            ),
            _ => warn!(
                job_id,
                candidate,
                "the interrupted attempt left this candidate object unrooted; it is reclaimed \
                 by the job's own generation collector and the fencing record no longer names it"
            ),
        }
    }
    let record = match obligation.into_record() {
        Ok(record) => record,
        // **Unreachable in this build**, for the reason
        // [`RecoveredObligation::into_record`](recovered::RecoveredObligation::into_record)
        // states where the operations are: the obligation was built from a record that already
        // passed `Fencing::validate`, and advancing it changes only a target's state — never the
        // number of targets, the identifiers, the addresses or the candidate key. Answered
        // rather than asserted away because that is an argument about today's operations rather
        // than a property of the type, and because the safe answer to a record that cannot be
        // rewritten is to leave the one already in the row exactly as it is.
        Err(source) => {
            metrics::record_error(&job_id, FencingError::Unrecordable);
            return Discharge::Unusable(RecoveryFailure::Unrecordable {
                job_id: job_id.clone(),
                source,
            });
        }
    };

    match write_record(status, db, record).await {
        Ok(StatusPublication::Published) => {}
        // Not stood down here. Losing the job is one thing the whole controller does one way,
        // and the caller has the route: `PhaseContext::stand_down_from` logs it once and records
        // it on the attempt so the interruption ends as a stand-down rather than as an error.
        // Logging it here as well would make one lost duel two log lines saying different
        // things about what happens next.
        Ok(StatusPublication::Superseded(stale)) => return Discharge::Superseded(stale),
        Err(e) => {
            metrics::record_error(&job_id, FencingError::PublicationFailed);
            return Discharge::Unusable(RecoveryFailure::NotWritten {
                job_id: job_id.clone(),
                report: format!("{e:?}"),
            });
        }
    }

    if pending == 0 {
        let _cleared = metrics::alert_settled(&job_id);
        return Discharge::Settled;
    }
    let _raised = metrics::alert_pending(
        &job_id,
        FencingReport {
            pending_targets: pending,
            outstanding_attempts,
            age,
        },
    );
    Discharge::StillPending {
        pending,
        outstanding_attempts,
    }
}

/// Steps 2 and 3: advance the fence where this controller can reach a target, and observe
/// terminations where it cannot.
///
/// Both are attempted for every pending generation, and neither failing settles anything. A
/// generation that refuses the advance, one this controller cannot open a channel to, and one
/// whose scheduler listing failed all leave their targets pending — which is the partition
/// outcome, arrived at by three different routes that must not be told apart by guessing.
async fn advance_and_observe(
    obligation: &mut RecoveredObligation,
    status: &JobStatus,
    scheduler: &Arc<dyn Scheduler>,
    job_id: &str,
) -> Result<(), RecoveryFailure> {
    for (generation, targets) in obligation.pending_by_generation() {
        let protocol =
            FenceProtocol::for_job(LifecycleMode::FencedV2, status.authority(), generation)
                .map_err(|source| RecoveryFailure::Unfenced {
                    job_id: job_id.to_string(),
                    source,
                })?;
        let FenceProtocol::Fenced(fenced) = protocol else {
            // **Unreachable, by the literal one line above.** `FenceProtocol::for_job` matches
            // exhaustively on the mode it is given and answers `Legacy` only for `LegacyT08`;
            // this call names `FencedV2`, so its two possible answers are `Fenced` and the
            // `UnfencedAuthority` error the `?` above already took. It is answered rather than
            // `unreachable!()`d because a recovery pass that had somehow reached the pre-fence
            // protocol must leave the record standing, and a panic here would take the whole
            // controller down over one job's row.
            return Err(RecoveryFailure::Unfenced {
                job_id: job_id.to_string(),
                source: UnfencedAuthority::Unadopted {
                    job_id: job_id.to_string(),
                },
            });
        };

        let addressed: Vec<WorkerId> = targets.iter().map(|target| target.worker).collect();
        let mut connects = HashMap::new();
        for target in &targets {
            let Some(address) = target.rpc_address.as_deref() else {
                continue;
            };
            if let Some(client) = connect(address, target.worker).await {
                // The process the obligation is owed by, from the record rather than from
                // anything this pass observes: the controller that registered this generation
                // is gone, so the durable incarnation is the only thing that can address the
                // advance to the process that actually owes it (M11.D39d, PR #167 round 6). A
                // record written before the field names none, and a generation that has one
                // refuses an advance that names none — so such a target stays pending, which is
                // M11.D39g's declared outcome for one this controller cannot fence.
                connects.insert(target.worker, WorkerChannel::to(client, target.incarnation));
            }
        }
        let advance = advance_fence_each(fenced, connects).await;
        for refusal in advance.unacknowledged() {
            metrics::record_error(job_id, FencingError::NotAcknowledged);
            warn!(job_id, generation, error = %refusal,
                "a recorded target did not acknowledge this controller's fence");
        }
        for acknowledgement in advance.acknowledged() {
            if obligation.acknowledge(acknowledgement) {
                metrics::record_settlement(job_id, Accounting::AcknowledgedFence);
            }
        }

        match observe_terminations(scheduler, job_id, generation, &addressed).await {
            Ok(terminations) => {
                for termination in &terminations {
                    if obligation.terminate(termination) {
                        metrics::record_settlement(job_id, Accounting::TerminatedGeneration);
                        info!(
                            job_id,
                            generation,
                            worker_id = termination.worker().0,
                            "a recorded target's worker generation was observed terminated"
                        );
                    }
                }
            }
            Err(e) => {
                metrics::record_error(job_id, FencingError::TerminationUnobservable);
                warn!(job_id, generation, error = %e,
                    "this controller cannot observe whether a recorded target has terminated, \
                     so its obligation stays pending");
            }
        }
    }
    Ok(())
}

/// Opens a channel to one recorded target, or answers `None`.
///
/// `None` is not a failure to report and is emphatically not settlement: a target this
/// controller cannot reach is a target it cannot fence, which is the partition case. One
/// attempt with a short connect timeout, because the retry that matters is the next recovery
/// pass and holding the job's admission open across a long dial helps nobody.
async fn connect(address: &str, worker: WorkerId) -> Option<WorkerClient> {
    let endpoint = grpc_channel_builder(
        "controller",
        address.to_string(),
        &config().controller.tls,
        &config().worker.tls,
        Some(CONNECT_TIMEOUT),
    )
    .await
    .inspect_err(|e| {
        warn!(worker_id = worker.0, address, error = %e,
            "a recorded target's address cannot be turned into an endpoint")
    })
    .ok()?;
    endpoint
        .timeout(REQUEST_TIMEOUT)
        .connect()
        .await
        .inspect_err(|e| {
            warn!(worker_id = worker.0, address, error = %e,
                "a recorded target cannot be reached, so its obligation stays pending")
        })
        .ok()
        .map(|channel| worker_client(channel, worker))
}

/// How long to spend opening a channel to a recorded target.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// How long a single fence directive to a recorded target may take.
///
/// The same 90s the scheduling path gives a worker channel, because it is the same kind of
/// call to the same server.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);

/// Writes the advanced obligation to the job's row, through the one publication funnel.
///
/// The record is installed on the status *before* the write and is left installed if the write
/// fails, which is the opposite of what
/// [`JobStatus::install_metadata_root`](crate::JobStatus::install_metadata_root) does — and
/// deliberately so, because the two fail in opposite directions. A metadata root this status
/// claimed but never installed would be presented to a later reader as authoritative; an
/// obligation this status claims but has not yet written keeps the job fencing, which is the
/// safe answer. The next pass republishes it, so the row and the status converge rather than
/// diverging.
async fn write_record(
    status: &mut JobStatus,
    db: &DatabaseSource,
    record: Option<Fencing>,
) -> Result<StatusPublication, AuthorityWriteError> {
    status.record_fencing_obligation(record);
    publish_status(status, db).await
}
