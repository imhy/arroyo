//! Durable fencing recovery (M11.T26f, design M11.D39d/M11.D39g).
//!
//! These rows run against the schema the SQLite migrations actually produce and against real
//! worker servers on real sockets, for the reason [`super::fence_tests`] gives: the properties
//! are properties of a row that is written by one process and read by another, and a fixture
//! that mirrored the schema would be a second opinion about it that cannot fail when a
//! migration changes.
//!
//! **A controller restart is modelled by re-reading the row.** There is no way to keep an
//! in-process value across one, which is exactly the property under test: everything a
//! replacement controller knows about the obligation, it knows because the row said so.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arroyo_rpc::fencing::{FenceTarget, FenceTargetState, Fencing};
use arroyo_rpc::grpc::rpc::worker_grpc_server::{WorkerGrpc, WorkerGrpcServer};
use arroyo_rpc::grpc::rpc::{
    CheckpointReq, CheckpointResp, CommitReq, CommitResp, GetWorkerPhaseReq, GetWorkerPhaseResp,
    HeartbeatNodeReq, JobControllerInitReq, JobControllerInitResp, JobFinishedReq, JobFinishedResp,
    LoadCompactedDataReq, LoadCompactedDataRes, MetricsReq, MetricsResp, RegisterNodeReq,
    StartExecutionOutcome, StartExecutionReq, StartExecutionResp, StopExecutionReq,
    StopExecutionResp, WorkerFinishedReq,
};
use arroyo_rpc::{StateContext, fence_wire};
use arroyo_types::WorkerId;

/// Whether a record on the row owes nothing: every target settled, and no identifier left that
/// anything could still be answerable for.
///
/// Since PR #167 round 6 a discharged record is **kept**, not deleted: what it still names is the
/// worker generation that can act and the address it answers at, which is what a later refusal or
/// replacement has to fence. So "discharged" is read off its contents rather than off its absence.
fn owes_nothing(record: &Option<arroyo_rpc::fencing::Fencing>) -> bool {
    record.as_ref().is_some_and(|record| {
        record.targets().iter().all(|target| {
            target.state != arroyo_rpc::fencing::FenceTargetState::Pending
                && target.attempt_id.is_none()
        })
    })
}
use cornucopia_async::DatabaseSource;
use cornucopia_async::rusqlite::Connection;

use super::LifecycleMode;
use super::fence::metrics::{self, AlertTransition, FencingError};
use super::fence_tests::{JOB, adopt, cold_status, migrated_job_named};
use super::handshake::FenceAcknowledgement;
use super::recovery::{
    Discharge, DischargeReason, ObservedTermination, RecoveredObligation, RecoveryFailure,
    discharge_recorded_obligation, observe_terminations,
};
use crate::schedulers::{GenerationObservation, Scheduler, SchedulerError, StartPipelineReq};
use crate::states::scheduling::fanout::Accounting;

/// The scheduling generation the obligations below were addressed to.
const GENERATION: u64 = 4;

/// The worker the obligations below name.
const WORKER: WorkerId = WorkerId(7);

/// The identifier that worker was issued.
const ATTEMPT: &str = "0123456789abcdef0123456789abcdef";

// ---------------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------------

/// The process the recorded target was answering as.
///
/// Durable, because the controller that registered it is gone: a fenced advance names the
/// process it is for, and the record is the only thing that can tell its successor which
/// (M11.D39d, PR #167 round 6).
const INCARNATION: u64 = 21;

/// One recorded target, pending, reachable at `address` if it is reachable at all.
fn pending_target(address: Option<String>) -> FenceTarget {
    FenceTarget {
        worker_id: WORKER.0,
        generation: GENERATION,
        attempt_id: Some(ATTEMPT.to_string()),
        rpc_address: address,
        incarnation: std::num::NonZeroU64::new(INCARNATION),
        state: FenceTargetState::Pending,
    }
}

/// The obligation an interrupted attempt would have left: one pending target, one unrooted
/// candidate, and an origin an hour ago.
fn obligation(address: Option<String>) -> Fencing {
    Fencing::record(
        vec![pending_target(address)],
        Some("pl_1/job/generations/4/candidates/c.json".to_string()),
        Some(an_hour_ago()),
    )
    .expect("the fixture obligation is writable")
}

/// An origin an hour before now, so the age this obligation reports is non-zero and closed-form
/// enough to bound.
fn an_hour_ago() -> u64 {
    metrics::now_millis().expect("the host clock is after the epoch") - 3_600_000
}

/// The job, its database and the connection under it, one job per row.
///
/// The job id is the row's own name, so the metric series this suite asserts are the series this
/// row wrote and nothing else. See [`migrated_job_named`].
fn job(row: &str) -> (String, DatabaseSource, Arc<Mutex<Connection>>) {
    let job_id = format!("{JOB}-{row}");
    let (db, connection) = migrated_job_named(&job_id);
    (job_id, db, connection)
}

/// Writes `record` into the job's durable execution record, as an earlier attempt would have.
///
/// Straight SQL rather than through the publication funnel, because this is the *previous*
/// controller's write: a row a process that is gone left behind, which is what a recovering
/// controller reads.
fn seed_obligation(job_id: &str, connection: &Mutex<Connection>, record: Option<&Fencing>) {
    let context = StateContext {
        version: 1,
        leader: None,
        execution_selector: Some("parquet".to_string()),
        fencing: record.cloned(),
        metadata_root: None,
    };
    connection
        .lock()
        .unwrap()
        .execute(
            "UPDATE job_statuses SET state_context = ?1 WHERE id = ?2",
            cornucopia_async::rusqlite::params![
                serde_json::to_string(&context).expect("the fixture context serializes"),
                job_id
            ],
        )
        .expect("the fixture obligation must be written");
}

/// Leaves the row carrying the authority a previous controller installed.
///
/// Without it every fixture starts at the column default, and the *first* adoption installs
/// fence 1 — so the highest fence anything in a recovered record could have been issued under
/// is 0, and every acknowledgement supersedes it. A row about the height check has to start
/// from a job some controller has already held.
fn seed_previous_authority(job_id: &str, connection: &Mutex<Connection>, fence: i64) {
    connection
        .lock()
        .unwrap()
        .execute(
            "UPDATE job_statuses SET lifecycle_fence = ?1, controller_epoch = ?2 WHERE id = ?3",
            cornucopia_async::rusqlite::params![fence, "epoch-before-this-controller", job_id],
        )
        .expect("the fixture authority must be written");
}

/// The fencing record the job's row carries now, read straight out of the fixture.
///
/// This is what "read back after a controller restart" means here: the value is decoded from the
/// column by the same `StateContext` a fresh controller decodes, and nothing in this process is
/// consulted.
fn recorded(job_id: &str, connection: &Mutex<Connection>) -> Option<Fencing> {
    let raw: String = connection
        .lock()
        .unwrap()
        .query_row(
            "SELECT state_context FROM job_statuses WHERE id = ?1",
            [job_id],
            |row| row.get(0),
        )
        .expect("the job's row must be readable");
    serde_json::from_str::<StateContext>(&raw)
        .expect("the row's execution record must decode")
        .fencing
}

// ---------------------------------------------------------------------------------------------
// A scheduler that answers about live worker generations
// ---------------------------------------------------------------------------------------------

/// What the scheduler under test says about a job's live worker generations.
#[derive(Clone)]
enum Lists {
    /// These worker generations are still running, and nothing else is.
    Live(Vec<(u64, WorkerId)>),
    /// The listing itself failed.
    Fails,
    /// This scheduler cannot say whether a generation has terminated.
    Untracked,
}

struct TestScheduler(Lists);

impl TestScheduler {
    fn shared(lists: Lists) -> Arc<dyn Scheduler> {
        Arc::new(TestScheduler(lists))
    }
}

#[async_trait::async_trait]
impl Scheduler for TestScheduler {
    async fn start_workers(&self, _: StartPipelineReq) -> Result<(), SchedulerError> {
        Ok(())
    }
    async fn register_node(&self, _: RegisterNodeReq) {}
    async fn heartbeat_node(&self, _: HeartbeatNodeReq) -> Result<(), tonic::Status> {
        Ok(())
    }
    async fn worker_finished(&self, _: WorkerFinishedReq) {}
    async fn stop_workers(&self, _: &str, _: Option<u64>, _: bool) -> anyhow::Result<()> {
        Ok(())
    }
    async fn workers_for_job(
        &self,
        _: &str,
        generation: Option<u64>,
    ) -> anyhow::Result<Vec<WorkerId>> {
        match &self.0 {
            Lists::Live(live) => Ok(live
                .iter()
                .filter(|(g, _)| generation.is_none_or(|generation| generation == *g))
                .map(|(_, worker)| *worker)
                .collect()),
            Lists::Fails => Err(anyhow::anyhow!("the scheduler cannot list this job")),
            Lists::Untracked => Ok(vec![]),
        }
    }
    async fn observe_generation(
        &self,
        job_id: &str,
        generation: u64,
    ) -> anyhow::Result<GenerationObservation> {
        match &self.0 {
            Lists::Untracked => Ok(GenerationObservation::Untracked {
                scheduler: "test",
                why: "this fixture answers the way a scheduler with no worker registry answers",
            }),
            _ => Ok(GenerationObservation::Live(
                self.workers_for_job(job_id, Some(generation)).await?,
            )),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// A worker that answers a fence directive
// ---------------------------------------------------------------------------------------------

/// How a [`FenceWorker`] answers a `FENCE_ONLY` directive.
#[derive(Clone)]
enum Answers {
    /// Announces that it has been asked and then waits, so a row can drop the recovery pass
    /// while a directive is in flight. Acknowledges once released.
    Pausing(Arc<Paused>),
    /// Acknowledges the fence it was addressed under, as M11.T26d's guard does.
    Acknowledging,
    /// Acknowledges, but reports this height instead of the one addressed.
    AcknowledgingAt(u64),
    /// Refuses definitively — its own decision about this controller's fence, which is what a
    /// reused endpoint answering for its predecessor's generation produces.
    Refusing,
    /// Answers `Unavailable` to every attempt, forever. The shape a partition presents: the peer
    /// is reachable enough to fail and never settles anything.
    NeverSettling,
}

/// A worker that has been asked to advance its fence and has not answered yet.
#[derive(Default)]
struct Paused {
    /// Fired from inside the handler, so no row has to guess when the directive arrived.
    asked: tokio::sync::Notify,
    /// Fired by a row to let the handler acknowledge.
    released: tokio::sync::Notify,
}

#[derive(Default)]
struct Directives {
    /// Every `(fence, generation)` this worker was addressed under, in arrival order.
    /// Every directive this worker was sent, as `(fence, generation, incarnation)`.
    ///
    /// The incarnation is recorded because it is the half of the address a *durable* record has
    /// to carry: the controller that registered these workers is gone, so if the record does not
    /// name the process, the advance names none and a generation that has one refuses it
    /// (M11.D39d, PR #167 round 6).
    seen: Mutex<Vec<(u64, u64, Option<u64>)>>,
}

struct FenceWorker {
    directives: Arc<Directives>,
    answers: Answers,
}

#[tonic::async_trait]
impl WorkerGrpc for FenceWorker {
    async fn start_execution(
        &self,
        request: tonic::Request<StartExecutionReq>,
    ) -> Result<tonic::Response<StartExecutionResp>, tonic::Status> {
        let request = request.into_inner();
        let directive = fence_wire::start_directive(&request)
            .expect("this controller sent a malformed directive");
        let fence_wire::StartDirective::Fenced { address, .. } = directive else {
            panic!("a recovery pass sends only fenced directives");
        };
        self.directives.seen.lock().unwrap().push((
            address.fence(),
            address.target().generation(),
            address
                .target()
                .incarnation()
                .map(arroyo_rpc::fence_wire::WorkerIncarnation::get),
        ));
        match &self.answers {
            Answers::Acknowledging => Ok(tonic::Response::new(StartExecutionResp {
                observed_lifecycle_fence: address.fence(),
                outcome: StartExecutionOutcome::FenceAcknowledged as i32,
            })),
            Answers::AcknowledgingAt(height) => Ok(tonic::Response::new(StartExecutionResp {
                observed_lifecycle_fence: *height,
                outcome: StartExecutionOutcome::FenceAcknowledged as i32,
            })),
            Answers::Pausing(paused) => {
                paused.asked.notify_one();
                paused.released.notified().await;
                Ok(tonic::Response::new(StartExecutionResp {
                    observed_lifecycle_fence: address.fence(),
                    outcome: StartExecutionOutcome::FenceAcknowledged as i32,
                }))
            }
            Answers::Refusing => Err(tonic::Status::failed_precondition(
                "this generation refuses the directive",
            )),
            Answers::NeverSettling => Err(tonic::Status::unavailable("this worker is partitioned")),
        }
    }

    async fn get_worker_phase(
        &self,
        _: tonic::Request<GetWorkerPhaseReq>,
    ) -> Result<tonic::Response<GetWorkerPhaseResp>, tonic::Status> {
        Ok(tonic::Response::new(GetWorkerPhaseResp::default()))
    }
    async fn checkpoint(
        &self,
        _: tonic::Request<CheckpointReq>,
    ) -> Result<tonic::Response<CheckpointResp>, tonic::Status> {
        Ok(tonic::Response::new(CheckpointResp {}))
    }
    async fn commit(
        &self,
        _: tonic::Request<CommitReq>,
    ) -> Result<tonic::Response<CommitResp>, tonic::Status> {
        Ok(tonic::Response::new(CommitResp {}))
    }
    async fn load_compacted_data(
        &self,
        _: tonic::Request<LoadCompactedDataReq>,
    ) -> Result<tonic::Response<LoadCompactedDataRes>, tonic::Status> {
        Ok(tonic::Response::new(LoadCompactedDataRes {}))
    }
    async fn stop_execution(
        &self,
        _: tonic::Request<StopExecutionReq>,
    ) -> Result<tonic::Response<StopExecutionResp>, tonic::Status> {
        Ok(tonic::Response::new(StopExecutionResp {}))
    }
    async fn job_finished(
        &self,
        _: tonic::Request<JobFinishedReq>,
    ) -> Result<tonic::Response<JobFinishedResp>, tonic::Status> {
        Ok(tonic::Response::new(JobFinishedResp {}))
    }
    async fn get_metrics(
        &self,
        _: tonic::Request<MetricsReq>,
    ) -> Result<tonic::Response<MetricsResp>, tonic::Status> {
        Ok(tonic::Response::new(MetricsResp::default()))
    }
    async fn job_controller_init(
        &self,
        _: tonic::Request<JobControllerInitReq>,
    ) -> Result<tonic::Response<JobControllerInitResp>, tonic::Status> {
        Ok(tonic::Response::new(JobControllerInitResp {}))
    }
}

/// Serves a [`FenceWorker`] on a loopback port and returns the address the record would name.
async fn serve(answers: Answers) -> (String, Arc<Directives>) {
    let directives = Arc::new(Directives::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let served = Arc::clone(&directives);
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(WorkerGrpcServer::new(FenceWorker {
                directives: served,
                answers,
            }))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    (address, directives)
}

// ---------------------------------------------------------------------------------------------
// The record is written under authority, and read back by a process that did not write it
// ---------------------------------------------------------------------------------------------

/// A fencing obligation reaches the row only through the conditional write, and is read back
/// whole by a controller that did not write it (M11.D39d).
///
/// Three claims, and the middle one is the reason the first two matter:
///
/// * the record a controller stages is written under **id, fence and epoch** — the same
///   predicate every other M11.D39d write is under, because it goes through the same funnel;
/// * a controller whose authority the row no longer carries writes **nothing**, and the record
///   the winner wrote stands unchanged; and
/// * everything the record carries — targets, states, identifiers, addresses, the candidate and
///   the origin — survives the round trip through the column, because a value that lost a field
///   on the way out would be an obligation naming fewer targets than were addressed.
#[tokio::test]
async fn a_fencing_obligation_is_written_under_authority_and_read_back_whole() {
    let (job_id, db, connection) = job("written-under-authority");
    // Read *before* the winner adopts, which is what makes it a loser: a status read afterwards
    // would be carrying the winner's own authority and its write would land.
    let loser = cold_status(&db).await;
    let mut winner = cold_status(&db).await;
    adopt(&mut winner, &db).await;

    let record = obligation(Some("http://10.0.0.1:9191".to_string()));
    winner.record_fencing_obligation(Some(record.clone()));
    assert!(
        matches!(
            super::publish_status(&winner, &db).await,
            Ok(super::StatusPublication::Published)
        ),
        "the controller that holds the row publishes its obligation"
    );

    // The simulated controller loss: nothing of the writer is consulted below.
    let read_back = recorded(&job_id, &connection).expect("the row carries the obligation");
    assert_eq!(
        read_back, record,
        "the record round-trips through the column"
    );
    assert_eq!(
        read_back
            .targets()
            .iter()
            .map(|t| (
                t.worker_id,
                t.generation,
                t.attempt_id.clone(),
                t.rpc_address.clone(),
                t.incarnation.map(std::num::NonZeroU64::get),
                t.state
            ))
            .collect::<Vec<_>>(),
        vec![(
            WORKER.0,
            GENERATION,
            Some(ATTEMPT.to_string()),
            Some("http://10.0.0.1:9191".to_string()),
            Some(INCARNATION),
            FenceTargetState::Pending
        )],
        "with the target, the identifier it was issued, the address it was reached at and the \
         process that was answering there — the four things a replacement controller needs to \
         fence it (PR #167 round 6, finding 3: without the process, an advance names one that \
         may already be gone and a successor refuses it)"
    );
    assert_eq!(read_back.candidate_root(), record.candidate_root());
    assert_eq!(
        read_back.fencing_since_millis(),
        record.fencing_since_millis()
    );

    // And a controller that no longer holds the row cannot replace it.
    let mut loser = loser;
    loser.record_fencing_obligation(None);
    assert!(
        matches!(
            super::publish_status(&loser, &db).await,
            Ok(super::StatusPublication::Superseded(_))
        ),
        "a superseded controller's write matches no row"
    );
    assert_eq!(
        recorded(&job_id, &connection),
        Some(record),
        "so the obligation the holder wrote stands: clearing it is a publication like any \
         other, and a controller that has lost the job may not perform one"
    );

    // The same status, read through the poll a fresh controller runs, carries it too.
    let restarted = cold_status(&db).await;
    assert!(
        restarted.recorded_fencing().is_some(),
        "a controller that has just read this row for the first time recovers the obligation \
         from it, which is the whole of what survives a process"
    );
}

// ---------------------------------------------------------------------------------------------
// D96 row 17
// ---------------------------------------------------------------------------------------------

/// **D96 row 17.** A replacement controller cannot publish before the worker fence settles.
///
/// The replacement re-adopts the job — conditionally, and it wins — and then finds an obligation
/// its predecessor left. It advances its own fence at the recorded target, which refuses; the
/// scheduler still lists that worker generation as live; so neither of M11.D39e(v)'s two
/// non-response facts is observed and the obligation is **not** discharged.
///
/// What that costs the replacement is the thing the row is named for: the discharge answers
/// [`Discharge::StillPending`], the preamble step turns that into an interruption, and the
/// preamble reaches it *before* `persist_generation` — which is M11.D39d's "admission of a
/// replacement generation". Both halves are asserted: the value, against a real row and a real
/// worker, and the position, against the driver's own source, because a behavioural test of a
/// successful attempt cannot show the order of steps it took.
/// **PR #167 round 7, finding 1.** A controller that is about to *supersede* the generations a
/// record names asks them again; one that is *adopting* them does not.
///
/// Round 5 made a settled fan-out leave its targets `Acknowledged` rather than clearing the
/// record, because what it still says — which generations can act and where they answer — stays
/// true while they run. Round 6 stopped ordinary recovery deleting that. What neither did is
/// decide what a *reader* of those acknowledgements may conclude, and the answer is not the same
/// for both readers: `Acknowledged` says that generation took some **earlier** fence, so a
/// replacement preamble that treats it as settlement admits a new generation while the old one
/// still admits its old owner's directives, having sent no fence at all.
///
/// Both readings are asserted against the same record, because either one alone is consistent
/// with a build that has no idea which it is doing:
///
/// * superseding — every live target is re-opened and the advance goes out under the fence this
///   controller has just adopted, and the discharge does **not** report the job settled;
/// * adopting — nothing is asked, which is the documented cold worker-leader exception (PR #167
///   round 3): it admits no generation, so demanding a fresh acknowledgement would let a
///   partition wedge a job that is running perfectly well.
#[tokio::test]
async fn a_superseding_controller_asks_a_settled_generation_again_and_an_adopting_one_does_not() {
    for (reason, expected_directives) in [
        (
            DischargeReason::SupersedingTheGenerationsItNames,
            "the replacement advances its own fence at the generation it is superseding",
        ),
        (
            DischargeReason::AdoptingTheGenerationItNames,
            "the adopter asks the generation it is keeping nothing",
        ),
    ] {
        let superseding = reason == DischargeReason::SupersedingTheGenerationsItNames;
        let (job_id, db, connection) = job(if superseding {
            "row17-superseding"
        } else {
            "row17-adopting"
        });
        let (address, directives) = serve(Answers::Acknowledging).await;

        // Exactly what a healthy, settled fan-out leaves behind (PR #167 round 5): the targets
        // are acknowledged, and no identifier is outstanding.
        let settled = obligation(Some(address)).settled_and_still_running();
        assert!(
            settled
                .targets()
                .iter()
                .all(|t| t.state == FenceTargetState::Acknowledged),
            "the fixture is the record a healthy fan-out leaves, not a pending one"
        );
        seed_obligation(&job_id, &connection, Some(&settled));

        let mut controller = cold_status(&db).await;
        adopt(&mut controller, &db).await;
        let adopted = controller.authority().fence().get();

        let scheduler = TestScheduler::shared(Lists::Live(vec![(GENERATION, WORKER)]));
        let discharge = discharge_recorded_obligation(
            &mut controller,
            &db,
            &scheduler,
            LifecycleMode::FencedV2,
            reason,
        )
        .await;

        let sent = directives.seen.lock().unwrap().clone();
        if superseding {
            assert_eq!(
                sent,
                vec![(adopted, GENERATION, Some(INCARNATION))],
                "{expected_directives}"
            );
            assert!(
                matches!(discharge, Discharge::Settled),
                "and it settles only because that generation answered the *new* fence: {discharge:?}"
            );
        } else {
            assert_eq!(sent, Vec::new(), "{expected_directives}");
            assert!(
                matches!(discharge, Discharge::Settled),
                "and it settles on the record as it stands: {discharge:?}"
            );
        }
    }
}

/// A re-opening keeps authoritative termination evidence, and expires only acknowledgements.
///
/// The two settled states are not the same kind of fact (PR #167 round 7, finding 1).
/// `Acknowledged` is about a *fence* and expires the moment a higher one exists. `Terminated` is
/// about the world: the generation is gone, and no later fence brings it back. Re-opening a
/// terminated target would throw away evidence this controller already holds and ask it to
/// observe a teardown its scheduler may no longer be able to see — a job wedged in `Fencing` for
/// want of a fact it had.
#[test]
fn re_opening_a_record_expires_acknowledgements_and_keeps_terminations() {
    let record = Fencing::record(
        vec![
            FenceTarget {
                worker_id: 1,
                state: FenceTargetState::Acknowledged,
                ..pending_target(Some("http://10.0.0.1:9191".to_string()))
            },
            FenceTarget {
                worker_id: 2,
                state: FenceTargetState::Terminated,
                ..pending_target(Some("http://10.0.0.2:9191".to_string()))
            },
            FenceTarget {
                worker_id: 3,
                state: FenceTargetState::Pending,
                ..pending_target(Some("http://10.0.0.3:9191".to_string()))
            },
        ],
        None,
        Some(an_hour_ago()),
    )
    .expect("the fixture record is writable");

    let reopened = record.reopened();
    assert_eq!(
        reopened
            .targets()
            .iter()
            .map(|t| (t.worker_id, t.state, t.attempt_id.clone()))
            .collect::<Vec<_>>(),
        vec![
            // Acknowledged: expired, and its identifier with it — that identifier was settled by
            // the pass that acknowledged it, and what is asked for now is a *fence*.
            (1, FenceTargetState::Pending, None),
            // Terminated: kept whole. The generation is gone and no later fence brings it back.
            (2, FenceTargetState::Terminated, Some(ATTEMPT.to_string())),
            // Already pending: untouched, identifier included. It owes exactly what it owed, and
            // dropping the identifier here would quietly forgive an outstanding start.
            (3, FenceTargetState::Pending, Some(ATTEMPT.to_string())),
        ],
        "an acknowledgement of an older fence expires; an observed termination does not; and a \
         target that already owed something still owes it"
    );
}

/// **PR #167 round 6, finding 3.** A record written before the incarnation field addresses no
/// process, and its target stays pending.
///
/// The compatibility statement, made rather than argued. An advance is addressed to a *process*
/// since round 6, and the only thing that can tell a replacement controller which process a
/// target was is the record — the controller that registered it is gone. A record that names none
/// therefore produces an advance that names none, which a generation that has one refuses; the
/// target is left pending, which is M11.D39g's declared outcome for one this controller cannot
/// fence. The positive control is `replacement_cannot_publish_before_worker_fence_settlement`
/// above, whose record names a process and whose advance carries it.
#[tokio::test]
async fn a_record_written_before_the_incarnation_field_addresses_no_process() {
    let (job_id, db, connection) = job("row17-preincarnation");
    let (address, directives) = serve(Answers::Acknowledging).await;
    let seeded = Fencing::record(
        vec![FenceTarget {
            incarnation: None,
            ..pending_target(Some(address))
        }],
        None,
        Some(an_hour_ago()),
    )
    .expect("a record predating the field is writable");
    seed_obligation(&job_id, &connection, Some(&seeded));

    let mut replacement = cold_status(&db).await;
    adopt(&mut replacement, &db).await;
    let adopted = replacement.authority().fence().get();

    let scheduler = TestScheduler::shared(Lists::Live(vec![(GENERATION, WORKER)]));
    let _ = discharge_recorded_obligation(
        &mut replacement,
        &db,
        &scheduler,
        LifecycleMode::FencedV2,
        DischargeReason::SupersedingTheGenerationsItNames,
    )
    .await;

    assert_eq!(
        *directives.seen.lock().unwrap(),
        vec![(adopted, GENERATION, None)],
        "the advance carries what the record carried and invents nothing: a controller that \
         guessed a process here would address a live worker under a directive minted for no \
         one"
    );
}

#[tokio::test]
async fn replacement_cannot_publish_before_worker_fence_settlement() {
    let (job_id, db, connection) = job("row17");
    let (address, directives) = serve(Answers::Refusing).await;
    let seeded = obligation(Some(address));
    seed_obligation(&job_id, &connection, Some(&seeded));

    // The replacement controller: it reads the row it did not write, and re-adopts.
    let mut replacement = cold_status(&db).await;
    let before = replacement.authority().fence().get();
    adopt(&mut replacement, &db).await;
    let adopted = replacement.authority().fence().get();
    assert_eq!(
        adopted,
        before + 1,
        "adoption raises the fence, which is what gives the advance below something to send"
    );

    let scheduler = TestScheduler::shared(Lists::Live(vec![(GENERATION, WORKER)]));
    let discharge = discharge_recorded_obligation(
        &mut replacement,
        &db,
        &scheduler,
        LifecycleMode::FencedV2,
        DischargeReason::SupersedingTheGenerationsItNames,
    )
    .await;
    assert!(
        matches!(
            discharge,
            Discharge::StillPending {
                pending: 1,
                outstanding_attempts: 1
            }
        ),
        "one target has neither acknowledged nor been observed terminated, and it holds one \
         issued identifier"
    );
    assert_eq!(
        *directives.seen.lock().unwrap(),
        vec![(adopted, GENERATION, Some(INCARNATION))],
        "the replacement did advance its own fence at the recorded generation, addressed to the \
         process the record names — this is not a controller that failed to try, and not one \
         addressing a process that may already be gone (PR #167 round 6, finding 3)"
    );
    let still = recorded(&job_id, &connection).expect("the obligation stands");
    assert_eq!(
        still.targets()[0].state,
        FenceTargetState::Pending,
        "and the target it named is still pending in the row"
    );
    assert_eq!(
        still.fencing_since_millis(),
        seeded.fencing_since_millis(),
        "with the origin carried forward, so the age is the obligation's and not this \
         controller's"
    );

    // The position. `persist_generation` is the write that admits a replacement generation, and
    // the discharge is the step before it.
    let driver = include_str!("../scheduling/phases/driver.rs");
    let at = driver
        .find("async fn preamble<'a, 'ctx>(")
        .expect("the preamble driver has been renamed");
    let body = &driver[at..driver[at..]
        .find("\n}\n")
        .map(|end| at + end)
        .expect("unterminated function")];
    let steps: Vec<&str> = body
        .match_indices("preamble.")
        .map(|(i, m)| {
            let rest = &body[i + m.len()..];
            &rest[..rest.find('(').expect("a step is a call")]
        })
        .collect();
    let discharge_at = steps
        .iter()
        .position(|step| *step == "discharge_recovered_fencing")
        .expect("the preamble discharges a recovered obligation");
    let persist_at = steps
        .iter()
        .position(|step| *step == "persist_generation")
        .expect("the preamble persists the generation it raises the job to");
    assert!(
        discharge_at < persist_at,
        "the recovered obligation is discharged before the generation that would replace the \
         one it names is persisted: {steps:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// The two settling observations, and the negative for each
// ---------------------------------------------------------------------------------------------

/// An acknowledgement of a **superseding** fence settles a recovered target; one that does not
/// supersede settles nothing.
///
/// The height is the whole of it (M11.T26f). A recovered obligation's identifiers were issued
/// under some fence at or below the one the row carried before this controller adopted it, and a
/// worker revokes what is *below* the fence it takes — so an acknowledgement at that bound has
/// made nothing inapplicable. The two halves differ in one number and in nothing else.
#[tokio::test]
async fn an_acknowledgement_settles_a_recovered_target_only_if_it_supersedes_what_was_issued() {
    for (what, answers, expected) in [
        (
            "acknowledging the fence it was addressed under",
            Answers::Acknowledging,
            FenceTargetState::Acknowledged,
        ),
        (
            // The bound itself: the fence the row carried before this controller adopted it, and
            // therefore the highest anything in this record could have been issued under.
            "acknowledging only the fence the previous controller held",
            Answers::AcknowledgingAt(1),
            FenceTargetState::Pending,
        ),
    ] {
        let settles = expected == FenceTargetState::Acknowledged;
        let (job_id, db, connection) = job(&format!("ack-{settles}"));
        let (address, _directives) = serve(answers).await;
        // A job a previous controller already held: it left the row at fence 1, so anything in
        // the obligation it recorded was issued under at most fence 1.
        seed_previous_authority(&job_id, &connection, 1);
        seed_obligation(&job_id, &connection, Some(&obligation(Some(address))));

        let mut status = cold_status(&db).await;
        adopt(&mut status, &db).await;
        assert_eq!(
            status.authority().fence().get(),
            2,
            "{what}: this controller adopts to fence 2, so an acknowledgement of fence 1 is \
             exactly the one that supersedes nothing it inherited"
        );
        let scheduler = TestScheduler::shared(Lists::Live(vec![(GENERATION, WORKER)]));
        let discharge = discharge_recorded_obligation(
            &mut status,
            &db,
            &scheduler,
            LifecycleMode::FencedV2,
            DischargeReason::SupersedingTheGenerationsItNames,
        )
        .await;

        match expected {
            FenceTargetState::Acknowledged => {
                assert!(
                    matches!(discharge, Discharge::Settled),
                    "{what}: the only target settles, so the obligation is discharged"
                );
                assert!(
                    owes_nothing(&recorded(&job_id, &connection)),
                    "{what}: and the record on the row owes nothing — it still names the \
                     generation, which is what a later refusal fences"
                );
            }
            _ => {
                assert!(
                    matches!(discharge, Discharge::StillPending { pending: 1, .. }),
                    "{what}: the target is still pending, because the fence it acknowledged \
                     revoked nothing this obligation was issued under"
                );
                assert_eq!(
                    recorded(&job_id, &connection)
                        .expect("the obligation stands")
                        .targets()[0]
                        .state,
                    FenceTargetState::Pending,
                    "{what}: and the row still says so"
                );
            }
        }
    }
}

/// A generation the scheduler no longer lists is settled; one it still lists, one it cannot
/// list, and one it will not answer about are not.
///
/// Four cases over one dimension. The three negatives are the ones that matter: "I do not know"
/// and "they are gone" are the confusion this whole mechanism exists to prevent, and each of the
/// three reaches the recovery pass by a different route — a live listing, a failed listing, and a
/// scheduler that keeps no registry at all.
#[tokio::test]
async fn a_generation_settles_only_when_a_tracking_scheduler_says_it_is_gone() {
    for (what, lists, settles) in [
        (
            "the scheduler no longer lists it",
            Lists::Live(vec![]),
            true,
        ),
        (
            "the scheduler still lists it",
            Lists::Live(vec![(GENERATION, WORKER)]),
            false,
        ),
        ("the listing failed", Lists::Fails, false),
        (
            "the scheduler tracks no generations",
            Lists::Untracked,
            false,
        ),
    ] {
        let (job_id, db, connection) = job(&format!("termination-{settles}-{}", what.len()));
        // No address at all, so nothing can be settled by an acknowledgement and this row is
        // about the termination and only about it.
        seed_obligation(&job_id, &connection, Some(&obligation(None)));
        let mut status = cold_status(&db).await;
        adopt(&mut status, &db).await;

        let scheduler = TestScheduler::shared(lists);
        let discharge = discharge_recorded_obligation(
            &mut status,
            &db,
            &scheduler,
            LifecycleMode::FencedV2,
            DischargeReason::SupersedingTheGenerationsItNames,
        )
        .await;
        assert_eq!(
            matches!(discharge, Discharge::Settled),
            settles,
            "{what}: settling must be exactly '{settles}'"
        );
        assert_eq!(
            owes_nothing(&recorded(&job_id, &connection)),
            settles,
            "{what}: and the row must agree with it — a discharged record owes nothing and is \
             kept, rather than being deleted"
        );
    }
}

/// The termination witness cannot be built from a scheduler that cannot report one.
///
/// The unit-level half of the row above, on the one function that mints an
/// [`ObservedTermination`]. It is the reason there is no route in the crate by which a failed
/// listing becomes a settlement: not a rule the caller follows, but a value it cannot obtain.
#[tokio::test]
async fn no_termination_witness_exists_without_an_authoritative_listing() {
    for lists in [Lists::Fails, Lists::Untracked] {
        let scheduler = TestScheduler::shared(lists);
        assert!(
            observe_terminations(&scheduler, JOB, GENERATION, &[WORKER])
                .await
                .is_err(),
            "a scheduler that cannot say produces no witness at all"
        );
    }
    let scheduler = TestScheduler::shared(Lists::Live(vec![(GENERATION, WORKER)]));
    assert_eq!(
        observe_terminations(&scheduler, JOB, GENERATION, &[WORKER])
            .await
            .expect("a tracking scheduler answers"),
        Vec::new(),
        "a generation the scheduler lists is not terminated"
    );
    assert_eq!(
        observe_terminations(&scheduler, JOB, GENERATION + 1, &[WORKER])
            .await
            .expect("a tracking scheduler answers")
            .iter()
            .map(|t| (t.worker(), t.generation()))
            .collect::<Vec<_>>(),
        vec![(WORKER, GENERATION + 1)],
        "and a generation it does not list is, named by the generation asked about rather than \
         by the one that is live — a reused endpoint is a different target"
    );
}

// ---------------------------------------------------------------------------------------------
// Idempotence, conditional re-adoption, and the partition
// ---------------------------------------------------------------------------------------------

/// Running the recovery pass again says what the first one said, and leaves the row where the
/// first one left it.
///
/// Idempotent **by construction**: every step is a function of the durable record and of what
/// this pass observed, a target only ever leaves `Pending`, and the write replaces the whole
/// record rather than incrementing anything. So this row is not "retry twice and hope" — it is
/// the statement that there is nothing in the pass that has to happen exactly once.
///
/// Three passes, not two, and the middle one settles nothing so that the pass *after* a
/// no-progress pass is covered as well: a controller killed and restarted repeatedly against an
/// unreachable target must find the record readable and the job recoverable every time.
#[tokio::test]
async fn repeated_recovery_passes_reconcile_idempotently() {
    let (job_id, db, connection) = job("idempotent");
    let seeded = obligation(None);
    seed_obligation(&job_id, &connection, Some(&seeded));
    let scheduler = TestScheduler::shared(Lists::Live(vec![(GENERATION, WORKER)]));

    let mut rows = Vec::new();
    for pass in 0..3 {
        // A fresh status each time, read from the row: this is a *restarted* controller, not the
        // same one going round a loop, and re-adoption is part of what it repeats.
        let mut status = cold_status(&db).await;
        adopt(&mut status, &db).await;
        let discharge = discharge_recorded_obligation(
            &mut status,
            &db,
            &scheduler,
            LifecycleMode::FencedV2,
            DischargeReason::SupersedingTheGenerationsItNames,
        )
        .await;
        assert!(
            matches!(
                discharge,
                Discharge::StillPending {
                    pending: 1,
                    outstanding_attempts: 1
                }
            ),
            "pass {pass}: the same answer every time"
        );
        rows.push(recorded(&job_id, &connection).expect("the obligation stands"));
    }
    assert_eq!(rows[0], rows[1], "pass 2 left the record pass 1 left");
    assert_eq!(rows[1], rows[2], "and so did pass 3");
    assert_eq!(
        rows[0].fencing_since_millis(),
        seeded.fencing_since_millis(),
        "including the origin, which is carried forward rather than restamped — a job that has \
         been fencing for an hour across three controller restarts has been fencing for an hour"
    );
}

/// A controller that loses the re-adoption publishes nothing about the obligation.
///
/// The conditional half of "re-adopts authority conditionally". Two controllers read the same
/// row; one adopts and the other's later adoption matches nothing, so the second holds an
/// authority the row no longer carries. Its recovery pass must not write — not the record, not a
/// cleared record — because a controller that has lost the job cannot say anything about what
/// that job's workers owe.
#[tokio::test]
async fn a_recovery_that_loses_its_re_adoption_publishes_nothing() {
    let (job_id, db, connection) = job("lost-re-adoption");
    seed_obligation(&job_id, &connection, Some(&obligation(None)));

    // The reachable race, and the only one: the preamble discharges *after* it has adopted, so
    // a controller reaches this line holding an authority it won. What supersedes it is a second
    // controller adopting in the window between the two.
    let mut first = cold_status(&db).await;
    adopt(&mut first, &db).await;
    let mut second = cold_status(&db).await;
    adopt(&mut second, &db).await;
    assert!(
        first.authority().fence().get() < second.authority().fence().get(),
        "the second adoption raised the fence past the first controller's"
    );

    // The loser tries to discharge anyway. Everything it would have settled, it settles: the
    // scheduler reports the generation gone. The write is what refuses it.
    let scheduler = TestScheduler::shared(Lists::Live(vec![]));
    let discharge = discharge_recorded_obligation(
        &mut first,
        &db,
        &scheduler,
        LifecycleMode::FencedV2,
        DischargeReason::SupersedingTheGenerationsItNames,
    )
    .await;
    assert!(
        matches!(discharge, Discharge::Superseded(_)),
        "losing the row is reported as what it is, and not as a failure of the job"
    );
    assert!(
        recorded(&job_id, &connection).is_some(),
        "and the obligation is untouched: the loser's clearing write matched no row, which is \
         what stops a superseded controller from telling the live one its workers have answered"
    );
}

/// A permanently unobservable partition leaves the job's obligation pending, forever, and says
/// so (M11.D39g).
///
/// The declared liveness result, driven against a worker that is reachable enough to fail and
/// never settles anything — `Unavailable` to every attempt, which the handshake retries within
/// the same budget the fan-out uses and then reports as *unsettled*, never as an answer.
///
/// **There is no timeout here and this row cannot be made to pass by adding one.** What it
/// asserts is that after the whole retry budget has been spent the target is still `Pending` in
/// the row, and that a second pass — a controller that has restarted since — finds exactly that.
#[tokio::test]
async fn a_permanent_partition_leaves_the_obligation_pending_and_publishes_it() {
    let (job_id, db, connection) = job("partition");
    let (address, directives) = serve(Answers::NeverSettling).await;
    seed_obligation(&job_id, &connection, Some(&obligation(Some(address))));
    let scheduler = TestScheduler::shared(Lists::Live(vec![(GENERATION, WORKER)]));

    for pass in 0..2 {
        let mut status = cold_status(&db).await;
        adopt(&mut status, &db).await;
        let discharge = discharge_recorded_obligation(
            &mut status,
            &db,
            &scheduler,
            LifecycleMode::FencedV2,
            DischargeReason::SupersedingTheGenerationsItNames,
        )
        .await;
        assert!(
            matches!(discharge, Discharge::StillPending { pending: 1, .. }),
            "pass {pass}: an unobservable target is not settled by the retry budget running out"
        );
        assert_eq!(
            recorded(&job_id, &connection)
                .expect("the obligation stands")
                .targets()[0]
                .state,
            FenceTargetState::Pending,
            "pass {pass}: and the row still says the target owes an acknowledgement"
        );
    }
    let seen = directives.seen.lock().unwrap().len();
    assert!(
        seen >= 2,
        "the controller kept trying to fence it — {seen} directives — rather than deciding it \
         had answered"
    );
}

/// The recovered obligation's target is matched on the worker **and** the generation.
///
/// Endpoint reuse (M11.D39d/M11.D39g). An answer from worker 7 in generation 5 says nothing
/// about worker 7 in generation 4, and neither does a termination of it: the successor at the
/// same address is a different target, and settling the predecessor's obligation on its
/// successor's answer is how a delayed `StartExecution` becomes applicable again.
#[test]
fn an_answer_from_a_successor_generation_settles_nothing_of_its_predecessors() {
    let mut recovered = RecoveredObligation::of(&obligation(None), 2);
    assert!(
        !recovered.observe_acknowledgement(&FenceAcknowledgement::reported(
            WORKER,
            GENERATION + 1,
            9
        )),
        "an acknowledgement from the successor generation changes nothing"
    );
    assert!(
        !recovered.observe_termination(&ObservedTermination::observed(WORKER, GENERATION + 1)),
        "and neither does a termination of it"
    );
    assert!(
        !recovered.observe_acknowledgement(&FenceAcknowledgement::reported(
            WorkerId(WORKER.0 + 1),
            GENERATION,
            9
        )),
        "nor does an answer from a worker this obligation never addressed"
    );
    assert_eq!(
        recovered.states(),
        vec![(WORKER.0, GENERATION, FenceTargetState::Pending)],
        "the recorded target is exactly as pending as it was"
    );

    // The positive control, so the three negatives above are about the identity and not about a
    // value that refuses everything.
    assert!(
        recovered.observe_acknowledgement(&FenceAcknowledgement::reported(WORKER, GENERATION, 9)),
        "the same acknowledgement, at the generation the obligation names, settles it"
    );
    assert_eq!(
        recovered.states(),
        vec![(WORKER.0, GENERATION, FenceTargetState::Acknowledged)]
    );
}

/// An acknowledgement at or below what the obligation issued under settles nothing.
///
/// The *height* half of the four-part identity, driven against the value itself. On the
/// production path this check cannot fire, and that is deliberate rather than accidental:
/// `advance_fence_each` only reports an acknowledgement whose observed fence is at or above the
/// fence it addressed — `status.authority().fence()` — and this obligation is measured against
/// one below that. The two checks are a pair, and this row is what stops them from drifting
/// apart: a change that lowered the fence the recovery pass addresses, or raised the bound the
/// obligation carries, would make the second one load-bearing, and it has to already work.
#[test]
fn an_acknowledgement_that_does_not_supersede_what_the_obligation_issued_settles_nothing() {
    // Adopted at fence 6, so the identifiers in the record were issued under at most fence 5.
    let mut recovered = RecoveredObligation::of(&obligation(None), 6);
    for at in [4, 5] {
        assert!(
            !recovered
                .observe_acknowledgement(&FenceAcknowledgement::reported(WORKER, GENERATION, at)),
            "a generation holding fence {at} has revoked nothing this obligation issued"
        );
        assert_eq!(
            recovered.states(),
            vec![(WORKER.0, GENERATION, FenceTargetState::Pending)],
            "so the target is exactly as pending as it was"
        );
    }
    assert!(
        recovered.observe_acknowledgement(&FenceAcknowledgement::reported(WORKER, GENERATION, 6)),
        "the first fence strictly above the bound is the one that revokes what was issued"
    );
    assert_eq!(
        recovered.states(),
        vec![(WORKER.0, GENERATION, FenceTargetState::Acknowledged)]
    );
}

/// A recorded address that is not an address at all leaves its target pending.
///
/// The third route to the partition outcome, and the one a corrupted or forward-versioned row
/// takes: `a_permanent_partition_leaves_the_obligation_pending_and_publishes_it` reaches it by
/// dialling a socket nobody is listening on, and this one by never getting as far as a socket.
/// Neither is settlement — an address this controller cannot parse says nothing about whether
/// the generation behind it is still running — so the record must come back unchanged rather
/// than cleared.
#[tokio::test]
async fn a_recorded_address_that_is_not_an_address_leaves_its_target_pending() {
    let (job_id, db, connection) = job("unparseable-address");
    seed_obligation(
        &job_id,
        &connection,
        Some(&obligation(Some("this is not a url".to_string()))),
    );
    let mut status = cold_status(&db).await;
    adopt(&mut status, &db).await;

    // The scheduler still lists the generation, so the other route to settlement is closed too.
    let live = TestScheduler::shared(Lists::Live(vec![(GENERATION, WORKER)]));
    let discharge = discharge_recorded_obligation(
        &mut status,
        &db,
        &live,
        LifecycleMode::FencedV2,
        DischargeReason::SupersedingTheGenerationsItNames,
    )
    .await;
    assert!(
        matches!(
            discharge,
            Discharge::StillPending {
                pending: 1,
                outstanding_attempts: 1
            }
        ),
        "an address that cannot become an endpoint is a target this controller cannot fence"
    );
    assert_eq!(
        recorded(&job_id, &connection)
            .expect("the obligation stands")
            .targets()[0]
            .state,
        FenceTargetState::Pending,
        "and the row still carries it, unchanged, for the next pass"
    );
}

/// A pass run before the adoption that gives it a fence is unusable, and writes nothing.
///
/// The preamble adopts first — `the_preamble_adopts_before_every_other_effect_and_roots_last` is
/// where that order is pinned — and this is what makes the order enforced rather than
/// remembered. A controller holding the unadopted authority has no fence to advance at the
/// recorded generations, so there is no request it could send that would revoke anything; the
/// honest answer is that the pass could not run, and the record is left exactly as it was so the
/// next one repeats it.
#[tokio::test]
async fn a_recovery_pass_without_an_adopted_fence_is_unusable_and_writes_nothing() {
    let (job_id, db, connection) = job("unadopted-recovery");
    // Built once and compared against itself: the fixture stamps the obligation's origin from
    // the clock, so two calls differ by whatever milliseconds passed between them.
    let seeded = obligation(None);
    seed_obligation(&job_id, &connection, Some(&seeded));
    // Deliberately no `adopt`: the status carries the authority the row was read with, and the
    // row has never been adopted, so its fence is the column's default.
    let mut status = cold_status(&db).await;
    assert_eq!(status.authority().fence().get(), 0);

    let live = TestScheduler::shared(Lists::Live(vec![(GENERATION, WORKER)]));
    let discharge = discharge_recorded_obligation(
        &mut status,
        &db,
        &live,
        LifecycleMode::FencedV2,
        DischargeReason::SupersedingTheGenerationsItNames,
    )
    .await;
    let Discharge::Unusable(failure) = discharge else {
        panic!("a pass with no fence to advance cannot report anything about the obligation");
    };
    assert_eq!(
        failure.to_string(),
        format!(
            "job {job_id} cannot advance a lifecycle fence over its recovered obligation: job \
             {job_id} carries no adopted lifecycle fence, so this controller cannot address its \
             worker generations under one"
        ),
        "the report names the job and why it holds no fence"
    );
    assert_eq!(
        metrics::errors(&job_id, FencingError::Unrecordable),
        1,
        "and it is counted where an operator groups errors, rather than passing silently"
    );
    assert_eq!(
        recorded(&job_id, &connection),
        Some(seeded),
        "the record is left exactly as it was: this pass repeats rather than continues"
    );
}

/// A row that will not take the updated record is unusable, and settles nothing.
///
/// The last step of the pass. Everything before it may have settled targets in memory —
/// acknowledgements observed, terminations observed — and none of that is settlement until the
/// row says so: this controller's own belief about a target is not what a *replacement*
/// controller reads. So a write that could not be performed leaves the record exactly as the
/// previous pass left it and the attempt may not continue, rather than reporting the discharge
/// its in-memory obligation would justify.
#[tokio::test]
async fn a_row_that_will_not_take_the_updated_record_is_unusable_and_settles_nothing() {
    let (job_id, db, connection) = job("unwritable-row");
    seed_obligation(&job_id, &connection, Some(&obligation(None)));
    let mut status = cold_status(&db).await;
    adopt(&mut status, &db).await;

    // The scheduler says the generation is gone, so the pass *would* settle the only target and
    // clear the record — which is exactly what must not be reported when the write fails.
    let gone = TestScheduler::shared(Lists::Live(vec![]));
    connection
        .lock()
        .unwrap()
        .execute_batch("DROP TABLE job_statuses;")
        .expect("the fixture must be editable");

    let discharge = discharge_recorded_obligation(
        &mut status,
        &db,
        &gone,
        LifecycleMode::FencedV2,
        DischargeReason::SupersedingTheGenerationsItNames,
    )
    .await;
    let Discharge::Unusable(failure) = discharge else {
        panic!("a record that could not be written must not be reported as settled");
    };
    assert!(
        matches!(&failure, RecoveryFailure::NotWritten { job_id: named, .. } if *named == job_id),
        "the failure names the job whose row refused the write: {failure}"
    );
    assert_eq!(
        metrics::errors(&job_id, FencingError::PublicationFailed),
        1,
        "and it is counted as a publication failure rather than as an unrecordable obligation"
    );
    assert_eq!(
        metrics::errors(&job_id, FencingError::Unrecordable),
        0,
        "which is a different reason with a different next step"
    );
}

/// Every fencing error an operator can group by has a label of its own.
///
/// The labels are a closed list with no `other` bucket, and two errors sharing one would merge
/// groups an operator uses to tell "a target refused the fence" from "the row would not take the
/// record" — opposite situations with opposite next steps.
#[test]
fn every_fencing_error_carries_its_own_operator_label() {
    let labels: Vec<&str> = [
        FencingError::Unrecordable,
        FencingError::NotAcknowledged,
        FencingError::TerminationUnobservable,
        FencingError::PublicationFailed,
    ]
    .into_iter()
    .map(FencingError::as_str)
    .collect();
    assert_eq!(
        labels,
        vec![
            "unrecordable",
            "not_acknowledged",
            "termination_unobservable",
            "publication_failed",
        ]
    );
    let mut distinct = labels.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        labels.len(),
        "no two fencing errors share a label"
    );
}

/// Advancing a target is monotone, and a termination outranks an acknowledgement.
///
/// The property idempotence rests on: there is no operation that returns a target to `Pending`,
/// so a record that has settled a target stays settled however many passes run over it. A
/// generation that acknowledged and then went away is *gone* — the stronger of the two facts —
/// which is the same precedence M11.T25's `FenceTargets::terminate` has.
#[test]
fn a_recovered_target_only_ever_leaves_pending() {
    let mut recovered = RecoveredObligation::of(&obligation(None), 2);
    assert!(
        recovered.observe_acknowledgement(&FenceAcknowledgement::reported(WORKER, GENERATION, 9))
    );
    assert!(
        !recovered.observe_acknowledgement(&FenceAcknowledgement::reported(WORKER, GENERATION, 9)),
        "a second acknowledgement is not a second event"
    );
    assert!(
        recovered.observe_termination(&ObservedTermination::observed(WORKER, GENERATION)),
        "a termination of an acknowledged target is still news: it is the stronger fact"
    );
    assert!(
        !recovered.observe_acknowledgement(&FenceAcknowledgement::reported(WORKER, GENERATION, 9)),
        "and nothing takes a terminated target back"
    );
    assert_eq!(
        recovered.states(),
        vec![(WORKER.0, GENERATION, FenceTargetState::Terminated)]
    );
    assert!(
        recovered.record().is_some_and(|record| record
            .targets()
            .iter()
            .all(|target| target.state == FenceTargetState::Terminated
                && target.attempt_id.is_none())),
        "a record with nothing pending owes nothing, and is rewritten rather than cleared: what \
         it still names is the generation, which is what a later refusal or replacement fences"
    );
}

// ---------------------------------------------------------------------------------------------
// Metrics and the operator-visible alert
// ---------------------------------------------------------------------------------------------

/// The alert's whole lifecycle: raised, sustained, sustained again, cleared, and quiet.
///
/// M11.D39g chooses safety over this job's availability and accepts an unbounded wait, so the
/// wait has to be visible — and *clearing* has to be visible too, or an operator cannot tell a
/// job that recovered from one that nobody looked at again. The transitions are values rather
/// than log lines precisely so this row can assert the whole sequence in closed form.
///
/// The gauges are asserted beside them because the alert and the numbers are one report: an
/// alert raised with a pending-target count of zero would be an alert about nothing.
#[test]
fn the_fencing_alert_runs_its_whole_lifecycle() {
    let job_id = format!("{JOB}-alert-lifecycle");
    let report = metrics::FencingReport {
        pending_targets: 2,
        outstanding_attempts: 1,
        age: Some(Duration::from_secs(3_600)),
    };

    assert_eq!(
        metrics::alert_settled(&job_id),
        AlertTransition::Quiet,
        "a job nobody has raised an alert for is quiet, not cleared"
    );
    assert_eq!(metrics::published(&job_id), (0, 0, 0, 0));

    assert_eq!(
        metrics::alert_pending(&job_id, report),
        AlertTransition::Raised,
        "the first unsettled pass raises it"
    );
    assert_eq!(
        metrics::published(&job_id),
        (2, 1, 3_600, 1),
        "with the pending targets, the outstanding identifiers, the age in seconds and the \
         alert itself"
    );

    for pass in 0..2 {
        assert_eq!(
            metrics::alert_pending(&job_id, report),
            AlertTransition::Sustained,
            "pass {pass}: a job that is still fencing sustains it rather than raising it again"
        );
    }
    assert_eq!(metrics::published(&job_id), (2, 1, 3_600, 1));

    assert_eq!(
        metrics::alert_settled(&job_id),
        AlertTransition::Cleared,
        "and settling clears it"
    );
    assert_eq!(
        metrics::published(&job_id),
        (0, 0, 0, 0),
        "with every number it raised the alert about going back to zero — the series stays, \
         because an alert that disappeared would be indistinguishable from a job nobody is \
         reporting on"
    );
    assert_eq!(
        metrics::alert_settled(&job_id),
        AlertTransition::Quiet,
        "clearing a cleared alert is not a second clearing"
    );
}

/// An obligation with no recorded origin reports no age, and a clock that has gone backwards
/// does not report a negative one.
///
/// The two ways the age can be unusable. Both answer `None` rather than zero, because zero reads
/// as *"this job has just started fencing"* about a job that may have been fencing for a week —
/// which is the one thing the age metric exists to make visible.
#[test]
fn an_unusable_fencing_origin_reports_no_age_rather_than_no_time() {
    assert_eq!(metrics::age_of(None), None, "no origin, no age");
    let now = metrics::now_millis().expect("the host clock is after the epoch");
    assert_eq!(
        metrics::age_of(Some(now + 60_000)),
        None,
        "an origin in the future is a clock disagreement, not an age of minus a minute"
    );
    let hour = metrics::age_of(Some(now - 3_600_000)).expect("an origin in the past has an age");
    assert!(
        hour >= Duration::from_secs(3_600) && hour < Duration::from_secs(3_610),
        "and an origin an hour ago is an hour ago: {hour:?}"
    );
}

/// A discharge that settles publishes a settlement; one that cannot publishes an error and the
/// alert.
///
/// The wiring, on its own job so the series are this row's. Both of M11.D39e(v)'s non-response
/// facts are counted separately — an operator distinguishing "the workers acknowledged" from
/// "the workers went away" is distinguishing a fence that worked from a cluster that died — and
/// the error counter names why a pass could not settle.
#[tokio::test]
async fn a_discharge_publishes_its_settlements_its_errors_and_its_alert() {
    // First, a pass that cannot settle: the scheduler still lists the generation and the target
    // has no address, so neither fact is observed.
    let (unsettled_id, db, connection) = job("metrics-unsettled");
    seed_obligation(&unsettled_id, &connection, Some(&obligation(None)));
    let mut status = cold_status(&db).await;
    adopt(&mut status, &db).await;
    let live = TestScheduler::shared(Lists::Live(vec![(GENERATION, WORKER)]));
    let _pending = discharge_recorded_obligation(
        &mut status,
        &db,
        &live,
        LifecycleMode::FencedV2,
        DischargeReason::SupersedingTheGenerationsItNames,
    )
    .await;
    let (pending_targets, outstanding, age, alert) = metrics::published(&unsettled_id);
    assert_eq!(
        (pending_targets, outstanding, alert),
        (1, 1, 1),
        "one target, one identifier, and the alert raised"
    );
    assert!(
        age >= 3_600,
        "with the age measured from the obligation's own origin: {age}s"
    );
    assert_eq!(
        metrics::settlements(&unsettled_id, Accounting::TerminatedGeneration),
        0,
        "and nothing was settled"
    );

    // Then a pass that settles it: the scheduler no longer lists the generation.
    let gone = TestScheduler::shared(Lists::Live(vec![]));
    let mut status = cold_status(&db).await;
    adopt(&mut status, &db).await;
    assert!(matches!(
        discharge_recorded_obligation(
            &mut status,
            &db,
            &gone,
            LifecycleMode::FencedV2,
            DischargeReason::SupersedingTheGenerationsItNames,
        )
        .await,
        Discharge::Settled
    ));
    assert_eq!(
        metrics::settlements(&unsettled_id, Accounting::TerminatedGeneration),
        1,
        "the settlement is counted under the fact that produced it"
    );
    assert_eq!(
        metrics::settlements(&unsettled_id, Accounting::AcknowledgedFence),
        0,
        "and not under the other one"
    );
    assert_eq!(
        metrics::published(&unsettled_id),
        (0, 0, 0, 0),
        "and the alert is cleared with every number it was raised about"
    );

    // And the error a scheduler that cannot answer produces.
    let (untracked_id, db, connection) = job("metrics-untracked");
    seed_obligation(&untracked_id, &connection, Some(&obligation(None)));
    let mut status = cold_status(&db).await;
    adopt(&mut status, &db).await;
    let untracked = TestScheduler::shared(Lists::Untracked);
    let _pending = discharge_recorded_obligation(
        &mut status,
        &db,
        &untracked,
        LifecycleMode::FencedV2,
        DischargeReason::SupersedingTheGenerationsItNames,
    )
    .await;
    assert_eq!(
        metrics::errors(&untracked_id, FencingError::TerminationUnobservable),
        1,
        "a scheduler that cannot report a termination is an error an operator can group by, \
         and not a silent reason the job never leaves Fencing"
    );
    assert_eq!(
        metrics::errors(&untracked_id, FencingError::NotAcknowledged),
        0,
        "and it is not confused with a generation that refused the fence"
    );
}

// ---------------------------------------------------------------------------------------------
// The pre-flag-day peer's answer, and topology-independence
// ---------------------------------------------------------------------------------------------

/// Under `LegacyT08` nothing is recovered and nothing is written, whatever the row carries.
///
/// The pre-flag-day peer's answer at the level of the mechanism: the pass is handed a row with a
/// full obligation in it and a scheduler that would settle it, and it answers
/// [`Discharge::Inactive`] without reading either. The other direction — that a *legacy*
/// scheduling attempt writes no record in the first place — is
/// `a_legacy_scheduling_attempt_records_no_durable_fencing_obligation`, in `states/mod.rs`,
/// where the landed body is.
///
/// The mode's own answer is asserted as a co-occurrence rather than one arm at a time, for the
/// reason `the_production_status_write_is_conditional_since_the_activation_change` gives: a
/// half-applied activation must fail as one assertion instead of reading as a stale test. Since
/// M11.T26h the selected half of that co-occurrence is the *recovering* one, which is what makes
/// this row a statement about a peer rather than about production.
#[tokio::test]
async fn the_legacy_mechanism_neither_writes_nor_recovers_a_durable_fencing_obligation() {
    assert_eq!(
        (
            LifecycleMode::LegacyT08.recovers_a_durable_fencing_obligation(),
            LifecycleMode::FencedV2.recovers_a_durable_fencing_obligation(),
        ),
        (false, true),
        "exactly one of the two modes recovers a durable obligation, and since M11.T26h it is \
         the selected one"
    );
    assert!(
        LifecycleMode::SELECTED.recovers_a_durable_fencing_obligation(),
        "and since M11.T26h the mode a production controller runs under is the one that does"
    );

    let (job_id, db, connection) = job("legacy-inactive");
    let seeded = obligation(None);
    seed_obligation(&job_id, &connection, Some(&seeded));
    let mut status = cold_status(&db).await;
    adopt(&mut status, &db).await;
    // A scheduler that would settle every target, so an inactive pass cannot be mistaken for one
    // that ran and found nothing.
    let gone = TestScheduler::shared(Lists::Live(vec![]));
    assert!(
        matches!(
            discharge_recorded_obligation(
                &mut status,
                &db,
                &gone,
                LifecycleMode::LegacyT08,
                DischargeReason::SupersedingTheGenerationsItNames,
            )
            .await,
            Discharge::Inactive
        ),
        "the legacy mechanism does not discharge an obligation, even one that would settle"
    );
    assert_eq!(
        recorded(&job_id, &connection),
        Some(seeded),
        "and it writes nothing: the row carries exactly what it carried"
    );
    assert!(
        status.recorded_fencing().is_some(),
        "nor is anything staged on the status, which is what the next publication would carry"
    );
}

/// The recovery path reads no controller-topology knob, so it is one path in both topologies.
///
/// A source pin, and the name says so. `config().job_controller` is process-global and this
/// suite runs at up to sixteen threads, so flipping it to run these rows twice would silently
/// change which branch every concurrently running scheduling row took — the reason
/// `PhaseContext::run_as_leader_on` exists. What can be established instead is the stronger
/// statement: there is no branch to run twice. Neither the recovery pass nor the durable
/// projection mentions the knob, the mode it produces, or the `leader_mode` derived from it.
///
/// The topology-dependent rows are the ones that reach the *scheduling* path, and those are in
/// `states/mod.rs` beside the fixtures that can drive it.
#[test]
fn the_recovery_path_reads_no_controller_topology() {
    for (name, source) in [
        ("recovery.rs", include_str!("recovery.rs")),
        (
            "recovery/recovered.rs",
            include_str!("recovery/recovered.rs"),
        ),
        ("fence/obligation.rs", include_str!("fence/obligation.rs")),
        ("fence/metrics.rs", include_str!("fence/metrics.rs")),
    ] {
        for knob in ["job_controller", "JobControllerMode", "leader_mode"] {
            assert_eq!(
                source.matches(knob).count(),
                0,
                "{name} must not read `{knob}`: a recovery that behaved differently in the two \
                 controller topologies would be two mechanisms, and only one of them would be \
                 covered by the row that ran"
            );
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Failure paths: fail-closed projection, and a pass that is cancelled
// ---------------------------------------------------------------------------------------------

/// An obligation that cannot be described durably is refused, not truncated.
///
/// Fail-closed, in the one direction that matters: a record that dropped a target because the
/// collection was full would name fewer worker generations than the attempt addressed, and it
/// would read as **settled** to the controller that picked it up. So both refusals are checked —
/// the capacity, and the disagreement between the target set and the inventory about which
/// generation was addressed — and the positive control below is what makes them refusals of
/// something the projection would otherwise have produced.
#[test]
fn the_durable_projection_fails_closed_rather_than_truncating() {
    use crate::states::scheduling::fanout::IssuedAttempts;
    use crate::states::scheduling::fencing::FenceTargets;
    use arroyo_rpc::fencing::MAX_FENCE_TARGETS;

    let addresses = std::collections::HashMap::new();
    let inventory = IssuedAttempts::issued_under(GENERATION, 1);

    // The positive control: at capacity, the projection produces a record.
    let at_capacity = FenceTargets::for_workers((0..MAX_FENCE_TARGETS as u64).map(WorkerId));
    let described = super::fence::obligation::describe(
        GENERATION,
        &at_capacity,
        &inventory,
        &addresses,
        None,
        None,
    )
    .expect("an obligation at capacity is describable")
    .expect("and it owes something");
    assert_eq!(described.targets().len(), MAX_FENCE_TARGETS);

    // One more, and it is refused rather than shortened.
    let over = FenceTargets::for_workers((0..MAX_FENCE_TARGETS as u64 + 1).map(WorkerId));
    let refused =
        super::fence::obligation::describe(GENERATION, &over, &inventory, &addresses, None, None)
            .expect_err("an obligation past capacity is not describable");
    assert!(
        refused.to_string().contains("more than the"),
        "and the refusal says which rule it broke: {refused}"
    );

    // And an inventory that addressed some other generation is refused before anything is
    // assembled out of the two: the identifiers, the generation they went to and the authority
    // they were issued under are one fact.
    let mut elsewhere = IssuedAttempts::issued_under(GENERATION + 1, 1);
    elsewhere.issued(WORKER, ATTEMPT.to_string());
    let refused = super::fence::obligation::describe(
        GENERATION,
        &FenceTargets::for_workers([WORKER]),
        &elsewhere,
        &addresses,
        None,
        None,
    )
    .expect_err("a mismatched inventory is not describable");
    assert!(
        refused
            .to_string()
            .contains("while the identifiers it issued"),
        "and it says which two disagreed: {refused}"
    );

    // The control for *that*: the same inventory, at the generation the targets name.
    let mut here = IssuedAttempts::issued_under(GENERATION, 1);
    here.issued(WORKER, ATTEMPT.to_string());
    assert!(
        super::fence::obligation::describe(
            GENERATION,
            &FenceTargets::for_workers([WORKER]),
            &here,
            &addresses,
            None,
            None,
        )
        .is_ok(),
        "so the refusal above is about the disagreement and not about the shape"
    );
}

/// A recovery pass dropped part-way leaves the record readable and the job recoverable.
///
/// Cancellation is a failure path, and this is the one that matters most: a controller killed
/// while it is advancing a fence. The pass is driven until a directive is *in flight at the
/// worker* and then dropped, which is what a `SIGKILL` between the two looks like from the row's
/// point of view.
///
/// Nothing is left half-written, and the reason is structural rather than careful: the pass
/// writes exactly once, at the end, and everything before that is in memory. So the row after a
/// cancellation is the row before it — and the next pass is a repeat rather than a repair.
#[tokio::test]
async fn a_recovery_pass_dropped_part_way_leaves_the_record_readable_and_recoverable() {
    let (job_id, db, connection) = job("cancelled-mid-advance");
    let paused = Arc::new(Paused::default());
    let (address, _directives) = serve(Answers::Pausing(Arc::clone(&paused))).await;
    let seeded = obligation(Some(address));
    seed_obligation(&job_id, &connection, Some(&seeded));

    {
        let mut status = cold_status(&db).await;
        adopt(&mut status, &db).await;
        let scheduler = TestScheduler::shared(Lists::Live(vec![(GENERATION, WORKER)]));
        let mut pass = Box::pin(discharge_recorded_obligation(
            &mut status,
            &db,
            &scheduler,
            LifecycleMode::FencedV2,
            DischargeReason::SupersedingTheGenerationsItNames,
        ));
        tokio::select! {
            _ = &mut pass => panic!("the paused worker never answers, so the pass cannot finish"),
            _ = paused.asked.notified() => {}
        }
        // Dropped here, with a fence directive outstanding at the worker.
    }

    assert_eq!(
        recorded(&job_id, &connection),
        Some(seeded),
        "the row is exactly what it was: a cancelled pass writes nothing, because the one write \
         it performs is the last thing it does"
    );

    // And the job is recoverable again, by a controller that has restarted since. The worker is
    // released first, so the second pass discharges the obligation by the acknowledgement the
    // cancelled one never got to hear — the same directive, at the same target, answered.
    paused.released.notify_waiters();
    paused.released.notify_one();
    let mut status = cold_status(&db).await;
    adopt(&mut status, &db).await;
    let live = TestScheduler::shared(Lists::Live(vec![(GENERATION, WORKER)]));
    assert!(
        matches!(
            discharge_recorded_obligation(
                &mut status,
                &db,
                &live,
                LifecycleMode::FencedV2,
                DischargeReason::SupersedingTheGenerationsItNames,
            )
            .await,
            Discharge::Settled
        ),
        "the next pass reads the record the cancelled one left and discharges it"
    );
    assert!(
        owes_nothing(&recorded(&job_id, &connection)),
        "and leaves it on the row owing nothing, which is what makes the job schedulable again \
         while keeping the generation a later refusal has to fence"
    );
}

/// Only the two witnesses can advance a recovered target, and there is no third way in.
///
/// **A structural source pin, and the name says so.** The behavioural rows above show that a
/// failed listing, an unreachable peer and a retry budget running out settle nothing; what no
/// behavioural row can show is a *fourth* operation added later — an "expire", a "give up after
/// n passes", a "settle if older than" — which is exactly the shape a reviewer under pressure to
/// restore availability would reach for.
///
/// So the mutating surface is enumerated. `advance` is the only thing that moves a target and it
/// is private; the two callers of it each take a witness type with no public constructor. An
/// operation added to this list has to say what observed fact it carries, and M11.D39e(v) allows
/// exactly three.
#[test]
fn nothing_but_the_two_witnesses_can_advance_a_recovered_target() {
    let source = include_str!("recovery/recovered.rs");
    let at = source
        .find("impl RecoveredObligation {")
        .expect("the recovered obligation has been renamed");
    let body = &source[at..source[at..]
        .find("\n}\n")
        .map(|end| at + end)
        .expect("unterminated impl")];

    let mutating: Vec<&str> = body
        .match_indices("fn ")
        .map(|(i, m)| {
            let rest = &body[i + m.len()..];
            &rest[..rest
                .find(')')
                .map(|end| end + 1)
                .expect("a method has arguments")]
        })
        .filter(|signature| signature.contains("&mut self"))
        .collect();
    assert_eq!(
        mutating,
        [
            "acknowledge(&mut self, acknowledgement: &FenceAcknowledgement)",
            "terminate(&mut self, termination: &ObservedTermination)",
            "advance(&mut self, worker: WorkerId, generation: u64, state: FenceTargetState)",
        ],
        "a recovered obligation is advanced by exactly two operations, each taking one of \
         M11.D39e(v)'s observed facts as a value with no public constructor, and by the private \
         `advance` they both go through. There is no operation here that takes a duration, a \
         deadline, a pass count or nothing at all — adding one is the change this pin exists to \
         force a decision about"
    );

    // And the private one is reachable from nowhere else in the crate.
    assert_eq!(
        source.matches(".advance(").count(),
        2,
        "`advance` has exactly the two callers above"
    );
}
