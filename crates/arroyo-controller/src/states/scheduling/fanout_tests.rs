//! What the `StartExecution` fan-out owns, and what its inventory is entitled to say
//! (M11.T25c, design M11.D39b, D96 row 14).
//!
//! These rows drive the landed [`start_execution_on_workers`](super::start_execution_on_workers)
//! against real workers on real sockets, because every claim here is about what an RPC did:
//! which requests exist, when they exist, and what the controller may write down about them.
//!
//! # The claim, stated exactly
//!
//! The fan-out owns its request futures as children — no request is a task of its own — so a
//! client request task cannot silently outlive the phase that issued it. That is **client-task
//! ownership** and it is the whole claim. It is not cancellation of a worker: a request that
//! reached a worker was decided at the instant it arrived, and nothing the controller does to
//! its own futures can reach that. So the inventory these rows check never records an outcome
//! on the strength of a future having been dropped or a budget having been spent; it records
//! only what a worker answered.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arroyo_rpc::grpc::api;
use arroyo_rpc::grpc::rpc::worker_grpc_server::{WorkerGrpc, WorkerGrpcServer};
use arroyo_rpc::grpc::rpc::{
    CheckpointReq, CheckpointResp, CommitReq, CommitResp, GetWorkerPhaseReq, GetWorkerPhaseResp,
    JobControllerInitReq, JobControllerInitResp, JobFinishedReq, JobFinishedResp,
    LoadCompactedDataReq, LoadCompactedDataRes, MetricsReq, MetricsResp, StartExecutionReq,
    StartExecutionResp, StopExecutionReq, StopExecutionResp,
};
use arroyo_rpc::identity::{WorkerClient, worker_client};
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_types::{MachineId, PipelineId, WorkerId};

use super::ExecutionPlan;
use super::fanout::{AttemptLedger, IssuedAttempts, SettlementBundle, SettlementOwner};
use crate::states::{Admission, RefusalGate};

/// How long a row waits to establish that something has *not* happened.
///
/// The only deadlines here are negatives, which is the one thing no handshake can observe. Both
/// of them are about work that would otherwise complete over loopback in microseconds — a
/// request being sent, or a response being taken up by a task of its own — so reaching this
/// deadline is not a race the fixed code can lose by being slow.
const NOTHING_HAPPENS_GRACE: Duration = Duration::from_millis(400);

/// How many turns a row spends driving the fan-out towards a state it expects to see.
///
/// A bound rather than a wait, so a row that never reaches its state fails with its own
/// assertion instead of hanging the suite.
const MAX_TURNS: usize = 400;

/// One turn of that driving.
const TURN: Duration = Duration::from_millis(10);

/// What a worker was asked to start.
#[derive(Default)]
struct Calls {
    /// The `start_execution_id` of every request that arrived, in arrival order. Recorded
    /// before the worker decides how to answer, so a request that is refused still counts as
    /// one that arrived.
    ids: Mutex<Vec<String>>,
}

impl Calls {
    fn ids(&self) -> Vec<String> {
        self.ids.lock().unwrap().clone()
    }

    /// The identifier of the first request this worker was sent.
    fn first_id(&self) -> String {
        self.ids().first().expect("this worker was asked").clone()
    }
}

/// A worker that has been asked to start and has not answered yet.
#[derive(Default)]
struct Paused {
    /// Fired from inside the handler, so no row has to guess when the request arrived.
    asked: tokio::sync::Notify,
    /// Fired by a row to let the handler answer.
    released: tokio::sync::Notify,
}

/// How a [`TestWorker`] answers `StartExecution`.
#[derive(Clone)]
enum Answers {
    /// Acknowledges at once.
    Accepting,
    /// Announces that it has been asked, then waits to be released before acknowledging.
    Pausing(Arc<Paused>),
    /// Answers with an explicit status the fan-out does not retry — the worker's own decision
    /// about this request, and therefore an outcome.
    Refusing,
    /// Answers `Unavailable` to every attempt, forever, and never applies anything. The shape a
    /// partitioned or half-dead peer presents: reachable, and never an answer.
    NeverSettling,
}

/// A worker, as far as the controller can tell.
struct TestWorker {
    calls: Arc<Calls>,
    answers: Answers,
}

#[tonic::async_trait]
impl WorkerGrpc for TestWorker {
    async fn start_execution(
        &self,
        request: tonic::Request<StartExecutionReq>,
    ) -> Result<tonic::Response<StartExecutionResp>, tonic::Status> {
        self.calls
            .ids
            .lock()
            .unwrap()
            .push(request.into_inner().start_execution_id);
        match &self.answers {
            Answers::Accepting => {}
            Answers::Pausing(paused) => {
                paused.asked.notify_one();
                paused.released.notified().await;
            }
            Answers::Refusing => {
                return Err(tonic::Status::internal(
                    "this worker cannot start executing",
                ));
            }
            Answers::NeverSettling => {
                return Err(tonic::Status::unavailable(
                    "this worker can never settle the request",
                ));
            }
        }
        Ok(tonic::Response::new(StartExecutionResp {}))
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

/// Serves a [`TestWorker`] on a loopback port and returns a client for it.
async fn worker(id: WorkerId, answers: Answers) -> (Arc<Calls>, WorkerClient) {
    let calls = Arc::new(Calls::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(WorkerGrpcServer::new(TestWorker {
                calls: calls.clone(),
                answers,
            }))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    let channel = tonic::transport::Endpoint::from_shared(address)
        .unwrap()
        .timeout(Duration::from_secs(90))
        .connect()
        .await
        .unwrap();
    (calls, worker_client(channel, id))
}

/// The plan every worker of these rows is started with.
///
/// Empty of everything the fan-out does not read: what is under test is the request loop, and
/// the only field any assertion depends on is the `start_execution_id` the loop mints for
/// itself.
fn plan() -> ExecutionPlan {
    ExecutionPlan {
        assignments: vec![],
        program: api::ArrowProgram::default(),
        restore_epoch: None,
        start_epoch: 0,
        min_epoch: 0,
        leader: None,
        checkpoint_manifest_ref: None,
        checkpoint_interval_micros: 0,
        state_backend: StateBackendSelector::Parquet,
    }
}

fn machine_ids(workers: &[WorkerId]) -> HashMap<WorkerId, MachineId> {
    workers
        .iter()
        .map(|id| (*id, MachineId(Arc::new(format!("machine-{}", id.0)))))
        .collect()
}

/// An [`Admission`] taken from a gate the row also holds, so it can ask afterwards whether the
/// job's publication lock is free.
async fn admitted() -> (RefusalGate, Admission) {
    let mut gate = RefusalGate::default();
    let (admission, refusal) = gate.admit_scheduling().await;
    assert!(refusal.is_none(), "a fresh gate has refused nothing");
    (gate, admission)
}

/// Runs the fan-out over `connects` to completion and returns what it settled.
async fn fan_out(
    admission: Admission,
    connects: HashMap<WorkerId, WorkerClient>,
    attempts: Arc<AttemptLedger>,
) -> (Admission, anyhow::Result<HashMap<WorkerId, WorkerClient>>) {
    let ids = machine_ids(&connects.keys().copied().collect::<Vec<_>>());
    super::start_execution_on_workers(
        admission,
        Arc::new("job_1".to_string()),
        PipelineId(Arc::new("pipeline_1".to_string())),
        plan(),
        ids,
        connects,
        attempts,
    )
    .await
}

/// The `StartExecution` requests are children of the fan-out future, and its inventory is a
/// record of what came back (D96 row 14).
///
/// Three things, and they are one property seen from three sides:
///
/// * **Before the fan-out is polled there are no requests.** They are not tasks; they do not
///   exist independently of the future that owns them.
/// * **While the fan-out is not polled, an answer that has arrived is not taken up.** The one
///   thing that could record it is a child of a future nobody is driving. A spawned request
///   would have been driven by the runtime and would have recorded its outcome here — which is
///   exactly the round-10 shape, in which a request outlived the region that authorised it.
/// * **Each attempt is accounted for as its own answer arrives**, not in one stroke when the
///   fan-out returns, and the identifier recorded for it is the one the worker was actually
///   sent.
///
/// What is deliberately *not* asserted anywhere here: that dropping or failing to poll a
/// request stops the worker. It does not, and the fan-out settles rather than cancels for that
/// reason.
#[tokio::test]
async fn fanout_future_owns_all_request_children() {
    let paused = Arc::new(Paused::default());
    let (slow_calls, slow) = worker(WorkerId(1), Answers::Pausing(paused.clone())).await;
    let (quick_calls, quick) = worker(WorkerId(2), Answers::Accepting).await;
    let connects = HashMap::from([(WorkerId(1), slow), (WorkerId(2), quick)]);

    let (gate, admission) = admitted().await;
    let attempts = Arc::new(AttemptLedger::default());
    let requests = fan_out(admission, connects, Arc::clone(&attempts));

    // Nobody has polled it. If any request had a life of its own it would have run by now:
    // the runtime is free, and both peers are one loopback hop away.
    tokio::time::sleep(NOTHING_HAPPENS_GRACE).await;
    assert!(
        slow_calls.ids().is_empty() && quick_calls.ids().is_empty(),
        "an unpolled fan-out has issued nothing: its requests are its children, and a child \
         runs only when its parent is driven"
    );
    assert_eq!(
        attempts.snapshot().issued_count(),
        0,
        "and it has therefore recorded nothing"
    );

    // Drive it until the quick worker's answer is in and the slow worker's request is not.
    tokio::pin!(requests);
    let mut per_attempt = None;
    for _ in 0..MAX_TURNS {
        tokio::select! {
            _ = &mut requests => panic!(
                "the fan-out cannot finish while a worker it is waiting on has not answered"
            ),
            _ = tokio::time::sleep(TURN) => {}
        }
        let issued = attempts.snapshot();
        if issued.issued_count() == 2 && issued.outstanding_count() == 1 {
            per_attempt = Some(issued);
            break;
        }
    }
    let issued = per_attempt.expect(
        "one worker's answer must be accounted for while its sibling is still in flight: an \
         inventory that only settles when the fan-out returns cannot say what is outstanding \
         at the moment it is asked",
    );
    assert_eq!(
        issued.outstanding().map(|(id, _)| id).collect::<Vec<_>>(),
        vec![WorkerId(1)],
        "and what is outstanding is the attempt that has not been answered"
    );

    // Now stop polling and let the slow worker answer. The response arrives; the future that
    // would take it up is a child of a fan-out nobody is driving.
    paused.released.notify_one();
    tokio::time::sleep(NOTHING_HAPPENS_GRACE).await;
    assert_eq!(
        attempts.snapshot().outstanding_count(),
        1,
        "an answer is taken up by the request future that asked for it, and that future is a \
         child of the fan-out. While the fan-out is not polled nothing takes it up — which is \
         what a request of its own would have done"
    );

    let (admission, started) = requests.await;
    assert_eq!(
        started.expect("both workers accepted").len(),
        2,
        "and every request settled once the fan-out was driven to the end"
    );

    let mut expected = IssuedAttempts::default();
    expected.issued(WorkerId(1), slow_calls.first_id());
    expected.issued(WorkerId(2), quick_calls.first_id());
    expected.settled(WorkerId(1));
    expected.settled(WorkerId(2));
    assert_eq!(
        attempts.snapshot(),
        expected,
        "the inventory is one record per target worker, carrying the identifier that worker \
         was actually sent — read back from the workers themselves, so nothing here can be \
         satisfied by an identifier the controller made up"
    );

    drop(admission);
    assert!(
        gate.admit_publication().is_some(),
        "the control: the admission travelled with the fan-out and came back with it"
    );
}

/// An attempt the fan-out gives up on is not an attempt that was answered.
///
/// The terminal path exists so a worker that never settles cannot wedge the job, and what it
/// costs is *knowledge*: the controller stops offering the identifier, and never learns what
/// became of it. The inventory says so. Recording it as settled would be the client-side
/// mistake this whole half exists to avoid — treating the controller's own decision to stop
/// waiting as though it were the worker's decision about the request.
#[tokio::test]
async fn an_abandoned_attempt_stays_outstanding_under_its_own_identifier() {
    let (calls, never) = worker(WorkerId(3), Answers::NeverSettling).await;
    let (gate, admission) = admitted().await;
    let attempts = Arc::new(AttemptLedger::default());

    let (admission, started) = fan_out(
        admission,
        HashMap::from([(WorkerId(3), never)]),
        Arc::clone(&attempts),
    )
    .await;
    assert!(
        started.is_err(),
        "a fan-out no worker ever accepted cannot have succeeded"
    );

    let issued = attempts.snapshot();
    let mut expected = IssuedAttempts::default();
    expected.issued(WorkerId(3), calls.first_id());
    assert_eq!(
        issued, expected,
        "the attempt is still outstanding, under the identifier the worker was sent: the \
         controller gave up learning the outcome, and an inventory that marked this settled \
         would be reporting an answer nobody gave"
    );

    let ids = calls.ids();
    assert_eq!(
        ids.len(),
        super::START_EXECUTION_RECONCILE_ATTEMPTS + 1,
        "the first request plus a bounded number of reconciliation attempts"
    );
    assert!(
        ids.iter().all(|id| *id == ids[0]),
        "every one of them a replay of the same identifier, which is why the inventory is one \
         record however long the budget is"
    );
    assert_eq!(
        issued.issued_count(),
        1,
        "and why it does not grow with the retry count"
    );

    drop(admission);
    assert!(gate.admit_publication().is_some());
}

/// A worker's explicit refusal is an outcome, and settles that worker's attempt alone.
///
/// The other half of what "settled" means. `Aborted` and the ambiguous transport statuses are
/// retried because they say nothing about what was applied; an explicit status like this one is
/// the worker's own decision about the request, so there is nothing left to learn. Its sibling
/// is unaffected: settlement is per attempt, and one worker's answer is not another's.
#[tokio::test]
async fn an_explicitly_refused_attempt_settles_and_its_sibling_is_unaffected() {
    let (refused_calls, refusing) = worker(WorkerId(4), Answers::Refusing).await;
    let (accepted_calls, accepting) = worker(WorkerId(5), Answers::Accepting).await;
    let (gate, admission) = admitted().await;
    let attempts = Arc::new(AttemptLedger::default());

    let (admission, started) = fan_out(
        admission,
        HashMap::from([(WorkerId(4), refusing), (WorkerId(5), accepting)]),
        Arc::clone(&attempts),
    )
    .await;
    assert!(
        started.is_err(),
        "the fan-out reports the first failing worker's error"
    );

    let mut expected = IssuedAttempts::default();
    expected.issued(WorkerId(4), refused_calls.first_id());
    expected.issued(WorkerId(5), accepted_calls.first_id());
    expected.settled(WorkerId(4));
    expected.settled(WorkerId(5));
    assert_eq!(
        attempts.snapshot(),
        expected,
        "both attempts are accounted for: the refusal is an answer about the request, and the \
         sibling was waited for rather than dropped when the refusal landed"
    );
    assert_eq!(
        refused_calls.ids().len(),
        1,
        "and an explicit status is not retried, so the refused worker was asked once"
    );

    drop(admission);
    assert!(gate.admit_publication().is_some());
}

/// A settlement owner that keeps whatever it is handed, so a row can ask what actually moved.
///
/// M11.T25 has no owner of its own — `PhaseContext::settlement_owner` answers `None` — so a
/// double is the only way to observe the seam at all. What it does is what a real one must:
/// it takes the bundle apart and *holds* the authority, rather than dropping it.
#[derive(Default)]
struct RecordingOwner {
    /// The lifecycle authority, held exactly as a real owner would hold it.
    held: Mutex<Option<Admission>>,
    /// The inventory that arrived with it.
    issued: Mutex<Option<IssuedAttempts>>,
}

impl SettlementOwner for RecordingOwner {
    fn take_over(&self, bundle: SettlementBundle) -> Result<(), SettlementBundle> {
        let (admission, issued) = bundle.into_parts();
        *self.issued.lock().unwrap() = Some(issued);
        *self.held.lock().unwrap() = Some(admission);
        Ok(())
    }
}

/// Waits, bounded, for the detached region rescue to have handed the obligation over.
///
/// A bound rather than a wait: a row whose handover never happens fails with its own assertion
/// instead of hanging the suite.
async fn handed_over(owner: &Arc<RecordingOwner>) -> Option<IssuedAttempts> {
    for _ in 0..MAX_TURNS {
        if let Some(issued) = owner.issued.lock().unwrap().clone() {
            return Some(issued);
        }
        tokio::time::sleep(TURN).await;
    }
    None
}

/// A fan-out whose phase is **cancelled** still hands its obligation to the job's settlement
/// owner.
///
/// This is the path an owner exists for, and the one on which no line of the phase runs. When
/// the controller's shutdown token fires the job's state task is dropped as a whole — see
/// `ShutdownGuard::into_spawn_task` — so `StartFanOut::issue` never reaches its own
/// `settlement_owner()` / `hand_over` block: the `await` it is suspended at simply never
/// returns. What survives is the region rescue inside `settle_under_admission`, which holds the
/// admission until the issued requests settle; before this fix it then *dropped* what it had
/// rescued, so an owner received neither the inventory nor the authority on the only path it
/// was built for.
///
/// The three assertions are the three halves of "the obligation moved as one unit", at the
/// three moments they can be made:
///
/// * while the request is unsettled, nothing has been handed over and nothing is publishable;
/// * once the worker answers, the owner has the inventory — with the identifier that worker was
///   actually sent, read back from the worker itself;
/// * and it has the authority too, which is why the gate is still closed until the owner lets
///   it go.
#[tokio::test]
async fn a_cancelled_fan_out_hands_its_obligation_to_the_settlement_owner() {
    let paused = Arc::new(Paused::default());
    let (calls, slow) = worker(WorkerId(8), Answers::Pausing(paused.clone())).await;
    let (gate, admission) = admitted().await;

    let owner = Arc::new(RecordingOwner::default());
    let attempts = Arc::new(AttemptLedger::owned_by(Some(
        Arc::clone(&owner) as Arc<dyn SettlementOwner>
    )));
    let mut requests = Box::pin(fan_out(
        admission,
        HashMap::from([(WorkerId(8), slow)]),
        Arc::clone(&attempts),
    ));

    // Drive it until the worker has been asked and has not answered.
    let mut asked = false;
    for _ in 0..MAX_TURNS {
        tokio::select! {
            _ = &mut requests => panic!("the fan-out cannot finish while the worker is paused"),
            _ = tokio::time::sleep(TURN) => {}
        }
        if !calls.ids().is_empty() {
            asked = true;
            break;
        }
    }
    assert!(
        asked,
        "the fixture's precondition: the request reached the worker"
    );

    // The job's state task is cancelled. Everything the phase would have done next goes with
    // it, including the hand-over it performs on its own return path.
    drop(requests);

    tokio::time::sleep(NOTHING_HAPPENS_GRACE).await;
    assert!(
        owner.issued.lock().unwrap().is_none(),
        "nothing is handed over while a request the fan-out issued is still unsettled: the \
         rescue is holding the authority precisely so that nothing can be published behind it"
    );
    assert!(
        gate.admit_publication().is_none(),
        "and the job's publication lock is still held, by the rescue rather than by the phase"
    );

    // The worker answers. The rescue settles, and the obligation reaches the owner.
    paused.released.notify_one();
    let issued = handed_over(&owner).await.expect(
        "a cancelled fan-out's inventory and its lifecycle authority must reach the job's \
         settlement owner: the phase that would have handed them over no longer exists, so this \
         is the only path left that can",
    );

    let mut expected = IssuedAttempts::default();
    expected.issued(WorkerId(8), calls.first_id());
    expected.settled(WorkerId(8));
    assert_eq!(
        issued, expected,
        "what the owner receives is what the workers answered, under the identifier they were \
         actually sent — the live ledger the rescued region went on writing to, not a summary \
         composed before the cancellation"
    );
    assert!(
        gate.admit_publication().is_none(),
        "and the authority came with it: a refusal is no more publishable now than it was \
         before, because the owner is the one holding the lock"
    );

    drop(owner.held.lock().unwrap().take());
    assert!(
        gate.admit_publication().is_some(),
        "the control — the gate is closed only because the owner was holding the admission it \
         was handed"
    );
}

/// The same cancellation, for a controller with no settlement owner, releases the admission
/// once the requests settle and hands nothing anywhere.
///
/// The control for the row above and the statement that M11.T25's own behaviour is unchanged:
/// every ledger this half builds has no owner, so the rescue does exactly what it has always
/// done. If this row ever stopped passing, the landed M11.T08 path would have acquired a
/// behaviour it did not have.
#[tokio::test]
async fn a_cancelled_fan_out_without_an_owner_releases_its_admission_as_before() {
    let paused = Arc::new(Paused::default());
    let (calls, slow) = worker(WorkerId(9), Answers::Pausing(paused.clone())).await;
    let (gate, admission) = admitted().await;

    let attempts = Arc::new(AttemptLedger::default());
    let mut requests = Box::pin(fan_out(
        admission,
        HashMap::from([(WorkerId(9), slow)]),
        Arc::clone(&attempts),
    ));
    let mut asked = false;
    for _ in 0..MAX_TURNS {
        tokio::select! {
            _ = &mut requests => panic!("the fan-out cannot finish while the worker is paused"),
            _ = tokio::time::sleep(TURN) => {}
        }
        if !calls.ids().is_empty() {
            asked = true;
            break;
        }
    }
    assert!(
        asked,
        "the fixture's precondition: the request reached the worker"
    );

    drop(requests);
    tokio::time::sleep(NOTHING_HAPPENS_GRACE).await;
    assert!(
        gate.admit_publication().is_none(),
        "the rescue holds the admission while the request it issued is unsettled"
    );

    paused.released.notify_one();
    let mut released = false;
    for _ in 0..MAX_TURNS {
        if gate.admit_publication().is_some() {
            released = true;
            break;
        }
        tokio::time::sleep(TURN).await;
    }
    assert!(
        released,
        "and releases it once the worker has answered — the landed M11.T08 rescue, unchanged"
    );
    assert_eq!(
        attempts.snapshot().outstanding_count(),
        0,
        "with the attempt accounted for by the answer that arrived, not by the cancellation"
    );
}
