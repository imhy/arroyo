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

use arroyo_rpc::fencing::MAX_ATTEMPT_ID_CHARS;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arroyo_rpc::grpc::api;
use arroyo_rpc::grpc::rpc::worker_grpc_server::{WorkerGrpc, WorkerGrpcServer};
use arroyo_rpc::grpc::rpc::{
    CheckpointReq, CheckpointResp, CommitReq, CommitResp, GetWorkerPhaseReq, GetWorkerPhaseResp,
    JobControllerInitReq, JobControllerInitResp, JobFinishedReq, JobFinishedResp,
    LoadCompactedDataReq, LoadCompactedDataRes, MetricsReq, MetricsResp, StartExecutionOutcome,
    StartExecutionReq, StartExecutionResp, StopExecutionReq, StopExecutionResp,
};
use arroyo_rpc::identity::{WorkerChannel, WorkerClient, worker_client};
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_types::{MachineId, PipelineId, WorkerId};

use super::ExecutionPlan;
use super::fanout::{AttemptLedger, IssuedAttempts, Observed};
use crate::states::lifecycle::{FenceProtocol, StartTargets};
use crate::states::{Admission, AdmissionLock};

/// Records an authoritative response for `worker`'s attempt in an inventory a row built by hand,
/// through the one validated seam `AttemptLedger::answered` uses.
///
/// The identifier is named rather than assumed, because that is the whole of what the seam
/// checks: an inventory accounts for an answer only when the answer names the identifier that
/// worker was issued, in the generation this fan-out addressed. The rows that vary those against
/// each other are in `crate::states::lifecycle::settlement_tests`.
fn answered(issued: &mut IssuedAttempts, worker: WorkerId, attempt_id: &str) {
    let generation = issued.generation();
    let _accounted = issued.observe(&Observed::authoritative_response(
        worker, generation, attempt_id,
    ));
}

/// The ledger of a fan-out with no settlement owner, addressing [`GENERATION`].
///
/// A generation is named rather than defaulted because a ledger has one: the fan-out seeds it
/// from the job's own scheduling generation, and an inventory carrying zero would be claiming to
/// address nothing.
fn unowned_ledger() -> AttemptLedger {
    AttemptLedger::owned_by(None, GENERATION, FENCE)
}

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

/// The worker generation these rows' fan-outs address.
///
/// A fan-out's inventory carries the generation its identifiers were issued to (M11.T26e), so
/// that an observation about another generation accounts for nothing in it. These rows are about
/// what the fan-out records rather than about who observes it, so one generation is enough — but
/// it is a named non-zero value rather than the `Default` zero, because zero is the sentinel for
/// "addresses no generation" and an inventory that carried it would be claiming that.
const GENERATION: u64 = 3;

/// The lifecycle fence these rows' fan-outs issue their identifiers under.
///
/// Non-zero, so that an acknowledgement can be *above* it and settle: a worker revokes what is
/// below the fence it takes, so an inventory issued under fence zero could be settled by any
/// acknowledgement at all and would prove nothing about the height check (M11.T26f).
const FENCE: u64 = 5;

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
    /// Answers `Ok` with a settlement that is not an application of the request.
    ///
    /// The shape M11.T26c's response reading exists for: a `StartExecutionResp` is a success at
    /// the transport layer whatever it says, and a generation that acknowledged a fence has
    /// started nothing.
    NotApplying(StartExecutionResp),
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
            Answers::NotApplying(response) => {
                return Ok(tonic::Response::new(*response));
            }
        }
        // The double answers as the worker in this build does: it acknowledges no lifecycle
        // fence, and an `Ok` response means the addressed attempt is applied.
        Ok(tonic::Response::new(StartExecutionResp {
            observed_lifecycle_fence: 0,
            outcome: StartExecutionOutcome::Applied as i32,
        }))
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

/// An [`Admission`] taken from an [`AdmissionLock`] the row also holds, so it can ask
/// afterwards whether the job's lifecycle authority is free.
async fn admitted() -> (AdmissionLock, Admission) {
    let lock = AdmissionLock::default();
    let admission = lock.admit().await;
    (lock, admission)
}

/// Runs the fan-out over `connects` to completion and returns what it settled.
async fn fan_out(
    admission: Admission,
    connects: HashMap<WorkerId, WorkerClient>,
    attempts: Arc<AttemptLedger>,
) -> (Admission, anyhow::Result<HashMap<WorkerId, WorkerClient>>) {
    let ids = machine_ids(&connects.keys().copied().collect::<Vec<_>>());
    // The landed rows are about the pre-flag-day fan-out, so they address their workers the way
    // `LifecycleMode::LegacyT08` does: no fence, no addressed generation. The fenced shape has
    // its own rows in `lifecycle::handshake_tests`.
    let targets = StartTargets::without_a_handshake(
        FenceProtocol::Legacy,
        connects
            .into_iter()
            .map(|(id, client)| {
                // The legacy protocol addresses no generation and therefore no process; the
                // channel carries none, and `without_a_handshake` drops it.
                (id, WorkerChannel::to(client, None))
            })
            .collect(),
    )
    .expect("the legacy protocol needs no handshake");
    // Minted and recorded before the fan-out, exactly as both production routes do it since
    // PR #167 round 2: an identifier the fan-out minted for itself could not appear in a record
    // written before its request existed.
    let issued = super::IssuedFanOut::mint(targets, &attempts);
    super::start_execution_on_workers(
        admission,
        Arc::new("job_1".to_string()),
        PipelineId(Arc::new("pipeline_1".to_string())),
        plan(),
        ids,
        issued,
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

    let (authority, admission) = admitted().await;
    let attempts = Arc::new(unowned_ledger());
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

    let mut expected = IssuedAttempts::issued_under(GENERATION, FENCE);
    expected.issued(WorkerId(1), slow_calls.first_id());
    expected.issued(WorkerId(2), quick_calls.first_id());
    answered(&mut expected, WorkerId(1), &slow_calls.first_id());
    answered(&mut expected, WorkerId(2), &quick_calls.first_id());
    assert_eq!(
        attempts.snapshot(),
        expected,
        "the inventory is one record per target worker, carrying the identifier that worker \
         was actually sent — read back from the workers themselves, so nothing here can be \
         satisfied by an identifier the controller made up"
    );

    drop(admission);
    assert!(
        authority.is_free(),
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
    let (authority, admission) = admitted().await;
    let attempts = Arc::new(unowned_ledger());

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
    let mut expected = IssuedAttempts::issued_under(GENERATION, FENCE);
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
    assert!(authority.is_free());
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
    let (authority, admission) = admitted().await;
    let attempts = Arc::new(unowned_ledger());

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

    let mut expected = IssuedAttempts::issued_under(GENERATION, FENCE);
    expected.issued(WorkerId(4), refused_calls.first_id());
    expected.issued(WorkerId(5), accepted_calls.first_id());
    answered(&mut expected, WorkerId(4), &refused_calls.first_id());
    answered(&mut expected, WorkerId(5), &accepted_calls.first_id());
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
    assert!(authority.is_free());
}

/// The identifier the fan-out actually mints is exactly the width every bounded record accepts.
///
/// The worker's applied/revoked record (`arroyo_worker::lifecycle_fence::attempt_ids`), the
/// durable fencing record (`arroyo_rpc::fencing::Fencing`) and the revocation list on a decoded
/// `StartExecutionReq` (`arroyo_rpc::fence_wire`) all refuse an identifier wider than
/// [`MAX_ATTEMPT_ID_CHARS`], and none of them can widen to fit one: a refusal there is a request
/// the worker will not apply. So the producer has to be pinned against the bound rather than
/// argued about, and it has to be *this* producer — the expression the fan-out runs — so that a
/// change to the minting format fails here instead of on the wire.
///
/// `{:016x}` is a minimum width, and 16 is also the maximum number of hexadecimal digits a
/// `u64` has, so the width is a constant rather than a distribution; the sampling below is
/// belt-and-braces over that argument, not the argument itself.
#[test]
fn the_minted_start_execution_id_is_exactly_the_bounded_width() {
    assert_eq!(
        MAX_ATTEMPT_ID_CHARS, 32,
        "two u64s in zero-padded hexadecimal"
    );

    for _ in 0..1024 {
        let minted = super::mint_start_execution_id();
        assert_eq!(
            minted.chars().count(),
            MAX_ATTEMPT_ID_CHARS,
            "minted {minted:?}"
        );
        // Characters and bytes agree, so a record that bounds either bounds both.
        assert_eq!(minted.len(), MAX_ATTEMPT_ID_CHARS, "minted {minted:?}");
        assert!(
            minted
                .chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
            "minted {minted:?}"
        );
    }

    // The two extremes of the value space produce the same width as any sample, which is what
    // makes the constant a constant: a zero half is padded to 16 and a saturated half is 16.
    for (high, low) in [(0, 0), (u64::MAX, u64::MAX), (0, u64::MAX), (u64::MAX, 0)] {
        assert_eq!(
            format!("{high:016x}{low:016x}").chars().count(),
            MAX_ATTEMPT_ID_CHARS
        );
    }

    // And it is non-empty, which every one of those records also requires.
    assert!(!super::mint_start_execution_id().is_empty());
}

// ---------------------------------------------------------------------------------------------
// M11.T26c — the response is read, not merely received
// ---------------------------------------------------------------------------------------------

/// An `Ok` response that does not say the request was applied does not start a worker.
///
/// A `StartExecutionResp` is a transport success whatever it carries, and the landed loop treated
/// every one of them as "the worker entered initialization". Under M11.D39e a generation answers a
/// fence-only or revoke directive with `FENCE_ACKNOWLEDGED` or `REVOKED`; a `START` answered with
/// either is a worker that advanced its fence and ran nothing, and reporting it as started would
/// leave the controller waiting for tasks that were never launched.
///
/// Both are definitive: the attempt is accounted for, and the identifier is not re-offered.
#[tokio::test]
async fn a_response_that_acknowledges_a_fence_does_not_start_a_worker() {
    for outcome in [
        StartExecutionOutcome::FenceAcknowledged,
        StartExecutionOutcome::Revoked,
    ] {
        let (calls, client) = worker(
            WorkerId(1),
            Answers::NotApplying(StartExecutionResp {
                observed_lifecycle_fence: 9,
                outcome: outcome as i32,
            }),
        )
        .await;
        let (_authority, admission) = admitted().await;
        let attempts = Arc::new(unowned_ledger());
        let (admission, started) = fan_out(
            admission,
            HashMap::from([(WorkerId(1), client)]),
            Arc::clone(&attempts),
        )
        .await;
        drop(admission);

        assert!(
            started.is_err(),
            "{outcome:?}: a generation that acknowledged a fence has started nothing"
        );
        assert_eq!(
            calls.ids().len(),
            1,
            "{outcome:?}: and it is the generation's own answer, so the identifier is offered \
             once and not re-offered"
        );
        assert_eq!(
            attempts.snapshot().outstanding_count(),
            0,
            "{outcome:?}: a response is an answer, so the attempt is accounted for"
        );
    }
}

/// A response this build cannot name is not evidence that anything was applied.
///
/// `observed_settlement` refuses a response claiming to have acknowledged a fence while reporting
/// none, and an outcome value this build does not know — which is what a *newer* worker's answer
/// arrives as, since proto3 keeps an unrecognized enum value verbatim. Reading either as
/// `APPLIED` is how a controller would wait for tasks that were never started.
#[tokio::test]
async fn a_response_this_build_cannot_read_is_not_an_application() {
    for response in [
        // Acknowledges a fence while reporting none observed.
        StartExecutionResp {
            observed_lifecycle_fence: 0,
            outcome: StartExecutionOutcome::FenceAcknowledged as i32,
        },
        // An outcome from a build newer than this one.
        StartExecutionResp {
            observed_lifecycle_fence: 3,
            outcome: 99,
        },
    ] {
        let (calls, client) = worker(WorkerId(1), Answers::NotApplying(response)).await;
        let (_authority, admission) = admitted().await;
        let (admission, started) = fan_out(
            admission,
            HashMap::from([(WorkerId(1), client)]),
            Arc::new(unowned_ledger()),
        )
        .await;
        drop(admission);

        assert!(
            started.is_err(),
            "{response:?}: a response this build cannot name is not an application"
        );
        assert_eq!(calls.ids().len(), 1, "{response:?}: and it is not retried");
    }
}

/// The response the worker in this build sends before the flag day still starts the worker.
///
/// The control for the two rows above, and the compatibility half of M11.T26c: `APPLIED` with no
/// observed fence is what both a fence-capable generation and a worker predating the fields
/// answer, and the fan-out reads it exactly as it always did.
#[tokio::test]
async fn the_pre_flag_day_response_still_starts_the_worker() {
    let (calls, client) = worker(
        WorkerId(1),
        Answers::NotApplying(StartExecutionResp::default()),
    )
    .await;
    let (_authority, admission) = admitted().await;
    let (admission, started) = fan_out(
        admission,
        HashMap::from([(WorkerId(1), client)]),
        Arc::new(unowned_ledger()),
    )
    .await;
    drop(admission);

    assert!(
        started.is_ok(),
        "the default response is `APPLIED` with no observed fence, which is what a worker \
         predating these fields sends"
    );
    assert_eq!(calls.ids().len(), 1);
}
