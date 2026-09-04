//! The active replacement handshake (M11.T26c, design M11.D39e(i), M11.D75).
//!
//! These rows drive [`advance_fence`] against real worker servers on loopback, because the claim
//! is about what arrives at a worker and what this controller does with the answer.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arroyo_rpc::fence_wire::{StartDirective, start_directive};
use arroyo_rpc::grpc::rpc::worker_grpc_server::{WorkerGrpc, WorkerGrpcServer};
use arroyo_rpc::grpc::rpc::{
    CheckpointReq, CheckpointResp, CommitReq, CommitResp, GetWorkerPhaseReq, GetWorkerPhaseResp,
    JobControllerInitReq, JobControllerInitResp, JobFinishedReq, JobFinishedResp,
    LifecycleOperation, LoadCompactedDataReq, LoadCompactedDataRes, MetricsReq, MetricsResp,
    StartExecutionOutcome, StartExecutionReq, StartExecutionResp, StopExecutionReq,
    StopExecutionResp,
};
use arroyo_rpc::identity::{WorkerClient, worker_client};
use arroyo_server_common::shutdown::{Shutdown, SignalBehavior};
use arroyo_types::{JobId, MachineId, PipelineId, WorkerId};
use arroyo_worker::WorkerServer;

use super::LifecycleMode;
use super::fence::LifecycleAuthority;
use super::handshake::{NotAcknowledged, StartTargets, advance_fence};
use super::protocol::{FenceProtocol, FencedGeneration};

/// The fence and generation these rows address, unless they say otherwise.
const FENCE: u64 = 4;
const GENERATION: u64 = 2;

fn fenced(fence: u64, generation: u64) -> FencedGeneration {
    match FenceProtocol::for_job(
        LifecycleMode::FencedV2,
        &LifecycleAuthority::from_parts("job_abc", fence, "epoch-1"),
        generation,
    )
    .expect("this fixture names an adopted fence and a launched generation")
    {
        FenceProtocol::Fenced(generation) => generation,
        FenceProtocol::Legacy => unreachable!("the fenced mode produces the fenced protocol"),
    }
}

/// Every `StartExecution` one worker received, in arrival order.
#[derive(Default)]
struct Received {
    requests: Mutex<Vec<StartExecutionReq>>,
}

impl Received {
    fn requests(&self) -> Vec<StartExecutionReq> {
        self.requests.lock().unwrap().clone()
    }

    /// What each arriving request asked for, as the seam reads it.
    fn operations(&self) -> Vec<Option<LifecycleOperation>> {
        self.requests()
            .iter()
            .map(|req| match start_directive(req) {
                Ok(StartDirective::Fenced { operation, .. }) => Some(operation),
                Ok(StartDirective::Unfenced) => None,
                Err(e) => panic!("this controller sent a malformed directive: {e}"),
            })
            .collect()
    }
}

/// How a [`FenceWorker`] answers.
#[derive(Clone)]
enum Answers {
    /// A worker of this build: it advances to the fence it is sent and acknowledges it.
    FenceCapable,
    /// A worker that has already acknowledged a higher fence — from a controller that
    /// superseded this one — and reports it.
    AlreadyAt(u64),
    /// A worker predating M11.T26c: it does not know the operation, reads a `FENCE_ONLY`
    /// directive as an ordinary start, and answers `APPLIED`.
    PredatesTheProtocol,
    /// A worker that refuses definitively, as M11.T26d's guard does for every refusal it gives.
    Refusing(tonic::Code),
    /// A worker that never settles: every attempt ends in an ambiguous transport outcome.
    NeverSettling,
}

struct FenceWorker {
    received: Arc<Received>,
    answers: Answers,
}

#[tonic::async_trait]
impl WorkerGrpc for FenceWorker {
    async fn start_execution(
        &self,
        request: tonic::Request<StartExecutionReq>,
    ) -> Result<tonic::Response<StartExecutionResp>, tonic::Status> {
        let request = request.into_inner();
        let fence = request.lifecycle_fence;
        self.received.requests.lock().unwrap().push(request);
        match &self.answers {
            Answers::FenceCapable => Ok(tonic::Response::new(StartExecutionResp {
                observed_lifecycle_fence: fence,
                outcome: StartExecutionOutcome::FenceAcknowledged as i32,
            })),
            Answers::AlreadyAt(higher) => Ok(tonic::Response::new(StartExecutionResp {
                observed_lifecycle_fence: *higher,
                outcome: StartExecutionOutcome::FenceAcknowledged as i32,
            })),
            Answers::PredatesTheProtocol => Ok(tonic::Response::new(StartExecutionResp {
                observed_lifecycle_fence: 0,
                outcome: StartExecutionOutcome::Applied as i32,
            })),
            Answers::Refusing(code) => Err(tonic::Status::new(*code, "this generation refuses")),
            Answers::NeverSettling => Err(tonic::Status::unavailable("nothing is known")),
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

/// Serves a [`FenceWorker`] on a loopback port and returns a client for it.
async fn worker(id: WorkerId, answers: Answers) -> (Arc<Received>, WorkerClient) {
    let received = Arc::new(Received::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(WorkerGrpcServer::new(FenceWorker {
                received: received.clone(),
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
    (received, worker_client(channel, id))
}

/// Serves a **real** [`WorkerServer`] on a loopback port: the production `WorkerGrpc` guard, on
/// the receiving end of the production handshake, in the state a worker generation is in before
/// it has announced itself to any controller.
async fn real_worker(id: WorkerId, generation: u64, shutdown: &Shutdown) -> WorkerClient {
    let server = WorkerServer::new(
        MachineId(Arc::new("machine_1".to_string())),
        id,
        PipelineId(Arc::new("pipeline_1".to_string())),
        JobId(Arc::new("job_1".to_string())),
        generation,
        shutdown.guard("worker"),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(WorkerGrpcServer::new(server))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let channel = tonic::transport::Endpoint::from_shared(address)
        .unwrap()
        .timeout(Duration::from_secs(90))
        .connect()
        .await
        .unwrap();
    worker_client(channel, id)
}

// ---------------------------------------------------------------------------------------------
// The handshake itself
// ---------------------------------------------------------------------------------------------

/// A real generation that has announced itself to nobody refuses this controller's handshake, and
/// that refusal ends the whole attempt (M11.D39e(i), D96 row 21).
///
/// The one row in either crate where the production worker guard is on the receiving end of the
/// production [`advance_fence`], and it is here to price the gate rather than to celebrate it.
/// The guard answers `FailedPrecondition`, which
/// [`FenceProtocol::transport_settlement`](super::protocol::FenceProtocol::transport_settlement)
/// classifies as definitive, so nothing re-offers the directive: one generation that will not
/// answer refuses every target and the scheduling attempt fails.
///
/// That is exactly the cost an ordinary start must not pay, and it is why the worker's gate opens
/// when the generation *issues* its `RegisterWorkerReq` rather than when it applies the answer.
/// The ordering that makes the distinction load-bearing is this controller's own:
/// `ControllerGrpc::register_worker` enqueues the `WorkerConnect` that makes a generation
/// schedulable **before** it returns `RegisterWorkerResp`, so the handshake below is issued to
/// workers whose answer may still be in flight. `arroyo-worker`'s
/// `the_registration_request_opens_the_fenced_protocol_before_its_answer_arrives` is the other
/// half of that pair.
#[tokio::test]
async fn a_generation_that_has_not_announced_itself_fails_this_controllers_whole_attempt() {
    let shutdown = Shutdown::new("m11-t26-real-worker-handshake", SignalBehavior::None);
    let unannounced = real_worker(WorkerId(1), GENERATION, &shutdown).await;
    let (acknowledging, capable) = worker(WorkerId(2), Answers::FenceCapable).await;

    let refusal = advance_fence(
        fenced(FENCE, GENERATION),
        HashMap::from([(WorkerId(1), unannounced), (WorkerId(2), capable)]),
    )
    .await
    .expect_err("a generation that has announced itself to nobody does not acknowledge")
    .to_string();

    assert!(
        refusal.contains("FailedPrecondition")
            && refusal.contains("Worker generation has not begun registration"),
        "the refusal this controller reports is the worker guard's own answer: {refusal}"
    );
    assert_eq!(
        acknowledging.operations(),
        vec![Some(LifecycleOperation::FenceOnly)],
        "and it is all or nothing: the generation that did acknowledge is started by nobody"
    );
}

/// Every generation is advanced to this controller's fence and answers before any start exists.
///
/// The two halves of D96 row 21 as this module can see them: what arrives at each worker is a
/// `FENCE_ONLY` directive under this job's fence, addressed to that worker's generation, and the
/// [`StartTargets`] the fan-out needs do not exist until every one of them has answered.
#[tokio::test]
async fn the_handshake_advances_and_is_acknowledged_by_every_addressed_generation() {
    let (one, client_one) = worker(WorkerId(1), Answers::FenceCapable).await;
    let (two, client_two) = worker(WorkerId(2), Answers::FenceCapable).await;
    let generation = fenced(FENCE, GENERATION);

    let targets = advance_fence(
        generation,
        HashMap::from([(WorkerId(1), client_one), (WorkerId(2), client_two)]),
    )
    .await
    .expect("two fence-capable generations acknowledge");

    assert_eq!(targets.len(), 2);
    assert_eq!(targets.protocol(), FenceProtocol::Fenced(generation));

    for (id, received) in [(1u64, &one), (2u64, &two)] {
        assert_eq!(
            received.operations(),
            vec![Some(LifecycleOperation::FenceOnly)],
            "worker {id} is asked to advance its fence, once, and nothing else"
        );
        let req = &received.requests()[0];
        assert_eq!(req.lifecycle_fence, FENCE);
        assert_eq!(req.target_worker_id, id);
        assert_eq!(req.target_worker_generation, GENERATION);
        assert!(
            req.program.is_none() && req.start_execution_id.is_empty(),
            "a fence-only directive starts nothing and carries no attempt identifier"
        );
    }

    // And the starts the handshake authorises carry the same fence, addressed the same way.
    let mut addressed: Vec<(u64, u64, u64, i32)> = targets
        .into_starts()
        .into_iter()
        .map(|(id, _, directive)| {
            let mut req = StartExecutionReq::default();
            directive.stamp(&mut req);
            (
                id.0,
                req.lifecycle_fence,
                req.target_worker_generation,
                req.lifecycle_operation,
            )
        })
        .collect();
    addressed.sort_unstable();
    assert_eq!(
        addressed,
        vec![
            (1, FENCE, GENERATION, LifecycleOperation::Start as i32),
            (2, FENCE, GENERATION, LifecycleOperation::Start as i32),
        ]
    );
}

/// A generation that has acknowledged a *higher* fence has still acknowledged this one.
///
/// It reports the highest fence it holds, and everything below that is refused from now on —
/// which is the whole content of "this generation has taken my fence". A lower one has not.
#[tokio::test]
async fn a_generation_at_a_higher_fence_has_acknowledged_this_one_and_a_lower_one_has_not() {
    for (observed, acknowledged) in [(FENCE, true), (FENCE + 1, true), (FENCE - 1, false)] {
        let (_, client) = worker(WorkerId(1), Answers::AlreadyAt(observed)).await;
        let outcome = advance_fence(
            fenced(FENCE, GENERATION),
            HashMap::from([(WorkerId(1), client)]),
        )
        .await;
        assert_eq!(
            outcome.is_ok(),
            acknowledged,
            "a generation reporting fence {observed} against fence {FENCE}"
        );
    }
}

/// A worker predating the operation is refused loudly rather than read as an acknowledgement.
///
/// It answers `APPLIED`, because it read the `FENCE_ONLY` directive as an ordinary start. This
/// is the mixed-version state M11.D75's worker-first ordering exists to prevent; detecting it
/// here is what stops the controller starting a job on a generation that never took its fence.
#[tokio::test]
async fn a_worker_predating_the_fence_protocol_does_not_acknowledge_it() {
    let (_, client) = worker(WorkerId(1), Answers::PredatesTheProtocol).await;
    let refusal = advance_fence(
        fenced(FENCE, GENERATION),
        HashMap::from([(WorkerId(1), client)]),
    )
    .await
    .expect_err("an APPLIED answer is not an acknowledgement of a fence");
    assert!(matches!(
        refusal.refusals(),
        [NotAcknowledged::NotAnAcknowledgement { worker: 1, .. }]
    ));
}

/// Every definitive refusal ends this generation's handshake at once; nothing is retried.
///
/// The five codes M11.T26d's guard gives, and the two the controller must never read as
/// transport: `ResourceExhausted` is a full identifier record and `FailedPrecondition` is every
/// admission refusal, and retrying either forever is the wedge the reconciliation budget exists
/// to make impossible.
#[tokio::test]
async fn a_definitive_refusal_ends_the_handshake_without_a_retry() {
    for code in [
        tonic::Code::FailedPrecondition,
        tonic::Code::InvalidArgument,
        tonic::Code::ResourceExhausted,
        tonic::Code::Aborted,
        tonic::Code::Internal,
    ] {
        let (received, client) = worker(WorkerId(1), Answers::Refusing(code)).await;
        let refusal = advance_fence(
            fenced(FENCE, GENERATION),
            HashMap::from([(WorkerId(1), client)]),
        )
        .await
        .expect_err("a definitive refusal is not an acknowledgement");
        assert!(
            matches!(
                refusal.refusals(),
                [NotAcknowledged::Refused { worker: 1, .. }]
            ),
            "{code:?} must settle rather than be retried"
        );
        assert_eq!(
            received.requests().len(),
            1,
            "{code:?} is the generation's own answer, so the directive is offered exactly once"
        );
    }
}

/// A generation that never answers costs the bounded budget and then is simply not startable.
///
/// Not settlement, and not treated as any: the controller does not know whether the fence was
/// advanced. What it knows is that this is not a generation it may start.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_generation_that_never_settles_the_handshake_is_never_started() {
    let (received, client) = worker(WorkerId(1), Answers::NeverSettling).await;
    let refusal = advance_fence(
        fenced(FENCE, GENERATION),
        HashMap::from([(WorkerId(1), client)]),
    )
    .await
    .expect_err("an ambiguous outcome never becomes an acknowledgement");
    assert!(matches!(
        refusal.refusals(),
        [NotAcknowledged::Unsettled { worker: 1, .. }]
    ));
    assert_eq!(
        received.requests().len(),
        crate::states::scheduling::START_EXECUTION_RECONCILE_ATTEMPTS + 1,
        "the same directive is re-offered within the landed budget and then given up"
    );
}

/// One generation failing the handshake stops the whole fan-out, not just its own start.
///
/// "Start the ones that answered" would be a job running on a subset of its workers under a
/// fence the rest never heard, which is the state the fan-out's settlement accounting assumes
/// cannot exist.
#[tokio::test]
async fn one_generation_that_does_not_acknowledge_stops_every_start() {
    let (willing, client_one) = worker(WorkerId(1), Answers::FenceCapable).await;
    let (_, client_two) = worker(
        WorkerId(2),
        Answers::Refusing(tonic::Code::FailedPrecondition),
    )
    .await;

    let refusal = advance_fence(
        fenced(FENCE, GENERATION),
        HashMap::from([(WorkerId(1), client_one), (WorkerId(2), client_two)]),
    )
    .await
    .expect_err("a handshake is all or nothing");
    assert!(matches!(
        refusal.refusals(),
        [NotAcknowledged::Refused { worker: 2, .. }]
    ));
    assert_eq!(
        willing.operations(),
        vec![Some(LifecycleOperation::FenceOnly)],
        "the willing generation was advanced, and was still never sent a start"
    );
}

// ---------------------------------------------------------------------------------------------
// What a fan-out may address at all
// ---------------------------------------------------------------------------------------------

/// A fenced fan-out cannot be built without a handshake.
///
/// The structural half of D96 row 21: the fenced arm of [`StartTargets`] is built only by
/// [`advance_fence`], so "a fenced start to a generation that did not acknowledge" is not a
/// check that could be skipped — there is no value from which such a request could be issued.
#[test]
fn a_fenced_fan_out_cannot_be_addressed_without_a_handshake() {
    assert!(
        StartTargets::without_a_handshake(
            FenceProtocol::Fenced(fenced(FENCE, GENERATION)),
            HashMap::new()
        )
        .is_none(),
        "the fenced protocol has no fan-out that skips the handshake"
    );
    let legacy = StartTargets::without_a_handshake(FenceProtocol::Legacy, HashMap::new())
        .expect("the legacy protocol needs no handshake");
    assert_eq!(legacy.protocol(), FenceProtocol::Legacy);
}

/// The legacy fan-out stamps the request a controller predating the fields sends.
#[tokio::test]
async fn the_legacy_fan_out_addresses_its_workers_without_a_fence() {
    let (_, client) = worker(WorkerId(1), Answers::FenceCapable).await;
    let targets = StartTargets::without_a_handshake(
        FenceProtocol::Legacy,
        HashMap::from([(WorkerId(1), client)]),
    )
    .expect("the legacy protocol needs no handshake");

    let starts = targets.into_starts();
    assert_eq!(starts.len(), 1);
    let mut req = StartExecutionReq {
        start_execution_id: "attempt".to_string(),
        ..Default::default()
    };
    starts[0].2.stamp(&mut req);
    assert_eq!(
        req,
        StartExecutionReq {
            start_execution_id: "attempt".to_string(),
            ..Default::default()
        },
        "every lifecycle field stays at the value a controller predating them sends"
    );
}

/// The unfenced shape acknowledges nothing and still counts the workers it addresses.
///
/// The two readings a fan-out takes from its targets, on the arm where there is no handshake to
/// read them from. `acknowledgements()` answering *empty* rather than refusing is what keeps the
/// pre-flag-day peer's fan-out from carrying observations it never made into the attempt's
/// fencing reconciliation; `len()` answering the worker count is what stops the same arm from
/// looking like a fan-out that addressed nobody.
#[tokio::test]
async fn the_unfenced_shape_acknowledges_nothing_and_still_counts_its_workers() {
    let (_, one) = worker(WorkerId(1), Answers::FenceCapable).await;
    let (_, two) = worker(WorkerId(2), Answers::FenceCapable).await;
    let targets = StartTargets::without_a_handshake(
        FenceProtocol::Legacy,
        HashMap::from([(WorkerId(1), one), (WorkerId(2), two)]),
    )
    .expect("the legacy protocol needs no handshake");

    assert_eq!(
        targets.acknowledgements(),
        Vec::new(),
        "no fence was advanced, so there is nothing for the reconciliation to observe"
    );
    assert_eq!(
        targets.len(),
        2,
        "and both addressed workers are still addressed"
    );
}

/// The targets render which protocol addressed them and which workers they hold, and never the
/// channels.
///
/// A `Debug` an operator reads out of a log line: the shape is the question — a fan-out that
/// carries no fence is a different situation from one that carries a fence its generation
/// acknowledged — and a `WorkerClient` has no useful rendering. One worker per shape, because
/// the map's iteration order is not part of the claim.
#[tokio::test]
async fn the_targets_render_their_protocol_and_their_workers_and_not_their_channels() {
    let (_, legacy_client) = worker(WorkerId(1), Answers::FenceCapable).await;
    let legacy = StartTargets::without_a_handshake(
        FenceProtocol::Legacy,
        HashMap::from([(WorkerId(1), legacy_client)]),
    )
    .expect("the legacy protocol needs no handshake");
    assert_eq!(
        format!("{legacy:?}"),
        "StartTargets { protocol: \"Legacy\", workers: [WorkerId(1)] }"
    );

    let (_, fenced_client) = worker(WorkerId(1), Answers::FenceCapable).await;
    let fenced_targets = advance_fence(
        fenced(FENCE, GENERATION),
        HashMap::from([(WorkerId(1), fenced_client)]),
    )
    .await
    .expect("the addressed generation acknowledges");
    assert_eq!(
        format!("{fenced_targets:?}"),
        format!(
            "StartTargets {{ fence: {FENCE}, generation: {GENERATION}, workers: [WorkerId(1)] }}"
        )
    );
}
