//! The leader's real commit send path, from its lifecycle authority to a real worker's handler
//! (M11.T26e, design M11.D39d).
//!
//! `RunningJobModel::commit_to_workers` is the one loop every commit in either topology goes
//! out through, and these rows drive it — not `addressed_commit` beneath it — so what they
//! prove is that the wiring reaches the fence, not that the fence exists. The receiving end is
//! the production [`WorkerServer`] behind a real tonic server, so a commit that is refused is
//! refused by the same `WorkerGrpc::commit` a deployed worker runs.

use crate::job_controller::model::{
    CommitBody, JobState, RunningJobModel, TaskStatus, WorkerState, WorkerStatus,
};
use crate::{EngineState, WorkerExecutionPhase, WorkerServer};
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::ControlMessage;
use arroyo_rpc::fence_wire::{
    CommitAuthority, FenceAddress, LifecycleTarget, StartDirective, observed_settlement,
};
use arroyo_rpc::grpc::rpc::worker_grpc_server::WorkerGrpcServer;
use arroyo_rpc::grpc::rpc::{
    LifecycleOperation, OperatorCommitData, StartExecutionOutcome, StartExecutionReq,
    TableCommitData,
};
use arroyo_rpc::identity::{WorkerClient, worker_client};
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_server_common::shutdown::{Shutdown, SignalBehavior};
use arroyo_state::StorageProviderFor;
use arroyo_state_protocol::ProtocolPaths;
use arroyo_state_protocol::types::{Epoch, Generation};
use arroyo_types::{JobId, MachineId, PipelineId, WorkerId};
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::Receiver;

/// The generation every worker in this file runs under, and the fence its leader was started
/// under. Both above 1 so a predecessor and a lower fence can be addressed.
const GENERATION: u64 = 3;
const FENCE: u64 = 5;
const OPERATOR: &str = "op_1";

/// A real worker serving on loopback, with one operator control channel this test can read.
struct LiveWorker {
    _shutdown: Shutdown,
    client: WorkerClient,
    control: Receiver<ControlMessage>,
}

/// Starts a production [`WorkerServer`] for `worker_id`/`generation`, registers it, puts it into
/// `Running`, and connects a client to it.
async fn live_worker(worker_id: u64, generation: u64) -> LiveWorker {
    let shutdown = Shutdown::new("commit-wiring-test", SignalBehavior::None);
    let server = WorkerServer::new(
        MachineId(Arc::new("machine_1".to_string())),
        WorkerId(worker_id),
        PipelineId(Arc::new("pipeline_1".to_string())),
        JobId(Arc::new("job_1".to_string())),
        generation,
        shutdown.guard("worker"),
    );
    {
        let mut lifecycle = server.state.lifecycle.lock().unwrap();
        let announced = lifecycle.announce();
        lifecycle.registered(announced, false);
    }

    let (tx, control) = tokio::sync::mpsc::channel(8);
    *server.state.lifecycle.lock().unwrap().execution_mut() =
        WorkerExecutionPhase::Running(EngineState {
            sources: vec![],
            sinks: vec![],
            operator_to_node: HashMap::from([(OPERATOR.to_string(), 1u32)]),
            operator_controls: HashMap::from([(1u32, vec![tx])]),
            shutdown_guard: shutdown.guard("engine-state"),
        });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(WorkerGrpcServer::new(server))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    LiveWorker {
        _shutdown: shutdown,
        client: worker_client(channel, WorkerId(worker_id)),
        control,
    }
}

/// The leader's model, holding `authority` and one worker.
fn leader_model(
    authority: CommitAuthority,
    worker: WorkerId,
    client: WorkerClient,
) -> RunningJobModel {
    RunningJobModel {
        protocol_paths: ProtocolPaths::new(
            PipelineId(Arc::new("pipeline_1".to_string())),
            JobId(Arc::new("job_1".to_string())),
        ),
        pipeline_id: PipelineId(Arc::new("pipeline_1".to_string())),
        job_id: JobId(Arc::new("job_1".to_string())),
        generation: Generation(GENERATION),
        state: JobState::Running,
        checkpoint_state: None,
        epoch: Epoch(4),
        min_epoch: Epoch(0),
        last_checkpoint: Instant::now(),
        workers: HashMap::from([(
            worker,
            WorkerStatus {
                id: worker,
                connect: client,
                last_heartbeat: Instant::now(),
                state: WorkerState::Running,
            },
        )]),
        tasks: HashMap::<(u32, u32), TaskStatus>::new(),
        operator_parallelism: HashMap::new(),
        metric_update_task: None,
        last_updated_metrics: Instant::now(),
        checkpoint_parent_ref: None,
        program: Arc::new(LogicalProgram::default()),
        checkpoint_spans: vec![],
        worker_leader_mode: true,
        commit_authority: authority,
        storage_role: StorageProviderFor::Worker,
        finished_operators: vec![],
        state_backend: StateBackendSelector::Parquet,
        generation_manifest: None,
        job_metrics: None,
    }
}

/// One operator, one table, one subtask.
fn body(epoch: u64) -> CommitBody {
    CommitBody {
        epoch,
        committing_data: HashMap::from([(
            OPERATOR.to_string(),
            OperatorCommitData {
                committing_data: HashMap::from([(
                    "t".to_string(),
                    TableCommitData {
                        commit_data_by_subtask: HashMap::from([(0u32, vec![1, 2, 3])]),
                    },
                )]),
            },
        )]),
    }
}

/// What the worker published, in an order-independent shape.
fn published(
    control: &mut Receiver<ControlMessage>,
) -> Vec<(u32, Vec<(String, Vec<(u32, Vec<u8>)>)>)> {
    let mut seen = vec![];
    while let Ok(ControlMessage::Commit { epoch, commit_data }) = control.try_recv() {
        let mut tables: Vec<_> = commit_data
            .into_iter()
            .map(|(table, by_subtask)| {
                let mut subtasks: Vec<_> = by_subtask.into_iter().collect();
                subtasks.sort();
                (table, subtasks)
            })
            .collect();
        tables.sort();
        seen.push((epoch, tables));
    }
    seen
}

fn expected(epoch: u32) -> Vec<(u32, Vec<(String, Vec<(u32, Vec<u8>)>)>)> {
    vec![(epoch, vec![("t".to_string(), vec![(0u32, vec![1, 2, 3])])])]
}

fn nz(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("this fixture names a live value")
}

/// A leader started under a fence commits to the generation it was started as, and that
/// generation applies it.
///
/// The wiring end to end: the model's authority addresses the worker, the request crosses a real
/// gRPC connection, and the production `WorkerGrpc::commit` — which now decides the directive
/// before it reads what is being committed — publishes it to the operator.
#[tokio::test]
async fn a_fenced_leader_commits_to_the_generation_it_was_started_as() {
    let mut worker = live_worker(11, GENERATION).await;
    let mut model = leader_model(
        CommitAuthority::under(nz(FENCE), nz(GENERATION)),
        WorkerId(11),
        worker.client.clone(),
    );

    model.commit_to_workers(&body(4)).await.expect("published");
    assert_eq!(published(&mut worker.control), expected(4));
}

/// A leader still holding a predecessor generation's authority is refused by the live worker.
///
/// The endpoint is the same one that just accepted a commit; only the generation the directive
/// addresses differs. That is M11.D39d's *"a restarted worker generation rejects a request
/// addressed to its predecessor"*, driven from the sender that would otherwise publish it.
#[tokio::test]
async fn a_leader_addressing_a_predecessor_generation_is_refused_by_the_live_worker() {
    let mut worker = live_worker(11, GENERATION).await;
    let mut superseded = leader_model(
        CommitAuthority::under(nz(FENCE), nz(GENERATION - 1)),
        WorkerId(11),
        worker.client.clone(),
    );

    let refused = superseded
        .commit_to_workers(&body(4))
        .await
        .expect_err("a commit addressed to another generation is refused");
    assert!(
        refused.to_string().contains("FailedPrecondition"),
        "definitively, so the sender settles rather than retries: {refused}"
    );
    assert_eq!(
        published(&mut worker.control),
        vec![],
        "and the worker published nothing"
    );

    // The control: the same body from a leader holding this generation's authority is published,
    // so what the worker refused was the address and not the commit.
    let mut current = leader_model(
        CommitAuthority::under(nz(FENCE), nz(GENERATION)),
        WorkerId(11),
        worker.client,
    );
    current
        .commit_to_workers(&body(4))
        .await
        .expect("published");
    assert_eq!(published(&mut worker.control), expected(4));
}

/// A leader whose fence has been superseded cannot finish its two-phase commit.
///
/// The worker acknowledged a higher fence — as it would during a replacement controller's
/// handshake — and from then on the older leader's commits are refused. This is the direction
/// M11.D39d puts the fence on commit directives for.
#[tokio::test]
async fn a_leader_under_a_superseded_fence_cannot_commit() {
    let mut worker = live_worker(11, GENERATION).await;
    let mut model = leader_model(
        CommitAuthority::under(nz(FENCE), nz(GENERATION)),
        WorkerId(11),
        worker.client.clone(),
    );
    model.commit_to_workers(&body(4)).await.expect("published");
    assert_eq!(published(&mut worker.control), expected(4));

    // A replacement controller advances this generation's fence past the leader's, over the
    // same connection and through the same handshake `lifecycle::handshake::advance_fence`
    // performs.
    let mut request = StartExecutionReq::default();
    StartDirective::Fenced {
        address: FenceAddress::under(
            nz(FENCE + 1),
            LifecycleTarget::in_generation(11, nz(GENERATION)),
        ),
        operation: LifecycleOperation::FenceOnly,
        revoked_execution_ids: &[],
    }
    .stamp(&mut request);
    let acknowledgement = worker
        .client
        .clone()
        .start_execution(tonic::Request::new(request))
        .await
        .expect("the worker acknowledges the replacement's fence")
        .into_inner();
    let acknowledgement = observed_settlement(&acknowledgement).unwrap();
    assert_eq!(acknowledgement.observed_fence(), Some(FENCE + 1));
    assert_eq!(
        acknowledgement.outcome(),
        StartExecutionOutcome::FenceAcknowledged
    );

    let refused = model
        .commit_to_workers(&body(5))
        .await
        .expect_err("a superseded leader cannot finish its commit");
    assert!(
        refused.to_string().contains("FailedPrecondition"),
        "{refused}"
    );
}

/// Before the flag day the leader sends the commit it sent before the fence existed.
///
/// The bytes are compared at the seam that builds them — the same
/// `RunningJobModel::commit_to_workers` uses — against the same request with every lifecycle
/// field written back to its proto3 default; a field left set would encode its key and fail.
/// The live worker then publishes it, so the claim covers both what leaves and what arrives.
#[tokio::test]
async fn an_unfenced_leader_sends_and_a_worker_applies_the_commit_from_before_the_fence() {
    use crate::job_controller::model::addressed_commit;
    use arroyo_rpc::grpc::rpc::CommitReq;
    use prost::Message;

    let body = body(4);
    let sent = addressed_commit(CommitAuthority::unfenced(), WorkerId(11), &body);
    let legacy = CommitReq {
        epoch: 4,
        committing_data: sent.committing_data.clone(),
        lifecycle_fence: 0,
        target_worker_id: 0,
        target_worker_generation: 0,
    };
    assert_eq!(sent, legacy);
    assert_eq!(sent.encode_to_vec(), legacy.encode_to_vec());

    let mut worker = live_worker(11, GENERATION).await;
    let mut model = leader_model(CommitAuthority::unfenced(), WorkerId(11), worker.client);
    model.commit_to_workers(&body).await.expect("published");
    assert_eq!(published(&mut worker.control), expected(4));
}

/// Every worker of the job is addressed to itself, under the one fence the leader holds.
///
/// Two live workers of the same generation at different ids. Each publishes, and each was sent a
/// request naming its own id — which is what would break if the fan-out stamped one address and
/// reused it.
#[tokio::test]
async fn every_worker_of_the_generation_is_addressed_to_itself() {
    use crate::job_controller::model::addressed_commit;

    let mut first = live_worker(11, GENERATION).await;
    let mut second = live_worker(12, GENERATION).await;
    let authority = CommitAuthority::under(nz(FENCE), nz(GENERATION));
    let mut model = leader_model(authority, WorkerId(11), first.client.clone());
    model.workers.insert(
        WorkerId(12),
        WorkerStatus {
            id: WorkerId(12),
            connect: second.client.clone(),
            last_heartbeat: Instant::now(),
            state: WorkerState::Running,
        },
    );

    model.commit_to_workers(&body(4)).await.expect("published");
    assert_eq!(published(&mut first.control), expected(4));
    assert_eq!(published(&mut second.control), expected(4));

    // And the requests differ in exactly the worker id, under one fence and one generation.
    let to_first = addressed_commit(authority, WorkerId(11), &body(4));
    let to_second = addressed_commit(authority, WorkerId(12), &body(4));
    assert_eq!(to_first.lifecycle_fence, FENCE);
    assert_eq!(to_second.lifecycle_fence, FENCE);
    assert_eq!(to_first.target_worker_generation, GENERATION);
    assert_eq!(to_second.target_worker_generation, GENERATION);
    assert_eq!(to_first.target_worker_id, 11);
    assert_eq!(to_second.target_worker_id, 12);
}
