//! What each scheduler can say about one job generation's live workers (M11.T26f, design
//! M11.D39e(v)).
//!
//! [`Scheduler::observe_generation`] is the one place the controller decides whether "this
//! scheduler does not list that worker" may be read as *"that worker generation has
//! terminated"* — one of the exactly three facts M11.D39e(v) allows to settle an issued
//! `StartExecution`. Every implementation answers it, and the answers are not interchangeable:
//! reading an untracking scheduler's empty listing as a termination would settle every target of
//! every job the moment it was asked, which is the false settlement the whole fence exists to
//! prevent.
//!
//! The answer is per **generation** rather than per scheduler type, and PR #167 round 2 is why.
//! A process-local registry covers the generations *this* controller incarnation started; a
//! process worker outlives the controller that spawned it and a node worker outlives it by
//! construction. So the same `ProcessScheduler` type is first-hand evidence about a generation it
//! launched and no evidence at all about one it inherited, and only a value can express that.

use std::collections::HashMap;

use arroyo_datastream::logical::{LogicalGraph, LogicalProgram};
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc::node_grpc_server::{NodeGrpc, NodeGrpcServer};
use arroyo_rpc::grpc::rpc::{
    GetWorkersReq, GetWorkersResp, RegisterNodeReq, StartWorkerReq, StartWorkerResp, StopWorkerReq,
    StopWorkerResp, StopWorkerStatus,
};
use arroyo_types::{JobId, PipelineId, WorkerId};
use std::sync::Arc;

use super::embedded::EmbeddedScheduler;
use super::kubernetes::KubernetesScheduler;
use super::{
    GenerationObservation, ManualScheduler, NodeScheduler, ProcessScheduler, Scheduler,
    StartPipelineReq,
};

const JOB: &str = "job_1";
const GENERATION: u64 = 7;

/// A start request that schedules **no** worker: `slots: 0` rounds to zero processes.
///
/// It is the production entry point all the same, and that is the point of using it — the launch
/// record under test is written by `start_workers` itself, so these rows exercise the same write
/// production does rather than a fixture that reaches into the scheduler's private state.
fn start_nothing(job_id: &str, generation: u64) -> StartPipelineReq {
    StartPipelineReq {
        name: "pipeline".to_string(),
        program: LogicalProgram::new(LogicalGraph::default(), Default::default()),
        wasm_path: String::new(),
        pipeline_id: PipelineId(Arc::new("pipeline_1".to_string())),
        organization_id: "org_1".to_string(),
        job_id: JobId(Arc::new(job_id.to_string())),
        hash: String::new(),
        generation,
        slots: 0,
        env_vars: HashMap::new(),
        pipeline_tags: HashMap::new(),
        scheduler_config: serde_json::json!({}),
    }
}

/// Every scheduler states what it can observe about a generation it never started, and the four
/// that cannot say so with the report an operator reads.
///
/// The `Untracked` arms are asserted whole — scheduler name and reason — rather than by variant,
/// because the reason is the payload: a job that will not leave `Fencing` because its
/// deployment's scheduler keeps no worker registry is a different report from one held by a
/// controller restart, and this is the only place the difference is written down.
///
/// Kubernetes is `Untracked` because its pod listing maps every pod to `WorkerId(1)`. That is a
/// live availability cost — a K8s deployment discharges a recovered obligation only by
/// acknowledgement — and it is recorded here so that "fix the listing" and "flip this answer"
/// stay one change rather than two.
///
/// Embedded is the one row that is `Live` without having launched anything, and its
/// justification is the whole of why the gate exists elsewhere: its workers are tasks in this
/// process, so a controller that did not start them is a controller they did not outlive.
#[tokio::test]
async fn every_scheduler_says_what_it_can_observe_about_a_generation_it_never_started() {
    let process = ProcessScheduler::new();
    let node = NodeScheduler::new();
    let embedded = EmbeddedScheduler::new();
    let manual = ManualScheduler::new();
    let kubernetes = KubernetesScheduler::with_config(None, config().kubernetes_scheduler.clone());

    let mut answers: Vec<(&str, GenerationObservation)> = vec![];
    for (name, scheduler) in [
        ("process", &process as &dyn Scheduler),
        ("node", &node as &dyn Scheduler),
        ("embedded", &embedded as &dyn Scheduler),
        ("manual", &manual as &dyn Scheduler),
        ("kubernetes", &kubernetes as &dyn Scheduler),
    ] {
        answers.push((
            name,
            scheduler
                .observe_generation(JOB, GENERATION)
                .await
                .expect("every scheduler here answers without erroring"),
        ));
    }

    assert_eq!(
        answers,
        vec![
            (
                "process",
                GenerationObservation::Untracked {
                    scheduler: "process",
                    why: "this controller did not start that worker generation, and a process \
                          worker outlives the controller that did, so an empty registry is not \
                          evidence that the generation has terminated",
                },
            ),
            (
                "node",
                GenerationObservation::Untracked {
                    scheduler: "node",
                    why: "this controller did not place that worker generation on any node, and \
                          a node worker outlives the controller that did, so an empty registry \
                          is not evidence that the generation has terminated",
                },
            ),
            ("embedded", GenerationObservation::Live(vec![])),
            (
                "manual",
                GenerationObservation::Untracked {
                    scheduler: "manual",
                    why: "its workers are started by an operator and it keeps no registry of \
                          them, so an empty listing says nothing about whether a generation has \
                          terminated",
                },
            ),
            (
                "kubernetes",
                GenerationObservation::Untracked {
                    scheduler: "kubernetes",
                    why: "its pod listing does not carry the worker id the controller assigned, \
                          so it cannot say that a particular worker generation has terminated",
                },
            ),
        ],
        "every scheduler in the crate answers, and the only one whose evidence needs no launch \
         record is the one whose workers cannot outlive this process"
    );
}

/// A controller that restarts does not read its own empty registry as termination.
///
/// The composition PR #167 round 2 names, as behaviour rather than as an enum value: the
/// scheduler that *started* the generation answers `Live` for it, and a second scheduler value —
/// which is what a restarted controller constructs — answers `Untracked` for the same job and
/// generation, from an identically empty registry. The two differ in nothing but which one did
/// the launching, which is exactly the fact a per-type answer threw away.
///
/// The same worker id is deliberately absent from both listings: the first scheduler's `Live` is
/// empty too. That is the sharp end of it — an empty `Live` settles every target, so if the
/// restarted controller answered `Live` it would settle a cluster it has never seen.
#[tokio::test]
async fn a_recreated_controller_reads_nothing_into_its_empty_registry() {
    for (name, started, inherited) in [
        (
            "process",
            Box::new(ProcessScheduler::new()) as Box<dyn Scheduler>,
            Box::new(ProcessScheduler::new()) as Box<dyn Scheduler>,
        ),
        (
            "node",
            Box::new(NodeScheduler::new()) as Box<dyn Scheduler>,
            Box::new(NodeScheduler::new()) as Box<dyn Scheduler>,
        ),
    ] {
        // `node` needs a node with free slots before it can place anything; with `slots: 0` both
        // schedulers take the launch record and place nothing, which is the state under test.
        started
            .start_workers(start_nothing(JOB, GENERATION))
            .await
            .unwrap_or_else(|e| panic!("{name}: starting zero workers is not an error: {e:?}"));

        assert_eq!(
            started
                .observe_generation(JOB, GENERATION)
                .await
                .expect("the launching scheduler answers"),
            GenerationObservation::Live(vec![]),
            "{name}: the controller that started the generation reads its own registry"
        );
        assert!(
            matches!(
                inherited
                    .observe_generation(JOB, GENERATION)
                    .await
                    .expect("the restarted scheduler answers"),
                GenerationObservation::Untracked { .. }
            ),
            "{name}: a controller that did not start the generation has an empty registry and no \
             evidence, and must not settle the cluster it inherited"
        );
        // And it is scoped to the generation, not to the job: the next attempt raises the
        // generation, so the launch record must not vouch for one this controller never started.
        assert!(
            matches!(
                started
                    .observe_generation(JOB, GENERATION + 1)
                    .await
                    .expect("the launching scheduler answers about the next generation too"),
                GenerationObservation::Untracked { .. }
            ),
            "{name}: launching one generation says nothing about the next"
        );
    }
}

/// **PR #167 round 6.** A stop *request* is not an observed termination.
///
/// `observe_generation` reads an absent registry entry as authoritative evidence that a worker
/// generation has terminated, and the fencing path releases obligations on that evidence. The
/// entry therefore has to leave only on an exit that was actually observed. It used to leave on
/// the *request*: `stop_workers` removed it before sending the shutdown, and the task that owns
/// the child removed it again on receiving one — under
/// `process_scheduler.shutdown_with_controller = false` without killing anything at all. A worker
/// asked to stop, still running, was reported gone.
///
/// The fixture reproduces that shape without a child process, because the claim is about the
/// registry and not about `Command`: an entry whose shutdown receiver nobody has polled and whose
/// `finished_rx` has not completed is exactly a worker that has been asked and has not exited.
#[tokio::test]
async fn a_stop_request_is_not_an_observed_termination() {
    let process = ProcessScheduler::new();
    process
        .start_workers(start_nothing(JOB, GENERATION))
        .await
        .expect("starting zero workers is not an error");

    // A worker of that generation, in the state the spawn path leaves one in: a shutdown channel
    // nobody is reading and an exit nobody has signalled.
    let (shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();
    let (_finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    process.workers.lock().await.insert(
        WorkerId(42),
        super::ProcessWorker {
            pipeline_id: PipelineId(Arc::new("pipeline_1".to_string())),
            job_id: JobId(Arc::new(JOB.to_string())),
            generation: GENERATION,
            shutdown_tx: Some(shutdown_tx),
            finished_rx,
        },
    );

    process
        .stop_workers(JOB, Some(GENERATION), false)
        .await
        .expect("asking a worker to stop is not an error");

    assert_eq!(
        process
            .observe_generation(JOB, GENERATION)
            .await
            .expect("the evidence listing answers"),
        GenerationObservation::Live(vec![WorkerId(42)]),
        "a worker that has been asked to stop and has not exited is still able to act, and the \
         evidence listing must say so — releasing a fencing obligation on this absence is what \
         lets a delayed request be applied by a worker nobody killed"
    );
}

/// A node worker the controller could not reach stays in the evidence listing.
///
/// `NodeScheduler::stop_worker` sets `running = false` when it *fails* to reach a worker's node —
/// "this likely means it is dead" — and `workers_for_job` filters that worker out. Read as
/// termination evidence, that settles the exact target a partition is hiding: the one still able
/// to apply a delayed `StartExecution`. So `observe_generation` ignores the flag, and only a
/// `worker_finished` report — which removes the entry outright — settles anything.
///
/// The partition is real rather than described: a node answers `start_worker` on loopback, the
/// worker is placed through the production path, and then the node stops answering. Nothing here
/// writes `running` by hand — `stop_workers` does it, for the reason production does.
#[tokio::test]
async fn a_node_worker_the_controller_could_not_reach_is_not_reported_terminated() {
    const WORKER: u64 = 42;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let node_server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(NodeGrpcServer::new(ReachableNode))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    let node = NodeScheduler::new();
    node.register_node(RegisterNodeReq {
        machine_id: "machine_1".to_string(),
        task_slots: 4,
        addr,
    })
    .await;

    let mut request = start_nothing(JOB, GENERATION);
    request.slots = 1;
    node.start_workers(request)
        .await
        .expect("the node answers, so the worker is placed");
    assert_eq!(
        node.observe_generation(JOB, GENERATION)
            .await
            .expect("the evidence listing answers"),
        GenerationObservation::Live(vec![WorkerId(WORKER)]),
        "the placed worker is live while its node answers"
    );

    // The partition. The node stops answering, and `stop_workers` gives up on the worker.
    node_server.abort();
    node.stop_workers(JOB, Some(GENERATION), false)
        .await
        .expect("a stop this controller could not deliver is not an error");

    assert_eq!(
        node.workers_for_job(JOB, Some(GENERATION))
            .await
            .expect("the stop listing answers"),
        vec![],
        "the listing `stop_workers` drives has given up on this worker"
    );
    assert_eq!(
        node.observe_generation(JOB, GENERATION)
            .await
            .expect("the evidence listing answers"),
        GenerationObservation::Live(vec![WorkerId(WORKER)]),
        "and the evidence listing still names it, because failing to reach a worker is not \
         evidence that it is gone — it names precisely the worker that might still act"
    );
}

/// A node that accepts one worker placement, until its server is stopped.
struct ReachableNode;

#[tonic::async_trait]
impl NodeGrpc for ReachableNode {
    async fn start_worker(
        &self,
        _: tonic::Request<StartWorkerReq>,
    ) -> Result<tonic::Response<StartWorkerResp>, tonic::Status> {
        Ok(tonic::Response::new(StartWorkerResp { worker_id: 42 }))
    }
    async fn get_workers(
        &self,
        _: tonic::Request<GetWorkersReq>,
    ) -> Result<tonic::Response<GetWorkersResp>, tonic::Status> {
        Ok(tonic::Response::new(GetWorkersResp { statuses: vec![] }))
    }
    async fn stop_worker(
        &self,
        _: tonic::Request<StopWorkerReq>,
    ) -> Result<tonic::Response<StopWorkerResp>, tonic::Status> {
        Ok(tonic::Response::new(StopWorkerResp {
            status: StopWorkerStatus::Stopped as i32,
        }))
    }
}
