#![allow(clippy::new_without_default)]
// TODO: factor out complex types
#![allow(clippy::type_complexity)]
// https://github.com/rust-lang/rust-clippy/issues/12908
#![allow(clippy::needless_lifetimes)]

use anyhow::Result;
use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::grpc::rpc::controller_grpc_server::{ControllerGrpc, ControllerGrpcServer};
use arroyo_rpc::grpc::rpc::job_controller_grpc_server::{
    JobControllerGrpc, JobControllerGrpcServer,
};
use arroyo_rpc::grpc::rpc::{
    GrpcOutputSubscription, HeartbeatNodeReq, HeartbeatNodeResp, HeartbeatReq, HeartbeatResp,
    JobMetricsReq, JobMetricsResp, NonfatalErrorReq, OutputData, RegisterNodeReq, RegisterNodeResp,
    RegisterWorkerReq, RegisterWorkerResp, SinkDataReq, SinkDataResp, TaskCheckpointCompletedReq,
    TaskCheckpointCompletedResp, TaskCheckpointEventReq, TaskCheckpointEventResp, TaskFailedReq,
    TaskFailedResp, TaskFinishedReq, TaskFinishedResp, TaskStartedReq, TaskStartedResp,
    WorkerErrorRes, WorkerFinishedReq, WorkerFinishedResp, WorkerInitializationCompleteReq,
    WorkerInitializationCompleteResp,
};
use arroyo_rpc::public_ids::{IdTypes, generate_id};
use arroyo_rpc::state_backend::{
    StateBackendError, StateBackendSelector, validate_unchanged_job_selector,
};
use arroyo_rpc::worker_types::{RunningMessage, TaskFailedEvent};
use arroyo_rpc::{StateContext, config, errors};
use arroyo_server_common::shutdown::ShutdownGuard;
use arroyo_server_common::wrap_start;
use arroyo_types::{MachineId, PipelineId, WorkerId, from_micros};
use arroyo_worker::job_controller::job_metrics::JobMetrics;
use cornucopia_async::DatabaseSource;
use lazy_static::lazy_static;
use prometheus::{IntGaugeVec, register_int_gauge_vec};
use states::{Created, State, StateMachine};
use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{self, AtomicU64};
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

//pub mod compiler;
pub mod job_controller;
pub mod schedulers;
mod states;

const TTL_PIPELINE_CLEANUP_TIME: Duration = Duration::from_secs(60 * 60);

lazy_static! {
    static ref JOBS_BY_STATE: IntGaugeVec = register_int_gauge_vec!(
        "arroyo_controller_jobs",
        "Current number of jobs by controller state",
        &["state"]
    )
    .unwrap();
}

fn metric_job_state<'a>(state: Option<&'a str>, failure_domain: Option<&str>) -> &'a str {
    let state = state.unwrap_or("Created");
    if state == "Failed" && failure_domain == Some("user") {
        "UserFailed"
    } else {
        state
    }
}

fn job_state_counts<'a>(
    jobs: impl Iterator<Item = (Option<&'a str>, Option<&'a str>)>,
) -> HashMap<&'a str, i64> {
    let mut counts = HashMap::new();
    for (state, failure_domain) in jobs {
        *counts
            .entry(metric_job_state(state, failure_domain))
            .or_default() += 1;
    }
    counts
}

fn update_job_state_metrics(counts: &HashMap<&str, i64>) {
    JOBS_BY_STATE.reset();
    for (state, count) in counts {
        JOBS_BY_STATE.with_label_values(&[state]).set(*count);
    }
}

include!(concat!(env!("OUT_DIR"), "/controller-sql.rs"));

use crate::schedulers::{ManualScheduler, NodeScheduler, ProcessScheduler, Scheduler};
use types::public::LogLevel;
use types::public::{RestartMode, StopMode};

pub const CHECKPOINTS_TO_KEEP: u32 = 5;

#[derive(PartialEq, Clone, Debug)]
pub struct JobConfig {
    id: Arc<String>,
    organization_id: String,
    pipeline_name: String,
    pipeline_id: i64,
    stop_mode: StopMode,
    checkpoint_interval: Duration,
    ttl: Option<Duration>,
    parallelism_overrides: HashMap<u32, usize>,
    restart_nonce: i32,
    restart_mode: RestartMode,
    ignore_state_before_epoch: Option<i32>,
    /// Per-job environment variables forwarded to workers at scheduling time.
    env_vars: serde_json::Value,
    /// Per-job scheduler configuration overlay as raw JSON (same
    /// shape as the controller-wide scheduler config). The scheduler
    /// interprets this; the controller treats it as opaque. An empty
    /// object is the no-override case.
    scheduler_config: serde_json::Value,
    /// The state backend this job stores its operator state in. Parsed once, when the
    /// row is read, so every later use — the workers' `StartExecutionReq`, the
    /// checkpoint metadata store — carries an already-validated value rather than a
    /// string that has to be re-interpreted.
    state_backend: StateBackendSelector,
}

impl JobConfig {
    /// Builds a job config from a `job_configs`/`job_statuses` row, under the state
    /// backend the job's *execution* is running with.
    ///
    /// `state_backend` is handed in rather than read from the row on purpose. The row is
    /// the operator's editable request; the selector is a property of the running job that
    /// was fixed when its state machine was created and is recovered from the job's own
    /// execution record. Substituting it here, at the single point the row is turned into
    /// a configuration, is what guarantees every consumer of a `JobConfig` — the workers'
    /// `StartExecutionReq`, the checkpoint metadata store, the generation publication —
    /// sees the selector the job is actually using, whatever the row now says.
    /// [`classify_polled_row`] is what decides the two apart and refuses the difference.
    fn from_row(
        row: queries::controller_queries::Job,
        state_backend: StateBackendSelector,
    ) -> Self {
        Self {
            id: Arc::new(row.id),
            organization_id: row.org_id,
            pipeline_id: row.pipeline_id,
            pipeline_name: row.pipeline_name,
            stop_mode: row.stop,
            checkpoint_interval: Duration::from_micros(row.checkpoint_interval_micros as u64),
            ttl: row.ttl_micros.map(|t| Duration::from_micros(t as u64)),
            parallelism_overrides: row
                .parallelism_overrides
                .as_object()
                .unwrap()
                .into_iter()
                .filter_map(|(k, v)| Some((u32::from_str(k).ok()?, v.as_u64()? as usize)))
                .collect(),
            restart_nonce: row.config_restart_nonce,
            restart_mode: row.restart_mode,
            ignore_state_before_epoch: row.ignore_state_before_epoch,
            env_vars: row.env_vars,
            scheduler_config: row.scheduler_config,
            state_backend,
        }
    }
}

/// One polled `job_configs` row, resolved against the job's own execution.
pub(crate) struct PolledJob {
    /// The state backend this execution of the job runs with. Recovered from the job's
    /// persisted execution record, or — for a job that has never had one — taken from the
    /// row, which is where a job's first and only choice of backend is made.
    pub(crate) execution_selector: StateBackendSelector,
    /// The row, already carrying `execution_selector` rather than whatever the row's own
    /// `state_backend` column says.
    pub(crate) config: JobConfig,
    /// Why the row's own `state_backend` was refused, if it was: either it names a
    /// different backend than the job is running with, or it cannot be interpreted at all.
    pub(crate) refusal: Option<StateBackendError>,
}

/// Resolves a polled job row against the job's own execution record.
///
/// The row is the operator's editable request; it is not the authority for the state
/// backend of a job that already exists. A job's execution records its selector in
/// `job_statuses.state_context` when its state machine is created, and *that* is what a
/// controller recovers on startup — otherwise a controller that has just been restarted
/// would re-baseline the value from an edited row and could go on to administer, and
/// reconnect to, a job that is still running under something else.
///
/// Returns `None` when there is nothing for the controller to do with the row: the row's
/// selector cannot be interpreted and the job has no execution on record, or the execution
/// record itself is unreadable. In both cases the job neither starts nor is adopted, and
/// nothing is guessed at. That must not be a failure for the *update thread*, though:
/// returning the error from the polling loop would stop every other job on the cluster
/// from being polled because one row is bad, so it is reported per row on each poll, which
/// is also what makes the condition visible until an operator fixes it.
fn classify_polled_row(
    row: queries::controller_queries::Job,
    status: &JobStatus,
) -> Option<PolledJob> {
    let job_id = row.id.clone();

    // Persisted, and therefore untrusted: a recorded value nobody recognizes is refused,
    // never guessed at, because guessing is what would pick a backend for a live job.
    let recorded = match status.recorded_execution_selector() {
        Ok(recorded) => recorded,
        Err(e) => {
            error!(job_id = %job_id, error = %e,
                "refusing job whose recorded execution state backend is unusable");
            return None;
        }
    };

    let requested = StateBackendSelector::normalize(&row.state_backend, &format!("job {job_id}"));

    let (execution_selector, refusal) = match (recorded, requested) {
        // No execution on record: this row is the job's declaration, and starting is the
        // only moment a job chooses its backend.
        (None, Ok(requested)) => (requested, None),
        // ...and a declaration that cannot be interpreted is never downgraded to a
        // default, so the job simply never starts. There is no execution to administer
        // and nothing to fail.
        (None, Err(e)) => {
            error!(job_id = %job_id, error = %e, "refusing job with an unusable config");
            return None;
        }
        // An execution exists, so it is the authority. The job goes on being administered
        // under the backend it is running with, and the row's value is refused.
        (Some(recorded), Ok(requested)) => (
            recorded,
            validate_unchanged_job_selector(&job_id, recorded, requested).err(),
        ),
        (Some(recorded), Err(e)) => (recorded, Some(e)),
    };

    let mut config = JobConfig::from_row(row, execution_selector);

    if refusal.is_some() {
        // Nothing else about a refused row is adopted either — and specifically not its
        // restart nonce, because `Failed` restarts a job when the row's nonce differs
        // from the status's. Pinning it to the status's own value is what makes "a
        // refused row must not restart the job under the value that was refused" a
        // property of the one place the row is read, rather than of each state that
        // could act on it.
        config.restart_nonce = status.restart_nonce;
    }

    Some(PolledJob {
        execution_selector,
        config,
        refusal,
    })
}

/// Decodes the controller's own durable record of a job's execution.
///
/// `job_statuses.state_context` is persisted state, and therefore untrusted input. A blob
/// that cannot be decoded is *not* turned into "this job has no execution": for a job that
/// does have one, that would erase its only selector authority, and the editable
/// configuration row would be adopted in its place — the very substitution
/// [`classify_polled_row`] exists to prevent.
///
/// `None` therefore means *skip this job*. It is reported on every poll, so the condition
/// stays visible until an operator repairs the row, and it is scoped to the one job: the
/// rest of the cluster goes on being polled.
fn decode_state_context(job_id: &str, raw: &serde_json::Value) -> Option<StateContext> {
    match serde_json::from_value::<StateContext>(raw.clone()) {
        Ok(state_context) => Some(state_context),
        Err(e) => {
            error!(job_id = %job_id, original =? raw, error =? e,
                "skipping job whose execution record cannot be decoded");
            None
        }
    }
}

/// Turns one polled row into the job's status and the configuration the state machine
/// should act on, or `None` if the update thread must move on without touching this job.
///
/// This is the whole of the per-row decision, in one place the tests can drive: decode the
/// execution record, build the status from it, then resolve the row against it. Every
/// `None` here is fail-closed for the job and fail-open for the cluster — the poll loop
/// skips to the next row rather than erroring out.
fn classify_polled_job(row: queries::controller_queries::Job) -> Option<(PolledJob, JobStatus)> {
    let id = Arc::new(row.id.clone());

    // Everything the status needs is read before the config consumes the row, so a
    // rejected row can skip just this job.
    let state_context = decode_state_context(&id, &row.state_context)?;

    let status = JobStatus {
        id: id.clone(),
        generation: row.run_id.unwrap_or(0).max(0) as u64,
        state: row
            .state
            .clone()
            .unwrap_or_else(|| Created {}.name().to_string()),
        start_time: row.start_time,
        finish_time: row.finish_time,
        tasks: row.tasks,
        failure_message: row.failure_message.clone(),
        failure_domain: row.failure_domain.clone(),
        restarts: row.restarts,
        pipeline_path: row.pipeline_path.clone(),
        wasm_path: row.wasm_path.clone(),
        restart_nonce: row.status_restart_nonce,
        state_context,
    };

    // Resolved against the job's own execution record, not just parsed: a controller that
    // has been restarted must recover the selector a running job is using rather than
    // re-baseline it from a row that may have been edited while the controller was down.
    let polled = classify_polled_row(row, &status)?;

    Some((polled, status))
}

/// Per-pipeline data that doesn't change for the lifetime of a job.
#[derive(Clone, Debug)]
pub struct PipelineInfo {
    pub pipeline_id: PipelineId,
    pub state_url: Option<String>,
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct JobStatus {
    id: Arc<String>,
    generation: u64,
    state: String,
    start_time: Option<OffsetDateTime>,
    finish_time: Option<OffsetDateTime>,
    tasks: Option<i32>,
    failure_message: Option<String>,
    failure_domain: Option<String>,
    restarts: i32,
    pipeline_path: Option<String>,
    wasm_path: Option<String>,
    restart_nonce: i32,
    state_context: StateContext,
}

impl JobStatus {
    /// Whether this job has an execution on record at all.
    ///
    /// "An execution exists" is a fact about the job's own durable status, and deliberately
    /// not about which controller mode it runs in. Before the execution selector existed, a
    /// *controller*-mode job recorded nothing in `state_context` — no leader and no
    /// selector — so leader presence cannot be the test: a pre-upgrade controller-mode job
    /// in `Running`, `Compiling` or `Scheduling` would read as a job that had never
    /// started, and its editable `job_configs.state_backend` would be adopted as the
    /// backend of a job whose workers, table configs and checkpoints are parquet.
    ///
    /// Two independent facts are consulted, and either is sufficient:
    ///
    /// * The status leaves `Created` exactly once — when the controller starts running the
    ///   job — and never goes back, so any other state name means workers have been, or
    ///   are being, brought up for this job. That includes the terminal states: a `Stopped`
    ///   or `Failed` job still owns the checkpoints its execution wrote, and restarting it
    ///   restores them.
    /// * `start_time` is stamped the first time the job reaches `Running` and is never
    ///   cleared except to be re-stamped, so it survives a status row whose state name this
    ///   build does not recognize.
    ///
    /// A status row that has neither is a job that has not started, and *that* is the one
    /// moment `job_configs.state_backend` is allowed to choose.
    fn has_execution(&self) -> bool {
        self.state != Created {}.name() || self.start_time.is_some()
    }

    /// The state backend this job's *execution* recorded for itself, if it has one.
    ///
    /// This is the row-independent half of the job's selector. `job_statuses.state_context`
    /// is written by the controller about a running job, never by an operator editing a
    /// configuration, so it survives an edit to `job_configs.state_backend` and is what a
    /// restarted controller recovers the job's real backend from.
    ///
    /// `Ok(None)` means no execution is on record at all and the configuration row is
    /// therefore still free to choose. An execution that *is* on record but recorded no
    /// selector was started by a build that had none — parquet is the only backend such a
    /// build could have used — which is the same "absent means parquet" rule every other
    /// persisted value in this system follows, and is why a job running across the upgrade
    /// to this build is not re-baselined from its row either.
    ///
    /// Whether an execution exists is [`Self::has_execution`]'s question, answered from the
    /// job's durable status rather than from the presence of a leader: leader context is
    /// only ever written in worker-leader mode, so testing for it made the upgrade rule
    /// hold for leader-mode jobs and silently not hold for controller-mode ones.
    ///
    /// # Errors
    ///
    /// Returns [`StateBackendError::UnknownValue`] if the recorded value is neither empty
    /// nor a known backend name. It is never defaulted: a value nobody recognizes leaves
    /// the controller unable to say what the job is running with, and picking one would be
    /// picking it for a job that is still running.
    fn recorded_execution_selector(
        &self,
    ) -> Result<Option<StateBackendSelector>, StateBackendError> {
        match &self.state_context.execution_selector {
            Some(raw) => {
                StateBackendSelector::normalize(raw, &format!("job {} execution", self.id))
                    .map(Some)
            }
            // An execution with no recorded selector predates the field, in either
            // controller mode: a leader on record is one proof it exists, the job's own
            // durable status is the other, and a build with no selector could only have
            // run parquet.
            None if self.has_execution() || self.state_context.leader.is_some() => {
                Ok(Some(StateBackendSelector::DEFAULT))
            }
            None => Ok(None),
        }
    }

    pub async fn update_db(&self, database: &DatabaseSource) -> Result<(), String> {
        let c = database.client().await.map_err(|e| format!("{e:?}"))?;
        let res = queries::controller_queries::execute_update_job_status(
            &c,
            &self.state,
            &self.start_time,
            &self.finish_time,
            &self.tasks,
            &self.failure_message,
            &self.failure_domain,
            &self.restarts,
            &self.pipeline_path,
            &self.wasm_path,
            &(self.generation as i64),
            &self.restart_nonce,
            &serde_json::to_value(&self.state_context).expect("failed to serialize"),
            &*self.id,
        )
        .await
        .map_err(|e| format!("{e:?}"))?;

        if res == 0 {
            Err("Job status does not exist".to_string())
        } else {
            Ok(())
        }
    }
}

fn job_in_final_state(config: &JobConfig, status: &JobStatus) -> bool {
    match status.state.as_str() {
        "Stopped" | "Finished" => config.stop_mode != StopMode::none,
        "Failed" => config.restart_nonce == status.restart_nonce,
        _ => false,
    }
}

/// A refusal of the job's persisted configuration, as it travels to the job's own state
/// task.
///
/// A refusal is queued rather than applied in place, and the queue is FIFO and cannot be
/// retracted, so between the poll that raised the refusal and the state that finally reads
/// it the operator can have repaired the row — which is exactly the remedy the refusal
/// asks for. Failing the job then would fail it for a configuration that no longer exists.
///
/// So a refusal carries the version it was raised at, together with a handle on the
/// version its state machine currently holds. They differ once the refusal has been
/// superseded — by a repair, by a different refusal, or by a stop that answers it — and a
/// superseded refusal is discarded instead of failing the job. Nothing about this makes
/// the *send* blocking: the version is stamped and read without waiting on anything.
#[derive(Clone, Debug)]
pub struct RefusedConfig {
    error: StateBackendError,
    /// The value [`Self::current`] held when this refusal was offered to the queue.
    version: u64,
    /// The state machine's live refusal version. Shared, so it keeps moving after this
    /// message has been queued.
    current: Arc<AtomicU64>,
}

impl RefusedConfig {
    pub(crate) fn new(error: StateBackendError, version: u64, current: Arc<AtomicU64>) -> Self {
        Self {
            error,
            version,
            current,
        }
    }

    /// Whether this refusal still describes the job's configuration.
    pub(crate) fn is_current(&self) -> bool {
        self.current.load(atomic::Ordering::SeqCst) == self.version
    }

    /// The version this refusal was raised at.
    ///
    /// Read by the state task's `RefusalGate`, which is consulted before every state and
    /// so has to be able to tell a refusal it has already turned fatal from a new one.
    pub(crate) fn version(&self) -> u64 {
        self.version
    }

    /// The error this refusal reports, or `None` if it has been superseded since it was
    /// queued and must not be acted on.
    pub(crate) fn into_current_error(self) -> Option<StateBackendError> {
        self.is_current().then_some(self.error)
    }
}

#[derive(Debug)]
pub enum JobMessage {
    ConfigUpdate(JobConfig),
    /// The job's persisted configuration was refused: the row either names a state
    /// backend other than the one this execution is running with, or holds a value that
    /// cannot be interpreted at all.
    ///
    /// A refusal is delivered as its own message, and never as a [`JobMessage::ConfigUpdate`]
    /// carrying the refused row, precisely so that no state can apply any part of it. The
    /// state machine's authoritative config is left holding the value the job's workers,
    /// table configs and checkpoints were built from, and every state routes this message
    /// to [`states::JobContext::handle`], which fails the job — unless the refusal has
    /// been superseded in the meantime, which is what [`RefusedConfig`] is for.
    ConfigRefused(RefusedConfig),
    WorkerConnect {
        worker_id: WorkerId,
        machine_id: MachineId,
        generation: u64,
        rpc_address: String,
        data_address: String,
        slots: usize,
    },
    WorkerInitializationComplete {
        worker_id: WorkerId,
        success: bool,
        error_message: Option<String>,
    },
    TaskStarted {
        worker_id: WorkerId,
        task_id: u32,
        subtask_idx: u32,
    },
    RunningMessage(RunningMessage),
}

#[derive(Clone)]
pub struct ControllerServer {
    job_state: Arc<tokio::sync::Mutex<HashMap<String, StateMachine>>>,
    data_txs: Arc<tokio::sync::Mutex<HashMap<String, Vec<Sender<Result<OutputData, Status>>>>>>,
    scheduler: Arc<dyn Scheduler>,
    metrics: Arc<RwLock<HashMap<Arc<String>, JobMetrics>>>,
    db: DatabaseSource,
}

#[allow(clippy::result_large_err)]
fn job_id_from_context(worker_context: &Option<rpc::WorkerContext>) -> Result<String, Status> {
    Ok(worker_context
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing worker_context"))?
        .job_id
        .clone())
}

#[tonic::async_trait]
impl ControllerGrpc for ControllerServer {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerReq>,
    ) -> Result<Response<RegisterWorkerResp>, Status> {
        let remote_addr = request.remote_addr();
        let req = request.into_inner();
        let worker = req
            .worker_context
            .ok_or_else(|| Status::invalid_argument("missing worker_context"))?;
        info!(
            job_id = %worker.job_id,
            pipeline_id = worker.pipeline_id,
            "Worker registered: {:?} -- {:?}",
            worker,
            remote_addr
        );

        self.send_to_job_queue(
            &worker.job_id,
            JobMessage::WorkerConnect {
                worker_id: WorkerId(worker.worker_id),
                machine_id: MachineId(worker.machine_id.into()),
                generation: worker.generation,
                rpc_address: req.rpc_address,
                data_address: req.data_address,
                slots: req.slots as usize,
            },
        )
        .await?;

        Ok(Response::new(RegisterWorkerResp {}))
    }

    async fn task_started(
        &self,
        request: Request<TaskStartedReq>,
    ) -> Result<Response<TaskStartedResp>, Status> {
        let req = request.into_inner();
        let ctx = req
            .worker_context
            .ok_or_else(|| Status::invalid_argument("missing worker_context"))?;
        info!(
            job_id = %ctx.job_id,
            pipeline_id = ctx.pipeline_id,
            worker_id = ctx.worker_id,
            task_id = req.task_id,
            subtask_idx = req.subtask_idx,
            "task started"
        );

        self.send_to_job_queue(
            &ctx.job_id,
            JobMessage::TaskStarted {
                worker_id: WorkerId(ctx.worker_id),
                task_id: req.task_id,
                subtask_idx: req.subtask_idx,
            },
        )
        .await?;

        Ok(Response::new(TaskStartedResp {}))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeReq>,
    ) -> Result<Response<RegisterNodeResp>, Status> {
        let req = request.into_inner();
        info!(
            "Received node registration from {} at {} with {} slots",
            req.machine_id, req.addr, req.task_slots
        );

        self.scheduler.register_node(req).await;

        Ok(Response::new(RegisterNodeResp {}))
    }

    async fn heartbeat_node(
        &self,
        request: Request<HeartbeatNodeReq>,
    ) -> Result<Response<HeartbeatNodeResp>, Status> {
        self.scheduler.heartbeat_node(request.into_inner()).await?;
        Ok(Response::new(HeartbeatNodeResp {}))
    }

    async fn worker_finished(
        &self,
        request: Request<WorkerFinishedReq>,
    ) -> Result<Response<WorkerFinishedResp>, Status> {
        self.scheduler.worker_finished(request.into_inner()).await;
        Ok(Response::new(WorkerFinishedResp {}))
    }

    async fn send_sink_data(
        &self,
        request: Request<SinkDataReq>,
    ) -> Result<Response<SinkDataResp>, Status> {
        let req = request.into_inner();
        let mut data_txs = self.data_txs.lock().await;
        if let Some(v) = data_txs.get_mut(&req.job_id) {
            let output = OutputData {
                operator_id: req.operator_id,
                subtask_idx: req.subtask_idx,
                timestamps: req.timestamps,
                batch: req.batch,
                start_id: req.start_id,
                done: req.done,
            };

            let mut remove = HashSet::new();
            for (i, tx) in v.iter().enumerate() {
                match tx.try_send(Ok(output.clone())) {
                    Ok(_) => {}
                    Err(TrySendError::Closed(_)) => {
                        remove.insert(i);
                    }
                    Err(TrySendError::Full(_)) => {
                        debug!("queue full");
                    }
                }
            }

            let mut i = 0;
            v.retain(|_tx| {
                i += 1;
                !remove.contains(&(i - 1))
            });
        }
        Ok(Response::new(SinkDataResp::default()))
    }

    type SubscribeToOutputStream = ReceiverStream<Result<OutputData, Status>>;

    async fn subscribe_to_output(
        &self,
        request: Request<GrpcOutputSubscription>,
    ) -> Result<Response<Self::SubscribeToOutputStream>, Status> {
        let job_id = request.into_inner().job_id;
        if self
            .job_state
            .lock()
            .await
            .get(&job_id)
            .ok_or_else(|| Status::not_found(format!("Job {job_id} does not exist")))?
            .state
            .read()
            .unwrap()
            .as_str()
            != "Running"
        {
            return Err(Status::failed_precondition(
                "Job must be running to read output",
            ));
        }

        let (tx, rx) = tokio::sync::mpsc::channel(32);

        let mut data_txs = self.data_txs.lock().await;
        data_txs.entry(job_id).or_default().push(tx);

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn worker_initialization_complete(
        &self,
        request: Request<WorkerInitializationCompleteReq>,
    ) -> Result<Response<WorkerInitializationCompleteResp>, Status> {
        let req = request.into_inner();
        let ctx = req
            .worker_context
            .ok_or_else(|| Status::invalid_argument("missing worker_context"))?;
        info!(
            job_id = %ctx.job_id,
            pipeline_id = ctx.pipeline_id,
            "Worker {} initialization completed: success={}, error={:?}",
            ctx.worker_id,
            req.success,
            req.error_message
        );

        self.send_to_job_queue(
            &ctx.job_id,
            JobMessage::WorkerInitializationComplete {
                worker_id: WorkerId(ctx.worker_id),
                success: req.success,
                error_message: req.error_message,
            },
        )
        .await?;

        Ok(Response::new(WorkerInitializationCompleteResp {}))
    }
}

#[tonic::async_trait]
impl JobControllerGrpc for ControllerServer {
    async fn task_checkpoint_event(
        &self,
        request: Request<TaskCheckpointEventReq>,
    ) -> Result<Response<TaskCheckpointEventResp>, Status> {
        let req = request.into_inner();

        let ctx = req
            .worker_context
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing worker_context"))?;
        debug!(
            job_id = %ctx.job_id,
            pipeline_id = ctx.pipeline_id,
            "received task checkpoint event {:?}",
            req
        );
        let job_id = ctx.job_id.clone();

        self.send_to_job_queue(
            &job_id,
            JobMessage::RunningMessage(RunningMessage::TaskCheckpointEvent(req)),
        )
        .await?;

        Ok(Response::new(TaskCheckpointEventResp {}))
    }

    async fn task_checkpoint_completed(
        &self,
        request: Request<TaskCheckpointCompletedReq>,
    ) -> Result<Response<TaskCheckpointCompletedResp>, Status> {
        let req = request.into_inner();

        let ctx = req
            .worker_context
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing worker_context"))?;
        debug!(
            job_id = %ctx.job_id,
            pipeline_id = ctx.pipeline_id,
            "received task checkpoint completed {:?}",
            req
        );
        let job_id = ctx.job_id.clone();

        self.send_to_job_queue(
            &job_id,
            JobMessage::RunningMessage(RunningMessage::TaskCheckpointFinished(req)),
        )
        .await?;

        Ok(Response::new(TaskCheckpointCompletedResp {}))
    }

    async fn task_finished(
        &self,
        request: Request<TaskFinishedReq>,
    ) -> Result<Response<TaskFinishedResp>, Status> {
        let req = request.into_inner();

        let ctx = req
            .worker_context
            .ok_or_else(|| Status::invalid_argument("missing worker_context"))?;

        self.send_to_job_queue(
            &ctx.job_id,
            JobMessage::RunningMessage(RunningMessage::TaskFinished {
                worker_id: WorkerId(ctx.worker_id),
                time: from_micros(req.time),
                task_id: req.task_id,
                subtask_idx: req.subtask_idx,
            }),
        )
        .await?;

        Ok(Response::new(TaskFinishedResp {}))
    }

    async fn task_failed(
        &self,
        request: Request<TaskFailedReq>,
    ) -> Result<Response<TaskFailedResp>, Status> {
        let req = request.into_inner();
        let ctx = req
            .worker_context
            .ok_or_else(|| Status::invalid_argument("TaskFailedReq missing worker_context"))?;
        let err = req
            .error
            .ok_or_else(|| Status::invalid_argument("TaskFailedReq missing error"))?;

        self.send_to_job_queue(
            &ctx.job_id,
            JobMessage::RunningMessage(RunningMessage::TaskFailed(TaskFailedEvent {
                worker_id: WorkerId(ctx.worker_id),
                task_id: err.task_id,
                subtask_idx: err.subtask_idx,
                error_domain: err.error_domain().into(),
                retry_hint: err.retry_hint().into(),
                operator_id: err.operator_id,
                reason: err.error,
                details: err.details,
            })),
        )
        .await?;

        Ok(Response::new(TaskFailedResp {}))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatReq>,
    ) -> Result<Response<HeartbeatResp>, Status> {
        let req = request.into_inner();

        let job_id = job_id_from_context(&req.worker_context)?;

        self.send_to_job_queue(
            &job_id,
            JobMessage::RunningMessage(RunningMessage::WorkerHeartbeat {
                worker_id: WorkerId(req.worker_context.as_ref().unwrap().worker_id),
                time: Instant::now(),
            }),
        )
        .await?;

        return Ok(Response::new(HeartbeatResp {}));
    }

    async fn nonfatal_error(
        &self,
        request: Request<NonfatalErrorReq>,
    ) -> Result<Response<WorkerErrorRes>, Status> {
        let req = request.into_inner();
        let ctx = req
            .worker_context
            .ok_or_else(|| Status::invalid_argument("NonfatalErrorReq missing worker_context"))?;
        let err = req
            .error
            .ok_or_else(|| Status::invalid_argument("NonfatalErrorReq missing error"))?;

        info!(
            job_id = %ctx.job_id,
            pipeline_id = ctx.pipeline_id,
            operator_id = err.operator_id,
            message = "operator error",
            error_message = err.error,
            error_details = err.details
        );

        let client = self.db.client().await.unwrap();
        match queries::controller_queries::execute_create_job_log_message(
            &client,
            &generate_id(IdTypes::JobLogMessage),
            &ctx.job_id,
            &err.operator_id,
            &(err.subtask_idx as i64),
            &LogLevel::error,
            &err.error,
            &err.details,
            &errors::ErrorDomain::from(err.error_domain()).as_str(),
            &errors::RetryHint::from(err.retry_hint()).as_str(),
        )
        .await
        {
            Ok(_) => Ok(Response::new(WorkerErrorRes {})),
            Err(db_err) => Err(Status::from_error(Box::new(db_err))),
        }
    }

    async fn job_metrics(
        &self,
        request: Request<JobMetricsReq>,
    ) -> Result<Response<JobMetricsResp>, Status> {
        let job_id = request.into_inner().job_id;
        let metrics = self
            .metrics
            .read()
            .await
            .get(&job_id)
            .ok_or_else(|| Status::not_found("No metrics for job"))?
            .clone();

        // TODO: send this over in a more efficient format like protobuf
        Ok(Response::new(JobMetricsResp {
            metrics: serde_json::to_string(&metrics.get_groups()).unwrap(),
        }))
    }
}

impl ControllerServer {
    pub async fn new(database: DatabaseSource) -> Self {
        let scheduler: Arc<dyn Scheduler> = match &config().controller.scheduler {
            config::Scheduler::Node => {
                info!("Using node scheduler");
                Arc::new(NodeScheduler::new())
            }
            config::Scheduler::Kubernetes => {
                info!("Using kubernetes scheduler");
                Arc::new(schedulers::kubernetes::KubernetesScheduler::new().await)
            }
            config::Scheduler::Embedded => {
                info!("Using embedded scheduler");
                Arc::new(schedulers::embedded::EmbeddedScheduler::new())
            }
            config::Scheduler::Process => {
                info!("Using process scheduler");
                Arc::new(ProcessScheduler::new())
            }
            config::Scheduler::Manual => {
                info!("Using manual scheduler");
                Arc::new(ManualScheduler::new())
            }
        };

        Self {
            scheduler,
            data_txs: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            job_state: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            db: database,
            metrics: Default::default(),
        }
    }

    async fn send_to_job_queue(&self, job_id: &str, msg: JobMessage) -> Result<(), Status> {
        // Keep per-job backpressure from holding the global job map lock.
        let tx = {
            let jobs = self.job_state.lock().await;
            let Some(sm) = jobs.get(job_id) else {
                warn!(message = "Received message for unknown job id", %job_id);
                return Err(Status::failed_precondition(format!(
                    "No job with id {job_id}"
                )));
            };

            sm.sender().ok_or_else(|| {
                Status::failed_precondition(format!(
                    "Cannot handle message for {job_id}: State machine is inactive"
                ))
            })?
        };

        tx.send(msg).await.map_err(|_| {
            Status::failed_precondition(format!(
                "Cannot handle message for {job_id}: State machine is inactive"
            ))
        })
    }

    fn start_updater(&self, guard: ShutdownGuard) {
        let db = self.db.clone();
        let jobs = Arc::clone(&self.job_state);
        let scheduler = Arc::clone(&self.scheduler);
        let metrics = Arc::clone(&self.metrics);
        // Read once, here, and handed to every job's state machine. This is the controller
        // process's own identity and the only place the controller reads it from the
        // process-wide cell, so the code that stamps it into a worker takes it as an
        // argument and can be exercised without a process identity existing at all.
        let cluster_id = Arc::new(arroyo_server_common::get_cluster_id());

        let token = guard.token();

        let mut cleaned_at = Instant::now();

        let our_guard = guard.child("update-thread");
        our_guard.into_spawn_task(async move {
            while !token.is_cancelled() {
                let client = db.client().await?;
                let res = queries::controller_queries::fetch_all_jobs(&client).await?;
                let state_counts = job_state_counts(
                    res.iter()
                        .map(|p| (p.state.as_deref(), p.failure_domain.as_deref())),
                );
                update_job_state_metrics(&state_counts);

                for p in res {
                    // Fail-open for the poll loop — one unusable row must never stop the
                    // other jobs on the cluster from being polled — but not for the job
                    // itself. A job with an execution on record is adopted under that
                    // execution's backend and then routed to its own refusal path,
                    // whether or not this controller was the one that started it; a job
                    // with none still simply never starts; and a job whose execution
                    // record cannot be decoded is skipped rather than guessed at.
                    let Some((polled, status)) = classify_polled_job(p) else {
                        continue;
                    };
                    let id = Arc::clone(&status.id);

                    let mut jobs = jobs.lock().await;

                    if let Some(sm) = jobs.get_mut(&*id) {
                        sm.update(polled, status, &guard).await;
                    } else if !job_in_final_state(&polled.config, &status) {
                        jobs.insert(
                            (*id).clone(),
                            StateMachine::new(
                                polled,
                                status,
                                db.clone(),
                                scheduler.clone(),
                                cluster_id.clone(),
                                guard.clone_temporary(),
                                metrics.clone(),
                            )
                            .await,
                        );
                    }
                }

                if cleaned_at.elapsed() > Duration::from_secs(5) {
                    let res = queries::controller_queries::execute_clean_preview_pipelines(
                        &client,
                        &(OffsetDateTime::now_utc() - TTL_PIPELINE_CLEANUP_TIME),
                    )
                    .await?;
                    if res > 0 {
                        info!("Cleaned {res} preview pipelines from database");
                    }
                    cleaned_at = Instant::now();
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Ok(())
        });
    }

    pub async fn start(self, guard: ShutdownGuard) -> anyhow::Result<u16> {
        // let reflection = tonic_reflection::server::Builder::configure()
        //     .register_encoded_file_descriptor_set(arroyo_rpc::grpc::API_FILE_DESCRIPTOR_SET)
        //     .build_v1()
        //     .unwrap();

        let config = config();
        let addr = SocketAddr::new(config.controller.bind_address, config.controller.rpc_port);

        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;

        let controller_service = ControllerGrpcServer::new(self.clone())
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd);

        let job_controller_service = JobControllerGrpcServer::new(self.clone())
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd);

        let scheduler_shutdown_guard = guard.child("scheduler-shutdown");
        let scheduler_shutdown_token = scheduler_shutdown_guard.token();
        let scheduler = Arc::clone(&self.scheduler);
        tokio::spawn(async move {
            scheduler_shutdown_token.cancelled().await;
            scheduler.shutdown().await;
            drop(scheduler_shutdown_guard);
        });

        self.start_updater(guard.child("updater"));

        if let Some(tls_config) = config.get_tls_config(&config.controller.tls) {
            info!("Starting arroyo-controller with TLS on {}", local_addr);

            let server = arroyo_server_common::grpc_server_with_tls(tls_config)
                .await?
                .accept_http1(true)
                .add_service(controller_service)
                .add_service(job_controller_service);

            guard.into_spawn_task(wrap_start(
                "controller",
                local_addr,
                server.serve_with_incoming(TcpListenerStream::new(listener)),
            ));
        } else {
            info!("Starting arroyo-controller on {}", local_addr);

            guard.into_spawn_task(wrap_start(
                "controller",
                local_addr,
                arroyo_server_common::grpc_server()
                    .accept_http1(true)
                    .add_service(controller_service)
                    .add_service(job_controller_service)
                    .serve_with_incoming(TcpListenerStream::new(listener)),
            ));
        }

        Ok(local_addr.port())
    }
}

#[cfg(test)]
mod tests {
    use prometheus::core::Collector;
    use prost::Message;

    use super::*;
    use crate::queries::controller_queries::{Job, LastSuccessfulCheckpoint};
    use arroyo_rpc::LeaderContext;
    use arroyo_rpc::grpc::rpc::StartExecutionReq;
    use arroyo_rpc::state_backend::validate_restored_checkpoint;

    /// A `job_configs`/`job_statuses` row as the update thread polls it. `state_backend`
    /// is the only field these tests vary; everything else is the shape a freshly created
    /// job has.
    fn job_row(state_backend: &str) -> Job {
        Job {
            id: "job_abc".to_string(),
            org_id: "org".to_string(),
            pipeline_name: "pipeline".to_string(),
            pipeline_id: 1,
            checkpoint_interval_micros: 10_000_000,
            ttl_micros: None,
            parallelism_overrides: serde_json::json!({}),
            stop: StopMode::none,
            state: None,
            start_time: None,
            finish_time: None,
            tasks: None,
            failure_message: None,
            failure_domain: None,
            restarts: 0,
            run_id: None,
            pipeline_path: None,
            wasm_path: None,
            config_restart_nonce: 0,
            status_restart_nonce: 0,
            restart_mode: RestartMode::safe,
            ignore_state_before_epoch: None,
            // Exactly what the V29/V7 migration's `DEFAULT '{"version": 1}'` puts in the
            // column for a job whose status has never been written, which is the shape
            // `decode_state_context` has to accept.
            state_context: serde_json::json!({ "version": 1 }),
            env_vars: serde_json::json!({}),
            scheduler_config: serde_json::json!({}),
            state_backend: state_backend.to_string(),
        }
    }

    /// A `job_statuses` row as the update thread reads it. `state_context` is the
    /// controller's own record of the job's execution — what a restarted controller
    /// recovers the job's real state backend from.
    fn job_status(state: &str, recorded: Option<&str>, leader: bool) -> JobStatus {
        JobStatus {
            id: Arc::new("job_abc".to_string()),
            generation: 1,
            state: state.to_string(),
            start_time: None,
            finish_time: None,
            tasks: None,
            failure_message: None,
            failure_domain: None,
            restarts: 0,
            pipeline_path: None,
            wasm_path: None,
            restart_nonce: 3,
            state_context: StateContext {
                version: 1,
                leader: leader.then(|| LeaderContext {
                    worker_id: WorkerId(1),
                    rpc_address: "http://worker:1234".to_string(),
                    generation: 1,
                }),
                execution_selector: recorded.map(str::to_string),
            },
        }
    }

    /// A job the controller has never seen: `Created`, never run, nothing recorded. This
    /// is the one shape whose `job_configs.state_backend` is allowed to choose.
    fn never_started() -> JobStatus {
        job_status("Created", None, false)
    }

    /// Every state a job's status can be in that is *not* `Created`, and therefore proof
    /// that an execution exists — including the terminal ones, which still own the
    /// checkpoints that execution wrote.
    const STATES_WITH_AN_EXECUTION: [&str; 13] = [
        "Compiling",
        "Scheduling",
        "Running",
        "Recovering",
        "Rescaling",
        "Restarting",
        "Failing",
        "Failed",
        "Stopping",
        "CheckpointStopping",
        "Stopped",
        "Finishing",
        "Finished",
    ];

    /// The deployability guarantee: a row written before the V33 migration takes the
    /// column's `DEFAULT ''`, and the job it describes keeps running on parquet.
    #[test]
    fn job_row_written_before_the_migration_is_parquet() {
        let polled = classify_polled_row(job_row(""), &never_started())
            .expect("a job with an ordinary row must be startable");
        assert_eq!(polled.execution_selector, StateBackendSelector::Parquet);
        assert_eq!(polled.config.state_backend, StateBackendSelector::Parquet);
        assert!(polled.refusal.is_none());
        // and the rest of the row still converts as it always did
        assert_eq!(*polled.config.id, "job_abc");
        assert_eq!(polled.config.checkpoint_interval, Duration::from_secs(10));
    }

    /// A row written with an explicit selector round-trips through the conversion, and a
    /// job that has never started takes its backend from that row: this is the one moment
    /// a job chooses.
    #[test]
    fn job_row_round_trips_an_explicit_state_backend() {
        for selector in [
            StateBackendSelector::Parquet,
            StateBackendSelector::StateEngine,
        ] {
            let polled = classify_polled_row(job_row(selector.as_str()), &never_started())
                .expect("a job with an ordinary row must be startable");
            assert_eq!(polled.execution_selector, selector);
            assert_eq!(polled.config.state_backend, selector);
            assert!(polled.refusal.is_none());
        }
    }

    /// An unrecognized column value is never defaulted to parquet. With no execution on
    /// record there is nothing to administer and nothing to fail, so the job simply never
    /// starts — and the update thread moves on to the next row rather than erroring out.
    #[test]
    fn a_job_that_never_started_and_has_an_unusable_row_never_starts() {
        assert!(
            classify_polled_row(job_row("rocksdb"), &never_started()).is_none(),
            "an unknown selector must not be downgraded to a default"
        );
        assert!(classify_polled_row(job_row(""), &never_started()).is_some());
        assert!(classify_polled_row(job_row("stateengine"), &never_started()).is_some());
    }

    /// Finding 1. A controller that restarts rebuilds an empty job map and then reads
    /// every row afresh. If it took the backend from the row, a row edited while it was
    /// down would become the new execution authority — and the controller would go on to
    /// administer, and reconnect to, a job that is still running under the old one.
    ///
    /// The selector is recovered from the job's own execution record instead, and the
    /// row's value is refused exactly as it would have been by a controller that had never
    /// stopped.
    #[test]
    fn a_cold_controller_recovers_the_execution_selector_rather_than_the_edited_row() {
        // A worker-leader job that is still running, whose execution recorded parquet.
        let status = job_status("Running", Some("parquet"), true);

        let polled = classify_polled_row(job_row("stateengine"), &status)
            .expect("a live job must still be adopted");

        assert_eq!(
            polled.execution_selector,
            StateBackendSelector::Parquet,
            "the running job's own backend, not the one the edited row now names"
        );
        assert_eq!(
            polled.config.state_backend,
            StateBackendSelector::Parquet,
            "and every consumer of the configuration must see that value"
        );
        assert_eq!(
            polled.refusal,
            Some(StateBackendError::JobSelectorChanged {
                label: "job \"job_abc\"".to_string(),
                running: StateBackendSelector::Parquet,
                requested: StateBackendSelector::StateEngine,
            }),
            "and the row's value must be refused, as it would be by a controller that \
             had never restarted"
        );
    }

    /// The same finding for an unknown value. Before this, a row the controller could not
    /// interpret was refused only to a job that already had a state machine; after a cold
    /// restart there is none, so the still-running job was skipped entirely — neither
    /// adopted nor failed, on every poll, forever.
    #[test]
    fn a_cold_controller_adopts_a_live_job_whose_row_names_an_unknown_backend() {
        let status = job_status("Running", Some("stateengine"), true);

        let polled = classify_polled_row(job_row("rocksdb"), &status)
            .expect("a live job must be adopted even when its row is unusable");

        assert_eq!(polled.execution_selector, StateBackendSelector::StateEngine);
        assert_eq!(
            polled.config.state_backend,
            StateBackendSelector::StateEngine
        );
        assert_eq!(
            polled.refusal,
            Some(StateBackendError::UnknownValue {
                label: "job job_abc".to_string(),
                value: "rocksdb".to_string(),
            })
        );
    }

    /// The upgrade direction, in **both** controller modes and in every state that means
    /// the job has an execution.
    ///
    /// A job that was running before this build existed recorded no selector at all, and a
    /// build with no selector could only have been running parquet. Recovering that, rather
    /// than falling back to the row, is what keeps a job live across the upgrade from being
    /// re-baselined by an edit.
    ///
    /// Round 4 decided "does an execution exist?" from the presence of a leader context,
    /// which is only ever written in worker-leader mode. A *controller*-mode job that was
    /// running across the upgrade therefore had neither a selector nor a leader, read as a
    /// job that had never started, and took its backend from the editable row. This test
    /// covers `leader = false` for exactly that reason.
    #[test]
    fn an_execution_started_before_the_selector_existed_recovers_as_parquet() {
        for leader in [true, false] {
            for state in STATES_WITH_AN_EXECUTION {
                let status = job_status(state, None, leader);

                let polled =
                    classify_polled_row(job_row("stateengine"), &status).unwrap_or_else(|| {
                        panic!(
                            "a job with an execution must still be adopted \
                                               ({state}, leader={leader})"
                        )
                    });
                assert_eq!(
                    polled.execution_selector,
                    StateBackendSelector::Parquet,
                    "{state}, leader={leader}: a pre-upgrade execution is parquet, not what \
                     the row now says"
                );
                assert_eq!(
                    polled.config.state_backend,
                    StateBackendSelector::Parquet,
                    "{state}, leader={leader}"
                );
                assert_eq!(
                    polled.refusal,
                    Some(StateBackendError::JobSelectorChanged {
                        label: "job \"job_abc\"".to_string(),
                        running: StateBackendSelector::Parquet,
                        requested: StateBackendSelector::StateEngine,
                    }),
                    "{state}, leader={leader}: and the row's value must be refused"
                );

                // and the ordinary case for such a job — an untouched row — is not refused
                let polled = classify_polled_row(job_row(""), &status).unwrap();
                assert_eq!(polled.execution_selector, StateBackendSelector::Parquet);
                assert!(
                    polled.refusal.is_none(),
                    "{state}, leader={leader}: a legacy all-parquet job must go on being \
                     administered without complaint"
                );
            }
        }
    }

    /// The same upgrade case with a row the controller cannot interpret at all. The job
    /// must still be adopted under parquet and routed to its refusal path; skipping it
    /// would leave a still-running job unadministered on every poll, forever.
    #[test]
    fn a_pre_upgrade_execution_with_an_unusable_row_is_adopted_under_parquet() {
        for leader in [true, false] {
            for state in STATES_WITH_AN_EXECUTION {
                let status = job_status(state, None, leader);

                let polled =
                    classify_polled_row(job_row("rocksdb"), &status).unwrap_or_else(|| {
                        panic!(
                            "{state}, leader={leader}: a job with an execution must be \
                               adopted even when its row is unusable"
                        )
                    });
                assert_eq!(polled.execution_selector, StateBackendSelector::Parquet);
                assert_eq!(polled.config.state_backend, StateBackendSelector::Parquet);
                assert_eq!(
                    polled.refusal,
                    Some(StateBackendError::UnknownValue {
                        label: "job job_abc".to_string(),
                        value: "rocksdb".to_string(),
                    }),
                    "{state}, leader={leader}"
                );
            }
        }
    }

    /// `start_time` is the second, independent proof that an execution exists: a status row
    /// whose state name this build does not recognize, but which has run, is still not
    /// re-baselined from its row.
    #[test]
    fn a_job_that_has_run_has_an_execution_whatever_its_state_name_says() {
        let mut status = job_status("Created", None, false);
        status.start_time = Some(OffsetDateTime::now_utc());

        let polled = classify_polled_row(job_row("stateengine"), &status)
            .expect("a job that has run must still be adopted");
        assert_eq!(polled.execution_selector, StateBackendSelector::Parquet);
        assert!(matches!(
            polled.refusal,
            Some(StateBackendError::JobSelectorChanged { .. })
        ));
    }

    /// The execution record is persisted state and therefore untrusted. A recorded value
    /// nobody recognizes leaves the controller unable to say what the job is running with,
    /// and picking one would be picking it for a job that is still running.
    ///
    /// Renamed in round 5: this covers a `state_context` that *decoded* and named an
    /// unknown backend. A `state_context` that cannot be decoded at all is a different
    /// path, and is covered by `an_undecodable_execution_record_skips_only_that_job`.
    #[test]
    fn an_execution_recording_an_unknown_backend_is_never_guessed_at() {
        for leader in [true, false] {
            let status = job_status("Running", Some("rocksdb"), leader);
            assert!(classify_polled_row(job_row("parquet"), &status).is_none());
            assert!(classify_polled_row(job_row("rocksdb"), &status).is_none());
        }
    }

    /// Finding 4. A `state_context` blob that cannot be decoded is not turned into "this
    /// job has no execution": that erases the job's only selector authority, after which
    /// the editable row is adopted in its place. The job is skipped instead — and only
    /// that job, so the rest of the cluster goes on being polled.
    #[test]
    fn an_undecodable_execution_record_skips_only_that_job() {
        for broken in [
            serde_json::json!("not an object"),
            serde_json::json!({}),
            serde_json::json!({ "version": "one" }),
            serde_json::json!({ "version": 1, "leader": 7 }),
            serde_json::json!({ "version": 1, "execution_selector": 3 }),
            serde_json::Value::Null,
        ] {
            let mut row = job_row("stateengine");
            row.state = Some("Running".to_string());
            row.state_context = broken.clone();
            assert!(
                classify_polled_job(row).is_none(),
                "an execution record that cannot be decoded must not be replaced by one \
                 that says the job never started: {broken}"
            );
        }

        // The control, through the same entry point: an ordinary row is still classified,
        // so the skip above is about the broken record and not about the path.
        let mut row = job_row("");
        row.state = Some("Running".to_string());
        let (polled, status) = classify_polled_job(row).expect("an ordinary row must classify");
        assert_eq!(polled.execution_selector, StateBackendSelector::Parquet);
        assert_eq!(status.state, "Running");
        assert!(polled.refusal.is_none());

        // ...and one carrying a real recorded selector round-trips through the decode.
        let mut row = job_row("");
        row.state = Some("Running".to_string());
        row.state_context = serde_json::json!({
            "version": 1,
            "execution_selector": "stateengine",
        });
        let (polled, _) = classify_polled_job(row).expect("a recorded selector must decode");
        assert_eq!(polled.execution_selector, StateBackendSelector::StateEngine);
        assert!(matches!(
            polled.refusal,
            Some(StateBackendError::JobSelectorChanged { .. })
        ));
    }

    /// Round 3's durability property, now enforced where the row is read: a refused row
    /// does not advance the restart nonce, so `Failed` never sees a restart request and
    /// the job cannot be restarted under a value that was just refused.
    #[test]
    fn a_refused_row_does_not_advance_the_restart_nonce() {
        let status = job_status("Failed", Some("parquet"), false);

        let mut row = job_row("stateengine");
        row.config_restart_nonce = status.restart_nonce + 1;
        let polled = classify_polled_row(row, &status).unwrap();
        assert!(polled.refusal.is_some());
        assert_eq!(
            polled.config.restart_nonce, status.restart_nonce,
            "a refused row must not carry a restart request into the state machine"
        );
        assert!(
            job_in_final_state(&polled.config, &status),
            "so a failed job stays failed rather than being restarted by the refused row"
        );

        // the control: the same nonce bump on an acceptable row does restart the job
        let mut row = job_row("parquet");
        row.config_restart_nonce = status.restart_nonce + 1;
        let polled = classify_polled_row(row, &status).unwrap();
        assert!(polled.refusal.is_none());
        assert_eq!(polled.config.restart_nonce, status.restart_nonce + 1);
        assert!(!job_in_final_state(&polled.config, &status));
    }

    /// The update thread must move on: one unusable row cannot be allowed to stop every
    /// other job on the cluster from being polled.
    ///
    /// A structural pin, named for what it actually checks — that the loop is *written*
    /// this way. The loop itself lives inside the spawned polling closure and needs a live
    /// Postgres and a running updater to drive. What it skips on, and that a skip is
    /// scoped to one job, is covered behaviourally by `classify_polled_job`'s own tests.
    #[test]
    fn the_poll_loop_is_written_to_skip_a_row_it_cannot_use() {
        // Assembled rather than written out: this file is its own fixture, so a literal
        // would match the assertion itself and pass however the poll loop is written.
        let source = include_str!("lib.rs");
        assert!(
            source.contains(&format!(
                "let Some((polled, status)) = {}(p) else {{\n",
                "classify_polled_job"
            )),
            "the poll loop must resolve every row through the one classifier"
        );
        assert!(
            source.contains("                        continue;\n"),
            "and must still move on to the next job: fail-open for the cluster, hard \
             failure for the job"
        );
    }

    /// Round 4's finding 4, as a property of the signature rather than of a timing test:
    /// the refusal is offered to the job's own queue without awaiting it, so the update
    /// thread cannot end up waiting for one job's consumer while it holds the global job
    /// map. A non-`async` `refuse_config` is what makes that unwritable.
    ///
    /// The behaviour itself is covered by `refusing_a_row_never_waits_for_the_jobs_own_queue`
    /// in `states::tests`; this pins the signature that guarantees it, which is what the
    /// name now says.
    #[test]
    fn refuse_config_is_not_async_so_it_cannot_await_the_jobs_queue() {
        let source = include_str!("states/mod.rs");
        assert!(
            source.contains(&format!(
                "pub(crate) fn {}(&mut self, error: StateBackendError)",
                "refuse_config"
            )),
            "refuse_config must not be async: the update thread calls it under the global \
             job map lock"
        );
        assert!(
            source.contains("    fn offer(&self, msg: JobMessage) -> Delivery {"),
            "and must deliver through the non-blocking offer path"
        );
    }

    /// What the controller reads from the row is exactly what every worker of that job
    /// receives, across a real prost encode/decode of the start request.
    #[test]
    fn the_job_selector_reaches_workers_unchanged() {
        for (raw, expected) in [
            ("", StateBackendSelector::Parquet),
            ("parquet", StateBackendSelector::Parquet),
            ("stateengine", StateBackendSelector::StateEngine),
        ] {
            let polled = classify_polled_row(job_row(raw), &never_started()).unwrap();

            // exactly how states::scheduling builds the request for each worker
            let req = StartExecutionReq {
                state_backend: polled.execution_selector.as_str().to_string(),
                ..Default::default()
            };
            let decoded = StartExecutionReq::decode(&req.encode_to_vec()[..]).unwrap();

            assert_eq!(
                StateBackendSelector::normalize(&decoded.state_backend, "job job_abc").unwrap(),
                expected
            );
        }
    }

    /// The `checkpoints` row the scheduler restores from, as `last_successful_checkpoint`
    /// returns it. Naming the field here is itself part of the contract: the restore
    /// check is only possible because that query reads the column.
    fn checkpoint_row(state_backend: &str) -> LastSuccessfulCheckpoint {
        LastSuccessfulCheckpoint {
            pub_id: "chk_abc".to_string(),
            epoch: 12,
            min_epoch: 3,
            state_backend: state_backend.to_string(),
            needs_commits: false,
        }
    }

    /// Runs the pairing `states::scheduling` performs when it resolves a checkpoint to
    /// restore: the job row says which backend the job selects, the checkpoint row records
    /// which backend wrote the checkpoint.
    fn restore(job: &str, checkpoint: &str) -> Result<(), StateBackendError> {
        let polled = classify_polled_row(job_row(job), &never_started()).unwrap();
        let row = checkpoint_row(checkpoint);
        validate_restored_checkpoint(
            polled.execution_selector,
            row.epoch as u64,
            &row.state_backend,
        )
    }

    /// The deployability guarantee across both rows: a cluster upgraded in place has jobs
    /// and checkpoints alike carrying the columns' `DEFAULT ''`, and every one of those
    /// jobs restores its own checkpoints exactly as before.
    #[test]
    fn a_checkpoint_written_before_the_migration_restores_into_its_job() {
        restore("", "").unwrap();
        restore("parquet", "").unwrap();
        restore("", "parquet").unwrap();
        restore("stateengine", "stateengine").unwrap();
    }

    /// Changing a running job's backend does not silently reinterpret its state: the
    /// checkpoint it would restore was written by the other backend, and it is refused
    /// with the checkpoint named.
    #[test]
    fn a_checkpoint_written_by_another_backend_is_refused_for_the_job() {
        let err = restore("stateengine", "parquet").unwrap_err();
        assert_eq!(
            err,
            StateBackendError::CheckpointMismatch {
                label: "restored checkpoint \"epoch 12\"".to_string(),
                found: StateBackendSelector::Parquet,
                job: StateBackendSelector::StateEngine,
            }
        );

        let err = restore("parquet", "stateengine").unwrap_err();
        assert_eq!(
            err,
            StateBackendError::CheckpointMismatch {
                label: "restored checkpoint \"epoch 12\"".to_string(),
                found: StateBackendSelector::StateEngine,
                job: StateBackendSelector::Parquet,
            }
        );
    }

    /// An unrecognized checkpoint row is a hard failure, not a fallback to the job's own
    /// backend — which would read another backend's files under this one's layout.
    #[test]
    fn a_checkpoint_row_with_an_unknown_state_backend_is_refused() {
        let err = restore("parquet", "rocksdb").unwrap_err();
        assert_eq!(
            err,
            StateBackendError::UnknownValue {
                label: "restored checkpoint \"epoch 12\"".to_string(),
                value: "rocksdb".to_string(),
            }
        );
    }

    #[test]
    fn metric_job_states_preserve_raw_states() {
        for state in ["Created", "Running", "Finished", "Unexpected"] {
            assert_eq!(metric_job_state(Some(state), None), state);
        }
        assert_eq!(metric_job_state(None, None), "Created");

        assert_eq!(metric_job_state(Some("Failed"), Some("user")), "UserFailed");
        assert_eq!(metric_job_state(Some("Failed"), Some("internal")), "Failed");
    }

    #[test]
    fn job_state_metrics_clear_absent_states() {
        let counts = job_state_counts(
            [
                (Some("Running"), None),
                (Some("Running"), None),
                (Some("Failed"), Some("user")),
            ]
            .into_iter(),
        );
        update_job_state_metrics(&counts);
        assert_eq!(JOBS_BY_STATE.with_label_values(&["Running"]).get(), 2);
        assert_eq!(JOBS_BY_STATE.with_label_values(&["UserFailed"]).get(), 1);

        update_job_state_metrics(&HashMap::new());
        assert!(JOBS_BY_STATE.collect()[0].get_metric().is_empty());
    }
}
