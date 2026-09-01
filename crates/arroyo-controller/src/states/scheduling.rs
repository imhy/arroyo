use arroyo_rpc::fence_wire::observed_settlement;
use arroyo_rpc::grpc::rpc::{
    JobState, StartExecutionOutcome, StartExecutionReq, StartExecutionResp, TaskAssignment,
};
use arroyo_rpc::identity::{WorkerClient, worker_client};
use arroyo_types::{CLUSTER_ID_ENV, JobId, MachineId, PipelineId, WorkerId};
use futures::stream::{FuturesUnordered, StreamExt};
use std::time::SystemTime;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{select, sync::Mutex, task::JoinHandle};
use tonic::Request;
use tracing::{debug, error, info, warn};

use super::{
    Admission, JobContext, State, Transition, check_config_update,
    leader_running::LeaderRunning,
    lifecycle::{
        ConsumptionPoint, ObservedIntent, StartTargets, StatusPublication, TransportSettlement,
        leaving, stand_down,
    },
    running::Running,
};
use crate::JobConfig;
use crate::job_controller::checkpoint_store::DbCheckpointMetadataStore;
use crate::job_controller::leader_manager::LeaderManager;
use crate::states::LeavingForStop;
use crate::{JobMessage, schedulers::SchedulerError};
use crate::{
    job_controller::JobController, queries::controller_queries, states::stop_if_desired_non_running,
};
use crate::{
    schedulers::StartPipelineReq,
    states::{StateError, fatal},
};
use anyhow::{anyhow, bail};
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::config::{JobControllerMode, config};
use arroyo_rpc::errors::StateError as StateStoreError;
use arroyo_rpc::grpc::api;
use arroyo_rpc::state_backend::validated::Validated;
use arroyo_rpc::state_backend::{
    StateBackendError, StateBackendSelector, validate_restored_checkpoint,
};
use arroyo_rpc::worker_types::RunningMessage;
use arroyo_rpc::{LeaderContext, grpc_channel_builder};
use arroyo_state::{
    BackingStore, StateBackend, StorageProviderFor, get_storage_provider,
    tables::{ErasedTable, global_keyed_map::GlobalKeyedTable},
    validated::{CheckpointIdentity, CheckpointMetadataWrite},
};
use arroyo_state_protocol::types::Generation;
use arroyo_state_protocol::workflow::{
    GenerationInitialization, GenerationRecovery, InitializeGenerationRequest,
    initialize_generation,
};
use arroyo_worker::job_controller::committing_state::{CheckpointIdOrRef, CommittingState};
use arroyo_worker::job_controller::job_metrics::JobMetrics;

pub(crate) mod admission;
pub(crate) mod fanout;
pub(crate) mod fencing;
pub(crate) mod phases;

use self::fanout::AttemptLedger;

/// How many times the fan-out re-offers an unsettled `start_execution_id` to a worker before
/// it gives the attempt up, on top of the first request.
///
/// This bounds how long the controller tries to *learn* an outcome. It is deliberately not a
/// deadline on the admission, and the distinction is the whole of why ending the loop is safe:
/// see the terminal-path section of [`start_execution_on_workers`]. Every peer the fan-out can
/// reach has advertised the reconciliation contract — `Scheduling::next` refuses to schedule
/// otherwise — so its handler decides within one synchronous poll of receiving a request and
/// can never be parked. What the controller loses by stopping is knowledge of whether the
/// worker started, and that is recovered by failing the attempt: the retry's preamble raises
/// the generation and tears the old one down.
pub(crate) const START_EXECUTION_RECONCILE_ATTEMPTS: usize = 8;

/// The pause between those attempts. Short, because the common case is a worker that is
/// momentarily busy rather than one that is gone.
pub(crate) const START_EXECUTION_RECONCILE_DELAY: Duration = Duration::from_millis(250);

#[derive(Debug, Clone)]
struct WorkerStatus {
    id: WorkerId,
    machine_id: MachineId,
    rpc_address: String,
    data_address: String,
    slots: usize,
    state: WorkerState,
    /// Whether this worker advertised the `StartExecution` reconciliation contract at
    /// registration. See [`JobMessage::WorkerConnect`] and [`START_EXECUTION_RECONCILE_ATTEMPTS`].
    reconciles_start_execution: bool,
}

#[derive(Debug, Clone)]
enum WorkerState {
    Connected,
    Initializing,
    Ready,
    Failed,
}

#[derive(Debug)]
pub struct Scheduling {}

impl Scheduling {
    /// The name this state is known by, in logs, in the job's status row, and — because
    /// [`State`] is object-safe and a `Box<dyn State>` cannot be downcast — in the one place
    /// that has to recognise it: [`run_state_body`].
    pub(crate) const NAME: &'static str = "Scheduling";
}

/// Runs one state's body, through the M11.D39b phase graph for a `Scheduling` whose job runs
/// the M11.D39a single-writer lifecycle.
///
/// This is the whole of the seam. It exists here, called once from
/// [`execute_state`](crate::states::execute_state), rather than inside
/// [`Scheduling::next`], because `next` is pinned by
/// `the_source_of_scheduling_next_keeps_every_irreversible_effect_inside_an_admitted_region`
/// down to the set of things its interruptible stretches may await — and the landed body must
/// keep passing that pin unchanged, which is exactly what M11.T25 promises about it.
///
/// **Every production job takes the first branch, since M11.T26h's activation change.** A
/// job's lifecycle mechanism is fixed when its state machine is created, from
/// [`LifecycleMode::SELECTED`](crate::states::lifecycle::LifecycleMode::SELECTED), and that is
/// `FencedV2`; such a job has a lifecycle actor, so
/// [`JobContext::runs_fenced_lifecycle`](crate::states::JobContext::runs_fenced_lifecycle) is
/// `true` and `Scheduling` runs the M11.D39b phase graph. The landed body below is what a job
/// built in the pre-flag-day peer mode runs — nothing outside a test module builds one — and it
/// still runs exactly as M11.T08 landed it, which is what its pins are for.
pub(crate) async fn run_state_body(
    state: Box<dyn State>,
    ctx: &mut JobContext<'_>,
) -> Result<Transition, StateError> {
    if state.name() == Scheduling::NAME && ctx.runs_fenced_lifecycle() {
        return phases::schedule(ctx).await;
    }
    state.next(ctx).await
}

fn slots_for_job(job: &LogicalProgram) -> usize {
    job.graph
        .node_weights()
        .map(|n| n.parallelism)
        .max()
        .unwrap_or(0)
}

fn compute_assignments(
    workers: Vec<&WorkerStatus>,
    program: &LogicalProgram,
) -> Vec<TaskAssignment> {
    let mut assignments = vec![];
    for node in program.graph.node_weights() {
        let mut worker_idx = 0;
        let mut current_count = 0;

        for i in 0..node.parallelism {
            assignments.push(TaskAssignment {
                task_id: node.node_id,
                subtask_idx: i as u32,
                worker_id: workers[worker_idx].id.0,
                worker_addr: workers[worker_idx].data_address.clone(),
                worker_rpc: workers[worker_idx].rpc_address.clone(),
            });
            current_count += 1;

            if current_count == workers[worker_idx].slots {
                worker_idx += 1;
                current_count = 0;
            }
        }
    }

    assignments
}

async fn handle_worker_connect<'a>(
    msg: JobMessage,
    workers: &mut HashMap<WorkerId, WorkerStatus>,
    worker_connects: Arc<Mutex<HashMap<WorkerId, WorkerClient>>>,
    handles: &mut Vec<JoinHandle<()>>,
    ctx: &mut JobContext<'a>,
) -> Result<(), StateError> {
    match msg {
        JobMessage::WorkerConnect {
            worker_id,
            machine_id,
            generation: run_id,
            rpc_address,
            data_address,
            slots,
            reconciles_start_execution,
            ..
        } => {
            let job_id = ctx.config.id.clone();
            let pipeline_id = ctx.pipeline_info.pipeline_id.clone();

            if ctx.status.generation != run_id {
                info!(
                    message = "worker connect from wrong run; ignoring",
                    job_id = %job_id,
                    pipeline_id = *pipeline_id,
                    worker_id = worker_id.0,
                    machine_id = *machine_id.0,
                );
                return Ok(());
            }

            workers.insert(
                worker_id,
                WorkerStatus {
                    id: worker_id,
                    machine_id: machine_id.clone(),
                    rpc_address: rpc_address.clone(),
                    data_address,
                    slots,
                    state: WorkerState::Connected,
                    reconciles_start_execution,
                },
            );

            let connects = worker_connects;

            handles.push(tokio::spawn(async move {
                info!(
                    message = "connecting to worker",
                    job_id = %job_id,
                    pipeline_id = *pipeline_id,
                    worker_id = worker_id.0,
                    machine_id = *machine_id.0,
                    rpc_address
                );

                for i in 0..3 {
                    match grpc_channel_builder(
                        "controller",
                        rpc_address.clone(),
                        &config().controller.tls,
                        &config().worker.tls,
                        None,
                    )
                    .await
                    .unwrap()
                    .timeout(Duration::from_secs(90))
                    .connect()
                    .await
                    {
                        Ok(channel) => {
                            {
                                let mut connects = connects.lock().await;
                                connects.insert(worker_id, worker_client(channel, worker_id));
                            }
                            return;
                        }
                        Err(e) => {
                            error!(
                                message = "Failed to connect to worker",
                                job_id = %job_id,
                                pipeline_id = *pipeline_id,
                                worker_id = worker_id.0,
                                machine_id = *machine_id.0,
                                error = format!("{:?}", e),
                                rpc_address,
                                retry = i
                            );
                            tokio::time::sleep(Duration::from_millis((i + 1) * 100)).await;
                        }
                    }
                }
                panic!("Failed to connect to worker {rpc_address}");
            }));
        }
        other => {
            ctx.handle(other)?;
        }
    }

    Ok(())
}

/// Everything every worker's `StartExecutionReq` shares.
///
/// Grouped so the fan-out below can be a function that takes an [`Admission`], rather than a
/// block of statements that happens to sit inside a region: a function cannot be called
/// without a guard in scope, and the guard is only in scope where a refusal cannot be
/// published.
struct ExecutionPlan {
    assignments: Vec<TaskAssignment>,
    program: api::ArrowProgram,
    restore_epoch: Option<u64>,
    start_epoch: u64,
    min_epoch: u64,
    /// The worker that runs the job controller, and its address — leader mode only.
    leader: Option<(WorkerId, String)>,
    checkpoint_manifest_ref: Option<String>,
    checkpoint_interval_micros: u64,
    /// The job's own selector, stamped into every worker of this execution.
    state_backend: StateBackendSelector,
}

/// Sends every connected worker its `StartExecutionReq`, and waits for all of them to accept.
///
/// This is the point at which the job stops being a cluster of idle workers and becomes a
/// running execution: each worker opens the state backend under
/// [`ExecutionPlan::state_backend`], restores from the checkpoint the preamble prepared, and
/// starts producing. None of that can be taken back, and none of it may happen for a
/// configuration the controller has refused — which is why the caller must hand over an
/// [`Admission`] to call this at all, and why the caller takes that admission by re-reading
/// the gate rather than by trusting the message loop that just ended.
///
/// # Why the requests are not spawned, and why the admission travels with them
///
/// The RPCs are concurrent but they are *not* separate tasks. `tokio::spawn` hands the child an
/// independent lifetime: dropping a [`JoinHandle`] detaches the task rather than cancelling it,
/// so the first worker to fail used to end this function — through `?` on a `JoinError` — while
/// every sibling request carried on, past the point where a refusal could be published.
///
/// Making the requests children of this future was necessary but not sufficient, because
/// dropping a `tonic` client future is not revocation. It resets the stream; it does not prove
/// what a server handler already did. The worker therefore never blocks inside the handler: a
/// contended phase lock returns `Aborted` without applying anything, and the controller retries
/// that same ID under the admission. Once the worker acquires the lock, recording the ID, setting
/// `Initializing`, and spawning initialization are one synchronous poll.
///
/// So this function does not cancel anything. It **settles** everything:
///
/// * every request is awaited to an outcome, including the siblings of one that has already
///   failed; the first error is remembered and reported once the last request is in;
/// * the region owns its [`Admission`] and hands it back only when that is done. A state task
///   dropped underneath it takes the requests and the admission with it, which is safe under
///   M11.D39a because the dropped task is the job's *only* decider: there is nobody left to
///   publish anything behind the requests, and what a replacement controller owes is in the
///   job's row — it re-adopts, which raises the fence above every identifier this attempt
///   could have issued, before it causes any effect (M11.D39d).
///
/// A transport error is not an answer from the worker: `Endpoint::timeout` is client-side and a
/// reset stream does not prove whether the synchronous application completed. Every request
/// therefore carries a stable non-empty `start_execution_id`. On an ambiguous timeout/reset this
/// function retries the same ID while retaining the admission; the worker acknowledges a
/// duplicate ID without applying it twice. `Aborted` is also retried because it is the worker's
/// definitive "phase lock busy, nothing applied" response under the pre-flag-day protocol; since
/// M11.T26h `Aborted` is settlement and only a later scheduling attempt may try again. Other
/// explicit statuses settle the attempt.
///
/// # Why every peer here is one that can be reconciled
///
/// All three of those readings are claims about the *peer*, not about the protocol: that a
/// repeated ID is idempotent, that `Aborted` means nothing was applied, and that `Unavailable`
/// is never an answer. A worker predating the contract satisfies none of them, so
/// `Scheduling::next` fails the attempt rather than fan out to one — see the check above the
/// call site. That is a precondition of this function, and the terminal path below depends on
/// it as much as the retry does.
///
/// # The terminal path, and why it is not a deadline
///
/// Retrying an ID no one ever answers cannot go on forever: the loop owns the admission, so a
/// worker that dies or stays partitioned during `StartExecution` would wedge the job — unable
/// to reschedule and unable to accept a refusal — for the life of the controller process.
///
/// After [`START_EXECUTION_RECONCILE_ATTEMPTS`] unsettled attempts the request is given up and
/// reported as an error, which the caller turns into the same retryable [`StateError`] as any
/// other failure. What makes that safe is not that enough time has passed — a deadline would
/// release the admission while a request could still be applied, which is exactly the hazard
/// this region exists for. It is that **for a peer that advertised the contract there is no
/// state in which a received request is pending application**: the handler takes `try_lock`
/// and, on the `Idle` branch, records the ID, sets the phase and spawns initialization within
/// one synchronous poll, without an `.await` anywhere between. So a request that reached the
/// worker was decided at the instant it arrived, and the only thing that can cause a *new*
/// application after this function stops is this function issuing another attempt. Ceasing to
/// offer it is therefore the terminal event, and the loop bound decides how long the controller
/// tries to *learn* the outcome, not whether it is still exposed to one.
///
/// What is lost by stopping is that knowledge — whether the worker started. It is recovered by
/// failing the attempt rather than by holding the admission: the next `Scheduling` pass raises
/// the job's generation, persists it, and tears the previous generation down before it starts
/// replacements, so a worker that did start is stopped by the ordinary path.
///
/// The residual is the one already disclosed for this region: a request still in transit when
/// the last attempt's stream is reset can arrive afterwards, and that is bounded by the
/// transport rather than by anything the controller holds.
///
/// # Latency
///
/// Mints the stable `start_execution_id` one worker's `StartExecution` attempt carries.
///
/// Two `u64`s in zero-padded lowercase hexadecimal. `{:016x}` is a minimum width and 16 is also
/// the maximum number of hexadecimal digits a `u64` has, so every identifier this produces is
/// exactly 32 ASCII characters — never fewer, never more.
///
/// That width is not incidental. `arroyo_rpc::fencing::MAX_ATTEMPT_ID_CHARS` is
/// `2 × (u64::BITS / 4)` *because* of this format, and both ends of the protocol bound
/// identifiers by it: the durable fencing record, the revocation list on a decoded
/// `StartExecutionReq`, and the worker's bounded applied/revoked record all refuse an identifier
/// wider than that. Changing this format therefore has to change that constant with it, which is
/// what `the_minted_start_execution_id_is_exactly_the_bounded_width` exists to enforce.
fn mint_start_execution_id() -> String {
    format!(
        "{:016x}{:016x}",
        rand::random::<u64>(),
        rand::random::<u64>()
    )
}

/// Reads a `StartExecution` response as "this generation applied the request".
///
/// Through [`observed_settlement`] rather than off the response's fields, for the reason that
/// seam exists: `outcome` and `observed_lifecycle_fence` are one statement, and a response that
/// claims to have acknowledged a fence while reporting none is a shape this build refuses rather
/// than reads half of.
///
/// Only `APPLIED` starts a worker. `FENCE_ACKNOWLEDGED` and `REVOKED` are what a generation
/// answers a fence-only or revoke directive with; a `START` answered with either is a worker that
/// advanced its fence and ran nothing, and reporting it as started would leave the controller
/// waiting for tasks that were never launched. Each is definitive — the generation's own answer
/// about this identifier — so none of them is retried.
///
/// # Errors
///
/// The report to put in the fan-out's error, for a response that does not say the request was
/// applied.
fn applied_start(response: &StartExecutionResp) -> Result<Option<u64>, String> {
    let settlement = observed_settlement(response).map_err(|e| e.to_string())?;
    match settlement.outcome() {
        StartExecutionOutcome::Applied => Ok(settlement.observed_fence()),
        outcome => Err(format!(
            "it answered {outcome:?}, which acknowledges a fence rather than starting the execution"
        )),
    }
}

/// An explicit server error costs the slowest sibling instead of returning at once. An ambiguous
/// transport failure — which is what
/// [`FenceProtocol::transport_settlement`](crate::states::lifecycle::FenceProtocol::transport_settlement)
/// says, exhaustively over `tonic::Code` and for the protocol these targets were addressed
/// under — costs up to [`START_EXECUTION_RECONCILE_ATTEMPTS`] further attempts, each
/// separated by [`START_EXECUTION_RECONCILE_DELAY`] and each bounded by the channel's own 90s
/// `Endpoint::timeout`. An interrupted phase hands the whole obligation — the inventory and
/// the admission together — to the job's `JobSettlementOwner`; see
/// [`SettlementBundle::hand_over`](fanout::settlement::hand_over).
///
/// # What it records while it runs
///
/// `attempts` is the fan-out's own inventory (M11.D39b). Every request records the stable
/// `start_execution_id` it was minted with before it is sent, and records its own outcome as
/// that outcome arrives, so a holder of the ledger can ask what is outstanding at any moment
/// rather than only after the last request is in. Both halves of that are per attempt: an
/// answered request is accounted for while its siblings are still in flight, and an attempt
/// the terminal path above gives up is not accounted for at all, because giving up is not an
/// answer. The ledger is one record per target worker — a replayed identifier is the same
/// attempt — so it costs nothing that grows with the reconcile budget.
///
/// It is a record, not a control: nothing in this function consults it, and marking an attempt
/// settled neither ends a request nor releases the admission.
///
/// # Errors
///
/// Returns the first failing worker's error, after every request has settled. The caller turns
/// it into the same retryable [`StateError`] as before.
async fn start_execution_on_workers(
    admission: Admission,
    job_id: Arc<String>,
    pipeline_id: PipelineId,
    plan: ExecutionPlan,
    machine_ids: HashMap<WorkerId, MachineId>,
    targets: StartTargets,
    attempts: Arc<AttemptLedger>,
) -> (Admission, anyhow::Result<HashMap<WorkerId, WorkerClient>>) {
    // Read once, from the targets themselves. A worker's channel and the directive its request
    // carries arrive here as one value (M11.D39e), so the retry table below and the fence a
    // request was issued under cannot be read from different modes.
    let protocol = targets.protocol();
    {
        // The whole of this — issuing the requests as much as waiting for them — is one effect
        // inside one region, and none of it may outlive the guard.
        let started = admission
                .effect("send every worker its StartExecution", async move {
                    let (leader_id, leader_addr) = plan.leader.unzip();

                    let mut requests: FuturesUnordered<_> = targets
                        .into_starts()
                        .into_iter()
                        .map(|(id, mut c, directive)| {
                            let assignments = plan.assignments.clone();
                            let job_id = job_id.clone();
                            let pipeline_id = pipeline_id.clone();
                            let program = plan.program.clone();
                            // For the log lines below and nothing else, so a worker that somehow has
                            // no status entry is still asked to start rather than panicking the
                            // region — the one panic site the fan-out's path used to contain.
                            let machine_id = machine_ids
                                .get(&id)
                                .cloned()
                                .unwrap_or_else(|| MachineId(Arc::new(format!("unknown-{}", id.0))));
                            let leader_addr = leader_addr.clone();
                            let checkpoint_manifest_ref = plan.checkpoint_manifest_ref.clone();
                            let state_backend = plan.state_backend;
                            let start_execution_id = mint_start_execution_id();
                            // Recorded here, where the identifier is minted and before the first
                            // request carrying it is sent, rather than when a request returns. An
                            // inventory that under-reports could let a phase release its authority
                            // while a request it had forgotten was still live; one that
                            // over-reports only holds the obligation longer than it had to.
                            attempts.issued(id, &start_execution_id);
                            let attempts = Arc::clone(&attempts);
                            let (restore_epoch, start_epoch, min_epoch, checkpoint_interval_micros) = (
                                plan.restore_epoch,
                                plan.start_epoch,
                                plan.min_epoch,
                                plan.checkpoint_interval_micros,
                            );

                            async move {
                                info!(
                                    message = "starting execution on worker",
                                    job_id = %job_id,
                                    pipeline_id = *pipeline_id,
                                    worker_id = id.0,
                                    machine_id = *machine_id.0,
                                );

                                let mut request = StartExecutionReq {
                                        restore_epoch,
                                        start_epoch,
                                        min_epoch,
                                        program: Some(program),
                                        tasks: assignments,
                                        job_controller_addr: leader_addr,
                                        is_leader: leader_id.is_some_and(|l| l == id),
                                        wait_for_leader: leader_id.is_some(),
                                        checkpoint_interval_micros,
                                        checkpoint_manifest_ref,
                                        state_backend: state_backend.as_str().to_string(),
                                        start_execution_id,
                                        // Every lifecycle field is written by the directive
                                        // below, on every arm, so none is named here: this
                                        // literal cannot leave a fence or a target behind that
                                        // the directive did not ask for.
                                        ..Default::default()
                                    };
                                // The five lifecycle fields, from one value. Under
                                // `LifecycleMode::LegacyT08` the directive is `Unfenced` and
                                // stamps the zeros a controller predating the fields sends, so
                                // what leaves this process is byte-identical to what left it
                                // before they existed.
                                directive.stamp(&mut request);
                                let mut unsettled = 0usize;
                                loop {
                                    match c.start_execution(Request::new(request.clone())).await {
                                        Ok(response) => {
                                            // A response is an answer, so the attempt is
                                            // accounted for however it reads.
                                            attempts.answered(id, &request.start_execution_id);
                                            // Read whole, through the seam, rather than as two
                                            // fields: a worker that acknowledged a fence rather
                                            // than applying the request has *not* started, and a
                                            // response this build cannot name is not evidence
                                            // that it has. Both are definitive, so neither is
                                            // retried. Before the flag day this is the same
                                            // answer the landed path gave: the worker reports
                                            // `APPLIED` and no observed fence, which is also what
                                            // a worker predating the fields reports.
                                            match applied_start(&response.into_inner()) {
                                                Ok(observed) => {
                                                    debug!(
                                                        message = "worker entered initialization phase",
                                                        job_id = %job_id,
                                                        pipeline_id = *pipeline_id,
                                                        worker_id = id.0,
                                                        machine_id = *machine_id.0,
                                                        observed_lifecycle_fence = observed,
                                                    );
                                                    break Ok((id, c));
                                                }
                                                Err(report) => {
                                                    error!(
                                                        message = "worker answered StartExecution without applying it",
                                                        job_id = %job_id,
                                                        pipeline_id = *pipeline_id,
                                                        worker_id = id.0,
                                                        machine_id = *machine_id.0,
                                                        report = %report,
                                                    );
                                                    break Err(anyhow!(
                                                        "worker {id:?} did not apply StartExecution \
                                                         {}: {report}",
                                                        request.start_execution_id
                                                    ));
                                                }
                                            }
                                        }
                                        Err(e)
                                            if protocol.transport_settlement(e.code())
                                                == TransportSettlement::Ambiguous =>
                                        {
                                            unsettled += 1;
                                            if unsettled > START_EXECUTION_RECONCILE_ATTEMPTS {
                                                // The terminal path. Not a deadline on the
                                                // admission: what ends the attempt is the
                                                // controller ceasing to offer it, and a peer that
                                                // advertised the contract cannot be holding a
                                                // parked handler for it.
                                                //
                                                // Deliberately *not* settled in the ledger. No
                                                // worker answered this, so the honest record is
                                                // that it is still unaccounted for; what the
                                                // controller gave up is knowing the outcome, and
                                                // an inventory that marked it settled would be
                                                // reporting an answer nobody gave.
                                                error!(
                                                    message = "StartExecution never settled; giving the attempt up so the job can be rescheduled",
                                                    job_id = %job_id,
                                                    pipeline_id = *pipeline_id,
                                                    worker_id = id.0,
                                                    machine_id = *machine_id.0,
                                                    start_execution_id = %request.start_execution_id,
                                                    attempts = unsettled,
                                                    error = format!("{:?}", e),
                                                );
                                                break Err(anyhow!(
                                                    "worker {id:?} never settled StartExecution \
                                                     {} after {unsettled} attempts: {e}",
                                                    request.start_execution_id
                                                ));
                                            }
                                            warn!(
                                                message = "StartExecution is not settled; retrying the same id under admission",
                                                job_id = %job_id,
                                                pipeline_id = *pipeline_id,
                                                worker_id = id.0,
                                                machine_id = *machine_id.0,
                                                start_execution_id = %request.start_execution_id,
                                                attempt = unsettled,
                                                error = format!("{:?}", e),
                                            );
                                            tokio::time::sleep(START_EXECUTION_RECONCILE_DELAY).await;
                                        }
                                        Err(e) => {
                                            // An explicit status other than the ambiguous set is
                                            // the worker's own decision about this request, so the
                                            // attempt is accounted for even though it failed.
                                            attempts.answered(id, &request.start_execution_id);
                                            error!(
                                            message = "failed to start execution on worker",
                                            job_id = %job_id,
                                            pipeline_id = *pipeline_id,
                                            worker_id = id.0,
                                            machine_id = *machine_id.0,
                                            error = format!("{:?}", e),
                                        );
                                            break Err(anyhow!(
                                                "failed to start execution on worker {id:?}: {e}"
                                            ));
                                        }
                                    }
                                }
                            }
                        })
                        .collect();

                    let mut started = HashMap::new();
                    let mut first_error: Option<anyhow::Error> = None;
                    // Every request, to an outcome. Leaving on the first error would drop the
                    // siblings' client futures, and a client future dropped is not a worker stopped:
                    // see this function's documentation. The loop is what makes "settled" a fact
                    // rather than a hope, and the admission is not handed back until it ends.
                    while let Some(outcome) = requests.next().await {
                        match outcome {
                            Ok((id, c)) => {
                                started.insert(id, c);
                            }
                            Err(e) => {
                                if first_error.is_none() {
                                    first_error = Some(e);
                                }
                            }
                        }
                    }
                    match first_error {
                        Some(e) => Err(e),
                        None => Ok(started),
                    }
                })
                .await;

        (admission, started)
    }
}

#[derive(Clone, Debug)]
struct CheckpointInfo {
    epoch: u64,
    min_epoch: u64,
    id: String,
    needs_commits: bool,
}

async fn get_checkpoint_info_legacy<'a>(
    state: Box<Scheduling>,
    ctx: &'a JobContext<'a>,
) -> Result<
    (
        Box<Scheduling>,
        Option<CheckpointInfo>,
        Option<CommittingState>,
    ),
    StateError,
> {
    let checkpoint_info = {
        let client = match ctx.db.client().await {
            Ok(c) => c,
            Err(e) => {
                return Err(ctx.retryable(state, "failed to get db client", e.into(), 10));
            }
        };

        let checkpoint_result =
            match controller_queries::fetch_last_successful_checkpoint(&client, &*ctx.config.id)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    return Err(ctx.retryable(
                        state,
                        "failed to fetch last checkpoint",
                        e.into(),
                        10,
                    ));
                }
            };

        let row = checkpoint_result.into_iter().next().filter(|r| {
            // Filter checkpoint based on ignore_state_before_epoch threshold
            let should_restore = ctx
                .config
                .ignore_state_before_epoch
                .map(|threshold| r.epoch >= threshold)
                .unwrap_or(true); // If no threshold, restore any checkpoint

            if should_restore {
                info!(
                    message = "restoring checkpoint",
                    job_id = %ctx.config.id,
                    pipeline_id = *ctx.pipeline_info.pipeline_id,
                    epoch = r.epoch,
                    min_epoch = r.min_epoch
                );
            } else {
                info!(
                    message = "skipping checkpoint due to ignore_state_before_epoch threshold",
                    job_id = %ctx.config.id,
                    pipeline_id = *ctx.pipeline_info.pipeline_id,
                    checkpoint_epoch = r.epoch,
                    threshold = ctx.config.ignore_state_before_epoch.unwrap()
                );
            }

            should_restore
        });

        // The checkpoint row records the backend that wrote it. Checking it here — before
        // in-progress checkpoints are marked failed, before any metadata is loaded,
        // before commit data is rebuilt, and before the rewritten metadata is put back —
        // means a job never touches state written by a backend it did not select. A
        // disagreement is fatal rather than retryable: the same row is read again on
        // every attempt.
        if let Some(r) = &row
            && let Err(e) = validate_restored_checkpoint(
                ctx.execution_selector,
                r.epoch as u64,
                &r.state_backend,
            )
        {
            return Err(fatal(
                "cannot restore a checkpoint written with a different state backend",
                e.into(),
            ));
        }

        row.map(|r| CheckpointInfo {
            id: r.pub_id,
            epoch: r.epoch as u64,
            min_epoch: r.min_epoch as u64,
            needs_commits: r.needs_commits,
        })
    };

    {
        // mark in-progress checkpoints as failed
        let last_epoch = checkpoint_info
            .as_ref()
            .map(|checkpoint_info| checkpoint_info.epoch)
            .unwrap_or(0);

        let client = match ctx.db.client().await {
            Ok(c) => c,
            Err(e) => {
                return Err(ctx.retryable(
                    state,
                    "failed to get db client for mark_failed",
                    e.into(),
                    10,
                ));
            }
        };

        if let Err(e) = controller_queries::execute_mark_failed(
            &client,
            &*ctx.config.id,
            &(last_epoch as i32 + 1),
        )
        .await
        {
            return Err(ctx.retryable(
                state,
                "failed to mark in-progress checkpoints as failed",
                e.into(),
                10,
            ));
        }
    }

    let committing_state = match &checkpoint_info {
        Some(info) => {
            let storage_role = StorageProviderFor::Controller {
                storage_url: ctx.pipeline_info.state_url.clone(),
            };
            // The operators the workers of this job will actually construct, keyed to the
            // parallelism each is built at. This is the same derivation the checkpoint
            // writer uses to fill in `operator_ids`, so it covers every operator of every
            // chain and not just the chain heads — a chained operator is restored, and
            // commits for it are replayed, exactly like any other.
            let restoring = ctx.program.tasks_per_operator();

            match prepare_restored_checkpoint(
                &storage_role,
                ctx.execution_selector,
                &ctx.config.id,
                info,
                &restoring,
            )
            .await
            {
                Ok(committing_state) => committing_state,
                Err(RestorePreparationError::Retryable { message, source }) => {
                    return Err(ctx.retryable(state, message, source, 10));
                }
                Err(RestorePreparationError::Fatal { message, source }) => {
                    return Err(fatal(message, source));
                }
            }
        }
        None => None,
    };

    Ok((state, checkpoint_info, committing_state))
}

/// Why a checkpoint the job is about to restore from could not be prepared.
///
/// Splitting the two is the caller's whole interest in this error: a storage failure is
/// worth retrying and a checkpoint this job may not read is not, however many times it is
/// tried.
#[derive(Debug)]
enum RestorePreparationError {
    /// Reading or writing checkpoint metadata failed; the same attempt may succeed later.
    Retryable {
        message: String,
        source: anyhow::Error,
    },
    /// The checkpoint cannot be restored by this job at all.
    Fatal {
        message: String,
        source: anyhow::Error,
    },
}

/// Prepares the checkpoint a legacy-mode job is about to restore from, and derives the
/// commit replay it implies.
///
/// The order here is the point of this function. A restore rewrites the checkpoint's
/// top-level metadata and then starts workers that build one operator's tables at a time,
/// so the whole checkpoint has to be classified before either happens:
///
/// 1. load the checkpoint's own metadata,
/// 2. check that the checkpoint covers exactly the operators the workers will build, then
///    load **every** one of their metadata objects and check each one's table configs
///    against the job's selector — this is the preflight, and it writes nothing,
/// 3. derive any committing state from the metadata already loaded in step 2,
/// 4. only then rewrite the top-level metadata with the new min epoch.
///
/// Validating each operator as it was reached, which is what step 2 replaced, still let
/// the operators before the disagreeing one be rebuilt, and let the metadata rewrite
/// happen, before the disagreement was seen. Steps 3 and 4 are also in this order for the
/// same reason: a checkpoint whose committing data cannot be derived is left exactly as it
/// was found.
///
/// Step 3 consumes the objects step 2 loaded rather than reading them again, so a
/// checkpoint that needs commits costs the same reads it did before the preflight existed;
/// a `ready` checkpoint pays one metadata read per operator, which is the read each worker
/// was about to do anyway as it built that operator.
///
/// `restoring` maps every operator the job's workers will construct to the parallelism it
/// is built at — `LogicalProgram::tasks_per_operator`. One value serves both halves of the
/// preflight: its key set is what the checkpoint must cover, and the same set is what
/// commit replay is derived over, so the operators that were checked are exactly the
/// operators that are acted on.
///
/// # Errors
///
/// Returns [`RestorePreparationError::Fatal`] if the checkpoint was written by a different
/// backend than the job selects, if the checkpoint does not cover exactly the operators
/// the workers will build, if any of them has no metadata object, or if its tables cannot
/// be interpreted — in each case nothing has been rewritten. Returns
/// [`RestorePreparationError::Retryable`] for the storage failures reading or writing
/// metadata can produce.
async fn prepare_restored_checkpoint(
    storage_role: &StorageProviderFor,
    job: StateBackendSelector,
    job_id: &str,
    checkpoint: &CheckpointInfo,
    restoring: &HashMap<String, usize>,
) -> Result<Option<CommittingState>, RestorePreparationError> {
    let CheckpointInfo {
        id,
        epoch,
        min_epoch,
        needs_commits,
    } = checkpoint;
    let epoch = *epoch;

    let retryable = |message: String| {
        move |err: StateStoreError| RestorePreparationError::Retryable {
            message,
            source: err.into(),
        }
    };

    let mut metadata = StateBackend::load_checkpoint_metadata(storage_role, job_id, epoch as u32)
        .await
        .map_err(retryable(format!(
            "Failed to load checkpoint metadata for epoch {epoch}"
        )))?;

    metadata.min_epoch = *min_epoch as u32;

    // The preflight. The checkpoint row said which backend wrote this checkpoint; each
    // operator's own table configs say it per table, and every one of them is checked
    // here — before the rewrite below, and before any worker is started. The set that is
    // checked is the workers' own, not the checkpoint's list, so an operator the
    // checkpoint omits cannot slip past unread and then fail in a worker.
    let expected: HashSet<&str> = restoring.keys().map(String::as_str).collect();
    // The checkpoint this job asked for: its own id, and the epoch its own `checkpoints` row
    // named. Every operator object the preflight reads is read from the *loaded object's*
    // `job_id` and `epoch`, and the metadata rewrite below writes back to them, so what the
    // caller asked for has to be carried in beside them rather than assumed to match.
    let asked_for = CheckpointIdentity::new(job_id, epoch as u32);
    let preflight = StateBackend::load_checkpoint_operators(
        storage_role,
        job,
        &asked_for,
        &expected,
        &metadata,
    )
    .await
    .map_err(|err| match err {
        StateStoreError::StateBackendError(e) => RestorePreparationError::Fatal {
            message: "cannot restore a checkpoint written with a different state backend"
                .to_string(),
            source: e.into(),
        },
        // Not retryable: the same checkpoint, and the same program, are read again on
        // every attempt.
        err @ StateStoreError::IncompleteCheckpoint { .. } => RestorePreparationError::Fatal {
            message: "cannot restore a checkpoint that does not cover the job's operators"
                .to_string(),
            source: err.into(),
        },
        err => RestorePreparationError::Retryable {
            message: format!("Failed to load operator metadata for epoch {epoch}"),
            source: err.into(),
        },
    })?;

    let mut committing_state = None;
    if *needs_commits {
        let mut commit_subtasks = HashSet::new();
        // (operator_id => (table_name => (subtask => data)))
        let mut committing_data: HashMap<String, HashMap<String, HashMap<u32, Vec<u8>>>> =
            HashMap::new();
        // Exactly the operators the preflight covered, which is exactly the set the
        // workers will build: the coverage check above is what makes those three the same
        // set, so nothing is committed for an operator that was not validated.
        for (operator_id, operator_metadata) in preflight.get().operators() {
            for (table_name, table_metadata) in &operator_metadata.table_checkpoint_metadata {
                let config = operator_metadata
                    .table_configs
                    .get(table_name)
                    .ok_or_else(|| RestorePreparationError::Fatal {
                        message: format!(
                            "Failed to restore job; table config for {table_name} not found."
                        ),
                        source: anyhow!("table config for {} not found", table_name),
                    })?;
                if let Some(commit_data) = match config.table_type() {
                    arroyo_rpc::grpc::rpc::TableEnum::MissingTableType => {
                        return Err(RestorePreparationError::Fatal {
                            message: "Missing table type".to_string(),
                            source: anyhow!("table type not found"),
                        });
                    }
                    arroyo_rpc::grpc::rpc::TableEnum::GlobalKeyValue => {
                        GlobalKeyedTable::committing_data(config.clone(), table_metadata)
                    }
                    arroyo_rpc::grpc::rpc::TableEnum::ExpiringKeyedTimeTable => None,
                } {
                    committing_data
                        .entry(operator_id.clone())
                        .or_default()
                        .insert(table_name.to_string(), commit_data);
                    // Present by construction: `operators` is the coverage-checked set,
                    // which is `restoring`'s key set.
                    let parallelism = *restoring.get(operator_id).ok_or_else(|| {
                        RestorePreparationError::Fatal {
                            message: format!(
                                "Failed to restore job; operator {operator_id} is not in the \
                                 job's program."
                            ),
                            source: anyhow!("operator {} not found in program", operator_id),
                        }
                    })?;
                    for subtask_index in 0..parallelism {
                        commit_subtasks.insert((operator_id.clone(), subtask_index as u32));
                    }
                }
            }
        }
        committing_state = Some(CommittingState::new(
            CheckpointIdOrRef::CheckpointId(id.clone()),
            commit_subtasks,
            committing_data,
        ));
    }

    // The rewrite takes the preflight's token, not the metadata on its own: this is the
    // write that makes the checkpoint the one a restart reads, and step 2 is what entitles
    // it. Skipping the preflight for a `ready` checkpoint — which is what this path used to
    // do — is no longer expressible.
    let write = Validated::validate(
        CheckpointMetadataWrite::after_restore_preflight(metadata, &preflight),
        (),
    )
    .map_err(|err| RestorePreparationError::Fatal {
        message: "cannot rewrite a checkpoint's metadata for operators that were not \
                  validated"
            .to_string(),
        source: err.into(),
    })?;

    StateBackend::write_checkpoint_metadata(storage_role, write)
        .await
        .map_err(retryable(format!(
            "Failed to write checkpoint metadata for epoch {epoch}"
        )))?;

    Ok(committing_state)
}

async fn get_and_register_checkpoint_info_leader<'a>(
    ctx: &'a JobContext<'a>,
) -> anyhow::Result<Option<CheckpointInfo>> {
    // in the future, this should likely move to the leader, but that will require rethinking how
    // worker initialization works
    let storage_role = StorageProviderFor::Controller {
        storage_url: ctx.pipeline_info.state_url.clone(),
    };
    let storage_provider = get_storage_provider(&storage_role).await?;
    let ignore_state = ctx
        .config
        .ignore_state_before_epoch
        .and_then(|generation| generation.try_into().ok())
        == Some(ctx.status.generation);

    if ignore_state {
        info!(
            message = "starting leader generation without state",
            job_id = %ctx.config.id,
            generation = ctx.status.generation,
        );
    }

    // Leader mode keeps no `checkpoints` row, so the recovery manifest is the whole
    // persisted record of both the backend that wrote the checkpoint this generation would
    // restore from and the operators it describes. `initialize_generation` reads it and
    // checks both — that it covers exactly the operators the workers will build, and that
    // its table configs agree with the job's selector — before it writes either the
    // current-generation file or the new generation manifest, so a mismatch leaves no
    // published state pointing at a checkpoint this job cannot restore, and returns the
    // manifest it validated for use here.
    let new_gen = initialize_generation(
        storage_provider.as_ref(),
        InitializeGenerationRequest {
            pipeline_id: ctx.pipeline_info.pipeline_id.clone(),
            job_id: JobId(ctx.config.id.clone()),
            generation: Generation(ctx.status.generation),
            updated_at: SystemTime::now(),
            state_backend: ctx.execution_selector,
            // The same derivation the legacy preflight uses, so both modes require the
            // recovery checkpoint to cover the same set: every operator of every chain,
            // not just the chain heads.
            program_operators: ctx.program.tasks_per_operator().into_keys().collect(),
        },
        true,
    )
    .await?;

    let recovery_checkpoint = match new_gen {
        GenerationInitialization::Initialized {
            recovery: GenerationRecovery::NoCheckpoint,
            ..
        } => None,
        GenerationInitialization::Initialized {
            recovery:
                GenerationRecovery::Ready { checkpoint_ref }
                | GenerationRecovery::ReplayCommit { checkpoint_ref, .. },
            recovery_checkpoint,
            ..
        } => Some((checkpoint_ref, recovery_checkpoint)),
        GenerationInitialization::StaleGeneration { .. } => {
            unreachable!(
                "cannot end up with stale generation given that we updated the generation\
            instead of checking it"
            );
        }
        GenerationInitialization::StopOrphaned { canonical_ref } => {
            bail!(
                "somehow ended up with an orphaned checkpoint during recovery... should not happen\
            canonical_ref = {:?}",
                canonical_ref
            );
        }
        GenerationInitialization::Failed(failure) => {
            bail!(
                "failed while resolving restoration checkpoint: {:?}",
                failure
            );
        }
    };

    Ok(if let Some((r, manifest)) = recovery_checkpoint {
        // Already read and already validated inside `initialize_generation`, above; using
        // it rather than re-reading is also what guarantees the checkpoint this generation
        // was published against is the one that passed validation.
        let manifest =
            manifest.ok_or_else(|| anyhow!("recovery checkpoint manifest {r} is missing!"))?;

        Some(CheckpointInfo {
            epoch: manifest.epoch,
            min_epoch: manifest.min_epoch,
            id: r.to_string(),
            needs_commits: manifest.needs_commit,
        })
    } else {
        None
    })
}

impl Scheduling {
    async fn start_workers<'a>(
        self: Box<Self>,
        ctx: &mut JobContext<'a>,
        slots_needed: usize,
    ) -> Result<Box<Self>, StateError> {
        let start = Instant::now();
        let mut env_vars: HashMap<String, String> =
            serde_json::from_value(ctx.config.env_vars.clone()).map_err(|e| {
                fatal(
                    format!("failed to deserialize env_vars from job_configs: {e}"),
                    anyhow::anyhow!("malformed env_vars in job_configs row"),
                )
            })?;

        let checkpoint_url = ctx
            .pipeline_info
            .state_url
            .clone()
            .unwrap_or_else(|| config().checkpoint_url.clone());
        env_vars
            .insert("ARROYO__CHECKPOINT_URL".to_string(), checkpoint_url)
            .inspect(|_| {
                warn!(
                    job_id = %ctx.config.id,
                    pipeline_id = *ctx.pipeline_info.pipeline_id,
                    key = "ARROYO__CHECKPOINT_URL",
                    "job env_vars contained a reserved infrastructure key; \
                     user-supplied value has been overridden by the controller"
                );
            });

        // Threaded from the controller that owns this job rather than read out of a
        // process-global. The value is the same one in production; what changes is that
        // running this code no longer requires a process to have an identity installed in it,
        // so nothing that exercises `Scheduling` has any reason to install one.
        env_vars
            .insert(CLUSTER_ID_ENV.to_string(), (*ctx.cluster_id).clone())
            .inspect(|_| {
                warn!(
                    job_id = %ctx.config.id,
                    pipeline_id = *ctx.pipeline_info.pipeline_id,
                    key = CLUSTER_ID_ENV,
                    "job env_vars contained a reserved infrastructure key; \
                     user-supplied value has been overridden by the controller"
                );
            });

        loop {
            match ctx
                .scheduler
                .start_workers(StartPipelineReq {
                    program: ctx.program.clone(),
                    wasm_path: "".to_string(),
                    pipeline_id: ctx.pipeline_info.pipeline_id.clone(),
                    organization_id: ctx.config.organization_id.clone(),
                    job_id: JobId(ctx.config.id.clone()),
                    generation: ctx.status.generation,
                    name: ctx.config.pipeline_name.clone(),
                    hash: ctx.program.get_hash(),
                    slots: slots_needed,
                    env_vars: env_vars.clone(),
                    pipeline_tags: ctx.pipeline_info.tags.clone(),
                    scheduler_config: ctx.config.scheduler_config.clone(),
                })
                .await
            {
                Ok(_) => break,
                Err(SchedulerError::NotEnoughSlots { slots_needed: s }) => {
                    warn!(
                        message = "not enough slots for job",
                        job_id = %ctx.config.id,
                        pipeline_id = *ctx.pipeline_info.pipeline_id,
                        slots_for_job = slots_needed,
                        slots_needed = s
                    );
                    if start.elapsed() > *config().pipeline.worker_startup_time {
                        return Err(fatal(
                            "Not enough slots to schedule job",
                            anyhow!("scheduler error -- needed {} slots", slots_needed),
                        ));
                    }
                }
                Err(SchedulerError::Other(s)) => {
                    return Err(ctx.retryable(
                        self,
                        "encountered error during scheduling",
                        anyhow::anyhow!("scheduling error: {}", s),
                        20,
                    ));
                }
                Err(SchedulerError::Fatal(s)) => {
                    return Err(fatal(
                        format!("scheduling failed: {s}"),
                        anyhow::anyhow!("non-retryable scheduling error: {}", s),
                    ));
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok(self)
    }
}

#[async_trait::async_trait]
impl State for Scheduling {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    /// Leaves, by the same macro both of this state's bodies open with.
    ///
    /// `Scheduling` already reads the published configuration before it does anything, in the
    /// legacy body's first statement and in the phase graph's `schedule`, so this changes no
    /// outcome for it. It is here because the trait admits no default: the state that persists
    /// an incremented generation and starts a cluster is the last one whose stop check should
    /// depend on a statement staying first in a five-hundred-line function.
    fn leave_for_stop(self: Box<Self>, config: &JobConfig) -> LeavingForStop {
        leaving::leaves_not_running(self, config)
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        // if we've started in scheduling but the job isn't supposed to be running, don't try
        // to schedule
        stop_if_desired_non_running!(self, &ctx.config);

        // `Scheduling` alternates between two kinds of phase, and the whole of this state's
        // refusal safety is the boundary between them.
        //
        // An *irreversible* phase does work that cannot be undone and never touches the job's
        // channel: this preamble (persist an incremented generation, tear down whatever
        // cluster the job is running on, start a replacement one, prepare checkpoint
        // recovery), the `StartExecution` fan-out that puts the job's own program on those
        // workers, and the publication of a restored checkpoint's commits. An *interruptible*
        // phase does nothing irreversible and waits on `ctx.rx.recv`: the wait for workers to
        // connect, and the wait for their tasks to start.
        //
        // Every irreversible phase runs under an `Admission` taken at its head, and there are
        // exactly three of them. The guard is the same lock `StateMachine::refuse_config` must
        // take to publish a refusal at all, so for as long as it is held nothing can be
        // published; and the gate is re-read as it is taken, so whatever *was* published
        // before the crossing stops the phase before its first effect.
        //
        // Re-reading at the crossing is the part that does not depend on the channel. An
        // interruptible phase ends as soon as enough messages have arrived — enough slots,
        // enough started tasks — and it ends on the message that made the count, so a refusal
        // published while it waited can still be sitting behind those messages, unread, or
        // never have been queued at all because the queue was full. The gate is written in
        // either case, and the gate is what the crossing reads.
        //
        // The admission is dropped before each `recv`: holding it across a wait that can take
        // minutes would make the job unrefusable for exactly as long as it is least able to
        // defend itself. It must also be dropped before the next crossing takes it again — the
        // guard does not re-enter.
        let admission = ctx.admit_irreversible_scheduling().await;

        // update the generation for this scheduling attempt
        ctx.status.generation += 1;
        match admission
            .effect(
                "persist the incremented scheduling generation",
                ctx.publish_status(),
            )
            .await
        {
            Ok(StatusPublication::Published) => {}
            // Unreachable on this body — it runs only for a job on the landed mechanism, whose
            // publication is unconditional and cannot be refused by another controller's
            // authority — and answered rather than asserted, because the answer a superseded
            // controller owes is the same everywhere: stand down, do not retry, do not fail a
            // job somebody else is running.
            Ok(StatusPublication::Superseded(stale)) => {
                stand_down(stale);
                return Ok(Transition::Stop);
            }
            Err(e) => {
                return Err(ctx.retryable(
                    self,
                    "failed to advance generation for scheduling retry",
                    anyhow!("{}", e),
                    10,
                ));
            }
        }

        // clear out any existing workers for this job
        if let Err(e) = admission
            .effect(
                "tear down the job's existing cluster",
                ctx.scheduler.stop_workers(&ctx.config.id, None, true),
            )
            .await
        {
            warn!(
                message = "failed to clean cluster prior to scheduling",
                job_id = %ctx.config.id,
                pipeline_id = *ctx.pipeline_info.pipeline_id,
                error = format!("{:?}", e)
            )
        }

        ctx.program
            .update_parallelism(&ctx.config.parallelism_overrides);

        let slots_needed: usize = slots_for_job(&*ctx.program);
        self = admission
            .effect(
                "start the job's replacement workers",
                self.start_workers(ctx, slots_needed),
            )
            .await?;

        let leader_mode = matches!(config().job_controller, JobControllerMode::Worker);

        let (self, checkpoint_info, committing_state) = if leader_mode {
            match admission
                .effect(
                    "register the generation and prepare its recovery checkpoint",
                    get_and_register_checkpoint_info_leader(ctx),
                )
                .await
            {
                Ok(ci) => (self, ci, None),
                // A rejected selector is not a transient failure: every attempt resolves
                // the same recovery manifest, so retrying only delays the report.
                Err(e) if e.downcast_ref::<StateBackendError>().is_some() => {
                    return Err(fatal(
                        "cannot restore a checkpoint written with a different state backend",
                        e,
                    ));
                }
                Err(e) => {
                    return Err(ctx.retryable(self, "failed to load checkpoint metadata", e, 20));
                }
            }
        } else {
            admission
                .effect(
                    "prepare the legacy recovery checkpoint",
                    get_checkpoint_info_legacy(self, ctx),
                )
                .await?
        };

        // The preamble is over and the next phase only waits, so the admission is released:
        // a refusal raised from here on is publishable again, and is read from the gate at the
        // next crossing whether or not the loop below ever gets to its message.
        drop(admission);

        // wait for them to connect and make outbound RPC connections
        let mut workers = HashMap::new();
        let worker_connects = Arc::new(Mutex::new(HashMap::new()));
        let mut handles = vec![];

        let pipeline_config = &config().pipeline;

        let start = Instant::now();
        loop {
            // D39a's second consumption point, on every turn of an interruptible wait.
            // This loop breaks on the message that makes its count rather than on the last
            // message in the queue, so a decision published while it waited can be sitting
            // behind those messages, unread; reading the job's intent here does not depend
            // on the channel at all. Not `async`, so it adds nothing to what the pinned
            // interruptible stretches await. A no-op in this body, which only a job built
            // in the pre-flag-day peer mode reaches: such a job has no actor, so the answer
            // is always `ObservedIntent::Continue`.
            //
            // A consumed stop leaves through the same macro the message-borne one below
            // uses. This body is not reachable under the other mode — `run_state_body`
            // sends a job with a D39a writer to the phase graph — so the read can only
            // ever be the no-op above; it is written this way so that no consumption
            // point in the crate can answer "the job stops" and be read as "carry on".
            match ctx.observe_lifecycle_intent(ConsumptionPoint::InsideInterruptibleWait)? {
                ObservedIntent::Stop => {
                    stop_if_desired_non_running!(self, &ctx.config);
                }
                // Nothing further. Besides its stop, all this loop's `ConfigUpdate` arm does
                // with an update is `check_config_update` — and a configuration that changes
                // the job's state backend is refused by the job's writer rather than adopted,
                // so an adopted configuration arrives here with that check already made. The
                // `StartExecutionReq` below stamps `ctx.config`'s selector into every worker,
                // and `ctx.config` is what the writer published into.
                ObservedIntent::Adopted(_) | ObservedIntent::Continue => {}
            }

            let timeout = pipeline_config
                .worker_startup_time
                .min(
                    ctx.config
                        .ttl
                        .unwrap_or(*pipeline_config.worker_startup_time),
                )
                .checked_sub(start.elapsed())
                .unwrap_or(Duration::ZERO);

            tokio::select! {
                val = ctx.rx.recv() => {
                    match val {
                        Some(JobMessage::ConfigUpdate(c)) => {
                            stop_if_desired_non_running!(self, &c);
                            // The workers this loop is waiting for were started from the
                            // job's selector, and the `StartExecutionReq` below stamps it
                            // into every one of them. An update that changes it may not be
                            // carried into either.
                            check_config_update(ctx.execution_selector, &c)?;
                        }
                        Some(msg) => {
                            handle_worker_connect(msg, &mut workers, worker_connects.clone(), &mut handles, ctx).await?;
                        }
                        None => {
                            panic!("Job message channel closed: {}", ctx.config.id);
                        }
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    return Err(ctx.retryable(self,
                        "timed out while waiting for workers to start",
                        anyhow!("timed out after {:?} while waiting for worker startup", *pipeline_config.worker_startup_time), 3));
                }
            }

            if workers.values().map(|w| w.slots).sum::<usize>() >= slots_needed {
                break;
            }
        }

        for h in handles {
            if let Err(e) = h.await {
                return Err(ctx.retryable(
                    self,
                    "Failed to start cluster for pipeline",
                    e.into(),
                    10,
                ));
            }
        }

        // Compute assignments and send to workers

        let assignments = compute_assignments(workers.values().collect(), &*ctx.program);
        let worker_connects = Arc::try_unwrap(worker_connects).unwrap().into_inner();
        let program = api::ArrowProgram::from(ctx.program.clone());

        // In controller mode this is an epoch threshold. Leader mode uses the same field as a
        // generation number, so it must not affect the checkpoint epoch.
        let default_epoch = if leader_mode {
            0
        } else {
            ctx.config
                .ignore_state_before_epoch
                .filter(|&t| t > 0)
                .map(|t| {
                    let epoch = (t - 1) as u64;
                    info!(
                        message = "starting from ignore_state_before_epoch threshold",
                        job_id = %ctx.config.id,
                        pipeline_id = *ctx.pipeline_info.pipeline_id,
                        default_epoch = epoch,
                    );
                    epoch
                })
                .unwrap_or(0)
        };

        let start_epoch = checkpoint_info
            .as_ref()
            .map(|info| info.epoch)
            .unwrap_or(default_epoch);
        let min_epoch = checkpoint_info
            .as_ref()
            .map(|info| info.min_epoch)
            .unwrap_or(default_epoch);

        let leader_info = leader_mode.then(|| {
            workers
                .iter()
                .min_by_key(|w| w.0.0)
                .map(|(id, status)| (*id, status.rpc_address.clone()))
                .unwrap()
        });

        let checkpoint_interval_micros = ctx.config.checkpoint_interval.as_micros() as u64;
        // The job's own selector, fixed for the life of this execution; every worker in
        // this job — leader and non-leader alike — is started with the same value. Read
        // from the execution rather than from the configuration cell, which is refreshed
        // from the database after every transition.
        let state_backend = ctx.execution_selector;

        let plan = ExecutionPlan {
            assignments,
            program,
            restore_epoch: checkpoint_info.as_ref().map(|info| info.epoch),
            start_epoch,
            min_epoch,
            leader: leader_info.clone(),
            checkpoint_manifest_ref: leader_info
                .as_ref()
                .and(checkpoint_info.as_ref())
                .map(|ci| ci.id.clone()),
            checkpoint_interval_micros,
            state_backend,
        };

        // The second crossing. The loop above broke on a queued `WorkerConnect` the moment the
        // slots added up; a refusal published while it was waiting can be behind that message,
        // unread, or never have been queued at all. Neither is a reason to start executing a
        // configuration the controller has refused, so the decision is taken from the gate
        // here, and the admission is held across the fan-out below — which is where this job's
        // program actually begins to run, restoring state under the job's selector and
        // producing to its sinks.
        //
        // The fan-out owns the admission for as long as the requests it issued are unsettled,
        // and hands it back only once every one of them has an outcome — a sibling's failure
        // no longer ends it early, and this task being cancelled no longer releases the
        // admission out from under it. A dropped client future is not a stopped worker, so
        // "nothing is still asking a worker to start" is established by waiting for the
        // answers rather than by taking the requests away. See `start_execution_on_workers`.
        // Nothing below may be issued to a worker that has not advertised the reconciliation
        // contract, and this is where that is enforced — before the admission is taken, because
        // refusing to schedule is not an irreversible effect and awaits nothing.
        //
        // Everything the fan-out does with an unsettled request assumes all three clauses of
        // that contract: it replays the same `start_execution_id` expecting an idempotent
        // acknowledgement, it reads `Unavailable` as transport rather than as an answer, and it
        // ends the attempt in the knowledge that no handler can be parked waiting for a phase
        // lock. A worker predating the contract satisfies none of them — it ignores the ID, it
        // answers `Unavailable` from its `Initializing` branch, and it takes the phase lock
        // blocking, so a handler of its can still be inside `poll` when the controller that
        // issued it exits and cannot be reached by any refusal a replacement publishes.
        //
        // So the mixed-version case is removed rather than reasoned about. This is the
        // worker-first rollout requirement made mechanical: a controller of this version never
        // puts a request into a worker that cannot reconcile it, which is what lets the
        // interlock below be a property of the operation instead of a property of the peer.
        let unreconciled: Vec<u64> = workers
            .values()
            .filter(|w| !w.reconciles_start_execution)
            .map(|w| w.id.0)
            .collect();
        if !unreconciled.is_empty() {
            return Err(ctx.retryable(
                self,
                "workers predating the StartExecution reconciliation contract",
                anyhow!(
                    "workers {unreconciled:?} did not advertise \
                     `reconciles_start_execution`; upgrade the worker image to at least this \
                     controller's version before scheduling this job"
                ),
                10,
            ));
        }

        let machine_ids: HashMap<WorkerId, MachineId> = workers
            .iter()
            .map(|(id, status)| (*id, status.machine_id.clone()))
            .collect();
        // The fan-out's own inventory of what it issues, bounded by the number of target
        // workers. The landed path does not consult it: acting on an attempt that never
        // settled needs the M11.D39b phase graph, which this body is not, so this is
        // created here and dropped with the region. It is the fan-out that keeps the record,
        // rather than either caller, so both routes are recording the same thing.
        //
        // The protocol this job's directives are issued under, derived from the job's own
        // lifecycle mechanism rather than written here (M11.D39e(i)). This body runs only for a
        // job whose mechanism is `JobLifecycle::LegacyT08` — `run_state_body` sends every other
        // one to the M11.D39b phase graph — so the answer is `Legacy`, and it is an answer
        // rather than a literal so that activating the fence does not leave a hard-coded zero
        // behind here.
        let protocol = match ctx.fence_protocol() {
            Ok(protocol) => protocol,
            Err(e) => {
                return Err(ctx.retryable(
                    self,
                    "the job's lifecycle fence cannot address its worker generations",
                    anyhow!("{e}"),
                    10,
                ));
            }
        };
        // No settlement owner, which is what a job running the landed mechanism has (M11.T26e):
        // the rescue releases the admission once the requests settle, exactly as it always has.
        // The generation and the fence are the job's own, so that this route's inventory says
        // what the phase graph's says about which worker generation its identifiers addressed
        // and under which fence. Built after the protocol above for that reason: the fence is
        // read from the same answer the directives are stamped from, not from a second one.
        let attempts = Arc::new(AttemptLedger::owned_by(
            None,
            ctx.status.generation,
            protocol.fence(),
        ));
        let Some(targets) = StartTargets::without_a_handshake(protocol, worker_connects) else {
            return Err(ctx.retryable(
                self,
                "a fenced job reached the pre-fence scheduling body",
                anyhow!(
                    "the landed M11.T08 scheduling body cannot advance a lifecycle fence, so a                      job running the fenced protocol belongs to the M11.D39b phase graph"
                ),
                10,
            ));
        };
        let admission = ctx.admit_irreversible_scheduling().await;
        let (admission, started) = start_execution_on_workers(
            admission,
            ctx.config.id.clone(),
            ctx.pipeline_info.pipeline_id.clone(),
            plan,
            machine_ids,
            targets,
            attempts,
        )
        .await;
        drop(admission);

        let worker_connects = match started {
            Ok(connects) => connects,
            Err(e) => {
                return Err(ctx.retryable(self, "failed to initialize workers", e, 10));
            }
        };
        for id in worker_connects.keys() {
            if let Some(worker) = workers.get_mut(id) {
                worker.state = WorkerState::Initializing;
            }
        }

        // Now wait until all tasks are running
        let start = Instant::now();
        let mut started_tasks = HashSet::new();
        while started_tasks.len() < ctx.program.task_count() {
            // The same consumption point, in the other interruptible wait, for the same
            // reason: this loop also breaks on the message that makes its count, and for
            // the same reason a stop it consumes leaves rather than being written down.
            match ctx.observe_lifecycle_intent(ConsumptionPoint::InsideInterruptibleWait)? {
                ObservedIntent::Stop => {
                    stop_if_desired_non_running!(self, &ctx.config);
                }
                // Nothing further, for the same reason as the loop above: the selector guard
                // is the whole of what its `ConfigUpdate` arm does besides its stop, and the
                // writer refuses a selector change rather than adopting it.
                ObservedIntent::Adopted(_) | ObservedIntent::Continue => {}
            }

            let timeout = pipeline_config
                .task_startup_time
                .min(ctx.config.ttl.unwrap_or(*pipeline_config.task_startup_time))
                .checked_sub(start.elapsed())
                .unwrap_or(Duration::ZERO);

            select! {
                v = ctx.rx.recv() => {
                    match v {
                        Some(JobMessage::WorkerInitializationComplete {
                            worker_id,
                            success,
                            error_message,
                        }) => {
                            if let Some(worker) = workers.get_mut(&worker_id) {
                                if success {
                                    worker.state = WorkerState::Ready;
                                    info!(
                                        message = "worker initialization completed successfully",
                                        job_id = %ctx.config.id,
                                        pipeline_id = *ctx.pipeline_info.pipeline_id,
                                        worker_id = worker_id.0,
                                        machine_id = *worker.machine_id.0,
                                    );
                                } else {
                                    let error = error_message.unwrap_or_else(|| "Unknown error".to_string());
                                    worker.state = WorkerState::Failed;
                                    error!(
                                        message = "worker initialization failed",
                                        job_id = %ctx.config.id,
                                        pipeline_id = *ctx.pipeline_info.pipeline_id,
                                        worker_id = worker_id.0,
                                        machine_id = *worker.machine_id.0,
                                        error = error
                                    );
                                    return Err(ctx.retryable(self, "worker initialization failed",
                                        anyhow!("worker {} initialization failed: {}", worker_id.0, error), 5));
                                }
                            }
                        }
                        Some(JobMessage::TaskStarted {
                            task_id,
                            subtask_idx,
                            ..
                        }) => {
                            started_tasks.insert((task_id, subtask_idx));
                        }
                        Some(JobMessage::RunningMessage(RunningMessage::TaskFailed(event))) => {
                            return ctx.handle_task_error(self, event).await;
                        }
                        Some(JobMessage::ConfigUpdate(c)) => {
                            stop_if_desired_non_running!(self, &c);
                            // The tasks this loop is waiting for are already building
                            // their state under the job's selector; the job cannot adopt
                            // a different one now.
                            check_config_update(ctx.execution_selector, &c)?;
                        }
                        Some(msg) => {
                            ctx.handle(msg)?;
                        }
                        None => {
                            panic!("Job queue shutdown");
                        }
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    // if we timeout in leader mode, it may mean that the job failed on startup
                    // in which case, we need to collect the error to understand why (and handle
                    // it appropriately)
                    if let Some((id, addr)) = leader_info.clone()
                        && let Ok(mut leader_manager) = LeaderManager::connect(
                            JobId(ctx.config.id.clone()),
                            ctx.pipeline_info.pipeline_id.clone(),
                            ctx.status.generation,
                            id,
                            addr,
                            config().controller.connect_timeout.as_deref().copied(),
                            ctx.execution_selector,
                        ).await
                            && let Ok(status) = leader_manager.poll_leader_status().await {
                                match JobState::try_from(status.job_state) {
                                    Ok(JobState::JobFailing | JobState::JobFailed) => {
                                        let Some(failure) = status.job_failure else {
                                            return Err(ctx.retryable(
                                                self,
                                                "leader reported failing status without failure payload",
                                                anyhow!("missing job failure"),
                                                10,
                                            ));
                                        };
                                        return ctx.handle_job_failure(*self, failure).await;
                                    }
                                    Ok(_) => {}
                                    Err(e) => {
                                        warn!(
                                            message = "leader returned invalid job state before task startup timeout",
                                            error = format!("{:?}", e),
                                            job_id = %ctx.config.id,
                                            pipeline_id = *ctx.pipeline_info.pipeline_id,
                                        );
                                    }
                                }
                            }

                    return Err(ctx.retryable(self,
                        "timed out while waiting for tasks to start",
                        anyhow!("timed out after {:?} while waiting for worker startup", *pipeline_config.task_startup_time), 3));
                }
            }
        }

        ctx.status.tasks = Some(ctx.program.task_count() as i32);

        let needs_commit = committing_state.is_some();

        let program = Arc::new(ctx.program.clone());
        let metrics_enabled = config().controller.metrics.enabled;

        match leader_info {
            None => {
                let metrics = if metrics_enabled {
                    let metrics = JobMetrics::new(program.clone());
                    ctx.metrics
                        .write()
                        .await
                        .insert(ctx.config.id.clone(), metrics.clone());
                    Some(metrics)
                } else {
                    None
                };

                let checkpoint_store = Arc::new(DbCheckpointMetadataStore {
                    organization_id: ctx.config.organization_id.clone(),
                    job_id: ctx.config.id.clone(),
                    state_backend: ctx.execution_selector,
                    db: ctx.db.clone(),
                });
                let mut controller = JobController::new(
                    checkpoint_store,
                    ctx.config.clone(),
                    ctx.pipeline_info.pipeline_id.clone(),
                    ctx.pipeline_info.state_url.clone(),
                    ctx.status.generation,
                    program,
                    start_epoch,
                    min_epoch,
                    worker_connects,
                    committing_state,
                    metrics,
                    protocol,
                );
                if needs_commit {
                    info!(
                        job_id = %ctx.config.id,
                        pipeline_id = *ctx.pipeline_info.pipeline_id,
                        "restored checkpoint was in committing phase, sending commits"
                    );
                    // The third crossing, and the same argument as the second: the loop above
                    // broke on a queued `TaskStarted` and a refusal published while it ran can
                    // be behind that message. These commits finish a two-phase commit against
                    // the job's sinks — they are visible outside the cluster and cannot be
                    // withdrawn — so the gate is read again here, and held until they are out.
                    let admission = ctx.admit_irreversible_scheduling().await;
                    admission
                        .effect(
                            "publish the restored checkpoint's commits",
                            controller.send_commit_messages(),
                        )
                        .await
                        .expect("failed to send commit messages");
                    drop(admission);
                }

                ctx.job_controller = Some(controller);
                Ok(Transition::next(*self, Running {}))
            }
            Some((id, addr)) => {
                ctx.job_controller = None;

                let leader_manager = match LeaderManager::connect(
                    JobId(ctx.config.id.clone()),
                    ctx.pipeline_info.pipeline_id.clone(),
                    ctx.status.generation,
                    id,
                    addr.clone(),
                    config().controller.connect_timeout.as_deref().copied(),
                    ctx.execution_selector,
                )
                .await
                {
                    Ok(m) => m,
                    Err(e) => {
                        return Err(ctx.retryable(
                            self,
                            "failed to connect to worker leader",
                            e,
                            10,
                        ));
                    }
                };

                ctx.leader_manager = Some(leader_manager);
                ctx.status.state_context.leader = Some(LeaderContext {
                    worker_id: id,
                    rpc_address: addr,
                    generation: ctx.status.generation,
                });

                Ok(Transition::next(
                    *self,
                    LeaderRunning {
                        started: Instant::now(),
                    },
                ))
            }
        }
    }
}

// The M11.T25b modules' own tests. Declared *here*, below everything this file's production
// half contains, because `states/mod.rs`'s two source pins read that half as "everything before
// the first `#[cfg(test)]`" — a test module declared at the top of the file would truncate it
// to nothing and make both pins vacuous.
#[cfg(test)]
mod compile_fail;
#[cfg(test)]
mod fanout_tests;
#[cfg(test)]
mod phase_tests;

#[cfg(test)]
mod tests {
    use super::{
        CheckpointInfo, RestorePreparationError, StateBackendError, StateBackendSelector,
        prepare_restored_checkpoint,
    };
    use arroyo_rpc::grpc::rpc::{
        CheckpointMetadata, GlobalKeyedTableConfig, GlobalKeyedTableTaskCheckpointMetadata,
        OperatorCheckpointMetadata, OperatorMetadata, TableCheckpointMetadata, TableConfig,
        TableEnum,
    };
    use arroyo_rpc::state_backend::validated::Validated;
    use arroyo_state::validated::{
        CheckpointMetadataWrite, CompletedCheckpoint, CompletedOperator,
    };
    use arroyo_state::{BackingStore, StateBackend, StorageProviderFor};
    use prost::Message;
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    const JOB_ID: &str = "job_1";
    const EPOCH: u32 = 4;
    /// The min epoch the checkpoint is stored with, and the one it is asked to be rewritten
    /// to. They differ so that "was the metadata rewritten?" is answerable by reading it
    /// back.
    const STORED_MIN_EPOCH: u32 = 0;
    const NEW_MIN_EPOCH: u32 = 2;

    /// A checkpoint store on the local filesystem, so the metadata rewrite this preparation
    /// performs — or does not perform — is observable as bytes that did or did not change.
    struct LocalCheckpointStore {
        role: StorageProviderFor,
        directory: String,
    }

    impl LocalCheckpointStore {
        fn new(name: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let unique = format!(
                "{}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                COUNTER.fetch_add(1, Ordering::Relaxed),
            );
            let directory = std::env::temp_dir()
                .join(format!("arroyo-scheduling-{name}-{unique}"))
                .to_string_lossy()
                .into_owned();
            std::fs::create_dir_all(&directory).unwrap();

            Self {
                role: StorageProviderFor::Controller {
                    storage_url: Some(format!("file://{directory}")),
                },
                directory,
            }
        }

        /// The min epoch currently recorded in the checkpoint's top-level metadata. This is
        /// the only thing `prepare_restored_checkpoint` rewrites, so it is how a test says
        /// whether the rewrite happened.
        async fn stored_min_epoch(&self) -> u32 {
            StateBackend::load_checkpoint_metadata(&self.role, JOB_ID, EPOCH)
                .await
                .unwrap()
                .min_epoch
        }
    }

    impl Drop for LocalCheckpointStore {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }

    fn table_config(state_backend: &str, table_type: TableEnum) -> TableConfig {
        TableConfig {
            table_type: table_type as i32,
            config: GlobalKeyedTableConfig {
                table_name: "g".to_string(),
                description: "global".to_string(),
                uses_two_phase_commit: true,
            }
            .encode_to_vec(),
            state_version: 0,
            state_backend: state_backend.to_string(),
        }
    }

    /// One operator's checkpoint metadata: a single global table whose config states
    /// `state_backend` and whose declared type is `table_type`.
    fn operator_metadata(
        operator_id: &str,
        state_backend: &str,
        table_type: TableEnum,
    ) -> OperatorCheckpointMetadata {
        OperatorCheckpointMetadata {
            operator_metadata: Some(OperatorMetadata {
                job_id: JOB_ID.to_string(),
                operator_id: operator_id.to_string(),
                epoch: EPOCH,
                min_watermark: None,
                max_watermark: None,
                parallelism: 1,
            }),
            start_time: 0,
            finish_time: 0,
            table_checkpoint_metadata: HashMap::from([(
                "g".to_string(),
                TableCheckpointMetadata {
                    table_type: TableEnum::GlobalKeyValue as i32,
                    data: GlobalKeyedTableTaskCheckpointMetadata {
                        files: vec![],
                        commit_data_by_subtask: HashMap::from([(0, b"commit".to_vec())]),
                    }
                    .encode_to_vec(),
                },
            )]),
            table_configs: HashMap::from([(
                "g".to_string(),
                table_config(state_backend, table_type),
            )]),
        }
    }

    /// Lays down a checkpoint whose operators are `operators`, in that order, and returns
    /// the store holding it.
    async fn checkpoint_with(
        name: &str,
        operators: &[(&str, &str, TableEnum)],
    ) -> LocalCheckpointStore {
        let store = LocalCheckpointStore::new(name);

        for (operator_id, state_backend, table_type) in operators {
            StateBackend::write_operator_checkpoint_metadata(
                &store.role,
                operator_metadata(operator_id, state_backend, *table_type),
            )
            .await
            .unwrap();
        }

        write_checkpoint_listing(
            &store,
            &operators
                .iter()
                .map(|(operator_id, _, _)| operator_id.to_string())
                .collect::<Vec<_>>(),
        )
        .await;

        store
    }

    /// Writes the checkpoint's top-level metadata listing `operator_ids`.
    ///
    /// The write takes a token, so the fixture stands behind those operators the same way the
    /// worker that took the checkpoint does — with what each operator's subtasks reported.
    /// Every operator here is single-subtask and has finished, which is what entitles the
    /// write; a fixture that wanted to write metadata for an unfinished operator could not.
    async fn write_checkpoint_listing(store: &LocalCheckpointStore, operator_ids: &[String]) {
        let program: HashSet<&str> = operator_ids.iter().map(String::as_str).collect();
        let completed = Validated::validate(
            CompletedCheckpoint::new(
                JOB_ID.to_string(),
                EPOCH,
                operator_ids
                    .iter()
                    .map(|operator_id| CompletedOperator::reported(operator_id.clone(), 1, [0]))
                    .collect(),
            ),
            &program,
        )
        .unwrap();
        StateBackend::write_checkpoint_metadata(
            &store.role,
            Validated::validate(
                CheckpointMetadataWrite::for_completed_checkpoint(
                    CheckpointMetadata {
                        job_id: JOB_ID.to_string(),
                        epoch: EPOCH,
                        min_epoch: STORED_MIN_EPOCH,
                        start_time: 0,
                        finish_time: 0,
                        operator_ids: operator_ids.to_vec(),
                    },
                    &completed,
                ),
                (),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    }

    fn checkpoint_info(needs_commits: bool) -> CheckpointInfo {
        CheckpointInfo {
            epoch: EPOCH as u64,
            min_epoch: NEW_MIN_EPOCH as u64,
            id: "checkpoint_1".to_string(),
            needs_commits,
        }
    }

    /// The operators the job's workers would build, each at parallelism 1 — what
    /// `LogicalProgram::tasks_per_operator` supplies in production.
    fn restoring(operator_ids: &[&str]) -> HashMap<String, usize> {
        operator_ids.iter().map(|id| (id.to_string(), 1)).collect()
    }

    fn selector_error(err: &RestorePreparationError) -> &StateBackendError {
        let RestorePreparationError::Fatal { source, .. } = err else {
            panic!("expected a fatal error, got {err:?}");
        };
        source
            .downcast_ref::<StateBackendError>()
            .unwrap_or_else(|| panic!("expected a typed selector error, got {source:?}"))
    }

    /// A `ready` checkpoint — one that needs no commit replay — is the case that reached
    /// the metadata rewrite without any operator being looked at. Here the first operator
    /// agrees with the job and the second does not, and the checkpoint must be refused
    /// with its top-level metadata untouched: the rewrite is the controller's half of
    /// "before starting workers", and the workers are only started if this returns.
    #[tokio::test]
    async fn a_ready_checkpoint_with_a_disagreeing_operator_is_refused_before_the_rewrite() {
        let store = checkpoint_with(
            "ready-mixed",
            &[
                ("node_1", "parquet", TableEnum::GlobalKeyValue),
                ("node_2", "stateengine", TableEnum::GlobalKeyValue),
            ],
        )
        .await;

        let Err(err) = prepare_restored_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            JOB_ID,
            &checkpoint_info(false),
            &restoring(&["node_1", "node_2"]),
        )
        .await
        else {
            panic!("a checkpoint with a disagreeing operator must not be prepared");
        };

        assert!(
            matches!(
                selector_error(&err),
                StateBackendError::CheckpointMismatch { .. }
            ),
            "{err:?}"
        );
        let message = selector_error(&err).to_string();
        assert!(message.contains("node_2"), "{message}");

        assert_eq!(
            store.stored_min_epoch().await,
            STORED_MIN_EPOCH,
            "the checkpoint's metadata must not have been rewritten"
        );
    }

    /// The ordering, made observable. `node_1` agrees with the job but declares a table
    /// whose type is missing — a failure only reachable by interpreting that operator's
    /// tables — while `node_2` disagrees with the job. Whichever error comes back says
    /// which operator was reached first: `Missing table type` means `node_1`'s tables were
    /// walked before `node_2` was ever looked at.
    ///
    /// Both operators do the same work up to the divergence: each is loaded, and each has
    /// one global table with commit data, so neither can fail for an unrelated reason
    /// first.
    #[tokio::test]
    async fn a_later_operators_mismatch_is_found_before_an_earlier_operators_tables_are_read() {
        let store = checkpoint_with(
            "commit-mixed",
            &[
                ("node_1", "parquet", TableEnum::MissingTableType),
                ("node_2", "stateengine", TableEnum::GlobalKeyValue),
            ],
        )
        .await;

        let Err(err) = prepare_restored_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            JOB_ID,
            &checkpoint_info(true),
            &restoring(&["node_1", "node_2"]),
        )
        .await
        else {
            panic!("a checkpoint with a disagreeing operator must not be prepared");
        };

        assert!(
            matches!(
                selector_error(&err),
                StateBackendError::CheckpointMismatch { .. }
            ),
            "expected the selector rejection rather than an earlier operator's table \
             failure, got {err:?}"
        );

        assert_eq!(
            store.stored_min_epoch().await,
            STORED_MIN_EPOCH,
            "the checkpoint's metadata must not have been rewritten"
        );
    }

    /// The compatibility direction. A multi-operator checkpoint written before the
    /// selector existed carries `""` in every table config, and it must still restore into
    /// a parquet job — both as a plain `ready` checkpoint and as one that replays commits,
    /// which is the path that consumes the operator metadata the preflight loaded.
    #[tokio::test]
    async fn a_legacy_all_parquet_multi_operator_checkpoint_still_restores() {
        let operators = [
            ("node_1", "", TableEnum::GlobalKeyValue),
            ("node_2", "", TableEnum::GlobalKeyValue),
        ];

        let store = checkpoint_with("legacy-ready", &operators).await;
        let Ok(committing) = prepare_restored_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            JOB_ID,
            &checkpoint_info(false),
            &restoring(&["node_1", "node_2"]),
        )
        .await
        else {
            panic!("a legacy checkpoint must still restore");
        };
        assert!(committing.is_none());
        assert_eq!(store.stored_min_epoch().await, NEW_MIN_EPOCH);

        let store = checkpoint_with("legacy-commit", &operators).await;
        let Ok(committing) = prepare_restored_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            JOB_ID,
            &checkpoint_info(true),
            &restoring(&["node_1", "node_2"]),
        )
        .await
        else {
            panic!("a legacy checkpoint needing commit replay must still restore");
        };
        let committing = committing.expect("commit replay should have been derived");
        assert_eq!(
            committing.committing_data().len(),
            2,
            "both operators' commit data should have been rebuilt from the preflight's \
             metadata"
        );
        assert_eq!(store.stored_min_epoch().await, NEW_MIN_EPOCH);
    }

    /// Returns the message of a fatal preparation error, or panics saying what came back
    /// instead.
    fn fatal_message(err: &RestorePreparationError) -> String {
        let RestorePreparationError::Fatal { source, .. } = err else {
            panic!("expected a fatal error, got {err:?}");
        };
        source.to_string()
    }

    /// The set the preflight checks is the workers', not the checkpoint's. `node_2` is in
    /// the job's program and its metadata disagrees with the job, but the checkpoint does
    /// not list it — so walking the checkpoint's list finds nothing wrong, rewrites the
    /// metadata, and leaves the disagreement for a worker to hit. Deriving the set from
    /// the program refuses the restore with the rewrite untouched.
    ///
    /// `node_1` is listed and complete, so `node_2` is not reached first for any other
    /// reason.
    #[tokio::test]
    async fn an_operator_the_checkpoint_omits_is_refused_before_the_rewrite() {
        let store = checkpoint_with(
            "omitted-operator",
            &[("node_1", "parquet", TableEnum::GlobalKeyValue)],
        )
        .await;
        // node_2's object exists and disagrees, but is not in the checkpoint's list
        StateBackend::write_operator_checkpoint_metadata(
            &store.role,
            operator_metadata("node_2", "stateengine", TableEnum::GlobalKeyValue),
        )
        .await
        .unwrap();

        let Err(err) = prepare_restored_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            JOB_ID,
            &checkpoint_info(false),
            &restoring(&["node_1", "node_2"]),
        )
        .await
        else {
            panic!("a checkpoint that omits one of the job's operators must not be prepared");
        };

        let message = fatal_message(&err);
        assert!(message.contains("node_2"), "{message}");

        assert_eq!(
            store.stored_min_epoch().await,
            STORED_MIN_EPOCH,
            "the checkpoint's metadata must not have been rewritten"
        );
    }

    /// A `ready` checkpoint listing an operator whose metadata object is absent. Nothing
    /// disagrees about the backend, and commit replay — the only consumer that used to
    /// reject an absent object — never runs, so this reached the rewrite and then the
    /// worker, which requires the object. It must now be refused with the rewrite
    /// untouched.
    #[tokio::test]
    async fn a_listed_operator_with_no_object_is_refused_before_the_rewrite() {
        let store = checkpoint_with(
            "absent-object",
            &[("node_1", "parquet", TableEnum::GlobalKeyValue)],
        )
        .await;
        // relist the checkpoint with an operator that has no object of its own
        write_checkpoint_listing(&store, &["node_1".to_string(), "node_2".to_string()]).await;

        let Err(err) = prepare_restored_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            JOB_ID,
            &checkpoint_info(false),
            &restoring(&["node_1", "node_2"]),
        )
        .await
        else {
            panic!("a checkpoint missing an operator's metadata object must not be prepared");
        };

        let message = fatal_message(&err);
        assert!(message.contains("node_2"), "{message}");

        assert_eq!(
            store.stored_min_epoch().await,
            STORED_MIN_EPOCH,
            "the checkpoint's metadata must not have been rewritten"
        );
    }
}
