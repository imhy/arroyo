use anyhow::bail;
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::config::config;
use arroyo_rpc::connect_grpc;
use arroyo_rpc::grpc::rpc::node_grpc_client::NodeGrpcClient;
use arroyo_rpc::grpc::rpc::{
    HeartbeatNodeReq, RegisterNodeReq, StartWorkerReq, StopWorkerReq, StopWorkerStatus,
    WorkerFinishedReq,
};
use arroyo_types::{
    GENERATION_ENV, JOB_ID_ENV, JobId, MachineId, PIPELINE_ID_ENV, PipelineId, WorkerId,
};
use futures::future::join_all;
use lazy_static::lazy_static;
use prometheus::{Gauge, register_gauge};
use std::collections::{HashMap, HashSet};
use std::env::current_exe;
use std::ffi::OsString;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::process::Command;
use tokio::sync::{Mutex, oneshot};
use tonic::transport::Channel;
use tonic::{Request, Status};
use tracing::{info, warn};

pub mod embedded;
pub mod kubernetes;

lazy_static! {
    static ref FREE_SLOTS: Gauge =
        register_gauge!("arroyo_controller_free_slots", "number of free task slots").unwrap();
    static ref REGISTERED_SLOTS: Gauge = register_gauge!(
        "arroyo_controller_registered_slots",
        "total number of registered task slots"
    )
    .unwrap();
    static ref REGISTERED_NODES: Gauge = register_gauge!(
        "arroyo_controller_registered_nodes",
        "total number of registered nodes"
    )
    .unwrap();
}

#[async_trait::async_trait]
pub trait Scheduler: Send + Sync {
    async fn start_workers(
        &self,
        start_pipeline_req: StartPipelineReq,
    ) -> Result<(), SchedulerError>;

    async fn register_node(&self, req: RegisterNodeReq);
    async fn heartbeat_node(&self, req: HeartbeatNodeReq) -> Result<(), Status>;
    async fn worker_finished(&self, req: WorkerFinishedReq);
    async fn stop_workers(
        &self,
        job_id: &str,
        generation: Option<u64>,
        force: bool,
    ) -> anyhow::Result<()>;
    async fn workers_for_job(
        &self,
        job_id: &str,
        generation: Option<u64>,
    ) -> anyhow::Result<Vec<WorkerId>>;

    /// What this scheduler can say about one worker generation's live workers (M11.T26f,
    /// design M11.D39e(v)).
    ///
    /// M11.D39e(v) allows exactly three facts to settle an issued `StartExecution`, and one of
    /// them is *observed target worker-generation termination*. The controller observes it here:
    /// a target the scheduler no longer lists among a job's live workers is a target that cannot
    /// apply what was addressed to it. That reading is only sound for a scheduler whose listing
    /// names the workers it started, by id — and not every implementation's does. An empty
    /// listing from one that keeps no registry means *"I do not know"*, and reading it as
    /// *"they are gone"* would settle every target of every job the moment it was asked, which
    /// is precisely the false settlement the whole fence exists to prevent.
    ///
    /// # Why the answer is per generation, and why the listing comes back inside it
    ///
    /// A process-local registry knows about the generations *this* scheduler value started and
    /// nothing else. A process worker outlives the controller that spawned it — `kill_on_drop` is
    /// opt-in, a `SIGKILL`ed controller never runs it, and orphaned workers are the thing
    /// recovery exists for — and a node worker outlives it by construction. So a fresh
    /// controller's empty registry means *"I do not know"* for every generation it did not
    /// launch, and reading it as *"they are gone"* settles the exact target that is still able to
    /// apply a delayed request. Asking "is this scheduler authoritative?" and "what does it list?"
    /// as two questions is what allowed those two answers to come from different states of the
    /// world; one value cannot (PR #167 round 2).
    ///
    /// It has no default. A scheduler added later must say which of the two it is, because the
    /// safe answer is not the convenient one and an omission would inherit whichever was written
    /// here.
    ///
    /// # Errors
    ///
    /// Whatever the underlying listing failed with. Not knowing is never [`Untracked`]: an
    /// `Untracked` answer is a statement about the scheduler, and an error is a statement about
    /// this attempt to ask it.
    ///
    /// [`Untracked`]: GenerationObservation::Untracked
    async fn observe_generation(
        &self,
        job_id: &str,
        generation: u64,
    ) -> anyhow::Result<GenerationObservation>;

    async fn shutdown(&self) {}
}

/// What a scheduler can say about one job generation's live workers.
///
/// One value rather than an authority flag beside a separate listing, because they are one
/// observation: "this scheduler can answer for that generation" and "here is what it answered"
/// have to come from the same state of the world, and asking them separately is what let a
/// listing taken from a registry the scheduler had never populated be read as evidence of
/// termination (PR #167 round 2).
///
/// The negative arm carries what an operator needs — which scheduler could not answer, and why —
/// because a job that will not leave `Fencing` because its deployment keeps no worker registry is
/// a very different report from one held by a partitioned worker, and a flag could not tell them
/// apart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GenerationObservation {
    /// First-hand evidence: these worker ids are live now, so a target of this generation that is
    /// **not** among them has terminated.
    Live(Vec<WorkerId>),
    /// The scheduler cannot say whether this generation's workers are live.
    Untracked {
        /// The scheduler, for the log.
        scheduler: &'static str,
        /// Why it cannot say.
        why: &'static str,
    },
}

pub struct ProcessWorker {
    pipeline_id: PipelineId,
    job_id: JobId,
    generation: u64,
    /// Taken when this worker is asked to stop, and `None` afterwards.
    ///
    /// An `Option` because asking a worker to stop no longer removes it from the registry
    /// (PR #167 round 6): a stop *request* is not an exit, and the entry is what says this
    /// worker can still act. So the sender is taken out of an entry that stays.
    shutdown_tx: Option<oneshot::Sender<()>>,
    finished_rx: oneshot::Receiver<()>,
}

/// This Scheduler starts new processes to run the worker nodes
pub struct ProcessScheduler {
    workers: Arc<Mutex<HashMap<WorkerId, ProcessWorker>>>,
    /// The `(job, generation)` pairs **this** scheduler value started.
    ///
    /// Termination evidence is scoped to it because [`Self::workers`] is: a process worker
    /// outlives the controller that spawned it — `kill_on_drop` is opt-in
    /// (`process_scheduler.shutdown_with_controller`) and a `SIGKILL`ed controller never runs it
    /// — so a controller that restarts has an empty registry and a live cluster. Absence from
    /// this set is the difference between "that generation finished" and "I never saw it".
    launched: Arc<Mutex<HashSet<(String, u64)>>>,
    worker_counter: AtomicU64,
}

impl ProcessScheduler {
    pub fn new() -> Self {
        Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            launched: Arc::new(Mutex::new(HashSet::new())),
            worker_counter: AtomicU64::new(100),
        }
    }

    /// Whether this scheduler value started `generation`'s workers, and so can read its own
    /// registry as evidence about them.
    pub(crate) async fn started_generation(&self, job_id: &str, generation: u64) -> bool {
        self.launched
            .lock()
            .await
            .contains(&(job_id.to_string(), generation))
    }
}

const PROCESS_WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

pub struct StartPipelineReq {
    pub name: String,
    pub program: LogicalProgram,
    pub wasm_path: String,
    pub pipeline_id: PipelineId,
    pub organization_id: String,
    pub job_id: JobId,
    pub hash: String,
    pub generation: u64,
    pub slots: usize,
    pub env_vars: HashMap<String, String>,
    pub pipeline_tags: HashMap<String, String>,
    /// Per-job scheduler configuration overlay as raw JSON. An empty
    /// object means "use the controller's global scheduler config
    /// unchanged". The scheduler interprets the shape; the controller
    /// treats it as opaque and passes it through verbatim.
    pub scheduler_config: serde_json::Value,
}

#[async_trait::async_trait]
impl Scheduler for ProcessScheduler {
    async fn start_workers(
        &self,
        start_pipeline_req: StartPipelineReq,
    ) -> Result<(), SchedulerError> {
        // Recorded before anything is spawned, and never removed: what it says is "this
        // controller is the one that started that generation", which stays true after its workers
        // exit and is what makes this scheduler's own registry readable as evidence about them.
        // A generation this controller never attempted has no workers it could be wrong about;
        // one it attempted and failed to spawn has none either, and the registry says so.
        self.launched.lock().await.insert((
            (*start_pipeline_req.job_id).clone(),
            start_pipeline_req.generation,
        ));

        let workers = (start_pipeline_req.slots as f32
            / config().process_scheduler.slots_per_process as f32)
            .ceil() as usize;

        let mut slots_scheduled = 0;

        let base_path = PathBuf::from_str(&format!(
            "/tmp/arroyo-process/{}",
            start_pipeline_req.job_id
        ))
        .unwrap();

        for _ in 0..workers {
            let path = base_path.clone();

            let slots_here = (start_pipeline_req.slots - slots_scheduled)
                .min(config().process_scheduler.slots_per_process as usize);

            let worker_id = self.worker_counter.fetch_add(1, Ordering::SeqCst);

            let (tx, rx) = oneshot::channel();
            let (finished_tx, finished_rx) = oneshot::channel();

            {
                let mut workers = self.workers.lock().await;
                workers.insert(
                    WorkerId(worker_id),
                    ProcessWorker {
                        pipeline_id: start_pipeline_req.pipeline_id.clone(),
                        job_id: start_pipeline_req.job_id.clone(),
                        generation: start_pipeline_req.generation,
                        shutdown_tx: Some(tx),
                        finished_rx,
                    },
                );
            }

            slots_scheduled += slots_here;
            let pipeline_id = start_pipeline_req.pipeline_id.clone();
            let job_id = start_pipeline_req.job_id.clone();
            let workers = Arc::downgrade(&self.workers);
            let env_map = start_pipeline_req.env_vars.clone();

            tokio::spawn(async move {
                let mut command =
                    Command::new(current_exe().expect("Could not get path of worker binary"));

                for (env, value) in env_map {
                    command.env(env, value);
                }

                let config = config();

                let mut args = vec![];
                if let Some(path) = &config.config_path {
                    args.push(OsString::from_str("-c").unwrap());
                    args.push(path.clone().into_os_string());
                }

                if let Some(path) = &config.config_dir {
                    args.push(OsString::from_str("--config-dir").unwrap());
                    args.push(path.clone().into_os_string());
                }

                args.push("worker".into());

                let mut child = match command
                    .args(args)
                    .env("ARROYO__ADMIN__HTTP_PORT", "0")
                    .env("ARROYO__WORKER__TASK_SLOTS", format!("{slots_here}"))
                    .env("ARROYO__WORKER__ID", format!("{worker_id}")) // start at 100 to make same length
                    .env("ARROYO__CONTROLLER_ENDPOINT", config.controller_endpoint())
                    .env("UNDER_PROCESS_SCHEDULER", "true")
                    .env(PIPELINE_ID_ENV, &*pipeline_id)
                    .env(JOB_ID_ENV, &*job_id)
                    .env(GENERATION_ENV, format!("{}", start_pipeline_req.generation))
                    .kill_on_drop(config.process_scheduler.shutdown_with_controller)
                    .spawn()
                {
                    Ok(child) => child,
                    Err(e) => {
                        warn!(
                            message = "failed to start process scheduler worker",
                            worker_id,
                            job_id = %job_id,
                            pipeline_id = %pipeline_id,
                            error = format!("{:?}", e),
                        );

                        if let Some(workers) = workers.upgrade() {
                            let mut state = workers.lock().await;
                            state.remove(&WorkerId(worker_id));
                        }
                        let _ = finished_tx.send(());
                        return;
                    }
                };

                tokio::select! {
                    status = child.wait() => {
                        info!(
                            job_id = %job_id,
                            pipeline_id = %pipeline_id,
                            "Child ({:?}) exited with status {:?}",
                            path,
                            status
                        );
                    }
                    _ = rx => {
                        if config.process_scheduler.shutdown_with_controller {
                            info!(
                                message = "Killing child",
                                worker_id,
                                job_id = %job_id,
                                pipeline_id = *pipeline_id
                            );
                            if let Err(e) = child.kill().await {
                                warn!(
                                    message = "failed to kill process scheduler worker",
                                    worker_id,
                                    job_id = %job_id,
                                    pipeline_id = %pipeline_id,
                                    error = format!("{:?}", e),
                                );
                            }
                        }
                        // Waited for whether or not this deployment kills its workers, because
                        // the registry entry below is read as evidence that this worker can no
                        // longer act (PR #167 round 6). A kill that succeeded has already
                        // reaped it and this returns at once; a kill that failed, or one this
                        // deployment opted out of, leaves a child that is still running — and a
                        // worker that is still running must stay listed however long it takes.
                        let status = child.wait().await;
                        info!(
                            job_id = %job_id,
                            pipeline_id = %pipeline_id,
                            worker_id,
                            "child exited with status {:?} after being asked to stop",
                            status
                        );
                    }
                }

                // Only ever after an observed exit: this removal is what
                // `observe_generation` reports as an authoritative termination.
                if let Some(workers) = workers.upgrade() {
                    let mut state = workers.lock().await;
                    state.remove(&WorkerId(worker_id));
                }
                let _ = finished_tx.send(());
            });
        }

        Ok(())
    }

    async fn register_node(&self, _: RegisterNodeReq) {}
    async fn heartbeat_node(&self, _: HeartbeatNodeReq) -> Result<(), Status> {
        Ok(())
    }
    async fn worker_finished(&self, _: WorkerFinishedReq) {}

    async fn workers_for_job(
        &self,
        job_id: &str,
        run_id: Option<u64>,
    ) -> anyhow::Result<Vec<WorkerId>> {
        Ok(self
            .workers
            .lock()
            .await
            .iter()
            .filter(|(_, w)| {
                *w.job_id == job_id && (run_id.is_none() || w.generation == run_id.unwrap())
            })
            .map(|(k, _)| *k)
            .collect())
    }

    /// First-hand for the generations this value started, and nothing else.
    ///
    /// Inside one controller incarnation the registry is exact: this scheduler owns the worker
    /// *processes*, keys them by [`WorkerId`], and removes an entry when its process is finished.
    /// Across a restart it is empty while the cluster is not, which is why the answer is gated on
    /// [`ProcessScheduler::started_generation`] rather than given for the type.
    async fn observe_generation(
        &self,
        job_id: &str,
        generation: u64,
    ) -> anyhow::Result<GenerationObservation> {
        if !self.started_generation(job_id, generation).await {
            return Ok(GenerationObservation::Untracked {
                scheduler: "process",
                why: "this controller did not start that worker generation, and a process worker \
                      outlives the controller that did, so an empty registry is not evidence that \
                      the generation has terminated",
            });
        }
        Ok(GenerationObservation::Live(
            self.workers_for_job(job_id, Some(generation)).await?,
        ))
    }

    /// Asks every worker of this job generation to stop, and keeps them in the registry.
    ///
    /// **The request is not the exit** (PR #167 round 6). This used to remove each entry before
    /// sending the shutdown, and the spawned task removed it again on receiving one whether or
    /// not it killed anything — under `process_scheduler.shutdown_with_controller = false` it
    /// kills nothing at all. The registry therefore reported a worker gone the instant a stop was
    /// *asked for*, and [`Self::observe_generation`] handed that absence to the fencing path as
    /// authoritative termination, releasing an obligation while the worker could still act.
    ///
    /// An entry now leaves only where an exit is observed: the task that owns the child removes
    /// it after `wait` returns. A worker that ignores its shutdown, or one this deployment has
    /// opted out of killing, stays listed — which is the honest answer and the fail-closed one.
    async fn stop_workers(
        &self,
        job_id: &str,
        run_id: Option<u64>,
        _force: bool,
    ) -> anyhow::Result<()> {
        for worker_id in self.workers_for_job(job_id, run_id).await? {
            let shutdown = {
                let mut state = self.workers.lock().await;
                state
                    .get_mut(&worker_id)
                    .and_then(|worker| worker.shutdown_tx.take())
            };
            // A worker with no sender left has already been asked; asking twice is not an error,
            // and neither is one that finished between the listing and here.
            if let Some(shutdown) = shutdown {
                let _ = shutdown.send(());
            }
        }

        Ok(())
    }

    async fn shutdown(&self) {
        let workers: Vec<_> = self.workers.lock().await.drain().collect();

        if workers.is_empty() || !config().process_scheduler.shutdown_with_controller {
            return;
        }

        let worker_count = workers.len();
        info!(
            message = "shutting down process scheduler workers",
            workers = worker_count,
        );

        let waiters = workers
            .into_iter()
            .map(|(worker_id, mut worker)| async move {
                let job_id = worker.job_id.clone();
                let pipeline_id = worker.pipeline_id.clone();
                // `None` for a worker a `stop_workers` has already asked; its task is already on its
                // way out, and this still waits for the exit below.
                if let Some(shutdown) = worker.shutdown_tx.take() {
                    let _ = shutdown.send(());
                }
                (worker_id, job_id, pipeline_id, worker.finished_rx.await)
            });

        match tokio::time::timeout(PROCESS_WORKER_SHUTDOWN_TIMEOUT, join_all(waiters)).await {
            Ok(results) => {
                for (worker_id, job_id, pipeline_id, result) in results {
                    if result.is_err() {
                        warn!(
                            message = "process scheduler worker exited without completion signal",
                            worker_id = worker_id.0,
                            job_id = %job_id,
                            pipeline_id = *pipeline_id,
                        );
                    }
                }
            }
            Err(_) => {
                warn!(
                    message = "timed out waiting for process scheduler workers to stop",
                    workers = worker_count,
                    timeout_secs = PROCESS_WORKER_SHUTDOWN_TIMEOUT.as_secs(),
                );
            }
        }
    }
}

/// A "manual" scheduler that relies on the user to manage worker processes by executing commands
/// printed out to the terminal. This is mostly useful for testing scheduling behavior.
pub struct ManualScheduler {}

impl ManualScheduler {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Scheduler for ManualScheduler {
    async fn start_workers(
        &self,
        start_pipeline_req: StartPipelineReq,
    ) -> Result<(), SchedulerError> {
        let config = config();
        let slots_per_process = config.manual_scheduler.slots_per_process as usize;

        let workers = (start_pipeline_req.slots as f32 / slots_per_process as f32).ceil() as usize;

        let exe = current_exe().map_err(|e| {
            SchedulerError::Other(format!("Could not get path of worker binary: {e:?}"))
        })?;

        let mut slots_scheduled = 0;

        for _ in 0..workers {
            let slots_here = (start_pipeline_req.slots - slots_scheduled).min(slots_per_process);
            slots_scheduled += slots_here;

            let mut envs: Vec<(String, String)> = vec![
                ("ARROYO__ADMIN__HTTP_PORT".to_string(), "0".to_string()),
                (
                    "ARROYO__WORKER__TASK_SLOTS".to_string(),
                    slots_here.to_string(),
                ),
                (
                    "ARROYO__CONTROLLER_ENDPOINT".to_string(),
                    config.controller_endpoint(),
                ),
                (
                    PIPELINE_ID_ENV.to_string(),
                    start_pipeline_req.pipeline_id.to_string(),
                ),
                (
                    JOB_ID_ENV.to_string(),
                    start_pipeline_req.job_id.to_string(),
                ),
                (
                    GENERATION_ENV.to_string(),
                    start_pipeline_req.generation.to_string(),
                ),
            ];

            for (env, value) in &start_pipeline_req.env_vars {
                envs.push((env.clone(), value.clone()));
            }

            // Build the command-line arguments, mirroring the process scheduler.
            let mut args: Vec<String> = vec![];
            if let Some(path) = &config.config_path {
                args.push("-c".to_string());
                args.push(path.to_string_lossy().to_string());
            }
            if let Some(path) = &config.config_dir {
                args.push("--config-dir".to_string());
                args.push(path.to_string_lossy().to_string());
            }
            args.push("worker".to_string());

            // Assemble the copy-pasteable command line.
            let mut cmdline = String::new();
            for (env, value) in &envs {
                cmdline.push_str(&format!("{}={} ", env, value));
            }
            cmdline.push_str(&exe.to_string_lossy());
            for arg in &args {
                cmdline.push(' ');
                cmdline.push_str(arg);
            }

            println!();
            println!(
                "════════════════════════════════════════════════════════════════════════════════"
            );
            println!(
                "[manual scheduler] Run worker {} of {} (worker id {} slots) for job {} in a \
                 separate terminal:",
                slots_scheduled / slots_per_process.max(1),
                workers,
                slots_here,
                start_pipeline_req.job_id,
            );
            println!();
            println!("{cmdline}");
            println!(
                "════════════════════════════════════════════════════════════════════════════════"
            );
            println!();
        }

        Ok(())
    }

    async fn register_node(&self, _: RegisterNodeReq) {}
    async fn heartbeat_node(&self, _: HeartbeatNodeReq) -> Result<(), Status> {
        Ok(())
    }
    async fn worker_finished(&self, _: WorkerFinishedReq) {}

    async fn workers_for_job(
        &self,
        _job_id: &str,
        _run_id: Option<u64>,
    ) -> anyhow::Result<Vec<WorkerId>> {
        Ok(vec![])
    }

    /// Untracked, for every generation. This scheduler's workers are started by a person in
    /// another terminal and it keeps no registry of them at all, so its empty listing is not a
    /// statement about anything.
    async fn observe_generation(
        &self,
        _job_id: &str,
        _generation: u64,
    ) -> anyhow::Result<GenerationObservation> {
        Ok(GenerationObservation::Untracked {
            scheduler: "manual",
            why: "its workers are started by an operator and it keeps no registry of them, so \
                  an empty listing says nothing about whether a generation has terminated",
        })
    }

    async fn stop_workers(
        &self,
        job_id: &str,
        _run_id: Option<u64>,
        _force: bool,
    ) -> anyhow::Result<()> {
        println!("[manual scheduler] Stop workers for job {}", job_id);

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct NodeStatus {
    id: MachineId,
    free_slots: usize,
    scheduled_slots: HashMap<WorkerId, usize>,
    addr: String,
    last_heartbeat: Instant,
}

impl NodeStatus {
    fn new(id: MachineId, slots: usize, addr: String) -> NodeStatus {
        FREE_SLOTS.add(slots as f64);
        REGISTERED_SLOTS.add(slots as f64);

        NodeStatus {
            id,
            free_slots: slots,
            scheduled_slots: HashMap::new(),
            addr,
            last_heartbeat: Instant::now(),
        }
    }

    fn take_slots(&mut self, worker: WorkerId, slots: usize) {
        if let Some(v) = self.free_slots.checked_sub(slots) {
            FREE_SLOTS.sub(slots as f64);
            self.free_slots = v;
            self.scheduled_slots.insert(worker, slots);
        } else {
            panic!(
                "Attempted to schedule more slots than are available on node {} ({} < {})",
                self.addr, self.free_slots, slots
            );
        }
    }

    fn release_slots(&mut self, worker_id: WorkerId, slots: usize) {
        if let Some(freed) = self.scheduled_slots.remove(&worker_id) {
            assert_eq!(
                freed, slots,
                "Controller and node disagree about how many slots are scheduled for worker {worker_id:?} ({freed} != {slots})"
            );

            self.free_slots += slots;

            FREE_SLOTS.add(slots as f64);
        } else {
            warn!(
                "Received release request for unknown worker {:?}",
                worker_id
            );
        }
    }
}

#[derive(Clone)]
struct NodeWorker {
    pipeline_id: PipelineId,
    job_id: JobId,
    node_id: MachineId,
    generation: u64,
    running: bool,
}

#[derive(Default)]
pub struct NodeSchedulerState {
    nodes: HashMap<MachineId, NodeStatus>,
    workers: HashMap<WorkerId, NodeWorker>,
    /// The `(job, generation)` pairs **this** scheduler value scheduled onto nodes.
    ///
    /// A node worker outlives the controller that placed it by construction — it runs on another
    /// machine — and a restarted controller's [`Self::workers`] is empty because nodes re-register
    /// themselves and not the workers they are already running. So absence from this set is the
    /// difference between "that generation finished" and "I never placed it".
    launched: HashSet<(String, u64)>,
}

impl NodeSchedulerState {
    fn expire_nodes(&mut self, expiration_time: Instant) {
        let expired_nodes: Vec<_> = self
            .nodes
            .iter()
            .filter_map(|(node_id, status)| {
                if status.last_heartbeat >= expiration_time {
                    None
                } else {
                    Some(node_id.clone())
                }
            })
            .collect();
        for node_id in expired_nodes {
            warn!("expiring node {:?} from scheduler state", node_id);
            self.nodes.remove(&node_id);
        }
    }
}

pub struct NodeScheduler {
    state: Arc<Mutex<NodeSchedulerState>>,
}

#[derive(Debug)]
pub enum SchedulerError {
    NotEnoughSlots {
        slots_needed: usize,
    },
    Other(String),
    /// Non-retryable scheduler failure. The state machine treats
    /// this as fatal and transitions the job to `Failed` without retrying.
    Fatal(String),
}

pub fn is_empty_overlay(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Null => true,
        serde_json::Value::Object(m) => m.is_empty(),
        _ => false,
    }
}

impl NodeScheduler {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(NodeSchedulerState::default())),
        }
    }

    async fn client(node: &NodeStatus) -> anyhow::Result<NodeGrpcClient<Channel>> {
        let channel = connect_grpc(
            "controller",
            format!("http://{}", node.addr),
            &config().controller.tls,
            &config().node.tls,
            None,
        )
        .await?;
        Ok(NodeGrpcClient::new(channel))
    }

    async fn stop_worker(
        &self,
        job_id: &str,
        worker_id: WorkerId,
        force: bool,
    ) -> anyhow::Result<Option<WorkerId>> {
        let state = self.state.lock().await;

        let Some(worker) = state.workers.get(&worker_id) else {
            // assume it's already finished
            return Ok(Some(worker_id));
        };

        let Some(node) = state.nodes.get(&worker.node_id) else {
            warn!(
                message = "node not found for stop worker",
                node_id = *worker.node_id.0,
                job_id = %worker.job_id,
                pipeline_id = *worker.pipeline_id
            );
            return Ok(Some(worker_id));
        };

        let worker = worker.clone();
        let node = node.clone();
        drop(state);

        info!(
            message = "stopping worker",
            job_id = %worker.job_id,
            pipeline_id = *worker.pipeline_id,
            node_id = *worker.node_id.0,
            node_addr = node.addr,
            worker_id = worker_id.0
        );

        let Ok(mut client) = Self::client(&node).await else {
            warn!(
                job_id = %worker.job_id,
                pipeline_id = *worker.pipeline_id,
                "Failed to connect to worker to stop; this likely means it is dead"
            );
            return Ok(Some(worker_id));
        };

        let Ok(resp) = client
            .stop_worker(Request::new(StopWorkerReq {
                job_id: job_id.to_string(),
                worker_id: worker_id.0,
                force,
            }))
            .await
        else {
            warn!(
                job_id = %worker.job_id,
                pipeline_id = *worker.pipeline_id,
                "Failed to connect to worker to stop; this likely means it is dead"
            );
            return Ok(Some(worker_id));
        };

        match (resp.get_ref().status(), force) {
            (StopWorkerStatus::NotFound, false) => {
                bail!("couldn't find worker, will only continue if force")
            }
            (StopWorkerStatus::StopFailed, _) => bail!("tried to kill and couldn't"),
            _ => Ok(None),
        }
    }
}

#[async_trait::async_trait]
impl Scheduler for NodeScheduler {
    async fn register_node(&self, req: RegisterNodeReq) {
        let mut state = self.state.lock().await;
        if let std::collections::hash_map::Entry::Vacant(e) =
            state.nodes.entry(MachineId(req.machine_id.clone().into()))
        {
            e.insert(NodeStatus::new(
                MachineId(req.machine_id.into()),
                req.task_slots as usize,
                req.addr,
            ));
        }
    }

    async fn heartbeat_node(&self, req: HeartbeatNodeReq) -> Result<(), Status> {
        let mut state = self.state.lock().await;
        if let Some(node) = state
            .nodes
            .get_mut(&MachineId(req.machine_id.clone().into()))
        {
            node.last_heartbeat = Instant::now();
            Ok(())
        } else {
            warn!(
                "Received heartbeat for unregistered node {}, failing request",
                req.machine_id
            );
            Err(Status::not_found(format!(
                "node {} not in scheduler's collection of nodes",
                req.machine_id
            )))
        }
    }

    async fn worker_finished(&self, req: WorkerFinishedReq) {
        let mut state = self.state.lock().await;
        let Some(worker_context) = req.worker_context else {
            warn!("Got worker finished with no worker context");
            return;
        };

        let worker_id = WorkerId(worker_context.worker_id);
        let job_id = worker_context.job_id;
        let pipeline_id = worker_context.pipeline_id;
        let machine_id = MachineId(Arc::new(worker_context.machine_id));

        if let Some(node) = state.nodes.get_mut(&machine_id) {
            node.release_slots(worker_id, req.slots as usize);
        } else {
            warn!(
                %job_id,
                pipeline_id, "Got worker finished message for unknown node {}", machine_id
            );
        }

        if state.workers.remove(&worker_id).is_none() {
            warn!(
                %job_id,
                pipeline_id, "Got worker finished message for unknown worker {}", worker_id.0
            );
        }
    }

    /// First-hand for the generations this value placed, and nothing else.
    ///
    /// Two things separate this listing from [`Self::workers_for_job`], and both are the same
    /// mistake read from different ends. The generation gate is the controller-restart half: a
    /// fresh controller's registry is empty while the nodes are still running the cluster. The
    /// `running` flag is the partition half: `stop_worker` sets it false when it *could not reach*
    /// a worker, which is the opposite of evidence that the worker is gone — it names precisely
    /// the worker that may still apply a delayed request — so this listing ignores it and only a
    /// `worker_finished` report, which removes the entry outright, settles anything.
    async fn observe_generation(
        &self,
        job_id: &str,
        generation: u64,
    ) -> anyhow::Result<GenerationObservation> {
        let state = self.state.lock().await;
        if !state.launched.contains(&(job_id.to_string(), generation)) {
            return Ok(GenerationObservation::Untracked {
                scheduler: "node",
                why: "this controller did not place that worker generation on any node, and a \
                      node worker outlives the controller that did, so an empty registry is not \
                      evidence that the generation has terminated",
            });
        }
        Ok(GenerationObservation::Live(
            state
                .workers
                .iter()
                .filter(|(_, w)| *w.job_id == job_id && w.generation == generation)
                .map(|(id, _)| *id)
                .collect(),
        ))
    }

    async fn workers_for_job(
        &self,
        job_id: &str,
        run_id: Option<u64>,
    ) -> anyhow::Result<Vec<WorkerId>> {
        let state = self.state.lock().await;
        Ok(state
            .workers
            .iter()
            .filter(|(_, v)| {
                *v.job_id == job_id
                    && v.running
                    && (run_id.is_none() || v.generation == run_id.unwrap())
            })
            .map(|(w, _)| *w)
            .collect())
    }

    #[allow(unreachable_code, unused)]
    async fn start_workers(
        &self,
        start_pipeline_req: StartPipelineReq,
    ) -> Result<(), SchedulerError> {
        // TODO: make this locking more fine-grained
        let mut state = self.state.lock().await;

        // Recorded before any placement, and never removed: it says this controller is the one
        // that placed that generation, which stays true after its workers exit and is what makes
        // this registry readable as evidence about them. A generation this controller never
        // attempted has no workers here it could be wrong about.
        state.launched.insert((
            (*start_pipeline_req.job_id).clone(),
            start_pipeline_req.generation,
        ));

        state.expire_nodes(Instant::now() - Duration::from_secs(30));

        let free_slots = state.nodes.values().map(|n| n.free_slots).sum::<usize>();
        let slots = start_pipeline_req.slots;
        if slots > free_slots {
            return Err(SchedulerError::NotEnoughSlots {
                slots_needed: slots - free_slots,
            });
        }

        let mut to_schedule = slots;
        let mut slots_assigned = vec![];
        while to_schedule > 0 {
            // find the node with the most free slots and fill it
            let node = {
                if let Some(status) = state
                    .nodes
                    .values()
                    .filter(|n| {
                        n.free_slots > 0 && n.last_heartbeat.elapsed() < Duration::from_secs(30)
                    })
                    .max_by_key(|n| n.free_slots)
                    .cloned()
                {
                    status
                } else {
                    unreachable!();
                }
            };

            let slots_for_this_one = node.free_slots.min(to_schedule);
            info!(
                job_id = %start_pipeline_req.job_id,
                pipeline_id = *start_pipeline_req.pipeline_id,
                "Scheduling {} slots on node {}",
                slots_for_this_one,
                node.addr
            );

            let mut client = Self::client(&node)
                .await
                // TODO: handle this issue more gracefully by moving trying other nodes
                .map_err(|e| {
                    // release back slots already scheduled.
                    slots_assigned
                        .iter()
                        .for_each(|(node_id, worker_id, slots)| {
                            state
                                .nodes
                                .get_mut(node_id)
                                .unwrap()
                                .release_slots(*worker_id, *slots);
                        });
                    SchedulerError::Other(format!(
                        "Failed to connect to node {}: {:?}",
                        node.addr, e
                    ))
                })?;

            let req = StartWorkerReq {
                name: start_pipeline_req.name.clone(),
                pipeline_id: (*start_pipeline_req.pipeline_id).clone(),
                job_id: (*start_pipeline_req.job_id).clone(),
                slots: slots_for_this_one as u64,
                machine_id: node.id.to_string(),
                generation: start_pipeline_req.generation,
                env_vars: start_pipeline_req.env_vars.clone(),
            };

            let res = client
                .start_worker(Request::new(req))
                .await
                .map_err(|e| {
                    // release back slots already scheduled.
                    slots_assigned
                        .iter()
                        .for_each(|(node_id, worker_id, slots)| {
                            state
                                .nodes
                                .get_mut(node_id)
                                .unwrap()
                                .release_slots(*worker_id, *slots);
                        });
                    SchedulerError::Other(format!(
                        "Failed to start worker on node {}: {:?}",
                        node.addr, e
                    ))
                })?
                .into_inner();

            state
                .nodes
                .get_mut(&node.id)
                .unwrap()
                .take_slots(WorkerId(res.worker_id), slots_for_this_one);

            state.workers.insert(
                WorkerId(res.worker_id),
                NodeWorker {
                    pipeline_id: start_pipeline_req.pipeline_id.clone(),
                    job_id: start_pipeline_req.job_id.clone(),
                    generation: start_pipeline_req.generation,
                    node_id: node.id.clone(),
                    running: true,
                },
            );
            slots_assigned.push((node.id, WorkerId(res.worker_id), slots_for_this_one));

            to_schedule -= slots_for_this_one;
        }
        Ok(())
    }

    async fn stop_workers(
        &self,
        job_id: &str,
        run_id: Option<u64>,
        force: bool,
    ) -> anyhow::Result<()> {
        // iterate through all of the workers from workers_for_job and stop them in parallel
        let workers = self.workers_for_job(job_id, run_id).await?;
        let mut futures = vec![];
        for worker_id in workers {
            futures.push(self.stop_worker(job_id, worker_id, force));
        }

        for f in futures {
            match f.await? {
                Some(worker_id) => {
                    let mut state = self.state.lock().await;
                    if let Some(worker) = state.workers.get_mut(&worker_id) {
                        worker.running = false;
                    }
                }
                None => {
                    bail!("Failed to stop worker");
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod reporting_tests;
