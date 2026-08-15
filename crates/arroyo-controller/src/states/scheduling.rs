use arroyo_rpc::grpc::rpc::{JobState, StartExecutionReq, TaskAssignment};
use arroyo_rpc::identity::{WorkerClient, worker_client};
use arroyo_types::{CLUSTER_ID_ENV, JobId, MachineId, WorkerId};
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
    JobContext, State, Transition, check_config_update, leader_running::LeaderRunning,
    running::Running,
};
use crate::job_controller::checkpoint_store::DbCheckpointMetadataStore;
use crate::job_controller::leader_manager::LeaderManager;
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
use arroyo_rpc::state_backend::{
    StateBackendError, StateBackendSelector, validate_restored_checkpoint,
};
use arroyo_rpc::worker_types::RunningMessage;
use arroyo_rpc::{LeaderContext, grpc_channel_builder};
use arroyo_state::{
    BackingStore, StateBackend, StorageProviderFor, get_storage_provider,
    tables::{ErasedTable, global_keyed_map::GlobalKeyedTable},
};
use arroyo_state_protocol::types::Generation;
use arroyo_state_protocol::workflow::{
    GenerationInitialization, GenerationRecovery, InitializeGenerationRequest,
    initialize_generation,
};
use arroyo_worker::job_controller::committing_state::{CheckpointIdOrRef, CommittingState};
use arroyo_worker::job_controller::job_metrics::JobMetrics;

#[derive(Debug, Clone)]
struct WorkerStatus {
    id: WorkerId,
    machine_id: MachineId,
    rpc_address: String,
    data_address: String,
    slots: usize,
    state: WorkerState,
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
    let operators =
        StateBackend::load_checkpoint_operators(storage_role, job, &expected, &metadata)
            .await
            .map_err(|err| match err {
                StateStoreError::StateBackendError(e) => RestorePreparationError::Fatal {
                    message: "cannot restore a checkpoint written with a different state backend"
                        .to_string(),
                    source: e.into(),
                },
                // Not retryable: the same checkpoint, and the same program, are read again on
                // every attempt.
                err @ StateStoreError::IncompleteCheckpoint { .. } => {
                    RestorePreparationError::Fatal {
                        message:
                            "cannot restore a checkpoint that does not cover the job's operators"
                                .to_string(),
                        source: err.into(),
                    }
                }
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
        for (operator_id, operator_metadata) in &operators {
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

    StateBackend::write_checkpoint_metadata(storage_role, metadata)
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

        let cluster_id = arroyo_server_common::get_cluster_id();
        env_vars
            .insert(CLUSTER_ID_ENV.to_string(), cluster_id)
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
        "Scheduling"
    }

    async fn next(mut self: Box<Self>, ctx: &mut JobContext) -> Result<Transition, StateError> {
        // if we've started in scheduling but the job isn't supposed to be running, don't try
        // to schedule
        stop_if_desired_non_running!(self, &ctx.config);

        // update the generation for this scheduling attempt
        ctx.status.generation += 1;
        if let Err(e) = ctx.status.update_db(&ctx.db).await {
            return Err(ctx.retryable(
                self,
                "failed to advance generation for scheduling retry",
                anyhow!("{}", e),
                10,
            ));
        }

        // clear out any existing workers for this job
        if let Err(e) = ctx.scheduler.stop_workers(&ctx.config.id, None, true).await {
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
        self = self.start_workers(ctx, slots_needed).await?;

        let leader_mode = matches!(config().job_controller, JobControllerMode::Worker);

        let (self, checkpoint_info, committing_state) = if leader_mode {
            match get_and_register_checkpoint_info_leader(ctx).await {
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
            get_checkpoint_info_legacy(self, ctx).await?
        };

        // wait for them to connect and make outbound RPC connections
        let mut workers = HashMap::new();
        let worker_connects = Arc::new(Mutex::new(HashMap::new()));
        let mut handles = vec![];

        let pipeline_config = &config().pipeline;

        let start = Instant::now();
        loop {
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

        let tasks: Vec<_> = worker_connects
            .into_iter()
            .map(|(id, mut c)| {
                let assignments = assignments.clone();
                let job_id = ctx.config.id.clone();
                let pipeline_id = ctx.pipeline_info.pipeline_id.clone();
                let restore_epoch = checkpoint_info.as_ref().map(|info| info.epoch);
                let program = program.clone();
                let machine_id = workers.get(&id).as_ref().unwrap().machine_id.clone();
                let (leader_id, leader_addr) = leader_info.clone().unzip();

                let checkpoint_manifest_ref =
                    leader_id.and(checkpoint_info.as_ref().map(|ci| ci.id.clone()));
                tokio::spawn(async move {
                    info!(
                        message = "starting execution on worker",
                        job_id = %job_id,
                        pipeline_id = *pipeline_id,
                        worker_id = id.0,
                        machine_id = *machine_id.0,
                    );

                    match c
                        .start_execution(Request::new(StartExecutionReq {
                            restore_epoch,
                            start_epoch,
                            min_epoch,
                            program: Some(program.clone()),
                            tasks: assignments.clone(),
                            job_controller_addr: leader_addr,
                            is_leader: leader_id.is_some_and(|l| l == id),
                            wait_for_leader: leader_id.is_some(),
                            checkpoint_interval_micros,
                            checkpoint_manifest_ref: checkpoint_manifest_ref.clone(),
                            state_backend: state_backend.as_str().to_string(),
                        }))
                        .await
                    {
                        Ok(_) => {
                            debug!(
                                message = "worker entered initialization phase",
                                job_id = %job_id,
                                pipeline_id = *pipeline_id,
                                worker_id = id.0,
                                machine_id = *machine_id.0,
                            );
                            (id, c)
                        }
                        Err(e) => {
                            error!(
                                message = "failed to start execution on worker",
                                job_id = %job_id,
                                pipeline_id = *pipeline_id,
                                worker_id = id.0,
                                machine_id = *machine_id.0,
                                error = format!("{:?}", e),
                            );
                            panic!("Failed to start execution on worker {id:?}: {e}");
                        }
                    }
                })
            })
            .collect();

        let mut worker_connects = HashMap::new();
        for t in tasks {
            match t.await {
                Ok((id, c)) => {
                    if let Some(worker) = workers.get_mut(&id) {
                        worker.state = WorkerState::Initializing;
                    }
                    worker_connects.insert(id, c);
                }
                Err(e) => {
                    return Err(ctx.retryable(self, "failed to initialize workers", e.into(), 10));
                }
            }
        }

        // Now wait until all tasks are running
        let start = Instant::now();
        let mut started_tasks = HashSet::new();
        while started_tasks.len() < ctx.program.task_count() {
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
                );
                if needs_commit {
                    info!(
                        job_id = %ctx.config.id,
                        pipeline_id = *ctx.pipeline_info.pipeline_id,
                        "restored checkpoint was in committing phase, sending commits"
                    );
                    controller
                        .send_commit_messages()
                        .await
                        .expect("failed to send commit messages");
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
    use arroyo_state::{BackingStore, StateBackend, StorageProviderFor};
    use prost::Message;
    use std::collections::HashMap;
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

        StateBackend::write_checkpoint_metadata(
            &store.role,
            CheckpointMetadata {
                job_id: JOB_ID.to_string(),
                epoch: EPOCH,
                min_epoch: STORED_MIN_EPOCH,
                start_time: 0,
                finish_time: 0,
                operator_ids: operators
                    .iter()
                    .map(|(operator_id, _, _)| operator_id.to_string())
                    .collect(),
            },
        )
        .await
        .unwrap();

        store
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
        StateBackend::write_checkpoint_metadata(
            &store.role,
            CheckpointMetadata {
                job_id: JOB_ID.to_string(),
                epoch: EPOCH,
                min_epoch: STORED_MIN_EPOCH,
                start_time: 0,
                finish_time: 0,
                operator_ids: vec!["node_1".to_string(), "node_2".to_string()],
            },
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
