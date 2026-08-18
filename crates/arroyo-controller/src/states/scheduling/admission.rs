//! What an admitted region of scheduling *means* (M11.T25b, design M11.D39b).
//!
//! [`super::phases`] says who may do what; this module says what doing it is. The split is
//! not decorative. Everything here needs the controller's world — the database, the
//! scheduler, `tonic` channels, the job's message channel — while the phase graph next door
//! needs none of it, and that is what lets the compile-time restrictions of M11.D39b be
//! checked against the phase graph's own source by a plain `rustc` (see
//! `super::compile_fail`).
//!
//! [`PhaseContext`] is the phase graph's *only* access to a [`JobContext`]. A phase owns one
//! and hands it to its successor, so the job's channel, its status row and its scheduler are
//! reachable exactly through the methods the phase that holds them chooses to expose. In
//! particular [`PhaseContext::await_message_from_workers`] and
//! [`PhaseContext::await_message_from_tasks`] — the only wrappers around `ctx.rx.recv` in
//! this half — are reachable only from the token-free phases, because they are the only
//! types that offer a method leading to one.
//!
//! [`execution`] is a child module rather than a sibling for one reason: the two interruptible
//! waits and the handover need [`PhaseContext`]'s own fields, and a sibling would have forced
//! those open to the whole of `states::scheduling` — including [`super::phases`], whose entire
//! claim is that it reaches the job only through the methods a phase exposes.
//!
//! Nothing here is selected in production: M11.T25 keeps
//! [`LifecycleMode::SELECTED`](crate::states::lifecycle::LifecycleMode::SELECTED) on the
//! landed M11.T08 path, whose `Scheduling::next` is untouched and remains the only body a
//! production job runs.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::config::{JobControllerMode, config};
use arroyo_rpc::identity::WorkerClient;
use arroyo_rpc::state_backend::StateBackendError;
use arroyo_types::WorkerId;
use arroyo_worker::job_controller::committing_state::CommittingState;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::warn;

use super::fanout::IssuedAttempts;
use super::fencing::{FenceTargets, Fencing, Interrupted};
use super::{CheckpointInfo, Scheduling, WorkerState, WorkerStatus};
use crate::JobConfig;
use crate::job_controller::JobController;
use crate::states::lifecycle::ConsumptionPoint;
use crate::states::{
    Admission, JobContext, StateError, Transition, fatal, stop_if_desired_non_running,
};

pub(super) mod execution;

/// What one turn of a token-free wait produced.
///
/// A wait is a loop, and the two things that can end it early are a stop request and a
/// message that decides the job's fate; everything else is another turn. Returning the
/// decision rather than performing it keeps the loop in [`super::phases`], where the typestate
/// can see it, instead of hidden in a helper that could quietly do something irreversible.
pub(crate) enum PhaseWait {
    /// Nothing decisive happened; the phase should look at its own counters and, if they are
    /// not yet satisfied, wait again.
    Continue,
    /// The job leaves `Scheduling` for this state without reaching the next region.
    Leave(Transition),
}

/// What crossing into a region of irreversible work produced.
///
/// A crossing has three outcomes, not two, and the third is the one M11.D39a's consumption
/// points exist to make reachable: the job's single writer may have decided, since the last
/// look, that the job *stops*. A stop is not a failure — nothing is wrong, and the job must
/// end where a stop ends rather than where an error does — so it cannot be an `Err`; and it is
/// emphatically not a success to be carried into the region, because the regions are the
/// `StartExecution` fan-out and the publication of a restored checkpoint's commits.
///
/// So the admission itself is what a crossing has to *not* return when the job is stopping.
/// Leaving carries no token, which is the structural half of the claim: there is no value in
/// this enum from which a stopping job could perform an irreversible effect.
pub(crate) enum Admitted {
    /// The region was entered. Its authority is the job's publication lock, held until this
    /// value is dropped.
    Region(Admission),
    /// The job's writer decided it stops. Nothing was admitted, and the region's first effect
    /// has not run.
    Leave(Transition),
}

/// Everything the phase graph is allowed to do to a job, and nothing else.
///
/// # Why the borrow is owned rather than passed
///
/// A [`JobContext`] has a public `rx`, so any code holding one can wait on the job's channel.
/// The M11.D39b restriction — "`ctx.rx.recv` waits exist only on token-free types" — is
/// therefore a statement about *who holds the context*, not about what a free function could
/// be written to do. So the phases own this, and it owns the borrow: a
/// [`Preamble`](super::phases::Preamble) that wanted to receive would first have to be given a
/// method that reaches [`Self::await_message_from_workers`], and adding one is exactly the
/// change the `token_owning_phase_cannot_recv` fixtures show to be load-bearing.
///
/// # What it accumulates
///
/// A scheduling attempt is one long fold: workers register, channels open, tasks start. The
/// intermediate values belong to the attempt rather than to any one phase — the fan-out needs
/// the workers the first wait collected — so they live here and travel with the borrow as each
/// phase hands it on.
pub(crate) struct PhaseContext<'a, 'ctx> {
    ctx: &'a mut JobContext<'ctx>,
    /// Whether this controller runs the job's own controller on a worker.
    leader_mode: bool,
    /// Slots the program needs, computed once the parallelism overrides are applied.
    slots_needed: usize,
    /// The workers that have registered for this scheduling generation.
    workers: HashMap<WorkerId, WorkerStatus>,
    /// The outbound channel setup started for each of them, awaited before the fan-out.
    handles: Vec<JoinHandle<()>>,
    /// The clients those tasks produce, shared with them until they are done.
    worker_connects: Arc<Mutex<HashMap<WorkerId, WorkerClient>>>,
    /// The clients the fan-out actually reached, once it has.
    started_connects: HashMap<WorkerId, WorkerClient>,
    /// `(task_id, subtask_idx)` of every task reported started.
    started_tasks: HashSet<(u32, u32)>,
    /// The checkpoint this generation restores from, if any.
    checkpoint_info: Option<CheckpointInfo>,
    /// The commit replay that checkpoint implies, if any.
    ///
    /// Moved into the job controller at the handover, which is why
    /// [`restored_commits_pending`](Self::restored_commits_pending) exists: whether there were
    /// commits to publish has to survive the value that answered the question being consumed.
    committing_state: Option<CommittingState>,
    /// Whether the restored checkpoint left a two-phase commit for this execution to finish.
    ///
    /// Recorded at the handover, *before* [`committing_state`](Self::committing_state) is moved
    /// into the job controller. Asking `committing_state.is_some()` afterwards would answer
    /// `false` for every job — the commits would be built into the controller and never sent,
    /// which is a restored checkpoint silently not finishing its two-phase commit.
    restored_commits_pending: bool,
    /// The job controller built for a non-leader execution, once its tasks are up.
    job_controller: Option<JobController>,
    /// When the wait now running started, for its timeout.
    wait_started: Instant,
}

impl<'a, 'ctx> PhaseContext<'a, 'ctx> {
    /// The context one scheduling attempt starts from.
    pub(crate) fn new(ctx: &'a mut JobContext<'ctx>) -> Self {
        let leader_mode = matches!(config().job_controller, JobControllerMode::Worker);
        Self {
            ctx,
            leader_mode,
            slots_needed: 0,
            workers: HashMap::new(),
            handles: Vec::new(),
            worker_connects: Arc::new(Mutex::new(HashMap::new())),
            started_connects: HashMap::new(),
            started_tasks: HashSet::new(),
            checkpoint_info: None,
            committing_state: None,
            restored_commits_pending: false,
            job_controller: None,
            wait_started: Instant::now(),
        }
    }

    /// The transition a job whose configuration asks it to stop makes instead of scheduling.
    pub(crate) fn stop_if_desired(&self) -> Option<Transition> {
        stop_transition(&self.ctx.config)
    }

    /// Crosses from a token-free phase into a region of irreversible work.
    ///
    /// Both halves of M11.D39a's boundary happen here and in this order: the job's single
    /// writer is read first, because a wait ends on the message that made its count and a
    /// decision published while it waited can be sitting behind that message unread; then the
    /// admission is taken, which under the landed M11.T08 gate re-reads the refusal that is
    /// the same decision reached by the other mechanism. A job that runs this path has an
    /// actor, so the first read is the operative one; taking the gate as well costs one
    /// uncontended lock and keeps the two mechanisms from diverging while both exist.
    ///
    /// A writer that has decided the job stops leaves through [`Admitted::Leave`] instead, and
    /// leaves *before* the admission is taken — not after. Taking it first and then noticing
    /// would put the decision on the far side of the lock whose whole purpose is to fence the
    /// region's effects, and the phase holding it would already be one method call from the
    /// fan-out.
    ///
    /// # Errors
    ///
    /// The fatal [`StateError`] of a refused configuration, from either mechanism.
    pub(crate) async fn admit(&mut self) -> Result<Admitted, StateError> {
        if let PhaseWait::Leave(stop) = self.observe(ConsumptionPoint::BeforeIrreversiblePhase)? {
            return Ok(Admitted::Leave(stop));
        }
        Ok(Admitted::Region(
            self.ctx.admit_irreversible_scheduling().await?,
        ))
    }

    /// Reads the job's single writer on a turn of a wait (M11.D39a's second consumption
    /// point).
    pub(crate) fn observe_intent_in_wait(&mut self) -> Result<PhaseWait, StateError> {
        self.observe(ConsumptionPoint::InsideInterruptibleWait)
    }

    /// Reads the job's single writer immediately before a stretch of work that is not
    /// irreversible in itself but *prepares* one (M11.D39a's first consumption point).
    ///
    /// [`AwaitingTasks::admit_commit_publish`](super::phases::AwaitingTasks::admit_commit_publish)
    /// is the caller: the handover it runs first moves the restored checkpoint's commits into
    /// the job controller, and a stop read on the far side of that move would be a stop read
    /// after the thing it exists to prevent had been assembled.
    pub(crate) fn observe_before_phase(&mut self) -> Result<PhaseWait, StateError> {
        self.observe(ConsumptionPoint::BeforeIrreversiblePhase)
    }

    /// Reads the job's single writer, and says whether the job is now leaving `Scheduling`.
    ///
    /// The stop test is made against the job's *configuration* rather than against which
    /// decision the writer reached, and unconditionally rather than only when something was
    /// decided. Both are deliberate. Publication writes the stop into `ctx.config`, so the
    /// configuration is where the answer is; and asking the same question
    /// [`schedule`](super::phases::schedule) asks on entry — through the same
    /// `stop_if_desired_non_running!` macro, so the two cannot disagree about what a stop mode
    /// means — catches a stop that reached the configuration by any route, not only the one
    /// this call just observed.
    ///
    /// Nothing on the selected M11.T08 path reaches here: a `PhaseContext` exists only for a
    /// job whose lifecycle is M11.D39a's single writer.
    ///
    /// # Errors
    ///
    /// The fatal [`StateError`] of a refused configuration.
    fn observe(&mut self, at: ConsumptionPoint) -> Result<PhaseWait, StateError> {
        self.ctx.observe_lifecycle_intent(at)?;
        Ok(match self.stop_if_desired() {
            Some(stop) => PhaseWait::Leave(stop),
            None => PhaseWait::Continue,
        })
    }

    /// Starts the clock a wait's timeout is measured from.
    pub(crate) fn begin_wait(&mut self) {
        self.wait_started = Instant::now();
    }

    /// Persists the scheduling generation this attempt raises the job to.
    ///
    /// # Errors
    ///
    /// Retryable: the row can be written by a later attempt.
    pub(crate) async fn persist_generation(&mut self, a: &Admission) -> Result<(), StateError> {
        self.ctx.status.generation += 1;
        let db = self.ctx.db.clone();
        if let Err(e) = a
            .effect(
                "persist the incremented scheduling generation",
                self.ctx.status.update_db(&db),
            )
            .await
        {
            return Err(self.retryable(
                "failed to advance generation for scheduling retry",
                anyhow!("{}", e),
                10,
            ));
        }
        Ok(())
    }

    /// Tears down whatever cluster the job is running on.
    ///
    /// Failure is logged and not propagated, exactly as on the landed path: the replacement
    /// cluster is started under a raised generation, so a stale worker that survives this is
    /// ignored rather than able to join.
    pub(crate) async fn tear_down_existing_cluster(&mut self, a: &Admission) {
        let stop = self
            .ctx
            .scheduler
            .stop_workers(&self.ctx.config.id, None, true);
        if let Err(e) = a.effect("tear down the job's existing cluster", stop).await {
            warn!(
                message = "failed to clean cluster prior to scheduling",
                job_id = %self.ctx.config.id,
                pipeline_id = *self.ctx.pipeline_info.pipeline_id,
                error = format!("{:?}", e)
            );
        }
    }

    /// Applies the job's parallelism overrides and starts the cluster this attempt runs on.
    pub(crate) async fn start_replacement_workers(
        &mut self,
        a: &Admission,
    ) -> Result<(), StateError> {
        self.ctx
            .program
            .update_parallelism(&self.ctx.config.parallelism_overrides);
        self.slots_needed = super::slots_for_job(self.ctx.program);
        let slots = self.slots_needed;
        a.effect(
            "start the job's replacement workers",
            Box::new(Scheduling {}).start_workers(self.ctx, slots),
        )
        .await?;
        Ok(())
    }

    /// Registers this generation and resolves the checkpoint it restores from.
    ///
    /// # Errors
    ///
    /// Fatal when the recovery manifest was written by a different state backend — every
    /// attempt resolves the same manifest, so retrying only delays the report — and retryable
    /// otherwise.
    pub(crate) async fn prepare_recovery_checkpoint(
        &mut self,
        a: &Admission,
    ) -> Result<(), StateError> {
        if self.leader_mode {
            let prepared = a
                .effect(
                    "register the generation and prepare its recovery checkpoint",
                    super::get_and_register_checkpoint_info_leader(self.ctx),
                )
                .await;
            match prepared {
                Ok(info) => {
                    self.checkpoint_info = info;
                    self.committing_state = None;
                }
                Err(e) if e.downcast_ref::<StateBackendError>().is_some() => {
                    return Err(fatal(
                        "cannot restore a checkpoint written with a different state backend",
                        e,
                    ));
                }
                Err(e) => return Err(self.retryable("failed to load checkpoint metadata", e, 20)),
            }
        } else {
            let (_state, info, committing) = a
                .effect(
                    "prepare the legacy recovery checkpoint",
                    super::get_checkpoint_info_legacy(Box::new(Scheduling {}), self.ctx),
                )
                .await?;
            self.checkpoint_info = info;
            self.committing_state = committing;
        }
        Ok(())
    }

    /// The fencing substrate an interrupted phase releases its authority into.
    ///
    /// Constructing this is the only thing a phase does with an interruption, which is what
    /// makes "the admission can be released only into token-free `Fencing`" true by
    /// construction rather than by review.
    pub(crate) fn into_fencing(
        self,
        reason: StateError,
        outstanding: IssuedAttempts,
    ) -> Interrupted<'a, 'ctx> {
        let targets = FenceTargets::for_workers(self.workers.keys().copied());
        Interrupted::new(Fencing::new(self, targets, outstanding), reason)
    }

    /// The retryable [`StateError`] the landed path builds for this state.
    pub(crate) fn retryable(
        &self,
        message: impl Into<String>,
        source: anyhow::Error,
        retries: usize,
    ) -> StateError {
        self.ctx
            .retryable(Box::new(Scheduling {}), message, source, retries)
    }
}

/// What the scheduling attempt has accumulated, for the sibling modules of the phase graph.
///
/// Separate from the effect surface above so that "what the phases may *do*" and "what the
/// attempt has learned" stay visibly different things.
impl<'ctx> PhaseContext<'_, 'ctx> {
    pub(crate) fn job(&self) -> &JobContext<'ctx> {
        self.ctx
    }

    pub(crate) fn program(&self) -> &LogicalProgram {
        self.ctx.program
    }

    pub(super) fn workers(&self) -> &HashMap<WorkerId, WorkerStatus> {
        &self.workers
    }

    /// The targets that have acknowledged a newer fence and its revokes since the last look.
    ///
    /// Empty for the whole of M11.T25, and a method rather than a constant for the same reason
    /// [`settlement_owner`](Self::settlement_owner) is: an acknowledgement is a message the
    /// M11.D39e worker protocol carries, and M11.T25 adds no wire field. M11.T26 adds the
    /// protocol and answers here; until it does, a fencing job in this half observes nothing
    /// and says so rather than assuming.
    pub(super) fn observed_fence_acknowledgements(&mut self) -> Vec<WorkerId> {
        Vec::new()
    }

    /// The target worker generations observed to have gone away since the last look.
    ///
    /// Empty for the whole of M11.T25, for the same reason: observing a generation's
    /// termination is part of the M11.D39e protocol M11.T26 owns.
    pub(super) fn observed_generation_terminations(&mut self) -> Vec<WorkerId> {
        Vec::new()
    }

    pub(crate) fn leader_mode(&self) -> bool {
        self.leader_mode
    }

    pub(super) fn checkpoint_info(&self) -> Option<&CheckpointInfo> {
        self.checkpoint_info.as_ref()
    }

    /// The clients the channel-setup tasks produced, taken once they have all been awaited.
    pub(crate) fn take_worker_connects(&mut self) -> HashMap<WorkerId, WorkerClient> {
        let shared = std::mem::replace(
            &mut self.worker_connects,
            Arc::new(Mutex::new(HashMap::new())),
        );
        Arc::try_unwrap(shared)
            .expect("every channel-setup task has been awaited")
            .into_inner()
    }

    /// Records which workers the fan-out reached.
    pub(crate) fn record_started_connects(&mut self, connects: HashMap<WorkerId, WorkerClient>) {
        for id in connects.keys() {
            if let Some(worker) = self.workers.get_mut(id) {
                worker.state = WorkerState::Initializing;
            }
        }
        self.started_connects = connects;
    }
}

/// The transition a stop-requesting configuration produces, if it requests one.
///
/// The landed path spells this `stop_if_desired_non_running!`, which `return`s from the state
/// body; the macro is used here verbatim rather than reimplemented so that the two routes
/// cannot come to disagree about what each stop mode means.
fn stop_transition(config: &JobConfig) -> Option<Transition> {
    fn wanted(config: &JobConfig) -> Result<Transition, ()> {
        let state = Box::new(Scheduling {});
        stop_if_desired_non_running!(state, config);
        Err(())
    }
    wanted(config).ok()
}

// ---------------------------------------------------------------------------------------
// Test-only reach into leader mode.
//
// Declared *here*, below everything this file's production half contains, because the source
// pins in `super::phase_tests` and `states/mod.rs` read that half as "everything before the
// first `#[cfg(test)]`" — a block placed higher would truncate it and make them vacuous.
// ---------------------------------------------------------------------------------------

#[cfg(test)]
impl PhaseContext<'_, '_> {
    /// Puts this context in leader mode with one registered worker, without touching the
    /// process's configuration.
    ///
    /// The two things a leader-mode tail needs are `leader_mode` — which [`Self::new`] reads
    /// from the process-wide `config().job_controller`, an `ArcSwap` every other test in this
    /// binary is reading at the same time — and the workers a completed first wait would have
    /// collected. Flipping the process-wide value to reach the first would silently change
    /// which branch every concurrently running scheduling row took, and the controller suite
    /// is required to pass at `--test-threads` 1 through 16. So the row states both directly
    /// instead, and `leader_mode`'s production derivation stays the single expression in
    /// [`Self::new`].
    pub(crate) fn run_as_leader_on(&mut self, worker: WorkerId, rpc_address: String) {
        self.leader_mode = true;
        self.workers.insert(
            worker,
            WorkerStatus {
                id: worker,
                machine_id: arroyo_types::MachineId(Arc::new(format!("machine_{}", worker.0))),
                rpc_address,
                data_address: "127.0.0.1:1".to_string(),
                slots: 1,
                state: WorkerState::Connected,
                reconciles_start_execution: true,
            },
        );
    }
}
