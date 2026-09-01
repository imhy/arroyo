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
//! [`execution`] and [`observation`] are child modules rather than siblings for one reason: the
//! two interruptible waits, the handover and every consumption point need [`PhaseContext`]'s
//! own fields, and a sibling would have forced those open to the whole of `states::scheduling`
//! — including [`super::phases`], whose entire claim is that it reaches the job only through
//! the methods a phase exposes. What is left here is the part that is neither: the context
//! itself, and the effects a token-owning phase performs with it.
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
use crate::AuthorityOutcome;
use crate::job_controller::JobController;
use crate::states::lifecycle::handshake::FenceAcknowledgement;
use crate::states::lifecycle::recovery::ObservedTermination;
use crate::states::lifecycle::{StatusPublication, stand_down};
use crate::states::{Admission, JobContext, StateError, fatal};

pub(super) mod execution;
pub(super) mod fencing;
pub(super) mod handover;
pub(super) mod observation;
pub(super) mod root;

pub(super) use observation::stop_transition;
pub(crate) use observation::{Admitted, FencedIntent, PhaseWait};

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
    /// The lost fence duel this attempt has already learned about, if it has.
    ///
    /// Recorded rather than raised, because losing the duel is not a failure of the job: it is
    /// this controller learning that another one holds it. [`Self::into_fencing`] moves it onto
    /// the token-free [`Fencing`], which is what turns the attempt's end into a stand-down
    /// instead of an error — see [`Interrupted::reconcile_and_report`].
    superseded: Option<crate::StaleAuthority>,
    /// The candidate metadata object this attempt published and never rooted, if it published
    /// one.
    ///
    /// A losing controller leaves exactly this behind, and it belongs on the durable fencing
    /// record so that the controller which holds the job can see what is out there.
    unrooted_candidate: Option<String>,
    /// Fence acknowledgements this attempt has observed and not yet reconciled (M11.T26f).
    ///
    /// An inbox rather than a live poll, because an acknowledgement is something that *happened*
    /// at a moment — a `FENCE_ACKNOWLEDGED` response the handshake read — and a reconciliation
    /// asking "have any arrived?" must be answered from what was observed rather than by going
    /// and observing again. Two things write to it: the fan-out's own handshake
    /// ([`address_every_worker`](super::fanout)), whose acknowledgements settle nothing of this
    /// attempt's because the fence they acknowledge is the one its starts carry, and the
    /// recovery pass in [`fencing`](self::fencing), whose acknowledgements are of a *newer*
    /// fence and do settle.
    ///
    /// Drained by [`Self::observed_fence_acknowledgements`], so a reconciliation that runs twice
    /// does not count one acknowledgement twice — and does not need to, since every operation it
    /// feeds is idempotent anyway.
    observed_acknowledgements: Vec<FenceAcknowledgement>,
    /// Worker generation terminations this attempt has observed and not yet reconciled.
    ///
    /// The same shape, filled from the one place a termination can be observed at all: a
    /// scheduler that tracks its worker generations answering a listing successfully. See
    /// [`observe_terminations`](crate::states::lifecycle::recovery::observe_terminations).
    observed_terminations: Vec<ObservedTermination>,
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
            superseded: None,
            unrooted_candidate: None,
            observed_acknowledgements: Vec::new(),
            observed_terminations: Vec::new(),
        }
    }

    /// Starts the clock a wait's timeout is measured from.
    pub(crate) fn begin_wait(&mut self) {
        self.wait_started = Instant::now();
    }

    /// Adopts this job's durable lifecycle authority (M11.D39d).
    ///
    /// **The first effect of the attempt, and deliberately the first.** M11.D39d requires cold
    /// adoption to CAS-increment the fence and install a fresh epoch *before any effect*, and
    /// this is where "before any effect" is: the next four steps persist a generation, tear
    /// down the job's cluster, start a replacement one and publish a metadata root, and the
    /// admitted region that contains them starts here. Adoption is itself an effect — it raises
    /// a durable, monotonic counter — which is why it is inside the region rather than in front
    /// of it.
    ///
    /// Every scheduling attempt re-adopts rather than only the first. The fence a directive
    /// carries is the one this attempt installed, so an attempt that re-adopted is an attempt
    /// whose starts supersede the previous attempt's at every worker; and a controller that has
    /// been away long enough for another to take the job over learns so here, before it has
    /// touched anything.
    ///
    /// # Errors
    ///
    /// A fatal reason when another controller holds the job — recorded as
    /// [`Self::superseded`](PhaseContext) so the fencing this returns into stands the job down
    /// rather than failing it — and retryable when the adoption could not be attempted at all.
    pub(crate) async fn adopt_lifecycle_authority(
        &mut self,
        a: &Admission,
    ) -> Result<(), StateError> {
        let db = self.ctx.db.clone();
        match a
            .effect(
                "adopt the job's durable lifecycle authority",
                self.ctx.status.adopt_lifecycle_authority(&db),
            )
            .await
        {
            Ok(AuthorityOutcome::Applied(())) => Ok(()),
            Ok(AuthorityOutcome::Stale(stale)) => Err(self.stand_down_from(stale)),
            Err(e) => Err(self.retryable(
                "failed to adopt the job's durable lifecycle authority",
                anyhow!("{}", e),
                10,
            )),
        }
    }

    /// Persists the scheduling generation this attempt raises the job to.
    ///
    /// # Errors
    ///
    /// Retryable: the row can be written by a later attempt. A conditional write another
    /// controller's authority refused is not retryable and stands the job down instead.
    pub(crate) async fn persist_generation(&mut self, a: &Admission) -> Result<(), StateError> {
        self.ctx.status.generation += 1;
        match a
            .effect(
                "persist the incremented scheduling generation",
                self.ctx.publish_status(),
            )
            .await
        {
            Ok(StatusPublication::Published) => Ok(()),
            Ok(StatusPublication::Superseded(stale)) => Err(self.stand_down_from(stale)),
            Err(e) => Err(self.retryable(
                "failed to advance generation for scheduling retry",
                anyhow!("{}", e),
                10,
            )),
        }
    }

    /// Records that this controller has lost the job, and produces the reason its phase fences
    /// with.
    ///
    /// The reason is fatal-shaped so that no path retries it — retrying a lost authority is a
    /// superseded controller trying to overwrite a live one — but it is never reported as a
    /// job failure: [`Self::into_fencing`] carries the record onto the token-free `Fencing`,
    /// and `Interrupted::reconcile_and_report` answers a stop.
    fn stand_down_from(&mut self, stale: crate::StaleAuthority) -> StateError {
        stand_down(stale.clone());
        let reason = fatal(
            "another controller holds this job's durable lifecycle authority",
            anyhow!("{}", stale),
        );
        self.superseded = Some(stale);
        reason
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
        mut self,
        reason: StateError,
        outstanding: IssuedAttempts,
    ) -> Interrupted<'a, 'ctx> {
        // Taken before the context is moved into the fencing that will own it. Both are facts
        // about the attempt that is ending rather than about the context, which is why they
        // travel onto the record rather than staying reachable through it.
        let superseded = self.superseded.take();
        let unrooted_candidate = self.unrooted_candidate.take();
        let targets = FenceTargets::for_workers(self.workers.keys().copied());
        // The fence this attempt addressed those targets under, read before the context is
        // moved. It is what an acknowledgement is measured against: one at or below it revoked
        // nothing this attempt issued.
        let addressed_fence = self.addressed_fence();
        let mut fencing = Fencing::new(self, targets, addressed_fence, outstanding);
        if let Some(stale) = superseded {
            fencing.note_superseded(stale);
        }
        if let Some(candidate) = unrooted_candidate {
            fencing.note_unrooted_candidate(candidate);
        }
        Interrupted::new(fencing, reason)
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

    /// The acknowledgements this attempt has observed since the last look (M11.T26f).
    ///
    /// M11.T25 answered `Vec::new()` here because the protocol that makes an acknowledgement
    /// observable did not exist; M11.T26c built it and M11.T26f connects it. Every value this
    /// returns was minted by
    /// [`handshake`](crate::states::lifecycle::handshake) reading a `FENCE_ACKNOWLEDGED`
    /// response, and each carries the **height** that generation reported — which is what lets
    /// the reconciliation refuse an acknowledgement that revoked nothing rather than trust its
    /// caller not to offer one.
    ///
    /// Draining rather than copying: an observation is reconciled once, and everything it feeds
    /// is monotone, so a second look answers about what has happened since.
    pub(super) fn observed_fence_acknowledgements(&mut self) -> Vec<FenceAcknowledgement> {
        std::mem::take(&mut self.observed_acknowledgements)
    }

    /// Records acknowledgements this attempt observed, for its reconciliation to read.
    pub(super) fn record_observed_acknowledgements(
        &mut self,
        acknowledgements: Vec<FenceAcknowledgement>,
    ) {
        self.observed_acknowledgements.extend(acknowledgements);
    }

    /// The lifecycle fence this attempt addresses its worker generations under (M11.T26f).
    ///
    /// Derived from the job's own protocol rather than read from the row a second time, so that
    /// the fence a directive carries, the fence an inventory records for its identifiers and the
    /// fence an acknowledgement is measured against are one value. Zero under the pre-flag-day
    /// protocol and for a controller holding no adopted fence, which is the wire's own sentinel
    /// for "carries no fence" — and the honest answer, because such an attempt addresses nothing
    /// under one.
    pub(super) fn addressed_fence(&self) -> u64 {
        self.ctx
            .fence_protocol()
            .map(|protocol| protocol.fence())
            .unwrap_or(0)
    }

    /// The address each registered worker of this attempt was reached at.
    ///
    /// Carried onto the durable fencing record so that a controller which did not start these
    /// workers can still advance its fence at them — see
    /// [`FenceTarget::rpc_address`](arroyo_rpc::fencing::FenceTarget::rpc_address).
    pub(super) fn target_addresses(&self) -> HashMap<WorkerId, String> {
        self.workers
            .iter()
            .map(|(id, status)| (*id, status.rpc_address.clone()))
            .collect()
    }

    /// The job this attempt is for, mutably.
    ///
    /// The one route by which anything in the phase graph writes the job's status, and it exists
    /// for exactly one caller: the durable fencing record an interrupted attempt persists, which
    /// is staged on the status and published through the funnel. It is `pub(super)` rather than
    /// `pub(crate)` so that reach stays inside `scheduling`, where the phase graph's own
    /// restrictions are checked.
    pub(super) fn job_mut(&mut self) -> &mut JobContext<'ctx> {
        self.ctx
    }

    /// The worker generation this attempt addressed its `StartExecution` requests to.
    ///
    /// The job's own scheduling generation, read from the row rather than carried, so that the
    /// generation an observation is checked against and the generation
    /// [`fence_protocol`](crate::states::JobContext::fence_protocol) addresses its directives to
    /// are one value. It is what makes an observation about some *other* generation account for
    /// nothing in this attempt's obligation — see
    /// [`SettlementBundle::observe`](super::fanout::SettlementBundle).
    pub(super) fn addressed_generation(&self) -> u64 {
        self.ctx.status.generation
    }

    /// The job's settlement owner, if it has one.
    ///
    /// The concrete owner rather than the transfer trait, because the caller here is the
    /// fencing reconciliation and what it has to do is *tell* the owner what it observed. The
    /// transfer seam's view is
    /// [`settlement_owner`](super::fanout::PhaseContext::settlement_owner); both read the one
    /// value the job holds.
    pub(super) fn settlement(&self) -> Option<&Arc<crate::states::lifecycle::JobSettlementOwner>> {
        self.ctx.settlement()
    }

    /// The target worker generations observed to have gone away since the last look
    /// (M11.T26f).
    ///
    /// Filled from the one authoritative source there is: a scheduler that tracks the worker
    /// generations it started, answering a listing successfully. A listing that *failed*, a
    /// scheduler that keeps no registry, and a channel that would not open all leave this empty
    /// — which is the difference between "that generation is gone" and "I cannot see it", and
    /// the whole reason `ObservedTermination` has a private constructor.
    pub(super) fn observed_generation_terminations(&mut self) -> Vec<ObservedTermination> {
        std::mem::take(&mut self.observed_terminations)
    }

    /// Records terminations this attempt observed, for its reconciliation to read.
    pub(super) fn record_observed_terminations(&mut self, terminations: Vec<ObservedTermination>) {
        self.observed_terminations.extend(terminations);
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
    /// Whether the handover built this job's controller.
    ///
    /// Asked of the phase rather than of the [`JobContext`] because that is where the controller
    /// lives until `into_transition` moves it, and the topology question — a worker-leader
    /// execution builds none — is answered by the handover itself.
    pub(crate) fn built_job_controller(&self) -> bool {
        self.job_controller.is_some()
    }

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
