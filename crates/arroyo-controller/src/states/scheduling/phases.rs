//! `Scheduling` as a graph of phase types (M11.T25b, design M11.D39b).
//!
//! ```text
//! Preamble(Admission) → AwaitingWorkers → StartFanOut(Admission) → AwaitingTasks
//!                                                                → CommitPublish(Admission) → Running
//!        ╲                     ╲                    ╲                    ╲            ╲
//!         ╰────────────────────┴────────────────────┴────────────────────┴────────────┴──→ Fencing (token-free)
//! ```
//!
//! Three properties are enforced by the shape of this module, not by review:
//!
//! * **Every irreversible effect consumes the token.** Each effect method takes `self` by
//!   value, and the token-owning phases are the only values that hold an
//!   [`Admission`]. Performing an effect therefore consumes the admission; continuing means
//!   receiving a fresh phase back from the method that consumed it. An effect written after
//!   the token has been given up does not compile, and neither does one written on a phase
//!   that never had it.
//! * **A wait on the job's channel exists only on token-free types.** The channel is
//!   reachable only through [`PhaseContext`], which every phase owns privately;
//!   [`AwaitingWorkers`] and [`AwaitingTasks`] are the only types that expose a method
//!   leading to it.
//! * **A token can be released only into fencing.** The one way to leave a token-owning phase
//!   other than by finishing it is a method that drops the [`Admission`] and hands the
//!   context to token-free [`Fencing`](super::fencing::Fencing), carried by
//!   [`Interrupted`].
//!
//! # Why this file has no dependencies
//!
//! It imports nothing outside the crate, and nothing from the crate beyond a handful of
//! opaque types. That is deliberate: `super::compile_fail` compiles **this exact source** with
//! a plain `rustc` against a stub environment, both as written and with a named weakening
//! applied, which is what makes the two compile-time restrictions above evidence rather than
//! assertion. Everything that needs the controller's world lives in
//! [`super::admission`]. Adding an import here is allowed, but it has to be added to the stub
//! too, and the fixtures will say so loudly.
//!
//! # What runs this
//!
//! [`schedule`], for a job whose lifecycle mechanism is M11.D39a's single writer. No
//! production job has one through M11.T25 — see
//! [`LifecycleMode::SELECTED`](crate::states::lifecycle::LifecycleMode::SELECTED) — and the
//! landed `Scheduling::next` remains compiled, selected and unchanged.

use super::admission::{Admitted, PhaseContext, PhaseWait};
use super::fanout::{IssuedAttempts, SettlementBundle, hand_over};
use super::fencing::Interrupted;
use crate::states::{Admission, JobContext, StateError, Transition};

/// A step of the preamble: the same phase again, or fencing.
type PreambleStep<'a, 'ctx> = Result<Preamble<'a, 'ctx>, Interrupted<'a, 'ctx>>;

/// Either the phase an attempt advanced to, or the transition by which it left `Scheduling`
/// without reaching one.
///
/// Every crossing and every turn of a wait produces one of these, because both are points at
/// which the job's single writer is read and what it decided may be *stop*. A stop is not an
/// error and must not be reported as one — the job ends where a stop ends — so it travels as
/// the transition it is, in the `Ok` half, and the phase it would otherwise have become is
/// simply never constructed. Nothing in this enum carries an [`Admission`], which is the
/// structural half of "a stopping job takes no further irreversible effect".
pub(crate) enum Advanced<P> {
    To(P),
    Left(Transition),
}

/// The job's destructive preamble, holding the first admission.
///
/// Every method that does something the controller cannot take back takes `self` by value and
/// hands a fresh `Preamble` back, so the token travels through the preamble one effect at a
/// time and the preamble cannot perform two effects from one token without threading the
/// result. There is no method here that waits on the job's channel.
pub(crate) struct Preamble<'a, 'ctx> {
    admission: Admission,
    ctx: PhaseContext<'a, 'ctx>,
}

impl<'a, 'ctx> Preamble<'a, 'ctx> {
    /// Crosses into the preamble — or leaves for a stop the job's writer decided on, or fences
    /// because its configuration was refused.
    pub(crate) async fn enter(
        mut ctx: PhaseContext<'a, 'ctx>,
    ) -> Result<Advanced<Self>, Interrupted<'a, 'ctx>> {
        match ctx.admit().await {
            Ok(Admitted::Region(admission)) => Ok(Advanced::To(Self { admission, ctx })),
            Ok(Admitted::Leave(stop)) => Ok(Advanced::Left(stop)),
            Err(reason) => Err(ctx.into_fencing(reason, IssuedAttempts::default())),
        }
    }

    /// Persists the scheduling generation this attempt raises the job to.
    pub(crate) async fn persist_generation(mut self) -> PreambleStep<'a, 'ctx> {
        match self.ctx.persist_generation(&self.admission).await {
            Ok(()) => Ok(self),
            Err(reason) => Err(self.fence(reason)),
        }
    }

    /// Tears down whatever cluster the job is running on.
    pub(crate) async fn tear_down_existing_cluster(mut self) -> PreambleStep<'a, 'ctx> {
        self.ctx.tear_down_existing_cluster(&self.admission).await;
        Ok(self)
    }

    /// Starts the cluster this attempt runs on.
    pub(crate) async fn start_replacement_workers(mut self) -> PreambleStep<'a, 'ctx> {
        match self.ctx.start_replacement_workers(&self.admission).await {
            Ok(()) => Ok(self),
            Err(reason) => Err(self.fence(reason)),
        }
    }

    /// Registers this generation and prepares the checkpoint it restores from.
    pub(crate) async fn prepare_recovery_checkpoint(mut self) -> PreambleStep<'a, 'ctx> {
        match self.ctx.prepare_recovery_checkpoint(&self.admission).await {
            Ok(()) => Ok(self),
            Err(reason) => Err(self.fence(reason)),
        }
    }

    /// Ends the preamble, releasing the admission into the wait that follows it.
    ///
    /// The release is structural: an [`AwaitingWorkers`] can be obtained only from here or
    /// from fencing, and obtaining one consumes the token. That is why no wait in this graph
    /// can be reached while an admission is held — holding one across a wait for workers would
    /// make the job unrefusable for exactly as long as it is least able to defend itself.
    pub(crate) fn release(self) -> AwaitingWorkers<'a, 'ctx> {
        let Self { admission, mut ctx } = self;
        drop(admission);
        ctx.begin_wait();
        AwaitingWorkers { ctx }
    }

    /// Releases the admission into token-free fencing, carrying the reason.
    fn fence(self, reason: StateError) -> Interrupted<'a, 'ctx> {
        let Self { admission, ctx } = self;
        drop(admission);
        ctx.into_fencing(reason, IssuedAttempts::default())
    }
}

/// The wait for the job's replacement workers to register and open their channels.
///
/// Token-free: it holds no [`Admission`], so a refusal raised while it waits is publishable
/// for the whole of the wait.
pub(crate) struct AwaitingWorkers<'a, 'ctx> {
    ctx: PhaseContext<'a, 'ctx>,
}

impl<'a, 'ctx> AwaitingWorkers<'a, 'ctx> {
    /// Reads the job's single writer on this turn of the wait.
    ///
    /// [`PhaseWait::Leave`] is a stop it decided on, and it is the same value the message path
    /// produces for a stop that arrived as a `ConfigUpdate` — so the loop that drives this wait
    /// cannot treat one as decisive and the other as advisory.
    pub(crate) fn observe_intent(&mut self) -> Result<PhaseWait, StateError> {
        self.ctx.observe_intent_in_wait()
    }

    /// Waits for one message from the job's channel.
    pub(crate) async fn await_message(&mut self) -> Result<PhaseWait, StateError> {
        self.ctx.await_message_from_workers().await
    }

    /// Whether the workers that have registered supply the slots the program needs.
    pub(crate) fn workers_are_sufficient(&self) -> bool {
        self.ctx.workers_are_sufficient()
    }

    /// Waits for every registered worker's outbound channel to be open.
    pub(crate) async fn await_worker_channels(&mut self) -> Result<(), StateError> {
        self.ctx.await_worker_channels().await
    }

    /// Crosses into the fan-out.
    ///
    /// The capability gate runs *before* the admission is taken, deliberately: refusing to
    /// schedule onto a worker that cannot reconcile a `StartExecution` is not an irreversible
    /// effect and awaits nothing, so it has no business holding the job's publication lock.
    pub(crate) async fn admit_fan_out(
        self,
    ) -> Result<Advanced<StartFanOut<'a, 'ctx>>, Interrupted<'a, 'ctx>> {
        let Self { mut ctx } = self;
        if let Err(reason) = ctx.require_reconciling_workers() {
            return Err(ctx.into_fencing(reason, IssuedAttempts::default()));
        }
        match ctx.admit().await {
            Ok(Admitted::Region(admission)) => Ok(Advanced::To(StartFanOut {
                admission,
                ctx,
                issued: IssuedAttempts::default(),
            })),
            // A stop decided here leaves without a token, so the fan-out below is not merely
            // skipped: there is no value from which it could be performed.
            Ok(Admitted::Leave(stop)) => Ok(Advanced::Left(stop)),
            Err(reason) => Err(ctx.into_fencing(reason, IssuedAttempts::default())),
        }
    }

    /// Fences without ever having held a token.
    pub(crate) fn fence(self, reason: StateError) -> Interrupted<'a, 'ctx> {
        self.ctx.into_fencing(reason, IssuedAttempts::default())
    }
}

/// The `StartExecution` fan-out, holding the second admission and its issued-attempt
/// inventory.
///
/// This is where M11.T25c attaches. The inventory travels with the token because M11.D39b
/// makes them one obligation: what was issued and the authority under which it was issued are
/// handed over together or not at all — see [`SettlementBundle`].
pub(crate) struct StartFanOut<'a, 'ctx> {
    admission: Admission,
    ctx: PhaseContext<'a, 'ctx>,
    issued: IssuedAttempts,
}

impl<'a, 'ctx> StartFanOut<'a, 'ctx> {
    /// Sends every worker its `StartExecution` and waits for all of them to settle.
    ///
    /// Consumes the admission, as every irreversible effect does. On an interruption the
    /// obligation — the inventory *and* the authority — is offered to the job's settlement
    /// owner as one unit; M11.T25 has no such owner, so it comes back and the admission is
    /// released only after the fan-out has settled in place. An owner that declines, or that
    /// drops what it is handed, ends here the same way: with an inventory this phase is still
    /// answerable for and a record of what an owner took or lost. See
    /// `super::fanout::SettlementOutcome::into_fencing_record`.
    ///
    /// If this future is *dropped* instead — the job's state task cancelled mid-fan-out —
    /// nothing below the `await` runs at all, and the same offer is made from the region rescue
    /// that outlives it. See `super::fanout::AttemptLedger::settlement_rescue`: the seam is
    /// reached on both paths, and the cancelled one is the path an owner exists for.
    pub(crate) async fn issue(self) -> Result<Self, Interrupted<'a, 'ctx>> {
        let Self {
            admission,
            mut ctx,
            issued: _,
        } = self;
        let (admission, issued, outcome) = ctx.fan_out_start_execution(admission).await;
        let Err(reason) = outcome else {
            return Ok(Self {
                admission,
                ctx,
                issued,
            });
        };

        let owner = ctx.settlement_owner();
        // The admission is released inside this, and no arm of it hands one back: whatever an
        // owner did, what is left for the phase is an inventory and a record.
        let (issued, handover) =
            hand_over(SettlementBundle::new(admission, issued), owner.as_deref())
                .into_fencing_record();
        let mut interrupted = ctx.into_fencing(reason, issued);
        interrupted.fencing_mut().note_handover(handover);
        Err(interrupted)
    }

    /// Ends the fan-out, releasing the admission into the wait for the tasks it started.
    pub(crate) fn release(self) -> AwaitingTasks<'a, 'ctx> {
        let Self {
            admission,
            mut ctx,
            issued,
        } = self;
        drop(admission);
        ctx.begin_wait();
        AwaitingTasks { ctx, issued }
    }
}

/// The wait for the started execution's tasks to report in. Token-free.
pub(crate) struct AwaitingTasks<'a, 'ctx> {
    ctx: PhaseContext<'a, 'ctx>,
    issued: IssuedAttempts,
}

impl<'a, 'ctx> AwaitingTasks<'a, 'ctx> {
    /// Reads the job's single writer on this turn of the wait.
    pub(crate) fn observe_intent(&mut self) -> Result<PhaseWait, StateError> {
        self.ctx.observe_intent_in_wait()
    }

    /// Waits for one message from the job's channel.
    pub(crate) async fn await_message(&mut self) -> Result<PhaseWait, StateError> {
        self.ctx.await_message_from_tasks().await
    }

    /// Whether every task of the program has reported started.
    pub(crate) fn tasks_are_all_started(&self) -> bool {
        self.ctx.tasks_are_all_started()
    }

    /// Prepares the handover and crosses into the commit publication, if there is one.
    ///
    /// Building the job controller is not irreversible and is done here, before the third
    /// admission is taken; and the admission is taken only when the restored checkpoint
    /// actually left a two-phase commit to finish, so a job with nothing to publish never
    /// holds the lock at all.
    pub(crate) async fn admit_commit_publish(
        self,
    ) -> Result<Advanced<CommitOrRun<'a, 'ctx>>, Interrupted<'a, 'ctx>> {
        let Self { mut ctx, issued } = self;
        // Before the handover, not after it. The wait above ends on the message that made its
        // count, so this is the first look since; and the handover moves the restored
        // checkpoint's commits into the job controller, which is the assembly of the very
        // effect a stop has to prevent.
        match ctx.observe_before_phase() {
            Ok(PhaseWait::Continue) => {}
            Ok(PhaseWait::Leave(stop)) => return Ok(Advanced::Left(stop)),
            Err(reason) => return Err(ctx.into_fencing(reason, issued)),
        }
        ctx.prepare_handover().await;
        if !ctx.needs_restored_commits() {
            return Ok(Advanced::To(CommitOrRun::Run(Running { ctx, issued })));
        }
        match ctx.admit().await {
            Ok(Admitted::Region(admission)) => {
                Ok(Advanced::To(CommitOrRun::Publish(CommitPublish {
                    admission,
                    ctx,
                    issued,
                })))
            }
            Ok(Admitted::Leave(stop)) => Ok(Advanced::Left(stop)),
            Err(reason) => Err(ctx.into_fencing(reason, issued)),
        }
    }

    /// Fences without ever having held a token, carrying what the fan-out issued.
    pub(crate) fn fence(self, reason: StateError) -> Interrupted<'a, 'ctx> {
        self.ctx.into_fencing(reason, self.issued)
    }
}

/// Whether the execution has commits to publish before it runs.
pub(crate) enum CommitOrRun<'a, 'ctx> {
    Publish(CommitPublish<'a, 'ctx>),
    Run(Running<'a, 'ctx>),
}

/// The publication of a restored checkpoint's commits, holding the third admission.
///
/// These finish a two-phase commit against the job's sinks: they are visible outside the
/// cluster and cannot be withdrawn, which is the whole reason this is a region of its own.
pub(crate) struct CommitPublish<'a, 'ctx> {
    admission: Admission,
    ctx: PhaseContext<'a, 'ctx>,
    issued: IssuedAttempts,
}

impl<'a, 'ctx> CommitPublish<'a, 'ctx> {
    /// Publishes the restored checkpoint's commits, consuming the admission.
    pub(crate) async fn publish_restored_commits(mut self) -> Self {
        self.ctx.publish_restored_commits(&self.admission).await;
        self
    }

    /// Ends the publication, releasing the admission into the running execution.
    pub(crate) fn release(self) -> Running<'a, 'ctx> {
        let Self {
            admission,
            ctx,
            issued,
        } = self;
        drop(admission);
        Running { ctx, issued }
    }
}

/// The execution is up; all that is left is to hand it to the state that runs it.
/// Token-free.
pub(crate) struct Running<'a, 'ctx> {
    ctx: PhaseContext<'a, 'ctx>,
    issued: IssuedAttempts,
}

impl<'a, 'ctx> Running<'a, 'ctx> {
    /// Leaves `Scheduling` for the state that runs the execution.
    pub(crate) async fn into_transition(self) -> Result<Transition, Interrupted<'a, 'ctx>> {
        let Self { ctx, issued } = self;
        match ctx.into_transition().await {
            Ok(transition) => Ok(transition),
            Err((ctx, reason)) => Err(ctx.into_fencing(reason, issued)),
        }
    }
}

/// Runs one scheduling attempt through the M11.D39b phase graph.
///
/// Reached only from a job whose lifecycle mechanism is M11.D39a's single writer, which no
/// production job has through M11.T25.
pub(crate) async fn schedule(ctx: &mut JobContext<'_>) -> Result<Transition, StateError> {
    let ctx = PhaseContext::new(ctx);
    if let Some(stop) = ctx.stop_if_desired() {
        return Ok(stop);
    }
    match run(ctx).await {
        Ok(transition) => Ok(transition),
        // An interruption is not always a failure: the job's writer may have answered it by
        // asking the job to stop, and a stop ends where a stop ends.
        Err(interrupted) => interrupted.reconcile_and_report(),
    }
}

/// The graph itself, one phase per line.
async fn run<'a, 'ctx>(ctx: PhaseContext<'a, 'ctx>) -> Result<Transition, Interrupted<'a, 'ctx>> {
    let awaiting_workers = match preamble(ctx).await? {
        Advanced::To(phase) => phase,
        Advanced::Left(transition) => return Ok(transition),
    };
    let fan_out = match wait_for_workers(awaiting_workers).await? {
        Advanced::To(phase) => phase,
        Advanced::Left(transition) => return Ok(transition),
    };
    let awaiting_tasks = fan_out.issue().await?.release();
    let running = match wait_for_tasks(awaiting_tasks).await? {
        Advanced::To(phase) => phase,
        Advanced::Left(transition) => return Ok(transition),
    };
    running.into_transition().await
}

/// The first admitted region, effect by effect.
async fn preamble<'a, 'ctx>(
    ctx: PhaseContext<'a, 'ctx>,
) -> Result<Advanced<AwaitingWorkers<'a, 'ctx>>, Interrupted<'a, 'ctx>> {
    let preamble = match Preamble::enter(ctx).await? {
        Advanced::To(preamble) => preamble,
        Advanced::Left(transition) => return Ok(Advanced::Left(transition)),
    };
    let preamble = preamble.persist_generation().await?;
    let preamble = preamble.tear_down_existing_cluster().await?;
    let preamble = preamble.start_replacement_workers().await?;
    let preamble = preamble.prepare_recovery_checkpoint().await?;
    Ok(Advanced::To(preamble.release()))
}

/// The first interruptible wait, up to the crossing into the fan-out.
async fn wait_for_workers<'a, 'ctx>(
    mut awaiting: AwaitingWorkers<'a, 'ctx>,
) -> Result<Advanced<StartFanOut<'a, 'ctx>>, Interrupted<'a, 'ctx>> {
    loop {
        match awaiting.observe_intent() {
            Ok(PhaseWait::Continue) => {}
            Ok(PhaseWait::Leave(transition)) => return Ok(Advanced::Left(transition)),
            Err(reason) => return Err(awaiting.fence(reason)),
        }
        match awaiting.await_message().await {
            Ok(PhaseWait::Continue) => {}
            Ok(PhaseWait::Leave(transition)) => return Ok(Advanced::Left(transition)),
            Err(reason) => return Err(awaiting.fence(reason)),
        }
        if awaiting.workers_are_sufficient() {
            break;
        }
    }
    if let Err(reason) = awaiting.await_worker_channels().await {
        return Err(awaiting.fence(reason));
    }
    awaiting.admit_fan_out().await
}

/// The second interruptible wait, up to the crossing into the commit publication.
async fn wait_for_tasks<'a, 'ctx>(
    mut awaiting: AwaitingTasks<'a, 'ctx>,
) -> Result<Advanced<Running<'a, 'ctx>>, Interrupted<'a, 'ctx>> {
    while !awaiting.tasks_are_all_started() {
        match awaiting.observe_intent() {
            Ok(PhaseWait::Continue) => {}
            Ok(PhaseWait::Leave(transition)) => return Ok(Advanced::Left(transition)),
            Err(reason) => return Err(awaiting.fence(reason)),
        }
        match awaiting.await_message().await {
            Ok(PhaseWait::Continue) => {}
            Ok(PhaseWait::Leave(transition)) => return Ok(Advanced::Left(transition)),
            Err(reason) => return Err(awaiting.fence(reason)),
        }
    }
    match awaiting.admit_commit_publish().await? {
        Advanced::To(CommitOrRun::Publish(publishing)) => Ok(Advanced::To(
            publishing.publish_restored_commits().await.release(),
        )),
        Advanced::To(CommitOrRun::Run(running)) => Ok(Advanced::To(running)),
        Advanced::Left(transition) => Ok(Advanced::Left(transition)),
    }
}
