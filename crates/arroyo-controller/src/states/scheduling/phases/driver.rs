//! The phase driver: the flow that walks one scheduling attempt through the typestates.
//!
//! Split out of [`super`] when PR #160 review comment `5384611151`'s answer took `phases.rs`
//! to 507 lines, past the plan's 500-line production bar — PR #160 review comment
//! `5384870087`. A **child** of `phases` rather than a sibling, so the typestates' private
//! fields stay private to the module that defines them: a sibling would have to be handed
//! them, which is the structural argument the compile-fail rows rest on.
//!
//! The cut is along the boundary the file already had — the typestates and the flow that
//! drives them — and nothing moved was rewritten.

use super::*;
/// Runs one scheduling attempt through the M11.D39b phase graph.
///
/// Reached only from a job whose lifecycle mechanism is M11.D39a's single writer, which every
/// production job has had since M11.T26h.
pub(crate) async fn schedule(ctx: &mut JobContext<'_>) -> Result<Transition, StateError> {
    let ctx = PhaseContext::new(ctx);
    if let Some(stop) = ctx.stop_if_desired() {
        return Ok(stop);
    }
    match run(ctx).await {
        Ok(transition) => Ok(transition),
        // An interruption is not always a failure: the job's writer may have answered it by
        // asking the job to stop, and a stop ends where a stop ends. It is also where the
        // obligation this attempt leaves behind becomes durable (M11.T26f) — see
        // `Interrupted::reconcile_and_report`, which is `async` for exactly that reason.
        Err(interrupted) => interrupted.reconcile_and_report().await,
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
    let preamble = preamble.adopt_lifecycle_authority().await?;
    let preamble = preamble.discharge_recovered_fencing().await?;
    let preamble = preamble.persist_generation().await?;
    let preamble = preamble.tear_down_existing_cluster().await?;
    let preamble = preamble.start_replacement_workers().await?;
    let preamble = preamble.prepare_recovery_checkpoint().await?;
    let preamble = preamble.publish_metadata_root().await?;
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
    // The same shape as the loop above, and for the same reason: opening the workers'
    // channels is a wait, and every interruptible wait is a consumption point (M11.D39a).
    // PR #160 review comment `5384611151`.
    while !awaiting.worker_channels_are_open() {
        match awaiting.observe_intent() {
            Ok(PhaseWait::Continue) => {}
            Ok(PhaseWait::Leave(transition)) => return Ok(Advanced::Left(transition)),
            Err(reason) => return Err(awaiting.fence(reason)),
        }
        match awaiting.await_worker_channels().await {
            Ok(PhaseWait::Continue) => {}
            Ok(PhaseWait::Leave(transition)) => return Ok(Advanced::Left(transition)),
            Err(reason) => return Err(awaiting.fence(reason)),
        }
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
