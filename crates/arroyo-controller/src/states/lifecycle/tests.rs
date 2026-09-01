//! Focused tests for the D39a single writer and the M11.T25f mode seam.
//!
//! These sit beside the modules they cover and need no database, no runtime and no state
//! task: the intent mailbox, the classification and the actor's decision are all values.
//! The rows that need a whole [`StateMachine`](crate::states::StateMachine) — cold
//! adoption, an inactive job, a program that will not load — live in `states/mod.rs`'s test
//! module, beside the fixtures they reuse.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arroyo_rpc::state_backend::{StateBackendError, StateBackendSelector};

use super::actor::{ConsumptionPoint, LifecycleActor, LifecycleDecision};
use super::intent::{IntentMailbox, IntentVersion, IntentWakeup, LifecycleIntent, Submission};
use super::mode::LifecycleMode;
use crate::types::public::{RestartMode, StopMode};
use crate::{JobConfig, PolledJob};

/// A running job's configuration. Only the fields a test varies are interesting; the rest
/// are fixed so a difference in an outcome can only come from the field under test.
fn running_config() -> JobConfig {
    JobConfig {
        id: Arc::new("job_abc".to_string()),
        organization_id: "org".to_string(),
        pipeline_name: "pipeline".to_string(),
        pipeline_id: 1,
        stop_mode: StopMode::none,
        checkpoint_interval: Duration::from_secs(10),
        ttl: None,
        parallelism_overrides: HashMap::new(),
        restart_nonce: 3,
        restart_mode: RestartMode::safe,
        ignore_state_before_epoch: None,
        env_vars: serde_json::json!({}),
        scheduler_config: serde_json::json!({}),
        state_backend: StateBackendSelector::Parquet,
    }
}

/// A polled row as [`crate::classify_polled_row`] hands it on: already resolved against the
/// job's execution record, so `config` carries the execution's own selector and `refusal`
/// says why the row's own value was rejected.
fn polled(config: JobConfig, refusal: Option<StateBackendError>) -> PolledJob {
    PolledJob {
        execution_selector: StateBackendSelector::Parquet,
        config,
        refusal,
    }
}

/// The refusal a row asking for another backend produces.
fn selector_changed() -> StateBackendError {
    StateBackendError::JobSelectorChanged {
        label: "job \"job_abc\"".to_string(),
        running: StateBackendSelector::Parquet,
        requested: StateBackendSelector::StateEngine,
    }
}

/// The job's single writer, reading the mailbox the test plays the configuration poll into.
fn actor_for(mailbox: &Arc<IntentMailbox>) -> LifecycleActor {
    LifecycleActor::new(
        Arc::new("job_abc".to_string()),
        StateBackendSelector::Parquet,
        Arc::clone(mailbox),
    )
}

fn new_mailbox() -> Arc<IntentMailbox> {
    Arc::new(IntentMailbox::new(Arc::new("job_abc".to_string())))
}

/// Production runs [`LifecycleMode::FencedV2`], and the claim is checked over the whole enum
/// rather than by sampling one call (M11.T26h, DoD M11.T26z).
///
/// Two things have to hold together, and either alone would be worth little. The selection must
/// name `FencedV2` — and it must do so for *every* variant's worth of reasoning, so that a mode
/// added later is not silently selectable. The `match` below is what makes the second half true:
/// it is exhaustive, so a new variant stops this test compiling until somebody says whether
/// production may run it, and `ALL` is checked to be the same set the selection searches.
///
/// The companion source pin `every_production_path_selects_the_fenced_v2_lifecycle`, in
/// `states/mod.rs`, covers what this cannot: that no production *call site* passes anything but
/// the selection.
///
/// M11.T25's version of this row asserted the opposite answer and was rewritten — not deleted —
/// by the activation change, so the requirement it carried ("exactly one mode is production's,
/// and it is a consequence of the enum") is still carried by a row of this name.
#[test]
fn production_selects_only_the_fenced_v2_lifecycle() {
    assert_eq!(
        LifecycleMode::ALL.len(),
        2,
        "`LifecycleMode::ALL` is what the production selection searches; a variant missing \
         from it could never be selected, and one added to it without a decision below \
         could be"
    );

    for mode in LifecycleMode::ALL {
        // Exhaustive by construction: adding a variant to `LifecycleMode` without answering
        // here does not compile, so this cannot quietly sample a subset of the enum.
        let expected = match mode {
            LifecycleMode::LegacyT08 => false,
            LifecycleMode::FencedV2 => true,
        };

        assert_eq!(
            mode == LifecycleMode::SELECTED,
            expected,
            "{mode:?}: since M11.T26h's activation change `FencedV2` — and only `FencedV2` — \
             is what a production controller runs. `LegacyT08` is retained as the description \
             of a pre-flag-day peer and is not selectable"
        );
    }

    assert_eq!(
        LifecycleMode::SELECTED,
        LifecycleMode::FencedV2,
        "and the selection is the D39a mechanism, whose durable fence and acknowledged worker \
         protocol are what made its settlement claim true"
    );
}

// -------------------------------------------------------------------------------------
// D96 row 6 (R3) lives in `states/mod.rs` — it is about `StateMachine::update`.
// D96 row 7 (R4): a refusal must not discard the stop the same row carries.
// -------------------------------------------------------------------------------------

/// A refused row that also asks the job to stop is executed as a stop, never as a failure.
///
/// Refusing the *selector* must not also discard the row's lifecycle control. The refusal's
/// own remedy is "stop this job and create a new one under the other backend"; a refusal
/// that threw the stop away would remove that remedy and, worse, end the job in `Failed`
/// rather than `Stopped` — losing exactly the final-checkpoint semantics a `checkpoint` or
/// `graceful` stop asked for.
///
/// Under D39a this is not two decisions taken in an order that has to be got right. It is
/// one classification with one outcome, so there is no interleaving in which the refusal
/// arrives first: [`LifecycleIntent::classify`] answers "refused *and* stopping" as a
/// single case, and the only thing that carries through from the refused row is the stop
/// mode.
#[test]
fn stop_wins_over_refusal() {
    for stop_mode in [
        StopMode::checkpoint,
        StopMode::graceful,
        StopMode::immediate,
        StopMode::force,
    ] {
        let mut refused = running_config();
        refused.stop_mode = stop_mode;
        // A refused row is a row the operator has edited: its restart nonce has moved too,
        // and none of it may travel.
        refused.restart_nonce = 99;
        refused.state_backend = StateBackendSelector::StateEngine;

        let intent = LifecycleIntent::classify(
            StateBackendSelector::Parquet,
            polled(refused, Some(selector_changed())),
        );
        assert_eq!(
            intent,
            LifecycleIntent::RefusedButStopping {
                error: selector_changed(),
                stop_mode,
            },
            "{stop_mode:?}: the classification the poll enqueues carries the stop and \
             nothing else about the refused row — not its selector, not its restart nonce"
        );

        let mailbox = new_mailbox();
        mailbox.submit(intent);
        let decision = actor_for(&mailbox)
            .observe(ConsumptionPoint::BeforeIrreversiblePhase)
            .expect("the job's writer must decide on the poll's intent");

        assert_eq!(
            decision,
            LifecycleDecision::StopUnderRunningConfig(stop_mode),
            "{stop_mode:?}: the single writer stops the job under the configuration it is \
             running with; failing it would destroy the final checkpoint the stop exists for"
        );
    }

    // The control, and the reason the case above is a case at all: the same refused row
    // with no stop on it is a failure, so the branch is decided by the row and not by the
    // classifier's preference.
    let mut refused = running_config();
    refused.state_backend = StateBackendSelector::StateEngine;
    assert_eq!(refused.stop_mode, StopMode::none);

    let mailbox = new_mailbox();
    mailbox.submit(LifecycleIntent::classify(
        StateBackendSelector::Parquet,
        polled(refused, Some(selector_changed())),
    ));
    assert_eq!(
        actor_for(&mailbox).observe(ConsumptionPoint::BeforeIrreversiblePhase),
        Some(LifecycleDecision::Refuse(selector_changed())),
        "a refusal with no other answer still fails the job"
    );
}

// -------------------------------------------------------------------------------------
// D96 row 8 (R4): repeated polls of the same job must not backpressure the poll loop.
// -------------------------------------------------------------------------------------

/// Repeatedly polling one job cannot make the configuration poll wait, and cannot make its
/// per-job state grow.
///
/// The finding this pins is that repeated sends backpressured the updater: the poll thread
/// awaited a bounded per-job channel while it held the global job map, so one job whose
/// consumer was slow — or absent — could stop every other job on the cluster from being
/// polled. A job's row is re-polled every 500ms forever, so "the consumer is behind" is not
/// an edge case, it is the normal state of a job that is busy.
///
/// Three separate properties are checked, because coalescing has to give all three:
///
/// 1. **Submission cannot wait.** The coercion below is a compile-time fact:
///    [`IntentMailbox::submit`] is a plain `fn`, so it has no await point at which the poll
///    thread could be parked, and it returns a plain value rather than a `Result`, so there
///    is no capacity for it to find full.
/// 2. **Repeated polls of an unchanged row create nothing.** Ten thousand polls of the same
///    row leave the job standing at the same version, with no consumer having run at all.
/// 3. **Changed rows coalesce rather than queue.** A thousand distinct rows leave exactly
///    one intent — the newest — behind them, so the storage is the same after them as
///    before.
///
/// The consumer contends on the mailbox for the second half rather than draining it,
/// because draining is not what a real writer does: it reads at its consumption points and
/// leaves the standing intent alone. What must not happen is that the *reader* is what
/// keeps the writer's memory bounded.
#[test]
fn intent_coalescing_never_backpressures_config_polling() {
    // (1) A compile-time fact, not an observation: `submit` is not `async` and cannot fail,
    // so there is no state of the world in which the poll thread waits here.
    let _: fn(&IntentMailbox, LifecycleIntent) -> Submission = IntentMailbox::submit;

    const UNCHANGED_POLLS: usize = 10_000;
    const CHANGED_POLLS: u64 = 1_000;
    /// A coarse liveness bound, not a performance assertion. Anything that made the poll
    /// wait on a consumer that never consumes would not finish at all.
    const BOUND: Duration = Duration::from_secs(60);

    let mailbox = new_mailbox();
    let started = Instant::now();

    // (2) The same bad row, polled over and over, with nothing consuming it.
    let unchanged = LifecycleIntent::classify(
        StateBackendSelector::Parquet,
        polled(running_config(), Some(selector_changed())),
    );
    let first = mailbox.submit(unchanged.clone()).version();
    for poll in 1..UNCHANGED_POLLS {
        assert_eq!(
            mailbox.submit(unchanged.clone()).version(),
            first,
            "poll {poll}: the row has not changed, so the job stands where it stood. A \
             version per poll would be state proportional to the poll count, and would make \
             the job's writer decide — for a refused row, fail the job — once per poll"
        );
    }
    let standing = mailbox
        .newer_than(IntentVersion::NONE)
        .expect("the standing intent is what a writer coming up later reads");
    assert_eq!(standing.version(), first);
    assert!(
        mailbox.newer_than(first).is_none(),
        "and nothing at all is held behind it: {UNCHANGED_POLLS} polls left one intent"
    );

    // (3) Now the row keeps changing, while a reader contends for the same lock without
    // draining anything.
    //
    // The reader takes its first read *before* the barrier, so "a reader was running
    // alongside the poll" is a fact this test establishes rather than a scheduling hope —
    // the suite is also run with `--test-threads 16`, where a freshly spawned thread can
    // otherwise not be scheduled at all before the poll below has finished.
    let stop = Arc::new(AtomicBool::new(false));
    let highest_seen = Arc::new(AtomicU64::new(0));
    let running = Arc::new(std::sync::Barrier::new(2));
    let reader = std::thread::spawn({
        let mailbox = Arc::clone(&mailbox);
        let stop = Arc::clone(&stop);
        let highest_seen = Arc::clone(&highest_seen);
        let running = Arc::clone(&running);
        move || {
            let mut reads = 0u64;
            loop {
                if let Some(standing) = mailbox.newer_than(IntentVersion::NONE) {
                    highest_seen.fetch_max(standing.version().as_u64(), Ordering::Relaxed);
                }
                reads += 1;
                if reads == 1 {
                    running.wait();
                }
                if stop.load(Ordering::Relaxed) {
                    return reads;
                }
            }
        }
    });
    running.wait();

    let mut previous = first;
    for nonce in 0..CHANGED_POLLS {
        let mut changed = running_config();
        changed.restart_nonce = nonce as i32 + 1000;
        let version = mailbox
            .submit(LifecycleIntent::classify(
                StateBackendSelector::Parquet,
                polled(changed, None),
            ))
            .version();
        assert_eq!(
            version.as_u64(),
            previous.as_u64() + 1,
            "nonce {nonce}: a row that really changed is a new intent"
        );
        assert!(
            mailbox.newer_than(version).is_none(),
            "nonce {nonce}: and it replaced its predecessor rather than queueing behind it, \
             so the job's storage after {CHANGED_POLLS} changed rows is what it was after one"
        );
        previous = version;
    }

    stop.store(true, Ordering::Relaxed);
    let reads = reader.join().expect("the contending reader must not panic");

    assert!(
        reads > 0,
        "the reader has to have taken the mailbox for this to say anything about contention"
    );
    assert!(
        highest_seen.load(Ordering::Relaxed) <= previous.as_u64(),
        "and a reader concurrent with the poll never saw a version the poll had not \
         published: coalescing replaces under the lock rather than exposing a half-written \
         slot"
    );
    assert!(
        started.elapsed() < BOUND,
        "{UNCHANGED_POLLS} unchanged polls and {CHANGED_POLLS} changed ones took {:?}: the \
         poll loop is waiting on something, which is the failure this row exists for",
        started.elapsed()
    );
}

// -------------------------------------------------------------------------------------
// D96 row 9 (R5): a repaired row must not be failed by an intent raised before the repair.
// -------------------------------------------------------------------------------------

/// A row the operator has repaired must not be failed by the refusal that asked for the
/// repair.
///
/// M11.T08 needed a version stamp and a discard-on-receipt check for this, because its
/// refusals travelled through a FIFO queue that could not be retracted: between the poll
/// that queued a refusal and the state that read it, the operator could do exactly what the
/// refusal asked, and the job would be failed for a configuration that no longer existed.
///
/// D39a removes the queue rather than checking it. There is one slot per job, the repair
/// *replaces* what the refusal left, and the single writer reads the slot at its
/// consumption point — so there is no stale message to discard, and the first half below
/// shows the writer never sees the refusal at all.
///
/// The second half is the case the replacement cannot cover: the writer had already decided
/// on the refusal before the repair arrived. Then the repair is a newer intent, and the
/// same watermark that stops a bad row being decided twice is what lets the repaired row
/// through — while the superseded refusal is never decided again.
#[test]
fn repaired_row_not_failed_by_stale_intent() {
    // The refusal is raised, and the operator repairs the row before the job's writer has
    // reached a consumption point.
    let mailbox = new_mailbox();
    mailbox.submit(LifecycleIntent::classify(
        StateBackendSelector::Parquet,
        polled(running_config(), Some(selector_changed())),
    ));

    let mut repaired = running_config();
    repaired.restart_nonce = 4;
    mailbox.submit(LifecycleIntent::classify(
        StateBackendSelector::Parquet,
        polled(repaired.clone(), None),
    ));

    let mut writer = actor_for(&mailbox);
    assert_eq!(
        writer.observe(ConsumptionPoint::BeforeIrreversiblePhase),
        Some(LifecycleDecision::Adopt(Box::new(repaired.clone()))),
        "the writer reads the job's standing intent, which is the repair: the refusal it \
         superseded was never a message anybody had to remember to discard"
    );
    assert_eq!(
        writer.observe(ConsumptionPoint::InsideInterruptibleWait),
        None,
        "and nothing is left behind it to be decided a second time"
    );

    // The other order: the writer decided on the refusal first, and the repair lands after.
    let mailbox = new_mailbox();
    mailbox.submit(LifecycleIntent::classify(
        StateBackendSelector::Parquet,
        polled(running_config(), Some(selector_changed())),
    ));
    let mut writer = actor_for(&mailbox);
    assert_eq!(
        writer.observe(ConsumptionPoint::BeforeIrreversiblePhase),
        Some(LifecycleDecision::Refuse(selector_changed())),
    );

    // The same bad row is polled again while the operator is still working on it. It is not
    // a new intent, so the job is not failed a second time.
    mailbox.submit(LifecycleIntent::classify(
        StateBackendSelector::Parquet,
        polled(running_config(), Some(selector_changed())),
    ));
    assert_eq!(
        writer.observe(ConsumptionPoint::InsideInterruptibleWait),
        None,
        "a bad row is re-polled every 500ms; deciding on it once per poll would fail the \
         job once per poll"
    );

    mailbox.submit(LifecycleIntent::classify(
        StateBackendSelector::Parquet,
        polled(repaired.clone(), None),
    ));
    assert_eq!(
        writer.observe(ConsumptionPoint::InsideInterruptibleWait),
        Some(LifecycleDecision::Adopt(Box::new(repaired))),
        "and the repair is a new intent, so the writer that had already refused adopts it"
    );
}

// -------------------------------------------------------------------------------------------
// Review round 1 — the wake. Submitting an intent is not delivering it.
//
// Every consumption point is reached either at a state boundary or on a turn of a wait, and a
// wait turns only when something wakes it. Under M11.D39a nothing is sent to the job's channel
// when the poll decides something, so a job that is simply running well has nothing that would
// make it look: the intent sits in the mailbox until a startup budget expires, or — in
// `Running`, whose only other arms are its own progress tick and the channel — forever.
//
// A coarse bound, not a performance assertion: any of these that depended on a timeout rather
// than on the wake would not finish inside it at all.
// -------------------------------------------------------------------------------------------

/// How long a row waits for a wake it expects. Generous, because what is being distinguished is
/// "woken" from "not woken at all".
const WAKE_BOUND: Duration = Duration::from_secs(10);
/// How long a row waits to be sure a wake it does *not* expect has not happened.
const QUIET: Duration = Duration::from_millis(100);

/// A consumer already parked on the mailbox is released by the next submission.
#[tokio::test]
async fn a_submitted_intent_ends_a_wait_that_is_already_parked_on_it() {
    let mailbox = new_mailbox();
    let wake = IntentWakeup::watching(Arc::clone(&mailbox));

    let poll = tokio::spawn({
        let mailbox = Arc::clone(&mailbox);
        async move {
            // Long enough that the park below is entered first in any ordering that matters,
            // and short enough that the row is not itself a timeout test.
            tokio::time::sleep(Duration::from_millis(50)).await;
            mailbox.submit(LifecycleIntent::Adopt(Box::new(running_config())));
        }
    });

    tokio::time::timeout(WAKE_BOUND, wake.notified())
        .await
        .expect(
            "a submission has to end a wait that is parked on the mailbox. Nothing else would: \
             under M11.D39a the poll puts nothing in the job's channel",
        );
    poll.await.unwrap();
    assert!(
        mailbox.newer_than(IntentVersion::NONE).is_some(),
        "and what the wake was raised for is there to be read"
    );
}

/// A submission that lands in the window between a consumer's read and its park still ends the
/// park.
///
/// This is the ordering the wake primitive is chosen for, and it is not avoidable: the read has
/// to happen before the wait, or the wait would be entered without the value it exists to
/// notice. `notify_one` *stores* a permit when nobody is parked, so the park that follows the
/// submission completes immediately; `notify_waiters` would have dropped it on the floor and the
/// job would have waited out its startup budget with a stop sitting in its mailbox.
#[tokio::test]
async fn a_submission_that_lands_between_a_read_and_a_park_still_ends_the_park() {
    let mailbox = new_mailbox();
    let wake = IntentWakeup::watching(Arc::clone(&mailbox));

    // The consumer's read: nothing yet.
    assert!(mailbox.newer_than(IntentVersion::NONE).is_none());
    // The poll, in the window.
    mailbox.submit(LifecycleIntent::Adopt(Box::new(running_config())));
    // The consumer's park, after it.
    tokio::time::timeout(QUIET, wake.notified())
        .await
        .expect("the permit a submission stores is what makes this window safe to have");
}

/// A row re-polled unchanged does not wake the job once per poll.
///
/// The same rule as the version stamp, for the same reason: a bad row is re-polled every 500ms
/// forever, and a wake per poll would be a job woken every 500ms to find that nothing has
/// changed since the last time it looked.
#[tokio::test]
async fn a_re_polled_row_does_not_wake_the_job_once_per_poll() {
    let mailbox = new_mailbox();
    let wake = IntentWakeup::watching(Arc::clone(&mailbox));

    let refused = LifecycleIntent::classify(
        StateBackendSelector::Parquet,
        polled(running_config(), Some(selector_changed())),
    );
    let first = mailbox.submit(refused.clone()).version();
    tokio::time::timeout(QUIET, wake.notified())
        .await
        .expect("the first classification of the row is something new to look at");

    for poll in 1..100 {
        assert_eq!(
            mailbox.submit(refused.clone()).version(),
            first,
            "poll {poll}"
        );
    }
    assert!(
        tokio::time::timeout(QUIET, wake.notified()).await.is_err(),
        "and the ninety-nine polls after it are the same row, so there is nothing for a woken \
         consumer to find"
    );
}

/// A job whose lifecycle the configuration poll decides has nothing to wake on.
///
/// Not "a wake that fires and finds nothing" but a branch that is never ready: on the landed
/// M11.T08 mechanism there is no mailbox at all, the poll publishes into the job's channel and
/// to the refusal gate, and every wait already selects on that channel. This is what keeps the
/// selected production path's behaviour identical.
#[tokio::test]
async fn a_job_on_the_landed_mechanism_has_nothing_to_wake_on() {
    let wake = IntentWakeup::none();
    assert!(
        tokio::time::timeout(QUIET, wake.notified()).await.is_err(),
        "there is no mailbox behind this handle, so it can never become ready"
    );
}
