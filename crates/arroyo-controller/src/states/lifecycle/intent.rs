//! The versioned intent the configuration poll leaves for a job's single writer
//! (M11.T25a, design M11.D39a).
//!
//! Under [`LifecycleMode::FencedV2`](super::LifecycleMode::FencedV2) the configuration-
//! update thread does not decide anything about a job. It validates and classifies the
//! polled row — before it replaces any in-memory execution baseline and before it writes
//! any lifecycle status — and leaves a [`LifecycleIntent`] in the job's [`IntentMailbox`].
//! The job's own state task is what reads it, decides, and publishes.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Notify;

use arroyo_rpc::state_backend::{
    StateBackendError, StateBackendSelector, validate_unchanged_job_selector,
};
use tracing::debug;

use crate::types::public::StopMode;
use crate::{JobConfig, PolledJob};

/// How many distinct things the poll has said about a job, counting from zero.
///
/// A version is advanced only when the poll says something *different* from what the
/// mailbox already holds. That is what makes a bad row — which is re-polled every 500ms
/// until an operator repairs it — one intent rather than a stream of them, and it is the
/// direct descendant of M11.T08's rule that a `Pending` refusal retried at the same
/// version is the same refusal rather than a new one.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct IntentVersion(u64);

impl IntentVersion {
    /// The version an actor holds before it has decided on anything.
    ///
    /// No intent is ever submitted at this version — [`IntentMailbox::submit`] pre-
    /// increments — so an actor starting from here observes whatever the mailbox holds,
    /// including an intent enqueued before its state task existed.
    pub(crate) const NONE: Self = Self(0);

    /// The raw counter, for logs and for tests that assert a version was or was not
    /// advanced.
    pub(crate) fn as_u64(self) -> u64 {
        self.0
    }
}

/// What one polled `job_configs` row means for a job, after classification.
///
/// This is the *whole* of what crosses from the configuration poll to the job's state
/// task. In particular a refused row does not cross: refusing by not carrying the row
/// forward, rather than by carrying it forward and failing afterwards, is what keeps a
/// refused selector and a refused restart nonce out of every downstream baseline.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum LifecycleIntent {
    /// The row was accepted. This configuration becomes the job's baseline — but only
    /// when the job's own state task adopts it, never on the poll thread.
    ///
    /// Boxed because a `JobConfig` is ~304 bytes and the other two variants are a typed
    /// error and a stop mode. Unboxed, every value of this type — including the refusals,
    /// which is what a job in trouble produces — would be sized for the accepted row it is
    /// not. The indirection is paid once per *distinct* polled row, which
    /// [`IntentMailbox::submit`]'s coalescing already makes the rare case.
    Adopt(Box<JobConfig>),
    /// The row's `state_backend` was refused *and* the same row asks the job to stop.
    ///
    /// The stop wins. Refusing the selector must not also discard the row's lifecycle
    /// control: the refusal's remedy is "stop this job and create a new one under the
    /// other backend", and a refusal that threw the stop away would both remove that
    /// remedy and end the job in `Failed` rather than `Stopped`, losing exactly the
    /// final-checkpoint semantics a `checkpoint` or `graceful` stop asked for.
    ///
    /// Only the stop mode is carried. Everything else about the refused row — its
    /// selector, its restart nonce — is discarded here, so the stop is executed on top of
    /// the configuration the job's workers, table configs and checkpoints were built from.
    RefusedButStopping {
        /// Why the row was refused, for the log the operator has to act on.
        error: StateBackendError,
        /// The one field of the refused row that survives classification.
        stop_mode: StopMode,
    },
    /// The row's `state_backend` was refused and nothing about it is adopted: the job is
    /// failed by whichever state its task is in when the intent is observed.
    Refused(StateBackendError),
}

impl LifecycleIntent {
    /// Classifies one polled row against the job's immutable execution selector.
    ///
    /// Called on the configuration-update thread, and deliberately the *first* thing that
    /// happens to a polled row: nothing has been replaced, published, or written when this
    /// runs, so a row that turns out to be refused leaves the job exactly as it was. That
    /// ordering is the finding this exists for — a configuration adopted as the execution
    /// baseline before it was classified cannot be un-adopted by classifying it later.
    ///
    /// [`crate::classify_polled_row`] has already resolved the row against the job's own
    /// durable execution record, so `polled.config` carries the selector the job is
    /// actually running with and `polled.refusal` says why the row's own value was
    /// rejected. The re-validation below is defence in depth against that ever stopping
    /// being true, and it fails closed: a disagreement it finds becomes a refusal, never a
    /// silently adopted row.
    pub(crate) fn classify(execution_selector: StateBackendSelector, polled: PolledJob) -> Self {
        let PolledJob {
            config, refusal, ..
        } = polled;

        let refusal = refusal.or_else(|| {
            validate_unchanged_job_selector(&config.id, execution_selector, config.state_backend)
                .err()
        });

        match refusal {
            None => LifecycleIntent::Adopt(Box::new(config)),
            // Checked before the plain refusal below, which is the whole of "a stop wins
            // over a refusal": the two are not independent decisions taken in some order,
            // they are one classification with the stop as its outcome.
            Some(error) if config.stop_mode != StopMode::none => {
                LifecycleIntent::RefusedButStopping {
                    error,
                    stop_mode: config.stop_mode,
                }
            }
            Some(error) => LifecycleIntent::Refused(error),
        }
    }

    /// Whether a job with no state task has to be given one before anything can act on this.
    ///
    /// Asked by the configuration poll, and it is a question about the mailbox rather than a
    /// decision about the job: what the intent *means* is still the actor's, and nothing here
    /// reads a field of a configuration or names a state. A job with no task has no actor, so
    /// an intent left for one is decided by nobody until a task exists — and the poll thread
    /// is the only thing that can arrange for one.
    ///
    /// # The answers are the landed mechanism's, one per classification
    ///
    /// This enum *is* the branch M11.T08's
    /// [`StateMachine::update`](crate::states::StateMachine) takes on the same row, so the two
    /// paths cannot be allowed to answer it differently, and the answers below are read off
    /// that one rather than invented:
    ///
    /// | classification | what the landed path does with a job that has no task |
    /// |---|---|
    /// | [`Self::Adopt`] | stores the row and starts a task for it — an accepted row is what a job's next transition is decided from |
    /// | [`Self::RefusedButStopping`] | `request_stop` stores the stop and starts a task for it — a job with no task can be told nothing, and the stop is the refusal's whole remedy |
    /// | [`Self::Refused`] | `restart_if_needed`, and nothing more: a job that has legitimately reached a terminal state is not woken up to be failed again |
    ///
    /// So the `false` side is exactly [`Self::Refused`], and dropping either of the other two
    /// would take a landed behaviour away from the mechanism that replaces it. In particular
    /// this is deliberately *wider* than "could make the job runnable", which only
    /// [`Self::Adopt`] can: a stop cannot start a job, but it still has to reach one.
    ///
    /// # What a task started here may then do
    ///
    /// Nothing this decides. A job's configuration is the only input any state consults to
    /// decide whether it runs — [`Stopped`](crate::states::Stopped) restarts when `stop_mode`
    /// is [`StopMode::none`] and no `ttl` is set, [`Failed`](crate::states::Failed) when
    /// `restart_nonce` has moved past the one its status records, and a running state restarts
    /// or rescales by comparing an adopted configuration with the one it replaced — and only
    /// [`Self::Adopt`] replaces that configuration. *Which* of those fields matters is the
    /// state's question and deliberately not asked here: a poll that re-implemented
    /// `Stopped::next` would be deciding, and would be wrong the moment a state read one more
    /// field. A task started for a stop therefore re-terminates rather than resurrects, which
    /// is the same thing `request_stop`'s restart has always done.
    pub(crate) fn needs_a_state_task(&self) -> bool {
        match self {
            LifecycleIntent::Adopt(_) | LifecycleIntent::RefusedButStopping { .. } => true,
            LifecycleIntent::Refused(_) => false,
        }
    }
}

/// What one [`IntentMailbox::submit`] leaves the configuration poll to do.
///
/// The poll's whole contribution under
/// [`LifecycleMode::FencedV2`](super::LifecycleMode::FencedV2) is to leave a value and, when
/// the job has no state task, to start one so that something exists to decide it. Both halves
/// of the second question — is this intent new, and does it need a state task at all — are
/// answered here rather than at the call site, and the first of them is answered *inside* the
/// mailbox's lock, so there is no window in which the poll could read a version that a later
/// submission has already superseded.
///
/// Deliberately not `#[must_use]`. [`StateMachine::new`](crate::states::StateMachine) has no
/// use for the answer — it starts the job's task whatever the row said — and the thing that
/// stops a *third* submission path forgetting to ask is not an attribute a call site can
/// silence with `let _`, it is
/// `every_mailbox_submission_path_can_get_the_job_a_state_task`, which counts the sites in the
/// source and makes each declare what it does.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct Submission {
    version: IntentVersion,
    needs_a_state_task: bool,
}

impl Submission {
    /// The version the job now stands at — the newest one the mailbox has used, whether this
    /// submission stamped it or found it already there.
    pub(crate) fn version(self) -> IntentVersion {
        self.version
    }

    /// Whether a job with no state task has to be given one before anything can decide this.
    ///
    /// True exactly when this submission stamped a version the mailbox had not used before
    /// **and** the intent it stamped
    /// [needs a state task](LifecycleIntent::needs_a_state_task).
    ///
    /// The first conjunct is what keeps a job that is polled every 500ms from being started
    /// every 500ms: a row that has not changed is the *same* intent, so it is coalesced, so it
    /// asks for nothing. A task started for an intent and then ended by the state that read it
    /// — a `Stopped` job whose new configuration still asks it to stay stopped — is therefore
    /// started once, not once per poll.
    pub(crate) fn needs_a_state_task(self) -> bool {
        self.needs_a_state_task
    }
}

/// A [`LifecycleIntent`] together with the version the mailbox stamped it at.
#[derive(Clone, Debug)]
pub(crate) struct VersionedIntent {
    version: IntentVersion,
    intent: LifecycleIntent,
}

impl VersionedIntent {
    /// The version this intent was stamped at.
    pub(crate) fn version(&self) -> IntentVersion {
        self.version
    }

    /// The intent itself.
    pub(crate) fn into_intent(self) -> LifecycleIntent {
        self.intent
    }
}

/// The one intent a job's configuration poll has left for the job's state task.
///
/// # One slot, not a queue
///
/// A job's row is re-polled every 500ms, forever. Anything that *queued* per poll would
/// therefore be unbounded in the one case that matters — a job whose state task is slow,
/// blocked, or has not been started yet — and M11.T08's review round 4 found exactly that
/// failure in its predecessor: the update thread awaited a bounded per-job channel while
/// it held the global job map, so one slow consumer could stop every other job on the
/// cluster from being polled.
///
/// So this holds exactly one intent. A newer classification *replaces* an older one rather
/// than queueing behind it, because the newest poll is by definition the current truth
/// about the row, and a decision the job has not taken yet should be taken on the current
/// truth. That is coalescing, and it is what makes the storage per job constant no matter
/// how long a consumer is away.
///
/// # Why the poll can never wait here
///
/// [`Self::submit`] is a plain `fn`. It cannot await, so it cannot await the job's
/// consumer; there is no capacity for it to find full, so it cannot fail; and the only
/// lock it takes is held across an equality comparison and a move, with no allocation, no
/// user code and no I/O inside it. The update thread calls it while it holds the global
/// job map, which is the same constraint that keeps
/// [`StateMachine::refuse_config`](crate::states::StateMachine::refuse_config)
/// non-`async` on the M11.T08 path.
///
/// # Superseding, without anything to retract
///
/// M11.T08 needed a version stamp because its refusals travelled through a FIFO queue that
/// could not be retracted: between the poll that raised a refusal and the state that read
/// it, the operator could repair the row — the very remedy the refusal asked for — and the
/// queued message would fail the job for a configuration that no longer existed. Here the
/// repair simply replaces the slot, so there is nothing stale left to discard. The version
/// remains, for the other half of that rule: it is the watermark that tells the job's
/// single writer which intents it has already decided on, so a row that stays bad across
/// hundreds of polls is decided once rather than once per poll.
pub(crate) struct IntentMailbox {
    /// The job this mailbox belongs to, for the log lines below. Per job and owned by that
    /// job's state machine; there is no registry, static or global.
    job_id: Arc<String>,
    /// The newest classified intent, or `None` if the poll has never had anything to say.
    ///
    /// A `std::sync::Mutex` rather than a `tokio` one precisely because nothing may await
    /// while it is held: making that impossible is better than remembering it. The
    /// critical section is an equality comparison and a move.
    slot: Mutex<Option<VersionedIntent>>,
    /// The version of the newest intent ever submitted. Monotone, never reset.
    ///
    /// Advanced by [`Self::submit`], under the slot lock, so its ordering against the slot's
    /// contents is the lock's. It is an atomic rather than a plain `u64` because it lives
    /// outside the [`Mutex`] and `submit` takes `&self`.
    published: AtomicU64,
    /// Raised whenever [`Self::submit`] stamps a *new* version.
    ///
    /// Submitting a value is not delivering it. Every consumption point in the job's state
    /// task is reached either at a state boundary or on a turn of a wait, and a wait turns
    /// only when something wakes it — a worker message, a startup timeout, a progress tick.
    /// Without this, an intent left for a job that is simply running well is waited on by
    /// nothing at all: the poll thread stores it and the job's writer has no reason to look
    /// again. So the poll raises this, and every interruptible wait selects on it alongside
    /// whatever else it was waiting for.
    ///
    /// [`Notify::notify_one`] rather than `notify_waiters`, deliberately: it *stores* a
    /// permit when nobody is parked, so a submission that lands in the window between a
    /// consumer's read and its park is delivered by the park rather than lost in it. That
    /// window is unavoidable — the read has to happen before the wait, or the wait would be
    /// entered without the value it exists to notice — so the primitive has to be one that
    /// survives it.
    wake: Notify,
}

impl IntentMailbox {
    /// The mailbox for one job.
    pub(crate) fn new(job_id: Arc<String>) -> Self {
        Self {
            job_id,
            slot: Mutex::new(None),
            published: AtomicU64::new(IntentVersion::NONE.0),
            wake: Notify::new(),
        }
    }

    /// Leaves a classified intent for the job's state task, and says what the poll still has
    /// to do about it.
    ///
    /// Never waits, never fails, and never grows: see the type documentation. An intent
    /// equal to the one already held is *not* a new intent — it is the same row, polled
    /// again — so the version is left alone and the job's writer does not decide on it a
    /// second time. [`Submission::version`] is therefore also the answer to "how many distinct
    /// things has the poll said about this job", which is the quantity that must stay
    /// bounded however many times the row is polled.
    ///
    /// [`Submission::needs_a_state_task`] is the other half, and it is answered here because
    /// both of its conjuncts are: whether the version advanced is known only under this lock,
    /// and whether the intent needs a task at all is the intent's own. A caller that had to
    /// combine them would be a caller that could combine them wrongly.
    ///
    /// A submission that stamps a new version also *wakes* the job's writer — see
    /// [`wake`](Self::wake). The wake is raised outside the lock, so the critical section
    /// stays exactly what the type documentation claims it is: an equality comparison and a
    /// move. A coalesced re-submission raises nothing, because there is nothing new for a
    /// woken consumer to find and a wake per poll would be a wake per 500ms forever.
    pub(crate) fn submit(&self, intent: LifecycleIntent) -> Submission {
        let submission = {
            let mut slot = self.slot.lock().unwrap();

            if let Some(held) = slot.as_ref()
                && held.intent == intent
            {
                // The row has not changed since the last poll. Re-stamping it would
                // supersede an intent the writer has already decided on and make it decide
                // again — which for a refused row means failing the job once per 500ms poll.
                // It asks for no state task either, for the same reason: a job started for
                // this intent once must not be started for it again on the next poll. This is
                // the mailbox's version of the two guards the landed path uses for the same
                // purpose — an unchanged row takes `update`'s `restart_if_needed` branch, and
                // an already-stored stop returns at `request_stop`'s head.
                return Submission {
                    version: held.version,
                    needs_a_state_task: false,
                };
            }

            let version = IntentVersion(self.published.fetch_add(1, Ordering::SeqCst) + 1);
            debug!(
                job_id = %self.job_id,
                version = version.as_u64(),
                intent = ?intent,
                "the job's configuration poll left a lifecycle intent for its state task"
            );
            let needs_a_state_task = intent.needs_a_state_task();
            // Replaces rather than appends. Whatever the previous poll said is dropped here,
            // which is the coalescing this type exists for.
            *slot = Some(VersionedIntent { version, intent });
            Submission {
                version,
                needs_a_state_task,
            }
        };
        self.wake.notify_one();
        submission
    }

    /// Completes when the poll has submitted something at a version it had not used before.
    ///
    /// A permit stored by a submission that found nobody parked completes this immediately,
    /// which is the whole point: what a consumption point must not do is miss an intent
    /// submitted in the instant between its read and its park.
    ///
    /// A spurious completion — a permit left by an intent the caller has already decided on
    /// — costs one more turn of the caller's loop and nothing else, because deciding is what
    /// [`LifecycleActor::observe`](super::LifecycleActor::observe) does, not this.
    async fn notified(&self) {
        self.wake.notified().await;
    }

    /// The intent the job stands under, if the caller has not already decided on it.
    ///
    /// The version is compared *inside* the lock and before anything is cloned, so a
    /// consumption point that has nothing new to read costs a lock and a comparison. That
    /// matters because the single writer reads this at every state boundary and on every
    /// turn of every interruptible wait, and none of those may be proportional to the
    /// number of polls that have happened.
    pub(crate) fn newer_than(&self, decided: IntentVersion) -> Option<VersionedIntent> {
        let slot = self.slot.lock().unwrap();
        let held = slot.as_ref()?;
        (held.version > decided).then(|| held.clone())
    }
}

/// The thing an interruptible wait parks on so that a submitted intent ends it.
///
/// Owned rather than borrowed, because the waits that select on it hold the job's context
/// mutably for the other arm of the same `select!`: a handle that borrowed the context could
/// not sit beside `ctx.rx.recv()`.
///
/// # Why the `LegacyT08` arm is a future that never completes
///
/// A job on the landed mechanism has no mailbox at all — the poll thread publishes to the
/// [`RefusalGate`](crate::states::RefusalGate) and to the job's message queue, and every wait
/// already selects on that queue. So there is nothing to wake *for*, and the honest way to say
/// so is an arm that is never ready rather than one that fires and finds nothing. The selected
/// production path therefore behaves exactly as it did: the branch is polled once per turn,
/// returns `Pending`, and is never woken.
pub(crate) struct IntentWakeup(Option<Arc<IntentMailbox>>);

impl IntentWakeup {
    /// The handle for a job whose lifecycle transitions the configuration poll decides.
    ///
    /// There is no mailbox to wake from, so this never completes.
    pub(crate) fn none() -> Self {
        Self(None)
    }

    /// The handle for a job whose lifecycle transitions its own state task decides.
    pub(crate) fn watching(mailbox: Arc<IntentMailbox>) -> Self {
        Self(Some(mailbox))
    }

    /// Completes when the job's poll has left something new, and never otherwise.
    ///
    /// Safe to create and drop once per turn of a wait: the permit a submission stores lives
    /// on the mailbox rather than on this future, so a turn that ended for another reason does
    /// not consume it — and a turn that *did* consume it is a turn whose loop reads the mailbox
    /// before it parks again.
    pub(crate) async fn notified(&self) {
        match &self.0 {
            Some(mailbox) => mailbox.notified().await,
            None => std::future::pending().await,
        }
    }
}
