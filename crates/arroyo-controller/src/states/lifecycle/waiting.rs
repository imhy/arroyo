//! The one thing a job's state task parks on while it waits (M11.T25, design M11.D39a).
//!
//! # The question this module exists to make unforgettable
//!
//! D39a fixes two points at which a job's single writer is read: "before entering any
//! irreversible phase and inside every interruptible wait". The first is the state boundary,
//! and [`State::leave_for_stop`](crate::states::State::leave_for_stop) makes it impossible for
//! a state to skip. The second had no such forcing function: a wait was a `recv` on the job's
//! message channel, and whether it also watched the writer was up to whoever wrote it.
//!
//! A stop decided under [`LifecycleMode::FencedV2`](super::LifecycleMode::FencedV2) is *not*
//! sent to that channel. The configuration poll's entire contribution is
//! [`IntentMailbox::submit`](super::intent::IntentMailbox::submit), so a wait that watches only
//! the channel waits for a message that will never be sent — and a wait with no deadline waits
//! for it forever, which is a job an operator cannot stop.
//!
//! So there is one wait, and it is this one. [`JobWait::recv`] reads the writer before it parks
//! and again on the turn a submission ends, and it is the only way to take a message off the
//! job's channel from outside [`crate::states`]: [`JobController`](crate::job_controller::JobController)
//! cannot be handed a bare [`Receiver`] any more, so a wait cannot be written against half the
//! sources a stop can arrive on. That is the same argument `leave_for_stop` makes one level up
//! — a wait that had to remember to watch the mailbox would be a wait that could forget.
//!
//! # What this is not
//!
//! It is not a second decider. Everything it reads it reads through
//! [`LifecycleActor::observe`], and everything that decision does it does through
//! [`LifecycleDecision::apply`](super::actor::LifecycleDecision::apply) — the same one place
//! that publishes at every other consumption point. And it decides nothing about what a stop
//! *means*: [`Waited::Decided`] carries the fact, and the caller answers it with its own
//! family's landed `stop_if_desired*` mapping, so a stop that arrives as an intent and a stop
//! that arrives as a [`JobMessage::ConfigUpdate`] cannot come to mean different things.
//!
//! Under [`LifecycleMode::LegacyT08`](super::LifecycleMode::LegacyT08) — production through
//! M11.T25 — there is no actor and no mailbox. [`JobWait::decide`] answers `None` always,
//! [`IntentWakeup::none`] never completes, and every `recv` is the channel receive it always
//! was.

use tokio::sync::mpsc::Receiver;

use super::actor::{ConsumptionPoint, LifecycleActor, ObservedIntent};
use super::intent::IntentWakeup;
use crate::states::StateError;
use crate::{JobConfig, JobMessage};

/// Why a [`JobWait`] stopped waiting.
///
/// `#[must_use]` because [`Self::Decided`] is the one outcome that cannot be re-read: observing
/// an intent advances the writer's watermark, so whatever discards this is the last thing that
/// could act on the stop it carries. That is the same rule
/// [`ObservedIntent`](super::actor::ObservedIntent) is marked with, restated at the level a
/// caller actually holds.
#[derive(Debug)]
#[must_use = "a wait that ended because the job's writer decided something is the last place \
              that decision can be acted on"]
// `JobMessage` is 304 bytes and the other three variants are a byte, which is what
// `large_enum_variant` reports. Boxing it would be backwards here: a message is the *common*
// outcome — every worker heartbeat of every running job arrives as one — so the box would be an
// allocation per message on a job's hot path, bought to shrink an enum that is only ever a local
// on the stack of the wait that produced it. `LifecycleIntent` boxes its large variant for the
// opposite reason: there the large one is the rare one.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Waited {
    /// A message arrived on the job's channel. Every mechanism that predates M11.T25 delivers
    /// here, including every stop on the selected `LegacyT08` path.
    Message(JobMessage),
    /// The job's single writer decided something, and it has been published into the job's
    /// configuration. The caller answers it; this does not.
    Decided(ObservedIntent),
    /// The wait was woken by a submission that had already been decided on — the permit an
    /// earlier turn left behind. Nothing to do but park again.
    Woken,
    /// The job's channel closed. The job's task is going away.
    Closed,
}

/// Everything a job's state task can wait on, held together so that no wait can watch only
/// one of them.
///
/// Built in exactly one place — [`JobContext::controller_and_wait`](crate::states::JobContext::controller_and_wait)
/// — which is what `the_jobs_wait_is_assembled_in_one_place` pins. The fields are private and
/// the constructor is visible only inside [`crate::states`], so a caller outside cannot
/// assemble one from a subset.
pub(crate) struct JobWait<'a> {
    /// The job's message channel: the M11.T08 delivery path, and the only path a running
    /// worker's messages ever take.
    rx: &'a mut Receiver<JobMessage>,
    /// The job's single writer, or `None` for a job on the landed mechanism.
    lifecycle: Option<&'a mut LifecycleActor>,
    /// Where a decision is published. This is [`JobContext::config`](crate::states::JobContext::config)
    /// itself, borrowed rather than copied, because a decision published into a copy is a
    /// decision the state that reads the original never sees.
    config: &'a mut JobConfig,
    /// The job's pipeline, for the log a refusal has to leave behind.
    pipeline_id: &'a str,
}

impl<'a> JobWait<'a> {
    /// The wait for one job, from every source a decision about it can arrive on.
    ///
    /// `pub(in crate::states)` deliberately: [`crate::job_controller`] takes a `JobWait` and
    /// can no longer take a bare [`Receiver`], but it also cannot build one, so the set of
    /// sources a wait observes is not something a wait's author chooses.
    pub(in crate::states) fn new(
        rx: &'a mut Receiver<JobMessage>,
        lifecycle: Option<&'a mut LifecycleActor>,
        config: &'a mut JobConfig,
        pipeline_id: &'a str,
    ) -> Self {
        Self {
            rx,
            lifecycle,
            config,
            pipeline_id,
        }
    }

    /// The job's configuration, including anything this wait has published into it.
    ///
    /// The caller reads it to answer a [`Waited::Decided`] with its own family's mapping. It is
    /// the same value [`JobContext::config`](crate::states::JobContext::config) holds, so a
    /// state that reads it after the wait reads what the wait wrote.
    pub(crate) fn config(&self) -> &JobConfig {
        self.config
    }

    /// The job's pipeline, for a log that has to name it.
    ///
    /// Held because the states that wait cannot also borrow their context to read it: the wait
    /// has the job's channel and its configuration, and the controller it is waiting on is the
    /// other half of the same split.
    pub(crate) fn pipeline_id(&self) -> &str {
        self.pipeline_id
    }

    /// Reads the job's single writer, and publishes whatever it decided.
    ///
    /// `None` is "the writer has said nothing new"; `Some` is a decision that has been applied,
    /// and what the job's configuration says as a result. Always `None` for a job on the landed
    /// M11.T08 mechanism, which has no writer at all.
    ///
    /// # Errors
    ///
    /// The fatal [`StateError`] of a refused configuration, from whatever state is waiting —
    /// the same outcome the M11.T08 path reaches through
    /// [`handle_unhandled_message`](crate::states::handle_unhandled_message). A refusal leaves
    /// here as an `Err` and never as a value a caller could carry on past.
    pub(crate) fn decide(
        &mut self,
        at: ConsumptionPoint,
    ) -> Result<Option<ObservedIntent>, StateError> {
        let Some(decision) = self.lifecycle.as_mut().and_then(|actor| actor.observe(at)) else {
            return Ok(None);
        };
        decision.apply(self.config, self.pipeline_id).map(Some)
    }

    /// Waits for the next thing that happens to this job, from either source.
    ///
    /// The writer is read **before** this parks, not only after: a decision taken while the
    /// caller was doing something else — including one the state boundary left standing for
    /// this state to answer — is reported before the wait blocks on anything at all. Reading it
    /// only after parking is how an unbounded wait outlives the stop that was meant to end it.
    ///
    /// It is read again on the turn a submission causes, rather than being left to whatever the
    /// channel happens to deliver: a wait ends on the message that made its count, not on the
    /// last message in the queue, and under `FencedV2` no message is sent for a lifecycle
    /// decision at all.
    ///
    /// # Errors
    ///
    /// As [`Self::decide`].
    pub(crate) async fn recv(&mut self, at: ConsumptionPoint) -> Result<Waited, StateError> {
        if let Some(observed) = self.decide(at)? {
            return Ok(Waited::Decided(observed));
        }

        // Never ready for a job on the landed M11.T08 mechanism — see [`IntentWakeup`]. Taken
        // as an owned handle before the select, because the channel arm borrows this wait
        // mutably.
        let wake = match &self.lifecycle {
            Some(actor) => actor.wakeup(),
            None => IntentWakeup::none(),
        };

        // `Some` is the channel's answer, `None` is "the writer submitted something". The
        // select produces a value rather than acting, so that the branch which has to read the
        // writer can borrow this wait mutably once the channel arm's borrow has ended.
        let received = tokio::select! {
            msg = self.rx.recv() => Some(msg),
            () = wake.notified() => None,
        };

        match received {
            Some(Some(msg)) => Ok(Waited::Message(msg)),
            Some(None) => Ok(Waited::Closed),
            None => Ok(match self.decide(at)? {
                Some(observed) => Waited::Decided(observed),
                // A permit an earlier turn of this wait already decided on. The mailbox holds
                // one intent, so this happens at most once per submission and never spins.
                None => Waited::Woken,
            }),
        }
    }
}
