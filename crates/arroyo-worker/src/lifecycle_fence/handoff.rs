//! The operator handoff a commit needs, reserved before the decision that publishes it
//! (M11.T26d/T26e, design M11.D39d; PR #167 round 9).
//!
//! # The gap this closes
//!
//! `WorkerGrpc::commit` used to admit a commit under the lifecycle lock, clone the operator
//! control senders, release the lock, and only then `await` each bounded send. Between the
//! release and the last send a `FENCE_ONLY` could take the lock, advance and acknowledge a newer
//! fence, and let the replacement controller settle this generation and publish `Refused` — after
//! which the operators freed capacity and the stale commit was enqueued anyway, in whole or, with
//! several senders, in part. That is the validate→apply gap M11.D39d forbids, on the one directive
//! whose effect is named in M11.D39's invariant: *"commit publication"* must not *"become
//! committed/restorable"* after the refusal's linearization point.
//!
//! # The shape of the fix, and why it is this one
//!
//! The awaiting part of publication — waiting for capacity on every operator channel — is moved
//! **in front of** the decision, as a reservation that decides nothing: [`CommitHandoffPlan`]
//! is resolved under the lock from the phase and the request's operator ids, and
//! [`CommitHandoffPlan::reserve`] then holds an [`OwnedPermit`] on every channel with the lock
//! released. What is left for the guard is admission followed by a publication that cannot
//! block, and [`WorkerLifecycle::publish_commit`](super::guard::WorkerLifecycle::publish_commit)
//! does both in one synchronous call under the lock — so a fence acknowledgement linearizes
//! entirely before every operator hears the commit, or entirely after none of them does.
//!
//! The other way round — holding fence acknowledgement back while admitted commits are in
//! flight — was rejected because it couples the control plane to data-plane backpressure: a
//! full operator channel would then stall a replacement controller's settlement of this
//! generation, and D39's *"a permanent partition may hold one job in `Fencing`"* would acquire a
//! second cause with no fence semantics at all. Under this design the acknowledgement never
//! waits for an operator, and a commit never decides before it holds the capacity to act.
//!
//! # What a reservation certifies, and what it does not
//!
//! A reservation is capacity on the channels the phase *had* when it was planned. The guard does
//! not trust that the phase still has them: [`CommitReservation::take`] hands the permits back
//! only for channels that are, by [`Sender::same_channel`], the ones the current execution
//! publishes to, and refuses otherwise — a commit reserved against one execution is not
//! published into another. Every operator's handoff is validated before any operator's is
//! used, so a refusal there publishes nothing rather than something.
//!
//! Cancellation is the easy path: a handler future dropped while it waits for capacity drops
//! its permits, and nothing has been decided or published. The lock acquisition after the
//! reservation is synchronous, so there is no cancellation point between "holding every permit"
//! and "decided and published".
//!
//! # The wait must be one that can end
//!
//! Chained operators share a node, and so share its subtask control channels: a commit naming
//! `k` operators on one node needs `k` slots *at once* on each of that node's channels, where the
//! old handler needed one at a time. A channel holds a fixed number of messages (`16` per subtask,
//! `engine.rs`), and a demand above that could hold every slot of the channel and still wait for
//! one more — a wait no operator can end, because the slots it holds are not messages to drain.
//! [`CommitHandoffPlan::reserve`] counts the demand per channel before it holds anything and,
//! where any channel cannot meet its demand, holds nothing and records why; the guard then
//! refuses the commit definitively, after the fence decision, like every other handoff refusal.
//!
//! # One reserver at a time
//!
//! That bound is per commit; it says nothing about commits reserving *concurrently* (PR #167
//! round 10). A reservation is taken slot by slot and every slot already held is kept while the
//! next is awaited, so two commits that each need two slots of a channel with two free can each
//! take one and wait on the other for ever — and M11.D39g's fault model delivers duplicates, so
//! sixteen copies of one commit against a sixteen-slot channel is a finite, reachable state in
//! which every slot is a held permit, nothing is a message, and the operator has nothing to
//! drain. The reservation therefore runs behind [`HandoffGate`], an asynchronous mutex the worker
//! owns: exactly one commit reserves at a time, and a commit waiting at the gate holds nothing.
//! The one inside it has a demand every channel can hold, so every slot it still lacks is free
//! or a message the operator will drain, and its wait ends. The gate is separate from the
//! lifecycle lock and is never taken under it, so a fence acknowledgement never waits at it;
//! [`CommitHandoffPlan::reserve`] takes the gate itself, so no caller can reserve around it.

use arroyo_rpc::ControlMessage;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{OwnedPermit, Sender};
use tonic::Status;

/// Admits one commit at a time to the reservation of its operator capacity.
///
/// Owned by the worker beside the lifecycle lock and never taken under it. See the module
/// documentation for why a reservation must not run concurrently with another: a partial
/// reservation is capacity no operator can free.
#[derive(Debug, Default)]
pub(crate) struct HandoffGate(Mutex<()>);

/// The channels one commit would be published on, resolved under the lifecycle lock and decided
/// nothing about.
///
/// Built only by `WorkerLifecycle::plan_commit_handoff`, from the execution phase and the operator
/// ids the request names. An operator this phase does not host, or a phase with no execution at
/// all, contributes nothing: the plan is total, because a request that the guard will refuse must
/// be refused *by the guard*, under the lock, and not by whatever this happened to see first.
#[must_use = "a plan holds no capacity until it is reserved"]
#[derive(Debug, Default)]
pub(crate) struct CommitHandoffPlan {
    operators: Vec<(String, Vec<Sender<ControlMessage>>)>,
}

impl CommitHandoffPlan {
    /// A plan naming the channels `operator_id` publishes to.
    pub(crate) fn with_operator(
        mut self,
        operator_id: String,
        channels: &[Sender<ControlMessage>],
    ) -> Self {
        self.operators.push((operator_id, channels.to_vec()));
        self
    }

    /// Waits for one slot on every channel in the plan and holds all of them, as the only
    /// commit reserving behind `gate` while it does.
    ///
    /// Awaited with the lifecycle lock released. A channel whose receiver is gone yields no
    /// permit; that is recorded rather than answered, so that the fence decision — which the
    /// guard takes first — is still the first thing this commit is told. Dropping the future
    /// releases the gate and every permit it held.
    pub(crate) async fn reserve(self, gate: &HandoffGate) -> CommitReservation {
        if let Some(unholdable) = self.unholdable() {
            return CommitReservation {
                operators: HashMap::new(),
                unholdable: Some(unholdable),
            };
        }
        let _one_reserver_at_a_time = gate.0.lock().await;
        let mut operators = HashMap::with_capacity(self.operators.len());
        for (operator_id, channels) in self.operators {
            let mut slots = Vec::with_capacity(channels.len());
            for channel in channels {
                let permit = channel.clone().reserve_owned().await.ok();
                slots.push(ReservedSlot { channel, permit });
            }
            operators.insert(operator_id, slots);
        }
        CommitReservation {
            operators,
            unholdable: None,
        }
    }

    /// The first channel whose demand — slots needed at once, counted across every operator in
    /// the plan that publishes to it — exceeds what the channel can ever hold, if there is one.
    ///
    /// Quadratic in the number of channels, which is the number of subtasks a commit's operators
    /// have on this worker; channels have no identity to hash by, only [`Sender::same_channel`].
    fn unholdable(&self) -> Option<Unholdable> {
        let mut demand: Vec<(&Sender<ControlMessage>, usize)> = Vec::new();
        for (_, channels) in &self.operators {
            for channel in channels {
                match demand
                    .iter_mut()
                    .find(|(known, _)| known.same_channel(channel))
                {
                    Some((_, needed)) => *needed += 1,
                    None => demand.push((channel, 1)),
                }
            }
        }
        demand.into_iter().find_map(|(channel, needed)| {
            let capacity = channel.max_capacity();
            (needed > capacity).then_some(Unholdable { needed, capacity })
        })
    }
}

/// Why no slot of a plan was held: one of its channels would need more slots at once than it
/// has, so the reservation could never complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Unholdable {
    needed: usize,
    capacity: usize,
}

/// One slot held on one operator control channel, and the channel it was held on.
///
/// The channel is kept beside the permit because an [`OwnedPermit`] does not say which channel
/// it belongs to, and the guard has to be able to ask exactly that before it publishes through it.
#[derive(Debug)]
struct ReservedSlot {
    channel: Sender<ControlMessage>,
    /// `None` when the channel's receiver had gone by the time the slot was reserved.
    permit: Option<OwnedPermit<ControlMessage>>,
}

/// Capacity held on every channel a commit would be published on.
///
/// Reachable only by [`CommitHandoffPlan::reserve`], and spent only by the guard's
/// `publish_commit`, one operator at a time through [`Self::take`].
#[must_use = "a reservation that is not spent under the lock publishes nothing"]
#[derive(Debug)]
pub(crate) struct CommitReservation {
    operators: HashMap<String, Vec<ReservedSlot>>,
    /// Set when the plan could never be held at once; then `operators` is empty and nothing was
    /// reserved on any channel.
    unholdable: Option<Unholdable>,
}

impl CommitReservation {
    /// The permits held for `operator_id`, if they are permits on exactly `channels`.
    ///
    /// # Errors
    ///
    /// `FailedPrecondition` — definitive, like every other refusal the commit path gives — when
    /// the plan could never be held at once, when no slots were reserved for this operator, when
    /// the slots are not on the channels the current execution publishes to, or when a channel's
    /// receiver was already gone. The first is a commit this worker cannot publish as one step;
    /// each of the others is an execution that is not the one this commit was reserved against.
    /// A commit is published into the execution that reserved it, whole, or into none.
    #[allow(clippy::result_large_err)]
    pub(crate) fn take(
        &mut self,
        operator_id: &str,
        channels: &[Sender<ControlMessage>],
    ) -> Result<Vec<OwnedPermit<ControlMessage>>, Status> {
        if let Some(Unholdable { needed, capacity }) = self.unholdable {
            return Err(Status::failed_precondition(format!(
                "publishing this commit would need {needed} slots at once on an operator control channel that holds {capacity}"
            )));
        }
        let slots = self.operators.remove(operator_id).ok_or_else(|| {
            Status::failed_precondition(format!(
                "operator {operator_id} was not part of the execution this commit was reserved against"
            ))
        })?;
        if slots.len() != channels.len()
            || slots
                .iter()
                .zip(channels)
                .any(|(slot, channel)| !slot.channel.same_channel(channel))
        {
            return Err(Status::failed_precondition(format!(
                "operator {operator_id}'s control channels changed while this commit waited for capacity"
            )));
        }
        slots
            .into_iter()
            .map(|slot| {
                slot.permit.ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "an operator {operator_id} control channel closed while this commit waited for capacity"
                    ))
                })
            })
            .collect()
    }
}
