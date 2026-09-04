//! The worker half of M11.D39g's declared fault model, as named reusable injections
//! (M11.T26g).
//!
//! M11.D39g declares the faults this protocol is answerable for: *"message
//! loss/duplication/reorder and arbitrary in-transit delay; worker crash/restart and partition;
//! controller crash/restart at any point; endpoint reuse by a new worker generation; and
//! post-flag-day version skew."* Some of those are things a **controller** observes and decides
//! about — a partition, its own crash — and their injections live in the controller's
//! `states::lifecycle::faults`. The ones below are the faults that reach a *worker generation*
//! and are answered by [`WorkerLifecycle`](super::guard::WorkerLifecycle): what arrives at the
//! guard, in what order, how many times, and at which generation.
//!
//! # Why a link and not a mock
//!
//! Every injection here is a transformation of the **delivery** of a real
//! `StartExecutionReq`/`CommitReq` to a real [`WorkerServer`] through the real production
//! handler. Nothing here reimplements a fence rule or stands in for the guard: a fault is a
//! statement about *when and how often a message arrives*, and [`Link`] is the only thing that
//! decides that. A mock worker that "refuses stale fences" would pass every test in this file
//! while the guard did nothing at all.
//!
//! [`Link::hold`] is what makes in-transit delay expressible: a directive that has been *sent*
//! and not yet *delivered* is a value this module holds, so a test can advance the world around
//! it and then deliver it. That is the whole shape of M11.D39g's delayed-delivery row and of
//! D96 row 18.
//!
//! # The declared coverage, and why it cannot silently shrink
//!
//! [`WorkerFault::ALL`] is the enumeration, and [`WorkerFault::injection`] is an exhaustive
//! match naming the operation that injects each one. A fault added to the enum without an
//! injection does not compile, and an injection nothing calls fails the build under
//! `-D warnings` — so "every declared fault has a live injection" is a property of the build
//! rather than of this comment. `every_declared_worker_fault_has_a_live_injection` reads the
//! same table back.

use crate::lifecycle_fence::guard::WorkerLifecycle;
use crate::lifecycle_fence::tests::{GENERATION, WORKER, call, read, register};
use crate::{WorkerExecutionPhase, WorkerServer};
use arroyo_rpc::grpc::rpc::{StartExecutionReq, StartExecutionResp};
use arroyo_server_common::shutdown::{Shutdown, SignalBehavior};
use arroyo_types::{JobId, MachineId, PipelineId, WorkerId};
use std::sync::Arc;
use tonic::Status;

/// One fault from M11.D39g's declared model, as a worker generation can observe it.
///
/// The three D39g faults that are **not** here are the ones a worker cannot observe: a
/// controller crash (the worker sees only that a directive stopped arriving, which is
/// [`Self::Loss`]), a partition (same, from the other side), and the controller's own
/// crash-point choice. Those are injected in `states::lifecycle::faults`, against the party
/// that decides about them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WorkerFault {
    /// A directive that was sent and never arrived.
    Loss,
    /// A directive that arrived more than once.
    Duplication,
    /// Two directives that arrived in the opposite order to the one they were sent in.
    Reorder,
    /// A directive held in transit and delivered after later ones.
    Delay,
    /// The generation restarted: the same worker id and generation, and none of the fence
    /// state the predecessor had acknowledged.
    Restart,
    /// A *new* generation answering at the endpoint its predecessor had.
    EndpointReuse,
    /// A directive that reaches a generation which has not announced itself to any controller.
    Unregistered,
    /// Post-flag-day skew: a fence-less directive from a controller predating the fields,
    /// arriving at a generation that has already acknowledged a fence.
    VersionSkew,
}

impl WorkerFault {
    /// Every fault this side of the protocol answers for.
    pub(super) const ALL: [WorkerFault; 8] = [
        WorkerFault::Loss,
        WorkerFault::Duplication,
        WorkerFault::Reorder,
        WorkerFault::Delay,
        WorkerFault::Restart,
        WorkerFault::EndpointReuse,
        WorkerFault::Unregistered,
        WorkerFault::VersionSkew,
    ];

    /// The operation on [`Link`] that injects this fault.
    ///
    /// Exhaustive: a variant added above without an injection here does not compile.
    pub(super) fn injection(self) -> &'static str {
        match self {
            WorkerFault::Loss => "Link::lose",
            WorkerFault::Duplication => "Link::duplicate",
            WorkerFault::Reorder => "Link::deliver_held_in_reverse",
            WorkerFault::Delay => "Link::hold + Link::deliver_held",
            WorkerFault::Restart => "Link::restart_generation",
            WorkerFault::EndpointReuse => "Link::endpoint_reused_by",
            WorkerFault::Unregistered => "Link::to_unregistered_generation",
            WorkerFault::VersionSkew => "Link::deliver_from_a_predecessor_controller",
        }
    }
}

/// A directive that has been sent and has not been delivered.
///
/// Sending and delivering are two events, and the whole of M11.D39g's transport half is about
/// the interval between them. A test that could only "call the handler" could not express one.
struct InFlight {
    label: &'static str,
    req: StartExecutionReq,
}

/// The link between one controller and one worker generation.
///
/// Owns the receiving generation, so a fault that *replaces* it — a restart, an endpoint reuse —
/// is an operation on the link rather than a second fixture the test has to keep straight.
pub(super) struct Link {
    /// Held for the life of the link: dropping it shuts the generation's guard down.
    _shutdown: Shutdown,
    server: WorkerServer,
    /// This generation's identity, so a successor can be built against the same worker id.
    worker_id: u64,
    generation: u64,
    /// Directives sent and not yet delivered, oldest first.
    in_flight: Vec<InFlight>,
    /// Directives that were sent and never arrived, in send order — read back by
    /// [`Self::lost`] so a row can state what the link swallowed rather than assume it.
    lost: Vec<&'static str>,
}

impl Link {
    /// A link to a registered generation of [`WORKER`]/[`GENERATION`].
    ///
    /// `strict` is M11.D39e(i)'s flag-day state as the *registration response* set it: `false`
    /// is the pre-flag-day window, in which this generation still answers a fence-less
    /// directive.
    pub(super) fn to_registered_generation(strict: bool) -> Self {
        let link = Self::to_unregistered_generation();
        register(&link.server, strict);
        link
    }

    /// **Injects [`WorkerFault::Unregistered`].** A link to a generation that has not announced
    /// itself to any controller — it has not yet issued its `RegisterWorkerReq`.
    ///
    /// M11.T26c places an unregistered peer in the post-flag-day fail-closed set, and M11.T26d
    /// scopes that to the *fenced* arm: this generation still admits the legacy fence-less
    /// shape, and refuses every fenced directive definitively. The gate is the request rather
    /// than its answer, because a controller may address a generation from the moment it holds
    /// one — see `WorkerLifecycle::announce`.
    pub(super) fn to_unregistered_generation() -> Self {
        Self::to_generation(WORKER, GENERATION)
    }

    /// A link to an arbitrary worker generation that has not announced itself.
    fn to_generation(worker_id: u64, generation: u64) -> Self {
        let shutdown = Shutdown::new("m11-d39g-fault-link", SignalBehavior::None);
        let server = WorkerServer::new(
            MachineId(Arc::new("machine_1".to_string())),
            WorkerId(worker_id),
            PipelineId(Arc::new("pipeline_1".to_string())),
            JobId(Arc::new("job_1".to_string())),
            generation,
            shutdown.guard("worker"),
        );
        Self {
            _shutdown: shutdown,
            server,
            worker_id,
            generation,
            in_flight: Vec::new(),
            lost: Vec::new(),
        }
    }

    /// Sends a directive and delivers it immediately: the fault-free control.
    #[allow(clippy::result_large_err)]
    pub(super) fn deliver(&mut self, req: StartExecutionReq) -> Result<StartExecutionResp, Status> {
        call(&self.server, req)
    }

    /// **Injects [`WorkerFault::Loss`].** The directive is sent and never arrives.
    ///
    /// It is recorded rather than dropped on the floor so that a row can assert *which*
    /// directive the link swallowed; a lost message that no test can name is a fixture that
    /// could have sent nothing at all.
    pub(super) fn lose(&mut self, label: &'static str, req: StartExecutionReq) {
        let _ = req;
        self.lost.push(label);
    }

    /// The directives this link swallowed, in send order.
    pub(super) fn lost(&self) -> &[&'static str] {
        &self.lost
    }

    /// **Injects [`WorkerFault::Duplication`].** The same directive arrives twice.
    ///
    /// Both answers are returned, because the property under test is always about the pair: a
    /// duplicate that is refused where the original applied, or that applies a second time, is
    /// the defect — not the fact that two calls happened.
    pub(super) fn duplicate(
        &mut self,
        req: StartExecutionReq,
    ) -> [Result<StartExecutionResp, Status>; 2] {
        [call(&self.server, req.clone()), call(&self.server, req)]
    }

    /// **Injects the send half of [`WorkerFault::Delay`].** The directive is in transit.
    pub(super) fn hold(&mut self, label: &'static str, req: StartExecutionReq) {
        self.in_flight.push(InFlight { label, req });
    }

    /// **Injects the delivery half of [`WorkerFault::Delay`].** The held directive arrives now.
    ///
    /// # Panics
    ///
    /// If nothing was sent under `label`. A row that delivers a directive it never sent is
    /// asserting about a message that does not exist.
    #[allow(clippy::result_large_err)]
    pub(super) fn deliver_held(
        &mut self,
        label: &'static str,
    ) -> Result<StartExecutionResp, Status> {
        let at = self
            .in_flight
            .iter()
            .position(|held| held.label == label)
            .unwrap_or_else(|| panic!("nothing named {label} is in flight on this link"));
        let held = self.in_flight.remove(at);
        call(&self.server, held.req)
    }

    /// **Injects [`WorkerFault::Reorder`].** Everything in flight arrives newest-first.
    ///
    /// Reorder and delay are the same fault seen from two ends — a message that arrives late
    /// arrives after ones sent behind it — so they share the in-flight queue rather than each
    /// having a private notion of "sent". What distinguishes this operation is that it delivers
    /// *all* of them, in the order the link chose rather than the order the controller sent.
    pub(super) fn deliver_held_in_reverse(
        &mut self,
    ) -> Vec<(&'static str, Result<StartExecutionResp, Status>)> {
        let mut reversed: Vec<InFlight> = self.in_flight.drain(..).collect();
        reversed.reverse();
        reversed
            .into_iter()
            .map(|held| (held.label, call(&self.server, held.req)))
            .collect()
    }

    /// **Injects [`WorkerFault::VersionSkew`].** A fence-less directive from a controller that
    /// predates the lifecycle fields arrives at this generation.
    ///
    /// The post-flag-day half of M11.D75's rollout window: before the flag day this is the
    /// whole protocol and the directive is admitted unchanged; after it — that is, once this
    /// generation has acknowledged a fence or registered under strict mode — it is a directive
    /// this generation can no longer attribute to any controller, and fails closed.
    #[allow(clippy::result_large_err)]
    pub(super) fn deliver_from_a_predecessor_controller(
        &mut self,
        id: &str,
    ) -> Result<StartExecutionResp, Status> {
        call(
            &self.server,
            StartExecutionReq {
                start_execution_id: id.to_string(),
                ..Default::default()
            },
        )
    }

    /// **Injects [`WorkerFault::Restart`].** The generation restarts in place.
    ///
    /// The same worker id and the same generation number, and a guard that has acknowledged
    /// nothing: a restarted process keeps no fence state, which is precisely why M11.D39e(v)
    /// does not let a controller treat a *reachable* endpoint as evidence about what its
    /// predecessor applied. Anything still in flight stays in flight — a restart does not
    /// deliver or cancel messages already on the wire.
    pub(super) fn restart_generation(&mut self) {
        self.replace_receiver(self.worker_id, self.generation);
    }

    /// **Injects [`WorkerFault::EndpointReuse`].** A *new* generation answers at this endpoint.
    ///
    /// Identity is the (worker id, generation) pair, so this differs from a restart in exactly
    /// the field the guard compares. Directives already in flight are addressed to the
    /// predecessor and must be refused rather than answered on its behalf.
    pub(super) fn endpoint_reused_by(&mut self, generation: u64) {
        self.replace_receiver(self.worker_id, generation);
    }

    /// Puts a fresh generation, which has announced itself to nobody, at this link's receiving
    /// end.
    fn replace_receiver(&mut self, worker_id: u64, generation: u64) {
        let replacement = Self::to_generation(worker_id, generation);
        // The predecessor's shutdown is dropped with the value it belongs to; the in-flight
        // queue and the loss log belong to the *link* and survive, which is what makes a
        // delayed directive outlive the generation it was addressed to.
        self._shutdown = replacement._shutdown;
        self.server = replacement.server;
        self.worker_id = worker_id;
        self.generation = generation;
    }

    /// Completes this generation's registration.
    pub(super) fn register_receiver(&self, strict: bool) {
        register(&self.server, strict);
    }

    /// Delivers the `FENCE_ONLY` handshake that authorises a start at `fence`, addressed to the
    /// generation currently at this link's receiving end.
    ///
    /// A start is only ever issued out of an `AcknowledgedTarget`, so every row that delivers one
    /// performs this first — including the ones whose whole point is that the start is *late*,
    /// because being late is not the same as never having been authorised.
    pub(super) fn handshake_receiver(&mut self, fence: u64) {
        let directive = super::tests::addressed_fence_only(fence, self.worker_id, self.generation);
        self.deliver(directive)
            .expect("the handshake this generation needs before a start can be addressed to it");
    }

    /// A link to a [`WORKER`]/[`GENERATION`] generation that has registered and acknowledged
    /// `fence` — the state a controller's own workers are in when it addresses them.
    pub(super) fn handshaken_at(fence: u64) -> Self {
        let mut link = Self::to_registered_generation(false);
        link.handshake_receiver(fence);
        link
    }

    /// The highest fence this generation has acknowledged.
    pub(super) fn acknowledged(&self) -> u64 {
        read(&self.server, WorkerLifecycle::acknowledged_fence)
    }

    /// The one identifier this generation applied, if it applied one.
    pub(super) fn applied(&self) -> Option<String> {
        read(&self.server, |l| l.applied().map(str::to_string))
    }

    /// How many identifiers this generation is tracking.
    pub(super) fn tracked(&self) -> usize {
        read(&self.server, WorkerLifecycle::tracked_ids)
    }

    /// Whether this generation is in strict mode.
    pub(super) fn strict(&self) -> bool {
        read(&self.server, WorkerLifecycle::is_strict)
    }

    /// Whether this generation has started nothing.
    pub(super) fn idle(&self) -> bool {
        read(&self.server, |l| {
            matches!(l.execution(), WorkerExecutionPhase::Idle)
        })
    }
}
