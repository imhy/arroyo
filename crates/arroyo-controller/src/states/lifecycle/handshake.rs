//! The active replacement handshake: advancing a worker generation's fence and hearing it
//! acknowledged, before this controller asks that generation to do anything (M11.T26c, design
//! M11.D39e(i), M11.D75).
//!
//! # The window this closes
//!
//! M11.T26d's worker guard admits no *fenced* directive from a generation that has not announced
//! itself, and both of the switches that put a generation into strict mode are behind that gate.
//! The gate opens when the worker *issues* its `RegisterWorkerReq`, not when it applies the
//! answer, precisely so that this module's own first message is admissible: `register_worker`
//! enqueues the `WorkerConnect` that makes a generation schedulable before it replies, so the
//! handshake below can reach a worker whose registration answer is still in flight. What the
//! worker still cannot close from its side is the *other* half — a generation that has never been
//! sent a fence goes on admitting fence-less starts.
//!
//! M11.D39e(i) closes it from this side instead, and actively: *"a replacement controller that
//! schedules a replacement generation, publishes a refusal, or discharges a recorded obligation
//! actively advances and receives fence acknowledgement from existing workers before any job
//! effect or refusal publication, so there is no passive pre-first-message window on those
//! paths."* This module is that advance. Every generation this controller is about to start is
//! first sent a `FENCE_ONLY` directive under the job's own fence, and must answer with
//! `FENCE_ACKNOWLEDGED` reporting a fence at least that high, before any `StartExecution` is
//! issued to it.
//!
//! The clause is qualified because one takeover is excluded, deliberately (PR #167 round 3):
//! adopting an **already-running** execution admits no generation and issues no start, and in
//! worker-leader mode holds no worker set to address — it reconnects to the leader the job's row
//! names, and what makes it exclusive is the adoption CAS rather than anything sent here. That is
//! also why `WorkerLifecycle::admit_commit` reads a commit's fence as a guard and not as an
//! instruction: the workers such an adopter inherits have not heard its fence.
//!
//! # Why acknowledgement is a value and not a flag
//!
//! [`AcknowledgedTarget`] can be built nowhere but here, by [`advance_fence`] observing the
//! acknowledgement, and [`StartTargets`]'s fenced form pairs every client with one. A fenced
//! start to a generation that has not answered the handshake is therefore not "a check that was
//! skipped" — it is a value that cannot be constructed. That is also the answer to what a
//! cancelled handshake leaves behind: if this future is dropped part-way, some generations have
//! advanced their fence (monotone, idempotent, and to *this* controller's own fence, so a later
//! attempt of this controller is admitted and an older controller is refused) and **no**
//! acknowledgement escapes, so nothing downstream can act as though the handshake had finished.
//!
//! # All or nothing, and the one caller for which that is wrong
//!
//! A handshake that reached some generations and not others does not produce a partial fan-out.
//! [`advance_fence`] answers with every target or with a refusal naming those that did not
//! acknowledge, because "start the ones that answered" is a job running on a subset of its
//! workers under a fence the rest never heard — the state the fan-out's own settlement
//! accounting assumes cannot exist.
//!
//! A *fencing* job wants the opposite, and [`advance_fence_each`] is that answer. It is not
//! starting anything: it is discharging an obligation, target by target, and M11.D39g's declared
//! outcome for a partition is that the targets which answered settle while the one that did not
//! keeps the job in `Fencing`. Throwing away the acknowledgements because one target was
//! unreachable would turn a partition of one worker into an obligation over all of them.

use std::collections::HashMap;
use std::fmt::Write as _;

use arroyo_rpc::fence_wire::{
    FenceAddress, MalformedFenceFields, ObservedSettlement, StartDirective, observed_settlement,
};
use arroyo_rpc::grpc::rpc::{StartExecutionOutcome, StartExecutionReq};
use arroyo_rpc::identity::WorkerClient;
use arroyo_types::WorkerId;
use futures::stream::{FuturesUnordered, StreamExt};
use thiserror::Error;
use tonic::Request;
use tracing::{info, warn};

use super::protocol::{FenceProtocol, FencedGeneration, TransportSettlement};
use crate::states::scheduling::{
    START_EXECUTION_RECONCILE_ATTEMPTS, START_EXECUTION_RECONCILE_DELAY,
};

/// A worker generation that has acknowledged this controller's fence.
///
/// The field is private and the only constructor is [`acknowledgement`]'s own, so the type is a
/// witness rather than a record: holding one means this controller observed that generation
/// report a fence at least as high as the one being addressed to it, and there is no way to
/// assert that without having observed it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AcknowledgedTarget {
    address: FenceAddress,
    /// The height the generation reported, which is at least [`Self::address`]'s fence and may
    /// be higher — see [`acknowledgement`] on why a higher one is still an acknowledgement.
    observed_fence: u64,
}

impl AcknowledgedTarget {
    /// The `START` directive a request to this generation carries.
    ///
    /// It revokes nothing: the fence this generation acknowledged already supersedes every
    /// identifier below it, and naming identifiers as well is M11.T26e's reconciliation.
    pub(crate) fn start(self) -> StartDirective<'static> {
        StartDirective::Fenced {
            address: self.address,
            operation: arroyo_rpc::grpc::rpc::LifecycleOperation::Start,
            revoked_execution_ids: &[],
        }
    }

    /// What this target acknowledged, as the fact a settlement may be recorded from.
    ///
    /// The same witness, read for the other half of M11.D39e(v): the start path asks *"may I
    /// send this generation a request"* and the settlement path asks *"has this generation made
    /// an identifier permanently inapplicable"*. They are one observation, so they are one
    /// value, and this is the only conversion between them.
    pub(crate) fn acknowledgement(self) -> FenceAcknowledgement {
        FenceAcknowledgement {
            worker: WorkerId(self.address.target().worker_id()),
            generation: self.address.target().generation(),
            observed_fence: self.observed_fence,
        }
    }
}

/// A worker generation's acknowledgement of a fence, and **the height it reported**
/// (M11.T26f, design M11.D39d/M11.D39e(v)).
///
/// M11.T26e's [`Observed`](crate::states::scheduling::fanout::Observed) had an
/// `acknowledged_fence(worker, generation)` constructor that carried no height, and that is a
/// silent way to settle wrongly. An acknowledgement makes an issued identifier permanently
/// inapplicable only if the fence acknowledged is **above** the fence the identifier was issued
/// under: a generation acknowledging the very fence this attempt's starts carry has revoked
/// nothing of this attempt's, because a worker revokes what is *below* the fence it takes. The
/// two-argument constructor could not tell those apart, so the caller had to — and a caller who
/// forgot released the job's lifecycle authority behind a `StartExecution` a worker may still
/// apply.
///
/// So the height is part of the value. The fields are private, this module is the only place
/// that builds one, and the one production route to it is [`acknowledgement`] observing a
/// `FENCE_ACKNOWLEDGED` response — so "an acknowledgement of a lower fence" is not a check a
/// caller performs but a comparison
/// [`IssuedAttempts::observe`](crate::states::scheduling::fanout::IssuedAttempts) makes against
/// the fence the inventory records for itself.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FenceAcknowledgement {
    worker: WorkerId,
    generation: u64,
    observed_fence: u64,
}

impl FenceAcknowledgement {
    /// The worker whose generation acknowledged.
    pub(crate) fn worker(&self) -> WorkerId {
        self.worker
    }

    /// The worker generation that acknowledged.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// The highest fence that generation reported holding.
    pub(crate) fn observed_fence(&self) -> u64 {
        self.observed_fence
    }

    /// Whether this acknowledgement makes every identifier issued under `fence` permanently
    /// inapplicable.
    ///
    /// Strictly above, not at or above. A worker admits a directive carrying the fence it holds
    /// and refuses one below it, so acknowledging fence *f* revokes what was issued under
    /// *f - 1* and leaves what was issued under *f* exactly as applicable as it was.
    pub(crate) fn supersedes(&self, fence: u64) -> bool {
        self.observed_fence > fence
    }
}

/// The workers one fan-out addresses, each with the directive its `StartExecution` carries.
///
/// A struct around a private enum rather than a public one, because the invariant is which
/// pairs may exist: under the fenced protocol a client appears only alongside the
/// [`AcknowledgedTarget`] for its generation, and that arm can be built only inside this module
/// — that is, only by a completed handshake.
pub(crate) struct StartTargets(Addressed);

impl std::fmt::Debug for StartTargets {
    /// The shape and the workers, and deliberately not the channels: a `WorkerClient` has no
    /// useful rendering and a fan-out's targets are read for which generations they address.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Addressed::Unfenced(connects) => f
                .debug_struct("StartTargets")
                .field("protocol", &"Legacy")
                .field("workers", &connects.keys().collect::<Vec<_>>())
                .finish(),
            Addressed::Fenced(generation, targets) => f
                .debug_struct("StartTargets")
                .field("fence", &generation.fence())
                .field("generation", &generation.generation())
                .field("workers", &targets.keys().collect::<Vec<_>>())
                .finish(),
        }
    }
}

/// The two shapes a fan-out's targets have. Private: see [`StartTargets`].
enum Addressed {
    /// The pre-flag-day shape: every request carries no fence and addresses no generation.
    Unfenced(HashMap<WorkerId, WorkerClient>),
    /// The fenced shape: every request carries the fence its generation acknowledged.
    Fenced(
        FencedGeneration,
        HashMap<WorkerId, (WorkerClient, AcknowledgedTarget)>,
    ),
}

impl StartTargets {
    /// The targets of a fan-out that performs no fence handshake.
    ///
    /// `Some` only under [`FenceProtocol::Legacy`], where there is no fence to advance and
    /// nothing to acknowledge — the whole of the pre-flag-day protocol, and the only shape a
    /// fan-out can have without first running [`advance_fence`]. `None` under the fenced
    /// protocol, and the `None` is the point: it is what stops a caller that has no handshake
    /// to offer from quietly falling back to fence-less requests for a job whose worker
    /// generations this controller has put into strict mode.
    ///
    /// Taking the protocol rather than being called only on the legacy path is what makes that
    /// a decision the type checker forces rather than one a caller has to remember.
    pub(crate) fn without_a_handshake(
        protocol: FenceProtocol,
        connects: HashMap<WorkerId, WorkerClient>,
    ) -> Option<Self> {
        match protocol {
            FenceProtocol::Legacy => Some(Self(Addressed::Unfenced(connects))),
            FenceProtocol::Fenced(_) => None,
        }
    }

    /// The protocol these targets were addressed under.
    ///
    /// The fan-out's retry table is read from this rather than passed beside it, so the
    /// directives a request carries and the reading its failures get cannot come from different
    /// modes.
    pub(crate) fn protocol(&self) -> FenceProtocol {
        match &self.0 {
            Addressed::Unfenced(_) => FenceProtocol::Legacy,
            Addressed::Fenced(generation, _) => FenceProtocol::Fenced(*generation),
        }
    }

    /// Every worker this fan-out will address, before any of them is addressed.
    ///
    /// Read by the fan-out to mint and record its identifiers up front: M11.D39d's obligation has
    /// to name every target *before* the first request is polled, so the set has to be readable
    /// without consuming the targets (PR #167 round 2).
    pub(crate) fn worker_ids(&self) -> Vec<WorkerId> {
        match &self.0 {
            Addressed::Unfenced(connects) => connects.keys().copied().collect(),
            Addressed::Fenced(_, targets) => targets.keys().copied().collect(),
        }
    }

    /// Every acknowledgement this handshake observed, each carrying the height its generation
    /// reported.
    ///
    /// Empty under the pre-flag-day protocol, where no fence is advanced and there is nothing
    /// to acknowledge. Read by the fan-out so that what the handshake observed reaches the
    /// attempt's fencing reconciliation as an *observation* rather than being consumed here —
    /// none of these settles anything of that attempt's own, and refusing them is the
    /// reconciliation's job rather than this type's.
    pub(crate) fn acknowledgements(&self) -> Vec<FenceAcknowledgement> {
        match &self.0 {
            Addressed::Unfenced(_) => Vec::new(),
            Addressed::Fenced(_, targets) => targets
                .values()
                .map(|(_, target)| target.acknowledgement())
                .collect(),
        }
    }

    /// Each addressed worker, its channel, and the directive its request carries.
    pub(crate) fn into_starts(self) -> Vec<(WorkerId, WorkerClient, StartDirective<'static>)> {
        match self.0 {
            Addressed::Unfenced(connects) => connects
                .into_iter()
                .map(|(id, client)| (id, client, StartDirective::Unfenced))
                .collect(),
            Addressed::Fenced(_, targets) => targets
                .into_iter()
                .map(|(id, (client, target))| (id, client, target.start()))
                .collect(),
        }
    }
}

/// Why a worker generation did not acknowledge this controller's fence.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum NotAcknowledged {
    /// The generation answered, definitively, that it will not.
    ///
    /// Its own decision about this controller's fence: it has acknowledged a higher one, it is
    /// not the generation being addressed, or it has not announced itself to any controller.
    #[error("worker {worker} refused the fence handshake: {status}")]
    Refused {
        /// The worker that refused.
        worker: u64,
        /// What it said.
        status: String,
    },
    /// The generation never settled the handshake within the reconciliation budget.
    ///
    /// Every attempt ended in an ambiguous transport outcome, so this controller does not know
    /// whether the fence was advanced. It is not settlement, and it is not treated as one: the
    /// generation is simply not one this controller may start.
    #[error(
        "worker {worker} never settled the fence handshake after {attempts} attempts: {status}"
    )]
    Unsettled {
        /// The worker that never answered.
        worker: u64,
        /// How many attempts were made.
        attempts: usize,
        /// The last thing that went wrong.
        status: String,
    },
    /// The generation answered something this build cannot read as an acknowledgement.
    ///
    /// A response whose lifecycle fields do not describe one settlement, an outcome that is not
    /// `FENCE_ACKNOWLEDGED`, or an acknowledgement of a fence below the one addressed. The
    /// second is what a worker predating M11.T26c produces: it does not know the operation, so
    /// it reads a `FENCE_ONLY` directive as an ordinary start and answers `APPLIED`. Refusing
    /// that loudly is the mixed-version detection M11.D75's worker-first ordering exists to
    /// prevent ever being needed.
    #[error(
        "worker {worker} answered the fence handshake with something that is not an \
         acknowledgement of fence {fence}: {report}"
    )]
    NotAnAcknowledgement {
        /// The worker that answered.
        worker: u64,
        /// The fence it was asked to acknowledge.
        fence: u64,
        /// What it answered.
        report: String,
    },
}

/// Every generation this controller may now start, or the ones that stopped it.
#[derive(Debug, Error)]
#[error("{}", .0.iter().fold(String::new(), |mut acc, e| { let _ = write!(acc, "{e}; "); acc }))]
pub(crate) struct HandshakeRefusal(Vec<NotAcknowledged>);

/// Advances `generation`'s fence on every worker in `connects` and waits for each to acknowledge.
///
/// One `FENCE_ONLY` directive per worker, all in flight together and every one awaited to an
/// outcome — the same discipline the `StartExecution` fan-out is under, and for the same reason:
/// dropping a client future resets a stream and says nothing about what the server did with the
/// request.
///
/// # Errors
///
/// [`HandshakeRefusal`] naming every generation that did not acknowledge. Nothing is started when
/// one does: see the module documentation on why this is all or nothing.
pub(crate) async fn advance_fence(
    generation: FencedGeneration,
    connects: HashMap<WorkerId, WorkerClient>,
) -> Result<StartTargets, HandshakeRefusal> {
    let protocol = FenceProtocol::Fenced(generation);
    let mut handshakes: FuturesUnordered<_> = connects
        .into_iter()
        .map(
            |(id, client)| async move { (id, advance_one(protocol, generation, id, client).await) },
        )
        .collect();

    let mut acknowledged = HashMap::new();
    let mut refused = Vec::new();
    while let Some((id, outcome)) = handshakes.next().await {
        match outcome {
            Ok((client, target)) => {
                acknowledged.insert(id, (client, target));
            }
            Err(e) => {
                warn!(
                    worker_id = id.0,
                    fence = generation.fence(),
                    generation = generation.generation(),
                    error = %e,
                    "a worker generation did not acknowledge this controller's lifecycle fence"
                );
                refused.push(e);
            }
        }
    }

    if !refused.is_empty() {
        return Err(HandshakeRefusal(refused));
    }
    info!(
        fence = generation.fence(),
        generation = generation.generation(),
        workers = acknowledged.len(),
        "every addressed worker generation acknowledged this controller's lifecycle fence"
    );
    Ok(StartTargets(Addressed::Fenced(generation, acknowledged)))
}

/// One generation's handshake, retried under the same directive while the outcome is ambiguous.
async fn advance_one(
    protocol: FenceProtocol,
    generation: FencedGeneration,
    id: WorkerId,
    mut client: WorkerClient,
) -> Result<(WorkerClient, AcknowledgedTarget), NotAcknowledged> {
    let mut request = StartExecutionReq::default();
    generation.fence_only(id).stamp(&mut request);

    let mut unsettled = 0usize;
    loop {
        let status = match client.start_execution(Request::new(request.clone())).await {
            Ok(response) => {
                return acknowledgement(
                    generation,
                    id,
                    observed_settlement(&response.into_inner()),
                )
                .map(|target| (client, target));
            }
            Err(status) => status,
        };
        match protocol.transport_settlement(status.code()) {
            // The worker's own answer about this generation. Re-offering the same directive
            // cannot change it, and treating it as transport is what would wedge the attempt.
            TransportSettlement::Definitive => {
                return Err(NotAcknowledged::Refused {
                    worker: id.0,
                    status: format!("{status:?}"),
                });
            }
            TransportSettlement::Ambiguous => {
                unsettled += 1;
                if unsettled > START_EXECUTION_RECONCILE_ATTEMPTS {
                    return Err(NotAcknowledged::Unsettled {
                        worker: id.0,
                        attempts: unsettled,
                        status: format!("{status:?}"),
                    });
                }
                tokio::time::sleep(START_EXECUTION_RECONCILE_DELAY).await;
            }
        }
    }
}

/// Reads one response as an acknowledgement of `generation`'s fence, or refuses it.
fn acknowledgement(
    generation: FencedGeneration,
    id: WorkerId,
    settlement: Result<ObservedSettlement, MalformedFenceFields>,
) -> Result<AcknowledgedTarget, NotAcknowledged> {
    let refuse = |report: String| NotAcknowledged::NotAnAcknowledgement {
        worker: id.0,
        fence: generation.fence(),
        report,
    };
    let settlement = settlement.map_err(|e| refuse(e.to_string()))?;
    match settlement.outcome() {
        StartExecutionOutcome::FenceAcknowledged => {}
        // `APPLIED` is what a worker predating the operation answers, and what one that applied
        // a program answers; neither acknowledges a fence. `REVOKED` answers a directive that
        // named identifiers, and this one names none.
        outcome => return Err(refuse(format!("outcome {outcome:?}"))),
    }
    // At least, not exactly: the generation may have acknowledged a *higher* fence in the
    // meantime — from a controller that superseded this one — and it reports the highest it
    // holds. That is still an acknowledgement of this fence in the only sense that matters,
    // because everything below it is refused from now on. A lower one is not: it would mean the
    // generation had not taken this fence at all.
    match settlement.observed_fence() {
        Some(observed) if observed >= generation.fence() => Ok(AcknowledgedTarget {
            address: generation.address(id),
            observed_fence: observed,
        }),
        observed => Err(refuse(format!("observed fence {observed:?}"))),
    }
}

/// Advances `generation`'s fence on every worker in `connects` and reports **each** outcome.
///
/// The same advance [`advance_fence`] performs, answering per target rather than all-or-nothing,
/// because the two callers need opposite things from a partial result. A fan-out must not start
/// a job on the subset that answered, so [`advance_fence`] refuses. A *fencing* job must settle
/// exactly the subset that answered: M11.D39g's declared outcome for a permanently unobservable
/// partition is that the job stays in `Fencing` — with the targets that did acknowledge
/// settled, and only the partitioned one pending — and an all-or-nothing advance would throw
/// away the acknowledgements that made the difference.
///
/// Nothing here infers anything from a target that did not answer. An ambiguous transport
/// outcome is retried within the same budget the fan-out uses and then reported as
/// [`NotAcknowledged::Unsettled`]; that is not settlement, and the caller leaves the target
/// pending. There is no timeout that converts silence into an acknowledgement, and no value in
/// this module that could express one.
pub(crate) async fn advance_fence_each(
    generation: FencedGeneration,
    connects: HashMap<WorkerId, WorkerClient>,
) -> FenceAdvance {
    let protocol = FenceProtocol::Fenced(generation);
    let mut handshakes: FuturesUnordered<_> = connects
        .into_iter()
        .map(
            |(id, client)| async move { (id, advance_one(protocol, generation, id, client).await) },
        )
        .collect();

    let mut advance = FenceAdvance::default();
    while let Some((id, outcome)) = handshakes.next().await {
        match outcome {
            Ok((_client, target)) => {
                let acknowledgement = target.acknowledgement();
                info!(
                    worker_id = id.0,
                    addressed_fence = generation.fence(),
                    observed_fence = acknowledgement.observed_fence(),
                    generation = generation.generation(),
                    "a worker generation acknowledged this controller's lifecycle fence while \
                     the job was fencing"
                );
                advance.acknowledged.push(acknowledgement);
            }
            Err(e) => {
                warn!(
                    worker_id = id.0,
                    fence = generation.fence(),
                    generation = generation.generation(),
                    error = %e,
                    "a worker generation did not acknowledge this controller's lifecycle fence, \
                     so the obligation it owes stays pending"
                );
                advance.unacknowledged.push(e);
            }
        }
    }
    advance
}

/// What a per-target advance observed: the generations that acknowledged, and the ones that did
/// not.
///
/// Both halves are kept because both are reported: the first settles targets and the second is
/// what an operator reads when a job will not leave `Fencing`.
#[derive(Debug, Default)]
pub(crate) struct FenceAdvance {
    acknowledged: Vec<FenceAcknowledgement>,
    unacknowledged: Vec<NotAcknowledged>,
}

impl FenceAdvance {
    /// The acknowledgements observed, each carrying the height its generation reported.
    pub(crate) fn acknowledged(&self) -> &[FenceAcknowledgement] {
        &self.acknowledged
    }

    /// Why each addressed generation did not acknowledge.
    pub(crate) fn unacknowledged(&self) -> &[NotAcknowledged] {
        &self.unacknowledged
    }
}

// ---------------------------------------------------------------------------------------
// Test-only reach into what a handshake produced.
//
// Declared below the whole production half, for the reason `scheduling/fanout.rs` records: a
// `#[cfg(test)]` placed higher would truncate any source pin that cuts a file at its first one.
// ---------------------------------------------------------------------------------------

#[cfg(test)]
impl StartTargets {
    /// How many workers this fan-out addresses.
    pub(crate) fn len(&self) -> usize {
        match &self.0 {
            Addressed::Unfenced(connects) => connects.len(),
            Addressed::Fenced(_, targets) => targets.len(),
        }
    }
}

#[cfg(test)]
impl HandshakeRefusal {
    /// The generations that did not acknowledge.
    pub(crate) fn refusals(&self) -> &[NotAcknowledged] {
        &self.0
    }
}

#[cfg(test)]
impl FenceAcknowledgement {
    /// An acknowledgement assembled from loose values, for tests that need a *wrong* one.
    ///
    /// Test-only for the reason the type exists: the production route above is what stops an
    /// acknowledgement of a fence nobody reported — or of one too low to revoke anything — from
    /// existing, and a test that proves the height matters has to be able to state one. This is
    /// the same allowance `LifecycleAuthority::from_parts` carries, for the same reason.
    pub(crate) fn reported(worker: WorkerId, generation: u64, observed_fence: u64) -> Self {
        Self {
            worker,
            generation,
            observed_fence,
        }
    }
}
