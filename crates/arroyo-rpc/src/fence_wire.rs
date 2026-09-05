//! The lifecycle-fence fields the existing worker RPCs carry, read as whole values
//! (M11.T26c, design M11.D39d/M11.D39e).
//!
//! M11.P54a puts the fence protocol on the messages arroyo already has —
//! [`RegisterWorkerResp`](crate::grpc::rpc::RegisterWorkerResp),
//! [`StartExecutionReq`]/[`StartExecutionResp`] and [`CommitReq`] — rather than on new ones.
//! That is the right choice for compatibility and the wrong shape for reading: on the wire a
//! fenced directive is several independent scalars, each with its own default, and proto3 has
//! no way to say that they stand or fall together. A sender can set a fence from one decision
//! and a target from another, and a receiver can read one of the three without the others.
//!
//! This module is where the flat shape is turned back into single values. A reader gets "this
//! fence, addressed to that generation" ([`FenceAddress`]) or a typed refusal
//! ([`MalformedFenceFields`]), and never one field on its own.
//!
//! # What it does not decide
//!
//! Nothing here is policy. Whether an unfenced directive is acceptable depends on the
//! registration flag day (M11.D39e(i), M11.D75) and on what the receiving generation has
//! already acknowledged, and that decision belongs to the worker's admission guard. This
//! module answers only which of the two shapes a message has, and refuses the shapes that are
//! neither.
//!
//! Nor does it compare identities. A [`LifecycleTarget`] read off a request is the target the
//! *sender* named; whether it is the receiver has to be settled against the receiver's own
//! `WorkerContext`, which only the receiver holds.
//!
//! # The send side is the same seam read backwards
//!
//! [`StartDirective::stamp`] and [`CommitDirective::stamp`] write the fields this module
//! reads, and they are the reason the read types are also the *write* types. A sender that
//! assembled `lifecycle_fence`, `target_worker_id` and `target_worker_generation` one field at
//! a time could pair a fence from one adoption with a target from another, and the receiver
//! would have no way to tell; a sender that starts from a [`FenceAddress`] cannot, because
//! there is no way to build one from a fence alone. Each `stamp` writes *every* lifecycle
//! field of the message, including zeroing them all for an unfenced directive, so what a
//! request carries is decided in one place and cannot be a mixture of a directive and whatever
//! the caller's literal happened to leave behind.
//!
//! One step above the directives, [`CommitAuthority`] is what a sender *holds*: the fence it is
//! entitled to commit under and the worker generation that fence addresses, from which a
//! directive for any particular worker of that generation follows. A commit fan-out therefore
//! reads one authority and produces one address per worker, rather than deciding the fence again
//! at each of them.
//!
//! # Registration
//!
//! `RegisterWorkerResp::requires_lifecycle_fence` has no partner field — it is one bool whose
//! `false` is the pre-flag-day reading — so it needs no accessor here and has none.

use std::num::NonZeroU64;

use crate::fencing::{MAX_ATTEMPT_ID_CHARS, MAX_FENCE_TARGETS};
use crate::grpc::rpc::{
    CommitReq, LifecycleOperation, StartExecutionOutcome, StartExecutionReq, StartExecutionResp,
};
use thiserror::Error;

#[cfg(test)]
mod agreement_tests;
#[cfg(test)]
mod golden_tests;
#[cfg(test)]
mod send_tests;
#[cfg(test)]
mod tag_tests;

/// Why a message's lifecycle fields do not describe one directive.
///
/// Every variant is a refusal of *wire* input, so each is reachable from a decode of bytes this
/// process did not write. None of them is recoverable by guessing: a message that breaks one of
/// these rules describes an operation this build cannot name, and answering it would mean
/// inventing the half the sender did not send.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum MalformedFenceFields {
    /// A fence was carried without a worker generation to address it to.
    #[error(
        "lifecycle fence {fence} is carried without a target worker generation (target worker id \
         {target_worker_id})"
    )]
    FenceWithoutTarget {
        /// The fence the message carried.
        fence: u64,
        /// The target worker id it carried, which cannot address a generation by itself.
        target_worker_id: u64,
    },
    /// A target was addressed without the fence that would give it meaning.
    #[error(
        "target worker {worker_id} generation {generation} is addressed without a lifecycle fence"
    )]
    TargetWithoutFence {
        /// The target worker id the message carried.
        worker_id: u64,
        /// The target worker generation the message carried.
        generation: u64,
    },
    /// A lifecycle operation this build does not know, and therefore cannot perform.
    ///
    /// Read rather than defaulted: proto3 keeps an unrecognized enum value verbatim, so a
    /// request from a *newer* controller arrives here as an integer. Treating it as the zero
    /// value would turn "advance the fence" into "start the program".
    #[error("lifecycle operation {operation} is not one this build knows")]
    UnknownOperation {
        /// The value the message carried.
        operation: i32,
    },
    /// A fence-only or revoke operation carried no fence to advance to.
    #[error("lifecycle operation {operation:?} is requested without a lifecycle fence")]
    OperationWithoutFence {
        /// The operation the message requested.
        operation: LifecycleOperation,
    },
    /// Identifiers were named for revocation by a message carrying no fence.
    ///
    /// Revocation is what a fence advancement does to the identifiers it supersedes, so a
    /// revocation under no fence supersedes nothing and names its identifiers on no authority.
    #[error("{count} execution identifiers are revoked without a lifecycle fence")]
    RevocationWithoutFence {
        /// How many identifiers the message named.
        count: usize,
    },
    /// More identifiers to revoke than one job can have outstanding.
    #[error(
        "a start directive revokes {found} execution identifiers, more than the \
         {MAX_FENCE_TARGETS} one job can have outstanding"
    )]
    TooManyRevokedIds {
        /// How many identifiers the message named.
        found: usize,
    },
    /// A revoked identifier that is empty or longer than one the controller can mint.
    #[error(
        "revoked execution identifier {index} is {found} characters, which is not between 1 and \
         {MAX_ATTEMPT_ID_CHARS}"
    )]
    MalformedRevokedId {
        /// Its position in the message's list.
        index: usize,
        /// Its length in characters.
        found: usize,
    },
    /// A worker process was named by a message carrying no fence to address it under.
    ///
    /// An incarnation is one third of an address, not a field of its own: it says *which
    /// process* of the addressed generation a directive is for, and a message that names one
    /// while addressing no generation under no fence describes an operation this build cannot
    /// name.
    #[error(
        "target worker {worker_id} incarnation {incarnation} is named without a lifecycle fence"
    )]
    IncarnationWithoutFence {
        /// The target worker id the message carried.
        worker_id: u64,
        /// The incarnation it named.
        incarnation: u64,
    },
    /// A start-execution outcome this build does not know, and therefore cannot act on.
    #[error("start execution outcome {outcome} is not one this build knows")]
    UnknownOutcome {
        /// The value the message carried.
        outcome: i32,
    },
    /// A response claimed to have acknowledged a fence while reporting none observed.
    #[error("outcome {outcome:?} acknowledges a fence but reports no observed fence")]
    AcknowledgementWithoutObservedFence {
        /// The outcome the response carried.
        outcome: StartExecutionOutcome,
    },
}

/// One worker *process*, as distinct from the slot it runs in.
///
/// A worker id and a generation name a slot, and a restart reuses both: `WorkerFault::Restart`
/// puts a fresh process at the same id and generation holding none of the fence state its
/// predecessor held. Every check the guard makes against that state is therefore a check against
/// state the successor has *reconstructed*, and a directive delayed from before the restart
/// passes it (PR #167 round 6, finding 3). An incarnation is the part of a worker's identity a
/// restart cannot reconstruct, so a directive minted for the predecessor names a process that no
/// longer exists.
///
/// It is minted by the process it identifies — nothing else knows when a process began — and is
/// reported once, on `RegisterWorkerReq`, which is the only message that tells a controller a
/// generation exists.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkerIncarnation(NonZeroU64);

impl WorkerIncarnation {
    /// The incarnation `value` names, or `None` when it names none.
    ///
    /// Zero is the wire's "names no incarnation": it is the proto3 default and therefore what a
    /// peer predating the field reports, and no minted incarnation is zero.
    pub fn named(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    /// A fresh incarnation for the calling process.
    ///
    /// Random rather than a counter or a clock reading, because the value has to be distinct
    /// from what a *predecessor* process at the same worker id and generation minted, and a
    /// successor shares neither that process's memory nor, after a machine restart, necessarily
    /// a monotonic clock with it. There is no durable state to keep a counter in: a worker owns
    /// none, which is the whole reason this exists.
    pub fn mint() -> Self {
        loop {
            if let Some(incarnation) = Self::named(rand::random::<u64>()) {
                return incarnation;
            }
        }
    }

    /// Its wire value, which is never zero.
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

/// The durable record carries an incarnation as the `NonZeroU64` it is; these two conversions
/// are the only route between that and the addressing type, so a record cannot be read as an
/// incarnation the wire could not carry, nor written as one it could not name.
impl From<WorkerIncarnation> for NonZeroU64 {
    fn from(incarnation: WorkerIncarnation) -> Self {
        incarnation.0
    }
}

impl From<NonZeroU64> for WorkerIncarnation {
    fn from(value: NonZeroU64) -> Self {
        Self(value)
    }
}

/// The worker generation a fenced directive is addressed to, and the process answering for it.
///
/// The triple is the identity, not any part of it: an endpoint can be reused, so a restarted
/// worker at the same address and the same id is a different generation and must not answer for
/// its predecessor's requests (M11.D39d); and a *generation* can be reused too, by a process
/// that restarted under it, which is what [`WorkerIncarnation`] distinguishes. It mirrors the
/// durable [`FenceTarget`](crate::fencing::FenceTarget), which carries the same three values for
/// the same reason.
///
/// The incarnation is optional because zero is a shape the wire can carry — a peer predating the
/// field, or a controller addressing a worker that reported none — and a reader must be able to
/// tell that apart from a named one rather than defaulting it. What a *receiver* does about an
/// address that names none is policy, and belongs to the worker's guard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LifecycleTarget {
    worker_id: u64,
    generation: NonZeroU64,
    incarnation: Option<WorkerIncarnation>,
}

impl LifecycleTarget {
    /// The generation `worker_id`/`generation` addresses, or `None` if they address none.
    ///
    /// The generation decides, and neither the worker id nor the incarnation can.
    /// `job_statuses.run_id` starts at 0 and `states::scheduling`'s preamble increments it
    /// before that generation's workers are launched, so every generation a live worker runs
    /// under is at least 1 and zero addresses nothing. A worker id has no such floor: it is
    /// `worker.id` from configuration when that is set, and zero is a value an operator can
    /// write there. An incarnation of zero is the wire's "names none", which is a shape an
    /// address may legitimately have.
    pub fn addressed(worker_id: u64, generation: u64, incarnation: u64) -> Option<Self> {
        NonZeroU64::new(generation).map(|generation| {
            Self::in_generation(worker_id, generation, WorkerIncarnation::named(incarnation))
        })
    }

    /// The generation `worker_id`/`generation` addresses, for a caller that already holds a
    /// generation number it knows to be live.
    ///
    /// Total where [`Self::addressed`] is partial, and that is its whole purpose: a sender
    /// reading a job's own scheduling generation has already established the one thing
    /// [`Self::addressed`] checks, and a fallible constructor there would have to be answered
    /// with an `expect` at every send site. Both constructors agree by construction — this is
    /// the only one that builds the value.
    pub fn in_generation(
        worker_id: u64,
        generation: NonZeroU64,
        incarnation: Option<WorkerIncarnation>,
    ) -> Self {
        Self {
            worker_id,
            generation,
            incarnation,
        }
    }

    /// The worker this directive is addressed to.
    pub fn worker_id(&self) -> u64 {
        self.worker_id
    }

    /// The worker process this directive is addressed to, or `None` when it names none.
    pub fn incarnation(&self) -> Option<WorkerIncarnation> {
        self.incarnation
    }

    /// The worker generation this directive is addressed to, which is never zero.
    pub fn generation(&self) -> u64 {
        self.generation.get()
    }
}

/// A lifecycle fence together with the worker generation it is addressed to.
///
/// The two are one value because a fence is an instruction about a particular generation:
/// "advance past everything older than this" is only answerable by the generation that is being
/// asked. Holding them separately is what would let a caller carry a fence from one adoption and
/// a target from another; there is no constructor that takes one without the other.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FenceAddress {
    fence: NonZeroU64,
    target: LifecycleTarget,
}

impl FenceAddress {
    /// The fence `fence`, addressed to `target`.
    ///
    /// The only public constructor, and it takes the two halves at once: there is no way to
    /// name a fence here without naming the generation it is an instruction to, which is what
    /// stops a sender pairing a fence it read during one adoption with a target it decided on
    /// during another. The fence is a [`NonZeroU64`] because zero is the value that means "no
    /// fence" on the wire — cold adoption stores `lifecycle_fence + 1`, so no controller ever
    /// holds fence zero — and an address carrying it would decode as unfenced.
    pub fn under(fence: NonZeroU64, target: LifecycleTarget) -> Self {
        Self { fence, target }
    }

    /// The lifecycle fence, which is never zero.
    pub fn fence(&self) -> u64 {
        self.fence.get()
    }

    /// The worker generation the fence is addressed to.
    pub fn target(&self) -> LifecycleTarget {
        self.target
    }

    /// The authority to issue commits that a directive under this address confers.
    ///
    /// The worker id is the half that is dropped, and dropping it is the whole operation. A
    /// worker leader is admitted by a start addressed to *it*, and then commits to the other
    /// workers of the same job — which are the same generation, under the same fence, at
    /// different ids. Keeping the fence and the generation together across that step is what
    /// stops a leader pairing the fence it was started under with a generation it read
    /// somewhere else; there is no way here to replace one without replacing the other.
    pub fn commit_authority(self) -> CommitAuthority {
        CommitAuthority(Some(FencedCommits {
            fence: self.fence,
            generation: self.target.generation,
        }))
    }
}

/// What one sender's commits to one job's worker generation carry.
///
/// A struct around a private `Option` rather than a public enum, for the reason
/// [`FenceAddress`] has private fields: the two halves of a fenced authority must arrive
/// together or not at all, and a public variant would let a caller assemble a fence with a
/// generation it did not come from. The only ways to build one are [`Self::unfenced`],
/// [`Self::under`] — which takes both halves at once — and [`FenceAddress::commit_authority`].
///
/// It is deliberately not `FenceAddress`: an authority addresses a *generation*, and which
/// worker of that generation a particular commit names is decided per request by
/// [`Self::directive`]. One authority, many addresses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitAuthority(Option<FencedCommits>);

/// The fence and generation a fenced [`CommitAuthority`] issues under. Private: see there.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FencedCommits {
    fence: NonZeroU64,
    generation: NonZeroU64,
}

impl CommitAuthority {
    /// The pre-flag-day authority: every commit carries no fence and addresses no generation.
    ///
    /// Not "the fence was omitted" — it is the whole of the protocol a sender predating these
    /// fields speaks, and the commit it produces is byte-identical to that sender's.
    pub const fn unfenced() -> Self {
        Self(None)
    }

    /// Commits under `fence`, addressed to workers in `generation`.
    ///
    /// Both halves at once, and both [`NonZeroU64`]: zero is the wire's sentinel on each — a
    /// fence of zero decodes as unfenced and a generation of zero addresses nothing — so a
    /// value that could hold either could not produce an address without an `expect` at every
    /// send site.
    pub const fn under(fence: NonZeroU64, generation: NonZeroU64) -> Self {
        Self(Some(FencedCommits { fence, generation }))
    }

    /// The directive a commit to `worker_id`'s `incarnation` carries under this authority.
    ///
    /// Total: the two things that could make an address meaningless were settled when this
    /// value was built, so a send site has no failure to handle and no reason to reach for the
    /// four scalars underneath.
    ///
    /// The incarnation is per call rather than held here for the same reason the worker id is:
    /// an authority addresses a *generation*, and which process of which worker of that
    /// generation a particular commit is for is decided at the fan-out that sends it.
    pub fn directive(
        self,
        worker_id: u64,
        incarnation: Option<WorkerIncarnation>,
    ) -> CommitDirective {
        match self.0 {
            None => CommitDirective::Unfenced,
            Some(FencedCommits { fence, generation }) => {
                CommitDirective::Fenced(FenceAddress::under(
                    fence,
                    LifecycleTarget::in_generation(worker_id, generation, incarnation),
                ))
            }
        }
    }
}

/// What a decoded [`StartExecutionReq`]'s lifecycle fields say.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartDirective<'a> {
    /// Every lifecycle field is at its proto3 default.
    ///
    /// This is what a request from a controller predating M11.T26c decodes as, and it is also
    /// what a fence-capable controller sends while the fence protocol is inactive. Whether the
    /// receiving generation may act on it is the flag-day question (M11.D39e(i), M11.D75) and
    /// is not answered here.
    Unfenced,
    /// A fenced directive: one operation, under one fence, addressed to one generation.
    Fenced {
        /// The fence and the generation it is addressed to.
        address: FenceAddress,
        /// What the addressed generation is being asked to do.
        operation: LifecycleOperation,
        /// The outstanding attempt identifiers this directive revokes, possibly none.
        ///
        /// The set is acknowledged whole or not at all: [`StartExecutionResp`] carries no
        /// per-identifier result, so the response cannot describe a partial revocation and this
        /// build cannot be told about one.
        revoked_execution_ids: &'a [String],
    },
}

impl StartDirective<'_> {
    /// Writes this directive onto `req`'s five lifecycle fields.
    ///
    /// All five, on every arm: an unfenced directive zeroes them rather than leaving them, so a
    /// request cannot end up carrying half of one directive and half of whatever its literal was
    /// built with. `start_directive(req)` reads back exactly what was written here, which is
    /// what `send_tests` asserts for every shape.
    ///
    /// Nothing else about the request is touched. What a start *does* — the program, the
    /// assignments, the epochs — is the caller's, and this decides only under whose authority it
    /// is asked for.
    pub fn stamp(&self, req: &mut StartExecutionReq) {
        let (address, operation, revoked) = match self {
            StartDirective::Unfenced => (None, LifecycleOperation::Start, &[][..]),
            StartDirective::Fenced {
                address,
                operation,
                revoked_execution_ids,
            } => (Some(*address), *operation, *revoked_execution_ids),
        };
        let (fence, target_worker_id, target_worker_generation, target_worker_incarnation) =
            flat(address);
        req.lifecycle_fence = fence;
        req.target_worker_id = target_worker_id;
        req.target_worker_generation = target_worker_generation;
        req.target_worker_incarnation = target_worker_incarnation;
        req.lifecycle_operation = operation as i32;
        req.revoked_execution_ids = revoked.to_vec();
    }
}

/// What a decoded [`CommitReq`]'s lifecycle fields say.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitDirective {
    /// Every lifecycle field is at its proto3 default; see [`StartDirective::Unfenced`].
    Unfenced,
    /// A commit issued under one fence and addressed to one generation.
    Fenced(FenceAddress),
}

impl CommitDirective {
    /// Writes this directive onto `req`'s three lifecycle fields.
    ///
    /// All three, on both arms, for the reason [`StartDirective::stamp`] gives.
    pub fn stamp(&self, req: &mut CommitReq) {
        let (fence, target_worker_id, target_worker_generation, target_worker_incarnation) =
            flat(match self {
                CommitDirective::Unfenced => None,
                CommitDirective::Fenced(address) => Some(*address),
            });
        req.lifecycle_fence = fence;
        req.target_worker_id = target_worker_id;
        req.target_worker_generation = target_worker_generation;
        req.target_worker_incarnation = target_worker_incarnation;
    }
}

/// What a successful [`StartExecutionResp`] reports.
///
/// Only a successful response has one. A request the worker does not accept is answered with a
/// gRPC status and no `StartExecutionResp` at all, so nothing here means "rejected".
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservedSettlement {
    observed_fence: Option<u64>,
    outcome: StartExecutionOutcome,
}

impl ObservedSettlement {
    /// The highest fence the responding generation has acknowledged, or `None` for none.
    ///
    /// `None` is what a response from a worker predating the field reports and what a
    /// fence-capable generation that has acknowledged nothing reports. The two readings agree:
    /// zero is never an adopted fence, so there is no acknowledgement of fence zero for `None`
    /// to be confused with.
    pub fn observed_fence(&self) -> Option<u64> {
        self.observed_fence
    }

    /// What the response settles.
    pub fn outcome(&self) -> StartExecutionOutcome {
        self.outcome
    }
}

/// Reads the lifecycle fields of a decoded [`StartExecutionReq`] as one directive.
///
/// # Errors
///
/// Returns [`MalformedFenceFields`] when the fields do not describe one directive — a fence
/// without a target, a target without a fence, an operation or revocation carried without the
/// fence that would authorize it, an operation this build cannot name, or a revocation list
/// outside the bounds the controller's own ledger keeps.
pub fn start_directive(
    req: &StartExecutionReq,
) -> Result<StartDirective<'_>, MalformedFenceFields> {
    let operation = LifecycleOperation::try_from(req.lifecycle_operation).map_err(|_| {
        MalformedFenceFields::UnknownOperation {
            operation: req.lifecycle_operation,
        }
    })?;

    let Some(address) = fence_address(
        req.lifecycle_fence,
        req.target_worker_id,
        req.target_worker_generation,
        req.target_worker_incarnation,
    )?
    else {
        // No fence was carried, so every other lifecycle field must be at its default too:
        // each of them asks for something only a fence can authorize.
        match operation {
            LifecycleOperation::Start => {}
            LifecycleOperation::FenceOnly | LifecycleOperation::Revoke => {
                return Err(MalformedFenceFields::OperationWithoutFence { operation });
            }
        }
        if !req.revoked_execution_ids.is_empty() {
            return Err(MalformedFenceFields::RevocationWithoutFence {
                count: req.revoked_execution_ids.len(),
            });
        }
        return Ok(StartDirective::Unfenced);
    };

    check_revoked_ids(&req.revoked_execution_ids)?;

    Ok(StartDirective::Fenced {
        address,
        operation,
        revoked_execution_ids: &req.revoked_execution_ids,
    })
}

/// Reads the lifecycle fields of a decoded [`CommitReq`] as one directive.
///
/// # Errors
///
/// Returns [`MalformedFenceFields`] when a fence is carried without a target generation or a
/// target generation is addressed without a fence.
pub fn commit_directive(req: &CommitReq) -> Result<CommitDirective, MalformedFenceFields> {
    Ok(
        match fence_address(
            req.lifecycle_fence,
            req.target_worker_id,
            req.target_worker_generation,
            req.target_worker_incarnation,
        )? {
            Some(address) => CommitDirective::Fenced(address),
            None => CommitDirective::Unfenced,
        },
    )
}

/// Reads a successful [`StartExecutionResp`] as one settlement.
///
/// # Errors
///
/// Returns [`MalformedFenceFields`] for an outcome this build cannot name, and for a response
/// that claims to have acknowledged a fence while reporting none observed.
pub fn observed_settlement(
    resp: &StartExecutionResp,
) -> Result<ObservedSettlement, MalformedFenceFields> {
    let outcome = StartExecutionOutcome::try_from(resp.outcome).map_err(|_| {
        MalformedFenceFields::UnknownOutcome {
            outcome: resp.outcome,
        }
    })?;
    let observed_fence =
        (resp.observed_lifecycle_fence != 0).then_some(resp.observed_lifecycle_fence);

    match (outcome, observed_fence) {
        (StartExecutionOutcome::FenceAcknowledged, None)
        | (StartExecutionOutcome::Revoked, None) => {
            Err(MalformedFenceFields::AcknowledgementWithoutObservedFence { outcome })
        }
        (StartExecutionOutcome::Applied, _)
        | (StartExecutionOutcome::FenceAcknowledged, Some(_))
        | (StartExecutionOutcome::Revoked, Some(_)) => Ok(ObservedSettlement {
            observed_fence,
            outcome,
        }),
    }
}

/// The four wire scalars an address is carried as, or the four zeros that carry none.
///
/// The exact inverse of [`fence_address`], and beside it so that the two cannot drift: a shape
/// this produces is a shape that one accepts, which is what makes a stamped request decode back
/// into the directive it was stamped with.
fn flat(address: Option<FenceAddress>) -> (u64, u64, u64, u64) {
    match address {
        None => (0, 0, 0, 0),
        Some(address) => (
            address.fence(),
            address.target().worker_id(),
            address.target().generation(),
            address
                .target()
                .incarnation()
                .map_or(0, WorkerIncarnation::get),
        ),
    }
}

/// Pairs a fence with the generation and process it addresses, or refuses the parts that do not
/// pair.
fn fence_address(
    fence: u64,
    target_worker_id: u64,
    target_worker_generation: u64,
    target_worker_incarnation: u64,
) -> Result<Option<FenceAddress>, MalformedFenceFields> {
    // Checked before the pair below, because it is the one part that is never sufficient on its
    // own: an incarnation says which process of an addressed generation a directive is for, so a
    // message naming one while addressing nothing names a process for no purpose.
    if fence == 0 && target_worker_incarnation != 0 {
        return Err(MalformedFenceFields::IncarnationWithoutFence {
            worker_id: target_worker_id,
            incarnation: target_worker_incarnation,
        });
    }
    match (
        NonZeroU64::new(fence),
        LifecycleTarget::addressed(
            target_worker_id,
            target_worker_generation,
            target_worker_incarnation,
        ),
    ) {
        (None, None) if target_worker_id == 0 => Ok(None),
        (None, None) => Err(MalformedFenceFields::TargetWithoutFence {
            worker_id: target_worker_id,
            generation: target_worker_generation,
        }),
        (None, Some(target)) => Err(MalformedFenceFields::TargetWithoutFence {
            worker_id: target.worker_id(),
            generation: target.generation(),
        }),
        (Some(fence), None) => Err(MalformedFenceFields::FenceWithoutTarget {
            fence: fence.get(),
            target_worker_id,
        }),
        (Some(fence), Some(target)) => Ok(Some(FenceAddress { fence, target })),
    }
}

/// Bounds a revocation list against what the controller's own ledger can hold.
///
/// Both bounds are the durable record's, not new ones: [`MAX_FENCE_TARGETS`] is that record's
/// capacity for issued identifiers and [`MAX_ATTEMPT_ID_CHARS`] is the width the controller
/// mints them at. A list the durable side could not have produced is refused rather than
/// truncated, because a truncated revocation would leave an identifier applicable that the
/// sender believes it has revoked.
fn check_revoked_ids(ids: &[String]) -> Result<(), MalformedFenceFields> {
    if ids.len() > MAX_FENCE_TARGETS {
        return Err(MalformedFenceFields::TooManyRevokedIds { found: ids.len() });
    }
    for (index, id) in ids.iter().enumerate() {
        let found = id.chars().count();
        if found == 0 || found > MAX_ATTEMPT_ID_CHARS {
            return Err(MalformedFenceFields::MalformedRevokedId { index, found });
        }
    }
    Ok(())
}
