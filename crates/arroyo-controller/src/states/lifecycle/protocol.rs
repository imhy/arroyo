//! What this controller sends one job's worker generations, and how it reads what comes back
//! (M11.T26c, design M11.D39e).
//!
//! M11.D39e puts the fence protocol on the messages arroyo already has, and
//! [`arroyo_rpc::fence_wire`] turns their flat fields back into whole directives. This module is
//! the controller's half of that: it decides *which* directive a job's requests carry, and it
//! classifies the transport statuses those requests come back as.
//!
//! # One decision, not three fields
//!
//! A fenced directive names a fence, a target worker and a target generation. Those are one
//! decision — "advance past everything older than this, you, the generation I am addressing" —
//! and the fence half of it can come from exactly one place: the job's own
//! [`LifecycleAuthority`], which M11.T26b made unconstructible from loose values so that a fence
//! and the epoch it was read with always describe one row. [`FenceProtocol::for_job`] is the only
//! way a directive is built here, it takes that authority whole, and it takes the generation the
//! job's own scheduling attempt raised it to. There is no constructor that takes a fence.
//!
//! # Why an unadopted authority is a refusal and not an unfenced request
//!
//! Under [`LifecycleMode::FencedV2`] a controller that has not adopted the job holds the
//! column's own default — fence zero, which is below every fence an adoption installs and is the
//! wire's sentinel for "no fence". Sending an unfenced start in that state is precisely the
//! post-flag-day failure M11.D39e(i) closes: the worker generations this controller registered
//! were put into strict mode by their registration response and would refuse it, and any that
//! did *not* refuse it would be one this controller had no authority over. So it fails closed —
//! [`UnfencedAuthority`] — rather than degrading to the legacy shape.
//!
//! # What is not decided here
//!
//! Nothing about *when*. The handshake that turns a registered generation into one this
//! controller may start is [`super::handshake`]; this module only says what its directives look
//! like and what the answers mean.

use std::num::NonZeroU64;

use arroyo_rpc::fence_wire::{
    CommitAuthority, FenceAddress, LifecycleTarget, StartDirective, WorkerIncarnation,
};
use arroyo_rpc::grpc::rpc::LifecycleOperation;
use arroyo_types::WorkerId;
use thiserror::Error;
use tonic::Code;

use super::LifecycleMode;
use super::fence::LifecycleAuthority;

/// What this controller's directives to one job's workers carry.
///
/// Built once per scheduling attempt from the job's mode and its durable authority, and carried
/// rather than re-derived, so that every request of one attempt is issued under the same fence
/// and addressed to the same generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FenceProtocol {
    /// M11.T08's shape: no fence, no addressed generation, nothing revoked.
    ///
    /// This is what a controller predating M11.T26c sends and what a fence-capable controller
    /// sends before the flag day. It is not "the fence was omitted": it is the whole of the
    /// pre-flag-day protocol, and the worker admits it exactly as it always did.
    Legacy,
    /// M11.D39e's shape: every directive carries this fence, addressed to this generation.
    Fenced(FencedGeneration),
}

impl FenceProtocol {
    /// The fence every directive issued under this protocol carries.
    ///
    /// Zero under [`Self::Legacy`], which is the wire's own sentinel for "carries no fence"
    /// rather than a stand-in for one: a pre-flag-day request is fence-less, and an inventory of
    /// what it issued records that as the fence its identifiers were issued under. It is derived
    /// from the protocol rather than written as a literal at the two fan-out sites, so
    /// activating the fence cannot leave a hard-coded zero behind at either.
    pub(crate) fn fence(self) -> u64 {
        match self {
            FenceProtocol::Legacy => 0,
            FenceProtocol::Fenced(generation) => generation.fence(),
        }
    }
}

/// The fence one controller holds over one job, and the worker generation it addresses.
///
/// Both halves are [`NonZeroU64`], which is what makes [`Self::address`] total: zero is the
/// wire's sentinel on both — a fence of zero decodes as "unfenced" and a generation of zero
/// addresses nothing — so a value that could hold either could not be turned into an address
/// without an `expect` at every send site. They are checked once, here, at the one place the
/// value comes into existence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FencedGeneration {
    fence: NonZeroU64,
    generation: NonZeroU64,
}

/// A controller that must fence but holds no fence to send.
///
/// Either it has not adopted the job — the row's `DEFAULT 0`, which no adoption can install
/// because adoption stores `lifecycle_fence + 1` — or the attempt addresses generation zero,
/// which no launched worker generation runs under. Both are states in which this controller
/// cannot name what it is entitled to do, and both fail closed.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum UnfencedAuthority {
    /// No controller has adopted this job's durable lifecycle authority.
    #[error(
        "job {job_id} carries no adopted lifecycle fence, so this controller cannot address its \
         worker generations under one"
    )]
    Unadopted {
        /// The job whose authority is still at the column's default.
        job_id: String,
    },
    /// The attempt addresses generation zero, which no live worker generation runs under.
    #[error(
        "job {job_id} is scheduling generation 0, which no launched worker generation runs under"
    )]
    UnlaunchedGeneration {
        /// The job whose scheduling generation is still at the row's default.
        job_id: String,
    },
}

impl FenceProtocol {
    /// The protocol a job's scheduling attempt runs under.
    ///
    /// `authority` is taken whole rather than as a fence, so the fence a directive carries is
    /// necessarily the one the job's own row produced; `generation` is the scheduling generation
    /// this attempt raised the job to, which is the generation its workers registered under and
    /// therefore the only one they will answer for.
    ///
    /// # Errors
    ///
    /// [`UnfencedAuthority`] under [`LifecycleMode::FencedV2`] when this controller holds no
    /// adopted fence, or when the attempt addresses no launched generation. Under
    /// [`LifecycleMode::LegacyT08`] — the pre-flag-day peer — there is nothing to fail: the
    /// answer is the legacy protocol whatever the row says.
    pub(crate) fn for_job(
        mode: LifecycleMode,
        authority: &LifecycleAuthority,
        generation: u64,
    ) -> Result<Self, UnfencedAuthority> {
        if !mode.requires_lifecycle_fence() {
            return Ok(FenceProtocol::Legacy);
        }
        let job_id = || (**authority.job_id()).clone();
        let fence = NonZeroU64::new(authority.fence().get())
            .ok_or_else(|| UnfencedAuthority::Unadopted { job_id: job_id() })?;
        let generation = NonZeroU64::new(generation)
            .ok_or_else(|| UnfencedAuthority::UnlaunchedGeneration { job_id: job_id() })?;
        Ok(FenceProtocol::Fenced(FencedGeneration {
            fence,
            generation,
        }))
    }

    /// What a gRPC status about an issued attempt settles.
    ///
    /// Kept as a method on the protocol even though [`transport_settlement`] no longer reads a
    /// mode, because the fan-out's retry table is a property of the directives it issued and
    /// reading it off the value that issued them is what
    /// `the_fan_out_reads_its_retry_table_from_the_classification` pins. See
    /// [`transport_settlement`] for why there is one taxonomy now and not two.
    pub(crate) fn transport_settlement(self, code: Code) -> TransportSettlement {
        transport_settlement(code)
    }

    /// The authority this controller's commits to the job's workers are issued under.
    ///
    /// A commit needs no handshake of its own: it is issued to a generation this controller has
    /// already started, which under the fenced protocol means one that acknowledged this fence
    /// before its `StartExecution` was sent. What the fence buys here is the other direction —
    /// a generation that has since acknowledged a *higher* fence refuses this commit, so a
    /// superseded controller cannot finish a two-phase commit against a job it has lost
    /// (M11.D39d).
    ///
    /// The generation is carried and the worker is not, because which worker of that generation
    /// a particular commit names is decided per request, at the fan-out that sends it.
    pub(crate) fn commit_authority(self) -> CommitAuthority {
        match self {
            FenceProtocol::Legacy => CommitAuthority::unfenced(),
            FenceProtocol::Fenced(generation) => {
                CommitAuthority::under(generation.fence, generation.generation)
            }
        }
    }
}

impl FencedGeneration {
    /// The fence, addressed to `worker`'s `incarnation` in this generation.
    ///
    /// Total, and that is the point: the two things that could make an address meaningless were
    /// settled when this value was built, so a send site has no failure to handle and no reason
    /// to reach for the four scalars underneath.
    ///
    /// The incarnation is per call because it is per *worker*, and this value is per generation:
    /// each worker of a generation is its own process and reported its own nonce at
    /// registration. An `Option` because both of its sources may name none — a worker predating
    /// `RegisterWorkerReq::worker_incarnation`, and a durable fencing record written before the
    /// field — and a generation that has one refuses an address that names none, which is the
    /// fail-closed reading (M11.D39d, PR #167 round 6).
    pub(crate) fn address(
        self,
        worker: WorkerId,
        incarnation: Option<WorkerIncarnation>,
    ) -> FenceAddress {
        FenceAddress::under(
            self.fence,
            LifecycleTarget::in_generation(worker.0, self.generation, incarnation),
        )
    }

    /// The `FENCE_ONLY` directive that asks `worker` to advance to this fence and acknowledge it.
    ///
    /// It revokes nothing. Advancing the fence is what supersedes older identifiers; naming them
    /// as well is M11.T26e's reconciliation, and the worker refuses a `FENCE_ONLY` that names
    /// any.
    pub(crate) fn fence_only(
        self,
        worker: WorkerId,
        incarnation: Option<WorkerIncarnation>,
    ) -> StartDirective<'static> {
        StartDirective::Fenced {
            address: self.address(worker, incarnation),
            operation: LifecycleOperation::FenceOnly,
            revoked_execution_ids: &[],
        }
    }

    /// The fence this generation's directives carry.
    pub(crate) fn fence(self) -> u64 {
        self.fence.get()
    }

    /// The worker generation this protocol addresses.
    pub(crate) fn generation(self) -> u64 {
        self.generation.get()
    }
}

/// What a gRPC status says about the attempt it answered.
///
/// The distinction is the whole of M11.D39e(iii)/(iv), and getting it wrong is unsafe in both
/// directions: reading a definitive refusal as ambiguous retries an identifier the worker has
/// permanently settled, and reading an ambiguous transport outcome as definitive abandons an
/// attempt a worker may still be applying.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportSettlement {
    /// The worker's own answer about this attempt. Re-offering the identifier cannot change it.
    ///
    /// Only a later scheduling attempt, under a new generation, may try again — which is what
    /// makes `Aborted` settlement rather than a retry after the flag day.
    Definitive,
    /// Nothing is known about whether the request reached the worker or what it did with it.
    ///
    /// The same identifier may be re-offered under the same admission, within the landed budget.
    Ambiguous,
}

/// How a controller reads a gRPC status about an issued `StartExecution`.
///
/// # Why it is exhaustive, and why there is no catch-all
///
/// The set of codes a worker can produce is not a set this crate controls: a status can be
/// synthesized by `tonic`, by a proxy, or by a worker built from a newer or older source tree.
/// A catch-all arm would give every code nobody thought about the same reading, and the unsafe
/// direction is the likely one — `ResourceExhausted` is what M11.T26d's worker answers when its
/// bounded identifier record is full, and retrying that forever is exactly the wedge M11.T08's
/// round-15 fix removed. So every one of `tonic::Code`'s variants is named, a new one added by a
/// dependency upgrade fails to compile here, and the decision is taken once for the whole crate.
///
/// # One taxonomy, since the flag day
///
/// This used to take a [`LifecycleMode`], because exactly one code read differently on the two
/// sides of the flag day: `Aborted`. Before it, `Aborted` was M11.T08's "the worker's phase lock
/// was contended, nothing was applied" and the fan-out retried the same identifier under the
/// same admission. M11.D39e(iii) makes `Aborted` a *definitive* "nothing applied": the worker
/// settles it, and only a later scheduling attempt — under a new generation — may try again.
///
/// The reading changed because what a retry means changed: under the fence, a same-admission
/// retry of an attempt the worker has settled is a second start the controller cannot account
/// for. **M11.T26h removed the parameter with the arm**, so this controller has one taxonomy and
/// cannot be persuaded to read `Aborted` as ambiguous by anything, including a fixture that
/// names the pre-flag-day peer. `post_flag_day_skew_moves_exactly_one_transport_code` is the pin
/// that says so: it quantifies over both modes and requires *zero* codes to differ.
pub(crate) fn transport_settlement(code: Code) -> TransportSettlement {
    match code {
        // The four M11.D39e(iv) names. Each says the controller stopped hearing, not that the
        // worker stopped working: a reset stream, an expired client deadline or an unreachable
        // peer leaves a handler that may already have applied the request.
        Code::Cancelled | Code::Unknown | Code::DeadlineExceeded | Code::Unavailable => {
            TransportSettlement::Ambiguous
        }
        // The one code the flag day moved. See above: it is settlement now, in every mode.
        Code::Aborted
        // `Ok` reaches here only if a caller classifies a status it built for a successful
        // response. A success is settled by definition, and calling it ambiguous would retry a
        // request the worker answered.
        | Code::Ok
        // Every refusal M11.T26d's worker guard gives is one of the next four, and each is an
        // authoritative statement about this generation: a malformed directive, a strict-mode
        // or target-generation or phase refusal, a full identifier record, or a poisoned lock.
        | Code::InvalidArgument
        | Code::FailedPrecondition
        | Code::ResourceExhausted
        | Code::Internal
        // And the rest, which this build's worker does not produce but a peer, a proxy or a
        // future worker might. Each names a condition re-sending the same identifier cannot
        // change, so each settles rather than wedging the fan-out.
        | Code::NotFound
        | Code::AlreadyExists
        | Code::PermissionDenied
        | Code::OutOfRange
        | Code::Unimplemented
        | Code::DataLoss
        | Code::Unauthenticated => TransportSettlement::Definitive,
    }
}
