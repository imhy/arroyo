//! **The worker-first rollout proof (M11.T26h, design M11.D75).**
//!
//! The flag day is a *deployment* claim, and a deployment has two sides. Every other row in this
//! crate and in `arroyo-controller` tests one of them; this module puts both in one process and
//! runs the version matrix M11.D39g's post-flag-day-skew row names.
//!
//! # What is real here and what is modelled
//!
//! * The **worker** is real: [`WorkerServer`](crate::WorkerServer) with its production
//!   `WorkerGrpc::start_execution` handler, its one lock, its guard and its bounded identifier
//!   record. `register` is the same pair of calls `start_async` makes — the announcement it makes
//!   before issuing `register_worker`, and the answer it applies when that call returns — through
//!   the same lock.
//! * The **wire** is real: every request is stamped by [`StartDirective::stamp`] — the shared
//!   `arroyo-rpc` writer that `arroyo-controller`'s `FenceProtocol` calls, and the only thing
//!   allowed to write a lifecycle field — and then *encoded and decoded* before it is delivered,
//!   so what the worker reads is bytes rather than a struct a test filled in.
//! * The **controller's build** is modelled by the two things a controller of that build puts on
//!   the wire: the shape it stamps, and the `requires_lifecycle_fence` it answers a registration
//!   with. Those are one decision in `arroyo-controller` —
//!   `LifecycleMode::{requires_lifecycle_fence, is_available_in_production}` — and
//!   `the_registration_response_names_the_mode_and_not_a_literal` is the pin that keeps the
//!   registration byte derived from the mode rather than written as a literal. This module
//!   cannot reach that enum (it is `pub(crate)` in the other crate), so it takes the bool and
//!   says where it comes from.
//! * A **pre-flag-day worker** cannot be instantiated: this build's worker is fence-capable, and
//!   a build that is not is a different binary. It is modelled the only way a running system can
//!   model it — by what such a worker would see. It reads none of the lifecycle fields, so the
//!   question "does a legacy worker behave as it did?" is exactly "are these the bytes it used
//!   to receive?", and that is decided here by byte comparison.
//!
//! **This is a local simulation.** It is not, and does not claim to be, a cluster. Running the
//! three checks in `docs/lifecycle-fence-rollout.md` §5 against a real cluster is an operator
//! precondition of the flag day and is not substituted for by anything in this file.

use std::num::NonZeroU64;

use arroyo_rpc::fence_wire::{
    FenceAddress, LifecycleTarget, StartDirective, WorkerIncarnation, start_directive,
};
use arroyo_rpc::grpc::rpc::{
    LifecycleOperation, StartExecutionOutcome, StartExecutionReq, StartExecutionResp,
};
use prost::Message;
use tonic::{Code, Status};

use super::tests::{
    AMBIGUOUS, GENERATION, INCARNATION, WORKER, acknowledged, applied, call, generation,
    has_announced, idle, register, strict,
};

/// Which side of M11.D75's flag day a binary was cut on.
///
/// Two builds, named once, so the matrix below is a statement about deployments rather than a
/// list of cases someone assembled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Build {
    /// Anything older than the build that carries the lifecycle fence: it neither writes nor
    /// reads the fields, and its registration response leaves `requires_lifecycle_fence` at its
    /// proto3 default.
    PreFlagDay,
    /// This build.
    FenceCapable,
}

impl Build {
    /// Every build the rollout has to reason about, so the matrix quantifies rather than samples.
    const ALL: [Build; 2] = [Build::PreFlagDay, Build::FenceCapable];

    /// What a controller of this build answers `RegisterWorkerResp.requires_lifecycle_fence`
    /// with.
    ///
    /// In production this is `LifecycleMode::SELECTED.requires_lifecycle_fence()` and nothing
    /// else — see the module documentation.
    fn requires_lifecycle_fence(self) -> bool {
        match self {
            Build::PreFlagDay => false,
            Build::FenceCapable => true,
        }
    }

    /// The `StartExecution` a controller of this build sends `id` under, addressed to
    /// [`WORKER`]/[`GENERATION`].
    ///
    /// Stamped rather than assembled: `StartDirective::stamp` writes *every* lifecycle field on
    /// every arm, so a request cannot be half a directive and half whatever a literal left
    /// behind, and a pre-flag-day request is the zeroed shape rather than "the fields we
    /// remembered not to set".
    fn start(self, id: &str, fence: u64) -> StartExecutionReq {
        let mut req = StartExecutionReq {
            start_execution_id: id.to_string(),
            ..Default::default()
        };
        self.directive(fence, LifecycleOperation::Start)
            .stamp(&mut req);
        req
    }

    /// The fence advancement a controller of this build sends, if it sends one at all.
    fn fence_only(self, fence: u64) -> Option<StartExecutionReq> {
        match self {
            Build::PreFlagDay => None,
            Build::FenceCapable => {
                let mut req = StartExecutionReq::default();
                self.directive(fence, LifecycleOperation::FenceOnly)
                    .stamp(&mut req);
                Some(req)
            }
        }
    }

    fn directive(self, fence: u64, operation: LifecycleOperation) -> StartDirective<'static> {
        match self {
            Build::PreFlagDay => StartDirective::Unfenced,
            Build::FenceCapable => StartDirective::Fenced {
                address: FenceAddress::under(
                    NonZeroU64::new(fence).expect("a fenced controller has adopted a fence"),
                    LifecycleTarget::in_generation(
                        WORKER,
                        NonZeroU64::new(GENERATION).expect("no live generation is zero"),
                        WorkerIncarnation::named(INCARNATION),
                    ),
                ),
                operation,
                revoked_execution_ids: &[],
            },
        }
    }
}

/// Delivers `req` the way the network does: encoded, then decoded, then handed to the worker's
/// own handler.
///
/// The round trip is the point. A field this build sets that the receiving build does not know
/// about still reaches it as bytes, and a field left at its default never reaches it at all — so
/// "what does the other side see" is answered by protobuf rather than by the sender's struct.
#[allow(clippy::result_large_err)]
fn deliver(
    server: &crate::WorkerServer,
    req: &StartExecutionReq,
) -> Result<StartExecutionResp, Status> {
    let on_the_wire = req.encode_to_vec();
    let arrived =
        StartExecutionReq::decode(&on_the_wire[..]).expect("the worker decodes what was sent");
    call(server, arrived)
}

/// What a **pre-flag-day worker** would make of `req`: it reads none of the lifecycle fields, so
/// it behaves exactly as it would have for the same request with every one of them defaulted.
///
/// Answered by comparing encoded bytes rather than by reasoning about a struct: a field left set
/// encodes its key, so two requests that encode identically are indistinguishable to any reader,
/// including one built before the fields existed.
fn indistinguishable_from_the_pre_fence_shape(req: &StartExecutionReq) -> bool {
    let mut stripped = req.clone();
    StartDirective::Unfenced.stamp(&mut stripped);
    stripped.encode_to_vec() == req.encode_to_vec()
}

/// What one cell of the rollout matrix establishes.
///
/// The two readings are kept apart on purpose. A cell whose receiving side *is* this binary is
/// answered by the production handler and says what happened; a cell whose receiving side is a
/// build that reads none of these fields cannot be run at all, and the honest thing it can say
/// is whether the bytes are the ones that build has always received.
#[derive(Debug, Eq, PartialEq)]
enum Cell {
    /// The real `WorkerGrpc::start_execution` answered this.
    Answered(Result<StartExecutionOutcome, Code>),
    /// The receiving side is not this binary. `Whether the request is the pre-fence shape` is
    /// the whole of what can be said about it, and it is decided by byte comparison.
    NotThisBinary { pre_fence_shape: bool },
}

/// **Rollout matrix, all four version pairs.** What each side of the flag day does with the
/// other's messages.
///
/// Read as a table: for every controller build and every worker build, register the worker the
/// way that controller would and then send it that controller's start. The two cells on the
/// diagonal are ordinary same-version deployments; the two off it are the mixed-version states a
/// rolling upgrade passes through, and they are the reason the ordering in
/// `docs/lifecycle-fence-rollout.md` §3 is a requirement rather than a preference.
#[tokio::test]
async fn the_rollout_matrix_admits_only_the_orderings_the_flag_day_allows() {
    for controller in Build::ALL {
        for worker in Build::ALL {
            let cell = mixed_version_start(controller, worker);
            match (controller, worker) {
                // Step 2 of the rollout: workers upgraded, controllers not yet. The window
                // M11.D75 declares, and the one a worker-first rollout spends most of its time
                // in. The fence-capable worker is *not* in strict mode — nothing put it there —
                // and it applies the fence-less start.
                (Build::PreFlagDay, Build::FenceCapable) => assert_eq!(
                    cell,
                    Cell::Answered(Ok(StartExecutionOutcome::Applied)),
                    "a fence-capable worker registered to a pre-flag-day controller must accept \
                     its fence-less starts; refusing here is what would make a worker-first \
                     rollout impossible"
                ),
                // The deployment M11.D75 forbids. A fenced start is not a shape a legacy worker
                // has ever received, and it cannot refuse what it cannot read — it would take a
                // `FENCE_ONLY` for an ordinary start and run a program the controller did not
                // ask it to run. So the controller must never issue one, and what stops it is
                // the capability gate one crate over:
                // `a_worker_predating_the_reconciliation_contract_is_never_sent_a_start_execution`.
                (Build::FenceCapable, Build::PreFlagDay) => assert_eq!(
                    cell,
                    Cell::NotThisBinary {
                        pre_fence_shape: false
                    },
                    "a fenced controller's start is not the shape a pre-flag-day worker has \
                     ever received, which is exactly why controllers go last"
                ),
                // Both sides on the old build: the deployment as it was before any of this
                // existed, and the bytes say so.
                (Build::PreFlagDay, Build::PreFlagDay) => assert_eq!(
                    cell,
                    Cell::NotThisBinary {
                        pre_fence_shape: true
                    },
                    "the deployment as it was before any of this existed"
                ),
                // And the deployment after the flag day.
                (Build::FenceCapable, Build::FenceCapable) => assert_eq!(
                    cell,
                    Cell::Answered(Ok(StartExecutionOutcome::Applied)),
                    "with the fence advanced and acknowledged before the start was issued"
                ),
            }
        }
    }
}

/// One cell of [`the_rollout_matrix_admits_only_the_orderings_the_flag_day_allows`].
fn mixed_version_start(controller: Build, worker: Build) -> Cell {
    const FENCE: u64 = 4;
    let start = controller.start("attempt-1", FENCE);

    if worker == Build::PreFlagDay {
        return Cell::NotThisBinary {
            pre_fence_shape: indistinguishable_from_the_pre_fence_shape(&start),
        };
    }

    let (_shutdown, server) = generation(WORKER, GENERATION);
    register(&server, controller.requires_lifecycle_fence());
    assert_eq!(
        strict(&server),
        controller.requires_lifecycle_fence(),
        "{controller:?}: strict mode is what the registration response activated, and nothing \
         else"
    );

    // A fenced controller advances and has acknowledged its fence at this generation before it
    // issues a start to it — the active handshake, in the order M11.D39d requires.
    if let Some(handshake) = controller.fence_only(FENCE) {
        let acknowledgement = deliver(&server, &handshake).expect("the handshake is answered");
        assert_eq!(
            acknowledgement.outcome,
            StartExecutionOutcome::FenceAcknowledged as i32
        );
        assert_eq!(acknowledgement.observed_lifecycle_fence, FENCE);
        assert_eq!(acknowledged(&server), FENCE);
        assert!(idle(&server), "a fence advancement applies no program");
    }

    Cell::Answered(
        deliver(&server, &start)
            .map(|resp| {
                StartExecutionOutcome::try_from(resp.outcome)
                    .expect("the worker answers an outcome")
            })
            .map_err(|status| status.code()),
    )
}

/// **Registration precedes admission, and the fence handshake precedes the start.**
///
/// The ordering M11.D75 turns into a deployment step, asserted as an ordering rather than as
/// three independent facts: each stage is driven at the point *before* it is allowed to succeed
/// and again after, so a build that moved the gate earlier would pass the second half and fail
/// the first.
#[tokio::test]
async fn nothing_is_admitted_before_the_step_of_the_rollout_that_admits_it() {
    const FENCE: u64 = 9;
    let controller = Build::FenceCapable;
    let (_shutdown, server) = generation(WORKER, GENERATION);

    // Stage 0 — the worker is up and has not announced itself to any controller. A fenced
    // directive is refused, and definitively: nothing outside this process can know this
    // generation exists yet, so re-offering the directive could never make it admissible.
    assert!(!has_announced(&server));
    let before_registration = deliver(&server, &controller.start("attempt-1", FENCE))
        .expect_err("an unregistered generation admits no fenced start");
    assert_eq!(before_registration.code(), Code::FailedPrecondition);
    assert!(
        !AMBIGUOUS.contains(&before_registration.code()),
        "and the refusal is definitive, so the controller settles the attempt rather than \
         re-offering it forever"
    );
    assert!(applied(&server).is_none() && acknowledged(&server) == 0);

    // Stage 1 — registration completes. Strict mode is now on, monotonically.
    register(&server, controller.requires_lifecycle_fence());
    assert!(strict(&server) && has_announced(&server));

    // Stage 2 — a fenced start is still refused, because this generation has acknowledged no
    // fence yet and the one it is being addressed under is above the floor it holds. The
    // handshake is a step of its own and registration does not stand in for it.
    let acknowledgement = deliver(
        &server,
        &controller
            .fence_only(FENCE)
            .expect("a fenced controller sends a handshake"),
    )
    .expect("the handshake is answered once the generation has registered");
    assert_eq!(acknowledgement.observed_lifecycle_fence, FENCE);

    // Stage 3 — and only now does the start apply.
    let started = deliver(&server, &controller.start("attempt-1", FENCE))
        .expect("a fenced start applies once its fence has been acknowledged");
    assert_eq!(started.outcome, StartExecutionOutcome::Applied as i32);
    assert_eq!(applied(&server).as_deref(), Some("attempt-1"));
}

/// **Post-flag-day skew, in the direction that matters: a rollback that could emit fence-less
/// starts.**
///
/// This is the one-way half of `docs/lifecycle-fence-rollout.md` §6, as behaviour. A generation
/// that has entered strict mode refuses a pre-flag-day controller's start *forever* — the
/// refusal is definitive, so the predecessor cannot retry its way through it, and strict mode
/// never turns off, so waiting does not help either. Rolling a controller back to a build that
/// can only send that shape is therefore not a recovery; it is a job that can never be started.
#[tokio::test]
async fn a_strict_generation_never_accepts_a_predecessor_controllers_fence_less_start() {
    const FENCE: u64 = 5;
    let (_shutdown, server) = generation(WORKER, GENERATION);
    register(&server, Build::FenceCapable.requires_lifecycle_fence());
    deliver(
        &server,
        &Build::FenceCapable
            .fence_only(FENCE)
            .expect("a fenced controller sends a handshake"),
    )
    .expect("the handshake is answered");

    // The rolled-back controller. Its start is byte-identical to the one it sent before the
    // fields existed — that is what makes it a *predecessor*, and it is exactly why the worker
    // cannot tell it apart from one that is merely mis-configured.
    let rolled_back = Build::PreFlagDay.start("attempt-1", FENCE);
    assert!(indistinguishable_from_the_pre_fence_shape(&rolled_back));

    for attempt in 0..3 {
        let refusal = deliver(&server, &rolled_back)
            .expect_err("a strict generation admits no fence-less start");
        assert_eq!(
            refusal.code(),
            Code::FailedPrecondition,
            "attempt {attempt}: and it is the same refusal every time"
        );
        assert!(!AMBIGUOUS.contains(&refusal.code()));
    }
    assert!(
        applied(&server).is_none(),
        "nothing was applied, so the job is not half-started either — it simply cannot be \
         started by this controller at all"
    );
    assert!(
        strict(&server),
        "and strict mode is still on: it is monotone, so no amount of legacy traffic turns it \
         off and the only way out is a fence-capable controller or a coordinated stop"
    );

    // The other half of skew, and the reason the fence is addressed rather than global: a
    // *fenced* start from a controller that has adopted a lower fence than this generation
    // acknowledged is refused too, so a rollback to an older fence-capable controller cannot
    // reach past the one that superseded it either.
    let stale = Build::FenceCapable.start("attempt-2", FENCE - 1);
    let refusal = deliver(&server, &stale).expect_err("a stale fence is below this generation's");
    assert_eq!(refusal.code(), Code::FailedPrecondition);
    assert!(applied(&server).is_none());
}

/// **The pre-flag-day window is byte-exact, and it closes exactly when the registration response
/// says so.**
///
/// The compatibility claim a worker-first rollout rests on, stated at the wire: what a
/// fence-capable worker receives from a pre-flag-day controller is the same sequence of bytes
/// its predecessor received, and the *only* thing that changes what it will accept is the
/// registration response. Nothing about the worker's own build closes the window, which is what
/// makes step 2 of the rollout safe to leave running for as long as an operator needs.
#[tokio::test]
async fn the_compatibility_window_is_closed_by_the_registration_response_and_nothing_else() {
    let legacy_start = Build::PreFlagDay.start("attempt-1", 4);
    assert!(
        indistinguishable_from_the_pre_fence_shape(&legacy_start),
        "a pre-flag-day controller's start is byte-identical to the one sent before the \
         lifecycle fields existed"
    );

    for requires_lifecycle_fence in [false, true] {
        let (_shutdown, server) = generation(WORKER, GENERATION);
        register(&server, requires_lifecycle_fence);
        let outcome = deliver(&server, &legacy_start);
        assert_eq!(
            outcome.is_ok(),
            !requires_lifecycle_fence,
            "requires_lifecycle_fence={requires_lifecycle_fence}: the registration response is \
             the whole of what decides whether this generation still accepts the legacy shape"
        );
        assert_eq!(
            applied(&server).is_some(),
            !requires_lifecycle_fence,
            "requires_lifecycle_fence={requires_lifecycle_fence}: and what it accepted, it \
             applied"
        );
    }
}

/// Every directive this harness sends is one the shared writer produced, and it decodes back to
/// the directive it was stamped from.
///
/// The harness's own precondition, and not a formality: if a request here were assembled by hand
/// it could carry a combination `arroyo-controller` cannot produce, and the matrix above would
/// be testing a controller that does not exist.
#[test]
fn every_request_this_harness_sends_is_one_the_shared_writer_stamped() {
    for build in Build::ALL {
        let start = build.start("attempt-1", 4);
        let decoded = start_directive(&start).expect("the stamped request decodes");
        match build {
            Build::PreFlagDay => assert!(matches!(decoded, StartDirective::Unfenced)),
            Build::FenceCapable => {
                let StartDirective::Fenced {
                    address, operation, ..
                } = decoded
                else {
                    panic!("a fenced controller stamps a fenced directive");
                };
                assert_eq!(address.fence(), 4);
                assert_eq!(address.target().worker_id(), WORKER);
                assert_eq!(address.target().generation(), GENERATION);
                assert_eq!(operation, LifecycleOperation::Start);
            }
        }
        assert_eq!(
            build.fence_only(4).is_some(),
            build == Build::FenceCapable,
            "{build:?}: only a fence-capable controller has a handshake to send"
        );
    }
}
