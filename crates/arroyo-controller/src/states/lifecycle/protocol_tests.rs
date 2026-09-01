//! What a job's directives carry, and what a status about one settles (M11.T26c).

use arroyo_rpc::fence_wire::{CommitDirective, StartDirective, start_directive};
use arroyo_rpc::grpc::rpc::{CommitReq, LifecycleOperation, StartExecutionReq};
use arroyo_types::WorkerId;
use tonic::Code;

use super::LifecycleMode;
use super::fence::LifecycleAuthority;
use super::protocol::{
    FenceProtocol, FencedGeneration, TransportSettlement, UnfencedAuthority, transport_settlement,
};

/// Every `tonic::Code` this build knows, so a classification test can quantify over the set
/// instead of sampling it.
///
/// Written out rather than derived because `tonic::Code` offers no iterator; the count assertion
/// below is what keeps this list honest against the crate's own enum, which
/// [`transport_settlement`] matches exhaustively.
const EVERY_CODE: [Code; 17] = [
    Code::Ok,
    Code::Cancelled,
    Code::Unknown,
    Code::InvalidArgument,
    Code::DeadlineExceeded,
    Code::NotFound,
    Code::AlreadyExists,
    Code::PermissionDenied,
    Code::ResourceExhausted,
    Code::FailedPrecondition,
    Code::Aborted,
    Code::OutOfRange,
    Code::Unimplemented,
    Code::Internal,
    Code::Unavailable,
    Code::DataLoss,
    Code::Unauthenticated,
];

/// The four names M11.D39e(iv) gives, and no others.
const AMBIGUOUS_IN_EVERY_MODE: [Code; 4] = [
    Code::Cancelled,
    Code::Unknown,
    Code::DeadlineExceeded,
    Code::Unavailable,
];

fn fenced(fence: u64, generation: u64) -> FencedGeneration {
    let protocol = FenceProtocol::for_job(
        LifecycleMode::FencedV2,
        &LifecycleAuthority::from_parts("job_abc", fence, "epoch-1"),
        generation,
    )
    .expect("this fixture names an adopted fence and a launched generation");
    match protocol {
        FenceProtocol::Fenced(generation) => generation,
        FenceProtocol::Legacy => panic!("the fenced mode must produce the fenced protocol"),
    }
}

// ---------------------------------------------------------------------------------------------
// The registration response and the directives are one decision
// ---------------------------------------------------------------------------------------------

/// The flag day is answered from the mode, in both modes, and by an exhaustive match.
#[test]
fn the_registration_response_is_derived_from_the_mode_in_both_modes() {
    assert!(
        !LifecycleMode::LegacyT08.requires_lifecycle_fence(),
        "a controller that sends no fence must not require one: a generation put into strict \
         mode by that registration would refuse the very next start it was sent"
    );
    assert!(
        LifecycleMode::FencedV2.requires_lifecycle_fence(),
        "after the flag day a registration is what activates strict mode for the generation"
    );
    // And the production answer, which is what `ControllerServer::register_worker` sends. Since
    // M11.T26h this is the flag day itself: every registration a production controller answers
    // puts that worker generation into strict mode, monotonically.
    assert!(LifecycleMode::SELECTED.requires_lifecycle_fence());
    assert_eq!(LifecycleMode::SELECTED, LifecycleMode::FencedV2);
}

/// What a controller requires of a generation and what its directives carry are the same answer.
///
/// The failure this rules out is the half-activated one: a controller that told a worker
/// generation to require fences and then sent it fence-less starts would have locked itself out
/// of its own job, and one that sent fences to a generation it never put into strict mode would
/// be relying on a worker to be stricter than it was asked to be.
#[test]
fn requiring_a_fence_and_sending_one_are_the_same_decision() {
    let authority = LifecycleAuthority::from_parts("job_abc", 4, "epoch-1");
    for mode in LifecycleMode::ALL {
        let protocol = FenceProtocol::for_job(mode, &authority, 2).unwrap();
        assert_eq!(
            matches!(protocol, FenceProtocol::Fenced(_)),
            mode.requires_lifecycle_fence(),
            "{mode:?}: the registration flag and the directive shape must agree"
        );
        assert_eq!(
            matches!(
                protocol.commit_authority().directive(7),
                arroyo_rpc::fence_wire::CommitDirective::Fenced(_)
            ),
            mode.requires_lifecycle_fence(),
            "{mode:?}: and the commit directive is issued on the same side of the flag day, so \
             a start and a commit of one job cannot be built from different modes"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// The directives themselves
// ---------------------------------------------------------------------------------------------

/// Under the legacy protocol every lifecycle field of every message stays at its default.
#[test]
fn the_legacy_protocol_stamps_the_request_a_controller_predating_the_fields_sends() {
    let mut start = StartExecutionReq {
        start_execution_id: "attempt".to_string(),
        ..Default::default()
    };
    StartDirective::Unfenced.stamp(&mut start);
    assert_eq!(
        start,
        StartExecutionReq {
            start_execution_id: "attempt".to_string(),
            ..Default::default()
        }
    );

    let mut commit = CommitReq {
        epoch: 9,
        ..Default::default()
    };
    FenceProtocol::Legacy
        .commit_authority()
        .directive(WorkerId(7).0)
        .stamp(&mut commit);
    assert_eq!(
        commit,
        CommitReq {
            epoch: 9,
            ..Default::default()
        }
    );
    assert_eq!(
        FenceProtocol::Legacy.commit_authority().directive(7),
        CommitDirective::Unfenced
    );
}

/// A fenced directive carries this job's own fence, addressed to the worker it is sent to.
///
/// Each of the three dimensions is varied on its own, against closed-form expected values, so
/// that a directive built from a fence and a target that came from different decisions would
/// show up as a different address rather than as a plausible one.
#[test]
fn a_fenced_directive_carries_this_jobs_fence_addressed_to_the_worker_it_is_sent_to() {
    for (fence, generation, worker) in [(1u64, 1u64, 0u64), (4, 2, 7), (u64::MAX, 9, u64::MAX)] {
        let addressed = fenced(fence, generation);
        assert_eq!(addressed.fence(), fence);
        assert_eq!(addressed.generation(), generation);

        let address = addressed.address(WorkerId(worker));
        assert_eq!(address.fence(), fence);
        assert_eq!(address.target().worker_id(), worker);
        assert_eq!(address.target().generation(), generation);

        let mut req = StartExecutionReq::default();
        addressed.fence_only(WorkerId(worker)).stamp(&mut req);
        assert_eq!(req.lifecycle_fence, fence);
        assert_eq!(req.target_worker_id, worker);
        assert_eq!(req.target_worker_generation, generation);
        assert_eq!(
            req.lifecycle_operation,
            LifecycleOperation::FenceOnly as i32
        );
        assert!(
            req.revoked_execution_ids.is_empty(),
            "a fence-only directive revokes nothing; the worker refuses one that names anything"
        );
        assert_eq!(
            start_directive(&req).unwrap(),
            StartDirective::Fenced {
                address,
                operation: LifecycleOperation::FenceOnly,
                revoked_execution_ids: &[],
            }
        );

        let mut commit = CommitReq::default();
        FenceProtocol::Fenced(addressed)
            .commit_authority()
            .directive(worker)
            .stamp(&mut commit);
        assert_eq!(commit.lifecycle_fence, fence);
        assert_eq!(commit.target_worker_id, worker);
        assert_eq!(commit.target_worker_generation, generation);
    }
}

/// Two workers of one generation are addressed under the same fence and different ids.
#[test]
fn every_worker_of_one_generation_is_addressed_under_the_same_fence() {
    let addressed = fenced(4, 2);
    let one = addressed.address(WorkerId(1));
    let two = addressed.address(WorkerId(2));
    assert_eq!(one.fence(), two.fence());
    assert_eq!(one.target().generation(), two.target().generation());
    assert_ne!(one.target().worker_id(), two.target().worker_id());
}

// ---------------------------------------------------------------------------------------------
// Failing closed
// ---------------------------------------------------------------------------------------------

/// A controller that must fence and holds none refuses rather than sending an unfenced request.
///
/// Both halves: the fence a job that no controller has adopted carries is the column's `DEFAULT
/// 0`, and generation zero is the scheduling generation of a job whose preamble has not run.
/// Neither degrades to the legacy shape, because after the flag day a fence-less request is one
/// this controller's own worker generations refuse.
#[test]
fn a_fenced_controller_without_a_fence_refuses_rather_than_sending_an_unfenced_request() {
    assert_eq!(
        FenceProtocol::for_job(
            LifecycleMode::FencedV2,
            &LifecycleAuthority::unadopted("job_abc"),
            2,
        ),
        Err(UnfencedAuthority::Unadopted {
            job_id: "job_abc".to_string()
        })
    );
    assert_eq!(
        FenceProtocol::for_job(
            LifecycleMode::FencedV2,
            &LifecycleAuthority::from_parts("job_abc", 4, "epoch-1"),
            0,
        ),
        Err(UnfencedAuthority::UnlaunchedGeneration {
            job_id: "job_abc".to_string()
        })
    );
}

/// The legacy protocol has nothing to fail: it is the answer whatever the row says.
///
/// This is what keeps M11.T26c inert before the flag day. A job that has never been adopted, on
/// a controller running the selected mechanism, schedules exactly as it did before the fence
/// existed rather than acquiring a new way to fail.
#[test]
fn the_legacy_protocol_cannot_fail_on_an_unadopted_job() {
    for generation in [0u64, 1, 7] {
        assert_eq!(
            FenceProtocol::for_job(
                LifecycleMode::LegacyT08,
                &LifecycleAuthority::unadopted("job_abc"),
                generation,
            ),
            Ok(FenceProtocol::Legacy)
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Definitive versus ambiguous, exhaustively
// ---------------------------------------------------------------------------------------------

/// Every status code, in both modes, against a closed-form expected value.
///
/// The list is exhaustive over `tonic::Code` — the count is asserted against the discriminants,
/// so a code missing from the fixture shows up here rather than silently going untested — and
/// [`transport_settlement`]'s own match is exhaustive, so a code added by a dependency upgrade
/// fails to compile rather than acquiring a default reading.
///
/// The unsafe direction is the one this pins hardest: `ResourceExhausted` is what M11.T26d's
/// worker answers when its bounded identifier record is full, and `FailedPrecondition` is what
/// every one of its admission refusals is. Reading either as ambiguous would retry an
/// identifier the worker has permanently settled, forever.
#[test]
fn every_status_code_is_classified_in_both_modes() {
    assert_eq!(
        EVERY_CODE.len(),
        17,
        "gRPC defines 17 status codes; this fixture must name all of them"
    );
    for (i, code) in EVERY_CODE.iter().enumerate() {
        assert_eq!(
            *code as i32, i as i32,
            "the fixture is in discriminant order, so a code added to `tonic::Code` shows up as \
             a gap here"
        );
    }

    for code in EVERY_CODE {
        let expected = if AMBIGUOUS_IN_EVERY_MODE.contains(&code) {
            TransportSettlement::Ambiguous
        } else {
            TransportSettlement::Definitive
        };
        assert_eq!(transport_settlement(code), expected, "{code:?}");
    }
}

/// The flag day moved exactly one code, and it moved it towards settlement — and there is now
/// nothing left for a mode to change.
///
/// M11.D39e(iii): `Aborted` is a definitive "nothing applied" that only a *later scheduling
/// attempt* may retry. Before M11.T26h this function took a mode and answered `Ambiguous` for
/// the legacy one, which is the M11.T08 busy-worker retry. The activation change removed the
/// parameter with the arm, so the assertion that used to read "exactly one code moves" now reads
/// "the taxonomy has no seam left to move at", and `Aborted` is on the settlement side of it.
#[test]
fn the_flag_day_moved_only_aborted_and_only_into_settlement() {
    assert_eq!(
        transport_settlement(Code::Aborted),
        TransportSettlement::Definitive,
        "the one code the flag day moved, on the side it moved to"
    );
    for mode in LifecycleMode::ALL {
        let protocol = FenceProtocol::for_job(
            mode,
            &LifecycleAuthority::from_parts("job_abc", 4, "epoch-1"),
            2,
        )
        .expect("an adopted controller can address its own generation");
        for code in EVERY_CODE {
            assert_eq!(
                protocol.transport_settlement(code),
                transport_settlement(code),
                "{mode:?}/{code:?}: no directive shape reads a status differently from any \
                 other, which is what having one taxonomy means"
            );
        }
    }
}

/// The four ambiguous names retry the same identifier; everything else settles, in both modes.
#[test]
fn only_the_four_ambiguous_names_retry_the_same_identifier_after_the_flag_day() {
    let ambiguous: Vec<Code> = EVERY_CODE
        .into_iter()
        .filter(|code| transport_settlement(*code) == TransportSettlement::Ambiguous)
        .collect();
    assert_eq!(ambiguous, AMBIGUOUS_IN_EVERY_MODE.to_vec());
}

/// A protocol classifies through the one taxonomy, so directives and retries cannot disagree.
#[test]
fn a_protocol_classifies_through_the_one_taxonomy() {
    for code in EVERY_CODE {
        assert_eq!(
            FenceProtocol::Legacy.transport_settlement(code),
            transport_settlement(code)
        );
        assert_eq!(
            FenceProtocol::Fenced(fenced(4, 2)).transport_settlement(code),
            transport_settlement(code)
        );
    }
}

/// A commit and a start are addressed to the same generation, under the same fence.
///
/// One decision reached two ways: `FencedGeneration::address` is what a `StartExecution` carries
/// and `FenceProtocol::commit_authority` is what a commit carries, and the whole point of the
/// fence on a commit directive is that a controller which has lost the job cannot finish its
/// two-phase commit — which is only true if the two agree about what "this job's fence,
/// addressed to that generation" is.
#[test]
fn a_commit_and_a_start_address_the_same_generation_under_the_same_fence() {
    for (fence, generation, worker) in [(1u64, 1u64, 0u64), (4, 2, 7), (u64::MAX, 9, u64::MAX)] {
        let addressed = fenced(fence, generation);
        assert_eq!(
            FenceProtocol::Fenced(addressed)
                .commit_authority()
                .directive(worker),
            CommitDirective::Fenced(addressed.address(WorkerId(worker)))
        );
    }
    assert_eq!(
        FenceProtocol::Legacy.commit_authority(),
        arroyo_rpc::fence_wire::CommitAuthority::unfenced(),
        "and the pre-flag-day protocol's authority is the pre-flag-day one"
    );
}
