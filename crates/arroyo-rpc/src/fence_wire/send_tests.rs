//! The send half of the seam: what [`StartDirective::stamp`] and [`CommitDirective::stamp`]
//! write, and that reading it back yields the directive that was written.
//!
//! Two claims are worth separating. The first is *agreement*: a stamped message decodes into
//! the same directive, so the sender and the receiver of a fenced request cannot disagree about
//! which of the two shapes it has. The second is *totality*: every lifecycle field is written on
//! every arm, so a request carrying an unfenced directive is byte-identical to one from a
//! controller that predates these fields — however dirty the value it was stamped onto was.
//! Only the second catches a `stamp` that forgets a field, because a forgotten field on a
//! freshly built request is already at the value the directive wanted.

use super::*;
use crate::grpc::rpc::CommitReq;
use prost::Message;

/// A fence and the generation it addresses, built the only way a sender can build one.
fn address(fence: u64, worker_id: u64, generation: u64) -> FenceAddress {
    FenceAddress::under(
        NonZeroU64::new(fence).expect("this fixture names a live fence"),
        LifecycleTarget::in_generation(
            worker_id,
            NonZeroU64::new(generation).expect("this fixture names a live generation"),
        ),
    )
}

/// A start request whose every lifecycle field carries a value no directive here asks for.
///
/// The point of the fixture: stamping onto it must leave nothing of it behind. A test that
/// stamped onto `Default::default()` would pass even for a `stamp` that wrote nothing at all.
fn dirtied_start_request() -> StartExecutionReq {
    StartExecutionReq {
        start_execution_id: "attempt".to_string(),
        lifecycle_fence: 999,
        target_worker_id: 998,
        target_worker_generation: 997,
        lifecycle_operation: LifecycleOperation::Revoke as i32,
        revoked_execution_ids: vec!["stale".to_string()],
        ..Default::default()
    }
}

/// The same, for a commit.
fn dirtied_commit_request() -> CommitReq {
    CommitReq {
        epoch: 4,
        committing_data: Default::default(),
        lifecycle_fence: 999,
        target_worker_id: 998,
        target_worker_generation: 997,
    }
}

// ---------------------------------------------------------------------------------------------
// Agreement: what is stamped is what is read
// ---------------------------------------------------------------------------------------------

/// Every start directive this build can send reads back as itself.
#[test]
fn every_stamped_start_directive_reads_back_as_the_directive_that_was_stamped() {
    let revoked = vec!["one".to_string(), "two".to_string()];
    let directives = [
        StartDirective::Unfenced,
        StartDirective::Fenced {
            address: address(7, 42, 3),
            operation: LifecycleOperation::Start,
            revoked_execution_ids: &[],
        },
        StartDirective::Fenced {
            address: address(1, 0, 1),
            operation: LifecycleOperation::FenceOnly,
            revoked_execution_ids: &[],
        },
        StartDirective::Fenced {
            address: address(u64::MAX, u64::MAX, u64::MAX),
            operation: LifecycleOperation::Revoke,
            revoked_execution_ids: &revoked,
        },
    ];
    for directive in directives {
        let mut req = dirtied_start_request();
        directive.stamp(&mut req);

        assert_eq!(
            start_directive(&req).expect("a directive this build stamped"),
            directive,
            "the seam must read back what it wrote, in this process"
        );
        // And across the wire, which is the claim that matters: the receiver decodes bytes.
        let decoded = StartExecutionReq::decode(&req.encode_to_vec()[..]).unwrap();
        assert_eq!(
            start_directive(&decoded).expect("a directive this build stamped"),
            directive,
            "and after an encode/decode round trip"
        );
    }
}

/// Both commit directives read back as themselves.
#[test]
fn every_stamped_commit_directive_reads_back_as_the_directive_that_was_stamped() {
    for directive in [
        CommitDirective::Unfenced,
        CommitDirective::Fenced(address(9, 5, 2)),
    ] {
        let mut req = dirtied_commit_request();
        directive.stamp(&mut req);
        assert_eq!(commit_directive(&req).unwrap(), directive);

        let decoded = CommitReq::decode(&req.encode_to_vec()[..]).unwrap();
        assert_eq!(commit_directive(&decoded).unwrap(), directive);
    }
}

// ---------------------------------------------------------------------------------------------
// Totality: an unfenced directive leaves nothing of the value it was stamped onto
// ---------------------------------------------------------------------------------------------

/// Stamping an unfenced start directive produces exactly the request a controller predating
/// these fields sends — the same bytes, from an arbitrarily dirty starting value.
///
/// This is the byte-level half of the M11.T26c compatibility claim: a controller on the
/// pre-flag-day side of M11.D75's window stamps every request `Unfenced`, so what leaves that
/// process is byte-identical to what left it before the fields existed. It is what makes a
/// worker-first rollout possible, and `arroyo-worker`'s `lifecycle_fence::rollout_tests` drives
/// a real fence-capable worker against exactly these bytes.
#[test]
fn an_unfenced_start_directive_stamps_the_bytes_a_legacy_controller_sends() {
    let mut req = dirtied_start_request();
    StartDirective::Unfenced.stamp(&mut req);

    assert_eq!(
        req,
        StartExecutionReq {
            start_execution_id: "attempt".to_string(),
            ..Default::default()
        },
        "an unfenced directive must clear every lifecycle field, not merely fail to set one"
    );
    // The bytes, independently of the struct's own `PartialEq`: a field left set would encode
    // its key, and a legacy peer would decode a fenced request.
    assert_eq!(
        req.encode_to_vec(),
        StartExecutionReq {
            start_execution_id: "attempt".to_string(),
            ..Default::default()
        }
        .encode_to_vec()
    );
}

/// The same for a commit.
#[test]
fn an_unfenced_commit_directive_stamps_the_bytes_a_legacy_controller_sends() {
    let mut req = dirtied_commit_request();
    CommitDirective::Unfenced.stamp(&mut req);

    let legacy = CommitReq {
        epoch: 4,
        committing_data: Default::default(),
        lifecycle_fence: 0,
        target_worker_id: 0,
        target_worker_generation: 0,
    };
    assert_eq!(req, legacy);
    assert_eq!(req.encode_to_vec(), legacy.encode_to_vec());
}

/// Stamping is idempotent and forgets the directive before it.
///
/// A fenced request restamped as unfenced carries no fence — which is what makes "the directive
/// decides, not the literal" true for a request that is reused across attempts.
#[test]
fn restamping_replaces_the_previous_directive_rather_than_merging_with_it() {
    let mut req = StartExecutionReq::default();
    let revoked = vec!["gone".to_string()];
    StartDirective::Fenced {
        address: address(3, 1, 1),
        operation: LifecycleOperation::Revoke,
        revoked_execution_ids: &revoked,
    }
    .stamp(&mut req);
    StartDirective::Unfenced.stamp(&mut req);

    assert_eq!(start_directive(&req).unwrap(), StartDirective::Unfenced);
    assert_eq!(req, StartExecutionReq::default());
}

// ---------------------------------------------------------------------------------------------
// The pairing itself
// ---------------------------------------------------------------------------------------------

/// The send-side constructors are the read-side ones, and they agree on what addresses nothing.
///
/// `addressed` is the partial constructor a decode uses and `in_generation` the total one a
/// sender uses; the second is the only one that builds the value, so a generation either
/// addresses a target through both or through neither.
#[test]
fn the_two_target_constructors_are_one_constructor() {
    for generation in [1u64, 2, u64::MAX] {
        let nonzero = NonZeroU64::new(generation).unwrap();
        assert_eq!(
            LifecycleTarget::addressed(11, generation),
            Some(LifecycleTarget::in_generation(11, nonzero))
        );
    }
    assert_eq!(
        LifecycleTarget::addressed(11, 0),
        None,
        "generation zero is the sentinel and addresses nothing"
    );
}

/// A fence and a target are one value on the way out as well as on the way in.
///
/// There is no send-side constructor that takes a fence without a target, so the only way to
/// mutate the agreement between them is to build a *different* address — which produces a
/// different directive, and a receiver that compares it against its own identity sees that.
#[test]
fn a_mutated_agreement_between_fence_and_target_is_a_different_directive() {
    let mut req = StartExecutionReq::default();
    let original = address(7, 42, 3);
    StartDirective::Fenced {
        address: original,
        operation: LifecycleOperation::Start,
        revoked_execution_ids: &[],
    }
    .stamp(&mut req);

    for mutated in [address(8, 42, 3), address(7, 43, 3), address(7, 42, 4)] {
        let mut other = StartExecutionReq::default();
        StartDirective::Fenced {
            address: mutated,
            operation: LifecycleOperation::Start,
            revoked_execution_ids: &[],
        }
        .stamp(&mut other);
        assert_ne!(
            req, other,
            "changing any one of the three changes the request the worker decides on"
        );
        assert_ne!(original, mutated);
    }
}

// ---------------------------------------------------------------------------------------------
// The commit authority: one generation, many addresses
// ---------------------------------------------------------------------------------------------

/// Every worker of one generation is addressed under the same fence and its own id.
///
/// Each of the three dimensions is varied on its own against closed-form expected values, so a
/// directive built from a fence and a generation that came from different decisions would show
/// up as a different address rather than as a plausible one.
#[test]
fn a_commit_authority_addresses_every_worker_of_one_generation_under_one_fence() {
    for (fence, generation) in [(1u64, 1u64), (4, 2), (u64::MAX, 9)] {
        let authority = CommitAuthority::under(
            NonZeroU64::new(fence).unwrap(),
            NonZeroU64::new(generation).unwrap(),
        );
        for worker in [0u64, 7, u64::MAX] {
            let mut req = CommitReq::default();
            authority.directive(worker).stamp(&mut req);
            assert_eq!(req.lifecycle_fence, fence);
            assert_eq!(req.target_worker_id, worker);
            assert_eq!(req.target_worker_generation, generation);
            assert_eq!(
                commit_directive(&CommitReq::decode(&req.encode_to_vec()[..]).unwrap()).unwrap(),
                CommitDirective::Fenced(address(fence, worker, generation)),
                "and a stamped commit decodes back into the directive it was stamped with"
            );
        }
    }
}

/// The unfenced authority stamps the bytes a sender predating these fields produces.
///
/// The send half of the M11.T26e compatibility claim, at the level the four production commit
/// sites actually use: they hold a [`CommitAuthority`], not a [`CommitDirective`]. Measured on
/// the encoding and from a dirtied starting value, so a field left set would encode its key and
/// fail.
#[test]
fn an_unfenced_commit_authority_stamps_the_bytes_a_legacy_sender_sends() {
    let mut req = dirtied_commit_request();
    CommitAuthority::unfenced().directive(7).stamp(&mut req);

    let legacy = CommitReq {
        epoch: 4,
        committing_data: Default::default(),
        lifecycle_fence: 0,
        target_worker_id: 0,
        target_worker_generation: 0,
    };
    assert_eq!(req, legacy);
    assert_eq!(req.encode_to_vec(), legacy.encode_to_vec());
    assert_eq!(
        commit_directive(&req).unwrap(),
        CommitDirective::Unfenced,
        "and it reads back as the pre-flag-day directive rather than as a malformed one"
    );
}

/// A leader's authority is its own start's address with the worker id replaced, and nothing else.
///
/// This is the step M11.D39d's *"commit directives"* clause takes on the worker-leader path: the
/// leader was admitted by a start addressed to itself, and commits to its job's other workers,
/// which are the same generation under the same fence. Both surviving halves are asserted, and
/// so is the one that changes.
#[test]
fn a_leaders_commit_authority_keeps_its_starts_fence_and_generation() {
    for (fence, leader, generation, peer) in [(1u64, 0u64, 1u64, 5u64), (4, 7, 2, 8), (9, 3, 6, 3)]
    {
        let start = address(fence, leader, generation);
        let directive = start.commit_authority().directive(peer);
        assert_eq!(
            directive,
            CommitDirective::Fenced(address(fence, peer, generation))
        );
        // And the same authority still addresses the leader itself, which is what a job whose
        // leader also runs tasks needs.
        assert_eq!(
            start.commit_authority().directive(leader),
            CommitDirective::Fenced(start)
        );
    }
}

/// Mutating the agreement between a commit's fence and the generation it addresses produces a
/// different request, not a plausible one.
#[test]
fn a_mutated_commit_agreement_is_a_different_directive() {
    let stamp = |authority: CommitAuthority, worker: u64| {
        let mut req = CommitReq::default();
        authority.directive(worker).stamp(&mut req);
        req
    };
    let nz = |v: u64| NonZeroU64::new(v).unwrap();
    let original = stamp(CommitAuthority::under(nz(7), nz(3)), 42);

    for (label, mutated) in [
        (
            "a different fence",
            stamp(CommitAuthority::under(nz(8), nz(3)), 42),
        ),
        (
            "a different generation",
            stamp(CommitAuthority::under(nz(7), nz(4)), 42),
        ),
        (
            "a different worker",
            stamp(CommitAuthority::under(nz(7), nz(3)), 43),
        ),
        ("no fence at all", stamp(CommitAuthority::unfenced(), 42)),
    ] {
        assert_ne!(original, mutated, "{label}");
        assert_ne!(original.encode_to_vec(), mutated.encode_to_vec(), "{label}");
    }
}
