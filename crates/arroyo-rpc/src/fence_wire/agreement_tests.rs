//! What the fence-protocol fields must say about each other, and the values this build refuses.
//!
//! A fence, a target generation and an operation are not three independent numbers, but on the
//! wire they are exactly that. These are the cases where the wire allows a combination the
//! protocol does not: each asserts the combination survives the round trip unchanged — so a
//! receiver can see it — and that reading it as one directive is refused rather than guessed at.

use super::golden_tests::round_trip;
use super::*;

// ---------------------------------------------------------------------------------------------
// The agreement between fields
// ---------------------------------------------------------------------------------------------

/// A fence with no target names no generation to advance, and is refused.
#[test]
fn a_fence_without_a_target_is_refused() {
    let request = StartExecutionReq {
        lifecycle_fence: 7,
        target_worker_id: 42,
        ..Default::default()
    };
    assert_eq!(
        start_directive(&round_trip(&request)).unwrap_err(),
        MalformedFenceFields::FenceWithoutTarget {
            fence: 7,
            target_worker_id: 42
        }
    );

    let commit = CommitReq {
        lifecycle_fence: 7,
        ..Default::default()
    };
    assert_eq!(
        commit_directive(&round_trip(&commit)).unwrap_err(),
        MalformedFenceFields::FenceWithoutTarget {
            fence: 7,
            target_worker_id: 0
        }
    );
}

/// A target with no fence is addressed on no authority, and is refused — including the case
/// where only the worker id is set, which addresses no generation either.
#[test]
fn a_target_without_a_fence_is_refused() {
    let addressed = StartExecutionReq {
        target_worker_id: 42,
        target_worker_generation: 3,
        ..Default::default()
    };
    assert_eq!(
        start_directive(&round_trip(&addressed)).unwrap_err(),
        MalformedFenceFields::TargetWithoutFence {
            worker_id: 42,
            generation: 3
        }
    );

    let id_only = StartExecutionReq {
        target_worker_id: 42,
        ..Default::default()
    };
    assert_eq!(
        start_directive(&round_trip(&id_only)).unwrap_err(),
        MalformedFenceFields::TargetWithoutFence {
            worker_id: 42,
            generation: 0
        }
    );

    let commit = CommitReq {
        target_worker_generation: 3,
        ..Default::default()
    };
    assert_eq!(
        commit_directive(&round_trip(&commit)).unwrap_err(),
        MalformedFenceFields::TargetWithoutFence {
            worker_id: 0,
            generation: 3
        }
    );
}

/// A fence-only or revoke operation with no fence asks to advance to nothing, and is refused.
#[test]
fn an_operation_without_a_fence_is_refused() {
    for operation in [LifecycleOperation::FenceOnly, LifecycleOperation::Revoke] {
        let request = StartExecutionReq {
            lifecycle_operation: operation as i32,
            ..Default::default()
        };
        assert_eq!(
            start_directive(&round_trip(&request)).unwrap_err(),
            MalformedFenceFields::OperationWithoutFence { operation }
        );
    }
}

/// Identifiers named for revocation by a message carrying no fence are refused: revocation is
/// what a fence advancement does to what it supersedes, and there is nothing here to supersede.
#[test]
fn a_revocation_without_a_fence_is_refused() {
    let request = StartExecutionReq {
        revoked_execution_ids: vec!["ab".to_string(), "cd".to_string()],
        ..Default::default()
    };
    assert_eq!(
        start_directive(&round_trip(&request)).unwrap_err(),
        MalformedFenceFields::RevocationWithoutFence { count: 2 }
    );
}

/// A fenced directive addressed to a generation that is not the reader's still classifies as a
/// well-formed directive, carrying exactly the identity the sender named.
///
/// Nothing inside the message can say otherwise: "is this addressed to me" is a comparison
/// against the reader's own `WorkerContext`, which the message does not contain. The golden here
/// is that the wire neither hides the mismatch nor normalizes it away, so the comparison the
/// worker's admission guard makes is a comparison of two values that both arrived intact.
#[test]
fn a_directive_addressed_to_another_generation_arrives_intact() {
    let request = StartExecutionReq {
        lifecycle_fence: 7,
        target_worker_id: 42,
        target_worker_generation: 3,
        target_worker_incarnation: 11,
        ..Default::default()
    };
    let decoded = round_trip(&request);
    let StartDirective::Fenced { address, .. } = start_directive(&decoded).unwrap() else {
        panic!("a fenced request must not classify as unfenced");
    };

    // A reader whose own identity is worker 42 generation 4 — the same endpoint after a restart.
    assert_eq!(address.target().worker_id(), 42);
    assert_ne!(address.target().generation(), 4);
    assert_eq!(
        LifecycleTarget::addressed(42, 3, 11),
        Some(address.target()),
        "the target that arrived is the one the sender named, byte for byte"
    );
}

/// A directive addressed to a *predecessor process* of the same worker generation arrives with
/// the incarnation the sender named, so the receiver compares two intact values.
///
/// The wire half of PR #167 round 6, finding 3: the id and the generation agree — a restart
/// reuses both — and only the incarnation says the request was minted for a process that is
/// gone. Normalizing it away here would leave the guard nothing to refuse on.
#[test]
fn a_directive_addressed_to_a_predecessor_incarnation_arrives_intact() {
    let request = StartExecutionReq {
        lifecycle_fence: 7,
        target_worker_id: 42,
        target_worker_generation: 3,
        target_worker_incarnation: 11,
        ..Default::default()
    };
    let decoded = round_trip(&request);
    let StartDirective::Fenced { address, .. } = start_directive(&decoded).unwrap() else {
        panic!("a fenced request must not classify as unfenced");
    };

    let successor = LifecycleTarget::addressed(42, 3, 12).expect("generation 3 is addressable");
    assert_eq!(address.target().worker_id(), successor.worker_id());
    assert_eq!(address.target().generation(), successor.generation());
    assert_ne!(
        address.target(),
        successor,
        "the same worker and generation at a different incarnation is a different target"
    );
    assert_eq!(
        address.target().incarnation().map(WorkerIncarnation::get),
        Some(11)
    );
}

/// An incarnation carried without a fence describes nothing this build can act on, and is
/// refused rather than read as an address.
#[test]
fn an_incarnation_without_a_fence_is_refused() {
    let request = StartExecutionReq {
        target_worker_incarnation: 11,
        ..Default::default()
    };
    assert_eq!(
        start_directive(&round_trip(&request)).unwrap_err(),
        MalformedFenceFields::IncarnationWithoutFence {
            worker_id: 0,
            incarnation: 11,
        }
    );

    let commit = CommitReq {
        epoch: 4,
        target_worker_incarnation: 11,
        ..Default::default()
    };
    assert_eq!(
        commit_directive(&round_trip(&commit)).unwrap_err(),
        MalformedFenceFields::IncarnationWithoutFence {
            worker_id: 0,
            incarnation: 11,
        }
    );
}

/// Generation zero addresses nothing, whatever the worker id says; worker id zero is a worker.
#[test]
fn only_the_generation_decides_whether_a_target_is_addressed() {
    assert_eq!(LifecycleTarget::addressed(42, 0, 11), None);
    assert_eq!(LifecycleTarget::addressed(0, 0, 0), None);

    let addressed =
        LifecycleTarget::addressed(0, 3, 0).expect("generation 3 addresses a generation");
    assert_eq!(addressed.worker_id(), 0);
    assert_eq!(addressed.generation(), 3);
    assert_eq!(
        addressed.incarnation(),
        None,
        "incarnation zero names no process, which is a shape an address may have"
    );
}

// ---------------------------------------------------------------------------------------------
// Values this build cannot name
// ---------------------------------------------------------------------------------------------

/// An operation from a newer controller survives the wire as the integer it is, and is refused
/// rather than read as the zero value.
///
/// proto3 keeps an unrecognized enum value verbatim, so this is exactly what a request from a
/// build that added a fourth operation looks like here. Reading it as `START` would turn
/// "advance the fence" into "start the program".
#[test]
fn an_unknown_operation_is_refused_rather_than_read_as_a_start() {
    for operation in [3, 99, i32::MAX] {
        let request = StartExecutionReq {
            lifecycle_fence: 7,
            target_worker_id: 42,
            target_worker_generation: 3,
            lifecycle_operation: operation,
            ..Default::default()
        };
        let decoded = round_trip(&request);
        assert_eq!(
            decoded.lifecycle_operation, operation,
            "the wire must not silently normalize an unknown operation"
        );
        assert_eq!(
            start_directive(&decoded).unwrap_err(),
            MalformedFenceFields::UnknownOperation { operation }
        );
    }
}

/// An outcome from a newer worker is refused rather than read as `APPLIED`, which would settle
/// an attempt that was not applied.
#[test]
fn an_unknown_outcome_is_refused_rather_than_read_as_applied() {
    for outcome in [3, 99, i32::MAX] {
        let response = StartExecutionResp {
            observed_lifecycle_fence: 9,
            outcome,
        };
        let decoded = round_trip(&response);
        assert_eq!(decoded.outcome, outcome);
        assert_eq!(
            observed_settlement(&decoded).unwrap_err(),
            MalformedFenceFields::UnknownOutcome { outcome }
        );
    }
}

/// A response that claims to have acknowledged a fence while reporting none observed is refused:
/// the acknowledgement it describes has no fence value for a controller to record.
#[test]
fn an_acknowledgement_reporting_no_observed_fence_is_refused() {
    for outcome in [
        StartExecutionOutcome::FenceAcknowledged,
        StartExecutionOutcome::Revoked,
    ] {
        let response = StartExecutionResp {
            observed_lifecycle_fence: 0,
            outcome: outcome as i32,
        };
        assert_eq!(
            observed_settlement(&round_trip(&response)).unwrap_err(),
            MalformedFenceFields::AcknowledgementWithoutObservedFence { outcome }
        );
    }

    // An applied attempt reported by a generation that has acknowledged a fence is fine, and so
    // is one reported by a generation that has not.
    for (fence, expected) in [(0, None), (9, Some(9))] {
        let settlement = observed_settlement(&StartExecutionResp {
            observed_lifecycle_fence: fence,
            outcome: StartExecutionOutcome::Applied as i32,
        })
        .unwrap();
        assert_eq!(settlement.observed_fence(), expected);
        assert_eq!(settlement.outcome(), StartExecutionOutcome::Applied);
    }
}

/// The revocation list is bounded by what the durable ledger can hold, at N-1, N and N+1.
#[test]
fn a_revocation_list_is_bounded_by_the_ledgers_capacity() {
    let fenced = |count: usize| StartExecutionReq {
        lifecycle_fence: 7,
        target_worker_id: 42,
        target_worker_generation: 3,
        revoked_execution_ids: vec!["ab".to_string(); count],
        ..Default::default()
    };

    for count in [MAX_FENCE_TARGETS - 1, MAX_FENCE_TARGETS] {
        let request = fenced(count);
        let StartDirective::Fenced {
            revoked_execution_ids,
            ..
        } = start_directive(&request).unwrap()
        else {
            panic!("a fenced request must not classify as unfenced");
        };
        assert_eq!(revoked_execution_ids.len(), count);
    }

    let over = fenced(MAX_FENCE_TARGETS + 1);
    assert_eq!(
        start_directive(&over).unwrap_err(),
        MalformedFenceFields::TooManyRevokedIds {
            found: MAX_FENCE_TARGETS + 1
        }
    );
}

/// A revoked identifier that is empty or wider than one the controller mints is refused, and the
/// refusal names which one.
#[test]
fn a_revoked_identifier_outside_the_minted_width_is_refused() {
    let fenced = |ids: Vec<String>| StartExecutionReq {
        lifecycle_fence: 7,
        target_worker_id: 42,
        target_worker_generation: 3,
        revoked_execution_ids: ids,
        ..Default::default()
    };

    let with_empty = fenced(vec!["ab".to_string(), String::new()]);
    assert_eq!(
        start_directive(&with_empty).unwrap_err(),
        MalformedFenceFields::MalformedRevokedId { index: 1, found: 0 }
    );

    let with_too_wide = fenced(vec!["a".repeat(MAX_ATTEMPT_ID_CHARS + 1)]);
    assert_eq!(
        start_directive(&with_too_wide).unwrap_err(),
        MalformedFenceFields::MalformedRevokedId {
            index: 0,
            found: MAX_ATTEMPT_ID_CHARS + 1
        }
    );

    let exactly_wide = "a".repeat(MAX_ATTEMPT_ID_CHARS);
    let with_exactly_wide = fenced(vec![exactly_wide.clone()]);
    let StartDirective::Fenced {
        revoked_execution_ids,
        ..
    } = start_directive(&with_exactly_wide).unwrap()
    else {
        panic!("a fenced request must not classify as unfenced");
    };
    assert_eq!(revoked_execution_ids, [exactly_wide]);
}
