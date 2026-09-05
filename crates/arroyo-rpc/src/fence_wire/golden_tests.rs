//! Encode/decode goldens for the fence-protocol fields.
//!
//! The goldens assert bytes, not round-trip success. A message whose new fields are all at their
//! defaults must encode to exactly the bytes the build that predated them produced, and a
//! message that sets one must encode to exactly those bytes plus that field's key and value —
//! which is what pins the field numbers at the layer that actually carries them.

use super::*;
use crate::grpc::rpc::{RegisterWorkerReq, RegisterWorkerResp, TaskAssignment};
use prost::Message;

/// A request shaped exactly as a controller predating M11.T26c would send it: it carries an
/// attempt identifier and nothing else this module knows about.
fn legacy_start_request() -> StartExecutionReq {
    StartExecutionReq {
        start_execution_id: "a".to_string(),
        ..Default::default()
    }
}

/// The complete set of lifecycle fields, one fenced revoke directive.
fn fenced_start_request() -> StartExecutionReq {
    StartExecutionReq {
        lifecycle_fence: 7,
        target_worker_id: 42,
        target_worker_generation: 3,
        target_worker_incarnation: 5,
        lifecycle_operation: LifecycleOperation::Revoke as i32,
        revoked_execution_ids: vec!["ab".to_string()],
        ..Default::default()
    }
}

pub(super) fn round_trip<T: Message + Default>(message: &T) -> T {
    T::decode(&message.encode_to_vec()[..]).expect("a message this build encoded")
}

// ---------------------------------------------------------------------------------------------
// Legacy decoding: absent fields, and the bytes that prove they are absent
// ---------------------------------------------------------------------------------------------

/// A `RegisterWorkerResp` from a controller that predates the flag day carries no bytes at all,
/// and decodes to "this registration does not require the fence protocol".
#[test]
fn a_registration_response_predating_strict_mode_does_not_require_it() {
    let from_legacy_controller = RegisterWorkerResp::decode(&[][..]).unwrap();
    assert!(!from_legacy_controller.requires_lifecycle_fence);

    // And the converse: a response that does not require it is still those same zero bytes, so
    // adding the field changed nothing a legacy worker will read.
    assert_eq!(
        RegisterWorkerResp {
            requires_lifecycle_fence: false,
        }
        .encode_to_vec(),
        Vec::<u8>::new()
    );
    assert_eq!(
        RegisterWorkerResp {
            requires_lifecycle_fence: true,
        }
        .encode_to_vec(),
        vec![0x08, 0x01],
        "field 1, varint, true"
    );
}

/// A `StartExecutionReq` from a controller that predates the fence protocol leaves every
/// lifecycle field at its proto3 default, so none of them reaches the wire, and it classifies as
/// an unfenced directive.
///
/// This is the sibling of `arroyo_worker`'s `absent_state_backend_starts_the_worker_on_parquet`:
/// the same question asked of the fields M11.T26c added.
#[test]
fn a_start_request_predating_the_fence_protocol_is_unfenced() {
    let old_request = legacy_start_request();
    assert_eq!(old_request.lifecycle_fence, 0);
    assert_eq!(old_request.target_worker_id, 0);
    assert_eq!(old_request.target_worker_generation, 0);
    assert_eq!(old_request.target_worker_incarnation, 0);
    assert_eq!(old_request.lifecycle_operation, 0);
    assert!(old_request.revoked_execution_ids.is_empty());

    // Exactly the bytes the build that predated these fields produced for this request: field
    // 12, length-delimited, one byte of payload. Nothing else is on the wire.
    assert_eq!(
        old_request.encode_to_vec(),
        vec![0x62, 0x01, b'a'],
        "no new field may put a byte on the wire when it is at its default"
    );

    assert_eq!(
        start_directive(&round_trip(&old_request)).unwrap(),
        StartDirective::Unfenced
    );
}

/// A `StartExecutionResp` from a worker that predates the fence protocol is zero bytes, and
/// settles the attempt as applied with no fence acknowledged — which is what an `Ok` response
/// from such a worker has always meant.
#[test]
fn a_start_response_predating_the_fence_protocol_settles_an_applied_attempt() {
    let from_legacy_worker = StartExecutionResp::decode(&[][..]).unwrap();
    assert_eq!(from_legacy_worker.observed_lifecycle_fence, 0);
    assert_eq!(from_legacy_worker.outcome, 0);
    assert_eq!(
        StartExecutionResp::default().encode_to_vec(),
        Vec::<u8>::new()
    );

    let settlement = observed_settlement(&from_legacy_worker).unwrap();
    assert_eq!(settlement.observed_fence(), None);
    assert_eq!(settlement.outcome(), StartExecutionOutcome::Applied);
}

/// A `CommitReq` from a controller that predates the fence protocol carries only the two fields
/// it always carried, and classifies as unfenced.
#[test]
fn a_commit_request_predating_the_fence_protocol_is_unfenced() {
    let old_request = CommitReq {
        epoch: 42,
        ..Default::default()
    };
    assert_eq!(
        old_request.encode_to_vec(),
        vec![0x08, 0x2A],
        "field 1, varint, 42 — and nothing else"
    );

    let decoded = round_trip(&old_request);
    assert_eq!(decoded.epoch, 42);
    assert_eq!(decoded.lifecycle_fence, 0);
    assert_eq!(decoded.target_worker_id, 0);
    assert_eq!(decoded.target_worker_generation, 0);
    assert_eq!(decoded.target_worker_incarnation, 0);
    assert_eq!(
        commit_directive(&decoded).unwrap(),
        CommitDirective::Unfenced
    );
}

// ---------------------------------------------------------------------------------------------
// Every new field, on the wire
// ---------------------------------------------------------------------------------------------

/// Every lifecycle field of `StartExecutionReq` reaches the wire at the number it was allocated,
/// in tag order, and comes back carrying what was sent.
#[test]
fn every_start_request_fence_field_survives_the_wire() {
    let request = fenced_start_request();
    assert_eq!(
        request.encode_to_vec(),
        vec![
            0x68, 0x07, // 13 varint = 7          (lifecycle_fence)
            0x70, 0x2A, // 14 varint = 42         (target_worker_id)
            0x78, 0x03, // 15 varint = 3          (target_worker_generation)
            0x80, 0x01, 0x02, // 16 varint = 2    (lifecycle_operation = REVOKE)
            0x8A, 0x01, 0x02, b'a', b'b', // 17 len 2 = "ab" (revoked_execution_ids)
            0x90, 0x01, 0x05, // 18 varint = 5    (target_worker_incarnation)
        ]
    );

    let decoded = round_trip(&request);
    assert_eq!(decoded.lifecycle_fence, 7);
    assert_eq!(decoded.target_worker_id, 42);
    assert_eq!(decoded.target_worker_generation, 3);
    assert_eq!(decoded.target_worker_incarnation, 5);
    assert_eq!(
        decoded.lifecycle_operation,
        LifecycleOperation::Revoke as i32
    );
    assert_eq!(decoded.revoked_execution_ids, vec!["ab".to_string()]);

    let StartDirective::Fenced {
        address,
        operation,
        revoked_execution_ids,
    } = start_directive(&decoded).unwrap()
    else {
        panic!("a fenced request must not classify as unfenced");
    };
    assert_eq!(address.fence(), 7);
    assert_eq!(address.target().worker_id(), 42);
    assert_eq!(address.target().generation(), 3);
    assert_eq!(
        address.target().incarnation().map(WorkerIncarnation::get),
        Some(5)
    );
    assert_eq!(operation, LifecycleOperation::Revoke);
    assert_eq!(revoked_execution_ids, ["ab".to_string()]);
}

/// Both settlement fields of `StartExecutionResp` reach the wire and come back.
#[test]
fn every_start_response_settlement_field_survives_the_wire() {
    let response = StartExecutionResp {
        observed_lifecycle_fence: 9,
        outcome: StartExecutionOutcome::FenceAcknowledged as i32,
    };
    assert_eq!(
        response.encode_to_vec(),
        vec![
            0x08, 0x09, // 1 varint = 9  (observed_lifecycle_fence)
            0x10, 0x01, // 2 varint = 1  (outcome = FENCE_ACKNOWLEDGED)
        ]
    );

    let settlement = observed_settlement(&round_trip(&response)).unwrap();
    assert_eq!(settlement.observed_fence(), Some(9));
    assert_eq!(
        settlement.outcome(),
        StartExecutionOutcome::FenceAcknowledged
    );
}

/// All four fence fields of `CommitReq` reach the wire and come back.
#[test]
fn every_commit_request_fence_field_survives_the_wire() {
    let request = CommitReq {
        epoch: 42,
        lifecycle_fence: 5,
        target_worker_id: 42,
        target_worker_generation: 3,
        target_worker_incarnation: 11,
        ..Default::default()
    };
    assert_eq!(
        request.encode_to_vec(),
        vec![
            0x08, 0x2A, // 1 varint = 42 (epoch)
            0x18, 0x05, // 3 varint = 5  (lifecycle_fence)
            0x20, 0x2A, // 4 varint = 42 (target_worker_id)
            0x28, 0x03, // 5 varint = 3  (target_worker_generation)
            0x30, 0x0B, // 6 varint = 11 (target_worker_incarnation)
        ]
    );

    let CommitDirective::Fenced(address) = commit_directive(&round_trip(&request)).unwrap() else {
        panic!("a fenced commit must not classify as unfenced");
    };
    assert_eq!(address.fence(), 5);
    assert_eq!(address.target().worker_id(), 42);
    assert_eq!(address.target().generation(), 3);
    assert_eq!(
        address.target().incarnation().map(WorkerIncarnation::get),
        Some(11)
    );
}

/// A worker predating `RegisterWorkerReq::worker_incarnation` reports none, and the field puts
/// no byte on the wire when it does; a worker that mints one encodes it at number 10.
///
/// The registration half of PR #167 round 6, finding 3: the incarnation is reported here and
/// nowhere else, so this is the byte that decides whether a controller can address a generation
/// to a particular process at all.
#[test]
fn the_registration_incarnation_is_absent_for_a_worker_that_mints_none() {
    let from_legacy_worker = RegisterWorkerReq::decode(&[][..]).unwrap();
    assert_eq!(from_legacy_worker.worker_incarnation, 0);
    assert_eq!(WorkerIncarnation::named(0), None);

    assert_eq!(
        RegisterWorkerReq {
            worker_incarnation: 0,
            ..Default::default()
        }
        .encode_to_vec(),
        Vec::<u8>::new(),
        "no new field may put a byte on the wire when it is at its default"
    );
    assert_eq!(
        RegisterWorkerReq {
            worker_incarnation: 5,
            ..Default::default()
        }
        .encode_to_vec(),
        vec![0x50, 0x05],
        "field 10, varint, 5"
    );
}

/// A controller predating `TaskAssignment::worker_incarnation` names none, and the field puts no
/// byte on the wire when it does.
///
/// This is how a worker-leader execution learns which process each of its peers is; a leader
/// that reads zero addresses its commits to no incarnation, and a generation that has one
/// refuses them.
#[test]
fn the_assignment_incarnation_is_absent_for_a_controller_that_names_none() {
    let from_legacy_controller = TaskAssignment::decode(&[][..]).unwrap();
    assert_eq!(from_legacy_controller.worker_incarnation, 0);

    assert_eq!(
        TaskAssignment {
            worker_incarnation: 0,
            ..Default::default()
        }
        .encode_to_vec(),
        Vec::<u8>::new()
    );
    assert_eq!(
        TaskAssignment {
            worker_incarnation: 5,
            ..Default::default()
        }
        .encode_to_vec(),
        vec![0x38, 0x05],
        "field 7, varint, 5"
    );
}

/// Each new field of `StartExecutionReq` is carried independently of the others: setting one and
/// only one leaves every other at its default, on the wire and after the round trip.
///
/// The directive that results is refused in five of the six cases, and that is the point — a
/// single lifecycle field is never a directive.
#[test]
fn each_start_request_fence_field_varies_independently() {
    let cases: [(&str, StartExecutionReq, Vec<u8>); 6] = [
        (
            "lifecycle_fence",
            StartExecutionReq {
                lifecycle_fence: 7,
                ..Default::default()
            },
            vec![0x68, 0x07],
        ),
        (
            "target_worker_id",
            StartExecutionReq {
                target_worker_id: 42,
                ..Default::default()
            },
            vec![0x70, 0x2A],
        ),
        (
            "target_worker_generation",
            StartExecutionReq {
                target_worker_generation: 3,
                ..Default::default()
            },
            vec![0x78, 0x03],
        ),
        (
            "lifecycle_operation",
            StartExecutionReq {
                lifecycle_operation: LifecycleOperation::FenceOnly as i32,
                ..Default::default()
            },
            vec![0x80, 0x01, 0x01],
        ),
        (
            "revoked_execution_ids",
            StartExecutionReq {
                revoked_execution_ids: vec!["ab".to_string()],
                ..Default::default()
            },
            vec![0x8A, 0x01, 0x02, b'a', b'b'],
        ),
        (
            "target_worker_incarnation",
            StartExecutionReq {
                target_worker_incarnation: 5,
                ..Default::default()
            },
            vec![0x90, 0x01, 0x05],
        ),
    ];

    for (field, request, expected) in cases {
        assert_eq!(
            request.encode_to_vec(),
            expected,
            "{field} alone must encode as exactly its own key and value"
        );
        assert_eq!(
            round_trip(&request),
            request,
            "{field} alone must survive the wire unchanged"
        );
    }
}

/// The zero value of each new enum is the meaning the message had before the field existed.
#[test]
fn each_enums_zero_value_is_its_legacy_meaning() {
    assert_eq!(
        LifecycleOperation::try_from(0).unwrap(),
        LifecycleOperation::Start,
        "a request predating the field asks for a start, which is all StartExecution ever meant"
    );
    assert_eq!(
        StartExecutionOutcome::try_from(0).unwrap(),
        StartExecutionOutcome::Applied,
        "an Ok response predating the field means the attempt is applied"
    );
}
