//! Invariant tests for the durable fencing record (M11.T26b).
//!
//! Split from [`super`] rather than nested in it because the rules being tested are what make
//! that module's types safe to persist, and a rule and its proof that share a file grow into
//! one file nobody re-reads.

use super::*;

fn target(worker_id: u64) -> FenceTarget {
    FenceTarget {
        worker_id,
        generation: 7,
        attempt_id: Some("a".repeat(MAX_ATTEMPT_ID_CHARS)),
        rpc_address: Some("http://worker:9191".to_string()),
        state: FenceTargetState::Pending,
    }
}

/// The instant a fixture obligation began, in milliseconds since the epoch.
///
/// Any non-zero value: zero is refused, because absence already has a spelling.
const SINCE: u64 = 1_700_000_000_000;

/// The capacity is a boundary, not an approximation: the last record that fits is
/// accepted, and the first that does not is refused with the count it carried.
#[test]
fn the_target_capacity_is_exact_at_its_boundary() {
    for count in [MAX_FENCE_TARGETS - 1, MAX_FENCE_TARGETS] {
        let record = Fencing::record((0..count as u64).map(target).collect(), None, Some(SINCE))
            .expect("a record at or below capacity must be written");
        assert_eq!(record.targets().len(), count);
        assert_eq!(record.version(), FENCING_RECORD_VERSION);
    }

    let over = MAX_FENCE_TARGETS + 1;
    assert_eq!(
        Fencing::record((0..over as u64).map(target).collect(), None, Some(SINCE)),
        Err(FencingRecordError::TooManyTargets { found: over }),
    );
}

/// The same boundary on the way in. A record that could not have been written must not be
/// adoptable by decoding it instead, which is the whole reason the check is the type's.
#[test]
fn an_over_capacity_record_is_refused_by_the_decoder() {
    let over = MAX_FENCE_TARGETS + 1;
    let value = serde_json::json!({
        "version": FENCING_RECORD_VERSION,
        "targets": (0..over as u64).map(target).collect::<Vec<_>>(),
    });

    let error = serde_json::from_value::<Fencing>(value).expect_err("must not decode");
    assert_eq!(
        error.to_string(),
        FencingRecordError::TooManyTargets { found: over }.to_string(),
    );
}

/// Each malformed shape is refused for its own stated reason, so an operator reading the
/// log is told which rule the record broke rather than that it "failed to decode".
#[test]
fn every_malformed_record_is_refused_with_its_own_reason() {
    let cases: [(serde_json::Value, String); 9] = [
        (
            serde_json::json!({ "version": FENCING_RECORD_VERSION + 1, "targets": [] }),
            FencingRecordError::UnknownVersion {
                found: FENCING_RECORD_VERSION + 1,
            }
            .to_string(),
        ),
        (
            serde_json::json!({ "version": 0, "targets": [] }),
            FencingRecordError::UnknownVersion { found: 0 }.to_string(),
        ),
        (
            serde_json::json!({
                "version": FENCING_RECORD_VERSION,
                "targets": [target(4), target(4)],
            }),
            FencingRecordError::DuplicateTarget { worker_id: 4 }.to_string(),
        ),
        (
            serde_json::json!({
                "version": FENCING_RECORD_VERSION,
                "targets": [{
                    "worker_id": 9,
                    "generation": 1,
                    "attempt_id": "a".repeat(MAX_ATTEMPT_ID_CHARS + 1),
                    "state": "pending",
                }],
            }),
            FencingRecordError::MalformedAttemptId {
                worker_id: 9,
                found: MAX_ATTEMPT_ID_CHARS + 1,
            }
            .to_string(),
        ),
        (
            serde_json::json!({
                "version": FENCING_RECORD_VERSION,
                "targets": [{
                    "worker_id": 9,
                    "generation": 1,
                    "attempt_id": "",
                    "state": "pending",
                }],
            }),
            FencingRecordError::MalformedAttemptId {
                worker_id: 9,
                found: 0,
            }
            .to_string(),
        ),
        (
            serde_json::json!({
                "version": FENCING_RECORD_VERSION,
                "targets": [],
                "candidate_root": "c".repeat(MAX_CANDIDATE_ROOT_BYTES + 1),
            }),
            FencingRecordError::MalformedCandidateRoot {
                found: MAX_CANDIDATE_ROOT_BYTES + 1,
            }
            .to_string(),
        ),
        (
            serde_json::json!({
                "version": FENCING_RECORD_VERSION,
                "targets": [{
                    "worker_id": 9,
                    "generation": 1,
                    "rpc_address": "h".repeat(MAX_TARGET_ADDRESS_CHARS + 1),
                    "state": "pending",
                }],
            }),
            FencingRecordError::MalformedTargetAddress {
                worker_id: 9,
                found: MAX_TARGET_ADDRESS_CHARS + 1,
            }
            .to_string(),
        ),
        (
            serde_json::json!({
                "version": FENCING_RECORD_VERSION,
                "targets": [{
                    "worker_id": 9,
                    "generation": 1,
                    "rpc_address": "",
                    "state": "pending",
                }],
            }),
            FencingRecordError::MalformedTargetAddress {
                worker_id: 9,
                found: 0,
            }
            .to_string(),
        ),
        (
            serde_json::json!({
                "version": FENCING_RECORD_VERSION,
                "targets": [],
                "fencing_since_millis": 0,
            }),
            FencingRecordError::MalformedFencingSince.to_string(),
        ),
    ];

    for (value, expected) in cases {
        let error = serde_json::from_value::<Fencing>(value.clone())
            .expect_err(&format!("must not decode: {value}"));
        assert_eq!(error.to_string(), expected, "{value}");
    }
}

/// Shapes serde itself refuses, listed so that the fail-closed answer is a property of the
/// record and not of the fields that happen to be required today.
#[test]
fn a_structurally_wrong_record_is_refused_before_any_rule_runs() {
    for value in [
        // No version at all: a record this build cannot place.
        serde_json::json!({ "targets": [] }),
        // A field nobody here knows, which is what a future build's record looks like.
        serde_json::json!({ "version": FENCING_RECORD_VERSION, "reason": "stopping" }),
        // A target state this build does not have.
        serde_json::json!({
            "version": FENCING_RECORD_VERSION,
            "targets": [{ "worker_id": 1, "generation": 1, "state": "revoked" }],
        }),
        // A target missing the generation that distinguishes a reused endpoint.
        serde_json::json!({
            "version": FENCING_RECORD_VERSION,
            "targets": [{ "worker_id": 1, "state": "pending" }],
        }),
        serde_json::json!("not an object"),
        serde_json::Value::Null,
    ] {
        assert!(
            serde_json::from_value::<Fencing>(value.clone()).is_err(),
            "must not decode: {value}"
        );
    }
}

/// A record round-trips to exactly the fields it was written with: no version drift, and
/// no `candidate_root: null` appearing in a record that never had one.
#[test]
fn a_record_round_trips_without_gaining_fields() {
    let record = Fencing::record(
        vec![FenceTarget {
            worker_id: 3,
            generation: 11,
            attempt_id: None,
            rpc_address: None,
            state: FenceTargetState::Acknowledged,
        }],
        None,
        None,
    )
    .expect("an ordinary record must be written");

    let encoded = serde_json::to_value(&record).expect("must serialize");
    assert_eq!(
        encoded,
        serde_json::json!({
            "version": 1,
            "targets": [{ "worker_id": 3, "generation": 11, "state": "acknowledged" }],
        }),
    );
    assert_eq!(
        serde_json::from_value::<Fencing>(encoded).expect("must decode"),
        record
    );

    let rooted = Fencing::record(
        vec![],
        Some("candidates/7/root.json".to_string()),
        Some(SINCE),
    )
    .expect("a rooted record must be written");
    assert_eq!(
        serde_json::to_value(&rooted).expect("must serialize"),
        serde_json::json!({
            "version": 1,
            "targets": [],
            "candidate_root": "candidates/7/root.json",
            "fencing_since_millis": SINCE,
        }),
    );
    assert_eq!(rooted.candidate_root(), Some("candidates/7/root.json"));
    assert_eq!(rooted.fencing_since_millis(), Some(SINCE));
}

/// Both bounds are exact at their boundaries, on the way in *and* on the way out.
///
/// The capacity has its own row above; this is the pair M11.T26f added. Each is checked at the
/// widest accepted value and at the first refused one, through both routes into existence — the
/// constructor and the decoder — because a bound that only ran on one of them is a bound a
/// persisted record can walk around.
#[test]
fn the_address_and_origin_bounds_are_exact_at_their_boundaries() {
    for (chars, accepted) in [
        (1, true),
        (MAX_TARGET_ADDRESS_CHARS, true),
        (MAX_TARGET_ADDRESS_CHARS + 1, false),
    ] {
        let target = FenceTarget {
            worker_id: 1,
            generation: 2,
            attempt_id: None,
            rpc_address: Some("h".repeat(chars)),
            state: FenceTargetState::Pending,
        };
        let written = Fencing::record(vec![target.clone()], None, None);
        assert_eq!(
            written.is_ok(),
            accepted,
            "a {chars}-character address must {} be writable",
            if accepted { "" } else { "not" }
        );
        let decoded = serde_json::from_value::<Fencing>(serde_json::json!({
            "version": FENCING_RECORD_VERSION,
            "targets": [target],
        }));
        assert_eq!(
            decoded.is_ok(),
            accepted,
            "and a {chars}-character address must decode exactly as it writes"
        );
    }

    for (since, accepted) in [(None, true), (Some(0), false), (Some(1), true)] {
        assert_eq!(
            Fencing::record(vec![], Some("c".to_string()), since).is_ok(),
            accepted,
            "a fencing origin of {since:?} must {} be writable",
            if accepted { "" } else { "not" }
        );
        let mut value = serde_json::json!({
            "version": FENCING_RECORD_VERSION,
            "targets": [],
        });
        if let Some(since) = since {
            value["fencing_since_millis"] = serde_json::json!(since);
        }
        assert_eq!(
            serde_json::from_value::<Fencing>(value).is_ok(),
            accepted,
            "and a fencing origin of {since:?} must decode exactly as it writes"
        );
    }
}

/// The address bound is derived from three standards and nothing else.
///
/// A compile-time assertion rather than a runtime one, because the point is that the constant is
/// a *sum* rather than a round number somebody chose: a change to any of its three parts changes
/// this, and a change to the total that is not a change to a part does not compile.
#[test]
fn the_address_bound_is_the_sum_of_the_three_parts_of_a_uri() {
    const _: () = assert!(MAX_TARGET_ADDRESS_CHARS == 8 + 253 + 6);
    assert_eq!(
        MAX_TARGET_ADDRESS_CHARS, 267,
        "`https://` (8) plus an RFC 1035 presentation-form domain name (253) plus `:65535` (6)"
    );
}

/// A record written without the two facts M11.T26f added decodes and re-serializes without
/// acquiring them.
///
/// The deployability claim for extending a record M11.T26b already shipped the *shape* of: both
/// new fields default and are skipped when absent, so a `state_context` a build without them
/// wrote survives a round trip through this one byte for byte.
#[test]
fn a_record_without_the_address_or_the_origin_round_trips_without_gaining_them() {
    let written = serde_json::json!({
        "version": 1,
        "targets": [{ "worker_id": 3, "generation": 11, "state": "pending" }],
    });
    let decoded = serde_json::from_value::<Fencing>(written.clone()).expect("must decode");
    assert_eq!(decoded.targets()[0].rpc_address, None);
    assert_eq!(decoded.fencing_since_millis(), None);
    assert_eq!(
        serde_json::to_value(&decoded).expect("must serialize"),
        written,
        "a record that carried neither must not gain either on the way back out"
    );
}
