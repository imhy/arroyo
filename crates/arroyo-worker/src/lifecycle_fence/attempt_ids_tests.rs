//! The bounded identifier record on its own: the derived capacity at N-1/N/N+1, the width bound,
//! and the two relationships it refuses.
//!
//! [`super::guard_tests`] drives the same cap through `WorkerGrpc::start_execution`; these reach
//! the record directly so the boundary can be walked exactly and so the refusals a well-formed
//! directive cannot provoke are still exercised.

use super::attempt_ids::{
    AttemptDisposition, AttemptIdRefusal, AttemptIds, MAX_TRACKED_ATTEMPT_IDS,
};
use arroyo_rpc::fencing::{MAX_ATTEMPT_ID_CHARS, MAX_FENCE_TARGETS};

fn ids(count: usize) -> Vec<String> {
    (0..count)
        .map(|i| format!("{i:0width$x}", width = MAX_ATTEMPT_ID_CHARS))
        .collect()
}

/// The capacity is exactly one whole well-formed directive plus this generation's own applied
/// identifier, and it is the durable record's bound rather than a number chosen here.
#[test]
fn the_capacity_is_the_controllers_issued_attempt_bound_plus_one() {
    assert_eq!(MAX_TRACKED_ATTEMPT_IDS, MAX_FENCE_TARGETS + 1);
    assert_eq!(MAX_FENCE_TARGETS, 32 * 2 * 32);
}

/// N-1, N and N+1 against the derived capacity.
#[test]
fn the_record_holds_its_capacity_and_refuses_one_more() {
    let all = ids(MAX_TRACKED_ATTEMPT_IDS + 1);

    let mut under = AttemptIds::default();
    under
        .record(&all[..MAX_TRACKED_ATTEMPT_IDS - 1], None)
        .unwrap();
    assert_eq!(under.len(), MAX_TRACKED_ATTEMPT_IDS - 1);

    let mut exact = AttemptIds::default();
    exact.record(&all[..MAX_TRACKED_ATTEMPT_IDS], None).unwrap();
    assert_eq!(exact.len(), MAX_TRACKED_ATTEMPT_IDS);

    // One past the capacity in a single call refuses and stores none of it.
    let mut over = AttemptIds::default();
    assert_eq!(
        over.record(&all, None).unwrap_err(),
        AttemptIdRefusal::Overflow {
            held: 0,
            added: MAX_TRACKED_ATTEMPT_IDS + 1,
        }
    );
    assert_eq!(over.len(), 0);
    assert_eq!(over.disposition(&all[0]), AttemptDisposition::Unknown);

    // One past the capacity onto a full record refuses and evicts none of it.
    assert_eq!(
        exact
            .record(&all[MAX_TRACKED_ATTEMPT_IDS..], None)
            .unwrap_err(),
        AttemptIdRefusal::Overflow {
            held: MAX_TRACKED_ATTEMPT_IDS,
            added: 1,
        }
    );
    assert_eq!(exact.len(), MAX_TRACKED_ATTEMPT_IDS);
    for id in &all[..MAX_TRACKED_ATTEMPT_IDS] {
        assert_eq!(exact.disposition(id), AttemptDisposition::Revoked);
    }

    // The last free slot takes an applied identifier as readily as a revoked one, and then the
    // record is full for both kinds.
    let mut mixed = AttemptIds::default();
    mixed
        .record(&all[..MAX_TRACKED_ATTEMPT_IDS - 1], None)
        .unwrap();
    mixed
        .record(&[], Some(&all[MAX_TRACKED_ATTEMPT_IDS]))
        .unwrap();
    assert_eq!(mixed.len(), MAX_TRACKED_ATTEMPT_IDS);
    assert_eq!(
        mixed.disposition(&all[MAX_TRACKED_ATTEMPT_IDS]),
        AttemptDisposition::Applied
    );
    assert_eq!(
        mixed
            .record(
                &all[MAX_TRACKED_ATTEMPT_IDS - 1..MAX_TRACKED_ATTEMPT_IDS],
                None
            )
            .unwrap_err(),
        AttemptIdRefusal::Overflow {
            held: MAX_TRACKED_ATTEMPT_IDS,
            added: 1,
        }
    );
    assert_eq!(mixed.len(), MAX_TRACKED_ATTEMPT_IDS);
    assert_eq!(mixed.applied(), Some(all[MAX_TRACKED_ATTEMPT_IDS].as_str()));
}

/// Identifiers already held cost nothing, so a re-delivered directive never overflows a record
/// that answered it once.
#[test]
fn identifiers_already_held_consume_no_capacity() {
    let all = ids(MAX_TRACKED_ATTEMPT_IDS);
    let mut record = AttemptIds::default();
    record.record(&all, None).unwrap();
    assert_eq!(record.len(), MAX_TRACKED_ATTEMPT_IDS);

    for _ in 0..3 {
        record.record(&all, None).unwrap();
        assert_eq!(record.len(), MAX_TRACKED_ATTEMPT_IDS);
    }

    // Duplicates inside one call are counted once, so a list naming the same identifier twice
    // fits wherever the identifier itself fits.
    let mut duplicating = AttemptIds::default();
    let repeated: Vec<String> =
        std::iter::repeat_n(all[0].clone(), MAX_TRACKED_ATTEMPT_IDS + 4).collect();
    duplicating.record(&repeated, None).unwrap();
    assert_eq!(duplicating.len(), 1);
}

/// The record bounds the width of what it stores, on both the revoked and the applied side.
///
/// `arroyo_rpc::fence_wire` already refuses an over-wide revocation list, so the revoked side is
/// unreachable through a decoded directive; the applied side is not bounded anywhere else, and
/// an unbounded `start_execution_id` would make "bounded per-generation state" false in bytes
/// while remaining true in count.
#[test]
fn the_record_bounds_the_width_of_what_it_stores() {
    let too_wide = "x".repeat(MAX_ATTEMPT_ID_CHARS + 1);

    let mut record = AttemptIds::default();
    assert_eq!(
        record.record(&[], Some(&too_wide)).unwrap_err(),
        AttemptIdRefusal::MalformedId {
            found: MAX_ATTEMPT_ID_CHARS + 1,
        }
    );
    assert_eq!(
        record
            .record(std::slice::from_ref(&too_wide), None)
            .unwrap_err(),
        AttemptIdRefusal::MalformedId {
            found: MAX_ATTEMPT_ID_CHARS + 1,
        }
    );
    assert_eq!(
        record.record(&[String::new()], None).unwrap_err(),
        AttemptIdRefusal::MalformedId { found: 0 }
    );
    assert_eq!(record.len(), 0);

    // Exactly the width the controller mints is accepted; the bound is inclusive.
    let exact = "x".repeat(MAX_ATTEMPT_ID_CHARS);
    record.record(&[], Some(&exact)).unwrap();
    assert_eq!(record.applied(), Some(exact.as_str()));

    // Width is counted in characters, so a multi-byte identifier is bounded by the same rule
    // rather than by its byte length.
    let mut wide_chars = AttemptIds::default();
    let multibyte = "é".repeat(MAX_ATTEMPT_ID_CHARS);
    assert_eq!(multibyte.len(), 2 * MAX_ATTEMPT_ID_CHARS);
    wide_chars
        .record(std::slice::from_ref(&multibyte), None)
        .unwrap();
    assert_eq!(
        wide_chars.disposition(&multibyte),
        AttemptDisposition::Revoked
    );
}

/// The two relationships the record refuses, each leaving it untouched.
#[test]
fn the_record_refuses_a_second_application_and_a_revocation_of_the_applied_one() {
    let mut record = AttemptIds::default();
    record
        .record(&["older_1".to_string()], Some("attempt_1"))
        .unwrap();
    assert_eq!(record.len(), 2);

    assert_eq!(
        record.record(&[], Some("attempt_2")).unwrap_err(),
        AttemptIdRefusal::AlreadyApplied {
            held: "attempt_1".to_string(),
        }
    );
    assert_eq!(
        record
            .record(&["older_2".to_string(), "attempt_1".to_string()], None)
            .unwrap_err(),
        AttemptIdRefusal::RevokesApplied {
            id: "attempt_1".to_string(),
        }
    );
    // A directive that would apply and revoke the same identifier is the same refusal.
    let mut fresh = AttemptIds::default();
    assert_eq!(
        fresh
            .record(&["attempt_1".to_string()], Some("attempt_1"))
            .unwrap_err(),
        AttemptIdRefusal::RevokesApplied {
            id: "attempt_1".to_string(),
        }
    );
    assert_eq!(fresh.len(), 0);

    // Neither refusal recorded anything: the record still says exactly what it said.
    assert_eq!(record.len(), 2);
    assert_eq!(record.applied(), Some("attempt_1"));
    assert_eq!(record.disposition("attempt_1"), AttemptDisposition::Applied);
    assert_eq!(record.disposition("older_1"), AttemptDisposition::Revoked);
    assert_eq!(record.disposition("older_2"), AttemptDisposition::Unknown);
    assert_eq!(record.disposition("attempt_2"), AttemptDisposition::Unknown);

    // Re-applying the identifier it already holds is idempotent, not a second application.
    record.record(&[], Some("attempt_1")).unwrap();
    assert_eq!(record.len(), 2);
}
