//! M11.T26n's worker checklist, driven through `WorkerGrpc::start_execution`.
//!
//! What the guard *does* to its own state: strict-mode monotonicity, pre-registration refusal,
//! duplicate identifiers, the hard cap driven through the handler, and fail-closed overflow with
//! no eviction. What it *answers* is in [`super::refusal_tests`]; the target-generation mismatch,
//! the same-guard race and the `Aborted` taxonomy are the three named D96 rows in
//! [`super::tests`].

use super::tests::{
    AMBIGUOUS, GENERATION, WORKER, acknowledged, addressed_start, announced, applied,
    apply_registration_response, call, disposition, fence_only, fenced_start, generation,
    has_announced, idle, initializing, register, registered, revoke, revoke_owned, settlement,
    strict, tracked, unfenced,
};
use crate::lifecycle_fence::attempt_ids::{AttemptDisposition, MAX_TRACKED_ATTEMPT_IDS};
use arroyo_rpc::fencing::{MAX_ATTEMPT_ID_CHARS, MAX_FENCE_TARGETS};
use arroyo_rpc::grpc::rpc::{StartExecutionOutcome, StartExecutionResp};
use tonic::Code;

/// `count` distinct identifiers of exactly the width the controller mints.
fn bulk_ids(count: usize) -> Vec<String> {
    (0..count)
        .map(|i| format!("{i:0width$x}", width = MAX_ATTEMPT_ID_CHARS))
        .collect()
}

/// Strict mode has two on-switches, no off-switch, and is scoped to this generation.
#[tokio::test]
async fn strict_mode_is_monotonic_for_a_worker_generation() {
    // (a) The registration response turns it on, and a later registration cannot turn it off.
    let (_shutdown, server) = generation(WORKER, GENERATION);
    assert!(!strict(&server));
    register(&server, true);
    assert!(strict(&server));
    register(&server, false);
    assert!(strict(&server), "strict mode has no off-switch");
    let fence_less = call(&server, unfenced("attempt_1")).unwrap_err();
    assert_eq!(fence_less.code(), Code::FailedPrecondition);
    assert!(idle(&server));
    assert_eq!(
        call(&server, fenced_start("attempt_1", 4)).unwrap(),
        settlement(4, StartExecutionOutcome::Applied)
    );

    // (b) Acknowledging a fenced operation turns it on, under a registration that did not.
    let (_shutdown_b, server_b) = registered(false);
    assert!(!strict(&server_b));
    assert_eq!(
        call(&server_b, fence_only(4)).unwrap(),
        settlement(4, StartExecutionOutcome::FenceAcknowledged)
    );
    assert!(strict(&server_b));
    register(&server_b, false);
    assert!(strict(&server_b));
    assert_eq!(
        call(&server_b, unfenced("attempt_1")).unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        call(&server_b, fenced_start("attempt_1", 4)).unwrap(),
        settlement(4, StartExecutionOutcome::Applied)
    );

    // (c) Before the flag day a generation registered to a legacy controller still accepts a
    // fence-less start, and accepting one acknowledges nothing and activates nothing.
    let (_shutdown_c, server_c) = registered(false);
    assert_eq!(
        call(&server_c, unfenced("attempt_1")).unwrap(),
        settlement(0, StartExecutionOutcome::Applied)
    );
    assert_eq!(acknowledged(&server_c), 0);
    assert!(!strict(&server_c));
}

/// Registration gates the fenced protocol and not the legacy route (M11.T26c).
///
/// A fenced directive cannot legitimately precede the registration *request*, so all three of its
/// shapes are refused before it and admitted after — see
/// `the_registration_request_opens_the_fenced_protocol_before_its_answer_arrives` for why the
/// request and not its answer. A fence-less start is the pre-flag-day route and is not gated at
/// all — see `a_legacy_fence_less_start_before_registration_is_admitted_unchanged` for what it
/// does instead.
#[tokio::test]
async fn registration_gates_the_fenced_protocol_and_not_the_legacy_route() {
    for request in [
        fenced_start("attempt_1", 5),
        fence_only(5),
        revoke(5, &["older_1"]),
    ] {
        let (_shutdown, server) = generation(WORKER, GENERATION);
        assert!(!has_announced(&server));
        let refused = call(&server, request.clone()).unwrap_err();
        assert_eq!(refused.code(), Code::FailedPrecondition);
        assert!(!AMBIGUOUS.contains(&refused.code()));
        assert_eq!(acknowledged(&server), 0);
        assert_eq!(tracked(&server), 0);
        assert!(!strict(&server));
        assert!(idle(&server));

        register(&server, false);
        assert!(has_announced(&server));
        assert!(
            call(&server, request).is_ok(),
            "the identical fenced request is admitted once the generation has announced itself"
        );
    }

    // Because a fenced directive is gated here, and a fenced directive is the only on-switch for
    // strict mode other than the registration response itself, strict mode implies an announced
    // generation.
    let (_shutdown, server) = generation(WORKER, GENERATION);
    assert_eq!(
        call(&server, fence_only(5)).unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert!(!strict(&server));
    assert!(!has_announced(&server));
}

/// The fenced protocol opens at the registration *request*, not at its answer (M11.D39e(i)).
///
/// The controller does not wait for its own answer to be applied before it addresses a
/// generation, and cannot: `ControllerGrpc::register_worker` puts the `WorkerConnect` that makes
/// this generation schedulable onto the job's queue *before* it returns `RegisterWorkerResp`, so
/// the `FENCE_ONLY` handshake `arroyo-controller`'s `advance_fence` issues can arrive while that
/// answer is still in flight. A gate on the answer refuses it with `FailedPrecondition`, which
/// the controller reads as definitive rather than as transport, so nothing re-offers the
/// directive and an otherwise healthy scheduling attempt fails outright — see
/// `a_generation_that_has_not_announced_itself_fails_this_controllers_whole_attempt` in
/// `arroyo-controller`.
///
/// The two moments are varied independently, and that is the point: a build that holds them in
/// one flag can never disagree with itself, so every test written against such a build passes
/// whichever moment it meant. Here the announcement is made without an answer, the answer is
/// applied without a preceding announcement — which `registered` will not let anyone write, since
/// it consumes the proof `announce` returns — and each fenced shape is put through both.
#[tokio::test]
async fn the_registration_request_opens_the_fenced_protocol_before_its_answer_arrives() {
    for request in [
        fenced_start("attempt_1", 5),
        fence_only(5),
        revoke(5, &["older_1"]),
    ] {
        // The state a real worker is in for the whole of its registration round trip: it has
        // asked to be registered and has been told nothing back.
        let (_shutdown, server, proof) = announced();
        assert!(!strict(&server));

        assert!(
            call(&server, request.clone()).is_ok(),
            "a generation whose registration answer has not arrived still answers the \
             controller that is answering it"
        );
        assert!(has_announced(&server));
        assert_eq!(acknowledged(&server), 5);

        // And the answer, when it does arrive, lands on a generation the handshake has already
        // moved. Strict mode is monotone across both moments, so a legacy answer arriving after
        // a fenced directive cannot take this generation back out of it.
        apply_registration_response(&server, proof, false);
        assert!(strict(&server));
        assert_eq!(acknowledged(&server), 5);
    }

    // The other end of the same dimension, unchanged: a generation that has announced itself to
    // nobody refuses every fenced shape, definitively.
    let (_shutdown, server) = generation(WORKER, GENERATION);
    let refused = call(&server, fence_only(5)).unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        refused.message(),
        "Worker generation has not begun registration"
    );
    assert!(!AMBIGUOUS.contains(&refused.code()));
    assert_eq!(acknowledged(&server), 0);
    assert!(!strict(&server));
}

/// The pre-flag-day compatibility guarantee: a fence-less start arriving before this generation
/// announces itself takes the route it took before M11.T26d existed.
///
/// This is M11.D75's declared compatibility window, and it is the path a worker-first rollout
/// runs on between the worker upgrade and the controller upgrade — see
/// `docs/lifecycle-fence-rollout.md` §3. The window is real in a second way too: the worker
/// serves its gRPC port before it announces itself at all. Every assertion here is a landed T08
/// behaviour, and the response is byte-identical to the one a worker predating the lifecycle
/// fields sends, which is `StartExecutionResp::default()`.
#[tokio::test]
async fn a_legacy_fence_less_start_before_registration_is_admitted_unchanged() {
    let (_shutdown, server) = generation(WORKER, GENERATION);
    assert!(!has_announced(&server));
    assert!(!strict(&server));

    let accepted = call(&server, unfenced("attempt_1")).unwrap();
    assert_eq!(accepted, settlement(0, StartExecutionOutcome::Applied));
    assert_eq!(
        accepted,
        StartExecutionResp::default(),
        "the response a worker predating the lifecycle fields sends"
    );
    assert_eq!(applied(&server), Some("attempt_1".to_string()));
    assert!(initializing(&server));

    // The identical retry is acknowledged rather than replayed, and a different attempt is
    // refused definitively — both unchanged from T08.
    assert_eq!(call(&server, unfenced("attempt_1")).unwrap(), accepted);
    assert_eq!(
        call(&server, unfenced("attempt_2")).unwrap_err().code(),
        Code::FailedPrecondition
    );

    // Admitting it acknowledged nothing and activated nothing: the increment stays inactive.
    assert_eq!(acknowledged(&server), 0);
    assert!(!strict(&server));
    assert!(!has_announced(&server));

    // A controller predating the idempotency key sends no identifier at all; that is still
    // accepted and still recorded under no identifier.
    let (_shutdown_b, server_b) = generation(WORKER, GENERATION);
    assert_eq!(
        call(&server_b, unfenced("")).unwrap(),
        StartExecutionResp::default()
    );
    assert_eq!(applied(&server_b), None);
    assert_eq!(tracked(&server_b), 0);
    assert!(initializing(&server_b));

    // Once strict mode is on the same request fails closed, which is the post-flag-day rule.
    let (_shutdown_c, server_c) = registered(true);
    assert_eq!(
        call(&server_c, unfenced("attempt_1")).unwrap_err().code(),
        Code::FailedPrecondition
    );
}

/// A duplicated or re-delivered directive is answered the same way twice and costs nothing.
#[tokio::test]
async fn duplicate_applied_and_revoked_identifiers_are_idempotent() {
    let (_shutdown, server) = registered(true);

    let revoked = call(&server, revoke(4, &["older_1", "older_2"])).unwrap();
    assert_eq!(revoked, settlement(4, StartExecutionOutcome::Revoked));
    assert_eq!(tracked(&server), 2);
    assert_eq!(
        call(&server, revoke(4, &["older_1", "older_2"])).unwrap(),
        revoked
    );
    assert_eq!(tracked(&server), 2);

    // A directive naming one identifier twice costs one entry, not two.
    assert_eq!(
        call(&server, revoke(4, &["older_3", "older_3"])).unwrap(),
        revoked
    );
    assert_eq!(tracked(&server), 3);

    // The applied identifier: the identical retry is acknowledged, not replayed.
    let applied_first = call(&server, fenced_start("attempt_1", 4)).unwrap();
    assert_eq!(applied_first, settlement(4, StartExecutionOutcome::Applied));
    assert_eq!(tracked(&server), 4);
    assert!(initializing(&server));
    assert_eq!(
        call(&server, fenced_start("attempt_1", 4)).unwrap(),
        applied_first
    );
    assert_eq!(tracked(&server), 4);

    // A retry delayed past a fence advance is still acknowledged, and still advances the fence.
    assert_eq!(
        call(&server, fenced_start("attempt_1", 9)).unwrap(),
        settlement(9, StartExecutionOutcome::Applied)
    );
    assert_eq!(tracked(&server), 4);
    assert_eq!(applied(&server), Some("attempt_1".to_string()));

    // A revoked identifier is never applied, however often it is re-offered as a start.
    for _ in 0..3 {
        assert_eq!(
            call(&server, fenced_start("older_1", 9))
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
    }
    assert_eq!(disposition(&server, "older_1"), AttemptDisposition::Revoked);
    assert_eq!(tracked(&server), 4);
}

/// A fenced start may carry the revocations its own fence supersedes: one directive applies the
/// program and makes the named identifiers permanently non-applicable, and reports `APPLIED`.
#[tokio::test]
async fn a_start_may_carry_the_revocations_its_fence_supersedes() {
    let (_shutdown, server) = registered(true);
    let mut request = fenced_start("attempt_2", 5);
    request.revoked_execution_ids = vec!["attempt_1".to_string(), "attempt_0".to_string()];

    assert_eq!(
        call(&server, request).unwrap(),
        settlement(5, StartExecutionOutcome::Applied)
    );
    assert_eq!(acknowledged(&server), 5);
    assert!(initializing(&server));
    assert_eq!(applied(&server), Some("attempt_2".to_string()));
    assert_eq!(
        disposition(&server, "attempt_1"),
        AttemptDisposition::Revoked
    );
    assert_eq!(
        disposition(&server, "attempt_0"),
        AttemptDisposition::Revoked
    );
    assert_eq!(tracked(&server), 3);

    // The revocations took effect, so neither identifier can be applied afterwards.
    for revoked in ["attempt_1", "attempt_0"] {
        assert_eq!(
            call(&server, fenced_start(revoked, 5)).unwrap_err().code(),
            Code::FailedPrecondition
        );
    }
}

/// The hard cap, reached through the handler: N-1 and N fit, N+1 is refused whole, and the
/// refusal evicts nothing belonging to this live generation.
#[tokio::test]
async fn an_overflowing_directive_is_refused_whole_and_evicts_nothing() {
    let (_shutdown, server) = registered(true);
    let bulk = bulk_ids(MAX_FENCE_TARGETS);

    // N-1: one whole well-formed revocation list, minus one.
    assert_eq!(
        call(&server, revoke_owned(4, &bulk[..MAX_FENCE_TARGETS - 1])).unwrap(),
        settlement(4, StartExecutionOutcome::Revoked)
    );
    assert_eq!(tracked(&server), MAX_TRACKED_ATTEMPT_IDS - 2);

    // N: the whole list plus this generation's own applied identifier is exactly the capacity.
    assert_eq!(
        call(&server, revoke_owned(4, &bulk)).unwrap(),
        settlement(4, StartExecutionOutcome::Revoked)
    );
    assert_eq!(
        call(&server, fenced_start("attempt_1", 4)).unwrap(),
        settlement(4, StartExecutionOutcome::Applied)
    );
    assert_eq!(tracked(&server), MAX_TRACKED_ATTEMPT_IDS);

    // N+1: refused whole, with a definitive status that is not a transport code.
    let overflow = call(&server, revoke(5, &["one_too_many"])).unwrap_err();
    assert_eq!(overflow.code(), Code::ResourceExhausted);
    assert!(!AMBIGUOUS.contains(&overflow.code()));

    // Fail closed: the fence the refused directive carried was not acknowledged either.
    assert_eq!(acknowledged(&server), 4);
    assert_eq!(
        disposition(&server, "one_too_many"),
        AttemptDisposition::Unknown
    );

    // No live eviction: every identifier this generation held it still holds.
    assert_eq!(tracked(&server), MAX_TRACKED_ATTEMPT_IDS);
    assert_eq!(applied(&server), Some("attempt_1".to_string()));
    for id in &bulk {
        assert_eq!(disposition(&server, id), AttemptDisposition::Revoked);
    }
    assert_eq!(
        call(&server, fenced_start("attempt_1", 4)).unwrap(),
        settlement(4, StartExecutionOutcome::Applied)
    );

    // A full record still answers a directive that adds nothing to it.
    assert_eq!(
        call(&server, revoke_owned(5, &bulk[..3])).unwrap(),
        settlement(5, StartExecutionOutcome::Revoked)
    );
    assert_eq!(tracked(&server), MAX_TRACKED_ATTEMPT_IDS);
}

/// A worker running under generation zero is addressed by no fence, so it fails closed under
/// strict mode instead of matching a directive by accident.
#[tokio::test]
async fn a_worker_generation_zero_is_addressed_by_no_fence() {
    let (_shutdown, server) = generation(WORKER, 0);
    register(&server, false);

    // Nothing can address it: not its own worker id under generation zero, which is not an
    // addressable pair at all, and not any other generation.
    for target in [0, 1, GENERATION] {
        let refused = call(&server, addressed_start("attempt_1", 5, WORKER, target)).unwrap_err();
        assert!(
            matches!(
                refused.code(),
                Code::FailedPrecondition | Code::InvalidArgument
            ),
            "{target}: {refused:?}"
        );
        assert_eq!(acknowledged(&server), 0);
        assert!(idle(&server));
    }

    // Before the flag day it still runs unfenced work.
    assert_eq!(
        call(&server, unfenced("attempt_1")).unwrap(),
        settlement(0, StartExecutionOutcome::Applied)
    );

    // Under strict mode it can do nothing at all, which is the fail-closed answer.
    let (_shutdown_b, server_b) = generation(WORKER, 0);
    register(&server_b, true);
    assert_eq!(
        call(&server_b, unfenced("attempt_1")).unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        call(&server_b, fenced_start("attempt_1", 5))
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert!(idle(&server_b));
}
