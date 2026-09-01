//! What the guard *answers*: the agreements it requires between a fence, a target and an
//! operation, the status code of every refusal it can give, and the shape of every response it
//! can produce.
//!
//! Kept apart from [`super::guard_tests`], which covers what the guard *does* to its own state.
//! The two enumerations here are the ones that have to stay exhaustive as the guard grows: a new
//! refusal has to name its code, and a new response has to decode through the wire seam.

use super::tests::{
    AMBIGUOUS, GENERATION, WORKER, acknowledged, addressed_start, call, disposition, fence_only,
    fenced_start, generation, idle, register, registered, revoke, revoke_owned, strict, tracked,
    unfenced,
};
use crate::lifecycle_fence::attempt_ids::AttemptDisposition;
use crate::{EngineState, WorkerExecutionPhase};
use arroyo_rpc::fence_wire::observed_settlement;
use arroyo_rpc::fencing::{MAX_ATTEMPT_ID_CHARS, MAX_FENCE_TARGETS};
use arroyo_rpc::grpc::rpc::worker_grpc_server::WorkerGrpc;
use arroyo_rpc::grpc::rpc::{
    JobFinishedReq, LifecycleOperation, StartExecutionOutcome, StartExecutionReq,
};
use arroyo_server_common::shutdown::Shutdown;
use futures::FutureExt;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::time::SystemTime;
use tokio::sync::mpsc::channel;
use tonic::{Code, Request};

/// `count` distinct identifiers of exactly the width the controller mints.
fn bulk_ids(count: usize) -> Vec<String> {
    (0..count)
        .map(|i| format!("{i:0width$x}", width = MAX_ATTEMPT_ID_CHARS))
        .collect()
}

/// A fence, a target generation and an operation are one directive: mutating the agreement
/// between them is refused, and refused before anything is recorded.
#[tokio::test]
async fn a_fence_a_target_and_an_operation_must_agree() {
    let cases: Vec<(&str, StartExecutionReq)> = vec![
        (
            "a fence addressed to no generation",
            StartExecutionReq {
                target_worker_id: 0,
                target_worker_generation: 0,
                ..fenced_start("attempt_1", 5)
            },
        ),
        (
            "a target addressed under no fence",
            StartExecutionReq {
                lifecycle_fence: 0,
                ..fenced_start("attempt_1", 5)
            },
        ),
        (
            "a worker id carried without a generation or a fence",
            StartExecutionReq {
                lifecycle_fence: 0,
                target_worker_generation: 0,
                ..fenced_start("attempt_1", 5)
            },
        ),
        (
            "an operation this build cannot name",
            StartExecutionReq {
                lifecycle_operation: 99,
                ..fenced_start("attempt_1", 5)
            },
        ),
        (
            "a fence-only directive under no fence",
            StartExecutionReq {
                lifecycle_fence: 0,
                target_worker_id: 0,
                target_worker_generation: 0,
                ..fence_only(5)
            },
        ),
        (
            "revocations under no fence",
            StartExecutionReq {
                lifecycle_fence: 0,
                target_worker_id: 0,
                target_worker_generation: 0,
                lifecycle_operation: LifecycleOperation::Start as i32,
                ..revoke(5, &["older_1"])
            },
        ),
        (
            "an empty identifier named for revocation",
            revoke(5, &["older_1", ""]),
        ),
        (
            "a fence-only directive naming identifiers to revoke",
            StartExecutionReq {
                lifecycle_operation: LifecycleOperation::FenceOnly as i32,
                ..revoke(5, &["older_1"])
            },
        ),
        ("a revoke directive naming nothing", revoke(5, &[])),
        (
            "a start revoking the identifier it would apply",
            StartExecutionReq {
                lifecycle_operation: LifecycleOperation::Start as i32,
                start_execution_id: "attempt_1".to_string(),
                ..revoke(5, &["attempt_1"])
            },
        ),
        (
            "an identifier wider than one the controller mints",
            unfenced(&"x".repeat(MAX_ATTEMPT_ID_CHARS + 1)),
        ),
    ];

    for (label, request) in cases {
        let (_shutdown, server) = registered(false);
        let refused = call(&server, request)
            .map(|response| panic!("{label} was answered with {response:?}"))
            .unwrap_err();
        assert!(
            matches!(
                refused.code(),
                Code::InvalidArgument | Code::FailedPrecondition
            ),
            "{label}: {refused:?}"
        );
        assert!(!AMBIGUOUS.contains(&refused.code()), "{label}");
        assert_eq!(acknowledged(&server), 0, "{label}");
        assert_eq!(tracked(&server), 0, "{label}");
        assert!(!strict(&server), "{label}");
        assert!(idle(&server), "{label}");
    }

    // The same rule against an identifier this generation has already applied: the whole
    // directive is refused, because the response cannot describe a partial revocation and
    // an applied attempt ends by observed teardown, not by revocation.
    let (_shutdown, server) = registered(false);
    call(&server, fenced_start("attempt_1", 5)).unwrap();
    let refused = call(&server, revoke(6, &["older_1", "attempt_1"])).unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        acknowledged(&server),
        5,
        "the refused fence is not acknowledged"
    );
    assert_eq!(disposition(&server, "older_1"), AttemptDisposition::Unknown);
    assert_eq!(
        disposition(&server, "attempt_1"),
        AttemptDisposition::Applied
    );
    assert_eq!(tracked(&server), 1);
}

/// Every refusal this worker can give for a `StartExecution`, with its code.
///
/// The point of the enumeration is the last two assertions: none of these is one of the four
/// codes M11.D39e(iv) makes ambiguous, so no refusal added here can be mistaken for a transport
/// outcome and retried under the same admission, and in particular none of them is
/// `Unavailable`.
///
/// The other directive's refusals are enumerated the same way, and must stay so, by
/// `super::commit_tests::every_commit_refusal_this_worker_gives_is_definitive`. Two lists rather
/// than one because they are two handlers with two decisions; each is exhaustive over its own.
#[tokio::test]
async fn every_refusal_this_worker_gives_is_definitive() {
    let mut codes: Vec<(&str, Code)> = vec![];

    {
        let (_shutdown, server) = registered(true);
        let _held = server.state.lifecycle.lock().unwrap();
        codes.push((
            "contended guard",
            call(&server, fenced_start("attempt_1", 5))
                .unwrap_err()
                .code(),
        ));
    }
    {
        let (_shutdown, server) = generation(WORKER, GENERATION);
        codes.push((
            "a fenced directive before registration",
            call(&server, fenced_start("attempt_1", 5))
                .unwrap_err()
                .code(),
        ));
    }
    {
        let (_shutdown, server) = registered(true);
        codes.push((
            "fence-less under strict mode",
            call(&server, unfenced("attempt_1")).unwrap_err().code(),
        ));
    }
    {
        let (_shutdown, server) = registered(true);
        codes.push((
            "addressed to another generation",
            call(
                &server,
                addressed_start("attempt_1", 5, WORKER, GENERATION - 1),
            )
            .unwrap_err()
            .code(),
        ));
    }
    {
        let (_shutdown, server) = registered(true);
        call(&server, fence_only(9)).unwrap();
        codes.push((
            "fence older than the acknowledged one",
            call(&server, fenced_start("attempt_1", 5))
                .unwrap_err()
                .code(),
        ));
    }
    {
        let (_shutdown, server) = registered(true);
        call(&server, revoke(4, &["attempt_1"])).unwrap();
        codes.push((
            "identifier already revoked",
            call(&server, fenced_start("attempt_1", 4))
                .unwrap_err()
                .code(),
        ));
    }
    {
        let (_shutdown, server) = registered(true);
        call(&server, fenced_start("attempt_1", 4)).unwrap();
        codes.push((
            "another execution already initializing",
            call(&server, fenced_start("attempt_2", 4))
                .unwrap_err()
                .code(),
        ));
        WorkerGrpc::job_finished(&server, Request::new(JobFinishedReq {}))
            .now_or_never()
            .expect("job_finished must complete without awaiting")
            .unwrap();
        assert!(idle(&server));
        codes.push((
            "another execution already applied",
            call(&server, fenced_start("attempt_2", 4))
                .unwrap_err()
                .code(),
        ));
    }
    {
        let (_shutdown, server) = registered(true);
        call(&server, fenced_start("attempt_1", 4)).unwrap();
        codes.push((
            "revocation names the applied identifier",
            call(&server, revoke(4, &["attempt_1"])).unwrap_err().code(),
        ));
    }
    {
        let (_shutdown, server) = generation(WORKER, 0);
        register(&server, false);
        codes.push((
            "a generation no fence can address",
            call(&server, addressed_start("attempt_1", 5, WORKER, GENERATION))
                .unwrap_err()
                .code(),
        ));
    }
    {
        let (_shutdown, server) = registered(true);
        codes.push((
            "malformed lifecycle fields",
            call(
                &server,
                StartExecutionReq {
                    target_worker_generation: 0,
                    ..fenced_start("attempt_1", 5)
                },
            )
            .unwrap_err()
            .code(),
        ));
    }
    {
        let (_shutdown, server) = registered(false);
        codes.push((
            "identifier wider than one the controller mints",
            call(&server, unfenced(&"x".repeat(MAX_ATTEMPT_ID_CHARS + 1)))
                .unwrap_err()
                .code(),
        ));
    }
    {
        let (_shutdown, server) = registered(true);
        let bulk = bulk_ids(MAX_FENCE_TARGETS);
        call(&server, revoke_owned(4, &bulk)).unwrap();
        call(&server, fenced_start("attempt_1", 4)).unwrap();
        codes.push((
            "the identifier record is full",
            call(&server, revoke(4, &["one_too_many"]))
                .unwrap_err()
                .code(),
        ));
    }

    {
        // The poisoned-guard answer completes the enumeration. It is the one refusal that is not
        // about the request at all, and `Internal` keeps it out of the ambiguous table too.
        let (_shutdown, server) = registered(true);
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let poisoned = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _held = server.state.lifecycle.lock().unwrap();
            panic!("poisoning the guard");
        }));
        std::panic::set_hook(hook);
        assert!(poisoned.is_err());
        codes.push((
            "poisoned guard",
            call(&server, fenced_start("attempt_1", 5))
                .unwrap_err()
                .code(),
        ));
    }

    assert_eq!(
        codes,
        vec![
            ("contended guard", Code::Aborted),
            (
                "a fenced directive before registration",
                Code::FailedPrecondition
            ),
            ("fence-less under strict mode", Code::FailedPrecondition),
            ("addressed to another generation", Code::FailedPrecondition),
            (
                "fence older than the acknowledged one",
                Code::FailedPrecondition
            ),
            ("identifier already revoked", Code::FailedPrecondition),
            (
                "another execution already initializing",
                Code::FailedPrecondition
            ),
            (
                "another execution already applied",
                Code::FailedPrecondition
            ),
            (
                "revocation names the applied identifier",
                Code::FailedPrecondition
            ),
            (
                "a generation no fence can address",
                Code::FailedPrecondition
            ),
            ("malformed lifecycle fields", Code::InvalidArgument),
            (
                "identifier wider than one the controller mints",
                Code::InvalidArgument
            ),
            ("the identifier record is full", Code::ResourceExhausted),
            ("poisoned guard", Code::Internal),
        ]
    );
    for (label, code) in &codes {
        assert!(!AMBIGUOUS.contains(code), "{label} answered with {code:?}");
    }
}

/// Every response the guard can produce reads back through the wire seam as one settlement.
///
/// `observed_settlement` refuses a response that claims to have acknowledged a fence while
/// reporting none, so running the whole producible set through it is what pins
/// `observed_lifecycle_fence` and `outcome` to each other rather than only to this file.
#[tokio::test]
async fn every_response_the_guard_produces_decodes_as_one_settlement() {
    let mut produced = vec![];

    let (_shutdown, server) = registered(false);
    produced.push(call(&server, unfenced("attempt_0")).unwrap());
    produced.push(call(&server, unfenced("attempt_0")).unwrap());

    let (_shutdown_b, server_b) = registered(true);
    produced.push(call(&server_b, fence_only(4)).unwrap());
    produced.push(call(&server_b, revoke(5, &["older_1"])).unwrap());
    produced.push(call(&server_b, fenced_start("attempt_1", 6)).unwrap());
    produced.push(call(&server_b, fenced_start("attempt_1", 7)).unwrap());

    let decoded: Vec<_> = produced
        .iter()
        .map(|response| {
            let settlement = observed_settlement(response).expect("a well-formed settlement");
            (settlement.observed_fence(), settlement.outcome())
        })
        .collect();
    assert_eq!(
        decoded,
        vec![
            (None, StartExecutionOutcome::Applied),
            (None, StartExecutionOutcome::Applied),
            (Some(4), StartExecutionOutcome::FenceAcknowledged),
            (Some(5), StartExecutionOutcome::Revoked),
            (Some(6), StartExecutionOutcome::Applied),
            (Some(7), StartExecutionOutcome::Applied),
        ]
    );
}

/// Every execution phase that cannot admit a start answers definitively, and none of them
/// acknowledges the fence it refused.
///
/// The four non-`Idle` phases are the landed T08 answers carried into the guard. `Initializing`
/// is reachable only by admitting a start — no fixture can forge it, which is the point of the
/// witness it carries — so it is covered by `every_refusal_this_worker_gives_is_definitive`
/// above; the other three are built here.
#[tokio::test]
async fn every_phase_that_cannot_admit_a_start_answers_definitively() {
    fn engine_state(shutdown: &Shutdown) -> EngineState {
        EngineState {
            sources: vec![],
            sinks: vec![],
            operator_to_node: HashMap::new(),
            operator_controls: HashMap::new(),
            shutdown_guard: shutdown.guard("engine-state"),
        }
    }

    let (shutdown, server) = registered(true);
    for (label, phase) in [
        (
            "failed",
            WorkerExecutionPhase::Failed {
                started_at: SystemTime::now(),
                error_message: "engine failed".to_string(),
            },
        ),
        (
            "running",
            WorkerExecutionPhase::Running(engine_state(&shutdown)),
        ),
        (
            "waiting on leader",
            WorkerExecutionPhase::WaitingOnLeader {
                control_rx: channel(1).1,
                job_controller_addr: "http://127.0.0.1:0".to_string(),
                engine_state: engine_state(&shutdown),
            },
        ),
    ] {
        *server.state.lifecycle.lock().unwrap().execution_mut() = phase;
        let refused = call(&server, fenced_start("attempt_1", 5)).unwrap_err();
        assert_eq!(refused.code(), Code::FailedPrecondition, "{label}");
        assert!(!AMBIGUOUS.contains(&refused.code()), "{label}");
        assert_eq!(acknowledged(&server), 0, "{label}");
        assert_eq!(tracked(&server), 0, "{label}");
    }
}
