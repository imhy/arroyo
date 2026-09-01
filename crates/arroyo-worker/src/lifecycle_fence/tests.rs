//! The three D96 rows M11.T26d owns, and the helpers the sibling test modules share.
//!
//! Every request here goes through `WorkerGrpc::start_execution`, not through
//! [`WorkerLifecycle`] directly: the guard is only worth anything if the production handler is
//! what reaches it.

use crate::lifecycle_fence::attempt_ids::AttemptDisposition;
use crate::lifecycle_fence::guard::WorkerLifecycle;
use crate::{WorkerExecutionPhase, WorkerServer};
use arroyo_rpc::grpc::rpc::worker_grpc_server::WorkerGrpc;
use arroyo_rpc::grpc::rpc::{
    LifecycleOperation, StartExecutionOutcome, StartExecutionReq, StartExecutionResp,
};
use arroyo_server_common::shutdown::{Shutdown, SignalBehavior};
use arroyo_types::{JobId, MachineId, PipelineId, WorkerId};
use futures::FutureExt;
use std::sync::Arc;
use tonic::{Code, Request, Response, Status};

/// This worker generation's own worker id.
pub(super) const WORKER: u64 = 7;
/// This worker generation's own generation, chosen above 1 so a predecessor can be addressed.
pub(super) const GENERATION: u64 = 3;

/// The four gRPC codes M11.D39e(iv) makes ambiguous: the controller retries these with the same
/// identifier. Nothing this worker answers may be one of them.
pub(super) const AMBIGUOUS: [Code; 4] = [
    Code::Cancelled,
    Code::Unknown,
    Code::DeadlineExceeded,
    Code::Unavailable,
];

/// A worker generation that has not registered.
pub(super) fn generation(worker_id: u64, generation: u64) -> (Shutdown, WorkerServer) {
    let shutdown = Shutdown::new("lifecycle-fence-test", SignalBehavior::None);
    let server = WorkerServer::new(
        MachineId(Arc::new("machine_1".to_string())),
        WorkerId(worker_id),
        PipelineId(Arc::new("pipeline_1".to_string())),
        JobId(Arc::new("job_1".to_string())),
        generation,
        shutdown.guard("worker"),
    );
    (shutdown, server)
}

/// A [`WORKER`]/[`GENERATION`] worker whose registration has completed.
pub(super) fn registered(requires_lifecycle_fence: bool) -> (Shutdown, WorkerServer) {
    let (shutdown, server) = generation(WORKER, GENERATION);
    register(&server, requires_lifecycle_fence);
    (shutdown, server)
}

/// Records the registration this worker's `start_async` records after `register_worker` returns.
pub(super) fn register(server: &WorkerServer, requires_lifecycle_fence: bool) {
    server
        .state
        .lifecycle
        .lock()
        .unwrap()
        .registered(requires_lifecycle_fence);
}

/// Calls the production handler, which never awaits — that is M11.D39e(iii)'s non-blocking
/// admission, and polling it once is how these tests assert it rather than assume it.
#[allow(clippy::result_large_err)]
pub(super) fn call(
    server: &WorkerServer,
    req: StartExecutionReq,
) -> Result<StartExecutionResp, Status> {
    WorkerGrpc::start_execution(server, Request::new(req))
        .now_or_never()
        .expect("start_execution must complete without awaiting")
        .map(Response::into_inner)
}

pub(super) fn read<T>(server: &WorkerServer, f: impl FnOnce(&WorkerLifecycle) -> T) -> T {
    f(&server.state.lifecycle.lock().unwrap())
}

pub(super) fn acknowledged(server: &WorkerServer) -> u64 {
    read(server, WorkerLifecycle::acknowledged_fence)
}

pub(super) fn applied(server: &WorkerServer) -> Option<String> {
    read(server, |l| l.applied().map(str::to_string))
}

pub(super) fn disposition(server: &WorkerServer, id: &str) -> AttemptDisposition {
    read(server, |l| l.disposition(id))
}

pub(super) fn tracked(server: &WorkerServer) -> usize {
    read(server, WorkerLifecycle::tracked_ids)
}

pub(super) fn strict(server: &WorkerServer) -> bool {
    read(server, WorkerLifecycle::is_strict)
}

pub(super) fn has_registered(server: &WorkerServer) -> bool {
    read(server, WorkerLifecycle::is_registered)
}

pub(super) fn idle(server: &WorkerServer) -> bool {
    read(server, |l| {
        matches!(l.execution(), WorkerExecutionPhase::Idle)
    })
}

pub(super) fn initializing(server: &WorkerServer) -> bool {
    read(server, |l| {
        matches!(l.execution(), WorkerExecutionPhase::Initializing { .. })
    })
}

/// A start carrying no lifecycle fields at all — what a controller predating M11.T26c sends.
pub(super) fn unfenced(id: &str) -> StartExecutionReq {
    StartExecutionReq {
        start_execution_id: id.to_string(),
        ..Default::default()
    }
}

/// A start under `fence`, addressed to this worker generation.
pub(super) fn fenced_start(id: &str, fence: u64) -> StartExecutionReq {
    addressed_start(id, fence, WORKER, GENERATION)
}

/// A start under `fence`, addressed to worker `to_worker` generation `to_generation`.
pub(super) fn addressed_start(
    id: &str,
    fence: u64,
    to_worker: u64,
    to_generation: u64,
) -> StartExecutionReq {
    StartExecutionReq {
        start_execution_id: id.to_string(),
        lifecycle_fence: fence,
        target_worker_id: to_worker,
        target_worker_generation: to_generation,
        lifecycle_operation: LifecycleOperation::Start as i32,
        ..Default::default()
    }
}

/// A fence advancement addressed to this worker generation, applying no program.
pub(super) fn fence_only(fence: u64) -> StartExecutionReq {
    StartExecutionReq {
        lifecycle_fence: fence,
        target_worker_id: WORKER,
        target_worker_generation: GENERATION,
        lifecycle_operation: LifecycleOperation::FenceOnly as i32,
        ..Default::default()
    }
}

/// A revocation of `ids` under `fence`, addressed to this worker generation.
pub(super) fn revoke(fence: u64, ids: &[&str]) -> StartExecutionReq {
    revoke_owned(
        fence,
        &ids.iter().map(|id| id.to_string()).collect::<Vec<_>>(),
    )
}

pub(super) fn revoke_owned(fence: u64, ids: &[String]) -> StartExecutionReq {
    StartExecutionReq {
        lifecycle_fence: fence,
        target_worker_id: WORKER,
        target_worker_generation: GENERATION,
        lifecycle_operation: LifecycleOperation::Revoke as i32,
        revoked_execution_ids: ids.to_vec(),
        ..Default::default()
    }
}

/// The response a generation that has acknowledged `fence` gives for `outcome`.
pub(super) fn settlement(fence: u64, outcome: StartExecutionOutcome) -> StartExecutionResp {
    StartExecutionResp {
        observed_lifecycle_fence: fence,
        outcome: outcome as i32,
    }
}

/// D96 row 19 — fence acknowledgement serializes with start application.
///
/// M11.D39d: *"a start either linearizes before the fence acknowledgement (and is reported
/// applied, requiring observed generation teardown) or after it (and is rejected stale); there
/// is no validate→apply gap."* The proof is that there are exactly two reachable orders and a
/// closed-form outcome for each, plus a third case showing that a request which cannot take the
/// guard applies nothing at all rather than landing in a gap between the two.
#[tokio::test]
async fn fence_ack_serializes_with_start_application() {
    // Order A — the start linearizes first, and is reported applied.
    let (_shutdown, server) = registered(false);
    assert_eq!(
        call(&server, fenced_start("attempt_a", 5)).unwrap(),
        settlement(5, StartExecutionOutcome::Applied)
    );
    assert_eq!(acknowledged(&server), 5);
    assert_eq!(applied(&server), Some("attempt_a".to_string()));
    assert!(initializing(&server));

    // The later acknowledgement does not retract it: an applied attempt ends by observed
    // generation teardown, which is why revoking it is refused rather than quietly accepted.
    assert_eq!(
        call(&server, fence_only(9)).unwrap(),
        settlement(9, StartExecutionOutcome::FenceAcknowledged)
    );
    assert_eq!(applied(&server), Some("attempt_a".to_string()));
    assert!(initializing(&server));
    let refused = call(&server, revoke(9, &["attempt_a"])).unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        disposition(&server, "attempt_a"),
        AttemptDisposition::Applied
    );

    // Order B — the acknowledgement linearizes first, and the start is permanently stale.
    let (_shutdown_b, server_b) = registered(false);
    assert_eq!(
        call(&server_b, fence_only(9)).unwrap(),
        settlement(9, StartExecutionOutcome::FenceAcknowledged)
    );
    for _ in 0..3 {
        let stale = call(&server_b, fenced_start("attempt_a", 5)).unwrap_err();
        assert_eq!(stale.code(), Code::FailedPrecondition);
    }
    assert_eq!(applied(&server_b), None);
    assert_eq!(
        disposition(&server_b, "attempt_a"),
        AttemptDisposition::Unknown
    );
    assert_eq!(tracked(&server_b), 0);
    assert_eq!(acknowledged(&server_b), 9);
    assert!(idle(&server_b));

    // Staleness is about the fence and not about the worker: the same generation still admits a
    // start issued under the fence it acknowledged.
    assert_eq!(
        call(&server_b, fenced_start("attempt_b", 9)).unwrap(),
        settlement(9, StartExecutionOutcome::Applied)
    );

    // There is no third order. A request that arrives while the guard is held is answered
    // before any of the above runs, so it cannot be half-applied.
    let (_shutdown_c, server_c) = registered(false);
    let busy = {
        let _held = server_c.state.lifecycle.lock().unwrap();
        call(&server_c, fenced_start("attempt_a", 5)).unwrap_err()
    };
    assert_eq!(busy.code(), Code::Aborted);
    assert_eq!(acknowledged(&server_c), 0);
    assert_eq!(applied(&server_c), None);
    assert!(idle(&server_c));
}

/// D96 row 22 — a reused endpoint cannot impersonate its predecessor generation.
///
/// Identity is the (worker id, generation) pair and nothing else: there is no address in the
/// message, and the worker id alone agrees in the case that matters — a restarted worker holding
/// its predecessor's id and answering at its predecessor's address. Each half is varied
/// independently, including the halves that agree.
#[tokio::test]
async fn worker_generation_mismatch_rejects_delayed_start() {
    for (to_worker, to_generation, admits) in [
        (WORKER, GENERATION, true),
        // The delayed request: this worker's id, this worker's address, the predecessor's
        // generation. This is the case a worker id or an address cannot distinguish.
        (WORKER, GENERATION - 1, false),
        (WORKER, GENERATION + 1, false),
        (WORKER + 1, GENERATION, false),
        (WORKER + 1, GENERATION - 1, false),
    ] {
        let (_shutdown, server) = registered(false);
        let req = addressed_start("attempt_1", 5, to_worker, to_generation);
        match (call(&server, req), admits) {
            (Ok(response), true) => {
                assert_eq!(response, settlement(5, StartExecutionOutcome::Applied));
                assert_eq!(acknowledged(&server), 5);
                assert_eq!(applied(&server), Some("attempt_1".to_string()));
                assert!(initializing(&server));
            }
            (Err(status), false) => {
                assert_eq!(status.code(), Code::FailedPrecondition);
                assert_eq!(acknowledged(&server), 0);
                assert_eq!(applied(&server), None);
                assert_eq!(tracked(&server), 0);
                assert!(idle(&server));
            }
            (result, admits) => {
                panic!("({to_worker}, {to_generation}) admits={admits}: {result:?}")
            }
        }
    }

    // A mismatched fence directive is refused for the same reason, and acknowledges nothing —
    // otherwise a predecessor's fence could raise this generation's floor.
    let (_shutdown, server) = registered(false);
    let misaddressed = StartExecutionReq {
        target_worker_generation: GENERATION - 1,
        ..fence_only(9)
    };
    assert_eq!(
        call(&server, misaddressed).unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert_eq!(acknowledged(&server), 0);
    assert!(!strict(&server));

    // The refusal is about the pair, not about fencing: before the flag day this same
    // generation still accepts a start that addresses nobody.
    assert_eq!(
        call(&server, unfenced("attempt_1")).unwrap(),
        settlement(0, StartExecutionOutcome::Applied)
    );
}

/// D96 row 23 — `Aborted` is definitive "nothing applied".
///
/// It settles the attempt with no effect at all, so it is not part of the ambiguous
/// same-identifier retry table (M11.D39e(iii)/(iv)); only a later scheduling attempt may reuse
/// the identifier, and this proves it still can.
#[tokio::test]
async fn aborted_is_definitive_no_apply() {
    let (_shutdown, server) = registered(true);
    let request = fenced_start("attempt_1", 5);

    let busy = {
        let _held = server.state.lifecycle.lock().unwrap();
        call(&server, request.clone()).unwrap_err()
    };
    assert_eq!(busy.code(), Code::Aborted);
    assert!(!AMBIGUOUS.contains(&busy.code()));

    // Nothing applied: no fence advanced, no identifier recorded, no phase moved.
    assert_eq!(acknowledged(&server), 0);
    assert_eq!(applied(&server), None);
    assert_eq!(
        disposition(&server, "attempt_1"),
        AttemptDisposition::Unknown
    );
    assert_eq!(tracked(&server), 0);
    assert!(idle(&server));

    // A later scheduling attempt may retry it, because the first consumed nothing.
    assert_eq!(
        call(&server, request).unwrap(),
        settlement(5, StartExecutionOutcome::Applied)
    );

    // Contention still wins over an authoritative phase answer: the busy guard is answered
    // before the phase is even read, so `Aborted` never turns into "already running".
    let busy_again = {
        let _held = server.state.lifecycle.lock().unwrap();
        call(&server, fenced_start("attempt_2", 6)).unwrap_err()
    };
    assert_eq!(busy_again.code(), Code::Aborted);
    assert_eq!(acknowledged(&server), 5);
    assert_eq!(applied(&server), Some("attempt_1".to_string()));

    // With the guard free the same second attempt gets the definitive phase answer instead,
    // which is a different refusal and still not an ambiguous one.
    let occupied = call(&server, fenced_start("attempt_2", 6)).unwrap_err();
    assert_eq!(occupied.code(), Code::FailedPrecondition);
    assert_eq!(acknowledged(&server), 5);
    assert_eq!(
        disposition(&server, "attempt_2"),
        AttemptDisposition::Unknown
    );
}
