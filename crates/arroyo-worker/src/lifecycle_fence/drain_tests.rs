//! A raise of the acknowledged fence waits for the fenced requests — deletions and reservations —
//! admitted under the fence it moves past, with the lifecycle lock released (M11.T10b.02 as
//! amended by the PR #200 reviews, 2026-09-26; M11.D39d) — reached through
//! `WorkerGrpc::start_execution`, the production handler.
//!
//! A request guard stands in for a table's store request in flight: it is exactly what a table
//! holds for the length of one deletion, or of one reservation record's PUT
//! (`arroyo_state::ownership`). The handler future is polled by hand, so "still waiting" is a
//! closed-form `Pending`, never a timeout.
//!
//! The rows: a `FENCE_ONLY` and a `REVOKE` each wait for an in-flight deletion, and each for an
//! in-flight reservation, and acknowledge nothing meanwhile, while a phase read, a commit and
//! another directive are all answered — so the lock is not held — and the commit, under the fence
//! still acknowledged, linearizes before the acknowledgement; a raise waits for both kinds when
//! both are in flight; a raise cancelled mid-drain acknowledges nothing and reopens admission; two
//! raises draining at once settle in either order without lowering the fence; a second step that
//! finds the lock taken applies nothing; and a directive the identifier record refuses is refused
//! before it closes anything. The T26/T27 suites drive every other directive with no guard in
//! flight, where a raise is one step, exactly as before.

use super::tests::{
    GENERATION, WORKER, acknowledged, call, disposition, fence_only, handshaken, read, revoke,
    settlement, strict, tracked,
};
use crate::lifecycle_fence::attempt_ids::AttemptDisposition;
use crate::lifecycle_fence::guard::{StartAdmission, StartStep, WorkerLifecycle};
use crate::{EngineState, WorkerExecutionPhase, WorkerServer};
use arroyo_rpc::ControlMessage;
use arroyo_rpc::grpc::rpc::worker_grpc_server::WorkerGrpc;
use arroyo_rpc::grpc::rpc::{
    CommitReq, GetWorkerPhaseReq, OperatorCommitData, StartExecutionOutcome, StartExecutionReq,
    StartExecutionResp, TableCommitData, WorkerPhase,
};
use arroyo_server_common::shutdown::Shutdown;
use arroyo_state::ownership::{AcknowledgedFence, AdmissionRefused, FencedRequest};
use futures::FutureExt;
use std::collections::HashMap;
use std::future::Future;
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, channel};
use tonic::{Code, Request, Response, Status};

/// The fence every row starts acknowledged at.
const FLOOR: u64 = 4;

/// Longer than any answer here can take once it is due: a row that reaches it has hung.
const DUE: Duration = Duration::from_secs(60);

const KINDS: [FencedRequest; 2] = [FencedRequest::Deletion, FencedRequest::Reservation];

type Answer = Result<StartExecutionResp, Status>;

/// A handshaken generation at [`FLOOR`], running one operator behind one control channel, and
/// its tables' fence handle.
fn running_at_floor() -> (
    Shutdown,
    WorkerServer,
    AcknowledgedFence,
    Receiver<ControlMessage>,
) {
    let (shutdown, server) = handshaken(FLOOR);
    let (tx, rx) = channel(8);
    *server.state.lifecycle.lock().unwrap().execution_mut() =
        WorkerExecutionPhase::Running(EngineState {
            sources: vec![],
            sinks: vec![],
            operator_to_node: HashMap::from([("op_1".to_string(), 1u32)]),
            operator_controls: HashMap::from([(1u32, vec![tx])]),
            shutdown_guard: shutdown.guard("engine-state"),
        });
    let fence = read(&server, WorkerLifecycle::acknowledged_fence_handle);
    (shutdown, server, fence, rx)
}

/// A commit under `fence` at `epoch`, for [`running_at_floor`]'s operator.
fn commit_under(fence: u64, epoch: u64) -> CommitReq {
    CommitReq {
        epoch,
        committing_data: HashMap::from([(
            "op_1".to_string(),
            OperatorCommitData {
                committing_data: HashMap::from([(
                    "t".to_string(),
                    TableCommitData {
                        commit_data_by_subtask: HashMap::from([(0u32, vec![7])]),
                    },
                )]),
            },
        )]),
        lifecycle_fence: fence,
        target_worker_id: WORKER,
        target_worker_generation: GENERATION,
        target_worker_incarnation: super::tests::INCARNATION,
    }
}

async fn commit(server: &WorkerServer, req: CommitReq) -> Result<(), Status> {
    tokio::time::timeout(DUE, WorkerGrpc::commit(server, Request::new(req)))
        .await
        .expect("a commit with capacity answers")
        .map(|_| ())
}

/// The epochs of the commits published so far.
fn published(rx: &mut Receiver<ControlMessage>) -> Vec<u32> {
    let mut epochs = vec![];
    while let Ok(message) = rx.try_recv() {
        match message {
            ControlMessage::Commit { epoch, .. } => epochs.push(epoch),
            other => panic!("published {other:?}, which is not a commit"),
        }
    }
    epochs
}

/// One poll of a handler call, on this thread and inside the test's runtime, with no waker: the
/// row polls again itself, and [`answered`] polls with a real one.
fn poll_once(call: &mut Pin<&mut impl Future<Output = Answer>>) -> Poll<Answer> {
    call.as_mut().poll(&mut Context::from_waker(Waker::noop()))
}

/// Drives a handler call to its answer, which must be due now.
async fn answered(call: Pin<&mut impl Future<Output = Answer>>) -> Answer {
    tokio::time::timeout(DUE, call)
        .await
        .expect("the answer is due")
}

/// `start_execution`, as a future answering with the response body.
#[allow(clippy::result_large_err)]
fn start(server: &WorkerServer, req: StartExecutionReq) -> impl Future<Output = Answer> + '_ {
    WorkerGrpc::start_execution(server, Request::new(req)).map(|r| r.map(Response::into_inner))
}

/// `(acknowledged, deletions, reservations, raises pending)`.
fn gate(fence: &AcknowledgedFence) -> (u64, usize, usize, usize) {
    let status = fence.gate_status();
    (
        status.acknowledged,
        status.deletions_in_flight,
        status.reservations_in_flight,
        status.raises_pending,
    )
}

/// `n` requests of `kind` in flight, as `(deletions, reservations)`.
fn only(kind: FencedRequest, n: usize) -> (usize, usize) {
    match kind {
        FencedRequest::Deletion => (n, 0),
        FencedRequest::Reservation => (0, n),
    }
}

/// **The reviewer's row, at the worker.** A `FENCE_ONLY` and a `REVOKE` above the floor, each
/// with a deletion — and separately a reservation — in flight under the floor: the directive
/// waits, acknowledging nothing and recording nothing, and admission of both kinds is closed.
/// Meanwhile the lock is free — a phase read, a commit and a stale directive are all answered at
/// once — and the commit, under the fence still acknowledged, is published: it linearizes before
/// the acknowledgement. When the request returns, the directive acknowledges; the same commit is
/// then refused, and only the new fence admits a request of either kind.
#[tokio::test]
async fn a_raise_waits_for_an_in_flight_request_of_either_kind_with_the_lifecycle_lock_released() {
    let raises = [
        (StartExecutionOutcome::FenceAcknowledged, None),
        (StartExecutionOutcome::Revoked, Some("attempt_9")),
    ];
    for ((outcome, revoked), in_flight) in raises
        .into_iter()
        .flat_map(|raise| KINDS.map(|kind| (raise, kind)))
    {
        let label = format!("{outcome:?} over a {in_flight}");
        let directive = match revoked {
            None => fence_only(5),
            Some(id) => revoke(5, &[id]),
        };
        let (_shutdown, server, fence, mut rx) = running_at_floor();
        let requesting = fence
            .admit(FLOOR, in_flight)
            .expect("a request under the floor");

        let mut raising = pin!(start(&server, directive));
        assert!(poll_once(&mut raising).is_pending(), "{label}");
        assert!(poll_once(&mut raising).is_pending(), "{label}");
        let (deletions, reservations) = only(in_flight, 1);
        assert_eq!(
            gate(&fence),
            (FLOOR, deletions, reservations, 1),
            "{label}: closed, not published"
        );
        for request in KINDS {
            assert_eq!(
                fence.admit(FLOOR, request).unwrap_err(),
                AdmissionRefused::Rising {
                    acknowledged: FLOOR,
                    requested: FLOOR,
                    request
                },
                "{label}"
            );
        }

        assert!(
            server.state.lifecycle.try_lock().is_ok(),
            "{label}: lock free"
        );
        assert_eq!(
            (acknowledged(&server), tracked(&server)),
            (FLOOR, 0),
            "{label}"
        );
        assert_eq!(
            disposition(&server, "attempt_9"),
            AttemptDisposition::Unknown
        );
        let phase = WorkerGrpc::get_worker_phase(&server, Request::new(GetWorkerPhaseReq {}))
            .now_or_never()
            .expect("a phase read does not wait")
            .expect("answered")
            .into_inner();
        assert_eq!(phase.phase, WorkerPhase::Running as i32, "{label}");
        assert_eq!(
            call(&server, fence_only(3)).unwrap_err().code(),
            Code::FailedPrecondition,
            "{label}: another directive is decided at once"
        );
        commit(&server, commit_under(FLOOR, 11))
            .await
            .expect("a commit under the fence still acknowledged");
        assert_eq!(published(&mut rx), vec![11], "{label}");

        drop(requesting);
        assert_eq!(
            answered(raising).await.expect("acknowledged"),
            settlement(5, outcome),
            "{label}"
        );
        assert_eq!(gate(&fence), (5, 0, 0, 0), "{label}");
        for request in KINDS {
            assert_eq!(
                fence.admit(FLOOR, request).unwrap_err(),
                AdmissionRefused::Moved {
                    acknowledged: 5,
                    requested: FLOOR,
                    request
                },
                "{label}"
            );
            drop(fence.admit(5, request).expect("open under 5"));
        }
        assert_eq!(
            commit(&server, commit_under(FLOOR, 12))
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition,
            "{label}: after the acknowledgement"
        );
        assert_eq!(published(&mut rx), Vec::<u32>::new(), "{label}");
        if outcome == StartExecutionOutcome::Revoked {
            assert_eq!(
                disposition(&server, "attempt_9"),
                AttemptDisposition::Revoked
            );
        }
    }
}

/// With a deletion and a reservation both in flight, a raise acknowledges only once both have
/// returned — whichever returns first, the directive is still waiting and nothing is acknowledged.
#[tokio::test]
async fn a_raise_waits_for_both_kinds_in_flight() {
    for deletion_first in [true, false] {
        let label = format!("deletion first: {deletion_first}");
        let (_shutdown, server, fence, _rx) = running_at_floor();
        let deletion = fence
            .admit(FLOOR, FencedRequest::Deletion)
            .expect("deletion");
        let reservation = fence
            .admit(FLOOR, FencedRequest::Reservation)
            .expect("reservation");
        let mut raising = pin!(start(&server, fence_only(5)));
        assert!(poll_once(&mut raising).is_pending(), "{label}");
        assert_eq!(gate(&fence), (FLOOR, 1, 1, 1), "{label}");

        let (remaining, left) = if deletion_first {
            drop(deletion);
            (reservation, (FLOOR, 0, 1, 1))
        } else {
            drop(reservation);
            (deletion, (FLOOR, 1, 0, 1))
        };
        assert!(
            poll_once(&mut raising).is_pending(),
            "{label}: the other kind is in flight"
        );
        assert_eq!(gate(&fence), left, "{label}");
        assert_eq!(acknowledged(&server), FLOOR, "{label}");

        drop(remaining);
        assert_eq!(
            answered(raising).await.expect("acknowledged"),
            settlement(5, StartExecutionOutcome::FenceAcknowledged),
            "{label}"
        );
        assert_eq!(gate(&fence), (5, 0, 0, 0), "{label}");
    }
}

/// A raise whose handler is dropped mid-drain — the controller's deadline, a reset stream —
/// acknowledges nothing, records nothing, and reopens admission of both kinds under the floor;
/// the same directive sent again with nothing in flight is one step. For a deletion and a
/// reservation in flight alike.
#[tokio::test]
async fn a_raise_cancelled_while_it_drains_acknowledges_nothing_and_reopens_admission() {
    for in_flight in KINDS {
        let (_shutdown, server, fence, _rx) = running_at_floor();
        let requesting = fence.admit(FLOOR, in_flight).expect("in flight");
        let (deletions, reservations) = only(in_flight, 1);
        {
            let mut raising = pin!(start(&server, revoke(6, &["attempt_9"])));
            assert!(poll_once(&mut raising).is_pending());
            assert_eq!(
                gate(&fence),
                (FLOOR, deletions, reservations, 1),
                "{in_flight}"
            );
        }
        assert_eq!(
            gate(&fence),
            (FLOOR, deletions, reservations, 0),
            "{in_flight}: abandoned"
        );
        assert_eq!(
            (acknowledged(&server), tracked(&server), strict(&server)),
            (FLOOR, 0, true)
        );
        for request in KINDS {
            drop(
                fence
                    .admit(FLOOR, request)
                    .expect("open under the floor again"),
            );
        }

        drop(requesting);
        assert_eq!(
            call(&server, revoke(6, &["attempt_9"])).expect("one step now"),
            settlement(6, StartExecutionOutcome::Revoked)
        );
    }
}

/// Two raises draining at once over an in-flight reservation — a duplicate and a superseding one —
/// settle in either order: the fence ends at the higher, a lower one decided after it is refused
/// as stale and releases its close, and admission opens only under the fence finally
/// acknowledged.
#[tokio::test]
async fn two_raises_draining_at_once_settle_in_either_order_and_the_fence_never_falls() {
    for higher_first in [true, false] {
        let label = format!("higher first: {higher_first}");
        let (_shutdown, server, fence, _rx) = running_at_floor();
        let reserving = fence
            .admit(FLOOR, FencedRequest::Reservation)
            .expect("in flight");
        let mut low = pin!(start(&server, fence_only(5)));
        let mut duplicate = pin!(start(&server, fence_only(5)));
        let mut high = pin!(start(&server, fence_only(7)));
        for call in [&mut low, &mut duplicate] {
            assert!(poll_once(call).is_pending(), "{label}");
        }
        assert!(poll_once(&mut high).is_pending(), "{label}");
        assert_eq!(gate(&fence), (FLOOR, 0, 1, 3), "{label}");
        drop(reserving);

        let acknowledged_5 = settlement(5, StartExecutionOutcome::FenceAcknowledged);
        let acknowledged_7 = settlement(7, StartExecutionOutcome::FenceAcknowledged);
        if higher_first {
            assert_eq!(answered(high).await.unwrap(), acknowledged_7, "{label}");
            assert_eq!(
                gate(&fence),
                (7, 0, 0, 2),
                "{label}: the others still close it"
            );
            for call in [low, duplicate] {
                assert_eq!(
                    answered(call).await.unwrap_err().code(),
                    Code::FailedPrecondition,
                    "{label}: stale once 7 is acknowledged"
                );
            }
        } else {
            assert_eq!(answered(low).await.unwrap(), acknowledged_5, "{label}");
            assert_eq!(
                answered(duplicate).await.unwrap(),
                acknowledged_5,
                "{label}"
            );
            assert_eq!(gate(&fence), (5, 0, 0, 1), "{label}");
            for request in KINDS {
                assert!(
                    fence.admit(5, request).is_err(),
                    "{label}: 7 still closes it for a {request}"
                );
            }
            assert_eq!(answered(high).await.unwrap(), acknowledged_7, "{label}");
        }
        assert_eq!(gate(&fence), (7, 0, 0, 0), "{label}");
        for (below, request) in [FLOOR, 5].into_iter().flat_map(|b| KINDS.map(|k| (b, k))) {
            assert_eq!(
                fence.admit(below, request).unwrap_err(),
                AdmissionRefused::Moved {
                    acknowledged: 7,
                    requested: below,
                    request
                },
                "{label}"
            );
        }
        for request in KINDS {
            drop(fence.admit(7, request).expect("open under 7"));
        }
    }
}

/// The second step takes the lock with `try_lock` like the first: taken, it answers `Aborted` —
/// definitive, nothing applied — and the drained raise it drops reopens admission.
#[tokio::test]
async fn a_second_step_that_finds_the_lock_taken_applies_nothing() {
    let (_shutdown, server, fence, _rx) = running_at_floor();
    let deleting = fence
        .admit(FLOOR, FencedRequest::Deletion)
        .expect("in flight");
    let mut raising = pin!(start(&server, revoke(5, &["attempt_9"])));
    assert!(poll_once(&mut raising).is_pending());
    drop(deleting);
    let answer = {
        let _held = server.state.lifecycle.lock().unwrap();
        poll_once(&mut raising)
    };
    match answer {
        Poll::Ready(Err(refused)) => assert_eq!(refused.code(), Code::Aborted),
        other => panic!("expected the contention answer, got {other:?}"),
    }
    assert_eq!(gate(&fence), (FLOOR, 0, 0, 0));
    assert_eq!((acknowledged(&server), tracked(&server)), (FLOOR, 0));
    for request in KINDS {
        drop(fence.admit(FLOOR, request).expect("open under the floor"));
    }
}

/// A raise the identifier record would refuse — a revocation naming the execution this generation
/// applied — is refused in the first step, at once, with a reservation in flight: it closes
/// nothing and waits for nothing.
#[tokio::test]
async fn a_raise_the_record_refuses_is_refused_before_it_closes_anything() {
    let (_shutdown, server) = handshaken(FLOOR);
    assert_eq!(
        call(&server, super::tests::fenced_start("attempt_a", FLOOR)).unwrap(),
        settlement(FLOOR, StartExecutionOutcome::Applied)
    );
    let fence = read(&server, WorkerLifecycle::acknowledged_fence_handle);
    let reserving = fence
        .admit(FLOOR, FencedRequest::Reservation)
        .expect("in flight");
    assert_eq!(
        call(&server, revoke(5, &["attempt_a"])).unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert_eq!(gate(&fence), (FLOOR, 0, 1, 0), "nothing closed");
    drop(reserving);
    assert_eq!(acknowledged(&server), FLOOR);
}

impl WorkerLifecycle {
    /// [`Self::admit_start_step`] for a caller that holds no request guard, so every raise it
    /// makes drains at once: the one-step decision the T26 and T27 suites drive directly.
    ///
    /// # Errors
    ///
    /// [`Self::admit_start_step`]'s, and `Aborted` — nothing applied — if a fenced request were
    /// in flight after all.
    #[allow(clippy::result_large_err)]
    pub(crate) fn admit_start(
        &mut self,
        req: &StartExecutionReq,
    ) -> Result<StartAdmission, Status> {
        match self.admit_start_step(req, None)? {
            StartStep::Decided(admission) => Ok(admission),
            StartStep::Drain(pending) => Err(Status::aborted(format!(
                "a raise to lifecycle fence {} is waiting for in-flight fenced requests",
                pending.target()
            ))),
        }
    }
}
