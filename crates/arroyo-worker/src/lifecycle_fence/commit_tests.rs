//! What the guard answers a *commit* directive, reached through the production handler
//! (M11.T26e, design M11.D39d).
//!
//! M11.D39d carries the fence and the target worker id/generation on `StartExecutionReq` **and
//! commit directives**, and M11.D39's safety invariant names commit publication among the
//! effects a linearized refusal must not newly admit. These rows are that half: which commits
//! this worker generation publishes to its operators, which it refuses, and — the decision
//! [`WorkerLifecycle::admit_commit`](super::guard::WorkerLifecycle::admit_commit) documents —
//! what a fenced commit leaves the generation's own fence state saying afterwards.
//!
//! Every request goes through `WorkerGrpc::commit`. The guard is only worth anything if the
//! production handler is what reaches it, and the handler is also where the M11.T08 publication
//! this must not disturb lives.

use super::tests::{
    AMBIGUOUS, GENERATION, INCARNATION, SUCCESSOR_INCARNATION, WORKER, acknowledge, acknowledged,
    announced, applied, apply_registration_response, call, fence_only, fenced_start, generation,
    handshaken, register, registered, strict, unfenced,
};
use crate::{EngineState, WorkerExecutionPhase, WorkerServer};
use arroyo_rpc::ControlMessage;
use arroyo_rpc::grpc::rpc::worker_grpc_server::WorkerGrpc;
use arroyo_rpc::grpc::rpc::{CommitReq, CommitResp, OperatorCommitData, TableCommitData};
use arroyo_server_common::shutdown::Shutdown;
use prost::Message;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::mpsc::{OwnedPermit, Receiver, Sender, channel};
use tokio::time::timeout;
use tonic::{Code, Request, Status};

/// The operator this job's commits name, and the table inside it.
const OPERATOR: &str = "op_1";
const TABLE: &str = "t";

/// Puts `server` into `Running` behind one operator control channel, and hands back the
/// receiving end so a published commit can be read off it.
///
/// The phase is set through `execution_mut`, the same door `initialize_inner` uses when a start
/// finishes initializing; nothing here forges an admission.
fn running(shutdown: &Shutdown, server: &WorkerServer) -> Receiver<ControlMessage> {
    let (tx, rx) = channel(8);
    running_behind(shutdown, server, vec![tx]);
    rx
}

/// Puts `server` into `Running` behind exactly these operator control channels, all for the one
/// node [`OPERATOR`] runs on — several channels model several subtasks of that node.
fn running_behind(
    shutdown: &Shutdown,
    server: &WorkerServer,
    controls: Vec<Sender<ControlMessage>>,
) {
    *server.state.lifecycle.lock().unwrap().execution_mut() =
        WorkerExecutionPhase::Running(EngineState {
            sources: vec![],
            sinks: vec![],
            operator_to_node: HashMap::from([(OPERATOR.to_string(), 1u32)]),
            operator_controls: HashMap::from([(1u32, controls)]),
            shutdown_guard: shutdown.guard("engine-state"),
        });
}

/// An operator control channel with room for exactly one message, and that one slot already
/// taken by the test — the operator is "full" until the permit is dropped.
///
/// Holding the slot as a permit rather than as a queued message keeps the channel's contents
/// closed-form: whatever is read off it afterwards was published by the commit under test.
fn full_channel() -> (
    Sender<ControlMessage>,
    Receiver<ControlMessage>,
    OwnedPermit<ControlMessage>,
) {
    let (tx, rx) = channel(1);
    let held = tx
        .clone()
        .try_reserve_owned()
        .expect("the only slot is free");
    (tx, rx, held)
}

/// A commit handler call that is expected to be *waiting* — for operator capacity — and not
/// yet answered.
type InFlight<'a> = Pin<Box<dyn Future<Output = Result<CommitResp, Status>> + 'a>>;

/// Starts `req` through the production handler and proves it has not answered.
///
/// Fifty milliseconds is a deadline for a future that is expected to stay pending, not a race
/// against one that is expected to finish: a handler that answers inside it fails the test
/// loudly, and one that is waiting for capacity has nothing to make progress on until the test
/// gives it some.
async fn in_flight(server: &WorkerServer, req: CommitReq) -> InFlight<'_> {
    let mut call: InFlight<'_> = Box::pin(commit(server, req));
    assert!(
        timeout(Duration::from_millis(50), &mut call).await.is_err(),
        "the commit must wait for operator capacity, not answer without it"
    );
    call
}

/// What every commit in this file publishes: one operator, one table, one subtask.
fn committing_data() -> HashMap<String, OperatorCommitData> {
    HashMap::from([(
        OPERATOR.to_string(),
        OperatorCommitData {
            committing_data: HashMap::from([(
                TABLE.to_string(),
                TableCommitData {
                    commit_data_by_subtask: HashMap::from([(0u32, vec![1, 2, 3])]),
                },
            )]),
        },
    )])
}

/// What a published commit says, in an order-independent shape.
///
/// `ControlMessage` has no `PartialEq`, and its commit data is two levels of `HashMap`, so the
/// comparison is made against a sorted rendering rather than against the message: an assertion
/// that depended on hash order would be a flaky one.
type Published = (u32, Vec<(String, Vec<(u32, Vec<u8>)>)>);

/// The publication a commit at `epoch` produces.
fn published(epoch: u32) -> Published {
    (
        epoch,
        vec![(TABLE.to_string(), vec![(0u32, vec![1, 2, 3])])],
    )
}

/// A commit carrying no lifecycle fields at all — what a sender predating M11.T26c issues.
fn unfenced_commit(epoch: u64) -> CommitReq {
    CommitReq {
        epoch,
        committing_data: committing_data(),
        ..Default::default()
    }
}

/// A commit at `epoch` under `fence`, addressed to worker `to_worker` generation `to_generation`
/// running as [`INCARNATION`].
fn addressed_commit(epoch: u64, fence: u64, to_worker: u64, to_generation: u64) -> CommitReq {
    addressed_commit_to(epoch, fence, to_worker, to_generation, INCARNATION)
}

/// The same, naming the process the commit is for.
fn addressed_commit_to(
    epoch: u64,
    fence: u64,
    to_worker: u64,
    to_generation: u64,
    to_incarnation: u64,
) -> CommitReq {
    CommitReq {
        epoch,
        committing_data: committing_data(),
        lifecycle_fence: fence,
        target_worker_id: to_worker,
        target_worker_generation: to_generation,
        target_worker_incarnation: to_incarnation,
    }
}

/// A commit at `epoch` under `fence`, addressed to *this* worker generation.
fn fenced_commit(epoch: u64, fence: u64) -> CommitReq {
    addressed_commit(epoch, fence, WORKER, GENERATION)
}

/// Calls the production commit handler.
#[allow(clippy::result_large_err)]
async fn commit(server: &WorkerServer, req: CommitReq) -> Result<CommitResp, Status> {
    WorkerGrpc::commit(server, Request::new(req))
        .await
        .map(tonic::Response::into_inner)
}

/// Everything the operators have been told so far, without waiting for more.
fn drain(rx: &mut Receiver<ControlMessage>) -> Vec<Published> {
    let mut seen = vec![];
    while let Ok(message) = rx.try_recv() {
        match message {
            ControlMessage::Commit { epoch, commit_data } => {
                let mut tables: Vec<_> = commit_data
                    .into_iter()
                    .map(|(table, by_subtask)| {
                        let mut subtasks: Vec<_> = by_subtask.into_iter().collect();
                        subtasks.sort();
                        (table, subtasks)
                    })
                    .collect();
                tables.sort();
                seen.push((epoch, tables));
            }
            other => panic!("a commit published {other:?}, which is not a commit"),
        }
    }
    seen
}

// ---------------------------------------------------------------------------------------------
// The pre-flag-day route, unchanged
// ---------------------------------------------------------------------------------------------

/// Before the flag day a commit carrying no lifecycle fields is published exactly as it was
/// before those fields existed — and the request that carries it is byte-identical too.
///
/// This is the apply-side half of the M11.T26e compatibility claim, and it is measured on both
/// ends: the bytes that arrive are compared against the same message with every lifecycle field
/// stamped to its proto3 default, and the `ControlMessage` that leaves is compared against a
/// closed-form value. A fence field left set would encode its key and fail the first; a
/// publication the fence decision disturbed would fail the second.
#[tokio::test]
async fn a_legacy_commit_is_published_exactly_as_it_was_before_the_fence_existed() {
    let (shutdown, server) = registered(false);
    let mut rx = running(&shutdown, &server);

    let request = unfenced_commit(4);
    assert_eq!(
        request.encode_to_vec(),
        CommitReq {
            epoch: 4,
            committing_data: request.committing_data.clone(),
            lifecycle_fence: 0,
            target_worker_id: 0,
            target_worker_generation: 0,
            target_worker_incarnation: 0,
        }
        .encode_to_vec(),
        "a legacy commit puts no lifecycle field on the wire at all"
    );

    // And it survives the wire: what the handler decides is decided about a decoded message.
    let decoded = CommitReq::decode(&request.encode_to_vec()[..]).unwrap();
    commit(&server, decoded).await.expect("published");

    assert_eq!(drain(&mut rx), vec![published(4)]);
    assert_eq!(
        acknowledged(&server),
        0,
        "an unfenced commit acknowledges nothing"
    );
    assert!(!strict(&server), "and activates nothing");
}

/// A worker that has announced itself to nobody still publishes a fence-less commit.
///
/// The same pre-flag-day rule the start path keeps: registration gates the *fenced* protocol,
/// and refusing the legacy shape here would turn a compatible increment into a live change on
/// the path production runs.
#[tokio::test]
async fn a_legacy_commit_before_registration_is_published_unchanged() {
    let (shutdown, server) = generation(WORKER, GENERATION);
    let mut rx = running(&shutdown, &server);

    commit(&server, unfenced_commit(4))
        .await
        .expect("published");
    assert_eq!(drain(&mut rx), vec![published(4)]);
}

// ---------------------------------------------------------------------------------------------
// The five rules, each varied on its own
// ---------------------------------------------------------------------------------------------

/// A fence-less commit is refused once the generation is strict, and publishes nothing.
#[tokio::test]
async fn a_fence_less_commit_is_refused_once_this_generation_is_strict() {
    let (shutdown, server) = registered(true);
    let mut rx = running(&shutdown, &server);

    let refused = commit(&server, unfenced_commit(4)).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        drain(&mut rx),
        vec![],
        "a refused commit reaches no operator"
    );
}

/// A fenced commit before registration is refused, and publishes nothing.
#[tokio::test]
async fn a_fenced_commit_before_registration_is_refused() {
    let (shutdown, server) = generation(WORKER, GENERATION);
    let mut rx = running(&shutdown, &server);

    let refused = commit(&server, fenced_commit(4, 5)).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(drain(&mut rx), vec![]);
}

/// A fenced commit is admitted while this generation's registration answer is still in flight.
///
/// The commit half of `the_registration_request_opens_the_fenced_protocol_before_its_answer_arrives`.
/// Both directives ask `addressed_to_this_generation` the same question, so the window the
/// announcement closes is closed for both or for neither — and a commit refused in it would be a
/// two-phase commit the job cannot finish, not merely a scheduling attempt lost.
#[tokio::test]
async fn a_fenced_commit_is_admitted_while_the_registration_answer_is_in_flight() {
    let (shutdown, server, proof) = announced();
    let mut rx = running(&shutdown, &server);

    assert!(commit(&server, fenced_commit(4, 5)).await.is_ok());
    assert_eq!(
        drain(&mut rx),
        vec![published(4)],
        "the commit reaches the operators"
    );

    apply_registration_response(&server, proof, true);
    assert!(
        strict(&server),
        "and the answer that arrives afterwards is applied to the same generation"
    );
}

/// A commit addressed to another worker generation — or to another *process* of this one — is
/// refused, and the address is the discriminator.
///
/// Each part of the address is varied on its own and then together, against a control that
/// differs in none: an implementation that compared only the worker id would publish the second
/// row, one that compared only the fence would publish all of them, and one that stopped at the
/// worker and the generation would publish the incarnation rows. Those last two are the commit
/// sibling of PR #167 round 6's finding 3 — a restart reuses the worker id and the generation,
/// so a commit delayed from before one is addressed to a process that is gone.
#[tokio::test]
async fn a_commit_addressed_to_another_generation_is_refused() {
    let (shutdown, server) = registered(false);
    let mut rx = running(&shutdown, &server);

    for (label, request) in [
        (
            "a predecessor generation at this worker id",
            addressed_commit(4, 5, WORKER, GENERATION - 1),
        ),
        (
            "a successor generation at this worker id",
            addressed_commit(4, 5, WORKER, GENERATION + 1),
        ),
        (
            "this generation number at another worker id",
            addressed_commit(4, 5, WORKER + 1, GENERATION),
        ),
        (
            "another worker in another generation",
            addressed_commit(4, 5, WORKER + 1, GENERATION - 1),
        ),
        (
            "a predecessor process of this worker generation",
            addressed_commit_to(4, 5, WORKER, GENERATION, SUCCESSOR_INCARNATION),
        ),
        (
            "no process at all, from a sender predating the field",
            addressed_commit_to(4, 5, WORKER, GENERATION, 0),
        ),
    ] {
        let refused = commit(&server, request).await.unwrap_err();
        assert_eq!(refused.code(), Code::FailedPrecondition, "{label}");
        assert_eq!(drain(&mut rx), vec![], "{label}: nothing was published");
    }

    // The control: the same commit, addressed to this generation, is published.
    commit(&server, fenced_commit(4, 5))
        .await
        .expect("published");
    assert_eq!(drain(&mut rx), vec![published(4)]);
}

/// A commit under a fence older than the highest this generation has acknowledged is refused.
///
/// The floor is what the *start* path installed — this generation acknowledged fence 9 — and the
/// commit is read against it without moving it. `N-1` is refused, `N` and `N+1` are published:
/// the boundary is stated as three closed-form outcomes rather than as one.
#[tokio::test]
async fn a_commit_under_a_fence_older_than_the_acknowledged_one_is_refused() {
    let (shutdown, server) = registered(false);
    let mut rx = running(&shutdown, &server);
    // Acknowledged through the fenced start path, which is the only thing that advances it.
    assert_eq!(acknowledged(&server), 0);
    call(&server, fence_only(9)).expect("the fence is acknowledged");
    assert_eq!(acknowledged(&server), 9);

    let refused = commit(&server, fenced_commit(4, 8)).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(drain(&mut rx), vec![]);

    commit(&server, fenced_commit(4, 9))
        .await
        .expect("at the acknowledged fence");
    assert_eq!(drain(&mut rx), vec![published(4)]);
    commit(&server, fenced_commit(5, 10))
        .await
        .expect("above it");
    assert_eq!(drain(&mut rx), vec![published(5)]);
}

/// A worker generation no fence can address refuses every fenced commit.
///
/// Generation zero is the wire's sentinel for "addresses nothing", so a worker running under it
/// is one no controller can name; it refuses rather than matching a directive by accident.
#[tokio::test]
async fn a_generation_no_fence_can_address_refuses_a_fenced_commit() {
    let (shutdown, server) = generation(WORKER, 0);
    register(&server, false);
    let mut rx = running(&shutdown, &server);

    let refused = commit(&server, fenced_commit(4, 5)).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(drain(&mut rx), vec![]);

    // And it still publishes the legacy shape, which is what keeps such a worker running.
    commit(&server, unfenced_commit(4))
        .await
        .expect("published");
    assert_eq!(drain(&mut rx), vec![published(4)]);
}

/// A commit whose lifecycle fields do not describe one directive is refused before anything is
/// published.
///
/// The agreement between the fence and the address is mutated rather than either field alone:
/// half a directive is a statement the wire cannot make whole, and guessing the missing half is
/// what the seam exists to prevent.
#[tokio::test]
async fn a_commit_that_is_half_a_directive_is_refused() {
    let (shutdown, server) = registered(false);
    let mut rx = running(&shutdown, &server);

    for (label, request) in [
        (
            "a fence addressed to no generation",
            CommitReq {
                target_worker_id: 0,
                target_worker_generation: 0,
                ..fenced_commit(4, 5)
            },
        ),
        (
            "a fence carried with a worker id but no generation",
            CommitReq {
                target_worker_generation: 0,
                ..fenced_commit(4, 5)
            },
        ),
        (
            "a target addressed under no fence",
            CommitReq {
                lifecycle_fence: 0,
                ..fenced_commit(4, 5)
            },
        ),
        (
            "a worker id carried without a generation or a fence",
            CommitReq {
                lifecycle_fence: 0,
                target_worker_generation: 0,
                ..fenced_commit(4, 5)
            },
        ),
    ] {
        let refused = commit(&server, request).await.unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument, "{label}");
        assert_eq!(drain(&mut rx), vec![], "{label}: nothing was published");
    }
}

// ---------------------------------------------------------------------------------------------
// The decision: a commit is a guard and never an instruction
// ---------------------------------------------------------------------------------------------

/// A fenced commit does not advance the acknowledged fence, and does not activate strict mode.
///
/// The design decision `admit_commit` records, asserted from the outside. `CommitResp` carries
/// no observed fence, so a fence advanced here would be a state change no controller could read
/// back — and M11.D39e(v) makes an acknowledgement the controller *reads* one of only three
/// things that can settle an issued attempt. The consequences are what this measures rather than
/// the field: after publishing a commit under fence 9,
///
///  * a start under the older fence 5 is still admitted, so the floor did not rise; and
///  * a fence-less start is still admitted, so the flag-day switch was not flipped.
///
/// Both would fail if a commit acknowledged what it carries, and both are exactly the wedge a
/// delayed duplicate of a superseded controller's commit would otherwise open.
#[tokio::test]
async fn a_fenced_commit_neither_advances_the_fence_nor_activates_strict_mode() {
    let (shutdown, server) = registered(false);
    let mut rx = running(&shutdown, &server);
    assert_eq!(acknowledged(&server), 0);
    assert!(!strict(&server));

    commit(&server, fenced_commit(4, 9))
        .await
        .expect("published");
    assert_eq!(drain(&mut rx), vec![published(4)]);
    assert_eq!(
        acknowledged(&server),
        0,
        "a commit acknowledges no fence: `CommitResp` has nowhere to report one"
    );
    assert!(
        !strict(&server),
        "and therefore does not activate strict mode for this generation"
    );

    // The floor did not rise: a start under a fence *below* the one the commit carried is still
    // admitted, which is only true because the commit acknowledged nothing. The handshake is at
    // 5 rather than at 9 for exactly that reason — a commit at 9 leaves this generation
    // acknowledging 5, so a start at 5 is the one its controller may still send.
    let (_idle_shutdown, idle_server) = handshaken(5);
    assert_eq!(
        call(&idle_server, fenced_start("attempt_1", 5))
            .expect("admitted")
            .observed_lifecycle_fence,
        5
    );

    // And the flag-day switch was not flipped: a fence-less start is still the pre-flag-day
    // route on a generation that has only ever seen a fenced commit.
    let (_legacy_shutdown, legacy_server) = registered(false);
    let mut legacy_rx = running(&_legacy_shutdown, &legacy_server);
    commit(&legacy_server, fenced_commit(4, 9))
        .await
        .expect("published");
    assert_eq!(drain(&mut legacy_rx), vec![published(4)]);
    *legacy_server
        .state
        .lifecycle
        .lock()
        .unwrap()
        .execution_mut() = WorkerExecutionPhase::Idle;
    call(&legacy_server, unfenced("attempt_1")).expect("still the pre-flag-day route");
    assert_eq!(applied(&legacy_server), Some("attempt_1".to_string()));
}

// ---------------------------------------------------------------------------------------------
// Publication is under the guard (PR #167 round 9)
// ---------------------------------------------------------------------------------------------

/// D96 row 19, the commit half — a commit that is waiting for operator capacity when a newer
/// fence is acknowledged is refused, and no operator ever hears it.
///
/// The interleaving the review found (PR #167 round 9): an old-fence commit passes the fence
/// decision, its operator's control channel is full, and while it waits for capacity a
/// `FENCE_ONLY` at a higher fence is acknowledged and the replacement controller publishes
/// `Refused`; the operator then frees a slot and the stale commit is enqueued after the
/// refusal. There are exactly two reachable orders now and a closed-form outcome for each,
/// like `fence_ack_serializes_with_start_application` for the start: the commit is decided
/// after it holds the capacity to act, so the acknowledgement lands entirely before that
/// decision or entirely after the publication.
#[tokio::test]
async fn a_commit_waiting_for_operator_capacity_is_refused_by_a_fence_acknowledged_meanwhile() {
    // Order B — the acknowledgement linearizes first. The commit is at this generation's own
    // fence, 5, and would be admitted on the spot if the operator had room.
    let (shutdown, server) = handshaken(5);
    let (_tx, mut rx, held) = full_channel();
    running_behind(&shutdown, &server, vec![_tx.clone()]);
    let call = in_flight(&server, fenced_commit(4, 5)).await;

    // The acknowledgement does not wait for the operator: the guard is free while the commit
    // waits for capacity, so the replacement controller's handshake is answered at once.
    acknowledge(&server, 9);
    assert_eq!(
        drain(&mut rx),
        vec![],
        "nothing was published before the acknowledgement"
    );

    // The operator frees its slot. The commit now gets its decision, and the decision is the
    // one the fence at 9 gives a commit at 5.
    drop(held);
    let refused = call.await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        refused.message(),
        "lifecycle fence 5 is older than fence 9 this worker generation has acknowledged"
    );
    assert_eq!(
        drain(&mut rx),
        vec![],
        "a commit refused after the acknowledgement reaches no operator"
    );
    assert_eq!(acknowledged(&server), 9);

    // Order A — the same commit, the same full operator, no acknowledgement in between: the
    // wait is for capacity only, and the commit is published once it has it.
    let (shutdown_a, server_a) = handshaken(5);
    let (tx_a, mut rx_a, held_a) = full_channel();
    running_behind(&shutdown_a, &server_a, vec![tx_a]);
    let call_a = in_flight(&server_a, fenced_commit(4, 5)).await;
    drop(held_a);
    call_a.await.expect("published once the operator has room");
    assert_eq!(drain(&mut rx_a), vec![published(4)]);
    assert_eq!(
        acknowledged(&server_a),
        5,
        "a commit still acknowledges no fence"
    );
}

/// A commit reaches every operator or none: an acknowledgement cannot land between one
/// subtask's copy and another's.
///
/// The review's second interleaving — with several senders, one commit partially published on
/// opposite sides of the acknowledgement. The node has two subtasks; the first has room and the
/// second is full. Under the old handler the first subtask's copy was enqueued before the wait
/// on the second, and the acknowledgement then split the commit in two. Both halves are varied:
/// which subtask is full, and whether an acknowledgement lands during the wait.
#[tokio::test]
async fn a_commit_is_published_to_every_operator_or_to_none() {
    for full in [0usize, 1] {
        for acknowledged_meanwhile in [false, true] {
            let (shutdown, server) = handshaken(5);
            let (tx_full, mut rx_full, held) = full_channel();
            let (tx_free, mut rx_free) = channel(8);
            let controls = if full == 0 {
                vec![tx_full, tx_free]
            } else {
                vec![tx_free, tx_full]
            };
            running_behind(&shutdown, &server, controls);
            let call = in_flight(&server, fenced_commit(4, 5)).await;
            assert_eq!(
                (drain(&mut rx_free), drain(&mut rx_full)),
                (vec![], vec![]),
                "subtask {full} full: no subtask hears a commit that is still waiting"
            );

            if acknowledged_meanwhile {
                acknowledge(&server, 9);
            }
            drop(held);
            let outcome = call.await;

            if acknowledged_meanwhile {
                assert_eq!(outcome.unwrap_err().code(), Code::FailedPrecondition);
                assert_eq!(
                    (drain(&mut rx_free), drain(&mut rx_full)),
                    (vec![], vec![]),
                    "subtask {full} full, acknowledged meanwhile: neither subtask hears it"
                );
            } else {
                outcome.expect("published");
                assert_eq!(
                    (drain(&mut rx_free), drain(&mut rx_full)),
                    (vec![published(4)], vec![published(4)]),
                    "subtask {full} full, no acknowledgement: both subtasks hear it, once"
                );
            }
        }
    }
}

/// A commit is published into the execution that reserved its capacity, or into none.
///
/// The reservation is capacity on the channels the phase had when the commit arrived; the guard
/// does not trust that the phase still has them. Three things can change while the commit waits,
/// each varied on its own: the phase can lose its execution, the execution can be replaced by
/// another with channels of its own, and a channel's receiver can go away. The commit is the
/// pre-flag-day one, deliberately — it carries no fence for the guard to refuse it by, so the
/// only thing standing between it and a stranger's operators is this check.
#[tokio::test]
async fn a_commit_is_published_into_the_execution_that_reserved_it_or_into_none() {
    // The phase lost its execution while the commit waited.
    let (shutdown, server) = registered(false);
    let (tx, mut rx, held) = full_channel();
    running_behind(&shutdown, &server, vec![tx]);
    let call = in_flight(&server, unfenced_commit(4)).await;
    *server.state.lifecycle.lock().unwrap().execution_mut() = WorkerExecutionPhase::Idle;
    drop(held);
    let refused = call.await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(refused.message(), "Worker not in running phase");
    assert_eq!(
        drain(&mut rx),
        vec![],
        "the execution that is gone hears nothing"
    );

    // The execution was replaced by another, with room to spare, while the commit waited.
    let (shutdown, server) = registered(false);
    let (tx_old, mut rx_old, held) = full_channel();
    running_behind(&shutdown, &server, vec![tx_old]);
    let call = in_flight(&server, unfenced_commit(4)).await;
    let mut rx_new = running(&shutdown, &server);
    drop(held);
    let refused = call.await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        refused.message(),
        "operator op_1's control channels changed while this commit waited for capacity"
    );
    assert_eq!(
        (drain(&mut rx_old), drain(&mut rx_new)),
        (vec![], vec![]),
        "neither the execution that reserved it nor the one that replaced it hears it"
    );

    // The execution grew a subtask while the commit waited: the same node, one more channel.
    let (shutdown, server) = registered(false);
    let (tx_old, mut rx_old, held) = full_channel();
    running_behind(&shutdown, &server, vec![tx_old.clone()]);
    let call = in_flight(&server, unfenced_commit(4)).await;
    let (tx_extra, mut rx_extra) = channel(8);
    running_behind(&shutdown, &server, vec![tx_old, tx_extra]);
    drop(held);
    assert_eq!(call.await.unwrap_err().code(), Code::FailedPrecondition);
    assert_eq!(
        (drain(&mut rx_old), drain(&mut rx_extra)),
        (vec![], vec![]),
        "a commit reserved for one subtask is not published to two"
    );

    // A channel's receiver went away before the commit could hold a slot on it. The old handler
    // panicked here, after the lock was released, and answered nothing; the new one decides and
    // publishes under the lock, so what it must not do is carry that panic in there.
    let (shutdown, server) = registered(false);
    let (tx, rx) = channel(8);
    running_behind(&shutdown, &server, vec![tx]);
    drop(rx);
    let refused = commit(&server, unfenced_commit(4)).await.unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        refused.message(),
        "an operator op_1 control channel closed while this commit waited for capacity"
    );
    assert!(
        !server.state.lifecycle.is_poisoned(),
        "a closed operator is a refusal, not a poisoned guard"
    );

    // And the fence decision still comes first, in every one of those: a commit the guard
    // refuses is refused by the guard, whatever became of the operators meanwhile.
    let (shutdown, server) = handshaken(9);
    let (tx, rx) = channel(8);
    running_behind(&shutdown, &server, vec![tx]);
    drop(rx);
    let refused = commit(&server, fenced_commit(4, 5)).await.unwrap_err();
    assert_eq!(
        refused.message(),
        "lifecycle fence 5 is older than fence 9 this worker generation has acknowledged",
        "the fence refusal, not the closed channel, is what a stale commit is told"
    );
}

/// A commit whose reservation could never complete is refused, not waited for.
///
/// Chained operators share a node's subtask channels, so a commit naming two operators on one
/// node needs two slots at once on each of them. Where the channel holds two, the commit is
/// published to both, as one step; where it holds one, the old shape of the wait would hold the
/// only slot and wait for a second that no operator can free — the slot it holds is not a
/// message — so the guard refuses it instead, and holds nothing while it does. Capacity is the
/// dimension varied; the request is the same.
/// A second operator chained onto [`OPERATOR`]'s node, so that one commit needs two slots on
/// each of that node's channels.
const SECOND_OPERATOR: &str = "op_2";

/// A pre-flag-day commit at epoch 4 naming both [`OPERATOR`] and [`SECOND_OPERATOR`].
fn two_operator_commit() -> CommitReq {
    let mut data = committing_data();
    data.insert(
        SECOND_OPERATOR.to_string(),
        committing_data().remove(OPERATOR).unwrap(),
    );
    CommitReq {
        epoch: 4,
        committing_data: data,
        ..Default::default()
    }
}

/// Puts `server` into `Running` with both operators chained on node 1, behind the one channel.
fn running_both_on_one_node(
    server: &WorkerServer,
    shutdown: &Shutdown,
    tx: Sender<ControlMessage>,
) {
    *server.state.lifecycle.lock().unwrap().execution_mut() =
        WorkerExecutionPhase::Running(EngineState {
            sources: vec![],
            sinks: vec![],
            operator_to_node: HashMap::from([
                (OPERATOR.to_string(), 1u32),
                (SECOND_OPERATOR.to_string(), 1u32),
            ]),
            operator_controls: HashMap::from([(1u32, vec![tx])]),
            shutdown_guard: shutdown.guard("engine-state"),
        });
}

#[tokio::test]
async fn a_commit_that_could_never_hold_its_slots_at_once_is_refused_not_waited_for() {
    // Room for both: published to both, once each, in one step.
    let (shutdown, server) = registered(false);
    let (tx, mut rx) = channel(2);
    running_both_on_one_node(&server, &shutdown, tx);
    timeout(
        Duration::from_secs(1),
        commit(&server, two_operator_commit()),
    )
    .await
    .expect("a reservation the channel can hold completes")
    .expect("published");
    assert_eq!(drain(&mut rx), vec![published(4), published(4)]);

    // Room for one: refused at once, nothing held, nothing published.
    let (shutdown, server) = registered(false);
    let (tx, mut rx) = channel(1);
    running_both_on_one_node(&server, &shutdown, tx.clone());
    let refused = timeout(
        Duration::from_secs(1),
        commit(&server, two_operator_commit()),
    )
    .await
    .expect("a reservation that can never complete is refused, not waited for")
    .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        refused.message(),
        "publishing this commit would need 2 slots at once on an operator control channel that holds 1"
    );
    assert_eq!(drain(&mut rx), vec![]);
    assert_eq!(
        tx.capacity(),
        1,
        "a refused reservation holds no slot on the channel it could not fill"
    );
}

/// Concurrent duplicates of a commit all publish, and no partial reservation keeps the channel.
///
/// The review's round-10 interleaving: a channel with two slots, a commit needing both, and four
/// copies of it in flight while the channel is full. A reservation taken slot by slot and kept
/// while the next is awaited lets two copies take one slot each and wait on the other for ever —
/// held permits are not messages, so the operator draining the channel frees nothing. Behind the
/// gate one copy reserves at a time, so every copy is answered: four copies, each published to
/// both operators, eight messages through a two-slot channel that an operator drains meanwhile,
/// and afterwards the channel is entirely free. Duplication is inside M11.D39g's fault model, so
/// four is a small instance of a reachable state, not a stress test.
#[tokio::test]
async fn concurrent_duplicate_commits_all_publish_and_none_keeps_a_partial_reservation() {
    let (shutdown, server) = registered(false);
    let (tx, mut rx) = channel(2);
    running_both_on_one_node(&server, &shutdown, tx.clone());
    let held = [
        tx.clone().try_reserve_owned().expect("slot one is free"),
        tx.clone().try_reserve_owned().expect("slot two is free"),
    ];

    // The operator: drains whatever is published, and reports how many commits it heard.
    let operator = tokio::spawn(async move {
        let mut heard = 0usize;
        while heard < 8 {
            match rx.recv().await {
                Some(ControlMessage::Commit { epoch: 4, .. }) => heard += 1,
                other => panic!("the operator heard {other:?}"),
            }
        }
        heard
    });

    let copies = async {
        tokio::join!(
            commit(&server, two_operator_commit()),
            commit(&server, two_operator_commit()),
            commit(&server, two_operator_commit()),
            commit(&server, two_operator_commit()),
        )
    };
    // The four are in flight against a full channel before the slots are released, which is the
    // state the gate exists for; `join!` polls them all before the release below is reached.
    tokio::pin!(copies);
    assert!(
        timeout(Duration::from_millis(50), &mut copies)
            .await
            .is_err(),
        "four commits against a full channel are all waiting"
    );
    drop(held);

    let outcomes = timeout(Duration::from_secs(5), &mut copies)
        .await
        .expect("every copy is answered once the operator drains — none waits for ever");
    let (a, b, c, d) = outcomes;
    for outcome in [a, b, c, d] {
        outcome.expect("each copy is published, whole");
    }
    assert_eq!(
        timeout(Duration::from_secs(5), operator)
            .await
            .expect("the operator hears all eight")
            .unwrap(),
        8
    );
    assert_eq!(
        tx.capacity(),
        2,
        "no copy keeps a slot after it is answered"
    );
}

// ---------------------------------------------------------------------------------------------
// The enumeration
// ---------------------------------------------------------------------------------------------

/// Every refusal the commit path gives is definitive, and the list is exhaustive over the
/// decision `admit_commit` takes.
///
/// The sibling of `every_refusal_this_worker_gives_is_definitive` for the other directive. The
/// controller reads `FailedPrecondition` and `InvalidArgument` as settlement
/// (`transport_settlement`), so a refusal here ends the sender's attempt rather than being
/// re-offered against a generation that has already answered.
///
/// The phase refusal a commit gets when the worker is not running is M11.T08's, unchanged, and
/// is reached only after the fence decision has admitted the directive; the three refusals below
/// it are the handoff's (PR #167 round 9), reached in the same place, and listed because they
/// are new answers this worker can give.
#[tokio::test]
async fn every_commit_refusal_this_worker_gives_is_definitive() {
    let mut codes: Vec<(&str, Code)> = vec![];

    {
        let (shutdown, server) = generation(WORKER, GENERATION);
        let _rx = running(&shutdown, &server);
        codes.push((
            "a fenced commit before registration begins",
            commit(&server, fenced_commit(4, 5))
                .await
                .unwrap_err()
                .code(),
        ));
    }
    {
        let (shutdown, server) = registered(true);
        let _rx = running(&shutdown, &server);
        codes.push((
            "fence-less under strict mode",
            commit(&server, unfenced_commit(4))
                .await
                .unwrap_err()
                .code(),
        ));
    }
    {
        let (shutdown, server) = generation(WORKER, 0);
        register(&server, false);
        let _rx = running(&shutdown, &server);
        codes.push((
            "a generation no fence can address",
            commit(&server, addressed_commit(4, 5, WORKER, GENERATION))
                .await
                .unwrap_err()
                .code(),
        ));
    }
    {
        let (shutdown, server) = registered(false);
        let _rx = running(&shutdown, &server);
        codes.push((
            "addressed to another generation",
            commit(&server, addressed_commit(4, 5, WORKER, GENERATION - 1))
                .await
                .unwrap_err()
                .code(),
        ));
    }
    {
        let (shutdown, server) = registered(false);
        let _rx = running(&shutdown, &server);
        call(&server, fence_only(9)).unwrap();
        codes.push((
            "a fence older than the acknowledged one",
            commit(&server, fenced_commit(4, 8))
                .await
                .unwrap_err()
                .code(),
        ));
    }
    {
        let (shutdown, server) = registered(false);
        let _rx = running(&shutdown, &server);
        codes.push((
            "lifecycle fields that are half a directive",
            commit(
                &server,
                CommitReq {
                    target_worker_generation: 0,
                    ..fenced_commit(4, 5)
                },
            )
            .await
            .unwrap_err()
            .code(),
        ));
    }
    {
        let (shutdown, server) = registered(false);
        let (tx, rx) = channel(8);
        running_behind(&shutdown, &server, vec![tx]);
        drop(rx);
        codes.push((
            "an operator channel closed while waiting for capacity",
            commit(&server, unfenced_commit(4))
                .await
                .unwrap_err()
                .code(),
        ));
    }
    {
        let (shutdown, server) = registered(false);
        let (tx, _rx, held) = full_channel();
        running_behind(&shutdown, &server, vec![tx]);
        let call = in_flight(&server, unfenced_commit(4)).await;
        let _replacement_rx = running(&shutdown, &server);
        drop(held);
        codes.push((
            "the execution changed while waiting for capacity",
            call.await.unwrap_err().code(),
        ));
    }
    {
        let (shutdown, server) = registered(false);
        let (tx, _rx) = channel(1);
        running_behind(&shutdown, &server, vec![tx.clone(), tx]);
        codes.push((
            "a reservation that could never be held at once",
            commit(&server, unfenced_commit(4))
                .await
                .unwrap_err()
                .code(),
        ));
    }

    assert_eq!(
        codes,
        vec![
            (
                "a fenced commit before registration begins",
                Code::FailedPrecondition
            ),
            ("fence-less under strict mode", Code::FailedPrecondition),
            (
                "a generation no fence can address",
                Code::FailedPrecondition
            ),
            ("addressed to another generation", Code::FailedPrecondition),
            (
                "a fence older than the acknowledged one",
                Code::FailedPrecondition
            ),
            (
                "lifecycle fields that are half a directive",
                Code::InvalidArgument
            ),
            (
                "an operator channel closed while waiting for capacity",
                Code::FailedPrecondition
            ),
            (
                "the execution changed while waiting for capacity",
                Code::FailedPrecondition
            ),
            (
                "a reservation that could never be held at once",
                Code::FailedPrecondition
            ),
        ]
    );
    for (label, code) in &codes {
        assert!(!AMBIGUOUS.contains(code), "{label} answered with {code:?}");
    }
}

/// The commit path and the start path ask the same question of the same state.
///
/// Both go through `FenceState::addressed_to_this_generation`, so a directive one accepts is one
/// the other accepts and a directive one refuses is one the other refuses. Asserting the pairs
/// is what would catch a second copy of the rule appearing beside the first: under
/// `LegacyT08` a duplicate is usually the *same* answer, and only diverges later.
#[tokio::test]
async fn a_commit_and_a_start_agree_about_which_directives_this_generation_answers_for() {
    for (label, fence, to_worker, to_generation, admitted) in [
        (
            "addressed here, at a live fence",
            5u64,
            WORKER,
            GENERATION,
            true,
        ),
        ("a predecessor generation", 5, WORKER, GENERATION - 1, false),
        ("another worker id", 5, WORKER + 1, GENERATION, false),
    ] {
        let (shutdown, start_server) = registered(false);
        // The start path additionally requires the handshake that authorises a start at all, so
        // it is performed here and the rows below vary only the addressing — which is the
        // question these pairs are about. It is addressed to *this* generation whatever the row
        // addresses its directive to, so a misaddressed row is still refused for being
        // misaddressed.
        acknowledge(&start_server, fence);
        let start = call(
            &start_server,
            super::tests::addressed_start("attempt_1", fence, to_worker, to_generation),
        );
        drop(shutdown);

        let (commit_shutdown, commit_server) = registered(false);
        let mut rx = running(&commit_shutdown, &commit_server);
        let published_commit = commit(
            &commit_server,
            addressed_commit(4, fence, to_worker, to_generation),
        )
        .await;

        assert_eq!(start.is_ok(), admitted, "{label}: the start");
        assert_eq!(published_commit.is_ok(), admitted, "{label}: the commit");
        if !admitted {
            assert_eq!(
                start.unwrap_err().code(),
                published_commit.unwrap_err().code(),
                "{label}: and they refuse it with the same code"
            );
            assert_eq!(drain(&mut rx), vec![]);
        }
    }
}

// ---------------------------------------------------------------------------------------------
// What an admitted start hands its initialization
// ---------------------------------------------------------------------------------------------

/// The authority a start confers on the execution it admits is the address it was admitted
/// under — not what this generation has acknowledged since.
///
/// This is the value `WorkerGrpc::start_execution` hands `WorkerState::initialize`, and the only
/// thing a worker leader's commits are addressed with. Two properties are asserted, and the
/// second is the one a plausible-looking implementation gets wrong: the fence a leader commits
/// under is the fence *its own start* carried, and it does not follow the generation's highest
/// acknowledged fence upwards. A replacement controller's handshake raises that number, and a
/// leader that committed under it would be committing on an authority it was never given.
#[test]
fn the_authority_a_start_confers_is_the_address_it_was_admitted_under() {
    use crate::lifecycle_fence::guard::{StartAdmission, WorkerLifecycle};
    use arroyo_rpc::fence_wire::{
        CommitAuthority, CommitDirective, FenceAddress, LifecycleTarget, WorkerIncarnation,
    };
    use std::num::NonZeroU64;

    let nz = |v: u64| NonZeroU64::new(v).unwrap();
    let conferred = |req: arroyo_rpc::grpc::rpc::StartExecutionReq| {
        let mut lifecycle = WorkerLifecycle::idle(
            WORKER,
            GENERATION,
            WorkerIncarnation::named(INCARNATION).unwrap(),
        );
        let announced = lifecycle.announce();
        lifecycle.registered(announced, false);
        // The handshake at the start's own fence: what a controller holds before it may address
        // one at all — `guard_tests::a_start_is_admitted_only_under_a_fence_this_generation_acknowledged`.
        if req.lifecycle_fence != 0 {
            assert!(matches!(
                lifecycle
                    .admit_start(&fence_only(req.lifecycle_fence))
                    .expect("the handshake is acknowledged"),
                StartAdmission::Settled(_)
            ));
        }
        match lifecycle.admit_start(&req).expect("admitted") {
            StartAdmission::Apply(applied) => {
                let mut seen = None;
                applied.start(|authority| seen = Some(authority));
                (
                    seen.expect("the initialization is handed the authority"),
                    lifecycle,
                )
            }
            StartAdmission::Settled(_) => panic!("this fixture admits a start"),
        }
    };

    // A fenced start confers its own fence, addressed to its own generation.
    let (authority, mut lifecycle) = conferred(fenced_start("attempt_1", 5));
    assert_eq!(authority, CommitAuthority::under(nz(5), nz(GENERATION)));
    assert_eq!(
        authority.directive(WORKER + 1, WorkerIncarnation::named(INCARNATION)),
        CommitDirective::Fenced(FenceAddress::under(
            nz(5),
            LifecycleTarget::in_generation(
                WORKER + 1,
                nz(GENERATION),
                WorkerIncarnation::named(INCARNATION)
            )
        )),
        "and every other worker of that generation is addressed under the same fence"
    );

    // A replacement controller advances this generation past it. The authority already conferred
    // is unchanged, which is what stops the leader committing under a fence it was never given.
    let mut advance = super::tests::fence_only(9);
    advance.start_execution_id = String::new();
    let acknowledged = lifecycle.admit_start(&advance).expect("acknowledged");
    assert!(matches!(acknowledged, StartAdmission::Settled(_)));
    assert_eq!(lifecycle.acknowledged_fence(), 9);
    assert_eq!(authority, CommitAuthority::under(nz(5), nz(GENERATION)));

    // And the pre-flag-day start confers the pre-flag-day authority.
    let (legacy, _) = conferred(unfenced("attempt_1"));
    assert_eq!(legacy, CommitAuthority::unfenced());
    assert_eq!(
        legacy.directive(WORKER, WorkerIncarnation::named(INCARNATION)),
        CommitDirective::Unfenced
    );
}
