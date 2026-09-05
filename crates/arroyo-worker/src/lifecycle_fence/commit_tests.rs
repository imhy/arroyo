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
use tokio::sync::mpsc::{Receiver, channel};
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
    *server.state.lifecycle.lock().unwrap().execution_mut() =
        WorkerExecutionPhase::Running(EngineState {
            sources: vec![],
            sinks: vec![],
            operator_to_node: HashMap::from([(OPERATOR.to_string(), 1u32)]),
            operator_controls: HashMap::from([(1u32, vec![tx])]),
            shutdown_guard: shutdown.guard("engine-state"),
        });
    rx
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
/// The one outcome that is not in this list is the phase refusal a commit gets when the worker
/// is not running: it is M11.T08's, unchanged, and it is reached only after the fence decision
/// has admitted the directive.
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
