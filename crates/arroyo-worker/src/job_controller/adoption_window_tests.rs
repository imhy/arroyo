//! The stale-commit window an already-running worker-leader adoption leaves open at the worker,
//! and what closes it (M11.T27a and the worker half of M11.T27b, design M11.D39d; closes review
//! finding M11.R64a).
//!
//! # What this pins
//!
//! M11.D39d exempts one takeover from the active handshake: adopting an **already-running
//! worker-leader** execution admits no generation and issues no start, so the workers it inherits
//! are never sent the adopter's fence. M11.R64a found the design saying those workers learn the
//! new fence at the first directive carrying one. For a commit they do not:
//! `WorkerLifecycle::admit_commit` takes `&self`, so nothing on the commit path can raise the
//! floor, and `FenceState::addressed_to_this_generation` refuses only a fence *below* the floor.
//! A worker that acknowledged 5 therefore still admits a commit under 5 after admitting any number
//! under 6.
//!
//! # What ends the window (owner ruling 1, 2026-09-25)
//!
//! An acknowledged `FENCE_ONLY` or `REVOKE` at or above the adopted fence, or observed termination
//! of the addressed generation — the last is the controller's observation, not worker state, and
//! is not exercised here. The floor is written only by `FenceState::acknowledge`, whose one caller
//! is `WorkerLifecycle::commit(plan)`, reached only when `plan` admitted the directive. A fenced
//! `START` is not on the list because it can never raise a floor: the `Start` arm of `plan` admits
//! one only at the exact fence already acknowledged (`acknowledged_this_fence`, PR #167 round 2),
//! and a `Running` generation refuses every start at the phase check.
//!
//! # What the rows model, and what they do not
//!
//! Every commit goes through `RunningJobModel::commit_to_workers`, the one fan-out both topologies
//! send through, into a production `WorkerServer` behind a real tonic server; every lifecycle
//! directive crosses the same connection, stamped by `StartDirective::stamp`. The rows read no
//! topology — the per-topology route is the controller's half of the supplemental D96 row (owner
//! ruling 2) — and pass under both `ARROYO__JOB_CONTROLLER` modes.
//!
//! The two authorities are the guard's view, not two landed senders. After an already-running
//! worker-leader adoption the leader keeps committing under the authority its own start conferred
//! (`RunningJobModel::commit_authority`), and the adopting controller builds no `JobController`,
//! so no landed code sends a floor-5 worker a commit under 6. "Commit 6" states what a *newer*
//! commit does to the floor — nothing — and "delayed commit 5" is, at the worker, the same
//! `CommitReq` as the leader's own live commit under 5: the worker cannot tell a delayed commit
//! from a live one, and these rows do not claim it can. Whether a superseded controller's
//! checkpoint becomes the committed root is decided by the fence/epoch-conditional row update
//! (M11.T27c), not by this guard.

use super::commit_wiring_tests::{
    GENERATION, LiveWorker, body, expected, incarnation, leader_model, live_worker, nz, published,
};
use crate::WorkerExecutionPhase;
use crate::job_controller::model::{RunningJobModel, addressed_commit};
use crate::lifecycle_fence::attempt_ids::AttemptDisposition;
use arroyo_rpc::fence_wire::{
    CommitAuthority, CommitDirective, FenceAddress, LifecycleTarget, StartDirective,
    commit_directive, observed_settlement,
};
use arroyo_rpc::grpc::rpc::{LifecycleOperation, StartExecutionOutcome, StartExecutionReq};
use arroyo_types::WorkerId;
use tonic::{Code, Request, Status};

/// The worker every row addresses.
const WORKER: u64 = 11;
/// The floor the handshake installs: the fence the leader's own start ran under, and so the fence
/// a delayed commit from before the adoption carries.
const FLOOR: u64 = 5;
/// The fence the adoption CAS installed, which the inherited worker is never sent unasked.
const ADOPTED: u64 = 6;
/// A later controller's fence, above the adopted one.
const ABOVE: u64 = 7;

/// The worker's refusal of a commit under the floor once the adopted fence is acknowledged,
/// verbatim from `FenceState::addressed_to_this_generation`.
const STALE_UNDER_ADOPTED: &str =
    "lifecycle fence 5 is older than fence 6 this worker generation has acknowledged";
/// The worker's refusal of a start under the adopted fence at floor 5, verbatim from
/// `FenceState::acknowledged_this_fence`.
const START_UNDER_ADOPTED: &str = "lifecycle fence 6 is not fence 5, the one this worker \
                                   generation acknowledged, so no handshake of that authority \
                                   authorises this request";

/// What a live worker published, in the shape `published` renders it.
type Publications = Vec<(u32, Vec<(String, Vec<(u32, Vec<u8>)>)>)>;

/// One commit's whole result: the worker's refusal (code and text) if it gave one, and everything
/// it published — both always, so a refusal is asserted together with "nothing was published".
type Committed = (Result<(), (Code, String)>, Publications);

/// What a `StartExecution` came to: the settlement's observed fence and outcome, or the refusal's
/// code and text.
type Answer = Result<(Option<u64>, StartExecutionOutcome), (Code, String)>;

/// A running worker at [`FLOOR`], and the two authorities an already-running adoption leaves able
/// to reach it: `stale`, which the leader's own start conferred, and `adopted`.
struct Window {
    worker: LiveWorker,
    stale: RunningJobModel,
    adopted: RunningJobModel,
}

/// The floor and strict mode, read from the worker's own lifecycle.
fn floor(worker: &LiveWorker) -> (u64, bool) {
    let lifecycle = worker.lifecycle.lock().unwrap();
    (lifecycle.acknowledged_fence(), lifecycle.is_strict())
}

/// What the worker's identifier record says about `id`, and how many identifiers it holds.
fn record(worker: &LiveWorker, id: &str) -> (AttemptDisposition, usize) {
    let lifecycle = worker.lifecycle.lock().unwrap();
    (lifecycle.disposition(id), lifecycle.tracked_ids())
}

/// Whether the worker is still in the `Running` phase the fixture put it in.
fn running(worker: &LiveWorker) -> bool {
    matches!(
        worker.lifecycle.lock().unwrap().execution(),
        WorkerExecutionPhase::Running(_)
    )
}

/// The leader's model under `fence`, for [`WORKER`] in [`GENERATION`] over `worker`'s client.
fn authority(worker: &LiveWorker, fence: u64) -> RunningJobModel {
    leader_model(
        CommitAuthority::under(nz(fence), nz(GENERATION)),
        WorkerId(WORKER),
        worker.client.clone(),
    )
}

/// The fence and target `model`'s fan-out addresses [`WORKER`] with, decoded as the worker's
/// `admit_commit` decodes it.
fn addressed(model: &RunningJobModel) -> (u64, LifecycleTarget) {
    let to = WorkerId(WORKER);
    let request = addressed_commit(
        model.commit_authority,
        to,
        model.workers[&to].incarnation,
        &body(0),
    );
    match commit_directive(&request).expect("a directive the worker can read") {
        CommitDirective::Fenced(address) => (address.fence(), address.target()),
        CommitDirective::Unfenced => panic!("every authority in this file is fenced"),
    }
}

/// A refusal as the rows compare it: the code and the worker's own text.
fn refusal(status: &Status) -> (Code, String) {
    (status.code(), status.message().to_string())
}

/// Commits `epoch` through the production fan-out under `model`'s authority.
async fn commit(model: &mut RunningJobModel, worker: &mut LiveWorker, epoch: u32) -> Committed {
    let result = model.commit_to_workers(&body(epoch.into())).await;
    let result =
        result.map_err(|error| refusal(error.downcast_ref().expect("the worker's status")));
    (result, published(&mut worker.control))
}

fn admitted(epoch: u32) -> Committed {
    (Ok(()), expected(epoch))
}

fn refused(message: &str) -> Committed {
    (Err((Code::FailedPrecondition, message.to_string())), vec![])
}

/// A lifecycle operation, as the controller's `StartExecution` carries one.
enum Directive<'a> {
    FenceOnly,
    Revoke(&'a str),
    Start(&'a str),
}

/// Sends `directive` under `fence` to [`WORKER`]'s generation over its own connection.
async fn send(worker: &LiveWorker, fence: u64, directive: Directive<'_>) -> Answer {
    let (operation, revoked, id) = match directive {
        Directive::FenceOnly => (LifecycleOperation::FenceOnly, vec![], ""),
        Directive::Revoke(id) => (LifecycleOperation::Revoke, vec![id.to_string()], ""),
        Directive::Start(id) => (LifecycleOperation::Start, vec![], id),
    };
    let mut request = StartExecutionReq {
        start_execution_id: id.to_string(),
        ..Default::default()
    };
    StartDirective::Fenced {
        address: FenceAddress::under(
            nz(fence),
            LifecycleTarget::in_generation(WORKER, nz(GENERATION), incarnation()),
        ),
        operation,
        revoked_execution_ids: &revoked,
    }
    .stamp(&mut request);
    let mut client = worker.client.clone();
    let answer = client.start_execution(Request::new(request)).await;
    let response = answer.map_err(|status| refusal(&status))?.into_inner();
    let settled = observed_settlement(&response).expect("a settlement this build can read");
    Ok((settled.observed_fence(), settled.outcome()))
}

/// The acknowledgement a `FENCE_ONLY` under `fence` answers with.
fn fence_acknowledged(fence: u64) -> Answer {
    Ok((Some(fence), StartExecutionOutcome::FenceAcknowledged))
}

/// Step (a): a live running worker handshaken at [`FLOOR`] — the handshake is what makes it
/// strict, which is the post-flag-day state — and the two authorities that can reach it.
async fn handshaken_at_floor() -> Window {
    let worker = live_worker(WORKER, GENERATION).await;
    assert_eq!(
        floor(&worker),
        (0, false),
        "registered, fences not required"
    );
    assert_eq!(
        send(&worker, FLOOR, Directive::FenceOnly).await,
        fence_acknowledged(FLOOR),
        "step a: the handshake"
    );
    assert_eq!(floor(&worker), (FLOOR, true), "step a: floor 5, strict");

    // The two authorities differ in the fence and in nothing else. Both name one target, and the
    // guard compares that target to its own with whole-value `LifecycleTarget` equality before it
    // reads the fence — so whatever refuses one of them below and admits the other is the fence.
    let (stale, adopted) = (authority(&worker, FLOOR), authority(&worker, ADOPTED));
    let target = LifecycleTarget::in_generation(WORKER, nz(GENERATION), incarnation());
    assert_eq!(addressed(&stale), (FLOOR, target));
    assert_eq!(addressed(&adopted), (ADOPTED, target));
    Window {
        worker,
        stale,
        adopted,
    }
}

/// Steps (a)–(c) of the named row: after the handshake, two admitted commits under the adopted
/// fence that leave the floor at 5, then an admitted commit under 5.
async fn opened_window() -> Window {
    let mut w = handshaken_at_floor().await;
    // Two, because "no newer commit ends the window" is the claim and one commit is an example.
    for epoch in [4, 5] {
        assert_eq!(
            commit(&mut w.adopted, &mut w.worker, epoch).await,
            admitted(epoch),
            "step b: a commit under the adopted fence, epoch {epoch}"
        );
        assert_eq!(
            floor(&w.worker),
            (FLOOR, true),
            "step b: `admit_commit(&self)` leaves the floor at 5 after epoch {epoch}"
        );
    }
    // A later epoch than (b)'s, and admitted anyway: `admit_commit` reads only the directive, so
    // the fence is the one thing that could refuse it — and 5 is not below the floor.
    assert_eq!(
        commit(&mut w.stale, &mut w.worker, 6).await,
        admitted(6),
        "step c: the window is open"
    );
    assert_eq!(floor(&w.worker), (FLOOR, true), "step c");
    w
}

/// The supplemental D96 check (M11.T27a): newer commits do not end the window an already-running
/// adoption leaves open; an acknowledged `FENCE_ONLY` at the adopted fence does.
///
/// In the order M11.D39d states it: (a) the handshake installs floor 5; (b) two commits under 6
/// are admitted and the floor stays 5; (c) a commit under 5 is still admitted — M11.R64a's window;
/// (d) a `FENCE_ONLY` at 6 is acknowledged and raises the floor; (e) the commit under 5 is refused
/// by the floor comparison, in the worker's own words, and nothing is published; (f) the adopted
/// authority still commits — closing the window takes nothing from the live one.
#[tokio::test]
async fn already_running_adoption_commit_does_not_advance_worker_fence() {
    let mut w = opened_window().await;

    assert_eq!(
        send(&w.worker, ADOPTED, Directive::FenceOnly).await,
        fence_acknowledged(ADOPTED),
        "step d"
    );
    assert_eq!(floor(&w.worker), (ADOPTED, true), "step d: floor 6");

    assert_eq!(
        commit(&mut w.stale, &mut w.worker, 7).await,
        refused(STALE_UNDER_ADOPTED),
        "step e: the window is closed"
    );
    assert_eq!(
        commit(&mut w.adopted, &mut w.worker, 7).await,
        admitted(7),
        "step f"
    );

    // The identity half is compared, not coincident: the adopted fence addressed to another
    // generation is refused on the target, before the fence is read — and the same fence at this
    // generation was just admitted, so what (f) passed was the address as well as the fence.
    let mut misaddressed = leader_model(
        CommitAuthority::under(nz(ADOPTED), nz(GENERATION + 1)),
        WorkerId(WORKER),
        w.worker.client.clone(),
    );
    assert_eq!(
        commit(&mut misaddressed, &mut w.worker, 8).await,
        refused(
            "request is addressed to worker 11 generation 4 incarnation 21, and this is worker 11 \
             generation 3 incarnation 21"
        )
    );
    assert_eq!(floor(&w.worker), (ADOPTED, true));
}

/// A `REVOKE` at the adopted fence closes the window as a `FENCE_ONLY` does (M11.T27b).
///
/// Both are `AdmissionPlan::Acknowledge`, and both reach `FenceState::acknowledge` through
/// `WorkerLifecycle::commit`. Only the operation differs from the named row's step (d): the
/// revocation names an identifier this generation never applied — what a discharging controller
/// revokes — and the record is asserted to hold it revoked, so the directive did what it says.
#[tokio::test]
async fn a_revoke_at_the_adopted_fence_closes_the_adoption_window() {
    let mut w = opened_window().await;
    const NEVER_APPLIED: &str = "attempt_never_applied";
    assert_eq!(
        record(&w.worker, NEVER_APPLIED),
        (AttemptDisposition::Unknown, 0)
    );

    assert_eq!(
        send(&w.worker, ADOPTED, Directive::Revoke(NEVER_APPLIED)).await,
        Ok((Some(ADOPTED), StartExecutionOutcome::Revoked))
    );
    assert_eq!(floor(&w.worker), (ADOPTED, true));
    assert_eq!(
        record(&w.worker, NEVER_APPLIED),
        (AttemptDisposition::Revoked, 1)
    );

    assert_eq!(
        commit(&mut w.stale, &mut w.worker, 7).await,
        refused(STALE_UNDER_ADOPTED)
    );
    assert_eq!(commit(&mut w.adopted, &mut w.worker, 7).await, admitted(7));
}

/// A fenced `START` does not close the window, at the adopted fence or at the floor (owner ruling
/// 1: the end condition is an acknowledged `FENCE_ONLY` or `REVOKE`, not a start).
///
/// The two refusals are the two reasons a start cannot raise a floor. Under 6, the `Start` arm of
/// `WorkerLifecycle::plan` asks `acknowledged_this_fence`, which admits only the exact fence this
/// generation acknowledged, and refuses before the phase is read. Under 5 that check passes and
/// the phase check refuses: a `Running` generation takes no start. Either way `plan` fails and
/// `commit(plan)` — the floor's only writer — is never reached, so the floor, the identifier
/// record and the phase are as they were, and the delayed commit under 5 is admitted after each.
///
/// `WorkerGrpc::start_execution` checks nothing before `admit_start` but its `try_lock`, so the
/// texts asserted are the guard's own; each start carries an identifier so the record can show it
/// was neither applied nor revoked.
#[tokio::test]
async fn a_start_at_the_adopted_fence_does_not_close_the_adoption_window() {
    let mut w = opened_window().await;

    for (fence, id, text, epoch) in [
        (ADOPTED, "start_at_adopted", START_UNDER_ADOPTED, 7),
        (FLOOR, "start_at_floor", "Worker is already running", 8),
    ] {
        assert_eq!(
            send(&w.worker, fence, Directive::Start(id)).await,
            Err((Code::FailedPrecondition, text.to_string())),
            "a start under {fence}"
        );
        assert_eq!(floor(&w.worker), (FLOOR, true), "{id}: floor unmoved");
        assert_eq!(
            record(&w.worker, id),
            (AttemptDisposition::Unknown, 0),
            "{id}: nothing applied or revoked"
        );
        assert!(running(&w.worker), "{id}: the phase did not move");
        assert_eq!(
            commit(&mut w.stale, &mut w.worker, epoch).await,
            admitted(epoch),
            "{id}: the window is still open"
        );
    }
}

/// Only a floor above the stale fence closes the window for it (the boundary).
///
/// An acknowledgement alone is not the criterion: a `FENCE_ONLY` at the floor is acknowledged and
/// moves nothing, so the commit under 5 is still admitted. One at 7, above the adopted fence, then
/// refuses 5 and 6 alike — the adopted controller's own commits included, which is what a later
/// controller's fence is for — and admits 7: three closed-form outcomes, refusals in the worker's
/// own words.
#[tokio::test]
async fn only_a_fence_above_the_stale_one_closes_the_adoption_window() {
    let mut w = opened_window().await;

    assert_eq!(
        send(&w.worker, FLOOR, Directive::FenceOnly).await,
        fence_acknowledged(FLOOR)
    );
    assert_eq!(floor(&w.worker), (FLOOR, true));
    assert_eq!(commit(&mut w.stale, &mut w.worker, 7).await, admitted(7));

    assert_eq!(
        send(&w.worker, ABOVE, Directive::FenceOnly).await,
        fence_acknowledged(ABOVE)
    );
    assert_eq!(floor(&w.worker), (ABOVE, true));
    let mut later = authority(&w.worker, ABOVE);
    assert_eq!(
        [
            commit(&mut w.stale, &mut w.worker, 8).await,
            commit(&mut w.adopted, &mut w.worker, 8).await,
            commit(&mut later, &mut w.worker, 8).await,
        ],
        [
            refused(
                "lifecycle fence 5 is older than fence 7 this worker generation has acknowledged"
            ),
            refused(
                "lifecycle fence 6 is older than fence 7 this worker generation has acknowledged"
            ),
            admitted(8),
        ]
    );
}

/// Which directive, arriving first under the adopted fence, teaches a running floor-5 generation
/// that fence — the question M11.R64a found the design answering "any of them".
///
/// The regression rows above follow the one sequence the finding named; this is the check that
/// would have found it. Only the kind of the first directive varies, each against a fresh worker
/// at step (a): the two acknowledgements raise the floor and close the window, a commit (the
/// `&self` receiver) and a start (`acknowledged_this_fence`) do neither. A change that made any
/// kind teach the fence, or stop teaching it, fails its own row.
#[tokio::test]
async fn only_an_acknowledgement_teaches_a_running_generation_the_adopted_fence() {
    /// The first thing the generation hears under the adopted fence, and what it answers.
    enum First {
        Commit,
        Lifecycle(Directive<'static>, Answer),
    }
    for (kind, first, floor_after, delayed) in [
        ("commit", First::Commit, FLOOR, admitted(5)),
        (
            "fence-only",
            First::Lifecycle(Directive::FenceOnly, fence_acknowledged(ADOPTED)),
            ADOPTED,
            refused(STALE_UNDER_ADOPTED),
        ),
        (
            "revoke",
            First::Lifecycle(
                Directive::Revoke("attempt_never_applied"),
                Ok((Some(ADOPTED), StartExecutionOutcome::Revoked)),
            ),
            ADOPTED,
            refused(STALE_UNDER_ADOPTED),
        ),
        (
            "start",
            First::Lifecycle(
                Directive::Start("start_at_adopted"),
                Err((Code::FailedPrecondition, START_UNDER_ADOPTED.to_string())),
            ),
            FLOOR,
            admitted(5),
        ),
    ] {
        let mut w = handshaken_at_floor().await;
        match first {
            First::Commit => assert_eq!(
                commit(&mut w.adopted, &mut w.worker, 4).await,
                admitted(4),
                "{kind}: its answer"
            ),
            First::Lifecycle(directive, answer) => assert_eq!(
                send(&w.worker, ADOPTED, directive).await,
                answer,
                "{kind}: its answer"
            ),
        }
        assert_eq!(floor(&w.worker), (floor_after, true), "{kind}: the floor");
        assert_eq!(
            commit(&mut w.stale, &mut w.worker, 5).await,
            delayed,
            "{kind}: the delayed commit under 5"
        );
    }
}
