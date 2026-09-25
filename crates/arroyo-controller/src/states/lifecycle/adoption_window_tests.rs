//! The controller half of supplemental D96 row 38, and M11.T27b/M11.T27c on the route that row is
//! about (design M11.D39d, plan M11.P49g/M11.P64b; the controller side of review finding
//! M11.R64a).
//!
//! # Which topology opens the window
//!
//! `StateMachine::start` recovers a `Running` row into `LeaderRunning` when
//! `config().job_controller` is `JobControllerMode::Worker`, and into `Compiling` otherwise. The
//! leader arm is the one production caller of `adopt_before_administering`: the adoption CAS, then
//! a discharge under [`DischargeReason::AdoptingTheGenerationItNames`], which re-opens no
//! acknowledged target and so sends the generation it inherits nothing. Controller mode's
//! `Scheduling` preamble adopts and then discharges under
//! [`DischargeReason::SupersedingTheGenerationsItNames`], which re-opens every acknowledged target
//! and advances the adopted fence at it. So the window `arroyo-worker`'s
//! `job_controller::adoption_window_tests` pins at the worker — floor 5, a delayed commit under 5
//! still admitted after the adoption at 6 — is opened only by a worker-leader controller.
//!
//! # What ends it, and what it never reaches
//!
//! Owner ruling 1: an acknowledged `FENCE_ONLY` or `REVOKE` at or above the adopted fence (the
//! worker file), or observed termination of the addressed generation (the second row here). And
//! the window is one of worker *admission*, not of root *authority* (plan risk M11.T27j): the
//! adoption replaced the `(lifecycle_fence, controller_epoch)` pair the `job_statuses` update is
//! conditional on, so the superseded controller cannot root its candidate (the third row).
//!
//! Every row runs the migrated SQLite schema through the production conditional queries and a
//! `StateMachine::new`-built machine; the scheduler (`TestScheduler`) and the recording worker
//! (`FenceWorker`) are the recovery suite's, which M11.P64b allows because neither is under test.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arroyo_rpc::config::JobControllerMode;
use arroyo_rpc::fencing::FenceTargetState;
use arroyo_server_common::shutdown::{Shutdown, SignalBehavior};
use cornucopia_async::DatabaseSource;
use cornucopia_async::rusqlite::Connection;

use super::LifecycleMode;
use super::faults_tests::process_topology;
use super::fence::metrics::{self, FencingError};
use super::fence::{AuthorityOutcome, LifecycleAuthority};
use super::fence_tests::{
    JOB, adopt, cold_status, function_body, migrated_job, migrated_job_named, polled,
    production_call_sites, stored_authority,
};
use super::recovery::{Discharge, DischargeReason, discharge_recorded_obligation};
use super::recovery_tests::{
    ATTEMPT, Answers, GENERATION, INCARNATION, Lists, TestScheduler, WORKER, obligation, recorded,
    seed_obligation, seed_previous_authority, serve,
};
use super::root::RootCandidate;
use super::root_tests::{CandidateStore, GENERATION as ROOT_GENERATION, candidate_for};
use crate::JobStatus;
use crate::schedulers::Scheduler;
use crate::states::StateMachine;
use crate::states::scheduling::fanout::Accounting;

/// The fence the previous controller held and the old generation acknowledged (the worker file's
/// floor).
const FLOOR: u64 = 5;

/// The fence the already-running adoption installs (the worker file's adopted fence).
const ADOPTED: u64 = 6;

/// A controller that has just read `database`, its state machine built over `scheduler`.
///
/// `fence_tests`' cold controller with the scheduler as a parameter. `StateMachine::new` runs
/// `start`, which publishes the state the recovered row maps to under the authority it read. The
/// fixture's program (`x''`) does not load, so `start` adopts nothing and starts no task; a row
/// that wants the leader route calls `adopt_before_administering` itself, as row 15 does.
async fn cold_controller_over(
    database: &DatabaseSource,
    scheduler: Arc<dyn Scheduler>,
    shutdown: &Shutdown,
) -> StateMachine {
    let (polled, status) = polled(database).await;
    StateMachine::new(
        polled,
        status,
        database.clone(),
        scheduler,
        Arc::new("cluster".to_string()),
        shutdown.guard("state-machine"),
        Arc::new(tokio::sync::RwLock::new(HashMap::new())),
    )
    .await
}

/// The state and fence `job_id`'s row holds.
fn stored_row(job_id: &str, connection: &Mutex<Connection>) -> (String, u64) {
    connection
        .lock()
        .unwrap()
        .query_row(
            "SELECT state, lifecycle_fence FROM job_statuses WHERE id = ?1",
            [job_id],
            |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u64)),
        )
        .expect("the job's row must be readable")
}

/// The object key the job's row names as its root, if it names one.
async fn rooted(database: &DatabaseSource) -> Option<String> {
    let (_, status) = polled(database).await;
    status.state_context.metadata_root.map(|root| root.object())
}

/// How many times `needle` occurs in `source`.
fn count(source: &str, needle: &str) -> usize {
    source.matches(needle).count()
}

/// The topology this process is in, after checking it is the one `ARROYO__JOB_CONTROLLER` set.
///
/// Row 24's step 1, repeated rather than relied on: this row's two cells are two processes, and a
/// knob that stopped resolving the variable would make them one process under two labels.
fn declared_topology() -> JobControllerMode {
    let declared = std::env::var("ARROYO__JOB_CONTROLLER");
    let expected = match declared.as_deref() {
        Ok("worker") => JobControllerMode::Worker,
        Ok("controller") | Err(_) => JobControllerMode::Controller,
        Ok(other) => panic!("ARROYO__JOB_CONTROLLER={other:?} is not a topology this build has"),
    };
    let topology = process_topology();
    assert_eq!(
        format!("{topology:?}"),
        format!("{expected:?}"),
        "the configured topology must be the one the environment declares ({declared:?})"
    );
    topology
}

/// **Supplemental D96 row 38, controller half — topology-dependent (owner ruling 2).**
///
/// One fixture: a `Running` row whose previous controller held fence 5, carrying the record a
/// healthy fan-out leaves (its generation `Acknowledged`, reachable at a recording worker). One
/// closed-form expectation for the topology this process is in:
///
/// * **worker leader** — the row stays `Running`; the leader route adopts to 6, sends that
///   generation nothing and leaves the record un-reopened. Nothing asks the worker to take 6 —
///   the worker file's rows show that is all that raises its floor of 5 — so the window is open;
/// * **controller** — the row becomes `Compiling`; the preamble route sends that generation one
///   directive under 6, addressed to the recorded generation and process, and settles on its
///   answer. The window never opens.
///
/// The cells differ in the state `start` wrote and in what the worker received. The program does
/// not load, so the row runs each route's steps itself; the source pins tie them to their callers.
#[tokio::test]
async fn an_already_running_adoption_leaves_the_old_generation_unfenced_only_under_a_worker_leader()
{
    let topology = declared_topology();
    let job_id = format!("{JOB}-t27-row38");
    let (db, connection) = migrated_job_named(&job_id);
    let (address, directives) = serve(Answers::Acknowledging).await;
    let settled = obligation(Some(address)).settled_and_still_running();
    seed_obligation(&job_id, &connection, Some(&settled));
    seed_previous_authority(&job_id, &connection, FLOOR as i64);
    // Still listed, so nothing below can settle by termination: a settlement is an answer.
    let scheduler = TestScheduler::shared(Lists::Live(vec![(GENERATION, WORKER)]));
    let shutdown = Shutdown::new("t27-row38", SignalBehavior::None);
    let controller = cold_controller_over(&db, Arc::clone(&scheduler), &shutdown).await;

    // (a) The route, as the row `start` wrote records it: `LeaderRunning::name()` is `Running`,
    // `Compiling::name()` is `Compiling`, and publishing the recovered state moves no fence.
    let recovered_into = match topology {
        JobControllerMode::Worker => "Running",
        JobControllerMode::Controller => "Compiling",
    };
    assert_eq!(
        stored_row(&job_id, &connection),
        (recovered_into.to_string(), FLOOR),
        "{topology:?}: a recovered Running row goes to {recovered_into}, under the old fence"
    );

    // (b) That route's steps. Exhaustive, so a third topology does not compile until it is here.
    let mut status = cold_status(&db).await;
    match topology {
        JobControllerMode::Worker => {
            assert!(
                controller.adopt_before_administering(&mut status).await,
                "the leader route adopts the job and may administer it"
            );
            assert_eq!(
                *directives.seen.lock().unwrap(),
                Vec::new(),
                "and sends the generation it inherits nothing: `AdoptingTheGenerationItNames` \
                 re-opens no acknowledged target, so no directive asks that worker to take fence \
                 6, the only thing (per the worker file) that would raise its floor of 5"
            );
            assert_eq!(
                recorded(&job_id, &connection),
                Some(settled),
                "the record stays as the healthy fan-out left it: acknowledged, not re-opened"
            );
        }
        JobControllerMode::Controller => {
            adopt(&mut status, &db).await;
            let discharge = discharge_recorded_obligation(
                &mut status,
                &db,
                &scheduler,
                LifecycleMode::FencedV2,
                DischargeReason::SupersedingTheGenerationsItNames,
            )
            .await;
            assert_eq!(
                *directives.seen.lock().unwrap(),
                vec![(ADOPTED, GENERATION, Some(INCARNATION))],
                "the preamble route re-opens the record and advances fence 6 at the generation it \
                 names, addressed to the process it names"
            );
            assert!(
                matches!(discharge, Discharge::Settled),
                "and settles on that answer: the old acknowledgement was re-opened and the \
                 scheduler still lists the generation: {discharge:?}"
            );
        }
    }
    assert_eq!(
        stored_row(&job_id, &connection).1,
        ADOPTED,
        "{topology:?}: either route holds the job at the adopted fence afterwards"
    );

    // (c) Route and reason, tied by source. Each file is opened for a definition it must hold, so
    // a moved or renamed one fails here rather than passing on whatever text is left.
    const ADOPTING: &str = "DischargeReason::AdoptingTheGenerationItNames";
    const SUPERSEDING: &str = "DischargeReason::SupersedingTheGenerationsItNames";
    let squash = |source: &str| source.split_whitespace().collect::<Vec<_>>().join(" ");
    let machine = include_str!("../mod.rs");
    let machine = &machine[..machine.find("\n#[cfg(test)]").expect("a test half")];
    let adopting = function_body(
        machine,
        "async fn adopt_before_administering(&self, status: &mut JobStatus) -> bool {",
    );
    let start = squash(machine);
    assert_eq!(
        (
            count(&adopting, ADOPTING),
            adopting.contains(SUPERSEDING),
            count(machine, ADOPTING),
            count(&start, ".adopt_before_administering("),
            count(&start, "cold_leader_running = true;"),
        ),
        (1, false, 1, 1, 1),
        "adopt_before_administering discharges only as an adopter, is the one place in \
         states/mod.rs that passes that reason, and has one caller, behind a flag set once"
    );
    let at = |text: &str| start.find(text).expect("in start");
    let order = [
        "let leader_mode = matches!(config().job_controller, JobControllerMode::Worker);",
        "\"Running\" if leader_mode => {",
        "cold_leader_running = true;",
        "\"Compiling\" | \"Scheduling\" | \"Running\"",
        "if cold_leader_running && matches!(program, Ok(Some(_))) && \
         !self.adopt_before_administering(&mut status).await",
    ]
    .map(at);
    assert!(
        order.is_sorted(),
        "leader_mode is the process knob, only its `Running` arm sets the flag, and the flag \
         guards the one call: {order:?}"
    );
    assert_eq!(
        [ADOPTING, ".adopt_before_administering("].map(production_call_sites),
        [
            vec!["src/states/lifecycle/recovery.rs", "src/states/mod.rs"],
            vec!["src/states/mod.rs"]
        ],
        "outside states/mod.rs the adopting reason appears only where recovery.rs matches on it, \
         and nothing calls the leader route"
    );
    let fencing = include_str!("../scheduling/admission/fencing.rs");
    let driver = squash(include_str!("../scheduling/phases/driver.rs"));
    assert_eq!(
        (
            count(fencing, "pub(crate) async fn discharge_recovered_fencing("),
            count(fencing, "discharge_recorded_obligation("),
            count(fencing, SUPERSEDING),
            fencing.contains(ADOPTING),
            count(
                &driver,
                "let preamble = preamble.adopt_lifecycle_authority().await?; \
                 let preamble = preamble.discharge_recovered_fencing().await?;"
            ),
        ),
        (1, 1, 1, false, 1),
        "the preamble's discharge step is admission/fencing.rs's one discharge, as a superseder, \
         run directly after the preamble's adoption: the two steps the controller cell runs"
    );
    // The recording worker keeps a directive's address, not its operation: it is a FENCE_ONLY
    // because recovery.rs reaches a worker only through `advance_fence_each`, which sends what
    // `advance_one` stamps (`one_generation_that_does_not_acknowledge_stops_every_start` on wire).
    let recovery = include_str!("recovery.rs");
    let handshake = include_str!("handshake.rs");
    let body = |signature: &str| {
        let at = handshake.find(signature).expect("handshake.rs defines it");
        &handshake[at..at + handshake[at..].find("\n}\n").expect("a closed function")]
    };
    let each = body("async fn advance_fence_each(");
    let one = body("async fn advance_one(");
    assert_eq!(
        (
            count(recovery, "async fn discharge_recorded_obligation("),
            count(recovery, "advance_fence_each("),
            count(recovery, ".start_execution(") + count(recovery, ".stamp("),
            count(each, "advance_one("),
            count(one, ".stamp("),
            count(one, "generation.fence_only(id, incarnation).stamp("),
        ),
        (1, 1, 0, 1, 1, 1),
        "a recovery pass sends one directive shape, through advance_fence_each: a FENCE_ONLY"
    );
}

/// **M11.T27b, termination half, on the adopting route.** An observed termination settles the
/// generation an already-running adoption inherits, and nothing that merely looks like one does.
///
/// A `Pending` target of the old generation with **no address** — so no acknowledgement can
/// settle it and no directive can be sent — discharged by `adopt_before_administering`, the
/// production route, after its adoption at 6. The route answers `true` only when the discharge
/// settles. The scheduler's answer is the one dimension varied (`Untracked` is what Kubernetes and
/// manual schedulers always answer); the alert, settlement and error series tell `StillPending`
/// (alert raised, one target, one identifier) from a pass that could not run at all (no alert).
///
/// Topology-independent: `adopt_before_administering` reads no topology. Production reaches it
/// only under a worker leader (row 38); the preamble's superseding discharge is
/// `a_generation_settles_only_when_a_tracking_scheduler_says_it_is_gone`'s.
#[tokio::test]
async fn an_observed_termination_settles_the_generation_an_already_running_adoption_inherits() {
    let listed = Lists::Live(vec![(GENERATION, WORKER)]);
    for (row, (case, lists, settles, unobservable)) in [
        ("no longer listed", Lists::Live(vec![]), true, 0),
        ("still listed", listed, false, 0),
        ("the listing failed", Lists::Fails, false, 1),
        ("untracked", Lists::Untracked, false, 1),
    ]
    .into_iter()
    .enumerate()
    {
        let job_id = format!("{JOB}-t27-termination-{row}");
        let (db, connection) = migrated_job_named(&job_id);
        seed_obligation(&job_id, &connection, Some(&obligation(None)));
        seed_previous_authority(&job_id, &connection, FLOOR as i64);
        let shutdown = Shutdown::new("t27-termination", SignalBehavior::None);
        let controller = cold_controller_over(&db, TestScheduler::shared(lists), &shutdown).await;

        let mut status = cold_status(&db).await;
        assert_eq!(
            controller.adopt_before_administering(&mut status).await,
            settles,
            "{case}: the route may administer the job exactly when the inherited generation is \
             observed gone"
        );
        assert_eq!(
            stored_row(&job_id, &connection).1,
            ADOPTED,
            "{case}: the adoption landed first either way"
        );
        let record = recorded(&job_id, &connection).expect("the record is kept");
        let target = &record.targets()[0];
        assert_eq!(
            (target.state, target.attempt_id.as_deref()),
            if settles {
                (FenceTargetState::Terminated, None)
            } else {
                (FenceTargetState::Pending, Some(ATTEMPT))
            },
            "{case}: the row names the target terminated only on an authoritative listing, and \
             otherwise keeps it pending with the identifier it still owes"
        );
        let (pending, outstanding, _age, alert) = metrics::published(&job_id);
        assert_eq!(
            (pending, outstanding, alert),
            if settles { (0, 0, 0) } else { (1, 1, 1) },
            "{case}: a settled discharge clears the alert; an unsettled one raises it"
        );
        assert_eq!(
            (
                metrics::settlements(&job_id, Accounting::TerminatedGeneration),
                metrics::settlements(&job_id, Accounting::AcknowledgedFence),
                metrics::errors(&job_id, FencingError::TerminationUnobservable),
            ),
            (u64::from(settles), 0, unobservable),
            "{case}: a settlement is counted as a termination, never as an acknowledgement, and a \
             scheduler that cannot answer is counted as unobservable"
        );
    }
}

/// **M11.T27c, root authority on the adopting route.** The controller an already-running adoption
/// superseded cannot install its checkpoint candidate as the job's root.
///
/// The half the worker file cannot show (plan risk M11.T27j): there the inherited worker still
/// *admits* a delayed commit under 5; here the fence/epoch-conditional `job_statuses` update
/// refuses the root of the controller that held 5. That controller adopted at 5 — a minted epoch,
/// because the fixture's `epoch-before-this-controller` is not hexadecimal and names no candidate
/// — and the adopter takes the job to 6 through `adopt_before_administering`. Every stale
/// candidate is minted through M11.T25's `Validated<T>` and published, and refused (a) before the
/// adopter writes anything, (b) with the adopter's candidate written but unrooted, and (c) after
/// its root is installed: the same candidate, a fresh one for the next generation, and each half
/// of the adopter's authority paired with the stale other half.
///
/// Topology-independent: neither `install_metadata_root` nor `adopt_before_administering` reads the
/// topology (`the_topology_is_derived_only_from_the_process_knob` pins where it is read).
#[tokio::test]
async fn the_controller_an_already_running_adoption_superseded_cannot_install_its_root() {
    let store = CandidateStore::new("t27c");
    let provider = store.provider().await;
    let (db, connection) = migrated_job();
    seed_previous_authority(JOB, &connection, FLOOR as i64 - 1);
    let mut stale = cold_status(&db).await;
    adopt(&mut stale, &db).await;
    stale.generation = ROOT_GENERATION;
    let held = stale.authority().clone();
    assert_eq!(held.fence().get(), FLOOR);

    let shutdown = Shutdown::new("t27c", SignalBehavior::None);
    let untracked = TestScheduler::shared(Lists::Untracked);
    let adopter = cold_controller_over(&db, untracked, &shutdown).await;
    let mut adopted = cold_status(&db).await;
    assert!(
        adopter.adopt_before_administering(&mut adopted).await,
        "the leader route adopts; the fixture records no obligation, so it asks the scheduler \
         nothing"
    );
    adopted.generation = ROOT_GENERATION;
    assert_eq!(adopted.authority().fence().get(), ADOPTED);

    /// Installs `candidate` under `status`'s authority and asserts the row refused it.
    async fn refused(status: &mut JobStatus, db: &DatabaseSource, c: &RootCandidate, case: &str) {
        let outcome = status.install_metadata_root(db, c).await;
        assert!(
            matches!(outcome, Ok(Ok(AuthorityOutcome::Stale(_)))),
            "{case}: the stale controller's root update must match no row: {outcome:?}"
        );
    }

    // (a) Before the adopter writes anything: the adoption alone moved the predicate.
    let before = candidate_for(&stale);
    before.publish(&provider).await.expect("it publishes");
    refused(&mut stale, &db, &before, "(a) before the adopter writes").await;
    assert_eq!(rooted(&db).await, None, "and the row names no root");

    // (b) The adopter's candidate is written but unrooted; then the adopter installs it.
    let winner = candidate_for(&adopted);
    winner.publish(&provider).await.expect("it publishes");
    refused(&mut stale, &db, &before, "(b) with the adopter's unrooted").await;
    assert_eq!(rooted(&db).await, None, "a written candidate is not a root");
    assert_eq!(
        adopted
            .install_metadata_root(&db, &winner)
            .await
            .expect("the adopter's candidate agrees with its own status"),
        Ok(AuthorityOutcome::Applied(())),
        "the controller that holds the row installs its root"
    );
    assert_eq!(rooted(&db).await, Some(winner.key()));

    // (c) After the root: the same candidate, then each authority the stale controller can build.
    refused(&mut stale, &db, &before, "(c) the same candidate").await;
    // Each half of the adopter's authority, paired with the stale controller's other half.
    let adopter_fence = LifecycleAuthority::from_parts(JOB, ADOPTED, held.epoch());
    let adopter_epoch = LifecycleAuthority::from_parts(JOB, FLOOR, adopted.authority().epoch());
    let (root, mut keys) = (Some(winner.key()), vec![before.key(), winner.key()]);
    for (case, authority, generation) in [
        ("(c) a fresh candidate", held.clone(), ROOT_GENERATION + 1),
        ("(c) adopter's fence", adopter_fence, ROOT_GENERATION),
        ("(c) adopter's epoch", adopter_epoch, ROOT_GENERATION),
    ] {
        stale.authority = authority;
        stale.generation = generation;
        let candidate = candidate_for(&stale);
        candidate.publish(&provider).await.expect("it publishes");
        refused(&mut stale, &db, &candidate, case).await;
        assert_eq!(rooted(&db).await, root, "{case}: the root is untouched");
        keys.push(candidate.key());
    }

    keys.sort();
    assert_eq!(
        store.keys(),
        keys,
        "every candidate is still in the store and none replaced another: a loser leaves unrooted \
         candidates for the grace collector"
    );
    assert_eq!(
        stale.state_context.metadata_root, None,
        "and the stale controller does not believe it installed one"
    );
    assert_eq!(
        stored_authority(&connection),
        (ADOPTED as i64, adopted.authority().epoch().to_string()),
        "and the row still carries exactly the authority the adoption installed"
    );
}
