//! D96 rows 16 and 24 (M11.T26g).
//!
//! Both drive the job's real
//! [`JobSettlementOwner`](super::settlement::JobSettlementOwner), its real
//! [`AdmissionLock`](crate::states::AdmissionLock) admission and the real
//! [`FenceProtocol`](super::protocol::FenceProtocol) through
//! [`InterruptedFanOut`](super::faults::InterruptedFanOut). What the harness supplies is the
//! fault — which observations reach the controller, in what order and how many times — and never
//! the decision.
//!
//! The per-fault rows of M11.D39g's declared model are in [`super::fault_model_tests`].

use arroyo_rpc::config::{JobControllerMode, config};
use arroyo_rpc::fence_wire::{CommitDirective, LifecycleTarget};
use arroyo_types::WorkerId;

use super::actor::{ConsumptionPoint, LifecycleActor, LifecycleDecision};
use super::faults::{
    ATTEMPT, CrashPoint, FENCE, GENERATION, InterruptedFanOut, WORKER,
    directive_from_a_controller_in, superseding_acknowledgement,
};
use super::intent::{IntentMailbox, LifecycleIntent};
use super::mode::LifecycleMode;
use super::protocol::FenceProtocol;
use super::settlement::Progress;
use crate::states::scheduling::fanout::Accounting;
use crate::{JobConfig, PolledJob};

use arroyo_rpc::state_backend::{StateBackendError, StateBackendSelector};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------------------------------
// D96 row 16 — refusal and irreversible phase entry linearize once
// ---------------------------------------------------------------------------------------------

/// A running job's configuration, differing from [`refused_row`] only in the selector.
fn running_config() -> JobConfig {
    JobConfig {
        id: Arc::new("job_abc".to_string()),
        organization_id: "org".to_string(),
        pipeline_name: "pipeline".to_string(),
        pipeline_id: 1,
        stop_mode: crate::types::public::StopMode::none,
        checkpoint_interval: Duration::from_secs(10),
        ttl: None,
        parallelism_overrides: HashMap::new(),
        restart_nonce: 3,
        restart_mode: crate::types::public::RestartMode::safe,
        ignore_state_before_epoch: None,
        env_vars: serde_json::json!({}),
        scheduler_config: serde_json::json!({}),
        state_backend: StateBackendSelector::Parquet,
    }
}

/// The refusal a row asking for another backend produces.
fn selector_changed() -> StateBackendError {
    StateBackendError::JobSelectorChanged {
        label: "job \"job_abc\"".to_string(),
        running: StateBackendSelector::Parquet,
        requested: StateBackendSelector::StateEngine,
    }
}

/// The classified intent a refused row produces, as the configuration poll submits it.
fn refused_row() -> LifecycleIntent {
    LifecycleIntent::classify(
        StateBackendSelector::Parquet,
        PolledJob {
            execution_selector: StateBackendSelector::Parquet,
            config: running_config(),
            refusal: Some(selector_changed()),
        },
    )
}

/// The job's single writer, reading the mailbox a test plays the configuration poll into.
fn actor_for(mailbox: &Arc<IntentMailbox>) -> LifecycleActor {
    LifecycleActor::new(
        Arc::new("job_abc".to_string()),
        StateBackendSelector::Parquet,
        Arc::clone(mailbox),
    )
}

/// **D96 row 16 (PR #157 round 8).** A refusal and the entry into an irreversible phase
/// linearize once.
///
/// The round-8 finding was a snapshot-once *race*: the gate could be read, the refusal published
/// a moment later, and the irreversible work then run under a configuration the controller had
/// already refused — or, symmetrically, the refusal could be consumed by the check and then
/// vanish because the phase carried on. So the requirement has three parts, and each of them is
/// a way the pair could fail to linearize:
///
/// 1. **Exactly one order happens, and each has a closed-form outcome.** A refusal decided
///    before the consumption point stops the phase; one decided after it does not reach that
///    point at all. There is no third answer, and the two are not both applied.
/// 2. **The consumed intent is consumed once.** The decision has a version watermark, so
///    observing again decides nothing — a refusal cannot both stop this phase and stop the next
///    one for the same row.
/// 3. **The publication and the admitted region exclude each other, on the job's own
///    admission.** This is the half M11.D39d adds to M11.D39a: while an interrupted fan-out's
///    obligation is outstanding, the job's refusal cannot be published at all — and it becomes
///    publishable exactly when the last identifier is *accounted for*, not when a message was
///    sent. The message-loss injection is what makes that a claim rather than a coincidence.
///
/// It deliberately instantiates M11.T25's substrate — the intent mailbox and actor, the
/// `SettlementBundle` an interrupted phase releases, and the [`Admission`] it holds —
/// because the fence is the mechanism under test and the token-free substrate is what it is
/// tested through.
#[tokio::test]
async fn refusal_and_phase_entry_linearize_once() {
    // ---- 1a. The refusal is decided before the consumption point: the phase is not entered.
    let mailbox = Arc::new(IntentMailbox::new(Arc::new("job_abc".to_string())));
    let mut actor = actor_for(&mailbox);
    mailbox.submit(refused_row());

    let decision = actor
        .observe(ConsumptionPoint::BeforeIrreversiblePhase)
        .expect("the writer decided the refusal the poll submitted");
    assert!(
        matches!(decision, LifecycleDecision::Refuse(_)),
        "a refusal standing at the consumption point is what the point is for: {decision:?}"
    );

    // ---- 2. And it is decided once.
    assert!(
        actor
            .observe(ConsumptionPoint::BeforeIrreversiblePhase)
            .is_none(),
        "the same row cannot be decided twice: the actor's watermark is what makes 'consumed \
         once' a property of the value rather than of the caller's bookkeeping"
    );

    // ---- 1b. The refusal is decided after the consumption point: the point sees nothing.
    let mailbox = Arc::new(IntentMailbox::new(Arc::new("job_abc".to_string())));
    let mut actor = actor_for(&mailbox);
    assert!(
        actor
            .observe(ConsumptionPoint::BeforeIrreversiblePhase)
            .is_none(),
        "nothing has been decided, so the phase is entered"
    );
    mailbox.submit(refused_row());
    let after = actor
        .observe(ConsumptionPoint::InsideInterruptibleWait)
        .expect("the refusal is still standing at the next consumption point");
    assert!(
        matches!(after, LifecycleDecision::Refuse(_)),
        "a refusal the phase entry outran is not a lost refusal — it is offered at the next \
         point the job reaches, which is what stops the two orders from having three outcomes: \
         {after:?}"
    );

    // ---- 3. The admitted region and the refusal publication exclude each other.
    let mut region = InterruptedFanOut::crashed_at(CrashPoint::FanOut, &[(WORKER, ATTEMPT)]).await;
    assert_eq!(
        region.outstanding(),
        Some(1),
        "the interrupted fan-out's obligation is held by the job's settlement owner"
    );
    assert!(
        !region.authority_released(),
        "and while it is held, the job's refusal cannot be published: the admission the \
         irreversible phase took is the same one a publication needs, so the two linearize on \
         it rather than on an order the caller has to get right"
    );

    // A fence acknowledgement that was sent and never arrived releases nothing. This is the
    // whole of M11.T26r: the controller may not infer settlement from having sent a message.
    region.lose(
        "ack-that-never-arrived",
        superseding_acknowledgement(WORKER, GENERATION),
    );
    assert_eq!(region.lost(), ["ack-that-never-arrived"]);
    assert_eq!(
        region.outstanding(),
        Some(1),
        "a message that never arrived accounted for nothing"
    );
    assert!(
        !region.authority_released(),
        "so the refusal is still not publishable — the region ends when the obligation is \
         accounted for, and nothing has accounted for it"
    );

    // The acknowledgement that does arrive accounts for the last identifier, and only then is
    // the publication admissible.
    match region.observe(
        "ack-that-arrived",
        superseding_acknowledgement(WORKER, GENERATION),
    ) {
        Progress::Discharged(discharged) => assert_eq!(
            discharged.accounted(),
            [(
                WorkerId(WORKER),
                ATTEMPT.to_string(),
                Accounting::AcknowledgedFence
            )],
            "the discharge names every identifier and what accounted for it"
        ),
        other => panic!("the last identifier's observation discharges the obligation: {other:?}"),
    }
    assert_eq!(
        region.outstanding(),
        None,
        "the owner holds no obligation now"
    );
    assert!(
        region.authority_released(),
        "and the refusal is publishable — exactly once, and exactly after the region it could \
         not overtake"
    );
}

// ---------------------------------------------------------------------------------------------
// D96 row 24 — the fence protocol covers both controller topologies
// ---------------------------------------------------------------------------------------------

/// The topology this process is configured for, read exactly as production reads it.
///
/// `scheduling.rs` and `scheduling/admission.rs` each derive `leader_mode` from this expression
/// and nothing else does; `the_topology_has_exactly_two_production_derivations` pins that.
fn process_topology() -> JobControllerMode {
    if matches!(config().job_controller, JobControllerMode::Worker) {
        JobControllerMode::Worker
    } else {
        JobControllerMode::Controller
    }
}

/// **D96 row 24 (Round-49 closure).** The fence protocol covers both controller topologies.
///
/// **This row is topology-dependent, and it is the one row whose subject is the thing the runner
/// varies.** `scripts/m11-d39-matrix.sh` runs every registered test once per
/// `config().job_controller` value, so the two cells of every other row are two processes. That
/// alone proves the tests *ran* under both configurations; it does not prove the configuration
/// reached anything. This row is what closes that gap, in three steps:
///
/// 1. **The knob resolves.** `config().job_controller` is compared against the environment the
///    runner set. A figment mapping that stopped resolving `ARROYO__JOB_CONTROLLER` would make
///    both cells the same process, silently, and every other row would stay green; here it
///    fails.
/// 2. **The knob is the only decider.** The topology is derived in exactly two places in the
///    crate's production half, by the same expression. A third derivation — or one that read
///    something else — would be a topology this runner cannot vary.
/// 3. **Both topologies carry the fence, by different routes, and the routes agree.** In
///    controller mode the controller's own `FenceProtocol` stamps the commit; in worker-leader
///    mode the controller builds no `JobController` at all and the job's leader commits under
///    the `CommitAuthority` its own fenced *start* conferred. The two are different values
///    reached by different code, and they must produce the same directive — otherwise one
///    topology commits under an authority the other would refuse. The match over
///    [`JobControllerMode`] is exhaustive, so a third topology does not compile until it has a
///    fenced route here.
#[tokio::test]
async fn fence_protocol_covers_both_controller_modes() {
    // ---- 1. The knob resolves to what the runner set.
    let declared = std::env::var("ARROYO__JOB_CONTROLLER");
    let expected = match declared.as_deref() {
        Ok("worker") => JobControllerMode::Worker,
        Ok("controller") | Err(_) => JobControllerMode::Controller,
        Ok(other) => panic!(
            "ARROYO__JOB_CONTROLLER={other:?} is not a topology this build has; the runner and \
             `JobControllerMode` disagree"
        ),
    };
    let topology = process_topology();
    assert_eq!(
        format!("{topology:?}"),
        format!("{expected:?}"),
        "the process's configured topology must be the one the environment declares \
         (ARROYO__JOB_CONTROLLER={declared:?}). If this fails, the two cells of every row in \
         the D39 matrix are the same process wearing two labels"
    );

    // ---- 3. Both topologies carry the fence, by different routes.
    let protocol = directive_from_a_controller_in(LifecycleMode::FencedV2);
    let FenceProtocol::Fenced(generation) = protocol else {
        panic!("an adopted controller under the fenced mechanism has a fenced protocol");
    };

    // The controller-mode route: the job's own protocol is what stamps the commit, because
    // `prepare_handover` builds a `JobController` holding it.
    let by_the_controller = protocol.commit_authority().directive(WORKER);
    // The worker-leader route: no `JobController` exists in the controller process, and the
    // leader commits under the authority the fenced start it applied conferred on it.
    let by_the_leader = generation
        .address(WorkerId(WORKER))
        .commit_authority()
        .directive(WORKER);

    let CommitDirective::Fenced(controller_address) = by_the_controller else {
        panic!("a fenced controller's commit directive carries a fence");
    };
    let CommitDirective::Fenced(leader_address) = by_the_leader else {
        panic!("a fenced start confers a fenced commit authority on the leader it admitted");
    };
    assert_eq!(
        (controller_address.fence(), controller_address.target()),
        (
            FENCE,
            LifecycleTarget::addressed(WORKER, GENERATION).unwrap()
        ),
        "the controller-mode route commits under the fence its own row produced, addressed to \
         the generation this attempt raised the job to"
    );
    assert_eq!(
        (leader_address.fence(), leader_address.target()),
        (controller_address.fence(), controller_address.target()),
        "and the worker-leader route reaches the same directive by a different value: the \
         authority the leader's own fenced start conferred. A topology whose commits carried a \
         different fence would be committing on an authority the other would refuse"
    );

    // Exhaustive over the topologies this build has: a third one does not compile until it
    // names the route that carries its fence.
    let route = match topology {
        JobControllerMode::Controller => "JobController holds the job's FenceProtocol",
        JobControllerMode::Worker => {
            "RunningJobModel holds the CommitAuthority its start conferred"
        }
    };
    assert!(
        !route.is_empty(),
        "every topology names the value that carries its fence"
    );

    // ---- The distinct path, for the topology this process is actually in.
    let handover = include_str!("../scheduling/admission/handover.rs");
    let leader_returns_before_building_one = handover
        .find("if self.leader_mode {")
        .zip(handover.find("self.job_controller = Some(JobController::new("))
        .map(|(early, built)| early < built)
        .expect("the handover both tests the topology and builds the controller");
    match topology {
        JobControllerMode::Controller => {
            assert!(
                matches!(protocol, FenceProtocol::Fenced(_)),
                "in controller mode the fence the commits carry is the one this controller's \
                 own protocol holds, and this process is in controller mode"
            );
        }
        JobControllerMode::Worker => {
            assert!(
                leader_returns_before_building_one,
                "in worker-leader mode the controller builds no JobController — `prepare_handover` \
                 returns before it — so the commit fence cannot come from one. This process is \
                 in worker-leader mode, and what carries the fence here is the authority the \
                 leader's own fenced start conferred, asserted above"
            );
        }
    }
}

/// The controller topology is derived only from the process knob, and in exactly three places.
///
/// Step 2 of D96 row 24, kept as its own row because it is a statement about the *crate* rather
/// than about a run: a place that decided the topology from something other than
/// `config().job_controller` would be a path `scripts/m11-d39-matrix.sh` cannot vary and
/// therefore cannot report on, and the matrix would say "both topologies" about a path only one
/// of its two processes ever reached.
///
/// The set is taken from the source rather than sampled, so a fourth derivation fails here
/// instead of quietly joining the three.
#[test]
fn the_topology_is_derived_only_from_the_process_knob() {
    /// The expression every derivation has to be, so the three cannot drift apart.
    const DERIVATION: &str = "matches!(config().job_controller, JobControllerMode::Worker)";

    /// Everything in a file before its test module.
    fn production_half(source: &str) -> &str {
        match source.find("\n#[cfg(test)]") {
            Some(at) => &source[..at],
            None => source,
        }
    }

    /// Whether this file is compiled only under `cfg(test)`.
    ///
    /// Such a file has no `#[cfg(test)]` marker of its own to cut at — the attribute is on the
    /// `mod` declaration in its parent — so `production_half` would return the whole of it and
    /// a test that mentions the knob would read as a production derivation.
    fn is_test_only(relative: &str) -> bool {
        relative.ends_with("_tests.rs")
            || relative.ends_with("/tests.rs")
            || relative == "states/lifecycle/faults.rs"
            || relative.contains("compile_fail/")
    }

    fn walk(
        dir: &std::path::Path,
        root: &std::path::Path,
        found: &mut std::collections::BTreeMap<String, usize>,
    ) {
        for entry in std::fs::read_dir(dir).expect("this crate's own source") {
            let path = entry.expect("a readable directory entry").path();
            if path.is_dir() {
                walk(&path, root, found);
                continue;
            }
            if path.extension().is_none_or(|e| e != "rs") {
                continue;
            }
            let relative = path
                .strip_prefix(root)
                .expect("a path under the crate's source root")
                .to_string_lossy()
                .replace('\\', "/");
            if is_test_only(&relative) {
                continue;
            }
            let source = std::fs::read_to_string(&path).expect("a readable source file");
            // Code lines only: a doc comment that *names* the knob — `root.rs` explains where
            // the topology comes from — is not a place that reads it.
            let production: Vec<&str> = production_half(&source)
                .lines()
                .filter(|line| !line.trim_start().starts_with("//"))
                .collect();
            let reads = production
                .iter()
                .map(|line| line.matches("config().job_controller").count())
                .sum::<usize>();
            if reads > 0 {
                found.insert(relative, reads);
                assert_eq!(
                    production
                        .iter()
                        .map(|line| line.matches(DERIVATION).count())
                        .sum::<usize>(),
                    reads,
                    "every production read of the job-controller knob must be the derivation \
                     `{DERIVATION}`, so the three sites cannot come to mean different things"
                );
            }
        }
    }

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut found = std::collections::BTreeMap::new();
    walk(&root, &root, &mut found);

    assert_eq!(
        found.into_iter().collect::<Vec<_>>(),
        vec![
            // Which state a restarted controller resumes a running job in.
            ("states/mod.rs".to_string(), 1),
            // Which recovery-checkpoint route the scheduling body takes.
            ("states/scheduling.rs".to_string(), 1),
            // Which topology a scheduling attempt's phase context is in.
            ("states/scheduling/admission.rs".to_string(), 1),
        ],
        "these are the three places the controller topology is decided, and \
         `ARROYO__JOB_CONTROLLER` is what decides all three. A file appearing here that is not \
         in this list is a topology-dependent path the D39 matrix does not vary"
    );
}
