//! M11.D39g's declared fault model, one row per fault (M11.T26g).
//!
//! The controller's half: what a fault does to the *observation stream* an interrupted fan-out's
//! obligation is reconciled against, and therefore to what the job's settlement owner may
//! release. D96 rows 16 and 24 are in [`super::faults_tests`]; the worker's half of the same
//! model is `arroyo_worker::lifecycle_fence::faults_tests`.

use tonic::Code;

use super::faults::{
    ATTEMPT, CrashPoint, FENCE, Fault, GENERATION, InterruptedFanOut, WORKER, acknowledgement_at,
    authoritative_response, directive_from_a_controller_in, observed_termination, settlement_under,
    superseding_acknowledgement,
};
use super::mode::LifecycleMode;
use super::protocol::{FenceProtocol, TransportSettlement};
use super::settlement::Progress;

/// Every fault M11.D39g declares has an injection, and it is where the table says it is.
///
/// The anti-drift check. `Fault::ALL` is exhaustive by construction — a variant added without
/// an injection does not compile — and this reads the table back against the sources, so an
/// injection renamed or deleted makes the fault unfindable and fails here rather than leaving
/// the model claiming coverage it lost.
#[test]
fn every_declared_fault_has_a_live_injection() {
    assert_eq!(
        Fault::ALL.len(),
        13,
        "M11.D39g's declared model: loss, duplication, reorder, in-transit delay; worker \
         crash/restart, partition, endpoint reuse; controller crash at preamble, mid-fan-out \
         and mid-commit; incapable and unregistered peers; post-flag-day skew"
    );
    for fault in Fault::ALL {
        if let Err(path) = fault.resolves() {
            panic!(
                "{fault:?}: its declared injection {:?} is not in {path}. Either the injection \
                 was renamed and the table was not, or the fault has lost its coverage",
                fault.injection()
            );
        }
    }
}

/// A duplicated observation accounts for the identifier once.
///
/// M11.D39g's duplication row at the controller. Idempotence is not an optimisation here: an
/// obligation that counted a repeated acknowledgement twice would discharge with identifiers
/// still outstanding, which is a released lifecycle authority behind a `StartExecution` a worker
/// may still apply.
#[tokio::test]
async fn a_duplicated_observation_accounts_for_its_identifier_once() {
    let mut region =
        InterruptedFanOut::crashed_at(CrashPoint::FanOut, &[(WORKER, ATTEMPT), (WORKER + 1, "b")])
            .await;
    region.duplicate(
        "ack-worker-3",
        superseding_acknowledgement(WORKER, GENERATION),
    );

    let progress: Vec<String> = region
        .progress()
        .into_iter()
        .map(|(label, progress)| format!("{label}:{progress:?}"))
        .collect();
    assert_eq!(
        progress,
        vec![
            "ack-worker-3:StillOwed { outstanding: 1 }".to_string(),
            "ack-worker-3:StillOwed { outstanding: 1 }".to_string(),
        ],
        "the first arrival accounts for the identifier and the second finds it already \
         accounted for: the count does not move"
    );
    assert_eq!(region.outstanding(), Some(1));
    assert!(
        !region.authority_released(),
        "and the other worker's identifier is still owed, so nothing has been released"
    );
}

/// Observations that arrive in the opposite order still discharge exactly once.
///
/// M11.D39g's reorder row. Discharge is a fold over the whole inventory rather than a countdown,
/// so the order the facts arrive in cannot decide whether the last one discharges.
#[tokio::test]
async fn reordered_observations_discharge_the_obligation_exactly_once() {
    let mut region =
        InterruptedFanOut::crashed_at(CrashPoint::FanOut, &[(WORKER, ATTEMPT), (WORKER + 1, "b")])
            .await;
    region.hold("response-worker-3", authoritative_response(WORKER, ATTEMPT));
    region.hold(
        "termination-worker-4",
        observed_termination(WORKER + 1, GENERATION),
    );
    region.deliver_held_in_reverse();

    let kinds: Vec<&str> = region
        .progress()
        .into_iter()
        .map(|(label, progress)| match progress {
            Progress::StillOwed { .. } => label,
            Progress::Discharged(_) => "discharged",
            other => panic!("{label}: {other:?}"),
        })
        .collect();
    assert_eq!(
        kinds,
        vec!["termination-worker-4", "discharged"],
        "the newer fact arrives first and leaves one identifier owed; the older one then \
         arrives and is the last, so it discharges — which is the same pair of outcomes the \
         send order would have produced, with the labels swapped"
    );
    assert_eq!(region.outstanding(), None);
    assert!(region.authority_released());
}

/// An arbitrarily delayed acknowledgement of a fence too low to revoke anything settles nothing.
///
/// M11.D39g's in-transit delay row, at the case the delay actually creates: the message is not
/// corrupt, it is *stale*. A worker revokes what is below the fence it takes, so an
/// acknowledgement of the very fence this attempt's starts carry has made none of them
/// inapplicable — and treating it as settlement would release the authority behind a request the
/// worker may still apply.
#[tokio::test]
async fn a_delayed_acknowledgement_below_the_issuing_fence_settles_nothing() {
    let mut region = InterruptedFanOut::crashed_at(CrashPoint::FanOut, &[(WORKER, ATTEMPT)]).await;
    region.hold("stale-ack", acknowledgement_at(WORKER, GENERATION, FENCE));

    let progress = region.deliver_held("stale-ack");
    assert!(
        matches!(progress, Progress::NotThisObligation),
        "an acknowledgement that does not supersede the issuing fence accounts for nothing: \
         {progress:?}"
    );
    assert_eq!(region.outstanding(), Some(1));
    assert!(!region.authority_released());

    // One above it does, which is what makes the comparison the discriminator.
    let progress = region.observe(
        "superseding-ack",
        superseding_acknowledgement(WORKER, GENERATION),
    );
    assert!(
        matches!(progress, Progress::Discharged(_)),
        "and a fence one higher revokes what was issued below it: {progress:?}"
    );
}

/// A restarted worker generation, and a successor at the same endpoint, account for nothing.
///
/// M11.D39g's worker crash/restart and endpoint-reuse rows, at the controller's end. The
/// worker's end — that the successor refuses its predecessor's delayed start — is
/// `arroyo_worker::lifecycle_fence::faults_tests`. What is asserted here is the other half: an
/// answer from a *different* generation is not an answer about this obligation, so a controller
/// that read "the endpoint responded" as settlement would release its authority on a message
/// about somebody else's request.
#[tokio::test]
async fn an_answer_from_another_generation_accounts_for_nothing() {
    let mut region = InterruptedFanOut::crashed_at(CrashPoint::FanOut, &[(WORKER, ATTEMPT)]).await;

    for (label, observed) in [
        (
            "successor-generation",
            superseding_acknowledgement(WORKER, GENERATION + 1),
        ),
        (
            "predecessor-generation",
            superseding_acknowledgement(WORKER, GENERATION - 1),
        ),
        (
            "another-worker",
            superseding_acknowledgement(WORKER + 9, GENERATION),
        ),
        (
            "another-identifier",
            authoritative_response(WORKER, "some-other-attempt-id"),
        ),
    ] {
        let progress = region.observe(label, observed);
        assert!(
            matches!(progress, Progress::NotThisObligation),
            "{label}: an observation that does not name this obligation's target, generation \
             and identifier accounts for nothing: {progress:?}"
        );
    }
    assert_eq!(region.outstanding(), Some(1));
    assert!(
        !region.authority_released(),
        "the obligation stands: a reused endpoint answering for its predecessor is exactly \
         what M11.D39d's identity pair exists to reject"
    );
}

/// A controller that died before its fan-out owes nothing, and one that died during it owes
/// everything it issued.
///
/// M11.D39g's controller-crash rows. The three points differ in exactly one thing — what had
/// been issued — and that is what the obligation records. A preamble crash that left an
/// obligation behind would be a job fencing against workers it never addressed.
#[tokio::test]
async fn a_controller_crash_owes_exactly_what_it_had_issued() {
    let preamble = InterruptedFanOut::crashed_at(CrashPoint::Preamble, &[(WORKER, ATTEMPT)]).await;
    assert_eq!(
        preamble.outstanding(),
        None,
        "a preamble that never reached the fan-out issued nothing, so the obligation it handed \
         over is vacuously fully accounted for and the owner discharged it at the hand-over: \
         there is no identifier for anything to still be answerable for"
    );
    assert!(
        preamble.authority_released(),
        "and the job's lifecycle authority is released immediately, which is the correct answer \
         for a crash before any request existed — a controller that fenced against workers it \
         never addressed would hold the job's publication lock for nothing"
    );

    let fan_out = InterruptedFanOut::crashed_at(CrashPoint::FanOut, &[(WORKER, ATTEMPT)]).await;
    assert_eq!(fan_out.outstanding(), Some(1));

    let commit =
        InterruptedFanOut::crashed_at(CrashPoint::Commit, &[(WORKER, ATTEMPT), (WORKER + 1, "b")])
            .await;
    assert_eq!(
        commit.outstanding(),
        Some(2),
        "a controller that died with a two-phase commit outstanding owes every identifier its \
         fan-out issued: the commit is published to the same generation under the same fence"
    );
}

/// The two sides of the flag day send different messages and read every answer the same way.
///
/// M11.D39g's post-flag-day skew row at the controller, restated by M11.T26h's activation
/// change. The **directive** half is unchanged and is what skew actually is: a controller on the
/// legacy side sends the shape that predates the fields, one on the fenced side sends a fence and
/// a target, and a worker that has been put into strict mode refuses the first.
///
/// The **classification** half moved. Before the activation change `transport_settlement` took a
/// mode and `Aborted` was ambiguous to a legacy controller — the M11.T08 busy-worker retry — so
/// this row asserted that *exactly one* code differed across the modes. That parameter was
/// removed with the arm, and the assertion is inverted rather than deleted: **zero** codes now
/// differ, because a controller of this build cannot be persuaded to read a settled attempt as
/// retriable by anything, including a fixture that names the pre-flag-day peer. The requirement
/// it carries — "skew is which message you send, not how you read an answer" — is stronger for
/// it, and `Aborted` is separately pinned on the settlement side.
#[test]
fn post_flag_day_skew_moves_exactly_one_transport_code() {
    assert!(
        matches!(
            directive_from_a_controller_in(LifecycleMode::LegacyT08),
            FenceProtocol::Legacy
        ),
        "a controller predating the flag day sends the fence-less shape whatever its row says"
    );
    assert!(
        matches!(
            directive_from_a_controller_in(LifecycleMode::FencedV2),
            FenceProtocol::Fenced(_)
        ),
        "and one after it addresses the generation under its own fence"
    );

    let moved: Vec<Code> = [
        Code::Ok,
        Code::Cancelled,
        Code::Unknown,
        Code::InvalidArgument,
        Code::DeadlineExceeded,
        Code::NotFound,
        Code::AlreadyExists,
        Code::PermissionDenied,
        Code::ResourceExhausted,
        Code::FailedPrecondition,
        Code::Aborted,
        Code::OutOfRange,
        Code::Unimplemented,
        Code::Internal,
        Code::Unavailable,
        Code::DataLoss,
        Code::Unauthenticated,
    ]
    .into_iter()
    .filter(|code| {
        settlement_under(LifecycleMode::LegacyT08, *code)
            != settlement_under(LifecycleMode::FencedV2, *code)
    })
    .collect();
    assert_eq!(
        moved,
        Vec::<Code>::new(),
        "no code is classified differently on the two sides of the flag day any more: \
         M11.T26h removed the mode from the taxonomy along with the `Aborted` arm that was \
         the one thing it decided, so skew is which directive a controller sends and nothing \
         about how it reads an answer"
    );
    for mode in LifecycleMode::ALL {
        assert_eq!(
            settlement_under(mode, Code::Aborted),
            TransportSettlement::Definitive,
            "{mode:?}: `Aborted` is a definitive 'nothing applied' (M11.D39e(iii)), and there \
             is no directive shape a controller of this build can send that makes it anything \
             else"
        );
    }
}
