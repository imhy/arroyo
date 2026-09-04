//! D96 row 18, and the worker half of M11.D39g's fault model exercised through
//! [`Link`](super::faults::Link).
//!
//! Every row here delivers a real request to a real [`WorkerServer`](crate::WorkerServer)
//! through the production `start_execution` handler. What the harness supplies is the *fault* —
//! when a directive arrives, how many times, and which generation is at the other end — and
//! never the answer.

use tonic::Code;

use super::faults::{Link, WorkerFault};
use super::tests::{
    AMBIGUOUS, GENERATION, WORKER, addressed_start, fence_only, fenced_start, revoke, settlement,
    unfenced,
};
use arroyo_rpc::grpc::rpc::StartExecutionOutcome;

/// The fence a superseded controller's in-flight start was issued under.
const ISSUED_UNDER: u64 = 4;

/// The fence the replacement controller advanced this generation to.
///
/// M11.D39d makes acknowledging it the precondition of publishing `Refused`: *"after `Refused`
/// publication, every old worker generation has acked the newer fence or terminated, so a
/// delayed request cannot begin execution"*.
const REFUSAL_FENCE: u64 = 7;

/// The identifier the superseded controller issued.
const ATTEMPT: &str = "0123456789abcdef0123456789abcdef";

/// **D96 row 18 (PR #157 round 16).** A start delayed past the acknowledgement that permitted a
/// refusal cannot begin execution.
///
/// M11.D39g's delayed-delivery row, at the end that decides it. The controller half — that
/// `Refused` is not published until every target generation has acknowledged the newer fence or
/// been observed terminated — is D96 rows 17 and 20, in `lifecycle/recovery_tests.rs` and
/// `states/mod.rs`. This is what that acknowledgement is *worth*: once it has been given, the
/// request it superseded is refused however long it was in flight, and refused definitively.
///
/// Three things are asserted, and the third is what stops the first two from being an accident:
///
/// * **the control** — the identical directive, held identically, delivered *before* the
///   acknowledgement, applies. So the delay is not what rejects it;
/// * **the row** — delivered after, it is refused `FailedPrecondition` with the fence
///   comparison named in the message, nothing is applied, nothing is recorded, and the phase is
///   untouched. Re-sending it changes nothing;
/// * **the discriminator** — the same identifier, delivered just as late, under the fence this
///   generation acknowledged, applies. So it is the *fence height* that rejects it and not the
///   arrival order, not the identifier, and not the worker generation — which is D96 row 22's
///   discriminator and a different refusal.
#[tokio::test]
async fn delayed_start_after_refusal_rejected_by_acknowledged_fence() {
    // ---- The control: the same delayed directive, delivered before the acknowledgement. ----
    let mut control = Link::to_registered_generation(false);
    control.hold("start-under-4", fenced_start(ATTEMPT, ISSUED_UNDER));
    assert_eq!(
        control.deliver_held("start-under-4").unwrap(),
        settlement(ISSUED_UNDER, StartExecutionOutcome::Applied),
        "a start that is merely late is still a start: nothing about being held in transit \
         rejects it"
    );
    assert_eq!(control.applied(), Some(ATTEMPT.to_string()));

    // ---- The row. ----
    let mut link = Link::to_registered_generation(false);
    // In flight since before the replacement controller existed.
    link.hold("start-under-4", fenced_start(ATTEMPT, ISSUED_UNDER));

    // The replacement controller advances this generation. This response is the evidence
    // M11.D39d requires before `Refused` may be published at all.
    assert_eq!(
        link.deliver(fence_only(REFUSAL_FENCE)).unwrap(),
        settlement(REFUSAL_FENCE, StartExecutionOutcome::FenceAcknowledged),
        "the generation acknowledges the newer fence, and reports the height it acknowledged"
    );
    assert_eq!(link.acknowledged(), REFUSAL_FENCE);
    assert!(
        link.strict(),
        "and acknowledging a fenced directive turns strict mode on for this generation \
         (M11.D39e(i)), so it will not fall back to the fence-less route either"
    );

    // Now the delayed start arrives.
    let refused = link
        .deliver_held("start-under-4")
        .expect_err("a start under a superseded fence cannot begin execution");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        refused.message(),
        format!(
            "lifecycle fence {ISSUED_UNDER} is older than fence {REFUSAL_FENCE} this worker \
             generation has acknowledged"
        ),
        "and it is refused for the fence comparison itself — not for the generation, which \
         agrees, and not for registration, which completed"
    );
    assert!(
        !AMBIGUOUS.contains(&refused.code()),
        "the refusal is definitive (M11.D39e(iii)): none of the four codes the controller \
         retries with the same identifier, so the superseded controller cannot read it as a \
         transport failure and try again"
    );
    assert!(
        link.idle(),
        "nothing was applied: the phase is exactly where the acknowledgement left it"
    );
    assert_eq!(link.applied(), None);
    assert_eq!(
        link.tracked(),
        0,
        "and no identifier was recorded, so the refusal cost this generation none of its \
         bounded capacity either"
    );
    assert_eq!(
        link.acknowledged(),
        REFUSAL_FENCE,
        "and the refused directive did not lower the acknowledged fence: a stale start that \
         could pull the floor down would let the superseded controller re-open the window it \
         was fenced out of"
    );

    // Re-sending it — M11.D39g's duplication row on the same message — changes nothing.
    let [first, second] = link.duplicate(fenced_start(ATTEMPT, ISSUED_UNDER));
    for (which, answer) in [("first", first), ("second", second)] {
        match answer {
            Err(status) => assert_eq!(
                status.code(),
                Code::FailedPrecondition,
                "the {which} duplicate is refused definitively, exactly as the original was"
            ),
            Ok(response) => panic!(
                "the {which} duplicate of a superseded start must not be answered: {response:?}"
            ),
        }
    }
    assert!(link.idle());
    assert_eq!(link.applied(), None);

    // ---- The discriminator: the same identifier, just as late, under the acknowledged fence.
    assert_eq!(
        link.deliver(fenced_start(ATTEMPT, REFUSAL_FENCE)).unwrap(),
        settlement(REFUSAL_FENCE, StartExecutionOutcome::Applied),
        "the fence height is the whole difference: the same identifier, delivered after the \
         same acknowledgement, applies when it is issued under the fence this generation \
         acknowledged"
    );
    assert_eq!(link.applied(), Some(ATTEMPT.to_string()));
}

/// The other half of M11.D39d's refusal precondition: a revocation names the identifier, and the
/// delayed start it names is refused for that reason rather than for its fence.
///
/// The replacement controller does not only advance the fence — it *revokes all named
/// lower-fence outstanding IDs*. A start revoked by name is refused even when it is re-issued
/// under a fence this generation would otherwise accept, which is what makes the revocation
/// worth carrying at all.
#[tokio::test]
async fn a_revoked_identifier_stays_refused_under_a_fence_the_generation_accepts() {
    let mut link = Link::to_registered_generation(false);
    link.hold("start-under-4", fenced_start(ATTEMPT, ISSUED_UNDER));

    assert_eq!(
        link.deliver(revoke(REFUSAL_FENCE, &[ATTEMPT])).unwrap(),
        settlement(REFUSAL_FENCE, StartExecutionOutcome::Revoked),
    );

    let refused = link
        .deliver_held("start-under-4")
        .expect_err("a revoked identifier is refused");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        refused.message(),
        format!(
            "lifecycle fence {ISSUED_UNDER} is older than fence {REFUSAL_FENCE} this worker \
             generation has acknowledged"
        ),
        "the fence comparison is the wider check and answers first — the guard orders its \
         refusals from the widest, so a directive that is both stale and revoked is reported \
         stale"
    );

    // The revocation is what the identifier is refused *for* once the fence agrees. This is
    // the case the fence alone cannot cover: a superseded controller that re-issues its
    // identifier under the fence this generation has acknowledged.
    let still = link
        .deliver(fenced_start(ATTEMPT, REFUSAL_FENCE))
        .expect_err("a revoked identifier is permanently revoked");
    assert_eq!(still.code(), Code::FailedPrecondition);
    assert_eq!(
        still.message(),
        format!("execution {ATTEMPT} is permanently revoked for this worker generation"),
        "and it is permanent: M11.T26d's record has no eviction operation, so the identifier \
         cannot be rehabilitated by any later directive"
    );
    assert!(link.idle());
    assert_eq!(link.applied(), None);
}

/// A fence advancement that never arrives leaves the generation exactly where it was.
///
/// M11.D39g's message-loss row, at the worker's end. There is no timeout and no inference: a
/// directive that did not arrive is not a directive that was refused, and the generation goes on
/// answering for its predecessor's fence until something actually reaches it. That is the fact
/// that makes the *controller's* obligation durable — see D96 row 20.
#[tokio::test]
async fn a_lost_fence_advance_leaves_the_generation_answering_for_the_old_fence() {
    let mut link = Link::to_registered_generation(false);
    link.lose("fence-only-7", fence_only(REFUSAL_FENCE));

    assert_eq!(link.lost(), ["fence-only-7"]);
    assert_eq!(
        link.acknowledged(),
        0,
        "a directive that never arrived acknowledged nothing"
    );
    assert!(
        !link.strict(),
        "and did not turn strict mode on: a worker cannot be moved by a message it never got"
    );
    assert_eq!(
        link.deliver(fenced_start(ATTEMPT, ISSUED_UNDER)).unwrap(),
        settlement(ISSUED_UNDER, StartExecutionOutcome::Applied),
        "so the start the lost fence would have superseded still applies — which is exactly \
         why the controller may not publish `Refused` on the strength of having *sent* one"
    );
}

/// Directives that arrive in the opposite order leave the generation at the highest fence, and
/// refuse the lower one.
///
/// M11.D39g's reorder row. The guard's acknowledged fence is a maximum
/// (`FenceState::acknowledge`), so arrival order cannot lower it; and the directive that arrives
/// *after* a higher fence is refused rather than silently ignored, so a controller reading the
/// answers can tell which of its directives took effect.
#[tokio::test]
async fn reordered_directives_leave_the_generation_at_the_highest_fence() {
    let mut link = Link::to_registered_generation(false);
    link.hold("fence-only-4", fence_only(ISSUED_UNDER));
    link.hold("fence-only-7", fence_only(REFUSAL_FENCE));

    let delivered = link.deliver_held_in_reverse();
    let outcomes: Vec<(&str, Result<u64, Code>)> = delivered
        .into_iter()
        .map(|(label, answer)| {
            (
                label,
                answer
                    .map(|resp| resp.observed_lifecycle_fence)
                    .map_err(|status| status.code()),
            )
        })
        .collect();
    assert_eq!(
        outcomes,
        vec![
            ("fence-only-7", Ok(REFUSAL_FENCE)),
            ("fence-only-4", Err(Code::FailedPrecondition)),
        ],
        "the newer fence arrives first and is acknowledged; the older one then arrives and is \
         refused, because this generation has already left it behind"
    );
    assert_eq!(link.acknowledged(), REFUSAL_FENCE);
}

/// A restarted generation has acknowledged nothing, and says so.
///
/// M11.D39g's worker crash/restart row. The restarted process keeps no fence state, so a
/// controller that had advanced the predecessor learns nothing about what it applied by
/// reaching the successor — which is why M11.D39e(v) admits only an acknowledgement or an
/// observed *termination* as settlement, and never "the endpoint answered".
#[tokio::test]
async fn a_restarted_generation_answers_for_nothing_its_predecessor_acknowledged() {
    let mut link = Link::to_registered_generation(false);
    assert_eq!(
        link.deliver(fence_only(REFUSAL_FENCE)).unwrap(),
        settlement(REFUSAL_FENCE, StartExecutionOutcome::FenceAcknowledged),
    );
    assert_eq!(link.acknowledged(), REFUSAL_FENCE);

    link.restart_generation();

    assert_eq!(
        link.acknowledged(),
        0,
        "the restarted process starts from nothing: the fence its predecessor acknowledged is \
         not durable worker state, and treating a reachable endpoint as though it were would \
         settle attempts nothing accounted for"
    );
    assert!(
        !link.strict(),
        "and strict mode is not carried across either"
    );
    // It has not announced itself, so the fenced protocol is closed to it until it does.
    let unregistered = link
        .deliver(fenced_start(ATTEMPT, REFUSAL_FENCE))
        .expect_err("a restarted generation has announced itself to nobody");
    assert_eq!(unregistered.code(), Code::FailedPrecondition);
    assert_eq!(
        unregistered.message(),
        "Worker generation has not begun registration"
    );
}

/// A new generation at the predecessor's endpoint refuses the predecessor's delayed start.
///
/// M11.D39g's endpoint-reuse row. Identity is the (worker id, generation) pair: the address is
/// not in the message and the worker id agrees in exactly the case that matters. The successor
/// has announced itself here, so the refusal cannot be the registration one — it is the target
/// comparison and nothing else.
#[tokio::test]
async fn a_reused_endpoint_refuses_its_predecessors_delayed_start() {
    let mut link = Link::to_registered_generation(false);
    // Addressed to the generation that is at the endpoint *now*, and in flight when it dies.
    link.hold("start-to-generation-3", fenced_start(ATTEMPT, ISSUED_UNDER));

    link.endpoint_reused_by(GENERATION + 1);
    link.register_receiver(false);

    let refused = link
        .deliver_held("start-to-generation-3")
        .expect_err("a successor generation does not answer for its predecessor");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        refused.message(),
        format!(
            "request is addressed to worker {WORKER} generation {GENERATION}, and this is \
             worker {WORKER} generation {}",
            GENERATION + 1
        ),
    );
    assert!(link.idle());
    assert_eq!(link.tracked(), 0);

    // The successor answers for its own generation, under the same fence, normally.
    assert_eq!(
        link.deliver(addressed_start(
            ATTEMPT,
            ISSUED_UNDER,
            WORKER,
            GENERATION + 1
        ))
        .unwrap(),
        settlement(ISSUED_UNDER, StartExecutionOutcome::Applied),
    );
}

/// An unregistered generation refuses every fenced directive and still admits the legacy one.
///
/// M11.D39g's incapable/unregistered peer, scoped as M11.T26c scopes it: unregistered is in the
/// *post*-flag-day fail-closed set, so the fence-less pre-flag-day route stays open in the
/// window between a worker answering its port and announcing itself to a controller.
#[tokio::test]
async fn an_unregistered_generation_refuses_the_fenced_protocol_and_keeps_the_legacy_one() {
    let mut link = Link::to_unregistered_generation();

    let refused = link
        .deliver(fenced_start(ATTEMPT, ISSUED_UNDER))
        .expect_err("a fenced directive cannot precede the registration request");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        refused.message(),
        "Worker generation has not begun registration"
    );
    assert_eq!(link.acknowledged(), 0);

    assert_eq!(
        link.deliver(unfenced(ATTEMPT)).unwrap(),
        settlement(0, StartExecutionOutcome::Applied),
        "and the pre-flag-day shape is admitted byte-for-byte as it was before the fields \
         existed: refusing it here would turn a compatible increment into a live one"
    );
}

/// Post-flag-day skew fails closed, and the pre-flag-day window does not.
///
/// M11.D75's rollout contract, at the worker. The same fence-less directive — what a controller
/// predating M11.T26c sends — is admitted by a generation that has acknowledged nothing and
/// refused by one that has. Nothing between them changed except the generation's own strict
/// mode, which M11.D39e(i) makes monotone.
#[tokio::test]
async fn post_flag_day_skew_fails_closed_and_the_window_before_it_does_not() {
    let mut before = Link::to_registered_generation(false);
    assert_eq!(
        before
            .deliver_from_a_predecessor_controller(ATTEMPT)
            .unwrap(),
        settlement(0, StartExecutionOutcome::Applied),
        "before the flag day a legacy controller's start is the whole protocol"
    );

    let mut after = Link::to_registered_generation(false);
    assert_eq!(
        after.deliver(fence_only(REFUSAL_FENCE)).unwrap(),
        settlement(REFUSAL_FENCE, StartExecutionOutcome::FenceAcknowledged),
    );
    let refused = after
        .deliver_from_a_predecessor_controller(ATTEMPT)
        .expect_err("after the flag day a fence-less directive is unattributable");
    assert_eq!(refused.code(), Code::FailedPrecondition);
    assert_eq!(
        refused.message(),
        "Worker generation requires a lifecycle fence and this request carries none"
    );
    assert!(after.idle());
    assert_eq!(after.applied(), None);

    // The registration response is the other on-switch, and it is the one the flag day flips.
    let mut strict_from_registration = Link::to_registered_generation(true);
    assert_eq!(
        strict_from_registration
            .deliver_from_a_predecessor_controller(ATTEMPT)
            .expect_err("strict from registration refuses the same directive")
            .code(),
        Code::FailedPrecondition,
    );
}

/// Every fault M11.D39g declares for this side of the protocol has a named injection.
///
/// The enumeration is what stops the harness from quietly covering less than it claims: a
/// variant added to [`WorkerFault`] without an operation does not compile, and an injection
/// nothing calls fails the build under `-D warnings`. This reads the table back so that the
/// *names* are asserted too — an injection renamed without its declaration is the drift this
/// catches.
#[test]
fn every_declared_worker_fault_has_a_live_injection() {
    assert_eq!(
        WorkerFault::ALL.len(),
        8,
        "the declared worker-observable half of M11.D39g's fault model"
    );
    for fault in WorkerFault::ALL {
        let injection = fault.injection();
        assert!(
            injection.starts_with("Link::"),
            "{fault:?}: every injection is an operation on the link that delivers the \
             directive, not a mock that answers instead of the guard — got {injection:?}"
        );
    }
    assert_eq!(
        WorkerFault::ALL
            .iter()
            .map(|fault| fault.injection())
            .collect::<Vec<_>>(),
        vec![
            "Link::lose",
            "Link::duplicate",
            "Link::deliver_held_in_reverse",
            "Link::hold + Link::deliver_held",
            "Link::restart_generation",
            "Link::endpoint_reused_by",
            "Link::to_unregistered_generation",
            "Link::deliver_from_a_predecessor_controller",
        ],
        "and each fault keeps the injection the D39 registry names for it"
    );
}
