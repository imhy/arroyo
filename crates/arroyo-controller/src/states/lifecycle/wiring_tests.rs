//! Where this crate's half of the fence protocol is *wired*, pinned on source text (M11.T26c).
//!
//! Every row here is a structural source pin and says so in its own name. What they have in
//! common is the failure no behavioural test can see: a second place that answers a question the
//! protocol requires one answer to. Under `LifecycleMode::LegacyT08` a duplicate answer is
//! usually the *same* answer — a literal `false` for the registration flag, a hand-written zero
//! for a fence — so it passes every row in this build and only diverges at the flag day, which is
//! the one moment nobody is in a position to notice.

// ---------------------------------------------------------------------------------------------
// Source pins: the flag day and the wire fields have one writer each
// ---------------------------------------------------------------------------------------------

/// Everything in a file before its test module, so a mention inside a test does not count.
fn production_half(source: &'static str) -> &'static str {
    match source.find("\n#[cfg(test)]") {
        Some(at) => &source[..at],
        None => source,
    }
}

/// The controller's production sources for this crate's half of the wire protocol.
fn protocol_production_sources() -> Vec<(&'static str, &'static str)> {
    vec![
        ("lib.rs", production_half(include_str!("../../lib.rs"))),
        (
            "job_controller/mod.rs",
            production_half(include_str!("../../job_controller/mod.rs")),
        ),
        ("states/mod.rs", production_half(include_str!("../mod.rs"))),
        (
            "states/scheduling.rs",
            production_half(include_str!("../scheduling.rs")),
        ),
        (
            "states/scheduling/fanout.rs",
            production_half(include_str!("../scheduling/fanout.rs")),
        ),
        (
            "states/lifecycle/handshake.rs",
            production_half(include_str!("handshake.rs")),
        ),
    ]
}

/// The registration response is derived from the mode and never written as a literal.
///
/// **A structural source pin, and the name says so.** The behaviour is covered by
/// `the_registration_response_is_derived_from_the_mode_in_both_modes`; what no test of behaviour
/// can notice is a *second* place that answers the flag-day question, or the one place going back
/// to a literal — and a literal `false` there passes every test in this build, because
/// `LifecycleMode::SELECTED` answers `false` too. It would then survive the activation change
/// silently, leaving a fence-capable controller telling its workers not to require a fence.
#[test]
fn the_registration_response_names_the_mode_and_not_a_literal() {
    let lib = production_half(include_str!("../../lib.rs"));
    assert_eq!(
        lib.matches("requires_lifecycle_fence").count(),
        2,
        "the field is named exactly twice on the production path: once as the response's field \
         and once as the mode's answer to it"
    );
    assert!(
        lib.contains(
            "requires_lifecycle_fence: LifecycleMode::SELECTED.requires_lifecycle_fence()"
        ),
        "and the answer is the selected mode's, so activation flips it with everything else"
    );
    for literal in [
        "requires_lifecycle_fence: false",
        "requires_lifecycle_fence: true",
    ] {
        assert_eq!(
            lib.matches(literal).count(),
            0,
            "`{literal}` would answer the flag-day question without consulting the mode"
        );
    }
}

/// Nothing on a production path writes a lifecycle field of a request by hand.
///
/// **A structural source pin, and the name says so.** `arroyo_rpc::fence_wire` exists so that a
/// fence and the generation it addresses are one value; that guarantee is only as good as the
/// absence of a call site which sets `lifecycle_fence` and forgets `target_worker_generation`,
/// which proto3 cannot prevent and which the type system cannot see, because the generated
/// structs have public fields.
///
/// So the writers are counted. Every lifecycle field of every outbound message this crate sends
/// is written by a `stamp`, and this fails the moment a second writer appears — which is the
/// residual M11.T26c inherited from the wire half and the point at which it closes.
///
/// The other half of the workspace is pinned the same way by
/// `arroyo_worker::lifecycle_fence::wiring_tests::no_worker_production_call_site_writes_a_lifecycle_field_by_hand`,
/// which is a separate row rather than more sources here because `include_str!` reaches only
/// inside its own crate.
#[test]
fn no_production_call_site_writes_a_lifecycle_field_by_hand() {
    for (file, source) in protocol_production_sources() {
        for field in [
            "lifecycle_fence:",
            "target_worker_id:",
            "target_worker_generation:",
            "lifecycle_operation:",
            "revoked_execution_ids:",
            "observed_lifecycle_fence:",
        ] {
            let by_hand = source
                .match_indices(field)
                .filter(|(at, _)| {
                    // `requires_lifecycle_fence:` is the registration bool, which has no partner
                    // field and is pinned by the row above.
                    if source[..*at].trim_end().ends_with("requires_") {
                        return false;
                    }
                    // `revoked_execution_ids` names a field of both `StartExecutionReq` and
                    // `StartDirective::Fenced`, and *building a directive* is what this module
                    // is for. The two are told apart by the type rather than by the file: the
                    // message's field is a `Vec<String>` and the directive's is a slice
                    // reference, so only the latter is ever written as `: &`.
                    !source[at + field.len()..].starts_with(" &")
                })
                .count();
            assert_eq!(
                by_hand, 0,
                "{file}: `{field}` is written by hand. Every lifecycle field of an outbound \
                 message is written together, by `StartDirective::stamp` or \
                 `CommitDirective::stamp`, so that a fence and the generation it addresses \
                 cannot come from different decisions"
            );
        }
        for read in [
            "req.lifecycle_fence",
            "request.lifecycle_fence",
            ".observed_lifecycle_fence",
            "resp.outcome",
            "response.outcome",
        ] {
            assert_eq!(
                source.matches(read).count(),
                0,
                "{file}: `{read}` reads half of a statement the wire cannot make whole. A \
                 response is read through `fence_wire::observed_settlement` and a request \
                 through `start_directive`, each of which refuses the shapes that are neither"
            );
        }
    }
}

/// The fan-out's retry table is the classification, and nothing else.
///
/// **A structural source pin, and the name says so.** The landed table was a `matches!` over five
/// `Code` variants inside the request loop. What no behavioural test can notice is a second one
/// appearing beside it — a special case for one code, added where the loop already branches —
/// which would put `Aborted`'s reading in two places and let the flag day move only one of them.
#[test]
fn the_fan_out_reads_its_retry_table_from_the_classification() {
    let scheduling = production_half(include_str!("../scheduling.rs"));
    assert_eq!(
        scheduling.matches("Code::").count(),
        0,
        "no gRPC status code is named in the fan-out: the definitive/ambiguous decision is \
         `transport_settlement`'s, taken once and exhaustively"
    );
    assert_eq!(
        scheduling.matches("transport_settlement(").count(),
        1,
        "and the fan-out consults it exactly once, for the protocol its targets were addressed \
         under"
    );
    assert_eq!(
        scheduling.matches("observed_settlement(").count(),
        1,
        "and every `StartExecutionResp` it receives goes through the seam that reads one, \
         exactly once: a second reading is a second place that could decide a fence \
         acknowledgement was an application"
    );
}

/// The fence handshake is an irreversible effect, and it is inventoried as one.
///
/// **A structural source pin, and the name says so.** `Scheduling`'s own effect inventory —
/// `the_source_of_scheduling_next_keeps_every_irreversible_effect_inside_an_admitted_region` —
/// reads `states/scheduling.rs`, and the M11.D39b fan-out lives next door in
/// `scheduling/fanout.rs`, so an effect added there is in no inventory at all. This is that
/// inventory for the file M11.T26c adds one to.
///
/// Advancing a worker generation's fence is irreversible in the sense that matters: afterwards
/// the generation is in strict mode and refuses everything older, so it is exactly the kind of
/// thing a refusal published concurrently must not race. The reading of a failure here is the
/// same as of the landed pin's: not "the test is stale" but "say which region this belongs in".
#[test]
fn every_effect_of_the_fan_out_is_named_and_admitted() {
    let source = production_half(include_str!("../scheduling/fanout.rs"));
    let mut effects: Vec<&str> = source
        .match_indices(".effect(")
        .map(|(i, _)| {
            let rest = &source[i + ".effect(".len()..];
            let name = &rest[rest.find('"').expect("an effect is named") + 1..];
            &name[..name.find('"').expect("an unterminated effect name")]
        })
        .collect();
    effects.sort_unstable();
    assert_eq!(
        effects,
        ["advance the lifecycle fence on every worker generation"],
        "the fan-out's own effects. The `StartExecution` requests are not among them because \
         they are issued inside `start_execution_on_workers`, which the landed inventory covers"
    );
    assert_eq!(
        source.matches("advance_fence(").count(),
        1,
        "and the handshake is performed in exactly one place, inside that effect: a second \
         caller could advance a generation's fence outside the admitted region"
    );
    assert_eq!(
        source.matches("tokio::spawn").count(),
        0,
        "nothing here is spawned. A spawned effect outlives the admission that authorised it, \
         because dropping its handle detaches the task rather than cancelling it"
    );
}

/// The controller's commits are addressed by the authority its scheduling attempt established,
/// and by nothing it mints itself.
///
/// **A structural source pin, and the name says so.** The job's `FenceProtocol` is the one place
/// a fence for this job comes from, and `JobController` now holds no protocol of its own — the
/// authority lives on the model, where both of this topology's commit fan-outs read it. A
/// `CommitAuthority::unfenced()` written beside that would have been the same value before the
/// flag day and would have gone on sending fence-less commits after it — which is the defect
/// this pin exists for, and the reason it outlives the activation change.
#[test]
fn the_controllers_commit_authority_comes_from_the_jobs_fence_protocol() {
    let job_controller = production_half(include_str!("../../job_controller/mod.rs"));
    assert_eq!(
        job_controller
            .matches("commit_authority: fence_protocol.commit_authority()")
            .count(),
        1,
        "the model's authority is the job's protocol, named once"
    );
    assert_eq!(
        job_controller.matches("CommitAuthority::").count(),
        0,
        "and this file mints none of its own"
    );
    assert_eq!(
        job_controller.matches("CommitReq").count(),
        0,
        "nor does it build a commit: `RunningJobModel::commit_to_workers` addresses every one"
    );
}
