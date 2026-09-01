//! Where this crate's half of the fence protocol is *wired*, pinned on source text (M11.T26e).
//!
//! The sibling of the controller's `states/lifecycle/wiring_tests.rs`, for the crate on the
//! other side of the wire — and the reason it exists separately rather than as more rows there
//! is that `include_str!` reaches only inside its own crate, so a pin written over there could
//! not see these files at all.
//!
//! Every row is a structural source pin and says so in its own name. What they have in common
//! is the failure no behavioural test can see: a second place that answers a question the
//! protocol requires one answer to. Under `LifecycleMode::LegacyT08` a duplicate answer is
//! usually the *same* answer — a hand-written zero for a fence, an authority minted unfenced
//! beside one that was handed down — so it passes every behavioural row in this build and
//! diverges only at the flag day.

/// Everything in a file before its test module, so a mention inside a test does not count.
fn production_half(source: &'static str) -> &'static str {
    match source.find("\n#[cfg(test)]") {
        Some(at) => &source[..at],
        None => source,
    }
}

/// This crate's production sources for the fence protocol's *wiring*.
///
/// Deliberately not `lifecycle_fence/guard.rs`: that module is the seam itself, and it is the
/// one place that legitimately writes a `StartExecutionResp`'s lifecycle fields and names a
/// `CommitAuthority` constructor. These three are its callers.
fn wiring_sources() -> Vec<(&'static str, &'static str)> {
    vec![
        ("lib.rs", production_half(include_str!("../lib.rs"))),
        (
            "job_controller/controller.rs",
            production_half(include_str!("../job_controller/controller.rs")),
        ),
        (
            "job_controller/model.rs",
            production_half(include_str!("../job_controller/model.rs")),
        ),
    ]
}

/// Nothing on a production path writes a lifecycle field of a message by hand.
///
/// **A structural source pin, and the name says so.** `arroyo_rpc::fence_wire` exists so that a
/// fence and the generation it addresses are one value; that guarantee is only as good as the
/// absence of a call site which sets `lifecycle_fence` and forgets `target_worker_generation`,
/// which proto3 cannot prevent and the type system cannot see, because the generated structs
/// have public fields.
///
/// Before M11.T26e all four of this crate's commit sites wrote three zeros by hand under a
/// comment promising this build sent no fenced commit — a comment asserting more than anything
/// enforced. This is what enforces it.
#[test]
fn no_worker_production_call_site_writes_a_lifecycle_field_by_hand() {
    for (file, source) in wiring_sources() {
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
                    // `requires_lifecycle_fence:` is the registration bool, which has no
                    // partner field and is read as one value.
                    if source[..*at].trim_end().ends_with("requires_") {
                        return false;
                    }
                    // `lifecycle_fence::guard` is a module path, not a field. The field is
                    // written with one colon; the path continues with a second.
                    !source[at + field.len()..].starts_with(':')
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
            "req.target_worker_id",
            "req.target_worker_generation",
        ] {
            assert_eq!(
                source.matches(read).count(),
                0,
                "{file}: `{read}` reads half of a statement the wire cannot make whole. A \
                 request is read through `fence_wire::start_directive` or `commit_directive`, \
                 each of which refuses the shapes that are neither"
            );
        }
    }
}

/// A worker leader never mints the authority it commits under; it is handed one.
///
/// **A structural source pin, and the name says so.** The authority a leader issues commits
/// under is the one its own admitted `StartExecution` conferred (M11.D39d), and the only route
/// it takes is `AppliedStart::start`'s argument. A `CommitAuthority::unfenced()` written
/// anywhere along the leader's initialization would be indistinguishable from that today —
/// under `LifecycleMode::LegacyT08` the conferred authority *is* unfenced — and would silently
/// keep sending fence-less commits after the flag day, which every strict worker generation
/// would then refuse.
#[test]
fn a_worker_leader_never_mints_its_own_commit_authority() {
    for (file, source) in wiring_sources() {
        assert_eq!(
            source.matches("CommitAuthority::").count(),
            0,
            "{file}: names a `CommitAuthority` constructor. The leader's authority arrives as a \
             parameter from the start that admitted it and is never built on the way"
        );
    }
}

/// Exactly one place in this crate builds a `CommitReq` to send.
///
/// **A structural source pin, and the name says so.** Four production sites used to build one
/// each, and a fifth — the replay path — cloned a single request for every worker, which cannot
/// be right once the request names the worker it is addressed to. They are now one function,
/// `model::addressed_commit`, which takes the authority and the worker together; a second
/// builder would be a second place that could forget to stamp, and stamping is what makes the
/// three fields one directive.
#[test]
fn one_place_in_this_crate_builds_a_commit_to_send() {
    let model = production_half(include_str!("../job_controller/model.rs"));
    assert_eq!(
        model.matches("let mut req = CommitReq {").count(),
        1,
        "model.rs builds the one outbound commit, in `addressed_commit`"
    );
    assert_eq!(
        model.matches(".stamp(&mut req)").count(),
        1,
        "and stamps it exactly once, with the directive its authority gives for that worker"
    );
    for (file, source) in wiring_sources() {
        if file == "job_controller/model.rs" {
            continue;
        }
        assert_eq!(
            source.matches("CommitReq {").count(),
            0,
            "{file}: builds a commit of its own rather than addressing one through the model"
        );
    }
}
