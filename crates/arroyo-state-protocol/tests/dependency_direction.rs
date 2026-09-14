//! The direction of the state/protocol edge (work-plan item M11.P49).
//!
//! `arroyo-state` depends on this crate so that its providers can implement the GC liveness
//! seam declared in [`arroyo_state_protocol::gc::liveness`]. The reverse edge must not exist:
//! this crate decides what a checkpoint garbage collection deletes, and a dependency on the
//! crate whose payloads it is deleting would put the backend formats back inside the protocol.
//!
//! # What this file proves, and what proves the rest
//!
//! The test below reads this crate's own manifest and checks that no dependency section
//! declares `arroyo-state`. That is the *direct* edge, and it is the one a future change would
//! add by hand.
//!
//! A transitive path through a *normal* dependency is proven absent by cargo rather than by
//! an assertion. Since `arroyo-state` declares a dependency on this crate, any normal-
//! dependency path from here back to it — direct, or through `arroyo-types`, `arroyo-rpc`,
//! `arroyo-storage`, or anything they pull — is a package dependency cycle, which cargo
//! refuses to resolve:
//!
//! ```text
//! error: cyclic package dependency: package `arroyo-state v0.16.0-dev` depends on itself
//! ```
//!
//! The workspace would not build and this test binary would not exist to run, so the edge
//! added in the other direction is itself what enforces the direction.
//!
//! Cargo's refusal has one gap, and it is the gap this test covers: a **dev-dependency** cycle
//! is legal, so `arroyo-state-protocol` could take a `[dev-dependencies]` edge on
//! `arroyo-state` and still build. That would put the payload formats back into this crate's
//! own test build, which is where a "just for the tests" decode starts. The scan below reads
//! every dependency section, not only `[dependencies]`.
//!
//! `cargo metadata` says the same thing about the normal-dependency half and is the honest
//! source for a human checking by hand; it is not shelled out to here, because a test that
//! runs cargo inside cargo is a slower and more fragile way of learning what the build already
//! refused to do — and note that `cargo metadata --no-deps` does not resolve at all, so it
//! reports no cycle.

/// This crate declares no dependency on `arroyo-state`, in any section.
///
/// Covers `[dependencies]`, `[dev-dependencies]`, `[build-dependencies]` and their
/// target-specific spellings, in both the inline (`arroyo-state = { ... }`) and table
/// (`[dependencies.arroyo-state]`) forms.
#[test]
fn the_protocol_crate_declares_no_dependency_on_arroyo_state() {
    const MANIFEST: &str = include_str!("../Cargo.toml");
    const FORBIDDEN: &str = "arroyo-state";

    let mut in_dependency_section = false;
    for line in MANIFEST.lines() {
        let line = line.trim();
        if let Some(header) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            // `[dependencies.arroyo-state]`, and the target-specific spellings of it.
            let names_forbidden_table = header.rsplit_once('.').is_some_and(|(section, name)| {
                section.ends_with("dependencies") && name == FORBIDDEN
            });
            assert!(
                !names_forbidden_table,
                "the protocol crate's manifest declares `{FORBIDDEN}` in `[{header}]`; the \
                 dependency runs from `arroyo-state` to this crate and not back"
            );
            in_dependency_section = header.ends_with("dependencies");
            continue;
        }

        if !in_dependency_section {
            continue;
        }

        let Some((key, _)) = line.split_once('=') else {
            continue;
        };
        assert_ne!(
            key.trim().trim_matches('"'),
            FORBIDDEN,
            "the protocol crate's manifest declares `{FORBIDDEN}` as a dependency; the \
             dependency runs from `arroyo-state` to this crate and not back"
        );
    }

    // The scan is worth what it reads: if the manifest ever stops containing the sections this
    // walks, the loop above passes vacuously. Pin that it saw a real manifest with real
    // dependency entries in it.
    assert!(
        MANIFEST.contains("[dependencies]") && MANIFEST.contains("arroyo-rpc"),
        "this test read something that is not this crate's manifest"
    );
}
