//! The two M11.D39b compile-time restrictions, as tests that actually run (D96 rows 12 and
//! 13).
//!
//! # What is compiled, and why it is the real thing
//!
//! Each fixture is a tiny crate compiled by a plain `rustc`, and the phase graph it is
//! compiled against is **`states/scheduling/phases.rs` itself**, included verbatim. That is
//! what [`phases`](super::phases) has no crate dependencies for: everything it needs from the
//! controller is an opaque type, so a stub environment ([`compile_fail/stub.rs`]) can supply
//! those and the file compiles standalone. The restrictions being tested — which methods exist
//! on which phase type, and whether an effect takes its receiver by value — are properties of
//! that file and of nothing else, so a stub cannot make one hold or fail.
//!
//! No dependency is added for this. `trybuild` is not in this workspace, and could not be used
//! here in any case: `states` is a private module, so an external fixture crate could not name
//! a phase type at all.
//!
//! # The three parts each row needs
//!
//! 1. a **positive** fixture that compiles, using the permitted API;
//! 2. a **negative** fixture that fails, and fails for the intended reason — matched on the
//!    diagnostic *category* (`error[E0382]`, `error[E0599]`) rather than on message text; and
//! 3. a **weakened-API mutation**, applied to a copy of `phases.rs` by exact textual
//!    replacement, under which the negative fixture *compiles* — which is what makes the
//!    restriction load-bearing rather than incidental. Every replacement asserts that its
//!    needle occurred exactly once, so a mutation cannot silently fail to apply and leave a
//!    weakening "proved" by an unchanged file.
//!
//! # Toolchain
//!
//! Diagnostic codes are a property of the compiler, so the harness pins the one it was
//! verified against. A failure here after a toolchain bump means "re-verify the categories",
//! not "the restriction is gone".

use std::path::{Path, PathBuf};
use std::process::Command;

/// The toolchain the diagnostic categories below were verified against.
const PINNED_RUSTC: &str = "1.96.0";

/// The phase graph under test: the real source, verbatim.
const PHASES: &str = include_str!("phases.rs");

/// The stub environment it is compiled against.
const STUB: &str = include_str!("compile_fail/stub.rs");

/// One exact textual weakening of the phase graph.
///
/// Each edit must match exactly once. Applying a mutation that matched nothing — because the
/// code it names has since been rewritten — would leave the negative fixture failing for the
/// original reason and the test passing for the wrong one, so the count is asserted rather
/// than assumed.
struct Weakening {
    what: &'static str,
    edits: &'static [(&'static str, &'static str)],
}

impl Weakening {
    fn apply(&self, source: &str) -> String {
        let mut out = source.to_string();
        for (needle, replacement) in self.edits {
            assert_eq!(
                out.matches(needle).count(),
                1,
                "the weakening `{}` expects to find exactly one occurrence of:\n{needle}\n\
                 It found a different number, so `phases.rs` has been rewritten and this \
                 mutation no longer says what it claims to. Re-derive it from the current \
                 source rather than deleting it.",
                self.what
            );
            out = out.replace(needle, replacement);
        }
        out
    }
}

/// The result of one `rustc` invocation.
struct Compilation {
    succeeded: bool,
    stderr: String,
}

impl Compilation {
    fn assert_compiles(&self, what: &str) {
        assert!(
            self.succeeded,
            "{what} was expected to compile, and did not. Either the permitted phase API has \
             changed or the stub environment no longer matches it — both are real failures, \
             because a stub that has drifted stops the negative fixtures covering anything.\n\
             {}",
            self.stderr
        );
    }

    fn assert_fails_with(&self, what: &str, code: &str, category: &str) {
        assert!(
            !self.succeeded,
            "{what} was expected not to compile, and did. The restriction it exists to prove is \
             gone.\n{}",
            self.stderr
        );
        assert!(
            self.stderr.contains(code),
            "{what} failed to compile, but not with {code}. The diagnostic *category* is the \
             claim — a failure for some other reason proves nothing about the restriction.\n{}",
            self.stderr
        );
        assert!(
            self.stderr.contains(category),
            "{what} failed with {code} but without `{category}` in the diagnostic, so it is not \
             the intended instance of that category.\n{}",
            self.stderr
        );
    }
}

/// A directory holding one fixture's sources, removed with the test.
struct FixtureDir(PathBuf);

impl FixtureDir {
    fn new(name: &str) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "arroyo-t25b-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        Self(directory)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for FixtureDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The workspace toolchain, asserted before any diagnostic category is relied on.
fn assert_pinned_toolchain() {
    let out = Command::new("rustc")
        .arg("--version")
        .output()
        .expect("rustc is on the PATH of anything that can run this test suite");
    let version = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        version.contains(PINNED_RUSTC),
        "the compile-fail rows pin rustc {PINNED_RUSTC}, and this is `{}`. Diagnostic codes \
         belong to the compiler, so re-verify that the negative fixtures still fail with the \
         categories named here before changing this constant.",
        version.trim()
    );
}

/// Compiles one fixture against `phases`, and reports what `rustc` said.
///
/// `phases` is written to a file of its own and `include!`d, which is also why its leading
/// `//!` module documentation is stripped: an inner attribute is not permitted where an
/// `include!` expands. Nothing else about the source is touched.
fn compile(name: &str, phases: &str, fixture: &str) -> Compilation {
    let dir = FixtureDir::new(name);
    let phases_path = dir.path().join("phases_under_test.rs");
    let body: String = phases
        .lines()
        .filter(|line| !line.trim_start().starts_with("//!"))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&phases_path, body).unwrap();

    let source = format!(
        "{}\n{fixture}\n",
        STUB.replace("@PHASES@", &phases_path.to_string_lossy())
    );
    let fixture_path = dir.path().join("fixture.rs");
    std::fs::write(&fixture_path, source).unwrap();

    let out = Command::new("rustc")
        .arg("--edition")
        .arg("2024")
        .arg("--crate-type")
        .arg("lib")
        .arg("--emit")
        .arg("metadata")
        .arg("--out-dir")
        .arg(dir.path())
        .arg(&fixture_path)
        .output()
        .expect("rustc could not be run");
    Compilation {
        succeeded: out.status.success(),
        stderr: String::from_utf8_lossy(&out.stderr).to_string(),
    }
}

/// The weakening that makes an irreversible effect borrow the admission instead of consuming
/// it.
///
/// The second edit is the driver, which has to keep compiling for the fixture to say anything
/// at all: it is the same call, adjusted to the borrowing signature, and nothing else.
const BORROWS_INSTEAD_OF_CONSUMING: Weakening = Weakening {
    what: "an irreversible effect borrows the admission instead of consuming it",
    edits: &[
        (
            "    pub(crate) async fn persist_generation(mut self) -> PreambleStep<'a, 'ctx> {\n\
             \x20       match self.ctx.persist_generation(&self.admission).await {\n\
             \x20           Ok(()) => Ok(self),\n\
             \x20           Err(reason) => Err(self.fence(reason)),\n\
             \x20       }\n\
             \x20   }",
            "    pub(crate) async fn persist_generation(&mut self) -> Result<(), StateError> {\n\
             \x20       self.ctx.persist_generation(&self.admission).await\n\
             \x20   }",
        ),
        (
            "    let preamble = preamble.persist_generation().await?;",
            "    let mut preamble = preamble;\n    let _ = preamble.persist_generation().await;",
        ),
    ],
};

/// The weakening that gives a token-free wait the admission, and with it an irreversible
/// effect.
const A_WAIT_KEEPS_THE_TOKEN: Weakening = Weakening {
    what: "the wait for workers keeps the admission and gains an irreversible effect",
    edits: &[
        (
            "pub(crate) struct AwaitingWorkers<'a, 'ctx> {\n    ctx: PhaseContext<'a, 'ctx>,\n}",
            "pub(crate) struct AwaitingWorkers<'a, 'ctx> {\n    ctx: PhaseContext<'a, 'ctx>,\n    \
             admission: Admission,\n}",
        ),
        (
            "        let Self { admission, mut ctx } = self;\n\
             \x20       drop(admission);\n\
             \x20       ctx.begin_wait();\n\
             \x20       AwaitingWorkers { ctx }",
            "        let Self { admission, mut ctx } = self;\n\
             \x20       ctx.begin_wait();\n\
             \x20       AwaitingWorkers { ctx, admission }",
        ),
        (
            "        let Self { mut ctx } = self;\n        if let Err(reason) = ctx.require_reconciling_workers() {",
            "        let Self { mut ctx, admission: _ } = self;\n        if let Err(reason) = ctx.require_reconciling_workers() {",
        ),
        (
            "impl<'a, 'ctx> AwaitingWorkers<'a, 'ctx> {\n",
            "impl<'a, 'ctx> AwaitingWorkers<'a, 'ctx> {\n    \
             pub(crate) async fn persist_generation(&mut self) -> Result<(), StateError> {\n        \
             self.ctx.persist_generation(&self.admission).await\n    }\n\n",
        ),
    ],
};

/// The weakening that gives a token-owning phase a wait on the job's channel.
const A_TOKEN_OWNING_PHASE_MAY_WAIT: Weakening = Weakening {
    what: "the preamble is given a wait on the job's channel while it holds the admission",
    edits: &[(
        "impl<'a, 'ctx> Preamble<'a, 'ctx> {\n",
        "impl<'a, 'ctx> Preamble<'a, 'ctx> {\n    \
         pub(crate) async fn await_message(&mut self) -> Result<PhaseWait, StateError> {\n        \
         self.ctx.await_message_from_workers().await\n    }\n\n",
    )],
};

/// D96 row 12 (R7): an irreversible effect consumes the job's admission, and a phase that does
/// not hold one has no irreversible effect at all.
#[test]
fn irreversible_phases_consume_admission() {
    assert_pinned_toolchain();

    compile(
        "positive-admission",
        PHASES,
        include_str!("compile_fail/positive_admission.rs"),
    )
    .assert_compiles("the permitted use of the phase API");

    compile(
        "negative-admission-reuse",
        PHASES,
        include_str!("compile_fail/negative_admission_reuse.rs"),
    )
    .assert_fails_with(
        "two irreversible effects from one admission",
        "error[E0382]",
        "use of moved value",
    );

    compile(
        "negative-admission-absent",
        PHASES,
        include_str!("compile_fail/negative_admission_absent.rs"),
    )
    .assert_fails_with(
        "an irreversible effect on a token-free phase",
        "error[E0599]",
        "no method named `persist_generation` found",
    );

    // And both restrictions are load-bearing: weaken the API in the one place each depends on,
    // and the fixture that proved it stops proving anything.
    compile(
        "weakened-admission-reuse",
        &BORROWS_INSTEAD_OF_CONSUMING.apply(PHASES),
        include_str!("compile_fail/negative_admission_reuse.rs"),
    )
    .assert_compiles(
        "the double-effect fixture, once `persist_generation` borrows the admission instead of \
         consuming it — which is what shows the by-value receiver is what rejects it",
    );

    compile(
        "weakened-admission-absent",
        &A_WAIT_KEEPS_THE_TOKEN.apply(PHASES),
        include_str!("compile_fail/negative_admission_absent.rs"),
    )
    .assert_compiles(
        "the effect-without-a-token fixture, once the wait for workers keeps the admission and \
         is given the effect — which is what shows the absence of the method on a token-free \
         type is what rejects it",
    );
}

/// D96 row 13 (R9): a phase that holds the admission cannot wait on the job's channel.
#[test]
fn token_owning_phase_cannot_recv() {
    assert_pinned_toolchain();

    compile(
        "positive-recv",
        PHASES,
        include_str!("compile_fail/positive_recv.rs"),
    )
    .assert_compiles("a token-free phase waiting on the job's channel");

    compile(
        "negative-recv",
        PHASES,
        include_str!("compile_fail/negative_recv.rs"),
    )
    .assert_fails_with(
        "a token-owning phase waiting on the job's channel",
        "error[E0599]",
        "no method named `await_message` found",
    );

    compile(
        "weakened-recv",
        &A_TOKEN_OWNING_PHASE_MAY_WAIT.apply(PHASES),
        include_str!("compile_fail/negative_recv.rs"),
    )
    .assert_compiles(
        "the waiting-preamble fixture, once the preamble is given a wait — which is what shows \
         the absence of one on a token-owning type is what rejects it",
    );
}
