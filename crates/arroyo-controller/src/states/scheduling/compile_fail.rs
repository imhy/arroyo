//! The M11.D39b compile-time restrictions, as tests that actually run (D96 rows 12 and 13),
//! and the indivisible-obligation restriction beside them (review comment `5369004357`).
//!
//! # What is compiled, and why it is the real thing
//!
//! Each fixture is a tiny crate compiled by a plain `rustc`, and what it is compiled against
//! is **the real source file**, included verbatim: `states/scheduling/phases.rs` for the two
//! phase rows, and `states/scheduling/fanout/settlement.rs` for the third. That is what those
//! files have so few crate dependencies for: what they need from the controller is opaque, so
//! a stub environment ([`compile_fail/stub.rs`] and [`compile_fail/settlement_stub.rs`]) can
//! supply it and the file compiles standalone. The restrictions being tested — which methods
//! exist on which type, whether an effect takes its receiver by value, and which operations
//! are reachable from outside a module — are properties of those files and of nothing else, so
//! a stub cannot make one hold or fail.
//!
//! No dependency is added for this. `trybuild` is not in this workspace, and could not be used
//! here in any case: `states` is a private module, so an external fixture crate could not name
//! a phase type at all.
//!
//! # The three parts each row needs
//!
//! 1. a **positive** fixture that compiles, using the permitted API;
//! 2. a **negative** fixture that fails, and fails for the intended reason — matched on the
//!    diagnostic *category* (`error[E0382]`, `error[E0599]`, `error[E0624]`) rather than on
//!    message text; and
//! 3. a **weakened-API mutation**, applied to a copy of the file under test by exact textual
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
use std::sync::LazyLock;

/// The toolchain the diagnostic categories below were verified against.
const PINNED_RUSTC: &str = "1.96.0";

/// The phase graph under test: the real source, verbatim — both halves of it.
///
/// `phases.rs` was split at PR #160 review comment `5384870087`, which took it past the plan's
/// 500-line production bar. These rows compile the graph as one standalone file against a stub
/// environment, so the two halves are rejoined here exactly as the compiler sees them: the
/// typestates without the `mod driver;` that a standalone file cannot resolve, and the driver
/// without the `use super::*;` that would then have no parent. Rejoining rather than compiling
/// only one half is the point — the mutations below weaken code in both.
static PHASES: LazyLock<String> = LazyLock::new(|| {
    let typestates = include_str!("phases.rs")
        .replace("\nmod driver;\n", "\n")
        .replace("\npub(crate) use driver::schedule;\n", "\n");
    let driver = include_str!("phases/driver.rs").replace("\nuse super::*;\n", "\n");
    format!("{typestates}\n{driver}")
});

/// The stub environment it is compiled against.
const STUB: &str = include_str!("compile_fail/stub.rs");

/// The transfer seam under test: the real source, verbatim.
const SETTLEMENT: &str = include_str!("fanout/settlement.rs");

/// The stub environment *it* is compiled against.
///
/// A second stub rather than a second use of the first: `stub.rs` supplies an opaque
/// `SettlementBundle` so that `phases.rs` can be compiled without the seam, and the row below
/// needs the opposite — the seam's real source, and stubs for what it stands on.
const SETTLEMENT_STUB: &str = include_str!("compile_fail/settlement_stub.rs");

/// One exact textual weakening of a file under test.
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
                 It found a different number, so the file under test has been rewritten and \
                 this mutation no longer says what it claims to. Re-derive it from the current \
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
            "{what} was expected to compile, and did not. Either the permitted API has changed \
             or the stub environment no longer matches it — both are real failures, because a \
             stub that has drifted stops the negative fixtures covering anything.\n\
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
fn compile(name: &str, phases: &str, fixture: &str) -> Compilation {
    compile_against(name, STUB, "@PHASES@", phases, fixture)
}

/// Compiles one fixture against the transfer seam, and reports what `rustc` said.
fn compile_settlement(name: &str, settlement: &str, fixture: &str) -> Compilation {
    compile_against(name, SETTLEMENT_STUB, "@SETTLEMENT@", settlement, fixture)
}

/// Compiles `fixture` against `under_test` placed into `stub` at `placeholder`.
///
/// `under_test` is written to a file of its own and `include!`d, which is also why its leading
/// `//!` module documentation is stripped: an inner attribute is not permitted where an
/// `include!` expands. Nothing else about the source is touched.
fn compile_against(
    name: &str,
    stub: &str,
    placeholder: &str,
    under_test: &str,
    fixture: &str,
) -> Compilation {
    let dir = FixtureDir::new(name);
    let under_test_path = dir.path().join("source_under_test.rs");
    let body: String = under_test
        .lines()
        .filter(|line| !line.trim_start().starts_with("//!"))
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&under_test_path, body).unwrap();

    let source = format!(
        "{}\n{fixture}\n",
        stub.replace(placeholder, &under_test_path.to_string_lossy())
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
        PHASES.as_str(),
        include_str!("compile_fail/positive_admission.rs"),
    )
    .assert_compiles("the permitted use of the phase API");

    compile(
        "negative-admission-reuse",
        PHASES.as_str(),
        include_str!("compile_fail/negative_admission_reuse.rs"),
    )
    .assert_fails_with(
        "two irreversible effects from one admission",
        "error[E0382]",
        "use of moved value",
    );

    compile(
        "negative-admission-absent",
        PHASES.as_str(),
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
        &BORROWS_INSTEAD_OF_CONSUMING.apply(PHASES.as_str()),
        include_str!("compile_fail/negative_admission_reuse.rs"),
    )
    .assert_compiles(
        "the double-effect fixture, once `persist_generation` borrows the admission instead of \
         consuming it — which is what shows the by-value receiver is what rejects it",
    );

    compile(
        "weakened-admission-absent",
        &A_WAIT_KEEPS_THE_TOKEN.apply(PHASES.as_str()),
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
        PHASES.as_str(),
        include_str!("compile_fail/positive_recv.rs"),
    )
    .assert_compiles("a token-free phase waiting on the job's channel");

    compile(
        "negative-recv",
        PHASES.as_str(),
        include_str!("compile_fail/negative_recv.rs"),
    )
    .assert_fails_with(
        "a token-owning phase waiting on the job's channel",
        "error[E0599]",
        "no method named `await_message` found",
    );

    compile(
        "weakened-recv",
        &A_TOKEN_OWNING_PHASE_MAY_WAIT.apply(PHASES.as_str()),
        include_str!("compile_fail/negative_recv.rs"),
    )
    .assert_compiles(
        "the waiting-preamble fixture, once the preamble is given a wait — which is what shows \
         the absence of one on a token-owning type is what rejects it",
    );
}

/// The weakening that publishes the operation separating a bundle's two halves.
const INTO_PARTS_IS_REACHABLE: Weakening = Weakening {
    what: "an owner can take the obligation apart",
    edits: &[(
        "    fn into_parts(mut self) -> (Admission, IssuedAttempts) {",
        "    pub fn into_parts(mut self) -> (Admission, IssuedAttempts) {",
    )],
};

/// The same weakening under the sibling name, because closing one door and leaving the other
/// open would have closed nothing.
const KEEP_IS_REACHABLE: Weakening = Weakening {
    what: "an owner can take the obligation apart under its second name",
    edits: &[(
        "    fn keep(self) -> (Admission, IssuedAttempts) {",
        "    pub fn keep(self) -> (Admission, IssuedAttempts) {",
    )],
};

/// An owner cannot keep one half of an obligation and be issued a receipt for it
/// (review comment `5369004357`).
///
/// The finding: acceptance is observed through the bundle's `Drop`, and `into_parts` clears
/// the field that `Drop` reads. So an owner could take the obligation apart, drop the
/// `Admission` — opening the job's publication lock — or drop the `IssuedAttempts` — leaving
/// nobody with a record of what was owed — return `Ok(())`, and be issued a
/// `SettlementReceipt`. Fencing then counted those attempts as somebody's when they were
/// nobody's.
///
/// The answer is that the halves are no longer separable outside the module that observes the
/// separation, so this is a compile-time row rather than a behavioural one: the state the
/// finding describes is not reachable at runtime for a test to assert about. The behavioural
/// half is `phase_tests::a_receipt_is_issued_only_where_the_publication_lock_did_not_come_back`,
/// which asserts the agreement across every way of parting with an obligation that is left.
#[test]
fn an_owner_cannot_keep_half_an_obligation() {
    assert_pinned_toolchain();

    compile_settlement(
        "positive-settlement",
        SETTLEMENT,
        include_str!("compile_fail/positive_settlement.rs"),
    )
    .assert_compiles("an owner that keeps the whole obligation and reads what it lists");

    compile_settlement(
        "negative-settlement-into-parts",
        SETTLEMENT,
        include_str!("compile_fail/negative_settlement_into_parts.rs"),
    )
    .assert_fails_with(
        "an owner that keeps the authority and drops the inventory",
        "error[E0624]",
        "into_parts",
    );

    compile_settlement(
        "negative-settlement-keep",
        SETTLEMENT,
        include_str!("compile_fail/negative_settlement_keep.rs"),
    )
    .assert_fails_with(
        "an owner that keeps the inventory and drops the authority",
        "error[E0624]",
        "keep",
    );

    // And each restriction is load-bearing on its own name: publish one separator and the
    // fixture that used it stops proving anything, while the fixture that used the other still
    // does. That is what makes this two closed doors rather than one.
    compile_settlement(
        "weakened-settlement-into-parts",
        &INTO_PARTS_IS_REACHABLE.apply(SETTLEMENT),
        include_str!("compile_fail/negative_settlement_into_parts.rs"),
    )
    .assert_compiles(
        "the authority-only fixture, once `into_parts` is reachable from an owner — which is \
         what shows its privacy is what rejects it",
    );

    compile_settlement(
        "weakened-settlement-keep",
        &KEEP_IS_REACHABLE.apply(SETTLEMENT),
        include_str!("compile_fail/negative_settlement_keep.rs"),
    )
    .assert_compiles(
        "the inventory-only fixture, once `keep` is reachable from an owner — which is what \
         shows that closing `into_parts` alone would have left the same release available \
         under another name",
    );

    compile_settlement(
        "weakened-settlement-crossed",
        &KEEP_IS_REACHABLE.apply(SETTLEMENT),
        include_str!("compile_fail/negative_settlement_into_parts.rs"),
    )
    .assert_fails_with(
        "the authority-only fixture under the *other* name's weakening, which must still be \
         rejected — a weakening that made both fixtures compile would prove neither door",
        "error[E0624]",
        "into_parts",
    );
}
