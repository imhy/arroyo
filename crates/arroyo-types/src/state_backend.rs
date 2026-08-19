//! Explicit, persisted state-backend selection (design item M11.D13b).
//!
//! A job selects exactly **one** state backend. That choice is persisted with the job,
//! transported through worker/leader start and task initialization, copied into every
//! `TableConfig` the job's operators create, and recorded in the job's checkpoint
//! metadata. No ambient or process-global selector exists: every create, merge, commit,
//! and cleanup reads the explicit value it was handed and validates it against the job's
//! authoritative selection *before* doing any backend-specific work.
//!
//! Three rules keep old data working while making new data unambiguous:
//!
//! 1. **Empty means parquet.** The database column (`TEXT NOT NULL DEFAULT ''`) and a
//!    protobuf `string` field share one default — the empty string — so job rows, start
//!    requests, table configs, and checkpoints written before the selector existed decode
//!    as [`StateBackendSelector::Parquet`]. [`StateBackendSelector::normalize`] is the
//!    single place in the system where that mapping lives.
//! 2. **Anything else unrecognized is a typed error**, never a silent fallback: a job
//!    must never be quietly sent to a backend other than the one it asked for.
//! 3. **One selector per job.** Mixed-backend jobs are rejected, as is any disagreement
//!    between the job value and a table config or a restored checkpoint.
//!    [`validate_agreement`] is the single implementation of that check; callers adapt
//!    their own inputs into `(label, raw value)` pairs. The value is also fixed for the
//!    life of the job: a configuration that changes it on an existing job is refused by
//!    [`validate_unchanged_job_selector`], because the running workers, their table
//!    configs, and every checkpoint they have written already carry the old value. What a
//!    running job is actually using is asked of the job's own execution — its persisted
//!    execution record, and [`validate_leader_selector`] for a live worker leader — never
//!    of the configuration row, which an operator can edit at any time.

use std::fmt;
use thiserror::Error;

/// The state backend a job stores its operator state in.
///
/// The two variants are the only valid values; there is deliberately no "unknown" or
/// "other" variant, so an unrecognized string cannot be represented and must instead be
/// rejected by [`StateBackendSelector::normalize`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StateBackendSelector {
    /// Arroyo's built-in parquet backing store: the backend every job used before the
    /// selector existed, and the value that absent or empty fields normalize to.
    Parquet,
    /// The stateengine backend. Values parse, persist, and transport from M11.T08
    /// onwards; the provider that serves them is installed in M11.T11.
    StateEngine,
}

impl StateBackendSelector {
    /// The selector for data that carries no explicit value.
    ///
    /// This is the compatibility guarantee for everything written before the selector
    /// existed, and it is the *only* place the "empty means parquet" default is spelled
    /// in Rust — SQL migrations use `DEFAULT ''`, never `DEFAULT 'parquet'`.
    pub const DEFAULT: Self = Self::Parquet;

    /// The persisted and transported spelling of this selector.
    ///
    /// This is the exact string stored in the database, in `TableConfig`, and in
    /// checkpoint metadata; it round-trips through [`StateBackendSelector::normalize`].
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::StateEngine => "stateengine",
        }
    }

    /// Normalizes a raw persisted or transported selector value.
    ///
    /// `raw` is the value exactly as it was read from a database row, a protobuf field,
    /// or a request; the empty string — which is both the SQL and the protobuf default,
    /// and therefore what absent values decode as — normalizes to [`Self::DEFAULT`].
    /// `label` names where the value came from (`"job"`, a table name, a checkpoint
    /// description); it is used only to build the error message, so an operator can
    /// diagnose the failure from a single log line.
    ///
    /// Matching is **exact**: neither surrounding whitespace nor a different letter case
    /// is accepted, because these values are machine-written (migration default,
    /// protobuf default, and this module's [`Self::as_str`]) rather than hand-typed.
    /// This follows the strict spelling comparisons the rest of this crate uses for
    /// persisted enums, and it surfaces a producer bug immediately instead of laundering
    /// several spellings of the same backend into the persisted record.
    ///
    /// # Errors
    ///
    /// Returns [`StateBackendError::UnknownValue`] for any value that is not `""`,
    /// `"parquet"`, or `"stateengine"`. An unrecognized value is never defaulted.
    pub fn normalize(raw: &str, label: &str) -> Result<Self, StateBackendError> {
        Self::try_from_raw(raw).ok_or_else(|| StateBackendError::UnknownValue {
            label: label.to_string(),
            value: raw.to_string(),
        })
    }

    /// Exact-match parse shared by [`Self::normalize`] and [`validate_agreement`], which
    /// builds a differently-labelled error.
    fn try_from_raw(raw: &str) -> Option<Self> {
        match raw {
            "" => Some(Self::DEFAULT),
            "parquet" => Some(Self::Parquet),
            "stateengine" => Some(Self::StateEngine),
            _ => None,
        }
    }
}

impl Default for StateBackendSelector {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl fmt::Display for StateBackendSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What kind of thing a value validated by [`validate_agreement`] was read from.
///
/// It selects the typed error a disagreement produces and the noun the message uses, so
/// one validation implementation serves both table configs and restored checkpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SelectorScope {
    /// A per-table copy of the selector, i.e. `TableConfig.state_backend`.
    Table,
    /// The selector recorded in checkpoint metadata a job is restoring from.
    Checkpoint,
    /// The selector a live worker leader reports for the job it is running, i.e. the
    /// value it was handed in its own `StartExecutionReq`.
    Leader,
}

impl SelectorScope {
    /// The noun used to describe this scope in error messages.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Checkpoint => "restored checkpoint",
            Self::Leader => "worker leader for",
        }
    }

    /// Renders `label` with its scope noun, e.g. `table "w:window"`. Only called on the
    /// error path, so validation of agreeing values allocates nothing.
    fn describe(self, label: &str) -> String {
        format!("{} {label:?}", self.as_str())
    }

    /// Builds the disagreement error belonging to this scope.
    fn mismatch(
        self,
        label: &str,
        found: StateBackendSelector,
        job: StateBackendSelector,
    ) -> StateBackendError {
        let label = self.describe(label);
        match self {
            Self::Table => StateBackendError::TableMismatch { label, found, job },
            Self::Checkpoint => StateBackendError::CheckpointMismatch { label, found, job },
            Self::Leader => StateBackendError::LeaderMismatch { label, found, job },
        }
    }
}

/// Checks that every `(label, raw value)` entry agrees with the job's authoritative
/// selector, normalizing each value on the way.
///
/// `job` is the selector persisted with the job; `scope` says whether the entries came
/// from table configs or from restored checkpoint metadata. Entries are borrowed pairs
/// of a label identifying the value (a table name, a checkpoint description) and the raw
/// string as persisted, so callers adapt their own types with a `map` and this function
/// allocates only when it fails. An empty iterator succeeds: a job with no tables does
/// not violate anything.
///
/// Validation is fail-fast — the first entry that violates a rule is reported, and later
/// entries are not inspected. An entry that conflicts with an *earlier, agreeing* entry
/// is reported as [`StateBackendError::MixedSelectors`], because the two entries prove
/// the job mixes backends; otherwise it is reported as the disagreement with the job.
///
/// # Errors
///
/// - [`StateBackendError::UnknownValue`] if an entry's value is not `""`, `"parquet"`,
///   or `"stateengine"`.
/// - [`StateBackendError::MixedSelectors`] if two entries select different backends.
/// - [`StateBackendError::TableMismatch`] or [`StateBackendError::CheckpointMismatch`],
///   per `scope`, if an entry disagrees with `job`.
pub fn validate_agreement<'a, 'b, I>(
    job: StateBackendSelector,
    scope: SelectorScope,
    entries: I,
) -> Result<(), StateBackendError>
where
    I: IntoIterator<Item = (&'a str, &'b str)>,
{
    let mut agreeing: Option<&str> = None;

    for (label, raw) in entries {
        let found = StateBackendSelector::try_from_raw(raw).ok_or_else(|| {
            StateBackendError::UnknownValue {
                label: scope.describe(label),
                value: raw.to_string(),
            }
        })?;

        if found == job {
            agreeing.get_or_insert(label);
            continue;
        }

        return Err(match agreeing {
            Some(other) => StateBackendError::MixedSelectors {
                label: scope.describe(label),
                value: found,
                other_label: scope.describe(other),
                other_value: job,
            },
            None => scope.mismatch(label, found, job),
        });
    }

    Ok(())
}

/// A rejected state-backend selector.
///
/// Every variant names the offending value(s) and the label of whatever carried them, so
/// the failure is diagnosable from one log line. These are hard failures: they are
/// raised before any create, merge, commit, or cleanup runs, and never downgraded to a
/// default.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StateBackendError {
    /// A value that is neither empty nor a known backend name.
    #[error(
        "unknown state backend {value:?} for {label}; valid values are \"parquet\" and \
         \"stateengine\", and an empty value means \"parquet\""
    )]
    UnknownValue {
        /// Where the value came from, e.g. `"job"` or `table "w:window"`.
        label: String,
        /// The rejected value, exactly as it was read.
        value: String,
    },

    /// Two of a job's values select different backends. M11 permits one selector per job.
    #[error(
        "{label} selects state backend \"{value}\", but {other_label} selects \
         \"{other_value}\"; a job must use a single state backend"
    )]
    MixedSelectors {
        /// The entry that broke the agreement.
        label: String,
        /// The backend that entry selects.
        value: StateBackendSelector,
        /// An earlier entry that agreed with the job's selector.
        other_label: String,
        /// The backend that earlier entry — and the job — select.
        other_value: StateBackendSelector,
    },

    /// A checkpoint being restored was written with a different backend than the job
    /// selects; its state cannot be read by the selected backend.
    #[error("{label} was written with state backend \"{found}\", but the job selects \"{job}\"")]
    CheckpointMismatch {
        /// Which checkpoint disagreed.
        label: String,
        /// The backend recorded in the checkpoint metadata.
        found: StateBackendSelector,
        /// The job's authoritative selector.
        job: StateBackendSelector,
    },

    /// A table config disagrees with the job that owns it.
    #[error("{label} selects state backend \"{found}\", but the job selects \"{job}\"")]
    TableMismatch {
        /// Which table disagreed.
        label: String,
        /// The backend recorded in the table config.
        found: StateBackendSelector,
        /// The job's authoritative selector.
        job: StateBackendSelector,
    },

    /// The live worker leader of a job reports a different backend than the controller
    /// intends to administer it with. See [`validate_leader_selector`].
    #[error(
        "{label} is running with state backend \"{found}\", but this controller would \
         administer it as \"{job}\"; the leader's value is the one its workers, table \
         configs, and checkpoints were built with"
    )]
    LeaderMismatch {
        /// Which job's leader disagreed, e.g. `worker leader for "job_abc"`.
        label: String,
        /// The backend the live leader reports.
        found: StateBackendSelector,
        /// The backend this controller recovered for the job.
        job: StateBackendSelector,
    },

    /// A job that is already running was handed a different selector than the one it
    /// started with. See [`validate_unchanged_job_selector`].
    #[error(
        "{label} is running with state backend \"{running}\", but its configuration now \
         says \"{requested}\"; a job's state backend cannot be changed while it exists — \
         stop the job and create a new one, or restore the previous value. Either remedy \
         works with this value still in place: a stop requested alongside it is still \
         carried out, under the backend the job is running with"
    )]
    JobSelectorChanged {
        /// Which job's selector changed, e.g. `job "job_abc"`.
        label: String,
        /// The selector the job's workers, checkpoints, and cleanup are using.
        running: StateBackendSelector,
        /// The selector the new configuration asks for.
        requested: StateBackendSelector,
    },
}

/// Checks that a job's selector has not changed underneath a job that already exists.
///
/// Rule 3 — one selector per job — is a property of the job, not of one config read. The
/// selector is captured when a job's workers are started and is then baked into their
/// `TaskInfo`, into every `TableConfig` they stamp, and into every checkpoint they write.
/// A later configuration that names a different backend does not migrate any of that; it
/// only creates a second authority, so the database says one thing while the running job
/// says another. Cleanup, compaction, and the next restore then disagree about which
/// backend owns the state on disk.
///
/// Refusing the change is the recoverable direction. The job's state stays exactly as its
/// own backend wrote it, and an operator can either put the old value back or stop the job
/// and create a new one with the new backend. Accepting the change is not recoverable: the
/// job would keep writing state under one authority while being administered under
/// another.
///
/// Both of those remedies stay available while the bad value is still in the row. Refusing
/// the selector refuses only the selector: a row that carries a stop request alongside it
/// is still executed as a stop, under the backend the job is running with and with the
/// final-checkpoint semantics the stop mode asked for. A refusal that also discarded the
/// remedy it documents would not be a refusal, it would be a dead end.
///
/// `running` is the selector the job is actually running with and `requested` is the one
/// the new configuration carries; both are already-normalized values, so a row that
/// changes from `""` to `"parquet"` is not a change at all.
///
/// # Errors
///
/// Returns [`StateBackendError::JobSelectorChanged`] if the two differ.
pub fn validate_unchanged_job_selector(
    label: &str,
    running: StateBackendSelector,
    requested: StateBackendSelector,
) -> Result<(), StateBackendError> {
    if running == requested {
        return Ok(());
    }

    Err(StateBackendError::JobSelectorChanged {
        label: format!("job {label:?}"),
        running,
        requested,
    })
}

/// Checks the selector a live worker leader reports against the one a controller is about
/// to administer that job with.
///
/// This is the only check in the system that consults neither a database row nor a
/// persisted object. A worker leader received its selector in its own `StartExecutionReq`
/// and has been stamping it into task infos, table configs, and every checkpoint it has
/// written ever since; it is therefore the live authority for as long as it is running. A
/// controller that has just been restarted rebuilds its view of the job from persisted
/// state, and asking the leader is how it finds out that its reconstruction disagrees
/// *before* it starts administering a job that is still running under something else.
///
/// `reported` is the raw value exactly as the leader sent it. The empty string is what a
/// leader predating the field sends, and — as everywhere else — it means `parquet`, which
/// is the only backend such a leader can be running. That is what makes the check
/// backwards compatible in the direction that matters: a parquet job agrees with an old
/// leader, and a stateengine job does not, which is the conservative answer.
///
/// # Errors
///
/// Returns [`StateBackendError::UnknownValue`] if the leader reports a value that is
/// neither empty nor a known backend name, or [`StateBackendError::LeaderMismatch`] if it
/// reports a different backend than `execution`.
pub fn validate_leader_selector(
    job_id: &str,
    execution: StateBackendSelector,
    reported: &str,
) -> Result<(), StateBackendError> {
    validate_agreement(execution, SelectorScope::Leader, [(job_id, reported)])
}

#[cfg(test)]
mod tests {
    use super::{
        SelectorScope, StateBackendError, StateBackendSelector, validate_agreement,
        validate_leader_selector, validate_unchanged_job_selector,
    };

    #[test]
    fn empty_value_normalizes_to_parquet() {
        // The old-data guarantee: the SQL and protobuf default is "", and every job row,
        // start request, table config, and checkpoint written before the selector
        // existed carries it.
        assert_eq!(
            StateBackendSelector::normalize("", "job").unwrap(),
            StateBackendSelector::Parquet
        );
        assert_eq!(StateBackendSelector::DEFAULT, StateBackendSelector::Parquet);
        assert_eq!(
            StateBackendSelector::default(),
            StateBackendSelector::Parquet
        );
    }

    #[test]
    fn explicit_values_round_trip() {
        for selector in [
            StateBackendSelector::Parquet,
            StateBackendSelector::StateEngine,
        ] {
            assert_eq!(
                StateBackendSelector::normalize(selector.as_str(), "job").unwrap(),
                selector
            );
            assert_eq!(selector.to_string(), selector.as_str());
        }

        // Pin the persisted spellings themselves: they are on disk and on the wire.
        assert_eq!(StateBackendSelector::Parquet.as_str(), "parquet");
        assert_eq!(StateBackendSelector::StateEngine.as_str(), "stateengine");
    }

    #[test]
    fn unknown_value_is_rejected_rather_than_defaulted() {
        let err = StateBackendSelector::normalize("rocksdb", "job").unwrap_err();
        assert_eq!(
            err,
            StateBackendError::UnknownValue {
                label: "job".to_string(),
                value: "rocksdb".to_string(),
            }
        );

        let message = err.to_string();
        assert!(message.contains("rocksdb"), "{message}");
        assert!(message.contains("job"), "{message}");
    }

    #[test]
    fn whitespace_and_case_variants_are_rejected() {
        // Exact match only: these values are machine-written, so a near-miss is a
        // producer bug and must not be laundered into the persisted record.
        for raw in [
            " parquet",
            "parquet ",
            "\tparquet",
            "Parquet",
            "PARQUET",
            "StateEngine",
            "state_engine",
            " ",
        ] {
            assert!(
                matches!(
                    StateBackendSelector::normalize(raw, "job"),
                    Err(StateBackendError::UnknownValue { .. })
                ),
                "{raw:?} must not be accepted"
            );
        }
    }

    #[test]
    fn error_is_a_std_error() {
        fn assert_std_error<E: std::error::Error>(_: &E) {}
        assert_std_error(&StateBackendSelector::normalize("rocksdb", "job").unwrap_err());
    }

    #[test]
    fn agreement_accepts_agreeing_entries() {
        // An empty value is an old table config, which agrees with a parquet job.
        validate_agreement(
            StateBackendSelector::Parquet,
            SelectorScope::Table,
            [("w:window", "parquet"), ("s:source", ""), ("g:global", "")],
        )
        .unwrap();

        validate_agreement(
            StateBackendSelector::StateEngine,
            SelectorScope::Table,
            [("w:window", "stateengine")],
        )
        .unwrap();
    }

    #[test]
    fn agreement_accepts_no_entries() {
        // A job with no tables violates nothing.
        validate_agreement(
            StateBackendSelector::StateEngine,
            SelectorScope::Table,
            std::iter::empty(),
        )
        .unwrap();
    }

    #[test]
    fn agreement_rejects_table_disagreeing_with_job() {
        let err = validate_agreement(
            StateBackendSelector::Parquet,
            SelectorScope::Table,
            [("w:window", "stateengine")],
        )
        .unwrap_err();

        assert_eq!(
            err,
            StateBackendError::TableMismatch {
                label: "table \"w:window\"".to_string(),
                found: StateBackendSelector::StateEngine,
                job: StateBackendSelector::Parquet,
            }
        );

        let message = err.to_string();
        assert!(message.contains("w:window"), "{message}");
        assert!(message.contains("stateengine"), "{message}");
        assert!(message.contains("parquet"), "{message}");
    }

    #[test]
    fn agreement_rejects_checkpoint_disagreeing_with_job() {
        let err = validate_agreement(
            StateBackendSelector::StateEngine,
            SelectorScope::Checkpoint,
            [("epoch 12", "parquet")],
        )
        .unwrap_err();

        assert_eq!(
            err,
            StateBackendError::CheckpointMismatch {
                label: "restored checkpoint \"epoch 12\"".to_string(),
                found: StateBackendSelector::Parquet,
                job: StateBackendSelector::StateEngine,
            }
        );

        let message = err.to_string();
        assert!(message.contains("epoch 12"), "{message}");
        assert!(message.contains("parquet"), "{message}");
    }

    #[test]
    fn agreement_rejects_unknown_entry_value() {
        let err = validate_agreement(
            StateBackendSelector::Parquet,
            SelectorScope::Table,
            [("w:window", "parquet"), ("s:source", "rocksdb")],
        )
        .unwrap_err();

        assert_eq!(
            err,
            StateBackendError::UnknownValue {
                label: "table \"s:source\"".to_string(),
                value: "rocksdb".to_string(),
            }
        );
    }

    #[test]
    fn agreement_rejects_mixed_entries_even_when_job_matches_one() {
        let err = validate_agreement(
            StateBackendSelector::Parquet,
            SelectorScope::Table,
            [("w:window", "parquet"), ("s:source", "stateengine")],
        )
        .unwrap_err();

        assert_eq!(
            err,
            StateBackendError::MixedSelectors {
                label: "table \"s:source\"".to_string(),
                value: StateBackendSelector::StateEngine,
                other_label: "table \"w:window\"".to_string(),
                other_value: StateBackendSelector::Parquet,
            }
        );

        let message = err.to_string();
        assert!(message.contains("w:window"), "{message}");
        assert!(message.contains("s:source"), "{message}");

        // The same mixture reported through the entry that disagrees with the job:
        // still typed, still names the offender, never silently accepted.
        let err = validate_agreement(
            StateBackendSelector::Parquet,
            SelectorScope::Table,
            [("s:source", "stateengine"), ("w:window", "parquet")],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            StateBackendError::TableMismatch { ref label, .. } if label.contains("s:source")
        ));
    }

    /// The selector is a property of the job, so it cannot change while the job exists.
    /// Both directions matter: a real change is refused, and a value that only *looks*
    /// different — the pre-selector empty string against an explicit `"parquet"` — is not
    /// a change, because both normalize to the same selector before they get here.
    #[test]
    fn a_jobs_selector_cannot_change_while_the_job_exists() {
        validate_unchanged_job_selector(
            "job_abc",
            StateBackendSelector::Parquet,
            StateBackendSelector::Parquet,
        )
        .unwrap();
        validate_unchanged_job_selector(
            "job_abc",
            StateBackendSelector::normalize("", "job").unwrap(),
            StateBackendSelector::normalize("parquet", "job").unwrap(),
        )
        .unwrap();
        validate_unchanged_job_selector(
            "job_abc",
            StateBackendSelector::StateEngine,
            StateBackendSelector::StateEngine,
        )
        .unwrap();

        let err = validate_unchanged_job_selector(
            "job_abc",
            StateBackendSelector::Parquet,
            StateBackendSelector::StateEngine,
        )
        .unwrap_err();
        assert_eq!(
            err,
            StateBackendError::JobSelectorChanged {
                label: "job \"job_abc\"".to_string(),
                running: StateBackendSelector::Parquet,
                requested: StateBackendSelector::StateEngine,
            }
        );
        let message = err.to_string();
        assert!(message.contains("job_abc"), "{message}");
        assert!(message.contains("cannot be changed"), "{message}");

        // and symmetrically, so a stateengine job cannot be quietly demoted to parquet
        assert!(matches!(
            validate_unchanged_job_selector(
                "job_abc",
                StateBackendSelector::StateEngine,
                StateBackendSelector::Parquet,
            ),
            Err(StateBackendError::JobSelectorChanged {
                running: StateBackendSelector::StateEngine,
                requested: StateBackendSelector::Parquet,
                ..
            })
        ));
    }

    /// The live leader is the authority a restarted controller checks itself against, so
    /// agreement must be required and a disagreement must be typed and name the job.
    #[test]
    fn a_leader_running_another_backend_is_refused() {
        // Agreement, explicit on both sides.
        for selector in [
            StateBackendSelector::Parquet,
            StateBackendSelector::StateEngine,
        ] {
            validate_leader_selector("job_abc", selector, selector.as_str()).unwrap();
        }

        let err = validate_leader_selector("job_abc", StateBackendSelector::StateEngine, "parquet")
            .unwrap_err();
        assert_eq!(
            err,
            StateBackendError::LeaderMismatch {
                label: "worker leader for \"job_abc\"".to_string(),
                found: StateBackendSelector::Parquet,
                job: StateBackendSelector::StateEngine,
            }
        );
        let message = err.to_string();
        assert!(message.contains("job_abc"), "{message}");

        // and symmetrically
        assert!(matches!(
            validate_leader_selector("job_abc", StateBackendSelector::Parquet, "stateengine"),
            Err(StateBackendError::LeaderMismatch {
                found: StateBackendSelector::StateEngine,
                job: StateBackendSelector::Parquet,
                ..
            })
        ));
    }

    /// The compatibility direction, and the conservative one, in a single rule. A leader
    /// that predates the field reports nothing, which means parquet — the only backend it
    /// could be running — so a parquet job attaches to it as it always did, and a
    /// stateengine job refuses to, because such a leader cannot be running stateengine.
    #[test]
    fn a_leader_that_reports_no_backend_means_parquet() {
        validate_leader_selector("job_abc", StateBackendSelector::Parquet, "").unwrap();

        assert!(matches!(
            validate_leader_selector("job_abc", StateBackendSelector::StateEngine, ""),
            Err(StateBackendError::LeaderMismatch {
                found: StateBackendSelector::Parquet,
                ..
            })
        ));

        // and a value nobody recognizes is never downgraded to either of them
        assert!(matches!(
            validate_leader_selector("job_abc", StateBackendSelector::Parquet, "rocksdb"),
            Err(StateBackendError::UnknownValue { ref value, .. }) if value == "rocksdb"
        ));
    }
}
