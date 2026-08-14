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
//!    their own inputs into `(label, raw value)` pairs.

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
}

impl SelectorScope {
    /// The noun used to describe this scope in error messages.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Checkpoint => "restored checkpoint",
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
}

#[cfg(test)]
mod tests {
    use super::{SelectorScope, StateBackendError, StateBackendSelector, validate_agreement};

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
}
