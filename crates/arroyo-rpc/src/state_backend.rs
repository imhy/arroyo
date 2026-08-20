//! Explicit, persisted state-backend selection (design item M11.D13b).
//!
//! The vocabulary — [`StateBackendSelector`], [`normalize`](StateBackendSelector::normalize),
//! [`validate_agreement`] and [`StateBackendError`] — lives in `arroyo-types` so that
//! [`arroyo_types::TaskInfo`] can carry a typed selector, and is re-exported here because
//! `arroyo-rpc` is where the rest of the persisted and transported vocabulary (checkpoint
//! metadata, table configs, start requests) lives.
//!
//! On top of that this module adds the checkpoint-side half of the rule: the functions
//! that check a *persisted* selector — one read back out of a database row, a checkpoint
//! manifest, or an operator's checkpoint metadata — against the selector the job was
//! started with. They are called at the points a job restores, merges, commits, or cleans
//! up state, always *before* the operation they guard does any work.
//!
//! Reading a persisted selector is the mirror image of producing one. At the acquisition
//! boundary (`OperatorContext::new`) an empty `TableConfig.state_backend` means "the
//! operator that declared this table did not state a backend", and the job's value is
//! stamped in. Here, on the read-back side, an empty value means exactly what
//! [`StateBackendSelector::normalize`] says it means — `parquet` — because the only way a
//! persisted record can be empty is that it was written before the field existed, i.e. by
//! a build that had no backend but parquet. That is what makes an existing deployment's
//! checkpoints restorable, and what makes them refuse to restore into a stateengine job.

//!
//! On top of *that* — because "checked before the operation" is a claim about a whole
//! object and not about the item that happened to be checked — [`validated`] carries the
//! result of a whole-object check as a token that the destructive and publishing operations
//! take instead of the raw object (design item M11.D39c).

pub mod validated;

pub use arroyo_types::state_backend::*;

use crate::grpc::rpc::{CheckpointManifest, OperatorCheckpointMetadata, TableConfig};
use crate::state_backend::validated::identity::{CheckpointIdentity, check_operator_header};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

/// Checks a whole-checkpoint selector — the `state_backend` column of a `checkpoints` row
/// — against the job that is about to restore from it.
///
/// `epoch` names the checkpoint in the error, so the failure identifies which checkpoint
/// could not be restored. `raw` is the column value exactly as it was read; the empty
/// string is what rows written before the selector existed carry and means `parquet`.
///
/// # Errors
///
/// Returns [`StateBackendError::UnknownValue`] if the column holds a value that is
/// neither empty nor a known backend name, or [`StateBackendError::CheckpointMismatch`]
/// if the checkpoint was written by a different backend than the job selects. Neither is
/// recoverable: the same row is read again on every restore attempt.
pub fn validate_restored_checkpoint(
    job: StateBackendSelector,
    epoch: u64,
    raw: &str,
) -> Result<(), StateBackendError> {
    validate_agreement(
        job,
        SelectorScope::Checkpoint,
        [(format!("epoch {epoch}").as_str(), raw)],
    )
}

/// Checks every table config in one operator's restored checkpoint metadata against the
/// job's selector.
///
/// This is the per-table half of the restore check: a checkpoint row may say `parquet`
/// while the operator metadata underneath it says something else, and a checkpoint that
/// has no row at all — leader mode keeps its history in the generation manifest — has
/// nothing *but* these values. Table configs written before the selector existed are
/// empty and therefore read as `parquet`, which is what they were.
///
/// The label of each entry names the epoch, the operator, and the table, so a failure
/// says which checkpoint and which table disagreed. Labels are built up front rather than
/// lazily: this runs once per operator per restore, merge, or cleanup, never per record.
///
/// # Errors
///
/// Returns the [`StateBackendError`] [`validate_agreement`] raised — an unknown value,
/// two tables of one operator selecting different backends, or a table disagreeing with
/// the job.
pub fn validate_restored_operator_metadata(
    job: StateBackendSelector,
    metadata: &OperatorCheckpointMetadata,
) -> Result<(), StateBackendError> {
    let (epoch, operator_id) = match &metadata.operator_metadata {
        Some(m) => (m.epoch, m.operator_id.as_str()),
        // Metadata with no operator header is rejected by every caller a moment later,
        // for its own reasons; the selector check still runs, just without those names.
        None => (0, "unknown"),
    };

    validate_table_configs(
        job,
        SelectorScope::Checkpoint,
        &metadata.table_configs,
        |t| format!("epoch {epoch}, operator {operator_id}, table {t}"),
    )
}

/// Checks every operator in a restored checkpoint manifest against the job's selector.
///
/// A manifest is the leader-mode equivalent of the controller's checkpoint row plus every
/// operator's metadata, so this is the selector half of the restore check for that mode,
/// run on the controller before any worker is told to execute. The coverage half —
/// whether the manifest describes exactly the operators the workers will build — is
/// [`validate_manifest_covers_program`], and callers that publish protocol state run both.
///
/// # Errors
///
/// Returns the first [`StateBackendError`] any of the manifest's operators produces; see
/// [`validate_restored_operator_metadata`].
pub fn validate_restored_manifest(
    job: StateBackendSelector,
    manifest: &CheckpointManifest,
) -> Result<(), StateBackendError> {
    manifest
        .operators
        .iter()
        .try_for_each(|operator| validate_restored_operator_metadata(job, operator))
}

/// A restored checkpoint manifest that cannot restore the program its workers will build.
///
/// This is deliberately not a [`StateBackendError`]: nothing about it is a selector
/// disagreement. It is the leader-mode counterpart of
/// `StateError::IncompleteCheckpoint`, which reports the same condition for a legacy
/// checkpoint's operator metadata objects.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("the checkpoint at epoch {epoch} cannot restore this job's program: {detail}")]
pub struct IncompleteManifest {
    /// The epoch of the manifest that was rejected.
    pub epoch: u64,
    /// What exactly was wrong, naming the operators involved.
    pub detail: String,
}

/// Checks that a restored checkpoint manifest, and every entry in it, is the checkpoint the
/// caller read it for — and yields the operator ids its entries describe.
///
/// A manifest states the identity of its checkpoint many times over: once in its own
/// `pipeline_id`/`job_id`/`generation`/`epoch`, and once more in every entry's
/// `OperatorMetadata` header, which carries a `job_id` and an `epoch` of its own. Until review
/// round 7 of PR #160 nothing compared the header with anything: it was read for its
/// `operator_id` and no other field, so an entry lifted out of another job's or another
/// epoch's checkpoint — carrying the right operator id — described state that is not this
/// checkpoint's, and the worker that looks itself up in this manifest would restore from it.
///
/// `checkpoint` is the identity the *caller* established, and this check is worth exactly what
/// established it. The one production caller of
/// [`validate_manifest_covers_program`] binds it to the `CheckpointRef` the manifest was read
/// from before calling (`arroyo_state_protocol::types::identify_checkpoint_manifest`), which is
/// what makes this a comparison of storage's answer against the caller's question rather than
/// of a value with itself. Passing an identity taken out of the same manifest would type-check
/// and prove nothing; the parameter exists so that the caller has to have decided.
///
/// Nothing here reads or writes storage.
///
/// # Errors
///
/// Returns [`IncompleteManifest`] if the manifest names a different job or epoch than
/// `checkpoint`, if its epoch is wider than the epoch a checkpoint object can carry, if an
/// entry has no operator metadata header, or if an entry's header names a different job or
/// epoch. Coverage is *not* checked here — see [`validate_manifest_covers_program`].
pub fn validate_manifest_identity<'a>(
    manifest: &'a CheckpointManifest,
    checkpoint: &CheckpointIdentity,
) -> Result<Vec<&'a str>, IncompleteManifest> {
    let incomplete = |detail: String| IncompleteManifest {
        epoch: manifest.epoch,
        detail,
    };

    let claimed =
        CheckpointIdentity::at_wide_epoch(manifest.job_id.as_str(), manifest.epoch, incomplete)?;
    checkpoint.check_matches("the checkpoint manifest", &claimed, incomplete)?;

    let mut described = Vec::with_capacity(manifest.operators.len());
    for (index, operator) in manifest.operators.iter().enumerate() {
        // A headerless entry cannot be found by the worker that needs it — the lookup
        // matches on the header's operator id — so it is not a description of anything.
        let Some(header) = operator.operator_metadata.as_ref() else {
            return Err(incomplete(format!(
                "entry {index} has no operator metadata header, so no operator can match it"
            )));
        };

        // Which operator the entry describes is the entry's own to say — the coverage check
        // below compares the whole list against the program as a set. Which *checkpoint* it
        // belongs to is not, and that is the pair of fields this compares.
        check_operator_header(
            checkpoint.operator(header.operator_id.as_str()),
            operator,
            incomplete,
        )?;

        described.push(header.operator_id.as_str());
    }

    Ok(described)
}

/// Checks that a restored checkpoint manifest is the checkpoint the caller read it for and
/// describes **exactly** the operators the job's workers are going to build, one valid entry
/// each.
///
/// This is the leader-mode preflight for restoring, and it exists for the same reason the
/// legacy path's is a whole-set check rather than a per-item one: a worker constructs
/// every operator of its *current* program and each one looks itself up in this manifest
/// by operator id, requiring an entry that carries an operator header naming it. An entry
/// the manifest happens to contain therefore proves nothing — what matters is whether
/// every operator that will ask for one finds exactly one.
///
/// Validating the manifest's own entries instead, which is all a per-item check can do,
/// passes a manifest with an operator omitted, an operator the program does not contain,
/// the same operator twice, or an entry with no header at all — and each of those fails
/// only later, in a worker, after the generation has already been published and the
/// protocol state has already advanced.
///
/// `restoring` is the set of operator ids the workers will construct, derived from the
/// *current* program (`LogicalProgram::tasks_per_operator`), which is the same source of
/// truth the legacy path's preflight uses. Both a checkpoint's entries and the program's
/// operator set are compared as sets, so the manifest's order does not matter.
///
/// `checkpoint` is which checkpoint the caller read this manifest for; see
/// [`validate_manifest_identity`], which this runs first. Coverage is a claim about a
/// *particular* checkpoint's entries, so identity is the coarser question and is answered
/// before it: an entry set can cover the program perfectly and still belong to another job.
///
/// Nothing here reads or writes storage: it is pure set arithmetic over an object the
/// caller has already read, so it can run before the caller publishes anything.
///
/// # Errors
///
/// Returns [`IncompleteManifest`] for anything [`validate_manifest_identity`] reports, if two
/// entries name the same operator, if an operator of the program is not described, or if the
/// manifest describes an operator the program does not contain. Operators are listed sorted so
/// the message is stable.
pub fn validate_manifest_covers_program(
    manifest: &CheckpointManifest,
    checkpoint: &CheckpointIdentity,
    restoring: &HashSet<&str>,
) -> Result<(), IncompleteManifest> {
    let incomplete = |detail: String| IncompleteManifest {
        epoch: manifest.epoch,
        detail,
    };

    let described = validate_manifest_identity(manifest, checkpoint)?;

    let mut listed: HashSet<&str> = HashSet::with_capacity(described.len());
    for operator_id in described {
        if !listed.insert(operator_id) {
            return Err(incomplete(format!(
                "operator {operator_id} is described more than once, so which of its entries \
                 would be restored is not defined"
            )));
        }
    }

    let mut missing: Vec<&str> = restoring.difference(&listed).copied().collect();
    if !missing.is_empty() {
        missing.sort_unstable();
        return Err(incomplete(format!(
            "the job's program contains operator(s) {} that the checkpoint does not \
             describe, and every operator a worker builds looks itself up in it",
            missing.join(", ")
        )));
    }

    let mut extra: Vec<&str> = listed.difference(restoring).copied().collect();
    if !extra.is_empty() {
        extra.sort_unstable();
        return Err(incomplete(format!(
            "the checkpoint describes operator(s) {} that the job's program does not \
             contain",
            extra.join(", ")
        )));
    }

    Ok(())
}

/// Checks the table configs a subtask reported with its finished checkpoint against the
/// job's selector, before they are merged into the operator's checkpoint metadata.
///
/// These configs were stamped with the job's selector when the subtask's operator context
/// was built, so agreement is the normal case; a disagreement means the subtask belongs
/// to a job — or a generation of one — that selected a different backend, and merging it
/// would write a checkpoint that is half one backend's and half the other's.
///
/// # Errors
///
/// Returns [`StateBackendError::UnknownValue`], [`StateBackendError::MixedSelectors`], or
/// [`StateBackendError::TableMismatch`], each naming the operator, the subtask, and the
/// table that disagreed.
pub fn validate_subtask_table_configs(
    job: StateBackendSelector,
    operator_id: &str,
    subtask_index: u32,
    table_configs: &HashMap<String, TableConfig>,
) -> Result<(), StateBackendError> {
    validate_table_configs(job, SelectorScope::Table, table_configs, |t| {
        format!("{t} of operator {operator_id} subtask {subtask_index}")
    })
}

/// Runs [`validate_agreement`] over a map of table configs, labelling each entry with
/// `label`.
///
/// Entries are materialized because [`validate_agreement`] borrows its labels and these
/// are built per call; the allocation is bounded by the number of tables one operator
/// declares and happens once per restore, merge, or cleanup.
fn validate_table_configs(
    job: StateBackendSelector,
    scope: SelectorScope,
    table_configs: &HashMap<String, TableConfig>,
    label: impl Fn(&str) -> String,
) -> Result<(), StateBackendError> {
    let entries: Vec<(String, &str)> = table_configs
        .iter()
        .map(|(table, config)| (label(table), config.state_backend.as_str()))
        .collect();

    validate_agreement(
        job,
        scope,
        entries.iter().map(|(label, raw)| (label.as_str(), *raw)),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        CheckpointIdentity, StateBackendError, StateBackendSelector,
        validate_manifest_covers_program, validate_restored_checkpoint, validate_restored_manifest,
        validate_restored_operator_metadata, validate_subtask_table_configs,
    };
    use crate::grpc::rpc::{
        CheckpointManifest, OperatorCheckpointMetadata, OperatorMetadata, TableConfig, TableEnum,
    };
    use std::collections::{HashMap, HashSet};

    fn table_configs<'a>(
        entries: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> HashMap<String, TableConfig> {
        entries
            .into_iter()
            .map(|(table, state_backend)| {
                (
                    table.to_string(),
                    TableConfig {
                        table_type: TableEnum::GlobalKeyValue as i32,
                        config: vec![],
                        state_version: 0,
                        state_backend: state_backend.to_string(),
                    },
                )
            })
            .collect()
    }

    fn operator_metadata<'a>(
        epoch: u32,
        entries: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> OperatorCheckpointMetadata {
        OperatorCheckpointMetadata {
            operator_metadata: Some(OperatorMetadata {
                job_id: "job_1".to_string(),
                operator_id: "node_3".to_string(),
                epoch,
                min_watermark: None,
                max_watermark: None,
                parallelism: 1,
            }),
            start_time: 0,
            finish_time: 0,
            table_checkpoint_metadata: HashMap::new(),
            table_configs: table_configs(entries),
        }
    }

    /// The deployability guarantee, on the read-back side: a checkpoint written before
    /// the selector existed carries `""` in its row and in every table config, and it
    /// restores into a parquet job exactly as it always did.
    #[test]
    fn a_checkpoint_written_before_the_selector_existed_restores_into_a_parquet_job() {
        validate_restored_checkpoint(StateBackendSelector::Parquet, 12, "").unwrap();
        validate_restored_operator_metadata(
            StateBackendSelector::Parquet,
            &operator_metadata(12, [("w:window", ""), ("g:global", "")]),
        )
        .unwrap();

        // The same thing as a whole manifest, and with no operators at all.
        let manifest = CheckpointManifest {
            epoch: 12,
            operators: vec![operator_metadata(12, [("w:window", "")])],
            ..Default::default()
        };
        validate_restored_manifest(StateBackendSelector::Parquet, &manifest).unwrap();
        validate_restored_manifest(
            StateBackendSelector::StateEngine,
            &CheckpointManifest::default(),
        )
        .unwrap();
    }

    /// A checkpoint that explicitly states the job's own backend restores, for both
    /// backends — including the one whose provider does not exist yet.
    #[test]
    fn a_checkpoint_stating_the_jobs_own_backend_restores() {
        for job in [
            StateBackendSelector::Parquet,
            StateBackendSelector::StateEngine,
        ] {
            validate_restored_checkpoint(job, 12, job.as_str()).unwrap();
            validate_restored_operator_metadata(
                job,
                &operator_metadata(12, [("w:window", job.as_str())]),
            )
            .unwrap();
        }
    }

    /// The hole this sub-task closes: a checkpoint written by the parquet backend must
    /// not be restored into a job that selects stateengine, and the failure must name the
    /// checkpoint rather than degrade to a default.
    #[test]
    fn a_parquet_checkpoint_restored_into_a_stateengine_job_fails_typed() {
        let err = validate_restored_checkpoint(StateBackendSelector::StateEngine, 12, "parquet")
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

        // An old checkpoint — empty, therefore parquet — is refused for the same reason.
        let err =
            validate_restored_checkpoint(StateBackendSelector::StateEngine, 12, "").unwrap_err();
        assert!(
            matches!(err, StateBackendError::CheckpointMismatch { found, .. }
                if found == StateBackendSelector::Parquet),
            "{err:?}"
        );

        // and the same through the operator metadata, which names the table too
        let err = validate_restored_operator_metadata(
            StateBackendSelector::StateEngine,
            &operator_metadata(12, [("w:window", "parquet")]),
        )
        .unwrap_err();
        assert_eq!(
            err,
            StateBackendError::CheckpointMismatch {
                label: "restored checkpoint \"epoch 12, operator node_3, table w:window\""
                    .to_string(),
                found: StateBackendSelector::Parquet,
                job: StateBackendSelector::StateEngine,
            }
        );
    }

    /// The symmetric case: a stateengine checkpoint cannot be read by a parquet job.
    #[test]
    fn a_stateengine_checkpoint_restored_into_a_parquet_job_fails_typed() {
        let err = validate_restored_checkpoint(StateBackendSelector::Parquet, 7, "stateengine")
            .unwrap_err();
        assert_eq!(
            err,
            StateBackendError::CheckpointMismatch {
                label: "restored checkpoint \"epoch 7\"".to_string(),
                found: StateBackendSelector::StateEngine,
                job: StateBackendSelector::Parquet,
            }
        );

        let manifest = CheckpointManifest {
            epoch: 7,
            operators: vec![operator_metadata(7, [("w:window", "stateengine")])],
            ..Default::default()
        };
        let err = validate_restored_manifest(StateBackendSelector::Parquet, &manifest).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("epoch 7"), "{message}");
        assert!(message.contains("w:window"), "{message}");
        assert!(message.contains("stateengine"), "{message}");
    }

    /// A persisted value nobody recognizes is a hard failure, never a fallback to the
    /// job's own backend — which would silently read another backend's files.
    #[test]
    fn an_unknown_persisted_checkpoint_selector_fails_typed() {
        let err =
            validate_restored_checkpoint(StateBackendSelector::Parquet, 3, "rocksdb").unwrap_err();
        assert_eq!(
            err,
            StateBackendError::UnknownValue {
                label: "restored checkpoint \"epoch 3\"".to_string(),
                value: "rocksdb".to_string(),
            }
        );

        let err = validate_restored_operator_metadata(
            StateBackendSelector::Parquet,
            &operator_metadata(3, [("w:window", "rocksdb")]),
        )
        .unwrap_err();
        assert!(
            matches!(err, StateBackendError::UnknownValue { ref value, .. } if value == "rocksdb"),
            "{err:?}"
        );
    }

    /// One checkpoint cannot be half one backend's and half another's, even when one half
    /// agrees with the job.
    #[test]
    fn mixed_restored_table_configs_fail_typed() {
        let err = validate_restored_operator_metadata(
            StateBackendSelector::Parquet,
            &operator_metadata(4, [("w:window", "parquet"), ("s:source", "stateengine")]),
        )
        .unwrap_err();

        // Which of the two entries is named depends on map iteration order; both spellings
        // of the failure are typed and both name the offending table.
        assert!(
            matches!(
                err,
                StateBackendError::MixedSelectors { .. }
                    | StateBackendError::CheckpointMismatch { .. }
            ),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("s:source"), "{message}");
        assert!(message.contains("stateengine"), "{message}");
    }

    /// Metadata without an operator header still gets checked; it just cannot name the
    /// operator. The caller rejects the missing header separately.
    #[test]
    fn metadata_without_an_operator_header_is_still_checked() {
        let metadata = OperatorCheckpointMetadata {
            operator_metadata: None,
            table_configs: table_configs([("w:window", "stateengine")]),
            ..Default::default()
        };
        let err = validate_restored_operator_metadata(StateBackendSelector::Parquet, &metadata)
            .unwrap_err();
        assert!(
            matches!(err, StateBackendError::CheckpointMismatch { .. }),
            "{err:?}"
        );
    }

    /// A subtask reporting a table that selects another backend is refused before its
    /// state is merged, and the error names the operator, the subtask, and the table.
    #[test]
    fn a_subtask_table_config_disagreeing_with_the_job_fails_typed() {
        validate_subtask_table_configs(
            StateBackendSelector::StateEngine,
            "node_3",
            1,
            &table_configs([("w:window", "stateengine")]),
        )
        .unwrap();

        let err = validate_subtask_table_configs(
            StateBackendSelector::StateEngine,
            "node_3",
            1,
            &table_configs([("w:window", "parquet")]),
        )
        .unwrap_err();

        assert_eq!(
            err,
            StateBackendError::TableMismatch {
                label: "table \"w:window of operator node_3 subtask 1\"".to_string(),
                found: StateBackendSelector::Parquet,
                job: StateBackendSelector::StateEngine,
            }
        );
    }

    /// The checkpoint the fixtures below are of: job `job_1`, epoch 7.
    ///
    /// Named once so that the manifest, its entries' headers and the identity the caller asks
    /// for cannot drift apart in a fixture the way they did in the code — which is what review
    /// round 7 of PR #160 found. Before that round `manifest()` left `job_id` at its `Default`
    /// empty string while `described()` headed every entry `job_1`, so the fixtures were
    /// *incoherent* about which checkpoint they were; a doc comment in
    /// `arroyo-state-protocol` read that incoherence as a deliberate disagreement the rows were
    /// testing, and it was not. Only the identity fields moved; every assertion below is the
    /// one M11.T08 landed.
    const JOB: &str = "job_1";
    const EPOCH: u32 = 7;

    fn asked_for() -> CheckpointIdentity {
        CheckpointIdentity::new(JOB, EPOCH)
    }

    /// An entry for `operator_id`, headed as the writer heads it.
    fn described(operator_id: &str) -> OperatorCheckpointMetadata {
        headed(JOB, operator_id, EPOCH)
    }

    /// An entry headed exactly as stated, for the rows that vary one identity at a time.
    fn headed(job_id: &str, operator_id: &str, epoch: u32) -> OperatorCheckpointMetadata {
        OperatorCheckpointMetadata {
            operator_metadata: Some(OperatorMetadata {
                job_id: job_id.to_string(),
                operator_id: operator_id.to_string(),
                epoch,
                min_watermark: None,
                max_watermark: None,
                parallelism: 1,
            }),
            ..Default::default()
        }
    }

    fn manifest(operators: Vec<OperatorCheckpointMetadata>) -> CheckpointManifest {
        manifest_of(JOB, u64::from(EPOCH), operators)
    }

    fn manifest_of(
        job_id: &str,
        epoch: u64,
        operators: Vec<OperatorCheckpointMetadata>,
    ) -> CheckpointManifest {
        CheckpointManifest {
            job_id: job_id.to_string(),
            epoch,
            operators,
            ..Default::default()
        }
    }

    /// The operator ids a program's workers will build, as
    /// `LogicalProgram::tasks_per_operator` yields them.
    fn program<'a>(ids: &[&'a str]) -> HashSet<&'a str> {
        ids.iter().copied().collect()
    }

    /// The direction that must keep working: a manifest that describes exactly the
    /// program's operators, in any order, restores.
    #[test]
    fn a_manifest_describing_exactly_the_program_is_accepted() {
        validate_manifest_covers_program(
            &manifest(vec![described("b"), described("a")]),
            &asked_for(),
            &program(&["a", "b"]),
        )
        .unwrap();

        // ...including the degenerate case of a program with no operators at all
        validate_manifest_covers_program(&manifest(vec![]), &asked_for(), &program(&[])).unwrap();
    }

    /// The defect: an operator the program contains but the manifest omits used to pass,
    /// because the validator only ever looked at entries that happened to be present.
    /// The worker that builds that operator looks itself up in the manifest and fails.
    #[test]
    fn a_manifest_that_omits_an_operator_is_refused() {
        let err = validate_manifest_covers_program(
            &manifest(vec![described("a")]),
            &asked_for(),
            &program(&["a", "b"]),
        )
        .unwrap_err();
        assert_eq!(err.epoch, 7);
        assert!(err.detail.contains('b'), "{}", err.detail);
        assert!(err.to_string().contains("epoch 7"), "{err}");
    }

    /// An operator the manifest describes but the program does not contain means the
    /// checkpoint belongs to a different program.
    #[test]
    fn a_manifest_with_an_extra_operator_is_refused() {
        let err = validate_manifest_covers_program(
            &manifest(vec![described("a"), described("c")]),
            &asked_for(),
            &program(&["a"]),
        )
        .unwrap_err();
        assert!(err.detail.contains('c'), "{}", err.detail);
    }

    /// Two entries for one operator: which one would be restored is not defined, so
    /// neither is.
    #[test]
    fn a_manifest_describing_an_operator_twice_is_refused() {
        let err = validate_manifest_covers_program(
            &manifest(vec![described("a"), described("a")]),
            &asked_for(),
            &program(&["a"]),
        )
        .unwrap_err();
        assert!(err.detail.contains("more than once"), "{}", err.detail);
    }

    /// A headerless entry describes nothing: the worker's lookup matches on the header's
    /// operator id, so an entry without one can never be found — and the legacy loader
    /// reads the restored watermark straight out of that header.
    #[test]
    fn a_manifest_with_a_headerless_entry_is_refused() {
        let err = validate_manifest_covers_program(
            &manifest(vec![
                described("a"),
                OperatorCheckpointMetadata {
                    operator_metadata: None,
                    ..Default::default()
                },
            ]),
            &asked_for(),
            &program(&["a"]),
        )
        .unwrap_err();
        assert!(
            err.detail.contains("no operator metadata header"),
            "{}",
            err.detail
        );

        // and the coverage check is what catches it: the selector check tolerates it,
        // because a headerless entry states no selector to disagree with
        validate_restored_manifest(
            StateBackendSelector::Parquet,
            &manifest(vec![OperatorCheckpointMetadata {
                operator_metadata: None,
                ..Default::default()
            }]),
        )
        .expect("the selector half has nothing to say about a headerless entry");
    }

    /// An entry whose `OperatorMetadata` header belongs to another checkpoint is refused, and
    /// each identity is varied on its own (PR #160 review round 7, finding 2).
    ///
    /// The finding, exactly: this function read the header for its `operator_id` and no other
    /// field, so a manifest with the right outer job and the right operator ids could carry an
    /// entry headed with another job's or another epoch's identity — everything else correct,
    /// the operator set covering the program exactly — and be published as a recovery
    /// checkpoint. The header is what a restoring worker reads its watermark out of, and its
    /// `job_id`/`epoch` are what expiring-table compaction builds the path of every file it
    /// writes from, so the entry aims writes at a directory that is not this checkpoint's.
    ///
    /// One case per identity, each from an entry that agrees in the others: the wrong-job entry
    /// is at the right epoch and names the right operator, and the wrong-epoch entry names the
    /// right job and the right operator. Varying both at once would leave either check able to
    /// carry the row on its own.
    #[test]
    fn a_manifest_entry_headed_for_another_checkpoint_is_refused() {
        // The direction that must keep working, first: a manifest whose entries are headed for
        // the checkpoint the manifest is, covering the program exactly.
        validate_manifest_covers_program(
            &manifest(vec![described("a"), described("b")]),
            &asked_for(),
            &program(&["a", "b"]),
        )
        .expect("entries headed for their own checkpoint are what a manifest is made of");

        let wrong_job = validate_manifest_covers_program(
            &manifest(vec![described("a"), headed("job_2", "b", EPOCH)]),
            &asked_for(),
            &program(&["a", "b"]),
        )
        .unwrap_err();
        assert_eq!(wrong_job.epoch, 7);
        assert!(wrong_job.detail.contains("job \"job_2\""), "{wrong_job}");
        assert!(wrong_job.detail.contains("operator b"), "{wrong_job}");

        let wrong_epoch = validate_manifest_covers_program(
            &manifest(vec![described("a"), headed(JOB, "b", 8)]),
            &asked_for(),
            &program(&["a", "b"]),
        )
        .unwrap_err();
        assert!(wrong_epoch.detail.contains("epoch 8"), "{wrong_epoch}");
        assert!(wrong_epoch.detail.contains("operator b"), "{wrong_epoch}");

        // And the case the operator-id-only check was built for still fails the way it did:
        // an entry headed for an operator the program does not contain is extra, not
        // misheaded, so it is the coverage half that reports it.
        let extra = validate_manifest_covers_program(
            &manifest(vec![described("a"), described("c")]),
            &asked_for(),
            &program(&["a"]),
        )
        .unwrap_err();
        assert!(extra.detail.contains('c'), "{extra}");
    }

    /// A manifest that is not the checkpoint the caller read it for is refused before its
    /// entries are looked at (PR #160 review round 7, finding 1).
    ///
    /// The identity is the coarser claim, so it is answered first: a manifest can describe the
    /// program's operators perfectly and still be another job's or another epoch's, and once it
    /// is accepted every path derived from it points somewhere else. Job and epoch are varied
    /// independently, and the epoch is varied both by a value that fits a checkpoint epoch and
    /// by one that does not — the second is the pair a truncating cast would have made equal.
    #[test]
    fn a_manifest_that_is_not_the_checkpoint_it_was_read_for_is_refused() {
        let covering = || vec![described("a")];

        validate_manifest_covers_program(&manifest(covering()), &asked_for(), &program(&["a"]))
            .expect("the manifest of the checkpoint that was asked for is the ordinary case");

        let other_job = validate_manifest_covers_program(
            &manifest_of("job_2", u64::from(EPOCH), vec![headed("job_2", "a", EPOCH)]),
            &asked_for(),
            &program(&["a"]),
        )
        .unwrap_err();
        assert!(other_job.detail.contains("job_2"), "{other_job}");
        assert!(
            other_job.detail.contains("different checkpoint"),
            "{other_job}"
        );

        let other_epoch = validate_manifest_covers_program(
            &manifest_of(JOB, 8, vec![headed(JOB, "a", 8)]),
            &asked_for(),
            &program(&["a"]),
        )
        .unwrap_err();
        assert_eq!(other_epoch.epoch, 8, "the error names the manifest's epoch");
        assert!(other_epoch.detail.contains("epoch 8"), "{other_epoch}");
        assert!(other_epoch.detail.contains("epoch 7"), "{other_epoch}");

        let unrepresentable = validate_manifest_covers_program(
            &manifest_of(JOB, u64::from(u32::MAX) + 1 + u64::from(EPOCH), covering()),
            &asked_for(),
            &program(&["a"]),
        )
        .unwrap_err();
        assert!(
            unrepresentable.detail.contains("wider than"),
            "{unrepresentable}"
        );
    }
}
