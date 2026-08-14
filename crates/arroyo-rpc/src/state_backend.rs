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

pub use arroyo_types::state_backend::*;

use crate::grpc::rpc::{CheckpointManifest, OperatorCheckpointMetadata, TableConfig};
use std::collections::HashMap;

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
/// operator's metadata, so this is the whole restore check for that mode, run on the
/// controller before any worker is told to execute.
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
        StateBackendError, StateBackendSelector, validate_restored_checkpoint,
        validate_restored_manifest, validate_restored_operator_metadata,
        validate_subtask_table_configs,
    };
    use crate::grpc::rpc::{
        CheckpointManifest, OperatorCheckpointMetadata, OperatorMetadata, TableConfig, TableEnum,
    };
    use std::collections::HashMap;

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
}
