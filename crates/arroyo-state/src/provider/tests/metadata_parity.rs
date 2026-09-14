//! M11.T09k, metadata half — the provider extracts committing data, classifies a
//! checkpoint's files, and compacts exactly as the static dispatch it replaces does (risk
//! M11.T09o).
//!
//! These are M11.D20 families M11.T11 converts; nothing calls the provider for them yet.
//! Proving the parity here is what makes that conversion a change of caller rather than a
//! change of behaviour. Merging has its own module, [`super::merge_parity`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arroyo_rpc::grpc::rpc::{
    ExpiringKeyedTimeTableCheckpointMetadata, GlobalKeyedTableTaskCheckpointMetadata,
    OperatorCheckpointMetadata, OperatorMetadata, TableCheckpointMetadata, TableConfig,
};
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_rpc::state_backend::validated::Validated;
use arroyo_types::CheckpointFilePathLayout;
use prost::Message;

use super::{
    TABLE, at, batch, data_file, expiring_config, expiring_provider, global_config,
    global_provider, storage, task_info, time_file, write_epoch,
};
use crate::TableData;
use crate::provider::{StateBackendProvider, TableKind};
use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::tables::global_keyed_map::GlobalKeyedTable;
use crate::tables::{CompactionConfig, ErasedTable};
use crate::validated::{
    CheckpointCleanup, CheckpointIdentity, CleanupScope, OperatorCleanup, ValidatedTable,
};

/// Committing data is extracted identically through both paths, and the two-phase-commit
/// flag is what decides whether there is any.
#[test]
fn committing_data_matches_the_legacy_static_path() {
    let metadata = TableCheckpointMetadata {
        table_type: TableKind::GlobalKeyValue.as_table_enum().into(),
        data: GlobalKeyedTableTaskCheckpointMetadata {
            files: vec!["f0".to_string()],
            commit_data_by_subtask: HashMap::from([(0, b"x".to_vec()), (1, b"y".to_vec())]),
        }
        .encode_to_vec(),
    };

    let config = global_config(true);
    let legacy = <GlobalKeyedTable as ErasedTable>::committing_data(config.clone(), &metadata);
    let through_provider = global_provider().committing_data(config, &metadata);
    assert_eq!(legacy, through_provider);
    assert_eq!(
        through_provider,
        Some(HashMap::from([(0, b"x".to_vec()), (1, b"y".to_vec())])),
    );

    // The same metadata, with the flag off: the commit data is still there and is still not
    // committing data.
    let config = global_config(false);
    let legacy = <GlobalKeyedTable as ErasedTable>::committing_data(config.clone(), &metadata);
    let through_provider = global_provider().committing_data(config, &metadata);
    assert_eq!(legacy, through_provider);
    assert_eq!(through_provider, None);

    let metadata = TableCheckpointMetadata {
        table_type: TableKind::ExpiringKeyedTime.as_table_enum().into(),
        data: ExpiringKeyedTimeTableCheckpointMetadata {
            files: vec![time_file("f0", 1, 9_000_000)],
        }
        .encode_to_vec(),
    };
    let config = expiring_config();
    let legacy = <ExpiringTimeKeyTable as ErasedTable>::committing_data(config.clone(), &metadata);
    let through_provider = expiring_provider().committing_data(config, &metadata);
    assert_eq!(legacy, through_provider);
    assert_eq!(through_provider, None);
}

/// One validated table, and the file set both paths say its checkpoint references.
fn with_validated_table<R>(
    config: TableConfig,
    metadata: TableCheckpointMetadata,
    body: impl FnOnce(ValidatedTable<'_>) -> R,
) -> R {
    let info = task_info();
    let retained = OperatorCheckpointMetadata {
        operator_metadata: Some(OperatorMetadata {
            job_id: info.job_id.clone(),
            operator_id: info.operator_id.clone(),
            epoch: 1,
            min_watermark: None,
            max_watermark: None,
            parallelism: 1,
        }),
        start_time: 0,
        finish_time: 0,
        table_checkpoint_metadata: HashMap::from([(TABLE.to_string(), metadata)]),
        table_configs: HashMap::from([(TABLE.to_string(), config)]),
    };
    let cleanup = CheckpointCleanup::new(
        CheckpointIdentity::new(&info.job_id, 1),
        1,
        1,
        vec![OperatorCleanup::new(
            info.operator_id.clone(),
            retained,
            vec![],
        )],
    );
    let operator_ids = vec![info.operator_id.clone()];
    let expected = CheckpointIdentity::new(&info.job_id, 1);
    let token = Validated::validate(
        cleanup,
        CleanupScope {
            job: StateBackendSelector::Parquet,
            operator_ids: &operator_ids,
            expected: &expected,
        },
    )
    .expect("a whole, agreeing, single-operator cleanup");

    let operator = CheckpointCleanup::operators(&token)
        .next()
        .expect("one operator");
    let tables = operator.retained_tables().expect("one table");
    body(*tables.first().expect("one table"))
}

/// The files a checkpoint keeps are classified identically through both paths, for both
/// kinds and for a checkpoint that references more than one file.
#[test]
fn files_to_keep_matches_the_legacy_static_path() {
    let first = data_file(1, TABLE, 0);
    let second = data_file(1, TABLE, 1);

    with_validated_table(
        global_config(false),
        TableCheckpointMetadata {
            table_type: TableKind::GlobalKeyValue.as_table_enum().into(),
            data: GlobalKeyedTableTaskCheckpointMetadata {
                files: vec![first.clone(), second.clone()],
                commit_data_by_subtask: HashMap::new(),
            }
            .encode_to_vec(),
        },
        |table| {
            let legacy = <GlobalKeyedTable as ErasedTable>::files_to_keep(table).expect("legacy");
            let through_provider = global_provider().files_to_keep(table).expect("provider");
            assert_eq!(legacy, through_provider);
            assert_eq!(
                through_provider,
                HashSet::from([first.clone(), second.clone()]),
            );
        },
    );

    with_validated_table(
        expiring_config(),
        TableCheckpointMetadata {
            table_type: TableKind::ExpiringKeyedTime.as_table_enum().into(),
            data: ExpiringKeyedTimeTableCheckpointMetadata {
                files: vec![
                    time_file(&first, 1, 4_000_000),
                    time_file(&second, 1, 9_000_000),
                ],
            }
            .encode_to_vec(),
        },
        |table| {
            let legacy =
                <ExpiringTimeKeyTable as ErasedTable>::files_to_keep(table).expect("legacy");
            let through_provider = expiring_provider().files_to_keep(table).expect("provider");
            assert_eq!(legacy, through_provider);
            assert_eq!(
                through_provider,
                HashSet::from([first.clone(), second.clone()]),
            );
        },
    );
}

/// Compaction produces the same table metadata through both paths.
///
/// The expiring case compacts real state — two epochs of generation-0 files written by the
/// table's own checkpointer, with the minimum epoch count set so that they qualify — so the
/// comparison is of two completed compactions, not of two refusals to start one.
#[tokio::test]
async fn compact_data_matches_the_legacy_static_path() {
    let storage = storage("compaction").await;
    let config = expiring_config();

    let fresh: Arc<dyn ErasedTable> = Arc::new(
        <ExpiringTimeKeyTable as ErasedTable>::from_config(
            config.clone(),
            task_info(),
            storage.clone(),
            None,
        )
        .expect("fresh table"),
    );
    let mut reports = HashMap::new();
    for (index, epoch) in [1u32, 2].into_iter().enumerate() {
        reports.insert(
            index as u32,
            write_epoch(
                &fresh,
                epoch,
                None,
                vec![TableData::RecordBatch(batch(
                    100 * i64::from(epoch),
                    at(u64::from(epoch)),
                ))],
            )
            .await,
        );
    }
    let current =
        <ExpiringTimeKeyTable as ErasedTable>::merge_checkpoint_metadata(config.clone(), reports)
            .expect("merge")
            .expect("two subtasks reported state");
    assert_eq!(
        ExpiringKeyedTimeTableCheckpointMetadata::decode(current.data.as_slice())
            .expect("expiring table metadata")
            .files
            .len(),
        2,
        "the fixture has to have something to compact",
    );

    let compaction_config = |storage: &_| CompactionConfig {
        storage_provider: Arc::clone(storage),
        compact_generations: HashSet::from([0]),
        min_compaction_epochs: 2,
        file_path_layout: CheckpointFilePathLayout::Legacy,
    };
    let info = task_info();
    let operator_metadata = OperatorMetadata {
        job_id: info.job_id.clone(),
        operator_id: info.operator_id.clone(),
        epoch: 2,
        min_watermark: None,
        max_watermark: None,
        parallelism: 1,
    };

    let legacy = <ExpiringTimeKeyTable as ErasedTable>::compact_data(
        config.clone(),
        &compaction_config(&storage),
        &operator_metadata,
        current.clone(),
    )
    .await
    .expect("legacy compaction");
    let through_provider = expiring_provider()
        .compact_data(
            config.clone(),
            &compaction_config(&storage),
            &operator_metadata,
            current.clone(),
        )
        .await
        .expect("provider compaction");

    assert_eq!(
        legacy.as_ref().map(Message::encode_to_vec),
        through_provider.as_ref().map(Message::encode_to_vec),
    );
    let compacted = ExpiringKeyedTimeTableCheckpointMetadata::decode(
        through_provider
            .expect("two epochs of generation 0 compact")
            .data
            .as_slice(),
    )
    .expect("expiring table metadata");
    assert_eq!(
        compacted
            .files
            .iter()
            .map(|file| (file.epoch, file.generation))
            .collect::<Vec<_>>(),
        vec![(2, 1)],
        "compaction folds both generation-0 files into one generation-1 file at the newest epoch",
    );

    // The global key/value table compacts nothing, through either path, even with files to
    // consider.
    let global = TableCheckpointMetadata {
        table_type: TableKind::GlobalKeyValue.as_table_enum().into(),
        data: GlobalKeyedTableTaskCheckpointMetadata {
            files: vec![data_file(1, TABLE, 0), data_file(2, TABLE, 0)],
            commit_data_by_subtask: HashMap::new(),
        }
        .encode_to_vec(),
    };
    let legacy = <GlobalKeyedTable as ErasedTable>::compact_data(
        global_config(false),
        &compaction_config(&storage),
        &operator_metadata,
        global.clone(),
    )
    .await
    .expect("legacy compaction");
    let through_provider = global_provider()
        .compact_data(
            global_config(false),
            &compaction_config(&storage),
            &operator_metadata,
            global,
        )
        .await
        .expect("provider compaction");
    assert_eq!(legacy, through_provider);
    assert_eq!(through_provider, None);
}
