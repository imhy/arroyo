//! M11.T09k, construction half — the provider builds the same tables and opens the same
//! views as the static dispatch it replaces (risk M11.T09o, design item M11.D33).
//!
//! Each case drives the exact call the `TableEnum` match arm made and the provider call
//! that replaced it, over one set of inputs, and compares the results *and* a closed-form
//! expected value. The state the restored cases read is written by a real epoch
//! checkpointer, so the files a restored table opens are the files a checkpoint writes.

use std::collections::HashMap;
use std::sync::Arc;

use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{ExpiringKeyedTimeTableCheckpointMetadata, ParquetTimeFile};
use futures::StreamExt;
use prost::Message;

use super::{
    TABLE, at, batch, expiring_config, expiring_provider, global_config, global_provider,
    state_channel, storage, task_info, value_of, write_epoch,
};
use crate::provider::parquet::ParquetTable;
use crate::provider::{ExpiringTimeKeyProvider, StateBackendProvider, TableKind};
use crate::tables::ErasedTable;
use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::tables::expiring_time_key_view::{
    ExpiringTimeKeyBatchStream, ExpiringTimeKeyViewApi, ExpiringTimeKeyViewDrain,
};
use crate::tables::global_keyed_map::GlobalKeyedTable;
use crate::{BINCODE_CONFIG, TableData};

/// The kind a parquet table declares to the registry is the kind its own wire value names.
///
/// `ParquetTable::KIND` is what the registry files a provider under and
/// `ErasedTable::table_type` is what a table config has to state for that provider to be
/// looked up; if the two ever disagreed, a correctly-configured table would be built by the
/// provider for the other kind.
#[test]
fn the_declared_kind_of_each_parquet_table_matches_its_wire_table_type() {
    assert_eq!(
        <GlobalKeyedTable as ParquetTable>::KIND.as_table_enum(),
        <GlobalKeyedTable as ErasedTable>::table_type(),
    );
    assert_eq!(
        <ExpiringTimeKeyTable as ParquetTable>::KIND.as_table_enum(),
        <ExpiringTimeKeyTable as ErasedTable>::table_type(),
    );
    assert_eq!(global_provider().table_kind(), TableKind::GlobalKeyValue);
    assert_eq!(
        expiring_provider().table_kind(),
        TableKind::ExpiringKeyedTime
    );
}

/// A global key/value table built through the provider restores exactly the state one built
/// through the legacy static call restores.
#[tokio::test]
async fn global_table_construction_restores_the_same_state_as_the_legacy_static_path() {
    let storage = storage("global-construction").await;
    let config = global_config(false);

    // Real state: one epoch, written by the table's own checkpointer.
    let fresh: Arc<dyn ErasedTable> = Arc::new(
        <GlobalKeyedTable as ErasedTable>::from_config(
            config.clone(),
            task_info(),
            storage.clone(),
            None,
        )
        .expect("fresh table"),
    );
    let subtask = write_epoch(
        &fresh,
        4,
        None,
        vec![
            TableData::KeyedData {
                key: bincode::encode_to_vec("a".to_string(), BINCODE_CONFIG).unwrap(),
                value: bincode::encode_to_vec("1".to_string(), BINCODE_CONFIG).unwrap(),
            },
            TableData::KeyedData {
                key: bincode::encode_to_vec("b".to_string(), BINCODE_CONFIG).unwrap(),
                value: bincode::encode_to_vec("2".to_string(), BINCODE_CONFIG).unwrap(),
            },
        ],
    )
    .await;
    let checkpoint = <GlobalKeyedTable as ErasedTable>::merge_checkpoint_metadata(
        config.clone(),
        HashMap::from([(0, subtask)]),
    )
    .expect("merge")
    .expect("one subtask reported state");

    // The two construction paths: the arm `TableManager::load` used to run, and the call
    // that replaced it.
    let legacy: Arc<dyn ErasedTable> = Arc::new(
        <GlobalKeyedTable as ErasedTable>::from_config(
            config.clone(),
            task_info(),
            storage.clone(),
            Some(checkpoint.clone()),
        )
        .expect("legacy restore"),
    );
    let through_provider = global_provider()
        .table(
            config.clone(),
            task_info(),
            storage.clone(),
            Some(checkpoint.clone()),
        )
        .expect("provider restore");

    let restored = |table: &Arc<dyn ErasedTable>| {
        let table = table
            .as_any()
            .downcast_ref::<GlobalKeyedTable>()
            .expect("a global key/value table")
            .clone();
        async move {
            let (tx, _rx) = state_channel();
            let view = table
                .memory_view::<String, String>(tx)
                .await
                .expect("restored view");
            view.get_all().clone()
        }
    };

    let legacy_state = restored(&legacy).await;
    let provider_state = restored(&through_provider).await;

    assert_eq!(legacy_state, provider_state);
    assert_eq!(
        legacy_state,
        HashMap::from([
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ]),
    );
    assert_eq!(
        through_provider
            .as_any()
            .downcast_ref::<GlobalKeyedTable>()
            .expect("a global key/value table")
            .files,
        checkpoint_files(&checkpoint),
    );
}

/// The file list a restored global key/value checkpoint carries.
fn checkpoint_files(checkpoint: &arroyo_rpc::grpc::rpc::TableCheckpointMetadata) -> Vec<String> {
    arroyo_rpc::grpc::rpc::GlobalKeyedTableTaskCheckpointMetadata::decode(
        checkpoint.data.as_slice(),
    )
    .expect("global key/value checkpoint metadata")
    .files
}

/// An expiring table built through the provider derives the same subtask metadata from a
/// restored checkpoint as one built through the legacy static call.
#[tokio::test]
async fn expiring_table_construction_matches_the_legacy_static_path() {
    let storage = storage("expiring-construction").await;
    let config = expiring_config();
    let checkpoint = arroyo_rpc::grpc::rpc::TableCheckpointMetadata {
        table_type: TableKind::ExpiringKeyedTime.as_table_enum().into(),
        data: ExpiringKeyedTimeTableCheckpointMetadata {
            files: vec![
                time_file("instance-1/checkpoints/checkpoint-0000002/operator-test-operator-1/table-t-000", 2, 0, 9_000_000),
                time_file("instance-1/checkpoints/checkpoint-0000001/operator-test-operator-1/table-t-000", 1, 0, 4_000_000),
                time_file("instance-1/checkpoints/checkpoint-0000003/operator-test-operator-1/table-t-000", 3, 1, 14_000_000),
            ],
        }
        .encode_to_vec(),
    };

    let legacy: Arc<dyn ErasedTable> = Arc::new(
        <ExpiringTimeKeyTable as ErasedTable>::from_config(
            config.clone(),
            task_info(),
            storage.clone(),
            Some(checkpoint.clone()),
        )
        .expect("legacy restore"),
    );
    let through_provider = expiring_provider()
        .table(
            config.clone(),
            task_info(),
            storage.clone(),
            Some(checkpoint.clone()),
        )
        .expect("provider restore");

    let legacy_subtask = legacy
        .subtask_metadata_from_table(checkpoint.clone())
        .expect("legacy subtask metadata")
        .expect("an expiring table always derives subtask metadata");
    let provider_subtask = through_provider
        .subtask_metadata_from_table(checkpoint.clone())
        .expect("provider subtask metadata")
        .expect("an expiring table always derives subtask metadata");

    assert_eq!(
        legacy_subtask.encode_to_vec(),
        provider_subtask.encode_to_vec()
    );
    assert_eq!(provider_subtask.subtask_index, 0);
    assert_eq!(
        provider_subtask.table_type,
        i32::from(TableKind::ExpiringKeyedTime.as_table_enum()),
    );
    let decoded = arroyo_rpc::grpc::rpc::ExpiringKeyedTimeSubtaskCheckpointMetadata::decode(
        provider_subtask.data.as_slice(),
    )
    .expect("expiring subtask metadata");
    assert_eq!(decoded.watermark, None);
    assert_eq!(
        decoded.files.iter().map(|f| f.epoch).collect::<Vec<_>>(),
        vec![2, 1, 3],
        "the derived metadata carries the checkpoint's files in the checkpoint's order",
    );
}

fn time_file(
    file: &str,
    epoch: u32,
    generation: u64,
    max_timestamp_micros: u64,
) -> ParquetTimeFile {
    ParquetTimeFile {
        epoch,
        file: file.to_string(),
        min_routing_key: 0,
        max_routing_key: u64::MAX,
        max_timestamp_micros,
        generation,
    }
}

/// The provider's view of a restored table drains exactly what the legacy view drains, for
/// a watermark that retires part of the restored state and a batch inserted afterwards.
#[tokio::test]
async fn view_creation_matches_the_legacy_static_path() {
    let storage = storage("view-creation").await;
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
    let subtask = write_epoch(
        &fresh,
        1,
        None,
        vec![
            TableData::RecordBatch(batch(30, at(3))),
            TableData::RecordBatch(batch(70, at(7))),
            TableData::RecordBatch(batch(120, at(12))),
        ],
    )
    .await;
    let checkpoint = <ExpiringTimeKeyTable as ErasedTable>::merge_checkpoint_metadata(
        config.clone(),
        HashMap::from([(0, subtask)]),
    )
    .expect("merge")
    .expect("one subtask reported state");

    let table = expiring_provider()
        .table(
            config.clone(),
            task_info(),
            storage.clone(),
            Some(checkpoint.clone()),
        )
        .expect("restored table");

    // The watermark retires everything before `watermark - retention` = 5s, so the batch
    // written at 3s is not restored into the view at all and the two at 7s and 12s are.
    let watermark = Some(at(15));

    let (legacy_tx, _legacy_rx) = state_channel();
    let mut legacy: Box<dyn ExpiringTimeKeyViewApi + Send> = Box::new(
        table
            .as_any()
            .downcast_ref::<ExpiringTimeKeyTable>()
            .expect("an expiring table")
            .get_view(legacy_tx, watermark)
            .await
            .expect("legacy view"),
    );

    let (provider_tx, _provider_rx) = state_channel();
    let mut through_provider = expiring_provider()
        .expiring_time_key_view(TABLE, &*table, provider_tx, watermark)
        .await
        .expect("provider view");

    assert_eq!(legacy.get_min_time(), through_provider.get_min_time());
    assert_eq!(legacy.get_min_time(), Some(at(7)));

    legacy.insert(at(20), batch(200, at(20))).expect("insert");
    through_provider
        .insert(at(20), batch(200, at(20)))
        .expect("insert");

    let legacy_drained = drain(legacy.all_batches_for_watermark(watermark)).await;
    let provider_drained = drain(through_provider.all_batches_for_watermark(watermark)).await;

    assert_eq!(legacy_drained, provider_drained);
    assert_eq!(legacy_drained, vec![(7, 70), (12, 120), (20, 200)]);
}

/// Every `(timestamp seconds, value)` a drain yields, in order.
async fn drain(mut stream: ExpiringTimeKeyBatchStream<'_>) -> Vec<(u64, i64)> {
    let mut out = vec![];
    while let Some(item) = stream.next().await {
        let (timestamp, batch) = item.expect("drain");
        out.push((
            timestamp
                .duration_since(std::time::UNIX_EPOCH)
                .expect("after the epoch")
                .as_secs(),
            value_of(&batch),
        ));
    }
    out
}

/// A table of the other live kind is refused with the error the legacy downcast produced,
/// naming the table the caller asked for.
#[tokio::test]
async fn view_creation_refuses_a_table_of_another_kind() {
    let storage = storage("view-wrong-kind").await;
    let global = global_provider()
        .table(global_config(false), task_info(), storage, None)
        .expect("global table");

    let (tx, _rx) = state_channel();
    let err = expiring_provider()
        .expiring_time_key_view(TABLE, &*global, tx, None)
        .await
        .err()
        .expect("a global key/value table has no expiring view");

    assert!(
        matches!(
            &err,
            StateError::WrongTableKind { table, expected }
                if table == TABLE && *expected == "expiring_time_key_table"
        ),
        "{err:?}",
    );
}
