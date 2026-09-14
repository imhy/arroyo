//! M11.T09c.01 — the bounded-page global key/value load over real checkpointed state.
//!
//! Every case in the three suites below reads state a real epoch checkpointer wrote,
//! through the real provider the registry hands out, so what a restore reads is what a
//! checkpoint writes. This module holds what they share: the fixtures, and the oracle.
//!
//! | Suite | Claim |
//! |---|---|
//! | [`parity`] | the seam restores exactly what the walk it replaced read, for both decode flavours |
//! | [`bounds`] | the page bound holds, is observable, and is checked rather than trusted (design item M11.D36, work-plan item M11.P49d) |
//! | [`fail_closed`] | a null, an unreadable state version, and an over-wide page each fail the whole restore with the error the legacy walk raised |
//!
//! [`legacy_walk`] is the parity oracle. The walk that used to sit in
//! `GlobalKeyedTable::load_with_version` moved *behind* the seam, so comparing the seam
//! with `memory_view` would compare the seam with itself; the oracle is that walk written
//! out again here, with its own fetch, its own reader and its own decode, and every parity
//! case asserts a closed-form expected value as well.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{BinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{
    GlobalKeyedTableConfig, GlobalKeyedTableSubtaskCheckpointMetadata,
    GlobalKeyedTableTaskCheckpointMetadata, TableCheckpointMetadata, TableConfig,
};
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_storage::StorageProviderRef;
use bincode::{Decode, Encode};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prost::Message;

use super::{TABLE, task_info, write_epoch};
use crate::provider::{ProviderRegistry, TableKind};
use crate::tables::global_key_value_load::{
    GlobalKeyValueLoad, GlobalKeyValuePage, LoadPageLimits, LoadPagePeak,
};
use crate::tables::global_keyed_map::GlobalKeyedTable;
use crate::tables::{ErasedTable, MigratableState};
use crate::{BINCODE_CONFIG, TableData};

mod bounds;
mod fail_closed;
mod parity;

/// The bytes each `(key, value)` these fixtures write occupies in a page.
///
/// Keys are `k#####` and values are `v#####`, six characters each, and bincode's standard
/// configuration writes a string as a one-byte length for anything under 251 bytes. So an
/// entry is `(1 + 6) + (1 + 6)` payload bytes, which is what makes every payload assertion
/// below a closed form rather than a measurement.
const ENTRY_PAYLOAD_BYTES: usize = 14;

/// The value type of the migration fixtures: version 1, one step on from a plain `String`.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct Versioned(String);

impl MigratableState for Versioned {
    const VERSION: u32 = 1;

    type PreviousVersion = String;

    fn migrate(previous: Self::PreviousVersion) -> Result<Self, StateError> {
        Ok(Self(format!("migrated:{previous}")))
    }
}

/// A global key/value config that writes its checkpoint objects under `state_version`.
fn versioned_config(state_version: u32) -> TableConfig {
    TableConfig {
        table_type: TableKind::GlobalKeyValue.as_table_enum().into(),
        config: GlobalKeyedTableConfig {
            table_name: TABLE.to_string(),
            description: "global key/value load fixture".to_string(),
            uses_two_phase_commit: false,
        }
        .encode_to_vec(),
        state_version,
        state_backend: String::new(),
    }
}

/// The `(key, value)` pairs `k00000..k0000n`, bincode-encoded as the table stores them.
fn pairs(range: std::ops::Range<usize>) -> Vec<TableData> {
    range
        .map(|i| TableData::KeyedData {
            key: bincode::encode_to_vec(format!("k{i:05}"), BINCODE_CONFIG).expect("encode"),
            value: bincode::encode_to_vec(format!("v{i:05}"), BINCODE_CONFIG).expect("encode"),
        })
        .collect()
}

/// What [`pairs`] means once decoded.
fn expected(range: std::ops::Range<usize>) -> HashMap<String, String> {
    range
        .map(|i| (format!("k{i:05}"), format!("v{i:05}")))
        .collect()
}

/// Writes one checkpoint object of `data` under `config`'s state version and returns its
/// path.
///
/// The object is produced by the table's own epoch checkpointer, so its layout, its
/// compression and the state version in its footer are a real checkpoint's.
async fn write_source(
    storage: &StorageProviderRef,
    config: &TableConfig,
    epoch: u32,
    data: Vec<TableData>,
) -> String {
    let table: Arc<dyn ErasedTable> = Arc::new(
        <GlobalKeyedTable as ErasedTable>::from_config(
            config.clone(),
            task_info(),
            storage.clone(),
            None,
        )
        .expect("a fresh table"),
    );
    let subtask = write_epoch(&table, epoch, None, data).await;
    GlobalKeyedTableSubtaskCheckpointMetadata::decode(subtask.data.as_slice())
        .expect("global key/value subtask metadata")
        .file
        .expect("a checkpointed global table writes one object")
}

/// A restored table over exactly `files`, in that order.
///
/// The file list is built here rather than through `merge_checkpoint_metadata`, which
/// re-collects the subtask reports into a `HashMap` and therefore does not fix their order.
/// Order decides which of two colliding keys survives, so a case about order has to state
/// it.
fn restored(
    storage: &StorageProviderRef,
    config: &TableConfig,
    files: Vec<String>,
) -> GlobalKeyedTable {
    let checkpoint = TableCheckpointMetadata {
        table_type: TableKind::GlobalKeyValue.as_table_enum().into(),
        data: GlobalKeyedTableTaskCheckpointMetadata {
            files,
            commit_data_by_subtask: HashMap::new(),
        }
        .encode_to_vec(),
    };
    <GlobalKeyedTable as ErasedTable>::from_config(
        config.clone(),
        task_info(),
        storage.clone(),
        Some(checkpoint),
    )
    .expect("a restored table")
}

/// The loader the registry's parquet provider hands out for `table`.
async fn loader_through_the_registry(
    table: &GlobalKeyedTable,
) -> Box<dyn GlobalKeyValueLoad + Send> {
    ProviderRegistry::parquet_default()
        .global_key_value_provider(StateBackendSelector::Parquet)
        .expect("parquet serves global key/value tables")
        .global_key_value_load(TABLE, table)
        .await
        .expect("a loader over a parquet table")
}

/// Every page a loader produces, to exhaustion, plus its final high-water mark.
async fn drain(
    mut loader: Box<dyn GlobalKeyValueLoad + Send>,
) -> (Vec<GlobalKeyValuePage>, LoadPagePeak, LoadPageLimits) {
    let limits = loader.page_limits();
    let mut pages = Vec::new();
    while let Some(page) = loader.next_page().await.expect("a page") {
        assert!(
            limits.admits(&page),
            "page of {} entries and {} payload bytes exceeds the declared bound",
            page.len(),
            page.payload_bytes()
        );
        pages.push(page);
    }
    assert!(
        loader.next_page().await.expect("exhausted").is_none(),
        "exhaustion is stable"
    );
    (pages, loader.peak_page(), limits)
}

/// The walk `GlobalKeyedTable::load_with_version` performed before M11.T09c.01 moved it
/// behind the seam, preserved here as the parity oracle.
///
/// It shares no code with the loader under test: its own fetch, its own reader, its own
/// column extraction and its own decode. Comparing the seam with `memory_view` would
/// compare the seam with itself, since `memory_view` now delegates to it.
async fn legacy_walk(storage: &StorageProviderRef, files: &[String]) -> HashMap<String, String> {
    let mut data = HashMap::new();
    for file in files {
        let contents = storage.get(file.as_str()).await.expect("the object");
        let reader = ParquetRecordBatchReaderBuilder::try_new(contents)
            .expect("a parquet object")
            .build()
            .expect("a reader");
        for batch in reader {
            let batch = batch.expect("a batch");
            let keys = binary(&batch, "key");
            let values = binary(&batch, "value");
            for row in 0..batch.num_rows() {
                let key: String = bincode::decode_from_slice(keys.value(row), BINCODE_CONFIG)
                    .expect("a key")
                    .0;
                let value: String = bincode::decode_from_slice(values.value(row), BINCODE_CONFIG)
                    .expect("a value")
                    .0;
                data.insert(key, value);
            }
        }
    }
    data
}

fn binary(batch: &RecordBatch, name: &str) -> BinaryArray {
    batch
        .column_by_name(name)
        .expect("the column")
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("a binary column")
        .clone()
}

/// Writes a parquet object of this table's shape by hand, so a case can produce a file the
/// checkpointer never would — one whose key or value column is nullable and null.
async fn write_nullable_source(
    storage: &StorageProviderRef,
    path: &str,
    keys: Vec<Option<&[u8]>>,
    values: Vec<Option<&[u8]>>,
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Binary, true),
        Field::new("value", DataType::Binary, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(keys.into_iter().collect::<BinaryArray>()),
            Arc::new(values.into_iter().collect::<BinaryArray>()),
        ],
    )
    .expect("a batch of this schema");

    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).expect("a writer");
    writer.write(&batch).expect("write");
    writer.flush().expect("flush");
    let bytes = writer.into_inner().expect("the written object");
    storage.put(path, bytes).await.expect("put");
}
