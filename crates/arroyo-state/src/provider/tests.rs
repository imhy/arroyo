//! The provider seam's own suites.
//!
//! [`registry`] is M11.T09j — what the registry accepts, refuses, and answers. The two
//! parity modules are M11.T09k: [`table_parity`] covers the families that build something
//! (table construction, view creation) and [`metadata_parity`] the families that classify
//! or rewrite checkpoint metadata (merge, committing data, files-to-keep, compaction).
//! Both compare the provider's answer with the answer the legacy static dispatch gives for
//! the same input, and both assert a closed-form expected value as well — equality between
//! two paths that are both wrong is not parity evidence.
//!
//! [`liveness`] is M11.T09-S5 — the protocol-owned GC liveness resolver: parity against a
//! verbatim copy of the parquet decode that moved out of `arroyo-state-protocol`, the
//! refusals that keep an unreadable payload from being read as an empty file list, and the
//! agreement between a resolver's reported backend and the providers it consults.
//!
//! [`global_load`] is M11.T09c.01 — the bounded-page global key/value load seam over real
//! checkpointed state: parity against an independent copy of the walk it replaced, the page
//! bound at 1× and 10× the entries, and the fail-closed refusals.
//!
//! The process cell has no suite here on purpose: it is written at most once per process
//! and has no reset, so each of its cases needs a process of its own. They are
//! `tests/provider_install_once.rs` and `tests/provider_install_after_first_use.rs`.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, TimestampNanosecondArray};
use arrow_schema::{DataType, Field};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::grpc::rpc::{
    ExpiringKeyedTimeSubtaskCheckpointMetadata, GlobalKeyedTableConfig,
    GlobalKeyedTableSubtaskCheckpointMetadata, ParquetTimeFile, TableConfig, TableEnum,
    TableSubtaskCheckpointMetadata,
};
use arroyo_storage::{StorageProvider, StorageProviderRef};
use arroyo_types::{TaskInfo, get_test_task_info, to_nanos};
use prost::Message;

use super::TableKind;
use super::parquet::ParquetProvider;
use crate::tables::ErasedTable;
use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::tables::global_keyed_map::GlobalKeyedTable;
use crate::{CheckpointMessage, StateMessage, TableData, timestamp_table_config};

mod fake;
mod global_load;
mod liveness;
mod merge_parity;
mod metadata_parity;
mod registry;
mod registry_lookup;
mod table_parity;

/// The retention every expiring-table fixture uses, so a watermark's cutoff is
/// `watermark - 10s` in every case.
const RETENTION: Duration = Duration::from_secs(10);

/// The table name both kinds of fixture use.
const TABLE: &str = "t";

fn at(seconds: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
}

fn schema() -> ArroyoSchema {
    ArroyoSchema::from_fields(vec![Field::new("value", DataType::Int64, false)])
}

/// A local-filesystem storage provider in a directory of this test's own.
async fn storage(name: &str) -> StorageProviderRef {
    let dir = std::env::temp_dir().join(format!("arroyo-t09c-{name}"));
    std::fs::create_dir_all(&dir).expect("temp dir");
    Arc::new(
        StorageProvider::for_url(&format!("file://{}", dir.display()))
            .await
            .expect("local storage provider"),
    )
}

fn task_info() -> Arc<TaskInfo> {
    Arc::new(get_test_task_info())
}

/// A global key/value table config; `two_phase_commit` is the one field that changes what
/// the merge and committing-data families do.
fn global_config(two_phase_commit: bool) -> TableConfig {
    TableConfig {
        table_type: TableEnum::GlobalKeyValue.into(),
        config: GlobalKeyedTableConfig {
            table_name: TABLE.to_string(),
            description: "global key/value parity fixture".to_string(),
            uses_two_phase_commit: two_phase_commit,
        }
        .encode_to_vec(),
        state_version: 0,
        state_backend: String::new(),
    }
}

fn expiring_config() -> TableConfig {
    timestamp_table_config(
        TABLE,
        "expiring keyed time parity fixture",
        RETENTION,
        false,
        schema(),
    )
}

fn global_provider() -> ParquetProvider<GlobalKeyedTable> {
    ParquetProvider::new()
}

fn expiring_provider() -> ParquetProvider<ExpiringTimeKeyTable> {
    ParquetProvider::new()
}

/// A one-row batch carrying `value` at `timestamp`, in [`schema`].
fn batch(value: i64, timestamp: SystemTime) -> RecordBatch {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![value]));
    let times: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        to_nanos(timestamp) as i64
    ]));
    RecordBatch::try_new(schema().schema.clone(), vec![values, times])
        .expect("batch matches schema")
}

fn value_of(batch: &RecordBatch) -> i64 {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 value column")
        .value(0)
}

/// The path a worker writes `table`'s data at, for this job and operator.
///
/// A cleanup refuses table file references outside that namespace, so a fixture that names
/// files any other way cannot produce the validated cleanup the files-to-keep family takes.
fn data_file(epoch: u32, table: &str, part: u32) -> String {
    let info = get_test_task_info();
    format!(
        "{}/checkpoints/checkpoint-{epoch:0>7}/operator-{}/table-{table}-{part:0>3}",
        info.job_id, info.operator_id,
    )
}

/// Writes one epoch of state through `table`'s own checkpointer and returns the subtask
/// metadata it produced.
///
/// This is how a fixture gets *real* restorable state: the files a restored table reads are
/// the files a checkpoint actually wrote, in the layout it actually writes them in.
async fn write_epoch(
    table: &Arc<dyn ErasedTable>,
    epoch: u32,
    watermark: Option<SystemTime>,
    data: Vec<TableData>,
) -> TableSubtaskCheckpointMetadata {
    let mut checkpointer = table
        .epoch_checkpointer(epoch, None)
        .expect("epoch checkpointer");
    for item in data {
        checkpointer.insert_data(item).await.expect("insert");
    }
    let (metadata, _bytes) = checkpointer
        .finish(&CheckpointMessage {
            epoch,
            time: at(0),
            watermark,
            then_stop: false,
        })
        .await
        .expect("finish")
        .expect("a checkpoint that was given data produces metadata");
    metadata
}

/// A state channel whose receiver is kept alive, so a view's writes never fail on a closed
/// queue and can be read back if a test wants them.
fn state_channel() -> (
    tokio::sync::mpsc::Sender<StateMessage>,
    tokio::sync::mpsc::Receiver<StateMessage>,
) {
    tokio::sync::mpsc::channel(1024)
}

fn global_subtask(
    file: Option<&str>,
    commit_data: Option<&[u8]>,
) -> TableSubtaskCheckpointMetadata {
    TableSubtaskCheckpointMetadata {
        subtask_index: 0,
        table_type: TableKind::GlobalKeyValue.as_table_enum().into(),
        data: GlobalKeyedTableSubtaskCheckpointMetadata {
            subtask_index: 0,
            commit_data: commit_data.map(<[u8]>::to_vec),
            file: file.map(str::to_string),
        }
        .encode_to_vec(),
    }
}

fn time_file(file: &str, epoch: u32, max_timestamp_micros: u64) -> ParquetTimeFile {
    ParquetTimeFile {
        epoch,
        file: file.to_string(),
        min_routing_key: 0,
        max_routing_key: u64::MAX,
        max_timestamp_micros,
        generation: 0,
    }
}

fn expiring_subtask(
    watermark: Option<u64>,
    files: Vec<ParquetTimeFile>,
) -> TableSubtaskCheckpointMetadata {
    TableSubtaskCheckpointMetadata {
        subtask_index: 0,
        table_type: TableKind::ExpiringKeyedTime.as_table_enum().into(),
        data: ExpiringKeyedTimeSubtaskCheckpointMetadata {
            subtask_index: 0,
            watermark,
            files,
        }
        .encode_to_vec(),
    }
}
