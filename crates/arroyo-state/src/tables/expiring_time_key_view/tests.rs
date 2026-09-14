//! Characterization of the parquet view behind the M11.D13 seam.
//!
//! The helpers live here; the cases are split by what they pin — [`read`] for what a
//! drain returns, [`mutation`] for what the writing methods leave behind, and [`tokens`]
//! for the drain-token and fail-closed invariants.
//!
//! The table is built with no checkpoint files, so every batch a test sees was put there
//! by the test itself and no object store is read.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, TimestampNanosecondArray};
use arrow_schema::{DataType, Field};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_storage::{StorageProvider, StorageProviderRef};
use arroyo_types::{get_test_task_info, to_nanos};
use futures::StreamExt;
use tokio::sync::mpsc::{Receiver, channel};

use super::parquet_view::ExpiringTimeKeyView;
use super::{BatchDrainToken, ExpiringTimeKeyViewApi, ExpiringTimeKeyViewDrain};
use crate::tables::ErasedTable;
use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::{StateMessage, TableData, timestamp_table_config};

const RETENTION: Duration = Duration::from_secs(10);

fn at(seconds: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
}

fn schema() -> ArroyoSchema {
    ArroyoSchema::from_fields(vec![Field::new("value", DataType::Int64, false)])
}

async fn storage() -> StorageProviderRef {
    let dir = std::env::temp_dir().join("arroyo-t09b-expiring-view-tests");
    std::fs::create_dir_all(&dir).expect("temp dir");
    Arc::new(
        StorageProvider::for_url(&format!("file://{}", dir.display()))
            .await
            .expect("local storage provider"),
    )
}

/// A view over a table with no checkpoint files, plus the state channel it writes to.
async fn view() -> (ExpiringTimeKeyView, Receiver<StateMessage>) {
    let table = <ExpiringTimeKeyTable as ErasedTable>::from_config(
        timestamp_table_config(
            "t",
            "expiring view characterization",
            RETENTION,
            false,
            schema(),
        ),
        Arc::new(get_test_task_info()),
        storage().await,
        None,
    )
    .expect("table from config");
    let (tx, rx) = channel(1024);
    let view = table.get_view(tx, None).await.expect("empty view");
    (view, rx)
}

/// A one-row batch carrying `value`, returned with the value column it shares.
fn batch(value: i64, timestamp: SystemTime) -> (RecordBatch, ArrayRef) {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![value]));
    let times: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        to_nanos(timestamp) as i64
    ]));
    let batch = RecordBatch::try_new(schema().schema.clone(), vec![values.clone(), times])
        .expect("batch matches schema");
    (batch, values)
}

fn value_of(batch: &RecordBatch) -> i64 {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 value column")
        .value(0)
}

/// Inserts `(timestamp_seconds, value)` pairs as buffered batches.
fn insert_all(view: &mut ExpiringTimeKeyView, batches: &[(u64, i64)]) {
    for (seconds, value) in batches {
        let (batch, _) = batch(*value, at(*seconds));
        view.insert(at(*seconds), batch).expect("insert");
    }
}

/// Every `(timestamp_seconds, value)` the stream yields, in order.
async fn drained(view: &mut ExpiringTimeKeyView, watermark: Option<SystemTime>) -> Vec<(u64, i64)> {
    let mut stream = view.all_batches_for_watermark(watermark);
    let mut out = vec![];
    while let Some(item) = stream.next().await {
        let (timestamp, batch) = item.expect("drain");
        let seconds = timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("after epoch")
            .as_secs();
        out.push((seconds, value_of(&batch)));
    }
    out
}

/// Every value written to state so far, in order, leaving the channel empty.
fn written_to_state(rx: &mut Receiver<StateMessage>) -> Vec<i64> {
    let mut out = vec![];
    while let Ok(message) = rx.try_recv() {
        match message {
            StateMessage::TableData {
                data: TableData::RecordBatch(batch),
                ..
            } => out.push(value_of(&batch)),
            other => panic!("unexpected state message {other:?}"),
        }
    }
    out
}

mod mutation;
mod read;
mod tokens;
