//! M11.T10b.01 item 4 — every built-in table inherits the ownership hook's default, which does
//! nothing: a parquet table bound under a raised fence answers `Ok` and checkpoints exactly what
//! an unbound twin checkpoints (design item M11.D33, parquet unchanged).
//!
//! The call order of the hook is `tests/table_manager_hooks.rs`'s; this is the default it calls on
//! every table no provider overrides it for.

use std::sync::Arc;

use super::{
    at, batch, expiring_config, expiring_provider, global_config, global_provider, storage,
    task_info, write_epoch,
};
use crate::ownership::{AcknowledgedFence, AcknowledgedFenceWriter, Raise};
use crate::provider::StateBackendProvider;
use crate::tables::ErasedTable;
use crate::{BINCODE_CONFIG, TableData};

/// A fence acknowledged at `generation`, as a worker that adopted the subtask hands it over.
fn raised(generation: u64) -> (AcknowledgedFenceWriter, AcknowledgedFence) {
    let mut writer = AcknowledgedFenceWriter::unacknowledged();
    let Raise::Ready(ready) = writer.prepare(generation, None) else {
        panic!("no fenced request is in flight")
    };
    assert_eq!(ready.publish(), generation);
    let fence = writer.reader();
    (writer, fence)
}

fn keyed(key: &str, value: &str) -> TableData {
    TableData::KeyedData {
        key: bincode::encode_to_vec(key.to_string(), BINCODE_CONFIG).unwrap(),
        value: bincode::encode_to_vec(value.to_string(), BINCODE_CONFIG).unwrap(),
    }
}

/// The subtask metadata `table` checkpoints at epoch 1 for `data`, bound under `fence` first when
/// one is given.
async fn first_checkpoint(
    table: Arc<dyn ErasedTable>,
    fence: Option<&AcknowledgedFence>,
    data: Vec<TableData>,
) -> arroyo_rpc::grpc::rpc::TableSubtaskCheckpointMetadata {
    if let Some(fence) = fence {
        match table.bind_ownership(fence) {
            Ok(()) => {}
            Err(error) => panic!("the default ownership hook refused: {error}"),
        }
    }
    write_epoch(&table, 1, Some(at(20)), data).await
}

#[tokio::test]
async fn a_bound_parquet_table_checkpoints_exactly_what_an_unbound_one_does() {
    let (_writer, fence) = raised(5);

    let global = || vec![keyed("a", "1"), keyed("b", "2")];
    let mut reports = Vec::new();
    for bound in [None, Some(&fence)] {
        let storage = storage(&format!("t10-bind-global-{}", bound.is_some())).await;
        let table = global_provider()
            .table(global_config(false), task_info(), storage, None)
            .expect("a fresh global table");
        reports.push(first_checkpoint(table, bound, global()).await);
    }
    assert_eq!(reports[0], reports[1]);

    let expiring = || {
        vec![
            TableData::RecordBatch(batch(30, at(3))),
            TableData::RecordBatch(batch(70, at(7))),
        ]
    };
    let mut reports = Vec::new();
    for bound in [None, Some(&fence)] {
        let storage = storage(&format!("t10-bind-expiring-{}", bound.is_some())).await;
        let table = expiring_provider()
            .table(expiring_config(), task_info(), storage, None)
            .expect("a fresh expiring table");
        reports.push(first_checkpoint(table, bound, expiring()).await);
    }
    assert_eq!(reports[0], reports[1]);
}
