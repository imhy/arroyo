//! The M11.D11 provider seam, implemented and used the way another crate would.
//!
//! Nothing here is parquet and nothing here is `arroyo-state`'s: the provider below is
//! written against the public traits alone, registered beside the built-in parquet
//! providers, and used through the registry. If [`StateBackendProvider`] or
//! [`ExpiringTimeKeyProvider`] stopped being object-safe, if a method stopped naming only
//! public types, or if registering a backend required something this crate does not export,
//! this file would not compile.
//!
//! It also pins the seam's own composition rule: a provider's view is handed back as
//! `Box<dyn ExpiringTimeKeyViewApi + Send>`, and
//! [`ExpiringTimeKeyViewDrain::all_batches_for_watermark`] is not a method that view
//! supplied — it is the one blanket implementation over every `ExpiringTimeKeyViewApi`,
//! which coherence keeps unique. One poll of the stream is one `next_drained_batch` for
//! any backend, not only for parquet.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{
    OperatorMetadata, TableCheckpointMetadata, TableConfig, TableSubtaskCheckpointMetadata,
};
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_state::StateMessage;
use arroyo_state::provider::parquet::ParquetProvider;
use arroyo_state::provider::{
    ExpiringTimeKeyProvider, GlobalKeyValueProvider, ProviderLiveness, ProviderRegistry,
    ProviderRegistryBuilder, StateBackendProvider, TableKind,
};
use arroyo_state::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use arroyo_state::tables::expiring_time_key_view::{
    BatchDrainToken, ExpiringTimeKeyViewApi, ExpiringTimeKeyViewDrain,
};
use arroyo_state::tables::global_key_value_load::GlobalKeyValueLoad;
use arroyo_state::tables::global_keyed_map::GlobalKeyedTable;
use arroyo_state::tables::{CompactionConfig, ErasedTable};
use arroyo_state::validated::ValidatedTable;
use arroyo_state_protocol::gc::liveness::{CheckpointLiveness, LivenessRefusal};
use arroyo_storage::StorageProviderRef;
use arroyo_types::TaskInfo;
use async_trait::async_trait;
use futures::StreamExt;
use tokio::sync::mpsc::Sender;

fn at(seconds: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
}

fn batch(value: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))])
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

/// A view with no backend at all: a fixed list of batches, handed out one per pull.
struct OutsideView {
    source: Vec<(SystemTime, RecordBatch)>,
    drain: Option<(BatchDrainToken, usize)>,
}

#[async_trait]
impl ExpiringTimeKeyViewApi for OutsideView {
    fn insert(&mut self, max_timestamp: SystemTime, batch: RecordBatch) -> Result<(), StateError> {
        self.source.push((max_timestamp, batch));
        Ok(())
    }

    async fn flush(&mut self, _watermark: Option<SystemTime>) -> Result<(), StateError> {
        Ok(())
    }

    async fn flush_timestamp(&mut self, _timestamp: SystemTime) -> Result<(), StateError> {
        Ok(())
    }

    async fn expire_timestamp(&mut self, timestamp: SystemTime) -> Result<(), StateError> {
        self.source.retain(|(at, _)| *at != timestamp);
        Ok(())
    }

    fn get_min_time(&self) -> Option<SystemTime> {
        self.source.iter().map(|(at, _)| *at).min()
    }

    fn begin_batch_drain(&mut self, _watermark: Option<SystemTime>) -> BatchDrainToken {
        let token = BatchDrainToken::mint();
        self.drain = Some((token, 0));
        token
    }

    async fn next_drained_batch(
        &mut self,
        token: BatchDrainToken,
    ) -> Result<Option<(SystemTime, RecordBatch)>, StateError> {
        let Some((live, position)) = self.drain else {
            return Err(stale());
        };
        if live != token {
            return Err(stale());
        }
        self.drain = Some((live, position + 1));
        Ok(self.source.get(position).cloned())
    }
}

fn stale() -> StateError {
    StateError::Other {
        table: "outside".to_string(),
        error: "batch drain token does not identify this view's current drain".to_string(),
    }
}

/// A backend outside this crate, for one live table kind.
struct OutsideProvider {
    kind: TableKind,
}

#[async_trait]
impl StateBackendProvider for OutsideProvider {
    fn selector(&self) -> StateBackendSelector {
        StateBackendSelector::StateEngine
    }

    fn table_kind(&self) -> TableKind {
        self.kind
    }

    fn table(
        &self,
        _config: TableConfig,
        _task_info: Arc<TaskInfo>,
        _storage: StorageProviderRef,
        _checkpoint: Option<TableCheckpointMetadata>,
    ) -> Result<Arc<dyn ErasedTable>, StateError> {
        Err(StateError::Other {
            table: "outside".to_string(),
            error: "this fixture exists to prove the seam compiles, not to store state".to_string(),
        })
    }

    fn merge_checkpoint_metadata(
        &self,
        _config: TableConfig,
        subtask_metadata: HashMap<u32, TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<TableCheckpointMetadata>, StateError> {
        Ok(
            (!subtask_metadata.is_empty()).then(|| TableCheckpointMetadata {
                table_type: self.kind.as_table_enum().into(),
                data: vec![subtask_metadata.len() as u8],
            }),
        )
    }

    fn committing_data(
        &self,
        _config: TableConfig,
        _table_metadata: &TableCheckpointMetadata,
    ) -> Option<HashMap<u32, Vec<u8>>> {
        None
    }

    fn files_to_keep(&self, table: ValidatedTable<'_>) -> Result<HashSet<String>, StateError> {
        Ok(HashSet::from([table.name().to_string()]))
    }

    /// A liveness answer in a format that is not parquet's, which is the point: the seam's
    /// vocabulary is names, so an outside backend reads its own payload and the protocol
    /// validates what comes back.
    fn table_data_files(
        &self,
        metadata: &TableCheckpointMetadata,
    ) -> Result<Vec<String>, LivenessRefusal> {
        if metadata.table_type() != self.kind.as_table_enum() {
            return Err(LivenessRefusal::WrongTableKind {
                state_backend: StateBackendSelector::StateEngine,
                declared: metadata.table_type(),
                serves: self.kind.as_table_enum(),
            });
        }
        if metadata.data.is_empty() {
            return Ok(Vec::new());
        }
        Ok(metadata
            .data
            .split(|byte| *byte == b',')
            .map(|name| String::from_utf8_lossy(name).into_owned())
            .collect())
    }

    async fn compact_data(
        &self,
        _config: TableConfig,
        _compaction_config: &CompactionConfig,
        _operator_metadata: &OperatorMetadata,
        _current_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableCheckpointMetadata>, StateError> {
        Ok(None)
    }
}

#[async_trait]
impl GlobalKeyValueProvider for OutsideProvider {
    /// This fixture's subject is the view seam; the load seam has a binary of its own,
    /// `tests/global_key_value_load_seam.rs`, where an outside loader is drained.
    async fn global_key_value_load(
        &self,
        _table_name: &str,
        _table: &dyn ErasedTable,
    ) -> Result<Box<dyn GlobalKeyValueLoad + Send>, StateError> {
        Err(StateError::Other {
            table: "outside".to_string(),
            error: "this fixture exists to prove the seam compiles, not to store state".to_string(),
        })
    }
}

#[async_trait]
impl ExpiringTimeKeyProvider for OutsideProvider {
    async fn expiring_time_key_view(
        &self,
        _table_name: &str,
        _table: &dyn ErasedTable,
        _state_tx: Sender<StateMessage>,
        _watermark: Option<SystemTime>,
    ) -> Result<Box<dyn ExpiringTimeKeyViewApi + Send>, StateError> {
        Ok(Box::new(OutsideView {
            source: vec![(at(1), batch(10)), (at(2), batch(20))],
            drain: None,
        }))
    }
}

/// A backend written outside this crate registers beside parquet, is found by its own
/// selector, and opens a view that streams one batch per poll.
#[tokio::test]
async fn a_backend_outside_this_crate_registers_and_serves() {
    let registry = ProviderRegistry::builder()
        .register_global_key_value(Arc::new(ParquetProvider::<GlobalKeyedTable>::new()))
        .and_then(|b| {
            b.register_expiring_keyed_time(Arc::new(ParquetProvider::<ExpiringTimeKeyTable>::new()))
        })
        .and_then(|b| {
            b.register_global_key_value(Arc::new(OutsideProvider {
                kind: TableKind::GlobalKeyValue,
            }))
        })
        .and_then(|b| {
            b.register_expiring_keyed_time(Arc::new(OutsideProvider {
                kind: TableKind::ExpiringKeyedTime,
            }))
        })
        .and_then(ProviderRegistryBuilder::build)
        .expect("two complete backends");

    for kind in TableKind::ALL {
        assert_eq!(
            registry
                .provider(StateBackendSelector::StateEngine, kind)
                .expect("the outside backend is registered")
                .selector(),
            StateBackendSelector::StateEngine,
        );
        assert_eq!(
            registry
                .provider(StateBackendSelector::Parquet, kind)
                .expect("parquet is registered")
                .selector(),
            StateBackendSelector::Parquet,
        );
    }

    // A table built through the seam by an external caller, which is what the view call
    // below is handed. Nothing here names a concrete table type: the seam passes
    // `&dyn ErasedTable`, and the outside provider ignores a table it did not build.
    let directory = std::env::temp_dir().join("arroyo-t09c-provider-seam");
    std::fs::create_dir_all(&directory).expect("temp dir");
    let storage: StorageProviderRef = Arc::new(
        arroyo_storage::StorageProvider::for_url(&format!("file://{}", directory.display()))
            .await
            .expect("local storage provider"),
    );
    let config = arroyo_state::global_table_config("t", "provider seam fixture")
        .remove("t")
        .expect("one table");
    let table = registry
        .provider(StateBackendSelector::Parquet, TableKind::GlobalKeyValue)
        .expect("parquet is registered")
        .table(
            config,
            Arc::new(arroyo_types::get_test_task_info()),
            storage,
            None,
        )
        .expect("a global key/value table");

    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let mut view = registry
        .expiring_time_key_provider(StateBackendSelector::StateEngine)
        .expect("the outside backend opens views")
        .expiring_time_key_view("t", &*table, tx, None)
        .await
        .expect("the outside view");

    assert_eq!(view.get_min_time(), Some(at(1)));

    let mut drained = vec![];
    let mut stream = view.all_batches_for_watermark(None);
    while let Some(item) = stream.next().await {
        let (timestamp, batch) = item.expect("drain");
        drained.push((
            timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("after the epoch")
                .as_secs(),
            value_of(&batch),
        ));
    }
    assert_eq!(drained, vec![(1, 10), (2, 20)]);
}

/// A registry holding the outside backend for both live kinds, and nothing else.
fn outside_registry() -> ProviderRegistry {
    ProviderRegistry::builder()
        .register_global_key_value(Arc::new(OutsideProvider {
            kind: TableKind::GlobalKeyValue,
        }))
        .and_then(|b| {
            b.register_expiring_keyed_time(Arc::new(OutsideProvider {
                kind: TableKind::ExpiringKeyedTime,
            }))
        })
        .and_then(ProviderRegistryBuilder::build)
        .expect("one complete backend")
}

/// One table's metadata in the outside backend's own payload format, which is not a protobuf
/// and not parquet's.
fn outside_metadata(kind: TableKind, names: &str) -> TableCheckpointMetadata {
    TableCheckpointMetadata {
        table_type: kind.as_table_enum().into(),
        data: names.as_bytes().to_vec(),
    }
}

/// The checkpoint protocol's GC liveness seam, implemented outside the crate that declares it
/// and used as the trait object that crate consumes.
///
/// Three boundaries at once, which is the point: the payload format is the outside backend's,
/// the registry and the resolver are `arroyo-state`'s, the trait is
/// `arroyo-state-protocol`'s, and this file is none of the three. If
/// [`CheckpointLiveness`] stopped being object-safe, if its vocabulary stopped naming only
/// public types, or if building a resolver required something `arroyo-state` does not export,
/// this test would not compile.
#[test]
fn an_outside_backends_liveness_answers_through_the_protocol_trait_object() {
    let registry = outside_registry();
    let liveness = ProviderLiveness::new(&registry, StateBackendSelector::StateEngine)
        .expect("the outside backend serves both live kinds");
    let object: &dyn CheckpointLiveness = &liveness;

    assert_eq!(object.state_backend(), StateBackendSelector::StateEngine);
    assert_eq!(
        object
            .table_data_files(&outside_metadata(TableKind::GlobalKeyValue, "a,b,c"))
            .expect("the outside backend reads its own payload"),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    assert_eq!(
        object
            .table_data_files(&outside_metadata(TableKind::ExpiringKeyedTime, ""))
            .expect("a table with no files is an answer"),
        Vec::<String>::new()
    );

    // A refusal crosses the boundary as a refusal, not as an empty list.
    let refusal = object
        .table_data_files(&TableCheckpointMetadata::default())
        .expect_err("a metadata declaring no live kind selects no provider");
    assert!(
        matches!(refusal, LivenessRefusal::UnservedTableKind { .. }),
        "{refusal:?}"
    );
    assert_eq!(
        refusal.to_string(),
        "the \"stateengine\" state backend has no implementation for MissingTableType table \
         metadata"
    );
}
