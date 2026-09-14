//! The M11.D15c bounded-page load seam, implemented and used the way another crate would.
//!
//! Nothing here is parquet and nothing here is `arroyo-state`'s: the loader below is written
//! against the public seam alone — a page builder, a limits value, a peak — registered
//! through a provider that is also written here, and reached through the registry. If
//! [`GlobalKeyValueLoad`] or [`GlobalKeyValueProvider`] stopped being object-safe, if a
//! method stopped naming only public types, or if building a page required something this
//! crate does not export, this file would not compile.
//!
//! It also pins the two things a caller outside the crate depends on: that the storage type
//! really is `Box<dyn GlobalKeyValueLoad + Send>` — the loader is moved across a
//! `tokio::spawn` boundary, which only a `Send` box can be — and that a drain to exhaustion
//! stays exhausted.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{
    OperatorMetadata, TableCheckpointMetadata, TableConfig, TableSubtaskCheckpointMetadata,
};
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_state::StateMessage;
use arroyo_state::provider::parquet::ParquetProvider;
use arroyo_state::provider::{
    ExpiringTimeKeyProvider, GlobalKeyValueProvider, ProviderRegistry, ProviderRegistryBuilder,
    StateBackendProvider, TableKind,
};
use arroyo_state::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use arroyo_state::tables::expiring_time_key_view::ExpiringTimeKeyViewApi;
use arroyo_state::tables::global_key_value_load::{
    GlobalKeyValueLoad, GlobalKeyValuePage, GlobalKeyValuePageBuilder, LoadPageLimits,
    LoadPagePeak, MAX_LOAD_PAGE_BYTES, MAX_LOAD_PAGE_ENTRIES,
};
use arroyo_state::tables::global_keyed_map::GlobalKeyedTable;
use arroyo_state::tables::{CompactionConfig, ErasedTable};
use arroyo_state::validated::ValidatedTable;
use arroyo_state_protocol::gc::liveness::LivenessRefusal;
use arroyo_storage::StorageProviderRef;
use arroyo_types::TaskInfo;
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

/// The bound the outside backend promises: three entries a page, sixteen payload bytes.
///
/// Tighter than the seam's default in both units, which is the only direction a backend may
/// narrow it in.
fn outside_limits() -> LoadPageLimits {
    LoadPageLimits::new(3, 16).expect("tighter than the seam's default in both units")
}

/// One entry as it crosses the seam.
type Entry = (Vec<u8>, Vec<u8>);

/// One of this backend's sources: its name, the state version its entries were written
/// under, and those entries.
type Source = (String, u32, Vec<Entry>);

/// A backend with no storage at all: a fixed list of `(key, value)` entries per source,
/// paged through the seam's own builder.
struct OutsideLoad {
    sources: std::vec::IntoIter<Source>,
    open: Option<(GlobalKeyValuePageBuilder, std::vec::IntoIter<Entry>)>,
    peak: LoadPagePeak,
}

impl OutsideLoad {
    fn new(sources: Vec<Source>) -> Self {
        Self {
            sources: sources.into_iter(),
            open: None,
            peak: LoadPagePeak::ZERO,
        }
    }

    fn emit(&mut self, page: GlobalKeyValuePage) -> Option<GlobalKeyValuePage> {
        self.peak.record(&page);
        Some(page)
    }
}

#[async_trait]
impl GlobalKeyValueLoad for OutsideLoad {
    async fn next_page(&mut self) -> Result<Option<GlobalKeyValuePage>, StateError> {
        loop {
            let Some((builder, entries)) = &mut self.open else {
                let Some((name, version, entries)) = self.sources.next() else {
                    return Ok(None);
                };
                let builder = GlobalKeyValuePageBuilder::new(name, version, outside_limits());
                let header = builder.header();
                self.open = Some((builder, entries.into_iter()));
                return Ok(self.emit(header));
            };

            match entries.next() {
                Some((key, value)) => {
                    if let Some(page) = builder.push(key, value) {
                        return Ok(self.emit(page));
                    }
                }
                None => {
                    let (mut builder, _) = self.open.take().expect("matched above");
                    if let Some(page) = builder.take() {
                        return Ok(self.emit(page));
                    }
                }
            }
        }
    }

    fn page_limits(&self) -> LoadPageLimits {
        outside_limits()
    }

    fn peak_page(&self) -> LoadPagePeak {
        self.peak
    }
}

/// A backend written outside this crate, for one live table kind.
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
        _subtask_metadata: HashMap<u32, TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<TableCheckpointMetadata>, StateError> {
        Ok(None)
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

    fn table_data_files(
        &self,
        _metadata: &TableCheckpointMetadata,
    ) -> Result<Vec<String>, LivenessRefusal> {
        Ok(Vec::new())
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
    /// Hands out the outside loader, ignoring `table`: this backend's state is its own, and
    /// the seam passes `&dyn ErasedTable` precisely so a backend need not name a table type
    /// it did not build.
    async fn global_key_value_load(
        &self,
        _table_name: &str,
        _table: &dyn ErasedTable,
    ) -> Result<Box<dyn GlobalKeyValueLoad + Send>, StateError> {
        Ok(Box::new(OutsideLoad::new(vec![
            (
                "engine://subtask-0".to_string(),
                0,
                (0..4u8).map(|i| (vec![i], vec![i, i])).collect(),
            ),
            ("engine://subtask-1".to_string(), 3, Vec::new()),
            (
                "engine://subtask-2".to_string(),
                3,
                vec![(vec![9], vec![9, 9])],
            ),
        ])) as Box<dyn GlobalKeyValueLoad + Send>)
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
        Err(StateError::Other {
            table: "outside".to_string(),
            error: "this fixture opens no views".to_string(),
        })
    }
}

/// A registry serving parquet and the outside backend, both for both live kinds.
fn registry() -> ProviderRegistry {
    ProviderRegistry::builder()
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
        .expect("two complete backends")
}

/// A table built through the seam, to hand the loader call something of the right shape.
async fn a_table() -> Arc<dyn ErasedTable> {
    let directory = std::env::temp_dir().join("arroyo-t09c01-load-seam");
    std::fs::create_dir_all(&directory).expect("temp dir");
    let storage: StorageProviderRef = Arc::new(
        arroyo_storage::StorageProvider::for_url(&format!("file://{}", directory.display()))
            .await
            .expect("local storage provider"),
    );
    let config = arroyo_state::global_table_config("t", "load seam fixture")
        .remove("t")
        .expect("one table");
    registry()
        .provider(StateBackendSelector::Parquet, TableKind::GlobalKeyValue)
        .expect("parquet is registered")
        .table(
            config,
            Arc::new(arroyo_types::get_test_task_info()),
            storage,
            None,
        )
        .expect("a global key/value table")
}

/// A backend written outside this crate registers on the global slot, is found by its own
/// selector, and is drained to exhaustion through the seam.
///
/// The page sequence is the seam's contract, as closed forms of the bound this backend
/// declared: each source is announced by an empty page carrying its name and state version,
/// the four entries of the first source come back as pages of 3 and 1, the empty source
/// contributes only its announcement, and the third source's one entry is one page.
#[tokio::test]
async fn a_backend_outside_this_crate_loads_through_the_seam() {
    let registry = registry();

    let table = a_table().await;
    let mut loader: Box<dyn GlobalKeyValueLoad + Send> = registry
        .global_key_value_provider(StateBackendSelector::StateEngine)
        .expect("the outside backend serves global key/value tables")
        .global_key_value_load("t", &*table)
        .await
        .expect("the outside loader");

    assert_eq!(loader.page_limits(), outside_limits());
    assert!(outside_limits().max_entries() <= MAX_LOAD_PAGE_ENTRIES);
    assert!(outside_limits().max_payload_bytes() <= MAX_LOAD_PAGE_BYTES);

    let mut shape = Vec::new();
    let mut entries = Vec::new();
    while let Some(page) = loader.next_page().await.expect("a page") {
        assert!(
            loader.page_limits().admits(&page),
            "a page outside the bound its loader declared"
        );
        shape.push((page.source().to_string(), page.state_version(), page.len()));
        entries.extend(page.into_entries());
    }

    assert_eq!(
        shape,
        vec![
            ("engine://subtask-0".to_string(), 0, 0),
            ("engine://subtask-0".to_string(), 0, 3),
            ("engine://subtask-0".to_string(), 0, 1),
            ("engine://subtask-1".to_string(), 3, 0),
            ("engine://subtask-2".to_string(), 3, 0),
            ("engine://subtask-2".to_string(), 3, 1),
        ],
    );
    assert_eq!(
        entries,
        vec![
            (vec![0], vec![0, 0]),
            (vec![1], vec![1, 1]),
            (vec![2], vec![2, 2]),
            (vec![3], vec![3, 3]),
            (vec![9], vec![9, 9]),
        ],
    );
    assert_eq!(loader.peak_page().entries(), 3);
    assert_eq!(loader.peak_page().payload_bytes(), 9);

    // Exhaustion is stable: a drained loader keeps answering `None`.
    for _ in 0..3 {
        assert!(loader.next_page().await.expect("still exhausted").is_none());
    }
}

/// The loader really is stored as `Box<dyn GlobalKeyValueLoad + Send>`, which is what lets a
/// caller move one onto another task.
///
/// A box that were not `Send` could not be moved into `tokio::spawn`, so this failing to
/// compile is the whole assertion; the counts it returns are there so the task is not
/// optimised into nothing.
#[tokio::test]
async fn a_boxed_loader_moves_across_a_task_boundary() {
    let table = a_table().await;
    let loader: Box<dyn GlobalKeyValueLoad + Send> = registry()
        .global_key_value_provider(StateBackendSelector::StateEngine)
        .expect("the outside backend serves global key/value tables")
        .global_key_value_load("t", &*table)
        .await
        .expect("the outside loader");

    let (pages, entries) = tokio::spawn(async move {
        let mut loader = loader;
        let mut pages = 0;
        let mut entries = 0;
        while let Some(page) = loader.next_page().await.expect("a page") {
            pages += 1;
            entries += page.len();
        }
        (pages, entries)
    })
    .await
    .expect("the spawned drain");

    assert_eq!(pages, 6);
    assert_eq!(entries, 5);
}
