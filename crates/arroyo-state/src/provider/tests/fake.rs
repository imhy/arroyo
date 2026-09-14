//! A provider that implements nothing but its own identity, for the registry suites.
//!
//! Every family answers with `id`, so a test can say *which* provider a lookup returned
//! rather than only that one came back. It is registered under the `(selector, kind)` it
//! reports, exactly as a real provider is, which is what lets one type exercise both the
//! agreeing and the mis-filed registrations.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{
    OperatorMetadata, TableCheckpointMetadata, TableConfig, TableSubtaskCheckpointMetadata,
};
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_state_protocol::gc::liveness::LivenessRefusal;
use arroyo_storage::StorageProviderRef;
use arroyo_types::TaskInfo;
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

use crate::StateMessage;
use crate::provider::{
    ExpiringTimeKeyProvider, GlobalKeyValueProvider, ProviderRegistry, ProviderRegistryBuilder,
    StateBackendProvider, TableKind,
};
use crate::tables::expiring_time_key_view::ExpiringTimeKeyViewApi;
use crate::tables::global_key_value_load::GlobalKeyValueLoad;
use crate::tables::{CompactionConfig, ErasedTable};
use crate::validated::ValidatedTable;

pub(super) struct FakeProvider {
    pub(super) selector: StateBackendSelector,
    pub(super) kind: TableKind,
    pub(super) id: &'static str,
}

impl FakeProvider {
    /// The fake as the trait object a global key/value registration takes.
    ///
    /// `kind` is a parameter rather than fixed so that one constructor serves both the
    /// agreeing registrations and the mis-filed one.
    pub(super) fn erased(
        selector: StateBackendSelector,
        kind: TableKind,
        id: &'static str,
    ) -> Arc<dyn GlobalKeyValueProvider> {
        Arc::new(Self { selector, kind, id })
    }

    pub(super) fn expiring(
        selector: StateBackendSelector,
        id: &'static str,
    ) -> Arc<dyn ExpiringTimeKeyProvider> {
        Arc::new(Self {
            selector,
            kind: TableKind::ExpiringKeyedTime,
            id,
        })
    }
}

#[async_trait]
impl StateBackendProvider for FakeProvider {
    fn selector(&self) -> StateBackendSelector {
        self.selector
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
            table: self.id.to_string(),
            error: "fake provider builds no tables".to_string(),
        })
    }

    fn merge_checkpoint_metadata(
        &self,
        _config: TableConfig,
        _subtask_metadata: HashMap<u32, TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<TableCheckpointMetadata>, StateError> {
        Ok(Some(TableCheckpointMetadata {
            table_type: self.kind.as_table_enum().into(),
            data: self.id.as_bytes().to_vec(),
        }))
    }

    /// The identity probe every lookup assertion reads.
    fn committing_data(
        &self,
        _config: TableConfig,
        _table_metadata: &TableCheckpointMetadata,
    ) -> Option<HashMap<u32, Vec<u8>>> {
        Some(HashMap::from([(0, self.id.as_bytes().to_vec())]))
    }

    fn files_to_keep(&self, _table: ValidatedTable<'_>) -> Result<HashSet<String>, StateError> {
        Ok(HashSet::from([self.id.to_string()]))
    }

    /// The identity probe again, so a liveness assertion can say *which* provider answered a
    /// manifest entry rather than only that some provider did. A real backend reads the
    /// payload; this one ignores it, which is the whole point of a fake whose subject is
    /// routing.
    fn table_data_files(
        &self,
        _metadata: &TableCheckpointMetadata,
    ) -> Result<Vec<String>, LivenessRefusal> {
        Ok(vec![self.id.to_string()])
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
impl GlobalKeyValueProvider for FakeProvider {
    async fn global_key_value_load(
        &self,
        _table_name: &str,
        _table: &dyn ErasedTable,
    ) -> Result<Box<dyn GlobalKeyValueLoad + Send>, StateError> {
        Err(StateError::Other {
            table: self.id.to_string(),
            error: "fake provider loads no state".to_string(),
        })
    }
}

#[async_trait]
impl ExpiringTimeKeyProvider for FakeProvider {
    async fn expiring_time_key_view(
        &self,
        _table_name: &str,
        _table: &dyn ErasedTable,
        _state_tx: Sender<StateMessage>,
        _watermark: Option<SystemTime>,
    ) -> Result<Box<dyn ExpiringTimeKeyViewApi + Send>, StateError> {
        Err(StateError::Other {
            table: self.id.to_string(),
            error: "fake provider opens no views".to_string(),
        })
    }
}

/// Which provider a lookup returned, read through the identity probe.
pub(super) fn id_of(provider: &dyn StateBackendProvider) -> Vec<u8> {
    provider
        .committing_data(TableConfig::default(), &TableCheckpointMetadata::default())
        .expect("the fake always reports an identity")
        .remove(&0)
        .expect("the fake reports its identity under subtask 0")
}

/// The address of the provider object itself, so a test can say two lookups produced the
/// same object rather than two objects that answer alike.
pub(super) fn address_of(provider: &dyn StateBackendProvider) -> usize {
    provider as *const dyn StateBackendProvider as *const () as usize
}

/// A registry with a distinctly-identified fake in all four slots.
pub(super) fn fake_registry() -> ProviderRegistry {
    ProviderRegistry::builder()
        .register_global_key_value(FakeProvider::erased(
            StateBackendSelector::Parquet,
            TableKind::GlobalKeyValue,
            "parquet-global",
        ))
        .and_then(|b| {
            b.register_expiring_keyed_time(FakeProvider::expiring(
                StateBackendSelector::Parquet,
                "parquet-expiring",
            ))
        })
        .and_then(|b| {
            b.register_global_key_value(FakeProvider::erased(
                StateBackendSelector::StateEngine,
                TableKind::GlobalKeyValue,
                "engine-global",
            ))
        })
        .and_then(|b| {
            b.register_expiring_keyed_time(FakeProvider::expiring(
                StateBackendSelector::StateEngine,
                "engine-expiring",
            ))
        })
        .and_then(ProviderRegistryBuilder::build)
        .expect("four distinct keys")
}
