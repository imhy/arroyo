//! The parquet state backend as a [`StateBackendProvider`] (design item M11.D11, risk
//! M11.T09o).
//!
//! Every method here delegates to the [`ErasedTable`] associated function the matching
//! `TableEnum` arm already called, with the same arguments and no rewriting in between:
//! the provider is a place the dispatch decision is *made*, not a place the operation is
//! reimplemented. That is what makes the M11.T09k parity suites a comparison of two paths
//! to one implementation, and what makes "the default backend behaves exactly as before" a
//! property of the code rather than of a test that happened to pass.
//!
//! The provider is generic over the table type, so the two live kinds share one
//! implementation of the five kind-independent families. The two kind-specific families
//! are implemented only for the kind that has them: [`ExpiringTimeKeyProvider`] for
//! `ParquetProvider<ExpiringTimeKeyTable>`, and [`GlobalKeyValueProvider`] for
//! `ParquetProvider<GlobalKeyedTable>`.

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
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
use prost::Message;
use tokio::sync::mpsc::Sender;

use super::{ExpiringTimeKeyProvider, GlobalKeyValueProvider, StateBackendProvider, TableKind};
use crate::StateMessage;
use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::tables::expiring_time_key_view::ExpiringTimeKeyViewApi;
use crate::tables::global_key_value_load::GlobalKeyValueLoad;
use crate::tables::global_keyed_map::GlobalKeyedTable;
use crate::tables::{CompactionConfig, ErasedTable, Table};
use crate::validated::ValidatedTable;

mod sealed {
    /// Closes [`super::ParquetTable`] to the two table types this crate defines.
    pub trait Sealed {}
    impl Sealed for crate::tables::global_keyed_map::GlobalKeyedTable {}
    impl Sealed for crate::tables::expiring_time_key_map::ExpiringTimeKeyTable {}
}

/// A parquet-backed table type, and the live kind it is the implementation of.
///
/// Sealed: the two implementations below are exactly the two live [`TableKind`]s, and a
/// third parquet table type would have to be a third live kind, which the registry's
/// completeness rule would then also have to cover. `KIND` is checked against
/// [`ErasedTable::table_type`] by
/// `the_declared_kind_of_each_parquet_table_matches_its_wire_table_type`, so it cannot
/// drift into being a second, disagreeing statement of the same fact.
#[async_trait]
pub trait ParquetTable: ErasedTable + Sized + sealed::Sealed {
    /// The live kind this table implements.
    const KIND: TableKind;

    /// The typed table's own `data_files`, over the erased metadata the GC liveness seam
    /// speaks in.
    ///
    /// Declared here rather than derived from a `Table` bound because `Table` is this
    /// crate's private trait and this one is public; each implementation below is the
    /// decode-then-delegate and nothing else, exactly as [`Self::compact`] is.
    ///
    /// # Errors
    ///
    /// Returns [`LivenessRefusal::WrongTableKind`] when `metadata` declares another kind, and
    /// [`LivenessRefusal::UndecodablePayload`] when the bytes are not this kind's message.
    fn data_files(metadata: &TableCheckpointMetadata) -> Result<Vec<String>, LivenessRefusal>;

    /// [`ErasedTable::compact_data`], as a `Send` future.
    ///
    /// [`ErasedTable`] declares compaction as a bare `async fn`, whose future is not
    /// known to be `Send` for a generic implementor; a boxed `Send` future is what lets
    /// the one generic provider implementation below cover both table types. The body of
    /// each implementation is the delegation and nothing else.
    async fn compact(
        config: TableConfig,
        compaction_config: &CompactionConfig,
        operator_metadata: &OperatorMetadata,
        current_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableCheckpointMetadata>, StateError>;
}

#[async_trait]
impl ParquetTable for GlobalKeyedTable {
    const KIND: TableKind = TableKind::GlobalKeyValue;

    fn data_files(metadata: &TableCheckpointMetadata) -> Result<Vec<String>, LivenessRefusal> {
        Ok(<Self as Table>::data_files(&decode_checkpoint::<Self>(
            metadata,
        )?))
    }

    async fn compact(
        config: TableConfig,
        compaction_config: &CompactionConfig,
        operator_metadata: &OperatorMetadata,
        current_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableCheckpointMetadata>, StateError> {
        <Self as ErasedTable>::compact_data(
            config,
            compaction_config,
            operator_metadata,
            current_metadata,
        )
        .await
    }
}

#[async_trait]
impl ParquetTable for ExpiringTimeKeyTable {
    const KIND: TableKind = TableKind::ExpiringKeyedTime;

    fn data_files(metadata: &TableCheckpointMetadata) -> Result<Vec<String>, LivenessRefusal> {
        Ok(<Self as Table>::data_files(&decode_checkpoint::<Self>(
            metadata,
        )?))
    }

    async fn compact(
        config: TableConfig,
        compaction_config: &CompactionConfig,
        operator_metadata: &OperatorMetadata,
        current_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableCheckpointMetadata>, StateError> {
        <Self as ErasedTable>::compact_data(
            config,
            compaction_config,
            operator_metadata,
            current_metadata,
        )
        .await
    }
}

/// Decodes `metadata` as `T`'s checkpoint message, refusing a payload that declares another
/// kind.
///
/// The kind check comes first and is not a redundant assertion. `prost` skips fields it does
/// not recognise, so another backend's — or another kind's — bytes routinely decode into a
/// well-formed message of this one, and the file list that falls out is short rather than
/// wrong-looking. On the leader cleanup path a short list is not a diagnostic, it is a set of
/// files that stopped being protected, which is why this refuses instead of decoding.
fn decode_checkpoint<T: Table>(
    metadata: &TableCheckpointMetadata,
) -> Result<T::TableCheckpointMessage, LivenessRefusal> {
    let declared = metadata.table_type();
    let serves = <T as Table>::table_type();
    if declared != serves {
        return Err(LivenessRefusal::WrongTableKind {
            state_backend: StateBackendSelector::Parquet,
            declared,
            serves,
        });
    }

    T::TableCheckpointMessage::decode(metadata.data.as_slice()).map_err(|source| {
        LivenessRefusal::UndecodablePayload {
            table_type: declared,
            source,
        }
    })
}

/// The [`StateBackendSelector::Parquet`] provider for one live table kind.
///
/// Holds nothing: every operation takes the config, task info, storage and checkpoint
/// metadata it needs, exactly as the static dispatch it replaces did. Two of these — one
/// per kind — make up [`crate::provider::ProviderRegistry::parquet_default`].
pub struct ParquetProvider<T: ParquetTable> {
    table: PhantomData<fn() -> T>,
}

impl<T: ParquetTable> ParquetProvider<T> {
    /// The provider for `T`'s kind.
    pub const fn new() -> Self {
        Self { table: PhantomData }
    }
}

impl<T: ParquetTable> Default for ParquetProvider<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<T: ParquetTable> StateBackendProvider for ParquetProvider<T> {
    fn selector(&self) -> StateBackendSelector {
        StateBackendSelector::Parquet
    }

    fn table_kind(&self) -> TableKind {
        T::KIND
    }

    fn table(
        &self,
        config: TableConfig,
        task_info: Arc<TaskInfo>,
        storage: StorageProviderRef,
        checkpoint: Option<TableCheckpointMetadata>,
    ) -> Result<Arc<dyn ErasedTable>, StateError> {
        Ok(Arc::new(<T as ErasedTable>::from_config(
            config, task_info, storage, checkpoint,
        )?) as Arc<dyn ErasedTable>)
    }

    fn merge_checkpoint_metadata(
        &self,
        config: TableConfig,
        subtask_metadata: HashMap<u32, TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<TableCheckpointMetadata>, StateError> {
        <T as ErasedTable>::merge_checkpoint_metadata(config, subtask_metadata)
    }

    fn committing_data(
        &self,
        config: TableConfig,
        table_metadata: &TableCheckpointMetadata,
    ) -> Option<HashMap<u32, Vec<u8>>> {
        <T as ErasedTable>::committing_data(config, table_metadata)
    }

    fn files_to_keep(&self, table: ValidatedTable<'_>) -> Result<HashSet<String>, StateError> {
        <T as ErasedTable>::files_to_keep(table)
    }

    fn table_data_files(
        &self,
        metadata: &TableCheckpointMetadata,
    ) -> Result<Vec<String>, LivenessRefusal> {
        <T as ParquetTable>::data_files(metadata)
    }

    async fn compact_data(
        &self,
        config: TableConfig,
        compaction_config: &CompactionConfig,
        operator_metadata: &OperatorMetadata,
        current_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableCheckpointMetadata>, StateError> {
        T::compact(
            config,
            compaction_config,
            operator_metadata,
            current_metadata,
        )
        .await
    }
}

#[async_trait]
impl ExpiringTimeKeyProvider for ParquetProvider<ExpiringTimeKeyTable> {
    /// Opens `ExpiringTimeKeyTable::get_view`'s view, boxed as the seam's trait object.
    ///
    /// The downcast is how a provider recognises a table it built: `table` reaches this
    /// call as `&dyn ErasedTable` out of [`crate::tables::table_manager::TableManager`]'s
    /// table map, and only a table this backend constructed can produce a view of this
    /// backend's state. A table another backend built fails the downcast and is refused
    /// with the same [`StateError::WrongTableKind`] a caller has always received for
    /// asking a table for the wrong kind of view.
    async fn expiring_time_key_view(
        &self,
        table_name: &str,
        table: &dyn ErasedTable,
        state_tx: Sender<StateMessage>,
        watermark: Option<SystemTime>,
    ) -> Result<Box<dyn ExpiringTimeKeyViewApi + Send>, StateError> {
        let table = table
            .as_any()
            .downcast_ref::<ExpiringTimeKeyTable>()
            .ok_or_else(|| StateError::WrongTableKind {
                table: table_name.to_string(),
                expected: "expiring_time_key_table",
            })?;
        Ok(Box::new(table.get_view(state_tx, watermark).await?)
            as Box<dyn ExpiringTimeKeyViewApi + Send>)
    }
}

#[async_trait]
impl GlobalKeyValueProvider for ParquetProvider<GlobalKeyedTable> {
    /// Starts `GlobalKeyedTable`'s own bounded-page loader over the table's restored
    /// checkpoint files, boxed as the seam's trait object.
    ///
    /// The downcast is how a provider recognises a table it built, exactly as it is for
    /// views: `table` reaches this call as `&dyn ErasedTable` out of
    /// [`crate::tables::table_manager::TableManager`]'s table map, and only a table this
    /// backend constructed holds files this backend can read. A table another backend
    /// built fails the downcast and is refused with the same [`StateError::WrongTableKind`]
    /// the manager's own downcast produced before the lookup moved here.
    async fn global_key_value_load(
        &self,
        table_name: &str,
        table: &dyn ErasedTable,
    ) -> Result<Box<dyn GlobalKeyValueLoad + Send>, StateError> {
        let table = table
            .as_any()
            .downcast_ref::<GlobalKeyedTable>()
            .ok_or_else(|| StateError::WrongTableKind {
                table: table_name.to_string(),
                expected: "global_keyed_state",
            })?;
        Ok(Box::new(table.loader()) as Box<dyn GlobalKeyValueLoad + Send>)
    }
}
