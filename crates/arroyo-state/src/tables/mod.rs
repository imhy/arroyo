use crate::ownership::AcknowledgedFence;
use crate::validated::ValidatedTable;
use crate::{CheckpointMessage, DataOperation, TableData};
use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{
    OperatorMetadata, TableCheckpointMetadata, TableConfig, TableEnum,
    TableSubtaskCheckpointMetadata,
};
use arroyo_storage::StorageProviderRef;
use arroyo_types::{CheckpointFilePathLayout, Data, TaskInfo};
use parquet::format::KeyValue;
use prost::Message;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::debug;

pub mod expiring_time_key_map;
pub mod expiring_time_key_view;
pub mod global_key_value_load;
pub mod global_keyed_map;
pub mod table_manager;

/// Trait for bincode'd state struct that can be migrated from earlier versions
pub trait MigratableState: Data {
    const VERSION: u32;

    type PreviousVersion: Data;

    fn migrate(previous: Self::PreviousVersion) -> Result<Self, StateError>;
}

#[derive(Default)]
pub struct CheckpointParquetMetadata {
    pub state_version: u32,
}

const VERSION_KEY: &str = "version";

impl From<CheckpointParquetMetadata> for Option<Vec<KeyValue>> {
    fn from(value: CheckpointParquetMetadata) -> Self {
        Some(vec![KeyValue::new(
            VERSION_KEY.to_string(),
            value.state_version.to_string(),
        )])
    }
}

impl From<Option<&Vec<KeyValue>>> for CheckpointParquetMetadata {
    fn from(value: Option<&Vec<KeyValue>>) -> Self {
        let state_version = value
            .and_then(|v| v.iter().find(|f| f.key == VERSION_KEY))
            .and_then(|kv| kv.value.as_ref())
            .and_then(|v| u32::from_str(v).ok())
            .unwrap_or_default();

        Self { state_version }
    }
}

pub(crate) fn table_checkpoint_path(
    task_info: &TaskInfo,
    operator_id: &str,
    table: &str,
    subtask_index: usize,
    epoch: u32,
    compacted: bool,
) -> String {
    task_info.checkpoint_file_path_layout.table_checkpoint_path(
        &task_info.job_id,
        operator_id,
        table,
        subtask_index,
        epoch,
        compacted,
    )
}

pub(crate) fn table_checkpoint_path_with_layout(
    layout: &CheckpointFilePathLayout,
    job_id: &str,
    operator_id: &str,
    table: &str,
    subtask_index: usize,
    epoch: u32,
    compacted: bool,
) -> String {
    layout.table_checkpoint_path(job_id, operator_id, table, subtask_index, epoch, compacted)
}

pub struct DataTuple<K, V> {
    pub timestamp: SystemTime,
    pub key: K,
    pub value: Option<V>,
    pub operation: DataOperation,
}

/// BlindDataTuple's key and value are not decoded
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BlindDataTuple {
    pub key_hash: u64,
    pub timestamp: SystemTime,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub operation: DataOperation,
}

#[async_trait::async_trait]
pub(crate) trait Table: Send + Sync + 'static + Clone {
    // A stateful struct responsible for taking the checkpoint for a single epoch
    // contains an associated type that is a protobuf for the subtask checkpoint metadata.
    type Checkpointer: TableEpochCheckpointer<
        SubTableCheckpointMessage = Self::TableSubtaskCheckpointMetadata,
    >;
    // Protobuf message containing any configuration for the table.
    type ConfigMessage: prost::Message + Default;
    // A protobuf holding all necessary data for restoring from a specific epoch.
    // Will be produced by the controller checkpointing logic and read by subtasks when restoring from checkpoint.
    type TableCheckpointMessage: prost::Message + Default;

    type TableSubtaskCheckpointMetadata: prost::Message + Default;

    // produce the Table based on the
    // * config: (table specific configuration, such as retention duration),
    // * task_info: subtask specific info, including job_id, operator_id, and subtask_index
    // * checkpoint_message: If restoring from a checkpoint, the checkpoint data for that checkpoint's epoch.
    fn from_config(
        config: Self::ConfigMessage,
        task_info: Arc<TaskInfo>,
        storage_provider: StorageProviderRef,
        checkpoint_message: Option<Self::TableCheckpointMessage>,
        state_version: u32,
    ) -> Result<Self, StateError>
    where
        Self: Sized;
    // Returns a stateful struct that processes new data to checkpoint,
    // finishes said data, and returns a metadata protobuf.
    // the metadata protobuf should be sufficient to know all checkpoint data for the table that
    // the subtask cares about at that epoch, including previously written data,
    // which will be determined from the previous metadata.
    fn epoch_checkpointer(
        &self,
        epoch: u32,
        previous_metadata: Option<Self::TableSubtaskCheckpointMetadata>,
    ) -> Result<Self::Checkpointer, StateError>;
    // A controller method to merge the metadata from each subtask into a single Table metadata.
    // Will do things like dedup files and compute overall min and max watermarks.
    fn merge_checkpoint_metadata(
        config: Self::ConfigMessage,
        subtask_metadata: HashMap<u32, Self::TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<Self::TableCheckpointMessage>, StateError>;
    // compute the subtask metadata from the overall table metadata.
    // This is needed because of repartitioning, which means a subtask might need to read data "owned" by other subtasks in the previous epoch.
    fn subtask_metadata_from_table(
        &self,
        table_metadata: Self::TableCheckpointMessage,
    ) -> Result<Option<Self::TableSubtaskCheckpointMetadata>, StateError>;

    fn apply_compacted_checkpoint(
        &self,
        epoch: u32,
        compacted_checkpoint: Self::TableSubtaskCheckpointMetadata,
        subtask_metadata: Self::TableSubtaskCheckpointMetadata,
    ) -> Result<Self::TableSubtaskCheckpointMetadata, StateError>;

    fn table_type() -> TableEnum;

    fn task_info(&self) -> Arc<TaskInfo>;

    /// The data files `checkpoint` names, in the order its payload records them.
    ///
    /// This is the one statement of which field of this table's checkpoint payload holds a
    /// file name. Both questions that ask it are derived from it rather than repeating it:
    /// [`Self::files_to_keep`] deduplicates it into the set a checkpoint cleanup subtracts,
    /// and the provider's `table_data_files` hands the ordered list to leader GC's liveness
    /// seam, which validates each name into a `CheckpointRef` of its own.
    fn data_files(checkpoint: &Self::TableCheckpointMessage) -> Vec<String>;

    fn files_to_keep(
        config: Self::ConfigMessage,
        checkpoint: Self::TableCheckpointMessage,
    ) -> Result<HashSet<String>, StateError>;

    async fn compact_data(
        config: Self::ConfigMessage,
        compaction_config: &CompactionConfig,
        operator_metadata: &OperatorMetadata,
        current_metadata: Self::TableCheckpointMessage,
    ) -> Result<Option<Self::TableCheckpointMessage>, StateError>;

    fn committing_data(
        _config: Self::ConfigMessage,
        _table_metadata: Self::TableCheckpointMessage,
    ) -> Option<HashMap<u32, Vec<u8>>>
    where
        Self: Sized,
    {
        None
    }
}

pub struct CompactionConfig {
    pub storage_provider: StorageProviderRef,
    pub compact_generations: HashSet<u64>,
    pub min_compaction_epochs: usize,
    pub file_path_layout: CheckpointFilePathLayout,
}

pub trait ErasedTable: Send + Sync + 'static {
    // produce the Table based on the
    // * config: (table specific configuration, such as retention duration),
    // * task_info: subtask specific info, including job_id, operator_id, and subtask_index
    // * checkpoint_message: If restoring from a checkpoint, the checkpoint data for that checkpoint's epoch.
    fn from_config(
        config: TableConfig,
        task_info: Arc<TaskInfo>,
        storage_provider: StorageProviderRef,
        checkpoint_message: Option<TableCheckpointMetadata>,
    ) -> Result<Self, StateError>
    where
        Self: Sized;
    // Returns a stateful struct that processes new data to checkpoint,
    // finishes said data, and returns a metadata protobuf.
    // the metadata protobuf should be sufficient to know all checkpoint data for the table that
    // the subtask cares about at that epoch, including previously written data,
    // which will be determined from the previous metadata.
    fn epoch_checkpointer(
        &self,
        epoch: u32,
        previous_metadata: Option<TableSubtaskCheckpointMetadata>,
    ) -> Result<Box<dyn ErasedCheckpointer>, StateError>;
    // A controller method to merge the metadata from each subtask into a single Table metadata.
    // Will do things like dedup files and compute overall min and max watermarks.
    fn merge_checkpoint_metadata(
        config: TableConfig,
        subtask_metadata: HashMap<u32, TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<TableCheckpointMetadata>, StateError>
    where
        Self: Sized;
    // compute the subtask metadata from the overall table metadata.
    // This is needed because of repartitioning, which means a subtask might need to read data "owned" by other subtasks in the previous epoch.
    fn subtask_metadata_from_table(
        &self,
        table_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableSubtaskCheckpointMetadata>, StateError>;

    fn table_type() -> TableEnum
    where
        Self: Sized;

    fn checked_proto_decode<M: Message + Default>(
        table_type: TableEnum,
        data: Vec<u8>,
    ) -> Result<M, StateError>
    where
        Self: Sized,
    {
        if Self::table_type() != table_type {
            return Err(StateError::Other {
                table: "".to_string(),
                error: format!(
                    "mismatched table type, expected type {:?}, got {:?}",
                    Self::table_type(),
                    table_type
                ),
            });
        }

        Message::decode(&mut data.as_slice()).map_err(|e| StateError::Other {
            table: "".to_string(),
            error: format!("Failed to deserialize table config: {e:?}"),
        })
    }

    /// The files this table's checkpoint metadata references.
    ///
    /// Takes a [`ValidatedTable`] rather than a config and a metadata (design item
    /// M11.D39c). This is the classification a checkpoint cleanup subtracts to decide which
    /// of a job's files it may delete, so it must not be reachable from a pair of objects
    /// that no whole-checkpoint check covered: a foreign backend's metadata read through
    /// this backend's file layout names the wrong files, and the deletion that follows is
    /// not recoverable. A [`ValidatedTable`] has no public constructor and is only ever
    /// borrowed out of a token, which is what makes that unreachable rather than merely
    /// discouraged.
    fn files_to_keep(table: ValidatedTable<'_>) -> Result<HashSet<String>, StateError>
    where
        Self: Sized;

    fn as_any(&self) -> &dyn Any;

    #[allow(async_fn_in_trait)]
    async fn compact_data(
        config: TableConfig,
        compaction_config: &CompactionConfig,
        operator_metadata: &OperatorMetadata,
        current_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableCheckpointMetadata>, StateError>
    where
        Self: Sized;

    fn committing_data(
        config: TableConfig,
        table_metadata: &TableCheckpointMetadata,
    ) -> Option<HashMap<u32, Vec<u8>>>
    where
        Self: Sized;

    fn apply_compacted_checkpoint(
        &self,
        epoch: u32,
        compacted_checkpoint: TableSubtaskCheckpointMetadata,
        subtask_metadata: TableSubtaskCheckpointMetadata,
    ) -> Result<TableSubtaskCheckpointMetadata, StateError>;

    /// The barrier hook: called once per checkpoint barrier for every table of the subtask,
    /// synchronously on the operator task, before the checkpoint is enqueued for the flusher
    /// (plan M11.T10b.01; design M11.D12 and M11.D13a as corrected by ruling M11.T10R2).
    ///
    /// [`TableManager::checkpoint`](table_manager::TableManager::checkpoint) makes the call on
    /// the operator task — for a chained operator after its `handle_checkpoint`, for a source
    /// from `SourceOperator::start_checkpoint` — so it is ordered after every write the
    /// operator made before the barrier and before every write it makes after it. The flusher's [`ErasedCheckpointer::finish`] for the same epoch runs later,
    /// on another task, by which time the operator may have written more; a table that must
    /// cut its state exactly at the barrier cuts it here.
    ///
    /// `checkpoint` is the message the flusher will hand to this epoch's `finish` calls — the
    /// same epoch, `min_epoch`, `then_stop` and watermark. `acknowledged_fence` is the worker's
    /// acknowledged lifecycle fence, the ownership generation (ruling M11.T10R6): it reads
    /// the current value, and a table that keeps a clone can read it again at any later time.
    ///
    /// The default does nothing, and every built-in table inherits it, so parquet checkpoints
    /// exactly as it did before the hook existed.
    ///
    /// # Errors
    ///
    /// An `Err` refuses the checkpoint. No further table's hook runs for this barrier, the
    /// checkpoint is not enqueued, and the flusher — on reaching the refusal at the
    /// checkpoint's position in the state channel — fails the task with this error
    /// (`ControlResp::TaskFailed`) without calling any table's `finish` for the epoch, so the
    /// subtask never reports the checkpoint complete. The refusal does not itself stop the
    /// operator task: it keeps processing until the controller acts on the failure or its
    /// next use of the state channel finds the flusher gone, so a table that refuses a barrier
    /// must refuse the writes that follow it as well.
    #[allow(unused_variables)]
    fn on_checkpoint_barrier(
        &self,
        checkpoint: &CheckpointMessage,
        acknowledged_fence: &AcknowledgedFence,
    ) -> Result<(), StateError> {
        Ok(())
    }

    /// The restored hook: called once for every table of the subtask when the operator
    /// restored from a checkpoint, with that checkpoint's epoch (ruling M11.T10R7).
    ///
    /// [`TableManager::load`](table_manager::TableManager::load) makes the call after every
    /// table is constructed and before the flusher starts, so before the first
    /// [`Self::epoch_checkpointer`] call and before any view of the table exists. A table with
    /// no state of its own in the restored checkpoint is called too. A subtask that starts
    /// fresh, with nothing to restore, never calls it. The flusher's first epoch after a
    /// restore is `epoch + 1`.
    ///
    /// The default does nothing, and every built-in table inherits it.
    ///
    /// # Errors
    ///
    /// An `Err` fails `TableManager::load` with this error, before the flusher is started — for
    /// example when the state a table was constructed from belongs to a different epoch than
    /// the checkpoint the operator restored.
    #[allow(unused_variables)]
    fn restored(&self, epoch: u32) -> Result<(), StateError> {
        Ok(())
    }
}

impl<T: Table + Sized + 'static> ErasedTable for T {
    fn from_config(
        config: TableConfig,
        task_info: Arc<TaskInfo>,
        storage_provider: StorageProviderRef,
        checkpoint_message: Option<TableCheckpointMetadata>,
    ) -> Result<Self, StateError>
    where
        Self: Sized,
    {
        let state_version = config.state_version;
        let config = Self::checked_proto_decode(config.table_type(), config.config)?;
        let checkpoint_message = checkpoint_message
            .map(|metadata| Self::checked_proto_decode(metadata.table_type(), metadata.data))
            .transpose()?;
        debug!(
            "restoring from checkpoint message:\n{:#?}",
            checkpoint_message
        );
        T::from_config(
            config,
            task_info,
            storage_provider,
            checkpoint_message,
            state_version,
        )
    }

    fn epoch_checkpointer(
        &self,
        epoch: u32,
        previous_metadata: Option<TableSubtaskCheckpointMetadata>,
    ) -> Result<Box<dyn ErasedCheckpointer>, StateError> {
        let previous_metadata = previous_metadata
            .map(|metadata| Self::checked_proto_decode(metadata.table_type(), metadata.data))
            .transpose()?;
        let checkpointer = self.epoch_checkpointer(epoch, previous_metadata)?;
        Ok(Box::new(checkpointer) as Box<dyn ErasedCheckpointer>)
    }

    fn merge_checkpoint_metadata(
        config: TableConfig,
        subtask_metadata: HashMap<u32, TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<TableCheckpointMetadata>, StateError>
    where
        Self: Sized,
    {
        let config = Self::checked_proto_decode(config.table_type(), config.config)?;
        let subtask_metadata = subtask_metadata
            .into_iter()
            .map(|(key, value)| {
                let value = Self::checked_proto_decode(value.table_type(), value.data)?;
                Ok((key, value))
            })
            .collect::<Result<HashMap<_, _>, StateError>>()?;
        let result = T::merge_checkpoint_metadata(config, subtask_metadata)?;
        Ok(result.map(|table| TableCheckpointMetadata {
            table_type: T::table_type().into(),
            data: table.encode_to_vec(),
        }))
    }

    fn subtask_metadata_from_table(
        &self,
        table_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableSubtaskCheckpointMetadata>, StateError> {
        let table_metadata =
            Self::checked_proto_decode(table_metadata.table_type(), table_metadata.data)?;
        let subtask_metadata = self.subtask_metadata_from_table(table_metadata)?;
        Ok(
            subtask_metadata.map(|metadata| TableSubtaskCheckpointMetadata {
                subtask_index: self.task_info().task_index,
                table_type: T::table_type().into(),
                data: metadata.encode_to_vec(),
            }),
        )
    }

    fn apply_compacted_checkpoint(
        &self,
        epoch: u32,
        compacted_checkpoint: TableSubtaskCheckpointMetadata,
        subtask_metadata: TableSubtaskCheckpointMetadata,
    ) -> Result<TableSubtaskCheckpointMetadata, StateError> {
        let compacted_checkpoint = Self::checked_proto_decode(
            compacted_checkpoint.table_type(),
            compacted_checkpoint.data,
        )?;
        let subtask_metadata =
            Self::checked_proto_decode(subtask_metadata.table_type(), subtask_metadata.data)?;
        let result =
            self.apply_compacted_checkpoint(epoch, compacted_checkpoint, subtask_metadata)?;
        Ok(TableSubtaskCheckpointMetadata {
            subtask_index: self.task_info().task_index,
            table_type: T::table_type().into(),
            data: result.encode_to_vec(),
        })
    }

    fn table_type() -> TableEnum
    where
        Self: Sized,
    {
        T::table_type()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn files_to_keep(table: ValidatedTable<'_>) -> Result<HashSet<String>, StateError>
    where
        Self: Sized,
    {
        T::files_to_keep(
            Self::checked_proto_decode(T::table_type(), table.config().config.clone())?,
            Self::checked_proto_decode(T::table_type(), table.checkpoint().data.clone())?,
        )
    }
    fn committing_data(
        config: TableConfig,
        table_metadata: &TableCheckpointMetadata,
    ) -> Option<HashMap<u32, Vec<u8>>>
    where
        Self: Sized,
    {
        let config = Self::checked_proto_decode(config.table_type(), config.config).ok()?;
        let table_metadata =
            Self::checked_proto_decode(table_metadata.table_type(), table_metadata.data.clone())
                .ok()?;
        T::committing_data(config, table_metadata)
    }

    async fn compact_data(
        config: TableConfig,
        compaction_config: &CompactionConfig,
        operator_metadata: &OperatorMetadata,
        current_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableCheckpointMetadata>, StateError> {
        let config = Self::checked_proto_decode(config.table_type(), config.config)?;
        let result = T::compact_data(
            config,
            compaction_config,
            operator_metadata,
            Self::checked_proto_decode(current_metadata.table_type(), current_metadata.data)?,
        )
        .await?;
        Ok(result.map(|result| TableCheckpointMetadata {
            table_type: T::table_type().into(),
            data: result.encode_to_vec(),
        }))
    }
}

#[async_trait::async_trait]
pub trait TableEpochCheckpointer: Send {
    type SubTableCheckpointMessage: prost::Message;
    async fn insert_data(&mut self, data: TableData) -> Result<(), StateError>;
    // returning Ok(None) means there is no state to restore.
    async fn finish(
        self,
        checkpoint: &CheckpointMessage,
    ) -> Result<Option<(Self::SubTableCheckpointMessage, usize)>, StateError>;

    fn table_type() -> TableEnum;

    fn subtask_index(&self) -> u32;
}

#[async_trait::async_trait]
pub trait ErasedCheckpointer: Send {
    async fn insert_data(&mut self, data: TableData) -> Result<(), StateError>;
    async fn finish(
        mut self: Box<Self>,
        checkpoint: &CheckpointMessage,
    ) -> Result<Option<(TableSubtaskCheckpointMetadata, usize)>, StateError>;
}

#[async_trait::async_trait]
impl<T: TableEpochCheckpointer + Sized> ErasedCheckpointer for T {
    async fn insert_data(&mut self, data: TableData) -> Result<(), StateError> {
        self.insert_data(data).await
    }

    async fn finish(
        mut self: Box<Self>,
        checkpoint: &CheckpointMessage,
    ) -> Result<Option<(TableSubtaskCheckpointMetadata, usize)>, StateError> {
        let subtask_index = self.subtask_index();
        let subtask = (*self).finish(checkpoint).await?;
        Ok(subtask.map(|(metadata, size)| {
            (
                TableSubtaskCheckpointMetadata {
                    subtask_index,
                    table_type: T::table_type().into(),
                    data: metadata.encode_to_vec(),
                },
                size,
            )
        }))
    }
}
