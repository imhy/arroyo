use arrow_array::RecordBatch;
use arroyo_rpc::errors::{StateError, StorageError};
use arroyo_rpc::grpc::rpc::{
    CheckpointMetadata, ExpiringKeyedTimeTableConfig, GlobalKeyedTableConfig,
    OperatorCheckpointMetadata, TableCheckpointMetadata, TableConfig, TableEnum,
};
use arroyo_types::single_item_hash_map;
use async_trait::async_trait;
use bincode::config::Configuration;
use bincode::{Decode, Encode};

use crate::validated::{CheckpointIdentity, CheckpointMetadataWrite};
use arroyo_rpc::config::config;
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_rpc::state_backend::validated::Validated;
use arroyo_storage::StorageProvider;
pub use arroyo_storage::StorageProviderFor;
use prost::Message;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;

mod metrics;
pub mod ownership;
pub mod parquet;
pub mod provider;
pub(crate) mod schemas;
pub mod tables;
pub mod validated;

pub const BINCODE_CONFIG: Configuration = bincode::config::standard();
pub const FULL_KEY_RANGE: RangeInclusive<u64> = 0..=u64::MAX;

#[derive(Debug)]
pub enum StateMessage {
    Checkpoint(CheckpointMessage),
    /// A checkpoint one of the subtask's tables refused at its barrier hook
    /// ([`tables::ErasedTable::on_checkpoint_barrier`]), sent in place of the
    /// [`Self::Checkpoint`] it would have been. The flusher fails the task with the table's
    /// error when it reaches it (plan M11.T10b.01).
    BarrierRefused(BarrierRefusal),
    Compaction(HashMap<String, TableCheckpointMetadata>),
    TableData {
        table: String,
        data: TableData,
    },
}

/// The checkpoint a barrier asks a subtask's tables for.
///
/// [`TableManager::checkpoint`](tables::table_manager::TableManager::checkpoint) builds it
/// from the barrier, hands it to every table's barrier hook, and then enqueues it for the
/// flusher, which hands the same message to every table's
/// [`ErasedCheckpointer::finish`](tables::ErasedCheckpointer::finish) for the epoch. Its
/// fields are private and nothing outside this crate can build one; tables read it through
/// the accessors below (plan M11.T10b.01, design M11.D42 as amended by ruling M11.T10R2).
#[derive(Debug)]
pub struct CheckpointMessage {
    epoch: u32,
    /// The barrier's timestamp. It has no accessor: `RunningJobModel::start_checkpoint`, which
    /// both controllers use, sends it in microseconds and the worker decodes it as
    /// milliseconds, so its value is not one a table may rely on.
    time: SystemTime,
    watermark: Option<SystemTime>,
    then_stop: bool,
    min_epoch: u32,
}

impl CheckpointMessage {
    /// The epoch being checkpointed: the barrier's.
    pub fn epoch(&self) -> u32 {
        self.epoch
    }

    /// The watermark the subtask held when the barrier reached it, if it held one.
    pub fn watermark(&self) -> Option<SystemTime> {
        self.watermark
    }

    /// Whether the subtask stops after this checkpoint.
    ///
    /// When it does, `TableManager::checkpoint` does not return until the flusher has stopped:
    /// after reporting this checkpoint, or on failing it.
    pub fn then_stop(&self) -> bool {
        self.then_stop
    }

    /// The controller's retention floor carried on this barrier (`CheckpointBarrier::min_epoch`):
    /// the oldest epoch whose checkpoint the controller still retains.
    ///
    /// Not the epoch the subtask restored from, which a table is told once, through
    /// [`ErasedTable::restored`](tables::ErasedTable::restored), and which `TableManager`
    /// keeps in a private field that is also called `min_epoch`.
    pub fn min_epoch(&self) -> u32 {
        self.min_epoch
    }
}

/// A table's refusal of a checkpoint at its barrier hook, travelling the state channel to the
/// flusher at the position the checkpoint message would have taken.
///
/// Only `TableManager::checkpoint` builds one; its fields are private, so a view holding the
/// state channel's sender cannot forge a refusal.
#[derive(Debug)]
pub struct BarrierRefusal {
    table: String,
    epoch: u32,
    error: StateError,
}

#[derive(Debug)]
pub enum TableData {
    RecordBatch(RecordBatch),
    CommitData { data: Vec<u8> },
    KeyedData { key: Vec<u8>, value: Vec<u8> },
}

pub type StateBackend = parquet::ParquetBackend;

pub fn global_table_config(
    name: impl Into<String>,
    description: impl Into<String>,
) -> HashMap<String, TableConfig> {
    global_table_config_with_version(name, description, 0)
}

pub fn global_table_config_with_version(
    name: impl Into<String>,
    description: impl Into<String>,
    state_version: u32,
) -> HashMap<String, TableConfig> {
    let name = name.into();
    single_item_hash_map(
        name.clone(),
        TableConfig {
            table_type: TableEnum::GlobalKeyValue.into(),
            config: GlobalKeyedTableConfig {
                table_name: name,
                description: description.into(),
                uses_two_phase_commit: false,
            }
            .encode_to_vec(),
            state_version,
            // Left empty on purpose: the job's selector is stamped into every table
            // config centrally, when the operator's context is built.
            state_backend: String::new(),
        },
    )
}

pub fn timestamp_table_config(
    name: impl Into<String>,
    description: impl Into<String>,
    retention: Duration,
    generational: bool,
    schema: ArroyoSchema,
) -> TableConfig {
    TableConfig {
        table_type: TableEnum::ExpiringKeyedTimeTable.into(),
        config: ExpiringKeyedTimeTableConfig {
            table_name: name.into(),
            description: description.into(),
            retention_micros: retention.as_micros() as u64,
            generational,
            schema: Some(schema.into()),
        }
        .encode_to_vec(),
        state_version: 0,
        // Left empty on purpose: the job's selector is stamped into every table config
        // centrally, when the operator's context is built.
        state_backend: String::new(),
    }
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub struct DeleteTimeKeyOperation {
    pub timestamp: SystemTime,
    pub key: Vec<u8>,
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub struct DeleteKeyOperation {
    pub key: Vec<u8>,
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub struct DeleteValueOperation {
    pub key: Vec<u8>,
    pub timestamp: SystemTime,
    pub value: Vec<u8>,
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub struct DeleteTimeRangeOperation {
    pub key: Vec<u8>,
    pub start: SystemTime,
    pub end: SystemTime,
}

#[derive(Debug, Encode, Decode, PartialEq, Eq, Clone)]
pub enum DataOperation {
    Insert,
    DeleteTimeKey(DeleteTimeKeyOperation), // delete single key of a TimeKeyMap
    DeleteKey(DeleteKeyOperation),         // delete all data for a key in a KeyTimeMultiMap
    DeleteValue(DeleteValueOperation),     // delete single value of a KeyTimeMultiMap
    DeleteTimeRange(DeleteTimeRangeOperation), // delete all values for key in range (only for KeyTimeMultiMap)
}
#[async_trait]
pub trait BackingStore {
    /// loads the checkpoint metadata for a given job id and epoch
    async fn load_checkpoint_metadata(
        role: &StorageProviderFor,
        job_id: &str,
        epoch: u32,
    ) -> Result<CheckpointMetadata, StateError>;

    /// loads the operator checkpoint metadata for a given job id, operator id, and epoch
    async fn load_operator_metadata(
        role: &StorageProviderFor,
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>, StateError>;

    /// returns the name of the BackingStore implementation
    fn name() -> &'static str;

    /// writes the operator checkpoint metadata to the backing store
    async fn write_operator_checkpoint_metadata(
        role: &StorageProviderFor,
        metadata: OperatorCheckpointMetadata,
    ) -> Result<(), StateError>;

    /// writes the checkpoint metadata to the backing store
    ///
    /// Takes a [`Validated<CheckpointMetadataWrite>`] rather than the metadata (design item
    /// M11.D39c). This write is what makes a checkpoint the one a restart reads, so it may
    /// not name an operator that no whole-checkpoint check covered — which is exactly what a
    /// `ready` checkpoint used to do, reaching this call with no operator preflighted at
    /// all. The token is the only spelling of the argument, so a new caller cannot repeat
    /// that by forgetting to check first.
    async fn write_checkpoint_metadata(
        role: &StorageProviderFor,
        metadata: Validated<CheckpointMetadataWrite>,
    ) -> Result<(), StateError>;

    /// cleans up a checkpoint by deleting data that is no longer needed
    ///
    /// `job` is the state backend the job selected. The epochs being cleaned describe
    /// state that some run of this job wrote, and every one of them is checked against
    /// `job` before any file is deleted, so a job can never delete another backend's
    /// files.
    ///
    /// `checkpoint` is the checkpoint the caller asked for — its own job id, and the epoch of
    /// the top-level metadata object whose `min_epoch` this advances. It is passed separately
    /// from `metadata` on purpose: every path a cleanup deletes from is derived from the
    /// collected objects rather than from the arguments, so the objects have to be shown to be
    /// the ones the caller meant rather than trusted to be because of where the caller got
    /// them (design item M11.D39c; PR #160 review round 6).
    async fn cleanup_checkpoint(
        role: &StorageProviderFor,
        job: StateBackendSelector,
        checkpoint: CheckpointIdentity,
        metadata: CheckpointMetadata,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> Result<(), StateError>;
}

pub fn hash_key<K: Hash>(key: &K) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Per-URL cache of [`StorageProvider`] instances.
fn storage_provider_cache() -> &'static Mutex<HashMap<String, Arc<StorageProvider>>> {
    static CACHE: std::sync::OnceLock<Mutex<HashMap<String, Arc<StorageProvider>>>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Returns a cached [`StorageProvider`] for the given role.
///
/// Workers use [`StorageProviderFor::Worker`], which reads `config().checkpoint_url`
/// (set per-pipeline via the `ARROYO__CHECKPOINT_URL` env var at worker startup).
///
/// Controllers manage many pipelines that may have different storage URLs and
/// pass [`StorageProviderFor::Controller`] with the pipeline's `state_url`.
/// When `state_url` is `None`, falls back to `config().checkpoint_url`.
pub async fn get_storage_provider(
    role: &StorageProviderFor,
) -> Result<Arc<StorageProvider>, StorageError> {
    let storage_url = match role {
        StorageProviderFor::Controller {
            storage_url: Some(url),
        } => url,
        StorageProviderFor::Worker | StorageProviderFor::Controller { storage_url: None } => {
            &config().checkpoint_url
        }
    };
    let mut cache = storage_provider_cache().lock().await;

    if let Some(storage_provider) = cache.get(storage_url) {
        Ok(storage_provider.clone())
    } else {
        let storage_provider = Arc::new(StorageProvider::for_url(storage_url).await?);
        cache.insert(storage_url.clone(), storage_provider.clone());
        Ok(storage_provider)
    }
}
