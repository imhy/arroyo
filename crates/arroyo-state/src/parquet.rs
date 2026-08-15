use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::tables::global_keyed_map::GlobalKeyedTable;
use crate::tables::{CompactionConfig, ErasedTable};
use crate::{BackingStore, StorageProviderFor, get_storage_provider};
use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{
    CheckpointMetadata, OperatorCheckpointMetadata, TableCheckpointMetadata,
};
use arroyo_types::CheckpointFilePathLayout;
use futures::StreamExt;
use futures::stream::FuturesUnordered;

use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::state_backend::{StateBackendSelector, validate_restored_operator_metadata};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, info, warn};

pub const FULL_KEY_RANGE: RangeInclusive<u64> = 0..=u64::MAX;
pub const GENERATIONS_TO_COMPACT: u32 = 1; // only compact generation 0 files

pub struct ParquetBackend;

fn base_path(job_id: &str, epoch: u32) -> String {
    format!("{job_id}/checkpoints/checkpoint-{epoch:0>7}")
}

fn metadata_path(path: &str) -> String {
    format!("{path}/metadata")
}

fn operator_path(job_id: &str, epoch: u32, operator: &str) -> String {
    format!("{}/operator-{}", base_path(job_id, epoch), operator)
}

#[async_trait::async_trait]
impl BackingStore for ParquetBackend {
    fn name() -> &'static str {
        "parquet"
    }

    async fn load_checkpoint_metadata(
        role: &StorageProviderFor,
        job_id: &str,
        epoch: u32,
    ) -> Result<CheckpointMetadata, StateError> {
        let storage_client = get_storage_provider(role).await?;
        let data = storage_client
            .get(metadata_path(&base_path(job_id, epoch)).as_str())
            .await?;
        let metadata = CheckpointMetadata::decode(&data[..])?;
        Ok(metadata)
    }

    async fn load_operator_metadata(
        role: &StorageProviderFor,
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>, StateError> {
        let storage_client = get_storage_provider(role).await?;
        storage_client
            .get_if_present(metadata_path(&operator_path(job_id, epoch, operator_id)).as_str())
            .await?
            .map(|data| Ok(OperatorCheckpointMetadata::decode(&data[..])?))
            .transpose()
    }

    async fn write_operator_checkpoint_metadata(
        role: &StorageProviderFor,
        metadata: OperatorCheckpointMetadata,
    ) -> Result<(), StateError> {
        let storage_client = get_storage_provider(role).await?;
        let operator_metadata =
            metadata
                .operator_metadata
                .as_ref()
                .ok_or_else(|| StateError::Other {
                    table: "".to_string(),
                    error: "missing operator metadata".to_string(),
                })?;
        let path = metadata_path(&operator_path(
            &operator_metadata.job_id,
            operator_metadata.epoch,
            &operator_metadata.operator_id,
        ));
        storage_client
            .put(path.as_str(), metadata.encode_to_vec())
            .await?;
        // TODO: propagate error
        Ok(())
    }

    async fn write_checkpoint_metadata(
        role: &StorageProviderFor,
        metadata: CheckpointMetadata,
    ) -> Result<(), StateError> {
        debug!("writing checkpoint {:?}", metadata);
        let storage_client = get_storage_provider(role).await?;
        let path = metadata_path(&base_path(&metadata.job_id, metadata.epoch));
        storage_client
            .put(path.as_str(), metadata.encode_to_vec())
            .await?;
        Ok(())
    }

    async fn cleanup_checkpoint(
        role: &StorageProviderFor,
        job: StateBackendSelector,
        mut metadata: CheckpointMetadata,
        old_min_epoch: u32,
        min_epoch: u32,
    ) -> Result<(), StateError> {
        info!(
            message = "Cleaning checkpoint",
            min_epoch,
            job_id = metadata.job_id
        );

        // Classify first, delete second. Every operator and every epoch this cleanup touches
        // is loaded and checked against `job` before the first delete, so a mismatch found in
        // the last operator's oldest epoch cannot follow files the first operator has already
        // removed. Each metadata object is still read exactly once — planning keeps the file
        // set it derived — and operators are still planned concurrently.
        let mut planning: FuturesUnordered<_> = metadata
            .operator_ids
            .iter()
            .map(|operator_id| {
                Self::plan_operator_cleanup(
                    role,
                    job,
                    metadata.job_id.clone(),
                    operator_id.clone(),
                    old_min_epoch,
                    min_epoch,
                )
            })
            .collect();

        let mut plans = Vec::with_capacity(metadata.operator_ids.len());
        while let Some(plan) = planning.next().await {
            plans.push(plan?);
        }
        drop(planning);

        let storage_client = get_storage_provider(role).await?;

        let mut deleting: FuturesUnordered<_> = plans
            .into_iter()
            .map(|plan| {
                let storage_client = Arc::clone(&storage_client);
                let job_id = metadata.job_id.clone();
                async move {
                    for file in plan.files_to_delete {
                        storage_client.delete_if_present(file).await?;
                    }

                    for epoch_to_remove in old_min_epoch..min_epoch {
                        let path = metadata_path(&operator_path(
                            &job_id,
                            epoch_to_remove,
                            &plan.operator_id,
                        ));
                        storage_client.delete_if_present(path).await?;
                    }

                    Ok::<_, StateError>(plan.operator_id)
                }
            })
            .collect();

        // wait for all of the futures to complete
        while let Some(result) = deleting.next().await {
            let operator_id = result?;
            debug!(
                message = "Finished cleaning operator",
                job_id = metadata.job_id,
                operator_id,
                min_epoch
            );
        }
        drop(deleting);

        for epoch_to_remove in old_min_epoch..min_epoch {
            storage_client
                .delete_if_present(metadata_path(&base_path(&metadata.job_id, epoch_to_remove)))
                .await?;
        }
        metadata.min_epoch = min_epoch;
        Self::write_checkpoint_metadata(role, metadata).await?;
        Ok(())
    }
}

/// One operator's fully classified cleanup.
///
/// Produced by [`ParquetBackend::plan_operator_cleanup`] for every operator of a checkpoint
/// before any of them is executed, so the decision to delete is made with the whole checkpoint
/// in hand. It carries the derived file set rather than the metadata it came from: the metadata
/// is read once, during planning, and dropped there.
struct OperatorCleanupPlan {
    operator_id: String,
    files_to_delete: HashSet<String>,
}

impl ParquetBackend {
    /// Called after a checkpoint is committed
    ///
    /// `job` is the state backend the job selected. The checkpoint about to be rewritten
    /// is checked against it before any file is read or written: compacting a checkpoint
    /// another backend wrote would rewrite that backend's state under this one's rules.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::StateBackendError`] if the checkpoint's table configs
    /// disagree with `job`, alongside the storage and table failures compaction can
    /// otherwise produce.
    pub async fn compact_operator(
        role: &StorageProviderFor,
        job: StateBackendSelector,
        job_id: Arc<String>,
        operator_id: &str,
        epoch: u32,
    ) -> Result<HashMap<String, TableCheckpointMetadata>, StateError> {
        let operator_checkpoint_metadata =
            Self::load_operator_metadata(role, &job_id, operator_id, epoch)
                .await?
                .expect("expect operator metadata to still be present");
        validate_restored_operator_metadata(job, &operator_checkpoint_metadata)?;

        Self::compact_loaded_operator(role, operator_checkpoint_metadata).await
    }

    /// Loads and validates every operator's metadata for one checkpoint, without touching
    /// anything.
    ///
    /// This is the preflight for restoring: a checkpoint is restored by rewriting its
    /// top-level metadata and then starting workers that build one operator's tables at a
    /// time, and each of those steps is only safe if the *whole* checkpoint belongs to
    /// this job's backend. Validating each operator as it is reached would let the earlier
    /// operators be rebuilt, and the metadata be rewritten, before a later operator's
    /// disagreement is seen.
    ///
    /// Operators are loaded concurrently and returned in `operator_ids` order, so the
    /// caller reuses these objects instead of reading them again — restoring a checkpoint
    /// with this preflight costs the same reads it did without one, as long as the caller
    /// consumes what it is given.
    ///
    /// `restoring` is the set of operators the job's workers will actually construct,
    /// derived from the *current* program rather than from the checkpoint. The two are
    /// checked against each other before anything is read, because the checkpoint's own
    /// list is not the set that matters: a worker builds every operator of its program and
    /// each one loads its own metadata object, so an operator the checkpoint omits is
    /// never preflighted and yet is still required — and an operator the checkpoint lists
    /// but the program does not contain means the checkpoint belongs to a different
    /// program.
    ///
    /// Every listed operator must also *have* a metadata object, and that object must
    /// carry an operator header naming it. Absence used to be reported as `None` and left
    /// to the caller, on the grounds that absence states no selector; but the workers this
    /// preflight runs on behalf of reject it (`TableManager::load` requires the object
    /// whenever it restores at all, and reads the restored watermark out of its header
    /// without checking that there is one), so tolerating either only moved the failure
    /// past the point where the checkpoint's metadata has already been rewritten.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::StateBackendError`] if any operator's table configs disagree
    /// with `job`, name an unknown backend, or disagree with each other;
    /// [`StateError::IncompleteCheckpoint`] if the checkpoint's operator set is not
    /// exactly `restoring`, or a listed operator has no metadata object, or its object
    /// carries no operator header or one naming a different operator; alongside the
    /// storage failures reading the metadata can produce. Nothing is written or deleted in
    /// any case.
    pub async fn load_checkpoint_operators(
        role: &StorageProviderFor,
        job: StateBackendSelector,
        restoring: &HashSet<&str>,
        metadata: &CheckpointMetadata,
    ) -> Result<Vec<(String, OperatorCheckpointMetadata)>, StateError> {
        let incomplete = |detail: String| StateError::IncompleteCheckpoint {
            epoch: metadata.epoch,
            detail,
        };

        // Set arithmetic first: it needs no reads, and a checkpoint that cannot cover this
        // job's operators must be refused before even the preflight's own reads happen.
        let listed: HashSet<&str> = metadata.operator_ids.iter().map(String::as_str).collect();

        let mut missing: Vec<&str> = restoring.difference(&listed).copied().collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            return Err(incomplete(format!(
                "the job's program contains operator(s) {} that the checkpoint does not \
                 list, and every operator a worker builds loads its own metadata",
                missing.join(", ")
            )));
        }

        let mut extra: Vec<&str> = listed.difference(restoring).copied().collect();
        if !extra.is_empty() {
            extra.sort_unstable();
            return Err(incomplete(format!(
                "the checkpoint lists operator(s) {} that the job's program does not \
                 contain",
                extra.join(", ")
            )));
        }

        let mut loading: FuturesUnordered<_> = metadata
            .operator_ids
            .iter()
            .map(|operator_id| async move {
                let loaded = Self::load_operator_metadata(
                    role,
                    &metadata.job_id,
                    operator_id,
                    metadata.epoch,
                )
                .await?
                .ok_or_else(|| {
                    incomplete(format!(
                        "operator {operator_id} has no checkpoint metadata object, but the \
                         worker that builds it requires one"
                    ))
                })?;
                // ...and that object must carry an operator header naming this operator.
                // `TableManager::load` reads the restored watermark straight out of it
                // and panics if it is absent, so tolerating a headerless object here only
                // moves the failure into a worker, past the metadata rewrite.
                match loaded.operator_metadata.as_ref() {
                    Some(header) if header.operator_id == *operator_id => {}
                    Some(header) => {
                        return Err(incomplete(format!(
                            "the checkpoint metadata object for operator {operator_id} is \
                             headed \"{}\" instead",
                            header.operator_id
                        )));
                    }
                    None => {
                        return Err(incomplete(format!(
                            "the checkpoint metadata object for operator {operator_id} has \
                             no operator header, which the worker that builds it requires"
                        )));
                    }
                }
                validate_restored_operator_metadata(job, &loaded)?;
                Ok::<_, StateError>((operator_id.clone(), loaded))
            })
            .collect();

        let mut validated = HashMap::with_capacity(metadata.operator_ids.len());
        while let Some(loaded) = loading.next().await {
            let (operator_id, loaded) = loaded?;
            validated.insert(operator_id, loaded);
        }
        drop(loading);

        Ok(metadata
            .operator_ids
            .iter()
            .filter_map(|operator_id| {
                // `remove` rather than `get`: a checkpoint that lists the same operator
                // twice yields it once, which is also what the loading pass produced.
                validated
                    .remove(operator_id)
                    .map(|loaded| (operator_id.clone(), loaded))
            })
            .collect())
    }

    /// Compacts a whole checkpoint, one operator at a time, after the whole checkpoint has
    /// been validated
    ///
    /// `job` is the state backend the job selected. Every operator's checkpoint metadata is
    /// loaded and checked against it — concurrently, in one pass — before the first operator
    /// is compacted, so a mismatch in the last operator cannot leave earlier operators
    /// already rewritten. The loaded metadata is then compacted directly, so this reads each
    /// operator's metadata exactly once; the cost of the preflight is holding one checkpoint's
    /// operator metadata in memory rather than one operator's.
    ///
    /// Results are returned in `operator_ids` order for the caller to publish, which is what
    /// keeps a worker from being told about compacted data for a checkpoint that is about to
    /// be rejected.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::StateBackendError`] if any operator's table configs disagree with
    /// `job` — in which case nothing has been compacted — alongside the storage and table
    /// failures compaction can otherwise produce.
    pub async fn compact_checkpoint(
        role: &StorageProviderFor,
        job: StateBackendSelector,
        job_id: Arc<String>,
        operator_ids: Vec<String>,
        epoch: u32,
    ) -> Result<Vec<(String, HashMap<String, TableCheckpointMetadata>)>, StateError> {
        let mut loading: FuturesUnordered<_> = operator_ids
            .iter()
            .map(|operator_id| {
                let job_id = Arc::clone(&job_id);
                async move {
                    let metadata = Self::load_operator_metadata(role, &job_id, operator_id, epoch)
                        .await?
                        .expect("expect operator metadata to still be present");
                    validate_restored_operator_metadata(job, &metadata)?;
                    Ok::<_, StateError>((operator_id.clone(), metadata))
                }
            })
            .collect();

        let mut validated = HashMap::with_capacity(operator_ids.len());
        while let Some(loaded) = loading.next().await {
            let (operator_id, metadata) = loaded?;
            validated.insert(operator_id, metadata);
        }
        drop(loading);

        let mut compacted = Vec::with_capacity(operator_ids.len());
        for operator_id in operator_ids {
            let Some(metadata) = validated.remove(&operator_id) else {
                // Only reachable if the caller passed the same operator twice.
                continue;
            };
            compacted.push((
                operator_id,
                Self::compact_loaded_operator(role, metadata).await?,
            ));
        }

        Ok(compacted)
    }

    /// Compacts one operator's already-loaded, already-validated checkpoint metadata.
    ///
    /// Takes the metadata by value rather than re-reading it, so the whole-checkpoint
    /// preflight in [`ParquetBackend::compact_checkpoint`] costs no extra reads.
    async fn compact_loaded_operator(
        role: &StorageProviderFor,
        operator_checkpoint_metadata: OperatorCheckpointMetadata,
    ) -> Result<HashMap<String, TableCheckpointMetadata>, StateError> {
        let min_files_to_compact = config().pipeline.compaction.checkpoints_to_compact as usize;

        let storage_provider = get_storage_provider(role).await?;
        let compaction_config = CompactionConfig {
            compact_generations: vec![0].into_iter().collect(),
            min_compaction_epochs: min_files_to_compact,
            storage_provider: Arc::clone(&storage_provider),
            file_path_layout: CheckpointFilePathLayout::Legacy,
        };
        let operator_metadata = operator_checkpoint_metadata.operator_metadata.unwrap();

        let mut result = HashMap::new();

        for (table, table_metadata) in operator_checkpoint_metadata.table_checkpoint_metadata {
            let table_config = operator_checkpoint_metadata
                .table_configs
                .get(&table)
                .unwrap()
                .clone();
            if let Some(compacted_metadata) = match table_metadata.table_type() {
                rpc::TableEnum::MissingTableType => {
                    return Err(StateError::Other {
                        table: table.clone(),
                        error: "should have table type".to_string(),
                    });
                }
                rpc::TableEnum::GlobalKeyValue => {
                    GlobalKeyedTable::compact_data(
                        table_config,
                        &compaction_config,
                        &operator_metadata,
                        table_metadata,
                    )
                    .await?
                }
                rpc::TableEnum::ExpiringKeyedTimeTable => {
                    ExpiringTimeKeyTable::compact_data(
                        table_config,
                        &compaction_config,
                        &operator_metadata,
                        table_metadata,
                    )
                    .await?
                }
            } {
                result.insert(table, compacted_metadata);
            }
        }
        Ok(result)
    }

    /// Classify the files no longer referenced by the new min epoch, without deleting them
    ///
    /// `job` is the state backend the job selected. Both the epoch being kept and every
    /// epoch being dropped are checked against it, because the dropped epochs may predate a
    /// restart: a job that was previously run on another backend must not have that
    /// backend's files deleted by this one's file layout.
    ///
    /// Nothing is deleted here. [`ParquetBackend::cleanup_checkpoint`] plans every operator
    /// first and only then executes the plans, so a mismatch in *any* operator or epoch is
    /// found while the checkpoint is still whole. Each epoch's metadata is read once and the
    /// derived paths are kept, so classifying costs no extra reads over deleting inline.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::StateBackendError`] if any of those epochs' table configs
    /// disagree with `job`, alongside the storage failures reading metadata can produce.
    async fn plan_operator_cleanup(
        role: &StorageProviderFor,
        job: StateBackendSelector,
        job_id: String,
        operator_id: String,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> Result<OperatorCleanupPlan, StateError> {
        let operator_metadata =
            Self::load_operator_metadata(role, &job_id, &operator_id, new_min_epoch)
                .await?
                .expect("expect new_min_epoch metadata to still be present");
        validate_restored_operator_metadata(job, &operator_metadata)?;

        let paths_to_keep: HashSet<String> = operator_metadata
            .table_checkpoint_metadata
            .iter()
            .flat_map(|(table_name, metadata)| {
                let table_config = operator_metadata
                    .table_configs
                    .get(table_name)
                    .unwrap()
                    .clone();

                match table_config.table_type() {
                    rpc::TableEnum::MissingTableType => todo!("should handle error"),
                    rpc::TableEnum::GlobalKeyValue => {
                        GlobalKeyedTable::files_to_keep(table_config, metadata.clone()).unwrap()
                    }
                    rpc::TableEnum::ExpiringKeyedTimeTable => {
                        ExpiringTimeKeyTable::files_to_keep(table_config, metadata.clone()).unwrap()
                    }
                }
            })
            .collect();

        let mut files_to_delete = HashSet::new();

        for epoch_to_remove in old_min_epoch..new_min_epoch {
            let Some(operator_metadata) =
                Self::load_operator_metadata(role, &job_id, &operator_id, epoch_to_remove).await?
            else {
                continue;
            };
            validate_restored_operator_metadata(job, &operator_metadata)?;

            // delete any files that are not in the new min epoch
            let mut files = HashSet::new();
            for (table_name, metadata) in operator_metadata.table_checkpoint_metadata.iter() {
                let table_config = operator_metadata
                    .table_configs
                    .get(table_name)
                    .ok_or_else(|| StateError::Other {
                        table: table_name.clone(),
                        error: format!("missing table config for operator {operator_id}, table {table_name}, metadata is {metadata:?}, operator_metadata is {operator_metadata:?}")
                    })?
                    .clone();

                files.extend(match table_config.table_type() {
                    rpc::TableEnum::MissingTableType => {
                        warn!("found table without table type: {:?}", table_name);
                        HashSet::new()
                    }
                    rpc::TableEnum::GlobalKeyValue => {
                        GlobalKeyedTable::files_to_keep(table_config, metadata.clone())?
                    }
                    rpc::TableEnum::ExpiringKeyedTimeTable => {
                        ExpiringTimeKeyTable::files_to_keep(table_config, metadata.clone())?
                    }
                });
            }

            for file in files {
                if !paths_to_keep.contains(&file) {
                    files_to_delete.insert(file);
                }
            }
        }

        Ok(OperatorCleanupPlan {
            operator_id,
            files_to_delete,
        })
    }
}

#[derive(Debug)]
pub struct ParquetStats {
    pub max_timestamp: SystemTime,
    pub min_routing_key: u64,
    pub max_routing_key: u64,
}

impl Default for ParquetStats {
    fn default() -> Self {
        Self {
            max_timestamp: SystemTime::UNIX_EPOCH,
            min_routing_key: u64::MAX,
            max_routing_key: u64::MIN,
        }
    }
}

impl ParquetStats {
    pub fn merge(&mut self, other: ParquetStats) {
        self.max_timestamp = self.max_timestamp.max(other.max_timestamp);
        self.min_routing_key = self.min_routing_key.min(other.min_routing_key);
        self.max_routing_key = self.max_routing_key.max(other.max_routing_key);
    }
}

#[cfg(test)]
mod tests {
    use super::{HashSet, ParquetBackend, base_path, metadata_path, operator_path};
    use crate::{BackingStore, StorageProviderFor, get_storage_provider};
    use arroyo_rpc::errors::StateError;
    use arroyo_rpc::grpc::rpc::{
        CheckpointMetadata, GlobalKeyedTableConfig, GlobalKeyedTableTaskCheckpointMetadata,
        OperatorCheckpointMetadata, OperatorMetadata, TableCheckpointMetadata, TableConfig,
        TableEnum,
    };
    use arroyo_rpc::state_backend::{StateBackendError, StateBackendSelector};
    use arroyo_storage::StorageProvider;
    use prost::Message;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// A checkpoint store on the local filesystem, so a cleanup's deletions are observable as
    /// files that are or are not still there.
    ///
    /// `StorageProviderFor::Controller { storage_url: Some(..) }` is the one role that takes an
    /// explicit URL, which keeps each test in its own directory and off the process-wide
    /// `config().checkpoint_url`.
    struct LocalCheckpointStore {
        role: StorageProviderFor,
        directory: String,
    }

    impl LocalCheckpointStore {
        fn new(name: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let unique = format!(
                "{}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                COUNTER.fetch_add(1, Ordering::Relaxed),
            );
            let directory = std::env::temp_dir()
                .join(format!("arroyo-state-{name}-{unique}"))
                .to_string_lossy()
                .into_owned();
            std::fs::create_dir_all(&directory).unwrap();

            Self {
                role: StorageProviderFor::Controller {
                    storage_url: Some(format!("file://{directory}")),
                },
                directory,
            }
        }

        async fn provider(&self) -> Arc<StorageProvider> {
            get_storage_provider(&self.role).await.unwrap()
        }

        async fn put(&self, path: &str, bytes: Vec<u8>) {
            self.provider().await.put(path, bytes).await.unwrap();
        }

        async fn exists(&self, path: &str) -> bool {
            self.provider()
                .await
                .get_if_present(path)
                .await
                .unwrap()
                .is_some()
        }
    }

    impl Drop for LocalCheckpointStore {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }

    const JOB_ID: &str = "job_1";

    fn table_config(state_backend: &str) -> TableConfig {
        TableConfig {
            table_type: TableEnum::GlobalKeyValue as i32,
            config: GlobalKeyedTableConfig {
                table_name: "g".to_string(),
                description: "global".to_string(),
                uses_two_phase_commit: false,
            }
            .encode_to_vec(),
            state_version: 0,
            state_backend: state_backend.to_string(),
        }
    }

    /// One operator's checkpoint metadata for `epoch`, whose single global table references
    /// `files` and whose table config states `state_backend`.
    fn operator_metadata(
        operator_id: &str,
        epoch: u32,
        state_backend: &str,
        files: &[String],
    ) -> OperatorCheckpointMetadata {
        OperatorCheckpointMetadata {
            operator_metadata: Some(OperatorMetadata {
                job_id: JOB_ID.to_string(),
                operator_id: operator_id.to_string(),
                epoch,
                min_watermark: None,
                max_watermark: None,
                parallelism: 1,
            }),
            start_time: 0,
            finish_time: 0,
            table_checkpoint_metadata: HashMap::from([(
                "g".to_string(),
                TableCheckpointMetadata {
                    table_type: TableEnum::GlobalKeyValue as i32,
                    data: GlobalKeyedTableTaskCheckpointMetadata {
                        files: files.to_vec(),
                        commit_data_by_subtask: HashMap::new(),
                    }
                    .encode_to_vec(),
                },
            )]),
            table_configs: HashMap::from([("g".to_string(), table_config(state_backend))]),
        }
    }

    /// Writes one operator's epoch: its data file plus the metadata that references it.
    async fn write_epoch(
        store: &LocalCheckpointStore,
        operator_id: &str,
        epoch: u32,
        state_backend: &str,
    ) -> String {
        let file = format!("{}/{operator_id}-{epoch}.parquet", JOB_ID);
        store.put(&file, b"data".to_vec()).await;
        ParquetBackend::write_operator_checkpoint_metadata(
            &store.role,
            operator_metadata(
                operator_id,
                epoch,
                state_backend,
                std::slice::from_ref(&file),
            ),
        )
        .await
        .unwrap();
        file
    }

    fn checkpoint_metadata(operator_ids: &[&str], epoch: u32) -> CheckpointMetadata {
        CheckpointMetadata {
            job_id: JOB_ID.to_string(),
            epoch,
            min_epoch: 0,
            start_time: 0,
            finish_time: 0,
            operator_ids: operator_ids.iter().map(|id| id.to_string()).collect(),
        }
    }

    /// The operators the job's workers would build — what `LogicalProgram::tasks_per_operator`
    /// supplies in production, stated directly here.
    fn restoring<'a>(operator_ids: &[&'a str]) -> HashSet<&'a str> {
        operator_ids.iter().copied().collect()
    }

    /// The compatibility direction of the cleanup guard: a job whose checkpoints predate the
    /// selector (every table config empty) still collects its own old epochs. The guard must
    /// not strand a legacy deployment's state.
    #[tokio::test]
    async fn cleanup_still_collects_a_legacy_all_parquet_checkpoint() {
        let store = LocalCheckpointStore::new("legacy-cleanup");
        let dropped = write_epoch(&store, "node_1", 0, "").await;
        let kept = write_epoch(&store, "node_1", 1, "").await;

        ParquetBackend::cleanup_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            checkpoint_metadata(&["node_1"], 1),
            0,
            1,
        )
        .await
        .unwrap();

        assert!(!store.exists(&dropped).await, "old epoch's file was kept");
        assert!(
            store.exists(&kept).await,
            "retained epoch's file was deleted"
        );
        assert!(
            !store
                .exists(&metadata_path(&operator_path(JOB_ID, 0, "node_1")))
                .await
        );
    }

    /// Finding 1, the cross-epoch half: `cleanup_operator` used to validate and delete one
    /// epoch at a time, so a mismatch discovered in a later epoch arrived after the earlier
    /// epochs' files were gone.
    ///
    /// Epoch 0 agrees and epoch 1 does not, and both are being dropped. Deleting inline leaves
    /// epoch 0's file deleted; classifying the whole operator first leaves everything.
    #[tokio::test]
    async fn a_mismatching_epoch_stops_cleanup_before_an_earlier_epoch_is_deleted() {
        let store = LocalCheckpointStore::new("epoch-order");
        let agreeing = write_epoch(&store, "node_1", 0, "parquet").await;
        let mismatching = write_epoch(&store, "node_1", 1, "stateengine").await;
        let kept = write_epoch(&store, "node_1", 2, "parquet").await;

        let err = ParquetBackend::cleanup_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            checkpoint_metadata(&["node_1"], 2),
            0,
            2,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, StateError::StateBackendError(_)),
            "expected a typed selector rejection, got {err:?}"
        );

        assert!(
            store.exists(&agreeing).await,
            "the agreeing epoch's file was deleted before the mismatch was found"
        );
        assert!(store.exists(&mismatching).await);
        assert!(store.exists(&kept).await);
        for epoch in 0..2 {
            assert!(
                store
                    .exists(&metadata_path(&operator_path(JOB_ID, epoch, "node_1")))
                    .await,
                "epoch {epoch} metadata was deleted"
            );
        }
    }

    /// Finding 1, the cross-operator half: operator cleanups run concurrently and each used to
    /// delete as soon as it had validated itself, so an operator that failed could not stop one
    /// that had already finished.
    ///
    /// `node_1` agrees throughout. `node_2` agrees at the epoch being retained and disagrees
    /// only at the epoch being dropped, so both operators read the same number of objects
    /// before one deletes and the other refuses — the interleaving that used to lose the race.
    /// Whichever order the two futures complete in, nothing may be deleted and the
    /// checkpoint's `min_epoch` may not be advanced.
    #[tokio::test]
    async fn a_mismatching_operator_stops_cleanup_before_another_operator_is_deleted() {
        let store = LocalCheckpointStore::new("operator-order");
        let agreeing_dropped = write_epoch(&store, "node_1", 0, "parquet").await;
        write_epoch(&store, "node_1", 1, "parquet").await;
        let mismatching_dropped = write_epoch(&store, "node_2", 0, "stateengine").await;
        write_epoch(&store, "node_2", 1, "parquet").await;

        let err = ParquetBackend::cleanup_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            checkpoint_metadata(&["node_1", "node_2"], 1),
            0,
            1,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, StateError::StateBackendError(_)),
            "expected a typed selector rejection, got {err:?}"
        );

        assert!(
            store.exists(&agreeing_dropped).await,
            "the agreeing operator's file was deleted before the other operator was checked"
        );
        assert!(store.exists(&mismatching_dropped).await);
        assert!(
            store
                .exists(&metadata_path(&operator_path(JOB_ID, 0, "node_1")))
                .await,
            "the agreeing operator's dropped metadata was deleted"
        );
        // The checkpoint's own metadata is neither dropped nor rewritten with the new min epoch.
        assert!(!store.exists(&metadata_path(&base_path(JOB_ID, 1))).await);
    }

    /// Finding 4: compaction validated one operator at a time, so a mismatch in a later
    /// operator arrived after earlier operators had been compacted and their workers told.
    ///
    /// `node_1` agrees with the job but its table has no table type, which is a failure
    /// compaction raises only once it is *compacting* that operator — the same trick the merge
    /// guard's test uses to make "the operation started" observable. `node_2` disagrees.
    /// Compacting operator by operator therefore fails inside `node_1`; validating the whole
    /// checkpoint first fails on `node_2` before `node_1` is touched.
    #[tokio::test]
    async fn a_mismatching_operator_stops_compaction_before_an_earlier_one_is_compacted() {
        let store = LocalCheckpointStore::new("compaction-order");

        let mut untyped = operator_metadata("node_1", 1, "parquet", &[]);
        untyped
            .table_checkpoint_metadata
            .get_mut("g")
            .unwrap()
            .table_type = TableEnum::MissingTableType as i32;
        ParquetBackend::write_operator_checkpoint_metadata(&store.role, untyped)
            .await
            .unwrap();
        ParquetBackend::write_operator_checkpoint_metadata(
            &store.role,
            operator_metadata("node_2", 1, "stateengine", &[]),
        )
        .await
        .unwrap();

        let err = ParquetBackend::compact_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            Arc::new(JOB_ID.to_string()),
            vec!["node_1".to_string(), "node_2".to_string()],
            1,
        )
        .await
        .unwrap_err();

        // Compacting node_1 first would have produced `StateError::Other { .. }` from its
        // missing table type instead.
        assert!(
            matches!(
                err,
                StateError::StateBackendError(StateBackendError::CheckpointMismatch { .. })
            ),
            "expected node_2's selector to be rejected before node_1 was compacted, got {err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("node_2"), "{message}");
        assert!(message.contains("stateengine"), "{message}");
    }

    /// The restore preflight refuses the whole checkpoint, and reads nothing more than the
    /// operators it was asked about. `node_1` agrees with the job and `node_2` does not, so
    /// a preflight that stopped at the first disagreement would still have handed `node_1`
    /// back to be restored.
    #[tokio::test]
    async fn loading_a_checkpoints_operators_refuses_one_a_later_operator_disagrees_with() {
        let store = LocalCheckpointStore::new("restore-preflight-mixed");
        write_epoch(&store, "node_1", 1, "parquet").await;
        write_epoch(&store, "node_2", 1, "stateengine").await;

        let err = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &restoring(&["node_1", "node_2"]),
            &checkpoint_metadata(&["node_1", "node_2"], 1),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                StateError::StateBackendError(StateBackendError::CheckpointMismatch { .. })
            ),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("node_2"), "{message}");
        assert!(message.contains("stateengine"), "{message}");
    }

    /// The compatibility direction of the restore preflight, plus the property the caller
    /// depends on to avoid reading twice: a legacy all-parquet checkpoint is accepted, and
    /// every operator comes back in `operator_ids` order with the metadata that was read.
    #[tokio::test]
    async fn loading_a_legacy_all_parquet_checkpoints_operators_returns_them_in_order() {
        let store = LocalCheckpointStore::new("restore-preflight-legacy");
        write_epoch(&store, "node_1", 1, "").await;
        write_epoch(&store, "node_2", 1, "").await;

        let operators = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &restoring(&["node_1", "node_2"]),
            &checkpoint_metadata(&["node_1", "node_2"], 1),
        )
        .await
        .unwrap();

        let ids: Vec<&str> = operators.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, vec!["node_1", "node_2"]);
        assert_eq!(operators[0].1.table_configs["g"].state_backend, "");
        assert_eq!(operators[1].1.table_configs["g"].state_backend, "");
    }

    /// A checkpoint that lists an operator it has no metadata object for is refused,
    /// rather than reported as absent for the caller to interpret.
    ///
    /// This is the case the preflight used to pass through: `needs_commits` was the only
    /// consumer that rejected `None`, so a `ready` checkpoint reached the caller — and the
    /// caller's metadata rewrite — with an operator that no worker could build. Both
    /// operators here do the same work up to the divergence: each is listed, and `node_1`
    /// has a complete object, so `node_2` cannot fail for an unrelated reason first.
    #[tokio::test]
    async fn loading_a_checkpoints_operators_refuses_one_whose_object_is_absent() {
        let store = LocalCheckpointStore::new("restore-preflight-absent");
        write_epoch(&store, "node_1", 1, "").await;

        let err = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &restoring(&["node_1", "node_2"]),
            &checkpoint_metadata(&["node_1", "node_2"], 1),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, StateError::IncompleteCheckpoint { epoch: 1, .. }),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("node_2"), "{message}");
    }

    /// An operator the job's program contains but the checkpoint does not list is refused,
    /// even though nothing about the operators the checkpoint *does* list is wrong.
    ///
    /// The checkpoint's list is not the set that matters: the workers build every operator
    /// of the current program and each loads its own metadata, so an operator missing from
    /// the list is one the preflight would never read and a worker would still require.
    #[tokio::test]
    async fn loading_a_checkpoints_operators_refuses_a_program_operator_it_omits() {
        let store = LocalCheckpointStore::new("restore-preflight-omitted");
        write_epoch(&store, "node_1", 1, "").await;
        write_epoch(&store, "node_2", 1, "stateengine").await;

        let err = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &restoring(&["node_1", "node_2"]),
            // the checkpoint lists only node_1, so node_2's disagreeing metadata is never
            // read by the preflight — but node_2 is built by every worker all the same
            &checkpoint_metadata(&["node_1"], 1),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, StateError::IncompleteCheckpoint { epoch: 1, .. }),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("node_2"), "{message}");
    }

    /// A listed operator whose metadata object carries no operator header — or one naming
    /// a different operator — is refused as well.
    ///
    /// The object existing is not enough: `TableManager::load` reads the restored
    /// watermark straight out of that header and unwraps it, so a headerless object
    /// panics a worker. Round 3 required the object; this requires it to describe the
    /// operator it was loaded for, which is the same rule the leader path's manifest
    /// coverage check applies.
    #[tokio::test]
    async fn loading_a_checkpoints_operators_refuses_one_whose_object_has_no_header() {
        let store = LocalCheckpointStore::new("restore-preflight-headerless");
        write_epoch(&store, "node_1", 1, "").await;

        // Written straight to the path the loader reads: the writer refuses to produce a
        // headerless object, which is exactly why one can only arrive from outside.
        let mut headerless = operator_metadata("node_2", 1, "", &[]);
        headerless.operator_metadata = None;
        store
            .put(
                &metadata_path(&operator_path(JOB_ID, 1, "node_2")),
                headerless.encode_to_vec(),
            )
            .await;

        let err = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &restoring(&["node_1", "node_2"]),
            &checkpoint_metadata(&["node_1", "node_2"], 1),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, StateError::IncompleteCheckpoint { epoch: 1, .. }),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("node_2"), "{message}");
        assert!(message.contains("no operator header"), "{message}");
    }

    /// The other half of "exact coverage": a checkpoint listing an operator the current
    /// program does not contain belongs to a different program and is refused too.
    #[tokio::test]
    async fn loading_a_checkpoints_operators_refuses_one_the_program_does_not_contain() {
        let store = LocalCheckpointStore::new("restore-preflight-extra");
        write_epoch(&store, "node_1", 1, "").await;
        write_epoch(&store, "node_2", 1, "").await;

        let err = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &restoring(&["node_1"]),
            &checkpoint_metadata(&["node_1", "node_2"], 1),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, StateError::IncompleteCheckpoint { epoch: 1, .. }),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("node_2"), "{message}");
    }

    /// The compatibility direction of the compaction guard: a legacy all-parquet checkpoint
    /// still compacts, and its results come back in the order the caller asked for them so the
    /// caller can publish them to workers.
    #[tokio::test]
    async fn compaction_still_runs_for_a_legacy_all_parquet_checkpoint() {
        let store = LocalCheckpointStore::new("legacy-compaction");
        write_epoch(&store, "node_1", 1, "").await;
        write_epoch(&store, "node_2", 1, "").await;

        let compacted = ParquetBackend::compact_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            Arc::new(JOB_ID.to_string()),
            vec!["node_1".to_string(), "node_2".to_string()],
            1,
        )
        .await
        .unwrap();

        let operator_ids: Vec<&str> = compacted.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(operator_ids, vec!["node_1", "node_2"]);
        // Global keyed tables never produce compacted metadata; what matters here is that the
        // whole checkpoint was accepted rather than refused.
        assert!(compacted.iter().all(|(_, tables)| tables.is_empty()));
    }
}
