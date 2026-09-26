use std::any::Any;

use std::{collections::HashMap, sync::Arc, time::SystemTime};

use crate::StorageProviderFor;
use anyhow::{Result, anyhow, bail};
use arroyo_rpc::CompactionResult;
use arroyo_rpc::{
    CheckpointCompleted, ControlResp,
    grpc::rpc::{SubtaskCheckpointMetadata, TableConfig, TableSubtaskCheckpointMetadata},
};
use arroyo_storage::StorageProviderRef;
use arroyo_types::{CheckpointBarrier, Data, Key, TaskInfo, from_micros, to_micros};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};

use super::expiring_time_key_map::{ExpiringTimeKeyTable, KeyTimeView, UncachedKeyValueView};
use super::expiring_time_key_view::ExpiringTimeKeyViewApi;
use super::global_keyed_map::{GlobalKeyedView, restore};
use super::{ErasedCheckpointer, ErasedTable, MigratableState};
use crate::ownership::AcknowledgedFence;
use crate::provider::{TableKind, registry};
use crate::{BackingStore, StateBackend, StateMessage, get_storage_provider};
use crate::{BarrierRefusal, CheckpointMessage, TableData};
use arroyo_rpc::MetadataOrManifest;
use arroyo_rpc::errors::{DataflowResult, StateError};
use arroyo_rpc::grpc::rpc::OperatorCheckpointMetadata;
use arroyo_rpc::state_backend::validate_restored_operator_metadata;
use tracing::{debug, error, info, warn};

#[allow(unused)]
pub struct TableManager {
    epoch: u32,
    /// The epoch this subtask restored from, or 1 on a fresh start; never read.
    ///
    /// Not the controller's retention floor, although it shares the name: that is the
    /// barrier's `min_epoch`, which [`Self::checkpoint`] forwards to every table as
    /// [`CheckpointMessage::min_epoch`]. A table learns the restored epoch through
    /// [`ErasedTable::restored`].
    min_epoch: u32,
    // ordered by table, then epoch.
    tables: HashMap<String, Arc<dyn ErasedTable>>,
    writer: BackendWriter,
    task_info: Arc<TaskInfo>,
    storage: StorageProviderRef,
    /// The worker's acknowledged lifecycle fence, handed to every table's barrier hook
    /// (ruling M11.T10R6).
    acknowledged_fence: AcknowledgedFence,
    /// Views whose types this crate must name, recovered by `Any` downcast.
    ///
    /// The expiring-time-key view moved to [`Self::expiring_views`] (design item M11.D11).
    /// The global-keyed, key-time, and uncached key-value views stay here because their
    /// APIs are still generic (`GlobalKeyedView<K, V>`) or still hand out borrowed Arrow
    /// data, so none of them is expressible as a `dyn` view. For the global-keyed view that
    /// is not a gap: design item M11.D15c puts the backend seam *below* it, as a
    /// bounded-page loader, precisely because `GlobalKeyedView<K, V>` cannot be a trait
    /// object — so the view a backend's state produces is cached here while the backend
    /// choice is still made through the registry.
    caches: HashMap<String, Box<dyn Any + Send>>,
    /// The typed view map D11 requires: expiring-time-key views are owned trait objects,
    /// so a view can come from any backend and its lifetime is independent of the
    /// `Arc<dyn ErasedTable>` it was built from.
    ///
    /// A table name is in this map or in [`Self::caches`], never both: each getter
    /// refuses a name the other already holds, which is the same
    /// [`StateError::WrongTableKind`] a failed downcast produced when one map served all
    /// four kinds.
    expiring_views: HashMap<String, Box<dyn ExpiringTimeKeyViewApi + Send>>,
}

pub struct BackendWriter {
    sender: Sender<StateMessage>,
    finish_rx: Option<oneshot::Receiver<()>>,
    // TODO: compaction
}

#[allow(unused)]
pub struct BackendFlusher {
    queue: Receiver<StateMessage>,
    storage: StorageProviderRef,
    control_tx: Sender<ControlResp>,
    finish_tx: Option<oneshot::Sender<()>>,
    task_info: Arc<TaskInfo>,
    tables: HashMap<String, Arc<dyn ErasedTable>>,
    table_configs: HashMap<String, TableConfig>,
    table_checkpointers: HashMap<String, Box<dyn ErasedCheckpointer>>,
    current_epoch: u32,
    last_epoch_checkpoints: HashMap<String, TableSubtaskCheckpointMetadata>,
}

impl BackendFlusher {
    fn start(mut self) {
        tokio::spawn(async move {
            loop {
                match self.flush_iteration().await {
                    Ok(continue_flushing) => {
                        if !continue_flushing {
                            return;
                        }
                    }
                    Err(err) => {
                        error!("Failed to flush state file: {:?}", err);
                        self.control_tx
                            .send(ControlResp::TaskFailed {
                                task_id: self.task_info.operator_idx,
                                subtask_idx: self.task_info.task_index,
                                error: err.with_operator(self.task_info.operator_id.clone()).into(),
                            })
                            .await
                            .expect("control queue closed");
                        return;
                    }
                }
            }
        });
    }

    async fn flush_iteration(&mut self) -> DataflowResult<bool> {
        let mut checkpoint_epoch = None;

        for (table_name, checkpointer) in &self.tables {
            let epoch_checkpointer = checkpointer.epoch_checkpointer(
                self.current_epoch,
                self.last_epoch_checkpoints.remove(table_name),
            )?;
            self.table_checkpointers
                .insert(table_name.clone(), epoch_checkpointer);
        }
        self.last_epoch_checkpoints.clear();
        let mut compacted_tables = None;

        // accumulate writes in the RecordBatchBuilders until we get a checkpoint
        while checkpoint_epoch.is_none() {
            tokio::select! {
                op = self.queue.recv() => {
                    match op {
                        Some(StateMessage::Checkpoint(checkpoint)) => {
                            checkpoint_epoch = Some(checkpoint);
                        }
                        Some(StateMessage::BarrierRefused(refusal)) => {
                            error!(
                                table = %refusal.table,
                                epoch = refusal.epoch,
                                "table refused the checkpoint at its barrier hook"
                            );
                            return Err(refusal.error.into());
                        }
                        Some(StateMessage::Compaction(compacted_tables_message)) => {
                            compacted_tables = Some(compacted_tables_message);
                        }
                        Some(StateMessage::TableData { table, data }) => {
                            self.table_checkpointers
                                .get_mut(&table).expect("checkpointer should be there")
                                .insert_data(data).await?
                        },
                        None => {
                            debug!("Parquet flusher closed");
                            return Ok(false);
                        }
                    }
                }
            }
        }
        let Some(cp) = checkpoint_epoch else {
            unreachable!("somehow exited loop without checkpoint_epoch being set");
        };

        let mut metadatas = HashMap::new();
        let mut bytes = 0;
        for (table_name, checkpointer) in self.table_checkpointers.drain() {
            if let Some((subtask_checkpoint_data, size)) = checkpointer.finish(&cp).await? {
                metadatas.insert(table_name.clone(), subtask_checkpoint_data);
                bytes += size;
            }
        }

        if let Some(compaction_metas) = compacted_tables {
            for (table_name, compacted_metadata) in compaction_metas {
                let table = self.tables.get(&table_name).unwrap();
                let Some(compacted_metadata) =
                    table.subtask_metadata_from_table(compacted_metadata)?
                else {
                    continue;
                };
                if let Some(current_metadata) = metadatas.get(&table_name) {
                    let new_metadata = table.apply_compacted_checkpoint(
                        self.current_epoch,
                        compacted_metadata,
                        current_metadata.clone(),
                    )?;
                    metadatas.insert(table_name, new_metadata);
                } else {
                    warn!(
                        "received compaction map for operator {} table {} but no metadata. no checkpoint emitted, as we trust the subtask. map is {:?}",
                        self.task_info.operator_id, table_name, compacted_metadata
                    );
                }
            }
        }
        self.last_epoch_checkpoints = metadatas.clone();
        self.current_epoch += 1;

        // send controller the subtask metadata
        let subtask_metadata = SubtaskCheckpointMetadata {
            subtask_index: self.task_info.task_index,
            start_time: to_micros(cp.time),
            finish_time: to_micros(SystemTime::now()),
            watermark: cp.watermark.map(to_micros),
            table_metadata: metadatas,
            table_configs: self.table_configs.clone(),
            bytes: bytes as u64,
        };
        self.control_tx
            .send(ControlResp::CheckpointCompleted(CheckpointCompleted {
                checkpoint_epoch: cp.epoch as u64,
                operator_idx: self.task_info.operator_idx,
                operator_id: self.task_info.operator_id.clone(),
                subtask_metadata,
            }))
            .await
            .expect("control queue closed");
        if cp.then_stop {
            self.finish_tx
                .take()
                .unwrap()
                .send(())
                .map_err(|_| anyhow::anyhow!("can't send finish"))?;
            return Ok(false);
        }
        Ok(true)
    }
}

impl BackendWriter {
    fn new(
        task_info: Arc<TaskInfo>,
        control_tx: Sender<ControlResp>,
        table_configs: HashMap<String, TableConfig>,
        tables: HashMap<String, Arc<dyn ErasedTable>>,
        storage: StorageProviderRef,
        current_epoch: u32,
        last_epoch_checkpoints: HashMap<String, TableSubtaskCheckpointMetadata>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1024 * 1024);
        let (finish_tx, finish_rx) = oneshot::channel();

        (BackendFlusher {
            queue: rx,
            storage,
            control_tx,
            finish_tx: Some(finish_tx),
            task_info,
            tables,
            table_configs,
            current_epoch,
            table_checkpointers: HashMap::new(),
            last_epoch_checkpoints,
        })
        .start();

        Self {
            sender: tx,
            finish_rx: Some(finish_rx),
        }
    }
}

async fn load_operator_metadata(
    m: &MetadataOrManifest,
    operator_id: &str,
) -> Result<OperatorCheckpointMetadata, anyhow::Error> {
    match m {
        MetadataOrManifest::Metadata(m) => StateBackend::load_operator_metadata(
            &StorageProviderFor::Worker,
            &m.job_id,
            operator_id,
            m.epoch,
        )
        .await?
        .ok_or_else(|| anyhow!("missing metadata field in checkpoint; invalid protobuf")),
        MetadataOrManifest::Manifest(manifest) => manifest
            .operators
            .iter()
            .find(|op| {
                op.operator_metadata
                    .as_ref()
                    .map(|op| op.operator_id == operator_id)
                    .unwrap_or(false)
            })
            .cloned()
            .ok_or_else(|| anyhow!("operator is missing from checkpoint metadata")),
    }
}

impl TableManager {
    /// Builds this subtask's state, restoring it from `restore_from` when there is one.
    ///
    /// A restored checkpoint's table configs record the backend that wrote them. They are
    /// checked against the job's selector — carried explicitly on `task_info` — before a
    /// single table is constructed and before the writer that will produce the next
    /// checkpoint is started, so a job can never read another backend's state or extend a
    /// checkpoint lineage it does not own. This is the read-back side of the selector: an
    /// empty value in a restored config means the checkpoint predates the field and was
    /// therefore written by parquet, which is why it restores into a parquet job and is
    /// refused by a stateengine one.
    ///
    /// Each table is then built by the provider that the job's selector and the table's
    /// logical kind name (design item M11.D20, family (a)). The lookup is one immutable read per
    /// table, made once here when the subtask's state is built and never on the record
    /// path; a selector with no provider is a typed refusal rather than a fall back to
    /// parquet, so a job cannot be quietly run on a backend it did not select.
    ///
    /// # Errors
    ///
    /// Returns the [`arroyo_rpc::state_backend::StateBackendError`] the restored configs
    /// raised, alongside the pre-existing failures of loading and constructing state —
    /// which now include a table config that states no kind and a selector this process has
    /// no provider for. Callers that need the selector failure typed can downcast it.
    ///
    /// When the subtask restored, every table's [`ErasedTable::restored`] is then called
    /// with the restored epoch, after all of them are constructed and before the flusher is
    /// started (ruling M11.T10R7); a table's refusal fails `load` with that table's
    /// [`StateError`], and no flusher is started. `acknowledged_fence` is kept for the barrier
    /// hooks [`Self::checkpoint`] runs (ruling M11.T10R6).
    pub async fn load(
        task_info: Arc<TaskInfo>,
        table_configs: HashMap<String, TableConfig>,
        tx: Sender<ControlResp>,
        restore_from: Option<&MetadataOrManifest>,
        acknowledged_fence: AcknowledgedFence,
    ) -> Result<(Self, Option<SystemTime>)> {
        let (watermark, checkpoint_metadata) = if let Some(metadata) = restore_from {
            let operator_metadata =
                load_operator_metadata(metadata, &task_info.operator_id).await?;
            validate_restored_operator_metadata(task_info.state_backend, &operator_metadata)?;

            let watermark = operator_metadata
                .operator_metadata
                .as_ref()
                .unwrap()
                .min_watermark
                .map(from_micros);

            (watermark, Some(operator_metadata))
        } else {
            (None, None)
        };

        let storage = get_storage_provider(&StorageProviderFor::Worker).await?;

        let tables = table_configs
            .iter()
            .map(|(table_name, table_config)| {
                let table_restore_from = checkpoint_metadata.as_ref().and_then(|metadata| {
                    metadata.table_checkpoint_metadata.get(table_name).cloned()
                });
                // The job's selector, not the table config's copy of it: the two were
                // already checked against each other at the acquisition boundary
                // (`apply_job_state_backend`, design item M11.D13b) and the restored
                // checkpoint's copy a few lines above. Re-deriving the key from the
                // config here would be a second, weaker comparison of values that have
                // already been proven equal.
                let kind = TableKind::of_config(table_name, table_config)?;
                let erased_table = registry().provider(task_info.state_backend, kind)?.table(
                    table_config.clone(),
                    task_info.clone(),
                    storage.clone(),
                    table_restore_from,
                )?;
                Ok((table_name.to_string(), erased_table))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        let epoch;
        let min_epoch;
        let restored_epoch;
        let mut last_epoch_checkpoints = HashMap::new();
        match checkpoint_metadata {
            Some(metadata) => {
                // TODO: validate this logic.
                let Some(operator_metadata) = metadata.operator_metadata else {
                    bail!("missing operator metadata");
                };
                epoch = operator_metadata.epoch + 1;
                min_epoch = operator_metadata.epoch;
                restored_epoch = Some(operator_metadata.epoch);
                for (table, table_metadata) in metadata.table_checkpoint_metadata.clone() {
                    let table_implementation = tables
                        .get(&table)
                        .ok_or_else(|| anyhow!("missing table {}", table))?;
                    if let Some(metadata) =
                        table_implementation.subtask_metadata_from_table(table_metadata)?
                    {
                        last_epoch_checkpoints.insert(table.clone(), metadata);
                    }
                }
            }
            None => {
                epoch = 1;
                min_epoch = 1;
                restored_epoch = None;
            }
        }

        if let Some(restored_epoch) = restored_epoch {
            for table in tables.values() {
                table.restored(restored_epoch)?;
            }
        }

        let writer = BackendWriter::new(
            task_info.clone(),
            tx,
            table_configs,
            tables.clone(),
            storage.clone(),
            epoch,
            last_epoch_checkpoints,
        );
        Ok((
            Self {
                epoch,
                min_epoch,
                tables,
                writer,
                task_info,
                storage: Arc::clone(&storage),
                acknowledged_fence,
                caches: HashMap::new(),
                expiring_views: HashMap::new(),
            },
            watermark,
        ))
    }

    /// Starts checkpoint `barrier.epoch` for this subtask's tables.
    ///
    /// Every table's [`ErasedTable::on_checkpoint_barrier`] runs first, here on the operator
    /// task, with the checkpoint message and the acknowledged fence; only then is the message
    /// enqueued for the flusher, which hands it to each table's `finish` (plan M11.T10b.01).
    /// The message's `min_epoch` is the barrier's — the controller's retention floor — and
    /// not this manager's own `min_epoch` field.
    ///
    /// A hook's refusal is not returned from here: sources reach this through
    /// `SourceOperator::start_checkpoint`, which cannot fail. The refusal is enqueued in the
    /// checkpoint's place instead, and the flusher fails the task with the table's error when
    /// it reaches it, as it does for an error from `finish`.
    pub async fn checkpoint(&mut self, barrier: CheckpointBarrier, watermark: Option<SystemTime>) {
        let checkpoint = CheckpointMessage {
            epoch: barrier.epoch,
            time: barrier.timestamp,
            watermark,
            then_stop: barrier.then_stop,
            min_epoch: barrier.min_epoch,
        };
        let message = match self.run_barrier_hooks(&checkpoint) {
            Ok(()) => StateMessage::Checkpoint(checkpoint),
            Err(refusal) => StateMessage::BarrierRefused(refusal),
        };
        self.writer
            .sender
            .send(message)
            .await
            .expect("should be able to send checkpoint");

        if barrier.then_stop {
            match self.writer.finish_rx.take().unwrap().await {
                Ok(_) => info!("finished stopping checkpoint"),
                Err(err) => warn!("error waiting for stopping checkpoint {:?}", err),
            }
        }
    }

    /// Runs every table's barrier hook for `checkpoint`, stopping at the first refusal.
    fn run_barrier_hooks(&self, checkpoint: &CheckpointMessage) -> Result<(), BarrierRefusal> {
        for (table, implementation) in &self.tables {
            implementation
                .on_checkpoint_barrier(checkpoint, &self.acknowledged_fence)
                .map_err(|error| BarrierRefusal {
                    table: table.clone(),
                    epoch: checkpoint.epoch,
                    error,
                })?;
        }
        Ok(())
    }

    pub async fn load_compacted(&mut self, compacted: &CompactionResult) {
        assert_eq!(
            compacted.operator_id, self.task_info.operator_id,
            "shouldn't be loading compaction for other operator"
        );

        self.writer
            .sender
            .send(StateMessage::Compaction(compacted.compacted_tables.clone()))
            .await
            .expect("queue closed");
    }

    pub async fn insert_committing_data(&mut self, table: &str, data: Vec<u8>) {
        self.writer
            .sender
            .send(StateMessage::TableData {
                table: table.to_string(),
                data: TableData::CommitData { data },
            })
            .await
            .expect("checkpoint queue closed");
    }

    /// Refuses a table name the other view map already holds.
    ///
    /// One name held two views would mean two live handles writing the same table's state
    /// through different code paths. A single `Any` cache made that unrepresentable
    /// because the second getter's downcast failed; now that expiring views live in their
    /// own map, each getter has to check the other.
    fn reject_foreign_cache(
        held_elsewhere: bool,
        table_name: &str,
        expected: &'static str,
    ) -> Result<(), StateError> {
        if held_elsewhere {
            return Err(StateError::WrongTableKind {
                table: table_name.to_string(),
                expected,
            });
        }
        Ok(())
    }

    /// The cached global key/value view for `table_name`, restoring it on first use.
    ///
    /// The restore goes through the backend-neutral bounded-page seam (design item
    /// M11.D15c): the job's selector picks a provider, the provider starts a
    /// [`GlobalKeyValueLoad`], and the decode above the seam turns its pages into this
    /// `GlobalKeyedView<K, V>`. The view stays in the `Any` cache because it is generic
    /// over `K` and `V` and so is not expressible as a `dyn` view — which is exactly why
    /// D15c makes the seam a loader rather than a view.
    ///
    /// The lookup happens inside the cache-miss branch, so it is made once per table name
    /// per subtask and never on the record path.
    ///
    /// # Errors
    ///
    /// [`StateError::NoRegisteredTable`] when the job has no such table,
    /// [`StateError::WrongTableKind`] when the table is not a global key/value table or
    /// when another kind of view is already cached under this name, and
    /// [`StateError::Other`] carrying a [`LookupError`](crate::provider::LookupError) when
    /// this process has no provider for the job's backend. Nothing is cached on any of
    /// these paths, so a failed restore leaves no partial view behind.
    ///
    /// [`GlobalKeyValueLoad`]: crate::tables::global_key_value_load::GlobalKeyValueLoad
    pub async fn get_global_keyed_state<K: Key, V: Data>(
        &mut self,
        table_name: &str,
    ) -> Result<&mut GlobalKeyedView<K, V>, StateError> {
        Self::reject_foreign_cache(
            self.expiring_views.contains_key(table_name),
            table_name,
            "global_keyed_state",
        )?;
        // this is done because populating it is async, so can't use or_insert().
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.caches.entry(table_name.to_string())
        {
            let table_implementation =
                self.tables
                    .get(table_name)
                    .ok_or_else(|| StateError::NoRegisteredTable {
                        table: table_name.to_string(),
                    })?;

            let loader = registry()
                .global_key_value_provider(self.task_info.state_backend)
                .map_err(|e| e.for_table(table_name))?
                .global_key_value_load(table_name, &**table_implementation)
                .await?;
            let saved_data =
                restore::view::<K, V>(table_name, loader, self.writer.sender.clone()).await?;

            let cache: Box<dyn Any + Send> = Box::new(saved_data);
            e.insert(cache);
        }

        let cache = self.caches.get_mut(table_name).unwrap();
        let cache: &mut GlobalKeyedView<K, V> =
            cache
                .downcast_mut()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "global_keyed_state",
                })?;
        Ok(cache)
    }

    /// [`Self::get_global_keyed_state`] with the migrating decode: a source written one
    /// state version back is migrated as it is read.
    ///
    /// The two getters differ only in that decode. They drain the same seam, through the
    /// same provider lookup, with the same page walk.
    ///
    /// # Errors
    ///
    /// Everything [`Self::get_global_keyed_state`] returns, plus
    /// [`StateError::UnsupportedStateVersion`] for state more than one version behind
    /// `V::VERSION`.
    pub async fn get_global_keyed_state_migratable<K: Key, V: MigratableState>(
        &mut self,
        table_name: &str,
    ) -> Result<&mut GlobalKeyedView<K, V>, StateError> {
        Self::reject_foreign_cache(
            self.expiring_views.contains_key(table_name),
            table_name,
            "global_keyed_state",
        )?;
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.caches.entry(table_name.to_string())
        {
            let table_implementation =
                self.tables
                    .get(table_name)
                    .ok_or_else(|| StateError::NoRegisteredTable {
                        table: table_name.to_string(),
                    })?;

            let loader = registry()
                .global_key_value_provider(self.task_info.state_backend)
                .map_err(|e| e.for_table(table_name))?
                .global_key_value_load(table_name, &**table_implementation)
                .await?;
            let saved_data =
                restore::view_migratable::<K, V>(table_name, loader, self.writer.sender.clone())
                    .await?;

            let cache: Box<dyn Any + Send> = Box::new(saved_data);
            e.insert(cache);
        }

        let cache = self.caches.get_mut(table_name).unwrap();
        let cache: &mut GlobalKeyedView<K, V> =
            cache
                .downcast_mut()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "global_keyed_state",
                })?;
        Ok(cache)
    }

    /// The cached expiring-time-key view for `table_name`, building it on first use.
    ///
    /// The view is owned by this manager — it lives in the manager's own view map as a
    /// `Box<dyn ExpiringTimeKeyViewApi + Send>` — and the caller gets a borrow of it that
    /// ends with the caller's own borrow of the manager (design item M11.D13). The view
    /// is not borrowed out of the `Arc<dyn ErasedTable>` the table lives in, so a backend
    /// whose view type this crate does not name can supply one.
    ///
    /// Which backend supplies it is the job's selector, resolved through
    /// [`registry`](crate::provider::registry()) (design item M11.D20, family (a)). The lookup happens
    /// inside the cache-miss branch, so it is made once per table name per subtask and
    /// never on the record path.
    ///
    /// # Errors
    ///
    /// [`StateError::NoRegisteredTable`] when the job has no such table,
    /// [`StateError::WrongTableKind`] when the table is not an expiring time-key table or
    /// when another kind of view is already cached under this name, and
    /// [`StateError::Other`] carrying a [`LookupError`](crate::provider::LookupError) when this process
    /// has no provider for the job's backend.
    pub async fn get_expiring_time_key_table(
        &mut self,
        table_name: &str,
        watermark: Option<SystemTime>,
    ) -> Result<&mut (dyn ExpiringTimeKeyViewApi + Send), StateError> {
        Self::reject_foreign_cache(
            self.caches.contains_key(table_name),
            table_name,
            "expiring_time_key_table",
        )?;
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.expiring_views.entry(table_name.to_string())
        {
            let table_implementation =
                self.tables
                    .get(table_name)
                    .ok_or_else(|| StateError::NoRegisteredTable {
                        table: table_name.to_string(),
                    })?;
            let view = registry()
                .expiring_time_key_provider(self.task_info.state_backend)
                .map_err(|e| e.for_table(table_name))?
                .expiring_time_key_view(
                    table_name,
                    &**table_implementation,
                    self.writer.sender.clone(),
                    watermark,
                )
                .await?;
            e.insert(view);
        }
        Ok(&mut **self
            .expiring_views
            .get_mut(table_name)
            .expect("just inserted if it was missing"))
    }

    pub async fn get_key_time_table(
        &mut self,
        table_name: &str,
        watermark: Option<SystemTime>,
    ) -> Result<&mut KeyTimeView, StateError> {
        Self::reject_foreign_cache(
            self.expiring_views.contains_key(table_name),
            table_name,
            "key_time_table",
        )?;
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.caches.entry(table_name.to_string())
        {
            let table_implementation =
                self.tables
                    .get(table_name)
                    .ok_or_else(|| StateError::NoRegisteredTable {
                        table: table_name.to_string(),
                    })?;
            let expiring_time_key_table = table_implementation
                .as_any()
                .downcast_ref::<ExpiringTimeKeyTable>()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "key_time_table",
                })?;
            let saved_data = expiring_time_key_table
                .get_key_time_view(self.writer.sender.clone(), watermark)
                .await?;
            let cache: Box<dyn Any + Send> = Box::new(saved_data);
            e.insert(cache);
        }
        let cache = self.caches.get_mut(table_name).unwrap();
        let cache: &mut KeyTimeView =
            cache
                .downcast_mut()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "key_time_table",
                })?;
        Ok(cache)
    }

    pub async fn get_uncached_key_value_view(
        &mut self,
        table_name: &str,
    ) -> Result<&mut UncachedKeyValueView, StateError> {
        Self::reject_foreign_cache(
            self.expiring_views.contains_key(table_name),
            table_name,
            "uncached_key_value",
        )?;
        if let std::collections::hash_map::Entry::Vacant(e) =
            self.caches.entry(table_name.to_string())
        {
            let table_implementation =
                self.tables
                    .get(table_name)
                    .ok_or_else(|| StateError::NoRegisteredTable {
                        table: table_name.to_string(),
                    })?;

            let expiring_time_key_table = table_implementation
                .as_any()
                .downcast_ref::<ExpiringTimeKeyTable>()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "uncached_key_value",
                })?;

            let view = expiring_time_key_table
                .get_uncached_key_value_view(self.writer.sender.clone())
                .await?;

            let cache: Box<dyn Any + Send> = Box::new(view);
            e.insert(cache);
        }

        let cache = self.caches.get_mut(table_name).unwrap();
        let cache: &mut UncachedKeyValueView =
            cache
                .downcast_mut()
                .ok_or_else(|| StateError::WrongTableKind {
                    table: table_name.to_string(),
                    expected: "uncached_key_value",
                })?;

        Ok(cache)
    }
}
