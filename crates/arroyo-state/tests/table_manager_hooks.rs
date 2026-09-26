//! M11.T10b.01 through a real `TableManager` and its `BackendFlusher`: the barrier hook
//! (ruling M11.T10R2), the restored hook (ruling M11.T10R7), the checkpoint message's accessors
//! and forwarded `min_epoch`, and the acknowledged fence a table reads (ruling M11.T10R6).
//!
//! A test binary of its own because it installs a provider registry, which a process does once
//! (`provider_install_once.rs`). Every test here runs on that one registry: its stateengine
//! providers build [`RecordingTable`]s, which write what happens to them into the [`Journal`] of
//! the job they were built for. Each test uses its own job id, so the tests share the registry
//! and nothing else.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::time::{Duration, SystemTime};

use arrow_array::RecordBatch;
use arroyo_rpc::errors::{ErrorDomain, StateError};
use arroyo_rpc::grpc::rpc::{
    CheckpointManifest, OperatorCheckpointMetadata, OperatorMetadata, TableCheckpointMetadata,
    TableConfig, TableSubtaskCheckpointMetadata,
};
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_rpc::{ControlResp, MetadataOrManifest};
use arroyo_state::ownership::{AcknowledgedFence, AcknowledgedFenceWriter, Raise};
use arroyo_state::provider::{
    ExpiringTimeKeyProvider, GlobalKeyValueProvider, ProviderRegistry, ProviderRegistryBuilder,
    StateBackendProvider, TableKind, install,
};
use arroyo_state::tables::expiring_time_key_view::{BatchDrainToken, ExpiringTimeKeyViewApi};
use arroyo_state::tables::global_key_value_load::GlobalKeyValueLoad;
use arroyo_state::tables::table_manager::TableManager;
use arroyo_state::tables::{CompactionConfig, ErasedCheckpointer, ErasedTable};
use arroyo_state::validated::ValidatedTable;
use arroyo_state::{CheckpointMessage, StateMessage, TableData};
use arroyo_state_protocol::gc::liveness::LivenessRefusal;
use arroyo_storage::StorageProviderRef;
use arroyo_types::{CheckpointBarrier, TaskInfo};
use async_trait::async_trait;
use tokio::sync::mpsc::{Receiver, Sender};

/// The operator every subtask here belongs to.
const OPERATOR: &str = "op_1";

/// The subtask's tables: one of each live kind.
const TABLES: [(&str, TableKind); 2] = [
    ("global", TableKind::GlobalKeyValue),
    ("expiring", TableKind::ExpiringKeyedTime),
];

/// How long a barrier hook, or the restored hook, dwells before it returns.
///
/// Long enough for a flusher on another worker thread to run in the meantime — to dequeue a
/// message and call `finish`, or to ask for its first checkpointers — which it could do only if
/// the checkpoint had been enqueued before the barrier hooks ran, or had been started before
/// the restored hooks ran. That is what makes "every hook returned before the flusher acted"
/// evidence of the order rather than an accident of scheduling.
const HOOK_DWELL: Duration = Duration::from_millis(100);

fn at(seconds: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
}

fn barrier(epoch: u32, min_epoch: u32, then_stop: bool) -> CheckpointBarrier {
    CheckpointBarrier {
        epoch,
        min_epoch,
        timestamp: SystemTime::now(),
        then_stop,
    }
}

/// What happened to one table, as that table saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Restored {
        table: String,
        epoch: u32,
    },
    HookEntered {
        table: String,
        barrier: Seen,
        fence: u64,
    },
    HookLeft {
        table: String,
        epoch: u32,
    },
    Checkpointer {
        table: String,
        epoch: u32,
        previous: bool,
    },
    Finished {
        table: String,
        checkpoint: Seen,
    },
    ViewOpened {
        table: String,
    },
}

/// A checkpoint message as its public accessors read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Seen {
    epoch: u32,
    min_epoch: u32,
    then_stop: bool,
    watermark: Option<SystemTime>,
}

impl Seen {
    fn of(checkpoint: &CheckpointMessage) -> Self {
        Self {
            epoch: checkpoint.epoch(),
            min_epoch: checkpoint.min_epoch(),
            then_stop: checkpoint.then_stop(),
            watermark: checkpoint.watermark(),
        }
    }
}

/// What one table is told to refuse.
#[derive(Default, Clone, Copy)]
struct Script {
    refuse_barrier_at: Option<u32>,
    refuse_restore: bool,
}

/// One job's record of what happened to its tables, in the order it happened.
struct Journal {
    events: Mutex<Vec<Event>>,
    scripts: HashMap<String, Script>,
    /// The fence handle each table kept from the first barrier it saw.
    kept_fences: Mutex<HashMap<String, AcknowledgedFence>>,
}

impl Journal {
    fn record(&self, event: Event) {
        self.events.lock().unwrap().push(event);
    }

    fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    fn script(&self, table: &str) -> Script {
        self.scripts.get(table).copied().unwrap_or_default()
    }

    fn kept_fence(&self, table: &str) -> AcknowledgedFence {
        self.kept_fences
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .expect("the table saw a barrier")
    }

    /// The events once `done` holds for them; the flusher records on its own task.
    async fn wait_for(&self, done: impl Fn(&[Event]) -> bool) -> Vec<Event> {
        for _ in 0..3000 {
            let events = self.events();
            if done(&events) {
                return events;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "the journal never reached the expected state: {:?}",
            self.events()
        );
    }
}

fn journals() -> &'static Mutex<HashMap<String, Arc<Journal>>> {
    static JOURNALS: OnceLock<Mutex<HashMap<String, Arc<Journal>>>> = OnceLock::new();
    JOURNALS.get_or_init(Default::default)
}

fn open_journal(job: &str, scripts: &[(&str, Script)]) -> Arc<Journal> {
    let journal = Arc::new(Journal {
        events: Mutex::new(vec![]),
        scripts: scripts
            .iter()
            .map(|(table, script)| (table.to_string(), *script))
            .collect(),
        kept_fences: Mutex::new(HashMap::new()),
    });
    let previous = journals()
        .lock()
        .unwrap()
        .insert(job.to_string(), journal.clone());
    assert!(previous.is_none(), "each test uses its own job id");
    journal
}

fn journal_of(job: &str) -> Arc<Journal> {
    journals()
        .lock()
        .unwrap()
        .get(job)
        .cloned()
        .expect("the test opened this job's journal")
}

fn barrier_refusal(table: &str, epoch: u32) -> StateError {
    StateError::Other {
        table: table.to_string(),
        error: format!("checkpoint {epoch} arrived while an older one is still pending"),
    }
}

fn restore_refusal(table: &str, epoch: u32) -> StateError {
    StateError::Other {
        table: table.to_string(),
        error: format!("the root this table was built from is not the one of epoch {epoch}"),
    }
}

fn unsupported() -> StateError {
    StateError::Other {
        table: "recording".to_string(),
        error: "this fixture records the table lifecycle and stores no state".to_string(),
    }
}

/// A table that records its lifecycle into its job's journal and stores nothing.
struct RecordingTable {
    name: String,
    journal: Arc<Journal>,
}

impl ErasedTable for RecordingTable {
    fn from_config(
        _config: TableConfig,
        _task_info: Arc<TaskInfo>,
        _storage_provider: StorageProviderRef,
        _checkpoint_message: Option<TableCheckpointMetadata>,
    ) -> Result<Self, StateError> {
        Err(unsupported())
    }

    fn epoch_checkpointer(
        &self,
        epoch: u32,
        previous_metadata: Option<TableSubtaskCheckpointMetadata>,
    ) -> Result<Box<dyn ErasedCheckpointer>, StateError> {
        self.journal.record(Event::Checkpointer {
            table: self.name.clone(),
            epoch,
            previous: previous_metadata.is_some(),
        });
        Ok(Box::new(RecordingCheckpointer {
            name: self.name.clone(),
            journal: self.journal.clone(),
        }))
    }

    fn merge_checkpoint_metadata(
        _config: TableConfig,
        _subtask_metadata: HashMap<u32, TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<TableCheckpointMetadata>, StateError> {
        Err(unsupported())
    }

    fn subtask_metadata_from_table(
        &self,
        table_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableSubtaskCheckpointMetadata>, StateError> {
        Ok(Some(TableSubtaskCheckpointMetadata {
            subtask_index: 0,
            table_type: table_metadata.table_type,
            data: table_metadata.data,
        }))
    }

    fn table_type() -> arroyo_rpc::grpc::rpc::TableEnum {
        TableKind::GlobalKeyValue.as_table_enum()
    }

    fn files_to_keep(_table: ValidatedTable<'_>) -> Result<HashSet<String>, StateError> {
        Err(unsupported())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn compact_data(
        _config: TableConfig,
        _compaction_config: &CompactionConfig,
        _operator_metadata: &OperatorMetadata,
        _current_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableCheckpointMetadata>, StateError> {
        Err(unsupported())
    }

    fn committing_data(
        _config: TableConfig,
        _table_metadata: &TableCheckpointMetadata,
    ) -> Option<HashMap<u32, Vec<u8>>> {
        None
    }

    fn apply_compacted_checkpoint(
        &self,
        _epoch: u32,
        _compacted_checkpoint: TableSubtaskCheckpointMetadata,
        _subtask_metadata: TableSubtaskCheckpointMetadata,
    ) -> Result<TableSubtaskCheckpointMetadata, StateError> {
        Err(unsupported())
    }

    fn on_checkpoint_barrier(
        &self,
        checkpoint: &CheckpointMessage,
        acknowledged_fence: &AcknowledgedFence,
    ) -> Result<(), StateError> {
        self.journal.record(Event::HookEntered {
            table: self.name.clone(),
            barrier: Seen::of(checkpoint),
            fence: acknowledged_fence.get(),
        });
        self.journal
            .kept_fences
            .lock()
            .unwrap()
            .entry(self.name.clone())
            .or_insert_with(|| acknowledged_fence.clone());
        std::thread::sleep(HOOK_DWELL);
        self.journal.record(Event::HookLeft {
            table: self.name.clone(),
            epoch: checkpoint.epoch(),
        });
        if self.journal.script(&self.name).refuse_barrier_at == Some(checkpoint.epoch()) {
            return Err(barrier_refusal(&self.name, checkpoint.epoch()));
        }
        Ok(())
    }

    fn restored(&self, epoch: u32) -> Result<(), StateError> {
        self.journal.record(Event::Restored {
            table: self.name.clone(),
            epoch,
        });
        std::thread::sleep(HOOK_DWELL);
        if self.journal.script(&self.name).refuse_restore {
            return Err(restore_refusal(&self.name, epoch));
        }
        Ok(())
    }
}

struct RecordingCheckpointer {
    name: String,
    journal: Arc<Journal>,
}

#[async_trait]
impl ErasedCheckpointer for RecordingCheckpointer {
    async fn insert_data(&mut self, _data: TableData) -> Result<(), StateError> {
        Ok(())
    }

    async fn finish(
        self: Box<Self>,
        checkpoint: &CheckpointMessage,
    ) -> Result<Option<(TableSubtaskCheckpointMetadata, usize)>, StateError> {
        self.journal.record(Event::Finished {
            table: self.name.clone(),
            checkpoint: Seen::of(checkpoint),
        });
        Ok(None)
    }
}

/// A view with nothing in it, so that opening one can be recorded.
struct EmptyView;

#[async_trait]
impl ExpiringTimeKeyViewApi for EmptyView {
    fn insert(
        &mut self,
        _max_timestamp: SystemTime,
        _batch: RecordBatch,
    ) -> Result<(), StateError> {
        Ok(())
    }

    async fn flush(&mut self, _watermark: Option<SystemTime>) -> Result<(), StateError> {
        Ok(())
    }

    async fn flush_timestamp(&mut self, _timestamp: SystemTime) -> Result<(), StateError> {
        Ok(())
    }

    async fn expire_timestamp(&mut self, _timestamp: SystemTime) -> Result<(), StateError> {
        Ok(())
    }

    fn get_min_time(&self) -> Option<SystemTime> {
        None
    }

    fn begin_batch_drain(&mut self, _watermark: Option<SystemTime>) -> BatchDrainToken {
        BatchDrainToken::mint()
    }

    async fn next_drained_batch(
        &mut self,
        _token: BatchDrainToken,
    ) -> Result<Option<(SystemTime, RecordBatch)>, StateError> {
        Ok(None)
    }
}

/// The stateengine provider for one live kind, building [`RecordingTable`]s.
///
/// A table's name travels in its config's payload, which nothing but this provider reads.
struct RecordingProvider {
    kind: TableKind,
}

#[async_trait]
impl StateBackendProvider for RecordingProvider {
    fn selector(&self) -> StateBackendSelector {
        StateBackendSelector::StateEngine
    }

    fn table_kind(&self) -> TableKind {
        self.kind
    }

    fn table(
        &self,
        config: TableConfig,
        task_info: Arc<TaskInfo>,
        _storage: StorageProviderRef,
        _checkpoint: Option<TableCheckpointMetadata>,
    ) -> Result<Arc<dyn ErasedTable>, StateError> {
        Ok(Arc::new(RecordingTable {
            name: String::from_utf8(config.config).expect("a fixture names its table in UTF-8"),
            journal: journal_of(&task_info.job_id),
        }))
    }

    fn merge_checkpoint_metadata(
        &self,
        _config: TableConfig,
        _subtask_metadata: HashMap<u32, TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<TableCheckpointMetadata>, StateError> {
        Err(unsupported())
    }

    fn committing_data(
        &self,
        _config: TableConfig,
        _table_metadata: &TableCheckpointMetadata,
    ) -> Option<HashMap<u32, Vec<u8>>> {
        None
    }

    fn files_to_keep(&self, _table: ValidatedTable<'_>) -> Result<HashSet<String>, StateError> {
        Err(unsupported())
    }

    fn table_data_files(
        &self,
        _metadata: &TableCheckpointMetadata,
    ) -> Result<Vec<String>, LivenessRefusal> {
        Ok(vec![])
    }

    async fn compact_data(
        &self,
        _config: TableConfig,
        _compaction_config: &CompactionConfig,
        _operator_metadata: &OperatorMetadata,
        _current_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableCheckpointMetadata>, StateError> {
        Err(unsupported())
    }
}

#[async_trait]
impl GlobalKeyValueProvider for RecordingProvider {
    async fn global_key_value_load(
        &self,
        _table_name: &str,
        _table: &dyn ErasedTable,
    ) -> Result<Box<dyn GlobalKeyValueLoad + Send>, StateError> {
        Err(unsupported())
    }
}

#[async_trait]
impl ExpiringTimeKeyProvider for RecordingProvider {
    async fn expiring_time_key_view(
        &self,
        table_name: &str,
        table: &dyn ErasedTable,
        _state_tx: Sender<StateMessage>,
        _watermark: Option<SystemTime>,
    ) -> Result<Box<dyn ExpiringTimeKeyViewApi + Send>, StateError> {
        let table = table
            .as_any()
            .downcast_ref::<RecordingTable>()
            .expect("this provider built the table");
        table.journal.record(Event::ViewOpened {
            table: table_name.to_string(),
        });
        Ok(Box::new(EmptyView))
    }
}

/// Installs the recording backend as this process's registry and points worker storage at a
/// directory of this process's own; the first test to run does it for all of them.
fn install_recording_backend() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let registry = ProviderRegistry::builder()
            .register_global_key_value(Arc::new(RecordingProvider {
                kind: TableKind::GlobalKeyValue,
            }))
            .and_then(|b| {
                b.register_expiring_keyed_time(Arc::new(RecordingProvider {
                    kind: TableKind::ExpiringKeyedTime,
                }))
            })
            .and_then(ProviderRegistryBuilder::build)
            .expect("one backend serving both live kinds");
        install(registry).expect("nothing was installed or looked up yet");

        arroyo_rpc::config::config();
        let directory = std::env::temp_dir().join(format!(
            "arroyo-t10-table-manager-hooks-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).expect("checkpoint directory");
        let url = format!("file://{}", directory.display());
        arroyo_rpc::config::update(|config| config.checkpoint_url = url.clone());
    });
}

fn table_configs() -> HashMap<String, TableConfig> {
    TABLES
        .iter()
        .map(|(name, kind)| {
            (
                name.to_string(),
                TableConfig {
                    table_type: kind.as_table_enum() as i32,
                    config: name.as_bytes().to_vec(),
                    state_version: 0,
                    state_backend: StateBackendSelector::StateEngine.as_str().to_string(),
                },
            )
        })
        .collect()
}

/// A checkpoint of `OPERATOR` at `epoch` in which only the tables named in `with_state` have
/// state of their own.
fn restore_point(job: &str, epoch: u32, with_state: &[&str]) -> MetadataOrManifest {
    MetadataOrManifest::Manifest(CheckpointManifest {
        epoch: u64::from(epoch),
        operators: vec![OperatorCheckpointMetadata {
            operator_metadata: Some(OperatorMetadata {
                job_id: job.to_string(),
                operator_id: OPERATOR.to_string(),
                epoch,
                ..Default::default()
            }),
            table_configs: table_configs(),
            table_checkpoint_metadata: TABLES
                .iter()
                .filter(|(name, _)| with_state.contains(name))
                .map(|(name, kind)| {
                    (
                        name.to_string(),
                        TableCheckpointMetadata {
                            table_type: kind.as_table_enum() as i32,
                            data: name.as_bytes().to_vec(),
                        },
                    )
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    })
}

type Loaded = anyhow::Result<(TableManager, Option<SystemTime>)>;

/// Loads `OPERATOR`'s subtask 0 of `job` on the recording backend, with a live control channel.
async fn load(
    job: &str,
    restore_from: Option<&MetadataOrManifest>,
    acknowledged_fence: AcknowledgedFence,
) -> (Loaded, Receiver<ControlResp>) {
    install_recording_backend();
    let task_info = Arc::new(TaskInfo {
        state_backend: StateBackendSelector::StateEngine,
        ..TaskInfo::for_test(job, OPERATOR)
    });
    let (control_tx, control_rx) = tokio::sync::mpsc::channel(16);
    let loaded = TableManager::load(
        task_info,
        table_configs(),
        control_tx,
        restore_from,
        acknowledged_fence,
    )
    .await;
    (loaded, control_rx)
}

/// The next message the subtask sends its controller, or `None` once nothing can send one.
async fn next_control(control: &mut Receiver<ControlResp>) -> Option<ControlResp> {
    tokio::time::timeout(Duration::from_secs(30), control.recv())
        .await
        .expect("the subtask answers within the timeout")
}

fn assert_completed(response: Option<ControlResp>, epoch: u32) {
    match response {
        Some(ControlResp::CheckpointCompleted(completed)) => {
            assert_eq!(completed.checkpoint_epoch, u64::from(epoch));
            assert_eq!(completed.operator_id, OPERATOR);
        }
        other => panic!("expected checkpoint {epoch} to complete, got {other:?}"),
    }
}

fn hooks_of(events: &[Event], table: &str) -> Vec<(Seen, u64)> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::HookEntered {
                table: t,
                barrier,
                fence,
            } if t == table => Some((*barrier, *fence)),
            _ => None,
        })
        .collect()
}

fn finishes_of(events: &[Event], table: &str) -> Vec<Seen> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Finished {
                table: t,
                checkpoint,
            } if t == table => Some(*checkpoint),
            _ => None,
        })
        .collect()
}

fn position(events: &[Event], wanted: impl Fn(&Event) -> bool) -> Vec<usize> {
    events
        .iter()
        .enumerate()
        .filter(|(_, event)| wanted(event))
        .map(|(index, _)| index)
        .collect()
}

/// Every table's barrier hook for epoch `epoch` returned before any table's `finish` for it
/// began.
fn assert_hooks_precede_finishes(events: &[Event], epoch: u32) {
    let hooks_left = position(
        events,
        |event| matches!(event, Event::HookLeft { epoch: e, .. } if *e == epoch),
    );
    let finishes = position(
        events,
        |event| matches!(event, Event::Finished { checkpoint, .. } if checkpoint.epoch == epoch),
    );
    assert_eq!(hooks_left.len(), TABLES.len(), "{events:?}");
    assert_eq!(finishes.len(), TABLES.len(), "{events:?}");
    assert!(
        hooks_left.iter().max() < finishes.iter().min(),
        "a finish for {epoch} began before every barrier hook for it had returned: {events:?}"
    );
}

/// The barrier hook runs on the operator task before the checkpoint is enqueued, once per table
/// per barrier, with the message `finish` later receives, and it reads the fence live.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn barrier_hooks_run_before_the_checkpoint_is_enqueued_and_read_the_live_fence() {
    let job = "t10-barrier-hook";
    let journal = open_journal(job, &[]);
    let mut writer = AcknowledgedFenceWriter::unacknowledged();
    let (loaded, mut control) = load(job, None, writer.reader()).await;
    let (mut manager, _) = loaded.expect("a fresh subtask loads");

    manager.checkpoint(barrier(1, 0, false), Some(at(40))).await;
    assert_completed(next_control(&mut control).await, 1);

    // The fence rises between barriers, as an already-running adoption raises it, and the
    // handle each table kept at barrier 1 reads the rise with no barrier in between. No table
    // here holds a deletion guard, so the raise closes, drains and publishes in one step.
    let Raise::Ready(ready) = writer.prepare(7, None) else {
        panic!("no deletion is in flight")
    };
    assert_eq!(ready.publish(), 7);
    for (table, _) in TABLES {
        assert_eq!(journal.kept_fence(table).get(), 7);
    }

    // A stopping barrier: `checkpoint` returns only once the flusher has finished it.
    manager.checkpoint(barrier(2, 1, true), None).await;
    let events = journal.events();
    let first = Seen {
        epoch: 1,
        min_epoch: 0,
        then_stop: false,
        watermark: Some(at(40)),
    };
    let second = Seen {
        epoch: 2,
        min_epoch: 1,
        then_stop: true,
        watermark: None,
    };
    for (table, _) in TABLES {
        assert_eq!(hooks_of(&events, table), vec![(first, 0), (second, 7)]);
        assert_eq!(finishes_of(&events, table), vec![first, second]);
    }
    assert_completed(next_control(&mut control).await, 2);
    assert_hooks_precede_finishes(&events, 1);
    assert_hooks_precede_finishes(&events, 2);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Event::Restored { .. })),
        "a fresh start restores nothing and says so to nobody: {events:?}"
    );
}

/// A hook's refusal is surfaced by the flusher as the task's failure, carrying the table's own
/// error, and no table finishes the refused epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_barrier_fails_the_task_with_the_tables_error_and_finishes_nothing() {
    let job = "t10-barrier-refused";
    let journal = open_journal(
        job,
        &[(
            "global",
            Script {
                refuse_barrier_at: Some(2),
                refuse_restore: false,
            },
        )],
    );
    let (loaded, mut control) = load(job, None, AcknowledgedFence::unfenced()).await;
    let (mut manager, _) = loaded.expect("a fresh subtask loads");

    manager.checkpoint(barrier(1, 0, false), None).await;
    assert_completed(next_control(&mut control).await, 1);

    manager.checkpoint(barrier(2, 1, false), None).await;
    match next_control(&mut control).await {
        Some(ControlResp::TaskFailed {
            task_id,
            subtask_idx,
            error,
        }) => {
            assert_eq!((task_id, subtask_idx), (1, 0));
            assert_eq!(error.message, barrier_refusal("global", 2).to_string());
            assert_eq!(error.operator_id.as_deref(), Some(OPERATOR));
            assert_eq!(error.domain, ErrorDomain::Internal);
            assert_eq!(error.details, None);
        }
        other => panic!("expected the refusal to fail the task, got {other:?}"),
    }
    // The flusher stopped: every sender of the control channel is gone and nothing followed.
    assert!(next_control(&mut control).await.is_none());

    let events = journal.events();
    assert_eq!(
        hooks_of(&events, "global")
            .iter()
            .filter(|(seen, _)| seen.epoch == 2)
            .count(),
        1
    );
    for (table, _) in TABLES {
        assert!(
            finishes_of(&events, table)
                .iter()
                .all(|seen| seen.epoch == 1),
            "{events:?}"
        );
    }
}

/// A restored subtask tells every table the restored epoch once, before the flusher asks for a
/// checkpointer and before any view exists; the next barrier's `min_epoch` is the controller's,
/// not the restored epoch; and an unfenced handle reads zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restored_subtask_tells_every_table_its_epoch_before_the_flusher_starts() {
    let job = "t10-restored";
    let journal = open_journal(job, &[]);
    let restore_from = restore_point(job, 5, &["global"]);
    let (loaded, mut control) = load(job, Some(&restore_from), AcknowledgedFence::unfenced()).await;
    let (mut manager, _) = loaded.expect("the subtask restores");

    let events = journal
        .wait_for(|events| {
            events
                .iter()
                .filter(|event| matches!(event, Event::Checkpointer { .. }))
                .count()
                == TABLES.len()
        })
        .await;
    let restored = position(&events, |event| matches!(event, Event::Restored { .. }));
    let checkpointers = position(&events, |event| matches!(event, Event::Checkpointer { .. }));
    assert_eq!(restored.len(), TABLES.len(), "{events:?}");
    assert!(
        restored.iter().max() < checkpointers.iter().min(),
        "{events:?}"
    );
    for (table, _) in TABLES {
        assert!(events.contains(&Event::Restored {
            table: table.to_string(),
            epoch: 5,
        }));
    }
    assert!(events.contains(&Event::Checkpointer {
        table: "global".to_string(),
        epoch: 6,
        previous: true,
    }));
    assert!(events.contains(&Event::Checkpointer {
        table: "expiring".to_string(),
        epoch: 6,
        previous: false,
    }));

    manager
        .get_expiring_time_key_table("expiring", None)
        .await
        .expect("the view opens");
    let events = journal.events();
    let opened = position(&events, |event| matches!(event, Event::ViewOpened { .. }));
    assert_eq!(opened.len(), 1);
    assert!(restored.iter().max() < opened.iter().min(), "{events:?}");

    manager.checkpoint(barrier(6, 3, false), None).await;
    assert_completed(next_control(&mut control).await, 6);
    let events = journal.events();
    let sixth = Seen {
        epoch: 6,
        min_epoch: 3,
        then_stop: false,
        watermark: None,
    };
    for (table, _) in TABLES {
        assert_eq!(hooks_of(&events, table), vec![(sixth, 0)]);
        assert_eq!(finishes_of(&events, table), vec![sixth]);
    }
    assert_eq!(
        position(&events, |event| matches!(event, Event::Restored { .. })).len(),
        TABLES.len(),
        "restored is called once per table, at load, and never again"
    );
}

/// A table's refusal of the restored epoch fails `load` with that table's error, and no
/// flusher is started.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_restore_fails_load_with_the_tables_error_before_the_flusher_starts() {
    let job = "t10-restore-refused";
    let journal = open_journal(
        job,
        &[(
            "expiring",
            Script {
                refuse_barrier_at: None,
                refuse_restore: true,
            },
        )],
    );
    let restore_from = restore_point(job, 5, &[]);
    let (loaded, mut control) = load(job, Some(&restore_from), AcknowledgedFence::unfenced()).await;
    let error = match loaded {
        Ok(_) => panic!("a refused restore must not load"),
        Err(error) => error,
    };
    assert_eq!(
        error
            .downcast_ref::<StateError>()
            .expect("the table's own error")
            .to_string(),
        restore_refusal("expiring", 5).to_string()
    );

    // The control sender went down with the failed load, so no flusher holds it; and no table
    // was asked for a checkpointer.
    assert!(next_control(&mut control).await.is_none());
    assert!(
        !journal
            .events()
            .iter()
            .any(|event| matches!(event, Event::Checkpointer { .. }))
    );
}
