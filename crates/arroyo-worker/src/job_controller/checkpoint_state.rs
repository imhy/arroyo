use crate::job_controller::committing_state::{CheckpointIdOrRef, CommittingState};
use anyhow::{anyhow, bail};
use arroyo_datastream::logical::LogicalProgram;
use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::api::OperatorCheckpointDetail;
use arroyo_rpc::grpc::rpc::{
    CheckpointMetadata, OperatorCheckpointMetadata, OperatorMetadata, SubtaskCheckpointMetadata,
    TableCheckpointMetadata, TableConfig, TableEnum, TableSubtaskCheckpointMetadata,
    TaskCheckpointCompletedReq, TaskCheckpointEventReq,
};
use arroyo_rpc::grpc::{api, rpc};
use arroyo_rpc::state_backend::validated::Validated;
use arroyo_rpc::state_backend::{StateBackendSelector, validate_subtask_table_configs};
use arroyo_rpc::{TaskEventSpans, get_event_spans, grpc, log_trace_event};
use arroyo_state::tables::ErasedTable;
use arroyo_state::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use arroyo_state::tables::global_keyed_map::GlobalKeyedTable;
use arroyo_state::validated::{CheckpointMetadataWrite, CompletedCheckpoint, CompletedOperator};
use arroyo_state_protocol::types::Epoch;
use arroyo_types::{from_micros, to_micros};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{debug, warn};

pub type CommitData = HashMap<String, HashMap<String, HashMap<u32, Vec<u8>>>>;

#[derive(Debug, Clone)]
pub struct CheckpointState {
    job_id: Arc<String>,
    pub checkpoint_id: String,
    epoch: Epoch,
    min_epoch: Epoch,
    /// The selector of the job this checkpoint belongs to. Every subtask's table configs
    /// must agree with it before they are merged into an operator's checkpoint metadata.
    state_backend: StateBackendSelector,
    start_time: SystemTime,
    operators: usize,
    operators_checkpointed: usize,
    bytes: u64,
    operator_state: HashMap<String, OperatorState>,
    subtasks_to_commit: HashSet<(String, u32)>,
    // map of operator_id -> table_name -> subtask_index -> Data
    pub commit_data: CommitData,

    // Used for the web ui -- eventually should be replaced with some other way of tracking / reporting
    // this data
    pub operator_details: HashMap<String, OperatorCheckpointDetail>,
    operator_names: HashMap<String, String>,
}

/// What one operator of the job being checkpointed has reported so far.
///
/// `reported` is the set of subtask indices that have sent a finished checkpoint, not a count
/// of the messages that arrived. Those were the same thing until review round 5 of PR #160
/// found that they are not: `finish_subtask` incremented a counter for every message without
/// looking at the index it carried, so at parallelism 2 two reports from subtask 0 made the
/// operator "complete" while subtask 1 had never been heard from — and, since M11.T25d, that
/// counter is what entitles the checkpoint's metadata write. Counting the subtasks that
/// reported rather than the reports that arrived is the fail-closed direction: a duplicate no
/// longer advances completion, so a checkpoint waits for the subtask that genuinely has not
/// reported.
///
/// A report is admitted by [`Self::check_reportable`] *before* any of it is folded in, so a
/// duplicate or an out-of-range index changes nothing about this operator's watermarks, times,
/// bytes or table state rather than being merged and then discounted.
#[derive(Debug, Clone)]
pub struct OperatorState {
    subtasks: usize,
    reported: BTreeSet<u32>,
    bytes: usize,
    pub start_time: Option<SystemTime>,
    pub finish_time: Option<SystemTime>,
    table_state: HashMap<String, TableState>,
    watermarks: Vec<Option<SystemTime>>,
}

impl OperatorState {
    fn new(subtasks: usize) -> Self {
        OperatorState {
            subtasks,
            reported: BTreeSet::new(),
            bytes: 0,
            start_time: None,
            finish_time: None,
            table_state: HashMap::new(),
            watermarks: vec![],
        }
    }

    /// Whether this operator can still count a report from `subtask_index`.
    ///
    /// Two reasons it cannot, and both are refused rather than absorbed:
    ///
    /// - **The operator has no such subtask.** `subtask_index` is a field of a message from a
    ///   worker, and the program is what says how many subtasks this operator has. An index at
    ///   or above that is either a report for a different program or a report from an execution
    ///   this checkpoint is not accumulating, and counting it would complete the operator
    ///   without one of its actual subtasks.
    /// - **That subtask has already reported this checkpoint.** No path re-reports: a subtask
    ///   sends `ControlResp::CheckpointCompleted` once per barrier from `TableManager`, the
    ///   worker's control loop makes one unary call per message and cancels the worker rather
    ///   than retrying it, and the job controller drops any report whose epoch is not the one
    ///   in flight. A second report for an index that already reported is therefore not a retry
    ///   but two writers — a zombie execution's subtask alongside the live one, which is the
    ///   condition M11.D39's fence exists to end. Merging its bytes, watermark and table state
    ///   over the live subtask's would publish a checkpoint made of two executions, so it is
    ///   refused.
    ///
    /// # Errors
    ///
    /// A plain `anyhow` error, which the job controller treats as it treats the two guards
    /// beside this one — an unexpected operator id and a subtask whose table configs disagree
    /// with the job: the checkpoint does not complete and the job fails and restarts from the
    /// last one that did. That is louder than the alternative and deliberately so; the quiet
    /// alternative is the checkpoint publishing state it never received.
    fn check_reportable(&self, operator_id: &str, subtask_index: u32) -> anyhow::Result<()> {
        if subtask_index as usize >= self.subtasks {
            bail!(
                "operator {operator_id} has {} subtask(s), so subtask {subtask_index} is not \
                 one of them",
                self.subtasks
            );
        }
        if self.reported.contains(&subtask_index) {
            bail!(
                "subtask {subtask_index} of operator {operator_id} has already reported this \
                 checkpoint"
            );
        }
        Ok(())
    }

    /// Folds one admitted subtask report in, returning the operator's merged table metadata
    /// once every subtask of it has reported.
    ///
    /// The caller admits the report through [`Self::check_reportable`] first. Recording the
    /// index in a set rather than bumping a counter is what makes that admission belt and
    /// braces rather than the only line of defence: a duplicate that reached here anyway would
    /// still not move the operator any closer to complete.
    fn finish_subtask(
        &mut self,
        c: SubtaskCheckpointMetadata,
    ) -> Option<(
        HashMap<String, TableConfig>,
        HashMap<String, TableCheckpointMetadata>,
    )> {
        self.reported.insert(c.subtask_index);
        self.watermarks.push(c.watermark.map(from_micros));
        self.start_time = match self.start_time {
            Some(existing_start_time) => Some(existing_start_time.min(from_micros(c.start_time))),
            None => Some(from_micros(c.start_time)),
        };
        self.finish_time = match self.finish_time {
            Some(existing_finish_time) => {
                Some(existing_finish_time.max(from_micros(c.finish_time)))
            }
            None => Some(from_micros(c.finish_time)),
        };
        self.bytes += c.bytes as usize;
        for (table, table_metadata) in c.table_metadata {
            self.table_state
                .entry(table)
                .or_insert_with_key(|key| TableState {
                    table_config: c
                        .table_configs
                        .get(key)
                        .expect("should have metadata")
                        .clone(),
                    subtask_tables: HashMap::new(),
                })
                .subtask_tables
                .insert(table_metadata.subtask_index, table_metadata);
        }

        if self.subtasks == self.reported.len() {
            let (table_configs, table_metadatas) = self
                .table_state
                .drain()
                .filter_map(|(table_name, table_state)| {
                    table_state
                        .into_table_metadata()
                        .map(|(table_config, metadata)| {
                            ((table_name.clone(), table_config), (table_name, metadata))
                        })
                })
                .unzip();
            Some((table_configs, table_metadatas))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableState {
    table_config: TableConfig,
    subtask_tables: HashMap<u32, TableSubtaskCheckpointMetadata>,
}

impl TableState {
    fn into_table_metadata(self) -> Option<(TableConfig, TableCheckpointMetadata)> {
        match self.table_config.table_type() {
            TableEnum::MissingTableType => unreachable!(),
            TableEnum::GlobalKeyValue => GlobalKeyedTable::merge_checkpoint_metadata(
                self.table_config.clone(),
                self.subtask_tables,
            )
            .expect("should be able to merge checkpoints"),
            TableEnum::ExpiringKeyedTimeTable => ExpiringTimeKeyTable::merge_checkpoint_metadata(
                self.table_config.clone(),
                self.subtask_tables,
            )
            .expect("should be able to merge checkpoint metadatas"),
        }
        .map(|metadata| (self.table_config, metadata))
    }
}

impl CheckpointState {
    /// Starts accumulating one checkpoint of `program`.
    ///
    /// `state_backend` is the selector of the job being checkpointed, handed in by the
    /// caller — the job controller that owns this state — rather than read from anywhere
    /// ambient. Every subtask's table configs are checked against it as they arrive.
    pub fn new(
        job_id: Arc<String>,
        checkpoint_id: String,
        epoch: Epoch,
        min_epoch: Epoch,
        program: Arc<LogicalProgram>,
        state_backend: StateBackendSelector,
    ) -> Self {
        Self {
            job_id,
            checkpoint_id,
            epoch,
            min_epoch,
            state_backend,
            bytes: 0,
            start_time: SystemTime::now(),
            operators: program.tasks_per_operator().len(),
            operators_checkpointed: 0,
            operator_state: program
                .tasks_per_operator()
                .into_iter()
                .map(|(operator_id, subtasks)| (operator_id, OperatorState::new(subtasks)))
                .collect(),
            subtasks_to_commit: HashSet::new(),
            commit_data: HashMap::new(),
            operator_details: HashMap::new(),
            operator_names: program.operator_names_by_id(),
        }
    }

    fn log_checkpoint_event(
        operator_names: &HashMap<String, String>,
        operator_id: Option<&str>,
        subtask_idx: Option<u64>,
        epoch: Epoch,
        size: u64,
        total_time: Duration,
        spans: TaskEventSpans,
    ) {
        fn to_duration(s: Option<(u64, u64)>) -> f64 {
            match s {
                Some((start, end)) => from_micros(end)
                    .duration_since(from_micros(start))
                    .unwrap_or_default()
                    .as_millis() as f64,
                None => 0.0,
            }
        }

        let name = operator_id.and_then(|id| operator_names.get(id));
        log_trace_event!("checkpoint", {
            "operator_id": operator_id,
            "operator_name": name,
            "subtask_idx": subtask_idx.map(|i| i.to_string()).unwrap_or_default(),
            "epoch": epoch.to_string(),
        }, [
            "size_bytes" => size as f64,
            "total_time" => total_time.as_millis() as f64,
            "alignment_time" => to_duration(spans.alignment),
            "sync_time" => to_duration(spans.sync),
            "async_time" => to_duration(spans.r#async),
            "commit_time" => to_duration(spans.committing),
        ]);
    }

    pub fn start_time(&self) -> SystemTime {
        self.start_time
    }

    pub fn checkpoint_event(&mut self, c: TaskCheckpointEventReq) -> anyhow::Result<()> {
        debug!(message = "Checkpoint event", checkpoint_id = self.checkpoint_id, event_type = ?c.event_type(), subtask_idx = c.subtask_idx, operator_id = ?c.operator_id);

        if grpc::rpc::TaskCheckpointEventType::FinishedCommit == c.event_type() {
            bail!(
                "shouldn't receive finished commit {:?} while checkpointing",
                c
            );
        }

        // This is all for the UI
        self.operator_details
            .entry(c.operator_id.clone())
            .or_insert_with(|| OperatorCheckpointDetail {
                operator_id: c.operator_id.clone(),
                start_time: c.time,
                finish_time: None,
                has_state: false,
                started_metadata_write: None,
                tasks: HashMap::new(),
            })
            .tasks
            .entry(c.subtask_idx)
            .or_insert_with(|| api::TaskCheckpointDetail {
                subtask_index: c.subtask_idx,
                start_time: c.time,
                finish_time: None,
                bytes: None,
                events: vec![],
            })
            .events
            .push(api::TaskCheckpointEvent {
                time: c.time,
                event_type: match c.event_type() {
                    rpc::TaskCheckpointEventType::StartedAlignment => {
                        api::TaskCheckpointEventType::AlignmentStarted
                    }
                    rpc::TaskCheckpointEventType::StartedCheckpointing => {
                        api::TaskCheckpointEventType::CheckpointStarted
                    }
                    rpc::TaskCheckpointEventType::FinishedOperatorSetup => {
                        api::TaskCheckpointEventType::CheckpointOperatorSetupFinished
                    }
                    rpc::TaskCheckpointEventType::FinishedSync => {
                        api::TaskCheckpointEventType::CheckpointSyncFinished
                    }
                    rpc::TaskCheckpointEventType::FinishedCommit => {
                        api::TaskCheckpointEventType::CheckpointPreCommit
                    }
                } as i32,
            });
        Ok(())
    }

    /// Records one subtask's finished checkpoint, returning the operator's merged
    /// checkpoint metadata once every subtask of that operator has reported.
    ///
    /// The subtask's table configs are checked against the job's selector first, before
    /// any of its bytes, watermarks, or table state are folded into this checkpoint.
    /// Merging a subtask that selects a different backend would produce a checkpoint that
    /// is partly one backend's and partly another's, which nothing could later restore.
    ///
    /// The subtask *index* is checked in the same place and for the same reason: an index the
    /// operator does not have, or one that has already reported, is refused before anything of
    /// it is merged rather than merged and then discounted
    /// ([`OperatorState::check_reportable`]). Review round 5 of PR #160 found neither being
    /// checked at all, so a report's identity had no bearing on whether the operator counted
    /// it as one of its subtasks.
    ///
    /// # Errors
    ///
    /// Fails if the request carries no metadata, if the operator is not part of this
    /// checkpoint, if the report is one the operator cannot count, or — carrying an
    /// [`arroyo_rpc::state_backend::StateBackendError`] the caller can downcast — if the
    /// subtask's table configs disagree with the job.
    pub fn checkpoint_finished(
        &mut self,
        c: TaskCheckpointCompletedReq,
    ) -> anyhow::Result<Option<OperatorCheckpointMetadata>> {
        // TODO: UI management
        let metadata = c
            .metadata
            .as_ref()
            .ok_or_else(|| anyhow!("missing metadata for operator {}", c.operator_id))?;

        validate_subtask_table_configs(
            self.state_backend,
            &c.operator_id,
            metadata.subtask_index,
            &metadata.table_configs,
        )?;

        // Before the bytes and the UI detail below, which are the first things this report
        // changes about the checkpoint. An operator this checkpoint does not have is left to
        // the `unexpected operator checkpoint` error further down, which is where it has always
        // been refused; what is checked here is the index, against the operator's own record of
        // which of its subtasks have been heard from.
        if let Some(operator_state) = self.operator_state.get(&c.operator_id) {
            operator_state.check_reportable(&c.operator_id, metadata.subtask_index)?;
        }

        debug!(
            message = "Checkpoint finished",
            checkpoint_id = self.checkpoint_id,
            job_id = *self.job_id,
            epoch = *self.epoch,
            min_epoch = *self.min_epoch,
            operator_id = %c.operator_id,
            subtask_index = metadata.subtask_index,
            time = c.time);

        let operator_detail = self
            .operator_details
            .entry(c.operator_id.clone())
            .or_insert_with(|| OperatorCheckpointDetail {
                operator_id: c.operator_id.clone(),
                start_time: metadata.start_time,
                finish_time: None,
                has_state: false,
                started_metadata_write: None,
                tasks: HashMap::new(),
            });

        self.bytes += metadata.bytes;

        let detail = operator_detail
            .tasks
            .entry(metadata.subtask_index)
            .or_insert_with(|| {
                warn!(
                    "Received checkpoint completion but no start event {:?}",
                    metadata
                );
                api::TaskCheckpointDetail {
                    subtask_index: metadata.subtask_index,
                    start_time: metadata.start_time,
                    finish_time: None,
                    bytes: None,
                    events: vec![],
                }
            });
        detail.bytes = Some(metadata.bytes);
        detail.finish_time = Some(c.time);

        let operator_state = self
            .operator_state
            .get_mut(&c.operator_id)
            .ok_or_else(|| anyhow!("unexpected operator checkpoint {}", c.operator_id))?;
        if let Some((table_configs, table_checkpoint_metadata)) = operator_state.finish_subtask(
            c.metadata
                .ok_or_else(|| anyhow!("missing metadata for operator {}", c.operator_id))?,
        ) {
            self.operators_checkpointed += 1;

            Self::log_checkpoint_event(
                &self.operator_names,
                Some(&c.operator_id),
                None,
                self.epoch,
                operator_state.bytes as u64,
                operator_state
                    .finish_time
                    .unwrap()
                    .duration_since(operator_state.start_time.unwrap())
                    .unwrap_or_default(),
                Default::default(),
            );

            // watermarks are None if any subtasks are None.
            let (min_watermark, max_watermark) =
                if operator_state.watermarks.iter().any(|w| w.is_none()) {
                    (None, None)
                } else {
                    (
                        operator_state
                            .watermarks
                            .iter()
                            .map(|w| to_micros(w.unwrap()))
                            .min(),
                        operator_state
                            .watermarks
                            .iter()
                            .map(|w| to_micros(w.unwrap()))
                            .max(),
                    )
                };
            for (table, checkpoint_metadata) in table_checkpoint_metadata.iter() {
                let config = table_configs
                    .get(table)
                    .expect("should have a config for the table");
                if let Some(committing_data) = match config.table_type() {
                    TableEnum::MissingTableType => bail!("missing table type"),
                    TableEnum::GlobalKeyValue => {
                        GlobalKeyedTable::committing_data(config.clone(), checkpoint_metadata)
                    }
                    TableEnum::ExpiringKeyedTimeTable => {
                        ExpiringTimeKeyTable::committing_data(config.clone(), checkpoint_metadata)
                    }
                } {
                    for i in &operator_state.reported {
                        self.subtasks_to_commit.insert((c.operator_id.clone(), *i));
                    }
                    self.commit_data
                        .entry(c.operator_id.clone())
                        .or_default()
                        .insert(table.clone(), committing_data);
                }
            }

            operator_detail.started_metadata_write = Some(to_micros(SystemTime::now()));
            let metadata = OperatorCheckpointMetadata {
                start_time: to_micros(operator_state.start_time.unwrap()),
                finish_time: to_micros(operator_state.finish_time.unwrap()),
                table_checkpoint_metadata,
                table_configs,
                operator_metadata: Some(OperatorMetadata {
                    job_id: self.job_id.to_string(),
                    operator_id: c.operator_id,
                    epoch: *self.epoch as u32,
                    min_watermark,
                    max_watermark,
                    parallelism: operator_state.reported.len() as u64,
                }),
            };

            operator_detail.finish_time = Some(to_micros(SystemTime::now()));
            return Ok(Some(metadata));
        }
        Ok(None)
    }

    pub fn done(&self) -> bool {
        self.operators == self.operators_checkpointed
    }

    /// Builds this checkpoint's top-level metadata, paired with the operators that are
    /// entitled to appear in it.
    ///
    /// Returns a [`CheckpointMetadataWrite`] rather than the metadata because the write it
    /// feeds takes only a checked one (design item M11.D39c). The two halves are produced
    /// together, here, where the set of operators that reported completion — each having had
    /// its subtask table configs checked as it did — is known; a caller that had to supply
    /// them separately could supply a different set from the one the metadata names.
    ///
    /// # Why the entitlement is a token and not a list
    ///
    /// Producing both halves here was never enough, and review round 4 of PR #160 is where
    /// that showed. The list this used to hand over was `self.operator_state.keys()`, and
    /// `operator_state` is populated from `program.tasks_per_operator()` when the checkpoint
    /// is *opened*: every operator of the job is in it from the first moment, whether or not
    /// it has reported anything. So the "completed operators" were the program's operators,
    /// the metadata named the same set, and [`CheckpointMetadataWrite`]'s own check compared
    /// one with the other and always agreed. What actually stood between an unfinished
    /// checkpoint and its metadata write was [`Self::done`], called by the caller on the way
    /// past — a convention, not something the write required.
    ///
    /// It is carried now: the evidence is a [`Validated<CompletedCheckpoint>`] built from what
    /// each operator's subtasks actually reported, and it cannot be obtained for an operator
    /// that has not finished. `done()` is unchanged and still gates *when* a checkpoint is
    /// finished; what changed is that the write no longer takes anyone's word for it.
    ///
    /// # Which checkpoint the evidence is of
    ///
    /// Review round 5 of PR #160 found the evidence saying nothing about that. It recorded an
    /// epoch that only ever reached an error message and recorded no job at all, and the write
    /// token compared operator sets alone — so a completion of one checkpoint entitled the
    /// metadata of any other whose operators matched, which every epoch of a job's does. The
    /// evidence now names the job and the epoch, and the token compares both against the
    /// metadata. Here the two agree by construction, because both are read from `self.job_id`
    /// and `self.epoch` a few lines apart; the comparison is for every other caller, and for
    /// this one if those two lines ever drift.
    ///
    /// # Errors
    ///
    /// [`StateError::IncompleteCheckpoint`] if any operator of the job's program has not had
    /// every one of its subtasks report. Unreachable through the production callers, which
    /// build metadata only once [`Self::done`] is true — the two conditions are the same one,
    /// counted per operator here and in total there.
    pub fn build_metadata(&mut self) -> Result<CheckpointMetadataWrite, StateError> {
        let finish_time = SystemTime::now();

        for (op, details) in &self.operator_details {
            for (subtask_index, task_details) in &details.tasks {
                let spans = self
                    .operator_details
                    .get(op)
                    .and_then(|c| c.tasks.get(subtask_index))
                    .map(get_event_spans)
                    .unwrap_or_default();

                Self::log_checkpoint_event(
                    &self.operator_names,
                    Some(op),
                    Some(*subtask_index as u64),
                    self.epoch,
                    task_details.bytes.unwrap_or_default(),
                    Duration::from_micros(
                        task_details
                            .finish_time
                            .unwrap_or_default()
                            .saturating_sub(task_details.start_time),
                    ),
                    spans,
                );
            }
        }

        Self::log_checkpoint_event(
            &self.operator_names,
            None,
            None,
            self.epoch,
            self.bytes,
            finish_time
                .duration_since(self.start_time)
                .unwrap_or_default(),
            Default::default(),
        );

        // What each operator reported, not which operators exist. `operator_state` has held a
        // record for every operator of the program since the checkpoint was opened, so its
        // keys alone say nothing; the subtask indices inside are the part that does — and they
        // are indices rather than a count because a count cannot tell one subtask reporting
        // twice from two subtasks reporting once.
        let completed = CompletedCheckpoint::new(
            self.job_id.to_string(),
            *self.epoch as u32,
            self.operator_state
                .iter()
                .map(|(operator_id, state)| {
                    CompletedOperator::reported(
                        operator_id.clone(),
                        state.subtasks,
                        state.reported.iter().copied(),
                    )
                })
                .collect(),
        );
        // The job's program, read through the other derivation of it this checkpoint kept, so
        // the coverage half of the check compares two things rather than one with itself.
        let program_operators: HashSet<&str> =
            self.operator_names.keys().map(String::as_str).collect();
        let completed = Validated::validate(completed, &program_operators)?;

        let mut completed_operators: Vec<String> =
            completed.get().operator_ids().map(str::to_string).collect();
        // A stable order for a set that came out of a `HashMap`: the metadata is written to
        // storage and compared by tests, and iteration order is not a property of the job.
        completed_operators.sort_unstable();

        Ok(CheckpointMetadataWrite::for_completed_checkpoint(
            CheckpointMetadata {
                job_id: self.job_id.to_string(),
                epoch: *self.epoch as u32,
                min_epoch: *self.min_epoch as u32,
                start_time: to_micros(self.start_time),
                finish_time: to_micros(finish_time),
                operator_ids: completed_operators,
            },
            &completed,
        ))
    }

    pub fn needs_commit(&self) -> bool {
        !self.subtasks_to_commit.is_empty()
    }

    pub fn into_commit(self, checkpoint_id: CheckpointIdOrRef) -> CommittingState {
        CommittingState::new(checkpoint_id, self.subtasks_to_commit, self.commit_data)
    }
}

#[cfg(test)]
mod tests {
    use super::CheckpointState;
    use arroyo_datastream::logical::{LogicalNode, LogicalProgram, OperatorName};
    use arroyo_rpc::errors::StateError;
    use arroyo_rpc::grpc::rpc::{
        SubtaskCheckpointMetadata, TableConfig, TableEnum, TaskCheckpointCompletedReq,
    };
    use arroyo_rpc::state_backend::{StateBackendError, StateBackendSelector};
    use arroyo_state_protocol::types::Epoch;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// A checkpoint of a program with no operators. Every subtask report is therefore
    /// "unexpected" once it reaches the merge — which is exactly what makes the merge
    /// observable: reaching it produces that failure and leaves the subtask's bytes and
    /// details behind, while being stopped before it produces neither.
    fn checkpoint_state(job: StateBackendSelector) -> CheckpointState {
        CheckpointState::new(
            Arc::new("job_1".to_string()),
            "chk_1".to_string(),
            Epoch(4),
            Epoch(0),
            Arc::new(LogicalProgram::default()),
            job,
        )
    }

    /// One subtask's finished checkpoint, carrying a single table that states
    /// `state_backend`.
    fn subtask_completed(state_backend: &str) -> TaskCheckpointCompletedReq {
        TaskCheckpointCompletedReq {
            operator_id: "node_3".to_string(),
            epoch: 4,
            metadata: Some(SubtaskCheckpointMetadata {
                subtask_index: 1,
                bytes: 128,
                table_configs: HashMap::from([(
                    "w:window".to_string(),
                    TableConfig {
                        table_type: TableEnum::GlobalKeyValue as i32,
                        config: vec![],
                        state_version: 0,
                        state_backend: state_backend.to_string(),
                    },
                )]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// The merge guard, pinned by the difference between two otherwise identical reports.
    ///
    /// The agreeing one gets past the selector check and into the merge, which records
    /// the subtask's bytes and detail before failing for its own reason. The disagreeing
    /// one fails typed and leaves both untouched: nothing of a foreign backend's subtask
    /// is folded into this job's checkpoint. Move the check after the merge and the
    /// second half of this test fails.
    #[test]
    fn a_subtask_from_another_backend_is_refused_before_it_is_merged() {
        let mut merged = checkpoint_state(StateBackendSelector::StateEngine);
        let err = merged
            .checkpoint_finished(subtask_completed("stateengine"))
            .unwrap_err();
        assert!(err.downcast_ref::<StateBackendError>().is_none(), "{err:?}");
        assert_eq!(merged.bytes, 128);
        assert!(merged.operator_details.contains_key("node_3"));

        let mut refused = checkpoint_state(StateBackendSelector::StateEngine);
        let err = refused
            .checkpoint_finished(subtask_completed("parquet"))
            .unwrap_err();

        let typed = err
            .downcast_ref::<StateBackendError>()
            .unwrap_or_else(|| panic!("a disagreeing subtask must fail typed, got {err:?}"));
        assert!(
            matches!(typed, StateBackendError::TableMismatch { .. }),
            "{typed:?}"
        );
        let message = typed.to_string();
        assert!(message.contains("w:window"), "{message}");
        assert!(message.contains("node_3"), "{message}");
        assert!(message.contains("subtask 1"), "{message}");

        assert_eq!(refused.bytes, 0);
        assert!(refused.operator_details.is_empty());
    }

    /// The compatibility guarantee at the merge: a subtask running a build that predates
    /// the field sends empty table configs, which mean parquet, and a parquet job merges
    /// them exactly as before.
    #[test]
    fn a_subtask_predating_the_selector_merges_into_a_parquet_job() {
        let mut merged = checkpoint_state(StateBackendSelector::Parquet);
        let err = merged
            .checkpoint_finished(subtask_completed(""))
            .unwrap_err();

        assert!(err.downcast_ref::<StateBackendError>().is_none(), "{err:?}");
        assert_eq!(merged.bytes, 128);
    }

    /// A table config nobody recognizes stops the merge rather than being merged under
    /// the job's own selector.
    #[test]
    fn a_subtask_with_an_unknown_selector_is_refused() {
        let mut refused = checkpoint_state(StateBackendSelector::Parquet);
        let err = refused
            .checkpoint_finished(subtask_completed("rocksdb"))
            .unwrap_err();

        assert!(
            matches!(
                err.downcast_ref::<StateBackendError>(),
                Some(StateBackendError::UnknownValue { value, .. }) if value == "rocksdb"
            ),
            "{err:?}"
        );
        assert_eq!(refused.bytes, 0);
    }

    /// A checkpoint of a program with one operator at `parallelism`, so a report can be one
    /// of several rather than the whole of it.
    fn checkpoint_state_of_one_operator(parallelism: usize) -> CheckpointState {
        let mut program = LogicalProgram::default();
        program.graph.add_node(LogicalNode::single(
            1,
            "node_3".to_string(),
            OperatorName::ArrowValue,
            vec![],
            "the only operator".to_string(),
            parallelism,
        ));
        CheckpointState::new(
            Arc::new("job_1".to_string()),
            "chk_1".to_string(),
            Epoch(4),
            Epoch(0),
            Arc::new(program),
            StateBackendSelector::Parquet,
        )
    }

    /// One subtask's finished checkpoint with no tables in it, so the row is about the
    /// reporting rather than about the merge.
    fn subtask_reported(subtask_index: u32) -> TaskCheckpointCompletedReq {
        TaskCheckpointCompletedReq {
            operator_id: "node_3".to_string(),
            epoch: 4,
            metadata: Some(SubtaskCheckpointMetadata {
                subtask_index,
                bytes: 8,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// The metadata of a checkpoint an operator has not finished has no write token
    /// (PR #160 review round 4, finding 2).
    ///
    /// What this used to do is the finding: the entitled operators were
    /// `operator_state.keys()`, and `operator_state` has a record for every operator of the
    /// program from the moment the checkpoint is *opened*. So the metadata named the program's
    /// operators, the entitlement named the program's operators, and the token's check
    /// compared them and agreed — for a checkpoint in which one subtask of two had reported.
    /// The only thing between that and a published metadata object was `done()`, which the
    /// caller ran on the way past.
    ///
    /// The two halves of the row are the same checkpoint one report apart, so what changes the
    /// answer is the reporting and nothing else.
    #[test]
    fn building_the_metadata_of_an_unfinished_checkpoint_yields_no_write_token() {
        let mut half = checkpoint_state_of_one_operator(2);
        half.checkpoint_finished(subtask_reported(0)).unwrap();
        assert!(
            !half.done(),
            "the fixture's precondition: one subtask of two"
        );

        let err = half
            .build_metadata()
            .expect_err("a checkpoint one of whose operators has not finished entitles no write");
        assert!(
            matches!(err, StateError::IncompleteCheckpoint { epoch: 4, .. }),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("node_3 (1/2"), "{message}");

        // The same checkpoint, one report later. Nothing else about it changed.
        half.checkpoint_finished(subtask_reported(1)).unwrap();
        assert!(half.done());
        let write = half
            .build_metadata()
            .expect("every subtask has reported, so the write is entitled");
        assert_eq!(
            write.into_metadata().operator_ids,
            vec!["node_3".to_string()],
            "and it names the operator that finished it"
        );
    }

    /// A subtask reporting twice is refused, and does not complete the operator it is a
    /// subtask of (PR #160 review round 5, finding 1).
    ///
    /// The finding, exactly: `finish_subtask` opened with a bare `subtasks_checkpointed += 1`
    /// and never looked at `c.subtask_index`, so at parallelism 2 the second report from
    /// subtask 0 made `2 == 2`, completed the operator, and — since review round 4 — entitled
    /// the checkpoint's metadata write, while subtask 1 had never been heard from. The counter
    /// is pre-existing Arroyo behaviour; treating it as proof of completion is what M11.T25
    /// added, which is why this is fixed here.
    ///
    /// Both halves are asserted. The refusal is the guard, and it is *before* the merge: the
    /// duplicate leaves the checkpoint's bytes exactly where the first report left them, so
    /// nothing of it is folded in and then discounted. The completion is the evidence, and the
    /// operator is still one subtask short afterwards — a duplicate does not stall a checkpoint
    /// that would otherwise have completed, it declines to complete one that had not.
    #[test]
    fn a_second_report_from_the_same_subtask_is_refused_before_it_is_counted() {
        let mut duplicated = checkpoint_state_of_one_operator(2);
        duplicated.checkpoint_finished(subtask_reported(0)).unwrap();
        assert_eq!(
            duplicated.bytes, 8,
            "the fixture's precondition: one report"
        );

        let err = duplicated
            .checkpoint_finished(subtask_reported(0))
            .expect_err("a subtask that has reported cannot report again");
        let message = err.to_string();
        assert!(message.contains("subtask 0"), "{message}");
        assert!(message.contains("node_3"), "{message}");
        assert!(message.contains("already reported"), "{message}");

        // Refused before the merge, so nothing of it reached the checkpoint.
        assert_eq!(duplicated.bytes, 8, "{:?}", duplicated.bytes);

        // And the operator is still one subtask short, so there is no write token: this is the
        // `2 == 2` the finding names, no longer reached.
        assert!(!duplicated.done());
        let unfinished = duplicated
            .build_metadata()
            .expect_err("two reports from one subtask are not two subtasks");
        assert!(
            matches!(
                unfinished,
                StateError::IncompleteCheckpoint { epoch: 4, .. }
            ),
            "{unfinished:?}"
        );
        let message = unfinished.to_string();
        assert!(message.contains("node_3 (1/2"), "{message}");
        assert!(message.contains("subtask(s) 1 did not report"), "{message}");

        // The subtask that had not reported is the whole of the difference.
        duplicated.checkpoint_finished(subtask_reported(1)).unwrap();
        assert!(duplicated.done());
        duplicated
            .build_metadata()
            .expect("both subtasks have now reported");
    }

    /// A report carrying an index the operator does not have is refused rather than counted
    /// (PR #160 review round 5, finding 1).
    ///
    /// The other half of the same finding: `subtask_index` is a field of a message from a
    /// worker, and nothing compared it against the parallelism the program gives the operator.
    /// An out-of-range index counted exactly like an in-range one, so a single report from
    /// subtask 5 of a two-subtask operator was half of that operator's completion.
    ///
    /// It is refused at the same point as a disagreeing table config and leaves the checkpoint
    /// as untouched, which is what `bytes` asserts.
    #[test]
    fn a_subtask_index_the_operator_does_not_have_is_refused() {
        let mut refused = checkpoint_state_of_one_operator(2);
        let err = refused
            .checkpoint_finished(subtask_reported(5))
            .expect_err("an operator with two subtasks has no subtask 5");
        let message = err.to_string();
        assert!(message.contains("node_3"), "{message}");
        assert!(message.contains("subtask 5"), "{message}");
        assert!(message.contains("2 subtask(s)"), "{message}");

        assert_eq!(refused.bytes, 0);
        assert!(refused.operator_details.is_empty());
        assert!(!refused.done());

        // The boundary itself: index 1 is the last one a two-subtask operator has, and it is
        // admitted — so the guard is a range and not an off-by-one that refuses everything.
        refused.checkpoint_finished(subtask_reported(1)).unwrap();
        assert_eq!(refused.bytes, 8);
    }
}
