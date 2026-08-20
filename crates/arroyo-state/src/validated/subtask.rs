//! One subtask's finished checkpoint, as a whole object.
//!
//! This is the innermost boundary of the completion family. A checkpoint's metadata write is
//! entitled by a [`CompletedCheckpoint`](super::CompletedCheckpoint), which is entitled by
//! every operator having heard from every one of its subtasks — and *that* is entitled by the
//! reports the operator merged. A report is where the identity chain starts, so it is where an
//! identity that disagrees with itself has to be refused.
//!
//! # What review round 6 of PR #160 found
//!
//! A report carries its subtask index **twice**: once at the top, as
//! `SubtaskCheckpointMetadata.subtask_index`, and once per table, as
//! `TableSubtaskCheckpointMetadata.subtask_index`. Round 5 made the operator's completion
//! accounting turn on the first of those — the set of distinct top-level indices, replacing a
//! counter that could not tell one subtask reporting twice from two subtasks reporting once.
//! Nothing ever compared it with the second.
//!
//! So at parallelism 2 the top-level indices `{0, 1}` could complete the operator while both
//! reports' table payloads were labelled subtask `0`: the merge keyed each table payload by
//! *its own* index, the second overwrote the first in the operator's per-table map, and the
//! checkpoint certified state that one subtask had never contributed. Two representations of
//! one identity, each independently trusted, is the shape of the whole round-6 finding set —
//! see [`super::identity`].
//!
//! # Why a token rather than another check
//!
//! Round 5's fix was a guard the caller ran before the merge
//! (`OperatorState::check_reportable`). It was correct and it is still here — but as the
//! *content* of a whole-object check rather than as a convention the merge site remembers to
//! run, because a guard beside a call is exactly what M11.D39c exists to replace. The merge
//! now takes `Validated<SubtaskReport>`; there is no spelling of it that takes a report whose
//! identities have not been compared with each other and with the operator's own record.

use arroyo_rpc::grpc::rpc::{
    SubtaskCheckpointMetadata, TableConfig, TableSubtaskCheckpointMetadata,
};
use arroyo_rpc::state_backend::validated::{Validated, WholeObject};
use arroyo_rpc::state_backend::{StateBackendSelector, validate_subtask_table_configs};
use std::collections::BTreeSet;

/// The operator a report claims to be a subtask of, as that operator's own bookkeeping knows
/// it.
///
/// Everything here comes from the checkpoint rather than from the report, which is the point:
/// a report cannot be asked to confirm its own admissibility.
#[derive(Debug, Clone, Copy)]
pub struct ReportingOperator<'a> {
    /// The operator the checkpoint is merging this report into.
    pub operator_id: &'a str,
    /// How many subtasks the job's program gives that operator.
    pub subtasks: usize,
    /// The subtask indices that have already reported this checkpoint.
    pub already_reported: &'a BTreeSet<u32>,
    /// The state backend the job selected.
    pub job: StateBackendSelector,
}

/// One subtask's finished checkpoint, before it is merged into its operator's.
///
/// Carries the operator id the report arrived for alongside the report itself, so that the
/// check can compare the two rather than take the report's word for which operator it belongs
/// to.
#[derive(Debug, Clone)]
pub struct SubtaskReport {
    operator_id: String,
    metadata: SubtaskCheckpointMetadata,
}

impl SubtaskReport {
    /// The report a worker sent for `operator_id`, before it is checked.
    pub fn new(operator_id: String, metadata: SubtaskCheckpointMetadata) -> Self {
        Self {
            operator_id,
            metadata,
        }
    }

    /// The subtask this report is from, as the report's top-level field states it.
    ///
    /// After the check this is the *only* subtask identity the report contains: every table
    /// payload carries the same one, which is what [`Self::check_whole`] establishes.
    pub fn subtask_index(&self) -> u32 {
        self.metadata.subtask_index
    }

    /// The report itself, for the bytes, watermark and times the merge folds in.
    pub fn metadata(&self) -> &SubtaskCheckpointMetadata {
        &self.metadata
    }

    /// The tables of a *checked* report, each paired with the config that describes it.
    ///
    /// Takes the token rather than `self`, for the reason
    /// [`CheckpointCleanup::operators`](super::CheckpointCleanup::operators) does: the pairing
    /// is total only because [`Self::check_whole`] refused a report naming a table its own
    /// `table_configs` does not describe, so a pairing built from an unchecked report would
    /// drop that table silently instead. This is what replaced the
    /// `expect("should have metadata")` the merge used to run on a map key taken straight off
    /// the wire — a refusal in the check, not a panic at the merge, and not a quiet omission
    /// either.
    pub fn into_tables(
        validated: Validated<Self>,
    ) -> Vec<(String, TableConfig, TableSubtaskCheckpointMetadata)> {
        let SubtaskCheckpointMetadata {
            table_metadata,
            mut table_configs,
            ..
        } = validated.into_inner().metadata;

        table_metadata
            .into_iter()
            .filter_map(|(table, metadata)| {
                table_configs
                    .remove(&table)
                    .map(|config| (table, config, metadata))
            })
            .collect()
    }
}

impl WholeObject for SubtaskReport {
    type Context<'a> = ReportingOperator<'a>;

    /// The error type the merge site already handles.
    ///
    /// A plain `anyhow::Error`, and deliberately so: the job controller treats every refusal
    /// of a report the same way — the checkpoint does not complete, and the job fails and
    /// restarts from the last one that did — while a caller that wants the selector
    /// disagreement typed downcasts to
    /// [`StateBackendError`](arroyo_rpc::state_backend::StateBackendError), which is preserved
    /// through this boundary.
    type Error = anyhow::Error;

    /// Every identity in the report agrees with every other, the operator can still count it,
    /// and every table it names is described.
    ///
    /// In order, and each is a *relationship* rather than a field read in isolation:
    ///
    /// 1. **The table configs agree with the job's selector.** Merging a subtask that selects
    ///    a different backend would produce a checkpoint that is partly one backend's. This
    ///    also runs at the merge site, before this check, so that a report for an operator
    ///    this checkpoint does not have is still refused; running it here as well is what
    ///    makes this a complete statement about the value rather than one that leans on where
    ///    it happened to be called from.
    /// 2. **The report is for the operator it was delivered to.** The operator id is the key
    ///    the merge looks the operator's state up by, and the report is the thing being merged
    ///    into it.
    /// 3. **The subtask index is one the operator has.** The program says how many subtasks
    ///    the operator has; an index at or above that is a report for a different program or a
    ///    different execution, and counting it would complete the operator without one of its
    ///    actual subtasks.
    /// 4. **That subtask has not already reported.** No legitimate path re-reports: a subtask
    ///    sends `ControlResp::CheckpointCompleted` once per barrier, the worker's control loop
    ///    makes one unary call per message and cancels the worker rather than retrying it, and
    ///    the job controller drops any report whose epoch is not the one in flight. A second
    ///    report for an index that already reported is two writers, not a retry.
    /// 5. **Every table payload names the same subtask as the report.** The round-6 finding.
    ///    `TableSubtaskCheckpointMetadata.subtask_index` is the key the merge files the payload
    ///    under, so a payload labelled with another subtask's index overwrites that subtask's
    ///    contribution while the top-level accounting records a fresh, legitimate report.
    ///    Every legitimate report agrees here by construction — both indices are
    ///    `TaskInfo::task_index`, stamped a few lines apart in `TableManager::checkpoint` —
    ///    so this refuses nothing a running job produces.
    /// 6. **Every table the report names is described by its own configs.** A payload with no
    ///    config leaves nothing that can say how to read its files, and the merge used to
    ///    `expect` its way past that on caller-supplied data.
    ///
    /// # Errors
    ///
    /// A plain `anyhow` error naming the operator, the subtask and — where the failure is
    /// about one — the table. The one exception is (1), which is carried through as the typed
    /// [`StateBackendError`](arroyo_rpc::state_backend::StateBackendError) its callers already
    /// downcast to.
    fn check_whole(&self, operator: ReportingOperator<'_>) -> anyhow::Result<()> {
        let subtask_index = self.metadata.subtask_index;
        let operator_id = operator.operator_id;

        validate_subtask_table_configs(
            operator.job,
            operator_id,
            subtask_index,
            &self.metadata.table_configs,
        )
        .map_err(anyhow::Error::new)?;

        if self.operator_id != operator_id {
            anyhow::bail!(
                "a report for operator {} was delivered to operator {operator_id}",
                self.operator_id
            );
        }

        if subtask_index as usize >= operator.subtasks {
            anyhow::bail!(
                "operator {operator_id} has {} subtask(s), so subtask {subtask_index} is not \
                 one of them",
                operator.subtasks
            );
        }

        if operator.already_reported.contains(&subtask_index) {
            anyhow::bail!(
                "subtask {subtask_index} of operator {operator_id} has already reported this \
                 checkpoint"
            );
        }

        for (table, metadata) in &self.metadata.table_metadata {
            if metadata.subtask_index != subtask_index {
                anyhow::bail!(
                    "the report from subtask {subtask_index} of operator {operator_id} carries \
                     state for table {table} labelled subtask {}, which is a different \
                     subtask's state",
                    metadata.subtask_index
                );
            }
            if !self.metadata.table_configs.contains_key(table) {
                anyhow::bail!(
                    "the report from subtask {subtask_index} of operator {operator_id} carries \
                     state for table {table} but no config for it, so nothing can say how to \
                     read it"
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ReportingOperator, SubtaskReport};
    use arroyo_rpc::grpc::rpc::{
        SubtaskCheckpointMetadata, TableConfig, TableEnum, TableSubtaskCheckpointMetadata,
    };
    use arroyo_rpc::state_backend::validated::Validated;
    use arroyo_rpc::state_backend::{StateBackendError, StateBackendSelector};
    use std::collections::{BTreeSet, HashMap};

    /// A report from `subtask_index` carrying one table whose payload claims
    /// `table_subtask_index`.
    ///
    /// The two indices are separate parameters because that is the whole subject of this
    /// module: they are separate fields on the wire, and a test that could not vary one
    /// against the other could not see the defect.
    fn report(subtask_index: u32, table_subtask_index: u32) -> SubtaskReport {
        SubtaskReport::new(
            "node_1".to_string(),
            SubtaskCheckpointMetadata {
                subtask_index,
                bytes: 8,
                table_metadata: HashMap::from([(
                    "g".to_string(),
                    TableSubtaskCheckpointMetadata {
                        subtask_index: table_subtask_index,
                        table_type: TableEnum::GlobalKeyValue as i32,
                        data: vec![],
                    },
                )]),
                table_configs: HashMap::from([(
                    "g".to_string(),
                    TableConfig {
                        table_type: TableEnum::GlobalKeyValue as i32,
                        config: vec![],
                        state_version: 0,
                        state_backend: String::new(),
                    },
                )]),
                ..Default::default()
            },
        )
    }

    /// An operator of two subtasks, none of which has reported yet.
    fn two_subtasks(already_reported: &BTreeSet<u32>) -> ReportingOperator<'_> {
        ReportingOperator {
            operator_id: "node_1",
            subtasks: 2,
            already_reported,
            job: StateBackendSelector::Parquet,
        }
    }

    /// A report's table payloads must name the subtask the report is from (PR #160 review
    /// round 6, finding 1).
    ///
    /// The finding, exactly: the report carries its subtask index twice and only the top-level
    /// copy was ever looked at. At parallelism 2, reports whose top-level indices are `{0, 1}`
    /// complete the operator; if both reports' table payloads say `0`, the merge files both
    /// under key `0` and the second overwrites the first, so the checkpoint certifies state
    /// subtask 1 never contributed.
    ///
    /// The two indices are varied independently, which is the round-6 standard: the first case
    /// holds the top-level index at 1 — an index the operator has, not yet reported, admitted
    /// by every check round 5 added — and moves only the nested one.
    #[test]
    fn a_table_payload_labelled_with_another_subtask_is_refused() {
        let none = BTreeSet::new();

        let disagreeing = Validated::validate(report(1, 0), two_subtasks(&none)).unwrap_err();
        let message = disagreeing.to_string();
        assert!(message.contains("subtask 1"), "{message}");
        assert!(message.contains("node_1"), "{message}");
        assert!(message.contains("table g"), "{message}");
        assert!(message.contains("labelled subtask 0"), "{message}");

        // And the other direction, so this is agreement rather than an ordering of the two.
        let other_way = Validated::validate(report(0, 1), two_subtasks(&none)).unwrap_err();
        assert!(
            other_way.to_string().contains("labelled subtask 1"),
            "{other_way}"
        );

        // An index neither of them has: not "the report's index or the operator's", but the
        // report's.
        let foreign = Validated::validate(report(0, 7), two_subtasks(&none)).unwrap_err();
        assert!(
            foreign.to_string().contains("labelled subtask 7"),
            "{foreign}"
        );

        // The legitimate report — both copies agreeing, which is what `TableManager` produces
        // — passes. Both indices are `TaskInfo::task_index` there, so this is the whole of
        // real traffic.
        Validated::validate(report(1, 1), two_subtasks(&none))
            .expect("a report whose two subtask identities agree is the ordinary case");
        Validated::validate(report(0, 0), two_subtasks(&none)).expect("and so is subtask 0's");
    }

    /// A report naming a table it carries no config for is refused rather than panicking
    /// (PR #160 review round 6, finding 1).
    ///
    /// The merge used to reach `table_configs.get(key).expect("should have metadata")` on a key
    /// taken straight out of a message from a worker, so a report naming a table it did not
    /// describe panicked the job controller. Legitimate reports never do — `table_metadata` is
    /// built from the tables that produced data and `table_configs` is every declared table, so
    /// the first is a subset of the second — which is why this is a refusal and not a
    /// tolerated shape.
    #[test]
    fn a_report_naming_a_table_it_does_not_describe_is_refused() {
        let none = BTreeSet::new();
        let mut undescribed = report(0, 0);
        undescribed.metadata.table_configs.clear();

        let err = Validated::validate(undescribed, two_subtasks(&none)).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("table g"), "{message}");
        assert!(message.contains("no config for it"), "{message}");
        assert!(message.contains("subtask 0"), "{message}");
    }

    /// The three rules round 5 installed at the merge site are the same rules, now inside the
    /// token (PR #160 review rounds 5 and 6).
    ///
    /// Round 5's `check_reportable` refused an out-of-range index and a duplicate. Moving them
    /// into the whole-object check is a strengthening — the merge can no longer be reached
    /// without them — and the messages are unchanged, which is what lets the round-5 rows in
    /// `checkpoint_state.rs` keep asserting on them.
    #[test]
    fn an_index_the_operator_does_not_have_or_has_already_heard_is_refused() {
        let none = BTreeSet::new();
        let out_of_range = Validated::validate(report(5, 5), two_subtasks(&none)).unwrap_err();
        let message = out_of_range.to_string();
        assert!(message.contains("2 subtask(s)"), "{message}");
        assert!(message.contains("subtask 5"), "{message}");

        let reported = BTreeSet::from([0]);
        let duplicate = Validated::validate(report(0, 0), two_subtasks(&reported)).unwrap_err();
        assert!(
            duplicate.to_string().contains("already reported"),
            "{duplicate}"
        );

        // The subtask that has not reported is still admitted, so the guard is a set membership
        // and not a latch.
        Validated::validate(report(1, 1), two_subtasks(&reported))
            .expect("the operator has not heard from subtask 1");
    }

    /// A report is for the operator it was delivered to, and the selector failure stays typed.
    ///
    /// The operator half is the outermost identity of the four the report is checked against
    /// — job (through the selector), operator, subtask, table-subtask — and it was previously
    /// implicit in the caller having looked the operator up by the report's own id.
    #[test]
    fn a_report_is_checked_against_the_operator_it_was_delivered_to() {
        let none = BTreeSet::new();
        let elsewhere = ReportingOperator {
            operator_id: "node_2",
            ..two_subtasks(&none)
        };
        let err = Validated::validate(report(0, 0), elsewhere).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("node_1"), "{message}");
        assert!(message.contains("node_2"), "{message}");

        // The selector disagreement is still the typed error its callers downcast to, which is
        // what keeps the M11.T08 rows at the merge site green.
        let stateengine = ReportingOperator {
            job: StateBackendSelector::StateEngine,
            ..two_subtasks(&none)
        };
        let typed = Validated::validate(report(0, 0), stateengine).unwrap_err();
        assert!(
            matches!(
                typed.downcast_ref::<StateBackendError>(),
                Some(StateBackendError::TableMismatch { .. })
            ),
            "{typed:?}"
        );
    }

    /// The pairing a checked report hands out is total, and it is the pairing the merge uses.
    #[test]
    fn a_checked_report_pairs_every_table_with_its_config() {
        let none = BTreeSet::new();
        let checked = Validated::validate(report(1, 1), two_subtasks(&none)).unwrap();
        assert_eq!(checked.get().subtask_index(), 1);

        let tables = SubtaskReport::into_tables(checked);
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].0, "g");
        assert_eq!(tables[0].2.subtask_index, 1);
    }
}
