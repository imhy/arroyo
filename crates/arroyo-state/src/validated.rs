//! Whole-object validation for the parquet backend's destructive and publishing operations
//! (design item M11.D39c).
//!
//! Every operation in this crate that deletes a file, rewrites an operator's state, or
//! rewrites a checkpoint's top-level metadata acts on a whole checkpoint: all of its
//! operators, and — for a cleanup — all of the epochs it is collapsing. Validating one
//! operator, acting on it, and moving to the next is what let PR-#157's rounds 1–4 delete
//! one operator's files before the next operator's disagreement was discovered.
//!
//! This module carries the check as a value. Each type here is the *whole* object one of
//! those operations needs, its [`WholeObject`] impl is the complete statement about it, and
//! the operations in [`crate::parquet`] take [`Validated<T>`] rather than the object. A
//! caller cannot reach an effect without the token, and the only way to a token is the
//! check — see [`arroyo_rpc::state_backend::validated`] for why it cannot be forged.
//!
//! Three of the types here are the whole objects those operations need; the fourth,
//! [`CompletedCheckpoint`], is the evidence one of them is derived *from*. A checkpoint this
//! process just took has no earlier token to build on — nothing was restored, compacted or
//! cleaned up — so what entitles its first metadata write is the completion the checkpoint's
//! own bookkeeping recorded, and that has to be a checked value rather than a list the writer
//! passes itself.
//!
//! The views handed out from a token — [`ValidatedOperatorCleanup`] and [`ValidatedTable`]
//! — exist for the same reason one level down: `files_to_keep` decides which files a
//! cleanup will *not* delete, so it must not be reachable from a table config and a
//! checkpoint metadata that nothing validated. They have no public constructor; borrowing
//! one out of a token is the only way to obtain one. They and the cleanup they come from
//! live in [`cleanup`], which is what keeps "no public constructor" true.

pub mod cleanup;

pub use cleanup::{
    CheckpointCleanup, CleanupScope, OperatorCleanup, ValidatedOperatorCleanup, ValidatedTable,
};

use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{CheckpointMetadata, OperatorCheckpointMetadata};
use arroyo_rpc::state_backend::validated::{Validated, WholeObject};
use arroyo_rpc::state_backend::{StateBackendSelector, validate_restored_operator_metadata};
use std::collections::{BTreeSet, HashSet};

/// What a checkpoint has to agree with before it may be restored, compacted, or rewritten.
///
/// `restoring` is the set of operator ids the job's workers will construct, derived from the
/// *current* program (`LogicalProgram::tasks_per_operator`) rather than from the checkpoint:
/// a worker builds every operator of its program and each one loads its own metadata
/// object, so the checkpoint's own list is not the set that matters.
#[derive(Debug, Clone, Copy)]
pub struct RestoringProgram<'a> {
    /// The state backend the job selected.
    pub job: StateBackendSelector,
    /// The operators the job's workers will build.
    pub restoring: &'a HashSet<&'a str>,
}

/// Checks that `listed` names exactly the operators in `restoring`, once each.
///
/// Exposed separately from [`RestorableCheckpoint`]'s own check because a caller can run it
/// on a checkpoint's operator list *before* reading any of the objects that list names: a
/// checkpoint that cannot cover the job's program should be refused before the preflight
/// pays for its own reads. [`RestorableCheckpoint::check_whole`] runs it again over what was
/// actually loaded, which is the copy the operations depend on.
///
/// # Errors
///
/// Returns [`StateError::IncompleteCheckpoint`] naming the operators the program contains
/// and the checkpoint does not, the operators the checkpoint names and the program does not
/// contain, or an operator named twice — in which case which of its objects would be
/// restored is not defined. Operators are listed sorted so the message is stable.
pub fn check_program_coverage<'a>(
    epoch: u32,
    listed: impl IntoIterator<Item = &'a str>,
    restoring: &HashSet<&str>,
) -> Result<(), StateError> {
    let incomplete = |detail: String| StateError::IncompleteCheckpoint { epoch, detail };

    let mut seen: HashSet<&str> = HashSet::new();
    for operator_id in listed {
        if !seen.insert(operator_id) {
            return Err(incomplete(format!(
                "operator {operator_id} is listed more than once, so which of its objects \
                 would be used is not defined"
            )));
        }
    }

    let mut missing: Vec<&str> = restoring.difference(&seen).copied().collect();
    if !missing.is_empty() {
        missing.sort_unstable();
        return Err(incomplete(format!(
            "the job's program contains operator(s) {} that the checkpoint does not list, \
             and every operator a worker builds loads its own metadata",
            missing.join(", ")
        )));
    }

    let mut extra: Vec<&str> = seen.difference(restoring).copied().collect();
    if !extra.is_empty() {
        extra.sort_unstable();
        return Err(incomplete(format!(
            "the checkpoint lists operator(s) {} that the job's program does not contain",
            extra.join(", ")
        )));
    }

    Ok(())
}

/// One checkpoint epoch's operator metadata objects, all of them, in the checkpoint's order.
///
/// This is the whole object behind three of the five families: restoring reads it, the
/// metadata rewrite that follows a restore is entitled by it, and compaction rewrites the
/// state it describes. Every operator that is listed is present *by construction* — the type
/// has nowhere to record an absent object — so "required objects present" is a property of
/// having built one at all rather than something a later check has to remember.
#[derive(Debug, Clone)]
pub struct RestorableCheckpoint {
    epoch: u32,
    operators: Vec<(String, OperatorCheckpointMetadata)>,
}

impl RestorableCheckpoint {
    /// Collects one checkpoint's loaded operator metadata, before it is checked.
    ///
    /// `operators` must be everything the operation will act on, in the order the caller
    /// will act in; the check is over exactly this list.
    pub fn new(epoch: u32, operators: Vec<(String, OperatorCheckpointMetadata)>) -> Self {
        Self { epoch, operators }
    }

    /// The operators, in the order they were collected.
    pub fn operators(&self) -> &[(String, OperatorCheckpointMetadata)] {
        &self.operators
    }

    /// Takes the operators out, for a caller that consumes them rather than reading again.
    pub fn into_operators(self) -> Vec<(String, OperatorCheckpointMetadata)> {
        self.operators
    }
}

impl WholeObject for RestorableCheckpoint {
    type Context<'a> = RestoringProgram<'a>;
    type Error = StateError;

    /// Exact program coverage, an operator header on every object naming the operator it was
    /// loaded for, and every table config agreeing with the job's selector.
    ///
    /// The header is not decoration: `TableManager::load` reads the restored watermark
    /// straight out of it, so an object without one — or one describing a different operator
    /// — panics a worker, which is past every effect this check runs in front of.
    fn check_whole(&self, program: RestoringProgram<'_>) -> Result<(), StateError> {
        let incomplete = |detail: String| StateError::IncompleteCheckpoint {
            epoch: self.epoch,
            detail,
        };

        check_program_coverage(
            self.epoch,
            self.operators.iter().map(|(id, _)| id.as_str()),
            program.restoring,
        )?;

        for (operator_id, metadata) in &self.operators {
            match metadata.operator_metadata.as_ref() {
                Some(header) if header.operator_id == *operator_id => {}
                Some(header) => {
                    return Err(incomplete(format!(
                        "the checkpoint metadata object for operator {operator_id} is headed \
                         \"{}\" instead",
                        header.operator_id
                    )));
                }
                None => {
                    return Err(incomplete(format!(
                        "the checkpoint metadata object for operator {operator_id} has no \
                         operator header, which the worker that builds it requires"
                    )));
                }
            }
            validate_restored_operator_metadata(program.job, metadata)?;
        }

        Ok(())
    }
}
/// What one operator reported while this process took a checkpoint.
///
/// `subtasks` is how many subtasks the job's program gives that operator, fixed when the
/// checkpoint was opened; `subtasks_checkpointed` is how many of them have since reported a
/// finished checkpoint. The two are recorded rather than reduced to a flag because "finished"
/// is the *comparison*, and a flag would be exactly the kind of constructor-set field
/// [`WholeObject`] warns about — as forgeable as whoever set it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedOperator {
    operator_id: String,
    subtasks: usize,
    subtasks_checkpointed: usize,
}

impl CompletedOperator {
    /// What the checkpoint has heard from `operator_id` so far.
    pub fn reported(operator_id: String, subtasks: usize, subtasks_checkpointed: usize) -> Self {
        Self {
            operator_id,
            subtasks,
            subtasks_checkpointed,
        }
    }

    /// The operator this is about.
    pub fn operator_id(&self) -> &str {
        &self.operator_id
    }

    /// Whether every subtask the program gives this operator has reported.
    fn finished(&self) -> bool {
        self.subtasks > 0 && self.subtasks_checkpointed == self.subtasks
    }
}

/// A checkpoint this process has just taken, and what each of the job's operators reported.
///
/// The whole object behind the *first* write of a checkpoint's top-level metadata — the one
/// case that has no earlier token to derive from, because nothing has been restored,
/// compacted, or cleaned up. Review round 4 of PR #160 found that gap being filled with a
/// caller-supplied `Vec<String>`: [`CheckpointMetadataWrite::for_completed_checkpoint`] took
/// the metadata and the "completed" list side by side, so copying `metadata.operator_ids` into
/// the second argument minted a token that had checked nothing. This type is what that
/// argument became.
///
/// Its check is the `done()` the worker used to run beside the write, decomposed and carried:
/// the metadata cannot name an operator this does not list, and this cannot list an operator
/// that has not finished. See [`Self::check_whole`].
#[derive(Debug, Clone)]
pub struct CompletedCheckpoint {
    epoch: u32,
    operators: Vec<CompletedOperator>,
}

impl CompletedCheckpoint {
    /// Everything the checkpoint has heard, at the moment its metadata is built.
    pub fn new(epoch: u32, operators: Vec<CompletedOperator>) -> Self {
        Self { epoch, operators }
    }

    /// The operators, in the order they were collected.
    pub fn operators(&self) -> &[CompletedOperator] {
        &self.operators
    }

    /// The operator ids, for the metadata that is entitled by them.
    pub fn operator_ids(&self) -> impl Iterator<Item = &str> {
        self.operators.iter().map(CompletedOperator::operator_id)
    }
}

impl WholeObject for CompletedCheckpoint {
    /// The operators the job's program contains — the authority on what "all of them" means.
    ///
    /// Borrowed from the caller for the same reason [`RestoringProgram`] borrows it: the check
    /// is about the program the checkpoint was taken of, and that set is not something the
    /// checkpoint's own bookkeeping may be asked to confirm about itself.
    type Context<'a> = &'a HashSet<&'a str>;
    type Error = StateError;

    /// Exact program coverage, and every operator finished.
    ///
    /// Both halves are needed and neither implies the other. Coverage alone is what the old
    /// caller-supplied list already had, and it says nothing about whether an operator did
    /// anything; completion alone would let a write name a set that was never the job's.
    ///
    /// The completion half is the load-bearing one, and it is the `CheckpointState::done()`
    /// check the worker used to run *beside* this write rather than carry into it: an operator
    /// is finished when every subtask its program gives it has reported a finished checkpoint,
    /// and an operator with no subtasks has reported nothing whatever its counters say.
    fn check_whole(&self, program: &HashSet<&str>) -> Result<(), StateError> {
        check_program_coverage(self.epoch, self.operator_ids(), program)?;

        let mut unfinished: Vec<String> = self
            .operators
            .iter()
            .filter(|operator| !operator.finished())
            .map(|operator| {
                format!(
                    "{} ({}/{} subtasks)",
                    operator.operator_id, operator.subtasks_checkpointed, operator.subtasks
                )
            })
            .collect();
        if !unfinished.is_empty() {
            unfinished.sort_unstable();
            return Err(StateError::IncompleteCheckpoint {
                epoch: self.epoch,
                detail: format!(
                    "operator(s) {} have not finished this checkpoint, so nothing has vouched \
                     for the state a write would publish for them",
                    unfinished.join(", ")
                ),
            });
        }

        Ok(())
    }
}

/// A checkpoint's top-level metadata together with the operator set that entitles it to be
/// written.
///
/// The rewrite is the point at which a checkpoint becomes the one a restart will read, so it
/// may not name an operator nothing has vouched for: PR-#157 round 2 found a `ready`
/// checkpoint reaching this write without any operator having been preflighted at all. The
/// token therefore carries both halves and the check ties them together.
#[derive(Debug, Clone)]
pub struct CheckpointMetadataWrite {
    metadata: CheckpointMetadata,
    validated_operators: Vec<String>,
}

impl CheckpointMetadataWrite {
    /// The rewrite that follows a restore preflight; the entitled operators are the ones the
    /// preflight loaded and checked.
    pub fn after_restore_preflight(
        metadata: CheckpointMetadata,
        preflight: &Validated<RestorableCheckpoint>,
    ) -> Self {
        Self {
            metadata,
            validated_operators: preflight
                .get()
                .operators()
                .iter()
                .map(|(operator_id, _)| operator_id.clone())
                .collect(),
        }
    }

    /// The `min_epoch` rewrite that ends a cleanup; the entitled operators are the ones
    /// whose epochs the cleanup checked.
    pub fn after_cleanup(
        metadata: CheckpointMetadata,
        cleanup: &Validated<CheckpointCleanup>,
    ) -> Self {
        Self {
            metadata,
            validated_operators: CheckpointCleanup::operators(cleanup)
                .map(|operator| operator.operator_id().to_string())
                .collect(),
        }
    }

    /// The first write of a checkpoint this process just took; the entitled operators are the
    /// ones the checkpoint's own bookkeeping says finished it — each of them having had its
    /// subtask table configs checked as it did (`validate_subtask_table_configs`).
    ///
    /// Takes the evidence rather than a list, for the reason the two constructors above do:
    /// what entitles this write is that something was *checked*, and only a
    /// [`Validated<CompletedCheckpoint>`] carries that. Until review round 4 of PR #160 this
    /// took a bare `Vec<String>` beside the metadata, which made the token's whole check —
    /// "the metadata names exactly the operators that were validated" — a comparison of two
    /// values the same caller supplied. `for_completed_checkpoint(md.clone(),
    /// md.operator_ids.clone())` was a valid write token that had validated nothing.
    pub fn for_completed_checkpoint(
        metadata: CheckpointMetadata,
        completed: &Validated<CompletedCheckpoint>,
    ) -> Self {
        Self {
            metadata,
            validated_operators: completed.get().operator_ids().map(str::to_string).collect(),
        }
    }

    /// Takes the metadata out, for the writer that encodes it.
    pub fn into_metadata(self) -> CheckpointMetadata {
        self.metadata
    }
}

impl WholeObject for CheckpointMetadataWrite {
    type Context<'a> = ();
    type Error = StateError;

    /// The metadata names exactly the operators that were validated, once each.
    fn check_whole(&self, _context: ()) -> Result<(), StateError> {
        let named: BTreeSet<&str> = self
            .metadata
            .operator_ids
            .iter()
            .map(String::as_str)
            .collect();
        if named.len() != self.metadata.operator_ids.len() {
            return Err(StateError::Other {
                table: String::new(),
                error: format!(
                    "the metadata for epoch {} names an operator more than once: {:?}",
                    self.metadata.epoch, self.metadata.operator_ids
                ),
            });
        }

        let validated: BTreeSet<&str> = self
            .validated_operators
            .iter()
            .map(String::as_str)
            .collect();
        if named != validated {
            return Err(StateError::Other {
                table: String::new(),
                error: format!(
                    "the metadata for epoch {} names operators {named:?}, but {validated:?} \
                     were validated",
                    self.metadata.epoch
                ),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{CheckpointMetadataWrite, CompletedCheckpoint, CompletedOperator};
    use arroyo_rpc::errors::StateError;
    use arroyo_rpc::grpc::rpc::CheckpointMetadata;
    use arroyo_rpc::state_backend::validated::Validated;
    use std::collections::HashSet;

    /// A checkpoint's top-level metadata naming `operator_ids`.
    fn metadata(operator_ids: &[&str]) -> CheckpointMetadata {
        CheckpointMetadata {
            job_id: "job_1".to_string(),
            epoch: 4,
            min_epoch: 0,
            start_time: 0,
            finish_time: 0,
            operator_ids: operator_ids.iter().map(|id| id.to_string()).collect(),
        }
    }

    /// One operator's report, with every subtask in.
    fn finished(operator_id: &str, subtasks: usize) -> CompletedOperator {
        CompletedOperator::reported(operator_id.to_string(), subtasks, subtasks)
    }

    /// The write of a checkpoint this process just took needs evidence that it was taken
    /// (PR #160 review round 4, finding 2).
    ///
    /// The finding, exactly: `for_completed_checkpoint` took the metadata and a
    /// caller-supplied `Vec<String>`, and `check_whole` compared only those two. So
    /// `for_completed_checkpoint(md.clone(), md.operator_ids.clone())` minted a valid write
    /// token for metadata that no operator had completed or validated — the check was a
    /// comparison of one value with itself. That spelling no longer type-checks: the second
    /// argument is a `Validated<CompletedCheckpoint>`, and the only way to one is this check.
    ///
    /// Coverage was never the half that was missing, which is why the assertions below start
    /// with an unfinished operator and only then move on to a mis-named one.
    #[test]
    fn a_metadata_write_token_needs_evidence_that_every_operator_finished() {
        let program: HashSet<&str> = HashSet::from(["node_1", "node_2"]);

        // An operator that has reported some of its subtasks has finished nothing there is
        // anything to publish for, and there is no token for it.
        let unfinished = Validated::validate(
            CompletedCheckpoint::new(
                4,
                vec![
                    finished("node_1", 1),
                    CompletedOperator::reported("node_2".to_string(), 2, 1),
                ],
            ),
            &program,
        )
        .unwrap_err();
        assert!(
            matches!(
                unfinished,
                StateError::IncompleteCheckpoint { epoch: 4, .. }
            ),
            "{unfinished:?}"
        );
        assert!(
            unfinished.to_string().contains("node_2 (1/2"),
            "{unfinished}"
        );

        // Nor is one obtained by leaving it out: the evidence then does not cover the job's
        // program, which is the half that was already there.
        let dropped = Validated::validate(
            CompletedCheckpoint::new(4, vec![finished("node_1", 1)]),
            &program,
        )
        .unwrap_err();
        assert!(dropped.to_string().contains("node_2"), "{dropped}");

        // Nor by an operator that reported nothing because it has no subtasks: 0 of 0 is not
        // a completed checkpoint, it is an operator nothing was heard from.
        let empty = Validated::validate(
            CompletedCheckpoint::new(
                4,
                vec![
                    finished("node_1", 1),
                    CompletedOperator::reported("node_2".to_string(), 0, 0),
                ],
            ),
            &program,
        )
        .unwrap_err();
        assert!(empty.to_string().contains("node_2 (0/0"), "{empty}");

        // And the forgery the finding names, run against the current API: supplying the
        // metadata's own operator list as the program it is checked against buys nothing,
        // because what the caller cannot supply is the completion.
        let named = metadata(&["node_1", "node_2"]);
        let self_supplied: HashSet<&str> = named.operator_ids.iter().map(String::as_str).collect();
        let forged = Validated::validate(
            CompletedCheckpoint::new(
                4,
                vec![
                    CompletedOperator::reported("node_1".to_string(), 1, 0),
                    CompletedOperator::reported("node_2".to_string(), 1, 0),
                ],
            ),
            &self_supplied,
        )
        .unwrap_err();
        assert!(forged.to_string().contains("node_1 (0/1"), "{forged}");

        // A checkpoint every operator did finish entitles its own metadata...
        let completed = Validated::validate(
            CompletedCheckpoint::new(4, vec![finished("node_1", 1), finished("node_2", 2)]),
            &program,
        )
        .unwrap();
        Validated::validate(
            CheckpointMetadataWrite::for_completed_checkpoint(named, &completed),
            (),
        )
        .expect("metadata naming exactly the operators that finished is writable");

        // ...and nothing else. Metadata naming an operator the evidence does not cover is
        // refused, which is where an absent operator is caught...
        let extra = Validated::validate(
            CheckpointMetadataWrite::for_completed_checkpoint(
                metadata(&["node_1", "node_2", "node_3"]),
                &completed,
            ),
            (),
        )
        .unwrap_err();
        assert!(extra.to_string().contains("node_3"), "{extra}");

        // ...and so is metadata that quietly drops one the evidence does cover.
        let short = Validated::validate(
            CheckpointMetadataWrite::for_completed_checkpoint(metadata(&["node_1"]), &completed),
            (),
        )
        .unwrap_err();
        assert!(short.to_string().contains("node_2"), "{short}");
    }
}
