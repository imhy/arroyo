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

    /// The first write of a checkpoint this process just took, whose operators are the ones
    /// that reported completion — each of them having had its subtask table configs checked
    /// as it did (`validate_subtask_table_configs`).
    pub fn for_completed_checkpoint(
        metadata: CheckpointMetadata,
        completed_operators: Vec<String>,
    ) -> Self {
        Self {
            metadata,
            validated_operators: completed_operators,
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
