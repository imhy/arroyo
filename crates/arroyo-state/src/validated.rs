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
//! # The identity matrix
//!
//! A whole-object check is not only about *shape* — that every operator is present, that every
//! epoch of the range was collected. It is also about *identity*: which checkpoint of which job
//! the object is, and whether every part of it agrees about that. Review rounds 4, 5 and 6 of
//! PR #160 each found one identity relationship unstated, so this module now states all of them
//! in one place, and [`identity`] holds the vocabulary they share.
//!
//! | Boundary | Identities that must agree | Evidence | Effect derived from |
//! |---|---|---|---|
//! | [`SubtaskReport`] | job (selector), operator, subtask, every table payload's subtask | the report | the token's own pairing |
//! | [`CompletedCheckpoint`] | job, epoch, operator set, every subtask of every operator | what the checkpoint heard | the token |
//! | [`RestorableCheckpoint`] | job, epoch, and every persisted operator header's job/operator/epoch | the loaded objects | the token |
//! | [`CheckpointCleanup`] | job, the retained epoch, **each** dropped epoch, and every header | the collected range | the token |
//! | [`CheckpointMetadataWrite`] | the exact checkpoint identity, and the operator set | whichever family above entitles it | the token |
//!
//! Two of those rows must admit a legitimate difference rather than demand equality, and say so
//! rather than leaving it to be inferred. A cleanup spans epochs by construction — one retained
//! and a range of dropped ones — so its rule is "each object carries *the epoch it was collected
//! at*", not "every object carries one epoch". And a report legitimately omits a table it had no
//! data for, so nothing requires a table to appear in every subtask's report.
//!
//! Four families live beside their owners in child modules rather than here, and for the same
//! reason: each is self-contained, and each would otherwise push this file past the 500-line
//! production boundary M11.T25's plan sets. [`cleanup`] carries the whole object a checkpoint
//! cleanup acts on together with the views its token hands out, which is what keeps those
//! views constructor-free. [`completion`] carries [`CompletedCheckpoint`], the evidence one of
//! the objects here is derived *from*: a checkpoint this process just took has no earlier
//! token to build on — nothing was restored, compacted or cleaned up — so what entitles its
//! first metadata write is the completion the checkpoint's own bookkeeping recorded, and that
//! has to be a checked value rather than a list the writer passes itself. [`subtask`] carries
//! the report that completion is added up from, and [`identity`] the identity vocabulary every
//! row of the table above is written in.
//!
//! The views handed out from a token — [`ValidatedOperatorCleanup`] and [`ValidatedTable`]
//! — exist for the same reason one level down: `files_to_keep` decides which files a
//! cleanup will *not* delete, so it must not be reachable from a table config and a
//! checkpoint metadata that nothing validated. They have no public constructor; borrowing
//! one out of a token is the only way to obtain one. They and the cleanup they come from
//! live in [`cleanup`], which is what keeps "no public constructor" true.

pub mod cleanup;
pub mod completion;
pub mod identity;
pub mod subtask;

pub use cleanup::{
    CheckpointCleanup, CleanupScope, OperatorCleanup, ValidatedOperatorCleanup, ValidatedTable,
};
pub use completion::{CompletedCheckpoint, CompletedOperator};
pub use identity::{CheckpointIdentity, OperatorObject, check_operator_header};
pub use subtask::{ReportingOperator, SubtaskReport};

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
    /// The checkpoint the caller asked storage for.
    ///
    /// Every persisted operator header in the object being checked has to carry this job, this
    /// epoch, and the operator the object was read for. Without it the header was read for its
    /// operator id alone, which is the round-6 finding: an object headed with another job's or
    /// another epoch's identity, stored under the path the caller reads, passed whole-object
    /// validation and then redirected the writes compaction derives from that header.
    pub expected: &'a CheckpointIdentity,
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
    checkpoint: CheckpointIdentity,
    operators: Vec<(String, OperatorCheckpointMetadata)>,
}

impl RestorableCheckpoint {
    /// Collects one checkpoint's loaded operator metadata, before it is checked.
    ///
    /// `checkpoint` is the checkpoint the objects were read for — the job whose prefix they
    /// live under and the epoch within it — and `operators` must be everything the operation
    /// will act on, in the order the caller will act in; the check is over exactly this list.
    ///
    /// Recording the identity in the value rather than only in the check's context is what
    /// lets it travel: the metadata rewrite that follows a restore is entitled by this token,
    /// and what it needs to know is which checkpoint was validated, not which one the caller
    /// says it was.
    pub fn new(
        checkpoint: CheckpointIdentity,
        operators: Vec<(String, OperatorCheckpointMetadata)>,
    ) -> Self {
        Self {
            checkpoint,
            operators,
        }
    }

    /// The checkpoint these objects were read for.
    pub fn checkpoint(&self) -> &CheckpointIdentity {
        &self.checkpoint
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

    /// The checkpoint the caller asked for, exact program coverage, a persisted header on
    /// every object carrying **that** job, that epoch and the operator it was loaded for, and
    /// every table config agreeing with the job's selector.
    ///
    /// The header is not decoration and it is not description. `TableManager::load` reads the
    /// restored watermark straight out of it — with an `unwrap` — so an object without one
    /// panics a worker, which is past every effect this check runs in front of. And its
    /// `job_id` and `epoch` are what expiring-table compaction builds the path of every file it
    /// writes from, so a header naming another checkpoint redirects those writes. Until review
    /// round 6 of PR #160 only `operator_id` was read; the other two fields were treated as
    /// description of an object whose provenance the caller was trusted to know.
    ///
    /// The first comparison — the identity the value was built with against the identity the
    /// context declares — is the weaker of the two, because one caller supplies both. It is
    /// here so that a future constructor cannot collect one checkpoint's objects and declare
    /// another, which is exactly the drift that produced the `completed: None` entitlements.
    /// The per-object comparisons are the load-bearing ones: those compare a caller's question
    /// against what storage actually returned.
    fn check_whole(&self, program: RestoringProgram<'_>) -> Result<(), StateError> {
        let epoch = self.checkpoint.epoch();
        let incomplete = |detail: String| StateError::IncompleteCheckpoint { epoch, detail };

        program.expected.check_matches(
            "the checkpoint these objects were collected for",
            &self.checkpoint,
            incomplete,
        )?;

        check_program_coverage(
            epoch,
            self.operators.iter().map(|(id, _)| id.as_str()),
            program.restoring,
        )?;

        for (operator_id, metadata) in &self.operators {
            check_operator_header(self.checkpoint.operator(operator_id), metadata, incomplete)?;
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
    /// The checkpoint this entitlement is evidence *of*. Never absent.
    ///
    /// Review round 5 of PR #160 gave this to one of the three constructors and left the
    /// other two saying `None`, on the grounds that a restore preflight and a cleanup derive
    /// from objects that "already exist under the checkpoint being rewritten". Round 6's
    /// diagnosis is that this was provenance mistaken for validation: it describes how
    /// today's callers obtain those objects, and a type that skips its check whenever a field
    /// is `None` enforces nothing — the next constructor added is one `None` away from
    /// entitling any checkpoint whose operator set happens to line up, which every epoch of
    /// one job's does.
    ///
    /// So every constructor supplies it, the `Option` is gone, and
    /// [`Self::check_whole`] has no branch in which the comparison does not run. Each family
    /// takes it from the token it derives from — the preflight's checkpoint, the cleanup's
    /// checkpoint, the completion's — so the identity travels with the evidence rather than
    /// being re-declared by the writer.
    entitled: CheckpointIdentity,
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
            entitled: preflight.get().checkpoint().clone(),
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
            entitled: cleanup.get().checkpoint().clone(),
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
    ///
    /// It also records *which* checkpoint the evidence is of. Review round 5 of PR #160 found
    /// that the evidence and the metadata were only ever compared by operator set, so a
    /// completion of job A epoch 4 entitled the metadata of job B epoch 5 whenever both named
    /// the same operators — which two epochs of one job always do, and two jobs running the
    /// same pipeline usually do. The plan this implements requires validation to cover *every
    /// epoch* before the first effect (M11.T25d), and an epoch nothing compares is not
    /// covered. [`Self::check_whole`] compares both halves.
    pub fn for_completed_checkpoint(
        metadata: CheckpointMetadata,
        completed: &Validated<CompletedCheckpoint>,
    ) -> Self {
        Self {
            metadata,
            validated_operators: completed.get().operator_ids().map(str::to_string).collect(),
            entitled: completed.get().checkpoint().clone(),
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

    /// The metadata is the checkpoint the entitlement is evidence of, and it names exactly the
    /// operators that were validated, once each.
    ///
    /// The identity half runs first because it is the coarser claim: an entitlement for a
    /// different checkpoint is wrong however well its operator set lines up, and operator sets
    /// line up between checkpoints far more often than not — every epoch of one job has the
    /// same operators as every other. Since review round 6 of PR #160 it runs for every
    /// entitlement rather than for the one family that happened to carry an identity; see the
    /// [`entitled`](Self) field.
    fn check_whole(&self, _context: ()) -> Result<(), StateError> {
        let written = CheckpointIdentity::claimed_by(&self.metadata);
        written.check_matches("the entitlement", &self.entitled, |error| {
            StateError::Other {
                table: String::new(),
                error,
            }
        })?;

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
    use super::{
        CheckpointIdentity, CheckpointMetadataWrite, CompletedCheckpoint, CompletedOperator,
    };
    use arroyo_rpc::errors::StateError;
    use arroyo_rpc::grpc::rpc::CheckpointMetadata;
    use arroyo_rpc::state_backend::validated::Validated;
    use std::collections::HashSet;

    /// A checkpoint's top-level metadata naming `operator_ids`, for job `job_1` epoch 4.
    fn metadata(operator_ids: &[&str]) -> CheckpointMetadata {
        metadata_of("job_1", 4, operator_ids)
    }

    /// A checkpoint's top-level metadata, with its identity spelled out — for the rows about
    /// which checkpoint an entitlement is evidence of.
    fn metadata_of(job_id: &str, epoch: u32, operator_ids: &[&str]) -> CheckpointMetadata {
        CheckpointMetadata {
            job_id: job_id.to_string(),
            epoch,
            min_epoch: 0,
            start_time: 0,
            finish_time: 0,
            operator_ids: operator_ids.iter().map(|id| id.to_string()).collect(),
        }
    }

    /// One operator's report, with every subtask in.
    fn finished(operator_id: &str, subtasks: usize) -> CompletedOperator {
        CompletedOperator::reported(operator_id.to_string(), subtasks, 0..subtasks as u32)
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
                "job_1".to_string(),
                4,
                vec![
                    finished("node_1", 1),
                    CompletedOperator::reported("node_2".to_string(), 2, [0]),
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
            CompletedCheckpoint::new("job_1".to_string(), 4, vec![finished("node_1", 1)]),
            &program,
        )
        .unwrap_err();
        assert!(dropped.to_string().contains("node_2"), "{dropped}");

        // Nor by an operator that reported nothing because it has no subtasks: 0 of 0 is not
        // a completed checkpoint, it is an operator nothing was heard from.
        let empty = Validated::validate(
            CompletedCheckpoint::new(
                "job_1".to_string(),
                4,
                vec![
                    finished("node_1", 1),
                    CompletedOperator::reported("node_2".to_string(), 0, []),
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
                "job_1".to_string(),
                4,
                vec![
                    CompletedOperator::reported("node_1".to_string(), 1, []),
                    CompletedOperator::reported("node_2".to_string(), 1, []),
                ],
            ),
            &self_supplied,
        )
        .unwrap_err();
        assert!(forged.to_string().contains("node_1 (0/1"), "{forged}");

        // A checkpoint every operator did finish entitles its own metadata...
        let completed = Validated::validate(
            CompletedCheckpoint::new(
                "job_1".to_string(),
                4,
                vec![finished("node_1", 1), finished("node_2", 2)],
            ),
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

    /// A completion entitles the checkpoint it is a completion *of*, and no other (PR #160
    /// review round 5, finding 2).
    ///
    /// The finding, exactly: `CompletedCheckpoint` recorded an epoch that only ever reached an
    /// error message, recorded no job at all, and `for_completed_checkpoint` copied nothing but
    /// operator ids across — so the token's whole check was a comparison of two operator sets.
    /// Two epochs of one job always name the same operators, and two jobs running the same
    /// pipeline usually do, so that comparison agreed for exactly the pairs it needed to
    /// refuse. Review round 4 disclosed the missing epoch binding as a residual for M11.T26;
    /// this row is it being closed instead.
    ///
    /// The evidence below is held fixed and only the metadata's identity moves, so what
    /// changes the answer is the identity and nothing else — the operator set is the same
    /// `{node_1, node_2}` in every case, which is the situation the finding describes.
    #[test]
    fn completion_evidence_entitles_only_the_checkpoint_it_is_evidence_of() {
        let program: HashSet<&str> = HashSet::from(["node_1", "node_2"]);
        let completed = Validated::validate(
            CompletedCheckpoint::new(
                "job_a".to_string(),
                4,
                vec![finished("node_1", 1), finished("node_2", 2)],
            ),
            &program,
        )
        .unwrap();

        // Its own checkpoint: entitled.
        Validated::validate(
            CheckpointMetadataWrite::for_completed_checkpoint(
                metadata_of("job_a", 4, &["node_1", "node_2"]),
                &completed,
            ),
            (),
        )
        .expect("the checkpoint the evidence is of is the one it entitles");

        // Another job's, with an operator set that matches exactly.
        let other_job = Validated::validate(
            CheckpointMetadataWrite::for_completed_checkpoint(
                metadata_of("job_b", 4, &["node_1", "node_2"]),
                &completed,
            ),
            (),
        )
        .unwrap_err();
        let message = other_job.to_string();
        assert!(message.contains("job_b"), "{message}");
        assert!(message.contains("job_a"), "{message}");
        assert!(message.contains("different checkpoint"), "{message}");

        // A later epoch of the same job — the pair the finding names, and the one an operator
        // set can never tell apart.
        let other_epoch = Validated::validate(
            CheckpointMetadataWrite::for_completed_checkpoint(
                metadata_of("job_a", 5, &["node_1", "node_2"]),
                &completed,
            ),
            (),
        )
        .unwrap_err();
        let message = other_epoch.to_string();
        assert!(message.contains("epoch 5"), "{message}");
        assert!(message.contains("epoch 4"), "{message}");

        // An earlier epoch too: the binding is equality, not a floor a replay could clear.
        let earlier_epoch = Validated::validate(
            CheckpointMetadataWrite::for_completed_checkpoint(
                metadata_of("job_a", 3, &["node_1", "node_2"]),
                &completed,
            ),
            (),
        )
        .unwrap_err();
        assert!(
            earlier_epoch.to_string().contains("different checkpoint"),
            "{earlier_epoch}"
        );

        // The other two entitlements now carry an identity too, and there is no longer a shape
        // of this struct that has none: `entitled` is not an `Option`. Review round 5 left the
        // restore and cleanup constructors saying `None`, and what stood here was a
        // hand-assembled `cleanup_shaped` value asserting that an entitlement without an
        // identity passed on its operator set alone. Round 6's diagnosis is that this was the
        // defect rather than the scope: a check that a future constructor can skip by leaving
        // a field empty is not a check. The rows for the other two families are in
        // `parquet.rs`, where their tokens can be derived rather than fabricated.
        let cleanup_shaped = CheckpointMetadataWrite {
            metadata: metadata_of("job_a", 4, &["node_1", "node_2"]),
            validated_operators: vec!["node_1".to_string(), "node_2".to_string()],
            entitled: CheckpointIdentity::new("job_a", 5),
        };
        let wrong_epoch = Validated::validate(cleanup_shaped, ()).unwrap_err();
        assert!(
            wrong_epoch.to_string().contains("different checkpoint"),
            "{wrong_epoch}"
        );
    }
}
