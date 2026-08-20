//! The evidence that a checkpoint this process took was actually completed, and the identity
//! of the checkpoint it is evidence *of*.
//!
//! Split out of [`crate::validated`] for the reason [`super::cleanup`] is: this is a
//! self-contained family — the per-operator report, the whole-checkpoint completion it adds
//! up to, and the identity that completion entitles — and keeping it here is what holds
//! [`crate::validated`] under the 500-line production boundary M11.T25's plan sets for every
//! new module.
//!
//! # What this family is for
//!
//! Three of the four whole objects in [`crate::validated`] describe a checkpoint that already
//! exists: a restore reads it, a compaction rewrites it, a cleanup collapses it. The *first*
//! write of a checkpoint's top-level metadata has no such object to check, because the
//! checkpoint is the one this process is taking right now. What entitles that write is the
//! completion its own bookkeeping observed, and this module is the checked form of it.
//!
//! # What review round 5 of PR #160 changed here
//!
//! Round 4 introduced [`CompletedOperator`] carrying two numbers — how many subtasks the
//! program gives the operator, and how many had "reported" — and treated their equality as
//! proof of completion. Round 5's finding 1 is that neither the counter that produced the
//! second number nor the type that carried it could tell one subtask reporting twice from two
//! subtasks reporting once, so at parallelism 2 two reports from subtask 0 made `2 == 2` and
//! minted a token while subtask 1 had never been heard from. A count is exactly the
//! "constructor-set field" [`WholeObject`] warns about: whoever supplies it decides what it
//! says.
//!
//! So the evidence is no longer a count. [`CompletedOperator`] records the distinct subtask
//! *indices* that reported, and [`CompletedOperator::finished`] holds only when those indices
//! are exactly the program's — `0..subtasks`, each once. A duplicate collapses in the set and
//! moves nothing; an index the operator does not have cannot substitute for one it does.
//!
//! Round 5's finding 2 is that the completion recorded an epoch but no job, and that neither
//! was compared against the metadata the token entitled — so a completion of job A epoch 4
//! could authorize the metadata of job B epoch 5 whenever their operator sets matched.
//! [`CompletedCheckpoint`] now carries both halves of the checkpoint's identity and hands
//! them out as a [`CheckpointIdentity`], which
//! [`CheckpointMetadataWrite`](super::CheckpointMetadataWrite) checks against the metadata it
//! is being asked to entitle.
//!
//! # What review round 6 changed here
//!
//! Two things, neither of them in this file's own logic. The identity type is now
//! [`CheckpointIdentity`], shared with every other family rather than private to this one, so
//! that "which checkpoint is this about?" has one spelling across the matrix in
//! [`crate::validated`]. And the *subtask* half of the evidence got a boundary of its own:
//! [`CompletedOperator`] records the top-level index each report carried, and until round 6
//! nothing checked that a report's table payloads carried the same index — so the indices
//! `{0, 1}` could be recorded here while both reports' table state was filed under subtask 0.
//! That relationship is now checked where the report is merged, by
//! [`SubtaskReport`](super::SubtaskReport).

use arroyo_rpc::errors::StateError;
use arroyo_rpc::state_backend::validated::WholeObject;
use std::collections::{BTreeSet, HashSet};

use super::check_program_coverage;
use super::identity::CheckpointIdentity;

/// What one operator reported while this process took a checkpoint.
///
/// `subtasks` is how many subtasks the job's program gives that operator, fixed when the
/// checkpoint was opened; `reported` is which of them have since reported a finished
/// checkpoint, by index. Both are recorded rather than reduced to a flag because "finished"
/// is the *comparison*, and a flag would be exactly the kind of constructor-set field
/// [`WholeObject`] warns about — as forgeable as whoever set it.
///
/// The second half is a set of indices rather than a count for the same reason one level
/// down. A count says how many messages arrived; the claim the write token rests on is that
/// every subtask of the operator was heard from, and those are different statements the
/// moment one subtask reports twice. See the [module docs](self).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedOperator {
    operator_id: String,
    subtasks: usize,
    reported: BTreeSet<u32>,
}

impl CompletedOperator {
    /// What the checkpoint has heard from `operator_id` so far: the subtask indices that
    /// reported, deduplicated by the set they are collected into.
    ///
    /// Passing an index twice is therefore indistinguishable from passing it once, which is
    /// the point — a caller cannot advance this operator towards completion by repeating
    /// itself, only by naming a subtask that has not been named. Nothing is rejected here:
    /// an index the operator does not have is *recorded* and then refused by
    /// [`Self::finished`], so the whole-object check is where the complete statement lives
    /// rather than split between a constructor and a check.
    pub fn reported(
        operator_id: String,
        subtasks: usize,
        reported: impl IntoIterator<Item = u32>,
    ) -> Self {
        Self {
            operator_id,
            subtasks,
            reported: reported.into_iter().collect(),
        }
    }

    /// The operator this is about.
    pub fn operator_id(&self) -> &str {
        &self.operator_id
    }

    /// Whether every subtask the program gives this operator has reported, and nothing else
    /// has.
    ///
    /// The comparison is against the whole index range rather than against a count:
    /// [`BTreeSet`] iterates in ascending order, so this holds exactly when the reported
    /// indices are `0, 1, .., subtasks - 1`. That rules out the two ways a count could be
    /// satisfied without the operator having finished — one subtask reporting `subtasks`
    /// times, and a report carrying an index the operator does not have — in one statement
    /// rather than in two checks that could drift apart.
    ///
    /// An operator with no subtasks is never finished: it has reported nothing, whatever the
    /// arithmetic of an empty set against an empty range would otherwise say.
    fn finished(&self) -> bool {
        self.subtasks > 0 && self.reported.iter().copied().eq(0..self.subtasks as u32)
    }

    /// Why this operator is not finished, for the error a refused checkpoint reports.
    ///
    /// Opens with `id (reported/subtasks subtasks` so that the count a reader expects is
    /// still there, and then says which indices are the problem — a checkpoint stuck on one
    /// missing subtask and one stuck on a report from an index nobody assigned are different
    /// operational situations, and the message is the only place they can be told apart.
    fn unfinished_detail(&self) -> String {
        let missing: Vec<String> = (0..self.subtasks as u32)
            .filter(|index| !self.reported.contains(index))
            .map(|index| index.to_string())
            .collect();
        let foreign: Vec<String> = self
            .reported
            .iter()
            .filter(|index| **index as usize >= self.subtasks)
            .map(|index| index.to_string())
            .collect();

        let mut detail = format!(
            "{} ({}/{} subtasks",
            self.operator_id,
            self.reported.len(),
            self.subtasks
        );
        if !missing.is_empty() {
            detail.push_str(&format!(
                "; subtask(s) {} did not report",
                missing.join(", ")
            ));
        }
        if !foreign.is_empty() {
            detail.push_str(&format!(
                "; index(es) {} are not subtasks of an operator the program gives {}",
                foreign.join(", "),
                self.subtasks
            ));
        }
        detail.push(')');
        detail
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
///
/// [`CheckpointMetadataWrite::for_completed_checkpoint`]: super::CheckpointMetadataWrite::for_completed_checkpoint
#[derive(Debug, Clone)]
pub struct CompletedCheckpoint {
    checkpoint: CheckpointIdentity,
    operators: Vec<CompletedOperator>,
}

impl CompletedCheckpoint {
    /// Everything the checkpoint has heard, at the moment its metadata is built, together
    /// with which checkpoint that is.
    ///
    /// `job_id` and `epoch` are the identity the evidence entitles, and the caller is the
    /// bookkeeping that observed the completion — so they are the job and epoch it was
    /// accumulating for, not a description of the metadata object about to be written. The
    /// two being separately supplied is what makes the comparison in
    /// [`CheckpointMetadataWrite::check_whole`](super::CheckpointMetadataWrite) a comparison.
    ///
    /// This binds *reuse*, not fabrication, and the distinction is worth stating rather than
    /// glossing: nothing here stops a caller inventing a [`CompletedCheckpoint`] that names
    /// any job and epoch it likes. What stops that is the completion half — every subtask
    /// index of every operator has to be named — together with the fact that production's only
    /// producer, `CheckpointState::build_metadata` in `arroyo-worker`, derives the identity and
    /// the metadata from the same two fields of the checkpoint it is closing. That second half
    /// is a property of the caller, not of this type.
    pub fn new(job_id: String, epoch: u32, operators: Vec<CompletedOperator>) -> Self {
        Self {
            checkpoint: CheckpointIdentity::new(job_id, epoch),
            operators,
        }
    }

    /// The checkpoint this is evidence of.
    pub fn checkpoint(&self) -> &CheckpointIdentity {
        &self.checkpoint
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
    /// Borrowed from the caller for the same reason
    /// [`RestoringProgram`](super::RestoringProgram) borrows it: the check is about the
    /// program the checkpoint was taken of, and that set is not something the checkpoint's own
    /// bookkeeping may be asked to confirm about itself.
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
    /// and an operator with no subtasks has reported nothing whatever its counters say. Since
    /// review round 5 it is per subtask index rather than per message — see
    /// [`CompletedOperator::finished`].
    ///
    /// The checkpoint's own identity is deliberately *not* checked here. There is nothing to
    /// check it against: this value is the record of what a checkpoint completed, and a job id
    /// and epoch are true of it by being what the bookkeeping was accumulating for. The
    /// identity becomes a claim only when it is used to entitle a particular metadata object,
    /// which is where it is compared.
    fn check_whole(&self, program: &HashSet<&str>) -> Result<(), StateError> {
        check_program_coverage(self.checkpoint.epoch(), self.operator_ids(), program)?;

        let mut unfinished: Vec<String> = self
            .operators
            .iter()
            .filter(|operator| !operator.finished())
            .map(CompletedOperator::unfinished_detail)
            .collect();
        if !unfinished.is_empty() {
            unfinished.sort_unstable();
            return Err(StateError::IncompleteCheckpoint {
                epoch: self.checkpoint.epoch(),
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

#[cfg(test)]
mod tests {
    use super::{CompletedCheckpoint, CompletedOperator};
    use arroyo_rpc::errors::StateError;
    use arroyo_rpc::state_backend::validated::Validated;
    use std::collections::HashSet;

    /// One operator's completion evidence, for a program of exactly that operator.
    fn evidence(
        subtasks: usize,
        reported: impl IntoIterator<Item = u32>,
    ) -> Result<Validated<CompletedCheckpoint>, StateError> {
        let program: HashSet<&str> = HashSet::from(["node_1"]);
        Validated::validate(
            CompletedCheckpoint::new(
                "job_1".to_string(),
                4,
                vec![CompletedOperator::reported(
                    "node_1".to_string(),
                    subtasks,
                    reported,
                )],
            ),
            &program,
        )
    }

    /// One subtask reporting twice is one subtask, not two (PR #160 review round 5,
    /// finding 1).
    ///
    /// The finding, exactly: completion was `subtasks_checkpointed == subtasks` over a counter
    /// that `OperatorState::finish_subtask` advanced for every message it received, so at
    /// parallelism 2 two reports from subtask 0 satisfied `2 == 2` and entitled a metadata
    /// write while subtask 1 had never been heard from. That spelling is gone: the evidence is
    /// the set of indices, and the second `0` collapses into the first.
    ///
    /// The three cases below are the same operator one report apart, so what changes the
    /// answer is which subtask reported and nothing else.
    #[test]
    fn two_reports_from_one_subtask_are_not_two_subtasks() {
        let doubled = evidence(2, [0, 0]).unwrap_err();
        assert!(
            matches!(doubled, StateError::IncompleteCheckpoint { epoch: 4, .. }),
            "{doubled:?}"
        );
        let message = doubled.to_string();
        assert!(message.contains("node_1 (1/2 subtasks"), "{message}");
        assert!(message.contains("subtask(s) 1 did not report"), "{message}");

        // Reporting it a third and fourth time does not help either: the count a caller could
        // once inflate is not what is being compared.
        let quadrupled = evidence(2, [0, 0, 0, 0]).unwrap_err();
        assert!(
            quadrupled.to_string().contains("node_1 (1/2 subtasks"),
            "{quadrupled}"
        );

        // The subtask that had not reported is the whole of the difference.
        evidence(2, [0, 1]).expect("both subtasks reported, so the operator finished");
    }

    /// An index the operator does not have cannot stand in for one it does (PR #160 review
    /// round 5, finding 1).
    ///
    /// The other half of the same finding. A count of distinct indices would be satisfied by
    /// `{0, 7}` at parallelism 2 just as readily as by `{0, 1}`; comparing against the range
    /// is what tells them apart. Out-of-range indices are refused rather than counted at the
    /// worker's report boundary too, before anything is folded into the checkpoint — this is
    /// the evidence type refusing to certify one that reached it anyway.
    #[test]
    fn a_subtask_index_the_operator_does_not_have_is_not_evidence_for_one_it_does() {
        let foreign = evidence(2, [0, 7]).unwrap_err();
        assert!(
            matches!(foreign, StateError::IncompleteCheckpoint { epoch: 4, .. }),
            "{foreign:?}"
        );
        let message = foreign.to_string();
        assert!(message.contains("node_1 (2/2 subtasks"), "{message}");
        assert!(message.contains("subtask(s) 1 did not report"), "{message}");
        assert!(
            message.contains("index(es) 7 are not subtasks"),
            "{message}"
        );

        // Nor does an operator that reported only out-of-range indices reach completion by
        // having as many of them as it has subtasks.
        let all_foreign = evidence(2, [5, 9]).unwrap_err();
        assert!(
            all_foreign.to_string().contains("node_1 (2/2 subtasks"),
            "{all_foreign}"
        );

        // And an operator with no subtasks has reported nothing, whatever arrives for it.
        let none = evidence(0, [0]).unwrap_err();
        assert!(none.to_string().contains("node_1 (1/0 subtasks"), "{none}");
    }
}
