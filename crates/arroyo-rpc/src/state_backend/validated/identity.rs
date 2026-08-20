//! The identities a validate-then-act token has to bind, and the checks that bind them.
//!
//! Every whole object in the [`WholeObject`](super::WholeObject) families is *about* a
//! particular checkpoint of a particular job, and every one of them is assembled out of values
//! that arrive independently: a job id and an epoch the caller asked for, a top-level metadata
//! object read back out of storage, one persisted operator object per operator, a leader-mode
//! manifest read from a `CheckpointRef`, and — for a checkpoint this process is taking — a
//! report per subtask. Each of those carries its own copy of the identity. Nothing makes the
//! copies agree.
//!
//! # Why this module exists rather than a check per family
//!
//! Review rounds 4, 5 and 6 of PR #160 are the same defect three times, and the repository
//! owner's round-6 diagnosis names it: *fields were validated independently instead of their
//! relationships*. Round 4 checked that a metadata write named the operators that had been
//! validated, and not that they had validated the same checkpoint. Round 5 bound the identity
//! of one entitlement — the completion — and left the other two constructors saying `None`.
//! Round 6 found the persisted operator header being read for its `operator_id` while its
//! `job_id` and `epoch` went unread, even though compaction builds the path it *writes* to out
//! of exactly those two fields.
//!
//! Each of those was patched where it was found. This module is the model instead: one
//! identity type, one comparison, and one header check, used by every family, so that
//! "which checkpoint is this token about?" has a single answer that every boundary asks in the
//! same words.
//!
//! # Why it lives in `arroyo-rpc`
//!
//! Round 6 built it in `arroyo-state`, beside the parquet backend's families, and review round
//! 7 found the same class unenforced in the two sibling crates: `arroyo-state-protocol`'s
//! leader-mode generation publication and history collection, and this crate's own
//! [`validate_manifest_covers_program`](crate::state_backend::validate_manifest_covers_program).
//! Neither could reach `arroyo-state` — `arroyo-state` and `arroyo-state-protocol` both depend
//! on `arroyo-rpc`, not the other way round — so the choice was one identity type here or a
//! second one there. Two types that differ only in which crate they live in do not differ, so
//! it moved here, where the [`Validated`](super::Validated) token it serves already lives.
//! `arroyo_state::validated` re-exports it, so nothing that named it before names anything
//! else now.
//!
//! # What a caller supplies and what an object claims
//!
//! The distinction runs through everything here and is worth stating plainly, because it is
//! what the checks are and are not worth.
//!
//! - The **expected** identity is what the caller asked storage for. It is not evidence of
//!   anything; it is the question.
//! - The identity a **persisted object claims** — `CheckpointMetadata.job_id`/`epoch`,
//!   `OperatorMetadata.job_id`/`operator_id`/`epoch`, a `CheckpointManifest`'s own four fields
//!   — is input from storage, and after M11.T25d it is authoritative input: a token minted over
//!   it entitles effects, and expiring-table compaction builds its output paths from it
//!   (`expiring_time_key_map::CompactorState::write_batch`). An object that claims to be
//!   something else, stored under the path the caller reads, therefore redirects writes.
//!   Checking the claim against the question is the whole point.
//! - Where two values a *single caller* supplies are compared — the identity a token was
//!   built with against the identity its context declares — the check is worth less: a caller
//!   that is wrong twice in the same way passes it. It is kept anyway, because it is what
//!   stops a *future* constructor from quietly declaring a different checkpoint than the one
//!   whose objects it collected, and because leaving it out is how the `completed: None`
//!   constructors came to exist. Where a check is only this, the doc comment says so.

use crate::grpc::rpc::{CheckpointMetadata, OperatorCheckpointMetadata};

/// One checkpoint of one job: the identity every validate-then-act token is about.
///
/// `job_id` is the storage prefix a checkpoint's objects live under and `epoch` is the
/// checkpoint within it, so the pair is exactly what a path is built from — which is why it is
/// the pair that has to agree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointIdentity {
    job_id: String,
    epoch: u32,
}

impl CheckpointIdentity {
    /// The checkpoint at `epoch` of `job_id`.
    pub fn new(job_id: impl Into<String>, epoch: u32) -> Self {
        Self {
            job_id: job_id.into(),
            epoch,
        }
    }

    /// The identity a top-level metadata object claims for itself.
    pub fn claimed_by(metadata: &CheckpointMetadata) -> Self {
        Self::new(metadata.job_id.as_str(), metadata.epoch)
    }

    /// The checkpoint a leader-mode object names, when its epoch is carried in the wider type
    /// the protocol's manifests use.
    ///
    /// `CheckpointManifest.epoch` is a `uint64` while `CheckpointMetadata.epoch` and every
    /// `OperatorMetadata.epoch` under it are `uint32`, and every writer of a manifest widens
    /// the narrow one into it (`finish_checkpoint_leader`, `arroyo-worker`). A value that does
    /// not fit back is therefore a value no writer produced.
    ///
    /// It is refused rather than truncated, which is the whole reason this is fallible: a
    /// truncating cast would make epoch `2^32 + 4` and epoch `4` compare *equal*, and an
    /// identity type whose comparison can be satisfied by a value it was never given is worse
    /// than no identity type.
    ///
    /// # Errors
    ///
    /// Returns `refuse(detail)` when `epoch` does not fit the width every checkpoint object
    /// carries.
    pub fn at_wide_epoch<E>(
        job_id: impl Into<String>,
        epoch: u64,
        refuse: impl FnOnce(String) -> E,
    ) -> Result<Self, E> {
        match u32::try_from(epoch) {
            Ok(epoch) => Ok(Self::new(job_id, epoch)),
            Err(_) => Err(refuse(format!(
                "epoch {epoch} is wider than the epoch every checkpoint object carries, so no \
                 writer of one produced it"
            ))),
        }
    }

    /// The job whose prefix this checkpoint's objects live under.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// The checkpoint within that job.
    pub fn epoch(&self) -> u32 {
        self.epoch
    }

    /// The same job, at another epoch — for the boundaries that legitimately span epochs.
    ///
    /// A cleanup is the reason this exists: it retains one epoch and drops a range of older
    /// ones, all under one job, so "the identity the retained object must carry" and "the
    /// identity the epoch-3 object must carry" differ in exactly one field and must not be
    /// confused for a licence to differ in the other.
    pub fn at_epoch(&self, epoch: u32) -> Self {
        Self::new(self.job_id.as_str(), epoch)
    }

    /// The identity one of this checkpoint's operator objects must carry.
    pub fn operator<'a>(&'a self, operator_id: &'a str) -> OperatorObject<'a> {
        OperatorObject {
            job_id: &self.job_id,
            operator_id,
            epoch: self.epoch,
        }
    }

    /// Refuses unless `found` names this same checkpoint.
    ///
    /// `what` names the thing that claimed `found`, so the message says which of the several
    /// identities in play disagreed. `refuse` builds the caller's own error, because each
    /// family keeps the error type its callers already handle — see
    /// [`WholeObject`](super::WholeObject).
    ///
    /// # Errors
    ///
    /// Returns `refuse(detail)` when either half differs. Both halves are reported in one
    /// message rather than one at a time, so a token built for the wrong job *and* the wrong
    /// epoch does not have to be fixed twice to find that out.
    pub fn check_matches<E>(
        &self,
        what: &str,
        found: &CheckpointIdentity,
        refuse: impl FnOnce(String) -> E,
    ) -> Result<(), E> {
        if self == found {
            return Ok(());
        }
        Err(refuse(format!(
            "{what} is job {} epoch {}, but this operation is on job {} epoch {}, which is a \
             different checkpoint",
            found.job_id, found.epoch, self.job_id, self.epoch
        )))
    }
}

/// The identity a persisted operator metadata object must carry: which job, which operator,
/// which epoch.
///
/// Borrowed rather than owned because it is built per object from a
/// [`CheckpointIdentity`] and an operator id the caller already holds; see
/// [`CheckpointIdentity::operator`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperatorObject<'a> {
    /// The job whose prefix the object was read from.
    pub job_id: &'a str,
    /// The operator the object was read for.
    pub operator_id: &'a str,
    /// The epoch the object was read at.
    pub epoch: u32,
}

/// Checks a persisted operator metadata object's header against the identity the caller
/// expects of it.
///
/// This is the check that was missing until review round 6 of PR #160. The header was read
/// for its `operator_id` and nothing else, so an object carrying another job's or another
/// epoch's header passed "whole-object validation" as long as it was stored under the path
/// the caller read — and the token that resulted is consumed by compaction, whose expiring-
/// table half builds every file it writes out of `OperatorMetadata.job_id`,
/// `OperatorMetadata.operator_id` and `OperatorMetadata.epoch`. A foreign header could
/// therefore redirect writes *after* the object had been vouched for.
///
/// The header is also what a restoring worker reads its watermark out of
/// (`TableManager::load`), and it does so with an `unwrap`, which is why an absent header is
/// refused here rather than tolerated.
///
/// # Errors
///
/// Returns `refuse(detail)` if the object carries no header at all, or if the header names a
/// different job, operator or epoch. Every disagreeing field is named in one message.
pub fn check_operator_header<E>(
    expected: OperatorObject<'_>,
    metadata: &OperatorCheckpointMetadata,
    refuse: impl FnOnce(String) -> E,
) -> Result<(), E> {
    let OperatorObject {
        job_id,
        operator_id,
        epoch,
    } = expected;

    let Some(header) = metadata.operator_metadata.as_ref() else {
        return Err(refuse(format!(
            "the checkpoint metadata object for operator {operator_id} has no operator header, \
             which the worker that builds it requires"
        )));
    };

    let mut disagreeing = Vec::new();
    if header.job_id != job_id {
        disagreeing.push(format!("job \"{}\"", header.job_id));
    }
    if header.operator_id != operator_id {
        disagreeing.push(format!("operator \"{}\"", header.operator_id));
    }
    if header.epoch != epoch {
        disagreeing.push(format!("epoch {}", header.epoch));
    }
    if disagreeing.is_empty() {
        return Ok(());
    }

    Err(refuse(format!(
        "the checkpoint metadata object read for job {job_id} epoch {epoch} operator \
         {operator_id} is headed {} instead",
        disagreeing.join(", ")
    )))
}

#[cfg(test)]
mod tests {
    use super::{CheckpointIdentity, check_operator_header};
    use crate::grpc::rpc::{OperatorCheckpointMetadata, OperatorMetadata};

    /// A persisted operator object headed exactly as stated.
    fn headed(job_id: &str, operator_id: &str, epoch: u32) -> OperatorCheckpointMetadata {
        OperatorCheckpointMetadata {
            operator_metadata: Some(OperatorMetadata {
                job_id: job_id.to_string(),
                operator_id: operator_id.to_string(),
                epoch,
                min_watermark: None,
                max_watermark: None,
                parallelism: 1,
            }),
            ..Default::default()
        }
    }

    /// Each of the three fields of a persisted header is checked, and each is checked
    /// *independently* of the other two (PR #160 review round 6).
    ///
    /// The round-6 diagnosis is that identities were validated one field at a time while their
    /// relationships went unstated, and that the negative rows never varied one field against
    /// the others. So this row varies exactly one field per case, from an object that agrees in
    /// the other two: an object headed with the right operator at the right epoch but the wrong
    /// job is the attack the finding describes, and it is the case a check that read only
    /// `operator_id` admitted.
    #[test]
    fn a_persisted_operator_header_is_checked_field_by_field() {
        let checkpoint = CheckpointIdentity::new("job_a", 4);
        let expected = checkpoint.operator("node_1");

        check_operator_header(expected, &headed("job_a", "node_1", 4), |d| d)
            .expect("an object headed as the caller expects is the object the caller asked for");

        let wrong_job =
            check_operator_header(expected, &headed("job_b", "node_1", 4), |d| d).unwrap_err();
        assert!(wrong_job.contains("job \"job_b\""), "{wrong_job}");
        assert!(!wrong_job.contains("operator \"node_1\""), "{wrong_job}");

        let wrong_operator =
            check_operator_header(expected, &headed("job_a", "node_2", 4), |d| d).unwrap_err();
        assert!(
            wrong_operator.contains("operator \"node_2\""),
            "{wrong_operator}"
        );

        let wrong_epoch =
            check_operator_header(expected, &headed("job_a", "node_1", 5), |d| d).unwrap_err();
        assert!(wrong_epoch.contains("epoch 5"), "{wrong_epoch}");

        // Two fields wrong is reported once, not twice over two runs.
        let both =
            check_operator_header(expected, &headed("job_b", "node_1", 9), |d| d).unwrap_err();
        assert!(both.contains("job \"job_b\""), "{both}");
        assert!(both.contains("epoch 9"), "{both}");

        let headerless =
            check_operator_header(expected, &OperatorCheckpointMetadata::default(), |d| d)
                .unwrap_err();
        assert!(headerless.contains("no operator header"), "{headerless}");
    }

    /// Two checkpoints of one job, and one checkpoint of two jobs, are different checkpoints.
    ///
    /// The pair an operator set can never tell apart is two epochs of one job, which is why
    /// both halves are varied independently here rather than only the one that is easier to
    /// notice.
    #[test]
    fn a_checkpoint_identity_matches_only_the_same_job_and_epoch() {
        let checkpoint = CheckpointIdentity::new("job_a", 4);

        checkpoint
            .check_matches("the object", &CheckpointIdentity::new("job_a", 4), |d| d)
            .expect("the same job at the same epoch is the same checkpoint");

        let other_job = checkpoint
            .check_matches("the object", &CheckpointIdentity::new("job_b", 4), |d| d)
            .unwrap_err();
        assert!(other_job.contains("job_b"), "{other_job}");
        assert!(other_job.contains("different checkpoint"), "{other_job}");

        let other_epoch = checkpoint
            .check_matches("the object", &CheckpointIdentity::new("job_a", 5), |d| d)
            .unwrap_err();
        assert!(other_epoch.contains("epoch 5"), "{other_epoch}");
        assert!(other_epoch.contains("epoch 4"), "{other_epoch}");

        // `at_epoch` moves the epoch and nothing else, which is what a cleanup's per-epoch
        // expectations are built with.
        assert_eq!(
            checkpoint.at_epoch(2),
            CheckpointIdentity::new("job_a", 2),
            "at_epoch must not change the job"
        );
    }

    /// A manifest's wider epoch narrows only when it is the same number (PR #160 review round
    /// 7).
    ///
    /// The pair a truncating cast cannot tell apart is `epoch` and `epoch + 2^32`, so that is
    /// the pair this varies. It matters because the leader-mode manifest epoch is the one field
    /// of the four that changes width on its way into a [`CheckpointIdentity`], and a
    /// comparison satisfied by a value no writer produced is not a comparison.
    #[test]
    fn a_manifest_epoch_that_does_not_fit_a_checkpoint_epoch_is_refused_not_truncated() {
        let narrowed = CheckpointIdentity::at_wide_epoch("job_a", 4, |d| d)
            .expect("an epoch a checkpoint object could carry is one this can be built from");
        assert_eq!(narrowed, CheckpointIdentity::new("job_a", 4));

        let too_wide =
            CheckpointIdentity::at_wide_epoch("job_a", u64::from(u32::MAX) + 1 + 4, |d| d)
                .unwrap_err();
        assert!(too_wide.contains("4294967300"), "{too_wide}");
        assert!(too_wide.contains("wider than"), "{too_wide}");
    }
}
