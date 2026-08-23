//! The whole object a checkpoint cleanup acts on, and the views its token hands out.
//!
//! Split out of [`crate::validated`] because it is a self-contained family: the cleanup, the
//! per-operator view its token yields, and the per-table view `files_to_keep` takes. The
//! three types are declared together because the last two have no public constructor — the
//! only producer of either is [`CheckpointCleanup::operators`], which takes the token — and
//! that is only true while they live in one module.

use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{
    ExpiringKeyedTimeTableCheckpointMetadata, GlobalKeyedTableTaskCheckpointMetadata,
    OperatorCheckpointMetadata, TableCheckpointMetadata, TableConfig, TableEnum,
};
use arroyo_rpc::state_backend::validated::identity::{CheckpointIdentity, check_operator_header};
use arroyo_rpc::state_backend::validated::{Validated, WholeObject};
use arroyo_rpc::state_backend::{StateBackendSelector, validate_restored_operator_metadata};
use prost::Message;
use std::collections::BTreeSet;

/// Every operator and every epoch a checkpoint cleanup is about to act on.
///
/// A cleanup keeps one epoch and drops a range of older ones, for every operator of the
/// checkpoint. Those older epochs may predate a restart onto a different backend, so all of
/// them have to be checked — and checked before the first delete, because the operators are
/// collected concurrently and a delete by one cannot be taken back by another's refusal.
///
/// # The three epochs
///
/// A cleanup is the one boundary in [`crate::validated`] whose objects legitimately carry
/// *different* epochs, and the identity rule has to say so rather than demand one epoch
/// everywhere. `checkpoint` is the checkpoint whose top-level metadata the cleanup rewrites —
/// the job's current one, whose `min_epoch` is being advanced — and it is neither of the other
/// two. `new_min_epoch` is the epoch being retained, and every operator's retained object must
/// carry it. `old_min_epoch..new_min_epoch` are the epochs being dropped, and each dropped
/// object must carry **the epoch it was collected at**, not the retained one and not the
/// checkpoint's.
#[derive(Debug, Clone)]
pub struct CheckpointCleanup {
    checkpoint: CheckpointIdentity,
    old_min_epoch: u32,
    new_min_epoch: u32,
    operators: Vec<OperatorCleanup>,
}

/// One operator's share of a [`CheckpointCleanup`].
#[derive(Debug, Clone)]
pub struct OperatorCleanup {
    operator_id: String,
    retained: OperatorCheckpointMetadata,
    dropped: Vec<(u32, Option<OperatorCheckpointMetadata>)>,
}

impl OperatorCleanup {
    /// Collects one operator's epochs: the metadata at the epoch being kept, and the
    /// metadata at every epoch being dropped in ascending order.
    ///
    /// A dropped epoch whose object is already gone is recorded as `None` rather than
    /// omitted, so that the check can tell "collected and absent" from "never collected".
    pub fn new(
        operator_id: String,
        retained: OperatorCheckpointMetadata,
        dropped: Vec<(u32, Option<OperatorCheckpointMetadata>)>,
    ) -> Self {
        Self {
            operator_id,
            retained,
            dropped,
        }
    }
}

/// The job, the checkpoint, and the operator list a [`CheckpointCleanup`] has to match.
#[derive(Debug, Clone, Copy)]
pub struct CleanupScope<'a> {
    /// The state backend the job selected.
    pub job: StateBackendSelector,
    /// The checkpoint's own operator list, in order.
    pub operator_ids: &'a [String],
    /// The checkpoint the caller asked storage for: the job doing the cleanup, and the epoch
    /// of the top-level metadata object whose `min_epoch` is being advanced.
    ///
    /// Every path a cleanup deletes from is built out of the collected object rather than out
    /// of the caller's arguments — deliberately, so that what is deleted from is what was
    /// checked. That is only worth anything if the collected object is the one the caller
    /// asked for, which is what this is compared against.
    pub expected: &'a CheckpointIdentity,
}

impl CheckpointCleanup {
    /// Collects the whole cleanup, before it is checked.
    ///
    /// `checkpoint` is the checkpoint whose top-level metadata is being rewritten, not the
    /// epoch being retained; see the [type docs](Self) for the three epochs a cleanup holds.
    pub fn new(
        checkpoint: CheckpointIdentity,
        old_min_epoch: u32,
        new_min_epoch: u32,
        operators: Vec<OperatorCleanup>,
    ) -> Self {
        Self {
            checkpoint,
            old_min_epoch,
            new_min_epoch,
            operators,
        }
    }

    /// The checkpoint whose top-level metadata this cleanup rewrites.
    pub fn checkpoint(&self) -> &CheckpointIdentity {
        &self.checkpoint
    }

    /// The job whose checkpoints are being collected.
    pub fn job_id(&self) -> &str {
        self.checkpoint.job_id()
    }

    /// The first epoch being dropped.
    pub fn old_min_epoch(&self) -> u32 {
        self.old_min_epoch
    }

    /// The epoch being kept; everything below it is dropped.
    pub fn new_min_epoch(&self) -> u32 {
        self.new_min_epoch
    }

    /// The operators of a *checked* cleanup, each as a view its tables can be read through.
    ///
    /// Taking the token rather than `&self` is the point: this is the only producer of
    /// [`ValidatedOperatorCleanup`], so the file classification below it cannot run on a
    /// cleanup nothing checked.
    pub fn operators(
        validated: &Validated<Self>,
    ) -> impl Iterator<Item = ValidatedOperatorCleanup<'_>> {
        validated
            .get()
            .operators
            .iter()
            .map(|operator| ValidatedOperatorCleanup { operator })
    }
}

impl WholeObject for CheckpointCleanup {
    type Context<'a> = CleanupScope<'a>;
    type Error = StateError;

    /// The checkpoint the caller asked for, every operator of it, every epoch of the range,
    /// every one of those objects' persisted headers, and every one of their table configs.
    ///
    /// The structural half — that the operators are exactly the checkpoint's and that each
    /// carries exactly the epochs in `old_min_epoch..new_min_epoch` — is what stops a
    /// caller from earning a token for a cleanup it only partly collected and then deleting
    /// the rest anyway.
    ///
    /// The identity half is what review round 6 of PR #160 added, and it is per object rather
    /// than per call. Every path this cleanup deletes from is derived from the collected value
    /// — `job_id()`, `old_min_epoch()`, `new_min_epoch()` — so a collected object headed for
    /// another job or another epoch does not merely describe the wrong thing, it *aims* the
    /// deletions. The rule is the one the [type docs](Self) state: the retained object carries
    /// `new_min_epoch`, each dropped object carries the epoch it was collected at, and all of
    /// them carry this job.
    fn check_whole(&self, scope: CleanupScope<'_>) -> Result<(), StateError> {
        let structural = |error: String| StateError::Other {
            table: String::new(),
            error,
        };

        scope.expected.check_matches(
            "the checkpoint this cleanup collected",
            &self.checkpoint,
            structural,
        )?;

        // A duplicated id passes the positional comparison below — the collected operators
        // are built *from* this list, so `["a", "a"]` zips against itself and agrees. The
        // rule exists downstream, in `CheckpointMetadataWrite::check_whole`, which is the
        // write that ends the cleanup and therefore runs after every deletion. A token whose
        // check is weaker than the check gating the effect it authorizes is the defect:
        // PR #160 review comment `5384611151`.
        let listed: BTreeSet<&str> = scope.operator_ids.iter().map(String::as_str).collect();
        if listed.len() != scope.operator_ids.len() {
            return Err(structural(format!(
                "the cleanup of job {} was scoped to a checkpoint that lists an operator more \
                 than once: {:?}",
                self.job_id(),
                scope.operator_ids,
            )));
        }

        if self.operators.len() != scope.operator_ids.len()
            || self
                .operators
                .iter()
                .zip(scope.operator_ids)
                .any(|(collected, listed)| collected.operator_id != *listed)
        {
            return Err(structural(format!(
                "the cleanup of job {} collected operators {:?}, but the checkpoint lists {:?}",
                self.job_id(),
                self.operators
                    .iter()
                    .map(|o| o.operator_id.as_str())
                    .collect::<Vec<_>>(),
                scope.operator_ids,
            )));
        }

        // The epoch each collected object is expected to carry, per object. The retained
        // object's is `new_min_epoch` and a dropped object's is its own epoch; both are this
        // job's, because a cleanup never spans jobs (there is no clone or restore-from-another
        // -job path in Arroyo, so an object under this job's prefix headed with another job's
        // id is misplaced by definition).
        let retained_at = self.checkpoint.at_epoch(self.new_min_epoch);

        for operator in &self.operators {
            let collected: Vec<u32> = operator.dropped.iter().map(|(epoch, _)| *epoch).collect();
            let expected: Vec<u32> = (self.old_min_epoch..self.new_min_epoch).collect();
            if collected != expected {
                return Err(structural(format!(
                    "the cleanup of operator {} collected epochs {collected:?}, but it drops \
                     epochs {expected:?}",
                    operator.operator_id,
                )));
            }

            check_operator_header(
                retained_at.operator(&operator.operator_id),
                &operator.retained,
                structural,
            )?;
            validate_restored_operator_metadata(scope.job, &operator.retained)?;
            check_table_files_in_namespace(
                self.job_id(),
                &operator.operator_id,
                &operator.retained,
            )?;

            for (epoch, metadata) in &operator.dropped {
                let Some(metadata) = metadata else { continue };
                check_operator_header(
                    self.checkpoint
                        .at_epoch(*epoch)
                        .operator(&operator.operator_id),
                    metadata,
                    structural,
                )?;
                validate_restored_operator_metadata(scope.job, metadata)?;
                check_table_files_in_namespace(self.job_id(), &operator.operator_id, metadata)?;
            }
        }

        Ok(())
    }
}

/// One operator's share of a cleanup that has been checked as a whole.
///
/// Has no public constructor: [`CheckpointCleanup::operators`] is the only producer, and it
/// takes the token.
#[derive(Debug, Clone, Copy)]
pub struct ValidatedOperatorCleanup<'a> {
    operator: &'a OperatorCleanup,
}

impl<'a> ValidatedOperatorCleanup<'a> {
    /// The operator these epochs belong to.
    pub fn operator_id(&self) -> &'a str {
        &self.operator.operator_id
    }

    /// The tables of the epoch being kept; their files are the ones the cleanup protects.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Other`] if a table has checkpoint metadata but no table
    /// config, which leaves nothing that can say how to read its files.
    pub fn retained_tables(&self) -> Result<Vec<ValidatedTable<'a>>, StateError> {
        tables_of(&self.operator.operator_id, &self.operator.retained)
    }

    /// The tables of every epoch being dropped, oldest epoch first, skipping epochs whose
    /// object is already gone.
    ///
    /// # Errors
    ///
    /// As [`ValidatedOperatorCleanup::retained_tables`].
    pub fn dropped_tables(&self) -> Result<Vec<ValidatedTable<'a>>, StateError> {
        let mut tables = Vec::new();
        for (_, metadata) in &self.operator.dropped {
            let Some(metadata) = metadata else { continue };
            tables.extend(tables_of(&self.operator.operator_id, metadata)?);
        }
        Ok(tables)
    }
}

/// Every data file one collected epoch references, checked against the job's own namespace.
///
/// **PR #160 review comment `5384870087`.** The file strings inside a table's checkpoint
/// metadata are that object's *contents*: nothing about the operator header, the selector or
/// the epoch says where they point. `ParquetBackend::files_no_longer_referenced` reads exactly
/// these strings into the deletion plan and `cleanup_checkpoint` deletes them, so a metadata
/// object for job `A` naming a path under job `B` deleted `B`'s data. Checked here, where the
/// token is earned, rather than where the plan is built — a token whose check is weaker than
/// the effect it authorizes is the defect the round before this one closed.
///
/// The legacy layout is `{job_id}/checkpoints/checkpoint-{epoch}/operator-{op}/table-…`, and
/// the prefix asserted is `{job_id}/checkpoints/` rather than this epoch's own directory: a
/// file carried forward from an older epoch is still this job's, and `files_to_keep` exists
/// precisely because epochs share files.
fn check_table_files_in_namespace(
    job_id: &str,
    operator_id: &str,
    metadata: &OperatorCheckpointMetadata,
) -> Result<(), StateError> {
    for table in tables_of(operator_id, metadata)? {
        for file in table_file_refs(&table)? {
            if !is_table_data_file(job_id, operator_id, table.name, &file) {
                return Err(StateError::Other {
                    table: table.name.to_string(),
                    error: format!(
                        "operator {operator_id} table {} references {file:?}, which is not the \
                         path this job writes that table's data at",
                        table.name,
                    ),
                });
            }
        }
    }
    Ok(())
}

/// Whether `file` is the path a worker writes *this operator's, this table's* data at.
///
/// `{job_id}/checkpoints/checkpoint-{epoch:07}/operator-{operator_id}/table-{table}-{sub}`,
/// optionally suffixed `-compacted` — the layout
/// `CheckpointFilePathLayout::table_checkpoint_path` builds.
///
/// **Binding the operator and the table is the point, not the namespace — PR #160 review
/// comment `5385867064`.** A namespace-only test let operator A's *dropped* epoch name
/// operator B's *retained* data file. `files_no_longer_referenced` subtracts only the retained
/// references of the operator it is planning for, so B's file was in nobody's keep-set and was
/// deleted while every identity, selector and namespace check passed — B's live state, lost to
/// a cleanup that was correct about everything except which object it was looking at.
///
/// The epoch is deliberately unconstrained. A file carried forward from an older epoch is
/// still this operator's and this table's, and `files_to_keep` exists precisely because epochs
/// share files; pinning the epoch would delete state the retained checkpoint still needs.
fn is_table_data_file(job_id: &str, operator_id: &str, table_name: &str, file: &str) -> bool {
    let segments: Vec<&str> = file.split('/').collect();
    let [job, "checkpoints", checkpoint, operator, table] = segments.as_slice() else {
        return false;
    };
    if *job != job_id || *operator != format!("operator-{operator_id}") {
        return false;
    }
    let Some(epoch) = checkpoint.strip_prefix("checkpoint-") else {
        return false;
    };
    if epoch.is_empty() || !epoch.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    let Some(subtask) = table.strip_prefix(format!("table-{table_name}-").as_str()) else {
        return false;
    };
    let subtask = subtask.strip_suffix("-compacted").unwrap_or(subtask);
    !subtask.is_empty() && subtask.bytes().all(|b| b.is_ascii_digit())
}

/// The file strings one table's checkpoint metadata carries, by encoding.
///
/// Exhaustive over [`TableEnum`], so a third encoding cannot be added without answering where
/// its files live. `MissingTableType` yields nothing rather than erroring: it is refused a few
/// steps later by `files_no_longer_referenced`, which is the place that already says why a
/// table with no type cannot have its file set determined, and answering it twice would be two
/// spellings of one rule.
fn table_file_refs(table: &ValidatedTable<'_>) -> Result<Vec<String>, StateError> {
    let decode_failed = |e: prost::DecodeError| StateError::Other {
        table: table.name.to_string(),
        error: format!(
            "table {} has undecodable checkpoint metadata: {e}",
            table.name
        ),
    };
    Ok(match table.config.table_type() {
        TableEnum::MissingTableType => Vec::new(),
        TableEnum::GlobalKeyValue => {
            GlobalKeyedTableTaskCheckpointMetadata::decode(table.checkpoint.data.as_slice())
                .map_err(decode_failed)?
                .files
        }
        TableEnum::ExpiringKeyedTimeTable => {
            ExpiringKeyedTimeTableCheckpointMetadata::decode(table.checkpoint.data.as_slice())
                .map_err(decode_failed)?
                .files
                .into_iter()
                .map(|file| file.file)
                .collect()
        }
    })
}

/// Pairs each of one operator epoch's tables with the config that describes it.
fn tables_of<'a>(
    operator_id: &str,
    metadata: &'a OperatorCheckpointMetadata,
) -> Result<Vec<ValidatedTable<'a>>, StateError> {
    metadata
        .table_checkpoint_metadata
        .iter()
        .map(|(table_name, checkpoint)| {
            let config =
                metadata
                    .table_configs
                    .get(table_name)
                    .ok_or_else(|| StateError::Other {
                        table: table_name.clone(),
                        error: format!(
                            "missing table config for operator {operator_id}, table {table_name}, \
                         metadata is {checkpoint:?}, operator_metadata is {metadata:?}"
                        ),
                    })?;
            Ok(ValidatedTable {
                name: table_name,
                config,
                checkpoint,
            })
        })
        .collect()
}

/// One table of one operator epoch, drawn from an object that was checked as a whole.
///
/// This is what `files_to_keep` takes instead of a bare config and metadata pair. It has no
/// public constructor, so the classification that decides which of a job's files survive a
/// cleanup cannot be run on something nothing vouched for.
#[derive(Debug, Clone, Copy)]
pub struct ValidatedTable<'a> {
    name: &'a str,
    config: &'a TableConfig,
    checkpoint: &'a TableCheckpointMetadata,
}

impl<'a> ValidatedTable<'a> {
    /// The table's name within its operator.
    pub fn name(&self) -> &'a str {
        self.name
    }

    /// The table's config, as the operator's checkpoint metadata records it.
    pub fn config(&self) -> &'a TableConfig {
        self.config
    }

    /// The table's checkpoint metadata for this epoch.
    pub fn checkpoint(&self) -> &'a TableCheckpointMetadata {
        self.checkpoint
    }
}
