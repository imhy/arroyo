//! The whole object a checkpoint cleanup acts on, and the views its token hands out.
//!
//! Split out of [`crate::validated`] because it is a self-contained family: the cleanup, the
//! per-operator view its token yields, and the per-table view `files_to_keep` takes. The
//! three types are declared together because the last two have no public constructor — the
//! only producer of either is [`CheckpointCleanup::operators`], which takes the token — and
//! that is only true while they live in one module.

use super::identity::{CheckpointIdentity, check_operator_header};
use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{OperatorCheckpointMetadata, TableCheckpointMetadata, TableConfig};
use arroyo_rpc::state_backend::validated::{Validated, WholeObject};
use arroyo_rpc::state_backend::{StateBackendSelector, validate_restored_operator_metadata};

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
