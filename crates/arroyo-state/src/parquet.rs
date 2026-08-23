use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::tables::global_keyed_map::GlobalKeyedTable;
use crate::tables::{CompactionConfig, ErasedTable};
use crate::validated::{
    CheckpointCleanup, CheckpointIdentity, CheckpointMetadataWrite, CleanupScope, OperatorCleanup,
    RestorableCheckpoint, RestoringProgram, ValidatedOperatorCleanup, ValidatedTable,
    check_operator_header, check_program_coverage,
};
use crate::{BackingStore, StorageProviderFor, get_storage_provider};
use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{
    CheckpointMetadata, OperatorCheckpointMetadata, TableCheckpointMetadata,
};
use arroyo_types::CheckpointFilePathLayout;
use futures::StreamExt;
use futures::stream::FuturesUnordered;

use arroyo_rpc::config::config;
use arroyo_rpc::grpc::rpc;
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_rpc::state_backend::validated::Validated;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, info};

pub const FULL_KEY_RANGE: RangeInclusive<u64> = 0..=u64::MAX;
pub const GENERATIONS_TO_COMPACT: u32 = 1; // only compact generation 0 files

pub struct ParquetBackend;

fn base_path(job_id: &str, epoch: u32) -> String {
    format!("{job_id}/checkpoints/checkpoint-{epoch:0>7}")
}

fn metadata_path(path: &str) -> String {
    format!("{path}/metadata")
}

fn operator_path(job_id: &str, epoch: u32, operator: &str) -> String {
    format!("{}/operator-{}", base_path(job_id, epoch), operator)
}

#[async_trait::async_trait]
impl BackingStore for ParquetBackend {
    fn name() -> &'static str {
        "parquet"
    }

    /// Loads one checkpoint's top-level metadata, and checks that the object is the one that
    /// was asked for.
    ///
    /// The path is built from `job_id` and `epoch`; the object carries its own copy of both.
    /// Until review round 6 of PR #160 nothing compared them, and the object's copies are what
    /// everything downstream uses — `load_checkpoint_operators` reads every operator object
    /// from `metadata.job_id`/`metadata.epoch`, `cleanup_checkpoint` deletes from them, and
    /// `write_checkpoint_metadata` writes back to them. An object of another checkpoint stored
    /// under this path therefore redirected all of that; checking here is what makes "the
    /// checkpoint the caller asked for" true rather than assumed, for every caller including
    /// the worker's own restore (`Program::from_logical`), which has no token at all.
    ///
    /// # Errors
    ///
    /// [`StateError::IncompleteCheckpoint`] if the object names a different job or epoch,
    /// alongside the storage and decode failures reading it can produce.
    async fn load_checkpoint_metadata(
        role: &StorageProviderFor,
        job_id: &str,
        epoch: u32,
    ) -> Result<CheckpointMetadata, StateError> {
        let storage_client = get_storage_provider(role).await?;
        let data = storage_client
            .get(metadata_path(&base_path(job_id, epoch)).as_str())
            .await?;
        let metadata = CheckpointMetadata::decode(&data[..])?;
        CheckpointIdentity::new(job_id, epoch).check_matches(
            "the checkpoint metadata object read from this checkpoint's path",
            &CheckpointIdentity::claimed_by(&metadata),
            |detail| StateError::IncompleteCheckpoint { epoch, detail },
        )?;
        Ok(metadata)
    }

    /// Loads one operator's checkpoint metadata, and checks that the object is headed as the
    /// one that was asked for.
    ///
    /// Absent is `None`, as it has always been — the callers that require the object say so
    /// themselves. Present but headed for another job, another operator or another epoch is a
    /// failure rather than a value, because the header is authoritative input downstream:
    /// `TableManager::load` reads the restored watermark out of it, and expiring-table
    /// compaction builds the path of every file it writes from its `job_id`, `operator_id` and
    /// `epoch`. Checking it here covers every reader of an operator object in one place,
    /// including the worker's restore; each whole-object token checks it again over the value
    /// it actually holds, so the guarantee does not depend on where the value came from.
    ///
    /// # Errors
    ///
    /// [`StateError::IncompleteCheckpoint`] if the object carries no header or one naming a
    /// different job, operator or epoch, alongside the storage and decode failures reading it
    /// can produce.
    async fn load_operator_metadata(
        role: &StorageProviderFor,
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Result<Option<OperatorCheckpointMetadata>, StateError> {
        let storage_client = get_storage_provider(role).await?;
        let Some(data) = storage_client
            .get_if_present(metadata_path(&operator_path(job_id, epoch, operator_id)).as_str())
            .await?
        else {
            return Ok(None);
        };
        let metadata = OperatorCheckpointMetadata::decode(&data[..])?;
        check_operator_header(
            CheckpointIdentity::new(job_id, epoch).operator(operator_id),
            &metadata,
            |detail| StateError::IncompleteCheckpoint { epoch, detail },
        )?;
        Ok(Some(metadata))
    }

    async fn write_operator_checkpoint_metadata(
        role: &StorageProviderFor,
        metadata: OperatorCheckpointMetadata,
    ) -> Result<(), StateError> {
        let storage_client = get_storage_provider(role).await?;
        let operator_metadata =
            metadata
                .operator_metadata
                .as_ref()
                .ok_or_else(|| StateError::Other {
                    table: "".to_string(),
                    error: "missing operator metadata".to_string(),
                })?;
        let path = metadata_path(&operator_path(
            &operator_metadata.job_id,
            operator_metadata.epoch,
            &operator_metadata.operator_id,
        ));
        storage_client
            .put(path.as_str(), metadata.encode_to_vec())
            .await?;
        // TODO: propagate error
        Ok(())
    }

    async fn write_checkpoint_metadata(
        role: &StorageProviderFor,
        metadata: Validated<CheckpointMetadataWrite>,
    ) -> Result<(), StateError> {
        let metadata = metadata.into_inner().into_metadata();
        debug!("writing checkpoint {:?}", metadata);
        let storage_client = get_storage_provider(role).await?;
        let path = metadata_path(&base_path(&metadata.job_id, metadata.epoch));
        storage_client
            .put(path.as_str(), metadata.encode_to_vec())
            .await?;
        Ok(())
    }

    async fn cleanup_checkpoint(
        role: &StorageProviderFor,
        job: StateBackendSelector,
        checkpoint: CheckpointIdentity,
        mut metadata: CheckpointMetadata,
        old_min_epoch: u32,
        min_epoch: u32,
    ) -> Result<(), StateError> {
        info!(
            message = "Cleaning checkpoint",
            min_epoch,
            job_id = metadata.job_id
        );

        // Collect first, check second, delete third. Every operator and every epoch this
        // cleanup touches is read before the first delete and then checked against `job` as
        // one object, so a mismatch found in the last operator's oldest epoch cannot follow
        // files the first operator has already removed. Each metadata object is still read
        // exactly once and operators are still collected concurrently; what the check costs
        // is holding one cleanup's metadata — the operators of the epoch being kept plus the
        // few epochs being dropped — rather than one operator's at a time.
        //
        // The classification below runs off the token, so `files_to_keep` cannot decide
        // which of this job's files survive on the strength of an object nothing checked.
        let mut collecting: FuturesUnordered<_> = metadata
            .operator_ids
            .iter()
            .enumerate()
            .map(|(position, operator_id)| {
                let job_id = metadata.job_id.clone();
                let operator_id = operator_id.clone();
                async move {
                    let operator = Self::collect_operator_cleanup(
                        role,
                        job_id,
                        operator_id,
                        old_min_epoch,
                        min_epoch,
                    )
                    .await?;
                    Ok::<_, StateError>((position, operator))
                }
            })
            .collect();

        let mut collected = Vec::with_capacity(metadata.operator_ids.len());
        while let Some(operator) = collecting.next().await {
            collected.push(operator?);
        }
        drop(collecting);
        // Back into the checkpoint's own order, which is the order the check compares
        // against and the order the deletions below run in.
        collected.sort_by_key(|(position, _)| *position);

        let cleanup = Validated::validate(
            CheckpointCleanup::new(
                CheckpointIdentity::claimed_by(&metadata),
                old_min_epoch,
                min_epoch,
                collected
                    .into_iter()
                    .map(|(_, operator)| operator)
                    .collect(),
            ),
            CleanupScope {
                job,
                operator_ids: &metadata.operator_ids,
                expected: &checkpoint,
            },
        )?;

        // The entitlement for the write that *ends* this cleanup, proven here rather than
        // after the deletions — PR #160 review comment `5384611151`. `check_whole` on this
        // token is the strictest check the cleanup makes: it compares the identity the
        // metadata claims against the one the cleanup earned, and refuses metadata that names
        // an operator twice. Run last, as it was, any of those refusals arrived with the data
        // files and the old per-epoch metadata already deleted and `min_epoch` never
        // advanced, leaving the top-level object pointing at epochs that no longer exist.
        // Nothing below this line is reversible, so everything that can refuse is above it.
        let operator_count = metadata.operator_ids.len();
        metadata.min_epoch = min_epoch;
        let write = Validated::validate(
            CheckpointMetadataWrite::after_cleanup(metadata, &cleanup),
            (),
        )?;

        let mut plans = Vec::with_capacity(operator_count);
        for operator in CheckpointCleanup::operators(&cleanup) {
            plans.push(OperatorCleanupPlan {
                operator_id: operator.operator_id().to_string(),
                files_to_delete: Self::files_no_longer_referenced(&operator)?,
            });
        }

        let storage_client = get_storage_provider(role).await?;

        // Every path below is derived from the token rather than from the arguments, so the
        // job and the epoch range that are deleted from are literally the ones the check
        // covered. That is only worth anything because the check now compares the collected
        // object's identity against `checkpoint`, the one the caller asked for: until review
        // round 6 of PR #160 the paths came from a `job_id` nothing had checked, which made
        // "derived from the token" a statement about where the value was read rather than
        // about what it is.
        let deleting_from = cleanup.get();
        let mut deleting: FuturesUnordered<_> = plans
            .into_iter()
            .map(|plan| {
                let storage_client = Arc::clone(&storage_client);
                async move {
                    for file in plan.files_to_delete {
                        storage_client.delete_if_present(file).await?;
                    }

                    for epoch_to_remove in
                        deleting_from.old_min_epoch()..deleting_from.new_min_epoch()
                    {
                        let path = metadata_path(&operator_path(
                            deleting_from.job_id(),
                            epoch_to_remove,
                            &plan.operator_id,
                        ));
                        storage_client.delete_if_present(path).await?;
                    }

                    Ok::<_, StateError>(plan.operator_id)
                }
            })
            .collect();

        // wait for all of the futures to complete
        while let Some(result) = deleting.next().await {
            let operator_id = result?;
            debug!(
                message = "Finished cleaning operator",
                job_id = deleting_from.job_id(),
                operator_id,
                min_epoch
            );
        }
        drop(deleting);

        for epoch_to_remove in deleting_from.old_min_epoch()..deleting_from.new_min_epoch() {
            storage_client
                .delete_if_present(metadata_path(&base_path(
                    deleting_from.job_id(),
                    epoch_to_remove,
                )))
                .await?;
        }
        Self::write_checkpoint_metadata(role, write).await?;
        Ok(())
    }
}

/// One operator's fully classified cleanup.
///
/// Derived from a [`Validated<CheckpointCleanup>`] for every operator of a checkpoint before
/// any of them is executed, so the decision to delete is made with the whole checkpoint in
/// hand. It carries the derived file set rather than the metadata it came from.
struct OperatorCleanupPlan {
    operator_id: String,
    files_to_delete: HashSet<String>,
}

impl ParquetBackend {
    /// Called after a checkpoint is committed
    ///
    /// `job` is the state backend the job selected. The checkpoint about to be rewritten
    /// is checked against it before any file is read or written: compacting a checkpoint
    /// another backend wrote would rewrite that backend's state under this one's rules.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::StateBackendError`] if the checkpoint's table configs
    /// disagree with `job`, alongside the storage and table failures compaction can
    /// otherwise produce.
    pub async fn compact_operator(
        role: &StorageProviderFor,
        job: StateBackendSelector,
        job_id: Arc<String>,
        operator_id: &str,
        epoch: u32,
    ) -> Result<HashMap<String, TableCheckpointMetadata>, StateError> {
        // One operator is still a whole object: it goes through the same load-check-act path
        // as a whole checkpoint, with itself as the entire set being compacted, so there is
        // no second route to `compact_validated_checkpoint`.
        let compacted =
            Self::compact_checkpoint(role, job, job_id, vec![operator_id.to_string()], epoch)
                .await?;

        Ok(compacted
            .into_iter()
            .next()
            .map(|(_, tables)| tables)
            .unwrap_or_default())
    }

    /// Loads and validates every operator's metadata for one checkpoint, without touching
    /// anything.
    ///
    /// This is the preflight for restoring: a checkpoint is restored by rewriting its
    /// top-level metadata and then starting workers that build one operator's tables at a
    /// time, and each of those steps is only safe if the *whole* checkpoint belongs to
    /// this job's backend. Validating each operator as it is reached would let the earlier
    /// operators be rebuilt, and the metadata be rewritten, before a later operator's
    /// disagreement is seen.
    ///
    /// Operators are loaded concurrently and returned in `operator_ids` order, so the
    /// caller reuses these objects instead of reading them again — restoring a checkpoint
    /// with this preflight costs the same reads it did without one, as long as the caller
    /// consumes what it is given. They come back inside a
    /// [`Validated<RestorableCheckpoint>`], which is what the metadata rewrite that follows
    /// a restore takes: the preflight and the rewrite are joined by the type rather than by
    /// the caller remembering to run them in that order (design item M11.D39c).
    ///
    /// `restoring` is the set of operators the job's workers will actually construct,
    /// derived from the *current* program rather than from the checkpoint. The two are
    /// checked against each other before anything is read, because the checkpoint's own
    /// list is not the set that matters: a worker builds every operator of its program and
    /// each one loads its own metadata object, so an operator the checkpoint omits is
    /// never preflighted and yet is still required — and an operator the checkpoint lists
    /// but the program does not contain means the checkpoint belongs to a different
    /// program.
    ///
    /// Every listed operator must also *have* a metadata object, and that object must
    /// carry an operator header naming it. Absence used to be reported as `None` and left
    /// to the caller, on the grounds that absence states no selector; but the workers this
    /// preflight runs on behalf of reject it (`TableManager::load` requires the object
    /// whenever it restores at all, and reads the restored watermark out of its header
    /// without checking that there is one), so tolerating either only moved the failure
    /// past the point where the checkpoint's metadata has already been rewritten.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::StateBackendError`] if any operator's table configs disagree
    /// with `job`, name an unknown backend, or disagree with each other;
    /// [`StateError::IncompleteCheckpoint`] if the checkpoint's operator set is not
    /// exactly `restoring`, or a listed operator has no metadata object, or its object
    /// carries no operator header or one naming a different operator; alongside the
    /// storage failures reading the metadata can produce. Nothing is written or deleted in
    /// any case.
    pub async fn load_checkpoint_operators(
        role: &StorageProviderFor,
        job: StateBackendSelector,
        checkpoint: &CheckpointIdentity,
        restoring: &HashSet<&str>,
        metadata: &CheckpointMetadata,
    ) -> Result<Validated<RestorableCheckpoint>, StateError> {
        // Set arithmetic first: it needs no reads, and a checkpoint that cannot cover this
        // job's operators must be refused before even the preflight's own reads happen. The
        // token's own check runs it again over what was actually loaded, which is the copy
        // every operation below depends on.
        check_program_coverage(
            metadata.epoch,
            metadata.operator_ids.iter().map(String::as_str),
            restoring,
        )?;

        let mut loading: FuturesUnordered<_> = metadata
            .operator_ids
            .iter()
            .enumerate()
            .map(|(position, operator_id)| async move {
                let loaded = Self::load_operator_metadata(
                    role,
                    &metadata.job_id,
                    operator_id,
                    metadata.epoch,
                )
                .await?
                .ok_or_else(|| StateError::IncompleteCheckpoint {
                    epoch: metadata.epoch,
                    detail: format!(
                        "operator {operator_id} has no checkpoint metadata object, but the \
                         worker that builds it requires one"
                    ),
                })?;
                Ok::<_, StateError>((position, operator_id.clone(), loaded))
            })
            .collect();

        let mut loaded = Vec::with_capacity(metadata.operator_ids.len());
        while let Some(operator) = loading.next().await {
            loaded.push(operator?);
        }
        drop(loading);
        loaded.sort_by_key(|(position, _, _)| *position);

        // An operator that had no object never reaches here, so "every listed operator has
        // one" is a property of having built the value at all. What is left to check — the
        // headers, the exact coverage, and every table config — is the token's job.
        Validated::validate(
            RestorableCheckpoint::new(
                CheckpointIdentity::claimed_by(metadata),
                loaded
                    .into_iter()
                    .map(|(_, operator_id, loaded)| (operator_id, loaded))
                    .collect(),
            ),
            RestoringProgram {
                job,
                restoring,
                expected: checkpoint,
            },
        )
    }

    /// Compacts a whole checkpoint, one operator at a time, after the whole checkpoint has
    /// been validated
    ///
    /// `job` is the state backend the job selected. Every operator's checkpoint metadata is
    /// loaded and checked against it — concurrently, in one pass — before the first operator
    /// is compacted, so a mismatch in the last operator cannot leave earlier operators
    /// already rewritten. The loaded metadata is then compacted directly, so this reads each
    /// operator's metadata exactly once; the cost of the preflight is holding one checkpoint's
    /// operator metadata in memory rather than one operator's.
    ///
    /// Results are returned in `operator_ids` order for the caller to publish, which is what
    /// keeps a worker from being told about compacted data for a checkpoint that is about to
    /// be rejected.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::StateBackendError`] if any operator's table configs disagree with
    /// `job` — in which case nothing has been compacted — alongside the storage and table
    /// failures compaction can otherwise produce.
    pub async fn compact_checkpoint(
        role: &StorageProviderFor,
        job: StateBackendSelector,
        job_id: Arc<String>,
        operator_ids: Vec<String>,
        epoch: u32,
    ) -> Result<Vec<(String, HashMap<String, TableCheckpointMetadata>)>, StateError> {
        let mut loading: FuturesUnordered<_> = operator_ids
            .iter()
            .enumerate()
            .map(|(position, operator_id)| {
                let job_id = Arc::clone(&job_id);
                async move {
                    let metadata = Self::load_operator_metadata(role, &job_id, operator_id, epoch)
                        .await?
                        .expect("expect operator metadata to still be present");
                    Ok::<_, StateError>((position, operator_id.clone(), metadata))
                }
            })
            .collect();

        let mut loaded = Vec::with_capacity(operator_ids.len());
        while let Some(operator) = loading.next().await {
            loaded.push(operator?);
        }
        drop(loading);
        loaded.sort_by_key(|(position, _, _)| *position);

        let compacting: HashSet<&str> = operator_ids.iter().map(String::as_str).collect();
        // The identity the caller asked for. Compaction is the call site with the most to lose
        // from a header nothing checked: `compact_loaded_operator` hands the header to the
        // table implementations, and the expiring-table compactor builds the path of every
        // file it writes out of the header's `job_id`, `operator_id` and `epoch`.
        let expected = CheckpointIdentity::new(job_id.as_str(), epoch);
        let checkpoint = Validated::validate(
            RestorableCheckpoint::new(
                expected.clone(),
                loaded
                    .into_iter()
                    .map(|(_, operator_id, metadata)| (operator_id, metadata))
                    .collect(),
            ),
            RestoringProgram {
                job,
                restoring: &compacting,
                expected: &expected,
            },
        )?;

        Self::compact_validated_checkpoint(role, checkpoint).await
    }

    /// Compacts every operator of a checkpoint that has already been checked as a whole.
    ///
    /// This is the destructive half of compaction, and it takes only the token: there is no
    /// spelling of this call that names an operator nothing vouched for (design item
    /// M11.D39c). It consumes the token rather than borrowing it, because the metadata the
    /// check ran over is the metadata that is compacted — re-reading it would pay twice for
    /// the same bytes and would let a different object be rewritten than the one that was
    /// checked.
    ///
    /// Results come back in the order the token holds its operators, which is the order the
    /// caller asked for, so the caller can publish them to workers in that order.
    ///
    /// # Errors
    ///
    /// Returns the storage and table failures compaction can produce; a selector
    /// disagreement cannot reach here, because it is what stops the token existing.
    pub async fn compact_validated_checkpoint(
        role: &StorageProviderFor,
        checkpoint: Validated<RestorableCheckpoint>,
    ) -> Result<Vec<(String, HashMap<String, TableCheckpointMetadata>)>, StateError> {
        let operators = checkpoint.into_inner().into_operators();

        let mut compacted = Vec::with_capacity(operators.len());
        for (operator_id, metadata) in operators {
            compacted.push((
                operator_id,
                Self::compact_loaded_operator(role, metadata).await?,
            ));
        }

        Ok(compacted)
    }

    /// Compacts one operator's already-loaded, already-validated checkpoint metadata.
    ///
    /// Takes the metadata by value rather than re-reading it, so the whole-checkpoint
    /// preflight in [`ParquetBackend::compact_checkpoint`] costs no extra reads. Private,
    /// and reachable only from [`ParquetBackend::compact_validated_checkpoint`], which is
    /// what makes "already validated" true rather than hoped for.
    async fn compact_loaded_operator(
        role: &StorageProviderFor,
        operator_checkpoint_metadata: OperatorCheckpointMetadata,
    ) -> Result<HashMap<String, TableCheckpointMetadata>, StateError> {
        let min_files_to_compact = config().pipeline.compaction.checkpoints_to_compact as usize;

        let storage_provider = get_storage_provider(role).await?;
        let compaction_config = CompactionConfig {
            compact_generations: vec![0].into_iter().collect(),
            min_compaction_epochs: min_files_to_compact,
            storage_provider: Arc::clone(&storage_provider),
            file_path_layout: CheckpointFilePathLayout::Legacy,
        };
        let operator_metadata = operator_checkpoint_metadata.operator_metadata.unwrap();

        let mut result = HashMap::new();

        for (table, table_metadata) in operator_checkpoint_metadata.table_checkpoint_metadata {
            let table_config = operator_checkpoint_metadata
                .table_configs
                .get(&table)
                .unwrap()
                .clone();
            if let Some(compacted_metadata) = match table_metadata.table_type() {
                rpc::TableEnum::MissingTableType => {
                    return Err(StateError::Other {
                        table: table.clone(),
                        error: "should have table type".to_string(),
                    });
                }
                rpc::TableEnum::GlobalKeyValue => {
                    GlobalKeyedTable::compact_data(
                        table_config,
                        &compaction_config,
                        &operator_metadata,
                        table_metadata,
                    )
                    .await?
                }
                rpc::TableEnum::ExpiringKeyedTimeTable => {
                    ExpiringTimeKeyTable::compact_data(
                        table_config,
                        &compaction_config,
                        &operator_metadata,
                        table_metadata,
                    )
                    .await?
                }
            } {
                result.insert(table, compacted_metadata);
            }
        }
        Ok(result)
    }

    /// Reads one operator's share of a cleanup: the epoch being kept and every epoch being
    /// dropped, without checking or deleting anything.
    ///
    /// Every epoch is read, including ones whose object is already gone — recorded as
    /// absent rather than skipped — because the check that follows is a claim about the
    /// whole range and cannot tell "collected and absent" from "never collected" otherwise.
    /// The dropped epochs may predate a restart onto another backend, which is why they are
    /// part of the object being checked at all: a job must never have another backend's
    /// files deleted by this one's file layout.
    ///
    /// # Errors
    ///
    /// Returns the storage failures reading metadata can produce. Nothing is checked here,
    /// so no selector disagreement is reported; that is
    /// [`CheckpointCleanup`]'s whole-object check.
    async fn collect_operator_cleanup(
        role: &StorageProviderFor,
        job_id: String,
        operator_id: String,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> Result<OperatorCleanup, StateError> {
        let retained = Self::load_operator_metadata(role, &job_id, &operator_id, new_min_epoch)
            .await?
            .expect("expect new_min_epoch metadata to still be present");

        let mut dropped = Vec::with_capacity(new_min_epoch.saturating_sub(old_min_epoch) as usize);
        for epoch_to_remove in old_min_epoch..new_min_epoch {
            dropped.push((
                epoch_to_remove,
                Self::load_operator_metadata(role, &job_id, &operator_id, epoch_to_remove).await?,
            ));
        }

        Ok(OperatorCleanup::new(operator_id, retained, dropped))
    }

    /// The files one operator's dropped epochs reference and its retained epoch does not.
    ///
    /// Reachable only from a [`ValidatedOperatorCleanup`], which in turn comes only from a
    /// checked [`CheckpointCleanup`] — so the set of files a job is about to lose is never
    /// derived from an object nothing vouched for (design item M11.D39c).
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Other`] if a table has checkpoint metadata but no table config,
    /// or a config that states no table type: either leaves nothing that can say which files
    /// the table references, and treating that as "references nothing" would delete files the
    /// retained epoch still needs.
    fn files_no_longer_referenced(
        operator: &ValidatedOperatorCleanup<'_>,
    ) -> Result<HashSet<String>, StateError> {
        let mut paths_to_keep = HashSet::new();
        for table in operator.retained_tables()? {
            paths_to_keep.extend(Self::table_files_to_keep(table)?);
        }

        let mut files_to_delete = HashSet::new();
        for table in operator.dropped_tables()? {
            for file in Self::table_files_to_keep(table)? {
                if !paths_to_keep.contains(&file) {
                    files_to_delete.insert(file);
                }
            }
        }

        Ok(files_to_delete)
    }

    /// Dispatches one validated table to the table type that knows how to read its files.
    fn table_files_to_keep(table: ValidatedTable<'_>) -> Result<HashSet<String>, StateError> {
        match table.config().table_type() {
            rpc::TableEnum::MissingTableType => Err(StateError::Other {
                table: table.name().to_string(),
                error: format!(
                    "the table config for {} states no table type, so the files it \
                     references cannot be determined",
                    table.name()
                ),
            }),
            rpc::TableEnum::GlobalKeyValue => GlobalKeyedTable::files_to_keep(table),
            rpc::TableEnum::ExpiringKeyedTimeTable => ExpiringTimeKeyTable::files_to_keep(table),
        }
    }
}

#[derive(Debug)]
pub struct ParquetStats {
    pub max_timestamp: SystemTime,
    pub min_routing_key: u64,
    pub max_routing_key: u64,
}

impl Default for ParquetStats {
    fn default() -> Self {
        Self {
            max_timestamp: SystemTime::UNIX_EPOCH,
            min_routing_key: u64::MAX,
            max_routing_key: u64::MIN,
        }
    }
}

impl ParquetStats {
    pub fn merge(&mut self, other: ParquetStats) {
        self.max_timestamp = self.max_timestamp.max(other.max_timestamp);
        self.min_routing_key = self.min_routing_key.min(other.min_routing_key);
        self.max_routing_key = self.max_routing_key.max(other.max_routing_key);
    }
}

#[cfg(test)]
mod tests {
    use super::{HashSet, ParquetBackend, base_path, metadata_path, operator_path};
    use crate::validated::{
        CheckpointCleanup, CheckpointIdentity, CheckpointMetadataWrite, CleanupScope,
        OperatorCleanup, RestorableCheckpoint, RestoringProgram,
    };
    use crate::{BackingStore, StorageProviderFor, get_storage_provider};
    use arroyo_rpc::errors::StateError;
    use arroyo_rpc::grpc::rpc::{
        CheckpointMetadata, GlobalKeyedTableConfig, GlobalKeyedTableTaskCheckpointMetadata,
        OperatorCheckpointMetadata, OperatorMetadata, TableCheckpointMetadata, TableConfig,
        TableEnum,
    };
    use arroyo_rpc::state_backend::validated::Validated;
    use arroyo_rpc::state_backend::{StateBackendError, StateBackendSelector};
    use arroyo_storage::StorageProvider;
    use prost::Message;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// A checkpoint store on the local filesystem, so a cleanup's deletions are observable as
    /// files that are or are not still there.
    ///
    /// `StorageProviderFor::Controller { storage_url: Some(..) }` is the one role that takes an
    /// explicit URL, which keeps each test in its own directory and off the process-wide
    /// `config().checkpoint_url`.
    struct LocalCheckpointStore {
        role: StorageProviderFor,
        directory: String,
    }

    impl LocalCheckpointStore {
        fn new(name: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let unique = format!(
                "{}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                COUNTER.fetch_add(1, Ordering::Relaxed),
            );
            let directory = std::env::temp_dir()
                .join(format!("arroyo-state-{name}-{unique}"))
                .to_string_lossy()
                .into_owned();
            std::fs::create_dir_all(&directory).unwrap();

            Self {
                role: StorageProviderFor::Controller {
                    storage_url: Some(format!("file://{directory}")),
                },
                directory,
            }
        }

        async fn provider(&self) -> Arc<StorageProvider> {
            get_storage_provider(&self.role).await.unwrap()
        }

        async fn put(&self, path: &str, bytes: Vec<u8>) {
            self.provider().await.put(path, bytes).await.unwrap();
        }

        async fn exists(&self, path: &str) -> bool {
            self.provider()
                .await
                .get_if_present(path)
                .await
                .unwrap()
                .is_some()
        }
    }

    impl Drop for LocalCheckpointStore {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }

    const JOB_ID: &str = "job_1";

    fn table_config(state_backend: &str) -> TableConfig {
        TableConfig {
            table_type: TableEnum::GlobalKeyValue as i32,
            config: GlobalKeyedTableConfig {
                table_name: "g".to_string(),
                description: "global".to_string(),
                uses_two_phase_commit: false,
            }
            .encode_to_vec(),
            state_version: 0,
            state_backend: state_backend.to_string(),
        }
    }

    /// One operator's checkpoint metadata for `epoch`, whose single global table references
    /// `files` and whose table config states `state_backend`.
    fn operator_metadata(
        operator_id: &str,
        epoch: u32,
        state_backend: &str,
        files: &[String],
    ) -> OperatorCheckpointMetadata {
        OperatorCheckpointMetadata {
            operator_metadata: Some(OperatorMetadata {
                job_id: JOB_ID.to_string(),
                operator_id: operator_id.to_string(),
                epoch,
                min_watermark: None,
                max_watermark: None,
                parallelism: 1,
            }),
            start_time: 0,
            finish_time: 0,
            table_checkpoint_metadata: HashMap::from([(
                "g".to_string(),
                TableCheckpointMetadata {
                    table_type: TableEnum::GlobalKeyValue as i32,
                    data: GlobalKeyedTableTaskCheckpointMetadata {
                        files: files.to_vec(),
                        commit_data_by_subtask: HashMap::new(),
                    }
                    .encode_to_vec(),
                },
            )]),
            table_configs: HashMap::from([("g".to_string(), table_config(state_backend))]),
        }
    }

    /// Writes one operator's epoch: its data file plus the metadata that references it.
    async fn write_epoch(
        store: &LocalCheckpointStore,
        operator_id: &str,
        epoch: u32,
        state_backend: &str,
    ) -> String {
        let file = format!("{}/{operator_id}-{epoch}.parquet", JOB_ID);
        store.put(&file, b"data".to_vec()).await;
        ParquetBackend::write_operator_checkpoint_metadata(
            &store.role,
            operator_metadata(
                operator_id,
                epoch,
                state_backend,
                std::slice::from_ref(&file),
            ),
        )
        .await
        .unwrap();
        file
    }

    fn checkpoint_metadata(operator_ids: &[&str], epoch: u32) -> CheckpointMetadata {
        CheckpointMetadata {
            job_id: JOB_ID.to_string(),
            epoch,
            min_epoch: 0,
            start_time: 0,
            finish_time: 0,
            operator_ids: operator_ids.iter().map(|id| id.to_string()).collect(),
        }
    }

    /// The operators the job's workers would build — what `LogicalProgram::tasks_per_operator`
    /// supplies in production, stated directly here.
    fn restoring<'a>(operator_ids: &[&'a str]) -> HashSet<&'a str> {
        operator_ids.iter().copied().collect()
    }

    /// The checkpoint a caller asked storage for: this fixture's job, at `epoch`.
    fn asked_for(epoch: u32) -> CheckpointIdentity {
        CheckpointIdentity::new(JOB_ID, epoch)
    }

    /// The compatibility direction of the cleanup guard: a job whose checkpoints predate the
    /// selector (every table config empty) still collects its own old epochs. The guard must
    /// not strand a legacy deployment's state.
    #[tokio::test]
    async fn cleanup_still_collects_a_legacy_all_parquet_checkpoint() {
        let store = LocalCheckpointStore::new("legacy-cleanup");
        let dropped = write_epoch(&store, "node_1", 0, "").await;
        let kept = write_epoch(&store, "node_1", 1, "").await;

        ParquetBackend::cleanup_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            asked_for(1),
            checkpoint_metadata(&["node_1"], 1),
            0,
            1,
        )
        .await
        .unwrap();

        assert!(!store.exists(&dropped).await, "old epoch's file was kept");
        assert!(
            store.exists(&kept).await,
            "retained epoch's file was deleted"
        );
        assert!(
            !store
                .exists(&metadata_path(&operator_path(JOB_ID, 0, "node_1")))
                .await
        );
    }

    /// Finding 1, the cross-epoch half: `cleanup_operator` used to validate and delete one
    /// epoch at a time, so a mismatch discovered in a later epoch arrived after the earlier
    /// epochs' files were gone.
    ///
    /// Epoch 0 agrees and epoch 1 does not, and both are being dropped. Deleting inline leaves
    /// epoch 0's file deleted; classifying the whole operator first leaves everything.
    #[tokio::test]
    async fn a_mismatching_epoch_stops_cleanup_before_an_earlier_epoch_is_deleted() {
        let store = LocalCheckpointStore::new("epoch-order");
        let agreeing = write_epoch(&store, "node_1", 0, "parquet").await;
        let mismatching = write_epoch(&store, "node_1", 1, "stateengine").await;
        let kept = write_epoch(&store, "node_1", 2, "parquet").await;

        let err = ParquetBackend::cleanup_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            asked_for(2),
            checkpoint_metadata(&["node_1"], 2),
            0,
            2,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, StateError::StateBackendError(_)),
            "expected a typed selector rejection, got {err:?}"
        );

        assert!(
            store.exists(&agreeing).await,
            "the agreeing epoch's file was deleted before the mismatch was found"
        );
        assert!(store.exists(&mismatching).await);
        assert!(store.exists(&kept).await);
        for epoch in 0..2 {
            assert!(
                store
                    .exists(&metadata_path(&operator_path(JOB_ID, epoch, "node_1")))
                    .await,
                "epoch {epoch} metadata was deleted"
            );
        }
    }

    /// Finding 1, the cross-operator half: operator cleanups run concurrently and each used to
    /// delete as soon as it had validated itself, so an operator that failed could not stop one
    /// that had already finished.
    ///
    /// `node_1` agrees throughout. `node_2` agrees at the epoch being retained and disagrees
    /// only at the epoch being dropped, so both operators read the same number of objects
    /// before one deletes and the other refuses — the interleaving that used to lose the race.
    /// Whichever order the two futures complete in, nothing may be deleted and the
    /// checkpoint's `min_epoch` may not be advanced.
    #[tokio::test]
    async fn a_mismatching_operator_stops_cleanup_before_another_operator_is_deleted() {
        let store = LocalCheckpointStore::new("operator-order");
        let agreeing_dropped = write_epoch(&store, "node_1", 0, "parquet").await;
        write_epoch(&store, "node_1", 1, "parquet").await;
        let mismatching_dropped = write_epoch(&store, "node_2", 0, "stateengine").await;
        write_epoch(&store, "node_2", 1, "parquet").await;

        let err = ParquetBackend::cleanup_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            asked_for(1),
            checkpoint_metadata(&["node_1", "node_2"], 1),
            0,
            1,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, StateError::StateBackendError(_)),
            "expected a typed selector rejection, got {err:?}"
        );

        assert!(
            store.exists(&agreeing_dropped).await,
            "the agreeing operator's file was deleted before the other operator was checked"
        );
        assert!(store.exists(&mismatching_dropped).await);
        assert!(
            store
                .exists(&metadata_path(&operator_path(JOB_ID, 0, "node_1")))
                .await,
            "the agreeing operator's dropped metadata was deleted"
        );
        // The checkpoint's own metadata is neither dropped nor rewritten with the new min epoch.
        assert!(!store.exists(&metadata_path(&base_path(JOB_ID, 1))).await);
    }

    /// A checkpoint that names the same operator twice is refused before anything is deleted.
    ///
    /// **PR #160 review comment `5384611151`.** The cleanup token compared its collected
    /// operators with the checkpoint's list by length and position only — and the collected
    /// operators are built *from* that list, so `["node_1", "node_1"]` zips against itself and
    /// agrees on both. The uniqueness rule existed, but in
    /// `CheckpointMetadataWrite::check_whole`: the write that *ends* the cleanup, and therefore
    /// ran after every deletion. The cleanup failed with the data files and the dropped
    /// per-epoch metadata already gone and `min_epoch` never advanced, leaving the top-level
    /// object pointing at epochs that no longer existed.
    ///
    /// The error is asserted by the cleanup scope's own wording rather than the metadata
    /// write's, because both now refuse this input and the two closures are separate: moving
    /// the entitlement ahead of the deletions stops the *effect*, and the token's own
    /// uniqueness check is what makes the token honest about what it authorized.
    #[tokio::test]
    async fn a_duplicate_operator_stops_cleanup_before_anything_is_deleted() {
        let store = LocalCheckpointStore::new("duplicate-operator");
        let dropped = write_epoch(&store, "node_1", 0, "parquet").await;
        let kept = write_epoch(&store, "node_1", 1, "parquet").await;

        let err = ParquetBackend::cleanup_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            asked_for(1),
            checkpoint_metadata(&["node_1", "node_1"], 1),
            0,
            1,
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("lists an operator more than once"),
            "the cleanup token must refuse the duplicate itself, not leave it to the write \
             that ends the cleanup: {err}"
        );
        assert!(
            store.exists(&dropped).await,
            "the dropped epoch's file was deleted before the duplicate was found"
        );
        assert!(store.exists(&kept).await);
        assert!(
            store
                .exists(&metadata_path(&operator_path(JOB_ID, 0, "node_1")))
                .await,
            "the dropped epoch's metadata was deleted before the duplicate was found"
        );
        assert!(
            !store.exists(&metadata_path(&base_path(JOB_ID, 1))).await,
            "and `min_epoch` was never advanced, so nothing points at the deleted epochs"
        );
    }

    /// Nothing irreversible in `cleanup_checkpoint` runs before the token that authorizes it.
    ///
    /// **The row that would have found the ordering half of PR #160 review comment
    /// `5384611151` first**, stated as the property rather than as one input's behaviour. The
    /// duplicate was only the input that reached it: `CheckpointMetadataWrite::check_whole`
    /// also compares the identity the metadata claims against the one the cleanup earned, and
    /// every refusal it can make arrived, before this change, after the deletions.
    ///
    /// A source-order pin, and its gap is that: it reads the order of two statements, not the
    /// order two operations execute in. What makes that worth having anyway is that
    /// `cleanup_checkpoint` is one linear function with no branch between the two — which is
    /// itself the property being pinned, since a delete moved into a conditional above the
    /// entitlement would fail this row.
    #[test]
    fn a_cleanup_proves_its_write_entitlement_before_it_deletes_anything() {
        let source = include_str!("parquet.rs");
        let at = source
            .find("async fn cleanup_checkpoint(")
            .expect("the cleanup this row is about");
        let body = &source[at..];
        let end = body
            .find("\n    }")
            .expect("a function that ends at its impl block's indentation");
        let body = &body[..end];

        let entitlement = body
            .find("CheckpointMetadataWrite::after_cleanup(")
            .expect("the token that authorizes the write ending the cleanup");
        let first_delete = body
            .find("delete_if_present(")
            .expect("the deletions the cleanup performs");
        assert!(
            entitlement < first_delete,
            "the entitlement for the write that ends the cleanup is proven at byte \
             {entitlement} of this function and the first deletion runs at byte \
             {first_delete}. Everything that can refuse must stand above everything that \
             cannot be undone: a refusal found after the deletions leaves the checkpoint's \
             top-level metadata naming epochs that no longer exist"
        );
    }

    /// Finding 4: compaction validated one operator at a time, so a mismatch in a later
    /// operator arrived after earlier operators had been compacted and their workers told.
    ///
    /// `node_1` agrees with the job but its table has no table type, which is a failure
    /// compaction raises only once it is *compacting* that operator — the same trick the merge
    /// guard's test uses to make "the operation started" observable. `node_2` disagrees.
    /// Compacting operator by operator therefore fails inside `node_1`; validating the whole
    /// checkpoint first fails on `node_2` before `node_1` is touched.
    #[tokio::test]
    async fn a_mismatching_operator_stops_compaction_before_an_earlier_one_is_compacted() {
        let store = LocalCheckpointStore::new("compaction-order");

        let mut untyped = operator_metadata("node_1", 1, "parquet", &[]);
        untyped
            .table_checkpoint_metadata
            .get_mut("g")
            .unwrap()
            .table_type = TableEnum::MissingTableType as i32;
        ParquetBackend::write_operator_checkpoint_metadata(&store.role, untyped)
            .await
            .unwrap();
        ParquetBackend::write_operator_checkpoint_metadata(
            &store.role,
            operator_metadata("node_2", 1, "stateengine", &[]),
        )
        .await
        .unwrap();

        let err = ParquetBackend::compact_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            Arc::new(JOB_ID.to_string()),
            vec!["node_1".to_string(), "node_2".to_string()],
            1,
        )
        .await
        .unwrap_err();

        // Compacting node_1 first would have produced `StateError::Other { .. }` from its
        // missing table type instead.
        assert!(
            matches!(
                err,
                StateError::StateBackendError(StateBackendError::CheckpointMismatch { .. })
            ),
            "expected node_2's selector to be rejected before node_1 was compacted, got {err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("node_2"), "{message}");
        assert!(message.contains("stateengine"), "{message}");
    }

    /// The restore preflight refuses the whole checkpoint, and reads nothing more than the
    /// operators it was asked about. `node_1` agrees with the job and `node_2` does not, so
    /// a preflight that stopped at the first disagreement would still have handed `node_1`
    /// back to be restored.
    #[tokio::test]
    async fn loading_a_checkpoints_operators_refuses_one_a_later_operator_disagrees_with() {
        let store = LocalCheckpointStore::new("restore-preflight-mixed");
        write_epoch(&store, "node_1", 1, "parquet").await;
        write_epoch(&store, "node_2", 1, "stateengine").await;

        let err = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &asked_for(1),
            &restoring(&["node_1", "node_2"]),
            &checkpoint_metadata(&["node_1", "node_2"], 1),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                StateError::StateBackendError(StateBackendError::CheckpointMismatch { .. })
            ),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("node_2"), "{message}");
        assert!(message.contains("stateengine"), "{message}");
    }

    /// The compatibility direction of the restore preflight, plus the property the caller
    /// depends on to avoid reading twice: a legacy all-parquet checkpoint is accepted, and
    /// every operator comes back in `operator_ids` order with the metadata that was read.
    #[tokio::test]
    async fn loading_a_legacy_all_parquet_checkpoints_operators_returns_them_in_order() {
        let store = LocalCheckpointStore::new("restore-preflight-legacy");
        write_epoch(&store, "node_1", 1, "").await;
        write_epoch(&store, "node_2", 1, "").await;

        let preflight = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &asked_for(1),
            &restoring(&["node_1", "node_2"]),
            &checkpoint_metadata(&["node_1", "node_2"], 1),
        )
        .await
        .unwrap();

        let operators = preflight.get().operators();
        let ids: Vec<&str> = operators.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, vec!["node_1", "node_2"]);
        assert_eq!(operators[0].1.table_configs["g"].state_backend, "");
        assert_eq!(operators[1].1.table_configs["g"].state_backend, "");
    }

    /// A checkpoint that lists an operator it has no metadata object for is refused,
    /// rather than reported as absent for the caller to interpret.
    ///
    /// This is the case the preflight used to pass through: `needs_commits` was the only
    /// consumer that rejected `None`, so a `ready` checkpoint reached the caller — and the
    /// caller's metadata rewrite — with an operator that no worker could build. Both
    /// operators here do the same work up to the divergence: each is listed, and `node_1`
    /// has a complete object, so `node_2` cannot fail for an unrelated reason first.
    #[tokio::test]
    async fn loading_a_checkpoints_operators_refuses_one_whose_object_is_absent() {
        let store = LocalCheckpointStore::new("restore-preflight-absent");
        write_epoch(&store, "node_1", 1, "").await;

        let err = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &asked_for(1),
            &restoring(&["node_1", "node_2"]),
            &checkpoint_metadata(&["node_1", "node_2"], 1),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, StateError::IncompleteCheckpoint { epoch: 1, .. }),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("node_2"), "{message}");
    }

    /// An operator the job's program contains but the checkpoint does not list is refused,
    /// even though nothing about the operators the checkpoint *does* list is wrong.
    ///
    /// The checkpoint's list is not the set that matters: the workers build every operator
    /// of the current program and each loads its own metadata, so an operator missing from
    /// the list is one the preflight would never read and a worker would still require.
    #[tokio::test]
    async fn loading_a_checkpoints_operators_refuses_a_program_operator_it_omits() {
        let store = LocalCheckpointStore::new("restore-preflight-omitted");
        write_epoch(&store, "node_1", 1, "").await;
        write_epoch(&store, "node_2", 1, "stateengine").await;

        let err = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &asked_for(1),
            &restoring(&["node_1", "node_2"]),
            // the checkpoint lists only node_1, so node_2's disagreeing metadata is never
            // read by the preflight — but node_2 is built by every worker all the same
            &checkpoint_metadata(&["node_1"], 1),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, StateError::IncompleteCheckpoint { epoch: 1, .. }),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("node_2"), "{message}");
    }

    /// A listed operator whose metadata object carries no operator header — or one naming
    /// a different operator — is refused as well.
    ///
    /// The object existing is not enough: `TableManager::load` reads the restored
    /// watermark straight out of that header and unwraps it, so a headerless object
    /// panics a worker. Round 3 required the object; this requires it to describe the
    /// operator it was loaded for, which is the same rule the leader path's manifest
    /// coverage check applies.
    #[tokio::test]
    async fn loading_a_checkpoints_operators_refuses_one_whose_object_has_no_header() {
        let store = LocalCheckpointStore::new("restore-preflight-headerless");
        write_epoch(&store, "node_1", 1, "").await;

        // Written straight to the path the loader reads: the writer refuses to produce a
        // headerless object, which is exactly why one can only arrive from outside.
        let mut headerless = operator_metadata("node_2", 1, "", &[]);
        headerless.operator_metadata = None;
        store
            .put(
                &metadata_path(&operator_path(JOB_ID, 1, "node_2")),
                headerless.encode_to_vec(),
            )
            .await;

        let err = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &asked_for(1),
            &restoring(&["node_1", "node_2"]),
            &checkpoint_metadata(&["node_1", "node_2"], 1),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, StateError::IncompleteCheckpoint { epoch: 1, .. }),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("node_2"), "{message}");
        assert!(message.contains("no operator header"), "{message}");
    }

    /// The other half of "exact coverage": a checkpoint listing an operator the current
    /// program does not contain belongs to a different program and is refused too.
    #[tokio::test]
    async fn loading_a_checkpoints_operators_refuses_one_the_program_does_not_contain() {
        let store = LocalCheckpointStore::new("restore-preflight-extra");
        write_epoch(&store, "node_1", 1, "").await;
        write_epoch(&store, "node_2", 1, "").await;

        let err = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &asked_for(1),
            &restoring(&["node_1"]),
            &checkpoint_metadata(&["node_1", "node_2"], 1),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, StateError::IncompleteCheckpoint { epoch: 1, .. }),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("node_2"), "{message}");
    }

    /// The compatibility direction of the compaction guard: a legacy all-parquet checkpoint
    /// still compacts, and its results come back in the order the caller asked for them so the
    /// caller can publish them to workers.
    #[tokio::test]
    async fn compaction_still_runs_for_a_legacy_all_parquet_checkpoint() {
        let store = LocalCheckpointStore::new("legacy-compaction");
        write_epoch(&store, "node_1", 1, "").await;
        write_epoch(&store, "node_2", 1, "").await;

        let compacted = ParquetBackend::compact_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            Arc::new(JOB_ID.to_string()),
            vec!["node_1".to_string(), "node_2".to_string()],
            1,
        )
        .await
        .unwrap();

        let operator_ids: Vec<&str> = compacted.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(operator_ids, vec!["node_1", "node_2"]);
        // Global keyed tables never produce compacted metadata; what matters here is that the
        // whole checkpoint was accepted rather than refused.
        assert!(compacted.iter().all(|(_, tables)| tables.is_empty()));
    }

    /// One operator's collected epochs, all agreeing with the job, as a whole cleanup
    /// records them.
    fn agreeing_operator(operator_id: &str, dropped: &[u32]) -> OperatorCleanup {
        OperatorCleanup::new(
            operator_id.to_string(),
            operator_metadata(operator_id, 1, "parquet", &[]),
            dropped
                .iter()
                .map(|epoch| {
                    (
                        *epoch,
                        Some(operator_metadata(operator_id, *epoch, "parquet", &[])),
                    )
                })
                .collect(),
        )
    }

    /// D96 row 1 (round 1): a cleanup's first delete is reachable only through a token for
    /// the *whole* checkpoint, so no operator's files can go before every operator and every
    /// epoch has been accounted for.
    ///
    /// Two halves. The behavioural half puts the disagreement in the last operator's oldest
    /// dropped epoch, with both operators reading the same number of objects before they
    /// diverge — the interleaving PR-#157 round 1 lost, and the one an "equalize the work"
    /// setup is needed to expose at all. Nothing may be deleted and the checkpoint's
    /// `min_epoch` may not be advanced.
    ///
    /// The token half is what makes that structural rather than remembered: a cleanup that
    /// left an epoch of the range uncollected, or an operator of the checkpoint uncollected,
    /// cannot be checked either — so a caller cannot earn the argument the deletion needs by
    /// collecting less than the whole thing and then deleting all of it.
    #[tokio::test]
    async fn cleanup_requires_validated_whole_checkpoint() {
        let store = LocalCheckpointStore::new("cleanup-token");
        let agreeing_dropped = write_epoch(&store, "node_1", 0, "parquet").await;
        let agreeing_kept = write_epoch(&store, "node_1", 1, "parquet").await;
        let mismatching_dropped = write_epoch(&store, "node_2", 0, "stateengine").await;
        write_epoch(&store, "node_2", 1, "parquet").await;

        let err = ParquetBackend::cleanup_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            asked_for(1),
            checkpoint_metadata(&["node_1", "node_2"], 1),
            0,
            1,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                StateError::StateBackendError(StateBackendError::CheckpointMismatch { .. })
            ),
            "expected a typed selector rejection, got {err:?}"
        );
        assert!(
            store.exists(&agreeing_dropped).await,
            "the agreeing operator's dropped file went before the whole checkpoint was checked"
        );
        assert!(store.exists(&agreeing_kept).await);
        assert!(store.exists(&mismatching_dropped).await);
        assert!(
            store
                .exists(&metadata_path(&operator_path(JOB_ID, 0, "node_1")))
                .await,
            "the agreeing operator's dropped metadata was deleted"
        );
        assert!(
            !store.exists(&metadata_path(&base_path(JOB_ID, 1))).await,
            "the checkpoint's min_epoch was rewritten"
        );

        // The token half. `operator_ids` is the checkpoint's own list, which is what the
        // collected cleanup has to match.
        let operator_ids = vec!["node_1".to_string(), "node_2".to_string()];
        let expected = asked_for(1);
        let scope = CleanupScope {
            job: StateBackendSelector::Parquet,
            operator_ids: &operator_ids,
            expected: &expected,
        };

        Validated::validate(
            CheckpointCleanup::new(
                asked_for(1),
                0,
                1,
                vec![
                    agreeing_operator("node_1", &[0]),
                    agreeing_operator("node_2", &[0]),
                ],
            ),
            scope,
        )
        .expect("a whole, agreeing cleanup is exactly what a token is for");

        let missing_epoch = Validated::validate(
            CheckpointCleanup::new(
                asked_for(1),
                0,
                1,
                vec![
                    // node_1's dropped epoch 0 was never collected, so nothing has looked at
                    // the object whose files this cleanup would delete.
                    agreeing_operator("node_1", &[]),
                    agreeing_operator("node_2", &[0]),
                ],
            ),
            scope,
        )
        .unwrap_err();
        assert!(
            missing_epoch.to_string().contains("node_1"),
            "{missing_epoch}"
        );

        let missing_operator = Validated::validate(
            CheckpointCleanup::new(asked_for(1), 0, 1, vec![agreeing_operator("node_1", &[0])]),
            scope,
        )
        .unwrap_err();
        assert!(
            missing_operator.to_string().contains("node_2"),
            "{missing_operator}"
        );
    }

    /// D96 row 3 (round 1): the destructive half of compaction takes only a token for the
    /// whole job, so no operator is rewritten before every operator has been checked.
    ///
    /// The behavioural half gives both operators the same work to do before they diverge:
    /// `node_1` agrees with the job but its table states no table type, which compaction
    /// raises only once it is *compacting* that operator, and `node_2` disagrees. The
    /// control at the end is what makes that evidence rather than coincidence — with
    /// `node_2` agreeing, the same call does reach `node_1` and does report its missing
    /// table type.
    ///
    /// The token half pins the coverage claim that a per-operator check cannot make: a set
    /// that is missing one of the job's operators is refused, so `compact_validated_checkpoint`
    /// has no argument for a partial job.
    #[tokio::test]
    async fn compaction_after_whole_job_validation() {
        let store = LocalCheckpointStore::new("compaction-token");

        let untyped = |operator_id: &str| {
            let mut untyped = operator_metadata(operator_id, 1, "parquet", &[]);
            untyped
                .table_checkpoint_metadata
                .get_mut("g")
                .unwrap()
                .table_type = TableEnum::MissingTableType as i32;
            untyped
        };
        ParquetBackend::write_operator_checkpoint_metadata(&store.role, untyped("node_1"))
            .await
            .unwrap();
        ParquetBackend::write_operator_checkpoint_metadata(
            &store.role,
            operator_metadata("node_2", 1, "stateengine", &[]),
        )
        .await
        .unwrap();

        let err = ParquetBackend::compact_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            Arc::new(JOB_ID.to_string()),
            vec!["node_1".to_string(), "node_2".to_string()],
            1,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                StateError::StateBackendError(StateBackendError::CheckpointMismatch { .. })
            ),
            "expected node_2's selector to be refused before node_1 was compacted, got {err:?}"
        );

        // The control: with node_2 agreeing, the whole job passes the check and node_1 is
        // reached — which is what proves the run above stopped before compaction, rather
        // than node_1 simply never being compactable.
        ParquetBackend::write_operator_checkpoint_metadata(
            &store.role,
            operator_metadata("node_2", 1, "parquet", &[]),
        )
        .await
        .unwrap();
        let reached = ParquetBackend::compact_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            Arc::new(JOB_ID.to_string()),
            vec!["node_1".to_string(), "node_2".to_string()],
            1,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(reached, StateError::Other { .. }),
            "expected node_1's missing table type once the whole job passed, got {reached:?}"
        );

        // The token half: a set that is not the whole job cannot be checked at all.
        let partial = Validated::validate(
            RestorableCheckpoint::new(
                asked_for(1),
                vec![(
                    "node_1".to_string(),
                    operator_metadata("node_1", 1, "parquet", &[]),
                )],
            ),
            RestoringProgram {
                job: StateBackendSelector::Parquet,
                restoring: &restoring(&["node_1", "node_2"]),
                expected: &asked_for(1),
            },
        )
        .unwrap_err();
        assert!(
            matches!(partial, StateError::IncompleteCheckpoint { epoch: 1, .. }),
            "{partial:?}"
        );
        assert!(partial.to_string().contains("node_2"), "{partial}");
    }

    /// One operator's metadata object, written straight to the path the loader reads, headed
    /// for whatever job, operator and epoch the caller names.
    ///
    /// `write_operator_checkpoint_metadata` derives its path *from* the header, so it cannot
    /// produce a misplaced object; this is how one arrives from outside, which is exactly the
    /// situation the identity checks exist for.
    async fn put_foreign_object(
        store: &LocalCheckpointStore,
        at_epoch: u32,
        at_operator: &str,
        header_job: &str,
        header_operator: &str,
        header_epoch: u32,
    ) {
        let mut object = operator_metadata(header_operator, header_epoch, "parquet", &[]);
        object.operator_metadata.as_mut().unwrap().job_id = header_job.to_string();
        store
            .put(
                &metadata_path(&operator_path(JOB_ID, at_epoch, at_operator)),
                object.encode_to_vec(),
            )
            .await;
    }

    /// A persisted operator object headed for another checkpoint is refused even though it is
    /// stored where this checkpoint's object belongs (PR #160 review round 6, finding 3).
    ///
    /// This is the reviewer's attack, written the way the reviewer described it. `OperatorMetadata`
    /// carries `job_id`, `operator_id` and `epoch`; only the second was ever compared. The token
    /// that results is consumed by compaction, and the expiring-table compactor builds the path of
    /// every file it writes out of all three — so an object planted under the expected path
    /// redirected writes *after* "whole-object validation" had passed it.
    ///
    /// Each field is varied independently, from an object that agrees in the other two, because
    /// that is what a row varying only the top-level identity could never see.
    #[tokio::test]
    async fn a_foreign_operator_object_under_the_expected_path_is_refused() {
        let store = LocalCheckpointStore::new("foreign-header");

        // Right operator, right epoch, another job's id.
        put_foreign_object(&store, 1, "node_1", "job_2", "node_1", 1).await;
        let other_job = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &asked_for(1),
            &restoring(&["node_1"]),
            &checkpoint_metadata(&["node_1"], 1),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(other_job, StateError::IncompleteCheckpoint { epoch: 1, .. }),
            "{other_job:?}"
        );
        assert!(
            other_job.to_string().contains("job \"job_2\""),
            "{other_job}"
        );

        // Right job, right operator, another epoch — the pair no operator set can tell apart.
        put_foreign_object(&store, 1, "node_1", JOB_ID, "node_1", 7).await;
        let other_epoch = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &asked_for(1),
            &restoring(&["node_1"]),
            &checkpoint_metadata(&["node_1"], 1),
        )
        .await
        .unwrap_err();
        assert!(other_epoch.to_string().contains("epoch 7"), "{other_epoch}");

        // And compaction, which is the HIGH-impact consumer of the same token: it refuses the
        // object rather than compacting under the identity the object claims.
        let compaction = ParquetBackend::compact_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            Arc::new(JOB_ID.to_string()),
            vec!["node_1".to_string()],
            1,
        )
        .await
        .unwrap_err();
        assert!(compaction.to_string().contains("epoch 7"), "{compaction}");

        // The control: the same call with the object headed as it should be goes through, so
        // the refusals above are the header and nothing else.
        write_epoch(&store, "node_1", 1, "parquet").await;
        ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &asked_for(1),
            &restoring(&["node_1"]),
            &checkpoint_metadata(&["node_1"], 1),
        )
        .await
        .expect("an object headed as this checkpoint's is this checkpoint's");
        ParquetBackend::compact_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            Arc::new(JOB_ID.to_string()),
            vec!["node_1".to_string()],
            1,
        )
        .await
        .expect("and it compacts");
    }

    /// The token itself refuses a foreign header, not only the loader that produced it
    /// (PR #160 review round 6, finding 3).
    ///
    /// The loader check above covers every reader of an operator object in one place. It is
    /// not the guarantee, though: it is a property of *where the value came from*, and a
    /// validate-then-act token that leaned on that would be back to caller provenance. So the
    /// whole-object check makes the same statement about the value it actually holds, and this
    /// row is that check run on a hand-built value the loader never touched.
    #[test]
    fn a_restorable_checkpoints_own_check_refuses_a_foreign_header() {
        let mut foreign = operator_metadata("node_1", 1, "parquet", &[]);
        foreign.operator_metadata.as_mut().unwrap().job_id = "job_2".to_string();

        let refused = Validated::validate(
            RestorableCheckpoint::new(asked_for(1), vec![("node_1".to_string(), foreign)]),
            RestoringProgram {
                job: StateBackendSelector::Parquet,
                restoring: &restoring(&["node_1"]),
                expected: &asked_for(1),
            },
        )
        .unwrap_err();
        assert!(refused.to_string().contains("job_2"), "{refused}");

        // And a token collected for one checkpoint cannot be declared to be about another,
        // which is the drift that produced the identity-free entitlements round 5 left behind.
        let misdeclared = Validated::validate(
            RestorableCheckpoint::new(
                asked_for(1),
                vec![(
                    "node_1".to_string(),
                    operator_metadata("node_1", 1, "parquet", &[]),
                )],
            ),
            RestoringProgram {
                job: StateBackendSelector::Parquet,
                restoring: &restoring(&["node_1"]),
                expected: &CheckpointIdentity::new(JOB_ID, 2),
            },
        )
        .unwrap_err();
        assert!(
            misdeclared.to_string().contains("different checkpoint"),
            "{misdeclared}"
        );
    }

    /// A checkpoint's own top-level metadata object has to be the checkpoint that was asked
    /// for (PR #160 review round 6, finding 3).
    ///
    /// One level above the operator headers, and the same shape: the path is built from the
    /// caller's job id and epoch, the object carries its own copy of both, and every operator
    /// object the preflight then reads is read from the object's copies. The worker's own
    /// restore (`Program::from_logical`) reads this object with no token at all, which is why
    /// the check lives in the loader as well as in the tokens downstream of it.
    #[tokio::test]
    async fn a_top_level_metadata_object_claiming_another_checkpoint_is_refused() {
        let store = LocalCheckpointStore::new("foreign-checkpoint");
        write_epoch(&store, "node_1", 1, "parquet").await;

        let mut foreign = checkpoint_metadata(&["node_1"], 1);
        foreign.job_id = "job_2".to_string();
        store
            .put(
                &metadata_path(&base_path(JOB_ID, 1)),
                foreign.encode_to_vec(),
            )
            .await;

        let err = ParquetBackend::load_checkpoint_metadata(&store.role, JOB_ID, 1)
            .await
            .unwrap_err();
        assert!(
            matches!(err, StateError::IncompleteCheckpoint { epoch: 1, .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("job_2"), "{err}");

        // The epoch half, varied on its own.
        let mut wrong_epoch = checkpoint_metadata(&["node_1"], 1);
        wrong_epoch.epoch = 6;
        store
            .put(
                &metadata_path(&base_path(JOB_ID, 1)),
                wrong_epoch.encode_to_vec(),
            )
            .await;
        let err = ParquetBackend::load_checkpoint_metadata(&store.role, JOB_ID, 1)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("epoch 6"), "{err}");

        // The control.
        store
            .put(
                &metadata_path(&base_path(JOB_ID, 1)),
                checkpoint_metadata(&["node_1"], 1).encode_to_vec(),
            )
            .await;
        ParquetBackend::load_checkpoint_metadata(&store.role, JOB_ID, 1)
            .await
            .expect("the object at this checkpoint's path, claiming to be it, is it");
    }

    /// A cleanup's objects are bound to the epoch each of them was collected at — and a
    /// cleanup legitimately spans epochs, so the rule is per object rather than per call
    /// (PR #160 review round 6, findings 2 and 3).
    ///
    /// The positive half is the one that has to come first, because it is what a rule of
    /// "every object carries the checkpoint's epoch" would have broken: a real cleanup holds
    /// three different epochs at once — the top-level metadata object at the job's current
    /// epoch, the retained operator objects at `new_min_epoch`, and one dropped object per
    /// epoch in `old_min_epoch..new_min_epoch`.
    #[tokio::test]
    async fn a_cleanup_binds_each_object_to_the_epoch_it_was_collected_at() {
        let store = LocalCheckpointStore::new("cleanup-identity");
        let dropped_0 = write_epoch(&store, "node_1", 0, "parquet").await;
        let dropped_1 = write_epoch(&store, "node_1", 1, "parquet").await;
        let retained = write_epoch(&store, "node_1", 2, "parquet").await;

        // The positive: three epochs, three different expectations, all satisfied.
        ParquetBackend::cleanup_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            asked_for(3),
            checkpoint_metadata(&["node_1"], 3),
            0,
            2,
        )
        .await
        .expect("a cleanup spanning a retained epoch and a range of dropped ones is ordinary");
        assert!(!store.exists(&dropped_0).await);
        assert!(!store.exists(&dropped_1).await);
        assert!(store.exists(&retained).await);

        // The negative, on a fresh store: the retained object headed for the epoch being
        // dropped rather than the epoch being kept. Nothing may be deleted.
        let store = LocalCheckpointStore::new("cleanup-identity-negative");
        let dropped = write_epoch(&store, "node_1", 0, "parquet").await;
        put_foreign_object(&store, 1, "node_1", JOB_ID, "node_1", 0).await;

        let err = ParquetBackend::cleanup_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            asked_for(2),
            checkpoint_metadata(&["node_1"], 2),
            0,
            1,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("epoch 0"), "{err}");
        assert!(
            store.exists(&dropped).await,
            "a refused cleanup deleted a file"
        );

        // And the caller's own declaration is compared too: a cleanup told it is collecting
        // one checkpoint while the object says another is refused before the first delete.
        let store = LocalCheckpointStore::new("cleanup-identity-declared");
        let dropped = write_epoch(&store, "node_1", 0, "parquet").await;
        write_epoch(&store, "node_1", 1, "parquet").await;
        let err = ParquetBackend::cleanup_checkpoint(
            &store.role,
            StateBackendSelector::Parquet,
            CheckpointIdentity::new("job_2", 2),
            checkpoint_metadata(&["node_1"], 2),
            0,
            1,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("different checkpoint"), "{err}");
        assert!(store.exists(&dropped).await);
    }

    /// Every metadata-rewrite entitlement is bound to the checkpoint it is evidence of, not
    /// just the completion one (PR #160 review round 6, finding 2).
    ///
    /// Review round 5 bound the completion entitlement and left `after_restore_preflight` and
    /// `after_cleanup` supplying `None`, so those two writes were checked by operator set
    /// alone — and every epoch of a job has the same operator set. Both tokens here are real,
    /// derived from objects on disk rather than assembled by hand, which is what makes the
    /// refusals below about the binding rather than about the fixture.
    #[tokio::test]
    async fn a_restore_and_a_cleanup_entitle_only_their_own_checkpoint() {
        let store = LocalCheckpointStore::new("entitlement-identity");
        write_epoch(&store, "node_1", 1, "parquet").await;

        let preflight = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &asked_for(1),
            &restoring(&["node_1"]),
            &checkpoint_metadata(&["node_1"], 1),
        )
        .await
        .unwrap();

        // Its own checkpoint: entitled.
        Validated::validate(
            CheckpointMetadataWrite::after_restore_preflight(
                checkpoint_metadata(&["node_1"], 1),
                &preflight,
            ),
            (),
        )
        .expect("the checkpoint the preflight covered is the one it entitles");

        // Another epoch of the same job, same operator set — the case an operator-set check
        // cannot see.
        let other_epoch = Validated::validate(
            CheckpointMetadataWrite::after_restore_preflight(
                checkpoint_metadata(&["node_1"], 9),
                &preflight,
            ),
            (),
        )
        .unwrap_err();
        assert!(
            other_epoch.to_string().contains("different checkpoint"),
            "{other_epoch}"
        );

        // Another job, same operator set.
        let mut other_job = checkpoint_metadata(&["node_1"], 1);
        other_job.job_id = "job_2".to_string();
        let refused = Validated::validate(
            CheckpointMetadataWrite::after_restore_preflight(other_job, &preflight),
            (),
        )
        .unwrap_err();
        assert!(refused.to_string().contains("job_2"), "{refused}");

        // The cleanup entitlement, likewise, against a token derived from a real collection.
        let operator_ids = vec!["node_1".to_string()];
        let expected = asked_for(2);
        let cleanup = Validated::validate(
            CheckpointCleanup::new(
                asked_for(2),
                0,
                1,
                vec![OperatorCleanup::new(
                    "node_1".to_string(),
                    operator_metadata("node_1", 1, "parquet", &[]),
                    vec![(0, Some(operator_metadata("node_1", 0, "parquet", &[])))],
                )],
            ),
            CleanupScope {
                job: StateBackendSelector::Parquet,
                operator_ids: &operator_ids,
                expected: &expected,
            },
        )
        .expect("a whole, agreeing, correctly headed cleanup");

        Validated::validate(
            CheckpointMetadataWrite::after_cleanup(checkpoint_metadata(&["node_1"], 2), &cleanup),
            (),
        )
        .expect("the checkpoint the cleanup collected is the one its rewrite entitles");

        let wrong_checkpoint = Validated::validate(
            CheckpointMetadataWrite::after_cleanup(checkpoint_metadata(&["node_1"], 3), &cleanup),
            (),
        )
        .unwrap_err();
        assert!(
            wrong_checkpoint
                .to_string()
                .contains("different checkpoint"),
            "{wrong_checkpoint}"
        );
    }

    /// D96 row 4 (round 2): rewriting a checkpoint's top-level metadata takes only a token,
    /// and the token ties the operators the metadata names to the ones a whole-checkpoint
    /// preflight actually covered.
    ///
    /// This is the shape of the finding: a `ready` checkpoint reached this write with no
    /// operator preflighted at all, because the preflight was something the caller ran on
    /// the way past rather than something the write required. There is now no spelling of
    /// the write that takes the metadata on its own, and the only token a restore can build
    /// is derived from the preflight's own result.
    #[tokio::test]
    async fn metadata_write_requires_validated_operators() {
        let store = LocalCheckpointStore::new("metadata-write-token");
        write_epoch(&store, "node_1", 1, "").await;
        write_epoch(&store, "node_2", 1, "").await;

        let metadata = checkpoint_metadata(&["node_1", "node_2"], 1);
        let preflight = ParquetBackend::load_checkpoint_operators(
            &store.role,
            StateBackendSelector::Parquet,
            &asked_for(1),
            &restoring(&["node_1", "node_2"]),
            &metadata,
        )
        .await
        .unwrap();

        // Metadata naming an operator the preflight did not cover is not writable...
        let extra = Validated::validate(
            CheckpointMetadataWrite::after_restore_preflight(
                checkpoint_metadata(&["node_1", "node_2", "node_3"], 1),
                &preflight,
            ),
            (),
        )
        .unwrap_err();
        assert!(extra.to_string().contains("node_3"), "{extra}");

        // ...and neither is metadata that quietly drops one the preflight did cover, which
        // is what a checkpoint whose restore skipped an operator would produce.
        let dropped = Validated::validate(
            CheckpointMetadataWrite::after_restore_preflight(
                checkpoint_metadata(&["node_1"], 1),
                &preflight,
            ),
            (),
        )
        .unwrap_err();
        assert!(dropped.to_string().contains("node_2"), "{dropped}");

        assert!(
            !store.exists(&metadata_path(&base_path(JOB_ID, 1))).await,
            "a refused write still reached storage"
        );

        // The write the preflight does entitle goes through.
        let write = Validated::validate(
            CheckpointMetadataWrite::after_restore_preflight(metadata, &preflight),
            (),
        )
        .unwrap();
        ParquetBackend::write_checkpoint_metadata(&store.role, write)
            .await
            .unwrap();
        assert!(store.exists(&metadata_path(&base_path(JOB_ID, 1))).await);
    }
}
