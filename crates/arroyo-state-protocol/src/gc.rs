use crate::ProtocolPaths;
use crate::store::{ProtocolStore, StoreError, read_protobuf};
use crate::types::{CheckpointRef, Epoch, Generation, ProtocolError};
use crate::validated::{CheckpointHistory, CollectingJob, validate_history};
use arroyo_rpc::grpc::rpc::{
    CheckpointManifest, ExpiringKeyedTimeTableCheckpointMetadata,
    GlobalKeyedTableTaskCheckpointMetadata, TableCheckpointMetadata, TableEnum,
};
use arroyo_rpc::state_backend::validated::Validated;
use arroyo_rpc::state_backend::{StateBackendSelector, validate_restored_manifest};
use futures::{TryStreamExt, stream};
use prost::Message;
use std::collections::HashSet;
use std::path::Path;
use tracing::debug;

const MAX_CONCURRENT_DELETES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct CheckpointOwner {
    pub generation: Generation,
    pub epoch: Epoch,
}

/// Deletes the leader-mode checkpoint history below `new_min_epoch`.
///
/// `job` is the state backend the job selected, handed in by the caller rather than read from
/// anywhere ambient. Every manifest reachable from `head` — retained and expiring alike — is
/// validated against it while the history is classified, which is strictly before the first
/// delete: the reachable chain is the only thing that names the files this function removes, and
/// a chain some other backend wrote must not have its files named by this one's traversal.
///
/// Classification and deletion are two functions with a
/// [`Validated<CheckpointHistory>`] between them (design item M11.D39c). The traversal checks
/// each manifest as it reads it, which is what keeps its own parent links and file names from
/// being taken out of bytes nobody vouched for; the token is the separate claim about the
/// *whole* chain, and [`delete_classified_history`] takes nothing else — so a caller cannot
/// arrive at the deletion with a chain that was only partly classified.
///
/// Since review round 7 of PR #160 the token's check also binds each reached manifest to the
/// reference it was read from. `paths` is what makes that possible and is why it is handed to
/// [`validate_history`] rather than only to [`delete_classified_history`]: every object this
/// function removes is built from a generation and an epoch that came out of the manifest's own
/// bytes, so a misplaced or corrupt object would otherwise aim the deletes at a checkpoint
/// nobody asked about.
///
/// # Errors
///
/// Returns [`StoreError::StateBackend`] if any reachable manifest disagrees with `job`,
/// [`StoreError::Protocol`] if one is not the checkpoint its reference names, or
/// [`StoreError::IncompleteManifest`] if an entry of one is headed for another checkpoint — in
/// each case nothing has been deleted — alongside the storage and protocol failures traversal
/// can otherwise produce.
pub async fn cleanup_leader_checkpoints<S>(
    store: &S,
    paths: &ProtocolPaths,
    job: StateBackendSelector,
    head: CheckpointRef,
    new_min_epoch: Epoch,
) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let cleanup = validate_history(
        classify_checkpoint_history(store, job, head, new_min_epoch).await?,
        CollectingJob {
            state_backend: job,
            paths,
        },
    )?;

    delete_classified_history(store, paths, &cleanup).await
}

/// Deletes everything a checked history classified as collectable.
///
/// Takes only the token: this is the irreversible half, and there is no spelling of it that
/// names a checkpoint chain nothing validated.
///
/// # Errors
///
/// Returns the storage failures deleting can produce. Deletions run in an order that cannot
/// strand a still-reachable checkpoint if one of them fails part way.
pub async fn delete_classified_history<S>(
    store: &S,
    paths: &ProtocolPaths,
    cleanup: &Validated<CheckpointHistory>,
) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let cleanup = cleanup.get();

    let data_directories: HashSet<_> = cleanup
        .data_files()
        .iter()
        .filter_map(|f| Path::new(f.as_str()).parent().and_then(|p| p.to_str()))
        .map(|f| f.to_string())
        .collect();
    let checkpoint_directories: HashSet<_> = data_directories
        .iter()
        .filter_map(|directory| Path::new(directory).parent().and_then(|p| p.to_str()))
        .map(|directory| directory.to_string())
        .collect();

    let mut objects = cleanup.data_files().to_vec();
    for checkpoint in cleanup.old_checkpoints() {
        objects.push(paths.committed_marker(checkpoint.generation, checkpoint.epoch));
        objects.push(paths.epoch_record(checkpoint.epoch));
    }

    delete_objects(store, objects).await?;

    for directory in data_directories {
        store.delete_directory(&directory).await;
    }

    // A carried-forward file may live under a checkpoint whose manifest was deleted by an earlier
    // GC pass. Retry its checkpoint directory now that the file and its operator directory are
    // gone. Local storage removes only empty directories; object stores treat this as a no-op.
    for directory in checkpoint_directories {
        store.delete_directory(&directory).await;
    }

    // Traversal records checkpoints newest-to-oldest. Delete manifests in reverse so a failed
    // cleanup cannot create a gap that makes still-reachable older checkpoints undiscoverable.
    for c in cleanup.old_checkpoints().iter().rev() {
        debug!(
            generation = c.generation.0,
            epoch = c.epoch.0,
            "cleaning checkpoint"
        );

        let path = paths.checkpoint_manifest(c.generation, c.epoch);
        store.delete_object(&path).await?;

        let dir = paths.checkpoint_dir(c.generation, c.epoch);
        store.delete_directory(dir.as_str()).await;
    }

    Ok(())
}

async fn delete_objects<S>(store: &S, objects: Vec<CheckpointRef>) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
{
    stream::iter(objects.into_iter().map(Ok::<_, StoreError>))
        .try_for_each_concurrent(MAX_CONCURRENT_DELETES, |object| async move {
            store.delete_object(&object).await
        })
        .await
}

/// Traverses the reachable checkpoint history and classifies all files before deleting anything.
///
/// Data files referenced by retained checkpoints are protected even if an older checkpoint also
/// references them. This intentionally buffers deduplicated candidate and protected file refs for
/// the reachable chain, but not full manifests. That memory cost is required to safely handle
/// cumulative table metadata such as expiring keyed-time tables.
///
/// Each manifest is checked against `job` at the point it is read, before its files are added to
/// either set — so the selector check costs no extra reads, and a disagreement anywhere in the
/// chain aborts classification with an empty plan rather than a partial one. That per-object
/// check is the untrusted-bytes guard: the parent link that continues the traversal and the file
/// refs that become delete candidates both come out of the manifest, so who wrote it has to be
/// known before any of that is interpreted. The claim about the *chain* is separate, and is what
/// [`CheckpointHistory`]'s own check makes; the reduced evidence collected here is what lets it
/// be made without buffering the file lists a second time.
async fn classify_checkpoint_history<S>(
    store: &S,
    job: StateBackendSelector,
    current: CheckpointRef,
    new_min_epoch: Epoch,
) -> Result<CheckpointHistory, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let mut head = true;
    let mut history = CheckpointHistory::default();
    let mut old_checkpoints = vec![];
    let mut candidate_files = HashSet::new();
    let mut protected_files = HashSet::new();
    let mut seen = HashSet::new();
    let mut next = Some(current);

    while let Some(checkpoint_ref) = next {
        // TODO: use a metadata cache here so we're not re-reading checkpoints we've just written
        let Some(manifest): Option<CheckpointManifest> =
            read_protobuf(store, &checkpoint_ref).await?
        else {
            if head {
                return Err(StoreError::ExpectedObjectMissing {
                    path: checkpoint_ref,
                });
            }
            break;
        };

        // Untrusted bytes: this manifest was just read back from storage, and everything below
        // — the parent link that continues the traversal and the file refs that become delete
        // candidates — comes out of it. Check who wrote it before any of that is used.
        validate_restored_manifest(job, &manifest)?;

        let owner = CheckpointOwner {
            generation: Generation(manifest.generation),
            epoch: Epoch(manifest.epoch),
        };

        if head && owner.epoch < new_min_epoch {
            return Err(ProtocolError::CheckpointGcMinEpochBeyondHead {
                head_epoch: owner.epoch,
                new_min_epoch,
            }
            .into());
        }

        head = false;

        if !seen.insert(owner) {
            return Err(ProtocolError::CheckpointCycle {
                generation: owner.generation,
                epoch: owner.epoch,
            }
            .into());
        }

        let files = checkpoint_data_files(&checkpoint_ref, &manifest)?;
        if manifest.epoch < *new_min_epoch {
            old_checkpoints.push(owner);
            candidate_files.extend(files);
        } else {
            protected_files.extend(files);
        }

        history.reached(checkpoint_ref.clone(), &manifest);

        next = manifest
            .parent_checkpoint_ref
            .map(CheckpointRef::new)
            .transpose()?;
    }

    candidate_files.retain(|file| !protected_files.contains(file));

    history.classified(old_checkpoints, candidate_files.into_iter().collect());
    Ok(history)
}

fn checkpoint_data_files(
    manifest_path: &CheckpointRef,
    checkpoint: &CheckpointManifest,
) -> Result<Vec<CheckpointRef>, StoreError> {
    let mut files = vec![];
    for operator in &checkpoint.operators {
        for (table_name, metadata) in &operator.table_checkpoint_metadata {
            let op_metadata =
                operator
                    .operator_metadata
                    .as_ref()
                    .ok_or_else(|| StoreError::InvalidProtobuf {
                        path: manifest_path.clone(),
                        msg: "missing OperatorMetadata field".to_string(),
                    })?;

            table_checkpoint_data_files(
                &op_metadata.operator_id,
                table_name,
                manifest_path,
                metadata,
                &mut files,
            )?;
        }
    }

    Ok(files)
}

fn table_checkpoint_data_files(
    operator_id: &str,
    table_name: &str,
    metadata_path: &CheckpointRef,
    metadata: &TableCheckpointMetadata,
    files: &mut Vec<CheckpointRef>,
) -> Result<(), StoreError> {
    match metadata.table_type() {
        TableEnum::MissingTableType => {
            return Err(StoreError::InvalidProtobuf {
                path: metadata_path.clone(),
                msg: format!(
                    "table metadata for operator '{}' table '{}' is missing table type",
                    operator_id, table_name
                ),
            });
        }
        TableEnum::GlobalKeyValue => {
            let metadata = GlobalKeyedTableTaskCheckpointMetadata::decode(metadata.data.as_slice())
                .map_err(|e| StoreError::DecodeProtobuf {
                    path: metadata_path.clone(),
                    source: e,
                })?;

            for file in metadata.files {
                files.push(CheckpointRef::new(file.clone())?);
            }
        }
        TableEnum::ExpiringKeyedTimeTable => {
            let metadata =
                ExpiringKeyedTimeTableCheckpointMetadata::decode(metadata.data.as_slice())
                    .map_err(|e| StoreError::DecodeProtobuf {
                        path: metadata_path.clone(),
                        source: e,
                    })?;

            for file in metadata.files {
                files.push(CheckpointRef::new(file.file)?);
            }
        }
    }

    Ok(())
}
