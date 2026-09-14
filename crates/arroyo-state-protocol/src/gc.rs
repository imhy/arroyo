use crate::ProtocolPaths;
use crate::gc::liveness::{CheckpointLiveness, LivenessRefusal};
use crate::store::{ProtocolStore, StoreError, read_protobuf};
use crate::types::{CheckpointRef, Epoch, Generation, ProtocolError};
use crate::validated::{CheckpointHistory, CollectingJob, validate_history};
use arroyo_rpc::grpc::rpc::{CheckpointManifest, TableCheckpointMetadata, TableEnum};
use arroyo_rpc::state_backend::validate_restored_manifest;
use arroyo_rpc::state_backend::validated::Validated;
use futures::{TryStreamExt, stream};
use std::collections::HashSet;
use std::path::Path;
use tracing::debug;

pub mod liveness;

const MAX_CONCURRENT_DELETES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct CheckpointOwner {
    pub generation: Generation,
    pub epoch: Epoch,
}

/// Deletes the leader-mode checkpoint history below `new_min_epoch`.
///
/// `job` is the selected backend's [`CheckpointLiveness`] implementation, handed in by the
/// caller rather than read from anywhere ambient. It is one value doing two jobs, and
/// deliberately so. Its [`CheckpointLiveness::state_backend`] is the backend every manifest
/// reachable from `head` — retained and expiring alike — is validated against while the history
/// is classified, which is strictly before the first delete: the reachable chain is the only
/// thing that names the files this function removes, and a chain some other backend wrote must
/// not have its files named by this one's traversal. Its
/// [`table_data_files`](CheckpointLiveness::table_data_files) is what reads those names out of
/// each table's backend-specific payload.
///
/// Until M11.T09-S5 those were two arguments — a selector, and this function's own decode of
/// the parquet payload formats — and nothing related them. A job on any other backend passed
/// the selector check and then had its manifests read through parquet's decoder, whose answer
/// for a format it was not written for is not an error but a *different* file list; the
/// protected set and the candidate set are both built from that answer, so a name dropped from
/// a retained checkpoint's list while an expiring one still carries it is a live file
/// classified as collectable. Taking the resolver instead of the selector is what makes the
/// backend that decodes and the backend the history is checked against the same statement.
///
/// Classification and deletion are two functions with a
/// [`Validated<CheckpointHistory>`] between them (design item M11.D39c). The traversal checks
/// each manifest as it reads it, which is what keeps its own parent links and file names from
/// being taken out of bytes nobody vouched for; the token is the separate claim about the
/// *whole* chain, and [`delete_classified_history`] takes nothing else — so a caller cannot
/// arrive at the deletion with a chain that was only partly classified.
///
/// Since review round 7 of PR #160 the token's check also binds each reached manifest to the
/// reference it was read from: every object this function removes is built from a generation
/// and an epoch that came out of the manifest's own bytes, so a misplaced or corrupt object
/// would otherwise aim the deletes at a checkpoint nobody asked about.
///
/// `paths` enters here and nowhere else. The traversal records it on the history, the check
/// asks its questions against it, and the deletion builds every object it removes from it —
/// one value, stated once by the caller who is asking for this job to be collected.
///
/// # Errors
///
/// Returns [`StoreError::StateBackend`] if any reachable manifest disagrees with `job`,
/// [`StoreError::Protocol`] if one is not the checkpoint its reference names,
/// [`StoreError::IncompleteManifest`] if an entry of one is headed for another checkpoint, or
/// [`StoreError::UnresolvedTableLiveness`] if `job` cannot name the files one of its tables
/// keeps alive — in each case nothing has been deleted — alongside the storage and protocol
/// failures traversal can otherwise produce.
pub async fn cleanup_leader_checkpoints<S>(
    store: &S,
    paths: &ProtocolPaths,
    job: &dyn CheckpointLiveness,
    head: CheckpointRef,
    new_min_epoch: Epoch,
) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let cleanup = validate_history(
        classify_checkpoint_history(store, paths, job, head, new_min_epoch).await?,
        CollectingJob {
            state_backend: job.state_backend(),
        },
    )?;

    delete_classified_history(store, &cleanup).await
}

/// Deletes everything a checked history classified as collectable.
///
/// Takes only the token, and that is now literally true of the namespace as well: this is the
/// irreversible half, so there is no spelling of it that names a checkpoint chain nothing
/// validated, and none that names a *place* nothing validated either. The data files come out
/// of the checked manifests; the manifests, committed markers, epoch records and checkpoint
/// directories come out of [`CheckpointHistory::paths`], which is the same path builder the
/// check used. A `ProtocolPaths` argument here would be an unchecked second identity aiming
/// an irreversible effect — see [`CheckpointHistory::paths`] for the review finding that
/// closed it.
///
/// # Errors
///
/// Returns the storage failures deleting can produce. Deletions run in an order that cannot
/// strand a still-reachable checkpoint if one of them fails part way.
pub async fn delete_classified_history<S>(
    store: &S,
    cleanup: &Validated<CheckpointHistory>,
) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let cleanup = cleanup.get();
    let paths = cleanup.paths();

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
/// Each manifest is checked against `job`'s backend at the point it is read, before its files
/// are added to either set — so the selector check costs no extra reads, and a disagreement
/// anywhere in the chain aborts classification with an empty plan rather than a partial one.
/// That per-object check is the untrusted-bytes guard: the parent link that continues the
/// traversal and the file refs that become delete candidates both come out of the manifest, so
/// who wrote it has to be known before any of that is interpreted. The claim about the *chain*
/// is separate, and is what [`CheckpointHistory`]'s own check makes; the reduced evidence
/// collected here is what lets it be made without buffering the file lists a second time.
///
/// `job` is also what reads the file names out of each table's payload, and a refusal from it
/// returns from here — before a [`CheckpointHistory`] exists, and therefore before anything the
/// deletion could take.
///
/// `paths` is recorded on the history as it opens rather than used to read: the traversal
/// starts at `current` and then follows the parent links it finds. Recording it here is what
/// makes the namespace the caller asked to collect the same namespace the check asks its
/// questions against and the deletion removes objects from.
async fn classify_checkpoint_history<S>(
    store: &S,
    paths: &ProtocolPaths,
    job: &dyn CheckpointLiveness,
    current: CheckpointRef,
    new_min_epoch: Epoch,
) -> Result<CheckpointHistory, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let mut head = true;
    let mut history = CheckpointHistory::new(paths.clone());
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
        validate_restored_manifest(job.state_backend(), &manifest)?;

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

        let files = checkpoint_data_files(&checkpoint_ref, &manifest, job)?;
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

/// Every data file the tables of one checkpoint's manifest name.
///
/// The walk over operators and tables is this crate's — it is what decides which manifest
/// entries are asked about at all — and the reading of each entry's payload is `liveness`'s.
fn checkpoint_data_files(
    manifest_path: &CheckpointRef,
    checkpoint: &CheckpointManifest,
    liveness: &dyn CheckpointLiveness,
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
                liveness,
                &mut files,
            )?;
        }
    }

    Ok(files)
}

/// Appends the data files one table's checkpoint metadata names.
///
/// Two of the three steps here are this crate's and stay here. A metadata entry that states no
/// table type is refused before any implementation is consulted, so "nothing named a kind"
/// cannot be mistaken for "nothing named a file" — that refusal is the same message, naming the
/// same operator and table, that this function has produced since leader GC existed. And every
/// name that comes back is validated by [`CheckpointRef::new`] before it can join a delete plan,
/// which is where a path's shape is checked; whether it is also in this job's namespace is
/// [`CheckpointHistory`]'s check, over the whole plan.
///
/// The middle step — what the payload's bytes mean — is `liveness`'s, and a refusal from it
/// propagates out of the classification rather than shortening the list.
fn table_checkpoint_data_files(
    operator_id: &str,
    table_name: &str,
    metadata_path: &CheckpointRef,
    metadata: &TableCheckpointMetadata,
    liveness: &dyn CheckpointLiveness,
    files: &mut Vec<CheckpointRef>,
) -> Result<(), StoreError> {
    if metadata.table_type() == TableEnum::MissingTableType {
        return Err(StoreError::InvalidProtobuf {
            path: metadata_path.clone(),
            msg: format!(
                "table metadata for operator '{}' table '{}' is missing table type",
                operator_id, table_name
            ),
        });
    }

    let named = liveness
        .table_data_files(metadata)
        .map_err(|refusal| match refusal {
            // Undecodable bytes are reported exactly as this function has always reported
            // them: the payload is corrupt, which is a property of the object rather than of
            // which backend was asked.
            LivenessRefusal::UndecodablePayload { source, .. } => StoreError::DecodeProtobuf {
                path: metadata_path.clone(),
                source,
            },
            refusal @ (LivenessRefusal::UnservedTableKind { .. }
            | LivenessRefusal::WrongTableKind { .. }) => StoreError::UnresolvedTableLiveness {
                path: metadata_path.clone(),
                operator_id: operator_id.to_string(),
                table: table_name.to_string(),
                source: refusal,
            },
        })?;

    for file in named {
        files.push(CheckpointRef::new(file)?);
    }

    Ok(())
}
