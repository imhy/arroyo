//! Finding the checkpoint a new generation should recover from.
//!
//! This module is the *read* half of generation initialization: it walks back through the
//! previous generations' manifests, reads each candidate checkpoint together with the epoch
//! record and commit marker that say whether it is canonical, and turns what it found into a
//! [`GenerationRecovery`] — or into a candidate the caller still has to earn the right to make
//! canonical.
//!
//! # This module writes nothing
//!
//! That is the point of it being its own module, and it is not a convention: PR #160 review
//! round 8 found that an unclaimed candidate used to have its epoch record written *here*,
//! while resolving, and so became the canonical checkpoint of its epoch before
//! [`crate::workflow::initialize_generation`] had checked that this job could restore it at
//! all. An epoch record is immutable, so a candidate rejected a few lines later stayed
//! canonical for good and could orphan a valid checkpoint for the same epoch — a state no
//! later fence can repair.
//!
//! So resolution now reports [`GenerationResolution::ClaimRequired`] and returns
//! [`RecoverySearch::Unclaimed`] instead of claiming, and the claim happens on the far side of
//! the publication token, next to the two other objects that token gates.
//! `the_recovery_resolution_module_reaches_no_persistent_write` is a source pin over this file
//! naming every entry point in [`crate::store`] through which a byte can be written; adding
//! one here fails that row.

use crate::ProtocolPaths;
use crate::resolve::{ParentCheckpointStatus, ResolveDecision, ResolveFailure, resolve_candidate};
use crate::state::{CheckpointState, derive_checkpoint_state};
use crate::store::{ProtocolStore, StoreError, read_json, read_protobuf};
use crate::types::{
    CheckpointRef, Epoch, EpochRecord, Generation, GenerationManifest,
    checkpoint_parent_checkpoint_ref,
};
use arroyo_rpc::grpc::rpc::CheckpointManifest;

use super::{CommitPermit, GenerationRecovery, committed_marker_path};

/// Result of resolving a generation manifest candidate.
///
/// This is used both during recovery and when initializing a replacement
/// generation. A `ReplayCommit` result means callers may restore the checkpoint
/// only to replay external commit before normal execution continues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerationResolution {
    Ready {
        checkpoint_ref: CheckpointRef,
    },
    ReplayCommit {
        checkpoint_ref: CheckpointRef,
        commit_permit: CommitPermit,
    },
    /// The candidate exists and nothing owns its epoch yet, and this generation is the
    /// current one, so this generation is the one that may take it.
    ///
    /// Taking it is deliberately not done here. Writing the epoch record is what makes this
    /// checkpoint the canonical checkpoint of its epoch, permanently, so it is a publication
    /// effect and belongs after the candidate has been validated — see the module docs.
    ClaimRequired {
        checkpoint_ref: CheckpointRef,
    },
    StopOrphaned {
        canonical_ref: CheckpointRef,
    },
    Failed(ResolveFailure),
}

/// What searching the previous generations for a recovery point produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RecoverySearch {
    /// A checkpoint this generation may recover from, and how.
    Found(GenerationRecovery),
    /// A candidate that exists, is safe to recover from, and whose epoch nothing owns yet.
    ///
    /// The search stops here rather than claiming it. The caller reads the manifest, validates
    /// the publication it would commit this job to, and only then takes the epoch.
    Unclaimed { checkpoint_ref: CheckpointRef },
    /// The history the search reached is owned by another checkpoint.
    StopOrphaned { canonical_ref: CheckpointRef },
    /// No safe recovery point could be derived.
    Failed(ResolveFailure),
}

impl RecoverySearch {
    /// The checkpoint reference the search resolved, if it resolved one.
    ///
    /// This is what the publication's recovery manifest is read from, so it deliberately
    /// covers the unclaimed case too: a candidate has to be read and checked *before* it is
    /// claimed, which means the same reference feeds validation in both cases. The match is
    /// exhaustive so that a new variant has to answer this question rather than fall into a
    /// wildcard that says "nothing to recover from".
    pub(super) fn resolved_checkpoint_ref(&self) -> Option<&CheckpointRef> {
        match self {
            RecoverySearch::Found(GenerationRecovery::Ready { checkpoint_ref })
            | RecoverySearch::Found(GenerationRecovery::ReplayCommit { checkpoint_ref, .. })
            | RecoverySearch::Unclaimed { checkpoint_ref } => Some(checkpoint_ref),
            RecoverySearch::Found(GenerationRecovery::NoCheckpoint)
            | RecoverySearch::StopOrphaned { .. }
            | RecoverySearch::Failed(_) => None,
        }
    }
}

/// What resolving the canonical checkpoint of an epoch produced.
///
/// Deliberately narrower than [`RecoverySearch`]: it has no unclaimed variant, because the
/// canonical checkpoint of an epoch is by definition the checkpoint that already owns that
/// epoch's record. That is what bounds the redirect in
/// [`crate::workflow::initialize_generation`] — the one taken when a claim finds the epoch
/// already owned — to a single step, rather than a comment asserting that it terminates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CanonicalRecovery {
    Found(GenerationRecovery),
    StopOrphaned { canonical_ref: CheckpointRef },
    Failed(ResolveFailure),
}

impl From<CanonicalRecovery> for RecoverySearch {
    fn from(canonical: CanonicalRecovery) -> Self {
        match canonical {
            CanonicalRecovery::Found(recovery) => RecoverySearch::Found(recovery),
            CanonicalRecovery::StopOrphaned { canonical_ref } => {
                RecoverySearch::StopOrphaned { canonical_ref }
            }
            CanonicalRecovery::Failed(failure) => RecoverySearch::Failed(failure),
        }
    }
}

/// Walks back through the previous generations for a checkpoint this generation can recover
/// from.
pub(super) async fn find_recovery_checkpoint<S>(
    store: &S,
    paths: &ProtocolPaths,
    generation: Generation,
) -> Result<RecoverySearch, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let Some(previous_generation) = generation.0.checked_sub(1) else {
        return Ok(RecoverySearch::Found(GenerationRecovery::NoCheckpoint));
    };

    for previous_generation in (0..=previous_generation).rev() {
        let manifest_ref = paths.generation_manifest(Generation(previous_generation));
        let Some(manifest): Option<GenerationManifest> = read_json(store, &manifest_ref).await?
        else {
            continue;
        };

        match resolve_generation_manifest(store, &manifest, generation).await? {
            GenerationResolution::Ready { checkpoint_ref } => {
                return Ok(RecoverySearch::Found(GenerationRecovery::Ready {
                    checkpoint_ref,
                }));
            }
            GenerationResolution::ReplayCommit {
                checkpoint_ref,
                commit_permit,
            } => {
                return Ok(RecoverySearch::Found(GenerationRecovery::ReplayCommit {
                    checkpoint_ref,
                    commit_permit,
                }));
            }
            // Reported, not taken. The caller validates the candidate and then decides
            // whether this generation may own the epoch.
            GenerationResolution::ClaimRequired { checkpoint_ref } => {
                return Ok(RecoverySearch::Unclaimed { checkpoint_ref });
            }
            GenerationResolution::StopOrphaned { canonical_ref } => {
                return Ok(resolve_canonical_recovery_ref(store, paths, &canonical_ref)
                    .await?
                    .into());
            }
            GenerationResolution::Failed(
                ResolveFailure::NoCandidate
                | ResolveFailure::InvisibleBase
                | ResolveFailure::UnclaimedBase,
            ) => continue,
            GenerationResolution::Failed(failure) => {
                return Ok(RecoverySearch::Failed(failure));
            }
        }
    }

    Ok(RecoverySearch::Found(GenerationRecovery::NoCheckpoint))
}

/// Resolves the checkpoint that already owns an epoch into a recovery point.
///
/// Reached when a candidate lost its epoch to another checkpoint, either because the epoch was
/// already owned when the search read it or because the claim itself found it owned. The
/// checkpoint named here holds the epoch record, which is why this cannot come back as another
/// unclaimed candidate.
pub(super) async fn resolve_canonical_recovery_ref<S>(
    store: &S,
    paths: &ProtocolPaths,
    checkpoint_ref: &CheckpointRef,
) -> Result<CanonicalRecovery, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let Some(checkpoint): Option<CheckpointManifest> = read_protobuf(store, checkpoint_ref).await?
    else {
        return Ok(CanonicalRecovery::Failed(ResolveFailure::InvisibleBase));
    };

    if parent_status(store, paths, Some(&checkpoint)).await?
        == ParentCheckpointStatus::NotReadyCanonical
    {
        return Ok(CanonicalRecovery::Failed(
            ResolveFailure::ParentNotReadyCanonical,
        ));
    }

    let epoch_record: Option<EpochRecord> =
        read_json(store, &paths.epoch_record(Epoch(checkpoint.epoch))).await?;
    let committed_marker = if checkpoint.needs_commit {
        read_json(store, &committed_marker_path(paths, &checkpoint)).await?
    } else {
        None
    };

    match derive_checkpoint_state(
        checkpoint_ref,
        Some(&checkpoint),
        epoch_record,
        committed_marker.as_ref(),
    )? {
        CheckpointState::Ready => Ok(CanonicalRecovery::Found(GenerationRecovery::Ready {
            checkpoint_ref: checkpoint_ref.clone(),
        })),
        CheckpointState::Committing { epoch_record } => {
            let commit_permit =
                CommitPermit::new(checkpoint_ref.clone(), &checkpoint, epoch_record)?;
            Ok(CanonicalRecovery::Found(GenerationRecovery::ReplayCommit {
                checkpoint_ref: checkpoint_ref.clone(),
                commit_permit,
            }))
        }
        CheckpointState::Orphaned { canonical_ref } => {
            Ok(CanonicalRecovery::StopOrphaned { canonical_ref })
        }
        CheckpointState::Invisible => unreachable!("checkpoint was read above"),
        CheckpointState::Unclaimed => Ok(CanonicalRecovery::Failed(ResolveFailure::UnclaimedBase)),
    }
}

/// Resolves a generation manifest into a safe recovery action.
///
/// `latest_checkpoint_ref` and `base_checkpoint_ref` are candidate pointers, not proof of
/// recoverability. This workflow reads the candidate checkpoint and validates epoch ownership
/// and parent readiness.
///
/// It performs no persistent write. A candidate whose epoch nothing owns yet, under a
/// generation that is still current, is reported as [`GenerationResolution::ClaimRequired`]
/// rather than taken: the epoch record is immutable canonical state, so it may only be written
/// for a checkpoint whose caller has already established that it can restore it. See the
/// module docs, and [`crate::workflow::initialize_generation`], which is where that happens.
pub async fn resolve_generation_manifest<S>(
    store: &S,
    manifest: &GenerationManifest,
    runner_generation: Generation,
) -> Result<GenerationResolution, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let Some(candidate_ref) = manifest.candidate_checkpoint_ref().cloned() else {
        return Ok(GenerationResolution::Failed(ResolveFailure::NoCandidate));
    };

    let paths = ProtocolPaths::new(manifest.pipeline_id.clone(), manifest.job_id.clone());
    let is_current_generation = crate::workflow::current_generation(store, &paths)
        .await?
        .is_some_and(|current| current == runner_generation);

    let mut candidate_ref = candidate_ref;

    loop {
        match resolve_candidate_from_store(
            store,
            &paths,
            manifest,
            &candidate_ref,
            is_current_generation,
        )
        .await?
        {
            CandidateResolution::Done(resolution) => return Ok(resolution),
            CandidateResolution::FallbackToBase => {
                let Some(base_checkpoint_ref) = &manifest.base_checkpoint_ref else {
                    return Ok(GenerationResolution::Failed(ResolveFailure::NoCandidate));
                };

                candidate_ref = base_checkpoint_ref.clone();
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CandidateResolution {
    Done(GenerationResolution),
    FallbackToBase,
}

async fn resolve_candidate_from_store<S>(
    store: &S,
    paths: &ProtocolPaths,
    manifest: &GenerationManifest,
    candidate_ref: &CheckpointRef,
    is_current_generation: bool,
) -> Result<CandidateResolution, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let checkpoint: Option<CheckpointManifest> = read_protobuf(store, candidate_ref).await?;
    let parent_status = parent_status(store, paths, checkpoint.as_ref()).await?;
    let epoch_record = match &checkpoint {
        Some(checkpoint) => read_json(store, &paths.epoch_record(Epoch(checkpoint.epoch))).await?,
        None => None,
    };
    let committed_marker = match (&checkpoint, &epoch_record) {
        (Some(checkpoint), Some(_)) if checkpoint.needs_commit => {
            let path =
                paths.committed_marker(Generation(checkpoint.generation), Epoch(checkpoint.epoch));
            read_json(store, &path).await?
        }
        _ => None,
    };

    let decision = resolve_candidate(
        manifest,
        candidate_ref,
        checkpoint.as_ref(),
        epoch_record,
        committed_marker.as_ref(),
        parent_status,
        is_current_generation,
    )?;

    Ok(match decision {
        ResolveDecision::Ready { checkpoint_ref } => {
            CandidateResolution::Done(GenerationResolution::Ready { checkpoint_ref })
        }
        ResolveDecision::ReplayCommit {
            checkpoint_ref,
            epoch_record,
        } => CandidateResolution::Done(GenerationResolution::ReplayCommit {
            checkpoint_ref: checkpoint_ref.clone(),
            commit_permit: CommitPermit::new(
                checkpoint_ref,
                checkpoint
                    .as_ref()
                    .expect("replay commits must have a manifest"),
                epoch_record,
            )?,
        }),
        ResolveDecision::StopOrphaned { canonical_ref } => {
            CandidateResolution::Done(GenerationResolution::StopOrphaned { canonical_ref })
        }
        ResolveDecision::Failed(failure) => {
            CandidateResolution::Done(GenerationResolution::Failed(failure))
        }
        ResolveDecision::FallbackToBase => CandidateResolution::FallbackToBase,
        ResolveDecision::ClaimUnclaimed { checkpoint_ref } => {
            CandidateResolution::Done(GenerationResolution::ClaimRequired { checkpoint_ref })
        }
    })
}

/// Whether a checkpoint's parent is canonical and ready, which is what makes the child safe to
/// recover from.
pub(super) async fn parent_status<S>(
    store: &S,
    paths: &ProtocolPaths,
    checkpoint: Option<&CheckpointManifest>,
) -> Result<ParentCheckpointStatus, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let Some(checkpoint) = checkpoint else {
        return Ok(ParentCheckpointStatus::NoParent);
    };
    let Some(parent_checkpoint_ref) = checkpoint_parent_checkpoint_ref(checkpoint)? else {
        return Ok(ParentCheckpointStatus::NoParent);
    };

    let Some(parent_checkpoint): Option<CheckpointManifest> =
        read_protobuf(store, &parent_checkpoint_ref).await?
    else {
        return Ok(ParentCheckpointStatus::NotReadyCanonical);
    };
    let parent_epoch_record: Option<EpochRecord> =
        read_json(store, &paths.epoch_record(Epoch(parent_checkpoint.epoch))).await?;
    let parent_committed_marker = if parent_checkpoint.needs_commit {
        read_json(store, &committed_marker_path(paths, &parent_checkpoint)).await?
    } else {
        None
    };

    let state = derive_checkpoint_state(
        &parent_checkpoint_ref,
        Some(&parent_checkpoint),
        parent_epoch_record,
        parent_committed_marker.as_ref(),
    )?;

    // Enumerated rather than `_`: a parent is safe to build on only when it is canonical and
    // has nothing left to do, and a new checkpoint state has to say which of those it is
    // instead of inheriting the safe-sounding half of a wildcard.
    match state {
        CheckpointState::Ready => Ok(ParentCheckpointStatus::ReadyCanonical),
        CheckpointState::Invisible
        | CheckpointState::Unclaimed
        | CheckpointState::Orphaned { .. }
        | CheckpointState::Committing { .. } => Ok(ParentCheckpointStatus::NotReadyCanonical),
    }
}
