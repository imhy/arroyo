mod recovery;

use crate::ProtocolPaths;
use crate::resolve::{EpochClaimOutcome, ParentCheckpointStatus, ResolveFailure};
use crate::state::{CheckpointState, derive_checkpoint_state};
use crate::store::{
    CreateResult, ProtocolStore, StoreError, create_json_if_not_exist, create_protobuf, put_json,
    read_json, read_protobuf,
};
use crate::types::{
    CheckpointRef, CommittedMarker, CurrentGeneration, Epoch, EpochRecord, Generation,
    GenerationManifest, ProtocolError, identify_checkpoint_manifest,
    validate_epoch_record_matches_checkpoint,
};
use crate::validated::{GenerationPublication, PublishingJob};
use arroyo_rpc::grpc::rpc::CheckpointManifest;
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_rpc::state_backend::validated::Validated;
use arroyo_types::{JobId, PipelineId};
use recovery::{RecoverySearch, find_recovery_checkpoint, resolve_canonical_recovery_ref};
use std::collections::HashSet;
use std::time::SystemTime;

pub use recovery::{GenerationResolution, resolve_generation_manifest};

/// Request to claim canonical ownership of a checkpoint's epoch.
///
/// Crate-internal, and deliberately so. An epoch record is immutable: writing one names the
/// canonical checkpoint of that epoch permanently, so the two operations allowed to write one
/// are [`publish_checkpoint`], which has already bound the manifest to the reference it is
/// publishing at, and [`initialize_generation`], which has already validated the publication
/// the recovery candidate belongs to. Before PR #160 review round 8 this was public and any
/// caller could take an epoch for a manifest nothing had checked.
#[derive(Debug, Clone)]
pub(crate) struct ClaimEpochRecordRequest<'a> {
    pub epoch_record_path: &'a CheckpointRef,
    pub pipeline_id: &'a PipelineId,
    pub generation: Generation,
    pub checkpoint_ref: &'a CheckpointRef,
    pub checkpoint: &'a CheckpointManifest,
    pub created_at: SystemTime,
}

/// Outcome of writing `committed.json` for a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommittedMarkerOutcome {
    /// This call created the marker.
    Created,
    /// The marker already existed for the same checkpoint.
    AlreadyCommitted,
}

/// Input for starting a new worker generation.
///
/// The controller should write `current-generation.json` first. The new leader
/// then calls [`initialize_generation`] with the same generation id.
#[derive(Debug, Clone)]
pub struct InitializeGenerationRequest {
    pub pipeline_id: PipelineId,
    pub job_id: JobId,
    pub generation: Generation,
    pub updated_at: SystemTime,
    /// The state backend the job selects. The recovery checkpoint this initialization
    /// would restore from is checked against it before the generation is published, so a
    /// job cannot advance persistent protocol state towards a checkpoint it is not
    /// allowed to read.
    pub state_backend: StateBackendSelector,
    /// Every operator id the job's workers will construct, i.e. the key set of
    /// `LogicalProgram::tasks_per_operator` for the *current* program.
    ///
    /// The recovery checkpoint's manifest has to describe exactly these, one valid entry
    /// each, before the generation is published: each of these operators looks itself up
    /// in that manifest as it builds its state, so a manifest that omits one, describes
    /// one the program does not contain, or describes one twice fails in a worker — after
    /// the protocol state has already advanced. This is the same source of truth the
    /// legacy restore preflight uses.
    pub program_operators: HashSet<String>,
}

/// Checkpoint, if any, that a newly initialized generation should restore from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerationRecovery {
    NoCheckpoint,
    Ready {
        checkpoint_ref: CheckpointRef,
    },
    ReplayCommit {
        checkpoint_ref: CheckpointRef,
        commit_permit: CommitPermit,
    },
}

/// What an initialization does about the job's canonical `current-generation.json`.
///
/// The object is not bookkeeping: `publish_checkpoint` refuses a checkpoint whose generation is
/// not the current one, and `resolve_generation_manifest` reads a candidate differently
/// depending on whether its generation is current. Writing it is therefore a *protocol* effect,
/// which is why who writes it and when is a decision this type makes explicit rather than a
/// boolean at the call site (M11.D39c/M11.D39d, PR #167 round 6, finding 5).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CurrentGenerationPolicy {
    /// Write it here, once the generation's history is resolved and its epoch claimed.
    ///
    /// The unfenced route: the caller has no authority to establish, so there is nothing for the
    /// write to wait behind. It refuses to move the pointer backwards.
    Publish,
    /// Write nothing, and refuse unless this generation is already the current one.
    ///
    /// What a worker leader re-initializing its own generation asks for: the pointer already
    /// names this generation, and a worker has no business moving it.
    RequireCurrent,
    /// Write nothing, and hand the pointer back for the caller to publish itself.
    ///
    /// The fenced route (M11.D39d). A controller that has not yet won the metadata-root CAS may
    /// have lost the job to a replacement, and the canonical pointer is an authoritative
    /// reader/writer input rather than an unrooted candidate: publishing it before the duel is
    /// decided leaves a loser's generation named as current with nothing to undo it. Under this
    /// policy the pointer is prepared here — from the same validated publication that would have
    /// written it, so the object cannot differ — and returned as
    /// [`DeferredCurrentGeneration`] for the caller to publish once its authority is
    /// established.
    ///
    /// The monotonicity rule is [`Self::Publish`]'s, checked here as well: a caller whose
    /// generation is behind the current one is refused before anything is claimed, not after.
    Defer,
}

/// The canonical `current-generation.json` an initialization prepared and did not write.
///
/// Its field is private and it is built only by [`initialize_generation`], so a caller cannot
/// name a generation current that no initialization resolved. Publishing it is the last step of
/// making a generation authoritative, and under [`CurrentGenerationPolicy::Defer`] it is the
/// caller's to take once its own fence duel is won.
#[must_use = "a deferred current-generation pointer is published once the caller's authority is               established; dropping it leaves the job's previous generation current"]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeferredCurrentGeneration(CurrentGeneration);

impl DeferredCurrentGeneration {
    /// The generation this pointer makes current.
    pub fn generation(&self) -> Generation {
        self.0.generation
    }

    /// Writes it, making this generation the job's current one.
    ///
    /// # Errors
    ///
    /// [`StoreError`] when the object could not be written. Nothing else is written here, and
    /// nothing here is conditional: the condition is the caller's authority, which it has
    /// already established.
    pub async fn publish<S>(&self, store: &S) -> Result<(), StoreError>
    where
        S: ProtocolStore + ?Sized,
    {
        put_current_generation(store, &self.0).await
    }
}

/// Result of [`initialize_generation`].
///
/// Not `Eq`: it carries a [`CheckpointManifest`], whose generated `PartialEq` is all
/// prost provides.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum GenerationInitialization {
    Initialized {
        generation_manifest: GenerationManifest,
        recovery: GenerationRecovery,
        /// The recovery checkpoint's manifest, already read and already validated against
        /// the job's selector, or `None` when there is nothing to recover from.
        ///
        /// It is returned rather than left for the caller to fetch because it had to be
        /// read here anyway, before publication: re-reading it afterwards would pay twice
        /// for the same bytes and would let the caller act on a different object than the
        /// one that was validated.
        recovery_checkpoint: Option<CheckpointManifest>,
        /// The canonical current-generation pointer this initialization did not write.
        ///
        /// `Some` under [`CurrentGenerationPolicy::Defer`] and `None` under the other two, which
        /// either wrote it or were forbidden to. A caller that asked for the deferral and drops
        /// this leaves the previous generation current — see [`DeferredCurrentGeneration`].
        current_generation: Option<DeferredCurrentGeneration>,
    },
    StaleGeneration {
        current_generation: Generation,
    },
    StopOrphaned {
        canonical_ref: CheckpointRef,
    },
    Failed(ResolveFailure),
}

/// Input for publishing a completed checkpoint.
///
/// State files should already be written under the checkpoint directory. This
/// workflow publishes the immutable checkpoint manifest, updates the generation
/// manifest candidate pointer, and claims the epoch record.
#[derive(Debug, Clone)]
pub struct PublishCheckpointRequest<'a> {
    pub generation_manifest: &'a GenerationManifest,
    pub checkpoint_ref: &'a CheckpointRef,
    pub checkpoint: &'a CheckpointManifest,
    pub created_at: SystemTime,
}

/// Result of publishing a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointPublication {
    Ready {
        checkpoint_ref: CheckpointRef,
    },
    CommitRequired {
        checkpoint_ref: CheckpointRef,
        commit_permit: CommitPermit,
    },
    StopOrphaned {
        canonical_ref: CheckpointRef,
    },
    StaleGeneration,
    Failed(ResolveFailure),
}

/// Capability proving a checkpoint currently owns its epoch record and may
/// complete external commit once workers report success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitPermit {
    checkpoint_ref: CheckpointRef,
    epoch_record: EpochRecord,
}

impl CommitPermit {
    pub fn new(
        checkpoint_ref: CheckpointRef,
        checkpoint: &CheckpointManifest,
        epoch_record: EpochRecord,
    ) -> Result<Self, ProtocolError> {
        validate_epoch_record_matches_checkpoint(&checkpoint_ref, checkpoint, &epoch_record)?;

        if epoch_record.version != crate::types::PROTOCOL_VERSION
            || epoch_record.checkpoint_ref != checkpoint_ref
            || epoch_record.generation != Generation(checkpoint.generation)
            || *epoch_record.pipeline_id != checkpoint.pipeline_id
            || *epoch_record.job_id != checkpoint.job_id
        {
            return Err(ProtocolError::CheckpointManifestMismatch);
        }

        Ok(Self {
            checkpoint_ref,
            epoch_record,
        })
    }

    /// The canonical checkpoint ref authorized by this permit.
    pub fn checkpoint_ref(&self) -> &CheckpointRef {
        &self.checkpoint_ref
    }

    /// The canonical epoch record authorizing this checkpoint.
    pub fn epoch_record(&self) -> &EpochRecord {
        &self.epoch_record
    }

    fn committed_marker_path(&self) -> CheckpointRef {
        ProtocolPaths::new(
            self.epoch_record.pipeline_id.clone(),
            self.epoch_record.job_id.clone(),
        )
        .committed_marker(self.epoch_record.generation, self.epoch_record.epoch)
    }
}

/// Result of checking whether a checkpoint may send external `CommitReq`s.
///
/// Only `Authorized` permits callers to fan out commit messages. Every other
/// variant means no commit should be sent for the supplied checkpoint.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum CommitAuthorization {
    Authorized {
        checkpoint_ref: CheckpointRef,
        checkpoint: CheckpointManifest,
        commit_permit: CommitPermit,
    },
    AlreadyCommitted {
        checkpoint_ref: CheckpointRef,
    },
    NoCommitNeeded {
        checkpoint_ref: CheckpointRef,
    },
    StopOrphaned {
        canonical_ref: CheckpointRef,
    },
    NotCanonical {
        checkpoint_ref: CheckpointRef,
    },
    MissingCheckpoint {
        checkpoint_ref: CheckpointRef,
    },
}

/// Initializes a new generation and writes its generation manifest.
///
/// When older manifests point at orphaned checkpoints, this follows the epoch
/// record to the canonical checkpoint so replacement generations do not wedge on
/// the same losing manifest.
///
/// If `update_current_generation` is set, this method will write the current generation
/// file. If not set, it will read the current generation and enforce conformance.
///
/// # Recovery-checkpoint validation and write ordering
///
/// Publishing a generation is what commits this job to a recovery checkpoint. Three
/// persistent objects say so: the current generation file names the generation, the
/// generation manifest records its link to the checkpoint it will restore from, and — when
/// the candidate the search found had no owner yet — the *epoch record* names that candidate
/// the canonical checkpoint of its epoch. All three are protocol state a restart reads, so the
/// recovery checkpoint has to be resolved, read, and checked *before* any of them is written.
///
/// The epoch record is the sharpest of the three because it is immutable. Until PR #160 review
/// round 8 it was written during the search, before the checks below had run: a manifest this
/// job could not restore was made canonical and only then rejected, leaving a checkpoint no
/// job would ever recover from owning an epoch, and possibly orphaning a valid checkpoint for
/// the same epoch. Resolution now reports an unclaimed candidate instead of taking it, and the
/// claim happens here, on the far side of the token.
///
/// Three things are checked, and each is a whole-set claim about the manifest rather than a
/// per-entry one:
///
/// 1. It must be the checkpoint the reference it was read from names, entry headers included
///    (added in review round 7).
/// 2. It must describe exactly `request.program_operators`, one entry each, carrying an
///    operator header. Every one of those operators looks itself up in this manifest as it
///    builds its state; an entry the manifest merely happens to contain proves nothing.
/// 3. Every table config in it must agree with `request.state_backend`.
///
/// The resolved manifest is returned in
/// [`GenerationInitialization::Initialized::recovery_checkpoint`] so callers use the same
/// object that was validated rather than reading it again.
///
/// # Errors
///
/// Returns [`StoreError::Protocol`] if the recovery checkpoint is not the checkpoint its
/// reference names, [`StoreError::IncompleteManifest`] if it does not describe exactly the
/// operators the job's workers will build, or [`StoreError::StateBackend`] if it was written by
/// a different backend than the job selects or names an unknown one. In every case nothing has
/// been written: the previous generation and its manifest are untouched, the epoch the rejected
/// candidate would have taken is still there for a checkpoint that can be restored, and the
/// checkpoint remains restorable by a job it does fit.
pub async fn initialize_generation<S>(
    store: &S,
    request: InitializeGenerationRequest,
    policy: CurrentGenerationPolicy,
) -> Result<GenerationInitialization, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let paths = ProtocolPaths::new(request.pipeline_id.clone(), request.job_id.clone());

    // TODO: this read is not strictly necessary for a policy that writes the pointer, but is
    //  here to prevent potential corruption by a confused controller
    let current_generation =
        read_json::<_, CurrentGeneration>(store, &paths.current_generation()).await?;

    match policy {
        // Both policies that make this generation current owe the same monotonicity rule, and it
        // is checked here for both — `Defer` moves *when* the pointer is written and not what may
        // be written, so a controller behind the current generation is refused before it claims
        // an epoch rather than after.
        CurrentGenerationPolicy::Publish | CurrentGenerationPolicy::Defer => {
            if let Some(current) = &current_generation
                && current.generation > request.generation
            {
                return Err(StoreError::Protocol(
                    ProtocolError::NonMonotonicGenerationUpdate,
                ));
            }
        }
        CurrentGenerationPolicy::RequireCurrent => {
            if let Some(cur) = &current_generation
                && cur.generation != request.generation
            {
                return Ok(GenerationInitialization::StaleGeneration {
                    current_generation: cur.generation,
                });
            }
        }
    }

    // Whole-object check, before every persistent effect. `find_recovery_checkpoint` and
    // everything under it only read — that is what the `recovery` module is for, and
    // `the_recovery_resolution_module_reaches_no_persistent_write` is what keeps it true — so
    // a candidate whose epoch nothing owns arrives here as `RecoverySearch::Unclaimed` rather
    // than already made canonical.
    let restoring: HashSet<&str> = request
        .program_operators
        .iter()
        .map(String::as_str)
        .collect();
    let recovery = find_recovery_checkpoint(store, &paths, request.generation).await?;
    let mut publication = validate_publication(store, &request, &restoring, &recovery).await?;

    // Taking the epoch is the third of this function's persistent effects and the only
    // irreversible one, so it takes the token and nothing else, exactly as the other two do.
    let recovery = match recovery {
        RecoverySearch::Unclaimed { .. } => {
            match claim_recovery_epoch(store, &publication).await? {
                RecoveryClaim::Claimed(recovery) => RecoverySearch::Found(recovery),
                RecoveryClaim::Orphaned { canonical_ref } => {
                    // Another checkpoint already owned the epoch, so recovery follows it to the
                    // canonical one — exactly as it did when the claim was made inside the search.
                    // That is a different checkpoint from the one the token above certifies, so it
                    // is read and checked in its own right before anything is published for it.
                    // `CanonicalRecovery` carries no unclaimed candidate, so there is no second
                    // claim to make and no loop here.
                    let redirected: RecoverySearch =
                        resolve_canonical_recovery_ref(store, &paths, &canonical_ref)
                            .await?
                            .into();
                    publication =
                        validate_publication(store, &request, &restoring, &redirected).await?;
                    redirected
                }
            }
        }
        resolved => resolved,
    };

    // Written here only for the policy that owns the write. Under `Defer` the object is minted
    // from this same validated publication below and handed back, so the pointer the caller
    // publishes is the pointer this initialization resolved rather than one rebuilt from a
    // second read (M11.D39d, PR #167 round 6, finding 5).
    if policy == CurrentGenerationPolicy::Publish {
        publish_current_generation(store, &publication).await?;
    }

    // Reported only after the current generation has been claimed, as before: an orphaned
    // or unresolvable history is a state this generation still owns.
    let recovery = match recovery {
        RecoverySearch::Found(recovery) => recovery,
        RecoverySearch::StopOrphaned { canonical_ref } => {
            return Ok(GenerationInitialization::StopOrphaned { canonical_ref });
        }
        RecoverySearch::Failed(failure) => {
            return Ok(GenerationInitialization::Failed(failure));
        }
        // The claim step above replaces every unclaimed candidate with one of the three
        // outcomes claiming it produces, so this is unreachable by construction. It is
        // answered rather than asserted: a future path that reached it would publish no
        // generation manifest instead of panicking inside a controller.
        RecoverySearch::Unclaimed { .. } => {
            return Ok(GenerationInitialization::Failed(
                ResolveFailure::UnclaimedBase,
            ));
        }
    };

    let generation_manifest = publish_generation_manifest(store, &publication).await?;

    let current_generation = (policy == CurrentGenerationPolicy::Defer).then(|| {
        DeferredCurrentGeneration(publication.get().current_generation(SystemTime::now()))
    });

    Ok(GenerationInitialization::Initialized {
        generation_manifest,
        recovery,
        recovery_checkpoint: publication.into_inner().into_recovery_checkpoint(),
        current_generation,
    })
}

/// Reads the checkpoint a recovery search resolved and checks the publication it would commit
/// this job to, producing the token every persistent effect in [`initialize_generation`] takes.
///
/// The read is here rather than at the call site so that the object the token carries is the
/// object that was checked: a second read of the same reference could return different bytes,
/// and then the manifest handed back to the caller would not be the one validation saw.
async fn validate_publication<S>(
    store: &S,
    request: &InitializeGenerationRequest,
    program_operators: &HashSet<&str>,
    recovery: &RecoverySearch,
) -> Result<Validated<GenerationPublication>, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let base_checkpoint_ref = recovery.resolved_checkpoint_ref().cloned();
    let recovery_checkpoint = match &base_checkpoint_ref {
        Some(checkpoint_ref) => Some(
            read_protobuf::<_, CheckpointManifest>(store, checkpoint_ref)
                .await?
                .ok_or_else(|| {
                    StoreError::Protocol(ProtocolError::MissingCheckpointManifest {
                        checkpoint_ref: checkpoint_ref.clone(),
                    })
                })?,
        ),
        None => None,
    };

    Validated::validate(
        GenerationPublication::new(
            request.pipeline_id.clone(),
            request.job_id.clone(),
            request.generation,
            request.updated_at,
            base_checkpoint_ref,
            recovery_checkpoint,
        ),
        PublishingJob {
            state_backend: request.state_backend,
            program_operators,
        },
    )
}

/// Outcome of claiming the epoch of the checkpoint a validated publication commits to.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RecoveryClaim {
    /// This checkpoint owns its epoch, and this is how the generation recovers from it.
    Claimed(GenerationRecovery),
    /// Another checkpoint owns the epoch; recovery has to follow that one instead.
    Orphaned { canonical_ref: CheckpointRef },
}

/// Claims the epoch record of the checkpoint a validated publication commits to.
///
/// Takes only the [`Validated<GenerationPublication>`], for the reason
/// [`publish_current_generation`] and [`publish_generation_manifest`] do and for a sharper one:
/// an epoch record is immutable. Writing one names the canonical checkpoint of that epoch for
/// good, so a claim made for a candidate that then fails validation leaves a rejected
/// checkpoint canonical and can orphan a valid checkpoint for the same epoch — which no later
/// fence can undo. That is why this is here and not inside the search that found the candidate
/// (design item M11.D39c; PR #160 review round 8).
///
/// Everything it writes and reads is addressed out of the token, the path builder included, so
/// the objects it touches are the ones the identity check bound the manifest to. A publication
/// with nothing to recover from has no epoch to claim and recovers from no checkpoint, which is
/// the same statement as [`GenerationRecovery::NoCheckpoint`].
async fn claim_recovery_epoch<S>(
    store: &S,
    publication: &Validated<GenerationPublication>,
) -> Result<RecoveryClaim, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let Some((checkpoint_ref, checkpoint)) = publication.get().recovery_checkpoint() else {
        return Ok(RecoveryClaim::Claimed(GenerationRecovery::NoCheckpoint));
    };
    let paths = publication.get().paths();

    let outcome = claim_epoch_record(
        store,
        ClaimEpochRecordRequest {
            epoch_record_path: &paths.epoch_record(Epoch(checkpoint.epoch)),
            pipeline_id: publication.get().pipeline_id(),
            generation: Generation(checkpoint.generation),
            checkpoint_ref,
            checkpoint,
            created_at: SystemTime::now(),
        },
    )
    .await?;

    match outcome {
        EpochClaimOutcome::Owned { record } if checkpoint.needs_commit => {
            let committed_marker: Option<CommittedMarker> =
                read_json(store, &committed_marker_path(&paths, checkpoint)).await?;

            if committed_marker.is_some() {
                Ok(RecoveryClaim::Claimed(GenerationRecovery::Ready {
                    checkpoint_ref: checkpoint_ref.clone(),
                }))
            } else {
                Ok(RecoveryClaim::Claimed(GenerationRecovery::ReplayCommit {
                    checkpoint_ref: checkpoint_ref.clone(),
                    commit_permit: CommitPermit::new(checkpoint_ref.clone(), checkpoint, record)?,
                }))
            }
        }
        EpochClaimOutcome::Owned { .. } => Ok(RecoveryClaim::Claimed(GenerationRecovery::Ready {
            checkpoint_ref: checkpoint_ref.clone(),
        })),
        EpochClaimOutcome::Orphaned { canonical_ref } => {
            Ok(RecoveryClaim::Orphaned { canonical_ref })
        }
    }
}

/// Writes the current-generation fence.
///
/// Takes only the [`Validated<GenerationPublication>`]: this is the first of the three objects
/// that commit the job to a recovery checkpoint, so it may not be written for a checkpoint
/// nothing checked (design item M11.D39c). The path it writes to is derived from the token as
/// well, so the object cannot be addressed out of an identity the check never saw.
async fn publish_current_generation<S>(
    store: &S,
    publication: &Validated<GenerationPublication>,
) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
{
    put_current_generation(
        store,
        &publication.get().current_generation(SystemTime::now()),
    )
    .await
}

/// The one write of `current-generation.json`.
///
/// Both routes to the object go through it — the immediate one above and
/// [`DeferredCurrentGeneration::publish`] — so the bytes a deferred publication writes are the
/// bytes an immediate one would have, and the path is derived from the record rather than named
/// twice.
async fn put_current_generation<S>(
    store: &S,
    current_generation: &CurrentGeneration,
) -> Result<(), StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let paths = ProtocolPaths::new(
        current_generation.pipeline_id.clone(),
        current_generation.job_id.clone(),
    );
    put_json(store, &paths.current_generation(), current_generation).await
}

/// Writes the generation manifest and returns what was written.
///
/// Takes only the [`Validated<GenerationPublication>`], for the same reason as
/// [`publish_current_generation`]: this object records the link to the checkpoint the
/// generation will restore from, which is the commitment the check exists to gate.
async fn publish_generation_manifest<S>(
    store: &S,
    publication: &Validated<GenerationPublication>,
) -> Result<GenerationManifest, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let generation_manifest = publication.get().generation_manifest();
    put_json(
        store,
        &publication
            .get()
            .paths()
            .generation_manifest(publication.get().generation()),
        &generation_manifest,
    )
    .await?;
    Ok(generation_manifest)
}

/// Checks whether a checkpoint is allowed to perform external commit.
///
/// Call this immediately before sending `CommitReq`. It reads the checkpoint
/// manifest, epoch record, and commit marker. Only `CommitAuthorization::Authorized`
/// means the checkpoint is canonical, requires commit, and has not already been
/// committed.
///
/// If this is being called within a checkpointing pass (as opposed to on recovery),
/// pass `true` to `assume_not_committed` to avoid an unnecessary object store lookup.
pub async fn prepare_commit<S>(
    store: &S,
    checkpoint_ref: &CheckpointRef,
    checkpoint: CheckpointManifest,
    epoch_record: Option<EpochRecord>,
    assume_not_committed: bool,
) -> Result<CommitAuthorization, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let paths = ProtocolPaths::new(
        PipelineId::new(&checkpoint.pipeline_id),
        JobId::new(&checkpoint.job_id),
    );

    let committed_marker_path = committed_marker_path(&paths, &checkpoint);
    let committed_marker: Option<CommittedMarker> = if assume_not_committed {
        None
    } else {
        read_json(store, &committed_marker_path).await?
    };

    let epoch_record = match epoch_record {
        Some(r) => Some(r),
        None => {
            read_json::<_, EpochRecord>(store, &paths.epoch_record(Epoch(checkpoint.epoch))).await?
        }
    };

    match derive_checkpoint_state(
        checkpoint_ref,
        Some(&checkpoint),
        epoch_record,
        committed_marker.as_ref(),
    )? {
        CheckpointState::Invisible => unreachable!("checkpoint was read above"),
        CheckpointState::Unclaimed => Ok(CommitAuthorization::NotCanonical {
            checkpoint_ref: checkpoint_ref.clone(),
        }),
        CheckpointState::Orphaned { canonical_ref } => {
            Ok(CommitAuthorization::StopOrphaned { canonical_ref })
        }
        CheckpointState::Ready if checkpoint.needs_commit => {
            Ok(CommitAuthorization::AlreadyCommitted {
                checkpoint_ref: checkpoint_ref.clone(),
            })
        }
        CheckpointState::Ready => Ok(CommitAuthorization::NoCommitNeeded {
            checkpoint_ref: checkpoint_ref.clone(),
        }),
        CheckpointState::Committing { epoch_record } => {
            let commit_permit =
                CommitPermit::new(checkpoint_ref.clone(), &checkpoint, epoch_record)?;
            Ok(CommitAuthorization::Authorized {
                checkpoint_ref: checkpoint_ref.clone(),
                checkpoint,
                commit_permit,
            })
        }
    }
}

/// Writes `committed.json` after external commit succeeds.
///
/// The caller must pass a [`CommitPermit`] returned by this protocol after it
/// has already validated canonical epoch ownership. The write is conditional
/// and idempotent for retries of the same checkpoint.
pub async fn complete_commit<S>(
    store: &S,
    commit_permit: &CommitPermit,
    writer_generation: Generation,
) -> Result<CommittedMarkerOutcome, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let epoch_record = commit_permit.epoch_record();

    let marker = CommittedMarker::new(
        epoch_record.pipeline_id.clone(),
        epoch_record.job_id.clone(),
        epoch_record.epoch,
        epoch_record.generation,
        writer_generation,
        commit_permit.checkpoint_ref().clone(),
    );

    mark_committed(store, &marker, commit_permit).await
}

/// Publishes a completed checkpoint and claims its epoch record.
///
/// `request.checkpoint_ref` must be the reference the manifest's own generation and epoch
/// name, under the generation manifest's pipeline and job: a checkpoint manifest is read back
/// by reference and every path derived from it comes out of the identity it carries, so an
/// object written anywhere else is one no reader can safely act on.
///
/// Correct caller sequence:
/// 1. Write all checkpoint state files.
/// 2. Call this function with the immutable protobuf checkpoint manifest.
/// 3. If `Ready`, the checkpoint is recoverable.
/// 4. If `CommitRequired`, call [`prepare_commit`], send `CommitReq` only if it
///    returns `Authorized`, then call [`complete_commit`] after workers finish.
/// 5. If `StopOrphaned` or `StaleGeneration`, stop this generation.
pub async fn publish_checkpoint<S>(
    store: &S,
    request: PublishCheckpointRequest<'_>,
) -> Result<CheckpointPublication, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    validate_checkpoint_for_generation(request.generation_manifest, request.checkpoint)?;

    let paths = ProtocolPaths::new(
        request.generation_manifest.pipeline_id.clone(),
        request.generation_manifest.job_id.clone(),
    );

    // The write side of the same relationship the read side enforces. Every reader of a
    // checkpoint manifest — [`initialize_generation`]'s recovery publication and the leader-GC
    // traversal — now requires the object to be the checkpoint the reference it was read from
    // names (design item M11.D39c; PR #160 review round 7). This is the one place in Arroyo
    // that creates such an object, so refusing to *write* a manifest anywhere but at its own
    // reference is what makes the read-side rule a property of the store rather than a
    // convention the writer happens to keep. `finish_checkpoint_leader` builds both halves out
    // of the same job, generation and epoch, so no legitimate publication is refused.
    identify_checkpoint_manifest(&paths, request.checkpoint_ref, request.checkpoint)?;

    let is_current_generation =
        read_json::<_, CurrentGeneration>(store, &paths.current_generation())
            .await?
            .is_some_and(|current_generation| {
                current_generation.generation == request.generation_manifest.generation
            });

    if !is_current_generation {
        return Ok(CheckpointPublication::StaleGeneration);
    }

    // Ahead of the first write, not behind it. Parent readiness is the last thing that can
    // refuse this publication, and every check belongs on this side of the first persistent
    // effect: publishing under an unready parent used to create the manifest object and only
    // then refuse, leaving an object behind for a publication that never happened (PR #160
    // review round 8). The check reads the *parent*, so nothing about it needs the object that
    // is about to be written.
    if recovery::parent_status(store, &paths, Some(request.checkpoint)).await?
        == ParentCheckpointStatus::NotReadyCanonical
    {
        return Ok(CheckpointPublication::Failed(
            ResolveFailure::ParentNotReadyCanonical,
        ));
    }

    match create_protobuf(store, request.checkpoint_ref, request.checkpoint).await? {
        CreateResult::Created => {}
        CreateResult::AlreadyExists(existing) if existing == *request.checkpoint => {}
        CreateResult::AlreadyExists(_) => {
            return Err(StoreError::Protocol(
                ProtocolError::CheckpointManifestMismatch,
            ));
        }
    }

    let mut updated_generation_manifest = request.generation_manifest.clone();
    updated_generation_manifest.latest_checkpoint_ref = Some(request.checkpoint_ref.clone());
    put_json(
        store,
        &paths.generation_manifest(request.generation_manifest.generation),
        &updated_generation_manifest,
    )
    .await?;

    let outcome = claim_epoch_record(
        store,
        ClaimEpochRecordRequest {
            epoch_record_path: &paths.epoch_record(Epoch(request.checkpoint.epoch)),
            pipeline_id: &request.generation_manifest.pipeline_id,
            generation: request.generation_manifest.generation,
            checkpoint_ref: request.checkpoint_ref,
            checkpoint: request.checkpoint,
            created_at: request.created_at,
        },
    )
    .await?;

    match outcome {
        EpochClaimOutcome::Owned { record } if request.checkpoint.needs_commit => {
            let commit_permit =
                CommitPermit::new(request.checkpoint_ref.clone(), request.checkpoint, record)?;
            Ok(CheckpointPublication::CommitRequired {
                checkpoint_ref: request.checkpoint_ref.clone(),
                commit_permit,
            })
        }
        EpochClaimOutcome::Owned { .. } => Ok(CheckpointPublication::Ready {
            checkpoint_ref: request.checkpoint_ref.clone(),
        }),
        EpochClaimOutcome::Orphaned { canonical_ref } => {
            Ok(CheckpointPublication::StopOrphaned { canonical_ref })
        }
    }
}

/// Claims an epoch record for a published checkpoint.
///
/// A successful conditional create and an existing record for the same
/// checkpoint both return `Owned`. An existing record for a different checkpoint
/// returns `Orphaned`; callers must stop using the losing checkpoint.
///
/// Crate-internal for the reason [`ClaimEpochRecordRequest`] is: the record is immutable, so
/// the only two callers are the ones that have already established that the manifest is the
/// checkpoint the reference names and that this job may act on it.
pub(crate) async fn claim_epoch_record<S>(
    store: &S,
    request: ClaimEpochRecordRequest<'_>,
) -> Result<EpochClaimOutcome, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    let record = EpochRecord::for_checkpoint(
        request.pipeline_id.clone(),
        request.generation,
        request.checkpoint_ref.clone(),
        request.checkpoint,
        request.created_at,
    )?;

    match create_json_if_not_exist(store, request.epoch_record_path, &record).await? {
        CreateResult::Created => Ok(EpochClaimOutcome::Owned { record }),
        CreateResult::AlreadyExists(existing) => {
            if existing.checkpoint_ref == *request.checkpoint_ref {
                derive_checkpoint_state(
                    request.checkpoint_ref,
                    Some(request.checkpoint),
                    Some(existing.clone()),
                    None,
                )?;
                Ok(EpochClaimOutcome::Owned { record: existing })
            } else {
                Ok(EpochClaimOutcome::Orphaned {
                    canonical_ref: existing.checkpoint_ref.clone(),
                })
            }
        }
    }
}

/// Conditionally writes the commit marker for a checkpoint.
///
/// Most callers should use [`complete_commit`] so canonical ownership is checked
/// before the marker is written. Use this directly only when that check has
/// already been performed.
///
/// The path is derived from `commit_permit`, not supplied beside it. [`validate_marker`]
/// already requires the marker's contents to be the permit's checkpoint, but until PR #160's
/// GC-namespace review finding swept this class the *location* was a free argument: a permit
/// for one job could write a commit marker into another job's namespace, where the next
/// [`prepare_commit`] there reads it as that checkpoint's commit. It is the same defect the
/// finding named in leader GC — an effect addressed out of an identity nothing checked —
/// under a creating write rather than a destructive one.
pub async fn mark_committed<S>(
    store: &S,
    marker: &CommittedMarker,
    commit_permit: &CommitPermit,
) -> Result<CommittedMarkerOutcome, StoreError>
where
    S: ProtocolStore + ?Sized,
{
    validate_marker(marker, commit_permit)?;

    let committed_marker_path = &commit_permit.committed_marker_path();

    match create_json_if_not_exist(store, committed_marker_path, marker).await? {
        CreateResult::Created => Ok(CommittedMarkerOutcome::Created),
        CreateResult::AlreadyExists(existing) => {
            validate_marker(&existing, commit_permit)?;

            if existing.checkpoint_ref == marker.checkpoint_ref {
                Ok(CommittedMarkerOutcome::AlreadyCommitted)
            } else {
                Err(StoreError::Protocol(ProtocolError::CommittedMarkerMismatch))
            }
        }
    }
}

fn validate_marker(marker: &CommittedMarker, permit: &CommitPermit) -> Result<(), ProtocolError> {
    let record = permit.epoch_record();

    if marker.version != crate::types::PROTOCOL_VERSION
        || marker.pipeline_id != record.pipeline_id
        || marker.job_id != record.job_id
        || marker.epoch != record.epoch
        || marker.checkpoint_generation != record.generation
        || marker.checkpoint_ref != *permit.checkpoint_ref()
    {
        return Err(ProtocolError::CommittedMarkerMismatch);
    }

    Ok(())
}

fn validate_checkpoint_for_generation(
    generation_manifest: &GenerationManifest,
    checkpoint: &CheckpointManifest,
) -> Result<(), ProtocolError> {
    if *generation_manifest.pipeline_id != checkpoint.pipeline_id
        || *generation_manifest.job_id != checkpoint.job_id
        || generation_manifest.generation.0 != checkpoint.generation
    {
        return Err(ProtocolError::CheckpointManifestMismatch);
    }

    Ok(())
}

fn committed_marker_path(paths: &ProtocolPaths, checkpoint: &CheckpointManifest) -> CheckpointRef {
    paths.committed_marker(Generation(checkpoint.generation), Epoch(checkpoint.epoch))
}
