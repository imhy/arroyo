//! Immutable fence-scoped candidates and the conditional root that makes one authoritative
//! (M11.T26b/M11.T26g, design M11.D39c/M11.D39d).
//!
//! M11.D39d splits publishing a generation's metadata into two steps that cannot be collapsed:
//!
//! 1. the metadata is written to the object store under an **immutable, fence-scoped candidate
//!    name** — every part of the identity is in the name, so no two controllers write the same
//!    object and nothing is ever overwritten; and
//! 2. it becomes **authoritative** only when the conditional `job_statuses` update — matched on
//!    job id, `lifecycle_fence` and `controller_epoch` — installs its reference in
//!    `state_context.metadata_root`.
//!
//! A controller that loses the fence duel between the two steps has written an object nobody
//! points at. It cannot replace the winner's root, because its update matches no row; what it
//! leaves is an **unrooted candidate**, which is reclaimable exactly because the row does not
//! name it.
//!
//! # Three identities, checked against each other rather than assumed
//!
//! A root exists only when three things agree: the candidate object, the metadata that was
//! validated, and the row authority the update presents. "The caller obtained them together"
//! is a convention, so none of the three is taken on trust:
//!
//! * [`GenerationRoot`] is checked as a whole against the job's own context — its id, its
//!   pipeline, the generation this attempt raised it to and the selector its execution runs
//!   with — through M11.T25's [`Validated<T>`]. That is the D39c token, and every effect below
//!   takes it rather than the bare value.
//! * [`RootCandidate::mint`] takes the job's [`LifecycleAuthority`] whole — M11.T26b made it
//!   unconstructible from loose values — and *compares* it with the validated metadata's job
//!   id. There is no constructor that takes a fence, an epoch and a hope.
//! * [`JobStatus::install_metadata_root`](crate::JobStatus::install_metadata_root) compares the
//!   candidate against the authority the status holds **at install time**, because a
//!   re-adoption between minting and installing replaces that authority and the candidate would
//!   then name a fence the row no longer carries.
//!
//! The two comparisons are deliberately not one: minting binds the candidate to the authority
//! that existed then, installing binds it to the authority presented now.
//!
//! # What runs this
//!
//! The only caller is the M11.D39b scheduling preamble, which `run_state_body` enters for a job
//! whose lifecycle mechanism is the D39a single writer — every production job since M11.T26h.
//! A job built in the pre-flag-day peer mode writes no candidate and installs no root: it writes
//! exactly the objects it wrote before this module existed.

use arroyo_rpc::metadata_root::MetadataRootError;
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_rpc::state_backend::validated::WholeObject;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The version this build writes into a candidate object's body.
///
/// The body is read back by a controller that did not write it, so it carries its own version
/// for the same reason the record in the row does.
pub(crate) const CANDIDATE_BODY_VERSION: u32 = 1;

/// The generation metadata one scheduling attempt would make authoritative.
///
/// This is the *whole object* M11.D39c's token is about: not a checkpoint reference on its own
/// and not a generation number on its own, but the complete statement "generation `g` of job
/// `j` in pipeline `p`, running backend `b`, restoring from `c`". Every part of it is checked
/// against the job's own context before a candidate can be minted for it, because a root is
/// what a restarted controller reads to learn all four.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct GenerationRoot {
    /// The record version, so a reader can refuse a body it does not understand.
    version: u32,
    /// The pipeline whose object-store namespace this generation's metadata lives in.
    pipeline_id: String,
    /// The job this generation belongs to.
    job_id: String,
    /// The scheduling generation.
    generation: u64,
    /// The state backend the job's execution runs with, in its persisted spelling.
    execution_selector: String,
    /// The checkpoint this generation restores from, or `None` when it starts from nothing.
    recovery_checkpoint: Option<RecoveryReference>,
}

/// How a scheduling attempt names the checkpoint its generation restores from.
///
/// The two topologies name it differently, and that difference is load-bearing rather than
/// incidental: a worker-leader execution resolves an object-store reference inside the job's own
/// namespace, and a controller-mode execution resolves a row of the `checkpoints` table. A root
/// that carried one where the other belongs would send a restarted controller to look for the
/// reference in the wrong place, so the kind is part of the identity that is checked.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryReference {
    /// Worker-leader mode: the object-store reference the generation restores from.
    LeaderObject(String),
    /// Controller mode: the `checkpoints` row the generation restores from.
    ControllerCheckpointRow(String),
}

impl RecoveryReference {
    /// The kind's name, for the message a mismatch produces.
    fn kind(&self) -> &'static str {
        match self {
            RecoveryReference::LeaderObject(_) => "an object-store reference",
            RecoveryReference::ControllerCheckpointRow(_) => "a checkpoints row",
        }
    }
}

/// What the job itself says about the four identities [`GenerationRoot`] claims.
///
/// Read from the [`JobContext`](crate::states::JobContext) — its configuration id, its pipeline
/// info, the generation this attempt raised the status to, and the execution selector recovered
/// from the durable record — so the check compares two independently sourced statements rather
/// than re-reading one.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RootContext<'a> {
    /// The job the attempt is for.
    pub(crate) job_id: &'a str,
    /// The pipeline it belongs to.
    pub(crate) pipeline_id: &'a str,
    /// The generation this scheduling attempt raised the job to.
    pub(crate) generation: u64,
    /// The backend this execution runs with.
    pub(crate) execution_selector: StateBackendSelector,
    /// Whether this controller runs the job's own controller on a worker.
    ///
    /// The topology decides which kind of recovery reference is the right one, and it is read
    /// from the phase context — which derives it once, from `config().job_controller` — rather
    /// than from the metadata being checked.
    pub(crate) leader_mode: bool,
}

/// Why a generation's metadata cannot become a root.
///
/// Each variant is one identity failing to agree with another, named so that a failure says
/// *which* — an error that only said "mismatch" would leave an operator comparing two
/// controllers' logs with nothing to compare.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum RootRefusal {
    /// The metadata is about another job.
    #[error("generation metadata names job {found:?}, and this attempt is for job {expected:?}")]
    JobMismatch {
        /// The job the metadata named.
        found: String,
        /// The job the attempt is for.
        expected: String,
    },
    /// The metadata is about another pipeline.
    #[error(
        "generation metadata names pipeline {found:?}, and this attempt is for pipeline \
         {expected:?}"
    )]
    PipelineMismatch {
        /// The pipeline the metadata named.
        found: String,
        /// The pipeline the attempt is for.
        expected: String,
    },
    /// The metadata is about another generation of this job.
    #[error(
        "generation metadata names generation {found}, and this attempt raised the job to {expected}"
    )]
    GenerationMismatch {
        /// The generation the metadata named.
        found: u64,
        /// The generation this attempt raised the job to.
        expected: u64,
    },
    /// The metadata was written for another state backend.
    #[error(
        "generation metadata names state backend {found:?}, and this execution runs \
         {expected}"
    )]
    SelectorMismatch {
        /// The value the metadata named.
        found: String,
        /// The backend the job's execution runs with.
        expected: StateBackendSelector,
    },
    /// The metadata names a recovery checkpoint outside this job's own namespace.
    ///
    /// A root that pointed at another job's checkpoint would send a restarted controller to
    /// read state this job never wrote.
    #[error(
        "generation metadata names recovery checkpoint {found:?}, which is not inside \
         {namespace:?}"
    )]
    ForeignRecoveryCheckpoint {
        /// The reference the metadata named.
        found: String,
        /// The prefix every object of this job lives under.
        namespace: String,
    },
    /// The metadata names its recovery checkpoint the way the *other* topology names one.
    #[error(
        "generation metadata names its recovery checkpoint as {found}, and this attempt \
         resolves {expected}"
    )]
    RecoveryKindMismatch {
        /// How the metadata named it.
        found: &'static str,
        /// How this attempt's topology names one.
        expected: &'static str,
    },
    /// The metadata names an empty recovery checkpoint, which names nothing.
    #[error("generation metadata names an empty recovery checkpoint")]
    EmptyRecoveryCheckpoint,
    /// The body was written by a build whose record this one does not read.
    #[error(
        "generation metadata is version {found}, and this build reads only version {CANDIDATE_BODY_VERSION}"
    )]
    UnknownVersion {
        /// The version the body carried.
        found: u32,
    },
    /// The authority offered to mint a candidate is over another job.
    #[error(
        "a candidate for job {job:?} cannot be minted under an authority over job {authority:?}"
    )]
    AuthorityJobMismatch {
        /// The job the metadata is about.
        job: String,
        /// The job the authority is over.
        authority: String,
    },
    /// The identity cannot name a candidate object at all.
    ///
    /// Reached when this controller holds no adopted fence — the column's `DEFAULT 0`, which no
    /// adoption installs — or when the generation, the epoch or the derived key breaks a rule
    /// the durable record is under.
    #[error("no candidate object can be named for this attempt: {0}")]
    Unnameable(#[from] MetadataRootError),
}

impl GenerationRoot {
    /// The metadata a scheduling attempt would root, as the attempt itself describes it.
    ///
    /// Deliberately not `pub(crate)`-constructible from a context: a value built *from* the
    /// thing it is going to be checked against would make the check vacuous. The caller states
    /// the metadata, and [`Validated::validate`] compares it with the job.
    pub(crate) fn describing(
        pipeline_id: impl Into<String>,
        job_id: impl Into<String>,
        generation: u64,
        execution_selector: StateBackendSelector,
        recovery_checkpoint: Option<RecoveryReference>,
    ) -> Self {
        Self {
            version: CANDIDATE_BODY_VERSION,
            pipeline_id: pipeline_id.into(),
            job_id: job_id.into(),
            generation,
            execution_selector: execution_selector.as_str().to_string(),
            recovery_checkpoint,
        }
    }

    /// The job this metadata is about.
    pub(crate) fn job_id(&self) -> &str {
        &self.job_id
    }

    /// The pipeline this metadata is about.
    pub(crate) fn pipeline_id(&self) -> &str {
        &self.pipeline_id
    }

    /// The generation this metadata is about.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// The prefix every object of this job lives under, in the one spelling this crate uses.
    fn namespace(&self) -> String {
        format!("{}/{}/", self.pipeline_id, self.job_id)
    }
}

impl WholeObject for GenerationRoot {
    type Context<'a> = RootContext<'a>;
    type Error = RootRefusal;

    /// Checks the whole statement, before any object is written and before any row is touched.
    ///
    /// Every field is compared with the job's own answer, and the comparison runs in full: a
    /// value that agrees about its job and disagrees about its generation is refused for the
    /// generation, not accepted for the job.
    fn check_whole(&self, context: RootContext<'_>) -> Result<(), RootRefusal> {
        if self.version != CANDIDATE_BODY_VERSION {
            return Err(RootRefusal::UnknownVersion {
                found: self.version,
            });
        }
        if self.job_id != context.job_id {
            return Err(RootRefusal::JobMismatch {
                found: self.job_id.clone(),
                expected: context.job_id.to_string(),
            });
        }
        if self.pipeline_id != context.pipeline_id {
            return Err(RootRefusal::PipelineMismatch {
                found: self.pipeline_id.clone(),
                expected: context.pipeline_id.to_string(),
            });
        }
        if self.generation != context.generation {
            return Err(RootRefusal::GenerationMismatch {
                found: self.generation,
                expected: context.generation,
            });
        }
        if self.execution_selector != context.execution_selector.as_str() {
            return Err(RootRefusal::SelectorMismatch {
                found: self.execution_selector.clone(),
                expected: context.execution_selector,
            });
        }
        if let Some(recovery) = &self.recovery_checkpoint {
            let expected = match context.leader_mode {
                true => "an object-store reference",
                false => "a checkpoints row",
            };
            if recovery.kind() != expected {
                return Err(RootRefusal::RecoveryKindMismatch {
                    found: recovery.kind(),
                    expected,
                });
            }
            match recovery {
                RecoveryReference::LeaderObject(reference) => {
                    let namespace = self.namespace();
                    if !reference.starts_with(namespace.as_str()) {
                        return Err(RootRefusal::ForeignRecoveryCheckpoint {
                            found: reference.clone(),
                            namespace,
                        });
                    }
                }
                RecoveryReference::ControllerCheckpointRow(id) => {
                    if id.is_empty() {
                        return Err(RootRefusal::EmptyRecoveryCheckpoint);
                    }
                }
            }
        }
        Ok(())
    }
}

pub(crate) mod candidate;

pub(crate) use candidate::{RootCandidate, RootInstallRefusal};
