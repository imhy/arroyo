//! The candidate object, and what it takes to make one authoritative (M11.T26g, M11.D39d).
//!
//! A child of [`super`] rather than a sibling for the reason the split exists at all: a
//! candidate is minted from a [`GenerationRoot`] that has already been checked as a whole, and
//! the accessors it reads are the ones that module owns. What is here is the half that *acts* —
//! the name, the bytes, the publication, and the agreement a row update is conditional on.

use arroyo_rpc::errors::StorageError;
use arroyo_rpc::metadata_root::MetadataRoot;
use arroyo_rpc::state_backend::validated::Validated;
use arroyo_storage::StorageProvider;
use thiserror::Error;

use super::{GenerationRoot, RootRefusal};
use crate::states::lifecycle::fence::LifecycleAuthority;

/// An immutable, fence-scoped candidate object: the metadata, and the name it is published at.
///
/// The name is [`MetadataRoot::object`], derived from the identity the record carries, so the
/// object a root points at and the object a candidate was written to are the same string by
/// construction rather than by agreement between two formatters.
///
/// There is exactly one constructor, and it takes the two values whose agreement is the whole
/// point: the job's durable authority and the validated metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RootCandidate {
    /// The identity, which is also the name.
    root: MetadataRoot,
    /// The bytes published at that name.
    body: Vec<u8>,
}

impl RootCandidate {
    /// Mints the candidate `authority` would publish for `metadata`.
    ///
    /// The fence and the epoch come from the authority — taken whole, so they describe one row
    /// — and the generation, job and pipeline come from the validated metadata. The two are
    /// *compared* on the identity they share: an authority over another job cannot name this
    /// job's candidate, however it was obtained.
    ///
    /// # Errors
    ///
    /// [`RootRefusal::AuthorityJobMismatch`] when the two disagree about the job, and
    /// [`RootRefusal::Unnameable`] when the identity cannot name a candidate at all — an
    /// unadopted fence, a generation no worker ran under, or an identifier the durable record
    /// cannot carry.
    pub(crate) fn mint(
        authority: &LifecycleAuthority,
        metadata: &Validated<GenerationRoot>,
    ) -> Result<Self, RootRefusal> {
        let metadata = metadata.get();
        if **authority.job_id() != *metadata.job_id() {
            return Err(RootRefusal::AuthorityJobMismatch {
                job: metadata.job_id().to_string(),
                authority: (**authority.job_id()).clone(),
            });
        }
        let root = MetadataRoot::mint(
            metadata.pipeline_id(),
            metadata.job_id(),
            metadata.generation(),
            authority.fence().get(),
            authority.epoch(),
        )?;
        // Serializing cannot fail for this shape — every field is a string, a `u64` or an
        // `Option<String>` — and building the bytes here rather than at publish time is what
        // makes a candidate a value that is complete before anything is written.
        let body = serde_json::to_vec(metadata)
            .expect("a generation root is plain strings and integers and always serializes");
        Ok(Self { root, body })
    }

    /// The object-store key this candidate is published at.
    pub(crate) fn key(&self) -> String {
        self.root.object()
    }

    /// The record that would make this candidate authoritative.
    pub(crate) fn root(&self) -> &MetadataRoot {
        &self.root
    }

    /// The bytes this candidate publishes.
    #[cfg(test)]
    pub(crate) fn body(&self) -> &[u8] {
        &self.body
    }

    /// Publishes the candidate object, without making it authoritative.
    ///
    /// `put_if_not_exists` rather than `put`, and that is not belt-and-braces: the name embeds
    /// the whole identity, so the only thing that can already be at it is this same attempt's
    /// own bytes from a retry. Treating that as success is idempotence; overwriting would make
    /// an object whose name promises immutability mutable.
    ///
    /// Nothing about this publication is authoritative. A controller cancelled or superseded
    /// after it has written this object and before it has installed the reference leaves an
    /// unrooted candidate — never a half-installed root — because the only thing that roots a
    /// candidate is one conditional statement that either matches its row or does not.
    ///
    /// # Errors
    ///
    /// [`CandidatePublishError`] for a storage failure. An object that is already there is not
    /// a failure.
    pub(crate) async fn publish(
        &self,
        storage: &StorageProvider,
    ) -> Result<(), CandidatePublishError> {
        let key = self.key();
        match storage
            .put_if_not_exists(key.as_str(), self.body.clone())
            .await
        {
            Ok(()) => Ok(()),
            // The same identity writes the same bytes, so an object already at this name is
            // this attempt's own retry. See above for why the name makes that the only case.
            Err(StorageError::AlreadyExists { .. }) => Ok(()),
            Err(error) => Err(CandidatePublishError {
                key,
                report: format!("{error:?}"),
            }),
        }
    }
}

/// A candidate object that could not be written.
///
/// Separate from [`RootRefusal`] because it says nothing about whether the identities agree:
/// the metadata was validated and the candidate was nameable, and the store was unreachable.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("failed to publish the candidate metadata object {key:?}: {report}")]
pub(crate) struct CandidatePublishError {
    /// The key the candidate would have been written at.
    pub key: String,
    /// The store's own report, preserved rather than replaced.
    pub report: String,
}

/// Which identity a candidate and a row authority disagreed about at install time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootIdentity {
    /// The job the candidate is for.
    JobId,
    /// The lifecycle fence the candidate is scoped to.
    Fence,
    /// The controller epoch the candidate is scoped to.
    Epoch,
    /// The scheduling generation the candidate is for.
    Generation,
}

impl RootIdentity {
    /// The identity's name, for the message an operator reads.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            RootIdentity::JobId => "job id",
            RootIdentity::Fence => "lifecycle fence",
            RootIdentity::Epoch => "controller epoch",
            RootIdentity::Generation => "scheduling generation",
        }
    }
}

/// Why a candidate could not be installed as the authoritative root.
///
/// A *refusal*, not a lost duel: losing the duel is [`AuthorityOutcome::Stale`](
/// super::fence::AuthorityOutcome::Stale) and means the row is somebody else's. This means the
/// candidate and the authority describe different things, which no row could reconcile.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error(
    "a candidate for job {job_id} cannot be installed under this status: its {identity} is \
     {candidate:?} and the status presents {presented:?}",
    identity = identity.as_str()
)]
pub(crate) struct RootInstallRefusal {
    /// The job the candidate is for.
    pub job_id: String,
    /// Which identity disagreed.
    pub identity: RootIdentity,
    /// What the candidate named.
    pub candidate: String,
    /// What the status presents.
    pub presented: String,
}

impl RootCandidate {
    /// Checks this candidate against the authority and generation a status presents *now*.
    ///
    /// Called by [`JobStatus::install_metadata_root`](crate::JobStatus::install_metadata_root)
    /// immediately before the conditional write, and separate from
    /// [`Self::mint`]'s checks because the status's authority can have been replaced since: a
    /// re-adoption installs a new fence and epoch, and a candidate minted under the previous
    /// one names an object this controller is no longer entitled to root.
    ///
    /// # Errors
    ///
    /// [`RootInstallRefusal`] naming the first identity that disagrees.
    pub(crate) fn agrees_with(
        &self,
        authority: &LifecycleAuthority,
        generation: u64,
    ) -> Result<(), RootInstallRefusal> {
        let refuse = |identity, candidate: String, presented: String| RootInstallRefusal {
            job_id: self.root.job_id().to_string(),
            identity,
            candidate,
            presented,
        };
        if self.root.job_id() != **authority.job_id() {
            return Err(refuse(
                RootIdentity::JobId,
                self.root.job_id().to_string(),
                (**authority.job_id()).clone(),
            ));
        }
        if self.root.fence() != authority.fence().get() {
            return Err(refuse(
                RootIdentity::Fence,
                self.root.fence().to_string(),
                authority.fence().to_string(),
            ));
        }
        if self.root.epoch() != authority.epoch() {
            return Err(refuse(
                RootIdentity::Epoch,
                self.root.epoch().to_string(),
                authority.epoch().to_string(),
            ));
        }
        if self.root.generation() != generation {
            return Err(refuse(
                RootIdentity::Generation,
                self.root.generation().to_string(),
                generation.to_string(),
            ));
        }
        Ok(())
    }
}
