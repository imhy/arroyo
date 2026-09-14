//! The provider registry as the checkpoint protocol's GC liveness resolver (work-plan item
//! M11.P49, design item M11.D20).
//!
//! `arroyo-state-protocol` deletes a job's expiring checkpoints, and to do that it has to know
//! which files each table's checkpoint metadata keeps alive. Those bytes are in the backend's
//! own format, and the crate that knows the formats is this one — which the protocol crate
//! cannot depend on. [`ProviderLiveness`] is the value that closes that: it implements the
//! protocol's [`CheckpointLiveness`] seam by routing each manifest entry to the provider
//! registered for the kind the entry declares.
//!
//! # One value, not two
//!
//! `cleanup_leader_checkpoints` used to take the job's [`StateBackendSelector`] and decode the
//! parquet payloads itself, so "which backend is this job on" and "whose format are these
//! bytes in" were two independent statements that nothing compared. It now takes this resolver
//! instead, and answers the first question by asking it: the selector it reports is the
//! selector the providers were looked up under, because a [`ProviderLiveness`] cannot be built
//! any other way. A caller cannot hand leader GC a parquet resolver for a stateengine job,
//! because the resolver *is* the statement of which backend the job runs on — and if the
//! statement is wrong for the history being collected, the protocol's own per-manifest
//! selector check refuses the history rather than decoding it.
//!
//! # Fail-closed
//!
//! Every way this can fail to answer is an [`Err`], never a short list:
//!
//! - A backend with no providers in the registry is refused by [`ProviderLiveness::new`],
//!   before a pass begins. That matters even though the per-table lookup below refuses too: a
//!   manifest with no tables in it asks no questions, and without the constructor's check such
//!   a pass would delete a history's manifests, markers and epoch records under a backend
//!   nothing is installed for.
//! - A metadata entry declaring a kind that is not live — which is what `MissingTableType` is
//!   — is refused rather than defaulted to a kind.
//! - A registry miss is refused, and is never answered with another backend's provider; that
//!   rule belongs to [`ProviderRegistry`] and is not restated here.
//! - A payload that declares one kind and decodes as another is refused by the provider, not
//!   decoded permissively.

use arroyo_rpc::grpc::rpc::TableCheckpointMetadata;
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_state_protocol::gc::liveness::{CheckpointLiveness, LivenessRefusal};

use super::{LookupError, ProviderRegistry, TableKind, registry};

/// The GC liveness resolver for one backend, answered from one registry.
///
/// Borrows the registry rather than owning or cloning it: the production registry is the
/// process cell's, which lives for the process, and a test's is a local value the test built.
/// Holds no per-pass state, so one of these can serve a whole garbage-collection pass.
#[derive(Debug)]
pub struct ProviderLiveness<'a> {
    registry: &'a ProviderRegistry,
    selector: StateBackendSelector,
}

impl<'a> ProviderLiveness<'a> {
    /// The resolver for `selector`, answered from `registry`.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::NoProvider`] when `registry` serves no provider for `selector`.
    /// Checking here, rather than only when a table is asked about, is what keeps a manifest
    /// that happens to declare no tables from authorizing a deletion under a backend this
    /// process cannot read. Every live [`TableKind`] is looked up, so a third kind added to
    /// [`TableKind::ALL`] is checked here without this function being edited.
    pub fn new(
        registry: &'a ProviderRegistry,
        selector: StateBackendSelector,
    ) -> Result<Self, LookupError> {
        for kind in TableKind::ALL {
            registry.provider(selector, kind)?;
        }
        Ok(Self { registry, selector })
    }
}

/// The GC liveness resolver for `selector`, answered from the registry this process runs on.
///
/// This is what a worker's leader cleanup hands to
/// [`cleanup_leader_checkpoints`](arroyo_state_protocol::gc::cleanup_leader_checkpoints).
/// Calling it fixes the process registry if nothing has installed one, exactly as any other
/// provider lookup does — see [`registry`](super::registry()).
///
/// # Errors
///
/// Returns [`LookupError::NoProvider`] when this process's registry serves no provider for
/// `selector`; the cleanup is then refused rather than run through another backend's decoder.
pub fn liveness(selector: StateBackendSelector) -> Result<ProviderLiveness<'static>, LookupError> {
    ProviderLiveness::new(registry(), selector)
}

impl CheckpointLiveness for ProviderLiveness<'_> {
    fn state_backend(&self) -> StateBackendSelector {
        self.selector
    }

    fn table_data_files(
        &self,
        metadata: &TableCheckpointMetadata,
    ) -> Result<Vec<String>, LivenessRefusal> {
        let table_type = metadata.table_type();
        let unserved = || LivenessRefusal::UnservedTableKind {
            state_backend: self.selector,
            table_type,
        };

        // The kind comes out of the metadata the caller is asking about, which is what makes
        // the provider this selects the provider for *these* bytes rather than for a kind
        // stated somewhere else.
        let kind = TableKind::from_table_enum(table_type).ok_or_else(unserved)?;
        self.registry
            .provider(self.selector, kind)
            .map_err(|_| unserved())?
            .table_data_files(metadata)
    }
}
