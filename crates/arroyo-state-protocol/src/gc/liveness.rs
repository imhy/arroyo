//! The seam leader garbage collection asks "which files is this table's state made of?"
//! (work-plan item M11.P49, design item M11.D20).
//!
//! # Why this is a seam and not a match
//!
//! A checkpoint manifest carries one [`TableCheckpointMetadata`] per table, and the bytes in
//! its `data` field are in *the backend's* format: a parquet table's payload lists parquet
//! object names, a stateengine table's payload names engine manifests. Until M11.T09-S5
//! [`crate::gc`] decoded the parquet payloads itself, which made this crate a place that has
//! to learn a second format every time a backend is added — and the crate that knows those
//! formats, `arroyo-state`, cannot be depended on from here without a cycle.
//!
//! So the decode is injected. A caller that already knows which backend the job runs on hands
//! in the implementation for it, and this crate keeps the two things that are genuinely its
//! own: which manifest entries are asked about at all, and the validation of the names that
//! come back into [`CheckpointRef`](crate::types::CheckpointRef)s it is willing to delete.
//!
//! # This is a deletion path
//!
//! The answer is not advisory. [`crate::gc::cleanup_leader_checkpoints`] subtracts the files
//! named by *retained* checkpoints from the files named by *expiring* ones and deletes the
//! difference, and the same implementation answers for both — so a name omitted from a
//! retained checkpoint's answer while an expiring one still carries it is a live file
//! classified as collectable. "I cannot answer" and "this table is made of no files" are
//! therefore different answers and have
//! different spellings: the first is an [`Err`], the second is an empty [`Vec`]. An empty
//! vector is a legitimate answer — an expiring table whose files have all aged out records
//! none — which is exactly why it must not double as the failure.
//!
//! Four things keep a failure from being read as an empty list:
//!
//! 1. The method returns [`Result`], and returning `Err` is the only way to decline.
//! 2. A metadata entry that states no table type never reaches an implementation from here:
//!    [`crate::gc`] refuses it first, so "no kind was named" cannot arrive at the classifier
//!    as "no files were named".
//! 3. A single `Err` fails the whole classification, which is strictly before the first
//!    delete — [`crate::gc::delete_classified_history`] takes a checked history, and a
//!    classification that returned an error never produces one.
//! 4. [`CheckpointLiveness::state_backend`] is where the collecting job's backend comes from, so the
//!    implementation that decodes and the selector every manifest is checked against are one
//!    value rather than two a caller states separately.
//!
//! What none of that can check is an implementation that decodes successfully and returns
//! *fewer* names than its payload carries. Nothing in these types distinguishes that from a
//! table with fewer files; it is the implementation's own obligation, and its own tests'.

use arroyo_rpc::grpc::rpc::{TableCheckpointMetadata, TableEnum};
use arroyo_rpc::state_backend::StateBackendSelector;
use thiserror::Error;

/// One state backend's answer to which files a checkpoint's tables keep alive.
///
/// Implemented by the crate that owns the payload format — `arroyo_state::provider` supplies
/// the implementation that consults the installed provider registry — and consumed here as a
/// trait object, so this crate never links against it.
///
/// Implementations are shared across a garbage-collection pass and hold no per-table state;
/// `Send + Sync` is what lets the pass run inside a spawned task.
pub trait CheckpointLiveness: Send + Sync {
    /// The state backend this resolver decodes payloads for.
    ///
    /// This is the collecting job's backend: [`crate::gc::cleanup_leader_checkpoints`] takes
    /// no separate selector and checks every manifest it reads against this value. A resolver
    /// is therefore not something a caller can pick independently of the job — picking one
    /// *is* stating which backend the job runs on, and a resolver whose backend disagrees with
    /// the history refuses the history rather than decoding it.
    fn state_backend(&self) -> StateBackendSelector;

    /// The data files `metadata` names, in the order its payload records them.
    ///
    /// `metadata` is one manifest entry's table metadata. [`crate::gc`] refuses
    /// `TableEnum::MissingTableType` before asking, so nothing it passes here states no kind;
    /// this is a public method with other possible callers, though, so an implementation must
    /// refuse such a metadata rather than answer for it. Order is preserved because it is
    /// the order the caller validates the names in, so a manifest with two unusable names
    /// reports the same one it has always reported.
    ///
    /// # Errors
    ///
    /// Returns [`LivenessRefusal`] when this backend cannot read the payload. Returning
    /// `Ok(vec![])` says the table's state is made of no files, which withdraws those files'
    /// protection from the pass — see this module's header for why the two are spelled
    /// differently.
    fn table_data_files(
        &self,
        metadata: &TableCheckpointMetadata,
    ) -> Result<Vec<String>, LivenessRefusal>;
}

/// Why a [`CheckpointLiveness`] implementation declined to name a table's files.
///
/// Every variant is a refusal to answer, never a partial answer: a pass that receives one of
/// these has classified nothing and deleted nothing.
#[derive(Debug, Error)]
pub enum LivenessRefusal {
    /// This backend has no implementation for the table kind the metadata declares.
    ///
    /// A backend serves every live table kind or none (design item M11.D06), so in practice
    /// this says either that the backend itself is not installed, or that the metadata
    /// declares a kind no backend implements yet.
    #[error(
        "the \"{state_backend}\" state backend has no implementation for {} table metadata",
        table_type.as_str_name()
    )]
    UnservedTableKind {
        /// The backend that was asked.
        state_backend: StateBackendSelector,
        /// The table kind the metadata declares.
        table_type: TableEnum,
    },

    /// The implementation that was asked serves a different table kind than the metadata
    /// declares.
    ///
    /// Reachable only by calling one kind's implementation directly with another kind's
    /// metadata. It is a refusal rather than a decode attempt because protobuf decoding is
    /// permissive: one format's bytes frequently decode as another's, and a shorter file list
    /// obtained that way is what turns a live file into a deletion candidate.
    #[error(
        "{} table metadata cannot be read by the \"{state_backend}\" state backend's {} \
         implementation",
        declared.as_str_name(),
        serves.as_str_name()
    )]
    WrongTableKind {
        /// The backend that was asked.
        state_backend: StateBackendSelector,
        /// The table kind the metadata declares.
        declared: TableEnum,
        /// The table kind the implementation that was asked serves.
        serves: TableEnum,
    },

    /// The payload did not decode as the kind it declares.
    #[error(
        "the table metadata did not decode as {} metadata: {source}",
        table_type.as_str_name()
    )]
    UndecodablePayload {
        /// The table kind the metadata declares.
        table_type: TableEnum,
        /// What the decoder rejected.
        #[source]
        source: prost::DecodeError,
    },
}

#[cfg(test)]
pub(crate) mod fixture {
    //! The parquet payload decode `crate::gc` performed itself until M11.T09-S5.
    //!
    //! The leader-GC suite's manifests are written by fixtures that encode the two parquet
    //! table payloads, so the traversal has to be handed something that can read them. This
    //! is that something, and it is deliberately a *copy of the body that moved* rather than
    //! a stub: the suite's subject is the protocol — which manifest entries are asked about,
    //! which names are validated, what a refusal does to a pass — and it can only stay that
    //! subject if the payloads it collects are real ones.
    //!
    //! Keeping it here rather than moving the suite to `arroyo-state` is what preserves the
    //! evidence: every assertion those tests make about which files survive a cleanup is the
    //! assertion M11.T08 landed, made over the same bytes. That this copy and the real parquet
    //! provider agree is pinned from the other side, by `arroyo-state`'s
    //! `provider::tests::liveness` parity case.

    use super::{CheckpointLiveness, LivenessRefusal};
    use arroyo_rpc::grpc::rpc::{
        ExpiringKeyedTimeTableCheckpointMetadata, GlobalKeyedTableTaskCheckpointMetadata,
        TableCheckpointMetadata, TableEnum,
    };
    use arroyo_rpc::state_backend::StateBackendSelector;
    use prost::Message;

    /// Reads both parquet table payloads, as `gc::table_checkpoint_data_files` did.
    pub(crate) struct ParquetPayloads;

    /// Reads parquet's global key/value payload and refuses its expiring keyed-time one.
    ///
    /// Not a realistic backend — a backend serves every live kind or none — but it is the
    /// shape a deletion-path test needs: a manifest whose *other* tables classified perfectly
    /// well, so that "nothing was deleted" is about the refusal rather than about the pass
    /// having had nothing to do.
    pub(crate) struct RefusesExpiring;

    fn data_files(metadata: &TableCheckpointMetadata) -> Result<Vec<String>, LivenessRefusal> {
        let table_type = metadata.table_type();
        let undecodable = |source| LivenessRefusal::UndecodablePayload { table_type, source };
        match table_type {
            TableEnum::GlobalKeyValue => Ok(GlobalKeyedTableTaskCheckpointMetadata::decode(
                metadata.data.as_slice(),
            )
            .map_err(undecodable)?
            .files),
            TableEnum::ExpiringKeyedTimeTable => Ok(
                ExpiringKeyedTimeTableCheckpointMetadata::decode(metadata.data.as_slice())
                    .map_err(undecodable)?
                    .files
                    .into_iter()
                    .map(|file| file.file)
                    .collect(),
            ),
            TableEnum::MissingTableType => Err(unserved(table_type)),
        }
    }

    /// Reads parquet's payloads but reports the stateengine backend.
    ///
    /// The two halves of a resolver — which backend it says the job is on, and which format it
    /// reads — are deliberately in disagreement here, so that a test can show the *first* half
    /// is what a history is checked against.
    pub(crate) struct ClaimsStateEngine;

    fn unserved(table_type: TableEnum) -> LivenessRefusal {
        LivenessRefusal::UnservedTableKind {
            state_backend: StateBackendSelector::Parquet,
            table_type,
        }
    }

    impl CheckpointLiveness for ParquetPayloads {
        fn state_backend(&self) -> StateBackendSelector {
            StateBackendSelector::Parquet
        }

        fn table_data_files(
            &self,
            metadata: &TableCheckpointMetadata,
        ) -> Result<Vec<String>, LivenessRefusal> {
            data_files(metadata)
        }
    }

    impl CheckpointLiveness for ClaimsStateEngine {
        fn state_backend(&self) -> StateBackendSelector {
            StateBackendSelector::StateEngine
        }

        fn table_data_files(
            &self,
            metadata: &TableCheckpointMetadata,
        ) -> Result<Vec<String>, LivenessRefusal> {
            data_files(metadata)
        }
    }

    impl CheckpointLiveness for RefusesExpiring {
        fn state_backend(&self) -> StateBackendSelector {
            StateBackendSelector::Parquet
        }

        fn table_data_files(
            &self,
            metadata: &TableCheckpointMetadata,
        ) -> Result<Vec<String>, LivenessRefusal> {
            match metadata.table_type() {
                TableEnum::ExpiringKeyedTimeTable => {
                    Err(unserved(TableEnum::ExpiringKeyedTimeTable))
                }
                _ => data_files(metadata),
            }
        }
    }
}
