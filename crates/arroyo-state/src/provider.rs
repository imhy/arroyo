//! The object-safe state-backend provider seam and its build-once registry
//! (design items M11.D11, M11.D13, M11.D20).
//!
//! Every runtime decision that used to be made by matching on a [`TableEnum`] is one
//! *operation family*: constructing a table, opening an expiring-time-key view, merging a
//! checkpoint's subtask metadata, extracting committing data, compacting, and naming the
//! files a checkpoint keeps. [`StateBackendProvider`] has one method per family, and a
//! [`ProviderRegistry`] says which implementation serves each
//! `(backend selector, logical table kind)` key.
//!
//! # What is keyed, and why it is a pair
//!
//! The key is a [`StateBackendSelector`] — the typed, normalized value M11.T08 persists
//! and transports — paired with a [`TableKind`]. Both halves are enums, so neither an
//! unknown backend name nor a table config that states no table type can *become* a key:
//! the only way in is [`StateBackendSelector::normalize`] and [`TableKind::of_config`],
//! and each of those fails typed rather than defaulting. A lookup that misses is an
//! error, never a fall back to parquet.
//!
//! `TableEnum` itself is untouched (M11.D11): it stays the logical, on-the-wire table
//! kind, and existing plans and configs keep working. [`TableKind`] is this crate's
//! *live* subset of it — the two variants a job can actually run on — which is what makes
//! "a backend serves every live kind" a statement the registry builder can check.
//!
//! # Where lookups happen
//!
//! A lookup is a shared-reference read of an immutable value; there is no lock and no
//! lazy mutation behind it (risk M11.T09p). It is also never on the record path. The
//! call sites converted so far are:
//!
//! | Call site | Converted by | Frequency |
//! |---|---|---|
//! | [`TableManager::load`] | M11.T09c | once per table, per subtask, when the subtask's state is built |
//! | [`TableManager::get_expiring_time_key_table`] | M11.T09c | once per table name, per subtask — only on the call that finds the view cache empty |
//! | [`TableManager::get_global_keyed_state`] | M11.T09c.01 | once per table name, per subtask — only on the call that finds the view cache empty |
//! | [`TableManager::get_global_keyed_state_migratable`] | M11.T09c.01 | once per table name, per subtask — only on the call that finds the view cache empty |
//! | the worker's leader checkpoint cleanup, through [`ProviderLiveness`] | M11.T09-S5 | once per live table kind when the resolver is built, then once per table of each manifest the pass reads |
//!
//! The remaining M11.D20 sites (checkpoint-metadata merge and committing-data extraction
//! in the worker's checkpoint controller and leader manifest paths, compaction and
//! files-to-keep in [`crate::parquet`], that cleanup's own file-reference classification,
//! and the controller's scheduling restore) still match on `TableEnum` and are converted by
//! M11.T11. Each of those is a per-checkpoint or per-cleanup operation, so none of them
//! moves a lookup onto the record path either.
//!
//! # What each static dispatch point becomes
//!
//! The construct this seam replaces is the associated function on [`ErasedTable`] that
//! takes `where Self: Sized` — one per operation family, each selected by a `TableEnum`
//! match at its call site. Every one of them is listed here with what it becomes, so that
//! "the dispatch was converted" is a claim about the whole family rather than about the
//! site that happened to be edited. The last row is not one of them, and is listed with
//! them because it is the same shape: a `TableEnum` match selecting a payload format, which
//! happened to live in another crate rather than on [`ErasedTable`].
//!
//! | Static dispatch point | Provider method | Converted by |
//! |---|---|---|
//! | `from_config` | [`StateBackendProvider::table`] | M11.T09c, at `TableManager::load` |
//! | `merge_checkpoint_metadata` | [`StateBackendProvider::merge_checkpoint_metadata`] | M11.T11, at the worker checkpoint controller |
//! | `committing_data` | [`StateBackendProvider::committing_data`] | M11.T11, at the worker checkpoint controller, the leader manifest path, and the controller's scheduling restore |
//! | `files_to_keep` | [`StateBackendProvider::files_to_keep`] | M11.T11, at `ParquetBackend::table_files_to_keep` |
//! | `compact_data` | [`StateBackendProvider::compact_data`] | M11.T11, at `ParquetBackend::compact_loaded_operator` |
//! | `table_type` | — | Not dispatch. It is a table type's statement of which kind it is, which is what a provider's [`StateBackendProvider::table_kind`] is registered against; it stays. |
//! | `checked_proto_decode` | — | Not dispatch. It is the decode helper the others are built from, and it is called with a concrete type already chosen. |
//! | `arroyo_state_protocol::gc`'s own parquet payload decode | [`StateBackendProvider::table_data_files`] | M11.T09-S5, at the worker's leader cleanup, through [`ProviderLiveness`] |
//!
//! That last row is the protocol-owned GC liveness resolver (work-plan item M11.P49). The
//! question it asks — which files does this table's checkpoint payload name — is one both
//! live kinds answer, so it is a [`StateBackendProvider`] method rather than a sub-trait, and
//! the value the protocol consumes is a [`ProviderLiveness`] over a registry rather than a
//! third slot in one. See [`mod@liveness`].
//!
//! # Families only one kind has
//!
//! Two operation families belong to one live table kind and not the other: opening an
//! expiring-time-key view, and starting a bounded-page global key/value load (design item
//! M11.D15c). Neither is a method on [`StateBackendProvider`], because a method that is
//! meaningless for half the providers that must implement it is a method that will be
//! called on the wrong one. Each is a **sub-trait** — [`ExpiringTimeKeyProvider`] and
//! [`GlobalKeyValueProvider`] — and the registry slot for that kind holds the sub-trait, so
//! the provider a lookup returns already has the method and the provider for the other kind
//! does not have it to be called by mistake. [`ProviderRegistry::provider`] still answers
//! with `&dyn StateBackendProvider` for the families both kinds share, by upcasting out of
//! the slot.
//!
//! [`TableManager::load`]: crate::tables::table_manager::TableManager::load
//! [`TableManager::get_expiring_time_key_table`]: crate::tables::table_manager::TableManager::get_expiring_time_key_table
//! [`TableManager::get_global_keyed_state`]: crate::tables::table_manager::TableManager::get_global_keyed_state
//! [`TableManager::get_global_keyed_state_migratable`]: crate::tables::table_manager::TableManager::get_global_keyed_state_migratable
//! [`StateBackendSelector::normalize`]: arroyo_rpc::state_backend::StateBackendSelector::normalize

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use arroyo_rpc::errors::StateError;
use arroyo_rpc::grpc::rpc::{
    OperatorMetadata, TableCheckpointMetadata, TableConfig, TableEnum,
    TableSubtaskCheckpointMetadata,
};
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_state_protocol::gc::liveness::LivenessRefusal;
use arroyo_storage::StorageProviderRef;
use arroyo_types::TaskInfo;
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

use crate::StateMessage;
use crate::tables::expiring_time_key_view::ExpiringTimeKeyViewApi;
use crate::tables::global_key_value_load::GlobalKeyValueLoad;
use crate::tables::{CompactionConfig, ErasedTable};
use crate::validated::ValidatedTable;

pub mod installed;
pub mod liveness;
pub mod parquet;
pub mod registry;

#[cfg(test)]
mod tests;

pub use installed::{InstallError, install, registry};
pub use liveness::{ProviderLiveness, liveness};
pub use registry::{LookupError, ProviderRegistry, ProviderRegistryBuilder, RegistryError};

/// A logical table kind a job can actually run on.
///
/// [`TableEnum`] has a third variant, `MissingTableType`, which is the protobuf default
/// and therefore means "this config states no kind" rather than naming one. That variant
/// is deliberately absent here: a registry key, and every provider dispatch built on one,
/// is reachable only from a value that named a kind. Converting in from `TableEnum` is
/// the only way to obtain one, and it fails typed (M11.D06 requires both of these
/// variants to be served, which is what makes the pair exhaustive rather than a subset
/// someone chose).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableKind {
    /// A plain global key/value table, `TableEnum::GlobalKeyValue`.
    GlobalKeyValue,
    /// An expiring keyed-time table, `TableEnum::ExpiringKeyedTimeTable`.
    ExpiringKeyedTime,
}

impl TableKind {
    /// Every live kind, in a fixed order.
    ///
    /// The registry builder walks this to check that a backend serves all of them, so a
    /// third live kind added to [`TableEnum`] shows up here — and in the exhaustive match
    /// in [`Self::from_table_enum`] — before it can be forgotten.
    pub const ALL: [Self; 2] = [Self::GlobalKeyValue, Self::ExpiringKeyedTime];

    /// The wire kind this names.
    pub const fn as_table_enum(self) -> TableEnum {
        match self {
            Self::GlobalKeyValue => TableEnum::GlobalKeyValue,
            Self::ExpiringKeyedTime => TableEnum::ExpiringKeyedTimeTable,
        }
    }

    /// The live kind `table_type` names, or `None` for `MissingTableType`.
    pub const fn from_table_enum(table_type: TableEnum) -> Option<Self> {
        match table_type {
            TableEnum::MissingTableType => None,
            TableEnum::GlobalKeyValue => Some(Self::GlobalKeyValue),
            TableEnum::ExpiringKeyedTimeTable => Some(Self::ExpiringKeyedTime),
        }
    }

    /// The live kind `config` states, for the table called `table_name`.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Other`] when the config states no table type. That is the
    /// same refusal the `TableEnum::MissingTableType` arm of every dispatch match makes
    /// today: a config that names no kind cannot select an implementation, and choosing
    /// one for it would construct state in a format the config does not describe.
    pub fn of_config(table_name: &str, config: &TableConfig) -> Result<Self, StateError> {
        Self::from_table_enum(config.table_type()).ok_or_else(|| StateError::Other {
            table: table_name.to_string(),
            error: format!(
                "the table config for {table_name} states no table type, so no state \
                 backend provider can serve it"
            ),
        })
    }

    /// The name used for this kind in registry errors and provider documentation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GlobalKeyValue => "global key/value",
            Self::ExpiringKeyedTime => "expiring keyed time",
        }
    }
}

impl fmt::Display for TableKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One state backend's implementation of one logical table kind (design item M11.D11).
///
/// A provider is registered under the `(selector, kind)` pair it reports from
/// [`Self::selector`] and [`Self::table_kind`], and every method below is that pair's
/// answer to one M11.D20 operation family. Providers are immutable and shared: the
/// registry hands out `&dyn StateBackendProvider`, several threads may call one
/// concurrently, and a provider therefore holds no per-table or per-job state — each
/// method takes everything it needs.
///
/// The trait is object-safe and its methods name only public types, so a crate outside
/// `arroyo-state` can implement it; `tests/provider_seam.rs` is that implementation,
/// compiled the way another crate would compile it.
#[async_trait]
pub trait StateBackendProvider: Send + Sync + 'static {
    /// The backend this provider implements.
    ///
    /// This is the typed selector M11.T08 persists, not a free string: the registry keys
    /// on the value returned here, so a backend name that does not exist cannot be
    /// registered under one.
    fn selector(&self) -> StateBackendSelector;

    /// The logical table kind this provider serves.
    ///
    /// The registry checks this against the slot a provider is being registered into, so
    /// a provider placed in the wrong slot is refused at build rather than constructing
    /// the wrong kind of table at runtime.
    fn table_kind(&self) -> TableKind;

    /// Builds one subtask's table from its config, restoring `checkpoint` when there is
    /// one.
    ///
    /// # Errors
    ///
    /// Returns the [`StateError`] the backend raises for a config or checkpoint it
    /// cannot read — a config of the wrong kind, an undecodable message, or an
    /// unsupported state version.
    fn table(
        &self,
        config: TableConfig,
        task_info: Arc<TaskInfo>,
        storage: StorageProviderRef,
        checkpoint: Option<TableCheckpointMetadata>,
    ) -> Result<Arc<dyn ErasedTable>, StateError>;

    /// Merges every subtask's report of one table into the table's checkpoint metadata,
    /// or `None` when the table has no state at this epoch.
    ///
    /// # Errors
    ///
    /// Returns the [`StateError`] raised by a config or subtask metadata this backend
    /// cannot decode.
    fn merge_checkpoint_metadata(
        &self,
        config: TableConfig,
        subtask_metadata: HashMap<u32, TableSubtaskCheckpointMetadata>,
    ) -> Result<Option<TableCheckpointMetadata>, StateError>;

    /// The per-subtask committing data one table's checkpoint metadata carries, or `None`
    /// when the table commits nothing.
    fn committing_data(
        &self,
        config: TableConfig,
        table_metadata: &TableCheckpointMetadata,
    ) -> Option<HashMap<u32, Vec<u8>>>;

    /// The files one validated table's checkpoint metadata references.
    ///
    /// Takes a [`ValidatedTable`] for the reason [`ErasedTable::files_to_keep`] does
    /// (design item M11.D39c): this classification is what a cleanup subtracts to decide
    /// what it may delete, so it must not be reachable from a config and a metadata that
    /// no whole-checkpoint check covered.
    ///
    /// # Errors
    ///
    /// Returns the [`StateError`] raised by metadata this backend cannot decode.
    fn files_to_keep(&self, table: ValidatedTable<'_>) -> Result<HashSet<String>, StateError>;

    /// The data files one table's checkpoint metadata names, in the order its payload
    /// records them.
    ///
    /// This is the backend's half of the protocol's GC liveness seam (work-plan item
    /// M11.P49): `arroyo-state-protocol` walks a checkpoint manifest's operators and tables
    /// and validates the names that come back, and this is what reads them out of the
    /// backend-specific bytes in between. [`ProviderLiveness`] is the adapter that routes one
    /// manifest entry to the provider for the kind it declares.
    ///
    /// It is a deletion path. The names returned here are what protect those files from a
    /// leader cleanup, so `Ok(vec![])` says this table's state is made of no files — a real
    /// answer an expiring table whose files have aged out gives — and declining to answer is
    /// [`Err`]. The two must not be merged; see
    /// [`arroyo_state_protocol::gc::liveness`] for what does and does not enforce that.
    ///
    /// The list is deliberately ordered and deliberately not a set: it is
    /// [`Self::files_to_keep`]'s answer before deduplication, so the two cannot disagree about
    /// which files a payload names, and the order is the one the protocol validates the names
    /// in.
    ///
    /// # Errors
    ///
    /// Returns [`LivenessRefusal::WrongTableKind`] when `metadata` declares a kind this
    /// provider does not serve — protobuf decoding is permissive enough that one format's
    /// bytes often decode as another's, and a shortened file list obtained that way is exactly
    /// what turns a live file into a deletion candidate — and
    /// [`LivenessRefusal::UndecodablePayload`] when the bytes are not this kind's message.
    fn table_data_files(
        &self,
        metadata: &TableCheckpointMetadata,
    ) -> Result<Vec<String>, LivenessRefusal>;

    /// Compacts one table's checkpoint metadata, returning the replacement metadata or
    /// `None` when nothing was compacted.
    ///
    /// # Errors
    ///
    /// Returns the [`StateError`] raised by decoding failures and by the storage this
    /// compaction reads and writes.
    async fn compact_data(
        &self,
        config: TableConfig,
        compaction_config: &CompactionConfig,
        operator_metadata: &OperatorMetadata,
        current_metadata: TableCheckpointMetadata,
    ) -> Result<Option<TableCheckpointMetadata>, StateError>;
}

/// The provider of [`TableKind::GlobalKeyValue`], which additionally starts loads
/// (design item M11.D15c).
///
/// Restoring a global keyed table is a family only this kind has, so — like view creation
/// for the other kind — it is a method only this kind's providers carry. The registry keeps
/// the global slot as a `dyn GlobalKeyValueProvider`, so
/// [`ProviderRegistry::global_key_value_provider`] hands back something that can start a
/// load without any downcast, and an expiring-time-key provider has no
/// `global_key_value_load` to be called by mistake.
///
/// The seam is a *loader*, not a view, because `GlobalKeyedView<K, V>` is generic over
/// `K: Key, V: Data` and is therefore not object-safe. The loader yields bounded pages of
/// `(key bytes, value bytes)`; the bincode decode and the view construction stay above it,
/// unchanged for both backends.
#[async_trait]
pub trait GlobalKeyValueProvider: StateBackendProvider {
    /// Starts a bounded-page load of `table`, which this provider built.
    ///
    /// `table_name` names the table for diagnostics; a provider handed a table it does not
    /// recognise cannot recover a name from it, and the refusal has to say which table was
    /// asked for.
    ///
    /// The returned loader is owned by the caller and owns whatever it reads from, so its
    /// lifetime is not tied to `table`. It performs its I/O in
    /// [`GlobalKeyValueLoad::next_page`]: this call is `async` so that a backend which must
    /// reach storage to open a load can, not because the parquet implementation does.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::WrongTableKind`] when `table` was not built by this provider,
    /// and whatever [`StateError`] preparing to read the table's restored state raises.
    async fn global_key_value_load(
        &self,
        table_name: &str,
        table: &dyn ErasedTable,
    ) -> Result<Box<dyn GlobalKeyValueLoad + Send>, StateError>;
}

/// The provider of [`TableKind::ExpiringKeyedTime`], which additionally opens views
/// (design item M11.D13).
///
/// View creation is a family only this kind has, so it is a method only this kind's
/// providers carry. The registry keeps the expiring slot as a
/// `dyn ExpiringTimeKeyProvider`, so [`ProviderRegistry::expiring_time_key_provider`]
/// hands back something that can open a view without any downcast, and a global
/// key/value provider has no `expiring_time_key_view` to be called by mistake.
#[async_trait]
pub trait ExpiringTimeKeyProvider: StateBackendProvider {
    /// Opens an owned view of `table`, which this provider built.
    ///
    /// `table_name` names the table for diagnostics; a provider handed a table it does
    /// not recognise cannot recover a name from it, and the refusal has to say which
    /// table was asked for. `state_tx` is the subtask's state channel, which the view
    /// writes through, and `watermark` fixes the retention cutoff the view starts from.
    ///
    /// The returned view is owned by the caller — typically [`TableManager`]'s view cache
    /// — so its lifetime is not tied to `table`.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::WrongTableKind`] when `table` was not built by this
    /// provider, and whatever [`StateError`] reading the table's restored state raises.
    ///
    /// [`TableManager`]: crate::tables::table_manager::TableManager
    async fn expiring_time_key_view(
        &self,
        table_name: &str,
        table: &dyn ErasedTable,
        state_tx: Sender<StateMessage>,
        watermark: Option<SystemTime>,
    ) -> Result<Box<dyn ExpiringTimeKeyViewApi + Send>, StateError>;
}
