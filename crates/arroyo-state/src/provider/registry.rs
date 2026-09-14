//! The build-once provider registry: the value, and the rules it is built under
//! (design item M11.D11, work-plan item M11.P49, risk M11.T09p).
//!
//! # The value
//!
//! [`ProviderRegistry`] is an ordinary immutable value. It is built through
//! [`ProviderRegistryBuilder`], which is where every registration rule is enforced, and
//! once built it is never mutated: a lookup is a shared-reference read of an array, with
//! no lock, no interior mutability and no lazy initialisation behind it. Tests build
//! registries and call them directly, which is why none of them needs a global to be
//! resettable.
//!
//! Production reads the registry through [`registry`](super::registry()), which resolves a
//! single write-once process cell; that cell, and the rule that it is written at most
//! once, live in [`super::installed`].
//!
//! # Registration versus installation
//!
//! These are two different rules and they resolve differently.
//!
//! *Registration* is per `(selector, kind)` key, and a second provider for one key is
//! [`RegistryError::DuplicateRegistration`]. There is no "the same provider twice is
//! fine" exemption: two registrations for one key mean two pieces of code each believe
//! they own that key, and which one wins would then depend on their order.
//!
//! *Installation* is per process, and a second [`super::install`] is refused without
//! disturbing anything — the registry already in force keeps serving. That refusal reports
//! a sequencing mistake; it is not a corrupted process.

use std::fmt;
use std::sync::Arc;

use arroyo_rpc::errors::StateError;
use arroyo_rpc::state_backend::StateBackendSelector;
use thiserror::Error;

use super::parquet::ParquetProvider;
use super::{ExpiringTimeKeyProvider, GlobalKeyValueProvider, StateBackendProvider, TableKind};
use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::tables::global_keyed_map::GlobalKeyedTable;

/// How many backends [`ALL_SELECTORS`] covers, and therefore how wide a registry is.
const SELECTOR_COUNT: usize = 2;

/// Every backend a job can select, in the order [`selector_index`] assigns.
const ALL_SELECTORS: [StateBackendSelector; SELECTOR_COUNT] = [
    StateBackendSelector::Parquet,
    StateBackendSelector::StateEngine,
];

/// The slot a selector occupies.
///
/// Exhaustive over [`StateBackendSelector`], so a third backend fails to compile here
/// rather than silently sharing a slot.
const fn selector_index(selector: StateBackendSelector) -> usize {
    match selector {
        StateBackendSelector::Parquet => 0,
        StateBackendSelector::StateEngine => 1,
    }
}

/// The providers one backend supplies, which is always every live table kind.
///
/// This is the shape that makes M11.D06's rule structural rather than checked at lookup:
/// a backend is present in a registry only as a complete pair, so "registered for one
/// kind only" is not a state a built registry can be in.
///
/// Each slot holds that kind's **sub-trait**, not [`StateBackendProvider`]: the family only
/// that kind has — starting a load, opening a view — is reachable from the slot without a
/// downcast, and is absent from the other slot's type. [`ProviderRegistry::provider`]
/// upcasts out of a slot for the families both kinds share.
struct SelectorProviders {
    global_key_value: Arc<dyn GlobalKeyValueProvider>,
    expiring_keyed_time: Arc<dyn ExpiringTimeKeyProvider>,
}

/// An immutable map from `(backend selector, live table kind)` to the provider that
/// serves it.
///
/// Built once through [`Self::builder`], then only read. Lookups take `&self`, touch no
/// synchronisation primitive, and allocate nothing.
pub struct ProviderRegistry {
    selectors: [Option<SelectorProviders>; SELECTOR_COUNT],
}

impl ProviderRegistry {
    /// An empty builder.
    pub fn builder() -> ProviderRegistryBuilder {
        ProviderRegistryBuilder::default()
    }

    /// The registry Arroyo has always behaved as if it had: parquet, serving both live
    /// table kinds, and nothing else.
    ///
    /// This is what [`registry`](super::registry()) falls back to when no registry was installed, so an
    /// executable that never installs one is not a process with no providers — it is a
    /// parquet-only process, which is what every Arroyo build was before M11.T09.
    pub fn parquet_default() -> Self {
        Self::builder()
            .register_global_key_value(Arc::new(ParquetProvider::<GlobalKeyedTable>::new()))
            .and_then(|builder| {
                builder.register_expiring_keyed_time(Arc::new(ParquetProvider::<
                    ExpiringTimeKeyTable,
                >::new()))
            })
            .and_then(ProviderRegistryBuilder::build)
            .expect(
                "the parquet default registers one provider per live table kind for one \
                 selector, which is exactly what the builder requires",
            )
    }

    /// The provider for `selector` and `kind`.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::NoProvider`] when this registry has no provider for
    /// `selector`. A registry never holds a backend for only some kinds — the builder
    /// refuses to produce one — so a miss means the backend itself was never registered.
    /// It is never answered with another backend's provider.
    ///
    /// The slots hold each kind's sub-trait, so this is an upcast to the trait the families
    /// both kinds share; a caller that needs a kind-specific family asks for the slot's own
    /// trait through [`Self::global_key_value_provider`] or
    /// [`Self::expiring_time_key_provider`] instead.
    pub fn provider(
        &self,
        selector: StateBackendSelector,
        kind: TableKind,
    ) -> Result<&dyn StateBackendProvider, LookupError> {
        let entry = self.entry(selector, kind)?;
        Ok(match kind {
            TableKind::GlobalKeyValue => &*entry.global_key_value,
            TableKind::ExpiringKeyedTime => &*entry.expiring_keyed_time,
        })
    }

    /// The provider for `selector`'s global key/value tables, which can also start loads.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::NoProvider`], naming [`TableKind::GlobalKeyValue`], when this
    /// registry has no provider for `selector`.
    pub fn global_key_value_provider(
        &self,
        selector: StateBackendSelector,
    ) -> Result<&dyn GlobalKeyValueProvider, LookupError> {
        Ok(&*self
            .entry(selector, TableKind::GlobalKeyValue)?
            .global_key_value)
    }

    /// The provider for `selector`'s expiring keyed-time tables, which can also open
    /// views.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::NoProvider`], naming [`TableKind::ExpiringKeyedTime`], when
    /// this registry has no provider for `selector`.
    pub fn expiring_time_key_provider(
        &self,
        selector: StateBackendSelector,
    ) -> Result<&dyn ExpiringTimeKeyProvider, LookupError> {
        Ok(&*self
            .entry(selector, TableKind::ExpiringKeyedTime)?
            .expiring_keyed_time)
    }

    fn entry(
        &self,
        selector: StateBackendSelector,
        kind: TableKind,
    ) -> Result<&SelectorProviders, LookupError> {
        self.selectors[selector_index(selector)]
            .as_ref()
            .ok_or(LookupError::NoProvider { selector, kind })
    }
}

impl fmt::Debug for ProviderRegistry {
    /// Lists the backends this registry serves. The providers themselves are trait
    /// objects with nothing printable beyond the key they were registered under.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderRegistry")
            .field(
                "selectors",
                &self
                    .selectors
                    .iter()
                    .zip(ALL_SELECTORS)
                    .filter(|(entry, _)| entry.is_some())
                    .map(|(_, selector)| selector.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Collects providers and validates them into a [`ProviderRegistry`].
///
/// Each registration names the slot it fills, and the provider's own
/// [`StateBackendProvider::table_kind`] is checked against that slot, so a provider
/// cannot be filed under a kind it does not serve.
#[derive(Default)]
pub struct ProviderRegistryBuilder {
    global_key_value: [Option<Arc<dyn GlobalKeyValueProvider>>; SELECTOR_COUNT],
    expiring_keyed_time: [Option<Arc<dyn ExpiringTimeKeyProvider>>; SELECTOR_COUNT],
}

impl fmt::Debug for ProviderRegistryBuilder {
    /// Lists the `(backend, kind)` keys registered so far. The providers themselves are
    /// trait objects with nothing printable beyond the key they were registered under.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut keys = Vec::new();
        for (index, selector) in ALL_SELECTORS.into_iter().enumerate() {
            if self.global_key_value[index].is_some() {
                keys.push((selector.as_str(), TableKind::GlobalKeyValue));
            }
            if self.expiring_keyed_time[index].is_some() {
                keys.push((selector.as_str(), TableKind::ExpiringKeyedTime));
            }
        }
        f.debug_struct("ProviderRegistryBuilder")
            .field("registered", &keys)
            .finish()
    }
}

impl ProviderRegistryBuilder {
    /// Registers `provider` as the global key/value provider for the backend it reports.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::KindMismatch`] if `provider` does not serve
    /// [`TableKind::GlobalKeyValue`], and [`RegistryError::DuplicateRegistration`] if that
    /// backend already has one.
    pub fn register_global_key_value(
        mut self,
        provider: Arc<dyn GlobalKeyValueProvider>,
    ) -> Result<Self, RegistryError> {
        let selector = provider.selector();
        check_slot(
            selector,
            TableKind::GlobalKeyValue,
            provider.table_kind(),
            self.global_key_value[selector_index(selector)].is_some(),
        )?;
        self.global_key_value[selector_index(selector)] = Some(provider);
        Ok(self)
    }

    /// Registers `provider` as the expiring keyed-time provider for the backend it
    /// reports.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::KindMismatch`] if `provider` does not serve
    /// [`TableKind::ExpiringKeyedTime`], and [`RegistryError::DuplicateRegistration`] if
    /// that backend already has one.
    pub fn register_expiring_keyed_time(
        mut self,
        provider: Arc<dyn ExpiringTimeKeyProvider>,
    ) -> Result<Self, RegistryError> {
        let selector = provider.selector();
        check_slot(
            selector,
            TableKind::ExpiringKeyedTime,
            provider.table_kind(),
            self.expiring_keyed_time[selector_index(selector)].is_some(),
        )?;
        self.expiring_keyed_time[selector_index(selector)] = Some(provider);
        Ok(self)
    }

    /// Validates the registrations and freezes them into a [`ProviderRegistry`].
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::IncompleteSelector`] if any backend was registered for
    /// one live table kind but not the other. That is design item M11.D06's requirement:
    /// a whole pipeline creates tables of both kinds, so a backend that serves only one
    /// of them cannot run a job — and the alternative to refusing it here is falling back
    /// to parquet for the other kind, which would silently split one job's state across
    /// two backends.
    pub fn build(self) -> Result<ProviderRegistry, RegistryError> {
        let Self {
            global_key_value,
            expiring_keyed_time,
        } = self;
        let mut selectors: [Option<SelectorProviders>; SELECTOR_COUNT] =
            [const { None }; SELECTOR_COUNT];

        for ((slot, selector), (global, expiring)) in selectors
            .iter_mut()
            .zip(ALL_SELECTORS)
            .zip(global_key_value.into_iter().zip(expiring_keyed_time))
        {
            *slot = match (global, expiring) {
                (None, None) => None,
                (Some(global_key_value), Some(expiring_keyed_time)) => Some(SelectorProviders {
                    global_key_value,
                    expiring_keyed_time,
                }),
                (Some(_), None) => {
                    return Err(RegistryError::IncompleteSelector {
                        selector,
                        present: TableKind::GlobalKeyValue,
                        missing: TableKind::ExpiringKeyedTime,
                    });
                }
                (None, Some(_)) => {
                    return Err(RegistryError::IncompleteSelector {
                        selector,
                        present: TableKind::ExpiringKeyedTime,
                        missing: TableKind::GlobalKeyValue,
                    });
                }
            };
        }

        Ok(ProviderRegistry { selectors })
    }
}

/// The shared half of both registrations: the provider serves the slot's kind, and the
/// slot is empty.
fn check_slot(
    selector: StateBackendSelector,
    slot: TableKind,
    declared: TableKind,
    occupied: bool,
) -> Result<(), RegistryError> {
    if declared != slot {
        return Err(RegistryError::KindMismatch {
            selector,
            slot,
            declared,
        });
    }
    if occupied {
        return Err(RegistryError::DuplicateRegistration {
            selector,
            kind: slot,
        });
    }
    Ok(())
}

/// A registration, or a set of registrations, that does not describe usable backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RegistryError {
    /// Two providers claim one `(selector, kind)` key.
    #[error(
        "state backend \"{selector}\" already has a {kind} provider registered; one \
         provider serves one (state backend, table kind) key"
    )]
    DuplicateRegistration {
        /// The backend both providers claim.
        selector: StateBackendSelector,
        /// The table kind both providers claim.
        kind: TableKind,
    },

    /// A provider was registered as a kind it does not serve.
    #[error(
        "the provider registered for state backend \"{selector}\" as its {slot} provider \
         serves {declared} tables"
    )]
    KindMismatch {
        /// The backend the provider reports.
        selector: StateBackendSelector,
        /// The kind the registration filed it under.
        slot: TableKind,
        /// The kind the provider says it serves.
        declared: TableKind,
    },

    /// A backend serves one live table kind but not the other.
    #[error(
        "state backend \"{selector}\" has a {present} provider but no {missing} provider; \
         a backend serves every live table kind or none, because a job's tables are not \
         split across backends"
    )]
    IncompleteSelector {
        /// The incompletely registered backend.
        selector: StateBackendSelector,
        /// A kind that backend does serve.
        present: TableKind,
        /// A kind it does not.
        missing: TableKind,
    },
}

/// Why a registry lookup did not produce a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum LookupError {
    /// The registry holds no provider for this backend at all.
    ///
    /// There is deliberately no "registered for the other kind" variant: the builder
    /// refuses to produce a registry in that state, so the condition does not exist at
    /// lookup time.
    #[error(
        "no state backend provider is registered for \"{selector}\", so its {kind} tables \
         cannot be served; a missing provider is never answered with parquet"
    )]
    NoProvider {
        /// The backend that was looked up.
        selector: StateBackendSelector,
        /// The table kind that was looked up.
        kind: TableKind,
    },
}

impl LookupError {
    /// This failure in the [`StateError`] vocabulary the table APIs speak, naming the
    /// table whose provider was looked up.
    ///
    /// The rendered message is this error's own, so the reason stays exactly as typed
    /// here; what the adaptation adds is which table the caller was serving when the
    /// lookup missed.
    pub fn for_table(self, table: &str) -> StateError {
        StateError::Other {
            table: table.to_string(),
            error: self.to_string(),
        }
    }
}
