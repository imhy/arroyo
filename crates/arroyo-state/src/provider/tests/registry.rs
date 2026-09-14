//! M11.T09j, registration half — which sets of registrations the builder will produce a
//! registry from.
//!
//! Every case here builds a [`ProviderRegistry`] value directly. None of them touches the
//! process cell, which is why none of them needs it to be resettable; the cell's own two
//! cases are `tests/provider_install_once.rs` and
//! `tests/provider_install_after_first_use.rs`, one per process.

use std::sync::Arc;

use arroyo_rpc::state_backend::StateBackendSelector;

use super::fake::FakeProvider;
use crate::provider::{ProviderRegistry, ProviderRegistryBuilder, RegistryError, TableKind};

/// Two providers claiming one `(selector, kind)` key are refused, and the refusal names
/// both halves of the key.
///
/// The two halves are varied independently: the same builder that refuses the pair accepts
/// each value on its own under a different partner, so the rule is about the pair rather
/// than about either the selector or the kind.
#[test]
fn duplicate_registration_for_one_key_is_refused_naming_the_selector_and_the_kind() {
    let err = ProviderRegistry::builder()
        .register_global_key_value(FakeProvider::erased(
            StateBackendSelector::Parquet,
            TableKind::GlobalKeyValue,
            "first",
        ))
        .and_then(|b| {
            b.register_global_key_value(FakeProvider::erased(
                StateBackendSelector::Parquet,
                TableKind::GlobalKeyValue,
                "second",
            ))
        })
        .expect_err("one key, two providers");
    assert_eq!(
        err,
        RegistryError::DuplicateRegistration {
            selector: StateBackendSelector::Parquet,
            kind: TableKind::GlobalKeyValue,
        }
    );
    let rendered = err.to_string();
    assert!(rendered.contains("parquet"), "{rendered}");
    assert!(rendered.contains("global key/value"), "{rendered}");

    let err = ProviderRegistry::builder()
        .register_expiring_keyed_time(FakeProvider::expiring(
            StateBackendSelector::StateEngine,
            "first",
        ))
        .and_then(|b| {
            b.register_expiring_keyed_time(FakeProvider::expiring(
                StateBackendSelector::StateEngine,
                "second",
            ))
        })
        .expect_err("one key, two providers");
    assert_eq!(
        err,
        RegistryError::DuplicateRegistration {
            selector: StateBackendSelector::StateEngine,
            kind: TableKind::ExpiringKeyedTime,
        }
    );
    let rendered = err.to_string();
    assert!(rendered.contains("stateengine"), "{rendered}");
    assert!(rendered.contains("expiring keyed time"), "{rendered}");

    // The control: neither half of the key is refused on its own.
    ProviderRegistry::builder()
        .register_global_key_value(FakeProvider::erased(
            StateBackendSelector::Parquet,
            TableKind::GlobalKeyValue,
            "parquet",
        ))
        .and_then(|b| {
            b.register_global_key_value(FakeProvider::erased(
                StateBackendSelector::StateEngine,
                TableKind::GlobalKeyValue,
                "engine",
            ))
        })
        .and_then(|b| {
            b.register_expiring_keyed_time(FakeProvider::expiring(
                StateBackendSelector::Parquet,
                "parquet",
            ))
        })
        .expect("three distinct keys");
}

/// A provider filed under a kind it does not serve is refused at registration, in both
/// directions.
#[test]
fn a_provider_registered_as_a_kind_it_does_not_serve_is_refused() {
    let err = ProviderRegistry::builder()
        .register_global_key_value(FakeProvider::erased(
            StateBackendSelector::Parquet,
            TableKind::ExpiringKeyedTime,
            "mis-filed",
        ))
        .expect_err("an expiring provider is not the global key/value provider");
    assert_eq!(
        err,
        RegistryError::KindMismatch {
            selector: StateBackendSelector::Parquet,
            slot: TableKind::GlobalKeyValue,
            declared: TableKind::ExpiringKeyedTime,
        }
    );

    let err = ProviderRegistry::builder()
        .register_expiring_keyed_time(Arc::new(FakeProvider {
            selector: StateBackendSelector::Parquet,
            kind: TableKind::GlobalKeyValue,
            id: "mis-filed",
        }))
        .expect_err("a global key/value provider is not the expiring provider");
    assert_eq!(
        err,
        RegistryError::KindMismatch {
            selector: StateBackendSelector::Parquet,
            slot: TableKind::ExpiringKeyedTime,
            declared: TableKind::GlobalKeyValue,
        }
    );
}

/// A backend registered for one live kind and not the other is refused when the registry is
/// **built**, so no such registry ever exists to be looked up in.
///
/// Both directions, and once more with an otherwise-complete second backend present, so the
/// refusal is not an artifact of the registry being otherwise empty.
#[test]
fn a_selector_with_one_live_kind_fails_at_build_not_at_lookup() {
    let err = ProviderRegistry::builder()
        .register_global_key_value(FakeProvider::erased(
            StateBackendSelector::StateEngine,
            TableKind::GlobalKeyValue,
            "engine-global",
        ))
        .and_then(ProviderRegistryBuilder::build)
        .expect_err("a backend with no expiring provider cannot run a job");
    assert_eq!(
        err,
        RegistryError::IncompleteSelector {
            selector: StateBackendSelector::StateEngine,
            present: TableKind::GlobalKeyValue,
            missing: TableKind::ExpiringKeyedTime,
        }
    );

    let err = ProviderRegistry::builder()
        .register_expiring_keyed_time(FakeProvider::expiring(
            StateBackendSelector::StateEngine,
            "engine-expiring",
        ))
        .and_then(ProviderRegistryBuilder::build)
        .expect_err("a backend with no global key/value provider cannot run a job");
    assert_eq!(
        err,
        RegistryError::IncompleteSelector {
            selector: StateBackendSelector::StateEngine,
            present: TableKind::ExpiringKeyedTime,
            missing: TableKind::GlobalKeyValue,
        }
    );

    let err = ProviderRegistry::builder()
        .register_global_key_value(FakeProvider::erased(
            StateBackendSelector::Parquet,
            TableKind::GlobalKeyValue,
            "parquet-global",
        ))
        .and_then(|b| {
            b.register_expiring_keyed_time(FakeProvider::expiring(
                StateBackendSelector::Parquet,
                "parquet-expiring",
            ))
        })
        .and_then(|b| {
            b.register_global_key_value(FakeProvider::erased(
                StateBackendSelector::StateEngine,
                TableKind::GlobalKeyValue,
                "engine-global",
            ))
        })
        .and_then(ProviderRegistryBuilder::build)
        .expect_err("one complete backend does not excuse an incomplete one");
    assert_eq!(
        err,
        RegistryError::IncompleteSelector {
            selector: StateBackendSelector::StateEngine,
            present: TableKind::GlobalKeyValue,
            missing: TableKind::ExpiringKeyedTime,
        }
    );

    // The control: the same registrations with the missing half supplied do build.
    ProviderRegistry::builder()
        .register_global_key_value(FakeProvider::erased(
            StateBackendSelector::StateEngine,
            TableKind::GlobalKeyValue,
            "engine-global",
        ))
        .and_then(|b| {
            b.register_expiring_keyed_time(FakeProvider::expiring(
                StateBackendSelector::StateEngine,
                "engine-expiring",
            ))
        })
        .and_then(ProviderRegistryBuilder::build)
        .expect("a backend that serves both live kinds is complete");
}
