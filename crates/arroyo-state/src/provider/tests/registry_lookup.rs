//! M11.T09j, lookup half — what a built registry answers, and what it refuses to answer.

use arroyo_rpc::state_backend::StateBackendSelector;

use super::fake::{address_of, fake_registry, id_of};
use crate::provider::{LookupError, ProviderRegistry, TableKind};

/// A backend with no provider is a typed miss for every kind — never parquet's provider.
#[test]
fn an_unregistered_selector_is_a_typed_error_and_never_the_parquet_provider() {
    let registry = ProviderRegistry::parquet_default();

    for kind in TableKind::ALL {
        assert_eq!(
            registry
                .provider(StateBackendSelector::StateEngine, kind)
                .err(),
            Some(LookupError::NoProvider {
                selector: StateBackendSelector::StateEngine,
                kind,
            }),
            "a stateengine {kind} lookup was answered"
        );
        // The control: the same lookup against the backend that *is* registered succeeds,
        // so the miss above is about the selector and not about the kind.
        let parquet = registry
            .provider(StateBackendSelector::Parquet, kind)
            .expect("parquet serves every live kind");
        assert_eq!(parquet.selector(), StateBackendSelector::Parquet);
        assert_eq!(parquet.table_kind(), kind);
    }

    assert_eq!(
        registry
            .expiring_time_key_provider(StateBackendSelector::StateEngine)
            .err(),
        Some(LookupError::NoProvider {
            selector: StateBackendSelector::StateEngine,
            kind: TableKind::ExpiringKeyedTime,
        }),
    );
}

/// An empty registry answers nothing, for either kind of either backend.
///
/// This is the only shape in which a kind can be unregistered: a backend present for one
/// kind and absent for the other is refused at build, so "unregistered kind" and
/// "unregistered backend" are one condition rather than two.
#[test]
fn an_empty_registry_answers_no_lookup() {
    let registry = ProviderRegistry::builder()
        .build()
        .expect("an empty registry is a registry with no backends, not an invalid one");

    for selector in [
        StateBackendSelector::Parquet,
        StateBackendSelector::StateEngine,
    ] {
        for kind in TableKind::ALL {
            assert_eq!(
                registry.provider(selector, kind).err(),
                Some(LookupError::NoProvider { selector, kind }),
            );
        }
        assert_eq!(
            registry.expiring_time_key_provider(selector).err(),
            Some(LookupError::NoProvider {
                selector,
                kind: TableKind::ExpiringKeyedTime,
            }),
        );
    }
}

/// Both spellings of the default backend normalize to one key and reach one provider, and
/// an unrecognized spelling never becomes a key at all.
#[test]
fn normalized_spellings_reach_one_provider_and_an_unknown_one_is_no_key() {
    let registry = fake_registry();

    let empty = StateBackendSelector::normalize("", "job").expect("empty means parquet");
    let spelled = StateBackendSelector::normalize("parquet", "job").expect("parquet is parquet");
    assert_eq!(empty, StateBackendSelector::Parquet);
    assert_eq!(spelled, StateBackendSelector::Parquet);

    for kind in TableKind::ALL {
        let from_empty = registry.provider(empty, kind).expect("registered");
        let from_spelled = registry.provider(spelled, kind).expect("registered");
        assert_eq!(
            address_of(from_empty),
            address_of(from_spelled),
            "{kind}: the two spellings of parquet reached different providers"
        );
    }

    // A different spelling is a different key, not the same one.
    let engine = StateBackendSelector::normalize("stateengine", "job").expect("known backend");
    assert_eq!(
        id_of(
            registry
                .provider(engine, TableKind::GlobalKeyValue)
                .expect("registered")
        ),
        b"engine-global".to_vec(),
    );
    assert_eq!(
        id_of(
            registry
                .provider(empty, TableKind::GlobalKeyValue)
                .expect("registered")
        ),
        b"parquet-global".to_vec(),
    );

    // And an unrecognized value cannot be normalized at all, so no lookup can be made with
    // it: there is no path from a raw string to a key that does not pass through here.
    for raw in ["Parquet", " parquet", "rocksdb", "stateengine "] {
        assert!(
            StateBackendSelector::normalize(raw, "job").is_err(),
            "{raw:?} normalized into a key"
        );
    }
}

/// Many threads looking up at once get the same provider objects and the same answers.
///
/// The assertion is identity, not absence of panic: every thread compares the address of
/// what it was handed against the address the main thread captured before any thread
/// started, and reads the provider's identity probe. A lookup that mutated, replaced or
/// rebuilt anything would show up as a different address.
#[test]
fn concurrent_lookups_return_the_same_providers() {
    let registry = fake_registry();

    let expected: Vec<(StateBackendSelector, TableKind, usize, Vec<u8>)> = [
        StateBackendSelector::Parquet,
        StateBackendSelector::StateEngine,
    ]
    .into_iter()
    .flat_map(|selector| TableKind::ALL.map(|kind| (selector, kind)))
    .map(|(selector, kind)| {
        let provider = registry.provider(selector, kind).expect("registered");
        (selector, kind, address_of(provider), id_of(provider))
    })
    .collect();
    assert_eq!(expected.len(), 4, "four keys are registered");

    std::thread::scope(|scope| {
        for _ in 0..8 {
            let registry = &registry;
            let expected = &expected;
            scope.spawn(move || {
                for _ in 0..200 {
                    for (selector, kind, address, id) in expected {
                        let provider = registry.provider(*selector, *kind).expect("registered");
                        assert_eq!(
                            address_of(provider),
                            *address,
                            "{selector} {kind}: a concurrent lookup returned a different provider"
                        );
                        assert_eq!(&id_of(provider), id);
                    }
                    let expiring = registry
                        .expiring_time_key_provider(StateBackendSelector::StateEngine)
                        .expect("registered");
                    assert_eq!(expiring.table_kind(), TableKind::ExpiringKeyedTime);
                    assert_eq!(expiring.selector(), StateBackendSelector::StateEngine);
                }
            });
        }
    });
}

/// The default registry is parquet, complete, and nothing else — and it is produced by the
/// same fallible builder every other registry is, so its infallibility is a fact about the
/// registrations rather than about the constructor.
#[test]
fn the_parquet_default_registry_serves_every_live_kind_of_parquet_only() {
    let registry = ProviderRegistry::parquet_default();

    for kind in TableKind::ALL {
        let provider = registry
            .provider(StateBackendSelector::Parquet, kind)
            .expect("the default serves every live kind");
        assert_eq!(provider.selector(), StateBackendSelector::Parquet);
        assert_eq!(provider.table_kind(), kind);
    }
    assert_eq!(
        registry
            .expiring_time_key_provider(StateBackendSelector::Parquet)
            .expect("the default opens views")
            .table_kind(),
        TableKind::ExpiringKeyedTime,
    );
    assert_eq!(
        registry
            .provider(StateBackendSelector::StateEngine, TableKind::GlobalKeyValue)
            .err(),
        Some(LookupError::NoProvider {
            selector: StateBackendSelector::StateEngine,
            kind: TableKind::GlobalKeyValue,
        }),
        "the default installed a provider for a backend it does not implement"
    );
}
