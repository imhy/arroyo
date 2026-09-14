//! M11.T09j — a second [`install`] is refused, and the registry already in force keeps
//! serving.
//!
//! This is a test binary of its own because the process cell is written at most once and
//! has no reset: the two ways an install can be refused cannot both be reached in one
//! process. The other one is `provider_install_after_first_use.rs`, which gets
//! [`InstallError::DefaultedByFirstUse`] where this one gets
//! [`InstallError::AlreadyInstalled`] — the two are distinguishable precisely because the
//! same call in two different processes returns two different variants.
//!
//! There is exactly one `#[test]` here on purpose. A second one would share this process's
//! cell and would see whatever this one left in it.

use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_state::provider::{InstallError, ProviderRegistry, TableKind, install, registry};

#[test]
fn installing_twice_is_refused_and_the_first_registry_keeps_serving() {
    install(ProviderRegistry::parquet_default()).expect("nothing was installed or looked up yet");

    // An empty registry answers no lookup at all, so if this install took effect the
    // lookups below would fail. That is what makes them evidence that it did not.
    let empty = ProviderRegistry::builder()
        .build()
        .expect("an empty registry is a registry with no backends");
    assert_eq!(install(empty), Err(InstallError::AlreadyInstalled));

    for kind in TableKind::ALL {
        let provider = registry()
            .provider(StateBackendSelector::Parquet, kind)
            .expect("the first registry is still the one in force");
        assert_eq!(provider.selector(), StateBackendSelector::Parquet);
        assert_eq!(provider.table_kind(), kind);
    }
}
