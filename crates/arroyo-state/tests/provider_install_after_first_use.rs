//! M11.T09j — an [`install`] after the first lookup defaulted a registry in is refused,
//! with a different error than an install after an install.
//!
//! A process that never installs anything behaves exactly as Arroyo did before the provider
//! seam existed: the first lookup fixes the parquet-only default. That is a decision, not a
//! read, which is why the install that follows it cannot take effect — and why it says so
//! with [`InstallError::DefaultedByFirstUse`] rather than
//! [`InstallError::AlreadyInstalled`], which is what `provider_install_once.rs` gets.
//!
//! There is exactly one `#[test]` here on purpose. A second one would share this process's
//! cell and would see whatever this one left in it.

use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_state::provider::{
    InstallError, LookupError, ProviderRegistry, TableKind, install, registry,
};

#[test]
fn installing_after_a_first_lookup_is_refused_and_the_default_keeps_serving() {
    // Nothing has been installed, so this lookup is also what fixes the registry.
    for kind in TableKind::ALL {
        let provider = registry()
            .provider(StateBackendSelector::Parquet, kind)
            .expect("a process that installs nothing runs on parquet");
        assert_eq!(provider.selector(), StateBackendSelector::Parquet);
        assert_eq!(provider.table_kind(), kind);
    }
    assert_eq!(
        registry()
            .provider(StateBackendSelector::StateEngine, TableKind::GlobalKeyValue)
            .err(),
        Some(LookupError::NoProvider {
            selector: StateBackendSelector::StateEngine,
            kind: TableKind::GlobalKeyValue,
        }),
        "the default registry installed a backend nothing implements here",
    );

    let empty = ProviderRegistry::builder()
        .build()
        .expect("an empty registry is a registry with no backends");
    assert_eq!(install(empty), Err(InstallError::DefaultedByFirstUse));

    for kind in TableKind::ALL {
        let provider = registry()
            .provider(StateBackendSelector::Parquet, kind)
            .expect("the defaulted registry is still the one in force");
        assert_eq!(provider.table_kind(), kind);
    }
}
