//! Every way a load can refuse: an unreadable state version, a null key, a null value, a
//! source that fails part-way, and a table the provider did not build. Each fails the whole
//! restore with the error the legacy walk raised, and none of them yields a view.

use std::sync::Arc;

use arroyo_rpc::errors::StateError;

use super::{
    Versioned, expected, loader_through_the_registry, pairs, restored, versioned_config,
    write_nullable_source, write_source,
};
use crate::BINCODE_CONFIG;
use crate::provider::GlobalKeyValueProvider;
use crate::provider::tests::{
    TABLE, expiring_config, global_provider, state_channel, storage, task_info,
};
use crate::tables::{ErasedTable, MigratableState};

/// A source more than one version behind is refused, and the refusal names both versions.
///
/// The refusal reaches the decode on the page that announces the source, before any of the
/// source's entries have been read — which is why the loader announces a source with a page
/// of its own.
#[tokio::test]
async fn a_source_more_than_one_version_behind_is_refused() {
    let storage = storage("global-load-version").await;
    let ancient = versioned_config(5);
    let current = versioned_config(1);

    let files = vec![write_source(&storage, &ancient, 1, pairs(0..3)).await];
    let table = restored(&storage, &current, files);

    let (tx, _rx) = state_channel();
    let error = crate::tables::global_keyed_map::restore::view_migratable::<String, Versioned>(
        TABLE,
        loader_through_the_registry(&table).await,
        tx,
    )
    .await
    .map(|_| ())
    .expect_err("version 5 is not one step from version 1");

    match error {
        StateError::UnsupportedStateVersion {
            table,
            found,
            expected,
        } => {
            assert_eq!(table, TABLE);
            assert_eq!(found, 5);
            assert_eq!(expected, Versioned::VERSION);
        }
        other => panic!("expected an unsupported state version, got {other:?}"),
    }

    // The plain decode never read a version, and still does not.
    let table = restored(&storage, &current, vec![]);
    let (tx, _rx) = state_channel();
    assert!(
        crate::tables::global_keyed_map::restore::view::<String, String>(
            TABLE,
            loader_through_the_registry(&table).await,
            tx,
        )
        .await
        .is_ok()
    );
}

/// A null key and a null value are each refused with the message the legacy walk used, and
/// neither yields a view.
#[tokio::test]
async fn a_null_key_and_a_null_value_are_each_refused_with_the_legacy_message() {
    let storage = storage("global-load-null").await;
    let config = versioned_config(0);
    let key = bincode::encode_to_vec("k00000".to_string(), BINCODE_CONFIG).unwrap();
    let value = bincode::encode_to_vec("v00000".to_string(), BINCODE_CONFIG).unwrap();

    for (path, keys, values, message) in [
        (
            "null-key",
            vec![None, Some(key.as_slice())],
            vec![Some(value.as_slice()), Some(value.as_slice())],
            "unexpected null key from record batch",
        ),
        (
            "null-value",
            vec![Some(key.as_slice()), Some(key.as_slice())],
            vec![None, Some(value.as_slice())],
            "unexpected null value from record batch",
        ),
    ] {
        write_nullable_source(&storage, path, keys, values).await;
        let table = restored(&storage, &config, vec![path.to_string()]);

        let (tx, _rx) = state_channel();
        let error = crate::tables::global_keyed_map::restore::view::<String, String>(
            TABLE,
            loader_through_the_registry(&table).await,
            tx,
        )
        .await
        .map(|_| ())
        .expect_err("a null is not a key or a value");

        match error {
            StateError::Other { table, error } => {
                assert_eq!(table, TABLE);
                assert_eq!(error, message);
            }
            other => panic!("expected the legacy refusal, got {other:?}"),
        }
    }
}

/// A source that fails part-way contributes nothing, and leaves the sources before it
/// untouched.
///
/// The load is a good source followed by one whose second row has a null key: the first
/// row of the bad source decodes, the second does not, and the whole restore fails — there
/// is no view carrying the good source's entries plus half the bad one's. Re-reading the
/// good source alone then still produces its full contents, so nothing was consumed.
#[tokio::test]
async fn a_source_that_fails_part_way_contributes_nothing() {
    let storage = storage("global-load-partial").await;
    let config = versioned_config(0);
    let key = bincode::encode_to_vec("k09999".to_string(), BINCODE_CONFIG).unwrap();
    let value = bincode::encode_to_vec("v09999".to_string(), BINCODE_CONFIG).unwrap();

    let good = write_source(&storage, &config, 1, pairs(0..3)).await;
    let bad = "partial-null";
    write_nullable_source(
        &storage,
        bad,
        vec![Some(key.as_slice()), None],
        vec![Some(value.as_slice()), Some(value.as_slice())],
    )
    .await;

    let table = restored(&storage, &config, vec![good.clone(), bad.to_string()]);
    let (tx, _rx) = state_channel();
    let error = crate::tables::global_keyed_map::restore::view::<String, String>(
        TABLE,
        loader_through_the_registry(&table).await,
        tx,
    )
    .await
    .map(|_| ())
    .expect_err("the second source has a null key");
    assert!(
        matches!(&error, StateError::Other { error, .. } if error == "unexpected null key from record batch"),
        "got {error:?}"
    );

    let table = restored(&storage, &config, vec![good.clone()]);
    let (tx, _rx) = state_channel();
    let view = crate::tables::global_keyed_map::restore::view::<String, String>(
        TABLE,
        loader_through_the_registry(&table).await,
        tx,
    )
    .await
    .expect("the good source on its own still restores");
    assert_eq!(*view.get_all(), expected(0..3));
}

/// The provider refuses a table it did not build, with the refusal the manager's own
/// downcast produced before the lookup moved into the provider.
#[tokio::test]
async fn the_provider_refuses_a_table_it_did_not_build() {
    let storage = storage("global-load-wrong-kind").await;
    let expiring: Arc<dyn ErasedTable> = Arc::new(
        <crate::tables::expiring_time_key_map::ExpiringTimeKeyTable as ErasedTable>::from_config(
            expiring_config(),
            task_info(),
            storage,
            None,
        )
        .expect("an expiring table"),
    );

    let error = global_provider()
        .global_key_value_load(TABLE, &*expiring)
        .await
        .map(|_| ())
        .expect_err("an expiring table holds no global key/value state");

    assert!(
        matches!(
            &error,
            StateError::WrongTableKind { table, expected }
                if table == TABLE && *expected == "global_keyed_state"
        ),
        "got {error:?}"
    );
}
