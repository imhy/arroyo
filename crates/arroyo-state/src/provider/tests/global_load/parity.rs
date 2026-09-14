//! The seam restores exactly what the walk it replaced read — both decode flavours, the
//! union across predecessor subtasks, the order that decides a collision, and the empty
//! cases.

use std::collections::HashMap;

use super::{
    Versioned, drain, expected, legacy_walk, loader_through_the_registry, pairs, restored,
    versioned_config, write_source,
};
use crate::provider::tests::{TABLE, state_channel, storage};
use crate::tables::MigratableState;
use crate::tables::global_key_value_load::{GlobalKeyValuePage, LoadPagePeak};
use crate::{BINCODE_CONFIG, TableData};

/// The union of every predecessor subtask's object is restored, and it is exactly what the
/// legacy walk read.
///
/// Three sources of different sizes — 1, 3 and 1500 entries — so that the number of read
/// batches per source varies independently of everything else: the first two are one batch,
/// the third is two.
#[tokio::test]
async fn the_seam_restores_the_union_of_every_source_exactly_as_the_legacy_walk_did() {
    let storage = storage("global-load-union").await;
    let config = versioned_config(0);

    let files = vec![
        write_source(&storage, &config, 1, pairs(0..1)).await,
        write_source(&storage, &config, 2, pairs(1..4)).await,
        write_source(&storage, &config, 3, pairs(4..1504)).await,
    ];
    let table = restored(&storage, &config, files.clone());

    let (tx, _rx) = state_channel();
    let through_the_seam = crate::tables::global_keyed_map::restore::view::<String, String>(
        TABLE,
        loader_through_the_registry(&table).await,
        tx,
    )
    .await
    .expect("a restored view");

    assert_eq!(*through_the_seam.get_all(), expected(0..1504));
    assert_eq!(
        *through_the_seam.get_all(),
        legacy_walk(&storage, &files).await
    );
    assert_eq!(through_the_seam.get_all().len(), 1504);
}

/// Sources are read in order, so the last object holding a key is the one whose value
/// survives — the semantic arroyo's own restore has (M11.D15c).
#[tokio::test]
async fn sources_are_read_in_order_so_the_last_object_holding_a_key_wins() {
    let storage = storage("global-load-order").await;
    let config = versioned_config(0);

    let first = write_source(
        &storage,
        &config,
        1,
        vec![TableData::KeyedData {
            key: bincode::encode_to_vec("shared".to_string(), BINCODE_CONFIG).unwrap(),
            value: bincode::encode_to_vec("first".to_string(), BINCODE_CONFIG).unwrap(),
        }],
    )
    .await;
    let second = write_source(
        &storage,
        &config,
        2,
        vec![TableData::KeyedData {
            key: bincode::encode_to_vec("shared".to_string(), BINCODE_CONFIG).unwrap(),
            value: bincode::encode_to_vec("second".to_string(), BINCODE_CONFIG).unwrap(),
        }],
    )
    .await;

    for (files, winner) in [
        (vec![first.clone(), second.clone()], "second"),
        (vec![second, first], "first"),
    ] {
        let table = restored(&storage, &config, files.clone());
        let (tx, _rx) = state_channel();
        let view = crate::tables::global_keyed_map::restore::view::<String, String>(
            TABLE,
            loader_through_the_registry(&table).await,
            tx,
        )
        .await
        .expect("a restored view");
        assert_eq!(
            *view.get_all(),
            HashMap::from([("shared".to_string(), winner.to_string())]),
        );
        assert_eq!(*view.get_all(), legacy_walk(&storage, &files).await);
    }
}

/// The migrating decode reads each source under that source's own state version, and
/// produces exactly what a per-file decision produced.
///
/// One object is written under version 0 and one under version 1, in one load: the version
/// 0 entries are migrated and the version 1 entries are not, which is only correct if the
/// decision is taken per source rather than once per load.
#[tokio::test]
async fn the_migrating_decode_reads_each_source_under_its_own_state_version() {
    let storage = storage("global-load-migrate").await;
    let old = versioned_config(0);
    let current = versioned_config(1);

    let files = vec![
        write_source(&storage, &old, 1, pairs(0..2)).await,
        write_source(&storage, &current, 2, migratable_pairs(2..4)).await,
    ];
    let table = restored(&storage, &current, files.clone());

    let (tx, _rx) = state_channel();
    let view = crate::tables::global_keyed_map::restore::view_migratable::<String, Versioned>(
        TABLE,
        loader_through_the_registry(&table).await,
        tx,
    )
    .await
    .expect("a restored view");

    assert_eq!(
        *view.get_all(),
        HashMap::from([
            (
                "k00000".to_string(),
                Versioned("migrated:v00000".to_string())
            ),
            (
                "k00001".to_string(),
                Versioned("migrated:v00001".to_string())
            ),
            ("k00002".to_string(), Versioned("v00002".to_string())),
            ("k00003".to_string(), Versioned("v00003".to_string())),
        ]),
    );
    assert_eq!(view.version_info().1, Versioned::VERSION);
}

/// `Versioned` values, encoded as the current version writes them.
fn migratable_pairs(range: std::ops::Range<usize>) -> Vec<TableData> {
    range
        .map(|i| TableData::KeyedData {
            key: bincode::encode_to_vec(format!("k{i:05}"), BINCODE_CONFIG).expect("encode"),
            value: bincode::encode_to_vec(Versioned(format!("v{i:05}")), BINCODE_CONFIG)
                .expect("encode"),
        })
        .collect()
}

/// A table with no objects and a table with one empty object both restore an empty map,
/// and differ only in that the empty object is still announced.
#[tokio::test]
async fn an_empty_table_and_an_empty_source_both_restore_nothing() {
    let storage = storage("global-load-empty").await;
    let config = versioned_config(0);

    let no_files = restored(&storage, &config, vec![]);
    let (pages, peak, _) = drain(loader_through_the_registry(&no_files).await).await;
    assert_eq!(pages.len(), 0, "no source, no page");
    assert_eq!(peak, LoadPagePeak::ZERO);

    let empty_file = write_source(&storage, &config, 1, vec![]).await;
    let one_empty = restored(&storage, &config, vec![empty_file.clone()]);
    let (pages, peak, _) = drain(loader_through_the_registry(&one_empty).await).await;
    assert_eq!(
        pages
            .iter()
            .map(GlobalKeyValuePage::len)
            .collect::<Vec<_>>(),
        vec![0],
        "an empty source is announced and contributes no data page"
    );
    assert_eq!(pages[0].source(), empty_file);
    assert_eq!(peak, LoadPagePeak::ZERO);

    for table in [&no_files, &one_empty] {
        let (tx, _rx) = state_channel();
        let view = crate::tables::global_keyed_map::restore::view::<String, String>(
            TABLE,
            loader_through_the_registry(table).await,
            tx,
        )
        .await
        .expect("a restored view");
        assert_eq!(*view.get_all(), HashMap::new());
    }
}
