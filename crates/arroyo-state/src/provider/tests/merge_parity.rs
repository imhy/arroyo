//! M11.T09k, merge half — the provider merges one table's subtask reports exactly as the
//! static dispatch it replaces does (risk M11.T09o).
//!
//! Merging is the M11.D20 family the worker's checkpoint controller performs; M11.T11 moves
//! that call site. Proving the parity here is what makes that conversion a change of caller
//! rather than a change of behaviour.

use std::collections::HashMap;

use arroyo_rpc::grpc::rpc::{
    ExpiringKeyedTimeTableCheckpointMetadata, GlobalKeyedTableTaskCheckpointMetadata,
    TableCheckpointMetadata,
};
use prost::Message;

use super::{
    expiring_config, expiring_provider, expiring_subtask, global_config, global_provider,
    global_subtask, time_file,
};
use crate::provider::StateBackendProvider;
use crate::tables::ErasedTable;
use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::tables::global_keyed_map::GlobalKeyedTable;

/// Merging one subtask's report produces byte-identical table metadata through both paths,
/// for both kinds.
///
/// One subtask is what makes byte comparison meaningful here: the implementation being
/// extracted decodes the reports into a fresh `HashMap` and iterates it, so with two or
/// more subtasks the *order* of the merged file list is not fixed by the implementation at
/// all — two calls of the legacy path can disagree with each other. That case is covered by
/// [`merge_checkpoint_metadata_matches_the_legacy_static_path_across_subtasks`], which
/// compares what the implementation does fix. Neither input is the default one: the global
/// report carries commit data under a two-phase-commit config, and the expiring report
/// carries a watermark that retires one of its own files.
#[test]
fn merge_checkpoint_metadata_is_byte_identical_to_the_legacy_static_path() {
    let config = global_config(true);
    let reports = HashMap::from([(0, global_subtask(Some("f0"), Some(b"c0")))]);
    let legacy = <GlobalKeyedTable as ErasedTable>::merge_checkpoint_metadata(
        config.clone(),
        reports.clone(),
    )
    .expect("legacy merge");
    let through_provider = global_provider()
        .merge_checkpoint_metadata(config, reports)
        .expect("provider merge");
    assert_eq!(
        legacy.as_ref().map(Message::encode_to_vec),
        through_provider.as_ref().map(Message::encode_to_vec),
    );
    let merged = GlobalKeyedTableTaskCheckpointMetadata::decode(
        through_provider
            .expect("one subtask reported")
            .data
            .as_slice(),
    )
    .expect("global key/value table metadata");
    assert_eq!(merged.files, vec!["f0".to_string()]);
    assert_eq!(
        merged.commit_data_by_subtask,
        HashMap::from([(0, b"c0".to_vec())]),
    );

    let config = expiring_config();
    let reports = HashMap::from([(
        0,
        expiring_subtask(
            Some(15_000_000),
            vec![
                time_file("retired", 1, 4_000_000),
                time_file("kept", 1, 9_000_000),
            ],
        ),
    )]);
    let legacy = <ExpiringTimeKeyTable as ErasedTable>::merge_checkpoint_metadata(
        config.clone(),
        reports.clone(),
    )
    .expect("legacy merge");
    let through_provider = expiring_provider()
        .merge_checkpoint_metadata(config, reports)
        .expect("provider merge");
    assert_eq!(
        legacy.as_ref().map(Message::encode_to_vec),
        through_provider.as_ref().map(Message::encode_to_vec),
    );
    let merged = ExpiringKeyedTimeTableCheckpointMetadata::decode(
        through_provider
            .expect("one subtask reported")
            .data
            .as_slice(),
    )
    .expect("expiring table metadata");
    assert_eq!(
        merged
            .files
            .into_iter()
            .map(|file| file.file)
            .collect::<Vec<_>>(),
        vec!["kept".to_string()],
        "the file whose newest row predates the retention cutoff is dropped",
    );

    // The empty case, through both paths and for both kinds: no subtask reported, so there
    // is no table metadata rather than empty table metadata.
    for (legacy, through_provider) in [
        (
            <GlobalKeyedTable as ErasedTable>::merge_checkpoint_metadata(
                global_config(true),
                HashMap::new(),
            )
            .expect("legacy merge"),
            global_provider()
                .merge_checkpoint_metadata(global_config(true), HashMap::new())
                .expect("provider merge"),
        ),
        (
            <ExpiringTimeKeyTable as ErasedTable>::merge_checkpoint_metadata(
                expiring_config(),
                HashMap::new(),
            )
            .expect("legacy merge"),
            expiring_provider()
                .merge_checkpoint_metadata(expiring_config(), HashMap::new())
                .expect("provider merge"),
        ),
    ] {
        assert_eq!(legacy, through_provider);
        assert_eq!(through_provider, None);
    }
}

/// Merging reports from several subtasks produces the same table metadata through both
/// paths: the same files, the same deduplication, the same retention cutoff, and the same
/// commit data keyed by the subtask that reported it.
///
/// The merged file list is compared as a set. That is not a weaker assertion made for
/// convenience: the implementation being extracted collects the decoded reports into a
/// fresh `HashMap` and iterates it, so which order the files come out in is not something
/// it decides and not something an extraction could preserve or break. Everything the
/// implementation does decide is asserted against a closed-form value.
#[test]
fn merge_checkpoint_metadata_matches_the_legacy_static_path_across_subtasks() {
    // Global key/value, two-phase commit on: files are collected and commit data is keyed
    // by the subtask that reported it, so three subtasks with three different shapes of
    // report is the non-default case.
    let config = global_config(true);
    let reports = HashMap::from([
        (0, global_subtask(Some("f0"), Some(b"c0"))),
        (1, global_subtask(Some("f1"), None)),
        (2, global_subtask(None, Some(b"c2"))),
    ]);
    let legacy = GlobalKeyedTableTaskCheckpointMetadata::decode(
        <GlobalKeyedTable as ErasedTable>::merge_checkpoint_metadata(
            config.clone(),
            reports.clone(),
        )
        .expect("legacy merge")
        .expect("three subtasks reported")
        .data
        .as_slice(),
    )
    .expect("global key/value table metadata");
    let through_provider = GlobalKeyedTableTaskCheckpointMetadata::decode(
        global_provider()
            .merge_checkpoint_metadata(config, reports)
            .expect("provider merge")
            .expect("three subtasks reported")
            .data
            .as_slice(),
    )
    .expect("global key/value table metadata");

    let sorted = |mut files: Vec<String>| {
        files.sort();
        files
    };
    assert_eq!(
        sorted(legacy.files.clone()),
        sorted(through_provider.files.clone())
    );
    assert_eq!(
        sorted(through_provider.files),
        vec!["f0".to_string(), "f1".to_string()],
    );
    assert_eq!(
        legacy.commit_data_by_subtask,
        through_provider.commit_data_by_subtask
    );
    assert_eq!(
        through_provider.commit_data_by_subtask,
        HashMap::from([(0, b"c0".to_vec()), (2, b"c2".to_vec())]),
        "the subtask that reported no commit data contributes no entry",
    );

    // Expiring keyed time: the minimum watermark less the retention is the cutoff, files
    // below it are dropped, and a file two subtasks both report appears once.
    let config = expiring_config();
    let kept = time_file("kept", 1, 9_000_000);
    let reports = HashMap::from([
        (
            0,
            expiring_subtask(
                Some(15_000_000),
                vec![time_file("retired", 1, 4_000_000), kept.clone()],
            ),
        ),
        (
            1,
            expiring_subtask(
                Some(20_000_000),
                vec![kept.clone(), time_file("newest", 2, 14_000_000)],
            ),
        ),
    ]);
    let names = |metadata: Option<TableCheckpointMetadata>| {
        let mut names: Vec<String> = ExpiringKeyedTimeTableCheckpointMetadata::decode(
            metadata.expect("two subtasks reported").data.as_slice(),
        )
        .expect("expiring table metadata")
        .files
        .into_iter()
        .map(|file| file.file)
        .collect();
        names.sort();
        names
    };
    let legacy = names(
        <ExpiringTimeKeyTable as ErasedTable>::merge_checkpoint_metadata(
            config.clone(),
            reports.clone(),
        )
        .expect("legacy merge"),
    );
    let through_provider = names(
        expiring_provider()
            .merge_checkpoint_metadata(config, reports)
            .expect("provider merge"),
    );
    assert_eq!(legacy, through_provider);
    assert_eq!(
        through_provider,
        vec!["kept".to_string(), "newest".to_string()],
        "the retired file is dropped and the file both subtasks reported appears once",
    );
}
