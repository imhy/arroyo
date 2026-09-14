//! M11.T09-S5 — the protocol-owned GC liveness resolver, and the parquet payload reading it
//! moved out of `arroyo-state-protocol` (work-plan item M11.P49, risk M11.T09o).
//!
//! The parity oracle below is a verbatim copy of the body that moved, kept as the
//! characterization the move is compared against. The whole-manifest walk it is exercised
//! through is the protocol's and is unchanged; that half is pinned by the 80 leader-GC cases
//! in `arroyo-state-protocol`, which now run against an injected resolver and assert exactly
//! the files they always did.

use std::collections::HashSet;

use arroyo_rpc::grpc::rpc::{
    CheckpointManifest, ExpiringKeyedTimeTableCheckpointMetadata,
    GlobalKeyedTableTaskCheckpointMetadata, OperatorCheckpointMetadata, OperatorMetadata,
    ParquetTimeFile, TableCheckpointMetadata, TableEnum,
};
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_state_protocol::gc::liveness::{CheckpointLiveness, LivenessRefusal};
use prost::Message;

use super::fake::fake_registry;
use super::{expiring_provider, global_provider};
use crate::provider::{
    LookupError, ProviderLiveness, ProviderRegistry, StateBackendProvider, TableKind,
};
use crate::tables::Table;
use crate::tables::expiring_time_key_map::ExpiringTimeKeyTable;
use crate::tables::global_keyed_map::GlobalKeyedTable;

/// The decode `arroyo_state_protocol::gc::table_checkpoint_data_files` performed itself before
/// M11.T09-S5, copied unchanged.
///
/// This is the "before" the move is measured against: a hand-written expectation alone would
/// say only that the new path agrees with what this test's author believed, so every case
/// below asserts against both this and a closed-form list.
fn legacy_table_data_files(metadata: &TableCheckpointMetadata) -> Vec<String> {
    let mut files = vec![];
    match metadata.table_type() {
        TableEnum::MissingTableType => panic!("the legacy body refused this before decoding"),
        TableEnum::GlobalKeyValue => {
            let metadata = GlobalKeyedTableTaskCheckpointMetadata::decode(metadata.data.as_slice())
                .expect("fixture payload");
            for file in metadata.files {
                files.push(file.clone());
            }
        }
        TableEnum::ExpiringKeyedTimeTable => {
            let metadata =
                ExpiringKeyedTimeTableCheckpointMetadata::decode(metadata.data.as_slice())
                    .expect("fixture payload");
            for file in metadata.files {
                files.push(file.file);
            }
        }
    }
    files
}

/// The protocol's walk over a manifest, with the per-entry decode left as a parameter.
///
/// One walk, two decoders, so a difference between the two runs is a difference in the decode
/// rather than in the order the entries were visited.
fn walk(
    manifest: &CheckpointManifest,
    mut entry: impl FnMut(&TableCheckpointMetadata) -> Vec<String>,
) -> Vec<String> {
    let mut files = vec![];
    for operator in &manifest.operators {
        for metadata in operator.table_checkpoint_metadata.values() {
            files.extend(entry(metadata));
        }
    }
    files
}

fn global_metadata(files: &[&str]) -> TableCheckpointMetadata {
    TableCheckpointMetadata {
        table_type: TableEnum::GlobalKeyValue.into(),
        data: GlobalKeyedTableTaskCheckpointMetadata {
            files: files.iter().map(|f| (*f).to_string()).collect(),
            commit_data_by_subtask: Default::default(),
        }
        .encode_to_vec(),
    }
}

fn expiring_metadata(files: &[&str]) -> TableCheckpointMetadata {
    TableCheckpointMetadata {
        table_type: TableEnum::ExpiringKeyedTimeTable.into(),
        data: ExpiringKeyedTimeTableCheckpointMetadata {
            files: files
                .iter()
                .map(|f| ParquetTimeFile {
                    file: (*f).to_string(),
                    ..Default::default()
                })
                .collect(),
        }
        .encode_to_vec(),
    }
}

/// One manifest entry: one operator carrying exactly one table, so the walk's order is the
/// order of `operators` and nothing about a map's iteration enters the expectation.
fn operator(
    operator_id: &str,
    table: &str,
    metadata: TableCheckpointMetadata,
) -> OperatorCheckpointMetadata {
    OperatorCheckpointMetadata {
        operator_metadata: Some(OperatorMetadata {
            job_id: "J".to_string(),
            operator_id: operator_id.to_string(),
            epoch: 1,
            min_watermark: None,
            max_watermark: None,
            parallelism: 1,
        }),
        start_time: 0,
        finish_time: 0,
        table_checkpoint_metadata: [(table.to_string(), metadata)].into(),
        table_configs: Default::default(),
    }
}

fn manifest(operators: Vec<OperatorCheckpointMetadata>) -> CheckpointManifest {
    CheckpointManifest {
        pipeline_id: "P".to_string(),
        job_id: "J".to_string(),
        generation: 1,
        epoch: 1,
        operators,
        ..Default::default()
    }
}

/// The parquet registry, as a resolver.
fn parquet_liveness(registry: &ProviderRegistry) -> ProviderLiveness<'_> {
    ProviderLiveness::new(registry, StateBackendSelector::Parquet)
        .expect("the parquet default registry serves parquet")
}

/// A manifest carrying both live kinds, several files each, yields exactly the files the
/// pre-move decode yields, in the same order.
///
/// Three operators rather than one, and one of them with no files at all, because the three
/// dimensions that could each hide a bug — which kind, how many files, how many operators —
/// are otherwise varied only together.
#[test]
fn provider_liveness_names_exactly_the_files_the_parquet_payloads_do() {
    let registry = ProviderRegistry::parquet_default();
    let liveness = parquet_liveness(&registry);

    let manifest = manifest(vec![
        operator("op-a", "global", global_metadata(&["a/0", "a/1", "a/2"])),
        operator("op-b", "expiring", expiring_metadata(&["b/0", "b/1"])),
        operator("op-c", "global", global_metadata(&[])),
    ]);

    let before = walk(&manifest, legacy_table_data_files);
    let after = walk(&manifest, |metadata| {
        liveness
            .table_data_files(metadata)
            .expect("both kinds are served")
    });

    assert_eq!(before, after);
    assert_eq!(
        after,
        vec![
            "a/0".to_string(),
            "a/1".to_string(),
            "a/2".to_string(),
            "b/0".to_string(),
            "b/1".to_string()
        ]
    );
}

/// Kind, file count and operator count each varied on their own.
///
/// The expectation is built by the same generator that builds the manifest, so it states what
/// the fixture contains rather than what the decoder returned; the decoder's agreement with
/// the pre-move body is asserted on every row as well.
#[test]
fn liveness_varies_independently_over_kind_file_count_and_operator_count() {
    let registry = ProviderRegistry::parquet_default();
    let liveness = parquet_liveness(&registry);

    for kind in TableKind::ALL {
        for file_count in [0usize, 1, 3] {
            for operator_count in [1usize, 2, 3] {
                let mut operators = vec![];
                let mut expected = vec![];
                for op in 0..operator_count {
                    let names: Vec<String> =
                        (0..file_count).map(|f| format!("op{op}/file{f}")).collect();
                    let borrowed: Vec<&str> = names.iter().map(String::as_str).collect();
                    expected.extend(names.clone());
                    operators.push(operator(
                        &format!("op{op}"),
                        "t",
                        match kind {
                            TableKind::GlobalKeyValue => global_metadata(&borrowed),
                            TableKind::ExpiringKeyedTime => expiring_metadata(&borrowed),
                        },
                    ));
                }

                let manifest = manifest(operators);
                let after = walk(&manifest, |metadata| {
                    liveness.table_data_files(metadata).expect("kind is served")
                });
                assert_eq!(
                    after,
                    walk(&manifest, legacy_table_data_files),
                    "{kind} x {file_count} files x {operator_count} operators"
                );
                assert_eq!(
                    after, expected,
                    "{kind} x {file_count} files x {operator_count} operators"
                );
            }
        }
    }
}

/// A table that names no files is answered, not refused.
///
/// The fail-closed rule is about [`Err`], not about emptiness: an expiring table whose files
/// have all aged out records none, and a cleanup that treated that as a failure would never
/// collect such a job at all.
#[test]
fn a_table_with_no_files_is_an_answer_and_not_a_refusal() {
    let registry = ProviderRegistry::parquet_default();
    let liveness = parquet_liveness(&registry);

    assert_eq!(
        liveness
            .table_data_files(&global_metadata(&[]))
            .expect("an empty global table is a table"),
        Vec::<String>::new()
    );
    assert_eq!(
        liveness
            .table_data_files(&expiring_metadata(&[]))
            .expect("an expiring table whose files aged out is a table"),
        Vec::<String>::new()
    );
}

/// A resolver reports the backend its providers were looked up under, and routes each entry to
/// that backend's provider for the kind the entry declares.
///
/// The registry here holds four distinctly-identified fakes, so the assertion is about *which*
/// provider answered rather than only that some provider did. This is the agreement the seam
/// makes structural: there is no constructor that produces a resolver reporting one backend
/// and consulting another's providers.
#[test]
fn a_resolver_reports_and_consults_one_backend() {
    let registry = fake_registry();

    for (selector, global, expiring) in [
        (
            StateBackendSelector::Parquet,
            "parquet-global",
            "parquet-expiring",
        ),
        (
            StateBackendSelector::StateEngine,
            "engine-global",
            "engine-expiring",
        ),
    ] {
        let liveness = ProviderLiveness::new(&registry, selector).expect("both backends served");
        assert_eq!(liveness.state_backend(), selector);
        assert_eq!(
            liveness.table_data_files(&global_metadata(&[])).unwrap(),
            vec![global.to_string()]
        );
        assert_eq!(
            liveness.table_data_files(&expiring_metadata(&[])).unwrap(),
            vec![expiring.to_string()]
        );
    }
}

/// A backend the registry does not serve has no resolver at all, and the refusal comes before
/// a pass begins.
///
/// Checking at construction is what stops a manifest that declares no tables — which asks no
/// questions of a resolver — from authorizing the deletion of a history's manifests, markers
/// and epoch records under a backend this process cannot read.
#[test]
fn a_backend_the_registry_does_not_serve_has_no_resolver() {
    let registry = ProviderRegistry::parquet_default();
    let refused = ProviderLiveness::new(&registry, StateBackendSelector::StateEngine)
        .expect_err("the parquet default registry serves only parquet");

    assert_eq!(
        refused,
        LookupError::NoProvider {
            selector: StateBackendSelector::StateEngine,
            kind: TableKind::GlobalKeyValue,
        }
    );
    assert_eq!(
        refused.to_string(),
        "no state backend provider is registered for \"stateengine\", so its global key/value \
         tables cannot be served; a missing provider is never answered with parquet"
    );
}

/// A metadata that declares no live kind is refused rather than defaulted to one.
///
/// `MissingTableType` is the protobuf default, so this is what a writer that never set the
/// field produces. The protocol refuses it before asking a resolver; this is the resolver's own
/// answer for the same input, because a public method may be reached without that walk.
#[test]
fn a_metadata_that_declares_no_live_kind_is_refused() {
    let registry = ProviderRegistry::parquet_default();
    let liveness = parquet_liveness(&registry);

    let refusal = liveness
        .table_data_files(&TableCheckpointMetadata::default())
        .expect_err("no kind selects no provider");

    assert!(
        matches!(
            refusal,
            LivenessRefusal::UnservedTableKind {
                state_backend: StateBackendSelector::Parquet,
                table_type: TableEnum::MissingTableType,
            }
        ),
        "{refusal:?}"
    );
    assert_eq!(
        refusal.to_string(),
        "the \"parquet\" state backend has no implementation for MissingTableType table metadata"
    );
}

/// A provider handed another kind's payload refuses it instead of decoding it.
///
/// This is the fail-open the kind check closes. `prost` skips fields it does not recognise, so
/// one kind's bytes can decode into a well-formed message of the other and produce a *shorter*
/// file list — and on the leader cleanup path a shorter list is not a diagnostic, it is a set
/// of files that stopped being protected. Both directions are pinned, because the check is on
/// the relationship and not on one of the two kinds.
#[test]
fn a_provider_refuses_a_payload_that_declares_another_kind() {
    let refusal = global_provider()
        .table_data_files(&expiring_metadata(&["x"]))
        .expect_err("the global provider does not read expiring payloads");
    assert_eq!(
        refusal.to_string(),
        "ExpiringKeyedTimeTable table metadata cannot be read by the \"parquet\" state \
         backend's GlobalKeyValue implementation"
    );

    let refusal = expiring_provider()
        .table_data_files(&global_metadata(&["x"]))
        .expect_err("the expiring provider does not read global payloads");
    assert_eq!(
        refusal.to_string(),
        "GlobalKeyValue table metadata cannot be read by the \"parquet\" state backend's \
         ExpiringKeyedTimeTable implementation"
    );
}

/// Bytes that are not the declared kind's message are reported as an undecodable payload, not
/// as a table with no files.
#[test]
fn a_payload_that_is_not_this_kinds_message_is_refused() {
    let undecodable = TableCheckpointMetadata {
        table_type: TableEnum::GlobalKeyValue.into(),
        // Field 1 as a varint, where the message declares a length-delimited string.
        data: vec![0x08, 0x01],
    };

    let refusal = global_provider()
        .table_data_files(&undecodable)
        .expect_err("these bytes are not a GlobalKeyedTableTaskCheckpointMetadata");
    assert!(
        matches!(
            refusal,
            LivenessRefusal::UndecodablePayload {
                table_type: TableEnum::GlobalKeyValue,
                ..
            }
        ),
        "{refusal:?}"
    );
}

/// The ordered list leader GC reads and the set a checkpoint cleanup subtracts name the same
/// files, for both kinds.
///
/// They are two questions about one payload — which is why `Table::files_to_keep` is now
/// derived from `Table::data_files` rather than repeating which field holds a name. The
/// payloads below carry a repeated file so that the deduplication is observable and the two
/// answers are not trivially the same value.
#[test]
fn the_ordered_list_and_the_files_to_keep_set_name_the_same_files() {
    let global = GlobalKeyedTableTaskCheckpointMetadata {
        files: vec!["f0".to_string(), "f1".to_string(), "f0".to_string()],
        commit_data_by_subtask: Default::default(),
    };
    assert_eq!(
        <GlobalKeyedTable as Table>::data_files(&global),
        vec!["f0".to_string(), "f1".to_string(), "f0".to_string()]
    );
    assert_eq!(
        <GlobalKeyedTable as Table>::files_to_keep(Default::default(), global).unwrap(),
        HashSet::from(["f0".to_string(), "f1".to_string()])
    );

    let expiring = ExpiringKeyedTimeTableCheckpointMetadata {
        files: ["g0", "g1", "g0"]
            .into_iter()
            .map(|file| ParquetTimeFile {
                file: file.to_string(),
                ..Default::default()
            })
            .collect(),
    };
    assert_eq!(
        <ExpiringTimeKeyTable as Table>::data_files(&expiring),
        vec!["g0".to_string(), "g1".to_string(), "g0".to_string()]
    );
    assert_eq!(
        <ExpiringTimeKeyTable as Table>::files_to_keep(Default::default(), expiring).unwrap(),
        HashSet::from(["g0".to_string(), "g1".to_string()])
    );
}
