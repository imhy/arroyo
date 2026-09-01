//! The field-number allocation for the fence protocol, pinned against `proto/rpc.proto`.
//!
//! A protobuf field number is the only part of a message that is load-bearing on the wire, and
//! reusing one is silent: a peer running an older build encodes the old field at that number and
//! a newer build decodes it as the new one, with no error anywhere. The five messages M11.P54a
//! extends have between them two numbers that were used and abandoned before anybody thought to
//! reserve them, so "the next free number" is not the same question as "the next number nobody
//! has ever used".
//!
//! These tests read the `.proto` itself rather than the generated Rust, because the generated
//! Rust is downstream of the mistake they exist to catch.

use std::collections::{BTreeMap, BTreeSet};

/// The schema these tests are about, read at compile time from the crate's own `proto/`.
const RPC_PROTO: &str = include_str!("../../proto/rpc.proto");

/// One `message` block's field numbers, by field name, and the numbers it reserves.
struct MessageTags {
    fields: BTreeMap<String, u32>,
    reserved: BTreeSet<u32>,
}

/// Every `message` block in `RPC_PROTO`, by message name.
///
/// The file has no nested messages and no `oneof`, so a block is everything between
/// `message <Name> {` and the next line that is exactly `}`.
fn parse_messages() -> BTreeMap<String, MessageTags> {
    let mut messages = BTreeMap::new();
    let mut current: Option<(String, MessageTags)> = None;

    for line in RPC_PROTO.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("message ") {
            // `message X {}` on one line is a message with no fields; `message X {` opens a
            // block that runs to the next line that is exactly `}`.
            let (name, empty) = match rest.trim().strip_suffix("{}") {
                Some(name) => (name.trim().to_string(), true),
                None => (rest.trim_end_matches('{').trim().to_string(), false),
            };
            assert!(
                current.is_none(),
                "nested message {name}: this parser assumes rpc.proto has none"
            );
            if empty {
                assert!(
                    messages
                        .insert(
                            name.clone(),
                            MessageTags {
                                fields: BTreeMap::new(),
                                reserved: BTreeSet::new(),
                            },
                        )
                        .is_none(),
                    "message {name} is declared twice"
                );
                continue;
            }
            current = Some((
                name,
                MessageTags {
                    fields: BTreeMap::new(),
                    reserved: BTreeSet::new(),
                },
            ));
            continue;
        }
        let Some((name, tags)) = current.as_mut() else {
            continue;
        };
        if trimmed == "}" {
            let (name, tags) = current.take().expect("just borrowed it");
            assert!(
                messages.insert(name.clone(), tags).is_none(),
                "message {name} is declared twice"
            );
            continue;
        }
        if trimmed.starts_with("//") {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("reserved ") {
            for number in rest.trim_end_matches(';').split(',') {
                let number = number.trim();
                if let Ok(number) = number.parse::<u32>() {
                    assert!(
                        tags.reserved.insert(number),
                        "message {name} reserves {number} twice"
                    );
                }
            }
            continue;
        }
        // `<modifiers> <type> <field_name> = <number>;`, where `<type>` may contain a comma
        // (`map<string, OperatorCommitData>`) but the tail never does.
        let Some((declaration, number)) = trimmed.trim_end_matches(';').rsplit_once('=') else {
            continue;
        };
        let Ok(number) = number.trim().parse::<u32>() else {
            continue;
        };
        let field = declaration
            .trim()
            .rsplit(|c: char| c.is_whitespace())
            .next()
            .expect("a declaration has at least one token")
            .to_string();
        assert!(
            tags.fields.insert(field.clone(), number).is_none(),
            "message {name} declares field {field} twice"
        );
    }

    assert!(current.is_none(), "a message block was never closed");
    messages
}

fn tags_of(name: &str) -> MessageTags {
    parse_messages()
        .remove(name)
        .unwrap_or_else(|| panic!("rpc.proto has no message {name}"))
}

fn expect(name: &str, expected: &[(&str, u32)]) {
    let tags = tags_of(name);
    let expected: BTreeMap<String, u32> = expected
        .iter()
        .map(|(field, number)| ((*field).to_string(), *number))
        .collect();
    assert_eq!(
        tags.fields, expected,
        "the field-number allocation of {name} changed"
    );
}

/// No message may give one number to two fields.
///
/// This holds for every message in the file, not only the five the fence protocol touches: the
/// cheapest place to catch a duplicated number is the whole file, and a duplicate elsewhere
/// would be the same defect.
#[test]
fn no_message_gives_one_field_number_to_two_fields() {
    for (name, tags) in parse_messages() {
        let mut seen = BTreeSet::new();
        for (field, number) in &tags.fields {
            assert!(
                seen.insert(*number),
                "message {name} gives field number {number} to {field} and to another field"
            );
            assert!(
                !tags.reserved.contains(number),
                "message {name} declares {field} at reserved number {number}"
            );
        }
    }
}

/// `RegisterWorkerReq` keeps its two abandoned numbers out of circulation.
///
/// 3 held `string job_id` until #915 folded it into `worker_context`, and 7 held
/// `string job_hash`, gone since early 2024. A build old enough to still set either encodes a
/// length-delimited value there, which is exactly how a new `string` or `bytes` field at the
/// same number would arrive.
#[test]
fn register_worker_req_reserves_the_numbers_it_abandoned() {
    let tags = tags_of("RegisterWorkerReq");
    assert_eq!(
        tags.reserved,
        BTreeSet::from([3, 7]),
        "RegisterWorkerReq must reserve exactly the numbers it once used and gave up"
    );
    expect(
        "RegisterWorkerReq",
        &[
            ("worker_context", 1),
            ("time", 2),
            ("rpc_address", 4),
            ("data_address", 5),
            ("resources", 6),
            ("slots", 8),
            ("reconciles_start_execution", 9),
        ],
    );
}

/// The registration response's strict-mode field takes number 1 of a message that has never had
/// a field, so no build has ever encoded anything at that number.
#[test]
fn register_worker_resp_allocates_the_strict_mode_flag_at_one() {
    expect("RegisterWorkerResp", &[("requires_lifecycle_fence", 1)]);
    assert!(
        tags_of("RegisterWorkerResp").reserved.is_empty(),
        "RegisterWorkerResp has nothing to reserve"
    );
}

/// The start request's fence fields take 13-17, above every number it has ever used.
///
/// 1-12 are all live. Two of them changed type in the past — `restore_epoch` from `uint32` to
/// `uint64` at 2, `start_epoch`/`min_epoch` likewise at 6 and 7 — which is wire-compatible
/// because both are varints, and neither number ever meant a different field.
#[test]
fn start_execution_req_allocates_the_fence_fields_above_every_number_it_has_used() {
    expect(
        "StartExecutionReq",
        &[
            ("program", 1),
            ("restore_epoch", 2),
            ("tasks", 3),
            ("job_controller_addr", 4),
            ("is_leader", 5),
            ("start_epoch", 6),
            ("min_epoch", 7),
            ("wait_for_leader", 8),
            ("checkpoint_interval_micros", 9),
            ("checkpoint_manifest_ref", 10),
            ("state_backend", 11),
            ("start_execution_id", 12),
            ("lifecycle_fence", 13),
            ("target_worker_id", 14),
            ("target_worker_generation", 15),
            ("lifecycle_operation", 16),
            ("revoked_execution_ids", 17),
        ],
    );
}

/// The start response's two fields take 1 and 2 of a message that has never had a field.
#[test]
fn start_execution_resp_allocates_the_settlement_fields_at_one_and_two() {
    expect(
        "StartExecutionResp",
        &[("observed_lifecycle_fence", 1), ("outcome", 2)],
    );
}

/// The commit request's fence fields take 3-5, above both numbers it has ever used.
#[test]
fn commit_req_allocates_the_fence_fields_above_every_number_it_has_used() {
    expect(
        "CommitReq",
        &[
            ("epoch", 1),
            ("committing_data", 2),
            ("lifecycle_fence", 3),
            ("target_worker_id", 4),
            ("target_worker_generation", 5),
        ],
    );
}

/// `CommitResp` gains nothing.
///
/// M11.P54a's enumeration ends at "commit fence", and M11.D39e(v) settles *issued start
/// attempts*, whose definitive response is a `StartExecutionResp`. A commit is not an issued
/// attempt with an identifier, so there is no settlement for its response to carry.
#[test]
fn commit_resp_stays_empty() {
    expect("CommitResp", &[]);
}

/// Every number the fence protocol allocates is one no build has ever encoded a field at.
///
/// The historical sets come from reading `crates/arroyo-rpc/proto/rpc.proto` at every commit
/// that has ever touched it: `RegisterWorkerReq` has used 1-9, `StartExecutionReq` 1-12,
/// `CommitReq` 1-2, and `RegisterWorkerResp`/`StartExecutionResp`/`CommitResp` have never had a
/// field at all.
#[test]
fn no_fence_field_reuses_a_number_any_build_has_encoded() {
    let ever_used: [(&str, &[u32]); 6] = [
        ("RegisterWorkerReq", &[1, 2, 3, 4, 5, 6, 7, 8, 9]),
        ("RegisterWorkerResp", &[]),
        (
            "StartExecutionReq",
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        ),
        ("StartExecutionResp", &[]),
        ("CommitReq", &[1, 2]),
        ("CommitResp", &[]),
    ];
    let added: [(&str, &[&str]); 6] = [
        ("RegisterWorkerReq", &[]),
        ("RegisterWorkerResp", &["requires_lifecycle_fence"]),
        (
            "StartExecutionReq",
            &[
                "lifecycle_fence",
                "target_worker_id",
                "target_worker_generation",
                "lifecycle_operation",
                "revoked_execution_ids",
            ],
        ),
        (
            "StartExecutionResp",
            &["observed_lifecycle_fence", "outcome"],
        ),
        (
            "CommitReq",
            &[
                "lifecycle_fence",
                "target_worker_id",
                "target_worker_generation",
            ],
        ),
        ("CommitResp", &[]),
    ];

    let messages = parse_messages();
    for ((message, historical), (same_message, new_fields)) in ever_used.iter().zip(added.iter()) {
        assert_eq!(message, same_message, "the two tables must stay aligned");
        let tags = messages
            .get(*message)
            .unwrap_or_else(|| panic!("rpc.proto has no message {message}"));
        let historical: BTreeSet<u32> = historical.iter().copied().collect();
        for field in *new_fields {
            let number = tags
                .fields
                .get(*field)
                .unwrap_or_else(|| panic!("{message} has no field {field}"));
            assert!(
                !historical.contains(number),
                "{message}.{field} takes number {number}, which a previous build encoded a \
                 different field at"
            );
        }
    }
}

/// The fence protocol adds no RPC and no message type.
///
/// M11.P54a's whole compatibility argument is that the protocol rides on messages that already
/// exist, so a peer that has never heard of it still speaks the same service. A new `rpc` would
/// break that for a legacy server (unimplemented method), and a new `message` would mean a field
/// that a legacy peer must skip rather than default. Both are pinned here as inventories,
/// because "we did not add one" is not something a diff of one commit can keep true.
#[test]
fn the_fence_protocol_adds_no_rpc_and_no_message_type() {
    let methods: Vec<String> = {
        let mut service = None;
        let mut methods = Vec::new();
        for line in RPC_PROTO.lines() {
            let trimmed = line.trim();
            if let Some(rest) = trimmed.strip_prefix("service ") {
                service = Some(rest.trim_end_matches('{').trim().to_string());
            } else if let Some(rest) = trimmed.strip_prefix("rpc ") {
                let name = rest
                    .split('(')
                    .next()
                    .expect("an rpc names a method")
                    .trim();
                let service = service
                    .as_deref()
                    .expect("an rpc is declared inside a service");
                methods.push(format!("{service}.{name}"));
            }
        }
        methods.sort();
        methods
    };

    assert_eq!(
        methods,
        [
            "CompilerGrpc.BuildUdf",
            "CompilerGrpc.GetUdfPath",
            "ControllerGrpc.HeartbeatNode",
            "ControllerGrpc.RegisterNode",
            "ControllerGrpc.RegisterWorker",
            "ControllerGrpc.SendSinkData",
            "ControllerGrpc.SubscribeToOutput",
            "ControllerGrpc.TaskStarted",
            "ControllerGrpc.WorkerFinished",
            "ControllerGrpc.WorkerInitializationComplete",
            "JobControllerGrpc.Heartbeat",
            "JobControllerGrpc.JobMetrics",
            "JobControllerGrpc.NonfatalError",
            "JobControllerGrpc.TaskCheckpointCompleted",
            "JobControllerGrpc.TaskCheckpointEvent",
            "JobControllerGrpc.TaskFailed",
            "JobControllerGrpc.TaskFinished",
            "JobStatusGrpc.GetCheckpointDetails",
            "JobStatusGrpc.GetJobCheckpoints",
            "JobStatusGrpc.GetJobStatus",
            "JobStatusGrpc.StopJob",
            "NodeGrpc.GetWorkers",
            "NodeGrpc.StartWorker",
            "NodeGrpc.StopWorker",
            "WorkerGrpc.Checkpoint",
            "WorkerGrpc.Commit",
            "WorkerGrpc.GetMetrics",
            "WorkerGrpc.GetWorkerPhase",
            "WorkerGrpc.JobControllerInit",
            "WorkerGrpc.JobFinished",
            "WorkerGrpc.LoadCompactedData",
            "WorkerGrpc.StartExecution",
            "WorkerGrpc.StopExecution",
        ],
        "the fence protocol may not add, rename or remove an RPC"
    );

    assert_eq!(
        parse_messages().len(),
        104,
        "the fence protocol may not add a message type; it extends the ones that exist"
    );
}
