//! Metadata-root record tests (M11.T26g, design M11.D39d).
//!
//! Every rule here is a rule about a value that arrives from `job_statuses.state_context`, so
//! each is exercised on **both** paths into existence — [`MetadataRoot::mint`] and a decode —
//! rather than on the constructor alone. A rule that only ran in the constructor would be a
//! rule a persisted record could break.
//!
//! Whether a candidate key is one the landed history cleanup would delete is asserted against
//! the real `ProtocolPaths` in the controller, which can see `arroyo-state-protocol`; see
//! `a_published_candidate_is_never_a_deletable_object`.

use super::*;

const PIPELINE: &str = "pl_1";
const JOB: &str = "job_abc";
const EPOCH: &str = "0123456789abcdef0123456789abcdef";

/// The record a well-formed candidate identity produces.
fn root() -> MetadataRoot {
    MetadataRoot::mint(PIPELINE, JOB, 7, 3, EPOCH).expect("a well-formed identity")
}

/// The same identity as a raw JSON object, so a decode can break one field at a time.
fn raw(
    version: u32,
    pipeline_id: &str,
    job_id: &str,
    generation: u64,
    fence: u64,
    epoch: &str,
) -> String {
    format!(
        "{{\"version\":{version},\"pipeline_id\":{pipeline_id:?},\"job_id\":{job_id:?},\
         \"generation\":{generation},\"fence\":{fence},\"epoch\":{epoch:?}}}"
    )
}

/// The key is the identity: every part of it appears, in one spelling, and the two ways to ask
/// for it agree.
#[test]
fn the_candidate_key_names_the_whole_identity() {
    let root = root();
    assert_eq!(
        root.object(),
        "pl_1/job_abc/generations/7/candidates/\
         fence-00000000000000000003-epoch-0123456789abcdef0123456789abcdef.json"
    );
    assert!(root.roots(&root.object()));
    assert!(!root.roots("pl_1/job_abc/generations/7/candidates/something-else.json"));
}

/// Immutability, stated as the property that makes it true: the key is a function of the
/// identity, so two different identities never collide and one identity always produces the
/// same name.
#[test]
fn a_candidate_key_is_a_function_of_its_identity() {
    let base = root();
    assert_eq!(base.object(), root().object(), "the same identity, twice");

    let mut seen = std::collections::BTreeSet::new();
    seen.insert(base.object());
    for other in [
        MetadataRoot::mint("pl_2", JOB, 7, 3, EPOCH).unwrap(),
        MetadataRoot::mint(PIPELINE, "job_xyz", 7, 3, EPOCH).unwrap(),
        MetadataRoot::mint(PIPELINE, JOB, 8, 3, EPOCH).unwrap(),
        MetadataRoot::mint(PIPELINE, JOB, 7, 4, EPOCH).unwrap(),
        MetadataRoot::mint(PIPELINE, JOB, 7, 3, "ffffffffffffffffffffffffffffffff").unwrap(),
    ] {
        assert!(
            seen.insert(other.object()),
            "varying one component of the identity must vary the key: {}",
            other.object()
        );
        assert!(
            !base.roots(&other.object()),
            "and a root must not claim another identity's candidate"
        );
    }
    assert_eq!(seen.len(), 6);
}

/// Fences sort in fence order within a generation, which is what a collector reading the
/// prefix needs. Zero-padding to twenty digits is what makes that true of `u64::MAX` too.
#[test]
fn candidate_keys_of_one_generation_sort_in_fence_order() {
    let keys: Vec<String> = [1u64, 2, 10, 1_000, u64::MAX]
        .into_iter()
        .map(|fence| {
            MetadataRoot::mint(PIPELINE, JOB, 7, fence, EPOCH)
                .unwrap()
                .object()
        })
        .collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted, "{keys:#?}");
}

/// Every rule, on both paths into existence. The constructor's refusal and the decode's are
/// the same rule, and the decode is the one that matters: a row can be hand-edited.
#[test]
fn every_rule_refuses_on_both_paths_into_existence() {
    let oversized_id = "j".repeat(MAX_ROOT_IDENTIFIER_CHARS + 1);
    let oversized_epoch = "a".repeat(MAX_CONTROLLER_EPOCH_CHARS + 1);
    for (case, pipeline, job, generation, fence, epoch, expected) in [
        (
            "an empty pipeline id",
            "",
            JOB,
            7u64,
            3u64,
            EPOCH,
            MetadataRootError::MalformedIdentifier {
                field: "pipeline id",
                found: String::new(),
            },
        ),
        (
            "a pipeline id that escapes the namespace",
            "../other",
            JOB,
            7,
            3,
            EPOCH,
            MetadataRootError::MalformedIdentifier {
                field: "pipeline id",
                found: "../other".to_string(),
            },
        ),
        (
            "a job id that is a traversal",
            PIPELINE,
            "..",
            7,
            3,
            EPOCH,
            MetadataRootError::MalformedIdentifier {
                field: "job id",
                found: "..".to_string(),
            },
        ),
        (
            "a job id carrying a separator",
            PIPELINE,
            "job/abc",
            7,
            3,
            EPOCH,
            MetadataRootError::MalformedIdentifier {
                field: "job id",
                found: "job/abc".to_string(),
            },
        ),
        (
            "an oversized job id",
            PIPELINE,
            oversized_id.as_str(),
            7,
            3,
            EPOCH,
            MetadataRootError::MalformedIdentifier {
                field: "job id",
                found: oversized_id.clone(),
            },
        ),
        (
            "generation zero",
            PIPELINE,
            JOB,
            0,
            3,
            EPOCH,
            MetadataRootError::UnlaunchedGeneration,
        ),
        (
            "fence zero",
            PIPELINE,
            JOB,
            7,
            0,
            EPOCH,
            MetadataRootError::UnadoptedFence,
        ),
        (
            "an empty epoch",
            PIPELINE,
            JOB,
            7,
            3,
            "",
            MetadataRootError::MalformedEpoch {
                found: String::new(),
            },
        ),
        (
            "an epoch that is not hexadecimal",
            PIPELINE,
            JOB,
            7,
            3,
            "../../etc/passwd",
            MetadataRootError::MalformedEpoch {
                found: "../../etc/passwd".to_string(),
            },
        ),
        (
            "an oversized epoch",
            PIPELINE,
            JOB,
            7,
            3,
            oversized_epoch.as_str(),
            MetadataRootError::MalformedEpoch {
                found: oversized_epoch.clone(),
            },
        ),
    ] {
        assert_eq!(
            MetadataRoot::mint(pipeline, job, generation, fence, epoch),
            Err(expected.clone()),
            "{case}: the constructor must refuse it"
        );
        let encoded = raw(
            METADATA_ROOT_VERSION,
            pipeline,
            job,
            generation,
            fence,
            epoch,
        );
        let error = serde_json::from_str::<MetadataRoot>(&encoded)
            .expect_err(&format!("{case}: the decode must refuse it too"));
        assert!(
            error.to_string().contains(&expected.to_string()),
            "{case}: the decode must refuse it for the same reason, not another: {error}"
        );
    }
}

/// A version this build does not know is refused rather than partially interpreted: the
/// candidate layout it names is one this build cannot derive.
#[test]
fn a_record_from_another_version_is_refused() {
    let encoded = raw(METADATA_ROOT_VERSION + 1, PIPELINE, JOB, 7, 3, EPOCH);
    let error = serde_json::from_str::<MetadataRoot>(&encoded).expect_err("an unknown version");
    assert!(
        error.to_string().contains(
            &MetadataRootError::UnknownVersion {
                found: METADATA_ROOT_VERSION + 1,
            }
            .to_string()
        ),
        "{error}"
    );
}

/// A well-formed record round-trips, and the decoded value derives the same key the minted one
/// did — which is the whole of "the record cannot name an object other than its identity's".
#[test]
fn a_well_formed_record_round_trips_to_the_same_key() {
    let minted = root();
    let encoded = serde_json::to_string(&minted).expect("a record serializes");
    let decoded: MetadataRoot = serde_json::from_str(&encoded).expect("and decodes");
    assert_eq!(decoded, minted);
    assert_eq!(decoded.object(), minted.object());
    assert_eq!(decoded.version(), METADATA_ROOT_VERSION);
    assert_eq!(decoded.pipeline_id(), PIPELINE);
    assert_eq!(decoded.job_id(), JOB);
    assert_eq!(decoded.generation(), 7);
    assert_eq!(decoded.fence(), 3);
    assert_eq!(decoded.epoch(), EPOCH);
}

/// The reference an ASCII identity produces fits [`MAX_CANDIDATE_ROOT_BYTES`], which is what
/// ties M11.T26b's durable bound to a key this build actually mints — including the widest ASCII
/// identity the identifier bound admits. The identifier bound counts *characters*, so this is a
/// statement about these identities and not about every one the earlier rules admit; the
/// identity that does not fit is
/// `an_identity_whose_key_would_not_fit_a_store_is_refused`.
#[test]
fn an_ascii_identity_at_each_bound_derives_a_key_inside_the_durable_one() {
    let long_pipeline = "p".repeat(MAX_ROOT_IDENTIFIER_CHARS);
    let long_job = "j".repeat(MAX_ROOT_IDENTIFIER_CHARS);
    for (pipeline, job, generation, fence) in [
        (PIPELINE, JOB, 1u64, 1u64),
        (PIPELINE, JOB, u64::MAX, u64::MAX),
        (
            long_pipeline.as_str(),
            long_job.as_str(),
            u64::MAX,
            u64::MAX,
        ),
    ] {
        let key = MetadataRoot::mint(pipeline, job, generation, fence, EPOCH)
            .expect("a legal identity")
            .object();
        assert!(
            !key.is_empty() && key.len() <= MAX_CANDIDATE_ROOT_BYTES,
            "{} bytes is outside 1..={MAX_CANDIDATE_ROOT_BYTES}",
            key.len()
        );
    }
}

/// The derived-length rule is the last one, and the identifier bounds do not subsume it.
///
/// The earlier rules count **characters** and this one counts **bytes**, so the two do not
/// imply each other: the widest ASCII identity the earlier rules admit is well inside the
/// bound, and a same-width identity written in four-byte characters is outside it. Both halves
/// are asserted here because either on its own would be misleading — the first would read as
/// "this rule cannot fire" and the second as "this rule is what bounds the key".
#[test]
fn an_identity_whose_key_would_not_fit_a_store_is_refused() {
    let ascii = candidate_object_key(
        &"p".repeat(MAX_ROOT_IDENTIFIER_CHARS),
        &"j".repeat(MAX_ROOT_IDENTIFIER_CHARS),
        u64::MAX,
        u64::MAX,
        EPOCH,
    );
    assert_eq!(
        ascii.len(),
        372,
        "the widest ASCII identity the earlier rules admit derives a key of this many bytes"
    );
    assert_eq!(
        MetadataRoot::validate(
            METADATA_ROOT_VERSION,
            PIPELINE,
            JOB,
            u64::MAX,
            u64::MAX,
            EPOCH
        ),
        Ok(()),
        "so the rule passes for every ASCII identity the earlier rules admit"
    );

    // The same widths in four-byte characters. `MAX_ROOT_IDENTIFIER_CHARS` counts characters,
    // which is what makes this identity legal by every earlier rule and still name a key no
    // object store may be asked to hold: 128 + 128 characters is 1,024 bytes of identifier
    // before the fixed 116 bytes of the key's own shape are added.
    let wide = "\u{1d11e}".repeat(MAX_ROOT_IDENTIFIER_CHARS);
    assert_eq!(
        (wide.chars().count(), wide.len()),
        (MAX_ROOT_IDENTIFIER_CHARS, 4 * MAX_ROOT_IDENTIFIER_CHARS),
        "the identifier is exactly as wide as the character rule allows, in four-byte characters"
    );
    assert_eq!(
        MetadataRoot::mint(&wide, &wide, u64::MAX, u64::MAX, EPOCH),
        Err(MetadataRootError::CandidateKeyTooLong { found: 1140 }),
        "minting refuses it rather than naming an object nobody can write"
    );
    assert_eq!(
        MetadataRoot::validate(
            METADATA_ROOT_VERSION,
            &wide,
            &wide,
            u64::MAX,
            u64::MAX,
            EPOCH
        ),
        Err(MetadataRootError::CandidateKeyTooLong { found: 1140 }),
        "and so does a decode of a record carrying it, which is the path a persisted one takes"
    );
    const {
        assert!(
            1140 > MAX_CANDIDATE_ROOT_BYTES,
            "the refused length is outside the bound, and the 372 above is inside it"
        )
    };
}
