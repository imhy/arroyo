//! Candidate and authoritative-root tests (M11.T26g, design M11.D39c/M11.D39d).
//!
//! The boundary under test is "this candidate object becomes the job's authoritative metadata
//! root", and three identities have to agree before it exists: the candidate, the validated
//! metadata, and the row authority. Each row below varies **one** of them and leaves the others
//! alone, because a check that only fires when everything disagrees is a check that fires for
//! the wrong reason.

use std::sync::atomic::{AtomicU64, Ordering};

use arroyo_rpc::metadata_root::MetadataRoot;
use arroyo_rpc::state_backend::StateBackendSelector;
use arroyo_rpc::state_backend::validated::Validated;
use arroyo_state_protocol::ProtocolPaths;
use arroyo_storage::StorageProvider;
use arroyo_types::{JobId, PipelineId};

use super::fence::LifecycleAuthority;
use super::fence_tests::{JOB, adopt, cold_status, migrated_job, stored_state};
use super::root::candidate::RootIdentity;
use super::root::{GenerationRoot, RecoveryReference, RootCandidate, RootContext, RootRefusal};
use crate::AuthorityOutcome;

pub(super) const PIPELINE: &str = "pl_1";
pub(super) const GENERATION: u64 = 7;
pub(super) const EPOCH: &str = "0123456789abcdef0123456789abcdef";

/// The recovery reference a worker-leader attempt resolves: an object inside the job's own
/// namespace.
fn leader_recovery() -> RecoveryReference {
    RecoveryReference::LeaderObject(format!(
        "{PIPELINE}/{JOB}/generations/6/checkpoints/checkpoint-0000004/checkpoint-manifest.pb"
    ))
}

/// The metadata a well-formed worker-leader attempt would root.
fn metadata() -> GenerationRoot {
    GenerationRoot::describing(
        PIPELINE,
        JOB,
        GENERATION,
        StateBackendSelector::Parquet,
        Some(leader_recovery()),
    )
}

/// What the job itself says, for a worker-leader attempt.
fn context() -> RootContext<'static> {
    RootContext {
        job_id: JOB,
        pipeline_id: PIPELINE,
        generation: GENERATION,
        execution_selector: StateBackendSelector::Parquet,
        leader_mode: true,
    }
}

/// The token a well-formed attempt produces.
pub(super) fn validated_metadata() -> Validated<GenerationRoot> {
    Validated::validate(metadata(), context()).expect("the metadata describes the attempt")
}

/// The token a well-formed attempt for `generation` of this job produces.
///
/// Shared with [`super::fence_tests`], where the two M11.D96 rows about the duel need a real
/// M11.T25 `Validated<T>` rather than a stand-in: the point of those rows is that validation is
/// necessary and not sufficient.
pub(super) fn validated_for(generation: u64) -> Validated<GenerationRoot> {
    Validated::validate(
        GenerationRoot::describing(
            PIPELINE,
            JOB,
            generation,
            StateBackendSelector::Parquet,
            None,
        ),
        RootContext {
            generation,
            ..context()
        },
    )
    .expect("the metadata describes the attempt")
}

/// The candidate `status` would publish for its own generation, under its own authority.
pub(super) fn candidate_for(status: &crate::JobStatus) -> RootCandidate {
    RootCandidate::mint(status.authority(), &validated_for(status.generation))
        .expect("a status that has adopted names a candidate")
}

/// An authority for this job at a stated fence and epoch.
fn authority(fence: u64, epoch: &str) -> LifecycleAuthority {
    LifecycleAuthority::from_parts(JOB, fence, epoch)
}

/// The candidate a well-formed attempt mints.
fn candidate() -> RootCandidate {
    RootCandidate::mint(&authority(3, EPOCH), &validated_metadata()).expect("a nameable candidate")
}

// ---------------------------------------------------------------------------------------
// Row 1-6 of the matrix: the whole-object check, one identity at a time.
// ---------------------------------------------------------------------------------------

/// The positive control, first: everything agrees and the token exists.
#[test]
fn metadata_that_describes_the_attempt_produces_a_token() {
    let token = validated_metadata();
    assert_eq!(token.get().job_id(), JOB);
    assert_eq!(token.get().pipeline_id(), PIPELINE);
    assert_eq!(token.get().generation(), GENERATION);
    assert_eq!(
        token.get(),
        &metadata(),
        "the token carries the value that was checked, recovery reference included"
    );
}

/// Each identity, varied on its own, against a context that still agrees about the rest.
///
/// The metadata is the value a caller *states*; the context is what the job says. Provenance is
/// not what is being tested — the two are built independently here on purpose, because that is
/// the only way a comparison can be shown to run.
#[test]
fn one_disagreeing_identity_refuses_the_whole_object() {
    for (case, metadata, expected) in [
        (
            "another job",
            GenerationRoot::describing(
                PIPELINE,
                "job_xyz",
                GENERATION,
                StateBackendSelector::Parquet,
                Some(leader_recovery()),
            ),
            RootRefusal::JobMismatch {
                found: "job_xyz".to_string(),
                expected: JOB.to_string(),
            },
        ),
        (
            "another pipeline",
            GenerationRoot::describing(
                "pl_2",
                JOB,
                GENERATION,
                StateBackendSelector::Parquet,
                Some(leader_recovery()),
            ),
            RootRefusal::PipelineMismatch {
                found: "pl_2".to_string(),
                expected: PIPELINE.to_string(),
            },
        ),
        (
            "another generation of this job",
            GenerationRoot::describing(
                PIPELINE,
                JOB,
                GENERATION + 1,
                StateBackendSelector::Parquet,
                Some(leader_recovery()),
            ),
            RootRefusal::GenerationMismatch {
                found: GENERATION + 1,
                expected: GENERATION,
            },
        ),
        (
            "another state backend",
            GenerationRoot::describing(
                PIPELINE,
                JOB,
                GENERATION,
                StateBackendSelector::StateEngine,
                Some(leader_recovery()),
            ),
            RootRefusal::SelectorMismatch {
                found: "stateengine".to_string(),
                expected: StateBackendSelector::Parquet,
            },
        ),
        (
            "a recovery checkpoint in another job's namespace",
            GenerationRoot::describing(
                PIPELINE,
                JOB,
                GENERATION,
                StateBackendSelector::Parquet,
                Some(RecoveryReference::LeaderObject(
                    "pl_1/job_xyz/generations/6/checkpoints/checkpoint-0000004/x.pb".to_string(),
                )),
            ),
            RootRefusal::ForeignRecoveryCheckpoint {
                found: "pl_1/job_xyz/generations/6/checkpoints/checkpoint-0000004/x.pb".to_string(),
                namespace: format!("{PIPELINE}/{JOB}/"),
            },
        ),
        (
            "a recovery checkpoint named the way the other topology names one",
            GenerationRoot::describing(
                PIPELINE,
                JOB,
                GENERATION,
                StateBackendSelector::Parquet,
                Some(RecoveryReference::ControllerCheckpointRow("41".to_string())),
            ),
            RootRefusal::RecoveryKindMismatch {
                found: "a checkpoints row",
                expected: "an object-store reference",
            },
        ),
    ] {
        assert_eq!(
            Validated::validate(metadata, context()).err(),
            Some(expected),
            "{case}: the whole-object check must refuse it, naming which identity disagreed"
        );
    }
}

/// The other topology, which names its recovery checkpoint differently — so the same check runs
/// in both and refuses the *other* kind in each.
#[test]
fn the_controller_topology_names_its_recovery_checkpoint_its_own_way() {
    let controller_context = RootContext {
        leader_mode: false,
        ..context()
    };
    let row = GenerationRoot::describing(
        PIPELINE,
        JOB,
        GENERATION,
        StateBackendSelector::Parquet,
        Some(RecoveryReference::ControllerCheckpointRow("41".to_string())),
    );
    assert!(
        Validated::validate(row, controller_context).is_ok(),
        "a controller-mode attempt restores from a checkpoints row"
    );

    assert_eq!(
        Validated::validate(metadata(), controller_context).err(),
        Some(RootRefusal::RecoveryKindMismatch {
            found: "an object-store reference",
            expected: "a checkpoints row",
        }),
        "and the leader-mode reference is refused in controller mode, not silently accepted"
    );

    assert_eq!(
        Validated::validate(
            GenerationRoot::describing(
                PIPELINE,
                JOB,
                GENERATION,
                StateBackendSelector::Parquet,
                Some(RecoveryReference::ControllerCheckpointRow(String::new())),
            ),
            controller_context,
        )
        .err(),
        Some(RootRefusal::EmptyRecoveryCheckpoint),
        "an empty row id names nothing"
    );
}

/// A generation that restores from nothing still roots: `None` is a complete statement, not a
/// missing one.
#[test]
fn a_generation_with_nothing_to_restore_from_still_roots() {
    let fresh = GenerationRoot::describing(
        PIPELINE,
        JOB,
        GENERATION,
        StateBackendSelector::Parquet,
        None,
    );
    let token =
        Validated::validate(fresh.clone(), context()).expect("no recovery is a valid statement");
    assert_eq!(token.get(), &fresh);
    assert!(RootCandidate::mint(&authority(3, EPOCH), &token).is_ok());
}

// ---------------------------------------------------------------------------------------
// Rows 7-10: minting, where the authority meets the validated metadata.
// ---------------------------------------------------------------------------------------

/// An authority over another job cannot name this job's candidate, however the caller came by
/// the two together.
#[test]
fn an_authority_over_another_job_cannot_mint_this_jobs_candidate() {
    assert_eq!(
        RootCandidate::mint(
            &LifecycleAuthority::from_parts("job_xyz", 3, EPOCH),
            &validated_metadata()
        ),
        Err(RootRefusal::AuthorityJobMismatch {
            job: JOB.to_string(),
            authority: "job_xyz".to_string(),
        })
    );
}

/// A controller that has adopted nothing holds the column's default, and the default names no
/// candidate: the whole point of the fence being in the name is that the name says which
/// adoption wrote it.
#[test]
fn an_unadopted_authority_names_no_candidate() {
    let refusal = RootCandidate::mint(&LifecycleAuthority::unadopted(JOB), &validated_metadata())
        .expect_err("an unadopted authority cannot scope a candidate");
    assert!(
        matches!(refusal, RootRefusal::Unnameable(_)),
        "{refusal}: an unadopted fence must be reported as an unnameable candidate"
    );
    assert!(
        refusal.to_string().contains("lifecycle fence 0"),
        "{refusal}"
    );
}

/// The candidate's name embeds the identity, so two controllers duelling over one generation
/// write two different objects and neither can overwrite the other's.
#[test]
fn two_controllers_duelling_over_one_generation_name_two_objects() {
    let first = RootCandidate::mint(&authority(3, EPOCH), &validated_metadata()).unwrap();
    let second = RootCandidate::mint(
        &authority(4, "fedcba9876543210fedcba9876543210"),
        &validated_metadata(),
    )
    .unwrap();
    assert_ne!(first.key(), second.key());
    assert!(first.key().contains("fence-00000000000000000003"));
    assert!(second.key().contains("fence-00000000000000000004"));
    assert_eq!(
        first.body(),
        second.body(),
        "the metadata is the same; only the name the fence scopes it to differs"
    );
}

// ---------------------------------------------------------------------------------------
// Rows 11-14: installing, where the candidate meets the authority the status presents *now*.
// ---------------------------------------------------------------------------------------

/// Each install-time identity, varied on its own against a status that still agrees about the
/// rest.
///
/// This is the check `mint` cannot make: the status's authority is replaced by a re-adoption,
/// and a candidate minted under the previous one names a fence the row no longer carries.
#[tokio::test]
async fn one_disagreeing_identity_refuses_the_install() {
    let (database, connection) = migrated_job();
    let mut status = cold_status(&database).await;
    adopt(&mut status, &database).await;
    status.generation = GENERATION;
    let held = status.authority().clone();

    let good = RootCandidate::mint(
        &LifecycleAuthority::from_parts(JOB, held.fence().get(), held.epoch()),
        &validated_metadata(),
    )
    .expect("a candidate under the status's own authority");

    for (case, candidate, identity) in [
        (
            "a candidate for another job",
            RootCandidate::mint(
                &LifecycleAuthority::from_parts("job_xyz", held.fence().get(), held.epoch()),
                &Validated::validate(
                    GenerationRoot::describing(
                        PIPELINE,
                        "job_xyz",
                        GENERATION,
                        StateBackendSelector::Parquet,
                        None,
                    ),
                    RootContext {
                        job_id: "job_xyz",
                        ..context()
                    },
                )
                .unwrap(),
            )
            .unwrap(),
            RootIdentity::JobId,
        ),
        (
            "a candidate scoped to another fence",
            RootCandidate::mint(
                &LifecycleAuthority::from_parts(JOB, held.fence().get() + 1, held.epoch()),
                &validated_metadata(),
            )
            .unwrap(),
            RootIdentity::Fence,
        ),
        (
            "a candidate scoped to another epoch",
            RootCandidate::mint(
                &LifecycleAuthority::from_parts(JOB, held.fence().get(), EPOCH),
                &validated_metadata(),
            )
            .unwrap(),
            RootIdentity::Epoch,
        ),
        (
            "a candidate for another generation",
            RootCandidate::mint(
                &LifecycleAuthority::from_parts(JOB, held.fence().get(), held.epoch()),
                &Validated::validate(
                    GenerationRoot::describing(
                        PIPELINE,
                        JOB,
                        GENERATION + 1,
                        StateBackendSelector::Parquet,
                        None,
                    ),
                    RootContext {
                        generation: GENERATION + 1,
                        ..context()
                    },
                )
                .unwrap(),
            )
            .unwrap(),
            RootIdentity::Generation,
        ),
    ] {
        let refusal = status
            .install_metadata_root(&database, &candidate)
            .await
            .expect_err(&format!("{case}: the install must be refused"));
        assert_eq!(refusal.identity, identity, "{case}");
        assert_eq!(refusal.job_id, candidate.root().job_id(), "{case}");
        assert!(
            refusal
                .to_string()
                .contains(&format!("its {} is", identity.as_str())),
            "{case}: the message names the identity that disagreed, which is the only part of \
             the refusal an operator sees: {refusal}"
        );
        assert_eq!(
            status.state_context.metadata_root, None,
            "{case}: a refused install must leave the status carrying no root"
        );
    }

    // The control, through the same call.
    assert_eq!(
        status
            .install_metadata_root(&database, &good)
            .await
            .expect("the candidate agrees with the status"),
        Ok(AuthorityOutcome::Applied(()))
    );
    assert_eq!(
        status
            .state_context
            .metadata_root
            .as_ref()
            .map(MetadataRoot::object),
        Some(good.key())
    );
    assert_eq!(
        stored_state(&connection),
        "Running",
        "installing a root writes the same columns the status write does, and the status was \
         not otherwise changed"
    );
}

/// Each install-time identity names itself, and no two name the same thing.
///
/// The names are the whole of what an operator reads out of a refused install:
/// [`RootInstallRefusal`]'s message is *"its {identity} is X and the status presents Y"*, and
/// two identities sharing a name would make "the fence disagreed" and "the epoch disagreed"
/// indistinguishable in a log — two different situations with two different next steps.
#[test]
fn each_install_time_identity_names_itself() {
    let named: Vec<&str> = [
        RootIdentity::JobId,
        RootIdentity::Fence,
        RootIdentity::Epoch,
        RootIdentity::Generation,
    ]
    .into_iter()
    .map(RootIdentity::as_str)
    .collect();
    assert_eq!(
        named,
        vec![
            "job id",
            "lifecycle fence",
            "controller epoch",
            "scheduling generation",
        ]
    );
    let mut distinct = named.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        named.len(),
        "no two identities share a name"
    );
}

/// A candidate body this build does not understand is refused by its version, before any
/// identity is compared.
///
/// The body is written by one controller and read by another, which is why it carries a version
/// at all: a build that gained a field would write a body an older one must not act on. The
/// refusal is the *first* check in the whole-object comparison, so a body of an unknown version
/// is never partially believed — and it is reached by decoding, because that is how a body
/// written by another build arrives.
#[test]
fn a_candidate_body_of_an_unknown_version_is_refused_before_any_identity_is_compared() {
    // Everything else agrees with the context, so the version is the only thing refusing it.
    let from_a_newer_build: GenerationRoot = serde_json::from_value(serde_json::json!({
        "version": 2,
        "pipeline_id": PIPELINE,
        "job_id": JOB,
        "generation": GENERATION,
        "execution_selector": "parquet",
        "recovery_checkpoint": null,
    }))
    .expect("a body a newer build could have written still decodes");

    assert_eq!(
        Validated::validate(from_a_newer_build, context()).err(),
        Some(RootRefusal::UnknownVersion { found: 2 }),
        "an unknown body version is refused, and named"
    );

    // The control: the same body at this build's version, which the context agrees with.
    let from_this_build: GenerationRoot = serde_json::from_value(serde_json::json!({
        "version": 1,
        "pipeline_id": PIPELINE,
        "job_id": JOB,
        "generation": GENERATION,
        "execution_selector": "parquet",
        "recovery_checkpoint": null,
    }))
    .expect("this build's own body decodes");
    assert!(
        Validated::validate(from_this_build, context()).is_ok(),
        "so the refusal above is about the version and not about the rest of the body"
    );
}

/// Validate first, commit second: a refused install leaves the status's own record exactly as
/// it was, so nothing goes on to present a root the row refused.
#[tokio::test]
async fn a_refused_install_leaves_the_status_record_untouched() {
    let (database, connection) = migrated_job();
    let mut status = cold_status(&database).await;
    adopt(&mut status, &database).await;
    status.generation = GENERATION;
    let held = status.authority().clone();
    let candidate = RootCandidate::mint(
        &LifecycleAuthority::from_parts(JOB, held.fence().get(), held.epoch()),
        &validated_metadata(),
    )
    .unwrap();

    // A second controller takes the job between the mint and the install.
    let mut rival = cold_status(&database).await;
    adopt(&mut rival, &database).await;

    let outcome = status
        .install_metadata_root(&database, &candidate)
        .await
        .expect("the candidate still agrees with this status's own authority")
        .expect("losing the duel is an outcome, not a failure");
    assert!(
        matches!(outcome, AuthorityOutcome::Stale(_)),
        "the row now carries the rival's authority"
    );
    assert_eq!(
        status.state_context.metadata_root, None,
        "a status whose install was refused must not go on presenting the root it wanted"
    );
    let stored: serde_json::Value = connection
        .lock()
        .unwrap()
        .query_row(
            "SELECT state_context FROM job_statuses WHERE id = ?1",
            [JOB],
            |r| r.get(0),
        )
        .expect("the row must be readable");
    assert_eq!(
        stored.get("metadata_root"),
        None,
        "and the row carries no root at all: {stored}"
    );
}

// ---------------------------------------------------------------------------------------
// The object half: immutable, fence-scoped, and not something the landed cleanup deletes.
// ---------------------------------------------------------------------------------------

/// A directory holding one job's objects, removed with the test.
pub(super) struct CandidateStore(String);

impl CandidateStore {
    pub(super) fn new(name: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir()
            .join(format!(
                "arroyo-root-{name}-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                COUNTER.fetch_add(1, Ordering::Relaxed),
            ))
            .to_string_lossy()
            .into_owned();
        std::fs::create_dir_all(&directory).unwrap();
        Self(directory)
    }

    pub(super) async fn provider(&self) -> StorageProvider {
        StorageProvider::for_url(&format!("file://{}", self.0))
            .await
            .expect("a local store")
    }

    /// The directory this store's objects live in.
    pub(super) fn path(&self) -> &str {
        &self.0
    }

    /// Every object key under the store, relative to its root.
    pub(super) fn keys(&self) -> Vec<String> {
        fn walk(root: &std::path::Path, at: &std::path::Path, into: &mut Vec<String>) {
            for entry in std::fs::read_dir(at).into_iter().flatten().flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(root, &path, into);
                } else {
                    into.push(
                        path.strip_prefix(root)
                            .unwrap()
                            .to_string_lossy()
                            .into_owned(),
                    );
                }
            }
        }
        let mut keys = Vec::new();
        walk(
            std::path::Path::new(&self.0),
            std::path::Path::new(&self.0),
            &mut keys,
        );
        keys.sort();
        keys
    }
}

impl Drop for CandidateStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Publishing writes exactly one object, at the key the identity derives, carrying the metadata
/// that was validated — and publishing again writes nothing new.
#[tokio::test]
async fn publishing_a_candidate_writes_one_immutable_object() {
    let store = CandidateStore::new("immutable");
    let provider = store.provider().await;
    let candidate = candidate();

    candidate.publish(&provider).await.expect("a first publish");
    assert_eq!(store.keys(), vec![candidate.key()]);

    // The same identity, again: idempotent, and the bytes are the ones that were there.
    candidate
        .publish(&provider)
        .await
        .expect("a retry of the same attempt");
    assert_eq!(store.keys(), vec![candidate.key()]);
    assert_eq!(
        provider
            .get(candidate.key().as_str())
            .await
            .unwrap()
            .to_vec(),
        candidate.body(),
        "the object still carries the metadata that was validated"
    );

    // A rival at a higher fence: a second object, and the first is untouched.
    let rival = RootCandidate::mint(
        &authority(4, "fedcba9876543210fedcba9876543210"),
        &validated_metadata(),
    )
    .unwrap();
    rival.publish(&provider).await.expect("a rival's publish");
    let mut expected = vec![candidate.key(), rival.key()];
    expected.sort();
    assert_eq!(store.keys(), expected);
}

/// A store failure that is not "already there" is a failure, and names the key it was for.
///
/// The arm above it — [`StorageError::AlreadyExists`] — is the *only* one this attempt may read
/// as success, because the key embeds the whole identity and so the only thing that can already
/// be at it is this attempt's own bytes. Every other failure has to be reported: a preamble that
/// read one as success would install a root pointing at an object that was never written, and a
/// controller restarting into it would find the reference and not the metadata.
///
/// The failure is produced by taking the write permission off the store's directory after the
/// provider is built, so it is the *write* that fails rather than the provider's construction.
#[tokio::test]
async fn a_store_failure_that_is_not_an_existing_object_is_reported_with_its_key() {
    use std::os::unix::fs::PermissionsExt;

    let store = CandidateStore::new("unwritable");
    let provider = store.provider().await;
    let candidate = candidate();

    let readable_only = std::fs::Permissions::from_mode(0o500);
    std::fs::set_permissions(store.path(), readable_only).expect("the fixture directory is ours");
    let failure = candidate
        .publish(&provider)
        .await
        .expect_err("a store that will not take the object is a failure");
    // Restored before the assertions so that a failing row still leaves a removable directory.
    std::fs::set_permissions(store.path(), std::fs::Permissions::from_mode(0o700)).unwrap();

    assert_eq!(
        failure.key,
        candidate.key(),
        "the report names the object that was not written, which is what an operator looks for"
    );
    assert!(
        !failure.report.is_empty(),
        "and carries the store's own report rather than replacing it: {failure}"
    );
    assert_eq!(
        store.keys(),
        Vec::<String>::new(),
        "and nothing was written, so there is no candidate a root could point at"
    );

    // The control, through the same call once the directory is writable again.
    candidate
        .publish(&provider)
        .await
        .expect("the same publish succeeds once the store will take it");
    assert_eq!(store.keys(), vec![candidate.key()]);
}

/// A published candidate is never classified as a deletable table-data file by the landed
/// history cleanup, and it lives inside the job's own generations prefix so a collector that
/// reclaims a generation reclaims it.
#[test]
fn a_published_candidate_is_never_a_deletable_object() {
    let paths = ProtocolPaths::new(
        PipelineId(std::sync::Arc::new(PIPELINE.to_string())),
        JobId(std::sync::Arc::new(JOB.to_string())),
    );
    let key = candidate().key();
    assert!(
        key.starts_with(&paths.generations_prefix()),
        "{key} must live inside {}",
        paths.generations_prefix()
    );
    assert!(
        !paths.contains_deletable_object(&key),
        "{key} must not be classified as table data the landed cleanup deletes"
    );
}
