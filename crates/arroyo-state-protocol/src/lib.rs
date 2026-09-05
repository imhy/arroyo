//! Pure types and decision logic for Arroyo's object-store checkpoint protocol.
//!
//! Object-store I/O is kept at the edge: storage code reads protocol objects,
//! passes the observed facts into pure decision functions, and then executes the
//! returned decision.

pub mod gc;
pub mod resolve;
pub mod state;
pub mod store;
pub mod types;
pub mod validated;
pub mod workflow;

use crate::types::{CheckpointRef, Epoch, Generation};
use arroyo_types::{JobId, PipelineId};

/// Builds canonical object-store paths for one pipeline/job checkpoint namespace.
///
/// All paths are relative to the configured checkpoint storage URI. Callers
/// should use this rather than formatting protocol paths by hand so that all
/// readers and writers agree on object names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolPaths {
    pipeline_id: PipelineId,
    job_id: JobId,
}

impl ProtocolPaths {
    /// Creates path helpers for a pipeline/job pair.
    pub fn new(pipeline_id: PipelineId, job_id: JobId) -> Self {
        Self {
            pipeline_id,
            job_id,
        }
    }

    /// Returns the pipeline id this path builder is scoped to.
    pub fn pipeline_id(&self) -> &PipelineId {
        &self.pipeline_id
    }

    /// Returns the job id this path builder is scoped to.
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// Path to the controller-written current generation fence.
    /// The **legacy** single mutable current-generation object.
    ///
    /// Nothing writes or reads it since PR #167 round 7 replaced it with one immutable marker
    /// per generation — see [`Self::current_generation_marker`] for why. It is kept named here
    /// so the path cannot be quietly reused for something else, and so the cleanup guard goes on
    /// refusing to delete it in a deployment that still has one. A build predating the markers
    /// finds no object at the new location and fails closed, which is the direction M11.D75's
    /// worker-first rollout requires.
    pub fn current_generation(&self) -> CheckpointRef {
        self.path("current-generation.json")
    }

    /// Path to a generation manifest.
    pub fn generation_manifest(&self, generation: Generation) -> CheckpointRef {
        self.path(format!("generations/{generation}/generation-manifest.json"))
    }

    /// Path to the marker that makes one generation the job's current one.
    ///
    /// One object per generation, written once with put-if-absent, rather than one mutable
    /// object rewritten by each generation in turn (PR #167 round 7, finding 2). Two controllers
    /// can be racing to make *different* generations current, and a mutable pointer has no
    /// portable way to serialize that: `object_store`'s conditional update is unimplemented for
    /// the local filesystem, which a `file://` checkpoint URL uses. An immutable marker needs
    /// only put-if-absent, which every backend implements, and readers take the **highest**
    /// generation that has one.
    ///
    /// That is sound because a marker is written only after its controller has won the job's
    /// authoritative metadata-root update (M11.D39d): a controller that lost writes no marker at
    /// all, so it can never be the maximum, and a controller that won wrote a root the row
    /// agrees with.
    pub fn current_generation_marker(&self, generation: Generation) -> CheckpointRef {
        self.path(format!("generations/{generation}/current-generation.json"))
    }

    /// Prefix containing all artifacts for one checkpoint.
    pub fn checkpoint_dir(&self, generation: Generation, epoch: Epoch) -> CheckpointRef {
        self.path(format!(
            "generations/{generation}/checkpoints/checkpoint-{epoch:07}"
        ))
    }

    /// Path to the immutable protobuf checkpoint manifest.
    pub fn checkpoint_manifest(&self, generation: Generation, epoch: Epoch) -> CheckpointRef {
        self.path(format!(
            "generations/{generation}/checkpoints/checkpoint-{epoch:07}/checkpoint-manifest.pb"
        ))
    }

    /// Path to the commit-completion marker for a checkpoint.
    pub fn committed_marker(&self, generation: Generation, epoch: Epoch) -> CheckpointRef {
        self.path(format!(
            "generations/{generation}/checkpoints/checkpoint-{epoch:07}/committed.json"
        ))
    }

    /// Path to the canonical epoch record for an epoch.
    pub fn epoch_record(&self, epoch: Epoch) -> CheckpointRef {
        self.path(format!("epochs/epoch-{epoch:07}.record"))
    }

    /// The prefix every checkpoint artifact of this pipeline/job lives under.
    ///
    /// One spelling of the namespace, so that a check on where an object *is* and the builders
    /// that decide where an object *goes* cannot come to disagree. Every builder above routes
    /// through [`Self::path`], and `generations/` is the segment all of them but
    /// [`Self::current_generation`] and [`Self::epoch_record`] share.
    pub fn generations_prefix(&self) -> String {
        format!("{}/{}/generations/", self.pipeline_id, self.job_id)
    }

    /// Whether `path` names a **table data file** of this job that a cleanup may delete.
    ///
    /// Namespace is not enough, and neither is depth — PR #160 review comment `5385867064`.
    /// A retained checkpoint's own `checkpoint-manifest.pb` is inside the namespace and has a
    /// grandparent inside it too, so a namespace-and-depth test called it a data file: an
    /// older manifest naming that path had the *live* control object deleted, and the retained
    /// checkpoint's table references cannot protect it, because a control object is not a
    /// table reference. So the layout is parsed, and only the shape a worker writes table data
    /// at is accepted:
    ///
    /// `{pipeline}/{job}/generations/{g}/checkpoints/checkpoint-{e:07}/operator-{o}/table-{…}`
    ///
    /// Every control object this crate writes fails that by construction rather than by being
    /// listed: `checkpoint-manifest.pb` and `committed.json` sit one level shallower, in the
    /// checkpoint directory itself; `generation-manifest.json` is shallower again; and
    /// `current-generation.json` and `epochs/epoch-{e}.record` are not under `generations/` at
    /// all. Excluding them by the shape data *has*, rather than by a list of the things it is
    /// not, is what stops a control object added later from being deletable the day it lands.
    ///
    /// The depth this enforces also subsumes the directory rule it replaces: a path of exactly
    /// this shape has its parent at `operator-{o}` and its grandparent at the checkpoint
    /// directory, both inside the namespace, so the parent and grandparent
    /// `delete_classified_history` derives can never escape it.
    ///
    /// One rule, two readers: the check that lets a history become a token, and the deletion
    /// that spends it.
    pub fn contains_deletable_object(&self, path: &str) -> bool {
        let prefix = self.generations_prefix();
        let Some(tail) = path.strip_prefix(prefix.as_str()) else {
            return false;
        };
        let segments: Vec<&str> = tail.split('/').collect();
        let [generation, "checkpoints", checkpoint, operator, table] = segments.as_slice() else {
            return false;
        };
        let named = |segment: &str, kind: &str| {
            segment
                .strip_prefix(kind)
                .is_some_and(|name| !name.is_empty())
        };

        generation.parse::<u64>().is_ok()
            && checkpoint
                .strip_prefix("checkpoint-")
                .is_some_and(|epoch| !epoch.is_empty() && epoch.bytes().all(|b| b.is_ascii_digit()))
            && named(operator, "operator-")
            && named(table, "table-")
    }

    fn path(&self, suffix: impl AsRef<str>) -> CheckpointRef {
        CheckpointRef::from_validated(format!(
            "{}/{}/{}",
            self.pipeline_id,
            self.job_id,
            suffix.as_ref()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gc::{CheckpointOwner, cleanup_leader_checkpoints, delete_classified_history};
    use crate::resolve::{
        EpochClaimOutcome, ParentCheckpointStatus, ResolveDecision, ResolveFailure,
        resolve_candidate,
    };
    use crate::state::{CheckpointState, derive_checkpoint_state};
    use crate::store::tests::MemoryProtocolStore;
    use crate::store::{
        CreateResult, ProtocolStore, StoreError, create_json_if_not_exist, put_json, put_protobuf,
        read_json, read_protobuf,
    };
    use crate::types::{
        CommittedMarker, CurrentGeneration, EpochRecord, GenerationManifest, ProtocolError,
    };
    use crate::validated::{CheckpointHistory, CollectingJob};
    use crate::workflow::{
        CheckpointPublication, ClaimEpochRecordRequest, CommitAuthorization, CommitPermit,
        CommittedMarkerOutcome, CurrentGenerationPolicy, CurrentGenerationPublication,
        GenerationInitialization, GenerationRecovery, GenerationResolution,
        InitializeGenerationRequest, PublishCheckpointRequest, claim_epoch_record, complete_commit,
        initialize_generation, mark_committed, prepare_commit, publish_checkpoint,
        resolve_generation_manifest,
    };
    use arroyo_rpc::grpc::rpc::{
        CheckpointManifest, ExpiringKeyedTimeTableCheckpointMetadata,
        GlobalKeyedTableTaskCheckpointMetadata, OperatorCheckpointMetadata, OperatorMetadata,
        ParquetTimeFile, TableCheckpointMetadata, TableConfig, TableEnum,
    };
    use arroyo_rpc::state_backend::validated::Validated;
    use arroyo_rpc::state_backend::{StateBackendError, StateBackendSelector};
    use arroyo_types::{JobId, PipelineId, from_micros};
    use async_trait::async_trait;
    use prost::Message;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::time::SystemTime;

    fn checkpoint_ref(path: &str) -> CheckpointRef {
        CheckpointRef::new(path).unwrap()
    }

    fn checkpoint(
        epoch: u64,
        parent_checkpoint_ref: Option<CheckpointRef>,
        needs_commit: bool,
    ) -> CheckpointManifest {
        checkpoint_for_generation(Generation(1), epoch, parent_checkpoint_ref, needs_commit)
    }

    fn checkpoint_for_generation(
        generation: Generation,
        epoch: u64,
        parent_checkpoint_ref: Option<CheckpointRef>,
        needs_commit: bool,
    ) -> CheckpointManifest {
        CheckpointManifest {
            pipeline_id: "P".to_string(),
            job_id: "J".to_string(),
            generation: generation.0,
            epoch,
            min_epoch: epoch,
            start_time: 0,
            finish_time: 0,
            needs_commit,
            operators: vec![],
            parent_checkpoint_ref: parent_checkpoint_ref
                .map(|checkpoint_ref| checkpoint_ref.to_string()),
        }
    }

    /// Puts `operators` into `checkpoint`, headed as the checkpoint's own writer heads them.
    ///
    /// `finish_checkpoint_leader` fills every entry's `OperatorMetadata` from the same job id
    /// and epoch it puts in the manifest, so a fixture whose entries disagree with the manifest
    /// they are in is not a checkpoint any writer produces. Before PR #160 review round 7 these
    /// fixtures left the entry headers at `job_id: "J", epoch: 0` under manifests at epochs 1,
    /// 2 and 3, which was incidental rather than deliberate — nothing in the suite asserted on
    /// it — and it is the only thing this changes. Every assertion in every row below is the one
    /// M11.T08 landed.
    fn describing(
        mut checkpoint: CheckpointManifest,
        operators: Vec<OperatorCheckpointMetadata>,
    ) -> CheckpointManifest {
        checkpoint.operators = operators
            .into_iter()
            .map(|mut operator| {
                if let Some(header) = operator.operator_metadata.as_mut() {
                    header.job_id.clone_from(&checkpoint.job_id);
                    header.epoch = checkpoint.epoch as u32;
                }
                operator
            })
            .collect();
        checkpoint
    }

    fn epoch_record(checkpoint_ref: CheckpointRef, checkpoint: &CheckpointManifest) -> EpochRecord {
        EpochRecord::for_checkpoint(
            PipelineId::new("P"),
            Generation(checkpoint.generation),
            checkpoint_ref,
            checkpoint,
            SystemTime::UNIX_EPOCH,
        )
        .unwrap()
    }

    fn commit_permit(
        checkpoint_ref: CheckpointRef,
        checkpoint: &CheckpointManifest,
    ) -> CommitPermit {
        CommitPermit::new(
            checkpoint_ref.clone(),
            checkpoint,
            epoch_record(checkpoint_ref, checkpoint),
        )
        .unwrap()
    }

    fn committed_marker(checkpoint_ref: CheckpointRef, epoch: u64) -> CommittedMarker {
        CommittedMarker::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Epoch(epoch),
            Generation(1),
            Generation(2),
            checkpoint_ref,
        )
    }

    fn generation_manifest(
        base_checkpoint_ref: Option<CheckpointRef>,
        latest_checkpoint_ref: Option<CheckpointRef>,
    ) -> GenerationManifest {
        generation_manifest_for_generation(
            Generation(1),
            base_checkpoint_ref,
            latest_checkpoint_ref,
        )
    }

    fn generation_manifest_for_generation(
        generation: Generation,
        base_checkpoint_ref: Option<CheckpointRef>,
        latest_checkpoint_ref: Option<CheckpointRef>,
    ) -> GenerationManifest {
        let mut manifest = GenerationManifest::new(
            PipelineId::new("P"),
            JobId::new("J"),
            generation,
            base_checkpoint_ref,
            0,
        );
        manifest.latest_checkpoint_ref = latest_checkpoint_ref;
        manifest
    }

    /// The record whichever marker makes current, which is what every reader now consults.
    async fn current_marker<S: ProtocolStore + ?Sized>(store: &S) -> CurrentGeneration {
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        crate::workflow::read_current_generation(store, &paths)
            .await
            .expect("the markers are readable")
            .expect("a generation has been made current")
    }

    async fn write_current_generation(
        store: &MemoryProtocolStore,
        paths: &ProtocolPaths,
        generation: Generation,
    ) {
        let current_generation = CurrentGeneration::new(
            PipelineId::new("P"),
            JobId::new("J"),
            generation,
            from_micros(0),
        );

        put_json(
            store,
            &paths.current_generation_marker(generation),
            &current_generation,
        )
        .await
        .unwrap();
    }

    async fn write_canonical_checkpoint(
        store: &MemoryProtocolStore,
        paths: &ProtocolPaths,
        checkpoint_ref: &CheckpointRef,
        checkpoint: &CheckpointManifest,
    ) {
        put_protobuf(store, checkpoint_ref, checkpoint)
            .await
            .unwrap();
        put_json(
            store,
            &paths.epoch_record(Epoch(checkpoint.epoch)),
            &epoch_record(checkpoint_ref.clone(), checkpoint),
        )
        .await
        .unwrap();
    }

    /// A manifest cannot aim the cleanup at anything but this job's own table data.
    ///
    /// Renamed from `..._refuses_data_files_outside_the_namespace_it_was_collected_for` when
    /// PR #160 review comment `5385867064` widened it: being inside the namespace turned out
    /// not to be the property, because a *control* object is inside it too.
    ///
    /// **PR #160 review comment `5384870087`.** `CheckpointRef` validates a path's *shape* —
    /// relative, no empty or `..` segments, under a length cap — and says nothing about its
    /// place. These strings come out of the manifests' contents, so a manifest otherwise
    /// entirely valid for `P/J` could name a path under `P/J2`, and `delete_classified_history`
    /// deletes each string plus the two directories above it.
    ///
    /// The second case is the one the directory deletions make worse than a prefix test would
    /// suggest: a file sitting directly under `generations/` is inside the namespace by any
    /// `starts_with`, and its grandparent is `P/J` itself, which is what `delete_directory`
    /// would then be handed.
    #[test]
    fn a_history_refuses_anything_that_is_not_this_jobs_table_data() {
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let collecting = CollectingJob {
            state_backend: StateBackendSelector::Parquet,
        };
        let with_data_file = |file: CheckpointRef| {
            let mut history = CheckpointHistory::new(paths.clone());
            history.classified(vec![], vec![file]);
            history
        };
        let refused = |file: CheckpointRef, why: &str| {
            let err = Validated::validate(with_data_file(file), collecting).unwrap_err();
            assert!(
                matches!(
                    err,
                    StoreError::Protocol(ProtocolError::InvalidCheckpointRef { .. })
                ),
                "{why}: {err:?}"
            );
        };

        let other_job = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J2"));
        refused(
            data_ref(&other_job, 1),
            "a canonical path under a sibling job earned a cleanup token",
        );

        let other_pipeline = ProtocolPaths::new(PipelineId::new("P2"), JobId::new("J"));
        refused(
            data_ref(&other_pipeline, 1),
            "the same job id under another pipeline is a different namespace",
        );

        refused(
            checkpoint_ref(&format!("{}stray.parquet", paths.generations_prefix())),
            "a file whose grandparent is the job root would have `delete_directory` run on it",
        );

        // PR #160 review comment `5385867064`: every control object this crate writes is
        // inside the namespace, and three of them are at the depth the previous rule accepted.
        // A retained checkpoint's manifest named by an *older* manifest was therefore deletable,
        // and no table reference protects a control object, because it is not a table
        // reference. Each is refused by the shape data has rather than by being listed.
        for (control, what) in [
            (
                paths.checkpoint_manifest(Generation(1), Epoch(2)),
                "a retained checkpoint's own manifest",
            ),
            (
                paths.committed_marker(Generation(1), Epoch(2)),
                "a retained checkpoint's commit marker",
            ),
            (
                paths.generation_manifest(Generation(1)),
                "the generation manifest",
            ),
            (paths.epoch_record(Epoch(2)), "an epoch record"),
            (
                paths.current_generation(),
                "the legacy current-generation fence",
            ),
            (
                paths.current_generation_marker(Generation(1)),
                "a generation's current-generation marker",
            ),
        ] {
            refused(control, what);
        }

        // The controls: the layout a worker actually writes is accepted, at the retained epoch
        // and at an older one, so this rule refuses an object class rather than refusing data.
        for epoch in [1, 7] {
            Validated::validate(with_data_file(data_ref(&paths, epoch)), collecting)
                .expect("this job's own table data is what a cleanup exists to delete");
        }
    }

    fn data_ref(paths: &ProtocolPaths, epoch: u64) -> CheckpointRef {
        checkpoint_ref(&format!(
            "{}/operator-op/table-table-000",
            paths.checkpoint_dir(Generation(1), Epoch(epoch))
        ))
    }

    fn global_operator(files: Vec<CheckpointRef>) -> OperatorCheckpointMetadata {
        OperatorCheckpointMetadata {
            operator_metadata: Some(OperatorMetadata {
                job_id: "J".to_string(),
                operator_id: "op".to_string(),
                epoch: 0,
                min_watermark: None,
                max_watermark: None,
                parallelism: 1,
            }),
            start_time: 0,
            finish_time: 0,
            table_checkpoint_metadata: [(
                "table".to_string(),
                TableCheckpointMetadata {
                    table_type: TableEnum::GlobalKeyValue.into(),
                    data: GlobalKeyedTableTaskCheckpointMetadata {
                        files: files.into_iter().map(|file| file.to_string()).collect(),
                        commit_data_by_subtask: Default::default(),
                    }
                    .encode_to_vec(),
                },
            )]
            .into(),
            table_configs: Default::default(),
        }
    }

    /// A manifest entry headed for `operator_id`, as the checkpoint writer heads them.
    fn named_operator(operator_id: &str) -> OperatorCheckpointMetadata {
        let mut operator = global_operator(vec![]);
        operator.operator_metadata.as_mut().unwrap().operator_id = operator_id.to_string();
        operator
    }

    fn expiring_operator(
        operator_id: &str,
        files: Vec<CheckpointRef>,
    ) -> OperatorCheckpointMetadata {
        OperatorCheckpointMetadata {
            operator_metadata: Some(OperatorMetadata {
                job_id: "J".to_string(),
                operator_id: operator_id.to_string(),
                epoch: 0,
                min_watermark: None,
                max_watermark: None,
                parallelism: 1,
            }),
            start_time: 0,
            finish_time: 0,
            table_checkpoint_metadata: [(
                "expiring-table".to_string(),
                TableCheckpointMetadata {
                    table_type: TableEnum::ExpiringKeyedTimeTable.into(),
                    data: ExpiringKeyedTimeTableCheckpointMetadata {
                        files: files
                            .into_iter()
                            .map(|file| ParquetTimeFile {
                                file: file.to_string(),
                                ..Default::default()
                            })
                            .collect(),
                    }
                    .encode_to_vec(),
                },
            )]
            .into(),
            table_configs: Default::default(),
        }
    }

    async fn write_gc_checkpoint(
        store: &MemoryProtocolStore,
        paths: &ProtocolPaths,
        epoch: u64,
        parent_epoch: Option<u64>,
        operators: Vec<OperatorCheckpointMetadata>,
    ) {
        write_gc_checkpoint_in(
            store,
            paths,
            Generation(1),
            epoch,
            parent_epoch.map(|epoch| (Generation(1), epoch)),
            operators,
        )
        .await;
    }

    /// The same, for a history that spans generations: `generation` is the one this checkpoint
    /// was written by, and `parent` names the generation and epoch of its parent link.
    async fn write_gc_checkpoint_in(
        store: &MemoryProtocolStore,
        paths: &ProtocolPaths,
        generation: Generation,
        epoch: u64,
        parent: Option<(Generation, u64)>,
        operators: Vec<OperatorCheckpointMetadata>,
    ) {
        let checkpoint_ref = paths.checkpoint_manifest(generation, Epoch(epoch));
        let parent_checkpoint_ref =
            parent.map(|(generation, epoch)| paths.checkpoint_manifest(generation, Epoch(epoch)));
        let checkpoint = describing(
            checkpoint_for_generation(generation, epoch, parent_checkpoint_ref, false),
            operators,
        );
        write_canonical_checkpoint(store, paths, &checkpoint_ref, &checkpoint).await;
        put_json(
            store,
            &paths.committed_marker(generation, Epoch(epoch)),
            &committed_marker(checkpoint_ref, epoch),
        )
        .await
        .unwrap();
    }

    /// Replaces the manifest object at `at` with `claimed`, leaving every other object of the
    /// history exactly where its writer put it.
    ///
    /// This is what a misplaced or corrupt manifest looks like from the traversal's side: the
    /// bytes are readable, the selector agrees, and the object is simply not the checkpoint the
    /// reference it was read from names.
    async fn misplace_manifest(
        store: &MemoryProtocolStore,
        paths: &ProtocolPaths,
        at: (Generation, u64),
        claimed: CheckpointManifest,
    ) {
        put_protobuf(
            store,
            &paths.checkpoint_manifest(at.0, Epoch(at.1)),
            &claimed,
        )
        .await
        .unwrap();
    }

    async fn exists(store: &MemoryProtocolStore, path: &CheckpointRef) -> bool {
        store.read_bytes(path).await.unwrap().is_some()
    }

    #[tokio::test]
    async fn cleanup_deletes_only_checkpoints_below_new_min_epoch() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let file1 = data_ref(&paths, 1);
        let file2 = data_ref(&paths, 2);
        let file3 = data_ref(&paths, 3);
        let checkpoint1_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint2_ref = paths.checkpoint_manifest(Generation(1), Epoch(2));
        let checkpoint3_ref = paths.checkpoint_manifest(Generation(1), Epoch(3));

        store.put_bytes(&file1, b"1".to_vec()).await.unwrap();
        store.put_bytes(&file2, b"2".to_vec()).await.unwrap();
        store.put_bytes(&file3, b"3".to_vec()).await.unwrap();
        write_gc_checkpoint(
            &store,
            &paths,
            1,
            None,
            vec![global_operator(vec![file1.clone()])],
        )
        .await;
        write_gc_checkpoint(
            &store,
            &paths,
            2,
            Some(1),
            vec![global_operator(vec![file2.clone()])],
        )
        .await;
        write_gc_checkpoint(
            &store,
            &paths,
            3,
            Some(2),
            vec![global_operator(vec![file3.clone()])],
        )
        .await;

        cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            checkpoint3_ref.clone(),
            Epoch(2),
        )
        .await
        .unwrap();

        assert!(!exists(&store, &file1).await);
        assert!(!exists(&store, &paths.epoch_record(Epoch(1))).await);
        assert!(!exists(&store, &paths.committed_marker(Generation(1), Epoch(1))).await);
        assert!(
            read_protobuf::<_, CheckpointManifest>(&store, &checkpoint1_ref)
                .await
                .unwrap()
                .is_none()
        );

        assert!(exists(&store, &file2).await);
        assert!(exists(&store, &file3).await);
        assert!(
            read_protobuf::<_, CheckpointManifest>(&store, &checkpoint2_ref)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            read_protobuf::<_, CheckpointManifest>(&store, &checkpoint3_ref)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn cleanup_preserves_expiring_files_referenced_by_retained_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let shared_file = checkpoint_ref(&format!(
            "{}/operator-expiring-op/table-expiring-shared",
            paths.checkpoint_dir(Generation(1), Epoch(1))
        ));
        let expired_file = checkpoint_ref(&format!(
            "{}/operator-expiring-op/table-expiring-expired",
            paths.checkpoint_dir(Generation(1), Epoch(1))
        ));
        let checkpoint2_ref = paths.checkpoint_manifest(Generation(1), Epoch(2));

        store
            .put_bytes(&shared_file, b"shared".to_vec())
            .await
            .unwrap();
        store
            .put_bytes(&expired_file, b"expired".to_vec())
            .await
            .unwrap();
        write_gc_checkpoint(
            &store,
            &paths,
            1,
            None,
            vec![expiring_operator(
                "expiring-op",
                vec![shared_file.clone(), expired_file.clone()],
            )],
        )
        .await;
        write_gc_checkpoint(
            &store,
            &paths,
            2,
            Some(1),
            vec![expiring_operator("expiring-op", vec![shared_file.clone()])],
        )
        .await;

        cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            checkpoint2_ref.clone(),
            Epoch(2),
        )
        .await
        .unwrap();

        assert!(exists(&store, &shared_file).await);
        assert!(!exists(&store, &expired_file).await);

        let deleted_count = store.deleted_objects().len();
        cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            checkpoint2_ref,
            Epoch(2),
        )
        .await
        .unwrap();
        assert_eq!(deleted_count, store.deleted_objects().len());
        assert!(exists(&store, &shared_file).await);
    }

    #[tokio::test]
    async fn cleanup_retries_checkpoint_directory_when_carried_file_expires() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint1_dir = paths.checkpoint_dir(Generation(1), Epoch(1));
        let carried_file = checkpoint_ref(&format!(
            "{checkpoint1_dir}/operator-expiring-op/table-expiring-carried"
        ));

        store
            .put_bytes(&carried_file, b"carried".to_vec())
            .await
            .unwrap();
        write_gc_checkpoint(
            &store,
            &paths,
            1,
            None,
            vec![expiring_operator("expiring-op", vec![carried_file.clone()])],
        )
        .await;
        write_gc_checkpoint(
            &store,
            &paths,
            2,
            Some(1),
            vec![expiring_operator("expiring-op", vec![carried_file.clone()])],
        )
        .await;

        cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            paths.checkpoint_manifest(Generation(1), Epoch(2)),
            Epoch(2),
        )
        .await
        .unwrap();

        assert!(exists(&store, &carried_file).await);
        assert_eq!(
            1,
            store
                .deleted_directories()
                .iter()
                .filter(|directory| *directory == checkpoint1_dir.as_str())
                .count()
        );

        write_gc_checkpoint(
            &store,
            &paths,
            3,
            Some(2),
            vec![expiring_operator("expiring-op", vec![])],
        )
        .await;
        cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            paths.checkpoint_manifest(Generation(1), Epoch(3)),
            Epoch(3),
        )
        .await
        .unwrap();

        assert!(!exists(&store, &carried_file).await);
        assert_eq!(
            2,
            store
                .deleted_directories()
                .iter()
                .filter(|directory| *directory == checkpoint1_dir.as_str())
                .count(),
            "the old checkpoint directory should be retried when its carried file expires"
        );
    }

    #[tokio::test]
    async fn cleanup_classifies_mixed_global_and_expiring_metadata() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let global_old = checkpoint_ref(&format!(
            "{}/operator-op/table-global-old",
            paths.checkpoint_dir(Generation(1), Epoch(1))
        ));
        let global_retained = data_ref(&paths, 2);
        let expiring_old = checkpoint_ref(&format!(
            "{}/operator-expiring-op/table-expiring-old",
            paths.checkpoint_dir(Generation(1), Epoch(1))
        ));
        let expiring_shared = checkpoint_ref(&format!(
            "{}/operator-expiring-op/table-expiring-shared",
            paths.checkpoint_dir(Generation(1), Epoch(1))
        ));

        for file in [
            &global_old,
            &global_retained,
            &expiring_old,
            &expiring_shared,
        ] {
            store.put_bytes(file, b"data".to_vec()).await.unwrap();
        }
        write_gc_checkpoint(
            &store,
            &paths,
            1,
            None,
            vec![
                global_operator(vec![global_old.clone()]),
                expiring_operator(
                    "expiring-op",
                    vec![expiring_old.clone(), expiring_shared.clone()],
                ),
            ],
        )
        .await;
        write_gc_checkpoint(
            &store,
            &paths,
            2,
            Some(1),
            vec![
                global_operator(vec![global_retained.clone()]),
                expiring_operator("expiring-op", vec![expiring_shared.clone()]),
            ],
        )
        .await;

        cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            paths.checkpoint_manifest(Generation(1), Epoch(2)),
            Epoch(2),
        )
        .await
        .unwrap();

        assert!(!exists(&store, &global_old).await);
        assert!(!exists(&store, &expiring_old).await);
        assert!(exists(&store, &global_retained).await);
        assert!(exists(&store, &expiring_shared).await);
    }

    #[tokio::test]
    async fn cleanup_malformed_expiring_metadata_is_fail_closed() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let old_file = data_ref(&paths, 2);
        let mut malformed_operator = expiring_operator("expiring-op", vec![]);
        malformed_operator
            .table_checkpoint_metadata
            .get_mut("expiring-table")
            .unwrap()
            .data = vec![0x0a];

        store.put_bytes(&old_file, b"old".to_vec()).await.unwrap();
        write_gc_checkpoint(&store, &paths, 1, None, vec![malformed_operator]).await;
        write_gc_checkpoint(
            &store,
            &paths,
            2,
            Some(1),
            vec![global_operator(vec![old_file.clone()])],
        )
        .await;
        write_gc_checkpoint(&store, &paths, 3, Some(2), vec![global_operator(vec![])]).await;

        let err = cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            paths.checkpoint_manifest(Generation(1), Epoch(3)),
            Epoch(3),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, StoreError::DecodeProtobuf { .. }));
        assert!(store.deleted_objects().is_empty());
        assert!(exists(&store, &old_file).await);
    }

    #[tokio::test]
    async fn cleanup_deletes_checkpoint_manifests_oldest_first() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_gc_checkpoint(&store, &paths, 1, None, vec![global_operator(vec![])]).await;
        write_gc_checkpoint(&store, &paths, 2, Some(1), vec![global_operator(vec![])]).await;
        write_gc_checkpoint(&store, &paths, 3, Some(2), vec![global_operator(vec![])]).await;

        cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            paths.checkpoint_manifest(Generation(1), Epoch(3)),
            Epoch(3),
        )
        .await
        .unwrap();

        let deleted = store.deleted_objects();
        let manifest1 = paths
            .checkpoint_manifest(Generation(1), Epoch(1))
            .to_string();
        let manifest2 = paths
            .checkpoint_manifest(Generation(1), Epoch(2))
            .to_string();
        let manifest3 = paths
            .checkpoint_manifest(Generation(1), Epoch(3))
            .to_string();
        let manifest1_pos = deleted
            .iter()
            .position(|path| path == &manifest1)
            .expect("epoch 1 manifest should be deleted");
        let manifest2_pos = deleted
            .iter()
            .position(|path| path == &manifest2)
            .expect("epoch 2 manifest should be deleted");

        assert!(manifest1_pos < manifest2_pos);
        assert!(!deleted.contains(&manifest3));
    }

    #[tokio::test]
    async fn cleanup_detects_checkpoint_parent_cycles() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_gc_checkpoint(&store, &paths, 1, Some(2), vec![global_operator(vec![])]).await;
        write_gc_checkpoint(&store, &paths, 2, Some(1), vec![global_operator(vec![])]).await;

        let err = cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            paths.checkpoint_manifest(Generation(1), Epoch(2)),
            Epoch(2),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            StoreError::Protocol(ProtocolError::CheckpointCycle {
                generation: Generation(1),
                epoch: Epoch(2)
            })
        ));
        assert!(store.deleted_objects().is_empty());
    }

    #[tokio::test]
    async fn cleanup_rejects_min_epoch_newer_than_head() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let head = paths.checkpoint_manifest(Generation(1), Epoch(2));
        write_gc_checkpoint(&store, &paths, 2, None, vec![global_operator(vec![])]).await;

        let err = cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            head,
            Epoch(3),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            StoreError::Protocol(ProtocolError::CheckpointGcMinEpochBeyondHead {
                head_epoch: Epoch(2),
                new_min_epoch: Epoch(3),
            })
        ));
        assert!(store.deleted_objects().is_empty());
    }

    /// Stamps `state_backend` onto every table config of `operator`, so a manifest can state
    /// which backend wrote it.
    fn operator_with_selector(
        mut operator: OperatorCheckpointMetadata,
        state_backend: &str,
    ) -> OperatorCheckpointMetadata {
        operator.table_configs = operator
            .table_checkpoint_metadata
            .keys()
            .map(|table| {
                (
                    table.clone(),
                    TableConfig {
                        table_type: TableEnum::GlobalKeyValue as i32,
                        config: vec![],
                        state_version: 0,
                        state_backend: state_backend.to_string(),
                    },
                )
            })
            .collect();
        operator
    }

    /// Leader GC on a job whose history predates the selector: every table config is empty,
    /// which means parquet, and a parquet job still garbage-collects its own state exactly as
    /// before. The guard must not strand legacy deployments' checkpoints.
    #[tokio::test]
    async fn cleanup_still_collects_a_legacy_all_parquet_history() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let old_file = data_ref(&paths, 1);
        let kept_file = data_ref(&paths, 2);

        store.put_bytes(&old_file, b"1".to_vec()).await.unwrap();
        store.put_bytes(&kept_file, b"2".to_vec()).await.unwrap();
        // Epoch 1 states parquet explicitly, epoch 2 says nothing at all: both are this
        // parquet job's own, and both must be collectable.
        write_gc_checkpoint(
            &store,
            &paths,
            1,
            None,
            vec![operator_with_selector(
                global_operator(vec![old_file.clone()]),
                "parquet",
            )],
        )
        .await;
        write_gc_checkpoint(
            &store,
            &paths,
            2,
            Some(1),
            vec![global_operator(vec![kept_file.clone()])],
        )
        .await;

        cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            paths.checkpoint_manifest(Generation(1), Epoch(2)),
            Epoch(2),
        )
        .await
        .unwrap();

        assert!(!exists(&store, &old_file).await);
        assert!(exists(&store, &kept_file).await);
    }

    /// The hole this round closes: leader GC used to traverse manifests and delete the files
    /// they name without ever asking who wrote them.
    ///
    /// The disagreeing manifest is the *oldest* one, reached last, and it is one of the ones
    /// being collected — so an implementation that validated per-checkpoint as it deleted, or
    /// did not validate at all, would already have deleted the newer expiring checkpoint's file
    /// by the time it got there. Nothing may be deleted.
    #[tokio::test]
    async fn cleanup_rejects_a_history_written_by_another_backend() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let foreign_file = data_ref(&paths, 1);
        let old_file = data_ref(&paths, 2);
        let kept_file = data_ref(&paths, 3);

        store.put_bytes(&foreign_file, b"1".to_vec()).await.unwrap();
        store.put_bytes(&old_file, b"2".to_vec()).await.unwrap();
        store.put_bytes(&kept_file, b"3".to_vec()).await.unwrap();
        write_gc_checkpoint(
            &store,
            &paths,
            1,
            None,
            vec![operator_with_selector(
                global_operator(vec![foreign_file.clone()]),
                "stateengine",
            )],
        )
        .await;
        write_gc_checkpoint(
            &store,
            &paths,
            2,
            Some(1),
            vec![global_operator(vec![old_file.clone()])],
        )
        .await;
        write_gc_checkpoint(
            &store,
            &paths,
            3,
            Some(2),
            vec![global_operator(vec![kept_file.clone()])],
        )
        .await;

        let err = cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            paths.checkpoint_manifest(Generation(1), Epoch(3)),
            Epoch(3),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                StoreError::StateBackend(StateBackendError::CheckpointMismatch { .. })
            ),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("stateengine"), "{message}");

        assert!(store.deleted_objects().is_empty());
        assert!(store.deleted_directories().is_empty());
        assert!(exists(&store, &foreign_file).await);
        assert!(exists(&store, &old_file).await);
        assert!(exists(&store, &kept_file).await);
    }

    /// A retained manifest — one *above* the retention boundary, whose files are only ever
    /// protected, never deleted — is validated too. Everything reachable is inspected, because
    /// the whole reachable chain is what names the files.
    #[tokio::test]
    async fn cleanup_rejects_a_retained_checkpoint_written_by_another_backend() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let old_file = data_ref(&paths, 1);

        store.put_bytes(&old_file, b"1".to_vec()).await.unwrap();
        write_gc_checkpoint(
            &store,
            &paths,
            1,
            None,
            vec![global_operator(vec![old_file.clone()])],
        )
        .await;
        write_gc_checkpoint(
            &store,
            &paths,
            2,
            Some(1),
            vec![operator_with_selector(
                global_operator(vec![]),
                "stateengine",
            )],
        )
        .await;

        let err = cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            paths.checkpoint_manifest(Generation(1), Epoch(2)),
            Epoch(2),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                StoreError::StateBackend(StateBackendError::CheckpointMismatch { .. })
            ),
            "{err:?}"
        );
        assert!(store.deleted_objects().is_empty());
        assert!(exists(&store, &old_file).await);
    }

    /// A persisted selector nobody recognizes is a hard failure at GC too, never a fallback to
    /// the job's own backend — which would delete files under a layout nothing here understands.
    #[tokio::test]
    async fn cleanup_rejects_an_unknown_persisted_selector() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let old_file = data_ref(&paths, 1);

        store.put_bytes(&old_file, b"1".to_vec()).await.unwrap();
        write_gc_checkpoint(
            &store,
            &paths,
            1,
            None,
            vec![operator_with_selector(
                global_operator(vec![old_file.clone()]),
                "rocksdb",
            )],
        )
        .await;
        write_gc_checkpoint(&store, &paths, 2, Some(1), vec![global_operator(vec![])]).await;

        let err = cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            paths.checkpoint_manifest(Generation(1), Epoch(2)),
            Epoch(2),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                StoreError::StateBackend(StateBackendError::UnknownValue { ref value, .. })
                    if value == "rocksdb"
            ),
            "{err:?}"
        );
        assert!(store.deleted_objects().is_empty());
        assert!(exists(&store, &old_file).await);
    }

    /// The wiring row for finding 1 on the collecting path: leader GC refuses a reachable
    /// manifest that is not the checkpoint the reference it was read from names, and deletes
    /// nothing (PR #160 review round 7).
    ///
    /// Reached through [`cleanup_leader_checkpoints`] rather than through the token's check on
    /// its own, because the claim is that the *production* traversal records the reference and
    /// that the deletion cannot be reached without the binding — not that a helper exists.
    /// Every object [`delete_classified_history`] removes is built from the generation and epoch
    /// the manifest claims for itself, so the misplaced link below is one whose deletion would
    /// have been aimed at `generations/7/` — a prefix nothing in this history occupies.
    ///
    /// The misplaced manifest is the *oldest* one and one of the ones being collected, so an
    /// implementation that bound identity per checkpoint as it deleted would already have
    /// removed the newer one's objects by the time it got there.
    #[tokio::test]
    async fn cleanup_refuses_a_manifest_that_is_not_the_checkpoint_its_reference_names() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let old_file = data_ref(&paths, 1);
        let kept_file = data_ref(&paths, 2);

        store.put_bytes(&old_file, b"1".to_vec()).await.unwrap();
        store.put_bytes(&kept_file, b"2".to_vec()).await.unwrap();
        write_gc_checkpoint(
            &store,
            &paths,
            1,
            None,
            vec![global_operator(vec![old_file.clone()])],
        )
        .await;
        write_gc_checkpoint(
            &store,
            &paths,
            2,
            Some(1),
            vec![global_operator(vec![kept_file.clone()])],
        )
        .await;

        // Read from generation 1 epoch 1's reference; claims to be generation 7's.
        misplace_manifest(
            &store,
            &paths,
            (Generation(1), 1),
            describing(
                checkpoint_for_generation(Generation(7), 1, None, false),
                vec![global_operator(vec![old_file.clone()])],
            ),
        )
        .await;

        let err = cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            paths.checkpoint_manifest(Generation(1), Epoch(2)),
            Epoch(2),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                StoreError::Protocol(ProtocolError::CheckpointManifestMisplaced { .. })
            ),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("generation 7"), "{message}");

        assert!(store.deleted_objects().is_empty());
        assert!(store.deleted_directories().is_empty());
        assert!(exists(&store, &old_file).await);
        assert!(exists(&store, &kept_file).await);
    }

    /// The wiring row for finding 2 on the collecting path: an entry headed for another
    /// checkpoint refuses the whole history, and nothing is deleted (PR #160 review round 7).
    ///
    /// The manifest is exactly where it says it is here; only the entry's header moves. Leader
    /// GC reads those headers to name the operator whose table metadata it is decoding, so an
    /// entry from another checkpoint describes files under a directory this checkpoint does not
    /// own — and doubt on a deleting path resolves to retain.
    #[tokio::test]
    async fn cleanup_refuses_a_manifest_entry_headed_for_another_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let old_file = data_ref(&paths, 1);
        let kept_file = data_ref(&paths, 2);

        store.put_bytes(&old_file, b"1".to_vec()).await.unwrap();
        store.put_bytes(&kept_file, b"2".to_vec()).await.unwrap();
        write_gc_checkpoint(
            &store,
            &paths,
            1,
            None,
            vec![global_operator(vec![old_file.clone()])],
        )
        .await;
        write_gc_checkpoint(
            &store,
            &paths,
            2,
            Some(1),
            vec![global_operator(vec![kept_file.clone()])],
        )
        .await;

        // Everything about this manifest is right except the epoch its one entry is headed
        // with, which is a checkpoint two generations of this job ago.
        let mut planted = checkpoint_for_generation(Generation(1), 1, None, false);
        let mut operator = global_operator(vec![old_file.clone()]);
        operator.operator_metadata.as_mut().unwrap().epoch = 5;
        planted.operators = vec![operator];
        misplace_manifest(&store, &paths, (Generation(1), 1), planted).await;

        let err = cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            paths.checkpoint_manifest(Generation(1), Epoch(2)),
            Epoch(2),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, StoreError::IncompleteManifest(ref m) if m.detail.contains("epoch 5")),
            "{err:?}"
        );

        assert!(store.deleted_objects().is_empty());
        assert!(store.deleted_directories().is_empty());
        assert!(exists(&store, &old_file).await);
        assert!(exists(&store, &kept_file).await);
    }

    /// The difference the collecting row must admit: a reachable history spans generations and
    /// a range of epochs, and still mints its token (PR #160 review round 7).
    ///
    /// The positive half of the identity binding, and the reason the rule is "each manifest is
    /// the checkpoint *its own* reference names" rather than "every manifest carries one
    /// identity". This chain is four checkpoints across two generations and four epochs, so no
    /// single identity describes it; a check that bound the chain to one would refuse every
    /// history a restarted job has.
    #[tokio::test]
    async fn cleanup_collects_a_history_spanning_generations_and_epochs() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));

        let file = |generation: u64, epoch: u64| {
            checkpoint_ref(&format!(
                "{}/operator-op/table-table-000",
                paths.checkpoint_dir(Generation(generation), Epoch(epoch))
            ))
        };
        let old = [file(1, 1), file(1, 2)];
        let kept = [file(2, 3), file(2, 4)];
        for object in old.iter().chain(kept.iter()) {
            store.put_bytes(object, b"x".to_vec()).await.unwrap();
        }

        for (generation, epoch, parent, object) in [
            (1u64, 1u64, None, &old[0]),
            (1, 2, Some((Generation(1), 1u64)), &old[1]),
            (2, 3, Some((Generation(1), 2)), &kept[0]),
            (2, 4, Some((Generation(2), 3)), &kept[1]),
        ] {
            write_gc_checkpoint_in(
                &store,
                &paths,
                Generation(generation),
                epoch,
                parent,
                vec![global_operator(vec![object.clone()])],
            )
            .await;
        }

        cleanup_leader_checkpoints(
            &store,
            &paths,
            StateBackendSelector::Parquet,
            paths.checkpoint_manifest(Generation(2), Epoch(4)),
            Epoch(3),
        )
        .await
        .expect("a chain whose every link is where it says it is collects, across generations");

        for object in &old {
            assert!(
                !exists(&store, object).await,
                "{object} should be collected"
            );
        }
        for object in &kept {
            assert!(exists(&store, object).await, "{object} should be retained");
        }
        assert!(
            !exists(&store, &paths.checkpoint_manifest(Generation(1), Epoch(2))).await,
            "the collected generation's manifests should be gone"
        );
        assert!(
            exists(&store, &paths.checkpoint_manifest(Generation(2), Epoch(3))).await,
            "the retained generation's manifests should still be there"
        );
    }

    /// D96 row 2 (round 1): leader GC's first delete is reachable only through a token for
    /// the *whole* reachable manifest set, so nothing the traversal named can go before
    /// every link of the chain has been accounted for.
    ///
    /// [`cleanup_leader_checkpoints`] is now classify-then-`delete_classified_history`, and
    /// the deletion takes nothing but the token — so the three cases below are the complete
    /// set of ways into it. The chain that agrees deletes; a chain one of whose links was
    /// written by another backend does not; and — the part that makes the second case a
    /// claim about the *set* rather than about whichever links happened to be recorded — a
    /// plan that would delete a checkpoint the traversal never reached does not either.
    #[tokio::test]
    async fn gc_requires_validated_manifest_set() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let old_file = data_ref(&paths, 1);
        let kept_file = data_ref(&paths, 2);

        store.put_bytes(&old_file, b"1".to_vec()).await.unwrap();
        store.put_bytes(&kept_file, b"2".to_vec()).await.unwrap();
        write_gc_checkpoint(
            &store,
            &paths,
            1,
            None,
            vec![global_operator(vec![old_file.clone()])],
        )
        .await;
        write_gc_checkpoint(
            &store,
            &paths,
            2,
            Some(1),
            vec![global_operator(vec![kept_file.clone()])],
        )
        .await;

        let owner = |epoch: u64| CheckpointOwner {
            generation: Generation(1),
            epoch: Epoch(epoch),
        };
        let collecting = CollectingJob {
            state_backend: StateBackendSelector::Parquet,
        };
        // A classified history as the traversal records one: the links it read, newest
        // first — each with the reference it was read from, which is what binds what a
        // manifest says about itself to where it actually was — and the plan it derived
        // from them.
        let history = |reached: Vec<(u64, OperatorCheckpointMetadata)>, deleting: Vec<u64>| {
            let mut history = CheckpointHistory::new(paths.clone());
            for (epoch, operator) in reached {
                let manifest = describing(
                    checkpoint_for_generation(Generation(1), epoch, None, false),
                    vec![operator],
                );
                history.reached(
                    paths.checkpoint_manifest(Generation(1), Epoch(epoch)),
                    &manifest,
                );
            }
            history.classified(
                deleting.into_iter().map(owner).collect(),
                vec![old_file.clone()],
            );
            history
        };

        let agreeing = global_operator(vec![]);
        let foreign = operator_with_selector(global_operator(vec![]), "stateengine");

        // A link this job did not write: no token, and the deletion has no other argument.
        let err = Validated::validate(
            history(vec![(2, agreeing.clone()), (1, foreign)], vec![1]),
            collecting,
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::StateBackend(StateBackendError::CheckpointMismatch { .. })
            ),
            "{err:?}"
        );

        // A plan naming a checkpoint the traversal never read: nothing validated the
        // manifest that named its files, so it cannot be collected either.
        let err = Validated::validate(history(vec![(2, agreeing.clone())], vec![1]), collecting)
            .unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::Protocol(ProtocolError::CheckpointGcUnreached {
                    generation: Generation(1),
                    epoch: Epoch(1),
                })
            ),
            "{err:?}"
        );

        assert!(
            store.deleted_objects().is_empty(),
            "a refused history still deleted something"
        );
        assert!(exists(&store, &old_file).await);

        // The whole chain agrees: this is the one shape that yields the token, and the
        // deletion it authorizes is the ordinary one.
        // A link that is not the checkpoint the reference it was read from names: no token
        // either, because every object the deletion removes is built from the generation and
        // epoch those bytes claim (PR #160 review round 7, finding 1).
        let mut misplaced = CheckpointHistory::new(paths.clone());
        misplaced.reached(
            paths.checkpoint_manifest(Generation(1), Epoch(2)),
            &describing(
                checkpoint_for_generation(Generation(1), 2, None, false),
                vec![agreeing.clone()],
            ),
        );
        misplaced.reached(
            paths.checkpoint_manifest(Generation(1), Epoch(1)),
            // Read from epoch 1's reference, claiming to be epoch 9.
            &describing(
                checkpoint_for_generation(Generation(1), 9, None, false),
                vec![agreeing.clone()],
            ),
        );
        misplaced.classified(vec![owner(1)], vec![old_file.clone()]);
        let err = Validated::validate(misplaced, collecting).unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::Protocol(ProtocolError::CheckpointManifestMisplaced { .. })
            ),
            "{err:?}"
        );

        let validated = Validated::validate(
            history(vec![(2, agreeing.clone()), (1, agreeing)], vec![1]),
            collecting,
        )
        .expect("a whole, agreeing history is exactly what a token is for");
        delete_classified_history(&store, &validated).await.unwrap();

        assert!(!exists(&store, &old_file).await);
        assert!(exists(&store, &kept_file).await);
    }

    /// A checked history collects from the namespace it was checked against, and from no
    /// other.
    ///
    /// The token says *which chain* was checked. Until PR #160's GC-namespace review finding
    /// the deletion took a `ProtocolPaths` of its own that said *where to delete*, and nothing
    /// related the two: four of the five object kinds it removes — the checkpoint manifest,
    /// the committed marker, the epoch record and the checkpoint directory — were addressed
    /// out of that argument rather than out of the manifests the token covers. Only the data
    /// files came from the checked bytes.
    ///
    /// So the arrangement below is the one a misdirected deletion cannot tell from its own: a
    /// second job under the same pipeline, holding those same four objects at the same
    /// generation and epoch the collected job's chain names. They are written as opaque bytes
    /// on purpose — nothing traverses them, and what is under test is which paths the delete
    /// builds, not what they contain.
    #[tokio::test]
    async fn cleanup_deletes_only_within_the_namespace_the_history_was_checked_against() {
        let store = MemoryProtocolStore::default();
        let collected = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let bystander = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J2"));

        let old_file = data_ref(&collected, 1);
        let kept_file = data_ref(&collected, 2);
        store.put_bytes(&old_file, b"1".to_vec()).await.unwrap();
        store.put_bytes(&kept_file, b"2".to_vec()).await.unwrap();
        write_gc_checkpoint(
            &store,
            &collected,
            1,
            None,
            vec![global_operator(vec![old_file.clone()])],
        )
        .await;
        write_gc_checkpoint(
            &store,
            &collected,
            2,
            Some(1),
            vec![global_operator(vec![kept_file.clone()])],
        )
        .await;

        let untouchable = vec![
            bystander.checkpoint_manifest(Generation(1), Epoch(1)),
            bystander.committed_marker(Generation(1), Epoch(1)),
            bystander.epoch_record(Epoch(1)),
            data_ref(&bystander, 1),
        ];
        for object in &untouchable {
            store
                .put_bytes(object, b"another job's".to_vec())
                .await
                .unwrap();
        }

        cleanup_leader_checkpoints(
            &store,
            &collected,
            StateBackendSelector::Parquet,
            collected.checkpoint_manifest(Generation(1), Epoch(2)),
            Epoch(2),
        )
        .await
        .unwrap();

        assert!(
            !exists(
                &store,
                &collected.checkpoint_manifest(Generation(1), Epoch(1))
            )
            .await
        );
        assert!(!exists(&store, &collected.committed_marker(Generation(1), Epoch(1))).await);
        assert!(!exists(&store, &collected.epoch_record(Epoch(1))).await);
        assert!(!exists(&store, &old_file).await);
        assert!(exists(&store, &kept_file).await);

        for object in &untouchable {
            assert!(
                exists(&store, object).await,
                "the deletion reached outside the namespace its token was checked in: {object}"
            );
        }
    }

    /// A reached manifest claiming a job other than the one the history was opened for is
    /// refused, and the same chain under a history opened for *that* job is not.
    ///
    /// `gc_requires_validated_manifest_set` varies the epoch a manifest claims and the backend
    /// that wrote it. This varies the third identity, and the one a misdirected delete would
    /// have been built from: the job. Both directions are pinned, so the row cannot be
    /// satisfied by a check that simply refuses `"J2"`.
    ///
    /// What this row does **not** prove is that the namespace came from the history rather
    /// than from a separately supplied argument — nothing can, because after PR #160's
    /// GC-namespace review finding there is no second namespace to supply. That property is
    /// carried by the types, and `no_protocol_effect_takes_a_namespace_beside_its_token`
    /// keeps it. Worth recording from the mutation that established this row: pointing
    /// `identify_checkpoint_manifest` at each manifest's own pipeline and job ids does *not*
    /// flip it, because the reference rebuild then refuses the same fixture. The two halves of
    /// that function are redundant on this input and only the id comparison isolates it; a
    /// weakening that drops the id comparison and keeps the rebuild is what fails this row.
    #[test]
    fn a_history_is_checked_against_the_namespace_it_was_opened_for() {
        let collecting = CollectingJob {
            state_backend: StateBackendSelector::Parquet,
        };
        let owner = CheckpointOwner {
            generation: Generation(1),
            epoch: Epoch(1),
        };
        let claiming = |job: &str| {
            describing(
                CheckpointManifest {
                    job_id: job.to_string(),
                    ..checkpoint_for_generation(Generation(1), 1, None, false)
                },
                vec![global_operator(vec![])],
            )
        };
        let history = |paths: &ProtocolPaths, job: &str| {
            let mut history = CheckpointHistory::new(paths.clone());
            history.reached(
                paths.checkpoint_manifest(Generation(1), Epoch(1)),
                &claiming(job),
            );
            history.classified(vec![owner], vec![]);
            history
        };

        let collected = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let other = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J2"));

        let err = Validated::validate(history(&collected, "J2"), collecting).unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::Protocol(ProtocolError::CheckpointManifestMisplaced { .. })
            ),
            "{err:?}"
        );

        Validated::validate(history(&other, "J2"), collecting)
            .expect("the same chain, under the history opened for the job that wrote it");
    }

    /// No effect in the protocol that acts on a validation token also receives a namespace or
    /// an object reference beside it.
    ///
    /// This is the test that would have found the finding first. The rows above prove that
    /// *these* effects address the objects their tokens name; this one asserts the shape, and
    /// the shape is what recurred — review round 8 took the path builder off the two
    /// generation-publishing writes and off the epoch claim, and leader GC, written earlier in
    /// another module, kept its own. Reading either module tells you nothing about the other.
    ///
    /// It is a *signature-level* pin, and the gap is stated rather than discovered later: an
    /// effect handed a namespace inside a request struct, or reaching for one out of ambient
    /// state, would pass it. `workflow/recovery.rs` is not read here because it performs no
    /// writes at all, which `the_recovery_resolution_module_reaches_no_persistent_write`
    /// pins separately. The matched set is compared for equality rather than containment, so
    /// an effect that is added or renamed fails this row instead of quietly leaving its scope.
    #[test]
    fn no_protocol_effect_takes_a_namespace_beside_its_token() {
        /// Every `fn` in `source`, as `(name, parameter list)`, with line comments stripped.
        ///
        /// The parameter list alone: a function that *returns* a token — `validate_publication`
        /// is the one — is a check rather than an effect, and is not what this row is about.
        fn parameter_lists(source: &str) -> Vec<(String, String)> {
            let code = source
                .lines()
                .map(|line| line.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n");

            let mut lists = vec![];
            let mut rest = code.as_str();
            while let Some(at) = rest.find("fn ") {
                let after = &rest[at + 3..];
                let name: String = after
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                let Some(open) = after.find('(') else { break };
                let mut depth = 0usize;
                let mut end = None;
                for (offset, character) in after[open..].char_indices() {
                    match character {
                        '(' => depth += 1,
                        ')' => {
                            depth -= 1;
                            if depth == 0 {
                                end = Some(open + offset);
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                let Some(end) = end else { break };
                if !name.is_empty() {
                    lists.push((name, after[open + 1..end].to_string()));
                }
                rest = &after[end..];
            }
            lists
        }

        let modules = [
            ("gc.rs", include_str!("gc.rs")),
            ("workflow.rs", include_str!("workflow.rs")),
        ];

        let mut acting_on_a_token = vec![];
        for (module, source) in modules {
            for (name, parameters) in parameter_lists(source) {
                if !["Validated<", "CommitPermit"]
                    .iter()
                    .any(|token| parameters.contains(token))
                {
                    continue;
                }

                for namespace in ["ProtocolPaths", "CheckpointRef"] {
                    assert!(
                        !parameters.contains(namespace),
                        "{module}: {name} takes a {namespace} beside its token; \
                         derive it from the token instead ({parameters})"
                    );
                }

                acting_on_a_token.push(format!("{module}:{name}"));
            }
        }

        acting_on_a_token.sort();
        assert_eq!(
            acting_on_a_token,
            [
                "gc.rs:delete_classified_history",
                "workflow.rs:claim_recovery_epoch",
                "workflow.rs:complete_commit",
                "workflow.rs:mark_committed",
                "workflow.rs:publish_current_generation",
                "workflow.rs:publish_generation_manifest",
                "workflow.rs:validate_marker",
            ],
            "the token-taking surface changed; add the new function here after checking it \
             derives its paths from its token"
        );
    }

    #[tokio::test]
    async fn initialize_generation_without_prior_checkpoint_writes_empty_manifest() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(1),
                updated_at: from_micros(123),
                state_backend: StateBackendSelector::Parquet,
                program_operators: HashSet::new(),
            },
            CurrentGenerationPolicy::RequireCurrent,
        )
        .await
        .unwrap();

        let expected_manifest = GenerationManifest::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Generation(1),
            None,
            123,
        );
        assert_eq!(
            initialization,
            GenerationInitialization::Initialized {
                generation_manifest: expected_manifest.clone(),
                recovery: GenerationRecovery::NoCheckpoint,
                recovery_checkpoint: None,
                current_generation: None,
            }
        );

        let written_manifest: GenerationManifest =
            read_json(&store, &paths.generation_manifest(Generation(1)))
                .await
                .unwrap()
                .expect("new generation manifest should be written");
        assert_eq!(written_manifest, expected_manifest);
    }

    /// **PR #167 round 6, finding 5.** A deferred initialization publishes no canonical
    /// current-generation object, and the pointer it hands back writes exactly the one the
    /// publishing policy would have written.
    ///
    /// The canonical pointer is an authoritative protocol input, not an unrooted candidate:
    /// `publish_checkpoint` refuses a checkpoint whose generation is not the current one and
    /// `resolve_generation_manifest` reads a candidate differently depending on whether its
    /// generation is current. A controller that has not yet won its fence duel must therefore
    /// not have written it — losing the duel undoes the metadata root and nothing else.
    ///
    /// Three claims, and the third is what stops the first two from being an accident: the
    /// deferral writes the generation manifest and claims the epoch exactly as publishing does,
    /// it writes no pointer, and the pointer it defers is byte-identical to the published one
    /// apart from the instant it records.
    #[tokio::test]
    async fn a_deferred_initialization_writes_no_canonical_generation_until_it_is_published() {
        let request = || InitializeGenerationRequest {
            pipeline_id: PipelineId::new("P"),
            job_id: JobId::new("J"),
            generation: Generation(1),
            updated_at: from_micros(123),
            state_backend: StateBackendSelector::Parquet,
            program_operators: HashSet::new(),
        };
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));

        // The control: the publishing policy, which writes the pointer itself and defers none.
        let published = MemoryProtocolStore::default();
        let control =
            initialize_generation(&published, request(), CurrentGenerationPolicy::Publish)
                .await
                .unwrap();
        let GenerationInitialization::Initialized {
            current_generation: deferred_by_control,
            ..
        } = &control
        else {
            panic!("expected the generation to be initialized, got {control:?}");
        };
        assert!(
            deferred_by_control.is_none(),
            "the publishing policy writes the pointer and hands nothing back"
        );
        let written: CurrentGeneration = current_marker(&published).await;
        assert_eq!(written.generation, Generation(1));

        // The row: the deferring policy.
        let store = MemoryProtocolStore::default();
        let initialization =
            initialize_generation(&store, request(), CurrentGenerationPolicy::Defer)
                .await
                .unwrap();
        let GenerationInitialization::Initialized {
            generation_manifest,
            current_generation,
            ..
        } = initialization
        else {
            panic!("expected the generation to be initialized");
        };

        // Everything else the initialization does, it did.
        let manifest: GenerationManifest =
            read_json(&store, &paths.generation_manifest(Generation(1)))
                .await
                .unwrap()
                .expect("a deferred initialization still writes its generation manifest");
        assert_eq!(manifest, generation_manifest);

        // But nothing names this generation as current.
        assert_eq!(
            crate::workflow::current_generation(&store, &paths)
                .await
                .unwrap(),
            None,
            "a controller that has not won its fence duel must leave no generation current"
        );

        // And the pointer it deferred is the one publishing would have written.
        let deferred = current_generation.expect("the deferring policy hands the pointer back");
        assert_eq!(deferred.generation(), Generation(1));
        assert_eq!(
            deferred.publish(&store).await.expect("published"),
            CurrentGenerationPublication::Published
        );
        let after: CurrentGeneration = current_marker(&store).await;
        assert_eq!(
            CurrentGeneration {
                updated_at_micros: written.updated_at_micros,
                ..after
            },
            written,
            "the deferred object differs from the published one only in when it was minted"
        );
    }

    /// **PR #167 round 7, finding 2.** A deferred publication is a compare-and-set: a controller
    /// that was superseded between its root CAS and its canonical write cannot revert the
    /// generation the winner made current.
    ///
    /// The reviewer's interleaving, at the level the overwrite actually happens: A defers
    /// generation 1, B defers generation 2, B publishes, then A resumes and publishes. Before
    /// round 7 the last write won and the canonical generation became **1** — and every
    /// checkpoint B's live generation published from that moment was refused as stale, because
    /// `publish_checkpoint` follows this pointer.
    ///
    /// Three claims. A is told it was superseded rather than silently succeeding; the job's
    /// current generation is still B's; and B's own re-publication of the generation that is
    /// already current is accepted, so the rule is "never move it backwards" and not "never
    /// write it twice".
    ///
    /// A's marker for generation 1 may exist afterwards and that is harmless — an additive write
    /// reverts nothing, and readers take the highest marker, which is B's.
    #[tokio::test]
    async fn a_superseded_deferred_publication_cannot_revert_a_newer_current_generation() {
        let store = MemoryProtocolStore::default();
        let request = |generation: u64| InitializeGenerationRequest {
            pipeline_id: PipelineId::new("P"),
            job_id: JobId::new("J"),
            generation: Generation(generation),
            updated_at: from_micros(123),
            state_backend: StateBackendSelector::Parquet,
            program_operators: HashSet::new(),
        };
        let deferred = |initialization| match initialization {
            GenerationInitialization::Initialized {
                current_generation: Some(deferred),
                ..
            } => deferred,
            other => panic!("expected a deferred initialization, got {other:?}"),
        };

        // A resolves generation 1 and defers; B then adopts and resolves generation 2.
        let a = deferred(
            initialize_generation(&store, request(1), CurrentGenerationPolicy::Defer)
                .await
                .unwrap(),
        );
        let b = deferred(
            initialize_generation(&store, request(2), CurrentGenerationPolicy::Defer)
                .await
                .unwrap(),
        );

        // B wins its root CAS and publishes.
        assert_eq!(
            b.publish(&store).await.expect("B publishes"),
            CurrentGenerationPublication::Published
        );

        // A resumes. It won its *own* root update earlier, so nothing it holds says it has lost
        // the job — the store is the only thing that can tell it.
        assert_eq!(
            a.publish(&store)
                .await
                .expect("A's publication is answered"),
            CurrentGenerationPublication::Superseded {
                current_generation: Generation(2)
            },
            "a controller superseded between its root update and its canonical write is told so"
        );
        let current: CurrentGeneration = current_marker(&store).await;
        assert_eq!(
            current.generation,
            Generation(2),
            "and the winner's generation is still the job's current one"
        );

        // Re-publishing the generation that is already current is idempotent, not a refusal.
        assert_eq!(
            b.publish(&store).await.expect("B re-publishes"),
            CurrentGenerationPublication::Published
        );
        let current: CurrentGeneration = current_marker(&store).await;
        assert_eq!(current.generation, Generation(2));
    }

    /// A deferring initialization is under the same monotonicity rule as a publishing one, and is
    /// refused before it claims anything.
    ///
    /// The rule cannot move with the write: an epoch claimed by a controller whose generation is
    /// behind the current one is claimed by a controller that has already lost, and the claim is
    /// irreversible whether or not the pointer is ever written.
    #[tokio::test]
    async fn a_deferred_initialization_is_refused_below_the_current_generation() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(4)).await;

        let err = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(3),
                updated_at: from_micros(123),
                state_backend: StateBackendSelector::Parquet,
                program_operators: HashSet::new(),
            },
            CurrentGenerationPolicy::Defer,
        )
        .await
        .expect_err("a generation behind the current one may not initialize");
        assert!(
            matches!(
                err,
                StoreError::Protocol(ProtocolError::NonMonotonicGenerationUpdate)
            ),
            "got {err:?}"
        );

        let current: CurrentGeneration = current_marker(&store).await;
        assert_eq!(current.generation, Generation(4));
        assert_eq!(
            read_json::<_, GenerationManifest>(&store, &paths.generation_manifest(Generation(3)))
                .await
                .unwrap(),
            None,
            "and nothing was written for the generation that was refused"
        );
    }

    /// Publishing a generation is what commits a job to a recovery checkpoint, so a
    /// checkpoint written by another backend has to be refused *before* either the current
    /// generation file or the new generation manifest is written. The recording store is
    /// the assertion: after the fixture is in place, a rejected initialization must not
    /// write a single object.
    #[tokio::test]
    async fn initialize_generation_writes_nothing_when_the_recovery_checkpoint_disagrees() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = describing(
            checkpoint_for_generation(Generation(1), 1, None, false),
            vec![operator_with_selector(
                global_operator(vec![]),
                "stateengine",
            )],
        );
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;
        put_json(
            &store,
            &paths.generation_manifest(Generation(1)),
            &generation_manifest_for_generation(Generation(1), None, Some(checkpoint_ref.clone())),
        )
        .await
        .unwrap();

        store.forget_writes();

        let err = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(2),
                updated_at: from_micros(456),
                state_backend: StateBackendSelector::Parquet,
                program_operators: HashSet::from(["op".to_string()]),
            },
            CurrentGenerationPolicy::Publish,
        )
        .await
        .expect_err("a recovery checkpoint from another backend must not be published");

        assert!(
            matches!(
                err,
                StoreError::StateBackend(StateBackendError::CheckpointMismatch { .. })
            ),
            "{err:?}"
        );

        assert_eq!(
            store.written_objects(),
            Vec::<String>::new(),
            "no protocol state may be published for a rejected recovery checkpoint"
        );
        // and specifically, neither of the two objects publication consists of
        let current: CurrentGeneration = current_marker(&store).await;
        assert_eq!(current.generation, Generation(1));
        assert!(
            read_json::<_, GenerationManifest>(&store, &paths.generation_manifest(Generation(2)))
                .await
                .unwrap()
                .is_none(),
            "the new generation manifest must not have been written"
        );
    }

    /// The compatibility direction: a recovery checkpoint written before the selector
    /// existed carries no table configs at all, which means parquet, and a parquet job
    /// still initializes its next generation from it and gets the validated manifest back.
    #[tokio::test]
    async fn initialize_generation_publishes_a_legacy_recovery_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = describing(
            checkpoint_for_generation(Generation(1), 1, None, false),
            vec![operator_with_selector(global_operator(vec![]), "")],
        );
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;
        put_json(
            &store,
            &paths.generation_manifest(Generation(1)),
            &generation_manifest_for_generation(Generation(1), None, Some(checkpoint_ref.clone())),
        )
        .await
        .unwrap();

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(2),
                updated_at: from_micros(456),
                state_backend: StateBackendSelector::Parquet,
                program_operators: HashSet::from(["op".to_string()]),
            },
            CurrentGenerationPolicy::Publish,
        )
        .await
        .expect("a legacy recovery checkpoint must still be restorable");

        let GenerationInitialization::Initialized {
            recovery,
            recovery_checkpoint,
            ..
        } = initialization
        else {
            panic!("expected the generation to be initialized, got {initialization:?}");
        };
        assert_eq!(
            recovery,
            GenerationRecovery::Ready {
                checkpoint_ref: checkpoint_ref.clone()
            }
        );
        assert_eq!(
            recovery_checkpoint.as_ref(),
            Some(&checkpoint),
            "the validated manifest should be handed back rather than left to be re-read"
        );

        assert_eq!(current_marker(&store).await.generation, Generation(2));
        assert!(
            read_json::<_, GenerationManifest>(&store, &paths.generation_manifest(Generation(2)))
                .await
                .unwrap()
                .is_some()
        );
    }

    /// Finding 2, in the four shapes a manifest can fail to describe a program.
    ///
    /// Publishing a generation is what commits a job to a recovery checkpoint, so a
    /// manifest that cannot restore the program the workers will build has to be refused
    /// *before* either the current-generation file or the new generation manifest is
    /// written. The recording store is the assertion: after the fixture is in place, a
    /// rejected initialization must not write a single object.
    ///
    /// Each shape fails only in a worker if it gets past here — the omitted operator and
    /// the headerless entry cannot be found by the operator that looks itself up, the
    /// extra one means the checkpoint belongs to a different program, and the duplicate
    /// leaves it undefined which entry would be restored — by which point the protocol
    /// state has already advanced.
    #[tokio::test]
    async fn initialize_generation_refuses_every_unrestorable_manifest_shape() {
        let headerless = OperatorCheckpointMetadata {
            operator_metadata: None,
            ..Default::default()
        };

        let mut problems: Vec<String> = vec![];

        for (name, operators, program, expect) in [
            (
                "missing",
                vec![named_operator("op")],
                vec!["op", "other"],
                "other",
            ),
            (
                "extra",
                vec![named_operator("op"), named_operator("gone")],
                vec!["op"],
                "gone",
            ),
            (
                "duplicate",
                vec![named_operator("op"), named_operator("op")],
                vec!["op"],
                "more than once",
            ),
            (
                "headerless",
                vec![named_operator("op"), headerless.clone()],
                vec!["op"],
                "no operator metadata header",
            ),
        ] {
            let store = MemoryProtocolStore::default();
            let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
            write_current_generation(&store, &paths, Generation(1)).await;

            let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
            let checkpoint = describing(
                checkpoint_for_generation(Generation(1), 1, None, false),
                operators,
            );
            write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;
            put_json(
                &store,
                &paths.generation_manifest(Generation(1)),
                &generation_manifest_for_generation(
                    Generation(1),
                    None,
                    Some(checkpoint_ref.clone()),
                ),
            )
            .await
            .unwrap();

            store.forget_writes();

            let outcome = initialize_generation(
                &store,
                InitializeGenerationRequest {
                    pipeline_id: PipelineId::new("P"),
                    job_id: JobId::new("J"),
                    generation: Generation(2),
                    updated_at: from_micros(456),
                    state_backend: StateBackendSelector::Parquet,
                    program_operators: program.iter().map(|s| s.to_string()).collect(),
                },
                CurrentGenerationPolicy::Publish,
            )
            .await;

            // Collected rather than asserted one shape at a time, so a run reports every
            // shape that got through instead of stopping at the first.
            match outcome {
                Ok(_) => problems.push(format!("{name}: this manifest must not be published")),
                Err(StoreError::IncompleteManifest(incomplete))
                    if incomplete.detail.contains(expect) => {}
                Err(other) => problems.push(format!(
                    "{name}: expected an incomplete-manifest error naming {expect:?}, got \
                     {other:?}"
                )),
            }

            if !store.written_objects().is_empty() {
                problems.push(format!(
                    "{name}: no protocol state may be published for a manifest that cannot \
                     restore the program, but {:?} was written",
                    store.written_objects()
                ));
            }
            let current: CurrentGeneration = current_marker(&store).await;
            if current.generation != Generation(1) {
                problems.push(format!("{name}: the current generation was advanced"));
            }
            if read_json::<_, GenerationManifest>(&store, &paths.generation_manifest(Generation(2)))
                .await
                .unwrap()
                .is_some()
            {
                problems.push(format!(
                    "{name}: the new generation manifest must not have been written"
                ));
            }
        }

        assert!(problems.is_empty(), "{}", problems.join("\n"));
    }

    /// Points generation 1's manifest at `checkpoint_ref` and writes `checkpoint` there, then
    /// forgets the writes so a refusal's "nothing was published" can be asserted.
    async fn stage_recovery_candidate(
        store: &MemoryProtocolStore,
        paths: &ProtocolPaths,
        checkpoint_ref: &CheckpointRef,
        checkpoint: &CheckpointManifest,
    ) {
        write_current_generation(store, paths, Generation(1)).await;
        write_canonical_checkpoint(store, paths, checkpoint_ref, checkpoint).await;
        put_json(
            store,
            &paths.generation_manifest(Generation(1)),
            &generation_manifest_for_generation(Generation(1), None, Some(checkpoint_ref.clone())),
        )
        .await
        .unwrap();
        store.forget_writes();
    }

    /// Nothing at all was published, for a refused initialization.
    async fn assert_published_nothing(
        store: &MemoryProtocolStore,
        paths: &ProtocolPaths,
        name: &str,
        problems: &mut Vec<String>,
    ) {
        if !store.written_objects().is_empty() {
            problems.push(format!(
                "{name}: no protocol state may be published, but {:?} was written",
                store.written_objects()
            ));
        }
        let current: CurrentGeneration = current_marker(store).await;
        if current.generation != Generation(1) {
            problems.push(format!("{name}: the current generation was advanced"));
        }
        if read_json::<_, GenerationManifest>(store, &paths.generation_manifest(Generation(2)))
            .await
            .unwrap()
            .is_some()
        {
            problems.push(format!(
                "{name}: the new generation manifest must not have been written"
            ));
        }
    }

    /// The wiring row for finding 1 on the publishing path: `initialize_generation` refuses a
    /// recovery manifest that is not the checkpoint the reference it was read from names, and
    /// publishes neither of the two objects publication consists of (PR #160 review round 7).
    ///
    /// Every case is read from generation 1 epoch 1's own reference — the one the previous
    /// generation's manifest records — and differs from it in exactly one of the four
    /// identities the manifest carries, including the two that would otherwise be inferred from
    /// the others. The recovery resolution succeeds in all four: the epoch record, the parent
    /// status and the selector all agree, which is what makes this a test of the identity
    /// binding and not of the resolution around it.
    ///
    /// `generation` and `epoch` are the two review round 6 left out, on the reasoning that a
    /// recovery checkpoint is always from an earlier generation and epoch. That is true of the
    /// generation being *published* and says nothing about the *reference*.
    #[tokio::test]
    async fn initialize_generation_refuses_a_recovery_manifest_that_is_not_where_it_says_it_is() {
        let mut problems: Vec<String> = vec![];

        for (name, pipeline_id, job_id, generation, epoch) in [
            ("another pipeline", "P2", "J", 1u64, 1u64),
            ("another job", "P", "J2", 1, 1),
            ("another generation", "P", "J", 3, 1),
            ("another epoch", "P", "J", 1, 9),
        ] {
            let store = MemoryProtocolStore::default();
            let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));

            // Always read from generation 1, epoch 1: the location is fixed and only what the
            // object claims about itself moves.
            let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
            let mut checkpoint =
                checkpoint_for_generation(Generation(generation), epoch, None, false);
            checkpoint.pipeline_id = pipeline_id.to_string();
            checkpoint.job_id = job_id.to_string();
            let checkpoint = describing(checkpoint, vec![named_operator("op")]);
            stage_recovery_candidate(&store, &paths, &checkpoint_ref, &checkpoint).await;

            let outcome = initialize_generation(
                &store,
                InitializeGenerationRequest {
                    pipeline_id: PipelineId::new("P"),
                    job_id: JobId::new("J"),
                    generation: Generation(2),
                    updated_at: from_micros(456),
                    state_backend: StateBackendSelector::Parquet,
                    program_operators: HashSet::from(["op".to_string()]),
                },
                CurrentGenerationPolicy::Publish,
            )
            .await;

            match outcome {
                Ok(_) => problems.push(format!(
                    "{name}: a manifest that is not the checkpoint its reference names must \
                     not be published"
                )),
                Err(StoreError::Protocol(ProtocolError::CheckpointManifestMisplaced {
                    ref checkpoint_ref,
                    ..
                })) if *checkpoint_ref == paths.checkpoint_manifest(Generation(1), Epoch(1)) => {}
                Err(other) => problems.push(format!(
                    "{name}: expected a misplaced-manifest error naming the reference, got \
                     {other:?}"
                )),
            }

            assert_published_nothing(&store, &paths, name, &mut problems).await;
        }

        assert!(problems.is_empty(), "{}", problems.join("\n"));
    }

    /// The wiring row for finding 2 on the publishing path: `initialize_generation` refuses a
    /// recovery manifest whose entry is headed for another checkpoint, and publishes nothing
    /// (PR #160 review round 7).
    ///
    /// The outer manifest is right, the reference is right, and the operator set covers the
    /// program exactly — the finding's exact shape. What the header decides is which state
    /// directory the restoring worker reads and which one expiring-table compaction writes to,
    /// so accepting it publishes a generation pointed at another checkpoint's state.
    #[tokio::test]
    async fn initialize_generation_refuses_a_recovery_manifest_entry_from_another_checkpoint() {
        let mut problems: Vec<String> = vec![];

        for (name, header_job, header_epoch, expect) in [
            ("another job", "J2", 1u32, "job \"J2\""),
            ("another epoch", "J", 5, "epoch 5"),
        ] {
            let store = MemoryProtocolStore::default();
            let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));

            let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
            let mut checkpoint = checkpoint_for_generation(Generation(1), 1, None, false);
            let mut operator = named_operator("op");
            let header = operator.operator_metadata.as_mut().unwrap();
            header.job_id = header_job.to_string();
            header.epoch = header_epoch;
            checkpoint.operators = vec![operator];
            stage_recovery_candidate(&store, &paths, &checkpoint_ref, &checkpoint).await;

            let outcome = initialize_generation(
                &store,
                InitializeGenerationRequest {
                    pipeline_id: PipelineId::new("P"),
                    job_id: JobId::new("J"),
                    generation: Generation(2),
                    updated_at: from_micros(456),
                    state_backend: StateBackendSelector::Parquet,
                    program_operators: HashSet::from(["op".to_string()]),
                },
                CurrentGenerationPolicy::Publish,
            )
            .await;

            match outcome {
                Ok(_) => problems.push(format!(
                    "{name}: an entry headed for another checkpoint must not be published"
                )),
                Err(StoreError::IncompleteManifest(ref incomplete))
                    if incomplete.detail.contains(expect) => {}
                Err(other) => problems.push(format!(
                    "{name}: expected an incomplete-manifest error naming {expect:?}, got \
                     {other:?}"
                )),
            }

            assert_published_nothing(&store, &paths, name, &mut problems).await;
        }

        assert!(problems.is_empty(), "{}", problems.join("\n"));
    }

    /// An entry a parquet job can restore: `operator_id`'s tables, carrying the table configs a
    /// checkpoint written before the selector existed reports, which is what parquet means.
    fn restorable_operator(operator_id: &str) -> OperatorCheckpointMetadata {
        operator_with_selector(named_operator(operator_id), "")
    }

    /// The worker leader's own request: generation 2, and `update_current_generation` false, so
    /// the current generation is read and conformed to rather than written. This is the shape
    /// PR #160 review round 8 is about — it is the only one that can reach an unclaimed
    /// recovery candidate, because the controller writes the current generation before the
    /// leader runs.
    fn leader_initialization(program_operators: &[&str]) -> InitializeGenerationRequest {
        InitializeGenerationRequest {
            pipeline_id: PipelineId::new("P"),
            job_id: JobId::new("J"),
            generation: Generation(2),
            updated_at: from_micros(456),
            state_backend: StateBackendSelector::Parquet,
            program_operators: program_operators
                .iter()
                .map(|operator| operator.to_string())
                .collect(),
        }
    }

    /// Points generation 1's manifest at `checkpoint_ref` and writes `checkpoint` there with
    /// **no epoch record** — the unclaimed shape — under a store whose current generation is
    /// already the one being initialized.
    ///
    /// This is what the two round-7 rows could not stage: they call
    /// `initialize_generation(.., CurrentGenerationPolicy::Publish)` against already-canonical state, so the epoch is
    /// claimed before the call and no claim happens during it.
    async fn stage_unclaimed_recovery_candidate(
        store: &MemoryProtocolStore,
        paths: &ProtocolPaths,
        checkpoint_ref: &CheckpointRef,
        checkpoint: &CheckpointManifest,
    ) {
        write_current_generation(store, paths, Generation(2)).await;
        put_protobuf(store, checkpoint_ref, checkpoint)
            .await
            .unwrap();
        put_json(
            store,
            &paths.generation_manifest(Generation(1)),
            &generation_manifest_for_generation(Generation(1), None, Some(checkpoint_ref.clone())),
        )
        .await
        .unwrap();
        store.forget_writes();
    }

    /// Nothing was written, and in particular no epoch was taken — neither the one the
    /// reference names nor the one the object claims for itself.
    async fn assert_claimed_nothing(
        store: &MemoryProtocolStore,
        paths: &ProtocolPaths,
        name: &str,
        problems: &mut Vec<String>,
    ) {
        if !store.written_objects().is_empty() {
            problems.push(format!(
                "{name}: no protocol state may be written, but {:?} was",
                store.written_objects()
            ));
        }
        for epoch in [1, 9] {
            if read_json::<_, EpochRecord>(store, &paths.epoch_record(Epoch(epoch)))
                .await
                .unwrap()
                .is_some()
            {
                problems.push(format!(
                    "{name}: epoch {epoch} was claimed for a rejected recovery candidate"
                ));
            }
        }
        if read_json::<_, GenerationManifest>(store, &paths.generation_manifest(Generation(2)))
            .await
            .unwrap()
            .is_some()
        {
            problems.push(format!(
                "{name}: the new generation manifest must not have been written"
            ));
        }
    }

    /// Which family of check a recovery candidate is expected to fail.
    #[derive(Debug, Clone, Copy)]
    enum Refusal {
        /// The manifest is not the checkpoint the reference it was read from names.
        Misplaced,
        /// The manifest cannot restore the program, and the message names `&'static str`.
        Unrestorable(&'static str),
        /// The manifest was written by a backend this job does not select.
        ForeignBackend,
    }

    /// PR #160 review round 8: an unclaimed recovery candidate does not get its epoch until it
    /// has earned the publication token, and a candidate that fails to earn it leaves the epoch
    /// exactly as it found it.
    ///
    /// One row per validation family rather than one row that trips whichever check fires
    /// first, because the ordering being asserted is between *each* check and the claim, not
    /// between the first check and the claim.
    ///
    /// Each case ends by putting a manifest the job *can* restore at the same reference and
    /// initializing again. That is the assertion the epoch-absence check exists for: a claim
    /// made for the rejected candidate would have been immutable, and the valid checkpoint for
    /// the same epoch would have been orphaned by it for good — a state no later fence can
    /// repair, which is why this belongs to T25 and not to T26.
    #[tokio::test]
    async fn initialize_generation_claims_no_epoch_for_a_recovery_candidate_it_refuses() {
        let mut problems: Vec<String> = vec![];

        // Read from generation 1 epoch 1's own reference in every case; only the object at it
        // moves.
        let misplaced = {
            let mut checkpoint = checkpoint_for_generation(Generation(1), 9, None, false);
            checkpoint.min_epoch = 9;
            describing(checkpoint, vec![restorable_operator("op")])
        };
        let uncovered = describing(
            checkpoint_for_generation(Generation(1), 1, None, false),
            vec![restorable_operator("op")],
        );
        let foreign_entry = {
            let mut checkpoint = checkpoint_for_generation(Generation(1), 1, None, false);
            let mut operator = restorable_operator("op");
            operator.operator_metadata.as_mut().unwrap().job_id = "J2".to_string();
            checkpoint.operators = vec![operator];
            checkpoint
        };
        let foreign_backend = describing(
            checkpoint_for_generation(Generation(1), 1, None, false),
            vec![operator_with_selector(named_operator("op"), "stateengine")],
        );

        for (name, candidate, program, refusal) in [
            (
                "misplaced manifest",
                &misplaced,
                &["op"][..],
                Refusal::Misplaced,
            ),
            (
                "operator coverage",
                &uncovered,
                &["op", "other"][..],
                Refusal::Unrestorable("other"),
            ),
            (
                "entry identity",
                &foreign_entry,
                &["op"][..],
                Refusal::Unrestorable("job \"J2\""),
            ),
            (
                "backend selector",
                &foreign_backend,
                &["op"][..],
                Refusal::ForeignBackend,
            ),
        ] {
            let store = MemoryProtocolStore::default();
            let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
            let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
            stage_unclaimed_recovery_candidate(&store, &paths, &checkpoint_ref, candidate).await;

            let outcome = initialize_generation(
                &store,
                leader_initialization(program),
                CurrentGenerationPolicy::RequireCurrent,
            )
            .await;

            match (refusal, outcome) {
                (_, Ok(initialized)) => problems.push(format!(
                    "{name}: this candidate must not be published, got {initialized:?}"
                )),
                (
                    Refusal::Misplaced,
                    Err(StoreError::Protocol(ProtocolError::CheckpointManifestMisplaced {
                        checkpoint_ref: ref named,
                        ..
                    })),
                ) if *named == checkpoint_ref => {}
                (
                    Refusal::Unrestorable(expect),
                    Err(StoreError::IncompleteManifest(ref detail)),
                ) if detail.detail.contains(expect) => {}
                (
                    Refusal::ForeignBackend,
                    Err(StoreError::StateBackend(StateBackendError::CheckpointMismatch { .. })),
                ) => {}
                (expected, Err(other)) => {
                    problems.push(format!("{name}: expected {expected:?}, got {other:?}"))
                }
            }

            assert_claimed_nothing(&store, &paths, name, &mut problems).await;

            // The epoch is still there to be taken: the checkpoint that should have been at
            // this reference all along claims it and the generation initializes from it.
            let restorable = describing(
                checkpoint_for_generation(Generation(1), 1, None, false),
                program.iter().copied().map(restorable_operator).collect(),
            );
            put_protobuf(&store, &checkpoint_ref, &restorable)
                .await
                .unwrap();

            match initialize_generation(
                &store,
                leader_initialization(program),
                CurrentGenerationPolicy::RequireCurrent,
            )
            .await
            {
                Ok(GenerationInitialization::Initialized { recovery, .. })
                    if recovery
                        == (GenerationRecovery::Ready {
                            checkpoint_ref: checkpoint_ref.clone(),
                        }) => {}
                other => problems.push(format!(
                    "{name}: a valid candidate for the same epoch must still recover, got \
                     {other:?}"
                )),
            }

            match read_json::<_, EpochRecord>(&store, &paths.epoch_record(Epoch(1)))
                .await
                .unwrap()
            {
                Some(record)
                    if record.checkpoint_ref == checkpoint_ref
                        && record.generation == Generation(1)
                        && record.epoch == Epoch(1) => {}
                other => problems.push(format!(
                    "{name}: the epoch should now be owned by the valid checkpoint, got {other:?}"
                )),
            }
        }

        assert!(problems.is_empty(), "{}", problems.join("\n"));
    }

    /// A refused candidate is a refusal, not a hint to look further back.
    ///
    /// Deferring the claim raised the question of what a failed validation does to the search,
    /// and the answer has to be "stops it": an older generation's checkpoint is a *different*
    /// recovery point, so quietly initializing from it would restore a job from state its
    /// operator asked nothing about. Generation 0 here holds a perfectly restorable canonical
    /// checkpoint, and initialization still fails on generation 1's candidate.
    #[tokio::test]
    async fn initialize_generation_does_not_fall_back_past_a_recovery_candidate_it_refuses() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));

        let older_ref = paths.checkpoint_manifest(Generation(0), Epoch(0));
        let older = describing(
            checkpoint_for_generation(Generation(0), 0, None, false),
            vec![restorable_operator("op")],
        );
        write_canonical_checkpoint(&store, &paths, &older_ref, &older).await;
        put_json(
            &store,
            &paths.generation_manifest(Generation(0)),
            &generation_manifest_for_generation(Generation(0), None, Some(older_ref.clone())),
        )
        .await
        .unwrap();

        let candidate_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let candidate = describing(
            checkpoint_for_generation(Generation(1), 1, None, false),
            vec![operator_with_selector(named_operator("op"), "stateengine")],
        );
        stage_unclaimed_recovery_candidate(&store, &paths, &candidate_ref, &candidate).await;

        let err = initialize_generation(
            &store,
            leader_initialization(&["op"]),
            CurrentGenerationPolicy::RequireCurrent,
        )
        .await
        .expect_err("a refused candidate must not become a search for an older one");

        assert!(
            matches!(
                err,
                StoreError::StateBackend(StateBackendError::CheckpointMismatch { .. })
            ),
            "{err:?}"
        );
        assert_eq!(
            store.written_objects(),
            Vec::<String>::new(),
            "a refused candidate publishes nothing, including from an older generation"
        );
        assert!(
            read_json::<_, EpochRecord>(&store, &paths.epoch_record(Epoch(1)))
                .await
                .unwrap()
                .is_none(),
            "the refused candidate's epoch must still be unclaimed"
        );
        assert!(
            read_json::<_, GenerationManifest>(&store, &paths.generation_manifest(Generation(2)))
                .await
                .unwrap()
                .is_none(),
            "no generation manifest may be published for an older checkpoint instead"
        );
    }

    /// The positive half: a candidate that earns the token is claimed, and the claim is what
    /// decides how this generation recovers from it.
    ///
    /// `needs_commit` is the dimension that separates the two outcomes, so it is varied rather
    /// than fixed, and the replay-commit case drives the permit through `complete_commit` —
    /// a permit that cannot write the marker it authorizes is not a permit.
    #[tokio::test]
    async fn initialize_generation_claims_an_unclaimed_recovery_candidate_it_validated() {
        let mut problems: Vec<String> = vec![];

        for needs_commit in [false, true] {
            let name = if needs_commit {
                "needs commit"
            } else {
                "ready"
            };
            let store = MemoryProtocolStore::default();
            let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
            let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
            let checkpoint = describing(
                checkpoint_for_generation(Generation(1), 1, None, needs_commit),
                vec![restorable_operator("op")],
            );
            stage_unclaimed_recovery_candidate(&store, &paths, &checkpoint_ref, &checkpoint).await;

            let initialization = initialize_generation(
                &store,
                leader_initialization(&["op"]),
                CurrentGenerationPolicy::RequireCurrent,
            )
            .await
            .expect("a candidate this job can restore must initialize");

            let GenerationInitialization::Initialized {
                generation_manifest,
                recovery,
                recovery_checkpoint,
                ..
            } = initialization
            else {
                problems.push(format!("{name}: expected an initialized generation"));
                continue;
            };

            assert_eq!(
                generation_manifest,
                {
                    let mut expected = GenerationManifest::new(
                        PipelineId::new("P"),
                        JobId::new("J"),
                        Generation(2),
                        Some(checkpoint_ref.clone()),
                        456,
                    );
                    expected.latest_checkpoint_ref = None;
                    expected
                },
                "{name}"
            );
            assert_eq!(recovery_checkpoint.as_ref(), Some(&checkpoint), "{name}");

            let record = read_json::<_, EpochRecord>(&store, &paths.epoch_record(Epoch(1)))
                .await
                .unwrap()
                .expect("the claim writes the epoch record");
            assert_eq!(record.checkpoint_ref, checkpoint_ref, "{name}");
            assert_eq!(record.epoch, Epoch(1), "{name}");
            assert_eq!(record.generation, Generation(1), "{name}");
            assert_eq!(record.pipeline_id, PipelineId::new("P"), "{name}");
            assert_eq!(record.job_id, JobId::new("J"), "{name}");

            match (needs_commit, recovery) {
                (
                    false,
                    GenerationRecovery::Ready {
                        checkpoint_ref: got,
                    },
                ) => {
                    assert_eq!(got, checkpoint_ref, "{name}");
                }
                (
                    true,
                    GenerationRecovery::ReplayCommit {
                        checkpoint_ref: got,
                        commit_permit,
                    },
                ) => {
                    assert_eq!(got, checkpoint_ref, "{name}");
                    assert_eq!(commit_permit.checkpoint_ref(), &checkpoint_ref, "{name}");
                    assert_eq!(commit_permit.epoch_record(), &record, "{name}");

                    let completion = complete_commit(&store, &commit_permit, Generation(2))
                        .await
                        .expect("the permit must authorize the commit it was issued for");
                    assert_eq!(completion, CommittedMarkerOutcome::Created, "{name}");
                    assert!(
                        exists(&store, &paths.committed_marker(Generation(1), Epoch(1))).await,
                        "{name}: the marker the permit authorized should be there"
                    );
                }
                (_, other) => problems.push(format!("{name}: unexpected recovery {other:?}")),
            }
        }

        assert!(problems.is_empty(), "{}", problems.join("\n"));
    }

    /// A store that hides an object from the *first* read of it and shows it afterwards.
    ///
    /// This is exactly how an epoch claimed between resolution and the claim looks from inside
    /// `initialize_generation`: the search sees a candidate nothing owns, and the conditional
    /// create that follows finds the record already there. Nothing else changes — in particular
    /// the hidden object is fully present for the create, which is what makes the claim lose.
    struct HidesFromTheFirstRead {
        inner: MemoryProtocolStore,
        hidden: Mutex<HashSet<String>>,
    }

    impl HidesFromTheFirstRead {
        fn hiding(inner: MemoryProtocolStore, path: &CheckpointRef) -> Self {
            Self {
                inner,
                hidden: Mutex::new(HashSet::from([path.to_string()])),
            }
        }
    }

    #[async_trait]
    impl ProtocolStore for HidesFromTheFirstRead {
        async fn read_bytes(&self, path: &CheckpointRef) -> Result<Option<Vec<u8>>, StoreError> {
            if self.hidden.lock().unwrap().remove(&path.to_string()) {
                return Ok(None);
            }
            self.inner.read_bytes(path).await
        }

        async fn put_bytes(&self, path: &CheckpointRef, bytes: Vec<u8>) -> Result<(), StoreError> {
            self.inner.put_bytes(path, bytes).await
        }

        async fn create_bytes(
            &self,
            path: &CheckpointRef,
            bytes: Vec<u8>,
        ) -> Result<CreateResult<Vec<u8>>, StoreError> {
            self.inner.create_bytes(path, bytes).await
        }

        async fn list_child_directories(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
            self.inner.list_child_directories(prefix).await
        }

        async fn delete_object(&self, path: &CheckpointRef) -> Result<(), StoreError> {
            self.inner.delete_object(path).await
        }

        async fn delete_directory(&self, path: &str) {
            self.inner.delete_directory(path).await
        }
    }

    /// Losing the claim after validation redirects to the epoch's canonical checkpoint, and
    /// that checkpoint is validated in its own right before anything is published for it.
    ///
    /// Deferring the claim created this path: the claim can now come back `Orphaned` *after* a
    /// publication token has been produced, and that token certifies the candidate that lost.
    /// Publishing on it would record a generation manifest pointing at a checkpoint nothing
    /// checked — so the redirect is re-read and re-checked, and the second case proves the
    /// re-check is real by making the winner one this job cannot restore.
    #[tokio::test]
    async fn initialize_generation_revalidates_the_canonical_checkpoint_after_a_lost_claim() {
        for (name, winner_operator, expect_published) in [
            ("restorable winner", restorable_operator("op"), true),
            (
                "winner from another backend",
                operator_with_selector(named_operator("op"), "stateengine"),
                false,
            ),
        ] {
            let memory = MemoryProtocolStore::default();
            let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));

            // The winner owns epoch 1 and lives under generation 0; the loser is generation 1's
            // candidate for the same epoch.
            let winner_ref = paths.checkpoint_manifest(Generation(0), Epoch(1));
            let winner = describing(
                checkpoint_for_generation(Generation(0), 1, None, false),
                vec![winner_operator],
            );
            write_canonical_checkpoint(&memory, &paths, &winner_ref, &winner).await;

            let loser_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
            let loser = describing(
                checkpoint_for_generation(Generation(1), 1, None, false),
                vec![restorable_operator("op")],
            );
            stage_unclaimed_recovery_candidate(&memory, &paths, &loser_ref, &loser).await;

            let store =
                HidesFromTheFirstRead::hiding(memory.clone(), &paths.epoch_record(Epoch(1)));

            let outcome = initialize_generation(
                &store,
                leader_initialization(&["op"]),
                CurrentGenerationPolicy::RequireCurrent,
            )
            .await;

            if !expect_published {
                assert!(
                    matches!(
                        outcome,
                        Err(StoreError::StateBackend(
                            StateBackendError::CheckpointMismatch { .. }
                        ))
                    ),
                    "{name}: the redirect must be validated in its own right, got {outcome:?}"
                );
                assert_eq!(
                    memory.written_objects(),
                    Vec::<String>::new(),
                    "{name}: nothing may be published for a redirect that fails validation"
                );
                assert!(
                    read_json::<_, GenerationManifest>(
                        &memory,
                        &paths.generation_manifest(Generation(2))
                    )
                    .await
                    .unwrap()
                    .is_none(),
                    "{name}: no generation manifest may be written"
                );
                continue;
            }

            let GenerationInitialization::Initialized {
                generation_manifest,
                recovery,
                recovery_checkpoint,
                ..
            } = outcome.expect("the canonical checkpoint is restorable")
            else {
                panic!("{name}: expected an initialized generation");
            };

            assert_eq!(
                recovery,
                GenerationRecovery::Ready {
                    checkpoint_ref: winner_ref.clone()
                },
                "{name}"
            );
            assert_eq!(
                recovery_checkpoint.as_ref(),
                Some(&winner),
                "{name}: the manifest handed back must be the winner's, not the loser's"
            );
            assert_eq!(
                generation_manifest.base_checkpoint_ref,
                Some(winner_ref.clone()),
                "{name}: the published generation manifest must record the winner"
            );

            let written: GenerationManifest =
                read_json(&memory, &paths.generation_manifest(Generation(2)))
                    .await
                    .unwrap()
                    .expect("{name}: the generation manifest should be published");
            assert_eq!(written.base_checkpoint_ref, Some(winner_ref), "{name}");

            let record = read_json::<_, EpochRecord>(&memory, &paths.epoch_record(Epoch(1)))
                .await
                .unwrap()
                .expect("the winner's epoch record is untouched");
            assert_eq!(
                record.checkpoint_ref,
                paths.checkpoint_manifest(Generation(0), Epoch(1)),
                "{name}: the loser must not have taken the epoch"
            );
            assert_eq!(
                loser.epoch, 1,
                "{name}: the loser and the winner must be for the same epoch, or this row \
                 proves nothing"
            );
        }
    }

    /// The recovery search resolves; it does not write.
    ///
    /// PR #160 review round 8: an epoch record written while resolving made a candidate the
    /// canonical checkpoint of its epoch before `initialize_generation` had checked it, and an
    /// epoch record is immutable. The fix moved the claim to the far side of the publication
    /// token, and this is what keeps it there — a source pin over the resolution module naming
    /// every entry point in `store` through which a byte can be written or removed, plus the
    /// two workflow functions that wrap them.
    ///
    /// Line comments are stripped first, so the module's own prose may discuss the claim it
    /// deliberately does not make. Adding a write to that module fails this row, which is the
    /// forcing function rather than a stale assertion.
    #[test]
    fn the_recovery_resolution_module_reaches_no_persistent_write() {
        let source = include_str!("workflow/recovery.rs")
            .lines()
            .map(|line| line.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");

        let reached: Vec<&str> = [
            "put_bytes",
            "create_bytes",
            "delete_object",
            "delete_directory",
            "put_json",
            "put_protobuf",
            "create_json_if_not_exist",
            "create_protobuf",
            "claim_epoch_record",
            "mark_committed",
        ]
        .into_iter()
        .filter(|entry_point| source.contains(entry_point))
        .collect();

        assert!(
            reached.is_empty(),
            "the recovery search must perform no persistent write, but it reaches {reached:?}; \
             an effect that makes a checkpoint canonical belongs after the publication token, \
             not inside the search that found the candidate"
        );
    }

    #[tokio::test]
    async fn initialize_generation_restores_previous_ready_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(2)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint_for_generation(Generation(1), 1, None, false);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;
        let previous_manifest =
            generation_manifest_for_generation(Generation(1), None, Some(checkpoint_ref.clone()));
        put_json(
            &store,
            &paths.generation_manifest(Generation(1)),
            &previous_manifest,
        )
        .await
        .unwrap();

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(2),
                updated_at: from_micros(456),
                state_backend: StateBackendSelector::Parquet,
                program_operators: HashSet::new(),
            },
            CurrentGenerationPolicy::RequireCurrent,
        )
        .await
        .unwrap();

        let expected_manifest = GenerationManifest::new(
            PipelineId::new("P"),
            JobId::new("J"),
            Generation(2),
            Some(checkpoint_ref.clone()),
            456,
        );
        assert_eq!(
            initialization,
            GenerationInitialization::Initialized {
                generation_manifest: expected_manifest.clone(),
                recovery: GenerationRecovery::Ready {
                    checkpoint_ref: checkpoint_ref.clone()
                },
                recovery_checkpoint: Some(checkpoint.clone()),
                current_generation: None,
            }
        );

        let written_manifest: GenerationManifest =
            read_json(&store, &paths.generation_manifest(Generation(2)))
                .await
                .unwrap()
                .expect("new generation manifest should be written");
        assert_eq!(written_manifest, expected_manifest);
    }

    #[tokio::test]
    async fn initialize_generation_restores_previous_checkpoint_requiring_commit_replay() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(2)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint_for_generation(Generation(1), 1, None, true);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;
        let previous_manifest =
            generation_manifest_for_generation(Generation(1), None, Some(checkpoint_ref.clone()));
        put_json(
            &store,
            &paths.generation_manifest(Generation(1)),
            &previous_manifest,
        )
        .await
        .unwrap();

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(2),
                updated_at: from_micros(456),
                state_backend: StateBackendSelector::Parquet,
                program_operators: HashSet::new(),
            },
            CurrentGenerationPolicy::RequireCurrent,
        )
        .await
        .unwrap();

        assert_eq!(
            initialization,
            GenerationInitialization::Initialized {
                generation_manifest: GenerationManifest::new(
                    PipelineId::new("P"),
                    JobId::new("J"),
                    Generation(2),
                    Some(checkpoint_ref.clone()),
                    456,
                ),
                recovery: GenerationRecovery::ReplayCommit {
                    checkpoint_ref: checkpoint_ref.clone(),
                    commit_permit: commit_permit(checkpoint_ref, &checkpoint),
                },
                recovery_checkpoint: Some(checkpoint.clone()),
                current_generation: None,
            }
        );
    }

    #[tokio::test]
    async fn initialize_generation_skips_missing_previous_manifest() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(3)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint_for_generation(Generation(1), 1, None, false);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;
        put_json(
            &store,
            &paths.generation_manifest(Generation(1)),
            &generation_manifest_for_generation(Generation(1), None, Some(checkpoint_ref.clone())),
        )
        .await
        .unwrap();

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(3),
                updated_at: from_micros(789),
                state_backend: StateBackendSelector::Parquet,
                program_operators: HashSet::new(),
            },
            CurrentGenerationPolicy::RequireCurrent,
        )
        .await
        .unwrap();

        assert!(matches!(
            initialization,
            GenerationInitialization::Initialized {
                recovery: GenerationRecovery::Ready { .. },
                ..
            }
        ));
        let written_manifest: GenerationManifest =
            read_json(&store, &paths.generation_manifest(Generation(3)))
                .await
                .unwrap()
                .expect("new generation manifest should be written");
        assert_eq!(written_manifest.base_checkpoint_ref, Some(checkpoint_ref));
    }

    #[tokio::test]
    async fn initialize_generation_claims_unclaimed_previous_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(2)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint_for_generation(Generation(1), 1, None, false);
        put_protobuf(&store, &checkpoint_ref, &checkpoint)
            .await
            .unwrap();
        put_json(
            &store,
            &paths.generation_manifest(Generation(1)),
            &generation_manifest_for_generation(Generation(1), None, Some(checkpoint_ref.clone())),
        )
        .await
        .unwrap();

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(2),
                updated_at: from_micros(456),
                state_backend: StateBackendSelector::Parquet,
                program_operators: HashSet::new(),
            },
            CurrentGenerationPolicy::RequireCurrent,
        )
        .await
        .unwrap();

        assert!(matches!(
            initialization,
            GenerationInitialization::Initialized {
                recovery: GenerationRecovery::Ready { .. },
                ..
            }
        ));
        let record: EpochRecord = read_json(&store, &paths.epoch_record(Epoch(1)))
            .await
            .unwrap()
            .expect("unclaimed checkpoint should be claimed during initialization");
        assert_eq!(record.checkpoint_ref, checkpoint_ref);
        assert_eq!(record.generation, Generation(1));
    }

    #[tokio::test]
    async fn initialize_generation_recovers_canonical_checkpoint_from_orphaned_previous_manifest() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(3)).await;

        let winner_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let loser_ref = paths.checkpoint_manifest(Generation(2), Epoch(1));
        let winner_checkpoint = checkpoint_for_generation(Generation(1), 1, None, false);
        let loser_checkpoint = checkpoint_for_generation(Generation(2), 1, None, false);
        put_protobuf(&store, &winner_ref, &winner_checkpoint)
            .await
            .unwrap();
        put_protobuf(&store, &loser_ref, &loser_checkpoint)
            .await
            .unwrap();
        let epoch_record = epoch_record(winner_ref.clone(), &winner_checkpoint);
        put_json(&store, &paths.epoch_record(Epoch(1)), &epoch_record)
            .await
            .unwrap();
        put_json(
            &store,
            &paths.generation_manifest(Generation(2)),
            &generation_manifest_for_generation(Generation(2), None, Some(loser_ref)),
        )
        .await
        .unwrap();

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(3),
                updated_at: from_micros(456),
                state_backend: StateBackendSelector::Parquet,
                program_operators: HashSet::new(),
            },
            CurrentGenerationPolicy::RequireCurrent,
        )
        .await
        .unwrap();

        assert_eq!(
            initialization,
            GenerationInitialization::Initialized {
                generation_manifest: GenerationManifest::new(
                    PipelineId::new("P"),
                    JobId::new("J"),
                    Generation(3),
                    Some(winner_ref.clone()),
                    456,
                ),
                recovery: GenerationRecovery::Ready {
                    checkpoint_ref: winner_ref.clone()
                },
                recovery_checkpoint: Some(winner_checkpoint.clone()),
                current_generation: None,
            }
        );
        let written_manifest: GenerationManifest =
            read_json(&store, &paths.generation_manifest(Generation(3)))
                .await
                .unwrap()
                .expect("replacement generation manifest should be written");
        assert_eq!(written_manifest.base_checkpoint_ref, Some(winner_ref));
    }

    #[tokio::test]
    async fn initialize_generation_replays_commit_for_canonical_checkpoint_from_orphaned_manifest()
    {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(3)).await;

        let winner_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let loser_ref = paths.checkpoint_manifest(Generation(2), Epoch(1));
        let winner_checkpoint = checkpoint_for_generation(Generation(1), 1, None, true);
        let loser_checkpoint = checkpoint_for_generation(Generation(2), 1, None, true);
        put_protobuf(&store, &winner_ref, &winner_checkpoint)
            .await
            .unwrap();
        put_protobuf(&store, &loser_ref, &loser_checkpoint)
            .await
            .unwrap();
        let epoch_record = epoch_record(winner_ref.clone(), &winner_checkpoint);
        put_json(&store, &paths.epoch_record(Epoch(1)), &epoch_record)
            .await
            .unwrap();
        put_json(
            &store,
            &paths.generation_manifest(Generation(2)),
            &generation_manifest_for_generation(Generation(2), None, Some(loser_ref)),
        )
        .await
        .unwrap();

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(3),
                updated_at: from_micros(456),
                state_backend: StateBackendSelector::Parquet,
                program_operators: HashSet::new(),
            },
            CurrentGenerationPolicy::RequireCurrent,
        )
        .await
        .unwrap();

        assert_eq!(
            initialization,
            GenerationInitialization::Initialized {
                generation_manifest: GenerationManifest::new(
                    PipelineId::new("P"),
                    JobId::new("J"),
                    Generation(3),
                    Some(winner_ref.clone()),
                    456,
                ),
                recovery: GenerationRecovery::ReplayCommit {
                    checkpoint_ref: winner_ref.clone(),
                    commit_permit: commit_permit(winner_ref, &winner_checkpoint),
                },
                recovery_checkpoint: Some(winner_checkpoint.clone()),
                current_generation: None,
            }
        );
    }

    #[tokio::test]
    async fn initialize_generation_exits_when_stale() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(3)).await;

        let initialization = initialize_generation(
            &store,
            InitializeGenerationRequest {
                pipeline_id: PipelineId::new("P"),
                job_id: JobId::new("J"),
                generation: Generation(2),
                updated_at: from_micros(456),
                state_backend: StateBackendSelector::Parquet,
                program_operators: HashSet::new(),
            },
            CurrentGenerationPolicy::RequireCurrent,
        )
        .await
        .unwrap();

        assert_eq!(
            initialization,
            GenerationInitialization::StaleGeneration {
                current_generation: Generation(3)
            }
        );
        assert!(
            read_json::<_, GenerationManifest>(&store, &paths.generation_manifest(Generation(2)))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn non_committing_checkpoint_is_unclaimed_until_epoch_record_exists() {
        let checkpoint_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let checkpoint = checkpoint(1, None, false);

        let state =
            derive_checkpoint_state(&checkpoint_ref, Some(&checkpoint), None, None).unwrap();

        assert_eq!(state, CheckpointState::Unclaimed);
        assert!(!state.is_ready());
    }

    #[test]
    fn epoch_record_makes_non_committing_checkpoint_ready() {
        let checkpoint_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let checkpoint = checkpoint(1, None, false);
        let record = epoch_record(checkpoint_ref.clone(), &checkpoint);

        let state = derive_checkpoint_state(
            &checkpoint_ref,
            Some(&checkpoint),
            Some(record.clone()),
            None,
        )
        .unwrap();

        assert_eq!(state, CheckpointState::Ready);
        assert!(state.is_canonical());
    }

    #[test]
    fn orphaning_applies_to_non_committing_checkpoints() {
        let loser_ref = checkpoint_ref(
            "P/J/generations/2/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let winner_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let checkpoint = checkpoint(1, None, false);
        let record = epoch_record(winner_ref.clone(), &checkpoint);

        let state =
            derive_checkpoint_state(&loser_ref, Some(&checkpoint), Some(record), None).unwrap();

        assert_eq!(
            state,
            CheckpointState::Orphaned {
                canonical_ref: winner_ref
            }
        );
    }

    #[test]
    fn committing_checkpoint_requires_committed_marker_to_be_ready() {
        let checkpoint_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let checkpoint = checkpoint(1, None, true);
        let record = epoch_record(checkpoint_ref.clone(), &checkpoint);

        let state = derive_checkpoint_state(
            &checkpoint_ref,
            Some(&checkpoint),
            Some(record.clone()),
            None,
        )
        .unwrap();
        assert_eq!(
            state,
            CheckpointState::Committing {
                epoch_record: record.clone()
            }
        );
        assert!(state.requires_commit_replay());

        let marker = committed_marker(checkpoint_ref.clone(), 1);
        let state = derive_checkpoint_state(
            &checkpoint_ref,
            Some(&checkpoint),
            Some(record),
            Some(&marker),
        )
        .unwrap();
        assert_eq!(state, CheckpointState::Ready);
    }

    #[test]
    fn resolve_current_generation_claims_unclaimed_candidate() {
        let checkpoint_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let manifest = generation_manifest(None, Some(checkpoint_ref.clone()));
        let checkpoint = checkpoint(1, None, false);

        let decision = resolve_candidate(
            &manifest,
            &checkpoint_ref,
            Some(&checkpoint),
            None,
            None,
            ParentCheckpointStatus::NoParent,
            true,
        )
        .unwrap();

        assert_eq!(decision, ResolveDecision::ClaimUnclaimed { checkpoint_ref });
    }

    #[test]
    fn resolve_stale_generation_falls_back_from_unclaimed_latest() {
        let base_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let latest_ref = checkpoint_ref(
            "P/J/generations/2/checkpoints/checkpoint-0000002/checkpoint-manifest.pb",
        );
        let manifest = generation_manifest(Some(base_ref), Some(latest_ref.clone()));
        let checkpoint = checkpoint(2, None, false);

        let decision = resolve_candidate(
            &manifest,
            &latest_ref,
            Some(&checkpoint),
            None,
            None,
            ParentCheckpointStatus::NoParent,
            false,
        )
        .unwrap();

        assert_eq!(decision, ResolveDecision::FallbackToBase);
    }

    #[test]
    fn resolve_orphaned_candidate_stops_generation() {
        let loser_ref = checkpoint_ref(
            "P/J/generations/2/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let winner_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let manifest = generation_manifest(None, Some(loser_ref.clone()));
        let checkpoint = checkpoint(1, None, false);
        let record = epoch_record(winner_ref.clone(), &checkpoint);

        let decision = resolve_candidate(
            &manifest,
            &loser_ref,
            Some(&checkpoint),
            Some(record),
            None,
            ParentCheckpointStatus::NoParent,
            true,
        )
        .unwrap();

        assert_eq!(
            decision,
            ResolveDecision::StopOrphaned {
                canonical_ref: winner_ref
            }
        );
    }

    #[test]
    fn resolve_canonical_uncommitted_checkpoint_requires_replay() {
        let checkpoint_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let manifest = generation_manifest(None, Some(checkpoint_ref.clone()));
        let checkpoint = checkpoint(1, None, true);
        let record = epoch_record(checkpoint_ref.clone(), &checkpoint);

        let decision = resolve_candidate(
            &manifest,
            &checkpoint_ref,
            Some(&checkpoint),
            Some(record.clone()),
            None,
            ParentCheckpointStatus::NoParent,
            true,
        )
        .unwrap();

        assert_eq!(
            decision,
            ResolveDecision::ReplayCommit {
                checkpoint_ref,
                epoch_record: record,
            }
        );
    }

    #[test]
    fn resolve_rejects_child_when_parent_is_not_ready_canonical() {
        let parent_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000001/checkpoint-manifest.pb",
        );
        let child_ref = checkpoint_ref(
            "P/J/generations/1/checkpoints/checkpoint-0000002/checkpoint-manifest.pb",
        );
        let manifest = generation_manifest(None, Some(child_ref.clone()));
        let child = checkpoint(2, Some(parent_ref), false);

        let decision = resolve_candidate(
            &manifest,
            &child_ref,
            Some(&child),
            None,
            None,
            ParentCheckpointStatus::NotReadyCanonical,
            true,
        )
        .unwrap();

        assert_eq!(
            decision,
            ResolveDecision::Failed(ResolveFailure::ParentNotReadyCanonical)
        );
    }

    #[tokio::test]
    async fn claim_epoch_record_creates_canonical_record() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        let epoch_record_path = paths.epoch_record(Epoch(1));

        let outcome = claim_epoch_record(
            &store,
            ClaimEpochRecordRequest {
                epoch_record_path: &epoch_record_path,
                pipeline_id: &PipelineId::new("P"),
                generation: Generation(1),
                checkpoint_ref: &checkpoint_ref,
                checkpoint: &checkpoint,
                created_at: from_micros(123),
            },
        )
        .await
        .unwrap();

        assert!(matches!(outcome, EpochClaimOutcome::Owned { .. }));

        let record: EpochRecord = read_json(&store, &epoch_record_path)
            .await
            .unwrap()
            .expect("epoch record should have been written");
        assert_eq!(record.checkpoint_ref, checkpoint_ref);
        assert_eq!(record.epoch, Epoch(1));
    }

    #[tokio::test]
    async fn claim_epoch_record_treats_existing_same_record_as_owned() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        let epoch_record_path = paths.epoch_record(Epoch(1));
        let request = ClaimEpochRecordRequest {
            epoch_record_path: &epoch_record_path,
            pipeline_id: &PipelineId::new("P"),
            generation: Generation(1),
            checkpoint_ref: &checkpoint_ref,
            checkpoint: &checkpoint,
            created_at: from_micros(123),
        };

        claim_epoch_record(&store, request.clone()).await.unwrap();
        let outcome = claim_epoch_record(&store, request).await.unwrap();

        assert!(matches!(outcome, EpochClaimOutcome::Owned { .. }));
    }

    #[tokio::test]
    async fn claim_epoch_record_orphans_different_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let winner_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let loser_ref = paths.checkpoint_manifest(Generation(2), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        let epoch_record_path = paths.epoch_record(Epoch(1));

        claim_epoch_record(
            &store,
            ClaimEpochRecordRequest {
                epoch_record_path: &epoch_record_path,
                pipeline_id: &PipelineId::new("P"),
                generation: Generation(1),
                checkpoint_ref: &winner_ref,
                checkpoint: &checkpoint,
                created_at: from_micros(123),
            },
        )
        .await
        .unwrap();

        let outcome = claim_epoch_record(
            &store,
            ClaimEpochRecordRequest {
                epoch_record_path: &epoch_record_path,
                pipeline_id: &PipelineId::new("P"),
                generation: Generation(2),
                checkpoint_ref: &loser_ref,
                checkpoint: &checkpoint,
                created_at: from_micros(124),
            },
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            EpochClaimOutcome::Orphaned {
                canonical_ref: winner_ref
            }
        );
    }

    /// The commit marker is created where the permit's own epoch record says, and the
    /// same-shaped path under another job is left alone.
    ///
    /// `validate_marker` has always required the marker's *contents* to be the permit's
    /// checkpoint; its *location* was a free argument until PR #160's GC-namespace review
    /// finding was swept across the class. A permit for one job could therefore create a
    /// commit marker in another job's namespace, where the next `prepare_commit` there reads
    /// it as that checkpoint's commit and skips a commit that never happened.
    #[tokio::test]
    async fn the_commit_marker_is_written_only_where_the_permit_names() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let bystander = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J2"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        let permit = commit_permit(checkpoint_ref.clone(), &checkpoint);
        let marker = committed_marker(checkpoint_ref, 1);

        let outcome = mark_committed(&store, &marker, &permit).await.unwrap();

        assert_eq!(outcome, CommittedMarkerOutcome::Created);
        assert!(exists(&store, &paths.committed_marker(Generation(1), Epoch(1))).await);
        assert!(!exists(&store, &bystander.committed_marker(Generation(1), Epoch(1))).await);
    }

    #[tokio::test]
    async fn mark_committed_is_idempotent_for_same_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let committed_marker_path = paths.committed_marker(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        let permit = commit_permit(checkpoint_ref.clone(), &checkpoint);
        let marker = committed_marker(checkpoint_ref, 1);

        let outcome = mark_committed(&store, &marker, &permit).await.unwrap();
        assert_eq!(outcome, CommittedMarkerOutcome::Created);
        assert!(exists(&store, &committed_marker_path).await);

        let outcome = mark_committed(&store, &marker, &permit).await.unwrap();
        assert_eq!(outcome, CommittedMarkerOutcome::AlreadyCommitted);
    }

    #[tokio::test]
    async fn mark_committed_rejects_existing_marker_for_different_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let winner_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let loser_ref = paths.checkpoint_manifest(Generation(2), Epoch(1));
        let committed_marker_path = paths.committed_marker(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        let permit = commit_permit(winner_ref.clone(), &checkpoint);
        let winner_marker = committed_marker(winner_ref, 1);
        let loser_marker = committed_marker(loser_ref, 1);

        mark_committed(&store, &winner_marker, &permit)
            .await
            .unwrap();
        assert!(exists(&store, &committed_marker_path).await);

        let err = mark_committed(&store, &loser_marker, &permit)
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            StoreError::Protocol(ProtocolError::CommittedMarkerMismatch)
        ));
    }

    #[tokio::test]
    async fn prepare_commit_authorizes_canonical_uncommitted_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        let epoch_record = epoch_record(checkpoint_ref.clone(), &checkpoint);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;

        let authorization =
            prepare_commit(&store, &checkpoint_ref, checkpoint.clone(), None, false)
                .await
                .unwrap();

        assert_eq!(
            authorization,
            CommitAuthorization::Authorized {
                checkpoint_ref: checkpoint_ref.clone(),
                checkpoint: checkpoint.clone(),
                commit_permit: CommitPermit::new(checkpoint_ref, &checkpoint, epoch_record)
                    .unwrap(),
            }
        );
    }

    #[tokio::test]
    async fn prepare_commit_reports_already_committed() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;
        put_json(
            &store,
            &paths.committed_marker(Generation(1), Epoch(1)),
            &committed_marker(checkpoint_ref.clone(), 1),
        )
        .await
        .unwrap();

        let authorization = prepare_commit(&store, &checkpoint_ref, checkpoint, None, false)
            .await
            .unwrap();

        assert_eq!(
            authorization,
            CommitAuthorization::AlreadyCommitted { checkpoint_ref }
        );
    }

    #[tokio::test]
    async fn prepare_commit_rejects_unclaimed_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        put_protobuf(&store, &checkpoint_ref, &checkpoint)
            .await
            .unwrap();

        let authorization = prepare_commit(&store, &checkpoint_ref, checkpoint, None, false)
            .await
            .unwrap();

        assert_eq!(
            authorization,
            CommitAuthorization::NotCanonical { checkpoint_ref }
        );
    }

    #[tokio::test]
    async fn prepare_commit_stops_orphaned_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let winner_ref = paths.checkpoint_manifest(Generation(2), Epoch(1));
        let loser_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        put_protobuf(&store, &loser_ref, &checkpoint).await.unwrap();
        put_json(
            &store,
            &paths.epoch_record(Epoch(1)),
            &epoch_record(winner_ref.clone(), &checkpoint),
        )
        .await
        .unwrap();

        let authorization = prepare_commit(&store, &loser_ref, checkpoint, None, false)
            .await
            .unwrap();

        assert_eq!(
            authorization,
            CommitAuthorization::StopOrphaned {
                canonical_ref: winner_ref
            }
        );
    }

    #[tokio::test]
    async fn prepare_commit_skips_non_committing_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;

        let authorization = prepare_commit(&store, &checkpoint_ref, checkpoint, None, false)
            .await
            .unwrap();

        assert_eq!(
            authorization,
            CommitAuthorization::NoCommitNeeded { checkpoint_ref }
        );
    }

    #[tokio::test]
    async fn complete_commit_writes_committed_marker() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        let permit = commit_permit(checkpoint_ref.clone(), &checkpoint);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;

        let completion = complete_commit(&store, &permit, Generation(2))
            .await
            .unwrap();

        assert_eq!(completion, CommittedMarkerOutcome::Created);
        let marker: CommittedMarker =
            read_json(&store, &paths.committed_marker(Generation(1), Epoch(1)))
                .await
                .unwrap()
                .expect("committed marker should be written");
        assert_eq!(marker.checkpoint_ref, checkpoint_ref);
        assert_eq!(marker.writer_generation, Generation(2));
    }

    #[tokio::test]
    async fn complete_commit_is_idempotent() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        let permit = commit_permit(checkpoint_ref.clone(), &checkpoint);
        write_canonical_checkpoint(&store, &paths, &checkpoint_ref, &checkpoint).await;

        complete_commit(&store, &permit, Generation(2))
            .await
            .unwrap();
        let completion = complete_commit(&store, &permit, Generation(3))
            .await
            .unwrap();

        assert_eq!(completion, CommittedMarkerOutcome::AlreadyCommitted);
    }

    #[tokio::test]
    async fn complete_commit_writes_marker_from_epoch_record() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        let permit = commit_permit(checkpoint_ref.clone(), &checkpoint);
        put_protobuf(&store, &checkpoint_ref, &checkpoint)
            .await
            .unwrap();

        let completion = complete_commit(&store, &permit, Generation(2))
            .await
            .unwrap();

        assert_eq!(completion, CommittedMarkerOutcome::Created);
        let marker: CommittedMarker =
            read_json(&store, &paths.committed_marker(Generation(1), Epoch(1)))
                .await
                .unwrap()
                .expect("committed marker should be written");
        assert_eq!(marker.checkpoint_ref, checkpoint_ref);
    }

    #[tokio::test]
    async fn storage_provider_store_round_trips_conditional_json_create() {
        let temp_dir = std::env::temp_dir().join(format!(
            "arroyo-state-protocol-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let storage =
            arroyo_storage::StorageProvider::for_url(&format!("file://{}", temp_dir.display()))
                .await
                .unwrap();
        let store = storage;
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        let record = epoch_record(checkpoint_ref, &checkpoint);
        let epoch_record_path = paths.epoch_record(Epoch(1));

        let created = create_json_if_not_exist(&store, &epoch_record_path, &record)
            .await
            .unwrap();
        assert_eq!(created, CreateResult::Created);

        let existing = create_json_if_not_exist(&store, &epoch_record_path, &record)
            .await
            .unwrap();
        assert_eq!(existing, CreateResult::AlreadyExists(record));

        let _ = tokio::fs::remove_dir_all(temp_dir).await;
    }

    /// An unclaimed candidate under the current generation is *reported*, not taken.
    ///
    /// Renamed from `resolve_generation_manifest_claims_unclaimed_current_latest` in PR #160
    /// review round 8, which is the round that moved the claim out of resolution: writing the
    /// epoch record here made the candidate the canonical checkpoint of its epoch before
    /// anything had established that the job asking could restore it, and an epoch record is
    /// immutable. The claim itself is asserted by
    /// `initialize_generation_claims_an_unclaimed_recovery_candidate_it_validated`; what is
    /// asserted here is that resolution left the epoch alone.
    #[tokio::test]
    async fn resolve_generation_manifest_reports_an_unclaimed_current_latest_without_claiming_it() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        put_protobuf(&store, &checkpoint_ref, &checkpoint)
            .await
            .unwrap();

        let manifest = generation_manifest(None, Some(checkpoint_ref.clone()));
        store.forget_writes();

        let resolution = resolve_generation_manifest(&store, &manifest, Generation(1))
            .await
            .unwrap();

        assert_eq!(
            resolution,
            GenerationResolution::ClaimRequired { checkpoint_ref }
        );
        assert!(
            read_json::<_, EpochRecord>(&store, &paths.epoch_record(Epoch(1)))
                .await
                .unwrap()
                .is_none(),
            "resolving must not claim the epoch"
        );
        assert_eq!(
            store.written_objects(),
            Vec::<String>::new(),
            "resolving a generation manifest writes nothing at all"
        );
    }

    /// The same for a candidate that still owes external commit: which of `Ready` and
    /// `ReplayCommit` it becomes is decided by the claim, so resolution reports the candidate
    /// and says nothing about the commit. Renamed from
    /// `resolve_generation_manifest_claims_unclaimed_commit_checkpoint` in PR #160 review
    /// round 8; the replay-commit permit it used to assert is now asserted by
    /// `initialize_generation_claims_an_unclaimed_recovery_candidate_it_validated`.
    #[tokio::test]
    async fn resolve_generation_manifest_reports_an_unclaimed_commit_checkpoint_without_claiming_it()
     {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        put_protobuf(&store, &checkpoint_ref, &checkpoint)
            .await
            .unwrap();

        let manifest = generation_manifest(None, Some(checkpoint_ref.clone()));
        store.forget_writes();

        let resolution = resolve_generation_manifest(&store, &manifest, Generation(1))
            .await
            .unwrap();

        assert_eq!(
            resolution,
            GenerationResolution::ClaimRequired {
                checkpoint_ref: checkpoint_ref.clone()
            }
        );
        assert!(
            read_json::<_, EpochRecord>(&store, &paths.epoch_record(Epoch(1)))
                .await
                .unwrap()
                .is_none(),
            "resolving must not claim the epoch"
        );
        assert_eq!(
            store.written_objects(),
            Vec::<String>::new(),
            "resolving a generation manifest writes nothing at all"
        );
    }

    #[tokio::test]
    async fn resolve_generation_manifest_falls_back_to_base_for_stale_unclaimed_latest() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(3)).await;

        let base_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let base = checkpoint(1, None, false);
        put_protobuf(&store, &base_ref, &base).await.unwrap();
        put_json(
            &store,
            &paths.epoch_record(Epoch(1)),
            &epoch_record(base_ref.clone(), &base),
        )
        .await
        .unwrap();

        let latest_ref = paths.checkpoint_manifest(Generation(2), Epoch(2));
        let latest = checkpoint(2, Some(base_ref.clone()), false);
        put_protobuf(&store, &latest_ref, &latest).await.unwrap();

        let manifest = generation_manifest(Some(base_ref.clone()), Some(latest_ref));

        let resolution = resolve_generation_manifest(&store, &manifest, Generation(2))
            .await
            .unwrap();

        assert_eq!(
            resolution,
            GenerationResolution::Ready {
                checkpoint_ref: base_ref
            }
        );
    }

    #[tokio::test]
    async fn resolve_generation_manifest_stops_on_orphaned_latest() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(2)).await;

        let winner_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let loser_ref = paths.checkpoint_manifest(Generation(2), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        put_protobuf(&store, &loser_ref, &checkpoint).await.unwrap();
        put_json(
            &store,
            &paths.epoch_record(Epoch(1)),
            &epoch_record(winner_ref.clone(), &checkpoint),
        )
        .await
        .unwrap();

        let manifest = generation_manifest(None, Some(loser_ref));

        let resolution = resolve_generation_manifest(&store, &manifest, Generation(2))
            .await
            .unwrap();

        assert_eq!(
            resolution,
            GenerationResolution::StopOrphaned {
                canonical_ref: winner_ref
            }
        );
    }

    #[tokio::test]
    async fn resolve_generation_manifest_rejects_checkpoint_with_unready_parent() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let parent_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let child_ref = paths.checkpoint_manifest(Generation(1), Epoch(2));
        let child = checkpoint(2, Some(parent_ref), false);
        put_protobuf(&store, &child_ref, &child).await.unwrap();

        let manifest = generation_manifest(None, Some(child_ref));

        let resolution = resolve_generation_manifest(&store, &manifest, Generation(1))
            .await
            .unwrap();

        assert_eq!(
            resolution,
            GenerationResolution::Failed(ResolveFailure::ParentNotReadyCanonical)
        );
    }

    #[tokio::test]
    async fn publish_non_committing_checkpoint_writes_protocol_objects() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        let manifest = generation_manifest(None, None);

        let publication = publish_checkpoint(
            &store,
            PublishCheckpointRequest {
                generation_manifest: &manifest,
                checkpoint_ref: &checkpoint_ref,
                checkpoint: &checkpoint,
                created_at: from_micros(42),
            },
        )
        .await
        .unwrap();

        assert_eq!(
            publication,
            CheckpointPublication::Ready {
                checkpoint_ref: checkpoint_ref.clone()
            }
        );

        let written_checkpoint: CheckpointManifest = read_protobuf(&store, &checkpoint_ref)
            .await
            .unwrap()
            .expect("checkpoint manifest should be written");
        assert_eq!(written_checkpoint, checkpoint);

        let written_generation_manifest: GenerationManifest =
            read_json(&store, &paths.generation_manifest(Generation(1)))
                .await
                .unwrap()
                .expect("generation manifest should be updated");
        assert_eq!(
            written_generation_manifest.latest_checkpoint_ref,
            Some(checkpoint_ref.clone())
        );

        let record: EpochRecord = read_json(&store, &paths.epoch_record(Epoch(1)))
            .await
            .unwrap()
            .expect("epoch record should be written");
        assert_eq!(record.checkpoint_ref, checkpoint_ref);
    }

    #[tokio::test]
    async fn publish_committing_checkpoint_requires_commit() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        let manifest = generation_manifest(None, None);

        let publication = publish_checkpoint(
            &store,
            PublishCheckpointRequest {
                generation_manifest: &manifest,
                checkpoint_ref: &checkpoint_ref,
                checkpoint: &checkpoint,
                created_at: from_micros(42),
            },
        )
        .await
        .unwrap();

        let expected_record = EpochRecord::for_checkpoint(
            PipelineId::new("P"),
            Generation(1),
            checkpoint_ref.clone(),
            &checkpoint,
            from_micros(42),
        )
        .unwrap();
        assert_eq!(
            publication,
            CheckpointPublication::CommitRequired {
                checkpoint_ref: checkpoint_ref.clone(),
                commit_permit: CommitPermit::new(checkpoint_ref, &checkpoint, expected_record)
                    .unwrap(),
            }
        );
    }

    #[tokio::test]
    async fn publish_checkpoint_exits_when_generation_is_stale() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(2)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        let manifest = generation_manifest(None, None);

        let publication = publish_checkpoint(
            &store,
            PublishCheckpointRequest {
                generation_manifest: &manifest,
                checkpoint_ref: &checkpoint_ref,
                checkpoint: &checkpoint,
                created_at: from_micros(42),
            },
        )
        .await
        .unwrap();

        assert_eq!(publication, CheckpointPublication::StaleGeneration);
        assert!(
            read_protobuf::<_, CheckpointManifest>(&store, &checkpoint_ref)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn publish_checkpoint_is_idempotent_for_existing_same_manifest() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        let manifest = generation_manifest(None, None);
        let request = PublishCheckpointRequest {
            generation_manifest: &manifest,
            checkpoint_ref: &checkpoint_ref,
            checkpoint: &checkpoint,
            created_at: from_micros(42),
        };

        publish_checkpoint(&store, request.clone()).await.unwrap();
        let publication = publish_checkpoint(&store, request).await.unwrap();

        assert_eq!(publication, CheckpointPublication::Ready { checkpoint_ref });
    }

    #[tokio::test]
    async fn publish_checkpoint_stops_when_epoch_record_points_elsewhere() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let winner_ref = paths.checkpoint_manifest(Generation(2), Epoch(1));
        let loser_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        put_json(
            &store,
            &paths.epoch_record(Epoch(1)),
            &epoch_record(winner_ref.clone(), &checkpoint),
        )
        .await
        .unwrap();

        let manifest = generation_manifest(None, None);
        let publication = publish_checkpoint(
            &store,
            PublishCheckpointRequest {
                generation_manifest: &manifest,
                checkpoint_ref: &loser_ref,
                checkpoint: &checkpoint,
                created_at: from_micros(42),
            },
        )
        .await
        .unwrap();

        assert_eq!(
            publication,
            CheckpointPublication::StopOrphaned {
                canonical_ref: winner_ref
            }
        );
    }

    /// The write side of the identity binding: a checkpoint manifest may only be published at
    /// the reference its own generation and epoch name (PR #160 review round 7).
    ///
    /// [`publish_checkpoint`] is the only producer of a checkpoint manifest object in Arroyo,
    /// so this is what makes "a manifest is where it says it is" a property of the store rather
    /// than a convention `finish_checkpoint_leader` happens to keep — and the read-side rules
    /// that refuse a misplaced object have a matching write-side rule that cannot create one.
    ///
    /// Nothing is written for a refused publication: the manifest object is the *first* thing
    /// this function creates, so a late rejection would leave an object no reader will accept.
    #[tokio::test]
    async fn publish_checkpoint_refuses_a_manifest_written_away_from_its_own_reference() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint = checkpoint(1, None, false);
        let manifest = generation_manifest(None, None);
        store.forget_writes();

        // Generation 1, epoch 1's manifest, offered at generation 1 epoch 2's reference.
        let elsewhere = paths.checkpoint_manifest(Generation(1), Epoch(2));
        let err = publish_checkpoint(
            &store,
            PublishCheckpointRequest {
                generation_manifest: &manifest,
                checkpoint_ref: &elsewhere,
                checkpoint: &checkpoint,
                created_at: from_micros(42),
            },
        )
        .await
        .expect_err("a manifest may not be published away from its own reference");

        assert!(
            matches!(
                err,
                StoreError::Protocol(ProtocolError::CheckpointManifestMisplaced { .. })
            ),
            "{err:?}"
        );
        assert_eq!(store.written_objects(), Vec::<String>::new());
        assert!(!exists(&store, &elsewhere).await);

        // Its own reference publishes, and is otherwise the same call.
        let own = paths.checkpoint_manifest(Generation(1), Epoch(1));
        publish_checkpoint(
            &store,
            PublishCheckpointRequest {
                generation_manifest: &manifest,
                checkpoint_ref: &own,
                checkpoint: &checkpoint,
                created_at: from_micros(42),
            },
        )
        .await
        .expect("a manifest published at its own reference is the ordinary case");
        assert!(exists(&store, &own).await);
    }

    #[tokio::test]
    async fn publish_checkpoint_rejects_unready_parent() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let parent_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let child_ref = paths.checkpoint_manifest(Generation(1), Epoch(2));
        let child = checkpoint(2, Some(parent_ref), false);
        let manifest = generation_manifest(None, None);

        let publication = publish_checkpoint(
            &store,
            PublishCheckpointRequest {
                generation_manifest: &manifest,
                checkpoint_ref: &child_ref,
                checkpoint: &child,
                created_at: from_micros(42),
            },
        )
        .await
        .unwrap();

        assert_eq!(
            publication,
            CheckpointPublication::Failed(ResolveFailure::ParentNotReadyCanonical)
        );
        assert!(
            read_json::<_, EpochRecord>(&store, &paths.epoch_record(Epoch(2)))
                .await
                .unwrap()
                .is_none()
        );
        // PR #160 review round 8: a refused publication writes nothing at all. This check used
        // to run *after* the manifest object had been created, so a checkpoint whose parent was
        // not ready left its manifest behind for a publication that never happened.
        assert!(
            !exists(&store, &child_ref).await,
            "a refused publication must not leave the checkpoint manifest behind"
        );
        assert_eq!(
            store.written_objects(),
            vec![paths.current_generation_marker(Generation(1)).to_string()],
            "only the fixture's own current-generation write should have happened"
        );
    }
}
