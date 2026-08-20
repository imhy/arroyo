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
    pub fn current_generation(&self) -> CheckpointRef {
        self.path("current-generation.json")
    }

    /// Path to a generation manifest.
    pub fn generation_manifest(&self, generation: Generation) -> CheckpointRef {
        self.path(format!("generations/{generation}/generation-manifest.json"))
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
        CommittedMarkerOutcome, GenerationInitialization, GenerationRecovery, GenerationResolution,
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
    use prost::Message;
    use std::collections::HashSet;
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

        put_json(store, &paths.current_generation(), &current_generation)
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
            paths: &paths,
        };
        // A classified history as the traversal records one: the links it read, newest
        // first — each with the reference it was read from, which is what binds what a
        // manifest says about itself to where it actually was — and the plan it derived
        // from them.
        let history = |reached: Vec<(u64, OperatorCheckpointMetadata)>, deleting: Vec<u64>| {
            let mut history = CheckpointHistory::default();
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
        let mut misplaced = CheckpointHistory::default();
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
        delete_classified_history(&store, &paths, &validated)
            .await
            .unwrap();

        assert!(!exists(&store, &old_file).await);
        assert!(exists(&store, &kept_file).await);
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
            false,
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
            }
        );

        let written_manifest: GenerationManifest =
            read_json(&store, &paths.generation_manifest(Generation(1)))
                .await
                .unwrap()
                .expect("new generation manifest should be written");
        assert_eq!(written_manifest, expected_manifest);
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
            true,
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
        let current: CurrentGeneration = read_json(&store, &paths.current_generation())
            .await
            .unwrap()
            .expect("the previous current generation should still be there");
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
            true,
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

        let current: CurrentGeneration = read_json(&store, &paths.current_generation())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.generation, Generation(2));
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
                true,
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
            let current: CurrentGeneration = read_json(&store, &paths.current_generation())
                .await
                .unwrap()
                .expect("the previous current generation should still be there");
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
        let current: CurrentGeneration = read_json(store, &paths.current_generation())
            .await
            .unwrap()
            .expect("the previous current generation should still be there");
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
                true,
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
                true,
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
            false,
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
            false,
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
            false,
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
            false,
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
            false,
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
            false,
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
            false,
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

    #[tokio::test]
    async fn mark_committed_is_idempotent_for_same_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let committed_marker_path = paths.committed_marker(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        let permit = commit_permit(checkpoint_ref.clone(), &checkpoint);
        let marker = committed_marker(checkpoint_ref, 1);

        let outcome = mark_committed(&store, &committed_marker_path, &marker, &permit)
            .await
            .unwrap();
        assert_eq!(outcome, CommittedMarkerOutcome::Created);

        let outcome = mark_committed(&store, &committed_marker_path, &marker, &permit)
            .await
            .unwrap();
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

        mark_committed(&store, &committed_marker_path, &winner_marker, &permit)
            .await
            .unwrap();

        let err = mark_committed(&store, &committed_marker_path, &loser_marker, &permit)
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

    #[tokio::test]
    async fn resolve_generation_manifest_claims_unclaimed_current_latest() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, false);
        put_protobuf(&store, &checkpoint_ref, &checkpoint)
            .await
            .unwrap();

        let manifest = generation_manifest(None, Some(checkpoint_ref.clone()));

        let resolution = resolve_generation_manifest(&store, &manifest, Generation(1))
            .await
            .unwrap();

        assert_eq!(resolution, GenerationResolution::Ready { checkpoint_ref });
        assert!(
            read_json::<_, EpochRecord>(&store, &paths.epoch_record(Epoch(1)))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn resolve_generation_manifest_claims_unclaimed_commit_checkpoint() {
        let store = MemoryProtocolStore::default();
        let paths = ProtocolPaths::new(PipelineId::new("P"), JobId::new("J"));
        write_current_generation(&store, &paths, Generation(1)).await;

        let checkpoint_ref = paths.checkpoint_manifest(Generation(1), Epoch(1));
        let checkpoint = checkpoint(1, None, true);
        put_protobuf(&store, &checkpoint_ref, &checkpoint)
            .await
            .unwrap();

        let manifest = generation_manifest(None, Some(checkpoint_ref.clone()));

        let resolution = resolve_generation_manifest(&store, &manifest, Generation(1))
            .await
            .unwrap();

        match resolution {
            GenerationResolution::ReplayCommit {
                checkpoint_ref: recovered_ref,
                commit_permit,
            } => {
                assert_eq!(recovered_ref, checkpoint_ref);
                assert_eq!(commit_permit.checkpoint_ref(), &checkpoint_ref);
            }
            other => panic!("expected replay commit resolution, got {other:?}"),
        }
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
    }
}
