//! The handover from a started execution to the state that runs it.
//!
//! Split out of [`super::execution`] when the answer to PR #160 review comment `5384611151`
//! took that module past the plan's 500-line production bar. A **child** of `admission` rather
//! than a sibling, for the reason `execution.rs` and `observation.rs` are: these methods reach
//! `PhaseContext`'s private fields, and a sibling would force them open to `phases.rs` and
//! undo the structural argument the compile-fail rows rest on.
//!
//! The cut is the one the file already documented — `execution.rs` carried two `impl` blocks
//! under two headings, "the two interruptible waits" and this one. Nothing here was rewritten:
//! the block moved verbatim, which is what keeps this round's diff readable as the fix it is.

use std::sync::Arc;
use std::time::Instant;

use arroyo_rpc::LeaderContext;
use arroyo_rpc::config::config;
use arroyo_types::JobId;
use arroyo_worker::job_controller::job_metrics::JobMetrics;
use tracing::info;

use super::super::Scheduling;
use super::PhaseContext;
use crate::job_controller::JobController;
use crate::job_controller::checkpoint_store::DbCheckpointMetadataStore;
use crate::job_controller::leader_manager::LeaderManager;
use crate::states::leader_running::LeaderRunning;
use crate::states::running::Running as RunningState;
use crate::states::{Admission, StateError, Transition};

/// The handover from a started execution to the state that runs it.
impl PhaseContext<'_, '_> {
    /// Whether the restored checkpoint left a two-phase commit to finish.
    ///
    /// Answered from the flag [`Self::prepare_handover`] records rather than from
    /// `committing_state`, which that same handover has by then moved into the job controller.
    pub(crate) fn needs_restored_commits(&self) -> bool {
        self.restored_commits_pending
    }

    /// Records the task count and builds the job controller a non-leader execution runs under.
    ///
    /// Nothing here is irreversible, which is why it happens before the third admission is
    /// taken rather than inside it.
    ///
    /// # Errors
    ///
    /// Retryable, for the one reason [`fence_protocol`](Self::fence_protocol) can fail: a
    /// controller that must fence and holds no adopted fence cannot address the generation whose
    /// commits this controller would publish. It is not reachable from a completed fan-out —
    /// which failed for the same reason before reaching here — and it is propagated rather than
    /// defaulted, because a commit that quietly went out unfenced is precisely the write a
    /// superseded controller must not be able to make.
    pub(crate) async fn prepare_handover(&mut self) -> Result<(), StateError> {
        self.ctx.status.tasks = Some(self.ctx.program.task_count() as i32);
        // Before the controller is built, because building it takes the commits with it.
        self.restored_commits_pending = self.committing_state.is_some();
        if self.leader_mode {
            return Ok(());
        }

        let program = Arc::new(self.ctx.program.clone());
        let metrics = if config().controller.metrics.enabled {
            let metrics = JobMetrics::new(program.clone());
            self.ctx
                .metrics
                .write()
                .await
                .insert(self.ctx.config.id.clone(), metrics.clone());
            Some(metrics)
        } else {
            None
        };

        let checkpoint_store = Arc::new(DbCheckpointMetadataStore {
            organization_id: self.ctx.config.organization_id.clone(),
            job_id: self.ctx.config.id.clone(),
            state_backend: self.ctx.execution_selector,
            db: self.ctx.db.clone(),
        });
        let (start_epoch, min_epoch) = self.epochs();
        // The same protocol the fan-out addressed this generation under: a commit is a directive
        // from the same controller, under the same authority, to the same generation. Read here
        // rather than carried from the fan-out because the two readings are of the same two
        // values — the job's authority and its scheduling generation — neither of which changes
        // within one attempt; and a job whose fence cannot address its generations has already
        // failed the fan-out, so this is not reachable with an unfenced answer under the fenced
        // protocol.
        let fence_protocol = self.fence_protocol()?;
        self.job_controller = Some(JobController::new(
            checkpoint_store,
            self.ctx.config.clone(),
            self.ctx.pipeline_info.pipeline_id.clone(),
            self.ctx.pipeline_info.state_url.clone(),
            self.ctx.status.generation,
            program,
            start_epoch,
            min_epoch,
            std::mem::take(&mut self.started_connects),
            self.committing_state.take(),
            metrics,
            fence_protocol,
        ));
        Ok(())
    }

    /// Publishes the restored checkpoint's commits.
    ///
    /// These finish a two-phase commit against the job's sinks: they are visible outside the
    /// cluster and cannot be withdrawn, which is the whole reason this is a region of its own.
    pub(crate) async fn publish_restored_commits(&mut self, a: &Admission) {
        info!(
            job_id = %self.ctx.config.id,
            pipeline_id = *self.ctx.pipeline_info.pipeline_id,
            "restored checkpoint was in committing phase, sending commits"
        );
        let controller = self
            .job_controller
            .as_mut()
            .expect("the handover built a job controller before admitting a commit publication");
        a.effect(
            "publish the restored checkpoint's commits",
            controller.send_commit_messages(),
        )
        .await
        .expect("failed to send commit messages");
    }

    /// The transition out of `Scheduling` for an execution that is up.
    ///
    /// Hands the context back with the error, because a phase that cannot leave has to be able
    /// to fence, and fencing needs the context it was holding.
    pub(crate) async fn into_transition(mut self) -> Result<Transition, (Self, StateError)> {
        let Some((id, addr)) = self.leader_endpoint() else {
            self.ctx.job_controller = self.job_controller.take();
            return Ok(Transition::next(Scheduling {}, RunningState {}));
        };

        self.ctx.job_controller = None;
        let connected = LeaderManager::connect(
            JobId(self.ctx.config.id.clone()),
            self.ctx.pipeline_info.pipeline_id.clone(),
            self.ctx.status.generation,
            id,
            addr.clone(),
            config().controller.connect_timeout.as_deref().copied(),
            self.ctx.execution_selector,
        )
        .await;
        let leader_manager = match connected {
            Ok(m) => m,
            Err(e) => {
                let reason = self.retryable("failed to connect to worker leader", e, 10);
                return Err((self, reason));
            }
        };

        self.ctx.leader_manager = Some(leader_manager);
        self.ctx.status.state_context.leader = Some(LeaderContext {
            worker_id: id,
            rpc_address: addr,
            generation: self.ctx.status.generation,
        });
        Ok(Transition::next(
            Scheduling {},
            LeaderRunning {
                started: Instant::now(),
            },
        ))
    }

    /// The epochs the execution starts from.
    ///
    /// The two controller modes read `ignore_state_before_epoch` differently: in controller
    /// mode it is an epoch threshold, and in leader mode the same field is a generation number
    /// that must not affect the checkpoint epoch.
    pub(crate) fn epochs(&self) -> (u64, u64) {
        let default_epoch = if self.leader_mode {
            0
        } else {
            self.ctx
                .config
                .ignore_state_before_epoch
                .filter(|&t| t > 0)
                .map(|t| (t - 1) as u64)
                .unwrap_or(0)
        };
        let start = self
            .checkpoint_info
            .as_ref()
            .map(|i| i.epoch)
            .unwrap_or(default_epoch);
        let min = self
            .checkpoint_info
            .as_ref()
            .map(|i| i.min_epoch)
            .unwrap_or(default_epoch);
        (start, min)
    }
}
