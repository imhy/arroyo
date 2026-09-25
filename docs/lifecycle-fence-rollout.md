# Lifecycle fence: rollout and rollback runbook

> Operator documentation for the durable job-lifecycle fence (M11.T26, design M11.D39d/D39e/
> D39g/D75; the already-running worker-leader adoption window in §4 and the §10 activation
> checklist are M11.T27's). It covers the order builds must be deployed in, the one-way flag
> day, what has to be verified before crossing it, what rollback is still allowed afterwards,
> and what M11.T11's activation sign-off records.
>
> Read this before upgrading any cluster past the build that contains it. The ordering rule is
> not advisory: crossing the flag day with a controller that can emit fence-less starts is the
> one mixed-version state the protocol does not tolerate.

## 1. What the fence is, in one paragraph

Every job's `job_statuses` row carries a monotonic `lifecycle_fence` and a `controller_epoch`.
A controller adopts a job by a compare-and-set on both, which raises the fence and installs a
fresh epoch before it causes any effect. Every status, generation and authoritative
metadata-root write is then conditional on `(job id, lifecycle_fence, controller_epoch)`, so a
superseded controller's write matches no row and it stands down instead of overwriting a live
one. The fence and the addressed worker id/generation are carried on `StartExecutionReq` and
on commit directives; a worker generation records the highest fence it has acknowledged,
revokes the identifiers named below it, and admits or refuses a start under the same
non-blocking guard — so a start either linearizes before the acknowledgement and is reported
applied, or after it and is refused as stale. There is no validate-then-apply gap and no
fence-read RPC. A commit directive's fence is only checked against that record: it is never
acknowledged, so no commit raises the fence a generation holds (§4).

## 2. The two builds

| Build | What it does |
|---|---|
| **Fence-capable** | Understands every field in §2 of `docs/lifecycle-fence-protocol.md`. Its worker enforces strict mode when a registration response asks for it; its controller adopts, fences, and publishes conditionally. |
| **Fence-less (legacy)** | Any build older than the one carrying this document. Its `RegisterWorkerResp.requires_lifecycle_fence` is absent (decodes `false`), its starts carry `lifecycle_fence = 0`, `target_worker_generation = 0` and `target_worker_incarnation = 0`, and it neither adopts nor publishes conditionally. |

Every new field is a defaulted field on a message that already existed. There is no new RPC
service, no new message type and no fence-read RPC, so a fence-capable peer and a fence-less
peer can always exchange these messages; what differs is what each of them requires.

## 3. Deployment order — worker-first, always

**Workers are upgraded before controllers. This is a hard requirement, not a preference.**

It predates the fence: a controller of this version refuses to send `StartExecution` to a
worker that has not advertised `RegisterWorkerReq.reconciles_start_execution`, and fails the
scheduling attempt loudly instead. The fence adds a second reason — a fence-capable controller
must never send a *fenced* start to a generation it has not seen register, because a
fence-capable worker refuses that definitively (`FailedPrecondition`).

Only Kubernetes and node deployments are affected: the process and embedded schedulers launch
workers from `current_exe()`, so their workers are by construction the same build as the
controller that launched them.

### 3.1 The sequence

1. **Apply the schema migrations.** Postgres `V34__add_job_status_lifecycle_fence.sql` and
   SQLite `V12__add_job_status_lifecycle_fence.sql` add
   `job_statuses.lifecycle_fence BIGINT NOT NULL DEFAULT 0` and
   `job_statuses.controller_epoch TEXT NOT NULL DEFAULT ''`. Both are additive with defaults; a
   fence-less controller neither reads nor writes them, so the migration is safe to apply while
   the old build is still running and is a no-op for it.
2. **Roll out worker images.** A fence-capable worker registered to a fence-less controller
   receives `requires_lifecycle_fence = false` and stays in the pre-flag-day compatibility
   window: it admits fence-less starts byte-identically to the build it replaced. It is *not*
   in strict mode and does not become so on its own.
3. **Verify the ordering check in §5 passes for every live job.** This is the operator
   precondition for step 4.
4. **Roll out controller images.** The controller in this build selects `FencedV2`, so every
   registration it answers carries `requires_lifecycle_fence = true`. That registration is the
   flag day for the generation that sent it. A generation registers once, from its process's
   startup path, so a generation that registered with a fence-less controller and is still
   running receives no such response, and the flag day is not a handshake with every live
   worker: such a generation enters strict mode only by acknowledging a fenced operation (§4),
   and an already-running worker-leader adoption sends it none.

Steps 1–3 are reversible. Step 4 is the one-way door; see §6.

## 4. The flag day

The flag day is not a timestamp and not a configuration key. It is the moment a worker
generation receives a registration response with `requires_lifecycle_fence = true`, or
acknowledges any fenced operation — whichever happens first. From that moment that generation
is in **strict mode**, and strict mode is monotone: it is never turned off, because a worker
that could leave strict mode could be talked out of it by a stale peer.

A generation in strict mode fails closed on:

- a start carrying no fence (`lifecycle_fence = 0` or `target_worker_generation = 0`);
- a start that reaches it before it has issued its own registration request;
- a start addressed to another worker id or another generation (endpoint reuse);
- a start addressed to another *process* of its own worker id and generation, or to none — a
  restart reuses the id and the generation, so the per-process incarnation the worker reported at
  registration is what tells a successor apart from its predecessor;
- a start under any fence other than the one it has acknowledged — a fenced start is admitted
  only at that exact fence, so no start can raise it;
- a commit under a fence below the one it has acknowledged, or addressed to another worker id,
  generation or process;
- a revocation naming an identifier it has already applied.

Every one of those refusals is **definitive**: `FailedPrecondition`, `InvalidArgument`,
`ResourceExhausted`, `Aborted` or `Internal`. None of them is `Cancelled`, `Unknown`,
`DeadlineExceeded` or `Unavailable`, which are the only four codes a controller may retry the
same attempt identifier under.

A replacement controller that schedules a replacement generation, publishes a refusal, or
discharges the recorded fencing obligation that precedes either does **not** wait for a
worker's next message. It actively advances its fence at every worker generation the previous
scheduling generation could address — re-opening even the targets its durable record already
shows acknowledged, since those acknowledged an earlier fence — and every one of those must
acknowledge before it causes any job effect or publishes a refusal. There is therefore no
post-flag-day first-message gap on those paths.

Adopting an **already-running worker-leader** execution is the one excluded case, and it exists
only in worker-leader mode (`ARROYO__JOB_CONTROLLER=worker`). There, a controller that finds a
`Running` row recovers it into `LeaderRunning` (`StateMachine::start` reads the topology): it
admits no generation, issues no start, and has no worker set to address — it reconnects to the
leader the job's row names. What makes it exclusive is the adoption CAS on the job's row, not a
handshake: a second controller that reads the same row loses that CAS and administers nothing.
Its own discharge of the durable record (`DischargeReason::AdoptingTheGenerationItNames`)
re-opens no target the record shows acknowledged — and the generation it inherits is one — so
that generation is never sent its fence. In controller mode the same row recovers into
`Compiling`, and the `Scheduling` preamble adopts and then advances the adopted fence at every
generation the durable record names — the superseding discharge above — before any effect, so
the window described next never opens there.

**No commit teaches an inherited worker the adopter's fence.** A commit's fence is a guard and
never an instruction: `WorkerLifecycle::admit_commit` takes `&self`, so nothing on the commit
path can raise a floor, and the only fence it refuses is one *below* the floor
(`FenceState::addressed_to_this_generation`), besides a commit addressed to another worker id,
generation or process (protocol §4). For a generation whose acknowledged floor is 5 when the
adoption CAS installs fence 6:

1. a commit under 6 is admitted, and the floor is still 5;
2. a delayed commit under 5 is still admitted;
3. once that generation acknowledges a `FENCE_ONLY` or `REVOKE` at 6, its floor is 6 and the
   delayed commit under 5 is refused (`FailedPrecondition`).

The window ends **only** when that generation acknowledges a `FENCE_ONLY` or `REVOKE` at or
above the adopted fence, or is observed terminated: the worker writes its floor in one place,
when it acknowledges a fenced directive it has admitted, and a terminated generation admits
nothing. A fenced `START` never ends it: the worker admits a start only at the exact fence the
generation has already acknowledged (`acknowledged_this_fence`), and a running generation
refuses every start at its phase check, so no start can raise a floor. The window is also **per
worker generation**, not per job: every `CommitReq` is admitted by the worker it is addressed
to, against that worker's own floor, so after a discharge that reached only some of the
inherited workers, some are closed while others are still open — and
`RunningJobModel::commit_to_workers` stops at the first worker that refuses, after the workers
before it have already published that commit.

What the landed code does inside the window, and no more. The leader keeps committing under
the fence its own start conferred (`RunningJobModel::commit_authority`), and the adopting
controller builds no `JobController` of its own, so no landed path sends an inherited worker a
commit under 6: step 1 above is what a newer commit *would* do to the floor, not a sender that
exists. The worker cannot tell a delayed commit under 5 from the leader's live one — they are
the same `CommitReq` — and a commit it admits is published to its operators. What the adoption
does take away is root authority: the job's committed, restorable metadata root is installed
only by the fence/epoch-conditional `job_statuses` update (protocol §1), the adoption replaced
the `(lifecycle_fence, controller_epoch)` pair that update is conditional on, and so the
superseded controller's installation matches no row. Worker admission is not authority. The
commit rule here is about `CommitReq`: `CheckpointReq.is_commit` reaches the operators'
`ControlMessage::Commit` without the fence guard, but no landed sender sets it (the only setter,
in `job_controller/model.rs`, sets `false`) — pre-existing, and already disclosed by M11.T26.

## 5. The pre-flag-day verification — the operator precondition

Run this before step 4 of §3.1, on the cluster you are about to upgrade. All three checks must
pass **for every live job**; if any of them fails, do not roll out controllers.

1. **Every live worker has registered with the current controller.** Its registration must be
   the one this controller answered, not one inherited from a predecessor process. In the
   controller log, one `registered worker` line per live worker for the current controller
   process.
2. **Every live worker advertises the old reconciliation capability.**
   `RegisterWorkerReq.reconciles_start_execution` is `true`. A worker reporting `false` is
   pre-upgrade: the controller will refuse to schedule to it, so finish step 2 of §3.1 for it
   first. (This is the check the T08 round-15 fix already enforces loudly; it is listed here
   because it is also the precondition for the fence.)
3. **Every live worker runs the fence-capable build.** Confirm it from the deployment itself —
   the image or binary each live worker process was started from — not from anything the worker
   is sent: every live worker generation must have been started from the build step 2 of §3.1
   rolled out. A worker still on a fence-less build must be replaced before step 4. This check
   changes nothing on the cluster, which is what keeps steps 1–3 reversible.

   The fence handshake itself is **not** an operator step. It is performed by the fence-capable
   controller after step 4, on the paths §4 names, and the controller fails closed on it: a
   worker that answers its `FENCE_ONLY` with `APPLIED` — what a worker predating the operation
   answers — is refused loudly (`NotAnAcknowledgement`), so a scheduling attempt stops before
   any start is issued and a discharge leaves that target pending. That is a backstop and not a
   substitute for this check: a worker predating the operation reads the `FENCE_ONLY` as an
   ordinary start before it answers, which is why worker-first order and this check exist.

> **Never send a `FENCE_ONLY` (or any fenced directive) to a live worker while a fence-less
> controller still administers its job.** It cannot be addressed correctly — a fenced directive
> must name the worker process's incarnation (protocol §2), which the worker reports only in its
> registration request and which a fence-less controller neither records nor exposes — so a
> fence-capable worker refuses it (`FailedPrecondition`), which says nothing about whether the
> worker is fence-capable. And a directive it did admit would be worse: acknowledging a fenced
> operation puts the generation into strict mode (§4), after which it refuses every fence-less
> commit and start the fence-less controller can send, so the job it is running can no longer
> commit — and strict mode is never turned off, which is the state §6.1's unconditional rollback
> assumes no generation is in.

> **This runbook does not automate these checks against a live cluster.** The evidence recorded
> for M11.T26h is an in-process mixed-version harness — see §9 — which exercises all four
> version-skew combinations against real worker servers on loopback sockets. Running the three
> checks above against the real cluster is an **operator precondition** of the flag day and is
> not substituted for by that harness. (Check 3 replaced, in M11.T27 by owner ruling
> 2026-09-25, an earlier live `FENCE_ONLY` handshake that the warning above explains could
> neither succeed nor be safe.)

## 6. Rollback — one-way after the first strict-mode generation

The rule has two halves and they are not symmetric.

### 6.1 Before the flag day

While every controller still selects the fence-less mechanism and no generation has entered
strict mode, **rollback to the previous release (M11.T25) is allowed and unconditional.** The
migrations stay applied — their columns are defaulted and unread by that build — and no worker
is in strict mode, so nothing has to be undone. Roll controllers back first, then workers, or
leave the workers where they are.

### 6.2 After any generation enters strict mode

**Rollback is only to a fence-capable build, or through a documented coordinated stop. Never
to a controller that can emit fence-less starts.**

The reason is the monotonicity in §4. A worker generation in strict mode refuses a fence-less
start forever. A fence-less controller can emit nothing else. So rolling the controller back
while such a generation is live produces a job that can never be started, rescheduled or
refused by that controller, and whose fence no controller advances — and it does so silently,
one `FailedPrecondition` per attempt.

Two rollbacks are safe:

- **To another fence-capable build.** The fence, the epoch and the durable `Fencing` record are
  all in `job_statuses`, so a fence-capable predecessor reads them and continues.
- **Through a coordinated stop.** Stop every job (`StopMode::checkpoint` where you want the
  final checkpoint), wait for every job to reach a terminal state, confirm every worker
  generation has terminated, and only then roll the controller back. A generation that no
  longer exists cannot refuse anything. Restarting those jobs under the fence-less build starts
  new generations, which are not in strict mode.

There is no third option. In particular:

- **Do not** clear `job_statuses.lifecycle_fence` or `controller_epoch` to "un-fence" a job.
  The fence a worker generation has acknowledged lives in that worker's memory, not in the row;
  lowering the row's fence makes the controller's own next adoption present a fence the worker
  has already superseded, which the worker refuses. It also destroys the only record that
  distinguishes a live controller from a superseded one.
- **Do not** restart workers to clear strict mode while the fence-less controller is live. It
  works — a new generation is not in strict mode — but the window between the restart and the
  registration is exactly the mixed-version state the flag day exists to close, and a
  fence-capable controller that is still up will re-fence the new generation.

## 7. What "stuck in Fencing" means, and what to do about it

Safety wins over per-job availability. If a target worker generation is partitioned and neither
a fence acknowledgement nor an authoritative termination can be observed, that job stays in
token-free `Fencing` and `Refused` is **not** published. It has no timeout, deliberately: a
timeout would be the controller inferring settlement it cannot observe, which is the one thing
the protocol never does.

Only that job is affected. Other jobs continue to be polled, scheduled, checkpointed and
stopped.

### 7.1 Metrics

| Metric | Labels | Meaning |
|---|---|---|
| `arroyo_controller_job_fencing_age_seconds` | `job_id` | How long this job has been fencing, measured from the durable fencing origin — so it survives controller restarts and is not this process's uptime. Absent (not zero) when the origin is unknown or the clock went backwards. |
| `arroyo_controller_job_fencing_pending_targets` | `job_id` | Target worker generations that have neither acknowledged nor been observed terminated. |
| `arroyo_controller_job_fencing_outstanding_attempts` | `job_id` | Issued `start_execution_id`s with no authoritative outcome. |
| `arroyo_controller_job_fencing_settlements_total` | `job_id`, `accounted_by` | Targets settled, by which observed fact settled them: `authoritative_response`, `acknowledged_fence`, `terminated_generation`. |
| `arroyo_controller_job_fencing_errors_total` | `job_id`, `kind` | `unrecordable`, `not_acknowledged`, `termination_unobservable`, `publication_failed`. |
| `arroyo_controller_job_fencing_alert` | `job_id` | `1` while the job is held by a target generation that cannot be observed; `0` otherwise. |

### 7.2 The alert

`arroyo_controller_job_fencing_alert == 1` sustained past your scheduling timeout means a job is
held by a generation the controller can neither reach nor see terminated. Suggested alert:

```
arroyo_controller_job_fencing_alert == 1
  for: 15m
```

Operator response, in order:

1. **Read `arroyo_controller_job_fencing_pending_targets` and the controller log.** Each pending
   target is logged with its worker id, generation and the RPC address it was reached at.
2. **Establish whether that generation is alive.** If it is reachable again, the controller's
   next pass advances and acknowledges the fence on its own and the job leaves `Fencing`; no
   operator action is needed.
3. **If it is genuinely gone, make its termination observable.** With the process, node or
   embedded scheduler the controller observes termination itself. **With Kubernetes and the
   manual scheduler it cannot**: the pod listing maps every pod to worker id 1, so the
   controller declares generation-termination reporting *untracked* and fails closed rather
   than guessing. For those two schedulers, a recovered obligation is discharged only by
   acknowledgement. Deleting the pod does not settle the target — it removes the only party
   that could acknowledge.
4. **The safe manual resolution is a coordinated stop of that job**, exactly as in §6.2: stop
   the job, confirm every one of its worker generations has terminated, and restart it. The new
   attempt re-adopts, which raises the fence above everything the old generation could have
   been issued.

Never resolve this by editing `lifecycle_fence`, `controller_epoch` or `state_context` by hand.

### 7.3 The capacity bound, and the one configuration that can hit it

The durable `Fencing` record names at most **2048** target worker generations for one job. That
is 32 admitted workers per job × 2 addressable generations × 32 headroom. The first two numbers
are derived — 32 is the shipped default `max_parallelism`, and two generations is what a
controller takeover has to fence at once — but the **headroom factor is stated, not derived**,
because `max_parallelism` is per-organization configuration and arroyo's own cloud profile sets
it to `u32::MAX`. There is no compile-time worker count to derive a bound from.

A job that would exceed it **fails closed**: the obligation cannot be described durably, the
controller records `arroyo_controller_job_fencing_errors_total{kind="unrecordable"}` and the job
stays in *in-memory* `Fencing` — safe, because nothing is published behind an unsettled
attempt, but not recoverable across a controller restart the way a written record is.

**If you run a job at more than 1024 workers**, watch
`arroyo_controller_job_fencing_errors_total{kind="unrecordable"}`. A non-zero count means the
bound was reached, and the fix is a code change to `MAX_FENCE_TARGETS` in
`crates/arroyo-rpc/src/fencing.rs` — not a configuration change. There is no runtime knob, and
that is deliberate: a bound an operator can raise is a bound a corrupt record can raise.

## 8. Failure modes this rollout is designed against (M11.D39g)

Covered by the declared fault model, with a named injection test for each in both controller
topologies: message loss, duplication, reorder and arbitrary in-transit delay; worker
crash/restart and partition; controller crash/restart at any point (before adoption,
mid-preamble, mid-fan-out, mid-commit); endpoint reuse by a new worker generation; and
post-flag-day version skew.

Outside the model, and stated rather than mitigated: Byzantine workers, and an infrastructure
layer that falsely reports a generation as terminated. A start that had already been
authoritatively reported applied before the fence acknowledgement belongs to the old generation
and must be torn down before refusal is finalized; its pre-finalization non-2PC sink output
remains Arroyo's existing at-least-once exposure.

## 9. The evidence recorded for this rollout

**Locally simulated, not measured on a cluster.** The rollout evidence for M11.T26h is:

- an **in-process mixed-version harness**
  (`crates/arroyo-worker/src/lifecycle_fence/rollout_tests.rs`) that drives the real worker
  `StartExecution` handler with real encoded/decoded requests stamped by the shared wire writer,
  across the four version-skew combinations: fence-less controller × fence-capable worker,
  fence-capable controller × fence-less worker, registration-before-admission ordering, and
  post-flag-day skew. A pre-flag-day *worker* cannot be instantiated by this build, so that side
  is decided by byte comparison against the shape such a worker has always received; the
  harness says so in its own documentation;
- the 37-row × 2-topology D96 matrix (`scripts/m11-d39-matrix.sh`), which must report 74/74
  cells and 37/37 rows green. It is M11.T26's record and stays exactly that: its registry and
  runner are not edited, and it is not rewritten to include later rows;
- the unchanged parquet/default regression suites.

M11.T27 adds one supplemental D96 row for the §4 already-running worker-leader adoption
window, row 38, in both topologies, and it is topology-dependent: its controller half asserts
that the window opens under a worker leader and never opens in controller mode. Its registry
and runner are separate (`scripts/m11-t27-supplement-registry.json`,
`scripts/m11-t27-supplement.sh`), and the runner refuses to run unless T26's registry and runner
are byte-identical to their blobs at the registry's `base_pin`. Row 38 holds eight tests:

- in `arroyo-worker`, `job_controller::adoption_window_tests::` —
  `already_running_adoption_commit_does_not_advance_worker_fence` (the named check: floor 5,
  adopted 6, the §4 sequence), `a_revoke_at_the_adopted_fence_closes_the_adoption_window`,
  `a_start_at_the_adopted_fence_does_not_close_the_adoption_window`,
  `only_a_fence_above_the_stale_one_closes_the_adoption_window` and
  `only_an_acknowledgement_teaches_a_running_generation_the_adopted_fence`;
- in `arroyo-controller`, `states::lifecycle::adoption_window_tests::` —
  `an_already_running_adoption_leaves_the_old_generation_unfenced_only_under_a_worker_leader`
  (the per-topology route),
  `an_observed_termination_settles_the_generation_an_already_running_adoption_inherits` and
  `the_controller_an_already_running_adoption_superseded_cannot_install_its_root`.

The current M11 lifecycle sign-off is the two together — **38 checks / 76 topology-specific
results** — produced by

```bash
bash scripts/m11-t27-supplement.sh --with-aggregate
```

which runs T26's runner unchanged first (74/74 cells, 37/37 rows), then row 38 in both
topologies (2/2 cells, report `target/m11-t27-supplement-report.json`), and writes
`target/m11-lifecycle-signoff.json` with the two halves' counts kept apart and their sum as
`current`. It refuses the sign-off, naming the failed half, unless both halves are green. Like
the T26 matrix, row 38 is a local run of in-process tests; it is not cluster evidence either.

No GitHub Actions run is claimed: Actions has been disabled at the repository level since
2026-06-03 (M11.P94a). Every result is local.

## 10. Activation checklist for M11.T11

M11.T11 is the task that installs the state backend providers in every process role, and so
the first build in which a job can run on a backend other than parquet. Its plan,
`plans/M11/T11-runtime-dispatch.md`, and the design items this checklist cross-references —
M11.D39f (the three operations below) and M11.D75 (worker-first rollout and the one-way flag
day), in `plans/M11-arroyo-integration-design.md` — live in the **statebackend** repository,
not in this tree. T11's activation sign-off records every item below.

### 10.1 Three operations that must never be conflated

They are separate operations with separate rules. None is a substitute for another, and none
of them is a migration between backends.

1. **New-job backend choice.** A job that has never executed takes the backend its
   configuration names, and that is the only moment a job chooses one. It may select any
   backend *installed* in every process role; an empty or default selector is parquet. A
   backend with no installed provider is never answered with parquet: the provider lookup
   fails with `LookupError::NoProvider` (`docs/state-backend-provider-seam.md` §2).
2. **Existing-job recovery.** A job that has executed recovers only under the immutable
   selector in its durable execution record (`job_statuses.state_context`), in both controller
   topologies; the editable configuration row is never re-read as its backend. Changing an
   executing `stateengine` job's configuration to `parquet` is **refused** with a typed
   `JobSelectorChanged` naming both values, and the job goes on running under `stateengine` (a
   stop carried by the same row is still honoured). That is not a rollback. There is no
   cross-backend migration: nothing in this build moves a job's state between backends.
3. **Binary rollback.** It changes which build runs. It is not a backend change and is not a
   substitute for one: confirm before rolling back that the target build installs every
   backend a live job's durable record names. Before the flag day, rollback is unconditional
   (§6.1). After any generation has entered strict mode, it is only to a fence-capable build or
   through a coordinated stop (§6.2) — never to a controller that can emit fence-less starts.

### 10.2 Pre-activation checks

1. **Worker-first order** (§3): the schema migrations applied, then every worker image rolled
   out, before any controller image.
2. **The §5 operator precondition**: all three §5 checks pass for every live job, on the
   cluster being upgraded, before controllers roll out. They are the operator's pre-flag-day
   checks, not a controller handshake: a running generation receives no registration response
   from the new controllers (§3.1 step 4), and an already-running worker-leader adoption sends
   the generation it inherits nothing (§4).
3. **The §4 exception understood**: which controller topology the cluster runs
   (`ARROYO__JOB_CONTROLLER`), and, under a worker leader, that an already-running adoption
   leaves each inherited generation admitting commits under its old fence until that
   generation acknowledges a `FENCE_ONLY` or `REVOKE` at or above the adopted fence or is
   observed terminated — a fenced start does not end it, and worker admission is not root
   authority.
4. **The lifecycle sign-off green at the pin being activated**:
   `bash scripts/m11-t27-supplement.sh --with-aggregate` exits zero on exactly the Arroyo
   revision being activated, and `target/m11-lifecycle-signoff.json` records that revision as
   `arroyo_head`, `uncommitted_tracked_changes` as 0, the immutable T26 half at 37 rows / 74
   results, and `current` at 38 checks / 76 results.
