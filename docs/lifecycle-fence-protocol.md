# Lifecycle fence: protocol, schema and D39 conformance

> API/protobuf reference for the durable job-lifecycle fence (M11.T26, design M11.D39d/D39e),
> and the conformance note that says which named D39 invariant each part carries.
>
> The deployment ordering these fields imply is `docs/lifecycle-fence-rollout.md`. Read that
> before upgrading anything.

## 1. Schema — `job_statuses`

Two columns, added by Postgres `V34__add_job_status_lifecycle_fence.sql` and SQLite
`V12__add_job_status_lifecycle_fence.sql`:

| Column | Type | Default | Meaning |
|---|---|---|---|
| `lifecycle_fence` | `BIGINT NOT NULL` | `0` | Monotonic. Incremented by the compare-and-set a controller adopts the job with. `0` means no controller has ever adopted this job, which is why `0` is safe as the wire's "carries no fence" sentinel. |
| `controller_epoch` | `TEXT NOT NULL` | `''` | Minted fresh by each adoption. Distinguishes two adoptions that a lost update could otherwise give the same fence. |

Both are additive with defaults, so a build that does not read them is unaffected and the
migration may be applied while it is still running. The **execution selector stays in
`job_statuses.state_context`** — it did not move — and `state_context` additionally gains an
optional `fencing` record (see §5).

### Adoption

```sql
UPDATE job_statuses
   SET lifecycle_fence = lifecycle_fence + 1,
       controller_epoch = :controller_epoch
 WHERE id = :job_id
   AND lifecycle_fence = :observed_fence
   AND controller_epoch = :observed_epoch;
```

The predicate matches on **both** the fence and the epoch — stronger than design §3C's
illustrative SQL, which matches on the fence alone. Zero updated rows means another controller
has adopted the job since this one read it, and the losing controller stands down rather than
retrying.

### Every subsequent write is conditional

```sql
UPDATE job_statuses
   SET ... WHERE id = :job_id AND lifecycle_fence = :fence AND controller_epoch = :epoch;
```

Status publication, generation publication and authoritative metadata-root installation all use
this predicate. Since M11.T26h there is no unconditional form left: a superseded controller's
write matches no row, which is reported as a typed *stale authority* outcome rather than as an
error, and the controller stops administering the job.

## 2. Protobuf — what was added, and what each zero value means

**No new RPC service, no new message type, and no fence-read RPC.** Every field below is a
defaulted field on a message that already existed, so a peer that does not set it is
indistinguishable from a peer that predates it — which is the property the compatibility window
in `docs/lifecycle-fence-rollout.md` §4 rests on.

| Message | Tag | Field | Type | Zero value means |
|---|---:|---|---|---|
| `RegisterWorkerReq` | 10 | `worker_incarnation` | `uint64` | This worker names no process. A worker id and a generation identify the *slot* a worker runs in and a restart reuses both, so the incarnation is the part of a worker's identity a restart cannot reconstruct; a controller that reads `0` addresses its directives to no incarnation, which is the pre-flag-day shape. |
| `RegisterWorkerResp` | 1 | `requires_lifecycle_fence` | `bool` | This registration does **not** activate strict mode — the pre-flag-day window. It is also the only honest answer a controller that sends no fence could give. |
| `StartExecutionReq` | 13 | `lifecycle_fence` | `uint64` | This request carries no fence. Safe as a sentinel because `0` is never an *adopted* fence (§1). |
| `StartExecutionReq` | 14 | `target_worker_id` | `uint64` | — paired with 15; not a sentinel on its own, because `worker.id` is configuration and `0` is representable there. |
| `StartExecutionReq` | 15 | `target_worker_generation` | `uint64` | **The sentinel for the pair**: this request addresses no generation. `job_statuses.run_id` starts at 0 and the scheduling preamble increments it before launching that generation's workers, so no live generation is 0. |
| `StartExecutionReq` | 16 | `lifecycle_operation` | `LifecycleOperation` | `START` — the only thing `StartExecution` has ever meant. |
| `StartExecutionReq` | 17 | `revoked_execution_ids` | `repeated string` | Revoke nothing. |
| `StartExecutionReq` | 18 | `target_worker_incarnation` | `uint64` | This request addresses no *process* of the addressed generation. Not a sentinel of its own — it is only ever read alongside 13, and an incarnation carried without a fence is refused as malformed. A generation that reports an incarnation refuses a fenced address that does not name exactly its own. |
| `StartExecutionResp` | 1 | `observed_lifecycle_fence` | `uint64` | This generation has acknowledged no fence. |
| `StartExecutionResp` | 2 | `outcome` | `StartExecutionOutcome` | `APPLIED` — which is what an `Ok` `StartExecutionResp` has always meant. |
| `CommitReq` | 3 | `lifecycle_fence` | `uint64` | as on `StartExecutionReq`. |
| `CommitReq` | 4 | `target_worker_id` | `uint64` | as on `StartExecutionReq`. |
| `CommitReq` | 5 | `target_worker_generation` | `uint64` | as on `StartExecutionReq`. |
| `CommitReq` | 6 | `target_worker_incarnation` | `uint64` | as on `StartExecutionReq`. |
| `TaskAssignment` | 7 | `worker_incarnation` | `uint64` | This assignment names no process. Carried because a worker leader issues its generation's commits and has no registration exchange of its own to learn its peers' incarnations from; a leader that reads `0` addresses its commits to no incarnation, and a generation that has one refuses them. |

`CommitResp` gains nothing: M11.P54a's list ends at "commit fence", and D39e(v) settles issued
*start* attempts.

`RegisterWorkerReq` also carries **`reserved 3, 7;`**. Both tags once held
`string` fields (`job_id`, `job_hash`) that were removed without being reserved, so reusing
either would have been wire-indistinguishable from a message an old build sent. `protoc` now
refuses the reuse.

### `LifecycleOperation`

| Value | Number | Applies a program? |
|---|---:|---|
| `LIFECYCLE_OPERATION_START` | 0 | yes |
| `LIFECYCLE_OPERATION_FENCE_ONLY` | 1 | no — advances the addressed generation's acknowledged fence |
| `LIFECYCLE_OPERATION_REVOKE` | 2 | no — advances the fence and makes every named identifier permanently non-applicable |

### `StartExecutionOutcome`

| Value | Number | Meaning |
|---|---:|---|
| `START_EXECUTION_OUTCOME_APPLIED` | 0 | The addressed attempt is applied — accepted now, or already accepted under the same `start_execution_id`. |
| `START_EXECUTION_OUTCOME_FENCE_ACKNOWLEDGED` | 1 | A fence-only operation was acknowledged. Nothing was applied. |
| `START_EXECUTION_OUTCOME_REVOKED` | 2 | The fence was acknowledged and every named identifier is now permanently non-applicable. Nothing was applied. |

Only a *successful* response carries an outcome. A request the worker does not accept is
answered with a gRPC status and no `StartExecutionResp` at all, so no value here means
"rejected"; the status codes remain the sole carrier of that.

### Decoding rules a client must not relax

Both enums are decoded with an explicit `try_from` and an explicit refusal on an unknown value.
Silently defaulting turns a newer controller's `FENCE_ONLY` into a `START` and a newer worker's
outcome into "applied" — the two mis-readings that matter. The lifecycle fields are read as one
*directive* rather than field by field, so a request cannot be half a directive and half
whatever a literal left behind; the same value type is what writes them, and it writes **every**
lifecycle field on every arm.

## 3. Status codes — what settles an issued attempt, and what does not

Exhaustive over all 17 `tonic::Code` variants, with **no catch-all**: a code added by a
dependency upgrade fails to compile rather than inheriting a reading nobody chose.

| Reading | Codes | What a controller does |
|---|---|---|
| **Ambiguous** | `Cancelled`, `Unknown`, `DeadlineExceeded`, `Unavailable` | Nothing is known about whether the request reached the worker. The **same** `start_execution_id` may be re-offered, within the bounded reconcile budget. |
| **Definitive** | every other code, including `Ok`, `Aborted`, `FailedPrecondition`, `ResourceExhausted`, `InvalidArgument`, `Internal` | The worker has answered about this attempt. Re-offering the identifier cannot change it; only a *later scheduling attempt*, under a new generation, may try again. |

`Aborted` is the one code the flag day moved. Before it, `Aborted` meant "the worker's phase lock
was contended" and the fan-out retried the same identifier under the same admission. Since
M11.T26h it is definitive "nothing applied" (D39e(iii)), and the classification no longer takes a
mode at all — there is one taxonomy, and no directive shape can persuade a controller of this
build to read a settled attempt as retriable.

**Every refusal this build's worker gives is definitive.** Putting `ResourceExhausted` or
`FailedPrecondition` into an ambiguous-retry table leaves the worker's own tests green and the
protocol wrong.

## 4. What the worker enforces

Under one non-blocking guard — the same lock the execution phase already lives behind, so there
is no second lock and no validate-then-apply gap:

1. **Registration gates the fenced protocol.** A generation that has not *issued* its
   registration request refuses any *fenced* directive (`FailedPrecondition`) and still admits a
   fence-less one, which is the compatibility window. The gate is the request and not its answer:
   `register_worker` makes a generation schedulable before it replies, so a controller's fence
   handshake legitimately arrives while that reply is still in flight, and a gate on the reply
   would refuse it definitively and fail the scheduling attempt.
2. **Strict mode is monotone.** It is activated by a registration response that requires it, or
   by acknowledging any fenced operation, and it is never turned off.
3. **In strict mode, fence-less is refused.** So is a directive addressed to another worker id or
   another generation (endpoint reuse), and so is one under a fence below the highest this
   generation has acknowledged.
4. **Applied and revoked identifiers are recorded, hard-capped, and never evicted.** The cap is
   derived from the controller's own finite issued-attempt bound rather than chosen; overflow
   fails closed with `ResourceExhausted`.
5. **A contended phase answers `Aborted` having applied nothing** — from `try_lock`, above the
   guard, so no fence was advanced and no identifier recorded.

## 5. The durable `Fencing` record

Carried inside `job_statuses.state_context` under the `fencing` key, versioned and bounded. It
holds every target generation and its state, the `start_execution_id` each was issued, the RPC
address each was reached at, the unrooted candidate key, and when the obligation began.

It deliberately does **not** hold the in-process admission token. There is no field for one and
no code that could write one: a serialized admission would be a right two processes could
present.

An interrupted attempt *records* what it owes; the *next* attempt's preamble discharges it, after
re-adoption has raised the fence high enough to revoke the previous attempt's identifiers.

## 6. Metadata-root candidates

Generation and checkpoint metadata is written first as an immutable, fence-scoped candidate
object:

```
{pipeline}/{job}/generations/{generation}/candidates/fence-{fence:020}-epoch-{epoch}.json
```

written with `put_if_not_exists` (an `AlreadyExists` is success). It becomes authoritative only
when its reference is installed by the conditional row update in §1. A losing controller may
leave an unrooted candidate for the existing grace collector; it cannot replace a committed root.
The key is inside the job's `generations/` prefix so a generation collector reclaims it, and
outside the shape the table-data deletion rules match.

## 7. D39 conformance note

| Design item | What carries it | Where |
|---|---|---|
| **D39a** — one writer decides and publishes; no cross-task gate, mutex or counter | The per-job intent mailbox and the state task's lifecycle actor. The M11.T08 refusal gate was removed by M11.T26h's activation change. | `arroyo-controller` `states/lifecycle/{intent,actor}.rs` |
| **D39b** — typestate `Scheduling`; irreversible effects consume `Admission`; `recv` only on token-free types; interrupted fan-out transfers attempts *and* authority as one unit | The phase graph and its compile-fail fixtures; the per-job settlement owner | `states/scheduling/{phases,fanout}.rs`, `states/lifecycle/settlement.rs` |
| **D39c** — validate-then-act tokens | `Validated<T>` at the five destructive/publishing families | `arroyo-rpc` `state_backend/validated.rs` |
| **D39d** — durable fence carried on the wire and acknowledged at the worker | §1, §2, §4, §5, §6 | migrations, `proto/rpc.proto`, `states/lifecycle/fence.rs`, `arroyo-worker` `lifecycle_fence/` |
| **D39e** — worker start protocol: capability negotiation, registration-gated strict mode, same-guard fence/start serialization, bounded identity, definitive `Aborted`, bounded ambiguous retry | §2, §3, §4 | `arroyo-worker` `lifecycle_fence/{guard,attempt_ids}.rs`, `states/lifecycle/protocol.rs` |
| **D39f** — immutable, fail-closed selector classification | A job's selector is fixed at its first execution; a row that disagrees earns a typed refusal; an undecodable record skips the job | `states/lifecycle/classification.rs` |
| **D39g** — declared fault model, safety over per-job availability | Named injections for loss, duplication, reorder, delay, crash/restart/partition, endpoint reuse and post-flag-day skew, in both controller topologies | `states/lifecycle/faults.rs`, `arroyo-worker` `lifecycle_fence/faults.rs` |
| **D75** — worker-first rollout, one-way flag day | The rollout runbook and the mixed-version harness | `docs/lifecycle-fence-rollout.md`, `arroyo-worker` `lifecycle_fence/rollout_tests.rs` |
| **D96** — every PR-#157 finding mapped to a named invariant and a named executable check; 37 rows green in both topologies | The machine-readable registry and its runner | `scripts/m11-d39-registry.json`, `scripts/m11-d39-matrix.sh` |

### Running the conformance matrix

```bash
bash scripts/m11-d39-matrix.sh
```

It first proves that every registered test name resolves to **exactly one** test across the six
packages, then executes every row once per `ARROYO__JOB_CONTROLLER` value. A row is green only
when both cells pass. Topology-dependent rows additionally assert that the distinct path was
reached. The expected report is **74/74 cells, 37/37 rows**.

### What is deliberately outside the model

Byzantine workers, and an infrastructure layer that falsely reports a generation as terminated.
Bounded final settlement is **not** claimed for an unobservable worker partition: that job stays
in `Fencing` (see the rollout runbook §7). A start authoritatively reported applied before the
fence acknowledgement belongs to the old generation and must be torn down before refusal is
finalized; its pre-finalization non-2PC sink output remains Arroyo's existing at-least-once
exposure.
