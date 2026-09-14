# The state backend provider seam

> Reference for the seam that lets a job's operator state live in something other than
> arroyo's parquet backing store (M11.T09; design M11.D11/M11.D13/M11.D15c/M11.D20).
>
> Read this before implementing a state backend, before adding a runtime site that selects
> one, and before changing anything under `arroyo-state/src/provider/`. §7 is the
> conformance note for M11.D11, M11.D13, M11.D33 and M11.D35.

## 1. What the seam is

Until this landed, "which code serves this table" was a `match` on
`TableConfig.table_type()` at every site that needed it, with the arms naming
`GlobalKeyedTable` and `ExpiringTimeKeyTable` — parquet types. The answer was always
parquet, and there was no seam to hand a different one to.

What you can do now that you could not before:

- **Implement a state backend from outside `arroyo-state`.** `StateBackendProvider` is
  object-safe and names only public types; `crates/arroyo-state/tests/provider_seam.rs` is
  such an implementation, compiled the way another crate compiles it.
- **Own an expiring-time-key view whose type `arroyo-state` does not name**, and **restore
  a global key/value table from bytes that are not parquet** — the first through a cached
  trait object whose lifetime is not tied to the `Arc<dyn ErasedTable>` it came from, the
  second through a bounded-page loader that sits below `GlobalKeyedView<K, V>`.
- **Let leader garbage collection read your checkpoint payloads** without
  `arroyo-state-protocol` learning your format.
- **Take the concrete `object_store` client out of a `StorageProvider`** instead of an
  erased `Arc<dyn ObjectStore>`, so you can establish what the store physically supports.

None of this ships a second backend. Parquet is still the only provider registered in any
process; see §4.3 and §6.

## 2. How a table operation reaches a backend

```text
  job's StateBackendSelector           table config's TableEnum
  (arroyo_types::TaskInfo)                        |
              |                        TableKind::of_config()  -- refuses MissingTableType
              v                                   v
        +-----------------------------------------------+
        |  provider::registry()  ->  &ProviderRegistry   |   write-once process cell
        +-----------------------------------------------+
              |  .provider(selector, kind)             -> &dyn StateBackendProvider
              |  .global_key_value_provider(selector)  -> &dyn GlobalKeyValueProvider
              |  .expiring_time_key_provider(selector) -> &dyn ExpiringTimeKeyProvider
              v
        the backend's own value: Arc<dyn ErasedTable>,
        Box<dyn ExpiringTimeKeyViewApi + Send>, or
        Box<dyn GlobalKeyValueLoad + Send>
```

The selector is `arroyo_rpc::state_backend::StateBackendSelector` — the typed, normalized
value M11.T08 persists — and it comes from the *job*, not from the table config's copy of
it; the two were already proven equal at the acquisition boundary. `TableKind` is the
registry's live subset of `TableEnum`, with no variant for `MissingTableType`, so a config
that names no kind cannot become a key.

**A lookup that misses is `LookupError::NoProvider`. It is never answered with parquet.**

Every lookup is a shared-reference read of an immutable value — no lock, no lazy
initialisation, no allocation — and none is on the record path:

| Call site | Frequency |
|---|---|
| `TableManager::load` | once per table, per subtask, when the subtask's state is built |
| `TableManager::get_expiring_time_key_table` | once per table name, per subtask — only on the call that finds the view cache empty |
| `TableManager::get_global_keyed_state` and `…_migratable` | as above |
| the worker's leader cleanup, through `ProviderLiveness` | once per live kind when the resolver is built, then once per table of each manifest the pass reads |

## 3. The storage handoff

A backend that talks to object storage itself takes two things from the `StorageProvider`
it was handed, and must take both from the same one:

| Call | Gives you |
|---|---|
| `StorageProvider::backing_handle()` | `BackingStoreHandle` — the concrete client: `AmazonS3`, `GoogleCloudStorage`, `MicrosoftAzure`, or `LocalFileSystem` |
| `StorageProvider::configured_prefix()` | the `Path` every object under this provider is namespaced by |

`get_backing_store()` returns `Arc<dyn ObjectStore>`, and a consumer deciding whether it
can issue a native ranged GET or an atomically publishing multipart cannot recover that
from an erased handle — nor take the store's word for it, since a store supplies its own
`Display`. The concrete type is the witness that is not forgeable.

- **R2 has no variant.** `construct_r2` builds an `AmazonS3` against a Cloudflare endpoint,
  so an R2 provider's backing store *is* an `AmazonS3`. The one way R2 differs
  operationally is already published by `StorageProvider::requires_same_part_sizes()`.
- **The typed and erased handles are one allocation.** A private
  `StorageProvider::with_backing` is the only place a `StorageProvider` value is
  constructed, and it derives `object_store` and `multipart_store` from the handle;
  `every_backend_hands_all_three_views_one_allocation` compares the addresses.
- **`configured_prefix()` is the one supported prefix spelling.** It is *defined* as
  `qualify_path(&Path::default())`, so it cannot disagree with the qualification the
  provider's own operations apply. `StorageProvider::get_key` is not an alternative: it
  re-parses a URL with `with_key = true`, which disagrees with `for_url` for `file://`
  URLs and fails outright for a URL with no key.
  `arroyo-connectors/src/filesystem/sink/delta.rs` was swept onto `configured_prefix()`.

## 4. Adding a backend

### 4.1 The traits

`StateBackendProvider` carries the families **both** live kinds have. One provider serves
one `(selector, kind)` pair; the registry hands out `&dyn StateBackendProvider` and several
threads may call one concurrently, so an implementation must hold no per-table or per-job
state — each method takes everything it needs:

| Method | Answers |
|---|---|
| `selector` / `table_kind` | the key this provider is registered under |
| `table` | build one subtask's table, restoring a checkpoint if there is one |
| `merge_checkpoint_metadata` | fold every subtask's report of one table into the table's checkpoint metadata |
| `committing_data` | the per-subtask committing data one table's checkpoint metadata carries |
| `files_to_keep` | the file *set* a `ValidatedTable` references |
| `table_data_files` | the ordered file *list* a `TableCheckpointMetadata` names (§5.3) |
| `compact_data` | compact one table's checkpoint metadata |

The two families only one kind has are **sub-traits**, not methods with a wrong-kind arm:
`ExpiringTimeKeyProvider` adds `expiring_time_key_view`, `GlobalKeyValueProvider` adds
`global_key_value_load`. The registry slot for each kind holds that kind's sub-trait, so
the provider a lookup returns already has the method and the provider for the other kind
does not have it to be called by mistake. `ProviderRegistry::provider` upcasts out of a
slot for the shared families.

### 4.2 The registry entry

A backend enters a built registry **only as a complete pair of kinds**:

```rust
ProviderRegistry::builder()
    .register_global_key_value(Arc::new(MyGlobalProvider))?
    .register_expiring_keyed_time(Arc::new(MyExpiringProvider))?
    .build()?           // RegistryError::IncompleteSelector if either is missing
```

Three refusals, all at build time rather than at lookup:

- `RegistryError::KindMismatch` — the provider's own `table_kind()` disagrees with the slot
  it was registered into.
- `RegistryError::DuplicateRegistration` — two providers claim one key. There is no "the
  same provider twice is fine" exemption.
- `RegistryError::IncompleteSelector` — one kind registered, the other not. A job's tables
  are not split across backends, and the alternative to refusing here is falling back to
  parquet for the other kind.

Because the builder refuses that last state, `LookupError` has no "registered for the other
kind" variant: a miss means the backend itself was never registered.

### 4.3 Where `install` must be called

`provider::install(registry)` writes a single process cell. `provider::registry()` reads
it, and **initialises it to `ProviderRegistry::parquet_default()` if nothing was
installed** — which is why the ordering is load-bearing: the first `registry()` call
*fixes* the process registry rather than merely reading early.

- Call `install` **before anything constructs a table or opens a view** — before any
  operator context is built, and before any checkpoint is loaded.
- A second `install` is `InstallError::AlreadyInstalled`.
- An `install` after any lookup is `InstallError::DefaultedByFirstUse`. The fix is to
  install earlier, not to install again.

In both cases the registry already in force is unchanged and still serving and the argument
is dropped: the error says this call had no effect, not that the process is inconsistent.
**There is no reset, under `cfg(test)` or otherwise** — tests build `ProviderRegistry`
values and call them directly. `tests/provider_install_once.rs` and
`tests/provider_install_after_first_use.rs` are one-test-per-binary suites for this reason.

## 5. The three contracts an implementer must honour

### 5.1 The one-batch drain (`ExpiringTimeKeyViewApi`)

Reading a view is a drain, not a collect. `begin_batch_drain(watermark)` mints a
`BatchDrainToken` and fixes what the drain covers; `next_drained_batch(token)` returns **at
most one** owned `(SystemTime, RecordBatch)`. `all_batches_for_watermark` is not on that
trait at all: it is the single blanket implementation on the sealed extension trait
`ExpiringTimeKeyViewDrain`, covering every `T: ExpiringTimeKeyViewApi + ?Sized`, so
coherence forbids a view from supplying its own. One poll of the stream performs exactly
one `next_drained_batch` and the stream never holds more than the batch it is about to
yield — for every view, not only the ones that declined to override it.

The one-batch step is the primitive rather than the stream because a caller may need its
own `&mut` state between batches: `InstantJoin::restore_side` replays each restored batch
back through the operator's insert path, so it cannot hold a borrow of the view across the
call. Its whole-table `collect()` is gone.

`BatchDrainToken::mint()` is the only public constructor and draws from one process-wide
monotonic counter, so no value a caller builds is equal to a token a view is holding.
**What an implementation owes is not enforced by these types**: store the token you minted
and compare by equality, capture the drain's range beside it so validating the token
validates the range, and fail closed when the view is mutated inside the part of the range
still to come. `parquet_view.rs` does all three; `expiring_time_key_view::tests::tokens` is
the matrix.

### 5.2 The two-unit page bound (`GlobalKeyValueLoad`)

A loader yields `GlobalKeyValuePage`s of opaque `(key bytes, value bytes)`. The bound has
**two units**, because neither alone is a memory bound — the count alone leaves one page of
hundred-megabyte values unbounded in bytes, the payload alone leaves a million one-byte
entries costing tens of megabytes of `Vec` headers:

| Constant | Value | Binds when |
|---|---|---|
| `MAX_LOAD_PAGE_ENTRIES` | 1024 | entries are small (below ~1 KiB each) |
| `MAX_LOAD_PAGE_BYTES` | 1 MiB | entries are large |

Structural:

- `GlobalKeyValuePageBuilder` is the **only** constructor of a `GlobalKeyValuePage`.
- `LoadPageLimits::DEFAULT` and `LoadPageLimits::new` are the only ways to obtain a limits
  value, and `new` returns `None` for zero or for anything **wider** than the seam's own
  maximum — so a loader cannot declare a wide bound in order to pass its own check.
- The decode above the seam re-checks **every** page with `LoadPageLimits::admits` against
  the bound its loader declared, and fails the restore if one is over.
- An entry larger than `max_payload_bytes` is a **page of one**: never split — half a
  key/value pair is not a value — and never dropped. `admits` encodes that exception.

The implementation's obligation, not the type's: pages come in source order, one page
belongs to exactly one source, a source is announced by a page carrying its name and state
version and **no entries** before any of its entries are read, and `next_page` keeps
returning `None` once it has returned `None`. The announcing page is what lets the
migrating decode refuse a source's state version before touching its rows.

### 5.3 The liveness resolver's deletion path (`CheckpointLiveness`)

`arroyo-state-protocol` subtracts the files named by *retained* checkpoints from the files
named by *expiring* ones and deletes the difference, asking one resolver for both. So:

> **Returning fewer names than the payload carries silently marks live files collectable.**
> `Ok(vec![])` is a legitimate answer — an expiring table whose files have all aged out
> names none — which is exactly why it must not double as the failure. Declining is
> `Err(LivenessRefusal)`, and a single refusal fails the whole classification, which is
> strictly before the first delete.

Nothing in these types distinguishes a short-but-successful decode from a correct one. It
is the implementation's obligation and its own tests'; parquet's is pinned by
`provider::tests::liveness::provider_liveness_names_exactly_the_files_the_parquet_payloads_do`
and by `the_ordered_list_and_the_files_to_keep_set_name_the_same_files`. The list is
deliberately ordered and deliberately not a set: it is `files_to_keep`'s answer before
deduplication. Refuse — do not decode — a payload declaring a kind you do not serve
(`LivenessRefusal::WrongTableKind`); `prost` skips unrecognised fields, so another format's
bytes routinely decode into a well-formed message of yours, and the file list that falls
out is short rather than wrong-looking.

The trait is declared by `arroyo-state-protocol` — the crate that does the deleting — and
implemented in `arroyo-state` by `ProviderLiveness`, which routes each manifest entry to
the provider for the kind the entry declares. `cleanup_leader_checkpoints` no longer takes
a selector: it takes `&dyn CheckpointLiveness` and derives the selector from
`state_backend()`, so "whose decoder reads these manifests" and "which backend the job runs
on" cannot be two statements that disagree. `ProviderLiveness::new` additionally refuses a
backend the registry does not serve *before* a pass begins, because a manifest with no
tables asks no questions and would otherwise authorize deleting a history's manifests,
markers and epoch records.

`arroyo-state → arroyo-state-protocol` is the new dependency edge and runs one way only.
The reverse is a cargo-refused package cycle; the one gap cargo leaves — a legal
dev-dependency cycle — is covered by
`crates/arroyo-state-protocol/tests/dependency_direction.rs`.

## 6. Deliberately unchanged, and what M11.T10/M11.T11 still owe

- **Parquet is the sole installed and default provider.** `ProviderRegistry::parquet_default`
  registers one parquet provider per live kind for one selector, and nothing in the tree
  calls `install`.
- **`TableEnum` stays logical.** No backend-specific variants; existing plans and configs
  keep emitting `ExpiringKeyedTimeTable` and keep working. `TableKind` is a separate type.
- **`GlobalKeyedView<K, V>` and the bincode decode are untouched.** The seam is a *loader*
  below them precisely because the view is generic over `K: Key, V: Data` and so is never
  object-safe.
- **The parquet loader still fetches each checkpoint object whole** (`StorageProvider::get`)
  before reading record batches out of it, exactly as `GlobalKeyedTable::load_with_version`
  did. The pages bound what crosses the seam, not the fetch; making it ranged would change
  parquet's I/O pattern, which M11.D33 holds fixed.

**No stateengine provider is installed in any process.** `StateBackendSelector::StateEngine`
parses, persists and transports, and the registry reserves it a slot, but nothing registers
a provider for it, so selecting it yields `LookupError::NoProvider`.

Seven runtime sites still match on `TableEnum` to select a parquet implementation or a
parquet payload format. Each is a per-checkpoint or per-cleanup operation, so converting
them moves no lookup onto the record path:

| Site | Family |
|---|---|
| `crates/arroyo-worker/src/job_controller/checkpoint_state.rs:223` | `merge_checkpoint_metadata` |
| `crates/arroyo-worker/src/job_controller/checkpoint_state.rs:519` | `committing_data`, worker checkpoint controller |
| `crates/arroyo-worker/src/job_controller/controller.rs:932` | `committing_data`, leader manifest path |
| `crates/arroyo-state/src/parquet.rs:605` | `compact_data` |
| `crates/arroyo-state/src/parquet.rs:708` | `files_to_keep` (`ParquetBackend::table_files_to_keep`) |
| `crates/arroyo-state/src/validated/cleanup.rs:387` | checkpoint cleanup's own file-reference classification |
| `crates/arroyo-controller/src/states/scheduling.rs:1070` | the controller's scheduling restore |

M11.D20's family (e) — the backend-wide checkpoint metadata load/write/name calls made
through the `StateBackend` alias — is also untouched: `crates/arroyo-state/src/lib.rs:59`
still reads `pub type StateBackend = parquet::ParquetBackend;`.

## 7. Conformance note

| Design item | What carries it | Evidence |
|---|---|---|
| **M11.D11** — registry seam keyed by backend and logical kind; `TableEnum` stays logical; parquet extracted as the default | `StateBackendProvider` + `ProviderRegistry`, keyed by `(StateBackendSelector, TableKind)`; `ParquetProvider<T>` delegating to the `ErasedTable` associated functions | `provider::tests::{registry, registry_lookup, table_parity, merge_parity, metadata_parity}`; `tests/provider_seam.rs` |
| **M11.D13** — owned `Box<dyn ExpiringTimeKeyViewApi + Send>` cached by `TableManager`; never a borrowed view, never a state-sized owned collection | `TableManager::expiring_views`, a typed map lending `&mut dyn`; the drain primitive and the boxed borrowing `ExpiringTimeKeyBatchStream`; `instant_join`'s `collect()` removed | `tests/expiring_time_key_view_seam.rs`; `expiring_time_key_view::tests::{read, mutation, tokens}` |
| **M11.D15c** — the global-keyed seam is a bounded-page *loader*, decode and view above it | `GlobalKeyValueLoad`, `GlobalKeyValuePage`, `LoadPageLimits`; `GlobalKeyedView<K, V>` and the bincode decode unchanged, in `tables::global_keyed_map::restore` | `global_key_value_load::tests`; `provider::tests::global_load::{parity, bounds, fail_closed}`; `tests/global_key_value_load_seam.rs` |
| **M11.D20** — complete dispatch inventory | families (a) and the GC-liveness half of (c) converted; the rest inventoried by file and line, with the provider method already present for M11.T11 to call | the `provider.rs` module header; §6 of this page |
| **M11.D33** — with `parquet` selected, arroyo behaviour is unchanged | every provider method delegates to the function the `TableEnum` arm already called; `registry()` defaults to parquet-only, so a process that installs nothing is the process arroyo has always been | the parity suites above compare the seam against the legacy static path; the `arroyo-sql-testing` smoke leg is M11.T09u's gate, not this note's |
| **M11.D35** — arroyo pinned; GitNexus impact analysis precedes arroyo-side symbol edits | the tree these changes sit on is exactly the commit `arroyo-pin.txt` records, and the pin advances when they are committed | `arroyo-pin.txt` in the statebackend repo; the impact evidence is M11.T09v's, recorded with the change rather than here |

### Deliberate deviations

1. **`selector()` returns `StateBackendSelector`, not the design sketch's `&'static str`.**
   The typed selector is what M11.T08 persists and transports, and
   `StateBackendSelector::normalize` is the single place an unrecognized backend name
   becomes a typed error. Keying on a free string would stand a second, weaker spelling of
   that value next to the persisted one, and would let a name no backend has become a key.

2. **`begin_batch_drain` / `next_drained_batch` are surface beyond M11.D13's enumeration.**
   D13 names `all_batches_for_watermark` and five other methods. Putting the two-call
   primitive *underneath* the stream is what makes the bound structural rather than
   promised: the stream moved off the view trait onto the sealed extension trait
   `ExpiringTimeKeyViewDrain`, whose one blanket implementation is built from those two
   calls and which coherence keeps unique, so no implementation can buffer more than the
   batch it is yielding, and a caller needing its own `&mut` state between batches can
   drain without holding a borrow of the view. The stream D13 specifies is still there,
   signature unchanged; only the trait that carries it differs, and it is in scope wherever
   the view is.

3. **`GlobalKeyValuePage` carries a source and a state version beyond D15c's literal
   `(Vec<u8>, Vec<u8>)`.** `get_global_keyed_state_migratable` has to decide *per source*
   whether the state is one version behind and must be migrated, or further behind and must
   be refused. The pre-seam walk read that version out of each parquet file's own metadata
   inside the walk; with the walk below the seam, a page of bare byte pairs carries nothing
   a backend-neutral decode could decide from, and the decision cannot move back down
   without teaching every backend what a state version means. The envelope is what keeps
   the decode backend-neutral, and the announcing page is what lets it refuse a version
   *before* the source's entries are read.
