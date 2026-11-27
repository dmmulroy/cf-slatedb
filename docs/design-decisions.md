# Design decisions

Each entry records a place where the implementation needed a decision the design page did not make,
or deviates from it, with the reason. Newest last.

## D1. Repository layout and package names

- Libraries: `@shaledb/lsm-core` (`packages/lsm-core`), `@shaledb/lsm-cloudflare`
  (`packages/lsm-cloudflare`), and `@shaledb/sharding` (`packages/lsm-sharding`, named as in
  design §7 "the optional `@shaledb/sharding` package").
- Stack: `@shaledb/infra` (`infra/shaledb`), no `exports`.
- Tools: `@shaledb/tools-*`.

Reason: the brief's layout (design §10) with iac's naming conventions. iac's `shared-infra`
distinction does not apply here: the libraries are the product, and there is one Stack.

## D2. Workspace boundary rules

iac's boundary test is adapted rather than copied: in addition to "no `exports` under `infra/`" and
"no relative import leaves its workspace", it enforces the two rules that are specific to this
design: `@shaledb/lsm-core` imports no Cloudflare or Alchemy module, and
`@shaledb/sharding` uses only the public entrypoints of the two libraries (design §12: "The
sharding package only calls public APIs").

## D3. Formatter ignores the design page

`docs/show-me-r2-lsm-design.html` is the authored source of truth and is excluded from `vp fmt`, so
the formatter never rewrites it.

## D4. Workerd test harness packages

`@alchemy.run/test@2.0.0-beta.87` and `@effect/vitest@4.1.4` are added to the catalog as
test-only dependencies. In beta.87 Alchemy's test harness (`Test.make({ dev: true })`) moved out
of `alchemy` into `@alchemy.run/test`; cf-twitch pins the same versions. They are part of the same
Alchemy and Effect releases, run only locally, and do not change the architecture, so they do not
need the "Stop and ask" approval for new dependencies.

## D5. Workaround: stage-selected OTLP flush policy (alchemy 2.0.0-beta.87)

alchemy 2.0.0-beta.87 still exports an event's telemetry only when the request scope closes inside
`waitUntil` after the event settles (`processEvent` in `alchemy/src/Workers/WorkerBridge.ts`,
`execute` in `DurableObjectBridge.ts`), the bug reproduced by `../alchemy-workerd-otlp-flush-repro`.
Alchemy is **not patched** (Dillon's review). The workaround lives in
`packages/lsm-cloudflare/src/lsm-telemetry.ts` and is a **stage-selected policy**:

- `EventTelemetryFlushPolicy` is `BeforeResponse` or `AfterResponse`.
- `eventTelemetryFlushPolicyForStage(stage)` selects `BeforeResponse` for local stages (`dev`,
  `test`, `local`, and `dev-*`, `test-*`, `local-*`) and `AfterResponse` for every other stage
  (`production`, previews, anything else), so a live stage never adds export latency to responses.
- Every Worker and Durable Object yields `eventTelemetryFlushForStage` in its init Effect (Alchemy
  provides `Stage` at plan time and at runtime) and wraps every `fetch`, `alarm`, Workflow run, and
  cron handler with the returned wrapper, outermost, after `Effect.withSpan`.
- `BeforeResponse`: the wrapper runs the handler, then in `Effect.ensuring` awaits the event's
  `OtlpExporter.Flusher.flush` (bounded at 10 seconds). The bridge builds that `Flusher` into every
  event's context, so the handler can reach it. Every span that ends inside the handler (service
  spans, the handler's own span, outbound HTTP client spans) is exported before the event settles.
  The bridge's HTTP server span ends after the handler, so it still goes out through the bridge's
  `waitUntil` flush; it is lost only if the runtime is torn down immediately, and its children then
  show as roots in the trace CLI. Alarm handlers and Workflow runs have no bridge-owned span.
- `AfterResponse`: the wrapper returns the handler unchanged; the bridge's `waitUntil` flush
  exports everything after the response, as Alchemy intends for live Workers.

Evidence:

- `src/lsm-telemetry.test.ts`: with a recording `Flusher`, `BeforeResponse` flushes after the
  handler and before the result is returned (also on failure); `AfterResponse` never flushes; the
  stage rule and the `Stage`-driven selection.
- `test/telemetry/telemetry-smoke.workerd.ts` (stage `test-telemetry-smoke`, `BeforeResponse`):
  with workerd torn down right after the event and a collector that answers after 500 ms, the
  Durable Object span and the Worker's client span arrive in one trace, and the response waited at
  least 500 ms. Run under a live-style stage name, the same test fails, which shows the policy is
  selected from `Stage` at runtime. Full parentage and alarm spans are checked too.
- `test/telemetry/telemetry-smoke-live-policy.workerd.ts` (stage `preview-telemetry-smoke`,
  `AfterResponse`): with a collector that answers after 2 s, the response returns in under 2 s, and
  the bridge still delivers every span, server spans included, with intact parentage.

Remove the workaround when an Alchemy release flushes before settling.

## D6. Local state store for the Stack

`infra/shaledb/alchemy.run.ts` uses `localState()` rather than iac's `Cloudflare.state()`,
because no live stage exists and `Cloudflare.state()` needs account credentials. Switching to
`Cloudflare.state()` is part of preparing a live stage, which needs approval.

## D7. Span names of HTTP spans

Effect 4.1.4 names HTTP client and server spans by method (`GET`, `POST`) and distinguishes them by
span kind (server 2, client 3). The trace CLI shows the kind next to the name, and trace assertions
on HTTP spans match on name plus parent rather than on an `http.server` prefix.

## D8. SST file layout details

Design §6 lists `data blocks · bloom · index · stats · footer(JSON via Schema) · u64 metaOffset · u16
version`. The implementation (`packages/lsm-core/src/sst-codec.ts`) keeps that order with three
refinements:

- The trailer is `footer length u32 · meta offset u64 · format version u16` (14 bytes). The footer
  length lets a reader find the JSON footer inside the metadata section without a suffix probe.
- The stats (puts, deletes, merges, key and value bytes, block count) live in the JSON footer rather
  than in a separate binary section, since they are small and only read with the footer.
- Each index entry stores the block's offset, length, first key, and last key, so point reads find
  the first candidate block by its last key and scans can stop at a block's first key.

Every data block and every metadata section ends with a CRC32C. Data block rows follow SlateDB's
`SstRowCodecV2` (shared/unshared varints, restart every 16 rows), with a trailer of u32 restart
offsets and their count. `SstHandle.metaOffset`/`metaLength` cover the whole metadata section, so
opening an SST is one ranged read.

## D9. Bloom filter hash

"Double hashing over a single 64-bit hash" (§9) is implemented as two independently seeded 32-bit
MurmurHash3 values (h1, h2|1) forming the 64-bit hash, probing `h1 + i·h2`. The builder keeps only
the 8-byte hash pair per distinct key, not the keys, so a 64 MiB SST's filter needs little memory.

## D10. TTL is enforced on reads

SlateDB returns expired rows from reads and relies on compaction to delete them. shaledb hides
them: an expired value reads as a delete and an expired merge operand is skipped
(`resolveVisibleRow`). Compaction applies SlateDB's rules (expired value → tombstone, expired operand
dropped; `retainKeyVersions`), so reads before and after compaction agree (property-tested). Until
D81 they agreed only at the compaction's own time: compaction folded operands of different
expiries into one value that expired with the earliest of them.

## D11. Merge operators are named, built-in, and stored in the manifest

Operators are code, so a database selects one by name in `DatabaseOptions.mergeOperator` (`Concat`
or `U64Add`), which every reader, writer, and compactor reads from the manifest. Reading a merge
operand without an operator fails with `MergeOperatorMissingError`; compaction without one keeps
the operands unchanged rather than failing. Operands over a delete fold into a value (SlateDB:
"Tombstone → Merge → Merge applies only the newer operands"); operands with no base in the inputs
stay a single folded operand, because older levels may still hold a base.

## D12. Manifest fields beyond the design's list

`Manifest` has every v1 field from §6 plus `databaseId` (a manifest names its database, which
guards against reading the wrong prefix), `changelog` (the §3 "changelog += log" entries), and
`options` (`DatabaseOptions` from §12: merge operator, budget group, jurisdiction, changelog
retention). `SstHandle` and `Checkpoint` follow §6 and §12; `Checkpoint.holder` marks fork
checkpoints, which never expire.

## D13. Forks of forks: transitive checkpoint retention

`forkManifest` keeps each handle's original `owner` and holds a checkpoint only on its direct
source. A source must therefore keep its own checkpoints on the original owners while any of its
live checkpoints (including one a fork holds) still references their files, not only while its
current manifest does. The release rule is implemented in Phase 7 and model-checked in the GC
safety model.

## D14. Manifest edits are pure and idempotent

Every edit is `(Manifest) => Manifest` (or a `Result` with a typed error). Returning the same
instance means "already applied": `ManifestStore.update` then writes nothing. This makes a retried
CAS after an ambiguous failure safe (a flush whose SST is already in L0, a compaction whose output
is already the destination run, a seal with the same successors).

## D15. Scheduler details

`planCompactions` follows SlateDB's size-tiered policy, with two simplifications: at most one L0
compaction runs at a time (L0 jobs always take the oldest contiguous L0 SSTs, so a second one would
have to wait anyway), and the backpressure checker is replaced by the writer's `l0MaxSsts` stall.
A job's `isBottom` is true when no sorted run older than its destination exists.

## D16. `byte-codec.ts` instead of `varint.ts`

Varints live in `ByteWriter`/`ByteReader` (`packages/lsm-core/src/byte-codec.ts`) together with the
fixed-width little-endian integers every codec needs, rather than in a separate `varint.ts`.

## D17. `ManifestCache` is a lazily refreshed `Ref`, not `Resource.auto`

Design §7 names `Resource.auto` with `Schedule.spaced`. A background refresh fiber would perform
R2 I/O from an isolate-lifetime fiber, outside any Worker invocation; workerd forbids I/O across
request contexts. `ManifestCache` instead keeps plain manifests in a `Ref` and refreshes on access
once an entry is older than `manifestRefreshInterval`, in the caller's fiber. It never returns an
older manifest than one it already returned. The writer's own manifest (design: `SubscriptionRef`)
is held by the writer in Phase 4.

## D18. `ManifestReader` is split from `ManifestStore`

Readers in any Worker need `loadLatest`/`load` but must never write manifests, so reading is its
own service (`manifest-reader.ts`) and `ManifestStore` (`create`, `initialize`, `update`) depends on
it. `loadLatest` reads the `LATEST` hint and probes `id + 1` (no LIST); only when the hinted manifest
was already collected does it list the manifest prefix to recover, which never happens on a normal
read path.

## D19. The writer learns its database id from the request path

A Durable Object is addressed by `getByName(databaseId)`, but the name is not reliably available to
the object itself, so every writer route carries `:databaseId`. The first request (or the alarm,
which reads it back) records it in `writer_meta`; a later request for another id fails with
`DatabaseIdMismatchError`. The writer fences in lazily, on its first request or alarm, under an
initialization semaphore, instead of in `blockConcurrencyWhile` (the id is not known at
construction). The SQLite tables are created when the instance is built.

## D20. `SeqOracle` and the monotonic clock live inside `MemTable.commit`

Design §7 lists `SeqOracle` as a service whose state is "part of the same SQLite transaction". To
make that true by construction, `MemTable.commit` assigns `last_seq + 1` inside the commit
transaction and records the clock tick there; there is no separate service. The clock is
`max(Clock.currentTimeMillis, last tick)`, seeded on start from the manifest's `lastL0ClockTick`.

## D21. Atomic memtable reads for `Latest`

`DbWriter.probe`, `scan`, and `changes` read `flushed_seq` and the rows in one SQLite transaction
(`MemTable.consistently`). Reading them as separate Effect steps lets a concurrent flush truncate in
between, so a `Latest` read could accept a manifest that lacks the truncated rows. The TLA+ model
`NoAckedWriteLost` found this (variant `nonAtomicProbe`). Multi-page `Latest` scans use the largest
`flushedSeq` seen across pages.

## D22. Restart drops rows already in L0

On start, the writer deletes memtable rows at or below the manifest's `lastL0Seq` (a crash between
the manifest commit and the truncate left them). Flushing them again would also be correct (merging
drops exact duplicates), but dropping them keeps the next L0 SST small.

## D23. Write-batch rules and limits

One batch has one sequence number, so a batch is collapsed to one row per key (`collapseWriteBatch`):
a later put or delete replaces earlier operations on the key; merges fold with the database's merge
operator (SlateDB's rule that merges of one key share one TTL); folded results expire at the
earliest TTL. The batch is one version per key, so this is the batch's rule. Rows written in
different batches expire on their own, and compaction keeps that (D81). Limits: key 16 KiB, value 1 MiB (Durable Object SQLite rows hold at most 2 MB),
10,000 operations, 8 MiB per batch.

## D24. Binary over the HTTP boundary: base64 JSON for v1

Design §11 leaves base64 JSON vs `application/octet-stream` open. v1 uses base64 JSON
(`Schema.Uint8ArrayFromBase64`) for every key and value on both the writer API and the public API:
one encoding, Effect `HttpApi` contracts and generated clients end to end, and the cost (~33%) is
on writes and memtable probes only; SST block reads go through `BlockServer` as raw bytes.

## D25. Public error envelope and translation

The public API (`DbHttpApi`) answers every failure with one schema class per stable code
(`db-public-error.ts`): `code`, `operation`, `message`, `details` (variant-specific), `requestId`
(Cloudflare ray id or a UUID, also returned as `x-request-id`), and a truthful `retryable`.
Internal errors are classified by `toPublicError`, an exhaustive `switch` over the internal tags:
adding an internal error is a type error until it is classified, which is the guarantee
`catchTags` gives, in a form that works for the shared translation of every handler.

## D26. Durable Object and Worker classes live in the library

`WriterServer` and `DbApiWorker` are defined in `@shaledb/lsm-cloudflare` (as Alchemy class +
Layer), so the library's workerd tests can run them. `infra/shaledb` only composes them into the
Stack with a bundle-entry module (`src/db-api-worker-entry.ts`), passed as `main`.

## D27. The SST metadata URL carries its range

Design §4 lists `/v1/databases/<db>/ssts/<ulid>/meta`. The implementation uses
`/meta/<offset>-<length>` (from `SstHandle.metaOffset`/`metaLength`), so the `BlockServer` serves
the metadata section with one ranged R2 read and no suffix probe on a cache miss, and the URL still
names one immutable object. Blocks use `/blocks/<offset>-<length>` as designed.

## D28. Readers load manifests from R2; `BlockServer` also serves manifest URLs

`DbReader` loads manifests through `ManifestCache` → `ManifestReader` → R2 (the API Worker binds the
bucket): the `LATEST` hint plus probing `id + 1` needs uncached existence checks, and the manifest
is small and cached in the isolate for `manifestRefreshInterval`. The `BlockServer` serves
`/manifests/<id>` (immutable) and `/manifests/latest` (`max-age=1, stale-while-revalidate=10`) as
designed, for other readers and tools.

## D29. `Flushed` reads may lag by the manifest refresh interval

A `Flushed` read uses the isolate's cached manifest until it is `manifestRefreshInterval` (1 s by
default) old, so right after a flush it may still see the older manifest. This is the design's
"a stale manifest only makes it older, never wrong". `Latest` and `AtLeast(seq)` refresh the cached
manifest whenever it predates the writer's `flushedSeq` or `seq`, so they are never stale. The
`DbClient` property test models this, and a dedicated test pins it.

## D30. Snapshots pin a manifest id (pinned snapshots: D52)

`DbClient.snapshot` returns `{ databaseId, manifestId, snapshotSeq }`; reads with it read exactly
that manifest. Until checkpoints (Phase 7) pin it, a snapshot is protected by GC `minAge` only, and
a read of a collected snapshot fails with `SnapshotExpired`.

## D31. `BlockServer` is called with the service binding's `fetch()`

`Cloudflare.Workers.bindWorker` returns an RPC proxy, and RPC bypasses Workers Cache. `BlockSource`
calls `bindWorker` only to register the binding, reads the raw `Fetcher` from the runtime
`WorkerEnvironment` (present at runtime, absent while planning), and wraps it with
`fromCloudflareFetcher` → `toHttpClient` → the generated `BlockServerHttpApi` client, once per
invocation. A missing binding is a defect.

## D32. L1 cache budget is by entry count (superseded by D51)

`BlockCache` caps the data cache at 4,096 blocks (~16 MiB at 4 KiB) and the metadata cache at 1,024
entries, isolate-wide. Design §12 suggests a byte budget per `budgetGroup` (or a `LayerMap` per
group); Effect's `Cache` bounds entries, not bytes, so this is a coarser global budget. Per-group
budgets are a recorded gap.

## D33. GC boundaries: RFC 0026 for manifests, an SST boundary in the manifest

The design lists `gc/` boundary files without detail. Manifests follow SlateDB RFC 0026:
`gc/manifest.boundary` holds the largest collected manifest id; GC advances it (by etag CAS) before
deleting manifests, and `ManifestStore.create` re-reads it after create-if-absent succeeds and treats
an id at or below it as a conflict, so a stalled writer cannot recreate a collected id.

SSTs use a boundary _in the manifest_ (`gcSstBoundary`) instead of SlateDB's ULID-cutoff rules
(RFC 0029): GC advances it by manifest CAS to `now - gcMinAge` before deleting, and `commitL0Flush`
and `commitCompaction` reject new SSTs whose ULID time is below it (`SstBelowGcBoundaryError`).
Because every commit is a manifest CAS, an uploaded but uncommitted SST is either newer than the
boundary or can never be committed, with no clock bound needed beyond `gcMinAge` for readers. GC
moves the boundary only when an old unreferenced SST exists, so an idle database is never written.

Readers are protected by `gcMinAge` (the design's open choice; D29): GC keeps every manifest
superseded less than `gcMinAge` ago, measured from the _next_ manifest's write time. Measuring from
a manifest's own write time is wrong (a manifest can be the newest for hours); the GC model found
it. The deletion bound is `min(boundary, cutoff)`; the model shows the `min` redundant with one GC
at a time and it is kept for overlapping runs.

## D34. Compaction runs one output SST per Workflow step (corruption: D53)

`CompactionWorkflow` steps are `prepare` (resolve inputs in the newest manifest, fix `now` for TTL),
`output-<n>` (re-open the inputs at the step's start key, merge, write one SST up to
`compactionOutputSstSize`, closing only between keys), and `commit`. Each step's result is encoded
with Schema (Workflow results must be plain data) and replayed on resume, so a resumed job merges
the same inputs at the same time. Inputs are read straight from R2, one block at a time with one
block of prefetch, never through the block caches. Discards (fenced, inputs changed, below the GC
boundary, database gone) are step results, not failures, so Workflows does not retry them;
storage failures retry five times with exponential backoff; a corrupted SST is a defect (no retry).
The compactor notices finished jobs on its next poll rather than by a callback.

## D35. Compactor: fence once, poll only with work, woken by the writer

The compactor fences in (`compactorEpoch + 1`) on its first wake and keeps the epoch in Durable
Object storage, so jobs it started stay valid across evictions; only a different compactor fences
it (D78: a fenced compactor now fences in again on its next start). Its alarm polls every `compactorPollInterval` while jobs run or wait for budget, and stops
otherwise. Writers wake it after a flush leaves at least `minCompactionSources` L0 SSTs, and again
when a write stalls on `l0MaxSsts`. `CompactionBudget` is a global Durable Object (`compaction-budget`
ledger: fair share among groups holding or waiting for leases, leases renewed on each poll and
expiring after `compactionLeaseMs`). Settings added: `compactionBudgetCapacity` (32) and
`compactionLeaseMs` (15 minutes).

## D36. Workers: compactor and GC hosts; the test stack's own OTLP collector

`CompactorWorker` hosts `CompactorServer`, `CompactionWorkflow`, and `CompactionBudgetServer`;
`DbApiWorker` binds `CompactorServer` cross-script (`CompactorServer.from(CompactorWorker)`) so
writers can wake compactors. `GcWorker` runs the cron and an operator API (`POST /v1/gc/...`).
workerd rejects an explicit `nodejs_compat` flag when a Worker restarts (it is the default from
2026-08-04: "does not need to be specified anymore"), so every Worker sets only the date. Test stages flush telemetry before each response, which
waited out the 10-second flush timeout on every request when no collector listened; the integration
test now starts its own collector on port 4320 (`test/writer/integration-ports.ts`) and asserts on
the span names it records. The run dropped from minutes to 14 seconds.

## D37. A fork holds its own checkpoint; the caller's checkpoint stays the caller's

`forkRange(source, checkpoint, range, forkId)` does not adopt the caller's checkpoint. In one CAS on
the source it re-checks that the caller's checkpoint still exists and adds a separate checkpoint held
by the fork (`holder = forkId`, no expiry) pinning the same manifest, so GC never sees the manifest
unpinned and the caller can delete its own checkpoint at any time (a split does, right after its
forks). The fork's first manifest names that hold in `externalRefs`. A retried fork finds its hold
and its first manifest and succeeds; a fork id taken by another database fails and the hold is
released. `forkRange` then wakes the fork's compactor.

`createCheckpoint` flushes the writer first, so a checkpoint (and every fork of it) contains every
write acknowledged before the call. `ManifestStore.update` now passes the manifest id to the edit,
so the checkpoint pins exactly the manifest its CAS applied to.

## D38. Forks rewrite external SSTs with one full compaction, then release

> **Release rule corrected by D77.** A fork no longer releases its hold on the compactor's next
> poll: its own collection releases it once no manifest it keeps names another database's SST.

Size-tiered planning alone may never touch a fork's bottom run, which would keep the source's files
alive indefinitely. When nothing else is running or planned and the manifest still reads another
database's SSTs, `planCompactions` plans one full compaction (every L0 SST and sorted run into the
lowest run, bottom). After it commits, the compactor's next poll releases each external reference
that no kept manifest still needs (`releasableExternalRefs`: the newest manifest and the manifests
its own checkpoints pin, so a fork of a fork keeps its source's reference while its own fork still
reads the original owner, D13): first the checkpoint in the owner's manifest, then the reference in
its own, each an idempotent CAS.

## D39. Removal: mark, fence, destroy, release, delete, purge

`remove` first commits `markRemoved` (a new `Removed` database state; refused with
`DatabaseHasDependentsError` while forks hold checkpoints; increments `writerEpoch`). After it, no
flush, checkpoint, or fork can commit (`fenceWriter`, `commitL0Flush`, and `addCheckpoint` refuse a
removed database; writers report it as `DatabaseNotFoundError`). Then: the writer's SQLite state and
alarm are cleared (`DbWriter.destroy`) and the compactor's state and alarm (`Compactor.destroy`); a
fork releases the checkpoints it holds on its sources; every object under `db/<id>/` is deleted,
manifests last (they sort after `compacted/` and `gc/`), so an interrupted removal still has a
manifest and is retried; and the `db:<id>` cache tag is purged through `BlockServer`. Objects that
land after the deletes (a compaction step in flight) leave a prefix with no manifest; GC sweeps such
a prefix once its objects are older than `gcMinAge` (creation writes the first manifest before any
other object, so a prefix without one is never a database being created). The writer's state is
cleared with `DELETE`s rather than `deleteAll()`, so its tables survive and the next request starts a
writer that finds the database gone.

## D40. Sharding: the directory records each split; routers retry while it publishes

**Split order revised by D57** (the seal takes the successors' holds; forks run concurrently from them).

The design's `ShardSplit` is seal → fork once → publish. Two databases are created per split, and a
split that stops after sealing must be finishable, or the sealed shard refuses writes forever. So the
group's directory (one `ShardDirectoryServer` per group) records the split first (`beginSplit`) and
allocates the two new database ids (`s-<ulid>`; derived ids would outgrow `DatabaseId`'s 63
characters after a few splits). Only one split per group runs at a time; retrying a split resumes it
with the same ids (seal, checkpoint, and forks are idempotent), and `completeSplit` publishes the new
map (version + 1). The split's own checkpoint expires after an hour if the split stops, and is
deleted once both forks hold theirs.

`ShardRouter` caches each group's map in the isolate. A stale map reaches only a sealed, out-of-range,
or removed database, which fail with typed errors; the router then refreshes and retries every 250 ms
for up to 30 seconds (a split's publish window), as the design describes. (It was 100 ms for five
seconds until a live split on `preview-1` took 13 seconds and every write that arrived early in it
failed; D48.) The core keeps no map
versions. Batches are atomic only within a database, so a batch spanning shards is refused with
`CrossShardBatchError`.

The package uses only `@shaledb/lsm-cloudflare`'s public entrypoint. `dbServicesLayer` gives any
Worker in the Stack `DbClient` and `DatabaseLifecycle` over cross-script bindings; the example
`ShardRouterWorker` (in the Stack, port 1341 under `vp run dev`) hosts the directories and serves a
small sharded API.

## D41. Transactions are valid until data newer than their reads is flushed (narrowed by D50)

**Superseded by D58** (transactions keep exact reads and checks across flushes).

The writer's conflict check reads only its memtable (an indexed SQLite query per key or range:
`MemTable.writtenAfter`), which holds exactly the rows newer than `flushed_seq`. A transaction reads
at `readSeq` (the writer's `last_seq` when it began) and commits only if `flushed_seq <= readSeq`
still holds, so every commit since `readSeq` is in the memtable and the check is exact; otherwise it
fails with `TransactionConflictError { reason: "SnapshotTooOld" }` (retryable). Reads apply the
same rule: a read whose probe reports `flushedSeq > readSeq`, or that would need a manifest with
`lastL0Seq > readSeq` (`ReadView.maxLastL0Seq`), aborts, because compaction may have collapsed the
versions a read at `readSeq` needs (compaction keeps no versions for snapshots, D30). The two read
checks back each other up (mutation testing removes either one alone without effect).

This needs no extra stored state and no extra SQLite writes (every row written is billed). The cost:
a transaction aborts when a flush of newer writes lands during it — with the default `maxL0Age`
(60 s) and `l0SstSize` (64 MiB) rarely for short transactions, more often for long ones or very
hot databases. A writer-side record of keys flushed since a horizon would remove that, at the cost of
SQLite rows per flushed key; recorded as a possible improvement.

`DbClient.transaction` is interactive in a Worker (buffered writes, reads see them, merges folded
with the database's operator). Over HTTP the API is stateless: `transactions/begin` returns
`readSeq`, `transactions/get` and `transactions/scan` read at it, and `transactions/commit` sends the
writes with the keys read and ranges scanned; nothing is held on the server.

## D42. The change feed reads changelog SSTs directly and never splits a batch

The design's `ChangelogExport.read(afterSeq)` is `DbClient.changes`: it asks the writer for its
unflushed changes and `flushed_seq` together, reads the changelog SSTs for (`afterSeq`,
`flushed_seq`] from a manifest that covers `flushed_seq`, and appends the unflushed rows (the
memtable holds exactly the rows above `flushed_seq`). Changelog SSTs are read straight from R2 through
`ObjectReader` (sequential, once per consumer), not through the block caches, whose URLs address
`compacted/` SSTs. A page never ends inside a batch: it is cut between sequence numbers (and a batch
larger than the page is returned whole; the writer's internal page allows a whole batch,
`MAX_BATCH_OPERATIONS`), so a consumer resuming from `nextAfterSeq` cannot skip rows. A consumer
behind retention gets `ChangelogUnavailable` (`Expired`) instead of a silent gap. (Until D80 that
held only while some entry was kept; D80 states the rule exactly and enforces it in one place.)

## D43. Live stages keep Alchemy state in Cloudflare; local stages keep it on the machine

`infra/shaledb/alchemy.run.ts` selects its state Layer from the stage (`shaledbStateLayer` in
`infra/shaledb/src/shaledb-stage.ts`). Local stages (`dev`, `test`, `local`, and their `-*`
variants, the same rule the telemetry flush policy uses: `isLocalLsmStage`) use `localState()`, so
`vp run dev` and the workerd tests never contact Cloudflare. Every live stage (`preview-1`,
`preview-2`, `preview-3`, and `production` should it ever be approved) uses `Cloudflare.state()`, as
`../iac` does, so a preview deployed from one machine can be redeployed, inspected, or handed to
Dillon for destruction from another.

The store is pinned to Dillon's account (`accountId: 99459fce09d9a9eec83befd957af4ec5`), so a login
that reaches several accounts can never read or write this Stack's state elsewhere. It uses the
account's default `alchemy-state-store` Worker rather than a dedicated one: the Stack name
(`Shaledb`) and stage already key its records apart from other projects', and a second store would
add a second Worker and secret pair to the account for no isolation gain.

## D44. Live stages gate their public Workers with a per-stage access token

On a live stage the API Worker, the shard router, the GC operator API, and the telemetry probe have
public `workers.dev` URLs, and the design leaves authorization to callers ("Callers authorize access
to the database before they fetch"). An ungated preview would be a public, writable database API.
So every live stage generates one access token (`LsmAccessToken`, an Alchemy `Random` of 256 bits,
created once and kept in the stage's state, so it survives redeploys and every machine reads the
same value) and binds it to those Workers as a secret (`SHALEDB_ACCESS_TOKEN`).
`lsmAccessGateForStage` (`lsm-access-gate.ts`) wraps each `fetch` handler: a request without
`Authorization: Bearer <token>` gets `401` with `WWW-Authenticate: Bearer` and the public error
envelope (`UnauthenticatedPublicError`: `code: "Unauthenticated"`, `operation: "routeRequest"`,
`details`, `requestId`, `retryable: false`). The comparison's time depends only on the token's
length.

The gate stands in for Cloudflare Access and, like Access, rejects before any handler runs. Its
`401` is declared in `DbHttpApi`'s errors anyway, so generated clients decode it as a typed error
rather than an undeclared status. Local stages bind no
token and pass every request through, so `vp run dev`, the workerd tests, and the scenario CLI's
local runs are unchanged. `BlockServer` and the compactor keep `workersDev: false` and are reached
only over bindings. The GC Worker gains a `workers.dev` URL on live stages only (behind the gate), so
an operator or the scenario CLI can collect a database on demand; locally it stays on its dev port.

## D45. The preview CLI drives Alchemy through vp tasks and reads outputs from state

`tools/preview-cli` never calls the Alchemy CLI itself. `deploy <stage>` runs
`vp run @shaledb/infra#deploy:<stage>` (one explicit task per approved stage, so no stage name is
ever interpolated into a deploy command) with the task's log on stderr, then reads the outputs back.
`status`, `url`, and `token` read the Cloudflare state store through the read-only
`@shaledb/infra#state:read` task (`alchemy state read --backend cloudflare -r Shaledb[/<stage>]`)
and parse the Stack output record (`Shaledb/<stage>/output`) and the token record; nothing is kept
on the local machine, so they work wherever the account's Alchemy login does.

A preview's engine settings are the production defaults. `deploy --dev-timings` binds `vp run dev`'s
short GC and compactor timings (`SHALEDB_LSM_SETTINGS`); the CLI drops any value of that variable
inherited from the shell, so only the flag decides, and a later deploy without the flag restores the
defaults. `destroy-command <stage>` prints `vp run @shaledb/infra#destroy:<stage>` for Dillon;
agents never run it, by instruction. (The deployment guard blocks a directly typed `alchemy
destroy`, but not this `vp run` task in an allowlisted repository; corrected in D105, see
`docs/release-v1.md` section 10.)

## D46. Live stages send spans to their own TraceCollector Worker; the trace CLI pulls them

Deployed Workers cannot reach the trace CLI's `127.0.0.1:4318`. Every live stage therefore gets a
`TraceCollectorWorker` (`infra/shaledb/src/trace-collector-worker.ts`), and only live stages do:

- **Receiving.** `POST /v1/traces` stores each OTLP/HTTP export as received (JSON or protobuf, with
  its content type) in the stage's own `TraceBucket` under `traces/<arrival ms, 15 digits>-<uuid>`,
  so keys sort by arrival. Nothing is decoded in the Worker. `/v1/logs` and `/v1/metrics` are
  accepted and discarded, like the local receiver.
- **Reading.** `GET /v1/traces/batches?after=<key>&limit=<≤100>` returns the stored exports after a
  key, oldest first, with base64 bodies, the key to continue after, and whether more follow. `trace-cli
  pull --stage <stage>` decodes them with the same decoders as `serve` and appends the spans to the
  local store, keeping a cursor per stage and collector URL (`<store>/pulls/<stage>.json`), so a pull
  downloads only what is new and `list`, `show`, `grep`, `stats`, `diff`, and `assert` work on
  deployed traces unchanged. `tail --stage` pulls before every poll (at least every 2 s).
- **Authentication.** Every request needs the stage's `TraceCollectorToken` (a second per-stage
  `Random`, not the access token of D44, so reading traces grants no access to databases). The
  Workers send it as a bearer header bound as a secret; the trace CLI asks
  `preview-cli collector <stage> --json` for the URL and token.
- **Storage.** A separate bucket rather than a `traces/` prefix in the LSM bucket, so telemetry
  never mixes with database objects, GC never sees it, and removing a preview's traces cannot touch
  its data. It is generated-name and not retained, like every preview resource.
- **Wiring.** Each Worker layer takes an `LsmWorkerTelemetry` factory (a `service.name` → telemetry
  Layer). Tests pass `lsmOtlpTelemetry(url)`; the Stack passes `shaledbTelemetry`, which on live
  stages binds the collector's URL and token onto the Worker through `Telemetry.layerOtlp`. It
  reaches the collector through `Cloudflare.Worker.ref(TraceCollectorWorker)`, not by yielding the
  class: a same-program ref is bound to the declared Worker (the collector still deploys first), and
  it adds no requirement, so local stages, which do not declare the collector, still type-check.
  Declaring a Worker's Layer deploys it — an earlier version provided the collector's Layer on every
  stage and `vp run dev` started a local collector on the API's port.

D5's question for live stages is answered: spans from Durable Object fetches and alarms, and (with
D47) from Worker fetches, arrive through the bridge's `waitUntil` flush after the response; ten of
ten consecutive probe requests on `preview-1` arrived complete.

## D47. Live Workers use `global_fetch_strictly_public`

On `preview-1` every span of a Worker's own fetch events was lost (the API Worker's HTTP server
span and its client span to the writer), while the writer Durable Object's spans, exported from the
same script, arrived. The exporter's `fetch()` goes to the collector's `workers.dev` URL on the same
account, and Cloudflare does not route a Worker's plain `fetch()` to a Worker on its own subdomain.
With the `global_fetch_strictly_public` compatibility flag the request goes out as a public one and
every span arrives (probe: 10 of 10 complete traces; API: complete Worker → writer trees).

`lsmWorkerCompatibility` (`lsm-stage.ts`) adds the flag on live stages only, for every Worker that
exports spans. Calls between shaledb's Workers use service bindings and Durable Object stubs,
which the flag does not affect. Local stages leave it off, because their spans go to `127.0.0.1`,
which a strictly public `fetch()` cannot reach.

## D48. Live stages record per-invocation CPU time with a Tail Worker; measurements drive the router's window

**Router polling superseded by D57** (routers wait on the directory; the 30 s bound stays).

Design §12 asks for flush CPU per MB and rows/s per writer, but a Worker cannot read its own CPU
time (its clock advances only across I/O). Live stages therefore declare `CpuTailWorker`
(`infra/shaledb/src/cpu-tail-worker.ts`), a plain module Worker with a `tail()` handler — Alchemy's
Effect-native Workers cannot export one — attached as a tail consumer to the API Worker (writer
Durable Objects) and the compactor Worker through a new `tailConsumers` option
(`LsmWorkerTailConsumers`; `noLsmTailConsumers` locally and in tests). It stores one record per
invocation (script, Durable Object id, event kind, method, URL path, status, outcome, `cpuTime`,
`wallTime`) under `tails/` in the stage's trace bucket; headers, logs, and exception messages are
dropped, so no keys, values, or secrets are kept. The collector serves them at
`GET /v1/tails/batches`, and `scenario-cli measure writes | flush-cpu | split-pause` combine them with
spans (`docs/measurements.md`).

The measurements changed one setting: the shard router retried a sealed or out-of-range write for 5
seconds, but a split on `preview-1` takes about 13 seconds (each manifest CAS is about 1.1 s on R2),
so every write arriving early in a split failed. The window is now 30 seconds (every 250 ms).

## D49. Data residency is per Stack; the per-database option is gone

Dillon chose per-Stack residency (open question 3). One jurisdiction — `eu`, `fedramp`, or none —
applies to a whole installation: `SHALEDB_JURISDICTION` in the deploy's environment
(`preview-cli deploy <stage> --jurisdiction eu|fedramp|none`), read with `Config` as
`lsmJurisdiction` (`lsm-jurisdiction.ts`), so every Worker that reads it at plan time gets it bound and
reads the same value at runtime. A value other than `eu` or `fedramp` stops the Worker. It is applied
to:

- **R2:** the LSM bucket and the trace bucket are created with that `jurisdiction`.
- **Durable Objects:** every client addresses its namespace through `inLsmJurisdiction`
  (`namespace.jurisdiction(j)`): `WriterServer`, `CompactorServer`, `CompactionBudgetServer`,
  `ShardDirectoryServer`, and the probe. A name addresses a different object in each jurisdiction, so
  every Worker must agree, which the single bound setting guarantees.
- **Workflows:** the compaction Workflow's steps record SST handles, whose first and last keys are
  data, so it is declared with the jurisdiction. Workflow props are fixed when the class is declared,
  so this one reads the deploy's environment when the module loads (`lsmJurisdictionAtDeclaration`).
- **Workers Cache is turned off** in a restricted Stack: its tiered cache keeps `BlockServer`'s
  responses — raw block bytes — in data centers anywhere. Readers then rely on the L1 isolate cache
  and R2. L1 holds blocks only in the memory of the isolate serving a request, as any Worker
  processing the data does.

The jurisdiction is fixed per installation. Changing it replaces both buckets and the Workflow and
addresses new Durable Objects, so a database does not move with it; `preview-cli deploy` keeps a
stage's current jurisdiction unless `--jurisdiction` asks for another, and warns when it changes.

The advisory per-database `DatabaseOptions.jurisdiction` (stored, inherited by forks, never enforced)
is removed: it could only disagree with where the Stack actually stores data. Manifests written
with it still decode — Schema ignores the unknown field — and the next manifest written drops it
(`manifest.test.ts`). The public API never accepted it. `forkManifest` copies options unchanged, so
fork inheritance needs nothing.

## D50. Transactions keep the flush limit: only a transaction that outlasts a whole flush aborts

**Superseded by D58** (transactions keep exact reads and checks across flushes).

Hand-off item 6 asked to let a transaction's conflict check read rows flushed after its snapshot from
L0, or justify keeping D41's limit. It is kept, because live measurement shows the limit is far
narrower than D41 described.

**When it aborts.** A flush takes its cut (`last_seq` when it starts), uploads the L0 SST, commits
the manifest, and only then truncates the memtable and sets `flushed_seq` to the cut, in one SQLite
transaction (D21). A transaction aborts (`SnapshotTooOld`) only if `flushed_seq > readSeq` when it
reads or commits, that is, only if it **began before the flush took its cut and reads or commits
after the flush finished**: it must outlast the whole flush. A transaction that begins during a flush
has `readSeq` at or above the cut; one that commits before the flush finishes still sees the old
`flushed_seq` and its rows in the memtable. D41 said "a flush landing during it", which overstated it.

**How long a flush takes live.** At least one SST upload and one manifest CAS: about 1.5 s on
`preview-1` for a small flush (`DbWriter.flush` p50 1.6 s), 2–7 s for 6–52 MiB, and 28.7 s for a
19 MiB flush under write load (`docs/measurements.md`). So transactions shorter than about a second
and a half never abort for this reason. Measured: `scenario-cli measure transactions --stage
preview-1` ran 580 `SerializableSnapshot` transactions over the HTTP API (begin, two reads, commit;
p50 1.0 s, max 2.9 s from a laptop) while four writers loaded the same database through a 28.7 s
flush: 580 committed, 0 `SnapshotTooOld`. A transaction longer than its flush aborts with
probability about (duration − flush duration) / flush interval, and the error is retryable with
nothing written.

**Why not the alternative now.** Removing the limit needs reads at `readSeq` to stay exact after a
flush (pin the manifest seen at begin and accept a newer one only if no compaction has removed any
of its SSTs, since compaction keeps no versions for snapshots, D30), and the commit to check rows
in the L0 SSTs flushed since begin for the transaction's keys and ranges — including tombstones and
range deletes, which the reader's visible-row API hides — either in the writer (R2 reads inside the
commit lock, stalling every write) or in the caller before commit (a new commit precondition on the
flush horizon). That reworks the reader, the writer's commit check, and the public commit payload,
and both the transactions and no-acknowledged-write-lost models, to remove an abort that live
transactions under a second and a half cannot hit. The design is recorded here for when long
transactions matter.

The model (`Transactions.tla`) already lets `Flush` interleave anywhere, so it covers this exact
window; no model or code change.

## D51. The L1 block cache is bounded by bytes, per budget group and in total

Replaces D32's entry-count bound. `BlockCache` keeps decoded data blocks and SST metadata in two
`GroupByteBudgetLru`s (`lsm-core/src/group-byte-budget-lru.ts`), each entry charged its size as
stored on R2 (a decoded block keeps a view of exactly those bytes; metadata is charged
`metaLength`):

- `l1DataCacheBytes` (32 MiB) and `l1MetaCacheBytes` (16 MiB) bound each cache for the isolate —
  about a third of 128 MB. The old metadata bound of 1,024 _entries_ was not a memory bound at all:
  the index and Bloom filter of one 64 MiB SST are about a megabyte.
- `l1GroupCacheShare` (0.5) bounds what one `budgetGroup` may hold. A read is charged to the reading
  database's `budgetGroup`, or its id (a fork's reads of its source's SSTs are charged to the fork).
  A full group evicts its own least recently used entries, never another group's.
- When all groups together would exceed the total, the group holding the most bytes gives up its
  least recently used entry, repeatedly, so a busy tenant shrinks before a quiet one is touched;
  the entry being added is never the one evicted.

Effect's `Cache` bounds entries, so the cache is now private code with the same contract: concurrent
misses for one key join one read (a waiter whose reader is interrupted reads again itself), failures
are never kept, and reads run with the calling invocation's services. Design §12 also offers a
`LayerMap` per group with idle release; one structure with per-group accounting is simpler and
lets groups borrow unused room.

## D52. Long-lived snapshots pin through a checkpoint; plain snapshots stay free and expire

D30's snapshots pin a manifest id that GC keeps only for `gcMinAge` after a newer manifest replaces
it. Both of hand-off item 6's options are taken:

- **Pinned snapshots.** `DatabaseLifecycle.pinSnapshot(databaseId, lifetimeMs)` and
  `POST /v1/databases/:id/snapshots/pinned { lifetimeMs }` flush every acknowledged write, then pin the
  resulting manifest with a checkpoint (named `snapshot`) that expires after `lifetimeMs` (at most
  30 days over HTTP). They return `{ snapshot, checkpointId, expireAt }`; `snapshot` is an ordinary
  `DbSnapshot`, so reads take it unchanged, and deleting the checkpoint releases it early. GC keeps a
  checkpoint's manifest and every SST it references (the GC model's `PinnedManifestsComplete`), and
  drops the checkpoint when it expires. A lifetime is required: a forgotten pin would otherwise keep
  SSTs forever.
- **Plain snapshots, documented.** `DbClient.snapshot` and `POST …/snapshots` still write nothing
  — a pin is a manifest CAS, about a second on R2 — and their limit is now stated in the API: GC
  keeps the manifest for `gcMinAge` (an hour by default) after it is replaced, then reads fail with
  `SnapshotExpired`, whose message now says to take a pinned snapshot to read for longer.

Pinning only reuses checkpoints, so GC and fork semantics are unchanged; `database-lifecycle.test.ts`
shows a pinned snapshot readable past `gcMinAge` while an unpinned one has expired, and the pinned
one collected after its lifetime.

## D53. A corrupted input SST ends its compaction with a typed outcome the compactor reports

D34 turned a corrupted input (`SstCorruptedError`: a CRC32C mismatch, truncation, or malformed
section) into a defect. The Workflow run then errored without saying which SST, the compactor saw only
"not running", and its next poll planned the same job again, forever.

Now:

- **Typed outcome.** A step that hits `SstCorruptedError` returns `Corrupted { sstId, section,
  reason, message }` as its result, and the job ends with that `CompactionJobOutcome`. Like a
  discard, it is a result, so Workflows never retries it and the run completes. The job's span
  records `compaction.outcome=Corrupted` and the SST, and an error is logged.
- **The compactor reads outcomes.** `CompactionLauncher.isRunning` became `progress`: `Running`, or
  `Finished` with the outcome when known (decoded from the Workflow instance's output; none when the
  run errored or was terminated).
- **Reported and quarantined.** A `Corrupted` outcome adds the SST to the compactor's durable state
  (`corruptSsts`: SST, the L0 SST or sorted run holding it, section, reason, time;
  `GET /v1/databases/:id/compaction` returns it), increments `lsm_compaction_corrupt_ssts`, and
  logs an error. `planCompactionsAround` reserves those sources like a running job's, so no job reads
  them again; while any is blocked, no fork rewrite is planned. A blocked L0 SST pauses L0
  compaction (L0 must be merged oldest first), which can end in `WriteStalled` once L0 fills — an
  honest stop: compacting would spread or drop the corrupted data. The entry is dropped once the SST
  leaves the manifest (restored or removed by an operator).
- **Compatibility.** Compactor state written before this decodes with no corrupt SSTs
  (`withDecodingDefaultKey`).

Reads of the SST still fail with `SstCorrupted` (public `StoredDataInvalid`, 500, not retryable).
`compactor.test.ts` flips one byte of an L0 SST: one job runs, ends `Corrupted` naming that SST and
`DataBlock`, the compactor records it with its L0 source, plans nothing more, and L0 keeps all four
SSTs; restoring the defect, or not blocking the source, fails the test.

## D54. Deploys bind every setting explicitly; a setting a deploy stops setting stays bound

**Superseded by D55** (the mechanism; the finding stands).

Alchemy binds a `Config` value read during a Worker's init as a `secret_text`, and Cloudflare keeps a
script's secrets across uploads. So when a deploy _stops_ setting a value, nothing replaces it: on
`preview-1`, `SHALEDB_LSM_SETTINGS` from a `--dev-timings` deploy stayed bound through the next
deploys without the flag (found by reading the Workers' bindings in the state store). For the
engine settings that only meant dev GC and compactor timings lingered; for the jurisdiction it would
mean Workers still addressing EU Durable Objects after a Stack moved out of the EU.

So the preview CLI always sets both explicitly (`deployEnvironment`): `SHALEDB_LSM_SETTINGS` is
`{}` (no overrides) unless `--dev-timings`, and `SHALEDB_JURISDICTION` is `eu`, `fedramp`, or the
new value `none`, which `lsmJurisdiction` reads as no restriction. Verified on `preview-1`: every
Worker's bindings now hold `{}` and `none`. A future production deploy task must do the same.

## D55. The Stack owns its configuration and binds every Worker its complete settings

Supersedes D54's mechanism. D54 patched one deploy path: `preview-cli` always passed
`SHALEDB_LSM_SETTINGS` and `SHALEDB_JURISDICTION`, so the values Alchemy's `Config`
interception binds (and Cloudflare keeps across uploads) were overwritten. Any other path — a manual
`vp run …#deploy:*`, a future production task — still left whatever an earlier deploy bound, and
three Workers (`BlockServer`, the probe, the trace collector) never had the settings bound at all.

Now one typed configuration, `ShaledbStackConfig` (`infra/shaledb/src/shaledb-stack-config.ts`),
is resolved from the deploy's environment with complete defaults: stage, jurisdiction (`eu` |
`fedramp` | `none`, default `none`), the complete `LsmSettings` (defaults merged with the JSON
overrides; unknown setting names refused), telemetry (trace sample rate: 1, or 0.1 in production;
retention: 14 days), and the access mode (`Open` locally, `BearerToken` live). The Stack resolves it
before declaring anything, so a refused variable stops the plan as a `ConfigError` naming it.

Every Worker layer in `lsm-cloudflare` and `lsm-sharding` takes a required `settings` source
(`LsmWorkerSettingsSource`), and its init calls `bindLsmWorkerSettings` last, after every `Config`
read, so it replaces whatever interception bound from the environment. It binds both keys, always:
the complete settings as JSON and the jurisdiction (`none` included). "Unset" can no longer mean
"whatever was there before". `lsm-cloudflare` stays unaware of infra; the infra Workers (the probe
and the trace collector) call the same function.

- **The values stay `secret_text`.** They are not secret, but interception bound them that way on
  every existing deployment; binding the same type avoids switching a binding's type on a live Worker.
- **Workers read them back unchanged** (`makeLsmConfiguration`, `lsmJurisdiction`). The reader still
  accepts a partial object, for tests and deploys before D55 (`lsm-worker-settings.test.ts`).
- **Declaration-time reads.** The compaction Workflow's jurisdiction is a declaration prop read when
  its module loads (`lsmJurisdictionAtDeclaration`), from the same variable with the same default. An
  invalid value there reads as none, but the Stack's resolution refuses it first, so the plan fails.
- **Contradictory settings fail.** `LsmSettings` checks combinations (`minCompactionSources ≤
  maxCompactionSources`, `l0SstSize ≤ maxUnflushedBytes`, `compactorPollInterval <
  compactionLeaseMs`); workstream 4 adds `transactionRetentionMs < gcMinAge`.
- **`preview-cli` only translates flags.** `deployEnvironment` sets the jurisdiction and, with
  `--dev-timings`, the dev overrides, and removes the Stack's other variables from the inherited
  shell, so leftovers cannot change a preview. `preview-cli bindings <stage>` prints each Worker's
  bound values from state.
- **Ephemeral buckets are destroyable.** Found live: moving `preview-3` from `eu` to `none` replaces
  both buckets, and Alchemy refused to delete the old EU trace bucket because it held spans
  (`BucketNotEmpty`). The deploy had already rebound every Worker, but it failed and left the bucket.
  Outside production, `LsmBucket` and `TraceBucket` now set `forceDestroy`, so a replaced or destroyed
  preview bucket is emptied first; production keeps R2's refusal. A pending delete uses the props in
  state, so the one orphaned bucket (23 trace and tail records) was emptied through the R2 API and the
  deploy rerun.

Verified live: `preview-1` deployed with `--dev-timings` (every Worker bound the complete settings
with the dev timings), then by a bare `vp run @shaledb/infra#deploy:preview-1` with no
`SHALEDB_*` variables (every Worker bound the complete production settings and `none`).
`preview-3`, never deployed before, went `eu` → `none`, and every Worker was bound `none`. All three
previews were then redeployed with the final code.

## D56. Manifest commits start from the committer's own view; the LATEST hint is background work

A manifest CAS cost ~1.1 s on R2 (`docs/measurements.md`): `loadLatest` (~300 ms: hint, manifest,
probe of `id + 1`), the create-if-absent PUT (~400 ms), the GC-boundary re-read (~60 ms), and the
best-effort `LATEST` hint (a read and a PUT, ~420 ms). Every flush, seal, checkpoint, fork, and
compaction commit paid it.

- **Optimistic create from the committer's own view.** `ManifestStore.updateFrom(view, edit)`
  applies the edit to the newest manifest the caller knows and creates `view.id + 1` without
  loading. Create-if-absent plus the boundary re-read prove the view was the head, so a written
  commit is trusted. Anything else reloads and continues as `update` does: a conflict, an edit
  failure, or "already applied" against the view (an older view can answer those wrongly, for
  example a checkpoint added after it would read as not found). The writer uses it for flushes and
  the seal (`DbWriter.flushNow`, `DbWriter.seal`), holding the manifest it last committed.
  Compaction commits keep `update`: they run in a Workflow long after planning, so a view there
  would almost always be stale.
- **The hint leaves the critical path.** `ManifestStore.create` hands the hint to a new
  `BackgroundWork` service: `waitUntil` in Worker events, `state.waitUntil` in Durable Objects, and
  inline in Workflow steps (which have no `waitUntil`). The test layer forks, so tests see the hint
  land late. The write is conditional (read the hint, write only if newer, `If-Match` or
  create-if-absent), so a late write never moves it back.
- **The reader no longer depends on the hint.** Found while modeling: `loadLatest` probed forward
  from the hint until the first missing id. With a lagging hint (a lost write, already possible when
  the hint PUT failed, and likelier once it is background work) and a checkpoint keeping manifest
  `h` while GC deleted `h + 1`, it stopped at `h`. A committer would then conflict on every attempt,
  and a `Latest` reader would read an old manifest. Now it reads the hint and GC's manifest boundary
  together and probes from `max(hint, boundary + 1)`. Manifests above the boundary are never deleted
  and are contiguous up to the newest, so the probe cannot stop early. It also probes the start and
  the id after it in parallel. No extra round trip.
- **R2 placement.** A probe Durable Object per location hint (`GET /v1/probe/r2`, local and preview
  stages only) timed the engine's object store calls against `preview-1`'s bucket (ENAM).
  Create-if-absent costs ~285 ms p50 even from IAD, beside the bucket. From the US west coast it is
  ~510 ms, from Europe ~550–590 ms, from APAC and Oceania ~900–1,000 ms. `preview-1`'s writers
  already run near its bucket, so placement could not speed it up. An installation chooses its
  bucket's location once: `SHALEDB_BUCKET_LOCATION` (`wnam` … `oc`, default `auto`, meaning R2
  places it near the deploy) in the Stack config, bound to every Worker with the other settings and
  read by `LsmBucket`. It is refused with a jurisdiction (R2 places those itself), and changing it
  replaces the bucket.
- **`object.kind`** (`Manifest`, `LatestHint`, `ManifestBoundary`, `Sst`, `Changelog`, `Gc`,
  `Other`) is a new safe attribute on every ObjectStore span. The `create-race` scenario now counts
  manifest creates only, since the background hint's first write is also a create-if-absent.

Model: `ManifestCasFencing.tla` now models the optimistic first attempt, the boundary re-read as
part of each create, `loadLatest` from `max(hint, boundary + 1)`, hint writes that land in any order
or never, and (in the new `ManifestCasFencingGc.cfg`) GC deleting any manifest at or below an
advancing boundary. It adds two invariants, `CommitOnce` and `LoadFindsHead`, and two broken
variants that fail: `noBoundaryReread` (fails `CommitOnce`) and `hintOnlyProbe` (the old reader,
fails `LoadFindsHead`). Mutations of the real code are caught by `manifest-store.test.ts`.

Measured on `preview-1` with production settings, 30 small flushes in sequence:

|                            |   Before |  After |   Target |
| -------------------------- | -------: | -----: | -------: |
| `ManifestStore.update` p50 | 1,092 ms | 357 ms | ≤ 500 ms |
| own-view commit p50        |        – | 340 ms |          |
| `loadLatest` p50           |   274 ms | 180 ms |          |
| `DbWriter.flush` p50       | 1,508 ms | 709 ms | ≤ 900 ms |

## D57. A split seals, holds, and forks in one commit; routers wait on the directory

Supersedes D48's 30-second router polling window (the bound stays; the waiting is now on the
directory) and changes D40's split order. A live split paused a shard's writes for 13–16 s at the
start of v1. Workstream 2's faster commits brought it to ~8.6 s, still a chain of commits and waits:
the final flush and its commit, the seal commit, a checkpoint commit, a hold commit per fork, a
first-manifest create per fork, ~2 s per fork for its compactor to fence in, a checkpoint delete,
and the router polling every 250 ms.

- **One commit seals and holds.** `DbWriter.seal(…, holdForSuccessors)` commits the final flush,
  the seal, and each successor's hold (lsm-core `holdForSuccessors`: a checkpoint the successor
  holds, pinning the manifest this commit creates) in one manifest CAS under the commit lock
  (`flushThen`). It needs a separate seal CAS only when nothing is left to flush. The public seal
  endpoint keeps a plain seal.
- **Forks need no commit on the shard.** Each fork starts from its own hold (`forkRange` finds the
  hold already taken and skips its CAS). The two forks run concurrently, and the split publishes
  without a separate checkpoint or its deletion.
- **Forks are warmed in the background.** After its first manifest, each fork's writer fences in
  and its compactor is woken via `BackgroundWork`, so the first write after the publish does not
  pay the fence-in. A lost wake only delays the rewrite: the first write fences the writer, and
  workstream 5's maintenance sweep wakes the compactor of any database still holding another's SSTs.
- **Routers wait on the directory.** `ShardDirectory.awaitMap(group, afterVersion, timeout)` is a
  long poll on the group's Durable Object. It returns at once when the map is already newer, as
  soon as a split publishes (`Newer`), when the pending split reports a failure (`SplitFailed`,
  from `ShardSplit` via `reportSplitFailed`; the split stays recorded and resumable), immediately
  when nothing could publish (`NoSplitPending`), or after the timeout (at most 25 s). On
  `DatabaseSealed`, `KeyOutOfRange`, or `DatabaseNotFound` the router waits on it and retries the
  moment a newer map arrives. Otherwise it surfaces the routing error, which is retryable
  (`ShardedRequestUnavailable`), bounded by 30 s in total.
- **A lost CAS race is an outcome, not an error.** `ManifestStore.create` spans record
  `manifest.outcome` (`Created` or `Conflict`) instead of an error status when the CAS loop retries,
  like `ObjectStore.putIfAbsent`'s `AlreadyExists`. A legitimate race no longer fails a
  `NoErrorStatus` trace check (found in the local `gc` scenario).

Model: `SealSplit.tla`'s `Seal` now takes the checkpoint (the holds) in its own step; `Checkpoint` is
only the `checkpointBeforeSeal` broken variant. Concurrent forks were already covered, and every
broken variant still fails. Tests: `sharding.test.ts` covers each `awaitMap` outcome; a held write
routed within 2 s of the publish; a failed split failing held writes at once; and a resumed split
making exactly one commit on the shard. `db-writer.test.ts` checks that flush, seal, and holds are one
commit pinning it, and `manifest.test.ts` covers `holdForSuccessors`. Mutations caught: no publish
signal, no failure signal, a seal without holds.

Measured on `preview-1` with production settings, `measure split-pause`, three runs each:

| Writers |     Split | Longest gap between acknowledged writes | Failed writes |
| ------: | --------: | --------------------------------------- | ------------: |
|       4 | 1.8–2.0 s | 2.6 / 2.7 / 3.2 s (median 2.7 s)        |             0 |
|      16 | 1.8–2.0 s | 2.6 / 3.1 / 3.4 s (median 3.1 s)        |             0 |

Target: a write pause p50 ≤ 6 s with zero failed writes. Met.

## D58. Transactions are exact across flushes: the writer keeps flushed rows, reads pin the begin manifest

Supersedes D41's flush limit and D50. A transaction used to abort with `SnapshotTooOld` once a flush
deleted rows newer than its read sequence number from the writer's SQLite. After that, neither its
reads nor its commit check could be exact. Long or slow transactions hit it, and users had to learn
the limit.

- **The writer keeps flushed rows.** The memtable gains `retained_from_seq ≤ flushed_seq`: every row
  above it is still in SQLite. A flush still writes only rows in `(flushed_seq, cut]`, then
  `markFlushed` records `flushed_seq = cut`, the committing manifest's id (`flushed_manifest_id`),
  and a flush mark (cut, time) in one SQLite transaction. It deletes nothing.
- **A separate, atomic trim.** It deletes flushed rows older than `transactionRetentionMs` (new;
  default 60 s, and it must be shorter than `gcMinAge` or 0, enforced by `LsmSettings`). It is bounded
  by `maxRetainedBytes` (new; default 64 MiB), never removes the newest flush, and raises
  `retained_from_seq` past what it removed. It runs after each flush.
- **Latest paths are unchanged.** The probe, Latest scans, the change feed, unflushed bytes,
  backpressure, and flush streaming read only rows above `flushed_seq`.
- **Begin** returns `readSeq` and `manifestId` (the flushed manifest, whose `lastL0Seq` is
  `flushed_seq`), read in one SQLite statement. On the public API this is an additive `manifestId` in
  the `transactions/begin` response, accepted as an optional field by `transactions/get` and
  `transactions/scan`.
- **Reads at `readSeq`** take the writer's rows in `(retained_from_seq, readSeq]` (`probeAt`;
  `scan` with `includeRetained`) and the pinned begin manifest. The reader refuses
  (`SnapshotExpired`, surfaced as `SnapshotTooOld`) when that manifest's `lastL0Seq <
  retained_from_seq`, since the rows in between are then gone from both. GC keeps a superseded
  manifest for `gcMinAge`, which is longer than the retention.
- **Commit** aborts with `SnapshotTooOld` only if `retained_from_seq > readSeq`. Otherwise
  `writtenAfter` checks every row above `readSeq`, retained or unflushed.
- **Restarts keep retained rows.** On restart the writer records the fenced manifest's `lastL0Seq` as
  flushed (with that manifest's id) instead of deleting rows.
- **Old callers and old state keep working.** Clients without `manifestId` keep the pre-D58 reads (a
  flush past `readSeq` aborts them). Writer state from before D58, without `retained_from_seq` or
  `flushed_manifest_id`, reads its horizon as `flushed_seq` (its old flushes deleted those rows), and
  its begin returns no manifest until its next flush.

Cost: SQLite holds the retained rows, at most `maxRetainedBytes` plus the newest flush (up to
`l0SstSize`). A flush writes no more rows than before; the trim's deletes happen after the flush,
outside the commit lock. `dev` and preview `--dev-timings` set `transactionRetentionMs` to 5 s
(their `gcMinAge` is 10 s); the integration test uses 0.5 s against 1 s.

Models: `Transactions.tla` was rewritten with the retained horizon, nondeterministic trims, and
reads from the begin manifest plus retained rows. Its broken variants `trimWithoutAbort` and
`noReadHorizonCheck` fail `Serializable`, and `snapshotOnly` still does. Bounds went from 6 commits
to 5 (37M states). A pinned manifest is modeled by its `lastL0Seq`, which is what any compaction of
it preserves for a read. In `NoAckedWriteLost.tla` a flush now advances `flushed_seq` and a separate
`Trim` deletes, with the Latest probe reading above `flushed_seq`. `LatestSeesAcked` holds, and the
new `trimUnflushed` variant fails `NoAckedWriteLost`.

Measured on `preview-1` with production settings: 4 transactors held each transaction open ~21.7 s
between its reads while 4 writers loaded the database and it was flushed every 3 s (23 flushes).
24/24 committed, 0 `SnapshotTooOld`.

## D59. Routed `Latest` reads refuse a sealed database, so a stale map cannot hide a split

Found live while checking D58 on `preview-1`: the `fork-split` scenario intermittently read `None`
for a key it had just written to a new shard (and a scan missed it). The router Worker isolate
that served the write had learned the new map, because the sealed source refused the write. The next
request reached another isolate whose cached map was still the old one. Reads of a sealed database
succeed (it stays readable for its own callers), so nothing told that isolate its map was stale, and
it read the sealed source, missing the acknowledged write. The bug was latent since Phase 8;
D57's faster splits made the window easy to hit.

- **The writer says when it is sealed.** `DbWriter.probe` and `scan` report `sealedSuccessors`.
- **`ReadOptions.refuseSealed`.** A `Latest` read or scan with it set fails with
  `DatabaseSealedError` (naming the successors) on a sealed database, before returning any row. The
  shard router sets it; the public database API and every other caller leave it false.
- **Routed reads route like writes.** A routed get goes through the same directory wait as a write
  (`withRouting`). A routed scan goes shard by shard and, when one shard turns out sealed, waits on
  the directory and resumes that shard's part from the new map. A refused shard emits no rows, so
  nothing is returned twice.
- **`Flushed` and `AtLeast` reads** never consult the writer and stay eventually consistent; reading
  a sealed source through them returns its state at the seal, a valid older snapshot.

Test: `sharding.test.ts` "a router whose map went stale in another isolate still reads its writes
after a split" (a second router caches the old map; its get and scan see the new key), shown failing
without the refusal. Live: `fork-split` passes 5/5 on `preview-1` (it had failed 2 of 3), and all 11
scenarios pass.

## D60. Each database's compactor collects its garbage; the cron is a paged sweep

`GarbageCollector.collectAll` listed every `db/` prefix and collected every database in one cron
invocation (concurrency 4). With thousands of databases it would exceed subrequest, CPU, or
wall-time limits and silently stop partway.

- **The compactor owns its database's collection.** `CompactorState` gains `collectionDueAt` (absent
  in older state, which decodes as none). It is set `gcMinAge` after the compactor sees a compaction
  finish, because the replaced inputs and manifests are old enough then. Each collection reports
  `nextCollectionAt` (lsm-core `nextCollectionTime`): when something it had to keep becomes
  collectable (a young unreferenced SST or superseded manifest, or a checkpoint that expires), or
  none. The alarm runs a due collection and keeps the alarm set while one is due. An idle database
  with no garbage sets no alarm. A collection's own commit supersedes one manifest, so a database
  converges after a second, commit-free collection.
- **One collection per database at a time.** The compactor serializes collections with a lock, and
  the operator's "collect now" (`POST /v1/gc/databases/:id`) runs on the compactor
  (`Compactor.collect`). That keeps `GcSafety.tla`'s one-collection-at-a-time assumption true for
  every collection the compactor runs. Collections still interleave with compaction commits, which
  the model already covers.
- **Two collections at once are still possible, and safe** (found in review, D83). The sweep calls
  `GarbageCollector.collect` itself, outside the compactor's lock, for a database whose newest
  manifest it could not load (`loadLatest` failing for any reason, not only a missing manifest);
  if the collection's own load then succeeds, it runs beside the compactor's. Take an earlier run
  X and a later run Y of one database:
  1. **Neither moves a boundary back.** `gcSstBoundary` only rises (`advanceGcSstBoundary`), and
     `gc/manifest.boundary` is a version CAS that never lowers (`advanceManifestBoundary`). Each run
     deletes SSTs created before `min(its own committed manifest's gcSstBoundary, its cutoff)`.
  2. **What a run retains covers every newer manifest.** Each run plans from its own committed
     manifest `C`, and retains every manifest at or after `C`. A manifest committed later names `C`'s
     SSTs or SSTs uploaded since, and an upload created before the run's SST boundary cannot be
     committed after it (`SstBelowGcBoundaryError`). So any SST a run may delete that a newer
     manifest names is named by `C`, which the run holds in memory.
  3. **A retained manifest can vanish only by a later run's judgment.** A collection deletes a
     manifest only after moving `gc/manifest.boundary` past it, and only when it judged it
     deletable: superseded at least `gcMinAge` ago, and pinned by no checkpoint of its own
     committed manifest. When X finds a retained manifest gone at or below the boundary, Y deleted
     it, and every SST that manifest alone names is one Y deletes as well, since Y does not retain
     the manifest. X then deletes nothing Y retains: Y's retained manifests at or after X's `C` are
     covered by (2), and the older ones were stored when X listed, so X loaded them too.
  4. **Anything else stops the run.** A retained manifest gone above the boundary was deleted by
     something other than a collection (only removing the database may). The run then fails with
     `GarbageCollectorUnavailableError` before it deletes anything (`GarbageCollector.collect`).

  `GcSafety.tla` still has one collection; the argument above is checked by unit tests
  (`garbage-collector.test.ts`): two collections stopped at the listing and at the deletes by gates,
  and the guard in (4).

- **The cron is a paged maintenance sweep** (`MaintenanceSweep.sweepPage`, 50 databases per run,
  position in `maintenance/sweep.cursor`, listing with `startAfter`). It handles only what has no
  actor of its own:
  - It sweeps prefixes without a manifest (removed databases' leftovers).
  - It wakes the compactor of any database whose manifest still names external refs, so a lost fork
    wake (D57) only delays the rewrite.
  - It asks for a collection on any database more than 16 manifests past GC's boundary.
    `POST /v1/gc` runs one page. A run's cost is bounded by the page, whatever the number of databases.
- The GC Worker now also reports to the CPU Tail Worker.

Tests: `garbage-collector.test.ts` runs the sweep over 18 databases in pages of 5 and checks every
database is visited once per cycle (fork woken, piled-up manifests and, since D77, a rewritten
fork still holding its source requested, leftovers swept) and
that the next run starts over. `compactor.test.ts` shows collection due `gcMinAge` after a
compaction, run by the alarm, then idle with no alarm. The `gc` scenario asserts that the
compactor's alarm collected (`GarbageCollector.collect` under `Compactor.alarm` with
`gc.deleted_ssts=4`) and that collect-now runs on the compactor.

Measured on `preview-1` with `--dev-timings` (`gcMinAge` 10 s): 500 databases, each flushed four
times so each compacted. All 500 were collected by their own compactors (1,500 collection runs), and
the sweep paged in runs of at most 50 databases at ~160 ms CPU each. Of 15,535 invocation records,
15,517 had outcome `ok`; 18 were `canceled` (Durable Object alarms and Workflow RPC calls, at most
213 ms wall and 11 ms CPU, ended by the runtime rather than by a limit). None was `exceededCpu` or
`exceededMemory`.

## D61. Writer throughput and per-invocation limits: measured, then fixed where it was spent

v1 workstream 6 measured first, then fixed what the measurements showed.

**What the writer spent its CPU on.** Before D61, every write ran `scheduleFlush`, which ran
`MemTable.stats`. That is a scan of the whole unflushed memtable, so its cost grew with the
memtable. `DbWriter` now keeps the commit time of the oldest unflushed row in memory, beside the byte
counter it already kept. A commit sets it if none is set. Start-up and every flush refresh it from
SQLite with one scan. `scheduleFlush` reads only the two counters. On `preview-1` with production
settings:

| Batch rows | Before (rows/s) | After (rows/s) | Client latency p50 before / after |
| ---------: | --------------: | -------------: | --------------------------------- |
|        100 |           2,454 |          6,547 | 305 / 105 ms                      |
|      1,000 |           7,678 |         15,257 | 2,057 / 961 ms                    |

After the fix the writer was busy 47–48% of the time, so these rates are bounded by the client and
the round trip, not by the writer.

**No binary batch body (design §11 settled).** `tools/writer-bench` splits the writer's in-process
write path into its parts. JSON decoding of the batch is 14% of it at both 100 and 1,000 rows. Live
writer CPU per 100-row batch is now 6 ms p50. A binary internal body would save at most about 14%
of a component that is not the bottleneck. It would also add a second batch codec to keep in step.
The workstream's target, doubling the 100-row rate, was met without it (2.7×). The public body stays
base64 JSON, and the internal `WriterHttpApi` body stays the same schema. An additive
`application/x-shaledb-batch` body remains possible later if callers need it.

**Flush paging no longer builds a temp B-tree per page.** D58's flush queries bounded `seq` with
`seq > ? AND seq <= ?`. On a 60 MiB memtable SQLite chose the `seq` index and sorted each page in a
temporary B-tree, so one flush exceeded the 30 s CPU limit (`exceededCpu`). The flush queries
(`MEMTABLE_FLUSH_PAGE_QUERIES`) now write the bound as `+seq`, which stops SQLite from using that
index. Pages resume from the last `(key, seq)` with `key >= ? AND (key > ? OR seq < ?)`, which walks
the primary key in order. A query-plan test asserts that the flush queries use the primary key and
no `TEMP B-TREE`; reverting the fix fails it. 60 MiB flushes then cost 1.2 s CPU at p50 and 4.3 s
at most.

**Compaction reads inputs in 2 MiB chunks.** Compaction read one `getRange` per 4 KiB block, so a
64 MiB step made thousands of requests and failed with "Too many API requests by single Worker
invocation". lsm-core's `groupBlockReads(index, blocks, maxBytes)` groups consecutive blocks into
reads of at most `COMPACTION_READ_CHUNK_BYTES` (2 MiB; a single larger block is read alone).
`CompactionExecutor` decodes each chunk's blocks in memory. A 64 MiB input is now 32 reads. A
property test checks that the reads cover exactly the requested blocks, in order, each contiguous
and within the bound; dropping either the contiguity or the size check fails it. Four 60 MiB L0
SSTs then compacted in four steps (9.0 s CPU for the step measured after the fix; 17.7 s at most
across a run) with every outcome `ok`.

**CPU limits come from the Stack configuration (P5).** `ShaledbStackConfig.limits` sets
`apiCpuMs` (default 30,000) and `compactorCpuMs` (default 60,000). Both are bound as `limits.cpu_ms`
on the API Worker, whose writers run under it, and on the compactor Worker.
`SHALEDB_API_CPU_MS` and `SHALEDB_COMPACTOR_CPU_MS` override them, from 10 to 300,000 ms;
other values are refused. The defaults are the largest measured invocation times two:

- **Writer:** a 256 MiB flush peaked at 9.8 s, so 2× is 19.7 s and fits the 30 s default.
- **Compaction:** a step peaked at 17.7 s, so 2× is 35.4 s, which is above 30 s. The limit is raised
  to 60 s.

These values are read only while planning (`Alchemy.ALCHEMY_PHASE`). A Worker's props Effect also
runs inside the deployed Worker, where resolving the deploy environment threw (error 1101 on every
request) the first time this was deployed.

**Design §12's two open questions, measured** on `preview-3` with production settings plus
measurement overrides (`preview-cli deploy --settings`):

- **Memory.** One writer held 266,954,000 bytes unflushed (`l0SstSize` 256 MiB) and then flushed
  it. All 174 writer invocations were `ok`, with no `exceededMemory`. Five writes at the cap were
  refused with `WriteStalled`, which is the intended backpressure. So Durable Object SQLite at
  `maxUnflushedBytes` fits, whether or not its page cache counts against 128 MB.
- **Workflows.** 200 databases started compactions at once (`compactionBudgetCapacity` 1,000). All
  200 finished within 16.4 s, and 1,848 compactor invocations were `ok`. 200 simultaneous instances
  are within the plan's limit; production's budget (32) keeps far fewer running.

**Also:** `GET /databases/:id/status` now reports `l0Count` from the newest manifest. The writer's
own view lags a compaction, so a poller waiting for L0 to drain never saw it drain.

## D62. Corruption: prevented at upload, repaired by a confirmed drop through the compactor

D53 made a corrupted input a reported compaction outcome. Nothing prevented corruption at upload,
and there was no supported repair: a corrupted L0 SST paused L0 compaction until writes stalled.

**Integrity at upload.**

- **Single-part puts** (`put`, `putIfAbsent`, `putIfVersion`, and a multipart upload that fits in
  one part) send the body's SHA-256. R2 refuses a mismatch, and the write fails and is retried like
  any storage failure.
- **Multipart parts.** R2's `uploadPart` takes no checksum, so each part is checked after the fact
  (`uploadPartVerified`). A probe through R2's S3 API showed that R2 returns a part's MD5 as its
  ETag and refuses a wrong `Content-MD5` with `BadDigest`. A part whose ETag differs from the MD5
  of the bytes sent is uploaded again under the same number, up to three times; after that the
  upload fails and is aborted. The re-uploads are counted in `lsm_object_part_integrity_retries`.
- **Unverifiable ETags.** Local R2 (Miniflare) returns opaque ETags, so an ETag that is not an MD5
  cannot be checked. Such a part is accepted, counted in `lsm_object_parts_unverified`, and shown
  as `object.parts_unverified` on `ObjectStore.putMultipart`. On `preview-1`, 134 parts across 16
  uploads of 64 MiB SSTs were all verified.
- **How the MD5 is computed.** The digest is WebCrypto's native MD5, which the Workers runtime
  offers as a non-standard algorithm. Where WebCrypto has no MD5 (Node, in tests), a private
  `md5Hex` in lsm-core is used instead; it is tested against RFC 1321's vectors and `node:crypto`.
  The private JavaScript MD5 was tried first in workerd: it raised 60 MiB flush CPU from p50 1.2 s
  to 3.7 s (max 16.7 s). With the native digest it is back to p50 1.0 s (max 3.9 s).
- **The in-memory store** runs the same check. It stores each part as it "arrived" and gives it
  that MD5 as its ETag. `objectStoreWithTransitCorruptionTestLayer` damages chosen parts.

**A supported repair: drop the SST, explicitly and typed.**

- **The edit.** `dropSstFromManifest` (lsm-core) is a pure edit. It removes one SST from L0 or from
  its sorted run, and removes a run left empty. It records the drop in the manifest's new
  `droppedSsts`, which is additive: older manifests decode with none, and only the newest 32 drops
  are kept. Each entry holds the SST, its owner, where it sat, its key range and view, its row
  count, the reason (`Corrupted` or `Forced`), and the time. Epochs and sequence numbers are
  unchanged. Repeating a drop changes nothing. The object is left to GC like any replaced SST, so a
  reader holding an older manifest is unaffected.
- **The compactor makes the change** (`Compactor.dropSst`), as it does for collect-now (D60). A
  drop runs under a new planning lock that `poll` also takes. It refuses an SST whose L0 SST or
  sorted run a running job reads (`CompactionRunning`): that job's output would bring the rows
  back, and for a sorted run, `commitCompaction`'s exact-inputs check would not notice, because the
  run id survives. Without `force`, it refuses an SST no compaction reported
  (`NotReportedCorrupted`). After committing, it forgets the report and polls, so compaction
  resumes.
- **The lifecycle checks the confirmation.** `DatabaseLifecycle.dropSst` requires `confirm` to
  repeat the SST id (`ConfirmationMismatch`), then delegates to the compactor.
- **Public endpoints:**
  - `POST /v1/databases/:id/ssts/:sstId/drop` with `{confirm, force?}`. It returns
    `{manifestId, dropped}`, `404 SstNotFound`, or `409 SstDropRefused` with a `reason`; only
    `CompactionRunning` is retryable.
  - For the runbook's restore-or-drop decision: `GET /v1/databases/:id/compaction` now also reports
    `corruptSsts` (each with the SST's `createdAt`), `l0SstIds`, and `droppedSsts`, and the new
    `GET /v1/databases/:id/checkpoints` lists checkpoints. A checkpoint created before a corrupted
    SST can restore its data by a full-range fork.
- **Runbook.** `docs/operations.md` ("Corruption runbook"): find the report, decide between
  restoring and dropping, run the drop, and verify.

**The probe Worker leaves production.** The `repair` scenario corrupts a real SST through a new
fault-injection endpoint, `POST /v1/probe/damage` on the probe Worker. That Worker is a test tool,
so the Stack now deploys it only on local and preview stages (`deploysTelemetryProbe`). Production
has no probe, and its `probeUrl` output is absent.

**Formal model.** `GcSafety.tla` gains `DropSst(s)`, a commit that removes a reference and adds
none, and the broken variant `dropDeletesObject` (the drop also deletes the object at once). That
variant fails `ReadableManifestsComplete`. No other model changed (see the skills' notes).

**Tests:**

- `object-store.test.ts`: a part damaged once is re-sent; a part damaged every time fails the upload
  and stores nothing; the ETag is compared quoted or not; an opaque ETag counts as unverified.
- `md5.test.ts`: RFC 1321 vectors, a property against `node:crypto` from any buffer offset, and a
  whole 8 MiB part.
- `manifest.test.ts`: drops from L0 and from a sorted run, idempotence, refusals, the history cap,
  and decoding older manifests.
- `compactor.test.ts`: after the D53 corruption, L0 stays blocked at five SSTs. The drop is refused
  for a healthy SST without `force`, for an unknown SST, and while a job reads the source. It is
  then accepted, a repeat returns the same drop, the four healthy SSTs compact, and only the
  dropped SST's key is gone. Removing each refusal or the report's removal fails the test.
- `writer.workerd.ts`: a 12 MB flush uploads in parts against local R2.
- The `repair` scenario passes locally and on `preview-1`.

## D63. Production telemetry: redacted at the source, sampled by trace id, expired, and summarized

Traces were built for debugging previews. The HTTP middleware put the caller's address, user agent,
full URL, and query string on server spans. Every request was traced, traces were kept forever, and
the probe Worker would have shipped to production (removed by D62).

**One trace policy for every Worker** (`withLsmTracePolicy`, `lsm-trace-policy.ts`).

- **The hook.** Alchemy builds each event's OTLP exporters from the bound destinations. A Layer
  registered with `Telemetry.layer` is merged over them, and its `Tracer` wins. At runtime the
  policy registers a Layer that builds those same exporters and wraps their tracer, so every span an
  event creates goes through the policy, including the middleware's server span. The bound
  exporters are also built and stay idle.
- **What did not work.** Providing a new `Telemetry` reference inside a Worker's init does not
  reach the bridge: it reads the shared build's context. A local scenario run showed the
  attributes still exported, which is how this was found.
- **At plan time** the policy only binds.

**Redaction at the source.** `isExportedSpanAttribute` allows two kinds of attribute:

- the project's namespaces (`database.`, `sst.`, `manifest.`, `object.`, `compaction.`, ... and
  `seq`), which carry only identifiers, sizes, counts, and outcomes;
- `http.request.method`, `http.response.status_code`, `http.route`, `url.path`, `url.scheme`,
  `server.address`, `server.port`, and `error.type`.

The wrapped span records nothing else. That drops `client.address`, `user_agent.original`,
`url.full`, `url.query`, and request and response headers. A new project namespace must be added
to the list, or its attributes are dropped. The test enumerates both kinds.

**Head sampling by trace id.** A span that starts a trace in this process is kept when its trace
id's low 32 bits, as a fraction of 2^32, fall below the sample rate (OpenTelemetry's
trace-id-ratio sampler). Such a span has no parent, is an explicit root, or has a `traceparent`
parent from another process. Spans under a local parent inherit, as Effect does. Every Worker and
Durable Object decides the same for the same trace, so traces stay whole across hops without
trusting the incoming sampled flag.

The first version drew a random number at each root and honoured an incoming sampled flag, as W3C
parent-based sampling does. On `preview-3` at 25%, it kept all 200 traces: the client's Effect
`HttpClient`, like any instrumented client, sends `traceparent` with the sampled flag on every
request. Decided by trace id, it kept 45 of 200 (22.5%), and all 45 were whole: the API Worker's
`WriterClient.write` and the writer Durable Object's `DbWriter.write` were in the same trace, and
neither side appeared without the other. A caller's `traceparent` still joins our spans to its
trace.

The rate is `ShaledbStackConfig.telemetry.traceSampleRate`: 0.1 in production, 1 elsewhere (P4).
It reaches every Worker as one more always-bound worker setting, `SHALEDB_TRACE_SAMPLE_RATE`;
an unbound Worker keeps every trace. `preview-cli deploy --trace-sample-rate` sets it for a
measurement deploy.

**Retention.** The trace bucket has lifecycle rules that delete `traces/` and `tails/` after the
configured days (14). The value is read at plan time only (`shaledbTraceRetentionDays`), like the
CPU limits (D61). The LSM bucket aborts multipart uploads still open after a day; a Worker that
dies mid-upload otherwise leaves its parts stored. Both are confirmed on `preview-3` through the
Cloudflare API.

**What to watch.** `docs/operations.md` ("What to watch") lists the signals and when to act.
`scenario-cli measure health --stage <s> --minutes N` summarizes a stage from its unsampled tail
records and from its spans, pulled into a per-stage store (`trace-cli pull --since-minutes`). It
reports:

- invocation outcomes and CPU per Worker,
- refused writes by error type (only `WriteStalledError` is an issue),
- flush durations,
- the L0 backlog (the new `manifest.l0_ssts` on `Compactor.poll`),
- currently corrupted SSTs (each database's latest poll),
- GC and sweep failures, and
- the top error spans.

Its first run over two hours of `preview-1` found the seven `exception` invocations of the D61
deploy incident, and found the `repair` scenario's corruption, which it stopped reporting once the
drop had repaired it. It now reports healthy for the last hour.

**Also.** The SST corruption messages now point to the runbook (restore from a checkpoint created
before the SST, or drop it) instead of "recompact its inputs", which no tool does.

**Tests:**

- `lsm-trace-policy.test.ts` covers:
  - the allowlist;
  - all traces kept at rate 1, with only allowed attributes recorded;
  - about 25% kept at 0.25 over 2,000 roots, each child sharing its root's decision;
  - a caller's trace decided by its id whatever flag the caller sent;
  - a real `OtlpTracer` export that contains no dropped attribute.

  Removing the filter or the decision fails it.

- `lsm-worker-settings.test.ts`: the sample rate round-trips through the bindings and defaults
  to 1.
- `preview-stage.test.ts`: the flag passes through.
- The new `redaction` scenario sends a query string and a User-Agent, and asserts that every `GET`
  span carries the method and status and none of the four caller attributes. Before the fix it
  failed (three spans carried them). Across a whole local run (6,193 spans), only allowed keys
  appear and no span is exported twice.

## D64. Production readiness: rotatable tokens, production tasks, a checked-in contract, format versions

**Rotatable access tokens (P1).** Each live stage has two token slots, A and B. Each slot is a
Secrets Store secret in the account's `default_secrets_store`, named
`SHALEDB_<STAGE>_ACCESS_TOKEN_<SLOT>`.

- **Binding.** The Stack binds both slots to the API, GC, shard-router, and probe Workers by
  reference (`Secret.lookup`). Alchemy never writes their values. Its `Secret` resource would write
  its stored value back whenever it reconciled, silently undoing a rotation, so the values live
  outside Alchemy state.
- **The gate** (`lsmAccessGateFrom` over an `LsmAccessTokenSource` the Stack supplies, as D55 made
  it supply settings) does the following:
  - reads the slots at runtime and re-reads them every 30 s;
  - keeps a value it read before when a read fails, and stays closed when it has read none;
  - accepts a request presenting any accepted token, comparing against each in time that depends
    only on the lengths.
- **Previews** also keep their state-held token. `preview-cli token` and the scenarios use it.
  Production has no readable token: its tokens are printed once, when created or rotated.
- **`vp run @shaledb/infra#tokens`** manages the slots with the Alchemy profile's credentials:
  - `ensure <stage>` creates the missing slots; `preview-cli deploy` runs it first.
  - `check <stage>` fails unless both slots exist. The production deploy runs it, so a
    production token is never created inside a deploy's log.
  - `rotate <stage> <A|B>` replaces one slot's token.
- **Live check on `preview-1`.** A rotated token was accepted and a junk one refused. Right after a
  second rotation, both the old and the new token were accepted, which is the 30 s refresh window.
  After 35 s the old token got 401 and the new one passed, and the state-held token kept working
  throughout.

**Production tasks.** `plan:production` and `deploy:production` (infra, no cache) plan and deploy
the `production` stage. They give it:

- production names, including the retained `shaledb` bucket;
- 10% sampling;
- no probe Worker (D62);
- a trace collector, with 14-day expiry (D63).

A test pins production's resolved configuration, and the stage helpers keep the probe and the
state-held token out of production. Nothing runs these tasks until P6.

**A checked-in contract.**

- **Generation.** `vp run generate:openapi` writes `docs/openapi/shaledb-v1.json` from
  `DbHttpApi`, then completes it in `db-openapi.ts` with what the contract's schemas cannot carry.
- **Examples.** Every operation's examples are typed as decoded values and encoded through the
  schemas they exemplify, so they conform by construction. They cover every parameter, request,
  and success response, plus error examples per status.
- **Where examples live.** Effect drops `examples` annotations on schemas with transformations
  (base64 bytes, `Option` from null), and the typed OpenAPI model has no example field on media
  types or parameters. The examples therefore go in the JSON Schema `examples` keyword of each
  schema, which the open `JsonSchema` type allows, with no widening or assertion.
- **Also completed:** the bearer scheme, `x-request-id` on every response (the request middleware
  and the gate set it on all of them), and the local server URL only. No production hostname is
  invented.
- **The test** checks that the generated spec equals the checked-in one, so a contract change
  without regenerating fails `vp run ready`. It also checks that every operation has a summary and
  a description and that everything a caller sends has an example.
- **Descriptions** were added where four operations lacked them.
- **`docs/api-compatibility.md`** states what `/v1` promises: additive changes only within `/v1`.

**Format versions.** Two pieces of stored data carry a format version:

- **The manifest:** `MANIFEST_FORMAT_VERSION`, with older manifests decoding as 1.
- **The compactor state:** `COMPACTOR_STATE_FORMAT_VERSION`.

`ManifestReader`'s single decode point refuses a manifest whose format is newer than the code's,
with `StoredManifestInvalid`, naming the format. Every write starts from a load or from an
already-checked view, so code never rewrites newer data in an older format. A compactor on newer
state refuses to wake and its alarm does nothing.

A real manifest written by v1 workstream 1's code, read back from `preview-1`'s bucket, is checked
in as a fixture. It decodes as format 1 with no dropped SSTs, and writing it again keeps everything
it held. Tests show that a format-2 manifest is refused for reads and writes, with nothing written,
and that a compactor on format-2 state compacts nothing. Removing either check fails its test.

**Documentation and cleanup.**

- **`README.md`:** what shaledb is, a quick start, the API, and the repository layout.
- **`docs/operations.md`:** now covers deploying, rolling back, tokens, data residency, GC and
  removal, and capacity, alongside the telemetry, watch list, and corruption runbook.
- **Removed:** `lsmAccessGateForStage`, which described the gate as a preview stand-in for
  Cloudflare Access.

## D65. The soak and the production smoke test

**`scenario-cli soak`** (`soak.ts`) is the release gate's long test. It runs, concurrently, for
`--minutes`:

- **Writers**, one per database: batches of 20–140 puts and deletes on a fixed key space.
- **Readers**: random keys at `Latest`, `AtLeast` the last acknowledged sequence number, and
  `Flushed`.
- **Scanners**: ranges at `Latest`.
- **Transactors**: `SerializableSnapshot` increments of a per-database counter.
- **Snapshotters**: a pinned snapshot every few minutes, whose sampled keys are read at the snapshot
  four minutes later, then the checkpoint deleted.
- **A remover**: creates a short-lived database, uses it, removes it, and checks that it no longer
  answers.
- **A sharded group**: written through the router and split twice (`--split-at`).
- **A redeploy** of the stage midway (`--redeploy-at`), which evicts every Durable Object while the
  workload continues. It keeps the stage's jurisdiction.

**The model.** It records every acknowledged write as (sequence number, value), keeping each key's
last few, and the values of the batch in flight.

- **Concurrent reads.** A read that races a write is judged against what it may see: the value
  acknowledged when it started, anything acknowledged since, or the batch in flight.
- **Retried writes.** A failed write is retried as the same batch until acknowledged, so the final
  value is deterministic. Its keys are excluded from snapshot comparisons, because a replayed batch
  may have applied twice.
- **Counters.** A transaction whose commit outcome is unknown widens its counter's allowed final
  range by one.

**At the end** the soak checks:

- every key of every database at `Latest`, then after a flush at `Flushed`, against the model,
  with no key read that was never written;
- every counter;
- every key of the sharded group.

It also checks storage: after two collect-now runs, no database stores more SSTs older than
`gcMinAge + gcInterval + 10 min` than its manifest uses. The count comes from a new probe endpoint
(`GET /v1/probe/storage`, preview and local stages only).

**Pass criteria:**

- no mismatch and no failed operation;
- `measure health` reports no issue over the soak's window (no `exceededCpu`, `exceededMemory`, or
  `exception`, no current corruption, no GC failure, no stalled write);
- no error span with an exception outside the ones the workload provokes on purpose
  (`DatabaseSealedError` from router retries, `DatabaseNotFoundError` after removals,
  `TransactionConflictError`, `WriterFencedError` across the redeploy, and the `HttpClientError`
  calling side of those).

p50 and p99 client latencies are recorded per operation.

**How the soak reads spans and treats requests.**

- **Sampling.** The soak runs at production's sampling (`--trace-sample-rate 0.1`, which its
  redeploy keeps). At 100%, a busy stage exports about 40 traces a second, faster than they can be
  read back: on `preview-2` a 500-export page covering 11 s of traffic took 14 s to read.
- **Pulling.** It pulls spans every minute while it runs, without blocking its other fibers.
- **Failed pulls.** A failed pull, or a window with no spans read, fails the soak instead of
  passing silently. An earlier run reported no error spans because its pull had failed.
- **Timeouts.** Every request has a 90 s timeout, which counts as a retryable failure. Retries,
  timeouts, and failures are logged with their time, so they can be found in tail records and
  traces.

**Telemetry never holds up the soak.** A 130-minute release soak ran its workload and
never reported: its tail-record watcher was one of the workload's fibers, and one read paged for
as long as records kept arriving, so the workload could not end. It was stopped after 3 h 24 min
with nothing written. Since then (`soak-telemetry.ts`, `soak-report.ts`):

- **The watchers stop with the workload.** The tail-record watcher, the span puller, and the
  report writer run beside the workload and are interrupted when it ends, wherever they are
  (`whileWorkloadRuns`). An interrupted HTTP read is aborted; an interrupted `trace-cli` run
  (`pull`, `grep`) is killed.
- **Every telemetry read has a budget** (`readWithin`): 5 minutes while the workload runs, and
  `--telemetry-budget-minutes` (default 10) for each final read: the tail-record catch-up, the
  health report, the span pull, and each span search. A read that runs out is a failure that
  says how far it got (tail records read, the time of the newest one), and the next check runs.
  Pages read before a cut-off stay counted: a page is folded in and the cursor moved past it in
  one synchronous step (`TailTally.foldPage`).
- **Data first.** The final checks run in order: `final-reads` (every key at `Latest` and
  `Flushed`, counters, the sharded group), `storage`, then `telemetry`.
- **A report that survives.** `--report-file <path>` keeps the report as known so far, with its
  `phase` (`workload`, `final-reads`, `storage`, `telemetry`, `done`) and `updatedAt`. It is
  written to a temporary file and renamed over the path, every minute during the workload and
  after each phase, so a stopped run leaves its last complete report. `passed` is `true` only
  when `phase` is `done` (`soakPassed`). Stdout still prints the final report.
- **The exit status is the verdict.** The command exits 0 only when `passed` is `true`
  (`soakFailureMessage`). Before, it exited 0 whenever it could print a report, so a script
  could take a failed soak for a passed one (seen on the first live run of the rebuilt tool,
  which printed `passed: false` and exited 0).

Tests: `soak-telemetry.test.ts` (a source that never ends is cut off at its budget and the
failure says how far it got; a watcher in the middle of an endless read is interrupted when the
workload ends or fails), `soak-report.test.ts` (phases, the rename of a complete temporary file,
the previous report kept when a write dies before the rename; the verdict), and
`scenario.test.ts` (an interrupted child process is killed).

**`scenario-cli smoke`** (`smoke.ts`) is the production smoke test. It runs only operations safe on
a live installation, on databases it names itself (`smoke-<time>-…`), which it removes at the end
whatever happened. It checks:

- create, write, and reads at `Latest`, `AtLeast`, and `Flushed`;
- scan;
- a transaction, and a conflicting one refused;
- a checkpoint created, listed, and deleted;
- eight concurrent creates of one id, of which exactly one wins.

It makes no trace assertions, since production keeps a sample of traces. It takes `--api-url` with
the token in `SHALEDB_TOKEN` (production), or `--stage` (previews). On `preview-3` it passed
both ways, the second with a freshly rotated Secrets Store token.

## D66. A Worker's caches share values across requests, never reads in flight

**Found by the soak.** The first short soak on `preview-2` (`eu`) lost no data, but:

- three API Worker requests hung about 301 s until the client gave up (outcome `canceled`);
- one `get` failed with `InternalFailure` (500) during the redeploy.

The error span named the cause:

> Cannot call this AsyncLocalStorage bound function outside of the request in which it was created.

**The cause.** It was raised in `BlockCache.meta`. The L1 block cache joined concurrent misses for
one key into one read, through a `Deferred` in an isolate-wide map, across requests. In a Worker,
I/O belongs to the request that started it, which gives two failures:

- **Hangs.** If the runtime cancels that request (a client that hangs up, an isolate retired by a
  deploy), its read never finishes, and every request waiting on it hangs.
- **The 500.** When the read does finish after its request ended, resuming the waiter runs code
  bound to the ended request, which throws.

The unit tests ran in one process with no request boundaries, so they could not see this.

**The fix.** The L1 caches keep sharing completed values (decoded metadata and blocks, plain data)
across requests in the isolate, and no longer share reads in flight. Each miss reads with its own
request's services. Duplicate concurrent misses cost a duplicate read, which Workers Cache (L2) and
the `BlockServer` answer.

**The rest of the engine.** Every other cache that runs I/O is per request (`makeExecutionMemo`),
the manifest cache holds plain values, and the shard directory's `Deferred` signals are inside a
Durable Object, where requests share one context.

**Test.** `block-cache.test.ts` starts a read that never finishes, then a second read of the same
block, which must complete within a second. It fails on the previous code and passes now. The old
test that asserted joining was replaced by one asserting that a block, once read, is kept.

## D67. A read the writer answered uses any manifest holding its flushed rows; scans read ahead; 16 KiB blocks

**What the soak's spans showed** (`preview-3`, 100% sampling, a 5-minute soak): 82% of gets
reloaded the manifest, two R2 round trips of about 90 ms together, because a reader's cached
manifest was only good for `manifestRefreshInterval` (1 s) and most isolates see a database less
often than that. A get took 124 ms at p50. Scans waited on one block read at a time.

**A read that asked the writer needs no fresh manifest.** A `Latest` read's probe returns the
writer's `flushedSeq`. Any manifest with `lastL0Seq >= flushedSeq` holds every flushed row; later
manifests differ only by compactions, collections, and checkpoints, which change no row a read
resolves. So such a read (`ReadView.freshness: "Covering"`) takes the cached manifest whenever it
covers `flushedSeq`, however long ago it was refreshed (`ManifestCache.covering`). The cache
follows the writer's flushes: the first read after one finds the cached manifest too old and
loads.

- **How long a manifest is held.** A cached manifest was the newest when its load began, and is
  read for at most half of `gcMinAge` after that (30 minutes in production). GC keeps the SSTs of a
  manifest for `gcMinAge` after it is superseded (D29), so everything it names is still stored, with
  the other half of `gcMinAge` to spare for the read itself. This is inside `GcSafety.tla`'s
  `MayBeHeld`.
- **A hit vouches for the manifest.** The writer has just said the cached manifest holds every
  flushed row, so the cache records that (`confirmedAt`), and `Flushed` reads in the same isolate
  use it for another `manifestRefreshInterval` without loading.
- **`AtLeast` keeps its bound.** An `AtLeast` read skips the writer only when the manifest a
  `Flushed` read would use right now, without loading one, already covers its sequence number
  (`ManifestCache.recent`). Otherwise it asks the writer, as a `Latest` read does. Before, it
  loaded a manifest to find out (about 90 ms on most reads); a probe costs about 10 ms and returns
  the newest data. Served without the writer, it is exactly as old as a `Flushed` read may be, as
  design §4 says.
- **`Flushed` reads, snapshots, and transaction reads are unchanged** (`freshness: "Recent"`, or
  a pinned manifest).
- **A missing SST drops the cached manifest.** A reader that finds an SST of its manifest gone
  forgets the manifest (`ManifestCache.forget`) before failing with the retryable
  `StorageUnavailable`, so the retry loads the newest. Nothing above should let this happen; it
  makes the error's "retry the read" true if something does.

**The writer says where to look.** The probe and the memtable scan return `flushedManifestId`,
the manifest the writer's last flush committed (an optional field on the internal writer API, so
an older writer decodes as none). On a miss the reader calls `ManifestReader.loadLatestFrom`: it
reads GC's manifest boundary, that manifest, and the id after it together, and probes forward
while a next id exists. When the flush's manifest is still the newest, that is one round trip
instead of `loadLatest`'s two or three. At or below the boundary, or no longer stored, it falls
back to `loadLatest`.

**Found in review, before this was committed.** The first version loaded the hinted manifest
itself and cached it for half of `gcMinAge`, and let an `AtLeast` read use any cached manifest
that young. Both were wrong, and the soak could not have shown either: it writes constantly, and
its `AtLeast` reads carry a receipt just issued.

- **The hinted manifest is usually not the newest.** A compaction supersedes a flush's manifest
  within seconds. A database written, compacted, and then idle would hand a new isolate that
  manifest up to an hour after it was superseded, and the isolate would keep reading it for
  another 30 minutes while GC deleted its SSTs: retryable failures that no retry fixes. The
  safety argument needs "newest when loaded", so the hint is only where the search starts.
- **`AtLeast` could be 30 minutes stale.** A receipt from long ago is covered by any cached
  manifest, so the read would have skipped both the writer and every flush since.

**Reads open their SSTs together.**

- **Point reads** fetch the metadata (index and Bloom filter) of every SST that may hold the key
  at once, before checking any filter. A cold isolate otherwise waited on one metadata read per
  SST in turn.
- **Scans** fetch the metadata of every overlapping SST at once, and the merge pulls every
  input's first chunk concurrently (`mergeRowStreams`). The heap still breaks ties by input
  position, so the output is the same.
- **Scans read ahead** `SCAN_READ_AHEAD_BLOCKS` (8) blocks per SST while the merge consumes it.
  The merge pulls one source at a time, so without read-ahead a scan waited on one block at a
  time. A short scan wastes at most that many reads per SST.

**Blocks are 16 KiB** (`blockSize` default, from 4 KiB). Each block a scan or a compaction reads
is one request, and a point read fetches one block whatever its size. Larger blocks cut a scan's
block reads about threefold (p50 16 → 6 on the same workload) at no cost to gets. Readers follow
each SST's own index, so SSTs written with another block size stay readable; nothing is rewritten.

**The trace collector serves pages of 500 exports** (from 100), and the measurement tools read
tail records in pages of 500 with retries, so a busy stage's telemetry can be read back as fast
as it arrives.

**Model.**

- `NoAckedWriteLost.tla` already lets a `Latest` read use any manifest with `lastL0Seq >=
  flushedSeq` (`ReadManifest`); `LatestSeesAcked` covers the covering cache as it is.
- `GcSafety.tla`'s `MayBeHeld` (the newest manifest, or one superseded less than `gcMinAge` ago)
  covers the hold. No step changed.
- `ManifestCasFencing.tla` gained `LoadLatestFrom(k)` and the invariant `LoadFromKnownFindsHead`:
  from any manifest a commit was reported at, the reader finds the newest, whatever was committed
  since and whichever manifests GC deleted. The new broken variant `knownIgnoresBoundary` (probe
  from the known manifest even at or below the boundary) fails it: a checkpoint keeps the known
  manifest while GC deletes its successor. `vp run verify` is now 27 runs.
- `Transactions.tla`: transaction reads keep `Recent` or a pinned manifest. No step changed.

**Tests** (`manifest-store.test.ts`, `db-client.test.ts`; each mutation below fails one):

- A covering read is served from the cache past the refresh interval, never from a manifest
  missing flushed rows, and never past half of `gcMinAge`.
- From the writer's last flush the newest manifest is loaded, never that flush's superseded
  manifest; a stale `LATEST` hint is not read; a collected, pinned-below-the-boundary, or
  never-written known manifest still finds the newest.
- A covering hit lets `Flushed` reads use the manifest for one more refresh interval, not longer,
  and not past the hold.
- An `AtLeast` read skips the writer within the refresh interval and asks it afterwards.
- A read that finds an SST missing fails once, and its retry succeeds.
- Mutations caught: returning the known manifest without probing forward; ignoring the boundary;
  ignoring `flushedSeq`; ignoring the hold in `covering` or in `recent`; holding for all of
  `gcMinAge`; `recent` ignoring the refresh interval; a hit that does not vouch; no `forget`;
  `AtLeast` never or always skipping the writer. One is not caught, by design: removing the
  reader's own check that the manifest covers the view, which `covering` already guarantees.

**Measured** on `preview-3`, server-side spans: see `docs/measurements.md` ("Read latency").

## D68. The project is named shaledb

Dillon renamed the project from cf-slatedb to **shaledb**.

- **Packages and paths.** `@cf-slatedb/*` became `@shaledb/*`, `infra/cf-slatedb` became
  `infra/shaledb`, and files named `cf-slatedb-*` became `shaledb-*`.
- **The Stack and its configuration.** The Alchemy Stack `CfSlatedb` is now `Shaledb`, and every
  `CF_SLATEDB_*` variable or binding is now `SHALEDB_*`.
- **Live names.** Worker and bucket names follow, as do the production LSM bucket (`shaledb`) and
  the Secrets Store token secrets (`SHALEDB_<STAGE>_ACCESS_TOKEN_<SLOT>`).
- **Unchanged.** References to SlateDB itself (the reference design, `repos/slatedb`, its RFCs)
  are untouched.

Alchemy keys state by Stack name, so the next deploy of each preview created a new `Shaledb/<stage>`
stack with new resources. The old `CfSlatedb/<stage>` previews and their Secrets Store secrets
remain until Dillon destroys them from a checkout before the rename (`docs/progress.md`). Nothing
was deployed to production under either name.

## D69. A request the endpoint's schemas refuse is `InvalidRequest` (400)

A body that was not JSON, lacked a required field, or held an id the schema refuses came back as
`InternalFailure` (500). Effect's `HttpApiBuilder` raises a request that fails its endpoint's
schemas as a defect (`HttpApiSchemaError`), and the request middleware turned every defect into a 500. A content type no endpoint accepts was answered by the builder itself, in plain text with
415, outside the error envelope.

- **Request decoding failures** (`Params`, `Headers`, `Query`, `Payload`) are now
  `InvalidRequest` (400), already declared for every endpoint. The message names the part and
  repeats the schema's issue on one line, capped at 300 characters ("Missing key at
  ["databaseId"]"). The issue describes the caller's own request and goes only to the caller; it
  is neither logged nor put on a span.
- **Response encoding failures** (`Body`, `ResponseHeaders`) are our fault and stay
  `InternalFailure`, logged.
- **An unaccepted content type** is `InvalidRequest` too, telling the caller to send JSON.

Test: `writer.workerd.ts` sends five malformed creates through the real Worker and expects the
envelope with status 400, the request id of the `x-request-id` header, and the part named.

**Also.** `DbWriter.ensureReady` answers from memory once the writer is ready, so a
`DbWriter.start` span now means an actual start (a fence-in), not every request. The scenario
runner reports what caused a failed request (the `cause` chain of a transport error).

## D70. A read never uses a manifest flushed past its snapshot; a flush writes only the versions a read can reach

**A race found by the model, present since Phase 5.** A `Latest` read asks the writer, which gives
it a snapshot (the newest sequence number), `flushedSeq`, and the rows above `flushedSeq`. It then
reads the SSTs of a manifest covering `flushedSeq` and keeps rows at or below its snapshot. Nothing
stopped it from using a manifest **newer** than its snapshot. A compaction keeps only each key's
newest version, so:

1. write 1 of a key is flushed;
2. a read asks the writer: snapshot 1, nothing unflushed;
3. write 2 of the same key is flushed, and a compaction merges both SSTs, keeping only 2;
4. the read loads that manifest, drops version 2 (past its snapshot), and finds nothing.

The read returns "not found" for a key that had a value at every moment. Both things in step 3
must finish between the writer's answer and the manifest load, usually tens of milliseconds, and no
soak or scenario has shown it. But it was reachable, and `NoAckedWriteLost.tla` could not see it:
the model left compaction out.

**The rule.** A read at snapshot `S` uses a manifest `M` only if `M.lastL0Seq <= S`. Then every SST
in `M` holds, for each key, its newest version at or below `S`, whatever flushes and compactions
dropped. `ReadView.maxLastL0Seq` already enforced this for transactions; every view built from a
writer's answer now sets it to the snapshot (`DbClient.pointView`, `scanView`).

- **A cached manifest usually fits**: it was loaded before the writer was asked, or by a
  concurrent read in the same isolate, which the check catches.
- **A manifest loaded after the answer may not.** The reader then fails with `SnapshotExpired`,
  and `DbClient.getRow` and `scan` ask the writer again, up to three times
  (`LATEST_READ_ATTEMPTS`). Each further time needs another flush to commit in that same short
  window. After three, the read fails with the retryable `StorageUnavailable`.
- **A scan across a flush.** A scan reads the writer's unflushed rows in pages. If a flush commits
  between two pages, the later pages no longer hold its rows, and the manifest that does was
  flushed past the snapshot. The view's lower bound (the largest `flushedSeq` of its pages) is
  then above its upper bound, no manifest fits, and the scan starts again. The reader checks the
  manifest before it emits a row, so no row is returned twice.
- **`Flushed` reads and snapshots** read a manifest at its own `lastL0Seq`, which always fits.

**A flush writes only the versions a read can reach** (`keepReachableVersions`, lsm-core). With
the rule in place, every read of the SST a flush writes is at a snapshot at or after the flush's
newest row. A read at an older snapshot takes those rows from the writer, which keeps them (D58),
or reads an older manifest. `resolveVisibleRow` walks a key's versions newest first and stops at
the first value or delete, so a flush keeps each key's rows from the newest down to and including
the first that is not a merge operand, and drops the rest. Time and merge operators do not matter:
an expired value still ends the walk.

- A key rewritten many times between two flushes is one row in the SST instead of one per write.
- A key written once, and merge operands with no value under them, are written as before.
- The changelog export is separate and still holds every change.

**Also fixed: a transaction scan without a begin manifest.** `DbClient.scanAt`, for a caller that
sends no `manifestId` (the API before D58, still accepted), built its view with `minLastL0Seq = 0`.
Within `manifestRefreshInterval` of a flush it could read a cached manifest from before the flush
while the writer's pages no longer held the flushed rows, and miss them. It now requires the
largest `flushedSeq` its pages reported, as `getAt` always did.

**Model.** `NoAckedWriteLost.tla` now has the versions of one key:

- `Compact`: a new manifest holding only the newest version;
- `FlushUpload` keeps the newest version of its window;
- the read's snapshot, the two bounds in `ReadManifest`, and `ReadAgain`;
- invariants stated for the key's newest acknowledged version.

The new broken variant `noSnapshotUpperBound` fails `LatestSeesAcked`, with the flush as it is now
and, checked once by hand, with the flush as it was (every version) plus compaction: the code
before this change. `vp run verify` is 28 runs.

**Tests.**

- `db-client.test.ts`: a client whose writer is overtaken after it answers (the key is rewritten
  and flushed, and the older SST leaves the manifest). A get and a scan return the new value; on
  the code before this change they returned "not found". Overtaken on every attempt, the read
  fails as retryable. A scan of 1,200 unflushed rows overtaken between its two pages returns all
  1,200.
- `db-transaction.test.ts`: the scan without a begin manifest, which fails on the old code.
- `row-resolution.test.ts`: for any history, any flush window, and any snapshot from the
  window's newest row on, at any time, reads over the kept versions equal the oracle.
- `db-writer.test.ts`: a key written twice before a flush is one row in the SST.
- **The client-versus-oracle property test was weaker than it looked.** Effect's
  `Arbitrary.array` with `minLength: 1` makes arrays of two elements at the median and ten at
  most, so that test ran histories too short to write a key twice between flushes and read it.
  It now runs 20 to 60 steps (as the transactions test has since Phase 9), which the code before
  and after this change passes, and which catches a flush that drops merge operands.
- Eleven mutations are each caught by one of these tests.

**Measured** on `preview-3`, a 5-minute soak at 100% sampling, the code before against all of
D70 to D73 (`docs/measurements.md`, "Read latency"): SST bytes stored per database for the same
workload fell from 4.4 MB to 0.44 MB, a scan's block fetches from 7 (p50) and 37 (p90) to 2 and
12, and scan latency from 76 / 555 / 2,146 ms (p50 / p90 / p99) to 60 / 257 / 983 ms. The soak's
workload rewrites 500 keys continuously, so it shows the most this can do; a workload that never
rewrites a key between flushes is unchanged.

## D71. Tail records are sampled like traces, except the ones that did not end `ok`

The Tail Worker stored one R2 object per invocation, unsampled (D48, D63).

- **It could not be read back as fast as it was written.** During the 2-hour soak, `preview-1`
  finished 72 invocations a second. The collector returns 500 objects in about 7 s (14 s on
  `preview-2`, read from the US), which is 71 a second at best. The soak's watcher never caught
  up, and its final read took as long again as the backlog was old: the soak's report arrived
  more than half an hour after its workload ended.
- **It would have been a production cost.** One R2 write per invocation is about 25 million
  writes a day at 100 requests a second.

The Tail Worker now keeps every invocation that did not end `ok` (a limit hit, an exception, a
cancellation: the ones a health report acts on) and, of the `ok` ones, the fraction the stage
samples traces at (`TAIL_SAMPLE_RATE`, bound from `telemetry.traceSampleRate`: 10% in
production, everything on previews). Each record carries the rate it was kept at (`sampleRate`;
records from before have none and count as 1).

- `measure health` and the soak scale `ok` counts back up (`invocationsRepresentedBy`), and report
  how many records they read. CPU percentiles are taken from the kept records.
- The measurements that need one particular invocation's CPU (`flush-cpu`, `compaction-cpu`,
  `writes`) run on previews at 100% sampling, where nothing is dropped.

Test: `cpu-tail-worker.test.ts` (every outcome but `ok` kept at any rate; the sampled fraction of
`ok`; an unset or unreadable rate keeps everything).

## D72. The read path, second round: refresh from the manifest held, hedged block reads, scans that follow their limit

Measured first, on `preview-3` with 100% sampling; each change below answers one finding. The
numbers are in `docs/measurements.md` ("Read latency").

**A manifest refresh starts from the manifest the reader holds.** A `Flushed` read refreshes its
manifest every second with `loadLatest`: the `LATEST` hint and GC's boundary, then the manifest
and the id after it, two round trips of about 110 ms together. `ManifestCache.refresh` now calls
`loadLatestFrom` with the manifest it holds, which reads the boundary, the hint, that manifest,
and the id after it in one round. On a database that committed nothing since, that is the whole
load.

- `loadLatestFrom` now also reads the hint. When the known manifest was superseded it continues
  from the hint, if that names a stored manifest further along, so a known manifest far behind
  costs no more than `loadLatest`.
- Model: `LoadFromKnownFindsHead` is checked from every stored manifest, with the hint jump
  (`KnownStart`); `knownIgnoresBoundary` still fails.
- `Flushed` gets: p50 81 ms → 24 ms.

**A slow block read is hedged with a read straight from the bucket** (`hedgedBlockRead`,
`blockReadHedgeAfterMs`, default 50 ms). A read through the `BlockServer` is answered by Workers
Cache in about 6 ms nine times in ten. The rest are slow in two ways: a miss pays the hop to the
`BlockServer` on top of its bucket read (86 ms at p50, 328 ms at p90, of which the `BlockServer`
itself is 44 and 82 ms), and a rare hit takes seconds (the slowest get of the run was one
2.3-second `HIT`). Once the `BlockServer` read has taken 50 ms, the reader also reads the same
bytes from the bucket, which every reader Worker already reads manifests from, and takes
whichever answers first.

- Only a success of the bucket read counts. Its failures leave the outcome to the `BlockServer`
  read, so a hedge never fails a read that would have been answered, and the loser is
  interrupted.
- The losing `BlockServer` read is interrupted, and its `fetch` aborted with it. Whether Workers
  Cache still keeps a block whose reader hung up was not determined. The share of `HIT` answers
  did not fall with hedging on: 69% without it and 82% with it, on `preview-3` (the table below).
- Block fetches, p50 / p90 / p99 / max: 7 / 96 / 439 / 1,896 ms → 7 / 74 / 101 / 159 ms. 21% of
  fetches were hedged and the bucket read won a third of those, so about a fifth more bucket
  reads.
- **No hedging in a restricted (`eu`, `fedramp`) Stack.** There the `BlockServer` runs without
  Workers Cache (D49), so both reads take the same road to the same bucket. Measured from 5-minute
  soaks at 100% sampling (found in review):

  | Stage                              | Fetches | Hedged | Won by the bucket | `getRange` spans | Fetch p50 / p90 / p99 (ms) | Workers Cache |
  | ---------------------------------- | ------: | -----: | ----------------: | ---------------: | -------------------------- | ------------- |
  | `preview-2` (`eu`), hedge at 50 ms |   1,494 |    90% |      2% of hedged |            2,437 | 180 / 771 / 1,030          | none          |
  | `preview-2` (`eu`), no hedge       |   1,553 |      0 |                 — |            1,148 | 159 / 593 / 1,144          | none          |
  | `preview-3`, no hedge (D67)        |   1,744 |      0 |                 — |              512 | 7 / 96 / 439               | 69% `HIT`     |
  | `preview-3`, hedge at 50 ms        |   1,706 |    20% |     34% of hedged |              773 | 7 / 74 / 107               | 82% `HIT`     |

  In `eu` the hedge doubled the bucket reads and won almost nothing. `hedgeBlockSource` therefore
  hedges only when the Stack has no jurisdiction, by the rule the `BlockServer` uses to turn
  Workers Cache on (`lsmStackHasWorkersCache`); `blockReadHedgeAfterMs` has no effect in a
  restricted Stack. A local stage has no jurisdiction and hedges, although Workers Cache is not
  emulated there: one rule for every stage, and the scenarios exercise the hedge.

- **A stated limit: queueing alone can start a hedge.** The 50 ms timer starts when the read is
  issued, not when it gets a connection. A Worker invocation has six connections waiting for
  response headers at most, and further `fetch` and R2 calls queue behind them. On a scan wide
  enough to queue, a hedge can start only because its read waited in that queue, and the hedge
  then queues too. The measured net effect on `preview-3` is in the table above. A cap on hedges
  in flight is not adopted: no measurement supports one yet.
- `block.hedged` and `block.hedge_won` are span attributes on the cache read that started them.

**Scans stop reading when they have what they asked for.**

- **The merge emits before it pulls an input again.** `mergeRowStreams` filled a batch of 256
  rows before emitting any, so a scan with a smaller limit still read blocks for 256 rows: in a
  test, a 3-row scan read all 40 blocks of an SST. A batch now ends when it is full or when an
  input's chunk runs out, and the input is pulled only when the consumer asks for more. The
  merge's order and its handling of exact duplicates are unchanged; for a compaction, which
  drains every input, it costs 12% of the merge's own time (0.06 s of a 10 s step).
- **The first read-ahead follows the limit** (`scanFirstReadAheadBlocks`): the blocks that hold
  `limit` rows at the SST's average rows per block, plus one, at most `SCAN_READ_AHEAD_BLOCKS`
  (8). Reading ahead 8 blocks of every overlapping SST made a 100-row scan over seven SSTs start
  51 block reads, and a Worker invocation has six simultaneous subrequests, so the blocks it
  needed waited behind the rest. A scan that needs more reads on, 8 at a time.
- With D70's flush, each SST holds one row per key, so the estimate is close. Scan block fetches
  fell from 7 (p50) and 37 (p90) to 2 and 12.

**What is left in a get's tail.** With these changes the reader's part of a get is 205 ms at p99.
A whole get's p99 stayed near 400 ms in the short soaks, because their top 1% is writer Durable
Objects starting: a fence-in costs about 0.5 s (a manifest load and a create-if-absent), and the
soak starts 12 to 14 writers in five minutes for the databases and shards it creates.

**Tests.** `manifest-store.test.ts` (the hint jump, a hint behind or naming nothing stored, one
round of reads for an unchanged manifest), `block-source.test.ts` (six cases of the hedge under
the test clock), `db-client.test.ts` (the short scan reads two blocks and reads on past deleted
rows; the read-ahead estimate), and the merge's property tests. Sixteen mutations caught.
Added in review: `block-source.test.ts` builds `hedgeBlockSource` over a `BlockServer` that never
answers and checks that block and metadata reads of a source database's SST answer from the
bucket at the configured delay and not before, that a delay of 0 and a restricted Stack never
read the bucket, and catches four mutations (no jurisdiction rule, the delay ignored, metadata not
hedged, the wrong bucket key).

## D73. The flush path: the compactor wake leaves it, rows are encoded a chunk at a time; concurrent part uploads declined

**A flush no longer waits for the compactor it wakes.** When a flush leaves enough L0 SSTs to
compact, the writer wakes the database's compactor. It did so inside the flush, which holds the
flush lock, and for a split's seal the commit lock as well. On `preview-3` the first call to an
idle compactor took 5.4 to 9.2 s to reach its handler, which then ran 0.3 to 0.5 s: a 60 MiB
flush that wrote its SST in 5.4 s and committed in 0.5 s took 12.8 s. The wake is now
`BackgroundWork` (the Durable Object's `waitUntil`), as a fork's has been since D57. A lost wake
is repeated by the next flush that leaves enough L0 SSTs, and a write refused for L0 backlog
wakes the compactor the same way. Correction (D78): a wake no longer serialized by the flush lock
can reach a cold compactor while the previous wake is still fencing in. Two fence-ins then left
the compactor fenced by itself, so its database never compacted again. D78 makes the fence-in
single-flight.

**SST blocks are encoded a chunk of rows at a time.** `SstStore.write` made one stream per row.
Mapping each chunk of rows to the blocks it closes took a 60 MB flush from 1.93 s to 1.60 s in
local workerd (`flush-bench`, 17%). Two other suspects were measured and left alone: decoding the
rows read back from SQLite with their schema (2%), and converting each row's columns (within
noise).

**16 KiB blocks cost a flush no more CPU than 4 KiB.** A sweep on `preview-2` and `preview-3`,
three runs per cell and each cell on both stages, measured 3.4 to 4.9 s of CPU per 60 MiB flush
at either size, and the same ratio of flush CPU to write CPU (1.3 to 1.45) in every cell. D67's
default stands.

**Concurrent part uploads: measured, declined.** With three parts of a multipart upload in
flight, the SST write of a 60 MiB flush took 4.6 s instead of 5.5 s, and used no less CPU. A
flush is bound by its CPU (about 4 of those 5.5 s), not by its uploads, so the gain is 16% for
24 MiB more memory in a writer and a second code path in the one place uploads are checked
against their digests. Not adopted. It stays on the branch `proto/parallel-parts`.

**How flush CPU is counted.** A flush's work is split between the alarm that starts it and the
flush request that joins it, in proportions that vary. `measure compaction-cpu` summarized the
two kinds of record together, so D61's and D62's "flush CPU p50" (about 1 s for 60 MiB) was the
median of halves. Counted per flush it is about 4 s for 60 MiB, which is what `measure flush-cpu`
has always reported (55 to 90 ms per MiB). The limits are unaffected: they were set from the
largest single invocation, and the largest seen in this round is 7.7 s against the API Worker's
30 s. CPU also varies between runs by up to 2× with the machine a Durable Object lands on, so
comparisons use several runs and the ratio to the same run's write CPU.

**Tests.** `db-writer.test.ts`: with a compactor that never answers, three flushes in a row
complete and the compactor is woken twice; waiting for the wake, or dropping it, fails. The SST
contents are covered by the existing writer, reader, and compaction tests.

## D74. The writer keeps one figure for unflushed bytes; the alarm and `status` read counters, never the memtable

**Found by repeating D61's memory measurement** (`measure writer-memory`, one writer filled to
`maxUnflushedBytes`). The flush of 267 MB took 20 s in one run and 200 to 220 s in the next three.
In the slow runs the writer's alarm fired about six times every ten seconds for three and a half
minutes, each firing using about 1.5 s of CPU and ending `ok` without flushing. D61's own run shows
the same thing: 174 writer invocations and an 82-second flush, read then as "flush CPU p50 1.5 s".

**The cause: two figures for one quantity.**

- Writes, backpressure, and `scheduleFlush` used an in-memory counter that added
  `rowEncodedSizeEstimate(row) + 24` per row.
- The alarm ran `MemTable.stats`, which sums `LENGTH(key) + LENGTH(payload) + 32` in SQLite: a
  scan of every unflushed row, and a second scan of the rows kept for transactions.

The counter ran 1% above the scan for 1 KB values and about 8% above it for 100-byte ones. In
that gap the counter said "at `l0SstSize`, flush now" and set the alarm for now; the alarm
scanned, found less than `l0SstSize`, did not flush, and called `scheduleFlush`, which set it for
now again.

- **With production settings** the gap closes once another 1 to 8% of `l0SstSize` is written, so
  a busy writer wasted a few scans of up to 64 MiB per flush. A burst that stopped inside the gap
  would spin until `maxL0Age` (60 s), at about 0.4 s of CPU per turn.
- **In the measurement** `l0SstSize` equals `maxUnflushedBytes`, so the counter also refused
  further writes and the gap never closed. The scans took nearly all of the single-threaded
  writer's CPU, which is why the flush an operator then asked for took ten times as long.

**`status` scanned too, and every transaction begins with it.** `DbClient.beginTransaction` calls
`DbWriter.status`, which ran the same `stats`. One status call on the 267 MB memtable used 1.58 s
of CPU. No soak could show it: their memtables hold a few hundred rows.

**The fix.**

- **One formula.** `memTableWriteBytes` (key, payload, 32 bytes) is what SQLite sums and what a
  commit adds to the counter, so the counter equals what a scan would report, as long as no commit
  runs during a scan (next item).
- **Three counters**, in one value: unflushed bytes, unflushed rows, and the oldest unflushed
  commit time. They are read from SQLite when the writer starts and when a flush ends, and advanced
  by each commit.
- **A scan is installed only if no commit ran during it.** A flush holds only the flush lock, so a
  commit can land between its scan and the moment it sets the counters; setting them anyway lost
  that commit's row from them, and with the oldest commit time gone no alarm was set for it (found
  in review). Each commit now advances a generation number when it starts and again, in the same
  step that adds its row, when it ends, and marks itself running in between. The refresh installs
  its scan only if the generation is unchanged and no commit is running, and otherwise scans again,
  three times at most. If every scan is overtaken it keeps the counters it has: they still hold the
  rows the flush wrote, so they are above SQLite, never below it, and only bring the next flush
  forward; that flush refreshes them. The flush cannot take the commit lock instead: a seal takes
  the commit lock and then the flush lock.
- **A flush with nothing to write settles stale counters.** Counters left above SQLite (a refresh
  that gave up, or one that failed after the flush was recorded) kept the alarm due, and an alarm
  that found nothing to flush set itself for now again. A flush that writes nothing now rescans
  when the counters still show unflushed rows (none: counters showing nothing are never wrong).
- **An interrupted write is still counted and still sets its alarm.** A request interrupted
  after its rows reached SQLite, and before the counters or the alarm were updated, left rows
  nothing would flush until the next write (found in review of the fix above, with a failing
  test). The mark, the SQLite commit, and the counters' update are now uninterruptible together,
  and the alarm is scheduled when the write ends however it ends.
- **The alarm decides from the counters** `scheduleFlush` set it from, so the two cannot
  disagree, and an alarm costs no scan.
- **`status` reports the counters**: no scan on a transaction's begin or on a status request.
- **`MemTable.stats` no longer scans the kept rows.** Nothing read that figure.

A flush still scans twice when it ends: the refresh of the counters over what was written
meanwhile, and the trim's sum over the rows it keeps (at most `maxRetainedBytes` plus the newest
flush). Both are once per flush.

**Tests** (`db-writer.test.ts`):

- The counters equal `MemTable.stats` after writes of every kind and very different sizes, after a
  flush, and after the next write.
- With `l0SstSize` 1,040 and 1,033 bytes unflushed: the alarm is set for the age limit, an early
  alarm neither flushes nor sets itself for now, and one more row sets it for now and the alarm
  flushes. On the code before, the first assertion already fails.
- A recording SQLite storage: a write, a status read, and an alarm that is not due run no
  aggregate statement; the writer's start runs one and a flush two.
- Six mutations caught: another formula, a row counter not advanced, `status` scanning, the alarm
  scanning, a flush not refreshing, an alarm that flushes whenever anything is unflushed.
- A memtable whose `stats` lets a commit run between the scan and the install: the counters then
  equal a scan, and the alarm at the row's age limit flushes it (on the code before, the counters
  showed nothing and the alarm did not flush). A commit that started before the scan and ends after
  it is counted once (fails without the "no commit running" check). Every scan overtaken: the
  counters stay above SQLite and the next alarm flushes every row. A counter scan that fails after
  a flush: the next alarm settles the counters and does not set itself again.

**Model.** `NoAckedWriteLost` and `SealSplit` are indifferent to when a flush is scheduled; no
step changed (the skills record it).

## D75. The baseline is a plain Durable Object SQLite table, measured on the same stage

Dillon's architecture review asked for one representative workload compared against plain
Durable Object SQLite, with the same placement, payload, batch size and concurrency, reporting
p99, operation counts and cost assumptions, and what forks or off-writer reads buy, with
evidence kept apart from unmeasured claims.

**The baseline lives in the preview-only probe Worker** (`PlainSqliteServer`,
`infra/shaledb/src/telemetry-probe-worker.ts`): one `WITHOUT ROWID` table keyed by the key, a
batch written in one transaction, a point read by key, reached through a Worker over the same
kind of binding shaledb's writer is. The probe Worker is not part of a production Stack (D104
pins that), so the baseline can never reach production.

**One tool runs both sides.** `scenario-cli measure baseline` runs the same closed-loop load
(writers sending batches, readers doing point reads of preloaded keys) against shaledb with
reads at `Latest`, shaledb with reads at `Flushed`, and the plain table, and reports for each
what the client saw and what the stage did: invocations and CPU per Worker and Durable Object
from tail records, and R2 requests by billing class from spans. A `/rowcost` route measures what
SQLite bills per row for the memtable's statements and the plain table's.

**What it found** (`docs/measurements.md`, "Baseline: a plain Durable Object SQLite table";
provisional until the release-commit runs):

- Write latency is the same as the plain table's.
- A `Latest` read is slower than a plain read; a `Flushed` read is faster and reaches no
  Durable Object.
- **shaledb's SQLite write charge is three times the plain table's**: two rows written per row
  on insert (the memtable's table and its index) and one when the flushed row is trimmed. At
  100-row batches that charge is 99% of the marginal cost of either system.

**Decided.**

- The README states the comparison and says when the plain table is the better choice: when one
  object's table holds the data and its writer can serve every read.
- The memtable's index stays for v1. It serves the flush's and the feed's reads in sequence
  order and the probe by key; removing it would trade a billed row per write for a sort or a
  scan per flush. That trade has not been measured, so it is recorded here as the first cost
  experiment after v1, not done now.
- Storage costs, large datasets, and forks are not in the comparison, and the README says so.

## D76. A transaction reads the newest manifest when it fits; a snapshot is taken at the newest manifest

Corrects D58's "reads pin the begin manifest". Found by checking `docs/invariants.md` against the
code, and reproduced with a unit test before the fix.

**The defect.** A transaction begins with the manifest of the writer's last flush
(`flushed_manifest_id`), and its reads loaded exactly that manifest. A compaction replaces a
flush's manifest within seconds. GC deletes it `gcMinAge` after that. The writer goes on naming
it until its next flush.

- On a database that stays warm with reads but flushes nothing for more than `gcMinAge` after a
  compaction, every transaction that reads failed with `SnapshotTooOld`, and so did every retry,
  because each new transaction was handed the same deleted manifest.
- A transaction's own commit could not end the state: it has to read first. A plain write
  followed by a flush did, and so did a writer restart.
- No answer was ever wrong. D58's argument, "GC keeps a superseded manifest for `gcMinAge`, which
  is longer than the retention", counted from the transaction's begin; GC counts from when the
  manifest was replaced, which can be long before.
- No soak showed it: they write continuously.

**The rule.** A transaction's read at `readSeq`, holding the writer's rows above
`retained_from_seq`, is exact from any manifest `M` with

> `retained_from_seq <= M.lastL0Seq <= readSeq`

by D70's argument: such a manifest holds every row the writer dropped, and no flush or compaction
after `readSeq` shaped it. The begin manifest is one such manifest. So is the newest manifest,
whenever nothing was flushed since the transaction began.

`DbReader.manifestFor` now reads, for a transaction's view:

1. **the newest manifest, when it is between the bounds** (the cached one, as a `Flushed` read
   uses it). Its SSTs are always stored, which no older manifest can promise, and it needs no
   R2 read when cached. Before, every transactional read loaded its begin manifest from R2.
2. **otherwise the begin manifest**, when it is still stored and between the bounds. This is the
   case D58 was built for: a flush committed after the transaction began.
3. **otherwise `SnapshotTooOld`.** The retry begins with the newer flush's manifest.

A snapshot is unchanged: it names one manifest and reads exactly that one.

**A snapshot is taken at the newest manifest, loaded now.** `DbClient.snapshot` took the manifest
a `Flushed` read would use. Since D67 that can be a cached manifest a compaction replaced up to
half of `gcMinAge` ago, so an unpinned snapshot was guaranteed readable for 30 minutes from when
it was taken, not the documented hour. It now loads the newest manifest
(`DbReader.newestManifest`): one manifest load per snapshot, and the documented lifetime holds
again.

**Model.** `Transactions.tla`: a read uses the begin manifest or the newest one, whichever the
reader finds, and aborts when the one it finds is outside the bounds. New invariant
`ReadsAtBegin`: every read of a transaction, committed or not, returned the value the database
held at its begin (the old `Serializable` says nothing about read-only transactions). New broken
variant `noReadUpperBound` fails it, and `noReadHorizonCheck` now fails it too. 40.6M states.
(Widened in review, D83: a read may use any manifest the reader's cache can hold, not only these
two.)
`vp run verify` is 29 runs.

**Tests** (`db-transaction.test.ts`, `db-client.test.ts`):

- Flush, flush, compact, wait, collect: the writer still names the deleted manifest; a get, a
  scan, and a serializable commit succeed. A transaction that began before a later flush is
  served by the manifest the reader still holds, then is too old once the reader has moved on,
  and the next transaction reads. Fails on the code before.
- A begin manifest that is stored and between the bounds but whose SSTs are gone: the read uses
  the newest manifest.
- A snapshot taken while the reader holds a replaced manifest is taken at the newest.
- Six mutations caught: the begin manifest tried first, no fallback to it, a begin manifest
  outside the bounds used anyway, the newest manifest used past the snapshot, a snapshot reading
  the newest manifest when its own is gone, a snapshot taken from the cache.

**Not fixed here.** A fork's manifests name its source's SSTs, and the fork released its hold on
the source too early for readers of its older manifests (D77).

## D77. A fork keeps its hold on its source while a reader may still hold a manifest that reads it

**The defect.** D38 released a fork's hold on its source as soon as the fork's newest manifest,
and the manifests its own checkpoints pin, no longer named the source's SSTs. It ignored the
fork's manifests replaced less than `gcMinAge` ago. A reader may hold those: a snapshot from
`DbClient.snapshot` (promised for `gcMinAge`), a manifest a Worker isolate cached, or a
transaction's begin manifest. Every other part of the system keeps what such a manifest needs
(D29, D33, `MayBeHeld` in `GcSafety.tla`). Two scratch tests from an interaction audit proved the
consequences. Seconds after the fork's rewrite, the hold was gone. The source's collection then
deleted 5 SSTs the fork's snapshot named, or the source could be removed. The snapshot then
failed with `StorageUnavailable` on every retry, as did a transaction whose begin manifest was
the writer's fence-in manifest.

**A second early release, found while fixing it.** A fork of a fork (D13) reads the original
owner's SSTs under the owner's id, through its hold on the middle database. D38 matched a hold to
the owners its handles name. So the grandchild's hold on the middle database counted as
releasable before the grandchild's rewrite: any idle poll could release it, for example one
waiting for compaction budget. Without that hold the middle database's own hold on the owner
lapses, and the owner's SSTs can be deleted under the grandchild. The `manifest.test.ts` case
fails on the code before.

**The rule.** A fork releases its hold only when none of its manifests that a reader may still
hold names another database's SST. Those manifests are exactly the ones GC keeps:

- the newest;
- every manifest a checkpoint pins;
- every manifest superseded less than `gcMinAge` ago, measured from its successor's write time
  (D33).

A database has at most one external reference: only `forkManifest` creates one, and every other
database's SST the database reads came through it. So the hold is needed while any kept manifest
names any other database's SST, whichever owner's id it carries.

**The design: the fork's own collection releases the hold.** `GarbageCollector.collect` already
computes the kept manifests (`planManifestCollection`) and loads them to plan SST deletes. After
that, `releasableExternalRefs(newest, kept)` returns the hold when none of them names another
database's SST. Releasing is two idempotent CAS steps, as before:

1. remove the checkpoint from the owner's manifest;
2. remove the reference from the fork's own manifest.

A crash between them leaves the reference, and the next collection releases again. If any kept
manifest cannot be loaded, the collection releases nothing.

A manifest newer than the ones the collection listed names no other database's SST that they do
not. Flushes and compactions add only the database's own SSTs, `dropSstFromManifest` removes one,
and no edit after `forkManifest` adds a handle owned by another database. So checking the listed
kept manifests is enough, and nothing beyond what GC already loads is read.

**What brings the release about.**

- **The compactor's alarm.** The compactor sets a collection due `gcMinAge` after it sees the
  rewrite finish (D60). That collection finds the pre-rewrite manifests old enough and releases.
- **The maintenance sweep.** It now asks for a collection of any database that still holds a
  source but no longer reads another database's SSTs. It still wakes the compactor of one that
  does. The sweep is the backstop for a lost alarm, and for a middle database whose fork just
  released its hold on it: that database's collection is not otherwise due. A collection asked
  for early releases nothing while the pre-rewrite manifests are young.

**Rejected.**

- **A second "may still be held" rule in the compactor**, for example "release `gcMinAge` after
  the rewrite". It would copy GC's rule and drift from it, and it would not see the fork's own
  pins.
- **Loading every kept manifest in the compactor's poll.** That is the collection's work done
  twice, on every poll.
- **Delaying source collection or removal instead.** The source cannot know which of the fork's
  manifests readers may hold; only the fork's collection can.

**What it costs.** The source keeps the SSTs a fork read for about `gcMinAge` longer after the
fork's rewrite: an hour in production, 10 s with dev timings. For a split, the sealed database
cannot be removed (`DatabaseHasDependentsError`) until that time has passed for both halves.
Nothing in a split waits on it: `ShardSplit` never removes the sealed database, and whoever does
retries the removal later (`writer.workerd.ts` retries it). A middle database of a fork chain
releases its own hold only at its next collection after its fork's release (below).

**How long a middle database holds the original owner.** After a fork of a fork releases its hold
on the middle database, nothing but the maintenance sweep asks the middle database to collect.
The fork's collection does not call the middle database's compactor; `GarbageCollector` keeps its
dependencies. The bound follows from the sweep:

- **One sweep cycle.** Each cron run visits 50 databases (`MAINTENANCE_SWEEP_PAGE_DATABASES`),
  every 5 minutes (`*/5 * * * *` in `gc-worker-entry.ts`). With `P` prefixes under `db/`
  (databases and removed databases' leftovers), one cycle takes `ceil(P / 50)` runs, that is
  `ceil(P / 50) × 5` minutes. For 1,000 prefixes that is 20 runs, 100 minutes; for 10,000, 200
  runs, 16 h 40 min.
- **The middle database has rewritten already.** The sweep's visit asks it to collect within one
  cycle of the fork's release. If the manifest the released hold pinned was superseded less than
  `gcMinAge` before that collection, it is still kept. The collection then schedules the next one
  `gcMinAge` later (`nextCollectionTime`: a young manifest), and that one releases. So the hold
  ends at most one sweep cycle plus `gcMinAge` after the fork's release.
- **It has not.** It holds the owner until its own rewrite, and releases at the collection due
  `gcMinAge` after that, as any fork does.

A visit that fails is logged and retried a cycle later. Every chain adds its own wait: a chain of
`k` forks below the original owner can hold it for up to `k` such bounds.

**Tests and model.**

- `database-lifecycle.test.ts`:
  - **Split.** Each fork keeps its hold after the rewrite and the source cannot be removed. One
    `gcMinAge` later the fork's own due collection releases the hold, and the source is removed.
  - **Snapshot.** A fork's snapshot taken before the rewrite stays readable through the fork's
    and the source's collections and through an attempt to remove the source. After `gcMinAge`
    the hold is released, the source is removed, and the snapshot reads `SnapshotExpired`.
  - **Fork of a fork.** Its holds are kept down the chain while any kept manifest names the
    original owner.
- `garbage-collector.test.ts`: the sweep asks a rewritten fork to collect and wakes one still
  reading its source.
- `database-lifecycle.test.ts` also covers two more cases:
  - **A failed load.** A kept manifest that cannot be loaded releases nothing, whether its object
    is missing or its read fails.
  - **A crash between the two steps.** The source's checkpoint is gone and the fork's reference
    remains, and the next collection finishes the release.
- `database-lifecycle.test.ts` also covers a split successor split again before its rewrite,
  through `ShardSplit`'s own calls (`seal` with holds, then `forkRange` from each hold). The
  second split's halves keep their holds on the sealed left half. The left half keeps its hold
  on the original shard until its halves have released and it collects again.
- `manifest.test.ts`: a fork of a fork's hold is kept before its rewrite.
- Every `lsm-cloudflare` test above fails on the code before. Each mutation in the reports is
  caught, including a release despite an unloaded manifest and the two steps swapped.
- `GcSafety.tla` gains an abstract fork (`GcSafetyFork.cfg`: `ForkStart`, `ForkRewrite`,
  `ForkRelease`) and the invariant `ForkReadsComplete`. The broken variant `earlyForkRelease`
  (D38's rule) fails it.

## D78. One compactor fence-in per start, state written by read-modify-write, and a fenced compactor fences in again

**Found live.** `measure manifest-cas --stage preview-3` (30 small flushes back to back) on the
final code failed with `WriteStalled`. Database `ms-cas-mwy5ujbj` had 16 L0 SSTs at 2026-11-26
23:05 UTC and was still stuck twelve minutes later. Its compaction status was
`{"compactorEpoch":1,"running":[],"fenced":true}`, while the manifest's compactor epoch was 2 (the
spans: `agents/compactorfence/evidence/timeline.txt`).

**What happened** (`compactor.ts` before this change):

1. Flush 4 woke the cold compactor. Since D73 the wake is background work, so flush 5 woke it
   again 0.65 s later, while the first wake was still inside `ensureState`.
2. `ensureState` had no lock. Both wakes found no state, and both committed `fenceCompactor`:
   epochs 1 and 2 (manifests 8 and 9).
3. The first wake stored epoch 1 and set the alarm. Its poll loaded manifest 8 (epoch 1) and
   started a compaction for epoch 1. Meanwhile the second wake stored epoch 2.
4. The poll ended by writing back `{...state, running}` from the state it had read at its start,
   which overwrote epoch 2 with epoch 1.
5. The job found epoch 2 in the manifest and was discarded. The next poll saw
   `manifest.compactorEpoch !== state.compactorEpoch` and stored `fenced: true`.
6. A fenced compactor was terminal. `wake` skipped the poll, `runAlarm` returned at once, and
   `dropSst` refused. There is one compactor Durable Object per database, so no newer compactor
   existed to take over. The database stopped compacting, and once it reached `l0MaxSsts` every
   write was refused.

Before D73 a flush awaited the wake under its flush lock, which hid the race for flushes. Other
callers could reach it all along: the maintenance sweep, `requestCollection`, `dropSst`, and the
stalled-write wake. A Durable Object's input gate does not serialize two requests across a
manifest commit on R2.

**The rules.**

- **One fence-in per start.** `ensureState` runs under `startLock`, so concurrent first callers
  share one `fenceCompactor` commit. The lock order is written where the locks are declared:
  `planLock`, then `collectionLock` (never held with the others), then `startLock` (an SST drop
  takes it under `planLock`), then `stateLock`.
- **Every state write is a read-modify-write.** `updateState` applies a function to the stored
  state and writes the result under `stateLock`, around no other I/O. Each field has owners (see
  `CompactorState`):
  - A poll writes `running` and `corruptSsts` onto the current state, and only brings
    `collectionDueAt` forward.
  - It writes nothing over a newer epoch; its jobs carry the old epoch, are discarded, and have
    their leases released.
  - A collection sets `collectionDueAt` from its report only if no request moved it while the
    collection ran.
  - `requestCollection`, the alarm's retry, and an SST drop change only their own field.
- **A fenced compactor fences in again.** `fenced: true` is SlateDB's compactor process that
  exited on `Fenced`; SlateDB's next compactor start fences in with a new epoch. Here the next
  start is the next `wake`, `requestCollection`, `dropSst`, or alarm.
  - It commits `fenceCompactor` again and releases the old epoch's leases.
  - It keeps the corruption reports and the collection due time.
  - `commitCompaction`'s epoch check discards the old epoch's jobs, so "no fenced compactor
    commits" still holds: every commit carries its job's epoch.
- **No ping-pong.** No guard against two instances fencing each other is needed. Only this Durable
  Object fences in for its database, and the runtime runs at most one instance of it, sharing one
  storage. A newer epoch it did not store therefore means its own state was lost or overwritten,
  and fencing in again is the repair. Each re-fence needs a stored `fenced: true`, which only a
  poll that saw a newer epoch writes, so re-fences are bounded by those polls.
- **The compaction status** (`GET /v1/databases/{id}/compaction`) keeps its shape. `fenced` can
  now be true only between the poll that saw a newer epoch and the next start.
- **Revival.** A database already stuck, like the one above, recovers on its next start: a wake
  from the next flush or the stalled-write wake, the sweep's request, or an operator's drop.

**Rejected.**

- **Clearing the state on fencing, so the next wake starts fresh.** That would lose corruption
  reports and the collection due time.
- **A compare-and-set on the stored epoch only at the poll's write.** It fixes the one proven site
  but not the class. `collectionDueAt` was also lost when `requestCollection` or a collection
  stored it during a poll, and D77 relies on requested collections.
- **Making the wake synchronous again in the flush.** That would bring back D73's flush times, and
  the other callers would still race.

**Tests** (`compactor.test.ts`; every gate is a `Deferred` in a wrapped manifest store, reader, or
collector, not a sleep):

- **Two overlapping cold wakes.** On the code before, the test ends in the live state: two
  fence-ins, manifest epoch 2, stored epoch 1, `fenced`, and L0 at 4 after three more wakes. Now
  there is one fence-in, and the compaction commits.
- **A poll that ran across a fence-in** stores nothing over the newer epoch.
- **A collection requested during a poll**, and one requested during a collection, are still due
  afterwards.
- **A stored `fenced: true` with an older epoch** fences in again on a wake (epoch 3; the
  compaction commits), and again from the alarm alone (epoch 5), keeping the collection due time.
- **Six mutations, all caught:** the start lock removed, the poll's due time taken from its
  starting state, the poll's whole starting state written back, a collection overwriting a request,
  no re-fence on wake, and no re-fence from the alarm.

**The same pattern elsewhere.** These are the first-call initializers that write to R2 or a
manifest, checked against two concurrent first calls:

- **`DbWriter.startWriter`** runs under `initLock` and re-reads the phase inside it.
- **`DatabaseLifecycle.create`** is a create-if-absent of manifest 1, so exactly one succeeds
  (also measured live with `create-race`).
- **`CompactionBudget` and `ShardDirectory`** do their read-modify-write under a lock, with no
  other I/O inside it.
- **`MaintenanceSweep`** is a cron, not a Durable Object. Its cursor is a plain object, and two
  overlapping runs visit the same page twice, which is harmless (each visit is idempotent).

**Model.** `ManifestCasFencing.tla` already treats each fence-in as a compactor with its own epoch,
and a compactor that fences in again is one more of those. The overwritten epoch was in the
compactor's own Durable Object state, not in the manifest, so it is a property of that state, and
the tests check it. No step changed; reran, and every broken variant fails (the skill records it).

## D79. The sharded API answers refused requests with the public envelope; it is not part of `/v1`'s contract

D69 put the database API's refusals in the public error envelope. The sharded API (the shard
router Worker, `ShardedHttpApi`) had no request middleware. Measured in-process through the
router and Alchemy's failure boundary (`safeHttpEffect`), before this change:

| Request                                       | Status | Body                                          |
| --------------------------------------------- | ------ | --------------------------------------------- |
| Malformed JSON, a refused body, an empty body | 400    | empty                                         |
| A group id the path schema refuses            | 400    | empty                                         |
| A content type no endpoint accepts            | 415    | `Unsupported content-type: text/plain` (text) |
| An unknown route, or a wrong method           | 404    | empty                                         |
| A defect in a handler                         | 500    | empty                                         |
| Any answer                                    |        | no `x-request-id`                             |

**The mechanism is shared, not copied.**

- **The middleware.** `publicHttpRequestMiddlewareLayer` (`db-http-request.ts`) is now the
  middleware both APIs use. The only parameter is the contract its messages point to: the
  database API's OpenAPI document, or the sharded API's endpoints. `dbHttpRequestMiddlewareLayer`
  is the database API's instance.
- **The effect on the sharded API.** Each refusal above is now the envelope, with the request id
  in the body and in `x-request-id`:
  - `InvalidRequest` (400) names the malformed part.
  - `RouteNotFound` (404) covers an unknown route or a wrong method.
  - `InternalFailure` (500) answers a defect, which is logged.
- **Why the change is allowed.** Every status stays the same, except an unaccepted content type:
  415 becomes 400 `InvalidRequest`, as D69 did for the database API. The sharded API is not part
  of `/v1`'s contract (below), and an empty body that becomes the documented envelope breaks no
  caller that read only the status.

**A database failure's message.** The router passed a database failure's internal message to the
caller verbatim. A storage failure's message names object paths (`db/<id>/…`) and repeats the
storage error's text. `ShardedRequestUnavailable` and `ShardedRequestRefused` now carry the
database API's public message for the same failure (`toPublicError`, now exported). Their
`reason` still holds the internal error's tag (`WriterUnavailableError`). Changing what an
existing field holds is not additive, so it is left for a decision.

**The `401` message.** It told every caller that `preview-cli token <stage>` prints the token.
That is wrong for production, whose tokens are printed only when created or rotated (D64). It is
also the wrong token for the trace collector. The message now names the token to send: one of the
stage's access tokens, or, from the collector, its trace collector token, each pointing to
`docs/operations.md`. The OpenAPI examples were regenerated. The collector's `InvalidRequest` said
`limit` runs from 1 to 100; pages hold up to 500, and it now says so.

**An oversized body** never reaches either API's code: neither sets a body limit of its own.
Cloudflare's Workers limits page says a request to a Worker on a zone is answered `413` by
Cloudflare above the zone plan's limit (100 MB on Free and Pro, 200 MB on Business). It does not
state the limit for `workers.dev`, where these Workers are served. No test covers it.

**The GC operator API, the same way** (decided after review). The GC Worker's API (`GcHttpApi`) had
the sharded API's gaps. Its refusals and defects were answered with empty bodies. Its declared
errors carried internal messages: a collection or sweep that could not reach storage repeated the
object store's message, which names object paths and the storage error's text.

- **The middleware.** `gcHttpRouterLayer` now provides `publicHttpRequestMiddlewareLayer`, naming
  `GcHttpApi`.
- **The handlers** answer every failure with the declared error and a message of their own:
  - `DatabaseNotFoundError`: "does not exist".
  - `GarbageCollectorUnavailableError`: retry the same request, a collection is safe to repeat.
  - A served-id mismatch: an internal error, with the request id to give the operator.
  - The internal error is logged with the request id.
- **Why it is allowed:** statuses, `_tag`s, and fields are unchanged; only message texts change.
  The GC API is an operator API outside `/v1`'s contract.

**After review.**

- **Status guidance.** Both APIs keep `400` for an unaccepted content type. `docs/errors.md`'s
  status guidance now says what they do: a wrong method is `404` `RouteNotFound`, an unaccepted
  content type is `400` `InvalidRequest`, and neither `405` nor `415` is answered.
- **The sharded errors' `reason`** stays the internal tag. `docs/api-compatibility.md` says it may
  change.

**The contract.** The OpenAPI drift test covers `DbHttpApi` alone.
`docs/api-compatibility.md` now says the shard router's API is not part of `/v1`'s contract: it
is the sharding package's example (D40), and its declared errors are not the envelope. The GC
operator API is not part of the contract either.

**Tests.**

- **`db-http-request.test.ts`** sends the database API, in-process, a malformed body, a refused
  body, an empty body, an unaccepted content type, a refused path id, an unknown route, and a
  wrong method. It is the reference.
- **`shard-router-worker.test.ts`** sends the same requests to the sharded API and expects the
  envelope. It also covers:
  - its declared errors and successes, unchanged but now with a request id;
  - a defect, answered as `InternalFailure`;
  - two database failures carrying internal detail, which never reaches the body.
- **`lsm-access-gate.test.ts`**: the `401` names the token, not `preview-cli`.
- **`serveLsmTestRequest`** (`lsm-test-runtime.ts`) serves a request as the Worker does:
  `safeHttpEffect`, then `HttpEffect.toHandled`.
- **`gc-worker.test.ts`** (GC operator API):
  - a collection, and each failed collection (not found, compactor or storage unavailable, a
    served-id mismatch) as its declared error with no storage detail;
  - a failed sweep page;
  - a refused id, a wrong method, an unknown route, and a defect, in the envelope.
- **Mutations caught:**
  - the middleware removed from the GC router (all three GC tests fail);
  - each of the GC messages replaced by the internal one again: collect unavailable, sweep, and
    not found (one test fails each);
  - the middleware removed from the sharded router (all four sharded tests fail);
  - the internal message passed through again (1 fails);
  - the gate ignoring the token it names (1 fails).

## D80. The change feed answers only what its changelog provably holds; a fork's feed starts at its fork point

**Found by the review agent's scratch test.** `DbReader.changelogRows` detected retention only by
comparing the oldest kept entry's `firstSeq` with `afterSeq`. When no entry above `afterSeq` was
kept, it returned no rows and no error, so `DbClient.changes` moved the consumer's cursor over
`(afterSeq, flushed_seq]` as if nothing had changed. There were two ways in:

- **Retention pruned every entry.** GC's `pruneChangelog` dropped them all and nothing was flushed
  since. The consumer was handed the unflushed changes above them and lost the pruned ones without
  being told.
- **A fork or split successor before its first flush.** It starts with an empty changelog, and its
  sequence numbers continue above its source's, so a consumer asking from before the fork point
  got the fork's own changes and none of the source's.

D42 promised `Expired` in both cases; for a feed, a silent gap is data loss.

**The rule, enforced in one place.** The feed answers a request for changes after `afterSeq` only
if the manifest's changelog holds every change in `(afterSeq, lastL0Seq]`
(`changelogHoldsChangesAfter`). Otherwise it fails with `ChangelogUnavailable` (`Expired`).
`DbReader.changelogRows` is the only caller. The unflushed part comes from the writer, which holds
every row above `flushed_seq` (D42).

**What the manifest records.** A new field, `changelogAfterSeq`: the changelog holds every change
with a sequence number above it, up to `lastL0Seq`.

- `initialManifest` sets it to 0.
- A flush leaves it unchanged. Its entry covers `(previous lastL0Seq, cut]`, and a flush with no
  rows adds no entry and loses nothing.
- `pruneChangelog` raises it to the newest `lastSeq` it drops.
- `forkManifest` sets it to the fork point (the checkpoint's `lastL0Seq`).

**The format.** The field is optional and needs no `formatVersion` change (D64). A manifest
without it reads as follows (`changelogAfterSeqOf`):

- the oldest kept entry's `firstSeq - 1`, or `lastL0Seq` when no entry is kept;
- that is never below the true value, because an entry's rows all lie above the previous flush's
  cut;
- so the default only answers `Expired` more often, never with a gap.

Older code ignores the field when it decodes a manifest (as D49's dropped option showed), so a
manifest it rewrites after a rollback loses the field and falls back to that safe default.
`manifest.test.ts` checks the default against the field over random flushes, rows without
sequence numbers, and prunes.

**A fork's and a split successor's feed** start at the fork point; changes up to it are in the
source's feed. A consumer following a database through a split:

1. Reads the sealed source's feed until a page returns no rows. A sealed database accepts no
   writes, so the feed has ended. Its `status` shows `sealed: true`, and its `lastSeq` is the
   sequence number the seal made durable.
2. Reads each successor's feed with `afterSeq` set to the source's `lastSeq`. Each successor was
   forked from a checkpoint of the sealed manifest, whose `lastL0Seq` is that number, so the
   successor's feed starts exactly there.

The consumer must use the source's `lastSeq`, not its own cursor, which may be lower when the
last sequence numbers carried no rows. `database-lifecycle.test.ts` runs that procedure through a
split, and the OpenAPI description of the feed says it. The shard router exposes no feed.

**Tests:**

- `change-feed.test.ts`:
  - every entry pruned, with nothing flushed since and with unflushed changes above the pruned
    range;
  - the boundary at exactly `afterSeq`, and one below it;
  - a fork before and after its first flush;
  - the property test now also prunes, and checks each page against the oracle: exactly the
    committed changes in `(cursor, nextAfterSeq]`, or `Expired` only when the manifest says
    changes after the cursor were dropped.
- `database-lifecycle.test.ts`: the split procedure.
- `manifest.test.ts`: the edits and the default.

The three gap tests and the split test fail on the code before. Seven mutations are each caught:
the rule removed, the prune not raising the field (two tests), the fork starting at 0, the
boundary off by one, and two unsafe defaults.

## D81. Merge operands and values expire on their own; compaction folds only rows that expire together

**Found by the review agent's scratch test.** A key with a value `V` (no TTL), an operand `A`
expiring at 10, and an operand `B` (no TTL) reads `VAB` before 10 and `VB` after it.
`retainKeyVersions` folded all three into one value that expired with the earliest of them (10),
so after a compaction the key read as absent from 10 on. D10 promises that reads before and after
a compaction agree.

**The semantics** (`resolveVisibleRow`, unchanged; checked against this statement):

- **Each row expires on its own.** A row is a put, a merge operand, or a delete, written by its own
  batch.
- **A read at time `t`** walks the key's versions newest first, down to the first value or delete,
  and merges the operands it passed onto that value. At each point:
  - an operand that has expired by `t` is skipped;
  - a value that has expired by `t` reads as a delete, so it ends the walk, older versions stay
    hidden, and the operands above it merge onto nothing.

  So a key with an expired base under live operands reads as those operands merged onto nothing.

- **The `expireTs` a read reports** is the earliest among the rows it merged: when the answer
  next changes, not when the key disappears.
- **Within one batch** a key's operations are one version (D23): its merges share one TTL, and the
  version expires at the earliest TTL among them.

`DbReader.getRow` and `scan` collect the key's versions (memtable first, then the SSTs newest
level first, stopping at a level holding a value or delete) and call `resolveVisibleRow`. The read
path is consistent with the statement above.

**Compaction preserves it.** This is SlateDB's write-path rule (`merge_operator.rs`,
`merge_different_expire_ts: false`): only rows with the same `expire_ts` merge.
`retainKeyVersions` now:

- drops expired operands, and turns an expired value into a delete, as before;
- folds each run of consecutive operands with one expiry into one operand that expires with them;
- folds the run just above the base onto it, into a value, only when the base expires with the
  run or is a delete;
- otherwise keeps the base as its own row;
- drops a delete left on its own at the bottom.

**Why nothing else can fold.**

- **Rows that expire together** are all present or all absent at any later time. Their fold reads
  the same (merge operators are associative, D11).
- **A delete under a run expiring at `E`:** folding gives a value expiring at `E`. Before `E` it
  reads the same. After `E` it reads as a delete, as the delete under the expired run did.
- **Rows with different expiries** cannot fold without one of them outliving, or dying with,
  the other.
- **Non-adjacent rows with the same expiry** cannot fold either: a non-commutative operator such as
  `Concat` needs their order.

**Checked for the same fold elsewhere.**

- **The flush** (`keepReachableVersions`, D70) folds nothing. It only drops versions below a key's
  first value or delete, which no read reaches at any time.
- **`collapseWriteBatch`** folds within one batch, which is one version by D23.
- **The bottom-level rules** (expired value to delete, a lone delete dropped at the bottom) are
  unchanged and agree with the statement.

**Existing data.** SSTs compacted before D81 may hold a folded value that expires at the earliest
expiry among what it folded. Nothing can recover the parts that should have outlived it. After
the fix such a value still expires early, exactly as it did before, and later compactions turn it
into a delete as usual. New compactions fold nothing new across expiries.

**Tests:**

- **`row-resolution.test.ts`:**
  - the review's case at five times;
  - runs and base folding;
  - a delete absorbing the run above it.
- **`compaction-merge.test.ts`:** a new property. Random histories of puts, deletes and operands
  with random TTLs are split into three flush windows (`keepReachableVersions`), compacted over
  older data, then compacted again later with everything. The oracle reads the uncompacted rows,
  the flushed rows, and both compactions' outputs alike at every time from the compaction to past
  every expiry.

  The existing property compared reads only at the compaction's own `now`, before any expiry it
  had folded could pass, so it could not see this. On the code before, the new property and all
  three unit tests fail.

- **`merge-iterator.test.ts`:** its property asserted one surviving version per key, which is the
  old rule. It now asserts that no two adjacent survivors expire together.
- **Mutations, all five caught:** runs folding across expiries, the base folding whatever its
  expiry, a folded run that never expires, a delete not absorbing its run, and the base folding
  into every run.

## D82. Every signal the operations guide names is read from a span or a tail record; metrics are not collected

**The finding.** `docs/operations.md` ("What to watch") told an operator to watch three metrics:
`lsm_compaction_corrupt_ssts`, `lsm_object_part_integrity_retries`, and
`lsm_object_parts_unverified`. No live stage keeps any metric. The stage's trace collector accepts
OTLP metrics and discards them (`acceptAndDiscard` on `/v1/metrics`,
`infra/shaledb/src/trace-collector-worker.ts`), and so does the local receiver
(`tools/trace-cli/src/otlp-receiver.ts`). Nothing in the repository reads a metric. The health
report, which is the release soak's health criterion, also missed four signals the guide names:

- **Unverified parts.** Visible as `object.parts_unverified` on `ObjectStore.putMultipart`, but
  never checked.
- **A fenced compactor.** Its poll carried `compactor.fenced` but no `database.id`.
- **L0 at its limit.** Reported as a number, never as an issue.
- **Slow flushes.** Reported as percentiles, never as an issue.

A write stalled with `WriteStalled` was an issue, but without the database or the reason.

**D78's stuck database, read against the old report.** The report said "N writes stalled" only
while writes kept arriving and their spans were sampled. It named no database and gave no reason.
The stuck compactor no longer polled, so its L0 backlog was missing. Its one fenced poll was not
read. A database with no further writes would have looked healthy.

**The inventory.** Every signal `docs/operations.md`, `docs/release-v1.md` §11, and the
corruption runbook name, and where it can be read now:

| Signal                | Produced by                                                                                                    | Readable on a live stage through                                                                 |
| --------------------- | -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Limit hits            | the runtime, in tail records (`cpu-tail-worker.ts`; every non-`ok` invocation kept, D71)                       | `measure health` (issue)                                                                         |
| Stalled writes        | `WriteStalledError` on `DbWriter.write`, now with `writer.stall_reason` (`db-writer.ts`)                       | `measure health` (issue per database and reason); `trace-cli grep --name DbWriter.write --error` |
| Flush duration        | `DbWriter.flush` span                                                                                          | `measure health` (`flushMs`; issue above 30 s at p99); `trace-cli stats --name DbWriter.flush`   |
| L0 backlog            | `manifest.l0_ssts` on `Compactor.poll`                                                                         | `measure health` (`l0Backlog`; issue at `l0MaxSsts`)                                             |
| Fenced compactor      | `compactor.fenced` on `Compactor.poll`, now with `database.id`; `compactor.refenced` on the start's span (D78) | `measure health` (issue per database); `GET /v1/databases/:id/compaction` (`compactor.fenced`)   |
| Corrupted SSTs        | `compaction.corrupt_ssts` on `Compactor.poll`; the compactor's error log, a span event there                   | `measure health` (issue, from each database's latest poll); `GET …/compaction` (`corruptSsts`)   |
| Unverified parts      | `object.parts_unverified` on `ObjectStore.putMultipart`                                                        | `measure health` (issue)                                                                         |
| Re-sent parts         | now `object.part_retried` on `ObjectStore.putMultipart`, and the warning log already a span event there        | `measure health` (`uploads.withRetriedPart`, a count, not an issue)                              |
| GC and sweep failures | error status on `GarbageCollector.collect`, `MaintenanceSweep.sweepPage`                                       | `measure health` (issue)                                                                         |
| Error spans           | any span with an error status                                                                                  | `measure health` (`errorSpans`); `trace-cli stats --error`                                       |

The twelve metrics the engine defines (`rg "Metric\.counter"`) are listed below. No stage reads any
of them:

- `lsm_block_cache_requests` (`block-cache.ts`);
- `lsm_compaction_corrupt_ssts`, `lsm_compactions_started`, and `lsm_ssts_dropped` (`compactor.ts`);
- `lsm_compactions_committed` (`compaction-executor.ts`);
- `lsm_flush_bytes` and `lsm_write_stalls` (`db-writer.ts`);
- `lsm_gc_deleted_objects` (`garbage-collector.ts`);
- `lsm_manifest_cas_conflicts` (`manifest-store.ts`);
- `lsm_object_part_integrity_retries` and `lsm_object_parts_unverified` (`object-store.ts`);
- `lsm_sst_bytes_written` (`sst-store.ts`).

Each has a span attribute or span error that carries the same event, so none is the only record
of anything an operator needs.

**The decision: spans carry the signals, the health report judges them.**

- **Three span attributes, one line each.**
  - `writer.stall_reason` (`L0Backlog` or `UnflushedBytes`), on the write a stall refused.
  - `object.part_retried` on a multipart upload that re-sent a part. That was the one signal
    visible nowhere as an attribute; the warning log was already a span event.
  - `database.id` on the poll that finds its compactor fenced.

  All three keys are in namespaces the redaction policy already exports (`writer.`, `object.`,
  `database.`; D63). `lsm-trace-policy.test.ts` now lists them with the other keys the report
  reads.

- **The report's judgement is a pure function**, `summarizeHealth`
  (`tools/scenario-cli/src/health-report.ts`), so it is unit-tested without a stage.
  `measureHealth` reads the tail records and spans and calls it. Its issues:
  - limit hits;
  - stalled writes per database and reason;
  - corruption by each database's latest poll, judged by start time, because `trace-cli grep`
    lists spans by trace;
  - L0 at `l0MaxSsts` (the engine default, 16);
  - a fenced compactor per database. Only its own Durable Object fences in for a database, so a
    fenced poll means its state was overwritten, which is a defect even though D78 repairs it;
  - any unverified part;
  - a flush p99 above 30 s;
  - failed collections and sweeps.

  Re-sent parts are counted, not an issue: the guide calls them transient unless frequent.

- **Metrics stay emitted and are not collected.** The engine keeps its `Metric` counters, for the
  local receiver and a future collector. The guide no longer names them as signals. Collecting
  them would need a store and a reader for metrics. Every signal an operator needs already
  travels on a span, so that is not built.

**What this does not cover.**

- **Sampling.** In production 10% of traces are kept (P4), so a rare span-borne signal can be
  missed in a short window. A stall or corruption repeats on every write or poll, so a sustained
  one is seen. One unverified part or one fenced poll can go unseen at 10%. Tail records of
  invocations that did not end `ok` are always kept (D71); spans are not.
- **Silent databases.** A database that neither writes nor polls produces no spans, so the report
  cannot see that it is stuck. D78's fix makes a fenced compactor fence in again on its next
  start, so that state no longer persists.
- **The threshold.** The L0 limit is the engine default. A stage deployed with another
  `l0MaxSsts` is judged against 16.
- **Not tried live.** The new attributes are on code not yet deployed.

**Tests:**

- `health-report.test.ts` (8 tests) covers: quiet signals; stalls per database and reason;
  D78's stuck database (a fenced poll, L0 at 16, refused writes); the configurable L0 limit;
  corruption by the latest poll, in either order; unverified parts against re-sent parts; the
  30 s flush threshold; and limit hits with failed collections and sweeps.
- `db-writer.test.ts`: a stalled write's span names `UnflushedBytes`.
- `object-store.test.ts`: a re-sent part marks the span, and a first-time verified part does not.
- `compactor.test.ts`: the fenced poll names its database.
- **Mutations.** Ten mutations of the report, and the removal of each of the three attributes,
  are each caught by a test (`/tmp/shale-work/agents/signals/mutations.log`). One of the ten
  survived at first: two counts the test compared were equal, so the test was strengthened. One
  more mutation is not counted, because `later` is symmetric and the mutation changed nothing.
  `writer.stall_reason` for `L0Backlog` has no test of its own; the same line for
  `UnflushedBytes` does.

## D83. Review of D76: a transaction read may use any cached manifest; concurrent collections; TTL and clocks

Found in the review of D76 and in the interaction pairs `docs/invariants.md` had not reached. Six
items, each with a test or a model run that fails without it.

**Transactions model.** `Transactions.tla`'s `Read` chose between the begin manifest and the
newest one. The reader's "newest" is its cached manifest, which may be any committed manifest:
older than the begin manifest, between it and the newest, or a flush's manifest committed before
`flushed_seq` recorded it. `Read` now reads with any `m \in 0 .. seq` that `Fits`, and is refused
only when the begin manifest does not fit either. The model allows refusing whenever the begin
manifest does not fit; the code refuses less often, and refusing is always safe. The state count is
36.9M, checked in 167 s; the run's 420 s limit stands. A first draft, which refused whenever any
chosen manifest did not fit, timed out.

**Two tests the review named.**

- **A read the reader cannot yet serve is retryable, not too old.** A transaction read whose writer
  keeps rows only above every manifest the reader can load (the newest is below
  `retained_from_seq` after a refresh, and the begin manifest is too) fails with the retryable
  `StorageUnavailable`. It does not fail with `SnapshotExpired`, which the client turns into
  `SnapshotTooOld`. `db-transaction.test.ts`; the mutation that returns `SnapshotExpired` there
  fails it.
- **A transaction begun before a seal cannot commit after it.** It fails with
  `DatabaseSealedError`, and none of its writes reaches the source or either successor.
  `database-lifecycle.test.ts`; a `commitTransaction` that skips `writableReady` fails it, and no
  other test.

**Two collections of one database at once** are possible and safe. The argument is in D60. A
collection that cannot load a manifest it retained now re-reads `gc/manifest.boundary`:

- **At or below the boundary**, a later collection deleted the manifest, and the run goes on
  without it.
- **Above the boundary**, something other than a collection deleted it. The run fails with
  `GarbageCollectorUnavailableError` and deletes nothing.

This guard, not a second process in `GcSafety.tla`, was chosen for two reasons:

- **The collector can tell the two cases apart.** A collection deletes a manifest only after it has
  moved the boundary past it.
- **A model would cost more than the guard.** A second collection doubles the model's GC variables
  and its interleavings, for a property that rests on two monotonic writes and one ordering, which
  a test can check directly.

`garbage-collector.test.ts` stops one collection with gates at the load of a retained manifest and
at its first delete, runs the other whole, and asserts that neither deleted an SST that a manifest
the other loaded names. A third test deletes a retained manifest above the boundary and asserts
that the collection deletes nothing. Three mutations fail them: no guard, every missing manifest
treated as unexplained, and retained manifests ignored.

**What a read that lost an SST partway should do.** The message was "Retry the read" for every
read. It now depends on the read:

- **A read** retries; the retry reads the newest manifest.
- **A scan** resumes from the key after the last row it returned.
- **A snapshot** must be taken again: its SSTs go only when GC collects it, so a retry would end in
  `SnapshotExpired`.

Codes and statuses are unchanged. `db-client.test.ts` checks a get, a scan, a snapshot get, a
snapshot scan, and a transaction read. Three mutations fail it.

**TTL and clocks.** What readers can rely on (D10, D23, D81):

- **A value expires within clock skew of its `expireTs`.** Expiry is judged by the clock of the
  isolate that reads it, and by the compaction Workflow's clock, fixed when the compaction
  prepares.
- **Readers in different isolates may disagree for up to the skew between their clocks.** One may
  see a value that another, whose clock is ahead, already reads as deleted.
- **Once compaction has turned an expired value into a delete, it does not come back.** A reader
  whose clock is behind the compactor's then sees the value gone up to that skew early.
- **The writer's clock can only make expiry late.** A row's commit time is the later of the
  writer's clock and its monotonic tick (`createTs = max(now, clockTick)`), so `expireTs` is never
  earlier than the writer's clock plus the TTL.

These hold by reading the code (`resolveVisibleRow`, `retainKeyVersions`, `DbWriter.commitBatch`,
the executor's `prepare`). No code changed for them.

## D84. A transaction's conflict check re-reads the horizon after its queries

**Found by the Durable Object audit.** `DbWriter.commitTransaction` checked that
`retained_from_seq <= readSeq`, then queried `MemTable.writtenAfter`. These were separate SQLite
statements, outside any SQLite transaction and outside the flush lock. A flush's trim takes no
commit lock. If a flush's R2 response arrived while the commit's fiber yielded between the two,
the trim deleted flushed rows above `readSeq`. The conflict query then found nothing, and a
`Snapshot` commit over a newer write of the same key was accepted: a lost update.

**The rule.** After the conflict queries, the check reads `retained_from_seq` again. If it passed
`readSeq`, the commit fails with `SnapshotTooOld`, as it would had the trim come first. Trims only
raise the horizon, so a horizon still at or below `readSeq` proves every row the queries looked
for was present. That makes the code match `Transactions.tla`'s atomic `Commit` step.

**Rejected.** Running the check in `MemTable.consistently` would be atomic in production
(`transactionSync`), but no test can show it, because the test storage's transaction can be
interleaved.

**Test** (`db-writer.test.ts`). A hook runs a flush right before the conflict query. Before the
fix the commit was accepted over the write at seq 2; now it is refused with `SnapshotTooOld`. Two
mutations are caught: the re-read removed, and the re-read testing `flushed_seq`.

## D85. A seal that does not return forgets the writer's phase; an alarm for a database that is gone stops

**A seal whose commit the writer never saw.** `DbWriter.seal` set the writer's in-memory phase to
sealed only after its manifest commit returned. If the seal's caller hung up after the
create-if-absent landed, or the boundary read after it failed, the seal ended with:

- the manifest sealed, with the writer epoch raised by one;
- the writer still believing it was open.

The next write was then acknowledged into SQLite. It could never flush, because the seal's epoch
fences its writer, and it was in no successor of a split.

**The rule.** If the seal ends without returning (failure or interruption), it sets the phase to
uninitialized while it still holds the commit lock. The next request then starts the writer again
from the manifest:

- a sealed manifest starts a sealed writer, without fencing;
- an open one fences in with a new epoch and stays writable.

A writer evicted at that point restarts the same way. Forgetting the phase after a successful
seal too would be harmless, because the manifest it reloads is sealed. That is the one mutation
the tests cannot catch.

**Tests** (`db-writer.test.ts`). A store wrapper holds the seal's commit, then the seal's fiber is
interrupted:

- interrupted after the commit landed: the next write is refused with `DatabaseSealedError`;
- the writer evicted after the commit landed: the restarted writer is sealed;
- interrupted before the commit: the database stays open, and the next write is acknowledged and
  flushed.

**An alarm for a database that is gone.**

- **The problem.** `runAlarm` rescheduled itself one second out after every failure except
  fencing. A writer whose database was removed before its state was deleted (a removal
  interrupted before `writers.destroy`) failed every firing with `DatabaseNotFoundError`, and woke
  its object every second, forever.
- **The fix.** `DatabaseNotFoundError` now ends the alarm. Nothing the writer holds can be flushed,
  and a removal retry deletes the state.
- **Test** (`db-writer.test.ts`). A writer restarted after its manifest was marked removed sets no
  alarm on three firings.

**Mutations.** All caught except the harmless one named above.

**Models.** `seal-split` and `transactions` were rerun: pass, and every broken variant fails. No
step changed; the skills record why.

## D86. A compaction's `prepare` step records its inputs' ids, not their handles

**Found by the Workflow audit.** Cloudflare Workflows store a step's result only up to 1 MiB
(the Workflows limits page: "Maximum non-stream step result per step"). `prepare` returned every
input `SstHandle`. A handle carries the SST's first and last keys, each up to 16 KiB (`MAX_KEY_BYTES`).

- **The case the test builds.** The scheduler merges eight sorted runs that are within
  `includeSizeThreshold` of each other: eight runs of 8 GiB in 128 SSTs of 64 MiB, with 1 KiB keys.
  That `prepare` result encodes to 2,973,046 bytes.
- **The effect.** `prepare` fails on every retry. The Workflow ends errored, the compactor plans the
  same job again, and that merge never runs. Sorted runs then pile up, which raises read
  amplification. A fork's full rewrite holds every SST of the fork, so a large fork never releases
  its source.
- **With 32-byte keys** the limit is reached at about 3,500 input SSTs.

**The rule.** `PreparedCompaction` records `inputIds`: each input level's SST ids, newest first.
Each output step reads the handles by id from the newest manifest (`resolvePreparedInputs`). An SST
id names one immutable object and the one handle a manifest holds for it, so these are the handles
`prepare` resolved. When the newest manifest no longer holds every input id, the step fails with
`CompactionInputsChangedError` and the job is discarded. The commit would refuse that job anyway;
now it stops before it writes more outputs. The same merge's ids encode to about 40 KB, so a job can
hold about 26,000 input SSTs before the limit.

**At deploy.** A Workflow instance in flight holds a `prepare` result in the old format. Its next
step fails to decode it, the instance ends errored, and the compactor plans the job again. Its
outputs so far are orphans that GC removes after `gcMinAge`, as for any discarded job.

**Cost.** One manifest load per output step (the `LATEST` hint and GC's boundary, then the newest
manifest). Before, each step loaded nothing.

**Tests** (`compaction-executor.test.ts`):

- **The largest planned merge's `prepare` result fits in 1 MiB.** It fails on the code before, at
  2,973,046 bytes.
- **An output step of a job whose inputs another compaction replaced is discarded.** The mutation
  that ignores a missing id fails it.

## D87. Settings must leave L0 enough SSTs to compact before writes stop

**Found by the Workflow audit** with a property test: from a manifest whose L0 holds `l0MaxSsts`
SSTs, with nothing running, the scheduler must plan a job that shrinks L0.

**The defect.** `LsmSettings` accepted `l0MaxSsts` below `minCompactionSources` (for example 1
and 2). Two rules then never meet:

- **The writer** refuses writes once L0 holds `l0MaxSsts` SSTs (`WriteStalledError`, `L0Backlog`).
- **The scheduler** compacts L0 only once it holds `minCompactionSources` SSTs.

L0 never reaches the scheduler's threshold, so writes stall for good. The settings come from the
Stack's `SHALEDB_LSM_SETTINGS` override, so an operator could deploy them.

**The rule.** A combination rule refuses such settings at plan time: `minCompactionSources <=
l0MaxSsts`. The defaults (4 and 16) satisfy it.

**Test.** `size-tiered-compaction-scheduler.test.ts`, "plans a job that shrinks L0 whenever writes
are refused for L0 backlog and nothing runs". Over every settings value the schema accepts, with
any sorted runs, the plan includes an L0 job. It fails without the rule (shrunk to `l0MaxSsts` 1
and `minCompactionSources` 2), and the mutation that drops the rule fails it.

## D88. The public surface under hostile and careless callers: TTL bound, truthful answers to retries

An audit of every request field's largest accepted value and of every retried call (audit round 2).

**A TTL could make a database permanently unflushable.** `WriteOperation.ttlMillis` accepted any
positive integer. The writer stores expiry as `createTs + ttl` in the memtable's SQLite row.

- For a TTL near 2^53 the sum is past 2^53 - 1. SQLite cannot return it as a JavaScript number
  ("Value is too large to be represented as a JavaScript number").
- The write was acknowledged. From then on every flush of the database failed, and so did a
  `Latest` read of the key, each with the retryable `StorageUnavailable`.
- Retrying never helps, so unflushed bytes grow until `WriteStalled`.

`createCheckpoint` with a `lifetimeMs` near 2^53 died the same way (`expireAt` fails the `Timestamp`
schema): `InternalFailure` (500) on a path any caller can take.

- **Rule.** A TTL and a checkpoint lifetime are at most `MAX_TTL_MILLIS` = 100 years
  (3,153,600,000,000 ms), from 1 (`TtlMillisSchema`, lsm-core). A larger value is `InvalidRequest`
  (400) at the schema, before anything is stored.
- **Why it is allowed.** This tightens a limit within `/v1`, which `api-compatibility.md`
  otherwise forbids. Every value it now refuses was already broken: the TTL ones made the database
  unusable and the lifetime ones failed with 500. The OpenAPI document states the maximum.
- **Changelog retention** at 2^53 - 1 was checked and works (no sum is stored).
- **Test:** `db-http-limits.test.ts`. The largest accepted TTL is written, read at `Latest`, and
  flushed, and the database stays writable. Values past the bound are refused. The largest
  checkpoint lifetime makes a checkpoint. Mutations caught: the bound removed, the bound at 2^53
  (the defect itself), and the lifetime unbounded.

**Answers to retries.** What each call returns when it is retried after its first attempt got no
answer (`db-http-retries.test.ts`):

| Call                          | Second attempt                                                  | Change                                                                        |
| ----------------------------- | --------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| write (puts, deletes)         | a second write, new `seq`; same final value                     | none                                                                          |
| write (merges)                | applied twice                                                   | the unreachable-writer message now says so                                    |
| flush, seal (same successors) | succeed again (idempotent)                                      | none                                                                          |
| fork                          | idempotent retry (`database-lifecycle.test.ts`, not rerun here) | none                                                                          |
| create                        | `DatabaseAlreadyExists` (409)                                   | message: a completed first attempt answers this way, the database is ready    |
| commit                        | `TransactionConflict` (`WriteConflict`, 409)                    | message: the first attempt may have committed, read its keys before rerunning |
| remove                        | `DatabaseNotFound` (404) "Create it first"                      | message: never created or already removed, nothing to remove                  |
| drop SST                      | not checked in this audit                                       | none                                                                          |

A `StorageUnavailable` answer for a write or commit whose writer could not be reached
(`WriterUnreachable`) said "Retry the same request". The write may have committed, so the
message now says that retrying is safe for puts and deletes, and that a batch with merges may
apply twice. Codes, statuses, and `retryable` are unchanged. Three mutations, one per message,
are each caught.

**Sizes checked and cleared** (`db-http-limits.test.ts`):

- a 16 KiB key with a 1 MiB value;
- a batch of 10,000 operations totalling 8 MiB, and 10,001 operations refused;
- an empty key;
- a reversed range, and an empty range.

**Not fixed here (reported to their owners):**

- **Seal with other successors.** Sealing an already-sealed database with different successors
  succeeds and keeps the first ones.
- **`Concat` merges.** Their results are unbounded: 1 MiB per operand, any number of operands,
  resolved in the API isolate.
- **Request bodies.** No limit applies below Cloudflare's own (100 MB on Free and Pro), and the
  API isolate decodes a body whole.

## D89. A shard router refreshes a map older than a second before a read that never asks the writer

A router learns that its cached map is stale only from a routing error: a sealed, out-of-range,
or removed database. `Latest` reads set `refuseSealed`, and the writer's answer reports the seal
(D57). `Flushed` and `AtLeast` reads read a manifest and never ask the writer, so a sealed shard
does not fail them.

A router whose map predated a split therefore served the sealed shard's data at `Flushed`
indefinitely, `get` and `scan` both, after the successor had newer flushed data. It stayed stale
for as long as its isolate cached the map: unbounded, against a lag of about a second for
`Flushed`. The sharded HTTP API reads `Latest` only, so the defect was reached through the
`ShardRouter` API.

- **Rule.** Before a `Flushed` or `AtLeast` read without a snapshot, the router refreshes a map
  older than `SHARD_MAP_MAX_AGE_FOR_UNPROBED_READS_MS` (1 s). The cost is one directory read per
  group per second per isolate, only while such reads run. Reads at a snapshot read a fixed
  database and are not rerouted.
- **Test:** `shard-router-staleness.test.ts`. Two routers cache the one-shard map; the shard is
  split, and key 1 is overwritten in its successor and flushed. After the age bound, a `Flushed`
  get and a `Flushed` scan through the stale routers return the new value. Mutations caught: no
  refresh, a scan without the refresh, and an age bound never reached.
- **Not changed.** `DbClient` could refuse a sealed database at `Flushed` from the manifest's
  sealed state, which would make the router exact rather than bounded. That is
  `db-client.ts`'s owner's call.

## D90. A removed database's id is reusable only `gcMinAge` after its removal

**The defect.** `remove` deleted every object under `db/<id>/`, and `create` with the same id
then started a new manifest chain at id 1. A reader isolate that had cached a manifest of the
removed database went on serving it as the new database's:

- `ManifestCache.covering` hit on it, because its `lastL0Seq` was at least the new writer's
  `flushedSeq`;
- `current` served it for `manifestRefreshInterval`;
- `loadWith` kept it over a freshly loaded manifest because its id was higher, even after its
  hold window had ended.

Nothing in a manifest or in the writer's probe names an incarnation, so a hit (no I/O) could not
tell the two chains apart.

Test, before the fix (`database-lifecycle.test.ts`, "a reader that cached the removed database's
manifest reads the new database, not the old"): five flushes, then remove, create, and one write
of `a=new`.

- A `Flushed` get of `a` returned the removed database's `old-5`, served from the L1 block cache.
- A `Latest` get failed with `StorageUnavailable`, as did a `Flushed` get of a key only the
  removed database had.

**The rule.** A cached manifest is held at most `max(gcMinAge / 2, manifestRefreshInterval)`
after its load began (D67), and every manifest of the removed database was loaded before its
removal. So once `gcMinAge` has passed since the removal, no isolate can serve one, and the id
is reusable.

- **`create`** loads the newest manifest of the id first. If it is a `Removed` tombstone (D91)
  younger than `gcMinAge` (from its `removedAt`), `create` fails with
  `DatabaseRecentlyRemovedError`. The public code is `DatabaseRecentlyRemoved`: status 409,
  `retryable: true`, `details.retryAfterMs`, and a message naming the time. This is a new public
  error, allowed because v1 is not released, and the OpenAPI document is regenerated.
- **An older tombstone is cleared first.** If the removal stopped before it destroyed the writer's
  and compactor's state, `create` destroys them. Then it deletes every object under the prefix
  written no later than the tombstone (or `gcMinAge` ago), plus the `LATEST` hint and GC's
  manifest boundary, and only then writes manifest 1. The test "a stale GC manifest boundary and
  LATEST hint do not hide or refuse the new chain" failed before the clearing:
  - **A stale boundary** (7 in the test): `ManifestStore` refuses manifest 1 at or below it.
  - **A stale `LATEST` hint:** it never moves back, so every cache miss listed manifests.

  A concurrent create of the same id writes only later objects, so they are kept.

- **`ManifestCache.loadWith`** prefers a cached manifest with a higher id only while that entry
  is still inside its hold window.

**Rejected.**

- **An incarnation id in every manifest and in the writer's probe.** It would make any reuse
  safe at once, but it changes the stored format and the writer protocol, for a case a one-hour
  wait covers.
- **Never reusing an id.** That needs a permanent tombstone, and it refuses a legitimate pattern:
  remove, then later create a fresh database under the same id.

**What remains.**

- **An old handle.** A client holding a `DbSnapshot` or a transaction's begin manifest id from the
  removed database, used after the id was reused, names a manifest id in the new chain. If that id
  exists, the read resolves against the new database instead of failing with `SnapshotExpired`.
  Its promise had ended with the removal (checkpoints are deleted with the database). This is a
  risk that an incarnation id would close.
- **Two creates at once.** Two concurrent creates of a reused id whose removal never destroyed
  its writer's state can race. One may destroy the other's just-started writer state. Both
  creates must find the uncleared tombstone `gcMinAge` after the removal.

**Tests** (`database-lifecycle.test.ts`):

- **Recreate.** `create` is refused within `gcMinAge` (also 1 ms before it) and succeeds at it.
  `Latest` and `Flushed` reads then see only the new database, at once and 2 h later.
- **Leftovers.** A stale boundary and hint neither refuse nor hide the new chain. A concurrent
  create's young object and a live database's objects survive the clearing.
- **Reads after removal.** A warm isolate's `Latest` read reports `DatabaseNotFound` at once, and
  its `Flushed` read after `manifestRefreshInterval` (D29 allows that lag).
- **The public error.** `toPublicError` maps the refusal to `DatabaseRecentlyRemoved`,
  retryable, with `retryAfterMs`.
- **Mutations.** Each is caught:
  - `create` ignores the tombstone, or refuses for half of `gcMinAge` only;
  - `loadWith` keeps an expired entry with a higher id;
  - `create` clears no leftovers;
  - the clearing ignores its time filter, or runs for a live database;
  - the public error is not retryable.

## D91. A removal's `Removed` manifest stays at the head for `gcMinAge`, as its tombstone

**The defect.** `markRemoved` is a manifest edit, so a commit that reloads after it carries
`Removed` forward. But `remove` then deleted every manifest, its head too. A committer that had
loaded the newest manifest before the removal could create the next id once its key was free:
its edit had been applied to the old view, so the new manifest was `Open`. GC's boundary was
deleted too, so the boundary re-read passed.

Six tests (`database-lifecycle.test.ts`, "a <committer> that lands after the removal's deletes
does not bring the database back") each commit from a view loaded before `remove`, through
`ManifestStore.updateFrom`. Before the fix, each committer brought the database back:

- **Flush, compaction, collection, checkpoint, fork hold:** the database was `Open` again, and
  a later write was acknowledged.
- **Seal:** the database was `Sealed`, with no `Removed` manifest left.

The writer's own flush is serialized with removal by `DbWriter.destroy`'s locks. The others are
not.

**The rule.** The `Removed` manifest is the removal's tombstone and stays at the head of the
chain for `gcMinAge`.

- **`markRemoved(manifest, now)`** records `removedAt`. After the writer's and compactor's state
  is destroyed and the holds are released, a second edit, `markRemovalCleared`, records
  `cleared`.
  - Both fields are new and optional in the stored format.
  - A manifest without `removedAt` counts as removed at the moment it is read.
  - A manifest without `cleared` counts as not cleared.
- **`remove` keeps the head.** It moves GC's manifest boundary to just below the head. It then
  deletes everything else written no later than the head, except the `LATEST` hint and the
  boundary. Before each page it checks that the head is still this tombstone. A commit from any
  older view then creates an id at or below the boundary, loses, reloads, finds `Removed`, and is
  refused (a stale writer epoch, `addCheckpoint`, `holdForSuccessors`) or is a no-op
  (`commitCompaction`, `sealManifest`, and `prepareGarbageCollection` now change nothing on a
  `Removed` manifest). Exactly one manifest object is left, the head.
- **A create at or below the boundary** now deletes the object it wrote (`ManifestStore`'s create).
  No reader looks there.
- **Readers.** `ManifestCache` answers `DatabaseNotFound` for a `Removed` head. So do the status,
  checkpoint list, and compaction endpoints (`ManifestReader.loadLive`).
- **The maintenance sweep** finishes a cleared tombstone older than `gcMinAge`. It deletes every
  object under the prefix older than `gcMinAge`: the tombstone, its hint, the boundary, and
  anything that landed later, such as an SST a compaction step uploaded. It leaves an uncleared
  tombstone to a retried `remove` or a `create`, because it cannot reach the writer.

**The assumption, and who holds a view.** No view of a manifest chain outlives `gcMinAge`, which
is the same assumption GC rests on (D29, D33). Once the tombstone is gone, nothing can create
above it.

| Holder                                                       | Its view, and how long it lives                                                                                                                                   |
| ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The writer                                                   | Its own view (`updateFrom`). `remove` destroys its state under its commit and flush locks first, and a restarted writer's `fenceWriter` refuses a `Removed` head. |
| A compactor                                                  | Loads the newest manifest on every poll and fences in with `update`.                                                                                              |
| A compaction Workflow step                                   | Its commit is `ManifestStore.update(commitCompaction)`, which loads the newest manifest at the commit. It never creates from a view carried across steps.         |
| A collection                                                 | `update(prepareGarbageCollection)`, loaded at the commit.                                                                                                         |
| The lifecycle's own edits (checkpoint, seal, fork hold, pin) | `update`, loaded at the commit.                                                                                                                                   |

A view taken with `update` lives for one CAS round: a load, then one create request.

**Rejected.** A check after each create that its predecessor still exists, undoing the commit
when it does not. It costs every commit of every database one more read. It also races: a second
stale committer can build on the first one's doomed manifest and pass its own check before the
first deletes it, leaving an open manifest alone at the head.

**Tests and model.**

- **The six committer tests.** Each commit is refused or a no-op, and one manifest object is
  left.
- **An interrupted removal.** The removal stops after `markRemoved` with unflushed rows in the
  writer. The sweep leaves the uncleared tombstone. `create` `gcMinAge` later starts a writer
  whose first write is sequence number 1, and nothing of the old incarnation is readable.
- **A removal still deleting** when, `gcMinAge` later, the id is created and flushed: it deletes
  none of the new database's objects.
- **Tombstone shape.** Removal leaves exactly the head, its hint, and the boundary, and the sweep
  deletes them `gcMinAge` later. A late SST upload and `LATEST` hint go with them.
- **Mutations.** Twelve run, eleven caught:
  - the clearing skips the state;
  - the deletes ignore the tombstone's time;
  - the head is deleted;
  - the boundary is not moved;
  - a seal, a compaction, or a collection commits on the tombstone;
  - readers serve the tombstone;
  - the sweep deletes an uncleared tombstone;
  - `removedAt` is not recorded;
  - a create below the boundary leaves its object.

  One is equivalent: the sweep's own `removedAt` check repeats its age filter.

- **Model.** `ManifestCasFencing.tla` gains `WithRemove`, `ManifestCasFencingRemove.cfg`
  (3,033 states, about 2 s; with GC as well it does not finish in 150 s), and
  `NoResurrection`. The broken variant `removalDeletesHead` fails it.

**The manifest edits as a state machine.** `manifest-state-machine.test.ts` applies random
sequences of every edit and checks these invariants after each step. Several were assumed and
written nowhere:

- `lastL0Seq`, `lastL0ClockTick`, both epochs, and `gcSstBoundary` never decrease;
- no SST is named twice, and none is named again after it left;
- sorted run ids are unique and newest first, and each run's SSTs are key-ordered and disjoint;
- the state only moves Open, Sealed, Removed, and a seal or removal fences the writer;
- no flush commits after a seal or removal;
- checkpoint ids are unique, and each pins a manifest of the chain;
- a dropped SST is not in the manifest;
- `changelogAfterSeq` is at most `lastL0Seq` and never decreases, and the changelog is
  contiguous from it to `lastL0Seq`;
- a fork's views lie inside its range, it continues its source's sequence numbers, starts open
  with epochs 0, and names one hold, on its source.

The generator models a writer and a compactor that each carry the epoch they fenced in with:
3,000 runs of 30 to 80 steps, about 1 s. Mutations (`mut/d91.json`):

- **Caught, 8:** compaction leaves runs unsorted; a seal or a removal does not fence the writer;
  the GC boundary may move back; pruning does not advance `changelogAfterSeq`; a fork's views are
  not restricted; a duplicate checkpoint id is added; a flush lowers `lastL0Seq`.
- **Equivalent, 1:** `commitL0Flush` without its `Sealed` check. The epoch alone refuses the
  flush, because `fenceWriter` never hands out an epoch after a seal.

**A fork's hold is not a caller's checkpoint.** `deleteCheckpoint`
(`DELETE /v1/databases/<id>/checkpoints/<cp>`) deleted any checkpoint, a fork's hold among them,
after which the source's collection could delete the SSTs the fork reads. It now refuses a hold
whose holder still exists, with `DatabaseHasDependentsError` (409, already public). The fork's
collection releases the hold (D77), or the fork's removal does. A hold whose holder is gone stays
deletable, for repair. `forkRange`'s cleanup of its own hold, when the fork id belongs to another
database, removes it directly.

Tests (`database-lifecycle.test.ts`):

- "cannot be deleted as a checkpoint while the fork holds it" failed before: the hold was
  deleted.
- "is released when the fork's id belongs to another database" covers the cleanup path.

Two mutations, both caught: `deleteCheckpoint` ignores the holder; the cleanup uses the refusing
delete.

## D92. Cloudflare's platform limits against what shaledb can ask of them

An audit of every platform limit the design touches. Each documented value below was read from
developers.cloudflare.com on 2026-11-26, from the page named, whose last change is the date given.
Each demand is the largest one that the default settings (`DEFAULT_LSM_SETTINGS`) and the schema
limits allow (`MAX_KEY_BYTES` 16 KiB, `MAX_VALUE_BYTES` 1 MiB, `MAX_BATCH_BYTES` 8 MiB, 1,000 rows
per scan or change-feed page). Every row is marked: **measured** (on a preview,
`docs/measurements.md`), **derived** (computed from the code and the settings), or **docs** (the
documentation only).

**Pages read:**

- workers/platform/limits (2026-11-25);
- durable-objects/platform/limits (2026-11-13);
- r2/platform/limits (2026-10-30);
- r2/api/workers/workers-api-reference (2026-11-04);
- workflows/reference/limits (2026-11-10);
- durable-objects/api/alarms (2026-11-18);
- workers/configuration/cron-triggers (2026-11-18);
- workers/runtime-apis/context (2026-11-18);
- workers/runtime-apis/rpc (2026-10-12);
- secrets-store/manage-secrets (2026-10-22);
- workers/configuration/routing/workers-dev (2026-11-06);
- workers/observability/logs/tail-workers (2026-10-02).

| Limit (documented value)                                                                                                                                                   | Where shaledb meets it                                                                                                                                  | Largest demand                                                                                                                                                                                                                                                        |                       Margin | Kind                                                                    |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------: | ----------------------------------------------------------------------- |
| Workers CPU per invocation: 30 s default, up to 5 min                                                                                                                      | API Worker and writers 30 s, compactor 60 s (`limits.cpu_ms`, D61)                                                                                      | largest invocation 9.8 s (a 256 MiB flush), compaction step 17.7 s                                                                                                                                                                                                    |                     3×, 3.4× | measured                                                                |
| Same, one compaction step whose rows are dropped                                                                                                                           | `CompactionExecutor.writeOutput` ends at its output's size only                                                                                         | reads every input of the job: about 0.07 to 0.28 s CPU per input MiB (D61), so 60 s at about 0.2 to 0.9 GiB of deleted or expired input                                                                                                                               |                         < 1× | derived; finding 2                                                      |
| Memory per isolate: 128 MB                                                                                                                                                 | API Worker (readers, L1 caches), writer and compactor Durable Objects, Workflow steps                                                                   | L1 caches 48 MiB by default. A `Latest` scan holds every unflushed row of its range, up to 256 MiB. A flush page holds 1,024 rows, a writer scan page 1,000 rows, a public scan page 1,000 rows: up to 1 GiB each with 1 MiB values. GC holds every retained manifest |                         < 1× | derived; findings 3, 4                                                  |
| Subrequests per invocation: 10,000 (Paid)                                                                                                                                  | flush parts, compaction reads (2 MiB chunks), scans, GC lists, loads, and deletes                                                                       | flush 256 MiB: 32 parts. A 1,000-row scan page of 1 MiB rows: one block per row, doubled by hedges, about 2,000 reads. One GC collection: one read per manifest superseded within `gcMinAge`, so about 5,000 at 1.4 commits/s                                         |                 300×, 5×, 2× | derived; finding 4. The compactor Worker is set to 10 million since D99 |
| Six connections waiting for headers                                                                                                                                        | scans and hedged reads (D72)                                                                                                                            | read-ahead of 8 blocks per SST over up to 24 SSTs                                                                                                                                                                                                                     |  queueing, not failure (D72) | docs                                                                    |
| `waitUntil`: 30 s after the response                                                                                                                                       | `BackgroundWork`: the `LATEST` hint, fork warming, compactor wakes (D56, D57, D73)                                                                      | a cold compactor wake took 5.4 to 9.2 s (D73)                                                                                                                                                                                                                         |                           3× | measured                                                                |
| Variable size: 5 KB; 128 variables per Worker                                                                                                                              | `SHALEDB_LSM_SETTINGS` and three more (D55)                                                                                                             | the settings JSON with every field at its longest number is under 5,000 bytes (`shaledb-stack-config.test.ts`)                                                                                                                                                        |                 > 1× by test | derived                                                                 |
| Worker size: 64 MiB; startup: 1 s                                                                                                                                          | every Worker                                                                                                                                            | 3.35 MB uploaded (`deploy-p1` output)                                                                                                                                                                                                                                 |    19×; startup not measured | measured                                                                |
| Log data per request: 256 KB                                                                                                                                               | warnings and errors only                                                                                                                                | a few lines per request                                                                                                                                                                                                                                               |                        large | derived                                                                 |
| DO SQLite storage per object: 10 GB                                                                                                                                        | the writer's memtable                                                                                                                                   | `maxRetainedBytes` + 2 × `maxUnflushedBytes` + one batch = 584 MiB (audit risk 5c)                                                                                                                                                                                    |     16×, now a settings rule | derived                                                                 |
| DO row or value: 8 MB                                                                                                                                                      | one memtable row                                                                                                                                        | 16 KiB + 1 MiB                                                                                                                                                                                                                                                        |                         7.5× | derived                                                                 |
| DO SQL: 100 bound parameters, 100 KB statement                                                                                                                             | `MemTable` statements                                                                                                                                   | fixed statements, one row each                                                                                                                                                                                                                                        |                            — | derived                                                                 |
| DO soft limit: 1,000 requests/s per object                                                                                                                                 | one writer per database                                                                                                                                 | 65 requests/s with the writer 48% busy (100-row batches, D61), so about 136 at its ceiling                                                                                                                                                                            |                           7× | measured                                                                |
| DO alarm wall time: 15 min                                                                                                                                                 | writer flush, compactor poll and collection                                                                                                             | flush of 256 MiB: 82 s (D61). Collection: one sequential manifest read per retained manifest, about 350 s at 5,000                                                                                                                                                    |                    11×, 2.6× | measured, derived                                                       |
| DO alarm retries: 6, from 2 s, only when the handler throws                                                                                                                | writer `runAlarm` catches and reschedules. Compactor `runAlarm` catches the typed failures; a defect is retried 6 times, then the next wake restarts it | —                                                                                                                                                                                                                                                                     |                            — | derived                                                                 |
| RPC: 32 MiB                                                                                                                                                                | not used: Workers and Durable Objects talk HTTP over bindings                                                                                           | —                                                                                                                                                                                                                                                                     |                            — | derived                                                                 |
| DO outbound request body: 100 MiB                                                                                                                                          | the API Worker's calls to the writer                                                                                                                    | a batch of 8 MiB, base64 in JSON (about 11 MB)                                                                                                                                                                                                                        |                           9× | derived                                                                 |
| R2 object: 4.995 TiB; part: 5 GiB less 5 MiB; 10,000 parts                                                                                                                 | SSTs at `multipartPartSize` 8 MiB                                                                                                                       | 32 parts per flush; 8 per 64 MiB output                                                                                                                                                                                                                               |    300×, now a settings rule | derived                                                                 |
| R2 list: 1,000 per call; delete: 1,000 keys per call                                                                                                                       | `ObjectStore.list`, `ObjectStore.delete` (batches of 1,000)                                                                                             | paged                                                                                                                                                                                                                                                                 |                            — | derived                                                                 |
| R2: one write per second to the same key (429 above)                                                                                                                       | `manifest/LATEST` (every commit, in the background), `gc/manifest.boundary`, `maintenance/sweep.cursor`                                                 | about 1.4 hint writes/s with small flushes back to back. A failed hint lags, and readers probe past it (D56)                                                                                                                                                          |  < 1× for the hint, harmless | derived                                                                 |
| R2 conditional put                                                                                                                                                         | manifest create-if-absent                                                                                                                               | 16 concurrent creates: 1 `Created`, 15 `AlreadyExists`                                                                                                                                                                                                                |                            — | measured                                                                |
| Workflows: step result 1 MiB (non-stream)                                                                                                                                  | `prepare` returns every input `SstHandle`                                                                                                               | about 220 B per handle with 16-byte keys, 43,868 B with 16 KiB keys. A job over 4 runs of 6 SSTs with 16 KiB keys is 1,053,051 B; 4 runs of 1,300 SSTs with 16-byte keys is 1,133,867 B                                                                               |          < 1× for large jobs | derived; finding 1                                                      |
| Workflows: event payload 1 MiB                                                                                                                                             | job parameters: SST ids only                                                                                                                            | small                                                                                                                                                                                                                                                                 |                        large | derived                                                                 |
| Workflows: 10,000 steps per instance                                                                                                                                       | one step per 64 MiB output                                                                                                                              | 625 GiB of output per job                                                                                                                                                                                                                                             | large; finding 1 binds first | derived                                                                 |
| Workflows: 50,000 concurrent instances; 100 created/s per Workflow                                                                                                         | `compactionBudgetCapacity` 32                                                                                                                           | 32 running; 200 started in 3 s on `preview-3` (D61)                                                                                                                                                                                                                   |  1,500×, now a settings rule | measured                                                                |
| Workflows: completed state kept 7 days; 10,000 retries per step                                                                                                            | the compactor reads a job's progress while it runs (lease 15 min)                                                                                       | —                                                                                                                                                                                                                                                                     |                            — | derived                                                                 |
| Cron: CPU 30 s default under hourly; wall 15 min                                                                                                                           | the maintenance sweep every 5 min, 50 databases a page                                                                                                  | about 160 ms CPU per run (D60)                                                                                                                                                                                                                                        |                         180× | measured                                                                |
| Cache API: 512 MB per object; 1,000 calls per request                                                                                                                      | `BlockServer` caches blocks and metadata                                                                                                                | one block or one SST's metadata per call                                                                                                                                                                                                                              |                        large | derived                                                                 |
| Tail Workers                                                                                                                                                               | `CpuTailWorker`                                                                                                                                         | what the platform drops under load is not documented on its page                                                                                                                                                                                                      |                      unknown | docs                                                                    |
| Secrets Store: 100 secrets per account (open beta); one store; 65,536 bytes each                                                                                           | two token slots per live stage (D64)                                                                                                                    | 8 today, plus 6 orphaned `CF_SLATEDB_*`                                                                                                                                                                                                                               |                           7× | docs                                                                    |
| `workers.dev`: "treated as a Free website … intended for personal or hobby projects that aren't business-critical"; a route or custom domain is recommended for production | the production API URL (P3)                                                                                                                             | —                                                                                                                                                                                                                                                                     |                            — | docs; finding 5                                                         |

**Findings.** The code paths are other auditors' files. Each has a failing test, or a measured
path, in `/tmp/shale-work/agents/audit-limits/`.

1. **A large compaction's `prepare` result exceeds 1 MiB** (`zz-limits-step-result.test.ts`).
   Such a step can never be persisted. The job fails, the compactor replans it, L0 fills, and
   writes are refused for good. The proposed rule: no step result carries an unbounded list.
2. **One output step reads all of a job's droppable input** (`zz-limits-compaction-step.test.ts`:
   13 reads and the whole job in one step, against 4). It fails the same way at a few hundred MiB
   of deleted or expired data. The proposed rule: a step also ends after a bounded number of input
   bytes.
3. **A `Latest` scan reads every unflushed row of its range before emitting one**
   (`zz-limits-latest-scan.test.ts`: 6 writer calls for a one-row scan over 5,000 rows). It also
   holds them all. Pages are bounded by rows, not bytes: 1,024 or 1,000 rows of up to 1 MiB.
   The proposed rule: every page is bounded by bytes too, and the scan merges pages lazily.
4. **One collection loads, one after another, and holds every manifest superseded within
   `gcMinAge`** (`zz-limits-gc-loads.test.ts`: 404 loads after 400 commits, against 24 after 20).
   Its cost follows the commit rate, not the data, so it fails on every retry above a few commits
   a second, and garbage is then never collected.
5. **`workers.dev` for production (P3)** goes against Cloudflare's own guidance. A custom domain
   needs a zone in the account. The checklist's limitations say so.

**Fixed here: settings that the platform cannot hold are refused** (`LSM_PLATFORM_LIMITS`,
`lsm-settings.ts`):

- the largest flush and each compaction output fit in 10,000 R2 parts;
- a part is at most R2's largest;
- the two L1 caches together are below one isolate's memory;
- the writer's SQLite fits in a Durable Object: `maxRetainedBytes` + 2 × `maxUnflushedBytes` +
  one batch;
- the compaction budget is at most the account's concurrent Workflow instances.

The defaults pass each rule with the margins above. Tests: `lsm-settings.test.ts` checks each rule
at its edge, and six mutations are each caught. `shaledb-stack-config.test.ts` checks that the
bound settings fit a Worker variable.

**Costs that grow without use** (derived):

- **The maintenance sweep.** It runs every 5 minutes on every live stage, so 288 runs a day,
  each visiting up to 50 databases, idle or not.
- **The writer's newest flush.** Every idle database keeps its newest flush's rows in its
  writer's SQLite indefinitely: the trim never removes the newest flush (D58), so up to
  `l0SstSize` (64 MiB) of Durable Object storage per database.
- **Checkpoints.** A checkpoint without a lifetime keeps every SST its manifest names, forever.
  The number of checkpoints, and the changelog's retention (`changelogRetentionMs`, no maximum),
  are unbounded, so the manifest grows with them. Every commit rewrites the manifest whole, and
  every cold read decodes it.
- **Traces and tail records.** They are kept 14 days. Every invocation that did not end `ok` is
  kept (D71).
- **Workflow instance state.** Each compaction's state is kept 7 days by the platform.
- **Stale multipart uploads.** R2 aborts them after a day (D63).
- **Idle compactors cost nothing:** an idle database with no garbage sets no alarm (D60).

## D93. A Latest scan pulls the writer's pages as it needs them; every memtable page is bounded by bytes

**Found by the limits audit.** A `Latest` scan read every unflushed row of its range from the
writer, 1,000 rows per call, before it returned one, whatever its limit:

- **Memory:** up to `maxUnflushedBytes` (256 MiB) in one 128 MB API isolate.
- **Subrequests:** one per 1,000 rows. With 5,000 unflushed rows, a scan for one row made 6
  writer calls.
- **Pages:** none was bounded by bytes. The writer's scan page, the flush's 1,024-row page and the
  change feed's page could each hold 1,000 values of 1 MiB.

**The rules.**

- **Every memtable page is bounded by bytes as well as rows.** `MemTable` reads each row's size
  first (`LENGTH(key) + LENGTH(payload)`, same conditions and order), then the rows that fit in
  `MAX_MEMTABLE_PAGE_BYTES` (8 MiB). A page always holds at least one row, and returns whether
  more may follow (`complete`). This covers the writer's scan page, the flush's page, and the
  change feed's page.
- **A change feed page holds whole batches.** It backs off to the start of a batch cut by its byte
  bound or row limit. A page that is all one batch returns that whole batch, which holds at most
  8 MiB (`MAX_BATCH_BYTES`).
- **A `Latest` scan reads in segments** (`DbClient` `scanUnflushed`):
  - A segment reads writer pages, at the snapshot the first page fixed, until its rows hold a
    second key or the range ends.
  - It covers the keys before the last key read, so it has every version of each.
  - It merges with one manifest between D70's bounds: at least the largest `flushed_seq` of every
    page so far, at most the snapshot.
  - The next segment starts at the last key.
  - A scan that has its rows reads no more pages. Memory is one segment: a page, plus the versions
    of one key that span pages.
- **A flush that overtakes the scan between segments** leaves no manifest between the bounds.
  - Before any row was returned, the scan starts again at a new snapshot, as D70 did.
  - After rows were returned, a restart would mix two snapshots. Instead the scan fails retryably
    (`ReaderUnavailableError`, `StorageUnavailable`) with `resumeFrom`, the first key it did not
    return. Every row before it was returned, and none at or after it.
  - The public scan fails that page, and the client repeats the request.

**Also fixed: the change feed split large batches** (found while bounding its page).
`DbWriter.changes` capped its page at 1,000 rows, so for an unflushed batch of 1,500 rows
`DbClient.changes` received 1,000 and moved its cursor past the batch: 500 changes skipped
silently. The changelog export resumed each 1,024-row page after its last row's sequence number,
which dropped the rest of a batch the page had cut. Both now page by whole batches.

**Tests.**

- **`db-client-scan.test.ts`:**
  - the limits audit's case: one writer page, not six;
  - a scan overtaken after returning rows: its error's `resumeFrom` is the next key, and the
    resumed scan returns every key exactly once;
  - pages of large values: 40 rows of 512 KiB give at most 16 rows per page;
  - a property against the oracle with writes, flushes and compactions between pages. A scan
    returns the database at one snapshot, or a prefix at one snapshot cut exactly at
    `resumeFrom`.
- **`mem-table.test.ts`:** scan, feed and flush pages at the byte bound; feed pages cut only
  between batches.
- **`change-feed.test.ts`:** a 1,500-row batch returned whole (1,000 of 1,500 before).
- **Mutations:** 6 of 7 caught. The missed one keeps the last key's partial versions in a segment.
  It is equivalent, because the segment's range ends before that key.

**Follow-ups (in D102's round):**

- **The public scan.** A scan that fails with `resumeFrom` after returning rows now ends its page
  early with those rows and `resumeFrom`; before, it failed the whole page.
- **`ShardRouter.scan`.** It resumes the shard's part from `resumeFrom` within its deadline, then
  goes on to the following shards.
- **The feed's flushed part** (`DbReader.changelogRows`). It is bounded the same way, at a batch
  boundary. A page cut there ends the page; appending the unflushed rows after it would skip the
  flushed changes between them.
- **Still open:** the public scan page's own byte bound belongs to D100 (`audit-api`).

## D94. A compaction job unseen for compactionLeaseMs is finished

**Found by the Workflow audit.** The compactor mapped every failure of `launcher.progress` to
`Running`, including a Workflow instance that no longer exists (purged, or never created). Such a
job stayed recorded for good: its lease was renewed on every poll, and its sources were reserved,
so its L0 or runs were never compacted again. With D96's rule that GC holds back while a job runs,
it would also stop SST collection.

**The rule.**

- A running job records when a progress read last said it may still commit (`lastProgressAt`;
  absent means `startedAt`).
- A job no read has seen running for `compactionLeaseMs` (15 minutes) is finished, with no
  outcome: its lease is released and its sources are planned again.
- A long job that is read as running is kept, however long it takes.
- If the abandoned job does commit later, `commitCompaction`'s exact-inputs check refuses it once
  another job has taken its sources.

**Format.** `lastProgressAt` is optional in the compactor state, so the format stays 1. The
compaction status response gains it as an optional field, which is additive.

**Tests** (`compactor.test.ts`):

- a job whose reads all fail is kept before the bound and replanned after it;
- the bound counts from the last read that saw the job running.

Three mutations are caught.

**Left to the Workflow launcher's owner.** `progress` cannot yet tell a missing instance from an
unreachable service, and it counts the status `unknown` as running.

## D95. A seal naming other successors than the recorded ones fails

**Found by the API audit.** Sealing a sealed database with different successors succeeded and kept
the old ones. With holds it also took holds for the new successors on the manifest sealed for the
old ones.

**The rule.**

- A seal of a sealed database with the same successors, in the same order, is a retry and
  succeeds.
- A seal with other successors fails with `DatabaseSealedError` naming the recorded ones (409).
- `ShardSplit`'s retry passes the pending split's own successors, so it is unaffected.

**Test** (`db-writer.test.ts`): other successors are refused, with and without holds; the same
successors succeed; only the first successors hold checkpoints. One mutation is caught.

## D96. A collection keeps what a running compaction may have written

**Found by the Workflow audit.**

1. A compaction job writes its outputs one step at a time and commits them at the end. Until then
   no manifest names them.
2. If the job's steps span more than `gcMinAge` (retries, waits for budget, a large merge), a
   collection meanwhile deletes its early outputs: they are unreferenced and older than
   `now - gcMinAge`. It also moves `gcSstBoundary` past them.
3. The commit is refused (`SstBelowGcBoundaryError`). The replanned job takes as long, so a merge
   or fork rewrite longer than about an hour of steps never commits (about 15 GiB at D61's ~15 s
   per 64 MiB step).
4. Sorted runs then pile up, and a large fork never releases its source.

**The rule.** A collection never deletes an unreferenced SST, and never advances `gcSstBoundary`,
at or past the start of the oldest job its compactor records as running. The cutoff is
`min(now - gcMinAge, oldest running job's startedAt - 60 s)`.

- **Only the compactor collects a database that has a manifest.** Every collection of a database
  with a manifest runs in its compactor (`Compactor.collect`, `requestCollection`, and the alarm,
  all through `runCollection`), which passes the oldest running job's start. The maintenance sweep
  called `GarbageCollector.collect` itself whenever it could not load a manifest, for any reason.
  It now calls `collectLeftovers`, which deletes only what a database with no manifest left, and
  leaves alone one that has a manifest.
- **The start is taken before the launch.** `startedAt` is read before `launcher.start`, so no
  output is minted before it. Before, it was read after `start` returned.
- **The 60 s margin** covers skew between the Workflow's clock, which mints the outputs' ULIDs,
  and the compactor's clock, which records the start. The first output is written after
  `prepare` has read the manifest.
- **When the cap is lifted.** A finished, failed, or discarded job leaves `running` at the next
  poll. Its outputs are then collected `gcMinAge` after they were written, as before. D94 bounds
  how long a job can be recorded as running.
- **Safety.** A lower cutoff deletes a subset of what a collection deleted before, and never
  lowers the boundary, so GC safety is unchanged. The `verify-gc-safety` skill records this; no
  model step changed.

**Tests.**

- `garbage-collector.test.ts`: a job whose steps span more than `gcMinAge` keeps its output
  through a collection given its start, and commits. Older garbage is still deleted, the sweep's
  leftover pass deletes nothing from it, and a discarded job's output is collected `gcMinAge`
  later.
- `compactor.test.ts`: a collection is told the running job's start, and the start precedes the
  launch.
- Four mutations caught: the cap ignored, the compactor passing none, `startedAt` taken after the
  launch, and the leftover pass collecting a live database.

## D97. A compaction step is bounded by the input it merges

**Found by `audit-limits`.**

- **The defect.** An output step ended only when its output reached `compactionOutputSstSize`.
  When most input rows are dropped, one step merges every input of the job in one invocation.
  That happens with deletes at the bottom, values expiring together, or a key rewritten many
  times. Their failing test (`zz-limits-compaction-step.test.ts`): with 2,000 deleted 10 KB rows,
  the first step made 13 reads, all of the input, and wrote nothing.
- **The effect.** D61 measured up to 17.7 s of CPU for a step of 64 MiB in and out, about 0.28 s
  per input MiB. Against the compactor's 60 s, a step over roughly 0.2 GiB of droppable input
  fails on every retry. The job never commits, L0 fills, and writes are refused.

**The rule.** A step also ends once it has merged `COMPACTION_STEP_INPUT_FACTOR` (1.5) ×
`compactionOutputSstSize` of input, counted with the same row-size estimate as the output. It
ends between keys and returns the key the next step starts at. For 64 MiB outputs that is 96 MiB
of input, at most 27 s at D61's worst rate: half the limit. No setting is needed: the bound scales
with the output target.

- **How it counts dropped keys.** lsm-core's `compactKeys` emits every key with what it keeps
  (possibly nothing), so the bound also counts keys whose versions are all dropped.
  `compactRowStreams` is that stream without the empty keys.
- **A step may write no SST.** Its result is then `{ output: none, resumeFrom: key }`.
  - `runCompactionJob` already adds only present outputs.
  - The commit accepts no outputs.
  - The result's size is one key, at most 16 KiB, well inside D86's bound.
- **More steps.** A job takes up to (its input ÷ 96 MiB) + its outputs steps, far below the 10,000
  steps a Workflow may take.
- **A span attribute.** `compaction.step_input_bytes` on `CompactionExecutor.writeOutput`
  records each step's input.

**Tests.**

- `compaction-executor.test.ts`: 200 deleted rows at the bottom. The first step writes nothing and
  resumes, the job takes more than 10 steps, commits with no output, and reads find nothing.
- `audit-limits`' test passes on this code.
- Two mutations caught: no input bound, and input not counted.

## D98. A collection that retains too many manifests still makes progress

**Found by `audit-limits`** (failing test `zz-limits-gc-loads.test.ts`).

- **The defect.** One collection loaded every manifest superseded less than `gcMinAge` ago, one R2
  read each, and held them all. After 400 commits that was 404 loads against 24 after 20.
- **What drives it.** The demand grows with the commit rate over the last hour, not with the data.
- **The effect.** Above about 2.8 commits a second, sustained for an hour, a collection exceeds an
  invocation's 10,000 subrequests. It then fails on every retry, so garbage is never collected and
  the pile only grows.

**The rule.**

- **References are folded as each manifest is read.** Each retained manifest's SST references go
  into one set (`RetainedReferences`, lsm-core), with a flag for "reads another database's SST",
  and the manifest is dropped. Memory holds SST ids, not manifests.
  `planSstCollection` and `nextCollectionTime` take that set.
- **A budget on reads.** One collection reads at most `gcMaxRetainedManifestLoads` (5,000, a new
  setting) retained manifests. That is half of the 10,000 subrequests; the rest is listings and
  deletes. Over it, the collection:
  - still deletes the manifests older than `gcMinAge`, which need no read, and advances GC's
    manifest boundary;
  - **defers SST deletion**: it deletes no SST, since an unread manifest may name one;
  - **does not release a fork's hold** (D77), as for a manifest it could not load;
  - records `gc.sst_collection_deferred` (the count left unread) and `gc.retained_manifests` on its
    span and logs a warning that names them;
  - still reports `nextCollectionAt`.
- **The documented limit** (`docs/operations.md`, "Garbage collection and removal"): 5,000 per
  `gcMinAge` is about 1.4 commits a second sustained for an hour. `measure health` reports a
  database whose latest collection deferred as an issue (`health-report.ts`). The `gc.` namespace is
  already among the trace policy's exported attribute namespaces, so no allowlist entry was needed.

**Rejected for v1: each manifest records what it stopped referencing.** If each commit wrote down
the SSTs it dropped, a collection would read only the newest manifest and the dropped lists of the
last `gcMinAge`, with no full manifest reads. That removes the limit, but it changes the manifest
format, every commit edit (`commitL0Flush`, `commitCompaction`, repairs, forks), and
`GcSafety.tla`'s model of what a collection knows. Too large for v1; the budget bounds the failure
to deferred deletion, which reverses itself when the commit rate falls.

**Tests.**

- `garbage-collector.test.ts`: twenty recent commits against a budget of five. The collection
  deletes old manifests, deletes no SST, records the count left unread, and schedules the next run.
  An hour later a collection deletes the four garbage SSTs, and reads return every row.
- `health-report.test.ts`: the latest collection of each database decides the issue.
- `garbage-collection-plan.test.ts`: `planSstCollection` over the folded set.
- Mutations caught: the budget ignored, and SSTs deleted on partial knowledge.
- One mutation survives, and is equivalent: releasing a fork's hold despite deferral. Retained
  manifests are read oldest first, and a fork's manifests read its source only before its
  rewrite, so when a deferred run's read manifests do not read the source, the unread newer ones
  do not either. The guard stays as stated.
- `audit-limits`' test still fails as written: it asks that loads not grow with commits. They now
  grow only up to the budget, which is the decision for v1.

## D99. Bounds on what a caller can grow: checkpoints, the changelog, merged values, the manifest; the compactor's subrequests

D92 found lists and values that a caller can grow without limit. The manifest holds most of them,
and it is rewritten whole on every commit and decoded on every cold read. This decision sets each
bound from a measured size budget. It also sets the compactor Worker's subrequest limit.

**The manifest's size budget.** `manifest-size.test.ts` (lsm-core) measures the encoded size of
each item, and Node's time to encode and decode a whole manifest (`Schema` over `JSON`, as
`ManifestStore` and `ManifestReader` do):

| Item                     | Bytes in the manifest |
| ------------------------ | --------------------: |
| SST handle, 16-byte keys |                   217 |
| SST handle, 16 KiB keys  |                43,868 |
| Checkpoint               |                   130 |
| Changelog entry          |                   286 |

| Manifest | SST handles | Encode | Decode | Heap of the decoded manifest |
| -------: | ----------: | -----: | -----: | ---------------------------: |
|    1 MiB |       4,832 |  10 ms |  12 ms |                      ~10 MiB |
|   10 MiB |      48,321 | 100 ms |  90 ms |               not determined |
|   50 MiB |     241,607 | 600 ms | 410 ms |                     ~194 MiB |

The heap figures were taken without a forced GC, so they are rough; the 10 MiB reading was
negative. They cover the decoded objects only, beside the JSON text.

**The budget is 4 MiB.** At 4 MiB a commit spends about 40 ms encoding and a cold read about 40 ms
decoding. The decoded manifest takes about 16 to 40 MiB of an isolate that also holds 48 MiB of L1
caches and serves many requests, so it fits within 128 MB. At 10 MiB, 100 ms per commit and per
cold read, plus up to 100 MiB of heap, is the point where a commit or a cold read becomes a
problem. These times were measured in Node; workerd was not measured.

> **Corrected by D129:** the cap is now 128. 1,000 fit the manifest, but a collection reads every
> pinned manifest whole.

**Checkpoints: at most 1,000 per database** (`MAX_CHECKPOINTS_PER_DATABASE`, lsm-core
`addCallerCheckpoint`).

- **What counts.** Checkpoints and pinned snapshots that callers create. A fork's or successor's
  hold is not counted, so a split or fork never fails because of a caller's checkpoints.
- **The 1,001st.** It is refused with `CheckpointLimitError`. The public error is
  `CheckpointLimitReached` (409, not retryable). It names how many exist and says to delete one, or
  to wait for a pinned snapshot to expire.
- **A repeat is never refused.**
- **Cost.** 1,000 checkpoints are about 127 KiB, 3% of the budget.

**The changelog: at most its newest 2,048 flushes** (`MAX_CHANGELOG_ENTRIES`, `capChangelog`,
applied in `commitL0Flush`).

- **The bound holds however short the flush interval.** The oldest entries are dropped first, and
  `changelogAfterSeq` is raised past them (D80). A consumer behind them gets a clean `Expired`, and
  GC collects the dropped SSTs.
- **What it holds.** At one flush a minute (`maxL0Age`) that is about 34 hours of changes; a
  database that flushes more often keeps fewer hours.
- **Cost.** About 570 KiB, 14% of the budget.
- **Retention.** `changelogRetentionMs` is now at most 30 days (`MAX_CHANGELOG_RETENTION_MS`),
  like a pinned snapshot's lifetime.

**Strings a caller names.** A checkpoint name may be at most 256 characters, and a budget group at
most 63 (a database id's length). Both are stored in the manifest; neither had a bound.

**SSTs and sorted runs: the v1 database size limit.**

- **What bounds them.** L0 is bounded by `l0MaxSsts` (16), and D87 makes the scheduler able to
  drain it. Compaction keeps the rest in sorted runs of `compactionOutputSstSize` SSTs, so the
  number of handles is about the data's size divided by 64 MiB. Nothing bounds that number but the
  data.
- **A handle's size** is about 169 bytes plus 2.67 bytes per byte of the SST's first and last keys
  (base64), plus as much again for a fork's view.
- **1 TiB with default settings and 16-byte keys** is about 16,400 SSTs, about 3.4 MiB: inside the
  budget.
- **The v1 database size limit is therefore about 1 TiB for keys of up to 16 bytes, and less for
  longer keys.** About 300 GiB for 256-byte keys, about 90 GiB for 1 KiB keys, and about 6 GiB for
  16 KiB keys, where every SST's boundary keys are that long.
- **Larger databases** use range sharding (`packages/lsm-sharding`), or a larger
  `compactionOutputSstSize`.

**Merged values: at most 1 MiB, the largest value a put may write** (`MAX_MERGED_VALUE_BYTES`).

- **Before.** `MAX_VALUE_BYTES` bounded each operand only. N merges of 1 MiB with `Concat` read
  back as one N MiB value in the API isolate, and compaction folded them into one row of N MiB, which
  every read of its block would load.
- **Reads.** A read whose operands resolve past the bound fails with `MergedValueTooLargeError`;
  the public error is `MergedValueTooLarge` (409, not retryable). It tells the caller to replace the
  key with a put (or delete it). This is complete: every read path resolves through
  `resolveVisibleRow`. A key stays unreadable until it is rewritten.
- **Compaction never fails on such a key** (D97's lesson). `retainKeyVersions` leaves a run of
  operands that would fold past the bound as it is, with the base below it. No stored row grows
  past the bound, and a read over what compaction kept answers as it did before.
- **Rejected: refusing a merge write once the key's unflushed operands pass the bound.** It is
  cheap but partial: the writer sees only unflushed operands, so the read bound is needed anyway.
- **SlateDB** puts no bound on a merged value (`slatedb/src/merge_operator.rs`; RFC 0006).

**Other lists checked.**

- `droppedSsts` keeps its newest 32 entries (D62).
- `externalRefs` has one entry per database a fork reads from (its source, and the source's own
  sources in a fork of a fork), bounded by the fork chain.
- Ids are fixed-length ULIDs and database ids at most 63 characters. Keys in handles are at most
  16 KiB each (`MAX_KEY_BYTES`).

**The compactor Worker's subrequests: 10 million** (`limits.subrequests`;
`SHALEDB_COMPACTOR_SUBREQUESTS`, from 1 to 10,000,000).

- **How Cloudflare counts them.** The Workflows documentation counts subrequests per Workflow
  instance: 10,000 by default, configurable up to 10 million with `limits.subrequests`
  (developers.cloudflare.com/workflows/reference/limits, 2026-11-10). The Workers documentation
  counts them per invocation, set per Worker the same way (workers/platform/limits, 2026-11-25). A
  compaction job is one instance.
- **What a job needs.** The Workflow audit counted about 8,600 subrequests for a 10 GiB merge:
  5,100 chunk reads, 1,300 part uploads, 1,300 metadata reads, and 640 manifest reads. That is
  about 860 per GiB, so the default fails every attempt above about 11 GiB.
- **What 10 million covers.** About 11 TiB, past the database size limit above. The same Worker's
  Durable Objects run the compactors' garbage collections: about 5,000 subrequests at 1.4 commits a
  second over the last hour (D92).
- **How it is set.** Alchemy passes a Worker's `limits` to the script upload's metadata, whose type
  has `cpuMs` and `subrequests`. `compactorWorkerLayer` takes an optional `subrequestLimit`, and
  the Stack's resolves from the configuration at plan time, like the CPU limits (D61).
- **The API Worker keeps the default.** A flush of 256 MiB makes about 40 subrequests (32 parts and
  its commits). A scan is bounded by D93's pages. Neither is within 100× of 10,000.

**Tests.**

- `manifest-size.test.ts`: the budget.
- `manifest-limits.test.ts`: the checkpoint cap at its edge, repeats, and holds; the changelog
  bound and its `changelogAfterSeq`.
- `merge-limits.test.ts`: the read at and past the bound; compaction unfolded and consistent.
- `db-public-error-limits.test.ts`: both public errors; the request bounds on retention, budget
  group, and checkpoint name.
- `shaledb-stack-config.test.ts`: the subrequest default, an override, and the refusal above
  10 million.
- **Mutations:** 12, each caught (`/tmp/shale-work/agents/audit-limits/mutations.log`). One
  survived at first (the read bound made inclusive) and is caught since a test reads at exactly
  the bound.

**Before the release.** Three of these bounds narrow what `/v1` accepted: changelog retention,
budget-group length, and checkpoint-name length. That is done before v1 is released.
`docs/api-compatibility.md` forbids tightening a limit only within a released `/v1`, so from the
release on these bounds are part of the contract.

## D100. Every request is bounded: the body, every string, bytes, and array field

No request may make the API isolate (128 MB, shared by every database it serves) hold an
unbounded body. v1 is not released, so these bounds go in now.

**The body.** The shared request middleware (`publicHttpRequestMiddlewareLayer`, used by the
database, shard-router, and GC APIs) reads the body itself, at most `MAX_REQUEST_BODY_BYTES`
(16 MiB). The router then reads that copy.

- **A declared length:** a `content-length` over the bound is refused before anything is read.
- **No declared length:** a chunked body is refused once the bound is passed, and the rest is
  never read.
- **Why the reader is bounded too.** The Workers runtime gives the handler every body as a
  `ReadableStream`, whether or not the client declared its length. Cloudflare's edge enforces only
  the zone plan's limit (100 MB on Free and Pro), so the read itself is bounded, not only the
  header.
- **The refusal.** It is `InvalidRequest` (400), naming the 16 MiB.
- **Why 16 MiB.** The largest legitimate body is a write batch at its limits: 10,000 operations
  and 8 MiB of keys and values. That is 11.4 MiB as base64 JSON, measured in
  `db-http-request.test.ts`. A commit carries a batch plus its reads, and the body bound caps the
  sum.

**Fields.** Request schemas have their own bounded types: `RequestKeyBytes`, `RequestValueBytes`,
`RequestKeyRange`, `RequestWriteBatch`, `RequestTransactionCommit`, and `RequestSnapshot`.
`KeyRange` and `WriteBatch` stay unbounded because stored manifests and internal calls decode
with them. `collapseWriteBatch` still checks a batch for callers inside the Worker.

| Field                                                                | Bound          | Why                                                        |
| -------------------------------------------------------------------- | -------------- | ---------------------------------------------------------- |
| keys (`key`, `readKeys[]`, range `start`/`end`, `atKey`)             | 16 KiB         | `MAX_KEY_BYTES`, the engine's key limit                    |
| values and merge operands                                            | 1 MiB          | `MAX_VALUE_BYTES` (a Durable Object SQLite row holds 2 MB) |
| `operations`                                                         | 10,000         | `MAX_BATCH_OPERATIONS`                                     |
| `readKeys`                                                           | 10,000         | as many as a batch's operations                            |
| `readRanges`                                                         | 1,000          | each costs the writer one query in the conflict check      |
| `successors` (seal)                                                  | 16             | a split names 2                                            |
| checkpoint `name`                                                    | 256 characters | a label                                                    |
| SST drop `confirm`                                                   | 64 characters  | repeats a 26-character ULID                                |
| database ids, `budgetGroup`, shard groups, a snapshot's `databaseId` | 63 characters  | the id pattern                                             |
| ULIDs (SST, checkpoint)                                              | 26 characters  | `isULID`                                                   |
| `ttlMillis`, `lifetimeMs`                                            | 100 years      | D88                                                        |
| scan, change-feed `limit`                                            | 1,000          | unchanged                                                  |

**How it is enforced.**

- **The schema walker.** `unboundedRequestFields` (`request-schema-bounds.ts`) walks every
  endpoint's params, query, headers, and payloads. It fails on any string, bytes, or array without
  a length check, and on any record. It runs for `DbHttpApi`, `GcHttpApi`, and `ShardedHttpApi`,
  so a future field cannot be added without a bound.
- **Errors at the HTTP boundary.** A key or value over its limit, or more than 10,000 operations,
  is now `InvalidRequest` naming the field. Before, it was `BatchInvalid`. `BatchInvalid` stays for
  an empty batch, the 8 MiB total, and merge rules.
- **`budgetGroup`** takes the database-id pattern. Groups, like ids, are trusted names inside one
  installation, and the access token is the only tenant boundary (`docs/api-compatibility.md`).

**Responses a caller can make large.**

- **Get:** a value is at most 1 MiB, but merged values are unbounded (D99, audit-limits).
- **Scan and change-feed pages:** bounded by bytes in D93 (audit-do).
- **Compaction status:**
  - `l0SstIds`: about `l0MaxSsts` (16), where writes stall.
  - `sortedRuns`: limited by the size-tiered schedule.
  - `corruptSsts`: at most the SSTs the manifest holds.
  - `droppedSsts`: grows by one per operator drop.
- **Checkpoint list:** unbounded. Each checkpoint lives in the manifest, so the count bounds the
  manifest as well as this answer. The bound belongs where checkpoints are created
  (`database-lifecycle.ts`); reported as `CROSS:` with a proposed 1,000 per database, counting
  pinned snapshots and forks' holds.
- **Shard map:** one shard per split.
- **GC sweep report:** at most 50 databases per page.

**Tests.**

- **`db-http-request.test.ts`:**
  - a declared length over 16 MiB, refused with at most one chunk read;
  - a chunked body refused after about 17 of its 64 MiB;
  - the largest batch fits.
- **`request-schema-bounds.test.ts`:** the walker finds each kind of unbounded field, and the
  database and GC APIs have none.
- **`shard-router-worker.test.ts`:** the sharded API has none.
- **`db-http-boundary.property.test.ts`:** 180 random write, scan, and commit bodies around every
  bound. Each is accepted by its schema and runs (writes: no defect) or is refused with
  `InvalidRequest` naming the body, never a 500.
- **`block-server.test.ts`:** traversal and malformed ids and ranges are refused (400), so no
  key outside `db/<id>/` is formed. The `BlockServer` is `workersDev: false`
  (`block-server-worker.ts`); the API Worker reaches it by service binding only.
- **Mutations caught:** `readKeys` unbounded, the checkpoint name unbounded, `budgetGroup` any
  string, a sharded value unbounded, no declared-length refusal, and an unbounded chunked read.

**Corrected at integration: the body is bounded where it is read, not buffered first.** The first
form read the body in the middleware and handed the router a copy. It passed its unit tests and
failed every `POST` in workerd (`writer.workerd.ts`): the router read the original request, whose
body was already used. The middleware now refuses a declared `content-length` over the bound and
provides Effect's `HttpIncomingMessage.MaxBodySize`, so the router's one read of the body stops
at 16 MiB and fails with `RequestParseError` (`BodyTooLarge`), which the middleware answers as
`InvalidRequest`. Nothing reads a body twice. The agents' harness could not show this: only the
workerd tests run the Workers runtime's request, which is why they are part of `vp run ready`.

## D101. A request that waits out an abandoned split finishes it

A split has four steps: record it, seal the shard, fork both halves, publish
(`shard-split.ts`). Killed at the seal, the left fork, or the right fork
(`shard-split-faults.test.ts`):

- **(a) Retried as is:** it resumes with the same two databases. Every key written before and
  after it reads back through a fresh router.
- **(b) Another split of the group:** refused with `ShardSplitInProgress`, naming the pending one.
- **(c) Never retried, dead at the seal:** the shard is still open and writable; only further
  splits are refused.
- **(c) Never retried, dead after the seal:** every write and `Latest` read to its range waits the
  router's 30 s and fails as retryable. Before this change that lasted forever, and nobody was
  told.

**The rule.** When a routed request of the sharded API (put, delete, get, scan) fails after that
wait, the handler finishes the group's pending split from the directory's record alone
(`ShardSplit.resumePending`, `withAbandonedSplitFinished`), then runs the request once more.

- **Why the bound.** A split normally publishes within seconds (13 s at most on `preview-1`,
  D48), so one still pending after 30 s was abandoned.
- **Why it is safe.** Finishing it from two places at once is safe: two identical concurrent
  splits publish one map with the same two databases (tested). Two different concurrent splits
  of one shard publish one and refuse the other, and no write is lost (tested).
- **What an operator sees.**
  - The `ShardSplit.resumePending` span carries `shard.resumed_split`, and `measure health` reports
    it as an issue.
  - `ShardRouter.refresh` spans carry `shard.pending_split` while a split is pending.
  - `docs/operations.md`, "Shard splits", gives the one-command way to finish a split by hand: the
    same `POST …/splits`.
  - A split cannot be abandoned after its seal.
- **Not done.** The pending split records no start time, so its age is not shown. Adding it
  needs `shard-directory.ts` (audit-do) to stamp `beginSplit`; reported as `CROSS:`.

**Also tested.** A router whose map is two generations old reads and writes every key.

**Mutations caught:** the waiting request not resuming, `resumePending` ignoring the record, and
a second split allowed while one is pending.

## D102. One alarm retry rule for the writer and the compactor; the writer's view and its alarm are set by one party at a time

**The retry rule** (`DurableObjectAlarm.retryAfterFailure`, `clearFailures`):

- **Delay.** A failed alarm run sets the alarm again after 1 s (`ALARM_RETRY_BASE_MILLIS`). Each
  further consecutive failure doubles the wait, up to a ceiling.
- **The count** is stored with the alarm, so it outlives an eviction, and a run that succeeds
  clears it.
- **Never thrown.** Both handlers catch every cause, defects included. Cloudflare retries a
  handler that throws up to 6 times from 2 s, then stops for good
  (developers.cloudflare.com/durable-objects/api/alarms, 2026-11-18). Never throwing means those
  retries never stack on this rule, and an outage longer than six retries cannot silence an
  alarm.

| Handler   | Failure                                                            | Before                                             | Now                                                                                                     |
| --------- | ------------------------------------------------------------------ | -------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Writer    | `writer_meta` unreadable at the start                              | no alarm: unflushed rows waited for the next write | retried, backing off                                                                                    |
| Writer    | R2 unavailable (start or flush), stored manifest invalid, a defect | every 1 s, forever                                 | from 1 s, doubling to `maxL0Age` (60 s, the longest a row waits for its flush)                          |
| Writer    | fenced                                                             | stops                                              | stops: a newer epoch serves the database                                                                |
| Writer    | database removed (D85)                                             | stops                                              | stops                                                                                                   |
| Compactor | manifest unreadable in a poll                                      | every `compactorPollInterval` (5 s), forever       | from 1 s, doubling to min(`gcInterval`, `compactionLeaseMs` / 2) = 300 s, so leases are renewed in time |
| Compactor | fence-in unreachable                                               | every 5 s                                          | the same backoff                                                                                        |
| Compactor | GC storage unavailable                                             | after `gcInterval`                                 | unchanged                                                                                               |
| Compactor | a defect                                                           | thrown: the runtime's 6 retries, then nothing      | caught, with the backoff                                                                                |

**A writer that cannot flush still shows.** Its flush spans end in error at least once per
`maxL0Age`. `measure health` now reports failed `DbWriter.flush` spans per database as an issue
(`failedFlushes`); before, they showed only in the top error spans.

**The stalled-write reload.** A write that finds L0 at its limit reloads the manifest. It used to
set the writer's view to that manifest unconditionally. A flush that committed while the reload
loaded was overwritten by the older manifest (status `l0Count` 2 where L0 held 3), and so was a
fence-out.

- **What it cost.** No write was lost: the next flush's `updateFrom` conflicts and reloads. The
  cost was one extra round trip and a stale `l0Count`.
- **The fix.** The reload is installed only over a `Ready` view holding an older manifest.

**One flush scheduler at a time.** Two writes' schedulers could both read the alarm before either
set it, and the one holding older counters, with the later time, could set it last.

- **What it cost.** A row still waited at most `maxL0Age`: a scheduler's counters are never newer
  than the oldest unflushed row. But a flush due now, by size, waited up to 60 s more.
- **The fix.** `scheduleFlush` runs under its own lock, the innermost one, with only the counters
  and the alarm inside.

**Tests.**

| File                    | Test                                                              | On the code before            |
| ----------------------- | ----------------------------------------------------------------- | ----------------------------- |
| `db-writer.test.ts`     | a failing flush backs off 1, 2, 4, … 60 s and resets on success   | 1 s every time                |
| `db-writer.test.ts`     | an unreadable `writer_meta` and a defect both set the alarm again | no alarm                      |
| `compactor.test.ts`     | a failing poll backs off 1 … 300 s and resets                     | 5 s every time                |
| `db-writer.test.ts`     | the reload's older manifest is not installed over the flush's     | `l0Count` 2                   |
| `db-writer.test.ts`     | the gated two-scheduler race                                      | alarm 60 s, where now was due |
| `health-report.test.ts` | failed flushes are an issue                                       | —                             |

**Mutations:** 8 of 9 caught. The missed one removes the writer handler's outer `catchCause`,
which nothing reaches: every effect in the handler is already inside an `Effect.exit`.

## D103. A compaction's commit records its job; a missing Workflow instance is finished; what a large compaction needs

**A retried commit of a job with no outputs.**

- **The defect.** `commitCompaction` recognized its own retried commit only by its outputs, which
  needed at least one. Since D97 a job can drop every row: deletes at the bottom, or values that
  expired together. When such a job's commit step ran again after its result was lost, the inputs
  were already gone. It failed with `CompactionInputsChangedError`, and the job reported
  `Discarded`.
- **What the compactor did with that.** No harm: `poll` releases the job's lease and plans from the
  newest manifest, where those sources are gone, so nothing is replanned. Only the outcome was
  wrong.
- **The rule.** A commit records its job id in the manifest: `recentCompactionJobIds`, the newest 16. A commit whose id is recorded is already applied and changes nothing, whatever its outputs.
  - The executor's `commit` takes the job id, which is the Workflow instance id.
  - The field is optional. A manifest written before D103 decodes with none.
  - Older code that rewrites a manifest drops the field. A retry then reports `Discarded`, as
    before, and loses nothing. So the format stays 1 (D64's rule for additive fields).
  - 16 ids cover far more than a commit step's retries: compactions of one database run one at a
    time.
- **Tests.**
  - `garbage-collector.test.ts`: an all-dropped job whose commit step runs twice ends `Committed`,
    once, with its sources gone and its id recorded once.
  - `manifest.test.ts`: the record survives a later flush and caps at 16; another job over the same
    inputs is still refused; a manifest without the key decodes.
  - Four mutations caught.
  - The `verify-manifest-cas-fencing` skill records why no model step changed: the model has no
    retried commit, and the already-applied path writes nothing.
- **`CROSS`.** `manifest.ts` is `audit-lifecycle`'s file; this function was assigned here.

**A Workflow instance that does not exist** (left open by D94). `CompactionLauncher.progress` now
reports a missing instance as finished with no outcome, at once rather than after D94's bound.
Workflows' `get` rejects such an instance with `instance.not_found`
(`isWorkflowInstanceNotFound`). Any other failure still reads as running.

- **Statuses.** Only `queued`, `running`, `paused`, `waiting`, and `waitingForPause` count as
  running. `unknown`, and any status this code does not know, finish the job. If such an instance
  commits after all, the exact-inputs check refuses it once another job took its sources.
- **The risk this takes.** Workflows has answered `instance.not_found` for an instance that existed
  (a report on Cloudflare's community forum, 2025-09-16). The compactor reads progress at least
  one poll interval (5 s) after it created the instance. A false not-found costs a duplicate job:
  one of the two commits is refused, and the other's outputs become an orphan.
- **Not unit-testable.** The launcher's own call of the check needs the Workflows binding.
  `compaction-workflow.test.ts` tests the check (two mutations caught) and the status mapping.

**What a large compaction needs.** A job of 8 inputs of N GiB each, 8N GiB in all, with the default
settings (64 MiB outputs, 2 MiB input reads, 8 MiB parts, 16 KiB blocks), since D86, D96, and D97.
Kind: **measured** on a preview, **derived** from the code and the measured figures, or **unknown**.

| Quantity                | Value                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Limit                                                                         | Largest N with 2× headroom                                                             | Kind                                           |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | ---------------------------------------------- |
| Steps per job           | outputs + 2: up to 128N + 2 if little is dropped (one per 64 MiB written), at least 85N if everything is (one per 96 MiB read, D97)                                                                                                                                                                                                                                                                                                                                                                        | 10,000 per instance (configurable to 25,000)                                  | **N ≈ 39 (a 312 GiB merge)**; 10,000 is reached at a 625 GiB merge                     | derived                                        |
| Subrequests per step    | about 48 chunk reads (≤ 96 MiB), ≤ 8 metadata reads, 10 part calls, 4 manifest reads (D86) ≈ 70                                                                                                                                                                                                                                                                                                                                                                                                            | —                                                                             | —                                                                                      | derived                                        |
| Subrequests per job     | about 860 per GiB of input (agrees with `audit-limits`' D99 figure) ≈ 6,900 N                                                                                                                                                                                                                                                                                                                                                                                                                              | 10 million (set by `audit-limits`, D99)                                       | N ≈ 727                                                                                | derived                                        |
| `prepare` step result   | about 40 bytes per input SST (D86)                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | 1 MiB                                                                         | about 13,000 input SSTs: N ≈ 100                                                       | derived                                        |
| An output step's result | one handle, about 300 bytes (two 16 KiB keys at most: 44 KB)                                                                                                                                                                                                                                                                                                                                                                                                                                               | 1 MiB                                                                         | any N                                                                                  | derived                                        |
| CPU per step            | 9.0 to 17.7 s for a 64 MiB step (D61, D73); at most 26.6 s for a 96 MiB-input step at the worst rate                                                                                                                                                                                                                                                                                                                                                                                                       | 60 s (compactor)                                                              | any N: steps are bounded, not jobs                                                     | measured (64 MiB), derived (96 MiB)            |
| Wall time per step      | about 25 s: four steps finished 112 s after the last flush, less a 5 to 9 s wake (D61)                                                                                                                                                                                                                                                                                                                                                                                                                     | none per step                                                                 | —                                                                                      | derived from measured                          |
| Wall time per job       | about 25 s × 128N ≈ 53 N minutes: an 8 GiB job runs about an hour                                                                                                                                                                                                                                                                                                                                                                                                                                          | none; D96 keeps a job past `gcMinAge`, D94 only ends one no read sees running | —                                                                                      | derived                                        |
| Peak memory per step    | inputs: 2 chunks of ≤ 2 MiB raw per input plus their decoded rows, 8 to 10 MiB per input, 64 to 80 MiB for 8. Output: the 8 MiB part buffer and the chunk it is cut from, 8 to 16 MiB. Index: 4,096 blocks × 2 keys, 0.4 MiB with 16-byte keys, 8 MiB with 1 KiB keys. Bloom hashes: 8 bytes per key, 0.5 MiB for 1 KB rows, 5 MiB for 100-byte rows. The newest manifest (D86), 4 to 10 × its size, up to 40 MiB at D99's 4 MiB budget. About 83 MiB in the common case, up to about 130 MiB in the worst | 128 MB per isolate                                                            | does not grow with N. **2× headroom is not met for 8 inputs**, even in the common case | derived; the decoded-row factor is **unknown** |
| Workflow state per job  | about 300 bytes per output step plus `prepare`                                                                                                                                                                                                                                                                                                                                                                                                                                                             | 1 GB                                                                          | any N                                                                                  | derived                                        |

- **Measured evidence.** The largest compaction ever run live is about 0.5 GiB. The documented runs
  are four 60 MiB L0 SSTs (240 MiB, four steps, every invocation `ok`, `docs/measurements.md`).
  Everything above that size in the table is derived.
- **Database size.** A size-tiered bottom merge can take most of a database, so the step limit is
  the first one reached. A database above about 312 GiB can plan a merge without 2× headroom, and
  above about 625 GiB one that exceeds 10,000 steps and fails at its last step on every attempt.
  Raising the Workflow's step limit to 25,000 moves this to about 1.5 TiB, past D99's 1 TiB
  manifest bound for 16-byte keys. Passed to `audit-limits` (`CROSS:`).
- **Memory** is the figure to measure first: a heap snapshot of one output step of an 8-input L0
  job on a preview.

## D104. What the first production deploy creates, its tokens, and its smoke test

**Read, never run.** Nothing that names `production` was executed in this audit.

**The Stack at stage `production`, with no environment set** (`alchemy.run.ts`;
`shaledb-stack-config.test.ts` pins the configuration):

- **The LSM bucket.** Named `shaledb`, retained (`RemovalPolicy.retain`), not force-destroyed, with
  no jurisdiction and location `auto`. A preview's is generated and force-destroyed.
- **The trace bucket.** Its name is generated; `forceDestroy` is false only in production; traces
  and tail records expire after 14 days.
- **Workers:**
  - API, with `WriterServer`;
  - `BlockServer`, no `workers.dev`;
  - compactor, with `CompactorServer`, `CompactionBudgetServer`, and `CompactionWorkflow`;
    60 s CPU and 10 million subrequests;
  - GC, with its cron;
  - shard router, with `ShardDirectoryServer`;
  - trace collector;
  - CPU tail Worker, no `workers.dev`.
- **No probe Worker,** so no `TelemetryProbeServer`, `R2ProbeServer`, or `PlainSqliteServer`
  (`deploysTelemetryProbe`, tested).
- **Sampling:** 10% of traces, against every trace on previews.
- **Access:** production has no state-held access token. Its two Secrets Store slots are
  `SHALEDB_PRODUCTION_ACCESS_TOKEN_A` and `_B`.
- **What a later deploy would replace or delete.**
  - A changed jurisdiction replaces the buckets and the Workflow and addresses new Durable
    Objects (D49).
  - A changed bucket location does nothing to the existing bucket (R2 cannot move one).
  - A Durable Object class that is removed or renamed is a `deleted_classes` migration, which
    destroys its storage. The classes are pinned by `production-durable-objects.test.ts`, and the
    rule is in `docs/operations.md`, "Durable Object classes".
- **Not pinned by a test:** physical names other than the bucket's, which Alchemy generates from
  the stage.

**Tokens** (`shaledb-access-tokens.ts`, `access-token-store.ts`, `lsm-access-gate.ts`):

- **At deploy time.** The Workers bind both slots by reference: Alchemy never writes their values.
- **At request time.** The gate reads them on the first request and every 30 s after. A rotation
  needs no deploy, and Workers accept the new token within 30 s.
- **A missing secret.** `deploy:production` runs `tokens check production` first and stops when
  either secret is missing. The decision (`missingAccessTokenSlots`, exact names only) and the
  task's order are now tested.
- **Rotation without an outage:**
  1. move every caller to B;
  2. `tokens rotate production A`;
  3. wait 30 s before giving anyone the new A.
- **Never printed.** No test or log prints a token.
- **Not changed, risk.** When a written secret does not turn active within about 10 s
  (`awaitActive`), `ensure` reports success anyway.

**The smoke test** (`smoke.ts`, `smoke.test.ts`):

- **Interruption.** A run interrupted mid-way (a killed process, Ctrl-C) left its databases behind.
  The cleanup now also runs on interruption (`Effect.onInterrupt`).
- **A lost answer.** A create whose answer was lost was not recorded for removal; the id is now
  recorded first.
- **Every removal.** Every database is removed, and every leftover is reported, where the first
  failed check used to stop the loop.
- **Unique ids.** Ids carry a random part as well as the time.
- **A hung step.** Each step fails after 60 s.
- **What a run leaves in production.** After a passing run, its two databases are gone from the
  API. Their head manifests stay for `gcMinAge` (1 h), and their ids cannot be created again for as
  long (D90). The maintenance sweep then removes the rest.
- **Exit status.** It exits non-zero on any failure.
- **Safe on a live installation.** It touches only its own databases.
- **Mutations caught:** cleanup without interruption, and the leftover check.

**The deploy guard** (`docs/release-v1.md` §10, corrected). The guard was read at
`~/.dotfiles/home/.pi/agent/extensions/cloudflare-deployment-guard/`.

- **What it exempts.** In an allowlisted repository, and every worktree of it, the guard blocks
  only a destroy or delete named directly on the command line.
- **What it does not resolve.** It does not resolve package tasks there. So
  `vp run @shaledb/infra#destroy:preview-<n>` and `deploy:production` are not blocked for agents.
  The only gates are instructions and the token check.

## D105. Rollback against the stored formats: what older code does with newer data

Production starts at the release commit, so a rollback can only go back to a commit at or after
it. Every stored record decodes with unknown keys ignored, and the next write of it drops them.
The rule (`docs/operations.md`, "Rolling back") is that a field whose loss changes behavior raises
the record's format, which older code refuses (D64).

The two older commits that matter:

- **`f3b2ad2`:** the last soaked commit.
- **`c3807f7`:** what the previews run.

Their schemas differ from today's only in the fields below (`git diff <commit> -- manifest.ts
compactor.ts shard-map.ts`). The tests rebuild them from today's schemas without those fields and
run them against today's fixtures (`manifest-format-fixtures.test.ts`,
`stored-format-fixtures.test.ts`).

| Record          | Field (decision)                             | Missing at | Older code reads it and rewrites it                                                                                                                                                                                                                        | Today's code reads what it wrote                                                                                                                               | Test                                                     |
| --------------- | -------------------------------------------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| manifest        | `changelogAfterSeq` (D80)                    | `f3b2ad2`  | decodes; its next commit drops it                                                                                                                                                                                                                          | `changelogAfterSeqOf` reads the safe side: `Expired` more often, never a gap                                                                                   | fixtures test (f3b2ad2 schema); `manifest.test.ts` (D80) |
| manifest        | `recentCompactionJobIds` (D103)              | both       | decodes; its next commit drops it                                                                                                                                                                                                                          | `[]`: a compaction commit retried after the rollback reports `Discarded`, as before D103, and loses nothing                                                    | fixtures test (both schemas)                             |
| manifest        | `Removed.removedAt`, `Removed.cleared` (D91) | both       | decodes `{ "_tag": "Removed" }`. Older `markRemoved` returns a removed manifest unchanged, so it is not rewritten by a remove. Older edits that D91 made no-ops on a removed head (compaction commit, seal, GC preparation) could rewrite it without them. | no `removedAt` reads as removed when read: the tombstone stays `gcMinAge` longer. No `cleared` reads as false: the sweep clears it again, which is idempotent. | fixtures test (both schemas, removed fixture)            |
| compactor state | `lastProgressAt` (D94)                       | both       | decodes; its next write drops it                                                                                                                                                                                                                           | `None`: the job is judged from `startedAt`, as state written before D94 is                                                                                     | `stored-format-fixtures.test.ts`                         |
| shard map       | none since either commit                     | n/a        | n/a                                                                                                                                                                                                                                                        | n/a                                                                                                                                                            | `shard-map-format.test.ts` (round trip)                  |
| writer SQLite   | no schema change since D58                   | n/a        | n/a                                                                                                                                                                                                                                                        | n/a                                                                                                                                                            | —                                                        |

**What older code does with a tombstone** (read, not tested: the older code itself cannot run here).

- **At `c3807f7` and `f3b2ad2`, the maintenance sweep skips a `Removed` head**
  (`maintenance-sweep.ts`, `state._tag === "Removed"` → `Nothing`). So a tombstone is not finished
  while rolled back.
- **A remove retried there deletes every object under `db/<id>/`, the tombstone too**
  (`database-lifecycle.ts`, the delete loop after `markRemoved`). That is the behavior D91 fixed:
  while rolled back, a commit from a view loaded before the removal can bring that database back.
- **Readers there do not answer `DatabaseNotFound` for a `Removed` head** (`ManifestReader.loadLive`
  came with D91). A read of a removed database then fails on its deleted SSTs instead.

None of this loses an acknowledged write of a live database. The roll-forward finishes every
tombstone.

**`PendingSplit.startedAt`** (D101) is not a stored field at `e2dc1d7`. D101 records why: the
directory's `beginSplit` (audit-do) does not stamp it. The shard map is unchanged.

**Fixtures.**

- `manifest-v1-release-shape.json` (sealed), `manifest-v1-release-removed.json` (removed, with
  `removedAt` and `cleared`), `compactor-state-v1-release-shape.json`, and
  `shard-map-v1-release-shape.json` hold every stored field set. Their tests require that decoding and
  re-encoding gives the same JSON, so the next change to a record fails until someone decides what
  older code does with it.
- A further test shows today's code dropping an unknown field, which is why the format rule
  exists.

## D106. The soak exercises the classes of defect the v1 audits found

**Why.** A 132-minute soak, the 13 scenarios, and both gates passed on code that still held every
defect from D76 to D103. The soak's workload never did what those defects needed: follow the change
feed, write merge operands with TTLs, continue a scan page by page, flush in bursts, leave a
database idle, fork from a checkpoint, recreate a removed id, or abort a request.

**What it adds.** `docs/testing.md` ("The soak's workload") lists each workload, the defect class
it exercises, and its `checks` keys. The models live in `tools/scenario-cli/src/soak-checks.ts`:

- **`ChangeLedger` and `FeedFollower`.**
  - The ledger records every batch the soak sent to a database: acknowledged by sequence number,
    or uncertain (a failed attempt that may have committed once).
  - Each feed page must hold sequence numbers above the cursor, in order, with `nextAfterSeq` at
    its last row.
  - Every acknowledged batch at or below the cursor must have arrived whole. A sequence number is
    assigned inside its commit, so this holds whenever the check runs.
  - Every batch that arrived must be acknowledged, or match one uncertain attempt. This is judged
    at the end.
- **`MergeKeyModel`.**
  - Each part of a `Concat` value (the base and each operand) expires on its own, from its write
    receipt's `createTs` plus its TTL.
  - A read accepts the value either side of an expiry within `CLOCK_SKEW_MS` (5 s) of its
    interval.
  - A key whose merge outcome is unknown is not judged until its next base.
- **`ScanTraversal`.** Across one traversal of pages, keys must strictly increase, and a key that
  held a value throughout must be returned.
- **Aborted writes** go to keys no other write touches, written once each. Such a key holds its
  value or nothing.

**Tolerances, and why each is sound.**

- **Uncertain batches.** A batch the soak never learned the outcome of may appear once at an
  unknown sequence number, and only as exactly its changes.
- **Merge reads.** The 5 s window covers the skew between the laptop's clock and Cloudflare's,
  which keep NTP time. Within it, either answer is correct under D83.
- **Merge keys sent twice.** A key whose merge may have been applied twice is skipped until its
  next base.
- **Paged scans.** A key written, deleted, or in flight during the traversal is not required.

**Checks that wait on unmerged fixes,** marked as such in their check names:

- deleting a fork's hold is refused (audit-lifecycle's hold fix);
- recreating a removed id is refused (D90).

On a build without them, these checks fail. That is the point.

**Reach.** D76 needs the compaction's replaced manifest collected (`gcMinAge`, one hour) while the
writer stays up. The idle database's reads every 20 s keep its writer in memory, so a soak of more
than about 65 minutes reaches it (`idle.transaction_after_gc_age`). A shorter run checks only that
idle reads and transactions are exact.

**The scenarios** (`repair`, `fork-split`, `changelog`, `remove`) were read against D80, D90, D95,
and D99. None recreates an id, deletes a hold, reads a feed below a fork point, or reseals with
other successors, so none changed.

## D107. An SST's metadata stays a small fraction of its data, whatever its key size

**Found by the read audit.**

- **The cause.** An index entry records each data block's first and last key. A block closed once
  it reached `blockSize` (16 KiB), so with long keys each block held one or two rows and the
  index grew with the keys.
- **Measured** on 4 MiB SSTs with 16 KiB blocks and 16-byte values, metadata as a share of data:

  | Key size | 16 B | 1 KiB | 4 KiB | 16 KiB (`MAX_KEY_BYTES`) |
  | -------- | ---: | ----: | ----: | -----------------------: |
  | Before   | 3.2% | 12.5% |   50% |                     200% |
  | After    | 3.2% |  3.3% |  3.4% |        3.4% (about 3.3%) |

- **At a 64 MiB SST** that was 32 MiB of metadata at 4 KiB keys and 128 MiB at 16 KiB.
  - Every reader reads the metadata whole, and the L1 metadata cache never holds an entry larger
    than its group share (8 MiB of 16 MiB). So every get re-read it, and gets fetch the metadata
    of all their candidate SSTs at once (D67).
  - At 16 KiB keys, the writer's flush builds that index in memory, past the 128 MB isolate: the
    flush fails on every attempt and writes then stall.

**The rule.** A data block closes once it reaches `blockSize`, and also not before it holds
`BLOCK_BYTES_PER_INDEX_KEY_BYTE` (32) times the bytes of its index entry's two keys. The index is
then at most about 1/32 of the data.

- **Keys up to 256 bytes** change nothing at the default 16 KiB block.
- **16 KiB keys** make 1 MiB blocks of about 64 rows. A point read of such a key fetches one 1 MiB
  block, which the L1 data cache charges as 1 MiB.
- **Older SSTs stay readable.** Readers follow each SST's own index, so no format changes and
  nothing is rewritten.

**Tests.**

- `sst-codec.test.ts`: metadata is at most 1/16 of the data for keys of 16 bytes to 16 KiB, and the
  last key is still found. It fails without the rule, at 200%.
- `db-client.test.ts`'s short-scan test (D72) asserted exactly two block reads over 64-byte test
  blocks. Those blocks now hold about ten rows, so it asserts one or two. The D72 mutation (a
  merge that fills its batch before emitting) still fails it.

**Revises D103's memory row.** The index of a 64 MiB output is now at most about 2 MiB, at any key
size.

**Also found: a damaged SST trailer threw instead of failing typed.**

- **The defect.** The trailer (footer length, metadata offset, format version) has no checksum of
  its own. `decodeSstMeta` read it with unguarded `ByteReader` calls. A damaged metadata offset (above
  2^53 - 1), or metadata cut short by a short response or cache entry, threw a plain error: the read
  died as a defect (`InternalFailure`), naming no SST, where every other section fails with
  `SstCorruptedError`.
- **The fix.** The trailer is read inside `Result.try` and fails as `SstCorruptedError` (`Trailer`,
  `Malformed`). Any other damage to it is then caught by the footer's checksum.
- **Tests.** New property tests in `sst-codec.test.ts`, 400 runs each, cover any one byte changed
  and a cut at any length, for both the metadata section and a data block. Each decoder must fail
  typed, never throw and never answer. Neither decoder had such a property before, only one fixed
  damage each. The metadata property fails without the fix: shrunk, a one-row SST whose metadata is
  cut to 8% of its length.

## D108. A scan page examines a bounded amount and ends early, resumably

**Found by the read audit.**

- **The defect.** A scan ended only when it had `limit` visible rows or the range ended. Over a
  range of deletes or expired values, the same shape as D97 on the write side, one page read and
  resolved every version in the range to return a few rows or none.
- **The cost.** Reading, decoding, and resolving versions costs up to about 0.28 s of CPU per MiB
  (D61's compaction rate, an upper bound for reads). A range holding more than about 100 MiB of
  deleted versions then exceeded the API Worker's 30 s on every request: a timeout, not a
  resumable page.
- **The block reads.** At 16 KiB blocks each MiB is 64 block reads, 128 hedged, against an
  invocation's 10,000 subrequests.

**The rule.**

- **The budget.** A scan with a limit (every public scan page, and any in-Worker scan given a
  limit) examines at most `SCAN_PAGE_EXAMINED_BYTES` (16 MiB) of versions, counted with
  `rowEncodedSizeEstimate` whether returned or not. That is at most about 4.5 s and 2,048 hedged
  block reads.
- **Sharing.** The budget (`ScanBudget`) is shared by the segments a `Latest` page is made of
  (D93).
- **Where it ends.** Once spent, the scan ends before the next key with `ReaderUnavailableError`
  carrying `resumeFrom`, that key. Every row before it was returned, and none at or after it, as
  for D93's early end.
- **Progress.** Each underlying scan examines at least one key, so a resumed page always moves on.
- **The public page.** It turns that into a page with the rows it has, possibly none, and
  `resumeFrom`. Before, it required at least one row, which D93 guaranteed; that guard is gone.
- **The contract.** The OpenAPI description says a page may end early, with fewer rows or none, and
  that a client follows `resumeFrom` until it is null. That was already the rule for continuing.
  The contract is regenerated.
- **Unlimited scans stay unbounded.** An in-Worker stream with no limit reads its whole range, as
  before.

**Also: a damaged cached copy no longer fails a block for a year.**

- **The defect.** Responses of the `BlockServer` are cached immutable for a year. A copy damaged in
  Workers Cache, or in transit, failed its checksum, and the read failed with `SstCorruptedError`
  even though the bucket held intact bytes. The L1 never kept it, but Workers Cache did, so every
  read of that block failed until the copy was evicted.
- **The fix.** `BlockCache` reads a range whose bytes fail their checksum once more past every
  cache (`BlockSource.uncached`). Where the Stack has Workers Cache, that reads the bucket. In a
  restricted Stack it goes through the `BlockServer` again, which reads the bucket on every
  request there.
  - An intact copy answers, with `cache.damaged_copy` on the span and a warning that names the SST
    and the offset.
  - A damaged bucket copy still fails, typed.
- **Tests.**
  - `block-cache.test.ts`: a damaged cached copy with an intact bucket copy answers; damage in both
    fails as `SstCorruptedError` naming the SST.
  - `block-source.test.ts`: the hedged source's `uncached` reads the bucket at once.
  - Two mutations caught.

**Also: a read fetches at most eight SSTs' metadata at once.**

- **Before.** D67 fetched every candidate SST's metadata at once: a get reads one SST per level (up
  to 16 L0 SSTs and as many runs), and a scan reads every overlapping SST. Metadata is up to 1/32
  of an SST (D107), 2 MiB for a 64 MiB SST, so a cold read could hold 64 MiB of bodies and their
  decoded form at once.
- **Now.** `METADATA_READS_AT_ONCE` (8). An invocation has only six connections waiting for headers
  at a time, so this costs no latency.
- **Test** (`db-reader-limits.test.ts`): over 20 L0 SSTs holding one key, a get and a scan keep at
  most 8 metadata reads in flight; with the fetches unbounded, the get keeps 20.

**Not covered.** The transaction scan (`DbClient.scanAt`, `transactions/scan`) passes no budget.
Its pages are bounded by `limit` rows, not by what they examine, and its `resumeFrom`-less contract
needs a decision of its own (`CROSS:` to the API owner).

**Test** (`db-http-scan.test.ts`): 24,000 keys of 1 KiB values, all but the last deleted. The
first page answers 200 with no rows and a `resumeFrom`, and following `resumeFrom` returns exactly
the live key, with every page answered. Three mutations caught: no budget, the handler requiring a
row to end early, and versions not counted.

## D109. The writer Durable Object is model-checked at await granularity

**Why.** The writer models before this one (`NoAckedWriteLost`, `Transactions`, `SealSplit`) make
each code step atomic. Five of the defects found on 2026-11-26 lived between two awaits of one
Durable Object: D74b, D84, D85, D102, and D74's alarm loop. No atomic model can show them.

**The model.** `WriterObject.tla` is written in PlusCal. The TLA+ translation sits in the same
file, so `verify.ts` runs it like the others.

- **Processes.** Two requests (write, transaction commit, flush, seal, probe) and the alarm.
- **Labels.** One per `yield*` that leaves the object (R2), one per lock, and one between two
  SQLite statements that are not in one `MemTable.consistently`.
- **Eviction** loses the phase, the counters, the locks, and every request. SQLite and the alarm
  stay.
- **Interruption** can come at any label outside the commit's uninterruptible region. It runs the
  finalizers.
- **R2** may fail before a manifest commit lands, or after it lands with the response lost.
- **Time** is abstract: the alarm is set whenever the counters show an unflushed row.

**Each known defect is a `Broken` value that fails a named property.** `noGeneration` and
`interruptedWrite` (D74b) fail `CountersCover`. `noHorizonReread` (D84) fails `Serializable`.
`sealKeepsPhase` (D85) fails `SealNotHalf`. `noAlarmRetry` (D102) fails `AlarmCovers` and
`AckedFlushed`. `noSettle` (D74's loop) fails `AlarmQuiets`. The code as written passes four
configs, with bounds in `.agents/skills/verify-writer-object`.

**What it found.**

- **D115**, a defect: a flush refused at an older epoch marked the restarted writer `Fenced`.
- **A gap, a risk rather than a defect.** A write evicted after its SQLite commit and before
  `scheduleFlush` leaves an unacknowledged row with no alarm until the next request to the
  object. The row is durable and `Latest` reads see it (a probe starts the writer, which sets the
  alarm). A database that gets no further writer request never flushes it. Whether the platform
  can evict between the two depends on the output gate: `scheduleFlush` awaits `getAlarm` first.
  `AlarmCovers` excepts these rows. A fix would set the alarm in the same storage turn as the
  commit; it is left to the writer's owner.

## D110. The compactor Durable Object is modeled at await granularity

`CompactorObject.tla` models one database's compactor:

- two concurrent wakes through `ensureState`;
- the alarm's poll, with its awaits and its `updateState` write;
- Workflow jobs whose commit step may run twice;
- the compaction budget's lease (one slot);
- GC with D96's running-job cap;
- the writer's flushes and its stalled-write wake;
- eviction, which loses the processes and locks but keeps the cell, the alarm, and the Workflow
  instances.

**Properties.**

- `EpochNeverRegresses`, an action property;
- `OutcomeTrue`;
- `LeaseCapacity`;
- `L0Drains` (liveness): L0 at its limit is eventually compacted;
- `RunningJobsLeave` (liveness).

**Each known defect is a variant that fails:**

| Variant                         | Fails                 |
| ------------------------------- | --------------------- |
| D78's missing lock              | `EpochNeverRegresses` |
| the code before D78             | `L0Drains`            |
| D103's unrecognized retry       | `OutcomeTrue`         |
| D96's GC ignoring a running job | `L0Drains`            |
| D94's unbounded unreadable job  | `RunningJobsLeave`    |

D78's stale write and terminal fence are unreachable once `startLock` is held, because only this
object fences in. The model shows them only together with the missing lock.

**Assumptions the code relies on.** Building the model surfaced three, each first a lasso that
was a modeling error, not a defect:

- the runtime retries an alarm handler that an eviction cut off;
- a lease expires on time whatever the polls do;
- `gcMinAge` is far longer than the gap between a job's launch and its record.

No new defect was found. `verify.ts` now reads a violated temporal property's name from a config
that has exactly one, because TLC does not print it.

## D111. Liveness checked for the writer and transaction models

**Why.** Every model checked safety only: nothing bad happens. D76 (transactions refused forever)
and D74's alarm loop were "nothing good ever happens", which no invariant sees.

**What "keeps working" means, and where it is checked.**

| Model                                           | Property                                                               | Fairness                                               | Broken value that fails it                  |
| ----------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------ | ------------------------------------------- |
| `WriterObject` (`WriterObjectLive.cfg`)         | `AckedFlushed`: every acknowledged write reaches the manifest          | each started request step and the alarm, strongly fair | `noAlarmRetry` (D102)                       |
| `WriterObject` (`WriterObjectAlarmLoop.cfg`)    | `AlarmQuiets`: the alarm stops firing                                  | the same                                               | `noSettle` (D74's loop)                     |
| `NoAckedWriteLost` (`NoAckedWriteLostLive.cfg`) | `EventuallyFlushed`: every acknowledged write is readable at `Flushed` | the flush's steps and the restart, weakly fair         | `noWake` (nothing starts an evicted writer) |
| `Transactions` (`TransactionsLive.cfg`)         | `EveryTxnCommits`: a transaction retried after each abort commits      | each transaction's steps, weakly fair                  | `beforeD76`                                 |

`Transactions` gains a `Live` constant. With it, a read decides as `DbReader.manifestFor` does,
GC deletes a flush's own manifest (`Collect`), and an aborted transaction is retried. Writes are
finite, so conflicts with real writes end. The safety config is unchanged.

**Not checked here.** Liveness for `GcSafety`, `SealSplit`, and `ManifestCasFencing` (a fork
eventually releases its source, a removal eventually leaves nothing) belongs to the second formal
agent's `Lifecycle` and `CompactorObject` work (D110, D114).

## D112. A shard split is modeled end to end

`ShardSplit.tla` models a split from start to finish:

- the directory's pending split, the seal with both holds, both forks in either order, and the
  publish;
- a crash after any step, resumed by a request that waited out the routing wait (D101);
- a writer whose router has a stale map;
- a `Flushed` reader whose map ages past D89's bound;
- a rogue seal naming other successors (D95);
- a change-feed consumer following D80's procedure.

**Properties and the variants that fail them:**

| Property                        | Variant     |
| ------------------------------- | ----------- |
| `EveryShardWriteInItsSuccessor` | —           |
| `HoldsMatchSeal`                | before D95  |
| `ConsumerMissesNothing`         | before D80  |
| `NoStaleBeyondBound`            | before D89  |
| `WaitingWritesLand` (liveness)  | before D101 |

The safety run has 24,118 states. An exploration at larger bounds (228,918 states) found nothing.

## D113. A flush is exact about where it starts; the change feed reads each change once

**Found** while placing the labels of the change-feed model. A failing test showed it before any
model was written.

**The defect.**

- **Where the commit lands without an answer.** A flush's manifest create lands, and then
  `attemptCreate` fails reading GC's boundary, or the `putIfAbsent` response is lost. The flush
  fails, so `markFlushed` never runs.
- **The second export.** The next flush read `flushed_seq` from SQLite, still the old one, and
  exported its changelog from it again (`exportChangelog(meta.flushedSeq, cut)`).
  `commitL0Flush` appended a second entry holding the same changes, and a second L0 SST with the
  same rows. The merge removes duplicate rows in L0; the feed does not.
- **What a consumer saw.** `DbReader.changelogRows` read every entry above `afterSeq` in
  `firstSeq` order and kept every row in range. With entries `(1, 1)` and `(1, 2)` a consumer got
  1, 1, 2: a change twice, and the sequence going back. Nothing was skipped. The "same batch"
  check kept the repeat inside one batch.

**The rules.**

- **A flush says where its rows start.** `L0Flush.afterSeq` is the `flushed_seq` the flush read
  its rows above. `commitL0Flush` refuses a flush whose `afterSeq` is below the manifest's
  `lastL0Seq` with `FlushedSeqStaleError`. The check comes after the epoch and sealed checks and
  after the "already applied" check, so a retried commit is still recognized. The error is
  internal: the writer handles it, and no caller sees it.
- **The writer records the manifest's flushed state and flushes again.** It runs
  `markFlushed(lastL0Seq, the refusing manifest's id)`, as `startWriter` does, then flushes again
  from it, still under the flush lock. No row is exported or uploaded twice, and none is
  skipped: the rows up to `lastL0Seq` are in the landed manifest. Nothing else commits a flush
  under the lock, so a second attempt is not stale again; a second refusal fails the flush.
- **The refused attempt's SSTs** (the L0 SST and the changelog SST) are orphans. No manifest
  names them, and GC collects them after `gcMinAge`, like any upload whose commit was refused
  (`GcSafety`'s `Discard`).
- **The feed reads each change once.** `changelogRows` skips rows at or below the highest
  `lastSeq` of the entries it has already read. An entry holds every change in its range, so
  those rows are repeats. This makes manifests written before this decision read correctly; an
  entry entirely covered by an earlier one gives no rows. Entries are ordered by `firstSeq`, then
  `lastSeq`.

**The other committers in `db-writer.ts`, in the same lost-response window.**

| Committer                                   | Lands, response lost | Outcome                                                                                                                                                                      |
| ------------------------------------------- | -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A flush with a seal (`flushThen(sealEdit)`) | sealed and flushed   | `onExit` forgets the phase (D85, D115); the restart sees the seal and flushes nothing. A stale refusal comes before `sealEdit` runs, so the second attempt carries the seal. |
| The seal's own commit                       | sealed               | the same                                                                                                                                                                     |
| `startWriter`'s fence-in                    | epoch raised         | the next start fences again: one more epoch, nothing else                                                                                                                    |
| A transaction's commit, a write             | no manifest commit   | —                                                                                                                                                                            |
| The stalled-write reload                    | an identity edit     | no write                                                                                                                                                                     |

**Tests.**

- `db-writer.test.ts`, "DbWriter changelog after a flush commit whose response was lost
  (D113)". Its checks: the entries are `(1, 1)` and `(2, 2)`; a flush with nothing new records
  the landed manifest's id; `flushed_seq` is 2; L0 holds each row. On the code before, the
  entries are `(1, 1)` and `(1, 2)`.
- `change-feed.test.ts`, "change feed over a changelog entry an earlier one overlaps (D113)". A
  manifest holds a repeated entry, and the feed returns 1, 2. On the code before it returns 1,
  1, 2.
- `manifest.test.ts`, "commitL0Flush's start (D113)".

**Mutations: 6 of 6 caught.**

- the stale check removed;
- the manifest's state not recorded;
- no second attempt;
- the refusing manifest's id ignored;
- the reader's high-water mark never advanced;
- the reader skipping by `afterSeq` only.

**Model.** `ChangeFeed.tla` (`change-feed`, skill `verify-change-feed`). A consumer follows the
feed against flushes whose commit may land and lose its response, restarts, and prunes.

- `ChangeFeed.cfg`: the code passes `ExactlyOnce`, `NoOverlappingEntries`, and
  `FlushedInManifest`. `lostResponseReexport` (no stale refusal) fails `NoOverlappingEntries`.
  `noExpiredCheck` (before D80) and `appendAfterCut` (before D93) fail `ExactlyOnce`.
- `ChangeFeedOldData.cfg` drops `NoOverlappingEntries`: overlapping entries are allowed, as in
  old manifests. `beforeD113` (no stale refusal and no reader skip) fails `ExactlyOnce`.
  `lostResponseReexport` passes: the reader alone reads old data once.

## D114. The lifecycle is modeled across three databases

`Lifecycle.tla` models a source, a fork, and a fork of the fork:

- holds, rewrites, and releases (D77);
- GC on every database, protecting pinned manifests;
- `deleteCheckpoint` of a hold (D91);
- removal step by step, interrupted and resumed, keeping its `Removed` head (D91);
- a stale committer after a removal;
- recreation with readers that cached the old incarnation (D90).

**Properties and the variants that fail them:**

| Property                   | Variant                                                                 |
| -------------------------- | ----------------------------------------------------------------------- |
| `NoReaderNeedsDeleted`     | both forms of the early release before D77; the deleted hold before D91 |
| `RemovedNeverReopens`      | the deleted head before D91                                             |
| `NoOldIncarnationServed`   | the immediate recreation before D90                                     |
| `HoldsReleased` (liveness) | —                                                                       |
| `PrefixEmptied` (liveness) | —                                                                       |

Two collections at once (D83) are not modeled. D60's argument and the collection's boundary
re-read cover them, with tests.

## D115. A refused flush marks only its own phase fenced; a seal forgets its phase only after applying its edit

**Found by the `WriterObject` model (D109)**, not by an audit. Two TLC counterexamples, both
replayed as failing tests in `db-writer.test.ts`.

**What went wrong.** A flush reads the writer's phase (`ready`, epoch e) when it takes the flush
lock. If the writer starts again while that flush is in flight, the new start fences in at e + 1,
and the flush's commit at e is refused with `WriterFencedError`. `markFenced` then set the phase to
`Fenced` unconditionally, over the restarted writer's own `Ready` at e + 1.

- **What it cost.** Every later request to the object failed with `WriterFencedError`.
  `WriterClient` retries a fenced writer, but the retry reaches the same object. The alarm handler
  stops on a fenced writer, so acknowledged rows waited with no alarm. It lasted until the object
  was evicted, which an object under traffic is not.
- **How the writer starts again under a flush.** Only a seal's `onExit` (D85) forgets the phase
  while another fiber can hold a `ready`. The two paths TLC found:
  1. A seal holds the commit lock, waits for the flush lock held by a flush, and its caller hangs
     up. The seal never applied its edit, but `onExit` forgot the phase anyway.
  2. A seal with nothing to flush commits outside the flush lock. Its edit is applied and the
     create fails or its caller hangs up, so `onExit` must forget (the commit may have landed). A
     flush took the flush lock and its `ready` before that, and reads `writer_meta` after a write
     has restarted the writer and committed a row. It cuts that row and its commit is refused.

**The rules.**

- **`markFenced` replaces only the `Ready` phase of the epoch the refused commit used.** A newer
  epoch is this object's own restart and stays.
- **A flush's manifest view is installed only over the `Ready` phase of its own epoch, and only
  over an older manifest.** This is the same rule as D102's reload.
- **The rule is stated once, where `phase` is declared.** A start sets the phase from the manifest
  it fenced in with. A seal sets it from the manifest it sealed. `destroy` resets it under both
  locks. A seal whose commit may have landed forgets it. Every other writer changes only the
  phase it read, at the same epoch. The stalled-write reload already did (D102). The seal's
  `Ref.set` after its commit is unconditional and stays so: the sealed manifest it commits is
  the newest truth, and the commit lock keeps any other seal out.
- **A seal forgets its phase on exit only once its edit was applied** (`commitAttempted`, set by
  the edit function). Before that, no commit of this seal can land, so the phase is still right.

**Is not forgetting sound?** Yes. D85 forgets because a seal whose commit may have landed could
leave the writer open over a sealed manifest. A seal whose edit never ran sent no create, so the
manifest is what the phase says. Not forgetting is also simpler: nothing restarts. It closes path 1
on its own, but not path 2, where the forget is required. The model shows it:

- `sealForgetsUntried` (the old forget, with the `markFenced` rule) passes.
- `markFencedAnyPhase` (the old `markFenced`, with the new forget) fails `NeverSelfFenced`
  through path 2.
- `beforeD115` (both old) fails `NeverSelfFenced`.

So the `markFenced` rule is what keeps the writer correct, and the seal rule removes a needless
restart.

**Tests** (`db-writer.test.ts`):

- "a seal interrupted before it applied its edit keeps the phase, and the flush in flight
  commits". On the code before: the flush fails with `WriterFencedError`, and so does the next
  write.
- "a flush refused at an older epoch leaves the restarted writer serving" (path 2, with a gated
  `MemTable.meta`). On the code before: the next write fails with `WriterFencedError`.

**Mutations: 4 of 6 caught.** Caught: the seal forgetting with no edit applied, `commitAttempted`
never set, `markFenced` over any phase, and the epoch test inverted. Missed: the two guards on the
flush's view install (any epoch; over a newer view). They need a flush whose commit lands, a
restart, and only then the install. Their only effect is a stale view: the next commit reloads
from it, as before D102, and `status` shows a stale `l0Count`. No test reaches them.

**Model.** `WriterObject` (D109) checks `NeverSelfFenced` with `beforeD115`, `markFencedAnyPhase`,
and `sealForgetsUntried` as `Broken` values. The model has no manifest view, so the install
guard is not in it.

## D116. The models interleave inside the locks; two defects found and fixed

The models from D110, D112 and D114 passed at once. Each lock in the code they cover is now a
broken variant, steps that stood for several awaits are split where another process can run
between them, and each spec has a deeper search configuration outside the gate. Two defects came
out, neither planted.

**A fork's hold deleted while the fork is being created** (`Lifecycle.tla`). `forkRange` commits
the hold on its source, then, after awaits, writes the fork's first manifest. `deleteCheckpoint`
(D91) read the holder, found no manifest, took the hold as an orphan, and removed it. The fork
was then created with no hold, and its source's collection could delete the SSTs it reads.

- **The rule.** A holder with no manifest counts as gone only once the hold is `gcMinAge` old. A
  removed holder is gone at once, as before. A `forkRange` that committed a hold writes the fork's
  first manifest well within `gcMinAge`, the bound D90 and D91 already rely on. An orphan hold
  (its `forkRange` died) is deletable after that.
- **Tests** (`database-lifecycle.test.ts`):
  - "deleteCheckpoint between forkRange's hold and its first manifest cannot leave the fork without
    a hold" failed before the fix;
  - "a hold whose fork was never created is deletable only once it is gcMinAge old".
- **Model.** `ForkHold`, `ForkInit`, `ForkDies`, `DeleteHoldRead`, `DeleteHoldCommit`; variant
  `beforeD116` fails `NoReaderNeedsDeleted`.

**A dropped SST's rows brought back by an unrecorded job** (`CompactorObject.tla`, with every lock
held). The poll launched a job and then recorded it in the state. An eviction between the two left
the job running in Workflows and absent from the state. `dropSst` (D62) checked the state, saw no
job reading the SST's sorted run, and committed the drop. The run keeps its id, and
`commitCompaction` checks a sorted-run input by its id, so the job then committed its merge of the
run, the dropped SST's rows included. The same gap left the job's lease unrenewed and its outputs
outside D96's protection.

- **The rule.**
  - The poll records a job (an epoch-guarded `updateExisting`) before `launcher.start`.
  - A start that fails keeps the job recorded, with its lease: Workflows may have created the
    instance and lost the reply. The next poll's progress read finishes a job Workflows does not
    hold (D103), or keeps one that runs.
- **Tests** (`compactor.test.ts`):
  - "a job launched by a poll that was evicted before recording it still blocks a drop of its
    sources" failed before the fix;
  - "a start that failed after Workflows created the instance leaves the job recorded until a read
    finishes it" fails with the old release-and-forget.
- **Model.** `PollLoad` records, `PollLaunch` launches; variant `launchBeforeRecord` fails
  `DroppedStaysDropped`.

**Each lock, and what it protects.**

| Lock                            | Variant                          | Fails                  | Unit test                                                                               |
| ------------------------------- | -------------------------------- | ---------------------- | --------------------------------------------------------------------------------------- |
| `compactor.ts` `startLock`      | `twoFenceIns`                    | `EpochNeverRegresses`  | D78's                                                                                   |
| `compactor.ts` `stateLock`      | `noStateLock`                    | `RunningStaysRecorded` | "a collection's state write never puts back the running jobs a poll replaced meanwhile" |
| `compactor.ts` `planLock`       | `noPlanLock`                     | `DroppedStaysDropped`  | "an SST drop waits for a poll that is starting a compaction of it"                      |
| `compactor.ts` `collectionLock` | —                                | none                   | —                                                                                       |
| `shard-directory.ts` lock       | `noDirectoryLock` (`ShardSplit`) | `PublishedIsSealed`    | `shard-directory-lock.test.ts`                                                          |

- **`collectionLock`** protects no safety property. `GcSafety` with two collections at once
  (`Collectors`, D83's guard, `GcSafetyTwoCollectors.cfg`) passes, and the collection's due-time
  write keeps a time it did not see (D78). The lock saves duplicate R2 work.
- **`stateLock` and the directory's lock.** Each guards a read and a write that are both Durable
  Object storage calls, with only pure code between. Input gates deliver no event during a
  storage call, and Effect resumes a fiber in the promise callback, so in production the pair is
  already atomic unless the fiber's 2048-operation budget runs out between them and Effect yields
  through `setImmediate`. The locks are the guarantee; the gates make the window rare. Their unit
  tests hold a test cell's read open, which production's gates mostly would not.
- **Removal has no lock.** Its steps are ordered and idempotent, and `markRemoved` against a new
  hold is ordered by the manifest CAS. `LifecycleRemovers.cfg` runs a second removal that may
  stall past a recreation: variant `deleteByPrefix` (deleting everything under the prefix) fails
  `NewIncarnationIntact`. `create` is split into its clear and its first manifest, and D91's
  assumption (no committer's view outlives `gcMinAge`) is explicit: variant `staleViewOutlives`
  fails `RemovedNeverReopens`.

**Not reached:**

- corrupted SSTs (D53) and the budget's fairness across groups in `CompactorObject`;
- routers two generations behind and a second split of a successor in `ShardSplit`.

The reasons are in each skill's "Bounds and assumptions".

## D117. A mutation sweep of `lsm-core`: what a surviving mutant means, and the edges now tested

- **Context.** The core's tests are mostly properties over generated histories. They pass for code
  that is right, but a property whose inputs rarely reach an edge also passes for code that is
  wrong there. A mutation sweep asks the opposite question: for each small change to the code
  (a comparison flipped, a bound moved by one, a constant raised by one, an early return removed,
  an `Option.some` replaced by `none`), does some test fail?
- **The sweep.** 800 mutants over the 15 core modules (`/tmp/shale-work/scripts/mutants.py`, a
  token-level generator that skips comments, strings, types, and imports). Each mutant runs the
  test files that import its module, one worker, `nice -n 15`. 286 survived the first pass;
  every survivor was classified.
- **Classification.** Each survivor is one of:
  - **Equivalent.** No input tells it apart: an index past the end that an `undefined` guard
    stops, a choice between equal values, a buffer capacity, a leading zero byte in
    `readU64`, a value the format rewrites anyway (mod 2^64), a branch an earlier check makes
    unreachable.
  - **Untested behavior.** The behavior is intended and nothing checked it. A test was added, and
    each fails under its mutant.
  - **Unintended behavior.** The code did something it should not. None found.
- **What the new tests pin.**
  - **Formats that are already on disk.** The Bloom filter's bits for keys of every length mod 4
    (a changed hash answers `absent` for keys older SSTs hold), the probe clamp 1–30, the
    builder's growth past 512 keys, and the SST footer's min and max seq (the change feed's entry
    bounds, `db-writer.ts`).
  - **Every limit at and one past its value.** Keys, values, operations, and bytes per batch;
    the merged-value bound in compaction (a history exactly at the bound folds); TTLs; settings
    rules at equality; the scheduler's `minCompactionSources` and `includeSizeThreshold`; a
    manifest superseded exactly `gcMinAge` ago; `groupBlockReads` at exactly `maxBytes`; a
    `ByteReader` read that ends at the last byte; a 9-byte varint refused.
  - **Documented constants.** The write-batch limits, `DEFAULT_LSM_SETTINGS`,
    `LSM_PLATFORM_LIMITS`, `MAX_MERGED_VALUE_BYTES`, `MAX_DROPPED_SST_HISTORY`,
    `MAX_CHECKPOINTS_PER_DATABASE`, `MAX_CHANGELOG_ENTRIES`, and the scheduler defaults. A change
    to one is then a deliberate edit to a test, not a slip.
  - **Manifest edits nothing in the core checked.** The removal lifecycle (`markRemoved`,
    `markRemovalCleared`, `removalExpiresAt`), checkpoint expiry at `now` and never without an
    expiry, `removeExternalRef` matching both fields, a fork leaving out runs its range empties,
    a pre-D103 retry recognized by its outputs, a pre-D80 `changelogAfterSeq`, the writer fence's
    epoch step, and `sealedSuccessors`.
  - **Order.** Key ranges against an exhaustive membership oracle over every two-byte key of
    {0, 1, 2}. The merge with a chunk ending exactly on a 256-row batch (a mutant ended the stream
    there), with 3 to 42 inputs at once, and with ties resolved to the earlier input. An L0 job's
    output above every run while a run merge is running. A put and a merge of one key take the
    sooner TTL. The merge refusals name their operation.
- **Why the generated properties missed some of these.** Generated histories are short, so most of
  40 inputs are empty and the merge heap rarely holds the five or more cursors a wrong parent index
  needs before it misorders. The many-input merge test is seeded and sized for the heap instead.
- **What survives:** 82 mutants. 80 are equivalent. Two, both in the block seek's binary search, change only how many rows a seek decodes, which no test can see without instrumenting the decoder. The report lists each with its reason
  (`/tmp/shale-work/agents/mut-core/REPORT.md`).
- **Precondition stated.** Key-range functions assume a non-empty range (`start < end`). Every
  database range is non-empty. The oracle test covers non-empty ranges only.

## D118. A `BlockServer` error answer is never cached

- **Before.** A 400 or 404 from the `BlockServer` carried no `Cache-Control`. Workers Cache stores
  nothing without a cache header by default, but nothing in the code said so.
- **Now.** Every answer with status 400 or above carries `cache-control: no-store`
  (`ERROR_CACHE_CONTROL`, set by a middleware in `block-server-http-handlers.ts`). A miss, for
  example a block of an SST not yet visible to the bucket, therefore never outlives itself in a
  cache.
- **Byte ranges.** D100's pattern (`^\d{1,15}-\d{1,10}$`) already bounds offsets below 10^15 and
  lengths below 10^10, so `1000000000000000000000-4` (which `Number` reads as 1e21) is refused
  with 400 before any arithmetic.
- **Test** (`block-server.test.ts`): a 404 and a 400 both carry `no-store`. The largest range the
  pattern allows (`999999999999999-9999999999`) answers 404 for a missing SST. An over-long offset
  and an over-long length answer 400. Commit `1ff8f4b` names this D117; it is recorded here.

## D119. A reused id's chain continues past its tombstone; no incarnation id in v1

D90 left two hazards for an id reused after a removal. Each is a concrete sequence, and each had
a failing test (`database-lifecycle.test.ts`).

- **An old snapshot.**
  1. A client takes a `DbSnapshot` (manifest 3, sequence number 1).
  2. The database is removed. After `gcMinAge` the id is created again.
  3. The new chain's manifests 1 to 3 are its init, its writer's fence, and its first flush,
     which is also sequence number 1.
  4. A read at the old snapshot returned the new database's value (`new-0`), not
     `SnapshotExpired`. The snapshot's promise had ended, but the documented answer for an
     expired snapshot is `SnapshotExpired`.
- **An old compaction.**
  1. A job of the removed database merges its sorted run 0 at compactor epoch 1, and outlives
     the removal by more than `gcMinAge`. A fork's rewrite reads its source's SSTs, which the
     removal does not delete, and D103 puts a large job at about an hour per 8 GiB.
  2. The new chain's first compactor also fences in at epoch 1, and the new chain also has a run 0.
  3. The old commit's epoch and inputs check passed: it would have replaced the new run with
     the old outputs. The test commits it by hand, so it shows the checks passing.

**The rule.** When `create` clears a removed incarnation's tombstone (D91), the new chain starts
past it:

- `lastL0Seq` = the tombstone's + 1 (and `changelogAfterSeq` there);
- `lastL0ClockTick` = the tombstone's;
- writer and compactor epochs = the tombstone's + 1.

Every old job's epoch is then below the new chain's, so `commitCompaction` fences it. Every old
handle's sequence number is at or below the tombstone's, so no manifest of the new chain matches
it. A snapshot read now also uses its manifest only when that manifest's `lastL0Seq` equals the
snapshot's sequence number, which every snapshot this code issues satisfies (`snapshot`,
`pinSnapshot`). Otherwise the read is `SnapshotExpired`.

**Why not an incarnation id.** Both hazards need a handle or a job of the removed database that
outlives its removal by more than `gcMinAge`. While the tombstone exists, continuing the
numbering closes both with no format or protocol change.

**What remains.** The sweep deletes a cleared tombstone `gcMinAge` after the removal, and the
next sweep cycle at most later. A `create` after that finds no tombstone, and its chain starts
at 0 again. Both hazards then need an old handle or job that outlives the removal by more than
`gcMinAge` plus that wait. The snapshot's promise ended long before. A compaction job's steps
read inputs the removal deleted, except a fork's rewrite.

**Tests** (`database-lifecycle.test.ts`):

- an old snapshot reads `SnapshotExpired` after the id's reuse;
- an old compaction over a run with the new chain's id is `CompactorFenced`;
- after an interrupted removal, the new chain's first write is sequence number 3, past the
  tombstone's 1.

Mutations: dropping the sequence carry, the compactor-epoch carry, or the snapshot's sequence
check is each caught. Dropping the writer-epoch carry is equivalent: the removal destroys the
writer under its commit and flush locks, so no writer of the removed database can commit later.

**Also in this round.**

- **A missing Workflow instance** finishes its job on the next poll (`compactor.test.ts`, a
  fake launcher that reports it). The D94 lease covers an unreadable one.
- **A thousand caller checkpoints** (half pinned snapshots, half checkpoints with 256-character
  names):
  - The newest manifest is 242,091 bytes, 6% of the 4 MiB budget. The 1,001st checkpoint is
    refused, and a commit and a cold read work.
  - A collection reads the 1,000 pinned manifests, 121 MB in all. That is a cost each pinned
    snapshot's expiry repeats, and it is left to the GC owner.
- **`manifest-state-machine.test.ts`** gets a 60 s limit. Beside the package's other files it
  ran past the default 5 s.

## D121. `tokens ensure` and `rotate` fail when a written secret never turns active

`awaitActive` waited up to about 10 s (20 reads, 500 ms apart) for a written secret to turn
`active`, then reported success whatever it saw. A deploy right after could then fail to bind a
pending secret (D104's finding 5).

**The rule.**

- `awaitSecretActive` returns whether the secret turned active; a failed read counts as not yet.
- The tokens are still shown: they cannot be read again.
- The command then exits non-zero with `SecretNotActive`, naming the secrets, and says to wait and
  deploy, not to write the token again.

**Tests** (`shaledb-access-tokens.test.ts`, `TestClock`):

- active after a pending read and a failed one: active, in 3 reads;
- never active: not active after exactly 20 reads, and the failure names the secret without the
  token.
- **Mutations caught:** the status ignored, the failure never reported, and a failed read
  aborting the wait.

## D122. Mutation sweep of the writer, the compactor, and the memtable (partial)

**The method.**

- **The generator** (`/tmp/shale-work/agents/mut-write/mutants.py`) is a token scan. It blanks
  comments and strings, then emits:
  - comparisons with their boundaries moved or flipped;
  - `&&` and `||` swapped, a negation removed, a boolean swapped;
  - `± 1` removed or flipped, an integer constant changed by one;
  - `Math.min` and `Math.max` swapped, `Option.isSome` and `isNone` swapped;
  - a lock's `withPermits` removed, an unused `yield*` statement removed, an early `return`
    removed.
- **The runner** (`run.py`) runs each mutant with
  `nice -n 15 vp test run --bail=1 --maxWorkers=1 --no-file-parallelism`, with a timeout that kills
  the process group.

**Coverage.** Under the one-worker rule, the sweep reached only part of its files in its time:

- `db-writer.ts`: 98 of 221 mutants;
- `mem-table.ts`: 115 of 138;
- `compactor.ts`: 101 of 181;
- `compaction-executor.ts`: 4 of 37.

The per-file numbers, every survivor's triage, and the files not reached are in the agent's
`REPORT.md`.

**Test gaps closed.**

- **The commit lock** (`db-writer.ts`: `commitLock` with two permits) survived every writer test.
  The mutant acknowledged a write sent while a seal's commit was in flight, and the sealed manifest
  lacked it: an acknowledged write lost. `db-writer.test.ts`, "DbWriter commit lock", holds the
  seal's manifest commit, sends a write, and requires `DatabaseSealedError` with nothing else
  durable.
- **The memtable's stored row kinds** (`ROW_KIND`: value 0, merge 1, tombstone 2) could change
  with no test failing, though a writer's SQLite outlives a deploy. `mem-table.test.ts` pins them.

**Reported to the formal agents.** Survivors in other locks: `initLock`, `flushLock`, and the
compactor's `planLock`, `collectionLock`, and `stateLock`.

## D123. Span events pass the trace policy; no key, value, or secret in telemetry or an error

**Found while auditing every attribute, log line, and error added since `f3b2ad2`.** D63's policy
(`lsmPolicyTracer`) checked every span attribute against an allowlist, but not span events. Effect
turns every log line into an event on the current span (its tracer logger):

- **The event's name is the whole message.** A message of several parts, such as
  `Effect.logError("text", { cause })`, is serialized with every object's fields.
- **The event's attributes are the log annotations,** under any name.

A probe under the policy exported `Effect.logWarning("…", { cause: { resumeFrom: "MARKERKEY" } })`
with the marker in the event's name, and `annotateLogs({ key: "MARKERKEY" })` as an attribute. No
call site at the integration head logs a key or a value. Each one was read: they log ids, R2 object
keys (`db/<id>/…`, no caller key), an `HttpServerError` (its headers redacted by Effect, the
authorization header included), or `Cause.pretty` output, which prints messages and stacks, never
an error's fields. So this was a bypass of the policy, not an observed leak.

**The rule.** Events pass the same policy as attributes:

- **The name** keeps a message's string parts; every other part becomes `[omitted]`
  (`exportedEventName`).
- **The attributes** must be primitive values. Their names must be allowlisted span attribute
  names, `effect.*`, `exception.*`, or the request middleware's `cause`, `requestId`, `method`, and
  `path` (`isExportedEventAttribute`).
- **Exception events** are built by the OTLP exporter from each error's name, message, and stack.
  Every typed error's message is code-written and names ids, counts, and sizes, never a key.
  Fields such as a scan's `resumeFrom` are not exported.

**What was checked and holds.**

- **Span attributes added in D76 to D108.** All carry ids, counts, enums, or booleans:
  `database.id`, `sst.id`, `manifest.id`, `block.offset`, `compaction.*`, `gc.*`, `alarm.failures`,
  `writer.stall_reason`, `shard.*`, `changelog.page_cut`, `cache.damaged_copy`, `object.part_retried`.
- **Error messages with an interpolated `key`** name R2 object keys, never a caller's key.
  `KeyOutOfRange` names the operation's index, not the key.
- **Schema issues repeated to the caller (D69).** A malformed bytes field's issue says what was
  expected and where, and does not quote the input (tested for `GetRequest` and `ScanRequest`).
- **The D86 `prepare` result** holds SST ids only.
- **Health and soak reports** are built from exported spans and tail records, which carry only what
  the policy lets through. The soak's mismatch list names the soak's own synthetic keys, in a local
  report file.
- **No span bypasses the policy.** No code outside `lsm-trace-policy.ts` builds a tracer, and every
  Worker's telemetry goes through `withLsmTracePolicy` (D63).

**Where a response returns the caller's own data on purpose.** The access token is the tenant
boundary (`docs/api-compatibility.md`). In each case below, the data belongs to a database the
caller's token already reads:

- **get, scan, and transaction reads** return rows;
- **the change feed** returns changes;
- **a scan page's `resumeFrom`** is the next key of the caller's own range;
- **the compaction status and an SST drop's report** give SST handles with their first and last
  keys, for the operator who holds the token;
- **Workflow step results** hold output SST handles with their keys, in the account's own Workflow
  state, like the manifest in R2.

**Not covered.** The Workers runtime's own console output (Workers Logs, `wrangler tail`) gets each
log line's full message, objects included. It is unchanged, and the call sites above put no key in
it. The policy governs only what shaledb exports to its collector.

**Tests** (`telemetry-leaks.test.ts`):

- 25 random markers, each placed where a careless line would put a key: a log line's object part,
  an unknown annotation, a known annotation holding an object, an unknown attribute, and a typed
  error's `resumeFrom`. Each exported attribute, event, and exception is searched for the marker as
  UTF-8, base64, hex, and serialized bytes.
- Public error bodies from failing operations on marked keys and values: a key out of range, a key
  over the limit, and a merged value over the bound.
- The schema issues of malformed bytes fields.
- **Four mutations caught:** the event filter removed (the code before), the event name kept whole,
  object values kept under allowed names, and every event attribute kept.

## D125. Every scan surface does a bounded amount of work per page and says where to continue

Dillon's second review: limited regular scans could end on the work budget (D108), but the
transaction scan collected every writer page and passed no budget, and the sharded scan passed
`limit: none` to each shard.

**Measured first** (counting fakes, `scan-bounds.test.ts`, `shard-scan-bounds.test.ts`):

- **The transaction scan.** For one row over 5,000 unflushed keys it read 6 writer pages and held
  all 5,000 rows; a `Latest` scan of the same reads one page.
- **The routed scan.** For one row over 4,999 unflushed tombstones, with a 2 KiB page budget, it
  read 11 writer pages, its whole shard; the database's own page ends after one, with
  `resumeFrom`.
- **One key overwritten 2,500 times.** One page of the regular or the transaction scan read three
  writer pages and held all 2,500 versions. A segment ended only at a second key, so one key's
  unflushed versions, up to `maxUnflushedBytes` (256 MiB), could be held at once.

**The rule, for all three.** A page gets one examined-bytes budget: `ScanPageBudget`, a
`Context.Reference`, defaulting to `SCAN_PAGE_EXAMINED_BYTES`, 16 MiB, and set by tests. A page
has exactly three outcomes:

- **rows up to its limit, and `resumeFrom`;**
- **fewer rows, or none, and `resumeFrom`,** because the budget was spent: not an error;
- **a typed refusal:** `SnapshotTooOld`, a routing error, or D93's retryable restart.

A null `resumeFrom` means the range is exhausted. `ReaderUnavailableError` now says why a scan
ended with `resumeFrom` (`pageEnd`: `BudgetSpent` or `OvertakenByFlush`).

- **The transaction scan** (`DbClient.scanAtPage`, `transactions/scan`):
  - It pulls writer pages as its merge needs them, in segments as D93's `Latest` scan does, all at
    `readSeq`.
  - Each segment uses a manifest within the transaction's bounds (D58, D70). By D70's argument any
    such manifest answers the same at `readSeq`, so segments and pages may use different ones.
  - The response gains `resumeFrom`, nullable, an additive change. The next page is another request
    with the same `readSeq` and `manifestId`.
  - The commit's `readRanges` must name the whole range scanned, not each page's part (OpenAPI
    description).
  - The in-Worker `DbClient.scanAt` keeps its signature and has no budget, but no longer collects
    every page first.
- **The routed scan** (`ShardRouter.scan`, the sharded `scan` endpoint):
  - Each shard gets the page's remaining rows and the one shared budget.
  - A shard's `BudgetSpent` ends the routed page, and its `resumeFrom` is the routed continuation
    (the key also names the shard).
  - Only `OvertakenByFlush` continues within the shard, as D93 did.
  - The sharded response gains `resumeFrom`, nullable.
  - A routed scan is not one snapshot across shards.
- **A settled key.** Once one key's unflushed versions already settle it (a value or a delete; older
  versions cannot change what any read at or above them sees), a segment ends at that key and the
  next starts after it (`settledVersions`, `keySuccessor`). One page of a key overwritten 2,500
  times then reads two writer pages and holds about 1,000 rows. Merge operands do not settle a key:
  a long run of them is still read whole (D99 bounds the merged result).

**Memory for one page, from named constants.** What one page holds at once is the sum of:

- the segment's writer rows: one writer page, at most `MAX_MEMTABLE_PAGE_BYTES` (8 MiB) and 1,000
  rows, plus the unsettled merge operands of one key that span pages;
- the rows returned, at most `SCAN_PAGE_EXAMINED_BYTES` (16 MiB, since every returned row was
  examined) plus one key's versions;
- the reader's blocks in flight, as D108 bounds them.

So the bound is 8 MiB + 16 MiB + one key's versions + the reader's blocks, the same for the
regular, transaction, and routed page. A routed page holds one shard's segment at a time.

**Tests.**

- **`scan-bounds.test.ts`:**
  - limit 1 over 2,999 deleted, and over 2,999 expired, keys, in the writer, in L0, and in a sorted
    run (where compaction at the bottom has dropped them). On the regular and the transaction scan,
    every page reads at most two writer pages, and following `resumeFrom` reaches exactly the live
    key.
  - a key overwritten 2,500 times;
  - a transaction scan for one row reads one writer page.
- **`shard-scan-bounds.test.ts`:**
  - the routed page ends at its budget after one writer page;
  - over two shards, it ends where one database's page over the same rows ends (one budget, not
    one per shard).
- **Five mutations caught:**
  - the transaction page without a budget;
  - no skip past a settled key;
  - a whole shard per routed page;
  - the budget not shared;
  - a budget end treated as a flush overtaking.

**Second pass.**

- **The oracle property tests:**
  - `scan-oracle.property.test.ts` covers the regular and the transaction surface. It runs 30
    seeded histories, each with runs of up to 40 deletes or expiring values, puts, flushes,
    compactions, and time passing. Each history is followed by pages with random limits (1–6) and
    budgets (200–1,000 bytes), with history between pages.
    - A regular page's rows are exactly the live keys of `[start, next)` at that page's time.
    - A transaction's pages, with writes, flushes, and compactions between them, are exactly the
      begin state.
    - Every page examines at most its budget plus the one key it finishes.
    - More than 30 pages ended on their budget.
  - `shard-scan-oracle.property.test.ts` covers the routed surface: 12 histories with splits
    between pages (more than 5 made), each routed page checked against `[start, next)`.
  - **Mutations:** 4 of 5 caught (no budget end; a transaction segment's manifest bound; a shard
    going on past its budget; a routed page skipping the rest of the range after a shard). The
    fifth, keeping one version fewer of a settled key, is caught by `scan-bounds.test.ts` instead.
- **One transaction scan in several pages** (`scan-bounds.test.ts`):
  - Two pages around an overwrite of every key, a flush, and a compaction both read the `readSeq`
    values.
  - A phantom past an early page's end but inside the range fails the commit when `readRanges`
    names the whole range. Naming only what the page returned lets the commit succeed; the OpenAPI
    text, the README, and `invariants.md` say so.
  - Mutations: a segment view not at `readSeq` is caught. Writer pages not at `readSeq` are
    equivalent, because the reader filters by the view's snapshot.
- **A long run of one key's merge operands: held whole, never split.**
  - **Why not split it.** Operands cannot be split across pages without the caller merging them,
    so a page that starts a key finishes it.
  - **What the page does.** It returns the merged value and ends right after the key with
    `resumeFrom`, the next key, when that key spent its budget. A refusal would have made a hot
    counter unreadable until a compaction folded its operands.
  - **The cost.** That key's operands. In the writer they are bounded by `maxUnflushedBytes`, and
    D99's `MergedValueTooLarge` bounds the merged result, not the operands read.
  - **Test:** 2,500 `U64Add` operands, unflushed, read whole (sum 2,500) on the regular and the
    transaction page, which end with key 1 next.
  - **Mutations:** the progress guard (`examinedAny`) removed is equivalent, because a page's
    budget starts at zero.
- **workerd** (`packages/lsm-cloudflare/test/writer/writer.workerd.ts`, the second
  `test.provider`, "scan and transaction scan pages end at their work budget with resumeFrom"):
  - 24,000 keys of 1 KiB, all but one deleted;
  - on the regular and the transaction scan, the first page answers no rows with a `resumeFrom`,
    and following it finds exactly the live key.
  - The sharded form (round 3): `packages/lsm-sharding/test/router/shard-router.workerd.ts`, on its
    own stack (`router-stack.ts`). It holds the API Worker, the `BlockServer`, the compactor, and
    the shard router Worker with its directory objects, on ports 8803–8806 and collector 4322.
    - A group's one shard holds 24,000 keys, all but two deleted. The first routed page (limit 1)
      is empty with a non-null `resumeFrom`, and following it reaches both live keys.
    - A malformed `put` is answered with D79's `InvalidRequest` envelope by the real runtime.
    - `@shaledb/sharding#test:workerd` runs it; `test` runs it after the unit tests.

## D127. The writer and compactor mutation sweep: the gaps closed, and what is left

- **Context.** The first sweep of the writer and compactor (D122) reached half its mutants, against
  a narrow test set. This one reruns them with the D117 generator, which now also mutates locks:
  `Semaphore.make(1)` to 2 permits and `.withPermits(1)` to 0. It has two stages: each file's
  direct test importers first, then every other test file that reaches the module.
- **A runner fix.** A package copied for mutation could not import its workspace siblings
  (`@shaledb/lsm-core`), so every `lsm-cloudflare` mutant "failed" on import. The runner now links
  the siblings into the copy directory and requires an unmutated baseline to pass before any
  mutant counts.
- **Gaps closed (each test fails on its mutant):**
  - **Scanned ranges (phantom protection).** No test passed a non-empty `readRanges`, so
    `MemTable.writtenAfter`'s range branch could answer "no conflict" with every test passing. A
    SerializableSnapshot commit is now refused when another commit wrote into a range it
    scanned: bounded or open, start inclusive, end exclusive (`db-transaction-ranges.test.ts`).
  - **The writer's start path** (`db-writer-start.test.ts`):
    - A writer restarted with unflushed rows schedules its flush at start, with no further
      write, and the alarm flushes them.
    - Start failures map to `StoredDataInvalid` and `ManifestContention`.
    - A sealed writer refuses other successors of the same number, and the same ones reordered
      (D95).
    - Concurrent first requests (status reads and probes, which take no other lock) fence in
      once (`initLock`).
    - Concurrent flushes write the rows once (`flushLock`).
  - **The memtable** (`mem-table.test.ts`):
    - trim at exactly `retentionMs` and at exactly `maxRetainedBytes`;
    - `seed` only raises;
    - a stale smaller flush cut keeps the recorded flushed manifest.
  - **The compactor** (`compactor.test.ts`):
    - `destroy` cancels the alarm and clears the state.
    - A collection is told the oldest of several running jobs' starts (D96).
    - A sorted run's SST drops, refused only while a job reads that run.
- **Locks no unit test reaches.** The test `CompactorStateCell` never yields between `get` and
  `set`, so `stateLock` (and `collectionLock`, `planLock` around it) can be removed with every test
  passing. Production storage does yield. `CROSS: formal`: model them in `CompactorObject`, or
  give the test cell a yielding variant.
- **Not finished.** Part of the compactor triage, the `db-writer` runner sweep,
  `compaction-executor`, `compaction-budget`, `sst-store`, and `object-store`. The numbers and
  the resume command are in `/tmp/shale-work/agents/mut-write2/REPORT.md`.

## D128. Mutation sweep of the request edge and the sharding services

- **Scope.** The request bound (`db-http-request.ts`, D100), the public error translation
  (`db-public-error-translation.ts`), the access gate (`lsm-access-gate.ts`, D64), the trace policy
  (`lsm-trace-policy.ts`, D63, D123), and `lsm-sharding`'s `shard-directory.ts` and
  `shard-split.ts`. Same generator and method as D117: 165 mutants, each run against every test
  file that imports its module, one worker, `nice -n 15`.
- **Result.**

  | File                             | Mutants | Survived, first pass | Survived, after new tests         |
  | -------------------------------- | ------- | -------------------- | --------------------------------- |
  | `lsm-access-gate.ts`             | 31      | 0                    | 0                                 |
  | `lsm-trace-policy.ts`            | 37      | 5                    | 1 (equivalent)                    |
  | `db-http-request.ts`             | 30      | 10                   | 4 (2 need workerd, 2 no contract) |
  | `db-public-error-translation.ts` | 32      | 21                   | 0                                 |
  | `shard-directory.ts`             | 22      | 0                    | 0                                 |
  | `shard-split.ts`                 | 13      | 1                    | 1 (equivalent)                    |
  | **Total**                        | **165** | **37**               | **6**                             |

- **Unintended behavior found: one.** A read of a corrupted SST answers `InternalFailure`, not the
  `StoredDataInvalid` that D53 and the corruption runbook (`docs/operations.md`) promise. It has
  done so since Phase 4. `SstCorruptedError` carries no database id, and `StoredDataInvalid`'s
  details need one, so the fix belongs where the id is known: the handlers' `publicFailures`
  (`db-http-handlers.ts`) or the reader.
  - **Fixed (scan-bounds round 3).** `publicFailures(operation, databaseId)` passes the database
    the request names to `toPublicError`. With it, `SstCorruptedError` answers `StoredDataInvalid`
    (500, not retryable), naming the database and the SST. This covers every database API read:
    get, scan, snapshot, transaction get and scan, and changes. Without an id, the answer stays
    `InternalFailure`.
  - **The sharded API** answers `ShardedRequestRefused` with reason `SstCorruptedError` (409, as
    every non-retryable refusal). Its message names the SST, not the shard.
  - **The health report** (`measure health`) gains an issue for error spans of reads that met
    `SstCorruptedError`. A compaction may not have reported the SST yet.
  - **The soak** does not list `SstCorruptedError` as expected, so one fails the soak.
  - **Tests:** the `it.fails` case is now a plain case. `db-http-corruption.test.ts` covers a
    `Flushed` get and scan through the router. `shard-router-worker.test.ts` and
    `health-report.test.ts` each gain one case. Two mutations caught: the handler passing no id,
    and the answer marked retryable.
- **What the new tests pin.**
  - Every internal error's public code, error, `retryable`, and details, one case each
    (`db-public-error-translation.test.ts`). Before, 21 of 32 mutants survived: 20 of them changed
    a `retryable`, so a caller could have been told to give up on a request that would succeed, or
    to repeat one that never will. Also pinned: the "may have committed" advice only on an
    unreachable writer for a write or transaction (D88), and only on a `WriteConflict`.
  - The shared request middleware over a probe API (`db-http-request-middleware.test.ts`): a
    handler defect and a response that fails its own schema both answer `InternalFailure` (500,
    not retryable), never `InvalidRequest`; a schema issue reaches the caller whole.
  - A declared body of exactly 16 MiB reaches its handler (`db-http-request.test.ts`).
  - `withLsmTracePolicy` registers the policy at runtime and only binds at plan time
    (`lsm-trace-policy-layer.test.ts`). Without it, spans would reach the exporter unredacted.
  - An event with no attributes passes through; the inner tracer's context hook is kept; rate 1
    keeps every trace whatever its id (`telemetry-leaks.test.ts`, `lsm-trace-policy.test.ts`).
- **What survives.**
  - `lsm-trace-policy.ts:125`, the `!Array.isArray` guard removed: equivalent, since a JSON text
    that starts with `[` parses only to an array.
  - `shard-split.ts:105`, fork concurrency 2 to 3: equivalent, since there are two forks.
  - `db-http-request.ts:169`, the declared-length refusal removed, and `:202`, the `BodyTooLarge`
    branch flipped: need workerd. In Node, Effect's `MaxBodySize` already refuses a declared length
    over the bound, and an oversized chunked body arrives as a defect (`bodyTooLarge`), not as the
    `RequestParseError` tag. D100's correction says the Workers runtime differs, and only the
    workerd tests run it.
  - `db-http-request.ts:61` and `:79`, the 300-character issue cap moved by one: no contract
    names the exact cap.
- the oracle property test over the three surfaces with random budgets and writes, flushes, and
  compactions between pages;
- the tests of two transaction pages separated by a flush and a compaction, and of a phantom
  beyond an early page's end but inside the scanned range;
- the workerd assertions of the HTTP shapes.

## D129. The checkpoint cap is set by what a collection reads: 128 per database

**The contradiction.** D99 allowed 1,000 caller checkpoints per database, sized by their cost
inside the manifest (about 130 bytes each). D98 bounds a collection by the number of manifests it
loads (5,000), not by their bytes. A collection reads every manifest a checkpoint pins, whole, so
with 1,000 pins it reads 1,000 manifests.

- **Measured.** At small manifests (242 KB, 1,000 checkpoints with 256-character names), one
  collection read 121,124,785 bytes, and each pin's expiry schedules another collection.
- **Derived worst case.** At D99's 4 MiB manifest budget, one collection reads
  1,000 × 4 MiB = 4 GiB. Decoding that is about 41 s at D99's measured 10 ms per MiB in Node,
  against the compactor's 60 s CPU limit. That leaves no room for workerd being slower, the
  listings, or 4 GB of R2 reads. The collection exceeds its CPU on every run and deletes no SST
  again.
- **What a collection holds.** Little: it folds each manifest into a set of SST ids and drops it
  (D98). One decoded manifest at a time, about 16 to 40 MiB at 4 MiB. The cost is CPU and bytes
  read, not memory.

So the two limits contradicted each other at their maxima.

**The rule.** The cap is derived from a read budget:

- A collection may read `GC_PINNED_MANIFEST_READ_BUDGET_BYTES` = 512 MiB of pinned manifests.
  That is about 5 s of decoding, a tenth of the compactor's CPU limit.
- A manifest is kept within `MANIFEST_SIZE_BUDGET_BYTES` = 4 MiB (D99).
- So `MAX_CHECKPOINTS_PER_DATABASE` = 512 MiB / 4 MiB = **128**.
- The public error is unchanged (`CheckpointLimitReached`, 409); only the number changes, which
  is allowed because v1 is not released.
- Forks' and successors' holds are still not counted. A database has as many of those as it has
  forks.

**Rejected.**

- **Reading pinned manifests' references in bounded pieces across runs.** A collection could
  delete only once a full pass is done, and pins can be added and expire between pieces. The
  progress would have to live in the compactor's state, and the pass restarts whenever the set
  of pins changes.
- **A pinned manifest's reference set cached in the compactor's state.** Up to 16,400 SST ids per
  pinned manifest at the 1 TiB limit (D99), times the pins. Too large for one Durable Object
  value, and it is a second copy of data the manifest already holds.

Both would keep 1,000. Neither is needed for a limit that covers the use: a pinned snapshot
serves a long read, and 128 at once per database is many.

**What D98's budget still allows.** Young manifests count against the 5,000 loads by number,
not bytes. At 4 MiB manifests that is up to 20 GiB, which takes a busy database at the 1 TiB
limit. That is D98's limit, not this one's; it is left to D98's owner.

**Tests.**

- `manifest.test.ts`: the cap times the manifest budget fits the read budget, and the read
  budget's decode time is at most a tenth of the CPU limit. D99's 1,000 fails it, and so does
  a budget twice as large.
- `database-lifecycle.test.ts`, with the most checkpoints allowed:
  - the 129th is refused;
  - the newest manifest is 31,556 bytes;
  - a collection reads 2,037,306 bytes of pinned manifests and deletes the four SSTs a
    compaction replaced.
- `CROSS`: `garbage-collector.ts` is unchanged. The fix is in the cap.

**Also in this round (D119's follow-ups).**

- `ManifestCache` keeps the newer manifest when an older load finishes last
  (`manifest-store.test.ts`).
- A change-feed page that ends at a batch boundary (`change-feed.test.ts`).
- Two mutants of `db-reader.ts`'s change feed are equivalent:
  - **The byte bound with `batchStart >= 0`.** A batch's keys and values are at most
    `MAX_BATCH_BYTES`, which equals `MAX_MEMTABLE_PAGE_BYTES`, counted the same way, so a
    first batch alone never passes the bound. No consumer stalls.
  - **The row limit with `>`.** `DbClient.changes` cuts again at the limit.
