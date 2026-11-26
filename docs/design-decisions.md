# Design decisions

Each entry records a place where the implementation needed a decision the design page did not make,
or deviates from it, with the reason. Newest last.

## D1. Repository layout and package names

- Libraries: `@cf-slatedb/lsm-core` (`packages/lsm-core`), `@cf-slatedb/lsm-cloudflare`
  (`packages/lsm-cloudflare`), and `@cf-slatedb/sharding` (`packages/lsm-sharding`, named as in
  design §7 "the optional `@cf-slatedb/sharding` package").
- Stack: `@cf-slatedb/infra` (`infra/cf-slatedb`), no `exports`.
- Tools: `@cf-slatedb/tools-*`.

Reason: the brief's layout (design §10) with iac's naming conventions. iac's `shared-infra`
distinction does not apply here: the libraries are the product, and there is one Stack.

## D2. Workspace boundary rules

iac's boundary test is adapted rather than copied: in addition to "no `exports` under `infra/`" and
"no relative import leaves its workspace", it enforces the two rules that are specific to this
design: `@cf-slatedb/lsm-core` imports no Cloudflare or Alchemy module, and
`@cf-slatedb/sharding` uses only the public entrypoints of the two libraries (design §12: "The
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

`infra/cf-slatedb/alchemy.run.ts` uses `localState()` rather than iac's `Cloudflare.state()`,
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

SlateDB returns expired rows from reads and relies on compaction to delete them. cf-slatedb hides
them: an expired value reads as a delete and an expired merge operand is skipped
(`resolveVisibleRow`). Compaction applies SlateDB's rules (expired value → tombstone, expired operand
dropped; `retainKeyVersions`), so reads before and after compaction agree (property-tested).

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
earliest TTL. Limits: key 16 KiB, value 1 MiB (Durable Object SQLite rows hold at most 2 MB),
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

`WriterServer` and `DbApiWorker` are defined in `@cf-slatedb/lsm-cloudflare` (as Alchemy class +
Layer), so the library's workerd tests can run them. `infra/cf-slatedb` only composes them into the
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
it. Its alarm polls every `compactorPollInterval` while jobs run or wait for budget, and stops
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

The package uses only `@cf-slatedb/lsm-cloudflare`'s public entrypoint. `dbServicesLayer` gives any
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
behind retention gets `ChangelogUnavailable` (`Expired`) instead of a silent gap.

## D43. Live stages keep Alchemy state in Cloudflare; local stages keep it on the machine

`infra/cf-slatedb/alchemy.run.ts` selects its state Layer from the stage (`cfSlatedbStateLayer` in
`infra/cf-slatedb/src/cf-slatedb-stage.ts`). Local stages (`dev`, `test`, `local`, and their `-*`
variants, the same rule the telemetry flush policy uses: `isLocalLsmStage`) use `localState()`, so
`vp run dev` and the workerd tests never contact Cloudflare. Every live stage (`preview-1`,
`preview-2`, `preview-3`, and `production` should it ever be approved) uses `Cloudflare.state()`, as
`../iac` does, so a preview deployed from one machine can be redeployed, inspected, or handed to
Dillon for destruction from another.

The store is pinned to Dillon's account (`accountId: 99459fce09d9a9eec83befd957af4ec5`), so a login
that reaches several accounts can never read or write this Stack's state elsewhere. It uses the
account's default `alchemy-state-store` Worker rather than a dedicated one: the Stack name
(`CfSlatedb`) and stage already key its records apart from other projects', and a second store would
add a second Worker and secret pair to the account for no isolation gain.

## D44. Live stages gate their public Workers with a per-stage access token

On a live stage the API Worker, the shard router, the GC operator API, and the telemetry probe have
public `workers.dev` URLs, and the design leaves authorization to callers ("Callers authorize access
to the database before they fetch"). An ungated preview would be a public, writable database API.
So every live stage generates one access token (`LsmAccessToken`, an Alchemy `Random` of 256 bits,
created once and kept in the stage's state, so it survives redeploys and every machine reads the
same value) and binds it to those Workers as a secret (`CF_SLATEDB_ACCESS_TOKEN`).
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
`vp run @cf-slatedb/infra#deploy:<stage>` (one explicit task per approved stage, so no stage name is
ever interpolated into a deploy command) with the task's log on stderr, then reads the outputs back.
`status`, `url`, and `token` read the Cloudflare state store through the read-only
`@cf-slatedb/infra#state:read` task (`alchemy state read --backend cloudflare -r CfSlatedb[/<stage>]`)
and parse the Stack output record (`CfSlatedb/<stage>/output`) and the token record; nothing is kept
on the local machine, so they work wherever the account's Alchemy login does.

A preview's engine settings are the production defaults. `deploy --dev-timings` binds `vp run dev`'s
short GC and compactor timings (`CF_SLATEDB_LSM_SETTINGS`); the CLI drops any value of that variable
inherited from the shell, so only the flag decides, and a later deploy without the flag restores the
defaults. `destroy-command <stage>` prints `vp run @cf-slatedb/infra#destroy:<stage>` for Dillon;
the deployment guard blocks agents from running `alchemy destroy`.

## D46. Live stages send spans to their own TraceCollector Worker; the trace CLI pulls them

Deployed Workers cannot reach the trace CLI's `127.0.0.1:4318`. Every live stage therefore gets a
`TraceCollectorWorker` (`infra/cf-slatedb/src/trace-collector-worker.ts`), and only live stages do:

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
  Layer). Tests pass `lsmOtlpTelemetry(url)`; the Stack passes `cfSlatedbTelemetry`, which on live
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
exports spans. Calls between cf-slatedb's Workers use service bindings and Durable Object stubs,
which the flag does not affect. Local stages leave it off, because their spans go to `127.0.0.1`,
which a strictly public `fetch()` cannot reach.

## D48. Live stages record per-invocation CPU time with a Tail Worker; measurements drive the router's window

**Router polling superseded by D57** (routers wait on the directory; the 30 s bound stays).

Design §12 asks for flush CPU per MB and rows/s per writer, but a Worker cannot read its own CPU
time (its clock advances only across I/O). Live stages therefore declare `CpuTailWorker`
(`infra/cf-slatedb/src/cpu-tail-worker.ts`), a plain module Worker with a `tail()` handler — Alchemy's
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
applies to a whole installation: `CF_SLATEDB_JURISDICTION` in the deploy's environment
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
`preview-1`, `CF_SLATEDB_LSM_SETTINGS` from a `--dev-timings` deploy stayed bound through the next
deploys without the flag (found by reading the Workers' bindings in the state store). For the
engine settings that only meant dev GC and compactor timings lingered; for the jurisdiction it would
mean Workers still addressing EU Durable Objects after a Stack moved out of the EU.

So the preview CLI always sets both explicitly (`deployEnvironment`): `CF_SLATEDB_LSM_SETTINGS` is
`{}` (no overrides) unless `--dev-timings`, and `CF_SLATEDB_JURISDICTION` is `eu`, `fedramp`, or the
new value `none`, which `lsmJurisdiction` reads as no restriction. Verified on `preview-1`: every
Worker's bindings now hold `{}` and `none`. A future production deploy task must do the same.

## D55. The Stack owns its configuration and binds every Worker its complete settings

Supersedes D54's mechanism. D54 patched one deploy path: `preview-cli` always passed
`CF_SLATEDB_LSM_SETTINGS` and `CF_SLATEDB_JURISDICTION`, so the values Alchemy's `Config`
interception binds (and Cloudflare keeps across uploads) were overwritten. Any other path — a manual
`vp run …#deploy:*`, a future production task — still left whatever an earlier deploy bound, and
three Workers (`BlockServer`, the probe, the trace collector) never had the settings bound at all.

Now one typed configuration, `CfSlatedbStackConfig` (`infra/cf-slatedb/src/cf-slatedb-stack-config.ts`),
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
with the dev timings), then by a bare `vp run @cf-slatedb/infra#deploy:preview-1` with no
`CF_SLATEDB_*` variables (every Worker bound the complete production settings and `none`).
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
  bucket's location once: `CF_SLATEDB_BUCKET_LOCATION` (`wnam` … `oc`, default `auto`, meaning R2
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
  (`Compactor.collect`). That keeps `GcSafety.tla`'s one-collection-at-a-time assumption true by
  construction. Collections still interleave with compaction commits, which the model already covers.
- **The cron is a paged maintenance sweep** (`MaintenanceSweep.sweepPage`, 50 databases per run,
  position in `maintenance/sweep.cursor`, listing with `startAfter`). It handles only what has no
  actor of its own:
  - It sweeps prefixes without a manifest (removed databases' leftovers).
  - It wakes the compactor of any database whose manifest still names external refs, so a lost fork
    wake (D57) only delays the rewrite.
  - It asks for a collection on any database more than 16 manifests past GC's boundary.
    `POST /v1/gc` runs one page. A run's cost is bounded by the page, whatever the number of databases.
- The GC Worker now also reports to the CPU Tail Worker.

Tests: `garbage-collector.test.ts` runs the sweep over 17 databases in pages of 5 and checks every
database is visited once per cycle (fork woken, piled-up manifests requested, leftovers swept) and
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
`application/x-cf-slatedb-batch` body remains possible later if callers need it.

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

**CPU limits come from the Stack configuration (P5).** `CfSlatedbStackConfig.limits` sets
`apiCpuMs` (default 30,000) and `compactorCpuMs` (default 60,000). Both are bound as `limits.cpu_ms`
on the API Worker, whose writers run under it, and on the compactor Worker.
`CF_SLATEDB_API_CPU_MS` and `CF_SLATEDB_COMPACTOR_CPU_MS` override them, from 10 to 300,000 ms;
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
