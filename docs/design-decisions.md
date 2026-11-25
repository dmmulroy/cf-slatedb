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

## D30. Snapshots pin a manifest id

`DbClient.snapshot` returns `{ databaseId, manifestId, snapshotSeq }`; reads with it read exactly
that manifest. Until checkpoints (Phase 7) pin it, a snapshot is protected by GC `minAge` only, and
a read of a collected snapshot fails with `SnapshotExpired`.

## D31. `BlockServer` is called with the service binding's `fetch()`

`Cloudflare.Workers.bindWorker` returns an RPC proxy, and RPC bypasses Workers Cache. `BlockSource`
calls `bindWorker` only to register the binding, reads the raw `Fetcher` from the runtime
`WorkerEnvironment` (present at runtime, absent while planning), and wraps it with
`fromCloudflareFetcher` → `toHttpClient` → the generated `BlockServerHttpApi` client, once per
invocation. A missing binding is a defect.

## D32. L1 cache budget is by entry count

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

## D34. Compaction runs one output SST per Workflow step

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

The design's `ShardSplit` is seal → fork once → publish. Two databases are created per split, and a
split that stops after sealing must be finishable, or the sealed shard refuses writes forever. So the
group's directory (one `ShardDirectoryServer` per group) records the split first (`beginSplit`) and
allocates the two new database ids (`s-<ulid>`; derived ids would outgrow `DatabaseId`'s 63
characters after a few splits). Only one split per group runs at a time; retrying a split resumes it
with the same ids (seal, checkpoint, and forks are idempotent), and `completeSplit` publishes the new
map (version + 1). The split's own checkpoint expires after an hour if the split stops, and is
deleted once both forks hold theirs.

`ShardRouter` caches each group's map in the isolate. A stale map reaches only a sealed, out-of-range,
or removed database, which fail with typed errors; the router then refreshes and retries every 100 ms
for up to five seconds (a split's publish window), as the design describes. The core keeps no map
versions. Batches are atomic only within a database, so a batch spanning shards is refused with
`CrossShardBatchError`.

The package uses only `@cf-slatedb/lsm-cloudflare`'s public entrypoint. `dbServicesLayer` gives any
Worker in the Stack `DbClient` and `DatabaseLifecycle` over cross-script bindings; the example
`ShardRouterWorker` (in the Stack, port 1341 under `vp run dev`) hosts the directories and serves a
small sharded API.

## D41. Transactions are valid until data newer than their reads is flushed

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
`Authorization: Bearer <token>` gets `401` with `WWW-Authenticate: Bearer` and a JSON body
(`code: "Unauthenticated"`, `requestId`, `retryable: false`). The comparison's time depends only on
the token's length.

The gate stands in for Cloudflare Access and, like Access, rejects before the API contract, so its
`401` is not one of `DbHttpApi`'s declared errors (docs/errors.md: "Infrastructure such as Cloudflare
Access may reject a request before it reaches that application contract"). Local stages bind no
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
