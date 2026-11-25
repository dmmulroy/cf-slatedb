# Coverage matrix

Every design section and decision from `docs/show-me-r2-lsm-design.html` → implementing files →
tests, trace assertions, and formal checks. Status is `pending`, `partial`, or `done`. Paths are
relative to `packages/` unless they start with `infra/`, `tools/`, or `docs/`.

## §1 What changes from SlateDB

- **Single writer per database, fenced by `writerEpoch`** — done. `WriterServer` DO keyed by
  `DatabaseId` (`writer-server.ts`); `DbWriter.ensureReady` fences in with `fenceWriter`; flushes
  carry the epoch (`commitL0Flush`). Tests: `db-writer.test.ts` (a stale writer's flush and later
  writes fail with `WriterFencedError`; its SST stays an unreferenced orphan), `manifest-store.test.ts`.
  Formal: `ManifestCasFencing` (`NoStaleCommit`, `FencedWriterOrphansUnreferenced`).
- **WAL = DO SQLite memtable; an ack is durable; no `awaitDurable`** — done. `mem-table.ts`
  (`memtable_rows (key, seq DESC)`), `writer-sql-storage.ts`; `WriteReceipt` has only `seq` and
  `createTs`. Tests: `mem-table.test.ts` (real SQLite), `writer.workerd.ts` (DO SQLite).
  Formal: `NoAckedWriteLost`.
- **Manifest CAS with R2 `If-None-Match: *`** — partial. `lsm-cloudflare/src/object-store.ts`
  (`putIfAbsent`), `manifest-store.ts` (`create`, `update` with CAS retry). Tests:
  `manifest-store.test.ts` (32 concurrent create-if-absent → one `Created`; 12 concurrent edits →
  13 manifests, none lost), `test/object-store/object-store.workerd.ts` against Alchemy's local R2
  (Headers form; 16 concurrent flush commits). Real-R2 check needs approval. Formal: fencing/CAS
  model (in progress).
- **Compaction in `CompactorServer` + Workflow** — done. `compactor.ts` (fence once, poll on the
  alarm, plan, lease, launch; idle with no work), `compactor-server.ts`, `compaction-workflow.ts`
  (`CompactionWorkflow`: `prepare`, `output-<n>`, `commit` steps; results encoded with Schema),
  `compaction-job.ts` (`runCompactionJob`, discard outcomes), `compaction-executor.ts`,
  `compactor-worker.ts`; writers wake compactors (`compaction-trigger.ts`, `compactor-client.ts`).
  Tests: `compactor.test.ts` (fence-once, idle, fencing, shared budget), `compaction-executor.test.ts`
  (oracle property across flushes and compactions, key-ordered outputs, bottom deletes, inputs
  changed, fenced), `writer.workerd.ts` (four flushes → compactor wake → Workflow → commit, reads at
  every consistency, span assertions). D33–D36.
- **Readers in any Worker; `Latest`/`AtLeast` through the writer** — done. `db-reader.ts` runs
  in the caller's isolate; `db-client.ts` builds `Latest` views from the writer's probe. Trace: the
  `Flushed` read assertion (no `WriterClient.probe`, no `ObjectStore.list`).

## §2 System

- **Topology: API Worker, `WriterServer`, `BlockServer`, R2, `CompactorServer`, Workflow, GC cron** —
  done (sharding's `ShardDirectory` comes in Phase 8). `infra/cf-slatedb/alchemy.run.ts`: `DbApiWorker`
  (hosts `WriterServer`, binds `CompactorServer` cross-script), `BlockServerWorker`, `CompactorWorker`
  (`CompactorServer`, `CompactionWorkflow`, `CompactionBudgetServer`), `GcWorker` (cron), the bucket,
  and the telemetry probe. `writer.workerd.ts` runs the same topology.

## §3 Write path

- **`POST /v1/databases/:id/batches`, `Semaphore(1)`, one SQLite transaction** — done.
  `writer-http-api.ts`, `db-http-api.ts`, `lsm-cloudflare/src/db-writer.ts` (`commitLock`), `MemTable.commit`.
  Tests: `db-writer.test.ts`, `writer-client.test.ts`, `writer.workerd.ts`.
- **Flush trigger: `l0SstSize`, `maxL0Age`, `setAlarm(now)`** — done. `scheduleFlush`/`runAlarm`
  in `lsm-cloudflare/src/db-writer.ts`, `durable-object-alarm.ts`. Tests: `db-writer.test.ts` (alarm at `maxL0Age`, `TestClock`),
  `writer.workerd.ts` (the real alarm flushes after `maxL0Age`).
- **`flushL0`: SST build, PUT, manifest CAS, delete rows ≤ cut** — done. `flushNow` in `lsm-cloudflare/src/db-writer.ts`,
  `sst-store.ts` (streamed multipart), `commitL0Flush`, `MemTable.truncate`. Tests:
  `db-writer.test.ts` (SST bytes decode to the flushed rows; crash between commit and truncate).
  Trace: the flush assertion (exactly one `ManifestStore.create`, `MemTable.truncate` under
  `DbWriter.flush`, no `ObjectStore.list`, no errors, safe attributes). Formal: `NoAckedWriteLost`.
- **Changelog export (CDC), `changes?afterSeq=`** — done (write side). `exportChangelog` in
  `lsm-cloudflare/src/db-writer.ts` (sequence-ordered `Changelog` SST per flush, `Manifest.changelog` entry),
  `GET /v1/databases/:id/changes`. Tests: `db-writer.test.ts`, `writer-client.test.ts`. Retention
  by GC: Phase 6.

## §4 Read path

- **`Flushed`, `AtLeast(seq)`, `Latest` (probe + `flushedSeq`)** — done. `read-consistency.ts`
  (lsm-core), `DbClient.pointView`/`scanView`, `DbReader.manifestFor` (refresh when older than
  `flushedSeq`/`seq`). Tests: `db-client.test.ts` (Map-oracle property at every consistency;
  staleness bound, shown failing without the refresh), `writer.workerd.ts`. Formal:
  `NoAckedWriteLost` (`LatestSeesAcked`) (D29).
- **SST skip by range and Bloom filter; index search; block seek** — done. `DbReader.getRow`/
  `sstVersions` (range and view skip, Bloom skip, `firstBlockForKey`, seek continuing across blocks,
  stop at the first settled level), `DbReader.scan` (`blocksOverlappingRange`, prefetch of the next
  block). Tests: `sst-codec.test.ts`, `db-client.test.ts`.
- **L1 `BlockCache` (separate data and metadata caches)** — done. `block-cache.ts` (joined
  misses, failures not cached, `cache.hit` span attribute, `lsm_block_cache_requests` metric) (D32).
  Trace: the repeat-read assertion (both caches hit, no `BlockServer` span).
- **L2 Workers Cache in front of `BlockServer`; block/meta/manifest URLs; immutable headers;
  `Cache-Tag`** — done locally; live hit check pending approval. `block-server-http-api.ts`,
  `block-server-http-handlers.ts` (D27, D28). Test: `block-server.test.ts` (status codes,
  `Cache-Control`, `Cache-Tag: db:<owner>`, latest hint, no short bodies). Live
  `Cf-Cache-Status: HIT`: open question 1.
- **`BlockServer`: separate Worker, `crossVersionCache`, `workersDev: false`, `fetch()` not RPC**
  — done. `block-server-worker.ts`, `block-source.ts` (D31). Test: `writer.workerd.ts` reads through
  the binding; trace shows `GET (client)` → `BlockServer.*` on the `cf-slatedb-blocks` service.
- **`SstHandle.metaOffset`/`metaLength`** — done. One ranged request opens an SST
  (`BlockCache.meta` → `/meta/<offset>-<length>`); `sst-codec.test.ts` checks the bounds.

## §5 Manifest CAS and fencing

- **Writer init fencing loop** — done. `ensureReady` (`ManifestStore.update(fenceWriter)` with CAS
  retry, epoch stored in `writer_meta`, sequence oracle and clock seeded from the manifest) (D19).
- **`manifest/LATEST` hint + probe `id+1`; no LIST on read paths** — partial.
  `lsm-cloudflare/src/manifest-reader.ts` (`loadLatest`; LIST only to recover when the hint names a
  collected manifest). `manifest-store.test.ts` covers a lagging hint and a collected hinted
  manifest. Trace assertion "no `ObjectStore.list` on a read path": Phase 5 scenarios.
- **Compactor edit replaces exactly its inputs, else fails** — done. `commitCompaction`
  (`manifest.test.ts`: replace, idempotent retry, `CompactionInputsChangedError`,
  `CompactorFencedError`, `SstBelowGcBoundaryError`); `CompactionExecutor.prepare` re-checks the
  inputs (`compaction-executor.test.ts`). Formal: `ManifestCasFencing` (`noInputsCheck`).
- **Delete-after-commit; duplicate replay is deduplicated** — done. `flushNow` truncates only after
  the manifest commit; restart drops rows `<= lastL0Seq` (D22); `mergeRowStreams` drops exact
  duplicates. Tests: `db-writer.test.ts` (crash between commit and truncate), `merge-iterator.test.ts`.
  Formal: `NoAckedWriteLost` (`truncateBeforeCommit` fails).
- **`onlyIf: new Headers({ "If-None-Match": "*" })` (workerd #2572)** — partial.
  `object-store.ts` `putIfAbsent` passes the Headers form; verified against local R2 in
  `object-store.workerd.ts` (second create returns `AlreadyExists`, stored bytes unchanged). The
  real-R2 integration test is prepared as an approval-gated live check.

## §6 Types

- **Branded ids, `KeyRange`, `RowValue`, `Row`** — done. `lsm-core/src/{ids,key-range,row}.ts`;
  `key-range.test.ts` (intersection and prefix properties).
- **`Manifest` v1 fields, `SstHandle`, `SortedRun`, `Checkpoint`, `ExternalRef`, `DatabaseState`** —
  done. `lsm-core/src/manifest.ts`; `manifest.test.ts` round-trips every field (D12).
- **SST file layout** — done. `lsm-core/src/sst-codec.ts` (D8); `sst-codec.test.ts` (bounds, counts,
  Bloom, damaged footer, unsupported version).
- **Data block codec: prefix compression, restart every 16, CRC32C** — done.
  `lsm-core/src/{block-codec,byte-codec,crc32c}.ts`; round-trip and seek properties (including
  versions crossing restart points, shown failing on a broken restart search), checksum mismatch,
  CRC32C check value.
- **Manifest as `Schema.fromJsonString(Manifest)`** — done. `ManifestJson`.

## §7 Services

- **`ObjectStore`, `ManifestStore`, `ManifestCache`** — partial. `object-store.ts` (R2 adapter
  owning `R2Error` → `ObjectStoreUnavailableError` with retry; multipart with abort on failure;
  faithful in-memory test layer: atomic create-if-absent, ranges, 1000-key pages, 5 MiB part
  minimum, `Clock` upload times), `manifest-reader.ts`, `manifest-store.ts`
  (`ManifestConflictError` internal and retried, `ManifestWriteError`, idempotent edits write
  nothing, `lsm_manifest_cas_conflicts` metric), `manifest-cache.ts` (D17), `lsm-configuration.ts`,
  `lsm-bucket.ts`, `object-keys.ts`. Tests: `manifest-store.test.ts`, `object-store.workerd.ts`.
- **`SstStore`, `BlockCache`, `BlockSource`, `BlockServer`, `DbReader`, `DbClient`** — done
  (`SstStore.openReader` is `BlockCache.meta`/`block`). Execution-scoped HTTP clients:
  `writer-client.ts`, `block-source.ts`.
- **`MemTable`, `SeqOracle`, `DbWriter`, `ChangelogExport`** — done. `mem-table.ts`,
  `db-writer.ts` (sequence oracle and clock in the commit transaction, D20; `ChangelogExport` is
  `exportChangelog`), `writer-client.ts`, `writer-server.ts`, `writer-http-api.ts`.
- **`CompactionScheduler` (pure)** — done. `lsm-core/src/size-tiered-compaction-scheduler.ts`;
  `size-tiered-compaction-scheduler.test.ts` over `Arbitrary` manifests and options: jobs are
  consecutive, all-L0 or all-runs, within min/max sources and `includeSizeThreshold`, never overlap
  each other or a job in flight, have the right destination and `isBottom`; repeatedly committing
  planned jobs converges (fallback consolidation terminates). Shown failing with the size
  threshold ignored and with in-flight sources not reserved (D15).
- **`DatabaseLifecycle`, `GarbageCollector`, `CompactionBudget`** — `GarbageCollector` and
  `CompactionBudget` done (`garbage-collector.ts`, `compaction-budget.ts`,
  `compaction-budget-server.ts`, lsm-core `compaction-budget-ledger.ts`); `DatabaseLifecycle`
  `create` done, `remove`/`seal`/`forkRange` in Phase 7.

## §8 Public API and semantics

- **`put`, `delete`, `merge`, `write`, `get`, `getRow`, `scan`, `flush`** — done. `DbClient`
  (plus `scanPrefix`, `snapshot`) and the public `DbHttpApi`. Tests: `db-client.test.ts`,
  `writer-client.test.ts`, `writer.workerd.ts`.
- **Errors** — partial (public envelope done: `db-public-error.ts`, `db-public-error-translation.ts`, D25; `WriteStalledError`, `WriterUnavailableError`, `DatabaseNotFoundError` in `lsm-errors.ts`). Core: `SstCorruptedError`, `MergeOperatorMissingError`
  (`lsm-core-errors.ts`), `WriterFencedError`, `CompactorFencedError`, `DatabaseSealedError`,
  `CompactionInputsChangedError` (`manifest.ts`). Pending: `KeyOutOfRangeError`,
  `WriteStalledError`, `ManifestConflictError`, `ObjectStoreUnavailableError`, public HTTP errors.
- **Merge operators and TTL during reads and compaction** — done. `lsm-core/src/row-resolution.ts`
  (D10, D11) and `compaction-merge.ts` (`compactRowStreams`). Tests: `row-resolution.test.ts`
  (oracle at any snapshot and time; earliest expiry; `U64Add`; `MergeOperatorMissingError`; raw
  operands without an operator), `compaction-merge.test.ts` (any split of inputs over three keys,
  with and without older data and an operator, matches the oracle; shown failing when deletes are
  dropped above the bottom, an input is skipped, or expired operands are kept),
  `compaction-executor.test.ts` (merges through real SSTs).
- **Checkpoints, clones (forks), transactions** — partial (edits in `manifest.ts`); Phases 7, 9.

## §9 Effect audit and private gaps

- **Sorted map via DO SQLite** — pending (Phase 4).
- **k-way merge over `Pull` with a private heap** — done. `lsm-core/src/merge-iterator.ts`.
- **CRC32C, Bloom filter (double hashing), ULID (`Clock` + `Random`), no compression** — done.
  `crc32c.ts` (check value in `sst-codec.test.ts`); `bloom-filter.ts` (D9) with
  `bloom-filter.test.ts`: no false negatives for any bits per key, false-positive rate within 2x of
  the optimal-probe theory at 6/10/14 bits per key, order-independent; shown failing with probes
  collapsed to one position and with one probe instead of `round(b ln 2)`. `ulid.ts` with
  `ulid.test.ts`: time round trip, valid SST ids, later milliseconds always sort after,
  `Clock`/`Random`-driven (`TestClock`). SSTs are stored uncompressed.
- **Varints and fixed-width integers** — done. `byte-codec.ts` (D16) with `byte-codec.test.ts`:
  every safe integer round-trips in minimal bytes, byte-length boundaries, truncation rejected,
  u8/u16/u32/u64/length-prefixed round trips; shown failing with an off-by-one continuation bit.

## §10 Infrastructure

- **Alchemy Stack with every Worker, DO, Workflow, and cron** — done for Phases 1–6
  (`infra/cf-slatedb/alchemy.run.ts`); `ShardDirectory` joins in Phase 8.
- **Layout `packages/lsm-core`, `packages/lsm-cloudflare`, `packages/lsm-sharding`,
  `infra/cf-slatedb`** — done. Enforced by `tools/workspace-boundaries` (D1, D2).

## §11 Trade-offs

- **`maxUnflushedBytes` backpressure; 128 MB streaming; multipart parts ≥ 5 MiB** — done.
  `WriteStalledError` on `maxUnflushedBytes` and `l0MaxSsts` (`db-writer.test.ts`; an L0 stall also
  wakes the compactor). Flushes and compaction steps stream rows into the SST builder and closed
  blocks into uniform multipart parts (`ObjectStore.putMultipart`; `toUniformParts` copies each byte
  once, `object-store.test.ts`); compaction reads inputs one block at a time with one block of
  prefetch (`CompactionExecutor.sstRows`). Peak memory per compaction step: two blocks per input
  plus one multipart part (D34).

## §12 Multi-tenancy primitives and sharding

- **Key-range bounds (`KeyOutOfRangeError`)** — done (writes). `collapseWriteBatch` checks
  `Manifest.keyRange`; public `KeyOutOfRange` (409). Tests: `write-batch.test.ts`,
  `db-writer.test.ts`.
- **Seal (`DatabaseSealedError { successors }`)** — partial. `DbWriter.seal` holds the commit lock,
  flushes, then `sealManifest` (fences the writer); later writes fail with `DatabaseSealedError` and
  the successors. Tests: `db-writer.test.ts`, `writer-client.test.ts`. Lifecycle orchestration and
  formal check 5: Phase 7.
- **Zero-copy `forkRange` (SST views + external references)** — partial (`forkManifest`;
  `manifest.test.ts`) (D13).
- **GC per database respecting fork checkpoints** — done for checkpoints (fork release is Phase 7).
  `garbage-collector.ts` lists only `db/<id>/`; checkpointed manifests and their SSTs are kept
  (`garbage-collector.test.ts`). Formal: `GcSafety` (`PinnedManifestsComplete`, `ignorePins`).
- **`DatabaseLifecycle.create`/`remove` (`DatabaseHasDependentsError`, purge `db:<id>`)** — pending.
- **`CompactionBudget` by `budgetGroup`** — done. lsm-core `compaction-budget-ledger.ts` (fair share
  among holders and waiting groups, expiring leases; property test against capacity and share,
  shown failing on four mutations), `compaction-budget.ts` (unlimited, ledger, test),
  `compaction-budget-server.ts` (global `CompactionBudgetServer` Durable Object and client).
  `compactor.test.ts`: a second database waits for the first job's lease (D35).
- **Sharding: `ShardDirectory`, `ShardRouter`, `ShardSplit`** — pending (Phase 8).

## Brief requirements

- **Trace CLI (`serve`, `list`, `show`, `tail`, `grep`, `stats`, `diff`, `assert`)** — done.
  `tools/trace-cli`; tests export real spans through `OtlpTracer` (JSON and protobuf).
- **OTLP from every Worker and DO, including alarms, flushed before the event settles** — done for
  the probe and smoke Workers; every new handler must use `eventTelemetryFlushForStage` (D5).
  Tests: `lsm-cloudflare/src/lsm-telemetry.test.ts`, `test/telemetry/*.workerd.ts`.
- **Scenario CLI (write/read, flush, compaction, fork/split, fencing, changelog)** — pending.
- **Formal checks 1–6** — partial. Check 1 (no acknowledged write lost or hidden): `NoAckedWriteLost.tla`, skill
  `.agents/skills/verify-no-acked-write-lost`. Check 2 (fencing) and 3 (manifest CAS linearizability):
  `tools/formal-verification/specs/ManifestCasFencing.tla`, skill
  `.agents/skills/verify-manifest-cas-fencing`. Check 4 (GC safety): `GcSafety.tla`, skill
  `.agents/skills/verify-gc-safety`. All run by `vp run verify`, each broken variant shown failing.
  Checks 5 and 6 pending.
- **Hardening scenarios** — pending (Phase 10).

## Phase 6 additions

- **GC boundaries (`gc/manifest.boundary`, `gcSstBoundary`)** — done. RFC 0026 manifest boundary in
  `ManifestStore.create`/`advanceManifestBoundary` (`manifest-store.test.ts`: a stale create over a
  collected id is a conflict, shown failing with `<` and without monotonicity); the SST boundary in
  the manifest (`manifest.test.ts`; `garbage-collector.test.ts`: an old orphan is collected and its
  late commit refused). Formal: `GcSafety` (`noBoundaryCheck`). D33.
- **Manifest age measured from its successor** — done. `planManifestCollection`
  (`garbage-collection-plan.test.ts`), found by the GC model (`ownUploadAge`). D33.
- **Compaction status endpoint** — done. `GET /v1/databases/:id/compaction` (`db-http-api.ts`), used
  by `writer.workerd.ts`.
- **GC operator API and cron** — done. `gc-http-api.ts`, `gc-worker.ts` (`POST /v1/gc/databases/:id`,
  `POST /v1/gc`, cron `*/5 * * * *`); `writer.workerd.ts` collects on demand. D36.
