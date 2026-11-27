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
- **Manifest CAS with R2 `If-None-Match: *`** — done, confirmed on real R2 (`preview-1`,
  scenario `create-race`: 16 concurrent creates, one `Created`). `object-store.ts` (`putIfAbsent`), `manifest-store.ts` (`create`,
  `update` with CAS retry; RFC 0026 boundary). Tests: `manifest-store.test.ts` (32 concurrent
  create-if-absent → one `Created`; 12 concurrent edits → 13 manifests, none lost; stale create over
  a collected id refused), `test/object-store/object-store.workerd.ts` against Alchemy's local R2
  (Headers form; 16 concurrent flush commits). Formal: `ManifestCasFencing` (`CreateOnce`,
  `blindCreate`).
- **Compaction in `CompactorServer` + Workflow** — done. `compactor.ts` (fence once, poll on the
  alarm, plan, lease, launch; idle with no work), `compactor-server.ts`, `compaction-workflow.ts`
  (`CompactionWorkflow`: `prepare`, `output-<n>`, `commit` steps; results encoded with Schema),
  `compaction-job.ts` (`runCompactionJob`, discard outcomes), `compaction-executor.ts`,
  `compactor-worker.ts`; writers wake compactors (`compaction-trigger.ts`, `compactor-client.ts`).
  Tests: `compactor.test.ts` (fence-once, idle, fencing, shared budget; D78: concurrent first
  wakes share one fence-in, no state write over a newer epoch or a requested collection, a fenced
  compactor fences in again on wake and alarm; D102: a failing poll backs off to its ceiling; D94: a job unseen for `compactionLeaseMs` is
  finished), `compaction-executor.test.ts`
  (oracle property across flushes and compactions, key-ordered outputs, bottom deletes, inputs
  changed, fenced; D103: a commit records its job id, so an all-dropped job's retried commit ends
  `Committed`, and a missing Workflow instance or an unknown status finishes the job; D97: a step whose rows are all dropped ends after a bounded input and
  writes nothing; D96: a job running longer than `gcMinAge` keeps its outputs through a
  collection and commits, older garbage still goes, a discarded job's outputs go later, and the
  compactor passes the oldest running job's start, taken before its launch; D86: the `prepare` step result of the largest planned merge fits Workflows'
  1 MiB, and a later step of a job whose inputs left the manifest is discarded),
  `writer.workerd.ts` (four flushes → compactor wake → Workflow → commit, reads at
  every consistency, span assertions). D33–D36.
- **Readers in any Worker; `Latest`/`AtLeast` through the writer** — done. `db-reader.ts` runs
  in the caller's isolate; `db-client.ts` builds `Latest` views from the writer's probe. Trace: the
  `Flushed` read assertion (no `WriterClient.probe`, no `ObjectStore.list`).

## §2 System

- **Topology: API Worker, `WriterServer`, `BlockServer`, R2, `CompactorServer`, Workflow, GC cron** —
  done (sharding's `ShardDirectory` comes in Phase 8). `infra/shaledb/alchemy.run.ts`: `DbApiWorker`
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
- **Changelog export (CDC), `changes?afterSeq=`** — done. Write side: `exportChangelog` in
  `db-writer.ts` (a sequence-ordered `Changelog` SST per flush, `Manifest.changelog` entry); retention
  by GC (`prepareGarbageCollection` prunes entries past `retentionMs`; their SSTs are then
  unreferenced). Read side (`ChangelogExport.read`): `DbClient.changes` (flushed changes from the
  changelog SSTs via `DbReader.changelogRows`, then the writer's unflushed ones; pages never split
  a batch), `GET /v1/databases/:id/changes?afterSeq=&limit=` (`nextAfterSeq`), errors
  `ChangelogUnavailable` (`Disabled`, `Expired`). Tests: `db-writer.test.ts`, `change-feed.test.ts`
  (exactly-once, in-order delivery across random batches, flushes, page sizes, and retention
  prunes, or `Expired`; shown failing when a page can cut a batch; D80: every entry pruned, the
  boundary at `afterSeq`, a fork before and after its first flush), `database-lifecycle.test.ts`
  (following the feed through a split), `manifest.test.ts` (`changelogAfterSeq` and its default),
  D42, D80.

## §4 Read path

- **`Flushed`, `AtLeast(seq)`, `Latest` (probe + `flushedSeq`)** — done. `read-consistency.ts`
  (lsm-core), `DbClient.pointView`/`scanUnflushed` (D93: a `Latest` scan pages lazily by segment;
  `db-client-scan.test.ts`; D108: a page examines at most 16 MiB of versions and ends early with
  `resumeFrom`, `db-http-scan.test.ts`), `DbReader.manifestFor` (refresh when older than
  `flushedSeq`/`seq`). Tests: `db-client.test.ts` (Map-oracle property at every consistency;
  staleness bound, shown failing without the refresh), `writer.workerd.ts`. Formal:
  `NoAckedWriteLost` (`LatestSeesAcked`) (D29).
- **A read the writer answered needs no fresh manifest (v1 workstream 10)** — done (D67).
  - **Code.** `ManifestCache.covering` (any cached manifest holding the probe's `flushedSeq`,
    held at most half of `gcMinAge` from its load), `recent` and `forget` (`manifest-cache.ts`);
    `ReadView.freshness` and `manifestHint` (`db-reader.ts`); `flushedManifestId` on the writer's
    probe and scan (`db-writer.ts`, `writer-http-api.ts`); `ManifestReader.loadLatestFrom`
    (`manifest-reader.ts`); `DbClient.coveredByManifest` (an `AtLeast` read skips the writer only
    while a `Flushed` read would use the same manifest).
  - **Tests.** `manifest-store.test.ts` (served past the refresh interval, never missing flushed
    rows, never past the hold; the newest manifest from the writer's last flush, never the
    superseded one; a hit vouches for one refresh interval) and `db-client.test.ts` (`AtLeast`
    within and after the interval; a missing SST drops the cached manifest and the retry
    succeeds). Eleven mutations caught.
  - **Formal.** `NoAckedWriteLost` (`ReadManifest` is any covering manifest), `GcSafety`
    (`MayBeHeld`), and `ManifestCasFencing` (`LoadFromKnownFindsHead`; `knownIgnoresBoundary`
    fails).
  - **Live.** `docs/measurements.md` ("Read latency").
- **A read never uses a manifest flushed past its snapshot** — done (D70). `ReadView.maxLastL0Seq`
  set to the writer's snapshot by `DbClient.pointView` and `scanUnflushed`; `DbClient.getRow` and `scan`
  ask the writer again on `SnapshotExpired` (`LATEST_READ_ATTEMPTS`); `DbClient.scanAt` without a
  begin manifest requires the largest `flushedSeq` of its pages. Tests: `db-client.test.ts` (a
  writer overtaken after it answers: get, scan, a scan between its pages, giving up as
  retryable; each fails on the code before), `db-transaction.test.ts` (the scan without a begin
  manifest). Formal: `NoAckedWriteLost` with `Compact`, the snapshot, and `ReadAgain`
  (`noSnapshotUpperBound` fails `LatestSeesAcked`).
- **Second round of the read path** — done (D72).
  - **Refresh from the manifest held.** `ManifestCache.refresh` → `ManifestReader.loadLatestFrom`,
    which also reads the `LATEST` hint and continues from it when the known manifest was
    superseded. Tests: `manifest-store.test.ts`. Formal: `LoadFromKnownFindsHead` from every
    stored manifest.
  - **Hedged block reads.** `hedgedBlockRead`, `makeHedgedBlockSource`, `hedgedBlockSourceLayer`
    (`block-source.ts`), `blockReadHedgeAfterMs`. Tests: `block-source.test.ts`.
  - **Scans that follow their limit.** `mergeRowStreams` emits before it pulls an input again
    (lsm-core); `scanFirstReadAheadBlocks` (`db-reader.ts`). Tests: `merge-iterator.test.ts`,
    `db-client.test.ts`.
  - **Live.** `docs/measurements.md` ("Read latency").
- **SST skip by range and Bloom filter; index search; block seek** — done. `DbReader.getRow`/
  `sstVersions` (range and view skip, Bloom skip, `firstBlockForKey`, seek continuing across blocks,
  stop at the first settled level), `DbReader.scan` (`blocksOverlappingRange`). Since D67 a point
  read fetches every candidate SST's metadata at once, a scan fetches every overlapping SST's
  metadata at once and reads ahead `SCAN_READ_AHEAD_BLOCKS` (8) blocks per SST, and
  `mergeRowStreams` pulls every input's first chunk concurrently. Blocks default to 16 KiB
  (`DEFAULT_LSM_SETTINGS.blockSize`), and hold at least 32 times their index keys (D107);
  readers follow each SST's own index. Tests: `sst-codec.test.ts` (D107: metadata at most 1/16
  of the data for keys up to 16 KiB), `merge-iterator.test.ts` (the merge properties), `db-client.test.ts`.
- **L1 `BlockCache` (separate data and metadata caches)** — done. `block-cache.ts` (completed
  values shared across requests, reads in flight never shared (D66), failures not cached,
  `cache.hit` span attribute, `lsm_block_cache_requests` metric; bounded by bytes per
  `budgetGroup`, D51). Test: `block-cache.test.ts` (a read that never finishes does not hold up a
  second read of the same block).
  Trace: the repeat-read assertion (both caches hit, no `BlockServer` span).
- **L2 Workers Cache in front of `BlockServer`; block/meta/manifest URLs; immutable headers;
  `Cache-Tag`** — done, confirmed live. `block-server-http-api.ts`,
  `block-server-http-handlers.ts` (D27, D28). Test: `block-server.test.ts` (status codes,
  `Cache-Control`, `Cache-Tag: db:<owner>`, latest hint, no short bodies; D118: every error answer `no-store`, the largest and over-long byte ranges). Live: scenario
  `block-cache` on `preview-1` — `Cf-Cache-Status: HIT` (`cache.l2.status` on `BlockSource` spans)
  for a block another Worker read first; off in a restricted Stack (D49).
- **`BlockServer`: separate Worker, `crossVersionCache`, `workersDev: false`, `fetch()` not RPC**
  — done. `block-server-worker.ts`, `block-source.ts` (D31). Test: `writer.workerd.ts` reads through
  the binding; trace shows `GET (client)` → `BlockServer.*` on the `shaledb-blocks` service.
- **`SstHandle.metaOffset`/`metaLength`** — done. One ranged request opens an SST
  (`BlockCache.meta` → `/meta/<offset>-<length>`); `sst-codec.test.ts` checks the bounds.

## §5 Manifest CAS and fencing

- **Writer init fencing loop** — done. `ensureReady` (`ManifestStore.update(fenceWriter)` with CAS
  retry, epoch stored in `writer_meta`, sequence oracle and clock seeded from the manifest) (D19).
- **`manifest/LATEST` hint + probe `id+1`; no LIST on read paths** — done, revised (D56).
  `manifest-reader.ts` `loadLatest` reads the hint and GC's manifest boundary together and probes
  from `max(hint, boundary + 1)` (the start and the next id in parallel); LIST only to recover a
  missing start. The hint is advanced in the background, never backwards (`advanceLatestHint`,
  `background-work.ts`); GC no longer rewrites it. Tests: `manifest-store.test.ts` (lagging hint,
  collected hinted manifest, a hint lagging below manifests GC deleted, the background hint never
  moving back). Trace: the `write-read` scenario and the Phase 5 dev check assert no
  `ObjectStore.list` on a `Flushed` read. Formal: `ManifestCasFencingGc.cfg` (`LoadFindsHead`;
  `hintOnlyProbe` fails). `loadLatestFrom` starts from the writer's last flush instead of the hint
  (D67; `LoadFromKnownFindsHead`; `knownIgnoresBoundary` fails).
- **Fast manifest commits (v1 workstream 2)** — done (D56). `ManifestStore.updateFrom` (an optimistic
  create from the committer's own view; only a written commit trusted from it), used by
  `DbWriter.flushNow` and `DbWriter.seal`; `BackgroundWork` (`background-work.ts`: Worker,
  Durable Object, inline-for-Workflows, and forking test layers) carries the hint off the critical
  path; `object.kind` on ObjectStore spans; `SHALEDB_BUCKET_LOCATION` in the Stack config
  (`LsmBucket` `locationHint`). Tests: `manifest-store.test.ts` (own view with one create and no
  load, from traces; stale view; collected next id; failure and no-op rechecked; hint), each
  mutation of the real code caught; `object-store.test.ts` (`objectKindOfKey`);
  `shaledb-stack-config.test.ts`, `lsm-worker-settings.test.ts` (bucket location). Trace: the
  `flush` scenario (one `ManifestStore.update` from `OwnView` in one attempt, no
  `ManifestReader.loadLatest`), `create-race` (manifest creates by `object.kind`). Formal:
  `ManifestCasFencing` (three configs; `CommitOnce`, `LoadFindsHead`; `noBoundaryReread`,
  `hintOnlyProbe`). Live (`preview-1`, production settings): `ManifestStore.update` p50 1,092 →
  357 ms, small flush p50 1,508 → 709 ms; R2 latency per region (`measure r2-placement`);
  11/11 scenarios.
- **Compactor edit replaces exactly its inputs, else fails** — done. `commitCompaction`
  (`manifest.test.ts`: replace, idempotent retry, `CompactionInputsChangedError`,
  `CompactorFencedError`, `SstBelowGcBoundaryError`); `CompactionExecutor.prepare` re-checks the
  inputs (`compaction-executor.test.ts`). Formal: `ManifestCasFencing` (`noInputsCheck`).
- **Delete-after-commit; duplicate replay is deduplicated** — done. `flushNow` truncates only after
  the manifest commit; restart drops rows `<= lastL0Seq` (D22); `mergeRowStreams` drops exact
  duplicates. Tests: `db-writer.test.ts` (crash between commit and truncate), `merge-iterator.test.ts`.
  Formal: `NoAckedWriteLost` (`truncateBeforeCommit` fails).
- **`onlyIf: new Headers({ "If-None-Match": "*" })` (workerd #2572)** — done, confirmed on real R2.
  `object-store.ts` `putIfAbsent` passes the Headers form (and `putIfVersion` the `If-Match` form);
  local R2 in `object-store.workerd.ts`; real R2 on `preview-1` (scenario `create-race`: 1
  `Created`, 15 `AlreadyExists`).

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

- **`ObjectStore`, `ManifestStore`, `ManifestCache`** — done. `object-store.ts` (R2 adapter
  owning `R2Error` → `ObjectStoreUnavailableError` with retry; multipart with abort on failure and
  single-copy re-chunking; versioned reads and `If-Match` writes; delimited listings; faithful
  in-memory test layer), `manifest-reader.ts`, `manifest-store.ts` (CAS retry, RFC 0026 boundary,
  `lsm_manifest_cas_conflicts`), `manifest-cache.ts` (D17), `lsm-configuration.ts`, `lsm-bucket.ts`,
  `object-keys.ts`. Tests: `object-store.test.ts`, `manifest-store.test.ts`,
  `object-store.workerd.ts`, `fault-injection.test.ts` (R2 outages).
- **`SstStore`, `BlockCache`, `BlockSource`, `BlockServer`, `DbReader`, `DbClient`** — done
  (`SstStore.openReader` is `BlockCache.meta`/`block`). Execution-scoped HTTP clients:
  `writer-client.ts`, `block-source.ts`.
- **`MemTable`, `SeqOracle`, `DbWriter`, `ChangelogExport`** — done. `mem-table.ts`,
  `db-writer.ts` (sequence oracle and clock in the commit transaction, D20; `ChangelogExport` is
  `exportChangelog`), `writer-client.ts`, `writer-server.ts`, `writer-http-api.ts`.
- **`CompactionScheduler` (pure)** — done. `lsm-core/src/size-tiered-compaction-scheduler.ts`;
  `size-tiered-compaction-scheduler.test.ts` over `Arbitrary` manifests and options (D87: with L0
  at `l0MaxSsts` and nothing running, every valid setting plans an L0 job): jobs are
  consecutive, all-L0 or all-runs, within min/max sources and `includeSizeThreshold`, never overlap
  each other or a job in flight, have the right destination and `isBottom`; repeatedly committing
  planned jobs converges (fallback consolidation terminates). Shown failing with the size
  threshold ignored and with in-flight sources not reserved (D15).
- **`DatabaseLifecycle`, `GarbageCollector`, `CompactionBudget`** — done. `database-lifecycle.ts`
  (`create`, `createCheckpoint`, `deleteCheckpoint`, `seal`, `forkRange`, `remove`),
  `garbage-collector.ts`, `compaction-budget.ts` + `compaction-budget-server.ts` (lsm-core
  `compaction-budget-ledger.ts`). Tests: `database-lifecycle.test.ts`, `garbage-collector.test.ts`
  (D96: a running job's outputs are kept; D98: over the manifest-read budget, old manifests go and
  SST deletion is deferred), `compactor.test.ts`, `writer.workerd.ts`; `health-report.test.ts`
  (D98: a deferring database is an issue).

## §8 Public API and semantics

- **`put`, `delete`, `merge`, `write`, `get`, `getRow`, `scan`, `flush`** — done. `DbClient`
  (plus `scanPrefix`, `snapshot`) and the public `DbHttpApi`. Tests: `db-client.test.ts`,
  `writer-client.test.ts`, `writer.workerd.ts`.
- **Errors** — done. Core (`Schema.TaggedError`): `SstCorruptedError`, `MergeOperatorMissingError`,
  `WriterFencedError`, `CompactorFencedError`, `DatabaseSealedError`, `DatabaseRemovedError`,
  `DatabaseHasDependentsError`, `CompactionInputsChangedError`, `SstBelowGcBoundaryError`,
  `KeyOutOfRangeError`, `WriteBatchInvalidError`, `TransactionConflictError`. Cloudflare:
  `WriteStalledError`, `WriterUnavailableError`, `DatabaseNotFoundError`, `DatabaseIdMismatchError`
  (`lsm-errors.ts`), `ManifestConflictError` (internal, retried), `ManifestWriteError`,
  `ManifestReadError`, `ObjectStoreUnavailableError`, `ReaderUnavailableError`,
  `ChangelogUnavailableError`, `CheckpointNotFoundError`, compaction, GC, and budget errors. Public
  envelope: `db-public-error.ts` + the exhaustive `db-public-error-translation.ts` (D25): every
  internal error maps to a code, message, request id, and `retryable`. A request its endpoint's
  schemas refuse, or with a content type no endpoint accepts, is `InvalidRequest` (400) naming the
  malformed part (`db-http-request.ts`, D69). Tests: `writer-client.test.ts`, `writer.workerd.ts`
  (public errors over HTTP, five malformed requests), each module's tests.
- **Merge operators and TTL during reads and compaction** — done. `lsm-core/src/row-resolution.ts`
  (D10, D11) and `compaction-merge.ts` (`compactRowStreams`). Tests: `row-resolution.test.ts`
  (oracle at any snapshot and time; earliest expiry; `U64Add`; `MergeOperatorMissingError`; raw
  operands without an operator), `compaction-merge.test.ts` (any split of inputs over three keys,
  with and without older data and an operator, matches the oracle; shown failing when deletes are
  dropped above the bottom, an input is skipped, or expired operands are kept; D81: agrees at
  every later time through flush windows and two compactions; mixed-expiry folds in
  `row-resolution.test.ts`), `compaction-executor.test.ts` (merges through real SSTs). D10, D11,
  D81.
- **Checkpoints, clones (forks), transactions** — done. Checkpoints and forks (Phase 7, D37). Optimistic
  transactions (Phase 9, D41): lsm-core `transaction.ts` (`TransactionIsolation`,
  `TransactionCommit`, `TransactionConflictError`); `DbWriter.commitTransaction` (conflict check and
  commit under the commit lock; `MemTable.writtenAfter`); `DbClient.transaction` (interactive:
  buffered writes, reads of its own writes and merges, recorded read keys and ranges) and the
  stateless `beginTransaction`/`getAt`/`scanAt`/`commitTransaction`; public
  `/v1/databases/:id/transactions/{begin,get,scan,commit}`. Tests: `db-transaction.test.ts`
  (serializability-in-commit-order property for `SerializableSnapshot` and no-lost-update property
  for `Snapshot` over interleavings with direct writes and flushes; write skew; own writes;
  `SnapshotTooOld`; each shown failing on its mutation), `db-writer.test.ts` (D84: a trim between
  the horizon check and the conflict query cannot hide a write; D85: a seal interrupted after or
  before its commit, and a writer evicted after it), `writer.workerd.ts` (write skew refused
  through the public API). Formal: `Transactions` (check 6).

## §9 Effect audit and private gaps

- **Sorted map via DO SQLite** — done. `mem-table.ts` (`memtable_rows` with primary key
  `(key, seq DESC)`, sorted scans as streams, D19). Tests: `mem-table.test.ts`, `db-writer.test.ts`.
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
  (`infra/shaledb/alchemy.run.ts`); `ShardDirectory` joins in Phase 8.
- **`state: Cloudflare.state()` for live stages** — done. `shaledbStateLayer`
  (`infra/shaledb/src/shaledb-stage.ts`): local state for `dev`/`test`/`local` stages,
  the account's Cloudflare state store (pinned account) for previews and production. Test:
  `infra/shaledb/src/shaledb-stage.test.ts`; `vp run dev` still runs offline. D43.
- **Preview deployment tool** — done. `tools/preview-cli` (`deploy`, `status`, `url`, `token`,
  `destroy-command`; `--json`; `--dev-timings`), tasks `deploy:preview-<n>`, `destroy:preview-<n>`
  (Dillon only), `state:read` (`infra/shaledb/vite.config.ts`). Test:
  `tools/preview-cli/src/preview-stage.test.ts` (state parsing, outputs, token, destroy command).
  Live: `preview-1` deployed and redeployed in place (no-op), `status`/`url`/`token` read the
  Cloudflare state store. D45.
- **Traces from previews** — done. `infra/shaledb/src/trace-collector-worker.ts`
  (`TraceCollectorWorker`, `TraceBucket`, `TraceCollectorToken`; live stages only),
  `shaledb-telemetry.ts` (`shaledbTelemetry`), `LsmWorkerTelemetry`/`lsmOtlpTelemetry`
  (`lsm-telemetry.ts`), `lsmWorkerCompatibility` (`lsm-stage.ts`), `tools/trace-cli/src/
  preview-trace-pull.ts` (`pull --stage`, `tail --stage`), `preview-cli collector`. Tests:
  `preview-trace-pull.test.ts` (JSON and protobuf exports across pages, repeated pulls download
  nothing twice, one collector named by stage then by URL stored once, a refused token for each
  form). Production form: `--collector-url` with `SHALEDB_TRACE_TOKEN` on `trace-cli pull|tail`
  (`trace-collector-option.ts`, `trace-collector-option.test.ts`) and `scenario-cli measure
  health|latency` (`measurement-collector-option.ts` and its test); `preview-cli trace-token`
  (`traceCollectorToken`, `traceTokenRefusal`; `preview-stage.test.ts`). Live (`preview-1`): API Worker → writer DO trees complete, DO
  alarm spans arrive, 10/10 probe traces complete, unauthenticated reads `401`. D46, D47.
- **Live checks and §12 measurements (hand-off item 4)** — done. Scenarios `block-cache`
  (Workers Cache `HIT` on `BlockServer` through a second Worker; `cache.l2.status` on `BlockSource`
  spans from `Cf-Cache-Status`, `block-source.ts`) and `create-race` (16 concurrent creates, one
  `Created`, 15 `AlreadyExists`), both also local; all 11 scenarios pass on `preview-1` and under
  `vp run dev`. `scenario-cli measure writes | flush-cpu | split-pause`
  (`tools/scenario-cli/src/measurements.ts`) with `CpuTailWorker` CPU records. Results:
  `docs/measurements.md`. D48.
- **Access gate on live stages' public Workers** — done. `lsm-access-gate.ts`
  (`LsmAccessToken`, `lsmAccessGateForStage`, replaced by `lsmAccessGateFrom` in D64), applied by `db-api-worker.ts`, `gc-worker.ts`,
  `shard-router-worker.ts`, and the telemetry probe. Test: `lsm-access-gate.test.ts`. Live:
  `preview-1` answers `401` without the token and serves the API with it. D44.
- **The Stack owns its configuration (v1 workstream 1)** — done (D55). `ShaledbStackConfig`
  (`infra/shaledb/src/shaledb-stack-config.ts`): stage, jurisdiction, complete `LsmSettings`,
  telemetry, access mode, resolved once from the deploy's environment with complete defaults; the
  Stack resolves it before declaring anything and a refused variable fails the plan.
  `bindLsmWorkerSettings` / `lsmWorkerSettingsBindings` (`lsm-worker-settings.ts`) bind every Worker
  the complete settings and an explicit jurisdiction on every deploy (each Worker layer's `settings`
  option); `LsmSettings` refuses contradictory combinations (`lsm-settings.ts`). Both buckets are
  `forceDestroy` outside production. `preview-cli bindings <stage>` reads each Worker's bound values
  from state. Tests: `shaledb-stack-config.test.ts` (defaults by stage, each variable, each
  refusal, bindings never absent), `lsm-worker-settings.test.ts` (bindings round-trip through the
  Workers' `Config` reads; pre-D55 partial values still read), `lsm-settings.test.ts`,
  `preview-stage.test.ts` (`deployEnvironment`, `previewWorkerBindings`), `writer.workerd.ts` (the
  integration stack's timings arrive only through the new binding). Live: `preview-1` deployed with
  `--dev-timings`, then by a bare `vp run …#deploy:preview-1` with no variables set → every Worker
  bound the complete production settings and `none`; `preview-3` `eu` → `none` → every Worker `none`.
- **Layout `packages/lsm-core`, `packages/lsm-cloudflare`, `packages/lsm-sharding`,
  `infra/shaledb`** — done. Enforced by `tools/workspace-boundaries` (D1, D2).

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
- **Seal (`DatabaseSealedError { successors }`)** — done. `DbWriter.seal` holds the commit lock
  across the final flush and the seal CAS; `DatabaseLifecycle.seal`; `POST /v1/databases/:id/seal`.
  Tests: `db-writer.test.ts`, `database-lifecycle.test.ts`, `writer.workerd.ts` (late write refused
  with `DatabaseSealed`). Formal: `SealSplit` (`NoCommitAfterSeal`; `sealWithoutLock`,
  `writerNotFenced`).
- **Zero-copy `forkRange` (SST views + external references)** — done. `forkManifest` (lsm-core),
  `DatabaseLifecycle.forkRange` (hold taken in a CAS that re-checks the caller's checkpoint;
  idempotent retry; wakes the fork's compactor), fork rewrite (`planCompactions` full compaction
  while external SSTs remain), release by the fork's own collection once no manifest GC keeps names another database's SST
  (`releasableExternalRefs` in `GarbageCollector.collect`, D77; D13 for forks of forks; the
  sweep asks a rewritten fork to collect). Tests: `database-lifecycle.test.ts` (split, rewrite,
  release `gcMinAge` later; a fork snapshot from before the rewrite readable until then; a fork of
  a fork's holds kept down the chain; a split half split again before its rewrite; nothing released
  while a kept manifest cannot be loaded; a release interrupted between its two steps finished by
  the next collection; shown failing without release or with duplicate holds, and
  with the release rule before D77), `garbage-collector.test.ts` (the sweep), `size-tiered-compaction-scheduler.test.ts`,
  `manifest.test.ts`, `writer.workerd.ts`. Formal: `SealSplit` (`EveryAckedWriteInExactlyOneSuccessor`),
  `GcSafety` (`PinnedManifestsComplete`; `ForkReadsComplete` in `GcSafetyFork.cfg`, broken
  variant `earlyForkRelease`). D37, D38, D77.
- **GC per database respecting fork checkpoints** — done. `garbage-collector.ts` lists only
  `db/<id>/`; pinned manifests and their SSTs are kept; forks release their holds once no kept manifest reads their source (D77);
  a prefix left without a manifest (an interrupted removal) is swept after `gcMinAge`
  (`garbage-collector.test.ts`). Formal: `GcSafety`.
- **`DatabaseLifecycle.create`/`remove` (`DatabaseHasDependentsError`, purge `db:<id>`)** — done.
  `markRemoved` (lsm-core: refuses while forks hold checkpoints; fences writers, checkpoints, forks),
  then writer and compactor state destroyed (`DbWriter.destroy`, `Compactor.destroy`), the fork's
  holds released, `db/<id>/` deleted (manifests last), and `db:<id>` purged through `BlockServer`.
  `DELETE /v1/databases/:id`. Tests: `manifest.test.ts`, `database-lifecycle.test.ts`,
  `writer.workerd.ts` (blocked by dependents, then removed). D39. An id is reusable only
  `gcMinAge` after its removal (`create` refuses a young `Removed` head; `ManifestCache.loadWith`
  drops an expired entry with a higher id): `database-lifecycle.test.ts` (recreate, leftovers,
  reads after removal). D90. The `Removed` head stays as the tombstone for `gcMinAge`
  (`markRemoved` records `removedAt`, `markRemovalCleared`; compaction, seal, and collection are
  no-ops on it; the sweep finishes a cleared one): `database-lifecycle.test.ts` (six committers
  in flight, an interrupted removal, a removal still deleting after a create, the tombstone's
  shape), `ManifestCasFencingRemove.cfg` (`NoResurrection`; broken `removalDeletesHead`). D91.
  Manifest edits as a state machine: `manifest-state-machine.test.ts`. D91.
- **`CompactionBudget` by `budgetGroup`** — done. lsm-core `compaction-budget-ledger.ts` (fair share
  among holders and waiting groups, expiring leases; property test against capacity and share,
  shown failing on four mutations), `compaction-budget.ts` (unlimited, ledger, test),
  `compaction-budget-server.ts` (global `CompactionBudgetServer` Durable Object and client).
  `compactor.test.ts`: a second database waits for the first job's lease (D35).
- **Per-database budget (~1k req/s, single-threaded CPU, 10 GB SQLite, 128 MB)** — done. Reads run
  in the caller's isolate (`db-reader.ts`), `AtLeast`/`Flushed` skip the writer, batches raise rows per
  request; `l0SstSize`/`maxL0Age` flush triggers; `maxUnflushedBytes` backpressure far below 10 GB;
  the memtable lives in SQLite and flushes/compactions stream (no writer block cache). Measured on
  `preview-1` (`docs/measurements.md`): a writer is CPU-bound at ~12–15k small rows/s with 1,000-row
  batches; a flush costs ~55–90 ms CPU per MiB; a split pauses writes ~13–16 s. Not measured: SQLite
  page cache against 128 MB, the Workflows concurrency limit.
- **Shared resources: R2 request rate, Workers Cache per Worker, L1 cache budget** — done.
  One bucket with `db/<id>/` prefixes and no LIST on read paths; owner database in every block URL
  (`block-server-http-api.ts`); L1 cache bounded by bytes per `budgetGroup` and in total
  (`group-byte-budget-lru.ts`, `block-cache.ts`; D51). Tests: `group-byte-budget-lru.test.ts`
  (properties: budgets and accounting, LRU within a group, another group evicted only under total
  pressure and only while heaviest; four mutations caught), `block-cache.test.ts` (joined misses,
  failures not kept, a busy group never evicts a quiet one, a lone group held to its share).
- **Corrupted SST in compaction (known gap, hand-off item 6)** — done (D53). `compaction-job.ts`
  (`Corrupted` outcome), `compaction-launcher.ts` / `compaction-workflow.ts` (`progress` with the
  outcome), `compactor.ts` (`corruptSsts`, metric, blocked sources),
  `size-tiered-compaction-scheduler.ts` (`planCompactionsAround`). Tests: `compactor.test.ts` (one
  corrupted byte → typed outcome, reported, never planned again; two mutations caught),
  `size-tiered-compaction-scheduler.test.ts` (blocked run never planned).
- **Long-lived snapshots (known gap, hand-off item 6)** — done (D52). `DatabaseLifecycle.pinSnapshot`,
  `POST /v1/databases/:id/snapshots/pinned` (`db-http-api.ts`); plain snapshots' `gcMinAge` limit and
  `SnapshotExpired` documented. Tests: `database-lifecycle.test.ts` (pinned readable past `gcMinAge`,
  unpinned expired, pinned collected after its lifetime), `writer.workerd.ts` (pin, read, release
  through the public API). Formal: `GcSafety` (`PinnedManifestsComplete`).
- **Transactions across a flush (v1 workstream 4)** — done, exact (D58, supersedes D41/D50). The
  memtable keeps flushed rows (`retained_from_seq`, `flush_marks`; `markFlushed`, `trim` with
  `transactionRetentionMs`/`maxRetainedBytes`); Latest paths filter above `flushed_seq`;
  `DbWriter.probeAt`, `scan` with `includeRetained`, `status.flushedManifestId`;
  `DbClient.beginTransaction` returns the flushed manifest and `getAt`/`scanAt` read from it; the
  reader checks a pinned manifest against the retained horizon; the public `transactions/begin`
  returns `manifestId`, which `get`/`scan` accept (additive). Tests: `db-transaction.test.ts`
  (property with flushes, compactions, and trims; exact across flushes and a compaction; too old only
  once trimmed, or at the first flush for callers without a manifest id), `mem-table.test.ts`
  (flushes keep rows; trims keep the newest flush), mutations caught. Formal: `Transactions`
  (`trimWithoutAbort`, `noReadHorizonCheck`), `NoAckedWriteLost` (`Trim`, `trimUnflushed`). Live
  (`preview-1`, production settings): 24/24 transactions held ~21.7 s across 6–7 flushes committed,
  0 `SnapshotTooOld`.
- **Data residency** — done, per Stack (D49). `lsm-jurisdiction.ts` (`lsmJurisdiction`,
  `inLsmJurisdiction`, `lsmJurisdictionAtDeclaration`), applied in `lsm-bucket.ts`,
  `trace-collector-worker.ts` (buckets), `writer-client.ts`, `compactor-client.ts`,
  `compaction-budget-server.ts`, `shard-directory-server.ts`, the probe (Durable Objects),
  `compaction-workflow.ts` (Workflow), and `block-server-worker.ts` (Workers Cache off when
  restricted); `preview-cli deploy --jurisdiction`. The per-database option is removed; old manifests
  decode. Tests: `lsm-jurisdiction.test.ts`, `manifest.test.ts`, `preview-stage.test.ts`. Live:
  `preview-2` deployed with `eu` — Cloudflare reports both buckets and the Workflow in `eu`, all 11
  scenarios pass, and `BlockServer` responses carry no `Cf-Cache-Status`.
- **Removal (fails while forks depend; purges in batches)** — done. `DatabaseLifecycle.remove`
  (`DatabaseHasDependentsError`; one tag purge per database through `BlockServer`).
- **GC and maintenance that scale with the number of databases (v1 workstream 5)** — done (D60).
  `Compactor` runs its database's collection from its alarm (`collectionDueAt`,
  `Compactor.collect`/`requestCollection`, one at a time); lsm-core `nextCollectionTime`;
  `MaintenanceSweep.sweepPage` (paged, cursor at `maintenance/sweep.cursor`; leftovers, fork wakes,
  piled-up manifests); the GC Worker's cron runs a page and "collect now" goes through the
  compactor; `ObjectStore.listPrefixes(prefix, after, limit)`. Tests: `garbage-collector.test.ts`
  (17 databases in pages of 5, each visited once per cycle), `compactor.test.ts` (due after a
  compaction, run by the alarm, idle with no alarm), `writer.workerd.ts` (collect-now through the
  compactor), the `gc` scenario (the compactor's alarm collected 4 SSTs). Formal: `GcSafety` (one
  collection per database, now enforced by the compactor). Live (`preview-1`, dev timings): 500
  databases all collected by their compactors; no invocation exceeded a limit.
- **Writer throughput and per-invocation limits (v1 workstream 6)** — done (D61). `DbWriter`'s
  in-memory `oldestUnflushed` (no memtable scan per write); `MEMTABLE_FLUSH_PAGE_QUERIES` (`+seq`
  bounds, cursor resume) in `mem-table.ts`; lsm-core `groupBlockReads` and
  `COMPACTION_READ_CHUNK_BYTES` in `compaction-executor.ts`; `ShaledbStackConfig.limits` bound as
  `limits.cpu_ms` (`shaledbApiCpuLimit`, `shaledbCompactorCpuLimit`); status `l0Count` from the
  newest manifest; `preview-cli deploy --settings`; `tools/writer-bench`; `scenario-cli measure
  writes|compaction-cpu|writer-memory|workflow-burst`. Tests: `mem-table.test.ts` (flush query plans
  use the primary key and no temp B-tree), `sst-codec.test.ts` (`groupBlockReads` property),
  `shaledb-stack-config.test.ts` (limits and refusals), `preview-stage.test.ts` (overrides over
  dev timings). Live: `docs/measurements.md` "Writer throughput and limits".
- **Corruption: prevent, detect, repair (v1 workstream 7)** — done (D62). Upload integrity in
  `object-store.ts` (`sha256` on single-part puts; `uploadPartVerified` with native WebCrypto MD5,
  falling back to lsm-core `md5Hex`; `object.part_retried` and `object.parts_unverified` on
  `ObjectStore.putMultipart`, D82; `objectStoreWithTransitCorruptionTestLayer`). Repair:
  lsm-core `dropSstFromManifest`, `DroppedSst`, `Manifest.droppedSsts`, `SstNotInManifestError`;
  `Compactor.dropSst` under the planning lock (`SstDropRefusedError`), `CompactorClient.dropSst`,
  `DatabaseLifecycle.dropSst` (confirmation); public `POST /v1/databases/:id/ssts/:sstId/drop`
  (`SstNotFound`, `SstDropRefused`), `GET /v1/databases/:id/checkpoints`, and compaction status
  `corruptSsts`, `l0SstIds`, `droppedSsts`. Runbook: `docs/operations.md`. The probe Worker's
  `POST /v1/probe/damage` exists on local and preview stages only (`deploysTelemetryProbe`). Tests:
  `object-store.test.ts`, `md5.test.ts`, `manifest.test.ts`, `compactor.test.ts`,
  `writer.workerd.ts` (multipart flush on local R2), the `repair` scenario. Formal: `GcSafety`
  `DropSst` with broken variant `dropDeletesObject`.
- **Production telemetry (v1 workstream 8)** — done (D63).
  - **Policy.** `lsm-trace-policy.ts` holds `withLsmTracePolicy`, `lsmPolicyTracer`,
    `isExportedSpanAttribute`, and `traceIdSampled`. It is applied to every Worker's telemetry:
    the API, compactor, BlockServer, GC, shard router, and probe.
  - **Sample rate.** `LsmWorkerSettings.traceSampleRate` is bound as
    `SHALEDB_TRACE_SAMPLE_RATE` and read back by `lsmTraceSampleRate`.
  - **Retention.** The TraceBucket expires `traces/` and `tails/` (`shaledbTraceRetentionDays`).
    The LsmBucket aborts stale multipart uploads.
  - **Observability tooling.** `manifest.l0_ssts` on `Compactor.poll`;
    `scenario-cli measure health|sampling`; `trace-cli pull --since-minutes`;
    `preview-cli deploy --trace-sample-rate`. The "What to watch" section is in
    `docs/operations.md`.
  - **Tests.** `lsm-trace-policy.test.ts`, `lsm-worker-settings.test.ts`,
    `preview-stage.test.ts`, and the `redaction` scenario.
- **Every watched signal readable on a live stage (D82)** — done. Span attributes
  `writer.stall_reason` (`db-writer.ts`), `object.part_retried` (`object-store.ts`), and
  `database.id` on the fenced `Compactor.poll` (`compactor.ts`); the health judgement
  `summarizeHealth` (`tools/scenario-cli/src/health-report.ts`), called by `measureHealth`.
  Metrics are emitted and not collected (`docs/operations.md`, "What to watch"). Tests:
  `health-report.test.ts`; the span attributes in `db-writer.test.ts`, `object-store.test.ts`,
  `compactor.test.ts`; the exported keys in `lsm-trace-policy.test.ts`.
- **Platform limits (D92)** — partial. The settings Cloudflare's limits cannot hold are refused
  (`LSM_PLATFORM_LIMITS` and the combination rules in `lsm-core/src/lsm-settings.ts`). Tests:
  `lsm-settings.test.ts` (each rule at its edge), `shaledb-stack-config.test.ts` (the bound
  settings fit a Worker variable's 5 KB). Open, with failing tests in the audit's directory:
  the compaction step result size, a step over dropped rows, a `Latest` scan's and every page's
  memory, and the garbage collection's reads per run. The table of limits is D92;
  `docs/operations.md` "Platform limits" is the operator's summary.
- **Bounds a caller can grow (D99)** — done. lsm-core: `MAX_CHECKPOINTS_PER_DATABASE`,
  `addCallerCheckpoint`, `CheckpointLimitError`; `MAX_CHANGELOG_ENTRIES`, `capChangelog` (in
  `commitL0Flush`); `MAX_MERGED_VALUE_BYTES`, `MergedValueTooLargeError` (`resolveVisibleRow`;
  `retainKeyVersions` leaves oversized runs unfolded). lsm-cloudflare: `CheckpointLimitReached` and
  `MergedValueTooLarge` public errors; `MAX_CHANGELOG_RETENTION_MS`, `MAX_CHECKPOINT_NAME_LENGTH`
  and the budget group's length in `db-http-api.ts`. infra: the compactor's `limits.subrequests`
  (`DEFAULT_COMPACTOR_SUBREQUESTS`, `SHALEDB_COMPACTOR_SUBREQUESTS`). Tests: `manifest-size.test.ts`
  (the budget), `manifest-limits.test.ts`, `merge-limits.test.ts`, `db-public-error-limits.test.ts`,
  `shaledb-stack-config.test.ts`.
- **No key, value, or secret in telemetry or an error (D123)** — done. `lsmPolicyTracer` filters
  span events as well as attributes (`exportedEventName`, `isExportedEventAttribute`). Tests:
  `telemetry-leaks.test.ts` (random markers through the policy and through public error bodies;
  schema issues of bytes fields), `lsm-trace-policy.test.ts`.
- **Transactions after GC (D76)** — done. `DbReader.manifestFor` reads the newest manifest when it
  fits the transaction's bounds, else the begin manifest; `DbClient.snapshot` loads the newest.
  Tests: `db-transaction.test.ts`, `db-client.test.ts`. Formal: `Transactions.tla` `ReadsAtBegin`,
  `noReadUpperBound`.
- **Review of D76 (D83)** — done. Transaction reads over any cached manifest that fits; concurrent
  collections; a collection without a retained manifest above GC's boundary deletes nothing.
  Tests: `db-transaction.test.ts`, `garbage-collector.test.ts`, `database-lifecycle.test.ts`,
  `db-client.test.ts`. Formal: `Transactions.tla` `Read` over any fitting manifest.
- **A seal with other successors (D95)** — done. `DbWriter.seal` refuses successors other than the
  recorded ones. Tests: `db-writer.test.ts`.
- **The first production deploy (D104)** — done, read and never run. The Stack at stage
  `production` and its tokens and smoke test. Tests: `shaledb-stack-config.test.ts`,
  `production-durable-objects.test.ts`.
- **Rollback against the stored formats (D105)** — done. Older code's view of today's records.
  Tests: `manifest-format-fixtures.test.ts` (lsm-core), `stored-format-fixtures.test.ts`
  (lsm-cloudflare).
- **The soak's workload (D106)** — done. `tools/scenario-cli/src/soak-checks.ts` (`ChangeLedger`,
  `FeedFollower`, `MergeKeyModel`, and the other models); `docs/testing.md`, "The soak's
  workload". Tests: `soak-checks.test.ts`.
- **Production readiness (v1 workstream 9)** — done (D64).
  - **Tokens.** `lsmAccessGateFrom`, `lsmAccessGateOver`, `LsmAccessTokenSource`, and
    `lsmStageAccessTokens` (`lsm-access-gate.ts`). In infra: `shaledbAccessTokens`
    (Secrets Store slots by `Secret.lookup`, plus the state token on previews), and
    `access-token-store.ts` with `access-tokens-cli.ts` (the `tokens` task: `ensure`, `check`,
    `rotate`).
  - **Production tasks.** `plan:production` and `deploy:production`.
  - **Contract.** `db-openapi.ts`, `generate-db-openapi.ts`, `docs/openapi/shaledb-v1.json`
    (`vp run generate:openapi`), and `docs/api-compatibility.md`.
  - **Format versions.** `MANIFEST_FORMAT_VERSION` with the `formatVersion` field, the check in
    `decodeStoredManifest`, and `COMPACTOR_STATE_FORMAT_VERSION`.
  - **Documentation.** `README.md` and `docs/operations.md`.
  - **Tests.** `lsm-access-gate.test.ts` (rotation, refresh, closed when unreadable),
    `shaledb-stack-config.test.ts` (production's configuration), and
    `shaledb-access-tokens.test.ts`. Also `db-openapi.test.ts` (checked in as generated,
    examples, scheme, headers), `manifest.test.ts` (the workstream 1 fixture),
    `manifest-store.test.ts` (newer format refused), and `compactor.test.ts` (newer state idle).
  - **Live.** Rotation on `preview-1`.
- **A flush writes only the versions a read can reach** — done (D70). `keepReachableVersions`
  (lsm-core `row-resolution.ts`), applied in `DbWriter.flushThen`. Tests:
  `row-resolution.test.ts` (against the oracle for any window and any snapshot from its newest
  row on), `db-writer.test.ts` (one row for a key written twice), the client-versus-oracle
  property over histories of 20 to 60 steps. Formal: `NoAckedWriteLost` (`FlushUpload` keeps the
  newest version).
- **The flush path** — done (D73). The compactor wake is `BackgroundWork` (`DbWriter.wakeCompactor`;
  `db-writer.test.ts`: a compactor that never answers holds up no flush). `SstStore.write` encodes
  blocks a chunk of rows at a time. `tools/writer-bench/src/flush-bench.ts` times a flush in
  process. Declined with measurements: concurrent multipart part uploads.
- **One figure for unflushed bytes; no memtable scan per alarm, status, or transaction begin** —
  done (D74). `memTableWriteBytes` (`mem-table.ts`); `DbWriter`'s `unflushedBytes`,
  `unflushedRows`, and `oldestUnflushed` counters, read by `scheduleFlush`, `runAlarm`, and
  `status`. Tests: `db-writer.test.ts` (counters equal a scan; the alarm near the threshold; a
  recording SQLite storage shows no aggregate statement while idle).
- **Tail records sampled like traces** — done (D71). `keepsInvocationRecord`, `tailSampleRateOf`
  (`infra/shaledb/src/cpu-tail-worker.ts`), `TAIL_SAMPLE_RATE` bound by the Stack;
  `invocationsRepresentedBy` in the measurement tools. Test: `cpu-tail-worker.test.ts`.
- **A request the endpoint's schemas refuse is `InvalidRequest`** — done (D69).
  `db-http-request.ts`; `writer.workerd.ts` (five malformed requests); `db-http-request.test.ts`
  (in-process: malformed, refused, and empty bodies, an unaccepted content type, a refused path
  id, an unknown route, a wrong method).
- **The sharded API's refusals in the public envelope** — done (D79). The shared
  `publicHttpRequestMiddlewareLayer` applies to `shardedHttpRouterLayer`. A database failure's
  message is the public one (`toPublicError`). Test: `shard-router-worker.test.ts`. The `401`
  names the token to send: `lsm-access-gate.test.ts`. The GC operator API shares the middleware and
  answers failures with its own messages (`gc-worker.ts`); test: `gc-worker.test.ts`.
- **Soak and production smoke test (v1 workstream 10)** — done (D65). `scenario-cli soak`
  (`tools/scenario-cli/src/soak.ts`: writers, readers at every consistency, scanners, transactors,
  snapshotters, a remover, a sharded group split twice, a midway redeploy; a model of every
  acknowledged write; final reads of every key; storage, tail-record, health, and span checks) and
  `scenario-cli smoke` (`smoke.ts`: only operations safe on a live installation, on databases it
  removes). Live: `docs/measurements.md` ("Release soaks"). Telemetry reads are bounded and stop
  with the workload, and `--report-file` keeps the report through each phase (D65):
  `soak-telemetry.test.ts`, `soak-report.test.ts`, `scenario.test.ts`.
- **Routed reads see writes across a split (stale maps)** — done (D59). `DbWriter.probe`/`scan`
  report `sealedSuccessors`; `ReadOptions.refuseSealed` (`db-client.ts`); `ShardRouter` gets route
  through `withRouting` and scans resume shard by shard (`scanFrom`). Test: `sharding.test.ts` (a
  second router with a stale map reads the new key by get and scan), mutation caught. Live:
  `fork-split` 5/5 on `preview-1`.
- **Splits pause briefly and never fail (v1 workstream 3)** — done (D57). `DbWriter.seal` with
  `holdForSuccessors` (`flushThen`: final flush, seal, and lsm-core `holdForSuccessors` in one
  CAS); `DatabaseLifecycle.forkRange` reuses a hold already taken and warms the fork's writer and
  compactor in the background; `ShardSplit` seals with holds, forks concurrently, publishes, and
  reports failures (`reportSplitFailed`); `ShardDirectory.awaitMap` (long poll on the group's
  Durable Object) and `ShardRouter.withRouting`. Tests: `sharding.test.ts` (every `awaitMap`
  outcome, a held write routed within 2 s of the publish, a failed split failing held writes at once,
  a resumed split making one commit), `db-writer.test.ts` (one commit seals, flushes, and holds),
  `manifest.test.ts` (`holdForSuccessors`); mutations caught. Formal: `SealSplit` (the seal takes the
  holds). Live (`preview-1`, production settings): pause median 2.7 s (4 writers) and 3.1 s (16),
  0 failed writes; 11/11 scenarios.
- **Sharding: `ShardDirectory`, `ShardRouter`, `ShardSplit`** — done. `packages/lsm-sharding`:
  `shard-map.ts` (range map, pending split, pure edits), `shard-directory.ts` +
  `shard-directory-server.ts` (one Durable Object per group), `shard-router.ts` (cached map; refresh
  and retry on `DatabaseSealed`, `KeyOutOfRange`, or a removed database; cross-shard batches
  refused; scans concatenate shards in key order), `shard-split.ts` (begin in the directory, seal,
  checkpoint, fork ×2, delete the checkpoint, publish; resumable), `shard-router-worker.ts` (example
  Worker, in the Stack). Public APIs only: `dbServicesLayer` / `dbServicesTestLayer` from
  lsm-cloudflare; `workspace-boundaries.test.ts` enforces it. Tests: `shard-map.test.ts` (cover and
  routing property, one split at a time), `sharding.test.ts` (Map oracle through the router across
  random splits, also read straight from each shard's database and its key range; stale router;
  cross-shard batch; resumed split; shown failing without the seal, without the map refresh, and with
  overlapping fork ranges). Trace (`vp run dev`): one split crosses the shard router and API Workers,
  two forks, one publish, no errors. Formal: `SealSplit`. D40.

## Brief requirements

- **Trace CLI (`serve`, `list`, `show`, `tail`, `grep`, `stats`, `diff`, `assert`)** — done.
  `tools/trace-cli`; tests export real spans through `OtlpTracer` (JSON and protobuf).
- **OTLP from every Worker and DO, including alarms, flushed before the event settles** — done for
  the probe and smoke Workers; every new handler must use `eventTelemetryFlushForStage` (D5).
  Tests: `lsm-cloudflare/src/lsm-telemetry.test.ts`, `test/telemetry/*.workerd.ts`.
- **Scenario CLI (write/read, flush, compaction, fork/split, fencing, changelog)** — done.
  `tools/scenario-cli` (`list`, `run [names|all]`): `write-read`, `flush`, `compaction`, `gc`,
  `fork-split`, `fencing`, `changelog`, `transactions`, `remove`, each checking results through the
  typed public APIs (`DbHttpApi`, `GcHttpApi`, `ShardedHttpApi`) and its traces through the trace
  CLI's `assert` (a selection matching no trace fails). All nine pass against `vp run dev` (dev
  stage: `gcMinAge` 10 s, compactor polls every second).
- **Formal checks 1–6** — done. Check 1 (no acknowledged write lost or hidden): `NoAckedWriteLost.tla`,
  skill `.agents/skills/verify-no-acked-write-lost`. Check 2 (fencing) and 3 (manifest CAS
  linearizability): `ManifestCasFencing.tla`, skill `.agents/skills/verify-manifest-cas-fencing`.
  Check 4 (GC safety): `GcSafety.tla`, skill `.agents/skills/verify-gc-safety`. Check 5 (seal and
  split): `SealSplit.tla`, skill `.agents/skills/verify-seal-split`. Check 6 (transactions):
  `Transactions.tla`, skill `.agents/skills/verify-transactions`. All run by `vp run verify`, each
  broken variant shown failing.
- **Hardening scenarios** — done. Writer eviction mid-flush and a crash between the manifest commit
  and the truncate: `db-writer.test.ts`; a crash between the L0 upload and the manifest commit
  (orphan left, nothing lost, GC removes it): `fault-injection.test.ts`; CAS races:
  `manifest-store.test.ts` and `ManifestCasFencing`; a stale writer after fencing:
  `db-writer.test.ts` and `ManifestCasFencing`; R2 failures (outages and flaky calls never lose or
  hide an acknowledged write; shown failing with truncate-before-commit):
  `fault-injection.test.ts`; a compaction resumed after a failed step (finished steps replayed, one
  commit): `fault-injection.test.ts`; a split under load (a writer through the router during the
  split loses nothing; shown failing without the seal): `sharding.test.ts`. End to end:
  `writer.workerd.ts` and the scenario CLI.

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

## Phase 7 additions

- **Public lifecycle endpoints** — done. `POST /v1/databases/:id/checkpoints`,
  `DELETE /v1/databases/:id/checkpoints/:checkpointId`, `POST /v1/databases/:id/seal`,
  `POST /v1/databases/:id/forks`, `DELETE /v1/databases/:id` (`db-http-api.ts`); errors
  `CheckpointNotFound` (404) and `DatabaseHasDependents` (409). `writer.workerd.ts`.
- **`ManifestStore.update` passes the manifest id to the edit** — done (a checkpoint pins exactly
  the manifest its CAS applied to).

## Phase 8 additions

- **`dbServicesLayer` / `dbServicesTestLayer`** — done. `db-services.ts`: `DbClient` and
  `DatabaseLifecycle` for any Worker in the Stack (cross-script `WriterServer` and `CompactorServer`,
  `BlockServer`, bucket), and in memory for consumers' tests. `makeWriterClientOver` builds the writer
  client over any `WriterServer` namespace.
- **Request limits and retries (D88)** — done. `MAX_TTL_MILLIS` / `TtlMillisSchema` (lsm-core
  `write-batch.ts`) on `ttlMillis` and checkpoint `lifetimeMs`; retry messages in
  `db-public-error-translation.ts`. Tests: `db-http-limits.test.ts` (TTL and lifetime bounds,
  largest key, value, and batch, empty key, reversed range), `db-http-retries.test.ts`.
- **Stale shard maps on reads that never ask the writer (D89)** — done.
  `SHARD_MAP_MAX_AGE_FOR_UNPROBED_READS_MS` in `shard-router.ts`. Test:
  `shard-router-staleness.test.ts`.
- **Bounded requests (D100)** — done. `MAX_REQUEST_BODY_BYTES` and the bounded read in
  `db-http-request.ts`; the `Request*` schemas in lsm-core; `request-schema-bounds.ts`. Tests:
  `db-http-request.test.ts`, `request-schema-bounds.test.ts`, `db-http-boundary.property.test.ts`,
  `shard-router-worker.test.ts`, `block-server.test.ts`.
- **Abandoned splits finished by a waiting request (D101)** — done. `ShardSplit.resumePending`,
  `withAbandonedSplitFinished`. Tests: `shard-split-faults.test.ts` (each step killed, concurrent
  splits, a two-generation stale router), `health-report.test.ts`.
- **The compactor Durable Object, modeled (D110)**: done. `CompactorObject.tla`: two wakes, the
  alarm's poll, Workflow jobs with retried commits, a one-slot lease, GC, and eviction. Properties:
  `EpochNeverRegresses`, `OutcomeTrue`, `LeaseCapacity`, `L0Drains`, `RunningJobsLeave`. The
  variants for D78, D94, D96 and D103 each fail.
- **A shard split end to end, modeled (D112)**: done. `ShardSplit.tla`: crashes after any step and
  resumption, stale routers, Flushed reads, a rogue seal, and the change feed across the split.
  Properties: `EveryShardWriteInItsSuccessor`, `HoldsMatchSeal`, `ConsumerMissesNothing`,
  `NoStaleBeyondBound`, `WaitingWritesLand`. The variants for D80, D89, D95 and D101 each fail.
- **The lifecycle across databases, modeled (D114)**: done. `Lifecycle.tla`: a source, a fork, and
  a fork of the fork; holds, GC, deleteCheckpoint, removal interrupted and resumed, stale
  committers, and recreation. Properties: `NoReaderNeedsDeleted`, `RemovedNeverReopens`,
  `NoOldIncarnationServed`, `HoldsReleased`, `PrefixEmptied`. The variants for D77 (both forms),
  D90 and D91 (both forms) each fail.
- **Mutation sweep of `lsm-core` (D117)** — done. 800 mutants over the 15 core modules; every
  survivor classified, the untested edges given tests in `write-batch`, `row-resolution`,
  `merge-limits`, `garbage-collection-plan`, `size-tiered-compaction-scheduler`, `key-range`,
  `bloom-filter`, `lsm-settings`, `manifest`, `sst-codec`, `byte-codec`, `merge-iterator`, and
  `ulid` tests. Of the 82 that survive, 80 are equivalent and 2 change only how many rows a block seek decodes (`/tmp/shale-work/agents/mut-core/REPORT.md`).
- **Bounded scan pages on every surface (D125)** — done. `ScanPageBudget`, `DbClient.scanAtPage`,
  `settledVersions`, and the routed page's shared budget (`shard-router.ts`). Tests:
  `scan-bounds.test.ts` (tiers, overwritten keys, merge runs, cross-page transactions, phantoms),
  `shard-scan-bounds.test.ts`, `scan-oracle.property.test.ts`, `shard-scan-oracle.property.test.ts`;
  workerd: `writer.workerd.ts` (budget pages over deletes, regular and transaction).
  Open: no workerd test hosts the shard router, so the sharded scan's HTTP response is covered
  only by the schema.
- **The writer Durable Object at await granularity (D109)** — done. `WriterObject.tla` (PlusCal),
  skill `.agents/skills/verify-writer-object`: `db-writer.ts` entry points interleaved with
  eviction, interruption, and R2 failures. Invariants `NoAckedWriteLost`, `LatestSeesAcked`,
  `Serializable`, `NoAckAfterSeal`, `SealNotHalf`, `NeverSelfFenced`, `CountersCover`,
  `AlarmCovers`; liveness `AckedFlushed`, `AlarmQuiets`. Broken variants for D74b (both halves),
  D84, D85, D102, D74's loop, and D115 each fail their property.
- **A refused flush fences only its own phase (D115)** — done. `markFenced` and the flush's view
  install in `db-writer.ts`; the seal's `commitAttempted`. Tests: `db-writer.test.ts` ("DbWriter
  seal interrupted while a flush is in flight (D115)", "DbWriter flush fenced by its own writer's
  restart (D115)"). Formal: `WriterObject` (`NeverSelfFenced`; `beforeD115`,
  `markFencedAnyPhase`).
  `scan-bounds.test.ts` (tiers, overwritten keys, merge runs, cross-page transactions, phantoms),
  `shard-scan-bounds.test.ts`, `scan-oracle.property.test.ts`, `shard-scan-oracle.property.test.ts`;
  workerd: `writer.workerd.ts` (budget pages over deletes, regular and transaction).
- **A flush is exact about where it starts; the feed reads each change once (D113)** — done.
  `commitL0Flush`'s `afterSeq` and `FlushedSeqStaleError` (lsm-core `manifest.ts`), `flushThen`'s
  second attempt (`db-writer.ts`), `changelogRows`' skip (`db-reader.ts`). Tests:
  `db-writer.test.ts` ("DbWriter changelog after a flush commit whose response was lost (D113)"),
  `change-feed.test.ts` ("change feed over a changelog entry an earlier one overlaps (D113)"),
  `manifest.test.ts` ("commitL0Flush's start (D113)"). Formal: `ChangeFeed.tla` (`ExactlyOnce`,
  `NoOverlappingEntries`; `beforeD113`, `lostResponseReexport`, `noExpiredCheck`,
  `appendAfterCut`), skill `.agents/skills/verify-change-feed`.
- **The plain Durable Object SQLite baseline (D75)** — done, live only. `PlainSqliteServer`
  (`infra/shaledb/src/telemetry-probe-worker.ts`, preview-only) and `measure baseline`
  (`tools/scenario-cli/src/measurements.ts`). No unit test. One provisional run on `preview-3` at
  `ed4e787` (`docs/measurements.md`, "Baseline"); the release-commit runs are pending.
- **Liveness of the writer and transaction models (D111)** — done. `TransactionsLive.cfg`
  (`EveryTxnCommits`; `beforeD76` fails) and `NoAckedWriteLostLive.cfg` (`EventuallyFlushed`;
  `noWake` fails), with `WriterObject`'s `AckedFlushed` and `AlarmQuiets`. `Transactions.cfg`
  with the code as written is still to be observed in full under D111 (`docs/progress.md`).
- **The change feed repeats a change after a lost flush response (D113)** — open, not fixed.
  Recorded by `db-writer.test.ts` ("DbWriter changelog after a flush commit whose response was
  lost (D113)"), an expected failure. No model yet.
- **A fork's hold during its creation; a job recorded before its launch (D116)** — done; no decision
  entry yet. `DatabaseLifecycle.deleteCheckpoint` keeps a hold whose fork has no manifest until the
  hold is `gcMinAge` old (`ccd48db`). The compactor's poll records a job before `launcher.start`
  (`f83cd56`). Tests: `database-lifecycle.test.ts` ("a fork's hold deleted while the fork is being
  created (D116)"); `compactor.test.ts` ("Compactor jobs launched by a poll that died (D116)",
  "Compactor launches whose reply was lost (D116)", and the `stateLock` and `planLock` tests).
  Formal: `Lifecycle.cfg` with `beforeD116` fails `NoReaderNeedsDeleted`.
- **A reused id's chain continues past its tombstone (D119)** — done. `create` over a cleared
  tombstone starts past its `lastL0Seq` and both epochs; a snapshot read needs its manifest's
  `lastL0Seq`. Tests: `database-lifecycle.test.ts` ("reads as expired, not as the new database
  (D119)", "is fenced, not committed into the new chain (D119)", "DatabaseLifecycle: gaps the
  mutation sweep found (D119)"), and `compactor.test.ts` (a missing Workflow instance, D103 and
  D119). Mutations: 3 of 4 caught; the writer-epoch carry is equivalent.
- **Token secrets that never turn active (D121)** — done. `awaitSecretActive`
  (`infra/shaledb/src/access-token-store.ts`). Test: `shaledb-access-tokens.test.ts` ("a written
  secret that never turns active (D121)"), with `TestClock`; 3 mutations caught.
- **Mutation sweeps of the writer, the compactor, and the memtable (D122, D127)** — partial.
  Tests: `db-writer.test.ts` ("DbWriter commit lock (D122)"); `mem-table.test.ts` ("the memtable's
  stored row kinds (D122)", the trim's bounds and the seed, D127); `db-writer-start.test.ts` (a
  restarted writer schedules its flush, start failures, a seal with the same count of successors,
  the locks: D127). D127 has no decision entry yet. Not reached: `docs/release-v1.md` §8.
- **Mutation sweep of the request edge and sharding (D128)** — done. 165 mutants; 6 survive. Tests:
  `db-public-error-translation.test.ts` (every internal error's public answer; the corrupted SST's
  `StoredDataInvalid` as an expected failure until `edfe57b` is merged),
  `db-http-request-middleware.test.ts`, `lsm-trace-policy-layer.test.ts`, and cases added to
  `db-http-request.test.ts`, `telemetry-leaks.test.ts`, and `lsm-trace-policy.test.ts`.
- **The checkpoint cap is set by what a collection reads (D129)** — done. `MAX_CHECKPOINTS_PER_DATABASE`
  = `GC_PINNED_MANIFEST_READ_BUDGET_BYTES` / `MANIFEST_SIZE_BUDGET_BYTES` = 128 (`lsm-core/src/manifest.ts`). Tests:
  `manifest.test.ts` ("caps checkpoints so the manifests they pin fit one collection's read budget
  (D129)"), `database-lifecycle.test.ts` ("the most checkpoints and pinned snapshots a database may
  hold (D99, D129)"); 2 mutations caught.
- **Mutation sweep of the writer, memtable, and compactor (D127)** — partial. `mem-table` swept
  with every survivor triaged; compactor stage 1 complete and stage 2 two-thirds done; the
  writer's named candidates (start-path scheduling, start-failure reasons, D95 successors,
  `initLock`, `flushLock`) tested. Tests: `db-transaction-ranges.test.ts` (scanned ranges),
  `db-writer-start.test.ts`, `mem-table.test.ts`, `compactor.test.ts`.
- **The models interleave inside the locks (D116)**: done. Each lock in `compactor.ts` and the
  sharding directory is a broken variant with a gated unit test, except `collectionLock`, which no
  safety property needs (`GcSafetyTwoCollectors.cfg`). New properties: `DroppedStaysDropped`,
  `RunningStaysRecorded`, `PublishedIsSealed`, `NewIncarnationIntact`. Two defects found and
  fixed: `deleteCheckpoint` during `forkRange` (`database-lifecycle.test.ts`), and a job launched
  before it was recorded (`compactor.test.ts`). Deeper search configs, unregistered, in each skill.
