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
- **Manifest CAS with R2 `If-None-Match: *`** — done locally; the real-R2 check waits for
  approval (open question 2). `object-store.ts` (`putIfAbsent`), `manifest-store.ts` (`create`,
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
- **Changelog export (CDC), `changes?afterSeq=`** — done. Write side: `exportChangelog` in
  `db-writer.ts` (a sequence-ordered `Changelog` SST per flush, `Manifest.changelog` entry); retention
  by GC (`prepareGarbageCollection` prunes entries past `retentionMs`; their SSTs are then
  unreferenced). Read side (`ChangelogExport.read`): `DbClient.changes` (flushed changes from the
  changelog SSTs via `DbReader.changelogRows`, then the writer's unflushed ones; pages never split
  a batch), `GET /v1/databases/:id/changes?afterSeq=&limit=` (`nextAfterSeq`), errors
  `ChangelogUnavailable` (`Disabled`, `Expired`). Tests: `db-writer.test.ts`, `change-feed.test.ts`
  (exactly-once, in-order delivery across random batches, flushes, and page sizes; shown failing
  when a page can cut a batch), D42.

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
- **`manifest/LATEST` hint + probe `id+1`; no LIST on read paths** — done.
  `manifest-reader.ts` (`loadLatest`; LIST only to recover when the hint names a collected
  manifest); GC rewrites the hint before deleting manifests. Tests: `manifest-store.test.ts` (lagging
  hint, collected hinted manifest). Trace: the `write-read` scenario and the Phase 5 dev check assert
  no `ObjectStore.list` on a `Flushed` read.
- **Compactor edit replaces exactly its inputs, else fails** — done. `commitCompaction`
  (`manifest.test.ts`: replace, idempotent retry, `CompactionInputsChangedError`,
  `CompactorFencedError`, `SstBelowGcBoundaryError`); `CompactionExecutor.prepare` re-checks the
  inputs (`compaction-executor.test.ts`). Formal: `ManifestCasFencing` (`noInputsCheck`).
- **Delete-after-commit; duplicate replay is deduplicated** — done. `flushNow` truncates only after
  the manifest commit; restart drops rows `<= lastL0Seq` (D22); `mergeRowStreams` drops exact
  duplicates. Tests: `db-writer.test.ts` (crash between commit and truncate), `merge-iterator.test.ts`.
  Formal: `NoAckedWriteLost` (`truncateBeforeCommit` fails).
- **`onlyIf: new Headers({ "If-None-Match": "*" })` (workerd #2572)** — done locally; real R2
  pending approval. `object-store.ts` `putIfAbsent` passes the Headers form (and `putIfVersion` the
  `If-Match` form); verified against local R2 in `object-store.workerd.ts`. The real-R2 run is open
  question 2.

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
  `size-tiered-compaction-scheduler.test.ts` over `Arbitrary` manifests and options: jobs are
  consecutive, all-L0 or all-runs, within min/max sources and `includeSizeThreshold`, never overlap
  each other or a job in flight, have the right destination and `isBottom`; repeatedly committing
  planned jobs converges (fallback consolidation terminates). Shown failing with the size
  threshold ignored and with in-flight sources not reserved (D15).
- **`DatabaseLifecycle`, `GarbageCollector`, `CompactionBudget`** — done. `database-lifecycle.ts`
  (`create`, `createCheckpoint`, `deleteCheckpoint`, `seal`, `forkRange`, `remove`),
  `garbage-collector.ts`, `compaction-budget.ts` + `compaction-budget-server.ts` (lsm-core
  `compaction-budget-ledger.ts`). Tests: `database-lifecycle.test.ts`, `garbage-collector.test.ts`,
  `compactor.test.ts`, `writer.workerd.ts`.

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
  internal error maps to a code, message, request id, and `retryable`. Tests:
  `writer-client.test.ts`, `writer.workerd.ts` (public errors over HTTP), each module's tests.
- **Merge operators and TTL during reads and compaction** — done. `lsm-core/src/row-resolution.ts`
  (D10, D11) and `compaction-merge.ts` (`compactRowStreams`). Tests: `row-resolution.test.ts`
  (oracle at any snapshot and time; earliest expiry; `U64Add`; `MergeOperatorMissingError`; raw
  operands without an operator), `compaction-merge.test.ts` (any split of inputs over three keys,
  with and without older data and an operator, matches the oracle; shown failing when deletes are
  dropped above the bottom, an input is skipped, or expired operands are kept),
  `compaction-executor.test.ts` (merges through real SSTs).
- **Checkpoints, clones (forks), transactions** — done. Checkpoints and forks (Phase 7, D37). Optimistic
  transactions (Phase 9, D41): lsm-core `transaction.ts` (`TransactionIsolation`,
  `TransactionCommit`, `TransactionConflictError`); `DbWriter.commitTransaction` (conflict check and
  commit under the commit lock; `MemTable.writtenAfter`); `DbClient.transaction` (interactive:
  buffered writes, reads of its own writes and merges, recorded read keys and ranges) and the
  stateless `beginTransaction`/`getAt`/`scanAt`/`commitTransaction`; public
  `/v1/databases/:id/transactions/{begin,get,scan,commit}`. Tests: `db-transaction.test.ts`
  (serializability-in-commit-order property for `SerializableSnapshot` and no-lost-update property
  for `Snapshot` over interleavings with direct writes and flushes; write skew; own writes;
  `SnapshotTooOld`; each shown failing on its mutation), `writer.workerd.ts` (write skew refused
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
  (`infra/cf-slatedb/alchemy.run.ts`); `ShardDirectory` joins in Phase 8.
- **`state: Cloudflare.state()` for live stages** — done. `cfSlatedbStateLayer`
  (`infra/cf-slatedb/src/cf-slatedb-stage.ts`): local state for `dev`/`test`/`local` stages,
  the account's Cloudflare state store (pinned account) for previews and production. Test:
  `infra/cf-slatedb/src/cf-slatedb-stage.test.ts`; `vp run dev` still runs offline. D43.
- **Preview deployment tool** — done. `tools/preview-cli` (`deploy`, `status`, `url`, `token`,
  `destroy-command`; `--json`; `--dev-timings`), tasks `deploy:preview-<n>`, `destroy:preview-<n>`
  (Dillon only), `state:read` (`infra/cf-slatedb/vite.config.ts`). Test:
  `tools/preview-cli/src/preview-stage.test.ts` (state parsing, outputs, token, destroy command).
  Live: `preview-1` deployed and redeployed in place (no-op), `status`/`url`/`token` read the
  Cloudflare state store. D45.
- **Traces from previews** — done. `infra/cf-slatedb/src/trace-collector-worker.ts`
  (`TraceCollectorWorker`, `TraceBucket`, `TraceCollectorToken`; live stages only),
  `cf-slatedb-telemetry.ts` (`cfSlatedbTelemetry`), `LsmWorkerTelemetry`/`lsmOtlpTelemetry`
  (`lsm-telemetry.ts`), `lsmWorkerCompatibility` (`lsm-stage.ts`), `tools/trace-cli/src/
  preview-trace-pull.ts` (`pull --stage`, `tail --stage`), `preview-cli collector`. Tests:
  `preview-trace-pull.test.ts` (JSON and protobuf exports across pages, repeated pulls download
  nothing twice, a refused token). Live (`preview-1`): API Worker → writer DO trees complete, DO
  alarm spans arrive, 10/10 probe traces complete, unauthenticated reads `401`. D46, D47.
- **Live checks and §12 measurements (hand-off item 4)** — done. Scenarios `block-cache`
  (Workers Cache `HIT` on `BlockServer` through a second Worker; `cache.l2.status` on `BlockSource`
  spans from `Cf-Cache-Status`, `block-source.ts`) and `create-race` (16 concurrent creates, one
  `Created`, 15 `AlreadyExists`), both also local; all 11 scenarios pass on `preview-1` and under
  `vp run dev`. `scenario-cli measure writes | flush-cpu | split-pause`
  (`tools/scenario-cli/src/measurements.ts`) with `CpuTailWorker` CPU records. Results:
  `docs/measurements.md`. D48.
- **Access gate on live stages' public Workers** — done. `lsm-access-gate.ts`
  (`LsmAccessToken`, `lsmAccessGateForStage`), applied by `db-api-worker.ts`, `gc-worker.ts`,
  `shard-router-worker.ts`, and the telemetry probe. Test: `lsm-access-gate.test.ts`. Live:
  `preview-1` answers `401` without the token and serves the API with it. D44.
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
- **Seal (`DatabaseSealedError { successors }`)** — done. `DbWriter.seal` holds the commit lock
  across the final flush and the seal CAS; `DatabaseLifecycle.seal`; `POST /v1/databases/:id/seal`.
  Tests: `db-writer.test.ts`, `database-lifecycle.test.ts`, `writer.workerd.ts` (late write refused
  with `DatabaseSealed`). Formal: `SealSplit` (`NoCommitAfterSeal`; `sealWithoutLock`,
  `writerNotFenced`).
- **Zero-copy `forkRange` (SST views + external references)** — done. `forkManifest` (lsm-core),
  `DatabaseLifecycle.forkRange` (hold taken in a CAS that re-checks the caller's checkpoint;
  idempotent retry; wakes the fork's compactor), fork rewrite (`planCompactions` full compaction
  while external SSTs remain), release (`releasableExternalRefs`, `Compactor.releaseExternalRefs`,
  D13 for forks of forks). Tests: `database-lifecycle.test.ts` (split, rewrite, release; shown
  failing without release or with duplicate holds), `size-tiered-compaction-scheduler.test.ts`,
  `manifest.test.ts`, `writer.workerd.ts`. Formal: `SealSplit` (`EveryAckedWriteInExactlyOneSuccessor`),
  `GcSafety` (`PinnedManifestsComplete`). D37, D38.
- **GC per database respecting fork checkpoints** — done. `garbage-collector.ts` lists only
  `db/<id>/`; pinned manifests and their SSTs are kept; forks release their holds once rewritten;
  a prefix left without a manifest (an interrupted removal) is swept after `gcMinAge`
  (`garbage-collector.test.ts`). Formal: `GcSafety`.
- **`DatabaseLifecycle.create`/`remove` (`DatabaseHasDependentsError`, purge `db:<id>`)** — done.
  `markRemoved` (lsm-core: refuses while forks hold checkpoints; fences writers, checkpoints, forks),
  then writer and compactor state destroyed (`DbWriter.destroy`, `Compactor.destroy`), the fork's
  holds released, `db/<id>/` deleted (manifests last), and `db:<id>` purged through `BlockServer`.
  `DELETE /v1/databases/:id`. Tests: `manifest.test.ts`, `database-lifecycle.test.ts`,
  `writer.workerd.ts` (blocked by dependents, then removed). D39.
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
- **Long-lived snapshots (known gap, hand-off item 6)** — done (D52). `DatabaseLifecycle.pinSnapshot`,
  `POST /v1/databases/:id/snapshots/pinned` (`db-http-api.ts`); plain snapshots' `gcMinAge` limit and
  `SnapshotExpired` documented. Tests: `database-lifecycle.test.ts` (pinned readable past `gcMinAge`,
  unpinned expired, pinned collected after its lifetime), `writer.workerd.ts` (pin, read, release
  through the public API). Formal: `GcSafety` (`PinnedManifestsComplete`).
- **Transactions across a flush (known gap, hand-off item 6)** — kept and justified (D50): only a
  transaction outlasting a whole flush aborts; measured 580/580 commits through a 28.7 s flush
  (`scenario-cli measure transactions`). Formal: `Transactions` (`Flush` interleaves anywhere).
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
