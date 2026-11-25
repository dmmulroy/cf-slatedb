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
- **Compaction in `CompactorServer` + Workflow** — pending (Phase 6).
- **Readers in any Worker; `Latest`/`AtLeast` through the writer** — pending (Phase 5).

## §2 System

- **Topology: API Worker, `WriterServer`, `BlockServer`, R2, `CompactorServer`, Workflow, GC cron** —
  pending. `infra/cf-slatedb/alchemy.run.ts` currently runs only the telemetry probe.

## §3 Write path

- **`POST /v1/databases/:id/batches`, `Semaphore(1)`, one SQLite transaction** — done.
  `writer-http-api.ts`, `db-http-api.ts`, `lsm-cloudflare/src/db-writer.ts` (`commitLock`), `MemTable.commit`.
  Tests: `db-writer.test.ts`, `writer-client.test.ts`, `writer.workerd.ts`.
- **Flush trigger: `l0SstSize`, `maxL0Age`, `setAlarm(now)`** — done. `scheduleFlush`/`runAlarm`
  in `lsm-cloudflare/src/db-writer.ts`, `writer-alarm.ts`. Tests: `db-writer.test.ts` (alarm at `maxL0Age`, `TestClock`),
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

- **`Flushed`, `AtLeast(seq)`, `Latest` (probe + `flushedSeq`)** — pending (Phase 5).
- **SST skip by range and Bloom filter; index search; block seek** — partial.
  `lsm-core/src/sst-codec.ts` (`sstMayContainKey`, `firstBlockForKey`, `blocksOverlappingRange`),
  `block-codec.ts` (`DataBlock.seek`), `row-resolution.ts` (`resolveVisibleRow`). Tests:
  `sst-codec.test.ts` reads every key at random snapshots and times exactly as a `Map` oracle does.
- **L1 `BlockCache` (separate data and metadata caches)** — pending (Phase 5).
- **L2 Workers Cache in front of `BlockServer`; block/meta/manifest URLs; immutable headers;
  `Cache-Tag`** — pending (Phase 5; live `Cf-Cache-Status: HIT` check needs approval).
- **`BlockServer`: separate Worker, `crossVersionCache`, `workersDev: false`, `fetch()` not RPC** —
  pending (Phase 5).
- **`SstHandle.metaOffset`/`metaLength`** — partial. `lsm-core/src/manifest.ts`
  (`sstHandleFromInfo`), `sst-codec.ts` (`SstInfo`); `sst-codec.test.ts` checks
  `metaLength = size − metaOffset`.

## §5 Manifest CAS and fencing

- **Writer init fencing loop** — done. `ensureReady` (`ManifestStore.update(fenceWriter)` with CAS
  retry, epoch stored in `writer_meta`, sequence oracle and clock seeded from the manifest) (D19).
- **`manifest/LATEST` hint + probe `id+1`; no LIST on read paths** — partial.
  `lsm-cloudflare/src/manifest-reader.ts` (`loadLatest`; LIST only to recover when the hint names a
  collected manifest). `manifest-store.test.ts` covers a lagging hint and a collected hinted
  manifest. Trace assertion "no `ObjectStore.list` on a read path": Phase 5 scenarios.
- **Compactor edit replaces exactly its inputs, else fails** — partial. `commitCompaction`
  (`manifest.test.ts`: replace, idempotent retry, `CompactionInputsChangedError`,
  `CompactorFencedError`).
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
- **`SstStore`, `BlockCache`, `BlockSource`, `BlockServer`, `DbReader`, `DbClient`** — pending
  (Phase 5).
- **`MemTable`, `SeqOracle`, `DbWriter`, `ChangelogExport`** — done. `mem-table.ts`,
  `db-writer.ts` (sequence oracle and clock in the commit transaction, D20; `ChangelogExport` is
  `exportChangelog`), `writer-client.ts`, `writer-server.ts`, `writer-http-api.ts`.
- **`CompactionScheduler` (pure)** — done. `lsm-core/src/size-tiered-compaction-scheduler.ts`;
  `size-tiered-compaction-scheduler.test.ts` over `Arbitrary` manifests and options: jobs are
  consecutive, all-L0 or all-runs, within min/max sources and `includeSizeThreshold`, never overlap
  each other or a job in flight, have the right destination and `isBottom`; repeatedly committing
  planned jobs converges (fallback consolidation terminates). Shown failing with the size
  threshold ignored and with in-flight sources not reserved (D15).
- **`DatabaseLifecycle`, `GarbageCollector`, `CompactionBudget`** — pending (Phases 6–7).

## §8 Public API and semantics

- **`put`, `delete`, `merge`, `write`, `get`, `getRow`, `scan`, `flush`** — partial. Write side:
  `DbHttpApi` (`write`, `flush`) and `WriterClient`; reads in Phase 5 (`DbClient`).
- **Errors** — partial (public envelope done: `db-public-error.ts`, `db-public-error-translation.ts`, D25; `WriteStalledError`, `WriterUnavailableError`, `DatabaseNotFoundError` in `lsm-errors.ts`). Core: `SstCorruptedError`, `MergeOperatorMissingError`
  (`lsm-core-errors.ts`), `WriterFencedError`, `CompactorFencedError`, `DatabaseSealedError`,
  `CompactionInputsChangedError` (`manifest.ts`). Pending: `KeyOutOfRangeError`,
  `WriteStalledError`, `ManifestConflictError`, `ObjectStoreUnavailableError`, public HTTP errors.
- **Merge operators and TTL during reads and compaction** — partial (wiring in Phases 4–6).
  `lsm-core/src/row-resolution.ts` (D10, D11). `row-resolution.test.ts`: `resolveVisibleRow`
  matches a plain oracle for puts, deletes, merges, and TTL at any snapshot and time (shown failing
  with operands applied newest-first); earliest expiry and newest seq of merge results; `U64Add`;
  `MergeOperatorMissingError`; compaction without an operator keeps raw operands.
  `merge-iterator.test.ts` retention properties (newest-snapshot reads unchanged; versions above
  the floor kept; shown failing when expired values are dropped instead of tombstoned).
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

- **Alchemy Stack with every Worker, DO, Workflow, and cron** — pending (grows each phase).
- **Layout `packages/lsm-core`, `packages/lsm-cloudflare`, `packages/lsm-sharding`,
  `infra/cf-slatedb`** — done. Enforced by `tools/workspace-boundaries` (D1, D2).

## §11 Trade-offs

- **`maxUnflushedBytes` backpressure; 128 MB streaming; multipart parts ≥ 5 MiB** — partial.
  `WriteStalledError` on `maxUnflushedBytes` and `l0MaxSsts` (`db-writer.test.ts`); flush streams
  memtable pages into the SST builder and closed blocks into uniform multipart parts
  (`ObjectStore.putMultipart`, test layer enforces the 5 MiB minimum). Compaction: Phase 6.

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
- **GC per database respecting fork checkpoints** — pending (Phase 6–7).
- **`DatabaseLifecycle.create`/`remove` (`DatabaseHasDependentsError`, purge `db:<id>`)** — pending.
- **`CompactionBudget` by `budgetGroup`** — partial (`DatabaseOptions.budgetGroup`).
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
  `.agents/skills/verify-manifest-cas-fencing`, run by `vp run verify`, each broken variant shown
  failing. Checks 4, 5, 6 pending.
- **Hardening scenarios** — pending (Phase 10).
