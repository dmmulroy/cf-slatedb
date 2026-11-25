# Coverage matrix

Every design section and decision from `docs/show-me-r2-lsm-design.html` → implementing files →
tests, trace assertions, and formal checks. Status is `pending`, `partial`, or `done`. Paths are
relative to `packages/` unless they start with `infra/`, `tools/`, or `docs/`.

## §1 What changes from SlateDB

- **Single writer per database, fenced by `writerEpoch`** — pending. Edits: `lsm-core/src/manifest.ts`
  (`fenceWriter`, `commitL0Flush` epoch check; tested in `manifest.test.ts`). Writer: Phase 4.
  Formal: fencing model (Phase 3).
- **WAL = DO SQLite memtable; an ack is durable; no `awaitDurable`** — pending (Phase 4).
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

- **`POST /v1/databases/:id/batches`, `Semaphore(1)`, one SQLite transaction** — pending (Phase 4).
- **Flush trigger: `l0SstSize`, `maxL0Age`, `setAlarm(now)`** — pending (Phase 4). Settings:
  `lsm-core/src/lsm-settings.ts`.
- **`flushL0`: SST build, PUT, manifest CAS, delete rows ≤ cut** — partial. SST builder:
  `lsm-core/src/sst-codec.ts`; edit: `commitL0Flush` (idempotent; `manifest.test.ts`). Writer: Phase 4.
- **Changelog export (CDC), `changes?afterSeq=`** — partial. `SstKind` `Changelog` and
  `changelogRowOrder` (`lsm-core`); `Manifest.changelog`, `pruneChangelog`. Export: Phase 4.

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

- **Writer init fencing loop** — pending (Phase 4).
- **`manifest/LATEST` hint + probe `id+1`; no LIST on read paths** — partial.
  `lsm-cloudflare/src/manifest-reader.ts` (`loadLatest`; LIST only to recover when the hint names a
  collected manifest). `manifest-store.test.ts` covers a lagging hint and a collected hinted
  manifest. Trace assertion "no `ObjectStore.list` on a read path": Phase 5 scenarios.
- **Compactor edit replaces exactly its inputs, else fails** — partial. `commitCompaction`
  (`manifest.test.ts`: replace, idempotent retry, `CompactionInputsChangedError`,
  `CompactorFencedError`).
- **Delete-after-commit; duplicate replay is deduplicated** — partial. `mergeRowStreams` drops exact
  duplicates (`merge-iterator.test.ts`, shown failing without the drop). Writer: Phase 4.
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
- **`MemTable`, `SeqOracle`, `DbWriter`, `ChangelogExport`** — pending (Phase 4).
- **`CompactionScheduler` (pure)** — done. `lsm-core/src/size-tiered-compaction-scheduler.ts`;
  `size-tiered-compaction-scheduler.test.ts` over `Arbitrary` manifests and options: jobs are
  consecutive, all-L0 or all-runs, within min/max sources and `includeSizeThreshold`, never overlap
  each other or a job in flight, have the right destination and `isBottom`; repeatedly committing
  planned jobs converges (fallback consolidation terminates). Shown failing with the size
  threshold ignored and with in-flight sources not reserved (D15).
- **`DatabaseLifecycle`, `GarbageCollector`, `CompactionBudget`** — pending (Phases 6–7).

## §8 Public API and semantics

- **`put`, `delete`, `merge`, `write`, `get`, `getRow`, `scan`, `flush`** — pending (Phases 4–5).
- **Errors** — partial. Core: `SstCorruptedError`, `MergeOperatorMissingError`
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
  Settings in `lsm-settings.ts`; streaming SST builder (`SstBuilder.add` returns closed blocks);
  Bloom builder keeps 8 bytes per key (D9). Writer and compaction: Phases 4 and 6.

## §12 Multi-tenancy primitives and sharding

- **Key-range bounds (`KeyOutOfRangeError`)** — partial (`Manifest.keyRange`); writer: Phase 4.
- **Seal (`DatabaseSealedError { successors }`)** — partial (`sealManifest`, `fenceWriter` and
  `commitL0Flush` reject sealed; `manifest.test.ts`).
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
- **Formal checks 1–6** — pending.
- **Hardening scenarios** — pending (Phase 10).
