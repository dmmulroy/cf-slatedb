# Progress

## Current phase

All ten phases are done. Working through the "Remaining work" in [`docs/handoff.md`](handoff.md),
in order.

| Item                                   | Status  |
| -------------------------------------- | ------- |
| 1. Portable Alchemy state for previews | done    |
| 2. Preview deployment tool             | next    |
| 3. Traces from previews                | pending |
| 4. Live checks                         | pending |
| 5. Data residency, per Stack           | pending |
| 6. Known gaps                          | pending |
| 7. Finish                              | pending |

## Remaining-work log

- **Set up (new session).** Node 24.22, `vp` 1.8, pnpm 12.14; `repos/slatedb` and `.tlc/` present;
  the deploy guard already allowlists `CfSlatedb` for `dev`, `preview-1..3`. `vp run ready` exit 0
  before any change.
- **1. Portable Alchemy state.** `cfSlatedbStateLayer` picks `localState()` for local stages and
  `Cloudflare.state({ accountId })` (the account's default state store, pinned to Dillon's account)
  for every live stage. `isLocalLsmStage` (lsm-cloudflare) is now the one local-stage rule, shared
  with the telemetry flush policy. `vp run dev` still runs offline. D43.
- **2. Preview deployment tool.** `tools/preview-cli`: `deploy <stage> [--dev-timings]` (through
  the new `deploy:preview-<n>` tasks; production settings by default), `status`, `url <stage>`,
  `token <stage>`, `destroy-command <stage>`, all with `--json`. Outputs and the access token are
  read from the Cloudflare state store (`state:read` task), so any machine sees the same previews
  (D45). Added before the first live deploy: live stages gate the API, shard router, GC operator
  API, and probe with a per-stage bearer token (D44); the GC operator API gets a `workers.dev` URL
  on live stages only. **Live:** `preview-1` deployed (44 s, 9 resources), redeployed in place
  (no-op), answers `401` without the token, and created a database and returned its status with it.
- **3. Traces from previews.** `TraceCollectorWorker` on live stages only (its own bucket and
  bearer token; stores raw OTLP exports; pages them out), `trace-cli pull --stage` and
  `tail --stage`, `preview-cli collector`. Worker layers now take an `LsmWorkerTelemetry` factory;
  the Stack's binds the collector's URL and token on live stages (D46). **Live findings:**
  (1) Durable Object fetch and alarm spans arrive after the response through the bridge's
  `waitUntil` flush, as D5 assumed; (2) every span of a _Worker's_ own fetch events was lost,
  because a Worker's `fetch()` to another `workers.dev` Worker on the same account is not routed;
  fixed with `global_fetch_strictly_public` on live stages (D47), after which 10/10 probe traces and
  the API's Worker → writer traces are complete. (3) A random external client requested the probe's
  `/favicon.ico` minutes after the first deploy and got `401`, which supports gating previews (D44).
- **4. Live checks** (all on `preview-1`; details and method in
  [`docs/measurements.md`](measurements.md)):
  1. **Workers Cache: `HIT`.** New scenario `block-cache`: the API Worker's read of a flushed block
     is `MISS` (`cache.l2.status`, read from `Cf-Cache-Status` on the `BlockServer` binding's
     response), and the shard router's read of the same block URL is `HIT`. A second read through
     the same isolate never reaches `BlockServer` (L1), hence the second Worker.
  2. **R2 create-if-absent: confirmed.** New scenario `create-race`: 16 concurrent creates of one
     database, exactly one `Created`, 15 `AlreadyExists` on real R2 with the `Headers` form.
  3. **Scenarios: 11/11 pass** on `preview-1` (`run --stage preview-1 all`, `--dev-timings`
     deploy) with trace assertions pulled from the collector, and 11/11 under `vp run dev`.
  4. **Measurements** (production settings): a writer is CPU-bound at ~2.7k rows/s (100-row
     batches) to ~12.3k rows/s (1,000-row batches); a flush costs ~55–90 ms CPU per MiB; a split
     pauses the shard's writes ~13–16 s, because each manifest CAS is ~1.1 s on R2.
     **Finding (fixed):** the shard router's 5 s retry window assumed a local split; live, every write
     arriving early in a 13 s split failed with `DatabaseSealed`. Now 30 s; the re-run had 0 failed
     writes (D40, D48). **Finding (recorded):** the best-effort `LATEST` hint is ~40% of every CAS and
     the forks' compactor wake-ups add ~4 s to a split; moving them off the critical path is left for a
     change that re-runs the manifest CAS model. CPU comes from a new live-only Tail Worker (D48).
- **5. Data residency, per Stack.** `CF_SLATEDB_JURISDICTION` (`eu` | `fedramp` | unset;
  `preview-cli deploy --jurisdiction`) restricts both R2 buckets, every Durable Object namespace
  (through `inLsmJurisdiction` in each client), and the compaction Workflow, and turns Workers Cache
  off (it would keep block bytes anywhere). The advisory per-database option is removed; old
  manifests still decode (D49). **Live:** `preview-2` deployed with `eu`: Cloudflare reports the LSM
  bucket, the trace bucket, and the Workflow in `eu`; all 11 scenarios pass (the `block-cache`
  scenario expects no Workers Cache `HIT` there and saw none).
- **6a. Transactions across a flush: kept, with evidence (D50).** A transaction aborts only if it
  began before a flush took its cut and reads or commits after the flush finished — it must outlast
  the whole flush, which takes at least ~1.5 s on R2. Live: 580/580 transactions committed through a
  28.7 s flush under write load (`measure transactions`). The exact alternative (pinned begin
  manifest + caller-side L0 check) is recorded in D50.

## Done

- **Phase 0 — repository setup** (`b3a4e84`). pnpm workspace with iac's catalog (effect 4.1.4,
  alchemy and `@alchemy.run/cloudflare` 2.0.0-beta.87, vite-plus 1.2.0, typescript ^7), iac's lint
  rules and tsconfig, `.node-version`, `.gitignore`, workspaces `packages/lsm-core`,
  `packages/lsm-cloudflare`, `packages/lsm-sharding`, `infra/cf-slatedb`,
  `tools/{trace-cli,scenario-cli,formal-verification,traits,workspace-boundaries}`. Boundary test
  adapted (pure core, sharding public-API-only, no `exports` under `infra/`). Docs adapted;
  `docs/testing.md` rewritten local-first; `AGENTS.md` written; iac traits copied; SlateDB cloned
  read-only into `repos/slatedb`.
- **Phase 1 — trace tooling.**
  - `tools/trace-cli` (`effect/cli`): `serve` (OTLP/HTTP receiver on `127.0.0.1:4318`, JSON and
    protobuf, logs/metrics accepted and discarded), `list`, `show`, `tail`, `grep`, `stats`, `diff`
    (trace ids or `run:<startMs>-<endMs>`), `assert` (file or stdin; `SpanExists` with parent,
    `NoSpan`, `SpanCount`, `NoErrorStatus`, `SameTrace`, `Attribute`), `clear`. `--json` on every
    command. Store: `.traces/spans/<traceId>.ndjson`. Typed errors (`TraceStoreError`,
    `OtlpTraceDecodeError`, `TraceCliError`). Tests export real spans through Effect's
    `OtlpTracer` in both serializations.
  - Review fix: `list` is one pass (summarize, filter, bounded binary-insert top-`limit`) and
    `summarizeTrace` is one pass over spans plus a pass over root candidates only.
  - `lsmTelemetryLayer` (`@cf-slatedb/lsm-cloudflare`) wraps `Telemetry.layerOtlp`.
  - OTLP flush bug **confirmed on beta.87**. Alchemy is not patched (review). Workaround (D5): a
    stage-selected flush policy. Local stages (`dev`, `test`, `test-*`, ...) export a handler's spans
    before the response; live stages (`production`, previews, ...) add no export latency and rely on
    the bridge's `waitUntil` flush. Both modes are covered by unit tests and by workerd tests
    (`telemetry-smoke.workerd.ts`, `telemetry-smoke-live-policy.workerd.ts`).
  - `infra/cf-slatedb/alchemy.run.ts` + `vp run dev` run a telemetry probe Worker + DO under
    `alchemy dev`; verified with the trace CLI (Worker → DO span nesting, alarm span,
    `assert` pass/fail exit codes).
  - `@cf-slatedb/lsm-cloudflare` `test` now runs unit tests and the workerd suite (`--mode workerd`).
- **Phase 2 — lsm-core.** Branded ids (`DatabaseId`, `SeqNum`, `SstId` ULID, `ManifestId`,
  writer/compactor epochs, `SortedRunId`, `CheckpointId`, `Timestamp`), `KeyRange`, `Row`/`RowValue`
  (`Data.TaggedEnum`), key order via `Bytes.Order` (key asc, seq desc), CRC32C, Bloom filter
  (hash-pair builder), byte codec (varints), data block codec (SlateDB V2 rows, restart every 16,
  CRC32C), SST builder (streaming blocks) and metadata reader, k-way merge iterator over `Pull`
  with a private heap (drops exact duplicates), merge operators (`Concat`, `U64Add`), read
  resolution (snapshot, TTL, merge) and compaction retention, `Manifest` v1 with pure idempotent
  edits (flush, compaction, fencing, seal, checkpoints, fork, changelog), size-tiered scheduler,
  `LsmSettings`, ULIDs. 24 tests, including properties against a `Map` oracle; each property was
  shown failing on a deliberately broken variant (TTL, restart seek, dedup, retention, scheduler
  destination). Decisions D8–D16.
- `vp run ready` passes.

- **Orchestrator review of Phase 2 addressed.** Dedicated property tests for the scheduler
  (`size-tiered-compaction-scheduler.test.ts`: overlap/in-flight, min/max, size threshold,
  convergence), Bloom filter (no false negatives; FP rate vs theory), row resolution (plain oracle
  for merge/tombstone/TTL), varints (`byte-codec.test.ts`), and ULIDs (round trip, monotonic sort).
  Each was shown failing on a deliberate break (size threshold ignored, in-flight not reserved,
  probes collapsed, one probe, varint off-by-one, operand order inverted). Coverage matrix updated.
- **Phase 3 — object storage and manifests.** `ObjectStore` (R2 adapter + faithful in-memory
  layer), `ManifestReader`, `ManifestStore` (CAS with retry, idempotent edits, `LATEST` hint),
  `ManifestCache`, `LsmConfiguration`, `LsmBucket`. Local-R2 workerd test shows the Headers
  `If-None-Match: *` form works and 16 concurrent commits lose nothing.
- **Formal check 2 (fencing) and 3 (manifest CAS linearizability).**
  `tools/formal-verification/specs/ManifestCasFencing.tla`, two configs; `vp run verify`
  (`tools/formal-verification/src/verify.ts`) downloads a pinned JDK 21 + tla2tools 1.7.4 into
  `.tlc/`, runs every check under a hard time limit, and requires each broken variant to fail:
  `noWriterEpochCheck` → `NoStaleCommit`, `blindCreate` → `CreateOnce`, `noInputsCheck` →
  `AckedDataVisible`. ~10 s total. Skill: `.agents/skills/verify-manifest-cas-fencing`. The
  matching real-code checks were also broken and caught by unit tests.

- **Phase 4 — writer.** `WriteBatch`/`WriteReceipt` and `collapseWriteBatch` (lsm-core, property
  test against sequential application), `WriterSqlStorage` (Durable Object SQLite; `node:sqlite`
  test layer), `MemTable` (the WAL: `memtable_rows (key, seq DESC)`, sequence oracle and clock in
  the commit transaction, keyset-paged flush stream, atomic `consistently` reads), `SstStore`
  (streams blocks into multipart uploads), `DurableObjectAlarm`, `DbWriter` (lazy fence-in, `KeyOutOfRange`,
  sealed, backpressure `WriteStalled` on bytes and L0 backlog, alarm flush on `l0SstSize`/`maxL0Age`,
  upload → CAS → truncate, restart recovery, `Latest` probe with `flushedSeq`, memtable scan pages,
  changes feed, changelog SST export, seal), `WriterHttpApi` (`/v1`), `WriterServer` DO,
  `WriterClient` (execution memo + `Cache`; test layer crosses the real HTTP contract in-process),
  public `DbHttpApi` (create, write, flush, status, changes) with the public error envelope and
  `x-request-id`, `DbApiWorker` (hosts `WriterServer`), `DatabaseLifecycle.create`.
  Tests: memtable properties on real SQLite, writer behavior (fencing with two writers, crash
  between commit and truncate, backpressure, seal, alarm, changelog), writer client over HTTP, and
  `test/writer/writer.workerd.ts` (API Worker → DO SQLite → local R2, including the alarm flush).
  `vp run dev` serves the API on `http://localhost:1337`; the trace CLI shows API → writer → flush →
  one manifest CAS → truncate, and the flush assertion passes.
- **Formal check 1 (no acknowledged write lost or hidden).**
  `tools/formal-verification/specs/NoAckedWriteLost.tla`, skill
  `.agents/skills/verify-no-acked-write-lost`. Found a real bug while being written (non-atomic
  `flushed_seq` + rows in `probe`); fixed (D21). Broken variants `truncateBeforeCommit` →
  `NoAckedWriteLost`, `nonAtomicProbe` → `LatestSeesAcked`.

- **Phase 5 — reader and block serving.** `ReadConsistency` (lsm-core), `ObjectReader` (read-only
  R2), `BlockServerHttpApi` + handlers (immutable `200`s, `Cache-Control: public, max-age=31536000,
  immutable`, `Cache-Tag: db:<owner>`, latest-manifest hint, tag purge), `BlockServerWorker`
  (`Cloudflare.cache({ crossVersionCache: true })`, `workersDev: false`), `BlockSource` (service
  binding `fetch()`, never RPC; test layer reads the store), `BlockCache` (separate data and meta
  `Cache`s, joined misses, failures not cached, hit/miss span attributes and metric), `DbReader`
  (manifest for a view, range + Bloom skip, block seek across blocks, merged scans with prefetch,
  resolution), `DbClient` (`put`, `delete`, `merge`, `write`, `get`, `getRow`, `scan`,
  `scanPrefix`, `flush`, `snapshot`; `Latest`/`AtLeast`/`Flushed` views), public read endpoints
  (`get`, `scan` with `resumeFrom`, `snapshots`). Tests: a `DbClient` property test against a `Map`
  oracle (puts, deletes, merges, flushes, reads at every consistency, scans); staleness and
  snapshot tests; `BlockServer` contract test; workerd read path through the real binding. Under
  `vp run dev` the trace CLI shows a `Flushed` read with no writer call and no `LIST`, served
  through `BlockServer`, and the repeat read hitting L1. Decisions D27–D32.

- **Phase 6 — compaction and GC.** lsm-core: `compactRowStreams` (streaming compaction merge),
  GC planning (`planManifestCollection` by successor age, `planSstCollection`,
  `prepareGarbageCollection`), `gcSstBoundary` with `SstBelowGcBoundaryError`, the compaction
  budget ledger. lsm-cloudflare: `CompactionExecutor`, `runCompactionJob`, `CompactionWorkflow`,
  `Compactor`/`CompactorServer`, `CompactionBudget` (+ global DO), `CompactionTrigger` (writers wake
  compactors), `GarbageCollector` and `GcWorker` (cron + operator API), RFC 0026 manifest boundary,
  versioned and delimited `ObjectStore` operations, `GET /v1/databases/:id/compaction`. Fixed: the
  multipart re-chunker copied the whole pending part on every block. Formal check 4 (`GcSafety.tla`,
  skill `verify-gc-safety`) found that manifest age must be measured from the successor. The workerd
  integration test runs four flushes through a real compaction Workflow and an on-demand GC, with
  its own OTLP collector (14 s). Decisions D33–D36.

- **Phase 7 — lifecycle.** lsm-core: `Removed` state, `markRemoved` with
  `DatabaseHasDependentsError`, `DatabaseRemovedError` from `fenceWriter`/`addCheckpoint`,
  `releasableExternalRefs`, fork rewrite planning. lsm-cloudflare: `DatabaseLifecycle`
  `createCheckpoint`/`deleteCheckpoint`/`seal`/`forkRange`/`remove`, writer and compactor `destroy`
  endpoints, compactor release of fork holds, GC sweep of prefixes without a manifest, public
  endpoints for checkpoints, seal, forks, and removal. Formal check 5 (`SealSplit.tla`, skill
  `verify-seal-split`). The workerd test splits a database through the public API: seal, checkpoint,
  two forks, each key in exactly one successor, removal blocked until the forks' compactors rewrite
  and release, then removed (15 s). Fixed along the way: the trace CLI ignored unknown keys in
  assertion files, so a misspelled bound passed vacuously. Decisions D37–D39.

- **Phase 8 — sharding package.** `packages/lsm-sharding`: `ShardMap` (pure), `ShardDirectory` +
  `ShardDirectoryServer` (one DO per group; records the pending split and allocates its databases),
  `ShardRouter` (cached map, refresh-and-retry on sealed/out-of-range/removed), `ShardSplit` (begin,
  seal, checkpoint, fork ×2, publish; resumable), and the example `ShardRouterWorker` in the Stack.
  lsm-cloudflare gained `dbServicesLayer` (DbClient + lifecycle for any Worker, cross-script) and
  `dbServicesTestLayer`. Under `vp run dev` a split through the router Worker crosses Workers and the
  reads and scans before and after it are correct. Decision D40.

- **Phase 9 — transactions.** Optimistic transactions with `Snapshot` and `SerializableSnapshot`
  isolation: the conflict check runs inside the writer's commit lock against its SQLite memtable;
  `DbClient.transaction` (interactive) and the stateless begin/get/scan/commit primitives, also on
  the public API. A property test checks serializability in commit order (and no lost updates for
  `Snapshot`) over random interleavings with direct writes and flushes; formal check 6
  (`Transactions.tla`, skill `verify-transactions`). The property test first found nothing because
  the generator produced mostly one-step schedules; it now requires 20–60 steps, and every mutation
  of the checks is caught. Decision D41.

- **Phase 10 — hardening.** The change feed's read side (D42); `tools/scenario-cli` (nine
  end-to-end scenarios against `vp run dev` with trace assertions, all passing); fault injection
  (R2 outages and flaky calls, a crash between the L0 upload and the manifest commit, a compaction
  resumed after a failed step) and a split under concurrent writes, each shown failing on a broken
  variant; erasable-syntax-only TypeScript so Node runs every package directly; the coverage matrix
  complete (one partial row: data residency, open question 3).

## Hand-off

**What was built.** cf-slatedb, a SlateDB-style LSM key-value store on Cloudflare:

- `packages/lsm-core` (pure, no Cloudflare): ids, rows and resolution (merge operators, TTL), CRC32C,
  Bloom filters, block and SST codecs, the k-way merge, the manifest and its pure edits (fencing,
  flush, compaction, seal, removal, checkpoints, forks, GC boundaries), the size-tiered scheduler,
  the compaction merge, GC planning, the compaction budget ledger, write batches, read consistency,
  transactions.
- `packages/lsm-cloudflare`: `ObjectStore` (R2), manifests (CAS, RFC 0026 boundary), the writer
  (`WriterServer` Durable Object; SQLite memtable = WAL; flush, backpressure, seal, change feed,
  transactions), the reader (`DbReader` in the caller's isolate; L1 `BlockCache`; `BlockServer`
  Worker behind Workers Cache), compaction (`CompactorServer`, `CompactionWorkflow`,
  `CompactionBudgetServer`), GC (`GarbageCollector`, cron `GcWorker`), `DatabaseLifecycle`
  (checkpoints, seal, zero-copy forks, removal), `DbClient`, and the public `DbHttpApi` (`/v1`).
- `packages/lsm-sharding` (public APIs only): range-sharded groups (`ShardDirectoryServer`,
  `ShardRouter`, resumable `ShardSplit`) and an example `ShardRouterWorker`.
- `infra/cf-slatedb/alchemy.run.ts`: the Stack (bucket; `DbApiWorker` with `WriterServer`;
  `BlockServerWorker`; `CompactorWorker`; `GcWorker`; `ShardRouterWorker`; telemetry probe).
- Tools: `trace-cli`, `scenario-cli`, `formal-verification` (TLC runner), `workspace-boundaries`.

**How to run it.**

- `vp run ready` — format, lint, types, and every test (unit, property, and the workerd
  integration test, which brings up the whole Stack locally with its own OTLP collector).
- `vp run verify` — the six formal checks (20 TLC runs, about 2 minutes; downloads a pinned JDK
  and TLA+ tools into `.tlc/` on first use).
- `vp run dev` — the full Stack locally (API on `:1337`, BlockServer `:1338`, compactor `:1339`,
  GC `:1340`, shard router `:1341`; dev settings: `gcMinAge` 10 s, compactor polls every second).
  With `node tools/trace-cli/src/trace-cli.ts serve` running, `node tools/scenario-cli/src/
  scenario-cli.ts run all` runs every scenario with trace assertions.

**What is verified, and under which bounds.** Each check has a skill in `.agents/skills/verify-*`
with its bounds, assumptions, covered code, and sync commit; every check was shown failing on
deliberately broken variants:

1. No acknowledged write lost or hidden (`NoAckedWriteLost.tla`): 4 writes, 2 crashes, 2 reads.
2. Fencing and 3. manifest CAS linearizability (`ManifestCasFencing.tla`): 2 writers and 2
   compactors over 9 manifest ids, plus a race config for changed compaction inputs.
3. GC safety (`GcSafety.tla`): 2 SSTs, clock 0–3, 5 manifests, interleaved GC steps (found that
   manifest age must be measured from the successor).
4. Seal and split (`SealSplit.tla`): 3 keys, 4 writes, every interleaving of seal, checkpoint, forks.
5. Transactions (`Transactions.tla`): 2 keys, 2 transactions, 6 commits; serializable in commit order.

Code-level properties (codecs, merge, compaction merge, GC plans, ledger, shard map, the client and
change feed against oracles, transactions, fault injection) are property tests with mutation checks
(`docs/coverage-matrix.md` names each).

**Known gaps.**

- Data residency is advisory (open question 3).
- The L1 block cache is bounded by entry count, not a per-`budgetGroup` byte budget (D32).
- Transactions abort when data newer than their reads is flushed during them (D41).
- Snapshots are protected by `gcMinAge`, not checkpoints (D30); a snapshot older than that may
  expire (`SnapshotExpired`).
- A corrupted SST fails its compaction job as a defect; the compactor plans it again (D34).

**Needs a live preview to confirm** (open questions 1, 2, 4): Workers Cache hits on `BlockServer`,
R2's `If-None-Match` Headers form, and the §12 measurements. Nothing has run against the real
account.

## Findings

- Effect's HTTP server middleware records `client.address` (the caller's IP) and
  `user_agent.original` on server spans. They reach the trace store on live stages. They are not
  keys, values, or secrets, but they are personal data; before a production deploy, drop them
  from server spans or keep traces in a store with matching retention.

- alchemy 2.0.0-beta.87 still flushes event telemetry inside `waitUntil` after the event settles;
  worked around in application code (D5), no patch.
- Effect 4.1.4 names HTTP spans by method (`GET`); the trace CLI shows span kind (D7).
- No Java or mise on this machine; `vp run verify` downloads a pinned JDK and `tla2tools.jar` into
  `.tlc/` (gitignored) and passes `JAVA`/`TLA2TOOLS` to `/statechart` runs.
- The traits MCP checker is configured (`.pi/mcp.json`) and `TYPESAFE_API_KEY` is set; it runs as an
  MCP server in the next pi session. Until then, review against `.agents/traits/*.md` by hand.

## Open questions

1. ~~Workers Cache hit on a deployed preview~~ — confirmed (item 4, `docs/measurements.md`).
2. ~~R2 `If-None-Match: *` with the Headers form on real R2~~ — confirmed (item 4).
3. ~~Data residency design~~ — Dillon chose per-Stack residency (hand-off item 5).
4. **Live measurements** — rows/s per writer, flush CPU per MB, and the split pause are measured
   (`docs/measurements.md`). Still open, if Dillon wants them: whether DO SQLite's page cache counts
   against 128 MB, and the Workflows concurrent-instance limit on the plan.
