# Progress

## Current phase

Phase 5: reader and block serving (starting). Phases 0–4 are done.

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
  (streams blocks into multipart uploads), `WriterAlarm`, `DbWriter` (lazy fence-in, `KeyOutOfRange`,
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

## Next

- Phase 5: `DbReader` (`Latest`/`AtLeast`/`Flushed`, `get`, `getRow`, `scan`, `scanPrefix`,
  snapshots), `BlockCache` (separate data and metadata caches), `BlockSource`, `BlockServer` Worker
  (Workers Cache, immutable URLs), `DbClient`, read endpoints on the public API.

## Findings

- alchemy 2.0.0-beta.87 still flushes event telemetry inside `waitUntil` after the event settles;
  worked around in application code (D5), no patch.
- Effect 4.1.4 names HTTP spans by method (`GET`); the trace CLI shows span kind (D7).
- No Java or mise on this machine; `vp run verify` downloads a pinned JDK and `tla2tools.jar` into
  `.tlc/` (gitignored) and passes `JAVA`/`TLA2TOOLS` to `/statechart` runs.
- The traits MCP checker is configured (`.pi/mcp.json`) and `TYPESAFE_API_KEY` is set; it runs as an
  MCP server in the next pi session. Until then, review against `.agents/traits/*.md` by hand.

## Open questions

None yet.
