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
