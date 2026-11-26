# shaledb implementation brief

You are implementing **shaledb**: a SlateDB-style LSM key-value store on Cloudflare R2 and Durable Objects,
written with Effect 4 and deployed with Alchemy v2. Implement the design exhaustively and end to end. The work is
done when every part of the design is implemented, it runs locally, and the correctness tooling below proves it.

Work autonomously. Do not stop to ask for confirmation between phases. Ask Dillon only for the decisions listed under
"Stop and ask", and record every other judgment call in `docs/design-decisions.md`.

## Source of truth

1. **`docs/show-me-r2-lsm-design.html`** is the design. Read all 12 sections and the Effect audit before you write
   code. Where this brief and the design disagree, the design wins, unless the brief explicitly overrides it.
2. Settled decisions that the design page already reflects, and which you must not reopen without a recorded reason:
   - The core has no tenant concept. Its unit is a database: one opaque `DatabaseId` with its own `WriterServer`,
     `CompactorServer`, manifest chain, R2 prefix `db/<id>/`, and cache tag `db:<id>`. Sharding is an optional
     package (`packages/lsm-sharding`) that uses only public APIs.
   - The WAL is the writer Durable Object's SQLite table, which is also the memtable. There is no `WalStore`
     abstraction and no R2 WAL. An acknowledged write is durable; `WriteReceipt` has no `awaitDurable`. Change data
     capture is an optional changelog SST exported during each L0 flush.
   - SST reads run in the caller's isolate. The writer handles writes and memtable-only `Latest` probes only.
     Read consistency is `Latest | AtLeast(seq) | Flushed`.
   - The block cache is L1 Effect `Cache` in isolate memory, plus L2 Workers Cache (not `caches.default`) in front of
     a separate Effect-native `BlockServer` Worker. Every block, SST metadata section, and manifest has its own URL
     (`/v1/databases/<owner>/ssts/<ulid>/blocks/<offset>-<length>`, `/meta`, `/manifests/<id>`), returns a full
     `200` with `Cache-Control: public, max-age=31536000, immutable` and `Cache-Tag: db:<owner>`, and is called over
     a service binding `fetch()`, never RPC. Use `Cloudflare.cache({ crossVersionCache: true })`. The database id is
     in the path, not `ctx.props` (Alchemy drops binding props at upload).
   - Manifests use R2 create-if-absent (`If-None-Match: *`). Pass `new Headers({ "If-None-Match": "*" })` to the
     R2 binding's `onlyIf`, because the `R2Conditional` object form once treated `*` as a literal etag
     (workerd #2572). Readers find the latest manifest through a `manifest/LATEST` hint plus probing `id + 1`,
     never R2 `LIST` on a read path.
   - Core lifecycle primitives are in the manifest schema from v1: key-range bounds (`KeyOutOfRangeError`),
     `seal` (`DatabaseSealedError { successors }`), zero-copy `forkRange` (SST views plus external references), and
     GC that stays per database because forks hold checkpoints in their sources' manifests.
3. **SlateDB itself** is the reference implementation of the LSM semantics. Clone it read-only into
   `repos/slatedb` (gitignored): `git clone --depth 1 https://github.com/slatedb/slatedb.git repos/slatedb`.
   Read `repos/slatedb/website/src/content/docs/docs/design/*.mdx`, `repos/slatedb/rfcs/*.md`, and the Rust source
   under `repos/slatedb/slatedb/src` whenever you implement the matching part. Its FizzBee specs in
   `repos/slatedb/specs/fizzbee` (`WalProtocol.fizz`, `KeyValueStore.fizz`, `SequencedMetadataBoundary.fizz`) are
   prior art for the formal models.

## Reference repositories (read-only; copy patterns, never import)

- `../iac` — the closest pattern source. Read its `AGENTS.md`, `docs/coding-standards.md`, `docs/errors.md`,
  `docs/vite-plus.md`, `.agents/traits/*.md`, `tools/workspace-boundaries`, `pnpm-workspace.yaml` (catalog versions),
  `vite.config.ts` (lint rules), `tsconfig.json`, and `infra/apps/recipes.mulroy/api` (Effect HTTP API on a Worker,
  R2 and D1 services, Alchemy Stack, typed errors, test layers).
- `../R10/main` — a second monorepo with the same conventions. Read its `AGENT.md`, `docs/testing.md`, and
  `tools/` for repository tooling patterns.
- `../cf-twitch/apps/api/src/features/song-queue/` — a Durable Object served as an Effect `HttpApi`, with SQLite via
  `@effect/sql-sqlite-do`, an alarm, and `SongQueueServer.make(...)`.
- `../alchemy-workerd-otlp-flush-repro` — local OTLP tracing from Alchemy workerd Workers and Durable Objects via
  `Telemetry.layerOtlp`, and a known bug where an event's spans are lost when the event settles before the exporter
  flushes (reproduced on `alchemy@2.0.0-beta.72`). Check whether the version you install still has it, and work
  around it if it does.
- Installed sources are the API reference. Effect 4 and Alchemy v2 differ from your training data: read
  `node_modules/effect/src`, `node_modules/alchemy/src`, and `node_modules/@alchemy.run/cloudflare/src` before you
  use an API.

## Repository setup (Phase 0)

The repo is `/Users/dmmulroy/Code/personal/shaledb`. It has no commits yet. Already present:
`docs/coding-standards.md`, `docs/errors.md`, `docs/vite-plus.md` (from iac), `docs/testing.md` (from R10/Overseer),
the design page, and three pstack skills in `.agents/skills/`.

- Set the repo up like iac: pnpm workspace with a catalog pinned to iac's versions (effect 4.1.4, alchemy and
  `@alchemy.run/cloudflare` 2.0.0-beta.87, vite-plus 1.2.0, typescript ^7), Vite+ (`vp`) tasks, iac's lint rules
  and tsconfig, `.node-version`, `.gitignore` (`node_modules`, `.alchemy`, `repos/`, trace stores), and
  `tools/workspace-boundaries` adapted to this layout.
- Layout, following design §10: `packages/lsm-core` (pure; no Cloudflare imports), `packages/lsm-cloudflare`,
  `packages/lsm-sharding` (optional; public API only), `infra/shaledb` (the deployable Alchemy Stack; no
  `exports`), `tools/*` (trace CLI, formal verification, load and scenario drivers).
- Adapt the copied docs to this project. Replace iac, Overseer, and Account/Order/Invoice references with
  shaledb's own vocabulary only where they name a project. Keep the rules. Rewrite `docs/testing.md` so the
  default test strategy is **local**: unit and property tests, workerd integration through Alchemy's local dev
  providers, and scenario tests against a locally running Stack with trace assertions. Real-cloud end-to-end tests
  become an opt-in task that never runs without Dillon's approval.
- Write `AGENTS.md` in the iac style: project summary, stack, links to the docs, the infrastructure rules (local
  first; no live deploys without approval; use `vp run` tasks rather than the Alchemy CLI directly), the trace CLI,
  the formal verification skills, and `vp run ready` as the validation gate.
- Copy iac's traits (`.agents/traits/*.md`) and keep them as review guidance. The traits MCP checker needs a
  `TYPESAFE_API_KEY`; set it up only if the key is available, and otherwise review against the trait files by hand.
- Commit early and often with conventional commit messages. Never rewrite history.

## Phases

Each phase ends with `vp run ready` passing, a commit, and an update to `docs/progress.md`. Build the trace CLI in
Phase 1, before the system it observes, and use it in every later phase.

1. **Trace tooling.** `tools/trace-cli` (see "Trace CLI" below), plus OTLP wiring through `Telemetry.layerOtlp` for
   every Worker and Durable Object, verified against a toy Worker and Durable Object under `alchemy dev`.
2. **lsm-core.** Branded ids (`DatabaseId`, `SeqNum`, `SstId` ULID, `ManifestId`, epochs), `Row` and `RowValue`,
   key order via `Bytes.Order` (key ascending, seq descending), CRC32C, Bloom filter, varint, data block codec
   (prefix compression, restart every 16 rows), SST builder and reader (§SST layout in SlateDB's docs), k-way merge
   iterator over `Pull` with a private heap, `Manifest` schema with every v1 field from design §6 and §12, pure
   manifest edits, size-tiered compaction scheduler. Property tests with `Arbitrary` against a plain `Map` oracle.
3. **Object storage and manifests.** `ObjectStore` (R2 adapter that owns `R2Error` translation, plus a faithful
   in-memory test layer with real create-if-absent semantics), `ManifestStore` with CAS retry, `manifest/LATEST`
   hint, `ManifestCache` (`Resource.auto`), writer and compactor epoch fencing.
4. **Writer.** `WriterServer` Durable Object as an Effect `HttpApi` (versioned `/v1` routes; Durable Object HTTP
   boundary rules from the coding standards), `MemTable` on DO SQLite, `SeqOracle`, write batches, puts, deletes,
   merges, TTL, the monotonic clock, backpressure (`WriteStalledError`), alarm-driven L0 flush with
   `l0SstSize`, `maxL0Age`, and `maxUnflushedBytes`, the `Latest` memtable probe returning `flushedSeq`, and the
   optional changelog export.
5. **Reader and block serving.** `DbReader`, `BlockCache` (separate data and metadata caches), `BlockSource`,
   `BlockServer` Worker with Workers Cache, `SstHandle.metaOffset`/`metaLength`, the three read consistency levels,
   `get`, `getRow`, `scan`, `scanPrefix`, snapshots, and `DbClient` with per-invocation HTTP client caching.
6. **Compaction and GC.** `CompactorServer` Durable Object with an alarm, Cloudflare Workflow execution (streaming
   merge within 128 MB; multipart output), `CompactionBudget`, compactor epoch fencing, compaction-time TTL and merge
   operator resolution, `GarbageCollector` with `minAge` and checkpoint protection, and the `gc/` boundary files.
7. **Lifecycle.** Checkpoints, `DatabaseLifecycle.create`, `remove` (with `DatabaseHasDependentsError`), `seal`,
   `forkRange`, and fork checkpoint release once compaction has rewritten all external SSTs.
8. **Sharding package.** `ShardDirectory` Durable Object, `ShardRouter`, and `ShardSplit` (seal, then fork once,
   then publish), with retries on `DatabaseSealedError` and `KeyOutOfRangeError`.
9. **Transactions.** Optimistic transactions with `Snapshot` and `SerializableSnapshot` isolation, with the conflict
   check inside the writer.
10. **Hardening.** Load and failure scenarios: writer eviction mid-flush, a crash between L0 upload and manifest
    commit, CAS races, a stale writer after fencing, R2 failures, compaction resumed after failure, a split under
    load. Fill the coverage matrix.

Phases 2 and 3 can overlap. Do not start Phase 4 until the trace CLI works under `alchemy dev`.

## Running locally

- `infra/shaledb/alchemy.run.ts` declares the full Stack: R2 bucket, `WriterServer`, `CompactorServer`,
  `ShardDirectory`, `BlockServer` Worker, API Worker, compaction Workflow, and the GC cron Worker. Production names
  and retention follow iac's stage rules, but **never deploy a live stage** without Dillon's approval.
- Provide `vp run dev` (Alchemy local dev, stage `dev`, local R2 and Durable Objects) and a scenario driver CLI,
  `tools/scenario-cli`, that runs named scenarios against the local Stack (write and read back, flush, compaction,
  fork and split, fencing, changelog) and asserts on both results and collected traces.
- Workers Cache is not emulated locally (neither in Vitest nor in `alchemy dev`). Keep `blockSourceTestLayer`
  honest, and prepare a live-preview check for `Cf-Cache-Status: HIT` that runs only with approval.

## Trace CLI (your primary debugging and testing tool)

Build `tools/trace-cli` early and use it as your main way to test, debug, and form hypotheses about the running
system.

- `serve`: a local OTLP/HTTP receiver (default `127.0.0.1:4318`, `/v1/traces`, JSON and protobuf) that stores spans
  in a local store under the repo (gitignored), keyed by trace id.
- `list`, `show <traceId>` (indented span tree with durations, attributes, status, and errors), `tail` (follow live
  spans), `grep` (by span name, attribute, error, database id), `stats` (latency percentiles per span name), and
  `diff` (compare two traces or runs).
- `assert`: declarative checks a scenario can run, such as "this request produced a `WriterServer.write` span whose
  parent is the API Worker's HTTP server span", "no `R2.list` span on a read path", "the flush made exactly one
  manifest CAS", or "no span has an error status".
- Built with `effect/cli` in the repo's style, with typed errors per `docs/errors.md`. It must work from scripts
  with machine-readable JSON output.
- Every service operation uses `Effect.fn` spans with safe identifying attributes (`database.id`, `sst.id`,
  `manifest.id`, `seq`, sizes, cache hit or miss). Never put keys, values, or secrets in attributes.
- Verify that spans from Durable Object events and alarms arrive, including after the event settles (see the
  OTLP flush repro).

## Formal verification

Read `.agents/skills/create-formal-verification-skill/SKILL.md` and follow it, together with
`.agents/skills/statechart/SKILL.md` for TLA+ models and `.agents/skills/maintain-formal-verification-skill/SKILL.md`
after later changes. These pstack skills are disabled for automatic invocation, so open them by path.

Verify at least these properties, each as its own check with a generated project-local verify skill under
`.agents/skills/verify-*`:

1. **No acknowledged write is lost or hidden.** Across writer eviction, flush, crash between L0 upload and manifest
   commit, and the delete-after-commit rule, a `Latest` read always sees every acknowledged write
   (memtable plus manifest at or past `flushedSeq`).
2. **Fencing.** After a new writer or compactor epoch commits, no stale writer or compactor can commit a manifest,
   and a stale writer's orphan SSTs are never referenced.
3. **Manifest CAS linearizability** of concurrent writer, compactor, and lifecycle edits, including compactions whose
   inputs changed.
4. **GC safety.** No SST that any live manifest, checkpoint, or fork of the database references is ever deleted,
   including across `forkRange` and fork checkpoint release.
5. **Seal and split.** After `seal`, no write commits to the sealed database, and every write acknowledged before
   the seal is visible in exactly one successor.
6. **Transactions.** `SerializableSnapshot` admits only serializable histories within the model's bounds.

TLA+ is the default for these protocol properties. For code-level properties (codecs, merge iterator, block
seek), use property-based tests on the real code with generated inputs. Show every check failing on a deliberately
broken variant, record bounds, assumptions, covered files, and sync commits, and run the verifications from
`vp run verify`.

## Quality bar

- Follow `docs/coding-standards.md` and `docs/errors.md` exactly: service module layout and naming, `Option` for
  optional values, parsing at the owning boundary, no `as unknown as`, JSDoc on every export, `Effect.fn` with
  whole-function transforms, explicit `catchTags`, and a faithful test layer for each service.
- Keep `docs/coverage-matrix.md`: every design section and decision → implementing files → tests, trace assertions,
  and formal checks that cover it. Nothing in the design may be left without an entry.
- Keep `docs/design-decisions.md`: each place where implementation required a decision the design didn't make, or
  where you deviated, with the reason.
- Keep `docs/progress.md`: the current phase, what's done, what's next, and open questions. Update it at least at
  every commit. The orchestrating agent reads this file to follow your work.

## Stop and ask (write the question under "Open questions" in `docs/progress.md`, then keep working on anything not blocked)

- Any live Cloudflare deploy, plan against the live account, or real-cloud test.
- Adding a paid service or a dependency outside the iac catalog that changes the architecture.
- Changing a settled decision listed above.

## Done

- Every design section is implemented and listed in the coverage matrix.
- `vp run ready` passes: format, lint, type check, and all tests.
- `vp run dev` brings up the full Stack locally, and `tools/scenario-cli` runs every scenario green, with trace
  assertions.
- `vp run verify` passes every formal check, and each has been shown to fail on a broken variant.
- `docs/progress.md` ends with a hand-off: what was built, how to run it, what is verified and under which bounds,
  known gaps, and anything that needs a live deploy to confirm.
