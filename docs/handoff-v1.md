# shaledb v1 hand-off: from working previews to a production release

This document lets a new agent, on any machine, take shaledb from where it is (every design item
built, verified locally and on live previews) to a **v1 that Dillon can deploy to production**. Read
it in full before you change anything. The prompt to start the agent is at the end.

## Where things stand

Read these first, in this order:

| Document                                                                             | What it is                                                                                        |
| ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| `AGENTS.md`                                                                          | Project rules for agents.                                                                         |
| `docs/progress.md` → "Hand-off"                                                      | What was built, how to run it locally and on previews, verification bounds, live results.         |
| `docs/measurements.md`                                                               | Every live number, with its method and stage. The targets below are measured against it.          |
| `docs/design-decisions.md`                                                           | D1–D54. Continue from **D55**. D44–D54 are this round's live findings and decisions.              |
| `docs/coverage-matrix.md`                                                            | Every design item → code → tests, trace assertions, formal checks.                                |
| `docs/show-me-r2-lsm-design.html`                                                    | The design. `docs/implementation-brief.md`: the original brief; its settled decisions still hold. |
| `docs/coding-standards.md`, `docs/errors.md`, `docs/testing.md`, `docs/vite-plus.md` | Standards. Follow them exactly.                                                                   |

State at commit `cb405ff` (2026-11-25):

- `vp run ready` and `vp run verify` (20/20 TLC runs) pass. Six formal checks; property tests with
  mutation checks; a workerd integration test of the whole Stack.
- 11/11 scenarios pass under `vp run dev`, on `preview-1` (unrestricted), and on `preview-2`
  (`eu` jurisdiction), with trace assertions.
- `preview-1` and `preview-2` are deployed with the final code and production settings.
  `preview-3` is unused. Their Alchemy state is in the account's Cloudflare state store, so any
  machine logged in to the account sees them (`node tools/preview-cli/src/preview-cli.ts status`).
- Nothing has been deployed to `production`. There is no production deploy task yet.

## Set up on a new machine

1. Node ≥ 22.18 and the Vite+ CLI (`vp`). pnpm 12.14 is pinned through `devEngines`. Then
   `pnpm install`.
2. `git clone --depth 1 https://github.com/slatedb/slatedb.git repos/slatedb` (read-only reference,
   gitignored).
3. `vp run ready` must pass before you change anything.
4. `vp run verify` downloads a pinned JDK 21 and `tla2tools.jar` into `.tlc/` on macOS arm64; on any
   other platform install Java 11+ and set `JAVA`.
5. Cloudflare: log Alchemy in to Dillon's account `99459fce09d9a9eec83befd957af4ec5` (Alchemy's
   Cloudflare provider reads its login or the environment; see
   `node_modules/@alchemy.run/cloudflare/src/Credentials.ts`). Check with
   `node tools/preview-cli/src/preview-cli.ts status`: it must list `preview-1` and `preview-2`.
   Never commit credentials.
6. If pi's Cloudflare deployment guard runs on this machine, Dillon runs
   `scripts/allowlist-shaledb-deploys.sh` once (stages `dev`, `preview-1..3`). After it, the guard
   blocks only an `alchemy destroy` typed directly in a command; it does not resolve `vp run` tasks
   in this repository or its worktrees, so `destroy:preview-<n>` and `deploy:production` are not
   blocked (`docs/release-v1.md`, section 10). Its stage list does **not** keep agents off
   `production`; see "Production".
7. Traits: `.pi/mcp.json` configures the traits checker (needs `TYPESAFE_API_KEY` and a Claude Code
   login). It is a probabilistic per-chunk classifier: read its findings against
   `.agents/traits/*.md`; don't chase every one.
8. Local Stack: `node tools/trace-cli/src/trace-cli.ts serve &`, then `vp run dev`, then
   `node tools/scenario-cli/src/scenario-cli.ts run all`. Kill every `workerd` process afterwards.

## Rules

- **Approved:** live deploys to `preview-1`, `preview-2`, `preview-3` through `preview-cli`, and any
  live check, scenario, measurement, or soak on them. Reuse stages by redeploying; never create
  others.
- **Needs Dillon's explicit go-ahead in this session:** any production plan or deploy, and adding
  `production` to the deployment guard. Prepare everything; ask once at the end (see "Release").
- **Never:** `alchemy destroy` (give Dillon the commands), production resource names on a preview,
  credentials in commits or span attributes.
- **Compatibility:** `/v1` is the public contract. Changes are additive: new optional fields, new
  endpoints, new error variants with a decided status. Stored state must keep decoding: manifests,
  compactor state, writer SQLite, trace exports. Add schema defaults rather than migrations, and
  prove old data decodes with a test.
- **Formal models:** when you change a function a check covers, follow its
  `.agents/skills/verify-*` skill. Update the model so each action is still one awaited effect,
  rerun it, and show every broken variant still fails before moving its sync commit. For drift,
  follow `.agents/skills/maintain-formal-verification-skill/SKILL.md`. Workstreams 2, 3, and 4 touch
  covered code.
- **Every workstream ends with:** `vp run ready` and `vp run verify` passing (observed exit 0), the
  scenarios green locally and on a preview when it touches live behavior, `docs/progress.md`,
  `docs/design-decisions.md`, `docs/coverage-matrix.md`, and `docs/measurements.md` updated, then a
  conventional commit pushed to `origin main`. Commit inside a workstream as often as is useful.
- **Measure live, compare to `docs/measurements.md`.** A target below is met only when a preview with
  production settings shows it. Record the stage, method, and settings.

## Decisions Dillon must make (ask on day one, then keep working)

Write these under "Open questions" in `docs/progress.md` and ask Dillon once, at the start. Each has
a recommended default. If he hasn't answered by the time a workstream needs it, build the default,
record it as a decision, and keep it easy to change.

| #   | Decision                            | Recommended default                                                                                                                                                                               |
| --- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P1  | How production callers authenticate | Keep the access gate for production, with **rotatable** tokens: two valid tokens at a time, held as Secrets Store secrets, rotated by a `vp run` task. Cloudflare Access in front stays optional. |
| P2  | Production data residency           | None (`SHALEDB_JURISDICTION=none`). It is fixed per installation; changing it later means a new installation.                                                                                     |
| P3  | Production hostname                 | The API Worker's `workers.dev` URL. A custom domain needs a zone in the account; take the zone name if he gives one.                                                                              |
| P4  | Production telemetry                | The stage's own `TraceCollector`, with a 14-day R2 lifecycle rule and head sampling at 10% (100% on previews).                                                                                    |
| P5  | Plan and limits                     | Workers Paid. Raise the per-invocation CPU limit (`limits.cpu_ms`) of the API and compactor Workers to what workstream 6 measures plus 2× headroom.                                               |
| P6  | Release go-ahead                    | Dillon reviews the release checklist, adds `production` to the guard, and either says "deploy" or runs the deploy task himself.                                                                   |

## The work

Do the workstreams in order: later ones build on earlier ones. Each says what is wrong and why it
matters, the proper long-term fix, what it touches, and how you know it is done. Where a fix
supersedes a decision, record a new D-number that names the decision it replaces, and mark the old
one "superseded by Dnn".

### 1. The Stack owns its configuration (root cause of D54)

**Problem.** Engine settings (`SHALEDB_LSM_SETTINGS`) and the jurisdiction
(`SHALEDB_JURISDICTION`) reach Workers through Alchemy's `Config` interception. A value a deploy
stops setting stays bound, because Cloudflare keeps secrets across uploads. D54 patched this in
`preview-cli` by always passing both. Any other deploy path — a production task, a manual
`vp run …#deploy:*` — still gets stale values. For the jurisdiction, that means Workers addressing
the wrong Durable Objects.

**Proper fix.** One typed Stack configuration, decoded once in `infra/shaledb` at plan time from
the deploy's environment with a schema and complete defaults. Every Worker is bound the **full**
resolved value on every deploy, so "unset" can no longer mean "whatever was there before":

- `ShaledbStackConfig` (infra): `stage`, `jurisdiction` (`eu` | `fedramp` | `none`), the full
  `LsmSettings` (defaults merged with overrides), `telemetry` (sample rate, trace retention days),
  and access-token policy. Decode it with Schema from `process.env` in one place.
- Bind it explicitly onto each Worker. Either the Worker props' `env`, or `runtimeContext.set` as
  `lsmAccessGateFor` does. Workers read it back with the existing `Config` reads, so
  `lsm-cloudflare` stays unaware of infra.
- Bind the complete `LsmSettings` JSON, never a partial override. `makeLsmConfiguration` keeps
  accepting a partial value for tests and old deploys.
- `preview-cli` only translates flags into that environment. `deployEnvironment` stays as a thin
  adapter.
- Fail the plan if a setting combination is invalid, for example `transactionRetentionMs >= gcMinAge`
  (workstream 4).

**Done when.** A unit test decodes the config from environments with and without each value. A
preview deployed with `--dev-timings` and then without shows production values on every Worker
(read the bindings from state, as D54 did). The same holds when going `--jurisdiction eu` → `none` on
`preview-3`. Record D55, superseding D54's mechanism.

### 2. Fast manifest commits (live finding: ~1.1 s per CAS)

**Problem.** `ManifestStore.update` costs about 1.1 s on R2 (`docs/measurements.md`):

| Step                                                          | Cost    | Status                                |
| ------------------------------------------------------------- | ------- | ------------------------------------- |
| `loadLatest` (`LATEST` hint, the manifest, a probe of `id+1`) | ~300 ms | avoidable for warm committers         |
| create-if-absent PUT                                          | ~400 ms | the commit itself                     |
| GC-boundary read (RFC 0026, D33)                              | ~60 ms  | required                              |
| best-effort `LATEST` hint PUT                                 | ~420 ms | on the critical path, but only a hint |

Every flush, compaction commit, checkpoint, seal, and fork pays it. It is most of the 13–16 s split
pause.

**Proper fix**, keeping create-if-absent as the only thing that decides:

1. **Optimistic create from the committer's own view.** The writer and compactor already hold the
   newest manifest they committed. `ManifestStore.update` takes an optional starting point
   (`Option<VersionedManifest>`) and tries `create(id + 1)` directly. Only on `AlreadyExists` does it
   `loadLatest` and reapply the edit. Correctness is unchanged: a stale view can only lose the race.
2. **The `LATEST` hint leaves the critical path.** Write it in the background after the commit
   returns: `ctx.waitUntil` in Workers, a background fiber in Durable Objects, which outlive the
   response. Add a small `BackgroundWork` service with a faithful test layer. The hint may lag;
   readers already probe past it, and GC never trusts it.
3. **Measure R2 placement.** Puts take ~400 ms from where the Durable Objects run. Measure PUT
   latency per region with a probe. Add a `locationHint` option to the Stack config if the bucket's
   default location is far from the writers. Only for unrestricted Stacks; check what R2 allows with
   a jurisdiction.

Model impact: `ManifestStore.update/create` are covered by `verify-manifest-cas-fencing` (and
`verify-gc-safety` for the boundary). Model the optimistic first attempt from a possibly stale view,
and a hint write that may land late or never. Keep every broken variant failing. Add one more: a
create that skips the boundary re-read, which must fail.

**Targets.** `ManifestStore.update` p50 ≤ 500 ms and a flush of a small memtable ≤ 900 ms on
`preview-1` with production settings. Record before and after in `docs/measurements.md`.

### 3. Splits pause writes briefly and never fail them

**Problem.** A live split pauses the shard's writes 13–16 s. The router waited only 5 s, so writes
failed; the window is now 30 s (D48), but the router busy-polls the directory every 250 ms, and the
pause itself is long. On top of the CAS cost, each fork waits ~2 s for its new compactor to fence in
(`CompactorClient.wake` inside `forkRange`).

**Proper fix.**

- **Wake forks' compactors without waiting.** `forkRange` hands the wake to background work (as in
  workstream 2). A periodic sweep in the GC/maintenance path (workstream 5) wakes the compactor of any
  database whose manifest still has `externalRefs`, so a lost wake only delays the fork's rewrite,
  never its correctness.
- **Router waits on the directory, not a timer.** Add `ShardDirectory.awaitMap(group, afterVersion)`:
  a long poll that the directory Durable Object answers as soon as a newer map is published, or
  when the split it is waiting on fails, bounded by a timeout. On `DatabaseSealed` or `KeyOutOfRange`,
  the router waits on it instead of sleeping. A split that fails mid-way leaves the directory's
  pending split resumable; the router surfaces `ShardedRequestUnavailable` (retryable) only when the
  bounded wait ends.
- With workstream 2, the split's critical path is one small flush plus about seven fast CAS
  operations.

Model impact: `verify-seal-split` covers the seal/checkpoint/fork order; if the order of awaited
effects changes, update the model. Routing stays outside the model.

**Targets.** Split write pause p50 ≤ 6 s, zero failed writes, with `scenario-cli measure split-pause`
on `preview-1` (4 and 16 writers).

### 4. Transactions are exact across flushes (replaces D41/D50's limit)

**Problem.** A transaction aborts with `SnapshotTooOld` if it outlasts a whole flush (D50). It is
rare for short transactions, but long or slow-network transactions hit it, and it is a semantic
limit users must learn.

**Proper fix: retain the newest flushed rows in the writer's SQLite**, so the writer can still check
them, and pin transaction reads to the manifest seen at begin. There are no R2 reads in the commit
lock, and no extra row writes; deletes only happen later.

- **Memtable invariant.** Add `retained_from_seq` beside `flushed_seq`, with
  `retained_from_seq ≤ flushed_seq`, and every row with `seq > retained_from_seq` present in
  SQLite. A flush still writes only rows with `flushed_seq < seq ≤ cut`, then sets
  `flushed_seq = cut` in the same SQLite transaction as today. It no longer deletes those rows.
- **A separate, atomic trim.** It deletes rows older than the retention policy and raises
  `retained_from_seq`. Policy: keep rows flushed within `transactionRetentionMs` (new setting;
  default 60 s, and it must be `< gcMinAge`), bounded by `maxRetainedBytes` (new; default 64 MiB),
  and always at least the newest flush.
- **Latest-path queries stay as they are.** `probe`, `scan`, `changes`, unflushed bytes,
  backpressure, and flush streaming filter `seq > flushed_seq`, so latest reads, the change feed,
  and backpressure behave exactly as today.
- **Begin** returns `readSeq` and the manifest id the writer holds with `lastL0Seq == flushed_seq`,
  read consistently with `readSeq`. On the public API this is an additive `manifestId` in the
  `transactions/begin` response, accepted as an optional field by `transactions/get`, `scan`, and
  `commit`.
- **Reads at `readSeq`.** Retained-or-unflushed memtable rows with `seq ≤ readSeq`, plus the begin
  manifest (kept by GC for `gcMinAge`). They are exact while `retained_from_seq ≤ beginManifest.lastL0Seq`;
  otherwise `SnapshotTooOld`.
- **Commit.** `SnapshotTooOld` only if `retained_from_seq > readSeq`. Otherwise
  `MemTable.writtenAfter` checks all rows with `seq > readSeq`.
- **Restart** keeps retained rows and their horizon (persisted with the rows).
- **Clients without `manifestId`** (old callers) keep today's behavior.

Model impact: rewrite `Transactions.tla` with the retained horizon, trim, pinned-manifest reads, and
a `Compact` action that collapses versions. Add a broken variant that trims past an open
transaction without aborting it, which must fail `Serializable`. Update `NoAckedWriteLost.tla`:
truncate becomes advance-then-trim; `LatestSeesAcked` must still hold. Update both skills.

**Done when.** The property test in `db-transaction.test.ts` interleaves transactions with flushes
and compactions and checks serializability, with mutations caught. On `preview-1`,
`measure transactions` runs transactions held open for 20 s across several flushes, with zero
`SnapshotTooOld`. Record D56 superseding D41 and D50.

### 5. GC and maintenance that scale with the number of databases

**Problem.** `GarbageCollector.collectAll` lists every `db/` prefix and collects every database in
one cron invocation (concurrency 4). With thousands of databases it will exceed subrequest, CPU, or
wall-time limits and silently collect nothing past the failure. The GC cron is also the only
periodic maintenance.

**Proper fix.** Per-database maintenance belongs to the per-database actor:

- The database's `CompactorServer` runs its GC (`collect(databaseId)`) from its own alarm: after
  compactions and flushes leave garbage, and at `gcInterval` while the manifest shows anything
  collectable (superseded manifests, unreferenced SSTs, expired checkpoints). An idle database
  without garbage sets no alarm and costs nothing.
- The cron becomes a **paged sweeper** for what has no actor: prefixes left by removed databases
  (today's sweep), and waking compactors of databases with `externalRefs` (workstream 3). It keeps a
  cursor and processes at most N databases per run.
- The operator API keeps `POST /v1/gc/databases/:id` (collect now); `collectAll` becomes "sweep a
  page".

Model impact: `verify-gc-safety` covers the collection functions. Calling them from the compactor
does not change their steps. If you overlap a GC run with a compaction commit on the same database,
check that the model already interleaves them; extend it if not.

**Done when.** A test runs the sweeper across many databases with a page size and shows every
database visited across runs. On a preview, create 500 databases with garbage and show it collected
without any invocation exceeding limits (tail records: `outcome` stays `ok`).

### 6. Writer throughput and per-invocation limits

**Problem.** One writer is CPU-bound at ~12k rows/s with 1,000-row batches and ~2.7k rows/s with
100-row batches. About 15–20 ms of every batch is fixed cost: HTTP plus Schema decoding of base64
JSON, twice (API Worker, then writer). Design §11 asked to settle binary bodies before v1 routes
freeze. CPU limits are the defaults, and a 64 MiB flush costs 3.5–6 s of CPU.

**Proper fix**, measurement first:

- Profile one batch's CPU split in workerd (decode, SQLite insert, response) with a benchmark under
  `tools/`, and confirm on a preview with the tail records.
- If decoding is a large share, switch the **internal** `WriterHttpApi` batch body to a compact
  binary encoding: length-prefixed keys and values with varints, reusing `byte-codec.ts`. The API
  Worker and `WriterServer` deploy in one script, so there is no compatibility window. Keep the
  public JSON body. Add an optional public `application/x-shaledb-batch` body later only if
  callers need it; it would be additive.
- Measure CPU per compaction step and per flush at production sizes with the tail Worker. Set
  `limits.cpu_ms` on the API and compactor Workers (P5) with 2× headroom, in the Stack config.
- Measure the two items design §12 left open:
  - Does Durable Object SQLite's page cache count against 128 MB? Load a writer with
    `maxUnflushedBytes` of rows and watch for `exceededMemory`.
  - What is the Workflows concurrent-instance limit? Start N compactions at once.

**Targets.** Record rows/s at 100- and 1,000-row batches before and after. If the binary protocol is
adopted, the 100-row case should at least double. No `exceededCpu` or `exceededMemory` outcome in
any measurement or the soak.

### 7. Corruption: prevent it, detect it, repair it

**Problem.** D53 made a corrupted input a typed, reported compaction outcome, and the compactor
stops compacting it. But nothing prevents corruption at upload, and an operator has no supported
way to recover. A corrupted L0 SST pauses L0 compaction until it is repaired, and writes eventually
stall.

**Proper fix.**

- **Integrity at upload.** Single-part puts pass `sha256` so R2 rejects a body that changed in
  transit. Multipart uploads compute each part's MD5 (a private ~60-line MD5, like the private
  CRC32C) and compare it with the part's returned ETag. A mismatch retries the part. Faithful in the
  in-memory object store.
- **A supported repair, explicit and typed.** `DatabaseLifecycle.dropSst(databaseId, sstId, { confirm })`
  and an operator endpoint, gated. It removes an SST the compactor has reported corrupted from the
  manifest by CAS, names the key range whose older versions are lost, records the drop in the
  manifest's history (an additive field), and wakes the compactor. Refuse SSTs that are not
  reported corrupted, unless `force`.
- **Runbook.** In the operations doc (workstream 9): find the report (`GET …/compaction`), decide
  between restoring from a checkpoint (fork the full range into a new database) and `dropSst`, and
  verify.

**Done when.** Tests cover a corrupted part being retried, and `dropSst` unblocking L0 compaction
after the corruption scenario in `compactor.test.ts`. A scenario exercises the operator endpoint
locally.

### 8. Production telemetry

**Problem.** Traces are designed for debugging previews.

- Effect's HTTP middleware puts `client.address` and `user_agent.original` (personal data) on server
  spans.
- Traces are kept forever.
- Every request is traced.
- The telemetry probe Worker would ship to production.

**Proper fix.**

- **Redact at the source.** Wrap the tracer installed by `lsmTelemetryLayer` and the Stack's
  telemetry so an allowlist of span attributes is exported: project attributes plus safe HTTP ones
  such as method, route, and status. Drop everything else, including `client.address`,
  `user_agent.original`, and query strings in `url.full`. Prove it with a trace assertion
  (`Attribute` with `forbiddenKeys`) in a scenario, locally and on a preview.
- **Head sampling** from the Stack config (P4): 100% on local and preview stages, a fraction in
  production. Make it parent-based, so a sampled request's spans across Workers and Durable Objects
  stay together.
- **Retention.** A `TraceBucket` lifecycle rule expires `traces/` and `tails/` after the configured
  days.
- **No probe in production.** The telemetry probe deploys to local and preview stages only.
- **Metrics that matter.** Document which existing metrics and spans to watch and alert on:
  `WriteStalled` rate, flush duration, compaction backlog (L0 count), corrupt SSTs, GC failures,
  `exceededCpu`/`exceededMemory` from tail records. A small `trace-cli` or `scenario-cli` report over
  a preview's tail records and spans gives a health summary.

### 9. Production readiness

- **Authentication (P1).** Rotatable access tokens:
  - The gate accepts any of the active tokens, compared in constant time per token.
  - Tokens are Secrets Store secrets the Stack binds.
  - A `vp run` task rotates one token while the other stays valid.
  - `preview-cli token` keeps working.

  If Dillon picks Cloudflare Access instead, keep the gate and add Access in front.

- **Production tasks.** `plan:production` and `deploy:production` in `infra/shaledb/vite.config.ts`
  (`cache: false`):
  - They decode the Stack config with production values (P2–P5) and use production names and
    retention (`LsmBucket` keeps `shaledb` and is retained).
  - The trace bucket and every preview-only resource are absent or stage-appropriate.
  - Nothing runs them until P6.
- **Public contract.**
  - Generate a checked-in OpenAPI spec from `DbHttpApi` (`vp run generate:openapi`, per
    `docs/coding-standards.md`), with examples, summaries, the error envelope, `x-request-id`, and
    the auth scheme. Inspect the diff on every contract change.
  - Write down the `/v1` compatibility policy.
- **Format versions.** Add an additive `formatVersion` (default 1) to the manifest and the compactor
  state, and refuse to write a manifest older code can't read. Prove old manifests decode.
- **Documentation.**
  - A `README.md`: what shaledb is, quick start, the API, links.
  - `docs/operations.md`: deploy, rollback (redeploy the previous commit; Durable Objects and data
    are untouched), token rotation, jurisdiction policy, corruption runbook, GC and removal,
    capacity numbers from `docs/measurements.md`, alerts.
- **Clean up** what was preview scaffolding but is now permanent: the GC operator API's exposure,
  scenario-only defaults, and comments that say "until Phase N".

### 10. Soak, release checklist, and the go-ahead

1. **Soak on `preview-1`** with production settings, at least 2 hours. Add `scenario-cli soak`:
   - Several databases.
   - Mixed batched writes, `Latest`/`AtLeast`/`Flushed` reads, scans, and transactions.
   - Two splits, checkpoints and pinned snapshots, removals.
   - Compaction and GC running, and a redeploy midway to evict every Durable Object.
   - A checker that records every acknowledged write and, at the end, reads every key back at
     `Latest` and `Flushed`.

   Pass criteria:
   - No acknowledged write lost or hidden.
   - No unexpected error status in traces.
   - No `exceededCpu`/`exceededMemory`.
   - Storage bounded after GC.
   - p99 latencies recorded.

2. **Repeat the soak's short form on `preview-2`** (`eu`).
3. **Release checklist** in `docs/release-v1.md`:
   - Every workstream done.
   - Targets met, with links to `docs/measurements.md`.
   - Formal checks and their bounds.
   - The soak report.
   - The open-question answers (P1–P6).
   - The exact production commands.
   - What to watch in the first day.
   - How to roll back.
4. **Ask Dillon for P6** with that checklist. On his go-ahead, he adds `production` to the guard (or
   runs the task himself). Run `plan:production`, show him the plan, then `deploy:production`. Smoke
   test with the scenarios that are safe in production: write-read, flush, transactions, block-cache,
   create-race, each on its own uniquely named databases, removed afterwards. Record the result.
5. **Final hand-off** in `docs/progress.md`: what changed, what was verified, the production URLs,
   and the destroy commands for the preview stages (Dillon runs them).

## Definition of done for v1

- Workstreams 1–9 are done with their targets met on a preview with production settings; the soak
  passed on both previews.
- `vp run ready` and `vp run verify` pass. Every model touched was updated, rerun, and its broken
  variants fail.
- The public contract is documented (OpenAPI, compatibility policy). Old manifests and state decode.
- `docs/operations.md`, `README.md`, and `docs/release-v1.md` exist and match the code.
- Dillon has the release checklist, and either production is deployed and smoke-tested with his
  go-ahead, or it is one command away and waiting on him.

## Prompt for the new agent

```text
You're finishing shaledb v1: a SlateDB-style LSM key-value store on Cloudflare R2 and Durable
Objects, written with Effect 4 and Alchemy v2, already built and verified locally and on live preview
stages. Read docs/handoff-v1.md in full first, then AGENTS.md, the "Hand-off" section of
docs/progress.md, docs/measurements.md, and the standards it lists. Set up the machine per "Set up on
a new machine" and confirm `vp run ready` passes before changing anything. On day one, write the
P1–P6 decisions into docs/progress.md and ask Dillon once; keep working on the recommended defaults
while you wait. Then do the ten workstreams in order, autonomously: proper long-term fixes, not
patches. Measure every target on a preview with production settings; keep the formal models in step
with covered code through the verify-* skills; keep docs/progress.md, docs/design-decisions.md
(from D55), docs/coverage-matrix.md, and docs/measurements.md current; and commit and push to
origin main after each workstream with `vp run ready` and `vp run verify` passing. Live deploys are
approved for preview-1, preview-2, and preview-3 only. Never plan or deploy production without
Dillon's explicit go-ahead in this session, and never run `alchemy destroy`. Finish with the
release checklist and ask Dillon for the production go-ahead.
```
