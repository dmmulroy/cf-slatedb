# Progress

## Current phase

**v1 for production** ([`docs/handoff-v1.md`](handoff-v1.md)): ten workstreams, then the release
checklist and Dillon's go-ahead (P6). The decisions asked of Dillon are under "Open questions".

| Workstream                                | Status      | Commit               |
| ----------------------------------------- | ----------- | -------------------- |
| 1. The Stack owns its configuration       | done        | `012bdb5`            |
| 2. Fast manifest commits                  | done        | `774ed7b`            |
| 3. Splits pause briefly, never fail       | done        | `393758c`            |
| 4. Transactions exact across flushes      | done        | `5f2850b`, `5df4099` |
| 5. GC and maintenance that scale          | done        | `e06f3eb`            |
| 6. Writer throughput and limits           | done        | `308defd`            |
| 7. Corruption: prevent, detect, repair    | done        | `2cb5116`            |
| 8. Production telemetry                   | done        | `62eab3d`            |
| 9. Production readiness                   | done        | `8a96667`            |
| 10. Soak, release checklist, the go-ahead | in progress |                      |

The previous round (all ten build phases and `docs/handoff.md`'s seven items) is complete; its
hand-off is below.

## v1 log

- **Set up (v1 session, 2026-11-25).** Node 24.22, `vp` 1.8, pnpm 12.14; `repos/slatedb` and
  `.tlc/` present; Alchemy logged in to the account (`preview-cli status` lists `preview-1` and
  `preview-2`). Before any change: `vp run ready` exit 0, `vp run verify` exit 0 (20/20). P1–P6
  written under "Open questions" and asked.
- **1. The Stack owns its configuration (D55, supersedes D54's mechanism).** `ShaledbStackConfig`
  (infra) is resolved once from the deploy's environment with complete defaults (stage,
  jurisdiction, complete `LsmSettings`, telemetry, access mode) before the Stack declares anything; a
  refused variable fails the plan. Every Worker layer takes a `settings` source and binds the
  complete settings and an explicit jurisdiction last in its init (`bindLsmWorkerSettings`), so no
  deploy path can leave an earlier value bound. `LsmSettings` refuses contradictory combinations.
  `preview-cli` only translates flags; `preview-cli bindings <stage>` prints each Worker's bound
  values. **Live:** `preview-1` `--dev-timings` → a bare `vp run …#deploy:preview-1` with no
  variables → every Worker bound the complete production settings and `none` (D54's failure mode,
  now fixed in the Stack); `preview-3` (first deploy) `eu` → `none` → every Worker `none`.
  **Finding (fixed):** that move failed to delete the replaced EU trace bucket (`BucketNotEmpty`);
  both buckets are now `forceDestroy` outside production, and the orphan was emptied once through
  the R2 API. Scenarios: 11/11 locally; `write-read` and `transactions` on `preview-1`. All three
  previews redeployed with the final code (`preview-1` and `preview-3` unrestricted, `preview-2`
  `eu`; production settings).
- **2. Fast manifest commits (D56).** `ManifestStore.updateFrom`: the writer's flushes and seal create
  `view.id + 1` from the manifest they last committed, without loading; only a written commit is
  trusted from that view (a conflict, failure, or no-op reloads). The `LATEST` hint is advanced in
  the background (`BackgroundWork`: `waitUntil` in Workers and Durable Objects, inline in Workflow
  steps) and conditionally, so it never moves back. **Finding (fixed):** `loadLatest` probed from the
  hint alone, so a lagging hint plus a checkpoint keeping manifest `h` while GC deleted `h + 1` made
  it stop at `h`; it now probes from `max(hint, boundary + 1)`, reading both together, and GC no
  longer rewrites the hint. Model: GC deletes, hint writes in any order or never, the optimistic
  attempt; new invariants `CommitOnce`, `LoadFindsHead`; new broken variants `noBoundaryReread`,
  `hintOnlyProbe` fail; 23/23 TLC runs. **Live (`preview-1`, production settings):**
  `ManifestStore.update` p50 1,092 → 357 ms (target ≤ 500), small flush p50 1,508 → 709 ms (target
  ≤ 900). **R2 placement:** create-if-absent ~285 ms even beside the bucket (ENAM, from IAD), ~510 ms
  from the US west coast, ~550–590 ms from Europe, ~900–1,000 ms from APAC/Oceania; `preview-1`'s
  writers are already near its bucket. `SHALEDB_BUCKET_LOCATION` (default `auto`) lets an
  installation place its bucket once. Scenarios: 11/11 locally and on `preview-1`.
- **3. Splits pause briefly and never fail (D57).** One manifest commit seals the shard, flushes
  the last rows, and takes each successor's hold; the forks start from their holds concurrently
  with no commit on the shard, and are warmed (writer fence-in, compactor wake) in the background;
  routers long-poll the directory (`awaitMap`) instead of polling every 250 ms, and stop waiting at
  once when a split reports failure. A lost CAS race is now a span outcome, not an error status.
  **Live (`preview-1`, production settings):** write pause median 2.7 s (4 writers), 3.1 s (16),
  from 13–16 s; 0 failed writes; split ~2 s. Model: `SealSplit`'s seal takes the holds. 11/11
  scenarios locally and on `preview-1`.
- **4. Transactions exact across flushes (D58, supersedes D41/D50).** The writer keeps flushed rows
  (`retained_from_seq`) until a separate trim (60 s, 64 MiB, never the newest flush); transactions
  begin with the flushed manifest and read it plus the kept rows; a commit aborts as too old only
  if its rows were trimmed. Additive public `manifestId` on `transactions/begin`, `get`, and `scan`;
  old callers and old writer state keep the previous behavior. Models: `Transactions` rewritten
  (new broken variants `trimWithoutAbort`, `noReadHorizonCheck`), `NoAckedWriteLost` with `Trim`
  (`trimUnflushed`). **Live (`preview-1`, production settings):** 24/24 transactions held ~21.7 s
  across 6–7 flushes committed, 0 `SnapshotTooOld`. 11/11 scenarios locally. **Process slip:** the
  live scenario run after deploying it failed `fork-split`, and the commit (`5f2850b`) went out
  before I read the result; the cause was not D58 but a latent routing bug, fixed in the next
  commit (D59) before any further work.
- **Finding (fixed, D59):** a router isolate with a stale map read a sealed shard and missed a write
  just acknowledged by a new shard (intermittent `fork-split` failure live). Routed `Latest` reads
  now refuse sealed databases (`refuseSealed`) and wait on the directory like writes; scans resume
  shard by shard. `fork-split` 5/5 and 11/11 scenarios on `preview-1`.
- **5. GC and maintenance that scale (D60).** Each database's compactor collects its garbage from
  its own alarm (due `gcMinAge` after a compaction, then when the last collection says something
  becomes collectable; idle databases set no alarm), one collection at a time; "collect now" runs on
  the compactor. The cron is a paged maintenance sweep (50 databases per run, stored cursor) for
  removed databases' leftovers, fork compactor wakes, and piled-up manifests. **Live (`preview-1`,
  dev timings):** 500 databases all collected by their own compactors; 15,517 of 15,535 invocations
  `ok`, 18 runtime-`canceled` alarms and Workflow calls, none over a limit. Also fixed: the local
  cleanup never matched `alchemy dev`'s process, so stacks piled up between runs (environment only).
  11/11 scenarios locally and on `preview-1`.
- **6. Writer throughput and limits (D61).** Each write ran a memtable stats scan to schedule the
  flush. It now reads in-memory counters. 100-row batches went from 2,454 to 6,547 rows/s, and
  1,000-row batches from 7,678 to 15,257; the writer is busy about half the time. The binary batch
  body is declined, because decoding is 14% of the write path (`tools/writer-bench`). Measuring at
  production sizes found two real failures, both fixed:
  - **A 60 MiB flush exceeded 30 s of CPU.** D58's seq-bounded queries made SQLite build a temp
    B-tree per page. The fix is `+seq` bounds and a cursor resume, with a query-plan test.
  - **Compaction hit the per-invocation request limit.** It made one `getRange` per 4 KiB block.
    `groupBlockReads` now reads 2 MiB chunks, with a property test.

  The CPU limits come from the Stack config (P5): the API at 30 s, which is 2 × the 9.8 s peak for a
  256 MiB flush, and the compactor at 60 s, which is 2 × the 17.7 s peak per compaction step. A
  first deploy resolved the config inside the Worker and failed every request with 1101. Limits are
  now resolved only at plan time.

  Design §12's two open questions were answered on `preview-3`:
  - **Memory:** 256 MiB unflushed, then flushed, gave no `exceededMemory`.
  - **Workflows:** 200 compactions started at once all finished within 16 s.

  New tooling: `preview-cli deploy --settings` and `measure writer-memory|workflow-burst`. Status
  `l0Count` now comes from the newest manifest.

  Results: 11/11 scenarios passed locally and on `preview-1` (`--dev-timings`). Both previews were
  then redeployed with production settings.

- **7. Corruption: prevent, detect, repair (D62).**
  - **Prevent.** Single-part puts send their SHA-256. Multipart parts are checked against the MD5
    R2 returns as their ETag and re-sent on a mismatch. A probe through R2's S3 API confirmed both
    behaviors.
  - **Repair.** An operator can drop a reported-corrupted SST with
    `POST /v1/databases/:id/ssts/:sstId/drop`, where `confirm` repeats the SST id. The compactor
    makes the change under a lock shared with planning, refuses while a job reads the SST's source,
    records the drop in `droppedSsts`, and compaction resumes. Two new operator views support the
    restore-or-drop decision: compaction status `corruptSsts` (with each SST's creation time) and
    `GET …/checkpoints`. The runbook is in the new `docs/operations.md`.
  - **Probe Worker.** It has a `damage` endpoint for the new `repair` scenario and is now deployed
    on local and preview stages only.
  - **Model.** `GcSafety` gained `DropSst` and the broken variant `dropDeletesObject`.
  - **Finding (fixed before commit).** A private JavaScript MD5 tripled 60 MiB flush CPU in workerd
    (p50 3.7 s, max 16.7 s), though it is fast in Node. The native WebCrypto MD5 is back at baseline
    (p50 1.0 s, max 3.9 s), and all 134 parts were verified on R2.
  - **Results.** 12/12 scenarios locally and on `preview-1` (`--dev-timings`), which was then
    redeployed with production settings.
- **8. Production telemetry (D63).**
  - **Redaction.** Every Worker's tracer is wrapped with a policy that exports only project
    attributes plus HTTP method, route, path, and status. The new `redaction` scenario proves it
    locally and on `preview-1`.
  - **Sampling.** Head sampling is decided from the trace id, at 10% in production and 100%
    elsewhere. The finding behind it: honouring incoming sampled flags kept every trace, because
    instrumented clients send the flag. Measured on `preview-3` at 25%: 45 of 200 kept, all whole
    across the API Worker and the writer Durable Object.
  - **Retention.** The trace bucket expires `traces/` and `tails/` after 14 days, and the LSM
    bucket aborts stale multipart uploads. Both are confirmed on `preview-3`.
  - **Health report.** "What to watch" is in `docs/operations.md`, and `measure health` reports a
    stage's health. Its first run found the D61 deploy incident's seven exceptions.
  - **First attempt, fixed.** It overrode `Telemetry` inside the init context, which never reaches
    Alchemy's bridge. The local scenario caught it.
  - **Results.** 13/13 scenarios pass locally and on `preview-1`.
- **9. Production readiness (D64).**
  - **Tokens.** Each live stage has two rotatable tokens in the account's Secrets Store. Workers
    bind them by reference and re-read them every 30 s, and the `tokens` task ensures, checks, and
    rotates them. Previews keep their state token as well. Rotation was verified live on
    `preview-1`.
  - **Production tasks.** `plan:production` and `deploy:production` exist, and the deploy refuses
    without tokens. Nothing has run them.
  - **Contract.** The OpenAPI spec is checked in, generated with examples conforming by
    construction, the bearer scheme, and `x-request-id`. A test fails on drift.
  - **Compatibility and formats.** The `/v1` compatibility policy is written down. Format versions
    are on the manifest and the compactor state; newer data is refused, never rewritten. A real
    workstream 1 manifest is checked in as a fixture.
  - **Documentation.** `README.md` is new and `docs/operations.md` is complete. The dead preview
    gate is removed.
- **Renamed to shaledb (D68).** Every project reference changed: packages, paths, the Stack,
  environment variables, bindings, and live names. Upstream SlateDB references are kept. The
  previews are redeployed as new `Shaledb/<stage>` stacks; the old `CfSlatedb/<stage>` stacks
  remain for Dillon to destroy.
- **10. Soak (in progress; D65–D67).**
  - **New tools.** `scenario-cli soak` and `scenario-cli smoke`; the smoke test passed on
    `preview-3`.
  - **The soak's first runs** lost or hid no acknowledged write, but found a defect, now fixed
    (D66): the API Worker's block cache shared reads in flight across requests, which caused
    5-minute hangs and a 500 during a redeploy.
  - **Fixed in the soak tool:** request timeouts, retryable sharded errors, and trace pulls that
    keep up.
  - **Also made:** scans now read ahead 8 blocks per SST (D67), the collector serves pages of
    500, and the shard router has tail records.
  - **Sampling.** The soak runs at production's 10% sampling; at 100% a busy preview exports
    faster than it can be read back.
- **10, continued: read-path work validated and fixed (D67, D69).**
  - **What it does.** A read the writer answered uses any cached manifest that holds the writer's
    flushed rows, for up to half of `gcMinAge`, instead of reloading the manifest every second.
    The writer's probe says where the newest manifest starts. Point reads and scans open their
    SSTs together, scans read ahead 8 blocks per SST, and blocks default to 16 KiB.
  - **Two defects found in review before it was committed, both fixed.** The first version cached
    the manifest the writer named (its last flush), which a compaction has usually superseded:
    on a database idle after a compaction, a new isolate would have read it for up to 30 minutes
    while GC deleted its SSTs. It also let an `AtLeast` read be up to 30 minutes stale. The soak
    could show neither (constant writes, fresh receipts). Now the writer's manifest is only where
    the search for the newest starts (`loadLatestFrom`), and an `AtLeast` read skips the writer
    only while a `Flushed` read would use the same manifest; otherwise it asks the writer.
  - **Model.** `ManifestCasFencing` gained `LoadFromKnownFindsHead` and the broken variant
    `knownIgnoresBoundary`; `vp run verify` is 27 runs. The other three skills record why no step
    changed.
  - **Also fixed (D69).** A request its endpoint's schemas refuse returned 500; it is now
    `InvalidRequest` (400) naming the malformed part. `DbWriter.start` spans now mean an actual
    start. The scenario runner reports what caused a failed request.
  - **Verified.** `vp run ready` exit 0; `vp run verify` exit 0 (27/27); 13/13 scenarios under
    `vp run dev` (exit 0). Eleven mutations of the new read path are each caught by a test.
  - **The transport error, found.** The first request of a scenario sometimes failed with
    "other side closed": while the runner blocked on trace assertions, the Workers closed its
    idle connections. The runner now reopens them before each scenario; three full local runs
    since passed.
- **10, continued: experiments run in parallel on the three previews (D70–D74).** Dillon asked
  for experiments, measurements, and prototypes in parallel. Each prototype was built and tested
  in its own git worktree; each A/B ran both variants on two stages in opposite order; deploys
  from the main checkout were serialized.
  - **A correctness bug, found by adding compaction to the model (D70).** A `Latest` read could
    load a manifest flushed and compacted past the snapshot the writer gave it, and return "not
    found" for a key that always had a value. Present since Phase 5; needs a rewrite, a flush,
    and a compaction inside the tens of milliseconds between the writer's answer and the manifest
    load; no soak showed it. A read now refuses such a manifest and asks the writer again. A
    transaction scan from a caller without a begin manifest had a related gap, also fixed.
  - **A flush writes only the versions a read can reach (D70).** One row per rewritten key
    instead of one per write: the soak's databases stored 0.44 MB instead of 4.4 MB.
  - **Reads (D72).** A manifest refresh in one round trip from the manifest held; a slow block
    read hedged with a read straight from the bucket; scans stop reading when they have their
    rows. `preview-3`, p50 / p90 / p99 in ms, before → after: `Flushed` get 81 / 162 / 515 →
    5 / 113 / 243; scan 76 / 555 / 2,146 → 60 / 257 / 983; block fetch p99 439 → 101.
  - **Flushes (D73).** The compactor wake left the flush (it waited 5 to 9 s for an idle
    compactor); blocks are encoded a chunk of rows at a time. 16 KiB blocks cost a flush no more
    CPU than 4 KiB. Concurrent part uploads were measured and declined.
  - **The writer's alarm and `status` scanned the memtable (D74).** Two different figures for
    "unflushed bytes" made the alarm fire, scan, and be set again without flushing: a 267 MB flush
    took 200 s instead of 20 s, and two such writers at once were reset. Every transaction begin
    paid the same scan through `status` (1.58 s of CPU on a full memtable). One figure and
    counters now; six runs of two full writers at once: 2 alarms each, no reset.
  - **Telemetry (D71).** Tail records are sampled like traces, except those that did not end
    `ok`. Unsampled, a busy stage wrote them as fast as they could be read back.
  - **Corrected.** D61's and D62's "flush CPU p50" (about 1 s for 60 MiB) was a median over
    alarm and request records, each part of a flush. Per flush it is about 4 s. The limits were
    set from the largest single invocation and stand.
  - **The 2-hour soak on the D67 code did not report.** Its workload ran its 130 minutes; its
    final checks then waited behind the tail-record backlog (D71's finding) for another 80
    minutes, and it was stopped because the code it tested was superseded. What its pulled spans
    show: 12,997 sampled gets and 879 scans (6,016 and 399 after minute 70), 16 sampled
    collections that deleted 48 SSTs, and no error span other than the remover's deliberate
    `DatabaseNotFound`. The release soaks are rerun on the final code.
  - **The soak tool can no longer lose its results (D65).** A later 130-minute soak never
    reported: its tail watcher's read never returned, which held the workload open, and the
    report was only printed at the end. The watchers are now interrupted when the workload ends,
    every telemetry read has a budget whose overrun is a failure saying how far it got, the data
    checks run before telemetry, and `--report-file` keeps the report, atomically replaced, every
    minute and after each phase. Tests: `soak-telemetry.test.ts`, `soak-report.test.ts`,
    `scenario.test.ts` in `tools/scenario-cli`. A split or redeploy planned after `--minutes` is
    skipped instead of holding the workload open (no test). Not yet run live.
  - **Lessons for running in parallel.** TLC on every core starves a client-driven measurement
    on the same laptop (transport errors, one failed run): run it niced and apart from timed
    runs. A tool started while a deploy has the checkout on another ref loads a mix of two
    trees. A shell script edited while it runs re-executes lines.
  - **Verified.** `vp run ready` exit 0, `vp run verify` exit 0 (28/28), 13/13 scenarios locally
    and on `preview-3` (`--dev-timings`), 5-minute soaks passing on `preview-2` and `preview-3`
    for each arm. Thirty-five mutations of the new code are each caught by a test.
- **Review of D67 and D69 to D74 (branch `proto/review`).** The review agent checked the day's
  changes against the standards (no defect found) and then fixed what it found.
  - **D74, the writer's counters.** The counter refresh at the end of a flush installs its scan
    only if no commit ran during it. Before, a commit landing between the scan and the install
    could lose its row from the counters and leave it with no alarm. A flush that writes
    nothing settles counters that still show unflushed rows.
  - **D72, hedged reads.** No hedging in a restricted Stack (`eu` measurements: 90% of block
    reads hedged, 2% won). D72 is corrected: the losing read is interrupted, and hedging has a
    connection-queue limit.
  - **Tests and small fixes.** New tests cover the hedged source's wiring and `ensureReady`'s
    fast path. D70's "always fits" sentence is corrected, along with three standards points.
  - **Verified.** `vp check` and `vp test run` exit 0 in `packages/lsm-core` and
    `packages/lsm-cloudflare`. Each new test fails on the code before or under a named mutation
    (`/tmp/shale-work/agents/review/REPORT-2.md`). `vp run ready` and `vp run verify` are left
    to integration.
- **Production telemetry reads (release review finding).** `trace-cli pull`/`tail` and
  `scenario-cli measure health`/`latency` resolved their stage only through `preview-cli`, so
  nothing could read production's spans, tail records, or health. Each now also takes
  `--collector-url <url>` (the `traceCollectorUrl` output) with the token in
  `SHALEDB_TRACE_TOKEN`, and fails before any request without a token, or with both or neither
  forms. `preview-cli trace-token <stage>` (any live stage, `production` included) writes only the
  collector token into a pipe or command substitution and refuses a terminal; `deploy` still
  accepts the three previews alone. A pull resumes from any cursor in the store for the same
  collector URL, so the two forms never store an export twice. `measure` pulls by URL with the
  token in the trace CLI's environment. `docs/operations.md` shows the production and preview
  forms side by side. Not yet tried against a live collector.
  - **A fork keeps its hold while a reader may hold a manifest that reads the source (D77).** A
    fork released its hold seconds after its rewrite. Its snapshot from before the rewrite, cached
    manifests, and fence-in transactions then failed once the source collected or was removed.
    A fork of a fork could also release its hold on the middle database before its own rewrite.
    The fork's own collection now releases the hold, once no manifest GC keeps names another
    database's SST. The source of a split lives about `gcMinAge` longer. Tests in
    `database-lifecycle.test.ts`, `garbage-collector.test.ts`, and `manifest.test.ts` fail on the
    code before. `GcSafety` gains `GcSafetyFork.cfg` (`ForkReadsComplete`, broken variant
    `earlyForkRelease`). Not yet run in workerd or on a preview.
- **D78: the compactor fenced itself (found live, 2026-11-26).** `measure manifest-cas` on
  `preview-3` stalled database `ms-cas-mwy5ujbj` for good. Two cold wakes each fenced in, and a
  poll wrote the older epoch back, so the compactor was `fenced` with no newer compactor to take
  over. Now the fence-in is single-flight, every compactor state write is a read-modify-write,
  and a fenced compactor fences in again on its next start.
  - Five new `compactor.test.ts` tests; the first reproduced the stuck state on the code before
    the fix. Six mutations are each caught.
  - `manifest-cas-fencing` model check: 9/9 (no step changed).
  - Still to do live: wake `ms-cas-mwy5ujbj` after the deploy and confirm it compacts.
    `earlyForkRelease`). Not yet run in workerd or on a preview. More tests followed:
    - a kept manifest that cannot be loaded releases nothing;
    - a release interrupted between its two steps is finished by the next collection;
    - a split half split again before its rewrite keeps its holds down the chain.

    D77 and `operations.md` state how long a middle database of a fork chain holds the original
    owner: at most one sweep cycle (`ceil(P / 50)` runs, 5 minutes apart) plus `gcMinAge` after its
    fork's release.

- **D81: compaction folded merge operands across TTLs (found by review, 2026-11-26).** A
  short-TTL operand made the whole folded key expire early. Compaction now folds only rows that
  expire together (SlateDB's write-path rule).
  - A new property compares reads at every later time through flushes and two compactions; it
    fails on the code before.
  - Five mutations are each caught.
  - Already-compacted data keeps its early expiry; the release notes need a sentence (see D81).
- **D80: the change feed's silent gap (found by review, 2026-11-26).** With every changelog entry
  pruned, or on a fork before its first flush, the feed skipped changes without `Expired`. Now the
  manifest records `changelogAfterSeq`, and the feed answers only when its changelog provably holds
  every change after `afterSeq`.
  - The four new tests failed before the fix; seven mutations are each caught.
  - A fork's feed starts at its fork point. The split procedure is in D80 and the OpenAPI text.

- **Review, round 4 (D83, branch `proto/review`).**
  - **Transactions model.** A transaction read may use any manifest the reader's cache holds
    (36.9M states, 167 s).
  - **Two test gaps closed.** A transaction read the reader cannot yet serve is retryable, not
    too old. A transaction begun before a seal cannot commit after it.
  - **Two concurrent collections** are argued in D60. A collection that cannot load a retained
    manifest above GC's boundary deletes nothing. Gated tests cover both.
  - **Read failures** now say how to retry, resume a scan, or take a new snapshot.
  - **TTL bounds under clock skew** are in D83 and the README.
  - **Verified.** `vp check` exit 0. `vp test run` in `packages/lsm-cloudflare` exit 0 (149
    tests). `verify.ts transactions` and `verify.ts gc-safety` exit 0. Each new test fails under
    a named mutation (`/tmp/shale-work/agents/review/REPORT-4.md`).

- **Audit round 2, lifecycle (D90, D91).**
  - **Recreating a removed id** served the removed database's data to warm readers. An id is now
    reusable only `gcMinAge` after its removal, with a new `DatabaseRecentlyRemoved` (409,
    retryable).
  - **A commit in flight during a removal** brought the database back as `Open` (or `Sealed`) and
    let its writer acknowledge writes. The `Removed` manifest now stays at the head as a tombstone
    for `gcMinAge`; the sweep deletes it.
  - **A property test** applies every manifest edit as a state machine.
  - **Model:** `ManifestCasFencingRemove.cfg`.
  - **Not run:** workerd, scenarios, previews.

- **Decisions D76 to D108 and D123, one line each** (the decisions hold the detail; the audit
  agents' reports, under `/tmp/shale-work/agents/`, hold the evidence).
  - **D76:** a transaction reads the newest manifest when it fits; a snapshot is taken at the
    newest manifest (found by checking `docs/invariants.md` against the code).
  - **D79:** the sharded API answers refused requests with the public envelope and is outside
    `/v1`'s contract.
  - **D82:** every signal the operations guide names is read from a span or a tail record;
    metrics are emitted and not collected.
  - **D84:** a `Snapshot` commit re-reads the horizon after its conflict queries (a lost update).
  - **D85:** a seal that does not return forgets the writer's phase; an alarm for a removed
    database stops.
  - **D86:** a compaction's `prepare` step records its inputs' ids, not their handles (the 1 MiB
    step result).
  - **D87:** settings must leave L0 enough SSTs to compact before writes stop.
  - **D88:** TTL and lifetime bounded at 100 years; truthful answers to retried requests.
  - **D89:** a shard router refreshes a map older than a second before a `Flushed` or `AtLeast`
    read.
  - **D92:** Cloudflare's platform limits against shaledb's demand; settings the platform cannot
    hold are refused.
  - **D93:** a `Latest` scan pulls the writer's pages as it needs them; every memtable page is
    bounded by bytes; the change feed pages whole batches.
  - **D94:** a compaction job unseen for `compactionLeaseMs` is finished.
  - **D95:** a seal naming other successors than the recorded ones fails.
  - **D96:** a collection keeps what a running compaction may have written.
  - **D97:** a compaction step is bounded by the input it merges.
  - **D98:** a collection that retains too many manifests defers SST deletion and still makes
    progress.
  - **D99:** bounds on checkpoints, the changelog, merged values, names, and the manifest; the
    compactor's subrequest limit.
  - **D100:** every request is bounded: the body (16 MiB), every string, bytes, and array field.
  - **D101:** a request that waits out an abandoned split finishes it.
  - **D102:** one alarm retry rule, with backoff, for the writer and the compactor; the writer's
    view and its alarm are set by one party at a time.
  - **D103:** a compaction's commit records its job; a missing Workflow instance is finished; what
    a large compaction needs (the database size bound by Workflow steps).
  - **D104:** what the first production deploy creates, its tokens, and its smoke test (read, never
    run).
  - **D105:** rollback against the stored formats, with fixtures for `f3b2ad2` and `c3807f7`.
  - **D106:** the soak exercises the classes of defect the v1 audits found.
  - **D107:** an SST's metadata stays a small fraction of its data, whatever its key size.
  - **D108:** a scan page examines at most 16 MiB and ends early, resumably.
  - **D123:** span events pass the trace policy, so no log line's object parts or annotations
    export a key or value.
  - **Still on their branches:** the formal models (D109 and on) and the mutation sweeps (D117 to
    D122).
- **Mutation sweep of `lsm-core` (D117, D118).**
  - 800 mutants. 286 survived the first pass. Tests for the untested edges were added. 82 survive:
    80 equivalent, 2 performance-only (block seek).
  - **Found:** no wrong behavior. The weakest tests were the merge properties: short generated
    histories never filled the heap enough for a wrong parent index to misorder rows.
  - **Pinned:** the Bloom filter's stored bits, the SST footer's seq bounds, and the documented
    limits and defaults.
  - **`BlockServer`:** every error answer is `no-store` (D118).
  - **Verified:** `vp check` exit 0. `vp test run` exit 0 in `packages/lsm-core` (141 tests) and
    `packages/lsm-cloudflare` (247 tests).
  - **Not run:** workerd, scenarios, previews, TLC (no protocol code changed).

## Remaining-work log

- **Set up (new session).** Node 24.22, `vp` 1.8, pnpm 12.14; `repos/slatedb` and `.tlc/` present;
  the deploy guard already allowlists `Shaledb` for `dev`, `preview-1..3`. `vp run ready` exit 0
  before any change.
- **1. Portable Alchemy state.** `shaledbStateLayer` picks `localState()` for local stages and
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
  4. **Measurements** (production write-path settings; see D54 for the GC and compactor timings
     that stayed bound): a writer is CPU-bound at ~2.7k rows/s (100-row
     batches) to ~12.3k rows/s (1,000-row batches); a flush costs ~55–90 ms CPU per MiB; a split
     pauses the shard's writes ~13–16 s, because each manifest CAS is ~1.1 s on R2.
     **Finding (fixed):** the shard router's 5 s retry window assumed a local split; live, every write
     arriving early in a 13 s split failed with `DatabaseSealed`. Now 30 s; the re-run had 0 failed
     writes (D40, D48). **Finding (recorded):** the best-effort `LATEST` hint is ~40% of every CAS and
     the forks' compactor wake-ups add ~4 s to a split; moving them off the critical path is left for a
     change that re-runs the manifest CAS model. CPU comes from a new live-only Tail Worker (D48).
- **5. Data residency, per Stack.** `SHALEDB_JURISDICTION` (`eu` | `fedramp` | unset;
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
- **6b. L1 cache bounded by bytes per budget group (D51).** `GroupByteBudgetLru` (lsm-core, property
  tested, four mutations caught) behind `BlockCache`: 32 MiB data and 16 MiB metadata per isolate,
  half per `budgetGroup`, heaviest group evicts first under pressure; joined misses and
  never-kept failures preserved (`block-cache.test.ts`). The old metadata bound counted entries,
  each up to ~1 MB.
- **6c. Pinned snapshots (D52).** `pinSnapshot` / `POST …/snapshots/pinned { lifetimeMs }` pins a
  snapshot with an expiring checkpoint; plain snapshots stay free and their `gcMinAge` limit is
  documented, with `SnapshotExpired` pointing to pinning.
- **6d. Corrupted SSTs in compaction (D53).** A corrupted input ends the job with a typed
  `Corrupted` outcome (no defect, no retry loop); the compactor reads job outcomes, records the SST
  in its state (`GET …/compaction`), counts and logs it, and never plans its source again until the
  SST leaves the manifest.
- **Finding (fixed, D54):** a `Config`-bound setting that a later deploy stops setting stays bound on
  the Workers (Cloudflare keeps secrets across uploads), so `preview-1` kept the dev timings after
  redeploys without `--dev-timings`. The preview CLI now binds `SHALEDB_LSM_SETTINGS` (`{}` by
  default) and `SHALEDB_JURISDICTION` (new value `none`) on every deploy; verified in the
  Workers' bindings. `docs/measurements.md` states which settings applied to its runs.
- **Standards review** (`158b6ad`): the traits checker over the new code; the access gate's `401`
  and the trace collector's errors now use the public error envelope (declared in `DbHttpApi`, no
  vendor messages), the tools' errors carry typed context, optional Stack outputs decode to
  `Option`, and a trace pull's cursor advances per export.
- **7. Finish.** Both previews redeployed with the final code (`preview-1` unrestricted, `preview-2`
  in `eu`, production settings); this hand-off.

## Done

- **Phase 0 — repository setup** (`b3a4e84`). pnpm workspace with iac's catalog (effect 4.1.4,
  alchemy and `@alchemy.run/cloudflare` 2.0.0-beta.87, vite-plus 1.2.0, typescript ^7), iac's lint
  rules and tsconfig, `.node-version`, `.gitignore`, workspaces `packages/lsm-core`,
  `packages/lsm-cloudflare`, `packages/lsm-sharding`, `infra/shaledb`,
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
  - `lsmTelemetryLayer` (`@shaledb/lsm-cloudflare`) wraps `Telemetry.layerOtlp`.
  - OTLP flush bug **confirmed on beta.87**. Alchemy is not patched (review). Workaround (D5): a
    stage-selected flush policy. Local stages (`dev`, `test`, `test-*`, ...) export a handler's spans
    before the response; live stages (`production`, previews, ...) add no export latency and rely on
    the bridge's `waitUntil` flush. Both modes are covered by unit tests and by workerd tests
    (`telemetry-smoke.workerd.ts`, `telemetry-smoke-live-policy.workerd.ts`).
  - `infra/shaledb/alchemy.run.ts` + `vp run dev` run a telemetry probe Worker + DO under
    `alchemy dev`; verified with the trace CLI (Worker → DO span nesting, alarm span,
    `assert` pass/fail exit codes).
  - `@shaledb/lsm-cloudflare` `test` now runs unit tests and the workerd suite (`--mode workerd`).
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

### What was built

shaledb, a SlateDB-style LSM key-value store on Cloudflare R2 and Durable Objects:

- `packages/lsm-core` (pure): ids, rows and resolution (merge operators, TTL), CRC32C, Bloom filters,
  block and SST codecs, the k-way merge, the manifest and its pure edits, the size-tiered scheduler
  (with blocked sources), the compaction merge, GC planning, the compaction budget ledger, write
  batches, read consistency, transactions, and the byte-budget LRU behind the L1 cache.
- `packages/lsm-cloudflare`: `ObjectStore` (R2), manifests (CAS, RFC 0026 boundary), the writer
  (`WriterServer`; the SQLite memtable is the WAL), the reader (`DbReader` in the caller's isolate;
  L1 `BlockCache` bounded by bytes per `budgetGroup`; `BlockServer` behind Workers Cache),
  compaction (`CompactorServer`, `CompactionWorkflow`, `CompactionBudgetServer`; corrupted inputs
  reported and quarantined), GC, `DatabaseLifecycle` (checkpoints, pinned snapshots, seal, forks,
  removal), `DbClient`, the public `DbHttpApi`, the access gate for live stages, and the Stack's
  jurisdiction.
- `packages/lsm-sharding`: range-sharded groups over the public APIs.
- `infra/shaledb`: the Alchemy Stack. On live stages it adds a `TraceCollector` Worker, a
  `CpuTailWorker`, and per-stage access and trace tokens; state lives in the account's Cloudflare
  state store.
- `tools/`: `trace-cli` (with `pull`/`tail --stage`), `scenario-cli` (11 scenarios; `--stage`;
  `measure`), `preview-cli`, `formal-verification`, `workspace-boundaries`, `traits`.

### How to run it

Locally (no Cloudflare account needed):

```sh
vp run ready                                              # format, lint, types, every test
vp run verify                                             # 20 TLC runs, about 2 minutes
node tools/trace-cli/src/trace-cli.ts serve &             # OTLP receiver on 127.0.0.1:4318
vp run dev                                                # API :1337, blocks :1338, compactor :1339, GC :1340, shards :1341
node tools/scenario-cli/src/scenario-cli.ts run all       # 11 scenarios with trace assertions
```

Stop every `workerd` process `vp run dev` started when you finish.

On a preview (`preview-1`, `preview-2`, `preview-3`; reuse them, never `production`):

```sh
P="node tools/preview-cli/src/preview-cli.ts"
$P deploy preview-1 --dev-timings                      # in place; production settings without the flag
$P deploy preview-2 --jurisdiction eu                  # eu | fedramp | none; kept on later deploys
$P status                                              # what is deployed where (any machine)
$P url preview-1 --json; $P token preview-1            # URLs; the access token for Authorization: Bearer
node tools/scenario-cli/src/scenario-cli.ts run --stage preview-1 all
node tools/trace-cli/src/trace-cli.ts pull --stage preview-1   # then list/show/grep/stats/assert
node tools/scenario-cli/src/scenario-cli.ts measure writes|flush-cpu|split-pause|transactions --stage preview-1
```

### What is verified, and under which bounds

Formal checks (`vp run verify`, each with a skill in `.agents/skills/verify-*` recording bounds,
assumptions, covered code, and sync commit; every check fails on its deliberately broken
variants):

1. No acknowledged write lost or hidden (`NoAckedWriteLost.tla`): 4 writes, 2 crashes, 2 reads.
2. Fencing and 3. manifest CAS linearizability (`ManifestCasFencing.tla`): 2 writers and 2
   compactors over 9 manifest ids, plus a race config for changed compaction inputs.
3. GC safety (`GcSafety.tla`): 2 SSTs, clock 0–3, 5 manifests, interleaved GC steps.
4. Seal and split (`SealSplit.tla`): 3 keys, 4 writes, every interleaving of seal, checkpoint, forks.
5. Transactions (`Transactions.tla`): 2 keys, 2 transactions, 6 commits; serializable in commit
   order. It already lets flushes interleave anywhere, which covers D50's window.

The models assume one writer instance per database (the Durable Object guarantee), durable SQLite
commits before acknowledgement, and R2 create-if-absent. The last is now confirmed on real R2.
Nothing in this round changed a covered function; each skill records the re-check.

Code-level properties (codecs, merge, compaction merge, GC plans, ledger, shard map, the client and
change feed against oracles, transactions, fault injection, the byte-budget LRU) are property tests
with mutation checks; `docs/coverage-matrix.md` maps every design item to code and tests.

### Live results (Dillon's account, 2026-11-25)

Details and methods are in [`docs/measurements.md`](measurements.md).

- **Workers Cache:** `HIT` on `BlockServer` for a block another Worker had read (`MISS` first).
- **R2 create-if-absent (`Headers` form):** 16 concurrent creates → 1 `Created`, 15 `AlreadyExists`.
- **Scenarios:** 11/11 on `preview-1`, 11/11 on `preview-2` in `eu`, 11/11 under `vp run dev`.
- **Residency:** Cloudflare reports both buckets and the Workflow in `eu` on `preview-2`.
- **Traces:** spans from Worker fetches, Durable Object fetches, and alarms all arrive (after D47).
- **Measurements:**
  - Writes: one writer is CPU-bound at ~2.7k rows/s (100-row batches) up to ~12.3k rows/s
    (1,000-row batches) for 100-byte values.
  - Flush: ~55–90 ms CPU per MiB.
  - Manifest CAS: ~1.1 s on R2.
  - Split: writes to the splitting shard pause ~13–16 s, with no failed writes after the router fix.
  - Transactions: 580/580 committed through a 28.7 s flush.
- **Findings fixed live:**
  - Worker spans were lost without `global_fetch_strictly_public` (D47).
  - The router's 5 s split window was too short for a live split (D48).
  - Unset settings stayed bound on Workers across deploys (D54).

### Anything left (planned in `docs/handoff-v1.md`)

- Shorten every manifest CAS ~40% by writing the `LATEST` hint off the critical path, and make the
  forks' compactor wake asynchronous (~4 s per split). Both touch code the manifest CAS model covers;
  re-run `verify-manifest-cas-fencing` with the change (`docs/measurements.md`).
- Removing D50's transaction limit for transactions longer than a flush: design recorded in D50.
- Drop `client.address` and `user_agent.original` from server spans before any production deploy
  (Findings).
- Not measured: whether Durable Object SQLite's page cache counts against 128 MB, and the Workflows
  concurrent-instance limit on the plan.
- A production deploy task, if ever approved, must bind `SHALEDB_LSM_SETTINGS` and
  `SHALEDB_JURISDICTION` explicitly (D54) and keep production names and retention
  (`lsm-bucket.ts`).

### Destroying the previews (Dillon runs these; agents never do)

All three previews are deployed (`preview-3` since v1 workstream 1).

```sh
vp run @shaledb/infra#destroy:preview-1
vp run @shaledb/infra#destroy:preview-2
vp run @shaledb/infra#destroy:preview-3
```

`node tools/preview-cli/src/preview-cli.ts destroy-command <stage>` prints the same. Each removes
the stage's Workers, Durable Objects, Workflow, both buckets (data and traces, emptied first: they
are `forceDestroy` outside production since D55), and its state records; nothing in them is
retained.

## Findings

- A transaction's begin manifest is the writer's last flush manifest (`flushed_manifest_id`). A
  compaction replaces it within seconds, and on a database idle for `gcMinAge` after a compaction,
  with its writer still up, GC collects it: every transaction read then fails with `SnapshotTooOld`,
  and every retry gets the same manifest until the next flush. Liveness only; nothing is read
  wrongly. Found while checking `docs/invariants.md` (2026-11-26), reproduced in a unit test, not yet
  fixed. Also: an unpinned snapshot is guaranteed half of `gcMinAge` from when it is taken, not the
  full hour `DbClient.snapshot`'s doc and the OpenAPI description say.

- Effect's HTTP server middleware records `client.address` (the caller's IP) and
  `user_agent.original` on server spans. They reach the trace store on live stages. They are not
  keys, values, or secrets, but they are personal data; before a production deploy, drop them
  from server spans or keep traces in a store with matching retention.

- alchemy 2.0.0-beta.87 still flushes event telemetry inside `waitUntil` after the event settles;
  worked around in application code (D5), no patch.
- Effect 4.1.4 names HTTP spans by method (`GET`); the trace CLI shows span kind (D7).
- No Java or mise on this machine; `vp run verify` downloads a pinned JDK and `tla2tools.jar` into
  `.tlc/` (gitignored) and passes `JAVA`/`TLA2TOOLS` to `/statechart` runs.
- The traits MCP checker (`.pi/mcp.json`) works in pi sessions; it reviewed the code of this round
  (`158b6ad`). It is a probabilistic classifier per chunk, so read its findings against the trait
  files rather than chasing every one.

## Open questions

### v1 decisions (asked 2026-11-25; Dillon delegated P1–P5)

Asked once, at the start of the v1 round (`docs/handoff-v1.md`). Dillon answered: decide with best
judgement for the product, UX, and performance. P1–P5 are therefore decided as below (each recorded
as a decision when its workstream builds it). P6 stays Dillon's: production is deployed only on his
explicit go-ahead.

| #   | Decision                            | Recommended default (building this until answered)                                                                                                           | Answer               |
| --- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------- |
| P1  | How production callers authenticate | The access gate, with rotatable tokens: two valid at a time, held as Secrets Store secrets, rotated by a `vp run` task. Cloudflare Access in front optional. | decided: the default |
| P2  | Production data residency           | None (`SHALEDB_JURISDICTION=none`). Fixed per installation; changing it later means a new installation.                                                      | decided: the default |
| P3  | Production hostname                 | The API Worker's `workers.dev` URL. A custom domain needs a zone in the account.                                                                             | decided: the default |
| P4  | Production telemetry                | The stage's own `TraceCollector`, a 14-day R2 lifecycle rule, head sampling at 10% (100% on previews).                                                       | decided: the default |
| P5  | Plan and limits                     | Workers Paid; `limits.cpu_ms` on the API and compactor Workers set to workstream 6's measurement plus 2× headroom.                                           | decided: the default |
| P6  | Release go-ahead                    | Dillon reviews `docs/release-v1.md`, adds `production` to the deploy guard, then says "deploy" or runs the deploy task himself.                              | pending              |

Related to P2 (found in workstream 2, D56): the production LSM bucket's R2 location is fixed when it
is created. `SHALEDB_BUCKET_LOCATION` defaults to `auto` (R2 places it near the machine that
deploys). If production callers are concentrated somewhere else (for example Europe), choose that
region (`weur`) before the first production deploy; writes cost ~285 ms beside the bucket and up to
~1 s from the far side of the world (`docs/measurements.md`).

The two optional measurements and the optimizations of the last round's open questions are now
workstreams 2, 3, and 6 of the v1 plan.

Resolved this round: Workers Cache hit and R2 create-if-absent confirmed live; data residency is
per Stack (Dillon's choice, D49); the §12 write, flush, and split measurements are recorded.
