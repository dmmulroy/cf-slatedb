# shaledb v1 release checklist

The checklist Dillon reads before deciding P6 (`docs/handoff-v1.md` §10.3). Every claim here names
the file, commit, test, model, or measurement it comes from, so it can be checked in the
repository. A line starting `PENDING(<id>):` is a result still being produced. It is not a claim
until the line is replaced.

PENDING(final-commit): the release commit hash on `main` that this checklist describes, with
`vp run ready` and `vp run verify` observed at exit 0 on it.

## 1. Decision asked

You are asked to approve deploying the commit above as the `production` stage of the `Shaledb`
Stack in account `99459fce09d9a9eec83befd957af4ec5`. Production has never been planned or deployed.
What it deploys:

- a retained LSM bucket named `shaledb`;
- no jurisdiction (P2);
- the API Worker's `workers.dev` URL (P3);
- 10% trace sampling with 14-day retention (P4);
- CPU limits of 30 s for the API Worker and 60 s for the compactor (P5);
- two rotatable access tokens in the account's Secrets Store (P1);
- no telemetry probe Worker.

On your go-ahead, the following happens in order (section 10). You create the production tokens.
`production` is added to the deployment guard, or you run the tasks yourself. `plan:production` is
run and its plan shown to you. `deploy:production` runs. `scenario-cli smoke` runs against the new
URL and removes its own databases. The result is recorded in `docs/progress.md`. If you do not give
the go-ahead, nothing changes: production stays one command away.

## 2. Workstreams 1 to 10

Workstreams 1 to 9 and the first part of 10 are on `main` (up to `f3b2ad2`); the rest of 10 is on
the integration branch `proto/integration-2` (up to `fec2698`), and D77 and D78 are on their own
branches until merged. Status comes from the workstream table in `docs/progress.md`.

| #   | Workstream                            | Done                          | Commit(s)                                                                                                                                                                                                                                                   | Decisions                        |
| --- | ------------------------------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| 1   | The Stack owns its configuration      | yes                           | `012bdb5`                                                                                                                                                                                                                                                   | D55 (supersedes D54's mechanism) |
| 2   | Fast manifest commits                 | yes                           | `774ed7b`                                                                                                                                                                                                                                                   | D56                              |
| 3   | Splits pause briefly, never fail      | yes                           | `393758c`                                                                                                                                                                                                                                                   | D57                              |
| 4   | Transactions exact across flushes     | yes                           | `5f2850b`, `5df4099` (the D59 routing fix)                                                                                                                                                                                                                  | D58, D59                         |
| 5   | GC and maintenance that scale         | yes                           | `e06f3eb`                                                                                                                                                                                                                                                   | D60                              |
| 6   | Writer throughput and limits          | yes                           | `308defd`                                                                                                                                                                                                                                                   | D61                              |
| 7   | Corruption: prevent, detect, repair   | yes                           | `2cb5116`                                                                                                                                                                                                                                                   | D62                              |
| 8   | Production telemetry                  | yes                           | `62eab3d`                                                                                                                                                                                                                                                   | D63                              |
| 9   | Production readiness                  | yes                           | `8a96667`                                                                                                                                                                                                                                                   | D64                              |
| 10  | Soak, release checklist, the go-ahead | no: soaks pending, P6 pending | `a506228`, `fe552f0`, `18aa655`, `a98eb15`, `93c337c`, `04df0f9`, `9776317`, `5bedcbd`, `c72e0f3`, `c9e7487`, `4082f97`, `335b775`, `af51ae2`, `3d9f70f`, `dddd49a`, `8a36402`, `6f9c4ab`, `fec2698`, `ab5b929`, this checklist; PENDING(d77), PENDING(d78) | D65–D78                          |

`docs/progress.md` still lists workstream 9's commit as "this commit"; it is `8a96667`
(`feat(release): rotatable Secrets Store tokens, production tasks, …`).

## 3. Targets

Every target `docs/handoff-v1.md` sets, with what was measured. Measurements are on preview stages
with production settings unless the row says otherwise. Links go to `docs/measurements.md`.

| Target (handoff section)                                                                                                                                                               | Measured                                                                                                                                                       | Met                                 | Where                                                                                                                                                                     |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| WS1: a deploy without overrides binds production values on every Worker                                                                                                                | `preview-1` `--dev-timings` then a bare `deploy:preview-1`: every Worker bound the complete production settings and `none`                                     | yes                                 | D55; `docs/progress.md` v1 log item 1                                                                                                                                     |
| WS1: `eu` → `none` on `preview-3` binds `none` everywhere                                                                                                                              | every Worker bound `none`                                                                                                                                      | yes                                 | D55                                                                                                                                                                       |
| WS2: `ManifestStore.update` p50 ≤ 500 ms on `preview-1`                                                                                                                                | 357 ms (from 1,092 ms)                                                                                                                                         | yes                                 | [Manifest commits and small flushes](measurements.md#manifest-commits-and-small-flushes-v1-workstream-2-d56)                                                              |
| WS2: small-memtable flush p50 ≤ 900 ms on `preview-1`                                                                                                                                  | 709 ms (from 1,508 ms)                                                                                                                                         | yes                                 | [same section](measurements.md#manifest-commits-and-small-flushes-v1-workstream-2-d56)                                                                                    |
| WS2: R2 PUT latency per region measured                                                                                                                                                | create-if-absent ~285 ms beside the bucket to ~1 s from APAC; `SHALEDB_BUCKET_LOCATION` added                                                                  | yes                                 | [R2 latency by region](measurements.md#r2-latency-by-region-v1-workstream-2-d56)                                                                                          |
| WS3: split write pause p50 ≤ 6 s, zero failed writes, 4 and 16 writers                                                                                                                 | median 2.7 s (4 writers) and 3.1 s (16); 0 failed writes in six runs                                                                                           | yes                                 | [Split write pause after D57](measurements.md#split-write-pause-after-d57-v1-workstream-3)                                                                                |
| WS4: transactions held open 20 s across several flushes, zero `SnapshotTooOld`                                                                                                         | 24/24 committed, each held ~21.7 s across 6–7 flushes, 0 `SnapshotTooOld`                                                                                      | yes                                 | [Transactions held open across flushes](measurements.md#transactions-held-open-across-flushes-v1-workstream-4-d58)                                                        |
| WS5: 500 databases with garbage collected, no invocation over a limit                                                                                                                  | 500/500 collected by their own compactors; 15,517 of 15,535 invocations `ok`, 18 runtime `canceled`, 0 `exceeded*`. Run with `--dev-timings` (`gcMinAge` 10 s) | yes, with dev GC timings            | [Garbage collection across 500 databases](measurements.md#garbage-collection-across-500-databases-v1-workstream-5-d60)                                                    |
| WS6: rows/s at 100- and 1,000-row batches recorded before and after                                                                                                                    | 100 rows: 2,454 → 6,547 rows/s; 1,000 rows: 7,678 → 15,257 rows/s                                                                                              | yes                                 | [Writer throughput and limits](measurements.md#writer-throughput-and-limits-v1-workstream-6-d61)                                                                          |
| WS6: 100-row rate at least doubles if the binary protocol is adopted                                                                                                                   | binary body not adopted (decoding is 14%); the 100-row rate rose 2.7× without it                                                                               | not applicable; doubled anyway      | [same section](measurements.md#writer-throughput-and-limits-v1-workstream-6-d61), D61                                                                                     |
| WS6: `limits.cpu_ms` set from measurement with 2× headroom (P5)                                                                                                                        | API 30 s (2 × 9.8 s peak, a 256 MiB flush); compactor 60 s (2 × 17.7 s peak step). Largest single invocation since: 7.7 s (a flush)                            | yes                                 | [same section](measurements.md#writer-throughput-and-limits-v1-workstream-6-d61); [D73's sweep](measurements.md#flush-and-compaction-cpu-block-size-and-part-uploads-d73) |
| WS6: does DO SQLite's page cache count against 128 MB                                                                                                                                  | not observable directly; a writer at 256 MiB unflushed, and two at once (six runs), never reported `exceededMemory`                                            | answered indirectly                 | [Not measured](measurements.md#not-measured); [D74 section](measurements.md#the-writers-alarm-and-status-under-a-full-memtable-d74)                                       |
| WS6: Workflows concurrent-instance limit                                                                                                                                               | the limit itself not found; 200 compactions at once all finished within 16.4 s                                                                                 | answered indirectly                 | [Not measured](measurements.md#not-measured)                                                                                                                              |
| WS6: no `exceededCpu` or `exceededMemory` in any measurement                                                                                                                           | one `exceededCpu` (a 60 MiB flush under D58's queries) found and fixed in D61; none in any measurement since                                                   | yes for measurements; soaks pending | [Writer throughput and limits](measurements.md#writer-throughput-and-limits-v1-workstream-6-d61)                                                                          |
| WS7: a corrupted part retried; `dropSst` unblocks L0 compaction; operator scenario                                                                                                     | `object-store.test.ts`, `compactor.test.ts`, the `repair` scenario locally and on `preview-1`; 134 of 134 live parts verified                                  | yes (tests, scenario)               | D62; [Upload integrity cost](measurements.md#upload-integrity-cost-v1-workstream-7-d62)                                                                                   |
| WS8: redaction proven by a trace assertion, locally and on a preview                                                                                                                   | `redaction` scenario passes locally and on `preview-1`; a 6,193-span local run carries only allowed keys                                                       | yes                                 | [Trace sampling and redaction](measurements.md#trace-sampling-and-redaction-v1-workstream-8-d63)                                                                          |
| WS8: head sampling keeps traces whole                                                                                                                                                  | at 25%: 45 of 200 kept, all whole across the API Worker and the writer                                                                                         | yes                                 | [same section](measurements.md#trace-sampling-and-redaction-v1-workstream-8-d63)                                                                                          |
| WS9: OpenAPI spec checked in; old manifests decode                                                                                                                                     | `docs/openapi/shaledb-v1.json` with a drift test; a workstream 1 manifest fixture decodes as format 1                                                          | yes (tests)                         | D64                                                                                                                                                                       |
| WS10: soak ≥ 2 h on `preview-1`: no acknowledged write lost or hidden, no unexpected error status, no `exceededCpu`/`exceededMemory`, storage bounded after GC, p99 latencies recorded | see section 5                                                                                                                                                  | pending                             | section 5                                                                                                                                                                 |
| WS10: the soak's short form on `preview-2` (`eu`)                                                                                                                                      | see section 5                                                                                                                                                  | pending                             | section 5                                                                                                                                                                 |

The numbers for workstreams 2, 3, and 6 were measured before D70–D78 and the counter fix changed
the read, flush, writer, and compactor paths. They are re-measured on the final code:

PENDING(step4): re-measured on the final code, each with stage, settings, and runs, and added to
`docs/measurements.md`: `measure writes` at 100- and 1,000-row batches (rows/s, latency, writer
CPU); `measure split-pause` with 4 and 16 writers (longest gap, failed writes); `measure
compaction-cpu --megabytes-per-flush 60` (CPU per flush, per compaction step, outcomes); `measure
flush-cpu` (CPU per MiB); `measure latency` after a 5-minute soak (get `Latest`/`AtLeast`/`Flushed`,
scan, write, commit, p50/p90/p99). Each with the target it is checked against and met or not.

PENDING(baseline): the comparison with plain Durable Object SQLite: what was measured on each side
(workload, stage, rows/s, read and write latency), the commit and branch of the baseline (the
`proto/baseline` worktree), and what the difference means for a caller choosing between them.

## 4. Formal checks

`vp run verify` runs every entry of `FORMAL_CHECKS` in
`tools/formal-verification/src/formal-checks.ts`, over five specs. The code as written must pass,
and each broken variant must violate the invariant named. Bounds are in the `.cfg` files in
`tools/formal-verification/specs/`. Each check has a skill under `.agents/skills/verify-*` that
records its assumptions and sync commit.

All models assume three things: one writer instance per database (the Durable Object guarantee),
durable SQLite commits before an acknowledgement, and R2 create-if-absent. The last is confirmed on
real R2 by the `create-race` scenario.

The table is `formal-checks.ts` as integrated (`ddb63dd`, 31 runs listed).

| Spec and config              | What it proves                                                                                                                                                                                                                     | Bounds                                                                      | Broken variants (must fail)                                                                                                                                               | Runs |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---: |
| `ManifestCasFencing.cfg`     | Fencing and manifest CAS linearizability: one manifest per id, a commit lands once, no fenced writer or compactor commits, acknowledged data stays visible, the reader finds the head from the hint and from any known manifest    | 2 writers, 2 compactors, 1 job each, manifest ids up to 9, 2 flushes, no GC | `noWriterEpochCheck` → `NoStaleCommit`; `blindCreate` → `CreateOnce`                                                                                                      |    3 |
| `ManifestCasFencingRace.cfg` | The same, with a compaction whose inputs changed before it committed                                                                                                                                                               | 1 writer, 1 compactor, 2 jobs, ids up to 8, 2 flushes                       | `noInputsCheck` → `AckedDataVisible`                                                                                                                                      |    2 |
| `ManifestCasFencingGc.cfg`   | The same while GC deletes manifests at or below an advancing boundary and hint writes land late or never                                                                                                                           | 1 writer, 1 compactor, 1 job, ids up to 7, 2 flushes, GC on                 | `noBoundaryReread` → `CommitOnce`; `hintOnlyProbe` → `LoadFindsHead`; `knownIgnoresBoundary` → `LoadFromKnownFindsHead`                                                   |    4 |
| `NoAckedWriteLost.cfg`       | No acknowledged write is lost across eviction, crash between upload and commit, flush, trim, and compaction; a `Latest` read sees every acknowledged write                                                                         | 4 writes, 2 crashes, 2 reads                                                | `truncateBeforeCommit` → `NoAckedWriteLost`; `nonAtomicProbe` → `LatestSeesAcked`; `trimUnflushed` → `NoAckedWriteLost`; `noSnapshotUpperBound` → `LatestSeesAcked` (D70) |    5 |
| `GcSafety.cfg`               | GC never deletes an SST that the newest manifest, a manifest a reader may still hold, or a checkpoint references, nor an upload whose commit can still succeed; SST drops included                                                 | 2 SSTs (symmetric), clock 0–3, min age 1, 5 manifests                       | `noBoundaryCheck`, `ownUploadAge`, `dropDeletesObject` → `ReadableManifestsComplete`; `ignorePins` → `PinnedManifestsComplete`                                            |    5 |
| `GcSafetyFork.cfg`           | With one fork of the database: a fork releases its hold only once no manifest a reader of the fork may hold names the database's SSTs (D77)                                                                                        | 1 SST, clock 0–3, min age 1, up to 6 manifests, `WithFork`                  | `earlyForkRelease` → `ForkReadsComplete`                                                                                                                                  |    2 |
| `SealSplit.cfg`              | After a seal no write commits; a split leaves every write acknowledged before the seal in exactly one successor                                                                                                                    | 3 keys (1 to the left successor), 4 writes                                  | `sealWithoutLock`, `checkpointBeforeSeal`, `overlappingRanges` → `EveryAckedWriteInExactlyOneSuccessor`; `writerNotFenced` → `NoCommitAfterSeal`                          |    5 |
| `Transactions.cfg`           | `SerializableSnapshot` transactions admit only serializable histories, and every read returns what the database held at the transaction's begin (`ReadsAtBegin`, D76), with retained rows, trims, and the begin or newest manifest | 2 keys, 2 transactions (symmetric), sequence numbers up to 5                | `snapshotOnly`, `trimWithoutAbort` → `Serializable`; `noReadHorizonCheck`, `noReadUpperBound` → `ReadsAtBegin`                                                            |    5 |

The models are bounded. They check every interleaving within these bounds, not beyond them. What
they leave out is part of section 8.

Code-level properties are property tests with mutation checks (`docs/coverage-matrix.md`). They
cover codecs, the merge, the compaction merge, GC plans, the client and transactions against
oracles, and fault injection.

D78 changed no model step. A compactor that fences in again is one more compactor with its own
epoch in `ManifestCasFencing.tla`, and the overwritten epoch was local state of the Durable Object,
which the D78 tests check (D78, "Model").

PENDING(final-commit): the number of runs `vp run verify` reports on the release commit, observed
at exit 0. The last count observed was 28/28, on the D70–D74 code (`docs/progress.md`). The 31 runs
listed today have not been observed in one run.

## 5. Soaks

### The gate

`scenario-cli soak` (D65) is the release gate. While it runs, it puts several databases under:

- mixed batched writes;
- reads at `Latest`, `AtLeast`, and `Flushed`;
- scans;
- `SerializableSnapshot` counter transactions;
- pinned snapshots, read four minutes later;
- a remover;
- a sharded group split at `--split-at`;
- a redeploy at `--redeploy-at`.

A model records every acknowledged write.

As rebuilt (`/tmp/shale-work/agents/soaktool/REPORT.md`, D65 "Telemetry never holds up the soak"),
it runs in phases: `workload`, `final-reads`, `storage`, `telemetry`, `done`.

- **The watchers stop with the workload.** The tail-record watcher and the span puller are
  interrupted when the workload ends. Each of their reads has a 5-minute budget.
- **The data checks come first.** `final-reads` reads every key at `Latest`, then at `Flushed`
  after a flush, against the model, along with every counter and the sharded group. `storage`
  checks that no database keeps more old SSTs than its manifest uses.
- **Telemetry comes last, and is bounded.** Each final telemetry read (the tail catch-up,
  `measure health` over 15 minutes, the span pull, the error grep) has a budget of
  `--telemetry-budget-minutes` (default 10). An overrun is a failure naming how far the read got.
- **The report is never lost.** `--report-file` is replaced atomically every minute, at each phase
  change, and at `done`. At `done` it equals what the soak prints.
- **Late events are skipped.** A split or redeploy planned at or after `--minutes` does not run.
- **The exit status is the verdict.** The command exits 0 only when the report says
  `passed: true`.

`passed` is true only at phase `done` with no mismatch, no failed operation, no unexpected error
span, and a health report with no issue (`soakPassed`, `soak-report.ts`). A health issue is any
`exceededCpu`, `exceededMemory`, or `exception`, any current corruption, a GC or sweep failure, or
a stalled write. Tests: `soak-telemetry.test.ts`, `soak-report.test.ts`, and `scenario.test.ts` in
`tools/scenario-cli`. The rebuilt tool has not yet run live (`docs/progress.md`).

### The release runs, on the final commit

D76, D77, D78, and the counter fix change deployed code. The soaks that gate the release are
therefore run again on the release commit.

PENDING(soak-preview-1): the 130-minute soak on the release commit on `preview-1`, with production
settings and `--trace-sample-rate 0.1`, two splits, a redeploy midway, and `--report-file`. Fill in:
the exact command, start and end time, the commit, pass or fail on each criterion above, the final
`phase`, operation counts, p50/p99 per operation, the `measure health` summary, storage after GC,
and any error span outside the expected list.

PENDING(soak-preview-2): the 20-minute soak on the release commit on `preview-2` (`eu`), with the
same fields.

### Evidence about `f3b2ad2`, not the release gate

These soaks ran the code before D76, D77, D78, and the counter fix. They say nothing about those
changes, and they use the soak tool from before its rebuild.

**`preview-2` (`eu`), 20 minutes, `f3b2ad2`: passed.** The run was started at 22:25:52 UTC and
ended at 22:50:33, exit 0 (`/tmp/shale-work/release/log`). The command was
`soak --stage preview-2 --minutes 20 --databases 3 --keys-per-database 1000 --redeploy-at 10
--split-at 5,14 --trace-sample-rate 0.1` (`/tmp/shale-work/scripts/release-soaks.sh`). The report is
`/tmp/shale-work/release/soak-p2.json`, as summarized by `soak-summary.mjs`:

- `passed: true`, after 21.1 minutes, on 3 databases.
- No mismatch and no failure. No operation was retried, and the event log is empty.
- Checks: 4,459 `Latest` gets, 1,871 `AtLeast` gets, 645 scans, 1,340 transaction commits, 2
  splits, 2 removals, and 1 redeploy. Every key of the 3 databases and of the sharded group was
  checked at the end, at `Latest` and `Flushed`.
- Counters: 443, 454, and 443 committed increments, none uncertain.
- Storage: 0 SSTs older than the bound beyond what each manifest uses.
- Telemetry: 711 spans and 4,282 invocation records checked. Every invocation ended `ok`: 37,040
  API Worker, 5,560 shard router, 200 compactor, and 20 GC, as scaled from the sample.
- Error spans by exception: 5 `DatabaseNotFoundError` (the remover's) and 1 `HttpClientError`.
  None was unexpected.
- `measure health` over the last 15 minutes: healthy, no issue, no refused write, 8 flushes (p50
  265 ms, max 743 ms), deepest L0 backlog 4, no corruption, no GC failure.

| Operation (client, ms) |     n |   p50 |   p99 |   max |
| ---------------------- | ----: | ----: | ----: | ----: |
| get `Latest`           | 4,459 |   172 |   848 | 1,476 |
| get `AtLeast`          | 1,871 |   172 |   871 | 1,283 |
| get `Flushed`          | 1,096 |   197 |   986 | 1,394 |
| scan                   |   645 |   467 | 1,401 | 1,757 |
| write                  | 3,904 |   216 |   366 | 1,192 |
| sharded put            | 3,547 |   181 |   245 | 3,002 |
| transaction commit     | 1,340 |   199 |   305 |   621 |
| split                  |     2 | 2,725 | 2,725 | 2,725 |

These latencies are client-side. The client was in the US and the stage in `eu`, so every call
crosses the Atlantic.

**`preview-1`, 132 minutes, `f3b2ad2`: passed.** The soak ran from 22:25:52 to 00:43:35 UTC, exit
0, with 6 databases of 4,000 keys, splits at minutes 20 and 80, and a redeploy at minute 65 (23:31).
The report is `/tmp/shale-work/release/soak-p1.json`, summarized in `soak-p1.summary.txt`.

- `passed: true` after 132.1 minutes. No mismatch, no failure, and no unexpected error span.
- Every key of the 6 databases and of the sharded group was read at the end, at `Latest` and
  `Flushed`. The 6 counters committed 3,457 to 3,525 increments each, none uncertain.
- **Retries:** 9 operations, each retried once and then answered. Six were `HttpClientError` at
  the redeploy (23:32:08–13), two `StorageUnavailable` at 00:06, and one timeout at 00:12.
- **Storage:** no SST older than the bound beyond what each manifest uses.
- **Invocations,** as estimated from the sample: 554,110 API Worker invocations ended `ok` and 11
  `canceled`; 2,320 compactor `ok` and 7 `canceled`. None hit a limit. `measure health`: healthy,
  deepest L0 backlog 4, flushes p50 474 ms and max 802 ms.
- It did not see D78's stuck compactor, nor any other defect fixed since.

| Operation (client, ms) |      n |   p50 |   p99 |    max |
| ---------------------- | -----: | ----: | ----: | -----: |
| get `Latest`           | 81,519 |    56 |   382 | 11,928 |
| get `AtLeast`          | 33,954 |    56 |   380 |  7,917 |
| get `Flushed`          | 20,578 |   102 |   425 |  9,470 |
| scan                   |  9,086 |   128 |   474 |  8,916 |
| write                  | 59,914 |    73 |   237 |  1,944 |
| sharded put            | 34,590 |    69 |   160 |  1,794 |
| transaction commit     | 20,987 |    62 |   174 |    711 |
| split                  |      2 | 1,241 | 1,241 |  1,241 |

**`preview-3`, `measure manifest-cas`, `f3b2ad2`: failed, which found D78.** The measurement made
30 small flushes back to back. It exited 1 at 23:05:30 UTC (`/tmp/shale-work/release/log`) with
`WriteStalled` at 16 L0 SSTs. Twelve minutes later the database `ms-cas-mwy5ujbj` was still
stuck, with its compactor fenced against itself (section 7, D78).

**`preview-3`, D78's recovery, `ed4e787`: recovered** (`/tmp/shale-work/release2/NOTES.md`). The
fixture had been stuck since 23:05:29 UTC: 16 L0 SSTs, compactor epoch 1 `fenced: true`, manifest
epoch 2. After the D78 deploy at 23:32:56 its status did not change until something woke it.

- 23:34:48: one write was refused with `WriteStalled` (`L0Backlog`), which wakes the compactor.
- 23:34:52: epoch 3, `fenced: false`, one compaction running.
- 23:34:59: L0 at 0, with 2 sorted runs.
- 23:35:16: the next write was acknowledged.

Two runs of `measure manifest-cas` on `ed4e787` then exited 0. `ManifestStore.update` p50 was
248 and 282 ms, and a small flush p50 was 400 and 478 ms (D56 measured 709 ms).

## 6. Decisions P1 to P6

Dillon delegated P1–P5 ("decide with best judgement"); each took the recommended default
(`docs/progress.md`, "Open questions").

| #   | Decision         | Answer                                                                                                                                                                       | Implemented in                                                                                                                                                                                                         |
| --- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P1  | Authentication   | The access gate with two rotatable Secrets Store tokens per stage; Cloudflare Access optional in front                                                                       | D64: `infra/shaledb/src/shaledb-access-tokens.ts` (binding by `Secret.lookup`), `access-token-store.ts` and `access-tokens-cli.ts` (the `tokens` task), `packages/lsm-cloudflare/src/lsm-access-gate.ts` (the gate)    |
| P2  | Data residency   | None (`SHALEDB_JURISDICTION` defaults to `none`); bucket location `auto`                                                                                                     | D49, D55, D56: `infra/shaledb/src/shaledb-stack-config.ts`                                                                                                                                                             |
| P3  | Hostname         | The API Worker's `workers.dev` URL, printed as the `apiUrl` output                                                                                                           | `infra/shaledb/alchemy.run.ts`; no custom domain is configured                                                                                                                                                         |
| P4  | Telemetry        | The stage's own `TraceCollector`, 14-day expiry of `traces/` and `tails/`, 10% head sampling in production (100% on previews); tail records sampled the same except non-`ok` | D63, D71: `PRODUCTION_TRACE_SAMPLE_RATE` and `DEFAULT_TRACE_RETENTION_DAYS` in `shaledb-stack-config.ts`; `infra/shaledb/src/trace-collector-worker.ts`; `infra/shaledb/src/cpu-tail-worker.ts`; `lsm-trace-policy.ts` |
| P5  | Plan and limits  | Workers Paid; `limits.cpu_ms` 30,000 (API and its writers) and 60,000 (compactor)                                                                                            | D61: `DEFAULT_API_CPU_MS`, `DEFAULT_COMPACTOR_CPU_MS` in `shaledb-stack-config.ts`. The account's plan is not visible in the repository; the previews ran with these limits                                            |
| P6  | Release go-ahead | pending: this checklist                                                                                                                                                      | `plan:production` and `deploy:production` in `infra/shaledb/vite.config.ts`                                                                                                                                            |

## 7. What changed late (D67 to D103)

These landed after the first soaks. The release soaks (section 5) are the first long runs on all of
them.

- **D67.** A `Latest` read reuses any cached manifest that holds the writer's flushed rows, for up
  to half of `gcMinAge`. Reads open their SSTs together. Scans read ahead 8 blocks per SST. Blocks
  default to 16 KiB.
- **D68.** The project was renamed from cf-slatedb to shaledb: packages, paths, the Stack
  (`Shaledb`), variables, bindings, and live names. The old `CfSlatedb/preview-<n>` stacks and their
  secrets remain (`docs/final-handoff-draft.md`).
- **D69.** A request the endpoint's schemas refuse is `InvalidRequest` (400), not 500.
- **D70, a correctness fix.**
  - **What was wrong.** A `Latest` read could use a manifest that had been flushed and compacted
    past the snapshot the writer gave it. It then returned "not found" for a key that always had a
    value. The defect was present since Phase 5.
  - **How it was found.** By adding compaction to `NoAckedWriteLost.tla`. It was never seen live.
  - **The fix.** A read now refuses such a manifest and asks the writer again, up to three times,
    then fails with the retryable `StorageUnavailable`. A transaction scan without a begin manifest
    had a related gap, also fixed.
  - **Also in D70.** A flush writes only the versions a read can reach.
- **D71.** Tail records are sampled like traces, except those that did not end `ok`.
- **D72.** A manifest refresh takes one round trip from the manifest held. A block read slower than
  50 ms is hedged with a read from the bucket. Scans stop reading when they have their rows.
  Corrected since (review): there is no hedging in a restricted Stack, the losing read is
  interrupted, and queueing alone can start a hedge.
- **D73.** A flush no longer waits for the compactor it wakes. SST blocks are encoded a chunk of
  rows at a time. Concurrent part uploads were measured and declined. D78 shows that the
  background wake exposed a compactor race.
- **D74, a writer fix.**
  - **What was wrong.** The writer had two figures for unflushed bytes. In the gap between them,
    the alarm fired, scanned the memtable, did not flush, and set itself again. A 267 MB flush took
    200 s instead of 20 s. Two such writers at once were reset, with no acknowledged write lost.
    Every transaction begin also paid a full memtable scan through `status`: 1.58 s of CPU on a
    full memtable.
  - **The fix.** One formula and three counters. Neither the alarm nor `status` scans.
- **D74, corrected: the counter refresh race** (review, `/tmp/shale-work/agents/review/REPORT-2.md`;
  `fec2698`).
  - **What was wrong.** A commit landing between a flush's counter scan and its install could be
    lost from the counters. Its row was then in SQLite with no alarm counting it.
  - **What it could do to a caller.** That row was not flushed until a later write set the alarm
    again. It was never lost: SQLite held it, and `Latest` reads saw it.
  - **How it was found.** By code review.
  - **The fix.** The counters are one value with a generation and a "commit in progress" flag. The
    refresh installs a scan only when neither changed, and gives up, keeping the counters above
    SQLite, after 3 attempts. A flush that writes nothing settles counters that still show rows.
    The commit's mark is released on any exit, an interruption included.
- **D75, the plain Durable Object SQLite baseline.**

  PENDING(baseline): what was compared (workload, stage, rows/s, read and write latency), the
  commit, the tool (`measure` additions merged from `proto/baseline`), and what the difference
  means for a caller choosing between shaledb and one Durable Object's SQLite.

- **D76, a liveness fix in transactions and snapshots.**
  - **What was wrong.** A transaction began with the manifest of the writer's last flush and read
    exactly that manifest. A compaction replaces that manifest within seconds, and GC deletes it
    `gcMinAge` after that. The writer goes on naming it until its next flush.
  - **How it was found.** By checking `docs/invariants.md` line by line against the code. It was
    reproduced in a unit test before the fix.
  - **What it could do to a caller.** On a database that is read but not flushed for over an hour
    after a compaction, every transaction that read failed with `SnapshotTooOld`, and so did every
    retry. No answer was ever wrong. Separately, an unpinned snapshot was guaranteed for half of
    `gcMinAge` instead of the documented hour.
  - **The fix.** A transaction reads the newest manifest when it fits its bounds, else the begin
    manifest when stored, else `SnapshotTooOld`. A snapshot is taken at the newest manifest, loaded
    then. Model: `Transactions.tla` gained `ReadsAtBegin` and the broken variant `noReadUpperBound`.
- **D77, a fork released its hold on its source too early.**
  - **What was wrong.** A fork released its checkpoint on its source once its newest manifest and
    its pinned manifests no longer named the source. Its own manifests replaced less than
    `gcMinAge` ago were ignored, though readers may still hold them. A fork of a fork released its
    hold on the middle database before rewriting at all.
  - **How it was found.** The interaction audit, with two scratch tests; the second case was found
    while fixing the first.
  - **What it could do to a caller.** After the source was collected or removed (a split's
    cleanup), a fork snapshot taken just before the rewrite failed with `StorageUnavailable` on
    every retry, as did a transaction whose begin manifest named the source.
  - **The fix.** The release moved into the fork's own collection: a hold is released only when no
    manifest GC keeps names another database's SST. Model: `GcSafetyFork.cfg` with
    `earlyForkRelease`.
  - **What it costs.** A split's sealed source cannot be removed until about `gcMinAge` (1 hour)
    after both halves' rewrites; nothing in a split waits on it.
- **D78, a compactor fenced against itself, for good.**
  - **What was wrong.** Two overlapping first wakes both fenced in, a poll wrote back the older
    epoch, and the compactor stored itself as fenced. A fenced compactor did nothing, ever.
  - **How it was found.** `measure manifest-cas` on `preview-3`, 30 small flushes back to back.
  - **What it could do to a caller.** That database never compacted again: after 16 L0 SSTs every
    write was refused, permanently.
  - **The fix.** One fence-in per start, every state update a read-modify-write, and a fenced
    compactor fences in again on its next start. Six mutations caught; no model step changed.
  - **Shown live.** The stuck fixture on `preview-3` recovered within 11 s of its first write after
    the deploy (section 5).
- **D79.** The sharded API answers refused requests with the public envelope; it is not part of
  `/v1`'s contract.
- **D80, the change feed skipped changes silently.** When retention had pruned every entry above
  a consumer's position, or for a fork before its first flush, the feed returned nothing and moved
  the consumer's cursor past changes it never delivered. It now answers only what its changelog
  provably holds, and fails with `ChangelogUnavailable` (`Expired`) otherwise. Found by the review's
  scratch test.
- **D81, compaction changed what a key reads.** It folded operands with different expiries into one
  value that expired with the earliest, so after a compaction a key could read as absent. Now only
  rows that expire together fold. Found by the review's scratch test.
- **D82.** Every signal the operations guide names is read from a span or a tail record; metrics are
  emitted and not collected.
- **D83, review of D76.** A transaction read may use any cached manifest that fits. Two concurrent
  collections, and a collection that went on when a manifest it retained had vanished, are now
  safe. Also TTL and clock checks.
- **D84, a lost update in `Snapshot` commits.** A trim between the horizon check and the conflict
  queries hid a newer write. The check now re-reads the horizon after its queries. Found by the
  Durable Object audit.
- **D85, a seal the writer never saw.** If a seal's caller hung up after its commit landed, the
  writer still believed it was open and acknowledged writes that could never flush and were in no
  successor. Such a seal now forgets the writer's phase. An alarm for a removed database stops.
  Found by the Durable Object audit.
- **D86, a large compaction's first step result passed 1 MiB.** `prepare` now records its inputs'
  ids, not their handles. Found by the Workflow and limits audits.
- **D87, settings with `l0MaxSsts` below `minCompactionSources` stalled writes forever.** Refused at
  plan time. Found by the Workflow audit.
- **D88, a TTL near 2^53 made a database unflushable.** `ttlMillis` and `lifetimeMs` are at most 100
  years, and retried requests get truthful answers. Found by the API audit.
- **D89, a stale router served a sealed shard at `Flushed` indefinitely.** It now refreshes a map
  older than a second before such a read. Found by the API audit.
- **D90, D91, a removed database's id.** PENDING(d90): a reader that cached the removed database's
  manifest served it as the new one's, and a commit landing after the removal brought the database
  back. The fix keeps the `Removed` manifest at the head for `gcMinAge`, and `create` refuses the id
  until then (`DatabaseRecentlyRemoved`). Found by the lifecycle audit; not yet merged.
- **D92.** Cloudflare's platform limits against shaledb's demand; settings the platform cannot hold
  are refused.
- **D93, a `Latest` scan held every unflushed row; the change feed split large batches.** Scans pull
  writer pages as they need them, and every memtable page is bounded by bytes (8 MiB). The feed
  pages whole batches; before, an unflushed batch of more than 1,000 rows lost its rest silently.
  Found by the limits and Durable Object audits.
- **D94, a compaction job whose Workflow instance was gone stayed running for good,** reserving its
  sources. Now a job unseen for `compactionLeaseMs` is finished. Found by the Workflow and Durable
  Object audits.
- **D95, a seal naming other successors succeeded.** It now fails, naming the recorded ones. Found
  by the API audit.
- **D96, a compaction longer than `gcMinAge` lost its early outputs to GC** and never committed. A
  collection now keeps what a running compaction may have written. Found by the Workflow audit.
- **D97, one compaction step could read all of a job's dropped rows.** A step now also ends after
  merging 1.5 × its output size of input. Found by the limits audit.
- **D98, a collection's cost grew with the commit rate.** It now folds references as it reads, and
  past 5,000 retained manifests it defers SST deletion. Found by the limits audit.
- **D99.** Bounds on checkpoints, the changelog, merged values, and names; the manifest's budget
  and the database size limit; the compactor's subrequests.
- **D100, every request is bounded.** The body (16 MiB), and every string, bytes, and array field.
  Found by the API audit.
- **D101, an abandoned split left its range unwritable forever.** A request that waits it out now
  finishes it. Found by the API audit.
- **D102, the writer's alarm.** PENDING(d102): a failing flush retried every second forever, and an
  unreadable `writer_meta` set no alarm. Now there is a backoff; the stalled-write reload and two
  concurrent schedulers no longer move the writer's view or its alarm back. Found by the Durable
  Object audit; not yet merged.
- **D103, a retried commit of a compaction with no outputs, and the size of a large compaction.**
  PENDING(d103): a commit records its job id, so a retry is recognized whatever its outputs. A
  missing Workflow instance is finished. The derived needs of a large compaction are tabled. Found
  by the Workflow audit; not yet merged.

## 8. Defects found in the last day, and what that says

**What the gates did.** On `f3b2ad2`, every gate passed: the 132-minute soak on `preview-1`, the
20-minute `eu` soak on `preview-2` (section 5), the 13 scenarios, `vp run ready`, and `vp run
verify` (28 runs). Every defect below except D70 was in that code. Gates that run the system and
check its answers (soaks, scenarios, unit and property tests, the existing models) found none of
them. Every one was found by reading code against a stated rule, or by extending a model or a
measurement past what the gates did.

| Defect                                        | What a caller would have seen                                        | Found by                                | What catches it now                                                                              |
| --------------------------------------------- | -------------------------------------------------------------------- | --------------------------------------- | ------------------------------------------------------------------------------------------------ |
| D70: read past its snapshot                   | "not found" for a key that had a value                               | a model extended with compaction        | `noSnapshotUpperBound`; `db-client.test.ts`                                                      |
| D74b: counter refresh race                    | a row unflushed until a later write                                  | review of a diff                        | `db-writer.test.ts` (`runWithHookedStats`)                                                       |
| D76: begin manifest collected                 | every transaction read refused, every retry                          | guide checked against code              | `db-transaction.test.ts`; `ReadsAtBegin`                                                         |
| D77: fork hold released early                 | `StorageUnavailable` on every retry, fork snapshots and transactions | interaction audit                       | `GcSafetyFork.cfg` `earlyForkRelease`; `database-lifecycle.test.ts`, `garbage-collector.test.ts` |
| D78: compactor fenced against itself          | every write refused, permanently                                     | a measurement (`measure manifest-cas`)  | `compactor.test.ts`, two overlapping cold wakes                                                  |
| D80: change feed skipped changes              | changes lost without an error                                        | interaction audit                       | `change-feed.test.ts`, `manifest.test.ts`                                                        |
| D81: compaction folded expiries               | a key absent after a compaction                                      | interaction audit                       | `row-resolution.test.ts`, `compaction-merge.test.ts`                                             |
| D83: collection without a retained manifest   | SSTs a reader needed deleted                                         | interaction audit                       | `garbage-collector.test.ts`                                                                      |
| D84: `Snapshot` commit across a trim          | a lost update                                                        | area audit (Durable Objects)            | `db-writer.test.ts`; `Transactions.tla` `Commit`                                                 |
| D85: seal the writer never saw                | acknowledged writes that never flushed, in no successor              | area audit (Durable Objects)            | `db-writer.test.ts`                                                                              |
| D86: `prepare` result over 1 MiB              | a large merge never runs; runs pile up                               | area audits (Workflow, limits)          | `compaction-executor.test.ts`                                                                    |
| D87: `l0MaxSsts` below `minCompactionSources` | writes refused forever                                               | area audit (Workflow)                   | `size-tiered-compaction-scheduler.test.ts`; a settings rule                                      |
| D88: TTL near 2^53                            | every flush failed, writes stalled                                   | area audit (API)                        | `db-http-limits.test.ts`, `db-http-boundary.property.test.ts`                                    |
| D89: stale router at `Flushed`                | a sealed shard's old data, without bound                             | area audit (API)                        | `shard-router-staleness.test.ts`                                                                 |
| D90: recreated id                             | a removed database's data under the new one                          | area audit (lifecycle)                  | PENDING(d90)                                                                                     |
| D91: commit after a removal                   | a removed database back, acknowledging writes                        | area audit (lifecycle)                  | PENDING(d90)                                                                                     |
| D93a: `Latest` scan held all unflushed rows   | `exceededMemory` on an allowed scan                                  | area audit (limits)                     | `db-client-scan.test.ts`                                                                         |
| D93b: change feed split a large batch         | 500 of 1,500 changes skipped silently                                | area audit (Durable Objects)            | `change-feed.test.ts`, `mem-table.test.ts`                                                       |
| D94: job with no Workflow instance            | its sources never compacted again                                    | area audits (Workflow, Durable Objects) | `compactor.test.ts`                                                                              |
| D95: seal with other successors               | success, with the old successors kept                                | area audit (API)                        | `db-writer.test.ts`                                                                              |
| D96: compaction longer than `gcMinAge`        | a large merge never commits                                          | area audit (Workflow)                   | `garbage-collector.test.ts`, `compactor.test.ts`                                                 |
| D97: step over dropped rows                   | compaction stuck after mass deletes; writes refused                  | area audit (limits)                     | `compaction-executor.test.ts`                                                                    |
| D101: abandoned split                         | a key range unwritable forever                                       | area audit (API)                        | `shard-split-faults.test.ts`                                                                     |

Risks fixed alongside (not counted above): D98 (GC cost by commit rate), D99 (unbounded caller
growth), D100 (unbounded bodies), D102 (alarm retry), D103 (a retried zero-output commit).

**By class** (the classes of `/tmp/shale-work/agents/AUDIT-COMMON.md`, plus one it does not
name):

| Class                                                              | Defects                                 |  Count |
| ------------------------------------------------------------------ | --------------------------------------- | -----: |
| Two features each correct alone                                    | D70, D77, D80, D83, D87, D89, D91, D93b |      8 |
| State shared across an await inside one Durable Object             | D74b, D78, D84, D85                     |      4 |
| Liveness after long idle periods and after GC                      | D76, D90, D94, D101                     |      4 |
| Time (TTL, `gcMinAge`, retention)                                  | D81, D88, D96                           |      3 |
| A limit a caller or the platform sets (not in AUDIT-COMMON's list) | D86, D93a, D97                          |      3 |
| A property checked at only one moment                              | D95                                     |      1 |
| **Total**                                                          |                                         | **23** |

**By how each was found:** 15 by the round-2 area audits, 4 by the round-1 interaction audit, and
1 each by an extended model (D70), a guide checked against the code (D76), a review of a diff
(D74b), and a measurement (D78).

**The rounds.** Rates come from the `STATUS.md` timestamps of each agent. They are approximate,
because finding and fixing overlap.

- **Round 1: review and interaction audit of the last day's diff.** It found D76, D77, D80, D81,
  D83, and the counter race: six defects in about 67 auditor-minutes of finding (the invariants
  agent 22:40–23:00 UTC; the review agent's passes 22:40–22:51, 23:18–23:28, and 23:38–00:04).
  That is about 5 an auditor-hour.
- **The measurement.** `measure manifest-cas` found D78 at 23:05 UTC.
- **Round 2: five auditors by area, from 00:12 UTC.** Their first briefs found 12 defects in about
  79 auditor-minutes, about 9 an auditor-hour:
  - API: 2 (D88, D89);
  - Durable Objects: 2 (D84, D85);
  - lifecycle: 3, D90 and D91;
  - Workflow: 3 (D86, D87, D96);
  - limits: 2 new (D93a, D97) and D86 again.

  Their second briefs, while fixing, confirmed four more (D93b, D94, D95, D101).

- **No round has yet ended without a defect-class finding.** A third round (audit-prod, on the
  production deploy) started at 00:59 UTC; its result is not in.

**What has had a pass over every await or every field, and what has not** (each report's "Not
reached"):

- **Every await of every Durable Object:** the writer, compactor, compaction budget, and shard
  directory (audit-do). Not reached: `MemTable` migrations, the `LATEST`-hint writes after
  eviction, and the Workflow's own steps.
- **Every field of every request schema:** a schema walker that fails on any unbounded field
  (audit-api, D100). Not reached: the `BlockServer` under a hostile caller, and HTTP round trips
  that reach a writer in-process.
- **Every Cloudflare limit the design touches** (audit-limits, D92). Not reached: Worker startup
  time, what Tail Workers drop, and any live run of a derived limit.
- **Compaction and GC** (audit-workflow). Not reached: a compaction step's memory with 8 inputs,
  which needs workerd.
- **Lifecycle** (audit-lifecycle): still finishing D90 and D91; no report yet.
- **Not audited by area at all:** the tools (`scenario-cli`, `trace-cli`, `preview-cli`), the
  telemetry policy, and the read path's block cache beyond D72's review.

**What that says.** The checks that run the system at its normal timing found none of these in
soaks of 132 and 21 minutes. The two rounds' first passes, reading the code against a stated
rule, found 18 in about 2.4 auditor-hours. The rate went up from round 1 to round 2, not down. Each fix has a
regression test, which closes that defect, not its class. These are the facts as they stand; they
make no prediction.

## 9. Known limitations

What v1 does not do, or has not measured.

**Throughput and latency.**

- **One writer per database.** It was measured at ~6,500 rows/s with 100-row batches and ~15,000
  rows/s with 1,000-row batches (D61). A workload needing more must use more databases, for example
  through the range sharding in `packages/lsm-sharding`.
- **The batch body is base64 JSON.** A binary body was declined (D61). It would only be added later,
  as an additive content type.
- **Manifest commits cost ~285 ms beside the bucket**, and up to ~1 s from the far side of the world.
  The bucket's location is fixed when it is created (`SHALEDB_BUCKET_LOCATION`). The default, `auto`,
  places it near the machine that deploys, so choose it before the first production deploy if
  callers are not near that machine.
- **Writer start-ups dominate a get's p99.** A fence-in takes ~0.5 s (D72).
- **No Workers Cache for blocks in a restricted Stack.** A restricted (`eu`, `fedramp`) Stack has
  none, and it does not hedge block reads (D72, corrected). Reads from far away measured slower.
- **Hedging can be started by queueing alone.** Its 50 ms timer starts when a read is issued, not
  when it gets one of an invocation's six connections. On a scan wide enough to queue, a hedge can
  start only because its read waited, and the hedge then waits too. A cap on hedges in flight is
  not adopted (D72).
- **CPU varies between runs with the machine a Durable Object lands on.** The write CPU of the
  same 128 batches ranged from 9.6 to 28 s between runs (`docs/measurements.md`, D73's section).
- **One client.** Every measurement came from one client on a US residential connection, against
  preview stages.

**Semantics.**

- **`Flushed` and `AtLeast` reads are eventually consistent**, as designed. A routed `Flushed` read
  of a sealed shard returns its state at the seal (D59).
- **Long transactions abort.** A transaction aborts with `SnapshotTooOld` if it outlives the
  writer's retention (`transactionRetentionMs`, 60 s) or the `maxRetainedBytes` bound (64 MiB).
  Callers that send no `manifestId` keep the pre-D58 behavior: a flush past their read sequence
  aborts them (D58, D76).
- **Plain snapshots expire after `gcMinAge`** (1 hour), from when they are taken (D76). Longer ones
  must be pinned (D52).
- **Dropping a corrupted SST loses data.** That SST's versions of its keys are lost, and older
  versions show again (D62).
- **A corrupted L0 SST stalls the database's writes** until an operator acts. Compaction stops
  planning the corrupted source (D53), L0 fills, and writes fail with `WriteStalled` (`L0Backlog`).
  The cure is the corruption runbook in `docs/operations.md`. This is documented behavior, listed
  as risk 6c by the audit.
- **The jurisdiction is fixed per installation.** Changing it means a new installation, with data
  moved by forks or copies (D49).
- **`/v1` changes are additive only, and limits may only be relaxed** (`docs/api-compatibility.md`).
  Today's limits and error semantics are therefore permanent for `/v1`.
- **A rollback across a format version stops on the newer data**, with `StoredDataInvalid` (D64).
  v1 is format 1.
- **A split's source lives longer.** The sealed source cannot be removed until about `gcMinAge`
  (1 hour in production) after both halves' rewrites (`DatabaseHasDependents` until then). Reads and
  writes of the halves are unaffected (D77).
- **An abandoned split finishes only when a request waits it out.** A split that died after its
  seal and is never retried leaves its range refusing writes and `Latest` reads, each retryable,
  until a request through the router waits 30 s and finishes it (D101). Nothing finishes it without
  traffic. A pending split's age is not shown anywhere (an `audit-api` proposal, not built).
- **A compaction job no progress read has seen running for 15 minutes is finished**
  (`compactionLeaseMs`, D94), and its sources are planned again. A long job that reads as running
  is kept. If the abandoned job commits later, the exact-inputs check refuses it once another job
  has taken its sources: the work is wasted, nothing is wrong.
- **Above about 1.4 commits a second, sustained for an hour, SST collection is deferred** (D98). Old
  manifests are still deleted, but unreferenced SSTs wait until the rate falls. Storage grows
  meanwhile, and `measure health` reports the deferral.
- **A public scan page is bounded by rows, not bytes.** 1,000 rows of values up to 1 MiB can make a
  page of about 1 GiB in one 128 MB isolate (release3 `STATUS.md`; D93 bounded the memtable pages
  only). Use small limits over large values.
- **The writer's alarm.** PENDING(d102): a failing flush retried every second, and an unreadable
  `writer_meta` set no alarm, until D102 lands.
- **Not reached by the audits.** These were not checked:
  - a compaction step's memory with 8 inputs (needs workerd);
  - the `BlockServer` under a hostile caller;
  - `MemTable` migrations;
  - the tools.

**Storage.**

- **The writer's SQLite can hold more than D58 says.** D58 states at most `maxRetainedBytes` plus
  the newest flush, "up to `l0SstSize`". But the trim always keeps the newest flush whole
  (`MemTable.trim`), and after a stall a flush can be up to `maxUnflushedBytes` (256 MiB; D61
  measured a 267 MB flush). Retention can therefore reach 64 MiB + 256 MiB. This is Durable Object
  storage, not isolate memory (audit risk 5c).
- **An idle database keeps its newest flush in SQLite indefinitely** (up to `l0SstSize`, 64 MiB),
  because the trim never removes the newest flush (D58, D92).

**Platform limits (D92).** D92 found four places where the defaults and the schema limits allowed
a demand above a Cloudflare limit. Each is fixed:

- a large compaction's first step result (D86);
- a step over dropped rows (D97);
- a `Latest` scan and the memtable pages (D93);
- the cost of a garbage collection (D98, deferred above the commit rate above).

None has been run live at the derived size. PENDING(d103): the largest compaction run on a preview,
against D103's table.

Settings the platform cannot hold (R2 parts, the Durable Object's storage, the isolate's memory,
the account's Workflows) are now refused when a deploy is planned.

**Bounds a caller meets (D99).**

- **Checkpoints and pinned snapshots:** at most 1,000 per database. The next one is refused with
  `CheckpointLimitReached` (409).
- **The changelog:** its newest 2,048 flushes and at most 30 days of retention. A consumer behind
  them gets `Expired`.
- **Merged values:** at most 1 MiB. Past that, a read fails with `MergedValueTooLarge` (409) until
  the key is rewritten with a put.
- **Names:** a checkpoint name is at most 256 characters and a budget group at most 63.
- **Database size:** about 1 TiB for keys of 16 bytes, less for longer keys (about 90 GiB for 1 KiB
  keys). Beyond that the manifest passes its 4 MiB budget; such databases are sharded.
- **The compactor Worker** is set to 10 million subrequests, because Cloudflare counts a
  compaction job's subrequests together. `docs/operations.md`
  ("Platform limits") says what an operator should watch.

**Not measured or not shown.**

- **The SQLite page cache and 128 MB.** Whether Durable Object SQLite's page cache counts against
  the 128 MB isolate limit was not measured as such. Only the outcome was: no `exceededMemory` at
  256 MiB unflushed (`docs/measurements.md`, "Not measured").
- **The Workflows concurrent-instance limit** is documented as 50,000 per account (D92). 200 at
  once ran; production's compaction budget is 32.
- **GC at scale used dev timings** (`gcMinAge` 10 s), not production's 1 hour (D60).
- **D70's race was never reproduced live.** The fix rests on the model, on `db-client.test.ts`,
  and on mutation checks.
- **The newest changes have no long soak yet.** That covers D76–D103 (section 5).
- **The rebuilt soak tool ran live once** (`preview-3`, a 6-minute forced-backlog test,
  `/tmp/shale-work/release2/NOTES.md`): it reached `done` with a bounded telemetry phase. The run
  also found that a failed soak exited 0, which is fixed. The production telemetry reads have not
  run live.
  The guess that `trace-token production` reads `Shaledb/production/TraceCollectorToken` comes from
  the previews' state layout. No production state exists yet (`/tmp/shale-work/agents/prodhealth/REPORT.md`).

**Operations.**

- **Production's collector URL** comes only from the deploy's printed `traceCollectorUrl`.
  `preview-cli url production` cannot read it, because it requires a probe URL that production
  lacks.
- **Metrics are not collected.** The trace collector and the local receiver accept OTLP metrics
  and discard them, so the engine's twelve `lsm_*` counters are readable on no stage. Since D82
  every signal the operations guide names is read from a span or a tail record instead, and
  `measure health` judges them all (section 11). The counters stay in the code for a future
  collector.
- **Span-borne signals are a 10% sample in production.** A stall or a corruption repeats on every
  write or poll and shows while it lasts. A single unverified part or fenced poll can be missed. A
  database that neither writes nor compacts produces no spans, so `measure health` cannot see it
  stuck (D82).
- **The health report's L0 limit is the engine default** (16). A stage deployed with another
  `l0MaxSsts` is judged against 16 (D82).
- **The only production hostname is `workers.dev`.** Cloudflare treats a `workers.dev` subdomain as
  a Free website, "intended for personal or hobby projects that aren't business-critical", and
  recommends a route or custom domain for production
  ([workers.dev](https://developers.cloudflare.com/workers/configuration/routing/workers-dev/),
  2026-11-06). P3 chose it because no zone was given for a custom domain. The OpenAPI spec lists only the local
  server URL.
- **Secrets Store is in open beta**, with one store per account (Cloudflare's
  [Wrangler Secrets Store page](https://developers.cloudflare.com/workers/wrangler/commands/secrets-store/)).
  The tokens depend on the account's `default_secrets_store`.
- **The deployment guard's allowlist does not name this project.** See section 10, step 2.

## 10. Production commands

From `docs/operations.md` ("Deploying"), checked against `infra/shaledb/vite.config.ts`. Run them
from the repository root of a checkout at the release commit, logged in to the account through
Alchemy's Cloudflare profile.

1. **Create the production tokens, once.** This prints both tokens once. Store them in your secret
   manager.

   ```sh
   vp run @shaledb/infra#tokens ensure production
   ```

   This runs `node src/access-tokens-cli.ts ensure production`. It creates
   `SHALEDB_PRODUCTION_ACCESS_TOKEN_A` and `_B` in `default_secrets_store`.

2. **Let the deploy through the guard (only if an agent will run the tasks).** pi's Cloudflare
   deployment guard (`~/.pi/agent/cloudflare-deployment-allowlist.json`) holds agents' deploys to
   its allowlist. You edit the allowlist yourself; agents cannot.

   `scripts/allowlist-shaledb-deploys.sh` adds an entry for the project `Shaledb`. Run it from the
   main checkout, in your own terminal:

   ```sh
   /Users/dmmulroy/Code/personal/cf-slatedb/scripts/allowlist-shaledb-deploys.sh
   ```

   What the script does (read, not run):

   - **The project path.** It resolves from the script's own location
     (`REPO_ROOT=… pwd -P`), so it allowlists the checkout the script lives in:
     `/Users/dmmulroy/Code/personal/cf-slatedb/infra/shaledb/alchemy.run.ts`. Run from a worktree
     under `/tmp`, it would allowlist that worktree's path instead.
   - **What it requires.** `jq`, a policy of version 2, and the entrypoint file must exist.
   - **What it changes.** It backs the policy up to `<policy>.bak.<YYYYmmdd-HHMMSS>`. It replaces any
     entry with the same project path, adds `{ project, stages, stack: "Shaledb" }`, validates the
     result, and moves it into place. It does not remove the stale `CfSlatedb` entry, whose path
     differs.
   - **What it prints.** `Editing <policy file>`, then
     `Allowlisted Shaledb (<project>) for stages ["dev","preview-1","preview-2","preview-3"].`, then
     `Backup: <backup file>`. After a blank line come two lines saying that running pi sessions ask
     you to confirm the change or need a restart, and last the new entry as JSON.

   The stages are `dev` and `preview-1..3`. To allow `production` too, edit the script's
   `STAGES='["dev","preview-1","preview-2","preview-3"]'` to include `"production"` before running
   it. Then restart the agent's pi session, or confirm the change when it asks.

   Three things found while writing this, still true at `fec2698`:

   - **The current allowlist has no `Shaledb` entry.** Its only entry for this repository is the
     pre-rename `.../cf-slatedb/infra/cf-slatedb/alchemy.run.ts` (stack `CfSlatedb`). That path no
     longer exists, so the guard skips it (`policy.ts`, `parseCloudflareDeploymentPolicy`, which
     records it in `ignoredEntries`). Run the
     script once, whatever you decide about production.
   - **The stage list does not keep agents off production.** Once any project in a repository is
     allowlisted, the guard exempts the whole repository and every worktree of it, matched by the
     shared git directory (`isAllowlistedRepositoryWorkingDirectory` in `policy.ts`;
     `destructiveOnly` in `evaluate.ts`). Only `alchemy destroy` and
     `wrangler delete` named directly in a command stay blocked there; through a `vp run` task they
     are not (next point). So after the script runs, an agent in this repository can
     run `deploy:production` whether or not `production` is listed. The real gates are your
     instruction and `deploy:production` refusing to run without the token secrets.
   - **Only a destroy named directly stays blocked.** In an exempt repository the guard checks
     only a deployment CLI named in the command itself: `alchemy destroy`, `wrangler delete`, or one
     behind `pnpm exec`, `npx`, `vp exec`, or `vp dlx` (`evaluate.ts`, the `destructiveOnly`
     branch). It does not resolve package tasks there (`evaluatePackageTask` is never reached;
     the guard's test "exempts every canonical working directory in an allowlisted repository
     before script resolution"). So `vp run @shaledb/infra#destroy:preview-1`, whose task runs
     `alchemy destroy`, is not blocked in this repository or any of its worktrees, and neither is
     `deploy:production`. There is no `destroy:production` task. Rechecked at the D104 audit by
     reading the guard at `~/.dotfiles/home/.pi/agent/extensions/cloudflare-deployment-guard/`.

   You can skip this step by running steps 3 and 4 yourself.

3. **Review the plan.**

   ```sh
   vp run @shaledb/infra#plan:production
   ```

   This runs `alchemy plan --stage production`. Expect new resources only:

   - the `shaledb` LSM bucket (retained) and the trace bucket;
   - the API, `BlockServer`, compactor, GC, shard-router, trace-collector, and CPU Tail Workers;
   - the Durable Object namespaces and the compaction Workflow.

   There should be no probe Worker. Settings come from the environment: `SHALEDB_LSM_SETTINGS`,
   `SHALEDB_JURISDICTION`, `SHALEDB_BUCKET_LOCATION`, `SHALEDB_TRACE_SAMPLE_RATE`,
   `SHALEDB_TRACE_RETENTION_DAYS`, `SHALEDB_API_CPU_MS`, and `SHALEDB_COMPACTOR_CPU_MS`. Unset means
   the production default, so make sure none is set in your shell unless you mean it.

4. **Deploy.**

   ```sh
   vp run @shaledb/infra#deploy:production
   ```

   This runs `node src/access-tokens-cli.ts check production && alchemy deploy --stage production
   --yes`. It refuses unless both token secrets exist. Keep the two URLs it prints: the API's
   `apiUrl` and the trace collector's `traceCollectorUrl`.

5. **Smoke test.** `scenario-cli smoke` (D65) runs only operations safe on a live installation. It
   works on databases named `smoke-<time>-…` and removes them at the end, whatever happened. It
   checks:

   - create and write;
   - reads at `Latest`, `AtLeast`, and `Flushed`;
   - a scan;
   - a transaction, and a conflicting one refused;
   - a checkpoint created, listed, and deleted;
   - eight concurrent creates with one winner.

   ```sh
   SHALEDB_TOKEN='<production token A>' \
     node tools/scenario-cli/src/scenario-cli.ts smoke --api-url '<apiUrl from step 4>'
   ```

   Keep the token out of shell history: read it into `SHALEDB_TOKEN` from your secret manager rather
   than typing it.

## 11. The first day

From `docs/operations.md` ("Reading a stage's telemetry", "What to watch"), with the exact
production command for each signal. None of these needs an access token, except the compaction
status. Production keeps 10% of `ok` traces and tail records, and every tail record that did not
end `ok` (D71). Span counts below are therefore a sample, and `measure health` scales invocation
counts back up.

Once per shell, put the collector token in the environment. `trace-token` refuses a terminal, so
the token never shows on screen.

```sh
export SHALEDB_TRACE_TOKEN="$(node tools/preview-cli/src/preview-cli.ts trace-token production)"
export SHALEDB_TRACE_COLLECTOR_URL='<traceCollectorUrl from the deploy>'
S=.traces/production   # keeps production's spans apart from the previews'
```

**The summary.** This prints `healthy` and an `issues` list. Every signal in the table below
marked "issue" is judged there (D82), so the summary alone says whether to look further:

```sh
node tools/scenario-cli/src/scenario-cli.ts measure health --collector-url "$SHALEDB_TRACE_COLLECTOR_URL" --minutes 60
```

Before the span commands below, bring the last hour's spans into `$S`. Run this again to resume
where it stopped:

```sh
node tools/trace-cli/src/trace-cli.ts pull --collector-url "$SHALEDB_TRACE_COLLECTOR_URL" --store "$S" --since-minutes 60
```

| Signal                       | Production command                                                                                                                                  | Act when                                                                                               |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Invocations that hit a limit | `measure health`: issue `… invocation ended exceededCpu` (or `exceededMemory`, `exception`), and the per-Worker `invocations` outcomes              | Any. Limits are 30 s (API, writers) and 60 s (compactor)                                               |
| Writes stalled               | `measure health`: issue per database and reason (`L0Backlog`, `UnflushedBytes`); `trace-cli grep --store "$S" --name DbWriter.write --error`        | Sustained: L0 is not draining (backlog, corruption, a fenced compactor), or a writer is overloaded     |
| Flush duration               | `measure health`: `flushMs`, issue above 30 s at p99; `trace-cli stats --store "$S" --name DbWriter.flush`                                          | p99 above about 30 s, or rising                                                                        |
| Compaction backlog           | `measure health`: `l0Backlog.deepest` (5 deepest databases), issue at 16                                                                            | Near `l0MaxSsts` (16), where writes stall                                                              |
| Fenced compactor             | `measure health`: issue per database; `curl -H "Authorization: Bearer $SHALEDB_TOKEN" "<apiUrl>/v1/databases/<id>/compaction"` (`compactor.fenced`) | Any: since D78 it fences in again on its next start, but its state was overwritten, a defect to report |
| Corrupted SSTs               | `measure health`: issue and `corruptSsts`, from each database's latest poll; `GET …/compaction` (`corruptSsts`)                                     | Any: follow the corruption runbook in `docs/operations.md`                                             |
| Unverified parts             | `measure health`: issue and `uploads.withUnverifiedPart`                                                                                            | Any: R2 stopped returning part digests                                                                 |
| Re-sent parts                | `measure health`: `uploads.withRetriedPart` (a count, not an issue)                                                                                 | Frequent                                                                                               |
| Garbage collection           | `measure health`: issue and `gc`; `trace-cli grep --store "$S" --name GarbageCollector.collect --error` (and `MaintenanceSweep.sweepPage`)          | Repeated failures: storage unreachable or a manifest unreadable                                        |
| Error spans                  | `measure health`: `errorSpans`; `trace-cli stats --store "$S" --error`                                                                              | A new name in the list                                                                                 |

The `trace-cli` commands are `node tools/trace-cli/src/trace-cli.ts …`. The compaction status
keeps its shape after D78; `fenced` is true only between the poll that saw a newer epoch and the
compactor's next start (D78).

These errors are expected and are not trouble: `DatabaseSealedError` (a stale router retrying),
`TransactionConflictError`, and `DatabaseAlreadyExists` on create.

To follow spans as they arrive:

```sh
node tools/trace-cli/src/trace-cli.ts tail --collector-url "$SHALEDB_TRACE_COLLECTOR_URL" --store "$S"
```

For server-side latency per operation over the last 15 minutes:

```sh
node tools/scenario-cli/src/scenario-cli.ts measure latency --collector-url "$SHALEDB_TRACE_COLLECTOR_URL" --minutes 15
```

None of these commands has been run against a live collector yet (section 9).

**Signals added in audit round 2** (D94, D98, D101, D102):

| Signal                                     | Production command                                                                                                                                                                                | Act when                                                                                         |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| SST collection deferred (D98)              | `measure health`: an issue naming the database; the `gc.sst_collection_deferred` and `gc.retained_manifests` attributes on `GarbageCollector.collect`                                             | It persists: the database commits faster than about 1.4 a second, and its unreferenced SSTs wait |
| A compaction job finished unseen (D94)     | `node tools/trace-cli/src/trace-cli.ts grep --store "$S" --name Compactor.poll --json \| jq '[.[] \| select(.attributes["compaction.job_abandoned"])] \| length'`; not judged by `measure health` | More than rarely: the Workflow launcher cannot read its jobs                                     |
| A split finished by a later request (D101) | `measure health`: an issue naming the split, from `shard.resumed_split` on `ShardSplit.resumePending`                                                                                             | Any: a split died, and the writes to its range waited up to 30 s                                 |
| The writer's alarm backing off (D102)      | PENDING(d102): the signal D102 adds                                                                                                                                                               | —                                                                                                |

## 12. Rollback

From `docs/operations.md` ("Rolling back"). Redeploy the previous commit with the same task
(`vp run @shaledb/infra#deploy:production` from a checkout of that commit). A redeploy leaves
Durable Objects (writers, compactors, the shard directory) and everything in R2 untouched. Writes
acknowledged before it are in each writer's SQLite or in R2, and the new code reads both.

The manifest and the compactor state carry a format version (D64). Code refuses data in a newer
format rather than rewrite it. A rollback across a release that raised a format therefore stops on
those databases with `StoredDataInvalid`, naming the version to deploy, and damages nothing. Roll
forward again to recover. v1 is format 1, the format of all data written so far, so rolling back
from v1 to an earlier v1 commit crosses no format change.

A rollback to a commit before D76, D77, or D78 brings back those defects (section 8).
D77 and D78 change no stored format: the compactor state keeps format 1, and D78's
read-modify-write needs no new field. D94's `lastProgressAt` and D103's `recentCompactionJobIds` are
optional fields that older code ignores. A rollback past D77 or D78 brings back those defects.

Nothing removes production. Its LSM bucket is retained (`RemovalPolicy.retain` in
`packages/lsm-cloudflare/src/lsm-bucket.ts`) and is not `forceDestroy`.
