# shaledb v1 release checklist

The checklist Dillon reads before deciding P6 (`docs/handoff-v1.md` §10.3). Every claim here names
the file, commit, test, model, or measurement it comes from, so it can be checked in the
repository. A line starting `PENDING(<id>):` is a result still being produced. It is not a claim
until the line is replaced.

PENDING(final-commit): the release commit hash on `main` that this checklist describes, with
`vp run ready` and `vp run verify` observed at exit 0 on it.

This pass describes the integration head, `proto/integration-2` at `c149afb` (2026-11-27 03:25 UTC). Where a
statement holds only at an earlier commit, it names that commit.

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

## Second review of `d89beba`, and what was done

Dillon reviewed `d89beba` (the head of `main`) a second time and raised three points. Each is
below with its decision, its tests, and what is still open.

1. **Every public scan surface must do bounded work, hold bounded memory, and say how it ended.**
   Limited regular scans could already end early on the work budget (D108). But the transaction
   scan (`DbClient.scanAt`) collected every writer page and passed no budget, and the sharded scan
   (`ShardRouter.scanFrom`) passed `limit: none` to each shard. The test asked for: limit 1 over a
   long range of deleted keys, on all three surfaces, without mixing transaction snapshots.
   - **Decision: D125.** One examined-bytes budget per page (`ScanPageBudget`, 16 MiB) on the
     regular, transaction, and routed page. A page ends with rows and `resumeFrom`, with fewer
     rows or none and `resumeFrom`, or with a typed refusal. The transaction and sharded responses
     gain a nullable `resumeFrom`, an additive change. A transaction's pages all read at its
     `readSeq`.
   - **Tests:** `scan-bounds.test.ts` (the reviewer's test in the writer, in L0, and in a sorted
     run, with deleted, expired, and overwritten keys); `shard-scan-bounds.test.ts`; the oracle
     properties `scan-oracle.property.test.ts` and `shard-scan-oracle.property.test.ts`; and in
     workerd, `writer.workerd.ts` ("scan and transaction scan pages end at their work budget with
     resumeFrom"). Measured before the fix: one row from a transaction scan over 5,000 unflushed
     keys read 6 writer pages and held all 5,000 rows (D125).
   - **Open:** the sharded scan has no workerd assertion, because no workerd test hosts the shard
     router. Its router is covered in-process and its HTTP shape only by the schema. `scan-bounds`
     started on it at 03:20 UTC (`/tmp/shale-work/agents/scan-bounds/STATUS.md`).
2. **Snapshots freeze write visibility, not TTL time.** A pinned snapshot's value can expire
   between two reads of it.
   - **Decision: none needed.** The code already does this: expiry is judged with the time of each
     read (`resolveVisibleRow`, with `now` taken per point read in `DbReader.getRow` and per call
     in `DbReader.scan`). No D126 was taken.
   - **Docs:** `docs/invariants.md` ("What each read may see") states the rule with a worked example.
     The README, five OpenAPI descriptions, and `docs/api-compatibility.md` say the same.
     Serializability is stated over writes: an expiry is not a write, so a transaction that read a
     value which then expires still commits.
   - **Tests:** `snapshot-ttl.test.ts` (5 tests): an unpinned and a pinned snapshot, a transaction's
     `getAt` and its commit after the expiry, a fork, and two scan pages.
   - **Open:** nothing.
3. **Publish the matched baseline, and keep provisional measurements apart from the gates on the
   final commit.**
   - **Done.** The comparison with a plain Durable Object SQLite table is in the README ("Compared
     with a plain Durable Object SQLite table") and in `docs/measurements.md` ("Baseline"): same
     stage, workload, client, and placement (D75). Both say it is one provisional run on
     `ed4e787`.
   - **Every number in this checklist now says what it rests on:** measured on the release commit,
     measured on an earlier commit (naming it, and whether its code path changed since), or
     derived. The rule is at the top of section 3.
   - **Open:** PENDING(baseline), three runs on the release commit. Nothing here is measured on the
     release commit yet.

## 2. Workstreams 1 to 10

- **On `main`:** workstreams 1 to 9 and most of 10, up to `d89beba` (2026-11-26 21:12 EST): 148
  commits since `f3b2ad2` (`git log --oneline f3b2ad2..main`), through D108.
- **On `proto/integration-2` only:** 82 commits past `main` (`git log --oneline
  main..proto/integration-2`), merged up to `c149afb`. They hold:
  - the formal models D109 to D114, and the fixes they found, D115 and D116;
  - D113, a defect recorded by an expected-failing test and not fixed;
  - the mutation sweeps D117, D118, D122, D127 (tests only), and D128;
  - D119, D121, D123, D125, and D129.
- **Still on their own branches:** the `scan-bounds` agent's round 3 (`proto/audit-api`, `edfe57b`: the
  corrupted-SST answer, D128's defect) and one more D127 test (`proto/audit-workflow`, `3bb39a4`). D116 and D127 have no entry in, and D116 and D127 have no entry in
  `docs/design-decisions.md` yet: their tests and code cite them.

The status of each workstream comes from the workstream table in `docs/progress.md`.

| #   | Workstream                            | Done                          | Commit(s)                                                                                                                                                                                                                                                                                                   | Decisions                        |
| --- | ------------------------------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| 1   | The Stack owns its configuration      | yes                           | `012bdb5`                                                                                                                                                                                                                                                                                                   | D55 (supersedes D54's mechanism) |
| 2   | Fast manifest commits                 | yes                           | `774ed7b`                                                                                                                                                                                                                                                                                                   | D56                              |
| 3   | Splits pause briefly, never fail      | yes                           | `393758c`                                                                                                                                                                                                                                                                                                   | D57                              |
| 4   | Transactions exact across flushes     | yes                           | `5f2850b`, `5df4099` (the D59 routing fix)                                                                                                                                                                                                                                                                  | D58, D59                         |
| 5   | GC and maintenance that scale         | yes                           | `e06f3eb`                                                                                                                                                                                                                                                                                                   | D60                              |
| 6   | Writer throughput and limits          | yes                           | `308defd`                                                                                                                                                                                                                                                                                                   | D61                              |
| 7   | Corruption: prevent, detect, repair   | yes                           | `2cb5116`                                                                                                                                                                                                                                                                                                   | D62                              |
| 8   | Production telemetry                  | yes                           | `62eab3d`                                                                                                                                                                                                                                                                                                   | D63                              |
| 9   | Production readiness                  | yes                           | `8a96667`                                                                                                                                                                                                                                                                                                   | D64                              |
| 10  | Soak, release checklist, the go-ahead | no: soaks pending, P6 pending | `a506228`, `fe552f0`, `18aa655`, `a98eb15`, `93c337c`, `04df0f9`, `9776317`, `5bedcbd`, `c72e0f3`, `c9e7487`, `4082f97`, `335b775`, `af51ae2`, `3d9f70f`, `dddd49a`, `8a36402`, `6f9c4ab`, `fec2698`, `ab5b929`; then the merges through `d89beba` on `main` and through `c149afb` on `proto/integration-2` | D65–D129                         |

## 3. Targets

Every target `docs/handoff-v1.md` sets, with what was measured. Measurements are on preview stages
with production settings unless the row says otherwise. Links go to `docs/measurements.md`.

**What each number rests on.** Every number in this checklist and in the README carries one of
three labels, in the "Basis" column here and inline elsewhere:

- **release commit:** measured on the release commit. None is yet. Each such number is a
  `PENDING` line until it is.
- **earlier commit `<c>`:** measured on commit `<c>`. The label says whether the code on that
  path has changed since, from `git diff <c>..c149afb -- <the path's files>`. "Path changed"
  means the number is evidence about `<c>`, not about the release.
- **derived:** computed from other numbers or from Cloudflare's published limits, not measured.

Every row below rests on an earlier commit, and every one of those paths has changed since. The
files checked for each are in the Basis cell.

| Target (handoff section)                                                                                                                                                               | Measured                                                                                                                                                       | Met                                 | Where                                                                                                                                                                     | Basis                                                                                                                   |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| WS1: a deploy without overrides binds production values on every Worker                                                                                                                | `preview-1` `--dev-timings` then a bare `deploy:preview-1`: every Worker bound the complete production settings and `none`                                     | yes                                 | D55; `docs/progress.md` v1 log item 1                                                                                                                                     | earlier commit `012bdb5`; path changed (`shaledb-stack-config.ts`, `lsm-worker-settings.ts`)                            |
| WS1: `eu` → `none` on `preview-3` binds `none` everywhere                                                                                                                              | every Worker bound `none`                                                                                                                                      | yes                                 | D55                                                                                                                                                                       | earlier commit `012bdb5`; path changed (same files)                                                                     |
| WS2: `ManifestStore.update` p50 ≤ 500 ms on `preview-1`                                                                                                                                | 357 ms (from 1,092 ms)                                                                                                                                         | yes                                 | [Manifest commits and small flushes](measurements.md#manifest-commits-and-small-flushes-v1-workstream-2-d56)                                                              | earlier commit `774ed7b`; path changed (`manifest-store.ts`, `db-writer.ts`)                                            |
| WS2: small-memtable flush p50 ≤ 900 ms on `preview-1`                                                                                                                                  | 709 ms (from 1,508 ms)                                                                                                                                         | yes                                 | [same section](measurements.md#manifest-commits-and-small-flushes-v1-workstream-2-d56)                                                                                    | earlier commit `774ed7b`; path changed (same files)                                                                     |
| WS2: R2 PUT latency per region measured                                                                                                                                                | create-if-absent ~285 ms beside the bucket to ~1 s from APAC; `SHALEDB_BUCKET_LOCATION` added                                                                  | yes                                 | [R2 latency by region](measurements.md#r2-latency-by-region-v1-workstream-2-d56)                                                                                          | earlier commit `774ed7b`; R2 itself, and `object-store.ts` changed                                                      |
| WS3: split write pause p50 ≤ 6 s, zero failed writes, 4 and 16 writers                                                                                                                 | median 2.7 s (4 writers) and 3.1 s (16); 0 failed writes in six runs                                                                                           | yes                                 | [Split write pause after D57](measurements.md#split-write-pause-after-d57-v1-workstream-3)                                                                                | earlier commit `393758c`; path changed (`shard-split.ts`, `db-writer.ts`, `database-lifecycle.ts`)                      |
| WS4: transactions held open 20 s across several flushes, zero `SnapshotTooOld`                                                                                                         | 24/24 committed, each held ~21.7 s across 6–7 flushes, 0 `SnapshotTooOld`                                                                                      | yes                                 | [Transactions held open across flushes](measurements.md#transactions-held-open-across-flushes-v1-workstream-4-d58)                                                        | earlier commit `5f2850b`; path changed (`db-writer.ts`, `db-client.ts`, `mem-table.ts`)                                 |
| WS5: 500 databases with garbage collected, no invocation over a limit                                                                                                                  | 500/500 collected by their own compactors; 15,517 of 15,535 invocations `ok`, 18 runtime `canceled`, 0 `exceeded*`. Run with `--dev-timings` (`gcMinAge` 10 s) | yes, with dev GC timings            | [Garbage collection across 500 databases](measurements.md#garbage-collection-across-500-databases-v1-workstream-5-d60)                                                    | earlier commit `e06f3eb`, dev GC timings; path changed (`garbage-collector.ts`, `compactor.ts`, `maintenance-sweep.ts`) |
| WS6: rows/s at 100- and 1,000-row batches recorded before and after                                                                                                                    | 100 rows: 2,454 → 6,547 rows/s; 1,000 rows: 7,678 → 15,257 rows/s                                                                                              | yes                                 | [Writer throughput and limits](measurements.md#writer-throughput-and-limits-v1-workstream-6-d61)                                                                          | earlier commit `308defd`; path changed (`db-writer.ts`, `mem-table.ts`)                                                 |
| WS6: 100-row rate at least doubles if the binary protocol is adopted                                                                                                                   | binary body not adopted (decoding is 14%); the 100-row rate rose 2.7× without it                                                                               | not applicable; doubled anyway      | [same section](measurements.md#writer-throughput-and-limits-v1-workstream-6-d61), D61                                                                                     | earlier commit `308defd`; path changed (same files)                                                                     |
| WS6: `limits.cpu_ms` set from measurement with 2× headroom (P5)                                                                                                                        | API 30 s (2 × 9.8 s peak, a 256 MiB flush); compactor 60 s (2 × 17.7 s peak step). Largest single invocation since: 7.7 s (a flush)                            | yes                                 | [same section](measurements.md#writer-throughput-and-limits-v1-workstream-6-d61); [D73's sweep](measurements.md#flush-and-compaction-cpu-block-size-and-part-uploads-d73) | derived: 2 × peaks measured at `308defd` and D73; path changed (`db-writer.ts`, `compaction-executor.ts`)               |
| WS6: does DO SQLite's page cache count against 128 MB                                                                                                                                  | not observable directly; a writer at 256 MiB unflushed, and two at once (six runs), never reported `exceededMemory`                                            | answered indirectly                 | [Not measured](measurements.md#not-measured); [D74 section](measurements.md#the-writers-alarm-and-status-under-a-full-memtable-d74)                                       | earlier commit `308defd`, answered indirectly; path changed (`mem-table.ts`)                                            |
| WS6: Workflows concurrent-instance limit                                                                                                                                               | the limit itself not found; 200 compactions at once all finished within 16.4 s                                                                                 | answered indirectly                 | [Not measured](measurements.md#not-measured)                                                                                                                              | earlier commit `308defd`; the limit itself is Cloudflare's published figure (derived)                                   |
| WS6: no `exceededCpu` or `exceededMemory` in any measurement                                                                                                                           | one `exceededCpu` (a 60 MiB flush under D58's queries) found and fixed in D61; none in any measurement since                                                   | yes for measurements; soaks pending | [Writer throughput and limits](measurements.md#writer-throughput-and-limits-v1-workstream-6-d61)                                                                          | earlier commits, each measurement's own; paths changed                                                                  |
| WS7: a corrupted part retried; `dropSst` unblocks L0 compaction; operator scenario                                                                                                     | `object-store.test.ts`, `compactor.test.ts`, the `repair` scenario locally and on `preview-1`; 134 of 134 live parts verified                                  | yes (tests, scenario)               | D62; [Upload integrity cost](measurements.md#upload-integrity-cost-v1-workstream-7-d62)                                                                                   | tests at HEAD; the 134 live parts at `2cb5116`, path changed (`object-store.ts`)                                        |
| WS8: redaction proven by a trace assertion, locally and on a preview                                                                                                                   | `redaction` scenario passes locally and on `preview-1`; a 6,193-span local run carries only allowed keys                                                       | yes                                 | [Trace sampling and redaction](measurements.md#trace-sampling-and-redaction-v1-workstream-8-d63)                                                                          | tests at HEAD; live at `62eab3d`, path changed (`lsm-trace-policy.ts`, D123)                                            |
| WS8: head sampling keeps traces whole                                                                                                                                                  | at 25%: 45 of 200 kept, all whole across the API Worker and the writer                                                                                         | yes                                 | [same section](measurements.md#trace-sampling-and-redaction-v1-workstream-8-d63)                                                                                          | earlier commit `62eab3d`; path changed (`lsm-trace-policy.ts`)                                                          |
| WS9: OpenAPI spec checked in; old manifests decode                                                                                                                                     | `docs/openapi/shaledb-v1.json` with a drift test; a workstream 1 manifest fixture decodes as format 1                                                          | yes (tests)                         | D64                                                                                                                                                                       | tests at HEAD                                                                                                           |
| WS10: soak ≥ 2 h on `preview-1`: no acknowledged write lost or hidden, no unexpected error status, no `exceededCpu`/`exceededMemory`, storage bounded after GC, p99 latencies recorded | see section 5                                                                                                                                                  | pending                             | section 5                                                                                                                                                                 | release commit: PENDING(soak-preview-1)                                                                                 |
| WS10: the soak's short form on `preview-2` (`eu`)                                                                                                                                      | see section 5                                                                                                                                                  | pending                             | section 5                                                                                                                                                                 | release commit: PENDING(soak-preview-2)                                                                                 |

The numbers for workstreams 2, 3, and 6 were measured before D70 to D129 changed the read, flush,
writer, and compactor paths. They are re-measured on the release commit:

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
`tools/formal-verification/src/formal-checks.ts`: at `c149afb`, **9 specs and 73 TLC runs**
(counted from the list). The code as written must pass. Each broken variant (a value of the spec's
`Broken` constant) must violate the named property. Bounds are the `CONSTANTS` of the `.cfg` files
in `tools/formal-verification/specs/`. Each check has a skill under `.agents/skills/verify-*` that
records its assumptions and sync commit.

All models assume three things: one writer instance per database (the Durable Object guarantee),
durable SQLite commits before an acknowledgement, and R2 create-if-absent. The last is confirmed on
real R2 by the `create-race` scenario. The four object models (D109 to D114) also assume what
D110 lists: the runtime retries an alarm an eviction cut off, a lease expires on time, and
`gcMinAge` is far longer than a job's launch.

**Atomic-step models** (each code step is one model step), with their liveness configs (D111):

| Spec and config                | What it proves                                                                                                                      | Bounds                                                                      | Broken variant → property it fails                                                                                                               | Runs |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ---: |
| `ManifestCasFencing.cfg`       | One manifest per id, a commit lands once, no fenced writer or compactor commits, acknowledged data stays visible, the head is found | 2 writers, 2 compactors, 1 job each, manifest ids up to 9, 2 flushes, no GC | `noWriterEpochCheck` → `NoStaleCommit`; `blindCreate` → `CreateOnce`                                                                             |    3 |
| `ManifestCasFencingRace.cfg`   | The same, with a compaction whose inputs changed before it committed                                                                | 1 writer, 1 compactor, 2 jobs, ids up to 8, 2 flushes                       | `noInputsCheck` → `AckedDataVisible`                                                                                                             |    2 |
| `ManifestCasFencingGc.cfg`     | The same while GC deletes manifests and hint writes land late or never                                                              | 1 writer, 1 compactor, 1 job, ids up to 7, 2 flushes, GC on                 | `noBoundaryReread` → `CommitOnce`; `hintOnlyProbe` → `LoadFindsHead`; `knownIgnoresBoundary` → `LoadFromKnownFindsHead`                          |    4 |
| `ManifestCasFencingRemove.cfg` | The same while `remove` runs with commits in flight: a removed database never comes back (D91)                                      | 1 writer, 1 compactor, 1 job, ids up to 8, 2 flushes, no GC                 | `removalDeletesHead` → `NoResurrection`                                                                                                          |    2 |
| `NoAckedWriteLost.cfg`         | No acknowledged write lost across eviction, crash, flush, trim, and compaction; `Latest` sees every acknowledged write              | 4 writes, 2 crashes, 2 reads                                                | `truncateBeforeCommit`, `trimUnflushed` → `NoAckedWriteLost`; `nonAtomicProbe`, `noSnapshotUpperBound` (D70) → `LatestSeesAcked`                 |    5 |
| `GcSafety.cfg`                 | GC never deletes an SST a kept manifest, a pin, or a checkpoint references, nor an upload whose commit can succeed                  | 2 SSTs (symmetric), clock 0–3, min age 1, 5 manifests                       | `noBoundaryCheck`, `ownUploadAge`, `dropDeletesObject` → `ReadableManifestsComplete`; `ignorePins` → `PinnedManifestsComplete`                   |    5 |
| `GcSafetyFork.cfg`             | A fork releases its hold only once no manifest a reader of the fork may hold names the source's SSTs (D77)                          | 1 SST, clock 0–3, min age 1, up to 6 manifests                              | `earlyForkRelease` → `ForkReadsComplete`                                                                                                         |    2 |
| `SealSplit.cfg`                | After a seal no write commits; a split leaves every write acknowledged before the seal in exactly one successor                     | 3 keys, 4 writes                                                            | `sealWithoutLock`, `checkpointBeforeSeal`, `overlappingRanges` → `EveryAckedWriteInExactlyOneSuccessor`; `writerNotFenced` → `NoCommitAfterSeal` |    5 |
| `Transactions.cfg`             | `SerializableSnapshot` admits only serializable histories, and every read returns the begin state (D76)                             | 2 keys, 2 transactions (symmetric), sequence numbers up to 5                | `snapshotOnly`, `trimWithoutAbort` → `Serializable`; `noReadHorizonCheck`, `noReadUpperBound` → `ReadsAtBegin`                                   |    5 |
| `NoAckedWriteLostLive.cfg`     | Every acknowledged write is eventually readable at `Flushed` (liveness, D111)                                                       | as `NoAckedWriteLost.cfg`                                                   | `noWake` (nothing starts an evicted writer) → `EventuallyFlushed`                                                                                |    2 |
| `TransactionsLive.cfg`         | A transaction retried after each abort eventually commits, with GC deleting a flush's manifest (liveness, D111)                     | 2 keys, 2 transactions, sequence numbers up to 3, `Live` on                 | `beforeD76` → `EveryTxnCommits`                                                                                                                  |    2 |

**Await-granularity and end-to-end models** (D109 to D116): a label at every `yield*` that leaves
the object, every lock, and every gap between two SQLite statements; eviction and interruption at
any label.

| Spec and config             | What it proves                                                                                                                       | Bounds                                                                                     | Broken variant → property it fails                                                                                                                                                                               | Runs |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---: |
| `WriterObjectSeal.cfg`      | The writer object with seals: never fenced by itself, no acknowledged write lost, no acknowledgement after a seal                    | 2 requests (symmetric), 1 key, write/flush/seal, 3 ops, 1 eviction, 1 interrupt, 1 failure | `sealKeepsPhase` (D85) → `SealNotHalf`; `beforeD115`, `markFencedAnyPhase` (D115) → `NeverSelfFenced`; `noAlarmRetry` (D102) → `AlarmCovers`                                                                     |    5 |
| `WriterObjectTxn.cfg`       | The same with transactions: serializable, counters always cover SQLite                                                               | 2 requests, 2 keys, write/txn, 3 ops, 1 interrupt, no eviction                             | `noHorizonReread` (D84) → `Serializable`; `noGeneration`, `interruptedWrite` (D74b) → `CountersCover`                                                                                                            |    4 |
| `WriterObjectLive.cfg`      | Every acknowledged row is eventually flushed (liveness)                                                                              | 1 request, 1 key, write/flush, 2 ops, 1 eviction, 1 interrupt, 1 failure                   | `noAlarmRetry` (D102) → `AckedFlushed`                                                                                                                                                                           |    2 |
| `WriterObjectAlarmLoop.cfg` | The alarm quiets once nothing is unflushed (liveness)                                                                                | as `WriterObjectLive.cfg`                                                                  | `noSettle` (D74's loop) → `AlarmQuiets`                                                                                                                                                                          |    2 |
| `CompactorObject.cfg`       | The compactor object: the epoch never goes back, an outcome is true, the lease holds its capacity                                    | L0 up to 3, 4 flushes, epochs up to 2, 1 eviction                                          | `twoFenceIns` (D78) → `EpochNeverRegresses`; `noJobRecord` (D103) → `OutcomeTrue`                                                                                                                                |    3 |
| `CompactorObjectL0.cfg`     | L0 at its limit is eventually compacted (liveness)                                                                                   | as above                                                                                   | `beforeD78`, `gcIgnoresRunning` (D96) → `L0Drains`                                                                                                                                                               |    3 |
| `CompactorObjectJobs.cfg`   | Every running job eventually leaves (liveness)                                                                                       | as above                                                                                   | `noUnseenBound` (D94) → `RunningJobsLeave`                                                                                                                                                                       |    2 |
| `ShardSplit.cfg`            | A split end to end: every shard write in its successor, holds match the seal, a feed consumer misses nothing, staleness bounded      | 4 values, 1 crash                                                                          | `noFlushedRefresh` (D89) → `NoStaleBeyondBound`; `resealOthers` (D95) → `HoldsMatchSeal`; `feedGap` (D80) → `ConsumerMissesNothing`                                                                              |    4 |
| `ShardSplitLiveness.cfg`    | Writes waiting on a split eventually land (liveness)                                                                                 | 4 values, 1 crash                                                                          | `noResume` (D101) → `WaitingWritesLand`                                                                                                                                                                          |    2 |
| `Lifecycle.cfg`             | A source, a fork, and a fork of the fork: no reader needs a deleted SST, a removed database never reopens, no old incarnation served | fixed by the spec (three databases); `ForkRace` on                                         | `releaseNewestOnly`, `releaseIgnoresPins` (D77), `deleteHold` (D91), `beforeD116` (D116) → `NoReaderNeedsDeleted`; `deleteHead` (D91) → `RemovedNeverReopens`; `recreateAtOnce` (D90) → `NoOldIncarnationServed` |    7 |
| `LifecycleLiveness.cfg`     | Holds are eventually released (liveness)                                                                                             | `ForkRace` off                                                                             | none                                                                                                                                                                                                             |    1 |
| `LifecyclePrefix.cfg`       | A removed database's prefix is eventually emptied (liveness)                                                                         | `ForkRace` off                                                                             | none                                                                                                                                                                                                             |    1 |

`LifecycleDeepLiveness.cfg` and `LifecycleDeepPrefix.cfg` (with `ForkRace` on) are in `specs/` but
not in `FORMAL_CHECKS`, so `vp run verify` does not run them.

**Not in the list:**

- **The change feed and paged reads (D113):** no model. Placing its labels found a defect, which
  `db-writer.test.ts` records as an expected failure (section 8).
- **formal2's `noStateLock` variant** (D116) is recorded in its STATUS, not in `formal-checks.ts`.
  The compactor's `stateLock` and `planLock` are covered by unit tests instead (`compactor.test.ts`,
  D116).

The models are bounded. They check every interleaving within these bounds, not beyond them. What
they leave out is part of section 8.

Code-level properties are property tests with mutation checks (`docs/coverage-matrix.md`). They
cover codecs, the merge, the compaction merge, GC plans, the client and transactions against
oracles, scan pages against an oracle (D125), and fault injection. The mutation sweeps are in
section 8.

PENDING(final-commit): the number of runs `vp run verify` reports on the release commit, observed at
exit 0. Observed so far: 28/28 on the D70–D74 code (`docs/progress.md`); on `7dfed3f`, 32 of 33
with `transactions` past its time limit under load, then 5/5 alone in 413 s, after which the limit
was raised to 900 s (`/tmp/shale-work/release2/NOTES.md`, `29eb3d2`). The 73 runs listed at
`c149afb` have not been observed in one run. The formal agents ran each new spec's runs alone (D109
to D116). For D111, `Transactions.cfg` with the code as written timed out at 420 s with 2 TLC
workers under load (14.4M of its 40.6M states) and is to be rerun (`docs/progress.md`).

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

Since D106 the workload also does what the audits' defects needed, each with a model checked during
the soak (`tools/scenario-cli/src/soak-checks.ts`; the list and its `checks` keys are in
`docs/testing.md`, "The soak's workload"):

| Workload                                            | Defect class it exercises                     | Checked by                                                                                      |
| --------------------------------------------------- | --------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| follows the change feed, across a split             | D80, D93b: changes skipped or split           | `ChangeLedger`, `FeedFollower`: pages above the cursor in order; every acknowledged batch whole |
| merge operands with TTLs, read across their expiry  | D81: compaction folding expiries              | `MergeKeyModel`: each part expires on its own, within 5 s of clock skew                         |
| scans continued page by page                        | D93, D108: paged scans                        | `ScanTraversal`: keys strictly increase; a key live throughout is returned                      |
| bursts of flushes                                   | D78: compactor wakes overlapping              | `burst.flushes`, `burst.compactor`                                                              |
| an idle database, read every 20 s                   | D76: liveness after GC (reached past ~65 min) | `idle.get`, `idle.transaction`, `idle.transaction_after_gc_age`                                 |
| forks from a checkpoint; deleting a fork's hold     | D77, D91                                      | `fork.read`, `final.fork`, `fork.hold_delete_refused`                                           |
| recreating a removed id                             | D90                                           | `remove.recreate_refused`                                                                       |
| aborted writes and transaction commits; big batches | D85, D88: requests cut off                    | `write.aborted`, `transaction.aborted`, `final.aborted_key`, `write.big_batch`                  |

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
`tools/scenario-cli`.

- **Exit status.** The soak exits non-zero unless the report says `passed: true` (`1da8049`,
  `soakFailureMessage`). Before that fix, a failed soak exited 0; the first live test of the rebuilt
  tool found it.
- **Telemetry budget.** `--telemetry-budget-minutes`, default 10. The release soaks run with 20
  (`/tmp/shale-work/scripts/final-soaks.sh`). At 100% sampling, 10 minutes was too short for the
  health report on the first D106 run (below).

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

### Evidence, not the release gate

Each run below is labeled with its commit. None ran the release commit.

**`preview-2` (`eu`), 21.6 minutes, `d89beba`: passed** (`/tmp/shale-work/final/soak-p2-d89beba.report.json`).

- **Basis:** earlier commit `d89beba`, the head of `main`. The path changed since: 47 files in the three packages' `src` differ at `c149afb`, 12 of them not tests, among them the writer (D115), the lifecycle (D116), the
  scans (D125), and the checkpoint cap (D129).
- **The run.** Started 02:14:31 UTC and ended 02:46:16, exit 0 (`/tmp/shale-work/final/log`).
  The command was `soak --stage preview-2 --minutes 20 --databases 3 --keys-per-database 1000
  --redeploy-at 10 --split-at 5,14 --trace-sample-rate 0.1 --telemetry-budget-minutes 20`, after
  a deploy with `--jurisdiction eu --trace-sample-rate 0.1` (`final-soaks.sh`).
- **Result.** `passed: true`, phase `done`. No mismatch, no failure, no unexpected error span. The
  one error span by exception was a `DatabaseHasDependentsError`, which is expected.
- **The D106 checks.** 2,417 feed pages, 30 batches delivered, and 6 final feed checks. 705 merge
  reads and 510 after an expiry. 103 paged scans. 5 flush bursts. 57 idle reads and 6 idle
  transactions. 4 fork reads, and the hold delete refused once. The recreate refused twice. 83
  aborted writes, 78 aborted commits, 64 big batches, and 234 aborted keys checked at the end.
- **Other checks.** Gets: 4,147 at `Latest`, 1,741 at `AtLeast`, and 1,124 at `Flushed`. 636 scans,
  1,252 transaction commits, 2 splits, 2 removals, and 1 redeploy. Every key of the 3 databases and
  of the sharded group was read at the end at `Latest` and `Flushed`.
- **Retries.** All 78 were transaction commits that timed out and were retried: the aborted-commit
  workload. The 3 counters committed 427, 413, and 412 increments, with 21, 24, and 33 uncertain.
- **Storage.** No SST older than the bound beyond what each manifest uses.
- **Telemetry.** 888 spans and 6,205 invocation records checked. From the sample: 53,090 API Worker
  invocations `ok` and 284 `canceled`, 590 compactor `ok` and 1 `canceled`, and no `exceeded*`.
- **`measure health`, last 15 minutes:** healthy, no issue, 23 flushes (p50 495 ms, max 1,457 ms),
  deepest L0 backlog 5, no corruption, no GC failure.

| Operation (client, ms, US client to `eu`) |     n |   p50 |   p99 |    max |
| ----------------------------------------- | ----: | ----: | ----: | -----: |
| get `Latest`                              | 4,147 |   160 |   918 |  1,490 |
| get `AtLeast`                             | 1,741 |   161 |   878 |  1,409 |
| get `Flushed`                             | 1,124 |   301 | 1,118 |  1,470 |
| scan                                      |   636 |   451 | 1,675 |  2,054 |
| write                                     | 3,888 |   192 |   582 |  1,229 |
| sharded put                               | 3,602 |   167 |   242 |  2,416 |
| feed page                                 | 2,417 |   177 | 1,316 | 22,188 |
| transaction commit                        | 1,252 |   176 |   322 |    873 |
| split                                     |     2 | 2,553 | 2,553 |  2,553 |

PENDING(soak-preview-1-d89beba): the 130-minute soak on `d89beba` on `preview-1`, started 02:14:31
UTC with 6 databases, splits at minutes 20 and 80, a redeploy at 65, and `--telemetry-budget-minutes
20`. At 03:23 UTC its report file said phase `workload`, 68 minutes in, no mismatch and no failure
(`/tmp/shale-work/final/soak-p1.report.json`). It is evidence about `d89beba`, as above.

**The first live run of the D106 workload: `preview-3`, `7dfed3f`, 12 minutes, 100% sampling,
01:46–02:10 UTC** (`/tmp/shale-work/release2/NOTES.md`).

- **Data checks:** all passed, no mismatch. Among them: 1,268 feed pages, 555 merge reads and 368
  after an expiry, 60 paged scans, 3 flush bursts, 34 idle reads, 3 fork reads, the hold delete and
  the recreate refused, and 37 aborted writes.
- **Not passed, for two tool reasons.** The remover's own check met D91's tombstone
  (`DatabaseRemovedError`, now expected: `d89beba`). And the health report did not finish within
  10 minutes at 100% sampling.
- **It exited non-zero with `passed: false`,** as it should. The failure line comes after the JSON
  on stdout, so read `--report-file`, not stdout.
- **On the same deploy:** 13/13 scenarios on `preview-2` (`eu`, dev timings) at `e147572`. On
  `7dfed3f`, `block-cache` had failed its `NoErrorStatus` check: D90's create check left an error
  span on every create (section 8).

The runs on `f3b2ad2` below ran the code before D76, D77, D78, and the counter fix. They say
nothing about those changes, and they used the soak tool from before its rebuild. The path has
changed since: 117 files in the three packages' `src` differ at `c149afb`, 49 of them not tests.

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

## 7. What changed late (D67 to D129)

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
- **D90, D91, a removed database's id.** A reader that cached the removed database's manifest
  served it as the new one's, and a commit landing after the removal brought the database back. The
  `Removed` manifest now stays at the head for `gcMinAge` as a tombstone, and `create` refuses the
  id until then (`DatabaseRecentlyRemoved`, 409). Model: `ManifestCasFencingRemove.cfg`, whose
  broken variant `removalDeletesHead` fails `NoResurrection`. Found by the lifecycle audit.
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
- **D102, one alarm retry rule.** A failing flush retried every second forever, and an unreadable
  `writer_meta` set no alarm at all. Now both the writer's and the compactor's alarms back off from
  1 s, doubling to a ceiling, and never throw. The stalled-write reload and two concurrent
  schedulers no longer move the writer's view or its alarm back. Found by the Durable Object audit.
- **D103, a retried commit of a compaction with no outputs, and the size of a large compaction.** A
  commit records its job id, so a retry is recognized whatever its outputs. A missing Workflow
  instance is finished. The needs of a large compaction are tabled: the largest run live is about
  0.5 GiB, and Workflows' 10,000 steps per instance stop a bottom merge of a database above about
  625 GiB. Found by the Workflow audit.
- **D104.** What the first production deploy creates, its tokens, and its smoke test, read from the
  code and never run (the production audit).
- **D105.** Rollback against the stored formats: what `f3b2ad2` and `c3807f7` do with today's
  records, with fixtures.
- **D106, the soak exercises the classes of defect the audits found.** The change feed, merge
  operands with TTLs, paged scans, bursts of flushes, idle databases, forks from checkpoints,
  recreated ids, and aborted requests, each with a model checked during the soak.
- **D107, an SST's metadata grew with its keys.** At 16 KiB keys the index was twice the data, and
  a flush of such keys passed the isolate's memory on every attempt, so writes stalled. A block now
  holds enough rows to keep metadata near 3%. Found by the read audit.
- **D108, one scan page could read a whole range of deleted versions,** past the API Worker's CPU
  on every request. A page now examines at most 16 MiB (`SCAN_PAGE_EXAMINED_BYTES`) and ends early
  with `resumeFrom`. Found by the read audit.
- **D123, span events bypassed the trace policy.** A log line's object parts and annotations were
  exported unfiltered; no call site put a key there, but nothing stopped one. Events now pass the
  same allowlist as attributes (this audit).

- **D109, the writer Durable Object at await granularity.** `WriterObject.tla` (PlusCal): two
  requests and the alarm, a label at every await, eviction and interruption anywhere. D74b, D84,
  D85, D102, and D74's loop are each a broken variant that fails a named property. It found D115.
  It also found a risk, not fixed: a write evicted after its SQLite commit and before
  `scheduleFlush` leaves a durable, `Latest`-visible row with no alarm until the next request
  (section 9).
- **D110, the compactor Durable Object modeled.** Two wakes, the alarm's poll, Workflow jobs whose
  commit may run twice, the budget's lease, and GC. D78, D94, D96, and D103 each fail a property.
  Three lassos while building it were modeling errors that name what the code relies on (section 4).
- **D111, liveness.** `AckedFlushed`, `AlarmQuiets`, `EventuallyFlushed`, and `EveryTxnCommits`,
  each failed by the variant for D102, D74's loop, an unwoken writer, and D76.
- **D112, a split end to end.** `ShardSplit.tla`: crashes after any step, stale routers, a stale
  `Flushed` reader, a rogue seal, and a feed consumer. D80, D89, D95, and D101 each fail.
- **D113, a defect found while placing a model's labels, not fixed.**
  - **What is wrong.** After a flush commit whose response was lost, the change feed returns a
    sequence number twice.
  - **Where it comes from.** The next flush exports the same rows again, and the manifest holds
    two entries.
  - **Not affected:** L0, where the merge removes the duplicate rows.
  - **State.** Recorded as an expected-failing test in `db-writer.test.ts`. Two fixes are proposed;
    the owners decide which.
- **D114, the lifecycle across three databases.** `Lifecycle.tla`: holds, rewrites, releases, GC,
  `deleteCheckpoint`, removal, and recreation. D77 (both forms), D90, and D91 (both forms) each
  fail. It found D116's fork-hold race.
- **D115, a writer fenced by its own restart.** A flush refused at an older epoch set the phase to
  `Fenced` over the restarted writer's newer one. Every later request then failed with
  `WriterFencedError` until the object was evicted. TLC found two paths, and both were replayed as
  failing tests. `markFenced` now replaces only its own epoch's `Ready` phase, and a seal forgets
  its phase only once its edit was applied.
- **D116, found by the models (no decision entry yet; the tests and code cite it).**
  - **The fork-hold race.** `deleteCheckpoint` between `forkRange`'s hold and the fork's first
    manifest deleted the hold, so the fork could lose its data. A holder with no manifest now
    counts as gone only once the hold is `gcMinAge` old (`ccd48db`).
  - **A job launched before it was recorded.** If the poll was evicted between the launch and the
    record, the job ran unrecorded, and `dropSst` could commit while it read the dropped SST, so
    the dropped rows came back. Now a job is recorded before it is launched (`f83cd56`).
  - **The locks.** `stateLock` and `planLock` are pinned by unit tests.
- **D117, D118, the mutation sweep of `lsm-core`.** 800 mutants. No wrong behavior found. Tests now
  pin the Bloom filter's stored bits, the footer's seq bounds, and every limit. `BlockServer` error
  answers are `no-store`.
- **D119, a reused id's chain continues past its tombstone.** Two hazards D90 had left as risks,
  each with a failing test: an old snapshot read the new database's value, and an old compaction's
  commit passed its checks against the new chain. A chain created over a tombstone now starts
  past it. Found by working through D90's open list (`mut-read`).
- **D121.** `tokens ensure` and `rotate` fail when a written secret never turns active.
- **D122, D127, the mutation sweeps of the writer, the compactor, and the memtable** (both
  partial). Test gaps closed: the commit lock against a seal, the stored row kinds, the start
  failures, concurrent first requests, concurrent flushes, the trim's bounds, and a restarted
  writer scheduling its flush. No wrong behavior found.
- **D123, span events pass the trace policy.**
- **D125, every scan surface is bounded** (the second review, above).
- **D128, the mutation sweep of the request edge and sharding.** 165 mutants. It found one defect,
  by reading the code beside a survivor: a read of a corrupted SST answered `InternalFailure`,
  not the `StoredDataInvalid` the runbook promises. The fix is on `proto/audit-api` (`edfe57b`),
  not merged.
- **D129, the checkpoint cap is 128 per database.**
  - **Why.** At D99's maxima, a collection would read 1,000 pinned manifests of 4 MiB, 4 GiB in
    all: about 41 s of decoding (derived) against the compactor's 60 s CPU limit. No SST would be
    deleted again.
  - **The rule.** The cap is 512 MiB of pinned reads divided by the 4 MiB manifest budget.
  - **Found by** a test with 1,000 checkpoints and its arithmetic (`mut-read`).

## 8. Defects found, and what that says

**What the gates did.** On `f3b2ad2`, every gate passed: the 132-minute soak on `preview-1`, the
20-minute `eu` soak on `preview-2` (section 5), the 13 scenarios, `vp run ready`, and `vp run
verify` (28 runs). 31 of the 40 defects below were in that code. D70 had been fixed before it.
Eight came with fixes made after it: the two found in the runtime (with D100 and D90), D115a and
D115b (with D85's forget), D116a (the case D91's liveness check left open; before D91 any hold could be deleted), D119a and D119b (with D90's reuse
after `gcMinAge`), and D129 (with D99's cap). Gates that run the system and check its answers (soaks, scenarios, unit and property tests,
the atomic-step models) found none of them. The workerd tests and one live scenario found two at
integration; everything else was found by reading code against a stated rule, or by a model, a
measurement, or a sweep that went past what the gates did.

| Defect                                        | What a caller would have seen                                                      | Found by                                                                | What catches it now                                                                              |
| --------------------------------------------- | ---------------------------------------------------------------------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| D70: read past its snapshot                   | "not found" for a key that had a value                                             | a model extended (D70)                                                  | `noSnapshotUpperBound`; `db-client.test.ts`                                                      |
| D74b: counter refresh race                    | a row unflushed until a later write                                                | review of a diff                                                        | `db-writer.test.ts` (`runWithHookedStats`)                                                       |
| D76: begin manifest collected                 | every transaction read refused, every retry                                        | a guide checked against code                                            | `db-transaction.test.ts`; `ReadsAtBegin`                                                         |
| D77: fork hold released early                 | `StorageUnavailable` on every retry, fork snapshots and transactions               | interaction audit                                                       | `GcSafetyFork.cfg` `earlyForkRelease`; `database-lifecycle.test.ts`, `garbage-collector.test.ts` |
| D78: compactor fenced against itself          | every write refused, permanently                                                   | a measurement                                                           | `compactor.test.ts`, two overlapping cold wakes                                                  |
| D80: change feed skipped changes              | changes lost without an error                                                      | interaction audit                                                       | `change-feed.test.ts`, `manifest.test.ts`                                                        |
| D81: compaction folded expiries               | a key absent after a compaction                                                    | interaction audit                                                       | `row-resolution.test.ts`, `compaction-merge.test.ts`                                             |
| D83: collection without a retained manifest   | SSTs a reader needed deleted                                                       | interaction audit                                                       | `garbage-collector.test.ts`                                                                      |
| D84: `Snapshot` commit across a trim          | a lost update                                                                      | area audit, round 2 (Durable Objects)                                   | `db-writer.test.ts`; `Transactions.tla` `Commit`                                                 |
| D85: seal the writer never saw                | acknowledged writes that never flushed, in no successor                            | area audit, round 2 (Durable Objects)                                   | `db-writer.test.ts`                                                                              |
| D86: `prepare` result over 1 MiB              | a large merge never runs; runs pile up                                             | area audits, round 2 (Workflow, limits)                                 | `compaction-executor.test.ts`                                                                    |
| D87: `l0MaxSsts` below `minCompactionSources` | writes refused forever                                                             | area audit, round 2 (Workflow)                                          | `size-tiered-compaction-scheduler.test.ts`; a settings rule                                      |
| D88: TTL near 2^53                            | every flush failed, writes stalled                                                 | area audit, round 2 (API)                                               | `db-http-limits.test.ts`, `db-http-boundary.property.test.ts`                                    |
| D89: stale router at `Flushed`                | a sealed shard's old data, without bound                                           | area audit, round 2 (API)                                               | `shard-router-staleness.test.ts`                                                                 |
| D90: recreated id                             | a removed database's data under the new one                                        | area audit, round 2 (lifecycle)                                         | `database-lifecycle.test.ts`; `ManifestCasFencingRemove.cfg`                                     |
| D91: commit after a removal                   | a removed database back, acknowledging writes                                      | area audit, round 2 (lifecycle)                                         | `database-lifecycle.test.ts`; `ManifestCasFencingRemove.cfg`                                     |
| D93a: `Latest` scan held all unflushed rows   | `exceededMemory` on an allowed scan                                                | area audit, round 2 (limits)                                            | `db-client-scan.test.ts`                                                                         |
| D93b: change feed split a large batch         | 500 of 1,500 changes skipped silently                                              | area audit, round 2 (Durable Objects)                                   | `change-feed.test.ts`, `mem-table.test.ts`                                                       |
| D94: job with no Workflow instance            | its sources never compacted again                                                  | area audits, round 2 (Workflow, Durable Objects)                        | `compactor.test.ts`                                                                              |
| D95: seal with other successors               | success, with the old successors kept                                              | area audit, round 2 (API)                                               | `db-writer.test.ts`                                                                              |
| D96: compaction longer than `gcMinAge`        | a large merge never commits                                                        | area audit, round 2 (Workflow)                                          | `garbage-collector.test.ts`, `compactor.test.ts`                                                 |
| D97: step over dropped rows                   | compaction stuck after mass deletes; writes refused                                | area audit, round 2 (limits)                                            | `compaction-executor.test.ts`                                                                    |
| D101: abandoned split                         | a key range unwritable forever                                                     | area audit, round 2 (API)                                               | `shard-split-faults.test.ts`                                                                     |
| D107: metadata grew with key size             | gets re-reading huge metadata; at 16 KiB keys every flush failed, writes stalled   | area audit, round 2 (read)                                              | `sst-codec.test.ts`                                                                              |
| D107b: damaged SST metadata untyped           | a read died as `InternalFailure` (500), naming no SST                              | area audit, round 2 (read)                                              | `sst-codec.test.ts` property                                                                     |
| D108: scan page over deleted versions         | every request for that range timed out                                             | area audit, round 2 (read)                                              | `db-reader-limits.test.ts`, `db-http-scan.test.ts`                                               |
| D108b: damaged block cached                   | a block unreadable for as long as Workers Cache kept the bad copy (up to a year)   | area audit, round 2 (read)                                              | `block-cache.test.ts`                                                                            |
| D100c: body read twice                        | every `POST` to the API failed, in the Workers runtime only; the unit tests passed | integration in the real runtime (`writer.workerd.ts`)                   | `writer.workerd.ts` (`2ea228a`)                                                                  |
| create error span (`e147572`)                 | nothing; an operator saw an error span on every create                             | integration in the real runtime (`block-cache` scenario on `preview-2`) | `database-lifecycle.test.ts`                                                                     |
| D115a: seal interrupted before its edit       | every later request `WriterFencedError`, until the object was evicted              | TLA+ model at await granularity (`WriterObject`)                        | `db-writer.test.ts`; `beforeD115`                                                                |
| D115b: seal with nothing to flush             | the same, by a second path                                                         | TLA+ model at await granularity (`WriterObject`)                        | `db-writer.test.ts`; `markFencedAnyPhase`                                                        |
| D116a: `deleteCheckpoint` during `forkRange`  | a fork losing its data                                                             | TLA+ model (`Lifecycle`)                                                | `database-lifecycle.test.ts`; `beforeD116`                                                       |
| D116b: job launched before recorded           | a dropped SST's rows coming back                                                   | TLA+ model at await granularity (`CompactorObject`)                     | `compactor.test.ts` (`f83cd56`)                                                                  |
| D113: feed repeats after a lost response      | a change-feed consumer getting a change twice                                      | TLA+ model, while placing its labels                                    | `db-writer.test.ts`, an expected failure: **not fixed**                                          |
| D119a: old snapshot after an id's reuse       | the new database's value at an old snapshot                                        | a recorded risk worked through (`mut-read`, D90's list)                 | `database-lifecycle.test.ts`                                                                     |
| D119b: old compaction after an id's reuse     | an old job's outputs replacing the new chain's run                                 | a recorded risk worked through (`mut-read`)                             | `database-lifecycle.test.ts`                                                                     |
| D125a: transaction scan unbounded             | one row over 5,000 unflushed keys read 6 writer pages and held 5,000 rows          | the owner's review                                                      | `scan-bounds.test.ts`, `scan-oracle.property.test.ts`, `writer.workerd.ts`                       |
| D125b: sharded scan unbounded                 | a routed page read its whole shard (11 writer pages for one row)                   | the owner's review                                                      | `shard-scan-bounds.test.ts`, `shard-scan-oracle.property.test.ts`                                |
| D128: corrupted SST answer                    | `InternalFailure` (500), not the runbook's `StoredDataInvalid`                     | mutation sweep (`mut-edge`, reading a survivor's code)                  | `db-public-error-translation.test.ts` (`it.fails`); fix on `proto/audit-api`, not merged         |
| D129: checkpoint cap against GC               | at the most checkpoints allowed, no SST deleted again (derived)                    | a measurement in a test, with its arithmetic (`mut-read`)               | `manifest.test.ts`, `database-lifecycle.test.ts`                                                 |

Risks fixed alongside (not counted above): D98 (GC cost by commit rate), D99 (unbounded caller
growth), D100 (unbounded bodies), D102 (alarm retry), D103 (a retried zero-output commit), D118 (a
`BlockServer` error answer cacheable), D121 (a token secret never active), D123 (span events
outside the trace policy), and D125's one key overwritten 2,500 times. Recorded and not fixed: the
risk D109 found (section 9).

**By class** (the classes of `/tmp/shale-work/agents/AUDIT-COMMON.md`, plus three it does not
name):

| Class                                                              | Defects                                              |  Count |
| ------------------------------------------------------------------ | ---------------------------------------------------- | -----: |
| Two features each correct alone                                    | D70, D77, D80, D83, D87, D89, D91, D93b, D113, D116a |     10 |
| State shared across an await inside one Durable Object             | D74b, D78, D84, D85, D115a, D115b, D116b             |      7 |
| Liveness after long idle periods and after GC                      | D76, D90, D94, D101                                  |      4 |
| Time (TTL, `gcMinAge`, retention)                                  | D81, D88, D96, D119a, D119b                          |      5 |
| A limit a caller or the platform sets (not in AUDIT-COMMON's list) | D86, D93a, D97, D107, D108, D125a, D125b, D129       |      8 |
| Corruption not handled as corruption (not in AUDIT-COMMON's list)  | D107b, D108b, D128                                   |      3 |
| A property checked at only one moment                              | D95                                                  |      1 |
| Only the Workers runtime shows it (not in AUDIT-COMMON's list)     | D100c, the create error span                         |      2 |
| **Total**                                                          |                                                      | **40** |

**By how each was found:**

| Method                                                                             | Defects                                                       | Count |
| ---------------------------------------------------------------------------------- | ------------------------------------------------------------- | ----: |
| Area audits, round 2: API, Durable Objects, lifecycle, Workflow, limits, read path | D84–D97 (but D92), D93a, D93b, D101, D107, D107b, D108, D108b |    19 |
| Interaction audit, round 1                                                         | D77, D80, D81, D83                                            |     4 |
| TLA+ models at await granularity and end to end (D109–D116)                        | D113, D115a, D115b, D116a, D116b                              |     5 |
| The owner's review of `d89beba`                                                    | D125a, D125b                                                  |     2 |
| Integration in the real runtime                                                    | D100c, the create error span                                  |     2 |
| A recorded risk worked through                                                     | D119a, D119b                                                  |     2 |
| A measurement (`measure manifest-cas`; a 1,000-checkpoint test and its arithmetic) | D78, D129                                                     |     2 |
| A mutation sweep                                                                   | D128                                                          |     1 |
| A model extended (compaction added to `NoAckedWriteLost`)                          | D70                                                           |     1 |
| A guide checked against the code                                                   | D76                                                           |     1 |
| A review of a diff                                                                 | D74b                                                          |     1 |

The round-2 row is D84, D85, D86, D87, D88, D89, D90, D91, D93a, D93b, D94, D95, D96, D97, D101,
D107, D107b, D108, and D108b. The owner's review also asked for the snapshot and TTL statement
(the second review, above). It was a statement to write down: the code already did it, so it is
not a defect.

**The mutation sweeps.** Each mutant ran against the test files that cover its module, one vitest
worker, `nice -n 15` (D117's method). A survivor is equivalent, unobservable, a test gap, or a
defect.

| Sweep (decision)        | Scope                                                                                  | Mutants run |               Survived the first pass | Survive now                                                                          | Tests added                                                                                | Defects       |
| ----------------------- | -------------------------------------------------------------------------------------- | ----------- | ------------------------------------: | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ | ------------- |
| `mut-core` (D117, D118) | `lsm-core`, 15 modules                                                                 | 800 of 800  | 286 (270 against every covering file) | 82: 80 equivalent, 2 performance-only (the block seek)                               | known answers, limits, merge, manifest edits (`/tmp/shale-work/agents/mut-core/REPORT.md`) | 0             |
| `mut-read` (D119, D129) | reader, manifest, GC, lifecycle, block cache and source                                | 400 of 507  |                                   162 | 130, each classed in its report; 12 of `db-client.ts`'s untriaged                    | 6 survivors killed by new tests                                                            | 0 by a mutant |
| `mut-write` (D122)      | writer, memtable, compactor, compaction executor                                       | 318 of 580  |                                   121 | 2 test gaps closed, 6 named, 4 groups equivalent or unobservable, about 90 untriaged | the commit lock against a seal; the stored row kinds                                       | 0             |
| `mut-write2` (D127)     | the same files, and `object-store.ts`, `sst-store.ts`, the budget                      | running     |             `mem-table.ts`: 30 of 104 | PENDING(mutation)                                                                    | start, start failures, locks, trim bounds, range conflicts (`ac1603a` to `3bb39a4`)        | 0 so far      |
| `mut-edge` (D128)       | request bound, error translation, access gate, trace policy, shard directory and split | 165 of 165  |                                    37 | 6: 2 equivalent, 2 need workerd, 2 no contract                                       | 3 new test files, 3 extended                                                               | 1 (D128)      |

In all: 1,683 mutants run in the four sweeps that have reported, and `mut-write2`'s still running. One
defect came from a sweep, D128, and it was found by reading the code beside a survivor, not by
the mutant itself. Every other killed survivor was intended behavior that no test checked. The
largest classes of gap were stored formats with no known-answer test (the Bloom filter's bits, the
memtable's row kinds), `retryable` on 20 of 32 public error sites, and the locks (`commitLock`,
`initLock`, `flushLock`, `stateLock`, `planLock`). The lock gaps were closed with unit tests and,
for D116, model variants.

**Which methods have been applied where:**

| Package or area                                    | Area audit                          | Interaction audit         | Model                                                           | Mutation sweep                                                                     | Soak workload (D106)                       |
| -------------------------------------------------- | ----------------------------------- | ------------------------- | --------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------ |
| `lsm-core`                                         | read, limits                        | yes                       | `ManifestCasFencing`, `GcSafety`, `Transactions` (atomic)       | complete (800)                                                                     | yes                                        |
| Writer Durable Object and memtable                 | Durable Objects                     | yes                       | `WriterObject`, `NoAckedWriteLost`, `Transactions`, `SealSplit` | partial: `db-writer.ts` 98 of 221, `mem-table.ts` 115 of 138; `mut-write2` running | yes                                        |
| Compactor Durable Object and Workflow              | Workflow, Durable Objects, limits   | no (D78 by a measurement) | `CompactorObject`                                               | partial: `compactor.ts` 101 of 181, executor 4 of 37; `mut-write2` running         | yes (flush bursts)                         |
| Reader, manifest, GC, lifecycle                    | read, lifecycle                     | yes                       | `GcSafety`, `GcSafetyFork`, `Lifecycle`, `ManifestCasFencing`   | partial: 400 of 507 (`db-client.ts` 16 of 123; its round 2 running)                | yes                                        |
| `BlockServer` and the block cache                  | read (a hostile caller not reached) | no                        | none                                                            | block cache and source only                                                        | reads only                                 |
| Request edge (HTTP, errors, access gate, tracing)  | API                                 | no                        | none                                                            | complete (165), except `db-http-handlers.ts`                                       | partial: aborted requests, no hostile body |
| `lsm-sharding`                                     | API                                 | no                        | `ShardSplit`, `SealSplit`                                       | directory and split complete; `shard-router.ts` not swept                          | yes (splits, the feed across a split)      |
| `infra` (Stack, tokens, deploy)                    | production (read, never run)        | no                        | none                                                            | none                                                                               | deploy and redeploy only                   |
| Tools (`scenario-cli`, `trace-cli`, `preview-cli`) | none                                | no                        | none                                                            | none                                                                               | the soak is one of them                    |

**The rounds, as numbers.** Times come from each agent's `STATUS.md`. `formal`, `formal2`, and
`mut-write2` write local time (UTC − 5), converted here. Rates are approximate, because finding
and fixing overlap.

| Round                                                  | When (UTC, 2026-11-26/27) | Agent time finding                | Defects |          Per agent-hour |
| ------------------------------------------------------ | ------------------------- | --------------------------------- | ------: | ----------------------: |
| 1: review and interaction audit of the last day's diff | 22:40–00:04               | about 67 minutes                  |       6 |                 about 5 |
| The measurement (`measure manifest-cas`)               | 23:05                     | —                                 |       1 |                       — |
| 2: six area audits                                     | 00:12–01:30               | about 100 minutes in first briefs |      19 | about 9 in first briefs |
| 3: models at await granularity (two agents)            | 01:26–03:24               | about 217 minutes                 |       5 |               about 1.4 |
| 3: mutation agents (five), with their open lists       | 01:33–03:25               | about 270 minutes                 |       4 |               about 0.9 |
| 3: the owner's review                                  | before 02:23              | —                                 |       2 |                       — |
| 3: integration (workerd tests, a live scenario)        | 01:25–01:43               | —                                 |       2 |                       — |

Round 3's four from the mutation agents are D128 from a sweep, D119a and D119b from `mut-read`'s
open list, and D129 from its 1,000-checkpoint test. The production audit (00:59–01:30) and the
telemetry audit (D123, 01:34–01:53) found risks and documentation errors, no defect.

**What remains unexamined** (each report's "Not reached" that nobody has picked up):

- **Durable Objects:** `MemTable` migrations; the `LATEST`-hint writes after an eviction (the
  `WriterObject` model has no manifest view); D115's two view-install guards, which no test reaches.
- **The API:** the `BlockServer` under a hostile caller; HTTP round trips that reach a writer
  in-process; the sharded scan in workerd (D125).
- **Limits:** Worker startup time; what Tail Workers drop; any live run of a derived limit; a
  compaction step's memory with 8 inputs (needs workerd); live memory and scan-budget measurements.
- **Models:** two collections at once (D83); a second split of a successor before its rewrite;
  routers two generations behind; `dropSst`, corruption, and budget fairness in `CompactorObject`.
- **Mutation:** `db-client.ts` (its round 2 is running); about 90 untriaged survivors and the
  unrun mutants of `db-writer.ts`, `compactor.ts`, and `compaction-executor.ts` (`mut-write2` is
  running); `object-store.ts`, `sst-store.ts`, `compaction-budget*.ts`, `db-http-handlers.ts`, and
  `shard-router.ts`.
- **Production:** what `alchemy plan --stage production` prints (read, never run); a smoke step
  that hangs past 60 s.
- **The tools:** no area audit.

**What that says.**

- **The count.** 40 defects since `f3b2ad2`: 6 in round 1, 1 by a measurement, 19 in round 2,
  and 13 in round 3, plus D70 before round 1.
- **The gates.** The soaks of 132 and 21 minutes on `f3b2ad2` passed with 31 of them in the code.
  The 21.6-minute soak on `d89beba`, with the D106 workload, passed with 11 in the code: D113,
  D115a, D115b, D116a, D116b, D119a, D119b, D125a, D125b, D128, and D129. Their fixes are on the
  integration branch (`git merge-base --is-ancestor`), and D113's is not written.
- **Each method's first pass found defects:** the interaction audit, the area audits, the object
  models, the owner's review, and the runtime integration. The mutation sweeps found one in about
  1,700 mutants. What they mostly found were gaps in the tests, now closed.
- **The rate fell in round 3.** By agent-hour: about 5 in round 1, about 9 in round 2's first
  briefs, and about 1.4 and 0.9 in round 3.
- **Round 3's defects were of the same classes as before,** except the two only the runtime shows,
  and they were found by new methods.
- **What a fix closes.** Each fix has a regression test, which closes that defect, not its class.
  D113 and D128 are not fixed at `c149afb`.

These are the facts as they stand. They make no prediction.

## 9. Known limitations

What v1 does not do, or has not measured.

**Throughput and latency.**

- **One writer per database.** It was measured at ~6,500 rows/s with 100-row batches and ~15,000
  rows/s with 1,000-row batches (D61; measured at `308defd`, path changed). A workload needing more must use more databases, for example
  through the range sharding in `packages/lsm-sharding`.
- **The batch body is base64 JSON.** A binary body was declined (D61). It would only be added later,
  as an additive content type.
- **Manifest commits cost ~285 ms beside the bucket**, and up to ~1 s from the far side of the world
  (R2's own latency, measured at `774ed7b`).
  The bucket's location is fixed when it is created (`SHALEDB_BUCKET_LOCATION`). The default, `auto`,
  places it near the machine that deploys, so choose it before the first production deploy if
  callers are not near that machine.
- **Writer start-ups dominate a get's p99.** A fence-in takes ~0.5 s (D72; measured at `9776317`,
  path changed).
- **No Workers Cache for blocks in a restricted Stack.** A restricted (`eu`, `fedramp`) Stack has
  none, and it does not hedge block reads (D72, corrected). Reads from far away measured slower.
- **Hedging can be started by queueing alone.** Its 50 ms timer starts when a read is issued, not
  when it gets one of an invocation's six connections. On a scan wide enough to queue, a hedge can
  start only because its read waited, and the hedge then waits too. A cap on hedges in flight is
  not adopted (D72).
- **CPU varies between runs with the machine a Durable Object lands on.** The write CPU of the
  same 128 batches ranged from 9.6 to 28 s between runs (`docs/measurements.md`, D73's section;
  its commit is not recorded there).
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
- **Every scan page, on all three surfaces, is bounded** (D108, D125; the two new rows of
  `docs/api-compatibility.md`, "Limits of v1"). This covers the regular, the transaction, and the
  sharded scan:
  - **Rows:** at most 1,000.
  - **Work:** at most 16 MiB of versions examined (`SCAN_PAGE_EXAMINED_BYTES`).
  - **Memory:** at most one writer page (8 MiB and 1,000 rows), 16 MiB of returned rows, one key's
    versions, and the reader's blocks in flight. This is derived from the constants, not measured
    live.
  - **A short page is not the end.** A page over deleted or expired rows may return fewer rows, or
    none, with a `resumeFrom` to follow. A null `resumeFrom` is the end.
- **A long run of one key's merge operands is read whole** (D125). A page that starts such a key
  finishes it, so one page can read up to `maxUnflushedBytes` (256 MiB) of one hot counter's
  unflushed operands. D99 bounds only the merged result.
- **A routed scan is not one snapshot across shards** (D125). A transaction's scan pages all read
  at its `readSeq`. Its commit's `readRanges` must name the whole range scanned, not each page's
  part. Naming only what a page returned lets a phantom past an early page commit. The in-Worker
  `DbClient.scanAt` keeps its signature and has no budget.
- **The sharded scan's HTTP response is not checked in workerd** (D125). Its router is tested
  in-process.
- **The change feed can return a change twice** (D113, not fixed at `c149afb`). This happens after
  a flush commit whose response was lost: the next flush exports the same rows again. A consumer
  that applies changes by sequence number, ignoring any at or below the last one applied, is not
  affected. L0 is not affected.
- **A read of a corrupted SST answers `InternalFailure`** (500, not retryable), not the
  `StoredDataInvalid` the corruption runbook names (D128). The fix is on `proto/audit-api`
  (`edfe57b`), not merged at `c149afb`. Until it is, the runbook's signal for such a read is
  `InternalFailure` with the request id.
- **A write evicted after its SQLite commit and before its alarm is set** (D109, a risk, not fixed).
  The row is not acknowledged, but it is durable, and `Latest` reads see it. It has no alarm until
  the next request to that writer, so a database that gets no further writer request never
  flushes it. Whether the platform can evict there depends on the output gate.
- **A stale flush view after a restart** (D115). Two guards on a flush's view install are reached by
  no test. Their only effect is a stale view, which the next commit reloads, and a stale `l0Count`
  in `status`.
- **A reused id starts its numbering at 0 again once the sweep has deleted its tombstone** (D119).
  That is `gcMinAge` after the removal, plus up to one sweep cycle. An old snapshot or compaction
  job of the removed database would have to outlive that to meet the new chain.
- **A collection reads every manifest a checkpoint pins, whole** (D119, D129). With 128 pins of
  4 MiB manifests that is 512 MiB per collection (derived; about 5 s of decoding in Node). D98's
  5,000-load budget counts young manifests by number, not bytes: up to 20 GiB at 4 MiB manifests
  (derived), left to D98's owner.
- **Workers Logs are not under the trace policy** (D123). The runtime's own console output gets each
  log line's full message. No call site puts a key there.
- **Alarms back off.** A writer's or compactor's alarm that keeps failing retries from 1 s, doubling
  to a ceiling (`maxL0Age`, 60 s, for the writer), and never stops (D102). An outage of R2 therefore
  delays flushes by up to a minute after it ends.
- **Not reached by any audit, model, or sweep:** the list at the end of section 8.

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

None has been run live at the derived size. The largest compaction run live is about 0.5 GiB
(D103); everything above it in D103's table is derived.

Settings the platform cannot hold (R2 parts, the Durable Object's storage, the isolate's memory,
the account's Workflows) are now refused when a deploy is planned.

**Bounds a caller meets (D99).**

- **Checkpoints and pinned snapshots:** at most 128 per database (D129). The next one is refused with
  `CheckpointLimitReached` (409).
- **The changelog:** its newest 2,048 flushes and at most 30 days of retention. A consumer behind
  them gets `Expired`.
- **Merged values:** at most 1 MiB. Past that, a read fails with `MergedValueTooLarge` (409) until
  the key is rewritten with a put.
- **Names:** a checkpoint name is at most 256 characters and a budget group at most 63.
- **Database size: about 312 GiB.** A bottom merge of a larger database runs without 2× headroom
  under Workflows' 10,000 steps per instance; above about 625 GiB it fails at its last step on every
  attempt (D103). The manifest's 4 MiB budget is a second bound: about 1 TiB for 16-byte keys, 90 GiB
  for 1 KiB keys (D99). Larger databases are sharded. Raising the Workflow's step limit to 25,000
  (Cloudflare's maximum) would move the first bound to about 1.5 TiB; it is not set (release
  `leaks/STATUS.md`).
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
- **The newest changes have no long soak yet.** D76–D108 have a 21.6-minute soak (on `d89beba`),
  and the 130-minute one is running. D109–D129 have no soak at all (section 5).
- **One formal run is not yet observed at `c149afb`.** `Transactions.cfg` with the code as
  written timed out at 420 s under load with 2 TLC workers (D111, `docs/progress.md`).
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
   If a written secret has not turned `active` after 20 reads, 500 ms apart, the command still
   shows the tokens. It then exits non-zero with `SecretNotActive`, naming the secrets (D121). Do
   not write the tokens again: wait, then deploy.

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

   Found while writing this, and rechecked on 2026-11-27 by reading the allowlist and the guard:

   - **The current allowlist has no `Shaledb` entry.** Its only entry for this repository is the
     pre-rename `.../cf-slatedb/infra/cf-slatedb/alchemy.run.ts` (stack `CfSlatedb`). That path no
     longer exists, so the guard skips it (`policy.ts`, `parseCloudflareDeploymentPolicy`, which
     records it in `ignoredEntries`). Run the
     script once, whatever you decide about production.
   - **So today the guard treats this repository as not allowlisted,** and it resolves package
     tasks here (`evaluatePackageTask` in `evaluate.ts`). It evaluates the `alchemy destroy` inside
     `vp run @shaledb/infra#destroy:preview-1` against the policy, as it would a typed command,
     instead of exempting it. The two points below hold once the
     script has run.
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
| An alarm backing off (D102)                | `alarm.failures` on `DbWriter.alarm` and `Compactor.alarm` spans, and the warning each failed run logs                                                                                            | —                                                                                                |

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

**Where a rollback can go.** Production starts at the release commit, so a production rollback
goes to a commit at or after it (D105). Older commits matter only for the previews, which run
them today.

**Stored formats from `d89beba` to `c149afb`.** No stored record's schema changed:
`git diff d89beba..c149afb` over `manifest.ts`, `compactor.ts`, `shard-map.ts`, and `mem-table.ts`
adds no field and changes no format constant. D129 changes a cap, D119 changes where a new
chain's numbering starts, and D116 changes when a job is recorded, all with existing fields. The
fixtures hold every stored field, and their tests pass at `c149afb`
(`manifest-format-fixtures.test.ts`, 6 tests; `stored-format-fixtures.test.ts`, 3 tests; run for
this pass). D105's table of fields older code drops is therefore unchanged.

**What a rollback below the release commit would bring back** (previews only):

- D115, D116, and D119's defects, which are fixed only on the integration branch.
- The transaction and sharded scans without a work budget (D125). Their responses have no
  `resumeFrom`. A caller that follows `resumeFrom` would stop after one page, even when the page
  held `limit` rows.
- Below D91, the tombstone behavior D105 describes. Avoid removing databases while rolled back
  across D91 (`docs/operations.md`).

**Not shown here, because it needs workerd or the older code itself:**

- What `f3b2ad2` and `c3807f7` do with a tombstone was read from their code, not run (D105).
- The request-body bound's two paths only the Workers runtime takes (D100's correction, D128's
  two survivors).
- The sharded scan's HTTP response (D125).
- A compaction step's memory with 8 inputs.

Nothing removes production. Its LSM bucket is retained (`RemovalPolicy.retain` in
`packages/lsm-cloudflare/src/lsm-bucket.ts`) and is not `forceDestroy`.
