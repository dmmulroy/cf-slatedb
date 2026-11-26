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

On your go-ahead, the following happens in order (section 9). You create the production tokens.
`production` is added to the deployment guard, or you run the tasks yourself. `plan:production` is
run and its plan shown to you. `deploy:production` runs. `scenario-cli smoke` runs against the new
URL and removes its own databases. The result is recorded in `docs/progress.md`. If you do not give
the go-ahead, nothing changes: production stays one command away.

## 2. Workstreams 1 to 10

Commits are on `main`. Status comes from the workstream table in `docs/progress.md`.

| #   | Workstream                            | Done                          | Commit(s)                                                                                                                    | Decisions                        |
| --- | ------------------------------------- | ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| 1   | The Stack owns its configuration      | yes                           | `012bdb5`                                                                                                                    | D55 (supersedes D54's mechanism) |
| 2   | Fast manifest commits                 | yes                           | `774ed7b`                                                                                                                    | D56                              |
| 3   | Splits pause briefly, never fail      | yes                           | `393758c`                                                                                                                    | D57                              |
| 4   | Transactions exact across flushes     | yes                           | `5f2850b`, `5df4099` (the D59 routing fix)                                                                                   | D58, D59                         |
| 5   | GC and maintenance that scale         | yes                           | `e06f3eb`                                                                                                                    | D60                              |
| 6   | Writer throughput and limits          | yes                           | `308defd`                                                                                                                    | D61                              |
| 7   | Corruption: prevent, detect, repair   | yes                           | `2cb5116`                                                                                                                    | D62                              |
| 8   | Production telemetry                  | yes                           | `62eab3d`                                                                                                                    | D63                              |
| 9   | Production readiness                  | yes                           | `8a96667`                                                                                                                    | D64                              |
| 10  | Soak, release checklist, the go-ahead | no: soaks pending, P6 pending | `a506228`, `fe552f0`, `18aa655`, `a98eb15`, `93c337c`, `04df0f9`, `9776317`, `5bedcbd`, `c72e0f3`, `c9e7487`, this checklist | D65–D74                          |

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

The numbers for workstreams 2, 3, and 6 were measured before D70–D74 changed the read, flush, and
writer paths. They are re-measured on the final code:

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
`tools/formal-verification/src/formal-checks.ts`: **28 TLC runs** over five specs (the code as
written must pass; each broken variant must violate the named invariant). Bounds are the `.cfg`
files in `tools/formal-verification/specs/`. Each check has a skill under
`.agents/skills/verify-*` that records its assumptions and sync commit.

All models assume one writer instance per database (the Durable Object guarantee), durable SQLite
commits before an acknowledgement, and R2 create-if-absent (confirmed on real R2 by the
`create-race` scenario).

| Spec and config              | What it proves                                                                                                                                                                                                                  | Bounds                                                                      | Broken variants (must fail)                                                                                                                                               |   Runs |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -----: |
| `ManifestCasFencing.cfg`     | Fencing and manifest CAS linearizability: one manifest per id, a commit lands once, no fenced writer or compactor commits, acknowledged data stays visible, the reader finds the head from the hint and from any known manifest | 2 writers, 2 compactors, 1 job each, manifest ids up to 9, 2 flushes, no GC | `noWriterEpochCheck` → `NoStaleCommit`; `blindCreate` → `CreateOnce`                                                                                                      |      3 |
| `ManifestCasFencingRace.cfg` | The same, with a compaction whose inputs changed before it committed                                                                                                                                                            | 1 writer, 1 compactor, 2 jobs, ids up to 8, 2 flushes                       | `noInputsCheck` → `AckedDataVisible`                                                                                                                                      |      2 |
| `ManifestCasFencingGc.cfg`   | The same while GC deletes manifests at or below an advancing boundary and hint writes land late or never                                                                                                                        | 1 writer, 1 compactor, 1 job, ids up to 7, 2 flushes, GC on                 | `noBoundaryReread` → `CommitOnce`; `hintOnlyProbe` → `LoadFindsHead`; `knownIgnoresBoundary` → `LoadFromKnownFindsHead`                                                   |      4 |
| `NoAckedWriteLost.cfg`       | No acknowledged write is lost across eviction, crash between upload and commit, flush, trim, and compaction; a `Latest` read sees every acknowledged write                                                                      | 4 writes, 2 crashes, 2 reads                                                | `truncateBeforeCommit` → `NoAckedWriteLost`; `nonAtomicProbe` → `LatestSeesAcked`; `trimUnflushed` → `NoAckedWriteLost`; `noSnapshotUpperBound` → `LatestSeesAcked` (D70) |      5 |
| `GcSafety.cfg`               | GC never deletes an SST the newest manifest, a manifest a reader may still hold, or a checkpoint references, nor an upload whose commit can still succeed; SST drops included                                                   | 2 SSTs (symmetric), clock 0–3, min age 1, 5 manifests                       | `noBoundaryCheck`, `ownUploadAge`, `dropDeletesObject` → `ReadableManifestsComplete`; `ignorePins` → `PinnedManifestsComplete`                                            |      5 |
| `SealSplit.cfg`              | After a seal no write commits; a split leaves every write acknowledged before the seal in exactly one successor                                                                                                                 | 3 keys (1 to the left successor), 4 writes                                  | `sealWithoutLock`, `checkpointBeforeSeal`, `overlappingRanges` → `EveryAckedWriteInExactlyOneSuccessor`; `writerNotFenced` → `NoCommitAfterSeal`                          |      5 |
| `Transactions.cfg`           | `SerializableSnapshot` transactions admit only serializable histories, with retained rows, trims, and pinned begin manifests                                                                                                    | 2 keys, 2 transactions (symmetric), sequence numbers up to 5                | `snapshotOnly`, `trimWithoutAbort`, `noReadHorizonCheck` → `Serializable`                                                                                                 |      4 |
| **Total**                    |                                                                                                                                                                                                                                 |                                                                             |                                                                                                                                                                           | **28** |

The models are bounded: they check every interleaving within these bounds, not beyond them.
Code-level properties (codecs, merge, compaction merge, GC plans, the client and transactions
against oracles, fault injection) are property tests with mutation checks
(`docs/coverage-matrix.md`).

Last observed: `vp run verify` exit 0, 28/28 (`docs/progress.md`, "10, continued: experiments run
in parallel").

## 5. Soaks

`scenario-cli soak` (D65) is the release gate. Its pass criteria: no mismatch and no failed
operation, against a model of every acknowledged write at `Latest` and `Flushed`; `measure health`
reports no issue over the soak's window (no `exceededCpu`, `exceededMemory`, or `exception`; no
current corruption; no GC failure; no stalled write); no unexpected error span; storage bounded
after GC; p50 and p99 client latencies recorded.

The 2-hour soak on the D67 code ran its workload but did not report, and its code was superseded
(`docs/progress.md`). 5-minute soaks passed on `preview-2` and `preview-3` for each arm of the D70–D74
experiments. Those are not the release soaks.

PENDING(soak-preview-1): the 130-minute soak on the final code on `preview-1` (production settings,
`--trace-sample-rate 0.1`, two splits, a redeploy midway): the exact command, start and end time,
the commit, pass or fail on each criterion above, operation counts, p50/p99 per operation, the
`measure health` summary, storage after GC, and any error span outside the expected list.

PENDING(soak-preview-2): the 20-minute soak on the final code on `preview-2` (`eu`): the same
fields as above.

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

## 7. What changed late (D67 to D74)

These landed after the first soaks. The release soaks (section 5) are the first long runs on them.

- **D67.** A `Latest` read reuses any cached manifest that holds the writer's flushed rows, for up
  to half of `gcMinAge`. Reads open their SSTs together. Scans read ahead 8 blocks per SST. Blocks
  default to 16 KiB.
- **D68.** The project was renamed from cf-slatedb to shaledb: packages, paths, the Stack (`Shaledb`),
  variables, bindings, live names. The old `CfSlatedb/preview-<n>` stacks and their secrets remain
  (`docs/final-handoff-draft.md`).
- **D69.** A request the endpoint's schemas refuse is `InvalidRequest` (400), not 500.
- **D70, a correctness fix.** A `Latest` read could use a manifest that was flushed and compacted
  past the snapshot the writer gave it, and return "not found" for a key that always had a value.
  It was present since Phase 5. It was found by adding compaction to `NoAckedWriteLost.tla`, never
  seen live. A read now refuses such a manifest and asks the writer again (up to three times, then
  retryable `StorageUnavailable`). A transaction scan without a begin manifest had a related gap,
  also fixed. In the same decision, a flush writes only the versions a read can reach.
- **D71.** Tail records are sampled like traces, except those that did not end `ok`.
- **D72.** A manifest refresh takes one round trip from the manifest held. A block read slower than
  50 ms is hedged with a read from the bucket. Scans stop reading when they have their rows.
- **D73.** A flush no longer waits for the compactor it wakes. SST blocks are encoded a chunk of
  rows at a time. Concurrent part uploads were measured and declined.
- **D74, a writer fix.** The writer had two figures for unflushed bytes. In the gap between them
  the alarm fired, scanned the memtable, did not flush, and set itself again. A 267 MB flush took
  200 s instead of 20 s, and two such writers at once were reset (no acknowledged write lost). Every
  transaction begin also paid a full memtable scan through `status` (1.58 s of CPU on a full
  memtable). There is now one formula and three counters, and neither the alarm nor `status` scans.

## 8. Known limitations

What v1 does not do, or has not measured.

**Throughput and latency.**

- One writer per database. It was measured at ~6,500 rows/s with 100-row batches and ~15,000 rows/s
  with 1,000-row batches (D61). A workload needing more must use more databases, for example the
  range sharding in `packages/lsm-sharding`.
- The batch body is base64 JSON. A binary body was declined (D61) and would only be added later as
  an additive content type.
- Manifest commits cost ~285 ms beside the bucket and up to ~1 s from the far side of the world. The
  bucket's location is fixed when it is created (`SHALEDB_BUCKET_LOCATION`, default `auto`: near
  the machine that deploys). Choose it before the first production deploy if callers are not near
  that machine.
- A get's p99 is dominated by writers starting: a fence-in is ~0.5 s (D72).
- A restricted (`eu`, `fedramp`) Stack has no Workers Cache for blocks. Reads from far away measured
  slower and hedging does not help there (D72).
- CPU per invocation varies up to 2× with the machine a Durable Object lands on (D73).
- Every measurement came from one client on a US residential connection, against preview stages.

**Semantics.**

- `Flushed` and `AtLeast` reads are eventually consistent, as designed. A routed `Flushed` read of a
  sealed shard returns its state at the seal (D59).
- A transaction aborts with `SnapshotTooOld` if it outlives the writer's retention
  (`transactionRetentionMs`, 60 s) or the `maxRetainedBytes` bound (64 MiB). Callers that do not
  send `manifestId` keep the pre-D58 behavior: a flush past their read sequence aborts them (D58).
- Plain snapshots expire after `gcMinAge` (1 hour). Longer ones must be pinned (D52).
- Dropping a corrupted SST loses that SST's versions of its keys; older versions show again (D62).
- The data-residency jurisdiction is fixed per installation. Changing it means a new installation
  and moving data by forks or copies (D49).
- Within `/v1`, changes may only be additive, and limits may only be relaxed
  (`docs/api-compatibility.md`). Today's limits and error semantics are therefore permanent for
  `/v1`.
- A rollback across a release that raises a format version stops on the newer data with
  `StoredDataInvalid` (D64). v1 is format 1.

**Not measured or not shown.**

- Whether Durable Object SQLite's page cache counts against 128 MB, as such. Only the outcome was
  measured: no `exceededMemory` at 256 MiB unflushed (`docs/measurements.md`, "Not measured").
- The Workflows concurrent-instance limit itself. 200 at once ran; production's compaction budget
  is 32.
- GC at scale was measured with dev timings (`gcMinAge` 10 s), not production's 1 hour (D60).
- The D70 race was never reproduced live. The fix rests on the model, on `db-client.test.ts`
  (a writer overtaken after it answers), and on mutation checks.
- The read, flush, and writer changes D67–D74 have not yet run a long soak (section 5).

**Operations.**

- `measure health` and `trace-cli pull --stage` accept only `preview-1`, `preview-2`, and
  `preview-3` (`Argument.Literals("stage", PREVIEW_STAGES)` in `tools/preview-cli/src/preview-cli.ts`,
  which `scenario-cli` resolves stages through). Production has a trace collector and tail records,
  but no tool in the repository reads them yet. Section 10 depends on this.
- The only production hostname is `workers.dev`. The OpenAPI spec lists only the local server URL.
- Secrets Store is in open beta, with one store per account (Cloudflare's
  [Wrangler Secrets Store page](https://developers.cloudflare.com/workers/wrangler/commands/secrets-store/)).
  The tokens depend on the account's `default_secrets_store`.
- The deployment guard's allowlist does not currently name this project. See section 9, step 2.

## 9. Production commands

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
   its allowlist. You edit it yourself; agents cannot. `scripts/allowlist-shaledb-deploys.sh` adds
   `Shaledb` at `infra/shaledb/alchemy.run.ts` for `dev`, `preview-1`, `preview-2`, and `preview-3`.
   It does not add `production`. To add it, change the script's
   `STAGES='["dev","preview-1","preview-2","preview-3"]'` to include `"production"` and run it. Then
   restart the agent's pi session or confirm the change when it asks. Three things found while
   writing this:

   - **The current allowlist has no `Shaledb` entry.** Its only entry for this repository is the
     pre-rename `.../cf-slatedb/infra/cf-slatedb/alchemy.run.ts` (stack `CfSlatedb`). That path no
     longer exists, so the guard skips it (`policy.ts`, `parseCloudflareDeploymentPolicy`). Run the
     script once whatever you decide.
   - **The stage list does not keep agents off production.** Once any project in a repository is
     allowlisted, the guard exempts the whole repository (`isAllowlistedRepositoryWorkingDirectory`
     in `policy.ts`; `destructiveOnly` in `evaluate.ts`). Only `alchemy destroy` and
     `wrangler delete` stay blocked there. So after the script runs, an agent in this repository can
     run `deploy:production` whether or not `production` is listed. The real gate is your
     instruction, and `deploy:production` refusing to run without the token secrets.
   - **`alchemy destroy` stays blocked for agents** whatever the allowlist says.

   You can skip this step by running steps 3 and 4 yourself.

3. **Review the plan.**

   ```sh
   vp run @shaledb/infra#plan:production
   ```

   This runs `alchemy plan --stage production`. Expect new resources only: the `shaledb` LSM bucket
   (retained), the trace bucket, the API, `BlockServer`, compactor, GC, shard-router,
   trace-collector, and CPU Tail Workers, the Durable Object namespaces, and the compaction
   Workflow. There should be no probe Worker. Settings come from the environment
   (`SHALEDB_LSM_SETTINGS`, `SHALEDB_JURISDICTION`, `SHALEDB_BUCKET_LOCATION`,
   `SHALEDB_TRACE_SAMPLE_RATE`, `SHALEDB_TRACE_RETENTION_DAYS`, `SHALEDB_API_CPU_MS`,
   `SHALEDB_COMPACTOR_CPU_MS`). Unset is the production default. Make sure none is set in your
   shell unless you mean it.

4. **Deploy.**

   ```sh
   vp run @shaledb/infra#deploy:production
   ```

   This runs `node src/access-tokens-cli.ts check production && alchemy deploy --stage production
   --yes`. It refuses unless both token secrets exist. The API URL is printed as `apiUrl`.

5. **Smoke test.** `scenario-cli smoke` (D65) runs only operations safe on a live installation, on
   databases named `smoke-<time>-…`, and removes them at the end whatever happened. It checks
   create, write, reads at `Latest`, `AtLeast`, and `Flushed`, scan, a transaction and a refused
   conflicting one, a checkpoint created, listed, and deleted, and eight concurrent creates with
   one winner.

   ```sh
   SHALEDB_TOKEN='<production token A>' \
     node tools/scenario-cli/src/scenario-cli.ts smoke --api-url '<apiUrl from step 4>'
   ```

   Keep the token out of shell history: read it into `SHALEDB_TOKEN` from your secret manager rather
   than typing it.

## 10. The first day

From `docs/operations.md` ("What to watch").

| Signal                       | Where                                                                                            | Act when                                                                   |
| ---------------------------- | ------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------- |
| Invocations that hit a limit | Tail records: outcome `exceededCpu`, `exceededMemory`, `exception`                               | Any. Limits are 30 s (API, writers) and 60 s (compactor)                   |
| Writes stalled               | `DbWriter.write` errors with `WriteStalledError`; public `WriteStalled` (503)                    | Sustained: L0 not draining (backlog or corruption), or a writer overloaded |
| Flush duration               | `DbWriter.flush` span duration                                                                   | p99 above about 30 s, or rising                                            |
| Compaction backlog           | `manifest.l0_ssts` on `Compactor.poll`                                                           | Near `l0MaxSsts` (16), where writes stall                                  |
| Corrupted SSTs               | `lsm_compaction_corrupt_ssts`; `compaction.corrupt_ssts` on `Compactor.poll`; `GET …/compaction` | Any: follow the corruption runbook in `docs/operations.md`                 |
| Upload integrity             | `lsm_object_part_integrity_retries`, `lsm_object_parts_unverified`                               | Frequent retries, or any unverified part on R2                             |
| Garbage collection           | `GarbageCollector.collect` and `MaintenanceSweep.sweepPage` errors                               | Repeated failures                                                          |
| Error spans                  | `trace-cli grep --error`                                                                         | A new name in the top errors                                               |

These errors are expected and are not trouble: `DatabaseSealedError` (a stale router retrying),
`TransactionConflictError`, and `DatabaseAlreadyExists` on create.

`docs/operations.md` names `scenario-cli measure health --stage <stage> --minutes 60` as the
summary. It reads only preview stages today (section 8). Until a tool reads production's collector,
the signals above can be seen only in production's trace bucket (`traces/`, `tails/`) or in
Cloudflare's own dashboard for the Workers. Production keeps 10% of `ok` traces and tail records,
and every tail record that did not end `ok` (D71).

## 11. Rollback

From `docs/operations.md` ("Rolling back"). Redeploy the previous commit with the same task
(`vp run @shaledb/infra#deploy:production` from a checkout of that commit). A redeploy leaves
Durable Objects (writers, compactors, the shard directory) and everything in R2 untouched. Writes
acknowledged before it are in each writer's SQLite or in R2, and the new code reads both.

The manifest and the compactor state carry a format version (D64). Code refuses data in a newer
format rather than rewrite it. A rollback across a release that raised a format therefore stops on
those databases with `StoredDataInvalid`, naming the version to deploy, and damages nothing. Roll
forward again to recover. v1 is format 1, the format of all data written so far, so rolling back
from v1 to an earlier v1 commit crosses no format change.

Nothing removes production. Its LSM bucket is retained (`RemovalPolicy.retain` in
`packages/lsm-cloudflare/src/lsm-bucket.ts`) and is not `forceDestroy`.
