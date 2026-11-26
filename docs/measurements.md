# Measurements

Live measurements for design §12 ("Measure before trusting these numbers"), plus the live checks of
hand-off item 4. Every number here came from the **`preview-1`** stage on Dillon's account
(`99459fce09d9a9eec83befd957af4ec5`) on 2026-11-25 with the **production write-path settings**
(`DEFAULT_LSM_SETTINGS`: `l0SstSize` 64 MiB, `maxL0Age` 60 s, `maxUnflushedBytes` 256 MiB, 4 KiB
blocks). One correction (D54): an earlier `--dev-timings` deploy's `gcMinAge` (10 s) and
`compactorPollInterval` (1 s) stayed bound on the Workers during these runs. Neither is on the
writer's path (writes, flushes, the seal and forks); the faster compactor polling and GC could only
have added background load. Deploys now bind every setting explicitly, and since D55 every Worker is
bound the complete settings on every deploy; `preview-cli bindings <stage>` shows them, and every
later section records what it showed. The client was a laptop on a US residential connection calling the Workers'
`workers.dev` URLs, so client-side latencies include an internet round trip.

Reproduce with:

```sh
node tools/preview-cli/src/preview-cli.ts deploy preview-1            # production settings
node tools/scenario-cli/src/scenario-cli.ts measure writes --stage preview-1 --batch-rows 500 --concurrency 8
node tools/scenario-cli/src/scenario-cli.ts measure flush-cpu --stage preview-1 --megabytes 48
node tools/scenario-cli/src/scenario-cli.ts measure split-pause --stage preview-1 --writers 4
```

## How CPU time is measured

A Worker cannot measure its own CPU time: its clock advances only across I/O, which is why many
spans show `0.00ms`. Live stages therefore attach a Tail Worker (`CpuTailWorker`,
`infra/cf-slatedb/src/cpu-tail-worker.ts`, D48) to the API Worker (which hosts the writer Durable
Objects) and the compactor Worker. Cloudflare hands it every finished invocation with `cpuTime` and
`wallTime`; it stores one record per invocation (script, Durable Object id, event kind, method, URL
path, status, outcome, times) under `tails/` in the stage's trace bucket, and the measurement
commands read them back through the trace collector (`GET /v1/tails/batches`). Byte counts come
from spans (`sst.size` on `DbWriter.flush`).

## Rows per second per writer, batched writes

One database, one writer. `concurrency` loops each send `batch-rows` puts per request (16-byte keys,
100-byte values) for 30 s. No request was refused (`WriteStalled` never fired) and nothing flushed
during the runs (each stayed under 64 MiB and 60 s).

| Batch rows | In flight | Rows/s acknowledged | Requests/s | MiB/s | Client latency p50 / p90 / p99 (ms) | Writer CPU per batch p50 / p90 (ms) | Writer CPU-seconds per 30 s |
| ---------: | --------: | ------------------: | ---------: | ----: | ----------------------------------- | ----------------------------------- | --------------------------: |
|        100 |         8 |               2,669 |       26.7 |  0.31 | 263 / 450 / 872                     | 22 / 36                             |                        19.2 |
|        500 |         8 |               8,374 |       16.7 |  0.96 | 434 / 720 / 1,934                   | 39 / 70                             |                        21.4 |
|      1,000 |        16 |              12,322 |       12.3 |  1.41 | 1,228 / 1,665 / 2,814               | mean 67                             |                        26.2 |

Large values (from the flush runs: 2,000 rows × 1,000-byte values ≈ 2 MB per batch, 4 in flight):
writer CPU per batch p50 107 ms, p90 298 ms.

**Reading (before D61; see "Writer throughput and limits" below: the fixed cost was a per-write
memtable scan, now gone).** The writer is **CPU-bound**: it spent 19–26 s of CPU in each 30 s run, so these rates
are close to one writer's ceiling. A batch costs roughly a fixed ~15–20 ms (HTTP and schema
decoding of the base64 JSON body, the SQLite transaction) plus ~0.05–0.07 ms per 100-byte row. One
writer sustains **about 12–15k small rows/s with 1,000-row batches** (~1.4 MiB/s), and about
19 MiB/s of CPU budget for 1 KB values. Design §12's "~1k req/s" is not the binding limit: at these
batch sizes a writer saturates its CPU at 12–27 requests/s. Smaller batches waste most of the CPU on
per-request overhead. Design §11's open question about base64 JSON applies: an
`application/octet-stream` batch body would remove part of the fixed cost.

## Flush CPU per MiB

`flush-cpu` writes a database to the given size, calls `POST /flush`, and attributes the writer's
`/flush` invocation (and any alarm of that Durable Object) to the L0 SST the flush wrote.

| Written | L0 SST written | Flush CPU | Flush wall | CPU per MiB |
| ------: | -------------: | --------: | ---------: | ----------: |
|   4 MiB |       5.96 MiB |    315 ms |     1.90 s |     52.8 ms |
|  16 MiB |      17.90 MiB |  1,594 ms |     4.85 s |     89.1 ms |
|  48 MiB |      51.70 MiB |  2,888 ms |     7.38 s |     55.9 ms |

**Reading.** About **55–90 ms of CPU per MiB** (63 ms/MiB overall), so a full 64 MiB L0 flush costs
about 3.5–6 s of CPU, well inside the paid plan's per-invocation CPU limit (30 s by default) but
far beyond the Free plan's 10 ms. Wall time is about 2.5× the CPU (R2 multipart uploads and the
manifest CAS). One alarm flush of ~58 MiB that ran under load took 22 s of wall time. Design §12's
"8–16 MB `l0SstSize` when hot" keeps a flush under about 1.5 s of CPU, so writes queue behind it
for less time.

## Write pause during a seal-then-fork split

`split-pause` runs 4 writers through the shard router (one 100-byte row per request, keys spread
over `a`–`z`), splits the shard at `m` after 10 s, and keeps writing for 10 s after the split
returns. Before the split, puts took p50 65–77 ms and p99 0.7–1.9 s at the client.

| Run | Router retry window | Split (client) | `DbWriter.seal` | Longest gap between acks | Puts during the split p50 / max | Failed puts |
| --- | ------------------- | -------------: | --------------: | -----------------------: | ------------------------------- | ----------: |
| 1   | 5 s                 |         13.4 s |           2.7 s |                   14.8 s | 93 ms / 9.7 s                   |     4 / 800 |
| 2   | 5 s                 |         12.8 s |           3.2 s |                   14.6 s | 4.7 s / 10.1 s                  |     4 / 823 |
| 3   | 30 s (fixed)        |         14.3 s |           3.1 s |                   16.0 s | 74 ms / 16.3 s                  |     0 / 762 |

**Reading.** Writes to the splitting shard pause for **about 13–16 s**. Design §12 described the
pause as "one memtable flush plus a few manifest CAS operations", and that is the structure (one
flush, seven manifest CAS operations, two new databases, two compactor wake-ups), but each manifest
CAS costs **about 1.1 s** on live R2 (`ManifestStore.update` p50 1,076 ms), which no local test
could show:

| R2 operation (from spans, this run)         |      p50 |      p90 |
| ------------------------------------------- | -------: | -------: |
| `ObjectStore.putIfAbsent`                   |   386 ms |   625 ms |
| `ObjectStore.put` (incl. `manifest/LATEST`) |   435 ms |   577 ms |
| `ObjectStore.get`                           |   108 ms |   309 ms |
| `ObjectStore.getVersioned`                  |    72 ms |   110 ms |
| `ObjectStore.getRange`                      |    70 ms |   119 ms |
| `ManifestStore.update` (one CAS)            | 1,076 ms | 1,333 ms |

A CAS is `loadLatest` (~300 ms: the `LATEST` hint, the manifest, a probe of `id + 1`), the
create-if-absent PUT (~400 ms), and the best-effort `LATEST` hint (a read, ~70 ms, and a PUT,
~420 ms) — the hint alone is ~40% of every CAS. In the split, each fork also waits ~2 s for its new
compactor to fence in (`CompactorClient.wake`).

**Finding (fixed).** The router refreshed and retried for 5 s, sized for a local split. On the live
stage every write that arrived early in the 13 s split exhausted that window and failed with
`DatabaseSealed` (retryable). The window is now 30 s (`shard-router.ts`, D40); run 3 shows no failed
writes. **Not changed (recorded for later):** writing the `LATEST` hint outside the CAS's critical
path and waking the forks' compactors without waiting would shorten every CAS by ~40% and the split
by ~4 s; both touch code covered by the manifest CAS model and are left for a change that re-runs
it.

## How often a flush aborts a transaction

`measure transactions` runs 4 loops of `SerializableSnapshot` transactions over the HTTP API (begin,
read two keys, commit one put; disjoint keys, so they never conflict) while 4 loops write 100-row
batches to the same database, for 150 s, production settings.

| Transactions | Committed | `SnapshotTooOld` | Transaction p50 / max | Flushes during the run  |
| -----------: | --------: | ---------------: | --------------------- | ----------------------- |
|          580 |       580 |                0 | 1.0 s / 2.9 s         | 1 (19 MiB, 28.7 s wall) |

A transaction aborts only if it outlasts a whole flush (D50); live flushes take at least ~1.5 s.

## Manifest commits and small flushes (v1 workstream 2, D56)

`measure manifest-cas` runs 30 small flushes in sequence on a new database (10 rows of 100 bytes,
then `POST /flush`, repeated) and reads the server spans back from the stage's collector.
`preview-1`, production settings (`preview-cli bindings preview-1`: `production` on every Worker),
2026-11-25. "Before" is the code at `012bdb5`; "after" is D56. Values are p50 / p90 / p99 in ms.

```sh
node tools/scenario-cli/src/scenario-cli.ts measure manifest-cas --stage preview-1 --flushes 30
```

| Span                                                  | Before                | After               |
| ----------------------------------------------------- | --------------------- | ------------------- |
| `ManifestStore.update` (every commit)                 | 1,092 / 1,269 / 1,470 | 357 / 998 / 1,095   |
| … from the writer's own view (flushes)                | –                     | 340 / 539 / 651     |
| … from a loaded manifest (fences, compaction commits) | –                     | 995 / 1,095 / 1,095 |
| `ManifestReader.loadLatest`                           | 274 / 383 / 478       | 180 / 253 / 299     |
| `DbWriter.flush` (small memtable)                     | 1,508 / 1,683 / 3,678 | 709 / 917 / 1,801   |
| `POST /flush` at the client                           | 1,584 / 1,724 / 3,754 | 796 / 1,012 / 1,844 |

Commits: 40 before, 39 after
(30 flush commits plus the writer's and compactor's fences and compaction commits). **Targets met:**
`ManifestStore.update` p50 357 ms (≤ 500 ms) and a small flush p50
709 ms (≤ 900 ms). A commit from the writer's own view is one
create-if-absent (~285 ms here) and the boundary re-read (~76 ms); a loaded commit adds
`loadLatest` (~180 ms, down from ~274 ms: the hint and boundary read together, two probes in
parallel) and loses races to concurrent flushes more often.

## R2 latency by region (v1 workstream 2, D56)

`measure r2-placement` creates a probe Durable Object with each location hint and times the
engine's object store calls (128-byte objects) against `preview-1`'s LSM bucket, which Cloudflare
reports in **ENAM**. 20 samples per region; p50 / p90 in ms.

```sh
node tools/scenario-cli/src/scenario-cli.ts measure r2-placement --stage preview-1 --samples 20
```

| Hint | Data center | create-if-absent | read      | write         | versioned read |
| ---- | ----------- | ---------------- | --------- | ------------- | -------------- |
| wnam | SJC         | 511 / 559        | 229 / 275 | 558 / 667     | 230 / 268      |
| enam | IAD         | 284 / 325        | 86 / 122  | 352 / 379     | 76 / 98        |
| sam  | IAD         | 285 / 333        | 76 / 114  | 354 / 400     | 85 / 115       |
| weur | CDG         | 560 / 785        | 265 / 297 | 630 / 742     | 262 / 308      |
| eeur | MXP         | 586 / 896        | 290 / 331 | 658 / 972     | 281 / 308      |
| apac | HKG         | 984 / 1,575      | 550 / 607 | 1,029 / 1,088 | 558 / 606      |
| oc   | SYD         | 911 / 967        | 501 / 541 | 979 / 1,023   | 499 / 558      |
| afr  | CDG         | 548 / 596        | 278 / 295 | 604 / 666     | 259 / 294      |
| me   | FRA         | 535 / 628        | 272 / 317 | 601 / 694     | 268 / 301      |

**Reading.** A write to R2 costs ~280–350 ms even from the data center beside the bucket (IAD for
ENAM; `sam` landed in IAD too), and a read ~80 ms. Each region farther away adds roughly its round
trip: ~510 ms from the US west coast, ~550–590 ms from Europe, ~900–1,000 ms from Asia-Pacific and
Oceania. `preview-1`'s writers run near its bucket (an own-view commit is one ~285 ms create plus a
~76 ms read), so placement could not speed it up. An installation whose writers sit elsewhere should
create its bucket there: `CF_SLATEDB_BUCKET_LOCATION` (fixed at creation; unrestricted Stacks only).

## Split write pause after D57 (v1 workstream 3)

`measure split-pause` as above, on `preview-1` with production settings, after D56 (fast commits)
and D57 (seal, holds, and final flush in one commit; forks from their holds, concurrently and
warmed in the background; routers waiting on the directory). Three runs per writer count.

| Writers | Run | Split (client) | `DbWriter.seal` | Longest gap between acks | Longest put | Failed puts |
| ------: | --- | -------------: | --------------: | -----------------------: | ----------: | ----------: |
|       4 | a   |          2.0 s |           1.0 s |                    2.7 s |       3.5 s |     0 / 800 |
|       4 | b   |          1.8 s |           0.8 s |                    3.2 s |       3.3 s |     0 / 875 |
|       4 | c   |          1.8 s |           0.8 s |                    2.6 s |       3.0 s |     0 / 873 |
|      16 | a   |          1.9 s |           1.0 s |                    3.1 s |       3.4 s |    0 / 2578 |
|      16 | b   |          1.8 s |           1.3 s |                    2.6 s |       3.3 s |    0 / 3182 |
|      16 | c   |          2.0 s |           1.6 s |                    3.4 s |       3.7 s |    0 / 1951 |

**Reading.** Writes to a splitting shard now pause about **2.6–3.4 s** (median 2.7 s with 4
writers, 3.1 s with 16), down from 13–16 s, with no failed writes. The split itself takes ~2 s: one
upload and one manifest commit under the seal's lock (~0.8–1.6 s), two concurrent first-manifest
creates, and the publish. The rest of the gap is writes queued behind the seal's commit lock and
each router's retry once the directory answers. Between the D56-only runs (~8.6 s) and these, two
intermediate variants were measured and discarded: concurrent forks each taking their own hold
(their CAS raced) and a separate checkpoint (an extra commit). In one run R2 was slower overall
(create-if-absent ~0.8–1.1 s instead of ~0.3 s), which stretched every commit; the design now has
as few commits on the pause as it can.

## Transactions held open across flushes (v1 workstream 4, D58)

`measure transactions` with each transaction held open between its first and second read, while
plain writes load the database and a flusher forces a flush every 3 s. `preview-1`, production
settings (`transactionRetentionMs` 60 s, `maxRetainedBytes` 64 MiB).

```sh
node tools/scenario-cli/src/scenario-cli.ts measure transactions --stage preview-1 --transactors 4 --writers 4 --seconds 120 --hold-seconds 20 --flush-every-seconds 3
```

| Transactions | Committed | `SnapshotTooOld` | Transaction p50 / max | Forced flushes |
| -----------: | --------: | ---------------: | --------------------- | -------------: |
|           24 |        24 |                0 | 21.7 s / 22.9 s       |             23 |

**Reading.** Every transaction spanned 6–7 flushes and committed. Before D58 each would have
aborted at its first flush, since it outlasted a whole flush (D50). A transaction now aborts as too
old only if it outlives the writer's retention (60 s by default) or the retained-bytes bound.

## Garbage collection across 500 databases (v1 workstream 5, D60)

`measure gc-scale` creates 500 databases and flushes each four times (its writer wakes its
compactor, which compacts and leaves four L0 SSTs as garbage). It waits four minutes, runs the
maintenance sweep page by page until it wraps, then reads spans and tail records. `preview-1` with
`--dev-timings` (`gcMinAge` 10 s, so collections happen within the run).

```sh
node tools/preview-cli/src/preview-cli.ts deploy preview-1 --dev-timings
node tools/scenario-cli/src/scenario-cli.ts measure gc-scale --stage preview-1 --databases 500 --settle-seconds 240
```

| Databases | Collected by their compactor | Collection runs | Sweep pages (from the stored position to the wrap) | Invocations |   `ok` | `canceled` | `exceeded*` |
| --------: | ---------------------------: | --------------: | -------------------------------------------------- | ----------: | -----: | ---------: | ----------: |
|       500 |                          500 |            1500 | [50, 41]                                           |      15,535 | 15,517 |         18 |           0 |

GC Worker (sweep) CPU p50 / max: 163 / 202 ms; compactor
invocation CPU p50 / p99 / max: 16 / 111 / 177 ms.

**Reading.** Every database's garbage was collected by its own compactor from its alarm, three
collections each (the compaction's garbage, then the manifest that collection superseded, then a
check that converges). The sweep visits at most 50 databases per run, so a run's cost does not grow
with the number of databases. With 10,000 databases a full sweep cycle takes 200 runs, and it is
only a backstop. The 18 `canceled` outcomes were Durable Object alarms and Workflow RPC calls ended
by the runtime (at most 213 ms wall, 11 ms CPU), not limits.

## Writer throughput and limits (v1 workstream 6, D61)

`preview-1`, production settings; `measure writes` with 16-byte keys and 100-byte values for 30 s.
Writer and API CPU come from the tail records.

| Batch rows | In flight | Rows/s before | Rows/s after | Latency p50 / p90 / p99 after (ms) | Writer CPU per batch p50 / p90 (ms) | API CPU per batch p50 / p90 (ms) | Writer busy |
| ---------: | --------: | ------------: | -----------: | ---------------------------------- | ----------------------------------- | -------------------------------- | ----------: |
|        100 |         8 |         2,454 |        6,547 | 105 / 136 / 427                    | 6 / 12                              | 10 / 18                          |         48% |
|      1,000 |        16 |         7,678 |       15,257 | 961 / 1,706 / 2,400                | 12 / 176                            | 23 / 41                          |         47% |

`tools/writer-bench` (node:sqlite standing in for Durable Object SQLite; shares, not absolute
numbers):

| Batch rows | Body bytes | JSON decode (µs) | Collapse (µs) | Write path (µs) | Decode share |
| ---------: | ---------: | ---------------: | ------------: | --------------: | -----------: |
|        100 |     21,216 |              241 |           249 |           1,707 |        14.1% |
|      1,000 |    212,016 |            1,693 |         1,223 |          11,814 |        14.3% |

**Reading.** The per-write memtable scan was the writer's fixed cost. Without it, a writer is idle
half the time at these loads, and decoding is about a seventh of what remains, so the binary body is
not adopted (D61).

**Flush and compaction at production sizes** (`measure compaction-cpu --megabytes-per-flush 60`:
four 60 MiB flushes, then the compaction of the four L0 SSTs):

| Run                               | Flush CPU p50 / max (ms) | Flush wall p50 / max (s) |   Compaction step CPU (ms) | Compacted                       | Outcomes            |
| --------------------------------- | -----------------------: | -----------------------: | -------------------------: | ------------------------------- | ------------------- |
| D58 flush queries                 |                        — |                        — |                          — | no                              | flush `exceededCpu` |
| flush fixed, one read per block   |              649 / 2,990 |                        — |                          — | no (`Too many API requests`)    | flush `ok`          |
| reads in 2 MiB chunks             |              771 / 3,471 |              11.0 / 11.9 | 10,735 p50, 17,668 max (5) | yes (4 steps, one commit)       | all `ok`            |
| same, after the CPU limits (60 s) |            1,150 / 4,345 |              12.7 / 14.4 |                      9,046 | yes, 112 s after the last flush | all `ok`            |

**One writer at `maxUnflushedBytes`** (`measure writer-memory --megabytes 250` on `preview-3`
deployed with `--settings '{"l0SstSize":268435456,"maxL0Age":3600000}'`): 266,954,000 bytes held
unflushed, then one flush of all of it. 174 writer invocations, all `ok`; 5 writes refused with
`WriteStalled` at the cap. Flush CPU p50 1,482 ms, max 9,842 ms; the client's flush call took 82 s.
Write CPU per 2,000 × 1 KB batch p50 77 ms, max 617 ms.

**Compactions started at once** (`measure workflow-burst --databases 200`, `compactionBudgetCapacity`
1,000): the fourth flush on 200 databases in 3.0 s; all 200 compacted within 16.4 s of the burst
(at most 38 seen running at one poll); 1,848 compactor invocations, all `ok`.

**Limits set (P5):** API Worker and its writers 30 s (2 × 9.8 s), compactor 60 s (2 × 17.7 s).

## Upload integrity cost (v1 workstream 7, D62)

`preview-1`, production settings; `measure compaction-cpu --megabytes-per-flush 60` (four 60 MiB
flushes, then their compaction), after each change to how part MD5s are computed:

| Part digest                          | Flush CPU p50 / max (ms) | Flush wall p50 / p90 (s) | Compaction step CPU (ms) | Multipart parts verified   |
| ------------------------------------ | -----------------------: | -----------------------: | -----------------------: | -------------------------- |
| none (D61)                           |            1,150 / 4,345 |              12.7 / 14.4 |                    9,046 | —                          |
| JavaScript MD5 (`md5Hex`) in workerd |           3,742 / 16,732 |              13.1 / 29.9 |                   11,052 | 8 uploads, all             |
| WebCrypto MD5 (native)               |              992 / 3,881 |              14.2 / 16.5 |                    9,988 | 16 uploads, 134 parts, all |

**Reading.** The native digest costs nothing measurable. The JavaScript MD5, about 425 MB/s in
Node, was several times slower inside workerd. It stays only as the fallback where WebCrypto has no
MD5 (Node, in tests). Every part R2 stored matched the MD5 sent (`object.parts_unverified` 0, no
re-uploads).

## Live checks

| Check                                       | Result                         | How                                                                                                                                                                                                                                                                                                                                                                                             |
| ------------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Workers Cache in front of `BlockServer`     | **`Cf-Cache-Status: HIT`**     | `scenario-cli run --stage preview-1 block-cache`: the API Worker reads a flushed block (`cache.l2.status=MISS` on its `BlockSource` spans), then the shard router (another Worker, its own L1) reads the same block URL: `HIT` on both its `BlockSource.meta` and `BlockSource.block`. The second read goes through a second Worker because the first Worker's isolate would answer it from L1. |
| R2 create-if-absent with the `Headers` form | **`AlreadyExists` on real R2** | `scenario-cli run --stage preview-1 create-race`: 16 concurrent creates of one database; exactly one `ObjectStore.putIfAbsent` is `Created` and 15 are `AlreadyExists`, each a public `DatabaseAlreadyExists` (workerd #2572 does not affect the `Headers` form).                                                                                                                               |
| Every scenario with trace assertions        | **11 / 11 pass**               | `scenario-cli run --stage preview-1 all` against a `--dev-timings` deploy; spans pulled from the stage's collector before each assertion.                                                                                                                                                                                                                                                       |

## Not measured

- Whether Durable Object SQLite's page cache counts against the 128 MB isolate limit, as such: no
  API reports a Durable Object's memory. What matters was measured instead: a writer holding
  `maxUnflushedBytes` (256 MiB) unflushed, then flushing it, never reported `exceededMemory` (D61).
- The Workflows concurrent-instance limit itself: 200 compactions started at once all ran (D61);
  production's compaction budget (32) stays far below that.
