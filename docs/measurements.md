# Measurements

Live measurements for design §12 ("Measure before trusting these numbers"), plus the live checks of
hand-off item 4. Every number here came from the **`preview-1`** stage on Dillon's account
(`99459fce09d9a9eec83befd957af4ec5`) on 2026-11-25, deployed with the **production settings**
(`DEFAULT_LSM_SETTINGS`: `l0SstSize` 64 MiB, `maxL0Age` 60 s, `maxUnflushedBytes` 256 MiB) unless a
row says otherwise. The client was a laptop on a US residential connection calling the Workers'
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

**Reading.** The writer is **CPU-bound**: it spent 19–26 s of CPU in each 30 s run, so these rates
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

## Live checks

| Check                                       | Result                         | How                                                                                                                                                                                                                                                                                                                                                                                             |
| ------------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Workers Cache in front of `BlockServer`     | **`Cf-Cache-Status: HIT`**     | `scenario-cli run --stage preview-1 block-cache`: the API Worker reads a flushed block (`cache.l2.status=MISS` on its `BlockSource` spans), then the shard router (another Worker, its own L1) reads the same block URL: `HIT` on both its `BlockSource.meta` and `BlockSource.block`. The second read goes through a second Worker because the first Worker's isolate would answer it from L1. |
| R2 create-if-absent with the `Headers` form | **`AlreadyExists` on real R2** | `scenario-cli run --stage preview-1 create-race`: 16 concurrent creates of one database; exactly one `ObjectStore.putIfAbsent` is `Created` and 15 are `AlreadyExists`, each a public `DatabaseAlreadyExists` (workerd #2572 does not affect the `Headers` form).                                                                                                                               |
| Every scenario with trace assertions        | **11 / 11 pass**               | `scenario-cli run --stage preview-1 all` against a `--dev-timings` deploy; spans pulled from the stage's collector before each assertion.                                                                                                                                                                                                                                                       |

## Not measured

- Whether Durable Object SQLite's page cache counts against the 128 MB isolate limit: no load here
  came near it (the largest memtable was ~60 MB of rows), and no API reports a Durable Object's
  memory.
- The Workflows concurrent-instance limit on the plan: the measurements ran one compaction at a
  time (`maxConcurrentCompactions: 1`).
