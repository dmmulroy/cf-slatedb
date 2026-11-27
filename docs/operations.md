# Operations

How to run shaledb in production: deploying, rolling back, access tokens, data residency,
garbage collection and removal, capacity, telemetry, what to watch, and the corruption runbook.

Every endpoint below is on the API Worker under `/v1` and needs the stage's token
(`Authorization: Bearer <token>`).

## Deploying

Production is the `production` stage of the `Shaledb` Stack. It gets:

- the LSM bucket named `shaledb`, retained if the Stack is destroyed;
- 10% trace sampling;
- no probe Worker;
- the measured CPU limits: 30 s for the API Worker and its writers, 60 s for the compactor.

Every Worker is bound its complete settings on every deploy (D55).

1. **Once, before the first deploy:** create the two access tokens and store them in your secret
   manager. They are printed only here.

   ```sh
   vp run @shaledb/infra#tokens ensure production
   ```

2. **Review the plan:**

   ```sh
   vp run @shaledb/infra#plan:production
   ```

3. **Deploy:**

   ```sh
   vp run @shaledb/infra#deploy:production
   ```

   It first checks that both token secrets exist, and refuses otherwise.

Settings come from the deploy's environment. All are optional; defaults are in
`infra/shaledb/src/shaledb-stack-config.ts`:

| Variable                                         | Default (production)   | What it sets                                                                                                                                                                                                                                                                                 |
| ------------------------------------------------ | ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SHALEDB_LSM_SETTINGS`                           | none (engine defaults) | JSON overrides of engine settings, for example `{"blockReadHedgeAfterMs":0}` to turn off hedged block reads (D72; a Stack restricted to a jurisdiction never hedges, since its `BlockServer` has no Workers Cache, so the setting has no effect there). Invalid combinations refuse the plan |
| `SHALEDB_JURISDICTION`                           | `none`                 | `eu` or `fedramp` restricts every bucket, Durable Object, and the Workflow. Fixed per installation                                                                                                                                                                                           |
| `SHALEDB_BUCKET_LOCATION`                        | `auto`                 | R2 location hint for the LSM bucket (unrestricted Stacks). Fixed per installation                                                                                                                                                                                                            |
| `SHALEDB_TRACE_SAMPLE_RATE`                      | `0.1`                  | Fraction of traces kept                                                                                                                                                                                                                                                                      |
| `SHALEDB_TRACE_RETENTION_DAYS`                   | `14`                   | Days traces and tail records are kept                                                                                                                                                                                                                                                        |
| `SHALEDB_API_CPU_MS`, `SHALEDB_COMPACTOR_CPU_MS` | `30000`, `60000`       | CPU limit per invocation                                                                                                                                                                                                                                                                     |

The API's URL is the API Worker's `workers.dev` URL, printed as `apiUrl` when the deploy finishes.

## Rolling back

Redeploy the previous commit with the same task. Durable Objects (writers, compactors, the shard
directory) and everything in R2 are untouched by a redeploy:

- writes acknowledged before it are in each writer's SQLite or in R2;
- the new code reads both.

The manifest and the compactor state carry a format version (D64). Code refuses data written in a
newer format rather than rewrite it. A rollback across a release that raised a format therefore
stops on those databases with `StoredDataInvalid`, naming the version to deploy, and damages
nothing. Roll forward again to recover. v1 is format 1, the format of all data written so far.

## Access tokens

The API, GC, and shard-router Workers accept a request that presents either of the stage's two
tokens as `Authorization: Bearer <token>`. Each token is a Secrets Store secret
(`SHALEDB_<STAGE>_ACCESS_TOKEN_A` and `_B` in the account's `default_secrets_store`). Workers
read the secrets at runtime and re-read them every 30 s, so a rotation needs no deploy.

To replace a token without downtime:

1. Move every caller to the slot you keep, say B.
2. Rotate the slot you replace:

   ```sh
   vp run @shaledb/infra#tokens rotate production A
   ```

   It prints the new token. Within 30 s, Workers accept it and stop accepting the old one.

3. Next time, rotate B after moving callers to A.

Preview stages also accept the token `preview-cli token <stage>` prints. Production has no such
token. Cloudflare Access may be put in front of the Workers too; the gate stays.

## Data residency

The jurisdiction is chosen once per installation (P2: none). With `eu` or `fedramp`, every
bucket, Durable Object, and the compaction Workflow is created in that jurisdiction, and Workers
Cache is not used for blocks. Changing it later creates new, empty resources: move to it by
creating a new installation and forking or copying databases into it.

## Garbage collection and removal

- **Collection.** Each database's compactor collects its own garbage from its alarm, `gcMinAge` (1
  hour) after a compaction. That garbage is replaced SSTs and manifests no reader may still hold
  (D60).
- **The sweep.** A cron every 5 minutes runs a paged maintenance sweep of 50 databases. It removes
  what deleted databases left behind, wakes forks' compactors, and asks for collection where
  manifests piled up or where a fork has rewritten its source's SSTs but still holds it.
- **Collect now:** `POST /v1/gc/databases/<id>` on the GC Worker.
- **Removing a database:** `DELETE /v1/databases/<id>`. It fails with `DatabaseHasDependents`
  while forks still read its SSTs. Remove them first, or wait until each fork has rewritten what it
  read and `gcMinAge` (1 hour) has passed since. The fork's own collection releases its hold then,
  once no snapshot or cached manifest of the fork can still read the source (D77). After a split,
  the sealed database can be removed about an hour after both halves' rewrites.
- **A fork of a fork** (or a split half split again before its rewrite) holds the middle database,
  and the middle database holds the original one. After the inner fork releases its hold, the
  middle database releases the original at its next collection. Only the sweep asks for that
  collection, so it can take one full sweep cycle plus `gcMinAge`. A cycle is `ceil(P / 50)` runs
  5 minutes apart, for `P` databases: 100 minutes at 1,000 databases, 16 h 40 min at 10,000
  (D77). To release sooner, collect the middle database now (`POST /v1/gc/databases/<id>`).

## Capacity

Measured on preview stages with production settings ([`measurements.md`](measurements.md)):

| What                            | Measured                                                           |
| ------------------------------- | ------------------------------------------------------------------ |
| One writer, 1,000-row batches   | ~15,000 rows/s; p50 latency ~1 s                                   |
| One writer, 100-row batches     | ~6,500 rows/s; p50 latency ~105 ms                                 |
| Unflushed data per writer       | 256 MiB (`maxUnflushedBytes`); then `WriteStalled` until a flush   |
| L0 SSTs before writes stall     | 16 (`l0MaxSsts`)                                                   |
| Manifest commit (one CAS)       | p50 ~357 ms beside the bucket; ~550 ms from Europe; ~1 s from APAC |
| Small flush                     | p50 ~709 ms                                                        |
| 60 MiB flush                    | ~4 s CPU per flush (D73), ~14 s wall                               |
| Compaction step (64 MiB output) | ~10 s CPU (max ~18 s)                                              |
| Split (seal and fork)           | writes pause ~3 s                                                  |
| Transactions held open          | at least 60 s (`transactionRetentionMs`) across flushes            |
| Concurrent compactions          | 200 at once completed in 16 s                                      |
| GC sweep                        | 50 databases per run, ~160 ms CPU                                  |

Throughput per database is one writer's. A workload that needs more uses more databases, for
example with range sharding.

## Platform limits

The Cloudflare limits an operator must know, from developers.cloudflare.com as read on 2026-11-26
(the full table, with sources and margins, is D92). "Measured" means on a preview with production
settings, "derived" computed from the code and the settings, "docs" from the documentation only.

| Limit                                                            | What it means for shaledb                                                                                                                                                                              | Act                                                                                                                      | Kind     |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ | -------- |
| CPU per invocation (30 s default, up to 5 min)                   | Set to 30 s for the API Worker and writers and 60 s for the compactor. Largest seen: 9.8 s (a 256 MiB flush) and 17.7 s (a compaction step)                                                            | `exceededCpu` in `measure health` (What to watch); raise `SHALEDB_API_CPU_MS` / `SHALEDB_COMPACTOR_CPU_MS` up to 300,000 | measured |
| A compaction step whose rows are all dropped                     | One step reads every input of its job (D92, finding 2): a job over a few hundred MiB of deleted or expired data can exceed the compactor's CPU on every retry, and L0 then fills                       | Watch the L0 backlog after mass deletes or TTL expiry; raising the compactor's CPU limit buys room until the fix         | derived  |
| Memory per isolate (128 MB)                                      | A `Latest` scan holds every unflushed row of its range, and pages are bounded by rows (1,000), not bytes, with values up to 1 MiB (D92, finding 3)                                                     | Use small scan limits over large values; `exceededMemory` in `measure health`                                            | derived  |
| Subrequests per invocation (10,000) and alarm wall time (15 min) | One garbage collection reads every manifest superseded within the hour, one after another (D92, finding 4): about 5,000 reads and 350 s at 1.4 commits a second                                        | Avoid flushing or pinning snapshots several times a second on one database; GC failures in `measure health`              | derived  |
| Workflows step result (1 MiB)                                    | A compaction's first step carries every input SST's handle: about 220 bytes each with short keys, 44 KB with 16 KiB keys (D92, finding 1)                                                              | Prefer short keys; a database whose L0 stops draining with jobs failing at `prepare` has hit it                          | derived  |
| Durable Object storage (10 GB per object)                        | A writer holds at most `maxRetainedBytes` + 2 × `maxUnflushedBytes` + one batch: 584 MiB by default. Settings that exceed 10 GB are refused                                                            | None with the defaults                                                                                                   | derived  |
| R2: 10,000 parts, 5 GiB less 5 MiB per part                      | 32 parts for the largest flush; settings that exceed either are refused                                                                                                                                | None with the defaults                                                                                                   | derived  |
| R2: one write per second to one key                              | The `LATEST` hint is written on every commit; above one commit a second some hint writes get 429 and the hint lags, which readers tolerate (D56)                                                       | None                                                                                                                     | derived  |
| Workflows: 50,000 concurrent instances per account               | The compaction budget (32) is refused above it                                                                                                                                                         | None with the defaults                                                                                                   | docs     |
| `waitUntil` (30 s after the response)                            | Background hint writes, fork warming, and compactor wakes: a cold wake took 5 to 9 s                                                                                                                   | None                                                                                                                     | measured |
| Secrets Store (100 secrets per account, open beta)               | Two per live stage; delete orphaned ones (`docs/final-handoff-draft.md`)                                                                                                                               | Keep the account under 100                                                                                               | docs     |
| `workers.dev`                                                    | Cloudflare treats it as a Free website, "intended for personal or hobby projects that aren't business-critical", and recommends a route or custom domain for production; production uses it today (P3) | Put production behind a custom domain when the account has a zone                                                        | docs     |
| Tail Workers                                                     | What is dropped under load is not documented                                                                                                                                                           | Treat tail-record counts as a lower bound                                                                                | docs     |

Costs that grow without use: the maintenance sweep runs every 5 minutes on every live stage; an
idle database keeps its newest flush's rows (up to 64 MiB) in its writer's SQLite; checkpoints
without a lifetime and long changelog retention keep SSTs and grow the manifest; traces and tail
records are kept 14 days.

## Telemetry

Every Worker and Durable Object exports spans over OTLP to the stage's own trace collector, which
stores them in the stage's trace bucket (D63):

- **Sampling.** Production keeps 10% of traces (`SHALEDB_TRACE_SAMPLE_RATE`, P4). Local and
  preview stages keep every trace. The decision comes from each trace's id, so a kept trace is
  whole across the API Worker, its Durable Objects, and the BlockServer. A caller's `traceparent`
  joins our spans to its trace, but its sampled flag does not force them to be kept.
- **Redaction.** Spans carry only shaledb's own attributes (`database.id`, `sst.id`,
  `manifest.id`, sizes, counts, outcomes) and the HTTP method, route, path, and status. A caller's
  address, user agent, full URL, query string, and headers are dropped where they are set.
- **Retention.** The trace bucket deletes `traces/` and `tails/` objects after 14 days
  (`SHALEDB_TRACE_RETENTION_DAYS`).
- **Tail records.** The API, compactor, and GC Workers' invocations (their outcome, CPU time, and
  wall time) are stored under `tails/`. Every invocation that did not end `ok` is kept; of the
  rest, the same fraction as traces (D71). Each record carries the rate it was kept at, and
  `measure health` scales its counts back up.

The telemetry probe Worker, a test tool, is not deployed to production.

### Reading a stage's telemetry

The read-only tools name a stage's collector in one of two ways:

- **Production, or any live stage:** `--collector-url <url>`, with the collector's token in
  `SHALEDB_TRACE_TOKEN`. The URL is the `traceCollectorUrl` output the deploy prints.
- **A preview:** `--stage <preview-n>`. The preview CLI reads the URL and token from state.

The collector token is separate from the access tokens. It reads spans and tail records only,
not databases. It is held in the stage's Alchemy state, and `preview-cli trace-token <stage>`
reads it (any live stage, `production` included). That command writes the token only into a pipe
or a command substitution. It refuses a terminal, so the token never shows on screen. Put the token
in the environment this way, so it is not typed and not in shell history:

```sh
export SHALEDB_TRACE_TOKEN="$(node tools/preview-cli/src/preview-cli.ts trace-token production)"
export SHALEDB_TRACE_COLLECTOR_URL="https://…workers.dev"  # traceCollectorUrl, from the deploy
```

`SHALEDB_TRACE_COLLECTOR_URL` is only a shell variable for the commands below; the tools read the
URL from `--collector-url`.

In fish: `set -gx SHALEDB_TRACE_TOKEN (node tools/preview-cli/src/preview-cli.ts trace-token production)`.
`trace-token` reads state with the Alchemy profile's credentials, like `preview-cli status`. The
tools fail before any request when `SHALEDB_TRACE_TOKEN` is missing or empty, or when a command
names both `--stage` and `--collector-url`, or neither.

To bring a stage's spans into the local trace store, where `list`, `show`, `grep`, `stats`, and
`assert` read them:

```sh
# production
node tools/trace-cli/src/trace-cli.ts pull --collector-url "$SHALEDB_TRACE_COLLECTOR_URL" --since-minutes 60
node tools/trace-cli/src/trace-cli.ts tail --collector-url "$SHALEDB_TRACE_COLLECTOR_URL"
# a preview
node tools/trace-cli/src/trace-cli.ts pull --stage preview-1 --since-minutes 60
node tools/trace-cli/src/trace-cli.ts tail --stage preview-1
```

A pull resumes where the store's last pull from the same collector URL stopped, whichever form
named it, so nothing is stored twice. Without `--since-minutes` a fresh store pulls everything the
collector holds (up to 14 days). To keep production's spans apart from a preview's, pass the same
`--store <dir>` to `pull` and to the commands that read them.

## What to watch

`measure health` summarizes a stage from its tail records and spans. It reads only the stage's
trace collector, so it needs no access token:

```sh
# production
node tools/scenario-cli/src/scenario-cli.ts measure health --collector-url "$SHALEDB_TRACE_COLLECTOR_URL" --minutes 60
# a preview
node tools/scenario-cli/src/scenario-cli.ts measure health --stage preview-1 --minutes 60
```

It prints `healthy: false` and an `issues` list when any condition marked "issue" below holds
(D82). `measure latency` takes the same two forms and reports server-side latency per operation over
`--minutes` (default 15). Each keeps its spans in `.traces/measure/<name>`: the preview's name, or
the collector URL's host for `--collector-url`, so production and a preview never share a store.

Every signal below is read from a span or a tail record. For the `trace-cli` commands, first pull
the stage's spans into a store (`trace-cli pull … --store "$S"`, "Reading a stage's telemetry")
and pass the same `--store "$S"`.

| Signal                       | Where it is recorded                                                                                          | Read it with                                                                                                 | Act when                                                                                              |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| Invocations that hit a limit | Tail records: outcome `exceededCpu`, `exceededMemory`, `exception` (every one kept, D71)                      | `measure health`: issue                                                                                      | Any. CPU limits are 30 s (API, writers) and 60 s (compactor); see `docs/measurements.md`              |
| Writes stalled               | `DbWriter.write` errors with `WriteStalledError` and `writer.stall_reason` (`L0Backlog`, `UnflushedBytes`)    | `measure health`: issue per database and reason; `trace-cli grep --store "$S" --name DbWriter.write --error` | Sustained: L0 is not draining (backlog, corruption, a fenced compactor) or a writer is overloaded     |
| Flush duration               | `DbWriter.flush` span duration                                                                                | `measure health`: `flushMs`, issue above 30 s at p99; `trace-cli stats --store "$S" --name DbWriter.flush`   | p99 above about 30 s, or rising                                                                       |
| Compaction backlog           | `manifest.l0_ssts` on `Compactor.poll`                                                                        | `measure health`: `l0Backlog`, issue at `l0MaxSsts` (16)                                                     | Near `l0MaxSsts`, where writes stall                                                                  |
| Fenced compactor             | `compactor.fenced` and `database.id` on `Compactor.poll`; `compactor.refenced` on the next start's span (D78) | `measure health`: issue per database; `GET …/databases/<id>/compaction` (`compactor.fenced`)                 | Any: it fences in again on its next start, but its state was overwritten, which is a defect to report |
| Corrupted SSTs               | `compaction.corrupt_ssts` on `Compactor.poll`; the compactor's error log (a span event on that poll)          | `measure health`: issue, from each database's latest poll; `GET …/compaction` (`corruptSsts`)                | Any: follow the corruption runbook                                                                    |
| Unverified parts             | `object.parts_unverified` on `ObjectStore.putMultipart`                                                       | `measure health`: issue                                                                                      | Any: R2 stopped returning part digests, so parts are no longer checked                                |
| Re-sent parts                | `object.part_retried` on `ObjectStore.putMultipart`; its warning log is a span event there                    | `measure health`: `uploads.withRetriedPart` (a count, not an issue)                                          | Frequent: parts keep arriving changed                                                                 |
| Garbage collection           | errors on `GarbageCollector.collect` and `MaintenanceSweep.sweepPage`                                         | `measure health`: issue                                                                                      | Repeated failures: storage unreachable or a manifest unreadable                                       |
| Error spans                  | any span with an error status                                                                                 | `measure health`: `errorSpans`; `trace-cli stats --store "$S" --error`                                       | A new name in the top errors                                                                          |

**Sampling.** Spans are sampled in production (10%, P4), so these span-borne signals are samples.
A stall or a corruption repeats on every write or poll and is seen when it persists. A single
unverified part or fenced poll can be missed. A database that neither writes nor compacts produces
no spans, so nothing here shows it.

**Metrics are not collected.** The engine also counts events as OTLP metrics:
`lsm_compaction_corrupt_ssts`, `lsm_object_part_integrity_retries`, `lsm_object_parts_unverified`,
`lsm_write_stalls`, and eight more (D82). The trace collector and the local trace receiver accept
metrics and discard them, so no stage keeps them and no command reads them. They are there for a
future collector. Every signal above travels on a span instead.

Expected errors that are not trouble: `DatabaseSealedError` (a stale router's write to a sealed
shard; the router retries), `TransactionConflictError`, and `DatabaseAlreadyExists` on create.

## Corruption runbook

### How corruption is prevented

- Single-part uploads (manifests, small SSTs, hints) send their SHA-256. R2 refuses a body that
  changed in transit, and the write fails and retries.
- Multipart uploads (large SSTs) check each part against the MD5 R2 returns as its ETag. A part
  that arrived changed is uploaded again, up to three times. Then the upload fails, is aborted, and
  the flush or compaction retries. An upload that re-sent a part carries `object.part_retried`, and
  `object.parts_unverified` counts its parts whose ETag was not an MD5. On R2 that should stay 0;
  if it rises, R2 stopped reporting part digests and parts are no longer checked. `measure health`
  reports both ("What to watch").
- Every SST block and section carries a CRC32C that every read checks.

### How corruption is detected

A compaction that reads a damaged SST ends with a typed `Corrupted` outcome (D53). The database's
compactor then:

- records the SST,
- reports it on every poll as `compaction.corrupt_ssts` (which `measure health` reads),
- logs `Database <id>'s SST <sst> is corrupted (<reason> in <section>)`,
- stops compacting that SST's L0 SST or sorted run.

A corrupted **L0** SST pauses all L0 compaction, because L0 merges oldest first. Once L0 fills,
writes fail with `WriteStalled`. Reads that touch the SST fail with `StoredDataInvalid` (500).

Find the report:

```sh
curl -H "Authorization: Bearer $TOKEN" "$API/v1/databases/$DB/compaction"
```

`corruptSsts` lists each corrupted SST: `sstId`, `createdAt` (when it was written), `section`,
`reason`, and `detectedAt`. `l0SstIds` and `sortedRuns` show where it sits.

### Decide: restore or drop

|               | Restore from a checkpoint                                                                     | Drop the SST                                                                         |
| ------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| What is lost  | Every write after the checkpoint, across the whole database                                   | Only the dropped SST's versions of its keys; older versions of those keys show again |
| When it works | A checkpoint created **before** the SST's `createdAt` (its manifest cannot reference the SST) | Always                                                                               |
| Result        | A new database (a fork)                                                                       | The same database                                                                    |

List the checkpoints and compare each `createdAt` with the SST's `createdAt`:

```sh
curl -H "Authorization: Bearer $TOKEN" "$API/v1/databases/$DB/checkpoints"
```

Prefer dropping a small L0 SST, which holds only recent writes. Prefer restoring when a large
sorted-run SST holds old data and a recent checkpoint predates it.

### Restore

Fork the full key range from the checkpoint into a new database, then point callers at it:

```sh
curl -X POST -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  "$API/v1/databases/$DB/forks" \
  -d '{"forkId":"<new-db>","checkpointId":"<checkpoint>","range":{"start":"","end":null}}'
```

Remove the damaged database once nothing reads it (`DELETE /v1/databases/$DB`).

### Drop

```sh
curl -X POST -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  "$API/v1/databases/$DB/ssts/$SST/drop" -d "{\"confirm\":\"$SST\"}"
```

`confirm` must repeat the SST id. The response names what was lost: the SST, where it sat (`from`),
its key range (`firstKey`, `lastKey`, and the `view` the database read through), and its row count.
The database's compactor makes the change in one manifest commit. It records the drop in the
manifest's `droppedSsts` and resumes compaction. The SST's object stays until GC finds it
unreferenced, so readers holding an older manifest are unaffected. Repeating the request returns
the same drop.

The drop is refused with `409 SstDropRefused` in three cases. Nothing changes in any of them:

- `ConfirmationMismatch`: `confirm` did not repeat the SST id.
- `NotReportedCorrupted`: no compaction reported the SST corrupted. Check the id. Add
  `"force": true` only to drop a healthy SST on purpose, for example one that reads fail on before
  any compaction touched it.
- `CompactionRunning`: a running compaction reads the SST's source. Retry once it finishes.

An SST that is not in the manifest returns `404 SstNotFound`.

### Verify

```sh
curl -H "Authorization: Bearer $TOKEN" "$API/v1/databases/$DB/compaction"
```

- `corruptSsts` no longer lists the SST.
- `droppedSsts` lists it.
- `l0Ssts` falls as compaction resumes.
- Writes no longer stall, and reads of the affected range succeed.

The `repair` scenario (`node tools/scenario-cli/src/scenario-cli.ts run repair`) runs this whole
procedure on a local or preview stack: it damages an SST, waits for the report, and checks the
refusal, the drop, and the reads.
