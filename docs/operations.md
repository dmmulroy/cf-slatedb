# Operations

How to run cf-slatedb in production. Workstream 9 of [`handoff-v1.md`](handoff-v1.md) completes
this document (deploy, rollback, token rotation, jurisdiction, GC and removal, capacity). The
corruption runbook is from workstream 7 (D62); telemetry and what to watch are from workstream 8
(D63).

Every endpoint below is on the API Worker under `/v1` and needs the stage's token
(`Authorization: Bearer <token>`).

## Telemetry

Every Worker and Durable Object exports spans over OTLP to the stage's own trace collector, which
stores them in the stage's trace bucket (D63):

- **Sampling.** Production keeps 10% of traces (`CF_SLATEDB_TRACE_SAMPLE_RATE`, P4). Local and
  preview stages keep every trace. The decision comes from each trace's id, so a kept trace is
  whole across the API Worker, its Durable Objects, and the BlockServer. A caller's `traceparent`
  joins our spans to its trace, but its sampled flag does not force them to be kept.
- **Redaction.** Spans carry only cf-slatedb's own attributes (`database.id`, `sst.id`,
  `manifest.id`, sizes, counts, outcomes) and the HTTP method, route, path, and status. A caller's
  address, user agent, full URL, query string, and headers are dropped where they are set.
- **Retention.** The trace bucket deletes `traces/` and `tails/` objects after 14 days
  (`CF_SLATEDB_TRACE_RETENTION_DAYS`).
- **Tail records.** The API, compactor, and GC Workers' invocations (their outcome, CPU time, and
  wall time) are stored unsampled under `tails/`.

The telemetry probe Worker, a test tool, is not deployed to production.

## What to watch

`node tools/scenario-cli/src/scenario-cli.ts measure health --stage <stage> --minutes 60` summarizes
a stage from its tail records and spans. It prints `healthy: false` and an `issues` list when any of
the conditions below holds.

| Signal                       | Where                                                                                            | Act when                                                                                    |
| ---------------------------- | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- |
| Invocations that hit a limit | Tail records: outcome `exceededCpu`, `exceededMemory`, `exception`                               | Any. CPU limits are 30 s (API, writers) and 60 s (compactor); see `docs/measurements.md`    |
| Writes stalled               | `DbWriter.write` errors with `WriteStalledError`; public `WriteStalled` (503)                    | Sustained: L0 is not draining (compaction backlog or corruption) or a writer is overloaded  |
| Flush duration               | `DbWriter.flush` span duration                                                                   | p99 above about 30 s, or rising                                                             |
| Compaction backlog           | `manifest.l0_ssts` on `Compactor.poll`                                                           | Near `l0MaxSsts` (16), where writes stall                                                   |
| Corrupted SSTs               | `lsm_compaction_corrupt_ssts`; `compaction.corrupt_ssts` on `Compactor.poll`; `GET …/compaction` | Any: follow the corruption runbook                                                          |
| Upload integrity             | `lsm_object_part_integrity_retries`, `lsm_object_parts_unverified`                               | Retries: transient, unless frequent. Any unverified part on R2: parts are no longer checked |
| Garbage collection           | `GarbageCollector.collect` and `MaintenanceSweep.sweepPage` errors                               | Repeated failures: storage unreachable or a manifest unreadable                             |
| Error spans                  | `trace-cli grep --error`                                                                         | A new name in the top errors                                                                |

Expected errors that are not trouble: `DatabaseSealedError` (a stale router's write to a sealed
shard; the router retries), `TransactionConflictError`, and `DatabaseAlreadyExists` on create.

## Corruption runbook

### How corruption is prevented

- Single-part uploads (manifests, small SSTs, hints) send their SHA-256. R2 refuses a body that
  changed in transit, and the write fails and retries.
- Multipart uploads (large SSTs) check each part against the MD5 R2 returns as its ETag. A part
  that arrived changed is uploaded again, up to three times. Then the upload fails, is aborted, and
  the flush or compaction retries. `lsm_object_part_integrity_retries` counts the re-uploads.
  `lsm_object_parts_unverified` counts parts whose ETag was not an MD5. On R2 this should stay 0;
  if it rises, R2 stopped reporting part digests and parts are no longer checked.
- Every SST block and section carries a CRC32C that every read checks.

### How corruption is detected

A compaction that reads a damaged SST ends with a typed `Corrupted` outcome (D53). The database's
compactor then:

- records the SST,
- increments `lsm_compaction_corrupt_ssts`,
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
