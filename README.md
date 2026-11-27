# shaledb

A SlateDB-style LSM key-value store on Cloudflare R2 and Durable Objects.

shaledb keeps SlateDB's model:

- one writer and many readers per database;
- immutable SSTs in object storage;
- a manifest versioned by compare-and-swap;
- size-tiered compaction.

It swaps two pieces for Cloudflare-native ones:

- **The writer** is a Durable Object, and its SQLite table is both the write-ahead log and the
  memtable. A write is durable when the call returns.
- **Compaction** runs in a Workflow.

The unit is a **database**: one id with its own writer, compactor, manifest chain, R2 prefix
(`db/<id>/`), and cache tag. Databases are created, checkpointed, forked, sealed, split, and
removed through the API. Optional range sharding (`packages/lsm-sharding`) splits a key space
across databases.

The design is [`docs/show-me-r2-lsm-design.html`](docs/show-me-r2-lsm-design.html).
[`docs/design-decisions.md`](docs/design-decisions.md) records every judgment call it did not make.

## Who it is for

shaledb is for many independent writers spread across many databases, not one global hot writer.
The unit of scale is the database, and each database has one writer Durable Object. Throughput
grows by adding databases (and, for one key space, by splitting it with range sharding), not by
making one database's writer faster.

One database holds at most about 312 GiB: past that, a compaction of the whole database outgrows
what one Cloudflare Workflow may run (D103). Long keys lower it, because the manifest lists every
SST's first and last key (D99). Larger data is split into several databases with range sharding.
Every limit a caller can meet is listed in [`docs/api-compatibility.md`](docs/api-compatibility.md),
"Limits of v1".

One database's write rate is bounded by its writer, which commits one batch at a time. The highest
rates measured on one writer (`preview-1`, production settings, 16-byte keys and 100-byte values,
a laptop client on a US residential connection, so latencies include an internet round trip):

<!-- STEP4-NUMBERS: begin -->

| Batch rows | Batches in flight | Rows/s acknowledged | Client latency p50 / p90 / p99 (ms) |
| ---------: | ----------------: | ------------------: | ----------------------------------- |
|        100 |                 8 |               6,547 | 105 / 136 / 427                     |
|      1,000 |                16 |              15,257 | 961 / 1,706 / 2,400                 |

The writer was busy about half the time at both loads, so these are the rates measured, not a
proven ceiling ([`docs/measurements.md`](docs/measurements.md), "Writer throughput and limits").

<!-- STEP4-NUMBERS: end -->

Reads cost the writer differently by consistency:

- **`Latest`** sees every acknowledged write, and costs its database's writer one probe on every
  read (the probe took 10 ms at p50 measured in the API Worker). Every `Latest` read of a database
  is served by that one writer, and pays the distance to it.
- **`AtLeast`, `Flushed`, snapshots, and scans of flushed data** run in the caller's Worker and do
  not touch the writer (`AtLeast` asks it only when the caller's cached manifest does not yet
  cover the sequence number). They scale with readers, and a `Flushed` read may lag the newest
  flush by about a second and unflushed writes by up to a minute.

Both are measured in [`docs/measurements.md`](docs/measurements.md), "Read latency".
[`docs/invariants.md`](docs/invariants.md) states exactly what each read may see.

It is not a fit for:

- one hot key range that needs more than one writer's throughput, without range sharding;
- workloads that need atomic writes or transactions across databases: batches and transactions are
  atomic within one database only;
- `Latest` reads under 10 ms from far away: with the writer in the EU and the API Worker in the
  US, a `Latest` get took about 130 ms at p50 in the API Worker, against 19 ms on `preview-3`, whose
  writers answered probes in 10 ms;
- anything [`docs/measurements.md`](docs/measurements.md) does not cover (its "Not measured" list
  names what is open).

<!-- BASELINE: begin -->

### Compared with a plain Durable Object SQLite table

The same workload against shaledb and against one table in a plain Durable Object, on the same
stage (`preview-3`, production settings), from the same client, one target after the other, each
on a new database or object: 8 writers each sending 100-row batches (16-byte keys, 100-byte
values) and 8 readers each doing point reads of preloaded keys, for 30 seconds.

> **Provisional.** One run, 2026-11-27, at commit `ed4e787`, while a local test run loaded the
> client during the plain-table part. The release checklist repeats it three times on the release
> commit ([`docs/release-v1.md`](docs/release-v1.md)); this table is replaced by those runs.

| Target                      | Rows/s acknowledged | Write p50 / p90 / p99 (ms) | Reads/s | Read p50 / p90 / p99 (ms) |
| --------------------------- | ------------------: | -------------------------- | ------: | ------------------------- |
| shaledb, reads at `Latest`  |               6,134 | 114 / 162 / 425            |    60.3 | 114 / 175 / 592           |
| shaledb, reads at `Flushed` |               8,329 | 87 / 124 / 269             |   115.9 | 57 / 114 / 203            |
| plain Durable Object SQLite |               8,193 | 82 / 132 / 359             |    89.1 | 77 / 120 / 429            |

Each writer and reader waits for its answer before sending the next request, so the rates follow
the latencies: they are what 8 clients over the internet got, not a capacity. What the stage did
for each run, from its tail records and spans:

| Target                      | Worker CPU per request | Durable Object requests per read | Durable Object CPU per request | SQLite rows written per row | R2 class A / class B requests |
| --------------------------- | ---------------------: | -------------------------------: | -----------------------------: | --------------------------: | ----------------------------- |
| shaledb, reads at `Latest`  |                16.6 ms |                                1 |                         3.3 ms |                           3 | 5 / 609                       |
| shaledb, reads at `Flushed` |                11.2 ms |                                0 |                         4.7 ms |                           3 | 10 / 6,774                    |
| plain Durable Object SQLite |                 8.0 ms |                                1 |                         2.3 ms |                           1 | 0 / 0                         |

**What was measured.**

- **Writes cost the caller the same.** Write latency is within run-to-run noise of the plain
  table's; nearly all of it is the client's round trip.
- **A `Latest` read is slower than a plain read** (p50 114 ms against 77 ms): it asks the writer
  and then reads SSTs.
- **A `Flushed` read is faster and never touches a Durable Object** (p50 57 ms against 77 ms; the
  only Durable Object requests in that run were the writes).
- **shaledb's writes cost three times a plain table's in SQLite charges.** Its memtable bills two
  rows written per row (the table and its index) and one more when the flushed row is deleted;
  a plain table bills one (measured on `preview-3` by the row-cost probe). At Cloudflare's
  published overage prices, fetched 2026-11-26 and ignoring included allowances, that is $3.02
  against $1.01 per million rows written with this read mix; SQLite row writes are $3.00 and
  $1.00 of it.

**What that buys, and how far each claim is measured.**

- **Data leaves the Durable Object.** A plain table lives inside one object's SQLite storage,
  with that product's size limit and storage price; shaledb's flushed data is in R2. The price
  and the limit are Cloudflare's published figures, not measurements
  ([`docs/operations.md`](docs/operations.md), "Platform limits").
- **Reads that do not load the writer.** Measured above for `Flushed`; `AtLeast`, snapshots and
  scans of flushed data take the same path.
- **Forks, checkpoints, pinned snapshots, and splits** have no counterpart in a plain table.
  Their costs are measured separately ([`docs/measurements.md`](docs/measurements.md), "Split
  write pause"); this comparison does not include them.
- **Not measured here:** storage cost over time, a dataset near a Durable Object's storage
  limit, readers in other regions, and either system above 8 clients.

If one Durable Object's table holds the data and its writer can serve every read, the plain
table is simpler and cheaper to write to. The numbers, the commands, and the price arithmetic
are in [`docs/measurements.md`](docs/measurements.md), "Baseline: a plain Durable Object SQLite
table".

<!-- BASELINE: end -->

## Quick start (local)

Requirements: Node 24 and [Vite+](https://viteplus.dev) (`vp`).

```sh
vp install
vp run dev                                    # the whole Stack on local R2, Durable Objects, Workflows
node tools/trace-cli/src/trace-cli.ts serve   # optional: receive every span on 127.0.0.1:4318
```

The API is at `http://localhost:1337/v1`. Local stages need no token. Keys and values are base64
in JSON.

```sh
curl -X POST localhost:1337/v1/databases -H 'content-type: application/json' \
  -d '{"databaseId":"orders","keyRange":null,"mergeOperator":null,"changelogRetentionMs":null,"budgetGroup":null}'
curl -X POST localhost:1337/v1/databases/orders/batches -H 'content-type: application/json' \
  -d '{"operations":[{"_tag":"Put","key":"dXNlcjo0Mg==","value":"QWRh","ttlMillis":null}]}'
curl -X POST localhost:1337/v1/databases/orders/get -H 'content-type: application/json' \
  -d '{"key":"dXNlcjo0Mg==","consistency":{"_tag":"Latest"},"snapshot":null}'
```

`node tools/scenario-cli/src/scenario-cli.ts run all` runs every end-to-end scenario against the
local Stack, with trace assertions.

## The API

The contract is [`docs/openapi/shaledb-v1.json`](docs/openapi/shaledb-v1.json), generated
from `DbHttpApi`, with examples of every request and response. It covers:

- **Databases:** create, status, remove. A removed database's id can be created again only an
  hour (`gcMinAge`) after its removal; before that, create answers `DatabaseRecentlyRemoved` (D90).
- **Writes:** batches of puts, deletes, and merges, with an optional TTL each. `flush` makes
  everything acknowledged durable in R2. A TTL is judged by the reader's clock: a value expires
  within clock skew of its expiry time, readers on different machines may disagree for up to that
  skew, and a value compaction has removed does not return (D83).
- **Reads:** `get` and `scan` at `Latest` (sees every acknowledged write), `AtLeast` a sequence
  number, or `Flushed`; at a snapshot; pinned snapshots that outlive GC. A scan page ends at its
  limit or after examining 16 MiB of versions, whichever comes first, so a page may hold fewer
  rows than asked, or none: follow `resumeFrom` until it is null (D108).
- **Transactions:** `Snapshot` or `SerializableSnapshot` isolation, with conflicts detected at
  commit.
- **Change feed:** committed changes in sequence order, when the database's changelog is on.
  It says `Expired` rather than skip changes retention dropped. A fork's or split successor's
  feed starts at its fork point, where the sealed source's feed ends.
- **Lifecycle:** checkpoints, forks of a key range from a checkpoint, and seal (for splits).
- **Repair:** the compaction status, and dropping an SST that compaction reported corrupted.

Every response carries `x-request-id`. Errors share one envelope:

```json
{ "code": "...", "message": "...", "details": {}, "requestId": "...", "retryable": false }
```

[`docs/api-compatibility.md`](docs/api-compatibility.md) states what `/v1` promises.

Live stages require `Authorization: Bearer <token>`. Each stage has two rotatable tokens (see
[`docs/operations.md`](docs/operations.md)).

## Repository

| Path                      | What it is                                                                                                                                         |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `packages/lsm-core`       | Pure LSM logic: ids, rows, codecs, Bloom filters, the merge iterator, the manifest and its edits, the compaction scheduler. No Cloudflare imports. |
| `packages/lsm-cloudflare` | The engine on Workers: object store, manifest store, writer, reader, compactor, compaction Workflow, GC, lifecycle, the HTTP APIs.                 |
| `packages/lsm-sharding`   | Optional range sharding over the public API.                                                                                                       |
| `infra/shaledb`           | The Alchemy Stack: buckets, Durable Objects, Workers, the Workflow, telemetry, access tokens.                                                      |
| `tools/*`                 | Trace CLI, scenario and measurement CLI, preview CLI, writer benchmark, formal verification (TLA+).                                                |

## Documentation

- [`docs/invariants.md`](docs/invariants.md): what makes a write acknowledged and what makes it
  flushed, what each kind of read may see, and when GC may reclaim an SST, with the code, tests,
  and models that hold each. Read it before changing a write path, a read path, or GC.
- [`docs/operations.md`](docs/operations.md): deploying, rolling back, tokens, telemetry, what to
  watch, and the corruption runbook.
- [`docs/measurements.md`](docs/measurements.md): throughput, latency, and limits, measured on
  live stages.
- [`docs/design-decisions.md`](docs/design-decisions.md): decisions D1 onward.
- [`docs/coverage-matrix.md`](docs/coverage-matrix.md): each design section mapped to its code and
  tests.
- [`docs/testing.md`](docs/testing.md), [`docs/coding-standards.md`](docs/coding-standards.md),
  [`docs/errors.md`](docs/errors.md): how to change it.

## Verifying a change

```sh
vp run ready    # format, lint, type checks, every workspace's tests (unit, property, workerd)
vp run verify   # every TLA+ model, including its deliberately broken variants
```

A release is also gated by `scenario-cli soak` on a preview stage (D65, D106): mixed writes,
reads, scans, transactions, the change feed, splits, removals, and a redeploy, checked against a
model of every acknowledged write ([`docs/testing.md`](docs/testing.md)).
