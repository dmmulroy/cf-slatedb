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

<!-- BASELINE: the orchestrator fills this -->

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

- **Databases:** create, status, remove.
- **Writes:** batches of puts, deletes, and merges, with an optional TTL each. `flush` makes
  everything acknowledged durable in R2. A TTL is judged by the reader's clock: a value expires
  within clock skew of its expiry time, readers on different machines may disagree for up to that
  skew, and a value compaction has removed does not return (D83).
- **Reads:** `get` and `scan` at `Latest` (sees every acknowledged write), `AtLeast` a sequence
  number, or `Flushed`; at a snapshot; pinned snapshots that outlive GC.
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
