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
  everything acknowledged durable in R2.
- **Reads:** `get` and `scan` at `Latest` (sees every acknowledged write), `AtLeast` a sequence
  number, or `Flushed`; at a snapshot; pinned snapshots that outlive GC.
- **Transactions:** `Snapshot` or `SerializableSnapshot` isolation, with conflicts detected at
  commit.
- **Change feed:** committed changes in sequence order, when the database's changelog is on.
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
