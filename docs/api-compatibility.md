# `/v1` compatibility policy

`/v1` is shaledb's public HTTP API. Its contract is
[`openapi/shaledb-v1.json`](openapi/shaledb-v1.json). The OpenAPI test fails whenever the
code and the checked-in specification differ, and every change to the specification is reviewed in
its diff.

## What `/v1` promises

A client written against `/v1` keeps working for as long as `/v1` is served. Within `/v1`, a
release may only:

- add an endpoint;
- add an optional request field (absent or `null` means the earlier behavior);
- add a response field (clients must ignore fields they do not know);
- add an error `code`, or a new value of a `details.reason` (clients must treat an unknown code by
  its HTTP status and `retryable`);
- relax a limit, such as accepting a larger batch.

It never:

- removes or renames an endpoint, a field, or an error code;
- changes a field's type or meaning;
- makes an optional field required;
- tightens a limit;
- changes when a request is retryable;
- weakens what a read sees at a given consistency.

What a read sees at a snapshot, a pinned snapshot, or a transaction's `readSeq` is a set of
writes. TTLs are judged at each read's own time, so a value at a fixed point may expire between
two reads (`docs/invariants.md`). That is v1's contract, not a weakening a later release may
make.

Anything else is a new major version (`/v2`). During a published deprecation window, `/v1` and
`/v2` are served side by side.

## What the contract does not cover

The database API Worker's `/v1` is the contract. Two other Workers also serve paths under `/v1`
and are not part of it:

- **The shard router** (`ShardedHttpApi` in `packages/lsm-sharding`, the `shardsUrl` output). It
  is the sharding package's example consumer of the public API (D40). It may change in any
  release.
  - It answers a request it refuses before any handler runs (an unknown route, a malformed
    request) with the same envelope as the database API (D79).
  - Its declared errors are `{ _tag, message, … }`, not the envelope. Their `reason` field holds an
    internal error tag (such as `WriterUnavailableError`) and may change in any release: decide
    from the status and `_tag`, never from `reason`.
- **The GC operator API** (`gcUrl`), for operators. It shares the same envelope for refusals and
  defects (D79). Its declared errors (`DatabaseNotFoundError`, `GarbageCollectorUnavailableError`)
  are `{ _tag, databaseId, message }`, with messages of its own and never storage detail.

## Request bounds (D100)

- **Request bodies:** at most 16 MiB (16,777,216 bytes), whether or not the request declares its
  length. A larger body is refused with `InvalidRequest` (400) once its first 16 MiB are read; the
  rest is never read. The largest legitimate body, a write batch of 10,000 operations and 8 MiB of
  keys and values, is about 11.4 MiB as base64 JSON.
- **Fields:** every string, bytes, and array field of every request has a length bound, stated in
  the OpenAPI document. The table is in D100.
- **Names are trusted:** database ids and `budgetGroup` names follow one pattern (1–63 lowercase
  letters, digits, `.`, `_`, `-`). Inside one installation they are trusted names. A caller who
  holds the stage's access token may name any database or group. The access token is the only
  tenant boundary.

## Limits of v1

Every bound a caller can meet in v1. A later v1 release may relax a bound ("Relax" yes), never
tighten it. A bound held by a Cloudflare limit can be relaxed only when that limit moves.
`api-compatibility-limits.test.ts` (lsm-cloudflare) fails when a constant in the "Constant"
column changes without its value here.

| Bound                                                        | Value                                                                                                                                                                                                                                                                 | Constant (file)                                                                      | Error                                                                              | Relax                                                                                                 |
| ------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Operations in a write batch                                  | 10,000                                                                                                                                                                                                                                                                | `MAX_BATCH_OPERATIONS` (`lsm-core/src/write-batch.ts`)                               | `InvalidRequest` (400)                                                             | yes                                                                                                   |
| Keys and values in a write batch                             | 8,388,608 bytes (8 MiB)                                                                                                                                                                                                                                               | `MAX_BATCH_BYTES` (`lsm-core/src/write-batch.ts`)                                    | `BatchInvalid` (400)                                                               | yes, within the body bound                                                                            |
| A key                                                        | 16,384 bytes (16 KiB)                                                                                                                                                                                                                                                 | `MAX_KEY_BYTES` (`lsm-core/src/write-batch.ts`)                                      | `InvalidRequest` (400)                                                             | yes; it also sets the database size limit below                                                       |
| A value or merge operand                                     | 1,048,576 bytes (1 MiB)                                                                                                                                                                                                                                               | `MAX_VALUE_BYTES` (`lsm-core/src/write-batch.ts`)                                    | `InvalidRequest` (400)                                                             | yes, up to a Durable Object SQLite row (8 MB documented, 2026-11-13; D100 says 2 MB)                  |
| A merged value, as read                                      | 1,048,576 bytes (1 MiB)                                                                                                                                                                                                                                               | `MAX_MERGED_VALUE_BYTES` (`lsm-core/src/row-resolution.ts`)                          | `MergedValueTooLarge` (409)                                                        | yes                                                                                                   |
| `ttlMillis`, a checkpoint's `lifetimeMs`                     | 3,153,600,000,000 ms (100 years)                                                                                                                                                                                                                                      | `MAX_TTL_MILLIS` (`lsm-core/src/write-batch.ts`)                                     | `InvalidRequest` (400)                                                             | no: past it a timestamp leaves the safe-integer range (D88)                                           |
| Work of one scan page: regular, transaction, sharded         | 16,777,216 bytes (16 MiB) of versions examined; past it the page ends early, with fewer rows or none and `resumeFrom` (D108, D125)                                                                                                                                    | `SCAN_PAGE_EXAMINED_BYTES` (`lsm-cloudflare/src/db-reader.ts`)                       | none: a page with `resumeFrom`                                                     | yes                                                                                                   |
| Memory of one scan page: regular, transaction, sharded       | one writer page (8,388,608 bytes, 8 MiB, and 1,000 rows) + 16,777,216 bytes of returned rows + one key's versions + the reader's blocks (D125)                                                                                                                        | `MAX_MEMTABLE_PAGE_BYTES`, `SCAN_PAGE_EXAMINED_BYTES`                                | none                                                                               | yes                                                                                                   |
| A pinned snapshot's `lifetimeMs`                             | 30 days                                                                                                                                                                                                                                                               | inline in `PinSnapshotRequest` (`lsm-cloudflare/src/db-http-api.ts`)                 | `InvalidRequest` (400)                                                             | yes                                                                                                   |
| Rows in a scan or transaction-scan page                      | 1,000                                                                                                                                                                                                                                                                 | `MAX_SCAN_PAGE_ROWS` (`lsm-cloudflare/src/db-http-api.ts`)                           | `InvalidRequest` (400)                                                             | yes                                                                                                   |
| Bytes of one memtable page (writer scan, flush, change feed) | 8,388,608 bytes (8 MiB)                                                                                                                                                                                                                                               | `MAX_MEMTABLE_PAGE_BYTES` (`lsm-cloudflare/src/mem-table.ts`)                        | none: the page ends early and the caller continues                                 | internal                                                                                              |
| Versions a scan page examines (returned or not)              | 16,777,216 bytes (16 MiB), then the page ends early with `resumeFrom`                                                                                                                                                                                                 | `SCAN_PAGE_EXAMINED_BYTES` (`lsm-cloudflare/src/db-reader.ts`, D108)                 | none: fewer rows, or none, and a `resumeFrom` to follow                            | yes                                                                                                   |
| Rows in a change-feed page                                   | 1,000, whole batches                                                                                                                                                                                                                                                  | inline in `ChangeFeedQuery` (`db-http-api.ts`)                                       | `InvalidRequest` (400)                                                             | yes                                                                                                   |
| A request body                                               | 16,777,216 bytes (16 MiB)                                                                                                                                                                                                                                             | `MAX_REQUEST_BODY_BYTES` (`lsm-cloudflare/src/db-http-request.ts`)                   | `InvalidRequest` (400)                                                             | yes, up to the zone plan's limit (100 MB on Free and Pro)                                             |
| A transaction's `readKeys`                                   | 10,000                                                                                                                                                                                                                                                                | `MAX_TRANSACTION_READ_KEYS` (`lsm-core/src/transaction.ts`)                          | `InvalidRequest` (400)                                                             | yes                                                                                                   |
| A transaction's `readRanges`                                 | 1,000                                                                                                                                                                                                                                                                 | `MAX_TRANSACTION_READ_RANGES` (`lsm-core/src/transaction.ts`)                        | `InvalidRequest` (400)                                                             | yes                                                                                                   |
| A seal's `successors`                                        | 16                                                                                                                                                                                                                                                                    | `MAX_SEAL_SUCCESSORS` (`lsm-cloudflare/src/db-http-api.ts`)                          | `InvalidRequest` (400)                                                             | yes                                                                                                   |
| A checkpoint's `name`                                        | 256 characters                                                                                                                                                                                                                                                        | `MAX_CHECKPOINT_NAME_LENGTH` (`lsm-cloudflare/src/db-http-api.ts`)                   | `InvalidRequest` (400)                                                             | yes, within the manifest budget                                                                       |
| An SST drop's `confirm`                                      | 64 characters                                                                                                                                                                                                                                                         | inline in `DropSstRequest` (`db-http-api.ts`)                                        | `InvalidRequest` (400)                                                             | yes                                                                                                   |
| Database ids, `budgetGroup`, shard groups                    | 1–63 of `a-z 0-9 . _ -`, starting with a letter or digit                                                                                                                                                                                                              | `DatabaseId` (`lsm-core/src/ids.ts`), `ShardGroup` (`lsm-sharding/src/shard-map.ts`) | `InvalidRequest` (400)                                                             | no: the id names R2 prefixes, Durable Objects, and cache tags                                         |
| SST and checkpoint ids                                       | 26-character ULIDs                                                                                                                                                                                                                                                    | `SstId`, `CheckpointId` (`lsm-core/src/ids.ts`)                                      | `InvalidRequest` (400)                                                             | no                                                                                                    |
| Checkpoints and pinned snapshots per database                | 128 (forks' holds not counted)                                                                                                                                                                                                                                        | `MAX_CHECKPOINTS_PER_DATABASE` (`lsm-core/src/manifest.ts`)                          | `CheckpointLimitReached` (409)                                                     | yes, within GC's read budget (D99, D129)                                                              |
| Changelog entries kept                                       | the newest 2,048 flushes                                                                                                                                                                                                                                              | `MAX_CHANGELOG_ENTRIES` (`lsm-core/src/manifest.ts`)                                 | none at write; a reader behind them gets `ChangelogUnavailable` (`Expired`)        | yes, within the manifest budget                                                                       |
| `changelogRetentionMs`                                       | 2,592,000,000 ms (30 days)                                                                                                                                                                                                                                            | `MAX_CHANGELOG_RETENTION_MS` (`lsm-cloudflare/src/db-http-api.ts`)                   | `InvalidRequest` (400)                                                             | yes                                                                                                   |
| Database size                                                | about 312 GiB with the Workflow's 2× step headroom; a bottom merge above about 625 GiB fails at 10,000 steps (D103). The manifest's 4 MiB budget bounds it too: about 1 TiB with 16-byte keys, 300 GiB with 256-byte, 90 GiB with 1 KiB, 6 GiB with 16 KiB keys (D99) | derived                                                                              | not enforced: past it compactions fail, then commits and cold reads slow           | yes, with the Workflow's step limit raised (25,000 at most) and a manifest without one handle per SST |
| Manifest commits per database                                | about 1.4 a second sustained for `gcMinAge` (1 hour) before SST collection is deferred                                                                                                                                                                                | `gcMaxRetainedManifestLoads`, 5,000 (`lsm-core/src/lsm-settings.ts`, D98)            | none: old manifests still go, unreferenced SSTs wait (`measure health` reports it) | yes (a setting)                                                                                       |
| Compaction size run live                                     | about 0.5 GiB (four 60 MiB L0 SSTs); everything above it is derived (D103)                                                                                                                                                                                            | —                                                                                    | —                                                                                  | —                                                                                                     |
| Recreating a removed database's id                           | not before `gcMinAge` (1 hour) after its removal                                                                                                                                                                                                                      | `Removed.removedAt` in the head manifest (D90, D91)                                  | `DatabaseRecentlyRemoved` (409), with when it may be retried                       | no: shorter would let a cached manifest of the old database serve the new one                         |

The sharded API (`ShardedHttpApi`) shares the field bounds (D100) and is not part of the contract.

## Limits tightened to stop a defect

- `ttlMillis` and a checkpoint's `lifetimeMs` are at most 100 years (D88). A larger value had been
  accepted, but a larger TTL then made every flush of the database fail, and a larger lifetime
  failed with `InternalFailure`. No working request is refused.

## Examples from v1's history

These shipped inside `/v1` because they are additive:

- `manifestId` on transaction begin, get, and scan (D58), where an old client's request keeps the
  earlier behavior;
- `l0SstIds`, `corruptSsts`, and `droppedSsts` on the compaction status (D62);
- the SST drop and checkpoint-list endpoints (D62).

## Stored data

The manifest and the compactor state carry a `formatVersion` (D64). A release that writes a format
older code cannot read raises it, and older code refuses to read or write data in a newer format
instead of rewriting it. A rollback across such a release therefore stops on the newer data, with
an error naming the version to deploy, and never damages it. Data written before format versions
reads as format 1. The rollback procedure is in [`operations.md`](operations.md).
