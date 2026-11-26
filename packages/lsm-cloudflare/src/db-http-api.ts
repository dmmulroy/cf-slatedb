import {
  Checkpoint,
  CheckpointId,
  DatabaseId,
  DroppedSst,
  KeyRange,
  ManifestId,
  MergeOperatorName,
  ReadConsistency,
  RowSchema,
  SeqNum,
  SortedRunId,
  TransactionCommit,
  WriteBatch,
  WriteReceipt,
  SstId,
  Timestamp,
} from "@cf-slatedb/lsm-core";
import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema, OpenApi } from "effect/http-api";

import { CompactorState, SstDropReport } from "./compactor.ts";
import { PinnedSnapshot } from "./database-lifecycle.ts";
import { DbSnapshot } from "./db-client.ts";
import { DB_PUBLIC_ERRORS } from "./db-public-error.ts";
import { FlushedResponse, WriterStatusResponse } from "./writer-http-api.ts";

const DatabaseParams = Schema.Struct({
  databaseId: DatabaseId.annotate({ examples: [DatabaseId.make("orders-eu")] }),
});

/** Schema for creating a database. */
export const CreateDatabaseRequest = Schema.Struct({
  databaseId: DatabaseId.annotate({ examples: [DatabaseId.make("orders-eu")] }),
  /** Keys the database accepts; writes outside fail with `KeyOutOfRange`. Null: every key. */
  keyRange: Schema.OptionFromNullOr(KeyRange),
  /** Built-in merge operator for merge operands; null: merges are stored but cannot be read. */
  mergeOperator: Schema.OptionFromNullOr(MergeOperatorName),
  /** Changelog retention in milliseconds; null disables change data capture export. */
  changelogRetentionMs: Schema.OptionFromNullOr(Schema.Int.check(Schema.isGreaterThan(0))),
  /** Group for fair compaction and cache budgets; null: the database id. */
  budgetGroup: Schema.OptionFromNullOr(Schema.String),
});

/** Schema for a created database. */
export const CreateDatabaseResponse = Schema.Struct({
  databaseId: DatabaseId,
  manifestId: Schema.Int,
});

/** Schema for a point read: the key, how fresh the read must be, and an optional snapshot. */
export const GetRequest = Schema.Struct({
  key: Schema.Uint8ArrayFromBase64,
  consistency: ReadConsistency,
  snapshot: Schema.OptionFromNullOr(DbSnapshot),
});

/** Schema for a point read's result: the visible row (value, `seq`, timestamps), or null. */
export const GetResponse = Schema.Struct({ row: Schema.OptionFromNullOr(RowSchema) });

/**
 * Schema for a database's compaction state: the newest manifest's L0 SST count and sorted runs, and
 * its compactor's state (null when the compactor was never woken).
 */
export const CompactionStatusResponse = Schema.Struct({
  manifestId: ManifestId,
  l0Ssts: Schema.Int,
  /** The L0 SSTs' ids, newest first: what a corruption report or a drop names. */
  l0SstIds: Schema.Array(SstId),
  sortedRuns: Schema.Array(
    Schema.Struct({ id: SortedRunId, ssts: Schema.Int, sizeBytes: Schema.Int }),
  ),
  compactor: Schema.OptionFromNullOr(CompactorState),
  /**
   * SSTs a compaction found corrupted and the manifest still holds (D62), with each SST's creation
   * time: a checkpoint created before it can restore the data it held.
   */
  corruptSsts: Schema.Array(
    Schema.Struct({
      sstId: SstId,
      createdAt: Timestamp,
      section: Schema.String,
      reason: Schema.String,
      detectedAt: Timestamp,
    }),
  ),
  /** SSTs an operator dropped (D62), newest last. */
  droppedSsts: Schema.Array(DroppedSst),
});

/** Schema for an SST drop request (D62). */
export const DropSstRequest = Schema.Struct({
  /** Must repeat the SST id in the path: dropping loses the SST's rows. */
  confirm: Schema.String,
  /** Drop the SST even though no compaction reported it corrupted. */
  force: Schema.optionalKey(Schema.Boolean),
});

/** Schema for a database's checkpoints, oldest first (including forks' holds on it). */
export const CheckpointsResponse = Schema.Struct({ checkpoints: Schema.Array(Checkpoint) });

/** Schema for a checkpoint request: an optional name and lifetime (none: until deleted). */
export const CreateCheckpointRequest = Schema.Struct({
  name: Schema.OptionFromNullOr(Schema.String),
  lifetimeMs: Schema.OptionFromNullOr(Schema.Int.check(Schema.isGreaterThan(0))),
});

/** Schema for a pinned snapshot request: how long it stays readable, at most 30 days. */
export const PinSnapshotRequest = Schema.Struct({
  lifetimeMs: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 30 * 24 * 3_600_000 })),
});

/** Schema for a seal request: the databases that replace the sealed one. */
export const SealDatabaseRequest = Schema.Struct({ successors: Schema.Array(DatabaseId) });

/** Schema for a fork request: the new database, the source checkpoint, and the key range. */
export const ForkDatabaseRequest = Schema.Struct({
  forkId: DatabaseId,
  checkpointId: CheckpointId,
  range: KeyRange,
});

/**
 * Schema for the start of a transaction: its read sequence number and, when the database reports
 * one, the manifest its reads come from (`manifestId`, from `transactions/begin`; pass it to every
 * read so they stay exact across flushes, D58).
 */
export const TransactionBeginResponse = Schema.Struct({
  readSeq: SeqNum,
  manifestId: Schema.OptionFromOptionalKey(ManifestId),
});

/**
 * Schema for a transaction's read: a key at the transaction's read sequence number, from its begin
 * manifest when given. Without `manifestId` a flush past `readSeq` ends the read with
 * `SnapshotTooOld`.
 */
export const TransactionGetRequest = Schema.Struct({
  readSeq: SeqNum,
  manifestId: Schema.OptionFromOptionalKey(ManifestId),
  key: Schema.Uint8ArrayFromBase64,
});

/** Schema for a transaction's scan: a range at the transaction's read sequence number (see get). */
export const TransactionScanRequest = Schema.Struct({
  readSeq: SeqNum,
  manifestId: Schema.OptionFromOptionalKey(ManifestId),
  range: KeyRange,
  limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 })),
});

/** Schema for one page of the change feed (see `ChangeFeedPage`). */
export const ChangeFeedResponse = Schema.Struct({
  rows: Schema.Array(RowSchema),
  nextAfterSeq: SeqNum,
});

/** Schema for the change feed's query: changes after `afterSeq`, at most `limit` rows. */
export const ChangeFeedQuery = Schema.Struct({
  afterSeq: Schema.NumberFromString.pipe(Schema.decodeTo(SeqNum)),
  limit: Schema.NumberFromString.pipe(
    Schema.decodeTo(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 }))),
  ),
});

/** Largest page a scan returns. */
export const MAX_SCAN_PAGE_ROWS = 1000;

/** Schema for a scan: a key range, how fresh, a page size, and an optional snapshot. */
export const ScanRequest = Schema.Struct({
  range: KeyRange,
  consistency: ReadConsistency,
  snapshot: Schema.OptionFromNullOr(DbSnapshot),
  limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: MAX_SCAN_PAGE_ROWS })),
});

/**
 * Schema for one page of a scan: visible rows in key order, and where to resume (the next range
 * start), or null on the last page.
 */
export const ScanResponse = Schema.Struct({
  rows: Schema.Array(RowSchema),
  resumeFrom: Schema.OptionFromNullOr(Schema.Uint8ArrayFromBase64),
});

/** The public database API: create databases, write, flush, read, and follow changes. */
export class DbHttpApiGroup extends HttpApiGroup.make("databases")
  .add(
    HttpApiEndpoint.post("createDatabase", "/databases", {
      payload: CreateDatabaseRequest,
      success: CreateDatabaseResponse.pipe(HttpApiSchema.status(201)),
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Create a database",
        description:
          "Creates an empty database with the given id. The id names its writer, its storage prefix, and its cache tag; use it in every other operation.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("write", "/databases/:databaseId/batches", {
      params: DatabaseParams,
      payload: WriteBatch,
      success: WriteReceipt,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Write a batch",
        description:
          "Commits puts, deletes, and merges atomically with one sequence number. The write is durable when the response arrives. Pass the returned `seq` to an `AtLeast` read to read your own write.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("flush", "/databases/:databaseId/flush", {
      params: DatabaseParams,
      success: FlushedResponse,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Flush the memtable",
        description: "Flushes every committed write to an L0 SST now, so `Flushed` reads see it.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.get("status", "/databases/:databaseId/status", {
      params: DatabaseParams,
      success: WriterStatusResponse,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Read the writer's status",
        description:
          "The writer's epoch, its last and flushed sequence numbers, what is unflushed, the newest manifest's L0 SST count, and whether the database is sealed.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.get("compaction", "/databases/:databaseId/compaction", {
      params: DatabaseParams,
      success: CompactionStatusResponse,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Read the compaction state",
        description:
          "The newest manifest's L0 SST count and sorted runs, and the compactor's running jobs and epoch.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("dropSst", "/databases/:databaseId/ssts/:sstId/drop", {
      params: Schema.Struct({ databaseId: DatabaseId, sstId: SstId }),
      payload: DropSstRequest,
      success: SstDropReport,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Drop a corrupted SST (repair)",
        description:
          "Removes an SST that compaction reported corrupted from the manifest, so compaction resumes. Its rows are lost: older versions of its keys show again. `confirm` must repeat the SST id; `force` drops an SST no compaction reported. To lose nothing newer than a checkpoint instead, fork the full range from that checkpoint into a new database.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("createCheckpoint", "/databases/:databaseId/checkpoints", {
      params: DatabaseParams,
      payload: CreateCheckpointRequest,
      success: Checkpoint.pipe(HttpApiSchema.status(201)),
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Create a checkpoint",
        description:
          "Flushes every acknowledged write, then pins the resulting manifest: its data stays readable and forkable until the checkpoint is deleted or expires.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.get("listCheckpoints", "/databases/:databaseId/checkpoints", {
      params: DatabaseParams,
      success: CheckpointsResponse,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "List checkpoints",
        description:
          "Every checkpoint the newest manifest holds, including forks' holds, with the manifest each pins and when it expires.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.delete("deleteCheckpoint", "/databases/:databaseId/checkpoints/:checkpointId", {
      params: Schema.Struct({ databaseId: DatabaseId, checkpointId: CheckpointId }),
      success: HttpApiSchema.NoContent,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Delete a checkpoint",
        description:
          "Releases a checkpoint (its id comes from the create or list response), so GC may collect what only it kept. Deleting one that does not exist succeeds.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("seal", "/databases/:databaseId/seal", {
      params: DatabaseParams,
      payload: SealDatabaseRequest,
      success: Schema.Struct({ flushedSeq: SeqNum }),
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Seal a database",
        description:
          "Flushes and seals: every acknowledged write is in the sealed manifest, and later writes fail with DatabaseSealed naming the successors.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("fork", "/databases/:databaseId/forks", {
      params: DatabaseParams,
      payload: ForkDatabaseRequest,
      success: CreateDatabaseResponse.pipe(HttpApiSchema.status(201)),
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Fork a key range",
        description:
          "Creates a new database holding the range's data at a checkpoint, without copying: it reads the source's files until its compaction rewrites them.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.delete("removeDatabase", "/databases/:databaseId", {
      params: DatabaseParams,
      success: HttpApiSchema.NoContent,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Remove a database",
        description:
          "Deletes the database and all its data. Fails with DatabaseHasDependents while forks still read it.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("beginTransaction", "/databases/:databaseId/transactions/begin", {
      params: DatabaseParams,
      success: TransactionBeginResponse,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Begin a transaction",
        description:
          "Returns the read sequence number: read with transactions/get and transactions/scan at it, then send the writes, the keys read, and the ranges scanned to transactions/commit. Nothing is held on the server.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("transactionGet", "/databases/:databaseId/transactions/get", {
      params: DatabaseParams,
      payload: TransactionGetRequest,
      success: GetResponse,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Read a key in a transaction",
        description:
          "Reads a key as of the transaction's `readSeq` and `manifestId` (both from `transactions/begin`). Send the keys read to `transactions/commit` as `readKeys`.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("transactionScan", "/databases/:databaseId/transactions/scan", {
      params: DatabaseParams,
      payload: TransactionScanRequest,
      success: Schema.Struct({ rows: Schema.Array(RowSchema) }),
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Scan a range in a transaction",
        description:
          "Scans a key range as of the transaction's `readSeq` and `manifestId` (both from `transactions/begin`), at most `limit` rows. Send the range to `transactions/commit` as one of `readRanges`.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("commitTransaction", "/databases/:databaseId/transactions/commit", {
      params: DatabaseParams,
      payload: TransactionCommit,
      success: WriteReceipt,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Commit a transaction",
        description:
          "Commits the writes atomically unless another commit since readSeq wrote one of its keys (Snapshot) or one of the keys it read or ranges it scanned (SerializableSnapshot). TransactionConflict is retryable: begin again.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("get", "/databases/:databaseId/get", {
      params: DatabaseParams,
      payload: GetRequest,
      success: GetResponse,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Read a key",
        description:
          "Returns the key's visible row: `Latest` sees every acknowledged write, `AtLeast` sees at least the write with that `seq` (from a write receipt), `Flushed` sees flushed data without calling the writer. A `snapshot` from the snapshot operation reads a fixed manifest.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("scan", "/databases/:databaseId/scan", {
      params: DatabaseParams,
      payload: ScanRequest,
      success: ScanResponse,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Scan a key range",
        description:
          "Returns up to `limit` visible rows in the half-open range, in key order. To continue, send the same request with `range.start` set to `resumeFrom`.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("pinSnapshot", "/databases/:databaseId/snapshots/pinned", {
      params: DatabaseParams,
      payload: PinSnapshotRequest,
      success: PinnedSnapshot.pipe(HttpApiSchema.status(201)),
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Take a pinned snapshot",
        description:
          "Flushes every acknowledged write, then pins the resulting manifest with a checkpoint for lifetimeMs: pass `snapshot` to reads to see exactly that data until `expireAt`. Delete the checkpoint (`checkpointId`) to release it early. Each pinned snapshot is a manifest write; an unpinned snapshot (POST …/snapshots) costs none but is kept only about an hour.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("snapshot", "/databases/:databaseId/snapshots", {
      params: DatabaseParams,
      success: DbSnapshot,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Take a snapshot",
        description:
          "Pins the current flushed manifest without writing anything; pass the snapshot to reads to see exactly that data. It stays readable for GC's minimum age (an hour by default); reads after that may fail with SnapshotExpired. Take a pinned snapshot (POST …/snapshots/pinned) to read for longer.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.get("changes", "/databases/:databaseId/changes", {
      params: DatabaseParams,
      query: ChangeFeedQuery,
      success: ChangeFeedResponse,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Read the change feed",
        description:
          "Returns committed changes after `afterSeq` in sequence order (flushed ones from the changelog, then unflushed ones), never splitting a batch. Continue from `nextAfterSeq`. Requires the database's changelog; fails with ChangelogUnavailable when it is disabled or the changes were dropped by retention.",
      }),
    ),
  ) {}

/** The public database API, versioned under `/v1`. */
export class DbHttpApi extends HttpApi.make("DbHttpApi")
  .add(DbHttpApiGroup)
  .prefix("/v1")
  .annotateMerge(
    OpenApi.annotations({
      title: "cf-slatedb",
      version: "1.0.0",
      description:
        "A SlateDB-style LSM key-value store on R2 and Durable Objects. Keys and values are base64 in JSON. Every response carries `x-request-id`; error bodies repeat it as `requestId`.",
    }),
  ) {}
