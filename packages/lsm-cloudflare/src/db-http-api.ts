import {
  DatabaseId,
  KeyRange,
  MergeOperatorName,
  ReadConsistency,
  RowSchema,
  WriteBatch,
  WriteReceipt,
} from "@cf-slatedb/lsm-core";
import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema, OpenApi } from "effect/http-api";

import { DbSnapshot } from "./db-client.ts";
import { DB_PUBLIC_ERRORS } from "./db-public-error.ts";
import {
  ChangesQuery,
  ChangesResponse,
  FlushedResponse,
  WriterStatusResponse,
} from "./writer-http-api.ts";

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
    }).annotateMerge(OpenApi.annotations({ summary: "Read the writer's status" })),
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
    HttpApiEndpoint.post("snapshot", "/databases/:databaseId/snapshots", {
      params: DatabaseParams,
      success: DbSnapshot,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Take a snapshot",
        description:
          "Pins the current flushed manifest; pass the snapshot to reads to see exactly that data.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.get("changes", "/databases/:databaseId/changes", {
      params: DatabaseParams,
      query: ChangesQuery,
      success: ChangesResponse,
      error: DB_PUBLIC_ERRORS,
    }).annotateMerge(
      OpenApi.annotations({
        summary: "Read unflushed changes",
        description:
          "Returns committed but not yet flushed changes after `afterSeq`, in sequence order. Older changes are in the manifest's changelog SSTs.",
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
