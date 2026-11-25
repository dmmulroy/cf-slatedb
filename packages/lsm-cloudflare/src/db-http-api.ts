import {
  DatabaseId,
  KeyRange,
  MergeOperatorName,
  WriteBatch,
  WriteReceipt,
} from "@cf-slatedb/lsm-core";
import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema, OpenApi } from "effect/http-api";

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
