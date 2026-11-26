import {
  CheckpointId,
  DatabaseId,
  ManifestId,
  DatabaseSealedError,
  KeyOutOfRangeError,
  KeyRange,
  MAX_BATCH_OPERATIONS,
  RowSchema,
  TransactionCommit,
  TransactionConflictError,
  SeqNum,
  WriteBatch,
  WriteBatchInvalidError,
  WriteReceipt,
  WriterEpoch,
  WriterFencedError,
} from "@cf-slatedb/lsm-core";
import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "effect/http-api";

import {
  DatabaseIdMismatchError,
  DatabaseNotFoundError,
  WriterUnavailableError,
  WriteStalledError,
} from "./lsm-errors.ts";

const DatabaseParams = Schema.Struct({ databaseId: DatabaseId });

const MemTableCursorSchema = Schema.Struct({ key: Schema.Uint8ArrayFromBase64, seq: SeqNum });

/** Schema for a `Latest` probe request: the key to look up. */
export const ProbeRequest = Schema.Struct({ key: Schema.Uint8ArrayFromBase64 });

/** Schema for a `Latest` probe response (see `MemTableProbe`). */
export const ProbeResponse = Schema.Struct({
  versions: Schema.Array(RowSchema),
  snapshotSeq: SeqNum,
  flushedSeq: SeqNum,
});

/** Schema for a transaction's probe of one key at its read sequence number. */
export const ProbeAtRequest = Schema.Struct({ key: Schema.Uint8ArrayFromBase64, readSeq: SeqNum });

/** Schema for a transaction's probe response (see `MemTableProbeAt`). */
export const ProbeAtResponse = Schema.Struct({
  versions: Schema.Array(RowSchema),
  retainedFromSeq: SeqNum,
});

/**
 * Schema for a memtable scan request: one page of rows in `range`, unflushed only or, with
 * `includeRetained` (a transaction's scan), every row still in SQLite.
 */
export const MemTableScanRequest = Schema.Struct({
  range: KeyRange,
  snapshotSeq: Schema.OptionFromNullOr(SeqNum),
  cursor: Schema.OptionFromNullOr(MemTableCursorSchema),
  limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 })),
  includeRetained: Schema.Boolean,
});

/** Schema for one page of a memtable scan (see `MemTableScanPage`). */
export const MemTableScanResponse = Schema.Struct({
  rows: Schema.Array(RowSchema),
  snapshotSeq: SeqNum,
  flushedSeq: SeqNum,
  retainedFromSeq: SeqNum,
  nextCursor: Schema.OptionFromNullOr(MemTableCursorSchema),
});

/** Schema for the flush and seal responses: the sequence number now durable in L0. */
export const FlushedResponse = Schema.Struct({ flushedSeq: SeqNum });

/** Schema for the change-feed query: rows after `afterSeq`, at most `limit`. */
export const ChangesQuery = Schema.Struct({
  afterSeq: Schema.NumberFromString.pipe(Schema.decodeTo(SeqNum)),
  // Up to one whole write batch, so the change feed can return any batch in one page.
  limit: Schema.NumberFromString.pipe(
    Schema.decodeTo(
      Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: MAX_BATCH_OPERATIONS })),
    ),
  ),
});

/** Schema for unflushed changes in sequence order (see `ChangesPage`). */
export const ChangesResponse = Schema.Struct({
  rows: Schema.Array(RowSchema),
  lastSeq: SeqNum,
  flushedSeq: SeqNum,
});

/**
 * Schema for a seal request: the databases that replace this one, and whether the seal takes each a
 * hold on the sealed manifest (a split's seal, D57).
 */
export const SealRequest = Schema.Struct({
  successors: Schema.Array(DatabaseId),
  holdForSuccessors: Schema.Boolean,
});

/** Schema for a seal response (see `SealReceipt`). */
export const SealResponse = Schema.Struct({
  flushedSeq: SeqNum,
  successorHolds: Schema.Array(
    Schema.Struct({ successor: DatabaseId, checkpointId: CheckpointId }),
  ),
});

/** Schema for the writer's status (see `WriterStatus`). */
export const WriterStatusResponse = Schema.Struct({
  databaseId: DatabaseId,
  writerEpoch: WriterEpoch,
  lastSeq: SeqNum,
  flushedSeq: SeqNum,
  retainedFromSeq: SeqNum,
  flushedManifestId: Schema.OptionFromNullOr(ManifestId),
  unflushedBytes: Schema.Number,
  unflushedRows: Schema.Number,
  l0Count: Schema.Number,
  sealed: Schema.Boolean,
});

/**
 * Every error a writer endpoint can return, with the HTTP status that carries it: `400` for a
 * correctable request, `404` for an unknown database, `409` for a sealed database or an out-of-range
 * key, `503` for retryable conditions (a fenced instance, backpressure, unavailable storage).
 */
const WRITER_ERRORS = [
  WriteBatchInvalidError.pipe(HttpApiSchema.status(400)),
  DatabaseIdMismatchError.pipe(HttpApiSchema.status(400)),
  DatabaseNotFoundError.pipe(HttpApiSchema.status(404)),
  KeyOutOfRangeError.pipe(HttpApiSchema.status(409)),
  DatabaseSealedError.pipe(HttpApiSchema.status(409)),
  TransactionConflictError.pipe(HttpApiSchema.status(409)),
  WriterFencedError.pipe(HttpApiSchema.status(503)),
  WriteStalledError.pipe(HttpApiSchema.status(503)),
  WriterUnavailableError.pipe(HttpApiSchema.status(503)),
];

/** Endpoints of a database's writer Durable Object (`WriterServer`). */
export class WriterHttpApiGroup extends HttpApiGroup.make("writer")
  .add(
    HttpApiEndpoint.post("write", "/databases/:databaseId/batches", {
      params: DatabaseParams,
      payload: WriteBatch,
      success: WriteReceipt,
      error: WRITER_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("commitTransaction", "/databases/:databaseId/transactions", {
      params: DatabaseParams,
      payload: TransactionCommit,
      success: WriteReceipt,
      error: WRITER_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("probe", "/databases/:databaseId/probe", {
      params: DatabaseParams,
      payload: ProbeRequest,
      success: ProbeResponse,
      error: WRITER_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("probeAt", "/databases/:databaseId/probe-at", {
      params: DatabaseParams,
      payload: ProbeAtRequest,
      success: ProbeAtResponse,
      error: WRITER_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("scan", "/databases/:databaseId/scan", {
      params: DatabaseParams,
      payload: MemTableScanRequest,
      success: MemTableScanResponse,
      error: WRITER_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("flush", "/databases/:databaseId/flush", {
      params: DatabaseParams,
      success: FlushedResponse,
      error: WRITER_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.get("changes", "/databases/:databaseId/changes", {
      params: DatabaseParams,
      query: ChangesQuery,
      success: ChangesResponse,
      error: WRITER_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.get("status", "/databases/:databaseId/status", {
      params: DatabaseParams,
      success: WriterStatusResponse,
      error: WRITER_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("destroy", "/databases/:databaseId/destroy", {
      params: DatabaseParams,
      success: HttpApiSchema.NoContent,
      error: WRITER_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("seal", "/databases/:databaseId/seal", {
      params: DatabaseParams,
      payload: SealRequest,
      success: SealResponse,
      error: WRITER_ERRORS,
    }),
  ) {}

/** The writer Durable Object's HTTP API, versioned under `/v1`. */
export class WriterHttpApi extends HttpApi.make("WriterHttpApi")
  .add(WriterHttpApiGroup)
  .prefix("/v1") {}
