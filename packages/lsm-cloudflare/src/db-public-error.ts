import { CheckpointId, DatabaseId, SstId } from "@shaledb/lsm-core";
import { Schema } from "effect";
import { HttpApiSchema } from "effect/http-api";

/** Schema for the name of a public database API operation, as reported in its errors. */
export const DbApiOperation = Schema.Literals([
  "createDatabase",
  "write",
  "flush",
  "status",
  "changes",
  "compaction",
  "get",
  "scan",
  "seal",
  "fork",
  "remove",
  "checkpoint",
  "transaction",
  "routeRequest",
  "dropSst",
]);

/** A public database API operation name. */
export type DbApiOperation = typeof DbApiOperation.Type;

const common = {
  /** API operation that failed. */
  operation: DbApiOperation,
  /** What failed and what to do next. */
  message: Schema.String,
  /** Identifier of the failed request, for finding its traces and logs. */
  requestId: Schema.String,
  /** Whether sending the same request again may succeed. */
  retryable: Schema.Boolean,
};

/** `400` — the request is malformed (path, query, or body). */
export class InvalidRequestPublicError extends Schema.TaggedError<InvalidRequestPublicError>()(
  "InvalidRequestPublicError",
  {
    code: Schema.Literal("InvalidRequest"),
    details: Schema.Struct({ method: Schema.String, path: Schema.String }),
    ...common,
  },
) {}

/** `400` — a write batch is invalid; `details.reason` names why. */
export class BatchInvalidPublicError extends Schema.TaggedError<BatchInvalidPublicError>()(
  "BatchInvalidPublicError",
  {
    code: Schema.Literal("BatchInvalid"),
    details: Schema.Struct({ reason: Schema.String, operationIndex: Schema.NullOr(Schema.Int) }),
    ...common,
  },
) {}

/** `404` — no database has this id. */
export class DatabaseNotFoundPublicError extends Schema.TaggedError<DatabaseNotFoundPublicError>()(
  "DatabaseNotFoundPublicError",
  {
    code: Schema.Literal("DatabaseNotFound"),
    details: Schema.Struct({ databaseId: Schema.String }),
    ...common,
  },
) {}

/** `404` — no route matches the method and path. */
export class RouteNotFoundPublicError extends Schema.TaggedError<RouteNotFoundPublicError>()(
  "RouteNotFoundPublicError",
  {
    code: Schema.Literal("RouteNotFound"),
    details: Schema.Struct({ method: Schema.String, path: Schema.String }),
    ...common,
  },
) {}

/** `409` — a database with this id already exists. */
export class DatabaseAlreadyExistsPublicError extends Schema.TaggedError<DatabaseAlreadyExistsPublicError>()(
  "DatabaseAlreadyExistsPublicError",
  {
    code: Schema.Literal("DatabaseAlreadyExists"),
    details: Schema.Struct({ databaseId: DatabaseId }),
    ...common,
  },
) {}

/** `409` — a key is outside the database's key range; route it to the database that holds it. */
export class KeyOutOfRangePublicError extends Schema.TaggedError<KeyOutOfRangePublicError>()(
  "KeyOutOfRangePublicError",
  {
    code: Schema.Literal("KeyOutOfRange"),
    details: Schema.Struct({ databaseId: DatabaseId, operationIndex: Schema.Int }),
    ...common,
  },
) {}

/** `409` — the database is sealed; send writes to a successor. */
export class DatabaseSealedPublicError extends Schema.TaggedError<DatabaseSealedPublicError>()(
  "DatabaseSealedPublicError",
  {
    code: Schema.Literal("DatabaseSealed"),
    details: Schema.Struct({ databaseId: DatabaseId, successors: Schema.Array(DatabaseId) }),
    ...common,
  },
) {}

/** `409` — other databases (forks) still depend on this one, so it cannot be removed. */
export class DatabaseHasDependentsPublicError extends Schema.TaggedError<DatabaseHasDependentsPublicError>()(
  "DatabaseHasDependentsPublicError",
  {
    code: Schema.Literal("DatabaseHasDependents"),
    details: Schema.Struct({ databaseId: DatabaseId, dependents: Schema.Array(DatabaseId) }),
    ...common,
  },
) {}

/** `404` — the checkpoint does not exist (deleted or expired), or its manifest was collected. */
export class CheckpointNotFoundPublicError extends Schema.TaggedError<CheckpointNotFoundPublicError>()(
  "CheckpointNotFoundPublicError",
  {
    code: Schema.Literal("CheckpointNotFound"),
    details: Schema.Struct({ databaseId: DatabaseId, checkpointId: CheckpointId }),
    ...common,
  },
) {}

/** `404` — the SST is not in the database's manifest (wrong id, or already replaced). */
export class SstNotFoundPublicError extends Schema.TaggedError<SstNotFoundPublicError>()(
  "SstNotFoundPublicError",
  {
    code: Schema.Literal("SstNotFound"),
    details: Schema.Struct({ databaseId: DatabaseId, sstId: SstId }),
    ...common,
  },
) {}

/**
 * `409` — an SST drop was refused and nothing changed: no compaction reported the SST corrupted
 * (send `force` to drop it anyway), `confirm` did not repeat the SST id, or a running compaction
 * reads it (retry once it finishes).
 */
export class SstDropRefusedPublicError extends Schema.TaggedError<SstDropRefusedPublicError>()(
  "SstDropRefusedPublicError",
  {
    code: Schema.Literal("SstDropRefused"),
    details: Schema.Struct({
      databaseId: DatabaseId,
      sstId: SstId,
      reason: Schema.Literals([
        "NotReportedCorrupted",
        "CompactionRunning",
        "ConfirmationMismatch",
      ]),
    }),
    ...common,
  },
) {}

/** `409` — the change feed is disabled, or the requested changes were dropped by retention. */
export class ChangelogUnavailablePublicError extends Schema.TaggedError<ChangelogUnavailablePublicError>()(
  "ChangelogUnavailablePublicError",
  {
    code: Schema.Literal("ChangelogUnavailable"),
    details: Schema.Struct({
      databaseId: DatabaseId,
      reason: Schema.Literals(["Disabled", "Expired"]),
    }),
    ...common,
  },
) {}

/** `409` — a merge operand cannot be resolved because the database has no merge operator. */
export class MergeOperatorMissingPublicError extends Schema.TaggedError<MergeOperatorMissingPublicError>()(
  "MergeOperatorMissingPublicError",
  {
    code: Schema.Literal("MergeOperatorMissing"),
    details: Schema.Struct({}),
    ...common,
  },
) {}

/** `409` — a transaction conflicted with a concurrent write; retry the whole transaction. */
export class TransactionConflictPublicError extends Schema.TaggedError<TransactionConflictPublicError>()(
  "TransactionConflictPublicError",
  {
    code: Schema.Literal("TransactionConflict"),
    details: Schema.Struct({
      databaseId: DatabaseId,
      reason: Schema.Literals(["WriteConflict", "ReadConflict", "SnapshotTooOld"]),
    }),
    ...common,
  },
) {}

/** `409` — a snapshot's manifest was collected; take a new snapshot. */
export class SnapshotExpiredPublicError extends Schema.TaggedError<SnapshotExpiredPublicError>()(
  "SnapshotExpiredPublicError",
  {
    code: Schema.Literal("SnapshotExpired"),
    details: Schema.Struct({ databaseId: DatabaseId }),
    ...common,
  },
) {}

/** `503` — the writer refused the write (backpressure); retry after `retryAfterMs`. */
export class WriteStalledPublicError extends Schema.TaggedError<WriteStalledPublicError>()(
  "WriteStalledPublicError",
  {
    code: Schema.Literal("WriteStalled"),
    details: Schema.Struct({
      databaseId: DatabaseId,
      reason: Schema.String,
      retryAfterMs: Schema.Int,
    }),
    ...common,
  },
) {}

/** `503` — storage was temporarily unavailable; retry the same request. */
export class StorageUnavailablePublicError extends Schema.TaggedError<StorageUnavailablePublicError>()(
  "StorageUnavailablePublicError",
  {
    code: Schema.Literal("StorageUnavailable"),
    details: Schema.Struct({ databaseId: DatabaseId }),
    ...common,
  },
) {}

/** `500` — stored data failed validation; contact the operator with the request id. */
export class StoredDataInvalidPublicError extends Schema.TaggedError<StoredDataInvalidPublicError>()(
  "StoredDataInvalidPublicError",
  {
    code: Schema.Literal("StoredDataInvalid"),
    details: Schema.Struct({ databaseId: DatabaseId }),
    ...common,
  },
) {}

/**
 * `401` — the request did not carry the stage's access token (live preview stages only, D44). The
 * access gate answers it before the request reaches any handler.
 */
export class UnauthenticatedPublicError extends Schema.TaggedError<UnauthenticatedPublicError>()(
  "UnauthenticatedPublicError",
  { code: Schema.Literal("Unauthenticated"), details: Schema.Struct({}), ...common },
) {}

/** `500` — an internal failure; contact the operator with the request id. */
export class InternalFailurePublicError extends Schema.TaggedError<InternalFailurePublicError>()(
  "InternalFailurePublicError",
  { code: Schema.Literal("InternalFailure"), details: Schema.Struct({}), ...common },
) {}

/** Every public error, with the HTTP status that carries it. */
export const DB_PUBLIC_ERRORS = [
  InvalidRequestPublicError.pipe(HttpApiSchema.status(400)),
  BatchInvalidPublicError.pipe(HttpApiSchema.status(400)),
  UnauthenticatedPublicError.pipe(HttpApiSchema.status(401)),
  DatabaseNotFoundPublicError.pipe(HttpApiSchema.status(404)),
  RouteNotFoundPublicError.pipe(HttpApiSchema.status(404)),
  CheckpointNotFoundPublicError.pipe(HttpApiSchema.status(404)),
  SstNotFoundPublicError.pipe(HttpApiSchema.status(404)),
  DatabaseAlreadyExistsPublicError.pipe(HttpApiSchema.status(409)),
  KeyOutOfRangePublicError.pipe(HttpApiSchema.status(409)),
  DatabaseSealedPublicError.pipe(HttpApiSchema.status(409)),
  DatabaseHasDependentsPublicError.pipe(HttpApiSchema.status(409)),
  MergeOperatorMissingPublicError.pipe(HttpApiSchema.status(409)),
  TransactionConflictPublicError.pipe(HttpApiSchema.status(409)),
  SnapshotExpiredPublicError.pipe(HttpApiSchema.status(409)),
  ChangelogUnavailablePublicError.pipe(HttpApiSchema.status(409)),
  SstDropRefusedPublicError.pipe(HttpApiSchema.status(409)),
  WriteStalledPublicError.pipe(HttpApiSchema.status(503)),
  StorageUnavailablePublicError.pipe(HttpApiSchema.status(503)),
  StoredDataInvalidPublicError.pipe(HttpApiSchema.status(500)),
  InternalFailurePublicError.pipe(HttpApiSchema.status(500)),
];

/** Any public error. */
export type DbPublicError =
  | InvalidRequestPublicError
  | UnauthenticatedPublicError
  | BatchInvalidPublicError
  | DatabaseNotFoundPublicError
  | CheckpointNotFoundPublicError
  | RouteNotFoundPublicError
  | DatabaseAlreadyExistsPublicError
  | KeyOutOfRangePublicError
  | DatabaseSealedPublicError
  | DatabaseHasDependentsPublicError
  | MergeOperatorMissingPublicError
  | TransactionConflictPublicError
  | SnapshotExpiredPublicError
  | ChangelogUnavailablePublicError
  | SstNotFoundPublicError
  | SstDropRefusedPublicError
  | WriteStalledPublicError
  | StorageUnavailablePublicError
  | StoredDataInvalidPublicError
  | InternalFailurePublicError;
