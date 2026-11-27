import type {
  CheckpointLimitError,
  TransactionConflictError,
  DatabaseHasDependentsError,
  DatabaseSealedError,
  KeyOutOfRangeError,
  MergedValueTooLargeError,
  MergeOperatorMissingError,
  SstCorruptedError,
  SstNotInManifestError,
  WriteBatchInvalidError,
  WriterFencedError,
} from "@shaledb/lsm-core";
import { Option } from "effect";

import {
  BatchInvalidPublicError,
  ChangelogUnavailablePublicError,
  CheckpointLimitReachedPublicError,
  CheckpointNotFoundPublicError,
  DatabaseAlreadyExistsPublicError,
  DatabaseRecentlyRemovedPublicError,
  DatabaseHasDependentsPublicError,
  type DbApiOperation,
  DatabaseNotFoundPublicError,
  type DbPublicError,
  DatabaseSealedPublicError,
  InternalFailurePublicError,
  KeyOutOfRangePublicError,
  MergedValueTooLargePublicError,
  MergeOperatorMissingPublicError,
  SnapshotExpiredPublicError,
  StorageUnavailablePublicError,
  StoredDataInvalidPublicError,
  TransactionConflictPublicError,
  SstDropRefusedPublicError,
  SstNotFoundPublicError,
  WriteStalledPublicError,
} from "./db-public-error.ts";
import type {
  DatabaseIdMismatchError,
  DatabaseNotFoundError,
  WriterUnavailableError,
  WriteStalledError,
} from "./lsm-errors.ts";
import type { CompactorUnavailableError, SstDropRefusedError } from "./compactor.ts";
import type {
  CheckpointNotFoundError,
  DatabaseRecentlyRemovedError,
} from "./database-lifecycle.ts";
import type { ChangelogUnavailableError, ReaderUnavailableError } from "./db-reader.ts";
import type { DatabaseAlreadyExistsError } from "./manifest-store.ts";

/** Every internal failure the public API classifies. */
export type DbInternalError =
  | DatabaseNotFoundError
  | DatabaseIdMismatchError
  | DatabaseAlreadyExistsError
  | DatabaseRecentlyRemovedError
  | DatabaseSealedError
  | WriterFencedError
  | WriteStalledError
  | WriteBatchInvalidError
  | KeyOutOfRangeError
  | WriterUnavailableError
  | MergeOperatorMissingError
  | MergedValueTooLargeError
  | SstCorruptedError
  | ReaderUnavailableError
  | CompactorUnavailableError
  | DatabaseHasDependentsError
  | CheckpointNotFoundError
  | CheckpointLimitError
  | TransactionConflictError
  | ChangelogUnavailableError
  | SstNotInManifestError
  | SstDropRefusedError;

/**
 * Classifies an internal failure as its public error: a stable code, a caller-facing message, the
 * request id, and whether retrying may succeed. The switch is exhaustive, so a new internal error is a
 * type error until it is classified here.
 */
export const toPublicError =
  (operation: DbApiOperation, requestId: string) =>
  (error: DbInternalError): DbPublicError => {
    switch (error._tag) {
      case "DatabaseNotFoundError":
        return new DatabaseNotFoundPublicError({
          code: "DatabaseNotFound",
          operation,
          message:
            operation === "remove"
              ? `Database ${error.databaseId} does not exist: it was never created, or it is already removed (a retried remove whose first attempt completed answers this way). Nothing is left to remove.`
              : `Database ${error.databaseId} does not exist. Create it first, or check the id.`,
          details: { databaseId: error.databaseId },
          requestId,
          retryable: false,
        });
      case "DatabaseAlreadyExistsError":
        return new DatabaseAlreadyExistsPublicError({
          code: "DatabaseAlreadyExists",
          operation,
          message: `Database ${error.databaseId} already exists. Use it, or choose another id. A retried create whose first attempt completed answers this way too; the database is then ready.`,
          details: { databaseId: error.databaseId },
          requestId,
          retryable: false,
        });
      case "DatabaseSealedError":
        return new DatabaseSealedPublicError({
          code: "DatabaseSealed",
          operation,
          message: error.message,
          details: { databaseId: error.databaseId, successors: error.successors },
          requestId,
          retryable: false,
        });
      case "KeyOutOfRangeError":
        return new KeyOutOfRangePublicError({
          code: "KeyOutOfRange",
          operation,
          message: error.message,
          details: { databaseId: error.databaseId, operationIndex: error.operationIndex },
          requestId,
          retryable: false,
        });
      case "WriteBatchInvalidError":
        return new BatchInvalidPublicError({
          code: "BatchInvalid",
          operation,
          message: error.message,
          details: { reason: error.reason, operationIndex: Option.getOrNull(error.operationIndex) },
          requestId,
          retryable: false,
        });
      case "DatabaseRecentlyRemovedError":
        return new DatabaseRecentlyRemovedPublicError({
          code: "DatabaseRecentlyRemoved",
          operation,
          message: error.message,
          details: {
            databaseId: error.databaseId,
            retryAfterMs: Math.max(0, Math.ceil(error.retryAfterMs)),
          },
          requestId,
          retryable: true,
        });
      case "WriteStalledError":
        return new WriteStalledPublicError({
          code: "WriteStalled",
          operation,
          message: error.message,
          details: {
            databaseId: error.databaseId,
            reason: error.reason,
            retryAfterMs: error.retryAfterMs,
          },
          requestId,
          retryable: true,
        });
      case "MergedValueTooLargeError":
        return new MergedValueTooLargePublicError({
          code: "MergedValueTooLarge",
          operation,
          message: `A key's merge operands resolve to ${error.bytes} bytes, more than the ${error.limit} a read returns. Replace the key with a put (or delete it), then merge onto that.`,
          details: { bytes: error.bytes, limit: error.limit },
          requestId,
          retryable: false,
        });
      case "MergeOperatorMissingError":
        return new MergeOperatorMissingPublicError({
          code: "MergeOperatorMissing",
          operation,
          message: error.message,
          details: {},
          requestId,
          retryable: false,
        });
      case "WriterFencedError":
        // The client already retried against the current writer; the next attempt may succeed.
        return new StorageUnavailablePublicError({
          code: "StorageUnavailable",
          operation,
          message: `Database ${error.databaseId}'s writer was changing hands. Retry the same request.`,
          details: { databaseId: error.databaseId },
          requestId,
          retryable: true,
        });
      case "WriterUnavailableError":
        return error.reason === "StoredDataInvalid"
          ? new StoredDataInvalidPublicError({
              code: "StoredDataInvalid",
              operation,
              message: `Database ${error.databaseId}'s stored data failed validation. Contact the operator with request id ${requestId}.`,
              details: { databaseId: error.databaseId },
              requestId,
              retryable: false,
            })
          : new StorageUnavailablePublicError({
              code: "StorageUnavailable",
              operation,
              // Unreachable means no answer came back: a write may have committed (D88).
              message:
                error.reason === "WriterUnreachable" &&
                (operation === "write" || operation === "transaction")
                  ? `Database ${error.databaseId}'s writer could not be reached, so the write may or may not have committed. Retrying is safe for puts and deletes; a batch with merges may then apply twice, so read its keys first.`
                  : `Database ${error.databaseId}'s storage was temporarily unavailable. Retry the same request.`,
              details: { databaseId: error.databaseId },
              requestId,
              retryable: true,
            });
      case "ChangelogUnavailableError":
        return new ChangelogUnavailablePublicError({
          code: "ChangelogUnavailable",
          operation,
          message: error.message,
          details: { databaseId: error.databaseId, reason: error.reason },
          requestId,
          retryable: false,
        });
      case "TransactionConflictError":
        return new TransactionConflictPublicError({
          code: "TransactionConflict",
          operation,
          // A commit retried after a timeout conflicts with its own first attempt, which may have
          // committed (D88): say so, so the caller does not apply the transaction twice.
          message:
            error.reason === "WriteConflict"
              ? `${error.message} If an earlier attempt of this same commit got no answer, it may have committed: read its keys before running the transaction again.`
              : error.message,
          details: { databaseId: error.databaseId, reason: error.reason },
          requestId,
          retryable: true,
        });
      case "DatabaseHasDependentsError":
        return new DatabaseHasDependentsPublicError({
          code: "DatabaseHasDependents",
          operation,
          message: `Database ${error.databaseId} cannot be removed while forks ${error.dependents.join(", ")} still read its data. Remove them first, or retry after their compactions release it.`,
          details: { databaseId: error.databaseId, dependents: error.dependents },
          requestId,
          retryable: false,
        });
      case "CheckpointNotFoundError":
        return new CheckpointNotFoundPublicError({
          code: "CheckpointNotFound",
          operation,
          message: `Checkpoint ${error.checkpointId} of database ${error.databaseId} does not exist (deleted or expired). Create a new checkpoint.`,
          details: { databaseId: error.databaseId, checkpointId: error.checkpointId },
          requestId,
          retryable: false,
        });
      case "CheckpointLimitError":
        return new CheckpointLimitReachedPublicError({
          code: "CheckpointLimitReached",
          operation,
          message: `Database ${error.databaseId} already has ${error.checkpoints} checkpoints and pinned snapshots, the most it may hold (${error.limit}). Delete one, or wait for a pinned snapshot to expire, then retry.`,
          details: {
            databaseId: error.databaseId,
            checkpoints: error.checkpoints,
            limit: error.limit,
          },
          requestId,
          retryable: false,
        });
      case "SstNotInManifestError":
        return new SstNotFoundPublicError({
          code: "SstNotFound",
          operation,
          message: error.message,
          details: { databaseId: error.databaseId, sstId: error.sstId },
          requestId,
          retryable: false,
        });
      case "SstDropRefusedError":
        return new SstDropRefusedPublicError({
          code: "SstDropRefused",
          operation,
          message: error.message,
          details: { databaseId: error.databaseId, sstId: error.sstId, reason: error.reason },
          requestId,
          retryable: error.reason === "CompactionRunning",
        });
      case "CompactorUnavailableError":
        return new StorageUnavailablePublicError({
          code: "StorageUnavailable",
          operation,
          message: `Database ${error.databaseId}'s compactor could not be reached. Retry the same request.`,
          details: { databaseId: error.databaseId },
          requestId,
          retryable: true,
        });
      case "ReaderUnavailableError":
        switch (error.reason) {
          case "SnapshotExpired":
            return new SnapshotExpiredPublicError({
              code: "SnapshotExpired",
              operation,
              message: error.message,
              details: { databaseId: error.databaseId },
              requestId,
              retryable: false,
            });
          case "StoredManifestInvalid":
            return new StoredDataInvalidPublicError({
              code: "StoredDataInvalid",
              operation,
              message: `Database ${error.databaseId}'s stored manifest failed validation. Contact the operator with request id ${requestId}.`,
              details: { databaseId: error.databaseId },
              requestId,
              retryable: false,
            });
          case "StorageUnavailable":
            return new StorageUnavailablePublicError({
              code: "StorageUnavailable",
              operation,
              message: `Database ${error.databaseId}'s data could not be read just now. Retry the same request.`,
              details: { databaseId: error.databaseId },
              requestId,
              retryable: true,
            });
        }
      // falls through: the inner switch is exhaustive
      case "SstCorruptedError":
      case "DatabaseIdMismatchError":
        return new InternalFailurePublicError({
          code: "InternalFailure",
          operation,
          message: `The request could not be completed because of an internal error. Contact the operator with request id ${requestId}.`,
          details: {},
          requestId,
          retryable: false,
        });
    }
  };
