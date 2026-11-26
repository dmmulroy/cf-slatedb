import type {
  TransactionConflictError,
  DatabaseHasDependentsError,
  DatabaseSealedError,
  KeyOutOfRangeError,
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
  CheckpointNotFoundPublicError,
  DatabaseAlreadyExistsPublicError,
  DatabaseHasDependentsPublicError,
  type DbApiOperation,
  DatabaseNotFoundPublicError,
  type DbPublicError,
  DatabaseSealedPublicError,
  InternalFailurePublicError,
  KeyOutOfRangePublicError,
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
import type { CheckpointNotFoundError } from "./database-lifecycle.ts";
import type { ChangelogUnavailableError, ReaderUnavailableError } from "./db-reader.ts";
import type { DatabaseAlreadyExistsError } from "./manifest-store.ts";

/** Every internal failure the public API classifies. */
export type DbInternalError =
  | DatabaseNotFoundError
  | DatabaseIdMismatchError
  | DatabaseAlreadyExistsError
  | DatabaseSealedError
  | WriterFencedError
  | WriteStalledError
  | WriteBatchInvalidError
  | KeyOutOfRangeError
  | WriterUnavailableError
  | MergeOperatorMissingError
  | SstCorruptedError
  | ReaderUnavailableError
  | CompactorUnavailableError
  | DatabaseHasDependentsError
  | CheckpointNotFoundError
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
          message: `Database ${error.databaseId} does not exist. Create it first, or check the id.`,
          details: { databaseId: error.databaseId },
          requestId,
          retryable: false,
        });
      case "DatabaseAlreadyExistsError":
        return new DatabaseAlreadyExistsPublicError({
          code: "DatabaseAlreadyExists",
          operation,
          message: `Database ${error.databaseId} already exists. Use it, or choose another id.`,
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
              message: `Database ${error.databaseId}'s storage was temporarily unavailable. Retry the same request.`,
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
          message: error.message,
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
