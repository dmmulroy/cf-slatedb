import {
  CheckpointId,
  CheckpointLimitError,
  DatabaseHasDependentsError,
  DatabaseId,
  DatabaseSealedError,
  KeyOutOfRangeError,
  MergedValueTooLargeError,
  MergeOperatorMissingError,
  SstCorruptedError,
  SstId,
  SstNotInManifestError,
  TransactionConflictError,
  WriteBatchInvalidError,
  WriterEpoch,
  WriterFencedError,
} from "@shaledb/lsm-core";
import { Option } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CompactorUnavailableError, SstDropRefusedError } from "./compactor.ts";
import { CheckpointNotFoundError, DatabaseRecentlyRemovedError } from "./database-lifecycle.ts";
import { ChangelogUnavailableError, ReaderUnavailableError } from "./db-reader.ts";
import { type DbInternalError, toPublicError } from "./db-public-error-translation.ts";
import {
  DatabaseIdMismatchError,
  DatabaseNotFoundError,
  WriterUnavailableError,
  WriteStalledError,
} from "./lsm-errors.ts";
import { DatabaseAlreadyExistsError } from "./manifest-store.ts";

/**
 * Every internal failure the public API classifies, with the public answer a caller gets: the
 * error (whose HTTP status `DB_PUBLIC_ERRORS` fixes), its code, and whether retrying may succeed
 * (`docs/errors.md`). Pinned one by one (mutation sweep, D128): a wrong `retryable` tells a
 * caller to give up on a request that would succeed, or to repeat one that never will.
 */
const DB = DatabaseId.make("orders");
const SST = SstId.make("01JD0000000000000000000AAA");
const message = "m";

const cases: ReadonlyArray<
  readonly [DbInternalError, string, string, boolean, Record<string, unknown>?]
> = [
  [
    new DatabaseNotFoundError({ databaseId: DB, message }),
    "DatabaseNotFoundPublicError",
    "DatabaseNotFound",
    false,
  ],
  [
    new DatabaseAlreadyExistsError({ databaseId: DB, message }),
    "DatabaseAlreadyExistsPublicError",
    "DatabaseAlreadyExists",
    false,
  ],
  [
    new DatabaseSealedError({ databaseId: DB, successors: [DatabaseId.make("orders-a")], message }),
    "DatabaseSealedPublicError",
    "DatabaseSealed",
    false,
    { successors: ["orders-a"] },
  ],
  [
    new KeyOutOfRangeError({ databaseId: DB, operationIndex: 3, message }),
    "KeyOutOfRangePublicError",
    "KeyOutOfRange",
    false,
    { operationIndex: 3 },
  ],
  [
    new WriteBatchInvalidError({ reason: "Empty", operationIndex: Option.none(), message }),
    "BatchInvalidPublicError",
    "BatchInvalid",
    false,
    { reason: "Empty", operationIndex: null },
  ],
  [
    new DatabaseRecentlyRemovedError({
      databaseId: DB,
      reusableAt: 10_000,
      retryAfterMs: 1_234.2,
      message,
    }),
    "DatabaseRecentlyRemovedPublicError",
    "DatabaseRecentlyRemoved",
    true,
    { retryAfterMs: 1_235 },
  ],
  [
    new DatabaseRecentlyRemovedError({
      databaseId: DB,
      reusableAt: 10_000,
      retryAfterMs: -5,
      message,
    }),
    "DatabaseRecentlyRemovedPublicError",
    "DatabaseRecentlyRemoved",
    true,
    { retryAfterMs: 0 },
  ],
  [
    new WriteStalledError({ databaseId: DB, reason: "L0Backlog", retryAfterMs: 5_000, message }),
    "WriteStalledPublicError",
    "WriteStalled",
    true,
    { reason: "L0Backlog", retryAfterMs: 5_000 },
  ],
  [
    new MergedValueTooLargeError({ bytes: 2_000_000, limit: 1_048_576, message }),
    "MergedValueTooLargePublicError",
    "MergedValueTooLarge",
    false,
  ],
  [
    new MergeOperatorMissingError({ message }),
    "MergeOperatorMissingPublicError",
    "MergeOperatorMissing",
    false,
  ],
  [
    new WriterFencedError({
      databaseId: DB,
      writerEpoch: WriterEpoch.make(1),
      currentWriterEpoch: WriterEpoch.make(2),
      message,
    }),
    "StorageUnavailablePublicError",
    "StorageUnavailable",
    true,
  ],
  [
    new WriterUnavailableError({
      databaseId: DB,
      operation: "write",
      reason: "StoredDataInvalid",
      message,
    }),
    "StoredDataInvalidPublicError",
    "StoredDataInvalid",
    false,
  ],
  [
    new WriterUnavailableError({
      databaseId: DB,
      operation: "write",
      reason: "StorageUnavailable",
      message,
    }),
    "StorageUnavailablePublicError",
    "StorageUnavailable",
    true,
  ],
  [
    new WriterUnavailableError({
      databaseId: DB,
      operation: "write",
      reason: "ManifestContention",
      message,
    }),
    "StorageUnavailablePublicError",
    "StorageUnavailable",
    true,
  ],
  [
    new WriterUnavailableError({
      databaseId: DB,
      operation: "write",
      reason: "WriterUnreachable",
      message,
    }),
    "StorageUnavailablePublicError",
    "StorageUnavailable",
    true,
  ],
  [
    new ChangelogUnavailableError({ databaseId: DB, reason: "Expired", message }),
    "ChangelogUnavailablePublicError",
    "ChangelogUnavailable",
    false,
    { reason: "Expired" },
  ],
  [
    new TransactionConflictError({ databaseId: DB, reason: "ReadConflict", message }),
    "TransactionConflictPublicError",
    "TransactionConflict",
    true,
    { reason: "ReadConflict" },
  ],
  [
    new DatabaseHasDependentsError({
      databaseId: DB,
      dependents: [DatabaseId.make("orders-a")],
      message,
    }),
    "DatabaseHasDependentsPublicError",
    "DatabaseHasDependents",
    false,
  ],
  [
    new CheckpointNotFoundError({
      databaseId: DB,
      checkpointId: CheckpointId.make("01JD0000000000000000000CCC"),
      message,
    }),
    "CheckpointNotFoundPublicError",
    "CheckpointNotFound",
    false,
  ],
  [
    new CheckpointLimitError({ databaseId: DB, checkpoints: 1_000, limit: 1_000, message }),
    "CheckpointLimitReachedPublicError",
    "CheckpointLimitReached",
    false,
  ],
  [
    new SstNotInManifestError({ databaseId: DB, sstId: SST, message }),
    "SstNotFoundPublicError",
    "SstNotFound",
    false,
  ],
  [
    new SstDropRefusedError({ databaseId: DB, sstId: SST, reason: "CompactionRunning", message }),
    "SstDropRefusedPublicError",
    "SstDropRefused",
    true,
  ],
  [
    new SstDropRefusedError({
      databaseId: DB,
      sstId: SST,
      reason: "NotReportedCorrupted",
      message,
    }),
    "SstDropRefusedPublicError",
    "SstDropRefused",
    false,
  ],
  [
    new SstDropRefusedError({
      databaseId: DB,
      sstId: SST,
      reason: "ConfirmationMismatch",
      message,
    }),
    "SstDropRefusedPublicError",
    "SstDropRefused",
    false,
  ],
  [
    new CompactorUnavailableError({ databaseId: DB, message }),
    "StorageUnavailablePublicError",
    "StorageUnavailable",
    true,
  ],
  [
    new ReaderUnavailableError({ databaseId: DB, reason: "SnapshotExpired", message }),
    "SnapshotExpiredPublicError",
    "SnapshotExpired",
    false,
  ],
  [
    new ReaderUnavailableError({ databaseId: DB, reason: "StoredManifestInvalid", message }),
    "StoredDataInvalidPublicError",
    "StoredDataInvalid",
    false,
  ],
  [
    new ReaderUnavailableError({ databaseId: DB, reason: "StorageUnavailable", message }),
    "StorageUnavailablePublicError",
    "StorageUnavailable",
    true,
  ],
  [
    new DatabaseIdMismatchError({
      databaseId: DB,
      servedDatabaseId: DatabaseId.make("users"),
      message,
    }),
    "InternalFailurePublicError",
    "InternalFailure",
    false,
  ],
];

describe("toPublicError (D128)", () => {
  it("answers every internal failure with its public error, code, and retryable", () => {
    for (const [error, tag, code, retryable, details] of cases) {
      const answer = toPublicError("write", "req-1")(error);
      expect([error._tag, answer._tag, answer.code, answer.retryable]).toEqual([
        error._tag,
        tag,
        code,
        retryable,
      ]);
      expect(answer.requestId).toBe("req-1");
      expect(answer.operation).toBe("write");
      if (details !== undefined) expect(answer.details).toMatchObject(details);
    }
  });

  // D53 and the corruption runbook (`docs/operations.md`) promise StoredDataInvalid for a read of a
  // corrupted SST. The error carries no database id, so the handler passes the one its request
  // names (D128); without one the answer stays InternalFailure.
  it("answers a read of a corrupted SST with StoredDataInvalid naming the database (D53, D128)", () => {
    const corrupted = new SstCorruptedError({
      sstId: SST,
      section: "DataBlock",
      reason: "ChecksumMismatch",
      offset: 0,
      message,
    });
    for (const operation of ["get", "scan", "transaction", "changes"] as const) {
      const answer = toPublicError(operation, "r", Option.some(DB))(corrupted);
      expect([answer.code, answer.retryable, answer.details]).toEqual([
        "StoredDataInvalid",
        false,
        { databaseId: DB },
      ]);
      expect(answer.message).toContain(SST);
    }
    expect(toPublicError("get", "r")(corrupted).code).toBe("InternalFailure");
  });

  it("tells a write whose writer was unreachable that it may have committed, and other operations to retry", () => {
    const unreachable = new WriterUnavailableError({
      databaseId: DB,
      operation: "write",
      reason: "WriterUnreachable",
      message,
    });
    for (const operation of ["write", "transaction"] as const) {
      expect(toPublicError(operation, "r")(unreachable).message).toContain(
        "may or may not have committed",
      );
    }
    expect(toPublicError("get", "r")(unreachable).message).toContain("Retry the same request");
    const unavailable = new WriterUnavailableError({
      databaseId: DB,
      operation: "write",
      reason: "StorageUnavailable",
      message,
    });
    expect(toPublicError("write", "r")(unavailable).message).toContain("Retry the same request");
  });

  it("warns only a write conflict that an earlier attempt of the commit may have committed (D88)", () => {
    const conflict = (reason: "WriteConflict" | "ReadConflict") =>
      toPublicError(
        "transaction",
        "r",
      )(new TransactionConflictError({ databaseId: DB, reason, message: "Conflict." })).message;
    expect(conflict("WriteConflict")).toContain("it may have committed");
    expect(conflict("ReadConflict")).toBe("Conflict.");
  });

  it("tells a retried remove that a completed first attempt answers this way, and others to create first", () => {
    const missing = new DatabaseNotFoundError({ databaseId: DB, message });
    expect(toPublicError("remove", "r")(missing).message).toContain("already removed");
    expect(toPublicError("get", "r")(missing).message).toContain("Create it first");
  });
});
