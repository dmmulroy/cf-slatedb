import { CheckpointLimitError, DatabaseId, MergedValueTooLargeError } from "@shaledb/lsm-core";
import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  CreateCheckpointRequest,
  CreateDatabaseRequest,
  MAX_CHANGELOG_RETENTION_MS,
  MAX_CHECKPOINT_NAME_LENGTH,
} from "./db-http-api.ts";

import { toPublicError } from "./db-public-error-translation.ts";

describe("the public errors of D99's limits", () => {
  it("a checkpoint past the limit is a 409 CheckpointLimitReached, not retryable, naming the counts", () => {
    const error = toPublicError(
      "checkpoint",
      "req-1",
    )(
      new CheckpointLimitError({
        databaseId: DatabaseId.make("orders"),
        checkpoints: 1_000,
        limit: 1_000,
        message: "full",
      }),
    );
    expect(error).toMatchObject({
      _tag: "CheckpointLimitReachedPublicError",
      code: "CheckpointLimitReached",
      retryable: false,
      details: { databaseId: "orders", checkpoints: 1_000, limit: 1_000 },
    });
    expect(error.message).toContain("Delete one");
  });
});

describe("a merged value past its bound", () => {
  it("is a 409 MergedValueTooLarge, not retryable, telling the caller to replace the key", () => {
    const error = toPublicError(
      "get",
      "req-2",
    )(new MergedValueTooLargeError({ bytes: 2_000_000, limit: 1_048_576, message: "large" }));
    expect(error).toMatchObject({
      _tag: "MergedValueTooLargePublicError",
      code: "MergedValueTooLarge",
      retryable: false,
      details: { bytes: 2_000_000, limit: 1_048_576 },
    });
    expect(error.message).toContain("Replace the key with a put");
  });
});

describe("the request bounds of D99", () => {
  const database = (changelogRetentionMs: number | null, budgetGroup: string | null) =>
    Schema.decodeUnknownExit(CreateDatabaseRequest)({
      databaseId: "orders",
      keyRange: null,
      mergeOperator: null,
      changelogRetentionMs,
      budgetGroup,
    })._tag;
  const checkpoint = (name: string) =>
    Schema.decodeUnknownExit(CreateCheckpointRequest)({ name, lifetimeMs: null })._tag;

  it("bounds changelog retention at 30 days, a budget group at 63 characters, a checkpoint name at 256", () => {
    expect(database(MAX_CHANGELOG_RETENTION_MS, "g".repeat(63))).toBe("Success");
    expect(database(MAX_CHANGELOG_RETENTION_MS + 1, null)).toBe("Failure");
    expect(database(null, "g".repeat(64))).toBe("Failure");
    expect(checkpoint("n".repeat(MAX_CHECKPOINT_NAME_LENGTH))).toBe("Success");
    expect(checkpoint("n".repeat(MAX_CHECKPOINT_NAME_LENGTH + 1))).toBe("Failure");
  });
});
