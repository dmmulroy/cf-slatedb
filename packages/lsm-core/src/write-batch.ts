import { Array as Arr, Option, Result, Schema } from "effect";

import { DatabaseId, SeqNum, Timestamp } from "./ids.ts";
import { type KeyRange, keyRangeContains } from "./key-range.ts";
import type { MergeOperator } from "./row-resolution.ts";
import { RowValue } from "./row.ts";

/** Largest key, in bytes. */
export const MAX_KEY_BYTES = 16 * 1024;

/** Largest value or merge operand, in bytes (Durable Object SQLite rows hold at most 2 MB). */
export const MAX_VALUE_BYTES = 1024 * 1024;

/** Most operations in one write batch. */
export const MAX_BATCH_OPERATIONS = 10_000;

/** Largest total of keys and values in one batch, in bytes. */
export const MAX_BATCH_BYTES = 8 * 1024 * 1024;

const TtlMillis = Schema.OptionFromNullOr(Schema.Int.check(Schema.isGreaterThan(0)));

/**
 * Schema for one operation in a write batch, with bytes as base64.
 *
 * - `Put`: set the key's value; `ttlMillis` makes it expire that long after the batch commits.
 * - `Delete`: delete the key.
 * - `Merge`: add a merge operand, combined by the database's merge operator; `ttlMillis` makes this
 *   operand expire on its own (D81).
 */
export const WriteOperation = Schema.Union([
  Schema.TaggedStruct("Put", {
    key: Schema.Uint8ArrayFromBase64,
    value: Schema.Uint8ArrayFromBase64,
    ttlMillis: TtlMillis,
  }),
  Schema.TaggedStruct("Delete", { key: Schema.Uint8ArrayFromBase64 }),
  Schema.TaggedStruct("Merge", {
    key: Schema.Uint8ArrayFromBase64,
    operand: Schema.Uint8ArrayFromBase64,
    ttlMillis: TtlMillis,
  }),
]);

/** One operation in a write batch. */
export type WriteOperation = typeof WriteOperation.Type;

/** Schema for a write batch: operations that commit atomically with one sequence number. */
export const WriteBatch = Schema.Struct({ operations: Schema.Array(WriteOperation) });

/** A write batch. */
export type WriteBatch = typeof WriteBatch.Type;

/**
 * Schema for the receipt of a committed write: its sequence number and commit time.
 *
 * **Details**
 *
 * The write is durable when the receipt is returned. Pass `seq` to an `AtLeast` read to see it.
 */
export const WriteReceipt = Schema.Struct({ seq: SeqNum, createTs: Timestamp });

/** The receipt of a committed write. */
export type WriteReceipt = typeof WriteReceipt.Type;

/**
 * A write batch is invalid: empty, too large, a key or value over its limit, or merges of one key
 * that cannot be combined. Not retryable as is; correct the batch.
 */
export class WriteBatchInvalidError extends Schema.TaggedError<WriteBatchInvalidError>()(
  "WriteBatchInvalidError",
  {
    reason: Schema.Literals([
      "Empty",
      "TooManyOperations",
      "TooLarge",
      "KeyTooLarge",
      "ValueTooLarge",
      "MergeWithoutOperator",
      "MergeTtlMismatch",
    ]),
    /** Index of the offending operation, when one operation is at fault. */
    operationIndex: Schema.OptionFromNullOr(Schema.Int),
    message: Schema.String,
  },
) {}

/**
 * A key is outside the database's key range, as happens after a split. Not retryable against this
 * database; send the key to the database whose range holds it.
 */
export class KeyOutOfRangeError extends Schema.TaggedError<KeyOutOfRangeError>()(
  "KeyOutOfRangeError",
  {
    databaseId: DatabaseId,
    operationIndex: Schema.Int,
    message: Schema.String,
  },
) {}

/** One key's effect in a collapsed batch: a row value and its time to live. */
export interface CollapsedWrite {
  readonly key: Uint8Array;
  readonly value: RowValue;
  readonly ttlMillis: Option.Option<number>;
}

const invalid = (
  reason: WriteBatchInvalidError["reason"],
  operationIndex: Option.Option<number>,
  detail: string,
) =>
  new WriteBatchInvalidError({
    reason,
    operationIndex,
    message: `The write batch was not committed: ${detail}. Correct the batch and send it again; nothing was written.`,
  });

const hexOf = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

const minTtl = (
  left: Option.Option<number>,
  right: Option.Option<number>,
): Option.Option<number> =>
  Option.isNone(left)
    ? right
    : Option.isNone(right)
      ? left
      : Option.some(Math.min(left.value, right.value));

/**
 * Validates a batch and collapses it to one row value per key, since one batch has one sequence
 * number.
 *
 * **Details**
 *
 * Within one batch, a later put or delete replaces earlier operations on the key. Merges after a put
 * fold into that put's value; merges after a delete fold into a value with no base; several merges
 * with no put or delete fold into one operand. Folding needs the merge operator, and merges of one
 * key must share one TTL (SlateDB's batch rule). A folded result expires at the earliest TTL of its
 * parts.
 *
 * @returns The collapsed writes in key order of first appearance, or `WriteBatchInvalidError` /
 *   `KeyOutOfRangeError`; nothing is written on failure.
 */
export const collapseWriteBatch = (
  batch: WriteBatch,
  context: {
    readonly databaseId: DatabaseId;
    readonly keyRange: Option.Option<KeyRange>;
    readonly mergeOperator: Option.Option<MergeOperator>;
  },
): Result.Result<ReadonlyArray<CollapsedWrite>, WriteBatchInvalidError | KeyOutOfRangeError> => {
  if (batch.operations.length === 0)
    return Result.fail(invalid("Empty", Option.none(), "it has no operations"));
  if (batch.operations.length > MAX_BATCH_OPERATIONS) {
    return Result.fail(
      invalid(
        "TooManyOperations",
        Option.none(),
        `it has ${batch.operations.length} operations; the limit is ${MAX_BATCH_OPERATIONS}`,
      ),
    );
  }
  let totalBytes = 0;
  const perKey = new Map<
    string,
    {
      key: Uint8Array;
      base: Option.Option<{ value: RowValue; ttlMillis: Option.Option<number> }>;
      operands: Array<{ operand: Uint8Array; ttlMillis: Option.Option<number>; index: number }>;
    }
  >();
  for (const [index, operation] of batch.operations.entries()) {
    if (operation.key.length > MAX_KEY_BYTES) {
      return Result.fail(
        invalid(
          "KeyTooLarge",
          Option.some(index),
          `operation ${index}'s key is ${operation.key.length} bytes; the limit is ${MAX_KEY_BYTES}`,
        ),
      );
    }
    const payload =
      operation._tag === "Put"
        ? operation.value
        : operation._tag === "Merge"
          ? operation.operand
          : new Uint8Array(0);
    if (payload.length > MAX_VALUE_BYTES) {
      return Result.fail(
        invalid(
          "ValueTooLarge",
          Option.some(index),
          `operation ${index}'s value is ${payload.length} bytes; the limit is ${MAX_VALUE_BYTES}`,
        ),
      );
    }
    totalBytes += operation.key.length + payload.length;
    if (
      Option.isSome(context.keyRange) &&
      !keyRangeContains(context.keyRange.value, operation.key)
    ) {
      return Result.fail(
        new KeyOutOfRangeError({
          databaseId: context.databaseId,
          operationIndex: index,
          message: `Operation ${index}'s key is outside database ${context.databaseId}'s key range, so the batch was not written. Send the key to the database whose range holds it.`,
        }),
      );
    }
    const id = hexOf(operation.key);
    const entry = perKey.get(id) ?? { key: operation.key, base: Option.none(), operands: [] };
    if (operation._tag === "Merge") {
      entry.operands.push({ operand: operation.operand, ttlMillis: operation.ttlMillis, index });
    } else {
      entry.base = Option.some(
        operation._tag === "Put"
          ? { value: RowValue.Value({ bytes: operation.value }), ttlMillis: operation.ttlMillis }
          : { value: RowValue.Tombstone(), ttlMillis: Option.none() },
      );
      entry.operands = [];
    }
    perKey.set(id, entry);
  }
  if (totalBytes > MAX_BATCH_BYTES) {
    return Result.fail(
      invalid(
        "TooLarge",
        Option.none(),
        `its keys and values total ${totalBytes} bytes; the limit is ${MAX_BATCH_BYTES}`,
      ),
    );
  }
  const collapsed: Array<CollapsedWrite> = [];
  for (const entry of perKey.values()) {
    const [first, ...others] = entry.operands;
    if (first === undefined) {
      if (Option.isSome(entry.base)) collapsed.push({ key: entry.key, ...entry.base.value });
      continue;
    }
    const mismatch = others.find(
      (operand) => Option.getOrNull(operand.ttlMillis) !== Option.getOrNull(first.ttlMillis),
    );
    if (mismatch !== undefined) {
      return Result.fail(
        invalid(
          "MergeTtlMismatch",
          Option.some(mismatch.index),
          `merges of one key use different TTLs (operation ${mismatch.index})`,
        ),
      );
    }
    if (Option.isNone(entry.base) && others.length === 0) {
      collapsed.push({
        key: entry.key,
        value: RowValue.Merge({ operand: first.operand }),
        ttlMillis: first.ttlMillis,
      });
      continue;
    }
    if (Option.isNone(context.mergeOperator)) {
      return Result.fail(
        invalid(
          "MergeWithoutOperator",
          Option.some(first.index),
          `operation ${first.index} merges a key the batch also writes, and the database has no merge operator to combine them`,
        ),
      );
    }
    const operands = Arr.map(entry.operands, (operand) => operand.operand);
    if (!Arr.isReadonlyArrayNonEmpty(operands)) continue;
    const base = entry.base;
    const existing = Option.flatMap(base, (b) =>
      b.value._tag === "Value" ? Option.some(b.value.bytes) : Option.none(),
    );
    const merged = context.mergeOperator.value.merge(entry.key, existing, operands);
    collapsed.push({
      key: entry.key,
      value: Option.isSome(base)
        ? RowValue.Value({ bytes: merged })
        : RowValue.Merge({ operand: merged }),
      ttlMillis: Option.isSome(base)
        ? minTtl(base.value.ttlMillis, first.ttlMillis)
        : first.ttlMillis,
    });
  }
  return Result.succeed(collapsed);
};
