import { Arbitrary, Option, Result, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { DatabaseId, SeqNum, Timestamp } from "./ids.ts";
import type { KeyRange } from "./key-range.ts";
import {
  assertProperty,
  keyToHex,
  narrowHistoryArbitrary,
  oracleState,
  rowsFromWrites,
  sortRows,
} from "./lsm-core-arbitraries.ts";
import { MERGE_OPERATORS, resolveVisibleRow } from "./row-resolution.ts";
import type { Row } from "./row.ts";
import {
  collapseWriteBatch,
  MAX_BATCH_BYTES,
  MAX_BATCH_OPERATIONS,
  MAX_KEY_BYTES,
  MAX_TTL_MILLIS,
  MAX_VALUE_BYTES,
  TtlMillisSchema,
  type WriteBatch,
  type WriteOperation,
} from "./write-batch.ts";

const DB = DatabaseId.make("orders");
const context: Parameters<typeof collapseWriteBatch>[1] = {
  databaseId: DB,
  keyRange: Option.none<KeyRange>(),
  mergeOperator: Option.some(MERGE_OPERATORS.Concat),
};

const toOperation = (row: Row): WriteOperation =>
  row.value._tag === "Value"
    ? { _tag: "Put", key: row.key, value: row.value.bytes, ttlMillis: Option.none() }
    : row.value._tag === "Merge"
      ? { _tag: "Merge", key: row.key, operand: row.value.operand, ttlMillis: Option.none() }
      : { _tag: "Delete", key: row.key };

const withoutTtl = (rows: ReadonlyArray<Row>): Array<Row> =>
  rows.map((row) => ({ ...row, expireTs: Option.none() }));

describe("collapseWriteBatch", () => {
  it("reads the same as applying the batch's operations one by one, on top of older history", () => {
    assertProperty(
      Arbitrary.all({ prior: narrowHistoryArbitrary(30), batch: narrowHistoryArbitrary(25) }),
      ({ prior, batch }) => {
        const priorRows = withoutTtl(rowsFromWrites(prior));
        const batchRows = withoutTtl(rowsFromWrites(batch)).map((row) => ({
          ...row,
          seq: SeqNum.make(priorRows.length + row.seq),
        }));
        const expected = oracleState([...priorRows, ...batchRows], Number.MAX_SAFE_INTEGER, 0);
        const batchSeq = SeqNum.make(priorRows.length + 1);
        const collapsed = Result.getOrThrow(
          collapseWriteBatch({ operations: batchRows.map(toOperation) }, context),
        );
        const committed: Array<Row> = collapsed.map((write) => ({
          key: write.key,
          seq: batchSeq,
          value: write.value,
          createTs: Option.none(),
          expireTs: Option.none(),
        }));
        const all = sortRows([...priorRows, ...committed]);
        const keys = new Set(all.map((row) => keyToHex(row.key)));
        return [...keys].every((key) => {
          const versions = all.filter((row) => keyToHex(row.key) === key);
          const visible = Result.getOrThrow(
            resolveVisibleRow(versions, {
              snapshotSeq: batchSeq,
              now: Timestamp.make(0),
              mergeOperator: context.mergeOperator,
            }),
          );
          const want = expected.get(key);
          return Option.match(visible, {
            onNone: () => want === undefined,
            onSome: (row) =>
              want !== undefined &&
              row.value._tag === "Value" &&
              keyToHex(row.value.bytes) === keyToHex(want),
          });
        });
      },
    );
  });

  it("rejects empty batches, out-of-range keys, uncombinable merges, and mismatched merge TTLs", () => {
    const key = Uint8Array.of(5);
    const reason = (batch: WriteBatch, overrides: Partial<typeof context> = {}) => {
      const result = collapseWriteBatch(batch, { ...context, ...overrides });
      return Result.isFailure(result)
        ? result.failure._tag === "WriteBatchInvalidError"
          ? result.failure.reason
          : result.failure._tag
        : "ok";
    };
    expect(reason({ operations: [] })).toBe("Empty");
    expect(
      reason(
        { operations: [{ _tag: "Delete", key }] },
        { keyRange: Option.some({ start: Uint8Array.of(6), end: Option.none() }) },
      ),
    ).toBe("KeyOutOfRangeError");
    expect(
      reason(
        {
          operations: [
            { _tag: "Merge", key, operand: Uint8Array.of(1), ttlMillis: Option.none() },
            { _tag: "Merge", key, operand: Uint8Array.of(2), ttlMillis: Option.none() },
          ],
        },
        { mergeOperator: Option.none() },
      ),
    ).toBe("MergeWithoutOperator");
    expect(
      reason({
        operations: [
          { _tag: "Merge", key, operand: Uint8Array.of(1), ttlMillis: Option.some(10) },
          { _tag: "Merge", key, operand: Uint8Array.of(2), ttlMillis: Option.some(20) },
        ],
      }),
    ).toBe("MergeTtlMismatch");
    expect(
      reason(
        {
          operations: [{ _tag: "Merge", key, operand: Uint8Array.of(1), ttlMillis: Option.none() }],
        },
        { mergeOperator: Option.none() },
      ),
    ).toBe("ok");
  });
});

describe("collapseWriteBatch at its limits (mutation sweep, D117)", () => {
  const put = (key: Uint8Array, value: Uint8Array): WriteOperation => ({
    _tag: "Put",
    key,
    value,
    ttlMillis: Option.none(),
  });
  const reasonAndIndex = (batch: WriteBatch) => {
    const collapsed = collapseWriteBatch(batch, context);
    return Result.isSuccess(collapsed)
      ? "ok"
      : collapsed.failure._tag === "WriteBatchInvalidError"
        ? `${collapsed.failure.reason}@${Option.getOrElse(collapsed.failure.operationIndex, () => -1)}`
        : collapsed.failure._tag;
  };

  it("names the operation a merge refusal is about", () => {
    const key = Uint8Array.of(5);
    const merge = (operand: number, ttl: Option.Option<number>): WriteOperation => ({
      _tag: "Merge",
      key,
      operand: Uint8Array.of(operand),
      ttlMillis: ttl,
    });
    const other = put(Uint8Array.of(9), Uint8Array.of(1));
    // The second merge is the one whose TTL differs; without an operator, the first merge.
    expect(
      reasonAndIndex({ operations: [other, merge(1, Option.some(10)), merge(2, Option.some(20))] }),
    ).toBe("MergeTtlMismatch@2");
    const withoutOperator = collapseWriteBatch(
      { operations: [other, merge(1, Option.none()), merge(2, Option.none())] },
      { ...context, mergeOperator: Option.none() },
    );
    expect(
      Result.isFailure(withoutOperator) &&
        withoutOperator.failure._tag === "WriteBatchInvalidError" &&
        Option.getOrNull(withoutOperator.failure.operationIndex),
    ).toBe(1);
  });

  it("pins the documented limits: 16 KiB keys, 1 MiB values, 10,000 operations, 8 MiB per batch", () => {
    // Public limits (the OpenAPI contract and docs/api-compatibility.md name them).
    expect([MAX_KEY_BYTES, MAX_VALUE_BYTES, MAX_BATCH_OPERATIONS, MAX_BATCH_BYTES]).toEqual([
      16_384, 1_048_576, 10_000, 8_388_608,
    ]);
  });

  it("accepts a key, a value, a batch and an operation count exactly at each limit, and refuses one past it, naming the operation", () => {
    const small = Uint8Array.of(1);
    expect(reasonAndIndex({ operations: [put(new Uint8Array(MAX_KEY_BYTES), small)] })).toBe("ok");
    expect(
      reasonAndIndex({
        operations: [put(small, small), put(new Uint8Array(MAX_KEY_BYTES + 1), small)],
      }),
    ).toBe("KeyTooLarge@1");
    expect(reasonAndIndex({ operations: [put(small, new Uint8Array(MAX_VALUE_BYTES))] })).toBe(
      "ok",
    );
    expect(
      reasonAndIndex({
        operations: [put(small, small), put(Uint8Array.of(2), new Uint8Array(MAX_VALUE_BYTES + 1))],
      }),
    ).toBe("ValueTooLarge@1");
    const many = (count: number) =>
      Array.from({ length: count }, (_, index) =>
        put(Uint8Array.of(index >> 8, index & 0xff), small),
      );
    expect(reasonAndIndex({ operations: many(MAX_BATCH_OPERATIONS) })).toBe("ok");
    expect(reasonAndIndex({ operations: many(MAX_BATCH_OPERATIONS + 1) })).toBe(
      "TooManyOperations@-1",
    );
    // Eight values of exactly 1 MiB less their one-byte keys: exactly MAX_BATCH_BYTES, then one byte over.
    const value = new Uint8Array(MAX_BATCH_BYTES / 8 - 1);
    const exact = Array.from({ length: 8 }, (_, index) => put(Uint8Array.of(index), value));
    expect(reasonAndIndex({ operations: exact })).toBe("ok");
    expect(
      reasonAndIndex({ operations: [...exact, put(Uint8Array.of(9), new Uint8Array(0))] }),
    ).toBe("TooLarge@-1");
  });

  it("accepts a time to live of exactly MAX_TTL_MILLIS and refuses one past it", () => {
    const decode = Schema.decodeUnknownResult(TtlMillisSchema);
    expect(Result.isSuccess(decode(MAX_TTL_MILLIS))).toBe(true);
    expect(Result.isFailure(decode(MAX_TTL_MILLIS + 1))).toBe(true);
    // 100 years, in milliseconds: the documented limit, not one the code may drift from.
    expect(MAX_TTL_MILLIS).toBe(100 * 365 * 24 * 60 * 60 * 1000);
  });
});
