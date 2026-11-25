import { Arbitrary, Option, Result } from "effect";
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
import { collapseWriteBatch, type WriteBatch, type WriteOperation } from "./write-batch.ts";

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
