import { Option, Result } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { SeqNum, Timestamp } from "./ids.ts";
import {
  MAX_MERGED_VALUE_BYTES,
  MERGE_OPERATORS,
  resolveVisibleRow,
  retainKeyVersions,
} from "./row-resolution.ts";
import { type Row, RowValue } from "./row.ts";

const KEY = Uint8Array.of(0x6b);
const concat = Option.some(MERGE_OPERATORS.Concat);
const now = Timestamp.make(0);

const operand = (seq: number, bytes: number): Row => ({
  key: KEY,
  seq: SeqNum.make(seq),
  value: RowValue.Merge({ operand: new Uint8Array(bytes).fill(seq) }),
  createTs: Option.none(),
  expireTs: Option.none(),
});
const value = (seq: number, bytes: number): Row => ({
  ...operand(seq, 0),
  value: RowValue.Value({ bytes: new Uint8Array(bytes).fill(seq) }),
});

/** Newest first, as stored. */
const history = (operandBytes: number) => [
  operand(3, operandBytes),
  operand(2, operandBytes),
  value(1, 1),
];

const read = (versions: ReadonlyArray<Row>) =>
  resolveVisibleRow(versions, { snapshotSeq: SeqNum.make(10), now, mergeOperator: concat });

describe("merged values are bounded (D99)", () => {
  it("a read past the bound fails typed with the size; at the bound it returns the value", () => {
    // Exactly the bound: 524,288 + 524,287 operand bytes over a 1-byte value.
    const half = MAX_MERGED_VALUE_BYTES / 2;
    const at = read([operand(3, half), operand(2, half - 1), value(1, 1)]);
    expect(Result.isSuccess(at) && Option.map(at.success, (row) => row.value._tag)).toEqual(
      Option.some("Value"),
    );
    const past = read(history(MAX_MERGED_VALUE_BYTES / 2));
    expect(Result.isFailure(past) && past.failure).toMatchObject({
      _tag: "MergedValueTooLargeError",
      bytes: MAX_MERGED_VALUE_BYTES + 1,
      limit: MAX_MERGED_VALUE_BYTES,
    });
  });

  it("compaction leaves operands that would fold past the bound as they are, and never fails", () => {
    const versions = history(MAX_MERGED_VALUE_BYTES / 2);
    const kept = retainKeyVersions(versions, {
      now,
      retainAboveSeq: Option.none(),
      isBottom: true,
      mergeOperator: concat,
    });
    // Every row is kept as written: no stored row grows past the bound.
    expect(kept).toEqual(versions);
    // A read of what compaction kept answers exactly as before it.
    expect(read(kept)).toEqual(read(versions));
    // Below the bound the history folds into one value, as before.
    const small = retainKeyVersions(history(10), {
      now,
      retainAboveSeq: Option.none(),
      isBottom: true,
      mergeOperator: concat,
    });
    expect(small.map((row) => row.value._tag)).toEqual(["Value"]);
  });

  it("compaction folds a history that is exactly at the bound, the same size a read returns (mutation sweep, D117)", () => {
    const half = MAX_MERGED_VALUE_BYTES / 2;
    const atBound = [operand(3, half), operand(2, half - 1), value(1, 1)];
    const kept = retainKeyVersions(atBound, {
      now,
      retainAboveSeq: Option.none(),
      isBottom: true,
      mergeOperator: concat,
    });
    expect(kept.map((row) => row.value._tag)).toEqual(["Value"]);
    expect(read(kept)).toEqual(read(atBound));
    // The bound is the documented 1 MiB.
    expect(MAX_MERGED_VALUE_BYTES).toBe(1_048_576);
  });
});
