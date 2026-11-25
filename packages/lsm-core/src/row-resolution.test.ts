import { Arbitrary, Option, Result, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { SeqNum, Timestamp } from "./ids.ts";
import {
  assertProperty,
  keyToHex,
  narrowHistoryArbitrary,
  oracleState,
  rowsFromWrites,
  sortRows,
} from "./lsm-core-arbitraries.ts";
import { MERGE_OPERATORS, resolveVisibleRow, retainKeyVersions } from "./row-resolution.ts";
import { type Row, RowValue } from "./row.ts";

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

/** Each key's versions, newest first. */
const versionsByKey = (rows: ReadonlyArray<Row>): Map<string, Array<Row>> => {
  const groups = new Map<string, Array<Row>>();
  for (const row of sortRows(rows)) {
    const key = keyToHex(row.key);
    groups.set(key, [...(groups.get(key) ?? []), row]);
  }
  return groups;
};

const sameValue = (visible: Option.Option<Row>, expected: Uint8Array | undefined): boolean =>
  Option.match(visible, {
    onNone: () => expected === undefined,
    onSome: (row) =>
      expected !== undefined &&
      row.value._tag === "Value" &&
      keyToHex(row.value.bytes) === keyToHex(expected),
  });

describe("resolveVisibleRow", () => {
  it("matches a plain oracle for puts, deletes, merges, and TTL at any snapshot and time", () => {
    assertProperty(
      Arbitrary.all({
        writes: narrowHistoryArbitrary(60),
        snapshotFraction: smallInt(0, 100),
        nowOffset: smallInt(0, 120),
      }),
      ({ writes, snapshotFraction, nowOffset }) => {
        const rows = rowsFromWrites(writes);
        const snapshotSeq = SeqNum.make(Math.round((rows.length * snapshotFraction) / 100));
        const now = Timestamp.make(100 + nowOffset);
        const expected = oracleState(rows, snapshotSeq, now);
        return [...versionsByKey(rows)].every(([key, versions]) =>
          sameValue(
            Result.getOrThrow(
              resolveVisibleRow(versions, {
                snapshotSeq,
                now,
                mergeOperator: Option.some(MERGE_OPERATORS.Concat),
              }),
            ),
            expected.get(key),
          ),
        );
      },
      { runs: 300 },
    );
  });

  it("reports the earliest expiry among a merge result's contributors, and the newest seq", () => {
    const key = Uint8Array.of(1);
    const versions: Array<Row> = [
      {
        key,
        seq: SeqNum.make(3),
        value: RowValue.Merge({ operand: Uint8Array.of(3) }),
        createTs: Option.some(Timestamp.make(30)),
        expireTs: Option.some(Timestamp.make(500)),
      },
      {
        key,
        seq: SeqNum.make(2),
        value: RowValue.Merge({ operand: Uint8Array.of(2) }),
        createTs: Option.some(Timestamp.make(20)),
        expireTs: Option.some(Timestamp.make(200)),
      },
      {
        key,
        seq: SeqNum.make(1),
        value: RowValue.Value({ bytes: Uint8Array.of(1) }),
        createTs: Option.some(Timestamp.make(10)),
        expireTs: Option.none(),
      },
    ];
    const visible = Result.getOrThrow(
      resolveVisibleRow(versions, {
        snapshotSeq: SeqNum.make(3),
        now: Timestamp.make(0),
        mergeOperator: Option.some(MERGE_OPERATORS.Concat),
      }),
    );
    expect(
      Option.map(visible, (row) => [row.seq, Option.getOrNull(row.expireTs), row.value]),
    ).toEqual(Option.some([3, 200, RowValue.Value({ bytes: Uint8Array.of(1, 2, 3) })]));
  });

  it("adds little-endian u64 operands with U64Add", () => {
    const key = Uint8Array.of(9);
    const u64 = (value: number) => {
      const bytes = new Uint8Array(8);
      new DataView(bytes.buffer).setBigUint64(0, BigInt(value), true);
      return bytes;
    };
    const versions: Array<Row> = [5, 7, 30].map((amount, index) => ({
      key,
      seq: SeqNum.make(3 - index),
      value: RowValue.Merge({ operand: u64(amount) }),
      createTs: Option.none(),
      expireTs: Option.none(),
    }));
    const visible = Result.getOrThrow(
      resolveVisibleRow(versions, {
        snapshotSeq: SeqNum.make(3),
        now: Timestamp.make(0),
        mergeOperator: Option.some(MERGE_OPERATORS.U64Add),
      }),
    );
    expect(
      Option.map(visible, (row) => (row.value._tag === "Value" ? keyToHex(row.value.bytes) : "")),
    ).toEqual(Option.some(keyToHex(u64(42))));
  });

  it("fails with MergeOperatorMissingError when operands need an operator", () => {
    const resolved = resolveVisibleRow(
      [
        {
          key: Uint8Array.of(1),
          seq: SeqNum.make(1),
          value: RowValue.Merge({ operand: Uint8Array.of(1) }),
          createTs: Option.none(),
          expireTs: Option.none(),
        },
      ],
      { snapshotSeq: SeqNum.make(1), now: Timestamp.make(0), mergeOperator: Option.none() },
    );
    expect(Result.isFailure(resolved) && resolved.failure._tag).toBe("MergeOperatorMissingError");
  });
});

describe("retainKeyVersions", () => {
  it("keeps raw operands and their base when no merge operator is configured", () => {
    assertProperty(narrowHistoryArbitrary(60), (writes) => {
      const rows = rowsFromWrites(writes);
      return [...versionsByKey(rows)].every(([, versions]) => {
        const kept = retainKeyVersions(versions, {
          now: Timestamp.make(0),
          retainAboveSeq: Option.none(),
          isBottom: false,
          mergeOperator: Option.none(),
        });
        // Operands survive unchanged, newest first, followed by at most their base.
        const operands = kept.filter((row) => row.value._tag === "Merge");
        return (
          operands.every((row, index) => row.seq === versions[index]?.seq) &&
          kept.length <= operands.length + 1
        );
      });
    });
  });
});
