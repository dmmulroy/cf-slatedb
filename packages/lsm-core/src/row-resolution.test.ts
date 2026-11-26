import { Arbitrary, Effect, Option, Result, Schema, Stream } from "effect";
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
import {
  keepReachableVersions,
  MERGE_OPERATORS,
  resolveVisibleRow,
  retainKeyVersions,
} from "./row-resolution.ts";
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

describe("keepReachableVersions", () => {
  /** A history in which the rows of one flush window are replaced by the versions it keeps. */
  const withWindowKept = (rows: ReadonlyArray<Row>, after: number, upTo: number, chunk: number) => {
    const inWindow = (row: Row) => row.seq > after && row.seq <= upTo;
    const kept = Effect.runSync(
      Stream.runCollect(
        keepReachableVersions(
          Stream.fromIterable(sortRows(rows.filter(inWindow))).pipe(Stream.rechunk(chunk)),
        ),
      ),
    );
    return { kept, history: [...rows.filter((row) => !inWindow(row)), ...kept] };
  };

  it("drops only versions no read at or after the window's newest row can reach", () => {
    assertProperty(
      Arbitrary.all({
        writes: narrowHistoryArbitrary(60),
        afterFraction: smallInt(0, 100),
        upToFraction: smallInt(0, 100),
        snapshotFraction: smallInt(0, 100),
        nowOffset: smallInt(0, 120),
        chunk: smallInt(1, 7),
      }),
      ({ writes, afterFraction, upToFraction, snapshotFraction, nowOffset, chunk }) => {
        const rows = rowsFromWrites(writes);
        const [after, upTo] = [afterFraction, upToFraction]
          .map((fraction) => Math.round((rows.length * fraction) / 100))
          .sort((left, right) => left - right);
        if (after === undefined || upTo === undefined) return false;
        const { kept, history } = withWindowKept(rows, after, upTo, chunk);
        // Any snapshot from the window's newest row on, at any time.
        const snapshotSeq = SeqNum.make(
          upTo + Math.round(((rows.length - upTo) * snapshotFraction) / 100),
        );
        const now = Timestamp.make(100 + nowOffset);
        const expected = oracleState(rows, snapshotSeq, now);
        const reads = [...versionsByKey(history)].every(([key, versions]) =>
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
        // Still in row order, and no key of the window lost every row.
        const ordered = sortRows(kept).every((row, index) => row === kept[index]);
        const windowKeys = new Set(
          rows.filter((row) => row.seq > after && row.seq <= upTo).map((row) => keyToHex(row.key)),
        );
        const keptKeys = new Set(kept.map((row) => keyToHex(row.key)));
        return reads && ordered && windowKeys.size === keptKeys.size;
      },
      { runs: 400 },
    );
  });

  it("keeps one row for an overwritten key, and every operand down to the first value or delete", () => {
    const key = Uint8Array.of(1);
    const row = (seq: number, value: Row["value"]): Row => ({
      key,
      seq: SeqNum.make(seq),
      value,
      createTs: Option.none(),
      expireTs: Option.none(),
    });
    const keptSeqs = (versions: ReadonlyArray<Row>) =>
      Effect.runSync(Stream.runCollect(keepReachableVersions(Stream.fromIterable(versions)))).map(
        (kept) => kept.seq,
      );
    const value = RowValue.Value({ bytes: Uint8Array.of(7) });
    const operand = RowValue.Merge({ operand: Uint8Array.of(8) });
    expect(keptSeqs([row(3, value), row(2, value), row(1, value)])).toEqual([3]);
    expect(keptSeqs([row(3, RowValue.Tombstone()), row(2, value)])).toEqual([3]);
    expect(
      keptSeqs([row(5, operand), row(4, operand), row(3, value), row(2, operand), row(1, value)]),
    ).toEqual([5, 4, 3]);
    // Operands with no value or delete under them in the window all stay: their base is older.
    expect(keptSeqs([row(3, operand), row(2, operand)])).toEqual([3, 2]);
  });
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
