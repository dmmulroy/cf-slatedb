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

describe("retainKeyVersions with merge operands of mixed expiry (D81)", () => {
  const key = Uint8Array.of(7);
  const row = (seq: number, value: Row["value"], expireTs: number | undefined): Row => ({
    key,
    seq: SeqNum.make(seq),
    value,
    createTs: Option.some(Timestamp.make(0)),
    expireTs: expireTs === undefined ? Option.none() : Option.some(Timestamp.make(expireTs)),
  });
  const operand = (seq: number, byte: number, expireTs?: number) =>
    row(seq, RowValue.Merge({ operand: Uint8Array.of(byte) }), expireTs);
  const value = (seq: number, byte: number, expireTs?: number) =>
    row(seq, RowValue.Value({ bytes: Uint8Array.of(byte) }), expireTs);
  const compact = (versions: ReadonlyArray<Row>, isBottom = true) =>
    retainKeyVersions(versions, {
      now: Timestamp.make(5),
      retainAboveSeq: Option.none(),
      isBottom,
      mergeOperator: Option.some(MERGE_OPERATORS.Concat),
    });
  const readAt = (versions: ReadonlyArray<Row>, now: number) =>
    Option.map(
      Result.getOrThrow(
        resolveVisibleRow(versions, {
          snapshotSeq: SeqNum.make(100),
          now: Timestamp.make(now),
          mergeOperator: Option.some(MERGE_OPERATORS.Concat),
        }),
      ),
      (found) => (found.value._tag === "Value" ? [...found.value.bytes] : []),
    );
  const shape = (versions: ReadonlyArray<Row>) =>
    versions.map((version) => [
      version.value._tag,
      version.value._tag === "Value"
        ? [...version.value.bytes]
        : version.value._tag === "Merge"
          ? [...version.value.operand]
          : [],
      Option.getOrUndefined(version.expireTs),
    ]);

  it("an operand that expires early leaves the base and the other operands readable after compaction", () => {
    // Newest first: B (no TTL), A (expires at 10), base V (no TTL).
    const versions = [operand(3, 66), operand(2, 65, 10), value(1, 86)];
    const compacted = compact(versions);
    for (const now of [5, 9, 10, 20, 1_000]) {
      expect(readAt(compacted, now)).toEqual(readAt(versions, now));
    }
    expect(readAt(compacted, 20)).toEqual(Option.some([86, 66]));
    // Nothing folds across a different expiry.
    expect(shape(compacted)).toEqual([
      ["Merge", [66], undefined],
      ["Merge", [65], 10],
      ["Value", [86], undefined],
    ]);
  });

  it("folds runs of operands that expire together, and a base only with the same expiry", () => {
    const versions = [
      operand(5, 4, 30),
      operand(4, 3, 30),
      operand(3, 2),
      operand(2, 1),
      value(1, 0),
    ];
    expect(shape(compact(versions))).toEqual([
      ["Merge", [3, 4], 30],
      ["Value", [0, 1, 2], undefined],
    ]);
    // A base that expires on its own stays its own row.
    expect(shape(compact([operand(3, 2), operand(2, 1), value(1, 0, 40)]))).toEqual([
      ["Merge", [1, 2], undefined],
      ["Value", [0], 40],
    ]);
  });

  it("folds the operands just above a delete into a value that expires with them", () => {
    const deleted = row(1, RowValue.Tombstone(), undefined);
    const versions = [operand(3, 2), operand(2, 1, 30), deleted];
    const compacted = compact(versions, false);
    expect(shape(compacted)).toEqual([
      ["Merge", [2], undefined],
      ["Value", [1], 30],
    ]);
    for (const now of [5, 29, 30, 100]) {
      expect(readAt(compacted, now)).toEqual(readAt(versions, now));
    }
  });
});

describe("row resolution at its edges (mutation sweep, D117)", () => {
  const key = Uint8Array.of(4);
  const u64 = (value: bigint) => {
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setBigUint64(0, value, true);
    return bytes;
  };
  const merge = (seq: number, operand: Uint8Array): Row => ({
    key,
    seq: SeqNum.make(seq),
    value: RowValue.Merge({ operand }),
    createTs: Option.none(),
    expireTs: Option.none(),
  });
  const u64Sum = (operands: ReadonlyArray<Uint8Array>) =>
    Option.map(
      Result.getOrThrow(
        resolveVisibleRow(
          operands.map((operand, index) => merge(operands.length - index, operand)),
          {
            snapshotSeq: SeqNum.make(100),
            now: Timestamp.make(0),
            mergeOperator: Option.some(MERGE_OPERATORS.U64Add),
          },
        ),
      ),
      (row) => (row.value._tag === "Value" ? keyToHex(row.value.bytes) : ""),
    );

  it("U64Add zero-extends a short operand, reads only the first 8 bytes of a long one, and wraps", () => {
    // 0x0102 (2 bytes, little-endian 513) + 1.
    expect(u64Sum([Uint8Array.of(1, 2), u64(1n)])).toEqual(Option.some(keyToHex(u64(514n))));
    // A 10-byte operand: its last two bytes are not part of the number.
    expect(u64Sum([Uint8Array.of(5, 0, 0, 0, 0, 0, 0, 0, 9, 9), u64(1n)])).toEqual(
      Option.some(keyToHex(u64(6n))),
    );
    // 2^64 - 1 + 2 wraps to 1.
    expect(u64Sum([u64(2n ** 64n - 1n), u64(2n)])).toEqual(Option.some(keyToHex(u64(1n))));
  });

  it("compaction drops an expired value at the bottom, and keeps exactly the versions above the floor", () => {
    const value = (seq: number, expireTs: Option.Option<number>): Row => ({
      key,
      seq: SeqNum.make(seq),
      value: RowValue.Value({ bytes: Uint8Array.of(seq) }),
      createTs: Option.none(),
      expireTs: Option.map(expireTs, (ms) => Timestamp.make(ms)),
    });
    const atBottom = retainKeyVersions([value(2, Option.some(50)), value(1, Option.none())], {
      now: Timestamp.make(100),
      retainAboveSeq: Option.none(),
      isBottom: true,
      mergeOperator: Option.none(),
    });
    // Expired: a delete, and at the bottom a delete is dropped. Nothing is stored.
    expect(atBottom).toEqual([]);
    const versions = [4, 3, 2, 1].map((seq) => value(seq, Option.none()));
    const retained = retainKeyVersions(versions, {
      now: Timestamp.make(0),
      retainAboveSeq: Option.some(SeqNum.make(2)),
      isBottom: false,
      mergeOperator: Option.none(),
    });
    // 4 and 3 are above the floor and kept as written; 2 is the newest at or below it; 1 goes.
    expect(retained.map((row) => row.seq)).toEqual([4, 3, 2]);
  });
});
