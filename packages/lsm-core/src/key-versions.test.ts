import { Arbitrary, Effect, Option, Result, Schema, Stream } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { compactRowStreams } from "./compaction-merge.ts";
import { SeqNum, Timestamp } from "./ids.ts";
import { collectKeyVersions, keyVersionsOf, streamKeyVersions } from "./key-versions.ts";
import {
  assertProperty,
  keyToHex,
  narrowHistoryArbitrary,
  oracleState,
  rowsFromWrites,
  sortRows,
} from "./lsm-core-arbitraries.ts";
import { MERGE_OPERATORS, resolveVisibleRow } from "./row-resolution.ts";
import { type Row, RowValue } from "./row.ts";

const concat = Option.some(MERGE_OPERATORS.Concat);
const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

const merge = (seq: number, operand: number): Row => ({
  key: Uint8Array.of(1),
  seq: SeqNum.make(seq),
  value: RowValue.Merge({ operand: Uint8Array.of(operand) }),
  createTs: Option.none(),
  expireTs: Option.none(),
});

const bytesOf = (visible: Option.Option<Row>) =>
  Option.getOrUndefined(
    Option.map(visible, (row) => (row.value._tag === "Value" ? [...row.value.bytes] : [])),
  );

const resolve = (versions: Parameters<typeof resolveVisibleRow>[0], snapshotSeq: number) =>
  Result.getOrThrow(
    resolveVisibleRow(versions, {
      snapshotSeq: SeqNum.make(snapshotSeq),
      now: Timestamp.make(0),
      mergeOperator: concat,
    }),
  );

/**
 * A read's sources as the system builds them: a manifest holding every row through `lastL0Seq`
 * (compacted, so operands that expire together fold into one row), and the writer still keeping
 * every row above `retainedFromSeq`, overlapping the manifest (D58).
 */
const readSources = (
  rows: ReadonlyArray<Row>,
  cuts: {
    readonly retainedFromSeq: number;
    readonly lastL0Seq: number;
    readonly snapshotSeq: number;
  },
  now: Timestamp,
) => {
  const manifestRows = Effect.runSync(
    Stream.runCollect(
      compactRowStreams(
        [Stream.fromIterable(sortRows(rows.filter((row) => row.seq <= cuts.lastL0Seq)))],
        {
          now,
          isBottom: true,
          mergeOperator: concat,
        },
      ),
    ),
  ).flat();
  const writerRows = sortRows(
    rows.filter((row) => row.seq > cuts.retainedFromSeq && row.seq <= cuts.snapshotSeq),
  );
  return { manifestRows, writerRows };
};

const historyAndCuts = Arbitrary.all({
  writes: narrowHistoryArbitrary(50),
  fractions: Arbitrary.array(smallInt(0, 100), { minLength: 3, maxLength: 3 }),
  nowOffset: smallInt(0, 120),
});

const cutsOf = (length: number, fractions: ReadonlyArray<number>) => {
  const [retainedFromSeq = 0, lastL0Seq = 0, snapshotSeq = 0] = fractions
    .map((fraction) => Math.round((length * fraction) / 100))
    .sort((left, right) => left - right);
  return { retainedFromSeq, lastL0Seq, snapshotSeq };
};

describe("collectKeyVersions", () => {
  it("takes the writer's rows only above the manifest's lastL0Seq, so a retained operand is not applied twice", () => {
    // Both operands are flushed (lastL0Seq 2) and still kept by the writer for transactions.
    const versions = collectKeyVersions(
      Uint8Array.of(1),
      [merge(2, 2), merge(1, 1)],
      [[merge(2, 2), merge(1, 1)]],
      { manifestLastL0Seq: SeqNum.make(2), snapshotSeq: SeqNum.make(2) },
    );
    expect(bytesOf(resolve(versions, 2))).toEqual([1, 2]);
  });

  it("keeps a compaction's fold over the writer's unfolded copy, so the operands below it are not lost", () => {
    // The writer trimmed seq 1; the compaction folded seqs 1 and 2 into one operand at seq 2.
    const folded: Row = { ...merge(2, 0), value: RowValue.Merge({ operand: Uint8Array.of(1, 2) }) };
    const versions = collectKeyVersions(Uint8Array.of(1), [merge(2, 2)], [[folded]], {
      manifestLastL0Seq: SeqNum.make(2),
      snapshotSeq: SeqNum.make(2),
    });
    expect(bytesOf(resolve(versions, 2))).toEqual([1, 2]);
    // Deduplicating by sequence number (the writer's copy first) loses operand 1.
    expect(bytesOf(resolve(keyVersionsOf([merge(2, 2), folded]), 2))).toEqual([2]);
  });

  it("reads every key as the full history does, whatever the overlap between the writer and a compacted manifest", () => {
    assertProperty(
      historyAndCuts,
      ({ writes, fractions, nowOffset }) => {
        const rows = rowsFromWrites(writes);
        const cuts = cutsOf(rows.length, fractions);
        const now = Timestamp.make(100 + nowOffset);
        const { manifestRows, writerRows } = readSources(rows, cuts, now);
        const expected = oracleState(rows, cuts.snapshotSeq, now);
        const bounds = {
          manifestLastL0Seq: SeqNum.make(cuts.lastL0Seq),
          snapshotSeq: SeqNum.make(cuts.snapshotSeq),
        };
        const keys = new Set(rows.map((row) => keyToHex(row.key)));
        return [...keys].every((hex) => {
          const key = rows.find((row) => keyToHex(row.key) === hex)?.key ?? new Uint8Array(0);
          const versions = collectKeyVersions(
            key,
            writerRows,
            [manifestRows.filter((row) => keyToHex(row.key) === hex)],
            bounds,
          );
          const visible = Result.getOrThrow(
            resolveVisibleRow(versions, {
              snapshotSeq: bounds.snapshotSeq,
              now,
              mergeOperator: concat,
            }),
          );
          const want = expected.get(hex);
          return Option.match(visible, {
            onNone: () => want === undefined,
            onSome: (row) =>
              want !== undefined &&
              row.value._tag === "Value" &&
              keyToHex(row.value.bytes) === keyToHex(want),
          });
        });
      },
      { runs: 1_000 },
    );
  });
});

describe("streamKeyVersions", () => {
  it("scans every key as the full history does, whatever the overlap between the writer and a compacted manifest", () => {
    assertProperty(
      historyAndCuts,
      ({ writes, fractions, nowOffset }) => {
        const rows = rowsFromWrites(writes);
        const cuts = cutsOf(rows.length, fractions);
        const now = Timestamp.make(100 + nowOffset);
        const { manifestRows, writerRows } = readSources(rows, cuts, now);
        const expected = oracleState(rows, cuts.snapshotSeq, now);
        const snapshotSeq = SeqNum.make(cuts.snapshotSeq);
        const scanned = Effect.runSync(
          Stream.runCollect(
            streamKeyVersions(writerRows, [Stream.fromIterable(manifestRows)], {
              manifestLastL0Seq: SeqNum.make(cuts.lastL0Seq),
              snapshotSeq,
            }).pipe(
              Stream.map(({ versions }) =>
                Result.getOrThrow(
                  resolveVisibleRow(versions, { snapshotSeq, now, mergeOperator: concat }),
                ),
              ),
            ),
          ),
        ).flatMap((visible) => Option.toArray(visible));
        return (
          scanned.length === expected.size &&
          scanned.every(
            (row) =>
              row.value._tag === "Value" &&
              keyToHex(row.value.bytes) ===
                keyToHex(expected.get(keyToHex(row.key)) ?? Uint8Array.of()),
          )
        );
      },
      { runs: 1_000 },
    );
  });
});

describe("keyVersionsOf", () => {
  it("orders one source's versions newest first and keeps each sequence number once", () => {
    const versions = keyVersionsOf([merge(1, 1), merge(3, 3), merge(2, 2), merge(3, 9)]);
    expect(versions.map((row) => row.seq)).toEqual([3, 2, 1]);
    expect(bytesOf(resolve(versions, 3))).toEqual([1, 2, 3]);
  });
});
