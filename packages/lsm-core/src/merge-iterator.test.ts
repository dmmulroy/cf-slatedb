import { Arbitrary, Effect, Option, Result, Schema, Stream } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { SeqNum, Timestamp } from "./ids.ts";
import {
  assertProperty,
  historyArbitrary,
  keyToHex,
  oracleState,
  rowListsEqual,
  rowsFromWrites,
  sortRows,
} from "./lsm-core-arbitraries.ts";
import { mergeRowStreams } from "./merge-iterator.ts";
import {
  groupRowsByKey,
  MERGE_OPERATORS,
  resolveVisibleRow,
  retainKeyVersions,
} from "./row-resolution.ts";
import type { Row } from "./row.ts";

/** Splits rows round-robin-ish into `parts` sorted inputs by each row's sequence number. */
const partition = (rows: ReadonlyArray<Row>, parts: number): Array<Array<Row>> => {
  const inputs = Array.from({ length: parts }, () => new Array<Row>());
  for (const row of rows) inputs[row.seq % parts]?.push(row);
  return inputs.map(sortRows);
};

/** Streams an input in small chunks, so merging crosses chunk boundaries often. */
const chunked = (rows: ReadonlyArray<Row>) => Stream.fromIterable(rows).pipe(Stream.rechunk(3));

const runMerge = (inputs: ReadonlyArray<ReadonlyArray<Row>>) =>
  Effect.runSync(Stream.runCollect(mergeRowStreams(inputs.map(chunked))));

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

describe("mergeRowStreams", () => {
  it("merges sorted inputs into one sorted stream and drops exact duplicates", () => {
    assertProperty(
      Arbitrary.all({
        writes: historyArbitrary(150),
        parts: smallInt(1, 6),
        duplicateEvery: smallInt(2, 7),
      }),
      ({ writes, parts, duplicateEvery }) => {
        const rows = rowsFromWrites(writes);
        const inputs = partition(rows, parts);
        // Replay some rows into another input, as a crash between L0 upload and memtable truncation does.
        const replayed = rows.filter((row) => row.seq % duplicateEvery === 0);
        const withDuplicates = [...inputs, sortRows(replayed)];
        return rowListsEqual(runMerge(withDuplicates), sortRows(rows));
      },
    );
  });

  it("handles no inputs and empty inputs", () => {
    expect(runMerge([])).toEqual([]);
    expect(runMerge([[], []])).toEqual([]);
  });
});

describe("retainKeyVersions (compaction)", () => {
  it("never changes what the newest snapshot reads, at the bottom or above it", () => {
    assertProperty(
      Arbitrary.all({
        writes: historyArbitrary(150),
        nowOffset: smallInt(0, 200),
        isBottom: Arbitrary.schema(Schema.Boolean),
      }),
      ({ writes, nowOffset, isBottom }) => {
        const rows = rowsFromWrites(writes);
        const now = Timestamp.make(100 + nowOffset);
        const groups = Effect.runSync(
          Stream.runCollect(groupRowsByKey(Stream.fromIterable(sortRows(rows)))),
        );
        const compacted = groups.flatMap((versions) =>
          retainKeyVersions(versions, {
            now,
            retainAboveSeq: Option.none(),
            isBottom,
            mergeOperator: Option.some(MERGE_OPERATORS.Concat),
          }),
        );
        const expected = oracleState(rows, rows.length, now);
        // Every key the oracle sees is readable from the compacted rows, and nothing else is.
        const regrouped = Effect.runSync(
          Stream.runCollect(groupRowsByKey(Stream.fromIterable(compacted))),
        );
        const readable = new Map<string, Uint8Array>();
        for (const versions of regrouped) {
          const visible = Result.getOrThrow(
            resolveVisibleRow(versions, {
              snapshotSeq: SeqNum.make(rows.length),
              now,
              mergeOperator: Option.some(MERGE_OPERATORS.Concat),
            }),
          );
          if (Option.isSome(visible) && visible.value.value._tag === "Value") {
            readable.set(keyToHex(visible.value.key), visible.value.value.bytes);
          }
        }
        return (
          readable.size === expected.size &&
          [...expected].every(
            ([key, value]) =>
              keyToHex(readable.get(key) ?? new Uint8Array([0xde])) === keyToHex(value),
          ) &&
          // A key keeps one version per run of rows that expire together (D81): no two adjacent
          // survivors expire at the same time. No delete survives at the bottom.
          regrouped.every((versions) =>
            versions.every((version, index) => {
              const older = versions[index + 1];
              return (
                older === undefined ||
                Option.getOrUndefined(older.expireTs) !== Option.getOrUndefined(version.expireTs)
              );
            }),
          ) &&
          (!isBottom || compacted.every((row) => row.value._tag !== "Tombstone"))
        );
      },
    );
  });

  it("keeps every version newer than the retention floor, so older snapshots read the same", () => {
    assertProperty(
      Arbitrary.all({
        writes: historyArbitrary(120),
        floorFraction: smallInt(0, 100),
        snapshotFraction: smallInt(0, 100),
      }),
      ({ writes, floorFraction, snapshotFraction }) => {
        const rows = rowsFromWrites(writes);
        const floor = Math.round((rows.length * floorFraction) / 100);
        const snapshot = floor + Math.round(((rows.length - floor) * snapshotFraction) / 100);
        const now = Timestamp.make(0);
        const groups = Effect.runSync(
          Stream.runCollect(groupRowsByKey(Stream.fromIterable(sortRows(rows)))),
        );
        const compacted = groups.flatMap((versions) =>
          retainKeyVersions(versions, {
            now,
            retainAboveSeq: Option.some(SeqNum.make(floor)),
            isBottom: false,
            mergeOperator: Option.some(MERGE_OPERATORS.Concat),
          }),
        );
        if (floor === 0) return true;
        const expected = oracleState(rows, snapshot, now);
        const actual = oracleState(compacted, snapshot, now);
        return (
          expected.size === actual.size &&
          [...expected].every(
            ([key, value]) =>
              keyToHex(actual.get(key) ?? new Uint8Array([0xde])) === keyToHex(value),
          )
        );
      },
    );
  });
});
