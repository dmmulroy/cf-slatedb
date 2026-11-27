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
import { type Row, RowValue } from "./row.ts";

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

  it("gives any prefix of the merge to a consumer that stops early, whatever each input's chunk sizes (read audit)", () => {
    // The other merge properties chunk every input by 3 and drain the merge; a scan with a limit
    // takes a prefix of inputs chunked one row at a time (the memtable) or by blocks.
    assertProperty(
      Arbitrary.all({
        writes: historyArbitrary(120),
        parts: smallInt(1, 6),
        chunks: Arbitrary.array(smallInt(1, 9), { minLength: 6, maxLength: 6 }),
        take: smallInt(0, 130),
      }),
      ({ writes, parts, chunks, take }) => {
        const rows = rowsFromWrites(writes);
        const inputs = partition(rows, parts).map((input, index) =>
          Stream.fromIterable(input).pipe(Stream.rechunk(chunks[index] ?? 1)),
        );
        const prefix = Effect.runSync(
          Stream.runCollect(mergeRowStreams(inputs).pipe(Stream.take(take))),
        );
        return rowListsEqual(prefix, sortRows(rows).slice(0, take));
      },
      { runs: 300 },
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

describe("mergeRowStreams at batch and heap edges (mutation sweep, D117)", () => {
  const row = (key: number, seq: number, value = seq): Row => ({
    key: Uint8Array.of(key >> 8, key & 0xff),
    seq: SeqNum.make(seq),
    value: RowValue.Value({ bytes: Uint8Array.of(value & 0xff) }),
    createTs: Option.none(),
    expireTs: Option.none(),
  });
  const collect = (streams: ReadonlyArray<Stream.Stream<Row>>) =>
    Array.from(Effect.runSync(Stream.runCollect(mergeRowStreams(streams))));

  it("emits every row when an input's chunk ends exactly where a 256-row batch does", () => {
    const rows = Array.from({ length: 600 }, (_, index) => row(index, 1));
    for (const size of [256, 128, 512]) {
      const merged = collect([Stream.fromIterable(rows).pipe(Stream.rechunk(size))]);
      expect(merged.length).toBe(600);
    }
  });

  it("merges 3 to 42 non-empty inputs, one row per chunk, into key order (the heap's sift-up and sift-down)", () => {
    // Seeded, so each run checks the same 200 merges. Generated histories rarely fill this many
    // inputs at once, and a heap with a wrong parent index misorders only with several cursors in it.
    let seed = 12_345;
    const next = (bound: number) => {
      seed = (Math.imul(seed, 1_103_515_245) + 12_345) >>> 0;
      return seed % bound;
    };
    for (let trial = 0; trial < 200; trial++) {
      const inputs = Array.from({ length: 3 + next(40) }, (_, source) =>
        Array.from({ length: 1 + next(6) }, () => next(65_536))
          .sort((left, right) => left - right)
          .map((key, index) => row(key, source * 10 + index + 1)),
      );
      const merged = collect(
        inputs.map((input) => Stream.fromIterable(input).pipe(Stream.rechunk(1))),
      );
      expect(merged.map((each) => keyToHex(each.key))).toEqual(
        sortRows(inputs.flat()).map((each) => keyToHex(each.key)),
      );
    }
  });

  it("keeps the earlier input's copy of a row two inputs both hold", () => {
    const merged = collect([
      Stream.fromIterable([row(1, 5, 10), row(2, 5, 10)]),
      Stream.fromIterable([row(1, 5, 20), row(2, 5, 20)]),
      Stream.fromIterable([row(1, 5, 30)]),
    ]);
    expect(merged.map((each) => (each.value._tag === "Value" ? each.value.bytes[0] : -1))).toEqual([
      10, 10,
    ]);
  });
});
