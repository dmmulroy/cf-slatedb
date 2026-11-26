import { Arbitrary, Effect, Option, Schema, Stream } from "effect";
import { describe, it } from "vite-plus/test";

import { compactRowStreams } from "./compaction-merge.ts";
import { Timestamp } from "./ids.ts";
import {
  assertProperty,
  keyToHex,
  narrowHistoryArbitrary,
  oracleState,
  rowsFromWrites,
  sortRows,
} from "./lsm-core-arbitraries.ts";
import { type Row, rowOrder } from "./row.ts";
import { keepReachableVersions, MERGE_OPERATORS } from "./row-resolution.ts";

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

const sameState = (left: ReadonlyMap<string, Uint8Array>, right: ReadonlyMap<string, Uint8Array>) =>
  left.size === right.size &&
  [...left].every(([key, value]) => {
    const other = right.get(key);
    return other !== undefined && keyToHex(other) === keyToHex(value);
  });

const caseArbitrary = Arbitrary.all({
  // Three keys, so each key has many versions across sources, levels, and expiries.
  writes: narrowHistoryArbitrary(60),
  // Rows at or below this index stay in an older level the compaction does not read.
  olderCount: smallInt(0, 20),
  sourceOf: Arbitrary.array(smallInt(0, 3), { minLength: 60, maxLength: 60 }),
  now: smallInt(100, 200),
  withOperator: Arbitrary.schema(Schema.Boolean),
});

/** Times at and after a compaction's `now` that cross every expiry the generator makes (D81). */
const readTimesFrom = (now: number) => [0, 1, 5, 10, 15, 25, 40, 400].map((delay) => now + delay);

const caseWithFlushesArbitrary = Arbitrary.all({
  writes: narrowHistoryArbitrary(60),
  olderCount: smallInt(0, 20),
  // Two cuts split the newer rows into three flush windows.
  cuts: Arbitrary.array(smallInt(0, 60), { minLength: 2, maxLength: 2 }),
  sourceOf: Arbitrary.array(smallInt(0, 3), { minLength: 60, maxLength: 60 }),
  now: smallInt(100, 200),
  later: smallInt(0, 60),
  withOperator: Arbitrary.schema(Schema.Boolean),
});

const compact = (
  sources: ReadonlyArray<ReadonlyArray<Row>>,
  options: { readonly now: number; readonly isBottom: boolean; readonly withOperator: boolean },
): ReadonlyArray<Row> =>
  Effect.runSync(
    Stream.runCollect(
      compactRowStreams(
        sources.map((source) => Stream.fromIterable(sortRows(source))),
        {
          now: Timestamp.make(options.now),
          isBottom: options.isBottom,
          mergeOperator: options.withOperator ? Option.some(MERGE_OPERATORS.Concat) : Option.none(),
        },
      ),
    ),
  ).flat();

describe("compactRowStreams", () => {
  it("preserves every key's visible value (oracle) over any split of inputs, with or without older data", () => {
    assertProperty(
      caseArbitrary,
      ({ writes, olderCount, sourceOf, now, withOperator }) => {
        const rows = rowsFromWrites(writes);
        const older = rows.slice(0, olderCount);
        const inputs = rows.slice(olderCount);
        // Each input row goes to one of four sources; each source is sorted, as an SST would be.
        const sources = [0, 1, 2, 3].map((source) =>
          sortRows(inputs.filter((_, index) => (sourceOf[index] ?? 0) === source)),
        );
        const isBottom = older.length === 0;
        const output = Effect.runSync(
          Stream.runCollect(
            compactRowStreams(
              sources.map((source) => Stream.fromIterable(source)),
              {
                now: Timestamp.make(now),
                isBottom,
                mergeOperator: withOperator ? Option.some(MERGE_OPERATORS.Concat) : Option.none(),
              },
            ),
          ),
        );
        const outputRows: ReadonlyArray<Row> = output.flat();
        const sorted = outputRows.every(
          (row, index) => index === 0 || rowOrder(outputRows[index - 1] ?? row, row) < 0,
        );
        const oneKeyPerGroup = output.every((versions) =>
          versions.every((row) => keyToHex(row.key) === keyToHex(versions[0].key)),
        );
        const noBottomDeletes =
          !isBottom || outputRows.every((row) => row.value._tag !== "Tombstone");
        const maxSeq = rows.length;
        return (
          sorted &&
          oneKeyPerGroup &&
          noBottomDeletes &&
          sameState(
            oracleState(rows, maxSeq, now),
            oracleState([...older, ...outputRows], maxSeq, now),
          )
        );
      },
      { runs: 400 },
    );
  });
  it("agrees with the uncompacted rows at every later time, through flushes and two compactions (D81)", () => {
    assertProperty(
      caseWithFlushesArbitrary,
      ({ writes, olderCount, cuts, sourceOf, now, later, withOperator }) => {
        const rows = rowsFromWrites(writes);
        const older = rows.slice(0, olderCount);
        const newer = rows.slice(olderCount);
        // Each flush window keeps only the versions a read can reach (D70).
        const [first = 0, second = 0] = [...cuts].sort((a, b) => a - b);
        const windows = [newer.slice(0, first), newer.slice(first, second), newer.slice(second)];
        const flushed = windows.flatMap((window) =>
          Effect.runSync(
            Stream.runCollect(keepReachableVersions(Stream.fromIterable(sortRows(window)))),
          ),
        );
        // A first compaction of the flushed rows over older data, then a later one of everything.
        const sources = [0, 1, 2, 3].map((source) =>
          flushed.filter((_, index) => (sourceOf[index] ?? 0) === source),
        );
        const once = compact(sources, { now, isBottom: older.length === 0, withOperator });
        const twiceAt = now + later;
        const twice = compact([once, older], { now: twiceAt, isBottom: true, withOperator });
        const maxSeq = rows.length;
        return (
          readTimesFrom(now).every((time) =>
            [flushed.concat(older), once.concat(older)].every((stored) =>
              sameState(oracleState(rows, maxSeq, time), oracleState(stored, maxSeq, time)),
            ),
          ) &&
          readTimesFrom(twiceAt).every((time) =>
            sameState(oracleState(rows, maxSeq, time), oracleState(twice, maxSeq, time)),
          )
        );
      },
      { runs: 400 },
    );
  });
});
