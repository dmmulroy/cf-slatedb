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
import { MERGE_OPERATORS } from "./row-resolution.ts";

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
});
