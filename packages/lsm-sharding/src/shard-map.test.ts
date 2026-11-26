import { DatabaseId, keyRangeContains } from "@shaledb/lsm-core";
import { Arbitrary, Bytes, Effect, Option, Result, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  beginShardSplit,
  completeShardSplit,
  initialShardMap,
  type ShardMap,
  ShardGroup,
  shardFor,
  shardsOverlapping,
} from "./shard-map.ts";

const GROUP = ShardGroup.make("orders");
const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));
const key = (n: number) => Uint8Array.of(n);

/** Shards in key order, the first starting at the empty key, each ending where the next starts. */
const coversEveryKeyOnce = (map: ShardMap) =>
  map.shards.length > 0 &&
  (map.shards[0]?.range.start.length ?? 1) === 0 &&
  Option.isNone(map.shards[map.shards.length - 1]?.range.end ?? Option.some(key(0))) &&
  map.shards.every((shard, index) => {
    const next = map.shards[index + 1];
    return (
      next === undefined ||
      (Option.isSome(shard.range.end) && Bytes.Equivalence(shard.range.end.value, next.range.start))
    );
  });

describe("ShardMap", () => {
  it("stays an ordered, gap-free cover of every key across any sequence of splits", () => {
    const result = Effect.runSync(
      Arbitrary.checkEffect(
        Arbitrary.array(Arbitrary.all({ shard: smallInt(0, 9), at: smallInt(0, 255) }), {
          maxLength: 12,
        }),
        (splits) => {
          let map = initialShardMap(GROUP, DatabaseId.make("db-0"));
          let next = 1;
          for (const { shard, at } of splits) {
            const target = map.shards[shard % map.shards.length];
            if (target === undefined) return false;
            const split = {
              databaseId: target.databaseId,
              atKey: key(at),
              left: DatabaseId.make(`db-${next}`),
              right: DatabaseId.make(`db-${next + 1}`),
            };
            const begun = beginShardSplit(map, split);
            // A key at a shard's own start (or outside it) is refused, and changes nothing.
            if (Result.isFailure(begun)) continue;
            const done = completeShardSplit(begun.success, target.databaseId);
            if (Result.isFailure(done)) return false;
            if (done.success.version !== map.version + 1) return false;
            map = done.success;
            next += 2;
          }
          if (!coversEveryKeyOnce(map)) return false;
          for (let probe = 0; probe < 256; probe++) {
            const linear = map.shards.filter((shard) => keyRangeContains(shard.range, key(probe)));
            const found = shardFor(map, key(probe));
            if (linear.length !== 1 || Option.getOrUndefined(found) !== linear[0]) return false;
          }
          return true;
        },
        { runs: 300 },
      ),
    );
    expect(Arbitrary.formatCheckFailure(result)).toBeUndefined();
  });

  it("allows one split at a time, resumes the same split, and publishes it once", () => {
    const map = initialShardMap(GROUP, DatabaseId.make("db-0"));
    const split = {
      databaseId: DatabaseId.make("db-0"),
      atKey: key(10),
      left: DatabaseId.make("db-1"),
      right: DatabaseId.make("db-2"),
    };
    const begun = Result.getOrThrow(beginShardSplit(map, split));
    expect(Result.getOrThrow(beginShardSplit(begun, split))).toBe(begun);
    const other = beginShardSplit(begun, { ...split, atKey: key(20) });
    expect(Result.isFailure(other) && other.failure._tag).toBe("ShardSplitInProgressError");
    const done = Result.getOrThrow(completeShardSplit(begun, split.databaseId));
    expect(done.shards.map((shard) => shard.databaseId)).toEqual(["db-1", "db-2"]);
    expect(Result.getOrThrow(completeShardSplit(done, split.databaseId))).toBe(done);
    // The empty key is every map's first start: splitting there would leave an empty half.
    const edge = beginShardSplit(map, { ...split, atKey: new Uint8Array(0) });
    expect(Result.isFailure(edge) && edge.failure._tag).toBe("ShardSplitInvalidError");
    expect(
      shardsOverlapping(done, { start: key(5), end: Option.some(key(15)) }).map(
        (shard) => shard.databaseId,
      ),
    ).toEqual(["db-1", "db-2"]);
  });
});
