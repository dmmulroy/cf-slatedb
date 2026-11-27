import { DatabaseId, DEFAULT_DATABASE_OPTIONS, FULL_KEY_RANGE } from "@shaledb/lsm-core";
import {
  DatabaseLifecycle,
  DbClient,
  dbServicesTestLayer,
  lsmTestRuntimeContextLayer,
} from "@shaledb/lsm-cloudflare";
import { Effect, Layer, Option, Stream } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { ShardDirectory, shardDirectoryTestLayer } from "./shard-directory.ts";
import { ShardGroup, shardFor } from "./shard-map.ts";
import {
  makeShardRouter,
  SHARD_MAP_MAX_AGE_FOR_UNPROBED_READS_MS,
  ShardRouter,
  shardRouterLayerWithoutDependencies,
} from "./shard-router.ts";
import { ShardSplit, shardSplitLayerWithoutDependencies } from "./shard-split.ts";

const GROUP = ShardGroup.make("orders");
const FIRST = DatabaseId.make("orders-0");
const key = (n: number) => Uint8Array.of(n);
const read = (tag: "Latest" | "Flushed") => ({
  consistency: tag === "Latest" ? { _tag: "Latest" as const } : { _tag: "Flushed" as const },
  snapshot: Option.none(),
  refuseSealed: true,
});

const testLayer = Layer.mergeAll(
  shardRouterLayerWithoutDependencies,
  shardSplitLayerWithoutDependencies,
).pipe(
  Layer.provideMerge(shardDirectoryTestLayer),
  Layer.provideMerge(dbServicesTestLayer({ blockSize: 64 })),
);

describe("a router whose map is older than a split (D89)", () => {
  it("does not serve a sealed shard's data at Flushed after the successor has newer flushed data", async () => {
    const outcome = await Effect.gen(function* () {
      yield* (yield* DatabaseLifecycle).create(FIRST, {
        keyRange: Option.none(),
        databaseOptions: DEFAULT_DATABASE_OPTIONS,
      });
      yield* (yield* ShardDirectory).create(GROUP, FIRST);
      const writer = yield* ShardRouter;
      const client = yield* DbClient;
      // A second router (another isolate) caches the one-shard map before the split.
      const stale = yield* makeShardRouter;
      yield* writer.put(GROUP, key(1), Uint8Array.of(1));
      yield* client.flush(FIRST);
      const before = yield* stale.get(GROUP, key(1), read("Flushed"));
      // A third router, used only to scan, also caches the one-shard map.
      const staleScanner = yield* makeShardRouter;
      yield* staleScanner.get(GROUP, key(1), read("Flushed"));
      const map = yield* (yield* ShardSplit).split(GROUP, FIRST, key(8));
      // Overwrite key 1 in its successor and flush it.
      yield* writer.put(GROUP, key(1), Uint8Array.of(2));
      const successor = Option.getOrThrow(shardFor(map, key(1))).databaseId;
      yield* client.flush(successor);
      const fresh = yield* makeShardRouter;
      const latest = yield* fresh.get(GROUP, key(1), read("Latest"));
      const freshFlushed = yield* fresh.get(GROUP, key(1), read("Flushed"));
      // Past the map's age bound for reads that never ask the writer (D89).
      yield* Effect.sleep(SHARD_MAP_MAX_AGE_FOR_UNPROBED_READS_MS + 100);
      const staleFlushed = yield* stale.get(GROUP, key(1), read("Flushed"));
      const staleScan = yield* Stream.runCollect(
        staleScanner.scan(GROUP, FULL_KEY_RANGE, { ...read("Flushed"), limit: Option.none() }),
      );
      return {
        before,
        latest,
        freshFlushed,
        staleFlushed,
        staleScan: staleScan.map((row) => row.value),
      };
    }).pipe(
      Effect.scoped,
      Effect.provide(testLayer),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );
    expect(outcome.before).toEqual(Option.some(Uint8Array.of(1)));
    expect(outcome.latest).toEqual(Option.some(Uint8Array.of(2)));
    expect(outcome.freshFlushed).toEqual(Option.some(Uint8Array.of(2)));
    // The stale router must not keep reading the sealed shard: same answer as a fresh router.
    expect(outcome.staleFlushed).toEqual(outcome.freshFlushed);
    expect(outcome.staleScan).toEqual([{ _tag: "Value", bytes: Uint8Array.of(2) }]);
  }, 60_000);
});
