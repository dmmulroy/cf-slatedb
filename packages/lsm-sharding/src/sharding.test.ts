import { DatabaseId, DEFAULT_DATABASE_OPTIONS, FULL_KEY_RANGE } from "@cf-slatedb/lsm-core";
import {
  DatabaseLifecycle,
  DbClient,
  dbServicesTestLayer,
  ManifestReader,
  lsmTestRuntimeContextLayer,
} from "@cf-slatedb/lsm-cloudflare";
import { Arbitrary, Bytes, Effect, Layer, Option, Schema, Stream } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { ShardDirectory, shardDirectoryTestLayer } from "./shard-directory.ts";
import { ShardGroup, shardFor } from "./shard-map.ts";
import { ShardRouter, shardRouterLayerWithoutDependencies } from "./shard-router.ts";
import { ShardSplit, shardSplitLayerWithoutDependencies } from "./shard-split.ts";

const GROUP = ShardGroup.make("orders");
const FIRST = DatabaseId.make("orders-0");
const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));
const key = (n: number) => Uint8Array.of(n);
const latest = { consistency: { _tag: "Latest" } as const, snapshot: Option.none() };

/** Router, split, directory, and an in-memory database deployment. */
const testLayer = Layer.mergeAll(
  shardRouterLayerWithoutDependencies,
  shardSplitLayerWithoutDependencies,
).pipe(
  Layer.provideMerge(shardDirectoryTestLayer),
  Layer.provideMerge(dbServicesTestLayer({ blockSize: 64 })),
);

// The real clock: routers back off 100 ms while a split publishes.
const provide = <A, E, R>(program: Effect.Effect<A, E, R>) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(testLayer),
    Effect.provide(lsmTestRuntimeContextLayer),
  );

const createGroup = Effect.gen(function* () {
  yield* (yield* DatabaseLifecycle).create(FIRST, {
    keyRange: Option.none(),
    databaseOptions: DEFAULT_DATABASE_OPTIONS,
  });
  yield* (yield* ShardDirectory).create(GROUP, FIRST);
});

type Step =
  | { readonly _tag: "Put"; readonly key: number; readonly value: number }
  | { readonly _tag: "Delete"; readonly key: number }
  | { readonly _tag: "Split"; readonly shard: number; readonly at: number };

const stepArbitrary: Arbitrary.Arbitrary<Step> = Arbitrary.all({
  kind: smallInt(0, 9),
  key: smallInt(0, 15),
  value: smallInt(0, 255),
  shard: smallInt(0, 7),
}).pipe(
  Arbitrary.map(({ kind, key, value, shard }): Step => {
    if (kind <= 5) return { _tag: "Put", key, value };
    if (kind <= 7) return { _tag: "Delete", key };
    return { _tag: "Split", shard, at: key };
  }),
);

describe("sharding", () => {
  it("every acknowledged write stays readable through the router across any splits", async () => {
    const result = await Effect.runPromise(
      Arbitrary.checkEffect(
        Arbitrary.array(stepArbitrary, { minLength: 1, maxLength: 24 }),
        (steps) =>
          provide(
            Effect.gen(function* () {
              yield* createGroup;
              const router = yield* ShardRouter;
              const splitter = yield* ShardSplit;
              const directory = yield* ShardDirectory;
              const oracle = new Map<number, number>();
              for (const step of steps) {
                if (step._tag === "Put") {
                  yield* router.put(GROUP, key(step.key), Uint8Array.of(step.value));
                  oracle.set(step.key, step.value);
                } else if (step._tag === "Delete") {
                  yield* router.delete(GROUP, key(step.key));
                  oracle.delete(step.key);
                } else {
                  const map = yield* directory.map(GROUP);
                  const shard = map.shards[step.shard % map.shards.length];
                  if (shard === undefined) return false;
                  const at = key(step.at);
                  // Only keys strictly inside the shard can split it; others are refused.
                  const outcome = yield* Effect.result(splitter.split(GROUP, shard.databaseId, at));
                  if (
                    outcome._tag === "Failure" &&
                    outcome.failure._tag !== "ShardSplitInvalidError"
                  ) {
                    return false;
                  }
                }
              }
              for (let probe = 0; probe < 16; probe++) {
                const got = yield* router.get(GROUP, key(probe), latest);
                const want = oracle.get(probe);
                if (Option.getOrUndefined(Option.map(got, (bytes) => bytes[0])) !== want)
                  return false;
              }
              // And straight from the database the published map names (no router cache): a write
              // that reached a shard after it was handed to its successors would be missing here.
              const fresh = yield* directory.map(GROUP);
              const client = yield* DbClient;
              for (let probe = 0; probe < 16; probe++) {
                const shard = Option.getOrUndefined(shardFor(fresh, key(probe)));
                if (shard === undefined) return false;
                const got = yield* client.get(shard.databaseId, key(probe), latest);
                if (
                  Option.getOrUndefined(Option.map(got, (bytes) => bytes[0])) !== oracle.get(probe)
                )
                  return false;
              }
              // Each shard's database accepts exactly its range.
              const manifests = yield* ManifestReader;
              for (const shard of fresh.shards) {
                const { manifest } = yield* manifests.loadLatest(shard.databaseId);
                const range = Option.getOrElse(manifest.keyRange, () => FULL_KEY_RANGE);
                const same =
                  Bytes.Equivalence(range.start, shard.range.start) &&
                  Option.makeEquivalence(Bytes.Equivalence)(range.end, shard.range.end);
                if (!same) return false;
              }
              const scanned = yield* Stream.runCollect(
                router.scan(GROUP, FULL_KEY_RANGE, { ...latest, limit: Option.none() }),
              );
              const expected = [...oracle.keys()].sort((a, b) => a - b);
              return scanned.map((row) => row.key[0]).join(",") === expected.join(",");
            }),
          ),
        { runs: 40 },
      ),
    );
    const failure = Arbitrary.formatCheckFailure(result);
    if (failure !== undefined) throw new Error(failure);
  }, 120_000);

  it("refuses a batch spanning shards, and a stale router reaches the new shards", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* createGroup;
          const router = yield* ShardRouter;
          yield* router.put(GROUP, key(1), Uint8Array.of(1));
          // The router caches the one-shard map; the split happens behind its back.
          const map = yield* (yield* ShardSplit).split(GROUP, FIRST, key(8));
          yield* router.put(GROUP, key(9), Uint8Array.of(9));
          const cross = yield* Effect.flip(
            router.write(GROUP, {
              operations: [
                { _tag: "Put", key: key(1), value: Uint8Array.of(2), ttlMillis: Option.none() },
                { _tag: "Put", key: key(9), value: Uint8Array.of(3), ttlMillis: Option.none() },
              ],
            }),
          );
          const nine = yield* router.get(GROUP, key(9), latest);
          return { map, cross: cross._tag, nine };
        }),
      ),
    );
    expect(outcome.map.version).toBe(2);
    expect(outcome.map.shards.length).toBe(2);
    expect(outcome.cross).toBe("CrossShardBatchError");
    expect(outcome.nine).toEqual(Option.some(Uint8Array.of(9)));
  }, 60_000);

  it("resumes a split that stopped after sealing, with the same new databases", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* createGroup;
          const router = yield* ShardRouter;
          yield* router.put(GROUP, key(3), Uint8Array.of(3));
          const directory = yield* ShardDirectory;
          // A split that crashed right after the seal.
          const pending = yield* directory.beginSplit(GROUP, FIRST, key(5));
          yield* (yield* DatabaseLifecycle).seal(FIRST, [pending.left, pending.right]);
          const map = yield* (yield* ShardSplit).split(GROUP, FIRST, key(5));
          return { pending, map, three: yield* router.get(GROUP, key(3), latest) };
        }),
      ),
    );
    expect(outcome.map.shards.map((shard) => shard.databaseId)).toEqual([
      outcome.pending.left,
      outcome.pending.right,
    ]);
    expect(outcome.three).toEqual(Option.some(Uint8Array.of(3)));
  }, 60_000);
});
