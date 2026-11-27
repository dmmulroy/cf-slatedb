import { DatabaseId, DEFAULT_DATABASE_OPTIONS, FULL_KEY_RANGE } from "@shaledb/lsm-core";
import {
  DatabaseLifecycle,
  DbClient,
  ReaderUnavailableError,
  dbServicesTestLayer,
  ManifestReader,
  lsmTestRuntimeContextLayer,
} from "@shaledb/lsm-cloudflare";
import { Arbitrary, Bytes, Effect, Fiber, Layer, Option, Schema, Stream } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { ShardDirectory, shardDirectoryTestLayer } from "./shard-directory.ts";
import { ShardGroup, shardFor } from "./shard-map.ts";
import {
  makeShardRouter,
  ShardRouter,
  shardRouterLayerWithoutDependencies,
} from "./shard-router.ts";
import { ShardSplit, shardSplitLayerWithoutDependencies } from "./shard-split.ts";

const GROUP = ShardGroup.make("orders");
const FIRST = DatabaseId.make("orders-0");
const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));
const key = (n: number) => Uint8Array.of(n);
const latest = {
  consistency: { _tag: "Latest" } as const,
  snapshot: Option.none(),
  refuseSealed: true,
};

/** Router, split, directory, and an in-memory database deployment. */
const testLayer = Layer.mergeAll(
  shardRouterLayerWithoutDependencies,
  shardSplitLayerWithoutDependencies,
).pipe(
  Layer.provideMerge(shardDirectoryTestLayer),
  Layer.provideMerge(dbServicesTestLayer({ blockSize: 64 })),
);

// The real clock: routers wait on the directory while a split publishes.
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

  it("a router whose map went stale in another isolate still reads its writes after a split", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* createGroup;
          const writer = yield* ShardRouter;
          // A second router (another isolate) caches the one-shard map before the split.
          const reader = yield* makeShardRouter;
          yield* writer.put(GROUP, key(1), Uint8Array.of(1));
          yield* reader.get(GROUP, key(1), latest);
          yield* (yield* ShardSplit).split(GROUP, FIRST, key(8));
          // The writer learns the new map from the sealed shard; the reader never saw it.
          yield* writer.put(GROUP, key(9), Uint8Array.of(9));
          const nine = yield* reader.get(GROUP, key(9), latest);
          const scanned = yield* Stream.runCollect(
            reader.scan(GROUP, FULL_KEY_RANGE, { ...latest, limit: Option.none() }),
          );
          return { nine, scanned: scanned.map((row) => row.key[0]) };
        }),
      ),
    );
    expect(outcome.nine).toEqual(Option.some(Uint8Array.of(9)));
    expect(outcome.scanned).toEqual([1, 9]);
  }, 60_000);

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

  it("a split under concurrent writes loses no acknowledged write", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* createGroup;
          const router = yield* ShardRouter;
          const acked: Array<number> = [];
          // A writer keeps writing through the router while the shard is split under it.
          const writer = yield* Effect.forkChild(
            Effect.forEach(
              Array.from({ length: 60 }, (_, index) => index),
              (n) =>
                router.put(GROUP, key(n), Uint8Array.of(n)).pipe(
                  Effect.tap(() => Effect.sync(() => acked.push(n))),
                  Effect.andThen(Effect.yieldNow),
                ),
              { discard: true },
            ),
          );
          yield* Effect.sleep("5 millis");
          const beforeSplit = acked.length;
          const map = yield* (yield* ShardSplit).split(GROUP, FIRST, key(30));
          yield* Fiber.join(writer);
          // Read straight from the shard the published map names, without the router's cache.
          const client = yield* DbClient;
          const missing: Array<number> = [];
          for (const n of acked) {
            const shard = Option.getOrUndefined(shardFor(map, key(n)));
            const got =
              shard === undefined
                ? Option.none()
                : yield* client.get(shard.databaseId, key(n), latest);
            if (Option.getOrUndefined(Option.map(got, (bytes) => bytes[0])) !== n) missing.push(n);
          }
          return { acked: acked.length, missing, shards: map.shards.length, beforeSplit };
        }),
      ),
    );
    expect(outcome).toMatchObject({ acked: 60, missing: [], shards: 2 });
    // The writer was still writing when the split began.
    expect(outcome.beforeSplit).toBeLessThan(60);
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
          yield* (yield* DatabaseLifecycle).seal(FIRST, [pending.left, pending.right], false);
          const reader = yield* ManifestReader;
          const sealedAt = (yield* reader.loadLatest(FIRST)).id;
          const map = yield* (yield* ShardSplit).split(GROUP, FIRST, key(5));
          const after = yield* reader.loadLatest(FIRST);
          const sealedChain = {
            holders: after.manifest.checkpoints.flatMap((checkpoint) =>
              Option.toArray(checkpoint.holder),
            ),
            commitsAfterFirstSeal: after.id - sealedAt,
          };
          return { pending, map, sealedChain, three: yield* router.get(GROUP, key(3), latest) };
        }),
      ),
    );
    expect(outcome.map.shards.map((shard) => shard.databaseId)).toEqual([
      outcome.pending.left,
      outcome.pending.right,
    ]);
    expect(outcome.three).toEqual(Option.some(Uint8Array.of(3)));
    // The resumed seal took both holds in one commit, and the forks needed no commit on the shard.
    expect(outcome.sealedChain).toEqual({
      holders: [outcome.pending.left, outcome.pending.right],
      commitsAfterFirstSeal: 1,
    });
  }, 60_000);

  it("answers a wait for a newer map at once, on publish, on a failed split, or on timeout", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* createGroup;
          const directory = yield* ShardDirectory;
          const initial = yield* directory.map(GROUP);
          const idle = yield* directory.awaitMap(GROUP, initial.version, 5_000);
          const stale = yield* directory.awaitMap(GROUP, initial.version - 1, 5_000);
          yield* directory.beginSplit(GROUP, FIRST, key(5));
          const timedOut = yield* directory.awaitMap(GROUP, initial.version, 50);
          // A waiter released by a failure report, then one released by the publish.
          const failed = yield* Effect.forkChild(directory.awaitMap(GROUP, initial.version, 5_000));
          yield* Effect.sleep("20 millis");
          yield* directory.reportSplitFailed(GROUP, FIRST);
          const published = yield* Effect.forkChild(
            directory.awaitMap(GROUP, initial.version, 20_000),
          );
          yield* Effect.sleep("20 millis");
          const map = yield* (yield* ShardSplit).split(GROUP, FIRST, key(5));
          const publishedAt = Date.now();
          const releasedAfterMs = yield* Effect.map(
            Fiber.join(published),
            () => Date.now() - publishedAt,
          );
          return {
            releasedAfterMs,
            idle: idle.outcome,
            stale: stale.outcome,
            timedOut: timedOut.outcome,
            failed: (yield* Fiber.join(failed)).outcome,
            published: yield* Fiber.join(published),
            version: map.version,
          };
        }),
      ),
    );
    expect(outcome).toMatchObject({
      idle: "NoSplitPending",
      stale: "Newer",
      timedOut: "TimedOut",
      failed: "SplitFailed",
    });
    expect(outcome.published.outcome).toBe("Newer");
    // Released by the publish itself, not by its timeout.
    expect(outcome.releasedAfterMs).toBeLessThan(2_000);
    expect(outcome.published.map.version).toBe(outcome.version);
  }, 60_000);

  it("routes a write held by a split to the new shard as soon as it publishes", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* createGroup;
          const router = yield* ShardRouter;
          const directory = yield* ShardDirectory;
          yield* router.put(GROUP, key(3), Uint8Array.of(3));
          // The split is under way: the shard is sealed and the new map not yet published.
          const pending = yield* directory.beginSplit(GROUP, FIRST, key(5));
          yield* (yield* DatabaseLifecycle).seal(FIRST, [pending.left, pending.right], false);
          const held = yield* Effect.forkChild(router.put(GROUP, key(9), Uint8Array.of(9)));
          yield* Effect.sleep("50 millis");
          yield* (yield* ShardSplit).split(GROUP, FIRST, key(5));
          const splitAt = Date.now();
          const receipt = yield* Fiber.join(held);
          const heldAfterMs = Date.now() - splitAt;
          // A split that failed part-way fails a held write at once (retryable), not after 30 s.
          yield* router.put(GROUP, key(1), Uint8Array.of(1));
          const leftShard = receipt.databaseId === pending.right ? pending.left : pending.right;
          const second = yield* directory.beginSplit(GROUP, leftShard, key(2));
          yield* (yield* DatabaseLifecycle).seal(leftShard, [second.left, second.right], false);
          const started = Date.now();
          const refusedPut = yield* Effect.forkChild(
            Effect.flip(router.put(GROUP, key(1), Uint8Array.of(2))),
          );
          yield* Effect.sleep("50 millis");
          yield* directory.reportSplitFailed(GROUP, leftShard);
          const refused = yield* Fiber.join(refusedPut);
          return {
            receipt,
            heldAfterMs,
            right: pending.right,
            refused: refused._tag,
            waitedMs: Date.now() - started,
          };
        }),
      ),
    );
    expect(outcome.receipt.databaseId).toBe(outcome.right);
    expect(outcome.heldAfterMs).toBeLessThan(2_000);
    expect(outcome.refused).toBe("DatabaseSealedError");
    expect(outcome.waitedMs).toBeLessThan(5_000);
  }, 60_000);
});

describe("sharded scans resume a scan overtaken after it returned rows (D93)", () => {
  it("resumes from resumeFrom in the same shard and goes on across the shard boundary", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* createGroup;
          const router = yield* ShardRouter;
          for (let n = 0; n < 12; n++) yield* router.put(GROUP, key(n), key(n));
          yield* (yield* ShardSplit).split(GROUP, FIRST, key(6));
          // The first shard's scan returns keys 0..2, then fails as overtaken, naming key 3.
          const client = yield* DbClient;
          let failed = false;
          const overtakingClient = DbClient.of({
            ...client,
            scan: (databaseId, range, options) =>
              failed || range.start.length > 0
                ? client.scan(databaseId, range, options)
                : Stream.concat(
                    client.scan(
                      databaseId,
                      { start: range.start, end: Option.some(key(3)) },
                      options,
                    ),
                    Stream.suspend(() => {
                      failed = true;
                      return Stream.fail(
                        new ReaderUnavailableError({
                          databaseId,
                          reason: "StorageUnavailable",
                          message: "overtaken by a flush after returning rows",
                          resumeFrom: key(3),
                        }),
                      );
                    }),
                  ),
          });
          const resuming = yield* makeShardRouter.pipe(
            Effect.provideService(DbClient, overtakingClient),
          );
          const rows = yield* Stream.runCollect(
            resuming.scan(GROUP, FULL_KEY_RANGE, { ...latest, limit: Option.none() }),
          );
          return { failed, keys: [...rows].map((row) => row.key[0]) };
        }),
      ),
    );
    expect(outcome).toEqual({ failed: true, keys: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] });
  });
});
