import { DatabaseId, DEFAULT_DATABASE_OPTIONS, FULL_KEY_RANGE } from "@shaledb/lsm-core";
import {
  DatabaseLifecycle,
  dbServicesTestLayer,
  lsmTestRuntimeContextLayer,
  WriterUnavailableError,
} from "@shaledb/lsm-cloudflare";
import { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Exit, Layer, Option, Ref, type Scope, Stream } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { ShardDirectory, shardDirectoryTestLayer } from "./shard-directory.ts";
import { ShardGroup } from "./shard-map.ts";
import {
  makeShardRouter,
  ShardRouter,
  shardRouterLayerWithoutDependencies,
} from "./shard-router.ts";
import {
  makeShardSplit,
  ShardSplit,
  shardSplitLayerWithoutDependencies,
  withAbandonedSplitFinished,
} from "./shard-split.ts";

const GROUP = ShardGroup.make("orders");
const FIRST = DatabaseId.make("orders-0");
const key = (n: number) => Uint8Array.of(n);
const latest = {
  consistency: { _tag: "Latest" as const },
  snapshot: Option.none(),
  refuseSealed: true,
};

/** The step of a split a fault stops it at. */
type Step = "seal" | "left fork" | "right fork" | "none";

const testLayer = Layer.mergeAll(
  shardRouterLayerWithoutDependencies,
  shardSplitLayerWithoutDependencies,
).pipe(
  Layer.provideMerge(shardDirectoryTestLayer),
  Layer.provideMerge(dbServicesTestLayer({ blockSize: 64 })),
);

const run = <A, E>(
  program: Effect.Effect<A, E, Layer.Success<typeof testLayer> | RuntimeContext | Scope.Scope>,
) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(testLayer),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.runPromise,
  );

/**
 * A split whose lifecycle fails at `step` once (as a storage failure would, after the steps before
 * it completed), built over the same deployment.
 */
const faultySplit = (step: Step) =>
  Effect.gen(function* () {
    const real = yield* DatabaseLifecycle;
    const forks = yield* Ref.make(0);
    const fault = new WriterUnavailableError({
      databaseId: FIRST,
      operation: step,
      reason: "StorageUnavailable",
      message: `injected at ${step}`,
    });
    const lifecycle = DatabaseLifecycle.of({
      ...real,
      seal: (...args) => (step === "seal" ? Effect.fail(fault) : real.seal(...args)),
      forkRange: (request) =>
        Effect.gen(function* () {
          const index = yield* Ref.getAndUpdate(forks, (count) => count + 1);
          const target = request.range.end._tag === "Some" ? "left fork" : "right fork";
          if (step === target && index < 2) return yield* Effect.fail(fault);
          return yield* real.forkRange(request);
        }),
    });
    return yield* makeShardSplit.pipe(Effect.provideService(DatabaseLifecycle, lifecycle));
  });

const setUp = Effect.gen(function* () {
  yield* (yield* DatabaseLifecycle).create(FIRST, {
    keyRange: Option.none(),
    databaseOptions: DEFAULT_DATABASE_OPTIONS,
  });
  yield* (yield* ShardDirectory).create(GROUP, FIRST);
  const router = yield* ShardRouter;
  for (const n of [1, 4, 6, 9]) yield* router.put(GROUP, key(n), Uint8Array.of(n));
});

const everyKey = (router: { readonly scan: ShardRouter["Service"]["scan"] }) =>
  Stream.runCollect(router.scan(GROUP, FULL_KEY_RANGE, { ...latest, limit: Option.none() })).pipe(
    Effect.map((rows) => rows.map((row) => row.key[0])),
  );

describe("a split that dies at each step (D101)", () => {
  for (const step of ["seal", "left fork", "right fork"] as const) {
    it(`dies at the ${step}: a retry finishes it with the same databases; another split is refused naming it; no write is lost`, async () => {
      const outcome = await run(
        Effect.gen(function* () {
          yield* setUp;
          const died = yield* Effect.exit((yield* faultySplit(step)).split(GROUP, FIRST, key(5)));
          const pending = (yield* (yield* ShardDirectory).map(GROUP)).pendingSplit;
          const other = yield* Effect.exit((yield* ShardSplit).split(GROUP, FIRST, key(7)));
          const map = yield* (yield* ShardSplit).split(GROUP, FIRST, key(5));
          const router = yield* ShardRouter;
          yield* router.put(GROUP, key(2), Uint8Array.of(2));
          yield* router.put(GROUP, key(8), Uint8Array.of(8));
          const fresh = yield* makeShardRouter;
          return { died, pending, other, map, keys: yield* everyKey(fresh) };
        }),
      );
      expect(Exit.isFailure(outcome.died)).toBe(true);
      expect(Option.isSome(outcome.pending)).toBe(true);
      expect(String(outcome.other)).toContain("ShardSplitInProgressError");
      const pending = Option.getOrThrow(outcome.pending);
      expect(outcome.map.shards.map((shard) => shard.databaseId)).toEqual([
        pending.left,
        pending.right,
      ]);
      expect(Option.isNone(outcome.map.pendingSplit)).toBe(true);
      expect(outcome.keys).toEqual([1, 2, 4, 6, 8, 9]);
    }, 60_000);
  }
});

describe("two splits of one shard at once (D101)", () => {
  it("the same split twice at once publishes one map with the same two databases", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* setUp;
        const splitter = yield* ShardSplit;
        const both = yield* Effect.all(
          [
            Effect.exit(splitter.split(GROUP, FIRST, key(5))),
            Effect.exit((yield* makeShardSplit).split(GROUP, FIRST, key(5))),
          ],
          { concurrency: 2 },
        );
        const map = yield* (yield* ShardDirectory).map(GROUP);
        return { both, map, keys: yield* everyKey(yield* makeShardRouter) };
      }),
    );
    expect(outcome.map.version).toBe(2);
    expect(outcome.map.shards).toHaveLength(2);
    expect(outcome.both.some(Exit.isSuccess)).toBe(true);
    expect(outcome.keys).toEqual([1, 4, 6, 9]);
  }, 60_000);

  it("two different splits of one shard at once: one publishes, the other is refused, no write lost", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* setUp;
        const both = yield* Effect.all(
          [
            Effect.exit((yield* ShardSplit).split(GROUP, FIRST, key(5))),
            Effect.exit((yield* makeShardSplit).split(GROUP, FIRST, key(7))),
          ],
          { concurrency: 2 },
        );
        const map = yield* (yield* ShardDirectory).map(GROUP);
        return { both, map, keys: yield* everyKey(yield* makeShardRouter) };
      }),
    );
    expect(outcome.both.filter(Exit.isSuccess)).toHaveLength(1);
    expect(String(outcome.both.find(Exit.isFailure))).toMatch(
      /ShardSplitInProgressError|ShardSplitInvalidError/,
    );
    expect(outcome.map.version).toBe(2);
    expect(outcome.keys).toEqual([1, 4, 6, 9]);
  }, 60_000);
});

describe("a router whose map is two generations old (D101)", () => {
  it("reads and writes every key after two splits it never saw", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* setUp;
        const stale = yield* makeShardRouter;
        yield* stale.get(GROUP, key(1), latest);
        const first = yield* (yield* ShardSplit).split(GROUP, FIRST, key(5));
        const right = first.shards[1]?.databaseId ?? FIRST;
        yield* (yield* ShardSplit).split(GROUP, right, key(8));
        yield* stale.put(GROUP, key(9), Uint8Array.of(99));
        const nine = yield* stale.get(GROUP, key(9), latest);
        return {
          nine,
          keys: yield* everyKey(stale),
          version: (yield* stale.refresh(GROUP)).version,
        };
      }),
    );
    expect(outcome.version).toBe(3);
    expect(outcome.nine).toEqual(Option.some(Uint8Array.of(99)));
    expect(outcome.keys).toEqual([1, 4, 6, 9]);
  }, 60_000);
});

describe("an abandoned split is finished by the next request that waits for it (D101)", () => {
  it("a put to a sealed shard whose split nobody retried waits out the routing wait, finishes the split, and succeeds", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* setUp;
        // A split that died right after its seal, and was never retried.
        const pending = yield* (yield* ShardDirectory).beginSplit(GROUP, FIRST, key(5));
        yield* (yield* DatabaseLifecycle).seal(FIRST, [pending.left, pending.right], true);
        const router = yield* ShardRouter;
        const receipt = yield* Effect.exit(
          withAbandonedSplitFinished(yield* ShardSplit, GROUP, () =>
            router.put(GROUP, key(7), Uint8Array.of(7)),
          ),
        );
        const map = yield* (yield* ShardDirectory).map(GROUP);
        const seven = yield* (yield* makeShardRouter).get(GROUP, key(7), latest);
        return { receipt, map, pending, seven };
      }),
    );
    expect(Exit.isSuccess(outcome.receipt), String(outcome.receipt)).toBe(true);
    expect(Option.isNone(outcome.map.pendingSplit)).toBe(true);
    expect(outcome.map.shards.map((shard) => shard.databaseId)).toEqual([
      outcome.pending.left,
      outcome.pending.right,
    ]);
    expect(outcome.seven).toEqual(Option.some(Uint8Array.of(7)));
  }, 120_000);
});
