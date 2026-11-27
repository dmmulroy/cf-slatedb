import { DatabaseId, DEFAULT_DATABASE_OPTIONS, FULL_KEY_RANGE } from "@shaledb/lsm-core";
import {
  DatabaseLifecycle,
  DbClient,
  dbServicesTestLayer,
  lsmTestRuntimeContextLayer,
  makeDbClient,
  ScanPageBudget,
  WriterClient,
} from "@shaledb/lsm-cloudflare";
import { Effect, Layer, Option, Stream } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { ShardDirectory, shardDirectoryTestLayer } from "./shard-directory.ts";
import { ShardGroup } from "./shard-map.ts";
import { makeShardRouter, shardRouterLayerWithoutDependencies } from "./shard-router.ts";
import { ShardSplit, shardSplitLayerWithoutDependencies } from "./shard-split.ts";

/** The sharded scan's work per page (D125): limit 1 must not read a shard's whole range. */
const GROUP = ShardGroup.make("orders");
const FIRST = DatabaseId.make("orders-0");
const key = (index: number) => Uint8Array.of(0x6b, index >> 8, index & 0xff);

const testLayer = shardDirectoryTestLayer.pipe(
  Layer.provideMerge(dbServicesTestLayer({ blockSize: 4096 })),
);

const splitLayer = Layer.mergeAll(
  shardRouterLayerWithoutDependencies,
  shardSplitLayerWithoutDependencies,
).pipe(
  Layer.provideMerge(shardDirectoryTestLayer),
  Layer.provideMerge(dbServicesTestLayer({ blockSize: 4096 })),
);

describe("a routed scan's work per page (D125)", () => {
  it("a routed scan for one row over a long run of deletes ends at its page's budget with where to continue", async () => {
    const outcome = await Effect.gen(function* () {
      yield* (yield* DatabaseLifecycle).create(FIRST, {
        keyRange: Option.none(),
        databaseOptions: DEFAULT_DATABASE_OPTIONS,
      });
      yield* (yield* ShardDirectory).create(GROUP, FIRST);
      const db = yield* DbClient;
      for (let start = 0; start < 5000; start += 1000) {
        yield* db.write(FIRST, {
          operations: Array.from({ length: 1000 }, (_, offset) => ({
            _tag: "Put" as const,
            key: key(start + offset),
            value: Uint8Array.of(1),
            ttlMillis: Option.none(),
          })),
        });
      }
      // Every key but the last deleted, unflushed: tombstones in the writer's rows.
      for (let start = 0; start < 4999; start += 1000) {
        yield* db.write(FIRST, {
          operations: Array.from({ length: Math.min(1000, 4999 - start) }, (_, offset) => ({
            _tag: "Delete" as const,
            key: key(start + offset),
          })),
        });
      }
      const writer = yield* WriterClient;
      const seen = { pages: 0 };
      const client = yield* makeDbClient.pipe(
        Effect.provideService(
          WriterClient,
          WriterClient.of({
            ...writer,
            scan: (databaseId, request) =>
              Effect.tap(writer.scan(databaseId, request), () =>
                Effect.sync(() => void seen.pages++),
              ),
          }),
        ),
      );
      const router = yield* makeShardRouter.pipe(Effect.provideService(DbClient, client));
      const page = yield* Stream.runCollect(
        router.scan(GROUP, FULL_KEY_RANGE, {
          consistency: { _tag: "Latest" },
          snapshot: Option.none(),
          refuseSealed: true,
          limit: Option.some(1),
        }),
      ).pipe(
        Effect.map((rows) => ({ rows: rows.length, resumeFrom: false })),
        Effect.catchTag("ReaderUnavailableError", (error) =>
          Effect.succeed({ rows: 0, resumeFrom: error.resumeFrom !== undefined }),
        ),
        // A page budget of 2 KiB: a few dozen tombstones.
        Effect.provideService(ScanPageBudget, { maxBytes: 2048, shared: Option.none() }),
      );
      return { ...page, pages: seen.pages };
    }).pipe(
      Effect.scoped,
      Effect.provide(testLayer),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );
    // The page ends early, resumably, after one writer page: not a full read of the shard.
    expect(outcome).toEqual({ rows: 0, resumeFrom: true, pages: 1 });
  });
});

describe("one budget for a routed page across shards (D125)", () => {
  it("a page that spends part of its budget in one shard has only the rest in the next", async () => {
    const outcome = await Effect.gen(function* () {
      yield* (yield* DatabaseLifecycle).create(FIRST, {
        keyRange: Option.none(),
        databaseOptions: DEFAULT_DATABASE_OPTIONS,
      });
      yield* (yield* ShardDirectory).create(GROUP, FIRST);
      const db = yield* DbClient;
      // 2,000 keys written and deleted, split at key 30: the first shard's tombstones use part of
      // the page's budget, the second shard's the rest.
      for (const tag of ["Put", "Delete"] as const) {
        for (let start = 0; start < 2000; start += 1000) {
          yield* db.write(FIRST, {
            operations: Array.from({ length: 1000 }, (_, offset) =>
              tag === "Put"
                ? {
                    _tag: "Put" as const,
                    key: key(start + offset),
                    value: Uint8Array.of(1),
                    ttlMillis: Option.none(),
                  }
                : { _tag: "Delete" as const, key: key(start + offset) },
            ),
          });
        }
      }
      const resumeIndex = <R>(scan: Stream.Stream<unknown, { readonly _tag: string }, R>) =>
        Stream.runDrain(scan).pipe(
          Effect.as(-1),
          Effect.catch((error) =>
            Effect.succeed(
              error._tag === "ReaderUnavailableError" &&
                "resumeFrom" in error &&
                error.resumeFrom instanceof Uint8Array
                ? ((error.resumeFrom[1] ?? 0) << 8) | (error.resumeFrom[2] ?? 0)
                : -2,
            ),
          ),
        );
      const options = {
        consistency: { _tag: "Latest" as const },
        snapshot: Option.none(),
        refuseSealed: true,
        limit: Option.some(1),
      };
      const budget = { maxBytes: 2048, shared: Option.none() };
      // Flushed first: the split's seal flushes too, so both scans read the same SST rows.
      yield* db.flush(FIRST);
      const unsplit = yield* resumeIndex(db.scan(FIRST, FULL_KEY_RANGE, options)).pipe(
        Effect.provideService(ScanPageBudget, budget),
      );
      yield* (yield* ShardSplit).split(GROUP, FIRST, key(30));
      const router = yield* makeShardRouter;
      const routed = yield* resumeIndex(router.scan(GROUP, FULL_KEY_RANGE, options)).pipe(
        Effect.provideService(ScanPageBudget, budget),
      );
      return { unsplit, routed };
    }).pipe(
      Effect.scoped,
      Effect.provide(splitLayer),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );
    // The routed page ends where one database's page over the same rows ends: not a fresh budget
    // per shard (which would end 30 keys later, the first shard's share).
    expect(outcome.unsplit).toBeGreaterThan(30);
    expect(Math.abs(outcome.routed - outcome.unsplit)).toBeLessThanOrEqual(2);
  }, 120_000);
});
