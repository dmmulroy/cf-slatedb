import { DatabaseId, DEFAULT_DATABASE_OPTIONS, type Row } from "@shaledb/lsm-core";
import {
  DatabaseLifecycle,
  DbClient,
  dbServicesTestLayer,
  lsmTestRuntimeContextLayer,
  ScanBudget,
  ScanPageBudget,
} from "@shaledb/lsm-cloudflare";
import { Bytes, Cause, Clock, Effect, Exit, Layer, Option, Stream } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { ShardDirectory, shardDirectoryTestLayer } from "./shard-directory.ts";
import { ShardGroup, shardFor } from "./shard-map.ts";
import { ShardRouter, shardRouterLayerWithoutDependencies } from "./shard-router.ts";
import { ShardSplit, shardSplitLayerWithoutDependencies } from "./shard-split.ts";

/**
 * Routed scan pages against an oracle (D125): random runs of deletes and expiring values, random
 * limits and page budgets, and writes, flushes, and splits between pages. A routed scan is not
 * one snapshot across shards, so each page is checked against the live keys of `[start, next)` at
 * that page's time: no key skipped or repeated across pages and shards, and each page inside its
 * budget (plus the key it finishes).
 */
const GROUP = ShardGroup.make("orders");
const FIRST = DatabaseId.make("orders-0");
const KEYS = 120;
const key = (index: number) => Uint8Array.of(0x6b, index);
const indexOf = (bytes: Uint8Array) => bytes[1] ?? -1;
const ONE_KEY_SLACK = 4096;

const testLayer = Layer.mergeAll(
  shardRouterLayerWithoutDependencies,
  shardSplitLayerWithoutDependencies,
).pipe(
  Layer.provideMerge(shardDirectoryTestLayer),
  Layer.provideMerge(dbServicesTestLayer({ blockSize: 256 })),
);

const generator = (seed: number) => {
  let state = seed;
  const next = () => {
    state = (state * 1103515245 + 12345) & 0x7fffffff;
    return state / 0x7fffffff;
  };
  return { int: (below: number) => Math.floor(next() * below), chance: (p: number) => next() < p };
};

type Oracle = Map<number, { value: number; expireAt: Option.Option<number> }>;
const liveIn = (oracle: Oracle, now: number, from: number, to: number) =>
  [...oracle.entries()]
    .filter(
      ([index, entry]) =>
        index >= from && index < to && !Option.exists(entry.expireAt, (at) => at <= now),
    )
    .map(([index, entry]) => [index, entry.value] as const)
    .sort((left, right) => left[0] - right[0]);

describe("routed scan pages against the oracle (D125)", () => {
  it("yield exactly the live keys of each page's span, across shards and splits, each inside its budget", async () => {
    let earlyEnds = 0;
    let splits = 0;
    for (let seed = 1; seed <= 12; seed++) {
      const random = generator(seed);
      const outcome = await Effect.gen(function* () {
        yield* (yield* DatabaseLifecycle).create(FIRST, {
          keyRange: Option.none(),
          databaseOptions: DEFAULT_DATABASE_OPTIONS,
        });
        yield* (yield* ShardDirectory).create(GROUP, FIRST);
        const router = yield* ShardRouter;
        const db = yield* DbClient;
        const oracle: Oracle = new Map();
        let version = 0;
        const history = Effect.gen(function* () {
          const kind = random.int(9);
          const now = yield* Clock.currentTimeMillis;
          const from = random.int(KEYS);
          const indexes = Array.from({ length: 1 + random.int(20) }, (_, at) => from + at).filter(
            (index) => index < KEYS,
          );
          if (kind <= 2) {
            for (const index of indexes) {
              yield* router.delete(GROUP, key(index));
              oracle.delete(index);
            }
          } else if (kind <= 5) {
            const value = ++version & 0xff;
            const expiring = kind === 5;
            for (const index of indexes) {
              const databaseId = Option.getOrThrow(
                shardFor(yield* router.refresh(GROUP), key(index)),
              ).databaseId;
              yield* db.write(databaseId, {
                operations: [
                  {
                    _tag: "Put",
                    key: key(index),
                    value: Uint8Array.of(value),
                    ttlMillis: expiring ? Option.some(1000) : Option.none(),
                  },
                ],
              });
              oracle.set(index, {
                value,
                expireAt: expiring ? Option.some(now + 1000) : Option.none(),
              });
            }
          } else if (kind === 6) {
            for (const shard of (yield* router.refresh(GROUP)).shards)
              yield* db.flush(shard.databaseId);
          } else if (kind === 7) {
            yield* TestClock.adjust("2 seconds");
          } else {
            // A split of the shard holding a random key, at that key.
            const at = 1 + random.int(KEYS - 1);
            const map = yield* router.refresh(GROUP);
            const shard = Option.getOrThrow(shardFor(map, key(at)));
            if (shard.range.start.length === 0 || Bytes.Order(shard.range.start, key(at)) < 0) {
              yield* Effect.ignore((yield* ShardSplit).split(GROUP, shard.databaseId, key(at)));
            }
          }
        });
        for (let index = 0; index < 20; index++) yield* history;
        const failures: Array<string> = [];
        let early = 0;
        let start: Uint8Array = new Uint8Array(0);
        for (let pages = 0; pages < 400; pages++) {
          const limit = 1 + random.int(5);
          const maxBytes = 200 + random.int(800);
          const budget = new ScanBudget(maxBytes);
          const rows: Array<Row> = [];
          const exit = yield* Effect.exit(
            Stream.runForEach(
              router.scan(
                GROUP,
                { start, end: Option.none() },
                {
                  consistency: { _tag: "Latest" },
                  snapshot: Option.none(),
                  refuseSealed: true,
                  limit: Option.some(limit + 1),
                },
              ),
              (row) => Effect.sync(() => void rows.push(row)),
            ).pipe(
              Effect.provideService(ScanPageBudget, { maxBytes, shared: Option.some(budget) }),
            ),
          );
          let resumeFrom = Option.none<Uint8Array>();
          if (Exit.isFailure(exit)) {
            const failure = Cause.findErrorOption(exit.cause);
            if (
              Option.isSome(failure) &&
              failure.value._tag === "ReaderUnavailableError" &&
              failure.value.resumeFrom !== undefined
            ) {
              resumeFrom = Option.some(failure.value.resumeFrom);
            } else return yield* Effect.failCause(exit.cause);
          } else {
            const next = rows[limit];
            if (next !== undefined) resumeFrom = Option.some(next.key);
          }
          const pageRows = Exit.isFailure(exit) ? rows : rows.slice(0, limit);
          const now = yield* Clock.currentTimeMillis;
          const from = start.length === 0 ? 0 : indexOf(start);
          const to = Option.match(resumeFrom, { onNone: () => KEYS, onSome: indexOf });
          const got = pageRows.map((row) => [
            indexOf(row.key),
            row.value._tag === "Value" ? (row.value.bytes[0] ?? -1) : -1,
          ]);
          const want = liveIn(oracle, now, from, to);
          if (JSON.stringify(got) !== JSON.stringify(want)) {
            failures.push(
              `seed ${seed} page ${pages}: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`,
            );
          }
          if (budget.examinedBytes > maxBytes + ONE_KEY_SLACK) {
            failures.push(
              `seed ${seed} page ${pages}: examined ${budget.examinedBytes} of ${maxBytes}`,
            );
          }
          if (Option.isSome(resumeFrom) && pageRows.length < limit) early++;
          if (Option.isNone(resumeFrom)) break;
          start = resumeFrom.value;
          if (random.chance(0.4)) yield* history;
        }
        const shards = (yield* router.refresh(GROUP)).shards.length;
        return { failures, early, shards };
      }).pipe(
        Effect.scoped,
        Effect.provide(testLayer),
        Effect.provide(lsmTestRuntimeContextLayer),
        Effect.provide(TestClock.layer()),
        Effect.runPromise,
      );
      expect(outcome.failures, `seed ${seed}`).toEqual([]);
      earlyEnds += outcome.early;
      splits += outcome.shards - 1;
    }
    expect(earlyEnds).toBeGreaterThan(10);
    // Pages crossed shard boundaries made by splits between pages.
    expect(splits).toBeGreaterThan(5);
  }, 600_000);
});
