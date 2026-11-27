import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  FULL_KEY_RANGE,
  initialManifest,
} from "@shaledb/lsm-core";
import { Effect, Layer, Option, type Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { DbClient, dbClientTestLayer, makeDbClient } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestStore } from "./manifest-store.ts";
import { WriterClient } from "./writer-client.ts";

/**
 * Every scan surface does a bounded amount of work per page (D125): "limit 1 over a long range of
 * deleted keys", the review's test, on the regular and the transaction scan; the sharded scan's
 * is in `packages/lsm-sharding`.
 */
const DB = DatabaseId.make("orders");
const key = (index: number) => Uint8Array.of(0x6b, index >> 8, index & 0xff);

const testLayer = dbClientTestLayer({ blockSize: 4096 });
type TestServices = Layer.Success<typeof testLayer> | DbReaderServices | Scope.Scope;
const run = <A, E>(program: Effect.Effect<A, E, TestServices>) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(testLayer),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
    Effect.runPromise,
  );

const createDatabase = Effect.flatMap(ManifestStore, (store) =>
  store.initialize(
    initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
  ),
);

/** A client whose writer counts the scan pages it answers and the most rows one page carried. */
const countingClient = Effect.gen(function* () {
  const writer = yield* WriterClient;
  const seen = { pages: 0, rows: 0 };
  const client = yield* makeDbClient.pipe(
    Effect.provideService(
      WriterClient,
      WriterClient.of({
        ...writer,
        scan: (databaseId, request) =>
          Effect.tap(writer.scan(databaseId, request), (page) =>
            Effect.sync(() => {
              seen.pages += 1;
              seen.rows += page.rows.length;
            }),
          ),
      }),
    ),
  );
  return { client, seen };
});

/** `count` keys, one live first and the rest after it, all unflushed (5 writer pages for 5,000). */
const liveThenMany = (count: number) =>
  Effect.gen(function* () {
    const db = yield* DbClient;
    for (let start = 0; start < count; start += 1000) {
      yield* db.write(DB, {
        operations: Array.from({ length: Math.min(1000, count - start) }, (_, offset) => ({
          _tag: "Put" as const,
          key: key(start + offset),
          value: Uint8Array.of(1),
          ttlMillis: Option.none(),
        })),
      });
    }
  });

describe("the transaction scan's work per page (D125)", () => {
  it("a transaction scan for one row reads one writer page, as a Latest scan does", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        yield* liveThenMany(5000);
        const { client, seen } = yield* countingClient;
        const { readSeq, manifestId } = yield* client.beginTransaction(DB);
        const latest = yield* Stream.runCollect(
          client.scan(DB, FULL_KEY_RANGE, {
            consistency: { _tag: "Latest" },
            snapshot: Option.none(),
            refuseSealed: false,
            limit: Option.some(1),
          }),
        );
        const latestPages = seen.pages;
        seen.pages = 0;
        const rows = yield* client.scanAt(DB, FULL_KEY_RANGE, readSeq, 1, manifestId);
        return { latest: latest.length, latestPages, rows: rows.length, pages: seen.pages };
      }),
    );
    expect(outcome).toEqual({ latest: 1, latestPages: 1, rows: 1, pages: 1 });
  });
});
