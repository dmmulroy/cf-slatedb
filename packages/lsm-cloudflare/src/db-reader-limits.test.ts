import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  FULL_KEY_RANGE,
  initialManifest,
} from "@shaledb/lsm-core";
import { Effect, Layer, Option, Stream } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { DbClient, dbClientTestLayer } from "./db-client.ts";
import { METADATA_READS_AT_ONCE } from "./db-reader.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestStore } from "./manifest-store.ts";
import { ObjectStore, objectStoreTestLayer } from "./object-store.ts";

const DB = DatabaseId.make("orders");

describe("a read's metadata fetches (read audit)", () => {
  it("fetch at most METADATA_READS_AT_ONCE SSTs' metadata at once, for a get and a scan over 20 L0 SSTs", async () => {
    const reads = { inFlight: 0, most: 0, counting: false };
    // Each ranged read stays in flight across a few scheduler turns, so concurrent ones overlap.
    const countingStore = Layer.effect(
      ObjectStore,
      Effect.gen(function* () {
        const store = yield* ObjectStore;
        return ObjectStore.of({
          ...store,
          getRange: (key, range) =>
            Effect.gen(function* () {
              if (reads.counting) {
                reads.inFlight++;
                reads.most = Math.max(reads.most, reads.inFlight);
              }
              for (let turn = 0; turn < 5; turn++) yield* Effect.yieldNow;
              if (reads.counting) reads.inFlight--;
              return yield* store.getRange(key, range);
            }),
        });
      }),
    ).pipe(Layer.provide(objectStoreTestLayer));
    const most = await Effect.gen(function* () {
      yield* (yield* ManifestStore).initialize(
        initialManifest(DB, {
          keyRange: Option.none(),
          databaseOptions: { ...DEFAULT_DATABASE_OPTIONS, mergeOperator: Option.some("Concat") },
        }),
      );
      const client = yield* DbClient;
      // 20 L0 SSTs, each holding the same key: a get reads every level's metadata.
      for (let flush = 0; flush < 20; flush++) {
        yield* client.merge(DB, Uint8Array.of(1), Uint8Array.of(flush));
        yield* client.flush(DB);
      }
      yield* TestClock.adjust(2_000);
      const flushed = {
        consistency: { _tag: "Flushed" as const },
        snapshot: Option.none(),
        refuseSealed: false,
      };
      reads.counting = true;
      yield* client.get(DB, Uint8Array.of(1), flushed);
      const afterGet = reads.most;
      reads.most = 0;
      yield* Stream.runDrain(
        client.scan(DB, FULL_KEY_RANGE, { ...flushed, limit: Option.some(5) }),
      );
      return { afterGet, afterScan: reads.most };
    }).pipe(
      Effect.scoped,
      Effect.provide(dbClientTestLayer({ blockSize: 64, l0MaxSsts: 32 }, countingStore)),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    expect(most.afterGet).toBeGreaterThan(1);
    // Eight: the bound itself (not imported, so a change to it shows here).
    expect(METADATA_READS_AT_ONCE).toBe(8);
    expect(most.afterGet).toBeLessThanOrEqual(8);
    expect(most.afterScan).toBeLessThanOrEqual(16);
  });
});
