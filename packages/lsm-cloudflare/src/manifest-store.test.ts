import {
  commitL0Flush,
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  fenceWriter,
  initialManifest,
  ManifestId,
  SeqNum,
  SstId,
  Timestamp,
  type SstHandle,
  WriterEpoch,
} from "@cf-slatedb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer, Option, Result, Stream } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestCache, manifestCacheLayerWithoutDependencies } from "./manifest-cache.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore, manifestStoreTestLayer } from "./manifest-store.ts";
import { latestManifestKey, manifestKey } from "./object-keys.ts";
import { ObjectStore, objectStoreTestLayer } from "./object-store.ts";

const DB = DatabaseId.make("orders");

const handle = (n: number): SstHandle => ({
  id: SstId.make(`01JD${String(n).padStart(22, "0")}`),
  firstKey: Uint8Array.of(n),
  lastKey: Uint8Array.of(n),
  sizeBytes: 100,
  rowCount: 1,
  metaOffset: 50,
  metaLength: 50,
  owner: Option.none(),
  view: Option.none(),
});

const flushEdit =
  (n: number, writerEpoch: WriterEpoch) => (manifest: Parameters<typeof commitL0Flush>[0]) =>
    commitL0Flush(manifest, {
      writerEpoch,
      sst: handle(n),
      lastSeq: SeqNum.make(n),
      clockTick: Timestamp.make(n),
      changelog: Option.none(),
    });

const run = <A, E>(
  effect: Effect.Effect<A, E, ManifestStore | ManifestReader | ObjectStore | RuntimeContext>,
) =>
  effect.pipe(
    Effect.provide(manifestStoreTestLayer),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.runPromise,
  );

const initialize = Effect.gen(function* () {
  const store = yield* ManifestStore;
  return yield* store.initialize(
    initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
  );
});

describe("ObjectStore test layer", () => {
  it("creates an object once under concurrent create-if-absent writes", async () => {
    const outcomes = await Effect.gen(function* () {
      const objects = yield* ObjectStore;
      return yield* Effect.forEach(
        Array.from({ length: 32 }, (_, index) => index),
        (index) => objects.putIfAbsent("db/x/manifest/1", Uint8Array.of(index)),
        { concurrency: "unbounded" },
      );
    }).pipe(
      Effect.provide(objectStoreTestLayer),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );
    expect(outcomes.filter((outcome) => outcome === "Created")).toHaveLength(1);
  });

  it("uploads uniform multipart parts, reads ranges, and pages listings", async () => {
    const result = await Effect.gen(function* () {
      const objects = yield* ObjectStore;
      const chunk = new Uint8Array(3 * 1024 * 1024).fill(7);
      const size = yield* objects.putMultipart(
        "db/x/compacted/a.sst",
        Stream.make(chunk, chunk, chunk),
        5 * 1024 * 1024,
      );
      const range = yield* objects.getRange("db/x/compacted/a.sst", {
        offset: size - 4,
        length: 10,
      });
      for (let index = 0; index < 1005; index++)
        yield* objects.put(`db/y/k${String(index).padStart(4, "0")}`, Uint8Array.of(1));
      const first = yield* objects.list("db/y/", Option.none());
      const second = yield* objects.list("db/y/", first.cursor);
      const tooSmall = yield* objects
        .putMultipart("db/x/compacted/b.sst", Stream.make(chunk, chunk), 1024)
        .pipe(Effect.exit);
      return { size, range, first, second, tooSmall };
    }).pipe(
      Effect.provide(objectStoreTestLayer),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );
    expect(result.size).toBe(9 * 1024 * 1024);
    expect(Option.map(result.range, (bytes) => bytes.length)).toEqual(Option.some(4));
    expect(result.first.objects).toHaveLength(1000);
    expect(result.second.objects).toHaveLength(5);
    expect(result.second.cursor).toEqual(Option.none());
    expect(result.tooSmall._tag).toBe("Failure");
  });
});

describe("ManifestStore", () => {
  it("creates a database once and reports a missing one", async () => {
    const { first, again, missing } = await run(
      Effect.gen(function* () {
        const first = yield* initialize;
        const again = yield* Effect.flip(initialize);
        const missing = yield* Effect.flip(
          (yield* ManifestReader).loadLatest(DatabaseId.make("absent")),
        );
        return { first, again, missing };
      }),
    );
    expect(first.id).toBe(1);
    expect(again._tag).toBe("DatabaseAlreadyExistsError");
    expect(missing._tag).toBe("DatabaseNotFoundError");
  });

  it("serializes concurrent edits through create-if-absent, losing none", async () => {
    const latest = await run(
      Effect.gen(function* () {
        yield* initialize;
        const store = yield* ManifestStore;
        yield* Effect.forEach(
          Array.from({ length: 12 }, (_, index) => index + 1),
          (n) => store.update(DB, flushEdit(n, WriterEpoch.make(0))),
          { concurrency: "unbounded" },
        );
        return yield* (yield* ManifestReader).loadLatest(DB);
      }),
    );
    expect(latest.id).toBe(13);
    expect(latest.manifest.l0.map((sst) => sst.id).sort()).toEqual(
      Array.from({ length: 12 }, (_, index) => handle(index + 1).id).sort(),
    );
  });

  it("writes nothing for an already-applied edit", async () => {
    const ids = await run(
      Effect.gen(function* () {
        yield* initialize;
        const store = yield* ManifestStore;
        const first = yield* store.update(DB, flushEdit(1, WriterEpoch.make(0)));
        const retried = yield* store.update(DB, flushEdit(1, WriterEpoch.make(0)));
        return [first.id, retried.id];
      }),
    );
    expect(ids).toEqual([2, 2]);
  });

  it("fences a stale writer: after a new epoch commits, the old writer's flush fails", async () => {
    const { fenced, epochs } = await run(
      Effect.gen(function* () {
        yield* initialize;
        const store = yield* ManifestStore;
        const writerA = yield* store.update(DB, fenceWriter);
        const writerB = yield* store.update(DB, fenceWriter);
        const fenced = yield* Effect.flip(
          store.update(DB, flushEdit(1, writerA.manifest.writerEpoch)),
        );
        yield* store.update(DB, flushEdit(2, writerB.manifest.writerEpoch));
        const latest = yield* (yield* ManifestReader).loadLatest(DB);
        return {
          fenced,
          epochs: [
            writerA.manifest.writerEpoch,
            writerB.manifest.writerEpoch,
            latest.manifest.l0.length,
          ],
        };
      }),
    );
    expect(fenced._tag).toBe("WriterFencedError");
    expect(epochs).toEqual([1, 2, 1]);
  });

  it("finds the newest manifest when the LATEST hint lags or names a collected manifest", async () => {
    const result = await run(
      Effect.gen(function* () {
        yield* initialize;
        const store = yield* ManifestStore;
        for (const n of [1, 2, 3, 4]) yield* store.update(DB, flushEdit(n, WriterEpoch.make(0)));
        const objects = yield* ObjectStore;
        const reader = yield* ManifestReader;
        yield* objects.put(latestManifestKey(DB), new TextEncoder().encode("2"));
        const lagging = yield* reader.loadLatest(DB);
        yield* objects.delete([
          manifestKey(DB, ManifestId.make(1)),
          manifestKey(DB, ManifestId.make(2)),
        ]);
        const collected = yield* reader.loadLatest(DB);
        return [lagging.id, collected.id];
      }),
    );
    expect(result).toEqual([5, 5]);
  });

  it("caches a manifest until the refresh interval passes", async () => {
    const ids = await Effect.gen(function* () {
      yield* initialize;
      const cache = yield* ManifestCache;
      const store = yield* ManifestStore;
      const first = yield* cache.current(DB);
      yield* store.update(DB, flushEdit(1, WriterEpoch.make(0)));
      const cached = yield* cache.current(DB);
      yield* TestClock.adjust("2 seconds");
      const refreshed = yield* cache.current(DB);
      return [first.id, cached.id, refreshed.id];
    }).pipe(
      Effect.provide(
        manifestCacheLayerWithoutDependencies.pipe(
          Layer.provideMerge(manifestStoreTestLayer),
          Layer.provide(lsmConfigurationTestLayer({ manifestRefreshInterval: 1000 })),
        ),
      ),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    expect(ids).toEqual([1, 1, 2]);
  });

  it("propagates an edit's own failure without writing", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* initialize;
        const store = yield* ManifestStore;
        const failure = yield* Effect.flip(
          store.update(DB, () => Result.fail("rejected" as const)),
        );
        const latest = yield* (yield* ManifestReader).loadLatest(DB);
        return { failure, id: latest.id };
      }),
    );
    expect(outcome).toEqual({ failure: "rejected", id: 1 });
  });
});
