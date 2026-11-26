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
} from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer, Option, Result, Stream, Tracer } from "effect";
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

  it("does not commit a stale create that reuses an id GC collected (RFC 0026 boundary)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* initialize;
        const store = yield* ManifestStore;
        const objects = yield* ObjectStore;
        // A writer read manifest 1 and stalled; others committed 2 and 3; GC collected 2.
        const staleBase = yield* (yield* ManifestReader).loadLatest(DB);
        for (const n of [1, 2]) yield* store.update(DB, flushEdit(n, WriterEpoch.make(0)));
        yield* store.advanceManifestBoundary(DB, ManifestId.make(2));
        yield* objects.delete([manifestKey(DB, ManifestId.make(2))]);
        const staleEdit = Result.getOrThrow(flushEdit(9, WriterEpoch.make(0))(staleBase.manifest));
        // Create-if-absent succeeds (the object is gone), but the boundary check refuses it.
        const stale = yield* Effect.flip(store.create(DB, ManifestId.make(2), staleEdit));
        // The boundary never moves back, and an update still commits past it.
        yield* store.advanceManifestBoundary(DB, ManifestId.make(1));
        const next = yield* store.update(DB, flushEdit(3, WriterEpoch.make(0)));
        const boundary = yield* objects.get("db/orders/gc/manifest.boundary");
        return { stale, nextId: next.id, boundary };
      }),
    );
    expect(outcome.stale._tag).toBe("ManifestConflictError");
    expect(outcome.nextId).toBe(4);
    expect(Option.map(outcome.boundary, (bytes) => new TextDecoder().decode(bytes))).toEqual(
      Option.some("2"),
    );
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

  it("refuses to read, or write over, a manifest a newer format wrote (D64)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* initialize;
        const objects = yield* ObjectStore;
        // Manifest 2, as a later shaledb would write it: format 2, with a field this one lacks.
        const current = Option.getOrThrow(yield* objects.get(manifestKey(DB, ManifestId.make(1))));
        const newer = JSON.parse(new TextDecoder().decode(current));
        newer.formatVersion = 2;
        newer.somethingNew = { introducedIn: 2 };
        yield* objects.putIfAbsent(
          manifestKey(DB, ManifestId.make(2)),
          new TextEncoder().encode(JSON.stringify(newer)),
        );
        const read = yield* Effect.flip((yield* ManifestReader).loadLatest(DB));
        const written = yield* Effect.flip(
          (yield* ManifestStore).update(DB, flushEdit(1, WriterEpoch.make(0))),
        );
        const third = yield* objects.get(manifestKey(DB, ManifestId.make(3)));
        return { read, written, third };
      }),
    );
    for (const error of [outcome.read, outcome.written]) {
      expect(error._tag).toBe("ManifestReadError");
      expect(error.message).toContain("written by a newer shaledb (format 2");
    }
    expect(outcome.third).toEqual(Option.none());
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

  it("commits from its own view with one create and no load, and through a reload from a stale one", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const created = yield* initialize;
        const store = yield* ManifestStore;
        // The span names each commit records: a fresh view must not load the newest manifest.
        const spansOf = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
          Effect.gen(function* () {
            const names: Array<string> = [];
            const recording = Tracer.make({
              span: (options) => {
                names.push(options.name);
                return new Tracer.NativeSpan(options);
              },
            });
            const value = yield* Effect.withTracer(effect, recording);
            return { value, names };
          });
        const first = yield* spansOf(store.updateFrom(created, flushEdit(1, WriterEpoch.make(0))));
        // Another committer moves on; the writer's view (manifest 2) is now stale.
        yield* store.update(DB, flushEdit(2, WriterEpoch.make(0)));
        const fromStale = yield* spansOf(
          store.updateFrom(first.value, flushEdit(3, WriterEpoch.make(0))),
        );
        return { first, fromStale };
      }),
    );
    expect(outcome.first.value.id).toBe(2);
    expect(outcome.first.names).not.toContain("ManifestReader.loadLatest");
    expect(outcome.first.names.filter((name) => name === "ManifestStore.create")).toHaveLength(1);
    expect(outcome.fromStale.value.id).toBe(4);
    expect(outcome.fromStale.names).toContain("ManifestReader.loadLatest");
    expect(outcome.fromStale.value.manifest.l0.map((sst) => sst.id)).toEqual(
      [3, 2, 1].map((n) => handle(n).id),
    );
  });

  it("reloads instead of committing when its own view's next id was collected by GC", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* initialize;
        const store = yield* ManifestStore;
        const objects = yield* ObjectStore;
        const view = yield* store.update(DB, flushEdit(1, WriterEpoch.make(0)));
        for (const n of [2, 3]) yield* store.update(DB, flushEdit(n, WriterEpoch.make(0)));
        // GC collected manifests 2 and 3; the view's next id (3) is free again.
        yield* store.advanceManifestBoundary(DB, ManifestId.make(3));
        yield* objects.delete([
          manifestKey(DB, ManifestId.make(2)),
          manifestKey(DB, ManifestId.make(3)),
        ]);
        return yield* store.updateFrom(view, flushEdit(4, WriterEpoch.make(0)));
      }),
    );
    expect(outcome.id).toBe(5);
    expect(outcome.manifest.l0).toHaveLength(4);
  });

  it("trusts only a written commit from its own view: a failure or no-op there is rechecked", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* initialize;
        const store = yield* ManifestStore;
        const view = yield* store.update(DB, flushEdit(1, WriterEpoch.make(0)));
        yield* store.update(DB, flushEdit(2, WriterEpoch.make(0)));
        // An edit whose answer depends on what the view holds: it needs two L0 SSTs.
        const needsTwo = (manifest: Parameters<typeof commitL0Flush>[0]) =>
          manifest.l0.length >= 2
            ? Result.mapError(flushEdit(3, WriterEpoch.make(0))(manifest), (error) => error._tag)
            : Result.fail("TooFew" as const);
        const afterFailure = yield* store.updateFrom(view, needsTwo);
        // An edit that is a no-op on the view but not on the newest manifest.
        const noOpBelowThree = (manifest: Parameters<typeof commitL0Flush>[0]) =>
          manifest.l0.length >= 3
            ? flushEdit(4, WriterEpoch.make(0))(manifest)
            : Result.succeed(manifest);
        const afterNoOp = yield* store.updateFrom(view, noOpBelowThree);
        return { afterFailure: afterFailure.id, afterNoOp: afterNoOp.id };
      }),
    );
    expect(outcome).toEqual({ afterFailure: 4, afterNoOp: 5 });
  });

  it("advances the LATEST hint in the background and never moves it back", async () => {
    const hints = await run(
      Effect.gen(function* () {
        yield* initialize;
        const store = yield* ManifestStore;
        const objects = yield* ObjectStore;
        const hint = Effect.gen(function* () {
          // Background work lands after the commit returns.
          yield* Effect.sleep("20 millis");
          const body = yield* objects.get(latestManifestKey(DB));
          return Option.map(body, (bytes) => new TextDecoder().decode(bytes));
        });
        yield* store.update(DB, flushEdit(1, WriterEpoch.make(0)));
        const advanced = yield* hint;
        // A hint already ahead (a later commit's write landed first) stays where it is.
        yield* objects.put(latestManifestKey(DB), new TextEncoder().encode("9"));
        yield* store.update(DB, flushEdit(2, WriterEpoch.make(0)));
        const kept = yield* hint;
        return [advanced, kept];
      }),
    );
    expect(hints).toEqual([Option.some("2"), Option.some("9")]);
  });

  it("finds the newest manifest when the hint lags below manifests GC deleted", async () => {
    const newest = await run(
      Effect.gen(function* () {
        yield* initialize;
        const store = yield* ManifestStore;
        const objects = yield* ObjectStore;
        for (const n of [1, 2, 3, 4, 5]) yield* store.update(DB, flushEdit(n, WriterEpoch.make(0)));
        // The hint's writes were lost at 2; a checkpoint keeps manifest 2 while GC deleted 3 and 4.
        yield* Effect.sleep("20 millis");
        yield* objects.put(latestManifestKey(DB), new TextEncoder().encode("2"));
        yield* store.advanceManifestBoundary(DB, ManifestId.make(4));
        yield* objects.delete([
          manifestKey(DB, ManifestId.make(3)),
          manifestKey(DB, ManifestId.make(4)),
        ]);
        return (yield* (yield* ManifestReader).loadLatest(DB)).id;
      }),
    );
    expect(newest).toBe(6);
  });
});
