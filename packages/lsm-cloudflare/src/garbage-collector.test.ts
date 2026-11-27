import {
  addCheckpoint,
  CheckpointId,
  commitL0Flush,
  type CompactionJob,
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  fenceWriter,
  initialManifest,
  ManifestId,
  manifestSstHandles,
  replaceManifestFields,
  SeqNum,
  SortedRunId,
  SstId,
  Timestamp,
} from "@shaledb/lsm-core";
import { Deferred, Effect, Fiber, Layer, Option, Ref, Stream } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { CompactionExecutor, makeCompactionExecutor } from "./compaction-executor.ts";
import { directStepRunner } from "./compaction-launcher.ts";
import { runCompactionJob } from "./compaction-job.ts";
import { DbClient, dbClientTestLayer } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { CompactorClientRecord, compactorClientTestLayer } from "./compactor-client.ts";
import { GarbageCollector, makeGarbageCollector } from "./garbage-collector.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import {
  MaintenanceSweep,
  maintenanceSweepLayerWithoutDependencies,
  MAX_UNCOLLECTED_MANIFESTS,
} from "./maintenance-sweep.ts";
import { decodeStoredManifest, ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import {
  compactedPrefix,
  databasePrefix,
  manifestKey,
  manifestPrefix,
  sstKey,
} from "./object-keys.ts";
import { ObjectStore, objectStoreTestLayer } from "./object-store.ts";
import { SstStore, sstStoreLayerWithoutDependencies } from "./sst-store.ts";

const DB = DatabaseId.make("orders");
const MIN_AGE = 10_000;

/** Client, writer, executor, collector, and SST store over one in-memory bucket. */
const testLayer = Layer.mergeAll(
  Layer.effect(CompactionExecutor, makeCompactionExecutor),
  Layer.effect(GarbageCollector, makeGarbageCollector),
).pipe(
  Layer.provideMerge(sstStoreLayerWithoutDependencies),
  Layer.provideMerge(dbClientTestLayer({ blockSize: 64, gcMinAge: MIN_AGE })),
);

const run = <A, E>(
  program: Effect.Effect<
    A,
    E,
    | DbClient
    | CompactionExecutor
    | GarbageCollector
    | SstStore
    | ManifestStore
    | ManifestReader
    | ObjectStore
    | DbReaderServices
  >,
) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(testLayer),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
    Effect.runPromise,
  );

const create = Effect.gen(function* () {
  yield* (yield* ManifestStore).initialize(
    initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
  );
});

const writeAndFlush = (keys: ReadonlyArray<number>) =>
  Effect.gen(function* () {
    const client = yield* DbClient;
    for (const key of keys) {
      yield* client.put(DB, Uint8Array.of(key), Uint8Array.of(key));
      yield* client.flush(DB);
    }
  });

const compactL0 = Effect.gen(function* () {
  const { manifest } = yield* (yield* ManifestReader).loadLatest(DB);
  const job: CompactionJob = {
    sources: manifest.l0.map((sst) => ({ _tag: "L0" as const, sstId: sst.id })),
    destination: SortedRunId.make(0),
    isBottom: manifest.sortedRuns.length === 0,
  };
  return yield* runCompactionJob(
    { databaseId: DB, compactorEpoch: manifest.compactorEpoch, jobId: "job", job },
    directStepRunner,
  );
});

const storedKeys = (prefix: string) =>
  Effect.gen(function* () {
    const page = yield* (yield* ObjectStore).list(prefix, Option.none());
    return page.objects.map((object) => object.key);
  });

const readAll = Effect.gen(function* () {
  const rows = yield* Stream.runCollect(
    (yield* DbClient).scan(
      DB,
      { start: new Uint8Array(0), end: Option.none() },
      {
        consistency: { _tag: "Latest" },
        snapshot: Option.none(),
        refuseSealed: false,
        limit: Option.none(),
      },
    ),
  );
  return rows.map((row) => row.key[0]);
});

/**
 * Two collections of one database, interleaved (D83). Collection X starts first and stops at
 * `gate`: the load of the manifest the compaction superseded (after X listed the manifests and
 * planned), or its first delete (after it loaded every retained manifest). Collection Y then runs
 * whole, `laterBy` ms later, and X resumes. Or, with `deleteSupersededWhileStopped`, nothing
 * collects meanwhile and the superseded manifest is deleted directly, as no collection would.
 *
 * @returns Each run's report or failure, the SST keys each deleted, and the SST keys named by
 *   the manifests each loaded (its retained manifests, and the manifests its search for the
 *   newest read).
 */
const interleavedCollections = (options: {
  readonly gate: "superseded manifest load" | "first delete";
  readonly laterBy: number;
  readonly deleteSupersededWhileStopped: boolean;
}) => {
  const phase: { current: "X" | "Y" } = { current: "X" };
  const deleted = { X: new Set<string>(), Y: new Set<string>() };
  const named = { X: new Set<string>(), Y: new Set<string>() };
  // Whether each run has listed the manifests: the loads after that are its retained manifests.
  const listed = { X: false, Y: false };
  const reached = Deferred.makeUnsafe<void>();
  const release = Deferred.makeUnsafe<void>();
  let gateKey = Option.none<string>();
  let gateOpen = true;
  const stop = (matches: boolean) =>
    matches && gateOpen && phase.current === "X"
      ? Effect.sync(() => {
          gateOpen = false;
        }).pipe(
          Effect.andThen(Deferred.succeed(reached, undefined)),
          Effect.andThen(Deferred.await(release)),
        )
      : Effect.void;
  const gatedStore = Layer.effect(
    ObjectStore,
    Effect.gen(function* () {
      const store = yield* ObjectStore;
      return ObjectStore.of({
        ...store,
        get: (key) =>
          Effect.gen(function* () {
            yield* stop(
              options.gate === "superseded manifest load" &&
                listed.X &&
                Option.contains(gateKey, key),
            );
            const body = yield* store.get(key);
            if (
              listed[phase.current] &&
              Option.isSome(body) &&
              key.startsWith(manifestPrefix(DB)) &&
              !key.endsWith("LATEST")
            ) {
              const id = Number(key.slice(manifestPrefix(DB).length));
              const manifest = yield* Effect.orDie(
                decodeStoredManifest(DB, Option.getOrThrow(decodeId(id)), body.value),
              );
              for (const sst of manifestSstHandles(manifest))
                named[phase.current].add(sstKey(DB, sst.id));
            }
            return body;
          }),
        list: (prefix, cursor) =>
          Effect.sync(() => {
            if (prefix === manifestPrefix(DB)) listed[phase.current] = true;
          }).pipe(Effect.andThen(store.list(prefix, cursor))),
        delete: (keys) =>
          Effect.gen(function* () {
            yield* stop(options.gate === "first delete");
            for (const key of keys) deleted[phase.current].add(key);
            return yield* store.delete(keys);
          }),
      });
    }),
  ).pipe(Layer.provide(objectStoreTestLayer));
  return Effect.gen(function* () {
    yield* create;
    // L0 SSTs at 0; at 5 s a compaction replaces them and supersedes the last flush's manifest.
    yield* writeAndFlush([1, 2, 3, 4]);
    yield* TestClock.adjust(5_000);
    const superseded = (yield* (yield* ManifestReader).loadLatest(DB)).id;
    yield* compactL0;
    gateKey = Option.some(manifestKey(DB, superseded));
    // X at 14 s: the superseded manifest is younger than gcMinAge (10 s) after its replacement,
    // so X retains it; the L0 SSTs it names are older than X's boundary (4 s).
    yield* TestClock.adjust(9_000);
    const gc = yield* GarbageCollector;
    const x = yield* Effect.forkChild(Effect.result(gc.collect(DB)));
    yield* Deferred.await(reached);
    phase.current = "Y";
    yield* TestClock.adjust(options.laterBy);
    const y = options.deleteSupersededWhileStopped
      ? Option.none()
      : Option.some(yield* gc.collect(DB));
    if (options.deleteSupersededWhileStopped) {
      yield* (yield* ObjectStore).delete([manifestKey(DB, superseded)]);
      deleted.Y.clear();
    }
    phase.current = "X";
    yield* Deferred.succeed(release, undefined);
    const xOutcome = yield* Fiber.join(x);
    // Every manifest still stored names only SSTs that are still stored.
    const stored = new Set(yield* storedKeys(compactedPrefix(DB)));
    const manifests = (yield* storedKeys(manifestPrefix(DB))).filter(
      (key) => !key.endsWith("LATEST"),
    );
    const dangling: Array<string> = [];
    for (const key of manifests) {
      const body = yield* (yield* ObjectStore).get(key);
      if (Option.isNone(body)) continue;
      const id = Option.getOrThrow(decodeId(Number(key.slice(manifestPrefix(DB).length))));
      const manifest = yield* Effect.orDie(decodeStoredManifest(DB, id, body.value));
      for (const sst of manifestSstHandles(manifest)) {
        if (!stored.has(sstKey(DB, sst.id))) dangling.push(sst.id);
      }
    }
    const overlap = (left: Set<string>, right: Set<string>) =>
      [...left].filter((key) => right.has(key));
    return {
      x: xOutcome,
      y,
      supersededStored: manifests.includes(manifestKey(DB, superseded)),
      deletedByX: deleted.X.size,
      deletedByY: deleted.Y.size,
      xDeletedWhatYNamed: overlap(deleted.X, named.Y).filter((key) => key.endsWith(".sst")),
      yDeletedWhatXNamed: overlap(deleted.Y, named.X).filter((key) => key.endsWith(".sst")),
      dangling,
      rows: yield* readAll,
    };
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Layer.mergeAll(
        Layer.effect(CompactionExecutor, makeCompactionExecutor),
        Layer.effect(GarbageCollector, makeGarbageCollector),
      ).pipe(
        Layer.provideMerge(sstStoreLayerWithoutDependencies),
        Layer.provideMerge(dbClientTestLayer({ blockSize: 64, gcMinAge: MIN_AGE }, gatedStore)),
      ),
    ),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
    Effect.runPromise,
  );
};

const decodeId = (id: number) =>
  Number.isSafeInteger(id) && id > 0 ? Option.some(ManifestId.make(id)) : Option.none<ManifestId>();

describe("GarbageCollector", () => {
  it("two collections interleaved at the listing: the later deletes the superseded manifest, the earlier collects without it (D83)", async () => {
    const outcome = await interleavedCollections({
      gate: "superseded manifest load",
      laterBy: 2_000,
      deleteSupersededWhileStopped: false,
    });
    expect(outcome.x._tag).toBe("Success");
    expect(outcome.supersededStored).toBe(false);
    expect(outcome.deletedByY).toBeGreaterThan(0);
    expect(outcome.xDeletedWhatYNamed).toEqual([]);
    expect(outcome.yDeletedWhatXNamed).toEqual([]);
    expect(outcome.dangling).toEqual([]);
    expect(outcome.rows).toEqual([1, 2, 3, 4]);
  });

  it("two collections interleaved at the deletes delete nothing the other's retained manifests name (D83)", async () => {
    const outcome = await interleavedCollections({
      gate: "first delete",
      laterBy: 0,
      deleteSupersededWhileStopped: false,
    });
    expect(outcome.x._tag).toBe("Success");
    expect(outcome.deletedByY).toBeGreaterThan(0);
    expect(outcome.xDeletedWhatYNamed).toEqual([]);
    expect(outcome.yDeletedWhatXNamed).toEqual([]);
    expect(outcome.dangling).toEqual([]);
    expect(outcome.rows).toEqual([1, 2, 3, 4]);
  });

  it("a retained manifest deleted above GC's boundary, not by a collection, stops the collection before it deletes anything (D83)", async () => {
    const outcome = await interleavedCollections({
      gate: "superseded manifest load",
      laterBy: 0,
      deleteSupersededWhileStopped: true,
    });
    expect(outcome.x._tag === "Failure" && outcome.x.failure._tag).toBe(
      "GarbageCollectorUnavailableError",
    );
    expect(outcome.deletedByX).toBe(0);
  });

  it("keeps young manifests and their SSTs, then collects them once older than gcMinAge", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* create;
        yield* writeAndFlush([1, 2, 3, 4]);
        const l0Keys = yield* storedKeys(compactedPrefix(DB));
        yield* compactL0;
        const gc = yield* GarbageCollector;
        const young = yield* gc.collect(DB);
        const manifestsWhileYoung = (yield* storedKeys(manifestPrefix(DB))).filter(
          (key) => !key.endsWith("/LATEST"),
        ).length;
        yield* TestClock.adjust(MIN_AGE + 1);
        const old = yield* gc.collect(DB);
        const remaining = yield* storedKeys(compactedPrefix(DB));
        const latest = yield* (yield* ManifestReader).loadLatest(DB);
        // Collecting an idle database writes no manifest.
        const idle = yield* gc.collect(DB);
        return {
          l0Keys,
          young,
          manifestsWhileYoung,
          old,
          remaining,
          latest,
          idle,
          rows: yield* readAll,
          boundary: yield* (yield* ObjectStore).get(`db/${DB}/gc/manifest.boundary`),
        };
      }),
    );
    expect(outcome.young).toMatchObject({ deletedSsts: 0, deletedManifests: 0 });
    // The four replaced L0 SSTs are gone; the compacted output (referenced) stays.
    expect(outcome.old.deletedSsts).toBe(4);
    expect(outcome.remaining.some((key) => outcome.l0Keys.includes(key))).toBe(false);
    expect(outcome.remaining).toEqual(
      manifestSstHandles(outcome.latest.manifest).map((sst) => sstKey(DB, sst.id)),
    );
    // GC's own commit supersedes the newest one, which readers may still hold: it stays.
    expect(outcome.old.deletedManifests).toBe(outcome.manifestsWhileYoung - 1);
    expect(outcome.latest.manifest.gcSstBoundary).toBe(1);
    expect(outcome.idle).toMatchObject({
      manifestId: outcome.old.manifestId,
      deletedSsts: 0,
      deletedManifests: 0,
    });
    expect(outcome.rows).toEqual([1, 2, 3, 4]);
    // The boundary is the newest deleted id: below GC's commit and the manifest it superseded.
    expect(
      Option.map(outcome.boundary, (bytes) => Number(new TextDecoder().decode(bytes))),
    ).toEqual(Option.map(outcome.old.manifestId, (id) => id - 2));
  });

  it("keeps the SSTs of a checkpointed manifest however old it is", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* create;
        yield* writeAndFlush([1, 2]);
        const store = yield* ManifestStore;
        const pinned = yield* (yield* ManifestReader).loadLatest(DB);
        yield* store.update(DB, (manifest) =>
          addCheckpoint(manifest, {
            id: CheckpointId.make("01JD00000000000000000000CP"),
            manifestId: pinned.id,
            createdAt: Timestamp.make(0),
            expireAt: Option.none(),
            holder: Option.none(),
            name: Option.some("backup"),
          }),
        );
        yield* compactL0;
        yield* TestClock.adjust(MIN_AGE + 1);
        const report = yield* (yield* GarbageCollector).collect(DB);
        const stillThere = yield* storedKeys(compactedPrefix(DB));
        return { report, pinned, stillThere };
      }),
    );
    expect(outcome.report.deletedSsts).toBe(0);
    for (const sst of manifestSstHandles(outcome.pinned.manifest)) {
      expect(outcome.stillThere).toContain(sstKey(DB, sst.id));
    }
  });

  it("collects an old orphan, and a late commit of it then fails instead of referencing a deleted SST", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* create;
        yield* writeAndFlush([1]);
        // A writer uploads an SST and stalls before committing it.
        const orphan = Option.getOrThrow(
          yield* (yield* SstStore).write({
            owner: DB,
            kind: "Compacted",
            objectKey: (id) => sstKey(DB, id),
            rows: Stream.make({
              key: Uint8Array.of(9),
              seq: SeqNum.make(99),
              value: { _tag: "Value" as const, bytes: Uint8Array.of(9) },
              createTs: Option.none(),
              expireTs: Option.none(),
            }),
          }),
        );
        yield* TestClock.adjust(MIN_AGE + 1);
        const report = yield* (yield* GarbageCollector).collect(DB);
        const late = yield* (yield* ManifestStore)
          .update(DB, (manifest) =>
            commitL0Flush(manifest, {
              writerEpoch: manifest.writerEpoch,
              sst: orphan.handle,
              lastSeq: SeqNum.make(99),
              clockTick: Timestamp.make(0),
              changelog: Option.none(),
            }),
          )
          .pipe(Effect.flip);
        return {
          report,
          late,
          orphanKey: orphan.key,
          stored: yield* storedKeys(compactedPrefix(DB)),
        };
      }),
    );
    expect(outcome.report.deletedSsts).toBe(1);
    expect(outcome.stored).not.toContain(outcome.orphanKey);
    expect(outcome.late._tag).toBe("SstBelowGcBoundaryError");
  });

  it("sweeps what a removed database left behind once it is older than gcMinAge", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const objects = yield* ObjectStore;
        // A compaction output uploaded after removal deleted everything else: no manifest remains.
        yield* objects.put(sstKey(DB, SstId.make("01JD0000000000000000000ZZZ")), Uint8Array.of(1));
        const gc = yield* GarbageCollector;
        const young = yield* gc.collect(DB);
        yield* TestClock.adjust(MIN_AGE + 1);
        const old = yield* gc.collect(DB);
        return { young, old, left: yield* storedKeys(compactedPrefix(DB)) };
      }),
    );
    expect(outcome.young).toMatchObject({ deletedSsts: 0 });
    expect(outcome.young.manifestId).toEqual(Option.none());
    expect(outcome.old).toMatchObject({ deletedSsts: 1 });
    expect(outcome.left).toEqual([]);
  });

  it("sweeps every database across paged runs, each kind handled once per cycle", async () => {
    const outcome = await Effect.gen(function* () {
      const store = yield* ManifestStore;
      const objects = yield* ObjectStore;
      const sweep = yield* MaintenanceSweep;
      const options = { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS };
      const idle = Array.from({ length: 14 }, (_, index) => DatabaseId.make(`idle-${index}`));
      for (const databaseId of idle) yield* store.initialize(initialManifest(databaseId, options));
      // A fork still reading its source, a fork that rewrote its source's SSTs but still holds it
      // (its collection releases the hold, D77), and a database whose manifests piled up.
      const holding = (databaseId: DatabaseId, checkpoint: string) =>
        replaceManifestFields(initialManifest(databaseId, options), {
          externalRefs: [
            { owner: DatabaseId.make("idle-0"), checkpointId: CheckpointId.make(checkpoint) },
          ],
        });
      const fork = DatabaseId.make("fork-a");
      yield* store.initialize(
        replaceManifestFields(holding(fork, "01JD0000000000000000000001"), {
          l0: [
            {
              id: SstId.make("01JD0000000000000000000009"),
              firstKey: Uint8Array.of(1),
              lastKey: Uint8Array.of(2),
              sizeBytes: 100,
              rowCount: 2,
              metaOffset: 0,
              metaLength: 0,
              owner: Option.some(DatabaseId.make("idle-0")),
              view: Option.none(),
            },
          ],
        }),
      );
      const rewritten = DatabaseId.make("fork-b");
      yield* store.initialize(holding(rewritten, "01JD0000000000000000000002"));
      const busy = DatabaseId.make("busy-a");
      yield* store.initialize(initialManifest(busy, options));
      for (let round = 0; round <= MAX_UNCOLLECTED_MANIFESTS; round++) {
        yield* store.update(busy, (manifest) => fenceWriter(manifest));
      }
      // A removed database's leftovers: an object and no manifest.
      const ghost = DatabaseId.make("ghost-a");
      yield* objects.put(`${compactedPrefix(ghost)}leftover.sst`, Uint8Array.of(1));
      yield* TestClock.adjust(MIN_AGE + 1);
      const pages: Array<number> = [];
      for (;;) {
        const report = yield* sweep.sweepPage(5);
        pages.push(report.visited);
        if (Option.isNone(report.cursor)) break;
      }
      const calls = yield* Ref.get(yield* CompactorClientRecord);
      const ghostLeft = yield* objects.list(databasePrefix(ghost), Option.none());
      // The next run starts over from the first database.
      const again = yield* sweep.sweepPage(5);
      return { pages, calls, ghostLeft: ghostLeft.objects.length, again: again.visited };
    }).pipe(
      Effect.provide(
        maintenanceSweepLayerWithoutDependencies.pipe(
          Layer.provideMerge(compactorClientTestLayer),
          Layer.provideMerge(testLayer),
        ),
      ),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    // 18 databases (14 idle, two forks, the busy one, the ghost) in pages of at most 5.
    expect(outcome.pages).toEqual([5, 5, 5, 3]);
    expect(outcome.calls.woken).toEqual(["fork-a"]);
    expect(outcome.calls.collectionsRequested).toEqual(["busy-a", "fork-b"]);
    expect(outcome.ghostLeft).toBe(0);
    expect(outcome.again).toBe(5);
  });
});
