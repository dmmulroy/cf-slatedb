import {
  addCheckpoint,
  CheckpointId,
  commitL0Flush,
  type CompactionJob,
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  ManifestId,
  manifestSstHandles,
  SeqNum,
  SortedRunId,
  SstId,
  Timestamp,
} from "@cf-slatedb/lsm-core";
import { Effect, Layer, Option, Stream } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { CompactionExecutor, makeCompactionExecutor } from "./compaction-executor.ts";
import { directStepRunner } from "./compaction-launcher.ts";
import { runCompactionJob } from "./compaction-job.ts";
import { DbClient, dbClientTestLayer } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { GarbageCollector, makeGarbageCollector } from "./garbage-collector.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import { compactedPrefix, manifestPrefix, sstKey } from "./object-keys.ts";
import { ObjectStore } from "./object-store.ts";
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
      { consistency: { _tag: "Latest" }, snapshot: Option.none(), limit: Option.none() },
    ),
  );
  return rows.map((row) => row.key[0]);
});

describe("GarbageCollector", () => {
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

  it("collects every database in the bucket", async () => {
    const reports = await run(
      Effect.gen(function* () {
        yield* create;
        yield* (yield* ManifestStore).initialize(
          initialManifest(DatabaseId.make("users"), {
            keyRange: Option.none(),
            databaseOptions: DEFAULT_DATABASE_OPTIONS,
          }),
        );
        return yield* (yield* GarbageCollector).collectAll;
      }),
    );
    expect(reports.map((report) => report.databaseId).sort()).toEqual(["orders", "users"]);
    expect(reports.map((report) => report.manifestId)).toEqual([
      Option.some(ManifestId.make(1)),
      Option.some(ManifestId.make(1)),
    ]);
  });
});
