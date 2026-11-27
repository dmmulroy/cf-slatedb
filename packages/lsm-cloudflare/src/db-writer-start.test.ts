/**
 * The writer's start path (mutation sweep, D127): a writer restarted on its Durable Object's SQLite
 * storage must be able to flush what it holds with no further write, and a start that cannot read
 * or write the manifest must fail with the reason a client acts on.
 */
import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  type WriteBatch,
} from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, Option, Result } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { backgroundWorkTestLayer } from "./background-work.ts";
import { compactionTriggerTestLayer } from "./compaction-trigger.ts";
import { DbWriter, dbWriterLayerWithoutDependencies, dbWriterTestLayer } from "./db-writer.ts";
import { DurableObjectAlarm, durableObjectAlarmTestLayer } from "./durable-object-alarm.ts";
import { lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader, ManifestReadError } from "./manifest-reader.ts";
import { ManifestStore, manifestStoreTestLayer, ManifestWriteError } from "./manifest-store.ts";
import { MemTable, memTableLayerWithoutDependencies } from "./mem-table.ts";
import { ObjectStore } from "./object-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";
import { writerSqlStorageTestLayer } from "./writer-sql-storage.ts";

const DB = DatabaseId.make("orders");

const put = (key: string, value: string): WriteBatch => ({
  operations: [
    {
      _tag: "Put",
      key: new TextEncoder().encode(key),
      value: new TextEncoder().encode(value),
      ttlMillis: Option.none(),
    },
  ],
});

describe("DbWriter restarted with unflushed rows (D127)", () => {
  it("schedules the flush when it starts, with no further write, and the alarm flushes every row", async () => {
    const outcome = await Effect.gen(function* () {
      yield* (yield* ManifestStore).initialize(
        initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
      );
      const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
      // One alarm and one SQLite storage, kept across the restart, as a Durable Object keeps them.
      const alarm = yield* Effect.provide(DurableObjectAlarm, durableObjectAlarmTestLayer);
      const newWriter = () =>
        Layer.build(
          dbWriterLayerWithoutDependencies.pipe(
            Layer.provide(sstStoreLayerWithoutDependencies),
            Layer.provide(memTableLayerWithoutDependencies),
            Layer.provide([
              Layer.succeed(DurableObjectAlarm, alarm),
              compactionTriggerTestLayer,
              backgroundWorkTestLayer,
              lsmConfigurationTestLayer(),
            ]),
            Layer.provide(Layer.succeedContext(shared)),
          ),
        ).pipe(Effect.map((context) => Context.get(context, DbWriter)));
      yield* TestClock.setTime(10_000);
      const before = yield* newWriter();
      yield* before.write(DB, put("a", "1"));
      yield* before.write(DB, put("b", "2"));
      // The alarm fired and the object was evicted before it set the alarm again.
      yield* alarm.cancel;
      yield* TestClock.setTime(20_000);
      const after = yield* newWriter();
      // Any request starts it; a status read writes nothing.
      const started = yield* after.status(DB);
      const scheduled = yield* alarm.scheduled;
      yield* TestClock.setTime(10_000 + 60_000);
      yield* after.runAlarm;
      const flushed = yield* after.status(DB);
      return {
        unflushedAtStart: started.unflushedRows,
        scheduled,
        flushedSeq: flushed.flushedSeq,
        unflushedAfter: flushed.unflushedRows,
      };
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.merge(manifestStoreTestLayer, writerSqlStorageTestLayer)),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    // Due maxL0Age after the oldest row, which the restart read back from SQLite.
    expect(outcome).toEqual({
      unflushedAtStart: 2,
      scheduled: Option.some(70_000),
      flushedSeq: 2,
      unflushedAfter: 0,
    });
  });
});

describe("DbWriter start failures (D127)", () => {
  /** The reason a write gives when the start's fence-in fails with `failure`. */
  const reasonWhenStartFails = (failure: ManifestReadError | ManifestWriteError) =>
    Effect.gen(function* () {
      yield* (yield* ManifestStore).initialize(
        initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
      );
      const store = yield* ManifestStore;
      const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
      const context = yield* Layer.build(
        dbWriterLayerWithoutDependencies.pipe(
          Layer.provide(sstStoreLayerWithoutDependencies),
          Layer.provide(memTableLayerWithoutDependencies),
          Layer.provide([
            durableObjectAlarmTestLayer,
            compactionTriggerTestLayer,
            backgroundWorkTestLayer,
            lsmConfigurationTestLayer(),
          ]),
          Layer.provide(
            Layer.succeedContext(
              shared.pipe(
                Context.add(
                  ManifestStore,
                  ManifestStore.of({ ...store, update: () => Effect.fail(failure) }),
                ),
              ),
            ),
          ),
        ),
      );
      const exit = yield* Effect.exit(Context.get(context, DbWriter).write(DB, put("a", "1")));
      return exit._tag === "Failure" && exit.cause.reasons[0]?._tag === "Fail"
        ? `${exit.cause.reasons[0].error._tag}:${"reason" in exit.cause.reasons[0].error ? exit.cause.reasons[0].error.reason : ""}`
        : "succeeded";
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.merge(manifestStoreTestLayer, writerSqlStorageTestLayer)),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );

  it("maps an unreadable stored manifest to StoredDataInvalid and persistent contention to ManifestContention", async () => {
    const read = (reason: ManifestReadError["reason"]) =>
      new ManifestReadError({
        databaseId: DB,
        manifestId: Option.none(),
        reason,
        message: "read failed",
      });
    const write = (reason: ManifestWriteError["reason"]) =>
      new ManifestWriteError({ databaseId: DB, reason, message: "write failed" });
    expect([
      await reasonWhenStartFails(read("StoredManifestInvalid")),
      await reasonWhenStartFails(read("StorageUnavailable")),
      await reasonWhenStartFails(write("ContentionPersisted")),
      await reasonWhenStartFails(write("StorageUnavailable")),
    ]).toEqual([
      "WriterUnavailableError:StoredDataInvalid",
      "WriterUnavailableError:StorageUnavailable",
      "WriterUnavailableError:ManifestContention",
      "WriterUnavailableError:StorageUnavailable",
    ]);
  });
});

describe("DbWriter seal of a sealed database, same count of successors (D95, D127)", () => {
  it("refuses other successors of the same number, and the same successors in another order", async () => {
    const LEFT = DatabaseId.make("orders-a");
    const RIGHT = DatabaseId.make("orders-b");
    const OTHER = DatabaseId.make("orders-c");
    const outcome = await Effect.gen(function* () {
      yield* (yield* ManifestStore).initialize(
        initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
      );
      const writer = yield* DbWriter;
      yield* writer.seal(DB, [LEFT, RIGHT], false);
      const attempt = (successors: ReadonlyArray<DatabaseId>) =>
        Effect.map(Effect.result(writer.seal(DB, successors, false)), (result) =>
          Result.isSuccess(result) ? "sealed" : result.failure._tag,
        );
      return [
        yield* attempt([LEFT, OTHER]),
        yield* attempt([RIGHT, LEFT]),
        yield* attempt([LEFT, RIGHT]),
      ];
    }).pipe(
      Effect.provide(dbWriterTestLayer()),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    expect(outcome).toEqual(["DatabaseSealedError", "DatabaseSealedError", "sealed"]);
  });
});

describe("DbWriter locks (D127)", () => {
  const started = <A, E>(
    body: Effect.Effect<A, E, DbWriter | ManifestStore | ManifestReader | RuntimeContext>,
  ) =>
    Effect.gen(function* () {
      yield* (yield* ManifestStore).initialize(
        initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
      );
      return yield* body;
    }).pipe(
      Effect.provide(dbWriterTestLayer()),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );

  it("concurrent first requests fence in once (initLock), so the writer's flushes still commit", async () => {
    const outcome = await Effect.gen(function* () {
      yield* (yield* ManifestStore).initialize(
        initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
      );
      const store = yield* ManifestStore;
      const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
      // R2 answers after other fibers ran: a second request may reach the start meanwhile.
      const slowStore = ManifestStore.of({
        ...store,
        update: (databaseId, edit) =>
          Effect.flatMap(
            Effect.forEach(Array.from({ length: 20 }), () => Effect.yieldNow, { discard: true }),
            () => store.update(databaseId, edit),
          ),
      });
      const context = yield* Layer.build(
        dbWriterLayerWithoutDependencies.pipe(
          Layer.provide(sstStoreLayerWithoutDependencies),
          Layer.provide(memTableLayerWithoutDependencies),
          Layer.provide([
            durableObjectAlarmTestLayer,
            compactionTriggerTestLayer,
            backgroundWorkTestLayer,
            lsmConfigurationTestLayer(),
          ]),
          Layer.provide(Layer.succeedContext(shared.pipe(Context.add(ManifestStore, slowStore)))),
        ),
      );
      const writer = Context.get(context, DbWriter);
      // Status reads and probes start the writer under no lock but the start's own.
      yield* Effect.all(
        [
          writer.status(DB),
          writer.probe(DB, new TextEncoder().encode("a")),
          writer.status(DB),
          writer.probe(DB, new TextEncoder().encode("b")),
        ],
        { concurrency: "unbounded" },
      );
      for (const key of ["a", "b", "c", "d"]) yield* writer.write(DB, put(key, key));
      const flushed = yield* Effect.result(writer.flush(DB));
      const manifest = (yield* (yield* ManifestReader).loadLatest(DB)).manifest;
      return {
        flushed: flushed._tag,
        writerEpoch: manifest.writerEpoch,
        lastL0Seq: manifest.lastL0Seq,
      };
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.merge(manifestStoreTestLayer, writerSqlStorageTestLayer)),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    // Without the lock each request fences in: the epoch passes 1, and one start's phase is stale.
    expect(outcome).toEqual({ flushed: "Success", writerEpoch: 1, lastL0Seq: 4 });
  });

  it("concurrent flushes write the unflushed rows once (flushLock)", async () => {
    const outcome = await started(
      Effect.gen(function* () {
        const writer = yield* DbWriter;
        for (const key of ["a", "b", "c"]) yield* writer.write(DB, put(key, key));
        yield* Effect.all([writer.flush(DB), writer.flush(DB), writer.flush(DB)], {
          concurrency: "unbounded",
        });
        const manifest = (yield* (yield* ManifestReader).loadLatest(DB)).manifest;
        return { l0: manifest.l0.length, lastL0Seq: manifest.lastL0Seq };
      }),
    );
    expect(outcome).toEqual({ l0: 1, lastL0Seq: 3 });
  });
});

describe("DbWriter flushedManifestId (D58, D127)", () => {
  it("names the manifest that covers flushedSeq after a flush, and after a restart that followed a crash before recording it", async () => {
    const outcome = await Effect.gen(function* () {
      yield* (yield* ManifestStore).initialize(
        initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
      );
      const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
      const crashNext = { value: false };
      // A memtable whose next record of a flush above 0 "crashes" after the manifest commit.
      const crashingMemTable = Layer.effect(
        MemTable,
        Effect.map(MemTable, (real) =>
          MemTable.of({
            ...real,
            markFlushed: (cut, manifestId, at) =>
              cut > 0 && crashNext.value
                ? Effect.die(new Error("simulated crash before recording the flush"))
                : real.markFlushed(cut, manifestId, at),
          }),
        ),
      ).pipe(Layer.provide(memTableLayerWithoutDependencies));
      const newWriter = () =>
        Layer.build(
          dbWriterLayerWithoutDependencies.pipe(
            Layer.provide(sstStoreLayerWithoutDependencies),
            Layer.provide(crashingMemTable),
            Layer.provide([
              durableObjectAlarmTestLayer,
              compactionTriggerTestLayer,
              backgroundWorkTestLayer,
              lsmConfigurationTestLayer(),
            ]),
            Layer.provide(Layer.succeedContext(shared)),
          ),
        ).pipe(Effect.map((context) => Context.get(context, DbWriter)));
      const latestId = Effect.map(
        Effect.flatMap(ManifestReader, (reader) => reader.loadLatest(DB)),
        (latest) => latest.id,
      );
      const writer = yield* newWriter();
      yield* writer.write(DB, put("a", "1"));
      yield* writer.flush(DB);
      const afterFlush = { status: yield* writer.status(DB), latest: yield* latestId };
      yield* writer.write(DB, put("b", "2"));
      crashNext.value = true;
      yield* Effect.exit(writer.flush(DB));
      crashNext.value = false;
      // The Durable Object restarts on the same SQLite storage; the flush's commit landed.
      const restarted = yield* newWriter();
      const afterRestart = { status: yield* restarted.status(DB), latest: yield* latestId };
      return {
        afterFlush: [
          afterFlush.status.flushedSeq,
          Option.getOrNull(afterFlush.status.flushedManifestId) === afterFlush.latest,
        ],
        afterRestart: [
          afterRestart.status.flushedSeq,
          Option.getOrNull(afterRestart.status.flushedManifestId) === afterRestart.latest,
        ],
      };
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.merge(manifestStoreTestLayer, writerSqlStorageTestLayer)),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    expect(outcome).toEqual({ afterFlush: [1, true], afterRestart: [2, true] });
  });
});
