/**
 * A writer restarted on its Durable Object's SQLite storage (mutation sweep, D127): the start path
 * alone must leave the object able to flush what it holds, with no further write.
 */
import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  type WriteBatch,
} from "@shaledb/lsm-core";
import { Context, Effect, Layer, Option } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { backgroundWorkTestLayer } from "./background-work.ts";
import { compactionTriggerTestLayer } from "./compaction-trigger.ts";
import { DbWriter, dbWriterLayerWithoutDependencies } from "./db-writer.ts";
import { DurableObjectAlarm, durableObjectAlarmTestLayer } from "./durable-object-alarm.ts";
import { lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore, manifestStoreTestLayer } from "./manifest-store.ts";
import { memTableLayerWithoutDependencies } from "./mem-table.ts";
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
