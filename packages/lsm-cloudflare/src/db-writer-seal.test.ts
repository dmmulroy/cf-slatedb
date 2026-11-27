/**
 * DbWriter.seal's fencing and successor holds (mutation sweep, D127): a fenced writer never seals,
 * a retried seal with holds answers with the holds it recorded, and one without holds records none.
 */
import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  type WriteBatch,
} from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, Option } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { backgroundWorkTestLayer } from "./background-work.ts";
import { compactionTriggerTestLayer } from "./compaction-trigger.ts";
import { DbWriter, dbWriterLayerWithoutDependencies, type IDbWriter } from "./db-writer.ts";
import { durableObjectAlarmTestLayer } from "./durable-object-alarm.ts";
import { lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore, manifestStoreTestLayer } from "./manifest-store.ts";
import { memTableLayerWithoutDependencies } from "./mem-table.ts";
import { ObjectStore } from "./object-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";
import { writerSqlStorageTestLayer } from "./writer-sql-storage.ts";

const DB = DatabaseId.make("orders");
const LEFT = DatabaseId.make("orders-a");
const RIGHT = DatabaseId.make("orders-b");

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

/** Runs `body` with a way to start writers, each with its own SQLite, over one bucket. */
const withWriters = <A, E>(
  body: (
    newWriter: () => Effect.Effect<IDbWriter>,
  ) => Effect.Effect<A, E, ManifestStore | ManifestReader | ObjectStore | RuntimeContext>,
) =>
  Effect.gen(function* () {
    yield* (yield* ManifestStore).initialize(
      initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
    );
    const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
    const scope = yield* Effect.scope;
    const newWriter = () =>
      Layer.buildWithScope(
        dbWriterLayerWithoutDependencies.pipe(
          Layer.provide(sstStoreLayerWithoutDependencies),
          Layer.provide(
            memTableLayerWithoutDependencies.pipe(Layer.provide(writerSqlStorageTestLayer)),
          ),
          Layer.provide([
            durableObjectAlarmTestLayer,
            compactionTriggerTestLayer,
            backgroundWorkTestLayer,
            lsmConfigurationTestLayer(),
          ]),
          Layer.provide(Layer.succeedContext(shared)),
        ),
        scope,
      ).pipe(
        Effect.map((context) => Context.get(context, DbWriter)),
        Effect.orDie,
      );
    return yield* body(newWriter);
  }).pipe(
    Effect.scoped,
    Effect.provide(manifestStoreTestLayer),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
    Effect.runPromise,
  );

const latestManifest = Effect.map(
  Effect.flatMap(ManifestReader, (reader) => reader.loadLatest(DB)),
  (latest) => latest.manifest,
);

describe("DbWriter.seal fencing and holds (D127)", () => {
  it("a writer fenced by a newer one cannot seal, even with nothing to flush", async () => {
    const outcome = await withWriters((newWriter) =>
      Effect.gen(function* () {
        const stale = yield* newWriter();
        yield* stale.write(DB, put("a", "1"));
        yield* stale.flush(DB);
        // A newer writer fences the stale one in; the stale one still believes it is ready.
        yield* (yield* newWriter()).status(DB);
        const sealed = yield* Effect.flip(stale.seal(DB, [LEFT, RIGHT], false));
        return { sealed: sealed._tag, state: (yield* latestManifest).state._tag };
      }),
    );
    expect(outcome).toEqual({ sealed: "WriterFencedError", state: "Open" });
  });

  it("a retried seal with holds answers with the recorded holds; one without holds records none", async () => {
    const outcome = await withWriters((newWriter) =>
      Effect.gen(function* () {
        const writer = yield* newWriter();
        yield* writer.write(DB, put("a", "1"));
        const first = yield* writer.seal(DB, [LEFT, RIGHT], true);
        const retried = yield* writer.seal(DB, [LEFT, RIGHT], true);
        return {
          holds: first.successorHolds.length,
          sameHolds:
            JSON.stringify(retried.successorHolds) === JSON.stringify(first.successorHolds),
        };
      }),
    );
    expect(outcome).toEqual({ holds: 2, sameHolds: true });
    const withoutHolds = await withWriters((newWriter) =>
      Effect.gen(function* () {
        const writer = yield* newWriter();
        yield* writer.write(DB, put("a", "1"));
        const sealed = yield* writer.seal(DB, [LEFT, RIGHT], false);
        const manifest = yield* latestManifest;
        return {
          returned: sealed.successorHolds.length,
          checkpoints: manifest.checkpoints.length,
          state: manifest.state._tag,
        };
      }),
    );
    expect(withoutHolds).toEqual({ returned: 0, checkpoints: 0, state: "Sealed" });
  });
});
