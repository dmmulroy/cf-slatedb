import type { LsmSettings } from "@cf-slatedb/lsm-core";
import { Effect, Layer } from "effect";

import { backgroundWorkTestLayer, workerBackgroundWorkLayer } from "./background-work.ts";
import { blockSourceLayer, blockSourceTestLayer } from "./block-source.ts";
import { compactorClientLayer, compactorClientTestLayer } from "./compactor-client.ts";
import { databaseLifecycleLayerWithoutDependencies } from "./database-lifecycle.ts";
import { DbApiWorker } from "./db-api-worker.ts";
import { dbClientLayerWithoutDependencies, dbClientTestLayer } from "./db-client.ts";
import { dbReaderLayer } from "./db-reader.ts";
import { lsmConfigurationLayer } from "./lsm-configuration.ts";
import { manifestReaderLayerWithoutDependencies } from "./manifest-reader.ts";
import { manifestStoreLayerWithoutDependencies } from "./manifest-store.ts";
import { objectStoreLayer } from "./object-store.ts";
import { makeWriterClientOver, WriterClient } from "./writer-client.ts";
import { WriterServer } from "./writer-server.ts";

/**
 * Provides `DbClient` and `DatabaseLifecycle` to any Worker in the Stack other than `DbApiWorker`
 * (which hosts the writers): it binds `WriterServer` from `DbApiWorker` and `CompactorServer` from
 * `CompactorWorker` (cross-script), the `BlockServer` service, and the bucket. This is how a
 * library such as `@cf-slatedb/sharding` uses the database from its own Worker.
 */
export const dbServicesLayer = Layer.mergeAll(
  dbClientLayerWithoutDependencies.pipe(Layer.provide(dbReaderLayer)),
  databaseLifecycleLayerWithoutDependencies.pipe(Layer.provide(blockSourceLayer)),
).pipe(
  Layer.provideMerge(
    Layer.effect(
      WriterClient,
      Effect.gen(function* () {
        return yield* makeWriterClientOver(yield* WriterServer.from(DbApiWorker));
      }),
    ),
  ),
  Layer.provideMerge(compactorClientLayer),
  Layer.provideMerge(
    manifestStoreLayerWithoutDependencies.pipe(
      Layer.provideMerge(manifestReaderLayerWithoutDependencies),
    ),
  ),
  // Manifest hints and forks' compactor wakes run after the response.
  Layer.provideMerge(workerBackgroundWorkLayer),
  Layer.provideMerge(Layer.mergeAll(objectStoreLayer, lsmConfigurationLayer)),
);

/**
 * Provides `DbClient` and `DatabaseLifecycle` in memory, for tests of code built on them: in-process
 * writers behind the real writer HTTP API, an in-memory bucket, compactor wakes recorded but not run.
 * Also outputs the stores and clients so tests can inspect them.
 */
export const dbServicesTestLayer = (overrides: Partial<LsmSettings> = {}) =>
  databaseLifecycleLayerWithoutDependencies.pipe(
    Layer.provide(blockSourceTestLayer),
    Layer.provide(backgroundWorkTestLayer),
    Layer.provideMerge(compactorClientTestLayer),
    Layer.provideMerge(dbClientTestLayer(overrides)),
  );
