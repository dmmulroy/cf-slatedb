import * as Cloudflare from "@alchemy.run/cloudflare";
import type { HttpEffect } from "alchemy/Http";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/http";

import { CompactionTrigger } from "./compaction-trigger.ts";
import { DbWriter, dbWriterLayerWithoutDependencies } from "./db-writer.ts";
import { LsmConfiguration, lsmConfigurationLayer } from "./lsm-configuration.ts";
import { eventTelemetryFlushForStage } from "./lsm-telemetry.ts";
import { memTableLayerWithoutDependencies } from "./mem-table.ts";
import { manifestReaderLayerWithoutDependencies } from "./manifest-reader.ts";
import { manifestStoreLayerWithoutDependencies } from "./manifest-store.ts";
import { ObjectStore, objectStoreLayer } from "./object-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";
import { durableObjectAlarmLayer } from "./durable-object-alarm.ts";
import { writerHttpRouterLayer } from "./writer-http-handlers.ts";
import { writerSqlStorageLayer } from "./writer-sql-storage.ts";

/** The writer Durable Object's handlers: its versioned HTTP API and the flush alarm. */
export interface WriterServerContract {
  readonly fetch: HttpEffect<RuntimeContext>;
  readonly alarm: () => Effect.Effect<void, never, RuntimeContext>;
}

/**
 * The database's single writer, a Durable Object keyed by `DatabaseId`.
 *
 * **Details**
 *
 * Its SQLite table is both the WAL and the memtable; it serves `WriterHttpApi` (`/v1`) and flushes
 * the memtable to L0 from its alarm. Reach it only through `DbClient`.
 */
export class WriterServer extends Cloudflare.DurableObject<WriterServer, WriterServerContract>()(
  "WriterServer",
) {}

/**
 * Hosts `WriterServer`, leaving the object store and settings visible: the hosting Worker binds the
 * bucket and reads the settings.
 *
 * **Details**
 *
 * Each instance builds its writer once: the memtable tables are migrated before the first event,
 * and the writer fences in with a new epoch on its first request or alarm.
 *
 * @throws {MemTableError} As a defect when the instance's SQLite tables cannot be created.
 */
export const writerServerLayerWithoutDependencies = WriterServer.make(
  Effect.gen(function* () {
    const objects = yield* ObjectStore;
    const settings = yield* LsmConfiguration;
    const compaction = yield* CompactionTrigger;
    const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
    const state = yield* Cloudflare.DurableObjectState;

    return Effect.gen(function* () {
      const storage = state.raw.storage;
      const writerLayer = dbWriterLayerWithoutDependencies.pipe(
        Layer.provide(sstStoreLayerWithoutDependencies),
        Layer.provide(
          manifestStoreLayerWithoutDependencies.pipe(
            Layer.provide(manifestReaderLayerWithoutDependencies),
          ),
        ),
        Layer.provide(
          memTableLayerWithoutDependencies.pipe(Layer.provide(writerSqlStorageLayer(storage))),
        ),
        Layer.provide(durableObjectAlarmLayer(storage)),
        Layer.provide(Layer.succeed(ObjectStore, objects)),
        Layer.provide(Layer.succeed(LsmConfiguration, settings)),
        Layer.provide(Layer.succeed(CompactionTrigger, compaction)),
      );
      return yield* Effect.gen(function* () {
        const writer = yield* DbWriter;
        const fetch = yield* HttpRouter.toHttpEffect(
          writerHttpRouterLayer.pipe(Layer.provide(Layer.succeed(DbWriter, writer))),
        );
        return {
          fetch: withEventTelemetryFlush(fetch),
          alarm: () => withEventTelemetryFlush(writer.runAlarm),
        };
      }).pipe(Effect.provide(writerLayer));
    }).pipe(Effect.orDie);
  }),
);

/** Hosts `WriterServer` over the R2-backed object store and configured settings. */
export const writerServerLayer = writerServerLayerWithoutDependencies.pipe(
  Layer.provide([objectStoreLayer, lsmConfigurationLayer]),
);
