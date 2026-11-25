import * as Cloudflare from "@alchemy.run/cloudflare";
import type { HttpEffect } from "alchemy/Http";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/http";

import {
  DatabaseLifecycle,
  databaseLifecycleLayerWithoutDependencies,
} from "./database-lifecycle.ts";
import {
  CompactorClient,
  compactionTriggerLayer,
  compactorClientLayer,
} from "./compactor-client.ts";
import { blockSourceLayer } from "./block-source.ts";
import { DbClient, dbClientLayerWithoutDependencies } from "./db-client.ts";
import { dbHttpRouterLayer } from "./db-http-handlers.ts";
import { dbReaderLayer } from "./db-reader.ts";
import { lsmAccessGateForStage } from "./lsm-access-gate.ts";
import { lsmWorkerCompatibility } from "./lsm-stage.ts";
import { lsmConfigurationLayer } from "./lsm-configuration.ts";
import {
  eventTelemetryFlushForStage,
  type LsmWorkerTailConsumers,
  type LsmWorkerTelemetry,
} from "./lsm-telemetry.ts";
import { ManifestReader, manifestReaderLayerWithoutDependencies } from "./manifest-reader.ts";
import { manifestStoreLayerWithoutDependencies } from "./manifest-store.ts";
import { objectStoreLayer } from "./object-store.ts";
import { WriterClient, writerClientLayer } from "./writer-client.ts";
import { WriterServer, writerServerLayerWithoutDependencies } from "./writer-server.ts";

/** Options of the API Worker. */
export interface DbApiWorkerOptions<R = never> {
  /**
   * URL of the module that default-exports this Worker's Layer (built with the same options); the
   * bundler uses it as the Worker's entry.
   */
  readonly main: string;
  /**
   * Builds the Worker's telemetry Layer from its `service.name`: `lsmOtlpTelemetry(url)` locally;
   * the Stack's own factory on live stages. `R` is what building it needs at plan time.
   */
  readonly telemetry: LsmWorkerTelemetry<R>;
  /** Tail Workers attached to the Worker (`noLsmTailConsumers` locally). */
  readonly tailConsumers: LsmWorkerTailConsumers<R>;
  /** Local dev port. */
  readonly devPort: number;
}

/**
 * The public database API Worker: serves `DbHttpApi` (`/v1`) and hosts every database's
 * `WriterServer` Durable Object. It binds `CompactorWorker`'s `CompactorServer` so writers can wake
 * compactors.
 */
export class DbApiWorker extends Cloudflare.Worker<
  DbApiWorker,
  { readonly fetch: HttpEffect<RuntimeContext> },
  WriterServer
>()("DbApiWorker") {}

/**
 * Builds the API Worker: binds the LSM bucket read-write and the `BlockServer` service, hosts
 * `WriterServer`, and serves the public API. Reads run here, in the caller's isolate; writes and
 * `Latest` probes go to the writer. On live stages every request needs the stage's access token
 * (`lsmAccessGateForStage`).
 */
export const dbApiWorkerLayer = <R = never>(options: DbApiWorkerOptions<R>) =>
  DbApiWorker.make(
    Effect.gen(function* () {
      return {
        main: options.main,
        compatibility: yield* lsmWorkerCompatibility,
        tailConsumers: yield* options.tailConsumers,
        dev: { host: "127.0.0.1", port: options.devPort },
      };
    }),
    Effect.gen(function* () {
      yield* WriterServer;
      const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
      const withAccessGate = yield* lsmAccessGateForStage;
      const writer = yield* WriterClient;
      const lifecycle = yield* DatabaseLifecycle;
      const client = yield* DbClient;
      const compactors = yield* CompactorClient;
      const manifests = yield* ManifestReader;
      // The router's scope must not outlive the invocation that built it.
      const router = yield* makeExecutionMemo(
        HttpRouter.toHttpEffect(
          dbHttpRouterLayer.pipe(
            Layer.provide(Layer.succeed(WriterClient, writer)),
            Layer.provide(Layer.succeed(DatabaseLifecycle, lifecycle)),
            Layer.provide(Layer.succeed(DbClient, client)),
            Layer.provide(Layer.succeed(CompactorClient, compactors)),
            Layer.provide(Layer.succeed(ManifestReader, manifests)),
          ),
        ),
      );
      return { fetch: withEventTelemetryFlush(withAccessGate(Effect.flatten(router))) };
    }).pipe(
      // Built once per isolate: the reader's manifest cache and L1 block cache, the lifecycle, and
      // the writer and compactor clients they share.
      Effect.provide(
        Layer.mergeAll(
          dbClientLayerWithoutDependencies.pipe(Layer.provide(dbReaderLayer)),
          databaseLifecycleLayerWithoutDependencies.pipe(Layer.provide(blockSourceLayer)),
        ).pipe(
          Layer.provideMerge(writerClientLayer),
          Layer.provideMerge(
            manifestStoreLayerWithoutDependencies.pipe(
              Layer.provideMerge(manifestReaderLayerWithoutDependencies),
            ),
          ),
        ),
      ),
      Effect.provide(writerServerLayerWithoutDependencies),
      // Writers wake their database's compactor (hosted by CompactorWorker) after flushes; the
      // lifecycle and the compaction endpoint call it too.
      Effect.provide(compactionTriggerLayer.pipe(Layer.provideMerge(compactorClientLayer))),
      Effect.provide(Layer.mergeAll(objectStoreLayer, lsmConfigurationLayer)),
      Effect.provide(options.telemetry("cf-slatedb-api")),
    ),
  );
