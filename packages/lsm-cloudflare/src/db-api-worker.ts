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
import { dbHttpRouterLayer } from "./db-http-handlers.ts";
import { lsmConfigurationLayer } from "./lsm-configuration.ts";
import { eventTelemetryFlushForStage, lsmTelemetryLayer } from "./lsm-telemetry.ts";
import { manifestReaderLayerWithoutDependencies } from "./manifest-reader.ts";
import { manifestStoreLayerWithoutDependencies } from "./manifest-store.ts";
import { objectStoreLayer } from "./object-store.ts";
import { WriterClient, writerClientLayer } from "./writer-client.ts";
import { WriterServer, writerServerLayerWithoutDependencies } from "./writer-server.ts";

/** Options of the API Worker. */
export interface DbApiWorkerOptions {
  /**
   * URL of the module that default-exports this Worker's Layer (built with the same options); the
   * bundler uses it as the Worker's entry.
   */
  readonly main: string;
  /** OTLP/HTTP base URL for traces. */
  readonly otlpBaseUrl: string;
  /** Local dev port. */
  readonly devPort: number;
}

/**
 * The public database API Worker: serves `DbHttpApi` (`/v1`) and hosts every database's
 * `WriterServer` Durable Object.
 */
export class DbApiWorker extends Cloudflare.Worker<
  DbApiWorker,
  { readonly fetch: HttpEffect<RuntimeContext> },
  WriterServer
>()("DbApiWorker") {}

/**
 * Builds the API Worker: binds the LSM bucket read-write, hosts `WriterServer`, and serves the
 * public API with execution-scoped writer clients.
 */
export const dbApiWorkerLayer = (options: DbApiWorkerOptions) =>
  DbApiWorker.make(
    {
      main: options.main,
      compatibility: { date: "2026-10-15", flags: ["nodejs_compat"] },
      dev: { host: "127.0.0.1", port: options.devPort },
    },
    Effect.gen(function* () {
      yield* WriterServer;
      const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
      const writer = yield* WriterClient;
      const lifecycle = yield* DatabaseLifecycle;
      // The router's scope must not outlive the invocation that built it.
      const router = yield* makeExecutionMemo(
        HttpRouter.toHttpEffect(
          dbHttpRouterLayer.pipe(
            Layer.provide(Layer.succeed(WriterClient, writer)),
            Layer.provide(Layer.succeed(DatabaseLifecycle, lifecycle)),
          ),
        ),
      );
      return { fetch: withEventTelemetryFlush(Effect.flatten(router)) };
    }).pipe(
      Effect.provide(writerClientLayer),
      Effect.provide(
        databaseLifecycleLayerWithoutDependencies.pipe(
          Layer.provide(
            manifestStoreLayerWithoutDependencies.pipe(
              Layer.provideMerge(manifestReaderLayerWithoutDependencies),
            ),
          ),
        ),
      ),
      Effect.provide(writerServerLayerWithoutDependencies),
      Effect.provide(Layer.mergeAll(objectStoreLayer, lsmConfigurationLayer)),
      Effect.provide(
        lsmTelemetryLayer({ baseUrl: options.otlpBaseUrl, serviceName: "cf-slatedb-api" }),
      ),
    ),
  );
