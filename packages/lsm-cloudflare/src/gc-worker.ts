import * as Cloudflare from "@alchemy.run/cloudflare";
import type { HttpEffect } from "alchemy/Http";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/http";
import { HttpApiBuilder } from "effect/http-api";

import { GarbageCollector, garbageCollectorLayerWithoutDependencies } from "./garbage-collector.ts";
import { GcHttpApi } from "./gc-http-api.ts";
import { lsmConfigurationLayer } from "./lsm-configuration.ts";
import { lsmHttpServerLayer } from "./lsm-http-server.ts";
import { eventTelemetryFlushForStage, lsmTelemetryLayer } from "./lsm-telemetry.ts";
import { objectStoreLayer } from "./object-store.ts";

/** Handlers of the GC API, delegating to the contextual `GarbageCollector`. */
export const gcHttpHandlersLayer = HttpApiBuilder.group(GcHttpApi, "gc", (handlers) =>
  Effect.gen(function* () {
    const collector = yield* GarbageCollector;
    return handlers
      .handle("collect", ({ params }) => collector.collect(params.databaseId))
      .handle("collectAll", () =>
        collector.collectAll.pipe(Effect.map((reports) => ({ reports }))),
      );
  }),
);

/** Every GC HTTP route, needing only a `GarbageCollector`. */
export const gcHttpRouterLayer = HttpApiBuilder.layer(GcHttpApi).pipe(
  Layer.provide(gcHttpHandlersLayer),
  Layer.provide(lsmHttpServerLayer),
);

/** Options of the GC Worker. */
export interface GcWorkerOptions {
  /** URL of the module that default-exports this Worker's Layer; the bundle entry. */
  readonly main: string;
  readonly otlpBaseUrl: string;
  readonly devPort: number;
  /** Cron expression for collecting every database (`gcInterval` in cron form). */
  readonly schedule: string;
}

/**
 * Collects every database on a cron schedule (design §2, "GC cron Worker"); its HTTP API lets an
 * operator collect one database now. No `workers.dev` URL.
 */
export class GcWorker extends Cloudflare.Worker<
  GcWorker,
  { readonly fetch: HttpEffect<RuntimeContext> }
>()("GcWorker") {}

/** Builds the GC Worker: the bucket bound read-write, the cron trigger, and the operator API. */
export const gcWorkerLayer = (options: GcWorkerOptions) =>
  GcWorker.make(
    {
      main: options.main,
      workersDev: false,
      // `nodejs_compat` is the default from 2026-08-04; workerd rejects it as an explicit flag here.
      compatibility: { date: "2026-10-15" },
      dev: { host: "127.0.0.1", port: options.devPort },
    },
    Effect.gen(function* () {
      const collector = yield* GarbageCollector;
      const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
      yield* Cloudflare.Workers.cron(options.schedule, () =>
        withEventTelemetryFlush(collector.collectAll),
      );
      const router = yield* makeExecutionMemo(
        HttpRouter.toHttpEffect(
          gcHttpRouterLayer.pipe(Layer.provide(Layer.succeed(GarbageCollector, collector))),
        ),
      );
      return { fetch: withEventTelemetryFlush(Effect.flatten(router)) };
    }).pipe(
      Effect.provide(Cloudflare.Workers.CronEventSourceLive),
      Effect.provide(garbageCollectorLayerWithoutDependencies),
      Effect.provide(Layer.mergeAll(objectStoreLayer, lsmConfigurationLayer)),
      Effect.provide(
        lsmTelemetryLayer({ baseUrl: options.otlpBaseUrl, serviceName: "cf-slatedb-gc" }),
      ),
    ),
  );
