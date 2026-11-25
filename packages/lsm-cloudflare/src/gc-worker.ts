import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import type { HttpEffect } from "alchemy/Http";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/http";
import { HttpApiBuilder } from "effect/http-api";

import { GarbageCollector, garbageCollectorLayerWithoutDependencies } from "./garbage-collector.ts";
import { GcHttpApi } from "./gc-http-api.ts";
import { lsmAccessGateForStage } from "./lsm-access-gate.ts";
import { lsmConfigurationLayer } from "./lsm-configuration.ts";
import { lsmHttpServerLayer } from "./lsm-http-server.ts";
import { isLocalLsmStage, lsmWorkerCompatibility } from "./lsm-stage.ts";
import { eventTelemetryFlushForStage, type LsmWorkerTelemetry } from "./lsm-telemetry.ts";
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
export interface GcWorkerOptions<R = never> {
  /** URL of the module that default-exports this Worker's Layer; the bundle entry. */
  readonly main: string;
  /**
   * Builds the Worker's telemetry Layer from its `service.name`: `lsmOtlpTelemetry(url)` locally;
   * the Stack's own factory on live stages. `R` is what building it needs at plan time.
   */
  readonly telemetry: LsmWorkerTelemetry<R>;
  readonly devPort: number;
  /** Cron expression for collecting every database (`gcInterval` in cron form). */
  readonly schedule: string;
}

/**
 * Collects every database on a cron schedule (design §2, "GC cron Worker"); its HTTP API lets an
 * operator collect one database now. On live stages the API has a `workers.dev` URL behind the
 * stage's access token; local stages serve it on the dev port.
 */
export class GcWorker extends Cloudflare.Worker<
  GcWorker,
  { readonly fetch: HttpEffect<RuntimeContext> }
>()("GcWorker") {}

/** Builds the GC Worker: the bucket bound read-write, the cron trigger, and the operator API. */
export const gcWorkerLayer = <R = never>(options: GcWorkerOptions<R>) =>
  GcWorker.make(
    Effect.gen(function* () {
      const stage = yield* Alchemy.Stage;
      return {
        main: options.main,
        // Live stages reach the operator API over workers.dev, behind the access token.
        workersDev: !isLocalLsmStage(stage),
        compatibility: yield* lsmWorkerCompatibility,
        dev: { host: "127.0.0.1", port: options.devPort },
      };
    }),
    Effect.gen(function* () {
      const collector = yield* GarbageCollector;
      const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
      const withAccessGate = yield* lsmAccessGateForStage;
      yield* Cloudflare.Workers.cron(options.schedule, () =>
        withEventTelemetryFlush(collector.collectAll),
      );
      const router = yield* makeExecutionMemo(
        HttpRouter.toHttpEffect(
          gcHttpRouterLayer.pipe(Layer.provide(Layer.succeed(GarbageCollector, collector))),
        ),
      );
      return { fetch: withEventTelemetryFlush(withAccessGate(Effect.flatten(router))) };
    }).pipe(
      Effect.provide(Cloudflare.Workers.CronEventSourceLive),
      Effect.provide(garbageCollectorLayerWithoutDependencies),
      Effect.provide(Layer.mergeAll(objectStoreLayer, lsmConfigurationLayer)),
      Effect.provide(options.telemetry("cf-slatedb-gc")),
    ),
  );
