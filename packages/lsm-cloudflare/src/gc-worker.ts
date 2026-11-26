import * as Cloudflare from "@alchemy.run/cloudflare";
import { withLsmTracePolicy } from "./lsm-trace-policy.ts";
import * as Alchemy from "alchemy";
import type { HttpEffect } from "alchemy/Http";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/http";
import { HttpApiBuilder } from "effect/http-api";

import { workerBackgroundWorkLayer } from "./background-work.ts";
import { CompactorClient, compactorClientLayer } from "./compactor-client.ts";
import {
  GarbageCollectorUnavailableError,
  garbageCollectorLayerWithoutDependencies,
} from "./garbage-collector.ts";
import { GcHttpApi } from "./gc-http-api.ts";
import { lsmAccessGateFrom, type LsmAccessTokenSource } from "./lsm-access-gate.ts";
import { lsmConfigurationLayer } from "./lsm-configuration.ts";
import { bindLsmWorkerSettings, type LsmWorkerSettingsSource } from "./lsm-worker-settings.ts";
import { lsmHttpServerLayer } from "./lsm-http-server.ts";
import { isLocalLsmStage, lsmWorkerCompatibility } from "./lsm-stage.ts";
import {
  eventTelemetryFlushForStage,
  type LsmWorkerTailConsumers,
  type LsmWorkerTelemetry,
} from "./lsm-telemetry.ts";
import { MaintenanceSweep, maintenanceSweepLayerWithoutDependencies } from "./maintenance-sweep.ts";
import { manifestReaderLayerWithoutDependencies } from "./manifest-reader.ts";
import { objectStoreLayer } from "./object-store.ts";

/** Databases one maintenance sweep visits per cron run or `POST /v1/gc` (D60). */
export const MAINTENANCE_SWEEP_PAGE_DATABASES = 50;

/**
 * Handlers of the GC API: "collect now" runs on the database's compactor, which owns its collection
 * (one at a time per database, D60); `POST /v1/gc` runs one page of the maintenance sweep.
 */
export const gcHttpHandlersLayer = HttpApiBuilder.group(GcHttpApi, "gc", (handlers) =>
  Effect.gen(function* () {
    const compactors = yield* CompactorClient;
    const sweep = yield* MaintenanceSweep;
    return handlers
      .handle("collect", ({ params }) =>
        compactors.collect(params.databaseId).pipe(
          Effect.scoped,
          Effect.catchTags({
            CompactorUnavailableError: (error) =>
              Effect.fail(
                new GarbageCollectorUnavailableError({
                  databaseId: params.databaseId,
                  message: error.message,
                }),
              ),
            DatabaseIdMismatchError: (error) =>
              Effect.fail(
                new GarbageCollectorUnavailableError({
                  databaseId: params.databaseId,
                  message: error.message,
                }),
              ),
          }),
        ),
      )
      .handle("sweep", () =>
        sweep.sweepPage(MAINTENANCE_SWEEP_PAGE_DATABASES).pipe(
          Effect.map((report) => ({
            visited: report.visited,
            reports: report.sweptLeftovers,
            woken: report.woken,
            collectionsRequested: report.collectionsRequested,
            cursor: report.cursor,
          })),
        ),
      );
  }),
);

/** Every GC HTTP route, needing a `CompactorClient` and a `MaintenanceSweep`. */
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
  /** Tail Workers attached to the Worker (`noLsmTailConsumers` locally). */
  readonly tailConsumers: LsmWorkerTailConsumers<R>;
  /**
   * The deployment's engine settings and jurisdiction, bound onto the Worker in full on every
   * deploy (`bindLsmWorkerSettings`, D55). The Stack owns where they come from.
   */
  readonly settings: LsmWorkerSettingsSource<R>;
  /**
   * The access tokens its gate accepts (D64): the Stack's rotatable tokens, or
   * `lsmStageAccessTokens` for a stage's single state-held one.
   */
  readonly accessTokens: LsmAccessTokenSource<R>;
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
        tailConsumers: yield* options.tailConsumers,
        dev: { host: "127.0.0.1", port: options.devPort },
      };
    }),
    Effect.gen(function* () {
      const compactors = yield* CompactorClient;
      const sweep = yield* MaintenanceSweep;
      const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
      const withAccessGate = yield* lsmAccessGateFrom(options.accessTokens);
      // One page per run: the cost of a run is bounded whatever the number of databases.
      yield* Cloudflare.Workers.cron(options.schedule, () =>
        withEventTelemetryFlush(sweep.sweepPage(MAINTENANCE_SWEEP_PAGE_DATABASES)),
      );
      const router = yield* makeExecutionMemo(
        HttpRouter.toHttpEffect(
          gcHttpRouterLayer.pipe(
            Layer.provide(Layer.succeed(CompactorClient, compactors)),
            Layer.provide(Layer.succeed(MaintenanceSweep, sweep)),
          ),
        ),
      );
      yield* bindLsmWorkerSettings(options.settings);
      return { fetch: withEventTelemetryFlush(withAccessGate(Effect.flatten(router))) };
    }).pipe(
      Effect.provide(Cloudflare.Workers.CronEventSourceLive),
      Effect.provide(
        maintenanceSweepLayerWithoutDependencies.pipe(
          Layer.provideMerge(compactorClientLayer),
          Layer.provide(
            garbageCollectorLayerWithoutDependencies.pipe(Layer.provide(workerBackgroundWorkLayer)),
          ),
          Layer.provide(manifestReaderLayerWithoutDependencies),
        ),
      ),
      Effect.provide(Layer.mergeAll(objectStoreLayer, lsmConfigurationLayer)),
      Effect.provide(withLsmTracePolicy(options.telemetry("cf-slatedb-gc"))),
    ),
  );
