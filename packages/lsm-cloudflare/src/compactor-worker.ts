import * as Cloudflare from "@alchemy.run/cloudflare";
import type { HttpEffect } from "alchemy/Http";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer } from "effect";
import { HttpServerResponse } from "effect/http";

import {
  compactionBudgetClientLayer,
  CompactionBudgetServer,
  compactionBudgetServerLayerWithoutDependencies,
} from "./compaction-budget-server.ts";
import { compactionExecutorLayerWithoutDependencies } from "./compaction-executor.ts";
import { compactionLauncherLayer } from "./compaction-workflow.ts";
import { CompactorServer, compactorServerLayerWithoutDependencies } from "./compactor-server.ts";
import { lsmConfigurationLayer } from "./lsm-configuration.ts";
import { eventTelemetryFlushForStage, lsmTelemetryLayer } from "./lsm-telemetry.ts";
import { objectStoreLayer } from "./object-store.ts";

/** Options of the compactor Worker. */
export interface CompactorWorkerOptions {
  /** URL of the module that default-exports this Worker's Layer; the bundle entry. */
  readonly main: string;
  readonly otlpBaseUrl: string;
  readonly devPort: number;
}

/**
 * Hosts every database's `CompactorServer` Durable Object, the `CompactionWorkflow`, and the shared
 * `CompactionBudgetServer`. It has no public routes; writers wake compactors through the
 * `CompactorServer` binding.
 */
export class CompactorWorker extends Cloudflare.Worker<
  CompactorWorker,
  { readonly fetch: HttpEffect<RuntimeContext> },
  CompactorServer
>()("CompactorWorker") {}

/** Builds the compactor Worker: the bucket bound read-write, no `workers.dev` URL. */
export const compactorWorkerLayer = (options: CompactorWorkerOptions) =>
  CompactorWorker.make(
    {
      main: options.main,
      workersDev: false,
      compatibility: { date: "2026-10-15" },
      dev: { host: "127.0.0.1", port: options.devPort },
    },
    Effect.gen(function* () {
      yield* CompactorServer;
      yield* CompactionBudgetServer;
      const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
      return {
        fetch: withEventTelemetryFlush(
          Effect.succeed(HttpServerResponse.text("Not found", { status: 404 })),
        ),
      };
    }).pipe(
      Effect.provide(compactorServerLayerWithoutDependencies),
      Effect.provide(
        Layer.mergeAll(
          compactionLauncherLayer,
          compactionBudgetClientLayer.pipe(
            Layer.provideMerge(compactionBudgetServerLayerWithoutDependencies),
          ),
        ),
      ),
      Effect.provide(compactionExecutorLayerWithoutDependencies),
      Effect.provide(Layer.mergeAll(objectStoreLayer, lsmConfigurationLayer)),
      Effect.provide(
        lsmTelemetryLayer({ baseUrl: options.otlpBaseUrl, serviceName: "cf-slatedb-compactor" }),
      ),
    ),
  );
