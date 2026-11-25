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
import { lsmWorkerCompatibility } from "./lsm-stage.ts";
import { lsmConfigurationLayer } from "./lsm-configuration.ts";
import { eventTelemetryFlushForStage, type LsmWorkerTelemetry } from "./lsm-telemetry.ts";
import { objectStoreLayer } from "./object-store.ts";

/** Options of the compactor Worker. */
export interface CompactorWorkerOptions<R = never> {
  /** URL of the module that default-exports this Worker's Layer; the bundle entry. */
  readonly main: string;
  /**
   * Builds the Worker's telemetry Layer from its `service.name`: `lsmOtlpTelemetry(url)` locally;
   * the Stack's own factory on live stages. `R` is what building it needs at plan time.
   */
  readonly telemetry: LsmWorkerTelemetry<R>;
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
export const compactorWorkerLayer = <R = never>(options: CompactorWorkerOptions<R>) =>
  CompactorWorker.make(
    Effect.gen(function* () {
      return {
        main: options.main,
        workersDev: false,
        compatibility: yield* lsmWorkerCompatibility,
        dev: { host: "127.0.0.1", port: options.devPort },
      };
    }),
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
      Effect.provide(options.telemetry("cf-slatedb-compactor")),
    ),
  );
