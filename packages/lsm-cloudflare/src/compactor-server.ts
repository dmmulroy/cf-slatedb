import * as Cloudflare from "@alchemy.run/cloudflare";
import type { HttpEffect } from "alchemy/Http";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/http";
import { HttpApiBuilder } from "effect/http-api";

import { durableObjectBackgroundWorkLayer } from "./background-work.ts";
import { CompactionBudget } from "./compaction-budget.ts";
import { CompactionLauncher } from "./compaction-launcher.ts";
import {
  Compactor,
  compactorLayerWithoutDependencies,
  CompactorState,
  CompactorStateCell,
} from "./compactor.ts";
import { CompactorHttpApi } from "./compactor-http-api.ts";
import { durableObjectAlarmLayer } from "./durable-object-alarm.ts";
import { durableObjectCell } from "./durable-object-cell.ts";
import { LsmConfiguration } from "./lsm-configuration.ts";
import { lsmHttpServerLayer } from "./lsm-http-server.ts";
import { eventTelemetryFlushForStage } from "./lsm-telemetry.ts";
import { manifestReaderLayerWithoutDependencies } from "./manifest-reader.ts";
import { garbageCollectorLayerWithoutDependencies } from "./garbage-collector.ts";
import { manifestStoreLayerWithoutDependencies } from "./manifest-store.ts";
import { ObjectStore } from "./object-store.ts";

/** Handlers of the compactor HTTP API, delegating to the contextual `Compactor`. */
export const compactorHttpHandlersLayer = HttpApiBuilder.group(
  CompactorHttpApi,
  "compactor",
  (handlers) =>
    Effect.gen(function* () {
      const compactor = yield* Compactor;
      return handlers
        .handle("wake", ({ params }) => compactor.wake(params.databaseId))
        .handle("destroy", ({ params }) => compactor.destroy(params.databaseId))
        .handle("collect", ({ params }) => compactor.collect(params.databaseId))
        .handle("requestCollection", ({ params }) => compactor.requestCollection(params.databaseId))
        .handle("dropSst", ({ params, payload }) => compactor.dropSst(params.databaseId, payload))
        .handle("status", ({ params }) =>
          compactor.status(params.databaseId).pipe(Effect.map((state) => ({ state }))),
        );
    }),
);

/** Every compactor HTTP route, needing only a `Compactor`. */
export const compactorHttpRouterLayer = HttpApiBuilder.layer(CompactorHttpApi).pipe(
  Layer.provide(compactorHttpHandlersLayer),
  Layer.provide(lsmHttpServerLayer),
);

/** The compactor Durable Object's handlers: its versioned HTTP API and the poll alarm. */
export interface CompactorServerContract {
  readonly fetch: HttpEffect<RuntimeContext>;
  readonly alarm: () => Effect.Effect<void, never, RuntimeContext>;
}

/**
 * A database's compactor, a Durable Object keyed by `DatabaseId` (see `Compactor`). Its state (epoch
 * and running jobs) lives in its storage; its alarm polls while it has work.
 */
export class CompactorServer extends Cloudflare.DurableObject<
  CompactorServer,
  CompactorServerContract
>()("CompactorServer") {}

/**
 * Hosts `CompactorServer`, leaving the object store, settings, launcher, and budget visible: the
 * hosting Worker provides them.
 */
export const compactorServerLayerWithoutDependencies = CompactorServer.make(
  Effect.gen(function* () {
    const objects = yield* ObjectStore;
    const settings = yield* LsmConfiguration;
    const launcher = yield* CompactionLauncher;
    const budget = yield* CompactionBudget;
    const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
    const state = yield* Cloudflare.DurableObjectState;

    return Effect.gen(function* () {
      const storage = state.raw.storage;
      const backgroundWork = durableObjectBackgroundWorkLayer.pipe(
        Layer.provide(Layer.succeed(Cloudflare.DurableObjectState, state)),
      );
      const compactorLayer = compactorLayerWithoutDependencies.pipe(
        // The compactor runs its database's garbage collection from its alarm (D60).
        Layer.provide(garbageCollectorLayerWithoutDependencies),
        Layer.provide(
          manifestStoreLayerWithoutDependencies.pipe(
            Layer.provideMerge(manifestReaderLayerWithoutDependencies),
          ),
        ),
        Layer.provide(backgroundWork),
        Layer.provide(
          Layer.succeed(
            CompactorStateCell,
            durableObjectCell(storage, "compactor", CompactorState),
          ),
        ),
        Layer.provide(durableObjectAlarmLayer(storage)),
        Layer.provide(Layer.succeed(ObjectStore, objects)),
        Layer.provide(Layer.succeed(LsmConfiguration, settings)),
        Layer.provide(Layer.succeed(CompactionLauncher, launcher)),
        Layer.provide(Layer.succeed(CompactionBudget, budget)),
      );
      return yield* Effect.gen(function* () {
        const compactor = yield* Compactor;
        const fetch = yield* HttpRouter.toHttpEffect(
          compactorHttpRouterLayer.pipe(Layer.provide(Layer.succeed(Compactor, compactor))),
        );
        return {
          fetch: withEventTelemetryFlush(fetch),
          alarm: () => withEventTelemetryFlush(compactor.runAlarm),
        };
      }).pipe(Effect.provide(compactorLayer));
    });
  }),
);
