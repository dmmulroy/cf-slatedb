import * as Cloudflare from "@alchemy.run/cloudflare";
import { withLsmTracePolicy } from "./lsm-trace-policy.ts";
import type { HttpEffect } from "alchemy/Http";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer, Option } from "effect";
import { HttpRouter } from "effect/http";

import {
  BlockCachePurge,
  blockServerHttpRouterLayer,
  databaseCacheTag,
} from "./block-server-http-handlers.ts";
import { lsmJurisdiction } from "./lsm-jurisdiction.ts";
import { bindLsmWorkerSettings, type LsmWorkerSettingsSource } from "./lsm-worker-settings.ts";
import { lsmWorkerCompatibility } from "./lsm-stage.ts";
import { eventTelemetryFlushForStage, type LsmWorkerTelemetry } from "./lsm-telemetry.ts";
import { ObjectReader, objectReaderLayer } from "./object-reader.ts";

/** Options of the `BlockServer` Worker. */
export interface BlockServerWorkerOptions<R = never> {
  /** URL of the module that default-exports this Worker's Layer; the bundle entry. */
  readonly main: string;
  /**
   * Builds the Worker's telemetry Layer from its `service.name`: `lsmOtlpTelemetry(url)` locally;
   * the Stack's own factory on live stages. `R` is what building it needs at plan time.
   */
  readonly telemetry: LsmWorkerTelemetry<R>;
  /**
   * The deployment's engine settings and jurisdiction, bound onto the Worker in full on every
   * deploy (`bindLsmWorkerSettings`, D55). The Stack owns where they come from.
   */
  readonly settings: LsmWorkerSettingsSource<R>;
  readonly devPort: number;
}

/**
 * Serves SST blocks, SST metadata, and manifests from R2, one immutable URL each, behind Workers
 * Cache. It runs only on a cache miss and is reachable only through its service binding.
 */
export class BlockServerWorker extends Cloudflare.Worker<
  BlockServerWorker,
  { readonly fetch: HttpEffect<RuntimeContext> }
>()("BlockServerWorker") {}

/**
 * Builds the `BlockServer`: Workers Cache on, shared across deploys (`crossVersionCache`, since the
 * bytes do not depend on the code), no `workers.dev` URL, the bucket bound read-only.
 */
export const blockServerWorkerLayer = <R = never>(options: BlockServerWorkerOptions<R>) =>
  BlockServerWorker.make(
    Effect.gen(function* () {
      return {
        main: options.main,
        workersDev: false,
        compatibility: yield* lsmWorkerCompatibility,
        dev: { host: "127.0.0.1", port: options.devPort },
      };
    }),
    Effect.gen(function* () {
      // Workers Cache keeps block bytes in data centers anywhere: a Stack restricted to a
      // jurisdiction turns it off, and its readers rely on L1 and R2 alone (D49).
      const jurisdiction = yield* lsmJurisdiction;
      const cache = yield* Cloudflare.cache({
        crossVersionCache: true,
        enabled: Option.isNone(jurisdiction),
      });
      const objects = yield* ObjectReader;
      const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
      const cachePurge = BlockCachePurge.of({
        purgeDatabase: (databaseId) =>
          cache.purge({ tags: [databaseCacheTag(databaseId)] }).pipe(
            Effect.as(true),
            Effect.catchTags({
              CachePurgeError: (error) =>
                Effect.logWarning(
                  "Workers Cache purge failed; cached blocks expire only by eviction.",
                  {
                    databaseId,
                    message: error.message,
                  },
                ).pipe(Effect.as(false)),
            }),
          ),
      });
      const router = yield* makeExecutionMemo(
        HttpRouter.toHttpEffect(
          blockServerHttpRouterLayer.pipe(
            Layer.provide(Layer.succeed(ObjectReader, objects)),
            Layer.provide(Layer.succeed(BlockCachePurge, cachePurge)),
          ),
        ),
      );
      yield* bindLsmWorkerSettings(options.settings);
      return { fetch: withEventTelemetryFlush(Effect.flatten(router)) };
    }).pipe(
      Effect.provide(objectReaderLayer),
      Effect.provide(withLsmTracePolicy(options.telemetry("cf-slatedb-blocks"))),
    ),
  );
