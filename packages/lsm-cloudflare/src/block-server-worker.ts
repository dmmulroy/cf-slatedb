import * as Cloudflare from "@alchemy.run/cloudflare";
import type { HttpEffect } from "alchemy/Http";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/http";

import {
  BlockCachePurge,
  blockServerHttpRouterLayer,
  databaseCacheTag,
} from "./block-server-http-handlers.ts";
import { eventTelemetryFlushForStage, lsmTelemetryLayer } from "./lsm-telemetry.ts";
import { ObjectReader, objectReaderLayer } from "./object-reader.ts";

/** Options of the `BlockServer` Worker. */
export interface BlockServerWorkerOptions {
  /** URL of the module that default-exports this Worker's Layer; the bundle entry. */
  readonly main: string;
  readonly otlpBaseUrl: string;
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
export const blockServerWorkerLayer = (options: BlockServerWorkerOptions) =>
  BlockServerWorker.make(
    {
      main: options.main,
      workersDev: false,
      compatibility: { date: "2026-10-15", flags: ["nodejs_compat"] },
      dev: { host: "127.0.0.1", port: options.devPort },
    },
    Effect.gen(function* () {
      const cache = yield* Cloudflare.cache({ crossVersionCache: true });
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
      return { fetch: withEventTelemetryFlush(Effect.flatten(router)) };
    }).pipe(
      Effect.provide(objectReaderLayer),
      Effect.provide(
        lsmTelemetryLayer({ baseUrl: options.otlpBaseUrl, serviceName: "cf-slatedb-blocks" }),
      ),
    ),
  );
