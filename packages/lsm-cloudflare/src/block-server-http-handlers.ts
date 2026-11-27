import { type DatabaseId, ManifestId, type SstId } from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, Option, Ref, Schema } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import { HttpApiBuilder } from "effect/http-api";

import {
  BlockNotFoundError,
  BlockServerHttpApi,
  BlockStorageUnavailableError,
  parseByteRangeParam,
} from "./block-server-http-api.ts";
import { lsmHttpServerLayer } from "./lsm-http-server.ts";
import { latestManifestKey, manifestKey, sstKey } from "./object-keys.ts";
import { ObjectReader } from "./object-reader.ts";
import type { ObjectStoreUnavailableError } from "./object-store.ts";

/** Purges a database's entries from the `BlockServer`'s Workers Cache. */
export interface IBlockCachePurge {
  /** Purges every response tagged `db:<databaseId>`. */
  readonly purgeDatabase: (databaseId: DatabaseId) => Effect.Effect<boolean, never, RuntimeContext>;
}

/** The contextual Workers Cache purge of the `BlockServer`. */
export class BlockCachePurge extends Context.Service<BlockCachePurge, IBlockCachePurge>()(
  "@shaledb/BlockCachePurge",
) {}

/** Provides a purge that records the purged databases; Workers Cache is not emulated locally. */
export const blockCachePurgeTestLayer = Layer.effect(
  BlockCachePurge,
  Effect.gen(function* () {
    const purged = yield* Ref.make<ReadonlyArray<DatabaseId>>([]);
    return BlockCachePurge.of({
      purgeDatabase: (databaseId) =>
        Ref.update(purged, (ids) => [...ids, databaseId]).pipe(Effect.as(true)),
    });
  }),
);

/** `Cache-Control` of immutable objects: SSTs and manifests never change once written. */
export const IMMUTABLE_CACHE_CONTROL = "public, max-age=31536000, immutable";

/** `Cache-Control` of the latest-manifest hint: fresh for a second, then revalidated in the background. */
export const LATEST_HINT_CACHE_CONTROL = "public, max-age=1, stale-while-revalidate=10";

/** The cache tag of every response about a database's objects: `db:<id>`. */
export const databaseCacheTag = (databaseId: DatabaseId): string => `db:${databaseId}`;

const decodeManifestIdText = Schema.decodeEffect(
  Schema.NumberFromString.pipe(Schema.decodeTo(ManifestId)),
);

const bytesResponse = (bytes: Uint8Array, databaseId: DatabaseId) =>
  HttpServerResponse.uint8Array(bytes, {
    contentType: "application/octet-stream",
    headers: {
      "cache-control": IMMUTABLE_CACHE_CONTROL,
      "cache-tag": databaseCacheTag(databaseId),
    },
  });

const notFound = (key: string) =>
  new BlockNotFoundError({
    key,
    message: `The object ${key} does not exist: it was never written or has been collected. Reload the manifest and read again.`,
  });

const storageUnavailable = (error: ObjectStoreUnavailableError) =>
  new BlockStorageUnavailableError({ key: error.key, message: error.message });

/** Handlers of the `BlockServer` API over the contextual read-only object reader and cache purge. */
export const blockServerHttpHandlersLayer = HttpApiBuilder.group(
  BlockServerHttpApi,
  "blocks",
  (handlers) =>
    Effect.gen(function* () {
      const objects = yield* ObjectReader;
      const purge = yield* BlockCachePurge;

      const rangeOf = (
        databaseId: DatabaseId,
        sstId: SstId,
        range: string,
        kind: "block" | "meta",
      ) =>
        Effect.gen(function* () {
          const key = sstKey(databaseId, sstId);
          const { offset, length } = parseByteRangeParam(range);
          yield* Effect.annotateCurrentSpan({
            "database.id": databaseId,
            "sst.id": sstId,
            "block.kind": kind,
            "block.offset": offset,
            "block.length": length,
          });
          const bytes = yield* objects
            .getRange(key, { offset, length })
            .pipe(Effect.mapError(storageUnavailable));
          if (Option.isNone(bytes) || bytes.value.length !== length)
            return yield* Effect.fail(notFound(key));
          return bytesResponse(bytes.value, databaseId);
        });

      return handlers
        .handle("block", ({ params }) =>
          rangeOf(params.databaseId, params.sstId, params.range, "block").pipe(
            Effect.withSpan("BlockServer.block"),
          ),
        )
        .handle("meta", ({ params }) =>
          rangeOf(params.databaseId, params.sstId, params.range, "meta").pipe(
            Effect.withSpan("BlockServer.meta"),
          ),
        )
        .handle("manifest", ({ params }) =>
          Effect.gen(function* () {
            const key = manifestKey(params.databaseId, params.manifestId);
            yield* Effect.annotateCurrentSpan({
              "database.id": params.databaseId,
              "manifest.id": params.manifestId,
            });
            const bytes = yield* objects.get(key).pipe(Effect.mapError(storageUnavailable));
            if (Option.isNone(bytes)) return yield* Effect.fail(notFound(key));
            return bytesResponse(bytes.value, params.databaseId);
          }).pipe(Effect.withSpan("BlockServer.manifest")),
        )
        .handle("latestManifest", ({ params }) =>
          Effect.gen(function* () {
            const hint = yield* objects
              .get(latestManifestKey(params.databaseId))
              .pipe(Effect.mapError(storageUnavailable));
            const manifestId = Option.isSome(hint)
              ? yield* decodeManifestIdText(new TextDecoder().decode(hint.value)).pipe(
                  Effect.option,
                )
              : Option.none<ManifestId>();
            return HttpServerResponse.jsonUnsafe(
              { manifestId: Option.getOrNull(manifestId) },
              {
                headers: {
                  "cache-control": LATEST_HINT_CACHE_CONTROL,
                  "cache-tag": databaseCacheTag(params.databaseId),
                },
              },
            );
          }).pipe(Effect.withSpan("BlockServer.latestManifest")),
        )
        .handle("purge", ({ params }) =>
          purge.purgeDatabase(params.databaseId).pipe(
            Effect.map((purged) => ({ purged })),
            Effect.withSpan("BlockServer.purge"),
          ),
        );
    }),
);

/** `Cache-Control` of every `BlockServer` answer that is not a success. */
export const ERROR_CACHE_CONTROL = "no-store";

/**
 * Marks every answer of 400 or above `no-store`, so Workers Cache never keeps one (read audit). A
 * missing SST does not come back, but a 503 from an unavailable bucket, or a 404 for an object
 * read before a slow write was visible, must not answer later requests from the cache; and what
 * Workers Cache does with an error that names no caching was not determined.
 */
const noStoreOnErrorsLayer = HttpRouter.middleware()(
  (httpEffect) =>
    Effect.map(httpEffect, (response) =>
      response.status >= 400
        ? HttpServerResponse.setHeader(response, "cache-control", ERROR_CACHE_CONTROL)
        : response,
    ),
  { global: true },
);

/** Every `BlockServer` route, needing the object reader and the cache purge. */
export const blockServerHttpRouterLayer = HttpApiBuilder.layer(BlockServerHttpApi).pipe(
  Layer.provide(blockServerHttpHandlersLayer),
  Layer.provide(noStoreOnErrorsLayer),
  Layer.provide(lsmHttpServerLayer),
);
