import * as Cloudflare from "@alchemy.run/cloudflare";
import { DatabaseId } from "@cf-slatedb/lsm-core";
import {
  durableObjectCell,
  eventTelemetryFlushForStage,
  inLsmJurisdiction,
  lsmHttpServerLayer,
  lsmJurisdiction,
} from "@cf-slatedb/lsm-cloudflare";
import type { HttpEffect } from "alchemy/Http";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Cache, Effect, Layer, Schema } from "effect";
import { HttpRouter } from "effect/http";
import {
  HttpApi,
  HttpApiBuilder,
  HttpApiClient,
  HttpApiEndpoint,
  HttpApiGroup,
  HttpApiSchema,
} from "effect/http-api";

import {
  makeShardDirectoryOver,
  ShardDirectory,
  ShardDirectoryUnavailableError,
  ShardGroupExistsError,
  ShardGroupNotFoundError,
} from "./shard-directory.ts";
import {
  PendingSplit,
  ShardGroup,
  ShardMap,
  ShardSplitInProgressError,
  ShardSplitInvalidError,
} from "./shard-map.ts";

const GroupParams = Schema.Struct({ group: ShardGroup });

/** Every error a directory endpoint can return. */
const DIRECTORY_ERRORS = [
  ShardSplitInvalidError.pipe(HttpApiSchema.status(400)),
  ShardGroupNotFoundError.pipe(HttpApiSchema.status(404)),
  ShardGroupExistsError.pipe(HttpApiSchema.status(409)),
  ShardSplitInProgressError.pipe(HttpApiSchema.status(409)),
  ShardDirectoryUnavailableError.pipe(HttpApiSchema.status(503)),
];

/** Endpoints of a shard group's directory Durable Object (`ShardDirectoryServer`). */
export class ShardDirectoryHttpApiGroup extends HttpApiGroup.make("directory")
  .add(
    HttpApiEndpoint.post("create", "/groups/:group", {
      params: GroupParams,
      payload: Schema.Struct({ databaseId: DatabaseId }),
      success: ShardMap,
      error: DIRECTORY_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.get("map", "/groups/:group/map", {
      params: GroupParams,
      success: ShardMap,
      error: DIRECTORY_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("beginSplit", "/groups/:group/splits", {
      params: GroupParams,
      payload: Schema.Struct({ databaseId: DatabaseId, atKey: Schema.Uint8ArrayFromBase64 }),
      success: PendingSplit,
      error: DIRECTORY_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("completeSplit", "/groups/:group/splits/complete", {
      params: GroupParams,
      payload: Schema.Struct({ databaseId: DatabaseId }),
      success: ShardMap,
      error: DIRECTORY_ERRORS,
    }),
  ) {}

/** The directory Durable Object's HTTP API, versioned under `/v1`. */
export class ShardDirectoryHttpApi extends HttpApi.make("ShardDirectoryHttpApi")
  .add(ShardDirectoryHttpApiGroup)
  .prefix("/v1") {}

/** Handlers of the directory HTTP API, delegating to the contextual `ShardDirectory`. */
const shardDirectoryHttpHandlersLayer = HttpApiBuilder.group(
  ShardDirectoryHttpApi,
  "directory",
  (handlers) =>
    Effect.gen(function* () {
      const directory = yield* ShardDirectory;
      return handlers
        .handle("create", ({ params, payload }) =>
          directory.create(params.group, payload.databaseId),
        )
        .handle("map", ({ params }) => directory.map(params.group))
        .handle("beginSplit", ({ params, payload }) =>
          directory.beginSplit(params.group, payload.databaseId, payload.atKey),
        )
        .handle("completeSplit", ({ params, payload }) =>
          directory.completeSplit(params.group, payload.databaseId),
        );
    }),
);

/** Every directory HTTP route, needing only a `ShardDirectory`. */
export const shardDirectoryHttpRouterLayer = HttpApiBuilder.layer(ShardDirectoryHttpApi).pipe(
  Layer.provide(shardDirectoryHttpHandlersLayer),
  Layer.provide(lsmHttpServerLayer),
);

/**
 * One shard group's directory, a Durable Object keyed by the group's name: it stores the group's
 * range map and serializes its edits.
 */
export class ShardDirectoryServer extends Cloudflare.DurableObject<
  ShardDirectoryServer,
  { readonly fetch: HttpEffect<RuntimeContext> }
>()("ShardDirectoryServer") {}

/** Hosts `ShardDirectoryServer`: the map lives in the object's storage. */
export const shardDirectoryServerLayer = ShardDirectoryServer.make(
  Effect.gen(function* () {
    const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
    const state = yield* Cloudflare.DurableObjectState;
    return Effect.gen(function* () {
      const cell = durableObjectCell(state.raw.storage, "map", ShardMap);
      const directory = yield* makeShardDirectoryOver(() => cell);
      const fetch = yield* HttpRouter.toHttpEffect(
        shardDirectoryHttpRouterLayer.pipe(Layer.provide(Layer.succeed(ShardDirectory, directory))),
      );
      return { fetch: withEventTelemetryFlush(fetch) };
    });
  }),
);

/**
 * Provides the directory as a client of the `ShardDirectoryServer` namespace the Worker being built
 * hosts, one generated HTTP client per group per invocation.
 */
export const shardDirectoryLayer = Layer.effect(
  ShardDirectory,
  Effect.gen(function* () {
    const namespace = yield* ShardDirectoryServer;
    const jurisdiction = yield* lsmJurisdiction;
    const clients = yield* makeExecutionMemo(
      Cache.make({
        capacity: Number.POSITIVE_INFINITY,
        lookup: (group: ShardGroup) =>
          Effect.suspend(() =>
            HttpApiClient.makeWith(ShardDirectoryHttpApi, {
              baseUrl: "http://shard-directory.internal",
              httpClient: Cloudflare.toHttpClient(
                inLsmJurisdiction(namespace, jurisdiction).getByName(group),
              ),
            }),
          ),
      }),
    );
    const client = (group: ShardGroup) =>
      Effect.flatMap(clients, (cache) => Cache.get(cache, group));
    const unreachable = (group: ShardGroup) => (error: { readonly message: string }) =>
      Effect.fail(
        new ShardDirectoryUnavailableError({
          group,
          message: `The directory of shard group ${group} could not be reached: ${error.message} Retry.`,
        }),
      );
    const transport = (group: ShardGroup) => ({
      HttpClientError: unreachable(group),
      SchemaError: unreachable(group),
    });
    return ShardDirectory.of({
      create: (group, databaseId) =>
        Effect.flatMap(client(group), (http) =>
          http.directory.create({ params: { group }, payload: { databaseId } }),
        ).pipe(Effect.catchTags(transport(group)), Effect.scoped),
      map: (group) =>
        Effect.flatMap(client(group), (http) => http.directory.map({ params: { group } })).pipe(
          Effect.catchTags(transport(group)),
          Effect.scoped,
        ),
      beginSplit: (group, databaseId, atKey) =>
        Effect.flatMap(client(group), (http) =>
          http.directory.beginSplit({ params: { group }, payload: { databaseId, atKey } }),
        ).pipe(Effect.catchTags(transport(group)), Effect.scoped),
      completeSplit: (group, databaseId) =>
        Effect.flatMap(client(group), (http) =>
          http.directory.completeSplit({ params: { group }, payload: { databaseId } }),
        ).pipe(Effect.catchTags(transport(group)), Effect.scoped),
    });
  }),
);
