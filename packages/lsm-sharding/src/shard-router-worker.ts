import * as Cloudflare from "@alchemy.run/cloudflare";
import { DatabaseId, DEFAULT_DATABASE_OPTIONS, KeyRange, SeqNum } from "@cf-slatedb/lsm-core";
import {
  DatabaseLifecycle,
  dbServicesLayer,
  eventTelemetryFlushForStage,
  lsmAccessGateForStage,
  lsmHttpServerLayer,
  lsmTelemetryLayer,
} from "@cf-slatedb/lsm-cloudflare";
import type { HttpEffect } from "alchemy/Http";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer, Option, Schema, Stream } from "effect";
import { HttpRouter } from "effect/http";
import {
  HttpApi,
  HttpApiBuilder,
  HttpApiEndpoint,
  HttpApiGroup,
  HttpApiSchema,
} from "effect/http-api";

import {
  ShardDirectory,
  ShardDirectoryUnavailableError,
  ShardGroupExistsError,
  ShardGroupNotFoundError,
} from "./shard-directory.ts";
import {
  shardDirectoryLayer,
  ShardDirectoryServer,
  shardDirectoryServerLayer,
} from "./shard-directory-server.ts";
import {
  ShardGroup,
  ShardMap,
  ShardSplitInProgressError,
  ShardSplitInvalidError,
} from "./shard-map.ts";
import {
  CrossShardBatchError,
  ShardRouter,
  shardRouterLayerWithoutDependencies,
} from "./shard-router.ts";
import { ShardSplit, shardSplitLayerWithoutDependencies } from "./shard-split.ts";

/**
 * A database operation behind the router failed in a way retrying may fix (storage, backpressure,
 * a split taking longer than the router waits). `reason` is the database error's tag.
 */
export class ShardedRequestUnavailableError extends Schema.TaggedError<ShardedRequestUnavailableError>()(
  "ShardedRequestUnavailableError",
  { reason: Schema.String, message: Schema.String },
) {}

/** A database operation behind the router was refused; retrying the same request will not help. */
export class ShardedRequestRefusedError extends Schema.TaggedError<ShardedRequestRefusedError>()(
  "ShardedRequestRefusedError",
  { reason: Schema.String, message: Schema.String },
) {}

const SHARDED_ERRORS = [
  ShardSplitInvalidError.pipe(HttpApiSchema.status(400)),
  CrossShardBatchError.pipe(HttpApiSchema.status(400)),
  ShardGroupNotFoundError.pipe(HttpApiSchema.status(404)),
  ShardGroupExistsError.pipe(HttpApiSchema.status(409)),
  ShardSplitInProgressError.pipe(HttpApiSchema.status(409)),
  ShardedRequestRefusedError.pipe(HttpApiSchema.status(409)),
  ShardDirectoryUnavailableError.pipe(HttpApiSchema.status(503)),
  ShardedRequestUnavailableError.pipe(HttpApiSchema.status(503)),
];

const GroupParams = Schema.Struct({ group: ShardGroup });
const KeyPayload = Schema.Struct({ key: Schema.Uint8ArrayFromBase64 });
const Receipt = Schema.Struct({ databaseId: DatabaseId, seq: SeqNum });
const Pair = Schema.Struct({
  key: Schema.Uint8ArrayFromBase64,
  value: Schema.Uint8ArrayFromBase64,
});

/** The sharded key-value API: a group of range-sharded databases behind one key space. */
export class ShardedHttpApiGroup extends HttpApiGroup.make("groups")
  .add(
    HttpApiEndpoint.post("create", "/groups/:group", {
      params: GroupParams,
      payload: Schema.Struct({ databaseId: DatabaseId }),
      success: ShardMap,
      error: SHARDED_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.get("map", "/groups/:group/map", {
      params: GroupParams,
      success: ShardMap,
      error: SHARDED_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("put", "/groups/:group/put", {
      params: GroupParams,
      payload: Pair,
      success: Receipt,
      error: SHARDED_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("delete", "/groups/:group/delete", {
      params: GroupParams,
      payload: KeyPayload,
      success: Receipt,
      error: SHARDED_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("get", "/groups/:group/get", {
      params: GroupParams,
      payload: KeyPayload,
      success: Schema.Struct({ value: Schema.OptionFromNullOr(Schema.Uint8ArrayFromBase64) }),
      error: SHARDED_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("scan", "/groups/:group/scan", {
      params: GroupParams,
      payload: Schema.Struct({
        range: KeyRange,
        limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 })),
      }),
      success: Schema.Struct({ rows: Schema.Array(Pair) }),
      error: SHARDED_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("split", "/groups/:group/splits", {
      params: GroupParams,
      payload: Schema.Struct({ databaseId: DatabaseId, atKey: Schema.Uint8ArrayFromBase64 }),
      success: ShardMap,
      error: SHARDED_ERRORS,
    }),
  ) {}

/** The shard router Worker's HTTP API, versioned under `/v1`. */
export class ShardedHttpApi extends HttpApi.make("ShardedHttpApi")
  .add(ShardedHttpApiGroup)
  .prefix("/v1") {}

/** Tags of database errors that retrying may fix. */
const RETRYABLE = new Set([
  "WriterUnavailableError",
  "WriteStalledError",
  "WriterFencedError",
  "ReaderUnavailableError",
  "CompactorUnavailableError",
  "DatabaseSealedError",
  "DatabaseNotFoundError",
  "KeyOutOfRangeError",
]);

/** Classifies a database failure the router could not route around. */
const classify = (error: { readonly _tag: string; readonly message: string }) =>
  RETRYABLE.has(error._tag)
    ? new ShardedRequestUnavailableError({ reason: error._tag, message: error.message })
    : new ShardedRequestRefusedError({ reason: error._tag, message: error.message });

/** The sharding errors the API declares as themselves. */
const isDeclaredShardingError = Schema.is(
  Schema.Union([
    ShardSplitInvalidError,
    CrossShardBatchError,
    ShardGroupNotFoundError,
    ShardGroupExistsError,
    ShardSplitInProgressError,
    ShardDirectoryUnavailableError,
  ]),
);

/** Keeps the sharding errors the API declares; classifies every other (database) failure. */
const shardedFailures = <A, R>(
  effect: Effect.Effect<A, { readonly _tag: string; readonly message: string }, R>,
) =>
  effect.pipe(
    Effect.mapError((error) => (isDeclaredShardingError(error) ? error : classify(error))),
  );

/** Handlers of the sharded API over the contextual router, split, directory, and lifecycle. */
const shardedHttpHandlersLayer = HttpApiBuilder.group(ShardedHttpApi, "groups", (handlers) =>
  Effect.gen(function* () {
    const router = yield* ShardRouter;
    const splitter = yield* ShardSplit;
    const directory = yield* ShardDirectory;
    const lifecycle = yield* DatabaseLifecycle;
    return handlers
      .handle("create", ({ params, payload }) =>
        shardedFailures(
          Effect.gen(function* () {
            yield* lifecycle
              .create(payload.databaseId, {
                keyRange: Option.none(),
                databaseOptions: DEFAULT_DATABASE_OPTIONS,
              })
              .pipe(Effect.catchTags({ DatabaseAlreadyExistsError: () => Effect.void }));
            return yield* directory.create(params.group, payload.databaseId);
          }),
        ),
      )
      .handle("map", ({ params }) => shardedFailures(router.refresh(params.group)))
      .handle("put", ({ params, payload }) =>
        shardedFailures(router.put(params.group, payload.key, payload.value)),
      )
      .handle("delete", ({ params, payload }) =>
        shardedFailures(router.delete(params.group, payload.key)),
      )
      .handle("get", ({ params, payload }) =>
        shardedFailures(
          router
            .get(params.group, payload.key, {
              consistency: { _tag: "Latest" },
              snapshot: Option.none(),
            })
            .pipe(Effect.map((value) => ({ value }))),
        ),
      )
      .handle("scan", ({ params, payload }) =>
        shardedFailures(
          Stream.runCollect(
            router.scan(params.group, payload.range, {
              consistency: { _tag: "Latest" },
              snapshot: Option.none(),
              limit: Option.some(payload.limit),
            }),
          ).pipe(
            Effect.map((rows) => ({
              rows: rows.flatMap((row) =>
                row.value._tag === "Value" ? [{ key: row.key, value: row.value.bytes }] : [],
              ),
            })),
          ),
        ),
      )
      .handle("split", ({ params, payload }) =>
        shardedFailures(splitter.split(params.group, payload.databaseId, payload.atKey)),
      );
  }),
);

/** Every sharded API route. */
const shardedHttpRouterLayer = HttpApiBuilder.layer(ShardedHttpApi).pipe(
  Layer.provide(shardedHttpHandlersLayer),
  Layer.provide(lsmHttpServerLayer),
);

/** Options of the shard router Worker. */
export interface ShardRouterWorkerOptions {
  /** URL of the module that default-exports this Worker's Layer; the bundle entry. */
  readonly main: string;
  readonly otlpBaseUrl: string;
  readonly devPort: number;
}

/**
 * An example Worker built on the sharding package: hosts every group's `ShardDirectoryServer` and
 * serves `ShardedHttpApi`, reaching the databases through `dbServicesLayer` (public APIs only).
 */
export class ShardRouterWorker extends Cloudflare.Worker<
  ShardRouterWorker,
  { readonly fetch: HttpEffect<RuntimeContext> },
  ShardDirectoryServer
>()("ShardRouterWorker") {}

/** Builds the shard router Worker. */
export const shardRouterWorkerLayer = (options: ShardRouterWorkerOptions) =>
  ShardRouterWorker.make(
    {
      main: options.main,
      compatibility: { date: "2026-10-15" },
      dev: { host: "127.0.0.1", port: options.devPort },
    },
    Effect.gen(function* () {
      yield* ShardDirectoryServer;
      const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
      const withAccessGate = yield* lsmAccessGateForStage;
      const context = yield* Effect.context<
        ShardRouter | ShardSplit | ShardDirectory | DatabaseLifecycle
      >();
      // The router's scope must not outlive the invocation that built it.
      const router = yield* makeExecutionMemo(
        HttpRouter.toHttpEffect(
          shardedHttpRouterLayer.pipe(Layer.provide(Layer.succeedContext(context))),
        ),
      );
      return { fetch: withEventTelemetryFlush(withAccessGate(Effect.flatten(router))) };
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          shardRouterLayerWithoutDependencies,
          shardSplitLayerWithoutDependencies,
        ).pipe(Layer.provideMerge(shardDirectoryLayer), Layer.provideMerge(dbServicesLayer)),
      ),
      Effect.provide(shardDirectoryServerLayer),
      Effect.provide(
        lsmTelemetryLayer({ baseUrl: options.otlpBaseUrl, serviceName: "cf-slatedb-shards" }),
      ),
    ),
  );
