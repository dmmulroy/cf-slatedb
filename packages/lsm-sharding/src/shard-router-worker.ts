import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  RequestKeyBytes,
  RequestKeyRange,
  RequestValueBytes,
  type Row,
  SeqNum,
} from "@shaledb/lsm-core";
import {
  bindLsmWorkerSettings,
  DatabaseLifecycle,
  type DbInternalError,
  DbHttpRequest,
  dbServicesLayer,
  eventTelemetryFlushForStage,
  lsmAccessGateFrom,
  type LsmAccessTokenSource,
  type LsmWorkerTailConsumers,
  lsmHttpServerLayer,
  lsmWorkerCompatibility,
  type LsmWorkerSettingsSource,
  publicHttpRequestMiddlewareLayer,
  toPublicError,
  type LsmWorkerTelemetry,
  withLsmTracePolicy,
} from "@shaledb/lsm-cloudflare";
import type { HttpEffect } from "alchemy/Http";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Cause, Effect, Layer, Option, Schema, Stream } from "effect";
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
  type ShardDirectoryError,
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
import {
  ShardSplit,
  shardSplitLayerWithoutDependencies,
  withAbandonedSplitFinished,
} from "./shard-split.ts";

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
const KeyPayload = Schema.Struct({ key: RequestKeyBytes });
const Receipt = Schema.Struct({ databaseId: DatabaseId, seq: SeqNum });
/** A key and value; as a request, both bounded like the database API's (D100). */
const Pair = Schema.Struct({ key: RequestKeyBytes, value: RequestValueBytes });

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
        range: RequestKeyRange,
        limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1000 })),
      }),
      // `resumeFrom`: where the next page starts, or null when the range is exhausted. A page may
      // hold fewer rows than its limit, or none, and still have one: its budget was spent (D125).
      success: Schema.Struct({
        rows: Schema.Array(Pair),
        resumeFrom: Schema.OptionFromNullOr(Schema.Uint8ArrayFromBase64),
      }),
      error: SHARDED_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("split", "/groups/:group/splits", {
      params: GroupParams,
      payload: Schema.Struct({ databaseId: DatabaseId, atKey: RequestKeyBytes }),
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

/**
 * Classifies a database failure the router could not route around. The message is the database
 * API's public one for the same failure (`toPublicError`), never the internal error's, which may
 * name storage paths or carry a storage error's text (D79).
 */
const classify = (error: DbInternalError, requestId: string) => {
  // A corrupted SST names no database, and the router does not say which shard it read (D128):
  // the message names the SST, as the database API's StoredDataInvalid does.
  const { message } =
    error._tag === "SstCorruptedError"
      ? {
          message: `Stored data of this group failed validation: SST ${error.sstId} is corrupted (${error.section}). Retrying will not help. Contact the operator with request id ${requestId}; the corruption runbook restores or drops the SST.`,
        }
      : toPublicError("routeRequest", requestId)(error);
  return RETRYABLE.has(error._tag)
    ? new ShardedRequestUnavailableError({ reason: error._tag, message })
    : new ShardedRequestRefusedError({ reason: error._tag, message });
};

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

/** Every failure a sharded handler can report before classification. */
type ShardedHandlerError = ShardDirectoryError | CrossShardBatchError | DbInternalError;

/** Keeps the sharding errors the API declares; classifies every other (database) failure. */
const shardedFailures = <A, R>(effect: Effect.Effect<A, ShardedHandlerError, R>) =>
  Effect.gen(function* () {
    const { requestId } = yield* DbHttpRequest;
    return yield* effect.pipe(
      Effect.mapError((error) =>
        isDeclaredShardingError(error) ? error : classify(error, requestId),
      ),
    );
  });

/** Handlers of the sharded API over the contextual router, split, directory, and lifecycle. */
const shardedHttpHandlersLayer = HttpApiBuilder.group(ShardedHttpApi, "groups", (handlers) =>
  Effect.gen(function* () {
    const router = yield* ShardRouter;
    const splitter = yield* ShardSplit;
    const directory = yield* ShardDirectory;
    const lifecycle = yield* DatabaseLifecycle;
    const finishingAbandonedSplit = <A, E extends { readonly _tag: string }, R>(
      group: ShardGroup,
      request: () => Effect.Effect<A, E, R>,
    ) => withAbandonedSplitFinished(splitter, group, request);
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
        shardedFailures(
          finishingAbandonedSplit(params.group, () =>
            router.put(params.group, payload.key, payload.value),
          ),
        ),
      )
      .handle("delete", ({ params, payload }) =>
        shardedFailures(
          finishingAbandonedSplit(params.group, () => router.delete(params.group, payload.key)),
        ),
      )
      .handle("get", ({ params, payload }) =>
        shardedFailures(
          finishingAbandonedSplit(params.group, () =>
            router.get(params.group, payload.key, {
              consistency: { _tag: "Latest" },
              snapshot: Option.none(),
              refuseSealed: true,
            }),
          ).pipe(Effect.map((value) => ({ value }))),
        ),
      )
      .handle("scan", ({ params, payload }) =>
        shardedFailures(
          finishingAbandonedSplit(params.group, () =>
            Effect.gen(function* () {
              const rows: Array<Row> = [];
              // One extra row tells whether another page follows; a page whose budget is spent
              // ends early with the key to continue from (D125), as the database API's does.
              const scanned = yield* Effect.exit(
                Stream.runForEach(
                  router.scan(params.group, payload.range, {
                    consistency: { _tag: "Latest" },
                    snapshot: Option.none(),
                    refuseSealed: true,
                    limit: Option.some(payload.limit + 1),
                  }),
                  (row) => Effect.sync(() => void rows.push(row)),
                ),
              );
              if (scanned._tag === "Failure") {
                const failure = Cause.findErrorOption(scanned.cause);
                if (
                  Option.isSome(failure) &&
                  failure.value._tag === "ReaderUnavailableError" &&
                  failure.value.resumeFrom !== undefined &&
                  rows.length <= payload.limit
                ) {
                  return { rows, resumeFrom: Option.some(failure.value.resumeFrom) };
                }
                return yield* Effect.failCause(scanned.cause);
              }
              const next = rows[payload.limit];
              return {
                rows: rows.slice(0, payload.limit),
                resumeFrom: next === undefined ? Option.none() : Option.some(next.key),
              };
            }),
          ).pipe(
            Effect.map(({ rows, resumeFrom }) => ({
              rows: rows.flatMap((row) =>
                row.value._tag === "Value" ? [{ key: row.key, value: row.value.bytes }] : [],
              ),
              resumeFrom,
            })),
          ),
        ),
      )
      .handle("split", ({ params, payload }) =>
        shardedFailures(splitter.split(params.group, payload.databaseId, payload.atKey)),
      );
  }),
);

/**
 * The database API's request middleware, naming the sharded API's own definition: an unmatched
 * route, a malformed request, and a defect get the public error envelope with a request id, as on
 * the database API (D69, D79).
 */
const shardedHttpRequestMiddlewareLayer = publicHttpRequestMiddlewareLayer({
  contract: "the sharded API's endpoints (`ShardedHttpApi` in @shaledb/sharding)",
});

/** Every sharded API route, needing the router, split, directory, and lifecycle. */
export const shardedHttpRouterLayer = HttpApiBuilder.layer(ShardedHttpApi).pipe(
  Layer.provide(shardedHttpHandlersLayer),
  Layer.provide(shardedHttpRequestMiddlewareLayer),
  Layer.provide(lsmHttpServerLayer),
);

/** Options of the shard router Worker. */
export interface ShardRouterWorkerOptions<R = never> {
  /** URL of the module that default-exports this Worker's Layer; the bundle entry. */
  readonly main: string;
  /**
   * Builds the Worker's telemetry Layer from its `service.name`: `lsmOtlpTelemetry(url)` locally;
   * the Stack's own factory on live stages. `R` is what building it needs at plan time.
   */
  readonly telemetry: LsmWorkerTelemetry<R>;
  /**
   * The deployment's engine settings and jurisdiction, bound onto the Worker in full on every
   * deploy (`bindLsmWorkerSettings`). The Stack owns where they come from.
   */
  readonly settings: LsmWorkerSettingsSource<R>;
  /**
   * The access tokens its gate accepts (D64): the Stack's rotatable tokens, or
   * `lsmStageAccessTokens` for a stage's single state-held one.
   */
  readonly accessTokens: LsmAccessTokenSource<R>;
  /** Tail Workers attached to the Worker and its directories (`noLsmTailConsumers` locally). */
  readonly tailConsumers: LsmWorkerTailConsumers<R>;
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
export const shardRouterWorkerLayer = <R = never>(options: ShardRouterWorkerOptions<R>) =>
  ShardRouterWorker.make(
    Effect.gen(function* () {
      return {
        main: options.main,
        compatibility: yield* lsmWorkerCompatibility,
        tailConsumers: yield* options.tailConsumers,
        dev: { host: "127.0.0.1", port: options.devPort },
      };
    }),
    Effect.gen(function* () {
      yield* ShardDirectoryServer;
      const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
      const withAccessGate = yield* lsmAccessGateFrom(options.accessTokens);
      const context = yield* Effect.context<
        ShardRouter | ShardSplit | ShardDirectory | DatabaseLifecycle
      >();
      // The router's scope must not outlive the invocation that built it.
      const router = yield* makeExecutionMemo(
        HttpRouter.toHttpEffect(
          shardedHttpRouterLayer.pipe(Layer.provide(Layer.succeedContext(context))),
        ),
      );
      yield* bindLsmWorkerSettings(options.settings);
      return { fetch: withEventTelemetryFlush(withAccessGate(Effect.flatten(router))) };
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          shardRouterLayerWithoutDependencies,
          shardSplitLayerWithoutDependencies,
        ).pipe(Layer.provideMerge(shardDirectoryLayer), Layer.provideMerge(dbServicesLayer)),
      ),
      Effect.provide(shardDirectoryServerLayer),
      Effect.provide(withLsmTracePolicy(options.telemetry("shaledb-shards"))),
    ),
  );
