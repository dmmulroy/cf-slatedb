import { DEFAULT_DATABASE_OPTIONS } from "@cf-slatedb/lsm-core";
import { Effect, Layer, Option } from "effect";
import { HttpApiBuilder } from "effect/http-api";

import { DatabaseLifecycle } from "./database-lifecycle.ts";
import { DbHttpApi } from "./db-http-api.ts";
import { DbHttpRequest, dbHttpRequestMiddlewareLayer } from "./db-http-request.ts";
import { type DbApiOperation } from "./db-public-error.ts";
import { toPublicError, type DbInternalError } from "./db-public-error-translation.ts";
import { lsmHttpServerLayer } from "./lsm-http-server.ts";
import { WriterClient } from "./writer-client.ts";

/** Runs a handler body and classifies its failure as the public error for `operation`. */
const publicFailures =
  (operation: DbApiOperation) =>
  <A, R>(effect: Effect.Effect<A, DbInternalError, R>) =>
    Effect.gen(function* () {
      const { requestId } = yield* DbHttpRequest;
      return yield* effect.pipe(Effect.mapError(toPublicError(operation, requestId)));
    });

/** Handlers of the public database API (write side; reads are added with the reader). */
export const dbHttpHandlersLayer = HttpApiBuilder.group(DbHttpApi, "databases", (handlers) =>
  Effect.gen(function* () {
    const writer = yield* WriterClient;
    const lifecycle = yield* DatabaseLifecycle;
    return handlers
      .handle("createDatabase", ({ payload }) =>
        lifecycle
          .create(payload.databaseId, {
            keyRange: payload.keyRange,
            databaseOptions: {
              ...DEFAULT_DATABASE_OPTIONS,
              mergeOperator: payload.mergeOperator,
              budgetGroup: payload.budgetGroup,
              changelog: Option.map(payload.changelogRetentionMs, (retentionMs) => ({
                retentionMs,
              })),
            },
          })
          .pipe(
            Effect.map((created) => ({
              databaseId: created.manifest.databaseId,
              manifestId: created.id,
            })),
            publicFailures("createDatabase"),
          ),
      )
      .handle("write", ({ params, payload }) =>
        writer.write(params.databaseId, payload).pipe(publicFailures("write")),
      )
      .handle("flush", ({ params }) =>
        writer.flush(params.databaseId).pipe(
          Effect.map((flushedSeq) => ({ flushedSeq })),
          publicFailures("flush"),
        ),
      )
      .handle("status", ({ params }) =>
        writer.status(params.databaseId).pipe(publicFailures("status")),
      )
      .handle("changes", ({ params, query }) =>
        writer
          .changes(params.databaseId, query.afterSeq, query.limit)
          .pipe(publicFailures("changes")),
      );
  }),
);

/** Every public API route behind the request middleware, needing the writer client and lifecycle. */
export const dbHttpRouterLayer = HttpApiBuilder.layer(DbHttpApi).pipe(
  Layer.provide(dbHttpHandlersLayer),
  Layer.provide(dbHttpRequestMiddlewareLayer),
  Layer.provide(lsmHttpServerLayer),
);
