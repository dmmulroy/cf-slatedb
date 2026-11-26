import { DEFAULT_DATABASE_OPTIONS } from "@cf-slatedb/lsm-core";
import { Chunk, Stream } from "effect";
import { Effect, Layer, Option } from "effect";
import { HttpApiBuilder } from "effect/http-api";

import { DatabaseLifecycle } from "./database-lifecycle.ts";
import { DbHttpApi } from "./db-http-api.ts";
import { DbHttpRequest, dbHttpRequestMiddlewareLayer } from "./db-http-request.ts";
import { type DbApiOperation } from "./db-public-error.ts";
import { toPublicError, type DbInternalError } from "./db-public-error-translation.ts";
import { lsmHttpServerLayer } from "./lsm-http-server.ts";
import { CompactorClient } from "./compactor-client.ts";
import { CompactorUnavailableError } from "./compactor.ts";
import { DbClient } from "./db-client.ts";
import { ManifestReader } from "./manifest-reader.ts";
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
    const client = yield* DbClient;
    const compactors = yield* CompactorClient;
    const manifests = yield* ManifestReader;
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
      .handle("createCheckpoint", ({ params, payload }) =>
        lifecycle.createCheckpoint(params.databaseId, payload).pipe(publicFailures("checkpoint")),
      )
      .handle("pinSnapshot", ({ params, payload }) =>
        lifecycle
          .pinSnapshot(params.databaseId, payload.lifetimeMs)
          .pipe(publicFailures("checkpoint")),
      )
      .handle("deleteCheckpoint", ({ params }) =>
        lifecycle
          .deleteCheckpoint(params.databaseId, params.checkpointId)
          .pipe(publicFailures("checkpoint")),
      )
      .handle("seal", ({ params, payload }) =>
        lifecycle.seal(params.databaseId, payload.successors, false).pipe(
          Effect.map((receipt) => ({ flushedSeq: receipt.flushedSeq })),
          publicFailures("seal"),
        ),
      )
      .handle("fork", ({ params, payload }) =>
        lifecycle
          .forkRange({
            source: params.databaseId,
            checkpointId: payload.checkpointId,
            range: payload.range,
            forkId: payload.forkId,
          })
          .pipe(
            Effect.map((created) => ({
              databaseId: created.manifest.databaseId,
              manifestId: created.id,
            })),
            publicFailures("fork"),
          ),
      )
      .handle("removeDatabase", ({ params }) =>
        lifecycle.remove(params.databaseId).pipe(publicFailures("remove")),
      )
      .handle("beginTransaction", ({ params }) =>
        client.beginTransaction(params.databaseId).pipe(
          Effect.map((readSeq) => ({ readSeq })),
          publicFailures("transaction"),
        ),
      )
      .handle("transactionGet", ({ params, payload }) =>
        client.getAt(params.databaseId, payload.key, payload.readSeq).pipe(
          Effect.map((row) => ({ row })),
          publicFailures("transaction"),
        ),
      )
      .handle("transactionScan", ({ params, payload }) =>
        client.scanAt(params.databaseId, payload.range, payload.readSeq, payload.limit).pipe(
          Effect.map((rows) => ({ rows })),
          publicFailures("transaction"),
        ),
      )
      .handle("commitTransaction", ({ params, payload }) =>
        client.commitTransaction(params.databaseId, payload).pipe(publicFailures("transaction")),
      )
      .handle("compaction", ({ params }) =>
        Effect.gen(function* () {
          const latest = yield* manifests.loadLatest(params.databaseId).pipe(
            Effect.catchTags({
              ManifestReadError: (error) =>
                Effect.fail(
                  new CompactorUnavailableError({
                    databaseId: params.databaseId,
                    message: error.message,
                  }),
                ),
            }),
          );
          const compactor = yield* compactors.status(params.databaseId).pipe(Effect.scoped);
          return {
            manifestId: latest.id,
            l0Ssts: latest.manifest.l0.length,
            sortedRuns: latest.manifest.sortedRuns.map((run) => ({
              id: run.id,
              ssts: run.ssts.length,
              sizeBytes: run.ssts.reduce((sum, sst) => sum + sst.sizeBytes, 0),
            })),
            compactor,
          };
        }).pipe(publicFailures("compaction")),
      )
      .handle("get", ({ params, payload }) =>
        client
          .getRow(params.databaseId, payload.key, {
            consistency: payload.consistency,
            snapshot: payload.snapshot,
          })
          .pipe(
            Effect.map((row) => ({ row })),
            publicFailures("get"),
          ),
      )
      .handle("scan", ({ params, payload }) =>
        Stream.runCollect(
          client.scan(params.databaseId, payload.range, {
            consistency: payload.consistency,
            snapshot: payload.snapshot,
            // One extra row tells whether another page follows.
            limit: Option.some(payload.limit + 1),
          }),
        ).pipe(
          Effect.map((collected) => {
            const rows = Chunk.toReadonlyArray(Chunk.fromIterable(collected));
            const next = rows[payload.limit];
            return {
              rows: rows.slice(0, payload.limit),
              resumeFrom: next === undefined ? Option.none() : Option.some(next.key),
            };
          }),
          publicFailures("scan"),
        ),
      )
      .handle("snapshot", ({ params }) =>
        client.snapshot(params.databaseId).pipe(publicFailures("get")),
      )
      .handle("changes", ({ params, query }) =>
        client
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
