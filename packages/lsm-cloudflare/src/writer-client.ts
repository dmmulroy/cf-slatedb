import * as Cloudflare from "@alchemy.run/cloudflare";
import type {
  DatabaseId,
  DatabaseSealedError,
  KeyOutOfRangeError,
  KeyRange,
  SeqNum,
  WriteBatch,
  WriteBatchInvalidError,
  WriteReceipt,
  WriterFencedError,
} from "@cf-slatedb/lsm-core";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import { RuntimeContext } from "alchemy/RuntimeContext";
import { Cache, Context, Effect, Layer, type Option, Schedule, Scope } from "effect";
import { HttpRouter, HttpServerRequest } from "effect/http";
import { HttpApiClient } from "effect/http-api";

import {
  type ChangesPage,
  DbWriter,
  dbWriterLayerWithoutDependencies,
  type MemTableProbe,
  type MemTableScanPage,
  type WriterStatus,
} from "./db-writer.ts";
import type { CompactionTrigger } from "./compaction-trigger.ts";
import type { LsmConfiguration } from "./lsm-configuration.ts";
import {
  type DatabaseIdMismatchError,
  type DatabaseNotFoundError,
  WriterUnavailableError,
  type WriteStalledError,
} from "./lsm-errors.ts";
import { type MemTableCursor, memTableLayerWithoutDependencies } from "./mem-table.ts";
import type { ManifestReader } from "./manifest-reader.ts";
import type { ManifestStore } from "./manifest-store.ts";
import type { ObjectStore } from "./object-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";
import { durableObjectAlarmTestLayer } from "./durable-object-alarm.ts";
import { WriterHttpApi } from "./writer-http-api.ts";
import { writerHttpRouterLayer } from "./writer-http-handlers.ts";
import { WriterServer } from "./writer-server.ts";
import { writerSqlStorageTestLayer } from "./writer-sql-storage.ts";

/** Every failure a writer call can report to its caller. */
export type WriterClientError =
  | DatabaseNotFoundError
  | DatabaseIdMismatchError
  | DatabaseSealedError
  | WriterFencedError
  | WriteStalledError
  | WriteBatchInvalidError
  | KeyOutOfRangeError
  | WriterUnavailableError;

/** Services every writer call needs from the current Worker invocation. */
export type WriterClientServices = RuntimeContext | Scope.Scope;

/** Calls a database's writer (`WriterServer`) over its versioned HTTP API. */
export interface IWriterClient {
  /** Commits a batch; durable when it returns. */
  readonly write: (
    databaseId: DatabaseId,
    batch: WriteBatch,
  ) => Effect.Effect<WriteReceipt, WriterClientError, WriterClientServices>;
  /** The key's unflushed versions and the sequence numbers a `Latest` read needs. */
  readonly probe: (
    databaseId: DatabaseId,
    key: Uint8Array,
  ) => Effect.Effect<MemTableProbe, WriterClientError, WriterClientServices>;
  /** One page of unflushed rows in `range`. */
  readonly scan: (
    databaseId: DatabaseId,
    request: {
      readonly range: KeyRange;
      readonly snapshotSeq: Option.Option<SeqNum>;
      readonly cursor: Option.Option<MemTableCursor>;
      readonly limit: number;
    },
  ) => Effect.Effect<MemTableScanPage, WriterClientError, WriterClientServices>;
  /** Flushes every committed row to L0. @returns The new `flushedSeq`. */
  readonly flush: (
    databaseId: DatabaseId,
  ) => Effect.Effect<SeqNum, WriterClientError, WriterClientServices>;
  /** Unflushed changes after `afterSeq`, in sequence order. */
  readonly changes: (
    databaseId: DatabaseId,
    afterSeq: SeqNum,
    limit: number,
  ) => Effect.Effect<ChangesPage, WriterClientError, WriterClientServices>;
  /** The writer's state. */
  readonly status: (
    databaseId: DatabaseId,
  ) => Effect.Effect<WriterStatus, WriterClientError, WriterClientServices>;
  /** Deletes the writer's state for a database being removed. Safe to repeat. */
  readonly destroy: (
    databaseId: DatabaseId,
  ) => Effect.Effect<void, WriterClientError, WriterClientServices>;
  /** Flushes everything and seals the database with `successors`. Safe to repeat. */
  readonly seal: (
    databaseId: DatabaseId,
    successors: ReadonlyArray<DatabaseId>,
  ) => Effect.Effect<SeqNum, WriterClientError, WriterClientServices>;
}

/** The contextual writer client. */
export class WriterClient extends Context.Service<WriterClient, IWriterClient>()(
  "@cf-slatedb/WriterClient",
) {}

/**
 * Retries a call that failed only because this writer instance was fenced or unreachable: the
 * Durable Object name routes the retry to the current instance. Three attempts, 50 ms apart.
 */
const retryRoutable = {
  while: (error: WriterClientError) =>
    error._tag === "WriterFencedError" ||
    (error._tag === "WriterUnavailableError" && error.reason === "WriterUnreachable"),
  schedule: Schedule.spaced("50 millis"),
  times: 2,
};

/** The generated HTTP client of the writer API. */
type WriterHttpClient = HttpApiClient.ForApi<typeof WriterHttpApi>;

/**
 * Builds the writer client's operations from a way to obtain the generated HTTP client of one
 * database's writer. Private: production obtains it from the `WriterServer` namespace, tests from an
 * in-process router.
 */
const writerClientOperations = <E, R>(
  writerHttpClient: (databaseId: DatabaseId) => Effect.Effect<WriterHttpClient, E, R>,
) => {
  const unreachable =
    (databaseId: DatabaseId, operation: string) => (error: { readonly message: string }) =>
      Effect.fail(
        new WriterUnavailableError({
          databaseId,
          operation,
          reason: "WriterUnreachable",
          message: `The writer of database ${databaseId} could not be reached for ${operation}: ${error.message}. Retry the same operation; a write that failed this way may not have been acknowledged.`,
        }),
      );
  const transport = (databaseId: DatabaseId, operation: string) => ({
    HttpClientError: unreachable(databaseId, operation),
    SchemaError: unreachable(databaseId, operation),
  });

  return {
    write: Effect.fn("WriterClient.write")(function* (databaseId: DatabaseId, batch: WriteBatch) {
      yield* Effect.annotateCurrentSpan({
        "database.id": databaseId,
        "batch.operations": batch.operations.length,
      });
      const client = yield* writerHttpClient(databaseId);
      return yield* client.writer
        .write({ params: { databaseId }, payload: batch })
        .pipe(Effect.catchTags(transport(databaseId, "write")), Effect.retry(retryRoutable));
    }),
    probe: Effect.fn("WriterClient.probe")(function* (databaseId: DatabaseId, key: Uint8Array) {
      yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
      const client = yield* writerHttpClient(databaseId);
      return yield* client.writer
        .probe({ params: { databaseId }, payload: { key } })
        .pipe(Effect.catchTags(transport(databaseId, "probe")), Effect.retry(retryRoutable));
    }),
    scan: Effect.fn("WriterClient.scan")(function* (
      databaseId: DatabaseId,
      request: {
        readonly range: KeyRange;
        readonly snapshotSeq: Option.Option<SeqNum>;
        readonly cursor: Option.Option<MemTableCursor>;
        readonly limit: number;
      },
    ) {
      yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
      const client = yield* writerHttpClient(databaseId);
      return yield* client.writer
        .scan({ params: { databaseId }, payload: request })
        .pipe(Effect.catchTags(transport(databaseId, "scan")), Effect.retry(retryRoutable));
    }),
    flush: Effect.fn("WriterClient.flush")(function* (databaseId: DatabaseId) {
      yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
      const client = yield* writerHttpClient(databaseId);
      const response = yield* client.writer
        .flush({ params: { databaseId } })
        .pipe(Effect.catchTags(transport(databaseId, "flush")), Effect.retry(retryRoutable));
      return response.flushedSeq;
    }),
    changes: Effect.fn("WriterClient.changes")(function* (
      databaseId: DatabaseId,
      afterSeq: SeqNum,
      limit: number,
    ) {
      yield* Effect.annotateCurrentSpan({ "database.id": databaseId, seq: afterSeq });
      const client = yield* writerHttpClient(databaseId);
      return yield* client.writer
        .changes({ params: { databaseId }, query: { afterSeq, limit } })
        .pipe(Effect.catchTags(transport(databaseId, "changes")), Effect.retry(retryRoutable));
    }),
    status: Effect.fn("WriterClient.status")(function* (databaseId: DatabaseId) {
      yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
      const client = yield* writerHttpClient(databaseId);
      return yield* client.writer
        .status({ params: { databaseId } })
        .pipe(Effect.catchTags(transport(databaseId, "status")), Effect.retry(retryRoutable));
    }),
    destroy: Effect.fn("WriterClient.destroy")(function* (databaseId: DatabaseId) {
      yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
      const client = yield* writerHttpClient(databaseId);
      yield* client.writer
        .destroy({ params: { databaseId } })
        .pipe(Effect.catchTags(transport(databaseId, "destroy")), Effect.retry(retryRoutable));
    }),
    seal: Effect.fn("WriterClient.seal")(function* (
      databaseId: DatabaseId,
      successors: ReadonlyArray<DatabaseId>,
    ) {
      yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
      const client = yield* writerHttpClient(databaseId);
      const response = yield* client.writer
        .seal({ params: { databaseId }, payload: { successors } })
        .pipe(Effect.catchTags(transport(databaseId, "seal")), Effect.retry(retryRoutable));
      return response.flushedSeq;
    }),
  };
};

/**
 * Constructs the writer client over a `WriterServer` namespace, with one generated HTTP client per
 * database per invocation (Alchemy execution memo + `Cache`), never retained across invocations.
 */
export const makeWriterClientOver = (namespace: {
  readonly getByName: (name: string) => Parameters<typeof Cloudflare.toHttpClient>[0];
}) =>
  Effect.gen(function* () {
    const clients = yield* makeExecutionMemo(
      Cache.make({
        capacity: Number.POSITIVE_INFINITY,
        lookup: (databaseId: DatabaseId) =>
          Effect.suspend(() =>
            HttpApiClient.makeWith(WriterHttpApi, {
              baseUrl: "http://writer.internal",
              httpClient: Cloudflare.toHttpClient(namespace.getByName(databaseId)),
            }),
          ),
      }),
    );
    return WriterClient.of(
      writerClientOperations((databaseId: DatabaseId) =>
        Effect.flatMap(clients, (cache) => Cache.get(cache, databaseId)),
      ),
    );
  });

/** Constructs the writer client over the `WriterServer` namespace the Worker being built hosts. */
export const makeWriterClient = Effect.gen(function* () {
  return yield* makeWriterClientOver(yield* WriterServer);
});

/** Provides the writer client; the Worker's init supplies the `WriterServer` namespace binding. */
export const writerClientLayerWithoutDependencies = Layer.effect(WriterClient, makeWriterClient);

/** Provides the writer client (its only dependency is the `WriterServer` binding). */
export const writerClientLayer = writerClientLayerWithoutDependencies;

/**
 * Provides a writer client backed by in-process writers: one `DbWriter` per database, each with its
 * own in-memory SQLite memtable and recording alarm, over the contextual object store, manifest
 * store, compaction trigger, and settings. Calls cross the real `WriterHttpApi` (encoding, routing, and error statuses)
 * through `Cloudflare.toHttpClient` over the router.
 */
export const writerClientTestLayer = Layer.effect(
  WriterClient,
  Effect.gen(function* () {
    const shared = yield* Effect.context<
      ManifestStore | ManifestReader | ObjectStore | LsmConfiguration | CompactionTrigger
    >();
    const runtime = yield* RuntimeContext;
    const scope = yield* Effect.scope;
    const routers = yield* Cache.make({
      capacity: Number.POSITIVE_INFINITY,
      lookup: (_databaseId: DatabaseId) =>
        Effect.gen(function* () {
          const writerLayer = dbWriterLayerWithoutDependencies.pipe(
            Layer.provide(sstStoreLayerWithoutDependencies),
            Layer.provide(
              memTableLayerWithoutDependencies.pipe(Layer.provide(writerSqlStorageTestLayer)),
            ),
            Layer.provide(durableObjectAlarmTestLayer),
            Layer.provide(Layer.succeedContext(shared)),
          );
          const writer = Context.get(yield* Layer.buildWithScope(writerLayer, scope), DbWriter);
          return yield* HttpRouter.toHttpEffect(
            writerHttpRouterLayer.pipe(Layer.provide(Layer.succeed(DbWriter, writer))),
          ).pipe(Scope.provide(scope));
        }).pipe(
          // A test writer whose SQLite tables cannot be created is a broken test fixture.
          Effect.orDie,
        ),
    });
    const clients = yield* Cache.make({
      capacity: Number.POSITIVE_INFINITY,
      lookup: (databaseId: DatabaseId) =>
        HttpApiClient.makeWith(WriterHttpApi, {
          baseUrl: "http://writer.internal",
          httpClient: Cloudflare.toHttpClient({
            fetch: (request: HttpServerRequest.HttpServerRequest) =>
              Effect.flatMap(Cache.get(routers, databaseId), (router) =>
                router.pipe(
                  Effect.provideService(HttpServerRequest.HttpServerRequest, request),
                  Effect.scoped,
                ),
              ).pipe(Effect.provideService(RuntimeContext, runtime)),
          }),
        }),
    });
    return WriterClient.of(
      writerClientOperations((databaseId: DatabaseId) => Cache.get(clients, databaseId)),
    );
  }),
);
