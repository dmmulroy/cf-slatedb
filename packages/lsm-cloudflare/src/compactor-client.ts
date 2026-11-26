import * as Cloudflare from "@alchemy.run/cloudflare";
import { CompactorEpoch, type DatabaseId, type Timestamp } from "@shaledb/lsm-core";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Cache, Context, Effect, Layer, Option, Ref, type Scope } from "effect";
import { HttpApiClient } from "effect/http-api";

import { CompactionTrigger } from "./compaction-trigger.ts";
import {
  type CompactorError,
  type CompactorState,
  CompactorUnavailableError,
  type SstDropError,
  type SstDropReport,
  type SstDropRequest,
} from "./compactor.ts";
import { CompactorHttpApi } from "./compactor-http-api.ts";
import type {
  GarbageCollectionReport,
  GarbageCollectorUnavailableError,
} from "./garbage-collector.ts";
import { CompactorServer } from "./compactor-server.ts";
import { CompactorWorker } from "./compactor-worker.ts";
import { inLsmJurisdiction, lsmJurisdiction } from "./lsm-jurisdiction.ts";

/** Calls a database's compactor (`CompactorServer`, hosted by `CompactorWorker`) over HTTP. */
export interface ICompactorClient {
  /** Wakes the compactor (fencing in on first use) so it polls now. */
  readonly wake: (
    databaseId: DatabaseId,
  ) => Effect.Effect<CompactorState, CompactorError, RuntimeContext | Scope.Scope>;
  /** Forgets the compactor's state and alarm for a database being removed. */
  readonly destroy: (
    databaseId: DatabaseId,
  ) => Effect.Effect<void, CompactorError, RuntimeContext | Scope.Scope>;
  /** The compactor's state; none if it was never woken. */
  readonly status: (
    databaseId: DatabaseId,
  ) => Effect.Effect<Option.Option<CompactorState>, CompactorError, RuntimeContext | Scope.Scope>;
  /** Has the database's compactor collect its garbage now (`Compactor.collect`). */
  readonly collect: (
    databaseId: DatabaseId,
  ) => Effect.Effect<
    GarbageCollectionReport,
    CompactorError | GarbageCollectorUnavailableError,
    RuntimeContext | Scope.Scope
  >;
  /** Makes a collection due now on the database's compactor (`Compactor.requestCollection`). */
  readonly requestCollection: (
    databaseId: DatabaseId,
  ) => Effect.Effect<void, CompactorError, RuntimeContext | Scope.Scope>;
  /** Has the database's compactor drop one SST from the manifest (`Compactor.dropSst`, D62). */
  readonly dropSst: (
    databaseId: DatabaseId,
    request: SstDropRequest,
  ) => Effect.Effect<SstDropReport, SstDropError, RuntimeContext | Scope.Scope>;
}

/** The contextual compactor client. */
export class CompactorClient extends Context.Service<CompactorClient, ICompactorClient>()(
  "@shaledb/CompactorClient",
) {}

/**
 * Provides the compactor client over the `CompactorServer` namespace of `CompactorWorker` (a
 * cross-script binding on the Worker being built), one generated HTTP client per database per
 * invocation.
 */
export const compactorClientLayer = Layer.effect(
  CompactorClient,
  Effect.gen(function* () {
    const namespace = yield* CompactorServer.from(CompactorWorker);
    const jurisdiction = yield* lsmJurisdiction;
    const clients = yield* makeExecutionMemo(
      Cache.make({
        capacity: Number.POSITIVE_INFINITY,
        lookup: (databaseId: DatabaseId) =>
          Effect.suspend(() =>
            HttpApiClient.makeWith(CompactorHttpApi, {
              baseUrl: "http://compactor.internal",
              httpClient: Cloudflare.toHttpClient(
                inLsmJurisdiction(namespace, jurisdiction).getByName(databaseId),
              ),
            }),
          ),
      }),
    );
    const client = (databaseId: DatabaseId) =>
      Effect.flatMap(clients, (cache) => Cache.get(cache, databaseId));
    const unreachable = (databaseId: DatabaseId) => (error: { readonly message: string }) =>
      Effect.fail(
        new CompactorUnavailableError({
          databaseId,
          message: `The compactor of database ${databaseId} could not be reached: ${error.message} Retry.`,
        }),
      );
    return CompactorClient.of({
      wake: Effect.fn("CompactorClient.wake")(function* (databaseId: DatabaseId) {
        yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
        return yield* (yield* client(databaseId)).compactor.wake({ params: { databaseId } }).pipe(
          Effect.catchTags({
            HttpClientError: unreachable(databaseId),
            SchemaError: unreachable(databaseId),
          }),
        );
      }),
      destroy: Effect.fn("CompactorClient.destroy")(function* (databaseId: DatabaseId) {
        yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
        yield* (yield* client(databaseId)).compactor.destroy({ params: { databaseId } }).pipe(
          Effect.catchTags({
            HttpClientError: unreachable(databaseId),
            SchemaError: unreachable(databaseId),
          }),
        );
      }),
      status: Effect.fn("CompactorClient.status")(function* (databaseId: DatabaseId) {
        yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
        const response = yield* (yield* client(databaseId)).compactor
          .status({ params: { databaseId } })
          .pipe(
            Effect.catchTags({
              HttpClientError: unreachable(databaseId),
              SchemaError: unreachable(databaseId),
            }),
          );
        return response.state;
      }),
      collect: Effect.fn("CompactorClient.collect")(function* (databaseId: DatabaseId) {
        yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
        return yield* (yield* client(databaseId)).compactor
          .collect({ params: { databaseId } })
          .pipe(
            Effect.catchTags({
              HttpClientError: unreachable(databaseId),
              SchemaError: unreachable(databaseId),
            }),
          );
      }),
      requestCollection: Effect.fn("CompactorClient.requestCollection")(function* (
        databaseId: DatabaseId,
      ) {
        yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
        yield* (yield* client(databaseId)).compactor
          .requestCollection({ params: { databaseId } })
          .pipe(
            Effect.catchTags({
              HttpClientError: unreachable(databaseId),
              SchemaError: unreachable(databaseId),
            }),
          );
      }),
      dropSst: Effect.fn("CompactorClient.dropSst")(function* (
        databaseId: DatabaseId,
        request: SstDropRequest,
      ) {
        yield* Effect.annotateCurrentSpan({ "database.id": databaseId, "sst.id": request.sstId });
        return yield* (yield* client(databaseId)).compactor
          .dropSst({ params: { databaseId }, payload: request })
          .pipe(
            Effect.catchTags({
              HttpClientError: unreachable(databaseId),
              SchemaError: unreachable(databaseId),
            }),
          );
      }),
    });
  }),
);

/** What the test compactor client was asked to do, by database, in order. */
export interface CompactorClientCalls {
  readonly destroyed: ReadonlyArray<DatabaseId>;
  readonly woken: ReadonlyArray<DatabaseId>;
  readonly collectionsRequested: ReadonlyArray<DatabaseId>;
}

/** The calls the test compactor client recorded. */
export class CompactorClientRecord extends Context.Service<
  CompactorClientRecord,
  Ref.Ref<CompactorClientCalls>
>()("@shaledb/CompactorClientRecord") {}

/**
 * Provides a compactor client with no compactors: it records the databases it was asked to destroy,
 * wake, or collect later (in {@link CompactorClientRecord}, also output), reports no state, and runs
 * nothing; an immediate collection is unavailable. For tests of code that only notifies compactors.
 */
export const compactorClientTestLayer = Layer.effect(
  CompactorClient,
  Effect.gen(function* () {
    const record = yield* CompactorClientRecord;
    return CompactorClient.of({
      wake: (databaseId) =>
        Ref.update(record, (calls) => ({ ...calls, woken: [...calls.woken, databaseId] })).pipe(
          Effect.as({
            formatVersion: 1,
            databaseId,
            compactorEpoch: CompactorEpoch.make(0),
            running: [],
            fenced: false,
            corruptSsts: [],
            collectionDueAt: Option.none<Timestamp>(),
          }),
        ),
      destroy: (databaseId) =>
        Ref.update(record, (calls) => ({ ...calls, destroyed: [...calls.destroyed, databaseId] })),
      status: () => Effect.succeed(Option.none()),
      collect: (databaseId) =>
        Effect.fail(
          new CompactorUnavailableError({
            databaseId,
            message: `The test compactor client runs no collection for database ${databaseId}.`,
          }),
        ),
      requestCollection: (databaseId) =>
        Ref.update(record, (calls) => ({
          ...calls,
          collectionsRequested: [...calls.collectionsRequested, databaseId],
        })),
      dropSst: (databaseId) =>
        Effect.fail(
          new CompactorUnavailableError({
            databaseId,
            message: `The test compactor client drops no SSTs of database ${databaseId}.`,
          }),
        ),
    });
  }),
).pipe(
  Layer.provideMerge(
    Layer.effect(
      CompactorClientRecord,
      Ref.make<CompactorClientCalls>({ destroyed: [], woken: [], collectionsRequested: [] }),
    ),
  ),
);

/**
 * Provides the writer's compaction trigger over the compactor client: a failed wake is logged and
 * left to the next flush.
 */
export const compactionTriggerLayer = Layer.effect(
  CompactionTrigger,
  Effect.gen(function* () {
    const compactors = yield* CompactorClient;
    return CompactionTrigger.of({
      wake: (databaseId) =>
        compactors.wake(databaseId).pipe(
          Effect.scoped,
          Effect.asVoid,
          Effect.catchCause((cause) =>
            Effect.logWarning("Waking the compactor failed; the next flush wakes it again.", {
              databaseId,
              cause,
            }),
          ),
        ),
    });
  }),
);
