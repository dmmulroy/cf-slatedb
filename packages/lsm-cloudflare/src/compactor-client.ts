import * as Cloudflare from "@alchemy.run/cloudflare";
import { CompactorEpoch, type DatabaseId } from "@cf-slatedb/lsm-core";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Cache, Context, Effect, Layer, Option, Ref, type Scope } from "effect";
import { HttpApiClient } from "effect/http-api";

import { CompactionTrigger } from "./compaction-trigger.ts";
import {
  type CompactorError,
  type CompactorState,
  CompactorUnavailableError,
} from "./compactor.ts";
import { CompactorHttpApi } from "./compactor-http-api.ts";
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
}

/** The contextual compactor client. */
export class CompactorClient extends Context.Service<CompactorClient, ICompactorClient>()(
  "@cf-slatedb/CompactorClient",
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
    });
  }),
);

/** The databases the test compactor client was asked to destroy, in order. */
export class CompactorClientRecord extends Context.Service<
  CompactorClientRecord,
  Ref.Ref<ReadonlyArray<DatabaseId>>
>()("@cf-slatedb/CompactorClientRecord") {}

/**
 * Provides a compactor client with no compactors: it records the databases it was asked to destroy
 * (in {@link CompactorClientRecord}, also output), reports no state, and wakes nothing. For tests of
 * code that only notifies compactors.
 */
export const compactorClientTestLayer = Layer.effect(
  CompactorClient,
  Effect.gen(function* () {
    const record = yield* CompactorClientRecord;
    return CompactorClient.of({
      wake: (databaseId) =>
        Effect.succeed({
          databaseId,
          compactorEpoch: CompactorEpoch.make(0),
          running: [],
          fenced: false,
        }),
      destroy: (databaseId) => Ref.update(record, (destroyed) => [...destroyed, databaseId]),
      status: () => Effect.succeed(Option.none()),
    });
  }),
).pipe(
  Layer.provideMerge(Layer.effect(CompactorClientRecord, Ref.make<ReadonlyArray<DatabaseId>>([]))),
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
