import * as Cloudflare from "@alchemy.run/cloudflare";
import type * as cf from "@cloudflare/workers-types";
import type { DatabaseId, SstId } from "@cf-slatedb/lsm-core";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Data, Effect, Layer, Option, type Scope } from "effect";
import { HttpApiClient } from "effect/http-api";

import { BlockServerHttpApi } from "./block-server-http-api.ts";
import { BlockServerWorker } from "./block-server-worker.ts";
import { sstKey } from "./object-keys.ts";
import { ObjectStore } from "./object-store.ts";

/** The requested SST is gone: the manifest that named it is stale. Reload the manifest and retry. */
export class SstMissingError extends Data.TaggedError("SstMissingError")<{
  readonly owner: DatabaseId;
  readonly sstId: SstId;
  readonly message: string;
}> {}

/** Block storage or the `BlockServer` was unavailable. Retryable. */
export class BlockSourceUnavailableError extends Data.TaggedError("BlockSourceUnavailableError")<{
  readonly owner: DatabaseId;
  /** The SST being read, or none for a database-wide operation such as a purge. */
  readonly sstId: Option.Option<SstId>;
  readonly message: string;
}> {}

/** Services a block read needs from the current invocation. */
export type BlockSourceServices = RuntimeContext | Scope.Scope;

/**
 * Where SST bytes come from on an L1 cache miss: the `BlockServer` (behind Workers Cache) in
 * production, the object store directly in tests.
 */
export interface IBlockSource {
  /** One data block of `owner`'s SST. */
  readonly block: (
    owner: DatabaseId,
    sstId: SstId,
    offset: number,
    length: number,
  ) => Effect.Effect<
    Uint8Array,
    SstMissingError | BlockSourceUnavailableError,
    BlockSourceServices
  >;
  /** One SST's metadata section (`SstHandle.metaOffset`/`metaLength`). */
  readonly meta: (
    owner: DatabaseId,
    sstId: SstId,
    offset: number,
    length: number,
  ) => Effect.Effect<
    Uint8Array,
    SstMissingError | BlockSourceUnavailableError,
    BlockSourceServices
  >;
  /** Purges `databaseId`'s cached blocks (after the database is removed). */
  readonly purge: (
    databaseId: DatabaseId,
  ) => Effect.Effect<boolean, BlockSourceUnavailableError, BlockSourceServices>;
}

/** The contextual block source. */
export class BlockSource extends Context.Service<BlockSource, IBlockSource>()(
  "@cf-slatedb/BlockSource",
) {}

/**
 * Whether a binding value is a service-binding `Fetcher`. The Workers environment is untyped at
 * runtime; this checks the one capability the block source uses.
 */
const isServiceBinding = (value: unknown): value is cf.Fetcher =>
  typeof value === "object" &&
  value !== null &&
  "fetch" in value &&
  typeof value.fetch === "function";

/**
 * Constructs the block source over the `BlockServer` service binding, called with plain `fetch()`
 * (never RPC, which would bypass Workers Cache), with one generated client per invocation.
 */
export const makeBlockSource = Effect.gen(function* () {
  // Registers the service binding on the Worker being built; the RPC stub it returns is unused.
  yield* Cloudflare.Workers.bindWorker(BlockServerWorker);
  // The Worker's runtime environment: present when the Worker runs, absent while Alchemy plans.
  const environment = yield* Effect.serviceOption(Cloudflare.WorkerEnvironment);
  const client = yield* makeExecutionMemo(
    Effect.gen(function* () {
      const binding: unknown = Option.isSome(environment)
        ? environment.value[BlockServerWorker.LogicalId]
        : undefined;
      if (!isServiceBinding(binding)) {
        return yield* Effect.die(
          new Error(
            `The BlockServer service binding ${BlockServerWorker.LogicalId} is missing from this Worker.`,
          ),
        );
      }
      return yield* HttpApiClient.makeWith(BlockServerHttpApi, {
        baseUrl: "http://blocks.internal",
        httpClient: Cloudflare.toHttpClient(Cloudflare.fromCloudflareFetcher(binding)),
      });
    }),
  );

  const failures = (owner: DatabaseId, sstId: SstId) => ({
    BlockNotFoundError: (error: { readonly message: string }) =>
      Effect.fail(new SstMissingError({ owner, sstId, message: error.message })),
    BlockStorageUnavailableError: (error: { readonly message: string }) =>
      Effect.fail(
        new BlockSourceUnavailableError({
          owner,
          sstId: Option.some(sstId),
          message: error.message,
        }),
      ),
    HttpClientError: (error: { readonly message: string }) =>
      Effect.fail(
        new BlockSourceUnavailableError({
          owner,
          sstId: Option.some(sstId),
          message: `The BlockServer could not be reached: ${error.message}`,
        }),
      ),
    SchemaError: (error: { readonly message: string }) =>
      Effect.fail(
        new BlockSourceUnavailableError({
          owner,
          sstId: Option.some(sstId),
          message: `The BlockServer answered unexpectedly: ${error.message}`,
        }),
      ),
  });

  return BlockSource.of({
    block: Effect.fn("BlockSource.block")(function* (
      owner: DatabaseId,
      sstId: SstId,
      offset: number,
      length: number,
    ) {
      yield* Effect.annotateCurrentSpan({
        "database.id": owner,
        "sst.id": sstId,
        "block.offset": offset,
        "block.length": length,
      });
      const blocks = yield* client;
      return yield* blocks.blocks
        .block({ params: { databaseId: owner, sstId, range: `${offset}-${length}` } })
        .pipe(Effect.catchTags(failures(owner, sstId)));
    }),
    meta: Effect.fn("BlockSource.meta")(function* (
      owner: DatabaseId,
      sstId: SstId,
      offset: number,
      length: number,
    ) {
      yield* Effect.annotateCurrentSpan({ "database.id": owner, "sst.id": sstId });
      const blocks = yield* client;
      return yield* blocks.blocks
        .meta({ params: { databaseId: owner, sstId, range: `${offset}-${length}` } })
        .pipe(Effect.catchTags(failures(owner, sstId)));
    }),
    purge: Effect.fn("BlockSource.purge")(function* (databaseId: DatabaseId) {
      yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
      const blocks = yield* client;
      const response = yield* blocks.blocks.purge({ params: { databaseId } }).pipe(
        Effect.catchTags({
          BlockNotFoundError: () => Effect.succeed({ purged: false }),
          BlockStorageUnavailableError: () => Effect.succeed({ purged: false }),
          HttpClientError: (error) =>
            Effect.fail(
              new BlockSourceUnavailableError({
                owner: databaseId,
                sstId: Option.none(),
                message: `The BlockServer could not be reached to purge: ${error.message}`,
              }),
            ),
          SchemaError: () => Effect.succeed({ purged: false }),
        }),
      );
      return response.purged;
    }),
  });
});

/** Provides the block source over the `BlockServer` service binding. */
export const blockSourceLayer = Layer.effect(BlockSource, makeBlockSource);

/**
 * Provides a block source that reads ranges straight from the contextual (in-memory) object store,
 * with the same missing-SST behavior. Workers Cache is not emulated.
 */
export const blockSourceTestLayer = Layer.effect(
  BlockSource,
  Effect.gen(function* () {
    const objects = yield* ObjectStore;
    const read = (owner: DatabaseId, sstId: SstId, offset: number, length: number) =>
      objects.getRange(sstKey(owner, sstId), { offset, length }).pipe(
        Effect.catchTags({
          ObjectStoreUnavailableError: (error) =>
            Effect.fail(
              new BlockSourceUnavailableError({
                owner,
                sstId: Option.some(sstId),
                message: error.message,
              }),
            ),
        }),
        Effect.flatMap((bytes) =>
          Option.isSome(bytes) && bytes.value.length === length
            ? Effect.succeed(bytes.value)
            : Effect.fail(
                new SstMissingError({
                  owner,
                  sstId,
                  message: `SST ${sstId} of database ${owner} does not exist.`,
                }),
              ),
        ),
      );
    return BlockSource.of({
      block: Effect.fn("BlockSource.block")(function* (
        owner: DatabaseId,
        sstId: SstId,
        offset: number,
        length: number,
      ) {
        yield* Effect.annotateCurrentSpan({
          "database.id": owner,
          "sst.id": sstId,
          "block.offset": offset,
        });
        return yield* read(owner, sstId, offset, length);
      }),
      meta: Effect.fn("BlockSource.meta")(function* (
        owner: DatabaseId,
        sstId: SstId,
        offset: number,
        length: number,
      ) {
        yield* Effect.annotateCurrentSpan({ "database.id": owner, "sst.id": sstId });
        return yield* read(owner, sstId, offset, length);
      }),
      purge: () => Effect.succeed(true),
    });
  }),
);
