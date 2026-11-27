import * as Cloudflare from "@alchemy.run/cloudflare";
import type * as cf from "@cloudflare/workers-types";
import type { DatabaseId, SstId } from "@shaledb/lsm-core";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import {
  Cause,
  Context,
  Data,
  Deferred,
  Duration,
  Effect,
  Layer,
  Option,
  type Scope,
} from "effect";
import { HttpClient } from "effect/http";
import { HttpApiClient } from "effect/http-api";

import { BlockServerHttpApi } from "./block-server-http-api.ts";
import { BlockServerWorker } from "./block-server-worker.ts";
import { LsmConfiguration, lsmConfigurationLayer } from "./lsm-configuration.ts";
import { lsmJurisdiction, lsmStackHasWorkersCache } from "./lsm-jurisdiction.ts";
import { sstKey } from "./object-keys.ts";
import {
  type IObjectReader,
  ObjectReader,
  objectReaderFromObjectStoreLayer,
} from "./object-reader.ts";
import { ObjectStore, objectStoreLayer } from "./object-store.ts";

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
  /**
   * The same byte range, read past every cache: for a read whose bytes failed their checksum, which
   * may be a damaged cached or in-transit copy of an intact object (read audit). Where the Stack
   * has Workers Cache it reads the bucket; otherwise the `BlockServer` reads the bucket on every
   * request, so this reads through it again.
   */
  readonly uncached: (
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
  "@shaledb/BlockSource",
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
        httpClient: Cloudflare.toHttpClient(Cloudflare.fromCloudflareFetcher(binding)).pipe(
          // Workers Cache answers in front of the BlockServer: `HIT` means it did not run.
          HttpClient.tap((response) =>
            Effect.annotateCurrentSpan(
              "cache.l2.status",
              response.headers["cf-cache-status"] ?? "absent",
            ),
          ),
        ),
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

  const block = Effect.fn("BlockSource.block")(function* (
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
  });

  return BlockSource.of({
    block,
    // Through the BlockServer again: right where it runs without Workers Cache (a restricted
    // Stack), which is the only place this source is used for reads; `hedgeBlockSource` reads the
    // bucket instead where there is a cache.
    uncached: block,
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

/** Reads a byte range of `owner`'s SST straight from the bucket, as the `BlockServer` does. */
const readSstRangeFromBucket =
  (getRange: IObjectReader["getRange"]) =>
  (owner: DatabaseId, sstId: SstId, offset: number, length: number) =>
    getRange(sstKey(owner, sstId), { offset, length }).pipe(
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

/**
 * `primary`, and once it has taken `hedgeAfterMs` or failed, `direct` as well: whichever succeeds
 * first.
 *
 * **Details**
 *
 * `primary` is a read through the `BlockServer`. Workers Cache answers most of them in a few
 * milliseconds, but the slowest take hundreds of milliseconds to seconds (a miss pays the hop
 * to the `BlockServer` on top of its bucket read; a rare hit is slow). `direct` reads the same
 * immutable bytes from the bucket, which costs one more bucket read only when `primary` is slow
 * or failed.
 *
 * A failed arm leaves the answer to the other (D134; before it, a `primary` failure failed the
 * read even with `direct` in flight). When `primary` fails before the hedge delay, `direct`
 * starts at once (`block.primary_failed`). The read fails only when both have failed, and then
 * with `primary`'s failure, so every failure a caller sees is still the `BlockServer`'s. The
 * loser is interrupted. With `hedgeAfterMs` 0 this is `primary`: a Stack without Workers Cache,
 * where the `BlockServer`'s own retry of R2's transient failures (D134) protects the read.
 */
export const hedgedBlockRead = <A, E, R, E2, R2>(
  primary: Effect.Effect<A, E, R>,
  direct: Effect.Effect<A, E2, R2>,
  hedgeAfterMs: number,
): Effect.Effect<A, E, R | R2> =>
  hedgeAfterMs <= 0
    ? primary
    : Effect.gen(function* () {
        const primaryFailed = yield* Deferred.make<Cause.Cause<E>>();
        // A failed primary does not end the race: it hands its cause to the direct arm.
        const primaryArm = primary.pipe(
          Effect.catchCause((cause) =>
            Deferred.succeed(primaryFailed, cause).pipe(Effect.andThen(Effect.never)),
          ),
        );
        const directArm = Effect.raceFirst(
          Effect.sleep(Duration.millis(hedgeAfterMs)),
          Deferred.await(primaryFailed).pipe(
            Effect.andThen(Effect.annotateCurrentSpan("block.primary_failed", true)),
          ),
        ).pipe(
          Effect.andThen(Effect.annotateCurrentSpan("block.hedged", true)),
          Effect.andThen(direct),
          Effect.tap(() => Effect.annotateCurrentSpan("block.hedge_won", true)),
          // Both failed: the read fails with the BlockServer's failure.
          Effect.catchCause(() => Effect.flatMap(Deferred.await(primaryFailed), Effect.failCause)),
        );
        return yield* Effect.raceFirst(primaryArm, directArm);
      });

/**
 * `blockServer` with each block and metadata read hedged by a read straight from the contextual
 * bucket after `blockReadHedgeAfterMs` (see {@link hedgedBlockRead}), when the Stack has Workers
 * Cache; otherwise `blockServer` itself.
 *
 * **Details**
 *
 * In a Stack restricted to a jurisdiction the `BlockServer` runs without Workers Cache (D49), so
 * it reads the same bucket by the same road, and a hedge doubles the bucket reads and wins
 * nothing: on `preview-2` (`eu`) 90% of block reads were hedged and the bucket won 2% of those
 * (D72). The rule is the one the `BlockServer` uses, {@link lsmStackHasWorkersCache}. A local
 * stage has no jurisdiction and hedges: Workers Cache is not emulated there, but its scenarios
 * then exercise the hedge.
 *
 * @param blockServer - Reads through the `BlockServer` (production: {@link makeBlockSource}).
 */
export const hedgeBlockSource = (blockServer: IBlockSource) =>
  Effect.gen(function* () {
    if (!lsmStackHasWorkersCache(yield* lsmJurisdiction)) return BlockSource.of(blockServer);
    const objects = yield* ObjectReader;
    const settings = yield* LsmConfiguration;
    const fromBucket = readSstRangeFromBucket(objects.getRange);
    // No span of its own: the `BlockServer` read keeps the one `BlockSource.block` span per fetch,
    // the bucket read has `ObjectReader.getRange`, and `block.hedged` lands on the caller's span.
    return BlockSource.of({
      block: (owner, sstId, offset, length) =>
        hedgedBlockRead(
          blockServer.block(owner, sstId, offset, length),
          fromBucket(owner, sstId, offset, length),
          settings.blockReadHedgeAfterMs,
        ),
      meta: (owner, sstId, offset, length) =>
        hedgedBlockRead(
          blockServer.meta(owner, sstId, offset, length),
          fromBucket(owner, sstId, offset, length),
          settings.blockReadHedgeAfterMs,
        ),
      uncached: fromBucket,
      purge: blockServer.purge,
    });
  });

/**
 * Constructs the block source readers use: the `BlockServer` service binding, hedged with reads
 * straight from the bucket where the Stack has Workers Cache (see {@link hedgeBlockSource}).
 */
export const makeHedgedBlockSource = Effect.flatMap(makeBlockSource, hedgeBlockSource);

/** Provides the block source over the `BlockServer` service binding alone (for purges). */
export const blockSourceLayer = Layer.effect(BlockSource, makeBlockSource);

/** Provides the hedged block source over the `BlockServer` service binding and the R2 bucket. */
export const hedgedBlockSourceLayer = Layer.effect(BlockSource, makeHedgedBlockSource).pipe(
  Layer.provide(objectReaderFromObjectStoreLayer),
  Layer.provide(Layer.mergeAll(objectStoreLayer, lsmConfigurationLayer)),
);

/**
 * Provides a block source that reads ranges straight from the contextual (in-memory) object store,
 * with the same missing-SST behavior. Workers Cache is not emulated.
 */
export const blockSourceTestLayer = Layer.effect(
  BlockSource,
  Effect.gen(function* () {
    const objects = yield* ObjectStore;
    const read = readSstRangeFromBucket(objects.getRange);
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
      uncached: read,
      purge: () => Effect.succeed(true),
    });
  }),
);
