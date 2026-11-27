import {
  type DataBlock,
  type DatabaseId,
  decodeDataBlock,
  GroupByteBudgetLru,
  decodeSstMeta,
  type SstCorruptedError,
  type SstHandle,
  type SstId,
  type SstIndexEntry,
  type SstMeta,
} from "@shaledb/lsm-core";
import { Context, Effect, Layer, Metric, Option, type Result } from "effect";

import {
  BlockSource,
  type BlockSourceServices,
  type BlockSourceUnavailableError,
  blockSourceTestLayer,
  hedgedBlockSourceLayer,
  type SstMissingError,
} from "./block-source.ts";
import {
  LsmConfiguration,
  lsmConfigurationLayer,
  lsmConfigurationTestLayer,
} from "./lsm-configuration.ts";

/** Block cache lookups by tier and outcome (SlateDB RFC-0021 parity). */
export const blockCacheRequests = Metric.counter("lsm_block_cache_requests", {
  description: "L1 block cache lookups; the attributes say which cache and whether it hit",
});

/** Failures a block read can report. */
export type BlockReadError = SstMissingError | BlockSourceUnavailableError | SstCorruptedError;

/**
 * The isolate's L1 block cache: decoded data blocks and SST metadata, in two separate caches so a
 * scan does not evict the indexes and filters point reads need (SlateDB's split cache). Both are
 * bounded by bytes, per `budgetGroup` and in total (`l1DataCacheBytes`, `l1MetaCacheBytes`,
 * `l1GroupCacheShare`; D51), so one tenant's scans cannot empty another's cache.
 */
export interface IBlockCache {
  /**
   * An SST's decoded metadata, read once per isolate (one request: `metaOffset`/`metaLength`),
   * charged to `budgetGroup` (the reading database's group, or its id).
   *
   * @returns The metadata, or `SstMissingError`, `BlockSourceUnavailableError` (retryable), or
   *   `SstCorruptedError` (not retryable); failures are never kept.
   */
  readonly meta: (
    owner: DatabaseId,
    handle: SstHandle,
    budgetGroup: string,
  ) => Effect.Effect<SstMeta, BlockReadError, BlockSourceServices>;
  /**
   * One decoded, checksum-verified data block, charged to `budgetGroup`.
   *
   * @returns The block, or the same failures as `meta`; failures are never kept.
   */
  readonly block: (
    owner: DatabaseId,
    handle: SstHandle,
    entry: SstIndexEntry,
    budgetGroup: string,
  ) => Effect.Effect<DataBlock, BlockReadError, BlockSourceServices>;
}

/** The contextual L1 block cache. */
export class BlockCache extends Context.Service<BlockCache, IBlockCache>()("@shaledb/BlockCache") {}

/** Cache key of a byte range of one SST: its owner, id, and range (SSTs are immutable). */
const rangeKey = (owner: DatabaseId, sstId: SstId, offset: number, length: number) =>
  `${owner}/${sstId}/${offset}-${length}`;

/**
 * One L1 cache: decoded values within a {@link GroupByteBudgetLru}, each charged its encoded size.
 * Failures are never kept.
 *
 * **Details**
 *
 * The isolate shares the values, which are plain data, across requests. It does not share reads in
 * flight (D66): a Worker's I/O belongs to the request that started it, so a request that waited on
 * another request's read hung when the runtime cancelled that request (a client that hung up, an
 * isolate retired by a deploy), and once it ended its work threw "Cannot call this
 * AsyncLocalStorage bound function outside of the request in which it was created" in the waiter
 * (found by the soak). Concurrent misses for one key each read; Workers Cache (L2) and the
 * BlockServer answer the duplicates.
 */
const makeL1Cache = <A>(budget: { readonly totalBytes: number; readonly groupBytes: number }) => {
  const held = new GroupByteBudgetLru<A>(budget);
  const get = <R>(
    key: string,
    budgetGroup: string,
    bytes: number,
    read: Effect.Effect<A, BlockReadError, R>,
  ): Effect.Effect<A, BlockReadError, R> =>
    Effect.suspend(() => {
      const cached = held.get(key);
      if (Option.isSome(cached)) return Effect.succeed(cached.value);
      return read.pipe(
        Effect.tap((value) => Effect.sync(() => held.set(key, budgetGroup, bytes, value))),
      );
    });
  return { has: (key: string) => held.has(key), get };
};

/**
 * Constructs the L1 cache over the contextual block source and settings. Each miss reads with the
 * calling invocation's services, never waiting on another invocation's read (D66).
 */
export const makeBlockCache = Effect.gen(function* () {
  const source = yield* BlockSource;
  const settings = yield* LsmConfiguration;
  const metas = makeL1Cache<SstMeta>({
    totalBytes: settings.l1MetaCacheBytes,
    groupBytes: Math.ceil(settings.l1MetaCacheBytes * settings.l1GroupCacheShare),
  });
  const blocks = makeL1Cache<DataBlock>({
    totalBytes: settings.l1DataCacheBytes,
    groupBytes: Math.ceil(settings.l1DataCacheBytes * settings.l1GroupCacheShare),
  });

  const observe = (
    key: string,
    hit: boolean,
    tier: "meta" | "data",
    owner: DatabaseId,
    sstId: SstId,
  ) =>
    Effect.gen(function* () {
      yield* Effect.annotateCurrentSpan({
        "cache.hit": hit,
        "cache.tier": tier,
        "sst.id": sstId,
        "database.id": owner,
      });
      yield* Metric.update(
        Metric.withAttributes(blockCacheRequests, { tier, hit: String(hit) }),
        1,
      );
      return key;
    });

  /**
   * Decodes a range read by `read`; when its bytes fail their checksum, reads the range once more
   * past every cache (`BlockSource.uncached`). A damaged copy in Workers Cache, or one damaged in
   * transit, then costs one bucket read instead of failing every read of the block for as long as
   * the copy is cached (an immutable year). Damage in the bucket itself still fails, typed.
   */
  const decodedOrUncached = <A>(
    owner: DatabaseId,
    sstId: SstId,
    read: Effect.Effect<
      Uint8Array,
      SstMissingError | BlockSourceUnavailableError,
      BlockSourceServices
    >,
    offset: number,
    length: number,
    decode: (bytes: Uint8Array) => Result.Result<A, SstCorruptedError>,
  ): Effect.Effect<A, BlockReadError, BlockSourceServices> =>
    read.pipe(
      Effect.flatMap((bytes) => Effect.fromResult(decode(bytes))),
      Effect.catchTags({
        SstCorruptedError: (damaged) =>
          source.uncached(owner, sstId, offset, length).pipe(
            Effect.flatMap((bytes) => Effect.fromResult(decode(bytes))),
            Effect.tap(() =>
              Effect.annotateCurrentSpan({ "cache.damaged_copy": true }).pipe(
                Effect.andThen(
                  Effect.logWarning(
                    `A cached or in-transit copy of SST ${sstId} of database ${owner} at offset ${offset} failed its checksum (${damaged.reason} in ${damaged.section}); the bucket's copy is intact and was used.`,
                  ),
                ),
              ),
            ),
          ),
      }),
    );

  return BlockCache.of({
    meta: (owner, handle, budgetGroup) =>
      Effect.gen(function* () {
        const key = rangeKey(owner, handle.id, handle.metaOffset, handle.metaLength);
        yield* observe(key, metas.has(key), "meta", owner, handle.id);
        return yield* metas.get(
          key,
          budgetGroup,
          handle.metaLength,
          decodedOrUncached(
            owner,
            handle.id,
            source.meta(owner, handle.id, handle.metaOffset, handle.metaLength),
            handle.metaOffset,
            handle.metaLength,
            (bytes) => decodeSstMeta(handle.id, bytes),
          ),
        );
      }).pipe(Effect.withSpan("BlockCache.meta")),
    block: (owner, handle, entry, budgetGroup) =>
      Effect.gen(function* () {
        const key = rangeKey(owner, handle.id, entry.offset, entry.length);
        yield* observe(key, blocks.has(key), "data", owner, handle.id);
        return yield* blocks.get(
          key,
          budgetGroup,
          entry.length,
          decodedOrUncached(
            owner,
            handle.id,
            source.block(owner, handle.id, entry.offset, entry.length),
            entry.offset,
            entry.length,
            (bytes) => decodeDataBlock(bytes, { sstId: handle.id, offset: entry.offset }),
          ),
        );
      }).pipe(Effect.withSpan("BlockCache.block")),
  });
});

/** Provides the L1 cache, leaving the block source visible. */
export const blockCacheLayerWithoutDependencies = Layer.effect(BlockCache, makeBlockCache);

/** Provides the L1 cache over the `BlockServer` service binding and the configured budgets. */
export const blockCacheLayer = blockCacheLayerWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(hedgedBlockSourceLayer, lsmConfigurationLayer)),
);

/** Provides the L1 cache over the in-memory object store (the test block source). */
export const blockCacheTestLayer = blockCacheLayerWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(blockSourceTestLayer, lsmConfigurationTestLayer())),
);
