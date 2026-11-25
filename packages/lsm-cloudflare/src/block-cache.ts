import {
  type DataBlock,
  type DatabaseId,
  decodeDataBlock,
  decodeSstMeta,
  type SstCorruptedError,
  type SstHandle,
  type SstId,
  type SstIndexEntry,
  type SstMeta,
} from "@cf-slatedb/lsm-core";
import { Cache, Context, Data, Duration, Effect, Exit, Layer, Metric } from "effect";

import {
  BlockSource,
  type BlockSourceServices,
  type BlockSourceUnavailableError,
  blockSourceLayer,
  blockSourceTestLayer,
  type SstMissingError,
} from "./block-source.ts";

/** Data blocks the isolate keeps (4 KiB blocks: about 16 MiB). */
export const DATA_BLOCK_CACHE_CAPACITY = 4096;

/** SST metadata sections (index, filter, footer) the isolate keeps, apart from data blocks. */
export const META_CACHE_CAPACITY = 1024;

/** Block cache lookups by tier and outcome (SlateDB RFC-0021 parity). */
export const blockCacheRequests = Metric.counter("lsm_block_cache_requests", {
  description: "L1 block cache lookups; the attributes say which cache and whether it hit",
});

/** Failures a block read can report. */
export type BlockReadError = SstMissingError | BlockSourceUnavailableError | SstCorruptedError;

/**
 * The isolate's L1 block cache: decoded data blocks and SST metadata, in two separate caches so a
 * scan does not evict the indexes and filters point reads need (SlateDB's split cache).
 */
export interface IBlockCache {
  /** An SST's decoded metadata, read once per isolate (one request: `metaOffset`/`metaLength`). */
  readonly meta: (
    owner: DatabaseId,
    handle: SstHandle,
  ) => Effect.Effect<SstMeta, BlockReadError, BlockSourceServices>;
  /** One decoded, checksum-verified data block. */
  readonly block: (
    owner: DatabaseId,
    handle: SstHandle,
    entry: SstIndexEntry,
  ) => Effect.Effect<DataBlock, BlockReadError, BlockSourceServices>;
}

/** The contextual L1 block cache. */
export class BlockCache extends Context.Service<BlockCache, IBlockCache>()(
  "@cf-slatedb/BlockCache",
) {}

/** Cache key of a byte range of one SST. */
class RangeKey extends Data.Class<{
  readonly owner: DatabaseId;
  readonly sstId: SstId;
  readonly offset: number;
  readonly length: number;
}> {}

/** Keep successes forever (SSTs are immutable); never keep failures. */
const keepSuccesses = <A, E>(exit: Exit.Exit<A, E>) =>
  Exit.isSuccess(exit) ? Duration.infinity : Duration.zero;

/**
 * Constructs the L1 cache over the contextual block source. Concurrent misses for one key join one
 * lookup; lookups run with the calling invocation's services.
 */
export const makeBlockCache = Effect.gen(function* () {
  const source = yield* BlockSource;
  const metas = yield* Cache.makeWith(
    (key: RangeKey) =>
      source
        .meta(key.owner, key.sstId, key.offset, key.length)
        .pipe(Effect.flatMap((bytes) => Effect.fromResult(decodeSstMeta(key.sstId, bytes)))),
    { capacity: META_CACHE_CAPACITY, timeToLive: keepSuccesses, requireServicesAt: "lookup" },
  );
  const blocks = yield* Cache.makeWith(
    (key: RangeKey) =>
      source
        .block(key.owner, key.sstId, key.offset, key.length)
        .pipe(
          Effect.flatMap((bytes) =>
            Effect.fromResult(decodeDataBlock(bytes, { sstId: key.sstId, offset: key.offset })),
          ),
        ),
    { capacity: DATA_BLOCK_CACHE_CAPACITY, timeToLive: keepSuccesses, requireServicesAt: "lookup" },
  );

  const lookup = <A, E, R>(
    cache: Cache.Cache<RangeKey, A, E, R>,
    key: RangeKey,
    tier: "meta" | "data",
  ) =>
    Effect.gen(function* () {
      const hit = yield* Cache.has(cache, key);
      yield* Effect.annotateCurrentSpan({
        "cache.hit": hit,
        "cache.tier": tier,
        "sst.id": key.sstId,
        "database.id": key.owner,
      });
      yield* Metric.update(
        Metric.withAttributes(blockCacheRequests, { tier, hit: String(hit) }),
        1,
      );
      return yield* Cache.get(cache, key);
    });

  return BlockCache.of({
    meta: (owner, handle) =>
      lookup(
        metas,
        new RangeKey({
          owner,
          sstId: handle.id,
          offset: handle.metaOffset,
          length: handle.metaLength,
        }),
        "meta",
      ).pipe(Effect.withSpan("BlockCache.meta")),
    block: (owner, handle, entry) =>
      lookup(
        blocks,
        new RangeKey({ owner, sstId: handle.id, offset: entry.offset, length: entry.length }),
        "data",
      ).pipe(Effect.withSpan("BlockCache.block")),
  });
});

/** Provides the L1 cache, leaving the block source visible. */
export const blockCacheLayerWithoutDependencies = Layer.effect(BlockCache, makeBlockCache);

/** Provides the L1 cache over the `BlockServer` service binding. */
export const blockCacheLayer = blockCacheLayerWithoutDependencies.pipe(
  Layer.provide(blockSourceLayer),
);

/** Provides the L1 cache over the in-memory object store (the test block source). */
export const blockCacheTestLayer = blockCacheLayerWithoutDependencies.pipe(
  Layer.provide(blockSourceTestLayer),
);
