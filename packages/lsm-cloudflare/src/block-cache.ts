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
} from "@cf-slatedb/lsm-core";
import { Cause, Context, Deferred, Effect, Layer, Metric, Option } from "effect";

import {
  BlockSource,
  type BlockSourceServices,
  type BlockSourceUnavailableError,
  blockSourceLayer,
  blockSourceTestLayer,
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
   */
  readonly meta: (
    owner: DatabaseId,
    handle: SstHandle,
    budgetGroup: string,
  ) => Effect.Effect<SstMeta, BlockReadError, BlockSourceServices>;
  /** One decoded, checksum-verified data block, charged to `budgetGroup`. */
  readonly block: (
    owner: DatabaseId,
    handle: SstHandle,
    entry: SstIndexEntry,
    budgetGroup: string,
  ) => Effect.Effect<DataBlock, BlockReadError, BlockSourceServices>;
}

/** The contextual L1 block cache. */
export class BlockCache extends Context.Service<BlockCache, IBlockCache>()(
  "@cf-slatedb/BlockCache",
) {}

/** Cache key of a byte range of one SST: its owner, id, and range (SSTs are immutable). */
const rangeKey = (owner: DatabaseId, sstId: SstId, offset: number, length: number) =>
  `${owner}/${sstId}/${offset}-${length}`;

/**
 * One L1 cache: decoded values within a {@link GroupByteBudgetLru}, each charged its encoded size,
 * with concurrent misses for a key joined into one read. Failures are never kept.
 */
const makeL1Cache = <A>(budget: { readonly totalBytes: number; readonly groupBytes: number }) => {
  const held = new GroupByteBudgetLru<A>(budget);
  const reading = new Map<string, Deferred.Deferred<A, BlockReadError>>();
  const get = <R>(
    key: string,
    budgetGroup: string,
    bytes: number,
    read: Effect.Effect<A, BlockReadError, R>,
  ): Effect.Effect<A, BlockReadError, R> =>
    Effect.suspend(() => {
      const cached = held.get(key);
      if (Option.isSome(cached)) return Effect.succeed(cached.value);
      const pending = reading.get(key);
      if (pending !== undefined) {
        // The read belongs to another invocation: if that one is interrupted, read again here.
        return Deferred.await(pending).pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? get(key, budgetGroup, bytes, read)
              : Effect.failCause(cause),
          ),
        );
      }
      return Effect.gen(function* () {
        const deferred = yield* Deferred.make<A, BlockReadError>();
        reading.set(key, deferred);
        return yield* read.pipe(
          Effect.tap((value) => Effect.sync(() => held.set(key, budgetGroup, bytes, value))),
          Effect.onExit((exit) =>
            Effect.andThen(
              Effect.sync(() => reading.delete(key)),
              Deferred.done(deferred, exit),
            ),
          ),
        );
      });
    });
  return { has: (key: string) => held.has(key), get };
};

/**
 * Constructs the L1 cache over the contextual block source and settings. Concurrent misses for one
 * key join one read; reads run with the calling invocation's services.
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

  return BlockCache.of({
    meta: (owner, handle, budgetGroup) =>
      Effect.gen(function* () {
        const key = rangeKey(owner, handle.id, handle.metaOffset, handle.metaLength);
        yield* observe(key, metas.has(key), "meta", owner, handle.id);
        return yield* metas.get(
          key,
          budgetGroup,
          handle.metaLength,
          source
            .meta(owner, handle.id, handle.metaOffset, handle.metaLength)
            .pipe(Effect.flatMap((bytes) => Effect.fromResult(decodeSstMeta(handle.id, bytes)))),
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
          source
            .block(owner, handle.id, entry.offset, entry.length)
            .pipe(
              Effect.flatMap((bytes) =>
                Effect.fromResult(
                  decodeDataBlock(bytes, { sstId: handle.id, offset: entry.offset }),
                ),
              ),
            ),
        );
      }).pipe(Effect.withSpan("BlockCache.block")),
  });
});

/** Provides the L1 cache, leaving the block source visible. */
export const blockCacheLayerWithoutDependencies = Layer.effect(BlockCache, makeBlockCache);

/** Provides the L1 cache over the `BlockServer` service binding and the configured budgets. */
export const blockCacheLayer = blockCacheLayerWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(blockSourceLayer, lsmConfigurationLayer)),
);

/** Provides the L1 cache over the in-memory object store (the test block source). */
export const blockCacheTestLayer = blockCacheLayerWithoutDependencies.pipe(
  Layer.provide(Layer.mergeAll(blockSourceTestLayer, lsmConfigurationTestLayer())),
);
