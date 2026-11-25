import {
  DatabaseId,
  intersectKeyRanges,
  type KeyRange,
  type Row,
  type SeqNum,
  type Timestamp,
  type WriteBatch,
  type WriteReceipt,
} from "@cf-slatedb/lsm-core";
import {
  DbClient,
  type DbClientReadError,
  type DbReaderServices,
  type DbWriteError,
  type ReadOptions,
  type ScanReadOptions,
} from "@cf-slatedb/lsm-cloudflare";
import { Context, Effect, Layer, Option, Ref, Schedule, Schema, Stream } from "effect";

import { ShardDirectory, type ShardDirectoryError } from "./shard-directory.ts";
import { ShardGroup, type ShardMap, shardFor, shardsOverlapping } from "./shard-map.ts";

/** A batch's keys belong to more than one shard. Not retryable: send one batch per shard. */
export class CrossShardBatchError extends Schema.TaggedError<CrossShardBatchError>()(
  "CrossShardBatchError",
  { group: ShardGroup, databases: Schema.Array(DatabaseId), message: Schema.String },
) {}

/** A write's receipt, with the shard's database (a receipt's `seq` is per database). */
export interface ShardedWriteReceipt {
  readonly databaseId: DatabaseId;
  readonly seq: SeqNum;
  readonly createTs: Timestamp;
}

/** Every failure a routed write can report. */
export type ShardRouterWriteError = DbWriteError | ShardDirectoryError | CrossShardBatchError;

/** Every failure a routed read can report. */
export type ShardRouterReadError = DbClientReadError | ShardDirectoryError;

/**
 * Routes keys to the databases of a shard group (design §12, `ShardRouter`).
 *
 * **Details**
 *
 * The group's map is cached in the isolate. A stale map can only reach a sealed, out-of-range, or
 * removed database, and each fails with a typed error, so the router refreshes the map and retries;
 * while a split is publishing (the database is sealed and the map not yet updated) it retries for a
 * few seconds. The core keeps no map versions.
 */
export interface IShardRouter {
  readonly put: (
    group: ShardGroup,
    key: Uint8Array,
    value: Uint8Array,
  ) => Effect.Effect<ShardedWriteReceipt, ShardRouterWriteError, DbReaderServices>;
  readonly delete: (
    group: ShardGroup,
    key: Uint8Array,
  ) => Effect.Effect<ShardedWriteReceipt, ShardRouterWriteError, DbReaderServices>;
  readonly merge: (
    group: ShardGroup,
    key: Uint8Array,
    operand: Uint8Array,
  ) => Effect.Effect<ShardedWriteReceipt, ShardRouterWriteError, DbReaderServices>;
  /** Commits a batch whose keys all belong to one shard, atomically. */
  readonly write: (
    group: ShardGroup,
    batch: WriteBatch,
  ) => Effect.Effect<ShardedWriteReceipt, ShardRouterWriteError, DbReaderServices>;
  readonly get: (
    group: ShardGroup,
    key: Uint8Array,
    options: ReadOptions,
  ) => Effect.Effect<Option.Option<Uint8Array>, ShardRouterReadError, DbReaderServices>;
  /** Visible rows in `range` across the shards it spans, in key order. */
  readonly scan: (
    group: ShardGroup,
    range: KeyRange,
    options: ScanReadOptions,
  ) => Stream.Stream<Row, ShardRouterReadError, DbReaderServices>;
  /** The group's map as this router last read it, refreshed first. */
  readonly refresh: (
    group: ShardGroup,
  ) => Effect.Effect<ShardMap, ShardDirectoryError, DbReaderServices>;
}

/** The contextual shard router. */
export class ShardRouter extends Context.Service<ShardRouter, IShardRouter>()(
  "@cf-slatedb/ShardRouter",
) {}

/** Failures that mean the cached map is stale (or a split is still publishing). */
const isRoutingError = (error: { readonly _tag: string }) =>
  error._tag === "DatabaseSealedError" ||
  error._tag === "KeyOutOfRangeError" ||
  error._tag === "DatabaseNotFoundError";

/** Refresh-and-retry: every 100 ms for five seconds, the time a split takes to publish. */
const routingRetry = {
  while: isRoutingError,
  schedule: Schedule.spaced("100 millis"),
  times: 50,
};

/** Constructs the router over the contextual database client and shard directory. */
export const makeShardRouter = Effect.gen(function* () {
  const client = yield* DbClient;
  const directory = yield* ShardDirectory;
  const maps = yield* Ref.make(new Map<ShardGroup, ShardMap>());

  const refresh = Effect.fn("ShardRouter.refresh")(function* (group: ShardGroup) {
    const map = yield* directory.map(group);
    yield* Ref.update(maps, (current) => new Map(current).set(group, map));
    yield* Effect.annotateCurrentSpan({ "shard.group": group, "shard.version": map.version });
    return map;
  });
  const mapFor = (group: ShardGroup) =>
    Effect.flatMap(Ref.get(maps), (current) => {
      const cached = current.get(group);
      return cached === undefined ? refresh(group) : Effect.succeed(cached);
    });

  /** The database holding `key`; the map covers every key, so a miss is a broken map. */
  const databaseFor = (group: ShardGroup, key: Uint8Array) =>
    Effect.flatMap(mapFor(group), (map) =>
      Option.match(shardFor(map, key), {
        onNone: () => Effect.die(new Error(`Shard map of group ${group} does not cover a key.`)),
        onSome: (shard) => Effect.succeed(shard.databaseId),
      }),
    );

  /** Runs `operation` on the key's database; on a routing error refreshes the map and retries. */
  const routed = <A, E extends { readonly _tag: string }>(
    group: ShardGroup,
    key: Uint8Array,
    operation: (databaseId: DatabaseId) => Effect.Effect<A, E, DbReaderServices>,
  ) =>
    Effect.flatMap(databaseFor(group, key), operation).pipe(
      Effect.tapError((error) =>
        isRoutingError(error) ? Effect.ignore(refresh(group)) : Effect.void,
      ),
      Effect.retry(routingRetry),
    );

  const receipt =
    (databaseId: DatabaseId) =>
    (written: WriteReceipt): ShardedWriteReceipt => ({
      databaseId,
      seq: written.seq,
      createTs: written.createTs,
    });

  const put = Effect.fn("ShardRouter.put")(function* (
    group: ShardGroup,
    key: Uint8Array,
    value: Uint8Array,
  ) {
    return yield* routed(group, key, (databaseId) =>
      Effect.map(client.put(databaseId, key, value), receipt(databaseId)),
    );
  });
  const remove = Effect.fn("ShardRouter.delete")(function* (group: ShardGroup, key: Uint8Array) {
    return yield* routed(group, key, (databaseId) =>
      Effect.map(client.delete(databaseId, key), receipt(databaseId)),
    );
  });
  const merge = Effect.fn("ShardRouter.merge")(function* (
    group: ShardGroup,
    key: Uint8Array,
    operand: Uint8Array,
  ) {
    return yield* routed(group, key, (databaseId) =>
      Effect.map(client.merge(databaseId, key, operand), receipt(databaseId)),
    );
  });
  const write = Effect.fn("ShardRouter.write")(function* (group: ShardGroup, batch: WriteBatch) {
    const [first] = batch.operations;
    if (first === undefined) {
      // An empty batch: the database reports it (WriteBatchInvalidError) once routed anywhere.
      return yield* routed(group, new Uint8Array(0), (databaseId) =>
        Effect.map(client.write(databaseId, batch), receipt(databaseId)),
      );
    }
    const attempt = Effect.gen(function* () {
      const map = yield* mapFor(group);
      const databases = new Set(
        batch.operations.flatMap((operation) =>
          Option.toArray(Option.map(shardFor(map, operation.key), (shard) => shard.databaseId)),
        ),
      );
      if (databases.size > 1) {
        return yield* new CrossShardBatchError({
          group,
          databases: [...databases],
          message: `The batch's keys belong to ${databases.size} shards of group ${group}. Send one batch per shard; batches are atomic only within a database.`,
        });
      }
      const databaseId = yield* databaseFor(group, first.key);
      return receipt(databaseId)(yield* client.write(databaseId, batch));
    });
    return yield* attempt.pipe(
      Effect.tapError((error) =>
        isRoutingError(error) ? Effect.ignore(refresh(group)) : Effect.void,
      ),
      Effect.retry(routingRetry),
    );
  });
  const get = Effect.fn("ShardRouter.get")(function* (
    group: ShardGroup,
    key: Uint8Array,
    options: ReadOptions,
  ) {
    return yield* routed(group, key, (databaseId) => client.get(databaseId, key, options));
  });

  const scan = (group: ShardGroup, range: KeyRange, options: ScanReadOptions) =>
    Stream.unwrap(
      Effect.map(mapFor(group), (map) => {
        const perShard = shardsOverlapping(map, range).map((shard) =>
          Option.match(intersectKeyRanges(range, shard.range), {
            onNone: () => Stream.empty,
            onSome: (part) =>
              client.scan(shard.databaseId, part, { ...options, limit: Option.none() }),
          }),
        );
        // Shards are in key order and disjoint, so concatenation is the merged order.
        const all = Stream.fromIterable(perShard).pipe(Stream.flatMap((rows) => rows));
        return Option.match(options.limit, {
          onNone: () => all,
          onSome: (limit) => Stream.take(all, limit),
        });
      }),
    ).pipe(Stream.withSpan("ShardRouter.scan", { attributes: { "shard.group": group } }));

  return ShardRouter.of({
    put,
    delete: remove,
    merge,
    write,
    get,
    scan,
    refresh,
  });
});

/** Provides the router, leaving the database client and shard directory visible. */
export const shardRouterLayerWithoutDependencies = Layer.effect(ShardRouter, makeShardRouter);
