import {
  DatabaseId,
  intersectKeyRanges,
  type KeyRange,
  type Row,
  type SeqNum,
  type Timestamp,
  type WriteBatch,
  type WriteReceipt,
} from "@shaledb/lsm-core";
import {
  DbClient,
  type DbClientReadError,
  type DbReaderServices,
  type DbWriteError,
  type ReaderUnavailableError,
  ScanBudget,
  ScanPageBudget,
  type ReadOptions,
  type ScanReadOptions,
} from "@shaledb/lsm-cloudflare";
import { Bytes, Clock, Context, Effect, Layer, Option, Ref, Result, Schema, Stream } from "effect";

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
  "@shaledb/ShardRouter",
) {}

/** Failures that mean the cached map is stale (or a split is still publishing). */
const isRoutingError = (error: { readonly _tag: string }) =>
  error._tag === "DatabaseSealedError" ||
  error._tag === "KeyOutOfRangeError" ||
  error._tag === "DatabaseNotFoundError";

/**
 * Longest a routed request waits for a split to publish before it fails (retryable). A split pauses a
 * shard's writes for one small flush and a few manifest commits (`docs/measurements.md`); the router
 * waits on the directory (`ShardDirectory.awaitMap`), so it retries the moment the new map is
 * published rather than on a timer.
 */
const ROUTING_WAIT_MS = 30_000;

/**
 * Oldest a cached map may be when a `Flushed` or `AtLeast` read uses it (D89). Those reads never
 * ask the writer, so a sealed shard does not fail them and a stale map would serve the sealed
 * shard's data for as long as the isolate lives. Refreshing a map older than this bounds that
 * staleness to about the lag a `Flushed` read already has.
 */
export const SHARD_MAP_MAX_AGE_FOR_UNPROBED_READS_MS = 1_000;

/**
 * One routed scan page (D125): the budget every shard it reads shares, and the rows it still may
 * return.
 */
interface RoutedPage {
  readonly budget: { readonly maxBytes: number; readonly shared: Option.Option<ScanBudget> };
  readonly remaining: () => Option.Option<number>;
  readonly emitted: () => void;
}

/** Constructs the router over the contextual database client and shard directory. */
export const makeShardRouter = Effect.gen(function* () {
  const client = yield* DbClient;
  const directory = yield* ShardDirectory;
  const maps = yield* Ref.make(new Map<ShardGroup, ShardMap>());
  /** When each group's cached map was read from the directory (or learned from a wait). */
  const readAt = yield* Ref.make(new Map<ShardGroup, number>());
  const remember = (group: ShardGroup, map: ShardMap) =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      yield* Ref.update(maps, (current) => new Map(current).set(group, map));
      yield* Ref.update(readAt, (current) => new Map(current).set(group, now));
    });

  const refresh = Effect.fn("ShardRouter.refresh")(function* (group: ShardGroup) {
    const map = yield* directory.map(group);
    yield* remember(group, map);
    yield* Effect.annotateCurrentSpan({
      "shard.group": group,
      "shard.version": map.version,
      // A split still pending shows here (D101); `measure health` reports one that a request had
      // to finish (`ShardSplit.resumePending`).
      ...(Option.isSome(map.pendingSplit)
        ? { "shard.pending_split": map.pendingSplit.value.databaseId }
        : {}),
    });
    return map;
  });
  const mapFor = (group: ShardGroup) =>
    Effect.flatMap(Ref.get(maps), (current) => {
      const cached = current.get(group);
      return cached === undefined ? refresh(group) : Effect.succeed(cached);
    });

  /**
   * The map a read with `options` uses: the cached one, refreshed first when the read never asks
   * the writer (`Flushed`, `AtLeast`) and the map is older than
   * {@link SHARD_MAP_MAX_AGE_FOR_UNPROBED_READS_MS}. A read at a snapshot is not rerouted.
   */
  const refreshForUnprobedRead = (group: ShardGroup, options: ReadOptions) =>
    Effect.gen(function* () {
      if (Option.isSome(options.snapshot) || options.consistency._tag === "Latest") return;
      const at = (yield* Ref.get(readAt)).get(group);
      const now = yield* Clock.currentTimeMillis;
      if (at === undefined || now - at > SHARD_MAP_MAX_AGE_FOR_UNPROBED_READS_MS) {
        yield* refresh(group);
      }
    });

  /** The database holding `key` in `map`; a map covers every key, so a miss is a broken map. */
  const databaseIn = (map: ShardMap, group: ShardGroup, key: Uint8Array) =>
    Option.match(shardFor(map, key), {
      onNone: () => Effect.die(new Error(`Shard map of group ${group} does not cover a key.`)),
      onSome: (shard) => Effect.succeed(shard.databaseId),
    });

  /**
   * Runs `attempt` against the cached map; on a routing error (a split in progress, or a stale map)
   * waits on the directory for a newer map and retries. It gives up with the routing error, which is
   * retryable, when no newer map can come (the split failed, or none is pending) or after
   * {@link ROUTING_WAIT_MS}.
   */
  const withRouting = <A, E extends { readonly _tag: string }, R>(
    group: ShardGroup,
    attempt: (map: ShardMap) => Effect.Effect<A, E, R>,
  ) =>
    Effect.gen(function* () {
      const deadline = (yield* Clock.currentTimeMillis) + ROUTING_WAIT_MS;
      for (;;) {
        const map = yield* mapFor(group);
        const outcome = yield* Effect.result(attempt(map));
        if (Result.isSuccess(outcome)) return outcome.success;
        const error = outcome.failure;
        const remaining = deadline - (yield* Clock.currentTimeMillis);
        if (!isRoutingError(error) || remaining <= 0) return yield* Effect.fail(error);
        const waited = yield* directory.awaitMap(group, map.version, remaining);
        yield* remember(group, waited.map);
        yield* Effect.annotateCurrentSpan({ "shard.wait_outcome": waited.outcome });
        if (waited.outcome !== "Newer") return yield* Effect.fail(error);
      }
    });

  /** Runs `operation` on the key's database, routing around splits (see `withRouting`). */
  const routed = <A, E extends { readonly _tag: string }>(
    group: ShardGroup,
    key: Uint8Array,
    operation: (databaseId: DatabaseId) => Effect.Effect<A, E, DbReaderServices>,
  ) => withRouting(group, (map) => Effect.flatMap(databaseIn(map, group, key), operation));

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
    return yield* withRouting(group, (map) =>
      Effect.gen(function* () {
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
        const databaseId = yield* databaseIn(map, group, first.key);
        return receipt(databaseId)(yield* client.write(databaseId, batch));
      }),
    );
  });
  const get = Effect.fn("ShardRouter.get")(function* (
    group: ShardGroup,
    key: Uint8Array,
    options: ReadOptions,
  ) {
    yield* refreshForUnprobedRead(group, options);
    return yield* routed(group, key, (databaseId) => client.get(databaseId, key, options));
  });

  /**
   * Scans `range` shard by shard in key order. A shard whose database turns out to be sealed (a
   * stale map; `Latest` scans refuse sealed databases when `refuseSealed` is set) fails before it
   * emits a row, so the scan waits on the directory for the newer map and resumes that shard's part
   * from it, within the same deadline as a routed write.
   */
  const scanFrom = (
    group: ShardGroup,
    range: KeyRange,
    options: ScanReadOptions,
    deadline: number,
    page: RoutedPage,
  ): Stream.Stream<Row, DbClientReadError | ShardDirectoryError, DbReaderServices> =>
    Stream.unwrap(
      Effect.gen(function* () {
        const map = yield* mapFor(group);
        const [first] = shardsOverlapping(map, range);
        if (first === undefined) return Stream.empty;
        const part = intersectKeyRanges(range, first.range);
        if (Option.isNone(part)) return Stream.empty;
        // Each shard gets the page's remaining rows and its shared budget (D125): a routed page does
        // no more work than one database's page, wherever its shards' boundaries fall.
        const head = client
          .scan(first.databaseId, part.value, { ...options, limit: page.remaining() })
          .pipe(
            Stream.provideService(ScanPageBudget, page.budget),
            Stream.tap(() => Effect.sync(() => page.emitted())),
            // A scan overtaken by a flush after it returned rows (D93): the rest of this shard's
            // part from the key it names, within the same deadline; the shards after it follow
            // below. Every row before it was returned. A page whose budget is spent ends instead:
            // its `resumeFrom` is the routed page's continuation.
            Stream.catchIf(
              (error): error is ReaderUnavailableError & { readonly resumeFrom: Uint8Array } =>
                error._tag === "ReaderUnavailableError" &&
                error.resumeFrom !== undefined &&
                error.pageEnd === "OvertakenByFlush",
              (error) =>
                Stream.unwrap(
                  Effect.map(Clock.currentTimeMillis, (now) =>
                    now >= deadline
                      ? Stream.fail(error)
                      : scanFrom(
                          group,
                          { start: error.resumeFrom, end: part.value.end },
                          options,
                          deadline,
                          page,
                        ),
                  ),
                ),
            ),
            Stream.catchIf(isRoutingError, (error) =>
              Stream.unwrap(
                Effect.gen(function* () {
                  const remaining = deadline - (yield* Clock.currentTimeMillis);
                  if (remaining <= 0) return Stream.fail(error);
                  const waited = yield* directory.awaitMap(group, map.version, remaining);
                  yield* remember(group, waited.map);
                  return waited.outcome === "Newer"
                    ? scanFrom(group, range, options, deadline, page)
                    : Stream.fail(error);
                }),
              ),
            ),
          );
        // Shards are in key order and disjoint: the rest of the range starts where this shard ends.
        const rest = Option.match(first.range.end, {
          onNone: () => Stream.empty,
          onSome: (end) =>
            Option.isSome(range.end) && Bytes.Order(range.end.value, end) <= 0
              ? Stream.empty
              : Stream.suspend(() =>
                  Option.exists(page.remaining(), (left) => left === 0)
                    ? Stream.empty
                    : scanFrom(group, { start: end, end: range.end }, options, deadline, page),
                ),
        });
        return Stream.concat(head, rest);
      }),
    );

  const scan = (group: ShardGroup, range: KeyRange, options: ScanReadOptions) =>
    Stream.unwrap(
      Effect.gen(function* () {
        yield* refreshForUnprobedRead(group, options);
        const now = yield* Clock.currentTimeMillis;
        const pageBudget = yield* Effect.service(ScanPageBudget);
        // One budget for the whole routed page, shared by every shard it reads (D125).
        const shared = Option.getOrElse(
          pageBudget.shared,
          () => new ScanBudget(pageBudget.maxBytes),
        );
        let emitted = 0;
        const page: RoutedPage = {
          budget: { maxBytes: pageBudget.maxBytes, shared: Option.some(shared) },
          remaining: () => Option.map(options.limit, (limit) => Math.max(0, limit - emitted)),
          emitted: () => {
            emitted += 1;
          },
        };
        const all = scanFrom(group, range, options, now + ROUTING_WAIT_MS, page);
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
