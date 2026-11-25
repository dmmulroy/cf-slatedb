import { DatabaseId, generateCompactionJobId } from "@cf-slatedb/lsm-core";
import { type DurableObjectCell, inMemoryCell } from "@cf-slatedb/lsm-cloudflare";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, Option, Result, Schema, Semaphore } from "effect";

import {
  beginShardSplit,
  completeShardSplit,
  initialShardMap,
  type PendingSplit,
  ShardGroup,
  type ShardMap,
  type ShardSplitInProgressError,
  type ShardSplitInvalidError,
} from "./shard-map.ts";

/** No shard group has this name. Not retryable: create the group first. */
export class ShardGroupNotFoundError extends Schema.TaggedError<ShardGroupNotFoundError>()(
  "ShardGroupNotFoundError",
  { group: ShardGroup, message: Schema.String },
) {}

/** The shard group already exists. Not retryable: use it, or choose another name. */
export class ShardGroupExistsError extends Schema.TaggedError<ShardGroupExistsError>()(
  "ShardGroupExistsError",
  { group: ShardGroup, message: Schema.String },
) {}

/** The shard directory could not be reached. Retryable. */
export class ShardDirectoryUnavailableError extends Schema.TaggedError<ShardDirectoryUnavailableError>()(
  "ShardDirectoryUnavailableError",
  { group: ShardGroup, message: Schema.String },
) {}

/** Every failure a directory call can report. */
export type ShardDirectoryError =
  | ShardGroupNotFoundError
  | ShardGroupExistsError
  | ShardSplitInvalidError
  | ShardSplitInProgressError
  | ShardDirectoryUnavailableError;

/**
 * One shard group's range map (design §12, "Optional sharding package"): which database holds each
 * key range, with a version that grows with each split.
 *
 * **Details**
 *
 * One directory per group serializes its edits. A split is two edits: `beginSplit` records it and
 * allocates the two new database ids (so a retried split resumes with the same ids, and only one
 * split runs at a time), `completeSplit` publishes the new map.
 */
export interface IShardDirectory {
  /** Creates a group with one shard, holding every key, in `databaseId`. */
  readonly create: (
    group: ShardGroup,
    databaseId: DatabaseId,
  ) => Effect.Effect<ShardMap, ShardDirectoryError, RuntimeContext>;
  /** The group's current map. */
  readonly map: (group: ShardGroup) => Effect.Effect<ShardMap, ShardDirectoryError, RuntimeContext>;
  /** Records a split of `databaseId` at `atKey` (or returns the one already recorded). */
  readonly beginSplit: (
    group: ShardGroup,
    databaseId: DatabaseId,
    atKey: Uint8Array,
  ) => Effect.Effect<PendingSplit, ShardDirectoryError, RuntimeContext>;
  /** Publishes the recorded split of `databaseId`. Safe to repeat. */
  readonly completeSplit: (
    group: ShardGroup,
    databaseId: DatabaseId,
  ) => Effect.Effect<ShardMap, ShardDirectoryError, RuntimeContext>;
}

/** The contextual shard directory. */
export class ShardDirectory extends Context.Service<ShardDirectory, IShardDirectory>()(
  "@cf-slatedb/ShardDirectory",
) {}

/** A new database id for one half of a split: `s-` and a lowercase ULID. */
const newShardDatabaseId = Effect.map(generateCompactionJobId, (ulid) =>
  DatabaseId.make(`s-${ulid.toLowerCase()}`),
);

/**
 * Builds one group's directory over the cell that stores its map: the logic the
 * `ShardDirectoryServer` Durable Object runs, and the in-memory test directory.
 */
export const makeShardDirectoryOver = (
  cellFor: (group: ShardGroup) => DurableObjectCell<ShardMap>,
) =>
  Effect.gen(function* () {
    // Edits of a group are serialized (a Durable Object's gates already serialize its events).
    const lock = yield* Semaphore.make(1);
    const current = (group: ShardGroup) =>
      Effect.flatMap(cellFor(group).get, (stored) =>
        Option.isSome(stored)
          ? Effect.succeed(stored.value)
          : Effect.fail(
              new ShardGroupNotFoundError({
                group,
                message: `Shard group ${group} does not exist. Create it with its first database.`,
              }),
            ),
      );
    const editMap = <E>(group: ShardGroup, edit: (map: ShardMap) => Result.Result<ShardMap, E>) =>
      lock.withPermits(1)(
        Effect.gen(function* () {
          const map = yield* current(group);
          const edited = yield* Effect.fromResult(edit(map));
          if (edited !== map) yield* cellFor(group).set(edited);
          return edited;
        }),
      );
    return ShardDirectory.of({
      create: Effect.fn("ShardDirectory.create")(function* (
        group: ShardGroup,
        databaseId: DatabaseId,
      ) {
        yield* Effect.annotateCurrentSpan({ "shard.group": group, "database.id": databaseId });
        return yield* lock.withPermits(1)(
          Effect.gen(function* () {
            const stored = yield* cellFor(group).get;
            if (Option.isSome(stored)) {
              const existing = stored.value;
              const same =
                existing.shards.length === 1 && existing.shards[0]?.databaseId === databaseId;
              if (same) return existing;
              return yield* new ShardGroupExistsError({
                group,
                message: `Shard group ${group} already exists. Use it, or choose another name.`,
              });
            }
            const map = initialShardMap(group, databaseId);
            yield* cellFor(group).set(map);
            return map;
          }),
        );
      }),
      map: Effect.fn("ShardDirectory.map")(function* (group: ShardGroup) {
        const map = yield* current(group);
        yield* Effect.annotateCurrentSpan({ "shard.group": group, "shard.version": map.version });
        return map;
      }),
      beginSplit: Effect.fn("ShardDirectory.beginSplit")(function* (
        group: ShardGroup,
        databaseId: DatabaseId,
        atKey: Uint8Array,
      ) {
        yield* Effect.annotateCurrentSpan({ "shard.group": group, "database.id": databaseId });
        const left = yield* newShardDatabaseId;
        const right = yield* newShardDatabaseId;
        const map = yield* editMap(group, (map) =>
          beginShardSplit(map, { databaseId, atKey, left, right }),
        );
        if (Option.isNone(map.pendingSplit)) {
          return yield* Effect.die(new Error("beginShardSplit always records the pending split"));
        }
        return map.pendingSplit.value;
      }),
      completeSplit: Effect.fn("ShardDirectory.completeSplit")(function* (
        group: ShardGroup,
        databaseId: DatabaseId,
      ) {
        const map = yield* editMap(group, (map) => completeShardSplit(map, databaseId));
        yield* Effect.annotateCurrentSpan({ "shard.group": group, "shard.version": map.version });
        return map;
      }),
    });
  });

/** Provides a directory whose groups' maps live in memory, for tests. */
export const shardDirectoryTestLayer = Layer.effect(
  ShardDirectory,
  Effect.gen(function* () {
    const cells = new Map<ShardGroup, DurableObjectCell<ShardMap>>();
    const cellFor = (group: ShardGroup): DurableObjectCell<ShardMap> => {
      const existing = cells.get(group);
      if (existing !== undefined) return existing;
      const cell = Effect.runSync(inMemoryCell<ShardMap>());
      cells.set(group, cell);
      return cell;
    };
    return yield* makeShardDirectoryOver(cellFor);
  }),
);
