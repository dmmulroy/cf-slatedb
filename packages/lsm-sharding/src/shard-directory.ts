import { DatabaseId, generateCompactionJobId } from "@shaledb/lsm-core";
import { type DurableObjectCell, inMemoryCell } from "@shaledb/lsm-cloudflare";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import {
  Context,
  Deferred,
  Duration,
  Effect,
  Layer,
  Option,
  Ref,
  Result,
  Schema,
  Semaphore,
} from "effect";

import {
  beginShardSplit,
  completeShardSplit,
  initialShardMap,
  type PendingSplit,
  ShardGroup,
  ShardMap,
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
/**
 * How a wait for a newer shard map ended: `Newer` (a newer map is published), `SplitFailed` (the
 * pending split reported a failure; it stays recorded and resumable), `NoSplitPending` (nothing is
 * publishing, so waiting cannot help), or `TimedOut`.
 */
export const ShardMapWaitOutcome = Schema.Literals([
  "Newer",
  "SplitFailed",
  "NoSplitPending",
  "TimedOut",
]);

/** How a wait for a newer shard map ended. */
export type ShardMapWaitOutcome = typeof ShardMapWaitOutcome.Type;

/** Schema for the end of a wait for a newer map: the group's current map and why the wait ended. */
export const ShardMapWait = Schema.Struct({ map: ShardMap, outcome: ShardMapWaitOutcome });

/** The end of a wait for a newer map. */
export type ShardMapWait = typeof ShardMapWait.Type;

/** Longest a single wait for a newer map lasts before it ends `TimedOut`. */
export const MAX_SHARD_MAP_WAIT_MS = 25_000;

export interface IShardDirectory {
  /** Creates a group with one shard, holding every key, in `databaseId`. */
  readonly create: (
    group: ShardGroup,
    databaseId: DatabaseId,
  ) => Effect.Effect<ShardMap, ShardDirectoryError, RuntimeContext>;
  /** The group's current map. */
  readonly map: (group: ShardGroup) => Effect.Effect<ShardMap, ShardDirectoryError, RuntimeContext>;
  /**
   * Waits until the group's map is newer than `afterVersion`, and returns it: at once when it
   * already is; otherwise as soon as a split publishes, or when the pending split reports a failure,
   * or after `timeoutMs` (at most {@link MAX_SHARD_MAP_WAIT_MS}). Ends at once with
   * `NoSplitPending` when nothing could publish a newer map.
   */
  readonly awaitMap: (
    group: ShardGroup,
    afterVersion: number,
    timeoutMs: number,
  ) => Effect.Effect<ShardMapWait, ShardDirectoryError, RuntimeContext>;
  /**
   * Records that the pending split of `databaseId` failed part-way: it stays recorded, so repeating
   * the split resumes it, and routers waiting for it stop waiting.
   */
  readonly reportSplitFailed: (
    group: ShardGroup,
    databaseId: DatabaseId,
  ) => Effect.Effect<void, ShardDirectoryError, RuntimeContext>;
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
  "@shaledb/ShardDirectory",
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
    // Waiters for a newer map, per group: completed (and replaced) when a split publishes or fails.
    const signals = yield* Ref.make(
      new Map<ShardGroup, Deferred.Deferred<"Newer" | "SplitFailed">>(),
    );
    const signalFor = (group: ShardGroup) =>
      Effect.gen(function* () {
        const fresh = yield* Deferred.make<"Newer" | "SplitFailed">();
        return yield* Ref.modify(signals, (current) => {
          const existing = current.get(group);
          if (existing !== undefined) return [existing, current];
          return [fresh, new Map(current).set(group, fresh)];
        });
      });
    const signal = (group: ShardGroup, outcome: "Newer" | "SplitFailed") =>
      Effect.gen(function* () {
        const waiting = yield* Ref.modify(signals, (current) => {
          const next = new Map(current);
          next.delete(group);
          return [current.get(group), next];
        });
        if (waiting !== undefined) yield* Deferred.succeed(waiting, outcome);
      });
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
        yield* signal(group, "Newer");
        yield* Effect.annotateCurrentSpan({ "shard.group": group, "shard.version": map.version });
        return map;
      }),
      awaitMap: Effect.fn("ShardDirectory.awaitMap")(function* (
        group: ShardGroup,
        afterVersion: number,
        timeoutMs: number,
      ) {
        yield* Effect.annotateCurrentSpan({
          "shard.group": group,
          "shard.after_version": afterVersion,
        });
        // Take the signal before reading the map, so a publish in between is not missed.
        const published = yield* signalFor(group);
        const map = yield* current(group);
        if (map.version > afterVersion) return { map, outcome: "Newer" as const };
        if (Option.isNone(map.pendingSplit)) return { map, outcome: "NoSplitPending" as const };
        const outcome = yield* Deferred.await(published).pipe(
          Effect.timeoutOption(Duration.millis(Math.min(timeoutMs, MAX_SHARD_MAP_WAIT_MS))),
        );
        const latest = yield* current(group);
        const ended: ShardMapWaitOutcome =
          latest.version > afterVersion
            ? "Newer"
            : Option.getOrElse(outcome, (): ShardMapWaitOutcome => "TimedOut");
        yield* Effect.annotateCurrentSpan({ "shard.wait_outcome": ended });
        return { map: latest, outcome: ended };
      }),
      reportSplitFailed: Effect.fn("ShardDirectory.reportSplitFailed")(function* (
        group: ShardGroup,
        databaseId: DatabaseId,
      ) {
        yield* Effect.annotateCurrentSpan({ "shard.group": group, "database.id": databaseId });
        yield* signal(group, "SplitFailed");
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
