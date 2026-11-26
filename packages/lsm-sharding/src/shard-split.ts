import type { DatabaseId } from "@cf-slatedb/lsm-core";
import {
  DatabaseLifecycle,
  type DatabaseLifecycleError,
  type DatabaseLifecycleServices,
} from "@cf-slatedb/lsm-cloudflare";
import { Context, Effect, Layer, Option } from "effect";

import { ShardDirectory, type ShardDirectoryError } from "./shard-directory.ts";
import type { ShardGroup, ShardMap } from "./shard-map.ts";

/** Every failure a split can report. */
export type ShardSplitError = DatabaseLifecycleError | ShardDirectoryError;

/**
 * Splits a shard in two (design §12, `ShardSplit`): seal first, fork once, then publish.
 *
 * **Details**
 *
 * 1. Record the split in the group's directory (allocating the two new database ids).
 * 2. Seal the shard's database with the two as successors: its memtable is flushed, every later
 *    write fails with `DatabaseSealedError` (routers wait on the directory until the new map is
 *    published, `ShardDirectory.awaitMap`), and the same commit takes each half a hold on the
 *    sealed manifest.
 * 3. Fork both halves at once, each from its own hold (zero-copy, no commit on the shard).
 * 4. Publish the new map.
 *
 * A failure after step 1 is reported to the directory (`reportSplitFailed`), so waiting routers
 * stop waiting.
 *
 * Every step is idempotent, so retrying a failed split resumes it. The sealed database stays until
 * its forks' compactions release it; remove it then with `DatabaseLifecycle.remove`.
 */
export interface IShardSplit {
  readonly split: (
    group: ShardGroup,
    databaseId: DatabaseId,
    atKey: Uint8Array,
  ) => Effect.Effect<ShardMap, ShardSplitError, DatabaseLifecycleServices>;
}

/** The contextual shard split. */
export class ShardSplit extends Context.Service<ShardSplit, IShardSplit>()(
  "@cf-slatedb/ShardSplit",
) {}

/** Constructs the split over the contextual database lifecycle and shard directory. */
export const makeShardSplit = Effect.gen(function* () {
  const lifecycle = yield* DatabaseLifecycle;
  const directory = yield* ShardDirectory;

  const split = Effect.fn("ShardSplit.split")(function* (
    group: ShardGroup,
    databaseId: DatabaseId,
    atKey: Uint8Array,
  ) {
    yield* Effect.annotateCurrentSpan({ "shard.group": group, "database.id": databaseId });
    const pending = yield* directory.beginSplit(group, databaseId, atKey);
    const map = yield* directory.map(group);
    const shard = map.shards.find((candidate) => candidate.databaseId === databaseId);
    if (shard === undefined) return map;
    // From here writes to the shard pause until the publish. A failure leaves the split recorded
    // (repeating it resumes) and stops routers waiting for it.
    const published = yield* Effect.gen(function* () {
      // One commit seals the shard and takes each half a hold on the sealed manifest (D57).
      const sealed = yield* lifecycle.seal(databaseId, [pending.left, pending.right], true);
      const holdOf = (successor: DatabaseId) =>
        Option.match(
          Option.fromUndefinedOr(
            sealed.successorHolds.find((hold) => hold.successor === successor),
          ),
          {
            onNone: () =>
              Effect.die(new Error(`The seal of ${databaseId} took no hold for ${successor}.`)),
            onSome: (hold) => Effect.succeed(hold.checkpointId),
          },
        );
      const leftHold = yield* holdOf(pending.left);
      const rightHold = yield* holdOf(pending.right);
      // Each half forks from its own hold, so neither needs another commit on the shard.
      yield* Effect.all(
        [
          lifecycle.forkRange({
            source: databaseId,
            checkpointId: leftHold,
            range: { start: shard.range.start, end: Option.some(pending.atKey) },
            forkId: pending.left,
          }),
          lifecycle.forkRange({
            source: databaseId,
            checkpointId: rightHold,
            range: { start: pending.atKey, end: shard.range.end },
            forkId: pending.right,
          }),
        ],
        { concurrency: 2, discard: true },
      );
      return yield* directory.completeSplit(group, databaseId);
    }).pipe(Effect.tapError(() => Effect.ignore(directory.reportSplitFailed(group, databaseId))));
    yield* Effect.annotateCurrentSpan({
      "shard.version": published.version,
      "shard.left": pending.left,
      "shard.right": pending.right,
    });
    return published;
  });

  return ShardSplit.of({ split });
});

/** Provides the split, leaving the database lifecycle and shard directory visible. */
export const shardSplitLayerWithoutDependencies = Layer.effect(ShardSplit, makeShardSplit);
