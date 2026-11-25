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

/** How long the split's own checkpoint lasts if a split stops after taking it. */
const SPLIT_CHECKPOINT_LIFETIME_MS = 3_600_000;

/**
 * Splits a shard in two (design §12, `ShardSplit`): seal first, fork once, then publish.
 *
 * **Details**
 *
 * 1. Record the split in the group's directory (allocating the two new database ids).
 * 2. Seal the shard's database with the two as successors: its memtable is flushed and every later
 *    write fails with `DatabaseSealedError` (routers retry until the new map is published).
 * 3. Checkpoint the sealed manifest and fork each half from it (zero-copy).
 * 4. Delete the split's checkpoint (each fork holds its own) and publish the new map.
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
    yield* lifecycle.seal(databaseId, [pending.left, pending.right]);
    const checkpoint = yield* lifecycle.createCheckpoint(databaseId, {
      name: Option.some(`split ${pending.left} ${pending.right}`),
      lifetimeMs: Option.some(SPLIT_CHECKPOINT_LIFETIME_MS),
    });
    yield* lifecycle.forkRange({
      source: databaseId,
      checkpointId: checkpoint.id,
      range: { start: shard.range.start, end: Option.some(pending.atKey) },
      forkId: pending.left,
    });
    yield* lifecycle.forkRange({
      source: databaseId,
      checkpointId: checkpoint.id,
      range: { start: pending.atKey, end: shard.range.end },
      forkId: pending.right,
    });
    yield* lifecycle.deleteCheckpoint(databaseId, checkpoint.id);
    const published = yield* directory.completeSplit(group, databaseId);
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
