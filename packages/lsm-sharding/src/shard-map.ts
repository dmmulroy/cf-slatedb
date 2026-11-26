import { DatabaseId, FULL_KEY_RANGE, KeyRange, keyRangeContains } from "@shaledb/lsm-core";
import { Bytes, Option, Result, Schema } from "effect";

/** Schema for a shard group's name: one directory, one range map. */
export const ShardGroup = Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9._-]{0,62}$/)).pipe(
  Schema.brand("ShardGroup"),
);

/** A shard group's name. */
export type ShardGroup = typeof ShardGroup.Type;

/** Schema for one shard: a key range and the database that holds it. */
export const Shard = Schema.Struct({ range: KeyRange, databaseId: DatabaseId });

/** One shard. */
export type Shard = typeof Shard.Type;

/** Schema for a split in progress: which shard, where, and the two databases that replace it. */
export const PendingSplit = Schema.Struct({
  databaseId: DatabaseId,
  atKey: Schema.Uint8ArrayFromBase64,
  left: DatabaseId,
  right: DatabaseId,
});

/** A split in progress. */
export type PendingSplit = typeof PendingSplit.Type;

/**
 * Schema for a shard group's range map: shards in key order that cover every key exactly once, a
 * version that grows with each published split, and the split in progress, if any.
 */
export const ShardMap = Schema.Struct({
  group: ShardGroup,
  version: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  shards: Schema.Array(Shard),
  pendingSplit: Schema.OptionFromNullOr(PendingSplit),
});

/** A shard group's range map. */
export type ShardMap = typeof ShardMap.Type;

/**
 * The split cannot apply: the database is not a shard of the group, or the key is not strictly
 * inside its range (a split must leave both halves non-empty). Not retryable as asked.
 */
export class ShardSplitInvalidError extends Schema.TaggedError<ShardSplitInvalidError>()(
  "ShardSplitInvalidError",
  { group: ShardGroup, databaseId: DatabaseId, message: Schema.String },
) {}

/**
 * Another split of the group is in progress. Retryable: finish it (retrying it completes it), then
 * split again.
 */
export class ShardSplitInProgressError extends Schema.TaggedError<ShardSplitInProgressError>()(
  "ShardSplitInProgressError",
  { group: ShardGroup, pending: PendingSplit, message: Schema.String },
) {}

/** A new group's map: one shard, holding every key, in `databaseId`. */
export const initialShardMap = (group: ShardGroup, databaseId: DatabaseId): ShardMap => ({
  group,
  version: 1,
  shards: [{ range: FULL_KEY_RANGE, databaseId }],
  pendingSplit: Option.none(),
});

/** The shard whose range holds `key` (binary search; the shards cover every key). */
export const shardFor = (map: ShardMap, key: Uint8Array): Option.Option<Shard> => {
  let low = 0;
  let high = map.shards.length - 1;
  while (low <= high) {
    const middle = (low + high) >> 1;
    const shard = map.shards[middle];
    if (shard === undefined) break;
    if (Bytes.Order(key, shard.range.start) < 0) high = middle - 1;
    else if (keyRangeContains(shard.range, key)) return Option.some(shard);
    else low = middle + 1;
  }
  return Option.none();
};

/** The shards whose ranges overlap `range`, in key order. */
export const shardsOverlapping = (map: ShardMap, range: KeyRange): ReadonlyArray<Shard> =>
  map.shards.filter(
    (shard) =>
      (Option.isNone(range.end) || Bytes.Order(shard.range.start, range.end.value) < 0) &&
      (Option.isNone(shard.range.end) || Bytes.Order(range.start, shard.range.end.value) < 0),
  );

const invalid = (map: ShardMap, databaseId: DatabaseId, message: string) =>
  new ShardSplitInvalidError({ group: map.group, databaseId, message });

/** The shard of `databaseId`, if `atKey` is strictly inside its range. */
const splittableShard = (
  map: ShardMap,
  databaseId: DatabaseId,
  atKey: Uint8Array,
): Result.Result<Shard, ShardSplitInvalidError> => {
  const shard = map.shards.find((candidate) => candidate.databaseId === databaseId);
  if (shard === undefined) {
    return Result.fail(
      invalid(
        map,
        databaseId,
        `Database ${databaseId} is not a shard of group ${map.group}. Read the map and split one of its shards.`,
      ),
    );
  }
  const inside = Bytes.Order(atKey, shard.range.start) > 0 && keyRangeContains(shard.range, atKey);
  return inside
    ? Result.succeed(shard)
    : Result.fail(
        invalid(
          map,
          databaseId,
          `The split key is not strictly inside the range of shard ${databaseId}, so one half would be empty. Choose a key inside it.`,
        ),
      );
};

/**
 * Records a split as in progress, with the databases that will replace the shard (a pure edit).
 *
 * @returns The map (unchanged when this split is already pending), `ShardSplitInvalidError`, or
 *   `ShardSplitInProgressError` when a different split is pending.
 */
export const beginShardSplit = (
  map: ShardMap,
  split: PendingSplit,
): Result.Result<ShardMap, ShardSplitInvalidError | ShardSplitInProgressError> => {
  if (Option.isSome(map.pendingSplit)) {
    const pending = map.pendingSplit.value;
    return pending.databaseId === split.databaseId && Bytes.Equivalence(pending.atKey, split.atKey)
      ? Result.succeed(map)
      : Result.fail(
          new ShardSplitInProgressError({
            group: map.group,
            pending,
            message: `Group ${map.group} is splitting shard ${pending.databaseId}. Retry that split to finish it, then split again.`,
          }),
        );
  }
  return Result.map(splittableShard(map, split.databaseId, split.atKey), () => ({
    ...map,
    pendingSplit: Option.some(split),
  }));
};

/**
 * Publishes the pending split (a pure edit): the shard is replaced by its two halves and the
 * version grows by one.
 *
 * @returns The new map; the same map when the split was already published; or
 *   `ShardSplitInvalidError` when no such split is pending.
 */
export const completeShardSplit = (
  map: ShardMap,
  databaseId: DatabaseId,
): Result.Result<ShardMap, ShardSplitInvalidError> => {
  if (Option.isNone(map.pendingSplit) || map.pendingSplit.value.databaseId !== databaseId) {
    const published = map.shards.every((shard) => shard.databaseId !== databaseId);
    return published
      ? Result.succeed(map)
      : Result.fail(
          invalid(
            map,
            databaseId,
            `No split of shard ${databaseId} is pending in group ${map.group}. Begin the split first.`,
          ),
        );
  }
  const split = map.pendingSplit.value;
  return Result.map(splittableShard(map, databaseId, split.atKey), (shard) => ({
    ...map,
    version: map.version + 1,
    shards: map.shards.flatMap((candidate) =>
      candidate.databaseId === databaseId
        ? [
            {
              range: { start: shard.range.start, end: Option.some(split.atKey) },
              databaseId: split.left,
            },
            { range: { start: split.atKey, end: shard.range.end }, databaseId: split.right },
          ]
        : [candidate],
    ),
    pendingSplit: Option.none(),
  }));
};
