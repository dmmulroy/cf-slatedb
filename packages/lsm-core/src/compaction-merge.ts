import { Array as Arr, Option, Result, Stream } from "effect";

import type { Timestamp } from "./ids.ts";
import { mergeRowStreams } from "./merge-iterator.ts";
import type { Row } from "./row.ts";
import { groupRowsByKey, type MergeOperator, retainKeyVersions } from "./row-resolution.ts";

/** How a compaction resolves history. */
export interface CompactionRetention {
  /** The compaction's clock time, fixed when the job starts, for TTL. */
  readonly now: Timestamp;
  /** Whether no data older than the inputs exists, so deletes can be dropped. */
  readonly isBottom: boolean;
  readonly mergeOperator: Option.Option<MergeOperator>;
}

/**
 * Merges compaction inputs into each key's surviving versions, in key order.
 *
 * **Details**
 *
 * A streaming k-way merge (one chunk per input in memory) grouped by key, then
 * {@link retainKeyVersions} with no retained history: readers that need older versions read the
 * manifest they pinned, whose SSTs GC keeps. Expired values become deletes, expired operands are
 * dropped, operands fold onto their base, and deletes are dropped at the bottom. Keys left with no
 * version are skipped. Each element holds one key's versions, newest first, so callers can split
 * output SSTs between keys.
 *
 * @param sources - Inputs in {@link rowOrder}, newest first (L0 newest to oldest, then sorted runs
 *   by id descending).
 */
export const compactRowStreams = <E, R>(
  sources: ReadonlyArray<Stream.Stream<Row, E, R>>,
  retention: CompactionRetention,
): Stream.Stream<Arr.NonEmptyReadonlyArray<Row>, E, R> =>
  groupRowsByKey(mergeRowStreams(sources)).pipe(
    Stream.filterMap((versions) => {
      const kept = retainKeyVersions(versions, {
        now: retention.now,
        retainAboveSeq: Option.none(),
        isBottom: retention.isBottom,
        mergeOperator: retention.mergeOperator,
      });
      return Arr.isReadonlyArrayNonEmpty(kept) ? Result.succeed(kept) : Result.failVoid;
    }),
  );
