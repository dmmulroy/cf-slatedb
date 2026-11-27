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

/** One key of a compaction: its versions in the inputs and those that survive. */
export interface CompactedKey {
  /** The key's versions read from the inputs, newest first. */
  readonly input: Arr.NonEmptyReadonlyArray<Row>;
  /** The versions the output keeps (none when every version is dropped). */
  readonly kept: ReadonlyArray<Row>;
}

/**
 * Merges compaction inputs key by key, in key order: every key read, with what it keeps.
 *
 * **Details**
 *
 * Unlike {@link compactRowStreams}, a key whose versions are all dropped is still emitted (with
 * nothing kept), so a caller can bound a step by what it read, not only by what it wrote (D97).
 */
export const compactKeys = <E, R>(
  sources: ReadonlyArray<Stream.Stream<Row, E, R>>,
  retention: CompactionRetention,
): Stream.Stream<CompactedKey, E, R> =>
  groupRowsByKey(mergeRowStreams(sources)).pipe(
    Stream.map((input) => ({
      input,
      kept: retainKeyVersions(input, {
        now: retention.now,
        retainAboveSeq: Option.none(),
        isBottom: retention.isBottom,
        mergeOperator: retention.mergeOperator,
      }),
    })),
  );

/**
 * Merges compaction inputs into each key's surviving versions, in key order.
 *
 * **Details**
 *
 * A streaming k-way merge (one chunk per input in memory) grouped by key, then
 * {@link retainKeyVersions} with no retained history: readers that need older versions read the
 * manifest they pinned, whose SSTs GC keeps. Expired values become deletes, expired operands are
 * dropped, operands that expire together fold (onto their base when it expires with them, D81),
 * and deletes are dropped at the bottom. Keys left with no
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
  compactKeys(sources, retention).pipe(
    Stream.filterMap(({ kept }) =>
      Arr.isReadonlyArrayNonEmpty(kept) ? Result.succeed(kept) : Result.failVoid,
    ),
  );
