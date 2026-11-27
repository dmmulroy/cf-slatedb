import { Array as Arr, Brand, Bytes, Stream } from "effect";

import type { SeqNum } from "./ids.ts";
import { mergeRowStreams } from "./merge-iterator.ts";
import type { Row } from "./row.ts";
import { groupRowsByKey } from "./row-resolution.ts";

/**
 * One key's versions as a read resolves them: newest first, each sequence number once, every
 * version at or below the read's snapshot, and each sequence number taken from exactly one source.
 *
 * **Details**
 *
 * Only {@link collectKeyVersions}, {@link streamKeyVersions}, and {@link keyVersionsOf} build one,
 * and `resolveVisibleRow` accepts nothing else. A merge operand applied twice (D135) or lost to a
 * compaction's fold (D135) is a read that assembled its versions by hand.
 */
export type KeyVersions = Brand.Branded<ReadonlyArray<Row>, "KeyVersions">;

// The invariant is established by the functions below before either constructor is called.
const brandKeyVersions = Brand.nominal<KeyVersions>();

/**
 * Which sequence numbers a read takes from where: the manifest holds every row at or below its
 * `lastL0Seq`, so the writer supplies only the rows above it, up to the snapshot (D135).
 *
 * **Details**
 *
 * A writer is seeded at its manifest's `lastL0Seq` and a flush records its cut only after the
 * manifest commit, so every writer row at or below a manifest's `lastL0Seq` is in that manifest,
 * possibly folded by a compaction into a row with a newer operand's sequence number. Taking such a
 * row from the writer as well applies an operand twice; deduplicating by sequence number instead
 * may keep the writer's unfolded row and drop the fold, losing the operands below it.
 */
export interface ReadSourceBounds {
  /** `lastL0Seq` of the manifest the read takes its SSTs from. */
  readonly manifestLastL0Seq: SeqNum;
  /** Newest sequence number the read sees. */
  readonly snapshotSeq: SeqNum;
}

/**
 * Whether a read takes `row` from the writer rather than from its manifest: above the manifest's
 * `lastL0Seq` and at or below the snapshot.
 */
export const readTakesWriterRow = (row: Row, bounds: ReadSourceBounds): boolean =>
  row.seq > bounds.manifestLastL0Seq && row.seq <= bounds.snapshotSeq;

/** Whether `rows` are newest first with no sequence number twice. */
const strictlyNewestFirst = (rows: ReadonlyArray<Row>): boolean =>
  rows.every((row, index) => index === 0 || (rows[index - 1]?.seq ?? row.seq) > row.seq);

/** `rows` newest first, the first of each sequence number kept. */
const newestFirstOnce = (rows: ReadonlyArray<Row>): ReadonlyArray<Row> => {
  if (strictlyNewestFirst(rows)) return rows;
  const sorted = rows
    .map((row, position) => ({ row, position }))
    .sort((left, right) =>
      left.row.seq === right.row.seq
        ? left.position - right.position
        : left.row.seq > right.row.seq
          ? -1
          : 1,
    );
  return sorted.flatMap(({ row }, index) =>
    index > 0 && sorted[index - 1]?.row.seq === row.seq ? [] : [row],
  );
};

/**
 * One key's versions from a single source (one SST's rows, or a test's list), in any order:
 * newest first, each sequence number once (the first given wins).
 *
 * **Details**
 *
 * For rows from one source only. A read that combines the writer's rows with a manifest's must use
 * {@link collectKeyVersions} or {@link streamKeyVersions}, which take each sequence number from the
 * one source that owns it; deduplicating across sources is not enough (see
 * {@link ReadSourceBounds}).
 */
export const keyVersionsOf = (rows: ReadonlyArray<Row>): KeyVersions =>
  brandKeyVersions(newestFirstOnce(rows));

/**
 * The versions of `key` a point read resolves: the writer's rows above the manifest's `lastL0Seq`
 * (see {@link ReadSourceBounds}) and the manifest's SST rows at or below the snapshot.
 *
 * @param writerRows - The writer's rows the read was given (any keys, any order).
 * @param sstRows - Each candidate SST's rows for `key`, newest SST first.
 */
export const collectKeyVersions = (
  key: Uint8Array,
  writerRows: ReadonlyArray<Row>,
  sstRows: ReadonlyArray<ReadonlyArray<Row>>,
  bounds: ReadSourceBounds,
): KeyVersions =>
  brandKeyVersions(
    newestFirstOnce([
      ...writerRows.filter(
        (row) => Bytes.Equivalence(row.key, key) && readTakesWriterRow(row, bounds),
      ),
      // Two SSTs share a sequence number only when a crash replayed a flush: the rows are equal.
      ...sstRows.flat().filter((row) => row.seq <= bounds.snapshotSeq),
    ]),
  );

/**
 * The writer's rows a read takes beside its manifest (see {@link ReadSourceBounds}), in the order
 * given.
 */
export const writerRowsForRead = (
  writerRows: ReadonlyArray<Row>,
  bounds: ReadSourceBounds,
): ReadonlyArray<Row> => writerRows.filter((row) => readTakesWriterRow(row, bounds));

/** One key a scan examined: the versions it resolves, and what examining the key cost. */
export interface ScannedKey {
  readonly key: Uint8Array;
  /**
   * The versions the scan resolves; empty when the key's only rows are the writer's copies of rows
   * the manifest holds (a compaction may have dropped them there, a delete at the bottom).
   */
  readonly versions: KeyVersions;
  /**
   * Every row the merge read for the key, the writer's copies the read does not take among them:
   * they were fetched, so a page's work budget counts them (D125, D135).
   */
  readonly examined: Arr.NonEmptyReadonlyArray<Row>;
}

/**
 * Each key a scan examines, in key order: the writer's rows above the manifest's `lastL0Seq`
 * merged with the manifest's SST rows at or below the snapshot (see {@link ReadSourceBounds}).
 *
 * @param writerRows - The writer's rows the scan was given, in `rowOrder`.
 * @param ssts - Each overlapping SST's rows in `rowOrder`, newest SST first.
 */
export const streamKeyVersions = <E, R>(
  writerRows: ReadonlyArray<Row>,
  ssts: ReadonlyArray<Stream.Stream<Row, E, R>>,
  bounds: ReadSourceBounds,
): Stream.Stream<ScannedKey, E, R> => {
  const taken = writerRowsForRead(writerRows, bounds);
  // The writer's copies of rows the manifest holds go last: the merge keeps the earliest input's
  // row of a (key, seq), so an SST's row (perhaps a compaction's fold) always wins over them.
  // Copies, so each is told apart by identity from an SST row that may be the same object.
  const skipped = new Set(
    writerRows
      .filter((row) => row.seq <= bounds.snapshotSeq && !readTakesWriterRow(row, bounds))
      .map((row): Row => ({ ...row })),
  );
  return groupRowsByKey(
    mergeRowStreams([
      Stream.fromIterable(taken),
      ...ssts.map((rows) => rows.pipe(Stream.filter((row) => row.seq <= bounds.snapshotSeq))),
      Stream.fromIterable([...skipped]),
    ]),
  ).pipe(
    Stream.map((examined) => ({
      key: examined[0].key,
      // The merge emits each (key, seq) once and newest first within a key; the writer's rows
      // taken share no sequence number with the SSTs, so each version has one source.
      versions: brandKeyVersions(examined.filter((row) => !skipped.has(row))),
      examined,
    })),
  );
};
