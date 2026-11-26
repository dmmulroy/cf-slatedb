import { Array as Arr, Bytes, Option, Result, Schema, Stream } from "effect";

import type { SeqNum, Timestamp } from "./ids.ts";
import { MergeOperatorMissingError } from "./lsm-core-errors.ts";
import { type Row, RowValue } from "./row.ts";

/**
 * Combines a key's merge operands with its older value.
 *
 * **Details**
 *
 * Must be associative: shaledb regroups operands across the memtable, flushes, and compactions,
 * and combines operands with no base (`existing` none) into one operand that later merges onto an
 * older value.
 */
export interface MergeOperator {
  readonly name: MergeOperatorName;
  /**
   * @param existing - The older value, or none when the history has no base (or a delete).
   * @param operands - Operands from oldest to newest.
   */
  readonly merge: (
    key: Uint8Array,
    existing: Option.Option<Uint8Array>,
    operands: Arr.NonEmptyReadonlyArray<Uint8Array>,
  ) => Uint8Array;
}

/**
 * Schema for the built-in merge operators a database can select (operators are code, so a database
 * names one rather than storing it):
 *
 * - `Concat`: appends operand bytes to the existing value.
 * - `U64Add`: adds operands to the existing value as little-endian unsigned 64-bit integers
 *   (wrapping); shorter inputs are zero-extended.
 */
export const MergeOperatorName = Schema.Literals(["Concat", "U64Add"]);

/** Name of a built-in merge operator. */
export type MergeOperatorName = typeof MergeOperatorName.Type;

const readU64 = (bytes: Uint8Array): bigint => {
  let value = 0n;
  for (let index = Math.min(8, bytes.length) - 1; index >= 0; index--) {
    value = (value << 8n) | BigInt(bytes[index] ?? 0);
  }
  return value;
};

const writeU64 = (value: bigint): Uint8Array => {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, BigInt.asUintN(64, value), true);
  return bytes;
};

/** The built-in merge operators by name. */
export const MERGE_OPERATORS: Readonly<Record<MergeOperatorName, MergeOperator>> = {
  Concat: {
    name: "Concat",
    merge: (_key, existing, operands) => Bytes.concat([...Option.toArray(existing), ...operands]),
  },
  U64Add: {
    name: "U64Add",
    merge: (_key, existing, operands) =>
      writeU64(
        operands.reduce(
          (sum, operand) => sum + readU64(operand),
          Option.match(existing, { onNone: () => 0n, onSome: readU64 }),
        ),
      ),
  },
};

const isExpired = (row: Row, now: Timestamp): boolean =>
  Option.isSome(row.expireTs) && row.expireTs.value <= now;

const minExpire = (
  left: Option.Option<Timestamp>,
  right: Option.Option<Timestamp>,
): Option.Option<Timestamp> =>
  Option.isNone(left)
    ? right
    : Option.isNone(right)
      ? left
      : Option.some(left.value <= right.value ? left.value : right.value);

/** Options for reading a key's visible value. */
export interface ReadResolutionOptions {
  /** Newest sequence number the read may see (its snapshot). */
  readonly snapshotSeq: SeqNum;
  /** Current time, for TTL: a version whose `expireTs <= now` is treated as deleted. */
  readonly now: Timestamp;
  readonly mergeOperator: Option.Option<MergeOperator>;
}

/**
 * Resolves a key's versions (newest first) into the value a read at `snapshotSeq` sees.
 *
 * **Details**
 *
 * Walks from the newest visible version until a value or a delete, then applies the newer merge
 * operands oldest to newest on top. An expired value reads as a delete; an expired operand is
 * skipped. The result carries the newest contributing version's `seq` and `createTs` and the
 * earliest `expireTs` among contributors.
 *
 * @returns The visible row (always a `Value`), none when the key is deleted, expired, or absent at
 *   the snapshot, or `MergeOperatorMissingError` when operands need an operator and none is set.
 */
export const resolveVisibleRow = (
  versions: ReadonlyArray<Row>,
  options: ReadResolutionOptions,
): Result.Result<Option.Option<Row>, MergeOperatorMissingError> => {
  const operands: Array<Row> = [];
  let base = Option.none<Row>();
  for (const version of versions) {
    if (version.seq > options.snapshotSeq) continue;
    if (version.value._tag === "Tombstone") break;
    if (version.value._tag === "Value") {
      if (!isExpired(version, options.now)) base = Option.some(version);
      break;
    }
    if (!isExpired(version, options.now)) operands.push(version);
  }
  const [newest, ...rest] = operands;
  if (newest === undefined) return Result.succeed(base);
  if (Option.isNone(options.mergeOperator)) {
    return Result.fail(
      new MergeOperatorMissingError({
        message:
          "A key has merge operands, but the database has no merge operator, so its value cannot be read. Configure the database's merge operator, then read again.",
      }),
    );
  }
  const ordered = [newest, ...rest].reverse();
  const operandBytes = Arr.map(ordered, (row) =>
    row.value._tag === "Merge" ? row.value.operand : new Uint8Array(0),
  );
  if (!Arr.isReadonlyArrayNonEmpty(operandBytes)) return Result.succeed(base);
  const existing = Option.map(base, (row) =>
    row.value._tag === "Value" ? row.value.bytes : new Uint8Array(0),
  );
  const merged = options.mergeOperator.value.merge(newest.key, existing, operandBytes);
  const expireTs = [...ordered, ...Option.toArray(base)].reduce<Option.Option<Timestamp>>(
    (earliest, row) => minExpire(earliest, row.expireTs),
    Option.none(),
  );
  return Result.succeed(
    Option.some({
      key: newest.key,
      seq: newest.seq,
      value: RowValue.Value({ bytes: merged }),
      createTs: newest.createTs,
      expireTs,
    }),
  );
};

/** Options for compacting a key's versions. */
export interface RetentionOptions {
  /** Current time, for TTL. */
  readonly now: Timestamp;
  /**
   * Versions newer than this sequence number are kept unchanged, for snapshots that still read
   * below the newest version; none keeps only the newest resolved version.
   */
  readonly retainAboveSeq: Option.Option<SeqNum>;
  /** Whether the output is the oldest data for its keys, so deletes need not be kept. */
  readonly isBottom: boolean;
  readonly mergeOperator: Option.Option<MergeOperator>;
}

/**
 * Compacts a key's versions (newest first) into the versions that must survive.
 *
 * **Details**
 *
 * Versions above `retainAboveSeq` are kept as they are. Below it, history collapses into one
 * version (SlateDB's retention rules): an expired value becomes a delete (so older versions stay
 * hidden), an expired operand is dropped, operands fold onto their base with the merge operator,
 * versions older than the base are dropped, and a delete is dropped at the bottom. Without a merge
 * operator, operands and their base are kept unchanged. Never fails.
 */
export const retainKeyVersions = (
  versions: ReadonlyArray<Row>,
  options: RetentionOptions,
): ReadonlyArray<Row> => {
  const kept: Array<Row> = [];
  let index = 0;
  if (Option.isSome(options.retainAboveSeq)) {
    const floor = options.retainAboveSeq.value;
    while (index < versions.length && (versions[index]?.seq ?? 0) > floor) {
      const version = versions[index];
      if (version !== undefined) kept.push(version);
      index++;
    }
  }
  const operands: Array<Row> = [];
  let base = Option.none<Row>();
  for (; index < versions.length; index++) {
    const version = versions[index];
    if (version === undefined) break;
    if (version.value._tag === "Merge") {
      if (!isExpired(version, options.now)) operands.push(version);
      continue;
    }
    base =
      version.value._tag === "Value" && isExpired(version, options.now)
        ? Option.some({ ...version, value: RowValue.Tombstone(), expireTs: Option.none() })
        : Option.some(version);
    break;
  }
  const baseIsDelete = Option.isSome(base) && base.value.value._tag === "Tombstone";
  const [newest] = operands;
  if (newest === undefined) {
    if (Option.isSome(base) && !(baseIsDelete && options.isBottom)) kept.push(base.value);
    return kept;
  }
  if (Option.isNone(options.mergeOperator)) {
    kept.push(...operands);
    if (Option.isSome(base) && !(baseIsDelete && options.isBottom)) kept.push(base.value);
    return kept;
  }
  const ordered = [...operands].reverse();
  const operandBytes = ordered.map((row) =>
    row.value._tag === "Merge" ? row.value.operand : new Uint8Array(0),
  );
  if (!Arr.isReadonlyArrayNonEmpty(operandBytes)) return kept;
  const existing = Option.flatMap(base, (row) =>
    row.value._tag === "Value" ? Option.some(row.value.bytes) : Option.none(),
  );
  const merged = options.mergeOperator.value.merge(newest.key, existing, operandBytes);
  const expireTs = [...ordered, ...Option.toArray(base)].reduce<Option.Option<Timestamp>>(
    (earliest, row) => minExpire(earliest, row.expireTs),
    Option.none(),
  );
  // With a base (value or delete) the history is complete: the result is a value. Without one,
  // older data may still sit in lower levels, so the folded operands stay an operand.
  kept.push({
    key: newest.key,
    seq: newest.seq,
    value: Option.isSome(base)
      ? RowValue.Value({ bytes: merged })
      : RowValue.Merge({ operand: merged }),
    createTs: newest.createTs,
    expireTs,
  });
  return kept;
};

/**
 * Keeps, of rows in {@link rowOrder}, the versions of each key that a read can reach when its
 * snapshot is at or after the key's newest row: from that newest row down to and including the
 * first row that is not a merge operand. The rows below it are dropped.
 *
 * **Details**
 *
 * {@link resolveVisibleRow} walks a key's versions newest first and stops at the first value or
 * delete, so it never looks at the rows this drops, whatever the time (an expired value still
 * ends the walk) and whatever the merge operator. A flush uses it: every read of the SST a flush
 * writes is at a snapshot at or after the flush's newest row, because a read at an older snapshot
 * takes those rows from the writer or from an older manifest (D70).
 */
export const keepReachableVersions = <E, R>(
  rows: Stream.Stream<Row, E, R>,
): Stream.Stream<Row, E, R> =>
  rows.pipe(
    Stream.mapAccum<{ readonly key: Uint8Array | undefined; readonly settled: boolean }, Row, Row>(
      () => ({ key: undefined, settled: false }),
      (state, row) => {
        const sameKey = state.key !== undefined && Bytes.Equivalence(state.key, row.key);
        if (sameKey && state.settled) return [state, []] as const;
        return [{ key: row.key, settled: row.value._tag !== "Merge" }, [row]] as const;
      },
    ),
  );

/**
 * Groups a stream in {@link rowOrder} into each key's versions, newest first.
 */
export const groupRowsByKey = <E, R>(
  rows: Stream.Stream<Row, E, R>,
): Stream.Stream<Arr.NonEmptyReadonlyArray<Row>, E, R> =>
  rows.pipe(
    Stream.mapAccum<Array<Row>, Row, Arr.NonEmptyReadonlyArray<Row>>(
      () => [],
      (pending, row) => {
        const head = pending[0];
        if (head === undefined || Bytes.Equivalence(head.key, row.key)) {
          pending.push(row);
          return [pending, []] as const;
        }
        return [[row], Arr.isReadonlyArrayNonEmpty(pending) ? [pending] : []] as const;
      },
      { onHalt: (pending) => (Arr.isReadonlyArrayNonEmpty(pending) ? [pending] : []) },
    ),
  );
