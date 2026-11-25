import { Bytes, Option, Result } from "effect";

import { ByteReader, ByteReadOutOfBounds, ByteWriter } from "./byte-codec.ts";
import { crc32c } from "./crc32c.ts";
import { SeqNum, Timestamp } from "./ids.ts";
import { SstCorruptedError } from "./lsm-core-errors.ts";
import { type Row, RowValue } from "./row.ts";

/** Rows between restart points, where the full key is stored (SlateDB's `SstRowCodecV2`). */
export const BLOCK_RESTART_INTERVAL = 16;

const FLAG_TOMBSTONE = 1;
const FLAG_MERGE = 2;
const FLAG_HAS_EXPIRE = 4;
const FLAG_HAS_CREATE = 8;

const sharedPrefixLength = (left: Uint8Array, right: Uint8Array): number => {
  const limit = Math.min(left.length, right.length);
  let index = 0;
  while (index < limit && left[index] === right[index]) index++;
  return index;
};

/**
 * Builds one data block.
 *
 * **Details**
 *
 * Each row is `shared varint · unshared varint · value_len varint · key suffix · value · seq u64 ·
 * flags u8 · [expire_ts u64] · [create_ts u64]`, keys prefix-compressed against the previous row and
 * stored in full every {@link BLOCK_RESTART_INTERVAL} rows. The block ends with the restart offsets
 * (u32 each), their count (u32), and a CRC32C (u32) of everything before it. Rows must be added in
 * {@link rowOrder}.
 */
export class DataBlockBuilder {
  private readonly writer = new ByteWriter(4096);
  private readonly restarts: Array<number> = [];
  private previousKey: Uint8Array = new Uint8Array(0);
  private count = 0;
  private first: Option.Option<Uint8Array> = Option.none();

  /** Number of rows added. */
  get rowCount(): number {
    return this.count;
  }

  /** Whether no row has been added. */
  get isEmpty(): boolean {
    return this.count === 0;
  }

  /** First key added, if any. */
  get firstKey(): Option.Option<Uint8Array> {
    return this.first;
  }

  /** Last key added (empty before the first row). */
  get lastKey(): Uint8Array {
    return this.previousKey;
  }

  /** Size the block would have if finished now. */
  get estimatedSize(): number {
    return this.writer.length + (this.restarts.length + 2) * 4;
  }

  /** Appends a row; rows must arrive in key-ascending, sequence-descending order. */
  add(row: Row): void {
    const restart = this.count % BLOCK_RESTART_INTERVAL === 0;
    if (restart) this.restarts.push(this.writer.length);
    const shared = restart ? 0 : sharedPrefixLength(this.previousKey, row.key);
    const payload =
      row.value._tag === "Value"
        ? row.value.bytes
        : row.value._tag === "Merge"
          ? row.value.operand
          : new Uint8Array(0);
    const flags =
      (row.value._tag === "Tombstone" ? FLAG_TOMBSTONE : 0) |
      (row.value._tag === "Merge" ? FLAG_MERGE : 0) |
      (Option.isSome(row.expireTs) ? FLAG_HAS_EXPIRE : 0) |
      (Option.isSome(row.createTs) ? FLAG_HAS_CREATE : 0);
    this.writer
      .varint(shared)
      .varint(row.key.length - shared)
      .varint(payload.length)
      .bytes(row.key.subarray(shared))
      .bytes(payload)
      .u64(row.seq)
      .u8(flags);
    if (Option.isSome(row.expireTs)) this.writer.u64(row.expireTs.value);
    if (Option.isSome(row.createTs)) this.writer.u64(row.createTs.value);
    if (Option.isNone(this.first)) this.first = Option.some(row.key);
    this.previousKey = row.key;
    this.count++;
  }

  /** Writes the restart trailer and checksum and returns the block's bytes. */
  finish(): Uint8Array {
    for (const offset of this.restarts) this.writer.u32(offset);
    this.writer.u32(this.restarts.length);
    const body = this.writer.toBytes();
    const block = new Uint8Array(body.length + 4);
    block.set(body);
    new DataView(block.buffer).setUint32(body.length, crc32c(body), true);
    return block;
  }
}

/** Location of a block or section inside an SST, for error reports. */
export interface BlockLocation {
  readonly sstId: string;
  readonly offset: number;
}

/** A decoded, checksum-verified data block that can seek by key. */
export class DataBlock {
  private readonly data: Uint8Array;
  private readonly restartOffsets: ReadonlyArray<number>;

  constructor(data: Uint8Array, restartOffsets: ReadonlyArray<number>) {
    this.data = data;
    this.restartOffsets = restartOffsets;
  }

  /** Number of restart points (each starts a run of up to 16 rows). */
  get restartCount(): number {
    return this.restartOffsets.length;
  }

  private decodeFrom(offset: number, previousKey: Uint8Array): { row: Row; next: number } {
    const reader = new ByteReader(this.data, offset);
    const shared = reader.varint();
    const unshared = reader.varint();
    const valueLength = reader.varint();
    const suffix = reader.bytes(unshared);
    const key = new Uint8Array(shared + unshared);
    key.set(previousKey.subarray(0, shared));
    key.set(suffix, shared);
    const payload = reader.bytes(valueLength).slice();
    const seq = SeqNum.make(reader.u64());
    const flags = reader.u8();
    const expireTs =
      flags & FLAG_HAS_EXPIRE ? Option.some(Timestamp.make(reader.u64())) : Option.none();
    const createTs =
      flags & FLAG_HAS_CREATE ? Option.some(Timestamp.make(reader.u64())) : Option.none();
    const value =
      flags & FLAG_TOMBSTONE
        ? RowValue.Tombstone()
        : flags & FLAG_MERGE
          ? RowValue.Merge({ operand: payload })
          : RowValue.Value({ bytes: payload });
    return { row: { key, seq, value, createTs, expireTs }, next: reader.offset };
  }

  private keyAtRestart(index: number): Uint8Array {
    return this.decodeFrom(this.restartOffsets[index] ?? 0, new Uint8Array(0)).row.key;
  }

  /** Every row in the block, in stored order. */
  *rows(): Generator<Row> {
    yield* this.rowsFromRestart(0);
  }

  private *rowsFromRestart(restartIndex: number): Generator<Row> {
    let offset = this.restartOffsets[restartIndex] ?? this.data.length;
    let previous: Uint8Array = new Uint8Array(0);
    while (offset < this.data.length) {
      const { row, next } = this.decodeFrom(offset, previous);
      yield row;
      previous = row.key;
      offset = next;
    }
  }

  /**
   * Rows from the first row whose key is `>= key`, to the end of the block.
   *
   * **Details**
   *
   * Binary-searches the restart points (full keys), then decodes at most one run of
   * {@link BLOCK_RESTART_INTERVAL} rows to reach the target.
   */
  *seek(key: Uint8Array): Generator<Row> {
    let low = 0;
    let high = this.restartOffsets.length - 1;
    // Last restart whose key is < key; rows for `key` can begin in that run.
    let start = 0;
    while (low <= high) {
      const middle = (low + high) >>> 1;
      if (Bytes.Order(this.keyAtRestart(middle), key) < 0) {
        start = middle;
        low = middle + 1;
      } else {
        high = middle - 1;
      }
    }
    for (const row of this.rowsFromRestart(start)) {
      if (Bytes.Order(row.key, key) >= 0) yield row;
    }
  }
}

const corrupted = (
  location: BlockLocation,
  reason: SstCorruptedError["reason"],
  detail: string,
): SstCorruptedError =>
  new SstCorruptedError({
    sstId: location.sstId,
    section: "DataBlock",
    reason,
    offset: location.offset,
    message: `Data block at offset ${location.offset} of SST ${location.sstId} is unreadable (${detail}). The stored object is damaged; restore it from a checkpoint or recompact its inputs.`,
  });

/**
 * Verifies a data block's checksum and trailer and returns a seekable {@link DataBlock}.
 *
 * @returns The block, or `SstCorruptedError` (`ChecksumMismatch`, `Truncated`, or `Malformed`).
 */
export const decodeDataBlock = (
  block: Uint8Array,
  location: BlockLocation,
): Result.Result<DataBlock, SstCorruptedError> => {
  if (block.length < 8)
    return Result.fail(corrupted(location, "Truncated", "shorter than its trailer"));
  const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
  const stored = view.getUint32(block.length - 4, true);
  const body = block.subarray(0, block.length - 4);
  if (crc32c(body) !== stored)
    return Result.fail(corrupted(location, "ChecksumMismatch", "CRC32C mismatch"));
  const restartCount = view.getUint32(body.length - 4, true);
  const restartsStart = body.length - 4 - restartCount * 4;
  if (restartsStart < 0)
    return Result.fail(corrupted(location, "Malformed", "restart count too large"));
  const restarts: Array<number> = [];
  for (let index = 0; index < restartCount; index++) {
    const offset = view.getUint32(restartsStart + index * 4, true);
    if (offset >= restartsStart)
      return Result.fail(corrupted(location, "Malformed", "restart past rows"));
    restarts.push(offset);
  }
  const decoded = new DataBlock(body.subarray(0, restartsStart), restarts);
  // Decode every row once so a malformed row is a typed failure here, not a thrown read later.
  try {
    for (const _row of decoded.rows()) {
      // validates
    }
  } catch (cause) {
    if (cause instanceof ByteReadOutOfBounds) {
      return Result.fail(corrupted(location, "Malformed", cause.message));
    }
    throw cause;
  }
  return Result.succeed(decoded);
};
