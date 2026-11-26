import { Bytes, Option, Result, Schema } from "effect";

import { type BloomFilter, BloomFilterBuilder, bloomFilterMayContain } from "./bloom-filter.ts";
import { DataBlockBuilder } from "./block-codec.ts";
import { ByteReader, ByteReadOutOfBounds, ByteWriter } from "./byte-codec.ts";
import { crc32c } from "./crc32c.ts";
import { SeqNum } from "./ids.ts";
import { type KeyRange, keyRangeOverlapsClosed } from "./key-range.ts";
import { SstCorruptedError } from "./lsm-core-errors.ts";
import type { Row } from "./row.ts";

/** Current SST format version, the u16 at the very end of every SST. */
export const SST_FORMAT_VERSION = 1;

/** Bytes of the trailer: footer length u32, meta offset u64, format version u16. */
export const SST_TRAILER_LENGTH = 14;

/**
 * Kind of SST: `Compacted` holds rows in key order with a Bloom filter (L0 and sorted runs);
 * `Changelog` holds a flush's rows in sequence order for change data capture.
 */
export const SstKind = Schema.Literals(["Compacted", "Changelog"]);

/** Kind of SST. */
export type SstKind = typeof SstKind.Type;

const SectionLocation = Schema.Struct({ offset: Schema.Number, length: Schema.Number });

/** Schema for the per-SST counters stored in the footer (SlateDB's stats block). */
export const SstStats = Schema.Struct({
  puts: Schema.Number,
  deletes: Schema.Number,
  merges: Schema.Number,
  keyBytes: Schema.Number,
  valueBytes: Schema.Number,
  blockCount: Schema.Number,
});

/** Per-SST counters. */
export type SstStats = typeof SstStats.Type;

/** Schema for an SST's footer, stored as JSON just before the trailer. */
export const SstFooter = Schema.Struct({
  formatVersion: Schema.Number,
  kind: SstKind,
  rowCount: Schema.Number,
  firstKey: Schema.Uint8ArrayFromBase64,
  lastKey: Schema.Uint8ArrayFromBase64,
  minSeq: SeqNum,
  maxSeq: SeqNum,
  filter: Schema.OptionFromNullOr(SectionLocation),
  index: SectionLocation,
  stats: SstStats,
});

/** An SST's footer. */
export type SstFooter = typeof SstFooter.Type;

const SstFooterJson = Schema.fromJsonString(SstFooter);
const encodeSstFooterJson = Schema.encodeSync(SstFooterJson);
const decodeSstFooterJson = Schema.decodeResult(SstFooterJson);

/** One index entry: where a data block is and the keys it spans. */
export interface SstIndexEntry {
  readonly offset: number;
  readonly length: number;
  readonly firstKey: Uint8Array;
  readonly lastKey: Uint8Array;
}

/** Options for {@link SstBuilder}. */
export interface SstBuilderOptions {
  readonly kind: SstKind;
  /** Target data block size in bytes; a block closes once it reaches this. */
  readonly blockSize: number;
  /** Bloom filter bits per distinct key (compacted SSTs only). */
  readonly filterBitsPerKey: number;
  /** Fewest distinct keys for which a compacted SST gets a Bloom filter. */
  readonly minFilterKeys: number;
}

/** What {@link SstBuilder.finish} reports about the finished SST. */
export interface SstInfo {
  readonly footer: SstFooter;
  /** Total SST size in bytes. */
  readonly sizeBytes: number;
  /** Offset where the metadata section (filter, index, footer, trailer) starts. */
  readonly metaOffset: number;
  /** Length of the metadata section, through the trailer. */
  readonly metaLength: number;
}

const encodeSection = (body: Uint8Array): Uint8Array => {
  const section = new Uint8Array(body.length + 4);
  section.set(body);
  new DataView(section.buffer).setUint32(body.length, crc32c(body), true);
  return section;
};

/**
 * Builds an SST incrementally so the caller can stream it out.
 *
 * **Details**
 *
 * Layout: `data blocks · [Bloom filter] · index · footer (JSON) · footer length u32 · meta offset u64
 * · format version u16`. Every block and section ends with a CRC32C. Compacted SSTs take rows in
 * {@link rowOrder}; changelog SSTs in {@link changelogRowOrder}. {@link add} returns each data block
 * as soon as it closes; {@link finish} returns the last block and the metadata section.
 */
export class SstBuilder {
  private block = new DataBlockBuilder();
  private readonly index: Array<SstIndexEntry> = [];
  private readonly filter = new BloomFilterBuilder();
  private offset = 0;
  private rowCount = 0;
  private firstKey = Option.none<Uint8Array>();
  private lastKey: Uint8Array = new Uint8Array(0);
  private minSeq = Number.MAX_SAFE_INTEGER;
  private maxSeq = 0;
  private readonly stats = { puts: 0, deletes: 0, merges: 0, keyBytes: 0, valueBytes: 0 };
  private previousFilterKey = Option.none<Uint8Array>();

  private readonly options: SstBuilderOptions;

  constructor(options: SstBuilderOptions) {
    this.options = options;
  }

  /** Number of rows added. */
  get count(): number {
    return this.rowCount;
  }

  /** Bytes produced so far, including the open block. */
  get estimatedSize(): number {
    return this.offset + this.block.estimatedSize;
  }

  private closeBlock(): Uint8Array {
    const firstKey = Option.getOrElse(this.block.firstKey, () => new Uint8Array(0));
    const lastKey = this.block.lastKey;
    const bytes = this.block.finish();
    this.index.push({ offset: this.offset, length: bytes.length, firstKey, lastKey });
    this.offset += bytes.length;
    this.block = new DataBlockBuilder();
    return bytes;
  }

  /**
   * Adds a row.
   *
   * @returns Data blocks that closed because of this row (usually none), ready to write in order.
   */
  add(row: Row): ReadonlyArray<Uint8Array> {
    this.block.add(row);
    this.rowCount++;
    if (Option.isNone(this.firstKey)) this.firstKey = Option.some(row.key);
    this.lastKey = row.key;
    if (row.seq < this.minSeq) this.minSeq = row.seq;
    if (row.seq > this.maxSeq) this.maxSeq = row.seq;
    this.stats.keyBytes += row.key.length;
    switch (row.value._tag) {
      case "Value":
        this.stats.puts++;
        this.stats.valueBytes += row.value.bytes.length;
        break;
      case "Merge":
        this.stats.merges++;
        this.stats.valueBytes += row.value.operand.length;
        break;
      case "Tombstone":
        this.stats.deletes++;
    }
    if (this.options.kind === "Compacted") {
      const previous = this.previousFilterKey;
      if (Option.isNone(previous) || !Bytes.Equivalence(previous.value, row.key)) {
        this.filter.addKey(row.key);
        this.previousFilterKey = Option.some(row.key);
      }
    }
    return this.block.estimatedSize >= this.options.blockSize ? [this.closeBlock()] : [];
  }

  /**
   * Closes the last block and writes the metadata section.
   *
   * @returns The remaining bytes to write and the SST's {@link SstInfo}. An SST must hold at least
   *   one row; finishing an empty builder is a defect.
   */
  finish(): { readonly tail: Uint8Array; readonly info: SstInfo } {
    if (this.rowCount === 0)
      throw new Error("SstBuilder.finish: an SST must contain at least one row");
    const parts: Array<Uint8Array> = [];
    if (!this.block.isEmpty) parts.push(this.closeBlock());
    const metaOffset = this.offset;
    let cursor = metaOffset;
    let filterLocation = Option.none<{ offset: number; length: number }>();
    if (this.options.kind === "Compacted" && this.filter.keyCount >= this.options.minFilterKeys) {
      const filter = this.filter.build(this.options.filterBitsPerKey);
      const section = encodeSection(
        new ByteWriter(filter.bits.length + 1).u8(filter.numHashes).bytes(filter.bits).toBytes(),
      );
      filterLocation = Option.some({ offset: cursor, length: section.length });
      cursor += section.length;
      parts.push(section);
    }
    const indexWriter = new ByteWriter(this.index.length * 32 + 8).varint(this.index.length);
    for (const entry of this.index) {
      indexWriter
        .varint(entry.offset)
        .varint(entry.length)
        .lengthPrefixed(entry.firstKey)
        .lengthPrefixed(entry.lastKey);
    }
    const indexSection = encodeSection(indexWriter.toBytes());
    const indexLocation = { offset: cursor, length: indexSection.length };
    cursor += indexSection.length;
    parts.push(indexSection);
    const footer: SstFooter = {
      formatVersion: SST_FORMAT_VERSION,
      kind: this.options.kind,
      rowCount: this.rowCount,
      firstKey: Option.getOrElse(this.firstKey, () => new Uint8Array(0)),
      lastKey: this.lastKey,
      minSeq: SeqNum.make(this.minSeq),
      maxSeq: SeqNum.make(this.maxSeq),
      filter: filterLocation,
      index: indexLocation,
      stats: { ...this.stats, blockCount: this.index.length },
    };
    const footerSection = encodeSection(new TextEncoder().encode(encodeSstFooterJson(footer)));
    cursor += footerSection.length;
    parts.push(footerSection);
    parts.push(
      new ByteWriter(SST_TRAILER_LENGTH)
        .u32(footerSection.length)
        .u64(metaOffset)
        .u16(SST_FORMAT_VERSION)
        .toBytes(),
    );
    const sizeBytes = cursor + SST_TRAILER_LENGTH;
    return {
      tail: Bytes.concat(parts),
      info: { footer, sizeBytes, metaOffset, metaLength: sizeBytes - metaOffset },
    };
  }
}

/** An SST's decoded metadata section: footer, block index, and Bloom filter. */
export interface SstMeta {
  readonly footer: SstFooter;
  readonly index: ReadonlyArray<SstIndexEntry>;
  readonly filter: Option.Option<BloomFilter>;
}

const metaCorrupted = (
  sstId: string,
  section: SstCorruptedError["section"],
  reason: SstCorruptedError["reason"],
  offset: number,
  detail: string,
): SstCorruptedError =>
  new SstCorruptedError({
    sstId,
    section,
    reason,
    offset,
    message: `The ${section} section of SST ${sstId} at offset ${offset} is unreadable (${detail}). The stored object is damaged; restore it from a checkpoint or recompact its inputs.`,
  });

const verifySection = (
  sstId: string,
  section: SstCorruptedError["section"],
  bytes: Uint8Array,
  absoluteOffset: number,
): Result.Result<Uint8Array, SstCorruptedError> => {
  if (bytes.length < 4) {
    return Result.fail(
      metaCorrupted(sstId, section, "Truncated", absoluteOffset, "shorter than its checksum"),
    );
  }
  const body = bytes.subarray(0, bytes.length - 4);
  const stored = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(
    body.length,
    true,
  );
  return crc32c(body) === stored
    ? Result.succeed(body)
    : Result.fail(
        metaCorrupted(sstId, section, "ChecksumMismatch", absoluteOffset, "CRC32C mismatch"),
      );
};

/**
 * Decodes an SST's metadata section: the bytes from `metaOffset` to the end of the SST, which one
 * request fetches (`SstHandle.metaOffset` and `metaLength`).
 *
 * @returns The metadata, or `SstCorruptedError` when a checksum, the trailer, or the format version
 *   is wrong.
 */
export const decodeSstMeta = (
  sstId: string,
  meta: Uint8Array,
): Result.Result<SstMeta, SstCorruptedError> =>
  Result.gen(function* () {
    if (meta.length < SST_TRAILER_LENGTH) {
      return yield* Result.fail(
        metaCorrupted(sstId, "Trailer", "Truncated", 0, "shorter than the trailer"),
      );
    }
    const trailer = new ByteReader(meta, meta.length - SST_TRAILER_LENGTH);
    const footerLength = trailer.u32();
    const metaOffset = trailer.u64();
    const version = trailer.u16();
    if (version !== SST_FORMAT_VERSION) {
      return yield* Result.fail(
        metaCorrupted(
          sstId,
          "Trailer",
          "UnsupportedVersion",
          metaOffset,
          `format version ${version}`,
        ),
      );
    }
    const footerStart = meta.length - SST_TRAILER_LENGTH - footerLength;
    if (footerStart < 0) {
      return yield* Result.fail(
        metaCorrupted(sstId, "Footer", "Truncated", metaOffset, "footer longer than meta"),
      );
    }
    const footerBody = yield* verifySection(
      sstId,
      "Footer",
      meta.subarray(footerStart, footerStart + footerLength),
      metaOffset + footerStart,
    );
    const footer = yield* decodeSstFooterJson(new TextDecoder().decode(footerBody)).pipe(
      Result.mapError(() =>
        metaCorrupted(
          sstId,
          "Footer",
          "Malformed",
          metaOffset + footerStart,
          "footer is not valid JSON",
        ),
      ),
    );
    const slice = (location: { offset: number; length: number }) =>
      meta.subarray(location.offset - metaOffset, location.offset - metaOffset + location.length);
    const indexBody = yield* verifySection(
      sstId,
      "Index",
      slice(footer.index),
      footer.index.offset,
    );
    const index = yield* Result.try({
      try: () => {
        const reader = new ByteReader(indexBody);
        const count = reader.varint();
        const entries: Array<SstIndexEntry> = [];
        for (let entry = 0; entry < count; entry++) {
          entries.push({
            offset: reader.varint(),
            length: reader.varint(),
            firstKey: reader.lengthPrefixed().slice(),
            lastKey: reader.lengthPrefixed().slice(),
          });
        }
        return entries;
      },
      catch: (cause) =>
        metaCorrupted(
          sstId,
          "Index",
          "Malformed",
          footer.index.offset,
          cause instanceof ByteReadOutOfBounds ? cause.message : "unreadable",
        ),
    });
    let filter = Option.none<BloomFilter>();
    if (Option.isSome(footer.filter)) {
      const filterBody = yield* verifySection(
        sstId,
        "Filter",
        slice(footer.filter.value),
        footer.filter.value.offset,
      );
      if (filterBody.length < 1) {
        return yield* Result.fail(
          metaCorrupted(sstId, "Filter", "Truncated", footer.filter.value.offset, "empty filter"),
        );
      }
      filter = Option.some({ numHashes: filterBody[0] ?? 1, bits: filterBody.slice(1) });
    }
    return { footer, index, filter };
  });

/**
 * Whether an SST may hold `key`: inside its key bounds and not excluded by its Bloom filter.
 */
export const sstMayContainKey = (meta: SstMeta, key: Uint8Array): boolean =>
  Bytes.Order(key, meta.footer.firstKey) >= 0 &&
  Bytes.Order(key, meta.footer.lastKey) <= 0 &&
  (Option.isNone(meta.filter) || bloomFilterMayContain(meta.filter.value, key));

/**
 * Index of the first block that can hold `key` (the first block whose last key is `>= key`).
 *
 * @returns The block index, or `Option.none()` when every block ends before `key`. Versions of one
 *   key can continue into the following blocks.
 */
export const firstBlockForKey = (meta: SstMeta, key: Uint8Array): Option.Option<number> => {
  let low = 0;
  let high = meta.index.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    const entry = meta.index[middle];
    if (entry !== undefined && Bytes.Order(entry.lastKey, key) < 0) low = middle + 1;
    else high = middle;
  }
  return low < meta.index.length ? Option.some(low) : Option.none();
};

/** Indexes of the blocks whose keys overlap `range`, in order. */
export const blocksOverlappingRange = (meta: SstMeta, range: KeyRange): ReadonlyArray<number> => {
  const result: Array<number> = [];
  const start = Option.getOrElse(firstBlockForKey(meta, range.start), () => meta.index.length);
  for (let index = start; index < meta.index.length; index++) {
    const entry = meta.index[index];
    if (entry === undefined) break;
    if (!keyRangeOverlapsClosed(range, entry.firstKey, entry.lastKey)) break;
    result.push(index);
  }
  return result;
};

/** One byte-range read that covers consecutive data blocks of an SST. */
export interface BlockRead {
  /** Offset of the first block's bytes. */
  readonly offset: number;
  /** Bytes from the first block's start to the last block's end. */
  readonly length: number;
  /** The blocks the read covers, in order. */
  readonly blocks: ReadonlyArray<number>;
}

/**
 * Groups `blocks` (positions in `index`, an SST's block index, in order) into byte-range reads of at most `maxBytes`
 * each, merging only blocks that sit next to each other in the file. A block larger than `maxBytes`
 * is read alone. A compaction step reads its inputs this way so a 64 MiB input costs tens of
 * requests rather than one per 4 KiB block (a Worker invocation may make only so many, D61).
 */
export const groupBlockReads = (
  index: ReadonlyArray<SstIndexEntry>,
  blocks: ReadonlyArray<number>,
  maxBytes: number,
): ReadonlyArray<BlockRead> => {
  const reads: Array<BlockRead> = [];
  let current: { offset: number; end: number; blocks: Array<number> } | undefined;
  for (const block of blocks) {
    const entry = index[block];
    if (entry === undefined) continue;
    const end = entry.offset + entry.length;
    if (current !== undefined && entry.offset === current.end && end - current.offset <= maxBytes) {
      current.end = end;
      current.blocks.push(block);
      continue;
    }
    if (current !== undefined) {
      reads.push({
        offset: current.offset,
        length: current.end - current.offset,
        blocks: current.blocks,
      });
    }
    current = { offset: entry.offset, end, blocks: [block] };
  }
  if (current !== undefined) {
    reads.push({
      offset: current.offset,
      length: current.end - current.offset,
      blocks: current.blocks,
    });
  }
  return reads;
};
