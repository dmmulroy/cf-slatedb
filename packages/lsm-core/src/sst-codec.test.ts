import { Arbitrary, Bytes, Effect, Option, Result, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { bloomFilterMayContain } from "./bloom-filter.ts";
import { DataBlockBuilder, decodeDataBlock, type DataBlock } from "./block-codec.ts";
import { crc32c } from "./crc32c.ts";
import { SeqNum, SstId, Timestamp } from "./ids.ts";
import {
  assertProperty,
  historyArbitrary,
  keyArbitrary,
  keyToHex,
  narrowHistoryArbitrary,
  narrowKeyArbitrary,
  oracleState,
  rowListsEqual,
  rowsFromWrites,
  sortRows,
} from "./lsm-core-arbitraries.ts";
import { MERGE_OPERATORS, resolveVisibleRow } from "./row-resolution.ts";
import { type Row, RowValue } from "./row.ts";
import {
  blocksOverlappingRange,
  decodeSstMeta,
  firstBlockForKey,
  groupBlockReads,
  SstBuilder,
  type SstMeta,
  sstMayContainKey,
} from "./sst-codec.ts";

const SST_ID = SstId.make("01JD00000000000000000000AA");

/** Builds a whole SST in memory, with tiny blocks so most SSTs have many. */
const buildSst = (rows: ReadonlyArray<Row>, blockSize = 64) => {
  const builder = new SstBuilder({
    kind: "Compacted",
    blockSize,
    filterBitsPerKey: 10,
    minFilterKeys: 0,
  });
  const parts: Array<Uint8Array> = [];
  for (const row of rows) parts.push(...builder.add(row));
  const { tail, info } = builder.finish();
  parts.push(tail);
  return { bytes: Bytes.concat(parts), info };
};

const openSst = (bytes: Uint8Array, metaOffset: number): SstMeta =>
  Result.getOrThrow(decodeSstMeta(SST_ID, bytes.subarray(metaOffset)));

const blockAt = (bytes: Uint8Array, meta: SstMeta, index: number): DataBlock => {
  const entry = meta.index[index];
  if (entry === undefined) throw new Error(`no block ${index}`);
  return Result.getOrThrow(
    decodeDataBlock(bytes.subarray(entry.offset, entry.offset + entry.length), {
      sstId: SST_ID,
      offset: entry.offset,
    }),
  );
};

/** Every version of `key` in the SST, newest first, by the reader's block path. */
const keyVersions = (bytes: Uint8Array, meta: SstMeta, key: Uint8Array): Array<Row> => {
  const versions: Array<Row> = [];
  const first = firstBlockForKey(meta, key);
  if (Option.isNone(first)) return versions;
  for (let index = first.value; index < meta.index.length; index++) {
    let past = false;
    for (const row of blockAt(bytes, meta, index).seek(key)) {
      if (!Bytes.Equivalence(row.key, key)) {
        past = true;
        break;
      }
      versions.push(row);
    }
    if (past) break;
  }
  return versions;
};

describe("crc32c", () => {
  it("matches the CRC32C check value", () => {
    expect(crc32c(new TextEncoder().encode("123456789"))).toBe(0xe3069283);
  });
});

describe("data block codec", () => {
  it("round-trips rows and seeks to the first key at or after the target", () => {
    assertProperty(
      Arbitrary.all({ writes: historyArbitrary(80), probe: keyArbitrary }),
      ({ writes, probe }) => {
        const rows = sortRows(rowsFromWrites(writes));
        if (rows.length === 0) return true;
        const builder = new DataBlockBuilder();
        for (const row of rows) builder.add(row);
        const block = Result.getOrThrow(
          decodeDataBlock(builder.finish(), { sstId: SST_ID, offset: 0 }),
        );
        const decoded = [...block.rows()];
        const sought = [...block.seek(probe)];
        const expected = rows.filter((row) => Bytes.Order(row.key, probe) >= 0);
        return (
          rowListsEqual(decoded, rows) &&
          sought.length === expected.length &&
          sought.every((row, index) => row.seq === expected[index]?.seq)
        );
      },
    );
  });

  it("seeks to every version of a key whose versions cross restart points", () => {
    assertProperty(
      Arbitrary.all({ writes: narrowHistoryArbitrary(120), probe: narrowKeyArbitrary }),
      ({ writes, probe }) => {
        const rows = sortRows(rowsFromWrites(writes));
        const builder = new DataBlockBuilder();
        for (const row of rows) builder.add(row);
        const block = Result.getOrThrow(
          decodeDataBlock(builder.finish(), { sstId: SST_ID, offset: 0 }),
        );
        const expected = rows.filter((row) => Bytes.Order(row.key, probe) >= 0);
        return rowListsEqual([...block.seek(probe)], expected);
      },
    );
  });

  it("rejects a block whose bytes were changed", () => {
    const builder = new DataBlockBuilder();
    builder.add({
      key: new Uint8Array([1]),
      seq: SeqNum.make(1),
      value: { _tag: "Value", bytes: new Uint8Array([7]) },
      createTs: Option.some(Timestamp.make(5)),
      expireTs: Option.none(),
    });
    const block = builder.finish();
    block[1] = (block[1] ?? 0) ^ 0xff;
    const decoded = decodeDataBlock(block, { sstId: SST_ID, offset: 0 });
    expect(Result.isFailure(decoded) && decoded.failure.reason).toBe("ChecksumMismatch");
  });
});

describe("SST codec and read path", () => {
  it("reads every key's visible value as a Map oracle does, at any snapshot and time", () => {
    assertProperty(
      Arbitrary.all({
        writes: historyArbitrary(120),
        probes: Arbitrary.array(keyArbitrary, { maxLength: 12 }),
        snapshotFraction: Arbitrary.schema(
          Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 100 })),
        ),
        nowOffset: Arbitrary.schema(
          Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 200 })),
        ),
      }),
      ({ writes, probes, snapshotFraction, nowOffset }) => {
        const rows = rowsFromWrites(writes);
        if (rows.length === 0) return true;
        const { bytes, info } = buildSst(sortRows(rows));
        const meta = openSst(bytes, info.metaOffset);
        const snapshotSeq = SeqNum.make(Math.round((rows.length * snapshotFraction) / 100));
        const now = Timestamp.make(100 + nowOffset);
        const expected = oracleState(rows, snapshotSeq, now);
        const keys = [...rows.map((row) => row.key), ...probes];
        return keys.every((key) => {
          if (!sstMayContainKey(meta, key)) return !expected.has(keyToHex(key));
          const visible = Result.getOrThrow(
            resolveVisibleRow(keyVersions(bytes, meta, key), {
              snapshotSeq,
              now,
              mergeOperator: Option.some(MERGE_OPERATORS.Concat),
            }),
          );
          const want = expected.get(keyToHex(key));
          return Option.match(visible, {
            onNone: () => want === undefined,
            onSome: (row) =>
              want !== undefined &&
              row.value._tag === "Value" &&
              Bytes.Equivalence(row.value.bytes, want),
          });
        });
      },
    );
  });

  it("records bounds, counts, and a Bloom filter with no false negatives", () => {
    assertProperty(historyArbitrary(80), (writes) => {
      const rows = sortRows(rowsFromWrites(writes));
      if (rows.length === 0) return true;
      const { bytes, info } = buildSst(rows);
      const meta = openSst(bytes, info.metaOffset);
      const filter = meta.filter;
      return (
        info.sizeBytes === bytes.length &&
        info.metaLength === bytes.length - info.metaOffset &&
        meta.footer.rowCount === rows.length &&
        Bytes.Equivalence(meta.footer.firstKey, rows[0]?.key ?? new Uint8Array()) &&
        Bytes.Equivalence(meta.footer.lastKey, rows[rows.length - 1]?.key ?? new Uint8Array()) &&
        Option.isSome(filter) &&
        rows.every((row) => bloomFilterMayContain(filter.value, row.key)) &&
        meta.index.reduce((sum, entry) => sum + entry.length, 0) === info.metaOffset
      );
    });
  });

  it("keeps an SST's metadata a small fraction of its data, whatever its key size (D107)", () => {
    // Rows of long keys (up to MAX_KEY_BYTES, 16 KiB) and short values, as a database keyed by
    // long paths or composite keys holds. 4 MiB of rows each, with production's 16 KiB blocks.
    const fractions: Array<readonly [number, number]> = [];
    for (const keyBytes of [16, 1024, 4096, 16 * 1024]) {
      const rows: Array<Row> = [];
      const count = Math.ceil((4 * 1024 * 1024) / (keyBytes + 16));
      for (let index = 0; index < count; index++) {
        const key = new Uint8Array(keyBytes).fill(0x61);
        key[0] = index >> 16;
        key[1] = (index >> 8) & 0xff;
        key[2] = index & 0xff;
        rows.push({
          key,
          seq: SeqNum.make(index + 1),
          value: RowValue.Value({ bytes: new Uint8Array(16).fill(1) }),
          createTs: Option.none(),
          expireTs: Option.none(),
        });
      }
      const { bytes, info } = buildSst(rows, 16 * 1024);
      fractions.push([keyBytes, Math.round((info.metaLength / info.metaOffset) * 1000) / 1000]);
      // Still readable: every key is found through the index.
      const meta = openSst(bytes, info.metaOffset);
      const last = rows.at(-1);
      if (last === undefined) throw new Error("no rows");
      const found = firstBlockForKey(meta, last.key);
      expect(Option.isSome(found)).toBe(true);
    }
    // The metadata is read whole by every reader of the SST and held in its L1 cache: at most a
    // sixteenth of the data (Bloom filter and index), 4 MiB for a 64 MiB SST. Before D107 it was
    // 50% at 4 KiB keys and 200% at 16 KiB.
    expect(
      fractions.every(([, fraction]) => fraction <= 1 / 16),
      JSON.stringify(fractions),
    ).toBe(true);
  });

  it("lists the blocks a key range overlaps", () => {
    assertProperty(
      Arbitrary.all({ writes: historyArbitrary(80), start: keyArbitrary }),
      ({ writes, start }) => {
        const rows = sortRows(rowsFromWrites(writes));
        if (rows.length === 0) return true;
        const { bytes, info } = buildSst(rows);
        const meta = openSst(bytes, info.metaOffset);
        const range = { start, end: Option.none<Uint8Array>() };
        const blockRows = blocksOverlappingRange(meta, range).flatMap((index) => [
          ...blockAt(bytes, meta, index).rows(),
        ]);
        const expected = rows.filter((row) => Bytes.Order(row.key, start) >= 0);
        return expected.every((row) => blockRows.some((candidate) => candidate.seq === row.seq));
      },
    );
  });

  it("rejects a metadata section with a damaged footer or unknown version", () => {
    const rows = sortRows(
      rowsFromWrites([{ key: new Uint8Array([1]), kind: 0, bytes: new Uint8Array([2]), ttl: 0 }]),
    );
    const { bytes, info } = buildSst(rows);
    const damaged = bytes.slice();
    damaged[damaged.length - 20] = (damaged[damaged.length - 20] ?? 0) ^ 0xff;
    const footerFailure = decodeSstMeta(SST_ID, damaged.subarray(info.metaOffset));
    expect(Result.isFailure(footerFailure) && footerFailure.failure.section).toBe("Footer");

    const wrongVersion = bytes.slice();
    wrongVersion[wrongVersion.length - 2] = 9;
    const versionFailure = decodeSstMeta(SST_ID, wrongVersion.subarray(info.metaOffset));
    expect(Result.isFailure(versionFailure) && versionFailure.failure.reason).toBe(
      "UnsupportedVersion",
    );
  });
});

const smallSize = Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 64 })));

describe("decoders over damaged bytes (read audit)", () => {
  const damageArbitrary = Arbitrary.all({
    writes: historyArbitrary(60),
    position: Arbitrary.schema(
      Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 100_000 })),
    ),
    flip: Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 255 }))),
    cut: Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 100 }))),
  });

  /** Whether `decode` fails typed (an `SstCorruptedError`) rather than throwing or answering. */
  const failsTyped = <A>(decode: () => Result.Result<A, { readonly _tag: string }>) => {
    try {
      const decoded = decode();
      return Result.isFailure(decoded) && decoded.failure._tag === "SstCorruptedError";
    } catch {
      return false;
    }
  };

  it("refuses a metadata section with any one byte changed, or cut anywhere, with a typed error", () => {
    assertProperty(
      damageArbitrary,
      ({ writes, position, flip, cut }) => {
        const rows = rowsFromWrites(writes);
        if (rows.length === 0) return true;
        const { bytes, info } = buildSst(sortRows(rows));
        const meta = bytes.slice(info.metaOffset);
        const changed = meta.slice();
        const at = position % changed.length;
        changed[at] = (changed[at] ?? 0) ^ flip;
        const truncated = meta.slice(0, Math.floor((meta.length * cut) / 101));
        return (
          failsTyped(() => decodeSstMeta(SST_ID, changed)) &&
          failsTyped(() => decodeSstMeta(SST_ID, truncated))
        );
      },
      { runs: 400 },
    );
  });

  it("refuses a data block with any one byte changed, or cut anywhere, with a typed error", () => {
    assertProperty(
      damageArbitrary,
      ({ writes, position, flip, cut }) => {
        const rows = rowsFromWrites(writes);
        if (rows.length === 0) return true;
        const { bytes, info } = buildSst(sortRows(rows));
        const meta = openSst(bytes, info.metaOffset);
        const entry = meta.index[position % meta.index.length];
        if (entry === undefined) return false;
        const block = bytes.slice(entry.offset, entry.offset + entry.length);
        const changed = block.slice();
        const at = position % changed.length;
        changed[at] = (changed[at] ?? 0) ^ flip;
        const truncated = block.slice(0, Math.floor((block.length * cut) / 101));
        const at0 = { sstId: SST_ID, offset: entry.offset };
        return (
          failsTyped(() => decodeDataBlock(changed, at0)) &&
          failsTyped(() => decodeDataBlock(truncated, at0))
        );
      },
      { runs: 400 },
    );
  });
});

describe("groupBlockReads", () => {
  it("covers exactly the given blocks in order with contiguous reads within the size bound", () => {
    const result = Effect.runSync(
      Arbitrary.checkEffect(
        Arbitrary.all({
          sizes: Arbitrary.array(smallSize, { minLength: 1, maxLength: 60 }),
          // Up to several blocks per read, so merging across a gap would be generated.
          maxBytes: Arbitrary.schema(
            Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 512 })),
          ),
          gapAt: Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 60 }))),
        }),
        ({ sizes, maxBytes, gapAt }) => {
          // Blocks laid out end to end, except one gap (as an SST's blocks are, minus what a view skips).
          let offset = 0;
          const index = sizes.map((length, position) => {
            if (position === gapAt) offset += 7;
            const entry = {
              offset,
              length,
              firstKey: Uint8Array.of(position),
              lastKey: Uint8Array.of(position),
            };
            offset += length;
            return entry;
          });
          const wanted = index
            .map((_, position) => position)
            .filter((position) => position % 5 !== 3);
          const reads = groupBlockReads(index, wanted, maxBytes);
          const covered = reads.flatMap((read) => read.blocks);
          const contiguous = reads.every((read) =>
            read.blocks.every((block, position) => {
              const entry = index[block];
              if (entry === undefined) return false;
              const previous = position === 0 ? undefined : index[read.blocks[position - 1] ?? -1];
              return previous === undefined || previous.offset + previous.length === entry.offset;
            }),
          );
          const bounded = reads.every(
            (read) => read.blocks.length === 1 || read.length <= maxBytes,
          );
          const spans = reads.every((read) => {
            const first = index[read.blocks[0] ?? -1];
            const last = index[read.blocks[read.blocks.length - 1] ?? -1];
            return (
              first !== undefined &&
              last !== undefined &&
              read.offset === first.offset &&
              read.length === last.offset + last.length - first.offset
            );
          });
          return Effect.succeed(
            covered.join(",") === wanted.join(",") && contiguous && bounded && spans,
          );
        },
      ),
    );
    const failure = Arbitrary.formatCheckFailure(result);
    if (failure !== undefined) throw new Error(failure);
  });
});
