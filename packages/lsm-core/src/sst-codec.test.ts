import { Arbitrary, Bytes, Option, Result, Schema } from "effect";
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
import type { Row } from "./row.ts";
import {
  blocksOverlappingRange,
  decodeSstMeta,
  firstBlockForKey,
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
