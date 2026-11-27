import {
  type CollapsedWrite,
  FULL_KEY_RANGE,
  ManifestId,
  type Row,
  rowOrder,
  RowValue,
  SeqNum,
  Timestamp,
} from "@shaledb/lsm-core";
import { Arbitrary, Effect, Layer, Option, Schema, Stream } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  MAX_MEMTABLE_PAGE_BYTES,
  MEMTABLE_FLUSH_PAGE_QUERIES,
  MemTable,
  memTableLayerWithoutDependencies,
  memTableTestLayer,
  type MemTableCursor,
} from "./mem-table.ts";
import { WriterSqlStorage, writerSqlStorageTestLayer } from "./writer-sql-storage.ts";

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

/** Batches of writes to a few keys (byte strings over 0, 1, 255 of length 0–2). */
const batchesArbitrary = Arbitrary.array(
  Arbitrary.array(
    Arbitrary.all({
      key: Arbitrary.array(smallInt(0, 2), { maxLength: 2 }),
      kind: smallInt(0, 2),
      value: smallInt(0, 255),
      ttl: smallInt(0, 2),
    }),
    { minLength: 1, maxLength: 5 },
  ),
  { minLength: 1, maxLength: 25 },
).pipe(
  Arbitrary.map((batches) =>
    batches.map((batch) => {
      // One row per key per batch, as the writer collapses batches.
      const byKey = new Map<string, CollapsedWrite>();
      for (const write of batch) {
        const key = Uint8Array.from(write.key.map((symbol) => [0, 1, 255][symbol] ?? 0));
        const value =
          write.kind === 0
            ? { _tag: "Value" as const, bytes: Uint8Array.of(write.value) }
            : write.kind === 1
              ? { _tag: "Merge" as const, operand: Uint8Array.of(write.value) }
              : { _tag: "Tombstone" as const };
        byKey.set(key.join(","), {
          key,
          value,
          ttlMillis:
            write.ttl === 0 || write.kind === 2 ? Option.none() : Option.some(write.ttl * 100),
        });
      }
      return [...byKey.values()];
    }),
  ),
);

const rowSignature = (row: Row) =>
  `${row.key.join(".")}@${row.seq}:${row.value._tag}:${row.value._tag === "Value" ? row.value.bytes.join(".") : row.value._tag === "Merge" ? row.value.operand.join(".") : ""}:${Option.getOrNull(row.expireTs)}`;

const check = async (
  property: (
    batches: ReadonlyArray<ReadonlyArray<CollapsedWrite>>,
  ) => Effect.Effect<boolean, unknown, MemTable>,
) => {
  const result = await Effect.runPromise(
    Arbitrary.checkEffect(
      batchesArbitrary,
      (batches) => property(batches).pipe(Effect.provide(memTableTestLayer)),
      { runs: 100 },
    ),
  );
  const failure = Arbitrary.formatCheckFailure(result);
  if (failure !== undefined) throw new Error(failure);
};

/** Commits batches at createTs = 1000 + i and returns the rows they should hold, in LSM order. */
const commitAll = (batches: ReadonlyArray<ReadonlyArray<CollapsedWrite>>) =>
  Effect.gen(function* () {
    const memTable = yield* MemTable;
    const expected: Array<Row> = [];
    for (const [index, batch] of batches.entries()) {
      const createTs = Timestamp.make(1000 + index);
      const seq = yield* memTable.commit(batch, createTs);
      if (seq !== index + 1) return Option.none<Array<Row>>();
      for (const write of batch) {
        expected.push({
          key: write.key,
          seq,
          value: write.value,
          createTs: Option.some(createTs),
          expireTs: Option.map(write.ttlMillis, (ttl) => Timestamp.make(createTs + ttl)),
        });
      }
    }
    return Option.some(expected.sort(rowOrder));
  });

describe("MemTable on SQLite", () => {
  it("assigns consecutive sequence numbers and reads back every version in LSM order", async () => {
    await check((batches) =>
      Effect.gen(function* () {
        const memTable = yield* MemTable;
        const expected = yield* commitAll(batches);
        if (Option.isNone(expected)) return false;
        const all = expected.value;
        const streamed = yield* Stream.runCollect(
          memTable.rowsUpTo(SeqNum.make(0), SeqNum.make(batches.length)),
        );
        // Paginated scan with a small page, at a snapshot in the middle.
        const snapshot = SeqNum.make(Math.ceil(batches.length / 2));
        const scanned: Array<Row> = [];
        let cursor = Option.none<MemTableCursor>();
        for (;;) {
          const { rows: page, complete } = yield* memTable.scan(
            FULL_KEY_RANGE,
            snapshot,
            SeqNum.make(0),
            cursor,
            3,
          );
          scanned.push(...page);
          const last = page[page.length - 1];
          if (complete || last === undefined) break;
          cursor = Option.some({ key: last.key, seq: last.seq });
        }
        const firstKey = all[0]?.key ?? new Uint8Array();
        const versions = yield* memTable.versions(
          firstKey,
          SeqNum.make(batches.length),
          SeqNum.make(0),
        );
        const meta = yield* memTable.meta;
        return (
          streamed.map(rowSignature).join("|") === all.map(rowSignature).join("|") &&
          scanned.map(rowSignature).join("|") ===
            all
              .filter((row) => row.seq <= snapshot)
              .map(rowSignature)
              .join("|") &&
          versions.map(rowSignature).join("|") ===
            all
              .filter((row) => row.key.join(",") === firstKey.join(","))
              .map(rowSignature)
              .join("|") &&
          meta.lastSeq === batches.length &&
          meta.clockTick === 1000 + batches.length - 1
        );
      }),
    );
  });

  it("records a cut as flushed without deleting rows, and the Latest queries see only rows above it", async () => {
    await check((batches) =>
      Effect.gen(function* () {
        const memTable = yield* MemTable;
        const expected = yield* commitAll(batches);
        if (Option.isNone(expected)) return false;
        const cut = SeqNum.make(Math.floor(batches.length / 2));
        const last = SeqNum.make(batches.length);
        yield* memTable.markFlushed(cut, Option.some(ManifestId.make(5)), 1_000);
        const meta = yield* memTable.meta;
        const unflushed = yield* Stream.runCollect(memTable.rowsUpTo(meta.flushedSeq, last));
        const kept = yield* Stream.runCollect(memTable.rowsUpTo(meta.retainedFromSeq, last));
        const stats = yield* memTable.stats;
        const { rows: bySeq } = yield* memTable.rowsBySeq(cut, last, 10_000);
        const above = expected.value.filter((row) => row.seq > cut);
        return (
          unflushed.map(rowSignature).join("|") === above.map(rowSignature).join("|") &&
          // Flushed rows stay for transactions until a trim (D58).
          kept.length === expected.value.length &&
          stats.rowCount === above.length &&
          bySeq.length === above.length &&
          bySeq.every((row, index) => index === 0 || (bySeq[index - 1]?.seq ?? 0) <= row.seq) &&
          meta.flushedSeq === cut &&
          meta.retainedFromSeq === 0 &&
          Option.contains(meta.flushedManifestId, ManifestId.make(5))
        );
      }),
    );
  });

  it("trims flushed rows past retention or the byte bound, never the newest flush or unflushed rows", async () => {
    const outcome = await Effect.gen(function* () {
      const memTable = yield* MemTable;
      const row = (n: number) => ({
        key: Uint8Array.of(n),
        value: RowValue.Value({ bytes: new Uint8Array(100) }),
        ttlMillis: Option.none(),
      });
      // Three flushes at 0 s, 10 s, and 20 s, then one unflushed row.
      for (const [index, at] of [0, 10_000, 20_000].entries()) {
        yield* memTable.commit([row(index)], Timestamp.make(at));
        yield* memTable.markFlushed(
          SeqNum.make(index + 1),
          Option.some(ManifestId.make(index + 2)),
          at,
        );
      }
      yield* memTable.commit([row(9)], Timestamp.make(21_000));
      const within = yield* memTable.trim({
        nowMs: 21_000,
        retentionMs: 15_000,
        maxRetainedBytes: 10_000,
      });
      const aged = yield* memTable.trim({
        nowMs: 21_000,
        retentionMs: 5_000,
        maxRetainedBytes: 10_000,
      });
      const all = yield* memTable.trim({ nowMs: 99_000, retentionMs: 0, maxRetainedBytes: 0 });
      const meta = yield* memTable.meta;
      const left = yield* Stream.runCollect(memTable.rowsUpTo(SeqNum.make(0), SeqNum.make(4)));
      // A writer from before D58 deleted flushed rows: its horizon reads as flushedSeq.
      return { within, aged, all, retained: meta.retainedFromSeq, left: left.map((r) => r.seq) };
    }).pipe(Effect.provide(memTableTestLayer), Effect.runPromise);
    // 15 s keeps the flushes at 10 s and 20 s; 5 s keeps only 20 s; nothing drops the newest flush.
    expect(outcome).toEqual({ within: 1, aged: 2, all: 2, retained: 2, left: [3, 4] });
  });

  it("trims a flush exactly retentionMs old, and keeps rows exactly at maxRetainedBytes (D127)", async () => {
    // Three flushed rows of 1 + 100 + 32 bytes (key, value, overhead) at 0 s, 10 s, and 20 s.
    const trimmedTo = (retention: {
      nowMs: number;
      retentionMs: number;
      maxRetainedBytes: number;
    }) =>
      Effect.gen(function* () {
        const memTable = yield* MemTable;
        for (const [index, at] of [0, 10_000, 20_000].entries()) {
          yield* memTable.commit(
            [
              {
                key: Uint8Array.of(index),
                value: RowValue.Value({ bytes: new Uint8Array(100) }),
                ttlMillis: Option.none(),
              },
            ],
            Timestamp.make(at),
          );
          yield* memTable.markFlushed(
            SeqNum.make(index + 1),
            Option.some(ManifestId.make(index + 2)),
            at,
          );
        }
        return yield* memTable.trim(retention);
      }).pipe(Effect.provide(memTableTestLayer), Effect.runPromise);
    const long = 1_000_000;
    expect([
      // The flush at 10 s is exactly 10 s old at 20 s: trimmed, with the one before it.
      await trimmedTo({ nowMs: 20_000, retentionMs: 10_000, maxRetainedBytes: long }),
      await trimmedTo({ nowMs: 19_999, retentionMs: 10_000, maxRetainedBytes: long }),
      // 399 bytes retained: a bound of 399 keeps them all, 398 drops the oldest flush.
      await trimmedTo({ nowMs: 20_000, retentionMs: long, maxRetainedBytes: 399 }),
      await trimmedTo({ nowMs: 20_000, retentionMs: long, maxRetainedBytes: 398 }),
    ]).toEqual([2, 1, 0, 1]);
  });

  it("seeds only upward, and a stale smaller flush cut never moves the flushed manifest back (D127)", async () => {
    const outcome = await Effect.gen(function* () {
      const memTable = yield* MemTable;
      yield* memTable.seed({ seq: SeqNum.make(10), clockTick: Timestamp.make(500) });
      // A lower floor (an older manifest) changes nothing: sequence numbers are never reused.
      yield* memTable.seed({ seq: SeqNum.make(4), clockTick: Timestamp.make(100) });
      const seeded = yield* memTable.meta;
      yield* memTable.markFlushed(SeqNum.make(12), Option.some(ManifestId.make(7)), 1_000);
      // A flush cut below the recorded one (a late, older flush) is ignored whole.
      yield* memTable.markFlushed(SeqNum.make(11), Option.some(ManifestId.make(5)), 2_000);
      const flushed = yield* memTable.meta;
      return {
        seeded: [seeded.lastSeq, seeded.flushedSeq, seeded.clockTick],
        flushed: [flushed.flushedSeq, Option.getOrNull(flushed.flushedManifestId)],
      };
    }).pipe(Effect.provide(memTableTestLayer), Effect.runPromise);
    expect(outcome).toEqual({ seeded: [10, 10, 500], flushed: [12, 7] });
  });

  it("rolls back a failed commit, keeping the sequence number unassigned", async () => {
    const outcome = await Effect.gen(function* () {
      const memTable = yield* MemTable;
      yield* memTable.commit(
        [{ key: Uint8Array.of(1), value: { _tag: "Tombstone" }, ttlMillis: Option.none() }],
        Timestamp.make(1),
      );
      // A duplicate key within one batch violates the primary key: the whole batch rolls back.
      const duplicate = {
        key: Uint8Array.of(2),
        value: { _tag: "Tombstone" as const },
        ttlMillis: Option.none<number>(),
      };
      const failure = yield* Effect.flip(
        memTable.commit([duplicate, duplicate], Timestamp.make(2)),
      );
      const meta = yield* memTable.meta;
      const stats = yield* memTable.stats;
      return { failure: failure.reason, lastSeq: meta.lastSeq, rows: stats.rowCount };
    }).pipe(Effect.provide(memTableTestLayer), Effect.runPromise);
    expect(outcome).toEqual({ failure: "StorageUnavailable", lastSeq: 1, rows: 1 });
  });

  it("streams a flush by primary key, never by sorting every row on each page (D61)", async () => {
    const plans = await Effect.gen(function* () {
      yield* MemTable;
      const sql = yield* WriterSqlStorage;
      const explain = (query: string, bindings: ReadonlyArray<number>) =>
        Effect.map(sql.exec(`EXPLAIN QUERY PLAN ${query}`, ...bindings), (rows) =>
          rows.map((row) => String(row["detail"])).join(" ; "),
        );
      return {
        first: yield* explain(MEMTABLE_FLUSH_PAGE_QUERIES.first, [0, 10, 1000]),
        next: yield* explain(MEMTABLE_FLUSH_PAGE_QUERIES.next, [0, 0, 5, 0, 10, 1000]),
      };
    }).pipe(
      Effect.provide(
        memTableLayerWithoutDependencies.pipe(Layer.provideMerge(writerSqlStorageTestLayer)),
      ),
      Effect.runPromise,
    );
    for (const plan of [plans.first, plans.next]) expect(plan).not.toContain("TEMP B-TREE");
    expect(plans.next).toContain("PRIMARY KEY");
  });
});

describe("MemTable pages are bounded by bytes (D93)", () => {
  const MiB = 1024 * 1024;
  /** One batch per row: `count` puts of `valueBytes` each, keys 0..count-1. */
  const commitLarge = (count: number, valueBytes: number, rowsPerBatch = 1) =>
    Effect.gen(function* () {
      const memTable = yield* MemTable;
      for (let first = 0; first < count; first += rowsPerBatch) {
        const writes: Array<CollapsedWrite> = [];
        for (let index = first; index < Math.min(count, first + rowsPerBatch); index++) {
          writes.push({
            key: Uint8Array.of(index >> 8, index & 0xff),
            value: RowValue.Value({ bytes: new Uint8Array(valueBytes) }),
            ttlMillis: Option.none(),
          });
        }
        yield* memTable.commit(writes, Timestamp.make(0));
      }
    });
  const bytesOf = (rows: ReadonlyArray<Row>) =>
    rows.reduce(
      (sum, row) =>
        sum + row.key.length + (row.value._tag === "Value" ? row.value.bytes.length : 0),
      0,
    );

  it("cuts a scan page and a change feed page at the byte bound, and a flush streams every row", async () => {
    const outcome = await Effect.gen(function* () {
      const memTable = yield* MemTable;
      // 20 rows of 1 MiB: 20 MiB, against an 8 MiB page.
      yield* commitLarge(20, MiB);
      const scanPage = yield* memTable.scan(
        FULL_KEY_RANGE,
        SeqNum.make(20),
        SeqNum.make(0),
        Option.none(),
        1000,
      );
      const feedPage = yield* memTable.rowsBySeq(SeqNum.make(0), SeqNum.make(20), 1000);
      const flushed = yield* Stream.runCollect(memTable.rowsUpTo(SeqNum.make(0), SeqNum.make(20)));
      return {
        scan: [scanPage.rows.length, scanPage.complete, bytesOf(scanPage.rows)],
        feed: [feedPage.rows.length, feedPage.complete, bytesOf(feedPage.rows)],
        flushed: flushed.length,
      };
    }).pipe(Effect.provide(memTableTestLayer), Effect.runPromise);
    expect(outcome.scan[0]).toBe(7);
    expect(outcome.scan[1]).toBe(false);
    expect(outcome.scan[2]).toBeLessThanOrEqual(MAX_MEMTABLE_PAGE_BYTES);
    expect(outcome.feed).toEqual(outcome.scan);
    expect(outcome.flushed).toBe(20);
  });

  it("cuts a change feed page only between batches, and returns one batch larger than the page whole", async () => {
    const outcome = await Effect.gen(function* () {
      const memTable = yield* MemTable;
      // Batches of 3 rows of 1 MiB; the 8 MiB bound falls inside the third batch.
      yield* commitLarge(12, MiB, 3);
      const byBytes = yield* memTable.rowsBySeq(SeqNum.make(0), SeqNum.make(4), 1000);
      // A row limit of 2 falls inside the first batch of 3: the page is that whole batch.
      const byRows = yield* memTable.rowsBySeq(SeqNum.make(0), SeqNum.make(4), 2);
      return {
        byBytes: [byBytes.rows.map((row) => row.seq), byBytes.complete],
        byRows: [byRows.rows.map((row) => row.seq), byRows.complete],
      };
    }).pipe(Effect.provide(memTableTestLayer), Effect.runPromise);
    expect(outcome).toEqual({
      byBytes: [[1, 1, 1, 2, 2, 2], false],
      byRows: [[1, 1, 1], false],
    });
  });
});

describe("the memtable's stored row kinds (D122)", () => {
  // A writer's SQLite outlives a deploy: the kind codes are a stored format, read back by every
  // later version (a rollback or a roll-forward), so they never change.
  it("stores a value as 0, a merge operand as 1, and a tombstone as 2", async () => {
    const kinds = await Effect.gen(function* () {
      const memTable = yield* MemTable;
      const sql = yield* WriterSqlStorage;
      yield* memTable.commit(
        [
          {
            key: Uint8Array.of(1),
            value: RowValue.Value({ bytes: Uint8Array.of(9) }),
            ttlMillis: Option.none(),
          },
          {
            key: Uint8Array.of(2),
            value: RowValue.Merge({ operand: Uint8Array.of(9) }),
            ttlMillis: Option.none(),
          },
          { key: Uint8Array.of(3), value: RowValue.Tombstone(), ttlMillis: Option.none() },
        ],
        Timestamp.make(1_000),
      );
      const rows = yield* sql.exec("SELECT key, kind FROM memtable_rows ORDER BY key");
      return rows.map((row) => row["kind"]);
    }).pipe(
      Effect.provide(
        memTableLayerWithoutDependencies.pipe(Layer.provideMerge(writerSqlStorageTestLayer)),
      ),
      Effect.runPromise,
    );
    expect(kinds).toEqual([0, 1, 2]);
  });
});
