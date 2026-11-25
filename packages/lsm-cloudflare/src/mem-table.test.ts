import {
  type CollapsedWrite,
  FULL_KEY_RANGE,
  type Row,
  rowOrder,
  SeqNum,
  Timestamp,
} from "@cf-slatedb/lsm-core";
import { Arbitrary, Effect, Option, Schema, Stream } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { MemTable, memTableTestLayer, type MemTableCursor } from "./mem-table.ts";

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
        const streamed = yield* Stream.runCollect(memTable.rowsUpTo(SeqNum.make(batches.length)));
        // Paginated scan with a small page, at a snapshot in the middle.
        const snapshot = SeqNum.make(Math.ceil(batches.length / 2));
        const scanned: Array<Row> = [];
        let cursor = Option.none<MemTableCursor>();
        for (;;) {
          const page = yield* memTable.scan(FULL_KEY_RANGE, snapshot, cursor, 3);
          scanned.push(...page);
          const last = page[page.length - 1];
          if (page.length < 3 || last === undefined) break;
          cursor = Option.some({ key: last.key, seq: last.seq });
        }
        const firstKey = all[0]?.key ?? new Uint8Array();
        const versions = yield* memTable.versions(firstKey, SeqNum.make(batches.length));
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

  it("truncates exactly the rows at or below a cut and records it as flushed", async () => {
    await check((batches) =>
      Effect.gen(function* () {
        const memTable = yield* MemTable;
        const expected = yield* commitAll(batches);
        if (Option.isNone(expected)) return false;
        const cut = SeqNum.make(Math.floor(batches.length / 2));
        yield* memTable.truncate(cut);
        const remaining = yield* Stream.runCollect(memTable.rowsUpTo(SeqNum.make(batches.length)));
        const stats = yield* memTable.stats;
        const meta = yield* memTable.meta;
        const bySeq = yield* memTable.rowsBySeq(cut, SeqNum.make(batches.length), 10_000);
        const want = expected.value.filter((row) => row.seq > cut);
        return (
          remaining.map(rowSignature).join("|") === want.map(rowSignature).join("|") &&
          stats.rowCount === want.length &&
          bySeq.length === want.length &&
          bySeq.every((row, index) => index === 0 || (bySeq[index - 1]?.seq ?? 0) <= row.seq) &&
          meta.flushedSeq === cut
        );
      }),
    );
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
});
