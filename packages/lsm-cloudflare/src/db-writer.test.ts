import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  type KeyRange,
  type Row,
  SeqNum,
  type WriteBatch,
} from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, Option, Ref } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { backgroundWorkTestLayer } from "./background-work.ts";
import { CompactionTrigger, compactionTriggerTestLayer } from "./compaction-trigger.ts";
import { DbWriter, dbWriterLayerWithoutDependencies, dbWriterTestLayer } from "./db-writer.ts";
import { lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { manifestRows } from "./lsm-test-support.ts";
import { MemTable, memTableLayerWithoutDependencies } from "./mem-table.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore, manifestStoreTestLayer } from "./manifest-store.ts";
import { ObjectStore } from "./object-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";
import { DurableObjectAlarm, durableObjectAlarmTestLayer } from "./durable-object-alarm.ts";
import { WriterSqlStorage, writerSqlStorageTestLayer } from "./writer-sql-storage.ts";

const DB = DatabaseId.make("orders");
const bytes = (text: string) => new TextEncoder().encode(text);
const text = (value: Uint8Array) => new TextDecoder().decode(value);

const put = (key: string, value: string): WriteBatch => ({
  operations: [{ _tag: "Put", key: bytes(key), value: bytes(value), ttlMillis: Option.none() }],
});

const createDatabase = (
  options: { keyRange?: Option.Option<KeyRange>; changelog?: boolean } = {},
) =>
  Effect.gen(function* () {
    const store = yield* ManifestStore;
    yield* store.initialize(
      initialManifest(DB, {
        keyRange: options.keyRange ?? Option.none(),
        databaseOptions: {
          ...DEFAULT_DATABASE_OPTIONS,
          changelog:
            options.changelog === true ? Option.some({ retentionMs: 60_000 }) : Option.none(),
        },
      }),
    );
  });

/** What the newest manifest makes durable: each key's newest value, as text. */
const durableValues = Effect.gen(function* () {
  const latest = yield* (yield* ManifestReader).loadLatest(DB);
  const rows = yield* manifestRows(latest.manifest);
  const values = new Map<string, string>();
  for (const row of rows) {
    const key = text(row.key);
    if (!values.has(key))
      values.set(key, row.value._tag === "Value" ? text(row.value.bytes) : `<${row.value._tag}>`);
  }
  return { values, manifest: latest.manifest };
});

const run = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    | DbWriter
    | MemTable
    | ManifestStore
    | ManifestReader
    | ObjectStore
    | DurableObjectAlarm
    | RuntimeContext
  >,
  overrides: Parameters<typeof dbWriterTestLayer>[0] = {},
) =>
  effect.pipe(
    Effect.provide(dbWriterTestLayer(overrides)),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
    Effect.runPromise,
  );

describe("DbWriter", () => {
  it("commits batches with increasing sequence numbers and answers Latest probes from the memtable", async () => {
    const result = await run(
      Effect.gen(function* () {
        yield* createDatabase();
        const writer = yield* DbWriter;
        const first = yield* writer.write(DB, put("a", "1"));
        const second = yield* writer.write(DB, {
          operations: [
            { _tag: "Put", key: bytes("a"), value: bytes("2"), ttlMillis: Option.none() },
            { _tag: "Delete", key: bytes("b") },
          ],
        });
        const probe = yield* writer.probe(DB, bytes("a"));
        const status = yield* writer.status(DB);
        return { first, second, probe, status };
      }),
    );
    expect([result.first.seq, result.second.seq]).toEqual([1, 2]);
    expect(result.second.createTs).toBeGreaterThanOrEqual(result.first.createTs);
    expect(
      result.probe.versions.map((row: Row) => [
        row.seq,
        row.value._tag === "Value" ? text(row.value.bytes) : "",
      ]),
    ).toEqual([
      [2, "2"],
      [1, "1"],
    ]);
    expect([result.probe.snapshotSeq, result.probe.flushedSeq]).toEqual([2, 0]);
    expect([result.status.writerEpoch, result.status.unflushedRows]).toEqual([1, 3]);
  });

  it("flushes to an L0 SST, commits lastL0Seq, and only then empties the memtable", async () => {
    const result = await run(
      Effect.gen(function* () {
        yield* createDatabase();
        const writer = yield* DbWriter;
        for (const [index, key] of ["c", "a", "b", "a"].entries())
          yield* writer.write(DB, put(key, `v${index}`));
        const flushed = yield* writer.flush(DB);
        const durable = yield* durableValues;
        const status = yield* writer.status(DB);
        const probe = yield* writer.probe(DB, bytes("a"));
        return { flushed, durable, status, probe };
      }),
    );
    expect(result.flushed).toBe(4);
    expect(Object.fromEntries(result.durable.values)).toEqual({ a: "v3", b: "v2", c: "v0" });
    expect([result.durable.manifest.lastL0Seq, result.durable.manifest.l0.length]).toEqual([4, 1]);
    // "a" was written twice since the last flush: the SST holds its newest version only (D70).
    expect(result.durable.manifest.l0.map((sst) => sst.rowCount)).toEqual([3]);
    expect([result.status.unflushedRows, result.status.flushedSeq]).toEqual([0, 4]);
    expect([result.probe.versions.length, result.probe.flushedSeq]).toEqual([0, 4]);
  });

  it("refuses writes outside the database's key range", async () => {
    const failure = await run(
      Effect.gen(function* () {
        yield* createDatabase({ keyRange: Option.some({ start: bytes("m"), end: Option.none() }) });
        const writer = yield* DbWriter;
        yield* writer.write(DB, put("p", "ok"));
        return yield* Effect.flip(writer.write(DB, put("a", "no")));
      }),
    );
    expect(failure._tag).toBe("KeyOutOfRangeError");
  });

  it("applies backpressure above maxUnflushedBytes, and a flush clears it", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase();
        const writer = yield* DbWriter;
        yield* writer.write(DB, put("a", "x".repeat(600)));
        yield* writer.write(DB, put("b", "x".repeat(600)));
        const stalled = yield* Effect.flip(writer.write(DB, put("c", "x")));
        yield* writer.flush(DB);
        const after = yield* writer.write(DB, put("c", "x"));
        return { stalled, after: after.seq };
      }),
      { maxUnflushedBytes: 1000 },
    );
    expect(outcome.stalled._tag).toBe("WriteStalledError");
    expect(outcome.after).toBe(3);
  });

  it("schedules a flush at maxL0Age and flushes from the alarm once it is due", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase();
        const writer = yield* DbWriter;
        const alarm = yield* DurableObjectAlarm;
        yield* TestClock.setTime(10_000);
        yield* writer.write(DB, put("a", "1"));
        const scheduled = yield* alarm.scheduled;
        yield* writer.runAlarm;
        const early = yield* writer.status(DB);
        yield* TestClock.adjust("61 seconds");
        yield* writer.runAlarm;
        const late = yield* writer.status(DB);
        return { scheduled, early: early.flushedSeq, late: late.flushedSeq };
      }),
    );
    expect(outcome.scheduled).toEqual(Option.some(10_000 + 60_000));
    expect([outcome.early, outcome.late]).toEqual([0, 1]);
  });

  it("seals: flushes every acknowledged write, then refuses writes with the successors", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase();
        const writer = yield* DbWriter;
        yield* writer.write(DB, put("a", "1"));
        yield* writer.write(DB, put("b", "2"));
        const successors = [DatabaseId.make("orders-a"), DatabaseId.make("orders-b")];
        const beforeSeal = (yield* (yield* ManifestReader).loadLatest(DB)).id;
        const holds = yield* writer.seal(DB, successors, true);
        const afterSeal = yield* (yield* ManifestReader).loadLatest(DB);
        const again = yield* writer.seal(DB, successors, false);
        const refused = yield* Effect.flip(writer.write(DB, put("c", "3")));
        const durable = yield* durableValues;
        return { again, refused, durable, beforeSeal, holds, afterSeal };
      }),
    );
    // The final flush, the seal, and both holds are one manifest commit, pinning that manifest.
    expect(outcome.afterSeal.id).toBe(outcome.beforeSeal + 1);
    expect(outcome.holds.successorHolds.map((hold) => hold.successor)).toEqual([
      "orders-a",
      "orders-b",
    ]);
    expect(
      outcome.afterSeal.manifest.checkpoints.map((checkpoint) => checkpoint.manifestId),
    ).toEqual([outcome.afterSeal.id, outcome.afterSeal.id]);
    expect(outcome.refused._tag).toBe("DatabaseSealedError");
    expect(outcome.refused._tag === "DatabaseSealedError" && outcome.refused.successors).toEqual([
      "orders-a",
      "orders-b",
    ]);
    expect(Object.fromEntries(outcome.durable.values)).toEqual({ a: "1", b: "2" });
    expect(outcome.durable.manifest.state._tag).toBe("Sealed");
    expect(outcome.again).toEqual({ flushedSeq: 2, successorHolds: [] });
  });

  it("exports a changelog SST in sequence order with each flush when the database enables it", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase({ changelog: true });
        const writer = yield* DbWriter;
        yield* writer.write(DB, put("b", "1"));
        yield* writer.write(DB, put("a", "2"));
        const changes = yield* writer.changes(DB, SeqNum.make(0), 10);
        yield* writer.flush(DB);
        const durable = yield* durableValues;
        return { changes, changelog: durable.manifest.changelog };
      }),
    );
    expect(outcome.changes.rows.map((row) => [row.seq, text(row.key)])).toEqual([
      [1, "b"],
      [2, "a"],
    ]);
    expect(outcome.changelog.map((entry) => [entry.firstSeq, entry.lastSeq])).toEqual([[1, 2]]);
  });
});

describe("DbWriter fencing and crash recovery", () => {
  it("a stale writer cannot commit after a newer writer starts, and its SST stays unreferenced", async () => {
    const outcome = await Effect.gen(function* () {
      yield* createDatabase();
      const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
      const newWriter = () =>
        Layer.build(
          dbWriterLayerWithoutDependencies.pipe(
            Layer.provide(sstStoreLayerWithoutDependencies),
            Layer.provide(
              memTableLayerWithoutDependencies.pipe(Layer.provide(writerSqlStorageTestLayer)),
            ),
            Layer.provide([
              durableObjectAlarmTestLayer,
              compactionTriggerTestLayer,
              backgroundWorkTestLayer,
              lsmConfigurationTestLayer(),
            ]),
            Layer.provide(Layer.succeedContext(shared)),
          ),
        ).pipe(Effect.map((context) => Context.get(context, DbWriter)));
      const stale = yield* newWriter();
      yield* stale.write(DB, put("a", "stale"));
      const fresh = yield* newWriter();
      yield* fresh.write(DB, put("b", "fresh"));
      const staleFlush = yield* Effect.flip(stale.flush(DB));
      const staleWrite = yield* Effect.flip(stale.write(DB, put("c", "late")));
      yield* fresh.flush(DB);
      const durable = yield* durableValues;
      const objects = yield* ObjectStore;
      const stored = yield* objects.list("db/orders/compacted/", Option.none());
      return { staleFlush, staleWrite, durable, storedSsts: stored.objects.length };
    }).pipe(
      Effect.scoped,
      Effect.provide(manifestStoreTestLayer),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    expect(outcome.staleFlush._tag).toBe("WriterFencedError");
    expect(outcome.staleWrite._tag).toBe("WriterFencedError");
    expect(Object.fromEntries(outcome.durable.values)).toEqual({ b: "fresh" });
    expect(outcome.durable.manifest.writerEpoch).toBe(2);
    // The stale writer's upload is an orphan: stored, but not in the manifest.
    expect(outcome.storedSsts).toBe(2);
    expect(outcome.durable.manifest.l0.length).toBe(1);
  });

  it("a flush wakes the compactor without waiting for it, and the next flush is not held up", async () => {
    const outcome = await Effect.gen(function* () {
      yield* createDatabase();
      const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
      // A compactor that never answers: an idle one takes seconds to answer its first call.
      const woken: Array<DatabaseId> = [];
      const unansweredCompactor = Layer.succeed(
        CompactionTrigger,
        CompactionTrigger.of({
          wake: (databaseId) =>
            Effect.sync(() => woken.push(databaseId)).pipe(Effect.andThen(Effect.never)),
        }),
      );
      const writer = yield* Layer.build(
        dbWriterLayerWithoutDependencies.pipe(
          Layer.provide(sstStoreLayerWithoutDependencies),
          Layer.provide(
            memTableLayerWithoutDependencies.pipe(Layer.provide(writerSqlStorageTestLayer)),
          ),
          Layer.provide([
            durableObjectAlarmTestLayer,
            unansweredCompactor,
            backgroundWorkTestLayer,
            lsmConfigurationTestLayer({ minCompactionSources: 2 }),
          ]),
          Layer.provide(Layer.succeedContext(shared)),
        ),
      ).pipe(Effect.map((context) => Context.get(context, DbWriter)));
      // The second flush leaves two L0 SSTs, enough to compact: it wakes the compactor. The
      // third starts while that wake is still unanswered.
      const flushed: Array<number> = [];
      for (const key of ["a", "b", "c"]) {
        yield* writer.write(DB, put(key, "1"));
        flushed.push(yield* writer.flush(DB).pipe(Effect.timeout("2 seconds")));
      }
      // The wakes run in the background: let them start.
      yield* Effect.yieldNow;
      const { manifest } = yield* (yield* ManifestReader).loadLatest(DB);
      return { flushed, woken, l0: manifest.l0.length };
    }).pipe(
      Effect.scoped,
      Effect.provide(manifestStoreTestLayer),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );
    expect(outcome.flushed).toEqual([1, 2, 3]);
    expect(outcome.l0).toBe(3);
    expect(outcome.woken).toEqual([DB, DB]);
  });

  it("a crash between the manifest commit and the memtable truncate loses and duplicates nothing", async () => {
    const outcome = await Effect.gen(function* () {
      yield* createDatabase();
      const shared = yield* Effect.context<
        ManifestStore | ManifestReader | ObjectStore | WriterSqlStorage
      >();
      const failTruncate = yield* Ref.make(true);
      // A memtable whose first record of a flush "crashes" after the manifest commit.
      const crashingMemTable = Layer.effect(
        MemTable,
        Effect.gen(function* () {
          const real = yield* MemTable;
          return MemTable.of({
            ...real,
            markFlushed: (cut, manifestId, flushedAtMs) =>
              // The start records the fenced manifest's empty cut (0); the flush's is the first above.
              (cut > 0 ? Ref.getAndSet(failTruncate, false) : Effect.succeed(false)).pipe(
                Effect.flatMap((fail) =>
                  fail
                    ? Effect.die(new Error("simulated crash before recording the flush"))
                    : real.markFlushed(cut, manifestId, flushedAtMs),
                ),
              ),
          });
        }),
      ).pipe(Layer.provide(memTableLayerWithoutDependencies));
      const newWriter = <E>(memTable: Layer.Layer<MemTable, E, WriterSqlStorage>) =>
        Layer.build(
          dbWriterLayerWithoutDependencies.pipe(
            Layer.provide(sstStoreLayerWithoutDependencies),
            Layer.provide(memTable),
            Layer.provide([
              durableObjectAlarmTestLayer,
              compactionTriggerTestLayer,
              backgroundWorkTestLayer,
              lsmConfigurationTestLayer(),
            ]),
            Layer.provide(Layer.succeedContext(shared)),
          ),
        ).pipe(Effect.map((context) => Context.get(context, DbWriter)));
      const before = yield* newWriter(crashingMemTable);
      yield* before.write(DB, put("a", "1"));
      yield* before.write(DB, put("b", "2"));
      const crash = yield* Effect.exit(before.flush(DB));
      // The Durable Object restarts on the same SQLite storage.
      const after = yield* newWriter(memTableLayerWithoutDependencies);
      const status = yield* after.status(DB);
      yield* after.write(DB, put("c", "3"));
      yield* after.flush(DB);
      const durable = yield* durableValues;
      return { crash: crash._tag, status, durable };
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.merge(manifestStoreTestLayer, writerSqlStorageTestLayer)),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    expect(outcome.crash).toBe("Failure");
    // The restart dropped the rows already in L0 instead of flushing them again.
    expect([outcome.status.unflushedRows, outcome.status.lastSeq]).toEqual([0, 2]);
    expect(Object.fromEntries(outcome.durable.values)).toEqual({ a: "1", b: "2", c: "3" });
    expect(outcome.durable.manifest.lastL0Seq).toBe(3);
  });
});
