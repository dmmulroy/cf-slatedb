import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  type KeyRange,
  markRemoved,
  type Row,
  SeqNum,
  Timestamp,
  type WriteBatch,
} from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import {
  Clock,
  Context,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Option,
  Ref,
  Result,
  Tracer,
} from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { backgroundWorkTestLayer } from "./background-work.ts";
import { CompactionTrigger, compactionTriggerTestLayer } from "./compaction-trigger.ts";
import {
  DbWriter,
  dbWriterLayerWithoutDependencies,
  dbWriterTestLayer,
  type IDbWriter,
} from "./db-writer.ts";
import { lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { manifestRows } from "./lsm-test-support.ts";
import {
  type IMemTable,
  MemTable,
  MemTableError,
  memTableLayerWithoutDependencies,
} from "./mem-table.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore, manifestStoreTestLayer, ManifestWriteError } from "./manifest-store.ts";
import { ObjectStore } from "./object-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";
import {
  DurableObjectAlarm,
  durableObjectAlarmTestLayer,
  type IDurableObjectAlarm,
} from "./durable-object-alarm.ts";
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

/** What a test of the counter refresh holds: the writer, its memtable, its alarm, and the hooks. */
interface HookedStatsWriter {
  readonly writer: IDbWriter;
  readonly memTable: IMemTable;
  readonly alarm: IDurableObjectAlarm;
  /**
   * Effects run, one per call and in order, right after `MemTable.stats` has scanned and before
   * the writer sees the result. A failing one replaces the scan's result with its failure.
   */
  readonly afterScan: Array<Effect.Effect<void, MemTableError>>;
  /** Effects run, one per call and in order, right after `MemTable.commit` wrote its rows. */
  readonly afterCommit: Array<Effect.Effect<void>>;
  /** Queues a hook that commits `batch` through the writer, as a concurrent request would. */
  readonly commitAfterScan: (batch: WriteBatch) => void;
  /** Effects run, one per call and in order, right before `MemTable.writtenAfter` queries. */
  readonly beforeWrittenAfter: Array<Effect.Effect<void>>;
}

/**
 * Runs `body` against a started writer whose `MemTable.stats` runs the queued `afterScan` hooks:
 * a commit can then land between the scan and the install, which the scheduler otherwise allows
 * only at an unpredictable yield.
 */
const runWithHookedStats = <A, E>(
  body: (
    writer: HookedStatsWriter,
  ) => Effect.Effect<A, E, DbWriter | MemTable | DurableObjectAlarm | RuntimeContext>,
  overrides: Parameters<typeof lsmConfigurationTestLayer>[0] = {},
) =>
  Effect.gen(function* () {
    yield* createDatabase();
    const afterScan: Array<Effect.Effect<void, MemTableError>> = [];
    const afterCommit: Array<Effect.Effect<void>> = [];
    const beforeWrittenAfter: Array<Effect.Effect<void>> = [];
    const hookedMemTable = Layer.effect(
      MemTable,
      Effect.map(MemTable, (memTable) =>
        MemTable.of({
          ...memTable,
          stats: Effect.tap(memTable.stats, () => afterScan.shift() ?? Effect.void),
          commit: (writes, createTs) =>
            Effect.tap(memTable.commit(writes, createTs), () => afterCommit.shift() ?? Effect.void),
          writtenAfter: (keys, ranges, afterSeq) =>
            Effect.andThen(
              Effect.suspend(() => beforeWrittenAfter.shift() ?? Effect.void),
              memTable.writtenAfter(keys, ranges, afterSeq),
            ),
        }),
      ),
    ).pipe(
      Layer.provide(
        memTableLayerWithoutDependencies.pipe(Layer.provide(writerSqlStorageTestLayer)),
      ),
    );
    const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
    const context = yield* Layer.build(
      Layer.provideMerge(
        dbWriterLayerWithoutDependencies.pipe(Layer.provide(sstStoreLayerWithoutDependencies)),
        Layer.mergeAll(
          hookedMemTable,
          durableObjectAlarmTestLayer,
          compactionTriggerTestLayer,
          backgroundWorkTestLayer,
          lsmConfigurationTestLayer(overrides),
        ),
      ).pipe(Layer.provide(Layer.succeedContext(shared))),
    );
    const writer = Context.get(context, DbWriter);
    const memTable = Context.get(context, MemTable);
    const alarm = Context.get(context, DurableObjectAlarm);
    const runtime = yield* Effect.context<RuntimeContext>();
    const commitAfterScan = (batch: WriteBatch) => {
      afterScan.push(
        writer.write(DB, batch).pipe(Effect.asVoid, Effect.orDie, Effect.provide(runtime)),
      );
    };
    return yield* body({
      writer,
      memTable,
      alarm,
      afterScan,
      afterCommit,
      commitAfterScan,
      beforeWrittenAfter,
    }).pipe(Effect.provide(context));
  }).pipe(
    Effect.scoped,
    Effect.provide(manifestStoreTestLayer),
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

  it("names the reason a write stalled on its span, for the health report (D82)", async () => {
    const spans: Array<Tracer.NativeSpan> = [];
    const recordingTracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    await run(
      Effect.gen(function* () {
        yield* createDatabase();
        const writer = yield* DbWriter;
        yield* writer.write(DB, put("a", "x".repeat(600)));
        yield* writer.write(DB, put("b", "x".repeat(600)));
        yield* Effect.flip(writer.write(DB, put("c", "x")));
      }).pipe(Effect.withTracer(recordingTracer)),
      { maxUnflushedBytes: 1000 },
    );
    const reasons = spans
      .filter((span) => span.name === "DbWriter.write")
      .map((span) => span.attributes.get("writer.stall_reason"));
    expect(reasons).toEqual([undefined, undefined, "UnflushedBytes"]);
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

  it("counts unflushed bytes and rows exactly as a scan of the memtable does, across writes and flushes (D74)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase();
        const writer = yield* DbWriter;
        const memTable = yield* MemTable;
        const counted = Effect.gen(function* () {
          const status = yield* writer.status(DB);
          const scanned = yield* memTable.stats;
          return [
            [status.unflushedBytes, status.unflushedRows],
            [scanned.unflushedBytes, scanned.rowCount],
          ];
        });
        // Rows of every kind and of very different sizes: short keys with long values, long keys
        // with none, a delete, a merge operand, a value that expires.
        const batches: ReadonlyArray<WriteBatch> = [
          put("a", "x".repeat(900)),
          put("k".repeat(200), ""),
          {
            operations: [
              { _tag: "Delete", key: bytes("a") },
              { _tag: "Merge", key: bytes("m"), operand: bytes("op"), ttlMillis: Option.none() },
              { _tag: "Put", key: bytes("t"), value: bytes("v"), ttlMillis: Option.some(5_000) },
            ],
          },
        ];
        const afterEach: Array<ReadonlyArray<ReadonlyArray<number>>> = [];
        for (const batch of batches) {
          yield* writer.write(DB, batch);
          afterEach.push(yield* counted);
        }
        yield* writer.flush(DB);
        const afterFlush = yield* counted;
        yield* writer.write(DB, put("z", "y".repeat(50)));
        const afterNextWrite = yield* counted;
        return { afterEach, afterFlush, afterNextWrite };
      }),
    );
    for (const [counters, scan] of [
      ...outcome.afterEach,
      outcome.afterFlush,
      outcome.afterNextWrite,
    ]) {
      expect(counters).toEqual(scan);
    }
    expect(outcome.afterFlush).toEqual([
      [0, 0],
      [0, 0],
    ]);
    expect(outcome.afterNextWrite[0]?.[1]).toBe(1);
  });

  it("an alarm set for the size threshold flushes; one set for age does not fire a flush early or set itself again for now (D74)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase();
        const writer = yield* DbWriter;
        const alarm = yield* DurableObjectAlarm;
        yield* TestClock.setTime(10_000);
        // 1,033 unflushed bytes as SQLite counts them (1 + 1,000 + 32), under the 1,040-byte
        // threshold. The writer's counter once used another formula that counted more, so it
        // set the alarm for "now"; the alarm, scanning 1,033, did not flush and was set for
        // "now" again: a loop of memtable scans until the age limit.
        yield* writer.write(DB, put("a", "x".repeat(1_000)));
        const underThreshold = yield* alarm.scheduled;
        yield* writer.runAlarm;
        const afterEarlyAlarm = {
          flushedSeq: (yield* writer.status(DB)).flushedSeq,
          scheduled: yield* alarm.scheduled,
        };
        // One more small row passes the threshold, and the alarm it is set for flushes.
        yield* writer.write(DB, put("b", "y".repeat(6)));
        const atThreshold = yield* alarm.scheduled;
        yield* writer.runAlarm;
        const afterDueAlarm = (yield* writer.status(DB)).flushedSeq;
        return { underThreshold, afterEarlyAlarm, atThreshold, afterDueAlarm };
      }),
      { l0SstSize: 1_040, maxUnflushedBytes: 100_000 },
    );
    expect(outcome.underThreshold).toEqual(Option.some(10_000 + 60_000));
    expect(outcome.afterEarlyAlarm).toEqual({
      flushedSeq: 0,
      scheduled: Option.some(10_000 + 60_000),
    });
    expect(outcome.atThreshold).toEqual(Option.some(10_000));
    expect(outcome.afterDueAlarm).toBe(2);
  });

  it("a write, a status read, and an alarm that is not due never scan the memtable (D74)", async () => {
    const outcome = await Effect.gen(function* () {
      yield* createDatabase();
      const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
      // SQLite storage that records every statement the writer runs.
      const statements: Array<string> = [];
      const recordingStorage = Layer.effect(
        WriterSqlStorage,
        Effect.map(WriterSqlStorage, (storage) =>
          WriterSqlStorage.of({
            ...storage,
            exec: (query, ...bindings) => {
              statements.push(query);
              return storage.exec(query, ...bindings);
            },
          }),
        ),
      ).pipe(Layer.provide(writerSqlStorageTestLayer));
      const writer = yield* Layer.build(
        dbWriterLayerWithoutDependencies.pipe(
          Layer.provide(sstStoreLayerWithoutDependencies),
          Layer.provide(memTableLayerWithoutDependencies.pipe(Layer.provide(recordingStorage))),
          Layer.provide([
            durableObjectAlarmTestLayer,
            compactionTriggerTestLayer,
            backgroundWorkTestLayer,
            lsmConfigurationTestLayer(),
          ]),
          Layer.provide(Layer.succeedContext(shared)),
        ),
      ).pipe(Effect.map((context) => Context.get(context, DbWriter)));
      // A scan of the memtable is an aggregate over its rows.
      const scans = () => statements.filter((query) => /SUM\(|COUNT\(/.test(query)).length;
      // The first request starts the writer, which reads its counters from SQLite once.
      yield* writer.write(DB, put("a", "1"));
      const atStart = scans();
      yield* writer.write(DB, put("b", "2"));
      const status = yield* writer.status(DB);
      yield* writer.runAlarm;
      const whileIdle = scans() - atStart;
      // A flush reads them again once when it ends, and its trim sums the rows it keeps.
      yield* writer.flush(DB);
      const afterFlush = scans() - atStart;
      return { atStart, whileIdle, afterFlush, rows: status.unflushedRows };
    }).pipe(
      Effect.scoped,
      Effect.provide(manifestStoreTestLayer),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    expect(outcome).toEqual({ atStart: 1, whileIdle: 0, afterFlush: 2, rows: 2 });
  });

  it("a commit that lands while a flush scans for fresh counters is kept in them, and its alarm flushes it (D74)", async () => {
    const outcome = await runWithHookedStats(
      ({ writer, memTable, alarm, afterScan, commitAfterScan }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(10_000);
          yield* writer.write(DB, put("a", "1"));
          // The end of this flush scans SQLite for its counters; the write of `b` commits right
          // after that scan and before its figures are installed.
          commitAfterScan(put("b", "2"));
          const flushedSeq = yield* writer.flush(DB);
          const status = yield* writer.status(DB);
          const scanned = yield* memTable.stats;
          // The Durable Object clears an alarm when it fires; then `b` reaches its age limit.
          yield* alarm.cancel;
          yield* TestClock.setTime(10_000 + 60_000);
          yield* writer.runAlarm;
          return {
            flushedSeq,
            counted: [status.unflushedBytes, status.unflushedRows],
            scanned: [scanned.unflushedBytes, scanned.rowCount],
            afterAlarm: (yield* writer.status(DB)).flushedSeq,
            hooksLeft: afterScan.length,
          };
        }),
    );
    expect(outcome.hooksLeft).toBe(0);
    expect(outcome.flushedSeq).toBe(1);
    expect(outcome.scanned).toEqual([1 + 1 + 32, 1]);
    expect(outcome.counted).toEqual(outcome.scanned);
    expect(outcome.afterAlarm).toBe(2);
  });

  it("a commit that started before a flush's counter scan and ends after it is counted once (D74)", async () => {
    const outcome = await runWithHookedStats(({ writer, memTable, afterScan, afterCommit }) =>
      Effect.gen(function* () {
        yield* writer.write(DB, put("a", "1"));
        // The write of `b` puts its row in SQLite and then waits, so it has started and not ended
        // while the flush that follows writes that row and scans. The write may end only during
        // the flush's second scan: a refresh must not install the first scan meanwhile, or the
        // write's row, already flushed, is counted again when it ends.
        const inserted = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        afterCommit.push(
          Deferred.succeed(inserted, undefined).pipe(Effect.andThen(Deferred.await(release))),
        );
        afterScan.push(Effect.void, Effect.asVoid(Deferred.succeed(release, undefined)));
        const write = yield* Effect.forkChild(writer.write(DB, put("b", "2")));
        yield* Deferred.await(inserted);
        const flushedSeq = yield* writer.flush(DB);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(write);
        const status = yield* writer.status(DB);
        const scanned = yield* memTable.stats;
        return {
          flushedSeq,
          counted: [status.unflushedBytes, status.unflushedRows],
          scanned: [scanned.unflushedBytes, scanned.rowCount],
        };
      }),
    );
    expect(outcome).toEqual({ flushedSeq: 2, counted: [0, 0], scanned: [0, 0] });
  });

  it("a write interrupted after its rows reached SQLite is still counted, so an alarm flushes it (D74)", async () => {
    const outcome = await runWithHookedStats(({ writer, memTable, alarm, afterCommit }) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(10_000);
        // The row of `a` is in SQLite when the request is interrupted, before `commit` returns.
        const inserted = yield* Deferred.make<void>();
        afterCommit.push(
          Deferred.succeed(inserted, undefined).pipe(
            Effect.andThen(Effect.yieldNow),
            Effect.andThen(Effect.yieldNow),
          ),
        );
        const write = yield* Effect.forkChild(writer.write(DB, put("a", "1")));
        yield* Deferred.await(inserted);
        yield* Fiber.interrupt(write);
        const status = yield* writer.status(DB);
        const scanned = yield* memTable.stats;
        return {
          counted: status.unflushedRows,
          inSqlite: scanned.rowCount,
          alarm: yield* alarm.scheduled,
        };
      }),
    );
    expect(outcome).toEqual({ counted: 1, inSqlite: 1, alarm: Option.some(10_000 + 60_000) });
  });

  it("counters a refresh could not settle stay above SQLite until the next flush, which flushes every row (D74)", async () => {
    const outcome = await runWithHookedStats(({ writer, memTable, alarm, commitAfterScan }) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(10_000);
        yield* writer.write(DB, put("a", "1"));
        // Every scan the flush makes is overtaken by a commit, so the refresh gives up.
        for (const key of ["b", "c", "d"]) {
          commitAfterScan(put(key, "2"));
        }
        yield* writer.flush(DB);
        const status = yield* writer.status(DB);
        const scanned = yield* memTable.stats;
        yield* alarm.cancel;
        yield* TestClock.setTime(10_000 + 60_000);
        yield* writer.runAlarm;
        const afterAlarm = yield* writer.status(DB);
        const scannedAfter = yield* memTable.stats;
        return {
          counted: status.unflushedRows,
          scanned: scanned.rowCount,
          afterAlarm: [afterAlarm.flushedSeq, afterAlarm.unflushedRows, scannedAfter.rowCount],
          scheduled: yield* alarm.scheduled,
        };
      }),
    );
    // Above SQLite (they still hold `a`), never below it.
    expect([outcome.counted, outcome.scanned]).toEqual([4, 3]);
    expect(outcome.afterAlarm).toEqual([4, 0, 0]);
    expect(outcome.scheduled).toEqual(Option.none());
  });

  it("an alarm that finds nothing to flush settles stale counters instead of setting itself again (D74)", async () => {
    const outcome = await runWithHookedStats(({ writer, alarm, afterScan }) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(10_000);
        yield* writer.write(DB, put("a", "1"));
        // The flush commits and records `a` flushed, then its counter scan fails.
        afterScan.push(
          Effect.fail(
            new MemTableError({
              operation: "stats",
              reason: "StorageUnavailable",
              message: "Test: the counter scan failed.",
            }),
          ),
        );
        const failedFlush = yield* Effect.result(writer.flush(DB));
        yield* alarm.cancel;
        yield* TestClock.setTime(10_000 + 60_000);
        yield* writer.runAlarm;
        const status = yield* writer.status(DB);
        return {
          failedFlush: failedFlush._tag,
          flushedSeq: status.flushedSeq,
          rows: status.unflushedRows,
          scheduled: yield* alarm.scheduled,
        };
      }),
    );
    expect(outcome).toEqual({
      failedFlush: "Failure",
      flushedSeq: 1,
      rows: 0,
      scheduled: Option.none(),
    });
  });

  it("a ready writer answers requests without starting again (D69)", async () => {
    const started: Array<string> = [];
    const recordingTracer = Tracer.make({
      span: (options) => {
        if (options.name === "DbWriter.start") started.push(options.name);
        return new Tracer.NativeSpan(options);
      },
    });
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase();
        const writer = yield* DbWriter;
        yield* writer.write(DB, put("a", "1"));
        const afterFirst = started.length;
        yield* writer.write(DB, put("b", "2"));
        yield* writer.probe(DB, bytes("a"));
        yield* writer.status(DB);
        return { afterFirst, afterMore: started.length };
      }).pipe(Effect.withTracer(recordingTracer)),
    );
    expect(outcome).toEqual({ afterFirst: 1, afterMore: 1 });
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

describe("DbWriter transaction commit across a trim (D84)", () => {
  it("a trim between the commit's horizon check and its conflict query cannot hide a conflicting write", async () => {
    const outcome = await runWithHookedStats(
      ({ writer, beforeWrittenAfter }) =>
        Effect.gen(function* () {
          yield* writer.write(DB, put("k", "1"));
          yield* writer.flush(DB);
          // The transaction reads k at seq 1; another commit then writes k (seq 2), and it flushes.
          const begin = yield* writer.status(DB);
          yield* writer.write(DB, put("k", "2"));
          yield* writer.flush(DB);
          yield* TestClock.adjust("2 seconds");
          yield* writer.write(DB, put("other", "3"));
          // A flush lands while the commit checks (the flush's R2 response arrives while the
          // commit's fiber yields): its trim deletes flushed rows up to seq 2.
          const runtime = yield* Effect.context<RuntimeContext>();
          beforeWrittenAfter.push(
            writer.flush(DB).pipe(Effect.asVoid, Effect.orDie, Effect.provide(runtime)),
          );
          const committed = yield* Effect.result(
            writer.commitTransaction(DB, {
              readSeq: begin.lastSeq,
              isolation: "Snapshot",
              batch: put("k", "from the transaction"),
              readKeys: [],
              readRanges: [],
            }),
          );
          const after = yield* writer.status(DB);
          return {
            readSeq: begin.lastSeq,
            retainedFromSeq: after.retainedFromSeq,
            committed: Result.isSuccess(committed)
              ? "committed"
              : committed.failure._tag === "TransactionConflictError"
                ? committed.failure.reason
                : committed.failure._tag,
          };
        }),
      { transactionRetentionMs: 1_000 },
    );
    // The trim passed the transaction's read sequence number, so the write at seq 2 can no longer
    // be checked: the commit must be refused, never accepted over it (a lost update).
    expect(outcome.retainedFromSeq).toBeGreaterThan(outcome.readSeq);
    expect(outcome.committed).toBe("SnapshotTooOld");
  });
});

/**
 * Seals with the seal's manifest commit held at `gate`: `"afterCommit"` lets the create land and
 * then never answers (the caller hung up during the read after it), `"beforeCommit"` never sends
 * it. The seal's fiber is then interrupted. With `restart`, the object is evicted at that point
 * and a new writer starts on the same SQLite storage.
 */
const sealInterrupted = (gate: "beforeCommit" | "afterCommit", restart: boolean) =>
  Effect.gen(function* () {
    yield* createDatabase();
    const store = yield* ManifestStore;
    const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
    const reached = yield* Deferred.make<void>();
    const armed = yield* Ref.make(false);
    const gatedStore = ManifestStore.of({
      ...store,
      updateFrom: (view, edit) =>
        Effect.gen(function* () {
          const hold = yield* Ref.getAndSet(armed, false);
          if (hold && gate === "beforeCommit") {
            yield* Deferred.succeed(reached, undefined);
            return yield* Effect.never;
          }
          const result = yield* store.updateFrom(view, edit);
          if (hold) {
            yield* Deferred.succeed(reached, undefined);
            return yield* Effect.never;
          }
          return result;
        }),
    });
    const newWriter = () =>
      Layer.build(
        dbWriterLayerWithoutDependencies.pipe(
          Layer.provide(sstStoreLayerWithoutDependencies),
          Layer.provide(memTableLayerWithoutDependencies),
          Layer.provide([
            durableObjectAlarmTestLayer,
            compactionTriggerTestLayer,
            backgroundWorkTestLayer,
            lsmConfigurationTestLayer(),
          ]),
          Layer.provide(Layer.succeedContext(shared.pipe(Context.add(ManifestStore, gatedStore)))),
        ),
      ).pipe(Effect.map((context) => Context.get(context, DbWriter)));
    const writer = yield* newWriter();
    yield* writer.write(DB, put("a", "before the seal"));
    yield* Ref.set(armed, true);
    const seal = yield* Effect.forkChild(writer.seal(DB, [], false));
    yield* Deferred.await(reached);
    yield* Fiber.interrupt(seal);
    const next = restart ? yield* newWriter() : writer;
    const manifestState = (yield* (yield* ManifestReader).loadLatest(DB)).manifest.state._tag;
    const late = yield* Effect.result(next.write(DB, put("b", "after the seal attempt")));
    const flushed = yield* Effect.result(next.flush(DB));
    const durable = yield* durableValues;
    return {
      manifestState,
      late: Result.isSuccess(late) ? "acknowledged" : late.failure._tag,
      flushed: Result.isSuccess(flushed) ? "flushed" : flushed.failure._tag,
      durable: Object.fromEntries(durable.values),
    };
  }).pipe(
    Effect.scoped,
    Effect.provide(Layer.merge(manifestStoreTestLayer, writerSqlStorageTestLayer)),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
    Effect.runPromise,
  );

describe("DbWriter seal whose commit outcome it never saw (D85)", () => {
  // An acknowledged write after the seal would be in no successor of a split, and its writer,
  // fenced by the seal's epoch, could never flush it.
  it("a seal interrupted after its manifest commit landed refuses every later write", async () => {
    expect(await sealInterrupted("afterCommit", false)).toEqual({
      manifestState: "Sealed",
      late: "DatabaseSealedError",
      flushed: "flushed",
      durable: { a: "before the seal" },
    });
  });

  it("a writer evicted after its seal's commit landed starts sealed", async () => {
    expect(await sealInterrupted("afterCommit", true)).toEqual({
      manifestState: "Sealed",
      late: "DatabaseSealedError",
      flushed: "flushed",
      durable: { a: "before the seal" },
    });
  });

  it("a seal interrupted before its manifest commit leaves the database open and writable", async () => {
    expect(await sealInterrupted("beforeCommit", false)).toEqual({
      manifestState: "Open",
      late: "acknowledged",
      flushed: "flushed",
      durable: { a: "before the seal", b: "after the seal attempt" },
    });
  });
});

describe("DbWriter alarm for a database that is gone (D85)", () => {
  it("a restarted writer whose database was removed before its state was deleted stops setting its alarm", async () => {
    const outcome = await Effect.gen(function* () {
      yield* createDatabase();
      const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
      // One alarm and one SQLite storage, kept across the restart, as a Durable Object keeps them.
      const alarm = yield* Effect.provide(DurableObjectAlarm, durableObjectAlarmTestLayer);
      const newWriter = () =>
        Layer.build(
          dbWriterLayerWithoutDependencies.pipe(
            Layer.provide(sstStoreLayerWithoutDependencies),
            Layer.provide(memTableLayerWithoutDependencies),
            Layer.provide([
              Layer.succeed(DurableObjectAlarm, alarm),
              compactionTriggerTestLayer,
              backgroundWorkTestLayer,
              lsmConfigurationTestLayer(),
            ]),
            Layer.provide(Layer.succeedContext(shared)),
          ),
        ).pipe(Effect.map((context) => Context.get(context, DbWriter)));
      const before = yield* newWriter();
      yield* before.write(DB, put("a", "unflushed"));
      const set = yield* alarm.scheduled;
      // Removal marks the manifest removed; the step that deletes the writer's state never runs
      // (the removal was interrupted), and the object restarts.
      const removedAt = Timestamp.make(yield* Clock.currentTimeMillis);
      yield* (yield* ManifestStore).update(DB, (manifest) => markRemoved(manifest, removedAt));
      const after = yield* newWriter();
      const fired = [];
      for (let firing = 0; firing < 3; firing++) {
        yield* alarm.cancel;
        yield* after.runAlarm;
        fired.push(Option.isSome(yield* alarm.scheduled));
      }
      return { set: Option.isSome(set), fired };
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.merge(manifestStoreTestLayer, writerSqlStorageTestLayer)),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    // Retrying every second would wake the object forever; nothing it holds can be flushed.
    expect(outcome).toEqual({ set: true, fired: [false, false, false] });
  });
});

describe("DbWriter seal of a sealed database (D95)", () => {
  it("a seal naming other successors fails with the recorded ones; the same successors again succeed", async () => {
    const LEFT = DatabaseId.make("orders-a");
    const RIGHT = DatabaseId.make("orders-b");
    const OTHER = DatabaseId.make("orders-c");
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase();
        const writer = yield* DbWriter;
        yield* writer.write(DB, put("a", "1"));
        yield* writer.seal(DB, [LEFT, RIGHT], true);
        const attempt = (successors: ReadonlyArray<DatabaseId>, holds: boolean) =>
          Effect.map(Effect.result(writer.seal(DB, successors, holds)), (result) =>
            Result.isSuccess(result)
              ? "sealed"
              : result.failure._tag === "DatabaseSealedError"
                ? `DatabaseSealedError:${result.failure.successors.join(",")}`
                : result.failure._tag,
          );
        const other = yield* attempt([OTHER], false);
        const otherWithHolds = yield* attempt([OTHER], true);
        const same = yield* attempt([LEFT, RIGHT], false);
        const sameWithHolds = yield* attempt([LEFT, RIGHT], true);
        const manifest = (yield* (yield* ManifestReader).loadLatest(DB)).manifest;
        return {
          other,
          otherWithHolds,
          same,
          sameWithHolds,
          successors: manifest.state._tag === "Sealed" ? manifest.state.successors : [],
          holders: manifest.checkpoints.flatMap((checkpoint) => Option.toArray(checkpoint.holder)),
        };
      }),
    );
    expect(outcome).toEqual({
      other: "DatabaseSealedError:orders-a,orders-b",
      otherWithHolds: "DatabaseSealedError:orders-a,orders-b",
      same: "sealed",
      sameWithHolds: "sealed",
      successors: ["orders-a", "orders-b"],
      holders: ["orders-a", "orders-b"],
    });
  });
});

describe("DbWriter alarm retries (D102)", () => {
  /**
   * A writer whose `writer_meta` reads fail while `metaFails`, and whose flush commits fail while
   * `r2Down`, with the alarm and storage it keeps across firings.
   */
  const failingWriter = Effect.gen(function* () {
    yield* createDatabase();
    const store = yield* ManifestStore;
    const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
    const flags = { metaFails: false, r2Down: false, defect: false };
    const failingStore = ManifestStore.of({
      ...store,
      updateFrom: (view, edit) =>
        flags.r2Down
          ? Effect.fail(
              new ManifestWriteError({
                databaseId: DB,
                reason: "StorageUnavailable",
                message: "R2 is unavailable",
              }),
            )
          : store.updateFrom(view, edit),
    });
    const failingMemTable = Layer.effect(
      MemTable,
      Effect.map(MemTable, (memTable) =>
        MemTable.of({
          ...memTable,
          meta: Effect.suspend(() =>
            flags.defect
              ? Effect.die(new Error("a bug"))
              : flags.metaFails
                ? Effect.fail(
                    new MemTableError({
                      operation: "meta",
                      reason: "StorageUnavailable",
                      message: "SQLite failed",
                    }),
                  )
                : memTable.meta,
          ),
        }),
      ),
    ).pipe(
      Layer.provide(
        memTableLayerWithoutDependencies.pipe(Layer.provide(writerSqlStorageTestLayer)),
      ),
    );
    const context = yield* Layer.build(
      Layer.provideMerge(
        dbWriterLayerWithoutDependencies.pipe(Layer.provide(sstStoreLayerWithoutDependencies)),
        Layer.mergeAll(
          failingMemTable,
          durableObjectAlarmTestLayer,
          compactionTriggerTestLayer,
          backgroundWorkTestLayer,
          lsmConfigurationTestLayer(),
        ),
      ).pipe(
        Layer.provide(Layer.succeedContext(shared.pipe(Context.add(ManifestStore, failingStore)))),
      ),
    );
    const writer = Context.get(context, DbWriter);
    const alarm = Context.get(context, DurableObjectAlarm);
    /** Fires the alarm at its scheduled time, as the runtime does; the delay it then set. */
    const fireAtScheduled = Effect.gen(function* () {
      const at = Option.getOrThrow(yield* alarm.scheduled);
      const now = yield* Clock.currentTimeMillis;
      if (at > now) yield* TestClock.adjust(at - now);
      yield* alarm.cancel;
      const exit = yield* Effect.exit(writer.runAlarm);
      const next = yield* alarm.scheduled;
      return {
        threw: exit._tag === "Failure",
        delay: Option.map(next, (time) => time - at),
      };
    });
    return { writer, alarm, flags, fireAtScheduled };
  });

  const runFailing = <A, E>(
    body: (
      writer: Effect.Success<typeof failingWriter>,
    ) => Effect.Effect<A, E, ManifestStore | ManifestReader | ObjectStore | RuntimeContext>,
  ) =>
    Effect.flatMap(failingWriter, body).pipe(
      Effect.scoped,
      Effect.provide(manifestStoreTestLayer),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );

  it("a flush that keeps failing is retried from 1 s, doubling to maxL0Age, and the first success resets it", async () => {
    const outcome = await runFailing(({ writer, flags, fireAtScheduled }) =>
      Effect.gen(function* () {
        yield* writer.write(DB, put("a", "1"));
        flags.r2Down = true;
        const failing: Array<number | undefined> = [];
        for (let firing = 0; firing < 9; firing++) {
          const fired = yield* fireAtScheduled;
          if (fired.threw) return "threw";
          failing.push(Option.getOrUndefined(fired.delay));
        }
        flags.r2Down = false;
        const recovered = yield* fireAtScheduled;
        const flushed = (yield* writer.status(DB)).flushedSeq;
        // The next failure starts again from 1 s.
        yield* writer.write(DB, put("b", "2"));
        flags.r2Down = true;
        yield* fireAtScheduled;
        const afterReset = yield* fireAtScheduled;
        return {
          failing,
          recovered: Option.getOrUndefined(recovered.delay),
          flushed,
          afterReset: Option.getOrUndefined(afterReset.delay),
        };
      }),
    );
    // The first failure waits 1 s (the alarm had been due at maxL0Age); the waits then double.
    expect(outcome).toEqual({
      failing: [1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 60_000, 60_000, 60_000],
      // Recovered: the flush ran; no row waits, so no alarm.
      recovered: undefined,
      flushed: 1,
      afterReset: 2_000,
    });
  });

  it("an unreadable writer_meta at the alarm's start sets the alarm again, and a defect never escapes", async () => {
    const outcome = await runFailing(({ writer, flags, fireAtScheduled }) =>
      Effect.gen(function* () {
        yield* writer.write(DB, put("a", "1"));
        flags.metaFails = true;
        const unreadable = [yield* fireAtScheduled, yield* fireAtScheduled];
        flags.metaFails = false;
        flags.defect = true;
        const defect = yield* fireAtScheduled;
        flags.defect = false;
        const recovered = yield* fireAtScheduled;
        return {
          unreadable: unreadable.map((fired) => [fired.threw, Option.getOrUndefined(fired.delay)]),
          defect: [defect.threw, Option.getOrUndefined(defect.delay)],
          flushed: (yield* writer.status(DB)).flushedSeq,
          recovered: Option.getOrUndefined(recovered.delay),
        };
      }),
    );
    expect(outcome).toEqual({
      unreadable: [
        [false, 1_000],
        [false, 2_000],
      ],
      defect: [false, 4_000],
      flushed: 1,
      recovered: undefined,
    });
  });
});

describe("DbWriter's view after the stalled-write reload (D102)", () => {
  it("a reload that loaded before a flush committed never puts the older manifest over the flush's", async () => {
    const outcome = await Effect.gen(function* () {
      yield* createDatabase();
      const store = yield* ManifestStore;
      const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
      // Hooks run, once each, before a flush's commit and after the reload's load.
      const beforeFlushCommit: Array<Effect.Effect<void>> = [];
      const afterReloadLoad: Array<Effect.Effect<void>> = [];
      const hookedStore = ManifestStore.of({
        ...store,
        updateFrom: (view, edit) =>
          Effect.andThen(
            Effect.suspend(() => beforeFlushCommit.shift() ?? Effect.void),
            store.updateFrom(view, edit),
          ),
        update: (databaseId, edit) =>
          Effect.tap(store.update(databaseId, edit), () =>
            Effect.suspend(() => afterReloadLoad.shift() ?? Effect.void),
          ),
      });
      const context = yield* Layer.build(
        dbWriterLayerWithoutDependencies.pipe(
          Layer.provide(sstStoreLayerWithoutDependencies),
          Layer.provide(
            memTableLayerWithoutDependencies.pipe(Layer.provide(writerSqlStorageTestLayer)),
          ),
          Layer.provide([
            durableObjectAlarmTestLayer,
            compactionTriggerTestLayer,
            backgroundWorkTestLayer,
            lsmConfigurationTestLayer({ l0MaxSsts: 2 }),
          ]),
          Layer.provide(Layer.succeedContext(shared.pipe(Context.add(ManifestStore, hookedStore)))),
        ),
      );
      const writer = Context.get(context, DbWriter);
      const runtime = yield* Effect.context<RuntimeContext>();
      const quietly = <A, E>(effect: Effect.Effect<A, E, RuntimeContext>) =>
        effect.pipe(Effect.asVoid, Effect.orDie, Effect.provide(runtime));
      yield* writer.write(DB, put("a", "1"));
      yield* writer.flush(DB);
      yield* writer.write(DB, put("b", "2"));
      // The second flush leaves L0 at its limit; a write lands while it uploads, so it stays
      // unflushed.
      beforeFlushCommit.push(quietly(writer.write(DB, put("c", "3"))));
      yield* writer.flush(DB);
      // The next write reloads (L0 at its limit); right after its load a third flush commits.
      afterReloadLoad.push(quietly(writer.flush(DB)));
      const stalled = yield* Effect.flip(writer.write(DB, put("d", "4")));
      const latest = yield* (yield* ManifestReader).loadLatest(DB);
      const status = yield* writer.status(DB);
      return {
        stalled: stalled._tag,
        latestL0: latest.manifest.l0.length,
        statusL0: status.l0Count,
        durable: Object.fromEntries((yield* durableValues).values),
      };
    }).pipe(
      Effect.scoped,
      Effect.provide(manifestStoreTestLayer),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    expect(outcome).toEqual({
      stalled: "WriteStalledError",
      latestL0: 3,
      // The writer's view is the newest manifest, not the one the reload loaded before the flush.
      statusL0: 3,
      durable: { a: "1", b: "2", c: "3" },
    });
  });
});

describe("DbWriter's flush alarm set from two writes at once (D102)", () => {
  it("a scheduler that read the alarm before another set an earlier one never sets its later time over it", async () => {
    const outcome = await Effect.gen(function* () {
      yield* createDatabase();
      const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
      const inner = yield* Effect.provide(DurableObjectAlarm, durableObjectAlarmTestLayer);
      // The first `scheduled` read after arming waits until `release`: the first write's
      // scheduler has read "no alarm" while the second write's runs.
      const armed = yield* Ref.make(false);
      const reading = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const alarm = DurableObjectAlarm.of({
        ...inner,
        scheduled: Effect.gen(function* () {
          const value = yield* inner.scheduled;
          if (yield* Ref.getAndSet(armed, false)) {
            yield* Deferred.succeed(reading, undefined);
            yield* Deferred.await(release);
          }
          return value;
        }),
      });
      const context = yield* Layer.build(
        dbWriterLayerWithoutDependencies.pipe(
          Layer.provide(sstStoreLayerWithoutDependencies),
          Layer.provide(
            memTableLayerWithoutDependencies.pipe(Layer.provide(writerSqlStorageTestLayer)),
          ),
          Layer.provide([
            Layer.succeed(DurableObjectAlarm, alarm),
            compactionTriggerTestLayer,
            backgroundWorkTestLayer,
            lsmConfigurationTestLayer({ l0SstSize: 4_096 }),
          ]),
          Layer.provide(Layer.succeedContext(shared)),
        ),
      );
      const writer = Context.get(context, DbWriter);
      yield* TestClock.adjust("10 seconds");
      // A small write: its scheduler wants the age limit, and is held after reading the alarm.
      yield* Ref.set(armed, true);
      const small = yield* Effect.forkChild(writer.write(DB, put("a", "1")));
      yield* Deferred.await(reading);
      // A write that brings the unflushed bytes over l0SstSize: its scheduler wants now. It is
      // committed before the held scheduler goes on.
      const large = yield* Effect.forkChild(writer.write(DB, put("b", "x".repeat(8_192))));
      yield* Effect.repeat(writer.status(DB), {
        until: (status) => status.unflushedRows === 2,
      });
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(small);
      yield* Fiber.join(large);
      const now = yield* Clock.currentTimeMillis;
      return { scheduledIn: Option.map(yield* inner.scheduled, (at) => at - now) };
    }).pipe(
      Effect.scoped,
      Effect.provide(manifestStoreTestLayer),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    // Due now: the size limit is reached. Without serialized scheduling, the held scheduler set the
    // age limit (60 s) over it, and the flush waited for that.
    expect(outcome.scheduledIn).toEqual(Option.some(0));
  });
});

describe("DbWriter commit lock (D122)", () => {
  /**
   * Holds the seal's manifest commit until released, and while it is held sends a write: the
   * commit lock must keep the write from committing between the seal's final flush and its
   * commit (an acknowledged write the sealed manifest would not hold), so it waits and is refused.
   */
  it("a write sent while a seal commits waits for it and is refused, never acknowledged and lost", async () => {
    const outcome = await Effect.gen(function* () {
      yield* createDatabase();
      const store = yield* ManifestStore;
      const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
      const reached = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const armed = yield* Ref.make(false);
      const gatedStore = ManifestStore.of({
        ...store,
        updateFrom: (view, edit) =>
          Effect.gen(function* () {
            if (yield* Ref.getAndSet(armed, false)) {
              yield* Deferred.succeed(reached, undefined);
              yield* Deferred.await(release);
            }
            return yield* store.updateFrom(view, edit);
          }),
      });
      const writer = yield* Layer.build(
        dbWriterLayerWithoutDependencies.pipe(
          Layer.provide(sstStoreLayerWithoutDependencies),
          Layer.provide(memTableLayerWithoutDependencies),
          Layer.provide([
            durableObjectAlarmTestLayer,
            compactionTriggerTestLayer,
            backgroundWorkTestLayer,
            lsmConfigurationTestLayer(),
          ]),
          Layer.provide(Layer.succeedContext(shared.pipe(Context.add(ManifestStore, gatedStore)))),
        ),
      ).pipe(Effect.map((context) => Context.get(context, DbWriter)));
      yield* writer.write(DB, put("a", "before the seal"));
      yield* Ref.set(armed, true);
      const seal = yield* Effect.forkChild(writer.seal(DB, [], false));
      yield* Deferred.await(reached);
      const late = yield* Effect.forkChild(
        Effect.result(writer.write(DB, put("b", "during the seal"))),
      );
      // Give the write every chance to run while the seal's commit is held.
      yield* Effect.yieldNow;
      yield* TestClock.adjust("1 second");
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(seal);
      const written = yield* Fiber.join(late);
      const durable = yield* durableValues;
      return {
        late: Result.isSuccess(written) ? "acknowledged" : written.failure._tag,
        durable: Object.fromEntries(durable.values),
      };
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.merge(manifestStoreTestLayer, writerSqlStorageTestLayer)),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    expect(outcome).toEqual({
      late: "DatabaseSealedError",
      durable: { a: "before the seal" },
    });
  });
});

/** Lets every other fiber run until it suspends. */
const settle = Effect.forEach(Array.from({ length: 50 }), () => Effect.yieldNow, {
  discard: true,
});

describe("DbWriter seal interrupted while a flush is in flight (D115)", () => {
  // Found by the WriterObject model (D109). On the code before D115: a flush holds the flush lock
  // at writer epoch e; a seal takes the commit lock, waits for the flush lock, and is interrupted,
  // so its onExit forgot the phase (D85); the next request started the writer again at e + 1;
  // the flush's commit was refused with WriterFencedError, and markFenced replaced the new phase
  // with Fenced. Every later request failed until the object was evicted, and the alarm stopped.
  it("a seal interrupted before it applied its edit keeps the phase, and the flush in flight commits", async () => {
    const outcome = await Effect.gen(function* () {
      const store = yield* ManifestStore;
      yield* store.initialize(
        initialManifest(DB, {
          keyRange: Option.none(),
          databaseOptions: { ...DEFAULT_DATABASE_OPTIONS, changelog: Option.none() },
        }),
      );
      const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
      const reached = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const armed = yield* Ref.make(false);
      const gatedStore = ManifestStore.of({
        ...store,
        updateFrom: (view, edit) =>
          Effect.gen(function* () {
            if (yield* Ref.getAndSet(armed, false)) {
              yield* Deferred.succeed(reached, undefined);
              yield* Deferred.await(release);
            }
            return yield* store.updateFrom(view, edit);
          }),
      });
      const writer = yield* Layer.build(
        dbWriterLayerWithoutDependencies.pipe(
          Layer.provide(sstStoreLayerWithoutDependencies),
          Layer.provide(memTableLayerWithoutDependencies),
          Layer.provide([
            durableObjectAlarmTestLayer,
            compactionTriggerTestLayer,
            backgroundWorkTestLayer,
            lsmConfigurationTestLayer(),
          ]),
          Layer.provide(Layer.succeedContext(shared.pipe(Context.add(ManifestStore, gatedStore)))),
        ),
      ).pipe(Effect.map((context) => Context.get(context, DbWriter)));

      yield* writer.write(DB, put("a", "1"));
      // The flush uploads and stops before its manifest commit, holding the flush lock.
      yield* Ref.set(armed, true);
      const flush = yield* Effect.forkChild(Effect.result(writer.flush(DB)));
      yield* Deferred.await(reached);
      // The seal takes the commit lock and waits for the flush lock; its caller hangs up.
      const seal = yield* Effect.forkChild(writer.seal(DB, [], false));
      yield* settle;
      yield* Fiber.interrupt(seal);
      // On the code before D115 this request started the writer again, at a new epoch.
      const status = yield* writer.status(DB);
      yield* Deferred.succeed(release, undefined);
      const flushed = yield* Fiber.join(flush);
      const later = yield* Effect.result(writer.write(DB, put("b", "2")));
      const flushedLater = yield* Effect.result(writer.flush(DB));
      const latest = yield* (yield* ManifestReader).loadLatest(DB);
      return {
        restartedEpoch: status.writerEpoch,
        manifestEpoch: latest.manifest.writerEpoch,
        firstFlush: Result.isSuccess(flushed) ? "flushed" : flushed.failure._tag,
        later: Result.isSuccess(later) ? "acknowledged" : later.failure._tag,
        flushedLater: Result.isSuccess(flushedLater) ? "flushed" : flushedLater.failure._tag,
      };
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.merge(manifestStoreTestLayer, writerSqlStorageTestLayer)),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    expect(outcome.restartedEpoch).toBe(outcome.manifestEpoch);
    expect(outcome.firstFlush).toBe("flushed");
    expect(outcome.later).toBe("acknowledged");
    expect(outcome.flushedLater).toBe("flushed");
  });
});

describe("DbWriter flush fenced by its own writer's restart (D115)", () => {
  // Found by the WriterObject model (D109). A seal with nothing to flush commits outside the flush
  // lock; its edit is applied and the seal is interrupted, so it forgets the phase (D85). A flush
  // that took the flush lock and its view at epoch e before that reads the memtable after a write
  // restarted the writer at e + 1; its commit at e is refused. On the code before D115 markFenced
  // then replaced the restarted writer's phase with Fenced: every later request failed until the
  // object was evicted, and the alarm stopped.
  it("a flush refused at an older epoch leaves the restarted writer serving", async () => {
    const outcome = await Effect.gen(function* () {
      yield* createDatabase();
      const store = yield* ManifestStore;
      const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
      const sealReached = yield* Deferred.make<void>();
      const sealArmed = yield* Ref.make(false);
      const gatedStore = ManifestStore.of({
        ...store,
        updateFrom: (view, edit) =>
          Effect.gen(function* () {
            if (yield* Ref.getAndSet(sealArmed, false)) {
              // The edit is applied and the create never answers.
              edit(view.manifest, view.id);
              yield* Deferred.succeed(sealReached, undefined);
              return yield* Effect.never;
            }
            return yield* store.updateFrom(view, edit);
          }),
      });
      const base = Context.get(yield* Layer.build(memTableLayerWithoutDependencies), MemTable);
      const metaReached = yield* Deferred.make<void>();
      const metaRelease = yield* Deferred.make<void>();
      const metaArmed = yield* Ref.make(false);
      const gatedMemTable = MemTable.of({
        ...base,
        meta: Effect.gen(function* () {
          if (yield* Ref.getAndSet(metaArmed, false)) {
            yield* Deferred.succeed(metaReached, undefined);
            yield* Deferred.await(metaRelease);
          }
          return yield* base.meta;
        }),
      });
      const writer = yield* Layer.build(
        dbWriterLayerWithoutDependencies.pipe(
          Layer.provide(sstStoreLayerWithoutDependencies),
          Layer.provide(Layer.succeed(MemTable, gatedMemTable)),
          Layer.provide([
            durableObjectAlarmTestLayer,
            compactionTriggerTestLayer,
            backgroundWorkTestLayer,
            lsmConfigurationTestLayer(),
          ]),
          Layer.provide(Layer.succeedContext(shared.pipe(Context.add(ManifestStore, gatedStore)))),
        ),
      ).pipe(Effect.map((context) => Context.get(context, DbWriter)));

      yield* writer.write(DB, put("a", "1"));
      yield* writer.flush(DB);
      // The seal has nothing to flush: its own commit applies the edit and hangs.
      yield* Ref.set(sealArmed, true);
      const seal = yield* Effect.forkChild(writer.seal(DB, [], false));
      yield* Deferred.await(sealReached);
      // A flush takes the flush lock and its view, and stops before reading the memtable.
      yield* Ref.set(metaArmed, true);
      const flush = yield* Effect.forkChild(Effect.result(writer.flush(DB)));
      yield* Deferred.await(metaReached);
      // The seal's caller hangs up: its commit may have landed, so it forgets the phase.
      yield* Fiber.interrupt(seal);
      // A write starts the writer again at a new epoch and commits a row for the flush to cut.
      yield* writer.write(DB, put("b", "2"));
      yield* Deferred.succeed(metaRelease, undefined);
      const flushed = yield* Fiber.join(flush);
      const later = yield* Effect.result(writer.write(DB, put("c", "3")));
      const flushedLater = yield* Effect.result(writer.flush(DB));
      const status = yield* Effect.result(writer.status(DB));
      return {
        firstFlush: Result.isSuccess(flushed) ? "flushed" : flushed.failure._tag,
        later: Result.isSuccess(later) ? "acknowledged" : later.failure._tag,
        flushedLater: Result.isSuccess(flushedLater) ? "flushed" : flushedLater.failure._tag,
        unflushedRows: Result.isSuccess(status) ? status.success.unflushedRows : -1,
        durable: Object.fromEntries((yield* durableValues).values),
      };
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.merge(manifestStoreTestLayer, writerSqlStorageTestLayer)),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    expect(outcome).toEqual({
      // Cut at the older epoch: refused, its rows stay in SQLite for the next flush.
      firstFlush: "WriterFencedError",
      later: "acknowledged",
      flushedLater: "flushed",
      unflushedRows: 0,
      durable: { a: "1", b: "2", c: "3" },
    });
  });
});

describe("DbWriter changelog after a flush commit whose response was lost (D113)", () => {
  // A flush's manifest commit lands and its response is lost (the read of GC's boundary after
  // the create fails), so flushed_seq is not recorded. Before D113 the next flush exported its
  // changelog from the old flushed_seq again: two entries held the same changes, and the feed,
  // which reads every entry above afterSeq in turn, delivered them twice. Now its commit is
  // refused as stale, the writer records the manifest's flushed state, and flushes again from it.
  it("no two changelog entries hold the same sequence number", async () => {
    const entries = await Effect.gen(function* () {
      yield* createDatabase({ changelog: true });
      const store = yield* ManifestStore;
      const shared = yield* Effect.context<ManifestStore | ManifestReader | ObjectStore>();
      const armed = yield* Ref.make(false);
      const lossyStore = ManifestStore.of({
        ...store,
        updateFrom: (view, edit) =>
          Effect.gen(function* () {
            const result = yield* store.updateFrom(view, edit);
            if (yield* Ref.getAndSet(armed, false)) {
              return yield* Effect.fail(
                new ManifestWriteError({
                  databaseId: DB,
                  reason: "StorageUnavailable",
                  message: "the response after the create was lost",
                }),
              );
            }
            return result;
          }),
      });
      const writer = yield* Layer.build(
        dbWriterLayerWithoutDependencies.pipe(
          Layer.provide(sstStoreLayerWithoutDependencies),
          Layer.provide(memTableLayerWithoutDependencies),
          Layer.provide([
            durableObjectAlarmTestLayer,
            compactionTriggerTestLayer,
            backgroundWorkTestLayer,
            lsmConfigurationTestLayer(),
          ]),
          Layer.provide(Layer.succeedContext(shared.pipe(Context.add(ManifestStore, lossyStore)))),
        ),
      ).pipe(Effect.map((context) => Context.get(context, DbWriter)));
      yield* writer.write(DB, put("a", "1"));
      yield* Ref.set(armed, true);
      yield* Effect.result(writer.flush(DB));
      // Nothing new: the flush finds its flushed_seq stale, records the landed manifest, and has
      // nothing left to write. A reader whose cache is behind loads that manifest (D67).
      yield* writer.flush(DB);
      const landed = yield* (yield* ManifestReader).loadLatest(DB);
      const recorded = yield* writer.status(DB);
      expect({ flushedSeq: recorded.flushedSeq, manifest: recorded.flushedManifestId }).toEqual({
        flushedSeq: 1,
        manifest: Option.some(landed.id),
      });
      yield* writer.write(DB, put("b", "2"));
      yield* writer.flush(DB);
      const latest = yield* (yield* ManifestReader).loadLatest(DB);
      const status = yield* writer.status(DB);
      // Every acknowledged row is in L0 once, and the writer recorded all of it as flushed.
      expect({ flushedSeq: status.flushedSeq, unflushedRows: status.unflushedRows }).toEqual({
        flushedSeq: 2,
        unflushedRows: 0,
      });
      expect(Object.fromEntries((yield* durableValues).values)).toEqual({ a: "1", b: "2" });
      return latest.manifest.changelog.map((entry) => [entry.firstSeq, entry.lastSeq]);
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.merge(manifestStoreTestLayer, writerSqlStorageTestLayer)),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    expect(entries).toEqual([
      [1, 1],
      [2, 2],
    ]);
    for (const [index, [, last]] of entries.entries()) {
      const next = entries[index + 1];
      if (next !== undefined) expect(next[0]).toBeGreaterThan(last ?? 0);
    }
    expect(entries.length).toBeGreaterThan(0);
  });
});
