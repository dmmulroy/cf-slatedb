/**
 * DbWriter at its limits (mutation sweep, D127): backpressure and the size threshold exactly at
 * their values, the L0 backlog refusal and its reload once compaction freed L0, the page limits of
 * scans and the change feed, and a destroy that leaves no alarm.
 */
import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  FULL_KEY_RANGE,
  initialManifest,
  replaceManifestFields,
  SeqNum,
  type WriteBatch,
} from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Option, Ref, Result } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { CompactionTriggerRecord } from "./compaction-trigger.ts";
import { DbWriter, dbWriterTestLayer } from "./db-writer.ts";
import { DurableObjectAlarm } from "./durable-object-alarm.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestStore } from "./manifest-store.ts";

const DB = DatabaseId.make("orders");

const put = (key: string): WriteBatch => ({
  operations: [
    {
      _tag: "Put",
      key: new TextEncoder().encode(key),
      value: Uint8Array.of(1),
      ttlMillis: Option.none(),
    },
  ],
});

/** A one-byte key with a one-byte value: 1 + 1 + 32 bytes of overhead in the memtable. */
const ONE_WRITE_BYTES = 34;

const run = <A, E>(
  body: Effect.Effect<
    A,
    E,
    DbWriter | ManifestStore | DurableObjectAlarm | CompactionTriggerRecord | RuntimeContext
  >,
  settings: Parameters<typeof dbWriterTestLayer>[0] = {},
) =>
  Effect.gen(function* () {
    yield* (yield* ManifestStore).initialize(
      initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
    );
    return yield* body;
  }).pipe(
    Effect.provide(dbWriterTestLayer(settings)),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
    Effect.runPromise,
  );

const outcomeOf = <A, E extends { readonly _tag: string }>(result: Result.Result<A, E>) =>
  Result.isSuccess(result) ? "Success" : result.failure._tag;

describe("DbWriter at its limits (D127)", () => {
  it("at exactly l0SstSize the flush is due now and the alarm flushes; at exactly maxUnflushedBytes writes stall", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const writer = yield* DbWriter;
        const alarm = yield* DurableObjectAlarm;
        yield* TestClock.setTime(5_000);
        yield* writer.write(DB, put("a"));
        const scheduled = yield* alarm.scheduled;
        const second = outcomeOf(yield* Effect.result(writer.write(DB, put("b"))));
        yield* alarm.cancel;
        yield* writer.runAlarm;
        return { scheduled, second, flushedSeq: (yield* writer.status(DB)).flushedSeq };
      }),
      { l0SstSize: ONE_WRITE_BYTES, maxUnflushedBytes: ONE_WRITE_BYTES },
    );
    expect(outcome).toEqual({
      scheduled: Option.some(5_000),
      second: "WriteStalledError",
      flushedSeq: 1,
    });
  });

  it("refuses writes at exactly l0MaxSsts L0 SSTs, and accepts them once a reload finds L0 compacted", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const writer = yield* DbWriter;
        for (const key of ["a", "b", "c", "d"]) {
          yield* writer.write(DB, put(key));
          yield* writer.flush(DB);
        }
        const stalled = outcomeOf(yield* Effect.result(writer.write(DB, put("e"))));
        // Compaction empties L0 in a manifest the writer has not seen (its view still holds four).
        yield* (yield* ManifestStore).update(DB, (manifest) =>
          Result.succeed(replaceManifestFields(manifest, { l0: [] })),
        );
        const afterCompaction = outcomeOf(yield* Effect.result(writer.write(DB, put("f"))));
        return { stalled, afterCompaction };
      }),
      { l0MaxSsts: 4 },
    );
    expect(outcome).toEqual({ stalled: "WriteStalledError", afterCompaction: "Success" });
  });

  it("a scan page holds at most its limit, and at most 1,000 rows; a change feed page at most its limit", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const writer = yield* DbWriter;
        yield* writer.write(DB, {
          operations: Array.from({ length: 1_001 }, (_, index) => ({
            _tag: "Put" as const,
            key: Uint8Array.of(index >> 8, index & 0xff),
            value: Uint8Array.of(1),
            ttlMillis: Option.none(),
          })),
        });
        for (const key of ["x", "y", "z"]) yield* writer.write(DB, put(key));
        const page = (limit: number) =>
          Effect.map(
            writer.scan(DB, {
              range: FULL_KEY_RANGE,
              snapshotSeq: Option.none(),
              cursor: Option.none(),
              limit,
              includeRetained: false,
            }),
            (scanned) => scanned.rows.length,
          );
        const changes = yield* writer.changes(DB, SeqNum.make(1), 2);
        return { two: yield* page(2), huge: yield* page(5_000), changes: changes.rows.length };
      }),
    );
    // A change feed page at its row limit backs off to the start of the batch it may cut (D93):
    // three one-row batches after seq 1, limit 2, give one row.
    expect(outcome).toEqual({ two: 2, huge: 1_000, changes: 1 });
  });

  it("destroy leaves no alarm set", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const writer = yield* DbWriter;
        const alarm = yield* DurableObjectAlarm;
        yield* writer.write(DB, put("a"));
        const before = Option.isSome(yield* alarm.scheduled);
        yield* writer.destroy(DB);
        return { before, after: Option.isSome(yield* alarm.scheduled) };
      }),
    );
    expect(outcome).toEqual({ before: true, after: false });
  });

  it("names each stall's reason and retry hint, and an L0 stall wakes the compactor", async () => {
    const stallOf = (result: Result.Result<unknown, { readonly _tag: string }>) => {
      if (Result.isSuccess(result)) return "not stalled";
      const failure = result.failure;
      return failure._tag === "WriteStalledError" &&
        "reason" in failure &&
        "retryAfterMs" in failure
        ? `${String(failure.reason)}:${String(failure.retryAfterMs)}`
        : failure._tag;
    };
    const bytes = await run(
      Effect.gen(function* () {
        const writer = yield* DbWriter;
        yield* writer.write(DB, put("a"));
        return stallOf(yield* Effect.result(writer.write(DB, put("b"))));
      }),
      { l0SstSize: ONE_WRITE_BYTES, maxUnflushedBytes: ONE_WRITE_BYTES },
    );
    const backlog = await run(
      Effect.gen(function* () {
        const writer = yield* DbWriter;
        for (const key of ["a", "b", "c", "d"]) {
          yield* writer.write(DB, put(key));
          yield* writer.flush(DB);
        }
        const woken = (yield* Ref.get(yield* CompactionTriggerRecord)).length;
        const stall = stallOf(yield* Effect.result(writer.write(DB, put("e"))));
        // The wake runs in the background: let it finish.
        yield* Effect.yieldNow;
        return { stall, wakes: (yield* Ref.get(yield* CompactionTriggerRecord)).length - woken };
      }),
      { l0MaxSsts: 4 },
    );
    expect({ bytes, backlog }).toEqual({
      bytes: "UnflushedBytes:1000",
      backlog: { stall: "L0Backlog:5000", wakes: 1 },
    });
  });
});
