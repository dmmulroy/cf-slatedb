import {
  FULL_KEY_RANGE,
  type CompactionJob,
  type CompactorEpoch,
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  planCompactions,
  type SstHandle,
  type TransactionIsolation,
} from "@shaledb/lsm-core";
import { Arbitrary, Effect, Layer, Option, Schema } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { CompactionExecutor, makeCompactionExecutor } from "./compaction-executor.ts";
import { DbClient, dbClientTestLayer, type TransactionBegin } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";

const DB = DatabaseId.make("orders");
const key = (n: number) => Uint8Array.of(0x6b, n);
const byte = (value: Option.Option<Uint8Array>) =>
  Option.getOrUndefined(Option.map(value, (bytes) => bytes[0]));
const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

/**
 * The client and an executor over one bucket. Flushed rows stay 3 s (the test clock) and at most
 * ~120 bytes, so trims happen while transactions are open (D58).
 */
const testLayer = (settings: Parameters<typeof dbClientTestLayer>[0]) =>
  Layer.effect(CompactionExecutor, makeCompactionExecutor).pipe(
    Layer.provide(sstStoreLayerWithoutDependencies),
    Layer.provideMerge(dbClientTestLayer({ blockSize: 64, ...settings })),
  );

type TestServices =
  | CompactionExecutor
  | DbClient
  | ManifestStore
  | ManifestReader
  | DbReaderServices;

const provideWith =
  (settings: Parameters<typeof dbClientTestLayer>[0]) =>
  <A, E>(program: Effect.Effect<A, E, TestServices>) =>
    program.pipe(
      Effect.scoped,
      Effect.provide(testLayer(settings)),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
    );

const provide = provideWith({ transactionRetentionMs: 3_000, maxRetainedBytes: 120 });

/** Runs one compaction job the way the Workflow does. */
const runJob = (job: CompactionJob, compactorEpoch: CompactorEpoch) =>
  Effect.gen(function* () {
    const executor = yield* CompactionExecutor;
    const prepared = yield* executor.prepare(DB, compactorEpoch, job);
    const outputs: Array<SstHandle> = [];
    let start = Option.none<Uint8Array>();
    do {
      const step = yield* executor.writeOutput(prepared, start);
      outputs.push(...Option.toArray(step.output));
      start = step.resumeFrom;
    } while (Option.isSome(start));
    return yield* executor.commit(prepared, outputs);
  });

/** Compacts L0 (and the sorted run) when there is anything to compact. */
const compact = Effect.gen(function* () {
  const { manifest } = yield* (yield* ManifestReader).loadLatest(DB);
  const [job] = planCompactions(manifest, [], {
    minCompactionSources: 2,
    maxCompactionSources: 8,
    includeSizeThreshold: 1_000_000,
    maxConcurrentCompactions: 1,
  });
  if (job !== undefined) yield* runJob(job, manifest.compactorEpoch);
});

const createDatabase = Effect.gen(function* () {
  yield* (yield* ManifestStore).initialize(
    initialManifest(DB, {
      keyRange: Option.none(),
      databaseOptions: { ...DEFAULT_DATABASE_OPTIONS, mergeOperator: Option.some("Concat") },
    }),
  );
});

type Step =
  | { readonly _tag: "Begin"; readonly txn: number }
  | { readonly _tag: "Read"; readonly txn: number; readonly key: number }
  | { readonly _tag: "Write"; readonly txn: number; readonly key: number; readonly value: number }
  | { readonly _tag: "Commit"; readonly txn: number }
  | { readonly _tag: "Direct"; readonly key: number; readonly value: number }
  | { readonly _tag: "Flush" }
  | { readonly _tag: "Compact" }
  | { readonly _tag: "Tick" };

const stepArbitrary: Arbitrary.Arbitrary<Step> = Arbitrary.all({
  kind: smallInt(0, 21),
  txn: smallInt(0, 2),
  key: smallInt(0, 2),
  value: smallInt(1, 250),
}).pipe(
  Arbitrary.map(({ kind, txn, key, value }): Step => {
    if (kind <= 3) return { _tag: "Begin", txn };
    if (kind <= 9) return { _tag: "Read", txn, key };
    if (kind <= 14) return { _tag: "Write", txn, key, value };
    if (kind <= 17) return { _tag: "Commit", txn };
    if (kind === 18) return { _tag: "Direct", key, value };
    if (kind === 19) return { _tag: "Flush" };
    if (kind === 20) return { _tag: "Compact" };
    return { _tag: "Tick" };
  }),
);

interface Running extends TransactionBegin {
  readonly startedAt: number;
  readonly reads: Map<number, number | undefined>;
  readonly writes: Map<number, number>;
}

/**
 * Runs a random interleaving of transactions (through the stateless primitives, with the begin
 * manifest), direct writes, flushes, compactions, and the clock moving (so the writer trims flushed
 * rows), against an oracle that applies commits in commit order.
 */
const checkSchedule = (isolation: TransactionIsolation) => (steps: ReadonlyArray<Step>) =>
  provide(
    Effect.gen(function* () {
      yield* createDatabase;
      const client = yield* DbClient;
      const oracle = new Map<number, number>();
      // Commit "time" of each key's last write in the oracle, to detect lost updates.
      const lastWriteAt = new Map<number, number>();
      let clock = 0;
      const running = new Map<number, Running>();
      for (const step of steps) {
        switch (step._tag) {
          case "Begin":
            if (!running.has(step.txn)) {
              running.set(step.txn, {
                ...(yield* client.beginTransaction(DB)),
                startedAt: clock,
                reads: new Map(),
                writes: new Map(),
              });
            }
            break;
          case "Read": {
            const txn = running.get(step.txn);
            if (txn === undefined || txn.writes.has(step.key)) break;
            const row = yield* Effect.result(
              client.getAt(DB, key(step.key), txn.readSeq, txn.manifestId),
            );
            if (row._tag === "Failure") {
              if (row.failure._tag !== "TransactionConflictError") return false;
              running.delete(step.txn);
              break;
            }
            const value = Option.flatMap(row.success, (visible) =>
              visible.value._tag === "Value" ? Option.some(visible.value.bytes) : Option.none(),
            );
            txn.reads.set(step.key, byte(value));
            break;
          }
          case "Write":
            running.get(step.txn)?.writes.set(step.key, step.value);
            break;
          case "Commit": {
            const txn = running.get(step.txn);
            if (txn === undefined || txn.writes.size === 0) break;
            running.delete(step.txn);
            const outcome = yield* Effect.result(
              client.commitTransaction(DB, {
                readSeq: txn.readSeq,
                isolation,
                batch: {
                  operations: [...txn.writes].map(([k, v]) => ({
                    _tag: "Put" as const,
                    key: key(k),
                    value: Uint8Array.of(v),
                    ttlMillis: Option.none(),
                  })),
                },
                readKeys: [...txn.reads.keys()].map(key),
                readRanges: [],
              }),
            );
            if (outcome._tag === "Failure") {
              if (outcome.failure._tag !== "TransactionConflictError") return false;
              break;
            }
            if (isolation === "SerializableSnapshot") {
              // Serializable in commit order: what it read is what the database held at its commit.
              for (const [k, v] of txn.reads) if (oracle.get(k) !== v) return false;
            } else {
              // Snapshot: no lost update, nobody else wrote its keys since it began.
              for (const k of txn.writes.keys())
                if ((lastWriteAt.get(k) ?? -1) > txn.startedAt) return false;
            }
            clock++;
            for (const [k, v] of txn.writes) {
              oracle.set(k, v);
              lastWriteAt.set(k, clock);
            }
            break;
          }
          case "Direct":
            yield* client.put(DB, key(step.key), Uint8Array.of(step.value));
            clock++;
            oracle.set(step.key, step.value);
            lastWriteAt.set(step.key, clock);
            break;
          case "Flush":
            yield* client.flush(DB);
            yield* TestClock.adjust("1 second");
            break;
          case "Compact":
            yield* compact;
            break;
          case "Tick":
            yield* TestClock.adjust("1 second");
        }
      }
      return true;
    }),
  );

const runProperty = async (isolation: TransactionIsolation) => {
  const result = await Effect.runPromise(
    Arbitrary.checkEffect(
      Arbitrary.array(stepArbitrary, { minLength: 20, maxLength: 60 }),
      checkSchedule(isolation),
      {
        runs: 200,
      },
    ),
  );
  const failure = Arbitrary.formatCheckFailure(result);
  if (failure !== undefined) throw new Error(failure);
};

describe("transactions", () => {
  it("SerializableSnapshot: every committed transaction read what the database held at its commit", async () => {
    await runProperty("SerializableSnapshot");
  });

  it("Snapshot: no committed transaction overwrote a key written since it began (no lost update)", async () => {
    await runProperty("Snapshot");
  });

  it("allows write skew under Snapshot and refuses it under SerializableSnapshot", async () => {
    const skew = (isolation: TransactionIsolation) =>
      provide(
        Effect.gen(function* () {
          yield* createDatabase;
          const client = yield* DbClient;
          yield* client.put(DB, key(1), Uint8Array.of(1));
          yield* client.put(DB, key(2), Uint8Array.of(1));
          // Both read both keys, then each writes a different one.
          const t1 = yield* client.beginTransaction(DB);
          const t2 = yield* client.beginTransaction(DB);
          const commit = ({ readSeq }: TransactionBegin, k: number) =>
            Effect.result(
              client.commitTransaction(DB, {
                readSeq,
                isolation,
                batch: {
                  operations: [
                    { _tag: "Put", key: key(k), value: Uint8Array.of(0), ttlMillis: Option.none() },
                  ],
                },
                readKeys: [key(1), key(2)],
                readRanges: [],
              }),
            );
          const first = yield* commit(t1, 1);
          const second = yield* commit(t2, 2);
          return [first._tag, second._tag === "Failure" ? second.failure._tag : "Success"];
        }),
      );
    expect(await Effect.runPromise(skew("Snapshot"))).toEqual(["Success", "Success"]);
    expect(await Effect.runPromise(skew("SerializableSnapshot"))).toEqual([
      "Success",
      "TransactionConflictError",
    ]);
  });

  it("reads its own writes and merges, scans with them applied, and commits atomically", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* createDatabase;
          const client = yield* DbClient;
          yield* client.put(DB, key(1), Uint8Array.of(1));
          yield* client.put(DB, key(3), Uint8Array.of(3));
          const result = yield* client.transaction(DB, "SerializableSnapshot", (txn) =>
            Effect.gen(function* () {
              yield* txn.merge(key(1), Uint8Array.of(9));
              const merged = yield* txn.get(key(1));
              yield* txn.put(key(2), Uint8Array.of(2));
              yield* txn.delete(key(3));
              const scanned = yield* txn.scan({ start: key(0), end: Option.none() }, 10);
              return { merged, scanned: scanned.map((row) => row.key[1]) };
            }),
          );
          const after = yield* client.get(DB, key(1), {
            consistency: { _tag: "Latest" },
            snapshot: Option.none(),
            refuseSealed: false,
          });
          const three = yield* client.get(DB, key(3), {
            consistency: { _tag: "Latest" },
            snapshot: Option.none(),
            refuseSealed: false,
          });
          return { result, after, three };
        }),
      ),
    );
    expect(outcome.result.value.merged).toEqual(Option.some(Uint8Array.of(1, 9)));
    expect(outcome.result.value.scanned).toEqual([1, 2]);
    expect(Option.isSome(outcome.result.receipt)).toBe(true);
    expect(outcome.after).toEqual(Option.some(Uint8Array.of(1, 9)));
    expect(outcome.three).toEqual(Option.none());
  });

  it("reads and commits exactly across flushes and a compaction, from its begin manifest", async () => {
    const outcome = await Effect.runPromise(
      provideWith({ transactionRetentionMs: 60_000 })(
        Effect.gen(function* () {
          yield* createDatabase;
          const client = yield* DbClient;
          yield* client.put(DB, key(1), Uint8Array.of(1));
          const begin = yield* client.beginTransaction(DB);
          // Newer writes, flushed and compacted while the transaction is open.
          yield* client.put(DB, key(1), Uint8Array.of(7));
          yield* client.flush(DB);
          yield* client.put(DB, key(2), Uint8Array.of(2));
          yield* client.flush(DB);
          yield* compact;
          const read = yield* client.getAt(DB, key(1), begin.readSeq, begin.manifestId);
          const scanned = yield* client.scanAt(
            DB,
            { start: key(0), end: Option.some(key(9)) },
            begin.readSeq,
            10,
            begin.manifestId,
          );
          // It read key 1, which changed since: a serializable commit refuses, as it must.
          const conflict = yield* Effect.flip(
            client.commitTransaction(DB, {
              readSeq: begin.readSeq,
              isolation: "SerializableSnapshot",
              batch: { operations: [{ _tag: "Delete", key: key(9) }] },
              readKeys: [key(1)],
              readRanges: [],
            }),
          );
          // One that read only untouched keys commits, though data it never saw was flushed.
          const committed = yield* client.commitTransaction(DB, {
            readSeq: begin.readSeq,
            isolation: "SerializableSnapshot",
            batch: { operations: [{ _tag: "Delete", key: key(9) }] },
            readKeys: [key(5)],
            readRanges: [],
          });
          return {
            read: Option.map(read, (row) => row.value),
            scanned: scanned.map((row) => row.key[1]),
            conflict:
              conflict._tag === "TransactionConflictError" ? conflict.reason : conflict._tag,
            committed: committed.seq > begin.readSeq,
          };
        }),
      ),
    );
    expect(Option.map(outcome.read, (value) => value._tag === "Value" && value.bytes[0])).toEqual(
      Option.some(1),
    );
    expect(outcome.scanned).toEqual([1]);
    expect(outcome.conflict).toBe("ReadConflict");
    expect(outcome.committed).toBe(true);
  });

  it("a scan by a caller that sends no begin manifest reads rows flushed since the reader cached its manifest", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* createDatabase;
          const client = yield* DbClient;
          yield* client.put(DB, key(1), Uint8Array.of(1));
          // The reader caches the manifest from before any flush.
          yield* client.get(DB, key(1), {
            consistency: { _tag: "Flushed" },
            snapshot: Option.none(),
            refuseSealed: false,
          });
          // The transaction begins, then the row is flushed: the writer's pages for a caller that
          // sends no begin manifest (one from before D58) no longer hold it.
          const begin = yield* client.beginTransaction(DB);
          yield* client.flush(DB);
          const scanned = yield* client.scanAt(
            DB,
            FULL_KEY_RANGE,
            begin.readSeq,
            10,
            Option.none(),
          );
          return scanned.map((row) => row.key);
        }),
      ),
    );
    expect(outcome).toEqual([key(1)]);
  });

  it("ends a transaction only once its rows are trimmed, and old callers at the first flush", async () => {
    const outcome = await Effect.runPromise(
      provideWith({ transactionRetentionMs: 1_000 })(
        Effect.gen(function* () {
          yield* createDatabase;
          const client = yield* DbClient;
          yield* client.put(DB, key(1), Uint8Array.of(1));
          const begin = yield* client.beginTransaction(DB);
          yield* client.put(DB, key(2), Uint8Array.of(2));
          yield* client.flush(DB);
          // Within retention: exact with the manifest; a caller without it (before D58) is too old.
          const kept = yield* client.getAt(DB, key(1), begin.readSeq, begin.manifestId);
          const legacy = yield* Effect.flip(client.getAt(DB, key(1), begin.readSeq, Option.none()));
          // Past retention, a later flush trims the rows after readSeq: too old, at read and commit.
          yield* TestClock.adjust("2 seconds");
          yield* client.put(DB, key(3), Uint8Array.of(3));
          yield* client.flush(DB);
          const read = yield* Effect.flip(
            client.getAt(DB, key(1), begin.readSeq, begin.manifestId),
          );
          const commit = yield* Effect.flip(
            client.commitTransaction(DB, {
              readSeq: begin.readSeq,
              isolation: "Snapshot",
              batch: { operations: [{ _tag: "Delete", key: key(9) }] },
              readKeys: [],
              readRanges: [],
            }),
          );
          return {
            kept: Option.isSome(kept),
            errors: [legacy, read, commit].map((error) =>
              error._tag === "TransactionConflictError" ? error.reason : error._tag,
            ),
          };
        }),
      ),
    );
    expect(outcome).toEqual({
      kept: true,
      errors: ["SnapshotTooOld", "SnapshotTooOld", "SnapshotTooOld"],
    });
  });
});
