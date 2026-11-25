import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  type SeqNum,
  type TransactionIsolation,
} from "@cf-slatedb/lsm-core";
import { Arbitrary, Effect, Option, Schema } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { DbClient, dbClientTestLayer } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestStore } from "./manifest-store.ts";

const DB = DatabaseId.make("orders");
const key = (n: number) => Uint8Array.of(0x6b, n);
const byte = (value: Option.Option<Uint8Array>) =>
  Option.getOrUndefined(Option.map(value, (bytes) => bytes[0]));
const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

const provide = <A, E>(program: Effect.Effect<A, E, DbClient | ManifestStore | DbReaderServices>) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(dbClientTestLayer({ blockSize: 64 })),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
  );

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
  | { readonly _tag: "Flush" };

const stepArbitrary: Arbitrary.Arbitrary<Step> = Arbitrary.all({
  kind: smallInt(0, 19),
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
    return { _tag: "Flush" };
  }),
);

interface Running {
  readonly readSeq: SeqNum;
  readonly startedAt: number;
  readonly reads: Map<number, number | undefined>;
  readonly writes: Map<number, number>;
}

/**
 * Runs a random interleaving of transactions (through the stateless primitives), direct writes, and
 * flushes against an oracle that applies commits in commit order.
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
                readSeq: yield* client.beginTransaction(DB),
                startedAt: clock,
                reads: new Map(),
                writes: new Map(),
              });
            }
            break;
          case "Read": {
            const txn = running.get(step.txn);
            if (txn === undefined || txn.writes.has(step.key)) break;
            const row = yield* Effect.result(client.getAt(DB, key(step.key), txn.readSeq));
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
            yield* TestClock.adjust("2 seconds");
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
          const commit = (readSeq: SeqNum, k: number) =>
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
          });
          const three = yield* client.get(DB, key(3), {
            consistency: { _tag: "Latest" },
            snapshot: Option.none(),
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

  it("refuses a transaction whose snapshot was flushed past, at read and at commit", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* createDatabase;
          const client = yield* DbClient;
          yield* client.put(DB, key(1), Uint8Array.of(1));
          const readSeq = yield* client.beginTransaction(DB);
          yield* client.put(DB, key(2), Uint8Array.of(2));
          yield* client.flush(DB);
          const read = yield* Effect.flip(client.getAt(DB, key(1), readSeq));
          const commit = yield* Effect.flip(
            client.commitTransaction(DB, {
              readSeq,
              isolation: "Snapshot",
              batch: { operations: [{ _tag: "Delete", key: key(9) }] },
              readKeys: [],
              readRanges: [],
            }),
          );
          return [read, commit].map((error) =>
            error._tag === "TransactionConflictError" ? error.reason : error._tag,
          );
        }),
      ),
    );
    expect(outcome).toEqual(["SnapshotTooOld", "SnapshotTooOld"]);
  });
});
