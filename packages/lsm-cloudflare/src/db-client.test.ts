import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  dropSstFromManifest,
  FULL_KEY_RANGE,
  initialManifest,
  type ReadConsistency,
  SeqNum,
  Timestamp,
} from "@shaledb/lsm-core";
import { Arbitrary, Effect, Option, Result, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { DbClient, dbClientTestLayer } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import { sstKey } from "./object-keys.ts";
import { ObjectStore } from "./object-store.ts";

const DB = DatabaseId.make("orders");
const hex = (bytes: Uint8Array) =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

/** One step of a random workload. */
type Step =
  | { readonly _tag: "Put"; readonly key: number; readonly value: number }
  | { readonly _tag: "Delete"; readonly key: number }
  | { readonly _tag: "Merge"; readonly key: number; readonly value: number }
  | { readonly _tag: "Flush" }
  | { readonly _tag: "Read"; readonly key: number; readonly consistency: number };

const stepArbitrary: Arbitrary.Arbitrary<Step> = Arbitrary.all({
  kind: smallInt(0, 9),
  key: smallInt(0, 5),
  value: smallInt(0, 255),
  consistency: smallInt(0, 2),
}).pipe(
  Arbitrary.map(({ kind, key, value, consistency }): Step => {
    if (kind <= 2) return { _tag: "Put", key, value };
    if (kind === 3) return { _tag: "Delete", key };
    if (kind <= 5) return { _tag: "Merge", key, value };
    if (kind === 6) return { _tag: "Flush" };
    return { _tag: "Read", key, consistency };
  }),
);

const keyBytes = (key: number) => Uint8Array.of(0x6b, key);

/** The Map oracle: applies writes in order with the Concat merge operator. */
const applyToOracle = (oracle: Map<number, Uint8Array>, step: Step) => {
  if (step._tag === "Put") oracle.set(step.key, Uint8Array.of(step.value));
  if (step._tag === "Delete") oracle.delete(step.key);
  if (step._tag === "Merge") {
    const existing = oracle.get(step.key) ?? new Uint8Array(0);
    oracle.set(step.key, Uint8Array.of(...existing, step.value));
  }
};

const run = <A, E>(
  program: Effect.Effect<
    A,
    E,
    DbClient | ManifestStore | ManifestReader | ObjectStore | DbReaderServices
  >,
) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(dbClientTestLayer({ blockSize: 64 })),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
    Effect.runPromise,
  );

const createDatabase = Effect.gen(function* () {
  const store = yield* ManifestStore;
  yield* store.initialize(
    initialManifest(DB, {
      keyRange: Option.none(),
      databaseOptions: { ...DEFAULT_DATABASE_OPTIONS, mergeOperator: Option.some("Concat") },
    }),
  );
});

describe("DbClient against a Map oracle", () => {
  it("Latest and AtLeast reads see every acknowledged write; Flushed reads see the last flush; scans agree", async () => {
    const result = await Effect.runPromise(
      Arbitrary.checkEffect(
        Arbitrary.array(stepArbitrary, { minLength: 1, maxLength: 40 }),
        (steps) =>
          Effect.gen(function* () {
            yield* createDatabase;
            const client = yield* DbClient;
            const latest = new Map<number, Uint8Array>();
            let flushed = new Map<number, Uint8Array>();
            let lastSeq = SeqNum.make(0);
            for (const step of steps) {
              switch (step._tag) {
                case "Put":
                  lastSeq = (yield* client.put(DB, keyBytes(step.key), Uint8Array.of(step.value)))
                    .seq;
                  break;
                case "Delete":
                  lastSeq = (yield* client.delete(DB, keyBytes(step.key))).seq;
                  break;
                case "Merge":
                  lastSeq = (yield* client.merge(DB, keyBytes(step.key), Uint8Array.of(step.value)))
                    .seq;
                  break;
                case "Flush":
                  yield* client.flush(DB);
                  flushed = new Map(latest);
                  // Flushed reads may use a cached manifest for up to manifestRefreshInterval
                  // ("a stale manifest only makes it older, never wrong"); let it pass so the
                  // oracle's flushed state is the one a Flushed read must see.
                  yield* TestClock.adjust("2 seconds");
                  break;
                case "Read": {
                  const consistency: ReadConsistency =
                    step.consistency === 0
                      ? { _tag: "Latest" }
                      : step.consistency === 1
                        ? { _tag: "AtLeast", seq: lastSeq }
                        : { _tag: "Flushed" };
                  const got = yield* client.get(DB, keyBytes(step.key), {
                    consistency,
                    snapshot: Option.none(),
                    refuseSealed: false,
                  });
                  const want = (consistency._tag === "Flushed" ? flushed : latest).get(step.key);
                  if (
                    hex(Option.getOrElse(got, () => Uint8Array.of(0xde, 0xad))) !==
                    hex(want ?? Uint8Array.of(0xde, 0xad))
                  ) {
                    return false;
                  }
                }
              }
              applyToOracle(latest, step);
            }
            // Full scans at each consistency agree with the oracle, in key order.
            for (const [consistency, oracle] of [
              [{ _tag: "Latest" } as const, latest],
              [{ _tag: "Flushed" } as const, flushed],
            ] as const) {
              const rows = yield* Stream.runCollect(
                client.scan(DB, FULL_KEY_RANGE, {
                  consistency,
                  snapshot: Option.none(),
                  refuseSealed: false,
                  limit: Option.none(),
                }),
              );
              const scanned = rows.map(
                (row) => `${row.key[1]}=${row.value._tag === "Value" ? hex(row.value.bytes) : "?"}`,
              );
              const expected = [...oracle.entries()]
                .sort(([a], [b]) => a - b)
                .map(([key, value]) => `${key}=${hex(value)}`);
              if (scanned.join(",") !== expected.join(",")) return false;
            }
            return true;
          }).pipe(
            Effect.scoped,
            Effect.provide(dbClientTestLayer({ blockSize: 64 })),
            Effect.provide(lsmTestRuntimeContextLayer),
            Effect.provide(TestClock.layer()),
          ),
        { runs: 150 },
      ),
    );
    const failure = Arbitrary.formatCheckFailure(result);
    if (failure !== undefined) throw new Error(failure);
  });

  it("a Flushed read is never newer than Latest and never older than the refresh interval allows", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        const client = yield* DbClient;
        const flushedRead = client.get(DB, keyBytes(1), {
          consistency: { _tag: "Flushed" },
          snapshot: Option.none(),
          refuseSealed: false,
        });
        const before = yield* flushedRead;
        const receipt = yield* client.put(DB, keyBytes(1), Uint8Array.of(9));
        yield* client.flush(DB);
        const stale = yield* flushedRead;
        const atLeast = yield* client.get(DB, keyBytes(1), {
          consistency: { _tag: "AtLeast", seq: receipt.seq },
          snapshot: Option.none(),
          refuseSealed: false,
        });
        yield* TestClock.adjust("2 seconds");
        const fresh = yield* flushedRead;
        return { before, stale, atLeast, fresh };
      }),
    );
    expect(outcome.before).toEqual(Option.none());
    // Within the refresh interval the cached manifest may predate the flush: older, never wrong.
    expect(outcome.stale).toEqual(Option.none());
    // AtLeast(seq) from the receipt always sees the write.
    expect(outcome.atLeast).toEqual(Option.some(Uint8Array.of(9)));
    expect(outcome.fresh).toEqual(Option.some(Uint8Array.of(9)));
  });

  it("an AtLeast read skips the writer only while a Flushed read would use the same manifest (D67)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        const client = yield* DbClient;
        const read = (consistency: ReadConsistency) =>
          client.get(DB, keyBytes(1), {
            consistency,
            snapshot: Option.none(),
            refuseSealed: false,
          });
        const first = yield* client.put(DB, keyBytes(1), Uint8Array.of(1));
        yield* client.flush(DB);
        // A Latest read leaves the manifest holding the flush in this reader's cache.
        yield* read({ _tag: "Latest" });
        yield* client.put(DB, keyBytes(1), Uint8Array.of(2));
        yield* client.flush(DB);
        // Within the refresh interval the cached manifest covers the receipt: no writer call, so
        // the read is as old as a Flushed read may be.
        const within = yield* read({ _tag: "AtLeast", seq: first.seq });
        // After it nothing vouches for the cached manifest, though it still covers the receipt
        // and is held far longer for Latest reads: the read asks the writer.
        yield* TestClock.adjust("2 seconds");
        const after = yield* read({ _tag: "AtLeast", seq: first.seq });
        return { within, after };
      }),
    );
    expect(outcome.within).toEqual(Option.some(Uint8Array.of(1)));
    expect(outcome.after).toEqual(Option.some(Uint8Array.of(2)));
  });

  it("a read that finds an SST of its cached manifest missing drops the manifest, so the retry succeeds (D67)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        const client = yield* DbClient;
        const latest = (key: number) =>
          client
            .get(DB, keyBytes(key), {
              consistency: { _tag: "Latest" },
              snapshot: Option.none(),
              refuseSealed: false,
            })
            .pipe(Effect.result);
        // Enough rows for several 64-byte blocks, so the first read leaves most blocks unread.
        for (let key = 1; key <= 40; key++) {
          yield* client.put(DB, keyBytes(key), Uint8Array.of(key));
        }
        yield* client.flush(DB);
        const cachedRead = yield* latest(1);
        // The SST leaves the manifest and is deleted, as after a compaction and a collection the
        // reader's cached manifest outlived.
        const { manifest } = yield* (yield* ManifestReader).loadLatest(DB);
        const [sst] = manifest.l0;
        if (sst === undefined) return yield* Effect.die("the flush wrote no L0 SST");
        yield* (yield* ManifestStore).update(DB, (current) =>
          dropSstFromManifest(current, {
            sstId: sst.id,
            reason: "Forced",
            droppedAt: Timestamp.make(0),
          }),
        );
        yield* (yield* ObjectStore).delete([sstKey(DB, sst.id)]);
        const missing = yield* latest(40);
        const retried = yield* latest(40);
        return { cachedRead, missing, retried };
      }),
    );
    expect(outcome.cachedRead).toEqual(Result.succeed(Option.some(Uint8Array.of(1))));
    expect(Result.isFailure(outcome.missing) && outcome.missing.failure._tag).toBe(
      "ReaderUnavailableError",
    );
    expect(outcome.retried).toEqual(Result.succeed(Option.none()));
  });

  it("reads a snapshot pinned to a manifest, unaffected by later writes and flushes", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        const client = yield* DbClient;
        yield* client.put(DB, keyBytes(1), Uint8Array.of(1));
        yield* client.flush(DB);
        const snapshot = yield* client.snapshot(DB);
        yield* client.put(DB, keyBytes(1), Uint8Array.of(2));
        yield* client.put(DB, keyBytes(2), Uint8Array.of(3));
        yield* client.flush(DB);
        const atSnapshot = yield* client.get(DB, keyBytes(1), {
          consistency: { _tag: "Latest" },
          snapshot: Option.some(snapshot),
          refuseSealed: false,
        });
        const prefix = yield* Stream.runCollect(
          client.scanPrefix(DB, Uint8Array.of(0x6b), {
            consistency: { _tag: "Flushed" },
            snapshot: Option.some(snapshot),
            refuseSealed: false,
            limit: Option.none(),
          }),
        );
        // A Flushed read may use the cached manifest until manifestRefreshInterval passes.
        yield* TestClock.adjust("2 seconds");
        const now = yield* client.get(DB, keyBytes(1), {
          consistency: { _tag: "Flushed" },
          snapshot: Option.none(),
          refuseSealed: false,
        });
        return { atSnapshot, prefix: prefix.length, now };
      }),
    );
    expect(outcome.atSnapshot).toEqual(Option.some(Uint8Array.of(1)));
    expect(outcome.prefix).toBe(1);
    expect(outcome.now).toEqual(Option.some(Uint8Array.of(2)));
  });
});
