import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  dropSstFromManifest,
  fenceCompactor,
  FULL_KEY_RANGE,
  initialManifest,
  type ReadConsistency,
  SeqNum,
  Timestamp,
} from "@shaledb/lsm-core";
import { Arbitrary, Bytes, Effect, Option, Result, Schema, Stream, Tracer } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { DbClient, dbClientTestLayer, makeDbClient } from "./db-client.ts";
import {
  type DbReader,
  type DbReaderServices,
  SCAN_READ_AHEAD_BLOCKS,
  scanFirstReadAheadBlocks,
} from "./db-reader.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import { sstKey } from "./object-keys.ts";
import { ObjectStore } from "./object-store.ts";
import { WriterClient } from "./writer-client.ts";

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
    | DbClient
    | DbReader
    | WriterClient
    | ManifestStore
    | ManifestReader
    | ObjectStore
    | DbReaderServices
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
        // At least 20 steps: left to itself the generator makes histories of a step or two,
        // too short for a key to be written twice between two flushes and then read.
        Arbitrary.array(stepArbitrary, { minLength: 20, maxLength: 60 }),
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

  it("sizes a scan's first read-ahead from its limit and the SST's rows per block (D72)", () => {
    const sst = (rowCount: number, blockCount: number) => ({ rowCount, blockCount });
    // No limit: everything is wanted, so read ahead fully.
    expect(scanFirstReadAheadBlocks(Option.none(), sst(10_000, 100))).toBe(SCAN_READ_AHEAD_BLOCKS);
    // 100 rows at 130 rows per block: one block, and one more for a range starting mid-block.
    expect(scanFirstReadAheadBlocks(Option.some(100), sst(13_000, 100))).toBe(2);
    // 100 rows at 16 rows per block (1 KB values in 16 KiB blocks): seven blocks and one more.
    expect(scanFirstReadAheadBlocks(Option.some(100), sst(1_600, 100))).toBe(8);
    // A full page of 1,000 small rows: capped.
    expect(scanFirstReadAheadBlocks(Option.some(1000), sst(13_000, 100))).toBe(
      SCAN_READ_AHEAD_BLOCKS,
    );
    expect(scanFirstReadAheadBlocks(Option.some(1), sst(13_000, 100))).toBe(2);
    // Degenerate metadata never asks for less than one block or more than the cap.
    expect(scanFirstReadAheadBlocks(Option.some(100), sst(0, 0))).toBe(SCAN_READ_AHEAD_BLOCKS);
  });

  it("a short scan reads only the blocks its limit needs, and reads on when rows turn out hidden (D72)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        const client = yield* DbClient;
        // One SST of 120 small rows in 64-byte blocks: dozens of blocks, a few rows each.
        for (let key = 1; key <= 120; key++) {
          yield* client.put(DB, keyBytes(key), Uint8Array.of(key));
        }
        yield* client.flush(DB);
        const scan = (limit: number) =>
          Effect.gen(function* () {
            const blockReads: Array<string> = [];
            const recording = Tracer.make({
              span: (options) => {
                if (options.name === "BlockSource.block") blockReads.push(options.name);
                return new Tracer.NativeSpan(options);
              },
            });
            const rows = yield* Effect.withTracer(
              Stream.runCollect(
                client.scan(DB, FULL_KEY_RANGE, {
                  consistency: { _tag: "Flushed" },
                  snapshot: Option.none(),
                  refuseSealed: false,
                  limit: Option.some(limit),
                }),
              ),
              recording,
            );
            return { keys: rows.map((row) => row.key[1]), blockReads: blockReads.length };
          });
        const short = yield* scan(3);
        // A second SST deletes the first 60 keys: the scan's first three visible rows are far
        // past the blocks its limit suggested.
        for (let key = 1; key <= 60; key++) yield* client.delete(DB, keyBytes(key));
        yield* client.flush(DB);
        yield* TestClock.adjust("2 seconds");
        const pastDeletes = yield* scan(3);
        return { short, pastDeletes: pastDeletes.keys };
      }),
    );
    expect(outcome.short.keys).toEqual([1, 2, 3]);
    // The block holding the three rows and the one after it. The merge used to fill a batch of
    // 256 rows before emitting any, which read all 40 blocks of this SST.
    expect(outcome.short.blockReads).toBe(2);
    expect(outcome.pastDeletes).toEqual([61, 62, 63]);
  });

  it("a snapshot is taken at the newest manifest, not at one this reader still holds (D76)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        const client = yield* DbClient;
        const store = yield* ManifestStore;
        yield* client.put(DB, keyBytes(1), Uint8Array.of(1));
        yield* client.flush(DB);
        // A Latest read leaves the flush's manifest in this reader's cache, and vouches for it.
        yield* client.get(DB, keyBytes(1), latest);
        const held = (yield* (yield* ManifestReader).loadLatest(DB)).id;
        // Another commit replaces it (a compaction would): GC's clock for the held manifest
        // starts now, while the cache goes on serving it to reads.
        yield* store.update(DB, (manifest) => Result.succeed(fenceCompactor(manifest)));
        const newest = (yield* (yield* ManifestReader).loadLatest(DB)).id;
        const snapshot = yield* client.snapshot(DB);
        return { held, newest, snapshot: snapshot.manifestId, seq: snapshot.snapshotSeq };
      }),
    );
    expect(outcome.newest).toBeGreaterThan(outcome.held);
    expect(outcome.snapshot).toBe(outcome.newest);
    expect(outcome.seq).toBe(1);
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

  /**
   * A client whose writer is overtaken after it answers: a newer version of key 1 is written and
   * flushed, and every older SST leaves the manifest, as when a compaction keeps only each key's
   * newest version. `times` is how many times that happens. With `alsoBeforeAsking` it also
   * happens before the writer is asked, so the manifest a previous attempt cached is behind too.
   * `rewrittenKey` is the key rewritten instead of key 1.
   */
  const clientOvertakenAfterWriterAnswers = (
    times: number,
    alsoBeforeAsking = false,
    rewrittenKey: Uint8Array = keyBytes(1),
  ) =>
    Effect.gen(function* () {
      const client = yield* DbClient;
      const writer = yield* WriterClient;
      const store = yield* ManifestStore;
      const manifests = yield* ManifestReader;
      let overtaken = 0;
      const overtake = Effect.gen(function* () {
        if (overtaken >= times) return;
        overtaken += 1;
        yield* client.put(DB, rewrittenKey, Uint8Array.of(100 + overtaken));
        yield* client.flush(DB);
        const { manifest } = yield* manifests.loadLatest(DB);
        for (const older of manifest.l0.slice(1)) {
          yield* store.update(DB, (current) =>
            dropSstFromManifest(current, {
              sstId: older.id,
              reason: "Forced",
              droppedAt: Timestamp.make(0),
            }),
          );
        }
      }).pipe(Effect.orDie);
      return yield* makeDbClient.pipe(
        Effect.provideService(
          WriterClient,
          WriterClient.of({
            ...writer,
            probe: (databaseId, key) =>
              (alsoBeforeAsking ? overtake : Effect.void).pipe(
                Effect.andThen(Effect.tap(writer.probe(databaseId, key), () => overtake)),
              ),
            scan: (databaseId, request) =>
              Effect.tap(writer.scan(databaseId, request), () => overtake),
          }),
        ),
      );
    });

  const latest = {
    consistency: { _tag: "Latest" as const },
    snapshot: Option.none(),
    refuseSealed: false,
  };

  it("a Latest read that finds only a manifest flushed past its snapshot asks the writer again, never missing the key (D70)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        const client = yield* DbClient;
        yield* client.put(DB, keyBytes(1), Uint8Array.of(1));
        yield* client.flush(DB);
        // The writer answers "everything up to sequence number 1 is flushed". Before the reader
        // loads a manifest, the key is rewritten, flushed, and compacted: the newest manifest
        // holds only the new version, which the read's snapshot does not include.
        const get = yield* (yield* clientOvertakenAfterWriterAnswers(1)).get(
          DB,
          keyBytes(1),
          latest,
        );
        yield* client.put(DB, keyBytes(1), Uint8Array.of(2));
        yield* client.flush(DB);
        const scan = yield* Stream.runCollect(
          (yield* clientOvertakenAfterWriterAnswers(1)).scan(DB, FULL_KEY_RANGE, {
            ...latest,
            limit: Option.none(),
          }),
        );
        // Overtaken on every attempt, the read gives up with a failure the caller can retry.
        const overtakenEachTime = yield* (yield* clientOvertakenAfterWriterAnswers(10, true))
          .get(DB, keyBytes(1), latest)
          .pipe(Effect.result);
        return { get, scan: scan.map((row) => row.value), overtakenEachTime };
      }),
    );
    // The value written while the read was in flight: never "not found", which no moment had.
    expect(outcome.get).toEqual(Option.some(Uint8Array.of(101)));
    expect(outcome.scan).toEqual([{ _tag: "Value", bytes: Uint8Array.of(101) }]);
    expect(
      Result.isFailure(outcome.overtakenEachTime) && [
        outcome.overtakenEachTime.failure._tag,
        "reason" in outcome.overtakenEachTime.failure && outcome.overtakenEachTime.failure.reason,
      ],
    ).toEqual(["ReaderUnavailableError", "StorageUnavailable"]);
  });

  it("a Latest scan overtaken by a flush between its pages of unflushed rows starts again (D70)", async () => {
    const wideKey = (index: number) => Uint8Array.of(0x6b, index >> 8, index & 0xff);
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        const client = yield* DbClient;
        // More unflushed rows than one page from the writer holds (1,000), in one batch.
        const keys = 1200;
        yield* client.write(DB, {
          operations: Array.from({ length: keys }, (_, index) => ({
            _tag: "Put" as const,
            key: wideKey(index),
            value: Uint8Array.of(1),
            ttlMillis: Option.none(),
          })),
        });
        // After the first page: a key of the second page is rewritten and everything is flushed.
        // The second page then holds no row, and the SST holds only the rewritten key's new
        // version, which is past the scan's snapshot.
        const rewritten = wideKey(1100);
        const rows = yield* Stream.runCollect(
          (yield* clientOvertakenAfterWriterAnswers(1, false, rewritten)).scan(DB, FULL_KEY_RANGE, {
            ...latest,
            limit: Option.none(),
          }),
        );
        const rewrittenRow = rows.find((row) => Bytes.Equivalence(row.key, rewritten));
        return { rows: rows.length, rewritten: rewrittenRow?.value };
      }),
    );
    // Every key, the rewritten one included: at the first snapshot it had a value too.
    expect(outcome).toEqual({
      rows: 1200,
      rewritten: { _tag: "Value", bytes: Uint8Array.of(101) },
    });
  });
});
