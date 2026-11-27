import {
  type CompactionJob,
  type CompactorEpoch,
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  type ManifestId,
  planCompactions,
  type Row,
  type SeqNum,
  type SstHandle,
} from "@shaledb/lsm-core";
import { Arbitrary, Effect, Layer, Option, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { CompactionExecutor, makeCompactionExecutor } from "./compaction-executor.ts";
import { DbClient, dbClientTestLayer } from "./db-client.ts";
import { DbReader, type DbReaderServices } from "./db-reader.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import { ObjectStore } from "./object-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";

/*
 * Every way to read a key agrees with an oracle of the writes (D135): a `Latest` get and scan, and
 * a transaction's getAt and scanAt at a sequence number taken earlier, across merges, flushes,
 * compactions (which fold operands), and trims of the writer's retained rows. A read that takes a
 * version from both the writer and the manifest applies an operand twice; one that drops a
 * compaction's fold for the writer's copy loses the operands below it.
 */

const DB = DatabaseId.make("orders");
const key = (n: number) => Uint8Array.of(0x6b, n);
const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));
const hexOf = (bytes: Uint8Array): string =>
  Array.from(bytes, (part) => part.toString(16).padStart(2, "0")).join("");

type TestServices =
  | CompactionExecutor
  | DbClient
  | DbReader
  | ManifestStore
  | ManifestReader
  | ObjectStore
  | DbReaderServices;

/** Flushed rows stay 3 s (the test clock) and at most ~120 bytes, so trims happen often (D58). */
const provide = <A, E>(program: Effect.Effect<A, E, TestServices>) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(
      Layer.effect(CompactionExecutor, makeCompactionExecutor).pipe(
        Layer.provide(sstStoreLayerWithoutDependencies),
        Layer.provideMerge(
          dbClientTestLayer({
            blockSize: 64,
            transactionRetentionMs: 3_000,
            maxRetainedBytes: 120,
          }),
        ),
      ),
    ),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
  );

let jobCounter = 0;

/** Compacts L0 (and the sorted run) the way the Workflow does, when there is anything to compact. */
const compact = Effect.gen(function* () {
  const { manifest } = yield* (yield* ManifestReader).loadLatest(DB);
  const [job] = planCompactions(manifest, [], {
    minCompactionSources: 2,
    maxCompactionSources: 8,
    includeSizeThreshold: 1_000_000,
    maxConcurrentCompactions: 1,
  });
  if (job === undefined) return;
  yield* runJob(job, manifest.compactorEpoch);
});

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
    return yield* executor.commit(prepared, outputs, `agreement-job-${++jobCounter}`);
  });

type Step =
  | { readonly _tag: "Put" | "Merge"; readonly key: number; readonly value: number }
  | { readonly _tag: "Delete"; readonly key: number }
  | { readonly _tag: "Flush" | "Compact" | "Tick" }
  | { readonly _tag: "Begin"; readonly slot: number }
  | { readonly _tag: "ReadLatest"; readonly key: number }
  | { readonly _tag: "ReadAt"; readonly slot: number; readonly key: number };

const stepArbitrary: Arbitrary.Arbitrary<Step> = Arbitrary.all({
  kind: smallInt(0, 19),
  key: smallInt(0, 1),
  slot: smallInt(0, 1),
  value: smallInt(1, 250),
}).pipe(
  Arbitrary.map(({ kind, key, slot, value }): Step => {
    if (kind <= 1) return { _tag: "Put", key, value };
    if (kind <= 6) return { _tag: "Merge", key, value };
    if (kind === 7) return { _tag: "Delete", key };
    if (kind <= 9) return { _tag: "Flush" };
    if (kind <= 11) return { _tag: "Compact" };
    if (kind <= 12) return { _tag: "Tick" };
    if (kind <= 13) return { _tag: "Begin", slot };
    if (kind <= 15) return { _tag: "ReadLatest", key };
    return { _tag: "ReadAt", slot, key };
  }),
);

/** Each key's value as hex; absent keys are not in the map. */
type Oracle = ReadonlyMap<number, string>;

interface Begun {
  readonly readSeq: SeqNum;
  readonly manifestId: Option.Option<ManifestId>;
  readonly oracle: Oracle;
}

const rowHex = (row: Option.Option<Row>): string | undefined =>
  Option.getOrUndefined(
    Option.flatMap(row, (visible) =>
      visible.value._tag === "Value" ? Option.some(hexOf(visible.value.bytes)) : Option.none(),
    ),
  );

const pointRange = (n: number) => ({ start: key(n), end: Option.some(key(n + 1)) });

/** Runs `steps`; true when every read matched the oracle at its sequence number. */
const readsAgree = (steps: ReadonlyArray<Step>) =>
  provide(
    Effect.gen(function* () {
      yield* (yield* ManifestStore).initialize(
        initialManifest(DB, {
          keyRange: Option.none(),
          databaseOptions: { ...DEFAULT_DATABASE_OPTIONS, mergeOperator: Option.some("Concat") },
        }),
      );
      const client = yield* DbClient;
      const oracle = new Map<number, string>();
      const begun = new Map<number, Begun>();
      const latest = {
        consistency: { _tag: "Latest" } as const,
        snapshot: Option.none(),
        refuseSealed: false,
      };
      for (const step of steps) {
        switch (step._tag) {
          case "Put":
            yield* client.put(DB, key(step.key), Uint8Array.of(step.value));
            oracle.set(step.key, hexOf(Uint8Array.of(step.value)));
            break;
          case "Merge":
            yield* client.merge(DB, key(step.key), Uint8Array.of(step.value));
            oracle.set(step.key, (oracle.get(step.key) ?? "") + hexOf(Uint8Array.of(step.value)));
            break;
          case "Delete":
            yield* client.delete(DB, key(step.key));
            oracle.delete(step.key);
            break;
          case "Flush":
            yield* client.flush(DB);
            yield* TestClock.adjust("1 second");
            break;
          case "Compact":
            yield* compact;
            break;
          case "Tick":
            yield* TestClock.adjust("2 seconds");
            break;
          case "Begin": {
            const begin = yield* client.beginTransaction(DB);
            begun.set(step.slot, { ...begin, oracle: new Map(oracle) });
            break;
          }
          case "ReadLatest": {
            const want = oracle.get(step.key);
            const got = yield* client.getRow(DB, key(step.key), latest);
            const scanned = yield* Stream.runCollect(
              client.scan(DB, pointRange(step.key), { ...latest, limit: Option.none() }),
            );
            if (rowHex(got) !== want) return false;
            if (rowHex(Option.fromUndefinedOr(scanned[0])) !== want || scanned.length > 1) {
              return false;
            }
            break;
          }
          case "ReadAt": {
            const at = begun.get(step.slot);
            if (at === undefined) break;
            const want = at.oracle.get(step.key);
            const got = yield* Effect.result(
              client.getAt(DB, key(step.key), at.readSeq, at.manifestId),
            );
            const scanned = yield* Effect.result(
              client.scanAt(DB, pointRange(step.key), at.readSeq, 10, at.manifestId),
            );
            // A transaction older than the writer's retention may no longer be read.
            if (got._tag === "Failure" || scanned._tag === "Failure") {
              const failure =
                got._tag === "Failure"
                  ? got.failure
                  : scanned._tag === "Failure"
                    ? scanned.failure
                    : undefined;
              if (failure?._tag !== "TransactionConflictError") return false;
              begun.delete(step.slot);
              break;
            }
            if (rowHex(got.success) !== want) return false;
            const [first, ...rest] = scanned.success;
            if (rowHex(Option.fromUndefinedOr(first)) !== want || rest.length > 0) return false;
            break;
          }
        }
      }
      return true;
    }),
  );

/** The getAt, scanAt, and Latest get of key 0 at a transaction begun after `setup`. */
const readsAfter = (setup: Effect.Effect<void, unknown, TestServices>) =>
  Effect.runPromise(
    provide(
      Effect.gen(function* () {
        yield* (yield* ManifestStore).initialize(
          initialManifest(DB, {
            keyRange: Option.none(),
            databaseOptions: { ...DEFAULT_DATABASE_OPTIONS, mergeOperator: Option.some("Concat") },
          }),
        );
        yield* setup;
        const client = yield* DbClient;
        const begin = yield* client.beginTransaction(DB);
        const getAt = yield* client.getAt(DB, key(0), begin.readSeq, begin.manifestId);
        const scanAt = yield* client.scanAt(DB, pointRange(0), begin.readSeq, 10, begin.manifestId);
        const latest = yield* client.getRow(DB, key(0), {
          consistency: { _tag: "Latest" },
          snapshot: Option.none(),
          refuseSealed: false,
        });
        return {
          getAt: rowHex(getAt),
          scanAt: scanAt.map((row) => rowHex(Option.some(row))),
          latest: rowHex(latest),
        };
      }),
    ),
  );

describe("reads agree", () => {
  it("applies an operand the writer still keeps after its flush once (issue #1)", async () => {
    const reads = await readsAfter(
      Effect.gen(function* () {
        const client = yield* DbClient;
        yield* client.merge(DB, key(0), Uint8Array.of(1));
        yield* client.merge(DB, key(0), Uint8Array.of(2));
        yield* client.flush(DB);
      }),
    );
    expect(reads).toEqual({ getAt: "0102", scanAt: ["0102"], latest: "0102" });
  });

  it("keeps the operands a compaction folded below an operand the writer still keeps", async () => {
    const reads = await readsAfter(
      Effect.gen(function* () {
        const client = yield* DbClient;
        yield* client.merge(DB, key(0), Uint8Array.of(1));
        yield* client.flush(DB);
        // Past the retention: the next flush trims operand 1 from the writer, keeping operand 2.
        yield* TestClock.adjust("4 seconds");
        yield* client.merge(DB, key(0), Uint8Array.of(2));
        yield* client.flush(DB);
        // Folds both operands into one row at operand 2's sequence number.
        yield* compact;
      }),
    );
    expect(reads).toEqual({ getAt: "0102", scanAt: ["0102"], latest: "0102" });
  });

  it("a Latest get and scan and a transaction's getAt and scanAt each read what the writes left, across merges, flushes, compactions, and trims (D135)", async () => {
    const result = await Effect.runPromise(
      Arbitrary.checkEffect(
        Arbitrary.array(stepArbitrary, { minLength: 20, maxLength: 60 }),
        readsAgree,
        { runs: 300 },
      ),
    );
    const failure = Arbitrary.formatCheckFailure(result);
    if (failure !== undefined) throw new Error(failure);
  });
});
