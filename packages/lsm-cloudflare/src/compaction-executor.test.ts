import {
  type CompactionJob,
  CompactorEpoch,
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  fenceCompactor,
  FULL_KEY_RANGE,
  initialManifest,
  type Manifest,
  planCompactions,
  replaceManifestFields,
  SstId,
  SortedRunId,
  type SstHandle,
} from "@shaledb/lsm-core";
import { Arbitrary, Bytes, Effect, Layer, Option, Result, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import {
  CompactionExecutor,
  makeCompactionExecutor,
  PreparedCompaction,
} from "./compaction-executor.ts";
import { DbClient, dbClientTestLayer } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";

const DB = DatabaseId.make("orders");
const hex = (bytes: Uint8Array) =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

/** The client, its writer and stores, and an executor over the same in-memory bucket. */
const testLayer = Layer.effect(CompactionExecutor, makeCompactionExecutor).pipe(
  Layer.provide(sstStoreLayerWithoutDependencies),
  // Tiny blocks and outputs, so a compaction writes several SSTs of several blocks each.
  Layer.provideMerge(dbClientTestLayer({ blockSize: 48, compactionOutputSstSize: 96 })),
);

const provide = <A, E>(
  program: Effect.Effect<
    A,
    E,
    CompactionExecutor | DbClient | ManifestStore | ManifestReader | DbReaderServices
  >,
) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(testLayer),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
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

/** Runs a job the way the Workflow does: prepare, one output per step, then commit. */
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
    return { committed: yield* executor.commit(prepared, outputs), outputs };
  });

/** Whether every sorted run's SSTs are in key order and do not overlap. */
const runsAreSorted = (manifest: Manifest) =>
  manifest.sortedRuns.every((run) =>
    run.ssts.every(
      (sst, index) =>
        Bytes.Order(sst.firstKey, sst.lastKey) <= 0 &&
        (index === 0 ||
          Bytes.Order(run.ssts[index - 1]?.lastKey ?? sst.firstKey, sst.firstKey) < 0),
    ),
  );

type Step =
  | { readonly _tag: "Put"; readonly key: number; readonly value: number }
  | { readonly _tag: "Delete"; readonly key: number }
  | { readonly _tag: "Merge"; readonly key: number; readonly value: number }
  | { readonly _tag: "Flush" }
  | { readonly _tag: "Compact" };

const stepArbitrary: Arbitrary.Arbitrary<Step> = Arbitrary.all({
  kind: smallInt(0, 9),
  key: smallInt(0, 7),
  value: smallInt(0, 255),
}).pipe(
  Arbitrary.map(({ kind, key, value }): Step => {
    if (kind <= 2) return { _tag: "Put", key, value };
    if (kind === 3) return { _tag: "Delete", key };
    if (kind <= 5) return { _tag: "Merge", key, value };
    if (kind <= 7) return { _tag: "Flush" };
    return { _tag: "Compact" };
  }),
);

const keyBytes = (key: number) => Uint8Array.of(0x6b, key);

/** Cloudflare Workflows' largest non-stream step result: 1 MiB (Workflows limits page). */
const WORKFLOW_STEP_RESULT_LIMIT_BYTES = 2 ** 20;

describe("CompactionExecutor", () => {
  it("bounds a step by the input it merges: a step whose rows are all dropped ends early and writes nothing (D97)", async () => {
    // 200 values, all deleted: a bottom compaction keeps nothing, so a step bounded only by its
    // output would merge every input before ending.
    const keys = 200;
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* createDatabase;
          const client = yield* DbClient;
          const key = (index: number) => Uint8Array.of(0x6b, index);
          for (let index = 0; index < keys; index++) {
            yield* client.put(DB, key(index), new Uint8Array(40).fill(1));
          }
          yield* client.flush(DB);
          for (let index = 0; index < keys; index++) yield* client.delete(DB, key(index));
          yield* client.flush(DB);
          const { manifest } = yield* (yield* ManifestReader).loadLatest(DB);
          const job: CompactionJob = {
            sources: manifest.l0.map((sst) => ({ _tag: "L0" as const, sstId: sst.id })),
            destination: SortedRunId.make(0),
            isBottom: true,
          };
          const executor = yield* CompactionExecutor;
          const prepared = yield* executor.prepare(DB, manifest.compactorEpoch, job);
          const first = yield* executor.writeOutput(prepared, Option.none());
          // The whole job, step by step: every step merges a bounded share, the last says done.
          let steps = 1;
          let start = first.resumeFrom;
          const outputs: Array<SstHandle> = [...Option.toArray(first.output)];
          while (Option.isSome(start)) {
            const step = yield* executor.writeOutput(prepared, start);
            outputs.push(...Option.toArray(step.output));
            start = step.resumeFrom;
            steps++;
          }
          const committed = yield* executor.commit(prepared, outputs);
          const rows = yield* Stream.runCollect(
            client.scan(DB, FULL_KEY_RANGE, {
              consistency: { _tag: "Latest" },
              snapshot: Option.none(),
              refuseSealed: false,
              limit: Option.none(),
            }),
          );
          return {
            firstWrote: Option.isSome(first.output),
            firstResumes: Option.isSome(first.resumeFrom),
            steps,
            outputs: outputs.length,
            l0: committed.manifest.l0.length,
            rows: rows.length,
          };
        }),
      ),
    );
    // About 20,000 bytes of input against a 144-byte bound (1.5 x the 96-byte output target).
    expect(outcome.firstWrote).toBe(false);
    expect(outcome.firstResumes).toBe(true);
    expect(outcome.steps).toBeGreaterThan(10);
    expect(outcome.outputs).toBe(0);
    expect(outcome.l0).toBe(0);
    expect(outcome.rows).toBe(0);
  });

  it("prepares the largest merge the scheduler plans with a step result Workflows can store (D86)", async () => {
    // Eight sorted runs of 8 GiB each in 64 MiB SSTs (128 per run), with 1 KiB keys: all eight are
    // within `includeSizeThreshold` of each other, so the scheduler merges them in one job.
    const runs = 8;
    const sstsPerRun = 128;
    const key = (run: number, index: number, end: number) => {
      const bytes = new Uint8Array(1024).fill(0x61);
      bytes[0] = run;
      bytes[1] = index;
      bytes[2] = end;
      return bytes;
    };
    let counter = 0;
    const sstId = () => SstId.make(`01JD${String(counter++).padStart(22, "0")}`);
    const sortedRuns = Array.from({ length: runs }, (_, run) => ({
      id: SortedRunId.make(runs - run),
      ssts: Array.from({ length: sstsPerRun }, (_, index): SstHandle => ({
        id: sstId(),
        firstKey: key(run, index, 0),
        lastKey: key(run, index, 1),
        sizeBytes: 64 * 1024 * 1024,
        rowCount: 60_000,
        metaOffset: 64 * 1024 * 1024 - 65_536,
        metaLength: 65_536,
        owner: Option.none(),
        view: Option.none(),
      })),
    }));
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          const manifest = replaceManifestFields(
            initialManifest(DB, {
              keyRange: Option.none(),
              databaseOptions: DEFAULT_DATABASE_OPTIONS,
            }),
            { sortedRuns },
          );
          yield* (yield* ManifestStore).initialize(manifest);
          const [job] = planCompactions(manifest, [], {
            minCompactionSources: 4,
            maxCompactionSources: 8,
            includeSizeThreshold: 4,
            maxConcurrentCompactions: 1,
          });
          if (job === undefined) return yield* Effect.die("the scheduler planned nothing");
          const prepared = yield* (yield* CompactionExecutor).prepare(
            DB,
            manifest.compactorEpoch,
            job,
          );
          const encoded = JSON.stringify(Schema.encodeSync(PreparedCompaction)(prepared));
          return { sources: job.sources.length, bytes: new TextEncoder().encode(encoded).length };
        }),
      ),
    );
    expect(outcome.sources).toBe(runs);
    expect(outcome.bytes).toBeLessThanOrEqual(WORKFLOW_STEP_RESULT_LIMIT_BYTES);
  });

  it("changes nothing a read sees, at any consistency, across many flushes and compactions", async () => {
    const result = await Effect.runPromise(
      Arbitrary.checkEffect(
        Arbitrary.array(stepArbitrary, { minLength: 1, maxLength: 50 }),
        (steps) =>
          provide(
            Effect.gen(function* () {
              yield* createDatabase;
              const client = yield* DbClient;
              const reader = yield* ManifestReader;
              const oracle = new Map<number, Uint8Array>();
              for (const step of steps) {
                switch (step._tag) {
                  case "Put":
                    yield* client.put(DB, keyBytes(step.key), Uint8Array.of(step.value));
                    oracle.set(step.key, Uint8Array.of(step.value));
                    break;
                  case "Delete":
                    yield* client.delete(DB, keyBytes(step.key));
                    oracle.delete(step.key);
                    break;
                  case "Merge":
                    yield* client.merge(DB, keyBytes(step.key), Uint8Array.of(step.value));
                    oracle.set(
                      step.key,
                      Uint8Array.of(...(oracle.get(step.key) ?? []), step.value),
                    );
                    break;
                  case "Flush":
                    yield* client.flush(DB);
                    break;
                  case "Compact": {
                    const { manifest } = yield* reader.loadLatest(DB);
                    const [job] = planCompactions(manifest, [], {
                      minCompactionSources: 2,
                      maxCompactionSources: 4,
                      includeSizeThreshold: 4,
                      maxConcurrentCompactions: 1,
                    });
                    if (job === undefined) break;
                    const { committed } = yield* runJob(job, manifest.compactorEpoch);
                    if (!runsAreSorted(committed.manifest)) return false;
                  }
                }
              }
              // Flush everything, so Flushed reads see the whole history through the compacted LSM.
              yield* client.flush(DB);
              yield* TestClock.adjust("2 seconds");
              const expected = [...oracle.entries()]
                .sort(([a], [b]) => a - b)
                .map(([key, value]) => `${key}=${hex(value)}`)
                .join(",");
              for (const consistency of [
                { _tag: "Latest" } as const,
                { _tag: "Flushed" } as const,
              ]) {
                const rows = yield* Stream.runCollect(
                  client.scan(DB, FULL_KEY_RANGE, {
                    consistency,
                    snapshot: Option.none(),
                    refuseSealed: false,
                    limit: Option.none(),
                  }),
                );
                const scanned = rows
                  .map(
                    (row) =>
                      `${row.key[1]}=${row.value._tag === "Value" ? hex(row.value.bytes) : "?"}`,
                  )
                  .join(",");
                if (scanned !== expected) return false;
                for (let key = 0; key < 8; key++) {
                  const got = yield* client.get(DB, keyBytes(key), {
                    consistency,
                    snapshot: Option.none(),
                    refuseSealed: false,
                  });
                  if (
                    hex(Option.getOrElse(got, () => Uint8Array.of())) !==
                    hex(oracle.get(key) ?? Uint8Array.of())
                  )
                    return false;
                }
              }
              return true;
            }),
          ),
        { runs: 120 },
      ),
    );
    const failure = Arbitrary.formatCheckFailure(result);
    if (failure !== undefined) throw new Error(failure);
  });

  it("splits a large merge into several key-ordered outputs and drops deletes at the bottom", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* createDatabase;
          const client = yield* DbClient;
          for (let round = 0; round < 3; round++) {
            for (let key = 0; key < 8; key++)
              yield* client.put(DB, keyBytes(key), Uint8Array.of(round, key));
            yield* client.delete(DB, keyBytes(round));
            yield* client.flush(DB);
          }
          const { manifest } = yield* (yield* ManifestReader).loadLatest(DB);
          const job: CompactionJob = {
            sources: manifest.l0.map((sst) => ({ _tag: "L0" as const, sstId: sst.id })),
            destination: SortedRunId.make(0),
            isBottom: true,
          };
          return yield* runJob(job, manifest.compactorEpoch);
        }),
      ),
    );
    expect(outcome.outputs.length).toBeGreaterThan(1);
    expect(outcome.committed.manifest.l0).toEqual([]);
    const rows = outcome.outputs.reduce((sum, sst) => sum + sst.rowCount, 0);
    // 24 puts and 3 deletes collapse to one row per live key: each round re-puts every key, so only
    // key 2's last delete survives, and at the bottom it is dropped with what it hid.
    expect(rows).toBe(7);
    expect(runsAreSorted(outcome.committed.manifest)).toBe(true);
  });

  it("keeps a delete that hides a value in an older sorted run", async () => {
    const visible = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* createDatabase;
          const client = yield* DbClient;
          const reader = yield* ManifestReader;
          const compactL0 = Effect.gen(function* () {
            const { manifest } = yield* reader.loadLatest(DB);
            const job: CompactionJob = {
              sources: manifest.l0.map((sst) => ({ _tag: "L0" as const, sstId: sst.id })),
              destination: SortedRunId.make(manifest.sortedRuns.length),
              isBottom: manifest.sortedRuns.length === 0,
            };
            yield* runJob(job, manifest.compactorEpoch);
          });
          yield* client.put(DB, keyBytes(1), Uint8Array.of(1));
          yield* client.flush(DB);
          yield* compactL0;
          yield* client.delete(DB, keyBytes(1));
          yield* client.flush(DB);
          // Not the bottom: run 0 still holds the old value, so the delete must survive.
          yield* compactL0;
          yield* TestClock.adjust("2 seconds");
          return yield* client.get(DB, keyBytes(1), {
            consistency: { _tag: "Flushed" },
            snapshot: Option.none(),
            refuseSealed: false,
          });
        }),
      ),
    );
    expect(visible).toEqual(Option.none());
  });

  it("refuses a fenced compactor and a job whose inputs another compaction replaced", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* createDatabase;
          const client = yield* DbClient;
          for (let round = 0; round < 2; round++) {
            yield* client.put(DB, keyBytes(round), Uint8Array.of(round));
            yield* client.flush(DB);
          }
          const store = yield* ManifestStore;
          const { manifest } = yield* (yield* ManifestReader).loadLatest(DB);
          const job: CompactionJob = {
            sources: manifest.l0.map((sst) => ({ _tag: "L0" as const, sstId: sst.id })),
            destination: SortedRunId.make(0),
            isBottom: true,
          };
          const executor = yield* CompactionExecutor;
          const prepared = yield* executor.prepare(DB, manifest.compactorEpoch, job);
          // Another compaction of the same inputs commits first.
          yield* runJob(job, manifest.compactorEpoch);
          const replaced = yield* Effect.flip(executor.commit(prepared, []));
          // A step of the prepared job that runs after that resolves inputs no manifest holds.
          const outputAfter = yield* Effect.flip(executor.writeOutput(prepared, Option.none()));
          const replanned = yield* Effect.flip(executor.prepare(DB, manifest.compactorEpoch, job));
          yield* store.update(DB, (current) => Result.succeed(fenceCompactor(current)));
          const fenced = yield* Effect.flip(executor.prepare(DB, manifest.compactorEpoch, job));
          return {
            replaced: replaced._tag,
            outputAfter: outputAfter._tag,
            replanned: replanned._tag,
            fenced: fenced._tag,
          };
        }),
      ),
    );
    expect(outcome).toEqual({
      replaced: "CompactionInputsChangedError",
      outputAfter: "CompactionInputsChangedError",
      replanned: "CompactionInputsChangedError",
      fenced: "CompactorFencedError",
    });
  });
});
