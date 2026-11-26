import {
  type CompactionJob,
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  SortedRunId,
} from "@cf-slatedb/lsm-core";
import { Arbitrary, Context, Effect, Layer, Option, Ref, Schema } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { CompactionExecutor, makeCompactionExecutor } from "./compaction-executor.ts";
import { type CompactionStepRunner, runCompactionJob } from "./compaction-job.ts";
import { DbClient, dbClientTestLayer } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { GarbageCollector, makeGarbageCollector } from "./garbage-collector.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import { compactedPrefix } from "./object-keys.ts";
import { ObjectStore, ObjectStoreUnavailableError, objectStoreTestLayer } from "./object-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";

const DB = DatabaseId.make("orders");

/** Which object-store calls fail: `true` fails the call, as R2 does after its retries. */
type FaultRule = (operation: ObjectStoreUnavailableError["operation"], key: string) => boolean;

/** The current fault rule. */
class Faults extends Context.Service<Faults, Ref.Ref<FaultRule>>()("test/Faults") {}

/** An in-memory bucket whose calls fail whenever `Faults` says so, as R2 does after retries. */
const faultyObjectStoreLayer = Layer.effect(
  ObjectStore,
  Effect.gen(function* () {
    const faults = yield* Faults;
    const inner = Context.get(yield* Layer.build(objectStoreTestLayer), ObjectStore);
    const gate = (operation: ObjectStoreUnavailableError["operation"], key: string) =>
      Effect.flatMap(Ref.get(faults), (fails) =>
        fails(operation, key)
          ? Effect.fail(
              new ObjectStoreUnavailableError({ operation, key, message: "injected R2 failure" }),
            )
          : Effect.void,
      );
    return ObjectStore.of({
      get: (key) => Effect.andThen(gate("get", key), inner.get(key)),
      getRange: (key, range) => Effect.andThen(gate("getRange", key), inner.getRange(key, range)),
      getVersioned: (key) => Effect.andThen(gate("get", key), inner.getVersioned(key)),
      putIfVersion: (key, body, version) =>
        Effect.andThen(gate("putIfVersion", key), inner.putIfVersion(key, body, version)),
      head: (key) => Effect.andThen(gate("head", key), inner.head(key)),
      putIfAbsent: (key, body) =>
        Effect.andThen(gate("putIfAbsent", key), inner.putIfAbsent(key, body)),
      put: (key, body) => Effect.andThen(gate("put", key), inner.put(key, body)),
      putMultipart: (key, body, partSize) =>
        Effect.andThen(gate("putMultipart", key), inner.putMultipart(key, body, partSize)),
      list: (prefix, cursor) => Effect.andThen(gate("list", prefix), inner.list(prefix, cursor)),
      listPrefixes: (prefix, cursor) =>
        Effect.andThen(gate("list", prefix), inner.listPrefixes(prefix, cursor)),
      delete: (keys) => Effect.andThen(gate("delete", keys[0] ?? ""), inner.delete(keys)),
    });
  }),
);

const noFaults: FaultRule = () => false;

/** The fault rule, one instance per test (layers are memoized by reference within a build). */
const faultsLayer = Layer.effect(Faults, Ref.make<FaultRule>(noFaults));

/** Client, executor, collector, and an SST store over one fault-injecting bucket. */
const testLayer = Layer.mergeAll(
  Layer.effect(CompactionExecutor, makeCompactionExecutor),
  Layer.effect(GarbageCollector, makeGarbageCollector),
).pipe(
  Layer.provideMerge(sstStoreLayerWithoutDependencies),
  Layer.provideMerge(
    dbClientTestLayer(
      { blockSize: 64, compactionOutputSstSize: 64, gcMinAge: 10_000 },
      faultyObjectStoreLayer.pipe(Layer.provide(faultsLayer)),
    ),
  ),
  Layer.provideMerge(faultsLayer),
);

type Services = Layer.Success<typeof testLayer> | DbReaderServices;

const provide = <A, E>(program: Effect.Effect<A, E, Services>) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(testLayer),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
  );

const create = Effect.gen(function* () {
  yield* (yield* ManifestStore).initialize(
    initialManifest(DB, {
      keyRange: Option.none(),
      databaseOptions: { ...DEFAULT_DATABASE_OPTIONS, mergeOperator: Option.some("Concat") },
    }),
  );
});

const setFaults = (fails: FaultRule) =>
  Effect.gen(function* () {
    yield* Ref.set(yield* Faults, fails);
  });

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

describe("fault injection", () => {
  it("an R2 outage never loses or hides an acknowledged write", async () => {
    const result = await Effect.runPromise(
      Arbitrary.checkEffect(
        Arbitrary.array(
          Arbitrary.all({ kind: smallInt(0, 10), key: smallInt(0, 5), value: smallInt(0, 255) }),
          { minLength: 15, maxLength: 40 },
        ),
        (steps) =>
          provide(
            Effect.gen(function* () {
              yield* create;
              const client = yield* DbClient;
              const oracle = new Map<number, number>();
              for (const step of steps) {
                if (step.kind <= 2) {
                  // R2 goes down, comes back, or turns flaky (every other call fails, so an
                  // upload can succeed and the manifest commit after it fail).
                  let calls = 0;
                  yield* setFaults(
                    step.kind === 0
                      ? () => true
                      : step.kind === 1
                        ? noFaults
                        : () => ++calls % 2 === 0,
                  );
                } else if (step.kind <= 6) {
                  // Writes only need the SQLite memtable once the writer has started.
                  const written = yield* Effect.result(
                    client.put(DB, Uint8Array.of(step.key), Uint8Array.of(step.value)),
                  );
                  if (written._tag === "Success") oracle.set(step.key, step.value);
                  else if (written.failure._tag !== "WriterUnavailableError") return false;
                } else if (step.kind <= 8) {
                  // A flush either commits or leaves the rows in SQLite.
                  const flushed = yield* Effect.result(client.flush(DB));
                  if (
                    flushed._tag === "Failure" &&
                    flushed.failure._tag !== "WriterUnavailableError"
                  )
                    return false;
                  yield* TestClock.adjust("2 seconds");
                } else {
                  // A read either sees the truth or fails as unavailable; never a wrong value.
                  const got = yield* Effect.result(
                    client.get(DB, Uint8Array.of(step.key), {
                      consistency: { _tag: "Latest" },
                      snapshot: Option.none(),
                      refuseSealed: false,
                    }),
                  );
                  if (got._tag === "Success") {
                    if (
                      Option.getOrUndefined(Option.map(got.success, (bytes) => bytes[0])) !==
                      oracle.get(step.key)
                    )
                      return false;
                  } else if (
                    got.failure._tag !== "ReaderUnavailableError" &&
                    got.failure._tag !== "WriterUnavailableError"
                  ) {
                    return false;
                  }
                }
              }
              // R2 recovers: everything acknowledged is readable, flushed or not.
              yield* setFaults(noFaults);
              yield* client.flush(DB);
              yield* TestClock.adjust("2 seconds");
              for (const consistency of [
                { _tag: "Latest" } as const,
                { _tag: "Flushed" } as const,
              ]) {
                for (let key = 0; key <= 5; key++) {
                  const got = yield* client.get(DB, Uint8Array.of(key), {
                    consistency,
                    snapshot: Option.none(),
                    refuseSealed: false,
                  });
                  if (
                    Option.getOrUndefined(Option.map(got, (bytes) => bytes[0])) !== oracle.get(key)
                  )
                    return false;
                }
              }
              return true;
            }),
          ),
        { runs: 80 },
      ),
    );
    const failure = Arbitrary.formatCheckFailure(result);
    if (failure !== undefined) throw new Error(failure);
  });

  it("a crash between the L0 upload and the manifest commit leaves an orphan GC removes, and loses nothing", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* create;
          const client = yield* DbClient;
          yield* client.put(DB, Uint8Array.of(1), Uint8Array.of(1));
          // The SST uploads, then the manifest create fails: the flush fails after the upload.
          yield* setFaults(
            (operation, key) => operation === "putIfAbsent" && key.includes("/manifest/"),
          );
          const failed = yield* Effect.flip(client.flush(DB));
          yield* setFaults(noFaults);
          const objects = yield* ObjectStore;
          const afterCrash = (yield* objects.list(compactedPrefix(DB), Option.none())).objects
            .length;
          yield* client.flush(DB);
          const afterRetry = (yield* objects.list(compactedPrefix(DB), Option.none())).objects
            .length;
          yield* TestClock.adjust("2 seconds");
          const value = yield* client.get(DB, Uint8Array.of(1), {
            consistency: { _tag: "Flushed" },
            snapshot: Option.none(),
            refuseSealed: false,
          });
          yield* TestClock.adjust("11 seconds");
          const report = yield* (yield* GarbageCollector).collect(DB);
          const afterGc = (yield* objects.list(compactedPrefix(DB), Option.none())).objects.length;
          return {
            failed: failed._tag,
            afterCrash,
            afterRetry,
            value,
            deleted: report.deletedSsts,
            afterGc,
          };
        }),
      ),
    );
    expect(outcome).toMatchObject({
      failed: "WriterUnavailableError",
      afterCrash: 1,
      afterRetry: 2,
      deleted: 1,
      afterGc: 1,
    });
    expect(outcome.value).toEqual(Option.some(Uint8Array.of(1)));
  });

  it("a compaction resumed after a failed step replays finished steps and commits once", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* create;
          const client = yield* DbClient;
          for (let round = 0; round < 3; round++) {
            for (let key = 0; key < 6; key++) {
              yield* client.put(DB, Uint8Array.of(key), Uint8Array.of(round, key));
            }
            yield* client.flush(DB);
          }
          const { manifest } = yield* (yield* ManifestReader).loadLatest(DB);
          const job: CompactionJob = {
            sources: manifest.l0.map((sst) => ({ _tag: "L0" as const, sstId: sst.id })),
            destination: SortedRunId.make(0),
            isBottom: true,
          };
          const params = {
            databaseId: DB,
            compactorEpoch: manifest.compactorEpoch,
            jobId: "job-1",
            job,
          };
          // A durable step runner, as Workflows: finished steps are replayed from their results.
          const finished = new Map<string, unknown>();
          const ran: Array<string> = [];
          const runner: CompactionStepRunner = (name, schema, step) =>
            Effect.gen(function* () {
              const stored = finished.get(name);
              if (stored !== undefined)
                return yield* Schema.decodeUnknownEffect(schema)(stored).pipe(Effect.orDie);
              ran.push(name);
              const value = yield* step;
              finished.set(name, yield* Schema.encodeEffect(schema)(value).pipe(Effect.orDie));
              return value;
            });
          // The second output's upload fails: the job stops there.
          let uploads = 0;
          yield* setFaults((operation) => operation === "putMultipart" && ++uploads === 2);
          const first = yield* Effect.flip(runCompactionJob(params, runner));
          yield* setFaults(noFaults);
          const firstRan = [...ran];
          ran.length = 0;
          const resumed = yield* runCompactionJob(params, runner);
          const { manifest: after } = yield* (yield* ManifestReader).loadLatest(DB);
          yield* TestClock.adjust("2 seconds");
          const values: Array<string> = [];
          for (let key = 0; key < 6; key++) {
            const got = yield* client.get(DB, Uint8Array.of(key), {
              consistency: { _tag: "Flushed" },
              snapshot: Option.none(),
              refuseSealed: false,
            });
            values.push(
              Option.match(got, { onNone: () => "-", onSome: (bytes) => bytes.join(".") }),
            );
          }
          return {
            first: first._tag,
            firstRan,
            resumedRan: ran,
            resumed,
            runs: after.sortedRuns,
            l0: after.l0.length,
            values,
          };
        }),
      ),
    );
    expect(outcome.first).toBe("CompactionUnavailableError");
    expect(outcome.firstRan).toEqual(["prepare", "output-0", "output-1"]);
    // Prepare and the first output are replayed, not rerun.
    expect(outcome.resumedRan[0]).toBe("output-1");
    expect(outcome.resumedRan).not.toContain("prepare");
    expect(outcome.resumed._tag).toBe("Committed");
    expect(outcome.l0).toBe(0);
    expect(outcome.runs.length).toBe(1);
    expect(outcome.values).toEqual(["2.0", "2.1", "2.2", "2.3", "2.4", "2.5"]);
  });
});
