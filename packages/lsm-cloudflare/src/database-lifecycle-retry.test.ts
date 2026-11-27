import { DatabaseId, DEFAULT_DATABASE_OPTIONS, type KeyRange, ManifestId } from "@shaledb/lsm-core";
import { Arbitrary, Context, Effect, Layer, Option, Schema } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { backgroundWorkTestLayer } from "./background-work.ts";
import { blockSourceTestLayer } from "./block-source.ts";
import { compactionBudgetTestLayer } from "./compaction-budget.ts";
import { CompactionExecutor, makeCompactionExecutor } from "./compaction-executor.ts";
import { compactionLauncherTestLayer } from "./compaction-launcher.ts";
import {
  Compactor,
  compactorLayerWithoutDependencies,
  compactorStateCellTestLayer,
} from "./compactor.ts";
import { compactorClientTestLayer } from "./compactor-client.ts";
import {
  DatabaseLifecycle,
  databaseLifecycleLayerWithoutDependencies,
} from "./database-lifecycle.ts";
import { DbClient, dbClientTestLayer } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { DurableObjectAlarm, durableObjectAlarmTestLayer } from "./durable-object-alarm.ts";
import { GarbageCollector, makeGarbageCollector } from "./garbage-collector.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";

/*
 * Every lifecycle operation documented as safe to retry (D136), and garbage collection ("safe to
 * repeat"), is run, then random later work (writes, flushes, compactions, collections, time), then
 * run again. Whatever the second call answers, every acknowledged write stays readable in every
 * database that was not removed, and a source with a fork that still reads its SSTs refuses
 * removal.
 */

const SOURCE = DatabaseId.make("orders");
const LEFT = DatabaseId.make("orders-a");
const FRESH = DatabaseId.make("orders-new");
const HOUR = 3_600_000;
const bytes = (text: string) => new TextEncoder().encode(text);
const text = (value: Uint8Array) => new TextDecoder().decode(value);
const LEFT_RANGE: KeyRange = { start: new Uint8Array(0), end: Option.some(bytes("d")) };
const KEYS = ["a", "b", "c", "e"];

const testLayer = Layer.mergeAll(
  databaseLifecycleLayerWithoutDependencies.pipe(
    Layer.provide(blockSourceTestLayer),
    Layer.provide(backgroundWorkTestLayer),
  ),
  Layer.effect(GarbageCollector, makeGarbageCollector).pipe(Layer.provide(backgroundWorkTestLayer)),
  compactionLauncherTestLayer,
  compactionBudgetTestLayer,
).pipe(
  Layer.provideMerge(compactorClientTestLayer),
  Layer.provideMerge(
    Layer.effect(CompactionExecutor, makeCompactionExecutor).pipe(
      Layer.provide(sstStoreLayerWithoutDependencies),
    ),
  ),
  Layer.provideMerge(dbClientTestLayer({ blockSize: 64 })),
);

type TestServices = Layer.Success<typeof testLayer> | DbReaderServices;

const run = <A, E>(program: Effect.Effect<A, E, TestServices>) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(testLayer),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
    Effect.runPromise,
  );

/** One database's compactor: woken, then its alarm fired until it has no more work. */
const compactUntilIdle = (databaseId: DatabaseId) =>
  Effect.gen(function* () {
    const shared = yield* Effect.context<TestServices>();
    const built = yield* Layer.build(
      compactorLayerWithoutDependencies.pipe(
        Layer.provideMerge(
          Layer.mergeAll(compactorStateCellTestLayer, durableObjectAlarmTestLayer),
        ),
        Layer.provide(Layer.succeedContext(shared)),
      ),
    );
    const compactor = Context.get(built, Compactor);
    const alarm = Context.get(built, DurableObjectAlarm);
    yield* compactor.wake(databaseId);
    for (let poll = 0; poll < 10; poll++) {
      if (Option.isNone(yield* alarm.scheduled)) break;
      yield* alarm.cancel;
      yield* compactor.runAlarm;
    }
  });

/** Each database's acknowledged values. */
type Oracle = Map<DatabaseId, Map<string, string>>;

/** The source with two flushed keys, a caller's checkpoint, and `LEFT` forked from it. */
const setUp = Effect.gen(function* () {
  const lifecycle = yield* DatabaseLifecycle;
  const client = yield* DbClient;
  yield* lifecycle.create(SOURCE, {
    keyRange: Option.none(),
    databaseOptions: DEFAULT_DATABASE_OPTIONS,
  });
  for (const key of ["a", "b"]) yield* client.put(SOURCE, bytes(key), bytes(key.toUpperCase()));
  yield* client.flush(SOURCE);
  const checkpoint = yield* lifecycle.createCheckpoint(SOURCE, {
    name: Option.none(),
    lifetimeMs: Option.none(),
  });
  const request = { source: SOURCE, checkpointId: checkpoint.id, range: LEFT_RANGE, forkId: LEFT };
  yield* lifecycle.forkRange(request);
  const oracle: Oracle = new Map([
    [
      SOURCE,
      new Map([
        ["a", "A"],
        ["b", "B"],
      ]),
    ],
    [
      LEFT,
      new Map([
        ["a", "A"],
        ["b", "B"],
      ]),
    ],
  ]);
  return { checkpointId: checkpoint.id, request, oracle };
});

type Setup = Effect.Success<typeof setUp>;

const withLifecycle = <A, E, R>(
  body: (lifecycle: Context.Service.Shape<typeof DatabaseLifecycle>) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    return yield* body(yield* DatabaseLifecycle);
  });

const withCollector = <A, E, R>(
  body: (collector: Context.Service.Shape<typeof GarbageCollector>) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    return yield* body(yield* GarbageCollector);
  });

/** An operation documented as safe to retry, and the answers its second call may give. */
interface RetriedOperation {
  readonly name: string;
  readonly call: (setup: Setup) => Effect.Effect<unknown, { readonly _tag: string }, TestServices>;
  readonly retryMayFailWith: ReadonlyArray<string>;
  /** Applies the operation to the oracle (the databases it removes, the writes it adds). */
  readonly apply?: (oracle: Oracle) => void;
}

const lifecycleOperations: ReadonlyArray<RetriedOperation> = [
  {
    name: "forkRange",
    call: ({ request }) => withLifecycle((l) => l.forkRange(request)),
    retryMayFailWith: ["DatabaseAlreadyExistsError"],
  },
  {
    name: "remove of the fork",
    call: () => withLifecycle((l) => l.remove(LEFT)),
    retryMayFailWith: [],
    apply: (oracle) => oracle.delete(LEFT),
  },
  {
    name: "deleteCheckpoint of the caller's checkpoint",
    call: ({ checkpointId }) => withLifecycle((l) => l.deleteCheckpoint(SOURCE, checkpointId)),
    retryMayFailWith: [],
  },
  {
    name: "seal",
    call: () => withLifecycle((l) => l.seal(SOURCE, [FRESH], false)),
    retryMayFailWith: [],
  },
  {
    name: "createCheckpoint",
    call: () =>
      withLifecycle((l) =>
        l.createCheckpoint(SOURCE, { name: Option.none(), lifetimeMs: Option.some(HOUR) }),
      ),
    retryMayFailWith: [],
  },
  {
    name: "pinSnapshot",
    call: () => withLifecycle((l) => l.pinSnapshot(SOURCE, HOUR)),
    retryMayFailWith: [],
  },
  {
    name: "create",
    call: () =>
      withLifecycle((l) =>
        l.create(FRESH, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
      ),
    retryMayFailWith: ["DatabaseAlreadyExistsError"],
    apply: (oracle) => {
      if (!oracle.has(FRESH)) oracle.set(FRESH, new Map());
    },
  },
  {
    name: "GarbageCollector.collect",
    call: () => withCollector((gc) => gc.collect(SOURCE, Option.none())),
    retryMayFailWith: [],
  },
];

type Between =
  | { readonly _tag: "Put"; readonly db: number; readonly key: number; readonly value: number }
  | { readonly _tag: "Flush" | "Compact" | "Collect"; readonly db: number }
  | { readonly _tag: "Age"; readonly hours: number };

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

const betweenArbitrary: Arbitrary.Arbitrary<Between> = Arbitrary.all({
  kind: smallInt(0, 9),
  db: smallInt(0, 2),
  key: smallInt(0, KEYS.length - 1),
  value: smallInt(0, 99),
  hours: smallInt(1, 2),
}).pipe(
  Arbitrary.map(({ kind, db, key, value, hours }): Between => {
    if (kind <= 2) return { _tag: "Put", db, key, value };
    if (kind <= 4) return { _tag: "Flush", db };
    if (kind <= 6) return { _tag: "Compact", db };
    if (kind <= 7) return { _tag: "Collect", db };
    return { _tag: "Age", hours };
  }),
);

const DATABASES = [SOURCE, LEFT, FRESH];
const databaseAt = (index: number) => DATABASES[index] ?? SOURCE;

/** Runs one later step; a step a database refuses (sealed, removed, absent) changes nothing. */
const runBetween = (step: Between, oracle: Oracle) =>
  Effect.gen(function* () {
    const client = yield* DbClient;
    const gc = yield* GarbageCollector;
    if (step._tag === "Age") {
      yield* TestClock.adjust(step.hours * HOUR);
      return;
    }
    const db = databaseAt(step.db);
    const values = oracle.get(db);
    if (values === undefined) return;
    switch (step._tag) {
      case "Put": {
        const key = KEYS[step.key] ?? "a";
        // The fork accepts only keys in its range.
        if (db === LEFT && key >= "d") return;
        const value = `${key}${step.value}`;
        const written = yield* Effect.result(client.put(db, bytes(key), bytes(value)));
        if (written._tag === "Success") values.set(key, value);
        return;
      }
      case "Flush":
        yield* Effect.ignore(client.flush(db));
        return;
      case "Compact":
        yield* compactUntilIdle(db);
        return;
      case "Collect":
        yield* Effect.ignore(gc.collect(db, Option.none()));
    }
  });

/** The values each database in the oracle reads, as `key=value`, and what the oracle expects. */
const readBack = (oracle: Oracle) =>
  Effect.gen(function* () {
    const client = yield* DbClient;
    const read: Array<string> = [];
    const expected: Array<string> = [];
    for (const [db, values] of oracle) {
      for (const key of KEYS) {
        const value = yield* Effect.result(
          client.get(db, bytes(key), {
            consistency: { _tag: "Latest" },
            snapshot: Option.none(),
            refuseSealed: false,
          }),
        );
        read.push(
          `${db}:${key}=${value._tag === "Success" ? Option.getOrElse(Option.map(value.success, text), () => "none") : value.failure._tag}`,
        );
        expected.push(`${db}:${key}=${values.get(key) ?? "none"}`);
      }
    }
    return { read, expected };
  });

/**
 * Runs `operation`, the later steps, the operation again, then GC on every database gcMinAge
 * later. True when the retry's answer is allowed, every acknowledged write reads back, and the
 * source refuses removal while the fork's newest manifest reads its SSTs.
 */
const retryIsSafe = (operation: RetriedOperation) => (steps: ReadonlyArray<Between>) =>
  run(
    Effect.gen(function* () {
      const lifecycle = yield* DatabaseLifecycle;
      const reader = yield* ManifestReader;
      const gc = yield* GarbageCollector;
      const setup = yield* setUp;
      const { oracle } = setup;
      const first = yield* Effect.result(operation.call(setup));
      if (first._tag === "Failure") return false;
      operation.apply?.(oracle);
      for (const step of steps) yield* runBetween(step, oracle);
      const retried = yield* Effect.result(operation.call(setup));
      if (
        retried._tag === "Failure" &&
        !operation.retryMayFailWith.includes(retried.failure._tag)
      ) {
        return false;
      }
      // What a lost hold would let happen: the caller's own checkpoint goes, time passes, every
      // database collects, and the source is removed if it allows it.
      yield* Effect.ignore(lifecycle.deleteCheckpoint(SOURCE, setup.checkpointId));
      yield* TestClock.adjust(2 * HOUR);
      for (const db of oracle.keys()) yield* Effect.ignore(gc.collect(db, Option.none()));
      const forkHead = oracle.has(LEFT) ? yield* reader.findLatest(LEFT) : Option.none();
      const forkReadsSource = Option.exists(forkHead, ({ manifest }) =>
        manifest.externalRefs.some((ref) => ref.owner === SOURCE),
      );
      const removed = yield* Effect.result(lifecycle.remove(SOURCE));
      if (forkReadsSource && removed._tag === "Success") return false;
      if (removed._tag === "Success") oracle.delete(SOURCE);
      const { read, expected } = yield* readBack(oracle);
      return read.every((entry, index) => entry === expected[index]);
    }),
  );

describe("lifecycle operations retried after later work (D136)", () => {
  for (const operation of lifecycleOperations) {
    it(`${operation.name}: a retry after writes, flushes, compactions, collections, and time loses nothing`, async () => {
      const result = await Effect.runPromise(
        Arbitrary.checkEffect(
          Arbitrary.array(betweenArbitrary, { minLength: 0, maxLength: 8 }),
          (steps) => Effect.promise(() => retryIsSafe(operation)(steps)),
          { runs: 200 },
        ),
      );
      const failure = Arbitrary.formatCheckFailure(result);
      if (failure !== undefined) throw new Error(failure);
    });
  }
});

describe("a retried forkRange (D136)", () => {
  it("keeps the fork's hold when the fork's GC has collected its first manifest (issue #2)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const reader = yield* ManifestReader;
        const gc = yield* GarbageCollector;
        const { request, checkpointId } = yield* setUp;
        // The fork moves on without rewriting the source's SSTs; its GC collects manifest 1.
        yield* client.put(LEFT, bytes("c"), bytes("C"));
        yield* client.flush(LEFT);
        yield* TestClock.adjust(2 * HOUR);
        yield* gc.collect(LEFT, Option.none());
        const firstManifest = yield* reader.load(LEFT, ManifestId.make(1));
        const retried = yield* Effect.result(lifecycle.forkRange(request));
        const holdsOf = Effect.map(
          reader.loadLatest(SOURCE),
          ({ manifest }) =>
            manifest.checkpoints.filter((checkpoint) => Option.contains(checkpoint.holder, LEFT))
              .length,
        );
        const holds = yield* holdsOf;
        yield* lifecycle.deleteCheckpoint(SOURCE, checkpointId);
        const removed = yield* Effect.result(lifecycle.remove(SOURCE));
        const { read } = yield* readBack(new Map([[LEFT, new Map()]]));
        return {
          firstManifestCollected: Option.isNone(firstManifest),
          retried:
            retried._tag === "Success" ? retried.success.manifest.databaseId : retried.failure._tag,
          holds,
          removed: removed._tag === "Success" ? "removed" : removed.failure._tag,
          read,
        };
      }),
    );
    expect(outcome).toEqual({
      firstManifestCollected: true,
      // The retry finds the fork its first call created.
      retried: LEFT,
      holds: 1,
      removed: "DatabaseHasDependentsError",
      read: [`${LEFT}:a=A`, `${LEFT}:b=B`, `${LEFT}:c=C`, `${LEFT}:e=none`],
    });
  });

  it("undoes the hold it created when the fork's id belongs to another database, and only that one", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const reader = yield* ManifestReader;
        const { checkpointId } = yield* setUp;
        yield* lifecycle.create(FRESH, {
          keyRange: Option.none(),
          databaseOptions: DEFAULT_DATABASE_OPTIONS,
        });
        const forkOntoTaken = yield* Effect.flip(
          lifecycle.forkRange({ source: SOURCE, checkpointId, range: LEFT_RANGE, forkId: FRESH }),
        );
        const holders = (yield* reader.loadLatest(SOURCE)).manifest.checkpoints.flatMap(
          (checkpoint) => Option.toArray(checkpoint.holder),
        );
        return { forkOntoTaken: forkOntoTaken._tag, holders };
      }),
    );
    expect(outcome).toEqual({ forkOntoTaken: "DatabaseAlreadyExistsError", holders: [LEFT] });
  });
});
