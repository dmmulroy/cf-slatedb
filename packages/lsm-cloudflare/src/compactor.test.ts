import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  fenceCompactor,
  initialManifest,
  SortedRunId,
  SstId,
  Timestamp,
} from "@shaledb/lsm-core";
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

import { GarbageCollector, makeGarbageCollector } from "./garbage-collector.ts";
import { CompactionBudget, compactionBudgetTestLayer } from "./compaction-budget.ts";
import { makeCompactionExecutor, CompactionExecutor } from "./compaction-executor.ts";
import {
  CompactionLauncher,
  CompactionLauncherRecord,
  CompactionLauncherUnavailableError,
  compactionLauncherTestLayer,
  type CompactionJobProgress,
} from "./compaction-launcher.ts";
import {
  Compactor,
  compactorLayerWithoutDependencies,
  CompactorStateCell,
  compactorStateCellTestLayer,
} from "./compactor.ts";
import { DbClient, dbClientTestLayer } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { DurableObjectAlarm, durableObjectAlarmTestLayer } from "./durable-object-alarm.ts";
import { LsmConfiguration } from "./lsm-configuration.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader, ManifestReadError } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import { sstKey } from "./object-keys.ts";
import { ObjectStore } from "./object-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";

const ORDERS = DatabaseId.make("orders");
const USERS = DatabaseId.make("users");

/** Client, stores, executor, in-process launcher, and a budget of one compaction at a time. */
const sharedLayer = Layer.mergeAll(
  compactionLauncherTestLayer,
  compactionBudgetTestLayer,
  Layer.effect(GarbageCollector, makeGarbageCollector),
).pipe(
  Layer.provideMerge(
    Layer.effect(CompactionExecutor, makeCompactionExecutor).pipe(
      Layer.provide(sstStoreLayerWithoutDependencies),
      Layer.provideMerge(dbClientTestLayer({ blockSize: 64, compactionBudgetCapacity: 1 })),
    ),
  ),
);

/** A compactor instance with its own durable state and alarm, over the shared services. */
const startCompactor = Effect.gen(function* () {
  const shared = yield* Effect.context<
    | ManifestReader
    | ManifestStore
    | CompactionLauncher
    | CompactionBudget
    | LsmConfiguration
    | GarbageCollector
  >();
  const built = yield* Layer.build(
    compactorLayerWithoutDependencies.pipe(
      Layer.provideMerge(Layer.mergeAll(compactorStateCellTestLayer, durableObjectAlarmTestLayer)),
      Layer.provide(Layer.succeedContext(shared)),
    ),
  );
  const compactor = Context.get(built, Compactor);
  const alarm = Context.get(built, DurableObjectAlarm);
  // As the runtime does: the alarm is cleared when it fires, then the handler runs.
  const fire = Effect.andThen(alarm.cancel, compactor.runAlarm);
  return { compactor, alarm, fire, cell: Context.get(built, CompactorStateCell) };
});

/**
 * Gates in front of the compactor's manifest access, so a test can hold one call while others run.
 * Each armed gate holds the next call that reaches it until the test opens it.
 */
interface ManifestGates {
  /** Holds the next `ManifestStore.update` calls, one gate each, in arrival order. */
  readonly updates: Ref.Ref<ReadonlyArray<Deferred.Deferred<void>>>;
  /** Called by the next `ManifestReader.loadLatest` after it loaded: signals, then waits. */
  readonly loadLatest: Ref.Ref<
    Option.Option<{
      readonly loaded: Deferred.Deferred<void>;
      readonly resume: Deferred.Deferred<void>;
    }>
  >;
  /** Called by the next `GarbageCollector.collect` before it collects: signals, then waits. */
  readonly collect: Ref.Ref<
    Option.Option<{
      readonly loaded: Deferred.Deferred<void>;
      readonly resume: Deferred.Deferred<void>;
    }>
  >;
  /** Fence-ins the compactor committed (its `update` calls). */
  readonly updateCalls: Ref.Ref<number>;
}

/** A compactor like {@link startCompactor}, whose manifest store and reader pass through gates. */
const startGatedCompactor = Effect.gen(function* () {
  const gates: ManifestGates = {
    updates: yield* Ref.make<ReadonlyArray<Deferred.Deferred<void>>>([]),
    loadLatest: yield* Ref.make<
      Option.Option<{
        readonly loaded: Deferred.Deferred<void>;
        readonly resume: Deferred.Deferred<void>;
      }>
    >(Option.none()),
    collect: yield* Ref.make<
      Option.Option<{
        readonly loaded: Deferred.Deferred<void>;
        readonly resume: Deferred.Deferred<void>;
      }>
    >(Option.none()),
    updateCalls: yield* Ref.make(0),
  };
  const store = yield* ManifestStore;
  const reader = yield* ManifestReader;
  const gatedStore = ManifestStore.of({
    ...store,
    update: (databaseId, edit) =>
      Effect.gen(function* () {
        yield* Ref.update(gates.updateCalls, (count) => count + 1);
        const gate = yield* Ref.modify(gates.updates, (pending) => [pending[0], pending.slice(1)]);
        if (gate !== undefined) yield* Deferred.await(gate);
        return yield* store.update(databaseId, edit);
      }),
  });
  const gatedReader = ManifestReader.of({
    ...reader,
    loadLatest: (databaseId) =>
      Effect.gen(function* () {
        const loaded = yield* reader.loadLatest(databaseId);
        const gate = yield* Ref.getAndSet(gates.loadLatest, Option.none());
        if (Option.isSome(gate)) {
          yield* Deferred.succeed(gate.value.loaded, undefined);
          yield* Deferred.await(gate.value.resume);
        }
        return loaded;
      }),
  });
  const collector = yield* GarbageCollector;
  const gatedCollector = GarbageCollector.of({
    collect: (databaseId, oldestRunningJobStartedAt) =>
      Effect.gen(function* () {
        const gate = yield* Ref.getAndSet(gates.collect, Option.none());
        if (Option.isSome(gate)) {
          yield* Deferred.succeed(gate.value.loaded, undefined);
          yield* Deferred.await(gate.value.resume);
        }
        return yield* collector.collect(databaseId, oldestRunningJobStartedAt);
      }),
    collectLeftovers: collector.collectLeftovers,
  });
  const shared = yield* Effect.context<
    CompactionLauncher | CompactionBudget | LsmConfiguration | GarbageCollector
  >();
  const built = yield* Layer.build(
    compactorLayerWithoutDependencies.pipe(
      Layer.provideMerge(Layer.mergeAll(compactorStateCellTestLayer, durableObjectAlarmTestLayer)),
      Layer.provide(
        Layer.succeedContext(
          shared.pipe(
            Context.add(ManifestStore, gatedStore),
            Context.add(ManifestReader, gatedReader),
            Context.add(GarbageCollector, gatedCollector),
          ),
        ),
      ),
    ),
  );
  const compactor = Context.get(built, Compactor);
  const alarm = Context.get(built, DurableObjectAlarm);
  const fire = Effect.andThen(alarm.cancel, compactor.runAlarm);
  /** Arms the next `loadLatest`: returns when it has loaded, and a way to let it go on. */
  const holdNextLoad = Effect.gen(function* () {
    const loaded = yield* Deferred.make<void>();
    const resume = yield* Deferred.make<void>();
    yield* Ref.set(gates.loadLatest, Option.some({ loaded, resume }));
    return { loaded, resume };
  });
  /** Arms the next collection: returns when it is about to collect, and a way to let it go on. */
  const holdNextCollection = Effect.gen(function* () {
    const loaded = yield* Deferred.make<void>();
    const resume = yield* Deferred.make<void>();
    yield* Ref.set(gates.collect, Option.some({ loaded, resume }));
    return { loaded, resume };
  });
  /** Arms the next `count` `update` calls, each behind its own gate. */
  const holdUpdates = (count: number) =>
    Effect.gen(function* () {
      const held: Array<Deferred.Deferred<void>> = [];
      for (let index = 0; index < count; index++) held.push(yield* Deferred.make<void>());
      yield* Ref.set(gates.updates, held);
      return held;
    });
  return {
    compactor,
    alarm,
    fire,
    cell: Context.get(built, CompactorStateCell),
    gates,
    holdNextLoad,
    holdNextCollection,
    holdUpdates,
  };
});

const createWithL0 = (databaseId: DatabaseId, flushes: number) =>
  Effect.gen(function* () {
    yield* (yield* ManifestStore).initialize(
      initialManifest(databaseId, {
        keyRange: Option.none(),
        databaseOptions: DEFAULT_DATABASE_OPTIONS,
      }),
    );
    const client = yield* DbClient;
    for (let flush = 0; flush < flushes; flush++) {
      yield* client.put(databaseId, Uint8Array.of(flush), Uint8Array.of(flush));
      yield* client.flush(databaseId);
    }
  });

const run = <A, E>(
  program: Effect.Effect<
    A,
    E,
    | DbClient
    | ManifestReader
    | ManifestStore
    | CompactionLauncher
    | CompactionLauncherRecord
    | CompactionBudget
    | LsmConfiguration
    | ObjectStore
    | GarbageCollector
    | DbReaderServices
  >,
) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(sharedLayer),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
    Effect.runPromise,
  );

describe("Compactor", () => {
  it("collects its database's garbage from its alarm once old enough, then sets no alarm", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        const { compactor, alarm, fire } = yield* startCompactor;
        const objects = yield* ObjectStore;
        const ssts = Effect.map(
          objects.list(`db/${ORDERS}/compacted/`, Option.none()),
          (page) => page.objects.length,
        );
        yield* compactor.wake(ORDERS);
        yield* fire;
        yield* TestClock.adjust("5 seconds");
        yield* fire;
        // The compaction finished: its four inputs are garbage, collectable after gcMinAge.
        const due = Option.flatMap(
          yield* compactor.status(ORDERS),
          (state) => state.collectionDueAt,
        );
        const scheduled = yield* alarm.scheduled;
        const before = yield* ssts;
        yield* TestClock.adjust("1 hour");
        yield* fire;
        const after = yield* ssts;
        // That collection's own commit superseded a manifest, collectable an hour later; the next
        // collection commits nothing, so nothing more becomes due.
        const secondDue = Option.flatMap(
          yield* compactor.status(ORDERS),
          (current) => current.collectionDueAt,
        );
        yield* TestClock.adjust("1 hour");
        yield* fire;
        const state = yield* compactor.status(ORDERS);
        return {
          due,
          scheduled,
          before,
          after,
          secondDue,
          nextDue: Option.flatMap(state, (current) => current.collectionDueAt),
          idleAlarm: yield* alarm.scheduled,
        };
      }),
    );
    expect(outcome.due).toEqual(Option.some(5_000 + 3_600_000));
    expect(outcome.scheduled).toEqual(outcome.due);
    expect(outcome.before).toBe(5);
    expect(outcome.after).toBe(1);
    expect(outcome.secondDue).toEqual(Option.some(5_000 + 2 * 3_600_000));
    expect(outcome.nextDue).toEqual(Option.none());
    expect(outcome.idleAlarm).toEqual(Option.none());
  });

  it("fences in on wake, compacts L0 from its alarm, and goes idle with no work", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        const { compactor, alarm, fire } = yield* startCompactor;
        const woken = yield* compactor.wake(ORDERS);
        const firstPoll = yield* alarm.scheduled;
        yield* fire;
        const afterStart = yield* (yield* ManifestReader).loadLatest(ORDERS);
        const running = yield* compactor.status(ORDERS);
        const secondPoll = yield* alarm.scheduled;
        yield* TestClock.adjust("5 seconds");
        yield* fire;
        const idle = yield* compactor.status(ORDERS);
        // Waking again keeps the epoch: jobs it started stay valid across wakes and evictions.
        const rewoken = yield* compactor.wake(ORDERS);
        const jobs = yield* Ref.get(yield* CompactionLauncherRecord);
        const value = yield* (yield* DbClient).get(ORDERS, Uint8Array.of(2), {
          consistency: { _tag: "Latest" },
          snapshot: Option.none(),
          refuseSealed: false,
        });
        return {
          woken,
          firstPoll,
          afterStart,
          running,
          secondPoll,
          idle,
          rewoken,
          jobs: [...jobs.values()],
          value,
        };
      }),
    );
    expect(outcome.woken.compactorEpoch).toBe(1);
    expect(outcome.rewoken.compactorEpoch).toBe(1);
    expect(outcome.firstPoll).toEqual(Option.some(0));
    expect(outcome.afterStart.manifest.l0).toEqual([]);
    expect(outcome.afterStart.manifest.sortedRuns.map((sortedRun) => sortedRun.id)).toEqual([0]);
    expect(Option.map(outcome.running, (state) => state.running.length)).toEqual(Option.some(1));
    // While a job runs the compactor polls again; once it is done and nothing is planned it stops.
    expect(outcome.secondPoll).toEqual(Option.some(5_000));
    expect(Option.map(outcome.idle, (state) => state.running.length)).toEqual(Option.some(0));
    expect(outcome.jobs.map((job) => (job === "Failed" ? job : job._tag))).toEqual(["Committed"]);
    expect(outcome.value).toEqual(Option.some(Uint8Array.of(2)));
  });

  it("stops once a newer compactor fences it", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 1);
        const first = yield* startCompactor;
        yield* first.compactor.wake(ORDERS);
        const second = yield* startCompactor;
        const newer = yield* second.compactor.wake(ORDERS);
        yield* first.fire;
        return { newer, stale: yield* first.compactor.status(ORDERS) };
      }),
    );
    expect(outcome.newer.compactorEpoch).toBe(2);
    expect(Option.map(outcome.stale, (state) => state.fenced)).toEqual(Option.some(true));
  });

  it("names the database on the poll that finds its compactor fenced, for the health report (D82)", async () => {
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 1);
        const first = yield* startCompactor;
        yield* first.compactor.wake(ORDERS);
        const second = yield* startCompactor;
        yield* second.compactor.wake(ORDERS);
        yield* first.fire;
      }).pipe(Effect.withTracer(tracer)),
    );
    const fenced = spans.filter(
      (span) => span.name === "Compactor.poll" && span.attributes.get("compactor.fenced") === true,
    );
    expect(fenced.map((span) => span.attributes.get("database.id"))).toEqual([ORDERS]);
  });

  it("collects around its running jobs: a collection is told when the oldest started, a time taken before its launch (D96)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        const collector = yield* GarbageCollector;
        const told: Array<Option.Option<Timestamp>> = [];
        const launchedAt: Array<number> = [];
        // A job that keeps running, launched a moment after the compactor read its clock.
        const heldLauncher = CompactionLauncher.of({
          start: () =>
            Effect.gen(function* () {
              yield* TestClock.adjust(1);
              launchedAt.push(yield* Clock.currentTimeMillis);
            }),
          progress: () => Effect.succeed({ _tag: "Running" as const }),
        });
        const recordingCollector = GarbageCollector.of({
          collect: (databaseId, oldestRunningJobStartedAt) =>
            Effect.sync(() => told.push(oldestRunningJobStartedAt)).pipe(
              Effect.andThen(collector.collect(databaseId, oldestRunningJobStartedAt)),
            ),
          collectLeftovers: collector.collectLeftovers,
        });
        return yield* Effect.gen(function* () {
          const orders = yield* startCompactor;
          yield* orders.compactor.collect(ORDERS);
          yield* orders.compactor.wake(ORDERS);
          yield* orders.fire;
          yield* orders.compactor.collect(ORDERS);
          const state = yield* orders.cell.get;
          return {
            told,
            launchedAt,
            startedAt: Option.map(state, (current) => current.running.map((job) => job.startedAt)),
          };
        }).pipe(
          Effect.provideService(CompactionLauncher, heldLauncher),
          Effect.provideService(GarbageCollector, recordingCollector),
        );
      }),
    );
    const [startedAt] = Option.getOrElse(outcome.startedAt, () => []);
    expect(startedAt).toBeDefined();
    expect(outcome.told).toEqual([Option.none(), Option.some(startedAt)]);
    // The recorded start precedes the launch, so no output of the job is older than it.
    expect(startedAt).toBeLessThan(outcome.launchedAt[0] ?? 0);
  });

  it("a fenced compactor releases its running jobs' leases, so another database compacts at once (Workflow audit)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        yield* createWithL0(USERS, 4);
        // The orders job keeps running and holds the one compaction slot.
        const heldLauncher = CompactionLauncher.of({
          start: () => Effect.void,
          progress: () => Effect.succeed({ _tag: "Running" as const }),
        });
        const orders = yield* startCompactor.pipe(
          Effect.provideService(CompactionLauncher, heldLauncher),
        );
        const users = yield* startCompactor;
        yield* orders.compactor.wake(ORDERS);
        yield* orders.fire;
        yield* users.compactor.wake(USERS);
        yield* users.fire;
        const reader = yield* ManifestReader;
        const usersWhileHeld = (yield* reader.loadLatest(USERS)).manifest.l0.length;
        // A newer compactor epoch for orders: its next poll finds itself fenced.
        yield* (yield* ManifestStore).update(ORDERS, (manifest) =>
          Result.succeed(fenceCompactor(manifest)),
        );
        yield* orders.fire.pipe(Effect.provideService(CompactionLauncher, heldLauncher));
        yield* users.fire;
        const usersAfter = (yield* reader.loadLatest(USERS)).manifest.l0.length;
        return { usersWhileHeld, usersAfter };
      }),
    );
    expect(outcome.usersWhileHeld).toBe(4);
    expect(outcome.usersAfter).toBe(0);
  });

  it("shares the compaction budget: a second database waits until the first job's lease is released", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        yield* createWithL0(USERS, 4);
        const orders = yield* startCompactor;
        const users = yield* startCompactor;
        yield* orders.compactor.wake(ORDERS);
        yield* users.compactor.wake(USERS);
        yield* orders.fire;
        yield* users.fire;
        const reader = yield* ManifestReader;
        const usersWaiting = (yield* reader.loadLatest(USERS)).manifest.l0.length;
        const usersPoll = yield* users.alarm.scheduled;
        // The orders job finished; its next poll releases the lease, then users gets it.
        yield* orders.fire;
        yield* users.fire;
        const usersAfter = (yield* reader.loadLatest(USERS)).manifest.l0.length;
        return { usersWaiting, usersPoll, usersAfter };
      }),
    );
    expect(outcome.usersWaiting).toBe(4);
    expect(Option.isSome(outcome.usersPoll)).toBe(true);
    expect(outcome.usersAfter).toBe(0);
  });

  it("reports a corrupted input SST as a typed outcome and stops compacting it (D53)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        const objects = yield* ObjectStore;
        const before = yield* (yield* ManifestReader).loadLatest(ORDERS);
        const victim = before.manifest.l0[1];
        if (victim === undefined) return yield* Effect.die("expected four L0 SSTs");
        // Flip a byte inside the SST's first data block: its CRC32C no longer matches.
        const stored = yield* objects.get(sstKey(ORDERS, victim.id));
        if (Option.isNone(stored)) return yield* Effect.die("the SST is missing");
        const damaged = Uint8Array.from(stored.value);
        damaged[2] = (damaged[2] ?? 0) ^ 0xff;
        yield* objects.put(sstKey(ORDERS, victim.id), damaged);

        const { compactor, fire } = yield* startCompactor;
        yield* compactor.wake(ORDERS);
        yield* fire;
        for (let poll = 0; poll < 3; poll++) {
          yield* TestClock.adjust("5 seconds");
          yield* fire;
        }
        const jobs = yield* Ref.get(yield* CompactionLauncherRecord);
        const status = yield* compactor.status(ORDERS);
        const after = yield* (yield* ManifestReader).loadLatest(ORDERS);
        return {
          victim: victim.id,
          jobs: [...jobs.values()],
          status,
          l0: after.manifest.l0.length,
        };
      }),
    );
    // One job ran and ended with a typed outcome naming the SST (no defect, no retry loop).
    expect(outcome.jobs).toHaveLength(1);
    expect(outcome.jobs[0]).toMatchObject({
      _tag: "Corrupted",
      sstId: outcome.victim,
      section: "DataBlock",
    });
    // The compactor reports it and plans nothing more over that L0 SST.
    const corrupt = Option.map(outcome.status, (state) => state.corruptSsts);
    expect(corrupt).toEqual(
      Option.some([
        expect.objectContaining({
          sstId: outcome.victim,
          source: Option.some({ _tag: "L0", sstId: outcome.victim }),
          reason: "ChecksumMismatch",
        }),
      ]),
    );
    expect(outcome.l0).toBe(4);
  });

  it("drops a reported corrupted SST on request, and L0 compaction resumes without its rows (D62)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        const objects = yield* ObjectStore;
        const reader = yield* ManifestReader;
        const before = yield* reader.loadLatest(ORDERS);
        const victim = before.manifest.l0[1];
        const healthy = before.manifest.l0[0];
        if (victim === undefined || healthy === undefined) {
          return yield* Effect.die("expected four L0 SSTs");
        }
        const stored = yield* objects.get(sstKey(ORDERS, victim.id));
        if (Option.isNone(stored)) return yield* Effect.die("the SST is missing");
        const damaged = Uint8Array.from(stored.value);
        damaged[2] = (damaged[2] ?? 0) ^ 0xff;
        yield* objects.put(sstKey(ORDERS, victim.id), damaged);

        const { compactor, fire, cell } = yield* startCompactor;
        yield* compactor.wake(ORDERS);
        yield* fire;
        yield* TestClock.adjust("5 seconds");
        yield* fire;
        // A fifth flush: L0 stays blocked behind the corrupted SST (merged oldest first).
        const client = yield* DbClient;
        yield* client.put(ORDERS, Uint8Array.of(4), Uint8Array.of(4));
        yield* client.flush(ORDERS);
        yield* compactor.wake(ORDERS);
        yield* fire;
        const blockedL0 = (yield* reader.loadLatest(ORDERS)).manifest.l0.length;

        // Refused, changing nothing: a healthy SST without force, and an SST not in the manifest.
        const notReported = yield* Effect.flip(
          compactor.dropSst(ORDERS, { sstId: healthy.id, force: false }),
        );
        const unknown = yield* Effect.flip(
          compactor.dropSst(ORDERS, {
            sstId: SstId.make("01JD0000000000000000000077"),
            force: true,
          }),
        );
        // A running job reading the SST's source would bring its rows back: refused even forced.
        const state = Option.getOrThrow(yield* cell.get);
        const running = {
          jobId: "job-reading-victim",
          job: {
            sources: [{ _tag: "L0" as const, sstId: victim.id }],
            destination: SortedRunId.make(9),
            isBottom: true,
          },
          budgetGroup: ORDERS,
          startedAt: Timestamp.make(0),
          lastProgressAt: Option.none(),
        };
        yield* cell.set({ ...state, running: [running] });
        const whileRunning = yield* Effect.flip(
          compactor.dropSst(ORDERS, { sstId: victim.id, force: true }),
        );
        yield* cell.set(state);

        const report = yield* compactor.dropSst(ORDERS, { sstId: victim.id, force: false });
        const again = yield* compactor.dropSst(ORDERS, { sstId: victim.id, force: false });
        const statusAfterDrop = yield* compactor.status(ORDERS);
        // The drop polls: the four healthy L0 SSTs compact.
        yield* fire;
        for (let poll = 0; poll < 3; poll++) {
          yield* TestClock.adjust("5 seconds");
          yield* fire;
        }
        const after = yield* reader.loadLatest(ORDERS);
        const values = [];
        for (let key = 0; key < 5; key++) {
          const row = yield* client.get(ORDERS, Uint8Array.of(key), {
            consistency: { _tag: "Flushed" },
            snapshot: Option.none(),
            refuseSealed: false,
          });
          values.push(Option.isSome(row));
        }
        return {
          victim: victim.id,
          blockedL0,
          refusals: [
            notReported._tag === "SstDropRefusedError" && notReported.reason,
            unknown._tag,
            whileRunning._tag === "SstDropRefusedError" && whileRunning.reason,
          ],
          report,
          again,
          corruptAfterDrop: Option.map(statusAfterDrop, (current) => current.corruptSsts.length),
          l0: after.manifest.l0.length,
          runs: after.manifest.sortedRuns.length,
          droppedSsts: after.manifest.droppedSsts.map((dropped) => dropped.sstId),
          values,
        };
      }),
    );
    expect(outcome.refusals).toEqual([
      "NotReportedCorrupted",
      "SstNotInManifestError",
      "CompactionRunning",
    ]);
    expect(outcome.report.dropped).toMatchObject({
      sstId: outcome.victim,
      from: { _tag: "L0", sstId: outcome.victim },
      reason: "Corrupted",
    });
    // A repeated request reports the same drop and commits nothing new.
    expect(outcome.again).toEqual(outcome.report);
    expect(outcome.corruptAfterDrop).toEqual(Option.some(0));
    expect([outcome.l0, outcome.runs]).toEqual([0, 1]);
    expect(outcome.droppedSsts).toEqual([outcome.victim]);
    // Key 2 lived only in the dropped SST (L0 is newest first: index 1 is the third flush).
    expect(outcome.blockedL0).toBe(5);
    expect(outcome.values).toEqual([true, true, false, true, true]);
  });

  it("does not run on compactor state a newer format wrote (D64)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        const { compactor, fire, cell } = yield* startCompactor;
        yield* compactor.wake(ORDERS);
        const state = Option.getOrThrow(yield* cell.get);
        yield* cell.set({ ...state, formatVersion: 2 });
        const woken = yield* Effect.flip(compactor.wake(ORDERS));
        yield* fire;
        const jobs = yield* Ref.get(yield* CompactionLauncherRecord);
        const after = yield* (yield* ManifestReader).loadLatest(ORDERS);
        return { woken, jobs: jobs.size, l0: after.manifest.l0.length };
      }),
    );
    expect(outcome.woken._tag).toBe("CompactorUnavailableError");
    expect(outcome.woken.message).toContain("written by a newer shaledb (format 2");
    // Its alarm compacts nothing.
    expect([outcome.jobs, outcome.l0]).toEqual([0, 4]);
  });
});

describe("Compactor fence-in (D78)", () => {
  it("two wakes that overlap inside the first fence-in share one fence, and compaction commits (D78)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        const { compactor, fire, cell, gates, holdNextLoad, holdUpdates } =
          yield* startGatedCompactor;
        // Hold the first two manifest updates: two cold wakes both reach their fence-in.
        const [firstFence, secondFence] = yield* holdUpdates(2);
        const first = yield* Effect.forkChild(compactor.wake(ORDERS));
        yield* Effect.yieldNow;
        const second = yield* Effect.forkChild(compactor.wake(ORDERS));
        yield* Effect.yieldNow;
        // The first fence-in commits and stores its state; its alarm is due now.
        if (firstFence !== undefined) yield* Deferred.succeed(firstFence, undefined);
        yield* Fiber.join(first);
        // Its poll loads the manifest (epoch 1) and is held there while the second wake finishes.
        const pollGate = yield* holdNextLoad;
        const poll = yield* Effect.forkChild(fire);
        yield* Deferred.await(pollGate.loaded);
        if (secondFence !== undefined) yield* Deferred.succeed(secondFence, undefined);
        yield* Fiber.join(second);
        yield* Deferred.succeed(pollGate.resume, undefined);
        yield* Fiber.join(poll);
        // Later polls, and a wake from the next flush.
        for (let round = 0; round < 3; round++) {
          yield* TestClock.adjust("5 seconds");
          yield* compactor.wake(ORDERS);
          yield* fire;
        }
        const latest = yield* (yield* ManifestReader).loadLatest(ORDERS);
        const state = Option.getOrThrow(yield* cell.get);
        const jobs = yield* Ref.get(yield* CompactionLauncherRecord);
        return {
          fenceIns: yield* Ref.get(gates.updateCalls),
          manifestEpoch: latest.manifest.compactorEpoch,
          stateEpoch: state.compactorEpoch,
          fenced: state.fenced,
          l0: latest.manifest.l0.length,
          committed: [...jobs.values()].filter(
            (job) => job !== "Failed" && job._tag === "Committed",
          ).length,
        };
      }),
    );
    expect(outcome).toEqual({
      fenceIns: 1,
      manifestEpoch: 1,
      stateEpoch: 1,
      fenced: false,
      l0: 0,
      committed: 1,
    });
  });
  it("a collection requested while a poll runs is still due after the poll writes its state (D78)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        const { compactor, fire, cell, holdNextLoad } = yield* startGatedCompactor;
        yield* compactor.wake(ORDERS);
        const before = Option.flatMap(yield* cell.get, (state) => state.collectionDueAt);
        // The poll loads the manifest and is held; the maintenance sweep asks for a collection.
        const pollGate = yield* holdNextLoad;
        const poll = yield* Effect.forkChild(fire);
        yield* Deferred.await(pollGate.loaded);
        yield* TestClock.adjust("1 second");
        yield* compactor.requestCollection(ORDERS);
        yield* Deferred.succeed(pollGate.resume, undefined);
        yield* Fiber.join(poll);
        const state = Option.getOrThrow(yield* cell.get);
        return { before, due: state.collectionDueAt, running: state.running.length };
      }),
    );
    expect(outcome.before).toEqual(Option.none());
    // The poll started a compaction and kept the request it did not see.
    expect(outcome.running).toBe(1);
    expect(outcome.due).toEqual(Option.some(1_000));
  });

  it("a collection requested while a collection runs is still due after it (D78)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 1);
        const { compactor, fire, cell, holdNextCollection } = yield* startGatedCompactor;
        yield* compactor.requestCollection(ORDERS);
        // The alarm's collection is held; a second request arrives meanwhile.
        const collectGate = yield* holdNextCollection;
        const alarmRun = yield* Effect.forkChild(fire);
        yield* Deferred.await(collectGate.loaded);
        yield* TestClock.adjust("1 second");
        yield* compactor.requestCollection(ORDERS);
        yield* Deferred.succeed(collectGate.resume, undefined);
        yield* Fiber.join(alarmRun);
        return Option.flatMap(yield* cell.get, (state) => state.collectionDueAt);
      }),
    );
    // The report (nothing more to collect) predates the request, which stands.
    expect(outcome).toEqual(Option.some(1_000));
  });

  it("a poll never writes its older epoch over a fence-in that happened while it ran (D78)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        const store = yield* ManifestStore;
        const { compactor, fire, cell, holdNextLoad } = yield* startGatedCompactor;
        yield* compactor.wake(ORDERS);
        const pollGate = yield* holdNextLoad;
        const poll = yield* Effect.forkChild(fire);
        yield* Deferred.await(pollGate.loaded);
        // While the poll runs on epoch 1, a fence-in commits epoch 2 and stores it (the overwrite
        // seen live on preview-3).
        const fenced = yield* store.update(ORDERS, (manifest) =>
          Result.succeed(fenceCompactor(manifest)),
        );
        const current = Option.getOrThrow(yield* cell.get);
        yield* cell.set({ ...current, compactorEpoch: fenced.manifest.compactorEpoch });
        yield* Deferred.succeed(pollGate.resume, undefined);
        yield* Fiber.join(poll);
        const state = Option.getOrThrow(yield* cell.get);
        return [state.compactorEpoch, state.fenced, state.running.length];
      }),
    );
    // The poll's job carries epoch 1 and is discarded; epoch 2's state stands, and it is not fenced.
    expect(outcome).toEqual([2, false, 0]);
  });

  it("a compactor stored as fenced by an older epoch fences in again on its next start and compacts (D78)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        const store = yield* ManifestStore;
        const reader = yield* ManifestReader;
        const { compactor, fire, cell } = yield* startCompactor;
        // The state preview-3's ms-cas-mwy5ujbj was left in: epoch 1 stored and fenced, epoch 2
        // in the manifest.
        yield* compactor.wake(ORDERS);
        const stuck = Option.getOrThrow(yield* cell.get);
        yield* store.update(ORDERS, (manifest) => Result.succeed(fenceCompactor(manifest)));
        yield* cell.set({ ...stuck, fenced: true });
        // A wake (the next flush, or the stalled-write wake) fences in again and compacts.
        const woken = yield* compactor.wake(ORDERS);
        yield* fire;
        yield* TestClock.adjust("5 seconds");
        yield* fire;
        const afterWake = yield* reader.loadLatest(ORDERS);
        // Fenced again with a collection due: the alarm alone fences in again.
        yield* store.update(ORDERS, (manifest) => Result.succeed(fenceCompactor(manifest)));
        yield* fire;
        const fencedByPoll = Option.getOrThrow(yield* cell.get);
        yield* fire;
        const afterAlarm = Option.getOrThrow(yield* cell.get);
        const jobs = yield* Ref.get(yield* CompactionLauncherRecord);
        return {
          woken: [woken.compactorEpoch, woken.fenced],
          afterWake: [afterWake.manifest.compactorEpoch, afterWake.manifest.l0.length],
          jobs: [...jobs.values()].map((job) => (job === "Failed" ? job : job._tag)),
          fencedByPoll: [fencedByPoll.compactorEpoch, fencedByPoll.fenced],
          afterAlarm: [
            afterAlarm.compactorEpoch,
            afterAlarm.fenced,
            Option.isSome(afterAlarm.collectionDueAt),
          ],
          manifestEpoch: (yield* reader.loadLatest(ORDERS)).manifest.compactorEpoch,
        };
      }),
    );
    expect(outcome.woken).toEqual([3, false]);
    expect(outcome.afterWake).toEqual([3, 0]);
    expect(outcome.jobs).toEqual(["Committed"]);
    expect(outcome.fencedByPoll).toEqual([3, true]);
    // The re-fence keeps the collection the finished compaction made due.
    expect(outcome.afterAlarm).toEqual([5, false, true]);
    expect(outcome.manifestEpoch).toBe(5);
  });
});

describe("Compactor jobs it can no longer see (D94)", () => {
  it("a job whose progress cannot be read for compactionLeaseMs is finished, freeing its lease and sources", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        const settings = yield* LsmConfiguration;
        // Workflows accepts the job, then can never report it (a purged instance, or the
        // service unreachable): every progress read fails.
        const started: Array<string> = [];
        const blindLauncher = CompactionLauncher.of({
          start: (params) => Effect.sync(() => void started.push(params.jobId)),
          progress: (jobId) =>
            Effect.fail(
              new CompactionLauncherUnavailableError({ jobId, message: "instance not found" }),
            ),
        });
        const shared = yield* Effect.context<
          ManifestReader | ManifestStore | CompactionBudget | LsmConfiguration | GarbageCollector
        >();
        const built = yield* Layer.build(
          compactorLayerWithoutDependencies.pipe(
            Layer.provideMerge(
              Layer.mergeAll(compactorStateCellTestLayer, durableObjectAlarmTestLayer),
            ),
            Layer.provide(
              Layer.succeedContext(shared.pipe(Context.add(CompactionLauncher, blindLauncher))),
            ),
          ),
        );
        const compactor = Context.get(built, Compactor);
        const alarm = Context.get(built, DurableObjectAlarm);
        const fire = Effect.andThen(alarm.cancel, compactor.runAlarm);
        const runningIds = Effect.map(compactor.status(ORDERS), (state) =>
          Option.match(state, {
            onNone: () => [],
            onSome: (current) => current.running.map((job) => job.jobId),
          }),
        );
        yield* compactor.wake(ORDERS);
        yield* fire;
        const first = yield* runningIds;
        // Polls while the reads fail, short of the bound: the job is kept.
        yield* TestClock.adjust(settings.compactionLeaseMs - 60_000);
        yield* fire;
        const beforeBound = yield* runningIds;
        // Past the bound: the job is finished, its lease released, and its sources planned again.
        yield* TestClock.adjust(120_000);
        yield* fire;
        const afterBound = yield* runningIds;
        return {
          first: first.length,
          kept: beforeBound.join() === first.join(),
          replanned: afterBound.length === 1 && afterBound[0] !== first[0],
          // The budget holds one compaction: the new job started only because the old lease
          // was released.
          starts: started.length,
        };
      }),
    );
    expect(outcome).toEqual({
      first: 1,
      kept: true,
      replanned: true,
      starts: 2,
    });
  });
  it("a job whose Workflow instance is not found is finished on the next poll, not after the lease bound (D103, D119)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        const settings = yield* LsmConfiguration;
        // Workflows accepts the job, then purges it from retention: the launcher reports a
        // missing instance as finished with no outcome (D103).
        const started: Array<string> = [];
        const blindLauncher = CompactionLauncher.of({
          start: (params) => Effect.sync(() => void started.push(params.jobId)),
          progress: () =>
            Effect.succeed<CompactionJobProgress>({ _tag: "Finished", outcome: Option.none() }),
        });
        const shared = yield* Effect.context<
          ManifestReader | ManifestStore | CompactionBudget | LsmConfiguration | GarbageCollector
        >();
        const built = yield* Layer.build(
          compactorLayerWithoutDependencies.pipe(
            Layer.provideMerge(
              Layer.mergeAll(compactorStateCellTestLayer, durableObjectAlarmTestLayer),
            ),
            Layer.provide(
              Layer.succeedContext(shared.pipe(Context.add(CompactionLauncher, blindLauncher))),
            ),
          ),
        );
        const compactor = Context.get(built, Compactor);
        const alarm = Context.get(built, DurableObjectAlarm);
        const fire = Effect.andThen(alarm.cancel, compactor.runAlarm);
        const runningIds = Effect.map(compactor.status(ORDERS), (state) =>
          Option.match(state, {
            onNone: () => [],
            onSome: (current) => current.running.map((job) => job.jobId),
          }),
        );
        yield* compactor.wake(ORDERS);
        yield* fire;
        const first = yield* runningIds;
        // One poll interval later, well inside compactionLeaseMs: the job is finished and its
        // sources planned again.
        yield* TestClock.adjust(settings.compactorPollInterval);
        yield* fire;
        const next = yield* runningIds;
        return {
          first: first.length,
          boundAhead: settings.compactorPollInterval < settings.compactionLeaseMs,
          replanned: next.length === 1 && next[0] !== first[0],
          starts: started.length,
        };
      }),
    );
    expect(outcome).toEqual({ first: 1, boundAhead: true, replanned: true, starts: 2 });
  });
  it("the bound counts from the last read that saw the job running, not from its start (D94)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        const settings = yield* LsmConfiguration;
        // Reads succeed ("Running") until `reachable` is cleared, then fail.
        let reachable = true;
        const launcher = CompactionLauncher.of({
          start: () => Effect.void,
          progress: (jobId) =>
            reachable
              ? Effect.succeed({ _tag: "Running" as const })
              : Effect.fail(
                  new CompactionLauncherUnavailableError({ jobId, message: "unreachable" }),
                ),
        });
        const shared = yield* Effect.context<
          ManifestReader | ManifestStore | CompactionBudget | LsmConfiguration | GarbageCollector
        >();
        const built = yield* Layer.build(
          compactorLayerWithoutDependencies.pipe(
            Layer.provideMerge(
              Layer.mergeAll(compactorStateCellTestLayer, durableObjectAlarmTestLayer),
            ),
            Layer.provide(
              Layer.succeedContext(shared.pipe(Context.add(CompactionLauncher, launcher))),
            ),
          ),
        );
        const compactor = Context.get(built, Compactor);
        const alarm = Context.get(built, DurableObjectAlarm);
        const fire = Effect.andThen(alarm.cancel, compactor.runAlarm);
        const runningIds = Effect.map(compactor.status(ORDERS), (state) =>
          Option.match(state, {
            onNone: () => [],
            onSome: (current) => current.running.map((job) => job.jobId),
          }),
        );
        yield* compactor.wake(ORDERS);
        yield* fire;
        const first = yield* runningIds;
        // A long job, seen running for longer than the bound.
        yield* TestClock.adjust(settings.compactionLeaseMs + 60_000);
        yield* fire;
        // Then unreachable for less than the bound since that read.
        reachable = false;
        yield* TestClock.adjust(settings.compactionLeaseMs - 120_000);
        yield* fire;
        return (yield* runningIds).join() === first.join() && first.length === 1;
      }),
    );
    expect(outcome).toBe(true);
  });
});

describe("Compactor alarm retries (D102)", () => {
  it("a poll that keeps failing is retried from 1 s, doubling to its ceiling, and a success resets it", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createWithL0(ORDERS, 4);
        const reader = yield* ManifestReader;
        const flags = { down: false };
        const failingReader = ManifestReader.of({
          ...reader,
          loadLatest: (databaseId) =>
            flags.down
              ? Effect.fail(
                  new ManifestReadError({
                    databaseId,
                    manifestId: Option.none(),
                    reason: "StorageUnavailable",
                    message: "R2 is unavailable",
                  }),
                )
              : reader.loadLatest(databaseId),
        });
        const shared = yield* Effect.context<
          | ManifestStore
          | CompactionLauncher
          | CompactionBudget
          | LsmConfiguration
          | GarbageCollector
        >();
        const built = yield* Layer.build(
          compactorLayerWithoutDependencies.pipe(
            Layer.provideMerge(
              Layer.mergeAll(compactorStateCellTestLayer, durableObjectAlarmTestLayer),
            ),
            Layer.provide(
              Layer.succeedContext(shared.pipe(Context.add(ManifestReader, failingReader))),
            ),
          ),
        );
        const compactor = Context.get(built, Compactor);
        const alarm = Context.get(built, DurableObjectAlarm);
        const fireAtScheduled = Effect.gen(function* () {
          const at = Option.getOrThrow(yield* alarm.scheduled);
          const now = yield* Clock.currentTimeMillis;
          if (at > now) yield* TestClock.adjust(at - now);
          yield* alarm.cancel;
          const exit = yield* Effect.exit(compactor.runAlarm);
          return {
            threw: exit._tag === "Failure",
            delay: Option.map(yield* alarm.scheduled, (time) => time - at),
          };
        });
        yield* compactor.wake(ORDERS);
        flags.down = true;
        const delays: Array<number | undefined> = [];
        for (let firing = 0; firing < 11; firing++) {
          const fired = yield* fireAtScheduled;
          if (fired.threw) return "threw";
          delays.push(Option.getOrUndefined(fired.delay));
        }
        flags.down = false;
        yield* fireAtScheduled;
        const status = Option.getOrThrow(yield* compactor.status(ORDERS));
        flags.down = true;
        const afterReset = yield* fireAtScheduled;
        return {
          delays,
          compacting: status.running.length,
          afterReset: Option.getOrUndefined(afterReset.delay),
        };
      }),
    );
    // Ceiling: min(gcInterval 300 s, compactionLeaseMs / 2 = 450 s), so leases are renewed in time.
    expect(outcome).toEqual({
      delays: [
        1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000, 256_000, 300_000, 300_000,
      ],
      // Recovered: the poll started the compaction it could not plan while the manifest was down.
      compacting: 1,
      afterReset: 1_000,
    });
  });
});
