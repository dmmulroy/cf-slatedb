import { DatabaseId, DEFAULT_DATABASE_OPTIONS, initialManifest } from "@cf-slatedb/lsm-core";
import { Context, Effect, Layer, Option, Ref } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { CompactionBudget, compactionBudgetTestLayer } from "./compaction-budget.ts";
import { makeCompactionExecutor, CompactionExecutor } from "./compaction-executor.ts";
import {
  CompactionLauncher,
  CompactionLauncherRecord,
  compactionLauncherTestLayer,
} from "./compaction-launcher.ts";
import {
  Compactor,
  compactorLayerWithoutDependencies,
  compactorStateCellTestLayer,
} from "./compactor.ts";
import { DbClient, dbClientTestLayer } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { DurableObjectAlarm, durableObjectAlarmTestLayer } from "./durable-object-alarm.ts";
import { LsmConfiguration } from "./lsm-configuration.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import { sstKey } from "./object-keys.ts";
import { ObjectStore } from "./object-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";

const ORDERS = DatabaseId.make("orders");
const USERS = DatabaseId.make("users");

/** Client, stores, executor, in-process launcher, and a budget of one compaction at a time. */
const sharedLayer = Layer.mergeAll(compactionLauncherTestLayer, compactionBudgetTestLayer).pipe(
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
    ManifestReader | ManifestStore | CompactionLauncher | CompactionBudget | LsmConfiguration
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
  return { compactor, alarm, fire };
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
});
