import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  type KeyRange,
  manifestSstHandles,
} from "@shaledb/lsm-core";
import { Context, Effect, Layer, Option, Ref } from "effect";
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
import { CompactorClientRecord, compactorClientTestLayer } from "./compactor-client.ts";
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
import { databasePrefix } from "./object-keys.ts";
import { ObjectStore } from "./object-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";

const SOURCE = DatabaseId.make("orders");
const LEFT = DatabaseId.make("orders-a");
const RIGHT = DatabaseId.make("orders-b");
const bytes = (text: string) => new TextEncoder().encode(text);
const text = (value: Uint8Array) => new TextDecoder().decode(value);
const SPLIT = bytes("d");
const LEFT_RANGE: KeyRange = { start: new Uint8Array(0), end: Option.some(SPLIT) };
const RIGHT_RANGE: KeyRange = { start: SPLIT, end: Option.none() };

/** Lifecycle, client, executor, in-process launcher, budget, and stores over one bucket. */
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

const valuesIn = (databaseId: DatabaseId) =>
  Effect.gen(function* () {
    const client = yield* DbClient;
    const found: Array<string> = [];
    for (const key of ["a", "b", "c", "d", "e", "f", "x"]) {
      const value = yield* client.get(databaseId, bytes(key), {
        consistency: { _tag: "Latest" },
        snapshot: Option.none(),
        refuseSealed: false,
      });
      if (Option.isSome(value)) found.push(`${key}=${text(value.value)}`);
    }
    return found;
  });

describe("DatabaseLifecycle", () => {
  it("seals, forks a split, rewrites and releases the forks' holds, then removes the source", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        yield* lifecycle.create(SOURCE, {
          keyRange: Option.none(),
          databaseOptions: DEFAULT_DATABASE_OPTIONS,
        });
        for (const key of ["a", "b", "c", "d", "e", "f"]) {
          yield* client.put(SOURCE, bytes(key), bytes(key.toUpperCase()));
          if (key === "c") yield* client.flush(SOURCE);
        }
        // Seal: the last writes are flushed into the sealed manifest; later writes are refused.
        yield* lifecycle.seal(SOURCE, [LEFT, RIGHT], false);
        const lateWrite = yield* Effect.flip(client.put(SOURCE, bytes("g"), bytes("G")));
        const checkpoint = yield* lifecycle.createCheckpoint(SOURCE, {
          name: Option.some("split"),
          lifetimeMs: Option.none(),
        });
        yield* lifecycle.forkRange({
          source: SOURCE,
          checkpointId: checkpoint.id,
          range: LEFT_RANGE,
          forkId: LEFT,
        });
        // Retrying a fork is safe.
        yield* lifecycle.forkRange({
          source: SOURCE,
          checkpointId: checkpoint.id,
          range: LEFT_RANGE,
          forkId: LEFT,
        });
        yield* lifecycle.forkRange({
          source: SOURCE,
          checkpointId: checkpoint.id,
          range: RIGHT_RANGE,
          forkId: RIGHT,
        });
        const left = yield* valuesIn(LEFT);
        const right = yield* valuesIn(RIGHT);
        const outOfRange = yield* Effect.flip(client.put(LEFT, bytes("x"), bytes("X")));
        yield* client.put(RIGHT, bytes("x"), bytes("X"));
        const reader = yield* ManifestReader;
        const holdsBefore = (yield* reader.loadLatest(SOURCE)).manifest.checkpoints.length;
        const blocked = yield* Effect.flip(lifecycle.remove(SOURCE));

        // Each fork's compactor rewrites the source's SSTs into its own, then releases its hold.
        yield* compactUntilIdle(LEFT);
        yield* compactUntilIdle(RIGHT);
        const leftManifest = (yield* reader.loadLatest(LEFT)).manifest;
        const holdsAfter = (yield* reader.loadLatest(SOURCE)).manifest.checkpoints.length;
        yield* lifecycle.deleteCheckpoint(SOURCE, checkpoint.id);
        yield* lifecycle.remove(SOURCE);
        const remaining = yield* (yield* ObjectStore).list(databasePrefix(SOURCE), Option.none());
        const gone = yield* Effect.flip(reader.loadLatest(SOURCE));
        const { destroyed } = yield* Ref.get(yield* CompactorClientRecord);
        return {
          lateWrite,
          left,
          right,
          outOfRange,
          holdsBefore,
          blocked,
          leftOwners: manifestSstHandles(leftManifest).map((sst) =>
            Option.getOrUndefined(sst.owner),
          ),
          leftRefs: leftManifest.externalRefs,
          holdsAfter,
          remaining: remaining.objects.length,
          gone,
          destroyed,
          leftAfter: yield* valuesIn(LEFT),
          rightAfter: yield* valuesIn(RIGHT),
        };
      }),
    );
    expect(outcome.lateWrite._tag).toBe("DatabaseSealedError");
    // Every acknowledged write is in exactly one successor.
    expect(outcome.left).toEqual(["a=A", "b=B", "c=C"]);
    expect(outcome.right).toEqual(["d=D", "e=E", "f=F"]);
    expect(outcome.outOfRange._tag).toBe("KeyOutOfRangeError");
    // The caller's checkpoint plus one hold per fork.
    expect(outcome.holdsBefore).toBe(3);
    expect(outcome.blocked._tag).toBe("DatabaseHasDependentsError");
    expect(outcome.leftOwners.every((owner) => owner === undefined)).toBe(true);
    expect(outcome.leftRefs).toEqual([]);
    expect(outcome.holdsAfter).toBe(1);
    expect(outcome.remaining).toBe(0);
    expect(outcome.gone._tag).toBe("DatabaseNotFoundError");
    expect(outcome.destroyed).toEqual([SOURCE]);
    expect(outcome.leftAfter).toEqual(["a=A", "b=B", "c=C"]);
    expect(outcome.rightAfter).toEqual(["d=D", "e=E", "f=F", "x=X"]);
  });

  it("removing a fork releases its hold on the source", async () => {
    const holds = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        yield* lifecycle.create(SOURCE, {
          keyRange: Option.none(),
          databaseOptions: DEFAULT_DATABASE_OPTIONS,
        });
        yield* client.put(SOURCE, bytes("a"), bytes("A"));
        const checkpoint = yield* lifecycle.createCheckpoint(SOURCE, {
          name: Option.none(),
          lifetimeMs: Option.none(),
        });
        yield* lifecycle.forkRange({
          source: SOURCE,
          checkpointId: checkpoint.id,
          range: LEFT_RANGE,
          forkId: LEFT,
        });
        // The unflushed write was flushed before the checkpoint, so the fork has it.
        const forked = yield* valuesIn(LEFT);
        const reader = yield* ManifestReader;
        const before = (yield* reader.loadLatest(SOURCE)).manifest.checkpoints.length;
        yield* lifecycle.remove(LEFT);
        const after = (yield* reader.loadLatest(SOURCE)).manifest.checkpoints.length;
        // The source is removable once only its own checkpoint is left and that is deleted.
        yield* lifecycle.deleteCheckpoint(SOURCE, checkpoint.id);
        yield* lifecycle.remove(SOURCE);
        return { forked, before, after };
      }),
    );
    expect(holds).toEqual({ forked: ["a=A"], before: 2, after: 1 });
  });

  it("refuses a fork from a checkpoint that does not exist", async () => {
    const error = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        yield* lifecycle.create(SOURCE, {
          keyRange: Option.none(),
          databaseOptions: DEFAULT_DATABASE_OPTIONS,
        });
        const checkpoint = yield* lifecycle.createCheckpoint(SOURCE, {
          name: Option.none(),
          lifetimeMs: Option.none(),
        });
        yield* lifecycle.deleteCheckpoint(SOURCE, checkpoint.id);
        return yield* Effect.flip(
          lifecycle.forkRange({
            source: SOURCE,
            checkpointId: checkpoint.id,
            range: LEFT_RANGE,
            forkId: LEFT,
          }),
        );
      }),
    );
    expect(error._tag).toBe("CheckpointNotFoundError");
  });

  it("keeps a pinned snapshot readable past gcMinAge until its checkpoint expires; an unpinned one expires (D52)", async () => {
    const HOUR = 3_600_000;
    const result = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const gc = yield* GarbageCollector;
        yield* lifecycle.create(SOURCE, {
          keyRange: Option.none(),
          databaseOptions: DEFAULT_DATABASE_OPTIONS,
        });
        yield* client.put(SOURCE, bytes("a"), bytes("0"));
        yield* client.flush(SOURCE);
        const unpinned = yield* client.snapshot(SOURCE);
        // The pin flushes this write first, so it pins a newer manifest than the unpinned one.
        yield* client.put(SOURCE, bytes("a"), bytes("1"));
        const pinned = yield* lifecycle.pinSnapshot(SOURCE, 3 * HOUR);
        const readAt = (snapshot: typeof unpinned) =>
          client
            .get(SOURCE, bytes("a"), {
              consistency: { _tag: "Flushed" },
              snapshot: Option.some(snapshot),
              refuseSealed: false,
            })
            .pipe(
              Effect.map((value) => Option.getOrElse(Option.map(value, text), () => "none")),
              Effect.catchTag("ReaderUnavailableError", (error) => Effect.succeed(error.reason)),
            );
        const fresh = { unpinned: yield* readAt(unpinned), pinned: yield* readAt(pinned.snapshot) };
        // Newer manifests replace both snapshots' manifest.
        for (const value of ["2", "3"]) {
          yield* client.put(SOURCE, bytes("a"), bytes(value));
          yield* client.flush(SOURCE);
        }
        yield* TestClock.adjust(HOUR + 1);
        yield* gc.collect(SOURCE);
        const afterAnHour = {
          unpinned: yield* readAt(unpinned),
          pinned: yield* readAt(pinned.snapshot),
        };
        // Past its lifetime the checkpoint is dropped, and its manifest collected like any other.
        yield* TestClock.adjust(2 * HOUR);
        yield* gc.collect(SOURCE);
        yield* gc.collect(SOURCE);
        return { fresh, afterAnHour, afterExpiry: yield* readAt(pinned.snapshot), pinned };
      }),
    );
    expect(result.fresh).toEqual({ unpinned: "0", pinned: "1" });
    expect(result.afterAnHour).toEqual({ unpinned: "SnapshotExpired", pinned: "1" });
    expect(result.afterExpiry).toBe("SnapshotExpired");
    expect(result.pinned.expireAt).toBeGreaterThan(0);
  });
});
