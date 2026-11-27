import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  type KeyRange,
  ManifestId,
  manifestSstHandles,
  SeqNum,
} from "@shaledb/lsm-core";
import { Clock, Context, Effect, Layer, Option, Ref } from "effect";
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
import { DbClient, type DbSnapshot, dbClientTestLayer } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { DurableObjectAlarm, durableObjectAlarmTestLayer } from "./durable-object-alarm.ts";
import { GarbageCollector, makeGarbageCollector } from "./garbage-collector.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import { databasePrefix, manifestKey, manifestPrefix } from "./object-keys.ts";
import { ObjectStore, ObjectStoreUnavailableError, objectStoreTestLayer } from "./object-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";
import { WriterClient } from "./writer-client.ts";

const SOURCE = DatabaseId.make("orders");
const LEFT = DatabaseId.make("orders-a");
const RIGHT = DatabaseId.make("orders-b");
const bytes = (text: string) => new TextEncoder().encode(text);
const text = (value: Uint8Array) => new TextDecoder().decode(value);
const SPLIT = bytes("d");
const LEFT_RANGE: KeyRange = { start: new Uint8Array(0), end: Option.some(SPLIT) };
const RIGHT_RANGE: KeyRange = { start: SPLIT, end: Option.none() };
const FULL_RANGE: KeyRange = { start: new Uint8Array(0), end: Option.none() };

/**
 * Faults injected into the bucket, none by default: a `get` that finds nothing (an object deleted
 * meanwhile), a `get` that fails, and a create-if-absent that fails (R2 unavailable after retries).
 */
interface StoreFaultRules {
  readonly hideGet: (key: string) => boolean;
  readonly failGet: (key: string) => boolean;
  readonly failCreate: (key: string, body: string) => boolean;
}

const NO_STORE_FAULTS: StoreFaultRules = {
  hideGet: () => false,
  failGet: () => false,
  failCreate: () => false,
};

/** The current fault rules. */
class StoreFaults extends Context.Service<StoreFaults, Ref.Ref<StoreFaultRules>>()(
  "test/StoreFaults",
) {}

const storeFaultsLayer = Layer.effect(StoreFaults, Ref.make(NO_STORE_FAULTS));

/** An in-memory bucket that applies `StoreFaults` to manifest reads and creates. */
const faultyObjectStoreLayer = Layer.effect(
  ObjectStore,
  Effect.gen(function* () {
    const faults = yield* StoreFaults;
    const inner = Context.get(yield* Layer.build(objectStoreTestLayer), ObjectStore);
    const rules = Ref.get(faults);
    const injected = (operation: "get" | "putIfAbsent", key: string) =>
      new ObjectStoreUnavailableError({ operation, key, message: "injected R2 failure" });
    return ObjectStore.of({
      ...inner,
      get: (key) =>
        Effect.gen(function* () {
          const rule = yield* rules;
          if (rule.failGet(key)) return yield* injected("get", key);
          if (rule.hideGet(key)) return Option.none();
          return yield* inner.get(key);
        }),
      putIfAbsent: (key, body) =>
        Effect.gen(function* () {
          const rule = yield* rules;
          if (rule.failCreate(key, new TextDecoder().decode(body)))
            return yield* injected("putIfAbsent", key);
          return yield* inner.putIfAbsent(key, body);
        }),
    });
  }),
);

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
  Layer.provideMerge(
    dbClientTestLayer(
      { blockSize: 64 },
      faultyObjectStoreLayer.pipe(Layer.provide(storeFaultsLayer)),
    ),
  ),
  Layer.provideMerge(storeFaultsLayer),
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

/**
 * One database's compactor kept across the test. It is woken and its compaction polls (seconds
 * apart) are run, moving the test clock to each, until only a collection due `gcMinAge` after a
 * compaction is left. `runDue` then fires its alarm when due at the test clock's time.
 */
const compactorOf = (databaseId: DatabaseId) =>
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
    /** Fires the alarm while it is due within `aheadMs`, moving the clock to it. */
    const fireWithin = (aheadMs: number) =>
      Effect.gen(function* () {
        for (let poll = 0; poll < 20; poll++) {
          const scheduled = yield* alarm.scheduled;
          const now = yield* Clock.currentTimeMillis;
          if (Option.isNone(scheduled) || scheduled.value > now + aheadMs) break;
          if (scheduled.value > now) yield* TestClock.adjust(scheduled.value - now);
          yield* alarm.cancel;
          yield* compactor.runAlarm;
        }
      });
    yield* compactor.wake(databaseId);
    yield* fireWithin(60_000);
    return { runDue: fireWithin(0) };
  });

/** Reads `key` at `snapshot`: its value, `none`, or the reason the reader refused. */
const readAtSnapshot = (databaseId: DatabaseId, key: string, snapshot: DbSnapshot) =>
  Effect.gen(function* () {
    const client = yield* DbClient;
    return yield* client
      .get(databaseId, bytes(key), {
        consistency: { _tag: "Flushed" },
        snapshot: Option.some(snapshot),
        refuseSealed: false,
      })
      .pipe(
        Effect.map((value) => Option.getOrElse(Option.map(value, text), () => "none")),
        Effect.catchTag("ReaderUnavailableError", (error) => Effect.succeed(error.reason)),
      );
  });

const HOUR = 3_600_000;

/** The source's checkpoints and the fork's external references, as one value to compare. */
const holdsAndRefs = Effect.gen(function* () {
  const reader = yield* ManifestReader;
  return {
    holds: (yield* reader.loadLatest(SOURCE)).manifest.checkpoints.length,
    refs: (yield* reader.loadLatest(LEFT)).manifest.externalRefs.length,
  };
});

/**
 * The source with two flushed keys, forked into `LEFT` (the caller's checkpoint deleted), and the
 * fork's rewrite done. Returns the fork's compactor, whose collection is due gcMinAge later.
 */
const forkAndRewrite = Effect.gen(function* () {
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
  yield* lifecycle.forkRange({
    source: SOURCE,
    checkpointId: checkpoint.id,
    range: LEFT_RANGE,
    forkId: LEFT,
  });
  yield* lifecycle.deleteCheckpoint(SOURCE, checkpoint.id);
  return yield* compactorOf(LEFT);
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
  it("a transaction begun before a seal cannot commit after it, in the source or its successors", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        yield* lifecycle.create(SOURCE, {
          keyRange: Option.none(),
          databaseOptions: DEFAULT_DATABASE_OPTIONS,
        });
        yield* client.put(SOURCE, bytes("a"), bytes("A"));
        // The transaction reads "a" and will write "b" (left) and "x" (right).
        const begin = yield* client.beginTransaction(SOURCE);
        const read = yield* client.getAt(SOURCE, bytes("a"), begin.readSeq, begin.manifestId);
        // The split: seal, one checkpoint, a fork per successor.
        yield* lifecycle.seal(SOURCE, [LEFT, RIGHT], false);
        const checkpoint = yield* lifecycle.createCheckpoint(SOURCE, {
          name: Option.some("split"),
          lifetimeMs: Option.none(),
        });
        for (const [forkId, range] of [
          [LEFT, LEFT_RANGE],
          [RIGHT, RIGHT_RANGE],
        ] as const) {
          yield* lifecycle.forkRange({
            source: SOURCE,
            checkpointId: checkpoint.id,
            range,
            forkId,
          });
        }
        const commit = yield* Effect.flip(
          client.commitTransaction(SOURCE, {
            readSeq: begin.readSeq,
            isolation: "SerializableSnapshot",
            batch: {
              operations: [
                { _tag: "Put", key: bytes("b"), value: bytes("T"), ttlMillis: Option.none() },
                { _tag: "Put", key: bytes("x"), value: bytes("T"), ttlMillis: Option.none() },
              ],
            },
            readKeys: [bytes("a")],
            readRanges: [],
          }),
        );
        return {
          read: Option.map(read, (row) =>
            row.value._tag === "Value" ? text(row.value.bytes) : "",
          ),
          commit: commit._tag,
          successors: commit._tag === "DatabaseSealedError" ? commit.successors : [],
          source: yield* valuesIn(SOURCE),
          left: yield* valuesIn(LEFT),
          right: yield* valuesIn(RIGHT),
        };
      }),
    );
    expect(outcome).toEqual({
      read: Option.some("A"),
      commit: "DatabaseSealedError",
      successors: [LEFT, RIGHT],
      source: ["a=A"],
      left: ["a=A"],
      right: [],
    });
  });

  it("seals, forks a split, rewrites, releases the forks' holds gcMinAge later, then removes the source", async () => {
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

        // Each fork's compactor rewrites the source's SSTs into its own. It keeps its hold while a
        // manifest a reader may still hold (replaced less than gcMinAge ago) names the source's
        // SSTs (D77), so the source cannot be removed yet even without the caller's checkpoint.
        const leftCompactor = yield* compactorOf(LEFT);
        const rightCompactor = yield* compactorOf(RIGHT);
        const leftRewritten = (yield* reader.loadLatest(LEFT)).manifest;
        const holdsAfterRewrite = (yield* reader.loadLatest(SOURCE)).manifest.checkpoints.length;
        yield* lifecycle.deleteCheckpoint(SOURCE, checkpoint.id);
        const blockedAfterRewrite = yield* Effect.flip(lifecycle.remove(SOURCE));
        // gcMinAge later, each fork's due collection releases its hold, with no other wake.
        yield* TestClock.adjust(HOUR);
        yield* leftCompactor.runDue;
        yield* rightCompactor.runDue;
        const leftManifest = (yield* reader.loadLatest(LEFT)).manifest;
        const holdsAfter = (yield* reader.loadLatest(SOURCE)).manifest.checkpoints.length;
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
          leftRefsAfterRewrite: leftRewritten.externalRefs.length,
          holdsAfterRewrite,
          blockedAfterRewrite,
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
    expect(outcome.leftRefsAfterRewrite).toBe(1);
    expect(outcome.holdsAfterRewrite).toBe(3);
    expect(outcome.blockedAfterRewrite._tag).toBe("DatabaseHasDependentsError");
    expect(outcome.leftRefs).toEqual([]);
    expect(outcome.holdsAfter).toBe(0);
    expect(outcome.remaining).toBe(0);
    expect(outcome.gone._tag).toBe("DatabaseNotFoundError");
    expect(outcome.destroyed).toEqual([SOURCE]);
    expect(outcome.leftAfter).toEqual(["a=A", "b=B", "c=C"]);
    expect(outcome.rightAfter).toEqual(["d=D", "e=E", "f=F", "x=X"]);
  });

  it("keeps a fork's hold while a snapshot from before its rewrite may be read, then releases it (D77)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const reader = yield* ManifestReader;
        const gc = yield* GarbageCollector;
        yield* lifecycle.create(SOURCE, {
          keyRange: Option.none(),
          databaseOptions: DEFAULT_DATABASE_OPTIONS,
        });
        for (const key of ["a", "b"]) {
          yield* client.put(SOURCE, bytes(key), bytes(key.toUpperCase()));
        }
        yield* client.flush(SOURCE);
        const checkpoint = yield* lifecycle.createCheckpoint(SOURCE, {
          name: Option.some("fork"),
          lifetimeMs: Option.none(),
        });
        yield* lifecycle.forkRange({
          source: SOURCE,
          checkpointId: checkpoint.id,
          range: LEFT_RANGE,
          forkId: LEFT,
        });
        yield* lifecycle.deleteCheckpoint(SOURCE, checkpoint.id);
        // The source moves on: its newest manifest no longer names the SSTs the fork reads.
        for (let round = 0; round < 4; round++) {
          yield* client.put(SOURCE, bytes(`z${round}`), bytes("Z"));
          yield* client.flush(SOURCE);
        }
        yield* compactUntilIdle(SOURCE);
        // Hours later a client of the fork takes a snapshot: readable for gcMinAge (D52).
        yield* TestClock.adjust(3 * HOUR);
        const snapshot = yield* client.snapshot(LEFT);
        // The fork's compactor rewrites the source's SSTs into its own.
        const left = yield* compactorOf(LEFT);
        const rewritten = (yield* reader.loadLatest(LEFT)).manifest;
        // Seconds after the rewrite the fork collects (an operator's collect-now, or the
        // maintenance sweep's request), then the source collects and an operator tries to remove it.
        yield* gc.collect(LEFT, Option.none());
        yield* gc.collect(SOURCE, Option.none());
        const removeAttempt = yield* Effect.result(lifecycle.remove(SOURCE));
        const soon = {
          holds: (yield* reader.loadLatest(SOURCE)).manifest.checkpoints.length,
          refs: rewritten.externalRefs.length,
          read: yield* readAtSnapshot(LEFT, "b", snapshot),
          retry: yield* readAtSnapshot(LEFT, "b", snapshot),
        };
        // Just under gcMinAge later the snapshot is still promised, and still readable.
        yield* TestClock.adjust(HOUR - 10_000);
        yield* left.runDue;
        yield* gc.collect(LEFT, Option.none());
        yield* gc.collect(SOURCE, Option.none());
        const late = {
          holds: (yield* reader.loadLatest(SOURCE)).manifest.checkpoints.length,
          read: yield* readAtSnapshot(LEFT, "b", snapshot),
        };
        // At gcMinAge the fork's own due collection releases the hold; the source is removable, and
        // the snapshot expires as documented.
        yield* TestClock.adjust(10_000);
        yield* left.runDue;
        const released = {
          holds: (yield* reader.loadLatest(SOURCE)).manifest.checkpoints.length,
          refs: (yield* reader.loadLatest(LEFT)).manifest.externalRefs.length,
        };
        const finalRemove = yield* Effect.result(lifecycle.remove(SOURCE));
        return {
          finalRemove: finalRemove._tag,
          rewrittenOwners: manifestSstHandles(rewritten).map((sst) =>
            Option.getOrUndefined(sst.owner),
          ),
          removeAttempt: removeAttempt._tag === "Success" ? "removed" : removeAttempt.failure._tag,
          soon,
          late,
          released,
          expired: yield* readAtSnapshot(LEFT, "b", snapshot),
          latest: yield* valuesIn(LEFT),
        };
      }),
    );
    expect(outcome.rewrittenOwners.every((owner) => owner === undefined)).toBe(true);
    expect(outcome.removeAttempt).toBe("DatabaseHasDependentsError");
    expect(outcome.finalRemove).toBe("Success");
    expect(outcome.soon).toEqual({ holds: 1, refs: 1, read: "B", retry: "B" });
    expect(outcome.late).toEqual({ holds: 1, read: "B" });
    expect(outcome.released).toEqual({ holds: 0, refs: 0 });
    expect(outcome.expired).toBe("SnapshotExpired");
    expect(outcome.latest).toEqual(["a=A", "b=B"]);
  });

  it("keeps a fork of a fork's holds down the chain while any kept manifest names the original owner (D13, D77)", async () => {
    const MIDDLE = LEFT;
    const GRANDCHILD = DatabaseId.make("orders-c");
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const reader = yield* ManifestReader;
        const gc = yield* GarbageCollector;
        const holdsOn = (databaseId: DatabaseId) =>
          Effect.map(reader.loadLatest(databaseId), (latest) => latest.manifest.checkpoints.length);
        const refsOf = (databaseId: DatabaseId) =>
          Effect.map(
            reader.loadLatest(databaseId),
            (latest) => latest.manifest.externalRefs.length,
          );
        const forkOf = (source: DatabaseId, forkId: DatabaseId) =>
          Effect.gen(function* () {
            const checkpoint = yield* lifecycle.createCheckpoint(source, {
              name: Option.none(),
              lifetimeMs: Option.none(),
            });
            yield* lifecycle.forkRange({
              source,
              checkpointId: checkpoint.id,
              range: LEFT_RANGE,
              forkId,
            });
            yield* lifecycle.deleteCheckpoint(source, checkpoint.id);
          });
        yield* lifecycle.create(SOURCE, {
          keyRange: Option.none(),
          databaseOptions: DEFAULT_DATABASE_OPTIONS,
        });
        for (const key of ["a", "b"]) {
          yield* client.put(SOURCE, bytes(key), bytes(key.toUpperCase()));
        }
        yield* client.flush(SOURCE);
        yield* forkOf(SOURCE, MIDDLE);
        // The grandchild forks the middle database before its rewrite: it reads the original
        // owner's SSTs under the owner's id, through its hold on the middle database.
        yield* forkOf(MIDDLE, GRANDCHILD);
        const grandchildOwners = manifestSstHandles(
          (yield* reader.loadLatest(GRANDCHILD)).manifest,
        ).map((sst) => Option.getOrUndefined(sst.owner));
        // The middle database rewrites; gcMinAge later it still holds the owner, because the
        // manifest its fork's hold pins names the owner's SSTs. The grandchild's collection before
        // its own rewrite keeps its hold on the middle database too.
        const middle = yield* compactorOf(MIDDLE);
        yield* TestClock.adjust(HOUR);
        yield* middle.runDue;
        yield* gc.collect(GRANDCHILD, Option.none());
        const middleRewritten = {
          onOwner: yield* holdsOn(SOURCE),
          onMiddle: yield* holdsOn(MIDDLE),
        };
        // The grandchild rewrites; it keeps its hold on the middle database for gcMinAge.
        const grandchild = yield* compactorOf(GRANDCHILD);
        const grandchildRewritten = {
          onOwner: yield* holdsOn(SOURCE),
          onMiddle: yield* holdsOn(MIDDLE),
          removeOwner: (yield* Effect.flip(lifecycle.remove(SOURCE)))._tag,
        };
        yield* TestClock.adjust(HOUR);
        yield* grandchild.runDue;
        const grandchildReleased = {
          onOwner: yield* holdsOn(SOURCE),
          onMiddle: yield* holdsOn(MIDDLE),
          grandchildRefs: yield* refsOf(GRANDCHILD),
        };
        // The middle database's next collection (the maintenance sweep asks for it) releases the
        // owner: none of its kept manifests names the owner's SSTs any more.
        yield* gc.collect(MIDDLE, Option.none());
        const middleReleased = {
          onOwner: yield* holdsOn(SOURCE),
          middleRefs: yield* refsOf(MIDDLE),
        };
        yield* lifecycle.remove(SOURCE);
        return {
          grandchildOwners,
          middleRewritten,
          grandchildRewritten,
          grandchildReleased,
          middleReleased,
          middleValues: yield* valuesIn(MIDDLE),
          grandchildValues: yield* valuesIn(GRANDCHILD),
        };
      }),
    );
    expect(outcome.grandchildOwners).toEqual([SOURCE]);
    expect(outcome.middleRewritten).toEqual({ onOwner: 1, onMiddle: 1 });
    expect(outcome.grandchildRewritten).toEqual({
      onOwner: 1,
      onMiddle: 1,
      removeOwner: "DatabaseHasDependentsError",
    });
    expect(outcome.grandchildReleased).toEqual({ onOwner: 1, onMiddle: 0, grandchildRefs: 0 });
    expect(outcome.middleReleased).toEqual({ onOwner: 0, middleRefs: 0 });
    expect(outcome.middleValues).toEqual(["a=A", "b=B"]);
    expect(outcome.grandchildValues).toEqual(["a=A", "b=B"]);
  });

  it("releases nothing while a manifest the fork's collection keeps cannot be loaded (D77)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const reader = yield* ManifestReader;
        const gc = yield* GarbageCollector;
        const faults = yield* StoreFaults;
        const left = yield* forkAndRewrite;
        const rewriteId = (yield* reader.loadLatest(LEFT)).id;
        // The manifests before the rewrite are young: GC keeps them, and they name the source's
        // SSTs. Their objects vanish between GC's listing and its loads (as if deleted meanwhile).
        const beforeRewrite = new Set(
          Array.from({ length: rewriteId - 1 }, (_, index) =>
            manifestKey(LEFT, ManifestId.make(index + 1)),
          ),
        );
        yield* Ref.set(faults, { ...NO_STORE_FAULTS, hideGet: (key) => beforeRewrite.has(key) });
        // Above GC's manifest boundary nothing but a removal deletes a manifest, so the collection
        // fails, deleting and releasing nothing (D83).
        const unexplained = yield* Effect.flip(gc.collect(LEFT, Option.none()));
        const whileUnexplained = yield* holdsAndRefs;
        // A later collection moved the boundary past them and deleted them: this one goes on
        // without them, and still releases nothing, because it cannot know what they named.
        yield* (yield* ManifestStore).advanceManifestBoundary(LEFT, ManifestId.make(rewriteId - 1));
        yield* gc.collect(LEFT, Option.none());
        const whileMissing = yield* holdsAndRefs;
        // Their loads fail instead: the collection fails, and releases nothing either.
        yield* Ref.set(faults, { ...NO_STORE_FAULTS, failGet: (key) => beforeRewrite.has(key) });
        const failed = yield* Effect.flip(gc.collect(LEFT, Option.none()));
        const whileFailing = yield* holdsAndRefs;
        // Storage answers again; gcMinAge after the rewrite the fork's due collection releases.
        yield* Ref.set(faults, NO_STORE_FAULTS);
        yield* TestClock.adjust(HOUR);
        yield* left.runDue;
        return {
          unexplained: unexplained._tag,
          whileUnexplained,
          whileMissing,
          failed: failed._tag,
          whileFailing,
          after: yield* holdsAndRefs,
        };
      }),
    );
    expect(outcome.unexplained).toBe("GarbageCollectorUnavailableError");
    expect(outcome.whileUnexplained).toEqual({ holds: 1, refs: 1 });
    expect(outcome.whileMissing).toEqual({ holds: 1, refs: 1 });
    expect(outcome.failed).toBe("GarbageCollectorUnavailableError");
    expect(outcome.whileFailing).toEqual({ holds: 1, refs: 1 });
    expect(outcome.after).toEqual({ holds: 0, refs: 0 });
  });

  it("finishes a release interrupted between its two steps at the next collection (D77)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const gc = yield* GarbageCollector;
        const faults = yield* StoreFaults;
        yield* forkAndRewrite;
        yield* TestClock.adjust(HOUR);
        // The fork's manifest without its reference cannot be written: the collection stops after
        // removing the source's checkpoint, as a crash between the two steps would.
        yield* Ref.set(faults, {
          ...NO_STORE_FAULTS,
          failCreate: (key, body) =>
            key.startsWith(manifestPrefix(LEFT)) && body.includes(`"externalRefs":[]`),
        });
        const failed = yield* Effect.flip(gc.collect(LEFT, Option.none()));
        const interrupted = yield* holdsAndRefs;
        yield* Ref.set(faults, NO_STORE_FAULTS);
        yield* gc.collect(LEFT, Option.none());
        const finished = yield* holdsAndRefs;
        yield* lifecycle.remove(SOURCE);
        return { failed: failed._tag, interrupted, finished };
      }),
    );
    expect(outcome.failed).toBe("GarbageCollectorUnavailableError");
    // The source's checkpoint went first; the fork's reference is still there to retry from.
    expect(outcome.interrupted).toEqual({ holds: 0, refs: 1 });
    expect(outcome.finished).toEqual({ holds: 0, refs: 0 });
  });

  it("keeps the holds of a split successor that is split again before its rewrite (D57, D77)", async () => {
    // ShardSplit's own calls: one seal that takes each half a hold, then a fork from each hold.
    const A1 = DatabaseId.make("orders-a1");
    const A2 = DatabaseId.make("orders-a2");
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const reader = yield* ManifestReader;
        const gc = yield* GarbageCollector;
        const holdsOn = (databaseId: DatabaseId) =>
          Effect.map(reader.loadLatest(databaseId), (latest) => latest.manifest.checkpoints.length);
        const refsOf = (databaseId: DatabaseId) =>
          Effect.map(
            reader.loadLatest(databaseId),
            (latest) => latest.manifest.externalRefs.length,
          );
        const splitLike = (
          databaseId: DatabaseId,
          halves: readonly [DatabaseId, DatabaseId],
          ranges: readonly [KeyRange, KeyRange],
        ) =>
          Effect.gen(function* () {
            const sealed = yield* lifecycle.seal(databaseId, halves, true);
            for (const [index, forkId] of halves.entries()) {
              const hold = sealed.successorHolds.find((held) => held.successor === forkId);
              if (hold === undefined) return yield* Effect.die(`no hold for ${forkId}`);
              yield* lifecycle.forkRange({
                source: databaseId,
                checkpointId: hold.checkpointId,
                range: ranges[index] ?? FULL_RANGE,
                forkId,
              });
            }
          });
        yield* lifecycle.create(SOURCE, {
          keyRange: Option.none(),
          databaseOptions: DEFAULT_DATABASE_OPTIONS,
        });
        for (const key of ["a", "b", "c", "e"]) {
          yield* client.put(SOURCE, bytes(key), bytes(key.toUpperCase()));
        }
        yield* splitLike(SOURCE, [LEFT, RIGHT], [LEFT_RANGE, RIGHT_RANGE]);
        // The left half is split again before its compactor rewrote anything: its halves read the
        // original shard's SSTs through their holds on the left half.
        const atB: KeyRange = { start: new Uint8Array(0), end: Option.some(bytes("b")) };
        const fromB: KeyRange = { start: bytes("b"), end: Option.some(SPLIT) };
        yield* splitLike(LEFT, [A1, A2], [atB, fromB]);
        const a1Owners = manifestSstHandles((yield* reader.loadLatest(A1)).manifest).map((sst) =>
          Option.getOrUndefined(sst.owner),
        );
        // The sealed left half and the right half rewrite; gcMinAge later the right half has
        // released the shard, but the left half keeps its hold: its halves' holds pin its sealed
        // manifest, which names the shard's SSTs.
        const left = yield* compactorOf(LEFT);
        const right = yield* compactorOf(RIGHT);
        yield* TestClock.adjust(HOUR);
        yield* left.runDue;
        yield* right.runDue;
        // The second split's halves collect before their own rewrite (an operator's collect-now, or
        // a due collection); they still read the shard's SSTs through the left half, so keep it.
        yield* gc.collect(A1, Option.none());
        yield* gc.collect(A2, Option.none());
        const middleRewritten = {
          onShard: yield* holdsOn(SOURCE),
          onLeft: yield* holdsOn(LEFT),
          leftRefs: yield* refsOf(LEFT),
        };
        // The second split's halves rewrite; they keep their holds for gcMinAge.
        const a1 = yield* compactorOf(A1);
        const a2 = yield* compactorOf(A2);
        const halvesRewritten = {
          onShard: yield* holdsOn(SOURCE),
          onLeft: yield* holdsOn(LEFT),
          removeShard: (yield* Effect.flip(lifecycle.remove(SOURCE)))._tag,
          removeLeft: (yield* Effect.flip(lifecycle.remove(LEFT)))._tag,
        };
        yield* TestClock.adjust(HOUR);
        yield* a1.runDue;
        yield* a2.runDue;
        const halvesReleased = { onShard: yield* holdsOn(SOURCE), onLeft: yield* holdsOn(LEFT) };
        // The left half's next collection (the maintenance sweep asks for it) releases the shard.
        yield* gc.collect(LEFT, Option.none());
        const leftReleased = { onShard: yield* holdsOn(SOURCE), leftRefs: yield* refsOf(LEFT) };
        yield* lifecycle.remove(LEFT);
        yield* lifecycle.remove(SOURCE);
        return {
          a1Owners,
          middleRewritten,
          halvesRewritten,
          halvesReleased,
          leftReleased,
          values: {
            a1: yield* valuesIn(A1),
            a2: yield* valuesIn(A2),
            right: yield* valuesIn(RIGHT),
          },
        };
      }),
    );
    expect(outcome.a1Owners).toEqual([SOURCE]);
    expect(outcome.middleRewritten).toEqual({ onShard: 1, onLeft: 2, leftRefs: 1 });
    expect(outcome.halvesRewritten).toEqual({
      onShard: 1,
      onLeft: 2,
      removeShard: "DatabaseHasDependentsError",
      removeLeft: "DatabaseHasDependentsError",
    });
    expect(outcome.halvesReleased).toEqual({ onShard: 1, onLeft: 0 });
    expect(outcome.leftReleased).toEqual({ onShard: 0, leftRefs: 0 });
    expect(outcome.values).toEqual({ a1: ["a=A"], a2: ["b=B", "c=C"], right: ["e=E"] });
  });

  it("a consumer follows the feed through a split: the sealed source to its last sequence, then each successor from it (D80)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const writers = yield* WriterClient;
        yield* lifecycle.create(SOURCE, {
          keyRange: Option.none(),
          databaseOptions: {
            ...DEFAULT_DATABASE_OPTIONS,
            changelog: Option.some({ retentionMs: 3_600_000 }),
          },
        });
        for (const key of ["a", "b", "c", "d", "e", "f"]) {
          yield* client.put(SOURCE, bytes(key), bytes(key.toUpperCase()));
          if (key === "c") yield* client.flush(SOURCE);
        }
        const receipt = yield* lifecycle.seal(SOURCE, [LEFT, RIGHT], false);
        const checkpoint = yield* lifecycle.createCheckpoint(SOURCE, {
          name: Option.some("split"),
          lifetimeMs: Option.none(),
        });
        for (const [forkId, range] of [
          [LEFT, LEFT_RANGE],
          [RIGHT, RIGHT_RANGE],
        ] as const) {
          yield* lifecycle.forkRange({
            source: SOURCE,
            checkpointId: checkpoint.id,
            range,
            forkId,
          });
        }
        yield* client.put(LEFT, bytes("b"), bytes("B2"));
        yield* client.put(RIGHT, bytes("x"), bytes("X"));
        const keysOf = (rows: ReadonlyArray<{ readonly key: Uint8Array }>) =>
          rows.map((row) => text(row.key));
        // 1. The sealed source's feed, to its end: no rows after its last sequence number.
        const sourcePage = yield* client.changes(SOURCE, SeqNum.make(0), 100);
        const sourceEnd = yield* client.changes(SOURCE, sourcePage.nextAfterSeq, 100);
        const sourceStatus = yield* writers.status(SOURCE);
        // 2. Each successor's feed from the source's last sequence number (the fork point).
        const from = sourceStatus.lastSeq;
        const left = yield* client.changes(LEFT, from, 100);
        const right = yield* client.changes(RIGHT, from, 100);
        // Below the fork point a successor's feed is Expired: those changes are the source's.
        const leftBelow = yield* Effect.flip(client.changes(LEFT, SeqNum.make(from - 1), 100));
        return {
          sealedSeq: receipt.flushedSeq,
          source: keysOf(sourcePage.rows),
          sourceEnd: sourceEnd.rows.length,
          sealed: sourceStatus.sealed,
          from,
          left: keysOf(left.rows),
          right: keysOf(right.rows),
          leftBelow: leftBelow._tag === "ChangelogUnavailableError" && leftBelow.reason,
        };
      }),
    );
    expect(outcome).toEqual({
      sealedSeq: 6,
      source: ["a", "b", "c", "d", "e", "f"],
      sourceEnd: 0,
      sealed: true,
      from: 6,
      left: ["b"],
      right: ["x"],
      leftBelow: "Expired",
    });
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
        yield* gc.collect(SOURCE, Option.none());
        const afterAnHour = {
          unpinned: yield* readAt(unpinned),
          pinned: yield* readAt(pinned.snapshot),
        };
        // Past its lifetime the checkpoint is dropped, and its manifest collected like any other.
        yield* TestClock.adjust(2 * HOUR);
        yield* gc.collect(SOURCE, Option.none());
        yield* gc.collect(SOURCE, Option.none());
        return { fresh, afterAnHour, afterExpiry: yield* readAt(pinned.snapshot), pinned };
      }),
    );
    expect(result.fresh).toEqual({ unpinned: "0", pinned: "1" });
    expect(result.afterAnHour).toEqual({ unpinned: "SnapshotExpired", pinned: "1" });
    expect(result.afterExpiry).toBe("SnapshotExpired");
    expect(result.pinned.expireAt).toBeGreaterThan(0);
  });
});
