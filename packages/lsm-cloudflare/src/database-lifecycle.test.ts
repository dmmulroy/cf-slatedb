import {
  addCheckpoint,
  CheckpointId,
  commitCompaction,
  commitL0Flush,
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  holdForSuccessors,
  type KeyRange,
  type Manifest,
  ManifestId,
  markRemoved,
  manifestSstHandles,
  prepareGarbageCollection,
  sealManifest,
  SeqNum,
  SortedRunId,
  SstId,
  Timestamp,
} from "@shaledb/lsm-core";
import { Clock, Context, Effect, Layer, Option, Ref, Result } from "effect";
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
import { toPublicError } from "./db-public-error-translation.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { DurableObjectAlarm, durableObjectAlarmTestLayer } from "./durable-object-alarm.ts";
import { GarbageCollector, makeGarbageCollector } from "./garbage-collector.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { MaintenanceSweep, maintenanceSweepLayerWithoutDependencies } from "./maintenance-sweep.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import {
  compactedPrefix,
  databasePrefix,
  latestManifestKey,
  manifestGcBoundaryKey,
  manifestKey,
  manifestPrefix,
} from "./object-keys.ts";
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
  /** Runs before a listing of `prefix` (once: the rule is cleared before it runs). */
  readonly beforeList: Option.Option<(prefix: string) => Effect.Effect<void>>;
}

const NO_STORE_FAULTS: StoreFaultRules = {
  hideGet: () => false,
  failGet: () => false,
  failCreate: () => false,
  beforeList: Option.none(),
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
      list: (prefix, cursor) =>
        Effect.gen(function* () {
          const rule = yield* rules;
          if (Option.isSome(rule.beforeList)) {
            yield* Ref.set(faults, { ...rule, beforeList: Option.none() });
            yield* rule.beforeList.value(prefix);
          }
          return yield* inner.list(prefix, cursor);
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

/** Whether `keys` are exactly a removal's tombstone: one manifest, the LATEST hint, GC's boundary. */
const tombstoneShape = (keys: ReadonlyArray<string>) => {
  const relative = keys.map((key) => key.slice(databasePrefix(SOURCE).length)).sort();
  return (
    relative.length === 3 &&
    relative[0] === "gc/manifest.boundary" &&
    /^manifest\/\d{20}$/.test(relative[1] ?? "") &&
    relative[2] === "manifest/LATEST"
  );
};

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
        const gone = (yield* reader.loadLatest(SOURCE)).manifest.state._tag;
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
          remaining: remaining.objects.map((object) => object.key),
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
    // Only the tombstone is left (the Removed head, its hint, GC's boundary below it); the
    // maintenance sweep deletes it gcMinAge later (D91).
    expect(tombstoneShape(outcome.remaining)).toBe(true);
    expect(outcome.gone).toBe("Removed");
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

describe("DatabaseLifecycle: recreate with the same id", () => {
  it("a reader that cached the removed database's manifest reads the new database, not the old", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const options = { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS };
        // Removal times are measured from the removal, not from the clock's start.
        yield* TestClock.adjust(5 * HOUR);
        yield* lifecycle.create(SOURCE, options);
        // Several flushes, so the old chain's newest manifest id is well above the new chain's.
        for (const round of [1, 2, 3, 4, 5]) {
          yield* client.put(SOURCE, bytes("a"), bytes(`old-${round}`));
          yield* client.put(SOURCE, bytes(`k${round}`), bytes("old"));
          yield* client.flush(SOURCE);
        }
        const latestRead = (key: string) =>
          client
            .get(SOURCE, bytes(key), {
              consistency: { _tag: "Latest" },
              snapshot: Option.none(),
              refuseSealed: false,
            })
            .pipe(
              Effect.map((value) => Option.getOrElse(Option.map(value, text), () => "none")),
              Effect.catchTag("ReaderUnavailableError", (error) => Effect.succeed(error.reason)),
            );
        const before = yield* latestRead("a");
        const flushedRead = (key: string) =>
          client
            .get(SOURCE, bytes(key), {
              consistency: { _tag: "Flushed" },
              snapshot: Option.none(),
              refuseSealed: false,
            })
            .pipe(
              Effect.map((value) => Option.getOrElse(Option.map(value, text), () => "none")),
              Effect.catchTag("ReaderUnavailableError", (error) => Effect.succeed(error.reason)),
            );
        const flushedBefore = yield* flushedRead("a");
        yield* lifecycle.remove(SOURCE);
        // The id is not reusable while readers may still hold the removed database's manifests.
        const tooSoon = yield* Effect.flip(lifecycle.create(SOURCE, options));
        yield* TestClock.adjust(HOUR - 1);
        const stillTooSoon = yield* Effect.flip(lifecycle.create(SOURCE, options));
        yield* TestClock.adjust(1);
        yield* lifecycle.create(SOURCE, options);
        yield* client.put(SOURCE, bytes("a"), bytes("new"));
        yield* client.flush(SOURCE);
        const latestAfter = yield* latestRead("a");
        const flushedAfter = yield* flushedRead("a");
        const flushedOldKey = yield* flushedRead("k1");
        // Long after any cache window.
        yield* TestClock.adjust(2 * HOUR);
        return {
          tooSoon: tooSoon._tag === "DatabaseRecentlyRemovedError" && tooSoon.retryAfterMs === HOUR,
          tooSoonPublic: toPublicError("createDatabase", "r1")(tooSoon),
          stillTooSoon: stillTooSoon._tag,
          before,
          flushedBefore,
          latestAfter,
          flushedAfter,
          flushedOldKey,
          flushedLater: yield* flushedRead("a"),
          latestOldKey: yield* latestRead("k1"),
        };
      }),
    );
    expect(outcome.tooSoon).toBe(true);
    expect(outcome.tooSoonPublic).toMatchObject({
      _tag: "DatabaseRecentlyRemovedPublicError",
      code: "DatabaseRecentlyRemoved",
      retryable: true,
      details: { databaseId: SOURCE, retryAfterMs: HOUR },
    });
    expect(outcome.stillTooSoon).toBe("DatabaseRecentlyRemovedError");
    expect(outcome.before).toBe("old-5");
    expect(outcome.flushedBefore).toBe("old-5");
    expect(outcome.latestAfter).toBe("new");
    expect(outcome.flushedAfter).toBe("new");
    expect(outcome.flushedOldKey).toBe("none");
    expect(outcome.flushedLater).toBe("new");
    expect(outcome.latestOldKey).toBe("none");
  });
});

describe("DatabaseLifecycle: reads after removal", () => {
  it("a reader that cached a removed database's manifest reports it not found", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const options = { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS };
        yield* lifecycle.create(SOURCE, options);
        yield* client.put(SOURCE, bytes("a"), bytes("old"));
        yield* client.flush(SOURCE);
        const read = (consistency: "Latest" | "Flushed") =>
          client
            .get(SOURCE, bytes("a"), {
              consistency: { _tag: consistency },
              snapshot: Option.none(),
              refuseSealed: false,
            })
            .pipe(
              Effect.map((value) => Option.getOrElse(Option.map(value, text), () => "none")),
              Effect.catchTags({
                ReaderUnavailableError: (error) => Effect.succeed(error.reason),
                DatabaseNotFoundError: () => Effect.succeed("DatabaseNotFound"),
              }),
            );
        const before = { latest: yield* read("Latest"), flushed: yield* read("Flushed") };
        yield* lifecycle.remove(SOURCE);
        // A Flushed read may lag by manifestRefreshInterval (D29); past it, removal is seen.
        yield* TestClock.adjust(1_001);
        const live = yield* Effect.flip((yield* ManifestReader).loadLive(SOURCE));
        return {
          before,
          latest: yield* read("Latest"),
          flushed: yield* read("Flushed"),
          live: live._tag,
        };
      }),
    );
    expect(outcome).toEqual({
      before: { latest: "old", flushed: "old" },
      latest: "DatabaseNotFound",
      flushed: "DatabaseNotFound",
      live: "DatabaseNotFoundError",
    });
  });
});

describe("DatabaseLifecycle: the removal tombstone", () => {
  it("is all removal leaves, and the sweep deletes it gcMinAge later (D91)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const objects = yield* ObjectStore;
        const keys = Effect.map(objects.list(databasePrefix(SOURCE), Option.none()), (page) =>
          page.objects.map((object) => object.key),
        );
        const sweep = Effect.gen(function* () {
          yield* (yield* MaintenanceSweep).sweepPage(50);
        }).pipe(Effect.provide(maintenanceSweepLayerWithoutDependencies));
        yield* lifecycle.create(SOURCE, {
          keyRange: Option.none(),
          databaseOptions: DEFAULT_DATABASE_OPTIONS,
        });
        yield* client.put(SOURCE, bytes("a"), bytes("A"));
        yield* client.flush(SOURCE);
        yield* lifecycle.remove(SOURCE);
        const afterRemove = tombstoneShape(yield* keys);
        yield* sweep;
        const sweptEarly = tombstoneShape(yield* keys);
        yield* TestClock.adjust(HOUR + 1);
        yield* sweep;
        return { afterRemove, sweptEarly, sweptLate: yield* keys };
      }),
    );
    expect(outcome).toEqual({ afterRemove: true, sweptEarly: true, sweptLate: [] });
  });
});

describe("DatabaseLifecycle: what an old incarnation may leave behind", () => {
  it("a stale GC manifest boundary and LATEST hint do not hide or refuse the new chain (D90)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const objects = yield* ObjectStore;
        const gc = yield* GarbageCollector;
        const reader = yield* ManifestReader;
        const options = { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS };
        yield* lifecycle.create(SOURCE, options);
        for (const round of [1, 2, 3, 4, 5, 6]) {
          yield* client.put(SOURCE, bytes("a"), bytes(`old-${round}`));
          yield* client.flush(SOURCE);
        }
        // Old manifests collected: GC's manifest boundary is above 1.
        yield* TestClock.adjust(HOUR + 1);
        yield* gc.collect(SOURCE, Option.none());
        const boundary = (yield* reader.loadManifestBoundary(SOURCE)).id;
        const leftovers = [manifestGcBoundaryKey(SOURCE), latestManifestKey(SOURCE)];
        const saved: Array<readonly [string, Uint8Array]> = [];
        for (const key of leftovers) {
          const body = yield* objects.get(key);
          if (Option.isSome(body)) saved.push([key, body.value]);
        }
        yield* lifecycle.remove(SOURCE);
        // A collection or a LATEST hint of the old incarnation, in flight during the removal,
        // lands after its deletes.
        for (const [key, body] of saved) yield* objects.put(key, body);
        yield* TestClock.adjust(HOUR + 1);
        const created = yield* Effect.result(lifecycle.create(SOURCE, options));
        if (created._tag === "Failure") {
          return {
            boundaryAboveOne: boundary > 1,
            savedKeys: saved.length,
            created: created.failure._tag,
            read: "not run",
          };
        }
        const read = yield* Effect.result(
          Effect.gen(function* () {
            yield* client.put(SOURCE, bytes("a"), bytes("new"));
            yield* client.flush(SOURCE);
            return Option.map(
              yield* client.get(SOURCE, bytes("a"), {
                consistency: { _tag: "Flushed" },
                snapshot: Option.none(),
                refuseSealed: false,
              }),
              text,
            );
          }),
        );
        return {
          boundaryAboveOne: boundary > 1,
          savedKeys: saved.length,
          created: "created",
          read: read._tag === "Success" ? Option.getOrNull(read.success) : read.failure._tag,
        };
      }),
    );
    expect(outcome).toEqual({
      boundaryAboveOne: true,
      savedKeys: 2,
      created: "created",
      read: "new",
    });
  });
});

describe("DatabaseLifecycle: clearing a removed incarnation", () => {
  it("keeps a live database's objects and a concurrent create's young objects (D90)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const objects = yield* ObjectStore;
        const options = { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS };
        const marker = `${databasePrefix(SOURCE)}removed`;
        const young = `${databasePrefix(SOURCE)}compacted/young.sst`;
        yield* lifecycle.create(SOURCE, options);
        yield* lifecycle.remove(SOURCE);
        yield* TestClock.adjust(HOUR);
        // A concurrent create of the same id has just written an object: it is young, so it stays.
        yield* objects.put(young, Uint8Array.of(1));
        yield* lifecycle.create(SOURCE, options);
        const youngKept = Option.isSome(yield* objects.head(young));
        yield* client.put(SOURCE, bytes("a"), bytes("A"));
        yield* client.flush(SOURCE);
        // A create that saw a marker while another create of the id won (the marker written again
        // here stands for that view): the live database's objects, old by now, stay.
        yield* objects.put(marker, Uint8Array.of(1));
        yield* TestClock.adjust(HOUR);
        const again = yield* Effect.flip(lifecycle.create(SOURCE, options));
        const read = yield* client.get(SOURCE, bytes("a"), {
          consistency: { _tag: "Flushed" },
          snapshot: Option.none(),
          refuseSealed: false,
        });
        return { youngKept, again: again._tag, read: Option.getOrNull(Option.map(read, text)) };
      }),
    );
    expect(outcome).toEqual({ youngKept: true, again: "DatabaseAlreadyExistsError", read: "A" });
  });
});

describe("DatabaseLifecycle: what lands after removal", () => {
  it("an SST upload and a LATEST hint written after the deletes go with the tombstone (D91)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const objects = yield* ObjectStore;
        yield* lifecycle.create(SOURCE, {
          keyRange: Option.none(),
          databaseOptions: DEFAULT_DATABASE_OPTIONS,
        });
        yield* client.put(SOURCE, bytes("a"), bytes("A"));
        yield* client.flush(SOURCE);
        yield* lifecycle.remove(SOURCE);
        // A compaction step in flight uploads its output; a commit's hint write lands late.
        yield* objects.put(`${compactedPrefix(SOURCE)}late.sst`, Uint8Array.of(1));
        yield* objects.put(latestManifestKey(SOURCE), new TextEncoder().encode("99"));
        const removedRead = yield* Effect.flip(
          client.get(SOURCE, bytes("a"), {
            consistency: { _tag: "Flushed" },
            snapshot: Option.none(),
            refuseSealed: false,
          }),
        );
        yield* TestClock.adjust(2 * HOUR);
        yield* Effect.gen(function* () {
          yield* (yield* MaintenanceSweep).sweepPage(50);
        }).pipe(Effect.provide(maintenanceSweepLayerWithoutDependencies));
        const page = yield* objects.list(databasePrefix(SOURCE), Option.none());
        return { removedRead: removedRead._tag, left: page.objects.map((object) => object.key) };
      }),
    );
    expect(outcome).toEqual({ removedRead: "DatabaseNotFoundError", left: [] });
  });
});

describe("DatabaseLifecycle: a manifest CAS in flight during removal", () => {
  const firstL0 = (manifest: Manifest) => {
    const sst = manifest.l0[0];
    if (sst === undefined) throw new Error("the database was flushed");
    return sst;
  };
  const checkpointAt = (id: string, manifestId: ManifestId) => ({
    id: CheckpointId.make(id),
    manifestId,
    createdAt: Timestamp.make(0),
    expireAt: Option.none(),
    holder: Option.none(),
    name: Option.none(),
  });
  // Each committer's edit, applied to the manifest it loaded before the removal (`view`).
  // A committer carries the epoch it fenced in with (the view's), not the one it reloads.
  const committers = (
    view: Manifest,
  ): ReadonlyArray<
    readonly [
      string,
      (
        manifest: Manifest,
        manifestId: ManifestId,
      ) => Result.Result<Manifest, { readonly _tag: string }>,
    ]
  > => [
    [
      "flush",
      (manifest) =>
        commitL0Flush(manifest, {
          writerEpoch: view.writerEpoch,
          sst: { ...firstL0(view), id: SstId.make("01JD00000000000000000000F1") },
          lastSeq: SeqNum.make(manifest.lastL0Seq + 1),
          clockTick: manifest.lastL0ClockTick,
          changelog: Option.none(),
        }),
    ],
    [
      "compaction",
      (manifest) =>
        commitCompaction(manifest, {
          compactorEpoch: view.compactorEpoch,
          job: {
            sources: view.l0.map((sst) => ({ _tag: "L0" as const, sstId: sst.id })),
            destination: SortedRunId.make(0),
            isBottom: true,
          },
          outputs: [{ ...firstL0(view), id: SstId.make("01JD00000000000000000000C1") }],
          jobId: "job-after-removal",
        }),
    ],
    [
      "collection",
      (manifest) =>
        Result.succeed(
          prepareGarbageCollection(manifest, {
            now: Timestamp.make(10),
            cutoff: Timestamp.make(5),
            advanceBoundary: true,
          }),
        ),
    ],
    [
      "checkpoint",
      (manifest, id) => addCheckpoint(manifest, checkpointAt("01JD0000000000000000000CK1", id)),
    ],
    ["seal", (manifest) => Result.succeed(sealManifest(manifest, [LEFT, RIGHT]))],
    [
      "fork hold",
      (manifest, id) =>
        holdForSuccessors(manifest, {
          pinnedManifestId: id,
          proposed: [
            { successor: LEFT, checkpointId: CheckpointId.make("01JD0000000000000000000HD1") },
          ],
          createdAt: Timestamp.make(0),
        }),
    ],
  ];
  const COMMITTERS = ["flush", "compaction", "collection", "checkpoint", "seal", "fork hold"];
  for (const committer of COMMITTERS) {
    it(`a ${committer} that lands after the removal's deletes does not bring the database back`, async () => {
      const outcome = await run(
        Effect.gen(function* () {
          const lifecycle = yield* DatabaseLifecycle;
          const client = yield* DbClient;
          const store = yield* ManifestStore;
          const reader = yield* ManifestReader;
          const objects = yield* ObjectStore;
          yield* lifecycle.create(SOURCE, {
            keyRange: Option.none(),
            databaseOptions: DEFAULT_DATABASE_OPTIONS,
          });
          yield* client.put(SOURCE, bytes("a"), bytes("A"));
          yield* client.flush(SOURCE);
          // The committer loaded the newest manifest, then the whole removal ran.
          const view = yield* reader.loadLatest(SOURCE);
          const edit = committers(view.manifest).find(([name]) => name === committer)?.[1];
          if (edit === undefined) return yield* Effect.die(`no committer ${committer}`);
          yield* lifecycle.remove(SOURCE);
          const headId = (yield* reader.loadLatest(SOURCE)).id;
          const landed = yield* Effect.result(store.updateFrom(view, edit));
          // Manifest objects, not the LATEST hint.
          const manifestsLeft = (yield* objects.list(
            manifestPrefix(SOURCE),
            Option.none(),
          )).objects.filter((object) => object.key !== latestManifestKey(SOURCE)).length;
          const seen = yield* Effect.result(reader.loadLatest(SOURCE));
          const write = yield* Effect.result(client.put(SOURCE, bytes("b"), bytes("B")));
          return {
            // Refused (the edit's error), or a no-op on the tombstone: nothing above the head.
            committedAboveHead: landed._tag === "Success" && landed.success.id > headId,
            manifestsLeft,
            seen: seen._tag === "Success" ? seen.success.manifest.state._tag : seen.failure._tag,
            write: write._tag === "Success" ? "acknowledged" : write.failure._tag,
          };
        }),
      );
      // Exactly one manifest object is left, the head: the Removed tombstone (D91).
      expect(outcome).toEqual({
        committedAboveHead: false,
        manifestsLeft: 1,
        seen: "Removed",
        write: "DatabaseNotFoundError",
      });
    });
  }
});

describe("DatabaseLifecycle: a removal that stopped after marking", () => {
  it("leaves nothing of the old incarnation to a create gcMinAge later, and sequence numbers start again (D91)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const store = yield* ManifestStore;
        const options = { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS };
        const latestRead = (key: string) =>
          Effect.map(
            client.get(SOURCE, bytes(key), {
              consistency: { _tag: "Latest" },
              snapshot: Option.none(),
              refuseSealed: false,
            }),
            (value) => Option.getOrNull(Option.map(value, text)),
          );
        yield* lifecycle.create(SOURCE, options);
        yield* client.put(SOURCE, bytes("a"), bytes("flushed"));
        yield* client.flush(SOURCE);
        // Unflushed rows in the writer's SQLite, at sequence numbers past the flushed ones.
        for (const round of [1, 2, 3]) yield* client.put(SOURCE, bytes(`u${round}`), bytes("old"));
        // The removal marks the manifest, then stops: the writer's state is never destroyed.
        const removedAt = Timestamp.make(yield* Clock.currentTimeMillis);
        yield* store.update(SOURCE, (manifest) => markRemoved(manifest, removedAt));
        const tooSoon = (yield* Effect.flip(lifecycle.create(SOURCE, options)))._tag;
        yield* TestClock.adjust(HOUR + 1_000);
        // The sweep cannot reach the writer, so it leaves an uncleared tombstone to create.
        yield* Effect.gen(function* () {
          yield* (yield* MaintenanceSweep).sweepPage(50);
        }).pipe(Effect.provide(maintenanceSweepLayerWithoutDependencies));
        yield* lifecycle.create(SOURCE, options);
        const receipt = yield* client.put(SOURCE, bytes("b"), bytes("new"));
        yield* client.flush(SOURCE);
        return {
          tooSoon,
          firstSeq: receipt.seq,
          a: yield* latestRead("a"),
          u1: yield* latestRead("u1"),
          b: yield* latestRead("b"),
        };
      }),
    );
    expect(outcome).toEqual({
      tooSoon: "DatabaseRecentlyRemovedError",
      firstSeq: 1,
      a: null,
      u1: null,
      b: "new",
    });
  });
});

describe("DatabaseLifecycle: a removal still deleting when the id is created again", () => {
  it("deletes none of the new database's objects (D91)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const faults = yield* StoreFaults;
        const context = yield* Effect.context<TestServices>();
        const options = { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS };
        yield* lifecycle.create(SOURCE, options);
        yield* client.put(SOURCE, bytes("a"), bytes("old"));
        yield* client.flush(SOURCE);
        // The removal's first listing is slow: gcMinAge passes, and a create of the id and its
        // first flush land before the listing returns.
        yield* Ref.update(faults, (rule) => ({
          ...rule,
          beforeList: Option.some((prefix: string) =>
            prefix === databasePrefix(SOURCE)
              ? Effect.gen(function* () {
                  yield* TestClock.adjust(HOUR + 1);
                  yield* lifecycle.create(SOURCE, options);
                  yield* client.put(SOURCE, bytes("b"), bytes("new"));
                  yield* client.flush(SOURCE);
                }).pipe(Effect.provideContext(context), Effect.scoped, Effect.orDie)
              : Effect.void,
          ),
        }));
        yield* lifecycle.remove(SOURCE);
        return Option.getOrNull(
          Option.map(
            yield* client.get(SOURCE, bytes("b"), {
              consistency: { _tag: "Flushed" },
              snapshot: Option.none(),
              refuseSealed: false,
            }),
            text,
          ),
        );
      }),
    );
    expect(outcome).toBe("new");
  });
});

describe("DatabaseLifecycle: a fork's hold", () => {
  it("cannot be deleted as a checkpoint while the fork holds it (D91)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const client = yield* DbClient;
        const reader = yield* ManifestReader;
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
        const hold = (yield* reader.loadLatest(SOURCE)).manifest.checkpoints.find((cp) =>
          Option.contains(cp.holder, LEFT),
        );
        if (hold === undefined) return yield* Effect.die("the fork holds no checkpoint");
        const deleted = yield* Effect.result(lifecycle.deleteCheckpoint(SOURCE, hold.id));
        const holdLeft = (yield* reader.loadLatest(SOURCE)).manifest.checkpoints.some(
          (cp) => cp.id === hold.id,
        );
        // The caller's own checkpoint is still deletable.
        yield* lifecycle.deleteCheckpoint(SOURCE, checkpoint.id);
        return {
          deleted: deleted._tag === "Success" ? "deleted" : deleted.failure._tag,
          holdLeft,
        };
      }),
    );
    expect(outcome).toEqual({ deleted: "DatabaseHasDependentsError", holdLeft: true });
  });

  it("is released when the fork's id belongs to another database (D37)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const lifecycle = yield* DatabaseLifecycle;
        const reader = yield* ManifestReader;
        const options = { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS };
        yield* lifecycle.create(SOURCE, options);
        yield* lifecycle.create(LEFT, options);
        const checkpoint = yield* lifecycle.createCheckpoint(SOURCE, {
          name: Option.none(),
          lifetimeMs: Option.none(),
        });
        const fork = yield* Effect.flip(
          lifecycle.forkRange({
            source: SOURCE,
            checkpointId: checkpoint.id,
            range: LEFT_RANGE,
            forkId: LEFT,
          }),
        );
        const checkpoints = (yield* reader.loadLatest(SOURCE)).manifest.checkpoints.map(
          (cp) => cp.id,
        );
        return { fork: fork._tag, checkpoints };
      }),
    );
    expect(outcome.fork).toBe("DatabaseAlreadyExistsError");
    expect(outcome.checkpoints).toHaveLength(1);
  });
});
