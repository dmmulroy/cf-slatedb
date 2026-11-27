import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  type KeyRange,
} from "@shaledb/lsm-core";
import { Effect, Layer, Option, Stream } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { backgroundWorkTestLayer } from "./background-work.ts";
import { blockSourceTestLayer } from "./block-source.ts";
import { compactionBudgetTestLayer } from "./compaction-budget.ts";
import { CompactionExecutor, makeCompactionExecutor } from "./compaction-executor.ts";
import { compactionLauncherTestLayer } from "./compaction-launcher.ts";
import { compactorClientTestLayer } from "./compactor-client.ts";
import {
  DatabaseLifecycle,
  databaseLifecycleLayerWithoutDependencies,
} from "./database-lifecycle.ts";
import { DbClient, type DbSnapshot, dbClientTestLayer } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestStore } from "./manifest-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";

/**
 * A snapshot fixes which writes a read sees, not the time its TTLs are judged at
 * (`docs/invariants.md`, "What each read may see"). Every read resolves expiry with the reading
 * isolate's clock when it runs (`DbReader.getRow`, `DbReader.scan`), so two reads of one fixed
 * point a few seconds apart return a value and then nothing, with no write between them.
 */
const DB = DatabaseId.make("orders");
const FORK = DatabaseId.make("orders-fork");
const bytes = (text: string) => new TextEncoder().encode(text);
const text = (value: Uint8Array) => new TextDecoder().decode(value);
const FULL_RANGE: KeyRange = { start: new Uint8Array(0), end: Option.none() };
/** The TTL of `a` and `b`, written at time 0: they expire at 10 s. */
const TTL = 10_000;

const testLayer = Layer.mergeAll(
  databaseLifecycleLayerWithoutDependencies.pipe(
    Layer.provide(blockSourceTestLayer),
    Layer.provide(backgroundWorkTestLayer),
  ),
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

/** At time 0: `a` and `b` with a 10 s TTL, `c` with none; flushed. */
const seed = (databaseId: DatabaseId) =>
  Effect.gen(function* () {
    yield* (yield* ManifestStore).initialize(
      initialManifest(databaseId, {
        keyRange: Option.none(),
        databaseOptions: DEFAULT_DATABASE_OPTIONS,
      }),
    );
    const client = yield* DbClient;
    yield* client.put(databaseId, bytes("a"), bytes("1"), { ttlMillis: Option.some(TTL) });
    yield* client.put(databaseId, bytes("b"), bytes("2"), { ttlMillis: Option.some(TTL) });
    yield* client.put(databaseId, bytes("c"), bytes("3"));
    yield* client.flush(databaseId);
  });

const getAt = (databaseId: DatabaseId, snapshot: Option.Option<DbSnapshot>, key: string) =>
  Effect.flatMap(DbClient, (client) =>
    client.get(databaseId, bytes(key), {
      consistency: { _tag: "Flushed" },
      snapshot,
      refuseSealed: false,
    }),
  ).pipe(Effect.map(Option.map(text)));

/** `a` read at a fixed point at 5 s, at 10 s (its expiry), and again at 5 s (a clock behind). */
const aroundExpiry = (read: Effect.Effect<Option.Option<string>, unknown, TestServices>) =>
  Effect.gen(function* () {
    yield* TestClock.setTime(5_000);
    const before = yield* read;
    yield* TestClock.setTime(TTL);
    const after = yield* read;
    yield* TestClock.setTime(5_000);
    const behind = yield* read;
    return { before, after, behind };
  });

const EXPIRES: {
  readonly before: Option.Option<string>;
  readonly after: Option.Option<string>;
  readonly behind: Option.Option<string>;
} = { before: Option.some("1"), after: Option.none(), behind: Option.some("1") };

describe("a snapshot fixes write visibility, not TTL time", () => {
  it("an unpinned snapshot reads a value, then none once it expires, then the value again on a clock behind", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* seed(DB);
        const snapshot = yield* (yield* DbClient).snapshot(DB);
        return yield* aroundExpiry(getAt(DB, Option.some(snapshot), "a"));
      }),
    );
    expect(outcome).toEqual(EXPIRES);
  });

  it("a pinned snapshot does the same: its manifest's bytes stay, the reader's clock moves", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* seed(DB);
        const pinned = yield* (yield* DatabaseLifecycle).pinSnapshot(DB, 3_600_000);
        return yield* aroundExpiry(getAt(DB, Option.some(pinned.snapshot), "a"));
      }),
    );
    expect(outcome).toEqual(EXPIRES);
  });

  it("a transaction's reads at readSeq see the value expire, and its commit, which checks writes only, succeeds", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* seed(DB);
        const client = yield* DbClient;
        const begin = yield* client.beginTransaction(DB);
        const read = client
          .getAt(DB, bytes("a"), begin.readSeq, begin.manifestId)
          .pipe(
            Effect.map(
              Option.map((row) => (row.value._tag === "Value" ? text(row.value.bytes) : "?")),
            ),
          );
        const reads = yield* aroundExpiry(read);
        yield* TestClock.setTime(TTL);
        // The transaction read `a`, which expired since; an expiry is not a write, so the
        // serializable commit's conflict check finds nothing and the commit succeeds.
        const receipt = yield* client.commitTransaction(DB, {
          readSeq: begin.readSeq,
          isolation: "SerializableSnapshot",
          batch: {
            operations: [
              { _tag: "Put", key: bytes("c"), value: bytes("4"), ttlMillis: Option.none() },
            ],
          },
          readKeys: [bytes("a")],
          readRanges: [],
        });
        return { reads, committed: receipt.seq > begin.readSeq };
      }),
    );
    expect(outcome).toEqual({ reads: EXPIRES, committed: true });
  });

  it("a fork read at its checkpoint does the same", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* seed(DB);
        const lifecycle = yield* DatabaseLifecycle;
        const checkpoint = yield* lifecycle.createCheckpoint(DB, {
          name: Option.none(),
          lifetimeMs: Option.none(),
        });
        yield* lifecycle.forkRange({
          source: DB,
          checkpointId: checkpoint.id,
          range: FULL_RANGE,
          forkId: FORK,
        });
        return yield* aroundExpiry(getAt(FORK, Option.none(), "a"));
      }),
    );
    expect(outcome).toEqual(EXPIRES);
  });

  it("two pages of one scan at one snapshot judge expiry each at its own time", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* seed(DB);
        const client = yield* DbClient;
        const snapshot = Option.some(yield* client.snapshot(DB));
        const page = (start: Uint8Array) =>
          Stream.runCollect(
            client.scan(
              DB,
              { start, end: Option.none() },
              {
                consistency: { _tag: "Flushed" },
                snapshot,
                refuseSealed: false,
                limit: Option.some(1),
              },
            ),
          ).pipe(Effect.map((rows) => Array.from(rows, (row) => text(row.key))));
        yield* TestClock.setTime(5_000);
        const first = yield* page(new Uint8Array(0));
        yield* TestClock.setTime(TTL);
        // The next page starts after the first page's key, as `resumeFrom` would have it.
        const second = yield* page(bytes("a\u0000"));
        return { first, second };
      }),
    );
    // At 5 s the first page holds `a`. At 10 s `b` has expired, so the second page skips to `c`:
    // the scan as a whole returned `a` and `c`, which no single moment held.
    expect(outcome).toEqual({ first: ["a"], second: ["c"] });
  });
});
