import {
  type CompactionJob,
  type CompactorEpoch,
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  FULL_KEY_RANGE,
  initialManifest,
  planCompactions,
  type Row,
  type SstHandle,
} from "@shaledb/lsm-core";
import {
  Arbitrary,
  Bytes,
  Effect,
  Layer,
  Option,
  Result,
  Schema,
  type Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { CompactionExecutor, makeCompactionExecutor } from "./compaction-executor.ts";
import { DbClient, dbClientTestLayer, makeDbClient } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";
import { WriterClient } from "./writer-client.ts";

/**
 * A `Latest` scan pulls the writer's unflushed rows a page at a time, as its merge needs them, and
 * stops when it has its rows (D93).
 */

const DB = DatabaseId.make("orders");
const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));
const key = (index: number) => Uint8Array.of(0x6b, index >> 8, index & 0xff);
const latest = (limit: Option.Option<number>) => ({
  consistency: { _tag: "Latest" as const },
  snapshot: Option.none(),
  refuseSealed: false,
  limit,
});

const testLayer = Layer.effect(CompactionExecutor, makeCompactionExecutor).pipe(
  Layer.provide(sstStoreLayerWithoutDependencies),
  Layer.provideMerge(dbClientTestLayer({ blockSize: 4096 })),
);

type TestServices = Layer.Success<typeof testLayer> | DbReaderServices | Scope.Scope;

const provide = <A, E>(program: Effect.Effect<A, E, TestServices>) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(testLayer),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
  );

const run = <A, E>(program: Effect.Effect<A, E, TestServices>) =>
  Effect.runPromise(provide(program));

const createDatabase = Effect.flatMap(ManifestStore, (store) =>
  store.initialize(
    initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
  ),
);

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
  const runJob = (job: CompactionJob, epoch: CompactorEpoch) =>
    Effect.gen(function* () {
      const executor = yield* CompactionExecutor;
      const prepared = yield* executor.prepare(DB, epoch, job);
      const outputs: Array<SstHandle> = [];
      let start = Option.none<Uint8Array>();
      do {
        const step = yield* executor.writeOutput(prepared, start);
        outputs.push(...Option.toArray(step.output));
        start = step.resumeFrom;
      } while (Option.isSome(start));
      return yield* executor.commit(prepared, outputs);
    });
  yield* runJob(job, manifest.compactorEpoch);
});

/**
 * A client whose writer calls run `after[n]` once the n-th scan page (from 0) has answered: a
 * write, flush, or compaction landing between two pages of one scan. It also counts the pages and
 * the rows they carried.
 */
const hookedClient = (
  after: ReadonlyArray<
    Effect.Effect<void, never, DbReaderServices | CompactionExecutor | ManifestReader>
  >,
) =>
  Effect.gen(function* () {
    const writer = yield* WriterClient;
    const services = yield* Effect.context<
      DbReaderServices | CompactionExecutor | ManifestReader
    >();
    const seen = { pages: 0, rows: 0 };
    const client = yield* makeDbClient.pipe(
      Effect.provideService(
        WriterClient,
        WriterClient.of({
          ...writer,
          scan: (databaseId, request) =>
            Effect.tap(writer.scan(databaseId, request), (page) =>
              Effect.suspend(() => {
                const hook = after[seen.pages] ?? Effect.void;
                seen.pages += 1;
                seen.rows += page.rows.length;
                return Effect.provide(hook, services);
              }),
            ),
        }),
      ),
    );
    return { client, seen };
  });

const writeKeys = (count: number, valueByte: number, valueBytes = 1) =>
  Effect.flatMap(DbClient, (client) =>
    client.write(DB, {
      operations: Array.from({ length: count }, (_, index) => ({
        _tag: "Put" as const,
        key: key(index),
        value: new Uint8Array(valueBytes).fill(valueByte),
        ttlMillis: Option.none(),
      })),
    }),
  );

describe("a Latest scan over unflushed rows (D93)", () => {
  it("a scan for one row reads one writer page, not every unflushed row", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        // 5,000 unflushed rows: five pages of the writer's 1,000.
        for (let batch = 0; batch < 5; batch++) {
          yield* (yield* DbClient).write(DB, {
            operations: Array.from({ length: 1000 }, (_, index) => ({
              _tag: "Put" as const,
              key: Uint8Array.of(0x6b, batch, index >> 8, index & 0xff),
              value: Uint8Array.of(1),
              ttlMillis: Option.none(),
            })),
          });
        }
        const { client, seen } = yield* hookedClient([]);
        const one = yield* Stream.runCollect(
          client.scan(DB, FULL_KEY_RANGE, latest(Option.some(1))),
        );
        const onePages = { ...seen };
        seen.pages = 0;
        seen.rows = 0;
        const all = yield* Stream.runCollect(
          client.scan(DB, FULL_KEY_RANGE, latest(Option.none())),
        );
        return { one: one.length, onePages, all: all.length, allPages: { ...seen } };
      }),
    );
    expect(outcome.one).toBe(1);
    // One page of at most 1,000 rows held, whatever the writer holds.
    expect(outcome.onePages).toEqual({ pages: 1, rows: 1000 });
    expect(outcome.all).toBe(5000);
    // A full scan reads each row about once: a segment ends before the last key it read, which
    // the next page reads again.
    expect(outcome.allPages.pages).toBeLessThanOrEqual(6);
    expect(outcome.allPages.rows).toBeLessThanOrEqual(5000 + outcome.allPages.pages);
  });

  it("a scan overtaken by a flush after it returned rows fails with the key to resume from; the resumed scan skips and repeats nothing", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        yield* writeKeys(2500, 1);
        const db = yield* DbClient;
        // After the second page: key 2000 is rewritten and everything flushed, past the snapshot.
        const { client } = yield* hookedClient([
          Effect.void,
          Effect.gen(function* () {
            yield* db.put(DB, key(2000), Uint8Array.of(2));
            yield* db.flush(DB);
          }).pipe(Effect.orDie),
        ]);
        const returned: Array<Row> = [];
        const first = yield* Effect.result(
          Stream.runForEach(client.scan(DB, FULL_KEY_RANGE, latest(Option.none())), (row) =>
            Effect.sync(() => void returned.push(row)),
          ),
        );
        const resumeFrom = Result.isFailure(first)
          ? first.failure._tag === "ReaderUnavailableError"
            ? first.failure.resumeFrom
            : undefined
          : undefined;
        if (resumeFrom === undefined) return { failed: Result.isFailure(first), resumed: false };
        const rest = yield* Stream.runCollect(
          db.scan(DB, { start: resumeFrom, end: Option.none() }, latest(Option.none())),
        );
        const keys = [...returned, ...rest].map(
          (row) => (row.key[1] ?? 0) * 256 + (row.key[2] ?? 0),
        );
        const rewritten = [...returned, ...rest].find((row) =>
          Bytes.Equivalence(row.key, key(2000)),
        );
        return {
          failed: true,
          resumed: true,
          returnedBefore: returned.length > 0,
          everyKeyOnce: keys.length === 2500 && keys.every((value, index) => value === index),
          resumeIsNextKey: Bytes.Equivalence(resumeFrom, key(returned.length)),
          rewritten: rewritten?.value,
        };
      }),
    );
    expect(outcome).toMatchObject({
      failed: true,
      resumed: true,
      everyKeyOnce: true,
      resumeIsNextKey: true,
      // The resumed scan reads at its own, newer snapshot.
      rewritten: { _tag: "Value", bytes: Uint8Array.of(2) },
      returnedBefore: true,
    });
  });

  it("a scan pulls pages bounded by bytes: large values make more, smaller pages (D93)", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        // 40 rows of 512 KiB (20 MiB) in five batches: about 16 rows per 8 MiB page.
        for (let batch = 0; batch < 5; batch++) {
          yield* (yield* DbClient).write(DB, {
            operations: Array.from({ length: 8 }, (_, index) => ({
              _tag: "Put" as const,
              key: key(batch * 8 + index),
              value: new Uint8Array(512 * 1024),
              ttlMillis: Option.none(),
            })),
          });
        }
        const { client, seen } = yield* hookedClient([]);
        const one = yield* Stream.runCollect(
          client.scan(DB, FULL_KEY_RANGE, latest(Option.some(1))),
        );
        return { one: one.length, rows: seen.rows, pages: seen.pages };
      }),
    );
    expect(outcome.one).toBe(1);
    expect(outcome.pages).toBe(1);
    expect(outcome.rows).toBeLessThanOrEqual(16);
  });
});

type Step =
  | { readonly _tag: "Put"; readonly key: number; readonly value: number }
  | { readonly _tag: "Delete"; readonly key: number }
  | { readonly _tag: "Flush" }
  | { readonly _tag: "Compact" };

const stepArbitrary: Arbitrary.Arbitrary<Step> = Arbitrary.all({
  kind: smallInt(0, 9),
  key: smallInt(0, 59),
  value: smallInt(1, 250),
}).pipe(
  Arbitrary.map(({ kind, key, value }): Step => {
    if (kind <= 4) return { _tag: "Put", key, value };
    if (kind <= 6) return { _tag: "Delete", key };
    if (kind <= 8) return { _tag: "Flush" };
    return { _tag: "Compact" };
  }),
);

/** 128 KiB values: an 8 MiB writer page holds about 60 rows, so a scan of 60 keys spans pages. */
const VALUE_BYTES = 128 * 1024;

describe("a Latest scan against the oracle (D93)", () => {
  it("returns the database at one snapshot, or a prefix and the key to resume from, whatever lands between pages", async () => {
    const result = await Effect.runPromise(
      Arbitrary.checkEffect(
        Arbitrary.all({
          before: Arbitrary.array(stepArbitrary, { minLength: 20, maxLength: 60 }),
          between: Arbitrary.array(Arbitrary.array(stepArbitrary, { maxLength: 3 }), {
            maxLength: 4,
          }),
          // 0: no limit.
          limitOrZero: smallInt(0, 70),
        }),
        ({ before, between, limitOrZero }) =>
          provide(
            Effect.gen(function* () {
              yield* createDatabase;
              const db = yield* DbClient;
              const state = new Map<number, number>();
              const apply = (step: Step) =>
                Effect.gen(function* () {
                  if (step._tag === "Put") {
                    yield* db.put(DB, key(step.key), new Uint8Array(VALUE_BYTES).fill(step.value));
                    state.set(step.key, step.value);
                  } else if (step._tag === "Delete") {
                    yield* db.delete(DB, key(step.key));
                    state.delete(step.key);
                  } else if (step._tag === "Flush") {
                    yield* db.flush(DB);
                  } else {
                    yield* compact;
                  }
                }).pipe(Effect.orDie);
              const limit = limitOrZero === 0 ? Option.none<number>() : Option.some(limitOrZero);
              for (const step of before) yield* apply(step);
              // The states a scan may read: at its start, and after each step landing between pages
              // (a scan overtaken before it returned a row starts again at a newer snapshot).
              const states: Array<ReadonlyMap<number, number>> = [new Map(state)];
              const { client } = yield* hookedClient(
                between.map((steps) =>
                  Effect.forEach(
                    steps,
                    (step) =>
                      Effect.andThen(
                        apply(step),
                        Effect.sync(() => void states.push(new Map(state))),
                      ),
                    { discard: true },
                  ),
                ),
              );
              const returned: Array<Row> = [];
              const scanned = yield* Effect.result(
                Stream.runForEach(client.scan(DB, FULL_KEY_RANGE, latest(limit)), (row) =>
                  Effect.sync(() => void returned.push(row)),
                ),
              );
              const keyOf = (row: Row) => (row.key[1] ?? 0) * 256 + (row.key[2] ?? 0);
              const valueOf = (row: Row) => (row.value._tag === "Value" ? row.value.bytes[0] : -1);
              const expected = (
                from: ReadonlyMap<number, number>,
                keys: (key: number) => boolean,
              ) =>
                [...from]
                  .filter(([k]) => keys(k))
                  .sort(([a], [b]) => a - b)
                  .map(([k, v]) => `${k}=${v}`);
              const got = returned.map((row) => `${keyOf(row)}=${valueOf(row)}`);
              if (Result.isSuccess(scanned)) {
                // One snapshot: the database as it was at one moment, up to the limit.
                return states.some((at) => {
                  const all = expected(at, () => true);
                  return (
                    got.join() ===
                    all
                      .slice(
                        0,
                        Option.getOrElse(limit, () => all.length),
                      )
                      .join()
                  );
                });
              }
              const failure = scanned.failure;
              if (failure._tag !== "ReaderUnavailableError" || failure.resumeFrom === undefined) {
                // Overtaken on every attempt before returning a row: retryable, and nothing returned.
                return failure._tag === "ReaderUnavailableError" && returned.length === 0;
              }
              const resume = (failure.resumeFrom[1] ?? 0) * 256 + (failure.resumeFrom[2] ?? 0);
              // A prefix at the scan's snapshot, cut exactly before the resume key; the resumed
              // scan then reads the rest at the database's newest state.
              const rest = yield* Stream.runCollect(
                db.scan(
                  DB,
                  { start: failure.resumeFrom, end: Option.none() },
                  latest(Option.none()),
                ),
              );
              return (
                returned.length > 0 &&
                states.some((at) => got.join() === expected(at, (k) => k < resume).join()) &&
                rest.map((row) => `${keyOf(row)}=${valueOf(row)}`).join() ===
                  expected(state, (k) => k >= resume).join()
              );
            }).pipe(Effect.orDie),
          ),
        { runs: 25 },
      ),
    );
    const failure = Arbitrary.formatCheckFailure(result);
    if (failure !== undefined) throw new Error(failure);
  });
});
