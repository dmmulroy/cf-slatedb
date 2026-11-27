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
import { Effect, Exit, Layer, Option, type Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { CompactionExecutor, makeCompactionExecutor } from "./compaction-executor.ts";
import { DbClient, dbClientTestLayer, makeDbClient } from "./db-client.ts";
import { type DbReaderServices, ScanPageBudget } from "./db-reader.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestStore } from "./manifest-store.ts";
import { WriterClient } from "./writer-client.ts";

/**
 * Every scan surface does a bounded amount of work per page (D125): "limit 1 over a long range of
 * deleted keys", the review's test, on the regular and the transaction scan; the sharded scan's
 * is in `packages/lsm-sharding`.
 */
const DB = DatabaseId.make("orders");
const key = (index: number) => Uint8Array.of(0x6b, index >> 8, index & 0xff);

const testLayer = Layer.effect(CompactionExecutor, makeCompactionExecutor).pipe(
  Layer.provide(sstStoreLayerWithoutDependencies),
  Layer.provideMerge(dbClientTestLayer({ blockSize: 4096 })),
);
type TestServices = Layer.Success<typeof testLayer> | DbReaderServices | Scope.Scope;
const run = <A, E>(program: Effect.Effect<A, E, TestServices>) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(testLayer),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
    Effect.runPromise,
  );

const createDatabase = Effect.flatMap(ManifestStore, (store) =>
  store.initialize(
    initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
  ),
);

/** A client whose writer counts the scan pages it answers and the rows they carried. */
const countingClient = Effect.gen(function* () {
  const writer = yield* WriterClient;
  const seen = { pages: 0, rows: 0 };
  const client = yield* makeDbClient.pipe(
    Effect.provideService(
      WriterClient,
      WriterClient.of({
        ...writer,
        scan: (databaseId, request) =>
          Effect.tap(writer.scan(databaseId, request), (page) =>
            Effect.sync(() => {
              seen.pages += 1;
              seen.rows += page.rows.length;
            }),
          ),
      }),
    ),
  );
  return { client, seen };
});

/** `count` keys, one live first and the rest after it, all unflushed (5 writer pages for 5,000). */
const liveThenMany = (count: number) =>
  Effect.gen(function* () {
    const db = yield* DbClient;
    for (let start = 0; start < count; start += 1000) {
      yield* db.write(DB, {
        operations: Array.from({ length: Math.min(1000, count - start) }, (_, offset) => ({
          _tag: "Put" as const,
          key: key(start + offset),
          value: Uint8Array.of(1),
          ttlMillis: Option.none(),
        })),
      });
    }
  });

describe("the transaction scan's work per page (D125)", () => {
  it("a transaction scan for one row reads one writer page, as a Latest scan does", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        yield* liveThenMany(5000);
        const { client, seen } = yield* countingClient;
        const { readSeq, manifestId } = yield* client.beginTransaction(DB);
        const latest = yield* Stream.runCollect(
          client.scan(DB, FULL_KEY_RANGE, {
            consistency: { _tag: "Latest" },
            snapshot: Option.none(),
            refuseSealed: false,
            limit: Option.some(1),
          }),
        );
        const latestPages = seen.pages;
        seen.pages = 0;
        const rows = yield* client.scanAt(DB, FULL_KEY_RANGE, readSeq, 1, manifestId);
        return { latest: latest.length, latestPages, rows: rows.length, pages: seen.pages };
      }),
    );
    expect(outcome).toEqual({ latest: 1, latestPages: 1, rows: 1, pages: 1 });
  });
});

let jobCounter = 0;
/** Compacts L0 into the sorted run the way the Workflow does. */
const compact = Effect.gen(function* () {
  const { manifest } = yield* (yield* ManifestReader).loadLatest(DB);
  const [job] = planCompactions(manifest, [], {
    minCompactionSources: 1,
    maxCompactionSources: 8,
    includeSizeThreshold: 1_000_000_000,
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
      return yield* executor.commit(prepared, outputs, `bounds-job-${++jobCounter}`);
    });
  yield* runJob(job, manifest.compactorEpoch);
});

const TOMBSTONES = 3000;
/** Every key but the last written, then deleted (or expired): the review's long range of deletes. */
const deletedRange = (how: "deleted" | "expired" | "overwritten") =>
  Effect.gen(function* () {
    const db = yield* DbClient;
    for (let start = 0; start < TOMBSTONES; start += 1000) {
      yield* db.write(DB, {
        operations: Array.from({ length: 1000 }, (_, offset) => ({
          _tag: "Put" as const,
          key: key(start + offset),
          value: new Uint8Array(32).fill(1),
          ttlMillis:
            how === "expired" && start + offset < TOMBSTONES - 1
              ? Option.some(1_000)
              : Option.none(),
        })),
      });
    }
    if (how === "deleted") {
      for (let start = 0; start < TOMBSTONES - 1; start += 1000) {
        yield* db.write(DB, {
          operations: Array.from(
            { length: Math.min(1000, TOMBSTONES - 1 - start) },
            (_, offset) => ({
              _tag: "Delete" as const,
              key: key(start + offset),
            }),
          ),
        });
      }
    }
    if (how === "expired") yield* TestClock.adjust("2 seconds");
  });

/** Where the rows are: in the writer, in L0, or in the sorted run. */
const place = (tier: "writer" | "L0" | "sorted run") =>
  Effect.gen(function* () {
    if (tier === "writer") return;
    yield* (yield* DbClient).flush(DB);
    if (tier === "sorted run") yield* compact;
  });

const BUDGET = { maxBytes: 2048, shared: Option.none() };
const latestOptions = (limit: number) => ({
  consistency: { _tag: "Latest" as const },
  snapshot: Option.none(),
  refuseSealed: false,
  limit: Option.some(limit),
});

/** One regular page of `limit` rows with the 2 KiB budget, as the public handler makes it. */
const regularPage = (client: Effect.Success<typeof countingClient>["client"], limit: number) =>
  Effect.gen(function* () {
    const rows: Array<Row> = [];
    const exit = yield* Effect.exit(
      Stream.runForEach(client.scan(DB, FULL_KEY_RANGE, latestOptions(limit + 1)), (row) =>
        Effect.sync(() => void rows.push(row)),
      ).pipe(Effect.provideService(ScanPageBudget, BUDGET)),
    );
    if (Exit.isFailure(exit)) {
      const failure = Option.getOrUndefined(Exit.findErrorOption(exit));
      if (failure?._tag === "ReaderUnavailableError" && failure.resumeFrom !== undefined) {
        return { rows, resumeFrom: Option.some(failure.resumeFrom) };
      }
      return yield* Effect.failCause(exit.cause);
    }
    const next = rows[limit];
    return {
      rows: rows.slice(0, limit),
      resumeFrom: next === undefined ? Option.none<Uint8Array>() : Option.some(next.key),
    };
  });

/** Follows a regular scan's pages (limit 1) to the end: each page, its rows, and its writer pages. */
const regularPages = Effect.gen(function* () {
  const { client, seen } = yield* countingClient;
  const pages: Array<{ rows: number; writerPages: number }> = [];
  const found: Array<number> = [];
  let start: Uint8Array = new Uint8Array(0);
  for (let page = 0; page < 10_000; page++) {
    seen.pages = 0;
    const outcome = yield* Stream.runCollect(
      client.scan(DB, { start, end: Option.none() }, latestOptions(1)),
    ).pipe(
      Effect.map((rows) => ({ rows, resumeFrom: Option.none<Uint8Array>() })),
      Effect.catchTag("ReaderUnavailableError", (error) =>
        error.pageEnd === "BudgetSpent" && error.resumeFrom !== undefined
          ? Effect.succeed({ rows: [], resumeFrom: Option.some(error.resumeFrom) })
          : Effect.fail(error),
      ),
      Effect.provideService(ScanPageBudget, BUDGET),
    );
    pages.push({ rows: outcome.rows.length, writerPages: seen.pages });
    for (const row of outcome.rows) found.push(((row.key[1] ?? 0) << 8) | (row.key[2] ?? 0));
    if (Option.isSome(outcome.resumeFrom)) start = outcome.resumeFrom.value;
    else if (outcome.rows.length === 1) {
      const last = outcome.rows[0]?.key ?? start;
      start = Uint8Array.of(...last, 0);
    } else break;
    if (found.length > 0) break;
  }
  return { pages, found };
});

/** Follows a transaction's scan pages (limit 1) to the end. */
const transactionPages = Effect.gen(function* () {
  const { client, seen } = yield* countingClient;
  const { readSeq, manifestId } = yield* client.beginTransaction(DB);
  const pages: Array<{ rows: number; writerPages: number }> = [];
  const found: Array<number> = [];
  let start: Uint8Array = new Uint8Array(0);
  for (let page = 0; page < 10_000; page++) {
    seen.pages = 0;
    const answer = yield* client
      .scanAtPage(DB, { start, end: Option.none() }, readSeq, 1, manifestId)
      .pipe(Effect.provideService(ScanPageBudget, BUDGET));
    pages.push({ rows: answer.rows.length, writerPages: seen.pages });
    for (const row of answer.rows) found.push(((row.key[1] ?? 0) << 8) | (row.key[2] ?? 0));
    if (Option.isNone(answer.resumeFrom)) break;
    start = answer.resumeFrom.value;
  }
  return { pages, found };
});

describe("limit 1 over a long range of deleted keys, on every tier (D125)", () => {
  for (const how of ["deleted", "expired"] as const) {
    for (const tier of ["writer", "L0", "sorted run"] as const) {
      it(`${how} keys in the ${tier}: every page is bounded, and following continuations reaches the live key`, async () => {
        const outcome = await run(
          Effect.gen(function* () {
            yield* createDatabase;
            yield* deletedRange(how);
            yield* place(tier);
            return { regular: yield* regularPages, transaction: yield* transactionPages };
          }),
        );
        for (const surface of [outcome.regular, outcome.transaction]) {
          expect(surface.found).toEqual([TOMBSTONES - 1]);
          // More than one page: the budget, not the range, ended the early ones. (A compaction at
          // the bottom drops deletes and expired values, so a sorted run has nothing to skip.)
          if (tier !== "sorted run") expect(surface.pages.length).toBeGreaterThan(1);
          // A page reads at most the writer pages one segment needs.
          for (const page of surface.pages) expect(page.writerPages).toBeLessThanOrEqual(2);
        }
      }, 120_000);
    }
  }
});

describe("one key overwritten many times (D125)", () => {
  it("a page skips a resolved key's older versions: two writer pages, one page of rows", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        const db = yield* DbClient;
        // Key 0 overwritten 2,500 times, unflushed: three writer pages of one key; key 1 after it.
        for (let version = 0; version < 2500; version++) {
          yield* db.write(DB, {
            operations: [
              {
                _tag: "Put",
                key: key(0),
                value: Uint8Array.of(version & 0xff),
                ttlMillis: Option.none(),
              },
            ],
          });
        }
        yield* db.write(DB, {
          operations: [
            { _tag: "Put", key: key(1), value: Uint8Array.of(7), ttlMillis: Option.none() },
          ],
        });
        const { client, seen } = yield* countingClient;
        const regular = yield* Stream.runCollect(client.scan(DB, FULL_KEY_RANGE, latestOptions(2)));
        const regularSeen = { ...seen };
        seen.pages = 0;
        seen.rows = 0;
        const { readSeq, manifestId } = yield* client.beginTransaction(DB);
        const transaction = yield* client.scanAtPage(DB, FULL_KEY_RANGE, readSeq, 2, manifestId);
        return {
          regular: regular.map((row) => (row.value._tag === "Value" ? row.value.bytes[0] : -1)),
          regularSeen,
          transaction: transaction.rows.map((row) =>
            row.value._tag === "Value" ? row.value.bytes[0] : -1,
          ),
          transactionSeen: { ...seen },
        };
      }),
    );
    // The newest version of key 0 (2,499 & 0xff = 195), then key 1.
    expect(outcome.regular).toEqual([195, 7]);
    expect(outcome.transaction).toEqual([195, 7]);
    for (const seen of [outcome.regularSeen, outcome.transactionSeen]) {
      expect(seen.pages).toBeLessThanOrEqual(2);
      expect(seen.rows).toBeLessThanOrEqual(1000 + 1);
    }
  }, 120_000);
});

const putAll = (count: number, value: number) =>
  Effect.flatMap(DbClient, (db) =>
    db.write(DB, {
      operations: Array.from({ length: count }, (_, index) => ({
        _tag: "Put" as const,
        key: key(index),
        value: Uint8Array.of(value),
        ttlMillis: Option.none(),
      })),
    }),
  );

describe("one transaction scan in several pages (D125)", () => {
  it("pages separated by an overwrite, a flush, and a compaction all read the readSeq state", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* createDatabase;
        yield* putAll(100, 1);
        const db = yield* DbClient;
        const { readSeq, manifestId } = yield* db.beginTransaction(DB);
        const first = yield* db.scanAtPage(DB, FULL_KEY_RANGE, readSeq, 10, manifestId);
        // Every key overwritten, flushed, and compacted between the two pages.
        yield* putAll(100, 2);
        yield* db.flush(DB);
        yield* compact;
        const resumeFrom = Option.getOrThrow(first.resumeFrom);
        const second = yield* db.scanAtPage(
          DB,
          { start: resumeFrom, end: Option.none() },
          readSeq,
          10,
          manifestId,
        );
        const values = (rows: ReadonlyArray<{ readonly value: { readonly _tag: string } }>) =>
          rows.map((row) =>
            row.value._tag === "Value" &&
            "bytes" in row.value &&
            row.value.bytes instanceof Uint8Array
              ? row.value.bytes[0]
              : -1,
          );
        return {
          first: values(first.rows),
          second: values(second.rows),
          secondStart: (second.rows[0]?.key ?? new Uint8Array(0))[2],
        };
      }),
    );
    expect(outcome.first).toEqual(Array(10).fill(1));
    expect(outcome.second).toEqual(Array(10).fill(1));
    expect(outcome.secondStart).toBe(10);
  }, 120_000);

  /** Begins, scans one page of `[0, 100)` (ending at key 10), lets another writer insert key 50, and commits. */
  const phantom = (readRange: "whole range" | "page only") =>
    run(
      Effect.gen(function* () {
        yield* createDatabase;
        yield* Effect.flatMap(DbClient, (db) =>
          db.write(DB, {
            operations: Array.from({ length: 50 }, (_, index) => ({
              _tag: "Put" as const,
              key: key(index),
              value: Uint8Array.of(1),
              ttlMillis: Option.none(),
            })),
          }),
        );
        const db = yield* DbClient;
        const whole = { start: key(0), end: Option.some(key(100)) };
        const { readSeq, manifestId } = yield* db.beginTransaction(DB);
        const page = yield* db.scanAtPage(DB, whole, readSeq, 10, manifestId);
        // A phantom: beyond the page's end, inside the range scanned.
        yield* db.write(DB, {
          operations: [
            { _tag: "Put", key: key(70), value: Uint8Array.of(9), ttlMillis: Option.none() },
          ],
        });
        const named =
          readRange === "whole range"
            ? whole
            : { start: key(0), end: Option.some(Option.getOrThrow(page.resumeFrom)) };
        const committed = yield* Effect.exit(
          db.commitTransaction(DB, {
            readSeq,
            isolation: "SerializableSnapshot",
            batch: {
              operations: [
                {
                  _tag: "Put",
                  key: key(200),
                  value: Uint8Array.of(page.rows.length),
                  ttlMillis: Option.none(),
                },
              ],
            },
            readKeys: [],
            readRanges: [named],
          }),
        );
        return Exit.isSuccess(committed)
          ? "committed"
          : String(Option.getOrUndefined(Exit.findErrorOption(committed))?._tag);
      }),
    );

  it("a phantom beyond an early page's end but inside the scanned range fails the commit when readRanges names the whole range", async () => {
    expect(await phantom("whole range")).toBe("TransactionConflictError");
  }, 120_000);

  it("naming only the part a page returned misses that phantom: the commit succeeds (what the docs warn of)", async () => {
    expect(await phantom("page only")).toBe("committed");
  }, 120_000);
});

describe("a long run of one key's merge operands (D125)", () => {
  it("a page holds the whole run, never splits it, and ends right after the key with where to continue", async () => {
    const outcome = await run(
      Effect.gen(function* () {
        yield* Effect.flatMap(ManifestStore, (store) =>
          store.initialize(
            initialManifest(DB, {
              keyRange: Option.none(),
              databaseOptions: {
                ...DEFAULT_DATABASE_OPTIONS,
                mergeOperator: Option.some("U64Add"),
              },
            }),
          ),
        );
        const db = yield* DbClient;
        const one = new Uint8Array(8);
        one[0] = 1;
        // Key 0: 2,500 increments, unflushed (three writer pages of operands); key 1 after it.
        for (let increment = 0; increment < 2500; increment++) {
          yield* db.write(DB, {
            operations: [{ _tag: "Merge", key: key(0), operand: one, ttlMillis: Option.none() }],
          });
        }
        yield* db.write(DB, {
          operations: [
            { _tag: "Put", key: key(1), value: Uint8Array.of(7), ttlMillis: Option.none() },
          ],
        });
        const sum = (bytes: Uint8Array) =>
          bytes.reduce((total, byte, index) => total + byte * 2 ** (8 * index), 0);
        const { client, seen } = yield* countingClient;
        const regular = yield* regularPage(client, 5);
        const regularPages = seen.pages;
        seen.pages = 0;
        const { readSeq, manifestId } = yield* client.beginTransaction(DB);
        const transaction = yield* client
          .scanAtPage(DB, FULL_KEY_RANGE, readSeq, 5, manifestId)
          .pipe(Effect.provideService(ScanPageBudget, BUDGET));
        const describe = (page: {
          rows: ReadonlyArray<{ key: Uint8Array; value: { _tag: string } }>;
          resumeFrom: Option.Option<Uint8Array>;
        }) => ({
          rows: page.rows.map((row) => [
            row.key[2],
            row.value._tag === "Value" &&
            "bytes" in row.value &&
            row.value.bytes instanceof Uint8Array
              ? sum(row.value.bytes)
              : -1,
          ]),
          resumeFrom: Option.map(page.resumeFrom, (bytes) => bytes[2]),
        });
        return {
          regular: describe(regular),
          regularPages,
          transaction: describe(transaction),
          transactionPages: seen.pages,
        };
      }),
    );
    // The run's value is whole (2,500), and the page ends before key 1 with key 1 to continue at.
    for (const page of [outcome.regular, outcome.transaction]) {
      expect(page).toEqual({ rows: [[0, 2500]], resumeFrom: Option.some(1) });
    }
    // Every page of the run was read: operands cannot be split across pages.
    expect(outcome.regularPages).toBeGreaterThanOrEqual(3);
    expect(outcome.transactionPages).toBeGreaterThanOrEqual(3);
  }, 180_000);
});
