import {
  type CompactionJob,
  type CompactorEpoch,
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  ManifestId,
  planCompactions,
  type Row,
  SeqNum,
  type SstHandle,
} from "@shaledb/lsm-core";
import { Bytes, Cause, Clock, Effect, Exit, Layer, Option, type Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { CompactionExecutor, makeCompactionExecutor } from "./compaction-executor.ts";
import { DbClient, dbClientTestLayer } from "./db-client.ts";
import { type DbReaderServices, ScanBudget, ScanPageBudget } from "./db-reader.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";
import { sstStoreLayerWithoutDependencies } from "./sst-store.ts";

/**
 * The oracle for scan pages (D125): random histories with long runs of deletes and expired values,
 * random limits and page budgets of a few hundred bytes, and writes, flushes, and compactions
 * between pages. Following `resumeFrom` must yield exactly the rows the surface promises, none
 * skipped or repeated, and every page must stay inside its budget (plus the one key it may finish).
 *
 * - **Regular (`Latest`):** each page is its own read; its rows are the live keys of
 *   `[start, next)` at the time of that page (`next` is `resumeFrom`, or the end).
 * - **Transaction:** every page reads at `readSeq`; together the pages are the live keys at the
 *   transaction's begin, whatever lands between pages.
 */
const DB = DatabaseId.make("orders");
const KEYS = 160;
const key = (index: number) => Uint8Array.of(0x6b, index);
const indexOf = (bytes: Uint8Array) => bytes[1] ?? -1;

const testLayer = Layer.effect(CompactionExecutor, makeCompactionExecutor).pipe(
  Layer.provide(sstStoreLayerWithoutDependencies),
  Layer.provideMerge(dbClientTestLayer({ blockSize: 256 })),
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

/** A small seeded generator, so a failing history can be replayed by its seed. */
const generator = (seed: number) => {
  let state = seed;
  const next = () => {
    state = (state * 1103515245 + 12345) & 0x7fffffff;
    return state / 0x7fffffff;
  };
  return {
    int: (below: number) => Math.floor(next() * below),
    chance: (p: number) => next() < p,
  };
};

/** The oracle: each key's value and when it expires (none: never), at the current time. */
type Oracle = Map<number, { value: number; expireAt: Option.Option<number> }>;

const liveIn = (oracle: Oracle, now: number, from: number, to: number) =>
  [...oracle.entries()]
    .filter(
      ([index, entry]) =>
        index >= from &&
        index < to &&
        !Option.exists(entry.expireAt, (expireAt) => expireAt <= now),
    )
    .map(([index, entry]) => [index, entry.value] as const)
    .sort((left, right) => left[0] - right[0]);

let jobCounter = 0;
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
      return yield* executor.commit(prepared, outputs, `oracle-job-${++jobCounter}`);
    });
  yield* runJob(job, manifest.compactorEpoch);
});

/** One random step of history: a run of puts, a long run of deletes or expiring puts, a flush, a compaction, or time passing. */
const step = (random: ReturnType<typeof generator>, oracle: Oracle, version: { n: number }) =>
  Effect.gen(function* () {
    const db = yield* DbClient;
    const now = yield* Clock.currentTimeMillis;
    const kind = random.int(10);
    const from = random.int(KEYS);
    const length = 1 + random.int(kind < 4 ? 40 : 8);
    const indexes = Array.from({ length }, (_, offset) => from + offset).filter(
      (index) => index < KEYS,
    );
    if (kind <= 3) {
      // A long run of deletes, or of values that expire in 1 s.
      const expiring = kind >= 2;
      const value = ++version.n & 0xff;
      yield* db.write(DB, {
        operations: indexes.map((index) =>
          expiring
            ? {
                _tag: "Put" as const,
                key: key(index),
                value: Uint8Array.of(value),
                ttlMillis: Option.some(1000),
              }
            : { _tag: "Delete" as const, key: key(index) },
        ),
      });
      for (const index of indexes) {
        if (expiring)
          oracle.set(index, { value: version.n & 0xff, expireAt: Option.some(now + 1000) });
        else oracle.delete(index);
      }
    } else if (kind <= 6) {
      const value = ++version.n & 0xff;
      yield* db.write(DB, {
        operations: indexes.map((index) => ({
          _tag: "Put" as const,
          key: key(index),
          value: Uint8Array.of(value),
          ttlMillis: Option.none(),
        })),
      });
      for (const index of indexes) oracle.set(index, { value, expireAt: Option.none() });
    } else if (kind === 7) {
      yield* db.flush(DB);
    } else if (kind === 8) {
      yield* db.flush(DB);
      yield* compact;
    } else {
      yield* TestClock.adjust("2 seconds");
    }
  });

/** A page's outcome: its rows, where to continue, and what it examined. */
interface Page {
  readonly rows: ReadonlyArray<Row>;
  readonly resumeFrom: Option.Option<Uint8Array>;
  readonly examined: number;
}

/** One regular `Latest` page, as the public handler makes it: limit + 1, budget end resumable. */
const regularPage = (start: Uint8Array, limit: number, maxBytes: number) =>
  Effect.gen(function* () {
    const budget = new ScanBudget(maxBytes);
    const rows: Array<Row> = [];
    const exit = yield* Effect.exit(
      Stream.runForEach(
        (yield* DbClient).scan(
          DB,
          { start, end: Option.none() },
          {
            consistency: { _tag: "Latest" },
            snapshot: Option.none(),
            refuseSealed: false,
            limit: Option.some(limit + 1),
          },
        ),
        (row) => Effect.sync(() => void rows.push(row)),
      ).pipe(Effect.provideService(ScanPageBudget, { maxBytes, shared: Option.some(budget) })),
    );
    if (Exit.isFailure(exit)) {
      const failure = Cause.findErrorOption(exit.cause);
      if (
        Option.isSome(failure) &&
        failure.value._tag === "ReaderUnavailableError" &&
        failure.value.resumeFrom !== undefined
      ) {
        const page: Page = {
          rows,
          resumeFrom: Option.some(failure.value.resumeFrom),
          examined: budget.examinedBytes,
        };
        return page;
      }
      return yield* Effect.failCause(exit.cause);
    }
    const next = rows[limit];
    const page: Page = {
      rows: rows.slice(0, limit),
      resumeFrom: next === undefined ? Option.none() : Option.some(next.key),
      examined: budget.examinedBytes,
    };
    return page;
  });

/** One transaction page at `readSeq`, through the public `scanAtPage`. */
const transactionPage = (
  start: Uint8Array,
  limit: number,
  maxBytes: number,
  readSeq: SeqNum,
  manifestId: Option.Option<ManifestId>,
) =>
  Effect.gen(function* () {
    const budget = new ScanBudget(maxBytes);
    const answer = yield* (yield* DbClient)
      .scanAtPage(DB, { start, end: Option.none() }, readSeq, limit, manifestId)
      .pipe(Effect.provideService(ScanPageBudget, { maxBytes, shared: Option.some(budget) }));
    const page: Page = { ...answer, examined: budget.examinedBytes };
    return page;
  });

/** The most a page may examine past its budget: the one key it finishes (a few versions here). */
const ONE_KEY_SLACK = 4096;

describe("scan pages against the oracle (D125)", () => {
  it("regular and transaction pages yield exactly the promised rows, each inside its budget", async () => {
    let earlyEnds = 0;
    for (let seed = 1; seed <= 30; seed++) {
      const random = generator(seed);
      const outcome = await run(
        Effect.gen(function* () {
          yield* Effect.flatMap(ManifestStore, (store) =>
            store.initialize(
              initialManifest(DB, {
                keyRange: Option.none(),
                databaseOptions: DEFAULT_DATABASE_OPTIONS,
              }),
            ),
          );
          const oracle: Oracle = new Map();
          const version = { n: 0 };
          for (let index = 0; index < 25; index++) yield* step(random, oracle, version);
          // The transaction begins now: its pages must read this state, whatever follows.
          const { readSeq, manifestId } = yield* (yield* DbClient).beginTransaction(DB);
          const atBegin = {
            oracle: new Map(oracle),
            now: yield* Clock.currentTimeMillis,
          };
          const failures: Array<string> = [];
          let early = 0;
          // Transaction pages at readSeq, with writes, flushes, and compactions between them (no
          // time passes, so the oracle at the begin is exact).
          const collected: Array<readonly [number, number]> = [];
          let txStart: Uint8Array = new Uint8Array(0);
          for (let pages = 0; pages < 400; pages++) {
            const limit = 1 + random.int(6);
            const maxBytes = 200 + random.int(800);
            const page = yield* transactionPage(txStart, limit, maxBytes, readSeq, manifestId);
            for (const row of page.rows) {
              collected.push([
                indexOf(row.key),
                row.value._tag === "Value" ? (row.value.bytes[0] ?? -1) : -1,
              ]);
            }
            if (page.rows.length > limit)
              failures.push(`seed ${seed} transaction page ${pages}: over its limit`);
            if (page.examined > maxBytes + ONE_KEY_SLACK) {
              failures.push(
                `seed ${seed} transaction page ${pages}: examined ${page.examined} of ${maxBytes}`,
              );
            }
            if (Option.isNone(page.resumeFrom)) break;
            txStart = page.resumeFrom.value;
            // Writes, flushes, and compactions between pages; not time: the oracle is at begin.
            if (random.chance(0.4)) {
              const kind = random.int(3);
              if (kind === 0) yield* (yield* DbClient).flush(DB);
              else if (kind === 1) {
                yield* (yield* DbClient).flush(DB);
                yield* compact;
              } else {
                const index = random.int(KEYS);
                oracle.set(index, { value: 250, expireAt: Option.none() });
                yield* (yield* DbClient).write(DB, {
                  operations: [
                    {
                      _tag: "Put",
                      key: key(index),
                      value: Uint8Array.of(250),
                      ttlMillis: Option.none(),
                    },
                  ],
                });
              }
            }
          }
          const want = liveIn(atBegin.oracle, atBegin.now, 0, KEYS);
          if (JSON.stringify(collected) !== JSON.stringify(want)) {
            failures.push(
              `seed ${seed} transaction: got ${JSON.stringify(collected)}, want ${JSON.stringify(want)}`,
            );
          }
          // Regular pages, with history between them.
          let start: Uint8Array = new Uint8Array(0);
          for (let pages = 0; pages < 400; pages++) {
            const limit = 1 + random.int(6);
            const maxBytes = 200 + random.int(800);
            const page = yield* regularPage(start, limit, maxBytes);
            const now = yield* Clock.currentTimeMillis;
            const from = indexOf(start.length === 0 ? Uint8Array.of(0, 0) : start);
            const to = Option.match(page.resumeFrom, { onNone: () => KEYS, onSome: indexOf });
            const got = page.rows.map((row) => [
              indexOf(row.key),
              row.value._tag === "Value" ? (row.value.bytes[0] ?? -1) : -1,
            ]);
            const want = liveIn(oracle, now, Math.max(0, from), to);
            if (JSON.stringify(got) !== JSON.stringify(want)) {
              failures.push(
                `seed ${seed} regular page ${pages}: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`,
              );
            }
            if (page.rows.length > limit)
              failures.push(`seed ${seed} regular page ${pages}: over its limit`);
            if (page.examined > maxBytes + ONE_KEY_SLACK) {
              failures.push(
                `seed ${seed} regular page ${pages}: examined ${page.examined} of ${maxBytes}`,
              );
            }
            if (Option.isSome(page.resumeFrom) && page.rows.length < limit) early++;
            if (Option.isNone(page.resumeFrom)) break;
            if (Bytes.Order(page.resumeFrom.value, start) <= 0 && start.length > 0) {
              failures.push(`seed ${seed} regular page ${pages}: did not move on`);
              break;
            }
            start = page.resumeFrom.value;
            if (random.chance(0.4)) yield* step(random, oracle, version);
          }
          return { failures, early };
        }),
      );
      expect(outcome.failures, `seed ${seed}`).toEqual([]);
      earlyEnds += outcome.early;
    }
    // The budgets did end pages early: the property covers that outcome, not only full pages.
    expect(earlyEnds).toBeGreaterThan(30);
  }, 600_000);
});
