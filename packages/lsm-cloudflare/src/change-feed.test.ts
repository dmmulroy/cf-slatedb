import {
  changelogAfterSeqOf,
  CheckpointId,
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  FULL_KEY_RANGE,
  forkManifest,
  initialManifest,
  pruneChangelog,
  replaceManifestFields,
  SeqNum,
  Timestamp,
} from "@shaledb/lsm-core";
import { Arbitrary, Clock, Effect, Option, Result, Schema } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { cutAtSequenceBoundary, DbClient, dbClientTestLayer } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";

const DB = DatabaseId.make("orders");
const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

const provide = <A, E>(
  program: Effect.Effect<A, E, DbClient | ManifestStore | ManifestReader | DbReaderServices>,
) =>
  program.pipe(
    Effect.scoped,
    Effect.provide(dbClientTestLayer({ blockSize: 64 })),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
  );

const create = (changelog: boolean) =>
  Effect.gen(function* () {
    yield* (yield* ManifestStore).initialize(
      initialManifest(DB, {
        keyRange: Option.none(),
        databaseOptions: {
          ...DEFAULT_DATABASE_OPTIONS,
          changelog: changelog ? Option.some({ retentionMs: 60_000 }) : Option.none(),
        },
      }),
    );
  });

type Step =
  | { readonly _tag: "Batch"; readonly keys: ReadonlyArray<number> }
  | { readonly _tag: "Flush" }
  | { readonly _tag: "Prune" }
  | { readonly _tag: "Consume"; readonly limit: number };

const stepArbitrary: Arbitrary.Arbitrary<Step> = Arbitrary.all({
  kind: smallInt(0, 10),
  keys: Arbitrary.array(smallInt(0, 5), { minLength: 1, maxLength: 3 }),
  limit: smallInt(1, 4),
}).pipe(
  Arbitrary.map(({ kind, keys, limit }): Step => {
    if (kind <= 5) return { _tag: "Batch", keys };
    if (kind <= 7) return { _tag: "Flush" };
    if (kind === 8) return { _tag: "Prune" };
    return { _tag: "Consume", limit };
  }),
);

describe("change feed", () => {
  it("delivers every committed change exactly once, in sequence order, across flushes, pages, and retention, or says Expired (D80)", async () => {
    const result = await Effect.runPromise(
      Arbitrary.checkEffect(
        Arbitrary.array(stepArbitrary, { minLength: 10, maxLength: 40 }),
        (steps) =>
          provide(
            Effect.gen(function* () {
              yield* create(true);
              const client = yield* DbClient;
              // Each committed change as "seq:key", in commit order (one seq per batch).
              const committed: Array<string> = [];
              const consumed: Array<string> = [];
              // Changes the feed reported lost (Expired), which the consumer skipped knowingly.
              const lost: Array<string> = [];
              let cursor = SeqNum.make(0);
              const seqOf = (change: string) => Number(change.split(":")[0]);
              const between = (after: number, upTo: number) =>
                committed.filter((change) => seqOf(change) > after && seqOf(change) <= upTo);
              const consume = (limit: number) =>
                Effect.gen(function* () {
                  const result = yield* Effect.result(client.changes(DB, cursor, limit));
                  if (Result.isFailure(result)) {
                    const failure = result.failure;
                    if (
                      failure._tag !== "ChangelogUnavailableError" ||
                      failure.reason !== "Expired"
                    ) {
                      return false;
                    }
                    // Expired only when retention really dropped changes after the cursor; the
                    // consumer restarts from where the changelog holds every change.
                    const heldAfter = changelogAfterSeqOf(
                      (yield* (yield* ManifestReader).loadLatest(DB)).manifest,
                    );
                    if (heldAfter <= cursor) return false;
                    lost.push(...between(cursor, heldAfter));
                    cursor = heldAfter;
                    return true;
                  }
                  const page = result.success;
                  // Exactly the committed changes in (cursor, nextAfterSeq]: no silent gap.
                  const got = page.rows.map((row) => `${row.seq}:${row.key[0]}`);
                  if (
                    [...got].sort().join(",") !==
                    [...between(cursor, page.nextAfterSeq)].sort().join(",")
                  ) {
                    return false;
                  }
                  // Pages never split a batch.
                  const seqs = new Set(page.rows.map((row) => row.seq));
                  for (const seq of seqs) {
                    const inPage = page.rows.filter((row) => row.seq === seq).length;
                    const total = committed.filter((change) => change.startsWith(`${seq}:`)).length;
                    if (inPage !== total) return false;
                  }
                  consumed.push(...page.rows.map((row) => `${row.seq}:${row.key[0]}`));
                  cursor = page.nextAfterSeq;
                  return true;
                });
              for (const step of steps) {
                if (step._tag === "Batch") {
                  const keys = [...new Set(step.keys)];
                  const receipt = yield* client.write(DB, {
                    operations: keys.map((key) => ({
                      _tag: "Put" as const,
                      key: Uint8Array.of(key),
                      value: Uint8Array.of(key),
                      ttlMillis: Option.none(),
                    })),
                  });
                  committed.push(
                    ...keys.sort((a, b) => a - b).map((key) => `${receipt.seq}:${key}`),
                  );
                } else if (step._tag === "Flush") {
                  yield* client.flush(DB);
                  yield* TestClock.adjust("2 seconds");
                } else if (step._tag === "Prune") {
                  // Retention drops every entry flushed so far.
                  yield* prune(yield* Clock.currentTimeMillis);
                  yield* TestClock.adjust("2 seconds");
                } else if (!(yield* consume(step.limit))) {
                  return false;
                }
              }
              // Drain.
              for (
                let page = 0;
                page < 200 && consumed.length + lost.length < committed.length;
                page++
              ) {
                if (!(yield* consume(3))) return false;
              }
              const sortBySeqThenKey = (changes: ReadonlyArray<string>) =>
                [...changes].sort((a, b) => {
                  const [seqA = 0, keyA = 0] = a.split(":").map(Number);
                  const [seqB = 0, keyB = 0] = b.split(":").map(Number);
                  return seqA - seqB || keyA - keyB;
                });
              return (
                sortBySeqThenKey([...lost, ...consumed]).join(",") === committed.join(",") &&
                consumed.every((change, index) => {
                  const previous = consumed[index - 1];
                  return (
                    previous === undefined ||
                    Number(previous.split(":")[0]) <= Number(change.split(":")[0])
                  );
                })
              );
            }),
          ),
        { runs: 120 },
      ),
    );
    const failure = Arbitrary.formatCheckFailure(result);
    if (failure !== undefined) throw new Error(failure);
  });

  it("refuses a disabled changelog, and changes already dropped by retention", async () => {
    const disabled = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* create(false);
          const client = yield* DbClient;
          yield* client.put(DB, Uint8Array.of(1), Uint8Array.of(1));
          yield* client.flush(DB);
          yield* TestClock.adjust("2 seconds");
          return yield* Effect.flip(client.changes(DB, SeqNum.make(0), 10));
        }),
      ),
    );
    expect(disabled._tag === "ChangelogUnavailableError" && disabled.reason).toBe("Disabled");
    const expired = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* create(true);
          const client = yield* DbClient;
          yield* client.put(DB, Uint8Array.of(1), Uint8Array.of(1));
          yield* client.flush(DB);
          yield* TestClock.adjust("120 seconds");
          yield* client.put(DB, Uint8Array.of(2), Uint8Array.of(2));
          yield* client.flush(DB);
          // GC's retention prune drops the first flush's changelog entry.
          yield* (yield* ManifestStore).update(DB, (manifest) =>
            Result.succeed(pruneChangelog(manifest, Timestamp.make(60_000))),
          );
          yield* TestClock.adjust("2 seconds");
          const current = yield* client.changes(DB, SeqNum.make(1), 10);
          const behind = yield* Effect.flip(client.changes(DB, SeqNum.make(0), 10));
          return { current: current.rows.map((row) => row.key[0]), behind };
        }),
      ),
    );
    expect(expired.current).toEqual([2]);
    expect(expired.behind._tag === "ChangelogUnavailableError" && expired.behind.reason).toBe(
      "Expired",
    );
  });

  it("cuts pages only between sequence numbers", () => {
    const row = (seq: number, key: number) => ({
      key: Uint8Array.of(key),
      seq: SeqNum.make(seq),
      value: { _tag: "Tombstone" as const },
      createTs: Option.none(),
      expireTs: Option.none(),
    });
    const rows = [row(1, 1), row(2, 1), row(2, 2), row(3, 1)];
    const seqs = (limit: number) => cutAtSequenceBoundary(rows, limit).map((r) => r.seq);
    expect(seqs(2)).toEqual([1]);
    expect(seqs(3)).toEqual([1, 2, 2]);
    expect(cutAtSequenceBoundary([row(2, 1), row(2, 2)], 1).map((r) => r.seq)).toEqual([2, 2]);
  });
});

/** The outcome of one feed request: the keys and the next cursor, or the unavailable reason. */
const feed = (databaseId: DatabaseId, afterSeq: number) =>
  Effect.gen(function* () {
    const result = yield* Effect.result(
      (yield* DbClient).changes(databaseId, SeqNum.make(afterSeq), 10),
    );
    if (Result.isSuccess(result)) {
      return {
        keys: result.success.rows.map((row) => row.key[0]),
        next: result.success.nextAfterSeq,
      };
    }
    const failure = result.failure;
    return failure._tag === "ChangelogUnavailableError" ? failure.reason : failure._tag;
  });

/** GC's retention prune of every changelog entry created before `cutoff`. */
const prune = (cutoff: number) =>
  Effect.flatMap(ManifestStore, (store) =>
    store.update(DB, (manifest) =>
      Result.succeed(pruneChangelog(manifest, Timestamp.make(cutoff))),
    ),
  );

describe("change feed gaps (D80)", () => {
  it("is Expired, not silently empty, when retention pruned every entry after afterSeq", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* create(true);
          const client = yield* DbClient;
          yield* client.put(DB, Uint8Array.of(1), Uint8Array.of(1));
          yield* client.flush(DB);
          yield* TestClock.adjust("120 seconds");
          yield* prune(60_000);
          const nothingSince = yield* feed(DB, 0);
          // An unflushed change above the pruned range.
          yield* client.put(DB, Uint8Array.of(2), Uint8Array.of(2));
          yield* TestClock.adjust("2 seconds");
          return {
            nothingSince,
            withUnflushed: yield* feed(DB, 0),
            // Exactly at the pruned boundary: everything after it is still there.
            atBoundary: yield* feed(DB, 1),
          };
        }),
      ),
    );
    expect(outcome).toEqual({
      nothingSince: "Expired",
      withUnflushed: "Expired",
      atBoundary: { keys: [2], next: 2 },
    });
  });

  it("answers from the boundary of a prune that kept later entries, and is Expired one below it", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* create(true);
          const client = yield* DbClient;
          yield* client.put(DB, Uint8Array.of(1), Uint8Array.of(1));
          yield* client.put(DB, Uint8Array.of(2), Uint8Array.of(2));
          yield* client.flush(DB);
          yield* TestClock.adjust("120 seconds");
          yield* client.put(DB, Uint8Array.of(3), Uint8Array.of(3));
          yield* client.flush(DB);
          yield* prune(60_000);
          yield* TestClock.adjust("2 seconds");
          return [yield* feed(DB, 1), yield* feed(DB, 2), yield* feed(DB, 3)];
        }),
      ),
    );
    expect(outcome).toEqual(["Expired", { keys: [3], next: 3 }, { keys: [], next: 3 }]);
  });

  it("serves a fork's feed only from the fork point, before and after its first flush", async () => {
    const FORK = DatabaseId.make("orders-fork");
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* create(true);
          const client = yield* DbClient;
          yield* client.put(DB, Uint8Array.of(1), Uint8Array.of(1));
          yield* client.put(DB, Uint8Array.of(2), Uint8Array.of(2));
          yield* client.flush(DB);
          const source = yield* (yield* ManifestReader).loadLatest(DB);
          yield* (yield* ManifestStore).initialize(
            forkManifest(source.manifest, {
              databaseId: FORK,
              checkpointId: CheckpointId.make("01JD0000000000000000000001"),
              range: FULL_KEY_RANGE,
            }),
          );
          const forkPoint = source.manifest.lastL0Seq;
          yield* client.put(FORK, Uint8Array.of(3), Uint8Array.of(3));
          const beforeFlush = [yield* feed(FORK, 0), yield* feed(FORK, forkPoint)];
          yield* client.flush(FORK);
          yield* TestClock.adjust("2 seconds");
          const afterFlush = [yield* feed(FORK, forkPoint - 1), yield* feed(FORK, forkPoint)];
          return { forkPoint, beforeFlush, afterFlush, source: yield* feed(DB, 0) };
        }),
      ),
    );
    expect(outcome).toEqual({
      forkPoint: 2,
      // Changes up to the fork point belong to the source's feed.
      beforeFlush: ["Expired", { keys: [3], next: 3 }],
      afterFlush: ["Expired", { keys: [3], next: 3 }],
      source: { keys: [1, 2], next: 2 },
    });
  });
});

describe("change feed pages (D93)", () => {
  it("returns an unflushed batch larger than the writer's page whole, never part of it", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* create(true);
          const client = yield* DbClient;
          const receipt = yield* client.write(DB, {
            operations: Array.from({ length: 1500 }, (_, index) => ({
              _tag: "Put" as const,
              key: Uint8Array.of(index >> 8, index & 0xff),
              value: Uint8Array.of(1),
              ttlMillis: Option.none(),
            })),
          });
          yield* client.put(DB, Uint8Array.of(0xff, 0xff), Uint8Array.of(2));
          const page = yield* client.changes(DB, SeqNum.make(0), 10);
          return { seq: receipt.seq, rows: page.rows.length, next: page.nextAfterSeq };
        }),
      ),
    );
    // The page holds the whole first batch; resuming after it skips none of its rows.
    expect(outcome).toEqual({ seq: 1, rows: 1500, next: 1 });
  });
});

describe("change feed pages of flushed changes (D93)", () => {
  it("bounds the flushed part by bytes at a batch boundary, and never skips the changes after it", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* create(true);
          const client = yield* DbClient;
          // Two batches of four 1,000 KiB values (about 4 MB each) and one of four 100 KiB values,
          // flushed; then one unflushed. The bound falls inside the third batch.
          const batch = (first: number, valueBytes = 1000 * 1024) =>
            client.write(DB, {
              operations: Array.from({ length: 4 }, (_, index) => ({
                _tag: "Put" as const,
                key: Uint8Array.of(first + index),
                value: new Uint8Array(valueBytes),
                ttlMillis: Option.none(),
              })),
            });
          yield* batch(0);
          yield* batch(4);
          yield* batch(8, 100 * 1024);
          yield* client.flush(DB);
          yield* TestClock.adjust("2 seconds");
          yield* client.put(DB, Uint8Array.of(99), Uint8Array.of(1));
          const pages: Array<{ readonly seqs: ReadonlyArray<number>; readonly next: number }> = [];
          let cursor = SeqNum.make(0);
          for (let page = 0; page < 10; page++) {
            const read = yield* client.changes(DB, cursor, 1000);
            if (read.rows.length === 0) break;
            pages.push({
              seqs: [...new Set(read.rows.map((row) => row.seq))],
              next: read.nextAfterSeq,
            });
            cursor = read.nextAfterSeq;
          }
          return pages;
        }),
      ),
    );
    // About 8 MB per page, whole batches only; the unflushed change follows the last flushed one.
    expect(outcome).toEqual([
      { seqs: [1, 2], next: 2 },
      { seqs: [3, 4], next: 4 },
    ]);
  });
});

describe("change feed over a changelog entry an earlier one overlaps (D113)", () => {
  // A manifest written before D113 can hold two entries for the same changes: a flush exported
  // them again after a commit whose response was lost. The feed delivers each change once.
  it("delivers each change once when a later entry repeats an earlier one", async () => {
    const outcome = await Effect.runPromise(
      provide(
        Effect.gen(function* () {
          yield* create(true);
          const client = yield* DbClient;
          yield* client.put(DB, Uint8Array.of(1), Uint8Array.of(1));
          yield* client.flush(DB);
          // The repeat: a second entry holding seq 1 again, as the code before D113 wrote.
          yield* (yield* ManifestStore).update(DB, (manifest) =>
            Result.succeed(
              replaceManifestFields(manifest, {
                changelog: [...manifest.changelog, ...manifest.changelog],
              }),
            ),
          );
          yield* client.put(DB, Uint8Array.of(2), Uint8Array.of(2));
          yield* client.flush(DB);
          const page = yield* client.changes(DB, SeqNum.make(0), 10);
          return page.rows.map((row) => row.seq);
        }),
      ),
    );
    expect(outcome).toEqual([1, 2]);
  });
});
