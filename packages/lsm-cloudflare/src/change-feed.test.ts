import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  pruneChangelog,
  SeqNum,
  Timestamp,
} from "@cf-slatedb/lsm-core";
import { Arbitrary, Effect, Option, Result, Schema } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { cutAtSequenceBoundary, DbClient, dbClientTestLayer } from "./db-client.ts";
import type { DbReaderServices } from "./db-reader.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestStore } from "./manifest-store.ts";

const DB = DatabaseId.make("orders");
const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

const provide = <A, E>(program: Effect.Effect<A, E, DbClient | ManifestStore | DbReaderServices>) =>
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
  | { readonly _tag: "Consume"; readonly limit: number };

const stepArbitrary: Arbitrary.Arbitrary<Step> = Arbitrary.all({
  kind: smallInt(0, 9),
  keys: Arbitrary.array(smallInt(0, 5), { minLength: 1, maxLength: 3 }),
  limit: smallInt(1, 4),
}).pipe(
  Arbitrary.map(({ kind, keys, limit }): Step => {
    if (kind <= 5) return { _tag: "Batch", keys };
    if (kind <= 7) return { _tag: "Flush" };
    return { _tag: "Consume", limit };
  }),
);

describe("change feed", () => {
  it("delivers every committed change exactly once, in sequence order, across flushes and pages", async () => {
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
              let cursor = SeqNum.make(0);
              const consume = (limit: number) =>
                Effect.gen(function* () {
                  const page = yield* client.changes(DB, cursor, limit);
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
                } else if (!(yield* consume(step.limit))) {
                  return false;
                }
              }
              // Drain.
              for (let page = 0; page < 200 && consumed.length < committed.length; page++) {
                if (!(yield* consume(3))) return false;
              }
              const sortBySeqThenKey = (changes: ReadonlyArray<string>) =>
                [...changes].sort((a, b) => {
                  const [seqA = 0, keyA = 0] = a.split(":").map(Number);
                  const [seqB = 0, keyB = 0] = b.split(":").map(Number);
                  return seqA - seqB || keyA - keyB;
                });
              return (
                sortBySeqThenKey(consumed).join(",") === committed.join(",") &&
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
