/**
 * A SerializableSnapshot transaction's scanned ranges (mutation sweep, D127): a commit into a range
 * the transaction scanned after it began is a read conflict, as a write to a key it read is.
 */
import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  type KeyRange,
  type TransactionIsolation,
} from "@shaledb/lsm-core";
import { Effect, Option } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { DbClient, dbClientTestLayer } from "./db-client.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestStore } from "./manifest-store.ts";

const DB = DatabaseId.make("orders");
const key = (n: number) => Uint8Array.of(0x6b, n);
const range = (start: number, end: Option.Option<number>): KeyRange => ({
  start: key(start),
  end: Option.map(end, key),
});

/** The outcome of a transaction that scanned `scanned`, after another commit wrote `written`. */
const commitAfterWriteInto = (
  scanned: KeyRange,
  written: number,
  isolation: TransactionIsolation = "SerializableSnapshot",
) =>
  Effect.gen(function* () {
    yield* (yield* ManifestStore).initialize(
      initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
    );
    const client = yield* DbClient;
    yield* client.put(DB, key(1), Uint8Array.of(1));
    const begin = yield* client.beginTransaction(DB);
    // Another client inserts a key after the transaction began.
    yield* client.put(DB, key(written), Uint8Array.of(2));
    const committed = yield* Effect.result(
      client.commitTransaction(DB, {
        readSeq: begin.readSeq,
        isolation,
        batch: {
          operations: [
            { _tag: "Put", key: key(99), value: Uint8Array.of(0), ttlMillis: Option.none() },
          ],
        },
        readKeys: [],
        readRanges: [scanned],
      }),
    );
    return committed._tag === "Failure" ? committed.failure._tag : "Success";
  }).pipe(
    Effect.scoped,
    Effect.provide(dbClientTestLayer({})),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.provide(TestClock.layer()),
    Effect.runPromise,
  );

describe("SerializableSnapshot scanned ranges (D127)", () => {
  it("refuses a commit when another wrote into a bounded or open-ended range it scanned", async () => {
    expect([
      await commitAfterWriteInto(range(3, Option.some(7)), 5),
      await commitAfterWriteInto(range(3, Option.none()), 50),
      // At the start (inclusive), a conflict; at the end (exclusive) and outside, none.
      await commitAfterWriteInto(range(3, Option.some(7)), 3),
      await commitAfterWriteInto(range(3, Option.some(7)), 7),
      await commitAfterWriteInto(range(3, Option.some(7)), 2),
      // Snapshot isolation does not check reads.
      await commitAfterWriteInto(range(3, Option.some(7)), 5, "Snapshot"),
    ]).toEqual([
      "TransactionConflictError",
      "TransactionConflictError",
      "TransactionConflictError",
      "Success",
      "Success",
      "Success",
    ]);
  });
});
