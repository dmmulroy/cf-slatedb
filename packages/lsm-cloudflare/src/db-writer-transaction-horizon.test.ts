/**
 * The transaction commit's horizon checks (mutation sweep, D127): a snapshot exactly at the
 * retention horizon can still be checked and commits; one below it, or beyond the newest
 * sequence number, is refused as SnapshotTooOld.
 */
import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  initialManifest,
  SeqNum,
  type WriteBatch,
} from "@shaledb/lsm-core";
import { Effect, Option } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { DbWriter, dbWriterTestLayer } from "./db-writer.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestStore } from "./manifest-store.ts";

const DB = DatabaseId.make("orders");

const put = (key: string): WriteBatch => ({
  operations: [
    {
      _tag: "Put",
      key: new TextEncoder().encode(key),
      value: Uint8Array.of(1),
      ttlMillis: Option.none(),
    },
  ],
});

describe("DbWriter.commitTransaction horizon (D127)", () => {
  it("commits a snapshot at the retention horizon, refuses one below it or beyond lastSeq", async () => {
    const outcome = await Effect.gen(function* () {
      yield* (yield* ManifestStore).initialize(
        initialManifest(DB, { keyRange: Option.none(), databaseOptions: DEFAULT_DATABASE_OPTIONS }),
      );
      const writer = yield* DbWriter;
      // Two flushes; retention of no bytes trims the older one, so the horizon is its cut.
      for (const key of ["a", "b"]) {
        yield* writer.write(DB, put(key));
        yield* writer.flush(DB);
      }
      yield* writer.write(DB, put("c"));
      const status = yield* writer.status(DB);
      const commitAt = (readSeq: number) =>
        Effect.map(
          Effect.result(
            writer.commitTransaction(DB, {
              readSeq: SeqNum.make(readSeq),
              isolation: "SerializableSnapshot",
              batch: put(`t${readSeq}`),
              readKeys: [new TextEncoder().encode("zz")],
              readRanges: [],
            }),
          ),
          (result) =>
            result._tag === "Success"
              ? "Success"
              : result.failure._tag === "TransactionConflictError"
                ? result.failure.reason
                : result.failure._tag,
        );
      const horizon = status.retainedFromSeq;
      return {
        horizon,
        below: yield* commitAt(horizon - 1),
        beyond: yield* commitAt(status.lastSeq + 1),
        at: yield* commitAt(horizon),
      };
    }).pipe(
      Effect.provide(dbWriterTestLayer({ maxRetainedBytes: 0 })),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );
    expect(outcome).toEqual({
      horizon: 1,
      below: "SnapshotTooOld",
      beyond: "SnapshotTooOld",
      at: "Success",
    });
  });
});
