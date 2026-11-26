import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  FULL_KEY_RANGE,
  initialManifest,
  SeqNum,
} from "@cf-slatedb/lsm-core";
import { Effect, Layer, Option } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { compactionTriggerTestLayer } from "./compaction-trigger.ts";
import { lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { ManifestStore, manifestStoreTestLayer } from "./manifest-store.ts";
import { WriterClient, writerClientTestLayer } from "./writer-client.ts";

const bytes = (text: string) => new TextEncoder().encode(text);
const A = DatabaseId.make("tenant-a");
const B = DatabaseId.make("tenant-b");

const layer = writerClientTestLayer.pipe(
  Layer.provideMerge(manifestStoreTestLayer),
  Layer.provideMerge(Layer.mergeAll(lsmConfigurationTestLayer(), compactionTriggerTestLayer)),
  Layer.provideMerge(lsmTestRuntimeContextLayer),
);

describe("WriterClient over the writer HTTP API", () => {
  it("round-trips writes, probes, scans, flushes, and typed errors for independent databases", async () => {
    const outcome = await Effect.gen(function* () {
      const store = yield* ManifestStore;
      for (const id of [A, B]) {
        yield* store.initialize(
          initialManifest(id, {
            keyRange: Option.none(),
            databaseOptions: DEFAULT_DATABASE_OPTIONS,
          }),
        );
      }
      const client = yield* WriterClient;
      const receipt = yield* client.write(A, {
        operations: [
          {
            _tag: "Put",
            key: bytes("k"),
            value: Uint8Array.of(0, 255, 7),
            ttlMillis: Option.some(1000),
          },
          { _tag: "Merge", key: bytes("m"), operand: bytes("x"), ttlMillis: Option.none() },
        ],
      });
      yield* client.write(B, { operations: [{ _tag: "Delete", key: bytes("k") }] });
      const probe = yield* client.probe(A, bytes("k"));
      const scan = yield* client.scan(A, {
        range: FULL_KEY_RANGE,
        snapshotSeq: Option.none(),
        cursor: Option.none(),
        limit: 1,
      });
      const changes = yield* client.changes(A, SeqNum.make(0), 10);
      const flushed = yield* client.flush(A);
      yield* client.seal(B, [DatabaseId.make("tenant-b2")], false);
      const sealed = yield* Effect.flip(
        client.write(B, { operations: [{ _tag: "Delete", key: bytes("k") }] }),
      );
      const invalid = yield* Effect.flip(client.write(A, { operations: [] }));
      const missing = yield* Effect.flip(client.status(DatabaseId.make("absent")));
      const statusB = yield* client.status(B);
      return { receipt, probe, scan, changes, flushed, sealed, invalid, missing, statusB };
    }).pipe(
      Effect.scoped,
      Effect.provide(layer),
      Effect.provide(TestClock.layer()),
      Effect.runPromise,
    );

    expect(outcome.receipt.seq).toBe(1);
    const [version] = outcome.probe.versions;
    expect(version?.value).toEqual({ _tag: "Value", bytes: Uint8Array.of(0, 255, 7) });
    expect(
      Option.map(version?.expireTs ?? Option.none(), (ts) => ts - outcome.receipt.createTs),
    ).toEqual(Option.some(1000));
    expect([outcome.scan.rows.length, Option.isSome(outcome.scan.nextCursor)]).toEqual([1, true]);
    expect(outcome.changes.rows.length).toBe(2);
    expect(outcome.flushed).toBe(1);
    expect(outcome.sealed._tag === "DatabaseSealedError" && outcome.sealed.successors).toEqual([
      "tenant-b2",
    ]);
    expect(outcome.invalid._tag === "WriteBatchInvalidError" && outcome.invalid.reason).toBe(
      "Empty",
    );
    expect(outcome.missing._tag).toBe("DatabaseNotFoundError");
    expect([outcome.statusB.sealed, outcome.statusB.flushedSeq]).toEqual([true, 1]);
  });
});
