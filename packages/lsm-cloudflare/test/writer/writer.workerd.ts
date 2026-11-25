import * as Cloudflare from "@alchemy.run/cloudflare";
import { DatabaseId, FULL_KEY_RANGE, type ReadConsistency } from "@cf-slatedb/lsm-core";
import * as Test from "@alchemy.run/test/Vitest";
import { expect } from "@effect/vitest";
import { Effect, Option, Schedule } from "effect";
import { HttpClient } from "effect/http";
import { HttpApiClient } from "effect/http-api";

import { DbHttpApi } from "../../src/db-http-api.ts";
import { GcHttpApi } from "../../src/gc-http-api.ts";
import { useDelayedCollector, workerUrl } from "../telemetry/delayed-otlp-collector.ts";
import { INTEGRATION_PORTS } from "./integration-ports.ts";
import { writerTestStack } from "./writer-stack.ts";

// Bound into the Worker at deploy time by the settings Config; flush after two seconds of age.
// Compactors poll every half second, and GC may collect what is a second old.
process.env["CF_SLATEDB_LSM_SETTINGS"] = JSON.stringify({
  maxL0Age: 2000,
  compactorPollInterval: 500,
  gcMinAge: 1000,
});

// The Workers flush telemetry before responding in test stages; this collector answers at once and
// records the span names the test asserts on.
const collector = useDelayedCollector(0, INTEGRATION_PORTS.collector);

const { test } = Test.make({
  providers: Cloudflare.providers(),
  adopt: false,
  dev: true,
  stage: "test-writer",
});

const bytes = (text: string) => new TextEncoder().encode(text);

test.provider(
  "API Worker → WriterServer (SQLite) → local R2: create, write, flush, alarm flush, and public errors",
  (stack) =>
    Effect.gen(function* () {
      const deployed = yield* stack.deploy(writerTestStack);
      const client = yield* HttpApiClient.makeWith(DbHttpApi, {
        baseUrl: workerUrl(deployed.url),
        httpClient: yield* HttpClient.HttpClient,
      });
      const databaseId = DatabaseId.make(`writer-${Date.now().toString(36)}`);
      const created = yield* client.databases.createDatabase({
        payload: {
          databaseId,
          keyRange: Option.none(),
          mergeOperator: Option.some("Concat"),
          changelogRetentionMs: Option.none(),
          budgetGroup: Option.none(),
        },
      });
      expect(created.manifestId).toBe(1);

      const duplicate = yield* Effect.flip(
        client.databases.createDatabase({
          payload: {
            databaseId,
            keyRange: Option.none(),
            mergeOperator: Option.none(),
            changelogRetentionMs: Option.none(),
            budgetGroup: Option.none(),
          },
        }),
      );
      expect(duplicate._tag).toBe("DatabaseAlreadyExistsPublicError");

      const first = yield* client.databases.write({
        params: { databaseId },
        payload: {
          operations: [
            { _tag: "Put", key: bytes("a"), value: bytes("1"), ttlMillis: Option.none() },
            { _tag: "Merge", key: bytes("log"), operand: bytes("x"), ttlMillis: Option.none() },
          ],
        },
      });
      const second = yield* client.databases.write({
        params: { databaseId },
        payload: {
          operations: [
            { _tag: "Merge", key: bytes("log"), operand: bytes("y"), ttlMillis: Option.none() },
          ],
        },
      });
      expect([first.seq, second.seq]).toEqual([1, 2]);

      const before = yield* client.databases.status({ params: { databaseId } });
      expect([before.writerEpoch, before.lastSeq]).toEqual([1, 2]);
      // Either still unflushed, or the age-based alarm already flushed both batches.
      expect([
        [0, 3],
        [2, 0],
      ]).toContainEqual([before.flushedSeq, before.unflushedRows]);

      const flushed = yield* client.databases.flush({ params: { databaseId } });
      expect(flushed.flushedSeq).toBe(2);

      // The alarm flushes a write once it is older than maxL0Age (two seconds here).
      yield* client.databases.write({
        params: { databaseId },
        payload: { operations: [{ _tag: "Delete", key: bytes("a") }] },
      });
      const afterAlarm = yield* client.databases.status({ params: { databaseId } }).pipe(
        Effect.repeat({
          schedule: Schedule.spaced("500 millis"),
          until: (status) => status.flushedSeq === 3,
          times: 20,
        }),
      );
      expect([afterAlarm.flushedSeq, afterAlarm.unflushedRows]).toEqual([3, 0]);
      // One L0 SST per flush; the age-based alarm may also have flushed between the first batches.
      expect(afterAlarm.l0Count).toBeGreaterThanOrEqual(2);

      const missing = yield* Effect.flip(
        client.databases.status({ params: { databaseId: DatabaseId.make("does-not-exist") } }),
      );
      expect(missing._tag).toBe("DatabaseNotFoundPublicError");
      expect(missing._tag === "DatabaseNotFoundPublicError" && missing.requestId.length > 0).toBe(
        true,
      );

      const invalid = yield* Effect.flip(
        client.databases.write({ params: { databaseId }, payload: { operations: [] } }),
      );
      expect(invalid._tag === "BatchInvalidPublicError" && invalid.details.reason).toBe("Empty");

      // Reads run in the API Worker and fetch blocks from the BlockServer over its binding.
      const read = (key: string, consistency: ReadConsistency) =>
        client.databases
          .get({
            params: { databaseId },
            payload: { key: bytes(key), consistency, snapshot: Option.none() },
          })
          .pipe(
            Effect.map((response) =>
              Option.map(response.row, (row) =>
                row.value._tag === "Value"
                  ? new TextDecoder().decode(row.value.bytes)
                  : `<${row.value._tag}>`,
              ),
            ),
          );
      // `a` was deleted (flushed by the alarm); `log` holds flushed merges "x" + "y".
      expect(yield* read("a", { _tag: "Flushed" })).toEqual(Option.none());
      expect(yield* read("log", { _tag: "Flushed" })).toEqual(Option.some("xy"));
      const unflushed = yield* client.databases.write({
        params: { databaseId },
        payload: {
          operations: [
            { _tag: "Merge", key: bytes("log"), operand: bytes("z"), ttlMillis: Option.none() },
            { _tag: "Put", key: bytes("b"), value: bytes("2"), ttlMillis: Option.none() },
          ],
        },
      });
      expect(yield* read("log", { _tag: "Latest" })).toEqual(Option.some("xyz"));
      expect(yield* read("b", { _tag: "AtLeast", seq: unflushed.seq })).toEqual(Option.some("2"));

      const snapshot = yield* client.databases.snapshot({ params: { databaseId } });
      const page = yield* client.databases.scan({
        params: { databaseId },
        payload: {
          range: FULL_KEY_RANGE,
          consistency: { _tag: "Latest" },
          snapshot: Option.none(),
          limit: 1,
        },
      });
      expect(page.rows.map((row) => new TextDecoder().decode(row.key))).toEqual(["b"]);
      expect(Option.map(page.resumeFrom, (key) => new TextDecoder().decode(key))).toEqual(
        Option.some("log"),
      );
      // A key written and flushed after the snapshot is invisible at the snapshot.
      yield* client.databases.write({
        params: { databaseId },
        payload: {
          operations: [
            { _tag: "Put", key: bytes("c"), value: bytes("3"), ttlMillis: Option.none() },
          ],
        },
      });
      yield* client.databases.flush({ params: { databaseId } });
      const atSnapshot = yield* client.databases.scan({
        params: { databaseId },
        payload: {
          range: FULL_KEY_RANGE,
          consistency: { _tag: "Flushed" },
          snapshot: Option.some(snapshot),
          limit: 100,
        },
      });
      const snapshotKeys = atSnapshot.rows.map((row) => new TextDecoder().decode(row.key));
      expect(snapshotKeys).toContain("log");
      expect(snapshotKeys).not.toContain("c");
      expect(yield* read("c", { _tag: "Latest" })).toEqual(Option.some("3"));

      // Compaction: four flushes wake the compactor (CompactorWorker), which runs a Workflow.
      const compacted = DatabaseId.make(`compaction-${Date.now().toString(36)}`);
      yield* client.databases.createDatabase({
        payload: {
          databaseId: compacted,
          keyRange: Option.none(),
          mergeOperator: Option.some("Concat"),
          changelogRetentionMs: Option.none(),
          budgetGroup: Option.none(),
        },
      });
      for (const round of [0, 1, 2, 3]) {
        yield* client.databases.write({
          params: { databaseId: compacted },
          payload: {
            operations: [
              {
                _tag: "Put",
                key: bytes(`k${round}`),
                value: bytes(`v${round}`),
                ttlMillis: Option.none(),
              },
              {
                _tag: "Merge",
                key: bytes("log"),
                operand: bytes(`${round}`),
                ttlMillis: Option.none(),
              },
            ],
          },
        });
        yield* client.databases.flush({ params: { databaseId: compacted } });
      }
      const settled = yield* client.databases
        .compaction({ params: { databaseId: compacted } })
        .pipe(
          Effect.repeat({
            schedule: Schedule.spaced("500 millis"),
            until: (status) =>
              status.l0Ssts === 0 &&
              status.sortedRuns.length === 1 &&
              Option.exists(status.compactor, (compactor) => compactor.running.length === 0),
            times: 120,
          }),
        );
      expect(settled.l0Ssts).toBe(0);
      expect(Option.map(settled.compactor, (compactor) => compactor.compactorEpoch)).toEqual(
        Option.some(1),
      );
      const readCompacted = (key: string, consistency: ReadConsistency) =>
        client.databases
          .get({
            params: { databaseId: compacted },
            payload: { key: bytes(key), consistency, snapshot: Option.none() },
          })
          .pipe(
            Effect.map((response) =>
              Option.map(response.row, (row) =>
                row.value._tag === "Value"
                  ? new TextDecoder().decode(row.value.bytes)
                  : `<${row.value._tag}>`,
              ),
            ),
          );
      yield* Effect.sleep("1100 millis");
      for (const consistency of [{ _tag: "Latest" } as const, { _tag: "Flushed" } as const]) {
        expect(yield* readCompacted("k2", consistency)).toEqual(Option.some("v2"));
        expect(yield* readCompacted("log", consistency)).toEqual(Option.some("0123"));
      }

      // GC (on demand through the GC Worker) removes the replaced L0 SSTs; reads are unaffected.
      const gc = yield* HttpApiClient.makeWith(GcHttpApi, {
        baseUrl: workerUrl(deployed.gcUrl),
        httpClient: yield* HttpClient.HttpClient,
      });
      const report = yield* gc.gc.collect({ params: { databaseId: compacted } });
      expect(report.deletedSsts).toBe(4);
      // The compaction ran as a Workflow in the compactor Worker and committed once.
      const spanNames = new Set(collector.spans.map((span) => span.name));
      for (const name of [
        "Compactor.poll",
        "CompactionWorkflow.run",
        "CompactionExecutor.commit",
        "GarbageCollector.collect",
      ]) {
        expect(spanNames.has(name)).toBe(true);
      }
      for (const consistency of [{ _tag: "Latest" } as const, { _tag: "Flushed" } as const]) {
        expect(yield* readCompacted("k0", consistency)).toEqual(Option.some("v0"));
        expect(yield* readCompacted("log", consistency)).toEqual(Option.some("0123"));
      }

      // Split: seal, one checkpoint, a fork per range. Each fork's compactor rewrites the source's
      // SSTs and releases its hold; then the source can be removed.
      const source = DatabaseId.make(`split-${Date.now().toString(36)}`);
      const left = DatabaseId.make(`${source}-a`);
      const right = DatabaseId.make(`${source}-b`);
      yield* client.databases.createDatabase({
        payload: {
          databaseId: source,
          keyRange: Option.none(),
          mergeOperator: Option.none(),
          changelogRetentionMs: Option.none(),
          budgetGroup: Option.none(),
        },
      });
      yield* client.databases.write({
        params: { databaseId: source },
        payload: {
          operations: ["a", "b", "c", "d", "e", "f"].map((key) => ({
            _tag: "Put" as const,
            key: bytes(key),
            value: bytes(key.toUpperCase()),
            ttlMillis: Option.none(),
          })),
        },
      });
      yield* client.databases.seal({
        params: { databaseId: source },
        payload: { successors: [left, right] },
      });
      const late = yield* Effect.flip(
        client.databases.write({
          params: { databaseId: source },
          payload: {
            operations: [
              { _tag: "Put", key: bytes("g"), value: bytes("G"), ttlMillis: Option.none() },
            ],
          },
        }),
      );
      expect(late._tag).toBe("DatabaseSealedPublicError");
      const checkpoint = yield* client.databases.createCheckpoint({
        params: { databaseId: source },
        payload: { name: Option.some("split"), lifetimeMs: Option.none() },
      });
      const split = bytes("d");
      for (const [forkId, range] of [
        [left, { start: new Uint8Array(0), end: Option.some(split) }],
        [right, { start: split, end: Option.none() }],
      ] as const) {
        yield* client.databases.fork({
          params: { databaseId: source },
          payload: { forkId, checkpointId: checkpoint.id, range },
        });
      }
      const readFork = (databaseId: DatabaseId, key: string) =>
        client.databases
          .get({
            params: { databaseId },
            payload: { key: bytes(key), consistency: { _tag: "Latest" }, snapshot: Option.none() },
          })
          .pipe(Effect.map((response) => Option.isSome(response.row)));
      for (const key of ["a", "b", "c", "d", "e", "f"]) {
        const inLeft = yield* readFork(left, key);
        const inRight = yield* readFork(right, key);
        // Exactly one successor holds each acknowledged write.
        expect([key, inLeft !== inRight, inLeft]).toEqual([key, true, key < "d"]);
      }
      const blocked = yield* Effect.flip(
        client.databases.removeDatabase({ params: { databaseId: source } }),
      );
      expect(blocked._tag).toBe("DatabaseHasDependentsPublicError");
      yield* client.databases.deleteCheckpoint({
        params: { databaseId: source, checkpointId: checkpoint.id },
      });
      // The forks' compactors (woken by forkRange) run their rewrite Workflows and release.
      yield* client.databases.removeDatabase({ params: { databaseId: source } }).pipe(
        Effect.retry({
          while: (error) => error._tag === "DatabaseHasDependentsPublicError",
          schedule: Schedule.spaced("500 millis"),
          times: 120,
        }),
      );
      const removed = yield* Effect.flip(
        client.databases.compaction({ params: { databaseId: source } }),
      );
      expect(removed._tag).toBe("DatabaseNotFoundPublicError");
      expect(yield* readFork(left, "b")).toBe(true);
      expect(yield* readFork(right, "e")).toBe(true);
      yield* stack.destroy();
    }),
  { timeout: 300_000 },
);
