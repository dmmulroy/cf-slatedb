import * as Cloudflare from "@alchemy.run/cloudflare";
import { DatabaseId, FULL_KEY_RANGE, type ReadConsistency } from "@cf-slatedb/lsm-core";
import * as Test from "@alchemy.run/test/Vitest";
import { expect } from "@effect/vitest";
import { Effect, Option, Schedule } from "effect";
import { HttpClient } from "effect/http";
import { HttpApiClient } from "effect/http-api";

import { DbHttpApi } from "../../src/db-http-api.ts";
import { workerUrl } from "../telemetry/delayed-otlp-collector.ts";
import { writerTestStack } from "./writer-stack.ts";

// Bound into the Worker at deploy time by the settings Config; flush after two seconds of age.
process.env["CF_SLATEDB_LSM_SETTINGS"] = JSON.stringify({ maxL0Age: 2000 });

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
      yield* stack.destroy();
    }),
  { timeout: 180_000 },
);
