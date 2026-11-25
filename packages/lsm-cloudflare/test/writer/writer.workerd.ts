import * as Cloudflare from "@alchemy.run/cloudflare";
import { DatabaseId } from "@cf-slatedb/lsm-core";
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
      yield* stack.destroy();
    }),
  { timeout: 180_000 },
);
