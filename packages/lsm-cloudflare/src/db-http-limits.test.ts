import { DEFAULT_DATABASE_OPTIONS, MAX_TTL_MILLIS, WriteBatch } from "@shaledb/lsm-core";
import { Effect, Exit, Option, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { DatabaseLifecycle } from "./database-lifecycle.ts";
import { DbClient } from "./db-client.ts";
import { CreateCheckpointRequest, CreateDatabaseRequest } from "./db-http-api.ts";
import { dbServicesTestLayer } from "./db-services.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";
import { WriterClient } from "./writer-client.ts";

/**
 * The limits a caller can reach (D88). Each request body is decoded by the endpoint's own public
 * schema, as the router decodes it, then given to the service the handler gives it to. A value the
 * schema accepts must work; a value it refuses is answered `InvalidRequest` (400) before anything is
 * stored.
 */
const decodeBatch = Schema.decodeUnknownExit(WriteBatch);
const decodeCheckpoint = Schema.decodeUnknownExit(CreateCheckpointRequest);
const decodeCreate = Schema.decodeUnknownSync(CreateDatabaseRequest);

const bytes = (n: number) => Uint8Array.of(n);
const read = (consistency: "Latest" | "Flushed") => ({
  consistency:
    consistency === "Latest" ? { _tag: "Latest" as const } : { _tag: "Flushed" as const },
  snapshot: Option.none(),
  refuseSealed: false,
});

const createDatabase = (databaseId: string) =>
  Effect.gen(function* () {
    const request = decodeCreate({
      databaseId,
      keyRange: null,
      mergeOperator: null,
      changelogRetentionMs: null,
      budgetGroup: null,
    });
    yield* (yield* DatabaseLifecycle).create(request.databaseId, {
      keyRange: request.keyRange,
      databaseOptions: DEFAULT_DATABASE_OPTIONS,
    });
    return request.databaseId;
  });

const putWithTtl = (ttlMillis: number) => ({
  operations: [{ _tag: "Put", key: "AQ==", value: "Ag==", ttlMillis }],
});

describe("TTLs at the limits a caller can send (D88)", () => {
  it("refuses a TTL past 100 years at the schema, before anything is stored", () => {
    expect(Exit.isSuccess(decodeBatch(putWithTtl(MAX_TTL_MILLIS)))).toBe(true);
    expect(Exit.isFailure(decodeBatch(putWithTtl(MAX_TTL_MILLIS + 1)))).toBe(true);
    expect(Exit.isFailure(decodeBatch(putWithTtl(Number.MAX_SAFE_INTEGER)))).toBe(true);
    expect(Exit.isFailure(decodeBatch(putWithTtl(0)))).toBe(true);
    const merge = {
      operations: [{ _tag: "Merge", key: "AQ==", operand: "Ag==", ttlMillis: 2 ** 60 }],
    };
    expect(Exit.isFailure(decodeBatch(merge))).toBe(true);
  });

  it("a put with the largest TTL the schema accepts is readable and flushes, and the database stays usable", async () => {
    const batch = decodeBatch(putWithTtl(MAX_TTL_MILLIS));
    if (Exit.isFailure(batch)) throw new Error("the largest TTL must decode");
    const outcome = await Effect.gen(function* () {
      const id = yield* createDatabase("ttl");
      const writer = yield* WriterClient;
      const client = yield* DbClient;
      yield* writer.write(id, batch.value);
      const latest = yield* Effect.exit(client.get(id, bytes(1), read("Latest")));
      const flush = yield* Effect.exit(writer.flush(id));
      const next = yield* Effect.exit(
        writer.write(id, {
          operations: [{ _tag: "Put", key: bytes(3), value: bytes(4), ttlMillis: Option.none() }],
        }),
      );
      const flushAgain = yield* Effect.exit(writer.flush(id));
      return { latest, flush, next, flushAgain };
    }).pipe(
      Effect.scoped,
      Effect.provide(dbServicesTestLayer()),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );
    expect(outcome.latest).toEqual(Exit.succeed(Option.some(bytes(2))));
    expect(Exit.isSuccess(outcome.flush), String(outcome.flush)).toBe(true);
    expect(Exit.isSuccess(outcome.next), String(outcome.next)).toBe(true);
    expect(Exit.isSuccess(outcome.flushAgain), String(outcome.flushAgain)).toBe(true);
  });
});

describe("checkpoint lifetimes at the limits (D88)", () => {
  it("refuses a lifetime past 100 years, and the largest accepted one makes a checkpoint", async () => {
    expect(Exit.isFailure(decodeCheckpoint({ name: null, lifetimeMs: MAX_TTL_MILLIS + 1 }))).toBe(
      true,
    );
    const request = decodeCheckpoint({ name: null, lifetimeMs: MAX_TTL_MILLIS });
    if (Exit.isFailure(request)) throw new Error("the largest lifetime must decode");
    const checkpoint = await Effect.gen(function* () {
      const id = yield* createDatabase("cp");
      return yield* Effect.exit((yield* DatabaseLifecycle).createCheckpoint(id, request.value));
    }).pipe(
      Effect.scoped,
      Effect.provide(dbServicesTestLayer()),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );
    expect(Exit.isSuccess(checkpoint), String(checkpoint)).toBe(true);
  });
});
