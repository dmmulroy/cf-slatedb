import {
  DatabaseId,
  DEFAULT_DATABASE_OPTIONS,
  MAX_BATCH_OPERATIONS,
  MAX_KEY_BYTES,
  MAX_TRANSACTION_READ_RANGES,
  MAX_VALUE_BYTES,
  RequestTransactionCommit,
  RequestWriteBatch,
} from "@shaledb/lsm-core";
import { Effect, Exit, Option, Schema } from "effect";
import { HttpRouter } from "effect/http";
import { describe, expect, it } from "vite-plus/test";

import { DatabaseLifecycle } from "./database-lifecycle.ts";
import { ScanRequest } from "./db-http-api.ts";
import { dbHttpRouterLayer } from "./db-http-handlers.ts";
import { dbServicesTestLayer } from "./db-services.ts";
import { lsmTestRuntimeContextLayer, serveLsmTestRequest } from "./lsm-test-runtime.ts";

/**
 * Random request bodies around every bound (D100): each is either accepted by the endpoint's
 * schema or refused by the API as `InvalidRequest` (400) naming the refused part. Never a 500.
 */
let seed = 0x5eed;
const random = () => {
  seed = (seed * 1103515245 + 12345) & 0x7fffffff;
  return seed / 0x7fffffff;
};
const pick = <A>(values: readonly [A, ...Array<A>]): A =>
  values[Math.floor(random() * values.length)] ?? values[0];
/** A length at, just under, or just over `bound`, or small. */
const around = (bound: number) => pick([0, 1, bound - 1, bound, bound + 1]);
const bytes = (length: number) => Buffer.alloc(Math.max(0, length), 7).toString("base64");

const operation = () =>
  pick([
    () => ({
      _tag: "Put",
      key: bytes(around(MAX_KEY_BYTES)),
      value: bytes(pick([1, 64])),
      ttlMillis: pick([null, 1, 0, -5, 1.5]),
    }),
    () => ({
      _tag: "Merge",
      key: bytes(4),
      operand: bytes(around(MAX_VALUE_BYTES)),
      ttlMillis: null,
    }),
    () => ({ _tag: "Delete", key: bytes(pick([0, 4])) }),
    () => ({ _tag: "Nope", key: bytes(4) }),
  ])();

const writeBody = () => ({
  operations: Array.from({ length: pick([0, 1, 3, MAX_BATCH_OPERATIONS + 1]) }, (_, index) =>
    index < 3 ? operation() : { _tag: "Delete", key: bytes(1) },
  ),
});
const range = () => ({
  start: bytes(around(MAX_KEY_BYTES)),
  end: pick([null, bytes(around(MAX_KEY_BYTES))]),
});
const scanBody = () => ({
  range: range(),
  consistency: pick([{ _tag: "Latest" }, { _tag: "Flushed" }, { _tag: "AtLeast", seq: -1 }]),
  snapshot: null,
  limit: pick([0, 1, 1000, 1001]),
});
const commitBody = () => ({
  readSeq: pick([0, 5, -1]),
  isolation: pick(["Snapshot", "SerializableSnapshot", "Nope"]),
  batch: writeBody(),
  readKeys: Array.from({ length: pick([0, 2]) }, () => bytes(around(MAX_KEY_BYTES))),
  readRanges: Array.from({ length: pick([0, 1, MAX_TRANSACTION_READ_RANGES + 1]) }, () => ({
    start: bytes(1),
    end: null,
  })),
});

const CASES = [
  { path: "/v1/databases/orders/batches", schema: RequestWriteBatch, body: writeBody },
  { path: "/v1/databases/orders/scan", schema: ScanRequest, body: scanBody },
  {
    path: "/v1/databases/orders/transactions/commit",
    schema: RequestTransactionCommit,
    body: commitBody,
  },
] as const;

describe("random bodies around every bound (D100)", () => {
  it("are accepted by the schema, or refused as InvalidRequest naming the body, never a 500", async () => {
    const refused = await Effect.gen(function* () {
      const app = yield* HttpRouter.toHttpEffect(dbHttpRouterLayer);
      yield* (yield* DatabaseLifecycle).create(DatabaseId.make("orders"), {
        keyRange: Option.none(),
        databaseOptions: DEFAULT_DATABASE_OPTIONS,
      });
      let count = 0;
      let ran = 0;
      for (let round = 0; round < 60; round++) {
        for (const { path, schema, body } of CASES) {
          const candidate = body();
          const decoded = Schema.decodeUnknownExit(schema)(candidate);
          if (Exit.isSuccess(decoded)) {
            // An accepted write runs through the API to the writer: it commits, or fails with a
            // typed 4xx answer, never a 5xx (the in-process writer starts as a Durable Object does,
            // D131).
            const batch = Schema.decodeUnknownExit(RequestWriteBatch)(candidate);
            if (path.endsWith("/batches") && Exit.isSuccess(batch)) {
              const written = yield* serveLsmTestRequest(
                app,
                new Request(`http://api.internal${path}`, {
                  method: "POST",
                  headers: { "content-type": "application/json" },
                  body: JSON.stringify(candidate),
                }),
              );
              expect(written.status, written.text.slice(0, 300)).toBeLessThan(500);
              ran += 1;
            }
            continue;
          }
          count += 1;
          const web = yield* serveLsmTestRequest(
            app,
            new Request(`http://api.internal${path}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(candidate),
            }),
          );
          expect(web.status, web.text.slice(0, 300)).toBe(400);
          const answer = JSON.parse(web.text);
          expect(answer.code).toBe("InvalidRequest");
          expect(answer.message).toContain("its body did not match");
        }
      }
      return { count, ran };
    }).pipe(
      Effect.scoped,
      Effect.provide(dbServicesTestLayer()),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );
    expect(refused.count).toBeGreaterThan(60);
    expect(refused.ran).toBeGreaterThan(5);
  }, 120_000);
});
