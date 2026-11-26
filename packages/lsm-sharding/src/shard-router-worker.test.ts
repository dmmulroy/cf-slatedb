import { DatabaseId, DEFAULT_DATABASE_OPTIONS } from "@shaledb/lsm-core";
import {
  DatabaseLifecycle,
  dbServicesTestLayer,
  lsmTestRuntimeContextLayer,
  ReaderUnavailableError,
  serveLsmTestRequest,
  WriterUnavailableError,
} from "@shaledb/lsm-cloudflare";
import { Effect, Layer, Option, Stream } from "effect";
import { HttpRouter } from "effect/http";
import { describe, expect, it } from "vite-plus/test";

import { ShardDirectory, shardDirectoryTestLayer } from "./shard-directory.ts";
import { ShardGroup } from "./shard-map.ts";
import { ShardRouter, shardRouterLayerWithoutDependencies } from "./shard-router.ts";
import { shardedHttpRouterLayer } from "./shard-router-worker.ts";
import { shardSplitLayerWithoutDependencies } from "./shard-split.ts";

const GROUP = ShardGroup.make("orders");
const FIRST = DatabaseId.make("orders-0");
/** A storage path and storage error text an internal message may carry; no answer may show it. */
const INTERNAL_DETAIL = "db/orders-0/sst/01JD0000000000000000000AAA.sst: R2 internal error 10001";

const databasesLayer = dbServicesTestLayer({ blockSize: 64 });

/** The real router, split, and directory over an in-memory deployment. */
const realLayer = Layer.mergeAll(
  shardRouterLayerWithoutDependencies,
  shardSplitLayerWithoutDependencies,
).pipe(Layer.provideMerge(shardDirectoryTestLayer), Layer.provideMerge(databasesLayer));

/**
 * A router whose every operation fails as storage might: `put` with a storage failure, `get` with a
 * read failure (both carrying internal detail), and the rest with a defect.
 */
const failingRouterLayer = Layer.succeed(
  ShardRouter,
  ShardRouter.of({
    put: () =>
      Effect.fail(
        new WriterUnavailableError({
          databaseId: FIRST,
          operation: "write",
          reason: "StorageUnavailable",
          message: INTERNAL_DETAIL,
        }),
      ),
    get: () =>
      Effect.fail(
        new ReaderUnavailableError({
          databaseId: FIRST,
          reason: "StorageUnavailable",
          message: INTERNAL_DETAIL,
        }),
      ),
    delete: () => Effect.die(new Error(INTERNAL_DETAIL)),
    merge: () => Effect.die(new Error(INTERNAL_DETAIL)),
    write: () => Effect.die(new Error(INTERNAL_DETAIL)),
    scan: () => Stream.die(new Error(INTERNAL_DETAIL)),
    refresh: () => Effect.die(new Error(INTERNAL_DETAIL)),
  }),
);
const failingLayer = Layer.mergeAll(failingRouterLayer, shardSplitLayerWithoutDependencies).pipe(
  Layer.provideMerge(shardDirectoryTestLayer),
  Layer.provideMerge(databasesLayer),
);

/** One answer of the sharded API, as the caller sees it. */
interface Answer {
  readonly status: number;
  readonly requestId: string | null;
  readonly text: string;
  readonly body: unknown;
}

type Body = { readonly text: string; readonly contentType: string };

const json = (text: string): Body => ({ text, contentType: "application/json" });

/** Sends requests through the sharded API's router in-process, as its Worker serves them. */
const callAll = (
  services: typeof realLayer,
  requests: ReadonlyArray<readonly [method: string, path: string, body?: Body]>,
) =>
  Effect.gen(function* () {
    yield* (yield* DatabaseLifecycle).create(FIRST, {
      keyRange: Option.none(),
      databaseOptions: DEFAULT_DATABASE_OPTIONS,
    });
    yield* (yield* ShardDirectory).create(GROUP, FIRST);
    const app = yield* HttpRouter.toHttpEffect(shardedHttpRouterLayer);
    const answers: Array<Answer> = [];
    for (const [method, path, body] of requests) {
      const init: RequestInit =
        body === undefined
          ? { method }
          : { method, body: body.text, headers: { "content-type": body.contentType } };
      const web = yield* serveLsmTestRequest(
        app,
        new Request(`http://shards.internal${path}`, init),
      );
      answers.push({
        status: web.status,
        requestId: web.headers.get("x-request-id"),
        text: web.text,
        body: web.text === "" ? "" : JSON.parse(web.text),
      });
    }
    return answers;
  }).pipe(
    Effect.scoped,
    Effect.provide(services),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.runPromise,
  );

/** The envelope a request refused before any handler runs is answered with (D69). */
const envelope = (answer: Answer, code: string, method: string, path: string) => ({
  _tag: `${code}PublicError`,
  code,
  operation: "routeRequest",
  details: { method, path },
  requestId: answer.requestId,
  retryable: false,
});

describe("the sharded API's answers to requests it refuses (D79)", () => {
  it("answers a malformed request with InvalidRequest (400) in the public envelope", async () => {
    const put = "/v1/groups/orders/put";
    const cases = [
      [json("{"), "body"],
      [json('{"key":1}'), "body"],
      [json(""), "body"],
      [{ text: "k=v", contentType: "text/plain" }, "content type"],
    ] as const;
    const answers = await callAll(
      realLayer,
      cases.map(([body]) => ["POST", put, body] as const),
    );
    answers.forEach((answer, index) => {
      const [body, naming] = cases[index] ?? cases[0];
      expect(answer.status, body.text).toBe(400);
      expect(answer.requestId, body.text).not.toBeNull();
      expect(answer.body, body.text).toMatchObject(envelope(answer, "InvalidRequest", "POST", put));
      expect(answer.text, body.text).toContain(naming);
    });
    // Only the schema's issue is repeated; a body the caller sent is not echoed.
    expect(answers[1]?.text).toContain("ShardedHttpApi");
    const [badGroup] = await callAll(realLayer, [["GET", "/v1/groups/NOT%20VALID/map"]]);
    expect(badGroup?.status).toBe(400);
    expect(badGroup?.text).toContain("path parameters");
  });

  it("answers an unknown route and a wrong method with RouteNotFound (404) in the envelope", async () => {
    const requests = [
      ["GET", "/v1/nope"],
      ["GET", "/v1/groups/orders/put"],
    ] as const;
    const answers = await callAll(realLayer, requests);
    answers.forEach((answer, index) => {
      const [method, path] = requests[index] ?? requests[0];
      expect(answer.status, path).toBe(404);
      expect(answer.body, path).toMatchObject(envelope(answer, "RouteNotFound", method, path));
    });
  });

  it("keeps its declared errors and successes, now with a request id", async () => {
    const [missing, map] = await callAll(realLayer, [
      ["GET", "/v1/groups/missing/map"],
      ["GET", "/v1/groups/orders/map"],
    ]);
    expect(missing?.status).toBe(404);
    expect(missing?.body).toEqual({
      _tag: "ShardGroupNotFoundError",
      group: "missing",
      message: "Shard group missing does not exist. Create it with its first database.",
    });
    expect(missing?.requestId).not.toBeNull();
    expect(map?.status).toBe(200);
    expect(map?.body).toMatchObject({ group: "orders", version: 1 });
    expect(map?.requestId).not.toBeNull();
  });

  it("never shows a database failure's internal message, and answers a defect with InternalFailure", async () => {
    const key = json(`{"key":"AQ==","value":"Ag=="}`);
    const [put, get, map] = await callAll(failingLayer, [
      ["POST", "/v1/groups/orders/put", key],
      ["POST", "/v1/groups/orders/get", json(`{"key":"AQ=="}`)],
      ["GET", "/v1/groups/orders/map"],
    ]);
    for (const answer of [put, get, map]) expect(answer?.text).not.toContain("R2");
    expect(put?.status).toBe(503);
    expect(put?.body).toMatchObject({
      _tag: "ShardedRequestUnavailableError",
      reason: "WriterUnavailableError",
      message: "Database orders-0's storage was temporarily unavailable. Retry the same request.",
    });
    expect(get?.status).toBe(503);
    expect(get?.body).toMatchObject({
      message: "Database orders-0's data could not be read just now. Retry the same request.",
    });
    expect(map?.status).toBe(500);
    expect(map?.body).toMatchObject({
      _tag: "InternalFailurePublicError",
      code: "InternalFailure",
      requestId: map?.requestId,
      retryable: false,
    });
  });
});
