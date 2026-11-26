import { DatabaseId, Timestamp } from "@shaledb/lsm-core";
import { Effect, Layer, Option } from "effect";
import { HttpRouter } from "effect/http";
import { describe, expect, it } from "vite-plus/test";

import { CompactorClient } from "./compactor-client.ts";
import { CompactorUnavailableError } from "./compactor.ts";
import { GarbageCollectorUnavailableError } from "./garbage-collector.ts";
import { gcHttpRouterLayer } from "./gc-worker.ts";
import { DatabaseIdMismatchError, DatabaseNotFoundError } from "./lsm-errors.ts";
import { lsmTestRuntimeContextLayer, serveLsmTestRequest } from "./lsm-test-runtime.ts";
import { MaintenanceSweep } from "./maintenance-sweep.ts";

/** A storage path and storage error text an internal message may carry; no answer may show it. */
const INTERNAL_DETAIL = "db/orders/manifest/00000000000000000042: R2 internal error 10001";
const SECRET_PARTS = ["db/orders/manifest", "R2", "10001"];

const notCalled = () => Effect.die(new Error("not called by the GC API"));

/**
 * A compactor client whose `collect` answers by database id: `ok` collects; `missing`, `down`,
 * `gc-down`, and `mismatch` fail with internal errors that carry {@link INTERNAL_DETAIL}; `defect`
 * dies with it.
 */
const compactorsLayer = Layer.succeed(
  CompactorClient,
  CompactorClient.of({
    wake: notCalled,
    destroy: notCalled,
    status: notCalled,
    requestCollection: notCalled,
    dropSst: notCalled,
    collect: (databaseId) => {
      switch (databaseId) {
        case "ok":
          return Effect.succeed({
            databaseId,
            manifestId: Option.none(),
            sstBoundary: Timestamp.make(0),
            deletedSsts: 2,
            deletedManifests: 1,
            nextCollectionAt: Option.none(),
          });
        case "missing":
          return Effect.fail(new DatabaseNotFoundError({ databaseId, message: INTERNAL_DETAIL }));
        case "down":
          return Effect.fail(
            new CompactorUnavailableError({ databaseId, message: INTERNAL_DETAIL }),
          );
        case "gc-down":
          return Effect.fail(
            new GarbageCollectorUnavailableError({ databaseId, message: INTERNAL_DETAIL }),
          );
        case "mismatch":
          return Effect.fail(
            new DatabaseIdMismatchError({
              databaseId,
              servedDatabaseId: DatabaseId.make("other"),
              message: INTERNAL_DETAIL,
            }),
          );
        default:
          return Effect.die(new Error(INTERNAL_DETAIL));
      }
    },
  }),
);

/** A sweep whose every page fails as storage might, carrying {@link INTERNAL_DETAIL}. */
const failingSweepLayer = Layer.succeed(
  MaintenanceSweep,
  MaintenanceSweep.of({
    sweepPage: () =>
      Effect.fail(
        new GarbageCollectorUnavailableError({ databaseId: "*", message: INTERNAL_DETAIL }),
      ),
  }),
);

interface Answer {
  readonly status: number;
  readonly requestId: string | null;
  readonly text: string;
  readonly body: unknown;
}

/** Sends one request through the GC API's router in-process, as its Worker serves it. */
const call = (method: string, path: string) =>
  Effect.gen(function* () {
    const app = yield* HttpRouter.toHttpEffect(gcHttpRouterLayer);
    const web = yield* serveLsmTestRequest(
      app,
      new Request(`http://gc.internal${path}`, { method }),
    );
    const answer: Answer = {
      status: web.status,
      requestId: web.headers.get("x-request-id"),
      text: web.text,
      body: web.text === "" ? "" : JSON.parse(web.text),
    };
    return answer;
  }).pipe(
    Effect.scoped,
    Effect.provide([compactorsLayer, failingSweepLayer]),
    Effect.provide(lsmTestRuntimeContextLayer),
    Effect.runPromise,
  );

const expectNoInternalDetail = (answer: Answer) => {
  for (const part of SECRET_PARTS) expect(answer.text, answer.text).not.toContain(part);
};

describe("the GC operator API's answers (D79)", () => {
  it("collects, and answers every failed collection with its declared error and a message of its own", async () => {
    const ok = await call("POST", "/v1/gc/databases/ok");
    expect(ok.status).toBe(200);
    expect(ok.body).toMatchObject({ databaseId: "ok", deletedSsts: 2 });
    expect(ok.requestId).not.toBeNull();

    const missing = await call("POST", "/v1/gc/databases/missing");
    expect(missing.status).toBe(404);
    expect(missing.body).toEqual({
      _tag: "DatabaseNotFoundError",
      databaseId: "missing",
      message: "Database missing does not exist. Check the id.",
    });
    for (const id of ["down", "gc-down"]) {
      const unavailable = await call("POST", `/v1/gc/databases/${id}`);
      expect(unavailable.status, id).toBe(503);
      expect(unavailable.body, id).toMatchObject({
        _tag: "GarbageCollectorUnavailableError",
        databaseId: id,
      });
      expect(unavailable.text, id).toContain("Retry the same request");
      expectNoInternalDetail(unavailable);
    }
    const mismatch = await call("POST", "/v1/gc/databases/mismatch");
    expect(mismatch.status).toBe(503);
    expect(mismatch.text).toContain(`request id ${mismatch.requestId}`);
    for (const answer of [missing, mismatch]) expectNoInternalDetail(answer);
  });

  it("answers a failed sweep page without the storage error", async () => {
    const sweep = await call("POST", "/v1/gc");
    expect(sweep.status).toBe(503);
    expect(sweep.body).toMatchObject({ _tag: "GarbageCollectorUnavailableError" });
    expectNoInternalDetail(sweep);
  });

  it("answers refused requests and defects with the public envelope", async () => {
    const refused = [
      ["POST", "/v1/gc/databases/NOT%20VALID!", 400, "InvalidRequest"],
      ["GET", "/v1/gc/databases/ok", 404, "RouteNotFound"],
      ["POST", "/v1/nope", 404, "RouteNotFound"],
      ["POST", "/v1/gc/databases/defect", 500, "InternalFailure"],
    ] as const;
    for (const [method, path, status, code] of refused) {
      const answer = await call(method, path);
      expect(answer.status, path).toBe(status);
      expect(answer.body, path).toMatchObject({
        _tag: `${code}PublicError`,
        code,
        operation: "routeRequest",
        requestId: answer.requestId,
        retryable: false,
      });
      expectNoInternalDetail(answer);
    }
  });
});
