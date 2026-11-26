import { Effect } from "effect";
import { HttpRouter } from "effect/http";
import { describe, expect, it } from "vite-plus/test";

import { dbHttpRouterLayer } from "./db-http-handlers.ts";
import { dbServicesTestLayer } from "./db-services.ts";
import { lsmTestRuntimeContextLayer, serveLsmTestRequest } from "./lsm-test-runtime.ts";

/** One answer of the database API, as the caller sees it. */
interface Answer {
  readonly status: number;
  readonly requestId: string | null;
  readonly body: unknown;
}

/** Sends one request through the database API's router in-process, as the Worker serves it. */
const call = (method: string, path: string, body?: { text: string; contentType: string }) =>
  Effect.gen(function* () {
    const app = yield* HttpRouter.toHttpEffect(dbHttpRouterLayer);
    const init: RequestInit =
      body === undefined
        ? { method }
        : { method, body: body.text, headers: { "content-type": body.contentType } };
    const web = yield* serveLsmTestRequest(app, new Request(`http://api.internal${path}`, init));
    const answer: Answer = {
      status: web.status,
      requestId: web.headers.get("x-request-id"),
      body: web.text === "" ? "" : JSON.parse(web.text),
    };
    return answer;
  }).pipe(
    Effect.scoped,
    Effect.provide(dbServicesTestLayer()),
    Effect.provide(lsmTestRuntimeContextLayer),
  );

const json = (text: string) => ({ text, contentType: "application/json" });

/** The envelope a request the API refuses before any handler runs is answered with. */
const envelope = (answer: Answer, code: string, method: string, path: string) => ({
  _tag: `${code}PublicError`,
  code,
  operation: "routeRequest",
  details: { method, path },
  requestId: answer.requestId,
  retryable: false,
});

describe("the database API's answers to requests it refuses", () => {
  it("answers every malformed request with InvalidRequest (400) in the envelope", async () => {
    const path = "/v1/databases/orders/get";
    for (const [body, naming] of [
      [json("{"), "body"],
      [json('{"key":1}'), "body"],
      [json(""), "body"],
      [{ text: "k=v", contentType: "text/plain" }, "content type"],
    ] as const) {
      const answer = await Effect.runPromise(call("POST", path, body));
      expect(answer.status, body.text).toBe(400);
      expect(answer.requestId, body.text).not.toBeNull();
      expect(answer.body, body.text).toMatchObject(
        envelope(answer, "InvalidRequest", "POST", path),
      );
      expect(JSON.stringify(answer.body), body.text).toContain(naming);
    }
    const badId = await Effect.runPromise(call("GET", "/v1/databases/NOT%20VALID!/status"));
    expect(badId.status).toBe(400);
    expect(JSON.stringify(badId.body)).toContain("path parameters");
  });

  it("answers an unknown route and a wrong method with RouteNotFound (404) in the envelope", async () => {
    for (const [method, path] of [
      ["GET", "/v1/nope"],
      ["GET", "/v1/databases/orders/get"],
    ] as const) {
      const answer = await Effect.runPromise(call(method, path));
      expect(answer.status, path).toBe(404);
      expect(answer.body, path).toMatchObject(envelope(answer, "RouteNotFound", method, path));
    }
  });
});
