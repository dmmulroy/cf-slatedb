import { Effect } from "effect";
import { HttpRouter } from "effect/http";
import { describe, expect, it } from "vite-plus/test";

import { MAX_BATCH_BYTES, MAX_BATCH_OPERATIONS, MAX_TTL_MILLIS } from "@shaledb/lsm-core";

import { dbHttpRouterLayer } from "./db-http-handlers.ts";
import { MAX_REQUEST_BODY_BYTES } from "./db-http-request.ts";
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

describe("request bodies are bounded (D100)", () => {
  const path = "/v1/databases/orders/batches";
  const send = (init: RequestInit) =>
    Effect.gen(function* () {
      const app = yield* HttpRouter.toHttpEffect(dbHttpRouterLayer);
      const web = yield* serveLsmTestRequest(app, new Request(`http://api.internal${path}`, init));
      return { status: web.status, body: web.text === "" ? "" : JSON.parse(web.text) };
    }).pipe(
      Effect.scoped,
      Effect.provide(dbServicesTestLayer()),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );

  /** A chunked body of `chunks` chunks of 1 MiB, counting how many the server pulled. */
  const chunked = (chunks: number) => {
    const pulled = { count: 0 };
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (pulled.count >= chunks) return controller.close();
        pulled.count += 1;
        controller.enqueue(new Uint8Array(1024 * 1024).fill(0x20));
      },
    });
    return { stream, pulled };
  };

  it("refuses a declared length over 16 MiB before reading the body", async () => {
    const { stream, pulled } = chunked(64);
    const answer = await send({
      method: "POST",
      body: stream,
      headers: {
        "content-type": "application/json",
        "content-length": String(MAX_REQUEST_BODY_BYTES + 1),
      },
      // @ts-expect-error: Node's fetch needs this for a stream body; workerd ignores it.
      duplex: "half",
    });
    expect(answer.status).toBe(400);
    expect(answer.body).toMatchObject({ code: "InvalidRequest" });
    expect(JSON.stringify(answer.body)).toContain("16 MiB");
    expect(pulled.count).toBeLessThanOrEqual(1);
  });

  it("passes a declared body of exactly 16 MiB to its handler (mutation sweep, D128)", async () => {
    const batch = '{"operations":[{"_tag":"Put","key":"AQ==","value":"Ag==","ttlMillis":null}]}';
    const body = batch + " ".repeat(MAX_REQUEST_BODY_BYTES - batch.length);
    const answer = await send({
      method: "POST",
      body,
      headers: {
        "content-type": "application/json",
        "content-length": String(MAX_REQUEST_BODY_BYTES),
      },
    });
    // The write handler answered (this harness has no writer), so the bound let the body through.
    expect(answer.body).toMatchObject({ operation: "write" });
  });

  it("refuses a chunked body once it passes 16 MiB, without reading the rest", async () => {
    const { stream, pulled } = chunked(64);
    const answer = await send({
      method: "POST",
      body: stream,
      headers: { "content-type": "application/json" },
      // @ts-expect-error: Node's fetch needs this for a stream body; workerd ignores it.
      duplex: "half",
    });
    expect(answer.status).toBe(400);
    expect(JSON.stringify(answer.body)).toContain("16 MiB");
    // 17 chunks pass 16 MiB; a few more may be buffered by the stream, never all 64.
    expect(pulled.count).toBeLessThan(24);
  });

  it("fits the largest legitimate write batch with room to spare", () => {
    const rowBytes = Math.floor(MAX_BATCH_BYTES / MAX_BATCH_OPERATIONS);
    const batch = {
      operations: Array.from({ length: MAX_BATCH_OPERATIONS }, (_, index) => ({
        _tag: "Merge",
        key: Buffer.from(new Uint8Array(16).fill(index & 0xff)).toString("base64"),
        operand: Buffer.from(new Uint8Array(rowBytes - 16)).toString("base64"),
        ttlMillis: MAX_TTL_MILLIS,
      })),
    };
    const size = new TextEncoder().encode(JSON.stringify(batch)).length;
    expect(size).toBeGreaterThan(11 * 1024 * 1024);
    expect(size).toBeLessThan(MAX_REQUEST_BODY_BYTES * 0.75);
  });
});
