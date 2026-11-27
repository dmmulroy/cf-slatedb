import { Effect, Layer, Schema } from "effect";
import { HttpRouter } from "effect/http";
import { HttpApi, HttpApiBuilder, HttpApiEndpoint, HttpApiGroup } from "effect/http-api";
import { describe, expect, it } from "vite-plus/test";

import { publicHttpRequestMiddlewareLayer } from "./db-http-request.ts";
import { lsmHttpServerLayer } from "./lsm-http-server.ts";
import { lsmTestRuntimeContextLayer, serveLsmTestRequest } from "./lsm-test-runtime.ts";

/**
 * The shared public request middleware (D69, D79) over a small API of its own, whose handlers fail
 * the ways no shaledb handler should: a defect, and a response that does not match its own schema.
 * Both are the server's fault, so the caller gets `InternalFailure` (500, not retryable), never
 * `InvalidRequest` (mutation sweep, D128).
 */
class ProbeGroup extends HttpApiGroup.make("probe")
  .add(
    HttpApiEndpoint.post("defect", "/defect", { success: Schema.Struct({ ok: Schema.Boolean }) }),
  )
  .add(
    HttpApiEndpoint.post("badResponse", "/bad-response", {
      success: Schema.Struct({ count: Schema.Int }),
    }),
  )
  .add(
    HttpApiEndpoint.post("echo", "/echo", {
      payload: Schema.Struct({ count: Schema.Int }),
      success: Schema.Struct({ count: Schema.Int }),
    }),
  ) {}

class ProbeApi extends HttpApi.make("ProbeApi").add(ProbeGroup) {}

const probeHandlers = HttpApiBuilder.group(ProbeApi, "probe", (handlers) =>
  Effect.succeed(
    handlers
      .handle("defect", () => Effect.die(new Error("a handler bug")))
      .handle("badResponse", () => Effect.succeed({ count: 1.5 }))
      .handle("echo", ({ payload }) => Effect.succeed(payload)),
  ),
);

const probeRouter = HttpApiBuilder.layer(ProbeApi).pipe(
  Layer.provide(probeHandlers),
  Layer.provide(publicHttpRequestMiddlewareLayer({ contract: "the probe API" })),
  Layer.provide(lsmHttpServerLayer),
);

const call = (path: string, body: string) =>
  Effect.gen(function* () {
    const app = yield* HttpRouter.toHttpEffect(probeRouter);
    const web = yield* serveLsmTestRequest(
      app,
      new Request(`http://api.internal${path}`, {
        method: "POST",
        body,
        headers: { "content-type": "application/json" },
      }),
    );
    return { status: web.status, body: JSON.parse(web.text) };
  }).pipe(Effect.scoped, Effect.provide(lsmTestRuntimeContextLayer), Effect.runPromise);

describe("the public request middleware's answers to the server's own failures (D128)", () => {
  it("answers a handler defect with InternalFailure (500, not retryable)", async () => {
    const answer = await call("/defect", "{}");
    expect(answer.status).toBe(500);
    expect(answer.body).toMatchObject({
      code: "InternalFailure",
      operation: "routeRequest",
      retryable: false,
    });
    expect(JSON.stringify(answer.body)).not.toContain("a handler bug");
  });

  it("answers a response that fails its own schema with InternalFailure, not InvalidRequest", async () => {
    const answer = await call("/bad-response", "{}");
    expect(answer.status).toBe(500);
    expect(answer.body).toMatchObject({ code: "InternalFailure", retryable: false });
  });

  it("repeats the schema's whole issue to a caller whose body does not match", async () => {
    const answer = await call("/echo", '{"count":"x"}');
    expect(answer.status).toBe(400);
    const { message } = Schema.decodeUnknownSync(Schema.Struct({ message: Schema.String }))(
      answer.body,
    );
    const issue = /did not match what the endpoint accepts: (.*)\. Check the request/.exec(
      message,
    )?.[1];
    // The issue is passed on whole when it fits the 300-character cap.
    expect(issue).toBe('Expected number at ["count"]');
  });
});
