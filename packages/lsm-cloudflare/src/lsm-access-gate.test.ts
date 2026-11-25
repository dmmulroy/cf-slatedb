import { Effect, Option, Redacted } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";
import { describe, expect, it } from "vite-plus/test";

import { lsmAccessGate } from "./lsm-access-gate.ts";

const handler = Effect.succeed(HttpServerResponse.text("served"));

const statusFor = (token: Option.Option<string>, authorization: Option.Option<string>) =>
  lsmAccessGate(Option.map(token, Redacted.make))(handler).pipe(
    Effect.provideService(
      HttpServerRequest.HttpServerRequest,
      HttpServerRequest.fromWeb(
        new Request("http://api.internal/v1/databases", {
          headers: Option.match(authorization, {
            onNone: () => ({}),
            onSome: (value) => ({ authorization: value }),
          }),
        }),
      ),
    ),
    Effect.scoped,
    Effect.map((response) => response.status),
    Effect.runPromise,
  );

describe("lsmAccessGate", () => {
  it("passes every request through when the stage has no token (local stages)", async () => {
    expect(await statusFor(Option.none(), Option.none())).toBe(200);
  });

  it("serves only requests that present the stage's token as a bearer token", async () => {
    const token = Option.some("a".repeat(64));
    expect(await statusFor(token, Option.some(`Bearer ${"a".repeat(64)}`))).toBe(200);
    expect(await statusFor(token, Option.some(`bearer ${"a".repeat(64)}`))).toBe(200);
    expect(await statusFor(token, Option.none())).toBe(401);
    expect(await statusFor(token, Option.some(`Bearer ${"a".repeat(63)}b`))).toBe(401);
    expect(await statusFor(token, Option.some(`Bearer ${"a".repeat(63)}`))).toBe(401);
    expect(await statusFor(token, Option.some(`Bearer ${"a".repeat(65)}`))).toBe(401);
    expect(await statusFor(token, Option.some("a".repeat(64)))).toBe(401);
  });
});
