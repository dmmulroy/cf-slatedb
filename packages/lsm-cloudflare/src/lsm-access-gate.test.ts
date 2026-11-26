import { Effect, Option, Redacted, Ref } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { lsmAccessGate, lsmAccessGateOver, type LsmAccessGate } from "./lsm-access-gate.ts";
import { lsmTestRuntimeContextLayer } from "./lsm-test-runtime.ts";

const handler = Effect.succeed(HttpServerResponse.text("served"));

/** The status a gate answers a request with `authorization` with. */
const statusThrough = (gate: LsmAccessGate, authorization: Option.Option<string>) =>
  gate(handler).pipe(
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
    Effect.provide(lsmTestRuntimeContextLayer),
  );

const statusFor = (token: Option.Option<string>, authorization: Option.Option<string>) =>
  Effect.runPromise(statusThrough(lsmAccessGate(Option.map(token, Redacted.make)), authorization));

const bearer = (token: string) => Option.some(`Bearer ${token}`);

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

describe("lsmAccessGateOver (D64)", () => {
  it("accepts either rotating slot or the fixed token, and sees a rotation after the refresh", async () => {
    const result = await Effect.gen(function* () {
      const slotA = yield* Ref.make("a".repeat(64));
      const slotB = yield* Ref.make("b".repeat(64));
      const reads = yield* Ref.make(0);
      const reader = (slot: Ref.Ref<string>) =>
        Ref.update(reads, (count) => count + 1).pipe(
          Effect.andThen(Ref.get(slot)),
          Effect.map(Redacted.make),
        );
      const gate = lsmAccessGateOver(
        Option.some(Redacted.make("f".repeat(64))),
        [reader(slotA), reader(slotB)],
        30_000,
      );
      const before = [
        yield* statusThrough(gate, bearer("a".repeat(64))),
        yield* statusThrough(gate, bearer("b".repeat(64))),
        yield* statusThrough(gate, bearer("f".repeat(64))),
        yield* statusThrough(gate, bearer("c".repeat(64))),
      ];
      const readsBefore = yield* Ref.get(reads);
      // Rotate slot A. Until the refresh the old value is still accepted; after it, only the new.
      yield* Ref.set(slotA, "n".repeat(64));
      const beforeRefresh = [
        yield* statusThrough(gate, bearer("a".repeat(64))),
        yield* statusThrough(gate, bearer("n".repeat(64))),
      ];
      yield* TestClock.adjust("30 seconds");
      const afterRefresh = [
        yield* statusThrough(gate, bearer("a".repeat(64))),
        yield* statusThrough(gate, bearer("n".repeat(64))),
        yield* statusThrough(gate, bearer("b".repeat(64))),
      ];
      return { before, readsBefore, beforeRefresh, afterRefresh };
    }).pipe(Effect.provide(TestClock.layer()), Effect.runPromise);
    expect(result.before).toEqual([200, 200, 200, 401]);
    // Read once for the first request, not again within the refresh interval.
    expect(result.readsBefore).toBe(2);
    expect(result.beforeRefresh).toEqual([200, 401]);
    expect(result.afterRefresh).toEqual([401, 200, 200]);
  });

  it("stays closed when no rotating token can be read, and keeps a value read before", async () => {
    const result = await Effect.gen(function* () {
      const available = yield* Ref.make(false);
      const reader = Ref.get(available).pipe(
        Effect.flatMap((ok) =>
          ok
            ? Effect.succeed(Redacted.make("a".repeat(64)))
            : Effect.fail({ message: "secret unavailable" }),
        ),
      );
      const gate = lsmAccessGateOver(Option.none(), [reader], 30_000);
      const unreadable = yield* statusThrough(gate, bearer("a".repeat(64)));
      yield* TestClock.adjust("30 seconds");
      yield* Ref.set(available, true);
      const readable = yield* statusThrough(gate, bearer("a".repeat(64)));
      yield* Ref.set(available, false);
      yield* TestClock.adjust("30 seconds");
      const keptAfterFailedRead = yield* statusThrough(gate, bearer("a".repeat(64)));
      return [unreadable, readable, keptAfterFailedRead];
    }).pipe(Effect.provide(TestClock.layer()), Effect.runPromise);
    expect(result).toEqual([401, 200, 200]);
  });
});

describe("the 401 answer", () => {
  /** The body a gate answers a request without a token with. */
  const refusedBody = (gate: LsmAccessGate) =>
    gate(handler).pipe(
      Effect.provideService(
        HttpServerRequest.HttpServerRequest,
        HttpServerRequest.fromWeb(new Request("http://api.internal/v1/databases")),
      ),
      Effect.scoped,
      Effect.flatMap((response) => Effect.promise(() => HttpServerResponse.toWeb(response).json())),
      Effect.provide(lsmTestRuntimeContextLayer),
      Effect.runPromise,
    );

  it("names the token to send without a command that works only on previews", async () => {
    const accessTokens = await refusedBody(
      lsmAccessGateOver(Option.some(Redacted.make("a".repeat(64))), [], 30_000),
    );
    expect(accessTokens).toMatchObject({ code: "Unauthenticated", retryable: false });
    expect(JSON.stringify(accessTokens)).toContain("access tokens");
    expect(JSON.stringify(accessTokens)).not.toContain("preview-cli");
    expect(JSON.stringify(accessTokens)).not.toContain("a".repeat(64));
    const collector = await refusedBody(
      lsmAccessGate(
        Option.some(Redacted.make("c".repeat(64))),
        "the stage's trace collector token",
      ),
    );
    expect(JSON.stringify(collector)).toContain("the stage's trace collector token");
  });
});
