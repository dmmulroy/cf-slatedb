import { Effect, Exit, Fiber, Option } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { describe, expect, it } from "vite-plus/test";

import { smoke } from "./smoke.ts";

/**
 * A stub of the database API that records which databases exist. `create` and `remove` work;
 * the write `fails` (503) or `hangs`, so the smoke test stops right after its first create.
 */
const stubApi = (write: "fails" | "hangs", removal: "works" | "fails" = "works") => {
  const existing = new Set<string>();
  const removed: Array<string> = [];
  const client = HttpClient.make((request, url) =>
    Effect.gen(function* () {
      const respond = (status: number, body: unknown) =>
        HttpClientResponse.fromWeb(request, new Response(JSON.stringify(body), { status }));
      const notFound = (databaseId: string) =>
        respond(404, {
          _tag: "DatabaseNotFoundPublicError",
          code: "DatabaseNotFound",
          operation: "status",
          message: "gone",
          details: { databaseId },
          requestId: "r",
          retryable: false,
        });
      const parts = url.pathname.split("/");
      const databaseId = parts[3] ?? "";
      if (request.method === "POST" && url.pathname === "/v1/databases") {
        if (request.body._tag !== "Uint8Array") return respond(400, {});
        const id = JSON.parse(new TextDecoder().decode(request.body.body)).databaseId;
        existing.add(id);
        return respond(201, { databaseId: id, manifestId: 1 });
      }
      if (request.method === "DELETE") {
        removed.push(databaseId);
        if (removal === "fails") return respond(503, {});
        existing.delete(databaseId);
        return HttpClientResponse.fromWeb(request, new Response(null, { status: 204 }));
      }
      if (url.pathname.endsWith("/status")) {
        return existing.has(databaseId) ? respond(200, {}) : notFound(databaseId);
      }
      if (url.pathname.endsWith("/batches")) {
        if (write === "hangs") return yield* Effect.never;
        return respond(503, {});
      }
      return respond(500, {});
    }),
  );
  return { client, existing, removed };
};

const failedStep = (exit: Exit.Exit<unknown, { readonly step: string }>) =>
  Option.getOrElse(
    Option.map(Exit.findErrorOption(exit), (failure) => failure.step),
    () => "none",
  );

const run = (api: ReturnType<typeof stubApi>) =>
  smoke("http://api.internal").pipe(Effect.provideService(HttpClient.HttpClient, api.client));

describe("the smoke test leaves nothing behind (D104)", () => {
  it("removes what it created when a step fails, and fails naming that step", async () => {
    const api = stubApi("fails");
    const exit = await Effect.runPromiseExit(run(api));
    expect(Exit.isFailure(exit)).toBe(true);
    expect(failedStep(exit)).toBe("write");
    expect([...api.existing]).toEqual([]);
    expect(api.removed.length).toBeGreaterThan(0);
  });

  it("removes what it created when it is interrupted mid-run", async () => {
    const api = stubApi("hangs");
    await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* Effect.forkChild(run(api));
        yield* Effect.sleep("200 millis");
        yield* Fiber.interrupt(fiber);
      }),
    );
    expect([...api.existing]).toEqual([]);
  });

  it("tries to remove every database even when one removal fails, and fails", async () => {
    const api = stubApi("fails", "fails");
    const exit = await Effect.runPromiseExit(run(api));
    expect(Exit.isFailure(exit)).toBe(true);
    expect(failedStep(exit)).toMatch(/^remove /);
  });
});
