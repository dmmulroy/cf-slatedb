import { type HttpEffect as AlchemyHttpEffect, safeHttpEffect } from "alchemy/Http";
import { RuntimeContext } from "alchemy/RuntimeContext";
import { Deferred, Effect, Layer } from "effect";
import { HttpEffect, HttpServerRequest, HttpServerResponse } from "effect/http";

/**
 * The smallest host `RuntimeContext` the in-memory test Layers accept: they need no Worker
 * environment or bindings. For tests only; production code always runs inside an Alchemy host.
 */
export const lsmTestRuntimeContextLayer = Layer.succeed(
  RuntimeContext,
  RuntimeContext.of({
    Type: "Test",
    id: "shaledb-test",
    env: {},
    get: () => Effect.succeed(undefined),
    set: (id) => Effect.succeed(id),
  }),
);

/** What a caller sees of one HTTP answer: its status, headers, and body text. */
export interface LsmTestHttpAnswer {
  readonly status: number;
  readonly headers: Headers;
  readonly text: string;
}

/**
 * Serves one request through a Worker's `fetch` handler in-process, as the Workers runtime does: a
 * failure becomes `causeResponse`'s answer (Alchemy's `safeHttpEffect`) and the pre-response
 * handlers run (`HttpEffect.toHandled`), so headers such as `x-request-id` are set. For tests of a
 * public API's answers without workerd.
 */
export const serveLsmTestRequest = <R>(handler: AlchemyHttpEffect<R>, request: Request) =>
  Effect.gen(function* () {
    const answered = yield* Deferred.make<Response>();
    yield* HttpEffect.toHandled(safeHttpEffect(handler), (_request, response) =>
      Deferred.succeed(answered, HttpServerResponse.toWeb(response)),
    ).pipe(
      Effect.provideService(
        HttpServerRequest.HttpServerRequest,
        HttpServerRequest.fromWeb(request),
      ),
    );
    const web = yield* Deferred.await(answered);
    const answer: LsmTestHttpAnswer = {
      status: web.status,
      headers: web.headers,
      text: yield* Effect.promise(() => web.text()),
    };
    return answer;
  });
