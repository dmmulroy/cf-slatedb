import { Cause, Context, Effect, Layer, Schema } from "effect";
import {
  HttpEffect,
  HttpRouter,
  type HttpServerError,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/http";

import {
  InternalFailurePublicError,
  InvalidRequestPublicError,
  RouteNotFoundPublicError,
} from "./db-public-error.ts";

/** What every public API handler knows about its request. */
export interface IDbHttpRequest {
  /** Identifier of this request: Cloudflare's ray id, or a random UUID when there is none. */
  readonly requestId: string;
}

/** Identifies the request being served, so error bodies carry its request id. */
export class DbHttpRequest extends Context.Service<DbHttpRequest, IDbHttpRequest>()(
  "@shaledb/DbHttpRequest",
) {}

/** Reads the request identity from the `cf-ray` header (or generates one). */
export const makeDbHttpRequest = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  return DbHttpRequest.of({ requestId: request.headers["cf-ray"] ?? crypto.randomUUID() });
});

/** Provides the request identity, built per request from the contextual `HttpServerRequest`. */
export const dbHttpRequestLayer = Layer.effect(DbHttpRequest, makeDbHttpRequest);

const encodeInvalidRequest = Schema.encodeSync(InvalidRequestPublicError);
const encodeRouteNotFound = Schema.encodeSync(RouteNotFoundPublicError);
const encodeInternalFailure = Schema.encodeSync(InternalFailurePublicError);

const noStore = { "cache-control": "no-store" };

/**
 * Global middleware: provides `DbHttpRequest`, returns the request id as `x-request-id`, answers an
 * unmatched route with `RouteNotFound` (404) and an unreadable request with `InvalidRequest` (400),
 * and answers any other router failure or defect with `InternalFailure` (500) after logging it, so no
 * internal detail reaches the caller.
 */
export const dbHttpRequestMiddlewareLayer = HttpRouter.middleware<{
  provides: DbHttpRequest;
  handles: HttpServerError.HttpServerError;
}>()(
  (httpEffect) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const { requestId } = yield* DbHttpRequest;
      const { pathname } = new URL(request.originalUrl, "http://api.internal");
      yield* HttpEffect.appendPreResponseHandler((_request, response) =>
        Effect.succeed(HttpServerResponse.setHeader(response, "x-request-id", requestId)),
      );
      const internalFailure = (description: string, cause: unknown) =>
        Effect.logError(description, { cause }).pipe(
          Effect.as(
            HttpServerResponse.jsonUnsafe(
              encodeInternalFailure(
                new InternalFailurePublicError({
                  code: "InternalFailure",
                  operation: "routeRequest",
                  message: `The request could not be completed because of an internal error. Contact the operator with request id ${requestId}.`,
                  details: {},
                  requestId,
                  retryable: false,
                }),
              ),
              { status: 500, headers: noStore },
            ),
          ),
        );
      return yield* httpEffect.pipe(
        Effect.catchTag("HttpServerError", (error) => {
          switch (error.reason._tag) {
            case "RouteNotFound":
              return Effect.succeed(
                HttpServerResponse.jsonUnsafe(
                  encodeRouteNotFound(
                    new RouteNotFoundPublicError({
                      code: "RouteNotFound",
                      operation: "routeRequest",
                      message: `No route matches ${request.method} ${pathname}. Check the method and path against the API's OpenAPI document.`,
                      details: { method: request.method, path: pathname },
                      requestId,
                      retryable: false,
                    }),
                  ),
                  { status: 404, headers: noStore },
                ),
              );
            case "RequestParseError":
              return Effect.succeed(
                HttpServerResponse.jsonUnsafe(
                  encodeInvalidRequest(
                    new InvalidRequestPublicError({
                      code: "InvalidRequest",
                      operation: "routeRequest",
                      message: `The request to ${request.method} ${pathname} could not be read because its path, query, or body is malformed. Fix the request and send it again.`,
                      details: { method: request.method, path: pathname },
                      requestId,
                      retryable: false,
                    }),
                  ),
                  { status: 400, headers: noStore },
                ),
              );
            case "InternalError":
            case "ResponseError":
              return internalFailure("shaledb API router failed", error);
          }
        }),
        Effect.catchCauseIf(Cause.hasDies, (cause) =>
          internalFailure("shaledb API request failed on a defect", Cause.pretty(cause)),
        ),
        Effect.annotateLogs({ requestId, path: pathname, method: request.method }),
      );
    }).pipe(Effect.provide(dbHttpRequestLayer)),
  { global: true },
);
