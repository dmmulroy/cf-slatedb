import { ByteSize, Cause, Context, Effect, Layer, Schema } from "effect";
import {
  HttpEffect,
  HttpIncomingMessage,
  HttpRouter,
  HttpServerError,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/http";
import { HttpApiError } from "effect/http-api";

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
 * Largest request body any public API reads, in bytes: 16 MiB (D100). The largest legitimate body
 * is a write batch at its limits (10,000 operations, 8 MiB of keys and values): about 11.4 MiB as
 * base64 JSON. A larger body is refused before it is held, so no request can make the API isolate
 * (128 MB, shared by every database it serves) hold an unbounded body.
 */
export const MAX_REQUEST_BODY_BYTES = 16 * 1024 * 1024;

/** The part of a request each kind of request decoding failure is about. */
const REQUEST_PART_BY_SCHEMA_ERROR_KIND = {
  Params: "path parameters",
  Headers: "headers",
  Query: "query string",
  Payload: "body",
} as const;

/** Longest schema issue repeated to the caller: enough for a path and what was expected. */
const MAX_REPORTED_ISSUE_LENGTH = 300;

/**
 * The request decoding failure in `cause`, if that is why the request died: the API builder raises
 * a request that does not match its endpoint's schemas as a defect (`HttpApiSchemaError`), and
 * its `Body` and `ResponseHeaders` kinds, which are our own responses failing to encode, are not
 * the caller's doing.
 *
 * @returns Which part of the request is malformed and the schema's issue on one line, or
 *   `undefined`.
 */
const malformedRequestPart = (cause: Cause.Cause<unknown>) => {
  for (const reason of cause.reasons) {
    if (!Cause.isDieReason(reason) || !HttpApiError.HttpApiSchemaError.is(reason.defect)) continue;
    const { kind, cause: issue } = reason.defect;
    if (kind === "Body" || kind === "ResponseHeaders") continue;
    return {
      part: REQUEST_PART_BY_SCHEMA_ERROR_KIND[kind],
      issue: issue.message.replace(/\s+/g, " ").trim().slice(0, MAX_REPORTED_ISSUE_LENGTH),
    };
  }
  return undefined;
};

/**
 * Whether the request died because its body passed the bound while the API builder read it: the
 * builder raises a body it cannot read as a defect holding the server's `RequestParseError`.
 */
const bodyTooLarge = (cause: Cause.Cause<unknown>) =>
  cause.reasons.some(
    (reason) =>
      Cause.isDieReason(reason) &&
      HttpServerError.isHttpServerError(reason.defect) &&
      reason.defect.reason._tag === "RequestParseError" &&
      reason.defect.reason.reason === "BodyTooLarge",
  );

/**
 * Where a public API's caller checks a request it got wrong, named at the end of the middleware's
 * `RouteNotFound` and `InvalidRequest` messages: the database API's OpenAPI document, or the sharded
 * API's endpoint definitions.
 */
export interface PublicHttpApiReference {
  /** Completes "Check the method and path against …" and "Check the request against …". */
  readonly contract: string;
}

/**
 * Global middleware every public API shares (D69, D79): provides `DbHttpRequest`, returns the
 * request id as `x-request-id`, answers an unmatched route with `RouteNotFound` (404) and a request
 * that cannot be read or does not match its endpoint's schemas with `InvalidRequest` (400), and
 * answers any other router failure or defect with `InternalFailure` (500) after logging it, so no
 * internal detail reaches the caller. Every body is the public error envelope.
 */
export const publicHttpRequestMiddlewareLayer = (reference: PublicHttpApiReference) =>
  HttpRouter.middleware<{
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
        const invalidRequest = (message: string) =>
          HttpServerResponse.jsonUnsafe(
            encodeInvalidRequest(
              new InvalidRequestPublicError({
                code: "InvalidRequest",
                operation: "routeRequest",
                message,
                details: { method: request.method, path: pathname },
                requestId,
                retryable: false,
              }),
            ),
            { status: 400, headers: noStore },
          );
        // A declared length over the bound is refused before anything is read. A body with no
        // declared length (chunked) is bounded where it is read: the router reads it once, through
        // `HttpIncomingMessage.MaxBodySize`, and stops at the bound (D100). The body is never read
        // here: a second read of a request body fails in the Workers runtime.
        const tooLarge = () =>
          invalidRequest(
            `The request to ${request.method} ${pathname} has a body larger than ${MAX_REQUEST_BODY_BYTES} bytes (16 MiB), the most any request may send. Send a smaller batch, or fewer keys and ranges.`,
          );
        const declared = Number(request.headers["content-length"] ?? "0");
        if (Number.isFinite(declared) && declared > MAX_REQUEST_BODY_BYTES) return tooLarge();
        return yield* httpEffect.pipe(
          Effect.provideService(
            HttpIncomingMessage.MaxBodySize,
            ByteSize.bytes(MAX_REQUEST_BODY_BYTES),
          ),
          // The API builder answers a content type no endpoint accepts itself, in plain text.
          Effect.map((response) =>
            response.status === 415
              ? invalidRequest(
                  `The request to ${request.method} ${pathname} has a content type the API does not accept. Send the body as application/json.`,
                )
              : response,
          ),
          Effect.catchTag("HttpServerError", (error) => {
            switch (error.reason._tag) {
              case "RouteNotFound":
                return Effect.succeed(
                  HttpServerResponse.jsonUnsafe(
                    encodeRouteNotFound(
                      new RouteNotFoundPublicError({
                        code: "RouteNotFound",
                        operation: "routeRequest",
                        message: `No route matches ${request.method} ${pathname}. Check the method and path against ${reference.contract}.`,
                        details: { method: request.method, path: pathname },
                        requestId,
                        retryable: false,
                      }),
                    ),
                    { status: 404, headers: noStore },
                  ),
                );
              case "RequestParseError":
                if (error.reason.reason === "BodyTooLarge") return Effect.succeed(tooLarge());
                return Effect.succeed(
                  invalidRequest(
                    `The request to ${request.method} ${pathname} could not be read because its path, query, or body is malformed. Fix the request and send it again.`,
                  ),
                );
              case "InternalError":
              case "ResponseError":
                return internalFailure("shaledb API router failed", error);
            }
          }),
          Effect.catchCauseIf(Cause.hasDies, (cause) => {
            if (bodyTooLarge(cause)) return Effect.succeed(tooLarge());
            const malformed = malformedRequestPart(cause);
            return malformed === undefined
              ? internalFailure("shaledb API request failed on a defect", Cause.pretty(cause))
              : Effect.succeed(
                  invalidRequest(
                    `The request to ${request.method} ${pathname} was refused because its ${malformed.part} did not match what the endpoint accepts: ${malformed.issue}. Check the request against ${reference.contract} and send it again.`,
                  ),
                );
          }),
          Effect.annotateLogs({ requestId, path: pathname, method: request.method }),
        );
      }).pipe(Effect.provide(dbHttpRequestLayer)),
    { global: true },
  );

/** The database API's request middleware (`DbHttpApi`, whose contract is its OpenAPI document). */
export const dbHttpRequestMiddlewareLayer = publicHttpRequestMiddlewareLayer({
  contract: "the API's OpenAPI document",
});
