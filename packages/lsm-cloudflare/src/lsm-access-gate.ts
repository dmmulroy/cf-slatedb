import * as Alchemy from "alchemy";
import type { HttpEffect } from "alchemy/Http";
import type * as Output from "alchemy/Output";
import type { Provider } from "alchemy/Provider";
import { CurrentRuntimeContext } from "alchemy/RuntimeContext";
import { Stage } from "alchemy/Stage";
import { Config, Effect, Option, Redacted, Schema } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";

import { UnauthenticatedPublicError } from "./db-public-error.ts";
import { isLocalLsmStage } from "./lsm-stage.ts";

/**
 * Name of the binding that carries a live stage's access token to its Workers (a `secret_text`
 * binding on Cloudflare).
 */
export const LSM_ACCESS_TOKEN_BINDING = "CF_SLATEDB_ACCESS_TOKEN";

/**
 * The access token of the current stage: `None` on local stages, which are not gated; on every live
 * stage, a random 256-bit secret generated once and kept in the stage's Alchemy state, so it stays
 * the same across redeploys and every machine that manages the stage reads the same value.
 *
 * **Details**
 *
 * Every Worker that yields it shares one `Random` resource (`LsmAccessToken`). Yield it at plan
 * time, in a Worker's init Effect or in the Stack.
 */
export const LsmAccessToken: Effect.Effect<
  Option.Option<Output.Output<Redacted.Redacted<string>>>,
  never,
  Stage | Provider<Alchemy.Random>
> = Effect.gen(function* () {
  const stage = yield* Stage;
  if (isLocalLsmStage(stage)) return Option.none();
  return Option.some(yield* Alchemy.makeRandom("LsmAccessToken"));
});

/** Wraps a Worker's `fetch` handler with the stage's access check. */
export type LsmAccessGate = <R>(handler: HttpEffect<R>) => HttpEffect<R>;

/**
 * Builds an access check for the Worker being initialized from a token resource: at plan time it
 * binds the token onto the Worker as the secret `binding`; at runtime it reads the binding back and
 * returns {@link lsmAccessGate} over it.
 *
 * @param binding - Name of the secret binding that carries the token.
 * @param token - Yields the token's Output at plan time, or `None` when the stage is not gated.
 * @returns The wrapper. A bound value that is not a valid secret stops the Worker (a defect): a
 *   misconfigured gate must not start open.
 */
export const lsmAccessGateFor = <R>(
  binding: string,
  token: Effect.Effect<Option.Option<Output.Output<Redacted.Redacted<string>>>, never, R>,
) =>
  Effect.gen(function* () {
    const phase = yield* Alchemy.ALCHEMY_PHASE;
    if (phase === "plan") {
      const planned = yield* token;
      const runtimeContext = yield* CurrentRuntimeContext;
      if (Option.isSome(planned) && runtimeContext !== undefined) {
        yield* runtimeContext.set(binding, planned.value);
      }
    }
    const bound = yield* Config.option(Config.Redacted(binding)).pipe(Effect.orDie);
    return lsmAccessGate(bound);
  });

/**
 * Builds the access check for the Worker being initialized: binds the stage's
 * {@link LsmAccessToken} onto the Worker at plan time, and at runtime returns a wrapper that answers
 * every request without `Authorization: Bearer <token>` with `401`.
 *
 * **Details**
 *
 * A stand-in for Cloudflare Access on the live preview stages, whose Workers have public
 * `workers.dev` URLs: like Access, it rejects a request before it reaches any handler, with the
 * public error envelope (`UnauthenticatedPublicError`, `401`), which `DbHttpApi` declares. Local
 * stages bind no token and pass every request through. The comparison takes the same time whatever
 * the presented token.
 *
 * @returns The wrapper; it never fails (a request without the token is answered, not failed).
 */
export const lsmAccessGateForStage = lsmAccessGateFor(LSM_ACCESS_TOKEN_BINDING, LsmAccessToken);

/**
 * The access check for a token: `None` passes every request through; `Some(token)` requires
 * `Authorization: Bearer <token>`.
 */
export const lsmAccessGate =
  (token: Option.Option<Redacted.Redacted<string>>): LsmAccessGate =>
  (handler) =>
    Option.isNone(token)
      ? handler
      : Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          const presented = bearerToken(request.headers["authorization"]);
          if (Option.isSome(presented) && sameSecret(presented.value, token.value)) {
            return yield* handler;
          }
          return unauthenticatedResponse(request.headers["cf-ray"] ?? crypto.randomUUID());
        });

const bearerToken = (header: string | undefined): Option.Option<string> => {
  if (header === undefined) return Option.none();
  const match = /^Bearer\s+(\S+)\s*$/i.exec(header);
  return match?.[1] === undefined ? Option.none() : Option.some(match[1]);
};

/** Compares in time that depends only on the expected token's length, never on where they differ. */
const sameSecret = (presented: string, expected: Redacted.Redacted<string>): boolean => {
  const left = new TextEncoder().encode(presented);
  const right = new TextEncoder().encode(Redacted.value(expected));
  let difference = left.length ^ right.length;
  for (let index = 0; index < right.length; index++) {
    difference |= (left[index] ?? 0) ^ (right[index] ?? 0);
  }
  return difference === 0;
};

const encodeUnauthenticated = Schema.encodeSync(UnauthenticatedPublicError);

/** The public error envelope (`Unauthenticated`, 401) for a request without the token. */
const unauthenticatedResponse = (requestId: string) =>
  HttpServerResponse.jsonUnsafe(
    encodeUnauthenticated(
      new UnauthenticatedPublicError({
        code: "Unauthenticated",
        operation: "routeRequest",
        message:
          "The request was not authenticated. Send the stage's token as `Authorization: Bearer <token>`; `preview-cli token <stage>` prints it.",
        details: {},
        requestId,
        retryable: false,
      }),
    ),
    {
      status: 401,
      headers: {
        "cache-control": "no-store",
        "www-authenticate": 'Bearer realm="cf-slatedb"',
        "x-request-id": requestId,
      },
    },
  );
