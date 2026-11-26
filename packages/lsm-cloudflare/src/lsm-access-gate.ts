import * as Alchemy from "alchemy";
import type { HttpEffect } from "alchemy/Http";
import type * as Output from "alchemy/Output";
import type { Provider } from "alchemy/Provider";
import { CurrentRuntimeContext, type RuntimeContext } from "alchemy/RuntimeContext";
import { Stage } from "alchemy/Stage";
import { Clock, Config, Effect, Option, Redacted, Schema } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";

import { UnauthenticatedPublicError } from "./db-public-error.ts";
import { isLocalLsmStage } from "./lsm-stage.ts";

/**
 * Name of the binding that carries a live stage's access token to its Workers (a `secret_text`
 * binding on Cloudflare).
 */
export const LSM_ACCESS_TOKEN_BINDING = "SHALEDB_ACCESS_TOKEN";

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
export type LsmAccessGate = <R>(handler: HttpEffect<R>) => HttpEffect<R | RuntimeContext>;

/**
 * Builds an access check for the Worker being initialized from a token resource: at plan time it
 * binds the token onto the Worker as the secret `binding`; at runtime it reads the binding back and
 * returns {@link lsmAccessGate} over it.
 *
 * @param binding - Name of the secret binding that carries the token.
 * @param token - Yields the token's Output at plan time, or `None` when the stage is not gated.
 * @param credential - Names the token in the `401` message (see {@link LSM_ACCESS_TOKEN_CREDENTIAL}).
 * @returns The wrapper. A bound value that is not a valid secret stops the Worker (a defect): a
 *   misconfigured gate must not start open.
 */
export const lsmAccessGateFor = <R>(
  binding: string,
  token: Effect.Effect<Option.Option<Output.Output<Redacted.Redacted<string>>>, never, R>,
  credential: string,
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
    return lsmAccessGate(bound, credential);
  });

/**
 * Reads one access token's current value at runtime, such as a Secrets Store secret bound to the
 * Worker: it can change without a deploy.
 */
export type LsmRotatingToken = Effect.Effect<
  Redacted.Redacted<string>,
  { readonly message: string },
  RuntimeContext
>;

/**
 * Where a Worker's access tokens come from; the composition root (the Stack) decides (D64). The
 * gate accepts a request that presents any of them.
 *
 * @template R - What the plan-time halves need, such as the Worker being bound.
 */
export interface LsmAccessTokenSource<R = never> {
  /**
   * A token fixed at deploy time, bound onto the Worker as the secret `SHALEDB_ACCESS_TOKEN`
   * (yielded at plan time), or `None`.
   */
  readonly fixed: Effect.Effect<Option.Option<Output.Output<Redacted.Redacted<string>>>, never, R>;
  /**
   * The readers of tokens that rotate without a deploy, set up in the Worker's init Effect (each
   * binds its secret at plan time and reads it at runtime). Empty for none.
   */
  readonly rotating: Effect.Effect<ReadonlyArray<LsmRotatingToken>, never, R>;
}

/**
 * The stage's state-held token only ({@link LsmAccessToken}): no gate on local stages, one fixed
 * token on live ones. For tests and Stacks without rotation.
 */
export const lsmStageAccessTokens: LsmAccessTokenSource<Stage | Provider<Alchemy.Random>> = {
  fixed: LsmAccessToken,
  rotating: Effect.succeed([]),
};

/** How long a Worker keeps a rotating token's value before reading it again. */
export const LSM_ROTATING_TOKEN_REFRESH_MS = 30_000;

/**
 * Builds the access check for the Worker being initialized from `source` (D64): binds the fixed
 * token at plan time, sets up the rotating tokens' readers, and returns {@link lsmAccessGateOver}.
 *
 * @returns The wrapper. A bound fixed token that is not a valid secret stops the Worker (a defect):
 *   a misconfigured gate must not start open.
 */
export const lsmAccessGateFrom = <R>(source: LsmAccessTokenSource<R>) =>
  Effect.gen(function* () {
    if ((yield* Alchemy.ALCHEMY_PHASE) === "plan") {
      const planned = yield* source.fixed;
      const runtimeContext = yield* CurrentRuntimeContext;
      if (Option.isSome(planned) && runtimeContext !== undefined) {
        yield* runtimeContext.set(LSM_ACCESS_TOKEN_BINDING, planned.value);
      }
    }
    const fixed = yield* Config.option(Config.Redacted(LSM_ACCESS_TOKEN_BINDING)).pipe(
      Effect.orDie,
    );
    const rotating = yield* source.rotating;
    return lsmAccessGateOver(fixed, rotating, LSM_ROTATING_TOKEN_REFRESH_MS);
  });

/**
 * The access check over a fixed token and rotating ones (D64): with neither, every request passes
 * (local stages); otherwise a request must present one of them as `Authorization: Bearer <token>`,
 * or it is answered `401` with the public error envelope (`UnauthenticatedPublicError`, which
 * `DbHttpApi` declares) before it reaches any handler. Cloudflare Access may sit in front of it.
 *
 * **Details**
 *
 * Rotating tokens are read on the first request and again once `refreshMs` has passed, so a
 * rotation takes effect within that time without a deploy. A read that fails keeps the value read
 * before; a token never read is not accepted, so a gate whose secrets cannot be read stays closed.
 * Each presented token is compared with every accepted one in time that depends only on their
 * lengths.
 */
export const lsmAccessGateOver = (
  fixed: Option.Option<Redacted.Redacted<string>>,
  rotating: ReadonlyArray<LsmRotatingToken>,
  refreshMs: number,
): LsmAccessGate => {
  if (Option.isNone(fixed) && rotating.length === 0) return (handler) => handler;
  let read: Array<Option.Option<Redacted.Redacted<string>>> = rotating.map(() => Option.none());
  let readAtMs = Number.NEGATIVE_INFINITY;
  const accepted = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    if (rotating.length > 0 && now - readAtMs >= refreshMs) {
      const fresh = yield* Effect.forEach(rotating, (token) => Effect.option(token), {
        concurrency: "unbounded",
      });
      read = fresh.map((value, index) => (Option.isSome(value) ? value : (read[index] ?? value)));
      readAtMs = now;
    }
    return [...Option.toArray(fixed), ...read.flatMap(Option.toArray)];
  });
  return (handler) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const presented = bearerToken(request.headers["authorization"]);
      const tokens = yield* accepted;
      if (Option.isSome(presented) && matchesAny(presented.value, tokens)) {
        return yield* handler;
      }
      return unauthenticatedResponse(
        request.headers["cf-ray"] ?? crypto.randomUUID(),
        LSM_ACCESS_TOKEN_CREDENTIAL,
      );
    });
};

/** Whether `presented` equals any of `tokens`, comparing with every one (no early exit). */
const matchesAny = (presented: string, tokens: ReadonlyArray<Redacted.Redacted<string>>) => {
  let matched = false;
  for (const token of tokens) matched = sameSecret(presented, token) || matched;
  return matched;
};

/**
 * The access check for a token: `None` passes every request through; `Some(token)` requires
 * `Authorization: Bearer <token>`. `credential` names the token in the `401` message.
 */
export const lsmAccessGate =
  (
    token: Option.Option<Redacted.Redacted<string>>,
    credential: string = LSM_ACCESS_TOKEN_CREDENTIAL,
  ): LsmAccessGate =>
  (handler) =>
    Option.isNone(token)
      ? handler
      : Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          const presented = bearerToken(request.headers["authorization"]);
          if (Option.isSome(presented) && sameSecret(presented.value, token.value)) {
            return yield* handler;
          }
          return unauthenticatedResponse(
            request.headers["cf-ray"] ?? crypto.randomUUID(),
            credential,
          );
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

/**
 * How the `401` message names the token the database, GC, and shard-router APIs accept. It points to
 * the runbook, not to a command: production's tokens are printed only when created or rotated (D64),
 * and `preview-cli token` reads previews alone.
 */
export const LSM_ACCESS_TOKEN_CREDENTIAL =
  'one of the stage\'s access tokens (docs/operations.md, "Access tokens")';

/**
 * The `401` message for a request without `credential`, the same on every gated Worker: it names
 * the token to send and never the one expected.
 */
export const unauthenticatedMessage = (credential: string) =>
  `The request was not authenticated. Send ${credential} as \`Authorization: Bearer <token>\`.`;

/** The public error envelope (`Unauthenticated`, 401) for a request without the token. */
const unauthenticatedResponse = (requestId: string, credential: string) =>
  HttpServerResponse.jsonUnsafe(
    encodeUnauthenticated(
      new UnauthenticatedPublicError({
        code: "Unauthenticated",
        operation: "routeRequest",
        message: unauthenticatedMessage(credential),
        details: {},
        requestId,
        retryable: false,
      }),
    ),
    {
      status: 401,
      headers: {
        "cache-control": "no-store",
        "www-authenticate": 'Bearer realm="shaledb"',
        "x-request-id": requestId,
      },
    },
  );
