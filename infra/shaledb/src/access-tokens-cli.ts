/**
 * Creates and rotates a live stage's access tokens in the account's Secrets Store (D64, P1).
 *
 * Usage (through the `tokens` task, with the Alchemy profile's Cloudflare credentials):
 *
 * - `vp run @shaledb/infra#tokens ensure <stage>`: create whichever of the stage's two token
 *   secrets are missing, printing each new token once. The deploy tasks run it first.
 * - `vp run @shaledb/infra#tokens check <stage>`: fail unless both slots exist (the production
 *   deploy task runs it first, so it never creates a token in a deploy's log).
 * - `vp run @shaledb/infra#tokens rotate <stage> <A|B>`: replace one slot's token, printing
 *   the new one. Move callers to the other slot's token before rotating a slot they use.
 *
 * Tokens are printed only here: the Secrets Store never returns them.
 */
import * as Cloudflare from "@alchemy.run/cloudflare";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import * as Auth from "alchemy/Auth";
import { Console, Effect, Layer, Option, Redacted } from "effect";
import * as SecureRandom from "effect/crypto/SecureRandom";
import { FetchHttpClient } from "effect/http";
import { Argument, Command } from "effect/cli";

import {
  AccessTokenStoreError,
  ensureAccessTokens,
  inactiveSecretsFailure,
  missingAccessTokens,
  rotateAccessToken,
  type WrittenAccessToken,
} from "./access-token-store.ts";
import { SHALEDB_ACCESS_TOKEN_SLOTS } from "./shaledb-access-tokens.ts";

const stageArgument = Argument.String("stage").pipe(
  Argument.withDescription("Live stage: production or preview-<n>"),
);

const show = (written: WrittenAccessToken) =>
  Console.log(
    `${written.stage} slot ${written.slot} (${written.secretName}): ${Redacted.value(written.token)}`,
  );

const ensure = Command.make("ensure", { stage: stageArgument }, ({ stage }) =>
  Effect.gen(function* () {
    const created = yield* ensureAccessTokens(stage);
    if (created.length === 0) {
      return yield* Console.log(`${stage}: both access-token slots exist; nothing changed.`);
    }
    yield* Console.log(
      `${stage}: created ${created.length} access token(s). Store them now; they are not shown again.`,
    );
    yield* Effect.forEach(created, show, { discard: true });
    const inactive = inactiveSecretsFailure(created);
    if (Option.isSome(inactive)) return yield* inactive.value;
  }),
).pipe(Command.withDescription("Create the stage's missing access-token secrets"));

const check = Command.make("check", { stage: stageArgument }, ({ stage }) =>
  Effect.gen(function* () {
    const missing = yield* missingAccessTokens(stage);
    if (missing.length === 0) {
      return yield* Console.log(`${stage}: both access-token slots exist.`);
    }
    return yield* new AccessTokenStoreError({
      reason: "StoreNotFound",
      message: `${stage} has no access token in slot(s) ${missing.join(", ")}, so its Workers cannot deploy. Run \`vp run @shaledb/infra#tokens ensure ${stage}\` first and store the tokens it prints.`,
    });
  }),
).pipe(Command.withDescription("Fail unless both of the stage's access-token secrets exist"));

const rotate = Command.make(
  "rotate",
  {
    stage: stageArgument,
    slot: Argument.Literals("slot", SHALEDB_ACCESS_TOKEN_SLOTS),
  },
  ({ stage, slot }) =>
    Effect.gen(function* () {
      const written = yield* rotateAccessToken(stage, slot);
      yield* Console.log(
        `${stage}: rotated slot ${slot}. Workers accept the new token within 30 s and stop accepting the old one. Store it now; it is not shown again.`,
      );
      yield* show(written);
      const inactive = inactiveSecretsFailure([written]);
      if (Option.isSome(inactive)) return yield* inactive.value;
    }),
).pipe(Command.withDescription("Replace one slot's access token with a new one"));

const tokens = Command.make("tokens").pipe(
  Command.withDescription("Create and rotate a live stage's access tokens (Secrets Store)"),
  Command.withSubcommands([ensure, check, rotate]),
);

Command.run(tokens, { version: "0.0.0" }).pipe(
  Effect.provide(
    Cloudflare.CloudflareApiLive().pipe(
      Layer.provideMerge(Auth.AuthProvidersLive),
      Layer.provideMerge(NodeServices.layer),
      Layer.provideMerge(FetchHttpClient.layer),
      Layer.provideMerge(SecureRandom.layerWebCrypto),
    ),
  ),
  NodeRuntime.runMain,
);
