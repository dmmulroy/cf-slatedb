import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  isLocalLsmStage,
  LsmAccessToken,
  type LsmAccessTokenSource,
} from "@shaledb/lsm-cloudflare";
import * as Alchemy from "alchemy";
import type { Provider } from "alchemy/Provider";
import { Stage } from "alchemy/Stage";
import { Effect, Option } from "effect";

/**
 * The two access-token slots of a live stage (D64). Both are valid at once, so a token can be
 * replaced without downtime: move callers to the other slot's token, then rotate this slot.
 */
export const SHALEDB_ACCESS_TOKEN_SLOTS = ["A", "B"] as const;

/** One of {@link SHALEDB_ACCESS_TOKEN_SLOTS}. */
export type ShaledbAccessTokenSlot = (typeof SHALEDB_ACCESS_TOKEN_SLOTS)[number];

/**
 * The Secrets Store that holds the access tokens: the account's store. Cloudflare gives an account
 * one store, `default_secrets_store`.
 */
export const SHALEDB_SECRETS_STORE_NAME = "default_secrets_store";

/**
 * The Secrets Store secret that holds a stage's token in `slot`, such as
 * `SHALEDB_PRODUCTION_ACCESS_TOKEN_A` (the store is account-wide, so the name carries the stage).
 */
export const accessTokenSecretName = (stage: string, slot: ShaledbAccessTokenSlot): string =>
  `SHALEDB_${stage.toUpperCase().replaceAll(/[^A-Z0-9]/g, "_")}_ACCESS_TOKEN_${slot}`;

/** Whether a stage is one of the reusable previews (`preview-<n>`). */
export const isShaledbPreviewStage = (stage: string): boolean => /^preview-\d+$/.test(stage);

/**
 * The access tokens of the Stack's public Workers (D64, P1).
 *
 * **Details**
 *
 * - **Every live stage:** the two slots' Secrets Store secrets, bound by reference
 *   (`Secret.lookup`): Alchemy never writes their values, so a rotation (`vp run
 *   @shaledb/infra#tokens rotate <stage> <slot>`) is never undone by a deploy, and Workers
 *   read the new value within 30 s, with no deploy. The secrets must exist before the first deploy
 *   (`tokens ensure <stage>`, which the deploy tasks run first).
 * - **Previews only:** also the stage's state-held token (`LsmAccessToken`), which
 *   `preview-cli token` prints and the scenarios use. Production has none: its tokens are readable
 *   nowhere but where an operator stored them when they were created.
 * - **Local stages:** none; the gate is open.
 */
export const shaledbAccessTokens: LsmAccessTokenSource<
  | Stage
  | Provider<Alchemy.Random>
  | Cloudflare.Providers
  | Cloudflare.Worker
  | Cloudflare.WorkerEnvironment
> = {
  fixed: Effect.gen(function* () {
    const stage = yield* Stage;
    return isShaledbPreviewStage(stage) ? yield* LsmAccessToken : Option.none();
  }),
  rotating: Effect.gen(function* () {
    const stage = yield* Stage;
    if (isLocalLsmStage(stage)) return [];
    const store = Cloudflare.SecretsStore.Store.lookup({ name: SHALEDB_SECRETS_STORE_NAME });
    return yield* Effect.forEach(SHALEDB_ACCESS_TOKEN_SLOTS, (slot) =>
      Cloudflare.SecretsStore.ReadSecret(
        Cloudflare.SecretsStore.Secret.lookup({
          storeId: store.storeId,
          name: accessTokenSecretName(stage, slot),
        }),
      ),
    ).pipe(Effect.provide(Cloudflare.SecretsStore.ReadSecretBinding));
  }),
};
