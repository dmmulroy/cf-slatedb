import * as secretsStore from "@distilled.cloud/cloudflare/secrets-store";
import { Data, Effect, Option, Redacted, Schedule, Stream } from "effect";

import {
  accessTokenSecretName,
  SHALEDB_ACCESS_TOKEN_SLOTS,
  SHALEDB_SECRETS_STORE_NAME,
  type ShaledbAccessTokenSlot,
} from "./shaledb-access-tokens.ts";
import { SHALEDB_CLOUDFLARE_ACCOUNT_ID } from "./shaledb-stage.ts";

/**
 * An access-token operation could not complete.
 *
 * **Details**
 *
 * - `StoreNotFound`: the account has no Secrets Store named `default_secrets_store` (create it in
 *   the Cloudflare dashboard, then retry), or the stage's token secrets do not exist yet (run
 *   `tokens ensure <stage>`).
 * - `CloudflareUnavailable`: the Secrets Store API failed or refused the request. Retry; check
 *   that the Alchemy profile's credentials can edit Secrets Store secrets.
 * - `SecretNotActive`: a token was written (and shown) but its secret did not turn active within
 *   the wait, so Workers cannot bind it yet (D121). Wait, then deploy.
 */
export class AccessTokenStoreError extends Data.TaggedError("AccessTokenStoreError")<{
  readonly reason: "StoreNotFound" | "CloudflareUnavailable" | "SecretNotActive";
  readonly message: string;
}> {}

/** A token this run wrote: which stage and slot, and its value (shown to the operator once). */
export interface WrittenAccessToken {
  readonly stage: string;
  readonly slot: ShaledbAccessTokenSlot;
  readonly secretName: string;
  readonly token: Redacted.Redacted<string>;
  /** Whether the secret turned active within the wait; a deploy cannot bind it before (D121). */
  readonly active: boolean;
}

/** A new 256-bit token as 64 hex digits, like the state-held preview tokens. */
const newToken = () => {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return Redacted.make(Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(""));
};

const unavailable = (action: string) => (error: { readonly message?: string }) =>
  new AccessTokenStoreError({
    reason: "CloudflareUnavailable",
    message: `Could not ${action}: ${error.message ?? "the Secrets Store API failed"}. Retry; it is safe to repeat.`,
  });

/** The account's Secrets Store that holds the access tokens. */
const tokenStore = secretsStore.listStores.items({ accountId: SHALEDB_CLOUDFLARE_ACCOUNT_ID }).pipe(
  Stream.filter((store) => store.name === SHALEDB_SECRETS_STORE_NAME),
  Stream.runHead,
  Effect.mapError(unavailable("list the account's Secrets Stores")),
  Effect.flatMap(
    Option.match({
      onNone: () =>
        Effect.fail(
          new AccessTokenStoreError({
            reason: "StoreNotFound",
            message: `The account has no Secrets Store named ${SHALEDB_SECRETS_STORE_NAME}. Create it in the Cloudflare dashboard (Secrets Store), then retry.`,
          }),
        ),
      onSome: (store) => Effect.succeed(store.id),
    }),
  ),
);

/** The id of the secret named `name` in the store, if it exists. */
const secretIdByName = (storeId: string, name: string) =>
  secretsStore.listStoreSecrets
    .items({
      accountId: SHALEDB_CLOUDFLARE_ACCOUNT_ID,
      storeId,
      search: name,
      order: "name",
    })
    .pipe(
      Stream.filter((secret) => secret.name === name),
      Stream.runHead,
      Effect.map(Option.map((secret) => secret.id)),
      Effect.mapError(unavailable(`look up secret ${name}`)),
    );

/** How often, and how many times, a written secret's status is read before giving up on it. */
export const SECRET_ACTIVE_POLL = { spacing: "500 millis", reads: 20 } as const;

/**
 * Whether a written secret turned `active` (D121): its status is read up to
 * {@link SECRET_ACTIVE_POLL}'s reads, `spacing` apart (about 10 s). Workers cannot bind a pending
 * secret, so a deploy right after a write that never turned active fails to bind it. A failed read
 * counts as not yet active.
 */
export const awaitSecretActive = <E, R>(readStatus: Effect.Effect<string, E, R>) =>
  readStatus.pipe(
    Effect.orElseSucceed(() => "unreadable"),
    Effect.repeat({
      schedule: Schedule.spaced(SECRET_ACTIVE_POLL.spacing),
      until: (status) => status === "active",
      times: SECRET_ACTIVE_POLL.reads - 1,
    }),
    Effect.map((status) => status === "active"),
  );

const awaitActive = (storeId: string, secretId: string) =>
  awaitSecretActive(
    secretsStore
      .getStoreSecret({ accountId: SHALEDB_CLOUDFLARE_ACCOUNT_ID, storeId, secretId })
      .pipe(Effect.map((secret) => String(secret.status))),
  );

/** Writes a new token into `slot`, creating its secret when missing. */
const writeSlot = (storeId: string, stage: string, slot: ShaledbAccessTokenSlot) =>
  Effect.gen(function* () {
    const secretName = accessTokenSecretName(stage, slot);
    const token = newToken();
    const existing = yield* secretIdByName(storeId, secretName);
    const secretId = yield* Option.match(existing, {
      onSome: (id) =>
        secretsStore
          .patchStoreSecret({
            accountId: SHALEDB_CLOUDFLARE_ACCOUNT_ID,
            storeId,
            secretId: id,
            value: Redacted.value(token),
          })
          .pipe(Effect.as(id), Effect.mapError(unavailable(`rotate secret ${secretName}`))),
      onNone: () =>
        secretsStore
          .createStoreSecret({
            accountId: SHALEDB_CLOUDFLARE_ACCOUNT_ID,
            storeId,
            body: [
              {
                name: secretName,
                value: Redacted.value(token),
                scopes: ["workers"],
                comment: `shaledb access token, stage ${stage}, slot ${slot} (D64)`,
              },
            ],
          })
          .pipe(
            Effect.map((created) => created.result[0]?.id ?? ""),
            Effect.mapError(unavailable(`create secret ${secretName}`)),
          ),
    });
    const active = yield* awaitActive(storeId, secretId);
    return { stage, slot, secretName, token, active } satisfies WrittenAccessToken;
  });

/**
 * Creates whichever of a stage's two token secrets are missing, each with a new random token, and
 * leaves existing ones untouched (D64). Run before every deploy of a live stage: its Workers bind
 * both secrets and cannot deploy without them.
 *
 * @returns The tokens it created (empty when both existed), or `AccessTokenStoreError`.
 */
export const ensureAccessTokens = (stage: string) =>
  Effect.gen(function* () {
    const storeId = yield* tokenStore;
    const created: Array<WrittenAccessToken> = [];
    for (const slot of SHALEDB_ACCESS_TOKEN_SLOTS) {
      const existing = yield* secretIdByName(storeId, accessTokenSecretName(stage, slot));
      if (Option.isNone(existing)) created.push(yield* writeSlot(storeId, stage, slot));
    }
    return created;
  });

/**
 * The stage's token slots whose secrets do not exist yet (D64): a production deploy refuses to start
 * until there are none, so a token is never created inside a deploy's log.
 *
 * @returns The missing slots (empty when both exist), or `AccessTokenStoreError`.
 */
export const missingAccessTokens = (stage: string) =>
  Effect.gen(function* () {
    const storeId = yield* tokenStore;
    const present = new Set<string>();
    for (const slot of SHALEDB_ACCESS_TOKEN_SLOTS) {
      const name = accessTokenSecretName(stage, slot);
      if (Option.isSome(yield* secretIdByName(storeId, name))) present.add(name);
    }
    return missingAccessTokenSlots(stage, present);
  });

/**
 * The slots of `stage` whose secret is not among `present` (secret names that exist in the store):
 * the decision `tokens check` and the production deploy make (D64, D104). Only this stage's exact
 * names count, so another stage's secret, or a name that merely contains this one, never satisfies
 * a slot.
 */
export const missingAccessTokenSlots = (
  stage: string,
  present: ReadonlySet<string>,
): ReadonlyArray<ShaledbAccessTokenSlot> =>
  SHALEDB_ACCESS_TOKEN_SLOTS.filter((slot) => !present.has(accessTokenSecretName(stage, slot)));

/**
 * Replaces the token in one slot with a new random one (D64). The other slot's token stays valid,
 * so callers move to it first; Workers accept the new token within 30 s, with no deploy, and stop
 * accepting the old one then.
 *
 * @returns The new token, or `AccessTokenStoreError`.
 */
export const rotateAccessToken = (stage: string, slot: ShaledbAccessTokenSlot) =>
  Effect.flatMap(tokenStore, (storeId) => writeSlot(storeId, stage, slot));

/**
 * The failure to report after showing `written` tokens, when any secret did not turn active
 * (D121): the tokens are shown first, since they cannot be read again, and the command still exits
 * non-zero, so a script does not deploy against a secret Workers cannot bind yet.
 */
export const inactiveSecretsFailure = (
  written: ReadonlyArray<WrittenAccessToken>,
): Option.Option<AccessTokenStoreError> => {
  const pending = written.filter((token) => !token.active).map((token) => token.secretName);
  return pending.length === 0
    ? Option.none()
    : Option.some(
        new AccessTokenStoreError({
          reason: "SecretNotActive",
          message: `${pending.join(", ")} ${pending.length === 1 ? "was" : "were"} written (the token is shown above; store it now) but did not become active within 10 s, so Workers cannot bind ${pending.length === 1 ? "it" : "them"} yet. Wait a minute, then deploy; do not write the token again.`,
        }),
      );
};
