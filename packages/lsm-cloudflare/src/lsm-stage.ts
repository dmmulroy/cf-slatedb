import { Stage } from "alchemy/Stage";
import { Effect } from "effect";

/** Stage names that run only under Alchemy's local providers: `dev`, `test`, `local`. */
const LOCAL_LSM_STAGE_PATTERN = /^(?:dev|test|local)(?:-[a-z0-9-]+)?$/;

/**
 * Whether an Alchemy stage runs only under Alchemy's local providers: `dev`, `test`, `local`, and
 * `dev-*`, `test-*`, `local-*`. Every other stage (`production`, `preview-1`, ...) is live.
 *
 * **Details**
 *
 * Local stages keep their Alchemy state on this machine and export spans before each response;
 * live stages keep their state in the account's Cloudflare state store and export spans after the
 * response.
 */
export const isLocalLsmStage = (stage: string): boolean => LOCAL_LSM_STAGE_PATTERN.test(stage);

/** Compatibility date of every shaledb Worker. */
export const LSM_WORKER_COMPATIBILITY_DATE = "2026-10-15";

/**
 * Compatibility settings of a shaledb Worker on the current stage.
 *
 * **Details**
 *
 * Live stages add `global_fetch_strictly_public`: their Workers export spans to a collector on
 * another `workers.dev` URL of the same account, and without the flag Cloudflare refuses a Worker's
 * `fetch()` to a Worker on its own subdomain, so every span of a Worker's own events is lost (seen on
 * `preview-1`; Durable Object events were unaffected). Service bindings, which carry every call
 * between shaledb's Workers, are not affected by the flag. Local stages leave it off: their spans
 * go to a receiver on `127.0.0.1`, which a strictly public `fetch()` cannot reach.
 */
export const lsmWorkerCompatibility = Effect.gen(function* () {
  const stage = yield* Stage;
  return {
    date: LSM_WORKER_COMPATIBILITY_DATE,
    flags: isLocalLsmStage(stage) ? [] : ["global_fetch_strictly_public"],
  };
});
