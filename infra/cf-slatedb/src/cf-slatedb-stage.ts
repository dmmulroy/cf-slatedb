import * as Cloudflare from "@alchemy.run/cloudflare";
import { isLocalLsmStage, LOCAL_OTLP_BASE_URL } from "@cf-slatedb/lsm-cloudflare";
import { Stage } from "alchemy/Stage";
import { localState } from "alchemy/State";
import { Effect, Layer } from "effect";

/**
 * Alchemy stage that would own live, retained cf-slatedb resources.
 *
 * **Details**
 *
 * Only this stage would pin production names and retain the R2 bucket. Nothing deploys it: live
 * deploys are approved for the preview stages only (see `docs/handoff.md`).
 */
export const CF_SLATEDB_PRODUCTION_STAGE = "production";

/**
 * The reusable preview stages. Each is redeployed in place rather than destroyed and recreated,
 * so at most three previews exist at once. Their resources get stage-generated names and are never
 * retained.
 */
export const CF_SLATEDB_PREVIEW_STAGES = ["preview-1", "preview-2", "preview-3"] as const;

/** One of {@link CF_SLATEDB_PREVIEW_STAGES}. */
export type CfSlatedbPreviewStage = (typeof CF_SLATEDB_PREVIEW_STAGES)[number];

/**
 * Cloudflare account that owns cf-slatedb's live stages (Dillon's personal account). The
 * Cloudflare state store is pinned to it, so a login that reaches several accounts can never read
 * or write preview state in another one.
 */
export const CF_SLATEDB_CLOUDFLARE_ACCOUNT_ID = "99459fce09d9a9eec83befd957af4ec5";

/**
 * OTLP/HTTP base URL every Worker and Durable Object in the Stack exports spans to.
 *
 * **Details**
 *
 * Read once at plan time from `CF_SLATEDB_OTLP_URL`, defaulting to the trace CLI receiver
 * (`http://127.0.0.1:4318`), and bound onto each Worker by `Telemetry.layerOtlp`.
 */
export const CF_SLATEDB_OTLP_BASE_URL = process.env["CF_SLATEDB_OTLP_URL"] ?? LOCAL_OTLP_BASE_URL;

/** Where a stage keeps its Alchemy state. */
export type CfSlatedbStateBackend = "Local" | "Cloudflare";

/**
 * Selects the Alchemy state backend for a stage: `Local` (files under `.alchemy/` on this machine)
 * for local stages such as `dev`, `Cloudflare` (the account's state store Worker) for every live
 * stage, so a preview deployed from one machine can be redeployed or inspected from another.
 */
export const cfSlatedbStateBackendForStage = (stage: string): CfSlatedbStateBackend =>
  isLocalLsmStage(stage) ? "Local" : "Cloudflare";

/**
 * The Stack's Alchemy state Layer, selected from the current stage (see
 * {@link cfSlatedbStateBackendForStage}).
 *
 * **Details**
 *
 * Local stages never contact Cloudflare for state, so `vp run dev` and the workerd tests stay
 * offline. Live stages use `Cloudflare.state()` pinned to {@link CF_SLATEDB_CLOUDFLARE_ACCOUNT_ID},
 * sharing the account's default `alchemy-state-store` Worker; the Stack name (`CfSlatedb`) and the
 * stage keep this Stack's records apart from other projects'.
 */
export const cfSlatedbStateLayer = Layer.unwrap(
  Effect.gen(function* () {
    const stage = yield* Stage;
    return cfSlatedbStateBackendForStage(stage) === "Local"
      ? localState()
      : Cloudflare.state({ accountId: CF_SLATEDB_CLOUDFLARE_ACCOUNT_ID });
  }),
);
