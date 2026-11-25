import { LOCAL_OTLP_BASE_URL } from "@cf-slatedb/lsm-cloudflare";

/**
 * Alchemy stage that would own live, retained cf-slatedb resources.
 *
 * **Details**
 *
 * Only this stage would pin production names and retain the R2 bucket. Nothing deploys it without
 * Dillon's approval; local development uses stage `dev` under Alchemy's local providers.
 */
export const CF_SLATEDB_PRODUCTION_STAGE = "production";

/**
 * OTLP/HTTP base URL every Worker and Durable Object in the Stack exports spans to.
 *
 * **Details**
 *
 * Read once at plan time from `CF_SLATEDB_OTLP_URL`, defaulting to the trace CLI receiver
 * (`http://127.0.0.1:4318`), and bound onto each Worker by `Telemetry.layerOtlp`.
 */
export const CF_SLATEDB_OTLP_BASE_URL = process.env["CF_SLATEDB_OTLP_URL"] ?? LOCAL_OTLP_BASE_URL;
