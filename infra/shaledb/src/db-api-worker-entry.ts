import { dbApiWorkerLayer } from "@shaledb/lsm-cloudflare";

import { shaledbAccessTokens } from "./shaledb-access-tokens.ts";
import { shaledbTailConsumers, shaledbTelemetry } from "./shaledb-telemetry.ts";
import { shaledbApiCpuLimit, shaledbWorkerSettings } from "./shaledb-stack-config.ts";

/** Local dev port of the public API Worker. */
export const DB_API_DEV_PORT = 1337;

/**
 * The public API Worker of the Stack; this module is its bundle entry. It hosts every database's
 * `WriterServer` and binds the LSM bucket.
 */
export const shaledbApiWorkerLayer = dbApiWorkerLayer({
  main: import.meta.url,
  telemetry: shaledbTelemetry,
  settings: shaledbWorkerSettings,
  accessTokens: shaledbAccessTokens,
  cpuLimitMs: shaledbApiCpuLimit,
  tailConsumers: shaledbTailConsumers,
  devPort: DB_API_DEV_PORT,
});

export default shaledbApiWorkerLayer;
