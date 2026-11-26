import { dbApiWorkerLayer } from "@cf-slatedb/lsm-cloudflare";

import { cfSlatedbAccessTokens } from "./cf-slatedb-access-tokens.ts";
import { cfSlatedbTailConsumers, cfSlatedbTelemetry } from "./cf-slatedb-telemetry.ts";
import { cfSlatedbApiCpuLimit, cfSlatedbWorkerSettings } from "./cf-slatedb-stack-config.ts";

/** Local dev port of the public API Worker. */
export const DB_API_DEV_PORT = 1337;

/**
 * The public API Worker of the Stack; this module is its bundle entry. It hosts every database's
 * `WriterServer` and binds the LSM bucket.
 */
export const cfSlatedbApiWorkerLayer = dbApiWorkerLayer({
  main: import.meta.url,
  telemetry: cfSlatedbTelemetry,
  settings: cfSlatedbWorkerSettings,
  accessTokens: cfSlatedbAccessTokens,
  cpuLimitMs: cfSlatedbApiCpuLimit,
  tailConsumers: cfSlatedbTailConsumers,
  devPort: DB_API_DEV_PORT,
});

export default cfSlatedbApiWorkerLayer;
