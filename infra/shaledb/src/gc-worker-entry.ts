import { gcWorkerLayer } from "@shaledb/lsm-cloudflare";

import { shaledbAccessTokens } from "./shaledb-access-tokens.ts";
import { shaledbTailConsumers, shaledbTelemetry } from "./shaledb-telemetry.ts";
import { shaledbWorkerSettings } from "./shaledb-stack-config.ts";

/** Local dev port of the GC Worker (live stages serve its operator API on workers.dev, gated). */
export const GC_DEV_PORT = 1340;

/** The GC cron Worker of the Stack (every 5 minutes, `gcInterval`); this module is its bundle entry. */
export const shaledbGcWorkerLayer = gcWorkerLayer({
  main: import.meta.url,
  telemetry: shaledbTelemetry,
  tailConsumers: shaledbTailConsumers,
  settings: shaledbWorkerSettings,
  accessTokens: shaledbAccessTokens,
  devPort: GC_DEV_PORT,
  schedule: "*/5 * * * *",
});

export default shaledbGcWorkerLayer;
