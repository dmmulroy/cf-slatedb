import { blockServerWorkerLayer } from "@shaledb/lsm-cloudflare";

import { shaledbTelemetry } from "./shaledb-telemetry.ts";
import { shaledbWorkerSettings } from "./shaledb-stack-config.ts";

/** Local dev port of the `BlockServer` Worker (it has no public URL when deployed). */
export const BLOCK_SERVER_DEV_PORT = 1338;

/** The `BlockServer` Worker of the Stack; this module is its bundle entry. */
export const shaledbBlockServerWorkerLayer = blockServerWorkerLayer({
  main: import.meta.url,
  telemetry: shaledbTelemetry,
  settings: shaledbWorkerSettings,
  devPort: BLOCK_SERVER_DEV_PORT,
});

export default shaledbBlockServerWorkerLayer;
