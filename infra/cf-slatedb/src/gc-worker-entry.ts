import { gcWorkerLayer } from "@cf-slatedb/lsm-cloudflare";

import { CF_SLATEDB_OTLP_BASE_URL } from "./cf-slatedb-stage.ts";

/** Local dev port of the GC Worker (live stages serve its operator API on workers.dev, gated). */
export const GC_DEV_PORT = 1340;

/** The GC cron Worker of the Stack (every 5 minutes, `gcInterval`); this module is its bundle entry. */
export const cfSlatedbGcWorkerLayer = gcWorkerLayer({
  main: import.meta.url,
  otlpBaseUrl: CF_SLATEDB_OTLP_BASE_URL,
  devPort: GC_DEV_PORT,
  schedule: "*/5 * * * *",
});

export default cfSlatedbGcWorkerLayer;
