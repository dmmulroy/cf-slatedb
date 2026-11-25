import { blockServerWorkerLayer } from "@cf-slatedb/lsm-cloudflare";

import { cfSlatedbTelemetry } from "./cf-slatedb-telemetry.ts";

/** Local dev port of the `BlockServer` Worker (it has no public URL when deployed). */
export const BLOCK_SERVER_DEV_PORT = 1338;

/** The `BlockServer` Worker of the Stack; this module is its bundle entry. */
export const cfSlatedbBlockServerWorkerLayer = blockServerWorkerLayer({
  main: import.meta.url,
  telemetry: cfSlatedbTelemetry,
  devPort: BLOCK_SERVER_DEV_PORT,
});

export default cfSlatedbBlockServerWorkerLayer;
