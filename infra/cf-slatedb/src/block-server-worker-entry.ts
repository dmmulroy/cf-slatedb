import { blockServerWorkerLayer } from "@cf-slatedb/lsm-cloudflare";

import { CF_SLATEDB_OTLP_BASE_URL } from "./cf-slatedb-stage.ts";

/** Local dev port of the `BlockServer` Worker (it has no public URL when deployed). */
export const BLOCK_SERVER_DEV_PORT = 1338;

/** The `BlockServer` Worker of the Stack; this module is its bundle entry. */
export const cfSlatedbBlockServerWorkerLayer = blockServerWorkerLayer({
  main: import.meta.url,
  otlpBaseUrl: CF_SLATEDB_OTLP_BASE_URL,
  devPort: BLOCK_SERVER_DEV_PORT,
});

export default cfSlatedbBlockServerWorkerLayer;
