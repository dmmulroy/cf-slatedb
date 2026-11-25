import { compactorWorkerLayer } from "@cf-slatedb/lsm-cloudflare";

import { CF_SLATEDB_OTLP_BASE_URL } from "./cf-slatedb-stage.ts";

/** Local dev port of the compactor Worker (it has no public URL when deployed). */
export const COMPACTOR_DEV_PORT = 1339;

/**
 * The compactor Worker of the Stack: every `CompactorServer`, the `CompactionWorkflow`, and the
 * shared `CompactionBudgetServer`. This module is its bundle entry.
 */
export const cfSlatedbCompactorWorkerLayer = compactorWorkerLayer({
  main: import.meta.url,
  otlpBaseUrl: CF_SLATEDB_OTLP_BASE_URL,
  devPort: COMPACTOR_DEV_PORT,
});

export default cfSlatedbCompactorWorkerLayer;
