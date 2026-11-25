import { compactorWorkerLayer } from "@cf-slatedb/lsm-cloudflare";

import { cfSlatedbTelemetry } from "./cf-slatedb-telemetry.ts";

/** Local dev port of the compactor Worker (it has no public URL when deployed). */
export const COMPACTOR_DEV_PORT = 1339;

/**
 * The compactor Worker of the Stack: every `CompactorServer`, the `CompactionWorkflow`, and the
 * shared `CompactionBudgetServer`. This module is its bundle entry.
 */
export const cfSlatedbCompactorWorkerLayer = compactorWorkerLayer({
  main: import.meta.url,
  telemetry: cfSlatedbTelemetry,
  devPort: COMPACTOR_DEV_PORT,
});

export default cfSlatedbCompactorWorkerLayer;
