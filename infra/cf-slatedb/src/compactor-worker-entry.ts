import { compactorWorkerLayer } from "@cf-slatedb/lsm-cloudflare";

import { cfSlatedbTailConsumers, cfSlatedbTelemetry } from "./cf-slatedb-telemetry.ts";
import { cfSlatedbWorkerSettings } from "./cf-slatedb-stack-config.ts";

/** Local dev port of the compactor Worker (it has no public URL when deployed). */
export const COMPACTOR_DEV_PORT = 1339;

/**
 * The compactor Worker of the Stack: every `CompactorServer`, the `CompactionWorkflow`, and the
 * shared `CompactionBudgetServer`. This module is its bundle entry.
 */
export const cfSlatedbCompactorWorkerLayer = compactorWorkerLayer({
  main: import.meta.url,
  telemetry: cfSlatedbTelemetry,
  settings: cfSlatedbWorkerSettings,
  tailConsumers: cfSlatedbTailConsumers,
  devPort: COMPACTOR_DEV_PORT,
});

export default cfSlatedbCompactorWorkerLayer;
