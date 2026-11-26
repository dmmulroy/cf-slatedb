import { compactorWorkerLayer } from "@shaledb/lsm-cloudflare";

import { shaledbTailConsumers, shaledbTelemetry } from "./shaledb-telemetry.ts";
import { shaledbCompactorCpuLimit, shaledbWorkerSettings } from "./shaledb-stack-config.ts";

/** Local dev port of the compactor Worker (it has no public URL when deployed). */
export const COMPACTOR_DEV_PORT = 1339;

/**
 * The compactor Worker of the Stack: every `CompactorServer`, the `CompactionWorkflow`, and the
 * shared `CompactionBudgetServer`. This module is its bundle entry.
 */
export const shaledbCompactorWorkerLayer = compactorWorkerLayer({
  main: import.meta.url,
  telemetry: shaledbTelemetry,
  settings: shaledbWorkerSettings,
  cpuLimitMs: shaledbCompactorCpuLimit,
  tailConsumers: shaledbTailConsumers,
  devPort: COMPACTOR_DEV_PORT,
});

export default shaledbCompactorWorkerLayer;
