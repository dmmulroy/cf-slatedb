import { Effect } from "effect";

import { compactorWorkerLayer } from "../../src/compactor-worker.ts";
import { lsmOtlpTelemetry, noLsmTailConsumers } from "../../src/lsm-telemetry.ts";
import {
  INTEGRATION_OTLP_BASE_URL,
  INTEGRATION_PORTS,
  INTEGRATION_WORKER_SETTINGS,
} from "./integration-ports.ts";

/** The integration tests' compactor Worker; this module is its bundle entry. */
const testCompactorLayer = compactorWorkerLayer({
  main: import.meta.url,
  telemetry: lsmOtlpTelemetry(INTEGRATION_OTLP_BASE_URL),
  settings: INTEGRATION_WORKER_SETTINGS,
  cpuLimitMs: Effect.succeed(30_000),
  tailConsumers: noLsmTailConsumers,
  devPort: INTEGRATION_PORTS.compactor,
});

export default testCompactorLayer;
