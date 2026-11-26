import { lsmStageAccessTokens } from "../../src/lsm-access-gate.ts";
import { gcWorkerLayer } from "../../src/gc-worker.ts";
import { lsmOtlpTelemetry, noLsmTailConsumers } from "../../src/lsm-telemetry.ts";
import {
  INTEGRATION_OTLP_BASE_URL,
  INTEGRATION_PORTS,
  INTEGRATION_WORKER_SETTINGS,
} from "./integration-ports.ts";

/** The integration tests' GC Worker (collected on demand over HTTP); this module is its bundle entry. */
const testGcLayer = gcWorkerLayer({
  main: import.meta.url,
  telemetry: lsmOtlpTelemetry(INTEGRATION_OTLP_BASE_URL),
  tailConsumers: noLsmTailConsumers,
  settings: INTEGRATION_WORKER_SETTINGS,
  accessTokens: lsmStageAccessTokens,
  devPort: INTEGRATION_PORTS.gc,
  schedule: "0 0 1 1 *",
});

export default testGcLayer;
