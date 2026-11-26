import { blockServerWorkerLayer } from "../../src/block-server-worker.ts";
import { lsmOtlpTelemetry } from "../../src/lsm-telemetry.ts";
import {
  INTEGRATION_OTLP_BASE_URL,
  INTEGRATION_PORTS,
  INTEGRATION_WORKER_SETTINGS,
} from "./integration-ports.ts";

/** The integration tests' `BlockServer` Worker; this module is its bundle entry. */
const testBlockServerLayer = blockServerWorkerLayer({
  main: import.meta.url,
  telemetry: lsmOtlpTelemetry(INTEGRATION_OTLP_BASE_URL),
  settings: INTEGRATION_WORKER_SETTINGS,
  devPort: INTEGRATION_PORTS.blockServer,
});

export default testBlockServerLayer;
