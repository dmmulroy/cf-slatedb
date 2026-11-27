import { blockServerWorkerLayer, lsmOtlpTelemetry } from "@shaledb/lsm-cloudflare";

import {
  ROUTER_TEST_OTLP_BASE_URL,
  ROUTER_TEST_PORTS,
  ROUTER_TEST_WORKER_SETTINGS,
} from "./router-ports.ts";

/** The router test's `BlockServer` Worker; this module is its bundle entry. */
const routerTestBlockServerLayer = blockServerWorkerLayer({
  main: import.meta.url,
  telemetry: lsmOtlpTelemetry(ROUTER_TEST_OTLP_BASE_URL),
  settings: ROUTER_TEST_WORKER_SETTINGS,
  devPort: ROUTER_TEST_PORTS.blockServer,
});

export default routerTestBlockServerLayer;
