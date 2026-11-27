import {
  compactorWorkerLayer,
  lsmOtlpTelemetry,
  noLsmTailConsumers,
} from "@shaledb/lsm-cloudflare";
import { Effect } from "effect";

import {
  ROUTER_TEST_OTLP_BASE_URL,
  ROUTER_TEST_PORTS,
  ROUTER_TEST_WORKER_SETTINGS,
} from "./router-ports.ts";

/** The router test's compactor Worker; this module is its bundle entry. */
const routerTestCompactorLayer = compactorWorkerLayer({
  main: import.meta.url,
  telemetry: lsmOtlpTelemetry(ROUTER_TEST_OTLP_BASE_URL),
  settings: ROUTER_TEST_WORKER_SETTINGS,
  cpuLimitMs: Effect.succeed(30_000),
  tailConsumers: noLsmTailConsumers,
  devPort: ROUTER_TEST_PORTS.compactor,
});

export default routerTestCompactorLayer;
