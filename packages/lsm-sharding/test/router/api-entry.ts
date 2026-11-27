import {
  dbApiWorkerLayer,
  lsmOtlpTelemetry,
  lsmStageAccessTokens,
  noLsmTailConsumers,
} from "@shaledb/lsm-cloudflare";
import { Effect } from "effect";

import {
  ROUTER_TEST_OTLP_BASE_URL,
  ROUTER_TEST_PORTS,
  ROUTER_TEST_WORKER_SETTINGS,
} from "./router-ports.ts";

/** The router test's API Worker (hosting every database's writer); this module is its bundle entry. */
const routerTestApiLayer = dbApiWorkerLayer({
  main: import.meta.url,
  telemetry: lsmOtlpTelemetry(ROUTER_TEST_OTLP_BASE_URL),
  settings: ROUTER_TEST_WORKER_SETTINGS,
  accessTokens: lsmStageAccessTokens,
  cpuLimitMs: Effect.succeed(30_000),
  tailConsumers: noLsmTailConsumers,
  devPort: ROUTER_TEST_PORTS.api,
});

export default routerTestApiLayer;
