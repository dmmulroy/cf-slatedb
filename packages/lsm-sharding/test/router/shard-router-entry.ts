import {
  lsmOtlpTelemetry,
  lsmStageAccessTokens,
  noLsmTailConsumers,
} from "@shaledb/lsm-cloudflare";

import { shardRouterWorkerLayer } from "../../src/shard-router-worker.ts";
import {
  ROUTER_TEST_OTLP_BASE_URL,
  ROUTER_TEST_PORTS,
  ROUTER_TEST_WORKER_SETTINGS,
} from "./router-ports.ts";

/** The router test's shard router Worker (hosting every group's directory); its bundle entry. */
const routerTestShardRouterLayer = shardRouterWorkerLayer({
  main: import.meta.url,
  telemetry: lsmOtlpTelemetry(ROUTER_TEST_OTLP_BASE_URL),
  settings: ROUTER_TEST_WORKER_SETTINGS,
  accessTokens: lsmStageAccessTokens,
  tailConsumers: noLsmTailConsumers,
  devPort: ROUTER_TEST_PORTS.shardRouter,
});

export default routerTestShardRouterLayer;
