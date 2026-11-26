import { shardRouterWorkerLayer } from "@shaledb/sharding";

import { shaledbAccessTokens } from "./shaledb-access-tokens.ts";
import { shaledbTailConsumers, shaledbTelemetry } from "./shaledb-telemetry.ts";
import { shaledbWorkerSettings } from "./shaledb-stack-config.ts";

/** Local dev port of the shard router Worker. */
export const SHARD_ROUTER_DEV_PORT = 1341;

/**
 * The shard router Worker of the Stack (the optional sharding package's example consumer): every
 * group's `ShardDirectoryServer` and the sharded API. This module is its bundle entry.
 */
export const shaledbShardRouterWorkerLayer = shardRouterWorkerLayer({
  main: import.meta.url,
  telemetry: shaledbTelemetry,
  settings: shaledbWorkerSettings,
  accessTokens: shaledbAccessTokens,
  tailConsumers: shaledbTailConsumers,
  devPort: SHARD_ROUTER_DEV_PORT,
});

export default shaledbShardRouterWorkerLayer;
