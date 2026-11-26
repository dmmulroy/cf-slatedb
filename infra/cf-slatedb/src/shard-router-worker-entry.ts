import { shardRouterWorkerLayer } from "@cf-slatedb/sharding";

import { cfSlatedbTelemetry } from "./cf-slatedb-telemetry.ts";
import { cfSlatedbWorkerSettings } from "./cf-slatedb-stack-config.ts";

/** Local dev port of the shard router Worker. */
export const SHARD_ROUTER_DEV_PORT = 1341;

/**
 * The shard router Worker of the Stack (the optional sharding package's example consumer): every
 * group's `ShardDirectoryServer` and the sharded API. This module is its bundle entry.
 */
export const cfSlatedbShardRouterWorkerLayer = shardRouterWorkerLayer({
  main: import.meta.url,
  telemetry: cfSlatedbTelemetry,
  settings: cfSlatedbWorkerSettings,
  devPort: SHARD_ROUTER_DEV_PORT,
});

export default cfSlatedbShardRouterWorkerLayer;
