import { shardRouterWorkerLayer } from "@cf-slatedb/sharding";

import { CF_SLATEDB_OTLP_BASE_URL } from "./cf-slatedb-stage.ts";

/** Local dev port of the shard router Worker. */
export const SHARD_ROUTER_DEV_PORT = 1341;

/**
 * The shard router Worker of the Stack (the optional sharding package's example consumer): every
 * group's `ShardDirectoryServer` and the sharded API. This module is its bundle entry.
 */
export const cfSlatedbShardRouterWorkerLayer = shardRouterWorkerLayer({
  main: import.meta.url,
  otlpBaseUrl: CF_SLATEDB_OTLP_BASE_URL,
  devPort: SHARD_ROUTER_DEV_PORT,
});

export default cfSlatedbShardRouterWorkerLayer;
