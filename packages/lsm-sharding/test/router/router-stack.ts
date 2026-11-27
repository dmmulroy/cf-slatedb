import { BlockServerWorker, CompactorWorker, DbApiWorker } from "@shaledb/lsm-cloudflare";
import { Effect } from "effect";

import { ShardRouterWorker } from "../../src/shard-router-worker.ts";
import testApiLayer from "./api-entry.ts";
import testBlockServerLayer from "./block-server-entry.ts";
import testCompactorLayer from "./compactor-entry.ts";
import testShardRouterLayer from "./shard-router-entry.ts";

/**
 * Stack body for the sharded API's workerd test: the writer stack's API Worker (with every
 * database's `WriterServer`), its `BlockServer` and compactor Workers, and the shard router Worker
 * (with every group's `ShardDirectoryServer`), which reaches the databases over its bindings.
 */
export const routerTestStack = Effect.gen(function* () {
  yield* BlockServerWorker;
  yield* CompactorWorker;
  const api = yield* DbApiWorker;
  const router = yield* ShardRouterWorker;
  return { url: router.url, apiUrl: api.url };
}).pipe(
  Effect.provide([testApiLayer, testBlockServerLayer, testCompactorLayer, testShardRouterLayer]),
);
