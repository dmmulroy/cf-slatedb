import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  BlockServerWorker,
  CompactorWorker,
  DbApiWorker,
  GcWorker,
} from "@cf-slatedb/lsm-cloudflare";
import * as Alchemy from "alchemy";
import { ShardRouterWorker } from "@cf-slatedb/sharding";
import { Effect } from "effect";

import { cfSlatedbBlockServerWorkerLayer } from "./src/block-server-worker-entry.ts";
import { cfSlatedbCompactorWorkerLayer } from "./src/compactor-worker-entry.ts";
import { cfSlatedbApiWorkerLayer } from "./src/db-api-worker-entry.ts";
import { cfSlatedbGcWorkerLayer } from "./src/gc-worker-entry.ts";
import { cfSlatedbShardRouterWorkerLayer } from "./src/shard-router-worker-entry.ts";
import { cfSlatedbStateLayer } from "./src/cf-slatedb-stage.ts";
import { TelemetryProbeWorker, telemetryProbeWorkerLayer } from "./src/telemetry-probe-worker.ts";

/**
 * Stack that runs cf-slatedb.
 *
 * **Details**
 *
 * Local first: `vp run dev` runs it under Alchemy's local providers (stage `dev`: local R2, Durable
 * Objects, and Workflows) with local state. Live stages (the approved previews `preview-1`,
 * `preview-2`, `preview-3`) keep their state in the account's Cloudflare state store, so any machine
 * can redeploy them (`cfSlatedbStateLayer`, decision D43).
 */
const CfSlatedbStack = Alchemy.Stack(
  "CfSlatedb",
  { providers: Cloudflare.providers(), state: cfSlatedbStateLayer },
  Effect.gen(function* () {
    yield* BlockServerWorker;
    yield* CompactorWorker;
    yield* GcWorker;
    const api = yield* DbApiWorker;
    const shards = yield* ShardRouterWorker;
    const probe = yield* TelemetryProbeWorker;
    return {
      /** URL of the public database API (`/v1`). */
      apiUrl: api.url,
      /** URL of the sharded API (the sharding package's example Worker). */
      shardsUrl: shards.url,
      /** URL of the telemetry probe Worker. */
      probeUrl: probe.url,
    };
  }).pipe(
    Effect.provide([
      cfSlatedbApiWorkerLayer,
      cfSlatedbBlockServerWorkerLayer,
      cfSlatedbCompactorWorkerLayer,
      cfSlatedbGcWorkerLayer,
      cfSlatedbShardRouterWorkerLayer,
      telemetryProbeWorkerLayer,
    ]),
  ),
);

export default CfSlatedbStack;
