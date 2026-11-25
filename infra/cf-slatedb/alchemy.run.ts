import * as Cloudflare from "@alchemy.run/cloudflare";
import { DbApiWorker } from "@cf-slatedb/lsm-cloudflare";
import * as Alchemy from "alchemy";
import { localState } from "alchemy/State";
import { Effect } from "effect";

import { cfSlatedbApiWorkerLayer } from "./src/db-api-worker-entry.ts";
import { TelemetryProbeWorker, telemetryProbeWorkerLayer } from "./src/telemetry-probe-worker.ts";

/**
 * Stack that runs cf-slatedb.
 *
 * **Details**
 *
 * Local first: `vp run dev` runs it under Alchemy's local providers (stage `dev`: local R2, Durable
 * Objects, and Workflows). State is the local state store, because no live stage exists yet; a live
 * stage needs Dillon's approval (see `docs/progress.md`).
 */
const CfSlatedbStack = Alchemy.Stack(
  "CfSlatedb",
  { providers: Cloudflare.providers(), state: localState() },
  Effect.gen(function* () {
    const api = yield* DbApiWorker;
    const probe = yield* TelemetryProbeWorker;
    return {
      /** URL of the public database API (`/v1`). */
      apiUrl: api.url,
      /** URL of the telemetry probe Worker. */
      probeUrl: probe.url,
    };
  }).pipe(Effect.provide([cfSlatedbApiWorkerLayer, telemetryProbeWorkerLayer])),
);

export default CfSlatedbStack;
