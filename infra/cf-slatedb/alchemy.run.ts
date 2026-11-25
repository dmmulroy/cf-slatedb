import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  BlockServerWorker,
  CompactorWorker,
  DbApiWorker,
  GcWorker,
} from "@cf-slatedb/lsm-cloudflare";
import * as Alchemy from "alchemy";
import { localState } from "alchemy/State";
import { Effect } from "effect";

import { cfSlatedbBlockServerWorkerLayer } from "./src/block-server-worker-entry.ts";
import { cfSlatedbCompactorWorkerLayer } from "./src/compactor-worker-entry.ts";
import { cfSlatedbApiWorkerLayer } from "./src/db-api-worker-entry.ts";
import { cfSlatedbGcWorkerLayer } from "./src/gc-worker-entry.ts";
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
    yield* BlockServerWorker;
    yield* CompactorWorker;
    yield* GcWorker;
    const api = yield* DbApiWorker;
    const probe = yield* TelemetryProbeWorker;
    return {
      /** URL of the public database API (`/v1`). */
      apiUrl: api.url,
      /** URL of the telemetry probe Worker. */
      probeUrl: probe.url,
    };
  }).pipe(
    Effect.provide([
      cfSlatedbApiWorkerLayer,
      cfSlatedbBlockServerWorkerLayer,
      cfSlatedbCompactorWorkerLayer,
      cfSlatedbGcWorkerLayer,
      telemetryProbeWorkerLayer,
    ]),
  ),
);

export default CfSlatedbStack;
