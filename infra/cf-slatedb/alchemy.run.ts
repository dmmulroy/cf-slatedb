import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import { localState } from "alchemy/State";
import { Effect } from "effect";

import { TelemetryProbeWorker, telemetryProbeWorkerLayer } from "./src/telemetry-probe-worker.ts";

/**
 * Stack that runs cf-slatedb.
 *
 * **Details**
 *
 * Local first: `vp run dev` runs it under Alchemy's local providers (stage `dev`). State is the
 * default local state store, because no live stage exists yet; a live stage needs Dillon's
 * approval (see `docs/progress.md`).
 */
const CfSlatedbStack = Alchemy.Stack(
  "CfSlatedb",
  { providers: Cloudflare.providers(), state: localState() },
  Effect.gen(function* () {
    const probe = yield* TelemetryProbeWorker;
    return {
      /** URL of the telemetry probe Worker. */
      probeUrl: probe.url,
    };
  }).pipe(Effect.provide(telemetryProbeWorkerLayer)),
);

export default CfSlatedbStack;
