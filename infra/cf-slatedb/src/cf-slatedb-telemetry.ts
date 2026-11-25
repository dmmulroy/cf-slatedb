import * as Cloudflare from "@alchemy.run/cloudflare";
import { isLocalLsmStage, lsmTelemetryLayer } from "@cf-slatedb/lsm-cloudflare";
import * as Alchemy from "alchemy";
import * as Output from "alchemy/Output";
import { Stage } from "alchemy/Stage";
import * as Telemetry from "alchemy/Telemetry";
import { Effect, Layer, Redacted } from "effect";

import { CF_SLATEDB_OTLP_BASE_URL } from "./cf-slatedb-stage.ts";
import { TraceCollectorToken, TraceCollectorWorker } from "./trace-collector-worker.ts";

/**
 * Where the Stack's Workers and Durable Objects export spans, by stage (an `LsmWorkerTelemetry`).
 *
 * **Details**
 *
 * Local stages export to {@link CF_SLATEDB_OTLP_BASE_URL}: the trace CLI's receiver by default.
 * Live stages export to the stage's own {@link TraceCollectorWorker} with its
 * {@link TraceCollectorToken} as a bearer token: Alchemy deploys the collector before the Workers
 * that export to it and binds its URL and the token onto each of them (the token as a secret). At
 * runtime the exporter reads those bindings back, so nothing is yielded then.
 */
export const cfSlatedbTelemetry = (serviceName: string) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const stage = yield* Stage;
      if (isLocalLsmStage(stage)) {
        return lsmTelemetryLayer({ baseUrl: CF_SLATEDB_OTLP_BASE_URL, serviceName });
      }
      if ((yield* Alchemy.ALCHEMY_PHASE) === "runtime") {
        return Telemetry.layerOtlp({ serviceName });
      }
      // A reference, not a declaration: the Stack declares the collector on live stages only, and
      // Alchemy binds this reference to it (the collector deploys first, like a direct reference).
      const collector = yield* Cloudflare.Worker.ref(TraceCollectorWorker);
      const token = yield* TraceCollectorToken;
      return Telemetry.layerOtlp({
        traces: {
          url: Output.map(collector.url, (url) => `${String(url).replace(/\/$/, "")}/v1/traces`),
          headers: {
            authorization: Output.map(token, (secret) =>
              Redacted.make(`Bearer ${Redacted.value(secret)}`),
            ),
          },
        },
        serviceName,
      });
    }),
  );
