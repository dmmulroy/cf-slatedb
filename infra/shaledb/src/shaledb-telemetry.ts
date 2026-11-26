import * as Cloudflare from "@alchemy.run/cloudflare";
import { isLocalLsmStage, lsmTelemetryLayer } from "@shaledb/lsm-cloudflare";
import * as Alchemy from "alchemy";
import * as Output from "alchemy/Output";
import { Stage } from "alchemy/Stage";
import * as Telemetry from "alchemy/Telemetry";
import { Effect, Layer, Redacted } from "effect";

import { SHALEDB_OTLP_BASE_URL } from "./shaledb-stage.ts";
import { TraceCollectorToken, TraceCollectorWorker } from "./trace-collector-worker.ts";

/**
 * Where the Stack's Workers and Durable Objects export spans, by stage (an `LsmWorkerTelemetry`).
 *
 * **Details**
 *
 * Local stages export to {@link SHALEDB_OTLP_BASE_URL}: the trace CLI's receiver by default.
 * Live stages export to the stage's own {@link TraceCollectorWorker} with its
 * {@link TraceCollectorToken} as a bearer token: Alchemy deploys the collector before the Workers
 * that export to it and binds its URL and the token onto each of them (the token as a secret). At
 * runtime the exporter reads those bindings back, so nothing is yielded then.
 */
export const shaledbTelemetry = (serviceName: string) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const stage = yield* Stage;
      if (isLocalLsmStage(stage)) {
        return lsmTelemetryLayer({ baseUrl: SHALEDB_OTLP_BASE_URL, serviceName });
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

/** Logical id of the live stages' Tail Worker (`cpu-tail-worker.ts`), declared by the Stack. */
export const CPU_TAIL_WORKER_ID = "CpuTailWorker";

/**
 * The Tail Workers of the API and compactor Workers, by stage: none locally; on live stages the
 * Stack's `CpuTailWorker`, which records each invocation's CPU and wall time (D48). Referenced, not
 * declared, like the trace collector.
 */
export const shaledbTailConsumers = Effect.gen(function* () {
  const stage = yield* Stage;
  if (isLocalLsmStage(stage)) return [];
  if ((yield* Alchemy.ALCHEMY_PHASE) === "runtime") return [];
  return [yield* Cloudflare.Worker.ref(CPU_TAIL_WORKER_ID)];
});
