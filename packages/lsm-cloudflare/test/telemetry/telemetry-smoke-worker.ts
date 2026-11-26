import * as Cloudflare from "@alchemy.run/cloudflare";
import type { HttpEffect } from "alchemy/Http";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect } from "effect";
import { HttpClientRequest, HttpServerRequest, HttpServerResponse } from "effect/http";

import { eventTelemetryFlushForStage, lsmTelemetryLayer } from "../../src/lsm-telemetry.ts";

/** Port of the delayed OTLP collector the telemetry smoke test starts. */
export const TELEMETRY_SMOKE_COLLECTOR_PORT = 54329;

/** Port the telemetry smoke Worker listens on under local workerd. */
export const TELEMETRY_SMOKE_WORKER_PORT = 8791;

interface TelemetrySmokeServerContract {
  readonly fetch: HttpEffect<RuntimeContext>;
  readonly alarm: () => Effect.Effect<void, never, RuntimeContext>;
}

/**
 * Durable Object for the telemetry smoke test: its fetch emits a child span and, on `/alarm`,
 * schedules an alarm whose event emits its own span.
 */
export class TelemetrySmokeServer extends Cloudflare.DurableObject<
  TelemetrySmokeServer,
  TelemetrySmokeServerContract
>()("TelemetrySmokeServer") {}

/** Durable Object Layer for {@link TelemetrySmokeServer}. */
export const telemetrySmokeServerLayer = TelemetrySmokeServer.make(
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
    return Effect.succeed({
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (request.url.endsWith("/alarm")) {
          yield* state.storage.setAlarm(Date.now() + 100);
        }
        return HttpServerResponse.text("durable-object-ok");
      }).pipe(Effect.withSpan("TelemetrySmokeServer.fetch"), withEventTelemetryFlush),
      alarm: () =>
        Effect.void.pipe(Effect.withSpan("TelemetrySmokeServer.alarm"), withEventTelemetryFlush),
    });
  }),
);

/** Worker for the telemetry smoke test: one traced HTTP call to {@link TelemetrySmokeServer}. */
export class TelemetrySmokeWorker extends Cloudflare.Worker<
  TelemetrySmokeWorker,
  { readonly fetch: HttpEffect },
  TelemetrySmokeServer
>()("TelemetrySmokeWorker") {}

/** Worker Layer for {@link TelemetrySmokeWorker}, exporting spans to the smoke test's collector. */
export const telemetrySmokeWorkerLayer = TelemetrySmokeWorker.make(
  {
    main: import.meta.url,
    compatibility: { date: "2026-10-15" },
    dev: { host: "127.0.0.1", port: TELEMETRY_SMOKE_WORKER_PORT, strictPort: true },
  },
  Effect.gen(function* () {
    const namespace = yield* TelemetrySmokeServer;
    const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const client = Cloudflare.toHttpClient(namespace.getByName("smoke"));
        const path = request.url.endsWith("/alarm") ? "/alarm" : "/";
        const response = yield* client.execute(HttpClientRequest.get(`http://smoke${path}`));
        const body = yield* response.text;
        return HttpServerResponse.text(`worker-saw:${body}`);
      }).pipe(
        Effect.catchCause(() =>
          Effect.succeed(HttpServerResponse.text("worker-failed", { status: 500 })),
        ),
        withEventTelemetryFlush,
      ),
    };
  }).pipe(
    Effect.provide(telemetrySmokeServerLayer),
    Effect.provide(
      lsmTelemetryLayer({
        baseUrl: `http://127.0.0.1:${TELEMETRY_SMOKE_COLLECTOR_PORT}`,
        serviceName: "shaledb-telemetry-smoke",
      }),
    ),
  ),
);

/** Stack body for the smoke test: the Worker and its Durable Object. */
export const telemetrySmokeStack = Effect.gen(function* () {
  const worker = yield* TelemetrySmokeWorker;
  return { url: worker.url };
}).pipe(Effect.provide(telemetrySmokeWorkerLayer));

export default telemetrySmokeWorkerLayer;
