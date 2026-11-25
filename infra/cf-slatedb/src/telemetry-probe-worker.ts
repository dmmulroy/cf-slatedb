import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  eventTelemetryFlushForStage,
  lsmAccessGateForStage,
  lsmWorkerCompatibility,
} from "@cf-slatedb/lsm-cloudflare";
import type { HttpEffect } from "alchemy/Http";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect } from "effect";
import { HttpClientRequest, HttpServerRequest, HttpServerResponse } from "effect/http";

import { cfSlatedbTelemetry } from "./cf-slatedb-telemetry.ts";

interface TelemetryProbeServerContract {
  readonly fetch: HttpEffect<RuntimeContext>;
  readonly alarm: () => Effect.Effect<void, never, RuntimeContext>;
}

/**
 * Durable Object that proves span export from Durable Object fetch and alarm events under
 * `alchemy dev`: `GET /v1/probe/alarm` schedules an alarm whose event emits its own span.
 */
export class TelemetryProbeServer extends Cloudflare.DurableObject<
  TelemetryProbeServer,
  TelemetryProbeServerContract
>()("TelemetryProbeServer") {}

const telemetryProbeServerLayer = TelemetryProbeServer.make(
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
    return Effect.succeed({
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (request.url.endsWith("/alarm")) yield* state.storage.setAlarm(Date.now() + 50);
        return HttpServerResponse.text("probe-ok");
      }).pipe(Effect.withSpan("TelemetryProbeServer.fetch"), withEventTelemetryFlush),
      alarm: () =>
        Effect.void.pipe(Effect.withSpan("TelemetryProbeServer.alarm"), withEventTelemetryFlush),
    });
  }),
);

/**
 * Worker serving `GET /v1/probe` and `GET /v1/probe/alarm`, each a traced HTTP call to
 * {@link TelemetryProbeServer}, so the trace CLI can check Worker → Durable Object propagation.
 */
export class TelemetryProbeWorker extends Cloudflare.Worker<
  TelemetryProbeWorker,
  { readonly fetch: HttpEffect },
  TelemetryProbeServer
>()("TelemetryProbeWorker") {}

/** Worker Layer for {@link TelemetryProbeWorker}. */
export const telemetryProbeWorkerLayer = TelemetryProbeWorker.make(
  Effect.gen(function* () {
    return {
      main: import.meta.url,
      compatibility: yield* lsmWorkerCompatibility,
      dev: { host: "127.0.0.1", port: 8790 },
    };
  }),
  Effect.gen(function* () {
    const namespace = yield* TelemetryProbeServer;
    const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
    const withAccessGate = yield* lsmAccessGateForStage;
    return {
      fetch: withAccessGate(
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          const client = Cloudflare.toHttpClient(namespace.getByName("probe"));
          const path = request.url.endsWith("/alarm") ? "/alarm" : "/";
          const response = yield* client.execute(HttpClientRequest.get(`http://probe${path}`));
          return HttpServerResponse.text(`worker-saw:${yield* response.text}`);
        }).pipe(
          Effect.catchCause(() =>
            Effect.succeed(HttpServerResponse.text("probe-failed", { status: 500 })),
          ),
        ),
      ).pipe(withEventTelemetryFlush),
    };
  }).pipe(
    Effect.provide(telemetryProbeServerLayer),
    Effect.provide(cfSlatedbTelemetry("cf-slatedb-telemetry-probe")),
  ),
);

export default telemetryProbeWorkerLayer;
