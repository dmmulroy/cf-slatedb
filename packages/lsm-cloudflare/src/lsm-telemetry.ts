import * as Telemetry from "alchemy/Telemetry";
import { Effect, type Layer, Option } from "effect";
import { OtlpExporter } from "effect/observability";

/** Default OTLP/HTTP base URL in local development: the trace CLI's receiver. */
export const LOCAL_OTLP_BASE_URL = "http://127.0.0.1:4318";

/** Upper bound on how long a handler waits for its spans to export before it returns. */
const EVENT_TELEMETRY_FLUSH_TIMEOUT = "10 seconds";

/** Options for {@link lsmTelemetryLayer}. */
export interface LsmTelemetryOptions {
  /** OTLP/HTTP base URL; `/v1/traces` is appended. Bound onto the Worker at deploy time. */
  readonly baseUrl: string;
  /** `service.name` the Worker's spans carry, such as `cf-slatedb-api`. */
  readonly serviceName: string;
}

/**
 * Telemetry Layer every cf-slatedb Worker provides on its init Effect: OTLP/HTTP trace export to
 * `baseUrl`, built per event by the Alchemy runtime bridges.
 *
 * **Details**
 *
 * Durable Object classes hosted by the same Worker script share the binding, so their fetch and
 * alarm events export through it too. Wrap every handler with {@link withEventTelemetryFlush}.
 */
export const lsmTelemetryLayer = (options: LsmTelemetryOptions): Layer.Layer<never> =>
  Telemetry.layerOtlp({
    traces: { url: `${options.baseUrl.replace(/\/$/, "")}/v1/traces` },
    serviceName: options.serviceName,
  });

/**
 * Exports the current event's buffered spans now, waiting at most ten seconds.
 *
 * **Details**
 *
 * Reads the event's `OtlpExporter.Flusher`, which the Alchemy bridge builds into every event's
 * context when a telemetry Layer is configured; without one it does nothing. It never fails.
 */
export const flushEventTelemetry: Effect.Effect<void> = Effect.serviceOption(
  OtlpExporter.Flusher,
).pipe(
  Effect.flatMap(
    Option.match({
      onNone: () => Effect.void,
      onSome: (flusher) =>
        flusher.flush.pipe(Effect.timeoutOption(EVENT_TELEMETRY_FLUSH_TIMEOUT), Effect.asVoid),
    }),
  ),
);

/**
 * Runs an event handler (Worker or Durable Object `fetch`, `alarm`, a Workflow run, a cron event)
 * and then exports its spans before the handler's result is returned.
 *
 * **Details**
 *
 * Works around alchemy 2.0.0-beta.87, whose bridges export an event's telemetry only inside
 * `waitUntil` after the event settles, so a runtime torn down right after the event loses it (see
 * `../alchemy-workerd-otlp-flush-repro` and `docs/design-decisions.md` D5). Every span that ends
 * inside `handler`, including outbound HTTP client spans, is exported before the event settles.
 * The bridge's own HTTP server span ends after the handler returns, so it still goes out through
 * the bridge's `waitUntil` flush.
 *
 * Apply it outermost, after `Effect.withSpan`, so the handler's own span has ended when the flush
 * runs. The response waits for the export.
 */
export const withEventTelemetryFlush = <A, E, R>(
  handler: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> => handler.pipe(Effect.ensuring(flushEventTelemetry));
