import { Stage } from "alchemy/Stage";
import * as Telemetry from "alchemy/Telemetry";
import { Effect, type Layer, Option } from "effect";
import { OtlpExporter } from "effect/observability";

import { isLocalLsmStage } from "./lsm-stage.ts";

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
 * alarm events export through it too. Wrap every handler with the wrapper from
 * {@link eventTelemetryFlushForStage}.
 */
export const lsmTelemetryLayer = (options: LsmTelemetryOptions): Layer.Layer<never> =>
  Telemetry.layerOtlp({
    traces: { url: `${options.baseUrl.replace(/\/$/, "")}/v1/traces` },
    serviceName: options.serviceName,
  });

/**
 * When an event's spans are exported.
 *
 * - `BeforeResponse`: the handler exports its spans before its result is returned, so they survive
 *   a runtime torn down right after the event. The response waits for the export (at most ten
 *   seconds). For local stages: development, tests, and scenarios.
 * - `AfterResponse`: the handler adds no export latency; Alchemy's bridge exports the event's
 *   telemetry in `waitUntil` after the response. For live stages.
 */
export type EventTelemetryFlushPolicy = "BeforeResponse" | "AfterResponse";

/**
 * Selects the flush policy for an Alchemy stage: `BeforeResponse` for local stages (`dev`, `test`,
 * `local`, and `dev-*`, `test-*`, `local-*`), `AfterResponse` for every other stage, so a live
 * stage never adds export latency to its responses.
 */
export const eventTelemetryFlushPolicyForStage = (stage: string): EventTelemetryFlushPolicy =>
  isLocalLsmStage(stage) ? "BeforeResponse" : "AfterResponse";

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

/** Wraps an event handler with an {@link EventTelemetryFlushPolicy}. */
export type EventTelemetryFlush = <A, E, R>(
  handler: Effect.Effect<A, E, R>,
) => Effect.Effect<A, E, R>;

/**
 * Returns the handler wrapper for a flush policy.
 *
 * **Details**
 *
 * Works around alchemy 2.0.0-beta.87, whose bridges export an event's telemetry only inside
 * `waitUntil` after the event settles (see `docs/design-decisions.md` D5). With `BeforeResponse`,
 * every span that ends inside the handler, including outbound HTTP client spans, is exported before
 * the handler's result is returned; the bridge's own HTTP server span ends after the handler, so it
 * still goes out through the bridge's flush. With `AfterResponse` the wrapper returns the handler
 * unchanged.
 *
 * Apply it outermost, after `Effect.withSpan`, so the handler's own span has ended when the flush
 * runs.
 */
export const withEventTelemetryFlush = (policy: EventTelemetryFlushPolicy): EventTelemetryFlush =>
  policy === "BeforeResponse"
    ? (handler) => handler.pipe(Effect.ensuring(flushEventTelemetry))
    : (handler) => handler;

/**
 * The handler wrapper for the current Alchemy stage. Yield it in a Worker's or Durable Object's
 * init Effect, where Alchemy provides `Stage` both at plan time and at runtime.
 */
export const eventTelemetryFlushForStage: Effect.Effect<EventTelemetryFlush, never, Stage> =
  Effect.gen(function* () {
    const stage = yield* Stage;
    return withEventTelemetryFlush(eventTelemetryFlushPolicyForStage(stage));
  });
