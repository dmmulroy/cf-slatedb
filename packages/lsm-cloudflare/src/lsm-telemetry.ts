import * as Telemetry from "alchemy/Telemetry";
import type { Layer } from "effect";

/** Default OTLP/HTTP base URL in local development: the trace CLI's receiver. */
export const LOCAL_OTLP_BASE_URL = "http://127.0.0.1:4318";

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
 * alarm events export through it too.
 */
export const lsmTelemetryLayer = (options: LsmTelemetryOptions): Layer.Layer<never> =>
  Telemetry.layerOtlp({
    traces: { url: `${options.baseUrl.replace(/\/$/, "")}/v1/traces` },
    serviceName: options.serviceName,
  });
