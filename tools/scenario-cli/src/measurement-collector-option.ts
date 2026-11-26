import { Data, Effect, Option } from "effect";

import type { IMeasurementCollector } from "./measurements.ts";

/**
 * Environment variable holding a live stage's trace collector token for `--collector-url` (the
 * trace CLI reads the same one). The token never goes on the command line.
 */
export const TRACE_TOKEN_VARIABLE = "SHALEDB_TRACE_TOKEN";

/**
 * The options of `measure health` or `measure latency` do not name one trace collector, checked
 * before any request.
 *
 * `reason` is `CollectorNotNamed` when neither `--stage` nor `--collector-url` was given,
 * `CollectorNamedTwice` when both were, `CollectorUrlInvalid` when `--collector-url` is not an http
 * or https URL, and `TraceTokenMissing` when `--collector-url` was given without a non-empty
 * `SHALEDB_TRACE_TOKEN`.
 */
export class MeasurementCollectorOptionError extends Data.TaggedError(
  "MeasurementCollectorOptionError",
)<{
  readonly reason:
    | "CollectorNotNamed"
    | "CollectorNamedTwice"
    | "CollectorUrlInvalid"
    | "TraceTokenMissing";
  readonly message: string;
}> {}

/** How the options name a collector: a preview stage (resolved later) or a URL with its token. */
export type MeasurementCollectorSelection =
  | { readonly _tag: "Stage"; readonly stage: string }
  | { readonly _tag: "Url"; readonly collector: IMeasurementCollector };

/** The collector options of the telemetry measurements, as parsed. */
export interface MeasurementCollectorOptions {
  readonly stage: Option.Option<string>;
  readonly collectorUrl: Option.Option<string>;
}

const BOTH_FORMS = `--stage <preview-n> for a preview, or --collector-url <url> with ${TRACE_TOKEN_VARIABLE} set for any live stage (production: its traceCollectorUrl output)`;

/**
 * The collector `options` name. `--collector-url` takes its token from `environment`'s
 * `SHALEDB_TRACE_TOKEN` and is named by its host (with anything but letters, digits, `.`, and `-`
 * replaced by `_`), which has a `.` where a preview's name has none, so its measurement store is
 * never a preview's. Nothing is requested here.
 *
 * @returns The selection, or `MeasurementCollectorOptionError`.
 */
export const selectMeasurementCollector = Effect.fn(
  "MeasurementCollectorOption.selectMeasurementCollector",
)(function* (
  options: MeasurementCollectorOptions,
  environment: Readonly<Record<string, string | undefined>>,
) {
  if (Option.isSome(options.stage) && Option.isSome(options.collectorUrl)) {
    return yield* new MeasurementCollectorOptionError({
      reason: "CollectorNamedTwice",
      message: `Both --stage and --collector-url name a trace collector. Pass one: ${BOTH_FORMS}.`,
    });
  }
  if (Option.isSome(options.stage)) {
    const selection: MeasurementCollectorSelection = { _tag: "Stage", stage: options.stage.value };
    return selection;
  }
  if (Option.isNone(options.collectorUrl)) {
    return yield* new MeasurementCollectorOptionError({
      reason: "CollectorNotNamed",
      message: `No trace collector was named. Pass ${BOTH_FORMS}.`,
    });
  }
  const url = parseCollectorUrl(options.collectorUrl.value);
  if (Option.isNone(url)) {
    // The value is not echoed: a token pasted into the wrong option must not be printed.
    return yield* new MeasurementCollectorOptionError({
      reason: "CollectorUrlInvalid",
      message:
        "--collector-url is not an http or https URL. Pass the stage's traceCollectorUrl output (https://…workers.dev).",
    });
  }
  const token = (environment[TRACE_TOKEN_VARIABLE] ?? "").trim();
  if (token === "") {
    return yield* new MeasurementCollectorOptionError({
      reason: "TraceTokenMissing",
      message: `${TRACE_TOKEN_VARIABLE} is not set or is empty, and --collector-url needs it. Set it to the stage's trace collector token without typing the token: export ${TRACE_TOKEN_VARIABLE}="$(node tools/preview-cli/src/preview-cli.ts trace-token <stage>)" (see docs/operations.md, "Telemetry").`,
    });
  }
  const selection: MeasurementCollectorSelection = {
    _tag: "Url",
    collector: {
      stage: url.value.host.replace(/[^A-Za-z0-9.-]/g, "_"),
      url: url.value.toString(),
      token,
    },
  };
  return selection;
});

const parseCollectorUrl = (text: string): Option.Option<URL> => {
  if (!URL.canParse(text)) return Option.none();
  const url = new URL(text);
  return url.protocol === "https:" || url.protocol === "http:" ? Option.some(url) : Option.none();
};
