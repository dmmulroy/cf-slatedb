import { Data, Effect, Option } from "effect";

/**
 * Environment variable holding a live stage's trace collector token for `--collector-url`. The
 * token never goes on the command line, where it would stay in shell history and the process list.
 */
export const TRACE_TOKEN_VARIABLE = "SHALEDB_TRACE_TOKEN";

/** A live stage's trace collector: what a pull reads from, and how it was named. */
export interface TraceCollector {
  /**
   * Names the collector in messages and its pull cursor file: the preview stage for `--stage`, the
   * URL's host for `--collector-url` (see {@link collectorNameOfUrl}).
   */
  readonly name: string;
  readonly url: string;
  /** The collector's bearer token (`TraceCollectorToken`). Never printed. */
  readonly token: string;
  /** The preview stage it was resolved from, or `None` when it was named by its URL. */
  readonly stage: Option.Option<string>;
}

/**
 * The name of a collector given by URL: its host, with anything but letters, digits, `.`, and `-`
 * replaced by `_` (a port's `:`), so it is a safe file name. A preview stage's name has no `.`, so
 * the two never coincide.
 */
export const collectorNameOfUrl = (url: URL) => url.host.replace(/[^A-Za-z0-9.-]/g, "_");

/**
 * The trace CLI's collector options do not name one collector, checked before any request.
 *
 * `reason` is `CollectorNotNamed` when neither `--stage` nor `--collector-url` was given to a
 * command that needs one, `CollectorNamedTwice` when both were, `CollectorUrlInvalid` when
 * `--collector-url` is not an http or https URL, and `TraceTokenMissing` when `--collector-url` was
 * given without a non-empty `SHALEDB_TRACE_TOKEN`.
 */
export class TraceCollectorOptionError extends Data.TaggedError("TraceCollectorOptionError")<{
  readonly reason:
    | "CollectorNotNamed"
    | "CollectorNamedTwice"
    | "CollectorUrlInvalid"
    | "TraceTokenMissing";
  readonly message: string;
}> {}

/** How the options name a collector: a preview stage (resolved later) or a URL with its token. */
export type TraceCollectorSelection =
  | { readonly _tag: "Stage"; readonly stage: string }
  | { readonly _tag: "Url"; readonly collector: TraceCollector };

/** The trace CLI's collector options, as parsed. */
export interface TraceCollectorOptions {
  readonly stage: Option.Option<string>;
  readonly collectorUrl: Option.Option<string>;
}

const BOTH_FORMS = `--stage <preview-n> for a preview, or --collector-url <url> with ${TRACE_TOKEN_VARIABLE} set for any live stage (production: its traceCollectorUrl output)`;

/**
 * The collector `options` name, or `None` when they name none. `--collector-url` takes its token
 * from `environment`'s `SHALEDB_TRACE_TOKEN`; nothing is requested here.
 *
 * @returns The selection, or `TraceCollectorOptionError` (`CollectorNamedTwice`,
 *   `CollectorUrlInvalid`, or `TraceTokenMissing`).
 */
export const selectTraceCollector = Effect.fn("TraceCollectorOption.selectTraceCollector")(
  function* (
    options: TraceCollectorOptions,
    environment: Readonly<Record<string, string | undefined>>,
  ) {
    if (Option.isSome(options.stage) && Option.isSome(options.collectorUrl)) {
      return yield* new TraceCollectorOptionError({
        reason: "CollectorNamedTwice",
        message: `Both --stage and --collector-url name a trace collector. Pass one: ${BOTH_FORMS}.`,
      });
    }
    if (Option.isSome(options.stage)) {
      return Option.some<TraceCollectorSelection>({ _tag: "Stage", stage: options.stage.value });
    }
    if (Option.isNone(options.collectorUrl)) return Option.none<TraceCollectorSelection>();
    const url = parseCollectorUrl(options.collectorUrl.value);
    if (Option.isNone(url)) {
      // The value is not echoed: a token pasted into the wrong option must not be printed.
      return yield* new TraceCollectorOptionError({
        reason: "CollectorUrlInvalid",
        message:
          "--collector-url is not an http or https URL. Pass the stage's traceCollectorUrl output (https://…workers.dev).",
      });
    }
    const token = (environment[TRACE_TOKEN_VARIABLE] ?? "").trim();
    if (token === "") {
      return yield* new TraceCollectorOptionError({
        reason: "TraceTokenMissing",
        message: `${TRACE_TOKEN_VARIABLE} is not set or is empty, and --collector-url needs it. Set it to the stage's trace collector token without typing the token: export ${TRACE_TOKEN_VARIABLE}="$(node tools/preview-cli/src/preview-cli.ts trace-token <stage>)" (see docs/operations.md, "Telemetry").`,
      });
    }
    return Option.some<TraceCollectorSelection>({
      _tag: "Url",
      collector: {
        name: collectorNameOfUrl(url.value),
        url: url.value.toString(),
        token,
        stage: Option.none(),
      },
    });
  },
);

/**
 * The collector `options` name, which a command such as `pull` requires.
 *
 * @returns The selection, or `TraceCollectorOptionError`: `CollectorNotNamed` when the options name
 *   none, or any failure of {@link selectTraceCollector}.
 */
export const requireTraceCollector = Effect.fn("TraceCollectorOption.requireTraceCollector")(
  function* (
    options: TraceCollectorOptions,
    environment: Readonly<Record<string, string | undefined>>,
  ) {
    const selection = yield* selectTraceCollector(options, environment);
    if (Option.isNone(selection)) {
      return yield* new TraceCollectorOptionError({
        reason: "CollectorNotNamed",
        message: `No trace collector was named. Pass ${BOTH_FORMS}.`,
      });
    }
    return selection.value;
  },
);

const parseCollectorUrl = (text: string): Option.Option<URL> => {
  if (!URL.canParse(text)) return Option.none();
  const url = new URL(text);
  return url.protocol === "https:" || url.protocol === "http:" ? Option.some(url) : Option.none();
};
