/**
 * Trace CLI: a local OTLP/HTTP receiver and trace store, and commands to inspect, compare, and
 * assert on the traces shaledb's Workers and Durable Objects export.
 *
 * Usage: `node tools/trace-cli/src/trace-cli.ts <command> [options]`. Every command accepts
 * `--store <dir>` (default `.traces/` at the repository root, or `TRACE_STORE_DIR`) and `--json`
 * for machine-readable output. `pull --stage <stage>` downloads a live preview stage's spans from
 * its trace collector into the store, and `tail --stage <stage>` follows them live; every other
 * command then works on them unchanged.
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Clock, Console, Data, Effect, FileSystem, Layer, Option, Schema } from "effect";
import { Argument, Command, Flag } from "effect/cli";
import { FetchHttpClient } from "effect/http";

import { CollectedSpan } from "./collected-span.ts";
import {
  DEFAULT_OTLP_RECEIVER_HOST,
  DEFAULT_OTLP_RECEIVER_PORT,
  otlpReceiverLayer,
} from "./otlp-receiver.ts";
import {
  diffSpans,
  matchesSpanFilter,
  renderSpanTree,
  type SpanFilter,
  spanStats,
  insertNewestTraceSummary,
  summarizeTrace,
  type TraceSummary,
} from "./trace-analysis.ts";
import { pullPreviewTraces, resolvePreviewCollector } from "./preview-trace-pull.ts";
import { decodeTraceAssertionJson, evaluateTraceAssertion } from "./trace-assertion.ts";
import { DEFAULT_TRACE_STORE_DIRECTORY, TraceStore, traceStoreLayer } from "./trace-store.ts";

/**
 * Failure the trace CLI reports to the person running it, exiting non-zero.
 *
 * `reason` is `AssertionFailed` when an assertion file's checks did not all pass,
 * `AssertionFileInvalid` when the file is not a valid assertion, and `SelectorInvalid` when a
 * `diff` selector is neither a trace id nor a `run:<startMs>-<endMs>` range.
 */
class TraceCliError extends Data.TaggedError("TraceCliError")<{
  readonly reason: "AssertionFailed" | "AssertionFileInvalid" | "SelectorInvalid";
  readonly message: string;
}> {}

const storeFlag = Flag.String("store").pipe(
  Flag.withDescription("Trace store directory (default .traces/ at the repository root)"),
  Flag.withDefault(process.env["TRACE_STORE_DIR"] ?? DEFAULT_TRACE_STORE_DIRECTORY),
);
const jsonFlag = Flag.Boolean("json").pipe(
  Flag.withDescription("Print machine-readable JSON"),
  Flag.withDefault(false),
);
const dbFlag = Flag.String("db").pipe(
  Flag.withDescription("Only spans or traces with this database.id attribute"),
  Flag.optional,
);
const stageFlag = Flag.String("stage").pipe(
  Flag.withDescription("Live preview stage (preview-1, preview-2, preview-3) to pull spans from"),
  Flag.optional,
);
const sinceFlag = Flag.Int("since").pipe(
  Flag.withDescription("Only traces that started at or after this Unix millisecond time"),
  Flag.optional,
);

const withStore = <A, E, R>(directory: string, effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(traceStoreLayer(directory)));

/** Reads every stored trace, most recently written first. */
const readAllTraces = Effect.gen(function* () {
  const store = yield* TraceStore;
  const ids = yield* store.listTraceIds;
  const traces = new Map<string, ReadonlyArray<CollectedSpan>>();
  for (const id of ids) traces.set(id, yield* store.readTrace(id));
  return traces;
});

const printJson = (value: unknown) => Console.log(JSON.stringify(value, null, 2));

const encodeCollectedSpans = Schema.encodeSync(Schema.Array(CollectedSpan));
const encodeCollectedSpan = Schema.encodeSync(CollectedSpan);

const formatTraceSummary = (summary: TraceSummary): string =>
  `${summary.traceId}  ${new Date(summary.startUnixMs).toISOString()}  ${summary.durationMs.toFixed(1)}ms  ${summary.spanCount} spans${summary.errorCount > 0 ? `  ${summary.errorCount} errors` : ""}  ${summary.rootName}  [${summary.serviceNames.join(", ")}]${summary.databaseIds.length > 0 ? `  db=${summary.databaseIds.join(",")}` : ""}`;

const serve = Command.make(
  "serve",
  {
    store: storeFlag,
    host: Flag.String("host").pipe(Flag.withDefault(DEFAULT_OTLP_RECEIVER_HOST)),
    port: Flag.Int("port").pipe(Flag.withDefault(DEFAULT_OTLP_RECEIVER_PORT)),
  },
  ({ store, host, port }) =>
    Effect.gen(function* () {
      yield* Console.log(
        `OTLP/HTTP receiver on http://${host}:${port}/v1/traces, storing spans in ${store}`,
      );
      return yield* Layer.launch(
        otlpReceiverLayer({ host, port }).pipe(Layer.provide(traceStoreLayer(store))),
      );
    }),
).pipe(
  Command.withDescription("Run the local OTLP/HTTP receiver (JSON and protobuf) and store spans"),
);

const list = Command.make(
  "list",
  {
    store: storeFlag,
    json: jsonFlag,
    db: dbFlag,
    since: sinceFlag,
    limit: Flag.Int("limit").pipe(Flag.withDefault(20)),
  },
  ({ store, json, db, since, limit }) =>
    withStore(
      store,
      Effect.gen(function* () {
        const traceStore = yield* TraceStore;
        // One pass over the stored traces: summarize each as it is read, drop it unless it passes
        // both filters, and keep only the newest `limit` in a bounded array sorted newest first.
        const newest: Array<TraceSummary> = [];
        for (const id of yield* traceStore.listTraceIds) {
          const summary = summarizeTrace(id, yield* traceStore.readTrace(id));
          if (Option.isSome(since) && summary.startUnixMs < since.value) continue;
          if (Option.isSome(db) && !summary.databaseIds.includes(db.value)) continue;
          insertNewestTraceSummary(newest, summary, limit);
        }
        if (json) return yield* printJson(newest);
        yield* Console.log(newest.map(formatTraceSummary).join("\n"));
      }),
    ),
).pipe(Command.withDescription("List recent traces, newest first"));

const show = Command.make(
  "show",
  { store: storeFlag, json: jsonFlag, traceId: Argument.String("traceId") },
  ({ store, json, traceId }) =>
    withStore(
      store,
      Effect.gen(function* () {
        const trace = yield* TraceStore;
        const spans = yield* trace.readTrace(traceId);
        if (json) {
          return yield* printJson({
            summary: summarizeTrace(traceId, spans),
            spans: encodeCollectedSpans(spans),
          });
        }
        yield* Console.log(renderSpanTree(spans));
      }),
    ),
).pipe(Command.withDescription("Show one trace as an indented span tree"));

const filterFlags = {
  name: Flag.String("name").pipe(Flag.withDescription("Exact span name"), Flag.optional),
  namePrefix: Flag.String("name-prefix").pipe(
    Flag.withDescription("Span name prefix"),
    Flag.optional,
  ),
  attributes: Flag.KeyValuePair("attr").pipe(
    Flag.withDescription("Attribute key=value the span must carry (repeatable)"),
    Flag.optional,
  ),
  error: Flag.Boolean("error").pipe(
    Flag.withDescription("Only spans with an error status"),
    Flag.withDefault(false),
  ),
  db: dbFlag,
};

const toSpanFilter = (flags: {
  readonly name: Option.Option<string>;
  readonly namePrefix: Option.Option<string>;
  readonly attributes: Option.Option<Record<string, string>>;
  readonly error: boolean;
  readonly db: Option.Option<string>;
}): SpanFilter => ({
  name: Option.getOrUndefined(flags.name),
  namePrefix: Option.getOrUndefined(flags.namePrefix),
  attributes: Option.getOrUndefined(flags.attributes),
  databaseId: Option.getOrUndefined(flags.db),
  error: flags.error ? true : undefined,
});

const grep = Command.make("grep", { store: storeFlag, json: jsonFlag, ...filterFlags }, (flags) =>
  withStore(
    flags.store,
    Effect.gen(function* () {
      const filter = toSpanFilter(flags);
      const traces = yield* readAllTraces;
      const matches = [...traces.values()].flat().filter((span) => matchesSpanFilter(filter, span));
      if (flags.json) return yield* printJson(encodeCollectedSpans(matches));
      for (const span of matches) {
        yield* Console.log(
          `${span.traceId} ${span.spanId} ${span.name} ${span.durationMs.toFixed(2)}ms ${span.status.code}${Option.match(span.status.message, { onNone: () => "", onSome: (message) => ` (${message})` })}`,
        );
      }
    }),
  ),
).pipe(Command.withDescription("Find spans by name, attribute, error status, or database id"));

const stats = Command.make(
  "stats",
  { store: storeFlag, json: jsonFlag, ...filterFlags, since: sinceFlag },
  (flags) =>
    withStore(
      flags.store,
      Effect.gen(function* () {
        const filter = toSpanFilter(flags);
        const traces = yield* readAllTraces;
        const spans = [...traces.entries()]
          .filter(
            ([id, traceSpans]) =>
              Option.isNone(flags.since) ||
              summarizeTrace(id, traceSpans).startUnixMs >= flags.since.value,
          )
          .flatMap(([, traceSpans]) => traceSpans)
          .filter((span) => matchesSpanFilter(filter, span));
        const rows = spanStats(spans);
        if (flags.json) return yield* printJson(rows);
        yield* Console.log("count  errors     p50ms     p90ms     p99ms     maxms  name");
        for (const row of rows) {
          yield* Console.log(
            `${String(row.count).padStart(5)}  ${String(row.errorCount).padStart(6)}  ${row.p50Ms.toFixed(2).padStart(8)}  ${row.p90Ms.toFixed(2).padStart(8)}  ${row.p99Ms.toFixed(2).padStart(8)}  ${row.maxMs.toFixed(2).padStart(8)}  ${row.name}`,
          );
        }
      }),
    ),
).pipe(Command.withDescription("Latency percentiles per span name"));

/**
 * Resolves a `diff` selector: a trace id, or `run:<startMs>-<endMs>` for every trace that started
 * in that Unix millisecond window.
 */
const selectSpans = (
  traces: ReadonlyMap<string, ReadonlyArray<CollectedSpan>>,
  selector: string,
): Effect.Effect<ReadonlyArray<CollectedSpan>, TraceCliError> => {
  const run = /^run:(\d+)-(\d+)$/.exec(selector);
  if (run !== null) {
    const start = Number(run[1]);
    const end = Number(run[2]);
    return Effect.succeed(
      [...traces.entries()]
        .filter(([id, spans]) => {
          const started = summarizeTrace(id, spans).startUnixMs;
          return started >= start && started <= end;
        })
        .flatMap(([, spans]) => spans),
    );
  }
  const spans = traces.get(selector);
  return spans === undefined
    ? Effect.fail(
        new TraceCliError({
          reason: "SelectorInvalid",
          message: `"${selector}" is neither a stored trace id nor a run:<startMs>-<endMs> range. Run \`list\` to see trace ids.`,
        }),
      )
    : Effect.succeed(spans);
};

const diff = Command.make(
  "diff",
  {
    store: storeFlag,
    json: jsonFlag,
    left: Argument.String("left"),
    right: Argument.String("right"),
  },
  ({ store, json, left, right }) =>
    withStore(
      store,
      Effect.gen(function* () {
        const traces = yield* readAllTraces;
        const rows = diffSpans(yield* selectSpans(traces, left), yield* selectSpans(traces, right));
        if (json) return yield* printJson(rows);
        yield* Console.log("left  right   left-mean  right-mean  name");
        for (const row of rows) {
          const marker = row.leftCount === row.rightCount ? " " : "*";
          yield* Console.log(
            `${marker}${String(row.leftCount).padStart(4)}  ${String(row.rightCount).padStart(5)}  ${row.leftMeanMs.toFixed(2).padStart(10)}  ${row.rightMeanMs.toFixed(2).padStart(10)}  ${row.name}`,
          );
        }
      }),
    ),
).pipe(Command.withDescription("Compare two traces or runs (run:<startMs>-<endMs>) by span name"));

const assert = Command.make(
  "assert",
  {
    store: storeFlag,
    json: jsonFlag,
    file: Argument.String("file").pipe(
      Argument.withDescription("Assertion JSON file, or - for stdin"),
    ),
  },
  ({ store, json, file }) =>
    withStore(
      store,
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const text =
          file === "-"
            ? yield* Effect.promise(async () => {
                const chunks: Array<Buffer> = [];
                for await (const chunk of process.stdin) chunks.push(Buffer.from(chunk));
                return Buffer.concat(chunks).toString("utf8");
              })
            : yield* fs.readFileString(file).pipe(
                Effect.mapError(
                  (error) =>
                    new TraceCliError({
                      reason: "AssertionFileInvalid",
                      message: `The assertion file ${file} could not be read: ${error.message}.`,
                    }),
                ),
              );
        const assertion = yield* decodeTraceAssertionJson(text).pipe(
          Effect.mapError(
            (error) =>
              new TraceCliError({
                reason: "AssertionFileInvalid",
                message: `The assertion in ${file} is not valid: ${error.message}`,
              }),
          ),
        );
        const report = evaluateTraceAssertion(assertion, yield* readAllTraces);
        if (json) {
          yield* printJson(report);
        } else {
          yield* Console.log(
            `${report.passed ? "PASS" : "FAIL"}: ${report.traceIds.length} trace(s), ${report.spanCount} span(s)`,
          );
          for (const result of report.results) {
            yield* Console.log(
              `  ${result.passed ? "✓" : "✗"} ${result.check._tag}: ${result.detail}`,
            );
          }
        }
        if (!report.passed) {
          return yield* new TraceCliError({
            reason: "AssertionFailed",
            message:
              report.traceIds.length === 0
                ? "No stored trace matches the assertion's selection."
                : "At least one trace check failed; see the report above.",
          });
        }
      }),
    ),
).pipe(
  Command.withDescription("Run declarative trace checks; exits non-zero when any check fails"),
);

const tail = Command.make(
  "tail",
  {
    store: storeFlag,
    json: jsonFlag,
    ...filterFlags,
    stage: stageFlag,
    interval: Flag.Int("interval").pipe(
      Flag.withDescription("Poll interval in milliseconds (at least 2000 with --stage)"),
      Flag.withDefault(250),
    ),
  },
  (flags) =>
    withStore(
      flags.store,
      Effect.gen(function* () {
        const store = yield* TraceStore;
        const filter = toSpanFilter(flags);
        const collector = Option.isSome(flags.stage)
          ? Option.some(yield* resolvePreviewCollector(flags.stage.value))
          : Option.none();
        // With a stage, each poll first pulls what its collector received since the last one.
        const pullOnce = Option.match(collector, {
          onNone: () => Effect.void,
          onSome: (target) =>
            pullPreviewTraces(target).pipe(
              Effect.catchTag("PreviewTracePullError", (error) => Console.error(error.message)),
              Effect.asVoid,
            ),
        });
        const interval = Option.isSome(collector) ? Math.max(flags.interval, 2000) : flags.interval;
        const seen = new Map<string, number>();
        for (const id of yield* store.listTraceIds)
          seen.set(id, (yield* store.readTrace(id)).length);
        yield* Effect.forever(
          Effect.gen(function* () {
            yield* pullOnce;
            for (const id of yield* store.listTraceIds) {
              const spans = yield* store.readTrace(id);
              const previous = seen.get(id) ?? 0;
              if (spans.length <= previous) continue;
              seen.set(id, spans.length);
              for (const span of spans
                .slice(previous)
                .filter((s) => matchesSpanFilter(filter, s))) {
                yield* flags.json
                  ? Console.log(JSON.stringify(encodeCollectedSpan(span)))
                  : Console.log(
                      `${span.traceId.slice(0, 8)} ${span.name} ${span.durationMs.toFixed(2)}ms [${span.serviceName}] ${span.status.code}`,
                    );
              }
            }
            yield* Effect.sleep(interval);
          }),
        );
      }).pipe(Effect.provide(FetchHttpClient.layer)),
    ),
).pipe(Command.withDescription("Follow spans as they arrive (from a preview stage with --stage)"));

const pull = Command.make(
  "pull",
  {
    store: storeFlag,
    json: jsonFlag,
    stage: Flag.String("stage").pipe(
      Flag.withDescription(
        "Live preview stage (preview-1, preview-2, preview-3) to pull spans from",
      ),
    ),
    sinceMinutes: Flag.Int("since-minutes").pipe(
      Flag.withDescription(
        "Only exports from the last N minutes (a fresh store otherwise pulls all)",
      ),
      Flag.optional,
    ),
  },
  ({ store, json, stage, sinceMinutes }) =>
    withStore(
      store,
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        const pulled = yield* pullPreviewTraces(
          yield* resolvePreviewCollector(stage),
          Option.map(sinceMinutes, (minutes) => now - minutes * 60_000),
        );
        yield* json
          ? Console.log(JSON.stringify(pulled))
          : Console.log(
              `Pulled ${pulled.exports} export(s), ${pulled.spans} span(s) from ${stage} into ${store}`,
            );
      }).pipe(Effect.provide(FetchHttpClient.layer)),
    ),
).pipe(
  Command.withDescription(
    "Download a preview stage's spans from its trace collector into the store (only what is new)",
  ),
);

const clear = Command.make("clear", { store: storeFlag }, ({ store }) =>
  withStore(
    store,
    Effect.gen(function* () {
      const trace = yield* TraceStore;
      yield* trace.clear;
      yield* Console.log(`Cleared ${trace.directory}`);
    }),
  ),
).pipe(Command.withDescription("Delete every stored trace"));

const traceCli = Command.make("trace").pipe(
  Command.withDescription("Local OTLP receiver and trace inspection for shaledb"),
  Command.withSubcommands([serve, pull, list, show, tail, grep, stats, diff, assert, clear]),
);

Command.run(traceCli, { version: "0.0.0" }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
