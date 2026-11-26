/**
 * Scenario CLI: runs named end-to-end scenarios against the local Stack (`vp run dev`) or a live
 * preview stage and checks both their results and the traces they produce (through the trace CLI's
 * `assert`).
 *
 * Usage: start `vp run dev` and the trace receiver (`node tools/trace-cli/src/trace-cli.ts serve`),
 * then `node tools/scenario-cli/src/scenario-cli.ts run all` (or name scenarios). `list` shows them.
 * `run --stage <preview-n> all` runs them against a deployed preview instead: its URLs and access
 * token come from the preview CLI, and its spans are pulled from the stage's trace collector before
 * each trace assertion. Deploy the preview with `--dev-timings` first: the scenarios expect
 * compaction and GC within seconds. `measure health` and `measure latency` read only a stage's trace
 * collector, so they also take `--collector-url <url>` with the token in `SHALEDB_TRACE_TOKEN`, the
 * form that works for production.
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { Clock, Console, Data, Effect, Layer, Option, Schema } from "effect";
import { Argument, Command, Flag } from "effect/cli";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";

import {
  MeasurementCollector,
  measureFlushCpu,
  measureSplitPause,
  measureCompactionCpu,
  measureGcScale,
  measureManifestCas,
  measureR2Placement,
  measureTransactionAborts,
  measureHealth,
  measureLatency,
  measureSampling,
  measureWorkflowBurst,
  measureWriterMemory,
  measureWrites,
} from "./measurements.ts";
import {
  type MeasurementCollectorOptions,
  selectMeasurementCollector,
  TRACE_TOKEN_VARIABLE,
} from "./measurement-collector-option.ts";
import { smoke } from "./smoke.ts";
import { soak } from "./soak.ts";
import { ScenarioStack, type ScenarioFailure } from "./scenario.ts";
import { SCENARIOS } from "./scenarios.ts";

/**
 * Some scenarios failed (`ScenariosFailed`), a name matched no scenario (`UnknownScenario`), or the
 * preview stage could not be resolved (`PreviewUnavailable`). Exits non-zero.
 */
class ScenarioCliError extends Data.TaggedError("ScenarioCliError")<{
  readonly reason: "ScenariosFailed" | "UnknownScenario" | "PreviewUnavailable";
  readonly message: string;
}> {}

const PREVIEW_CLI = fileURLToPath(new URL("../../preview-cli/src/preview-cli.ts", import.meta.url));

const decodePreviewUrls = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      apiUrl: Schema.String,
      gcUrl: Schema.String,
      shardsUrl: Schema.String,
      probeUrl: Schema.String,
      jurisdiction: Schema.optionalKey(Schema.String),
    }),
  ),
);
const decodePreviewToken = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Struct({ token: Schema.String })),
);
const decodePreviewCollector = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({ stage: Schema.String, url: Schema.String, token: Schema.String }),
  ),
);

/** Runs the preview CLI (in its own process) and returns its JSON output. */
const previewCli = (args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const result = yield* Effect.sync(() =>
      spawnSync(process.execPath, [PREVIEW_CLI, ...args], { encoding: "utf8" }),
    );
    if (result.status !== 0) {
      return yield* new ScenarioCliError({
        reason: "PreviewUnavailable",
        message: `preview-cli ${args.join(" ")} failed:\n${`${result.stdout}${result.stderr}`.trim()}`,
      });
    }
    return result.stdout.trim();
  });

/** A preview stage's URLs and access token, from the Cloudflare state store. */
const resolvePreview = (stage: string) =>
  Effect.gen(function* () {
    const unreadable = () =>
      new ScenarioCliError({
        reason: "PreviewUnavailable",
        message: `The preview CLI's answer for ${stage} could not be read.`,
      });
    const urls = yield* decodePreviewUrls(yield* previewCli(["url", stage, "--json"])).pipe(
      Effect.mapError(unreadable),
    );
    const { token } = yield* decodePreviewToken(yield* previewCli(["token", stage, "--json"])).pipe(
      Effect.mapError(unreadable),
    );
    return { urls, token };
  });

const list = Command.make("list", {}, () =>
  Effect.forEach(
    SCENARIOS,
    (scenario) => Console.log(`${scenario.name.padEnd(14)} ${scenario.description}`),
    {
      discard: true,
    },
  ),
).pipe(Command.withDescription("List the scenarios"));

const run = Command.make(
  "run",
  {
    names: Argument.String("scenario").pipe(
      Argument.withDescription("Scenario names, or `all`"),
      Argument.variadic(),
    ),
    api: Flag.String("api").pipe(Flag.withDefault("http://localhost:1337")),
    gc: Flag.String("gc").pipe(Flag.withDefault("http://localhost:1340")),
    shards: Flag.String("shards").pipe(Flag.withDefault("http://localhost:1341")),
    probe: Flag.String("probe").pipe(Flag.withDefault("http://localhost:8790")),
    stage: Flag.String("stage").pipe(
      Flag.withDescription(
        "Run against a live preview stage (preview-1, preview-2, preview-3) instead of vp run dev",
      ),
      Flag.optional,
    ),
  },
  ({ names, api, gc, shards, probe, stage }) =>
    runScenarios(names).pipe(
      Effect.provide(
        Option.match(stage, {
          onNone: () =>
            Layer.mergeAll(
              Layer.succeed(ScenarioStack, {
                api,
                gc,
                shards,
                probe,
                previewStage: Option.none(),
                workersCache: false,
              }),
              FetchHttpClient.layer,
            ),
          // Preview Workers answer `401` without the stage's access token (D44).
          onSome: previewStackLayer,
        }),
      ),
    ),
).pipe(
  Command.withDescription(
    "Run scenarios against `vp run dev` (or a preview with --stage); exits non-zero when any fails",
  ),
);

/** The services a run against a preview stage needs: its URLs, its token on every request. */
const previewStackLayer = (stage: string) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const { urls, token } = yield* resolvePreview(stage);
      return Layer.mergeAll(
        Layer.succeed(ScenarioStack, {
          api: urls.apiUrl,
          gc: urls.gcUrl,
          shards: urls.shardsUrl,
          probe: urls.probeUrl,
          previewStage: Option.some(stage),
          workersCache: (urls.jurisdiction ?? "none") === "none",
        }),
        Layer.effect(
          HttpClient.HttpClient,
          Effect.map(HttpClient.HttpClient, (client) =>
            client.pipe(HttpClient.mapRequest(HttpClientRequest.bearerToken(token))),
          ),
        ).pipe(Layer.provide(FetchHttpClient.layer)),
      );
    }),
  );

const collectorLayer = (stage: string) =>
  Layer.effect(
    MeasurementCollector,
    Effect.gen(function* () {
      return yield* decodePreviewCollector(yield* previewCli(["collector", stage, "--json"])).pipe(
        Effect.mapError(
          () =>
            new ScenarioCliError({
              reason: "PreviewUnavailable",
              message: `The trace collector of ${stage} could not be read.`,
            }),
        ),
      );
    }),
  );

const measureStage = Flag.String("stage").pipe(
  Flag.withDescription("Preview stage to measure (preview-1, preview-2, preview-3)"),
);

const runMeasurement = <A, E, R>(stage: string, measurement: Effect.Effect<A, E, R>) =>
  measurement.pipe(
    Effect.flatMap((result) => Console.log(JSON.stringify(result, null, 2))),
    Effect.provide(Layer.mergeAll(previewStackLayer(stage), collectorLayer(stage))),
  );

const measureWritesCommand = Command.make(
  "writes",
  {
    stage: measureStage,
    batchRows: Flag.Int("batch-rows").pipe(Flag.withDefault(500)),
    valueBytes: Flag.Int("value-bytes").pipe(Flag.withDefault(100)),
    concurrency: Flag.Int("concurrency").pipe(Flag.withDefault(8)),
    seconds: Flag.Int("seconds").pipe(Flag.withDefault(30)),
  },
  ({ stage, ...load }) => runMeasurement(stage, measureWrites(load)),
).pipe(Command.withDescription("Rows/s per writer with batched writes (design §12)"));

const measureFlushCommand = Command.make(
  "flush-cpu",
  {
    stage: measureStage,
    megabytes: Flag.Int("megabytes").pipe(Flag.withDefault(16)),
    valueBytes: Flag.Int("value-bytes").pipe(
      Flag.withDescription(
        "Bytes per value; the same megabytes as fewer, larger rows or more, smaller ones",
      ),
      Flag.withDefault(1000),
    ),
  },
  ({ stage, megabytes, valueBytes }) =>
    runMeasurement(stage, measureFlushCpu(megabytes, valueBytes)),
).pipe(Command.withDescription("Writer CPU time per MiB flushed to L0 (design §12)"));

const measureSplitCommand = Command.make(
  "split-pause",
  {
    stage: measureStage,
    writers: Flag.Int("writers").pipe(Flag.withDefault(4)),
    secondsBefore: Flag.Int("seconds-before").pipe(Flag.withDefault(10)),
    secondsAfter: Flag.Int("seconds-after").pipe(Flag.withDefault(10)),
  },
  ({ stage, ...load }) => runMeasurement(stage, measureSplitPause(load)),
).pipe(Command.withDescription("Write pause during a seal-then-fork split (design §12)"));

const measureTransactionsCommand = Command.make(
  "transactions",
  {
    stage: measureStage,
    transactors: Flag.Int("transactors").pipe(Flag.withDefault(4)),
    writers: Flag.Int("writers").pipe(Flag.withDefault(4)),
    seconds: Flag.Int("seconds").pipe(Flag.withDefault(150)),
    holdSeconds: Flag.Int("hold-seconds").pipe(Flag.withDefault(0)),
    flushEverySeconds: Flag.Int("flush-every-seconds").pipe(Flag.withDefault(0)),
  },
  ({ stage, ...load }) => runMeasurement(stage, measureTransactionAborts(load)),
).pipe(Command.withDescription("How often a transaction aborts as too old across flushes (D58)"));

const measureManifestCasCommand = Command.make(
  "manifest-cas",
  {
    stage: measureStage,
    flushes: Flag.Int("flushes").pipe(Flag.withDefault(30)),
    rowsPerFlush: Flag.Int("rows-per-flush").pipe(Flag.withDefault(10)),
  },
  ({ stage, ...load }) => runMeasurement(stage, measureManifestCas(load)),
).pipe(Command.withDescription("Manifest CAS and small-flush latency on the server (D56)"));

const measureR2PlacementCommand = Command.make(
  "r2-placement",
  { stage: measureStage, samples: Flag.Int("samples").pipe(Flag.withDefault(20)) },
  ({ stage, samples }) => runMeasurement(stage, measureR2Placement(samples)),
).pipe(
  Command.withDescription(
    "R2 latency from a Durable Object in each region to the LSM bucket (D56)",
  ),
);

const measureGcScaleCommand = Command.make(
  "gc-scale",
  {
    stage: measureStage,
    databases: Flag.Int("databases").pipe(Flag.withDefault(500)),
    settleSeconds: Flag.Int("settle-seconds").pipe(Flag.withDefault(60)),
  },
  ({ stage, ...load }) => runMeasurement(stage, measureGcScale(load)),
).pipe(
  Command.withDescription(
    "Garbage collection across many databases: compactors collect, the sweep pages (D60)",
  ),
);

const measureCompactionCpuCommand = Command.make(
  "compaction-cpu",
  {
    stage: measureStage,
    megabytesPerFlush: Flag.Int("megabytes-per-flush").pipe(Flag.withDefault(60)),
  },
  ({ stage, megabytesPerFlush }) => runMeasurement(stage, measureCompactionCpu(megabytesPerFlush)),
).pipe(Command.withDescription("CPU per flush and per compaction step at production sizes (D61)"));

const measureWriterMemoryCommand = Command.make(
  "writer-memory",
  { stage: measureStage, megabytes: Flag.Int("megabytes").pipe(Flag.withDefault(250)) },
  ({ stage, megabytes }) => runMeasurement(stage, measureWriterMemory(megabytes)),
).pipe(
  Command.withDescription("A writer holding and flushing ~256 MiB unflushed: memory and CPU (D61)"),
);

const measureWorkflowBurstCommand = Command.make(
  "workflow-burst",
  { stage: measureStage, databases: Flag.Int("databases").pipe(Flag.withDefault(200)) },
  ({ stage, databases }) => runMeasurement(stage, measureWorkflowBurst(databases)),
).pipe(Command.withDescription("Many compaction Workflows started at once (D61)"));

/**
 * The flags of the measurements that read only a stage's trace collector (`health`, `latency`):
 * a preview by `--stage`, or any live stage, production included, by `--collector-url`.
 */
const telemetryFlags = {
  stage: Flag.String("stage").pipe(
    Flag.withDescription("Preview stage to measure (preview-1, preview-2, preview-3)"),
    Flag.optional,
  ),
  collectorUrl: Flag.String("collector-url").pipe(
    Flag.withDescription(
      `Trace collector URL of any live stage (production: its traceCollectorUrl output), instead of --stage; the token is read from ${TRACE_TOKEN_VARIABLE}`,
    ),
    Flag.optional,
  ),
};

/**
 * Runs a measurement that reads only the stage's tail records and spans, so it needs no API URL or
 * access token: just the collector `options` name, checked before any request.
 */
const runTelemetryMeasurement = <A, E>(
  options: MeasurementCollectorOptions,
  measurement: Effect.Effect<A, E, MeasurementCollector>,
) =>
  Effect.gen(function* () {
    const selection = yield* selectMeasurementCollector(options, process.env);
    const collector =
      selection._tag === "Stage"
        ? collectorLayer(selection.stage)
        : Layer.succeed(MeasurementCollector, selection.collector);
    const result = yield* measurement.pipe(Effect.provide(collector));
    yield* Console.log(JSON.stringify(result, null, 2));
  });

const measureHealthCommand = Command.make(
  "health",
  { ...telemetryFlags, minutes: Flag.Int("minutes").pipe(Flag.withDefault(60)) },
  ({ minutes, ...options }) => runTelemetryMeasurement(options, measureHealth(minutes)),
).pipe(
  Command.withDescription(
    "A live stage's health from its tail records and spans: limits hit, refused writes, flushes, L0 backlog, corruption, GC (D63)",
  ),
);

const measureLatencyCommand = Command.make(
  "latency",
  { ...telemetryFlags, minutes: Flag.Int("minutes").pipe(Flag.withDefault(15)) },
  ({ minutes, ...options }) => runTelemetryMeasurement(options, measureLatency(minutes)),
).pipe(
  Command.withDescription(
    "Server-side latency and block reads per operation over the last minutes, from a live stage's spans (D67)",
  ),
);

const measureSamplingCommand = Command.make(
  "sampling",
  { stage: measureStage, requests: Flag.Int("requests").pipe(Flag.withDefault(200)) },
  ({ stage, requests }) => runMeasurement(stage, measureSampling(requests)),
).pipe(
  Command.withDescription(
    "Head sampling: fraction of traces kept, each whole across Workers (D63)",
  ),
);

const soakCommand = Command.make(
  "soak",
  {
    stage: measureStage,
    minutes: Flag.Int("minutes").pipe(Flag.withDefault(130)),
    databases: Flag.Int("databases").pipe(Flag.withDefault(6)),
    keysPerDatabase: Flag.Int("keys-per-database").pipe(Flag.withDefault(4000)),
    redeployAt: Flag.Int("redeploy-at").pipe(
      Flag.withDescription("Minute at which to redeploy the stage (production settings)"),
      Flag.optional,
    ),
    splitAt: Flag.String("split-at").pipe(
      Flag.withDescription("Comma-separated minutes at which to split the sharded group"),
      Flag.withDefault("20,80"),
    ),
    traceSampleRate: Flag.Finite("trace-sample-rate").pipe(
      Flag.withDescription(
        "The trace sample rate the stage was deployed with; the midway redeploy keeps it (soak at 0.1)",
      ),
      Flag.optional,
    ),
    reportFile: Flag.String("report-file").pipe(
      Flag.withDescription(
        "Keep the report in this file while the soak runs: replaced atomically every minute and after each phase",
      ),
      Flag.optional,
    ),
    telemetryBudgetMinutes: Flag.Int("telemetry-budget-minutes").pipe(
      Flag.withDescription(
        "Minutes each final telemetry read may take before it is cut off and recorded as a failure",
      ),
      Flag.withDefault(10),
    ),
  },
  ({
    stage,
    minutes,
    databases,
    keysPerDatabase,
    redeployAt,
    splitAt,
    traceSampleRate,
    reportFile,
    telemetryBudgetMinutes,
  }) =>
    runMeasurement(
      stage,
      soak({
        minutes,
        databases,
        keysPerDatabase,
        redeployAtMinute: redeployAt,
        splitAtMinutes: splitAt.split(",").map((minute) => Number(minute.trim())),
        traceSampleRate,
        reportFile,
        telemetryBudgetMinutes,
      }),
    ),
).pipe(
  Command.withDescription(
    "Mixed workload with splits, snapshots, removals, and a redeploy; checks every acknowledged write at the end (D65)",
  ),
);

const smokeCommand = Command.make(
  "smoke",
  {
    stage: Flag.String("stage").pipe(
      Flag.withDescription("A preview stage, whose URL and token preview-cli reads"),
      Flag.optional,
    ),
    apiUrl: Flag.String("api-url").pipe(
      Flag.withDescription(
        "The API's URL (production: its apiUrl output); the token is read from SHALEDB_TOKEN",
      ),
      Flag.optional,
    ),
  },
  ({ stage, apiUrl }) =>
    Effect.gen(function* () {
      const target = yield* Option.match(stage, {
        onSome: (name) =>
          Effect.map(resolvePreview(name), ({ urls, token }) => ({ url: urls.apiUrl, token })),
        onNone: () =>
          Option.match(apiUrl, {
            onNone: () =>
              Effect.fail(
                new ScenarioCliError({
                  reason: "PreviewUnavailable",
                  message: "Pass --stage <preview> or --api-url <url> (with SHALEDB_TOKEN set).",
                }),
              ),
            onSome: (url) => {
              const token = process.env["SHALEDB_TOKEN"];
              return token === undefined || token === ""
                ? Effect.fail(
                    new ScenarioCliError({
                      reason: "PreviewUnavailable",
                      message: "Set SHALEDB_TOKEN to one of the stage's access tokens.",
                    }),
                  )
                : Effect.succeed({ url, token });
            },
          }),
      });
      const report = yield* smoke(target.url).pipe(
        Effect.provide(
          Layer.effect(
            HttpClient.HttpClient,
            Effect.map(HttpClient.HttpClient, (client) =>
              client.pipe(HttpClient.mapRequest(HttpClientRequest.bearerToken(target.token))),
            ),
          ).pipe(Layer.provide(FetchHttpClient.layer)),
        ),
      );
      yield* Console.log(JSON.stringify(report, null, 2));
    }),
).pipe(
  Command.withDescription(
    "Production-safe smoke test on its own databases, removed afterwards; no trace assertions (D65)",
  ),
);

const measure = Command.make("measure").pipe(
  Command.withDescription("Measurements against a live preview stage; JSON results"),
  Command.withSubcommands([
    measureWritesCommand,
    measureFlushCommand,
    measureSplitCommand,
    measureTransactionsCommand,
    measureManifestCasCommand,
    measureR2PlacementCommand,
    measureGcScaleCommand,
    measureCompactionCpuCommand,
    measureWriterMemoryCommand,
    measureWorkflowBurstCommand,
    measureHealthCommand,
    measureLatencyCommand,
    measureSamplingCommand,
  ]),
);

/**
 * Lets this process notice the connections the Stack's Workers closed, then sends each Worker one
 * request whose outcome is ignored.
 *
 * **Details**
 *
 * A scenario ends with trace assertions that run the trace CLI synchronously, which blocks this
 * process for seconds. The Workers close its idle connections meanwhile, and the next scenario's
 * first request, sent before the process has seen that, fails with "other side closed". The pause
 * gives the process a turn to see the closed connections; a request that still meets one fails
 * here instead of in the scenario.
 */
const reopenConnections = Effect.gen(function* () {
  const stack = yield* ScenarioStack;
  const client = yield* HttpClient.HttpClient;
  yield* Effect.sleep("100 millis");
  yield* Effect.forEach(
    [stack.api, stack.gc, stack.shards, stack.probe],
    (url) =>
      client.execute(HttpClientRequest.get(url)).pipe(Effect.timeout("10 seconds"), Effect.ignore),
    { concurrency: "unbounded", discard: true },
  );
});

const runScenarios = (names: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const wanted =
      names.length === 0 || names.includes("all") ? SCENARIOS.map((s) => s.name) : names;
    const unknown = wanted.filter((name) => !SCENARIOS.some((scenario) => scenario.name === name));
    if (unknown.length > 0) {
      return yield* new ScenarioCliError({
        reason: "UnknownScenario",
        message: `Unknown scenario(s): ${unknown.join(", ")}. Run \`list\` to see them.`,
      });
    }
    const failures: Array<string> = [];
    for (const scenario of SCENARIOS.filter((candidate) => wanted.includes(candidate.name))) {
      yield* reopenConnections;
      const started = yield* Clock.currentTimeMillis;
      const outcome = yield* Effect.result(scenario.run);
      const seconds = (((yield* Clock.currentTimeMillis) - started) / 1000).toFixed(1);
      if (outcome._tag === "Success") {
        yield* Console.log(`✓ ${scenario.name} (${seconds}s)`);
      } else {
        const failure: ScenarioFailure = outcome.failure;
        failures.push(scenario.name);
        yield* Console.log(
          `✗ ${scenario.name} (${seconds}s) at "${failure.step}": ${failure.message}`,
        );
      }
    }
    if (failures.length > 0) {
      return yield* new ScenarioCliError({
        reason: "ScenariosFailed",
        message: `${failures.length} scenario(s) failed: ${failures.join(", ")}.`,
      });
    }
    yield* Console.log(`All ${wanted.length} scenario(s) passed.`);
  });

const scenarioCli = Command.make("scenario").pipe(
  Command.withDescription("End-to-end scenarios for the local shaledb Stack"),
  Command.withSubcommands([list, run, measure, soakCommand, smokeCommand]),
);

Command.run(scenarioCli, { version: "0.0.0" }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
