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
 * compaction and GC within seconds.
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
  measureManifestCas,
  measureR2Placement,
  measureTransactionAborts,
  measureWrites,
} from "./measurements.ts";
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
  { stage: measureStage, megabytes: Flag.Int("megabytes").pipe(Flag.withDefault(16)) },
  ({ stage, megabytes }) => runMeasurement(stage, measureFlushCpu(megabytes)),
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

const measure = Command.make("measure").pipe(
  Command.withDescription("Measurements against a live preview stage; JSON results"),
  Command.withSubcommands([
    measureWritesCommand,
    measureFlushCommand,
    measureSplitCommand,
    measureTransactionsCommand,
    measureManifestCasCommand,
    measureR2PlacementCommand,
  ]),
);

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
  Command.withDescription("End-to-end scenarios for the local cf-slatedb Stack"),
  Command.withSubcommands([list, run, measure]),
);

Command.run(scenarioCli, { version: "0.0.0" }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
