/**
 * Preview CLI: deploys shaledb to the three reusable live preview stages and reports what is
 * deployed where, reading the Stack's outputs from the Cloudflare state store so it works from any
 * machine.
 *
 * Usage: `node tools/preview-cli/src/preview-cli.ts <command>`. Every command accepts `--json`.
 *
 * - `deploy <stage> [--dev-timings]` redeploys the Stack to `preview-1`, `preview-2`, or
 *   `preview-3` in place, through `vp run @shaledb/infra#deploy:<stage>`.
 * - `status` lists each preview stage and its URLs.
 * - `url <stage>` prints one stage's URLs.
 * - `token <stage>` prints the stage's access token (its Workers answer `401` without it).
 * - `collector <stage>` prints the stage's trace collector URL and token (for `trace-cli pull`).
 * - `bindings <stage>` prints the engine settings and jurisdiction each Worker is bound.
 * - `destroy-command <stage>` prints the command that tears a stage down. It never runs it.
 */
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Data, Effect, Option, Schema } from "effect";
import { Argument, Command, Flag } from "effect/cli";

import {
  SHALEDB_STACK_NAME,
  deployEnvironment,
  deployJurisdiction,
  deployTask,
  describeBoundLsmSettings,
  destroyCommand,
  parseStateRead,
  PREVIEW_DEV_TIMINGS,
  PREVIEW_STAGES,
  PreviewJurisdiction,
  previewOutputs,
  previewSecret,
  previewWorkerBindings,
  encodePreviewOutputs,
  type PreviewOutputs,
  type PreviewStage,
  type PreviewStatus,
  type StateRecords,
} from "./preview-stage.ts";

/**
 * Failure the preview CLI reports to the person running it, exiting non-zero. `reason` is
 * `DeployFailed` when the deploy task exited non-zero, `NotDeployed` when the stage has no Stack, and
 * `TokenMissing` when a deployed stage has no access token.
 */
class PreviewCliError extends Data.TaggedError("PreviewCliError")<{
  readonly reason: "DeployFailed" | "NotDeployed" | "TokenMissing";
  readonly stage: PreviewStage;
  readonly message: string;
}> {}

const REPO_ROOT = fileURLToPath(new URL("../../../", import.meta.url));

const decodeSettingsOverrides = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Number)),
);

const jsonFlag = Flag.Boolean("json").pipe(
  Flag.withDescription("Print machine-readable JSON"),
  Flag.withDefault(false),
);
const stageArgument = Argument.Literals("stage", PREVIEW_STAGES).pipe(
  Argument.withDescription("Preview stage: preview-1, preview-2, or preview-3"),
);

/** Reads every state record under `Shaledb[/<stage>]` through the read-only `state:read` task. */
const readStateRecords = (stage: Option.Option<PreviewStage>) =>
  Effect.gen(function* () {
    const path = Option.match(stage, {
      onNone: () => SHALEDB_STACK_NAME,
      onSome: (name) => `${SHALEDB_STACK_NAME}/${name}`,
    });
    const result = yield* Effect.sync(() =>
      spawnSync("vp", ["run", "@shaledb/infra#state:read", "-r", path], {
        cwd: REPO_ROOT,
        encoding: "utf8",
      }),
    );
    return yield* parseStateRead(`${result.stdout}${result.stderr}`, path);
  });

/** The outputs of a deployed stage; fails with `NotDeployed` when nothing is deployed there. */
const deployedOutputs = (stage: PreviewStage) =>
  Effect.gen(function* () {
    const records = yield* readStateRecords(Option.some(stage));
    const outputs = yield* previewOutputs(records, stage);
    if (Option.isNone(outputs)) {
      return yield* new PreviewCliError({
        stage,
        reason: "NotDeployed",
        message: `Nothing is deployed to ${stage}. Deploy it with \`preview-cli deploy ${stage}\`.`,
      });
    }
    return { records, outputs: outputs.value };
  });

const printOutputs = (
  stage: PreviewStage,
  outputs: PreviewOutputs,
  extra: Readonly<Record<string, string | boolean>>,
  json: boolean,
) => printFields(stage, { ...encodePreviewOutputs(outputs), ...extra }, json);

const printFields = (
  stage: PreviewStage,
  outputs: Readonly<Record<string, unknown>>,
  json: boolean,
) =>
  json
    ? Console.log(JSON.stringify({ stage, ...outputs }))
    : Effect.forEach(Object.entries(outputs), ([name, value]) =>
        Console.log(`${stage} ${name.padEnd(22)} ${String(value)}`),
      ).pipe(Effect.asVoid);

const deploy = Command.make(
  "deploy",
  {
    stage: stageArgument,
    jurisdiction: Flag.Literals("jurisdiction", PreviewJurisdiction.literals).pipe(
      Flag.withDescription(
        "Data residency for the whole stage: eu, fedramp, or none. Default: the stage's current one. Changing it replaces the stage's buckets and Workflow and starts new Durable Objects: the data does not move.",
      ),
      Flag.optional,
    ),
    devTimings: Flag.Boolean("dev-timings").pipe(
      Flag.withDescription(
        `Use vp run dev's short GC and compactor timings (${JSON.stringify(PREVIEW_DEV_TIMINGS)}) instead of the production defaults`,
      ),
      Flag.withDefault(false),
    ),
    settings: Flag.String("settings").pipe(
      Flag.withDescription(
        "Engine setting overrides as JSON (e.g. '{\"l0SstSize\":268435456}'), for a measurement deploy; applied over --dev-timings",
      ),
      Flag.optional,
    ),
    traceSampleRate: Flag.Finite("trace-sample-rate").pipe(
      Flag.withDescription(
        "Fraction of traces kept (0 to 1), for a measurement deploy; previews otherwise keep every trace",
      ),
      Flag.optional,
    ),
    json: jsonFlag,
  },
  ({ stage, jurisdiction, devTimings, settings, traceSampleRate, json }) =>
    Effect.gen(function* () {
      const current = yield* previewOutputs(yield* readStateRecords(Option.some(stage)), stage);
      const residency = deployJurisdiction(jurisdiction, current);
      const previous = Option.flatMap(current, (outputs) => outputs.jurisdiction);
      if (Option.isSome(previous) && previous.value !== residency) {
        yield* Console.error(
          `${stage} moves from jurisdiction ${previous.value} to ${residency}: its buckets and Workflow are replaced and new Durable Objects are addressed, so its databases do not carry over.`,
        );
      }
      // Settings come only from the flags: values left in the shell must not change a preview.
      const overrides = yield* Option.match(settings, {
        onNone: () => Effect.succeed(Option.none<Readonly<Record<string, number>>>()),
        onSome: (text) =>
          decodeSettingsOverrides(text).pipe(
            Effect.map(Option.some),
            Effect.mapError(
              () =>
                new PreviewCliError({
                  stage,
                  reason: "DeployFailed",
                  message: `--settings must be a JSON object of numeric settings, such as '{"l0SstSize":268435456}'.`,
                }),
            ),
          ),
      });
      const env = deployEnvironment(process.env, {
        devTimings,
        jurisdiction: residency,
        settings: overrides,
        traceSampleRate,
      });
      yield* Console.error(
        `Deploying ${SHALEDB_STACK_NAME} to ${stage} with ${devTimings ? "dev" : "production"} timings, jurisdiction ${residency} (vp run ${deployTask(stage)})`,
      );
      // The Workers bind the stage's two Secrets Store token slots (D64): create any that are
      // missing first. Previews also keep their state-held token, which `token` prints.
      const tokens = yield* Effect.sync(() =>
        spawnSync("vp", ["run", "@shaledb/infra#tokens", "ensure", stage], {
          cwd: REPO_ROOT,
          env,
          stdio: ["ignore", 2, 2],
        }),
      );
      if (tokens.status !== 0) {
        return yield* new PreviewCliError({
          stage,
          reason: "DeployFailed",
          message: `Creating ${stage}'s access-token secrets failed (exit ${String(tokens.status ?? tokens.signal)}); its log is above. Run the same command again: it is safe to repeat.`,
        });
      }
      const result = yield* Effect.sync(() =>
        // The task's own output goes to stderr, so stdout carries only this command's result.
        spawnSync("vp", ["run", deployTask(stage)], {
          cwd: REPO_ROOT,
          env,
          stdio: ["ignore", 2, 2],
        }),
      );
      if (result.status !== 0) {
        return yield* new PreviewCliError({
          stage,
          reason: "DeployFailed",
          message: `Deploying ${stage} failed (exit ${String(result.status ?? result.signal)}); its log is above. Fix the cause and run the same command again: a deploy is safe to repeat.`,
        });
      }
      const { outputs } = yield* deployedOutputs(stage);
      yield* printOutputs(stage, outputs, { devTimings }, json);
    }),
).pipe(Command.withDescription("Deploy the Stack to a preview stage, in place"));

const status = Command.make("status", { json: jsonFlag }, ({ json }) =>
  Effect.gen(function* () {
    const records = yield* readStateRecords(Option.none());
    const statuses: Array<PreviewStatus> = [];
    for (const stage of PREVIEW_STAGES) {
      statuses.push({ stage, outputs: yield* previewOutputs(records, stage) });
    }
    if (json) {
      return yield* Console.log(
        JSON.stringify(
          statuses.map(({ stage, outputs }) => ({
            stage,
            deployed: Option.isSome(outputs),
            outputs: Option.getOrNull(Option.map(outputs, encodePreviewOutputs)),
          })),
        ),
      );
    }
    for (const { stage, outputs } of statuses) {
      yield* Option.match(outputs, {
        onNone: () => Console.log(`${stage} not deployed`),
        onSome: (deployed) => printOutputs(stage, deployed, {}, false),
      });
    }
  }),
).pipe(Command.withDescription("Show what is deployed to each preview stage"));

const url = Command.make("url", { stage: stageArgument, json: jsonFlag }, ({ stage, json }) =>
  deployedOutputs(stage).pipe(
    Effect.flatMap(({ outputs }) => printOutputs(stage, outputs, {}, json)),
  ),
).pipe(Command.withDescription("Print a preview stage's API, BlockServer, and other URLs"));

/** A deployed stage's generated secret; fails with `TokenMissing` when the stage has none. */
const requiredSecret = (
  records: StateRecords,
  stage: PreviewStage,
  secret: "LsmAccessToken" | "TraceCollectorToken",
) =>
  Effect.gen(function* () {
    const value = yield* previewSecret(records, stage, secret);
    if (Option.isNone(value)) {
      return yield* new PreviewCliError({
        stage,
        reason: "TokenMissing",
        message: `${stage} has no ${secret}. Redeploy it with \`preview-cli deploy ${stage}\`.`,
      });
    }
    return value.value;
  });

const token = Command.make("token", { stage: stageArgument, json: jsonFlag }, ({ stage, json }) =>
  Effect.gen(function* () {
    const { records } = yield* deployedOutputs(stage);
    const value = yield* requiredSecret(records, stage, "LsmAccessToken");
    yield* Console.log(json ? JSON.stringify({ stage, token: value }) : value);
  }),
).pipe(
  Command.withDescription(
    "Print a preview stage's access token, for `Authorization: Bearer <token>`",
  ),
);

const collector = Command.make(
  "collector",
  { stage: stageArgument, json: jsonFlag },
  ({ stage, json }) =>
    Effect.gen(function* () {
      const { records, outputs } = yield* deployedOutputs(stage);
      if (Option.isNone(outputs.traceCollectorUrl)) {
        return yield* new PreviewCliError({
          stage,
          reason: "NotDeployed",
          message: `${stage} has no trace collector yet. Redeploy it with \`preview-cli deploy ${stage}\`.`,
        });
      }
      const value = yield* requiredSecret(records, stage, "TraceCollectorToken");
      yield* json
        ? Console.log(JSON.stringify({ stage, url: outputs.traceCollectorUrl.value, token: value }))
        : Console.log(`${outputs.traceCollectorUrl.value}\n${value}`);
    }),
).pipe(
  Command.withDescription(
    "Print a preview stage's trace collector URL and token (what `trace-cli pull --stage` uses)",
  ),
);

const bindings = Command.make(
  "bindings",
  { stage: stageArgument, json: jsonFlag },
  ({ stage, json }) =>
    Effect.gen(function* () {
      const { records } = yield* deployedOutputs(stage);
      const workers = previewWorkerBindings(records, stage);
      if (json) {
        return yield* Console.log(
          JSON.stringify(
            workers.map((worker) => ({
              stage,
              worker: worker.worker,
              lsmSettings: Option.getOrNull(worker.lsmSettings),
              jurisdiction: Option.getOrNull(worker.jurisdiction),
            })),
          ),
        );
      }
      for (const worker of workers) {
        const settings = Option.match(worker.lsmSettings, {
          onNone: () => "settings unbound",
          onSome: describeBoundLsmSettings,
        });
        const residency = Option.getOrElse(worker.jurisdiction, () => "unbound");
        yield* Console.log(
          `${stage} ${worker.worker.padEnd(22)} jurisdiction ${residency.padEnd(8)} ${settings}`,
        );
      }
    }),
).pipe(
  Command.withDescription(
    "Print the engine settings and jurisdiction each of a preview stage's Workers is bound",
  ),
);

const destroy = Command.make(
  "destroy-command",
  { stage: stageArgument, json: jsonFlag },
  ({ stage, json }) =>
    Console.log(
      json ? JSON.stringify({ stage, command: destroyCommand(stage) }) : destroyCommand(stage),
    ),
).pipe(
  Command.withDescription(
    "Print the command that tears a preview stage down (for Dillon; this CLI never runs it)",
  ),
);

const previewCli = Command.make("preview").pipe(
  Command.withDescription("Deploy and inspect shaledb's live preview stages"),
  Command.withSubcommands([deploy, status, url, token, collector, bindings, destroy]),
);

Command.run(previewCli, { version: "0.0.0" }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
