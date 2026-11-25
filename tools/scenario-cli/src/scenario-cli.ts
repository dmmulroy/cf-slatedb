/**
 * Scenario CLI: runs named end-to-end scenarios against the local Stack (`vp run dev`) and checks
 * both their results and the traces they produce (through the trace CLI's `assert`).
 *
 * Usage: start `vp run dev` and the trace receiver (`node tools/trace-cli/src/trace-cli.ts serve`),
 * then `node tools/scenario-cli/src/scenario-cli.ts run all` (or name scenarios). `list` shows them.
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Clock, Console, Data, Effect, Layer } from "effect";
import { Argument, Command, Flag } from "effect/cli";
import { FetchHttpClient } from "effect/http";

import { LocalStack, type ScenarioFailure } from "./scenario.ts";
import { SCENARIOS } from "./scenarios.ts";

/** Some scenarios failed, or a name matched no scenario. Exits non-zero. */
class ScenarioCliError extends Data.TaggedError("ScenarioCliError")<{
  readonly reason: "ScenariosFailed" | "UnknownScenario";
  readonly message: string;
}> {}

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
  },
  ({ names, api, gc, shards }) =>
    Effect.gen(function* () {
      const wanted =
        names.length === 0 || names.includes("all") ? SCENARIOS.map((s) => s.name) : names;
      const unknown = wanted.filter(
        (name) => !SCENARIOS.some((scenario) => scenario.name === name),
      );
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
    }).pipe(
      Effect.provide(
        Layer.mergeAll(Layer.succeed(LocalStack, { api, gc, shards }), FetchHttpClient.layer),
      ),
    ),
).pipe(
  Command.withDescription("Run scenarios against `vp run dev`; exits non-zero when any fails"),
);

const scenarioCli = Command.make("scenario").pipe(
  Command.withDescription("End-to-end scenarios for the local cf-slatedb Stack"),
  Command.withSubcommands([list, run]),
);

Command.run(scenarioCli, { version: "0.0.0" }).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
