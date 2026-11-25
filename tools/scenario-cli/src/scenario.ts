import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { Context, Data, Effect, Schedule } from "effect";
import type { HttpClient } from "effect/http";

/**
 * A scenario did not behave as expected: `step` names what it was doing and `message` what went
 * wrong, in the system's words.
 */
export class ScenarioFailure extends Data.TaggedError("ScenarioFailure")<{
  readonly step: string;
  readonly message: string;
}> {}

/** Where the local Stack's Workers listen (`vp run dev`). */
export interface StackUrls {
  readonly api: string;
  readonly gc: string;
  readonly shards: string;
}

/** The local Stack a scenario runs against. */
export class LocalStack extends Context.Service<LocalStack, StackUrls>()(
  "@cf-slatedb/scenario/LocalStack",
) {}

/** Services every scenario may use. */
export type ScenarioServices = LocalStack | HttpClient.HttpClient;

/** A named end-to-end scenario: calls to the local Stack, checks on results, then trace checks. */
export interface Scenario {
  readonly name: string;
  readonly description: string;
  readonly run: Effect.Effect<void, ScenarioFailure, ScenarioServices>;
}

/** Fails the scenario unless `actual` equals `expected` (compared as JSON). */
export const expectEqual = <A>(step: string, actual: A, expected: A) => {
  const left = JSON.stringify(actual, binaryAware);
  const right = JSON.stringify(expected, binaryAware);
  return left === right
    ? Effect.void
    : Effect.fail(new ScenarioFailure({ step, message: `expected ${right}, got ${left}` }));
};

/** Fails the scenario unless `condition` holds. */
export const expectThat = (step: string, condition: boolean, message: string) =>
  condition ? Effect.void : Effect.fail(new ScenarioFailure({ step, message }));

function binaryAware(_key: string, value: unknown): unknown {
  return value instanceof Uint8Array ? `bytes:${Buffer.from(value).toString("hex")}` : value;
}

/**
 * Runs an effect that calls the Stack, turning any failure into a `ScenarioFailure` for `step` (a
 * typed API error becomes its tag and message).
 */
export const step = <A, E extends { readonly _tag: string }, R>(
  name: string,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, ScenarioFailure, R> =>
  effect.pipe(
    Effect.mapError(
      (error) =>
        new ScenarioFailure({
          step: name,
          message: `${error._tag}${"message" in error && typeof error.message === "string" ? `: ${error.message}` : ""}`,
        }),
    ),
  );

/** Expects `effect` to fail: returns its failure, and fails the scenario if it succeeded. */
export const expectFailure = <A, R>(name: string, effect: Effect.Effect<A, ScenarioFailure, R>) =>
  effect.pipe(
    Effect.matchEffect({
      onFailure: (failure) => Effect.succeed(failure),
      onSuccess: () =>
        Effect.fail(
          new ScenarioFailure({ step: name, message: "expected a failure, but it succeeded" }),
        ),
    }),
  );

/** Runs `effect` until `done` holds, every 250 ms for up to `seconds`. */
export const eventually = <A, E, R>(
  name: string,
  effect: Effect.Effect<A, E, R>,
  done: (value: A) => boolean,
  seconds = 30,
) =>
  effect.pipe(
    Effect.repeat({ schedule: Schedule.spaced("250 millis"), until: done, times: seconds * 4 }),
    Effect.flatMap((value) =>
      done(value)
        ? Effect.succeed(value)
        : Effect.fail(
            new ScenarioFailure({ step: name, message: `not reached within ${seconds}s` }),
          ),
    ),
  );

const TRACE_CLI = fileURLToPath(new URL("../../trace-cli/src/trace-cli.ts", import.meta.url));

/**
 * Runs a trace assertion (the trace CLI's `assert`, in its own process) against the local trace
 * store. The Workers flush spans before responding in the `dev` stage, so a scenario's traces are
 * stored by the time its requests return.
 */
export const assertTraces = (name: string, assertion: unknown) =>
  Effect.gen(function* () {
    const result = yield* Effect.sync(() =>
      spawnSync(process.execPath, [TRACE_CLI, "assert", "-"], {
        input: JSON.stringify(assertion),
        encoding: "utf8",
      }),
    );
    if (result.status !== 0) {
      return yield* new ScenarioFailure({
        step: name,
        message: `trace assertion failed:\n${result.stdout}${result.stderr}`.trimEnd(),
      });
    }
  });

/** A database id unique to this run of a scenario. */
export const uniqueId = (prefix: string) => `${prefix}-${Date.now().toString(36)}`;
