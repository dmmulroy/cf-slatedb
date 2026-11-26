import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { Clock, Context, Data, Effect, Option, Schedule } from "effect";
import type { HttpClient } from "effect/http";

/**
 * A scenario did not behave as expected: `step` names what it was doing and `message` what went
 * wrong, in the system's words.
 */
export class ScenarioFailure extends Data.TaggedError("ScenarioFailure")<{
  readonly step: string;
  readonly message: string;
}> {}

/** Where the Stack's Workers listen, and where their spans can be read. */
export interface StackUrls {
  readonly api: string;
  readonly gc: string;
  readonly shards: string;
  /** The telemetry probe Worker (also the R2 placement probe, `GET /v1/probe/r2`). */
  readonly probe: string;
  /**
   * The live preview stage the Workers belong to, whose spans are pulled from its trace collector
   * before each trace assertion; `None` for `vp run dev`, whose spans reach the local receiver
   * before each response returns.
   */
  readonly previewStage: Option.Option<string>;
  /**
   * Whether Workers Cache runs in front of `BlockServer`: on a preview stage without a
   * jurisdiction. Local stages do not emulate it, and a restricted Stack turns it off (D49).
   */
  readonly workersCache: boolean;
}

/** The Stack a scenario runs against: `vp run dev`, or a live preview stage. */
export class ScenarioStack extends Context.Service<ScenarioStack, StackUrls>()(
  "@shaledb/scenario/ScenarioStack",
) {}

/** Services every scenario may use. */
export type ScenarioServices = ScenarioStack | HttpClient.HttpClient;

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
 * What caused `error`, innermost last: a failed request's `HttpClientError` says only "Transport
 * error", and the reason (connection refused, reset, timed out) is in its `cause` chain.
 */
const describeCauses = (error: unknown): string => {
  const causes: Array<string> = [];
  let current: unknown = error;
  for (let depth = 0; depth < 4; depth++) {
    if (typeof current !== "object" || current === null || !("cause" in current)) break;
    current = current.cause;
    if (current instanceof Error) {
      const code =
        "code" in current && typeof current.code === "string" ? ` [${current.code}]` : "";
      causes.push(`${current.name}: ${current.message}${code}`);
    }
  }
  return causes.length === 0 ? "" : ` (caused by ${causes.join(" ← ")})`;
};

/**
 * Runs an effect that calls the Stack, turning any failure into a `ScenarioFailure` for `step` (a
 * typed API error becomes its tag and message, followed by what caused it).
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
          message: `${error._tag}${"message" in error && typeof error.message === "string" ? `: ${error.message}` : ""}${describeCauses(error)}`,
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

/** How long live Workers get to export a scenario's spans (after each response) before a pull. */
const PREVIEW_SPAN_SETTLE = "5 seconds";

/** Pulls and asserts again this many times, a few seconds apart, before a preview check fails. */
const PREVIEW_ASSERT_ATTEMPTS = 6;

/**
 * When this process started. A preview check pulls only spans exported since then: a stage that
 * ran a soak holds hours of exports nobody pulled, and reading them first would stall the check.
 */
const PROCESS_STARTED_AT = Date.now();

/**
 * Runs the trace CLI in its own process without blocking this one; returns its exit status, its
 * output (when `captureStdout`), and its error output. Interrupting the effect kills the process and
 * waits for it to exit, so a read cut off by a time budget stops reading.
 */
const runTraceCliProcess = (args: ReadonlyArray<string>, captureStdout: boolean) =>
  Effect.callback<{ readonly status: number; readonly stdout: string; readonly stderr: string }>(
    (resume) => {
      const child = spawn(process.execPath, [TRACE_CLI, ...args], {
        stdio: ["ignore", captureStdout ? "pipe" : "ignore", "pipe"],
      });
      const stdout: Array<Buffer> = [];
      let stderr = "";
      child.stdout?.on("data", (chunk: Buffer) => stdout.push(chunk));
      child.stderr?.on("data", (chunk: Buffer) => {
        stderr += chunk.toString();
      });
      const exited = new Promise<void>((settle) => child.on("exit", () => settle()));
      child.on("exit", (code) =>
        resume(
          Effect.succeed({ status: code ?? -1, stdout: Buffer.concat(stdout).toString(), stderr }),
        ),
      );
      return Effect.promise(() => {
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
        return exited;
      });
    },
  );

/**
 * Runs the trace CLI in its own process without blocking this one (a long `pull` must not stall a
 * soak's other fibers); returns its exit status and error output. Interrupting it kills the process.
 */
export const runTraceCliAsync = (args: ReadonlyArray<string>) =>
  Effect.map(runTraceCliProcess(args, false), ({ status, stderr }) => ({ status, stderr }));

/**
 * Runs the trace CLI in its own process without blocking this one; returns its exit status, output,
 * and error output. Interrupting it kills the process: a `grep` over a large store can be cut off.
 */
export const runTraceCliCaptured = (args: ReadonlyArray<string>) => runTraceCliProcess(args, true);

/** Runs the trace CLI in its own process, blocking this one; returns its exit status and output. */
export const runTraceCli = (args: ReadonlyArray<string>, input?: string) =>
  Effect.sync(() =>
    spawnSync(process.execPath, [TRACE_CLI, ...args], {
      ...(input === undefined ? {} : { input }),
      encoding: "utf8",
      // A grep over a busy stage prints far more than the default 1 MiB.
      maxBuffer: 1024 * 1024 * 1024,
    }),
  );

/**
 * Pulls into the default trace store the spans `stage` exported since this process started (see
 * {@link PROCESS_STARTED_AT}); returns the trace CLI's exit status and output.
 */
export const pullStageSpansSinceStart = (stage: string) =>
  Effect.gen(function* () {
    const sinceMinutes =
      Math.ceil(((yield* Clock.currentTimeMillis) - PROCESS_STARTED_AT) / 60_000) + 1;
    return yield* runTraceCli(["pull", "--stage", stage, "--since-minutes", String(sinceMinutes)]);
  });

/**
 * Runs a trace assertion (the trace CLI's `assert`, in its own process) against the local trace
 * store. Locally, the Workers flush spans before responding, so a scenario's traces are stored by
 * the time its requests return. On a preview stage they are exported after each response to the
 * stage's collector: the check waits, pulls them into the store, and retries a few times while
 * late spans arrive.
 */
export const assertTraces = (name: string, assertion: unknown) =>
  Effect.gen(function* () {
    const { previewStage } = yield* ScenarioStack;
    const input = JSON.stringify(assertion);
    if (Option.isNone(previewStage)) {
      const result = yield* runTraceCli(["assert", "-"], input);
      if (result.status === 0) return;
      return yield* new ScenarioFailure({
        step: name,
        message: `trace assertion failed:\n${result.stdout}${result.stderr}`.trimEnd(),
      });
    }
    let last = "";
    for (let attempt = 1; attempt <= PREVIEW_ASSERT_ATTEMPTS; attempt++) {
      yield* Effect.sleep(attempt === 1 ? PREVIEW_SPAN_SETTLE : "3 seconds");
      const pulled = yield* pullStageSpansSinceStart(previewStage.value);
      if (pulled.status !== 0) {
        last = `pulling ${previewStage.value}'s spans failed:\n${pulled.stdout}${pulled.stderr}`;
        continue;
      }
      const result = yield* runTraceCli(["assert", "-"], input);
      if (result.status === 0) return;
      last = `trace assertion failed:\n${result.stdout}${result.stderr}`;
    }
    return yield* new ScenarioFailure({ step: name, message: last.trimEnd() });
  });

/** A database id unique to this run of a scenario. */
export const uniqueId = (prefix: string) => `${prefix}-${Date.now().toString(36)}`;
