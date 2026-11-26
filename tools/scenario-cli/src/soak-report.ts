import { Effect, FileSystem, Option } from "effect";

/**
 * Where a soak is: `workload` while it runs, then its final checks in order (`final-reads`: every
 * key, counters, and the sharded group; `storage`; `telemetry`: tail records, health, spans), then
 * `done`.
 */
export type SoakPhase = "workload" | "final-reads" | "storage" | "telemetry" | "done";

/** What a soak's verdict depends on. */
export interface SoakVerdictInputs {
  readonly phase: SoakPhase;
  readonly mismatches: ReadonlyArray<string>;
  readonly failures: ReadonlyArray<string>;
  readonly unexpectedErrors: ReadonlyArray<unknown>;
  /** The health report's issues, or `None` when there is no health report (yet). */
  readonly healthIssues: Option.Option<ReadonlyArray<string>>;
}

/**
 * Whether a soak passed: only once `done`, with no mismatch, no failure, no unexpected error span,
 * and a health report without issues. A report written before the end, or by a run that was
 * stopped, never says `passed: true`.
 */
export const soakPassed = (inputs: SoakVerdictInputs) =>
  inputs.phase === "done" &&
  inputs.mismatches.length === 0 &&
  inputs.failures.length === 0 &&
  inputs.unexpectedErrors.length === 0 &&
  Option.exists(inputs.healthIssues, (issues) => issues.length === 0);

/** What the command's exit status depends on: the verdict and what to say when it is no. */
export interface SoakOutcome {
  readonly passed: boolean;
  readonly mismatches: ReadonlyArray<string>;
  readonly failures: ReadonlyArray<string>;
}

/**
 * Why the soak command must exit non-zero, or `None` when the soak passed: a release gate that a
 * script runs must fail by its exit status, not only by a field of the JSON it prints.
 */
export const soakFailureMessage = (outcome: SoakOutcome): Option.Option<string> =>
  outcome.passed
    ? Option.none()
    : Option.some(
        `The soak did not pass (${outcome.mismatches.length} mismatch(es), ${outcome.failures.length} failed check(s)). Its report, printed above, names each one under mismatches, failures, unexpectedErrors, and health.issues. Fix the cause and run the soak again.`,
      );

let reportWrites = 0;

/**
 * Writes `report` as JSON to `path`, replacing the file atomically: the JSON goes to a temporary
 * file beside it, which is then renamed over `path`. A reader, or a run killed mid-write, sees the
 * previous report or the new one, never part of one. The write is not interrupted once started.
 *
 * @returns Nothing, or the file system's `PlatformError`.
 */
export const writeReportFile = (path: string, report: unknown) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    reportWrites += 1;
    const temporary = `${path}.${process.pid}.${reportWrites}.tmp`;
    yield* fs.writeFileString(temporary, `${JSON.stringify(report, null, 2)}\n`);
    yield* fs.rename(temporary, path);
  }).pipe(Effect.uninterruptible);
