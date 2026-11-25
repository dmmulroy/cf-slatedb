import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, Ref, Schema } from "effect";

import type { CompactionExecutor } from "./compaction-executor.ts";
import {
  type CompactionJobOutcome,
  type CompactionJobParams,
  type CompactionStepRunner,
  runCompactionJob,
} from "./compaction-job.ts";

/**
 * The compaction runtime (Cloudflare Workflows) could not start or report a job. Retryable: the
 * compactor tries again on its next poll.
 */
export class CompactionLauncherUnavailableError extends Schema.TaggedError<CompactionLauncherUnavailableError>()(
  "CompactionLauncherUnavailableError",
  { jobId: Schema.String, message: Schema.String },
) {}

/** Starts compaction jobs in their durable runtime and reports which are still running. */
export interface ICompactionLauncher {
  /** Starts a job; its id names the run, so starting the same job twice starts it once. */
  readonly start: (
    params: CompactionJobParams,
  ) => Effect.Effect<void, CompactionLauncherUnavailableError, RuntimeContext>;
  /** Whether the job may still commit (queued, running, waiting, or paused). */
  readonly isRunning: (
    jobId: string,
  ) => Effect.Effect<boolean, CompactionLauncherUnavailableError, RuntimeContext>;
}

/** The contextual compaction launcher. */
export class CompactionLauncher extends Context.Service<CompactionLauncher, ICompactionLauncher>()(
  "@cf-slatedb/CompactionLauncher",
) {}

/** Runs each step directly, without persisting results. */
export const directStepRunner: CompactionStepRunner = (_name, _schema, step) => step;

/** What the in-process launcher recorded about each job it ran. */
export class CompactionLauncherRecord extends Context.Service<
  CompactionLauncherRecord,
  Ref.Ref<ReadonlyMap<string, CompactionJobOutcome | "Failed">>
>()("@cf-slatedb/CompactionLauncherRecord") {}

/**
 * Provides a launcher that runs each job to completion in-process when it starts (with
 * {@link directStepRunner}), over the contextual `CompactionExecutor`, and records the outcome in
 * {@link CompactionLauncherRecord}, which it also outputs. A job is never running after `start`.
 */
export const compactionLauncherTestLayer = Layer.effect(
  CompactionLauncher,
  Effect.gen(function* () {
    const record = yield* CompactionLauncherRecord;
    const context = yield* Effect.context<CompactionExecutor>();
    return CompactionLauncher.of({
      start: (params) =>
        runCompactionJob(params, directStepRunner).pipe(
          Effect.provide(context),
          Effect.matchEffect({
            onSuccess: (outcome) =>
              Ref.update(record, (jobs) => new Map(jobs).set(params.jobId, outcome)),
            onFailure: () =>
              Ref.update(record, (jobs) => new Map(jobs).set(params.jobId, "Failed" as const)),
          }),
        ),
      isRunning: () => Effect.succeed(false),
    });
  }),
).pipe(
  Layer.provideMerge(
    Layer.effect(
      CompactionLauncherRecord,
      Ref.make<ReadonlyMap<string, CompactionJobOutcome | "Failed">>(new Map()),
    ),
  ),
);
