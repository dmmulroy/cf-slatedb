import * as Cloudflare from "@alchemy.run/cloudflare";
import { Cause, Effect, Layer, Option, Schema } from "effect";

import { CompactionExecutor } from "./compaction-executor.ts";
import {
  CompactionJobOutcome,
  CompactionJobParams,
  type CompactionStepRunner,
  runCompactionJob,
} from "./compaction-job.ts";
import {
  CompactionLauncher,
  type CompactionJobProgress,
  CompactionLauncherUnavailableError,
} from "./compaction-launcher.ts";
import { lsmJurisdictionAtDeclaration } from "./lsm-jurisdiction.ts";

/**
 * Runs each step as a durable Workflow step: its result is encoded with the step's Schema (Workflow
 * results must be plain data) and replayed on resume. A step failing with a retryable error is
 * retried five times with exponential backoff; a defect stops retries.
 */
const workflowStepRunner: CompactionStepRunner<Cloudflare.Workflows.WorkflowStep> = (
  name,
  schema,
  step,
) =>
  Cloudflare.Workflows.task(
    name,
    step.pipe(Effect.flatMap((value) => Schema.encodeEffect(schema)(value).pipe(Effect.orDie))),
    { retries: { limit: 5, delay: "5 seconds", backoff: "exponential" }, timeout: "10 minutes" },
  ).pipe(
    Effect.flatMap((encoded) => Schema.decodeUnknownEffect(schema)(encoded).pipe(Effect.orDie)),
  );

/**
 * One compaction job as a Cloudflare Workflow: `prepare`, one `output-<n>` step per output SST, and
 * `commit` (see `runCompactionJob`). Each step stays within a Worker's limits: it streams its inputs
 * and its output, so memory holds a few blocks per input and one multipart part.
 */
export class CompactionWorkflow extends Cloudflare.Workflow<CompactionWorkflow>()(
  "CompactionWorkflow",
  // Steps record SST handles, whose first and last keys are data: the Workflow runs and stores in
  // the Stack's jurisdiction (D49). It is read when this module loads, from the deploy's environment.
  Option.isSome(lsmJurisdictionAtDeclaration)
    ? { jurisdiction: lsmJurisdictionAtDeclaration.value }
    : {},
  Effect.gen(function* () {
    const executor = yield* CompactionExecutor;
    return Effect.fn("CompactionWorkflow.run")(function* (
      input: typeof CompactionJobParams.Encoded,
    ) {
      const params = yield* Schema.decodeUnknownEffect(CompactionJobParams)(input).pipe(
        Effect.orDie,
      );
      const outcome = yield* runCompactionJob(params, workflowStepRunner).pipe(
        Effect.provideService(CompactionExecutor, executor),
        // Steps already retried; a job still failing ends errored and the compactor plans again.
        Effect.orDie,
      );
      return Schema.encodeSync(CompactionJobOutcome)(outcome);
    });
  }),
) {}

const decodeOutcome = Schema.decodeUnknownOption(CompactionJobOutcome);

/**
 * A compaction job's progress from its Workflow instance's status: running while the instance can
 * still commit; otherwise finished, with the outcome a completed instance returned (none for an
 * errored or terminated one, or an output this version cannot read).
 */
export const compactionJobProgressOf = (
  status: string,
  /** The instance's output: a completed run's encoded outcome, read from Workflows (untyped). */
  output: unknown,
): CompactionJobProgress => {
  if (RUNNING_STATUSES.has(status)) return { _tag: "Running" };
  return {
    _tag: "Finished",
    outcome: status === "complete" ? decodeOutcome(output) : Option.none(),
  };
};

/**
 * Whether `cause` is Workflows reporting that an instance does not exist: the binding's `get`
 * rejects with an error whose message is `instance.not_found`, which Alchemy raises as a defect
 * (possibly wrapped in an `UnknownError`).
 */
export const isWorkflowInstanceNotFound = (cause: Cause.Cause<unknown>): boolean => {
  const mentionsNotFound = (value: unknown, depth: number): boolean =>
    depth < 4 &&
    value instanceof Error &&
    // An `UnknownError` may carry no message of its own, only the rejection as its cause.
    ((typeof value.message === "string" && value.message.includes(WORKFLOW_INSTANCE_NOT_FOUND)) ||
      mentionsNotFound(value.cause, depth + 1));
  return cause.reasons.some(
    (reason) =>
      (Cause.isDieReason(reason) && mentionsNotFound(reason.defect, 0)) ||
      (Cause.isFailReason(reason) && mentionsNotFound(reason.error, 0)),
  );
};

/** The message Workflows' `get` rejects with for an instance it does not hold. */
const WORKFLOW_INSTANCE_NOT_FOUND = "instance.not_found";

/**
 * Workflow statuses after which a job can still commit. Any other status, `unknown` and any this
 * code does not know included, finishes the job without an outcome: its sources are planned
 * again, and if the instance commits after all, the commit's exact-inputs check refuses it once
 * another job took them (D94, D103).
 */
const RUNNING_STATUSES: ReadonlySet<string> = new Set([
  "queued",
  "running",
  "paused",
  "waiting",
  "waitingForPause",
]);

/**
 * Provides the launcher over `CompactionWorkflow`, registered on the Worker being built: a job is a
 * Workflow instance whose id is the job id.
 */
export const compactionLauncherLayer = Layer.effect(
  CompactionLauncher,
  Effect.gen(function* () {
    const workflow = yield* CompactionWorkflow;
    const encode = Schema.encodeSync(CompactionJobParams);
    const unavailable = (jobId: string, operation: string) => () =>
      Effect.fail(
        new CompactionLauncherUnavailableError({
          jobId,
          message: `Cloudflare Workflows could not ${operation} compaction job ${jobId}. The compactor retries on its next poll.`,
        }),
      );
    return CompactionLauncher.of({
      start: Effect.fn("CompactionLauncher.start")(function* (params: CompactionJobParams) {
        yield* Effect.annotateCurrentSpan({
          "database.id": params.databaseId,
          "compaction.job_id": params.jobId,
        });
        yield* workflow
          .create({ id: params.jobId, params: encode(params) })
          .pipe(Effect.catchCause(unavailable(params.jobId, "start")));
      }),
      progress: Effect.fn("CompactionLauncher.progress")(function* (jobId: string) {
        const status = yield* workflow.get(jobId).pipe(
          Effect.flatMap((instance) => instance.status()),
          Effect.map(Option.some),
          // An instance Workflows no longer holds (purged after its retention) cannot commit:
          // the job is finished, with no outcome. Any other failure may be transient.
          Effect.catchCause((cause) =>
            isWorkflowInstanceNotFound(cause)
              ? Effect.succeed(Option.none())
              : unavailable(jobId, "report the status of")(),
          ),
        );
        yield* Effect.annotateCurrentSpan({
          "compaction.job_id": jobId,
          "workflow.status": Option.match(status, {
            onNone: () => "not_found",
            onSome: (found) => found.status,
          }),
        });
        return Option.match(status, {
          onNone: (): CompactionJobProgress => ({ _tag: "Finished", outcome: Option.none() }),
          onSome: (found) => compactionJobProgressOf(found.status, found.output),
        });
      }),
    });
  }),
);
