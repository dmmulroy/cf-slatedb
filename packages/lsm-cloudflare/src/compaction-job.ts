import {
  CompactionJob,
  CompactorEpoch,
  DatabaseId,
  ManifestId,
  type SstHandle,
} from "@cf-slatedb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Option, Schema } from "effect";

import {
  type CompactionDiscardedError,
  type CompactionError,
  CompactionExecutor,
  CompactionOutputStep,
  type CompactionUnavailableError,
  PreparedCompaction,
} from "./compaction-executor.ts";

/** Schema for a compaction job's parameters: the Workflow instance's input. */
export const CompactionJobParams = Schema.Struct({
  databaseId: DatabaseId,
  compactorEpoch: CompactorEpoch,
  /** The job's id (a ULID); also the Workflow instance id. */
  jobId: Schema.String,
  job: CompactionJob,
});

/** A compaction job's parameters. */
export type CompactionJobParams = typeof CompactionJobParams.Type;

/** Schema for an input SST a compaction could not read because its bytes are corrupted. */
export const CorruptedCompactionInput = Schema.TaggedStruct("Corrupted", {
  /** The corrupted SST. */
  sstId: Schema.String,
  /** The section whose checksum or layout failed. */
  section: Schema.String,
  /** Why it is unreadable: `ChecksumMismatch`, `Truncated`, `UnsupportedVersion`, `Malformed`. */
  reason: Schema.String,
  message: Schema.String,
});

/**
 * Schema for how a job ended: committed as a manifest; discarded because the compactor was fenced,
 * its inputs changed, its output predates GC's boundary, or the database is gone; or stopped because
 * an input SST is corrupted (D53).
 */
export const CompactionJobOutcome = Schema.Union([
  Schema.TaggedStruct("Committed", { manifestId: ManifestId }),
  Schema.TaggedStruct("Discarded", { reason: Schema.String }),
  CorruptedCompactionInput,
]);

/** How a compaction job ended. */
export type CompactionJobOutcome = typeof CompactionJobOutcome.Type;

/**
 * Runs one named durable step whose result `schema` encodes; the Workflow persists and replays it,
 * tests run it directly.
 */
export type CompactionStepRunner<Requires = never> = <A, I, E, R>(
  name: string,
  schema: Schema.Codec<A, I>,
  step: Effect.Effect<A, E, R>,
) => Effect.Effect<A, E, R | Requires>;

/**
 * A step's result, why the job was discarded, or the corrupted input that stopped it. Discards and
 * corruption are results, so the Workflow never retries them.
 */
type Discardable<A> =
  | { readonly _tag: "Done"; readonly value: A }
  | { readonly _tag: "Discarded"; readonly reason: string }
  | typeof CorruptedCompactionInput.Type;

const discardable = <A, I>(schema: Schema.Codec<A, I>): Schema.Codec<Discardable<A>, unknown> =>
  Schema.Union([
    Schema.TaggedStruct("Done", { value: schema }),
    Schema.TaggedStruct("Discarded", { reason: Schema.String }),
    CorruptedCompactionInput,
  ]);

/**
 * Turns the errors that discard a job into a `Discarded` result, and a corrupted input into a
 * `Corrupted` result: retrying cannot fix it, and the compactor must learn which SST it was.
 */
const discardOnTags = <A, R>(
  effect: Effect.Effect<A, CompactionError, R>,
): Effect.Effect<Discardable<A>, CompactionUnavailableError, R> => {
  const discarded = (error: CompactionDiscardedError) =>
    Effect.succeed<Discardable<A>>({ _tag: "Discarded", reason: error.message });
  return effect.pipe(
    Effect.map((value): Discardable<A> => ({ _tag: "Done", value })),
    Effect.catchTags({
      CompactorFencedError: discarded,
      CompactionInputsChangedError: discarded,
      SstBelowGcBoundaryError: discarded,
      DatabaseNotFoundError: discarded,
      SstCorruptedError: (error) =>
        Effect.succeed<Discardable<A>>({
          _tag: "Corrupted",
          sstId: error.sstId,
          section: error.section,
          reason: error.reason,
          message: error.message,
        }),
    }),
  );
};

const Committed = Schema.Struct({ manifestId: ManifestId });

/** A job that ends before committing; a corrupted input is logged as an error with its SST. */
const finishedEarly = (
  params: CompactionJobParams,
  outcome: Exclude<Discardable<unknown>, { readonly _tag: "Done" }>,
) =>
  Effect.gen(function* () {
    if (outcome._tag === "Corrupted") {
      yield* Effect.annotateCurrentSpan({
        "compaction.outcome": "Corrupted",
        "sst.id": outcome.sstId,
        "sst.corruption": outcome.reason,
      });
      yield* Effect.logError(
        `Compaction job ${params.jobId} of database ${params.databaseId} stopped: input SST ${outcome.sstId} is corrupted (${outcome.reason} in ${outcome.section}).`,
      );
    } else {
      yield* Effect.annotateCurrentSpan({ "compaction.outcome": "Discarded" });
    }
    const ended: CompactionJobOutcome = outcome;
    return ended;
  });

/**
 * Runs a compaction job as durable steps: `prepare`, `output-<n>` (one output SST each), `commit`.
 *
 * **Details**
 *
 * Each step's result is persisted by `runStep`, so a resumed job continues at the first unfinished
 * step with the same inputs, time, and start key. A step that fails with
 * `CompactionUnavailableError` is retried by the runner; a discard ends the job with a `Discarded`
 * outcome (its outputs are orphans GC removes); a corrupted input SST ends it with a `Corrupted`
 * outcome naming the SST, which the compactor records and stops compacting (D53).
 */
export const runCompactionJob = <Requires>(
  params: CompactionJobParams,
  runStep: CompactionStepRunner<Requires>,
): Effect.Effect<
  CompactionJobOutcome,
  CompactionUnavailableError,
  CompactionExecutor | RuntimeContext | Requires
> =>
  Effect.gen(function* () {
    const executor = yield* CompactionExecutor;
    const prepared = yield* runStep(
      "prepare",
      discardable(PreparedCompaction),
      discardOnTags(executor.prepare(params.databaseId, params.compactorEpoch, params.job)),
    );
    if (prepared._tag !== "Done") return yield* finishedEarly(params, prepared);
    const outputs: Array<SstHandle> = [];
    let start = Option.none<Uint8Array>();
    let index = 0;
    do {
      const step = yield* runStep(
        `output-${index}`,
        discardable(CompactionOutputStep),
        discardOnTags(executor.writeOutput(prepared.value, start)),
      );
      if (step._tag !== "Done") return yield* finishedEarly(params, step);
      outputs.push(...Option.toArray(step.value.output));
      start = step.value.resumeFrom;
      index++;
    } while (Option.isSome(start));
    const committed = yield* runStep(
      "commit",
      discardable(Committed),
      discardOnTags(
        Effect.map(executor.commit(prepared.value, outputs), (manifest) => ({
          manifestId: manifest.id,
        })),
      ),
    );
    if (committed._tag !== "Done") return yield* finishedEarly(params, committed);
    return { _tag: "Committed" as const, manifestId: committed.value.manifestId };
  }).pipe(
    Effect.withSpan("CompactionJob.run", {
      attributes: { "database.id": params.databaseId, "compaction.job_id": params.jobId },
    }),
  );
