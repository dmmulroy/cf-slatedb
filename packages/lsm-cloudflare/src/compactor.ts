import {
  CompactionJob,
  CompactorEpoch,
  DatabaseId,
  fenceCompactor,
  generateCompactionJobId,
  planCompactions,
  Timestamp,
} from "@cf-slatedb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Clock, Context, Effect, Layer, Metric, Option, Result, Schema } from "effect";

import { CompactionBudget } from "./compaction-budget.ts";
import { CompactionLauncher } from "./compaction-launcher.ts";
import { type DurableObjectCell, inMemoryCell } from "./durable-object-cell.ts";
import { DurableObjectAlarm } from "./durable-object-alarm.ts";
import { LsmConfiguration } from "./lsm-configuration.ts";
import { DatabaseIdMismatchError, DatabaseNotFoundError } from "./lsm-errors.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { ManifestStore } from "./manifest-store.ts";

/** Schema for one compaction the compactor started and has not yet seen finish. */
export const RunningCompaction = Schema.Struct({
  jobId: Schema.String,
  job: CompactionJob,
  budgetGroup: Schema.String,
  startedAt: Timestamp,
});

/** One running compaction. */
export type RunningCompaction = typeof RunningCompaction.Type;

/**
 * Schema for a compactor's durable state: the database it serves, its epoch, its running jobs, and
 * whether a newer compactor fenced it.
 */
export const CompactorState = Schema.Struct({
  databaseId: DatabaseId,
  compactorEpoch: CompactorEpoch,
  running: Schema.Array(RunningCompaction),
  fenced: Schema.Boolean,
});

/** A compactor's durable state; also its status. */
export type CompactorState = typeof CompactorState.Type;

/** The compactor's durable state cell (Durable Object storage in production). */
export class CompactorStateCell extends Context.Service<
  CompactorStateCell,
  DurableObjectCell<CompactorState>
>()("@cf-slatedb/CompactorStateCell") {}

/**
 * The compactor could not reach the manifest to start. Retryable: wake it again.
 */
export class CompactorUnavailableError extends Schema.TaggedError<CompactorUnavailableError>()(
  "CompactorUnavailableError",
  { databaseId: DatabaseId, message: Schema.String },
) {}

/** Every failure waking or inspecting a compactor can report. */
export type CompactorError =
  | CompactorUnavailableError
  | DatabaseNotFoundError
  | DatabaseIdMismatchError;

/** Compaction jobs started (SlateDB RFC-0021 parity). */
export const compactionsStarted = Metric.counter("lsm_compactions_started", {
  description: "Compaction jobs the compactor started",
});

/**
 * One database's compactor: plans size-tiered compactions and starts each as a durable job.
 *
 * **Details**
 *
 * On its first wake it fences in (increments `compactorEpoch` by manifest CAS) and keeps the epoch
 * in durable storage, so jobs it started stay valid across evictions; a newer compactor fences it
 * and it stops. While jobs run or wait for budget, its alarm polls every `compactorPollInterval`:
 * it drops finished jobs (releasing their budget leases), renews the rest, and plans more. With no
 * work it sets no alarm, so an idle database costs nothing; the writer wakes it after a flush.
 */
export interface ICompactor {
  /** Fences in on first use and polls now. @returns The state after waking. */
  readonly wake: (
    databaseId: DatabaseId,
  ) => Effect.Effect<CompactorState, CompactorError, RuntimeContext>;
  /** The compactor's state, if it was ever woken. */
  readonly status: (
    databaseId: DatabaseId,
  ) => Effect.Effect<Option.Option<CompactorState>, DatabaseIdMismatchError, RuntimeContext>;
  /** The alarm handler: polls, and schedules the next poll while there is work. Never fails. */
  readonly runAlarm: Effect.Effect<void, never, RuntimeContext>;
}

/** The contextual compactor. */
export class Compactor extends Context.Service<Compactor, ICompactor>()("@cf-slatedb/Compactor") {}

/** Constructs the compactor over its state cell, alarm, manifests, launcher, budget, and settings. */
export const makeCompactor = Effect.gen(function* () {
  const cell = yield* CompactorStateCell;
  const alarm = yield* DurableObjectAlarm;
  const reader = yield* ManifestReader;
  const manifests = yield* ManifestStore;
  const launcher = yield* CompactionLauncher;
  const budget = yield* CompactionBudget;
  const settings = yield* LsmConfiguration;

  const unavailable = (databaseId: DatabaseId) => (error: { readonly message: string }) =>
    Effect.fail(
      new CompactorUnavailableError({
        databaseId,
        message: `The compactor of database ${databaseId} could not reach its manifest: ${error.message} Wake it again.`,
      }),
    );

  const checkDatabase = (state: CompactorState, databaseId: DatabaseId) =>
    state.databaseId === databaseId
      ? Effect.void
      : Effect.fail(
          new DatabaseIdMismatchError({
            databaseId,
            servedDatabaseId: state.databaseId,
            message: `This compactor serves database ${state.databaseId}, not ${databaseId}. Address the compactor by the database id.`,
          }),
        );

  const holder = (state: CompactorState, jobId: string) => `${state.databaseId}/${jobId}`;

  /** The state, fencing in first when this compactor has none. */
  const ensureState = (databaseId: DatabaseId) =>
    Effect.gen(function* () {
      const existing = yield* cell.get;
      if (Option.isSome(existing)) {
        yield* checkDatabase(existing.value, databaseId);
        return existing.value;
      }
      const fenced = yield* manifests
        .update(databaseId, (manifest) => Result.succeed(fenceCompactor(manifest)))
        .pipe(
          Effect.catchTags({
            ManifestReadError: unavailable(databaseId),
            ManifestWriteError: unavailable(databaseId),
          }),
        );
      const state: CompactorState = {
        databaseId,
        compactorEpoch: fenced.manifest.compactorEpoch,
        running: [],
        fenced: false,
      };
      yield* cell.set(state);
      yield* Effect.annotateCurrentSpan({
        "compactor.epoch": state.compactorEpoch,
        "manifest.id": fenced.id,
      });
      return state;
    });

  /** Polls at `atMillis` unless a poll is already due sooner. */
  const pollBy = (atMillis: number) =>
    Effect.gen(function* () {
      const scheduled = yield* alarm.scheduled;
      if (Option.isNone(scheduled) || scheduled.value > atMillis) yield* alarm.scheduleAt(atMillis);
    });

  /**
   * One poll: drop finished jobs, renew the rest, plan and start more.
   *
   * @returns Whether to poll again (jobs running or waiting for budget).
   */
  const poll = (state: CompactorState) =>
    Effect.gen(function* () {
      const { databaseId } = state;
      const latest = yield* reader.loadLatest(databaseId);
      const group = Option.getOrElse(latest.manifest.options.budgetGroup, () => databaseId);
      if (latest.manifest.compactorEpoch !== state.compactorEpoch) {
        // A newer compactor took over: its epoch fences every job this one started.
        for (const job of state.running)
          yield* Effect.ignore(budget.release(holder(state, job.jobId)));
        yield* cell.set({ ...state, running: [], fenced: true });
        yield* Effect.annotateCurrentSpan({ "compactor.fenced": true });
        return false;
      }
      const running: Array<RunningCompaction> = [];
      for (const job of state.running) {
        const stillRunning = yield* launcher
          .isRunning(job.jobId)
          .pipe(
            Effect.catchTags({ CompactionLauncherUnavailableError: () => Effect.succeed(true) }),
          );
        if (stillRunning) {
          running.push(job);
          yield* Effect.ignore(budget.acquire(job.budgetGroup, holder(state, job.jobId)));
        } else {
          yield* Effect.ignore(budget.release(holder(state, job.jobId)));
        }
      }
      const planned = planCompactions(
        latest.manifest,
        running.map((job) => job.job),
        settings,
      );
      let waitingForBudget = false;
      for (const job of planned) {
        const jobId = yield* generateCompactionJobId;
        const granted = yield* budget
          .acquire(group, holder(state, jobId))
          .pipe(
            Effect.catchTags({ CompactionBudgetUnavailableError: () => Effect.succeed(false) }),
          );
        if (!granted) {
          waitingForBudget = true;
          break;
        }
        const started = yield* launcher
          .start({ databaseId, compactorEpoch: state.compactorEpoch, jobId, job })
          .pipe(
            Effect.as(true),
            Effect.catchTags({ CompactionLauncherUnavailableError: () => Effect.succeed(false) }),
          );
        if (!started) {
          yield* Effect.ignore(budget.release(holder(state, jobId)));
          waitingForBudget = true;
          break;
        }
        yield* Metric.update(compactionsStarted, 1);
        running.push({
          jobId,
          job,
          budgetGroup: group,
          startedAt: Timestamp.make(yield* Clock.currentTimeMillis),
        });
      }
      yield* cell.set({ ...state, running });
      yield* Effect.annotateCurrentSpan({
        "database.id": databaseId,
        "manifest.id": latest.id,
        "compaction.running": running.length,
        "compaction.started": running.length - state.running.length,
        "compaction.waiting_for_budget": waitingForBudget,
      });
      return running.length > 0 || waitingForBudget;
    }).pipe(Effect.withSpan("Compactor.poll"));

  const wake = Effect.fn("Compactor.wake")(function* (databaseId: DatabaseId) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    const state = yield* ensureState(databaseId);
    if (!state.fenced) yield* pollBy(yield* Clock.currentTimeMillis);
    return state;
  });

  const status = Effect.fn("Compactor.status")(function* (databaseId: DatabaseId) {
    const state = yield* cell.get;
    if (Option.isSome(state)) yield* checkDatabase(state.value, databaseId);
    return state;
  });

  const runAlarm = Effect.gen(function* () {
    const state = yield* cell.get;
    if (Option.isNone(state) || state.value.fenced) return;
    const again = yield* poll(state.value).pipe(
      Effect.catchTags({ DatabaseNotFoundError: () => Effect.succeed(false) }),
      // Manifest unreadable now: poll again later.
      Effect.catchTags({ ManifestReadError: () => Effect.succeed(true) }),
    );
    if (again) yield* pollBy((yield* Clock.currentTimeMillis) + settings.compactorPollInterval);
  }).pipe(Effect.withSpan("Compactor.alarm"));

  return Compactor.of({ wake, status, runAlarm });
});

/** Provides the compactor, leaving its state cell, alarm, manifests, launcher, and budget visible. */
export const compactorLayerWithoutDependencies = Layer.effect(Compactor, makeCompactor);

/** Provides a compactor state cell held in memory, for tests. */
export const compactorStateCellTestLayer = Layer.effect(
  CompactorStateCell,
  inMemoryCell<CompactorState>(),
);
