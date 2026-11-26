import {
  CompactionJob,
  CompactionSource,
  CompactorEpoch,
  DatabaseId,
  DroppedSst,
  dropSstFromManifest,
  fenceCompactor,
  generateCompactionJobId,
  type Manifest,
  ManifestId,
  planCompactionsAround,
  SstId,
  SstNotInManifestError,
  Timestamp,
} from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Clock, Context, Effect, Layer, Metric, Option, Result, Schema, Semaphore } from "effect";

import { CompactionBudget } from "./compaction-budget.ts";
import { CompactionLauncher, type CompactionJobProgress } from "./compaction-launcher.ts";
import { type DurableObjectCell, inMemoryCell } from "./durable-object-cell.ts";
import { DurableObjectAlarm } from "./durable-object-alarm.ts";
import {
  GarbageCollector,
  type GarbageCollectionReport,
  type GarbageCollectorUnavailableError,
} from "./garbage-collector.ts";
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
 * Schema for an SST a compaction found corrupted (D53): the compactor no longer plans jobs that read
 * its source, and reports it until the SST leaves the database's manifest (repaired or replaced).
 */
export const CorruptSst = Schema.Struct({
  sstId: Schema.String,
  /** The L0 SST or sorted run that holds it, as the compactor schedules it. */
  source: Schema.OptionFromNullOr(CompactionSource),
  section: Schema.String,
  reason: Schema.String,
  message: Schema.String,
  detectedAt: Timestamp,
});

/** An SST a compaction found corrupted. */
export type CorruptSst = typeof CorruptSst.Type;

/**
 * Schema for a compactor's durable state: the database it serves, its epoch, its running jobs,
 * whether a newer epoch fenced it, the corrupted SSTs it found (absent in state written before
 * D53, which decodes as none), and when its database's garbage collection is next due (absent in
 * state written before D60: none until the next compaction or request).
 *
 * **Details**
 *
 * Every write goes through `updateState`, which applies a function to the stored state and writes
 * the result with no other work in between (D78). Each field has owners, and a writer changes only
 * its own fields of the state it is given:
 *
 * - `compactorEpoch`, `fenced`: the fence-in (`ensureState`), and a poll that finds a newer epoch.
 * - `running`, `corruptSsts`: a poll, and an SST drop (`corruptSsts` only); both under `planLock`.
 * - `collectionDueAt`: `requestCollection` and a poll only bring it forward; a collection sets it
 *   from its report unless a request arrived while it ran; the alarm's retry sets it.
 */
export const CompactorState = Schema.Struct({
  /** The format it was written in ({@link COMPACTOR_STATE_FORMAT_VERSION}); 1 when absent (D64). */
  formatVersion: Schema.Int.pipe(Schema.withDecodingDefaultKey(Effect.succeed(1))),
  databaseId: DatabaseId,
  compactorEpoch: CompactorEpoch,
  running: Schema.Array(RunningCompaction),
  fenced: Schema.Boolean,
  corruptSsts: Schema.Array(CorruptSst).pipe(Schema.withDecodingDefaultKey(Effect.succeed([]))),
  collectionDueAt: Schema.OptionFromOptionalKey(Timestamp),
});

/**
 * The newest compactor-state format this code reads and writes (D64). A compactor refuses to run on
 * state a newer version wrote, instead of rewriting it in an older format.
 */
export const COMPACTOR_STATE_FORMAT_VERSION = 1;

/** A compactor's durable state; also its status. */
export type CompactorState = typeof CompactorState.Type;

/** The compactor's durable state cell (Durable Object storage in production). */
export class CompactorStateCell extends Context.Service<
  CompactorStateCell,
  DurableObjectCell<CompactorState>
>()("@shaledb/CompactorStateCell") {}

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

/** Schema for an operator's request to drop one SST from the manifest (D62). */
export const SstDropRequest = Schema.Struct({
  sstId: SstId,
  /** Drop it even though no compaction reported it corrupted. */
  force: Schema.Boolean,
});

/** An operator's request to drop one SST. */
export type SstDropRequest = typeof SstDropRequest.Type;

/** Schema for a finished drop: the manifest that no longer holds the SST, and what was lost. */
export const SstDropReport = Schema.Struct({ manifestId: ManifestId, dropped: DroppedSst });

/** A finished drop. */
export type SstDropReport = typeof SstDropReport.Type;

/**
 * An SST drop was refused (D62), and nothing changed.
 *
 * **Details**
 *
 * - `NotReportedCorrupted`: no compaction reported the SST corrupted. Not retryable as is: check the
 *   SST id, or send `force` to drop a healthy SST, losing its data.
 * - `CompactionRunning`: a running compaction reads the SST's source, and its output would bring
 *   the SST's rows back. Retryable once the compaction finishes (seconds to minutes).
 * - `ConfirmationMismatch`: `confirm` did not repeat the SST id. Correct it and retry.
 */
export class SstDropRefusedError extends Schema.TaggedError<SstDropRefusedError>()(
  "SstDropRefusedError",
  {
    databaseId: DatabaseId,
    sstId: SstId,
    reason: Schema.Literals(["NotReportedCorrupted", "CompactionRunning", "ConfirmationMismatch"]),
    message: Schema.String,
  },
) {}

/** Every failure dropping an SST can report. */
export type SstDropError = CompactorError | SstDropRefusedError | SstNotInManifestError;

/** SSTs operators dropped from manifests (D62). */
export const sstsDropped = Metric.counter("lsm_ssts_dropped", {
  description: "SSTs removed from a manifest by an operator's repair",
});

/** Corrupted SSTs compactions found (D53); each also stops the compactor from reading its source. */
export const compactionCorruptSsts = Metric.counter("lsm_compaction_corrupt_ssts", {
  description: "Input SSTs a compaction found corrupted",
});

/** The scheduling source in `manifest` that holds SST `sstId`, among `job`'s sources. */
const sourceHolding = (
  job: CompactionJob,
  manifest: Manifest,
  sstId: string,
): Option.Option<CompactionSource> =>
  Option.fromUndefinedOr(
    job.sources.find((source) =>
      source._tag === "L0"
        ? source.sstId === sstId
        : manifest.sortedRuns.some(
            (run) => run.id === source.id && run.ssts.some((sst) => sst.id === sstId),
          ),
    ),
  );

/** The L0 SST or sorted run of `manifest` that holds SST `sstId`. */
const sourceInManifest = (manifest: Manifest, sstId: string): Option.Option<CompactionSource> => {
  if (manifest.l0.some((sst) => sst.id === sstId)) {
    return Option.some({ _tag: "L0", sstId: SstId.make(sstId) });
  }
  const run = manifest.sortedRuns.find((candidate) =>
    candidate.ssts.some((sst) => sst.id === sstId),
  );
  return run === undefined ? Option.none() : Option.some({ _tag: "SortedRun", id: run.id });
};

/** Whether two compaction sources name the same L0 SST or sorted run. */
const sameSource = (left: CompactionSource, right: CompactionSource) =>
  left._tag === "L0"
    ? right._tag === "L0" && left.sstId === right.sstId
    : right._tag === "SortedRun" && left.id === right.id;

/** Whether `manifest` still holds SST `sstId` (the corruption is still there to report). */
const manifestHolds = (manifest: Manifest, sstId: string) =>
  manifest.l0.some((sst) => sst.id === sstId) ||
  manifest.sortedRuns.some((run) => run.ssts.some((sst) => sst.id === sstId));

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
 * in durable storage, so jobs it started stay valid across evictions. Concurrent first callers
 * share one fence-in. A compactor that finds a newer epoch stops its jobs, and its next start
 * (a wake, a requested collection, an SST drop, or its alarm) fences in again (D78). While jobs run or wait for budget, its alarm polls every `compactorPollInterval`:
 * it drops finished jobs (releasing their budget leases), renews the rest, and plans more. With no
 * work it sets no alarm, so an idle database costs nothing; the writer wakes it after a flush.
 *
 * It also owns its database's garbage collection (D60): due `gcMinAge` after a compaction finishes
 * (its replaced inputs and manifests are then old enough), then again when the last collection says
 * something it kept becomes collectable. The alarm runs it; one collection runs at a time.
 */
export interface ICompactor {
  /** Fences in on first use (or again after being fenced) and polls now. @returns The state. */
  readonly wake: (
    databaseId: DatabaseId,
  ) => Effect.Effect<CompactorState, CompactorError, RuntimeContext>;
  /** The compactor's state, if it was ever woken. */
  readonly status: (
    databaseId: DatabaseId,
  ) => Effect.Effect<Option.Option<CompactorState>, DatabaseIdMismatchError, RuntimeContext>;
  /**
   * Forgets the compactor's state and alarm for a database being removed (its running jobs fail
   * to commit against the removed manifest). Safe to repeat.
   */
  readonly destroy: (
    databaseId: DatabaseId,
  ) => Effect.Effect<void, DatabaseIdMismatchError, RuntimeContext>;
  /**
   * Collects the database's garbage now, under the compactor's collection lock (the operator's
   * "collect now"), and schedules the next collection from the report. Needs no fenced compactor:
   * a database without a manifest has its leftovers swept.
   *
   * @returns The report, `GarbageCollectorUnavailableError`, or `DatabaseIdMismatchError`.
   */
  readonly collect: (
    databaseId: DatabaseId,
  ) => Effect.Effect<
    GarbageCollectionReport,
    GarbageCollectorUnavailableError | DatabaseNotFoundError | DatabaseIdMismatchError,
    RuntimeContext
  >;
  /**
   * Makes a collection due now and sets the alarm (the maintenance sweep asks this of a database
   * whose manifests piled up uncollected). Fences in on first use.
   */
  readonly requestCollection: (
    databaseId: DatabaseId,
  ) => Effect.Effect<void, CompactorError, RuntimeContext>;
  /**
   * Drops one SST from the manifest: the operator's repair of a corrupted SST (D62). Its versions of
   * the keys it held are lost, and compaction of its source resumes.
   *
   * **Details**
   *
   * Runs under the planning lock, so no poll plans a job between the checks and the commit. Refuses
   * an SST no compaction reported corrupted unless `force`, and an SST whose source a running job
   * reads (that job's output would bring its rows back). Records the drop in the manifest's
   * `droppedSsts`, forgets the corruption report, and polls. Dropping an SST already dropped
   * returns the recorded drop.
   *
   * @returns The manifest without the SST and what was dropped, `SstDropRefusedError`,
   *   `SstNotInManifestError`, or a `CompactorError`.
   */
  readonly dropSst: (
    databaseId: DatabaseId,
    request: SstDropRequest,
  ) => Effect.Effect<SstDropReport, SstDropError, RuntimeContext>;
  /**
   * The alarm handler: runs a due collection, polls, and schedules the next poll while there is
   * work and the next collection when one is due. Never fails.
   */
  readonly runAlarm: Effect.Effect<void, never, RuntimeContext>;
}

/** The contextual compactor. */
export class Compactor extends Context.Service<Compactor, ICompactor>()("@shaledb/Compactor") {}

/** Constructs the compactor over its state cell, alarm, manifests, launcher, budget, and settings. */
export const makeCompactor = Effect.gen(function* () {
  const cell = yield* CompactorStateCell;
  const alarm = yield* DurableObjectAlarm;
  const reader = yield* ManifestReader;
  const manifests = yield* ManifestStore;
  const launcher = yield* CompactionLauncher;
  const budget = yield* CompactionBudget;
  const settings = yield* LsmConfiguration;
  const collector = yield* GarbageCollector;
  // Locks, always taken in this order, none of them held while waiting for a later one's holder
  // to take an earlier one (Effect's semaphores are not reentrant):
  //
  // 1. `planLock`: planning (a poll) and an operator's SST drop exclude each other (D62).
  // 2. `collectionLock`: one collection per database at a time (the alarm and an operator's
  //    request may overlap). Never held together with `planLock` or `startLock`.
  // 3. `startLock`: one fence-in at a time; concurrent first callers share its result (D78). Taken
  //    under `planLock` by an SST drop.
  // 4. `stateLock`: one read-modify-write of the state cell at a time, around no other I/O.
  const planLock = yield* Semaphore.make(1);
  const collectionLock = yield* Semaphore.make(1);
  const startLock = yield* Semaphore.make(1);
  const stateLock = yield* Semaphore.make(1);

  /**
   * Applies `update` to the stored state and writes the result, with no other work in between:
   * the only way the state is written (D78). `update` returns none to leave the state as it is.
   *
   * @returns The state stored afterwards.
   */
  const updateState = (
    update: (current: Option.Option<CompactorState>) => Option.Option<CompactorState>,
  ) =>
    stateLock.withPermits(1)(
      Effect.gen(function* () {
        const current = yield* cell.get;
        const next = update(current);
        if (Option.isNone(next)) return current;
        yield* cell.set(next.value);
        return next;
      }),
    );

  /** `updateState` for a change to an existing state of `databaseId`; none leaves no state. */
  const updateExisting = (
    databaseId: DatabaseId,
    update: (current: CompactorState) => CompactorState,
  ) =>
    updateState((current) =>
      Option.filter(current, (state) => state.databaseId === databaseId).pipe(Option.map(update)),
    );

  /** Whether two optional times are the same. */
  const sameTime = (left: Option.Option<Timestamp>, right: Option.Option<Timestamp>) =>
    Option.isNone(left) ? Option.isNone(right) : Option.contains(right, left.value);

  /** The earlier of two optional times; none is never. */
  const earlier = (left: Option.Option<Timestamp>, right: Option.Option<Timestamp>) =>
    Option.isNone(left)
      ? right
      : Option.isNone(right)
        ? left
        : Option.some(Timestamp.make(Math.min(left.value, right.value)));

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

  /** Refuses state a newer shaledb wrote: this one would rewrite it in an older format. */
  const checkFormat = (state: CompactorState) =>
    state.formatVersion <= COMPACTOR_STATE_FORMAT_VERSION
      ? Effect.void
      : Effect.fail(
          new CompactorUnavailableError({
            databaseId: state.databaseId,
            message: `The compactor state of database ${state.databaseId} was written by a newer shaledb (format ${state.formatVersion}; this version reads up to format ${COMPACTOR_STATE_FORMAT_VERSION}), so this version does not compact it. Deploy that version or a later one.`,
          }),
        );

  /**
   * The state, fencing in first when this compactor has none or was fenced: the compactor's start.
   *
   * **Details**
   *
   * Single-flight (`startLock`, D78): concurrent first callers share one `fenceCompactor` commit.
   * Without it, two cold wakes each fenced in, and the older epoch's state, written back by a poll
   * that began before the newer fence, left the compactor fenced by itself for good.
   *
   * A fenced state is the compactor process that exited, as in SlateDB: the next start fences in
   * again with a new epoch. That discards every job of an older epoch (`commitCompaction` checks
   * the epoch), and their budget leases are released here. Only this Durable Object fences in for
   * its database, so a newer epoch it did not store means its own state was lost or overwritten,
   * and fencing in again is the repair. No second live instance exists to fence it back.
   */
  const ensureState = (databaseId: DatabaseId) =>
    startLock.withPermits(1)(
      Effect.gen(function* () {
        const existing = yield* cell.get;
        if (Option.isSome(existing)) {
          yield* checkDatabase(existing.value, databaseId);
          yield* checkFormat(existing.value);
          if (!existing.value.fenced) return existing.value;
          for (const job of existing.value.running)
            yield* Effect.ignore(budget.release(holder(existing.value, job.jobId)));
        }
        const fenced = yield* manifests
          .update(databaseId, (manifest) => Result.succeed(fenceCompactor(manifest)))
          .pipe(
            Effect.catchTags({
              ManifestReadError: unavailable(databaseId),
              ManifestWriteError: unavailable(databaseId),
            }),
          );
        const stored = yield* updateState((current) =>
          Option.some(
            Option.match(
              Option.filter(current, (state) => state.databaseId === databaseId),
              {
                onNone: (): CompactorState => ({
                  formatVersion: COMPACTOR_STATE_FORMAT_VERSION,
                  databaseId,
                  compactorEpoch: fenced.manifest.compactorEpoch,
                  running: [],
                  fenced: false,
                  corruptSsts: [],
                  collectionDueAt: Option.none(),
                }),
                // Fencing in again keeps what is still true: corruption reports (a poll drops
                // those whose SST left the manifest) and when collection is due.
                onSome: (state): CompactorState => ({
                  ...state,
                  formatVersion: COMPACTOR_STATE_FORMAT_VERSION,
                  compactorEpoch: fenced.manifest.compactorEpoch,
                  running: [],
                  fenced: false,
                }),
              },
            ),
          ),
        );
        yield* Effect.annotateCurrentSpan({
          "compactor.epoch": fenced.manifest.compactorEpoch,
          "compactor.refenced": Option.isSome(existing),
          "manifest.id": fenced.id,
        });
        return Option.getOrThrow(stored);
      }),
    );

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
        // A newer epoch fences every job this one started. The next start fences in again (D78).
        for (const job of state.running)
          yield* Effect.ignore(budget.release(holder(state, job.jobId)));
        yield* updateExisting(databaseId, (current) =>
          current.compactorEpoch === state.compactorEpoch
            ? { ...current, running: [], fenced: true }
            : current,
        );
        yield* Effect.annotateCurrentSpan({ "database.id": databaseId, "compactor.fenced": true });
        return false;
      }
      const running: Array<RunningCompaction> = [];
      // Corruption already reported stays until its SST leaves the manifest.
      const corruptSsts = state.corruptSsts.filter((corrupt) =>
        manifestHolds(latest.manifest, corrupt.sstId),
      );
      for (const job of state.running) {
        const progress = yield* launcher.progress(job.jobId).pipe(
          Effect.catchTags({
            CompactionLauncherUnavailableError: () =>
              Effect.succeed<CompactionJobProgress>({ _tag: "Running" }),
          }),
        );
        if (progress._tag === "Running") {
          running.push(job);
          yield* Effect.ignore(budget.acquire(job.budgetGroup, holder(state, job.jobId)));
          continue;
        }
        yield* Effect.ignore(budget.release(holder(state, job.jobId)));
        const found = Option.filter(progress.outcome, (outcome) => outcome._tag === "Corrupted");
        if (
          Option.isSome(found) &&
          found.value._tag === "Corrupted" &&
          !corruptSsts.some((corrupt) => corrupt.sstId === found.value.sstId)
        ) {
          const corrupted = found.value;
          corruptSsts.push({
            sstId: corrupted.sstId,
            source: sourceHolding(job.job, latest.manifest, corrupted.sstId),
            section: corrupted.section,
            reason: corrupted.reason,
            message: corrupted.message,
            detectedAt: Timestamp.make(yield* Clock.currentTimeMillis),
          });
          yield* Metric.update(compactionCorruptSsts, 1);
          yield* Effect.logError(
            `Database ${databaseId}'s SST ${corrupted.sstId} is corrupted (${corrupted.reason} in ${corrupted.section}); the compactor no longer compacts it. Restore the SST or remove it from the manifest; reads of it fail with SstCorrupted.`,
          );
        }
      }
      if (corruptSsts.length > 0) {
        yield* Effect.annotateCurrentSpan({
          "compaction.corrupt_ssts": corruptSsts.length,
          "sst.id": corruptSsts.map((corrupt) => corrupt.sstId).join(","),
        });
      }
      const planned = planCompactionsAround(
        latest.manifest,
        running.map((job) => job.job),
        corruptSsts.flatMap((corrupt) => Option.toArray(corrupt.source)),
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
      // A finished job left garbage (its inputs, older manifests): collectable after gcMinAge.
      const anyFinished = state.running.some(
        (job) => !running.some((still) => still.jobId === job.jobId),
      );
      const finishedDueAt = anyFinished
        ? Option.some(Timestamp.make((yield* Clock.currentTimeMillis) + settings.gcMinAge))
        : Option.none<Timestamp>();
      // Written onto the state as it is now, not as it was when the poll began: a request or a
      // collection may have changed `collectionDueAt` meanwhile (D78). A fence-in since then owns
      // the state; this poll's jobs carry the old epoch and will be discarded.
      const stored = yield* updateExisting(databaseId, (current) =>
        current.compactorEpoch === state.compactorEpoch && !current.fenced
          ? {
              ...current,
              running,
              corruptSsts,
              collectionDueAt: earlier(current.collectionDueAt, finishedDueAt),
            }
          : current,
      );
      const kept = Option.exists(
        stored,
        (current) => current.compactorEpoch === state.compactorEpoch && !current.fenced,
      );
      if (!kept) {
        for (const job of running) yield* Effect.ignore(budget.release(holder(state, job.jobId)));
        return false;
      }
      yield* Effect.annotateCurrentSpan({
        "database.id": databaseId,
        "manifest.id": latest.id,
        // The compaction backlog an operator watches (D63).
        "manifest.l0_ssts": latest.manifest.l0.length,
        "compaction.running": running.length,
        "compaction.started": running.length - state.running.length,
        "compaction.waiting_for_budget": waitingForBudget,
      });
      return running.length > 0 || waitingForBudget;
    }).pipe(Effect.withSpan("Compactor.poll"));

  const wake = Effect.fn("Compactor.wake")(function* (databaseId: DatabaseId) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    const state = yield* ensureState(databaseId);
    yield* pollBy(yield* Clock.currentTimeMillis);
    return state;
  });

  const status = Effect.fn("Compactor.status")(function* (databaseId: DatabaseId) {
    const state = yield* cell.get;
    if (Option.isSome(state)) yield* checkDatabase(state.value, databaseId);
    return state;
  });

  const destroy = Effect.fn("Compactor.destroy")(function* (databaseId: DatabaseId) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    const state = yield* cell.get;
    if (Option.isSome(state)) yield* checkDatabase(state.value, databaseId);
    yield* alarm.cancel;
    yield* stateLock.withPermits(1)(cell.clear);
  });

  /** Runs one collection and records when the next is due. */
  const runCollection = (databaseId: DatabaseId) =>
    collectionLock.withPermits(1)(
      Effect.gen(function* () {
        const before = yield* cell.get;
        const dueBefore = Option.flatMap(before, (state) => state.collectionDueAt);
        const report = yield* collector.collect(databaseId);
        // A request or a finished compaction that moved the due time during this collection
        // stands: the report predates it (D78).
        const stored = yield* updateExisting(databaseId, (state) => ({
          ...state,
          collectionDueAt: sameTime(state.collectionDueAt, dueBefore)
            ? report.nextCollectionAt
            : earlier(state.collectionDueAt, report.nextCollectionAt),
        }));
        const due = Option.flatMap(stored, (state) => state.collectionDueAt);
        if (Option.isSome(due)) yield* pollBy(due.value);
        return report;
      }),
    );

  const collect = Effect.fn("Compactor.collect")(function* (databaseId: DatabaseId) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    const state = yield* cell.get;
    if (Option.isSome(state)) yield* checkDatabase(state.value, databaseId);
    return yield* runCollection(databaseId);
  });

  const requestCollection = Effect.fn("Compactor.requestCollection")(function* (
    databaseId: DatabaseId,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    yield* ensureState(databaseId);
    const now = yield* Clock.currentTimeMillis;
    yield* updateExisting(databaseId, (state) => ({
      ...state,
      collectionDueAt: Option.some(Timestamp.make(now)),
    }));
    yield* pollBy(now);
  });

  const runAlarm = Effect.gen(function* () {
    const stored = yield* cell.get;
    if (Option.isNone(stored)) return;
    if (stored.value.formatVersion > COMPACTOR_STATE_FORMAT_VERSION) {
      return yield* Effect.logError(
        `The compactor of database ${stored.value.databaseId} is idle: its state was written by a newer shaledb (format ${stored.value.formatVersion}). Deploy that version or a later one.`,
      );
    }
    // An alarm is a start too: a fenced compactor fences in again before it collects or polls (D78).
    const before = stored.value.fenced
      ? yield* ensureState(stored.value.databaseId).pipe(
          Effect.map(Option.some),
          Effect.catchTags({
            DatabaseNotFoundError: () => Effect.succeed(Option.none<CompactorState>()),
            DatabaseIdMismatchError: () => Effect.succeed(Option.none<CompactorState>()),
            CompactorUnavailableError: () =>
              Effect.as(
                Effect.flatMap(Clock.currentTimeMillis, (now) =>
                  pollBy(now + settings.compactorPollInterval),
                ),
                Option.none<CompactorState>(),
              ),
          }),
        )
      : stored;
    if (Option.isNone(before)) return;
    const now = yield* Clock.currentTimeMillis;
    const { databaseId, collectionDueAt } = before.value;
    if (Option.isSome(collectionDueAt) && collectionDueAt.value <= now) {
      yield* runCollection(databaseId).pipe(
        Effect.catchTags({
          DatabaseNotFoundError: () => Effect.void,
          // Storage unavailable now: try again after the GC interval.
          GarbageCollectorUnavailableError: () =>
            Effect.gen(function* () {
              const retryAt = Timestamp.make(now + settings.gcInterval);
              const retried = yield* updateExisting(databaseId, (current) => ({
                ...current,
                collectionDueAt: Option.some(retryAt),
              }));
              if (Option.isSome(retried)) yield* pollBy(retryAt);
            }),
        }),
      );
    }
    const again = yield* planLock
      .withPermits(1)(
        Effect.gen(function* () {
          const state = yield* cell.get;
          if (Option.isNone(state) || state.value.fenced) return false;
          return yield* poll(state.value);
        }),
      )
      .pipe(
        Effect.catchTags({ DatabaseNotFoundError: () => Effect.succeed(false) }),
        // Manifest unreadable now: poll again later.
        Effect.catchTags({ ManifestReadError: () => Effect.succeed(true) }),
      );
    if (again) yield* pollBy((yield* Clock.currentTimeMillis) + settings.compactorPollInterval);
    // A collection still due keeps the alarm set even when compaction is idle.
    const after = yield* cell.get;
    if (Option.isSome(after) && Option.isSome(after.value.collectionDueAt)) {
      yield* pollBy(after.value.collectionDueAt.value);
    }
  }).pipe(Effect.withSpan("Compactor.alarm"));

  const dropSst = Effect.fn("Compactor.dropSst")(function* (
    databaseId: DatabaseId,
    request: SstDropRequest,
  ) {
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "sst.id": request.sstId,
      "drop.force": request.force,
    });
    const refused = (reason: SstDropRefusedError["reason"], message: string) =>
      new SstDropRefusedError({ databaseId, sstId: request.sstId, reason, message });
    return yield* planLock.withPermits(1)(
      Effect.gen(function* () {
        // Never fenced: a fenced compactor fences in again here (D78).
        const state = yield* ensureState(databaseId);
        const latest = yield* reader
          .loadLatest(databaseId)
          .pipe(Effect.catchTags({ ManifestReadError: unavailable(databaseId) }));
        const source = sourceInManifest(latest.manifest, request.sstId);
        if (Option.isNone(source)) {
          // Already dropped (a repeated request): report the recorded drop.
          const recorded = latest.manifest.droppedSsts.findLast(
            (dropped) => dropped.sstId === request.sstId,
          );
          if (recorded !== undefined) return { manifestId: latest.id, dropped: recorded };
          return yield* new SstNotInManifestError({
            databaseId,
            sstId: request.sstId,
            message: `SST ${request.sstId} is not in database ${databaseId}'s manifest, so there is nothing to drop. Check the SST id in the compaction status.`,
          });
        }
        const reported = state.corruptSsts.some((corrupt) => corrupt.sstId === request.sstId);
        if (!reported && !request.force) {
          return yield* refused(
            "NotReportedCorrupted",
            `No compaction reported SST ${request.sstId} of database ${databaseId} corrupted, so it was not dropped. Check the SST id in the compaction status, or send force to drop a healthy SST and lose its rows.`,
          );
        }
        if (
          state.running.some((job) =>
            job.job.sources.some((input) => sameSource(input, source.value)),
          )
        ) {
          return yield* refused(
            "CompactionRunning",
            `A running compaction of database ${databaseId} reads SST ${request.sstId}'s ${source.value._tag === "L0" ? "L0 SST" : "sorted run"}, so it was not dropped (the compaction would bring its rows back). Retry once the compaction finishes.`,
          );
        }
        const droppedAt = Timestamp.make(yield* Clock.currentTimeMillis);
        const committed = yield* manifests
          .update(databaseId, (manifest) =>
            dropSstFromManifest(manifest, {
              sstId: request.sstId,
              reason: reported ? "Corrupted" : "Forced",
              droppedAt,
            }),
          )
          .pipe(
            Effect.catchTags({
              ManifestReadError: unavailable(databaseId),
              ManifestWriteError: unavailable(databaseId),
              DatabaseRemovedError: (error) =>
                Effect.fail(new DatabaseNotFoundError({ databaseId, message: error.message })),
            }),
          );
        const dropped = committed.manifest.droppedSsts.findLast(
          (entry) => entry.sstId === request.sstId,
        );
        if (dropped === undefined) {
          // A concurrent edit removed the SST some other way (a fork rewrite); nothing was dropped.
          return yield* new SstNotInManifestError({
            databaseId,
            sstId: request.sstId,
            message: `SST ${request.sstId} left database ${databaseId}'s manifest before it could be dropped, so there is nothing to drop.`,
          });
        }
        yield* updateExisting(databaseId, (current) => ({
          ...current,
          corruptSsts: current.corruptSsts.filter((corrupt) => corrupt.sstId !== request.sstId),
        }));
        yield* pollBy(yield* Clock.currentTimeMillis);
        yield* Metric.update(sstsDropped, 1);
        yield* Effect.logWarning(
          `Database ${databaseId}'s SST ${request.sstId} was dropped from manifest ${committed.id} (${dropped.reason}); its rows are lost and compaction of its source resumes.`,
        );
        yield* Effect.annotateCurrentSpan({
          "manifest.id": committed.id,
          "drop.reason": dropped.reason,
        });
        return { manifestId: committed.id, dropped };
      }),
    );
  });

  return Compactor.of({
    wake,
    status,
    destroy,
    collect,
    requestCollection,
    dropSst,
    runAlarm,
  });
});

/** Provides the compactor, leaving its state cell, alarm, manifests, launcher, and budget visible. */
export const compactorLayerWithoutDependencies = Layer.effect(Compactor, makeCompactor);

/** Provides a compactor state cell held in memory, for tests. */
export const compactorStateCellTestLayer = Layer.effect(
  CompactorStateCell,
  inMemoryCell<CompactorState>(),
);
