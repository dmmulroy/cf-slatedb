import { Duration, Effect, Fiber, Option } from "effect";

import { invocationsRepresentedBy, tailCursorAt, type InvocationRecord } from "./measurements.ts";

/** The result of a read given a time budget: its value, or `OutOfTime` when the budget ran out first. */
export type BoundedRead<A> =
  | { readonly _tag: "Finished"; readonly value: A }
  | { readonly _tag: "OutOfTime" };

/**
 * Runs `read`, interrupting it when it has not finished within `budget`. Interruption stops it
 * wherever it is (an HTTP request is aborted, a trace CLI process is killed).
 *
 * @returns `Finished` with its value, `OutOfTime`, or the read's own error.
 */
export const readWithin = <A, E, R>(
  budget: Duration.Input,
  read: Effect.Effect<A, E, R>,
): Effect.Effect<BoundedRead<A>, E, R> =>
  read.pipe(
    Effect.map((value): BoundedRead<A> => ({ _tag: "Finished", value })),
    Effect.timeoutOrElse({
      duration: budget,
      orElse: () => Effect.succeed<BoundedRead<A>>({ _tag: "OutOfTime" }),
    }),
  );

/** How many invocations past which a limit hit is no longer listed (they are still counted). */
const LIMIT_HITS_KEPT = 50;

/** Outcomes that mean an invocation hit a Workers limit or threw. */
const LIMIT_OUTCOMES = new Set(["exceededCpu", "exceededMemory", "exception"]);

/**
 * The soak's running count of the stage's tail records: outcomes per Worker, limit hits, how many
 * records were read and the newest one's time, and the key to continue reading from.
 */
export class TailTally {
  /** Estimated invocations per `"<worker> <outcome>"` (a kept `ok` record stands for `1 / sampleRate`, D71). */
  readonly invocationOutcomes = new Map<string, number>();
  /** The first invocations that ended `exceededCpu`, `exceededMemory`, or `exception`. */
  readonly limitHits: Array<string> = [];
  /** Tail records read so far. */
  recordsRead = 0;
  /** The event time of the newest record read, in epoch milliseconds. */
  newestRecordAt: Option.Option<number> = Option.none();
  /** The tail key after the last page folded in: the next read starts there. */
  cursor: string;

  constructor(sinceMs: number) {
    this.cursor = tailCursorAt(sinceMs);
  }

  /**
   * Folds one page of records in and moves the cursor past it, in one synchronous step: a read
   * stopped between pages has counted every page before its cursor exactly once.
   */
  foldPage(records: ReadonlyArray<InvocationRecord>, cursor: string) {
    for (const record of records) {
      this.recordsRead += 1;
      const worker =
        Option.getOrElse(record.scriptName, () => "unknown").split("-")[1] ?? "unknown";
      const key = `${worker} ${record.outcome}`;
      this.invocationOutcomes.set(
        key,
        (this.invocationOutcomes.get(key) ?? 0) + invocationsRepresentedBy(record),
      );
      if (Option.isSome(record.eventTimestamp)) {
        const at = record.eventTimestamp.value;
        this.newestRecordAt = Option.some(
          Option.match(this.newestRecordAt, { onNone: () => at, onSome: (n) => Math.max(n, at) }),
        );
      }
      if (LIMIT_OUTCOMES.has(record.outcome) && this.limitHits.length < LIMIT_HITS_KEPT) {
        this.limitHits.push(
          `${worker} ${record.eventKind} ${Option.getOrElse(record.path, () => "")} ended ${record.outcome} (${record.cpuTimeMs} ms CPU)`,
        );
      }
    }
    this.cursor = cursor;
  }

  /** How far the reads got, for a failure message: records read and the newest one's time. */
  progress() {
    return `${this.recordsRead} records read, the newest from ${Option.match(this.newestRecordAt, {
      onNone: () => "no time (none read)",
      onSome: (at) => new Date(at).toISOString(),
    })}`;
  }
}

/**
 * Where tail records come from: pages after a key, each handed to `onPage` with the key after it
 * ({@link forEachInvocationRecord}'s shape, so tests can pass a source that never ends).
 */
export type TailPageSource<E, R> = (
  after: string,
  onPage: (records: ReadonlyArray<InvocationRecord>, cursor: string) => void,
) => Effect.Effect<string, E, R>;

/** How a read of tail records ended. */
export type TailRead =
  | { readonly _tag: "CaughtUp" }
  | { readonly _tag: "OutOfTime" }
  | { readonly _tag: "Failed"; readonly message: string };

/**
 * Reads tail records from `tally.cursor` into `tally` until the source has no more or `budget`
 * runs out. Pages read before the budget ran out stay counted, and the next read continues after
 * them.
 *
 * @returns `CaughtUp`, `OutOfTime`, or `Failed` with the source's error message.
 */
export const readTailsWithin = <E extends { readonly message: string }, R>(
  tally: TailTally,
  budget: Duration.Input,
  source: TailPageSource<E, R>,
): Effect.Effect<TailRead, never, R> =>
  readWithin(
    budget,
    source(tally.cursor, (records, cursor) => tally.foldPage(records, cursor)),
  ).pipe(
    Effect.map((read): TailRead => (read._tag === "Finished" ? { _tag: "CaughtUp" } : read)),
    Effect.catch((error) => Effect.succeed<TailRead>({ _tag: "Failed", message: error.message })),
  );

/**
 * Reads the tail records left at the end of a soak: tries until caught up, at most `attempts`
 * times after a failure (`retryAfter` apart), all within `budget`.
 *
 * @returns `None` when caught up, or the failure to record, saying how far the reads got.
 */
export const catchUpTails = <E extends { readonly message: string }, R>(
  tally: TailTally,
  budget: Duration.Input,
  source: TailPageSource<E, R>,
  attempts = 4,
  retryAfter: Duration.Input = "10 seconds",
) =>
  Effect.gen(function* () {
    let lastFailure = "";
    const caughtUp = yield* readWithin(
      budget,
      Effect.gen(function* () {
        for (let attempt = 1; attempt <= attempts; attempt++) {
          const read = yield* readTailsWithin(tally, Duration.infinity, source);
          if (read._tag === "CaughtUp") return true;
          if (read._tag === "Failed") lastFailure = read.message.slice(0, 200);
          if (attempt < attempts) yield* Effect.sleep(retryAfter);
        }
        return false;
      }),
    );
    if (caughtUp._tag === "OutOfTime") {
      return Option.some(
        `telemetry: the last tail records were not all read within ${Duration.format(Duration.fromInputUnsafe(budget))} (${tally.progress()}), so some invocations were not checked`,
      );
    }
    if (!caughtUp.value) {
      return Option.some(
        `telemetry: the last tail records could not be read after ${attempts} tries (${tally.progress()}; last error: ${lastFailure}), so some invocations were not checked`,
      );
    }
    return Option.none<string>();
  });

/**
 * Runs `workload` with `watchers` alongside it. When the workload ends, however it ends, the
 * watchers are interrupted wherever they are and awaited: they can never hold the workload open.
 *
 * @returns The workload's result or error.
 */
export const whileWorkloadRuns = <A, E, R, R2>(
  workload: Effect.Effect<A, E, R>,
  watchers: Effect.Effect<unknown, never, R2>,
): Effect.Effect<A, E, R | R2> =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(watchers);
    return yield* workload.pipe(Effect.ensuring(Fiber.interrupt(fiber)));
  });

/** Runs `effect` every `interval` (after the first wait) until interrupted. */
export const every = <R>(interval: Duration.Input, effect: Effect.Effect<unknown, never, R>) =>
  Effect.gen(function* () {
    for (;;) {
      yield* Effect.sleep(interval);
      yield* effect;
    }
  });
