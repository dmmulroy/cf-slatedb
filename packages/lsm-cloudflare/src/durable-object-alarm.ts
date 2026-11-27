import type * as cf from "@cloudflare/workers-types";
import { Clock, Context, Effect, Layer, Option, Ref } from "effect";

/**
 * The first retry delay of an alarm handler that failed (D102). Each further consecutive failure
 * doubles it, up to the handler's ceiling.
 */
export const ALARM_RETRY_BASE_MILLIS = 1_000;

/** The retry delay after `failures` consecutive failures (1 or more), at most `ceilingMs`. */
export const alarmRetryDelayMillis = (failures: number, ceilingMs: number): number =>
  Math.min(ceilingMs, ALARM_RETRY_BASE_MILLIS * 2 ** Math.min(30, Math.max(0, failures - 1)));

/** A Durable Object's single platform alarm (the writer's flushes, the compactor's polls). */
export interface IDurableObjectAlarm {
  /** The scheduled alarm time in epoch milliseconds, if any. */
  readonly scheduled: Effect.Effect<Option.Option<number>>;
  /** Replaces the alarm with one at `atMillis`. */
  readonly scheduleAt: (atMillis: number) => Effect.Effect<void>;
  /**
   * Removes the alarm. The runtime does this itself when the alarm fires: inside the alarm handler
   * `scheduled` is none until the handler schedules another.
   */
  readonly cancel: Effect.Effect<void>;
  /**
   * Records one more consecutive failure of the alarm handler and sets the alarm for its retry
   * (D102): {@link ALARM_RETRY_BASE_MILLIS} after the first, doubling after each further one, at
   * most `ceilingMs`. The count is stored with the alarm, so it outlives an eviction.
   *
   * @returns The consecutive failures, this one included.
   */
  readonly retryAfterFailure: (ceilingMs: number) => Effect.Effect<number>;
  /** Clears the failure count after a handler run that succeeded. */
  readonly clearFailures: Effect.Effect<void>;
}

/** The contextual Durable Object alarm. */
export class DurableObjectAlarm extends Context.Service<DurableObjectAlarm, IDurableObjectAlarm>()(
  "@shaledb/DurableObjectAlarm",
) {}

/** The storage key of the consecutive alarm failures. */
const FAILURES_KEY = "alarm.failures";

/** Provides the alarm of a Durable Object's native storage. */
export const durableObjectAlarmLayer = (storage: cf.DurableObjectStorage) => {
  const failures = Effect.promise(() => storage.get<number>(FAILURES_KEY)).pipe(
    Effect.map((stored) => (typeof stored === "number" ? stored : 0)),
  );
  return Layer.succeed(
    DurableObjectAlarm,
    DurableObjectAlarm.of({
      scheduled: Effect.promise(() => storage.getAlarm()).pipe(Effect.map(Option.fromNullOr)),
      scheduleAt: (atMillis) => Effect.promise(() => storage.setAlarm(atMillis)),
      cancel: Effect.promise(() => storage.deleteAlarm()),
      retryAfterFailure: (ceilingMs) =>
        Effect.gen(function* () {
          const count = (yield* failures) + 1;
          yield* Effect.promise(() => storage.put(FAILURES_KEY, count));
          const now = yield* Clock.currentTimeMillis;
          yield* Effect.promise(() =>
            storage.setAlarm(now + alarmRetryDelayMillis(count, ceilingMs)),
          );
          return count;
        }),
      clearFailures: Effect.flatMap(failures, (count) =>
        count === 0 ? Effect.void : Effect.promise(() => storage.delete(FAILURES_KEY)),
      ),
    }),
  );
};

/**
 * Provides an alarm that records the scheduled time and never fires; tests run the alarm
 * handler themselves.
 */
export const durableObjectAlarmTestLayer = Layer.effect(
  DurableObjectAlarm,
  Effect.gen(function* () {
    const at = yield* Ref.make(Option.none<number>());
    const failures = yield* Ref.make(0);
    return DurableObjectAlarm.of({
      scheduled: Ref.get(at),
      scheduleAt: (atMillis) => Ref.set(at, Option.some(atMillis)),
      cancel: Ref.set(at, Option.none()),
      retryAfterFailure: (ceilingMs) =>
        Effect.gen(function* () {
          const count = yield* Ref.updateAndGet(failures, (current) => current + 1);
          const now = yield* Clock.currentTimeMillis;
          yield* Ref.set(at, Option.some(now + alarmRetryDelayMillis(count, ceilingMs)));
          return count;
        }),
      clearFailures: Ref.set(failures, 0),
    });
  }),
);
