import type * as cf from "@cloudflare/workers-types";
import { Context, Effect, Layer, Option, Ref } from "effect";

/** A Durable Object's single platform alarm (the writer's flushes, the compactor's polls). */
export interface IDurableObjectAlarm {
  /** The scheduled alarm time in epoch milliseconds, if any. */
  readonly scheduled: Effect.Effect<Option.Option<number>>;
  /** Replaces the alarm with one at `atMillis`. */
  readonly scheduleAt: (atMillis: number) => Effect.Effect<void>;
}

/** The contextual Durable Object alarm. */
export class DurableObjectAlarm extends Context.Service<DurableObjectAlarm, IDurableObjectAlarm>()(
  "@cf-slatedb/DurableObjectAlarm",
) {}

/** Provides the alarm of a Durable Object's native storage. */
export const durableObjectAlarmLayer = (storage: cf.DurableObjectStorage) =>
  Layer.succeed(
    DurableObjectAlarm,
    DurableObjectAlarm.of({
      scheduled: Effect.promise(() => storage.getAlarm()).pipe(Effect.map(Option.fromNullOr)),
      scheduleAt: (atMillis) => Effect.promise(() => storage.setAlarm(atMillis)),
    }),
  );

/**
 * Provides an alarm that records the scheduled time and never fires; tests run the alarm
 * handler themselves.
 */
export const durableObjectAlarmTestLayer = Layer.effect(
  DurableObjectAlarm,
  Effect.gen(function* () {
    const at = yield* Ref.make(Option.none<number>());
    return DurableObjectAlarm.of({
      scheduled: Ref.get(at),
      scheduleAt: (atMillis) => Ref.set(at, Option.some(atMillis)),
    });
  }),
);
