import type * as cf from "@cloudflare/workers-types";
import { Context, Effect, Layer, Option, Ref } from "effect";

/** The writer Durable Object's single platform alarm, which drives L0 flushes. */
export interface IWriterAlarm {
  /** The scheduled alarm time in epoch milliseconds, if any. */
  readonly scheduled: Effect.Effect<Option.Option<number>>;
  /** Replaces the alarm with one at `atMillis`. */
  readonly scheduleAt: (atMillis: number) => Effect.Effect<void>;
}

/** The contextual writer alarm. */
export class WriterAlarm extends Context.Service<WriterAlarm, IWriterAlarm>()(
  "@cf-slatedb/WriterAlarm",
) {}

/** Provides the alarm of a Durable Object's native storage. */
export const writerAlarmLayer = (storage: cf.DurableObjectStorage) =>
  Layer.succeed(
    WriterAlarm,
    WriterAlarm.of({
      scheduled: Effect.promise(() => storage.getAlarm()).pipe(Effect.map(Option.fromNullOr)),
      scheduleAt: (atMillis) => Effect.promise(() => storage.setAlarm(atMillis)),
    }),
  );

/**
 * Provides an alarm that records the scheduled time and never fires; tests run the writer's alarm
 * handler themselves.
 */
export const writerAlarmTestLayer = Layer.effect(
  WriterAlarm,
  Effect.gen(function* () {
    const at = yield* Ref.make(Option.none<number>());
    return WriterAlarm.of({
      scheduled: Ref.get(at),
      scheduleAt: (atMillis) => Ref.set(at, Option.some(atMillis)),
    });
  }),
);
