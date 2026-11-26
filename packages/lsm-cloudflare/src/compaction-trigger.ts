import type { DatabaseId } from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, Ref } from "effect";

/**
 * Wakes a database's compactor when L0 has grown enough to compact. Best effort: it never fails, and
 * a missed wake is repeated after the next flush.
 */
export interface ICompactionTrigger {
  readonly wake: (databaseId: DatabaseId) => Effect.Effect<void, never, RuntimeContext>;
}

/** The contextual compaction trigger. */
export class CompactionTrigger extends Context.Service<CompactionTrigger, ICompactionTrigger>()(
  "@shaledb/CompactionTrigger",
) {}

/** The databases a test trigger was asked to wake, in order. */
export class CompactionTriggerRecord extends Context.Service<
  CompactionTriggerRecord,
  Ref.Ref<ReadonlyArray<DatabaseId>>
>()("@shaledb/CompactionTriggerRecord") {}

/** Provides a trigger that only records wakes (in {@link CompactionTriggerRecord}, also output). */
export const compactionTriggerTestLayer = Layer.effect(
  CompactionTrigger,
  Effect.gen(function* () {
    const record = yield* CompactionTriggerRecord;
    return CompactionTrigger.of({
      wake: (databaseId) => Ref.update(record, (woken) => [...woken, databaseId]),
    });
  }),
).pipe(
  Layer.provideMerge(
    Layer.effect(CompactionTriggerRecord, Ref.make<ReadonlyArray<DatabaseId>>([])),
  ),
);
