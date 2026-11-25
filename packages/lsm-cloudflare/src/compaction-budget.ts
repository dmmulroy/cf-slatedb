import {
  acquireCompactionLease,
  CompactionBudgetLedger,
  EMPTY_COMPACTION_BUDGET_LEDGER,
  releaseCompactionLease,
  Timestamp,
} from "@cf-slatedb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Clock, Context, Effect, Layer, Option, Schema, Semaphore } from "effect";

import type { DurableObjectCell } from "./durable-object-cell.ts";
import { inMemoryCell } from "./durable-object-cell.ts";
import { LsmConfiguration } from "./lsm-configuration.ts";

/**
 * The shared compaction budget could not be reached. Retryable: the compactor treats it as "not
 * granted now" and asks again on its next poll.
 */
export class CompactionBudgetUnavailableError extends Schema.TaggedError<CompactionBudgetUnavailableError>()(
  "CompactionBudgetUnavailableError",
  { message: Schema.String },
) {}

/**
 * Fair shares of compaction concurrency across budget groups (design §12, "Shared resources").
 *
 * **Details**
 *
 * A compaction runs only while it holds a lease. `holder` names one job (`<databaseId>/<jobId>`) and
 * `group` its database's `budgetGroup` (or id), so callers can group a tenant's databases. Leases
 * expire unless renewed (`acquire` again), so a lost compaction frees its slot.
 */
export interface ICompactionBudget {
  /** Grants or renews `holder`'s lease. @returns Whether it holds one now. */
  readonly acquire: (
    group: string,
    holder: string,
  ) => Effect.Effect<boolean, CompactionBudgetUnavailableError, RuntimeContext>;
  /** Releases `holder`'s lease; releasing one not held succeeds. */
  readonly release: (
    holder: string,
  ) => Effect.Effect<void, CompactionBudgetUnavailableError, RuntimeContext>;
}

/** The contextual compaction budget. */
export class CompactionBudget extends Context.Service<CompactionBudget, ICompactionBudget>()(
  "@cf-slatedb/CompactionBudget",
) {}

/** No shared budget: every compaction may run (the design's "none"). */
export const compactionBudgetUnlimitedLayer = Layer.succeed(
  CompactionBudget,
  CompactionBudget.of({
    acquire: () => Effect.succeed(true),
    release: () => Effect.void,
  }),
);

/**
 * Builds the budget over a stored ledger: {@link acquireCompactionLease} with the configured
 * capacity and lease length. Refused groups count as waiting for three compactor polls.
 */
export const makeLedgerCompactionBudget = (cell: DurableObjectCell<CompactionBudgetLedger>) =>
  Effect.gen(function* () {
    const settings = yield* LsmConfiguration;
    // One ledger update at a time (in a Durable Object the gates already serialize events).
    const lock = yield* Semaphore.make(1);
    const ledger = Effect.map(
      cell.get,
      Option.getOrElse(() => EMPTY_COMPACTION_BUDGET_LEDGER),
    );
    return CompactionBudget.of({
      acquire: Effect.fn("CompactionBudget.acquire")(function* (group: string, holder: string) {
        const now = Timestamp.make(yield* Clock.currentTimeMillis);
        const result = yield* lock.withPermits(1)(
          Effect.gen(function* () {
            const outcome = acquireCompactionLease(yield* ledger, {
              holder,
              group,
              now,
              capacity: settings.compactionBudgetCapacity,
              leaseMs: settings.compactionLeaseMs,
              waitMs: settings.compactorPollInterval * 3,
            });
            yield* cell.set(outcome.ledger);
            return outcome;
          }),
        );
        yield* Effect.annotateCurrentSpan({
          "budget.group": group,
          "budget.granted": result.granted,
          "budget.leases": result.ledger.leases.length,
        });
        return result.granted;
      }),
      release: Effect.fn("CompactionBudget.release")(function* (holder: string) {
        yield* lock.withPermits(1)(
          Effect.flatMap(ledger, (current) => cell.set(releaseCompactionLease(current, holder))),
        );
      }),
    });
  });

/** Provides a ledger budget held in memory, with the contextual settings, for tests. */
export const compactionBudgetTestLayer = Layer.effect(
  CompactionBudget,
  Effect.flatMap(inMemoryCell<CompactionBudgetLedger>(), makeLedgerCompactionBudget),
);
