import { Schema } from "effect";

import { Timestamp } from "./ids.ts";

/** Schema for one running compaction's lease on the shared compaction budget. */
export const CompactionLease = Schema.Struct({
  /** Who holds it: one compaction job (`<databaseId>/<jobId>`). */
  holder: Schema.String,
  /** The budget group (a database's `budgetGroup`, or its id). */
  group: Schema.String,
  expiresAt: Timestamp,
});

/** One running compaction's lease. */
export type CompactionLease = typeof CompactionLease.Type;

/** Schema for a group that was refused a lease and is still asking, until `until`. */
export const CompactionBudgetWaiter = Schema.Struct({ group: Schema.String, until: Timestamp });

/** A group that was refused a lease. */
export type CompactionBudgetWaiter = typeof CompactionBudgetWaiter.Type;

/** Schema for the budget's state: every unexpired lease and every group still waiting. */
export const CompactionBudgetLedger = Schema.Struct({
  leases: Schema.Array(CompactionLease),
  waiting: Schema.Array(CompactionBudgetWaiter),
});

/** The budget's state. */
export type CompactionBudgetLedger = typeof CompactionBudgetLedger.Type;

/** An empty ledger. */
export const EMPTY_COMPACTION_BUDGET_LEDGER: CompactionBudgetLedger = { leases: [], waiting: [] };

/** A lease request: who, for which group, at what time, against which limits. */
export interface CompactionLeaseRequest {
  readonly holder: string;
  readonly group: string;
  readonly now: Timestamp;
  /** Most compactions running at once across every group. */
  readonly capacity: number;
  /** How long a lease lasts without renewal; a holder that crashes frees its slot after this. */
  readonly leaseMs: number;
  /**
   * How long a refused group counts as waiting; it should exceed the interval at which refused
   * holders ask again.
   */
  readonly waitMs: number;
}

/**
 * Grants or renews a lease if the budget has room for the request's group (a pure function).
 *
 * **Details**
 *
 * Expired leases and waiters are dropped first. A holder that already has a lease renews it.
 * Otherwise the request is granted while fewer than `capacity` leases are held and its group holds
 * fewer than its fair share: `capacity` divided evenly among the groups holding leases, the groups
 * waiting, and the requester's (at least one). A refused group is recorded as waiting for `waitMs`.
 * A lone group can use the whole budget; once another group is refused, the first gets no new slots
 * until it is under its share, so every group makes progress.
 *
 * @returns Whether the lease is held, and the new ledger.
 */
export const acquireCompactionLease = (
  ledger: CompactionBudgetLedger,
  request: CompactionLeaseRequest,
): { readonly granted: boolean; readonly ledger: CompactionBudgetLedger } => {
  const live = ledger.leases.filter((lease) => lease.expiresAt > request.now);
  const waiting = ledger.waiting.filter((waiter) => waiter.until > request.now);
  const expiresAt = Timestamp.make(request.now + request.leaseMs);
  if (live.some((lease) => lease.holder === request.holder)) {
    return {
      granted: true,
      ledger: {
        leases: live.map((lease) =>
          lease.holder === request.holder ? { ...lease, expiresAt } : lease,
        ),
        waiting,
      },
    };
  }
  const groups = new Set([
    ...live.map((lease) => lease.group),
    ...waiting.map((waiter) => waiter.group),
    request.group,
  ]);
  const share = Math.max(1, Math.floor(request.capacity / groups.size));
  const held = live.filter((lease) => lease.group === request.group).length;
  const others = waiting.filter((waiter) => waiter.group !== request.group);
  if (live.length >= request.capacity || held >= share) {
    const until = Timestamp.make(request.now + request.waitMs);
    return {
      granted: false,
      ledger: { leases: live, waiting: [...others, { group: request.group, until }] },
    };
  }
  return {
    granted: true,
    ledger: {
      leases: [...live, { holder: request.holder, group: request.group, expiresAt }],
      waiting: others,
    },
  };
};

/** Releases a holder's lease; releasing one that is not held changes nothing. */
export const releaseCompactionLease = (
  ledger: CompactionBudgetLedger,
  holder: string,
): CompactionBudgetLedger => ({
  leases: ledger.leases.filter((lease) => lease.holder !== holder),
  waiting: ledger.waiting,
});
