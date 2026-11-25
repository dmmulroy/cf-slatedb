import { Arbitrary, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  acquireCompactionLease,
  type CompactionBudgetLedger,
  EMPTY_COMPACTION_BUDGET_LEDGER,
  releaseCompactionLease,
} from "./compaction-budget-ledger.ts";
import { Timestamp } from "./ids.ts";
import { assertProperty } from "./lsm-core-arbitraries.ts";

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

const request = (holder: string, group: string, now = 0, capacity = 4) => ({
  holder,
  group,
  now: Timestamp.make(now),
  capacity,
  leaseMs: 100,
  waitMs: 30,
});

describe("compaction budget ledger", () => {
  it("never exceeds capacity, and never grants a group past its fair share among holders", () => {
    assertProperty(
      Arbitrary.all({
        capacity: smallInt(1, 6),
        steps: Arbitrary.array(
          Arbitrary.all({ release: smallInt(0, 3), group: smallInt(0, 3), job: smallInt(0, 9) }),
          { maxLength: 60 },
        ),
      }),
      ({ capacity, steps }) => {
        let ledger: CompactionBudgetLedger = EMPTY_COMPACTION_BUDGET_LEDGER;
        for (const [index, step] of steps.entries()) {
          const holder = `g${step.group}/j${step.job}`;
          if (step.release === 0) {
            ledger = releaseCompactionLease(ledger, holder);
            continue;
          }
          const before = ledger;
          const result = acquireCompactionLease(ledger, {
            ...request(holder, `g${step.group}`, index, capacity),
            leaseMs: 1_000,
          });
          ledger = result.ledger;
          if (ledger.leases.length > capacity) return false;
          const alreadyHeld = before.leases.some((lease) => lease.holder === holder);
          if (result.granted && !alreadyHeld) {
            const groups = new Set([
              ...before.leases.map((lease) => lease.group),
              ...before.waiting
                .filter((waiter) => waiter.until > index)
                .map((waiter) => waiter.group),
              `g${step.group}`,
            ]);
            const share = Math.max(1, Math.floor(capacity / groups.size));
            const held = before.leases.filter((lease) => lease.group === `g${step.group}`).length;
            if (held >= share) return false;
          }
        }
        return true;
      },
      { runs: 300 },
    );
  });

  it("lets a lone group use the whole budget, then makes room for a second group", () => {
    let ledger: CompactionBudgetLedger = EMPTY_COMPACTION_BUDGET_LEDGER;
    for (const job of ["a1", "a2", "a3", "a4"]) {
      const result = acquireCompactionLease(ledger, request(job, "a"));
      expect(result.granted).toBe(true);
      ledger = result.ledger;
    }
    const refused = acquireCompactionLease(ledger, request("b1", "b"));
    expect(refused.granted).toBe(false);
    // The refusal records group b as waiting, so it counts toward the fair share.
    ledger = refused.ledger;
    ledger = releaseCompactionLease(releaseCompactionLease(ledger, "a1"), "a2");
    // Group a holds 2, its share among {a, b} is 2: a waits, b gets the freed slots.
    expect(acquireCompactionLease(ledger, request("a5", "a")).granted).toBe(false);
    const b = acquireCompactionLease(ledger, request("b1", "b"));
    expect(b.granted).toBe(true);
  });

  it("renews a held lease and frees an expired one", () => {
    const first = acquireCompactionLease(EMPTY_COMPACTION_BUDGET_LEDGER, request("a1", "a", 0, 1));
    const renewed = acquireCompactionLease(first.ledger, request("a1", "a", 50, 1));
    expect(renewed.granted).toBe(true);
    expect(renewed.ledger.leases[0]?.expiresAt).toBe(150);
    // Within the renewed lease the slot is taken; once it expires another holder gets it.
    expect(acquireCompactionLease(renewed.ledger, request("b1", "b", 149, 1)).granted).toBe(false);
    expect(acquireCompactionLease(renewed.ledger, request("b1", "b", 150, 1)).granted).toBe(true);
  });
});
