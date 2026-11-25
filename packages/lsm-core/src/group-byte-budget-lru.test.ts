import { Arbitrary, Option, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { GroupByteBudgetLru } from "./group-byte-budget-lru.ts";
import { assertProperty } from "./lsm-core-arbitraries.ts";

const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

/** A random workload: sets and gets over a few keys, groups, and sizes, against random budgets. */
const workload = Arbitrary.all({
  totalBytes: smallInt(1, 40),
  groupShare: smallInt(1, 4),
  steps: Arbitrary.array(
    Arbitrary.all({
      get: smallInt(0, 2),
      key: smallInt(0, 11),
      group: smallInt(0, 3),
      bytes: smallInt(1, 12),
    }),
    { maxLength: 80 },
  ),
});

describe("GroupByteBudgetLru", () => {
  it("keeps every group within its budget and all groups within the total, with exact accounting", () => {
    assertProperty(workload, ({ totalBytes, groupShare, steps }) => {
      const cache = new GroupByteBudgetLru<number>({
        totalBytes,
        groupBytes: Math.ceil(totalBytes / groupShare),
      });
      for (const step of steps) {
        const key = `k${step.key}`;
        if (step.get === 0) {
          cache.get(key);
          continue;
        }
        cache.set(key, `g${step.group}`, step.bytes, step.key);
        const held = cache.snapshot();
        const sum = held.reduce((total, entry) => total + entry.bytes, 0);
        if (sum !== cache.totalBytes || sum > cache.budget.totalBytes) return false;
        for (const group of new Set(held.map((entry) => entry.group))) {
          const bytes = held
            .filter((entry) => entry.group === group)
            .reduce((total, entry) => total + entry.bytes, 0);
          if (bytes !== cache.bytesOf(group) || bytes > cache.budget.groupBytes) return false;
        }
        // The entry just added stays unless it alone exceeds its group's budget.
        if (step.bytes <= cache.budget.groupBytes !== cache.has(key)) return false;
      }
      return true;
    });
  });

  it("evicts a group's least recently used entries first, and another group's only under total pressure, heaviest group first", () => {
    assertProperty(workload, ({ totalBytes, groupShare, steps }) => {
      const cache = new GroupByteBudgetLru<number>({
        totalBytes,
        groupBytes: Math.ceil(totalBytes / groupShare),
      });
      // Last use of each held key, by step.
      const lastUse = new Map<string, number>();
      for (const [index, step] of steps.entries()) {
        const key = `k${step.key}`;
        if (step.get === 0) {
          if (Option.isSome(cache.get(key))) lastUse.set(key, index);
          continue;
        }
        const before = cache.snapshot().filter((entry) => entry.key !== key);
        const usage = new Map<string, number>();
        for (const entry of before)
          usage.set(entry.group, (usage.get(entry.group) ?? 0) + entry.bytes);
        const totalBefore = before.reduce((total, entry) => total + entry.bytes, 0);
        cache.set(key, `g${step.group}`, step.bytes, step.key);
        lastUse.set(key, index);
        const after = new Set(cache.snapshot().map((entry) => entry.key));
        const evicted = before.filter((entry) => !after.has(entry.key));
        for (const gone of evicted) {
          // Within its group, nothing older than an evicted entry survives.
          const survivorsOfGroup = before.filter(
            (entry) => entry.group === gone.group && after.has(entry.key),
          );
          if (
            survivorsOfGroup.some(
              (entry) => (lastUse.get(entry.key) ?? -1) < (lastUse.get(gone.key) ?? -1),
            )
          ) {
            return false;
          }
        }
        const newGroup = `g${step.group}`;
        const finalBytes = (group: string) => cache.bytesOf(group);
        // A group holding only the entry being added has nothing it may give up.
        const newGroupEvictable = cache
          .snapshot()
          .some((entry) => entry.group === newGroup && entry.key !== key);
        for (const group of new Set(evicted.map((entry) => entry.group))) {
          if (group === newGroup) continue;
          // Another group loses entries only when the total budget was exceeded...
          if (totalBefore + step.bytes <= cache.budget.totalBytes) return false;
          // ...and only while it was the heaviest: at its last eviction it held its final bytes
          // plus that entry, at least as much as any group holds now (groups only shrank since).
          const lastEvicted = evicted
            .filter((entry) => entry.group === group)
            .reduce((newest, entry) =>
              (lastUse.get(entry.key) ?? -1) > (lastUse.get(newest.key) ?? -1) ? entry : newest,
            );
          const heldThen = finalBytes(group) + lastEvicted.bytes;
          for (const other of new Set([...usage.keys(), newGroup])) {
            if (other === group || (other === newGroup && !newGroupEvictable)) continue;
            if (finalBytes(other) > heldThen) return false;
          }
        }
        for (const gone of evicted) lastUse.delete(gone.key);
      }
      return true;
    });
  });

  it("does not keep an entry larger than a group's budget, and replaces a key's value", () => {
    const cache = new GroupByteBudgetLru<string>({ totalBytes: 10, groupBytes: 4 });
    cache.set("big", "a", 5, "too big");
    expect(cache.has("big")).toBe(false);
    cache.set("k", "a", 2, "one");
    cache.set("k", "a", 3, "two");
    expect(cache.get("k")).toEqual(Option.some("two"));
    expect(cache.bytesOf("a")).toBe(3);
    expect(cache.totalBytes).toBe(3);
  });
});
