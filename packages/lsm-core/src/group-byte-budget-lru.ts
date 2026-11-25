import { Option } from "effect";

/** Byte budgets of a {@link GroupByteBudgetLru}. */
export interface GroupByteBudget {
  /** Most bytes every group together may hold. */
  readonly totalBytes: number;
  /** Most bytes one group may hold (at most `totalBytes`). */
  readonly groupBytes: number;
}

interface Entry<V> {
  readonly key: string;
  readonly group: string;
  readonly bytes: number;
  readonly value: V;
}

/**
 * A least-recently-used cache bounded by bytes, both overall and per group (design §12: an L1
 * byte budget per `budgetGroup`).
 *
 * **Details**
 *
 * - A group never holds more than `groupBytes`: adding to a full group evicts that group's least
 *   recently used entries, never another group's.
 * - All groups together never hold more than `totalBytes`: when they would, the group holding the
 *   most bytes gives up its least recently used entry, repeatedly, so one busy group shrinks before
 *   any lighter one is touched. The entry being added is never the one evicted.
 * - An entry larger than `groupBytes` is not kept.
 *
 * Mutable and synchronous: every operation is O(1) apart from choosing the heaviest group, which is
 * linear in the number of groups.
 */
export class GroupByteBudgetLru<V> {
  readonly budget: GroupByteBudget;
  // Map iteration order is insertion order: re-inserting on access keeps the oldest first.
  private readonly entries = new Map<string, Entry<V>>();
  private readonly groups = new Map<string, Map<string, Entry<V>>>();
  private readonly groupUsage = new Map<string, number>();
  private usage = 0;

  constructor(budget: GroupByteBudget) {
    this.budget = {
      totalBytes: budget.totalBytes,
      groupBytes: Math.min(budget.groupBytes, budget.totalBytes),
    };
  }

  /** Bytes held by every group. */
  get totalBytes(): number {
    return this.usage;
  }

  /** Entries held. */
  get size(): number {
    return this.entries.size;
  }

  /** Bytes held by one group. */
  bytesOf(group: string): number {
    return this.groupUsage.get(group) ?? 0;
  }

  /** Whether `key` is held, without changing its recency. */
  has(key: string): boolean {
    return this.entries.has(key);
  }

  /** The value of `key`, marking it most recently used. */
  get(key: string): Option.Option<V> {
    const entry = this.entries.get(key);
    if (entry === undefined) return Option.none();
    this.touch(entry);
    return Option.some(entry.value);
  }

  /**
   * Holds `value` under `key` for `group`, counting `bytes`, then evicts until both budgets hold.
   * A value already held under `key` is replaced.
   */
  set(key: string, group: string, bytes: number, value: V): void {
    this.remove(key);
    if (bytes > this.budget.groupBytes) return;
    const entry: Entry<V> = { key, group, bytes, value };
    this.entries.set(key, entry);
    const members = this.groups.get(group) ?? new Map<string, Entry<V>>();
    members.set(key, entry);
    this.groups.set(group, members);
    this.groupUsage.set(group, this.bytesOf(group) + bytes);
    this.usage += bytes;
    while (this.bytesOf(group) > this.budget.groupBytes) {
      if (!this.evictOldest(group, key)) break;
    }
    while (this.usage > this.budget.totalBytes) {
      const heaviest = this.heaviestGroup(key);
      if (Option.isNone(heaviest) || !this.evictOldest(heaviest.value, key)) break;
    }
  }

  /** Every held entry, oldest first: for tests and diagnostics. */
  snapshot(): ReadonlyArray<{
    readonly key: string;
    readonly group: string;
    readonly bytes: number;
  }> {
    return [...this.entries.values()].map(({ key, group, bytes }) => ({ key, group, bytes }));
  }

  private touch(entry: Entry<V>): void {
    this.entries.delete(entry.key);
    this.entries.set(entry.key, entry);
    const members = this.groups.get(entry.group);
    members?.delete(entry.key);
    members?.set(entry.key, entry);
  }

  private remove(key: string): void {
    const entry = this.entries.get(key);
    if (entry === undefined) return;
    this.entries.delete(key);
    const members = this.groups.get(entry.group);
    members?.delete(key);
    if (members !== undefined && members.size === 0) this.groups.delete(entry.group);
    const remaining = this.bytesOf(entry.group) - entry.bytes;
    if (remaining > 0) this.groupUsage.set(entry.group, remaining);
    else this.groupUsage.delete(entry.group);
    this.usage -= entry.bytes;
  }

  /** Evicts `group`'s least recently used entry other than `keep`; false when it has none. */
  private evictOldest(group: string, keep: string): boolean {
    const members = this.groups.get(group);
    if (members === undefined) return false;
    for (const candidate of members.keys()) {
      if (candidate === keep) continue;
      this.remove(candidate);
      return true;
    }
    return false;
  }

  /** The group holding the most bytes that has an entry other than `keep` to give up. */
  private heaviestGroup(keep: string): Option.Option<string> {
    let heaviest = Option.none<string>();
    let most = -1;
    for (const [group, members] of this.groups) {
      const evictable = members.size > 1 || !members.has(keep);
      const bytes = this.bytesOf(group);
      if (evictable && bytes > most) {
        heaviest = Option.some(group);
        most = bytes;
      }
    }
    return heaviest;
  }
}
