/**
 * The soak's models for the classes of defect today's audits found (D106): the change feed
 * (D80, D93b), merge operands with TTLs (D81), scans continued page by page (D93), and writes whose
 * outcome the client never learned (D74b, D85). Each is pure: the soak feeds it what it sent and
 * what it read, and it answers what is wrong. Imported by `soak.ts` and the tests.
 */

/** One change as the feed or the model names it: key, kind, and payload. */
export type ChangeSignature = string;

/** A change of one key: a put, a delete, or a merge operand. */
export type SoakChange =
  | { readonly _tag: "Put"; readonly key: string; readonly value: string }
  | { readonly _tag: "Delete"; readonly key: string }
  | { readonly _tag: "Merge"; readonly key: string; readonly operand: string };

/** The signature a change has in the model and in a feed row. */
export const signatureOf = (change: SoakChange): ChangeSignature =>
  change._tag === "Put"
    ? `${change.key}=Value:${change.value}`
    : change._tag === "Delete"
      ? `${change.key}=Tombstone:`
      : `${change.key}=Merge:${change.operand}`;

/** A feed row's signature (`row.value._tag` and its payload, decoded as text). */
export const rowSignature = (
  key: string,
  kind: "Value" | "Tombstone" | "Merge",
  payload: string,
): ChangeSignature => `${key}=${kind}:${kind === "Tombstone" ? "" : payload}`;

const sorted = (signatures: ReadonlyArray<ChangeSignature>) => [...signatures].sort();

/**
 * Every batch the soak sent to one database: the acknowledged ones by sequence number, and the
 * attempts whose outcome it never learned (a retried, timed-out, or aborted request), each of which
 * may have committed once, at a sequence number the soak does not know.
 */
export class ChangeLedger {
  readonly acknowledged = new Map<number, ReadonlyArray<ChangeSignature>>();
  readonly uncertain: Array<{
    readonly changes: ReadonlyArray<ChangeSignature>;
    matched: boolean;
  }> = [];

  /** A batch the database acknowledged at `seq`. */
  ack(seq: number, changes: ReadonlyArray<SoakChange>) {
    this.acknowledged.set(seq, sorted(changes.map(signatureOf)));
  }

  /** A batch that may have committed (once) at a sequence number the soak never saw. */
  maybe(changes: ReadonlyArray<SoakChange>) {
    this.uncertain.push({ changes: sorted(changes.map(signatureOf)), matched: false });
  }
}

/** One page of the change feed, as the follower read it. */
export interface FeedPage {
  readonly rows: ReadonlyArray<{ readonly seq: number; readonly signature: ChangeSignature }>;
  readonly nextAfterSeq: number;
}

/**
 * A change-feed follower's record of what it was given, checked against a {@link ChangeLedger}.
 *
 * **Details**
 *
 * - **Each page** must hold sequence numbers above the cursor, ascending, none seen before
 *   (exactly once, in order), and its `nextAfterSeq` must be its last row's sequence number (or the
 *   cursor, for an empty page).
 * - **Every acknowledged batch at or below the cursor** must have been seen whole. A sequence number
 *   is assigned inside its commit, so a page that reached past it was read after it committed: this
 *   holds whenever the check runs, without waiting for acknowledgements in flight.
 * - **Every batch seen that the soak did not acknowledge** must be an uncertain attempt, each
 *   matched at most once. That is judged only at the end, when every attempt's outcome is known.
 */
export class FeedFollower {
  readonly seen = new Map<number, Array<ChangeSignature>>();
  cursor: number;
  /** Where the follower started: changes at or below it are not its to see. */
  readonly startedAfter: number;
  constructor(afterSeq: number) {
    this.cursor = afterSeq;
    this.startedAfter = afterSeq;
  }

  /** Records a page; returns what is wrong with it. */
  page(page: FeedPage): ReadonlyArray<string> {
    const problems: Array<string> = [];
    let previous = this.cursor;
    for (const row of page.rows) {
      if (row.seq <= this.cursor) {
        problems.push(`seq ${row.seq} at or below the cursor ${this.cursor}`);
        continue;
      }
      if (row.seq < previous) problems.push(`seq ${row.seq} after ${previous}: out of order`);
      if (this.seen.has(row.seq) && row.seq !== previous)
        problems.push(`seq ${row.seq} delivered twice`);
      previous = Math.max(previous, row.seq);
      const batch = this.seen.get(row.seq) ?? [];
      batch.push(row.signature);
      this.seen.set(row.seq, batch);
    }
    const last = page.rows[page.rows.length - 1];
    const expectedNext = last === undefined ? this.cursor : last.seq;
    if (page.nextAfterSeq !== expectedNext)
      problems.push(`nextAfterSeq ${page.nextAfterSeq}, expected ${expectedNext}`);
    this.cursor = Math.max(this.cursor, page.nextAfterSeq);
    return problems;
  }

  /** Acknowledged batches at or below the cursor that were not seen whole. */
  missing(ledger: ChangeLedger): ReadonlyArray<string> {
    const problems: Array<string> = [];
    for (const [seq, changes] of ledger.acknowledged) {
      if (seq <= this.startedAfter || seq > this.cursor) continue;
      const got = sorted(this.seen.get(seq) ?? []);
      if (got.join("\n") !== changes.join("\n")) {
        problems.push(
          got.length === 0
            ? `seq ${seq} (${changes.length} changes) never delivered`
            : `seq ${seq} delivered ${got.length} changes, acknowledged ${changes.length}`,
        );
      }
    }
    return problems;
  }

  /** Batches seen that no acknowledged or uncertain batch accounts for (judge at the end). */
  unaccounted(ledger: ChangeLedger): ReadonlyArray<string> {
    const problems: Array<string> = [];
    for (const [seq, signatures] of this.seen) {
      if (ledger.acknowledged.has(seq)) continue;
      const got = sorted(signatures).join("\n");
      const attempt = ledger.uncertain.find(
        (candidate) => !candidate.matched && candidate.changes.join("\n") === got,
      );
      if (attempt === undefined) problems.push(`seq ${seq}: a change the soak never sent`);
      else attempt.matched = true;
    }
    return problems;
  }
}

/** One merge operand the soak sent to a key, with when the database said it expires. */
interface MergeOperand {
  readonly seq: number;
  readonly operand: string;
  /** Epoch ms from the write receipt's `createTs` plus the TTL; none: never. */
  readonly expireAt: number | undefined;
}

/**
 * The expected value of one `Concat` key built from a base value and operands with mixed TTLs
 * (D81): each row expires on its own, and an expired base reads as a delete under the operands
 * above it.
 *
 * **Details**
 *
 * The soak and the database have different clocks, and a read takes time. An operand (or base)
 * whose expiry falls within `skewMs` of the read's interval may be read either way: the check
 * accepts any of those combinations. A key whose last write's outcome is unknown (a retried merge
 * may have applied twice) is not judged until the next acknowledged base resets it.
 */
export class MergeKeyModel {
  base: { readonly seq: number; readonly value: string; readonly expireAt: number | undefined } = {
    seq: 0,
    value: "",
    expireAt: undefined,
  };
  operands: Array<MergeOperand> = [];
  /** A write to it had an unknown outcome since the last base. */
  unknown = false;

  /** An acknowledged put of the base value (resets the key). */
  put(seq: number, value: string, expireAt: number | undefined) {
    this.base = { seq, value, expireAt };
    this.operands = [];
    this.unknown = false;
  }

  /** An acknowledged merge operand. */
  merge(seq: number, operand: string, expireAt: number | undefined) {
    this.operands.push({ seq, operand, expireAt });
  }

  /** The values a read between `startMs` and `endMs` may return; none when it cannot judge. */
  acceptable(startMs: number, endMs: number, skewMs: number): ReadonlySet<string> | undefined {
    if (this.unknown) return undefined;
    // Each part: live for sure, expired for sure, or either (inside the window).
    type Part = { readonly text: string; readonly state: "live" | "expired" | "either" };
    const stateOf = (expireAt: number | undefined): Part["state"] =>
      expireAt === undefined || expireAt > endMs + skewMs
        ? "live"
        : expireAt <= startMs - skewMs
          ? "expired"
          : "either";
    const baseState = stateOf(this.base.expireAt);
    const parts: Array<Part> = [...this.operands]
      .sort((left, right) => left.seq - right.seq)
      .map((operand) => ({ text: operand.operand, state: stateOf(operand.expireAt) }));
    const either = parts.filter((part) => part.state === "either").length;
    if (either + (baseState === "either" ? 1 : 0) > 10) return undefined;
    const values = new Set<string>();
    for (const baseLive of baseState === "either" ? [true, false] : [baseState === "live"]) {
      for (let mask = 0; mask < 2 ** either; mask++) {
        let bit = 0;
        let value = baseLive ? this.base.value : "";
        for (const part of parts) {
          const live =
            part.state === "live" || (part.state === "either" && (mask >> bit++) % 2 === 1);
          if (live) value += part.text;
        }
        values.add(value);
      }
    }
    return values;
  }
}

/**
 * A scan of one range continued page by page (D93): it may answer a page early with `resumeFrom`.
 * Across the traversal no key may repeat or come out of order, and a key that held a value for the
 * whole traversal must be returned.
 */
export class ScanTraversal {
  readonly returned: Array<string> = [];
  private last: string | undefined;

  /** Records a page's keys; returns what is wrong. */
  page(keys: ReadonlyArray<string>): ReadonlyArray<string> {
    const problems: Array<string> = [];
    for (const key of keys) {
      if (this.last !== undefined && key <= this.last)
        problems.push(`key ${key} after ${this.last}: repeated or out of order`);
      this.last = key;
      this.returned.push(key);
    }
    return problems;
  }

  /** Keys that held a value from before the traversal to after it but were not returned. */
  skipped(stableKeys: ReadonlyArray<string>): ReadonlyArray<string> {
    const got = new Set(this.returned);
    return stableKeys.filter((key) => !got.has(key)).map((key) => `key ${key} skipped`);
  }
}
