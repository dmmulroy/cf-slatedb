import { describe, expect, it } from "vite-plus/test";

import { DatabaseId } from "@shaledb/lsm-core";
import { Option } from "effect";

import { DatabaseModel } from "./soak.ts";
import {
  ChangeLedger,
  FeedFollower,
  MergeKeyModel,
  rowSignature,
  ScanTraversal,
  type SoakChange,
} from "./soak-checks.ts";

const put = (key: string, value: string): SoakChange => ({ _tag: "Put", key, value });
const rows = (seq: number, changes: ReadonlyArray<SoakChange>) =>
  changes.map((change) => ({
    seq,
    signature:
      change._tag === "Put"
        ? rowSignature(change.key, "Value", change.value)
        : change._tag === "Delete"
          ? rowSignature(change.key, "Tombstone", "")
          : rowSignature(change.key, "Merge", change.operand),
  }));

describe("FeedFollower against the ChangeLedger (D106)", () => {
  it("passes a feed that delivers every acknowledged batch once, in order, and an uncertain one once", () => {
    const ledger = new ChangeLedger();
    ledger.ack(1, [put("a", "1"), put("b", "1")]);
    ledger.maybe([put("c", "1")]);
    ledger.ack(3, [put("a", "2")]);
    const follower = new FeedFollower(0);
    expect(
      follower.page({ rows: rows(1, [put("b", "1"), put("a", "1")]), nextAfterSeq: 1 }),
    ).toEqual([]);
    // The uncertain attempt committed, at seq 2.
    expect(
      follower.page({
        rows: [...rows(2, [put("c", "1")]), ...rows(3, [put("a", "2")])],
        nextAfterSeq: 3,
      }),
    ).toEqual([]);
    expect(follower.page({ rows: [], nextAfterSeq: 3 })).toEqual([]);
    expect([...follower.missing(ledger), ...follower.unaccounted(ledger)]).toEqual([]);
  });

  it("finds a skipped batch, a split batch, a repeated row, a wrong cursor, and an unknown change", () => {
    const ledger = new ChangeLedger();
    ledger.ack(1, [put("a", "1"), put("b", "1")]);
    ledger.ack(2, [put("a", "2")]);
    ledger.ack(3, [put("a", "3")]);
    const follower = new FeedFollower(0);
    // Half of batch 1, then the page moves past it: the rest of batch 1 is skipped (D93b).
    expect(follower.page({ rows: rows(1, [put("a", "1")]), nextAfterSeq: 1 })).toEqual([]);
    expect(follower.page({ rows: rows(1, [put("b", "1")]), nextAfterSeq: 1 })).toEqual([
      "seq 1 at or below the cursor 1",
    ]);
    // Batch 2 skipped by a cursor that jumped (D80's silent gap).
    expect(follower.page({ rows: rows(3, [put("a", "3")]), nextAfterSeq: 3 })).toEqual([]);
    expect(follower.page({ rows: rows(4, [put("z", "9")]), nextAfterSeq: 5 })).toEqual([
      "nextAfterSeq 5, expected 4",
    ]);
    expect(follower.missing(ledger)).toEqual([
      "seq 1 delivered 1 changes, acknowledged 2",
      "seq 2 (1 changes) never delivered",
    ]);
    expect(follower.unaccounted(ledger)).toEqual(["seq 4: a change the soak never sent"]);
  });

  it("judges only what is after where it started (a successor followed from the fork point)", () => {
    const ledger = new ChangeLedger();
    ledger.ack(5, [put("a", "source")]);
    ledger.ack(7, [put("a", "successor")]);
    const follower = new FeedFollower(6);
    expect(follower.page({ rows: rows(7, [put("a", "successor")]), nextAfterSeq: 7 })).toEqual([]);
    expect(follower.missing(ledger)).toEqual([]);
  });
});

describe("MergeKeyModel (D106, D81)", () => {
  it("expects each operand to expire on its own, and accepts either side of an expiry within the skew", () => {
    const key = new MergeKeyModel();
    key.put(1, "V", undefined);
    key.merge(2, "a", 10_000);
    key.merge(3, "b", undefined);
    // Well before the expiry: everything. Well after: the base and b, never nothing (D81's fold).
    expect([...(key.acceptable(1_000, 1_100, 2_000) ?? [])]).toEqual(["Vab"]);
    expect([...(key.acceptable(20_000, 20_100, 2_000) ?? [])]).toEqual(["Vb"]);
    // Inside the window: either.
    expect(new Set(key.acceptable(9_500, 9_600, 2_000))).toEqual(new Set(["Vab", "Vb"]));
  });

  it("reads an expired base as a delete under live operands, and does not judge an unknown key", () => {
    const key = new MergeKeyModel();
    key.put(1, "V", 5_000);
    key.merge(2, "a", undefined);
    expect([...(key.acceptable(10_000, 10_100, 1_000) ?? [])]).toEqual(["a"]);
    key.unknown = true;
    expect(key.acceptable(10_000, 10_100, 1_000)).toBeUndefined();
    key.put(3, "W", undefined);
    expect([...(key.acceptable(10_000, 10_100, 1_000) ?? [])]).toEqual(["W"]);
  });
});

describe("ScanTraversal (D106, D93)", () => {
  it("finds a repeated key and a skipped one across pages", () => {
    const traversal = new ScanTraversal();
    expect(traversal.page(["a", "b"])).toEqual([]);
    expect(traversal.page(["b", "d"])).toEqual(["key b after b: repeated or out of order"]);
    expect(traversal.skipped(["a", "c", "d"])).toEqual(["key c skipped"]);
  });
});

describe("DatabaseModel with aborted writes (D106)", () => {
  it("accepts an aborted write's key holding its value or nothing, and nothing else", () => {
    const model = new DatabaseModel(DatabaseId.make("soak"));
    model.maybe.set("u000001-0", "aborted-1-0");
    expect(model.accepts("u000001-0", 0, Option.none())).toBe(true);
    expect(model.accepts("u000001-0", 0, Option.some("aborted-1-0"))).toBe(true);
    expect(model.accepts("u000001-0", 0, Option.some("other"))).toBe(false);
    // Keys written normally are judged as before.
    model.record("k1", 5, Option.some("v5"), false);
    expect(model.accepts("k1", 5, Option.some("v5"))).toBe(true);
    expect(model.accepts("k1", 5, Option.none())).toBe(false);
  });
});
