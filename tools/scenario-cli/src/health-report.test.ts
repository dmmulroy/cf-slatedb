import { describe, expect, it } from "vite-plus/test";

import { type HealthSpan, type HealthSpans, summarizeHealth } from "./health-report.ts";

let clock = 0n;
/** A span that started after every span made before it. */
const span = (
  name: string,
  attributes: Record<string, unknown> = {},
  options: { readonly durationMs?: number; readonly exception?: string } = {},
): HealthSpan => {
  clock += 1_000_000n;
  return {
    name,
    startTimeUnixNano: String(clock),
    durationMs: options.durationMs ?? 10,
    attributes,
    ...(options.exception === undefined
      ? {}
      : { events: [{ name: "exception", attributes: { "exception.type": options.exception } }] }),
  };
};

const none: HealthSpans = {
  refusedWrites: [],
  flushes: [],
  failedFlushes: [],
  polls: [],
  uploads: [],
  failedCollections: [],
  failedSweeps: [],
  errors: [],
};

const poll = (database: string, attributes: Record<string, unknown> = {}) =>
  span("Compactor.poll", { "database.id": database, "manifest.l0_ssts": 2, ...attributes });

const stalledWrite = (database: string, reason?: string) =>
  span(
    "DbWriter.write",
    { "database.id": database, ...(reason === undefined ? {} : { "writer.stall_reason": reason }) },
    { exception: "WriteStalledError" },
  );

describe("summarizeHealth (D82)", () => {
  it("is healthy when every signal is quiet, and reports what it saw", () => {
    const report = summarizeHealth([], {
      ...none,
      flushes: [span("DbWriter.flush", {}, { durationMs: 700 })],
      polls: [poll("orders")],
      uploads: [span("ObjectStore.putMultipart", { "object.parts_unverified": 0 })],
      refusedWrites: [
        span("DbWriter.write", { "database.id": "orders" }, { exception: "DatabaseSealedError" }),
      ],
    });
    expect(report.healthy).toBe(true);
    expect(report.issues).toEqual([]);
    expect(report.writes.refusedByError).toEqual({ DatabaseSealedError: 1 });
    expect(report.l0Backlog).toEqual({ deepest: { orders: 2 }, databases: 1 });
    expect(report.uploads).toEqual({ multipart: 1, withRetriedPart: 0, withUnverifiedPart: 0 });
  });

  it("names each database whose writes stalled, and why", () => {
    const report = summarizeHealth([], {
      ...none,
      refusedWrites: [
        stalledWrite("orders", "L0Backlog"),
        stalledWrite("orders", "L0Backlog"),
        stalledWrite("users", "UnflushedBytes"),
        stalledWrite("old"),
      ],
    });
    expect(report.issues).toEqual([
      "2 writes stalled (WriteStalled) on database orders (L0Backlog)",
      "1 writes stalled (WriteStalled) on database users (UnflushedBytes)",
      "1 writes stalled (WriteStalled) on database old (reason not recorded)",
    ]);
  });

  it("reports D78's stuck database: a fenced compactor, L0 at its limit, writes refused", () => {
    const report = summarizeHealth([], {
      ...none,
      polls: [
        poll("ms-cas", { "manifest.l0_ssts": 16 }),
        span("Compactor.poll", { "database.id": "ms-cas", "compactor.fenced": true }),
      ],
      refusedWrites: [stalledWrite("ms-cas", "L0Backlog")],
    });
    expect(report.healthy).toBe(false);
    expect(report.issues).toEqual([
      "1 writes stalled (WriteStalled) on database ms-cas (L0Backlog)",
      "database ms-cas has 16 L0 SSTs, at its 16 limit: writes are refused until compaction catches up",
      "database ms-cas's compactor found itself fenced (1 polls): its state was overwritten (D78)",
    ]);
    expect(report.fencedCompactors).toEqual({ "ms-cas": 1 });
  });

  it("keeps L0 below the limit healthy, and uses the configured limit", () => {
    const polls = [poll("orders", { "manifest.l0_ssts": 15 })];
    expect(summarizeHealth([], { ...none, polls }).issues).toEqual([]);
    expect(
      summarizeHealth([], { ...none, polls }, { l0MaxSsts: 8, flushP99Ms: 30_000 }).issues,
    ).toEqual([
      "database orders has 15 L0 SSTs, at its 8 limit: writes are refused until compaction catches up",
    ]);
  });

  it("judges corruption by each database's latest poll, so a repaired one is healthy", () => {
    const repaired = summarizeHealth([], {
      ...none,
      polls: [poll("orders", { "compaction.corrupt_ssts": 1 }), poll("orders")],
    });
    expect(repaired.issues).toEqual([]);
    const corrupted = summarizeHealth([], {
      ...none,
      polls: [poll("orders"), poll("orders", { "compaction.corrupt_ssts": 1 })],
    });
    expect(corrupted.corruptSsts).toEqual(["orders"]);
    expect(corrupted.issues).toEqual([
      "database orders has corrupted SSTs: see docs/operations.md",
    ]);
    // `trace-cli grep` lists spans by trace, not by time: the latest is found by its start.
    const earlier = poll("orders", { "compaction.corrupt_ssts": 1 });
    const latest = poll("orders");
    const unordered = summarizeHealth([], { ...none, polls: [latest, earlier] });
    expect(unordered.issues).toEqual([]);
  });

  it("makes an unverified part an issue, and a re-sent part a count", () => {
    const report = summarizeHealth([], {
      ...none,
      uploads: [
        span("ObjectStore.putMultipart", { "object.parts_unverified": 2 }),
        span("ObjectStore.putMultipart", { "object.part_retried": true }),
        span("ObjectStore.putMultipart", { "object.part_retried": true }),
        span("ObjectStore.putMultipart", {}),
      ],
    });
    expect(report.uploads).toEqual({ multipart: 4, withRetriedPart: 2, withUnverifiedPart: 1 });
    expect(report.issues).toEqual([
      "1 multipart uploads stored parts whose ETag was not an MD5: parts are no longer checked",
    ]);
  });

  it("makes a flush p99 above 30 s an issue, and 30 s itself not", () => {
    const flushes = (ms: number) => ({
      ...none,
      flushes: [
        span("DbWriter.flush", {}, { durationMs: 500 }),
        span("DbWriter.flush", {}, { durationMs: ms }),
      ],
    });
    expect(summarizeHealth([], flushes(30_000)).issues).toEqual([]);
    expect(summarizeHealth([], flushes(30_001)).issues).toEqual([
      "flush p99 is 30001 ms, above 30000 ms",
    ]);
  });

  it("reports limit hits and failed collections and sweeps as issues", () => {
    const report = summarizeHealth(
      [{ worker: "dbapiworker", outcome: "exceededCpu", eventKind: "alarm", cpuTimeMs: 30_001 }],
      {
        ...none,
        failedCollections: [span("GarbageCollector.collect")],
        failedSweeps: [span("MaintenanceSweep.sweepPage")],
        errors: [span("GarbageCollector.collect"), span("MaintenanceSweep.sweepPage")],
      },
    );
    expect(report.issues).toEqual([
      "dbapiworker invocation ended exceededCpu (alarm, 30001 ms CPU)",
      "1 collections failed",
      "1 maintenance sweeps failed",
    ]);
    expect(report.errorSpans).toEqual({
      "GarbageCollector.collect": 1,
      "MaintenanceSweep.sweepPage": 1,
    });
  });
});

describe("summarizeHealth: a writer that cannot flush (D102)", () => {
  it("reports failed flushes per database as an issue", () => {
    const failed = (database: string) =>
      span("DbWriter.flush", { "database.id": database }, { exception: "WriterUnavailableError" });
    const report = summarizeHealth([], {
      ...none,
      failedFlushes: [failed("orders"), failed("orders"), failed("users")],
    });
    expect(report.healthy).toBe(false);
    expect(report.issues).toEqual([
      "2 flushes failed on database orders: its acknowledged writes are not reaching R2",
      "1 flushes failed on database users: its acknowledged writes are not reaching R2",
    ]);
  });
});
