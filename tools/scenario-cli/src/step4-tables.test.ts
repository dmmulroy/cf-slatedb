import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vite-plus/test";

import {
  baselineCost,
  BaselineReport,
  clientLimited,
  flushedReachSentence,
  jsonOfOutput,
  median,
  parseRunLog,
  readRun,
  readStep4Directory,
  replaceBlock,
  ROW_COST_ED4E787,
  rowCostFor,
  type Run,
  step4Blocks,
  targetsTable,
  writesTable,
  WritesReport,
} from "./step4-tables.ts";

/**
 * Fixtures are real run outputs: `measure` reports from `preview-3` at `d89beba`
 * (`/tmp/shale-work/final`), the baseline from `ed4e787` (`/tmp/shale-work/release2/baseline-1.json`,
 * the run behind `docs/measurements.md`'s D75 table), the row-cost probe's answer, and the run log's
 * lines for those runs. The baseline's two log lines are made up: that run had no log.
 */
const FIXTURES = fileURLToPath(new URL("./fixtures/step4", import.meta.url));
const fixture = (name: string) => readFileSync(`${FIXTURES}/${name}`, "utf8");
const context = {
  commit: "d89beba",
  stage: "preview-3",
  settings: "production settings, 100% trace sampling",
  release: false,
};

describe("reading run outputs", () => {
  it("reads each run's window and exit status from the log", () => {
    const log = parseRunLog(fixture("log"));
    expect(log.get("p3-writes-100")).toEqual({ start: "02:15:26", end: "02:17:12", exit: 0 });
    expect(
      parseRunLog("01:00:00 start a\n01:00:05 end   a exit=3\n01:00:05 start b").get("a")?.exit,
    ).toBe(3);
    expect(parseRunLog("01:00:05 start b").get("b")).toEqual({
      start: "01:00:05",
      end: undefined,
      exit: undefined,
    });
  });

  it("finds the JSON after any leading text", () => {
    expect(jsonOfOutput('{"a":1}')).toBe('{"a":1}');
    expect(jsonOfOutput('deploying…\nwaiting {x}\n{\n  "a": 1\n}\n')).toBe('{\n  "a": 1\n}');
    expect(jsonOfOutput("no report")).toBeUndefined();
  });

  it("says why a run has no report: failed, still running, no output, or the wrong shape", () => {
    const good = fixture("p3-writes-100.out");
    const ended = { start: "01:00:00", end: "01:01:00", exit: 0 };
    expect(readRun("w", good, ended, WritesReport).ok).toBe(true);
    expect(readRun("w", good, { ...ended, exit: 1 }, WritesReport)).toMatchObject({
      ok: false,
      why: "failed (exit 1)",
    });
    expect(
      readRun("w", "", { start: "01:00:00", end: undefined, exit: undefined }, WritesReport),
    ).toMatchObject({ ok: false, why: "still running" });
    expect(readRun("w", undefined, undefined, WritesReport)).toMatchObject({
      ok: false,
      why: "no output",
    });
    expect(readRun("w", '{"rowsPerSecond": "fast"}', ended, WritesReport)).toMatchObject({
      ok: false,
      why: "its output does not match the report's shape",
    });
  });

  it("reads a directory, and a run absent from it is not available, never zero", () => {
    const runs = readStep4Directory(FIXTURES);
    expect(runs.writes.map((r) => r.ok)).toEqual([true, true]);
    expect(runs.splits.map((r) => r.ok)).toEqual([true, false, false, false, false, false]);
    expect(runs.baseline.map((r) => r.ok)).toEqual([true, false, false]);
    expect(rowCostFor(FIXTURES).rowCost).toEqual(ROW_COST_ED4E787);
  });
});

describe("the arithmetic", () => {
  it("takes the median of an odd and an even count", () => {
    expect(median([3, 1, 2])).toBe(2);
    expect(median([4, 1, 3, 2])).toBe(2.5);
    expect(Number.isNaN(median([]))).toBe(true);
  });

  it("prices the D75 run as baseline-cost.mjs did: $3.02, $3.03, and $1.01 per million rows", () => {
    const run = readRun("b", fixture("p3-baseline-1.out"), undefined, BaselineReport);
    if (!run.ok) throw new Error(run.why);
    const perMillion = run.report.targets.map(
      (t) => baselineCost(t, ROW_COST_ED4E787).perMillionRows,
    );
    // baseline-cost.mjs on the same file: $3.0184, $3.0250, $1.0139.
    expect(perMillion.map((c) => c.toFixed(4))).toEqual(["3.0184", "3.0250", "1.0139"]);
    expect(
      run.report.targets.map((t) =>
        baselineCost(t, ROW_COST_ED4E787).sqlitePerMillionRows.toFixed(2),
      ),
    ).toEqual(["3.00", "3.00", "1.00"]);
  });

  it("says a Flushed read reached no Durable Object only when the invocations do not pass the writes", () => {
    expect(flushedReachSentence(1826, 1842)).toContain(
      "no more than the writes, so no read reached a Durable Object",
    );
    expect(flushedReachSentence(2516, 2516)).toContain("no read reached");
    expect(flushedReachSentence(2600, 2516)).toContain(
      "84 more than the writes, so some reads reached a Durable Object",
    );
    expect(flushedReachSentence(Number.NaN, 1)).toContain("no run has them");
  });

  it("calls a writes run client-limited only when its own numbers show it", () => {
    const run = readRun("w", fixture("p3-writes-1000.out"), undefined, WritesReport);
    if (!run.ok) throw new Error(run.why);
    // d89beba: 2.56 MB/s, writer busy 0.70: not a client limit.
    expect(clientLimited(run.report)).toBe(false);
    // f3b2ad2's run (NOTES.md): under 1 MB/s with the writer busy a fifth of the time.
    const slow = { ...run.report, megabytesPerSecond: 0.76, writerBusyFraction: 0.21 };
    expect(clientLimited(slow)).toBe(true);
    const slowRun: Run<WritesReport> = {
      name: "p3-writes-1000",
      window: undefined,
      ok: true,
      report: slow,
    };
    expect(writesTable(context, [slowRun])).toContain("is a client limit, not a writer limit");
    expect(writesTable(context, [{ ...slowRun, report: run.report }])).not.toContain(
      "client limit",
    );
  });
});

describe("the tables", () => {
  const runs = readStep4Directory(FIXTURES);
  const blocks = step4Blocks(context, runs, rowCostFor(FIXTURES));

  it("names the commit, the run times, and provisional in each caption", () => {
    expect(blocks.measurements).toContain("## Step 4 on `d89beba` (provisional)");
    expect(blocks.measurements).toContain("`p3-writes-100` 02:15:26–02:17:12 UTC, exit 0");
    expect(blocks["readme-writes"]).toContain(
      "commit `d89beba` (provisional: not the release commit)",
    );
    const release = step4Blocks({ ...context, release: true }, runs, rowCostFor(FIXTURES));
    expect(release.measurements).not.toContain("provisional");
  });

  it("prints a row that says a run is not available, with no number in it", () => {
    const failed = blocks.measurements
      .split("\n")
      .find((line) => line.includes("`p3-split-4-2` no output"));
    expect(failed).toBeDefined();
    expect(failed?.replace("p3-split-4-2", "")).not.toMatch(/\d/);
  });

  it("puts the real figures in their rows", () => {
    expect(blocks.measurements).toContain(
      "| 1,000 | 16 | 22,359 | 2.56 | 596 / 1,187 / 2,465 | 23 / 319 | 23 / 134 | 0.7 | none |",
    );
    expect(blocks.measurements).toContain("| 4 | `p3-split-4-1` | 2,682 | 3,841 | 0 | 835 |");
    expect(blocks.measurements).toContain(
      "| 1,000 | 52,000 | 51 | 2,082 | 8,214 | 40.8 | 40 | ok |",
    );
    expect(blocks.measurements).toContain(
      "| get Latest | 2,365 | 23 / 141 / 1,146 | 1,682 | 0 / 2 |",
    );
    expect(blocks["readme-baseline"]).toContain(
      "| shaledb, reads at `Flushed` | 8,329 | 87 / 269 | 115.9 | 57 / 203 | $3.03 |",
    );
    expect(blocks.capacity).toContain(
      "| One writer, 100-row batches | 4,394 rows/s at 8 clients; p50 latency 146 ms | commit `d89beba`, provisional |",
    );
  });

  it("judges each handoff target from the runs, and says when no run backs it", () => {
    const targets = targetsTable(context, runs);
    expect(targets).toMatch(
      /WS2: `ManifestStore.update` p50 ≤ 500 ms .* \| median of run p50s 610 ms \| not met \|/,
    );
    expect(targets).toMatch(
      /4 writers; .* \| median longest gap 3,841 ms over 1 runs; 0 failed \| met \|/,
    );
    expect(targets).toMatch(/16 writers; .* \| no run \| no run \|/);
  });
});

describe("rewriting a marked block", () => {
  it("replaces what is between the markers and keeps the rest", () => {
    const text =
      "before\n<!-- STEP4 capacity: begin -->\nold\n<!-- STEP4 capacity: end -->\nafter\n";
    expect(replaceBlock(text, "capacity", "new")).toBe(
      "before\n<!-- STEP4 capacity: begin -->\n\nnew\n\n<!-- STEP4 capacity: end -->\nafter\n",
    );
  });

  it("fails, naming the marker, when a block has none", () => {
    expect(() => replaceBlock("no markers", "capacity", "new")).toThrow(
      "<!-- STEP4 capacity: begin -->",
    );
  });
});
