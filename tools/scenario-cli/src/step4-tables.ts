/**
 * The step-4 measurement tables (`docs/release-v1.md` §3), built from a directory of run outputs
 * so the release-commit rerun is one command.
 *
 * Usage: `node tools/scenario-cli/src/step4-tables.ts <directory> <commit> [--stage preview-3]
 * [--release] [--write <repository root>]`.
 *
 * - **Input:** `<directory>` holds `p3-<run>.out` (a `scenario-cli measure …` report, JSON after
 *   any leading text) and `log` (lines `HH:MM:SS start <run>` and `HH:MM:SS end <run> exit=<n>`,
 *   UTC), as `/tmp/shale-work/scripts/final-soaks.sh` writes them.
 * - **Output:** the Markdown of every block, printed in order. Each caption names the commit and
 *   each run's time and exit status.
 * - **A missing, failed, or unreadable run** prints a row that says so. Nothing is invented or
 *   carried over from another run.
 * - **`--release`** marks the figures final. Without it, every block says provisional (Dillon's
 *   second review: a figure is final only if its caption names the release commit).
 * - **`--write`** rewrites each block in place between its markers, `<!-- STEP4 <block>: begin -->`
 *   and `<!-- STEP4 <block>: end -->`, in the files `STEP4_BLOCK_FILES` names.
 */
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { Exit, Schema } from "effect";

// ---------------------------------------------------------------------------------------------
// Run outputs
// ---------------------------------------------------------------------------------------------

const Percentiles = Schema.Struct({
  count: Schema.Number,
  p50: Schema.Number,
  p90: Schema.Number,
  p99: Schema.Number,
  max: Schema.Number,
});
/** A latency or CPU distribution as `measure` prints it. */
export type Percentiles = typeof Percentiles.Type;

/** `measure writes`. */
export const WritesReport = Schema.Struct({
  load: Schema.Struct({ batchRows: Schema.Number, concurrency: Schema.Number }),
  rowsPerSecond: Schema.Number,
  megabytesPerSecond: Schema.Number,
  latencyMs: Percentiles,
  failures: Schema.Record(Schema.String, Schema.Number),
  writerCpuMsPerBatch: Percentiles,
  apiCpuMsPerBatch: Percentiles,
  writerBusyFraction: Schema.Number,
});
export type WritesReport = typeof WritesReport.Type;

/** `measure split-pause`. */
export const SplitReport = Schema.Struct({
  load: Schema.Struct({ writers: Schema.Number }),
  splitClientMs: Schema.Number,
  longestAckGapDuringSplitMs: Schema.Number,
  failedPuts: Schema.Number,
  totalPuts: Schema.Number,
});
export type SplitReport = typeof SplitReport.Type;

/** `measure compaction-cpu`. */
export const CompactionReport = Schema.Struct({
  megabytesPerFlush: Schema.Number,
  compacted: Schema.Boolean,
  compactedAfterSeconds: Schema.Number,
  flushCpuMsPerFlush: Schema.Number,
  flushWallMs: Percentiles,
  compactorCpuMsByEventKind: Schema.Record(Schema.String, Percentiles),
  compactorOutcomes: Schema.Array(Schema.String),
  writerOutcomes: Schema.Array(Schema.String),
});
export type CompactionReport = typeof CompactionReport.Type;

/** `measure flush-cpu`. */
export const FlushCpuReport = Schema.Struct({
  valueBytes: Schema.Number,
  rowsWritten: Schema.Number,
  sstMiB: Schema.Number,
  flushInvocations: Schema.Array(Schema.Struct({ outcome: Schema.String })),
  cpuMs: Schema.Number,
  wallMs: Schema.Number,
  cpuMsPerMiB: Schema.Number,
  cpuMicrosPerRow: Schema.Number,
});
export type FlushCpuReport = typeof FlushCpuReport.Type;

/** `measure transactions`. */
export const TransactionsReport = Schema.Struct({
  load: Schema.Struct({
    transactors: Schema.Number,
    writers: Schema.Number,
    seconds: Schema.Number,
    holdSeconds: Schema.Number,
  }),
  transactions: Schema.Number,
  outcomes: Schema.Record(Schema.String, Schema.Number),
  snapshotTooOldRate: Schema.Number,
  transactionMs: Percentiles,
  forcedFlushes: Schema.Number,
});
export type TransactionsReport = typeof TransactionsReport.Type;

/** `measure manifest-cas`. */
export const ManifestCasReport = Schema.Struct({
  flushes: Schema.Number,
  manifestUpdateMs: Percentiles,
  manifestUpdateFromOwnViewMs: Percentiles,
  manifestUpdateLoadedMs: Percentiles,
  casAttempts: Percentiles,
  writerFlushMs: Percentiles,
  flushClientMs: Percentiles,
});
export type ManifestCasReport = typeof ManifestCasReport.Type;

/** `measure latency`. */
export const LatencyReport = Schema.Struct({
  minutes: Schema.Number,
  operations: Schema.Array(
    Schema.Struct({
      operation: Schema.String,
      latencyMs: Percentiles,
      blockReads: Schema.Struct({ p50: Schema.Number, p90: Schema.Number }),
    }),
  ),
});
export type LatencyReport = typeof LatencyReport.Type;

const BaselineTarget = Schema.Struct({
  target: Schema.String,
  seconds: Schema.Number,
  acknowledgedRowsPerSecond: Schema.Number,
  writeBatchesPerSecond: Schema.Number,
  readsPerSecond: Schema.Number,
  writeLatencyMs: Percentiles,
  readLatencyMs: Percentiles,
  failures: Schema.Record(Schema.String, Schema.Number),
  workerInvocations: Schema.Struct({ count: Schema.Number, cpuMs: Schema.Number }),
  durableObjectInvocations: Schema.Struct({ count: Schema.Number, cpuMs: Schema.Number }),
  r2Requests: Schema.Struct({ classA: Schema.Number, classB: Schema.Number }),
});
export type BaselineTarget = typeof BaselineTarget.Type;

/** `measure baseline`. */
export const BaselineReport = Schema.Struct({
  load: Schema.Struct({
    batchRows: Schema.Number,
    valueBytes: Schema.Number,
    writers: Schema.Number,
    readers: Schema.Number,
    seconds: Schema.Number,
  }),
  targets: Schema.Array(BaselineTarget),
});
export type BaselineReport = typeof BaselineReport.Type;

/** The row-cost probe's answer (`/v1/probe/sqlite/<name>/rowcost`). */
export const RowCostReport = Schema.Struct({
  rows: Schema.Number,
  memtable: Schema.Struct({
    insert: Schema.Struct({ rowsWritten: Schema.Number }),
    trim: Schema.Struct({ rowsWritten: Schema.Number }),
  }),
  plain: Schema.Struct({ insert: Schema.Struct({ rowsWritten: Schema.Number }) }),
});
export type RowCostReport = typeof RowCostReport.Type;

/** When a run started and ended, from the log, in UTC `HH:MM:SS`. */
export interface RunWindow {
  readonly start: string;
  readonly end: string | undefined;
  readonly exit: number | undefined;
}

/** One run, read: its report, or why there is none. */
export type Run<A> =
  | {
      readonly name: string;
      readonly window: RunWindow | undefined;
      readonly ok: true;
      readonly report: A;
    }
  | {
      readonly name: string;
      readonly window: RunWindow | undefined;
      readonly ok: false;
      readonly why: string;
    };

/** The run log: each run's start, end, and exit status. */
export const parseRunLog = (text: string): ReadonlyMap<string, RunWindow> => {
  const runs = new Map<string, RunWindow>();
  for (const line of text.split("\n")) {
    const start = /^(?<at>\d\d:\d\d:\d\d) start\s+(?<run>\S+)/.exec(line)?.groups;
    if (start?.["at"] !== undefined && start["run"] !== undefined)
      runs.set(start["run"], { start: start["at"], end: undefined, exit: undefined });
    const end = /^(?<at>\d\d:\d\d:\d\d) end\s+(?<run>\S+) exit=(?<exit>\d+)/.exec(line)?.groups;
    if (end?.["at"] !== undefined && end["run"] !== undefined) {
      const known = runs.get(end["run"]);
      runs.set(end["run"], {
        start: known?.start ?? "?",
        end: end["at"],
        exit: Number(end["exit"]),
      });
    }
  }
  return runs;
};

/** The JSON document in a run's output: from the first line that opens an object. */
export const jsonOfOutput = (text: string): string | undefined => {
  const at = text.startsWith("{") ? 0 : text.indexOf("\n{");
  return at === -1 ? undefined : text.slice(at).trim();
};

/** Decodes one run's output, or says why it cannot. */
export const readRun = <A, I>(
  name: string,
  output: string | undefined,
  window: RunWindow | undefined,
  schema: Schema.Codec<A, I>,
): Run<A> => {
  if (window?.exit !== undefined && window.exit !== 0)
    return { name, window, ok: false, why: `failed (exit ${window.exit})` };
  if (output === undefined || output.trim() === "")
    return {
      name,
      window,
      ok: false,
      why: window !== undefined && window.end === undefined ? "still running" : "no output",
    };
  const json = jsonOfOutput(output);
  if (json === undefined) return { name, window, ok: false, why: "no JSON in its output" };
  const decoded = Schema.decodeUnknownExit(Schema.fromJsonString(schema))(json);
  return Exit.isSuccess(decoded)
    ? { name, window, ok: true, report: decoded.value }
    : { name, window, ok: false, why: "its output does not match the report's shape" };
};

// ---------------------------------------------------------------------------------------------
// Constants for the cost arithmetic
// ---------------------------------------------------------------------------------------------

/**
 * Cloudflare's published overage prices in US dollars per million, fetched 2026-11-26 from the
 * Workers, Durable Objects, and R2 pricing pages. Included monthly allowances are ignored, so the
 * costs are marginal at scale: an assumption, not a bill.
 */
export const PRICES_PER_MILLION = {
  workerRequest: 0.3,
  workerCpuMs: 0.02,
  durableObjectRequest: 0.15,
  /** Per million GB-seconds; 128 MB is charged while an object is active. */
  durableObjectGbSecond: 12.5,
  sqliteRowWritten: 1.0,
  sqliteRowRead: 0.001,
  r2ClassA: 4.5,
  r2ClassB: 0.36,
} as const;

/**
 * SQLite rows billed per row written, measured by the row-cost probe on `preview-3` at `ed4e787`
 * on 2026-11-27 (1,000 rows of 100 bytes; `/tmp/shale-work/release2/rowcost-1.json`). A
 * `p3-rowcost.json` in the run directory replaces them.
 */
export const ROW_COST_ED4E787 = {
  memtableInsert: 2,
  memtableTrim: 1,
  plainInsert: 1,
} as const;

/** Rows written per row, from a row-cost probe's answer. */
export const rowCostOf = (report: RowCostReport) => ({
  memtableInsert: report.memtable.insert.rowsWritten / report.rows,
  memtableTrim: report.memtable.trim.rowsWritten / report.rows,
  plainInsert: report.plain.insert.rowsWritten / report.rows,
});

/** What one baseline target's run cost, from its counts (the logic of `baseline-cost.mjs`). */
export const baselineCost = (
  target: BaselineTarget,
  rowCost: {
    readonly memtableInsert: number;
    readonly memtableTrim: number;
    readonly plainInsert: number;
  },
) => {
  const million = (n: number) => n / 1e6;
  const rows = target.acknowledgedRowsPerSecond * target.seconds;
  const reads = target.readsPerSecond * target.seconds;
  const plain = target.target.startsWith("plain");
  const workers =
    million(target.workerInvocations.count) * PRICES_PER_MILLION.workerRequest +
    million(target.workerInvocations.cpuMs + target.durableObjectInvocations.cpuMs) *
      PRICES_PER_MILLION.workerCpuMs;
  const durableObjects =
    million(target.durableObjectInvocations.count) * PRICES_PER_MILLION.durableObjectRequest +
    million(target.seconds * 0.128) * PRICES_PER_MILLION.durableObjectGbSecond;
  const rowsWritten = plain
    ? rows * rowCost.plainInsert
    : rows * (rowCost.memtableInsert + rowCost.memtableTrim);
  // A plain get reads one row; a Latest probe reads the memtable's table and its index.
  const rowsRead = plain ? reads : target.target.includes("Latest") ? reads * 2 : 0;
  const sqlite =
    million(rowsWritten) * PRICES_PER_MILLION.sqliteRowWritten +
    million(rowsRead) * PRICES_PER_MILLION.sqliteRowRead;
  const r2 =
    million(target.r2Requests.classA) * PRICES_PER_MILLION.r2ClassA +
    million(target.r2Requests.classB) * PRICES_PER_MILLION.r2ClassB;
  const total = workers + durableObjects + sqlite + r2;
  return {
    total,
    perMillionRows: rows > 0 ? total / million(rows) : Number.NaN,
    sqlitePerMillionRows: rows > 0 ? sqlite / million(rows) : Number.NaN,
  };
};

// ---------------------------------------------------------------------------------------------
// Arithmetic and formatting
// ---------------------------------------------------------------------------------------------

/** The median of some numbers: the middle one, or the mean of the two middle ones. */
export const median = (values: ReadonlyArray<number>): number => {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  const upper = sorted[middle];
  const lower = sorted.length % 2 === 1 ? upper : sorted[middle - 1];
  return upper === undefined || lower === undefined ? Number.NaN : (lower + upper) / 2;
};

/** A number with thousands separators and at most `digits` decimals. */
export const num = (value: number, digits = 0): string =>
  Number.isFinite(value)
    ? value.toLocaleString("en-US", { maximumFractionDigits: digits, minimumFractionDigits: 0 })
    : "—";

const pct = (p: Percentiles) => `${num(p.p50)} / ${num(p.p90)} / ${num(p.p99)}`;
const usd = (value: number) => (Number.isFinite(value) ? `$${value.toFixed(2)}` : "—");
const failuresOf = (failures: Readonly<Record<string, number>>) => {
  const entries = Object.entries(failures).filter(([, n]) => n > 0);
  return entries.length === 0 ? "none" : entries.map(([k, n]) => `${k} ${num(n)}`).join(", ");
};

const windowText = (run: Run<unknown>) =>
  run.window === undefined
    ? `\`${run.name}\` (not in the log)`
    : `\`${run.name}\` ${run.window.start}–${run.window.end ?? "?"} UTC, exit ${run.window.exit ?? "?"}`;

/** A row for a run with no report: its name and why, then a dash in every other column. */
const missingRow = (label: string, run: Run<unknown>, columns: number) =>
  `| ${label} | not available: \`${run.name}\` ${run.ok ? "" : run.why} |${" — |".repeat(columns - 2)}`;

const table = (header: ReadonlyArray<string>, rows: ReadonlyArray<string>) =>
  [`| ${header.join(" | ")} |`, `| ${header.map(() => "---").join(" | ")} |`, ...rows].join("\n");

/** Where a block's figures come from, for its caption. */
export interface Step4Context {
  readonly commit: string;
  readonly stage: string;
  readonly release: boolean;
}

const caption = (title: string, context: Step4Context, runs: ReadonlyArray<Run<unknown>>) =>
  `**${title}** \`${context.stage}\`, production settings, commit \`${context.commit}\`` +
  `${context.release ? " (the release commit)" : " (provisional: not the release commit)"}. ` +
  `Runs: ${runs.map(windowText).join("; ")}.`;

// ---------------------------------------------------------------------------------------------
// The tables
// ---------------------------------------------------------------------------------------------

/**
 * A writes run is limited by its client, not its writer, when it moved under 1 MB/s while the
 * writer was busy under a quarter of the time.
 */
export const clientLimited = (report: WritesReport) =>
  report.megabytesPerSecond < 1 && report.writerBusyFraction < 0.25;

export const writesTable = (context: Step4Context, runs: ReadonlyArray<Run<WritesReport>>) => {
  const header = [
    "Batch rows",
    "Clients in flight",
    "Rows/s acknowledged",
    "MB/s",
    "Client p50 / p90 / p99 (ms)",
    "Writer CPU per batch p50 / p99 (ms)",
    "API CPU per batch p50 / p99 (ms)",
    "Writer busy",
    "Failures",
  ];
  const rows = runs.map((run) =>
    run.ok
      ? `| ${num(run.report.load.batchRows)} | ${num(run.report.load.concurrency)} | ${num(run.report.rowsPerSecond)} | ${num(run.report.megabytesPerSecond, 2)} | ${pct(run.report.latencyMs)} | ${num(run.report.writerCpuMsPerBatch.p50)} / ${num(run.report.writerCpuMsPerBatch.p99)} | ${num(run.report.apiCpuMsPerBatch.p50)} / ${num(run.report.apiCpuMsPerBatch.p99)} | ${num(run.report.writerBusyFraction, 2)} | ${failuresOf(run.report.failures)} |`
      : missingRow(run.name, run, header.length),
  );
  const notes = runs.flatMap((run) =>
    run.ok && clientLimited(run.report)
      ? [
          `- **The ${num(run.report.load.batchRows)}-row row is a client limit, not a writer limit.** At ${num(run.report.load.concurrency)} clients the run moved ${num(run.report.megabytesPerSecond, 2)} MB/s and the writer was busy ${num(run.report.writerBusyFraction, 2)} of the time. Compare the writer-bound runs on \`preview-1\` in "Writer throughput and limits".`,
        ]
      : [],
  );
  return [
    caption("Writes.", context, runs),
    "",
    table(header, rows),
    "",
    "The load is closed-loop: each client waits for its answer, so rates follow latency. Writer busy is the writer's CPU over the run's wall time.",
    ...(notes.length === 0 ? [] : ["", ...notes]),
  ].join("\n");
};

export const splitTable = (context: Step4Context, runs: ReadonlyArray<Run<SplitReport>>) => {
  const header = [
    "Writers",
    "Run",
    "Split (client, ms)",
    "Longest gap between acknowledgements (ms)",
    "Failed writes",
    "Writes",
  ];
  const rows = runs.map((run) =>
    run.ok
      ? `| ${num(run.report.load.writers)} | \`${run.name}\` | ${num(run.report.splitClientMs)} | ${num(run.report.longestAckGapDuringSplitMs)} | ${num(run.report.failedPuts)} | ${num(run.report.totalPuts)} |`
      : missingRow("?", run, header.length),
  );
  const byWriters = new Map<number, Array<SplitReport>>();
  for (const run of runs)
    if (run.ok)
      byWriters.set(run.report.load.writers, [
        ...(byWriters.get(run.report.load.writers) ?? []),
        run.report,
      ]);
  const medians = [...byWriters.entries()].map(
    ([writers, reports]) =>
      `- **${writers} writers:** median longest gap ${num(median(reports.map((r) => r.longestAckGapDuringSplitMs)))} ms over ${reports.length} runs; ${num(reports.reduce((n, r) => n + r.failedPuts, 0))} failed writes.`,
  );
  return [caption("Split pause.", context, runs), "", table(header, rows), "", ...medians].join(
    "\n",
  );
};

export const compactionTable = (
  context: Step4Context,
  compactions: ReadonlyArray<Run<CompactionReport>>,
  flushes: ReadonlyArray<Run<FlushCpuReport>>,
) => {
  const header = [
    "Run",
    "CPU per 60 MiB flush (ms)",
    "Flush wall p50 (ms)",
    "Compaction step CPU (ms)",
    "Compacted after (s)",
    "Outcomes",
  ];
  const rows = compactions.map((run) => {
    if (!run.ok) return missingRow(`\`${run.name}\``, run, header.length);
    const steps = run.report.compactorCpuMsByEventKind["rpc"];
    const outcomes = [
      ...new Set([...run.report.compactorOutcomes, ...run.report.writerOutcomes]),
    ].join(", ");
    return `| \`${run.name}\` | ${num(run.report.flushCpuMsPerFlush)} | ${num(run.report.flushWallMs.p50)} | ${steps === undefined ? "none recorded" : `${num(steps.p50)} (${num(steps.count)} step${steps.count === 1 ? "" : "s"}, max ${num(steps.max)})`} | ${run.report.compacted ? num(run.report.compactedAfterSeconds, 1) : "not compacted"} | ${outcomes} |`;
  });
  const flushHeader = [
    "Value bytes",
    "Rows",
    "SST MiB",
    "CPU (ms)",
    "Wall (ms)",
    "CPU per MiB (ms)",
    "CPU per row (µs)",
    "Outcomes",
  ];
  const flushRows = flushes.map((run) =>
    run.ok
      ? `| ${num(run.report.valueBytes)} | ${num(run.report.rowsWritten)} | ${num(run.report.sstMiB, 1)} | ${num(run.report.cpuMs)} | ${num(run.report.wallMs)} | ${num(run.report.cpuMsPerMiB, 1)} | ${num(run.report.cpuMicrosPerRow, 1)} | ${[...new Set(run.report.flushInvocations.map((f) => f.outcome))].join(", ")} |`
      : missingRow(`\`${run.name}\``, run, flushHeader.length),
  );
  return [
    caption("Compaction and flush CPU.", context, compactions),
    "",
    table(header, rows),
    "",
    caption("Flush CPU per MiB.", context, flushes),
    "",
    table(flushHeader, flushRows),
  ].join("\n");
};

export const transactionsTable = (
  context: Step4Context,
  runs: ReadonlyArray<Run<TransactionsReport>>,
) => {
  const header = [
    "Transactors / writers",
    "Seconds",
    "Held open (s)",
    "Transactions",
    "Outcomes",
    "`SnapshotTooOld` rate",
    "p50 / p90 / p99 (ms)",
    "Forced flushes",
  ];
  const rows = runs.map((run) =>
    run.ok
      ? `| ${num(run.report.load.transactors)} / ${num(run.report.load.writers)} | ${num(run.report.load.seconds)} | ${num(run.report.load.holdSeconds)} | ${num(run.report.transactions)} | ${failuresOf(run.report.outcomes)} | ${num(run.report.snapshotTooOldRate, 3)} | ${pct(run.report.transactionMs)} | ${num(run.report.forcedFlushes)} |`
      : missingRow("?", run, header.length),
  );
  return [caption("Transactions.", context, runs), "", table(header, rows)].join("\n");
};

export const manifestCasTable = (
  context: Step4Context,
  runs: ReadonlyArray<Run<ManifestCasReport>>,
) => {
  const header = [
    "Run",
    "Flushes",
    "`ManifestStore.update` p50 / p90 / p99 (ms)",
    "From its own view, p50",
    "After a load, p50",
    "CAS attempts, max",
    "Writer flush p50 / p90 / p99 (ms)",
    "Client flush p50 (ms)",
  ];
  const rows = runs.map((run) =>
    run.ok
      ? `| \`${run.name}\` | ${num(run.report.flushes)} | ${pct(run.report.manifestUpdateMs)} | ${num(run.report.manifestUpdateFromOwnViewMs.p50)} | ${num(run.report.manifestUpdateLoadedMs.p50)} | ${num(run.report.casAttempts.max)} | ${pct(run.report.writerFlushMs)} | ${num(run.report.flushClientMs.p50)} |`
      : missingRow(`\`${run.name}\``, run, header.length),
  );
  return [
    caption("Manifest commits and small flushes.", context, runs),
    "",
    table(header, rows),
  ].join("\n");
};

export const latencyTable = (context: Step4Context, runs: ReadonlyArray<Run<LatencyReport>>) => {
  const header = [
    "Operation",
    "Sampled",
    "p50 / p90 / p99 (ms)",
    "Max (ms)",
    "Block reads p50 / p90",
  ];
  const rows = runs.flatMap((run) =>
    run.ok
      ? run.report.operations.map(
          (op) =>
            `| ${op.operation} | ${num(op.latencyMs.count)} | ${pct(op.latencyMs)} | ${num(op.latencyMs.max)} | ${num(op.blockReads.p50)} / ${num(op.blockReads.p90)} |`,
        )
      : [missingRow("all", run, header.length)],
  );
  return [
    caption("Read latency, server side.", context, runs),
    "",
    table(header, rows),
    "",
    "Latencies are span durations in the API Worker, so they leave out the client's round trip.",
  ].join("\n");
};
const LATEST_TARGET = "shaledb, reads at Latest";
const FLUSHED_TARGET = "shaledb, reads at Flushed";
const PLAIN_TARGET = "plain Durable Object SQLite";
const TARGET_ORDER = [LATEST_TARGET, FLUSHED_TARGET, PLAIN_TARGET];
const targetLabel = (name: string) =>
  name
    .replace("reads at Latest", "reads at `Latest`")
    .replace("reads at Flushed", "reads at `Flushed`");

/** Each target's report in each run that has one. */
const targetsByName = (runs: ReadonlyArray<Run<BaselineReport>>) =>
  TARGET_ORDER.map((name) => ({
    name,
    perRun: runs.map((run) =>
      run.ok ? run.report.targets.find((t) => t.target === name) : undefined,
    ),
    entries: runs.map((run) => ({
      run,
      target: run.ok ? run.report.targets.find((t) => t.target === name) : undefined,
    })),
  }));

export const baselineTables = (
  context: Step4Context,
  runs: ReadonlyArray<Run<BaselineReport>>,
  rowCost: {
    readonly memtableInsert: number;
    readonly memtableTrim: number;
    readonly plainInsert: number;
  },
  rowCostSource: string,
) => {
  const targets = targetsByName(runs);
  const clientHeader = [
    "Target",
    "Run",
    "Rows/s",
    "Write p50 / p90 / p99 (ms)",
    "Reads/s",
    "Read p50 / p90 / p99 (ms)",
    "Failures",
  ];
  const clientRows = targets.flatMap(({ name, perRun, entries }) => {
    const present = perRun.filter((t): t is BaselineTarget => t !== undefined);
    const rows = entries.map(({ run, target: t }) =>
      t === undefined
        ? missingRow(targetLabel(name), run, clientHeader.length)
        : `| ${targetLabel(name)} | \`${run.name}\` | ${num(t.acknowledgedRowsPerSecond)} | ${pct(t.writeLatencyMs)} | ${num(t.readsPerSecond, 1)} | ${pct(t.readLatencyMs)} | ${failuresOf(t.failures)} |`,
    );
    const m = (f: (t: BaselineTarget) => number, d = 0) => num(median(present.map(f)), d);
    return present.length === 0
      ? rows
      : [
          ...rows,
          `| **${targetLabel(name)}** | **median of ${present.length}** | **${m((t) => t.acknowledgedRowsPerSecond)}** | **${m((t) => t.writeLatencyMs.p50)} / ${m((t) => t.writeLatencyMs.p90)} / ${m((t) => t.writeLatencyMs.p99)}** | **${m((t) => t.readsPerSecond, 1)}** | **${m((t) => t.readLatencyMs.p50)} / ${m((t) => t.readLatencyMs.p90)} / ${m((t) => t.readLatencyMs.p99)}** | |`,
        ];
  });
  const stageHeader = [
    "Target (median of runs)",
    "Worker invocations",
    "Worker CPU (ms)",
    "Durable Object invocations",
    "Durable Object CPU (ms)",
    "R2 class A",
    "R2 class B",
  ];
  const stageRows = targets.map(({ name, perRun }) => {
    const present = perRun.filter((t): t is BaselineTarget => t !== undefined);
    if (present.length === 0) return `| ${targetLabel(name)} | no run has it |${" — |".repeat(5)}`;
    const m = (f: (t: BaselineTarget) => number) => num(median(present.map(f)));
    return `| ${targetLabel(name)} | ${m((t) => t.workerInvocations.count)} | ${m((t) => t.workerInvocations.cpuMs)} | ${m((t) => t.durableObjectInvocations.count)} | ${m((t) => t.durableObjectInvocations.cpuMs)} | ${m((t) => t.r2Requests.classA)} | ${m((t) => t.r2Requests.classB)} |`;
  });
  const costHeader = [
    "Target",
    ...runs.map((r) => `\`${r.name}\``),
    "Median",
    "of which SQLite row writes (median)",
  ];
  const costRows = targets.map(({ name, perRun }) => {
    const costs = perRun.map((t) => (t === undefined ? undefined : baselineCost(t, rowCost)));
    const present = costs.filter((c): c is ReturnType<typeof baselineCost> => c !== undefined);
    return `| ${targetLabel(name)} | ${costs.map((c) => (c === undefined ? "not available" : usd(c.perMillionRows))).join(" | ")} | ${usd(median(present.map((c) => c.perMillionRows)))} | ${usd(median(present.map((c) => c.sqlitePerMillionRows)))} |`;
  });
  const load = runs.find((r): r is Extract<Run<BaselineReport>, { ok: true }> => r.ok)?.report.load;
  return [
    caption("Baseline against a plain Durable Object SQLite table.", context, runs),
    load === undefined
      ? "No run has a report."
      : `Each run: ${load.writers} writers sending ${load.batchRows}-row batches of ${load.valueBytes}-byte values and ${load.readers} readers doing point reads, for ${load.seconds} seconds per target, one target after the other.`,
    "",
    table(clientHeader, clientRows),
    "",
    "Stage side, for each target's timed run (tail records and spans):",
    "",
    table(stageHeader, stageRows),
    "",
    `Cost per million rows written with this read mix, at \`PRICES_PER_MILLION\` (Cloudflare's overage prices, fetched 2026-11-26, allowances ignored: an assumption, not a bill). SQLite rows per row written: memtable ${num(rowCost.memtableInsert + rowCost.memtableTrim)} (${num(rowCost.memtableInsert)} on insert, ${num(rowCost.memtableTrim)} on trim), plain table ${num(rowCost.plainInsert)}, ${rowCostSource}.`,
    "",
    table(costHeader, costRows),
  ].join("\n");
};

// ---------------------------------------------------------------------------------------------
// Targets (docs/handoff-v1.md) and the capacity table (docs/operations.md)
// ---------------------------------------------------------------------------------------------

/** The step-4 runs, read. */
export interface Step4Runs {
  readonly writes: ReadonlyArray<Run<WritesReport>>;
  readonly splits: ReadonlyArray<Run<SplitReport>>;
  readonly compactions: ReadonlyArray<Run<CompactionReport>>;
  readonly flushes: ReadonlyArray<Run<FlushCpuReport>>;
  readonly transactions: ReadonlyArray<Run<TransactionsReport>>;
  readonly manifestCas: ReadonlyArray<Run<ManifestCasReport>>;
  readonly latency: ReadonlyArray<Run<LatencyReport>>;
  readonly baseline: ReadonlyArray<Run<BaselineReport>>;
}

const okReports = <A>(runs: ReadonlyArray<Run<A>>) =>
  runs.flatMap((run) => (run.ok ? [run.report] : []));
const names = (runs: ReadonlyArray<Run<unknown>>) => runs.map((r) => `\`${r.name}\``).join(", ");
const verdict = (met: boolean | undefined) =>
  met === undefined ? "no run" : met ? "met" : "not met";

export const targetsTable = (context: Step4Context, runs: Step4Runs) => {
  const cas = okReports(runs.manifestCas);
  const update = cas.length === 0 ? undefined : median(cas.map((r) => r.manifestUpdateMs.p50));
  const flush = cas.length === 0 ? undefined : median(cas.map((r) => r.writerFlushMs.p50));
  const splitRows = [4, 16].map((writers) => {
    const reports = okReports(runs.splits).filter((r) => r.load.writers === writers);
    const pause =
      reports.length === 0 ? undefined : median(reports.map((r) => r.longestAckGapDuringSplitMs));
    const failed = reports.reduce((n, r) => n + r.failedPuts, 0);
    return `| WS3: split write pause p50 ≤ 6 s, zero failed writes (${writers} writers; \`preview-1\`) | ${pause === undefined ? "no run" : `median longest gap ${num(pause)} ms over ${reports.length} runs; ${num(failed)} failed`} | ${verdict(pause === undefined ? undefined : pause <= 6000 && failed === 0)} | ${names(runs.splits.filter((r) => r.name.includes(`-${writers}-`)))} |`;
  });
  const writes = okReports(runs.writes);
  const outcomes = [
    ...okReports(runs.compactions).flatMap((r) => [...r.compactorOutcomes, ...r.writerOutcomes]),
    ...okReports(runs.flushes).flatMap((r) => r.flushInvocations.map((f) => f.outcome)),
  ];
  const exceeded = outcomes.filter((o) => o.startsWith("exceeded"));
  const rows = [
    `| WS2: \`ManifestStore.update\` p50 ≤ 500 ms (\`preview-1\`) | ${update === undefined ? "no run" : `median of run p50s ${num(update)} ms`} | ${verdict(update === undefined ? undefined : update <= 500)} | ${names(runs.manifestCas)} |`,
    `| WS2: small-memtable flush p50 ≤ 900 ms (\`preview-1\`) | ${flush === undefined ? "no run" : `median of run p50s ${num(flush)} ms (writer flush span)`} | ${verdict(flush === undefined ? undefined : flush <= 900)} | ${names(runs.manifestCas)} |`,
    ...splitRows,
    `| WS6: rows/s recorded at 100- and 1,000-row batches | ${writes.length === 0 ? "no run" : writes.map((r) => `${num(r.load.batchRows)} rows: ${num(r.rowsPerSecond)} rows/s`).join("; ")} | ${verdict(writes.length === 0 ? undefined : writes.length === 2)} | ${names(runs.writes)} |`,
    `| WS6: no \`exceededCpu\` or \`exceededMemory\` in any measurement | ${outcomes.length === 0 ? "no outcome recorded" : `outcomes the compaction and flush runs report: ${[...new Set(outcomes)].join(", ")}; the other runs report none`} | ${verdict(outcomes.length === 0 ? undefined : exceeded.length === 0)} | ${names([...runs.compactions, ...runs.flushes])} |`,
    `| WS10: the soaks | in \`docs/release-v1.md\` §5, not these runs | — | — |`,
  ];
  return [
    `**Targets of \`docs/handoff-v1.md\` against these runs.** Commit \`${context.commit}\`${context.release ? " (the release commit)" : " (provisional)"}. The targets name \`preview-1\`; these runs are on \`${context.stage}\`.`,
    "",
    table(["Target (workstream; stage it names)", "Measured", "Met", "Runs"], rows),
  ].join("\n");
};

export const capacityTable = (context: Step4Context, runs: Step4Runs) => {
  const label = `commit \`${context.commit}\`${context.release ? "" : ", provisional"}`;
  const writes = (rows: number) => {
    const r = okReports(runs.writes).find((w) => w.load.batchRows === rows);
    return r === undefined
      ? `| One writer, ${num(rows)}-row batches | no run | — |`
      : `| One writer, ${num(rows)}-row batches | ${num(r.rowsPerSecond)} rows/s at ${num(r.load.concurrency)} clients; p50 latency ${num(r.latencyMs.p50)} ms${clientLimited(r) ? " (a client limit)" : ""} | ${label} |`;
  };
  const cas = okReports(runs.manifestCas);
  const comp = okReports(runs.compactions);
  const steps = comp.flatMap((r) =>
    r.compactorCpuMsByEventKind["rpc"] === undefined
      ? []
      : [r.compactorCpuMsByEventKind["rpc"].max],
  );
  const split = okReports(runs.splits);
  const rows = [
    writes(1000),
    writes(100),
    "| Unflushed data per writer | 256 MiB (`maxUnflushedBytes`); then `WriteStalled` until a flush | setting |",
    "| L0 SSTs before writes stall | 16 (`l0MaxSsts`) | setting |",
    `| Manifest commit (one CAS) | ${cas.length === 0 ? "no run" : `p50 ${cas.map((r) => num(r.manifestUpdateMs.p50)).join(" and ")} ms`} | ${label} |`,
    `| Small flush | ${cas.length === 0 ? "no run" : `p50 ${cas.map((r) => num(r.writerFlushMs.p50)).join(" and ")} ms`} | ${label} |`,
    `| 60 MiB flush | ${comp.length === 0 ? "no run" : `${comp.map((r) => num(r.flushCpuMsPerFlush / 1000, 1)).join(" and ")} s CPU; wall p50 ${comp.map((r) => num(r.flushWallMs.p50 / 1000, 1)).join(" and ")} s`} | ${label} |`,
    `| Compaction step | ${steps.length === 0 ? "no run" : `${steps.map((s) => num(s / 1000, 1)).join(" and ")} s CPU`} | ${label} |`,
    `| Split (seal and fork) | ${split.length === 0 ? "no run" : `writes pause ${num(median(split.map((r) => r.longestAckGapDuringSplitMs)) / 1000, 1)} s (median longest gap of ${split.length} runs), ${num(split.reduce((n, r) => n + r.failedPuts, 0))} failed`} | ${label} |`,
    "| Transactions held open | at least 60 s (`transactionRetentionMs`) across flushes | setting; measured at `5f2850b` (D58) |",
    "| Concurrent compactions | 200 at once completed in 16 s | commit `308defd` (D61) |",
    "| GC sweep | 50 databases per run, ~160 ms CPU | commit `e06f3eb` (D60) |",
  ];
  return [
    `On \`${context.stage}\` with production settings (\`measurements.md\`, "Step 4"). The Basis column says what each figure rests on.`,
    "",
    table(["What", "Measured", "Basis"], rows),
  ].join("\n");
};

/** The README's throughput block. */
export const readmeWrites = (context: Step4Context, runs: Step4Runs) => {
  const rows = runs.writes.map((run) =>
    run.ok
      ? `| ${num(run.report.load.batchRows)} | ${num(run.report.load.concurrency)} | ${num(run.report.rowsPerSecond)} | ${pct(run.report.latencyMs)} | ${num(run.report.writerBusyFraction, 2)} |`
      : `| not available: \`${run.name}\` ${run.why} | — | — | — | — |`,
  );
  return [
    `Measured on \`${context.stage}\`, production settings, commit \`${context.commit}\`${context.release ? " (the release commit)" : " (provisional: not the release commit)"}; 16-byte keys and 100-byte values, from a laptop on a US residential connection, so latencies include an internet round trip:`,
    "",
    table(
      [
        "Batch rows",
        "Batches in flight",
        "Rows/s acknowledged",
        "Client latency p50 / p90 / p99 (ms)",
        "Writer busy",
      ],
      rows,
    ),
    "",
    `Writer busy below 1 means these are the rates measured, not a proven ceiling ([\`docs/measurements.md\`](docs/measurements.md), "Step 4").`,
  ].join("\n");
};

/** The README's baseline block: the median of each run, and what the comparison measured. */
export const readmeBaseline = (
  context: Step4Context,
  runs: Step4Runs,
  rowCost: Parameters<typeof baselineTables>[2],
) => {
  const targets = targetsByName(runs.baseline);
  const ok = runs.baseline.filter((r) => r.ok).length;
  const m = (name: string, f: (t: BaselineTarget) => number) =>
    median(
      (targets.find((t) => t.name === name)?.perRun ?? [])
        .filter((t): t is BaselineTarget => t !== undefined)
        .map(f),
    );
  const rows = targets.map(
    ({ name }) =>
      `| ${targetLabel(name)} | ${num(m(name, (t) => t.acknowledgedRowsPerSecond))} | ${num(m(name, (t) => t.writeLatencyMs.p50))} / ${num(m(name, (t) => t.writeLatencyMs.p99))} | ${num(
        m(name, (t) => t.readsPerSecond),
        1,
      )} | ${num(m(name, (t) => t.readLatencyMs.p50))} / ${num(m(name, (t) => t.readLatencyMs.p99))} | ${usd(median((targets.find((t) => t.name === name)?.perRun ?? []).filter((t): t is BaselineTarget => t !== undefined).map((t) => baselineCost(t, rowCost).perMillionRows)))} |`,
  );
  return [
    `The same workload against shaledb and against one table in a plain Durable Object, on \`${context.stage}\` with production settings, from the same client, one target after the other, each on a new database or object. Median of ${ok} of ${runs.baseline.length} runs, commit \`${context.commit}\`${context.release ? " (the release commit)" : " (provisional: not the release commit)"}${
      ok < runs.baseline.length
        ? `; not available: ${runs.baseline
            .filter((r) => !r.ok)
            .map((r) => `\`${r.name}\` (${r.ok ? "" : r.why})`)
            .join(", ")}`
        : ""
    }.`,
    "",
    table(
      [
        "Target",
        "Rows/s acknowledged",
        "Write p50 / p99 (ms)",
        "Reads/s",
        "Read p50 / p99 (ms)",
        "Cost per million rows written",
      ],
      rows,
    ),
    "",
    `- **Write p50:** ${num(m(LATEST_TARGET, (t) => t.writeLatencyMs.p50))} ms (\`Latest\` run), ${num(m(FLUSHED_TARGET, (t) => t.writeLatencyMs.p50))} ms (\`Flushed\` run), ${num(m(PLAIN_TARGET, (t) => t.writeLatencyMs.p50))} ms (plain table).`,
    `- **Read p50:** ${num(m(LATEST_TARGET, (t) => t.readLatencyMs.p50))} ms at \`Latest\`, ${num(m(FLUSHED_TARGET, (t) => t.readLatencyMs.p50))} ms at \`Flushed\`, ${num(m(PLAIN_TARGET, (t) => t.readLatencyMs.p50))} ms from the plain table.`,
    `- **Durable Object invocations at \`Flushed\`:** ${num(m(FLUSHED_TARGET, (t) => t.durableObjectInvocations.count))}, against ${num(m(FLUSHED_TARGET, (t) => t.writeBatchesPerSecond * t.seconds))} write batches: reads at \`Flushed\` reach no Durable Object when the two are equal.`,
    `- **SQLite rows written per row:** ${num(rowCost.memtableInsert + rowCost.memtableTrim)} for shaledb's memtable, ${num(rowCost.plainInsert)} for the plain table.`,
    "",
    `Each target's runs, the stage-side counts, and the price arithmetic are in [\`docs/measurements.md\`](docs/measurements.md), "Step 4".`,
  ].join("\n");
};

// ---------------------------------------------------------------------------------------------
// Reading a directory, and the blocks
// ---------------------------------------------------------------------------------------------

/** Every run step 4 reads, by its file name in the run directory. */
export const STEP4_RUNS = {
  writes: ["p3-writes-100", "p3-writes-1000"],
  splits: [
    "p3-split-4-1",
    "p3-split-4-2",
    "p3-split-4-3",
    "p3-split-16-1",
    "p3-split-16-2",
    "p3-split-16-3",
  ],
  compactions: ["p3-compaction-1", "p3-compaction-2"],
  flushes: ["p3-flushcpu-1k", "p3-flushcpu-100"],
  transactions: ["p3-transactions"],
  manifestCas: ["p3-manifest-cas-1", "p3-manifest-cas-2"],
  latency: ["p3-latency"],
  baseline: ["p3-baseline-1", "p3-baseline-2", "p3-baseline-3"],
} as const;

const readText = (path: string) => (existsSync(path) ? readFileSync(path, "utf8") : undefined);

/** Reads every step-4 run of a directory. */
export const readStep4Directory = (directory: string): Step4Runs => {
  const log = parseRunLog(readText(join(directory, "log")) ?? "");
  const read = <A, I>(name: string, schema: Schema.Codec<A, I>) =>
    readRun(name, readText(join(directory, `${name}.out`)), log.get(name), schema);
  return {
    writes: STEP4_RUNS.writes.map((n) => read(n, WritesReport)),
    splits: STEP4_RUNS.splits.map((n) => read(n, SplitReport)),
    compactions: STEP4_RUNS.compactions.map((n) => read(n, CompactionReport)),
    flushes: STEP4_RUNS.flushes.map((n) => read(n, FlushCpuReport)),
    transactions: STEP4_RUNS.transactions.map((n) => read(n, TransactionsReport)),
    manifestCas: STEP4_RUNS.manifestCas.map((n) => read(n, ManifestCasReport)),
    latency: STEP4_RUNS.latency.map((n) => read(n, LatencyReport)),
    baseline: STEP4_RUNS.baseline.map((n) => read(n, BaselineReport)),
  };
};

/** The row cost: the directory's own probe if it has one, else the constants. */
export const rowCostFor = (directory: string) => {
  const probe = readRun(
    "p3-rowcost",
    readText(join(directory, "p3-rowcost.json")),
    undefined,
    RowCostReport,
  );
  return probe.ok
    ? {
        rowCost: rowCostOf(probe.report),
        source: "measured by this directory's row-cost probe (`p3-rowcost.json`)",
      }
    : {
        rowCost: ROW_COST_ED4E787,
        source:
          "measured by the row-cost probe on `preview-3` at `ed4e787` on 2026-11-27 (`ROW_COST_ED4E787`)",
      };
};

/** Each block's Markdown, by block name. */
export const step4Blocks = (
  context: Step4Context,
  runs: Step4Runs,
  rowCost: { readonly rowCost: Parameters<typeof baselineTables>[2]; readonly source: string },
): Readonly<Record<Step4Block, string>> => ({
  measurements: [
    `## Step 4 on \`${context.commit}\`${context.release ? " (the release commit)" : " (provisional)"}`,
    "",
    `Generated by \`node tools/scenario-cli/src/step4-tables.ts <directory> ${context.commit}\` from the run outputs; do not edit by hand. ${context.release ? "These are the release commit's figures." : "Provisional: this is not the release commit, and the release-commit runs replace this section."}`,
    "",
    writesTable(context, runs.writes),
    "",
    splitTable(context, runs.splits),
    "",
    compactionTable(context, runs.compactions, runs.flushes),
    "",
    transactionsTable(context, runs.transactions),
    "",
    manifestCasTable(context, runs.manifestCas),
    "",
    latencyTable(context, runs.latency),
    "",
    baselineTables(context, runs.baseline, rowCost.rowCost, rowCost.source),
    "",
    targetsTable(context, runs),
  ].join("\n"),
  "readme-writes": readmeWrites(context, runs),
  "readme-baseline": readmeBaseline(context, runs, rowCost.rowCost),
  capacity: capacityTable(context, runs),
});

/** Every block, in the order they are printed and written. */
export const STEP4_BLOCKS = [
  "measurements",
  "readme-writes",
  "readme-baseline",
  "capacity",
] as const;
export type Step4Block = (typeof STEP4_BLOCKS)[number];

/** The file each block lives in, relative to the repository root. */
export const STEP4_BLOCK_FILES: Readonly<Record<Step4Block, string>> = {
  measurements: "docs/measurements.md",
  "readme-writes": "README.md",
  "readme-baseline": "README.md",
  capacity: "docs/operations.md",
};

/** Replaces the text between a block's markers, or fails naming the missing marker. */
export const replaceBlock = (text: string, block: Step4Block, content: string): string => {
  const begin = `<!-- STEP4 ${block}: begin -->`;
  const end = `<!-- STEP4 ${block}: end -->`;
  const from = text.indexOf(begin);
  const to = text.indexOf(end);
  if (from === -1 || to === -1 || to < from)
    throw new Error(
      `The markers ${begin} and ${end} are not both present, in order. Add them where the block belongs.`,
    );
  return `${text.slice(0, from + begin.length)}\n\n${content}\n\n${text.slice(to)}`;
};

const main = (argv: ReadonlyArray<string>) => {
  const flag = (name: string) => {
    const at = argv.indexOf(name);
    return at === -1 ? undefined : argv[at + 1];
  };
  const positional = argv.filter(
    (a, i) => !a.startsWith("--") && !["--stage", "--write"].includes(argv[i - 1] ?? ""),
  );
  const [directory, commit] = positional;
  if (directory === undefined || commit === undefined) {
    console.error(
      "Usage: node tools/scenario-cli/src/step4-tables.ts <directory> <commit> [--stage preview-3] [--release] [--write <repository root>]",
    );
    process.exit(2);
  }
  const context: Step4Context = {
    commit,
    stage: flag("--stage") ?? "preview-3",
    release: argv.includes("--release"),
  };
  const blocks = step4Blocks(context, readStep4Directory(directory), rowCostFor(directory));
  const root = flag("--write");
  if (root === undefined) {
    for (const [name, content] of Object.entries(blocks))
      console.log(`<!-- STEP4 ${name} -->\n\n${content}\n`);
    return;
  }
  for (const name of STEP4_BLOCKS) {
    const path = join(root, STEP4_BLOCK_FILES[name]);
    writeFileSync(path, replaceBlock(readFileSync(path, "utf8"), name, blocks[name]));
    console.log(`rewrote ${name} in ${STEP4_BLOCK_FILES[name]}`);
  }
};

if (import.meta.main) main(process.argv.slice(2));
