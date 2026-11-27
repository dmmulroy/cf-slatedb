import { DEFAULT_LSM_SETTINGS } from "@shaledb/lsm-core";

/**
 * The health report's judgement, apart from reading telemetry (D63, D82): given a stage's limit
 * hits from its tail records and its spans, which signals `docs/operations.md` names are present,
 * and which of them make the stage unhealthy.
 *
 * Every signal here is read from a span attribute, a span's error, or a tail record: the trace
 * collector keeps spans only and discards OTLP metrics (D82), so no `lsm_*` metric is an input.
 */

/** One span as `trace-cli grep --json` prints it, with the fields the report reads. */
export interface HealthSpan {
  readonly name: string;
  readonly startTimeUnixNano: string;
  readonly durationMs: number;
  readonly attributes: Readonly<Record<string, unknown>>;
  readonly events?:
    | ReadonlyArray<{
        readonly name: string;
        readonly attributes?: Readonly<Record<string, unknown>> | undefined;
      }>
    | undefined;
}

/** An invocation whose tail record ended `exceededCpu`, `exceededMemory`, or `exception`. */
export interface HealthLimitHit {
  readonly worker: string;
  readonly outcome: string;
  readonly eventKind: string;
  readonly cpuTimeMs: number;
}

/** The spans the report reads, each found by name (and `--error` where it says so). */
export interface HealthSpans {
  /** `DbWriter.write` spans with an error status. */
  readonly refusedWrites: ReadonlyArray<HealthSpan>;
  /** `DbWriter.flush` spans. */
  readonly flushes: ReadonlyArray<HealthSpan>;
  /**
   * `DbWriter.flush` spans with an error status. A writer that cannot flush retries from its alarm
   * with a backoff of at most `maxL0Age` (D102), so a failing writer shows here at least once a
   * minute.
   */
  readonly failedFlushes: ReadonlyArray<HealthSpan>;
  /** `Compactor.poll` spans. */
  readonly polls: ReadonlyArray<HealthSpan>;
  /** `ObjectStore.putMultipart` spans. */
  readonly uploads: ReadonlyArray<HealthSpan>;
  /** `GarbageCollector.collect` spans with an error status. */
  readonly failedCollections: ReadonlyArray<HealthSpan>;
  /** `MaintenanceSweep.sweepPage` spans with an error status. */
  readonly failedSweeps: ReadonlyArray<HealthSpan>;
  /** Every span with an error status. */
  readonly errors: ReadonlyArray<HealthSpan>;
}

/** Where a signal turns into an issue. */
export interface HealthThresholds {
  /** A database whose L0 reaches this many SSTs refuses writes (`l0MaxSsts`). */
  readonly l0MaxSsts: number;
  /** A flush p99 above this is an issue (`docs/operations.md`: "about 30 s"). */
  readonly flushP99Ms: number;
}

/** Production's thresholds: the engine's default `l0MaxSsts` and the guide's 30 s flush. */
export const DEFAULT_HEALTH_THRESHOLDS: HealthThresholds = {
  l0MaxSsts: DEFAULT_LSM_SETTINGS.l0MaxSsts,
  flushP99Ms: 30_000,
};

/** Percentile (nearest rank) of `values`, in their unit; `0` when empty. */
export const percentile = (values: ReadonlyArray<number>, fraction: number): number => {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  const rank = Math.min(sorted.length - 1, Math.max(0, Math.ceil(fraction * sorted.length) - 1));
  return sorted[rank] ?? 0;
};

/** Count, p50, p90, p99, and max of `values`. */
export const summarizeValues = (values: ReadonlyArray<number>) => ({
  count: values.length,
  p50: percentile(values, 0.5),
  p90: percentile(values, 0.9),
  p99: percentile(values, 0.99),
  max: values.length === 0 ? 0 : Math.max(...values),
});

/** The type of the exception a failed span recorded, such as `WriteStalledError`. */
export const exceptionType = (span: HealthSpan): string => {
  const type = span.events?.find((event) => event.name === "exception")?.attributes?.[
    "exception.type"
  ];
  return typeof type === "string" ? type : "unknown";
};

/** Counts of `values`, most frequent first. */
const tally = (values: ReadonlyArray<string>) =>
  Object.fromEntries(
    [
      ...values.reduce(
        (counts, value) => counts.set(value, (counts.get(value) ?? 0) + 1),
        new Map<string, number>(),
      ),
    ].sort((left, right) => right[1] - left[1]),
  );

const databaseOf = (span: HealthSpan) => {
  const id = span.attributes["database.id"];
  return typeof id === "string" ? id : "unknown";
};

const count = (value: unknown) => {
  const number = Number(value ?? 0);
  return Number.isFinite(number) ? number : 0;
};

const isTrue = (value: unknown) => value === true || value === "true";

const later = (left: HealthSpan, right: HealthSpan) =>
  BigInt(left.startTimeUnixNano) >= BigInt(right.startTimeUnixNano) ? left : right;

/**
 * Judges a stage's health from its limit hits and spans (D82).
 *
 * **Issues** (each makes the stage unhealthy):
 *
 * - an invocation that ended `exceededCpu`, `exceededMemory`, or `exception`;
 * - writes refused with `WriteStalled`, per database and reason (`writer.stall_reason`:
 *   `L0Backlog` or `UnflushedBytes`);
 * - a database whose latest poll reports corrupted SSTs (`compaction.corrupt_ssts`);
 * - a database whose L0 reached `l0MaxSsts` (`manifest.l0_ssts` on `Compactor.poll`);
 * - a compactor that found itself fenced (`compactor.fenced` on `Compactor.poll`): only its own
 *   Durable Object fences in for a database, so this means its state was overwritten. Since D78
 *   it fences in again on its next start; before D78 it stopped for good;
 * - a multipart upload with an unverified part (`object.parts_unverified`): R2 stopped returning
 *   part digests, so parts are no longer checked;
 * - a flush p99 above the threshold;
 * - failed collections or maintenance sweeps.
 *
 * **Reported, not issues:** uploads with a re-sent part (`object.part_retried`; transient unless
 * frequent), flush durations, the deepest L0 backlogs, and the most frequent error spans.
 *
 * Spans are sampled in production (P4), so every span count is a sample of the stage's.
 */
export const summarizeHealth = (
  limitHits: ReadonlyArray<HealthLimitHit>,
  spans: HealthSpans,
  thresholds: HealthThresholds = DEFAULT_HEALTH_THRESHOLDS,
) => {
  const backlog = new Map<string, number>();
  // Corruption is judged by each database's latest poll: a repaired one no longer reports it.
  const latestPoll = new Map<string, HealthSpan>();
  const fenced = new Map<string, number>();
  for (const poll of spans.polls) {
    const database = databaseOf(poll);
    if (isTrue(poll.attributes["compactor.fenced"])) {
      fenced.set(database, (fenced.get(database) ?? 0) + 1);
      continue;
    }
    backlog.set(
      database,
      Math.max(backlog.get(database) ?? 0, count(poll.attributes["manifest.l0_ssts"])),
    );
    const seen = latestPoll.get(database);
    latestPoll.set(database, seen === undefined ? poll : later(poll, seen));
  }
  const corrupt = [...latestPoll]
    .filter(([, poll]) => count(poll.attributes["compaction.corrupt_ssts"]) > 0)
    .map(([database]) => database);
  const atLimit = [...backlog].filter(([, l0]) => l0 >= thresholds.l0MaxSsts);
  // Sealed and fenced refusals are routing, not trouble; a stalled write is backpressure.
  const stalled = spans.refusedWrites.filter((span) => exceptionType(span) === "WriteStalledError");
  const stalledBy = tally(
    stalled.map((span) => {
      const reason = span.attributes["writer.stall_reason"];
      return `${databaseOf(span)} (${typeof reason === "string" ? reason : "reason not recorded"})`;
    }),
  );
  const unverified = spans.uploads.filter(
    (span) => count(span.attributes["object.parts_unverified"]) > 0,
  );
  const retried = spans.uploads.filter((span) => isTrue(span.attributes["object.part_retried"]));
  const flushMs = summarizeValues(spans.flushes.map((span) => span.durationMs));
  const deepestBacklog = [...backlog].sort((left, right) => right[1] - left[1]).slice(0, 5);
  const issues = [
    ...limitHits.map(
      (hit) =>
        `${hit.worker} invocation ended ${hit.outcome} (${hit.eventKind}, ${hit.cpuTimeMs} ms CPU)`,
    ),
    ...Object.entries(stalledBy).map(
      ([where, writes]) => `${writes} writes stalled (WriteStalled) on database ${where}`,
    ),
    ...corrupt.map((database) => `database ${database} has corrupted SSTs: see docs/operations.md`),
    ...atLimit.map(
      ([database, l0]) =>
        `database ${database} has ${l0} L0 SSTs, at its ${thresholds.l0MaxSsts} limit: writes are refused until compaction catches up`,
    ),
    ...[...fenced].map(
      ([database, polls]) =>
        `database ${database}'s compactor found itself fenced (${polls} polls): its state was overwritten (D78)`,
    ),
    ...(unverified.length > 0
      ? [
          `${unverified.length} multipart uploads stored parts whose ETag was not an MD5: parts are no longer checked`,
        ]
      : []),
    ...(flushMs.count > 0 && flushMs.p99 > thresholds.flushP99Ms
      ? [`flush p99 is ${flushMs.p99} ms, above ${thresholds.flushP99Ms} ms`]
      : []),
    ...Object.entries(tally(spans.failedFlushes.map((span) => databaseOf(span)))).map(
      ([database, failures]) =>
        `${failures} flushes failed on database ${database}: its acknowledged writes are not reaching R2`,
    ),
    ...(spans.failedCollections.length > 0
      ? [`${spans.failedCollections.length} collections failed`]
      : []),
    ...(spans.failedSweeps.length > 0
      ? [`${spans.failedSweeps.length} maintenance sweeps failed`]
      : []),
  ];
  return {
    healthy: issues.length === 0,
    issues,
    writes: {
      refusedByError: tally(spans.refusedWrites.map(exceptionType)),
      stalledByDatabase: stalledBy,
    },
    flushMs,
    l0Backlog: { deepest: Object.fromEntries(deepestBacklog), databases: backlog.size },
    corruptSsts: corrupt,
    fencedCompactors: Object.fromEntries(fenced),
    uploads: {
      multipart: spans.uploads.length,
      withRetriedPart: retried.length,
      withUnverifiedPart: unverified.length,
    },
    gc: {
      failedCollections: spans.failedCollections.length,
      failedSweeps: spans.failedSweeps.length,
    },
    errorSpans: Object.fromEntries(
      Object.entries(tally(spans.errors.map((span) => span.name))).slice(0, 10),
    ),
  };
};
