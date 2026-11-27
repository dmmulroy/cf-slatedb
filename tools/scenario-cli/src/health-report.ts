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
  /**
   * Object reads: `ObjectStore.get`, `getRange`, `getVersioned`, `head`, `list`, `listPrefixes`,
   * and `ObjectReader.get`, `getRange` (the `BlockServer`'s). A read R2 failed transiently and
   * that was retried carries `object.read_retried` (D134).
   */
  readonly objectReads: ReadonlyArray<HealthSpan>;
  /** `GarbageCollector.collect` spans with an error status. */
  readonly failedCollections: ReadonlyArray<HealthSpan>;
  /** Every `GarbageCollector.collect` span. */
  readonly collections: ReadonlyArray<HealthSpan>;
  /** `MaintenanceSweep.sweepPage` spans with an error status. */
  readonly failedSweeps: ReadonlyArray<HealthSpan>;
  /** `ShardSplit.resumePending` spans: a split a waiting request had to finish (D101). */
  readonly resumedSplits: ReadonlyArray<HealthSpan>;
  /** Every span with an error status. */
  readonly errors: ReadonlyArray<HealthSpan>;
}

/** Where a signal turns into an issue. */
export interface HealthThresholds {
  /** A database whose L0 reaches this many SSTs refuses writes (`l0MaxSsts`). */
  readonly l0MaxSsts: number;
  /** A flush p99 above this is an issue (`docs/operations.md`: "about 30 s"). */
  readonly flushP99Ms: number;
  /**
   * The fraction of object reads that needed a retry above which retries are an issue (D134), once
   * at least {@link HealthThresholds.readRetriesMinimum} reads were retried.
   */
  readonly readRetryRate: number;
  /** Retried reads below this count are never an issue: too few to measure a rate. */
  readonly readRetriesMinimum: number;
}

/** The span names {@link HealthSpans.objectReads} holds. */
export const OBJECT_READ_SPAN_NAMES: ReadonlySet<string> = new Set([
  "ObjectStore.get",
  "ObjectStore.getRange",
  "ObjectStore.getVersioned",
  "ObjectStore.head",
  "ObjectStore.list",
  "ObjectStore.listPrefixes",
  "ObjectReader.get",
  "ObjectReader.getRange",
]);

/** Production's thresholds: the engine's default `l0MaxSsts` and the guide's 30 s flush. */
export const DEFAULT_HEALTH_THRESHOLDS: HealthThresholds = {
  l0MaxSsts: DEFAULT_LSM_SETTINGS.l0MaxSsts,
  flushP99Ms: 30_000,
  // R2's own rate is about one transient failure in 150,000 reads (the 130-minute soak, D134); one
  // in 10,000 is still noise. One in 1,000 is ten times below one in 100, where a retry's 25-75 ms
  // reaches the read p99 and failures that come together start to exhaust the retries.
  readRetryRate: 0.001,
  readRetriesMinimum: 10,
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
 * - failed collections or maintenance sweeps;
 * - a database whose latest collection deferred SST deletion (`gc.sst_collection_deferred` on
 *   `GarbageCollector.collect`): it retains more manifests than one collection reads, so its
 *   garbage SSTs are not being deleted (D98).
 *
 * - object reads retried after a transient R2 failure (`object.read_retried`, D134) above
 *   `readRetryRate` of the reads, once at least `readRetriesMinimum` were retried: R2 is failing
 *   far more often than its normal rate.
 *
 * **Reported, not issues:** uploads with a re-sent part (`object.part_retried`; transient unless
 * frequent), retried object reads below the rate, flush durations, the deepest L0 backlogs, and
 * the most frequent error spans.
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
  // Deferral is judged by each database's latest collection: one that caught up no longer defers.
  const latestCollection = new Map<string, HealthSpan>();
  for (const collection of spans.collections) {
    const database = databaseOf(collection);
    const seen = latestCollection.get(database);
    latestCollection.set(database, seen === undefined ? collection : later(collection, seen));
  }
  const deferred = [...latestCollection]
    .filter(([, collection]) => count(collection.attributes["gc.sst_collection_deferred"]) > 0)
    .map(([database, collection]) => ({
      database,
      unread: count(collection.attributes["gc.sst_collection_deferred"]),
      retained: count(collection.attributes["gc.retained_manifests"]),
    }));
  const corrupt = [...latestPoll]
    .filter(([, poll]) => count(poll.attributes["compaction.corrupt_ssts"]) > 0)
    .map(([database]) => database);
  const atLimit = [...backlog].filter(([, l0]) => l0 >= thresholds.l0MaxSsts);
  // Sealed and fenced refusals are routing, not trouble; a stalled write is backpressure.
  const stalled = spans.refusedWrites.filter((span) => exceptionType(span) === "WriteStalledError");
  const readsOfCorruptSsts = spans.errors.filter(
    (span) => exceptionType(span) === "SstCorruptedError" && !span.name.startsWith("Compact"),
  );
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
  const reads = spans.objectReads.filter((span) => OBJECT_READ_SPAN_NAMES.has(span.name));
  const retriedReads = reads.filter((span) => count(span.attributes["object.read_retried"]) > 0);
  const readRetryRate = reads.length === 0 ? 0 : retriedReads.length / reads.length;
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
    // A read that met a corrupted SST (StoredDataInvalid to its caller, D128) before any
    // compaction reported it: the error spans carry it, the polls may not yet.
    ...(readsOfCorruptSsts.length > 0
      ? [
          `${readsOfCorruptSsts.length} reads met a corrupted SST (SstCorruptedError), answered StoredDataInvalid: see docs/operations.md, "Corruption runbook"`,
        ]
      : []),
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
    ...(retriedReads.length >= thresholds.readRetriesMinimum &&
    readRetryRate > thresholds.readRetryRate
      ? [
          `${retriedReads.length} of ${reads.length} object reads were retried after a transient R2 failure (object.read_retried), above ${thresholds.readRetryRate * 100}%: see docs/operations.md, "What to watch"`,
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
    ...deferred.map(
      ({ database, unread, retained }) =>
        `database ${database}'s collection deferred SST deletion: it retains ${retained} manifests and left ${unread} unread, so its garbage SSTs stay until it commits fewer manifests per gcMinAge (D98)`,
    ),
    ...(spans.resumedSplits.length > 0
      ? [
          `${spans.resumedSplits.length} abandoned shard splits were finished by a waiting request (${[
            ...new Set(
              spans.resumedSplits.map((span) => {
                const id = span.attributes["shard.resumed_split"];
                return typeof id === "string" ? id : "unknown";
              }),
            ),
          ].join(", ")}): see docs/operations.md, "Shard splits"`,
        ]
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
    objectReads: {
      reads: reads.length,
      retried: retriedReads.length,
      retryRate: readRetryRate,
    },
    gc: {
      failedCollections: spans.failedCollections.length,
      sstCollectionDeferred: deferred.map(({ database }) => database),
      failedSweeps: spans.failedSweeps.length,
    },
    errorSpans: Object.fromEntries(
      Object.entries(tally(spans.errors.map((span) => span.name))).slice(0, 10),
    ),
  };
};
