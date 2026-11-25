import { Option, Schema } from "effect";

import { type CollectedSpan, spanAttributeText } from "./collected-span.ts";

/** One stored trace, summarized for `list`. */
export interface TraceSummary {
  readonly traceId: string;
  /** Name of the earliest span with no parent in the trace, or of the earliest span. */
  readonly rootName: string;
  readonly serviceNames: ReadonlyArray<string>;
  readonly spanCount: number;
  readonly errorCount: number;
  /** Start of the earliest span, as Unix milliseconds. */
  readonly startUnixMs: number;
  /** Time from the earliest span start to the latest span end. */
  readonly durationMs: number;
  /** Every `database.id` attribute value in the trace. */
  readonly databaseIds: ReadonlyArray<string>;
}

const nanosToMs = (nanos: string): number => Number(BigInt(nanos) / 1_000n) / 1_000;

const byStart = (left: CollectedSpan, right: CollectedSpan): number => {
  const difference = BigInt(left.startTimeUnixNano) - BigInt(right.startTimeUnixNano);
  return difference < 0n ? -1 : difference > 0n ? 1 : 0;
};

/**
 * Summarizes one trace's spans.
 *
 * @param spans - Every span of one trace; must not be empty.
 */
export const summarizeTrace = (
  traceId: string,
  spans: ReadonlyArray<CollectedSpan>,
): TraceSummary => {
  // One pass gathers every aggregate. A span is a root candidate only when it has no parent or its
  // parent has not been seen yet; candidates are rechecked once every span id is known, which is a
  // pass over the few spans whose parent arrived later (usually just the root).
  const ids = new Set<string>();
  const serviceNames = new Set<string>();
  const databaseIds = new Set<string>();
  const rootCandidates: Array<{ readonly span: CollectedSpan; readonly start: bigint }> = [];
  let earliest: CollectedSpan | undefined;
  let start = 0n;
  let end = 0n;
  let errorCount = 0;
  for (const span of spans) {
    const spanStart = BigInt(span.startTimeUnixNano);
    const spanEnd = BigInt(span.endTimeUnixNano);
    if (earliest === undefined || spanStart < start) {
      earliest = span;
      start = spanStart;
    }
    if (spanEnd > end) end = spanEnd;
    ids.add(span.spanId);
    serviceNames.add(span.serviceName);
    if (span.status.code === "Error") errorCount += 1;
    const databaseId = spanAttributeText(span, "database.id");
    if (Option.isSome(databaseId)) databaseIds.add(databaseId.value);
    if (Option.isNone(span.parentSpanId) || !ids.has(span.parentSpanId.value)) {
      rootCandidates.push({ span, start: spanStart });
    }
  }
  let root = earliest;
  let rootStart: bigint | undefined;
  for (const candidate of rootCandidates) {
    const parent = candidate.span.parentSpanId;
    const isRoot = Option.isNone(parent) || !ids.has(parent.value);
    if (isRoot && (rootStart === undefined || candidate.start < rootStart)) {
      root = candidate.span;
      rootStart = candidate.start;
    }
  }
  return {
    traceId,
    rootName: root?.name ?? "",
    serviceNames: [...serviceNames],
    spanCount: spans.length,
    errorCount,
    startUnixMs: nanosToMs(start.toString()),
    durationMs: end > start ? Number(end - start) / 1_000_000 : 0,
    databaseIds: [...databaseIds],
  };
};

/**
 * Inserts `summary` into `newest`, which is sorted newest first, by binary search, keeping at most
 * `limit` entries. A summary older than every kept one is dropped without shifting, so building
 * the newest `limit` of `n` traces costs O(n log limit) comparisons and never sorts all `n`.
 *
 * @param newest - Mutated in place; must already be sorted newest first and hold at most `limit`.
 */
export const insertNewestTraceSummary = (
  newest: Array<TraceSummary>,
  summary: TraceSummary,
  limit: number,
): void => {
  if (limit <= 0) return;
  const oldestKept = newest[newest.length - 1];
  if (
    newest.length >= limit &&
    oldestKept !== undefined &&
    summary.startUnixMs <= oldestKept.startUnixMs
  ) {
    return;
  }
  let low = 0;
  let high = newest.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((newest[middle]?.startUnixMs ?? 0) >= summary.startUnixMs) low = middle + 1;
    else high = middle;
  }
  newest.splice(low, 0, summary);
  if (newest.length > limit) newest.pop();
};

/** A span placed in its trace's tree, with its depth below the trace's roots. */
export interface SpanTreeLine {
  readonly depth: number;
  readonly span: CollectedSpan;
}

/**
 * Orders a trace's spans depth-first under their parents, children by start time. A span whose
 * parent was not collected is treated as a root.
 */
export const spanTree = (spans: ReadonlyArray<CollectedSpan>): ReadonlyArray<SpanTreeLine> => {
  const sorted = [...spans].sort(byStart);
  const ids = new Set(sorted.map((span) => span.spanId));
  const children = new Map<string, Array<CollectedSpan>>();
  const roots: Array<CollectedSpan> = [];
  for (const span of sorted) {
    if (Option.isSome(span.parentSpanId) && ids.has(span.parentSpanId.value)) {
      const siblings = children.get(span.parentSpanId.value) ?? [];
      siblings.push(span);
      children.set(span.parentSpanId.value, siblings);
    } else {
      roots.push(span);
    }
  }
  const lines: Array<SpanTreeLine> = [];
  const visit = (span: CollectedSpan, depth: number) => {
    lines.push({ depth, span });
    for (const child of children.get(span.spanId) ?? []) visit(child, depth + 1);
  };
  for (const root of roots) visit(root, 0);
  return lines;
};

const formatAttributes = (span: CollectedSpan): string => {
  const entries = Object.entries(span.attributes)
    .filter(([key]) => !key.startsWith("code.") && key !== "effect.exit")
    .map(([key, value]) => `${key}=${Array.isArray(value) ? value.join(",") : String(value)}`);
  return entries.length === 0 ? "" : ` {${entries.join(", ")}}`;
};

/** Labels for OTLP span kinds that name the side of a call: server, client, producer, consumer. */
const SPAN_KIND_LABELS: Readonly<Record<number, string>> = {
  2: " (server)",
  3: " (client)",
  4: " (producer)",
  5: " (consumer)",
};

/**
 * Renders a trace as an indented span tree with durations, attributes, status, and errors.
 *
 * @returns One line per span; an error status adds the error message on the following line.
 */
export const renderSpanTree = (spans: ReadonlyArray<CollectedSpan>): string =>
  spanTree(spans)
    .flatMap(({ depth, span }) => {
      const indent = "  ".repeat(depth);
      const status = span.status.code === "Error" ? " ✗ ERROR" : "";
      const line = `${indent}${span.name}${SPAN_KIND_LABELS[span.kind] ?? ""} ${span.durationMs.toFixed(2)}ms [${span.serviceName}]${status}${formatAttributes(span)}`;
      return span.status.code === "Error"
        ? [line, `${indent}  ! ${Option.getOrElse(span.status.message, () => "error")}`]
        : [line];
    })
    .join("\n");

/** Schema for a filter that selects spans by name, attribute, error status, or database id. */
export const SpanFilter = Schema.Struct({
  /** Exact span name. */
  name: Schema.optional(Schema.String),
  /** Prefix of the span name, such as `http.server`. */
  namePrefix: Schema.optional(Schema.String),
  /** Attributes the span must carry, compared as text. */
  attributes: Schema.optional(Schema.Record(Schema.String, Schema.String)),
  /** `database.id` attribute value. */
  databaseId: Schema.optional(Schema.String),
  /** When true, only spans with an error status. */
  error: Schema.optional(Schema.Boolean),
});

/** A filter that selects spans. */
export type SpanFilter = typeof SpanFilter.Type;

/** Whether `span` satisfies every condition in `filter`. */
export const matchesSpanFilter = (filter: SpanFilter, span: CollectedSpan): boolean => {
  if (filter.name !== undefined && span.name !== filter.name) return false;
  if (filter.namePrefix !== undefined && !span.name.startsWith(filter.namePrefix)) return false;
  if (filter.error === true && span.status.code !== "Error") return false;
  if (filter.databaseId !== undefined) {
    const id = spanAttributeText(span, "database.id");
    if (Option.isNone(id) || id.value !== filter.databaseId) return false;
  }
  for (const [key, expected] of Object.entries(filter.attributes ?? {})) {
    const actual = spanAttributeText(span, key);
    if (Option.isNone(actual) || actual.value !== expected) return false;
  }
  return true;
};

/** Latency statistics for one span name. */
export interface SpanNameStats {
  readonly name: string;
  readonly count: number;
  readonly errorCount: number;
  readonly p50Ms: number;
  readonly p90Ms: number;
  readonly p99Ms: number;
  readonly maxMs: number;
  readonly meanMs: number;
}

const percentile = (sorted: ReadonlyArray<number>, fraction: number): number => {
  if (sorted.length === 0) return 0;
  const index = Math.min(sorted.length - 1, Math.ceil(fraction * sorted.length) - 1);
  return sorted[Math.max(0, index)] ?? 0;
};

/**
 * Computes latency percentiles (nearest rank) per span name.
 *
 * @returns One entry per span name, slowest p99 first.
 */
export const spanStats = (spans: ReadonlyArray<CollectedSpan>): ReadonlyArray<SpanNameStats> => {
  const byName = new Map<string, Array<CollectedSpan>>();
  for (const span of spans) {
    const group = byName.get(span.name) ?? [];
    group.push(span);
    byName.set(span.name, group);
  }
  return [...byName.entries()]
    .map(([name, group]) => {
      const durations = group.map((span) => span.durationMs).sort((a, b) => a - b);
      return {
        name,
        count: group.length,
        errorCount: group.filter((span) => span.status.code === "Error").length,
        p50Ms: percentile(durations, 0.5),
        p90Ms: percentile(durations, 0.9),
        p99Ms: percentile(durations, 0.99),
        maxMs: durations[durations.length - 1] ?? 0,
        meanMs: durations.reduce((sum, value) => sum + value, 0) / Math.max(1, durations.length),
      };
    })
    .sort((left, right) => right.p99Ms - left.p99Ms);
};

/** Difference between two traces (or runs) for one span name. */
export interface SpanNameDiff {
  readonly name: string;
  readonly leftCount: number;
  readonly rightCount: number;
  readonly leftMeanMs: number;
  readonly rightMeanMs: number;
  readonly leftErrors: number;
  readonly rightErrors: number;
}

/**
 * Compares two span sets by span name: counts, mean durations, and errors.
 *
 * @returns Every span name present on either side, names whose counts differ first.
 */
export const diffSpans = (
  left: ReadonlyArray<CollectedSpan>,
  right: ReadonlyArray<CollectedSpan>,
): ReadonlyArray<SpanNameDiff> => {
  const leftStats = new Map(spanStats(left).map((stats) => [stats.name, stats]));
  const rightStats = new Map(spanStats(right).map((stats) => [stats.name, stats]));
  const names = [...new Set([...leftStats.keys(), ...rightStats.keys()])].sort();
  return names
    .map((name) => {
      const l = leftStats.get(name);
      const r = rightStats.get(name);
      return {
        name,
        leftCount: l?.count ?? 0,
        rightCount: r?.count ?? 0,
        leftMeanMs: l?.meanMs ?? 0,
        rightMeanMs: r?.meanMs ?? 0,
        leftErrors: l?.errorCount ?? 0,
        rightErrors: r?.errorCount ?? 0,
      };
    })
    .sort(
      (a, b) =>
        Number(b.leftCount !== b.rightCount) - Number(a.leftCount !== a.rightCount) ||
        a.name.localeCompare(b.name),
    );
};
