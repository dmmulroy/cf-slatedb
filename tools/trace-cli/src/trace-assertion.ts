import { Option, Schema } from "effect";

import { type CollectedSpan, spanAttributeText } from "./collected-span.ts";
import { SpanFilter, matchesSpanFilter } from "./trace-analysis.ts";

/** Schema for an expected number of matching spans. Omitted bounds are not checked. */
const SpanCount = Schema.Struct({
  equals: Schema.optional(Schema.Number),
  atLeast: Schema.optional(Schema.Number),
  atMost: Schema.optional(Schema.Number),
});

/**
 * Schema for one declarative trace check.
 *
 * - `SpanExists`: spans matching `span` exist (at least one unless `count` says otherwise); with
 *   `parent`, each counted span's parent must match it.
 * - `NoSpan`: no span matches `span` (for example "no `ObjectStore.list` on a read path").
 * - `SpanCount`: the number of spans matching `span` is within `count`.
 * - `NoErrorStatus`: no span has an error status, except those whose name is in `except`.
 * - `SameTrace`: one trace contains a span matching each filter in `spans` (context propagated).
 * - `Attribute`: every span matching `span` carries attribute `key`; never `forbiddenKeys`.
 */
export const TraceCheck = Schema.Union([
  Schema.TaggedStruct("SpanExists", {
    span: SpanFilter,
    parent: Schema.optional(SpanFilter),
    count: Schema.optional(SpanCount),
  }),
  Schema.TaggedStruct("NoSpan", { span: SpanFilter }),
  Schema.TaggedStruct("SpanCount", { span: SpanFilter, count: SpanCount }),
  Schema.TaggedStruct("NoErrorStatus", { except: Schema.optional(Schema.Array(Schema.String)) }),
  Schema.TaggedStruct("SameTrace", { spans: Schema.Array(SpanFilter) }),
  Schema.TaggedStruct("Attribute", {
    span: SpanFilter,
    requiredKeys: Schema.optional(Schema.Array(Schema.String)),
    forbiddenKeys: Schema.optional(Schema.Array(Schema.String)),
  }),
]);

/** One declarative trace check. */
export type TraceCheck = typeof TraceCheck.Type;

/**
 * Schema for which stored traces an assertion file checks. Every given condition must hold.
 */
export const TraceSelection = Schema.Struct({
  /** Only these traces. */
  traceIds: Schema.optional(Schema.Array(Schema.String)),
  /** Only traces with a span whose `database.id` attribute equals this. */
  databaseId: Schema.optional(Schema.String),
  /** Only traces whose earliest span started at or after this Unix millisecond time. */
  sinceUnixMs: Schema.optional(Schema.Number),
  /** Only traces with a span matching this filter. */
  containing: Schema.optional(SpanFilter),
});

/** Which stored traces an assertion checks. */
export type TraceSelection = typeof TraceSelection.Type;

/** Schema for an assertion file: which traces to check and the checks to run on them. */
export const TraceAssertion = Schema.Struct({
  select: TraceSelection,
  checks: Schema.Array(TraceCheck),
});

/** Which traces to check and the checks to run. */
export type TraceAssertion = typeof TraceAssertion.Type;

/**
 * Decodes an assertion file's JSON text. Unknown keys are errors: a misspelled bound such as
 * `"exactly"` would otherwise be dropped and the check would pass vacuously.
 */
export const decodeTraceAssertionJson = (text: string) =>
  Schema.decodeUnknownEffect(Schema.fromJsonString(TraceAssertion))(text, {
    onExcessProperty: "error",
  });

/** Outcome of one check. */
export interface TraceCheckResult {
  readonly check: TraceCheck;
  readonly passed: boolean;
  /** Why the check passed or failed, in terms of the spans it saw. */
  readonly detail: string;
}

/** Outcome of an assertion file. */
export interface TraceAssertionReport {
  readonly passed: boolean;
  readonly traceIds: ReadonlyArray<string>;
  readonly spanCount: number;
  readonly results: ReadonlyArray<TraceCheckResult>;
}

/** Whether a trace (its spans) satisfies a selection. */
export const traceMatchesSelection = (
  selection: TraceSelection,
  traceId: string,
  spans: ReadonlyArray<CollectedSpan>,
): boolean => {
  if (selection.traceIds !== undefined && !selection.traceIds.includes(traceId)) return false;
  if (selection.databaseId !== undefined) {
    const id = selection.databaseId;
    const has = spans.some((span) => {
      const value = spanAttributeText(span, "database.id");
      return Option.isSome(value) && value.value === id;
    });
    if (!has) return false;
  }
  if (selection.sinceUnixMs !== undefined) {
    const since = BigInt(Math.floor(selection.sinceUnixMs)) * 1_000_000n;
    const earliest = spans.reduce(
      (min, span) => (BigInt(span.startTimeUnixNano) < min ? BigInt(span.startTimeUnixNano) : min),
      BigInt(spans[0]?.startTimeUnixNano ?? "0"),
    );
    if (earliest < since) return false;
  }
  if (selection.containing !== undefined) {
    const filter = selection.containing;
    if (!spans.some((span) => matchesSpanFilter(filter, span))) return false;
  }
  return true;
};

const describeFilter = (filter: SpanFilter): string =>
  JSON.stringify(
    Object.fromEntries(Object.entries(filter).filter(([, value]) => value !== undefined)),
  );

const countSatisfies = (
  count: { equals?: number | undefined; atLeast?: number | undefined; atMost?: number | undefined },
  actual: number,
): boolean =>
  (count.equals === undefined || actual === count.equals) &&
  (count.atLeast === undefined || actual >= count.atLeast) &&
  (count.atMost === undefined || actual <= count.atMost);

const evaluateCheck = (
  check: TraceCheck,
  traces: ReadonlyMap<string, ReadonlyArray<CollectedSpan>>,
): TraceCheckResult => {
  const all = [...traces.values()].flat();
  switch (check._tag) {
    case "SpanExists": {
      const matching = all.filter((span) => matchesSpanFilter(check.span, span));
      const parentFilter = check.parent;
      const withParent =
        parentFilter === undefined
          ? matching
          : matching.filter((span) => {
              if (Option.isNone(span.parentSpanId)) return false;
              const parentId = span.parentSpanId.value;
              const parent = (traces.get(span.traceId) ?? []).find(
                (candidate) => candidate.spanId === parentId,
              );
              return parent !== undefined && matchesSpanFilter(parentFilter, parent);
            });
      const count = check.count ?? { atLeast: 1 };
      const passed = countSatisfies(count, withParent.length);
      return {
        check,
        passed,
        detail: `${withParent.length} span(s) match ${describeFilter(check.span)}${
          parentFilter === undefined
            ? ""
            : ` under a parent matching ${describeFilter(parentFilter)}`
        } (${matching.length} before the parent check); expected ${JSON.stringify(count)}.`,
      };
    }
    case "NoSpan": {
      const matching = all.filter((span) => matchesSpanFilter(check.span, span));
      return {
        check,
        passed: matching.length === 0,
        detail:
          matching.length === 0
            ? `No span matches ${describeFilter(check.span)}.`
            : `${matching.length} span(s) match ${describeFilter(check.span)}, in trace(s) ${[
                ...new Set(matching.map((span) => span.traceId)),
              ].join(", ")}.`,
      };
    }
    case "SpanCount": {
      const actual = all.filter((span) => matchesSpanFilter(check.span, span)).length;
      return {
        check,
        passed: countSatisfies(check.count, actual),
        detail: `${actual} span(s) match ${describeFilter(check.span)}; expected ${JSON.stringify(check.count)}.`,
      };
    }
    case "NoErrorStatus": {
      const except = new Set(check.except ?? []);
      const errors = all.filter((span) => span.status.code === "Error" && !except.has(span.name));
      return {
        check,
        passed: errors.length === 0,
        detail:
          errors.length === 0
            ? "No span has an error status."
            : `Spans with an error status: ${errors
                .map(
                  (span) =>
                    `${span.name} (${Option.getOrElse(span.status.message, () => "error")})`,
                )
                .join("; ")}.`,
      };
    }
    case "SameTrace": {
      const traceId = [...traces.entries()].find(([, spans]) =>
        check.spans.every((filter) => spans.some((span) => matchesSpanFilter(filter, span))),
      )?.[0];
      return {
        check,
        passed: traceId !== undefined,
        detail:
          traceId === undefined
            ? `No single trace contains spans matching all of ${check.spans.map(describeFilter).join(", ")}.`
            : `Trace ${traceId} contains spans matching every filter.`,
      };
    }
    case "Attribute": {
      const matching = all.filter((span) => matchesSpanFilter(check.span, span));
      const missing = matching.filter((span) =>
        (check.requiredKeys ?? []).some((key) => span.attributes[key] === undefined),
      );
      const forbidden = matching.filter((span) =>
        (check.forbiddenKeys ?? []).some((key) => span.attributes[key] !== undefined),
      );
      return {
        check,
        passed: matching.length > 0 && missing.length === 0 && forbidden.length === 0,
        detail: `${matching.length} span(s) match ${describeFilter(check.span)}; ${missing.length} lack a required key; ${forbidden.length} carry a forbidden key.`,
      };
    }
  }
};

/**
 * Runs an assertion's checks against the traces it selects.
 *
 * @param traces - Every stored trace's spans by trace id.
 * @returns The report; `passed` is false when no trace is selected or any check fails.
 */
export const evaluateTraceAssertion = (
  assertion: TraceAssertion,
  traces: ReadonlyMap<string, ReadonlyArray<CollectedSpan>>,
): TraceAssertionReport => {
  const selected = new Map(
    [...traces.entries()].filter(([traceId, spans]) =>
      traceMatchesSelection(assertion.select, traceId, spans),
    ),
  );
  const results = assertion.checks.map((check) => evaluateCheck(check, selected));
  return {
    passed: selected.size > 0 && results.every((result) => result.passed),
    traceIds: [...selected.keys()],
    spanCount: [...selected.values()].reduce((sum, spans) => sum + spans.length, 0),
    results,
  };
};
