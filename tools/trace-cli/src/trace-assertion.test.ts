import { Option } from "effect";
import { describe, expect, it } from "vite-plus/test";

import type { CollectedSpan } from "./collected-span.ts";
import {
  diffSpans,
  insertNewestTraceSummary,
  spanStats,
  summarizeTrace,
  type TraceSummary,
} from "./trace-analysis.ts";
import { evaluateTraceAssertion, type TraceCheck } from "./trace-assertion.ts";

const span = (
  fields: Partial<CollectedSpan> & Pick<CollectedSpan, "name" | "spanId">,
): CollectedSpan => ({
  traceId: "t1",
  parentSpanId: Option.none(),
  kind: 1,
  serviceName: "cf-slatedb-api",
  startTimeUnixNano: "1000000000",
  endTimeUnixNano: "2000000000",
  durationMs: 1000,
  attributes: {},
  status: { code: "Unset", message: Option.none() },
  events: [],
  ...fields,
});

const readTrace = [
  span({ name: "http.server GET", spanId: "a" }),
  span({
    name: "DbReader.get",
    spanId: "b",
    parentSpanId: Option.some("a"),
    attributes: { "database.id": "orders" },
  }),
  span({
    name: "BlockCache.block",
    spanId: "c",
    parentSpanId: Option.some("b"),
    attributes: { "cache.hit": false },
  }),
];

const check = (checks: ReadonlyArray<TraceCheck>, traces = new Map([["t1", readTrace]])) =>
  evaluateTraceAssertion({ select: { databaseId: "orders" }, checks }, traces);

describe("evaluateTraceAssertion", () => {
  it("passes when every check holds on the selected traces", () => {
    const report = check([
      { _tag: "SpanExists", span: { name: "DbReader.get" }, parent: { namePrefix: "http.server" } },
      { _tag: "NoSpan", span: { name: "ObjectStore.list" } },
      { _tag: "SpanCount", span: { name: "BlockCache.block" }, count: { equals: 1 } },
      { _tag: "NoErrorStatus" },
      { _tag: "SameTrace", spans: [{ name: "DbReader.get" }, { name: "BlockCache.block" }] },
      {
        _tag: "Attribute",
        span: { name: "BlockCache.block" },
        requiredKeys: ["cache.hit"],
        forbiddenKeys: ["key"],
      },
    ]);
    expect(report.results.filter((result) => !result.passed)).toEqual([]);
    expect(report.passed).toBe(true);
  });

  it("fails a forbidden span, a wrong parent, a wrong count, and an error status", () => {
    const failing = [
      ...readTrace,
      span({ name: "ObjectStore.list", spanId: "d", parentSpanId: Option.some("b") }),
      span({
        name: "ObjectStore.getRange",
        spanId: "e",
        parentSpanId: Option.some("c"),
        status: { code: "Error", message: Option.some("R2 unavailable") },
      }),
    ];
    const report = check(
      [
        { _tag: "NoSpan", span: { name: "ObjectStore.list" } },
        {
          _tag: "SpanExists",
          span: { name: "BlockCache.block" },
          parent: { name: "http.server GET" },
        },
        { _tag: "SpanCount", span: { namePrefix: "ObjectStore." }, count: { atMost: 1 } },
        { _tag: "NoErrorStatus" },
      ],
      new Map([["t1", failing]]),
    );
    expect(report.passed).toBe(false);
    expect(report.results.map((result) => result.passed)).toEqual([false, false, false, false]);
    expect(report.results[3]?.detail).toContain("R2 unavailable");
  });

  it("fails when the selection matches no trace", () => {
    const report = evaluateTraceAssertion(
      { select: { databaseId: "missing" }, checks: [{ _tag: "NoErrorStatus" }] },
      new Map([["t1", readTrace]]),
    );
    expect(report.passed).toBe(false);
    expect(report.traceIds).toEqual([]);
  });
});

describe("spanStats and diffSpans", () => {
  it("computes nearest-rank percentiles and reports count differences first", () => {
    const durations = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const spans = durations.map((durationMs, index) =>
      span({ name: "DbReader.get", spanId: String(index), durationMs }),
    );
    const [stats] = spanStats(spans);
    expect(stats).toMatchObject({
      count: 10,
      p50Ms: 5,
      p90Ms: 9,
      p99Ms: 10,
      maxMs: 10,
      meanMs: 5.5,
    });

    const diff = diffSpans(spans, [
      ...spans.slice(0, 5),
      span({ name: "ObjectStore.list", spanId: "x" }),
    ]);
    expect(diff.map((row) => [row.name, row.leftCount, row.rightCount])).toEqual([
      ["DbReader.get", 10, 5],
      ["ObjectStore.list", 0, 1],
    ]);
  });
});

describe("summarizeTrace", () => {
  it("finds the root even when its child arrives first, and aggregates in one pass", () => {
    const spans = [
      span({
        name: "DbReader.get",
        spanId: "b",
        parentSpanId: Option.some("a"),
        startTimeUnixNano: "2000000",
        endTimeUnixNano: "9000000",
        attributes: { "database.id": "orders" },
      }),
      span({
        name: "http.server GET",
        spanId: "a",
        startTimeUnixNano: "1000000",
        endTimeUnixNano: "5000000",
      }),
      span({
        name: "BlockServer.block",
        spanId: "c",
        parentSpanId: Option.some("remote"),
        startTimeUnixNano: "3000000",
        endTimeUnixNano: "4000000",
        serviceName: "cf-slatedb-blocks",
        status: { code: "Error", message: Option.some("miss") },
      }),
    ];
    expect(summarizeTrace("t1", spans)).toEqual({
      traceId: "t1",
      rootName: "http.server GET",
      serviceNames: ["cf-slatedb-api", "cf-slatedb-blocks"],
      spanCount: 3,
      errorCount: 1,
      startUnixMs: 1,
      durationMs: 8,
      databaseIds: ["orders"],
    });
  });
});

describe("insertNewestTraceSummary", () => {
  it("keeps the newest `limit` summaries, newest first, for any arrival order", () => {
    const starts = [5, 1, 9, 3, 7, 7, 2, 8];
    const newest: Array<TraceSummary> = [];
    for (const [index, startUnixMs] of starts.entries()) {
      insertNewestTraceSummary(newest, { ...summarizeTrace(String(index), []), startUnixMs }, 3);
    }
    expect(newest.map((summary) => summary.startUnixMs)).toEqual([9, 8, 7]);
  });
});
