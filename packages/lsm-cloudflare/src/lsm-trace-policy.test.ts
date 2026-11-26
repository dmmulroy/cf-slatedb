import { ConfigProvider, Effect, Tracer } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { OtlpExporter, OtlpSerialization, OtlpTracer } from "effect/observability";
import { describe, expect, it } from "vite-plus/test";

import { isExportedSpanAttribute, lsmPolicyTracer, traceIdSampled } from "./lsm-trace-policy.ts";

/** A tracer that records every span it creates, with what was recorded on it. */
const recordingTracer = () => {
  const spans: Array<{ name: string; sampled: boolean; attributes: Map<string, unknown> }> = [];
  const tracer = Tracer.make({
    span(options) {
      const span = Tracer.nativeTracer.span(options);
      const attributes = new Map<string, unknown>();
      const record = span.attribute.bind(span);
      span.attribute = (key, value) => {
        attributes.set(key, value);
        record(key, value);
      };
      // The policy decides after creating the span: read the span's own flag.
      spans.push({
        name: options.name,
        get sampled() {
          return span.sampled;
        },
        attributes,
      });
      return span;
    },
  });
  return { tracer, spans };
};

describe("isExportedSpanAttribute (D63)", () => {
  it("exports project attributes and safe HTTP ones, and drops caller-identifying ones", () => {
    for (const key of [
      "database.id",
      "sst.id",
      "manifest.cas_attempts",
      "object.parts_unverified",
      "seq",
      "http.request.method",
      "http.response.status_code",
      "http.route",
      "url.path",
    ]) {
      expect([key, isExportedSpanAttribute(key)]).toEqual([key, true]);
    }
    for (const key of [
      "client.address",
      "user_agent.original",
      "url.full",
      "url.query",
      "http.request.header.authorization",
      "http.request.header.cookie",
      "http.response.header.set-cookie",
      "database",
      "key",
      "value",
      "enduser.id",
    ]) {
      expect([key, isExportedSpanAttribute(key)]).toEqual([key, false]);
    }
  });
});

describe("traceIdSampled (D63)", () => {
  it("keeps a trace when its id's low 32 bits fall below the rate", () => {
    const id = (low: string) => `${"a".repeat(24)}${low}`;
    expect(traceIdSampled(id("00000000"), 0.01)).toBe(true);
    expect(traceIdSampled(id("3fffffff"), 0.25)).toBe(true);
    expect(traceIdSampled(id("40000000"), 0.25)).toBe(false);
    expect(traceIdSampled(id("ffffffff"), 0.99)).toBe(false);
    expect(traceIdSampled(id("ffffffff"), 1)).toBe(true);
    expect(traceIdSampled(id("00000000"), 0)).toBe(false);
  });
});

describe("lsmPolicyTracer (D63)", () => {
  const traced = (sampleRate: number, requests: number) => {
    const { tracer, spans } = recordingTracer();
    const policy = lsmPolicyTracer(tracer, { sampleRate });
    const request = (index: number) =>
      Effect.gen(function* () {
        yield* Effect.annotateCurrentSpan({
          "database.id": "orders",
          "client.address": "203.0.113.7",
          "user_agent.original": "curl/8",
          "url.full": "https://api.example/v1/x?token=secret",
        });
        yield* Effect.void.pipe(Effect.withSpan(`child-${index}`));
      }).pipe(Effect.withSpan(`request-${index}`));
    Effect.runSync(
      Effect.forEach(
        Array.from({ length: requests }, (_, index) => index),
        request,
        { discard: true },
      ).pipe(Effect.withTracer(policy)),
    );
    return spans;
  };

  it("keeps every trace at rate 1 and records only exported attributes", () => {
    const spans = traced(1, 4);
    expect(spans.every((span) => span.sampled)).toBe(true);
    const root = spans.find((span) => span.name === "request-0");
    expect(root === undefined ? [] : [...root.attributes.keys()]).toEqual(["database.id"]);
  });

  it("keeps about the rate's fraction of traces, each kept or dropped whole", () => {
    const requests = 2000;
    const spans = traced(0.25, requests);
    const sampled = new Map(spans.map((span) => [span.name, span.sampled]));
    const kept = Array.from({ length: requests }, (_, index) => sampled.get(`request-${index}`));
    const fraction = kept.filter(Boolean).length / requests;
    expect(fraction).toBeGreaterThan(0.2);
    expect(fraction).toBeLessThan(0.3);
    // A child always shares its request's decision.
    expect(kept.every((decision, index) => sampled.get(`child-${index}`) === decision)).toBe(true);
  });

  it("decides a caller's trace by its id, whatever sampled flag the caller sent", () => {
    const { tracer, spans } = recordingTracer();
    const policy = lsmPolicyTracer(tracer, { sampleRate: 0.25 });
    const caller = (low: string, sampled: boolean) =>
      Tracer.externalSpan({ traceId: `${"a".repeat(24)}${low}`, spanId: "b".repeat(16), sampled });
    const serve = (name: string, parent: Tracer.ExternalSpan) =>
      Effect.void.pipe(Effect.withSpan(name), Effect.withSpan(`server-${name}`, { parent }));
    Effect.runSync(
      Effect.all([
        // The caller asked for its trace to be kept; its id says drop.
        serve("forced", caller("ffffffff", true)),
        // The caller did not keep its trace; its id says keep, as every other hop will.
        serve("unforced", caller("00000001", false)),
      ]).pipe(Effect.withTracer(policy)),
    );
    expect(spans.map((span) => [span.name, span.sampled])).toEqual([
      ["server-forced", false],
      ["forced", false],
      ["server-unforced", true],
      ["unforced", true],
    ]);
  });

  it("exports no dropped attribute through a real OTLP tracer", async () => {
    const bodies: Array<string> = [];
    const client = HttpClient.make((request) =>
      Effect.sync(() => {
        if (request.body._tag === "Uint8Array")
          bodies.push(new TextDecoder().decode(request.body.body));
        return HttpClientResponse.fromWeb(request, new Response("{}", { status: 200 }));
      }),
    );
    await Effect.gen(function* () {
      const otlp = yield* OtlpTracer.make({
        url: "http://collector.test/v1/traces",
        resource: { serviceName: "shaledb-test" },
      });
      const policy = lsmPolicyTracer(otlp, { sampleRate: 1 });
      yield* Effect.annotateCurrentSpan({
        "database.id": "orders",
        "client.address": "203.0.113.7",
        "url.query": "token=secret",
      }).pipe(Effect.withSpan("request"), Effect.withTracer(policy));
      yield* (yield* OtlpExporter.Flusher).flush;
    }).pipe(
      Effect.scoped,
      Effect.provide(OtlpSerialization.layerJson),
      Effect.provide(OtlpExporter.layerFlusher),
      Effect.provideService(HttpClient.HttpClient, client),
      // Nothing from the shell's OTEL_* environment.
      Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({})),
      Effect.runPromise,
    );
    const exported = bodies.join("\n");
    expect(exported).toContain("database.id");
    expect(exported).not.toContain("client.address");
    expect(exported).not.toContain("203.0.113.7");
    expect(exported).not.toContain("token=secret");
  });
});
