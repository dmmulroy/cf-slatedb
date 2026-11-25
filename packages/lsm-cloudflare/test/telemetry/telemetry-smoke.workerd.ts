import { createServer, type Server } from "node:http";

import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import { expect } from "@effect/vitest";
import { Effect, Schedule, Schema } from "effect";
import { HttpClient } from "effect/http";
import { afterAll, beforeAll } from "vite-plus/test";

import { TELEMETRY_SMOKE_COLLECTOR_PORT, telemetrySmokeStack } from "./telemetry-smoke-worker.ts";

/** Span topology the delayed collector keeps: no attributes, payloads, or headers. */
interface CollectedSpanTopology {
  readonly name: string;
  /** OTLP span kind: 2 is a server span, 3 a client span. */
  readonly kind: number;
  readonly traceId: string;
  readonly spanId: string;
  readonly parentSpanId?: string | undefined;
}

const OtlpTopology = Schema.fromJsonString(
  Schema.Struct({
    resourceSpans: Schema.Array(
      Schema.Struct({
        scopeSpans: Schema.Array(
          Schema.Struct({
            spans: Schema.Array(
              Schema.Struct({
                name: Schema.String,
                kind: Schema.Number,
                traceId: Schema.String,
                spanId: Schema.String,
                parentSpanId: Schema.optional(Schema.String),
              }),
            ),
          }),
        ),
      }),
    ),
  }),
);
const decodeOtlpTopology = Schema.decodeUnknownSync(OtlpTopology);

/**
 * Collector that answers each export only after `delayMs` and records it once the response has
 * been sent, so an export cut off by a torn-down runtime is never counted.
 */
const startDelayedCollector = (delayMs: number) => {
  const observation = { completedRequests: 0, spans: new Array<CollectedSpanTopology>() };
  const server: Server = createServer((request, response) => {
    const chunks: Array<Buffer> = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => {
      const spans = decodeOtlpTopology(
        Buffer.concat(chunks).toString("utf8"),
      ).resourceSpans.flatMap((resource) => resource.scopeSpans.flatMap((scope) => scope.spans));
      setTimeout(() => {
        response.once("finish", () => {
          observation.completedRequests += 1;
          observation.spans.push(...spans);
        });
        response.writeHead(200, { "content-type": "application/json" });
        response.end('{"partialSuccess":{}}');
      }, delayMs);
    });
  });
  return { server, observation };
};

const collector = startDelayedCollector(500);

beforeAll(
  () =>
    new Promise<void>((resolve, reject) => {
      collector.server.once("error", reject);
      collector.server.listen(TELEMETRY_SMOKE_COLLECTOR_PORT, "127.0.0.1", resolve);
    }),
);
afterAll(() => new Promise<void>((resolve) => collector.server.close(() => resolve())));

const { test } = Test.make({
  providers: Cloudflare.providers(),
  adopt: false,
  dev: true,
  stage: "telemetry-smoke",
});

/** The local Worker URL; a local stack always has one. */
const workerUrl = (url: string | undefined): string => {
  expect(url).toBeDefined();
  return url ?? "";
};

const waitFor = (predicate: () => boolean) =>
  Effect.sync(predicate).pipe(
    Effect.repeat({ schedule: Schedule.spaced("100 millis"), until: (done) => done, times: 50 }),
  );

test.provider(
  "handler spans are exported before the event settles, even when workerd stops right after",
  (stack) =>
    Effect.gen(function* () {
      collector.observation.spans.length = 0;
      const deployed = yield* stack.deploy(telemetrySmokeStack);
      const client = yield* HttpClient.HttpClient;
      const response = yield* client.get(workerUrl(deployed.url));
      expect(yield* response.text).toBe("worker-saw:durable-object-ok");

      // Tear workerd down as soon as the sole event settles. Alchemy's bridge exports only inside
      // `waitUntil` after the event settles, and the collector answers after 500 ms, so only what
      // `withEventTelemetryFlush` exported before the response survives.
      yield* stack.destroy();
      yield* waitFor(() => collector.observation.completedRequests >= 2);

      const spans = collector.observation.spans;
      // Effect names HTTP spans by method; kind 2 is a server span and kind 3 a client span.
      const fetchSpan = spans.find((span) => span.name === "TelemetrySmokeServer.fetch");
      const clientSpan = spans.find((span) => span.kind === 3);
      expect(fetchSpan).toBeDefined();
      expect(clientSpan).toBeDefined();
      // The Durable Object's span continues the Worker's trace through the `traceparent` header.
      expect(fetchSpan?.traceId).toBe(clientSpan?.traceId);
    }),
  { timeout: 120_000 },
);

test.provider(
  "with workerd running, Worker and Durable Object spans arrive with intact parentage",
  (stack) =>
    Effect.gen(function* () {
      collector.observation.spans.length = 0;
      const deployed = yield* stack.deploy(telemetrySmokeStack);
      const client = yield* HttpClient.HttpClient;
      const response = yield* client.get(workerUrl(deployed.url));
      expect(yield* response.text).toBe("worker-saw:durable-object-ok");
      // The bridge-owned server spans arrive through its own `waitUntil` flush.
      yield* waitFor(
        () => collector.observation.spans.filter((span) => span.kind === 2).length >= 2,
      );

      const spans = collector.observation.spans;
      const byId = new Map(spans.map((span) => [span.spanId, span]));
      // Every parent was collected, except the Worker's root server span, whose parent is the test
      // client's own span.
      const orphans = spans.filter(
        (span) => span.parentSpanId !== undefined && !byId.has(span.parentSpanId),
      );
      expect(orphans.map((span) => [span.name, span.kind])).toEqual([["GET", 2]]);
      for (const clientSpan of spans.filter((span) => span.kind === 3)) {
        expect(
          spans.some((span) => span.kind === 2 && span.parentSpanId === clientSpan.spanId),
        ).toBe(true);
      }
      yield* stack.destroy();
    }),
  { timeout: 120_000 },
);

test.provider(
  "Durable Object alarm spans are exported",
  (stack) =>
    Effect.gen(function* () {
      collector.observation.spans.length = 0;
      const deployed = yield* stack.deploy(telemetrySmokeStack);
      const client = yield* HttpClient.HttpClient;
      const response = yield* client.get(`${workerUrl(deployed.url)}/alarm`);
      expect(yield* response.text).toBe("worker-saw:durable-object-ok");
      yield* waitFor(() =>
        collector.observation.spans.some((span) => span.name === "TelemetrySmokeServer.alarm"),
      );
      expect(
        collector.observation.spans.some((span) => span.name === "TelemetrySmokeServer.alarm"),
      ).toBe(true);
      yield* stack.destroy();
    }),
  { timeout: 120_000 },
);
