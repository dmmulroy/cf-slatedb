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
  "Worker and Durable Object fetch spans are exported before the event settles",
  (stack) =>
    Effect.gen(function* () {
      const deployed = yield* stack.deploy(telemetrySmokeStack);
      const client = yield* HttpClient.HttpClient;
      const response = yield* client.get(workerUrl(deployed.url));
      expect(yield* response.text).toBe("worker-saw:durable-object-ok");

      // Tear workerd down as soon as the sole event settles: a bridge that flushes only inside
      // `waitUntil` loses the delayed exports; one that flushes before settling does not.
      yield* stack.destroy();
      yield* waitFor(() => collector.observation.completedRequests >= 2);

      expect(collector.observation.completedRequests).toBe(2);
      const byId = new Map(collector.observation.spans.map((span) => [span.spanId, span]));
      // Every span's parent was collected, except the Worker's root server span, whose parent is
      // the test client's own span (propagated through `traceparent`). Effect names HTTP spans by
      // method; kind 2 is a server span and kind 3 a client span.
      const orphans = collector.observation.spans.filter(
        (span) => span.parentSpanId !== undefined && !byId.has(span.parentSpanId),
      );
      expect(orphans.map((span) => [span.name, span.kind])).toEqual([["GET", 2]]);
      const fetchSpan = collector.observation.spans.find(
        (span) => span.name === "TelemetrySmokeServer.fetch",
      );
      expect(fetchSpan).toBeDefined();
      const clientSpans = collector.observation.spans.filter((span) => span.kind === 3);
      expect(clientSpans.length).toBeGreaterThan(0);
      for (const clientSpan of clientSpans) {
        expect(
          collector.observation.spans.some(
            (span) => span.kind === 2 && span.parentSpanId === clientSpan.spanId,
          ),
        ).toBe(true);
      }
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
