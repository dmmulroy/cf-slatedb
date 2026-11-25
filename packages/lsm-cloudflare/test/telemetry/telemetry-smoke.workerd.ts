import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import { expect } from "@effect/vitest";
import { Clock, Effect } from "effect";
import { HttpClient } from "effect/http";

import { useDelayedCollector, waitFor, workerUrl } from "./delayed-otlp-collector.ts";
import { telemetrySmokeStack } from "./telemetry-smoke-worker.ts";

const collector = { observation: useDelayedCollector(500) };

/** A local stage name, so the stage-selected flush policy is `BeforeResponse`. */

const { test } = Test.make({
  providers: Cloudflare.providers(),
  adopt: false,
  dev: true,
  stage: "test-telemetry-smoke",
});

test.provider(
  "handler spans are exported before the event settles, even when workerd stops right after",
  (stack) =>
    Effect.gen(function* () {
      collector.observation.spans.length = 0;
      const deployed = yield* stack.deploy(telemetrySmokeStack);
      const client = yield* HttpClient.HttpClient;
      const started = yield* Clock.currentTimeMillis;
      const response = yield* client.get(workerUrl(deployed.url));
      expect(yield* response.text).toBe("worker-saw:durable-object-ok");
      // `BeforeResponse`: the response waited for the 500 ms export.
      expect((yield* Clock.currentTimeMillis) - started).toBeGreaterThanOrEqual(500);

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
