import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Vitest";
import { expect } from "@effect/vitest";
import { Clock, Effect } from "effect";
import { HttpClient } from "effect/http";

import { useDelayedCollector, waitFor, workerUrl } from "./delayed-otlp-collector.ts";
import { telemetrySmokeStack } from "./telemetry-smoke-worker.ts";

/** How long the collector holds each export before answering. */
const COLLECTOR_DELAY_MS = 2_000;

const observation = useDelayedCollector(COLLECTOR_DELAY_MS);

/**
 * A live-style stage name run under local providers: the stage-selected flush policy is
 * `AfterResponse`, the policy a live stage gets.
 */
const { test } = Test.make({
  providers: Cloudflare.providers(),
  adopt: false,
  dev: true,
  stage: "preview-telemetry-smoke",
});

test.provider(
  "a live-stage policy adds no export latency, and the bridge still delivers every span",
  (stack) =>
    Effect.gen(function* () {
      observation.spans.length = 0;
      const deployed = yield* stack.deploy(telemetrySmokeStack);
      const client = yield* HttpClient.HttpClient;

      const started = yield* Clock.currentTimeMillis;
      const response = yield* client.get(workerUrl(deployed.url));
      expect(yield* response.text).toBe("worker-saw:durable-object-ok");
      const elapsed = (yield* Clock.currentTimeMillis) - started;
      // With `BeforeResponse` the Worker and the Durable Object would each wait for a 2 s export.
      expect(elapsed).toBeLessThan(COLLECTOR_DELAY_MS);

      // While workerd keeps running, the bridge's `waitUntil` flush delivers everything, server
      // spans included, with intact parentage.
      yield* waitFor(() => observation.spans.filter((span) => span.kind === 2).length >= 2);
      const byId = new Map(observation.spans.map((span) => [span.spanId, span]));
      const orphans = observation.spans.filter(
        (span) => span.parentSpanId !== undefined && !byId.has(span.parentSpanId),
      );
      expect(orphans.map((span) => [span.name, span.kind])).toEqual([["GET", 2]]);
      expect(observation.spans.some((span) => span.name === "TelemetrySmokeServer.fetch")).toBe(
        true,
      );
      yield* stack.destroy();
    }),
  { timeout: 120_000 },
);
