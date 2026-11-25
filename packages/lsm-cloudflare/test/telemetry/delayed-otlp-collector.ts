import { createServer, type Server } from "node:http";

import { expect } from "@effect/vitest";
import { Effect, Schedule, Schema } from "effect";
import { afterAll, beforeAll } from "vite-plus/test";

import { TELEMETRY_SMOKE_COLLECTOR_PORT } from "./telemetry-smoke-worker.ts";

/** Span topology the delayed collector keeps: no attributes, payloads, or headers. */
export interface CollectedSpanTopology {
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
export const startDelayedCollector = (delayMs: number) => {
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

/**
 * Starts a delayed collector for the current test file (on the smoke test's port unless `port` is
 * given) and stops it afterwards.
 */
export const useDelayedCollector = (
  delayMs: number,
  port: number = TELEMETRY_SMOKE_COLLECTOR_PORT,
) => {
  const collector = startDelayedCollector(delayMs);
  beforeAll(
    () =>
      new Promise<void>((resolve, reject) => {
        collector.server.once("error", reject);
        collector.server.listen(port, "127.0.0.1", resolve);
      }),
  );
  afterAll(() => new Promise<void>((resolve) => collector.server.close(() => resolve())));
  return collector.observation;
};

/** The local Worker URL; a local stack always has one. */
export const workerUrl = (url: string | undefined): string => {
  expect(url).toBeDefined();
  return url ?? "";
};

/** Polls `predicate` every 100 ms for up to five seconds. */
export const waitFor = (predicate: () => boolean) =>
  Effect.sync(predicate).pipe(
    Effect.repeat({ schedule: Schedule.spaced("100 millis"), until: (done) => done, times: 50 }),
  );
