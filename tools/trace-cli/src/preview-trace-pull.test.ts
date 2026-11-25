import { NodeServices } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { Base64 } from "effect/encoding";
import { HttpClient, HttpClientResponse } from "effect/http";
import { OtlpSerialization, OtlpTracer } from "effect/observability";
import { describe, expect, it } from "vite-plus/test";

import { pullPreviewTraces } from "./preview-trace-pull.ts";
import { TraceStore, traceStoreTestLayer } from "./trace-store.ts";

const COLLECTOR_URL = "https://collector.example.workers.dev";
const TOKEN = "c".repeat(64);

/**
 * An in-memory trace collector with the deployed one's contract: OTLP exports go in through
 * `POST /v1/traces`; `GET /v1/traces/batches` pages them oldest first after a key, `pageSize` at a
 * time, and refuses a missing or wrong bearer token.
 */
const makeCollector = (pageSize: number) => {
  const exports: Array<{ key: string; contentType: string; body: Uint8Array }> = [];
  const client = HttpClient.make((request, url) =>
    Effect.sync(() => {
      const respond = (status: number, body: unknown) =>
        HttpClientResponse.fromWeb(request, new Response(JSON.stringify(body), { status }));
      if (request.method === "POST" && url.pathname === "/v1/traces") {
        if (request.body._tag !== "Uint8Array") return respond(400, {});
        exports.push({
          key: `traces/${String(exports.length).padStart(15, "0")}`,
          contentType: request.body.contentType,
          body: request.body.body,
        });
        return respond(200, { partialSuccess: {} });
      }
      if (request.headers["authorization"] !== `Bearer ${TOKEN}`) return respond(401, {});
      const after = url.searchParams.get("after");
      const pending = exports.filter((entry) => after === null || entry.key > after);
      const page = pending.slice(0, pageSize);
      return respond(200, {
        batches: page.map((entry) => ({
          key: entry.key,
          contentType: entry.contentType,
          body: Base64.encode(entry.body),
        })),
        next: page.at(-1)?.key ?? after,
        more: pending.length > pageSize,
      });
    }),
  );
  return { client, exports };
};

/** Exports one trace of `spanCount` spans to the collector in the given serialization. */
const exportSpans = (
  client: HttpClient.HttpClient,
  serialization: Layer.Layer<OtlpSerialization.OtlpSerialization>,
  spanCount: number,
) => {
  let program: Effect.Effect<void> = Effect.void;
  for (let index = 0; index < spanCount; index++) {
    program = program.pipe(Effect.withSpan(`Span.${index}`));
  }
  return program.pipe(
    Effect.provide(
      OtlpTracer.layer({
        url: `${COLLECTOR_URL}/v1/traces`,
        resource: { serviceName: "preview-test" },
      }).pipe(Layer.provide([serialization, Layer.succeed(HttpClient.HttpClient, client)])),
    ),
  );
};

describe("pullPreviewTraces", () => {
  it("stores every export once, in JSON and protobuf, across pages and repeated pulls", async () => {
    const collector = makeCollector(2);
    const result = await Effect.gen(function* () {
      yield* exportSpans(collector.client, OtlpSerialization.layerJson, 3);
      yield* exportSpans(collector.client, OtlpSerialization.layerProtobuf, 2);
      yield* exportSpans(collector.client, OtlpSerialization.layerJson, 1);
      const target = { stage: "preview-1", url: COLLECTOR_URL, token: TOKEN };
      const pull = pullPreviewTraces(target).pipe(
        Effect.provideService(HttpClient.HttpClient, collector.client),
      );
      const first = yield* pull;
      const again = yield* pull;
      yield* exportSpans(collector.client, OtlpSerialization.layerJson, 4);
      const later = yield* pull;
      const store = yield* TraceStore;
      const counts: Array<number> = [];
      for (const id of yield* store.listTraceIds) counts.push((yield* store.readTrace(id)).length);
      return { first, again, later, counts: counts.sort((left, right) => left - right) };
    }).pipe(Effect.provide([traceStoreTestLayer, NodeServices.layer]), Effect.runPromise);
    expect(collector.exports.length).toBe(4);
    expect(result.first).toEqual({ stage: "preview-1", exports: 3, spans: 6 });
    expect(result.again).toEqual({ stage: "preview-1", exports: 0, spans: 0 });
    expect(result.later).toEqual({ stage: "preview-1", exports: 1, spans: 4 });
    expect(result.counts).toEqual([1, 2, 3, 4]);
  });

  it("reports a refused token as CollectorUnauthorized", async () => {
    const collector = makeCollector(10);
    const failure = await pullPreviewTraces({
      stage: "preview-2",
      url: COLLECTOR_URL,
      token: "wrong",
    }).pipe(
      Effect.provideService(HttpClient.HttpClient, collector.client),
      Effect.provide([traceStoreTestLayer, NodeServices.layer]),
      Effect.flip,
      Effect.runPromise,
    );
    expect(failure._tag).toBe("PreviewTracePullError");
    expect(failure._tag === "PreviewTracePullError" ? failure.reason : "").toBe(
      "CollectorUnauthorized",
    );
  });
});
