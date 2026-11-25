import { NodeHttpClient } from "@effect/platform-node";
import { Effect, Layer, Option } from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";
import { OtlpSerialization, OtlpTracer } from "effect/observability";
import { describe, expect, it } from "vite-plus/test";

import { otlpReceiverLayer } from "./otlp-receiver.ts";
import { renderSpanTree } from "./trace-analysis.ts";
import { evaluateTraceAssertion } from "./trace-assertion.ts";
import { TraceStore, traceStoreTestLayer } from "./trace-store.ts";

const randomPort = () => 40_000 + Math.floor(Math.random() * 20_000);

/** Spans an Effect program exports: a parent with a child carrying `database.id`, and a failure. */
const tracedProgram = Effect.gen(function* () {
  yield* Effect.void.pipe(
    Effect.withSpan("ManifestStore.create", {
      attributes: { "database.id": "db-1", "manifest.id": 7 },
    }),
    Effect.withSpan("WriterServer.flush", { attributes: { "database.id": "db-1" } }),
  );
  yield* Effect.fail("boom").pipe(Effect.withSpan("ObjectStore.put"), Effect.ignore);
}).pipe(Effect.withSpan("http.server POST"));

/** Exports `tracedProgram` to a receiver on `port` with the given serialization, then flushes. */
const exportTraces = (
  port: number,
  serialization: Layer.Layer<OtlpSerialization.OtlpSerialization>,
) =>
  tracedProgram.pipe(
    Effect.provide(
      OtlpTracer.layer({
        url: `http://127.0.0.1:${port}/v1/traces`,
        resource: { serviceName: "trace-cli-test" },
      }).pipe(Layer.provide([serialization, NodeHttpClient.layerUndici])),
    ),
  );

const collect = (serialization: Layer.Layer<OtlpSerialization.OtlpSerialization>) => {
  const port = randomPort();
  return Effect.gen(function* () {
    yield* Layer.build(otlpReceiverLayer({ host: "127.0.0.1", port }));
    yield* exportTraces(port, serialization);
    const store = yield* TraceStore;
    const [traceId] = yield* store.listTraceIds;
    expect(traceId).toBeDefined();
    return yield* store.readTrace(traceId ?? "");
  }).pipe(Effect.scoped, Effect.provide(traceStoreTestLayer), Effect.runPromise);
};

describe("OTLP receiver", () => {
  for (const [label, serialization] of [
    ["JSON", OtlpSerialization.layerJson],
    ["protobuf", OtlpSerialization.layerProtobuf],
  ] as const) {
    it(`stores spans exported as OTLP/${label} with parents, attributes, and error status`, async () => {
      const spans = await collect(serialization);
      const byName = new Map(spans.map((span) => [span.name, span]));
      const parent = byName.get("WriterServer.flush");
      const child = byName.get("ManifestStore.create");
      const failing = byName.get("ObjectStore.put");

      expect(spans).toHaveLength(4);
      expect(child?.parentSpanId).toEqual(Option.some(parent?.spanId));
      expect(child?.attributes["database.id"]).toBe("db-1");
      expect(child?.attributes["manifest.id"]).toBe(7);
      expect(child?.serviceName).toBe("trace-cli-test");
      expect(failing?.status.code).toBe("Error");
      expect(parent?.status.code).toBe("Unset");
      expect(renderSpanTree(spans)).toMatch(
        /http\.server POST .*\n {2}WriterServer\.flush .*\n {4}ManifestStore\.create/,
      );

      const report = evaluateTraceAssertion(
        {
          select: { databaseId: "db-1" },
          checks: [
            {
              _tag: "SpanExists",
              span: { name: "ManifestStore.create" },
              parent: { name: "WriterServer.flush" },
              count: { equals: 1 },
            },
            { _tag: "NoSpan", span: { name: "ObjectStore.list" } },
            { _tag: "NoErrorStatus", except: ["ObjectStore.put"] },
          ],
        },
        new Map([[spans[0]?.traceId ?? "", spans]]),
      );
      expect(report.passed).toBe(true);
    });
  }

  it("answers 400 to a body that is not an OTLP trace export", async () => {
    const port = randomPort();
    const status = await Effect.gen(function* () {
      yield* Layer.build(otlpReceiverLayer({ host: "127.0.0.1", port }));
      const client = yield* HttpClient.HttpClient;
      const response = yield* client.execute(
        HttpClientRequest.post(`http://127.0.0.1:${port}/v1/traces`).pipe(
          HttpClientRequest.bodyText('{"resourceSpans": "nope"}', "application/json"),
        ),
      );
      return response.status;
    }).pipe(
      Effect.scoped,
      Effect.provide([traceStoreTestLayer, NodeHttpClient.layerUndici]),
      Effect.runPromise,
    );
    expect(status).toBe(400);
  });
});
