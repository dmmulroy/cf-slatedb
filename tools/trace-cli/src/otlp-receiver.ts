import { createServer } from "node:http";

import { NodeHttpServer } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";

import { decodeOtlpJsonTraces, decodeOtlpProtobufTraces } from "./otlp-trace-decoder.ts";
import { TraceStore } from "./trace-store.ts";

/** Default OTLP/HTTP receiver address, the standard OTLP/HTTP port on the loopback interface. */
export const DEFAULT_OTLP_RECEIVER_HOST = "127.0.0.1";

/** Default OTLP/HTTP receiver port. */
export const DEFAULT_OTLP_RECEIVER_PORT = 4318;

const PARTIAL_SUCCESS = { partialSuccess: {} };

/**
 * Handles `POST /v1/traces`: decodes an OTLP/JSON or OTLP/protobuf export and stores its spans.
 *
 * @returns `200 {"partialSuccess":{}}`, or `400` with the decode error when the body is not an OTLP
 *   trace export, or `500` when the store cannot be written.
 */
const receiveTraces = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const store = yield* TraceStore;
  const contentType = request.headers["content-type"] ?? "";
  const spans = contentType.includes("protobuf")
    ? yield* request.arrayBuffer.pipe(
        Effect.orDie,
        Effect.flatMap((body) => decodeOtlpProtobufTraces(new Uint8Array(body))),
      )
    : yield* request.text.pipe(Effect.orDie, Effect.flatMap(decodeOtlpJsonTraces));
  yield* store.append(spans);
  return HttpServerResponse.jsonUnsafe(PARTIAL_SUCCESS);
}).pipe(
  Effect.catchTags({
    OtlpTraceDecodeError: (error) =>
      Effect.succeed(HttpServerResponse.jsonUnsafe({ message: error.message }, { status: 400 })),
    TraceStoreError: (error) =>
      Effect.succeed(HttpServerResponse.jsonUnsafe({ message: error.message }, { status: 500 })),
  }),
);

/** Accepts OTLP logs and metrics exports and discards them; the trace store keeps spans only. */
const acceptAndDiscard = Effect.succeed(HttpServerResponse.jsonUnsafe(PARTIAL_SUCCESS));

/**
 * Layer of the receiver's routes: `POST /v1/traces` (stored), `POST /v1/logs` and
 * `POST /v1/metrics` (accepted and discarded), and `GET /v1/health`.
 */
export const otlpReceiverRoutesLayer = Layer.mergeAll(
  HttpRouter.add("POST", "/v1/traces", receiveTraces),
  HttpRouter.add("POST", "/v1/logs", acceptAndDiscard),
  HttpRouter.add("POST", "/v1/metrics", acceptAndDiscard),
  HttpRouter.add("GET", "/v1/health", Effect.succeed(HttpServerResponse.jsonUnsafe({ ok: true }))),
);

/**
 * Serves the OTLP/HTTP receiver on `host:port` until the Layer's scope closes, storing spans in the
 * contextual {@link TraceStore}.
 */
export const otlpReceiverLayer = (options: { readonly host: string; readonly port: number }) =>
  HttpRouter.serve(otlpReceiverRoutesLayer, { disableLogger: true, disableListenLog: true }).pipe(
    Layer.provide(NodeHttpServer.layer(createServer, { host: options.host, port: options.port })),
  );
