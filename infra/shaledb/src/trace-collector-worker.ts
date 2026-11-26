import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  LSM_WORKER_COMPATIBILITY_DATE,
  lsmAccessGateFor,
  bindLsmWorkerSettings,
  lsmHttpServerLayer,
  lsmJurisdiction,
} from "@shaledb/lsm-cloudflare";
import * as Alchemy from "alchemy";
import type { HttpEffect } from "alchemy/Http";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, Option, Schedule, Schema } from "effect";
import { Base64 } from "effect/encoding";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";

import { SHALEDB_PRODUCTION_STAGE } from "./shaledb-stage.ts";
import { shaledbTraceRetentionDays } from "./shaledb-stack-config.ts";
import { shaledbWorkerSettings } from "./shaledb-stack-config.ts";

/** Name of the secret binding that carries a stage's trace collector token. */
export const TRACE_COLLECTOR_TOKEN_BINDING = "SHALEDB_TRACE_COLLECTOR_TOKEN";

/** Key prefix of every stored OTLP export in the trace bucket. */
export const TRACE_COLLECTOR_PREFIX = "traces/";

/** Key prefix of the Tail Worker's invocation records in the trace bucket (`cpu-tail-worker.ts`). */
export const TAIL_RECORDS_PREFIX = "tails/";

/**
 * Most exports one `GET /v1/traces/batches` returns. A busy preview at 100% sampling stores about 40
 * exports a second; pages of 500 read 50 at a time keep a pull ahead of that, within the R2
 * binding's limit of about 1,000 operations per invocation (one list and 500 reads).
 */
const MAX_BATCHES_PER_PAGE = 500;

/**
 * The bearer secret a live stage's Workers send their spans with, and that `trace-cli pull` reads
 * them with: 256 random bits generated once per stage and kept in its Alchemy state. It is separate
 * from the stage's access token, so reading traces grants no access to databases.
 */
export const TraceCollectorToken = Alchemy.makeRandom("TraceCollectorToken");

/**
 * The R2 bucket holding a live stage's OTLP exports, one object per export under `traces/`, keyed
 * by arrival time. Only live stages have one; its name is generated, it is never retained, and it
 * is created in the Stack's jurisdiction like the LSM bucket.
 */
export const TraceBucket = Cloudflare.R2.Bucket(
  "TraceBucket",
  Effect.gen(function* () {
    const stage = yield* Alchemy.Stage;
    const jurisdiction = yield* lsmJurisdiction;
    const retentionSeconds = (yield* shaledbTraceRetentionDays) * 24 * 60 * 60;
    return {
      ...(Option.isSome(jurisdiction) ? { jurisdiction: jurisdiction.value } : {}),
      // Traces and tail records expire after the configured days (P4, D63).
      lifecycleRules: [TRACE_COLLECTOR_PREFIX, TAIL_RECORDS_PREFIX].map((prefix) => ({
        id: `expire-${prefix.replace("/", "")}`,
        prefix,
        deleteObjectsTransition: { condition: { type: "Age" as const, maxAge: retentionSeconds } },
      })),
      // Traces are disposable. Outside production, replacing the bucket (a jurisdiction change) or
      // destroying the stage empties it first instead of failing on its contents.
      forceDestroy: stage !== SHALEDB_PRODUCTION_STAGE,
    };
  }),
);

class TraceBucketClient extends Context.Service<
  TraceBucketClient,
  Cloudflare.R2.ReadWriteBucketClient
>()("@shaledb/infra/TraceBucketClient") {}

const traceBucketClientLayer = Layer.effect(
  TraceBucketClient,
  Effect.gen(function* () {
    return yield* Cloudflare.R2.ReadWriteBucket(yield* TraceBucket);
  }),
).pipe(Layer.provide(Cloudflare.R2.ReadWriteBucketBinding));

/** R2 calls retry briefly: an export is safe to store twice, and a read is safe to repeat. */
const r2Retry = Schedule.max([Schedule.exponential("50 millis"), Schedule.recurs(3)]);

const OTLP_PARTIAL_SUCCESS = { partialSuccess: {} };

/** Sortable key for an export that arrived at `receivedAtMs`, unique per request. */
const exportKey = (receivedAtMs: number, extension: "json" | "pb") =>
  `${TRACE_COLLECTOR_PREFIX}${receivedAtMs.toString().padStart(15, "0")}-${crypto.randomUUID()}.${extension}`;

const BatchesQuery = Schema.Struct({
  after: Schema.optionalKey(Schema.String),
  limit: Schema.optionalKey(Schema.NumberFromString),
});
const decodeBatchesQuery = Schema.decodeUnknownEffect(BatchesQuery);

/**
 * The collector's error body: the public error envelope (code, operation, message, details, request
 * id, retryability), without vendor messages, which are logged instead.
 */
interface TraceCollectorPublicError {
  readonly code: "InvalidRequest" | "TraceStorageUnavailable";
  readonly operation: "storeTraces" | "readTraces";
  readonly message: string;
  readonly details: Record<string, never>;
  readonly requestId: string;
  readonly retryable: boolean;
}

const failure = (
  status: 400 | 503,
  error: Omit<TraceCollectorPublicError, "requestId" | "details" | "retryable">,
) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const requestId = request.headers["cf-ray"] ?? crypto.randomUUID();
    const body: TraceCollectorPublicError = {
      ...error,
      details: {},
      requestId,
      retryable: status === 503,
    };
    return HttpServerResponse.jsonUnsafe(body, {
      status,
      headers: { "cache-control": "no-store", "x-request-id": requestId },
    });
  });

/** `POST /v1/traces`: stores the OTLP/HTTP export (JSON or protobuf) as received. */
const receiveTraces = Effect.gen(function* () {
  const bucket = yield* TraceBucketClient;
  const request = yield* HttpServerRequest.HttpServerRequest;
  const contentType = request.headers["content-type"] ?? "application/json";
  const body = new Uint8Array(yield* request.arrayBuffer);
  const key = exportKey(Date.now(), contentType.includes("protobuf") ? "pb" : "json");
  yield* bucket.put(key, body, { httpMetadata: { contentType } }).pipe(Effect.retry(r2Retry));
  return HttpServerResponse.jsonUnsafe(OTLP_PARTIAL_SUCCESS);
}).pipe(
  Effect.catchTags({
    HttpServerError: () =>
      failure(400, {
        code: "InvalidRequest",
        operation: "storeTraces",
        message:
          "The trace collector could not read the export's body. Send a complete OTLP export.",
      }),
    R2Error: (error) =>
      Effect.logError("The trace collector could not store an export", { cause: error }).pipe(
        Effect.andThen(
          failure(503, {
            code: "TraceStorageUnavailable",
            operation: "storeTraces",
            message:
              "The trace collector could not store the export because trace storage was unavailable. Send the same export again.",
          }),
        ),
      ),
  }),
);

/**
 * `GET /v1/traces/batches?after=<key>&limit=<n>` (and `/v1/tails/batches` for invocation records):
 * the stored objects under `prefix` after `after`, oldest first, each with its content type and
 * body (base64). `next` is the key to continue after, and `more` says whether another page follows.
 */
const readBatchesUnder = (prefix: string) =>
  Effect.gen(function* () {
    const bucket = yield* TraceBucketClient;
    const request = yield* HttpServerRequest.HttpServerRequest;
    const query = yield* decodeBatchesQuery(
      Object.fromEntries(new URL(request.originalUrl, "http://collector.internal").searchParams),
    );
    const limit = Math.min(
      Math.max(1, Math.floor(query.limit ?? MAX_BATCHES_PER_PAGE)),
      MAX_BATCHES_PER_PAGE,
    );
    const page = yield* bucket
      .list({
        prefix,
        limit,
        ...(query.after === undefined ? {} : { startAfter: query.after }),
      })
      .pipe(Effect.retry(r2Retry));
    const batches = yield* Effect.forEach(
      page.objects,
      (object) =>
        Effect.gen(function* () {
          const stored = yield* bucket.get(object.key).pipe(Effect.retry(r2Retry));
          if (stored === null) return Option.none();
          const bytes = yield* stored.bytes();
          return Option.some({
            key: object.key,
            contentType: stored.httpMetadata?.contentType ?? "application/json",
            body: Base64.encode(bytes),
          });
        }),
      { concurrency: 50 },
    );
    const found = batches.flatMap((batch) => (Option.isSome(batch) ? [batch.value] : []));
    return HttpServerResponse.jsonUnsafe(
      {
        batches: found,
        next: page.objects.at(-1)?.key ?? query.after ?? null,
        more: page.truncated,
      },
      { headers: { "cache-control": "no-store" } },
    );
  }).pipe(
    Effect.catchTags({
      SchemaError: () =>
        failure(400, {
          code: "InvalidRequest",
          operation: "readTraces",
          message: `The trace collector could not read the query: \`after\` must be a stored key and \`limit\` a number (pages hold at most ${MAX_BATCHES_PER_PAGE} exports). Correct it and request again.`,
        }),
      R2Error: (error) =>
        Effect.logError("The trace collector could not read stored exports", { cause: error }).pipe(
          Effect.andThen(
            failure(503, {
              code: "TraceStorageUnavailable",
              operation: "readTraces",
              message:
                "The trace collector could not read stored exports because trace storage was unavailable. Request the same page again.",
            }),
          ),
        ),
    }),
  );

/** Accepts OTLP logs and metrics and discards them: the collector keeps spans only. */
const acceptAndDiscard = Effect.succeed(HttpServerResponse.jsonUnsafe(OTLP_PARTIAL_SUCCESS));

const traceCollectorRoutesLayer = Layer.mergeAll(
  HttpRouter.add("POST", "/v1/traces", receiveTraces),
  HttpRouter.add("GET", "/v1/traces/batches", readBatchesUnder(TRACE_COLLECTOR_PREFIX)),
  HttpRouter.add("GET", "/v1/tails/batches", readBatchesUnder(TAIL_RECORDS_PREFIX)),
  HttpRouter.add("POST", "/v1/logs", acceptAndDiscard),
  HttpRouter.add("POST", "/v1/metrics", acceptAndDiscard),
).pipe(Layer.provide(lsmHttpServerLayer));

/**
 * A live stage's OTLP/HTTP receiver: every Worker and Durable Object in the stage exports its spans
 * here, and `trace-cli pull --stage <stage>` downloads them into the local trace store. Every
 * request needs the stage's {@link TraceCollectorToken} as a bearer token. It exports no telemetry
 * of its own.
 */
export class TraceCollectorWorker extends Cloudflare.Worker<
  TraceCollectorWorker,
  { readonly fetch: HttpEffect<RuntimeContext> }
>()("TraceCollectorWorker") {}

/** Worker Layer for {@link TraceCollectorWorker}; this module is its bundle entry. */
export const traceCollectorWorkerLayer = TraceCollectorWorker.make(
  {
    main: import.meta.url,
    compatibility: { date: LSM_WORKER_COMPATIBILITY_DATE },
  },
  Effect.gen(function* () {
    const bucket = yield* TraceBucketClient;
    const withToken = yield* lsmAccessGateFor(
      TRACE_COLLECTOR_TOKEN_BINDING,
      Effect.map(TraceCollectorToken, Option.some),
      "the stage's trace collector token (docs/operations.md, \"Reading a stage's telemetry\")",
    );
    const router = yield* makeExecutionMemo(HttpRouter.toHttpEffect(traceCollectorRoutesLayer));
    // Its bucket is in the Stack's jurisdiction, read through the same binding as the engine's.
    yield* bindLsmWorkerSettings(shaledbWorkerSettings);
    return {
      fetch: withToken(
        Effect.flatten(router).pipe(Effect.provideService(TraceBucketClient, bucket)),
      ),
    };
  }).pipe(Effect.provide(traceBucketClientLayer)),
);

export default traceCollectorWorkerLayer;
