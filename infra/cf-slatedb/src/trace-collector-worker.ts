import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  LSM_WORKER_COMPATIBILITY_DATE,
  lsmAccessGateFor,
  lsmHttpServerLayer,
} from "@cf-slatedb/lsm-cloudflare";
import * as Alchemy from "alchemy";
import type { HttpEffect } from "alchemy/Http";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, Option, Schedule, Schema } from "effect";
import { Base64 } from "effect/encoding";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";

/** Name of the secret binding that carries a stage's trace collector token. */
export const TRACE_COLLECTOR_TOKEN_BINDING = "CF_SLATEDB_TRACE_COLLECTOR_TOKEN";

/** Key prefix of every stored OTLP export in the trace bucket. */
export const TRACE_COLLECTOR_PREFIX = "traces/";

/** Most exports one `GET /v1/traces/batches` returns. */
const MAX_BATCHES_PER_PAGE = 100;

/**
 * The bearer secret a live stage's Workers send their spans with, and that `trace-cli pull` reads
 * them with: 256 random bits generated once per stage and kept in its Alchemy state. It is separate
 * from the stage's access token, so reading traces grants no access to databases.
 */
export const TraceCollectorToken = Alchemy.makeRandom("TraceCollectorToken");

/**
 * The R2 bucket holding a live stage's OTLP exports, one object per export under `traces/`, keyed
 * by arrival time. Only live stages have one; its name is generated and it is never retained.
 */
export const TraceBucket = Cloudflare.R2.Bucket("TraceBucket", {});

class TraceBucketClient extends Context.Service<
  TraceBucketClient,
  Cloudflare.R2.ReadWriteBucketClient
>()("@cf-slatedb/infra/TraceBucketClient") {}

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

const failure = (status: number, message: string) =>
  HttpServerResponse.jsonUnsafe({ message }, { status, headers: { "cache-control": "no-store" } });

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
    HttpServerError: () => Effect.succeed(failure(400, "The export body could not be read.")),
    R2Error: (error) =>
      Effect.succeed(
        failure(503, `The export could not be stored: ${error.message}. Send it again.`),
      ),
  }),
);

/**
 * `GET /v1/traces/batches?after=<key>&limit=<n>`: the stored exports after `after`, oldest first,
 * each with its content type and body (base64). `next` is the key to continue after, and `more`
 * says whether another page follows.
 */
const readBatches = Effect.gen(function* () {
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
      prefix: TRACE_COLLECTOR_PREFIX,
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
    { concurrency: 8 },
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
      Effect.succeed(failure(400, "`after` must be a key and `limit` a number from 1 to 100.")),
    R2Error: (error) =>
      Effect.succeed(
        failure(503, `The stored exports could not be read: ${error.message}. Retry the request.`),
      ),
  }),
);

/** Accepts OTLP logs and metrics and discards them: the collector keeps spans only. */
const acceptAndDiscard = Effect.succeed(HttpServerResponse.jsonUnsafe(OTLP_PARTIAL_SUCCESS));

const traceCollectorRoutesLayer = Layer.mergeAll(
  HttpRouter.add("POST", "/v1/traces", receiveTraces),
  HttpRouter.add("GET", "/v1/traces/batches", readBatches),
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
    );
    const router = yield* makeExecutionMemo(HttpRouter.toHttpEffect(traceCollectorRoutesLayer));
    return {
      fetch: withToken(
        Effect.flatten(router).pipe(Effect.provideService(TraceBucketClient, bucket)),
      ),
    };
  }).pipe(Effect.provide(traceBucketClientLayer)),
);

export default traceCollectorWorkerLayer;
