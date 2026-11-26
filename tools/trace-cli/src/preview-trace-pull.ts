import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { Data, Effect, Fiber, FileSystem, Option, Path, Schema } from "effect";
import { Base64 } from "effect/encoding";
import { HttpClient, HttpClientRequest } from "effect/http";

import { decodeOtlpJsonTraces, decodeOtlpProtobufTraces } from "./otlp-trace-decoder.ts";
import { TraceStore } from "./trace-store.ts";

/**
 * Downloading a preview stage's traces failed.
 *
 * `reason` is `CollectorUnknown` when the preview CLI could not name the stage's collector (nothing
 * deployed, or no collector yet), `CollectorUnavailable` when the collector could not be reached or
 * answered with an error, `CollectorUnauthorized` when it refused the token, and `ExportInvalid` when
 * a stored export is not an OTLP trace export.
 */
export class PreviewTracePullError extends Data.TaggedError("PreviewTracePullError")<{
  readonly reason:
    | "CollectorUnknown"
    | "CollectorUnavailable"
    | "CollectorUnauthorized"
    | "ExportInvalid";
  readonly stage: string;
  readonly message: string;
}> {}

/** Schema for a preview stage's trace collector, as `preview-cli collector <stage> --json` prints it. */
const PreviewCollector = Schema.Struct({
  stage: Schema.String,
  url: Schema.String,
  token: Schema.String,
});

/** A preview stage's trace collector: its URL and bearer token. */
export type PreviewCollector = typeof PreviewCollector.Type;

const decodePreviewCollector = Schema.decodeUnknownEffect(Schema.fromJsonString(PreviewCollector));

const PREVIEW_CLI = fileURLToPath(new URL("../../preview-cli/src/preview-cli.ts", import.meta.url));

/**
 * Asks the preview CLI (in its own process; it reads the Cloudflare state store) for a stage's
 * collector URL and token.
 *
 * @returns The collector, or `PreviewTracePullError` (`CollectorUnknown`) when the stage has no
 *   deployment or no collector yet; the preview CLI's own message is included.
 */
export const resolvePreviewCollector = (stage: string) =>
  Effect.gen(function* () {
    const result = yield* Effect.sync(() =>
      spawnSync(process.execPath, [PREVIEW_CLI, "collector", stage, "--json"], {
        encoding: "utf8",
      }),
    );
    if (result.status !== 0) {
      return yield* new PreviewTracePullError({
        reason: "CollectorUnknown",
        stage,
        message: `The trace collector of ${stage} could not be found; deploy the stage with \`preview-cli deploy ${stage}\` and pull again:\n${`${result.stdout}${result.stderr}`.trim()}`,
      });
    }
    return yield* decodePreviewCollector(result.stdout.trim()).pipe(
      Effect.mapError(
        () =>
          new PreviewTracePullError({
            reason: "CollectorUnknown",
            stage,
            message: `The preview CLI printed something other than ${stage}'s collector; run \`preview-cli collector ${stage} --json\` to see why: ${result.stdout.trim()}`,
          }),
      ),
    );
  });

/** Schema for one page of `GET /v1/traces/batches` from the collector. */
const CollectorPage = Schema.Struct({
  batches: Schema.Array(
    Schema.Struct({ key: Schema.String, contentType: Schema.String, body: Schema.String }),
  ),
  next: Schema.NullOr(Schema.String),
  more: Schema.Boolean,
});
const decodeCollectorPage = Schema.decodeUnknownEffect(CollectorPage);

/** Schema for a stage's pull cursor: where the last pull from that collector stopped. */
const PullCursor = Schema.Struct({ url: Schema.String, after: Schema.String });
const decodePullCursor = Schema.decodeUnknownEffect(Schema.fromJsonString(PullCursor));
const encodePullCursor = Schema.encodeEffect(Schema.fromJsonString(PullCursor));

/** What one pull downloaded. */
export interface PreviewTracePull {
  readonly stage: string;
  /** OTLP exports downloaded. */
  readonly exports: number;
  /** Spans stored from them. */
  readonly spans: number;
}

/**
 * Downloads every export the stage's collector stored since the last pull into the trace store, so
 * `list`, `show`, `grep`, `stats`, `diff`, and `assert` see the stage's spans. The cursor is kept in
 * `<store>/pulls/<stage>.json`, per collector URL (a recreated collector starts from the beginning),
 * and advances after each export is stored, so a failed pull resumes where it stopped.
 *
 * @returns What was downloaded, or `PreviewTracePullError`: `CollectorUnauthorized` (the token was
 *   refused), `CollectorUnavailable` (unreachable or an error status; pull again), or `ExportInvalid`
 *   (a stored export is not OTLP traces); or `TraceStoreError` when the local store cannot be written.
 */
/** Prefix of a collector's export keys, which then start with the arrival time in ms. */
const TRACE_EXPORTS_PREFIX = "traces/";

export const pullPreviewTraces = (
  collector: PreviewCollector,
  /** Skip exports that arrived before this Unix millisecond time (keys start with it). */
  sinceMs: Option.Option<number> = Option.none(),
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const store = yield* TraceStore;
    const client = yield* HttpClient.HttpClient;
    const { stage } = collector;
    const cursorFile = path.join(store.directory, "pulls", `${stage}.json`);
    const saved = yield* fs.readFileString(cursorFile).pipe(
      Effect.flatMap(decodePullCursor),
      Effect.map(Option.some),
      Effect.orElseSucceed(() => Option.none<typeof PullCursor.Type>()),
    );
    const resumed = Option.filter(saved, (cursor) => cursor.url === collector.url).pipe(
      Option.map((cursor) => cursor.after),
    );
    const start = Option.map(
      sinceMs,
      (ms) => `${TRACE_EXPORTS_PREFIX}${(ms - 1).toString().padStart(15, "0")}`,
    );
    // Whichever is later: the saved position, or the requested start.
    let after =
      Option.isSome(resumed) && Option.isSome(start)
        ? Option.some(resumed.value > start.value ? resumed.value : start.value)
        : Option.orElse(resumed, () => start);
    let exports = 0;
    let spans = 0;
    const saveCursor = (key: string) =>
      Effect.gen(function* () {
        yield* fs.makeDirectory(path.dirname(cursorFile), { recursive: true });
        yield* fs.writeFileString(
          cursorFile,
          yield* encodePullCursor({ url: collector.url, after: key }),
        );
      });
    const unavailable = (message: string) =>
      new PreviewTracePullError({ reason: "CollectorUnavailable", stage, message });
    /** Fetches and decodes the page of exports after `from`. */
    const fetchPage = (from: Option.Option<string>) =>
      Effect.gen(function* () {
        const url = new URL("/v1/traces/batches", collector.url);
        url.searchParams.set("limit", "100");
        if (Option.isSome(from)) url.searchParams.set("after", from.value);
        const response = yield* client
          .execute(
            HttpClientRequest.get(url.toString()).pipe(
              HttpClientRequest.bearerToken(collector.token),
            ),
          )
          .pipe(
            Effect.mapError((error) =>
              unavailable(
                `${stage}'s trace collector could not be reached (${error.message}). Pull again: it resumes after the last export stored.`,
              ),
            ),
          );
        if (response.status === 401) {
          return yield* new PreviewTracePullError({
            reason: "CollectorUnauthorized",
            stage,
            message: `${stage}'s trace collector refused the token. Redeploy ${stage} with \`preview-cli deploy ${stage}\` and pull again.`,
          });
        }
        if (response.status !== 200) {
          return yield* unavailable(
            `${stage}'s trace collector answered ${response.status}. Pull again: it resumes after the last export stored.`,
          );
        }
        const page = yield* response.json.pipe(
          Effect.flatMap(decodeCollectorPage),
          Effect.mapError(() =>
            unavailable(
              `${stage}'s trace collector returned a page this CLI cannot read. Redeploy ${stage} so the collector matches this CLI, then pull again.`,
            ),
          ),
        );
        return page;
      });
    // The next page is requested while this one is stored: the collector's reads, not this
    // process, bound how fast a pull goes.
    let next = yield* Effect.forkChild(fetchPage(after));
    while (true) {
      const page = yield* Fiber.join(next);
      const nextAfter = page.next !== null ? Option.some(page.next) : after;
      const more = page.more && page.batches.length > 0;
      if (more) next = yield* Effect.forkChild(fetchPage(nextAfter));
      for (const batch of page.batches) {
        const body = Base64.decode(batch.body);
        if (body._tag === "Failure") {
          return yield* new PreviewTracePullError({
            reason: "ExportInvalid",
            stage,
            message: `Export ${batch.key} in ${stage}'s trace collector is not base64, so it cannot be read. Delete it from the stage's trace bucket and pull again.`,
          });
        }
        const decoded = yield* (
          batch.contentType.includes("protobuf")
            ? decodeOtlpProtobufTraces(body.success)
            : decodeOtlpJsonTraces(new TextDecoder().decode(body.success))
        ).pipe(
          Effect.mapError(
            (error) =>
              new PreviewTracePullError({
                reason: "ExportInvalid",
                stage,
                message: `Export ${batch.key} in ${stage}'s trace collector is not an OTLP trace export (${error.message}). Delete it from the stage's trace bucket and pull again.`,
              }),
          ),
        );
        yield* store.append(decoded);
        exports += 1;
        spans += decoded.length;
        after = Option.some(batch.key);
      }
      after = nextAfter;
      if (Option.isSome(after)) yield* saveCursor(after.value);
      if (!more) break;
    }
    const pulled: PreviewTracePull = { stage, exports, spans };
    return pulled;
  });
