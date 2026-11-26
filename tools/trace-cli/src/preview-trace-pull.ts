import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { Data, Effect, Fiber, FileSystem, Option, Path, Schema } from "effect";
import { Base64 } from "effect/encoding";
import { HttpClient, HttpClientRequest } from "effect/http";

import { TRACE_TOKEN_VARIABLE, type TraceCollector } from "./trace-collector-option.ts";
import { decodeOtlpJsonTraces, decodeOtlpProtobufTraces } from "./otlp-trace-decoder.ts";
import { TraceStore } from "./trace-store.ts";

/**
 * Downloading a live stage's traces failed.
 *
 * `reason` is `CollectorUnknown` when the preview CLI could not name the stage's collector (nothing
 * deployed, or no collector yet), `CollectorUnavailable` when the collector could not be reached or
 * answered with an error, `CollectorUnauthorized` when it refused the token, and `ExportInvalid` when
 * a stored export is not an OTLP trace export. `collector` is the collector's name (see
 * {@link TraceCollector}).
 */
export class PreviewTracePullError extends Data.TaggedError("PreviewTracePullError")<{
  readonly reason:
    | "CollectorUnknown"
    | "CollectorUnavailable"
    | "CollectorUnauthorized"
    | "ExportInvalid";
  readonly collector: string;
  readonly message: string;
}> {}

/** Schema for a preview stage's trace collector, as `preview-cli collector <stage> --json` prints it. */
const PreviewCollector = Schema.Struct({
  stage: Schema.String,
  url: Schema.String,
  token: Schema.String,
});

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
        collector: stage,
        message: `The trace collector of ${stage} could not be found; deploy the stage with \`preview-cli deploy ${stage}\` and pull again:\n${`${result.stdout}${result.stderr}`.trim()}`,
      });
    }
    const collector = yield* decodePreviewCollector(result.stdout.trim()).pipe(
      Effect.mapError(
        () =>
          new PreviewTracePullError({
            reason: "CollectorUnknown",
            collector: stage,
            message: `The preview CLI printed something other than ${stage}'s collector; check it with \`preview-cli url ${stage}\` and pull again.`,
          }),
      ),
    );
    const resolved: TraceCollector = {
      name: stage,
      url: collector.url,
      token: collector.token,
      stage: Option.some(stage),
    };
    return resolved;
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

/** Schema for a pull cursor: where the last pull from one collector URL stopped. */
const PullCursor = Schema.Struct({ url: Schema.String, after: Schema.String });
const decodePullCursor = Schema.decodeUnknownEffect(Schema.fromJsonString(PullCursor));
const encodePullCursor = Schema.encodeEffect(Schema.fromJsonString(PullCursor));

/** What one pull downloaded. */
export interface PreviewTracePull {
  /** The collector's name (see {@link TraceCollector}). */
  readonly collector: string;
  /** OTLP exports downloaded. */
  readonly exports: number;
  /** Spans stored from them. */
  readonly spans: number;
}

/** Prefix of a collector's export keys, which then start with the arrival time in ms. */
const TRACE_EXPORTS_PREFIX = "traces/";

/**
 * Where the store's last pull from `url` stopped: the furthest of every cursor in `<store>/pulls/`
 * recorded for that URL, whatever file it is in. A store pulled with `--stage` and later with
 * `--collector-url` for the same collector (or the reverse) resumes instead of storing its spans
 * twice.
 */
const savedPosition = (pullsDirectory: string, url: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const names = yield* fs
      .readDirectory(pullsDirectory)
      .pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));
    let furthest = Option.none<string>();
    for (const name of names.filter((file) => file.endsWith(".json"))) {
      const cursor = yield* fs.readFileString(path.join(pullsDirectory, name)).pipe(
        Effect.flatMap(decodePullCursor),
        Effect.map(Option.some),
        Effect.orElseSucceed(() => Option.none<typeof PullCursor.Type>()),
      );
      if (Option.isNone(cursor) || cursor.value.url !== url) continue;
      const after = cursor.value.after;
      if (Option.isNone(furthest) || after > furthest.value) furthest = Option.some(after);
    }
    return furthest;
  });

/**
 * Downloads every export the collector stored since the last pull into the trace store, so `list`,
 * `show`, `grep`, `stats`, `diff`, and `assert` see the stage's spans. The cursor is kept in
 * `<store>/pulls/<name>.json` (the collector's {@link TraceCollector} name) and is matched by
 * collector URL (a recreated collector starts from the beginning; see {@link savedPosition}). It
 * advances after each export is stored, so a failed pull resumes where it stopped.
 *
 * @returns What was downloaded, or `PreviewTracePullError`: `CollectorUnauthorized` (the token was
 *   refused), `CollectorUnavailable` (unreachable or an error status; pull again), or `ExportInvalid`
 *   (a stored export is not OTLP traces); or `TraceStoreError` when the local store cannot be written.
 */
export const pullPreviewTraces = (
  collector: TraceCollector,
  /** Skip exports that arrived before this Unix millisecond time (keys start with it). */
  sinceMs: Option.Option<number> = Option.none(),
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const store = yield* TraceStore;
    const client = yield* HttpClient.HttpClient;
    const { name } = collector;
    const pullsDirectory = path.join(store.directory, "pulls");
    const cursorFile = path.join(pullsDirectory, `${name}.json`);
    const resumed = yield* savedPosition(pullsDirectory, collector.url);
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
      new PreviewTracePullError({ reason: "CollectorUnavailable", collector: name, message });
    /** Fetches and decodes the page of exports after `from`. */
    const fetchPage = (from: Option.Option<string>) =>
      Effect.gen(function* () {
        const url = new URL("/v1/traces/batches", collector.url);
        url.searchParams.set("limit", "500");
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
                `${name}'s trace collector could not be reached (${error.message}). Pull again: it resumes after the last export stored.`,
              ),
            ),
          );
        if (response.status === 401) {
          return yield* new PreviewTracePullError({
            reason: "CollectorUnauthorized",
            collector: name,
            message: Option.match(collector.stage, {
              onSome: (stage) =>
                `${stage}'s trace collector refused the token. Redeploy ${stage} with \`preview-cli deploy ${stage}\` and pull again.`,
              onNone: () =>
                `${name}'s trace collector refused the token in ${TRACE_TOKEN_VARIABLE}. Set it to that stage's token (\`preview-cli trace-token <stage>\`, see docs/operations.md) and pull again.`,
            }),
          });
        }
        if (response.status !== 200) {
          return yield* unavailable(
            `${name}'s trace collector answered ${response.status}. Pull again: it resumes after the last export stored.`,
          );
        }
        const page = yield* response.json.pipe(
          Effect.flatMap(decodeCollectorPage),
          Effect.mapError(() =>
            unavailable(
              `${name}'s trace collector returned a page this CLI cannot read: the stage was deployed from a different commit. Run the trace CLI from the commit the stage was deployed from, then pull again.`,
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
            collector: name,
            message: `Export ${batch.key} in ${name}'s trace collector is not base64, so it cannot be read. Delete it from the stage's trace bucket and pull again.`,
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
                collector: name,
                message: `Export ${batch.key} in ${name}'s trace collector is not an OTLP trace export (${error.message}). Delete it from the stage's trace bucket and pull again.`,
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
    const pulled: PreviewTracePull = { collector: name, exports, spans };
    return pulled;
  });
