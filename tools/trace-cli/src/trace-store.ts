import { resolve } from "node:path";

import { NodeServices } from "@effect/platform-node";
import { Context, Data, Effect, FileSystem, Layer, Path, Schema, Semaphore } from "effect";

import { type CollectedSpan, CollectedSpanJsonLine } from "./collected-span.ts";

/** Default trace store directory: `.traces/` at the repository root (gitignored). */
export const DEFAULT_TRACE_STORE_DIRECTORY = resolve(
  import.meta.dirname,
  "..",
  "..",
  "..",
  ".traces",
);

/**
 * Failure to read or write the local trace store.
 *
 * `reason` is `StoreUnavailable` when the store directory or a trace file cannot be read or written,
 * `TraceNotFound` when no trace has the requested id, and `StoredSpanInvalid` when a stored line is
 * not a span the CLI wrote (delete the trace file, or run `clear`).
 */
export class TraceStoreError extends Data.TaggedError("TraceStoreError")<{
  readonly operation: "append" | "listTraceIds" | "readTrace" | "clear";
  readonly reason: "StoreUnavailable" | "TraceNotFound" | "StoredSpanInvalid";
  readonly traceId?: string;
  readonly message: string;
}> {}

/** Local, file-backed store of collected spans, one NDJSON file per trace id. */
export interface ITraceStore {
  /** Directory the store writes to. */
  readonly directory: string;
  /**
   * Appends spans, grouped into their trace files.
   *
   * @returns Nothing, or `TraceStoreError` (`StoreUnavailable`) when a file cannot be written.
   */
  readonly append: (spans: ReadonlyArray<CollectedSpan>) => Effect.Effect<void, TraceStoreError>;
  /**
   * Lists every stored trace id, most recently written first.
   *
   * @returns Trace ids, or `TraceStoreError` (`StoreUnavailable`).
   */
  readonly listTraceIds: Effect.Effect<ReadonlyArray<string>, TraceStoreError>;
  /**
   * Lists the trace ids whose files were last written at or after `sinceMs` (epoch ms, the local
   * clock), most recently written first, reading no file. A span is written when it arrives, so a
   * file last written before `sinceMs` holds only spans that arrived before it.
   *
   * @returns Trace ids, or `TraceStoreError` (`StoreUnavailable`).
   */
  readonly listTraceIdsWrittenSince: (
    sinceMs: number,
  ) => Effect.Effect<ReadonlyArray<string>, TraceStoreError>;
  /**
   * Reads every span of one trace, in the order they arrived.
   *
   * @returns The spans, or `TraceStoreError` (`TraceNotFound`, `StoreUnavailable`, or
   *   `StoredSpanInvalid`).
   */
  readonly readTrace: (
    traceId: string,
  ) => Effect.Effect<ReadonlyArray<CollectedSpan>, TraceStoreError>;
  /**
   * Deletes every stored trace.
   *
   * @returns Nothing, or `TraceStoreError` (`StoreUnavailable`).
   */
  readonly clear: Effect.Effect<void, TraceStoreError>;
}

/** Provides the local trace store. */
export class TraceStore extends Context.Service<TraceStore, ITraceStore>()(
  "@shaledb/tools-trace-cli/TraceStore",
) {}

const decodeCollectedSpanLine = Schema.decodeEffect(CollectedSpanJsonLine);
const encodeCollectedSpanLine = Schema.encodeEffect(CollectedSpanJsonLine);

const TRACE_ID_PATTERN = /^[0-9a-f]{1,64}$/;

/**
 * Constructs the trace store over the contextual file system, rooted at `directory`.
 *
 * @returns The store; constructing it creates the directory and fails with `TraceStoreError`
 *   (`StoreUnavailable`) when that is not possible.
 */
export const makeTraceStore = (directory: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const spansDirectory = path.join(directory, "spans");
    const writeLock = yield* Semaphore.make(1);
    const unavailable =
      (operation: TraceStoreError["operation"], traceId?: string) => (cause: { message: string }) =>
        new TraceStoreError({
          operation,
          reason: "StoreUnavailable",
          ...(traceId === undefined ? {} : { traceId }),
          message: `The trace store at ${directory} could not be used (${operation}): ${cause.message}. Check that the directory is writable.`,
        });
    yield* fs
      .makeDirectory(spansDirectory, { recursive: true })
      .pipe(Effect.mapError(unavailable("append")));

    const traceFile = (traceId: string) => path.join(spansDirectory, `${traceId}.ndjson`);
    /** Trace ids whose files were last written at or after `sinceMs`, newest first. */
    const listWrittenSince = (sinceMs: number) =>
      Effect.gen(function* () {
        const names = yield* fs
          .readDirectory(spansDirectory)
          .pipe(Effect.mapError(unavailable("listTraceIds")));
        const entries = yield* Effect.forEach(
          names.filter((name) => name.endsWith(".ndjson")),
          (name) =>
            fs.stat(path.join(spansDirectory, name)).pipe(
              Effect.map((info) => ({
                traceId: name.slice(0, -".ndjson".length),
                modified: info.mtime._tag === "Some" ? info.mtime.value.getTime() : 0,
              })),
              Effect.mapError(unavailable("listTraceIds")),
            ),
        );
        return entries
          .filter((entry) => entry.modified >= sinceMs)
          .sort((left, right) => right.modified - left.modified)
          .map((entry) => entry.traceId);
      });

    return TraceStore.of({
      directory,
      append: Effect.fn("TraceStore.append")(function* (spans) {
        const byTrace = new Map<string, Array<string>>();
        for (const span of spans) {
          if (!TRACE_ID_PATTERN.test(span.traceId)) continue;
          const line = yield* encodeCollectedSpanLine(span).pipe(Effect.orDie);
          const lines = byTrace.get(span.traceId) ?? [];
          lines.push(line);
          byTrace.set(span.traceId, lines);
        }
        yield* writeLock.withPermits(1)(
          Effect.forEach(byTrace, ([traceId, lines]) =>
            fs
              .writeFileString(traceFile(traceId), `${lines.join("\n")}\n`, { flag: "a" })
              .pipe(Effect.mapError(unavailable("append", traceId))),
          ),
        );
      }),
      listTraceIds: Effect.suspend(() => listWrittenSince(Number.NEGATIVE_INFINITY)),
      listTraceIdsWrittenSince: listWrittenSince,
      readTrace: Effect.fn("TraceStore.readTrace")(function* (traceId) {
        const file = traceFile(traceId);
        const exists = yield* fs
          .exists(file)
          .pipe(Effect.mapError(unavailable("readTrace", traceId)));
        if (!TRACE_ID_PATTERN.test(traceId) || !exists) {
          return yield* new TraceStoreError({
            operation: "readTrace",
            reason: "TraceNotFound",
            traceId,
            message: `No trace ${traceId} is in the trace store at ${directory}. Run \`list\` to see the stored traces.`,
          });
        }
        const text = yield* fs
          .readFileString(file)
          .pipe(Effect.mapError(unavailable("readTrace", traceId)));
        return yield* Effect.forEach(
          text.split("\n").filter((line) => line.length > 0),
          (line) =>
            decodeCollectedSpanLine(line).pipe(
              Effect.mapError(
                () =>
                  new TraceStoreError({
                    operation: "readTrace",
                    reason: "StoredSpanInvalid",
                    traceId,
                    message: `Trace ${traceId} in ${directory} has a line that is not a collected span. Delete the trace file or run \`clear\`.`,
                  }),
              ),
            ),
        );
      }),
      clear: fs
        .remove(spansDirectory, { recursive: true })
        .pipe(
          Effect.andThen(fs.makeDirectory(spansDirectory, { recursive: true })),
          Effect.mapError(unavailable("clear")),
        ),
    });
  });

/**
 * Provides the trace store at `directory`, leaving the file system and path services visible.
 */
export const traceStoreLayerWithoutDependencies = (directory: string) =>
  Layer.effect(TraceStore, makeTraceStore(directory));

/** Provides the trace store at `directory` on the Node.js file system. */
export const traceStoreLayer = (directory: string) =>
  traceStoreLayerWithoutDependencies(directory).pipe(Layer.provide(NodeServices.layer));

/**
 * Provides a trace store in a fresh temporary directory on the real file system, removed when the
 * Layer's scope closes.
 */
export const traceStoreTestLayer = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "trace-store-" });
    return traceStoreLayerWithoutDependencies(directory);
  }).pipe(Effect.orDie),
).pipe(Layer.provide(NodeServices.layer));
