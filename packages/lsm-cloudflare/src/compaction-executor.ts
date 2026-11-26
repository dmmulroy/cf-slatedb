import {
  blocksOverlappingRange,
  groupBlockReads,
  CompactionInputsChangedError,
  CompactionJob,
  CompactorEpoch,
  CompactorFencedError,
  commitCompaction,
  compactRowStreams,
  DatabaseId,
  decodeDataBlock,
  decodeSstMeta,
  intersectKeyRanges,
  keyRangeContains,
  keyRangeOverlapsClosed,
  MERGE_OPERATORS,
  MergeOperatorName,
  type Row,
  rowEncodedSizeEstimate,
  type SstBelowGcBoundaryError,
  type SstCorruptedError,
  SstHandle,
  sstHandleOwner,
  sstHandleVisibleRange,
  Timestamp,
} from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Clock, Context, Data, Effect, Layer, Metric, Option, Schema, Stream } from "effect";

import { backgroundWorkTestLayer, inlineBackgroundWorkLayer } from "./background-work.ts";
import { LsmConfiguration, lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import type { DatabaseNotFoundError } from "./lsm-errors.ts";
import {
  ManifestReader,
  type ManifestReadError,
  manifestReaderLayerWithoutDependencies,
  type VersionedManifest,
} from "./manifest-reader.ts";
import {
  ManifestStore,
  manifestStoreLayerWithoutDependencies,
  type ManifestWriteError,
} from "./manifest-store.ts";
import { sstKey } from "./object-keys.ts";
import {
  ObjectStore,
  objectStoreTestLayer,
  type ObjectStoreUnavailableError,
} from "./object-store.ts";
import { SstStore, sstStoreLayerWithoutDependencies } from "./sst-store.ts";

/**
 * Schema for a compaction prepared to run: everything its steps need, fixed when it starts so a
 * resumed step merges exactly the same inputs at the same time.
 *
 * **Details**
 *
 * `inputs` are newest first: each L0 SST alone (newest first), then each sorted run's SSTs in key
 * order (newest run first). `now` is the time TTL is evaluated at.
 */
export const PreparedCompaction = Schema.Struct({
  databaseId: DatabaseId,
  compactorEpoch: CompactorEpoch,
  job: CompactionJob,
  inputs: Schema.Array(Schema.Array(SstHandle)),
  now: Timestamp,
  mergeOperator: Schema.OptionFromNullOr(MergeOperatorName),
});

/** A compaction prepared to run. */
export type PreparedCompaction = typeof PreparedCompaction.Type;

/**
 * Schema for one output step's result: the SST it wrote (none when no rows were left) and the key
 * the next step starts from (none when the merge is done).
 */
export const CompactionOutputStep = Schema.Struct({
  output: Schema.OptionFromNullOr(SstHandle),
  resumeFrom: Schema.OptionFromNullOr(Schema.Uint8ArrayFromBase64),
});

/** One output step's result. */
export type CompactionOutputStep = typeof CompactionOutputStep.Type;

/**
 * A compaction step could not reach storage or the manifest. Retryable: the step can run again
 * (an output SST from a failed attempt is an orphan that GC removes).
 */
export class CompactionUnavailableError extends Data.TaggedError("CompactionUnavailableError")<{
  readonly databaseId: DatabaseId;
  readonly message: string;
}> {}

/** Failures that end a compaction: its output must be discarded and the job planned again. */
export type CompactionDiscardedError =
  | CompactorFencedError
  | CompactionInputsChangedError
  | SstBelowGcBoundaryError
  | DatabaseNotFoundError;

/** Every failure a compaction step can report. */
export type CompactionError =
  | CompactionDiscardedError
  | CompactionUnavailableError
  | SstCorruptedError;

/**
 * Most bytes one input read covers (D61). A Worker invocation may make only about 1,000 requests, so
 * a compaction step reads its inputs in chunks of consecutive blocks: a 64 MiB input is 32 reads,
 * and with two chunks in flight per input even eight inputs hold about 32 MiB.
 */
export const COMPACTION_READ_CHUNK_BYTES = 2 * 1024 * 1024;

/** Compactions committed, by kind of input (SlateDB RFC-0021 parity). */
export const compactionsCommitted = Metric.counter("lsm_compactions_committed", {
  description: "Compactions whose output was committed to the manifest",
});

/**
 * Runs one compaction job in steps that each fit a Workflow step: prepare, write one output SST at
 * a time, and commit.
 *
 * **Details**
 *
 * Each output step re-opens the inputs from its start key and streams them block by block straight
 * from R2 (never through the block caches), merges them with {@link compactRowStreams}, and streams
 * the output SST out as a multipart upload, so no input or output is ever fully buffered. Outputs
 * split only between keys, so a sorted run's SSTs never overlap.
 */
export interface ICompactionExecutor {
  /** Resolves the job's inputs in the newest manifest and fixes the time. */
  readonly prepare: (
    databaseId: DatabaseId,
    compactorEpoch: CompactorEpoch,
    job: CompactionJob,
  ) => Effect.Effect<PreparedCompaction, CompactionError, RuntimeContext>;
  /**
   * Writes the next output SST, starting at `startKey` (none: the first key), until it reaches
   * `compactionOutputSstSize`.
   */
  readonly writeOutput: (
    prepared: PreparedCompaction,
    startKey: Option.Option<Uint8Array>,
  ) => Effect.Effect<CompactionOutputStep, CompactionError, RuntimeContext>;
  /** Commits the outputs: replaces exactly the job's inputs with the destination sorted run. */
  readonly commit: (
    prepared: PreparedCompaction,
    outputs: ReadonlyArray<SstHandle>,
  ) => Effect.Effect<VersionedManifest, CompactionError, RuntimeContext>;
}

/** The contextual compaction executor. */
export class CompactionExecutor extends Context.Service<CompactionExecutor, ICompactionExecutor>()(
  "@shaledb/CompactionExecutor",
) {}

/** Constructs the executor over the contextual object store, SST store, manifests, and settings. */
export const makeCompactionExecutor = Effect.gen(function* () {
  const objects = yield* ObjectStore;
  const ssts = yield* SstStore;
  const reader = yield* ManifestReader;
  const manifests = yield* ManifestStore;
  const settings = yield* LsmConfiguration;

  const unavailable =
    (databaseId: DatabaseId) =>
    (error: ObjectStoreUnavailableError | ManifestReadError | ManifestWriteError) =>
      new CompactionUnavailableError({
        databaseId,
        message: `Compaction of database ${databaseId} could not reach storage: ${error.message} The step is retried.`,
      });

  const inputMissing = (databaseId: DatabaseId, handle: SstHandle) =>
    new CompactionInputsChangedError({
      databaseId,
      message: `Input SST ${handle.id} of a compaction of database ${databaseId} no longer exists. The compaction is discarded and planned again from the current manifest.`,
    });

  /**
   * One input SST's rows at or after `start` (inside its view), streamed in chunks of consecutive
   * blocks of up to {@link COMPACTION_READ_CHUNK_BYTES} (one request each).
   */
  const sstRows = (databaseId: DatabaseId, handle: SstHandle, start: Option.Option<Uint8Array>) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const range = Option.match(start, {
          onNone: () => Option.some(sstHandleVisibleRange(handle)),
          onSome: (key) =>
            intersectKeyRanges(sstHandleVisibleRange(handle), { start: key, end: Option.none() }),
        });
        if (
          Option.isNone(range) ||
          !keyRangeOverlapsClosed(range.value, handle.firstKey, handle.lastKey)
        ) {
          return Stream.empty;
        }
        const key = sstKey(sstHandleOwner(handle, databaseId), handle.id);
        const metaBytes = yield* objects
          .getRange(key, { offset: handle.metaOffset, length: handle.metaLength })
          .pipe(Effect.mapError(unavailable(databaseId)));
        if (Option.isNone(metaBytes)) return yield* inputMissing(databaseId, handle);
        const meta = yield* Effect.fromResult(decodeSstMeta(handle.id, metaBytes.value));
        const visible = range.value;
        const reads = groupBlockReads(
          meta.index,
          blocksOverlappingRange(meta, visible),
          COMPACTION_READ_CHUNK_BYTES,
        );
        return Stream.fromIterable(reads).pipe(
          // Fetch the next chunk while this one is merged: two chunks per input in memory.
          Stream.mapEffect(
            (read) =>
              Effect.gen(function* () {
                const bytes = yield* objects
                  .getRange(key, { offset: read.offset, length: read.length })
                  .pipe(Effect.mapError(unavailable(databaseId)));
                if (Option.isNone(bytes)) return yield* inputMissing(databaseId, handle);
                const rows: Array<Row> = [];
                for (const index of read.blocks) {
                  const entry = meta.index[index];
                  if (entry === undefined) continue;
                  const start = entry.offset - read.offset;
                  const block = yield* Effect.fromResult(
                    decodeDataBlock(bytes.value.subarray(start, start + entry.length), {
                      sstId: handle.id,
                      offset: entry.offset,
                    }),
                  );
                  rows.push(...block.rows());
                }
                return rows;
              }),
            { concurrency: 2 },
          ),
          Stream.flattenIterable,
          Stream.filter((row: Row) => keyRangeContains(visible, row.key)),
        );
      }),
    );

  const prepare = Effect.fn("CompactionExecutor.prepare")(function* (
    databaseId: DatabaseId,
    compactorEpoch: CompactorEpoch,
    job: CompactionJob,
  ) {
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "compaction.sources": job.sources.length,
      "compaction.destination": job.destination,
    });
    const { manifest } = yield* reader.loadLatest(databaseId).pipe(
      Effect.catchTags({
        ManifestReadError: (error) => Effect.fail(unavailable(databaseId)(error)),
      }),
    );
    if (manifest.compactorEpoch !== compactorEpoch) {
      return yield* new CompactorFencedError({
        databaseId,
        compactorEpoch,
        currentCompactorEpoch: manifest.compactorEpoch,
        message: `Compactor epoch ${compactorEpoch} of database ${databaseId} was fenced by epoch ${manifest.compactorEpoch}. This compaction is discarded; the newer compactor continues.`,
      });
    }
    const l0Sources = new Set(
      job.sources.flatMap((source) => (source._tag === "L0" ? [source.sstId] : [])),
    );
    const runSources = new Set(
      job.sources.flatMap((source) => (source._tag === "SortedRun" ? [source.id] : [])),
    );
    const l0Inputs = manifest.l0
      .filter((handle) => l0Sources.has(handle.id))
      .map((handle) => [handle]);
    const runInputs = manifest.sortedRuns
      .filter((run) => runSources.has(run.id))
      .map((run) => run.ssts);
    if (l0Inputs.length !== l0Sources.size || runInputs.length !== runSources.size) {
      return yield* new CompactionInputsChangedError({
        databaseId,
        message: `A compaction of database ${databaseId} was planned against inputs the newest manifest no longer holds. It is discarded and planned again.`,
      });
    }
    const now = Timestamp.make(yield* Clock.currentTimeMillis);
    return {
      databaseId,
      compactorEpoch,
      job,
      inputs: [...l0Inputs, ...runInputs],
      now,
      mergeOperator: manifest.options.mergeOperator,
    };
  });

  const writeOutput = Effect.fn("CompactionExecutor.writeOutput")(function* (
    prepared: PreparedCompaction,
    startKey: Option.Option<Uint8Array>,
  ) {
    const { databaseId } = prepared;
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "compaction.resumed": Option.isSome(startKey),
    });
    const sources = prepared.inputs.map((level) =>
      Stream.fromIterable(level).pipe(
        Stream.flatMap((handle) => sstRows(databaseId, handle, startKey)),
      ),
    );
    const target = settings.compactionOutputSstSize;
    let size = 0;
    let resumeFrom = Option.none<Uint8Array>();
    const rows = compactRowStreams(sources, {
      now: prepared.now,
      isBottom: prepared.job.isBottom,
      mergeOperator: Option.map(prepared.mergeOperator, (name) => MERGE_OPERATORS[name]),
    }).pipe(
      // Close this output between keys once it reaches the target; the next step starts there.
      Stream.takeWhile((versions) => {
        if (size >= target) {
          resumeFrom = Option.some(versions[0].key);
          return false;
        }
        for (const row of versions) size += rowEncodedSizeEstimate(row);
        return true;
      }),
      Stream.flattenArray,
    );
    const written = yield* ssts
      .write({
        owner: databaseId,
        kind: "Compacted",
        objectKey: (id) => sstKey(databaseId, id),
        rows,
      })
      .pipe(
        Effect.catchTags({
          ObjectStoreUnavailableError: (error) => Effect.fail(unavailable(databaseId)(error)),
        }),
      );
    yield* Effect.annotateCurrentSpan({
      "sst.id": Option.match(written, { onNone: () => "", onSome: (sst) => sst.handle.id }),
      "compaction.done": Option.isNone(resumeFrom),
    });
    return { output: Option.map(written, (sst) => sst.handle), resumeFrom };
  });

  const commit = Effect.fn("CompactionExecutor.commit")(function* (
    prepared: PreparedCompaction,
    outputs: ReadonlyArray<SstHandle>,
  ) {
    const { databaseId } = prepared;
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "compaction.outputs": outputs.length,
    });
    const committed = yield* manifests
      .update(databaseId, (manifest) =>
        commitCompaction(manifest, {
          compactorEpoch: prepared.compactorEpoch,
          job: prepared.job,
          outputs,
        }),
      )
      .pipe(
        Effect.catchTags({
          ManifestReadError: (error) => Effect.fail(unavailable(databaseId)(error)),
          ManifestWriteError: (error) => Effect.fail(unavailable(databaseId)(error)),
        }),
      );
    yield* Metric.update(compactionsCommitted, 1);
    yield* Effect.annotateCurrentSpan({ "manifest.id": committed.id });
    return committed;
  });

  return CompactionExecutor.of({ prepare, writeOutput, commit });
});

/** Provides the executor, leaving the object store and settings visible. */
export const compactionExecutorLayerWithoutDependencies = Layer.effect(
  CompactionExecutor,
  makeCompactionExecutor,
).pipe(
  Layer.provide(sstStoreLayerWithoutDependencies),
  Layer.provide(
    manifestStoreLayerWithoutDependencies.pipe(
      Layer.provideMerge(manifestReaderLayerWithoutDependencies),
      // The executor runs inside Workflow steps, which have no `waitUntil`.
      Layer.provide(inlineBackgroundWorkLayer),
    ),
  ),
);

/**
 * Provides the executor and the manifest store and reader over a fresh in-memory object store,
 * which the Layer also outputs, with default settings (overridable).
 */
export const compactionExecutorTestLayer = (
  overrides: Parameters<typeof lsmConfigurationTestLayer>[0] = {},
) =>
  Layer.effect(CompactionExecutor, makeCompactionExecutor).pipe(
    Layer.provide(sstStoreLayerWithoutDependencies),
    Layer.provideMerge(
      manifestStoreLayerWithoutDependencies.pipe(
        Layer.provideMerge(manifestReaderLayerWithoutDependencies),
        Layer.provide(backgroundWorkTestLayer),
      ),
    ),
    Layer.provideMerge(Layer.mergeAll(objectStoreTestLayer, lsmConfigurationTestLayer(overrides))),
  );
