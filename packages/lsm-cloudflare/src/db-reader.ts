import {
  firstBlockForKey,
  blocksOverlappingRange,
  type DatabaseId,
  groupRowsByKey,
  intersectKeyRanges,
  type KeyRange,
  keyRangeContains,
  keyRangeOverlapsClosed,
  type Manifest,
  type ManifestId,
  MERGE_OPERATORS,
  type MergeOperatorMissingError,
  mergeRowStreams,
  resolveVisibleRow,
  type Row,
  type SeqNum,
  type SstCorruptedError,
  type SstHandle,
  sstHandleOwner,
  sstHandleVisibleRange,
  sstMayContainKey,
  Timestamp,
} from "@cf-slatedb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Bytes, Clock, Context, Data, Effect, Layer, Option, Stream } from "effect";

import { BlockCache, blockCacheLayer, blockCacheLayerWithoutDependencies } from "./block-cache.ts";
import { blockSourceTestLayer } from "./block-source.ts";
import { lsmConfigurationLayer, lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import { objectStoreTestLayer } from "./object-store.ts";
import type { BlockSourceServices } from "./block-source.ts";
import type { DatabaseNotFoundError } from "./lsm-errors.ts";
import { ManifestCache, manifestCacheLayerWithoutDependencies } from "./manifest-cache.ts";
import {
  ManifestReader,
  manifestReaderLayer,
  manifestReaderLayerWithoutDependencies,
  type ManifestReadError,
  type VersionedManifest,
} from "./manifest-reader.ts";

/** What a read sees: how new its manifest must be, its snapshot, and memtable rows (for `Latest`). */
export interface ReadView {
  /** The manifest must have `lastL0Seq >= minLastL0Seq` (a `Latest` probe's `flushedSeq`). */
  readonly minLastL0Seq: SeqNum;
  /** Newest sequence number the read sees; none: the manifest's `lastL0Seq` (a `Flushed` read). */
  readonly snapshotSeq: Option.Option<SeqNum>;
  /** Unflushed rows from the writer, in LSM order; empty for `Flushed` reads. */
  readonly memtable: ReadonlyArray<Row>;
  /** Read exactly this manifest (a snapshot) instead of the newest one. */
  readonly manifestId: Option.Option<ManifestId>;
}

/**
 * A read could not be completed because manifests or blocks were unavailable. Retryable.
 */
export class ReaderUnavailableError extends Data.TaggedError("ReaderUnavailableError")<{
  readonly databaseId: DatabaseId;
  readonly reason: "StorageUnavailable" | "StoredManifestInvalid" | "SnapshotExpired";
  readonly message: string;
}> {}

/** Every failure a read can report. */
export type DbReadError =
  | DatabaseNotFoundError
  | ReaderUnavailableError
  | MergeOperatorMissingError
  | SstCorruptedError;

/** Services a read needs from the current invocation. */
export type DbReaderServices = BlockSourceServices | RuntimeContext;

/** Scan options. */
export interface ScanOptions {
  /** Most rows to return; none: all. */
  readonly limit: Option.Option<number>;
}

/**
 * Reads SSTs in the caller's isolate: finds the manifest a view needs, skips SSTs by key range and
 * Bloom filter, reads blocks through the L1 cache, merges with the view's memtable rows, and resolves
 * merges, deletes, and TTL.
 */
export interface IDbReader {
  /** The key's visible row at the view, or none. */
  readonly getRow: (
    databaseId: DatabaseId,
    key: Uint8Array,
    view: ReadView,
  ) => Effect.Effect<Option.Option<Row>, DbReadError, DbReaderServices>;
  /** Visible rows with keys in `range`, in key order. */
  readonly scan: (
    databaseId: DatabaseId,
    range: KeyRange,
    view: ReadView,
    options: ScanOptions,
  ) => Stream.Stream<Row, DbReadError, DbReaderServices>;
  /** The manifest a `Flushed` read at this moment would use (for snapshots and `AtLeast`). */
  readonly currentManifest: (
    databaseId: DatabaseId,
  ) => Effect.Effect<
    VersionedManifest,
    DatabaseNotFoundError | ReaderUnavailableError,
    DbReaderServices
  >;
}

/** The contextual database reader. */
export class DbReader extends Context.Service<DbReader, IDbReader>()("@cf-slatedb/DbReader") {}

/** Every SST a manifest references, grouped by level, newest level first: each L0 SST, then each run. */
const levels = (manifest: Manifest): ReadonlyArray<ReadonlyArray<SstHandle>> => [
  ...manifest.l0.map((handle) => [handle]),
  ...manifest.sortedRuns.map((run) => run.ssts),
];

/** Constructs the reader over the contextual manifest cache, manifest reader, and block cache. */
export const makeDbReader = Effect.gen(function* () {
  const manifests = yield* ManifestCache;
  const reader = yield* ManifestReader;
  const blocks = yield* BlockCache;

  const manifestUnavailable = (databaseId: DatabaseId) => ({
    ManifestReadError: (error: ManifestReadError) =>
      Effect.fail(
        new ReaderUnavailableError({
          databaseId,
          reason:
            error.reason === "StoredManifestInvalid"
              ? "StoredManifestInvalid"
              : "StorageUnavailable",
          message: `Database ${databaseId}'s manifest could not be read: ${error.message}`,
        }),
      ),
  });

  /** The manifest a view reads: the pinned one, or the cached newest one refreshed until it is new enough. */
  const manifestFor = (databaseId: DatabaseId, view: ReadView) =>
    Effect.gen(function* () {
      if (Option.isSome(view.manifestId)) {
        const pinned = yield* reader
          .load(databaseId, view.manifestId.value)
          .pipe(Effect.catchTags(manifestUnavailable(databaseId)));
        if (Option.isNone(pinned)) {
          return yield* new ReaderUnavailableError({
            databaseId,
            reason: "SnapshotExpired",
            message: `Snapshot manifest ${view.manifestId.value} of database ${databaseId} no longer exists. Take a new snapshot and read again.`,
          });
        }
        return { id: view.manifestId.value, manifest: pinned.value };
      }
      let current = yield* manifests
        .current(databaseId)
        .pipe(Effect.catchTags(manifestUnavailable(databaseId)));
      if (current.manifest.lastL0Seq < view.minLastL0Seq) {
        // The writer flushed rows the cached manifest predates; its commit happened first.
        current = yield* manifests
          .refresh(databaseId)
          .pipe(Effect.catchTags(manifestUnavailable(databaseId)));
        yield* Effect.annotateCurrentSpan({ "manifest.refreshed": true });
      }
      if (current.manifest.lastL0Seq < view.minLastL0Seq) {
        return yield* new ReaderUnavailableError({
          databaseId,
          reason: "StorageUnavailable",
          message: `Database ${databaseId}'s newest manifest does not yet include sequence number ${view.minLastL0Seq}. Retry the read.`,
        });
      }
      yield* Effect.annotateCurrentSpan({ "manifest.id": current.id });
      return current;
    });

  const blockFailures = (databaseId: DatabaseId) => ({
    SstMissingError: (error: { readonly message: string }) =>
      Effect.fail(
        new ReaderUnavailableError({
          databaseId,
          reason: "StorageUnavailable",
          message: `An SST of database ${databaseId} was missing (the manifest changed during the read): ${error.message} Retry the read.`,
        }),
      ),
    BlockSourceUnavailableError: (error: { readonly message: string }) =>
      Effect.fail(
        new ReaderUnavailableError({
          databaseId,
          reason: "StorageUnavailable",
          message: `Database ${databaseId}'s blocks could not be read: ${error.message}`,
        }),
      ),
  });

  /** Every version of `key` in one SST at or below `snapshotSeq`, newest first. */
  const sstVersions = (
    databaseId: DatabaseId,
    handle: SstHandle,
    key: Uint8Array,
    snapshotSeq: SeqNum,
  ) =>
    Effect.gen(function* () {
      const owner = sstHandleOwner(handle, databaseId);
      const meta = yield* blocks.meta(owner, handle);
      if (!sstMayContainKey(meta, key)) {
        yield* Effect.annotateCurrentSpan({ "filter.negative": true });
        return [];
      }
      const versions: Array<Row> = [];
      const first = firstBlockForKey(meta, key);
      if (Option.isNone(first)) return versions;
      for (let index = first.value; index < meta.index.length; index++) {
        const entry = meta.index[index];
        if (entry === undefined) break;
        const block = yield* blocks.block(owner, handle, entry);
        let pastKey = false;
        for (const row of block.seek(key)) {
          if (!Bytes.Equivalence(row.key, key)) {
            pastKey = true;
            break;
          }
          if (row.seq <= snapshotSeq) versions.push(row);
        }
        if (pastKey) break;
      }
      return versions;
    }).pipe(Effect.withSpan("DbReader.sstVersions", { attributes: { "sst.id": handle.id } }));

  const getRow = Effect.fn("DbReader.getRow")(function* (
    databaseId: DatabaseId,
    key: Uint8Array,
    view: ReadView,
  ) {
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "read.memtable_versions": view.memtable.length,
    });
    const versions: Array<Row> = view.memtable.filter((row) => Bytes.Equivalence(row.key, key));
    const settled = (rows: ReadonlyArray<Row>) => rows.some((row) => row.value._tag !== "Merge");
    const { manifest } = yield* manifestFor(databaseId, view);
    const snapshotSeq = Option.getOrElse(view.snapshotSeq, () => manifest.lastL0Seq);
    let sstsRead = 0;
    if (!settled(versions)) {
      for (const level of levels(manifest)) {
        const handle = level.find(
          (candidate) =>
            keyRangeContains(sstHandleVisibleRange(candidate), key) &&
            Bytes.Order(key, candidate.firstKey) >= 0 &&
            Bytes.Order(key, candidate.lastKey) <= 0,
        );
        if (handle === undefined) continue;
        sstsRead++;
        const found = yield* sstVersions(databaseId, handle, key, snapshotSeq).pipe(
          Effect.catchTags(blockFailures(databaseId)),
        );
        versions.push(...found);
        if (settled(found)) break;
      }
    }
    yield* Effect.annotateCurrentSpan({ seq: snapshotSeq, "read.ssts": sstsRead });
    const now = Timestamp.make(yield* Clock.currentTimeMillis);
    return yield* Effect.fromResult(
      resolveVisibleRow(versions, {
        snapshotSeq,
        now,
        mergeOperator: Option.map(manifest.options.mergeOperator, (name) => MERGE_OPERATORS[name]),
      }),
    );
  });

  /** One SST's rows in `range` (within its view) at or below `snapshotSeq`, in LSM order. */
  const sstRows = (
    databaseId: DatabaseId,
    handle: SstHandle,
    range: KeyRange,
    snapshotSeq: SeqNum,
  ) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const effective = intersectKeyRanges(range, sstHandleVisibleRange(handle));
        if (
          Option.isNone(effective) ||
          !keyRangeOverlapsClosed(effective.value, handle.firstKey, handle.lastKey)
        ) {
          return Stream.empty;
        }
        const owner = sstHandleOwner(handle, databaseId);
        const meta = yield* blocks.meta(owner, handle);
        const indexes = blocksOverlappingRange(meta, effective.value);
        return Stream.fromIterable(indexes).pipe(
          // Fetch the next block while the current one is consumed.
          Stream.mapEffect(
            (index) => {
              const entry = meta.index[index];
              return entry === undefined
                ? Effect.succeed([])
                : Effect.map(blocks.block(owner, handle, entry), (block) => [...block.rows()]);
            },
            { concurrency: 2 },
          ),
          Stream.flatMap((rows) => Stream.fromIterable(rows)),
          Stream.filter(
            (row) => row.seq <= snapshotSeq && keyRangeContains(effective.value, row.key),
          ),
        );
      }),
    ).pipe(
      Stream.catchTags({
        SstMissingError: (error) =>
          Stream.fromEffect(blockFailures(databaseId).SstMissingError(error)),
        BlockSourceUnavailableError: (error) =>
          Stream.fromEffect(blockFailures(databaseId).BlockSourceUnavailableError(error)),
      }),
    );

  const scan = (databaseId: DatabaseId, range: KeyRange, view: ReadView, options: ScanOptions) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const { manifest } = yield* manifestFor(databaseId, view);
        const snapshotSeq = Option.getOrElse(view.snapshotSeq, () => manifest.lastL0Seq);
        const now = Timestamp.make(yield* Clock.currentTimeMillis);
        const mergeOperator = Option.map(
          manifest.options.mergeOperator,
          (name) => MERGE_OPERATORS[name],
        );
        const memtable = Stream.fromIterable(
          view.memtable.filter((row) => row.seq <= snapshotSeq && keyRangeContains(range, row.key)),
        );
        const ssts = manifest.l0
          .concat(manifest.sortedRuns.flatMap((run) => run.ssts))
          .map((handle) => sstRows(databaseId, handle, range, snapshotSeq));
        const visible = groupRowsByKey(mergeRowStreams([memtable, ...ssts])).pipe(
          Stream.mapEffect((versions) =>
            Effect.fromResult(resolveVisibleRow(versions, { snapshotSeq, now, mergeOperator })),
          ),
          Stream.flatMap((row) => Stream.fromIterable(Option.toArray(row))),
        );
        return Option.match(options.limit, {
          onNone: () => visible,
          onSome: (limit) => Stream.take(visible, limit),
        });
      }),
    ).pipe(Stream.withSpan("DbReader.scan", { attributes: { "database.id": databaseId } }));

  const currentManifest = Effect.fn("DbReader.currentManifest")(function* (databaseId: DatabaseId) {
    return yield* manifests
      .current(databaseId)
      .pipe(Effect.catchTags(manifestUnavailable(databaseId)));
  });

  return DbReader.of({ getRow, scan, currentManifest });
});

/** Provides the reader, leaving its caches and manifest reader visible. */
export const dbReaderLayerWithoutDependencies = Layer.effect(DbReader, makeDbReader);

/** Provides the reader over the R2 manifest reader and the `BlockServer` block cache. */
export const dbReaderLayer = dbReaderLayerWithoutDependencies.pipe(
  Layer.provide(manifestCacheLayerWithoutDependencies),
  Layer.provide(Layer.mergeAll(manifestReaderLayer, lsmConfigurationLayer, blockCacheLayer)),
);

/** Provides the reader over a fresh in-memory object store. */
export const dbReaderTestLayer = dbReaderLayerWithoutDependencies.pipe(
  Layer.provide(manifestCacheLayerWithoutDependencies),
  Layer.provide(blockCacheLayerWithoutDependencies.pipe(Layer.provide(blockSourceTestLayer))),
  Layer.provide(manifestReaderLayerWithoutDependencies),
  Layer.provide(Layer.mergeAll(objectStoreTestLayer, lsmConfigurationTestLayer())),
);
