import {
  firstBlockForKey,
  blocksOverlappingRange,
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
  changelogAfterSeqOf,
  changelogHoldsChangesAfter,
  resolveVisibleRow,
  type Row,
  type SeqNum,
  type SstCorruptedError,
  type SstHandle,
  sstHandleOwner,
  sstHandleVisibleRange,
  sstMayContainKey,
  DatabaseId,
  Timestamp,
  decodeDataBlock,
  decodeSstMeta,
} from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Bytes, Clock, Context, Data, Effect, Layer, Option, Schema, Stream } from "effect";

import { BlockCache, blockCacheLayer, blockCacheLayerWithoutDependencies } from "./block-cache.ts";
import { blockSourceTestLayer } from "./block-source.ts";
import { lsmConfigurationLayer, lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import { MAX_MEMTABLE_PAGE_BYTES } from "./mem-table.ts";
import { changelogKey } from "./object-keys.ts";
import { ObjectReader, objectReaderFromObjectStoreLayer } from "./object-reader.ts";
import { objectStoreLayer, objectStoreTestLayer } from "./object-store.ts";
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
  /**
   * The manifest to read instead of the newest one.
   *
   * **Details**
   *
   * - **A snapshot** (no `maxLastL0Seq`) is this manifest: when it is gone, the read fails with
   *   `SnapshotExpired`.
   * - **A transaction's read** (with `maxLastL0Seq`) uses it only when the newest manifest is
   *   past the read's bounds, because a flush committed since the transaction began. Any manifest
   *   between the two bounds gives the same answer, and the newest one's SSTs are always stored.
   *   The manifest a transaction begins with is the writer's last flush, which a compaction
   *   replaces and GC later deletes while the writer still names it (D76).
   */
  readonly manifestId: Option.Option<ManifestId>;
  /**
   * The newest `lastL0Seq` the read accepts: the snapshot of a read that asked the writer, or a
   * transaction's read sequence number. A manifest flushed after it may hold only versions newer
   * than the snapshot (a compaction keeps each key's newest), so the read fails with
   * `SnapshotExpired` instead of missing a version. None: any (the read's snapshot is the
   * manifest's own `lastL0Seq`).
   */
  readonly maxLastL0Seq: Option.Option<SeqNum>;
  /**
   * Which newest manifest the read accepts (D67):
   *
   * - `Recent`: one known to have held every flushed row within `manifestRefreshInterval`. For
   *   reads that did not ask the writer (`Flushed`, and `AtLeast` served without it).
   * - `Covering`: any cached one with `lastL0Seq >= minLastL0Seq`. Only for a read whose
   *   `minLastL0Seq` is the writer's `flushedSeq`, just read from it: the writer has then said
   *   exactly which flushed rows exist.
   */
  readonly freshness: "Recent" | "Covering";
  /** The manifest the writer's last flush committed: where a `Covering` load starts looking. */
  readonly manifestHint: Option.Option<ManifestId>;
}

/**
 * A read could not be completed because manifests or blocks were unavailable. Retryable.
 */
export class ReaderUnavailableError extends Data.TaggedError("ReaderUnavailableError")<{
  readonly databaseId: DatabaseId;
  readonly reason: "StorageUnavailable" | "StoredManifestInvalid" | "SnapshotExpired";
  readonly message: string;
  /**
   * For a scan that had already returned rows: the key to resume from (the next range start).
   * Every row before it was returned, and none at or after it (D93).
   */
  readonly resumeFrom?: Uint8Array;
}> {}

/**
 * The change feed cannot serve the request: the database exports no changelog (`Disabled`), or
 * changes after `afterSeq` were already dropped by changelog retention (`Expired`, the consumer fell
 * behind). Not retryable as asked: enable the changelog, or restart the consumer from a snapshot.
 */
export class ChangelogUnavailableError extends Schema.TaggedError<ChangelogUnavailableError>()(
  "ChangelogUnavailableError",
  {
    databaseId: DatabaseId,
    reason: Schema.Literals(["Disabled", "Expired"]),
    message: Schema.String,
  },
) {}

/** Which read lost its blocks: a point read or a scan, and whether it reads a snapshot. */
interface BlockReadContext {
  readonly kind: "Get" | "Scan";
  /** A snapshot by manifest id (no upper bound): its manifest is read exactly. */
  readonly snapshot: boolean;
}

/** Whether `view` reads a snapshot: a named manifest with no upper bound (see `manifestFor`). */
const readsSnapshot = (view: ReadView): boolean =>
  Option.isSome(view.manifestId) && Option.isNone(view.maxLastL0Seq);

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
  /**
   * Up to `limit` changelog rows with sequence numbers in (`afterSeq`, `upToSeq`], in sequence
   * order, from a manifest that covers `upToSeq` (the change feed's flushed part). `upToSeq` must
   * be the writer's `flushedSeq`, just read from it: the manifest is found as for a `Covering` view.
   */
  readonly changelogRows: (
    databaseId: DatabaseId,
    afterSeq: SeqNum,
    upToSeq: SeqNum,
    limit: number,
  ) => Effect.Effect<
    {
      readonly rows: ReadonlyArray<Row>;
      /** False when the page stopped at its row limit or byte bound: more changes follow it. */
      readonly complete: boolean;
    },
    DbReadError | ChangelogUnavailableError,
    DbReaderServices
  >;
  /**
   * The manifest a `Flushed` read at this moment would use without loading one, if any; no I/O.
   * Lets an `AtLeast` read skip the writer when that manifest already covers its sequence number,
   * and ask the writer (cheaper than loading a manifest to find out) when it does not (D67).
   */
  readonly recentManifest: (
    databaseId: DatabaseId,
  ) => Effect.Effect<Option.Option<VersionedManifest>>;
  /**
   * The newest manifest, loaded now: what a new snapshot is taken at. GC keeps it for `gcMinAge`
   * after something replaces it, so a snapshot of it is readable for at least that long from now
   * (a cached manifest may already have been replaced, D76).
   */
  readonly newestManifest: (
    databaseId: DatabaseId,
  ) => Effect.Effect<
    VersionedManifest,
    DatabaseNotFoundError | ReaderUnavailableError,
    DbReaderServices
  >;
  /** The manifest a `Flushed` read at this moment would use. */
  readonly currentManifest: (
    databaseId: DatabaseId,
  ) => Effect.Effect<
    VersionedManifest,
    DatabaseNotFoundError | ReaderUnavailableError,
    DbReaderServices
  >;
}

/** The contextual database reader. */
export class DbReader extends Context.Service<DbReader, IDbReader>()("@shaledb/DbReader") {}

/** Every SST a manifest references, grouped by level, newest level first: each L0 SST, then each run. */
const levels = (manifest: Manifest): ReadonlyArray<ReadonlyArray<SstHandle>> => [
  ...manifest.l0.map((handle) => [handle]),
  ...manifest.sortedRuns.map((run) => run.ssts),
];

/**
 * The L1 budget group a read of `databaseId` is charged to: its `budgetGroup`, or its id. Blocks of
 * a fork's external SSTs are charged to the fork that reads them, not to their owner.
 */
const readerBudgetGroup = (databaseId: DatabaseId, manifest: Manifest): string =>
  Option.getOrElse(manifest.options.budgetGroup, () => databaseId);

/**
 * Most blocks of one SST a scan reads ahead of the merge (D67). A block read from a Worker far from
 * the bucket takes hundreds of milliseconds, so a full page of 1,000 rows over several SSTs needs
 * many reads in flight.
 */
export const SCAN_READ_AHEAD_BLOCKS = 8;

/**
 * How many of an SST's blocks a scan that wants `limit` rows reads at once to begin with: the
 * blocks that hold `limit` rows at the SST's average rows per block, plus one for a range that
 * starts in the middle of a block, at most {@link SCAN_READ_AHEAD_BLOCKS}.
 *
 * **Details**
 *
 * A Worker invocation has six simultaneous subrequests. Reading ahead 8 blocks of every
 * overlapping SST made a 100-row scan over seven SSTs start 51 block reads, and the one or two
 * blocks per SST it needed waited behind the rest. A scan that turns out to need more (rows hidden
 * by its snapshot, many versions of a key, deletes) reads on, {@link SCAN_READ_AHEAD_BLOCKS} at a
 * time.
 *
 * @param limit - Rows the scan wants; none: all, which reads ahead fully.
 * @param sstRowCount - The SST's rows (its metadata's `footer.rowCount`).
 * @param sstBlockCount - The SST's data blocks (its metadata's `index.length`).
 */
export const scanFirstReadAheadBlocks = (
  limit: Option.Option<number>,
  sstRowCount: number,
  sstBlockCount: number,
): number => {
  if (Option.isNone(limit)) return SCAN_READ_AHEAD_BLOCKS;
  const rowsPerBlock = Math.max(1, sstRowCount / Math.max(1, sstBlockCount));
  return Math.max(1, Math.min(SCAN_READ_AHEAD_BLOCKS, Math.ceil(limit.value / rowsPerBlock) + 1));
};

/** Constructs the reader over the contextual manifest cache, manifest reader, and block cache. */
export const makeDbReader = Effect.gen(function* () {
  const manifests = yield* ManifestCache;
  const reader = yield* ManifestReader;
  const blocks = yield* BlockCache;
  const objects = yield* ObjectReader;

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

  /**
   * The manifest a view reads.
   *
   * **Details**
   *
   * - **A snapshot** (a named manifest and no upper bound) reads exactly that manifest.
   * - **Every other view** reads the newest manifest when it is between the view's bounds: the
   *   cached one, refreshed when it is behind `minLastL0Seq`. The newest manifest's SSTs are
   *   always stored, which no older manifest can promise (D76).
   * - **A transaction's read** whose bounds the newest manifest is past (a flush committed since
   *   the transaction began) reads the manifest it began with instead, when that is still stored
   *   and between the bounds.
   */
  const manifestFor = (databaseId: DatabaseId, view: ReadView) =>
    Effect.gen(function* () {
      const named = view.manifestId;
      const loadNamed = (manifestId: ManifestId) =>
        reader.load(databaseId, manifestId).pipe(Effect.catchTags(manifestUnavailable(databaseId)));
      if (Option.isSome(named) && Option.isNone(view.maxLastL0Seq)) {
        const snapshot = yield* loadNamed(named.value);
        if (Option.isNone(snapshot)) {
          return yield* new ReaderUnavailableError({
            databaseId,
            reason: "SnapshotExpired",
            message: `Snapshot manifest ${named.value} of database ${databaseId} no longer exists: an unpinned snapshot is kept only for GC's minimum age, and a pinned one until its checkpoint expires or is deleted. Take a new snapshot (pinned, to read for longer) and read again.`,
          });
        }
        yield* Effect.annotateCurrentSpan({ "manifest.id": named.value });
        return { id: named.value, manifest: snapshot.value };
      }
      // Between the bounds: it holds every row the writer no longer returned, and no flush or
      // compaction after the read's snapshot shaped it (D58, D70).
      const fits = (manifest: Manifest) =>
        manifest.lastL0Seq >= view.minLastL0Seq &&
        (Option.isNone(view.maxLastL0Seq) || manifest.lastL0Seq <= view.maxLastL0Seq.value);
      let current = yield* (
        view.freshness === "Covering"
          ? manifests.covering(databaseId, view.minLastL0Seq, view.manifestHint)
          : manifests.current(databaseId)
      ).pipe(Effect.catchTags(manifestUnavailable(databaseId)));
      if (current.manifest.lastL0Seq < view.minLastL0Seq) {
        // The writer flushed rows the cached manifest predates; its commit happened first.
        current = yield* manifests
          .refresh(databaseId)
          .pipe(Effect.catchTags(manifestUnavailable(databaseId)));
        yield* Effect.annotateCurrentSpan({ "manifest.refreshed": true });
      }
      if (fits(current.manifest)) {
        yield* Effect.annotateCurrentSpan({ "manifest.id": current.id });
        return current;
      }
      if (Option.isSome(named)) {
        const begin = yield* loadNamed(named.value);
        if (Option.isSome(begin) && fits(begin.value)) {
          yield* Effect.annotateCurrentSpan({
            "manifest.id": named.value,
            "manifest.begin_manifest": true,
          });
          return { id: named.value, manifest: begin.value };
        }
      }
      if (
        Option.isSome(view.maxLastL0Seq) &&
        current.manifest.lastL0Seq > view.maxLastL0Seq.value
      ) {
        return yield* new ReaderUnavailableError({
          databaseId,
          reason: "SnapshotExpired",
          message: `Database ${databaseId} has flushed data newer than the read's snapshot (sequence number ${view.maxLastL0Seq.value}), so the snapshot can no longer be read. Read again at a new snapshot.`,
        });
      }
      return yield* new ReaderUnavailableError({
        databaseId,
        reason: "StorageUnavailable",
        message: `Database ${databaseId}'s newest manifest does not yet include sequence number ${view.minLastL0Seq}. Retry the read.`,
      });
    });

  /**
   * What a read that lost blocks partway should do next (docs/errors.md: a way forward), by its
   * kind: a scan may already have returned rows, so it resumes after the last one; a snapshot's
   * SSTs go only when GC collects the snapshot, so a retry of it fails as `SnapshotExpired`.
   */
  const blockFailures = (databaseId: DatabaseId, read: BlockReadContext) => {
    const resume =
      read.kind === "Scan"
        ? " A scan resumes from the key after the last row it returned (from its start when it returned none)."
        : "";
    const missingGuidance = read.snapshot
      ? `The snapshot's manifest is being collected: take a new snapshot (pinned, to read for longer) and read again.${resume}`
      : `Retry the read: it reads the newest manifest.${resume}`;
    return {
      // The cached manifest named an SST that is gone, so it must not serve the retry (D67).
      SstMissingError: (error: { readonly message: string }) =>
        Effect.andThen(
          manifests.forget(databaseId),
          Effect.fail(
            new ReaderUnavailableError({
              databaseId,
              reason: "StorageUnavailable",
              message: `An SST of database ${databaseId} was missing (the manifest changed during the read): ${error.message} ${missingGuidance}`,
            }),
          ),
        ),
      BlockSourceUnavailableError: (error: { readonly message: string }) =>
        Effect.fail(
          new ReaderUnavailableError({
            databaseId,
            reason: "StorageUnavailable",
            message: `Database ${databaseId}'s blocks could not be read: ${error.message} Retry the read.${resume}`,
          }),
        ),
    };
  };

  /** Every version of `key` in one SST at or below `snapshotSeq`, newest first. */
  const sstVersions = (
    databaseId: DatabaseId,
    handle: SstHandle,
    key: Uint8Array,
    snapshotSeq: SeqNum,
    budgetGroup: string,
  ) =>
    Effect.gen(function* () {
      const owner = sstHandleOwner(handle, databaseId);
      const meta = yield* blocks.meta(owner, handle, budgetGroup);
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
        const block = yield* blocks.block(owner, handle, entry, budgetGroup);
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
    const pointRead: BlockReadContext = { kind: "Get", snapshot: readsSnapshot(view) };
    let sstsRead = 0;
    if (!settled(versions)) {
      // The one SST per level that may hold the key, newest level first.
      const candidates = levels(manifest).flatMap((level) => {
        const handle = level.find(
          (candidate) =>
            keyRangeContains(sstHandleVisibleRange(candidate), key) &&
            Bytes.Order(key, candidate.firstKey) >= 0 &&
            Bytes.Order(key, candidate.lastKey) <= 0,
        );
        return handle === undefined ? [] : [handle];
      });
      // Their metadata (index and Bloom filter) all at once: an isolate with a cold cache would
      // otherwise wait on one metadata read per SST in turn before checking any filter (D67). The
      // loop below then reads them from memory.
      const budgetGroup = readerBudgetGroup(databaseId, manifest);
      if (candidates.length > 1) {
        yield* Effect.forEach(
          candidates,
          (handle) => blocks.meta(sstHandleOwner(handle, databaseId), handle, budgetGroup),
          { concurrency: "unbounded", discard: true },
        ).pipe(Effect.catchTags(blockFailures(databaseId, pointRead)));
      }
      for (const handle of candidates) {
        sstsRead++;
        const found = yield* sstVersions(databaseId, handle, key, snapshotSeq, budgetGroup).pipe(
          Effect.catchTags(blockFailures(databaseId, pointRead)),
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
    budgetGroup: string,
    limit: Option.Option<number>,
    read: BlockReadContext,
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
        const meta = yield* blocks.meta(owner, handle, budgetGroup);
        const indexes = blocksOverlappingRange(meta, effective.value);
        // Read ahead while the merge consumes this source: it pulls one source at a time, so
        // without read-ahead a scan waits on one block read at a time (D67).
        const readAhead = (blockIndexes: ReadonlyArray<number>, concurrency: number) =>
          Stream.fromIterable(blockIndexes).pipe(
            Stream.mapEffect(
              (index) => {
                const entry = meta.index[index];
                return entry === undefined
                  ? Effect.succeed([])
                  : Effect.map(blocks.block(owner, handle, entry, budgetGroup), (block) => [
                      ...block.rows(),
                    ]);
              },
              { concurrency: Math.max(1, Math.min(concurrency, blockIndexes.length)) },
            ),
          );
        // First the blocks the scan's limit is likely to need, then, only if the merge asks for
        // more, the rest.
        const first = scanFirstReadAheadBlocks(limit, meta.footer.rowCount, meta.index.length);
        return readAhead(indexes.slice(0, first), first).pipe(
          Stream.concat(
            Stream.suspend(() => readAhead(indexes.slice(first), SCAN_READ_AHEAD_BLOCKS)),
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
          Stream.fromEffect(blockFailures(databaseId, read).SstMissingError(error)),
        BlockSourceUnavailableError: (error) =>
          Stream.fromEffect(blockFailures(databaseId, read).BlockSourceUnavailableError(error)),
      }),
    );

  const scan = (databaseId: DatabaseId, range: KeyRange, view: ReadView, options: ScanOptions) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const { manifest } = yield* manifestFor(databaseId, view);
        const snapshotSeq = Option.getOrElse(view.snapshotSeq, () => manifest.lastL0Seq);
        const scanRead: BlockReadContext = { kind: "Scan", snapshot: readsSnapshot(view) };
        const now = Timestamp.make(yield* Clock.currentTimeMillis);
        const mergeOperator = Option.map(
          manifest.options.mergeOperator,
          (name) => MERGE_OPERATORS[name],
        );
        const memtable = Stream.fromIterable(
          view.memtable.filter((row) => row.seq <= snapshotSeq && keyRangeContains(range, row.key)),
        );
        const budgetGroup = readerBudgetGroup(databaseId, manifest);
        const overlapping = manifest.l0
          .concat(manifest.sortedRuns.flatMap((run) => run.ssts))
          .filter((handle) =>
            Option.exists(intersectKeyRanges(range, sstHandleVisibleRange(handle)), (effective) =>
              keyRangeOverlapsClosed(effective, handle.firstKey, handle.lastKey),
            ),
          );
        // Every overlapping SST's metadata at once: the merge opens its sources one after another,
        // each waiting on a metadata read, before it can yield its first row (D67).
        if (overlapping.length > 1) {
          yield* Effect.forEach(
            overlapping,
            (handle) => blocks.meta(sstHandleOwner(handle, databaseId), handle, budgetGroup),
            { concurrency: "unbounded", discard: true },
          ).pipe(Effect.catchTags(blockFailures(databaseId, scanRead)));
        }
        const ssts = overlapping.map((handle) =>
          sstRows(databaseId, handle, range, snapshotSeq, budgetGroup, options.limit, scanRead),
        );
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

  const recentManifest = (databaseId: DatabaseId) => manifests.recent(databaseId);

  const newestManifest = Effect.fn("DbReader.newestManifest")(function* (databaseId: DatabaseId) {
    return yield* manifests
      .refresh(databaseId)
      .pipe(Effect.catchTags(manifestUnavailable(databaseId)));
  });

  const currentManifest = Effect.fn("DbReader.currentManifest")(function* (databaseId: DatabaseId) {
    return yield* manifests
      .current(databaseId)
      .pipe(Effect.catchTags(manifestUnavailable(databaseId)));
  });

  const changelogRows = Effect.fn("DbReader.changelogRows")(function* (
    databaseId: DatabaseId,
    afterSeq: SeqNum,
    upToSeq: SeqNum,
    limit: number,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId, seq: afterSeq });
    const { manifest } = yield* manifestFor(databaseId, {
      minLastL0Seq: upToSeq,
      snapshotSeq: Option.none(),
      memtable: [],
      manifestId: Option.none(),
      maxLastL0Seq: Option.none(),
      freshness: "Covering",
      manifestHint: Option.none(),
    });
    if (Option.isNone(manifest.options.changelog)) {
      return yield* new ChangelogUnavailableError({
        databaseId,
        reason: "Disabled",
        message: `Database ${databaseId} exports no changelog, so flushed changes cannot be read. Create the database with a changelog retention to use the change feed.`,
      });
    }
    // Only when the changelog provably holds every change after `afterSeq` (D80): retention may
    // have pruned them, or they may belong to the source of a fork.
    if (!changelogHoldsChangesAfter(manifest, afterSeq)) {
      const heldAfter = changelogAfterSeqOf(manifest);
      return yield* new ChangelogUnavailableError({
        databaseId,
        reason: "Expired",
        message: `Changes of database ${databaseId} after sequence number ${afterSeq} are no longer in its changelog: it holds only the changes after sequence number ${heldAfter} (older ones were dropped by changelog retention, or, for a fork or split successor, belong to its source's feed). Restart the consumer from a snapshot, or read the source's feed up to ${heldAfter}.`,
      });
    }
    const entries = [...manifest.changelog]
      .filter((entry) => entry.lastSeq > afterSeq)
      .sort((left, right) => left.firstSeq - right.firstSeq);
    // Changelog SSTs are read sequentially and once per consumer: straight from R2, not cached.
    const unavailable = (key: string) => (error: { readonly message: string }) =>
      new ReaderUnavailableError({
        databaseId,
        reason: "StorageUnavailable",
        message: `Changelog SST ${key} of database ${databaseId} could not be read: ${error.message}`,
      });
    const readRange = (key: string, offset: number, length: number) =>
      objects.getRange(key, { offset, length }).pipe(
        Effect.mapError(unavailable(key)),
        Effect.flatMap((bytes) =>
          Option.isSome(bytes) && bytes.value.length === length
            ? Effect.succeed(bytes.value)
            : Effect.fail(
                unavailable(key)({ message: "it no longer exists (pruned by retention)" }),
              ),
        ),
      );
    const rows: Array<Row> = [];
    let bytes = 0;
    // A page holds at most `limit` rows' worth of batches and MAX_MEMTABLE_PAGE_BYTES (D93), and
    // never splits a batch: a batch that would pass the byte bound is left for the next page,
    // unless it is the page's first (a batch holds at most 8 MiB, so it fits alone).
    let lastSeq = Option.none<SeqNum>();
    let batchStart = 0;
    let bytesBeforeBatch = 0;
    let cut = false;
    entries: for (const entry of entries) {
      const key = changelogKey(databaseId, entry.lastSeq, entry.sst.id);
      const meta = yield* Effect.fromResult(
        decodeSstMeta(
          entry.sst.id,
          yield* readRange(key, entry.sst.metaOffset, entry.sst.metaLength),
        ),
      );
      for (const indexEntry of meta.index) {
        const block = yield* Effect.fromResult(
          decodeDataBlock(yield* readRange(key, indexEntry.offset, indexEntry.length), {
            sstId: entry.sst.id,
            offset: indexEntry.offset,
          }),
        );
        for (const row of block.rows()) {
          if (row.seq <= afterSeq || row.seq > upToSeq) continue;
          if (!Option.contains(lastSeq, row.seq)) {
            if (rows.length >= limit) {
              cut = true;
              break entries;
            }
            batchStart = rows.length;
            bytesBeforeBatch = bytes;
            lastSeq = Option.some(row.seq);
          }
          bytes +=
            row.key.length +
            (row.value._tag === "Value"
              ? row.value.bytes.length
              : row.value._tag === "Merge"
                ? row.value.operand.length
                : 0);
          if (bytes > MAX_MEMTABLE_PAGE_BYTES && batchStart > 0) {
            // This batch would pass the bound: it starts the next page.
            rows.length = batchStart;
            bytes = bytesBeforeBatch;
            cut = true;
            break entries;
          }
          rows.push(row);
        }
      }
    }
    yield* Effect.annotateCurrentSpan({
      "changelog.rows": rows.length,
      "changelog.ssts": entries.length,
      "changelog.page_cut": cut,
    });
    return { rows, complete: !cut };
  });

  return DbReader.of({
    getRow,
    scan,
    changelogRows,
    currentManifest,
    newestManifest,
    recentManifest,
  });
});

/** Provides the reader, leaving its caches and manifest reader visible. */
export const dbReaderLayerWithoutDependencies = Layer.effect(DbReader, makeDbReader);

/** Provides the reader over the R2 manifest reader and the `BlockServer` block cache. */
export const dbReaderLayer = dbReaderLayerWithoutDependencies.pipe(
  Layer.provide(manifestCacheLayerWithoutDependencies),
  Layer.provide(
    Layer.mergeAll(
      manifestReaderLayer,
      lsmConfigurationLayer,
      blockCacheLayer,
      objectReaderFromObjectStoreLayer.pipe(Layer.provide(objectStoreLayer)),
    ),
  ),
);

/** Provides the reader over a fresh in-memory object store. */
export const dbReaderTestLayer = dbReaderLayerWithoutDependencies.pipe(
  Layer.provide(manifestCacheLayerWithoutDependencies),
  Layer.provide(blockCacheLayerWithoutDependencies.pipe(Layer.provide(blockSourceTestLayer))),
  Layer.provide(
    Layer.mergeAll(manifestReaderLayerWithoutDependencies, objectReaderFromObjectStoreLayer),
  ),
  Layer.provide(Layer.mergeAll(objectStoreTestLayer, lsmConfigurationTestLayer())),
);
