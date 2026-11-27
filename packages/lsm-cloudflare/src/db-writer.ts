import {
  keepReachableVersions,
  changelogRowOrder,
  collapseWriteBatch,
  commitL0Flush,
  type DatabaseId,
  type DatabaseRemovedError,
  DatabaseSealedError,
  fenceWriter,
  generateCheckpointId,
  holdForSuccessors as holdForSuccessors_,
  type KeyOutOfRangeError,
  type KeyRange,
  type Manifest,
  ManifestId,
  MERGE_OPERATORS,
  type Row,
  sealManifest,
  SeqNum,
  successorHoldsOf,
  type SuccessorHold,
  Timestamp,
  type WriteBatch,
  type WriteBatchInvalidError,
  type WriteReceipt,
  WriterEpoch,
  WriterFencedError,
  TransactionConflictError,
  type TransactionCommit,
} from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import {
  Cause,
  Clock,
  Context,
  Effect,
  Layer,
  Metric,
  Option,
  Ref,
  Result,
  Semaphore,
  Stream,
} from "effect";

import { LsmConfiguration, lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import {
  DatabaseIdMismatchError,
  DatabaseNotFoundError,
  WriterUnavailableError,
  WriteStalledError,
} from "./lsm-errors.ts";
import {
  MemTable,
  type MemTableCursor,
  memTableTestLayer,
  memTableWriteBytes,
} from "./mem-table.ts";
import type { ManifestReadError, VersionedManifest } from "./manifest-reader.ts";
import {
  ManifestStore,
  manifestStoreTestLayer,
  type ManifestWriteError,
} from "./manifest-store.ts";
import { changelogKey, sstKey } from "./object-keys.ts";
import { SstStore, sstStoreLayerWithoutDependencies } from "./sst-store.ts";
import { BackgroundWork, backgroundWorkTestLayer } from "./background-work.ts";
import { CompactionTrigger, compactionTriggerTestLayer } from "./compaction-trigger.ts";
import { DurableObjectAlarm, durableObjectAlarmTestLayer } from "./durable-object-alarm.ts";

/** What a `Latest` read learns from the writer about one key. */
export interface MemTableProbe {
  /** The key's unflushed versions at or below `snapshotSeq`, newest first. */
  readonly versions: ReadonlyArray<Row>;
  /** The newest committed sequence number: the read's snapshot. */
  readonly snapshotSeq: SeqNum;
  /**
   * Newest sequence number committed to the manifest by a flush. Rows at or below it may already be
   * gone from the memtable, so the reader needs a manifest with `lastL0Seq >= flushedSeq`.
   */
  readonly flushedSeq: SeqNum;
  /**
   * The manifest the writer's last flush committed, which covers `flushedSeq`: a reader whose cache
   * is behind loads it directly instead of searching for the newest (D67). None before any flush.
   */
  readonly flushedManifestId: Option.Option<ManifestId>;
  /** The database's successors when it is sealed, so a router can tell its map is stale. */
  readonly sealedSuccessors: Option.Option<ReadonlyArray<DatabaseId>>;
}

/**
 * What a transaction read at `readSeq` learns from the writer about one key: every version still in
 * SQLite (retained or unflushed) at or below `readSeq`, and the horizon below which rows are gone.
 */
export interface MemTableProbeAt {
  /** The key's versions with `retainedFromSeq < seq <= readSeq`, newest first. */
  readonly versions: ReadonlyArray<Row>;
  /** Rows at or below it are gone from SQLite: the read needs a manifest at or past it. */
  readonly retainedFromSeq: SeqNum;
}

/** One page of a memtable scan. */
export interface MemTableScanPage {
  readonly rows: ReadonlyArray<Row>;
  readonly snapshotSeq: SeqNum;
  readonly flushedSeq: SeqNum;
  /** Rows at or below it are gone from SQLite (see `WriterMeta.retainedFromSeq`). */
  readonly retainedFromSeq: SeqNum;
  /** The manifest the last flush committed (see `MemTableProbe.flushedManifestId`). */
  readonly flushedManifestId: Option.Option<ManifestId>;
  /** The database's successors when it is sealed (see `MemTableProbe.sealedSuccessors`). */
  readonly sealedSuccessors: Option.Option<ReadonlyArray<DatabaseId>>;
  /** Where the next page starts, or none on the last page. */
  readonly nextCursor: Option.Option<MemTableCursor>;
}

/** Unflushed changes in sequence order, for change data capture. */
export interface ChangesPage {
  readonly rows: ReadonlyArray<Row>;
  /** Newest committed sequence number. */
  readonly lastSeq: SeqNum;
  readonly flushedSeq: SeqNum;
}

/** The writer's state, for operators and tests. */
export interface WriterStatus {
  readonly databaseId: DatabaseId;
  readonly writerEpoch: WriterEpoch;
  readonly lastSeq: SeqNum;
  readonly flushedSeq: SeqNum;
  /** Every row above it is still in SQLite for transactions to check (D58). */
  readonly retainedFromSeq: SeqNum;
  /** The manifest that made `flushedSeq` durable, or none for writer state from before D58. */
  readonly flushedManifestId: Option.Option<ManifestId>;
  readonly unflushedBytes: number;
  readonly unflushedRows: number;
  readonly l0Count: number;
  readonly sealed: boolean;
}

/**
 * What a seal left: the sequence number durable at the seal and, when the seal took them, each
 * successor's hold on the sealed manifest (`holdForSuccessors`).
 */
export interface SealReceipt {
  readonly flushedSeq: SeqNum;
  readonly successorHolds: ReadonlyArray<SuccessorHold>;
}

/** Every failure a writer operation can report. */
export type DbWriterError =
  | DatabaseNotFoundError
  | DatabaseIdMismatchError
  | DatabaseSealedError
  | WriterFencedError
  | TransactionConflictError
  | WriteStalledError
  | WriteBatchInvalidError
  | KeyOutOfRangeError
  | WriterUnavailableError;

/**
 * The database's single writer: it commits write batches to the memtable (the WAL), answers
 * `Latest` probes, and flushes the memtable to L0.
 */
export interface IDbWriter {
  /**
   * Commits a batch atomically with one sequence number. Durable when it returns.
   *
   * @returns The receipt, or a `DbWriterError`; on failure nothing was written.
   */
  readonly write: (
    databaseId: DatabaseId,
    batch: WriteBatch,
  ) => Effect.Effect<WriteReceipt, DbWriterError, RuntimeContext>;
  /** The key's unflushed versions and the sequence numbers a `Latest` read needs. */
  readonly probe: (
    databaseId: DatabaseId,
    key: Uint8Array,
  ) => Effect.Effect<MemTableProbe, DbWriterError, RuntimeContext>;
  /**
   * A transaction's read of one key at `readSeq`: its retained and unflushed versions at or below
   * `readSeq`, with the retained horizon read atomically (D58).
   */
  readonly probeAt: (
    databaseId: DatabaseId,
    key: Uint8Array,
    readSeq: SeqNum,
  ) => Effect.Effect<MemTableProbeAt, DbWriterError, RuntimeContext>;
  /**
   * One page of rows in `range` at `snapshotSeq` (default: newest): unflushed rows, or with
   * `includeRetained` (a transaction's scan) every row still in SQLite.
   */
  readonly scan: (
    databaseId: DatabaseId,
    request: {
      readonly range: KeyRange;
      readonly snapshotSeq: Option.Option<SeqNum>;
      readonly cursor: Option.Option<MemTableCursor>;
      readonly limit: number;
      readonly includeRetained: boolean;
    },
  ) => Effect.Effect<MemTableScanPage, DbWriterError, RuntimeContext>;
  /** Flushes every committed row to L0 now. @returns The new `flushedSeq`. */
  readonly flush: (databaseId: DatabaseId) => Effect.Effect<SeqNum, DbWriterError, RuntimeContext>;
  /**
   * Commits a transaction's writes if no other commit since its `readSeq` conflicts with it (its
   * written keys, and for `SerializableSnapshot` its read keys and ranges); the check and the commit
   * hold the commit lock together.
   */
  readonly commitTransaction: (
    databaseId: DatabaseId,
    commit: TransactionCommit,
  ) => Effect.Effect<WriteReceipt, DbWriterError, RuntimeContext>;
  /** Unflushed changes after `afterSeq`, in sequence order. */
  readonly changes: (
    databaseId: DatabaseId,
    afterSeq: SeqNum,
    limit: number,
  ) => Effect.Effect<ChangesPage, DbWriterError, RuntimeContext>;
  /** The writer's state. */
  readonly status: (
    databaseId: DatabaseId,
  ) => Effect.Effect<WriterStatus, DbWriterError, RuntimeContext>;
  /**
   * Flushes everything, then seals the database with `successors` (which fences this writer). Every
   * write acknowledged before the seal is in the sealed manifest; later writes fail with
   * `DatabaseSealedError`. Safe to repeat.
   */
  readonly seal: (
    databaseId: DatabaseId,
    successors: ReadonlyArray<DatabaseId>,
    holdForSuccessors: boolean,
  ) => Effect.Effect<SealReceipt, DbWriterError, RuntimeContext>;
  /**
   * Deletes the writer's state (the memtable and its sequence state) and its alarm, for a database
   * being removed; the next request starts a new writer, which finds the database gone.
   */
  readonly destroy: (databaseId: DatabaseId) => Effect.Effect<void, DbWriterError, RuntimeContext>;
  /** The alarm handler: flushes when a trigger is due and reschedules. Never fails. */
  readonly runAlarm: Effect.Effect<void, never, RuntimeContext>;
}

/** The contextual database writer. */
export class DbWriter extends Context.Service<DbWriter, IDbWriter>()("@shaledb/DbWriter") {}

/** Bytes flushed to L0 (SlateDB RFC-0021 parity). */
export const flushBytes = Metric.counter("lsm_flush_bytes", {
  description: "Bytes of L0 SSTs written by memtable flushes",
});

/** Writes refused by backpressure. */
export const writeStalls = Metric.counter("lsm_write_stalls", {
  description: "Writes refused with WriteStalledError",
});

/**
 * How many times the end of a flush scans the memtable for fresh counters before it gives up and
 * keeps the counters it has. Each further scan needs a commit to land during the one before.
 */
const COUNTER_REFRESH_ATTEMPTS = 3;

/**
 * The writer's figures for its unflushed rows (D74), in one value so that a commit changes all of
 * them in one indivisible step.
 *
 * **Details**
 *
 * The counters are never below what SQLite holds: a commit adds exactly its rows, and only a scan
 * installs lower figures, and only when no commit ran while it scanned (`generation`). A counter
 * above SQLite (a refresh that gave up, or a failed flush) is corrected by the next flush, and makes
 * that flush come sooner, never later, so no row is left unflushed past `maxL0Age`.
 */
interface UnflushedCounters {
  /** Bytes of the unflushed rows, as `memTableWriteBytes` counts them. */
  readonly bytes: number;
  readonly rows: number;
  /** Commit time of the oldest unflushed row; some whenever a row is unflushed. */
  readonly oldest: Option.Option<Timestamp>;
  /**
   * Advanced when a commit starts and again when it ends. A scan whose generation changed before
   * it is installed may have missed, or counted, a commit's rows.
   */
  readonly generation: number;
  /** A commit has started and not ended: its rows may or may not be in a scan. */
  readonly committing: boolean;
}

const NO_UNFLUSHED_ROWS: UnflushedCounters = {
  bytes: 0,
  rows: 0,
  oldest: Option.none(),
  generation: 0,
  committing: false,
};

type WriterPhase =
  | { readonly _tag: "Uninitialized" }
  | {
      readonly _tag: "Ready";
      readonly databaseId: DatabaseId;
      readonly writerEpoch: WriterEpoch;
      readonly manifest: VersionedManifest;
      readonly sealed: Option.Option<ReadonlyArray<DatabaseId>>;
    }
  | { readonly _tag: "Fenced"; readonly databaseId: DatabaseId; readonly error: WriterFencedError };

/** Constructs the writer over the contextual memtable, manifest store, SST store, alarm, and settings. */
export const makeDbWriter = Effect.gen(function* () {
  const memTable = yield* MemTable;
  const manifests = yield* ManifestStore;
  const ssts = yield* SstStore;
  const alarm = yield* DurableObjectAlarm;
  const settings = yield* LsmConfiguration;
  const compaction = yield* CompactionTrigger;
  const background = yield* BackgroundWork;

  /**
   * Wakes the database's compactor without waiting for it. A compactor that was idle takes
   * seconds to answer its first call, and a flush holds the flush lock (a seal, the commit lock
   * too), so the wake is background work. A lost wake is repeated by the next flush that leaves
   * enough L0 SSTs.
   */
  const wakeCompactor = (databaseId: DatabaseId) =>
    background.run("wakeCompactor", compaction.wake(databaseId));

  const phase = yield* Ref.make<WriterPhase>({ _tag: "Uninitialized" });
  // The unflushed rows' bytes and count, and the commit time of the oldest: everything the flush
  // scheduler, the alarm, backpressure, and `status` need. Kept in memory so that none of them
  // scans the memtable: read from SQLite at start and after each flush, and advanced by each
  // commit with the same per-row figure SQLite sums (`memTableWriteBytes`). A scan is installed
  // only if no commit ran during it, so the counters equal what a scan would report (D74).
  const unflushed = yield* Ref.make(NO_UNFLUSHED_ROWS);
  const initLock = yield* Semaphore.make(1);
  const commitLock = yield* Semaphore.make(1);
  const flushLock = yield* Semaphore.make(1);

  const unavailable =
    (databaseId: DatabaseId, operation: string) => (error: { readonly message: string }) =>
      new WriterUnavailableError({
        databaseId,
        operation,
        reason: "StorageUnavailable",
        message: `The writer of database ${databaseId} could not complete ${operation}: ${error.message}`,
      });

  /**
   * `catchTags` handlers that translate manifest failures to `WriterUnavailableError`; an edit's own
   * errors pass through unchanged.
   */
  const manifestFailures = (databaseId: DatabaseId, operation: string) => ({
    ManifestReadError: (error: ManifestReadError) =>
      Effect.fail(
        new WriterUnavailableError({
          databaseId,
          operation,
          reason:
            error.reason === "StoredManifestInvalid" ? "StoredDataInvalid" : "StorageUnavailable",
          message: `The writer of database ${databaseId} could not read the manifest during ${operation}: ${error.message}`,
        }),
      ),
    ManifestWriteError: (error: ManifestWriteError) =>
      Effect.fail(
        new WriterUnavailableError({
          databaseId,
          operation,
          reason:
            error.reason === "ContentionPersisted" ? "ManifestContention" : "StorageUnavailable",
          message: `The writer of database ${databaseId} could not write the manifest during ${operation}: ${error.message}`,
        }),
      ),
  });

  /**
   * Replaces the counters with a scan of SQLite, unless a commit ran during the scan: then it scans
   * again, up to {@link COUNTER_REFRESH_ATTEMPTS} times.
   *
   * **Details**
   *
   * Commits take the commit lock, which a flush must not take inside its own (a seal takes them in
   * the other order), so a commit can land between the scan and the install. Installing anyway
   * could drop that commit's row from the counters, leaving no alarm for it. When every attempt is
   * overtaken, the counters are kept: they are then above SQLite (they still hold the rows this
   * flush wrote), which only brings the next flush forward, and that flush refreshes them.
   */
  const refreshUnflushedCounters = (databaseId: DatabaseId) =>
    Effect.gen(function* () {
      for (let attempt = 1; attempt <= COUNTER_REFRESH_ATTEMPTS; attempt++) {
        const before = yield* Ref.get(unflushed);
        const stats = yield* memTable.stats.pipe(Effect.mapError(unavailable(databaseId, "stats")));
        const installed = yield* Ref.modify(
          unflushed,
          (current): readonly [boolean, UnflushedCounters] =>
            current.generation === before.generation && !current.committing
              ? [
                  true,
                  {
                    ...current,
                    bytes: stats.unflushedBytes,
                    rows: stats.rowCount,
                    oldest: stats.oldestCreateTs,
                  },
                ]
              : [false, current],
        );
        if (installed) return;
        // Let the commit that overtook the scan finish before scanning again.
        yield* Effect.yieldNow;
      }
      yield* Effect.annotateCurrentSpan({ "writer.counters_refresh_overtaken": true });
    });

  /**
   * For a flush that found no row to write: refreshes counters that still show unflushed rows.
   *
   * **Details**
   *
   * Counters left above SQLite (a refresh that gave up, or a flush that failed after recording its
   * rows flushed) would otherwise keep the alarm due: it would fire, find nothing to flush, and be
   * set for now again, the loop D74 removed. Counters showing nothing need no scan, since they are
   * never below SQLite.
   */
  const settleCountersWithNothingToFlush = (databaseId: DatabaseId) =>
    Effect.gen(function* () {
      const counters = yield* Ref.get(unflushed);
      if (counters.rows > 0 || Option.isSome(counters.oldest)) {
        yield* refreshUnflushedCounters(databaseId);
      }
    });

  /**
   * Schedules the flush alarm for when the oldest unflushed row reaches `maxL0Age`, or now once
   * `l0SstSize` bytes are unflushed. Reads only the in-memory counters: no memtable scan per write
   * (D61).
   */
  const scheduleFlush = (_databaseId: DatabaseId) =>
    Effect.gen(function* () {
      const counters = yield* Ref.get(unflushed);
      if (Option.isNone(counters.oldest)) return;
      const now = yield* Clock.currentTimeMillis;
      const due =
        counters.bytes >= settings.l0SstSize ? now : counters.oldest.value + settings.maxL0Age;
      const scheduled = yield* alarm.scheduled;
      if (Option.isNone(scheduled) || scheduled.value > due) yield* alarm.scheduleAt(due);
    });

  /**
   * Starts the writer for `databaseId` once per instance: fences in with a new writer epoch, seeds
   * the sequence oracle and clock from the manifest, and drops rows a crash left behind after their
   * flush committed.
   *
   * **Details**
   *
   * Every request calls this first. Once the writer is ready for `databaseId` it answers from
   * memory, with no lock and no span: only an actual start is a `DbWriter.start` span.
   */
  const ensureReady = (databaseId: DatabaseId) =>
    Effect.flatMap(Ref.get(phase), (current) =>
      current._tag === "Ready" && current.databaseId === databaseId
        ? Effect.succeed(current)
        : startWriter(databaseId),
    );

  const startWriter = (databaseId: DatabaseId) =>
    initLock
      .withPermits(1)(
        Effect.gen(function* () {
          const current = yield* Ref.get(phase);
          if (current._tag === "Fenced") {
            if (current.databaseId !== databaseId)
              return yield* mismatch(databaseId, current.databaseId);
            return yield* Effect.fail(current.error);
          }
          if (current._tag === "Ready") {
            if (current.databaseId !== databaseId)
              return yield* mismatch(databaseId, current.databaseId);
            return current;
          }
          const meta = yield* memTable.meta.pipe(Effect.mapError(unavailable(databaseId, "start")));
          if (Option.isSome(meta.databaseId) && meta.databaseId.value !== databaseId) {
            return yield* mismatch(databaseId, meta.databaseId.value);
          }
          const fenced = yield* manifests
            .update(databaseId, (manifest) =>
              manifest.state._tag === "Sealed" ? Result.succeed(manifest) : fenceWriter(manifest),
            )
            .pipe(
              Effect.catchTags(manifestFailures(databaseId, "start")),
              // A database being removed is gone for its writer.
              Effect.catchTags({
                DatabaseRemovedError: (error) =>
                  Effect.fail(new DatabaseNotFoundError({ databaseId, message: error.message })),
              }),
            );
          const sealed =
            fenced.manifest.state._tag === "Sealed"
              ? Option.some(fenced.manifest.state.successors)
              : Option.none();
          const writerEpoch = fenced.manifest.writerEpoch;
          yield* memTable
            .setIdentity(databaseId, writerEpoch)
            .pipe(Effect.mapError(unavailable(databaseId, "start")));
          yield* memTable
            .seed({ seq: fenced.manifest.lastL0Seq, clockTick: fenced.manifest.lastL0ClockTick })
            .pipe(Effect.mapError(unavailable(databaseId, "start")));
          // Rows at or below lastL0Seq are already in L0 (a crash may have hit between the commit
          // and recording it): record them as flushed, by the fenced manifest, and keep them for
          // transactions like any flushed rows (D58).
          yield* memTable
            .markFlushed(
              fenced.manifest.lastL0Seq,
              Option.some(fenced.id),
              yield* Clock.currentTimeMillis,
            )
            .pipe(Effect.mapError(unavailable(databaseId, "start")));
          yield* refreshUnflushedCounters(databaseId);
          const ready: WriterPhase = {
            _tag: "Ready",
            databaseId,
            writerEpoch,
            manifest: fenced,
            sealed,
          };
          yield* Ref.set(phase, ready);
          yield* Effect.annotateCurrentSpan({
            "database.id": databaseId,
            "writer.epoch": writerEpoch,
          });
          if (Option.isNone(sealed)) yield* scheduleFlush(databaseId);
          return ready;
        }),
      )
      .pipe(Effect.withSpan("DbWriter.start"));

  const mismatch = (databaseId: DatabaseId, served: DatabaseId) =>
    Effect.fail(
      new DatabaseIdMismatchError({
        databaseId,
        servedDatabaseId: served,
        message: `A request for database ${databaseId} reached the writer of database ${served}. Route requests by database id.`,
      }),
    );

  /** Whether two successor lists name the same databases in the same order. */
  const sameSuccessors = (left: ReadonlyArray<DatabaseId>, right: ReadonlyArray<DatabaseId>) =>
    left.length === right.length && left.every((id, index) => id === right[index]);

  const sealedError = (databaseId: DatabaseId, successors: ReadonlyArray<DatabaseId>) =>
    new DatabaseSealedError({
      databaseId,
      successors,
      message: `Database ${databaseId} is sealed and accepts no writes${
        successors.length > 0 ? `; its successors are ${successors.join(", ")}` : ""
      }. Send the write to the successor whose key range holds the key.`,
    });

  const writableReady = (databaseId: DatabaseId) =>
    Effect.gen(function* () {
      const ready = yield* ensureReady(databaseId);
      if (Option.isSome(ready.sealed))
        return yield* Effect.fail(sealedError(databaseId, ready.sealed.value));
      return ready;
    });

  /** Marks this instance fenced: a newer writer serves the database. */
  const markFenced = (databaseId: DatabaseId, error: WriterFencedError) =>
    Ref.set(phase, { _tag: "Fenced", databaseId, error });

  /**
   * Flushes every committed row and, in the same manifest commit, applies `alsoCommit` to the flushed
   * manifest (the seal uses it to seal in the flush's own commit, D57).
   *
   * @returns The flushed sequence number, and the manifest the flush committed (none when there was
   *   nothing to flush, so `alsoCommit` was not applied).
   */
  const flushThen = (
    databaseId: DatabaseId,
    alsoCommit: (
      manifest: Manifest,
      manifestId: ManifestId,
    ) => Result.Result<Manifest, WriterFencedError | DatabaseRemovedError>,
  ) =>
    flushLock
      .withPermits(1)(
        Effect.gen(function* () {
          const ready = yield* ensureReady(databaseId);
          const meta = yield* memTable.meta.pipe(Effect.mapError(unavailable(databaseId, "flush")));
          const cut = meta.lastSeq;
          yield* Effect.annotateCurrentSpan({ "database.id": databaseId, seq: cut });
          const nothingCommitted = Option.none<VersionedManifest>();
          if (cut <= meta.flushedSeq) {
            yield* settleCountersWithNothingToFlush(databaseId);
            return { flushedSeq: meta.flushedSeq, committed: nothingCommitted };
          }
          const options = ready.manifest.manifest.options;
          const written = yield* ssts
            .write({
              owner: databaseId,
              kind: "Compacted",
              objectKey: (id) => sstKey(databaseId, id),
              // Only the versions a read of this SST can reach: every such read is at a snapshot
              // at or after `cut`, since one at an older snapshot takes these rows from this
              // writer (they stay in SQLite) or reads an older manifest. A key rewritten many
              // times since the last flush is then one row, not one per write (D70).
              rows: keepReachableVersions(memTable.rowsUpTo(meta.flushedSeq, cut)),
            })
            .pipe(
              Effect.catchTags({
                MemTableError: (error) => Effect.fail(unavailable(databaseId, "flush")(error)),
                ObjectStoreUnavailableError: (error) =>
                  Effect.fail(unavailable(databaseId, "flush")(error)),
              }),
            );
          if (Option.isNone(written)) {
            // No row in (flushedSeq, cut] (sequence numbers without rows): just record it.
            yield* memTable
              .markFlushed(cut, Option.none(), yield* Clock.currentTimeMillis)
              .pipe(Effect.mapError(unavailable(databaseId, "flush")));
            yield* settleCountersWithNothingToFlush(databaseId);
            return { flushedSeq: cut, committed: nothingCommitted };
          }
          const changelog = Option.isSome(options.changelog)
            ? yield* exportChangelog(databaseId, meta.flushedSeq, cut)
            : Option.none();
          // From the writer's own view: usually nothing else committed since, so the commit is one
          // create with no load (D56).
          const committed = yield* manifests
            .updateFrom(ready.manifest, (manifest, manifestId) =>
              Result.flatMap(
                commitL0Flush(manifest, {
                  writerEpoch: ready.writerEpoch,
                  sst: written.value.handle,
                  lastSeq: cut,
                  clockTick: meta.clockTick,
                  changelog,
                }),
                (flushed) => alsoCommit(flushed, manifestId),
              ),
            )
            .pipe(
              Effect.catchTags(manifestFailures(databaseId, "flush")),
              // The upload outlasted GC's boundary (a stall longer than `gcMinAge`): the rows are
              // still in SQLite, so the next flush writes them to a new SST; this one is an orphan.
              Effect.catchTags({
                SstBelowGcBoundaryError: (error) =>
                  Effect.fail(unavailable(databaseId, "flush")(error)),
                // A database being removed is gone for its writer.
                DatabaseRemovedError: (error) =>
                  Effect.fail(new DatabaseNotFoundError({ databaseId, message: error.message })),
              }),
              Effect.tapError((error) =>
                error._tag === "WriterFencedError" ? markFenced(databaseId, error) : Effect.void,
              ),
            );
          // Recorded only after the manifest commit: a crash before this line leaves rows the next
          // start records as flushed (they are in L0), or that a later flush writes again with the
          // same sequence numbers, which merging deduplicates. The rows stay for transactions until
          // a trim, which never removes the newest flush (D58).
          yield* memTable
            .markFlushed(cut, Option.some(committed.id), yield* Clock.currentTimeMillis)
            .pipe(Effect.mapError(unavailable(databaseId, "flush")));
          yield* memTable
            .trim({
              nowMs: yield* Clock.currentTimeMillis,
              retentionMs: settings.transactionRetentionMs,
              maxRetainedBytes: settings.maxRetainedBytes,
            })
            .pipe(Effect.mapError(unavailable(databaseId, "flush")));
          yield* Ref.update(phase, (current) =>
            current._tag === "Ready" ? { ...current, manifest: committed } : current,
          );
          yield* refreshUnflushedCounters(databaseId);
          yield* Metric.update(flushBytes, written.value.info.sizeBytes);
          // Enough L0 SSTs to compact: wake the compactor (an idle database has no compactor alarm).
          if (committed.manifest.l0.length >= settings.minCompactionSources) {
            yield* wakeCompactor(databaseId);
          }
          yield* Effect.annotateCurrentSpan({
            "sst.id": written.value.handle.id,
            "sst.size": written.value.info.sizeBytes,
            "manifest.id": committed.id,
          });
          return { flushedSeq: cut, committed: Option.some(committed) };
        }),
      )
      .pipe(Effect.withSpan("DbWriter.flush"));

  const flushNow = (databaseId: DatabaseId) =>
    flushThen(databaseId, (manifest) => Result.succeed(manifest)).pipe(
      Effect.map((flushed) => flushed.flushedSeq),
    );

  /** Exports the flush's rows in sequence order as a changelog SST (change data capture). */
  const exportChangelog = (databaseId: DatabaseId, afterSeq: SeqNum, cut: SeqNum) =>
    Effect.gen(function* () {
      const rows = Stream.paginate(afterSeq, (from) =>
        memTable.rowsBySeq(from, cut, 1024).pipe(
          Effect.map(({ rows, complete }) => {
            const last = rows[rows.length - 1];
            // A page holds whole batches (D93), so the next one starts after its last sequence.
            return [
              rows,
              !complete && last !== undefined ? Option.some(last.seq) : Option.none(),
            ] as const;
          }),
        ),
      ).pipe(Stream.filter((row) => row.seq > afterSeq));
      const written = yield* ssts
        .write({
          owner: databaseId,
          kind: "Changelog",
          objectKey: (id) => changelogKey(databaseId, cut, id),
          rows: rows.pipe(Stream.rechunk(1024)),
        })
        .pipe(
          Effect.catchTags({
            MemTableError: (error) =>
              Effect.fail(unavailable(databaseId, "exportChangelog")(error)),
            ObjectStoreUnavailableError: (error) =>
              Effect.fail(unavailable(databaseId, "exportChangelog")(error)),
          }),
        );
      const now = Timestamp.make(yield* Clock.currentTimeMillis);
      return Option.map(written, (sst) => ({
        sst: sst.handle,
        firstSeq: sst.info.footer.minSeq,
        lastSeq: sst.info.footer.maxSeq,
        createdAt: now,
      }));
    }).pipe(Effect.withSpan("ChangelogExport.export"));

  /**
   * Commits a batch under the commit lock. `beforeCommit` runs inside the lock, after the writer is
   * ready and backpressure allows the write, so a transaction's conflict check sees every commit
   * before its own.
   */
  const commitBatch = <E>(
    databaseId: DatabaseId,
    batch: WriteBatch,
    beforeCommit: Effect.Effect<void, E>,
  ) =>
    Effect.gen(function* () {
      const receipt = yield* commitLock
        .withPermits(1)(
          Effect.gen(function* () {
            let ready = yield* writableReady(databaseId);
            const { bytes } = yield* Ref.get(unflushed);
            if (bytes >= settings.maxUnflushedBytes) {
              yield* Metric.update(writeStalls, 1);
              yield* Effect.annotateCurrentSpan({ "writer.stall_reason": "UnflushedBytes" });
              yield* scheduleFlush(databaseId);
              return yield* Effect.fail(
                new WriteStalledError({
                  databaseId,
                  reason: "UnflushedBytes",
                  retryAfterMs: 1_000,
                  message: `Database ${databaseId} has ${bytes} unflushed bytes, above its ${settings.maxUnflushedBytes} limit, so the write was refused. Retry the same batch after a short wait; nothing was written.`,
                }),
              );
            }
            if (ready.manifest.manifest.l0.length >= settings.l0MaxSsts) {
              // The cached manifest may predate compactions: reload before refusing.
              const latest = yield* manifests
                .update(databaseId, (manifest) => Result.succeed(manifest))
                .pipe(Effect.catchTags(manifestFailures(databaseId, "write")));
              ready = { ...ready, manifest: latest };
              yield* Ref.set(phase, ready);
              if (latest.manifest.l0.length >= settings.l0MaxSsts) {
                yield* Metric.update(writeStalls, 1);
                yield* Effect.annotateCurrentSpan({ "writer.stall_reason": "L0Backlog" });
                // Compaction is behind: make sure the compactor is awake (a lost wake would stall forever).
                yield* wakeCompactor(databaseId);
                return yield* Effect.fail(
                  new WriteStalledError({
                    databaseId,
                    reason: "L0Backlog",
                    retryAfterMs: 5_000,
                    message: `Database ${databaseId} has ${latest.manifest.l0.length} L0 SSTs waiting for compaction, at its ${settings.l0MaxSsts} limit, so the write was refused. Retry the same batch later; nothing was written.`,
                  }),
                );
              }
            }
            yield* beforeCommit;
            const manifest: Manifest = ready.manifest.manifest;
            const writes = yield* Effect.fromResult(
              collapseWriteBatch(batch, {
                databaseId,
                keyRange: manifest.keyRange,
                mergeOperator: Option.map(
                  manifest.options.mergeOperator,
                  (name) => MERGE_OPERATORS[name],
                ),
              }),
            );
            const meta = yield* memTable.meta.pipe(
              Effect.mapError(unavailable(databaseId, "write")),
            );
            // Monotonic clock: never behind the newest tick already handed out or flushed.
            const now = yield* Clock.currentTimeMillis;
            const createTs = Timestamp.make(Math.max(now, meta.clockTick));
            const added = writes.reduce((sum, item) => sum + memTableWriteBytes(item), 0);
            // A refresh scanning while this commit runs must not install its figures (see
            // `refreshUnflushedCounters`): mark the commit before its rows reach SQLite, and add
            // them in the same step that ends it. The three steps are uninterruptible together: an
            // interruption after the mark would leave it set and keep every later refresh from
            // installing, and one after SQLite committed would leave the row out of the counters,
            // with no alarm to flush it until the next write.
            const seq = yield* Effect.uninterruptible(
              Effect.gen(function* () {
                yield* Ref.update(unflushed, (current) => ({
                  ...current,
                  generation: current.generation + 1,
                  committing: true,
                }));
                const exit = yield* Effect.exit(memTable.commit(writes, createTs));
                yield* Ref.update(unflushed, (current) => {
                  const ended = {
                    ...current,
                    generation: current.generation + 1,
                    committing: false,
                  };
                  // A failed commit rolled back: none of its rows is in SQLite.
                  return exit._tag === "Success"
                    ? {
                        ...ended,
                        bytes: current.bytes + added,
                        rows: current.rows + writes.length,
                        oldest: Option.isSome(current.oldest)
                          ? current.oldest
                          : Option.some(createTs),
                      }
                    : ended;
                });
                return yield* exit;
              }),
            ).pipe(Effect.mapError(unavailable(databaseId, "write")));
            return { seq, createTs };
          }),
        )
        .pipe(
          // Even when the request is interrupted: rows that reached SQLite need their alarm, or
          // nothing flushes them until the next write.
          Effect.ensuring(scheduleFlush(databaseId)),
        );
      yield* Effect.annotateCurrentSpan({ seq: receipt.seq });
      return receipt;
    });

  const write = Effect.fn("DbWriter.write")(function* (databaseId: DatabaseId, batch: WriteBatch) {
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "batch.operations": batch.operations.length,
    });
    return yield* commitBatch(databaseId, batch, Effect.void);
  });

  const commitTransaction = Effect.fn("DbWriter.commitTransaction")(function* (
    databaseId: DatabaseId,
    commit: TransactionCommit,
  ) {
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "batch.operations": commit.batch.operations.length,
      "transaction.isolation": commit.isolation,
      "transaction.read_seq": commit.readSeq,
    });
    const conflict = (reason: TransactionConflictError["reason"], message: string) =>
      Effect.fail(new TransactionConflictError({ databaseId, reason, message }));
    const tooOld = conflict(
      "SnapshotTooOld",
      `Database ${databaseId} no longer keeps every write after the transaction's reads (sequence number ${commit.readSeq}), so its snapshot can no longer be checked: it ran longer than the writer keeps flushed rows. Begin a new transaction and run it again; nothing was written.`,
    );
    const readMeta = memTable.meta.pipe(Effect.mapError(unavailable(databaseId, "transaction")));
    const check = Effect.gen(function* () {
      const meta = yield* readMeta;
      // Every write after readSeq must still be in SQLite (retained or unflushed) to be checked.
      if (meta.retainedFromSeq > commit.readSeq || commit.readSeq > meta.lastSeq) {
        return yield* tooOld;
      }
      const writtenAfter = (keys: ReadonlyArray<Uint8Array>, ranges: ReadonlyArray<KeyRange>) =>
        memTable
          .writtenAfter(keys, ranges, commit.readSeq)
          .pipe(Effect.mapError(unavailable(databaseId, "transaction")));
      const writeKeys = commit.batch.operations.map((operation) => operation.key);
      if (yield* writtenAfter(writeKeys, [])) {
        return yield* conflict(
          "WriteConflict",
          `Another commit wrote a key this transaction writes in database ${databaseId} after the transaction began. Begin a new transaction and run it again; nothing was written.`,
        );
      }
      if (
        commit.isolation === "SerializableSnapshot" &&
        (yield* writtenAfter(commit.readKeys, commit.readRanges))
      ) {
        return yield* conflict(
          "ReadConflict",
          `Another commit wrote a key this transaction read in database ${databaseId} after the transaction began, so committing it would not be serializable. Begin a new transaction and run it again; nothing was written.`,
        );
      }
      // A flush's trim takes no commit lock, so one may have run between the horizon check and
      // the queries above and deleted rows they needed (D84). Trims only raise the horizon: if it
      // is still at or below readSeq, every row the queries looked for was there.
      if ((yield* readMeta).retainedFromSeq > commit.readSeq) return yield* tooOld;
    });
    return yield* commitBatch(databaseId, commit.batch, check);
  });

  const probe = Effect.fn("DbWriter.probe")(function* (databaseId: DatabaseId, key: Uint8Array) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    const ready = yield* ensureReady(databaseId);
    // Read flushedSeq and the versions atomically: a flush truncating in between would hide rows.
    const { meta, versions } = yield* memTable
      .consistently((meta) =>
        Effect.map(memTable.versions(key, meta.lastSeq, meta.flushedSeq), (versions) => ({
          meta,
          versions,
        })),
      )
      .pipe(Effect.mapError(unavailable(databaseId, "probe")));
    yield* Effect.annotateCurrentSpan({
      seq: meta.lastSeq,
      "probe.versions": versions.length,
      "probe.flushed_seq": meta.flushedSeq,
    });
    return {
      versions,
      snapshotSeq: meta.lastSeq,
      flushedSeq: meta.flushedSeq,
      flushedManifestId: meta.flushedManifestId,
      sealedSuccessors: ready.sealed,
    };
  });

  const probeAt = Effect.fn("DbWriter.probeAt")(function* (
    databaseId: DatabaseId,
    key: Uint8Array,
    readSeq: SeqNum,
  ) {
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "transaction.read_seq": readSeq,
    });
    yield* ensureReady(databaseId);
    // The horizon and the rows atomically: a trim in between would hide rows.
    const { meta, versions } = yield* memTable
      .consistently((meta) =>
        Effect.map(memTable.versions(key, readSeq, meta.retainedFromSeq), (versions) => ({
          meta,
          versions,
        })),
      )
      .pipe(Effect.mapError(unavailable(databaseId, "probe")));
    return { versions, retainedFromSeq: meta.retainedFromSeq };
  });

  const scan = Effect.fn("DbWriter.scan")(function* (
    databaseId: DatabaseId,
    request: {
      readonly range: KeyRange;
      readonly snapshotSeq: Option.Option<SeqNum>;
      readonly cursor: Option.Option<MemTableCursor>;
      readonly limit: number;
      readonly includeRetained: boolean;
    },
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    const ready = yield* ensureReady(databaseId);
    const limit = Math.max(1, Math.min(request.limit, 1000));
    // One page is read atomically with flushedSeq; across pages the reader takes the largest one.
    const { meta, snapshotSeq, rows, complete } = yield* memTable
      .consistently((meta) => {
        const snapshotSeq = Option.getOrElse(request.snapshotSeq, () => meta.lastSeq);
        return Effect.map(
          memTable.scan(
            request.range,
            snapshotSeq,
            request.includeRetained ? meta.retainedFromSeq : meta.flushedSeq,
            request.cursor,
            limit,
          ),
          (page) => ({
            meta,
            snapshotSeq,
            rows: page.rows,
            complete: page.complete,
          }),
        );
      })
      .pipe(Effect.mapError(unavailable(databaseId, "scan")));
    const last = rows[rows.length - 1];
    return {
      rows,
      snapshotSeq,
      flushedSeq: meta.flushedSeq,
      retainedFromSeq: meta.retainedFromSeq,
      flushedManifestId: meta.flushedManifestId,
      sealedSuccessors: ready.sealed,
      // A page ends at its row limit or its byte bound (D93): either way the next resumes after it.
      nextCursor:
        !complete && last !== undefined
          ? Option.some({ key: last.key, seq: last.seq })
          : Option.none(),
    };
  });

  const flush = Effect.fn("DbWriter.requestFlush")(function* (databaseId: DatabaseId) {
    yield* writableReady(databaseId).pipe(
      Effect.catchTags({ DatabaseSealedError: () => ensureReady(databaseId) }),
    );
    return yield* flushNow(databaseId);
  });

  const changes = Effect.fn("DbWriter.changes")(function* (
    databaseId: DatabaseId,
    afterSeq: SeqNum,
    limit: number,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId, seq: afterSeq });
    yield* ensureReady(databaseId);
    const { meta, rows } = yield* memTable
      .consistently((meta) =>
        Effect.map(
          memTable.rowsBySeq(
            SeqNum.make(Math.max(afterSeq, meta.flushedSeq)),
            meta.lastSeq,
            Math.max(1, Math.min(limit, 1000)),
          ),
          (page) => ({
            meta,
            rows: page.rows,
          }),
        ),
      )
      .pipe(Effect.mapError(unavailable(databaseId, "changes")));
    return {
      rows: [...rows].sort(changelogRowOrder),
      lastSeq: meta.lastSeq,
      flushedSeq: meta.flushedSeq,
    };
  });

  const status = Effect.fn("DbWriter.status")(function* (databaseId: DatabaseId) {
    const ready = yield* ensureReady(databaseId);
    const meta = yield* memTable.meta.pipe(Effect.mapError(unavailable(databaseId, "status")));
    // From the counters, not a scan: a transaction begins with this call.
    const counters = yield* Ref.get(unflushed);
    return {
      databaseId,
      writerEpoch: ready.writerEpoch,
      lastSeq: meta.lastSeq,
      flushedSeq: meta.flushedSeq,
      retainedFromSeq: meta.retainedFromSeq,
      flushedManifestId: meta.flushedManifestId,
      unflushedBytes: counters.bytes,
      unflushedRows: counters.rows,
      l0Count: ready.manifest.manifest.l0.length,
      sealed: Option.isSome(ready.sealed),
    };
  });

  const seal = Effect.fn("DbWriter.seal")(function* (
    databaseId: DatabaseId,
    successors: ReadonlyArray<DatabaseId>,
    holdForSuccessors: boolean,
  ) {
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "seal.successors": successors.length,
      "seal.holds": holdForSuccessors,
    });
    // Hold the commit lock: no write can be acknowledged between the final flush and the seal.
    return yield* commitLock.withPermits(1)(
      Effect.gen(function* () {
        const ready = yield* ensureReady(databaseId);
        // A seal names its successors once: a repeat with the same ones is a retry and succeeds;
        // one naming others is refused with the recorded ones (D95).
        if (Option.isSome(ready.sealed) && !sameSuccessors(ready.sealed.value, successors)) {
          return yield* Effect.fail(sealedError(databaseId, ready.sealed.value));
        }
        if (Option.isSome(ready.sealed) && !holdForSuccessors) {
          const meta = yield* memTable.meta.pipe(Effect.mapError(unavailable(databaseId, "seal")));
          return { flushedSeq: meta.flushedSeq, successorHolds: [] };
        }
        const proposed = yield* Effect.forEach(successors, (successor) =>
          Effect.map(generateCheckpointId, (checkpointId) => ({ successor, checkpointId })),
        );
        const createdAt = Timestamp.make(yield* Clock.currentTimeMillis);
        const sealEdit = (
          manifest: Manifest,
          manifestId: ManifestId,
        ): Result.Result<Manifest, WriterFencedError | DatabaseRemovedError> => {
          const alreadySealed = manifest.state._tag === "Sealed";
          if (!alreadySealed && manifest.writerEpoch !== ready.writerEpoch) {
            return Result.fail(
              new WriterFencedError({
                databaseId,
                writerEpoch: ready.writerEpoch,
                currentWriterEpoch: manifest.writerEpoch,
                message: `Writer epoch ${ready.writerEpoch} of database ${databaseId} was fenced by epoch ${manifest.writerEpoch} before it could seal. Retry the seal; the current writer will perform it.`,
              }),
            );
          }
          const sealedManifest = alreadySealed ? manifest : sealManifest(manifest, successors);
          if (!holdForSuccessors) return Result.succeed(sealedManifest);
          // The holds pin the manifest this commit creates: sealed, with every acknowledged row in
          // L0, and no later write (D57).
          return holdForSuccessors_(sealedManifest, {
            pinnedManifestId: ManifestId.make(manifestId + 1),
            proposed,
            createdAt,
          });
        };
        const sealFailures = {
          DatabaseRemovedError: (error: DatabaseRemovedError) =>
            Effect.fail(new DatabaseNotFoundError({ databaseId, message: error.message })),
        };
        // The final flush and the seal in one manifest commit, so the commit lock is held for one
        // upload and one commit (D57).
        const flushed = yield* flushThen(databaseId, sealEdit);
        // Nothing left to flush: the seal commits on its own, from the writer's view (D56).
        const sealed = Option.isSome(flushed.committed)
          ? flushed.committed.value
          : yield* manifests
              .updateFrom((yield* ensureReady(databaseId)).manifest, sealEdit)
              .pipe(
                Effect.catchTags(manifestFailures(databaseId, "seal")),
                Effect.catchTags(sealFailures),
              );
        const flushedSeq = flushed.flushedSeq;
        const sealedSuccessors =
          sealed.manifest.state._tag === "Sealed" ? sealed.manifest.state.successors : successors;
        yield* Ref.set(phase, {
          _tag: "Ready",
          databaseId,
          writerEpoch: WriterEpoch.make(sealed.manifest.writerEpoch),
          manifest: sealed,
          sealed: Option.some(sealedSuccessors),
        });
        return {
          flushedSeq,
          successorHolds: holdForSuccessors ? successorHoldsOf(sealed.manifest, successors) : [],
        };
      }).pipe(
        // A seal that does not return may still have committed (its caller hung up, or the read
        // after the create-if-absent failed): the manifest may be sealed, with a new writer epoch,
        // while this writer still believes it is open. Forget the phase while the commit lock is
        // still held, so the next request starts again from the manifest and sees the seal (D85).
        Effect.onExit((exit) =>
          exit._tag === "Success"
            ? Effect.void
            : Ref.update(phase, (current): WriterPhase =>
                current._tag === "Ready" ? { _tag: "Uninitialized" } : current,
              ),
        ),
      ),
    );
  });

  const destroy = Effect.fn("DbWriter.destroy")(function* (databaseId: DatabaseId) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    // No write or flush may run while the state is deleted.
    yield* commitLock.withPermits(1)(
      flushLock.withPermits(1)(
        Effect.gen(function* () {
          const meta = yield* memTable.meta.pipe(
            Effect.mapError(unavailable(databaseId, "destroy")),
          );
          if (Option.isSome(meta.databaseId) && meta.databaseId.value !== databaseId) {
            return yield* mismatch(databaseId, meta.databaseId.value);
          }
          yield* memTable.clear.pipe(Effect.mapError(unavailable(databaseId, "destroy")));
          yield* alarm.cancel;
          // Both locks are held, so no commit or refresh is running.
          yield* Ref.update(unflushed, (current) => ({
            ...NO_UNFLUSHED_ROWS,
            generation: current.generation + 1,
          }));
          yield* Ref.set(phase, { _tag: "Uninitialized" });
        }),
      ),
    );
  });

  /**
   * Sets the alarm again after a failed run (D102): one second after the first consecutive
   * failure, doubling after each further one, at most `maxL0Age` (the longest a row waits for its
   * flush). The handler never throws, so the runtime's own retries (6, from 2 s) never add to these.
   */
  const retryAlarm = (cause: Cause.Cause<unknown>, databaseId: Option.Option<DatabaseId>) =>
    Effect.gen(function* () {
      const failures = yield* alarm.retryAfterFailure(settings.maxL0Age);
      yield* Effect.annotateCurrentSpan({ "alarm.failures": failures });
      yield* Effect.logWarning("Writer alarm could not flush; it retries later.").pipe(
        Effect.annotateLogs({
          "database.id": Option.getOrElse(databaseId, () => "unknown"),
          "alarm.failures": failures,
          cause: Cause.pretty(cause),
        }),
      );
    });

  const runAlarm = Effect.gen(function* () {
    const meta = yield* Effect.exit(memTable.meta);
    // Unreadable storage: rows may be waiting, so the alarm is set again (D102).
    if (meta._tag === "Failure") return yield* retryAlarm(meta.cause, Option.none());
    const databaseId = meta.value.databaseId;
    // No database: nothing is stored, so nothing can wait for a flush.
    if (Option.isNone(databaseId)) return;
    const id = databaseId.value;
    const outcome = yield* Effect.gen(function* () {
      yield* ensureReady(id);
      // The same counters `scheduleFlush` set this alarm from, so the two always agree on
      // whether a flush is due. Deciding here from a scan of the memtable, with a figure the
      // scheduler did not share, made the alarm fire, scan, and be set again without flushing.
      const counters = yield* Ref.get(unflushed);
      const now = yield* Clock.currentTimeMillis;
      const due =
        Option.isSome(counters.oldest) &&
        (counters.bytes >= settings.l0SstSize || counters.oldest.value + settings.maxL0Age <= now);
      if (due) yield* flushNow(id);
      yield* scheduleFlush(id);
    }).pipe(Effect.exit);
    if (outcome._tag === "Success") return yield* alarm.clearFailures;
    // A database that no longer exists (removed, its writer's state not yet deleted) has nothing
    // to flush: retrying would wake the object forever (D85).
    const gone = Cause.findErrorOption(outcome.cause).pipe(
      Option.exists((error) => error._tag === "DatabaseNotFoundError"),
    );
    if (gone) {
      return yield* Effect.logWarning(
        "Writer alarm found its database gone; it stops until the removal deletes its state.",
      ).pipe(Effect.annotateLogs({ "database.id": id }));
    }
    // A fenced instance can never flush what it holds: a newer epoch serves the database.
    if ((yield* Ref.get(phase))._tag === "Fenced") {
      return yield* Effect.logWarning("Writer alarm found this writer fenced; it stops.").pipe(
        Effect.annotateLogs({ "database.id": id }),
      );
    }
    yield* retryAlarm(outcome.cause, Option.some(id));
  }).pipe(
    // A defect anywhere above (a bug) is retried like a failure, never thrown to the runtime.
    Effect.catchCause((cause) => retryAlarm(cause, Option.none())),
    Effect.withSpan("DbWriter.alarm"),
  );

  return DbWriter.of({
    write,
    commitTransaction,
    probe,
    probeAt,
    scan,
    flush,
    changes,
    status,
    seal,
    destroy,
    runAlarm,
  });
});

/** Provides the writer, leaving its memtable, stores, alarm, and settings visible. */
export const dbWriterLayerWithoutDependencies = Layer.effect(DbWriter, makeDbWriter);

/**
 * Provides a writer over an in-memory SQLite memtable, an in-memory object store (shared with the
 * manifest store, and output so tests can inspect it), a recording alarm, and test settings.
 */
export const dbWriterTestLayer = (
  overrides: Parameters<typeof lsmConfigurationTestLayer>[0] = {},
) =>
  dbWriterLayerWithoutDependencies.pipe(
    Layer.provideMerge(sstStoreLayerWithoutDependencies),
    Layer.provideMerge(
      Layer.mergeAll(
        memTableTestLayer,
        durableObjectAlarmTestLayer,
        compactionTriggerTestLayer,
        backgroundWorkTestLayer,
        lsmConfigurationTestLayer(overrides),
      ),
    ),
    Layer.provideMerge(manifestStoreTestLayer),
  );
