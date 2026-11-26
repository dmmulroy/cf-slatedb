import {
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
  rowEncodedSizeEstimate,
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
} from "@cf-slatedb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import {
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
import { MemTable, type MemTableCursor, memTableTestLayer } from "./mem-table.ts";
import type { ManifestReadError, VersionedManifest } from "./manifest-reader.ts";
import {
  ManifestStore,
  manifestStoreTestLayer,
  type ManifestWriteError,
} from "./manifest-store.ts";
import { changelogKey, sstKey } from "./object-keys.ts";
import { SstStore, sstStoreLayerWithoutDependencies } from "./sst-store.ts";
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
}

/** One page of a memtable scan. */
export interface MemTableScanPage {
  readonly rows: ReadonlyArray<Row>;
  readonly snapshotSeq: SeqNum;
  readonly flushedSeq: SeqNum;
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
  /** One page of unflushed rows in `range` at `snapshotSeq` (default: newest). */
  readonly scan: (
    databaseId: DatabaseId,
    request: {
      readonly range: KeyRange;
      readonly snapshotSeq: Option.Option<SeqNum>;
      readonly cursor: Option.Option<MemTableCursor>;
      readonly limit: number;
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
export class DbWriter extends Context.Service<DbWriter, IDbWriter>()("@cf-slatedb/DbWriter") {}

/** Bytes flushed to L0 (SlateDB RFC-0021 parity). */
export const flushBytes = Metric.counter("lsm_flush_bytes", {
  description: "Bytes of L0 SSTs written by memtable flushes",
});

/** Writes refused by backpressure. */
export const writeStalls = Metric.counter("lsm_write_stalls", {
  description: "Writes refused with WriteStalledError",
});

/** How long to wait before retrying a failed flush. */
const FLUSH_RETRY_MILLIS = 1_000;

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

  const phase = yield* Ref.make<WriterPhase>({ _tag: "Uninitialized" });
  const unflushedBytes = yield* Ref.make(0);
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

  /** Recomputes the in-memory byte counter from SQLite. */
  const refreshUnflushedBytes = (databaseId: DatabaseId) =>
    memTable.stats.pipe(
      Effect.tap((stats) => Ref.set(unflushedBytes, stats.unflushedBytes)),
      Effect.mapError(unavailable(databaseId, "stats")),
    );

  /** Schedules the flush alarm for when the oldest unflushed row reaches `maxL0Age`, or now. */
  const scheduleFlush = (databaseId: DatabaseId) =>
    Effect.gen(function* () {
      const stats = yield* memTable.stats.pipe(
        Effect.mapError(unavailable(databaseId, "scheduleFlush")),
      );
      if (stats.rowCount === 0) return;
      const now = yield* Clock.currentTimeMillis;
      const due =
        stats.unflushedBytes >= settings.l0SstSize
          ? now
          : Option.match(stats.oldestCreateTs, {
              onNone: () => now,
              onSome: (ts) => ts + settings.maxL0Age,
            });
      const scheduled = yield* alarm.scheduled;
      if (Option.isNone(scheduled) || scheduled.value > due) yield* alarm.scheduleAt(due);
    });

  /**
   * Starts the writer for `databaseId` once per instance: fences in with a new writer epoch, seeds
   * the sequence oracle and clock from the manifest, and drops rows a crash left behind after their
   * flush committed.
   */
  const ensureReady = (databaseId: DatabaseId) =>
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
          // Rows at or below lastL0Seq are already in L0: a crash hit between commit and truncate.
          if (fenced.manifest.lastL0Seq > 0) {
            yield* memTable
              .truncate(fenced.manifest.lastL0Seq)
              .pipe(Effect.mapError(unavailable(databaseId, "start")));
          }
          yield* refreshUnflushedBytes(databaseId);
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
          if (cut <= meta.flushedSeq)
            return { flushedSeq: meta.flushedSeq, committed: nothingCommitted };
          const options = ready.manifest.manifest.options;
          const written = yield* ssts
            .write({
              owner: databaseId,
              kind: "Compacted",
              objectKey: (id) => sstKey(databaseId, id),
              rows: memTable.rowsUpTo(cut),
            })
            .pipe(
              Effect.catchTags({
                MemTableError: (error) => Effect.fail(unavailable(databaseId, "flush")(error)),
                ObjectStoreUnavailableError: (error) =>
                  Effect.fail(unavailable(databaseId, "flush")(error)),
              }),
            );
          if (Option.isNone(written)) {
            // Nothing at or below the cut (already truncated): just record it.
            yield* memTable.truncate(cut).pipe(Effect.mapError(unavailable(databaseId, "flush")));
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
          // Delete only after the manifest commit: a crash before this line leaves rows that the next
          // start drops (they are in L0), or that a later flush writes again with the same sequence
          // numbers, which merging deduplicates.
          yield* memTable.truncate(cut).pipe(Effect.mapError(unavailable(databaseId, "flush")));
          yield* Ref.update(phase, (current) =>
            current._tag === "Ready" ? { ...current, manifest: committed } : current,
          );
          yield* refreshUnflushedBytes(databaseId);
          yield* Metric.update(flushBytes, written.value.info.sizeBytes);
          // Enough L0 SSTs to compact: wake the compactor (an idle database has no compactor alarm).
          if (committed.manifest.l0.length >= settings.minCompactionSources) {
            yield* compaction.wake(databaseId);
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
          Effect.map((page) => {
            const last = page[page.length - 1];
            return [
              page,
              page.length === 1024 && last !== undefined ? Option.some(last.seq) : Option.none(),
            ] as const;
          }),
        ),
      ).pipe(
        // Rows sharing the last sequence of a page continue on the next page by key order.
        Stream.filter((row) => row.seq > afterSeq),
      );
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
      const receipt = yield* commitLock.withPermits(1)(
        Effect.gen(function* () {
          let ready = yield* writableReady(databaseId);
          const bytes = yield* Ref.get(unflushedBytes);
          if (bytes >= settings.maxUnflushedBytes) {
            yield* Metric.update(writeStalls, 1);
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
              // Compaction is behind: make sure the compactor is awake (a lost wake would stall forever).
              yield* compaction.wake(databaseId);
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
          const meta = yield* memTable.meta.pipe(Effect.mapError(unavailable(databaseId, "write")));
          // Monotonic clock: never behind the newest tick already handed out or flushed.
          const now = yield* Clock.currentTimeMillis;
          const createTs = Timestamp.make(Math.max(now, meta.clockTick));
          const seq = yield* memTable
            .commit(writes, createTs)
            .pipe(Effect.mapError(unavailable(databaseId, "write")));
          const added = writes.reduce(
            (sum, item) =>
              sum +
              rowEncodedSizeEstimate({
                key: item.key,
                seq,
                value: item.value,
                createTs: Option.some(createTs),
                expireTs: Option.none(),
              }) +
              24,
            0,
          );
          yield* Ref.update(unflushedBytes, (current) => current + added);
          return { seq, createTs };
        }),
      );
      yield* scheduleFlush(databaseId).pipe(
        Effect.catchTags({ WriterUnavailableError: () => Effect.void }),
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
    const check = Effect.gen(function* () {
      const meta = yield* memTable.meta.pipe(
        Effect.mapError(unavailable(databaseId, "transaction")),
      );
      // Every write after readSeq must still be in the memtable to be checked.
      if (meta.flushedSeq > commit.readSeq || commit.readSeq > meta.lastSeq) {
        return yield* conflict(
          "SnapshotTooOld",
          `Data newer than the transaction's reads (sequence number ${commit.readSeq}) has been flushed in database ${databaseId}, so its snapshot can no longer be checked. Begin a new transaction and run it again; nothing was written.`,
        );
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
    });
    return yield* commitBatch(databaseId, commit.batch, check);
  });

  const probe = Effect.fn("DbWriter.probe")(function* (databaseId: DatabaseId, key: Uint8Array) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    yield* ensureReady(databaseId);
    // Read flushedSeq and the versions atomically: a flush truncating in between would hide rows.
    const { meta, versions } = yield* memTable
      .consistently((meta) =>
        Effect.map(memTable.versions(key, meta.lastSeq), (versions) => ({ meta, versions })),
      )
      .pipe(Effect.mapError(unavailable(databaseId, "probe")));
    yield* Effect.annotateCurrentSpan({
      seq: meta.lastSeq,
      "probe.versions": versions.length,
      "probe.flushed_seq": meta.flushedSeq,
    });
    return { versions, snapshotSeq: meta.lastSeq, flushedSeq: meta.flushedSeq };
  });

  const scan = Effect.fn("DbWriter.scan")(function* (
    databaseId: DatabaseId,
    request: {
      readonly range: KeyRange;
      readonly snapshotSeq: Option.Option<SeqNum>;
      readonly cursor: Option.Option<MemTableCursor>;
      readonly limit: number;
    },
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    yield* ensureReady(databaseId);
    const limit = Math.max(1, Math.min(request.limit, 1000));
    // One page is read atomically with flushedSeq; across pages the reader takes the largest one.
    const { meta, snapshotSeq, rows } = yield* memTable
      .consistently((meta) => {
        const snapshotSeq = Option.getOrElse(request.snapshotSeq, () => meta.lastSeq);
        return Effect.map(
          memTable.scan(request.range, snapshotSeq, request.cursor, limit),
          (rows) => ({
            meta,
            snapshotSeq,
            rows,
          }),
        );
      })
      .pipe(Effect.mapError(unavailable(databaseId, "scan")));
    const last = rows[rows.length - 1];
    return {
      rows,
      snapshotSeq,
      flushedSeq: meta.flushedSeq,
      nextCursor:
        rows.length === limit && last !== undefined
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
          memTable.rowsBySeq(afterSeq, meta.lastSeq, Math.max(1, Math.min(limit, 1000))),
          (rows) => ({
            meta,
            rows,
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
    const stats = yield* memTable.stats.pipe(Effect.mapError(unavailable(databaseId, "status")));
    return {
      databaseId,
      writerEpoch: ready.writerEpoch,
      lastSeq: meta.lastSeq,
      flushedSeq: meta.flushedSeq,
      unflushedBytes: stats.unflushedBytes,
      unflushedRows: stats.rowCount,
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
      }),
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
          yield* Ref.set(unflushedBytes, 0);
          yield* Ref.set(phase, { _tag: "Uninitialized" });
        }),
      ),
    );
  });

  const runAlarm = Effect.gen(function* () {
    const meta = yield* memTable.meta.pipe(Effect.option);
    const databaseId = Option.flatMap(meta, (value) => value.databaseId);
    if (Option.isNone(databaseId)) return;
    const id = databaseId.value;
    const outcome = yield* Effect.gen(function* () {
      yield* ensureReady(id);
      const stats = yield* memTable.stats.pipe(Effect.mapError(unavailable(id, "alarm")));
      const now = yield* Clock.currentTimeMillis;
      const due =
        stats.rowCount > 0 &&
        (stats.unflushedBytes >= settings.l0SstSize ||
          Option.match(stats.oldestCreateTs, {
            onNone: () => true,
            onSome: (ts) => ts + settings.maxL0Age <= now,
          }));
      if (due) yield* flushNow(id);
      yield* scheduleFlush(id);
    }).pipe(Effect.exit);
    if (outcome._tag === "Failure") {
      const current = yield* Ref.get(phase);
      if (current._tag !== "Fenced") {
        const now = yield* Clock.currentTimeMillis;
        yield* alarm.scheduleAt(now + FLUSH_RETRY_MILLIS);
      }
      yield* Effect.logWarning("Writer alarm could not flush; it retries shortly.").pipe(
        Effect.annotateLogs({ "database.id": id }),
      );
    }
  }).pipe(Effect.withSpan("DbWriter.alarm"));

  return DbWriter.of({
    write,
    commitTransaction,
    probe,
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
        lsmConfigurationTestLayer(overrides),
      ),
    ),
    Layer.provideMerge(manifestStoreTestLayer),
  );
