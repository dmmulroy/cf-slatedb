import {
  addCallerCheckpoint,
  addCheckpoint,
  type Checkpoint,
  type CheckpointLimitError,
  CheckpointId,
  DatabaseId,
  type DatabaseOptions,
  type DatabaseRemovedError,
  DatabaseHasDependentsError,
  forkManifest,
  generateCheckpointId,
  initialManifest,
  type KeyRange,
  type Manifest,
  ManifestId,
  markRemovalCleared,
  markRemoved,
  removalExpiresAt,
  removeCheckpoint,
  replaceManifestFields,
  SeqNum,
  WriterEpoch,
  CompactorEpoch,
  type SstId,
  Timestamp,
} from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Clock, Context, Effect, Layer, Option, Result, Schema, type Scope } from "effect";

import { BackgroundWork } from "./background-work.ts";
import { BlockSource, type BlockSourceServices } from "./block-source.ts";
import { CompactorClient } from "./compactor-client.ts";
import { DbSnapshot } from "./db-client.ts";
import type { SealReceipt } from "./db-writer.ts";
import {
  type CompactorError,
  type SstDropError,
  SstDropRefusedError,
  type SstDropReport,
} from "./compactor.ts";
import { DatabaseNotFoundError, WriterUnavailableError } from "./lsm-errors.ts";
import {
  ManifestReader,
  type ManifestReadError,
  type VersionedManifest,
} from "./manifest-reader.ts";
import { DatabaseAlreadyExistsError, ManifestStore, ManifestWriteError } from "./manifest-store.ts";
import { LsmConfiguration } from "./lsm-configuration.ts";
import {
  databasePrefix,
  latestManifestKey,
  manifestGcBoundaryKey,
  manifestKey,
} from "./object-keys.ts";
import { ObjectStore, type ObjectStoreUnavailableError } from "./object-store.ts";
import { WriterClient, type WriterClientError } from "./writer-client.ts";

/**
 * A fork's hold as `forkRange` found it: `Created` by this call (its to undo when the fork is not
 * created), or `Existing` (a split's seal or an earlier call took it, and the fork may read it).
 */
type ForkHoldAcquisition =
  | { readonly _tag: "Created"; readonly hold: Checkpoint }
  | { readonly _tag: "Existing"; readonly hold: Checkpoint };

/** Why a checkpoint may be removed (D136); see `removeCheckpointFor`. */
type CheckpointRemovalPermit =
  | { readonly _tag: "CallerCheckpoint"; readonly checkpointId: CheckpointId }
  | {
      readonly _tag: "HolderGone";
      readonly checkpointId: CheckpointId;
      readonly holder: DatabaseId;
    }
  | {
      readonly _tag: "UnusedHold";
      readonly checkpointId: CheckpointId;
      readonly holder: DatabaseId;
    };

/** Options of a new database. */
export interface CreateDatabaseOptions {
  /** Keys the database accepts; writes outside fail with `KeyOutOfRangeError`. None: every key. */
  readonly keyRange: Option.Option<KeyRange>;
  readonly databaseOptions: DatabaseOptions;
}

/** Options of a new checkpoint. */
export interface CreateCheckpointOptions {
  readonly name: Option.Option<string>;
  /** How long the checkpoint lasts; none: until deleted. */
  readonly lifetimeMs: Option.Option<number>;
}

/**
 * Schema for a snapshot pinned by a checkpoint: read with `snapshot` like any snapshot; it stays
 * readable until `expireAt` or until the checkpoint is deleted (`DELETE …/checkpoints/<id>`).
 */
export const PinnedSnapshot = Schema.Struct({
  snapshot: DbSnapshot,
  checkpointId: CheckpointId,
  expireAt: Timestamp,
});

/** A snapshot pinned by a checkpoint. */
export type PinnedSnapshot = typeof PinnedSnapshot.Type;

/** A fork: the new database, the source checkpoint it starts from, and the key range it takes. */
export interface ForkRangeRequest {
  readonly source: DatabaseId;
  readonly checkpointId: CheckpointId;
  readonly range: KeyRange;
  readonly forkId: DatabaseId;
}

/**
 * The checkpoint does not exist (it was deleted or expired), or the manifest it pinned was
 * collected. Not retryable: create a new checkpoint.
 */
export class CheckpointNotFoundError extends Schema.TaggedError<CheckpointNotFoundError>()(
  "CheckpointNotFoundError",
  { databaseId: DatabaseId, checkpointId: CheckpointId, message: Schema.String },
) {}

/**
 * A database with this id was removed less than `gcMinAge` ago, so the id cannot be used yet:
 * reader isolates may still hold the removed database's manifests (D90). Retryable after
 * `retryAfterMs`.
 */
export class DatabaseRecentlyRemovedError extends Schema.TaggedError<DatabaseRecentlyRemovedError>()(
  "DatabaseRecentlyRemovedError",
  {
    databaseId: DatabaseId,
    /** When the id may be used again, in epoch milliseconds. */
    reusableAt: Schema.Number,
    /** How long from the refusal until then. */
    retryAfterMs: Schema.Number,
    message: Schema.String,
  },
) {}

/** An operator's request to drop one SST (see {@link IDatabaseLifecycle.dropSst}). */
export interface DropSstOptions {
  readonly sstId: SstId;
  /** Must repeat `sstId`: dropping loses data, so the caller names the SST twice. */
  readonly confirm: string;
  /** Drop it even though no compaction reported it corrupted. */
  readonly force: boolean;
}

/** Every failure a lifecycle operation can report. */
export type DatabaseLifecycleError =
  | CheckpointLimitError
  | DatabaseNotFoundError
  | DatabaseAlreadyExistsError
  | DatabaseRecentlyRemovedError
  | DatabaseHasDependentsError
  | CheckpointNotFoundError
  | WriterUnavailableError
  | WriterClientError
  | CompactorError;

/** Services lifecycle operations need from the current invocation. */
export type DatabaseLifecycleServices = RuntimeContext | Scope.Scope | BlockSourceServices;

/**
 * Creates, checkpoints, seals, forks, and removes databases (design §12). Every step is a manifest
 * CAS, so each operation is safe to retry.
 */
export interface IDatabaseLifecycle {
  /**
   * Creates a database: writes its first manifest. The writer and compactor start lazily.
   *
   * @returns The first manifest; `DatabaseAlreadyExistsError`; `DatabaseRecentlyRemovedError`
   *   when a database with this id was removed less than `gcMinAge` ago (D90); or
   *   `WriterUnavailableError` (or a writer or compactor error while it clears a removed
   *   incarnation) when storage is unavailable.
   */
  readonly create: (
    databaseId: DatabaseId,
    options: CreateDatabaseOptions,
  ) => Effect.Effect<VersionedManifest, DatabaseLifecycleError, DatabaseLifecycleServices>;
  /**
   * Flushes every acknowledged write, then pins the resulting manifest with a checkpoint: GC keeps
   * every SST it references until the checkpoint is deleted or expires.
   */
  readonly createCheckpoint: (
    databaseId: DatabaseId,
    options: CreateCheckpointOptions,
  ) => Effect.Effect<Checkpoint, DatabaseLifecycleError, DatabaseLifecycleServices>;
  /**
   * Takes a snapshot that lasts `lifetimeMs` (D52): flushes every acknowledged write, then pins the
   * resulting manifest with a checkpoint that expires then. Unpinned snapshots (`DbClient.snapshot`)
   * cost no write but are kept only for GC's `gcMinAge`; pin one to read it for longer. Delete the
   * checkpoint to release it early.
   */
  readonly pinSnapshot: (
    databaseId: DatabaseId,
    lifetimeMs: number,
  ) => Effect.Effect<PinnedSnapshot, DatabaseLifecycleError, DatabaseLifecycleServices>;
  /** Deletes a checkpoint; deleting one that does not exist succeeds. */
  readonly deleteCheckpoint: (
    databaseId: DatabaseId,
    checkpointId: CheckpointId,
  ) => Effect.Effect<void, DatabaseLifecycleError, DatabaseLifecycleServices>;
  /**
   * Flushes and seals the database: no write commits after it, and later writes fail with
   * `DatabaseSealedError` naming `successors`.
   *
   * **Details**
   *
   * With `holdForSuccessors`, the seal's own commit also takes each successor a hold on the sealed
   * manifest (a checkpoint the successor holds), so each can be forked from its hold at once with
   * `forkRange`, without another checkpoint commit (a split, D57). A hold lasts until the fork's
   * compactor releases it, or until `deleteCheckpoint`. Safe to repeat: the holds are taken once
   * (the manifest records it), so a repeat returns the holds still there, and a successor whose
   * compactor released its hold after its rewrite has none in the receipt (D138).
   *
   * @returns The sequence number durable at the seal and the successors' holds still on it.
   */
  readonly seal: (
    databaseId: DatabaseId,
    successors: ReadonlyArray<DatabaseId>,
    holdForSuccessors: boolean,
  ) => Effect.Effect<SealReceipt, DatabaseLifecycleError, DatabaseLifecycleServices>;
  /**
   * Creates `forkId` as a zero-copy fork of `range` of the source at a checkpoint: its SSTs are
   * views of the source's files, and it holds its own checkpoint on the source until its compaction
   * has rewritten them.
   *
   * Safe to retry at any time (D136): a retry returns the fork's newest manifest while that
   * manifest still reads the hold, and otherwise fails with `DatabaseAlreadyExistsError` having
   * changed nothing. It never removes a hold it did not create.
   */
  readonly forkRange: (
    request: ForkRangeRequest,
  ) => Effect.Effect<VersionedManifest, DatabaseLifecycleError, DatabaseLifecycleServices>;
  /**
   * Whether a database with this id exists: it has a manifest, and it is not removed. A split
   * resumed after a successor released its hold (D77) finds the successor this way (D138).
   *
   * @returns Whether it exists, or `WriterUnavailableError` when storage is unavailable.
   */
  readonly exists: (
    databaseId: DatabaseId,
  ) => Effect.Effect<boolean, DatabaseLifecycleError, DatabaseLifecycleServices>;
  /**
   * Removes a database: fences it, deletes its writer's and compactor's state, releases the
   * checkpoints it holds on sources (if it is a fork), deletes every object under `db/<id>/`, and
   * purges its Workers Cache tag. Fails with `DatabaseHasDependentsError` while forks hold
   * checkpoints on it. Safe to retry.
   */
  readonly remove: (
    databaseId: DatabaseId,
  ) => Effect.Effect<void, DatabaseLifecycleError, DatabaseLifecycleServices>;
  /**
   * Repairs a database around a corrupted SST (D62): removes it from the manifest, recording what
   * was lost in the manifest's `droppedSsts`, so compaction of its source resumes.
   *
   * **Details**
   *
   * The SST's versions of the keys it held are lost; older versions of those keys show again. The
   * alternative that loses nothing newer than a checkpoint is restoring: fork the full key range
   * from a checkpoint taken before the corruption into a new database. `confirm` must repeat the
   * SST id. Without `force`, only an SST a compaction reported corrupted is dropped. The database's
   * compactor makes the change, so it never races a compaction of the same SST. Safe to retry.
   *
   * @returns The manifest without the SST and what was dropped; `SstDropRefusedError` (not
   *   reported, confirmation mismatch, or a compaction reading it), `SstNotInManifestError`, or a
   *   `CompactorError`.
   */
  readonly dropSst: (
    databaseId: DatabaseId,
    request: DropSstOptions,
  ) => Effect.Effect<SstDropReport, SstDropError, DatabaseLifecycleServices>;
}

/** The contextual database lifecycle. */
export class DatabaseLifecycle extends Context.Service<DatabaseLifecycle, IDatabaseLifecycle>()(
  "@shaledb/DatabaseLifecycle",
) {}

/**
 * Constructs the database lifecycle over the contextual manifests, object store, writer and
 * compactor clients, and block source (for cache purges).
 */
export const makeDatabaseLifecycle = Effect.gen(function* () {
  const manifests = yield* ManifestStore;
  const reader = yield* ManifestReader;
  const objects = yield* ObjectStore;
  const writers = yield* WriterClient;
  const compactors = yield* CompactorClient;
  const background = yield* BackgroundWork;
  const blocks = yield* BlockSource;
  const settings = yield* LsmConfiguration;

  const unavailable =
    (databaseId: DatabaseId, operation: string) =>
    (error: ManifestReadError | ManifestWriteError | ObjectStoreUnavailableError) =>
      Effect.fail(
        new WriterUnavailableError({
          databaseId,
          operation,
          reason: "StorageUnavailable",
          message: `Database ${databaseId}'s ${operation} could not reach storage: ${error.message} Retry; the operation is safe to repeat.`,
        }),
      );
  const updateFailures = (databaseId: DatabaseId, operation: string) => ({
    ManifestReadError: unavailable(databaseId, operation),
    ManifestWriteError: unavailable(databaseId, operation),
  });
  const removedIsNotFound = (databaseId: DatabaseId) => ({
    DatabaseRemovedError: (error: DatabaseRemovedError) =>
      Effect.fail(new DatabaseNotFoundError({ databaseId, message: error.message })),
  });
  const checkpointNotFound = (databaseId: DatabaseId, checkpointId: CheckpointId) =>
    new CheckpointNotFoundError({
      databaseId,
      checkpointId,
      message: `Checkpoint ${checkpointId} of database ${databaseId} does not exist (deleted or expired), or its manifest was collected. Create a new checkpoint.`,
    });

  /**
   * The part of a removal outside the bucket: the writer's SQLite state and the compactor's cell
   * destroyed, and the database's holds on its sources released. Each step is idempotent.
   */
  const destroyRemovedState = (databaseId: DatabaseId, removed: Manifest) =>
    Effect.gen(function* () {
      yield* writers.destroy(databaseId);
      yield* compactors.destroy(databaseId);
      // A fork releases the checkpoints it holds on its sources, so they can collect those SSTs.
      for (const ref of removed.externalRefs) {
        yield* removeCheckpointFor(
          ref.owner,
          {
            _tag: "HolderGone",
            checkpointId: ref.checkpointId,
            holder: databaseId,
          },
          "remove",
        ).pipe(Effect.catchTags({ DatabaseNotFoundError: () => Effect.void }));
      }
    });

  /** When the object of `databaseId`'s manifest `manifestId` was written, if it exists. */
  const writtenAt = (databaseId: DatabaseId, manifestId: ManifestId) =>
    Effect.map(objects.head(manifestKey(databaseId, manifestId)), (info) =>
      Option.map(info, (object) => object.uploadedAt),
    );

  /**
   * Clears a removed incarnation whose tombstone is `gcMinAge` old, so the id can start a new chain
   * (D90, D91): finishes the removal's state part if it stopped before it, then deletes every object
   * under the prefix written no later than the tombstone (or `gcMinAge` ago), the tombstone among
   * them, and the `LATEST` hint and GC manifest boundary, which would refuse or slow the new chain.
   * A concurrent create of the same id writes only later objects, which stay.
   */
  const clearRemovedIncarnation = (databaseId: DatabaseId, head: VersionedManifest, now: number) =>
    Effect.gen(function* () {
      if (head.manifest.state._tag === "Removed" && !head.manifest.state.cleared) {
        yield* destroyRemovedState(databaseId, head.manifest);
      }
      const headWritten = yield* writtenAt(databaseId, head.id);
      const cutoff = Math.max(
        now - settings.gcMinAge,
        Option.getOrElse(headWritten, () => now - settings.gcMinAge),
      );
      let cursor = Option.none<string>();
      do {
        const page = yield* objects.list(databasePrefix(databaseId), cursor);
        const old = page.objects.filter(
          (object) =>
            object.uploadedAt <= cutoff ||
            object.key === latestManifestKey(databaseId) ||
            object.key === manifestGcBoundaryKey(databaseId),
        );
        if (old.length > 0) yield* objects.delete(old.map((object) => object.key));
        cursor = page.cursor;
      } while (Option.isSome(cursor));
    }).pipe(
      Effect.catchTags({
        ObjectStoreUnavailableError: (error: { readonly message: string }) =>
          Effect.fail(
            new ManifestWriteError({
              databaseId,
              reason: "StorageUnavailable",
              message: error.message,
            }),
          ),
      }),
    );

  const create = Effect.fn("DatabaseLifecycle.create")(
    function* (databaseId: DatabaseId, options: CreateDatabaseOptions) {
      yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
      // An id removed less than gcMinAge ago is not reusable yet: a reader isolate may still hold
      // a manifest of the removed database, and would serve it as the new one's, and a commit from
      // a view loaded before the removal must still find the tombstone (D90, D91).
      const head = yield* reader.findLatest(databaseId).pipe(
        Effect.catchTags({
          ManifestReadError: (error) =>
            Effect.fail(
              new ManifestWriteError({
                databaseId,
                reason: "StorageUnavailable",
                message: error.message,
              }),
            ),
        }),
      );
      const now = yield* Clock.currentTimeMillis;
      if (Option.isSome(head) && head.value.manifest.state._tag === "Removed") {
        const reusableAt = Option.getOrElse(
          removalExpiresAt(head.value.manifest, Timestamp.make(now), settings.gcMinAge),
          () => now,
        );
        if (now < reusableAt) {
          return yield* new DatabaseRecentlyRemovedError({
            databaseId,
            reusableAt,
            retryAfterMs: reusableAt - now,
            message: `Database ${databaseId} was removed less than gcMinAge ago, so its id cannot be used yet: readers may still hold the removed database's manifests. Retry after ${new Date(reusableAt).toISOString()}, or use another id.`,
          });
        }
        yield* clearRemovedIncarnation(databaseId, head.value, now);
        // The new chain continues past the removed one: its sequence numbers and epochs start
        // above the tombstone's, so a handle or a job of the removed database names nothing in it
        // (D119). An old snapshot's sequence number then matches no manifest of the new chain, and
        // an old compaction's epoch is fenced.
        const removed = head.value.manifest;
        const nextSeq = SeqNum.make(removed.lastL0Seq + 1);
        return yield* manifests.initialize(
          replaceManifestFields(initialManifest(databaseId, options), {
            lastL0Seq: nextSeq,
            lastL0ClockTick: removed.lastL0ClockTick,
            writerEpoch: WriterEpoch.make(removed.writerEpoch + 1),
            compactorEpoch: CompactorEpoch.make(removed.compactorEpoch + 1),
            changelogAfterSeq: Option.some(nextSeq),
          }),
        );
      }
      return yield* manifests.initialize(initialManifest(databaseId, options));
    },
    Effect.catchTags({
      ManifestWriteError: (error: ManifestWriteError) =>
        Effect.fail(
          new WriterUnavailableError({
            databaseId: error.databaseId,
            operation: "create",
            reason: "StorageUnavailable",
            message: `Database ${error.databaseId} could not be created: ${error.message}`,
          }),
        ),
    }),
  );

  /** Flushes, then pins the resulting manifest: the checkpoint and that manifest's `lastL0Seq`. */
  const checkpointNow = (databaseId: DatabaseId, options: CreateCheckpointOptions) =>
    Effect.gen(function* () {
      // Every acknowledged write is in the manifest the checkpoint pins.
      yield* writers
        .flush(databaseId)
        .pipe(Effect.catchTags({ DatabaseSealedError: () => Effect.void }));
      const id = yield* generateCheckpointId;
      const now = Timestamp.make(yield* Clock.currentTimeMillis);
      const committed = yield* manifests
        .update(databaseId, (manifest, manifestId) =>
          addCallerCheckpoint(manifest, {
            id,
            manifestId,
            createdAt: now,
            expireAt: Option.map(options.lifetimeMs, (lifetime) => Timestamp.make(now + lifetime)),
            holder: Option.none(),
            name: options.name,
          }),
        )
        .pipe(
          Effect.catchTags(updateFailures(databaseId, "checkpoint")),
          Effect.catchTags(removedIsNotFound(databaseId)),
        );
      const checkpoint = committed.manifest.checkpoints.find((existing) => existing.id === id);
      if (checkpoint === undefined) return yield* checkpointNotFound(databaseId, id);
      yield* Effect.annotateCurrentSpan({
        "checkpoint.id": id,
        "manifest.id": checkpoint.manifestId,
      });
      // Adding a checkpoint changes no data: the pinned manifest's `lastL0Seq` is the committed one's.
      return { checkpoint, lastL0Seq: committed.manifest.lastL0Seq };
    });

  const createCheckpoint = Effect.fn("DatabaseLifecycle.createCheckpoint")(function* (
    databaseId: DatabaseId,
    options: CreateCheckpointOptions,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    return (yield* checkpointNow(databaseId, options)).checkpoint;
  });

  const pinSnapshot = Effect.fn("DatabaseLifecycle.pinSnapshot")(function* (
    databaseId: DatabaseId,
    lifetimeMs: number,
  ) {
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "snapshot.lifetime_ms": lifetimeMs,
    });
    const { checkpoint, lastL0Seq } = yield* checkpointNow(databaseId, {
      name: Option.some("snapshot"),
      lifetimeMs: Option.some(lifetimeMs),
    });
    const pinned: PinnedSnapshot = {
      snapshot: { databaseId, manifestId: checkpoint.manifestId, snapshotSeq: lastL0Seq },
      checkpointId: checkpoint.id,
      expireAt: Option.getOrElse(checkpoint.expireAt, () =>
        Timestamp.make(checkpoint.createdAt + lifetimeMs),
      ),
    };
    return pinned;
  });

  /**
   * The one way this module removes a checkpoint, with the reason it may go (D136): a caller's
   * checkpoint, a hold whose holder `deleteCheckpoint` found gone, or a hold this very `forkRange`
   * call created for a fork it then did not create. A hold that existed before the call is never
   * removed by it: the fork may be reading what it pins.
   */
  const removeCheckpointFor = Effect.fn("DatabaseLifecycle.removeCheckpointFor")(function* (
    databaseId: DatabaseId,
    permit: CheckpointRemovalPermit,
    operation: "checkpoint" | "fork" | "remove",
  ) {
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "checkpoint.id": permit.checkpointId,
      "checkpoint.removal": permit._tag,
    });
    yield* manifests
      .update(databaseId, (manifest) =>
        Result.succeed(removeCheckpoint(manifest, permit.checkpointId)),
      )
      .pipe(Effect.catchTags(updateFailures(databaseId, operation)));
  });

  const deleteCheckpoint = Effect.fn("DatabaseLifecycle.deleteCheckpoint")(function* (
    databaseId: DatabaseId,
    checkpointId: CheckpointId,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId, "checkpoint.id": checkpointId });
    // A fork's hold is not the caller's to delete while the fork exists: the fork reads the SSTs
    // it pins. The fork's own collection releases it (D77), or the fork's removal does.
    const latest = yield* reader
      .loadLive(databaseId)
      .pipe(Effect.catchTags({ ManifestReadError: unavailable(databaseId, "checkpoint") }));
    const target = latest.manifest.checkpoints.find((cp) => cp.id === checkpointId);
    const holder = Option.flatMap(
      Option.fromUndefinedOr(target),
      (checkpoint) => checkpoint.holder,
    );
    if (target !== undefined && Option.isSome(holder)) {
      const holderHead = yield* reader
        .findLatest(holder.value)
        .pipe(Effect.catchTags({ ManifestReadError: unavailable(databaseId, "checkpoint") }));
      const now = yield* Clock.currentTimeMillis;
      // A removed holder no longer reads the hold. A holder with no manifest is gone only once the
      // hold is older than gcMinAge: until then forkRange may still be writing the fork's first
      // manifest, which reads what the hold pins (D116).
      const holderGone = Option.match(holderHead, {
        onNone: () => now - target.createdAt >= settings.gcMinAge,
        onSome: (head) => head.manifest.state._tag === "Removed",
      });
      if (!holderGone) {
        return yield* new DatabaseHasDependentsError({
          databaseId,
          dependents: [holder.value],
          message: `Checkpoint ${checkpointId} of database ${databaseId} is the hold of fork ${holder.value}, which still reads its SSTs or is still being created, so it was not deleted. The fork releases it once it no longer needs it; remove the fork to release it now. A hold whose fork was never created is deletable once it is gcMinAge old.`,
        });
      }
    }
    yield* removeCheckpointFor(
      databaseId,
      Option.isSome(holder)
        ? { _tag: "HolderGone", checkpointId, holder: holder.value }
        : { _tag: "CallerCheckpoint", checkpointId },
      "checkpoint",
    );
  });

  const seal = Effect.fn("DatabaseLifecycle.seal")(function* (
    databaseId: DatabaseId,
    successors: ReadonlyArray<DatabaseId>,
    holdForSuccessors: boolean,
  ) {
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "seal.successors": successors.length,
    });
    return yield* writers.seal(databaseId, successors, holdForSuccessors);
  });

  /**
   * The fork's hold on `source` pinning `manifestId`: the one already there (a split's seal, or an
   * earlier call of this fork, took it), or a new one this call commits. Only a `Created` hold is
   * this call's to undo (D136).
   */
  const acquireForkHold = Effect.fn("DatabaseLifecycle.acquireForkHold")(function* (
    latest: VersionedManifest,
    source: DatabaseId,
    checkpointId: CheckpointId,
    manifestId: ManifestId,
    forkId: DatabaseId,
  ) {
    const holdOf = (manifest: Manifest) =>
      manifest.checkpoints.find(
        (candidate) =>
          Option.contains(candidate.holder, forkId) && candidate.manifestId === manifestId,
      );
    const existing = holdOf(latest.manifest);
    if (existing !== undefined) {
      return { _tag: "Existing", hold: existing } satisfies ForkHoldAcquisition;
    }
    const holdId = yield* generateCheckpointId;
    const now = Timestamp.make(yield* Clock.currentTimeMillis);
    const held = yield* manifests
      .update(
        source,
        (manifest): Result.Result<Manifest, CheckpointNotFoundError | DatabaseRemovedError> => {
          if (holdOf(manifest) !== undefined) return Result.succeed(manifest);
          if (!manifest.checkpoints.some((candidate) => candidate.id === checkpointId)) {
            return Result.fail(checkpointNotFound(source, checkpointId));
          }
          return addCheckpoint(manifest, {
            id: holdId,
            manifestId,
            createdAt: now,
            expireAt: Option.none(),
            holder: Option.some(forkId),
            name: Option.some(`fork ${forkId}`),
          });
        },
      )
      .pipe(
        Effect.catchTags(updateFailures(source, "fork")),
        Effect.catchTags(removedIsNotFound(source)),
      );
    const hold = holdOf(held.manifest);
    if (hold === undefined) return yield* checkpointNotFound(source, checkpointId);
    // A concurrent call may have committed the hold first: then it is that call's, not this one's.
    return (
      hold.id === holdId ? { _tag: "Created", hold } : { _tag: "Existing", hold }
    ) satisfies ForkHoldAcquisition;
  });

  const forkRange = Effect.fn("DatabaseLifecycle.forkRange")(function* (request: ForkRangeRequest) {
    const { source, checkpointId, forkId } = request;
    yield* Effect.annotateCurrentSpan({
      "database.id": source,
      "fork.id": forkId,
      "checkpoint.id": checkpointId,
    });
    const latest = yield* reader
      .loadLatest(source)
      .pipe(Effect.catchTags({ ManifestReadError: unavailable(source, "fork") }));
    const checkpoint = latest.manifest.checkpoints.find((existing) => existing.id === checkpointId);
    if (checkpoint === undefined) return yield* checkpointNotFound(source, checkpointId);
    const pinned = yield* reader
      .load(source, checkpoint.manifestId)
      .pipe(Effect.catchTags({ ManifestReadError: unavailable(source, "fork") }));
    if (Option.isNone(pinned)) return yield* checkpointNotFound(source, checkpointId);

    // The fork's own hold on the source, pinning the same manifest. Taken while the caller's
    // checkpoint still pins it (checked in the same CAS), so GC never sees the manifest unpinned.
    // A split's seal already took it (`seal` with `holdForSuccessors`), or an earlier call of this
    // fork did: then no commit is needed, and this call does not own the hold (D136).
    const acquired = yield* acquireForkHold(
      latest,
      source,
      checkpointId,
      checkpoint.manifestId,
      forkId,
    );
    const hold = acquired.hold;

    const fork = forkManifest(pinned.value, {
      databaseId: forkId,
      checkpointId: hold.id,
      range: request.range,
    });
    const created = yield* manifests.initialize(fork).pipe(
      Effect.catchTags({
        ManifestWriteError: unavailable(forkId, "fork"),
        DatabaseAlreadyExistsError: (error) =>
          Effect.gen(function* () {
            // A retried fork finds the fork its earlier call created: its newest manifest still
            // reads this hold. The first manifest is no evidence either way: the fork's GC deletes
            // it once newer ones replace it (D136).
            const head = yield* reader
              .findLatest(forkId)
              .pipe(Effect.catchTags({ ManifestReadError: unavailable(forkId, "fork") }));
            const ours = Option.filter(
              head,
              ({ manifest }) =>
                manifest.state._tag !== "Removed" &&
                manifest.externalRefs.some(
                  (ref) => ref.owner === source && ref.checkpointId === hold.id,
                ),
            );
            if (Option.isSome(ours)) return ours.value;
            // Another database has this id, or the fork no longer reads the hold. Only a hold this
            // call created is undone; one that existed before it stays with its fork (D136).
            if (acquired._tag === "Created") {
              yield* removeCheckpointFor(
                source,
                {
                  _tag: "UnusedHold",
                  checkpointId: hold.id,
                  holder: forkId,
                },
                "fork",
              );
            }
            return yield* Effect.fail(error);
          }),
      }),
    );
    // In the background, so a split's write pause waits for neither (D57): the fork's writer fences in
    // now rather than on its first write, and its compactor starts rewriting the source's SSTs into
    // its own, then releases the hold. A lost wake only delays them: the first write fences the
    // writer, and the maintenance sweep wakes the compactor of any database still holding another's
    // SSTs.
    yield* background.run(
      "DatabaseLifecycle.warmFork",
      Effect.all(
        [
          writers.status(forkId).pipe(
            Effect.asVoid,
            Effect.catchCause((cause) =>
              Effect.logWarning("Warming a new fork's writer failed; its first write starts it.", {
                forkId,
                cause,
              }),
            ),
          ),
          compactors.wake(forkId).pipe(
            Effect.asVoid,
            Effect.catchCause((cause) =>
              Effect.logWarning(
                "Waking a new fork's compactor failed; the maintenance sweep wakes it later.",
                {
                  forkId,
                  cause,
                },
              ),
            ),
          ),
        ],
        { concurrency: 2, discard: true },
      ).pipe(Effect.scoped),
    );
    yield* Effect.annotateCurrentSpan({ "manifest.id": created.id });
    return created;
  });

  const remove = Effect.fn("DatabaseLifecycle.remove")(function* (databaseId: DatabaseId) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    // Fence first: after this CAS no write, checkpoint, or fork can commit, and every later commit
    // carries the Removed state forward.
    const removedAt = Timestamp.make(yield* Clock.currentTimeMillis);
    const marked = yield* manifests
      .update(databaseId, (manifest) => markRemoved(manifest, removedAt))
      .pipe(Effect.catchTags(updateFailures(databaseId, "remove")));
    yield* destroyRemovedState(databaseId, marked.manifest);
    const head = yield* manifests
      .update(databaseId, (manifest) => Result.succeed(markRemovalCleared(manifest)))
      .pipe(Effect.catchTags(updateFailures(databaseId, "remove")));
    // The head stays as the tombstone for gcMinAge (D91). GC's manifest boundary moves to just
    // below it first, so readers start at the head and a commit from any older view lands at or
    // below the boundary, loses, reloads, and finds the database removed.
    if (head.id > 1) {
      yield* manifests
        .advanceManifestBoundary(databaseId, ManifestId.make(head.id - 1))
        .pipe(Effect.catchTags({ ManifestWriteError: unavailable(databaseId, "remove") }));
    }
    const keep = new Set([
      manifestKey(databaseId, head.id),
      latestManifestKey(databaseId),
      manifestGcBoundaryKey(databaseId),
    ]);
    // Everything else under db/<id>/ written no later than the tombstone, page by page, while the
    // head is still this tombstone. A removal still running after a later create of the id (it
    // waits gcMinAge) deletes none of the new database's objects: they are all newer than the
    // tombstone. What lands after it is the sweep's (D91).
    const tombstoneWritten = Option.getOrElse(
      yield* writtenAt(databaseId, head.id).pipe(
        Effect.catchTags({ ObjectStoreUnavailableError: unavailable(databaseId, "remove") }),
      ),
      () => Number.POSITIVE_INFINITY,
    );
    let deleted = 0;
    let cursor = Option.none<string>();
    do {
      const current = yield* reader.loadLatest(databaseId).pipe(
        Effect.map(Option.some),
        Effect.catchTags({
          ManifestReadError: unavailable(databaseId, "remove"),
          DatabaseNotFoundError: () => Effect.succeed(Option.none<VersionedManifest>()),
        }),
      );
      if (
        Option.isNone(current) ||
        current.value.id !== head.id ||
        current.value.manifest.state._tag !== "Removed"
      )
        break;
      const page = yield* objects
        .list(databasePrefix(databaseId), cursor)
        .pipe(Effect.catchTags({ ObjectStoreUnavailableError: unavailable(databaseId, "remove") }));
      const keys = page.objects
        .filter((object) => !keep.has(object.key) && object.uploadedAt <= tombstoneWritten)
        .map((object) => object.key);
      if (keys.length > 0) {
        yield* objects
          .delete(keys)
          .pipe(
            Effect.catchTags({ ObjectStoreUnavailableError: unavailable(databaseId, "remove") }),
          );
      }
      deleted += keys.length;
      cursor = page.cursor;
    } while (Option.isSome(cursor));
    const purged = yield* blocks
      .purge(databaseId)
      .pipe(Effect.catchTags({ BlockSourceUnavailableError: () => Effect.succeed(false) }));
    yield* Effect.annotateCurrentSpan({ "remove.objects": deleted, "cache.purged": purged });
  });

  const dropSst = Effect.fn("DatabaseLifecycle.dropSst")(function* (
    databaseId: DatabaseId,
    request: DropSstOptions,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId, "sst.id": request.sstId });
    if (request.confirm !== request.sstId) {
      return yield* new SstDropRefusedError({
        databaseId,
        sstId: request.sstId,
        reason: "ConfirmationMismatch",
        message: `Dropping SST ${request.sstId} of database ${databaseId} loses its rows, so confirm must repeat the SST id. Nothing was dropped; send confirm equal to the SST id to proceed.`,
      });
    }
    return yield* compactors.dropSst(databaseId, { sstId: request.sstId, force: request.force });
  });

  const exists = Effect.fn("DatabaseLifecycle.exists")(function* (databaseId: DatabaseId) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    const head = yield* reader
      .findLatest(databaseId)
      .pipe(Effect.catchTags({ ManifestReadError: unavailable(databaseId, "existence check") }));
    return Option.exists(head, ({ manifest }) => manifest.state._tag !== "Removed");
  });

  return DatabaseLifecycle.of({
    create,
    createCheckpoint,
    pinSnapshot,
    deleteCheckpoint,
    seal,
    forkRange,
    exists,
    remove,
    dropSst,
  });
});

/**
 * Provides the database lifecycle, leaving its manifests, object store, writer and compactor
 * clients, and block source visible.
 */
export const databaseLifecycleLayerWithoutDependencies = Layer.effect(
  DatabaseLifecycle,
  makeDatabaseLifecycle,
);
