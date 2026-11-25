import {
  addCheckpoint,
  type Checkpoint,
  CheckpointId,
  DatabaseId,
  type DatabaseOptions,
  type DatabaseRemovedError,
  type DatabaseHasDependentsError,
  forkManifest,
  generateCheckpointId,
  initialManifest,
  type KeyRange,
  type Manifest,
  ManifestId,
  markRemoved,
  removeCheckpoint,
  type SeqNum,
  Timestamp,
} from "@cf-slatedb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Clock, Context, Effect, Layer, Option, Result, Schema, type Scope } from "effect";

import { BlockSource, type BlockSourceServices } from "./block-source.ts";
import { CompactorClient } from "./compactor-client.ts";
import type { CompactorError } from "./compactor.ts";
import { DatabaseNotFoundError, WriterUnavailableError } from "./lsm-errors.ts";
import {
  ManifestReader,
  type ManifestReadError,
  type VersionedManifest,
} from "./manifest-reader.ts";
import {
  type DatabaseAlreadyExistsError,
  ManifestStore,
  type ManifestWriteError,
} from "./manifest-store.ts";
import { databasePrefix } from "./object-keys.ts";
import { ObjectStore, type ObjectStoreUnavailableError } from "./object-store.ts";
import { WriterClient, type WriterClientError } from "./writer-client.ts";

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

/** Every failure a lifecycle operation can report. */
export type DatabaseLifecycleError =
  | DatabaseNotFoundError
  | DatabaseAlreadyExistsError
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
   * @returns The first manifest, `DatabaseAlreadyExistsError`, or `WriterUnavailableError` when
   *   storage is unavailable.
   */
  readonly create: (
    databaseId: DatabaseId,
    options: CreateDatabaseOptions,
  ) => Effect.Effect<
    VersionedManifest,
    DatabaseAlreadyExistsError | WriterUnavailableError,
    RuntimeContext
  >;
  /**
   * Flushes every acknowledged write, then pins the resulting manifest with a checkpoint: GC keeps
   * every SST it references until the checkpoint is deleted or expires.
   */
  readonly createCheckpoint: (
    databaseId: DatabaseId,
    options: CreateCheckpointOptions,
  ) => Effect.Effect<Checkpoint, DatabaseLifecycleError, DatabaseLifecycleServices>;
  /** Deletes a checkpoint; deleting one that does not exist succeeds. */
  readonly deleteCheckpoint: (
    databaseId: DatabaseId,
    checkpointId: CheckpointId,
  ) => Effect.Effect<void, DatabaseLifecycleError, DatabaseLifecycleServices>;
  /**
   * Flushes and seals the database: no write commits after it, and later writes fail with
   * `DatabaseSealedError` naming `successors`. @returns The sequence number durable at the seal.
   */
  readonly seal: (
    databaseId: DatabaseId,
    successors: ReadonlyArray<DatabaseId>,
  ) => Effect.Effect<SeqNum, DatabaseLifecycleError, DatabaseLifecycleServices>;
  /**
   * Creates `forkId` as a zero-copy fork of `range` of the source at a checkpoint: its SSTs are
   * views of the source's files, and it holds its own checkpoint on the source until its compaction
   * has rewritten them. Safe to retry.
   */
  readonly forkRange: (
    request: ForkRangeRequest,
  ) => Effect.Effect<VersionedManifest, DatabaseLifecycleError, DatabaseLifecycleServices>;
  /**
   * Removes a database: fences it, deletes its writer's and compactor's state, releases the
   * checkpoints it holds on sources (if it is a fork), deletes every object under `db/<id>/`, and
   * purges its Workers Cache tag. Fails with `DatabaseHasDependentsError` while forks hold
   * checkpoints on it. Safe to retry.
   */
  readonly remove: (
    databaseId: DatabaseId,
  ) => Effect.Effect<void, DatabaseLifecycleError, DatabaseLifecycleServices>;
}

/** The contextual database lifecycle. */
export class DatabaseLifecycle extends Context.Service<DatabaseLifecycle, IDatabaseLifecycle>()(
  "@cf-slatedb/DatabaseLifecycle",
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
  const blocks = yield* BlockSource;

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

  const create = Effect.fn("DatabaseLifecycle.create")(
    function* (databaseId: DatabaseId, options: CreateDatabaseOptions) {
      yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
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

  const createCheckpoint = Effect.fn("DatabaseLifecycle.createCheckpoint")(function* (
    databaseId: DatabaseId,
    options: CreateCheckpointOptions,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    // Every acknowledged write is in the manifest the checkpoint pins.
    yield* writers
      .flush(databaseId)
      .pipe(Effect.catchTags({ DatabaseSealedError: () => Effect.void }));
    const id = yield* generateCheckpointId;
    const now = Timestamp.make(yield* Clock.currentTimeMillis);
    const committed = yield* manifests
      .update(databaseId, (manifest, manifestId) =>
        addCheckpoint(manifest, {
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
    return checkpoint;
  });

  const deleteCheckpoint = Effect.fn("DatabaseLifecycle.deleteCheckpoint")(function* (
    databaseId: DatabaseId,
    checkpointId: CheckpointId,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId, "checkpoint.id": checkpointId });
    yield* manifests
      .update(databaseId, (manifest) => Result.succeed(removeCheckpoint(manifest, checkpointId)))
      .pipe(Effect.catchTags(updateFailures(databaseId, "checkpoint")));
  });

  const seal = Effect.fn("DatabaseLifecycle.seal")(function* (
    databaseId: DatabaseId,
    successors: ReadonlyArray<DatabaseId>,
  ) {
    yield* Effect.annotateCurrentSpan({
      "database.id": databaseId,
      "seal.successors": successors.length,
    });
    return yield* writers.seal(databaseId, successors);
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
    const holdId = yield* generateCheckpointId;
    const now = Timestamp.make(yield* Clock.currentTimeMillis);
    const held = yield* manifests
      .update(
        source,
        (manifest): Result.Result<Manifest, CheckpointNotFoundError | DatabaseRemovedError> => {
          const existing = manifest.checkpoints.find(
            (candidate) =>
              Option.contains(candidate.holder, forkId) &&
              candidate.manifestId === checkpoint.manifestId,
          );
          if (existing !== undefined) return Result.succeed(manifest);
          if (!manifest.checkpoints.some((candidate) => candidate.id === checkpointId)) {
            return Result.fail(checkpointNotFound(source, checkpointId));
          }
          return addCheckpoint(manifest, {
            id: holdId,
            manifestId: checkpoint.manifestId,
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
    const hold = held.manifest.checkpoints.find(
      (candidate) =>
        Option.contains(candidate.holder, forkId) && candidate.manifestId === checkpoint.manifestId,
    );
    if (hold === undefined) return yield* checkpointNotFound(source, checkpointId);

    const fork = forkManifest(pinned.value, {
      databaseId: forkId,
      checkpointId: hold.id,
      range: request.range,
    });
    const created = yield* manifests.initialize(fork).pipe(
      Effect.catchTags({
        ManifestWriteError: unavailable(forkId, "fork"),
        // A retried fork finds its own first manifest; any other database with this id wins.
        DatabaseAlreadyExistsError: (error) =>
          Effect.gen(function* () {
            const first = yield* reader
              .load(forkId, ManifestId.make(1))
              .pipe(Effect.catchTags({ ManifestReadError: unavailable(forkId, "fork") }));
            const ours = Option.exists(first, (manifest) =>
              manifest.externalRefs.some(
                (ref) => ref.owner === source && ref.checkpointId === hold.id,
              ),
            );
            if (ours && Option.isSome(first)) {
              return { id: ManifestId.make(1), manifest: first.value };
            }
            yield* deleteCheckpoint(source, hold.id);
            return yield* Effect.fail(error);
          }),
      }),
    );
    // The fork's compactor rewrites the source's SSTs into its own, then releases the hold.
    yield* compactors.wake(forkId).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Waking a new fork's compactor failed; its first flush wakes it.", {
          forkId,
          cause,
        }),
      ),
    );
    yield* Effect.annotateCurrentSpan({ "manifest.id": created.id });
    return created;
  });

  const remove = Effect.fn("DatabaseLifecycle.remove")(function* (databaseId: DatabaseId) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    // Fence first: after this CAS no write, checkpoint, or fork can commit.
    const marked = yield* manifests
      .update(databaseId, (manifest) => markRemoved(manifest))
      .pipe(Effect.catchTags(updateFailures(databaseId, "remove")));
    yield* writers.destroy(databaseId);
    yield* compactors.destroy(databaseId);
    // A fork releases the checkpoints it holds on its sources, so they can collect those SSTs.
    for (const ref of marked.manifest.externalRefs) {
      yield* manifests
        .update(ref.owner, (manifest) =>
          Result.succeed(removeCheckpoint(manifest, ref.checkpointId)),
        )
        .pipe(
          Effect.catchTags(updateFailures(ref.owner, "remove")),
          Effect.catchTags({ DatabaseNotFoundError: () => Effect.void }),
        );
    }
    // Delete everything under db/<id>/, page by page; manifests sort last, so a partial removal
    // still has its manifest and is retried (GC also sweeps a prefix left without one).
    let deleted = 0;
    for (;;) {
      const page = yield* objects
        .list(databasePrefix(databaseId), Option.none())
        .pipe(Effect.catchTags({ ObjectStoreUnavailableError: unavailable(databaseId, "remove") }));
      if (page.objects.length === 0) break;
      yield* objects
        .delete(page.objects.map((object) => object.key))
        .pipe(Effect.catchTags({ ObjectStoreUnavailableError: unavailable(databaseId, "remove") }));
      deleted += page.objects.length;
    }
    const purged = yield* blocks
      .purge(databaseId)
      .pipe(Effect.catchTags({ BlockSourceUnavailableError: () => Effect.succeed(false) }));
    yield* Effect.annotateCurrentSpan({ "remove.objects": deleted, "cache.purged": purged });
  });

  return DatabaseLifecycle.of({
    create,
    createCheckpoint,
    deleteCheckpoint,
    seal,
    forkRange,
    remove,
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
