import { type DatabaseId, type Manifest, ManifestId, ManifestJson } from "@cf-slatedb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Data, Effect, Layer, Metric, Result, Schedule, Schema } from "effect";

import type { DatabaseNotFoundError } from "./lsm-errors.ts";
import {
  ManifestReader,
  type ManifestReadError,
  manifestReaderLayerWithoutDependencies,
  type VersionedManifest,
} from "./manifest-reader.ts";
import { latestManifestKey, manifestKey } from "./object-keys.ts";
import {
  ObjectStore,
  objectStoreLayer,
  objectStoreTestLayer,
  type ObjectStoreUnavailableError,
} from "./object-store.ts";

/**
 * Another writer created this manifest id first. Internal: `update` reloads and reapplies its edit.
 */
export class ManifestConflictError extends Data.TaggedError("ManifestConflictError")<{
  readonly databaseId: DatabaseId;
  readonly manifestId: ManifestId;
  readonly message: string;
}> {}

/**
 * A manifest could not be written.
 *
 * **Details**
 *
 * `StorageUnavailable` is retryable. `ContentionPersisted` means every CAS attempt lost to a
 * concurrent edit; retrying later is safe because edits are idempotent.
 */
export class ManifestWriteError extends Data.TaggedError("ManifestWriteError")<{
  readonly databaseId: DatabaseId;
  readonly reason: "StorageUnavailable" | "ContentionPersisted";
  readonly message: string;
}> {}

/** A database with this id already exists, so it cannot be created again. Not retryable. */
export class DatabaseAlreadyExistsError extends Data.TaggedError("DatabaseAlreadyExistsError")<{
  readonly databaseId: DatabaseId;
  readonly message: string;
}> {}

/**
 * Writes a database's manifest chain with create-if-absent CAS.
 *
 * **Details**
 *
 * Every new manifest is `db/<id>/manifest/<n + 1>` created with `If-None-Match: *`, so each id is
 * written exactly once and concurrent writers serialize. After a successful create, the
 * `manifest/LATEST` hint is updated on a best-effort basis.
 */
export interface IManifestStore {
  /**
   * Creates manifest `manifestId`.
   *
   * @returns Nothing, `ManifestConflictError` when the id already exists, or `ManifestWriteError`.
   */
  readonly create: (
    databaseId: DatabaseId,
    manifestId: ManifestId,
    manifest: Manifest,
  ) => Effect.Effect<void, ManifestConflictError | ManifestWriteError, RuntimeContext>;
  /**
   * Creates a database's first manifest (id 1).
   *
   * @returns The manifest, or `DatabaseAlreadyExistsError` when the database exists.
   */
  readonly initialize: (
    manifest: Manifest,
  ) => Effect.Effect<
    VersionedManifest,
    DatabaseAlreadyExistsError | ManifestWriteError,
    RuntimeContext
  >;
  /**
   * Applies a pure edit to the newest manifest with CAS, reloading and reapplying on conflict.
   *
   * **Details**
   *
   * An edit returning the same manifest instance means "already applied": nothing is written and
   * the current manifest is returned. Conflicts retry with jittered exponential backoff, at most 12
   * attempts.
   *
   * @returns The manifest after the edit, the edit's own error, `DatabaseNotFoundError`,
   *   `ManifestReadError`, or `ManifestWriteError`.
   */
  readonly update: <E>(
    databaseId: DatabaseId,
    edit: (manifest: Manifest) => Result.Result<Manifest, E>,
  ) => Effect.Effect<
    VersionedManifest,
    E | DatabaseNotFoundError | ManifestReadError | ManifestWriteError,
    RuntimeContext
  >;
}

/** The contextual manifest store. */
export class ManifestStore extends Context.Service<ManifestStore, IManifestStore>()(
  "@cf-slatedb/ManifestStore",
) {}

const encodeManifestJson = Schema.encodeSync(ManifestJson);
const textEncoder = new TextEncoder();

/** Manifest CAS attempts that lost to a concurrent edit (SlateDB RFC-0021 parity). */
export const manifestCasConflicts = Metric.counter("lsm_manifest_cas_conflicts", {
  description: "Manifest create-if-absent attempts that lost to a concurrent edit",
});

const CAS_ATTEMPTS = 12;

/** Constructs the manifest store over the contextual object store and manifest reader. */
export const makeManifestStore = Effect.gen(function* () {
  const objects = yield* ObjectStore;
  const reader = yield* ManifestReader;

  // The wrapped calls fail only with `ObjectStoreUnavailableError`, which has one meaning here.
  const writeUnavailable = (databaseId: DatabaseId) =>
    Effect.mapError(
      (error: ObjectStoreUnavailableError) =>
        new ManifestWriteError({
          databaseId,
          reason: "StorageUnavailable",
          message: `Writing database ${databaseId}'s manifest failed: ${error.message}`,
        }),
    );

  const create = Effect.fn("ManifestStore.create")(function* (
    databaseId: DatabaseId,
    manifestId: ManifestId,
    manifest: Manifest,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId, "manifest.id": manifestId });
    const outcome = yield* objects
      .putIfAbsent(
        manifestKey(databaseId, manifestId),
        textEncoder.encode(encodeManifestJson(manifest)),
      )
      .pipe(writeUnavailable(databaseId));
    if (outcome === "AlreadyExists") {
      yield* Metric.update(manifestCasConflicts, 1);
      return yield* new ManifestConflictError({
        databaseId,
        manifestId,
        message: `Manifest ${manifestId} of database ${databaseId} was created by a concurrent edit. Reload and reapply.`,
      });
    }
    // Best effort: the hint may lag, and readers probe past it.
    yield* objects
      .put(latestManifestKey(databaseId), textEncoder.encode(String(manifestId)))
      .pipe(Effect.catchTags({ ObjectStoreUnavailableError: () => Effect.void }));
  });

  const initialize = Effect.fn("ManifestStore.initialize")(function* (manifest: Manifest) {
    const id = ManifestId.make(1);
    yield* create(manifest.databaseId, id, manifest).pipe(
      Effect.catchTags({
        ManifestConflictError: () =>
          Effect.fail(
            new DatabaseAlreadyExistsError({
              databaseId: manifest.databaseId,
              message: `Database ${manifest.databaseId} already exists. Choose another id, or use the existing database.`,
            }),
          ),
      }),
    );
    return { id, manifest };
  });

  const update = <E>(
    databaseId: DatabaseId,
    edit: (manifest: Manifest) => Result.Result<Manifest, E>,
  ) =>
    Effect.gen(function* () {
      yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
      let attempt = 0;
      // The edit's own failure travels in the success channel, so the CAS loop's error channel
      // holds only known tags.
      const once = Effect.gen(function* () {
        attempt++;
        const current = yield* reader.loadLatest(databaseId);
        const edited = edit(current.manifest);
        if (Result.isFailure(edited)) return Result.fail(edited.failure);
        if (edited.success === current.manifest) return Result.succeed(current);
        const nextId = ManifestId.make(current.id + 1);
        yield* create(databaseId, nextId, edited.success);
        return Result.succeed<VersionedManifest>({ id: nextId, manifest: edited.success });
      });
      const outcome = yield* once.pipe(
        Effect.retry({
          while: (error) => error._tag === "ManifestConflictError",
          schedule: Schedule.exponential("10 millis").pipe(Schedule.jittered),
          times: CAS_ATTEMPTS - 1,
        }),
        Effect.catchTags({
          ManifestConflictError: () =>
            Effect.fail(
              new ManifestWriteError({
                databaseId,
                reason: "ContentionPersisted",
                message: `Database ${databaseId}'s manifest changed concurrently on every one of ${CAS_ATTEMPTS} attempts. Retry later; the edit is safe to repeat.`,
              }),
            ),
        }),
      );
      if (Result.isFailure(outcome)) return yield* Effect.fail(outcome.failure);
      const result = outcome.success;
      yield* Effect.annotateCurrentSpan({
        "manifest.id": result.id,
        "manifest.cas_attempts": attempt,
      });
      return result;
    }).pipe(Effect.withSpan("ManifestStore.update"));

  return ManifestStore.of({ create, initialize, update });
});

/** Provides the manifest store, leaving the object store and manifest reader visible. */
export const manifestStoreLayerWithoutDependencies = Layer.effect(ManifestStore, makeManifestStore);

/** Provides the manifest store, the reader, and the R2-backed object store they share. */
export const manifestStoreLayer = manifestStoreLayerWithoutDependencies.pipe(
  Layer.provideMerge(manifestReaderLayerWithoutDependencies),
  Layer.provideMerge(objectStoreLayer),
);

/**
 * Provides the manifest store and reader over one fresh in-memory object store, which the Layer
 * also outputs so tests can inspect or share it.
 */
export const manifestStoreTestLayer = manifestStoreLayerWithoutDependencies.pipe(
  Layer.provideMerge(manifestReaderLayerWithoutDependencies),
  Layer.provideMerge(objectStoreTestLayer),
);
