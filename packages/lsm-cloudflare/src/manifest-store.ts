import { type DatabaseId, type Manifest, ManifestId, ManifestJson } from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Data, Effect, Layer, Metric, Option, Result, Schedule, Schema } from "effect";

import { BackgroundWork, backgroundWorkTestLayer } from "./background-work.ts";
import type { DatabaseNotFoundError } from "./lsm-errors.ts";
import {
  ManifestReader,
  type ManifestReadError,
  manifestReaderLayerWithoutDependencies,
  type VersionedManifest,
} from "./manifest-reader.ts";
import { latestManifestKey, manifestGcBoundaryKey, manifestKey } from "./object-keys.ts";
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
 * written exactly once and concurrent writers serialize. GC deletes old manifests, after which
 * create-if-absent no longer remembers their ids, so a create also checks GC's manifest boundary
 * (`gc/manifest.boundary`, SlateDB RFC 0026): an id at or below it is a conflict, not a commit.
 * After a successful create, the `manifest/LATEST` hint is advanced in the background
 * (`BackgroundWork`): it may land late or never, and readers do not depend on it (D56).
 */
export interface IManifestStore {
  /**
   * Creates manifest `manifestId`.
   *
   * @returns Nothing, `ManifestConflictError` when the id already exists or is at or below GC's
   *   manifest boundary, or `ManifestWriteError`.
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
   * The edit receives the newest manifest and its id. An edit returning the same manifest instance
   * means "already applied": nothing is written and the current manifest is returned. Conflicts retry with jittered exponential backoff, at most 12
   * attempts.
   *
   * @returns The manifest after the edit, the edit's own error, `DatabaseNotFoundError`,
   *   `ManifestReadError`, or `ManifestWriteError`.
   */
  readonly update: <E>(
    databaseId: DatabaseId,
    edit: (manifest: Manifest, manifestId: ManifestId) => Result.Result<Manifest, E>,
  ) => Effect.Effect<
    VersionedManifest,
    E | DatabaseNotFoundError | ManifestReadError | ManifestWriteError,
    RuntimeContext
  >;
  /**
   * Like {@link IManifestStore.update}, but the first attempt applies the edit to `view`, the
   * newest manifest the caller knows (the one it last loaded or committed), without loading.
   *
   * **Details**
   *
   * For committers that keep their own view, such as a writer between flushes: when nothing else
   * committed since, the commit costs one create and no load. Only a written commit is trusted
   * from `view` (create-if-absent and the boundary re-read prove it was the newest). An edit that
   * fails or reports "already applied" against `view`, or a create that conflicts, reloads and
   * continues as `update` does, since an older view can give a wrong answer for those.
   *
   * @returns As {@link IManifestStore.update}.
   */
  readonly updateFrom: <E>(
    view: VersionedManifest,
    edit: (manifest: Manifest, manifestId: ManifestId) => Result.Result<Manifest, E>,
  ) => Effect.Effect<
    VersionedManifest,
    E | DatabaseNotFoundError | ManifestReadError | ManifestWriteError,
    RuntimeContext
  >;
  /**
   * Advances GC's manifest boundary to at least `manifestId` (it never moves back). GC calls this
   * before deleting manifests at or below `manifestId`.
   */
  readonly advanceManifestBoundary: (
    databaseId: DatabaseId,
    manifestId: ManifestId,
  ) => Effect.Effect<void, ManifestWriteError, RuntimeContext>;
}

/** The contextual manifest store. */
export class ManifestStore extends Context.Service<ManifestStore, IManifestStore>()(
  "@shaledb/ManifestStore",
) {}

const encodeManifestJson = Schema.encodeSync(ManifestJson);
const textEncoder = new TextEncoder();

/** Manifest CAS attempts that lost to a concurrent edit (SlateDB RFC-0021 parity). */
export const manifestCasConflicts = Metric.counter("lsm_manifest_cas_conflicts", {
  description: "Manifest create-if-absent attempts that lost to a concurrent edit",
});

const CAS_ATTEMPTS = 12;

/** Attempts to advance the `LATEST` hint when another writer moved it concurrently. */
const HINT_ATTEMPTS = 3;

const decodeManifestIdText = Schema.decodeUnknownOption(
  Schema.NumberFromString.pipe(Schema.decodeTo(ManifestId)),
);
const textDecoder = new TextDecoder();

/**
 * Constructs the manifest store over the contextual object store, manifest reader, and background
 * work runner.
 */
export const makeManifestStore = Effect.gen(function* () {
  const objects = yield* ObjectStore;
  const reader = yield* ManifestReader;
  const background = yield* BackgroundWork;

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

  /** GC's manifest boundary and its object version; 0 and none before GC first advances it. */
  const readManifestBoundary = (databaseId: DatabaseId) =>
    reader.loadManifestBoundary(databaseId).pipe(
      Effect.mapError(
        (error) =>
          new ManifestWriteError({
            databaseId,
            reason: "StorageUnavailable",
            message: error.message,
          }),
      ),
    );

  /**
   * Advances the `LATEST` hint to `manifestId` unless it already names a newer manifest, so a hint
   * write that lands late never moves it back. Best effort: an outage or repeated races leave it
   * behind, which only costs readers a probe.
   */
  const advanceLatestHint = (databaseId: DatabaseId, manifestId: ManifestId) => {
    const key = latestManifestKey(databaseId);
    const body = textEncoder.encode(String(manifestId));
    const attempt = Effect.gen(function* () {
      const current = yield* objects.getVersioned(key);
      if (Option.isNone(current)) return (yield* objects.putIfAbsent(key, body)) === "Created";
      const hinted = decodeManifestIdText(textDecoder.decode(current.value.bytes));
      if (Option.isSome(hinted) && hinted.value >= manifestId) return true;
      return (yield* objects.putIfVersion(key, body, current.value.version)) === "Written";
    });
    return attempt.pipe(
      Effect.repeat({ until: (done) => done, times: HINT_ATTEMPTS - 1 }),
      Effect.asVoid,
      Effect.catchTags({ ObjectStoreUnavailableError: () => Effect.void }),
      Effect.annotateSpans({ "database.id": databaseId, "manifest.id": manifestId }),
    );
  };

  const advanceManifestBoundary = Effect.fn("ManifestStore.advanceManifestBoundary")(function* (
    databaseId: DatabaseId,
    manifestId: ManifestId,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId, "manifest.id": manifestId });
    const key = manifestGcBoundaryKey(databaseId);
    const body = textEncoder.encode(String(manifestId));
    const attempt = Effect.gen(function* () {
      const current = yield* readManifestBoundary(databaseId);
      if (current.id >= manifestId) return true;
      const outcome = Option.isNone(current.version)
        ? (yield* objects.putIfAbsent(key, body).pipe(writeUnavailable(databaseId))) === "Created"
        : (yield* objects
            .putIfVersion(key, body, current.version.value)
            .pipe(writeUnavailable(databaseId))) === "Written";
      return outcome;
    });
    // A concurrent GC moved it first: re-read; it only ever moves forward.
    const advanced = yield* attempt.pipe(
      Effect.repeat({ until: (done) => done, times: CAS_ATTEMPTS - 1 }),
    );
    if (!advanced) {
      return yield* new ManifestWriteError({
        databaseId,
        reason: "ContentionPersisted",
        message: `Database ${databaseId}'s GC manifest boundary changed concurrently on every one of ${CAS_ATTEMPTS} attempts. Retry the collection later.`,
      });
    }
  });

  /**
   * One create attempt: `Created`, or a conflict (the id exists, or is at or below GC's manifest
   * boundary) as `Conflict` with its message. A conflict is the CAS working, not a failure, so it is
   * recorded as the span's `manifest.outcome` rather than an error status.
   */
  const attemptCreate = Effect.fn("ManifestStore.create")(function* (
    databaseId: DatabaseId,
    manifestId: ManifestId,
    manifest: Manifest,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId, "manifest.id": manifestId });
    const conflict = (message: string) =>
      Effect.gen(function* () {
        yield* Metric.update(manifestCasConflicts, 1);
        yield* Effect.annotateCurrentSpan({ "manifest.outcome": "Conflict" });
        return Result.fail(new ManifestConflictError({ databaseId, manifestId, message }));
      });
    const outcome = yield* objects
      .putIfAbsent(
        manifestKey(databaseId, manifestId),
        textEncoder.encode(encodeManifestJson(manifest)),
      )
      .pipe(writeUnavailable(databaseId));
    if (outcome === "AlreadyExists") {
      return yield* conflict(
        `Manifest ${manifestId} of database ${databaseId} was created by a concurrent edit. Reload and reapply.`,
      );
    }
    // Checked after the create: GC advances the boundary before deleting, so an id it may have
    // deleted (and that this create could have reused) is at or below it.
    const boundary = yield* readManifestBoundary(databaseId);
    if (manifestId <= boundary.id) {
      yield* Effect.annotateCurrentSpan({ "manifest.gc_boundary": boundary.id });
      return yield* conflict(
        `Manifest ${manifestId} of database ${databaseId} is at or below GC's manifest boundary ${boundary.id}: the id was used before and its manifest collected. Reload and reapply.`,
      );
    }
    yield* Effect.annotateCurrentSpan({ "manifest.outcome": "Created" });
    // Off the critical path: the hint may land late or never; readers probe past it (D56).
    yield* background.run(
      "ManifestStore.advanceLatestHint",
      advanceLatestHint(databaseId, manifestId),
    );
    return Result.succeed(undefined);
  });

  const create = (databaseId: DatabaseId, manifestId: ManifestId, manifest: Manifest) =>
    attemptCreate(databaseId, manifestId, manifest).pipe(Effect.flatMap(Effect.fromResult));

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

  /**
   * The CAS loop of `update` and `updateFrom`. The first attempt applies the edit to `ownView` when
   * there is one and trusts only a written commit from it; every other attempt loads the newest
   * manifest.
   */
  const commitEdit = <E>(
    databaseId: DatabaseId,
    edit: (manifest: Manifest, manifestId: ManifestId) => Result.Result<Manifest, E>,
    ownView: Option.Option<VersionedManifest>,
  ) =>
    Effect.gen(function* () {
      yield* Effect.annotateCurrentSpan({
        "database.id": databaseId,
        "manifest.start": Option.isSome(ownView) ? "OwnView" : "Loaded",
      });
      let attempt = 0;
      if (Option.isSome(ownView)) {
        attempt++;
        const view = ownView.value;
        const edited = edit(view.manifest, view.id);
        if (Result.isSuccess(edited) && edited.success !== view.manifest) {
          const nextId = ManifestId.make(view.id + 1);
          const committedFromView = Result.isSuccess(
            yield* attemptCreate(databaseId, nextId, edited.success),
          );
          if (committedFromView) {
            yield* Effect.annotateCurrentSpan({
              "manifest.id": nextId,
              "manifest.cas_attempts": attempt,
            });
            return { id: nextId, manifest: edited.success };
          }
        }
      }
      // The edit's own failure travels in the success channel, so the CAS loop's error channel
      // holds only known tags.
      const once = Effect.gen(function* () {
        attempt++;
        const current = yield* reader.loadLatest(databaseId);
        const edited = edit(current.manifest, current.id);
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

  const update = <E>(
    databaseId: DatabaseId,
    edit: (manifest: Manifest, manifestId: ManifestId) => Result.Result<Manifest, E>,
  ) => commitEdit(databaseId, edit, Option.none());

  const updateFrom = <E>(
    view: VersionedManifest,
    edit: (manifest: Manifest, manifestId: ManifestId) => Result.Result<Manifest, E>,
  ) => commitEdit(view.manifest.databaseId, edit, Option.some(view));

  return ManifestStore.of({ create, initialize, update, updateFrom, advanceManifestBoundary });
});

/**
 * Provides the manifest store, leaving the object store, manifest reader, and background work runner
 * visible.
 */
export const manifestStoreLayerWithoutDependencies = Layer.effect(ManifestStore, makeManifestStore);

/**
 * Provides the manifest store, the reader, and the R2-backed object store they share, leaving the
 * background work runner to the host (a Worker, Durable Object, or Workflow provides its own).
 */
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
  Layer.provide(backgroundWorkTestLayer),
);
