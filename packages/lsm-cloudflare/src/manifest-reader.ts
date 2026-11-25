import { type DatabaseId, Manifest, ManifestId, ManifestJson } from "@cf-slatedb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Data, Effect, Layer, Option, Schema } from "effect";

import { DatabaseNotFoundError } from "./lsm-errors.ts";
import { latestManifestKey, manifestKey, manifestPrefix } from "./object-keys.ts";
import {
  ObjectStore,
  objectStoreLayer,
  objectStoreTestLayer,
  type ObjectStoreUnavailableError,
} from "./object-store.ts";

/** A manifest with its id in the database's manifest chain. */
export interface VersionedManifest {
  readonly id: ManifestId;
  readonly manifest: Manifest;
}

/**
 * A manifest could not be read.
 *
 * **Details**
 *
 * `StorageUnavailable` is retryable. `StoredManifestInvalid` means a stored manifest does not
 * decode or names another database; it is not retryable and needs an operator.
 */
export class ManifestReadError extends Data.TaggedError("ManifestReadError")<{
  readonly databaseId: DatabaseId;
  readonly manifestId: Option.Option<ManifestId>;
  readonly reason: "StorageUnavailable" | "StoredManifestInvalid";
  readonly message: string;
}> {}

/** Reads a database's manifests. */
export interface IManifestReader {
  /**
   * Finds and reads the newest manifest: reads the `manifest/LATEST` hint, then probes `id + 1`
   * until one is missing. Never lists objects on this path.
   *
   * @returns The newest manifest, `DatabaseNotFoundError` when the database has none, or
   *   `ManifestReadError`.
   */
  readonly loadLatest: (
    databaseId: DatabaseId,
  ) => Effect.Effect<VersionedManifest, DatabaseNotFoundError | ManifestReadError, RuntimeContext>;
  /**
   * Reads one manifest by id, as for a checkpoint.
   *
   * @returns The manifest, none when it does not exist (never written, or collected), or
   *   `ManifestReadError`.
   */
  readonly load: (
    databaseId: DatabaseId,
    manifestId: ManifestId,
  ) => Effect.Effect<Option.Option<Manifest>, ManifestReadError, RuntimeContext>;
}

/** The contextual manifest reader. */
export class ManifestReader extends Context.Service<ManifestReader, IManifestReader>()(
  "@cf-slatedb/ManifestReader",
) {}

const decodeManifestJson = Schema.decodeEffect(ManifestJson);
const decodeManifestIdText = Schema.decodeEffect(
  Schema.NumberFromString.pipe(Schema.decodeTo(ManifestId)),
);
const textDecoder = new TextDecoder();

/**
 * Decodes a stored manifest body and checks that it belongs to `databaseId`.
 *
 * @returns The manifest, or `ManifestReadError` (`StoredManifestInvalid`).
 */
export const decodeStoredManifest = (
  databaseId: DatabaseId,
  manifestId: ManifestId,
  body: Uint8Array,
) =>
  decodeManifestJson(textDecoder.decode(body)).pipe(
    Effect.catchTags({
      SchemaError: (error) =>
        Effect.fail(
          new ManifestReadError({
            databaseId,
            manifestId: Option.some(manifestId),
            reason: "StoredManifestInvalid",
            message: `Manifest ${manifestId} of database ${databaseId} is not a valid manifest (${error.message}). An operator must restore it.`,
          }),
        ),
    }),
    Effect.flatMap((manifest) =>
      manifest.databaseId === databaseId
        ? Effect.succeed(manifest)
        : Effect.fail(
            new ManifestReadError({
              databaseId,
              manifestId: Option.some(manifestId),
              reason: "StoredManifestInvalid",
              message: `Manifest ${manifestId} under database ${databaseId}'s prefix names database ${manifest.databaseId}. An operator must restore it.`,
            }),
          ),
    ),
  );

/**
 * Constructs the manifest reader over the contextual object store.
 */
export const makeManifestReader = Effect.gen(function* () {
  const objects = yield* ObjectStore;

  // The wrapped calls fail only with `ObjectStoreUnavailableError`, which has one meaning here.
  const storageUnavailable = (databaseId: DatabaseId, manifestId: Option.Option<ManifestId>) =>
    Effect.mapError(
      (error: ObjectStoreUnavailableError) =>
        new ManifestReadError({
          databaseId,
          manifestId,
          reason: "StorageUnavailable",
          message: `Reading database ${databaseId}'s manifest failed: ${error.message}`,
        }),
    );

  const load = Effect.fn("ManifestReader.load")(function* (
    databaseId: DatabaseId,
    manifestId: ManifestId,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId, "manifest.id": manifestId });
    const body = yield* objects
      .get(manifestKey(databaseId, manifestId))
      .pipe(storageUnavailable(databaseId, Option.some(manifestId)));
    if (Option.isNone(body)) return Option.none<Manifest>();
    return Option.some(yield* decodeStoredManifest(databaseId, manifestId, body.value));
  });

  /** Recovery when the hint names a collected manifest: find the newest by listing (not a read path). */
  const newestByListing = Effect.fn("ManifestReader.newestByListing")(function* (
    databaseId: DatabaseId,
  ) {
    let cursor = Option.none<string>();
    let newest = Option.none<ManifestId>();
    do {
      const page = yield* objects
        .list(manifestPrefix(databaseId), cursor)
        .pipe(storageUnavailable(databaseId, Option.none()));
      for (const object of page.objects) {
        const id = Number(object.key.slice(manifestPrefix(databaseId).length));
        if (Number.isSafeInteger(id) && id > 0 && (Option.isNone(newest) || id > newest.value))
          newest = Option.some(ManifestId.make(id));
      }
      cursor = page.cursor;
    } while (Option.isSome(cursor));
    return newest;
  });

  const loadLatest = Effect.fn("ManifestReader.loadLatest")(function* (databaseId: DatabaseId) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    const hintBody = yield* objects
      .get(latestManifestKey(databaseId))
      .pipe(storageUnavailable(databaseId, Option.none()));
    const hint = Option.isSome(hintBody)
      ? yield* decodeManifestIdText(textDecoder.decode(hintBody.value)).pipe(Effect.option)
      : Option.none<ManifestId>();
    let id = Option.getOrElse(hint, () => ManifestId.make(1));
    let manifest = yield* load(databaseId, id);
    if (Option.isNone(manifest)) {
      if (Option.isNone(hint)) {
        return yield* new DatabaseNotFoundError({
          databaseId,
          message: `Database ${databaseId} does not exist. Create it before reading or writing it.`,
        });
      }
      const newest = yield* newestByListing(databaseId);
      if (Option.isNone(newest)) {
        return yield* new DatabaseNotFoundError({
          databaseId,
          message: `Database ${databaseId} does not exist. Create it before reading or writing it.`,
        });
      }
      id = newest.value;
      manifest = yield* load(databaseId, id);
    }
    let probes = 0;
    for (;;) {
      const next = yield* load(databaseId, ManifestId.make(id + 1));
      probes++;
      if (Option.isNone(next)) break;
      id = ManifestId.make(id + 1);
      manifest = next;
    }
    yield* Effect.annotateCurrentSpan({ "manifest.id": id, "manifest.probes": probes });
    if (Option.isNone(manifest)) {
      return yield* new ManifestReadError({
        databaseId,
        manifestId: Option.some(id),
        reason: "StorageUnavailable",
        message: `Manifest ${id} of database ${databaseId} disappeared while it was being read. Retry the read.`,
      });
    }
    return { id, manifest: manifest.value };
  });

  return ManifestReader.of({ load, loadLatest });
});

/** Provides the manifest reader, leaving the object store requirement visible. */
export const manifestReaderLayerWithoutDependencies = Layer.effect(
  ManifestReader,
  makeManifestReader,
);

/** Provides the manifest reader over the R2-backed object store. */
export const manifestReaderLayer = manifestReaderLayerWithoutDependencies.pipe(
  Layer.provide(objectStoreLayer),
);

/** Provides the manifest reader over a fresh in-memory object store. */
export const manifestReaderTestLayer = manifestReaderLayerWithoutDependencies.pipe(
  Layer.provide(objectStoreTestLayer),
);
