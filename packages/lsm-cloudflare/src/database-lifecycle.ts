import {
  type DatabaseId,
  type DatabaseOptions,
  initialManifest,
  type KeyRange,
} from "@cf-slatedb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, type Option } from "effect";

import { WriterUnavailableError } from "./lsm-errors.ts";
import type { VersionedManifest } from "./manifest-reader.ts";
import {
  type DatabaseAlreadyExistsError,
  ManifestStore,
  manifestStoreLayer,
  manifestStoreTestLayer,
  type ManifestWriteError,
} from "./manifest-store.ts";

/** Options of a new database. */
export interface CreateDatabaseOptions {
  /** Keys the database accepts; writes outside fail with `KeyOutOfRangeError`. None: every key. */
  readonly keyRange: Option.Option<KeyRange>;
  readonly databaseOptions: DatabaseOptions;
}

/** Creates, seals, forks, and removes databases. Every step is a manifest CAS. */
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
}

/** The contextual database lifecycle. */
export class DatabaseLifecycle extends Context.Service<DatabaseLifecycle, IDatabaseLifecycle>()(
  "@cf-slatedb/DatabaseLifecycle",
) {}

/** Constructs the database lifecycle over the contextual manifest store. */
export const makeDatabaseLifecycle = Effect.gen(function* () {
  const manifests = yield* ManifestStore;

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

  return DatabaseLifecycle.of({ create });
});

/** Provides the database lifecycle, leaving the manifest store visible. */
export const databaseLifecycleLayerWithoutDependencies = Layer.effect(
  DatabaseLifecycle,
  makeDatabaseLifecycle,
);

/** Provides the database lifecycle over the R2-backed manifest store. */
export const databaseLifecycleLayer = databaseLifecycleLayerWithoutDependencies.pipe(
  Layer.provide(manifestStoreLayer),
);

/** Provides the database lifecycle over an in-memory manifest store. */
export const databaseLifecycleTestLayer = databaseLifecycleLayerWithoutDependencies.pipe(
  Layer.provide(manifestStoreTestLayer),
);
