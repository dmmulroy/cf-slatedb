import type { DatabaseId } from "@cf-slatedb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Clock, Context, Effect, Layer, Ref } from "effect";

import {
  LsmConfiguration,
  lsmConfigurationLayer,
  lsmConfigurationTestLayer,
} from "./lsm-configuration.ts";
import type { DatabaseNotFoundError } from "./lsm-errors.ts";
import {
  ManifestReader,
  type ManifestReadError,
  manifestReaderLayer,
  manifestReaderTestLayer,
  type VersionedManifest,
} from "./manifest-reader.ts";

/** A database's newest known manifest, cached in isolate memory. */
export interface IManifestCache {
  /**
   * The cached manifest when it is younger than `manifestRefreshInterval`; otherwise a fresh load.
   * Never returns an older manifest than one it already returned.
   */
  readonly current: (
    databaseId: DatabaseId,
  ) => Effect.Effect<VersionedManifest, DatabaseNotFoundError | ManifestReadError, RuntimeContext>;
  /** Loads the newest manifest now and caches it. */
  readonly refresh: (
    databaseId: DatabaseId,
  ) => Effect.Effect<VersionedManifest, DatabaseNotFoundError | ManifestReadError, RuntimeContext>;
}

/** The contextual manifest cache. */
export class ManifestCache extends Context.Service<ManifestCache, IManifestCache>()(
  "@cf-slatedb/ManifestCache",
) {}

interface CachedManifest {
  readonly versioned: VersionedManifest;
  readonly loadedAt: number;
}

/**
 * Constructs the manifest cache over the contextual manifest reader.
 *
 * **Details**
 *
 * Entries are plain manifests in a `Ref`, refreshed lazily on access once they are older than
 * `manifestRefreshInterval`. Loads run in the caller's fiber, so no I/O outlives the invocation
 * that started it (`docs/design-decisions.md` D17).
 */
export const makeManifestCache = Effect.gen(function* () {
  const reader = yield* ManifestReader;
  const settings = yield* LsmConfiguration;
  const entries = yield* Ref.make(new Map<DatabaseId, CachedManifest>());

  const refresh = Effect.fn("ManifestCache.refresh")(function* (databaseId: DatabaseId) {
    const loaded = yield* reader.loadLatest(databaseId);
    const loadedAt = yield* Clock.currentTimeMillis;
    return yield* Ref.modify(
      entries,
      (current): readonly [VersionedManifest, Map<DatabaseId, CachedManifest>] => {
        const previous = current.get(databaseId);
        const newest =
          previous !== undefined && previous.versioned.id > loaded.id ? previous.versioned : loaded;
        return [newest, new Map(current).set(databaseId, { versioned: newest, loadedAt })];
      },
    );
  });

  const current = Effect.fn("ManifestCache.current")(function* (databaseId: DatabaseId) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    const cached = (yield* Ref.get(entries)).get(databaseId);
    const now = yield* Clock.currentTimeMillis;
    if (cached !== undefined && now - cached.loadedAt < settings.manifestRefreshInterval) {
      yield* Effect.annotateCurrentSpan({ "cache.hit": true, "manifest.id": cached.versioned.id });
      return cached.versioned;
    }
    yield* Effect.annotateCurrentSpan({ "cache.hit": false });
    return yield* refresh(databaseId);
  });

  return ManifestCache.of({ current, refresh });
});

/** Provides the manifest cache, leaving the reader and settings visible. */
export const manifestCacheLayerWithoutDependencies = Layer.effect(ManifestCache, makeManifestCache);

/** Provides the manifest cache over the R2-backed reader. */
export const manifestCacheLayer = manifestCacheLayerWithoutDependencies.pipe(
  Layer.provide([manifestReaderLayer, lsmConfigurationLayer]),
);

/** Provides the manifest cache over a reader of a fresh in-memory object store. */
export const manifestCacheTestLayer = manifestCacheLayerWithoutDependencies.pipe(
  Layer.provide([manifestReaderTestLayer, lsmConfigurationTestLayer()]),
);
