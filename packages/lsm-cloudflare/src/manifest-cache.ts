import type { DatabaseId, ManifestId } from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Clock, Context, Effect, Layer, Option, Ref } from "effect";

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
   * The cached manifest when it is known to have held every flushed row less than
   * `manifestRefreshInterval` ago; otherwise a fresh load. Never returns an older manifest than one
   * it already returned.
   */
  readonly current: (
    databaseId: DatabaseId,
  ) => Effect.Effect<VersionedManifest, DatabaseNotFoundError | ManifestReadError, RuntimeContext>;
  /** What {@link IManifestCache.current} would return without loading, if anything; no I/O. */
  readonly recent: (databaseId: DatabaseId) => Effect.Effect<Option.Option<VersionedManifest>>;
  /**
   * A manifest holding every row the writer has flushed, for a read that has just asked the writer
   * for `flushedSeq` (a `Latest` probe): the cached manifest when its `lastL0Seq >= flushedSeq`,
   * however long ago it was refreshed, otherwise a fresh load of the newest (D67).
   *
   * **Details**
   *
   * - **Why any covering manifest will do.** Manifests after it differ only by compactions,
   *   collections, and checkpoints, which change no row a read resolves.
   * - **How long one is kept.** A cached manifest was the newest when it was loaded, and is used
   *   for at most half of `gcMinAge` after that. GC keeps the SSTs of a manifest for `gcMinAge`
   *   after it is superseded (D29), so every SST it names is still stored, with half of
   *   `gcMinAge` to spare for the read itself.
   * - **A hit vouches for the manifest.** The writer has just confirmed it holds every flushed
   *   row, so `current` and `recent` serve it for another `manifestRefreshInterval`.
   *
   * @param knownManifestId - The manifest the writer's last flush committed, where a load starts
   *   looking for the newest (`ManifestReader.loadLatestFrom`).
   */
  readonly covering: (
    databaseId: DatabaseId,
    flushedSeq: number,
    knownManifestId: Option.Option<ManifestId>,
  ) => Effect.Effect<VersionedManifest, DatabaseNotFoundError | ManifestReadError, RuntimeContext>;
  /** Loads the newest manifest now and caches it. */
  readonly refresh: (
    databaseId: DatabaseId,
  ) => Effect.Effect<VersionedManifest, DatabaseNotFoundError | ManifestReadError, RuntimeContext>;
  /**
   * Drops the cached manifest, so the next read loads the newest: for a reader that found one of
   * its SSTs missing, which a manifest that was the newest when loaded should never show.
   */
  readonly forget: (databaseId: DatabaseId) => Effect.Effect<void>;
}

/** The contextual manifest cache. */
export class ManifestCache extends Context.Service<ManifestCache, IManifestCache>()(
  "@shaledb/ManifestCache",
) {}

interface CachedManifest {
  readonly versioned: VersionedManifest;
  /** When its load began. It was the newest manifest then, which bounds how long its SSTs stay. */
  readonly loadedAt: number;
  /** When it was last known to hold every flushed row: its load, or a later `covering` hit. */
  readonly confirmedAt: number;
}

/**
 * Constructs the manifest cache over the contextual manifest reader.
 *
 * **Details**
 *
 * Entries are plain manifests in a `Ref`, refreshed lazily on access. Loads run in the caller's
 * fiber, so no I/O outlives the invocation that started it (`docs/design-decisions.md` D17).
 */
export const makeManifestCache = Effect.gen(function* () {
  const reader = yield* ManifestReader;
  const settings = yield* LsmConfiguration;
  const entries = yield* Ref.make(new Map<DatabaseId, CachedManifest>());

  /**
   * How long after its load a cached manifest may be read: half of `gcMinAge` (see `covering`),
   * and never less than the refresh interval `current` has always served it for.
   */
  const maxHeldMs = Math.max(Math.floor(settings.gcMinAge / 2), settings.manifestRefreshInterval);

  const held = (entry: CachedManifest | undefined, now: number): entry is CachedManifest =>
    entry !== undefined && now - entry.loadedAt < maxHeldMs;

  const recentlyConfirmed = (entry: CachedManifest | undefined, now: number) =>
    held(entry, now) && now - entry.confirmedAt < settings.manifestRefreshInterval
      ? Option.some(entry.versioned)
      : Option.none<VersionedManifest>();

  /** Loads the newest manifest with `load` and caches it, unless the cache holds a newer one. */
  const loadWith = (
    databaseId: DatabaseId,
    load: Effect.Effect<
      VersionedManifest,
      DatabaseNotFoundError | ManifestReadError,
      RuntimeContext
    >,
  ) =>
    Effect.gen(function* () {
      // The load's start, not its end: the manifest was the newest at some moment in between.
      const loadedAt = yield* Clock.currentTimeMillis;
      const loaded = yield* load;
      return yield* Ref.modify(
        entries,
        (current): readonly [VersionedManifest, Map<DatabaseId, CachedManifest>] => {
          const previous = current.get(databaseId);
          // A concurrent load that started later already cached a newer manifest: keep it, with
          // its own times.
          if (previous !== undefined && previous.versioned.id > loaded.id) {
            return [previous.versioned, current];
          }
          return [
            loaded,
            new Map(current).set(databaseId, {
              versioned: loaded,
              loadedAt,
              confirmedAt: loadedAt,
            }),
          ];
        },
      );
    });

  const refresh = Effect.fn("ManifestCache.refresh")(function* (databaseId: DatabaseId) {
    // The search starts from the manifest this isolate loaded last, when it has one: on a
    // database that committed nothing since, that is one round trip instead of two.
    const cached = (yield* Ref.get(entries)).get(databaseId);
    return yield* loadWith(
      databaseId,
      cached === undefined
        ? reader.loadLatest(databaseId)
        : reader.loadLatestFrom(databaseId, cached.versioned.id),
    );
  });

  const recent = (databaseId: DatabaseId) =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      return recentlyConfirmed((yield* Ref.get(entries)).get(databaseId), now);
    });

  const current = Effect.fn("ManifestCache.current")(function* (databaseId: DatabaseId) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    const cached = yield* recent(databaseId);
    if (Option.isSome(cached)) {
      yield* Effect.annotateCurrentSpan({ "cache.hit": true, "manifest.id": cached.value.id });
      return cached.value;
    }
    yield* Effect.annotateCurrentSpan({ "cache.hit": false });
    return yield* refresh(databaseId);
  });

  const covering = Effect.fn("ManifestCache.covering")(function* (
    databaseId: DatabaseId,
    flushedSeq: number,
    knownManifestId: Option.Option<ManifestId>,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    const now = yield* Clock.currentTimeMillis;
    const hit = yield* Ref.modify(
      entries,
      (current): readonly [Option.Option<VersionedManifest>, Map<DatabaseId, CachedManifest>] => {
        const entry = current.get(databaseId);
        if (!held(entry, now) || entry.versioned.manifest.lastL0Seq < flushedSeq) {
          return [Option.none(), current];
        }
        return [
          Option.some(entry.versioned),
          new Map(current).set(databaseId, {
            ...entry,
            confirmedAt: Math.max(entry.confirmedAt, now),
          }),
        ];
      },
    );
    if (Option.isSome(hit)) {
      yield* Effect.annotateCurrentSpan({ "cache.hit": true, "manifest.id": hit.value.id });
      return hit.value;
    }
    yield* Effect.annotateCurrentSpan({ "cache.hit": false });
    return yield* loadWith(
      databaseId,
      Option.match(knownManifestId, {
        onNone: () => reader.loadLatest(databaseId),
        onSome: (knownId) => reader.loadLatestFrom(databaseId, knownId),
      }),
    );
  });

  const forget = (databaseId: DatabaseId) =>
    Ref.update(entries, (current) => {
      if (!current.has(databaseId)) return current;
      const next = new Map(current);
      next.delete(databaseId);
      return next;
    });

  return ManifestCache.of({ current, recent, covering, refresh, forget });
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
