import {
  DatabaseId,
  hasSstCollectionCandidates,
  nextCollectionTime,
  type Manifest,
  ManifestId,
  planManifestCollection,
  planSstCollection,
  prepareGarbageCollection,
  releasableExternalRefs,
  removeCheckpoint,
  removeExternalRef,
  SstId,
  type StoredManifest,
  type StoredSst,
  Timestamp,
} from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Clock, Context, Effect, Layer, Metric, Option, Result, Schema } from "effect";

import { backgroundWorkTestLayer } from "./background-work.ts";
import { LsmConfiguration, lsmConfigurationTestLayer } from "./lsm-configuration.ts";
import type { DatabaseNotFoundError } from "./lsm-errors.ts";
import {
  ManifestReader,
  manifestReaderLayerWithoutDependencies,
  type VersionedManifest,
} from "./manifest-reader.ts";
import { ManifestStore, manifestStoreLayerWithoutDependencies } from "./manifest-store.ts";
import {
  changelogPrefix,
  compactedPrefix,
  databasePrefix,
  manifestKey,
  manifestPrefix,
} from "./object-keys.ts";
import { type ObjectInfo, ObjectStore, objectStoreTestLayer } from "./object-store.ts";

/** What one collection of a database did. */
export interface GarbageCollectionReport {
  readonly databaseId: DatabaseId;
  /** The newest manifest when collection ran; none for a removed database's leftovers. */
  readonly manifestId: Option.Option<ManifestId>;
  /** SSTs created before this time and referenced by no retained manifest were deleted. */
  readonly sstBoundary: Timestamp;
  readonly deletedSsts: number;
  readonly deletedManifests: number;
  /**
   * When collecting again can delete something this run had to keep (young garbage, an expiring
   * checkpoint); none when nothing will become collectable (D60).
   */
  readonly nextCollectionAt: Option.Option<Timestamp>;
}

/**
 * Collection could not complete because storage or the manifest was unavailable. Retryable: the next
 * run starts over; everything collection does is safe to repeat.
 */
export class GarbageCollectorUnavailableError extends Schema.TaggedError<GarbageCollectorUnavailableError>()(
  "GarbageCollectorUnavailableError",
  { databaseId: Schema.String, message: Schema.String },
) {}

/** Objects GC deleted (SlateDB RFC-0021 parity). */
export const gcDeletedObjects = Metric.counter("lsm_gc_deleted_objects", {
  description: "SST, changelog, and manifest objects garbage collection deleted",
});

/**
 * Deletes a database's unreferenced SSTs and old manifests (design §12: per database, reading only
 * its own manifests and checkpoints).
 *
 * **Details**
 *
 * `collect` runs, in order: list SST objects; if any could be collected, advance the manifest's
 * `gcSstBoundary` to `now - gcMinAge` by CAS (also dropping expired checkpoints and changelog entries
 * past retention); list manifests and keep the latest, every checkpoint's, and every one younger than
 * `gcMinAge`; delete SSTs that no kept manifest references and that were created before the
 * boundary; then advance `gc/manifest.boundary` (SlateDB RFC 0026) and delete the other manifests.
 * An idle database costs listings and no writes. SSTs a fork reads from its source live under the
 * source's prefix, which this database's collection never lists.
 */
export interface IGarbageCollector {
  /**
   * Collects `databaseId`. Only its compactor calls this (D60): it knows the compactions running.
   *
   * @param oldestRunningJobStartedAt - When the oldest compaction job still running started, by
   *   the compactor's clock, or none. The collection then neither deletes an unreferenced SST
   *   created after {@link RUNNING_JOB_CLOCK_MARGIN_MS} before it nor advances `gcSstBoundary`
   *   past that time, so a job running longer than `gcMinAge` keeps its outputs and can commit
   *   (D96).
   */
  readonly collect: (
    databaseId: DatabaseId,
    oldestRunningJobStartedAt: Option.Option<Timestamp>,
  ) => Effect.Effect<
    GarbageCollectionReport,
    GarbageCollectorUnavailableError | DatabaseNotFoundError,
    RuntimeContext
  >;
  /**
   * Deletes what a removed database left under its prefix, once older than `gcMinAge`, when it has
   * no manifest; does nothing to a database that has one (its compactor collects it). For the
   * maintenance sweep.
   */
  readonly collectLeftovers: (
    databaseId: DatabaseId,
  ) => Effect.Effect<GarbageCollectionReport, GarbageCollectorUnavailableError, RuntimeContext>;
}

/**
 * How long before a running job's start (by the compactor's clock) its outputs may have been
 * minted. A job's outputs are named by ULIDs minted by the clock of the Workflow that writes them,
 * which may run behind the compactor's; a job's first output is written after its `prepare` step
 * reads the manifest, at least a manifest load after the start, so a minute covers any skew
 * between Cloudflare machines with room to spare (D96).
 */
export const RUNNING_JOB_CLOCK_MARGIN_MS = 60_000;

/** The contextual garbage collector. */
export class GarbageCollector extends Context.Service<GarbageCollector, IGarbageCollector>()(
  "@shaledb/GarbageCollector",
) {}

const decodeSstId = Schema.decodeUnknownOption(SstId);
const decodeManifestId = Schema.decodeUnknownOption(
  Schema.NumberFromString.pipe(Schema.decodeTo(ManifestId)),
);

/** `<ulid>.sst` (compacted) or `<20-digit seq>-<ulid>.sst` (changelog), under `prefix`. */
const storedSst = (prefix: string, object: ObjectInfo): Option.Option<StoredSst> => {
  const name = object.key.slice(prefix.length);
  if (!name.endsWith(".sst")) return Option.none();
  const stem = name.slice(0, -".sst".length);
  const id = stem.includes("-") ? stem.slice(stem.lastIndexOf("-") + 1) : stem;
  return Option.map(decodeSstId(id), (sstId) => ({ key: object.key, sstId }));
};

/** Constructs the collector over the contextual object store, manifests, and settings. */
export const makeGarbageCollector = Effect.gen(function* () {
  const objects = yield* ObjectStore;
  const reader = yield* ManifestReader;
  const manifests = yield* ManifestStore;
  const settings = yield* LsmConfiguration;

  const unavailable = (databaseId: string) => (error: { readonly message: string }) =>
    Effect.fail(
      new GarbageCollectorUnavailableError({
        databaseId,
        message: `Garbage collection of database ${databaseId} could not reach storage: ${error.message} The next run retries.`,
      }),
    );
  const objectFailures = (databaseId: string) => ({
    ObjectStoreUnavailableError: unavailable(databaseId),
  });

  /** Every object under `prefix`, page by page. */
  const listAll = (prefix: string) =>
    Effect.gen(function* () {
      const all: Array<ObjectInfo> = [];
      let cursor = Option.none<string>();
      do {
        const page = yield* objects.list(prefix, cursor);
        all.push(...page.objects);
        cursor = page.cursor;
      } while (Option.isSome(cursor));
      return all;
    });

  /**
   * A prefix with objects but no manifest is what an interrupted removal (or a compaction that
   * uploaded during one) left: creation writes the first manifest before anything else, and removal
   * deletes manifests last. Deletes its objects older than the cutoff.
   */
  const sweepLeftovers = (databaseId: DatabaseId, cutoff: Timestamp) =>
    Effect.gen(function* () {
      const leftovers = (yield* listAll(databasePrefix(databaseId)).pipe(
        Effect.catchTags(objectFailures(databaseId)),
      )).filter((object) => object.uploadedAt < cutoff);
      yield* objects
        .delete(leftovers.map((object) => object.key))
        .pipe(Effect.catchTags(objectFailures(databaseId)));
      yield* Metric.update(gcDeletedObjects, leftovers.length);
      yield* Effect.annotateCurrentSpan({ "gc.removed_leftovers": leftovers.length });
      return {
        databaseId,
        manifestId: Option.none<ManifestId>(),
        sstBoundary: cutoff,
        deletedSsts: leftovers.length,
        deletedManifests: 0,
        nextCollectionAt: Option.none<Timestamp>(),
      };
    });

  const collectLeftovers = Effect.fn("GarbageCollector.collectLeftovers")(function* (
    databaseId: DatabaseId,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    const now = Timestamp.make(yield* Clock.currentTimeMillis);
    const cutoff = Timestamp.make(Math.max(0, now - settings.gcMinAge));
    const found = yield* reader.loadLatest(databaseId).pipe(
      Effect.map(Option.some),
      Effect.catchTags({
        ManifestReadError: unavailable(databaseId),
        DatabaseNotFoundError: () => Effect.succeed(Option.none<VersionedManifest>()),
      }),
    );
    // A database with a manifest is its compactor's to collect: it knows the running jobs.
    if (Option.isSome(found)) {
      return {
        databaseId,
        manifestId: Option.some(found.value.id),
        sstBoundary: found.value.manifest.gcSstBoundary,
        deletedSsts: 0,
        deletedManifests: 0,
        nextCollectionAt: Option.none<Timestamp>(),
      };
    }
    return yield* sweepLeftovers(databaseId, cutoff);
  });

  const collect = Effect.fn("GarbageCollector.collect")(function* (
    databaseId: DatabaseId,
    oldestRunningJobStartedAt: Option.Option<Timestamp>,
  ) {
    yield* Effect.annotateCurrentSpan({ "database.id": databaseId });
    const now = Timestamp.make(yield* Clock.currentTimeMillis);
    // Nothing is older than gcMinAge yet while the clock itself is younger (fresh test clocks).
    // Nor is anything a running job may have written: its outputs are unreferenced until it
    // commits, and deleting them, or moving the boundary past them, would refuse that commit (D96).
    const cutoff = Timestamp.make(
      Math.max(
        0,
        Math.min(
          now - settings.gcMinAge,
          Option.match(oldestRunningJobStartedAt, {
            onNone: () => Number.POSITIVE_INFINITY,
            onSome: (startedAt) => startedAt - RUNNING_JOB_CLOCK_MARGIN_MS,
          }),
        ),
      ),
    );
    if (Option.isSome(oldestRunningJobStartedAt)) {
      yield* Effect.annotateCurrentSpan({
        "gc.oldest_running_job_started_at": oldestRunningJobStartedAt.value,
      });
    }
    const objectFailed = objectFailures(databaseId);

    const ssts: Array<StoredSst> = [];
    for (const prefix of [compactedPrefix(databaseId), changelogPrefix(databaseId)]) {
      for (const object of yield* listAll(prefix).pipe(Effect.catchTags(objectFailed))) {
        ssts.push(...Option.toArray(storedSst(prefix, object)));
      }
    }
    const found = yield* reader.loadLatest(databaseId).pipe(
      Effect.map(Option.some),
      Effect.catchTags({
        ManifestReadError: unavailable(databaseId),
        DatabaseNotFoundError: () => Effect.succeed(Option.none<VersionedManifest>()),
      }),
    );
    if (Option.isNone(found)) return yield* sweepLeftovers(databaseId, cutoff);
    const latest = found.value;
    const advanceBoundary = hasSstCollectionCandidates(latest.manifest, ssts, cutoff);
    const committed = yield* manifests
      .update(databaseId, (manifest) =>
        Result.succeed(prepareGarbageCollection(manifest, { now, cutoff, advanceBoundary })),
      )
      .pipe(
        Effect.catchTags({
          ManifestReadError: unavailable(databaseId),
          ManifestWriteError: unavailable(databaseId),
        }),
      );

    // Listed after the commit, so every manifest that could reference an SST created before the
    // boundary is either listed or newer than the commit (and kept).
    const storedManifests: Array<StoredManifest> = [];
    for (const object of yield* listAll(manifestPrefix(databaseId)).pipe(
      Effect.catchTags(objectFailed),
    )) {
      const id = decodeManifestId(object.key.slice(manifestPrefix(databaseId).length));
      if (Option.isSome(id)) storedManifests.push({ id: id.value, uploadedAt: object.uploadedAt });
    }
    const plan = planManifestCollection({
      latestId: committed.id,
      latest: committed.manifest,
      stored: storedManifests,
      now,
      minAge: settings.gcMinAge,
    });
    const retained: Array<Manifest> = [committed.manifest];
    const missing: Array<ManifestId> = [];
    for (const id of plan.retained) {
      if (id === committed.id) continue;
      const manifest = yield* reader
        .load(databaseId, id)
        .pipe(Effect.catchTags({ ManifestReadError: unavailable(databaseId) }));
      if (Option.isNone(manifest)) missing.push(id);
      retained.push(...Option.toArray(manifest));
    }
    const everyRetainedLoaded = missing.length === 0;
    // A retained manifest listed a moment ago and gone now. Another collection deletes manifests
    // only after moving `gc/manifest.boundary` past them, so one at or below the boundary was
    // collected by a run that judged it deletable later than this one, and every SST it alone
    // names is one that run deletes too (D83). One above the boundary was deleted by something
    // else (only a removal of the database may): delete nothing rather than act without it.
    if (!everyRetainedLoaded) {
      const boundary = yield* reader
        .loadManifestBoundary(databaseId)
        .pipe(Effect.catchTags({ ManifestReadError: unavailable(databaseId) }));
      const unexplained = missing.filter((id) => id > boundary.id);
      if (unexplained.length > 0) {
        return yield* new GarbageCollectorUnavailableError({
          databaseId,
          message: `Garbage collection of database ${databaseId} found manifest ${unexplained.join(", ")} missing above GC's manifest boundary (${boundary.id}), where only removing the database deletes manifests, so it deleted nothing. The next run lists the manifests again and collects without it: if the database is not being removed, find what deleted the manifest first.`,
        });
      }
    }
    // A fork releases its hold on its source once no manifest kept here names another database's
    // SST (D77). Each step is an idempotent CAS: first the owner's checkpoint, then the reference,
    // so a crash between them leaves the reference, and the next collection releases again.
    const released = everyRetainedLoaded
      ? releasableExternalRefs(committed.manifest, retained)
      : [];
    for (const ref of released) {
      yield* manifests
        .update(ref.owner, (manifest) =>
          Result.succeed(removeCheckpoint(manifest, ref.checkpointId)),
        )
        .pipe(
          Effect.catchTags({
            DatabaseNotFoundError: () => Effect.void,
            ManifestReadError: unavailable(databaseId),
            ManifestWriteError: unavailable(databaseId),
          }),
        );
      yield* manifests
        .update(databaseId, (manifest) => Result.succeed(removeExternalRef(manifest, ref)))
        .pipe(
          Effect.catchTags({
            ManifestReadError: unavailable(databaseId),
            ManifestWriteError: unavailable(databaseId),
          }),
        );
    }
    const sstBoundary =
      committed.manifest.gcSstBoundary < cutoff ? committed.manifest.gcSstBoundary : cutoff;
    const deletedSsts = planSstCollection({ retained, stored: ssts, boundary: sstBoundary });
    yield* objects
      .delete(deletedSsts.map((object) => object.key))
      .pipe(Effect.catchTags(objectFailed));

    const newestDeletable = plan.deletable.reduce((max, id) => Math.max(max, id), 0);
    if (newestDeletable > 0) {
      yield* manifests
        .advanceManifestBoundary(databaseId, ManifestId.make(newestDeletable))
        .pipe(Effect.catchTags({ ManifestWriteError: unavailable(databaseId) }));
      // Readers start probing above this boundary, so they never stop at a gap these deletes
      // leave, whatever the LATEST hint says (D56).
      yield* objects
        .delete(plan.deletable.map((id) => manifestKey(databaseId, id)))
        .pipe(Effect.catchTags(objectFailed));
    }
    yield* Metric.update(gcDeletedObjects, deletedSsts.length + plan.deletable.length);
    yield* Effect.annotateCurrentSpan({
      "manifest.id": committed.id,
      "gc.sst_boundary": sstBoundary,
      "gc.boundary_advanced": advanceBoundary,
      "gc.deleted_ssts": deletedSsts.length,
      "gc.deleted_manifests": plan.deletable.length,
      "fork.released_owner": released.map((ref) => ref.owner).join(","),
    });
    const nextCollectionAt = nextCollectionTime({
      latestId: committed.id,
      latest: committed.manifest,
      retainedIds: plan.retained,
      retained,
      stored: ssts,
      boundary: sstBoundary,
      now,
      minAge: settings.gcMinAge,
    });
    yield* Effect.annotateCurrentSpan({
      "gc.next_collection_at": Option.getOrElse(nextCollectionAt, () => 0),
    });
    return {
      databaseId,
      manifestId: Option.some(committed.id),
      sstBoundary,
      deletedSsts: deletedSsts.length,
      deletedManifests: plan.deletable.length,
      nextCollectionAt,
    };
  });

  return GarbageCollector.of({ collect, collectLeftovers });
});

/**
 * Provides the collector, leaving the object store, settings, and background work runner visible.
 */
export const garbageCollectorLayerWithoutDependencies = Layer.effect(
  GarbageCollector,
  makeGarbageCollector,
).pipe(
  Layer.provide(
    manifestStoreLayerWithoutDependencies.pipe(
      Layer.provideMerge(manifestReaderLayerWithoutDependencies),
    ),
  ),
);

/** Provides the collector over a fresh in-memory object store (also output) and test settings. */
export const garbageCollectorTestLayer = (
  overrides: Parameters<typeof lsmConfigurationTestLayer>[0] = {},
) =>
  garbageCollectorLayerWithoutDependencies.pipe(
    Layer.provide(backgroundWorkTestLayer),
    Layer.provideMerge(Layer.mergeAll(objectStoreTestLayer, lsmConfigurationTestLayer(overrides))),
  );
