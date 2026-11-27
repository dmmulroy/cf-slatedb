import { DatabaseId, externalOwners } from "@shaledb/lsm-core";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, Layer, Option, Schema } from "effect";

import { CompactorClient } from "./compactor-client.ts";
import {
  GarbageCollector,
  type GarbageCollectionReport,
  GarbageCollectorUnavailableError,
} from "./garbage-collector.ts";
import { ManifestReader } from "./manifest-reader.ts";
import { databasePrefix } from "./object-keys.ts";
import { ObjectStore, type ObjectStoreUnavailableError } from "./object-store.ts";

/**
 * Key of the sweep's position: the last database prefix it visited. It lives at the bucket's root,
 * outside `db/`, so it is never taken for a database.
 */
export const MAINTENANCE_SWEEP_CURSOR_KEY = "maintenance/sweep.cursor";

/**
 * A database whose newest manifest is this many ids past GC's manifest boundary has piled up
 * uncollected manifests (its compactor never collected): the sweep asks its compactor to collect.
 */
export const MAX_UNCOLLECTED_MANIFESTS = 16;

/** What one page of the maintenance sweep did. */
export interface MaintenanceSweepReport {
  /** Database prefixes visited on this page. */
  readonly visited: number;
  /** Leftovers of removed databases (prefixes without a manifest) that were swept. */
  readonly sweptLeftovers: ReadonlyArray<GarbageCollectionReport>;
  /** Compactors woken because their database still reads another database's SSTs. */
  readonly woken: number;
  /**
   * Compactors asked to collect: their database's manifests piled up, or it still holds a source
   * it no longer reads.
   */
  readonly collectionsRequested: number;
  /** Where the next page starts; none when this page reached the end (the next starts over). */
  readonly cursor: Option.Option<string>;
}

/**
 * Periodic maintenance for what has no actor of its own (D60), a page of databases at a time.
 *
 * **Details**
 *
 * Each database's compactor collects its garbage from its own alarm; the sweep only covers what that
 * misses, so its cost per run is bounded by the page size, whatever the number of databases:
 *
 * - a prefix with no manifest (a removed database's leftovers) is swept;
 * - a database whose newest manifest still reads another database's SSTs (a fork before its
 *   rewrite) has its compactor woken, so a lost wake only delays the fork's rewrite;
 * - a database that no longer reads them but still holds its source has its compactor asked to
 *   collect: its collection releases the hold once no manifest it keeps names the source (D77);
 * - a database with more than {@link MAX_UNCOLLECTED_MANIFESTS} manifests past GC's boundary has its
 *   compactor asked to collect.
 *
 * The position is kept in {@link MAINTENANCE_SWEEP_CURSOR_KEY}, so consecutive runs visit every
 * database. One database's failure is logged and skipped.
 */
export interface IMaintenanceSweep {
  /**
   * Visits up to `limit` databases after the stored position and stores the new position.
   *
   * @returns What the page did, or `GarbageCollectorUnavailableError` when the bucket could not be
   *   listed or the position kept.
   */
  readonly sweepPage: (
    limit: number,
  ) => Effect.Effect<MaintenanceSweepReport, GarbageCollectorUnavailableError, RuntimeContext>;
}

/** The contextual maintenance sweep. */
export class MaintenanceSweep extends Context.Service<MaintenanceSweep, IMaintenanceSweep>()(
  "@shaledb/MaintenanceSweep",
) {}

const decodeDatabaseId = Schema.decodeUnknownOption(DatabaseId);
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/** What visiting one database did. */
type Visit = "Leftovers" | "Woken" | "CollectionRequested" | "Nothing";

/** Constructs the sweep over the object store, manifest reader, collector, and compactor client. */
export const makeMaintenanceSweep = Effect.gen(function* () {
  const objects = yield* ObjectStore;
  const reader = yield* ManifestReader;
  const collector = yield* GarbageCollector;
  const compactors = yield* CompactorClient;

  const bucketUnavailable = (error: ObjectStoreUnavailableError) =>
    Effect.fail(
      new GarbageCollectorUnavailableError({
        databaseId: "*",
        message: `The maintenance sweep could not read or keep its position: ${error.message} The next run retries.`,
      }),
    );

  const visit = (databaseId: DatabaseId) =>
    Effect.gen(function* () {
      const latest = yield* reader.loadLatest(databaseId).pipe(Effect.option);
      if (Option.isNone(latest)) {
        // Only a prefix with no manifest: a database that has one is collected by its compactor,
        // which knows the compactions running (D96). A manifest this load could not read is left
        // alone; a later sweep visits again.
        const report = yield* collector.collectLeftovers(databaseId);
        return { visit: "Leftovers", report: Option.some(report) };
      }
      const manifest = latest.value.manifest;
      if (manifest.state._tag === "Removed") return { visit: "Nothing", report: Option.none() };
      if (manifest.externalRefs.length > 0 && externalOwners(manifest).size > 0) {
        yield* compactors.wake(databaseId).pipe(Effect.scoped);
        return { visit: "Woken", report: Option.none() };
      }
      // Rewritten but still holding its source: its collection releases the hold once no manifest
      // it keeps names another database's SST (D77), even if its own alarm was lost.
      if (manifest.externalRefs.length > 0) {
        yield* compactors.requestCollection(databaseId).pipe(Effect.scoped);
        return { visit: "CollectionRequested", report: Option.none() };
      }
      const boundary = yield* reader.loadManifestBoundary(databaseId);
      if (latest.value.id - boundary.id > MAX_UNCOLLECTED_MANIFESTS) {
        yield* compactors.requestCollection(databaseId).pipe(Effect.scoped);
        return { visit: "CollectionRequested", report: Option.none() };
      }
      return { visit: "Nothing", report: Option.none<GarbageCollectionReport>() };
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Maintenance of a database failed; a later sweep visits it again.", {
          databaseId,
          cause,
        }).pipe(Effect.as({ visit: "Nothing", report: Option.none<GarbageCollectionReport>() })),
      ),
      Effect.withSpan("MaintenanceSweep.visit", { attributes: { "database.id": databaseId } }),
    );

  const sweepPage = Effect.fn("MaintenanceSweep.sweepPage")(function* (limit: number) {
    const stored = yield* objects
      .get(MAINTENANCE_SWEEP_CURSOR_KEY)
      .pipe(Effect.catchTags({ ObjectStoreUnavailableError: bucketUnavailable }));
    const after = Option.map(stored, (bytes) => textDecoder.decode(bytes));
    const page = yield* objects
      .listPrefixes("db/", after, limit)
      .pipe(Effect.catchTags({ ObjectStoreUnavailableError: bucketUnavailable }));
    const databases = page.prefixes.flatMap((prefix) =>
      Option.toArray(
        Option.filter(
          decodeDatabaseId(prefix.slice("db/".length, -1)),
          (id) => databasePrefix(id) === prefix,
        ),
      ),
    );
    const visits = yield* Effect.forEach(databases, visit, { concurrency: 4 });
    yield* Option.match(page.cursor, {
      onNone: () => objects.delete([MAINTENANCE_SWEEP_CURSOR_KEY]),
      onSome: (cursor) => objects.put(MAINTENANCE_SWEEP_CURSOR_KEY, textEncoder.encode(cursor)),
    }).pipe(Effect.catchTags({ ObjectStoreUnavailableError: bucketUnavailable }));
    const count = (kind: Visit) => visits.filter((visited) => visited.visit === kind).length;
    const report: MaintenanceSweepReport = {
      visited: databases.length,
      sweptLeftovers: visits.flatMap((visited) => Option.toArray(visited.report)),
      woken: count("Woken"),
      collectionsRequested: count("CollectionRequested"),
      cursor: page.cursor,
    };
    yield* Effect.annotateCurrentSpan({
      "maintenance.visited": report.visited,
      "maintenance.leftovers": report.sweptLeftovers.length,
      "maintenance.woken": report.woken,
      "maintenance.collections_requested": report.collectionsRequested,
      "maintenance.wrapped": Option.isNone(page.cursor),
    });
    return report;
  });

  return MaintenanceSweep.of({ sweepPage });
});

/** Provides the sweep, leaving its object store, reader, collector, and compactor client visible. */
export const maintenanceSweepLayerWithoutDependencies = Layer.effect(
  MaintenanceSweep,
  makeMaintenanceSweep,
);
