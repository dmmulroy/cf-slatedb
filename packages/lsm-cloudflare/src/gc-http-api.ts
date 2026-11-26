import { DatabaseId, ManifestId, Timestamp } from "@shaledb/lsm-core";
import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "effect/http-api";

import { GarbageCollectorUnavailableError } from "./garbage-collector.ts";
import { DatabaseNotFoundError } from "./lsm-errors.ts";

/** Schema for what one database's collection did (see `GarbageCollectionReport`). */
export const GarbageCollectionReportSchema = Schema.Struct({
  databaseId: DatabaseId,
  manifestId: Schema.OptionFromNullOr(ManifestId),
  sstBoundary: Timestamp,
  deletedSsts: Schema.Int,
  deletedManifests: Schema.Int,
  nextCollectionAt: Schema.OptionFromNullOr(Timestamp),
});

/**
 * Operator endpoints of the GC Worker: collect one database now, or every database. The cron
 * trigger runs the second on a schedule.
 */
/**
 * Schema for one page of the maintenance sweep (see `MaintenanceSweepReport`); `reports` holds the
 * removed databases' leftovers it swept.
 */
export const MaintenanceSweepReportSchema = Schema.Struct({
  visited: Schema.Int,
  reports: Schema.Array(GarbageCollectionReportSchema),
  woken: Schema.Int,
  collectionsRequested: Schema.Int,
  cursor: Schema.OptionFromNullOr(Schema.String),
});

export class GcHttpApiGroup extends HttpApiGroup.make("gc")
  .add(
    HttpApiEndpoint.post("collect", "/gc/databases/:databaseId", {
      params: Schema.Struct({ databaseId: DatabaseId }),
      success: GarbageCollectionReportSchema,
      error: [
        DatabaseNotFoundError.pipe(HttpApiSchema.status(404)),
        GarbageCollectorUnavailableError.pipe(HttpApiSchema.status(503)),
      ],
    }),
  )
  .add(
    HttpApiEndpoint.post("sweep", "/gc", {
      success: MaintenanceSweepReportSchema,
      error: GarbageCollectorUnavailableError.pipe(HttpApiSchema.status(503)),
    }),
  ) {}

/** The GC Worker's HTTP API, versioned under `/v1`. */
export class GcHttpApi extends HttpApi.make("GcHttpApi").add(GcHttpApiGroup).prefix("/v1") {}
