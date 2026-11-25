import { DatabaseId, ManifestId, Timestamp } from "@cf-slatedb/lsm-core";
import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "effect/http-api";

import { GarbageCollectorUnavailableError } from "./garbage-collector.ts";
import { DatabaseNotFoundError } from "./lsm-errors.ts";

/** Schema for what one database's collection did (see `GarbageCollectionReport`). */
export const GarbageCollectionReportSchema = Schema.Struct({
  databaseId: DatabaseId,
  manifestId: ManifestId,
  sstBoundary: Timestamp,
  deletedSsts: Schema.Int,
  deletedManifests: Schema.Int,
});

/**
 * Operator endpoints of the GC Worker: collect one database now, or every database. The cron
 * trigger runs the second on a schedule.
 */
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
    HttpApiEndpoint.post("collectAll", "/gc", {
      success: Schema.Struct({ reports: Schema.Array(GarbageCollectionReportSchema) }),
      error: GarbageCollectorUnavailableError.pipe(HttpApiSchema.status(503)),
    }),
  ) {}

/** The GC Worker's HTTP API, versioned under `/v1`. */
export class GcHttpApi extends HttpApi.make("GcHttpApi").add(GcHttpApiGroup).prefix("/v1") {}
