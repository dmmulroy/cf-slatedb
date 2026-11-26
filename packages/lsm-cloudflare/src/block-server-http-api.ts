import { DatabaseId, ManifestId, SstId } from "@shaledb/lsm-core";
import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "effect/http-api";

/** Schema for a byte range in a path: `<offset>-<length>`. */
export const ByteRangeParam = Schema.String.check(Schema.isPattern(/^\d{1,15}-\d{1,10}$/));

const SstParams = Schema.Struct({ databaseId: DatabaseId, sstId: SstId, range: ByteRangeParam });

const Bytes = Schema.Uint8Array.pipe(
  HttpApiSchema.asUint8Array({ contentType: "application/octet-stream" }),
);

/** The requested SST or manifest does not exist (never written, or collected). */
export class BlockNotFoundError extends Schema.TaggedError<BlockNotFoundError>()(
  "BlockNotFoundError",
  {
    key: Schema.String,
    message: Schema.String,
  },
) {}

/** Object storage was unavailable; retry the read. */
export class BlockStorageUnavailableError extends Schema.TaggedError<BlockStorageUnavailableError>()(
  "BlockStorageUnavailableError",
  { key: Schema.String, message: Schema.String },
) {}

const BLOCK_SERVER_ERRORS = [
  BlockNotFoundError.pipe(HttpApiSchema.status(404)),
  BlockStorageUnavailableError.pipe(HttpApiSchema.status(503)),
];

/**
 * Endpoints of the `BlockServer` Worker. Each block, SST metadata section, and manifest has its own
 * URL and an immutable full `200` response, so Workers Cache can cache it. `<databaseId>` is the
 * database that owns the object, which after a fork may differ from the database being read.
 */
export class BlockServerHttpApiGroup extends HttpApiGroup.make("blocks")
  .add(
    HttpApiEndpoint.get("block", "/databases/:databaseId/ssts/:sstId/blocks/:range", {
      params: SstParams,
      success: Bytes,
      error: BLOCK_SERVER_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.get("meta", "/databases/:databaseId/ssts/:sstId/meta/:range", {
      params: SstParams,
      success: Bytes,
      error: BLOCK_SERVER_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.get("latestManifest", "/databases/:databaseId/manifests/latest", {
      params: Schema.Struct({ databaseId: DatabaseId }),
      success: Schema.Struct({ manifestId: Schema.NullOr(ManifestId) }),
      error: BLOCK_SERVER_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.get("manifest", "/databases/:databaseId/manifests/:manifestId", {
      params: Schema.Struct({
        databaseId: DatabaseId,
        manifestId: Schema.NumberFromString.pipe(Schema.decodeTo(ManifestId)),
      }),
      success: Bytes,
      error: BLOCK_SERVER_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("purge", "/databases/:databaseId/purge", {
      params: Schema.Struct({ databaseId: DatabaseId }),
      success: Schema.Struct({ purged: Schema.Boolean }),
      error: BLOCK_SERVER_ERRORS,
    }),
  ) {}

/** The `BlockServer` HTTP API, versioned under `/v1`. Reachable only through its service binding. */
export class BlockServerHttpApi extends HttpApi.make("BlockServerHttpApi")
  .add(BlockServerHttpApiGroup)
  .prefix("/v1") {}

/** Path of one SST data block. */
export const blockPath = (
  owner: DatabaseId,
  sstId: SstId,
  offset: number,
  length: number,
): string => `/v1/databases/${owner}/ssts/${sstId}/blocks/${offset}-${length}`;

/** Path of one SST's metadata section (filter, index, footer, trailer). */
export const metaPath = (owner: DatabaseId, sstId: SstId, offset: number, length: number): string =>
  `/v1/databases/${owner}/ssts/${sstId}/meta/${offset}-${length}`;

/** Parses a `<offset>-<length>` path parameter. */
export const parseByteRangeParam = (range: typeof ByteRangeParam.Type) => {
  const [offset, length] = range.split("-").map(Number);
  return { offset: offset ?? 0, length: length ?? 0 };
};
