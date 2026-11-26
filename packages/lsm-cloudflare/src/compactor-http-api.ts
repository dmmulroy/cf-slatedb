import { DatabaseId, SstNotInManifestError } from "@shaledb/lsm-core";
import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "effect/http-api";

import {
  CompactorState,
  CompactorUnavailableError,
  SstDropRefusedError,
  SstDropReport,
  SstDropRequest,
} from "./compactor.ts";
import { GarbageCollectorUnavailableError } from "./garbage-collector.ts";
import { GarbageCollectionReportSchema } from "./gc-http-api.ts";
import { DatabaseIdMismatchError, DatabaseNotFoundError } from "./lsm-errors.ts";

const DatabaseParams = Schema.Struct({ databaseId: DatabaseId });

/** Schema for a compactor's status: its state, or null when it was never woken. */
export const CompactorStatusResponse = Schema.Struct({
  state: Schema.OptionFromNullOr(CompactorState),
});

/**
 * Every error a compactor endpoint can return: `400` for a request routed to the wrong compactor,
 * `404` for an unknown database, `503` when the manifest is unreachable (retryable).
 */
const COMPACTOR_ERRORS = [
  DatabaseIdMismatchError.pipe(HttpApiSchema.status(400)),
  DatabaseNotFoundError.pipe(HttpApiSchema.status(404)),
  CompactorUnavailableError.pipe(HttpApiSchema.status(503)),
];

/** Endpoints of a database's compactor Durable Object (`CompactorServer`). */
export class CompactorHttpApiGroup extends HttpApiGroup.make("compactor")
  .add(
    HttpApiEndpoint.post("wake", "/databases/:databaseId/compactor/wake", {
      params: DatabaseParams,
      success: CompactorState,
      error: COMPACTOR_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("destroy", "/databases/:databaseId/compactor/destroy", {
      params: DatabaseParams,
      success: HttpApiSchema.NoContent,
      error: COMPACTOR_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("collect", "/databases/:databaseId/compactor/collect", {
      params: DatabaseParams,
      success: GarbageCollectionReportSchema,
      error: [
        ...COMPACTOR_ERRORS,
        GarbageCollectorUnavailableError.pipe(HttpApiSchema.status(503)),
      ],
    }),
  )
  .add(
    HttpApiEndpoint.post("requestCollection", "/databases/:databaseId/compactor/collection", {
      params: DatabaseParams,
      success: HttpApiSchema.NoContent,
      error: COMPACTOR_ERRORS,
    }),
  )
  .add(
    HttpApiEndpoint.post("dropSst", "/databases/:databaseId/compactor/drop-sst", {
      params: DatabaseParams,
      payload: SstDropRequest,
      success: SstDropReport,
      error: [
        ...COMPACTOR_ERRORS,
        SstNotInManifestError.pipe(HttpApiSchema.status(404)),
        SstDropRefusedError.pipe(HttpApiSchema.status(409)),
      ],
    }),
  )
  .add(
    HttpApiEndpoint.get("status", "/databases/:databaseId/compactor", {
      params: DatabaseParams,
      success: CompactorStatusResponse,
      error: COMPACTOR_ERRORS,
    }),
  ) {}

/** The compactor Durable Object's HTTP API, versioned under `/v1`. */
export class CompactorHttpApi extends HttpApi.make("CompactorHttpApi")
  .add(CompactorHttpApiGroup)
  .prefix("/v1") {}
