import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "effect/http-api";

/** Schema for a lease request: the job holding it and its budget group. */
export const LeaseRequest = Schema.Struct({ holder: Schema.String, group: Schema.String });

/** Schema for a lease decision. */
export const LeaseResponse = Schema.Struct({ granted: Schema.Boolean });

/** Schema for a lease release. */
export const ReleaseRequest = Schema.Struct({ holder: Schema.String });

/** Endpoints of the global compaction budget Durable Object (`CompactionBudgetServer`). */
export class CompactionBudgetHttpApiGroup extends HttpApiGroup.make("budget")
  .add(
    HttpApiEndpoint.post("acquire", "/compaction-budget/leases", {
      payload: LeaseRequest,
      success: LeaseResponse,
    }),
  )
  .add(
    HttpApiEndpoint.post("release", "/compaction-budget/releases", {
      payload: ReleaseRequest,
      success: HttpApiSchema.NoContent,
    }),
  ) {}

/** The compaction budget's HTTP API, versioned under `/v1`. */
export class CompactionBudgetHttpApi extends HttpApi.make("CompactionBudgetHttpApi")
  .add(CompactionBudgetHttpApiGroup)
  .prefix("/v1") {}
