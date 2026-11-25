import * as Cloudflare from "@alchemy.run/cloudflare";
import { CompactionBudgetLedger } from "@cf-slatedb/lsm-core";
import type { HttpEffect } from "alchemy/Http";
import { makeExecutionMemo } from "alchemy/Runtime/ExecutionMemo";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/http";
import { HttpApiBuilder, HttpApiClient } from "effect/http-api";

import {
  CompactionBudget,
  CompactionBudgetUnavailableError,
  makeLedgerCompactionBudget,
} from "./compaction-budget.ts";
import { CompactionBudgetHttpApi } from "./compaction-budget-http-api.ts";
import { durableObjectCell } from "./durable-object-cell.ts";
import { LsmConfiguration } from "./lsm-configuration.ts";
import { lsmHttpServerLayer } from "./lsm-http-server.ts";
import { inLsmJurisdiction, lsmJurisdiction } from "./lsm-jurisdiction.ts";
import { eventTelemetryFlushForStage } from "./lsm-telemetry.ts";

/** Handlers of the budget HTTP API, delegating to the contextual (ledger) `CompactionBudget`. */
export const compactionBudgetHttpHandlersLayer = HttpApiBuilder.group(
  CompactionBudgetHttpApi,
  "budget",
  (handlers) =>
    Effect.gen(function* () {
      const budget = yield* CompactionBudget;
      return handlers
        .handle("acquire", ({ payload }) =>
          budget.acquire(payload.group, payload.holder).pipe(
            Effect.map((granted) => ({ granted })),
            // The ledger lives in this object's storage: it cannot be unavailable here.
            Effect.orDie,
          ),
        )
        .handle("release", ({ payload }) => budget.release(payload.holder).pipe(Effect.orDie));
    }),
);

/** Every budget HTTP route, needing only a `CompactionBudget`. */
export const compactionBudgetHttpRouterLayer = HttpApiBuilder.layer(CompactionBudgetHttpApi).pipe(
  Layer.provide(compactionBudgetHttpHandlersLayer),
  Layer.provide(lsmHttpServerLayer),
);

/**
 * The shared compaction budget: one Durable Object instance (`global`) holding the lease ledger in
 * its storage. Compactors reach it through `compactionBudgetClientLayer`.
 */
export class CompactionBudgetServer extends Cloudflare.DurableObject<
  CompactionBudgetServer,
  { readonly fetch: HttpEffect<RuntimeContext> }
>()("CompactionBudgetServer") {}

/** Name of the single budget instance. */
export const COMPACTION_BUDGET_INSTANCE = "global";

/** Hosts `CompactionBudgetServer`, leaving the settings visible. */
export const compactionBudgetServerLayerWithoutDependencies = CompactionBudgetServer.make(
  Effect.gen(function* () {
    const settings = yield* LsmConfiguration;
    const withEventTelemetryFlush = yield* eventTelemetryFlushForStage;
    const state = yield* Cloudflare.DurableObjectState;
    return Effect.gen(function* () {
      const budget = yield* makeLedgerCompactionBudget(
        durableObjectCell(state.raw.storage, "ledger", CompactionBudgetLedger),
      ).pipe(Effect.provideService(LsmConfiguration, settings));
      const fetch = yield* HttpRouter.toHttpEffect(
        compactionBudgetHttpRouterLayer.pipe(
          Layer.provide(Layer.succeed(CompactionBudget, budget)),
        ),
      );
      return { fetch: withEventTelemetryFlush(fetch) };
    });
  }),
);

/**
 * Provides the budget as a client of the `CompactionBudgetServer` namespace (bound by the Worker
 * being built), with one HTTP client per invocation.
 */
export const compactionBudgetClientLayer = Layer.effect(
  CompactionBudget,
  Effect.gen(function* () {
    const namespace = yield* CompactionBudgetServer;
    const jurisdiction = yield* lsmJurisdiction;
    const client = yield* makeExecutionMemo(
      Effect.suspend(() =>
        HttpApiClient.makeWith(CompactionBudgetHttpApi, {
          baseUrl: "http://compaction-budget.internal",
          httpClient: Cloudflare.toHttpClient(
            inLsmJurisdiction(namespace, jurisdiction).getByName(COMPACTION_BUDGET_INSTANCE),
          ),
        }),
      ),
    );
    const unavailable = (error: { readonly message: string }) =>
      Effect.fail(
        new CompactionBudgetUnavailableError({
          message: `The compaction budget could not be reached: ${error.message} The compactor asks again on its next poll.`,
        }),
      );
    return CompactionBudget.of({
      acquire: Effect.fn("CompactionBudget.acquire")(function* (group: string, holder: string) {
        const response = yield* (yield* client).budget
          .acquire({ payload: { group, holder } })
          .pipe(Effect.catchTags({ HttpClientError: unavailable, SchemaError: unavailable }));
        return response.granted;
      }),
      release: Effect.fn("CompactionBudget.release")(function* (holder: string) {
        yield* (yield* client).budget
          .release({ payload: { holder } })
          .pipe(Effect.catchTags({ HttpClientError: unavailable, SchemaError: unavailable }));
      }),
    });
  }),
);
