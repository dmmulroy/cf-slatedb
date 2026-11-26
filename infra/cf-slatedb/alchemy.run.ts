import { join } from "node:path";

import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  BlockServerWorker,
  CompactorWorker,
  DbApiWorker,
  GcWorker,
  isLocalLsmStage,
  LSM_WORKER_COMPATIBILITY_DATE,
} from "@cf-slatedb/lsm-cloudflare";
import * as Alchemy from "alchemy";
import { ShardRouterWorker } from "@cf-slatedb/sharding";
import { Effect, Layer, Option } from "effect";
import { ConfigError } from "effect/Config";
import * as ConfigProvider from "effect/ConfigProvider";

import { cfSlatedbBlockServerWorkerLayer } from "./src/block-server-worker-entry.ts";
import { cfSlatedbCompactorWorkerLayer } from "./src/compactor-worker-entry.ts";
import { cfSlatedbApiWorkerLayer } from "./src/db-api-worker-entry.ts";
import { cfSlatedbGcWorkerLayer } from "./src/gc-worker-entry.ts";
import { cfSlatedbShardRouterWorkerLayer } from "./src/shard-router-worker-entry.ts";
import { cfSlatedbStateLayer } from "./src/cf-slatedb-stage.ts";
import { cfSlatedbStackConfig } from "./src/cf-slatedb-stack-config.ts";
import { TelemetryProbeWorker, telemetryProbeWorkerLayer } from "./src/telemetry-probe-worker.ts";
import { CPU_TAIL_WORKER_ID } from "./src/cf-slatedb-telemetry.ts";
import {
  TraceBucket,
  TraceCollectorWorker,
  traceCollectorWorkerLayer,
} from "./src/trace-collector-worker.ts";

const cfSlatedbWorkersLayer = Layer.mergeAll(
  cfSlatedbApiWorkerLayer,
  cfSlatedbBlockServerWorkerLayer,
  cfSlatedbCompactorWorkerLayer,
  cfSlatedbGcWorkerLayer,
  cfSlatedbShardRouterWorkerLayer,
  telemetryProbeWorkerLayer,
);

/**
 * The Stack's configuration (`CfSlatedbStackConfig`), resolved from the deploy's environment before
 * anything is declared: a refused variable stops the plan as a `ConfigError` naming it.
 */
const resolvedCfSlatedbStackConfig = cfSlatedbStackConfig.pipe(
  Effect.mapError(
    (error) => new ConfigError(new ConfigProvider.SourceError({ message: error.message })),
  ),
);

/**
 * Every Worker of the Stack, plus the trace collector on live stages only: declaring the collector's
 * Layer deploys it, so local stages leave it out, and `cfSlatedbTelemetry` finds it only where it
 * exists. The configuration is resolved first, so no Worker is declared under an invalid one.
 */
const cfSlatedbStackLayer = Layer.unwrap(
  Effect.gen(function* () {
    const stage = yield* Alchemy.Stage;
    yield* resolvedCfSlatedbStackConfig;
    return isLocalLsmStage(stage)
      ? cfSlatedbWorkersLayer
      : Layer.mergeAll(cfSlatedbWorkersLayer, traceCollectorWorkerLayer);
  }),
);

/**
 * Stack that runs cf-slatedb.
 *
 * **Details**
 *
 * Local first: `vp run dev` runs it under Alchemy's local providers (stage `dev`: local R2, Durable
 * Objects, and Workflows) with local state. Live stages (the approved previews `preview-1`,
 * `preview-2`, `preview-3`) keep their state in the account's Cloudflare state store, so any machine
 * can redeploy them (`cfSlatedbStateLayer`, decision D43). The deploy's environment is resolved once
 * into a `CfSlatedbStackConfig` (D55), and every Worker is bound its complete engine settings and
 * jurisdiction on every deploy. `CF_SLATEDB_JURISDICTION` (`eu`, `fedramp`, or `none`) restricts the
 * whole Stack to one jurisdiction (D49); it is fixed per installation, because changing it replaces
 * the buckets and the Workflow and addresses new Durable Objects.
 */
const CfSlatedbStack = Alchemy.Stack(
  "CfSlatedb",
  { providers: Cloudflare.providers(), state: cfSlatedbStateLayer },
  Effect.gen(function* () {
    const blocks = yield* BlockServerWorker;
    yield* CompactorWorker;
    const gc = yield* GcWorker;
    const api = yield* DbApiWorker;
    const shards = yield* ShardRouterWorker;
    const probe = yield* TelemetryProbeWorker;
    // Live stages export spans to their own collector (`cfSlatedbTelemetry`); local stages export
    // to the trace CLI's receiver and have none.
    const stage = yield* Alchemy.Stage;
    const config = yield* resolvedCfSlatedbStackConfig;
    const traces = isLocalLsmStage(stage)
      ? Option.none()
      : Option.some(yield* Cloudflare.Worker.ref(TraceCollectorWorker));
    // Live stages also record every API and compactor invocation's CPU time (D48).
    if (!isLocalLsmStage(stage)) {
      yield* Cloudflare.Worker(CPU_TAIL_WORKER_ID, {
        main: join(import.meta.dirname, "src", "cpu-tail-worker.ts"),
        workersDev: false,
        compatibility: { date: LSM_WORKER_COMPATIBILITY_DATE },
        env: { TRACE_BUCKET: yield* TraceBucket },
      });
    }
    return {
      /** URL of the public database API (`/v1`). */
      apiUrl: api.url,
      /** Data-residency jurisdiction of every bucket, Durable Object, and the Workflow (D49). */
      jurisdiction: config.jurisdiction,
      /** The LSM bucket's R2 location hint, or `auto` (D56). */
      bucketLocation: config.bucketLocation,
      /** URL of the sharded API (the sharding package's example Worker). */
      shardsUrl: shards.url,
      /** URL of the GC operator API: the dev port locally, workers.dev on live stages. */
      gcUrl: gc.url,
      /**
       * Physical name of the `BlockServer` Worker. It has no URL: only the API Worker reaches it, over
       * a service binding, behind Workers Cache.
       */
      blockServerWorkerName: blocks.workerName,
      /** URL of the telemetry probe Worker. */
      probeUrl: probe.url,
      /** OTLP receiver of a live stage's spans, read by `trace-cli pull --stage <stage>`. */
      ...(Option.isSome(traces) ? { traceCollectorUrl: traces.value.url } : {}),
    };
  }).pipe(Effect.provide(cfSlatedbStackLayer)),
);

export default CfSlatedbStack;
