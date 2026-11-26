import { join } from "node:path";

import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  BlockServerWorker,
  CompactorWorker,
  DbApiWorker,
  GcWorker,
  isLocalLsmStage,
  LSM_WORKER_COMPATIBILITY_DATE,
} from "@shaledb/lsm-cloudflare";
import * as Alchemy from "alchemy";
import { ShardRouterWorker } from "@shaledb/sharding";
import { Effect, Layer, Option } from "effect";
import { ConfigError } from "effect/Config";
import * as ConfigProvider from "effect/ConfigProvider";

import { shaledbBlockServerWorkerLayer } from "./src/block-server-worker-entry.ts";
import { shaledbCompactorWorkerLayer } from "./src/compactor-worker-entry.ts";
import { shaledbApiWorkerLayer } from "./src/db-api-worker-entry.ts";
import { shaledbGcWorkerLayer } from "./src/gc-worker-entry.ts";
import { shaledbShardRouterWorkerLayer } from "./src/shard-router-worker-entry.ts";
import { shaledbStateLayer } from "./src/shaledb-stage.ts";
import { shaledbStackConfig } from "./src/shaledb-stack-config.ts";
import {
  deploysTelemetryProbe,
  TelemetryProbeWorker,
  telemetryProbeWorkerLayer,
} from "./src/telemetry-probe-worker.ts";
import { CPU_TAIL_WORKER_ID } from "./src/shaledb-telemetry.ts";
import {
  TraceBucket,
  TraceCollectorWorker,
  traceCollectorWorkerLayer,
} from "./src/trace-collector-worker.ts";

const shaledbWorkersLayer = Layer.mergeAll(
  shaledbApiWorkerLayer,
  shaledbBlockServerWorkerLayer,
  shaledbCompactorWorkerLayer,
  shaledbGcWorkerLayer,
  shaledbShardRouterWorkerLayer,
);

/**
 * The Stack's configuration (`ShaledbStackConfig`), resolved from the deploy's environment before
 * anything is declared: a refused variable stops the plan as a `ConfigError` naming it.
 */
const resolvedShaledbStackConfig = shaledbStackConfig.pipe(
  Effect.mapError(
    (error) => new ConfigError(new ConfigProvider.SourceError({ message: error.message })),
  ),
);

/**
 * Every Worker of the Stack, plus the trace collector on live stages only, and the telemetry probe
 * (a test tool with fault injection) on local and preview stages only: declaring a Worker's Layer
 * deploys it, so a stage leaves out what it must not run, and `shaledbTelemetry` finds the
 * collector only where it exists. The configuration is resolved first, so no Worker is declared
 * under an invalid one.
 */
const shaledbStackLayer = Layer.unwrap(
  Effect.gen(function* () {
    const stage = yield* Alchemy.Stage;
    yield* resolvedShaledbStackConfig;
    const withProbe = deploysTelemetryProbe(stage)
      ? Layer.mergeAll(shaledbWorkersLayer, telemetryProbeWorkerLayer)
      : shaledbWorkersLayer;
    return isLocalLsmStage(stage)
      ? withProbe
      : Layer.mergeAll(withProbe, traceCollectorWorkerLayer);
  }),
);

/**
 * Stack that runs shaledb.
 *
 * **Details**
 *
 * Local first: `vp run dev` runs it under Alchemy's local providers (stage `dev`: local R2, Durable
 * Objects, and Workflows) with local state. Live stages (the approved previews `preview-1`,
 * `preview-2`, `preview-3`) keep their state in the account's Cloudflare state store, so any machine
 * can redeploy them (`shaledbStateLayer`, decision D43). The deploy's environment is resolved once
 * into a `ShaledbStackConfig` (D55), and every Worker is bound its complete engine settings and
 * jurisdiction on every deploy. `SHALEDB_JURISDICTION` (`eu`, `fedramp`, or `none`) restricts the
 * whole Stack to one jurisdiction (D49); it is fixed per installation, because changing it replaces
 * the buckets and the Workflow and addresses new Durable Objects.
 */
const ShaledbStack = Alchemy.Stack(
  "Shaledb",
  { providers: Cloudflare.providers(), state: shaledbStateLayer },
  Effect.gen(function* () {
    const blocks = yield* BlockServerWorker;
    yield* CompactorWorker;
    const gc = yield* GcWorker;
    const api = yield* DbApiWorker;
    const shards = yield* ShardRouterWorker;
    // Live stages export spans to their own collector (`shaledbTelemetry`); local stages export
    // to the trace CLI's receiver and have none.
    const stage = yield* Alchemy.Stage;
    const config = yield* resolvedShaledbStackConfig;
    const probe = deploysTelemetryProbe(stage)
      ? Option.some(yield* Cloudflare.Worker.ref(TelemetryProbeWorker))
      : Option.none();
    const traces = isLocalLsmStage(stage)
      ? Option.none()
      : Option.some(yield* Cloudflare.Worker.ref(TraceCollectorWorker));
    // Live stages also record API, compactor, and GC invocations' outcome and CPU time (D48),
    // sampled like traces except for the ones that did not end `ok` (D71).
    if (!isLocalLsmStage(stage)) {
      yield* Cloudflare.Worker(CPU_TAIL_WORKER_ID, {
        main: join(import.meta.dirname, "src", "cpu-tail-worker.ts"),
        workersDev: false,
        compatibility: { date: LSM_WORKER_COMPATIBILITY_DATE },
        env: {
          TRACE_BUCKET: yield* TraceBucket,
          TAIL_SAMPLE_RATE: String(config.telemetry.traceSampleRate),
        },
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
      /** URL of the telemetry probe Worker (local and preview stages only). */
      ...(Option.isSome(probe) ? { probeUrl: probe.value.url } : {}),
      /** OTLP receiver of a live stage's spans, read by `trace-cli pull --stage <stage>`. */
      ...(Option.isSome(traces) ? { traceCollectorUrl: traces.value.url } : {}),
    };
  }).pipe(Effect.provide(shaledbStackLayer)),
);

export default ShaledbStack;
