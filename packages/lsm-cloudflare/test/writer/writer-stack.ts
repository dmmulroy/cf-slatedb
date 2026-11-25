import { Effect } from "effect";

import { BlockServerWorker } from "../../src/block-server-worker.ts";
import { DbApiWorker, dbApiWorkerLayer } from "../../src/db-api-worker.ts";
import { CompactorWorker } from "../../src/compactor-worker.ts";
import { GcWorker } from "../../src/gc-worker.ts";
import testBlockServerLayer from "./block-server-entry.ts";
import testCompactorLayer from "./compactor-entry.ts";
import testGcLayer from "./gc-entry.ts";
import { lsmOtlpTelemetry, noLsmTailConsumers } from "../../src/lsm-telemetry.ts";
import { INTEGRATION_OTLP_BASE_URL, INTEGRATION_PORTS } from "./integration-ports.ts";

/** The API Worker of the writer integration test; this module is its bundle entry. */
const writerTestApiLayer = dbApiWorkerLayer({
  main: import.meta.url,
  telemetry: lsmOtlpTelemetry(INTEGRATION_OTLP_BASE_URL),
  tailConsumers: noLsmTailConsumers,
  devPort: INTEGRATION_PORTS.api,
});

/**
 * Stack body for the integration tests: the API Worker (hosting `WriterServer`), the `BlockServer`,
 * the compactor Worker, and the GC Worker.
 */
export const writerTestStack = Effect.gen(function* () {
  yield* BlockServerWorker;
  yield* CompactorWorker;
  yield* GcWorker;
  const api = yield* DbApiWorker;
  return { url: api.url, gcUrl: `http://127.0.0.1:${INTEGRATION_PORTS.gc}` };
}).pipe(
  Effect.provide([writerTestApiLayer, testBlockServerLayer, testCompactorLayer, testGcLayer]),
);

export default writerTestApiLayer;
