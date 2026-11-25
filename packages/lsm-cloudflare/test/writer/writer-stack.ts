import { Effect } from "effect";

import { BlockServerWorker } from "../../src/block-server-worker.ts";
import { DbApiWorker, dbApiWorkerLayer } from "../../src/db-api-worker.ts";
import testBlockServerLayer from "./block-server-entry.ts";

/** Local port of the writer integration test's API Worker. */
export const WRITER_TEST_API_PORT = 8793;

/** The API Worker of the writer integration test; this module is its bundle entry. */
const writerTestApiLayer = dbApiWorkerLayer({
  main: import.meta.url,
  otlpBaseUrl: "http://127.0.0.1:4318",
  devPort: WRITER_TEST_API_PORT,
});

/** Stack body for the integration tests: the API Worker (hosting `WriterServer`) and the `BlockServer`. */
export const writerTestStack = Effect.gen(function* () {
  yield* BlockServerWorker;
  const api = yield* DbApiWorker;
  return { url: api.url };
}).pipe(Effect.provide([writerTestApiLayer, testBlockServerLayer]));

export default writerTestApiLayer;
