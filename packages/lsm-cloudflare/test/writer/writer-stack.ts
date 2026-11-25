import { Effect } from "effect";

import { DbApiWorker, dbApiWorkerLayer } from "../../src/db-api-worker.ts";

/** Local port of the writer integration test's API Worker. */
export const WRITER_TEST_API_PORT = 8793;

/** The API Worker of the writer integration test; this module is its bundle entry. */
const writerTestApiLayer = dbApiWorkerLayer({
  main: import.meta.url,
  otlpBaseUrl: "http://127.0.0.1:4318",
  devPort: WRITER_TEST_API_PORT,
});

/** Stack body for the writer integration test: the API Worker, which hosts `WriterServer`. */
export const writerTestStack = Effect.gen(function* () {
  const api = yield* DbApiWorker;
  return { url: api.url };
}).pipe(Effect.provide(writerTestApiLayer));

export default writerTestApiLayer;
