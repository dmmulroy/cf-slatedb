import { gcWorkerLayer } from "../../src/gc-worker.ts";
import { INTEGRATION_OTLP_BASE_URL, INTEGRATION_PORTS } from "./integration-ports.ts";

/** The integration tests' GC Worker (collected on demand over HTTP); this module is its bundle entry. */
const testGcLayer = gcWorkerLayer({
  main: import.meta.url,
  otlpBaseUrl: INTEGRATION_OTLP_BASE_URL,
  devPort: INTEGRATION_PORTS.gc,
  schedule: "0 0 1 1 *",
});

export default testGcLayer;
