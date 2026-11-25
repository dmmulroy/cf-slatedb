import { compactorWorkerLayer } from "../../src/compactor-worker.ts";
import { INTEGRATION_OTLP_BASE_URL, INTEGRATION_PORTS } from "./integration-ports.ts";

/** The integration tests' compactor Worker; this module is its bundle entry. */
const testCompactorLayer = compactorWorkerLayer({
  main: import.meta.url,
  otlpBaseUrl: INTEGRATION_OTLP_BASE_URL,
  devPort: INTEGRATION_PORTS.compactor,
});

export default testCompactorLayer;
