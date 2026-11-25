import { blockServerWorkerLayer } from "../../src/block-server-worker.ts";
import { INTEGRATION_OTLP_BASE_URL, INTEGRATION_PORTS } from "./integration-ports.ts";

/** The integration tests' `BlockServer` Worker; this module is its bundle entry. */
const testBlockServerLayer = blockServerWorkerLayer({
  main: import.meta.url,
  otlpBaseUrl: INTEGRATION_OTLP_BASE_URL,
  devPort: INTEGRATION_PORTS.blockServer,
});

export default testBlockServerLayer;
