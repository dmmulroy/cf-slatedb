import { blockServerWorkerLayer } from "../../src/block-server-worker.ts";

/** Local port of the integration tests' `BlockServer` Worker. */
export const TEST_BLOCK_SERVER_PORT = 8794;

/** The integration tests' `BlockServer` Worker; this module is its bundle entry. */
const testBlockServerLayer = blockServerWorkerLayer({
  main: import.meta.url,
  otlpBaseUrl: "http://127.0.0.1:4318",
  devPort: TEST_BLOCK_SERVER_PORT,
});

export default testBlockServerLayer;
