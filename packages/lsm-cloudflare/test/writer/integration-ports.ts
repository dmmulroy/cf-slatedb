/** Local ports of the integration test stack's Workers. */
export const INTEGRATION_PORTS = {
  api: 8793,
  blockServer: 8794,
  compactor: 8795,
  gc: 8796,
  /**
   * The test's own OTLP collector. Test stages flush telemetry before each response, so the Workers
   * need a collector that answers; the test starts one here (not the trace CLI's 4318).
   */
  collector: 4320,
} as const;

/** OTLP base URL of the integration test's collector. */
export const INTEGRATION_OTLP_BASE_URL = `http://127.0.0.1:${INTEGRATION_PORTS.collector}`;
