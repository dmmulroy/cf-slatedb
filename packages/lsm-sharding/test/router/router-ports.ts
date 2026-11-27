import { DEFAULT_LSM_SETTINGS } from "@shaledb/lsm-core";
import type { LsmWorkerSettingsSource } from "@shaledb/lsm-cloudflare";
import { Effect, Option } from "effect";

/**
 * Local ports of the sharded API's workerd stack (D125): its own, so it can run beside the
 * writer stack's (`packages/lsm-cloudflare/test/writer/integration-ports.ts`, 8793–8796, 4320).
 */
export const ROUTER_TEST_PORTS = {
  api: 8803,
  blockServer: 8804,
  compactor: 8805,
  shardRouter: 8806,
  /** The test's own OTLP collector: test stages flush telemetry before each response. */
  collector: 4322,
} as const;

/** OTLP base URL of the test's collector. */
export const ROUTER_TEST_OTLP_BASE_URL = `http://127.0.0.1:${ROUTER_TEST_PORTS.collector}`;

/** Settings bound onto every Worker of the stack: the defaults (the page budget is 16 MiB). */
export const ROUTER_TEST_WORKER_SETTINGS: LsmWorkerSettingsSource = Effect.succeed({
  lsmSettings: DEFAULT_LSM_SETTINGS,
  jurisdiction: Option.none(),
  bucketLocation: Option.none(),
  traceSampleRate: 1,
});
