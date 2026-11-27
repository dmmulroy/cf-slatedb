import { defineConfig } from "vite-plus";

/**
 * Vite+ configuration for `@shaledb/sharding`.
 *
 * **Details**
 *
 * The default mode runs unit and property tests (`src/**\/*.test.ts`). Mode `workerd` runs the
 * shard router's local workerd test (`test/**\/*.workerd.ts`) through Alchemy's local providers;
 * the uncached `test:workerd` task runs it, and `test` runs both, so `vp run ready` covers it, as
 * in `@shaledb/lsm-cloudflare`.
 */
const lsmShardingViteConfig = defineConfig(({ mode }) => ({
  run: {
    tasks: {
      "test:workerd": {
        cache: false,
        command: "vp test run --mode workerd",
      },
    },
  },
  test:
    mode === "workerd"
      ? {
          include: ["test/**/*.workerd.ts"],
          testTimeout: 180_000,
          hookTimeout: 180_000,
          fileParallelism: false,
        }
      : {
          include: ["src/**/*.test.ts", "test/**/*.test.ts"],
          passWithNoTests: true,
        },
}));

export default lsmShardingViteConfig;
