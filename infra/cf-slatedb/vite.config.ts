import { defineConfig } from "vite-plus";

/**
 * Vite+ configuration for the cf-slatedb Stack.
 *
 * **Details**
 *
 * `dev` runs the Stack under Alchemy's local providers (stage `dev`: local R2, Durable Objects, and
 * Workflows) with live reload and short GC and compactor timings (`CF_SLATEDB_LSM_SETTINGS`). There
 * is deliberately no live plan or deploy task: adding one needs Dillon's approval.
 */
const cfSlatedbViteConfig = defineConfig({
  test: {
    passWithNoTests: true,
  },
  run: {
    tasks: {
      dev: {
        cache: false,
        // Local dev only: GC may collect what is 10 s old and compactors poll every second, so the
        // scenario CLI sees compaction and collection within seconds.
        command:
          'CF_SLATEDB_LSM_SETTINGS=\'{"gcMinAge":10000,"compactorPollInterval":1000}\' alchemy dev --stage dev',
      },
    },
  },
});

export default cfSlatedbViteConfig;
