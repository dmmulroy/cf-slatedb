import { defineConfig } from "vite-plus";

/**
 * Vite+ configuration for the cf-slatedb Stack.
 *
 * **Details**
 *
 * `dev` runs the Stack under Alchemy's local providers (stage `dev`: local R2, Durable Objects, and
 * Workflows) with live reload and short GC and compactor timings (`CF_SLATEDB_LSM_SETTINGS`).
 *
 * `deploy:preview-<n>` deploys the Stack to one of the three reusable live preview stages, in place
 * (approved; `docs/handoff.md`). Use them through `tools/preview-cli`, which sets the timings and
 * trace settings and reads the outputs back. Each preview's settings are the production defaults
 * unless `CF_SLATEDB_LSM_SETTINGS` is set when it deploys.
 *
 * `destroy:preview-<n>` tears a preview down. Agents never run them (the deployment guard blocks
 * `alchemy destroy`); `preview-cli destroy-command <stage>` prints the command for Dillon.
 *
 * There is no production task: deploying production needs Dillon's approval.
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
      "deploy:preview-1": { cache: false, command: "alchemy deploy --stage preview-1 --yes" },
      "deploy:preview-2": { cache: false, command: "alchemy deploy --stage preview-2 --yes" },
      "deploy:preview-3": { cache: false, command: "alchemy deploy --stage preview-3 --yes" },
      "destroy:preview-1": { cache: false, command: "alchemy destroy --stage preview-1 --yes" },
      "destroy:preview-2": { cache: false, command: "alchemy destroy --stage preview-2 --yes" },
      "destroy:preview-3": { cache: false, command: "alchemy destroy --stage preview-3 --yes" },
      // Read-only: prints the Cloudflare state store's records under a path (`CfSlatedb/<stage>`).
      "state:read": {
        cache: false,
        command: "alchemy state read --backend cloudflare",
      },
    },
  },
});

export default cfSlatedbViteConfig;
