import { defineConfig } from "vite-plus";

/**
 * Vite+ configuration for the shaledb Stack.
 *
 * **Details**
 *
 * `dev` runs the Stack under Alchemy's local providers (stage `dev`: local R2, Durable Objects, and
 * Workflows) with live reload and short GC and compactor timings (`SHALEDB_LSM_SETTINGS`).
 *
 * `deploy:preview-<n>` deploys the Stack to one of the three reusable live preview stages, in place
 * (approved; `docs/handoff-v1.md`). Use them through `tools/preview-cli`, which translates its flags
 * into the deploy's environment and reads the outputs back. The Stack resolves its configuration from
 * that environment (`ShaledbStackConfig`, D55) and binds every Worker the complete settings, so a
 * deploy without overrides runs the production defaults whatever an earlier deploy bound.
 *
 * `destroy:preview-<n>` tears a preview down. Agents never run them (the deployment guard blocks
 * `alchemy destroy`); `preview-cli destroy-command <stage>` prints the command for Dillon.
 *
 * `plan:production` and `deploy:production` plan and deploy the `production` stage: production
 * names, a retained LSM bucket, 10% trace sampling, no probe Worker. Nothing runs them until Dillon's
 * go-ahead (P6, `docs/release-v1.md`). The deploy first checks that both access-token secrets exist
 * (`tokens check production`); create them once with `tokens ensure production` and store the
 * tokens it prints.
 */
const shaledbViteConfig = defineConfig({
  test: {
    passWithNoTests: true,
  },
  run: {
    tasks: {
      dev: {
        cache: false,
        // Local dev only: GC may collect what is 10 s old and compactors poll every second, so the
        // scenario CLI sees compaction and collection within seconds; flushed rows stay 5 s for
        // transactions (it must be shorter than gcMinAge).
        command:
          'SHALEDB_LSM_SETTINGS=\'{"gcMinAge":10000,"compactorPollInterval":1000,"transactionRetentionMs":5000}\' alchemy dev --stage dev',
      },
      "deploy:preview-1": { cache: false, command: "alchemy deploy --stage preview-1 --yes" },
      "deploy:preview-2": { cache: false, command: "alchemy deploy --stage preview-2 --yes" },
      "deploy:preview-3": { cache: false, command: "alchemy deploy --stage preview-3 --yes" },
      "plan:production": { cache: false, command: "alchemy plan --stage production" },
      "deploy:production": {
        cache: false,
        command:
          "node src/access-tokens-cli.ts check production && alchemy deploy --stage production --yes",
      },
      "destroy:preview-1": { cache: false, command: "alchemy destroy --stage preview-1 --yes" },
      "destroy:preview-2": { cache: false, command: "alchemy destroy --stage preview-2 --yes" },
      "destroy:preview-3": { cache: false, command: "alchemy destroy --stage preview-3 --yes" },
      // A live stage's access tokens in the account's Secrets Store (D64): `ensure <stage>` or
      // `rotate <stage> <A|B>`. Prints new tokens once.
      tokens: { cache: false, command: "node src/access-tokens-cli.ts" },
      // Read-only: prints the Cloudflare state store's records under a path (`Shaledb/<stage>`).
      "state:read": {
        cache: false,
        command: "alchemy state read --backend cloudflare",
      },
    },
  },
});

export default shaledbViteConfig;
