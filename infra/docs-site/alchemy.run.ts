import { join } from "node:path";

import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import { Effect } from "effect";

/** The Cloudflare account that hosts shaledb (the same account as the shaledb Stack). */
const SHALEDB_CLOUDFLARE_ACCOUNT_ID = "99459fce09d9a9eec83befd957af4ec5";

/**
 * Stack that hosts the shaledb architecture page.
 *
 * **Details**
 *
 * One assets-only Worker serves `dist/` (built by the `build` task from
 * `docs/show-me-system-architecture.html`). No Worker code is uploaded; Cloudflare's asset layer
 * answers every request. The Worker is named `shaledb`, so its URL is
 * `https://shaledb.<account-subdomain>.workers.dev`. State lives in the account's Cloudflare state
 * store, like the shaledb Stack's live stages, so any machine can redeploy it.
 */
const DocsSiteStack = Alchemy.Stack(
  "ShaledbDocs",
  {
    providers: Cloudflare.providers(),
    state: Cloudflare.state({ accountId: SHALEDB_CLOUDFLARE_ACCOUNT_ID }),
  },
  Effect.gen(function* () {
    const site = yield* Cloudflare.Worker("ArchitecturePage", {
      name: "shaledb",
      assets: {
        directory: join(import.meta.dirname, "dist"),
        htmlHandling: "auto-trailing-slash",
        notFoundHandling: "404-page",
      },
    });
    return {
      /** URL of the architecture page. */
      url: site.url,
    };
  }),
);

export default DocsSiteStack;
