import { defineConfig } from "vite-plus";

/**
 * Vite+ configuration for the documentation site Stack.
 *
 * **Details**
 *
 * `build` copies `public/` (the OG image and favicon) and the architecture page
 * (`docs/show-me-system-architecture.html`, as `index.html`) into `dist/`, the directory the
 * assets-only Worker serves. `public/og.png` is rendered from `og-card.html`; rerender it with
 * `npx -y -p puppeteer@24 node render-og.mjs` after changing the card. `deploy` builds, then deploys the
 * `docs` stage, which serves the page at `https://shaledb.<account-subdomain>.workers.dev`. Dillon
 * approved this deploy; it touches no shaledb stage.
 */
const docsSiteViteConfig = defineConfig({
  run: {
    tasks: {
      build: {
        cache: false,
        command:
          "rm -rf dist && mkdir -p dist && cp -R public/. dist/ && cp ../../docs/show-me-system-architecture.html dist/index.html",
      },
      deploy: {
        cache: false,
        dependsOn: ["build"],
        command: "alchemy deploy --stage docs --yes",
      },
    },
  },
});

export default docsSiteViteConfig;
