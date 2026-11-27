import { defineConfig } from "vite-plus";

/**
 * Vite+ configuration for the documentation site Stack.
 *
 * **Details**
 *
 * `build` copies the architecture page (`docs/show-me-system-architecture.html`) into
 * `dist/index.html`, the directory the assets-only Worker serves. `deploy` builds, then deploys the
 * `docs` stage, which serves the page at `https://shaledb.<account-subdomain>.workers.dev`. Dillon
 * approved this deploy; it touches no shaledb stage.
 */
const docsSiteViteConfig = defineConfig({
  run: {
    tasks: {
      build: {
        cache: false,
        command: "mkdir -p dist && cp ../../docs/show-me-system-architecture.html dist/index.html",
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
