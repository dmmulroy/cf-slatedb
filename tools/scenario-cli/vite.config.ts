import { defineConfig } from "vite-plus";

/** Vite+ configuration for `@shaledb/tools-scenario-cli`; `vp run -r test` (and so `vp run ready`) runs its tests. */
const scenarioCliViteConfig = defineConfig({
  test: {
    passWithNoTests: true,
  },
});

export default scenarioCliViteConfig;
