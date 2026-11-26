import { defineConfig } from "vite-plus";

/** Vite+ configuration for `@shaledb/tools-trace-cli`; `vp run -r test` (and so `vp run ready`) runs its tests. */
const traceCliViteConfig = defineConfig({
  test: {
    passWithNoTests: true,
  },
});

export default traceCliViteConfig;
