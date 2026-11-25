import { defineConfig } from "vite-plus";

/** Vite+ configuration for `@cf-slatedb/infra`; `vp run -r test` (and so `vp run ready`) runs its tests. */
const cfSlatedbViteConfig = defineConfig({
  test: {
    passWithNoTests: true,
  },
});

export default cfSlatedbViteConfig;
