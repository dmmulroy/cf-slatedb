import { defineConfig } from "vite-plus";

/** Vite+ configuration for `@shaledb/lsm-core`; `vp run -r test` (and so `vp run ready`) runs its tests. */
const lsmCoreViteConfig = defineConfig({
  test: {
    passWithNoTests: true,
  },
});

export default lsmCoreViteConfig;
