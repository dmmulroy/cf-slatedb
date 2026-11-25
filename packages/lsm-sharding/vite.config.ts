import { defineConfig } from "vite-plus";

/** Vite+ configuration for `@cf-slatedb/sharding`; `vp run -r test` (and so `vp run ready`) runs its tests. */
const lsmShardingViteConfig = defineConfig({
  test: {
    passWithNoTests: true,
  },
});

export default lsmShardingViteConfig;
