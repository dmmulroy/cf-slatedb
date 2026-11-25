import { defineConfig } from "vite-plus";

/** Vite+ configuration for `@cf-slatedb/lsm-cloudflare`; `vp run -r test` (and so `vp run ready`) runs its tests. */
const lsmCloudflareViteConfig = defineConfig({
  test: {
    passWithNoTests: true,
  },
});

export default lsmCloudflareViteConfig;
