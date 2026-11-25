import { defineConfig } from "vite-plus";

/** Vite+ configuration for `@cf-slatedb/tools-preview-cli`; `vp run -r test` (and so `vp run ready`) runs its tests. */
const previewCliViteConfig = defineConfig({
  test: {
    passWithNoTests: true,
  },
});

export default previewCliViteConfig;
