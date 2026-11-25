import { defineConfig } from "vite-plus";

/** Vite+ configuration for `@cf-slatedb/tools-formal-verification`; `vp run -r test` (and so `vp run ready`) runs its tests. */
const formalVerificationViteConfig = defineConfig({
  test: {
    passWithNoTests: true,
  },
});

export default formalVerificationViteConfig;
