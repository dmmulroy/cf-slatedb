import { defineConfig } from "vite-plus";

/**
 * Vite+ configuration for the formal checks. `verify` (uncached; root alias `vp run verify`) runs
 * every TLA+ check and every deliberately broken variant with TLC, each under a hard time limit.
 */
const formalVerificationViteConfig = defineConfig({
  test: {
    passWithNoTests: true,
  },
  run: {
    tasks: {
      verify: {
        cache: false,
        command: "node src/verify.ts",
      },
    },
  },
});

export default formalVerificationViteConfig;
