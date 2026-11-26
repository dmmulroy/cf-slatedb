import { defineConfig } from "vite-plus";

/** Vite+ configuration for `@shaledb/tools-writer-bench` (a benchmark; it has no tests). */
const writerBenchViteConfig = defineConfig({
  test: {
    passWithNoTests: true,
  },
});

export default writerBenchViteConfig;
