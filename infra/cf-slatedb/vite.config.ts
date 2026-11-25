import { defineConfig } from "vite-plus";

/**
 * Vite+ configuration for the cf-slatedb Stack.
 *
 * **Details**
 *
 * `dev` runs the Stack under Alchemy's local providers (stage `dev`: local R2, Durable Objects, and
 * Workflows) with live reload. There is deliberately no live plan or deploy task: adding one needs
 * Dillon's approval.
 */
const cfSlatedbViteConfig = defineConfig({
  test: {
    passWithNoTests: true,
  },
  run: {
    tasks: {
      dev: {
        cache: false,
        command: "alchemy dev --stage dev",
      },
    },
  },
});

export default cfSlatedbViteConfig;
