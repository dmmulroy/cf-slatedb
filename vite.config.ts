import { defineConfig } from "vite-plus";

export default defineConfig({
  fmt: {
    // Trait files are written by Traits' own serializer; the design page is kept as authored;
    // `repos/` holds read-only reference clones; `.traces/` and `.tlc/` are local tool output.
    ignorePatterns: [
      ".agents/**",
      "**/dist/**",
      "repos/**",
      ".traces/**",
      "**/.tlc/**",
      "docs/show-me-r2-lsm-design.html",
    ],
  },
  lint: {
    jsPlugins: [{ name: "vite-plus", specifier: "vite-plus/oxlint-plugin" }],
    rules: {
      "typescript/consistent-type-assertions": ["error", { assertionStyle: "never" }],
      "typescript/no-explicit-any": "error",
      "typescript/no-non-null-assertion": "error",
      "typescript/no-unnecessary-type-assertion": "error",
      "typescript/no-unsafe-type-assertion": "error",
      "vite-plus/prefer-vite-plus-imports": "error",
    },
    options: { typeAware: true, typeCheck: true },
    ignorePatterns: [".agents/**", "**/dist/**", "repos/**", ".traces/**", "**/.tlc/**"],
  },
  run: {
    cache: {
      scripts: false,
      tasks: true,
    },
  },
});
