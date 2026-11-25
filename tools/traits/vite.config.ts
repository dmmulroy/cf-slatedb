import { defineConfig } from "vite-plus";

/**
 * Traits tooling: the Traits configuration (re-exported by the root `traits.ts`, where Traits
 * looks for it) and the Claude subscription language model. `vp run -r test` runs its tests.
 */
const toolsTraitsViteConfig = defineConfig({});

export default toolsTraitsViteConfig;
