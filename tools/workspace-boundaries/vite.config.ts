import { defineConfig } from "vite-plus";

/**
 * Vite+ configuration for the workspace boundary check, whose test fails when a workspace under
 * `infra/` can be imported or depends on another one.
 *
 * **Details**
 *
 * `vp run -r test` (and so `vp run ready`) runs it.
 */
const toolsWorkspaceBoundariesViteConfig = defineConfig({});

export default toolsWorkspaceBoundariesViteConfig;
