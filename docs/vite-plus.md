# Vite+ monorepo workflow

Vite+ owns repository checks and workspace task execution. The root `vite.config.ts` contains shared formatting, linting, type-checking, and Vite Task defaults; workspace `vite.config.ts` files own package-specific task graphs.

## Workspace boundaries

- `packages/*` contains libraries (`@cf-slatedb/lsm-core`, `@cf-slatedb/lsm-cloudflare`,
  `@cf-slatedb/sharding`). Each exposes `src/index.ts` through `exports`.
- `infra/*` contains independent composition roots, one Alchemy Stack each. They declare no
  `exports` and never depend on each other.
- `tools/*` contains repository tooling that nothing imports: `tools/trace-cli`,
  `tools/scenario-cli`, `tools/formal-verification`, `tools/traits` (the Traits configuration the
  root `traits.ts` re-exports), and `tools/workspace-boundaries` (the test that enforces these
  rules).
- Declare every import edge in the importing workspace's `package.json`.
- Stack order is not an import edge: put it in `dependsOn`, not in `package.json`.

## Vite Task policy

Package scripts are uncached by default. Configured tasks are cached by default, but every task that deploys, destroys, plans, synchronizes external state, or starts a long-running process must set `cache: false`.

Alchemy infrastructure lifecycle commands belong in workspace Vite tasks rather than duplicate package scripts. Root aliases invoke those tasks through exact `package#task` selectors so every entrypoint uses the same dependency graph.

Cross-workspace infrastructure prerequisites belong in `dependsOn`. An exact `package#task`
selector resolves without a `package.json` dependency on that package.

Local first: `vp run dev` runs the Stack under Alchemy's local providers (stage `dev`, local R2,
Durable Objects, and Workflows). Every task that plans against or changes a live Cloudflare stage
needs Dillon's approval before it runs, and none of them is reachable from `vp run ready`.
