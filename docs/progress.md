# Progress

## Current phase

Phase 1: trace tooling (starting).

## Done

- **Phase 0 — repository setup.** pnpm workspace with iac's catalog (effect 4.1.4, alchemy and
  `@alchemy.run/cloudflare` 2.0.0-beta.87, vite-plus 1.2.0, typescript ^7), iac's lint rules and
  tsconfig, `.node-version`, `.gitignore` (`node_modules`, `.alchemy`, `repos/`, `.traces/`),
  workspaces `packages/lsm-core`, `packages/lsm-cloudflare`, `packages/lsm-sharding`,
  `infra/cf-slatedb`, `tools/{trace-cli,scenario-cli,formal-verification,traits,workspace-boundaries}`.
  `tools/workspace-boundaries` adapted (pure core, sharding public-API-only, no `exports` under
  `infra/`). Docs adapted; `docs/testing.md` rewritten local-first. `AGENTS.md` written. iac traits
  copied to `.agents/traits/`. SlateDB cloned read-only into `repos/slatedb`.
- `vp run ready` passes.

## Next

- Phase 1: `tools/trace-cli` (OTLP/HTTP receiver, store, list/show/tail/grep/stats/diff/assert),
  OTLP wiring through `Telemetry.layerOtlp`, a toy Worker + Durable Object under `alchemy dev`, and
  a red/green check of the event-settle flush bug on beta.87.

## Findings

- alchemy 2.0.0-beta.87 still closes the event scope (and so flushes OTLP) inside `waitUntil` after
  the event settles (`alchemy/src/Workers/WorkerBridge.ts` `processEvent`,
  `DurableObjectBridge.ts` `execute`), the same structure as the beta.72 repro. To be confirmed by a
  workerd test in Phase 1.
- No Java or mise on this machine; TLC needs a JDK. Plan: download a pinned JDK and
  `tla2tools.jar` into a gitignored tool cache from the formal verification task.

## Open questions

None yet.
