# Progress

## Current phase

Phase 2: lsm-core (starting). Phase 3 (object storage and manifests) will overlap.

## Done

- **Phase 0 — repository setup** (`b3a4e84`). pnpm workspace with iac's catalog (effect 4.1.4,
  alchemy and `@alchemy.run/cloudflare` 2.0.0-beta.87, vite-plus 1.2.0, typescript ^7), iac's lint
  rules and tsconfig, `.node-version`, `.gitignore`, workspaces `packages/lsm-core`,
  `packages/lsm-cloudflare`, `packages/lsm-sharding`, `infra/cf-slatedb`,
  `tools/{trace-cli,scenario-cli,formal-verification,traits,workspace-boundaries}`. Boundary test
  adapted (pure core, sharding public-API-only, no `exports` under `infra/`). Docs adapted;
  `docs/testing.md` rewritten local-first; `AGENTS.md` written; iac traits copied; SlateDB cloned
  read-only into `repos/slatedb`.
- **Phase 1 — trace tooling.**
  - `tools/trace-cli` (`effect/cli`): `serve` (OTLP/HTTP receiver on `127.0.0.1:4318`, JSON and
    protobuf, logs/metrics accepted and discarded), `list`, `show`, `tail`, `grep`, `stats`, `diff`
    (trace ids or `run:<startMs>-<endMs>`), `assert` (file or stdin; `SpanExists` with parent,
    `NoSpan`, `SpanCount`, `NoErrorStatus`, `SameTrace`, `Attribute`), `clear`. `--json` on every
    command. Store: `.traces/spans/<traceId>.ndjson`. Typed errors (`TraceStoreError`,
    `OtlpTraceDecodeError`, `TraceCliError`). Tests export real spans through Effect's
    `OtlpTracer` in both serializations.
  - Review fix: `list` is one pass (summarize, filter, bounded binary-insert top-`limit`) and
    `summarizeTrace` is one pass over spans plus a pass over root candidates only.
  - `lsmTelemetryLayer` (`@cf-slatedb/lsm-cloudflare`) wraps `Telemetry.layerOtlp`.
  - OTLP flush bug **confirmed on beta.87** and patched (`patches/alchemy@2.0.0-beta.87.patch`,
    design decision D5): `telemetry-smoke.workerd.ts` is red without the patch (0/2 exports) and
    green with it; DO alarm spans arrive.
  - `infra/cf-slatedb/alchemy.run.ts` + `vp run dev` run a telemetry probe Worker + DO under
    `alchemy dev`; verified with the trace CLI (Worker → DO span nesting, alarm span,
    `assert` pass/fail exit codes).
  - `@cf-slatedb/lsm-cloudflare` `test` now runs unit tests and the workerd suite (`--mode workerd`).
- `vp run ready` passes.

## Next

- Phase 2: `lsm-core` — ids, `Row`/`RowValue`, key order, CRC32C, Bloom filter, varint, block
  codec, SST builder/reader, merge iterator, `Manifest` v1 schema and edits, size-tiered
  scheduler, property tests against a `Map` oracle.
- Phase 3: `ObjectStore`, `ManifestStore`, `ManifestCache`, epoch fencing.

## Findings

- alchemy 2.0.0-beta.87 still flushes event telemetry inside `waitUntil` after the event settles;
  patched (D5).
- Effect 4.1.4 names HTTP spans by method (`GET`); the trace CLI shows span kind (D7).
- No Java or mise on this machine; TLC needs a JDK. Plan: download a pinned JDK and `tla2tools.jar`
  into a gitignored tool cache from the formal verification task.
- The traits MCP checker is configured (`.pi/mcp.json`) and `TYPESAFE_API_KEY` is set; it runs as an
  MCP server in the next pi session. Until then, review against `.agents/traits/*.md` by hand.

## Open questions

None yet.
