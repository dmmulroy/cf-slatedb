# Agent Instructions

## Project

cf-slatedb is a SlateDB-style LSM key-value store on Cloudflare R2 and Durable Objects. It keeps
SlateDB's model (one writer, many readers, immutable SSTs, a CAS-versioned manifest, size-tiered
compaction) and swaps two pieces for Cloudflare-native ones: the writer is a Durable Object whose
SQLite table is both the WAL and the memtable, and compaction runs in a Workflow. The design is
[`docs/show-me-r2-lsm-design.html`](docs/show-me-r2-lsm-design.html); the build plan is
[`docs/implementation-brief.md`](docs/implementation-brief.md).

The unit is a **database**: one opaque `DatabaseId` with its own `WriterServer`,
`CompactorServer`, manifest chain, R2 prefix `db/<id>/`, and cache tag `db:<id>`. The core has no
tenant concept.

- `packages/lsm-core` (`@cf-slatedb/lsm-core`) — pure: ids, rows, CRC32C, Bloom filter, varint,
  block and SST codecs, the k-way merge iterator, the manifest schema and its edits, and the
  size-tiered compaction scheduler. No Cloudflare imports.
- `packages/lsm-cloudflare` (`@cf-slatedb/lsm-cloudflare`) — the engine: `ObjectStore` (R2),
  `ManifestStore`, `ManifestCache`, `SstStore`, `BlockCache`, `BlockSource`, `BlockServer`,
  `MemTable`, `WriterServer`, `DbReader`, `DbClient`, `CompactorServer`, the compaction Workflow,
  `GarbageCollector`, and `DatabaseLifecycle`.
- `packages/lsm-sharding` (`@cf-slatedb/sharding`) — optional range sharding (`ShardDirectory`,
  `ShardRouter`, `ShardSplit`) built only on the public APIs.
- `infra/cf-slatedb` (`@cf-slatedb/infra`) — the Alchemy Stack: R2 bucket, Durable Objects, the
  `BlockServer` Worker, the API Worker, the compaction Workflow, and the GC cron Worker.
- `tools/*` — the trace CLI, the scenario CLI, the preview CLI, formal verification, Traits, and
  the workspace boundary check.

Progress, open questions, and the hand-off live in [`docs/progress.md`](docs/progress.md). Every
judgment call the design did not make is in [`docs/design-decisions.md`](docs/design-decisions.md).
Every design section maps to code and tests in [`docs/coverage-matrix.md`](docs/coverage-matrix.md).

## Stack

Cloudflare Workers, Durable Objects (SQLite), R2, and Workflows; Effect 4 (`effect` 4.1.4);
Alchemy v2 (`alchemy` and `@alchemy.run/cloudflare` 2.0.0-beta.87); Vite+ (`vp`) for the pnpm
monorepo; TypeScript 7. Effect 4 and Alchemy v2 drift from training data: before writing code
against either, read the installed source under `node_modules/effect/src`,
`node_modules/alchemy/src`, and `node_modules/@alchemy.run/cloudflare/src`.

SlateDB is the reference for LSM semantics. Clone it read-only into `repos/slatedb` (gitignored)
with `git clone --depth 1 https://github.com/slatedb/slatedb.git repos/slatedb` and read its design
docs, RFCs, Rust source, and FizzBee specs when you implement the matching part. Never import from
`repos/`.

## Infrastructure rules

- **Local first.** Develop and test against `vp run dev` (Alchemy local providers, stage `dev`:
  local R2, Durable Objects, and Workflows). Workers Cache is not emulated locally.
- **Live deploys are approved for the reusable preview stages `preview-1`, `preview-2`, and
  `preview-3` only**, through `tools/preview-cli` (`node tools/preview-cli/src/preview-cli.ts
  deploy <stage>`), which runs the `deploy:preview-<n>` tasks. Redeploy a stage in place instead of
  creating new ones. Never deploy `production` and never run `alchemy destroy` (the
  `destroy:preview-<n>` tasks are for Dillon; `preview-cli destroy-command <stage>` prints them).
  Any other live operation needs Dillon's approval: write the request under "Open questions" in
  `docs/progress.md` and keep working on anything not blocked.
- A Stack runs in one data-residency jurisdiction (`--jurisdiction eu|fedramp|none` on
  `preview-cli deploy`, D49); it is fixed per installation, so do not change it on a stage holding
  data you need.
- Live stages keep Alchemy state in the account's Cloudflare state store (D43), and their public
  Workers require the stage's access token (`Authorization: Bearer $(preview-cli token <stage>)`,
  D44).
- Run Alchemy through `vp run` tasks, never the Alchemy CLI directly. Shell commands containing
  Alchemy lifecycle verbs may be blocked by a local deployment guard.
- Production names and retention follow the stage rules in `docs/coding-standards.md`
  ("Environment-Isolated Infrastructure").

## Project standards

Read [`docs/coding-standards.md`](docs/coding-standards.md) before writing or reviewing code, and
review finished code against it before declaring work complete. Its `Account`/`Order`/`Invoice`
examples are illustrative, not this project's domain.

Read [`docs/errors.md`](docs/errors.md) before adding or changing typed errors, error messages, or
retry guidance.

Read [`docs/testing.md`](docs/testing.md) before adding behavior or tests. The default strategy is
local: unit and property tests, workerd integration through Alchemy's local providers, and scenario
tests with trace assertions. Real-cloud tests are opt-in and need approval.

Read [`docs/vite-plus.md`](docs/vite-plus.md) before adding an entrypoint, a library, a workspace
dependency, or a Vite task.

## Trace CLI

`tools/trace-cli` is the primary tool for testing, debugging, and forming hypotheses about the
running system. Every Worker and Durable Object exports OTLP spans to its receiver.

```sh
node tools/trace-cli/src/trace-cli.ts serve          # OTLP/HTTP receiver on 127.0.0.1:4318
node tools/trace-cli/src/trace-cli.ts list --json    # recent traces
node tools/trace-cli/src/trace-cli.ts show <traceId> # indented span tree
node tools/trace-cli/src/trace-cli.ts tail           # follow live spans
node tools/trace-cli/src/trace-cli.ts grep --name WriterServer.write --db <id>
node tools/trace-cli/src/trace-cli.ts stats          # latency percentiles per span name
node tools/trace-cli/src/trace-cli.ts diff <a> <b>   # compare two traces
node tools/trace-cli/src/trace-cli.ts assert <file.json>  # declarative trace checks
node tools/trace-cli/src/trace-cli.ts pull --stage preview-1  # download a preview's spans
node tools/trace-cli/src/trace-cli.ts tail --stage preview-1  # follow a preview's spans live
```

On live preview stages every Worker and Durable Object exports to the stage's `TraceCollector`
Worker instead (D46); `pull` and `tail --stage` bring those spans into the local store, where every
other command works on them unchanged.

Spans carry only safe identifying attributes (`database.id`, `sst.id`, `manifest.id`, `seq`, sizes,
cache hit or miss). Never put keys, values, or secrets in span attributes or logs.

## Formal verification

Protocol properties (no acknowledged write lost, fencing, manifest CAS linearizability, GC safety,
seal and split, transactions) are TLA+ models under `tools/formal-verification`, each with a
project-local skill in `.agents/skills/verify-*`. `vp run verify` runs every check. Read a TLA+
result with the pstack `/statechart` skill (`.agents/skills/statechart/SKILL.md`). After a change to
covered code, follow the matching `verify-*` skill; for drift that skipped it, use
`.agents/skills/maintain-formal-verification-skill/SKILL.md`. The pstack skills are disabled for
automatic invocation, so open them by path.

Code-level properties (codecs, the merge iterator, block seek) are property tests on the real code.

## Traits

Project coding traits live in `.agents/traits/` (copied from iac) and are review guidance. The
`traits` MCP server (`.pi/mcp.json`; config in `tools/traits`, re-exported by the root `traits.ts`)
checks changed files when `TYPESAFE_API_KEY` is set and Claude Code is logged in. Otherwise review
the changed files against the trait files by hand.

## Validation

`vp run ready` runs format, lint, type checks, and every workspace's tests. It is the gate for every
commit. Report a command as passing only when its exit code was observed to be `0`. Commit early and
often with conventional commit messages, never rewrite history, and update `docs/progress.md` at
every commit.
