# cf-slatedb hand-off

This document lets a new agent, on any machine, pick up cf-slatedb and finish it. Read it in full
before you change anything. The prompt to start the agent is at the end.

## What cf-slatedb is

A SlateDB-style LSM key-value store on Cloudflare: R2 for SSTs and manifests, a writer Durable Object
whose SQLite table is both the WAL and the memtable, reads in the caller's isolate behind an L1
in-memory block cache and L2 Workers Cache, compaction in a Workflow, and optional range sharding.
Written in Effect 4 (`effect` 4.1.4), deployed with Alchemy v2 (2.0.0-beta.87), built with Vite+
(`vp`) in a pnpm monorepo.

| Document                                                                             | What it is                                                                        |
| ------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------- |
| `docs/show-me-r2-lsm-design.html`                                                    | The design (12 sections plus an Effect 4.1.4 module audit). Open it in a browser. |
| `docs/implementation-brief.md`                                                       | The original build brief (Phases 0–10). Its settled decisions still hold.         |
| `docs/progress.md`                                                                   | Phase-by-phase log, the Phase 10 hand-off, findings, open questions.              |
| `docs/design-decisions.md`                                                           | D1–D42: every judgment call the design didn't make, with reasons.                 |
| `docs/coverage-matrix.md`                                                            | Every design item → implementing files → tests, trace assertions, formal checks.  |
| `AGENTS.md`                                                                          | Project rules for agents. Read it before writing code.                            |
| `docs/coding-standards.md`, `docs/errors.md`, `docs/testing.md`, `docs/vite-plus.md` | Standards. Follow them exactly.                                                   |

## State at hand-off

All ten phases of `docs/implementation-brief.md` are done and verified **locally**. Nothing has ever
been deployed to Cloudflare.

Verified on the original machine at the commit that added this file:

| Check                                                                                                | Result                                                                                                                                               |
| ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `vp run ready` (format, lint, types, all tests)                                                      | exit 0; 151 tests in 38 files, including a workerd integration test that runs the whole Stack locally                                                |
| `vp run verify` (TLA+ via TLC)                                                                       | 20/20 runs: six models pass on the real design, and every deliberately broken variant fails on its intended invariant (largest model: ~20.7M states) |
| `vp run dev` + `node tools/scenario-cli/src/scenario-cli.ts run all` with the trace receiver running | 9/9 scenarios pass with trace assertions: write-read, flush, compaction, gc, fork-split, fencing, changelog, transactions, remove                    |

Layout:

```text
packages/lsm-core/        pure: ids, rows, codecs, merge, manifest + edits, scheduler, GC plans, transactions
packages/lsm-cloudflare/  ObjectStore (R2), manifests, writer DO, reader, BlockServer, compaction, GC, lifecycle, DbClient, public /v1 API
packages/lsm-sharding/    optional range sharding over public APIs only
infra/cf-slatedb/         the Alchemy Stack (alchemy.run.ts); no exports
tools/trace-cli/          local OTLP receiver + list/show/tail/grep/stats/diff/assert
tools/scenario-cli/       end-to-end scenarios with trace assertions against a running Stack
tools/formal-verification/  TLC runner (`vp run verify`), specs in specs/
.agents/skills/verify-*   one project-local verify skill per formal check (bounds, covered code, sync commit)
.agents/skills/{create,maintain}-formal-verification-skill, statechart   pstack skills (open by path)
scripts/allowlist-cf-slatedb-deploys.sh   adds this Stack to the pi deployment guard (Dillon runs it)
```

## Set up on a new machine

1. Node ≥ 22.18 and the Vite+ CLI (`vp`). pnpm 12.14 is pinned through `devEngines` and downloads on
   demand. Then `pnpm install` at the repo root.
2. Clone SlateDB read-only as the reference implementation (gitignored):
   `git clone --depth 1 https://github.com/slatedb/slatedb.git repos/slatedb`.
3. Run `vp run ready`. It must pass before you change anything.
4. Formal verification: `vp run verify` downloads a pinned JDK 21 and `tla2tools.jar` into `.tlc/`
   **on macOS arm64 only**. On any other platform, install Java 11+ and set `JAVA` to its binary.
5. Local Stack: start `node tools/trace-cli/src/trace-cli.ts serve` (OTLP receiver on
   `127.0.0.1:4318`), then `vp run dev` (API `:1337`, BlockServer `:1338`, compactor `:1339`, GC
   `:1340`, shard router `:1341`), then `node tools/scenario-cli/src/scenario-cli.ts run all`. Stop
   every `workerd` process the dev server started when you finish; they can outlive `vp run dev`.
6. Traits: `.pi/mcp.json` configures the traits checker. It needs `TYPESAFE_API_KEY` in the
   environment and a Claude Code login. Without them, review against `.agents/traits/*.md` by hand.
7. Cloudflare credentials, needed only for preview deploys: Alchemy's Cloudflare provider reads
   them from the environment or its login (see `node_modules/@alchemy.run/cloudflare/src/Credentials.ts`).
   The account is Dillon's personal account `99459fce09d9a9eec83befd957af4ec5`. Never commit
   credentials; `.env*` and `.alchemy/` are gitignored.
8. If this machine runs pi with the Cloudflare deployment guard, Dillon runs
   `scripts/allowlist-cf-slatedb-deploys.sh` once. It allowlists stages `dev`, `preview-1`,
   `preview-2`, `preview-3` and makes the repository exempt from the guard, except that
   `alchemy destroy` is always blocked. Never edit the guard's policy file yourself.

## Rules that still apply

- Local first. **Live deploys are now approved, for the `preview-1`, `preview-2`, and `preview-3`
  stages only**, through `vp run` tasks. Never deploy `production`, never touch production resource
  names, and never run `alchemy destroy`: reuse the three preview stages by redeploying them, and
  give Dillon the destroy commands to run himself.
- The settled decisions in `docs/implementation-brief.md` stand: the core has no tenant concept; the
  WAL is the writer's SQLite table; there is no R2 WAL; reads run in the caller's isolate; L2 is
  Workers Cache in front of `BlockServer`; manifests use R2 create-if-absent with the `Headers` form.
  Change one only with a recorded reason in `docs/design-decisions.md`.
- Keep `docs/progress.md`, `docs/design-decisions.md` (continue from D43), and
  `docs/coverage-matrix.md` current at every commit. Commit early and often with conventional
  commits, and push to `origin main`.
- When you change code that a formal check covers, follow its `.agents/skills/verify-*` skill, and
  for drift follow `.agents/skills/maintain-formal-verification-skill/SKILL.md`. Move a sync commit
  only after confirming the model describes the code.

## Remaining work

Do these in order. Each ends with `vp run ready` and `vp run verify` passing, a commit, and a push.

### 1. Portable Alchemy state for previews

The Stack uses `localState()` (`infra/cf-slatedb/alchemy.run.ts`), so a preview deployed from one
machine can't be managed from another. Use `Cloudflare.state()` for preview stages, as
`../iac` does, and keep `localState()` for the local `dev` stage. Record it as a design decision.

### 2. Preview deployment tool

Build `tools/preview-cli` (Effect CLI, the repo's style, JSON output for scripts):

- `deploy <preview-1|preview-2|preview-3>`: deploys the Stack to that stage through a `vp run` task.
  The bucket and Workers get stage-generated names and are never retained.
- `status` and `url <stage>`: what is deployed where, and the API and `BlockServer` URLs.
- `destroy-command <stage>`: prints the command for Dillon to run. It never runs it.
- Reuse stages: redeploy in place rather than creating new ones.
- Previews must use short dev-style timings only when a flag asks for them. Their defaults are the
  production settings, so measurements are honest.

### 3. Traces from previews

Deployed Workers can't reach the trace CLI's `127.0.0.1:4318`. Add a `TraceCollector` Worker to
preview stages only. It receives OTLP/HTTP spans from every Worker and Durable Object in the stage,
requires a bearer secret generated per stage, and stores spans durably (R2 under a `traces/` prefix,
or a Durable Object). Extend the trace CLI with `pull --stage <stage>` (download into the local
store) and `tail --stage <stage>` (follow live), so `list`, `show`, `grep`, `stats`, `diff`, and
`assert` work on deployed traces unchanged. Remember decision D5: on live stages the telemetry flush
runs after the response (`waitUntil`), so check that spans from Durable Object events and alarms
arrive.

### 4. Live checks (now approved)

On a preview stage:

1. **Workers Cache:** read the same block twice through the API and confirm `Cf-Cache-Status: HIT`
   on the `BlockServer` response.
2. **R2 create-if-absent:** confirm `If-None-Match: *` passed as `Headers` returns the
   `AlreadyExists` outcome on real R2 (workerd #2572 is why the object form isn't used).
3. Run every scenario against the preview with `scenario-cli` and trace assertions pulled from the
   collector.
4. **Measurements** from design §12: rows/s per writer with batched writes, flush CPU per MB, and the
   write pause during a seal-then-fork split. Record them in `docs/measurements.md` with the method
   and the stage they came from.

Record each result in `docs/progress.md`. If a live result contradicts a local test or a model,
treat it as a finding: fix the code, the test, or the model, and say which was wrong.

### 5. Data residency, per Stack

Dillon chose per-Stack residency. One jurisdiction (`eu`, `fedramp`, or none) is set as a Stack
option and applied to the R2 bucket and every Durable Object namespace. Remove or reconcile the
advisory per-database `jurisdiction` field in the manifest (`packages/lsm-core/src/manifest.ts`) and
fork inheritance, with a recorded decision. Don't break existing manifests: decode the old field and
ignore it.

### 6. Known gaps

- **Transactions abort when a flush lands mid-transaction** (D41). Let a transaction's conflict check
  read rows flushed after its snapshot from L0 rather than aborting, or justify keeping the limit.
  Update the transactions model and its verify skill.
- **L1 block cache is bounded by entry count** (D32). Bound it by bytes, with a per-`budgetGroup`
  budget as design §12 describes.
- **Snapshots are protected only for `gcMinAge`** (D30). Long-lived snapshots should pin through a
  checkpoint, or the limit should be documented in the public API with `SnapshotExpired`.
- **A corrupted SST fails its compaction as a defect** (D34). Make it a typed, reported failure.

### 7. Finish

Update `docs/progress.md` with a final hand-off: what was built, how to run it locally and on a
preview, what is verified (bounds and assumptions), live results and measurements, and anything
left. Give Dillon the commands to destroy each preview stage.

## Prompt for the new agent

```text
You're continuing cf-slatedb, a SlateDB-style LSM key-value store on Cloudflare R2 and Durable
Objects, written with Effect 4 and Alchemy v2. Read docs/handoff.md in full first, then AGENTS.md,
docs/implementation-brief.md, and the design at docs/show-me-r2-lsm-design.html. Set up the machine
per "Set up on a new machine" and confirm `vp run ready` passes before changing anything. Then do
the "Remaining work" items in order, autonomously, keeping docs/progress.md,
docs/design-decisions.md, and docs/coverage-matrix.md current, committing and pushing to origin main
after each item. Live deploys are approved for stages preview-1, preview-2, and preview-3 only:
never deploy production and never run `alchemy destroy`. Ask Dillon only for what the hand-off says
needs him, and keep working on anything not blocked while you wait.
```
