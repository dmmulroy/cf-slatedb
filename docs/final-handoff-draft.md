# Final hand-off (draft for `docs/progress.md`)

A draft of `docs/handoff-v1.md` §10.5. The orchestrator folds it into `docs/progress.md`'s
"Hand-off" when the release results are in. A line starting `PENDING(<id>):` is a result still
being produced.

## What changed in workstream 10

- **The release tools (D65).**
  - **`scenario-cli soak` is the release gate.** It runs several databases with mixed writes,
    reads at every consistency, scans, transactions, pinned snapshots, removals, a sharded group
    split twice, and a redeploy midway. At the end, a model of every acknowledged write is checked
    at `Latest` and `Flushed`, and storage must be bounded after GC.
  - **It was rebuilt so it cannot lose its results.** It runs in phases: data checks before
    telemetry, and every telemetry read bounded by a budget whose overrun is a failure.
    `--report-file` is replaced atomically every minute (`docs/release-v1.md` §5).
  - **`scenario-cli smoke` is the production smoke test.** It runs on databases it names and
    removes itself.
- **Defects found and fixed:**
  - **D66.** The block cache shared reads in flight across requests. That caused 5-minute hangs
    and a 500 during a redeploy.
  - **D69.** A request the schemas refuse returned 500. It is now `InvalidRequest` (400).
  - **D70.** A `Latest` read could return "not found" for a key that always had a value, by using a
    manifest flushed and compacted past its snapshot. Present since Phase 5. Found by the model,
    never seen live.
  - **D74.** The writer's alarm looped without flushing, and every transaction begin scanned the
    whole memtable.
  - **D74, corrected.** A commit racing the counter refresh could leave its row counted nowhere, so
    unflushed until a later write. Found by review.
  - **D76.** On a database read but not flushed for over an hour after a compaction, every
    transaction read failed with `SnapshotTooOld`, and so did every retry. Unpinned snapshots lived
    half the documented hour. Found by checking `docs/invariants.md` against the code.
  - **D77.** A fork released its hold on its source while readers of its recent manifests still
    needed the source's SSTs. Found by the interaction audit. Model: `GcSafetyFork.cfg`.
  - **D78.** Two overlapping first wakes fenced a compactor against itself for good, so writes to
    that database were refused permanently. Found by `measure manifest-cas` on `preview-3`. The
    stuck fixture recovered live within 11 s of its first write after the deploy.
  - **D80, D81, D83** from the review and interaction audit: the change feed skipped changes
    silently, compaction changed what a key with expiring operands reads, and a collection went
    on without a manifest it retained.
  - **Audit round 2** (five auditors by area): D84 to D101, with D90, D91, D102, and D103 still on
    their branches. Among them: a lost update in `Snapshot` commits (D84); writes acknowledged
    after an unseen seal (D85); compactions that could never run or never commit (D86, D96, D97);
    a TTL that made a database unflushable (D88); a stale router serving a sealed shard (D89); a
    removed database's id serving its old data (D90, D91); unbounded scans, pages, and bodies
    (D93, D100); an abandoned split that blocked a range forever (D101).
  - **What that says:** `docs/release-v1.md` §8. It covers 23 defects by class and by how each
    was found. Every gate that runs the system passed with them in the code; reading the code
    against a stated rule found them.
- **Read path (D67, D72).**
  - A read reuses any cached manifest holding the writer's flushed rows, and a refresh takes one
    round trip from the manifest held.
  - SSTs are opened together. A slow block read is hedged from the bucket, except in a restricted
    Stack. Scans read ahead, stop at their limit, and use 16 KiB blocks.
  - On `preview-3`, a `Flushed` get went from 81 / 162 / 515 ms to 5 / 113 / 243 ms (p50 / p90 /
    p99). A 100-row scan went from 76 / 555 / 2,146 ms to 60 / 257 / 983 ms
    (`docs/measurements.md`, "Read latency").
- **Flush path (D70, D73).**
  - A flush writes only the versions a read can reach: 0.44 MB stored instead of 4.4 MB for the
    soak's workload.
  - A flush no longer waits for the compactor it wakes, and rows are encoded a chunk at a time.
- **Telemetry (D71, and production reads).**
  - Tail records are sampled like traces, except those that did not end `ok`.
  - `trace-cli pull`/`tail` and `scenario-cli measure health`/`latency` read any live stage,
    production included, by `--collector-url` with `SHALEDB_TRACE_TOKEN`.
    `preview-cli trace-token <stage>` writes that token only into a pipe.
- **The baseline (D75).** A plain Durable Object SQLite baseline in the probe Worker and
  `measure baseline`. PENDING(baseline): its result.
- **The invariants guide.** `docs/invariants.md` ties each invariant (acknowledged, flushed, what
  each read may see, when GC may reclaim an SST) to the code that enforces it and the test or model
  that checks it.
- **The rename (D68).** cf-slatedb became shaledb. The previews were redeployed as new
  `Shaledb/preview-<n>` stacks, and the old `CfSlatedb/preview-<n>` stacks remain (below).
- **The release checklist** is `docs/release-v1.md`.

## What was verified

- **The gates on the D70–D74 code:** `vp run ready` exit 0 and `vp run verify` exit 0, 28/28 TLC
  runs (`docs/progress.md`). Also 13/13 scenarios under `vp run dev` and on `preview-3`
  (`--dev-timings`), and 5-minute soaks passing on `preview-2` and `preview-3` for each arm of the
  D70–D74 experiments.
- **Mutations:** thirty-five mutations of the D70–D74 code, each caught by a test. The later fixes
  (D76, the counter race, hedging, the soak tool, the telemetry reads) each list their mutation
  checks in their reports.
- **On `f3b2ad2`, before D76–D78 and the counter fix:** the 20-minute `eu` soak on `preview-2`
  passed. It found no mismatch and no failure, and every invocation ended `ok`
  (`docs/release-v1.md` §5). This is evidence about `f3b2ad2`, not the release gate.

- **On `f3b2ad2`, the 132-minute soak on `preview-1` passed** (22:25:52–00:43:35 UTC; 6 databases,
  two splits, a redeploy). It found no mismatch, no failure, and no unexpected error span; 9
  operations were retried once and then answered (`docs/release-v1.md` §5). Every defect from D76 on
  was in that code.

PENDING(final-commit): the release commit, with `vp run ready` and `vp run verify` observed at
exit 0 on it, and the number of TLC runs.

PENDING(soak-preview-1): the 130-minute soak on the release commit on `preview-1`: pass or fail on
each criterion, p50/p99 per operation, the health summary.

PENDING(soak-preview-2): the 20-minute `eu` soak on the release commit on `preview-2`: the same.

PENDING(step4): writes, split pause, compaction CPU, flush CPU, and read latency re-measured on the
final code, each against its target.

- **D77 and D78 are merged** (`proto/integration-2`), with their tests, `GcSafetyFork.cfg`, and
  D78's live recovery on `preview-3` (`ed4e787`, `/tmp/shale-work/release2/NOTES.md`).
- **`docs/api-compatibility.md`** lists every limit of v1, pinned to the constants by
  `api-compatibility-limits.test.ts`.

PENDING(d90), PENDING(d102), PENDING(d103): D90 and D91, D102, and D103 merged, with their tests.

## What is left

- **P6.** Dillon decides with `docs/release-v1.md`. If he says go, the production deploy, its smoke
  test, and the production URLs (`apiUrl`, `traceCollectorUrl`) are recorded here.
- **D90, D91, D102, D103** are merged, then the release soaks run on the release commit.
- **Not reached by the audits** (`docs/release-v1.md` §8): `MemTable` migrations, the `BlockServer`
  under a hostile caller, a compaction step's memory with 8 inputs, and the tools.
- **The public scan page is bounded by rows only** (1,000 rows of up to 1 MiB each); open.
- **The deployment guard.** Its allowlist names only the pre-rename project path, which the guard
  skips. `scripts/allowlist-shaledb-deploys.sh` must be rerun from the main checkout, and its stage
  list does not keep agents off `production` (`docs/release-v1.md` §10 step 2).
- **Metrics are emitted, not collected (D82).** Every signal `docs/operations.md` names is now
  read from a span or a tail record, and `measure health` judges each one. The `lsm_*` counters
  stay for a future collector. The new span attributes are not yet deployed or tried live.
- `docs/progress.md`'s workstream table still says "this commit" for workstream 9. It is `8a96667`.
- Everything in `docs/release-v1.md` §9 ("Known limitations").

## Destroying what is left on Cloudflare (Dillon runs these; agents never do)

Run these in your own terminal, not through an agent. The deployment guard blocks an
`alchemy destroy` an agent types directly, whatever its allowlist says, but in this allowlisted
repository it does not resolve `vp run` tasks, so it would not stop an agent running these
(`docs/release-v1.md`, section 10). Keep agents off them by instruction. Each command needs Alchemy logged in to
account `99459fce09d9a9eec83befd957af4ec5`; live stacks keep their state in that account's
Cloudflare state store (D43).

Order matters. Destroy the stacks first, then delete their Secrets Store secrets. A Stack looks its
token secrets up when it is evaluated (`Secret.lookup`), so a destroy run after the secrets are gone
may fail to evaluate. This is inferred from the code, not tried.

### 1. The three current previews: `Shaledb/preview-1`, `-2`, `-3`

From the repository root of any current checkout:

```sh
vp run @shaledb/infra#destroy:preview-1
vp run @shaledb/infra#destroy:preview-2
vp run @shaledb/infra#destroy:preview-3
```

Each task runs `alchemy destroy --stage preview-<n> --yes` in `infra/shaledb`
(`infra/shaledb/vite.config.ts`). `node tools/preview-cli/src/preview-cli.ts destroy-command <stage>`
prints the same line (`destroyCommand` in `tools/preview-cli/src/preview-stage.ts`; tested in
`preview-stage.test.ts`).

Each destroy removes everything the stage's state holds:

- the API, `BlockServer`, compactor, GC, shard-router, probe, trace-collector, and CPU Tail Workers;
- their Durable Objects and the compaction Workflow;
- the LSM bucket and the trace bucket. Both are `forceDestroy` outside production
  (`lsm-bucket.ts`, `trace-collector-worker.ts`), so they are emptied first;
- the state-held preview token (`LsmAccessToken`, an Alchemy random value);
- the stage's state records.

`preview-2` is in `eu`. If its destroy complains about the jurisdiction, rerun it with
`SHALEDB_JURISDICTION=eu` set, so the Stack resolves the jurisdiction it was deployed with.

It does **not** remove the stage's two token secrets. See step 3.

Wait until the release soaks and any measurements on a stage are finished before destroying it.

**D78's fixture on `preview-3` has served its purpose.** Database `ms-cas-mwy5ujbj` was stuck
from 23:05 UTC. After D78 was deployed (`ed4e787`) it compacted and accepted writes again, at
23:34:52–23:35:16 UTC (`/tmp/shale-work/release2/NOTES.md`). `preview-3` may be destroyed with the
others once the release measurements on it are done.

### 2. The orphaned stacks from before the rename: `CfSlatedb/preview-1`, `-2`, `-3`

The rename (D68, commit `fe552f0`) changed the Stack name from `CfSlatedb` to `Shaledb`. Alchemy
keys state by Stack name, so the old stacks are still deployed with their own Workers, buckets, and
state. Their destroy tasks exist only in a checkout from before the rename. The last such commit is
`8fab306`, the parent of `fe552f0`. At that commit:

- the package is `@cf-slatedb/infra` (`git show 8fab306:infra/cf-slatedb/package.json`);
- the Stack is `Alchemy.Stack("CfSlatedb", …)` (`infra/cf-slatedb/alchemy.run.ts`);
- `destroy:preview-<n>` runs `alchemy destroy --stage preview-<n> --yes`
  (`git show 8fab306:infra/cf-slatedb/vite.config.ts`);
- the LSM bucket sets `forceDestroy: !production` (`packages/lsm-cloudflare/src/lsm-bucket.ts`) and
  the trace bucket `forceDestroy: stage !== CF_SLATEDB_PRODUCTION_STAGE`
  (`infra/cf-slatedb/src/trace-collector-worker.ts`), so both are emptied and deleted;
- state is `Cloudflare.state({ accountId: CF_SLATEDB_CLOUDFLARE_ACCOUNT_ID })`, the same account's
  state store.

Make a worktree of that commit **outside** this repository's directory, install, destroy, and
remove the worktree:

```sh
git -C /Users/dmmulroy/Code/personal/cf-slatedb worktree add --detach /tmp/cf-slatedb-8fab306 8fab306
cd /tmp/cf-slatedb-8fab306
pnpm install
vp run @cf-slatedb/infra#destroy:preview-1
CF_SLATEDB_JURISDICTION=eu vp run @cf-slatedb/infra#destroy:preview-2
vp run @cf-slatedb/infra#destroy:preview-3
cd /Users/dmmulroy/Code/personal/cf-slatedb
git worktree remove /tmp/cf-slatedb-8fab306
```

Each destroy removes the old stage's Workers, Durable Objects, Workflow, both buckets (emptied
first), its state-held token, and its state records under `CfSlatedb/preview-<n>`. It does not touch
the `Shaledb` stacks, which have their own state.

What could make it fail:

- **The guard, if run through an agent.** The guard blocks a directly typed `alchemy destroy` for
  agents always, but not a `vp run @shaledb/infra#destroy:<stage>` task in this repository: it does
  not resolve package tasks in an allowlisted repository (`docs/release-v1.md`, section 10).
  Its allowlist names `/Users/dmmulroy/Code/personal/cf-slatedb/infra/cf-slatedb/alchemy.run.ts`,
  which no longer exists in the main checkout, so the guard skips that entry. That entry is only
  relevant if you allowlist the old worktree. In your own terminal the guard does not run.
- **`preview-2`'s jurisdiction.** It was deployed in `eu`. Setting `CF_SLATEDB_JURISDICTION=eu` (the
  variable's name before the rename) makes the old Stack resolve the same jurisdiction. It is
  harmless if the destroy works from state alone.
- **Leftover variables.** At `8fab306` the Stack reads `CF_SLATEDB_*`. An invalid value in your
  shell (for example a stale `CF_SLATEDB_LSM_SETTINGS`) refuses the plan. The `SHALEDB_*` variables
  are ignored there.
- **The install.** `pnpm install` at that commit downloads the dependencies of that day (pnpm 12.14
  through `devEngines`, Node ≥ 22.18) and needs the network.
- **A stage that was never deployed under the old name.** Alchemy then has nothing in state, and
  the destroy should report no changes.

### 3. The Secrets Store token secrets

Workers bind the token secrets by reference (`Cloudflare.SecretsStore.Secret.lookup` in
`infra/shaledb/src/shaledb-access-tokens.ts`, and the same at `8fab306` in
`cf-slatedb-access-tokens.ts`). Alchemy's documentation of `Secret.lookup` says such a secret "is read
during plan/deploy and never created, updated or deleted"
(`node_modules/@alchemy.run/cloudflare/src/SecretsStore/Secret.ts`). The `tokens` task created the
secrets directly through the API (`createStoreSecret` in `infra/shaledb/src/access-token-store.ts`).
**No destroy removes them.** Delete them yourself, after steps 1 and 2:

| Secrets                                                       | Left by                      | Delete after |
| ------------------------------------------------------------- | ---------------------------- | ------------ |
| `CF_SLATEDB_PREVIEW_1_ACCESS_TOKEN_A`, `_B`; `…_2_…`; `…_3_…` | the old `CfSlatedb` previews | step 2       |
| `SHALEDB_PREVIEW_1_ACCESS_TOKEN_A`, `_B`; `…_2_…`; `…_3_…`    | the current previews         | step 1       |

Do not delete `SHALEDB_PRODUCTION_ACCESS_TOKEN_A` or `_B` if they exist. Production's Workers bind
them.

The repository's code finds the store by name: the account's store named `default_secrets_store`
(`SHALEDB_SECRETS_STORE_NAME`), found by listing the account's stores. It finds a secret by listing
the store's secrets with `search=<name>` and keeping the exact name match (`tokenStore` and
`secretIdByName` in `access-token-store.ts`). Do the same.

**With Wrangler.** From
[Wrangler commands: Secrets Store](https://developers.cloudflare.com/workers/wrangler/commands/secrets-store/)
(`secrets-store store list`, `secrets-store secret list`, `secrets-store secret delete`). That page
says to append `--remote`; without it, the commands act on local development storage. Wrangler is not
a dependency of this repository, so `npx wrangler` downloads it.

```sh
npx wrangler secrets-store store list --remote
# the ID of the store named default_secrets_store:
STORE_ID=<store id>
npx wrangler secrets-store secret list "$STORE_ID" --remote --per-page 100
# for each of the twelve secrets above, its ID from the list:
npx wrangler secrets-store secret delete "$STORE_ID" --secret-id <secret id> --remote
```

**With the REST API.** From the API reference
[List account stores](https://developers.cloudflare.com/api/resources/secrets_store/subresources/stores/methods/list/)
(`GET /accounts/{account_id}/secrets_store/stores`),
[List store secrets](https://developers.cloudflare.com/api/resources/secrets_store/subresources/stores/subresources/secrets/methods/list/)
(`GET …/stores/{store_id}/secrets`, with `search` and `per_page` up to 100), and
[Secrets Store: How to, "Delete a secret"](https://developers.cloudflare.com/secrets-store/manage-secrets/how-to/)
(`DELETE …/stores/$STORE_ID/secrets/$SECRET_ID`). The token needs the `Secrets Store Write`
permission; listing also accepts `Secrets Store Read`. The documentation's examples authenticate with
`X-Auth-Email` and `X-Auth-Key`. Its duplicate and edit examples use `Authorization: Bearer`, used
here.

```sh
ACCOUNT_ID=99459fce09d9a9eec83befd957af4ec5
API=https://api.cloudflare.com/client/v4/accounts/$ACCOUNT_ID/secrets_store
curl "$API/stores" -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN"
STORE_ID=<id of default_secrets_store>
curl "$API/stores/$STORE_ID/secrets?search=CF_SLATEDB_PREVIEW&per_page=100" -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN"
curl "$API/stores/$STORE_ID/secrets?search=SHALEDB_PREVIEW&per_page=100" -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN"
# for each matching secret's id:
curl -X DELETE "$API/stores/$STORE_ID/secrets/<secret id>" -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN"
```

Check each listed `name` before deleting. `search` also matches comments. Cloudflare's how-to page
warns to delete a secret only once nothing deployed uses it, which steps 1 and 2 ensure.

## Local worktrees and branches to remove after the merge

The parallel rounds left 27 git worktrees under `/tmp/shale-work/wt/`, each on its own `proto/*`
branch of the main repository (`git worktree list`, after `proto/integration-2` reached `e33d1a0`).
They are on this laptop only and were never pushed. Once the release is merged into `main`, every
branch merged into `proto/integration-2` is contained in `main`, and its worktree can go.

| Worktree (`/tmp/shale-work/wt/…`)                                                                                                                                                                                                                                   | Branch                                                           | In `proto/integration-2`                                         |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- | ---------------------------------------------------------------- |
| `integ2`                                                                                                                                                                                                                                                            | `proto/integration-2`                                            | is it                                                            |
| `audit-do`, `audit-workflow`                                                                                                                                                                                                                                        | `proto/audit-do`, `proto/audit-workflow`                         | yes (D102, D103 after their next merge)                          |
| `audit-api`, `audit-lifecycle`, `audit-limits`                                                                                                                                                                                                                      | `proto/audit-api`, `proto/audit-lifecycle`, `proto/audit-limits` | not yet: work after their last merge (D90, D91; this checklist)  |
| `review`, `prod-health`, `feed-ttl`, `fork-hold`, `compactor-fence`, `txn-fix`, `soak-tool`, `docs`, `baseline`, `candidate`, `writer-counters`, `tools`, `newest-flush`, `flush-cpu`, `background-wake`, `hedged-reads`, `limit-read-ahead`, `refresh-from-cached` | `proto/<the same name>`                                          | yes                                                              |
| `release-doc`                                                                                                                                                                                                                                                       | `proto/release-doc`                                              | yes                                                              |
| `parallel-parts`                                                                                                                                                                                                                                                    | `proto/parallel-parts`                                           | no: the declined concurrent part uploads (D73 names this branch) |
| `integration`                                                                                                                                                                                                                                                       | `proto/integration`                                              | no: an earlier integration that merged `proto/parallel-parts`    |

From the main checkout, on `main`, after the merge:

```sh
cd /Users/dmmulroy/Code/personal/cf-slatedb
for name in integ2 audit-api audit-do audit-lifecycle audit-limits audit-workflow review \
  prod-health feed-ttl fork-hold compactor-fence txn-fix soak-tool docs baseline candidate \
  writer-counters tools newest-flush flush-cpu background-wake hedged-reads limit-read-ahead \
  refresh-from-cached release-doc; do
  git worktree remove "/tmp/shale-work/wt/$name"
done
for name in integration-2 audit-api audit-do audit-lifecycle audit-limits audit-workflow review \
  prod-health feed-ttl fork-hold compactor-fence txn-fix soak-tool docs baseline candidate \
  writer-counters tools newest-flush flush-cpu background-wake hedged-reads limit-read-ahead \
  refresh-from-cached release-doc; do
  git branch -d "proto/$name"
done
git worktree prune
```

- **The removal is safe by default.** `git worktree remove` refuses a worktree with uncommitted
  changes, and `git branch -d` refuses a branch that is not merged into `main`. A refusal means
  something was not merged: look before forcing.
- **`proto/parallel-parts` holds the only copy of a declined prototype** (`d9c145e`, concurrent
  multipart uploads), and D73 names the branch. `proto/integration` exists only to merge it. Keep
  both, or tag the prototype before removing them:

  ```sh
  git tag parallel-parts-prototype d9c145e
  git worktree remove /tmp/shale-work/wt/parallel-parts && git worktree remove /tmp/shale-work/wt/integration
  git branch -D proto/parallel-parts proto/integration
  ```

- **What else lives in `/tmp/shale-work`.** Besides the worktrees, the agents' directories
  (`agents/`), the release outputs (`release/`, including `soak-p2.json`), and the measurement
  scripts (`scripts/`) are the evidence several documents cite by path. Copy what you want to keep
  before `/tmp` is cleared.
