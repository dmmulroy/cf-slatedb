# Final hand-off (draft for `docs/progress.md`)

A draft of `docs/handoff-v1.md` §10.5. The orchestrator folds it into `docs/progress.md`'s
"Hand-off" when the release results are in. A line starting `PENDING(<id>):` is a result still
being produced.

## What changed in workstream 10

- **The release tools (D65).** `scenario-cli soak` is the release gate: several databases, mixed
  writes, reads at every consistency, scans, transactions, pinned snapshots, removals, a sharded
  group split twice, and a redeploy midway. A model of every acknowledged write is checked at
  `Latest` and `Flushed` at the end, with storage bounded after GC. `scenario-cli smoke` is the
  production smoke test, on databases it names and removes itself.
- **Defects the soaks and models found, all fixed:**
  - D66: the block cache shared reads in flight across requests, which caused 5-minute hangs and a
    500 during a redeploy.
  - D69: a request the schemas refuse returned 500; it is now `InvalidRequest` (400).
  - D70: a `Latest` read could return "not found" for a key that always had a value, by using a
    manifest flushed and compacted past its snapshot. Present since Phase 5, found by the model,
    never seen live.
  - D74: the writer's alarm looped without flushing, and every transaction begin scanned the whole
    memtable.
- **Read path (D67, D72).**
  - A read reuses any cached manifest holding the writer's flushed rows, and a refresh takes one
    round trip from the manifest held.
  - SSTs are opened together. A slow block read is hedged from the bucket. Scans read ahead, stop
    at their limit, and use 16 KiB blocks.
  - On `preview-3`, a `Flushed` get went from 81 / 162 / 515 ms to 5 / 113 / 243 ms (p50 / p90 /
    p99). A 100-row scan went from 76 / 555 / 2,146 ms to 60 / 257 / 983 ms.
- **Flush path (D70, D73).**
  - A flush writes only the versions a read can reach: 0.44 MB stored instead of 4.4 MB for the
    soak's workload.
  - A flush no longer waits for the compactor it wakes, and rows are encoded a chunk at a time.
- **Telemetry (D71).** Tail records are sampled like traces, except those that did not end `ok`.
- **The rename (D68).** cf-slatedb became shaledb. The previews were redeployed as new
  `Shaledb/preview-<n>` stacks, and the old `CfSlatedb/preview-<n>` stacks remain (below).
- **The release checklist** is `docs/release-v1.md`.

## What was verified

- `vp run ready` exit 0 and `vp run verify` exit 0, 28/28 TLC runs, on the D70–D74 code
  (`docs/progress.md`).
- 13/13 scenarios under `vp run dev` and on `preview-3` (`--dev-timings`).
- 5-minute soaks passing on `preview-2` and `preview-3` for each arm of the D70–D74 experiments.
- Thirty-five mutations of the D70–D74 code, each caught by a test.

PENDING(final-commit): the release commit, with `vp run ready` and `vp run verify` observed at
exit 0 on it.

PENDING(soak-preview-1): the 130-minute soak on the final code on `preview-1`: pass or fail on each
criterion, p50/p99 per operation, the health summary.

PENDING(soak-preview-2): the 20-minute `eu` soak on the final code on `preview-2`: the same.

PENDING(step4): writes, split pause, compaction CPU, flush CPU, and read latency re-measured on the
final code, each against its target.

PENDING(baseline): the comparison with plain Durable Object SQLite.

## What is left

- **P6.** Dillon decides with `docs/release-v1.md`. If he says go, the production deploy, its smoke
  test, and the production URL are recorded here.
- **Production health tooling.** `measure health` and `trace-cli pull --stage` read only
  `preview-1..3`. Nothing in the repository reads production's collector yet
  (`docs/release-v1.md` §8).
- **The deployment guard.** Its allowlist names only the pre-rename project path, which the guard
  skips. `scripts/allowlist-shaledb-deploys.sh` must be rerun, and its stage list does not keep
  agents off `production` (`docs/release-v1.md` §9 step 2).
- `docs/progress.md`'s workstream table still says "this commit" for workstream 9. It is `8a96667`.
- Everything in `docs/release-v1.md` §8 ("Known limitations").

## Destroying what is left on Cloudflare (Dillon runs these; agents never do)

Run these in your own terminal, not through an agent. The deployment guard blocks
`alchemy destroy` for agents whatever its allowlist says. Each command needs Alchemy logged in to
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

- **The guard, if run through an agent.** The guard blocks `alchemy destroy` for agents always.
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
