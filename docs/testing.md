# Testing

## Strategy

shaledb tests **locally by default**. Every layer of the system has a local form that is faithful
enough to prove its contract, and the default suite never touches a live Cloudflare account.

The confidence ladder, from narrowest to widest:

1. **Unit and property tests** on pure code in `@shaledb/lsm-core` and on services through
   their faithful test Layers: codecs, the merge iterator, manifest edits, the compaction
   scheduler, the memtable, the writer, the reader, compaction, GC, and lifecycle. Property tests
   generate inputs with Effect's `Arbitrary` (fast-check underneath) and compare against a plain
   `Map` oracle.
2. **Workerd integration** through Alchemy's local providers (`alchemy/Test/Vitest` with
   `dev: true`): the real Durable Objects with SQLite storage, the local R2 simulator, local
   Workflows, service bindings, and OTLP export to the trace CLI's receiver.
3. **Scenario tests** with `tools/scenario-cli` against a locally running Stack (`vp run dev`).
   Each scenario drives the public HTTP API and asserts on both results and the traces the trace
   CLI collected (`trace assert`).
4. **Formal verification** (`vp run verify`): TLA+ models of the protocols (fencing, manifest CAS,
   GC, seal and split, transactions), each with a project-local `verify-*` skill.

A feature is complete when it has unit or property coverage for its pure logic, coverage through
its service's public interface with faithful test Layers, and, when it crosses a Worker, Durable
Object, Workflow, or R2 boundary, a workerd or scenario test with trace assertions. Each design
section and its tests are listed in [`coverage-matrix.md`](coverage-matrix.md).

## Real-cloud tests are opt-in

Real-cloud checks run against one of the three reusable preview stages (`preview-1`, `preview-2`,
`preview-3`), which are approved for live deploys. Deploy or redeploy a stage in place with
`node tools/preview-cli/src/preview-cli.ts deploy <stage>` (add `--dev-timings` for `vp run dev`'s
short GC and compactor timings; the default is the production settings, so measurements are
honest), then run checks that need real Cloudflare: Workers Cache `Cf-Cache-Status: HIT`, R2
`If-None-Match: *` against real R2, Workflow limits, and the scenarios. They are never reachable
from `vp run ready` and never cached. Stages are reused, not destroyed; Dillon tears them down
(`preview-cli destroy-command <stage>`).

Never test against `production`, a developer's stage, or another run's stage.

`node tools/scenario-cli/src/scenario-cli.ts run --stage <stage> all` runs every scenario against a
preview (deploy it with `--dev-timings`: the scenarios expect compaction and GC within seconds), and
`scenario-cli measure writes | flush-cpu | split-pause --stage <stage>` takes the design §12
measurements (deploy without `--dev-timings`). `preview-cli bindings <stage>` shows the settings and
jurisdiction each Worker is bound (`production` means exactly the defaults); check it before a
measurement. Record results in
[`measurements.md`](measurements.md) with the stage and method.

A preview's spans go to its own `TraceCollector` Worker. Run `node tools/trace-cli/src/trace-cli.ts
pull --stage <stage>` (or `tail --stage <stage>`) to bring them into the local trace store before
`assert`, `show`, or `stats`. Production's are read with `--collector-url` instead
([`operations.md`](operations.md), "Reading a stage's telemetry"). Live Workers export after the response (D5), so pull a few seconds
after the requests a check asserts on.

## The soak's workload

`scenario-cli soak --stage <stage>` runs a mixed workload for its `--minutes` and checks it against
a model of what was acknowledged. Besides writes, gets at three consistencies, scans, transactions,
snapshots, removals, splits, and a redeploy, it runs one workload per class of defect found in the
v1 audits (D106; the models are in `soak-checks.ts`, tested in `soak-checks.test.ts`):

| Workload                                                                                              | Exercises                                                                        | `checks` keys                                                   |
| ----------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| A change-feed follower per soak database, for the whole run, read to the end at the finish            | D80, D93b: every acknowledged batch once, in order; a 1,500-row batch every 60th | `feed.page`, `feed.delivered`, `feed.final`, `write.big_batch`  |
| A database with a feed, split by the documented procedure partway (40% of the run, at least minute 2) | D80: the sealed source to its end, then each successor from its `lastSeq`        | `feedsplit.split`, `feedsplit.source_ended`, `feedsplit.final`  |
| Three `Concat` keys built from a base and operands, half with 10 to 40 s TTLs, flushed every 15 s     | D81: each operand expires on its own, through compactions                        | `merge.get`, `merge.get_after_expiry`, `merge.not_judged`       |
| Scans of 300 keys with `limit` 7, continued by `resumeFrom`, at `Latest`                              | D93: no key skipped or repeated across pages                                     | `scan.paged`, `scan.paged_early_end`                            |
| Eight write-and-flush pairs back to back, every four minutes                                          | D78: the compactor is never fenced, and L0 drains                                | `burst.flushes`, `burst.compactor`                              |
| A database written once, then read every 20 s, with a transaction every 3 minutes                     | D76, once the run passes about 65 minutes                                        | `idle.get`, `idle.transaction`, `idle.transaction_after_gc_age` |
| A fork from a checkpoint at the start, read every 5 minutes and at the end                            | D77; a refused deletion of the fork's hold                                       | `fork.read`, `final.fork`, `fork.hold_delete_refused …`         |
| Recreating each removed id at once                                                                    | D90: refused with `DatabaseRecentlyRemoved`                                      | `remove.recreate_refused …`                                     |
| One write in 50 and one commit in 20 aborted by the client after 2 to 22 ms                           | D74b, D85: every later check holds                                               | `write.aborted`, `transaction.aborted`, `final.aborted_key`     |

Every soak database keeps its changelog for the run's length plus an hour, so `Expired` is a
failure. A merge read is judged within `CLOCK_SKEW_MS` (5 s) of each expiry: inside that window
either answer passes.

**Its telemetry keeps up with it (D132).** While the workload runs, the soak pulls the stage's
spans every 30 seconds, one pull at a time, each within 5 minutes; a pull cut off resumes after the
last export it stored. After the workload it catches the pull up, then reads the health report and
the error spans from that store; each `grep` reads only the trace files written during its window
(`trace-cli grep --since`). The report shows `spansPulledThrough`, `spanPullLagAtWorkloadEndSeconds`,
and `spanPulls` (finished, cut off, failed) while it runs, so a collector too slow for the stage is
visible before the telemetry budget ends. Tests: `soak-telemetry.test.ts` (fake pulls: one that
cannot keep up, two at once, a failure, a workload ending mid-pull), `preview-trace-pull.test.ts`
(a pull cut off mid-page stores nothing twice), `trace-store.test.ts`.

## Faithful local substitutes

- `objectStoreTestLayer` is an in-memory object store with real create-if-absent semantics
  (`putIfAbsent` fails with a conflict when the key exists), byte ranges, multipart uploads, and
  prefix listing, so fencing and CAS tests are honest.
- `blockSourceTestLayer` reads blocks and SST metadata from the same in-memory object store. Workers
  Cache is not emulated by Vitest or `alchemy dev`, so cache-hit behavior is verified only by the
  approved live check.
- The memtable runs on real SQLite in workerd tests and on an in-memory SQLite-compatible test
  Layer in unit tests (see `mem-table.ts`).
- `TestClock` drives flush intervals, `maxL0Age`, GC `minAge`, and alarms.

Never add production test-only endpoints, corruption switches, or fault flags. Inject failures
through test Layers (a failing object store, a store that loses a race) or through operational
boundaries that naturally create the condition (evicting a Durable Object, restarting `alchemy
dev`).

## Error-path accounting

Every typed error that a caller can see has a test that produces it through the public interface:
`KeyOutOfRangeError`, `DatabaseSealedError`, `WriterFencedError`, `WriteStalledError`,
`DatabaseHasDependentsError`, `TransactionConflictError`, `SstCorruptedError`,
`ObjectStoreUnavailableError`, and every public HTTP error variant. The coverage matrix records
which test covers each one and why an error is covered below the scenario level when it cannot be
induced locally through the public API.

## Trace assertions

Every Worker and Durable Object exports OTLP spans to the trace CLI receiver
(`node tools/trace-cli/src/trace-cli.ts serve`, default `127.0.0.1:4318`). Scenario and workerd tests
assert on the collected traces, for example:

- a request produced a `WriterServer.write` span under the API Worker's HTTP server span;
- no `ObjectStore.list` span appears on a read path;
- a flush made exactly one manifest CAS;
- no span has an error status.

Spans carry only safe identifying attributes (`database.id`, `sst.id`, `manifest.id`, `seq`, sizes,
cache hit or miss). Never put keys, values, or secrets in attributes.

## Effect Vitest conventions

- Use ordinary `it` for synchronous pure behavior.
- Use `it.effect` for scoped Effect programs and test services such as `TestClock`.
- Provide a fresh Layer per test when mutable state must be isolated.
- Assert public values, errors, responses, persisted state, and traces, not implementation calls or
  spies. Module mocking with `vi.mock` is forbidden.
- Do not restate straightforward Schema declarations or Effect/library behavior.
- Use bounded schedules for polling and explicit timeouts for workerd startup.

## Commands

```sh
vp run ready                         # format, lint, type check, every workspace's tests (local only)
vp run dev                           # the full Stack under alchemy dev (stage dev)
node tools/trace-cli/src/trace-cli.ts serve       # OTLP receiver and trace store
node tools/scenario-cli/src/scenario-cli.ts run all   # every scenario against the local Stack
vp run verify                        # every formal check
node tools/preview-cli/src/preview-cli.ts status   # what is deployed to each preview stage
```

`vp run ready` is the validation gate. Report a command as passing only when its exit code was
observed to be `0`.
