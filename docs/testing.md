# Testing

## Strategy

cf-slatedb tests **locally by default**. Every layer of the system has a local form that is faithful
enough to prove its contract, and the default suite never touches a live Cloudflare account.

The confidence ladder, from narrowest to widest:

1. **Unit and property tests** on pure code in `@cf-slatedb/lsm-core` and on services through
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

Real-cloud end-to-end tests deploy a fresh Stack to a unique preview stage, run checks that need
real Cloudflare (Workers Cache `Cf-Cache-Status: HIT`, R2 `If-None-Match: *` against real R2,
Workflow limits), and destroy it. They are a separate task that **never runs without Dillon's
approval**, is not reachable from `vp run ready`, and is never cached. Record every check that is
waiting on a live run under "Open questions" in [`progress.md`](progress.md).

Never test against `production`, a developer's stage, or another run's stage.

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
```

`vp run ready` is the validation gate. Report a command as passing only when its exit code was
observed to be `0`.
