---
name: verify-seal-split
description: "Model-check that sealing a shaledb database stops every later write, and that splitting it (seal, one checkpoint, a range fork per successor) leaves every write acknowledged before the seal in exactly one successor. Use after any change to DbWriter.write/seal, the commit lock, DatabaseLifecycle.seal/createCheckpoint/forkRange, forkManifest, or the split order in lsm-sharding."
---

# Verify: seal and split

## What is verified, in plain words

- **NoCommitAfterSeal**: once the database is sealed, no write commits to it. Harm if broken: a
  caller is told a write succeeded on a database whose data has already been handed to its
  successors, so the write is in neither.
- **EveryAckedWriteInExactlyOneSuccessor**: after a split, every write acknowledged before the seal
  is in exactly one successor: the one whose key range holds the key. Harm if broken: a write is
  lost (in no successor) or duplicated (two successors both own the key).

## Run it

```sh
node tools/formal-verification/src/verify.ts seal-split   # about 5 seconds
vp run verify                                             # every formal check
```

Expected: five `✓` lines. The code as written passes; `writerNotFenced` fails `NoCommitAfterSeal`;
`sealWithoutLock`, `checkpointBeforeSeal`, and `overlappingRanges` fail
`EveryAckedWriteInExactlyOneSuccessor`.

## Read a result

Read results with the pstack `/statechart` skill (`.agents/skills/statechart/SKILL.md`, opened by
path), on a scratch copy of `tools/formal-verification/specs/SealSplit.tla` and its config, with
`JAVA` and `TLA2TOOLS` pointing at `.tlc/`. Replay a reported path against the code, one action per
step:

| Action | Code |
| --- | --- |
| `Commit(k)` | `DbWriter.write` under `commitLock`: `ensureReady` refuses a sealed database, then `MemTable.commit` |
| `Ack(w)` | the write's receipt, after the lock is released |
| `Flush` | `DbWriter.flushNow` from the alarm or a flush request |
| `Seal` | `DbWriter.seal` under `commitLock`: `flushNow`, then `sealManifest` by CAS (`writerEpoch + 1`) |
| `Checkpoint` | `DatabaseLifecycle.createCheckpoint` after the seal (flush, then `addCheckpoint` pinning the newest manifest) |
| `Fork(s)` | `DatabaseLifecycle.forkRange` → `forkManifest(pinned, range)` |

If a step cannot happen in the code, fix the model. If every step can, it is a bug: report it in
the code's words.

## Coverage record

Files: `packages/lsm-cloudflare/src/db-writer.ts` (`write`, `seal`, `flushNow`),
`database-lifecycle.ts` (`seal`, `createCheckpoint`, `forkRange`), `packages/lsm-core/src/
manifest.ts` (`sealManifest`, `forkManifest`, `addCheckpoint`). Code-level tests that bind the steps
to the code: `database-lifecycle.test.ts` ("seals, forks a split, rewrites and releases the forks'
holds, then removes the source": the late write fails sealed, each key is in exactly one fork, an
out-of-range write fails), `db-writer.test.ts` (seal flushes and fences), `manifest.test.ts` (fork
views and ranges), `writer.workerd.ts` (the same split through the public API).

## Bounds and assumptions

- `SealSplit.cfg`: 3 keys (one left, two right), 4 writes (~15k states). Every interleaving of
  writes, acknowledgements, flushes, the seal, the checkpoint, and both forks.
- One writer instance (the Durable Object's guarantee); a stale instance's late commits are refused
  by the epoch fence (`verify-manifest-cas-fencing`). Durable Object SQLite commits are durable
  before the acknowledgement (`verify-no-acked-write-lost`).
- The split order (seal, one checkpoint, forks) is what `ShardSplit` (Phase 8) must follow; the
  model checks that order, not the sharding directory's routing.

## Maintenance notes

- D110–D114 drift check (formal2), against the integration head:
  - **D95.** A seal of a sealed database naming other successors is refused. This model has one
    set of successors, so no step changed. `ShardSplit.tla` checks the refusal (`HoldsMatchSeal`,
    variant `resealOthers`).
  - **D101.** A request that waited out an abandoned split finishes it. That is after the forks,
    outside this model; `ShardSplit.tla` checks it (`WaitingWritesLand`, variant `noResume`).

- D85: the model's `Seal` is one step that also updates the writer, so it cannot express a seal
  whose manifest commit landed while the writer never saw the answer (its caller hung up, or the
  read after the create failed). The code then kept believing it was open and acknowledged writes
  the seal's epoch had already fenced: an acknowledged write in no successor. Now a seal that does
  not return forgets the writer's phase while still holding the commit lock, and the next request
  starts again from the manifest. `db-writer.test.ts` covers the seal interrupted after its
  commit, the writer evicted after it, and the seal interrupted before it (the database stays
  open and writable). No model step changed. Reran: every check passes, and every broken variant
  fails.

- D77: a successor's hold on the sealed database is now released by the successor's own
  collection, `gcMinAge` after its rewrite, instead of by its compactor's next idle poll. The
  holds still come from the seal's commit and the forks still read from them; only how long the
  sealed database stays changes. No step of this model changed (it ends at the forks); the release
  rule is in `verify-gc-safety` (`GcSafetyFork.cfg`). Reran; all broken variants still fail.

- D73: `DbWriter.flushThen`, which the seal runs under the commit lock, no longer waits for the
  compactor it wakes (`BackgroundWork`). The wake changes no state the model has; the seal's
  commit and the order of seal, holds, and forks are unchanged. A flush now writes only the
  versions a read can reach (D70): a fork reads its source at the seal's manifest, at a snapshot
  at or after every row in it. No model step changed; reran, all broken variants still fail.

- D57 (v1 workstream 3): a split's seal (`DbWriter.seal` with `holdForSuccessors`) commits the final
  flush, the seal, and each successor's hold in one manifest CAS under the commit lock (`flushThen`
  with the seal edit; a separate seal CAS only when nothing is left to flush). The model's `Seal`
  now takes the checkpoint (the holds) in the same step; `Checkpoint` exists only for the
  `checkpointBeforeSeal` broken variant. The forks run concurrently from their own holds, which
  the model already allows (each `Fork(s)` interleaves freely). The split publishes without a
  separate checkpoint or its deletion. Reran; every broken variant still fails.

- Checked against `c4ea735` (Phases 9–10). `DbWriter.write` now commits through `commitBatch`
  (the same commit lock and SQLite commit; the ack after it); `commitTransaction` runs its conflict
  check inside that lock before the same commit, so it is one more kind of `Commit`/`Write`.
  `MemTable.clear` runs only on removal. The change feed only reads.

## Keeping it in step with the code

Sync commit: the D110–D114 drift check (formal2); before it, the D84/D85 commit (`fix(writer)`: a seal that does not return forgets the writer's phase); before it the D77 commit (a fork releases its hold from its own collection). When a covered function changes after it, update the model
so each action is still one awaited effect in the code, rerun, and show each invariant still fails on
a broken variant before moving the sync commit. For drift that skipped this skill, use
`.agents/skills/maintain-formal-verification-skill/SKILL.md`.
