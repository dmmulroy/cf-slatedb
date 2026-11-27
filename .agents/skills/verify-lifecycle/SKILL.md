---
name: verify-lifecycle
description: "Model-check shaledb's database lifecycle across three databases (TLA+, TLC): a source, a fork, and a fork of the fork; each fork's hold, rewrite and release (D77); GC on every database; deleteCheckpoint of a hold (D91); removal interrupted at any step and resumed, keeping its Removed head (D91); a stale committer after a removal; recreation of a removed id with readers that cached the old incarnation (D90); a fork being created while an operator deletes its hold (D116); a second removal that stalls past a recreation; forkRange called again after the fork moved on and its GC collected the fork's first manifest (D136). Use after any change to forkManifest/forkRange, releasableExternalRefs, deleteCheckpoint, DatabaseLifecycle.remove or create, acquireForkHold, forkRange's DatabaseAlreadyExists handling, removeCheckpointFor, or GC's protection of pinned manifests."
---

# Verify: the lifecycle across databases

## What is verified, in plain words

- **NoReaderNeedsDeleted.** No open database's newest or recently replaced manifest, which a
  reader may still hold, needs a deleted SST. That includes a fork of a fork, reading the original
  owner's files through its hold on its own source.
- **RemovedNeverReopens.** A removed database never answers as open again, even when a committer
  that loaded a view before the removal commits after its deletes (D91).
- **NoOldIncarnationServed.** A recreated database never serves a manifest its readers cached
  from the removed incarnation (D90).
- **NewIncarnationIntact** (`LifecycleRemovers.cfg`). A removal of the old incarnation, however
  late, never deletes the recreated database's first manifest: it deletes only objects written no
  later than its tombstone.
- **HoldsReleased** (liveness, own config). Every fork's hold on its owner is eventually released.
- **PrefixEmptied** (liveness, own config). A removed database's prefix is eventually empty.

## Run it

```sh
nice -n 15 node tools/formal-verification/src/verify.ts lifecycle
```

Expect seventeen `✓` lines: six configs pass, and each variant fails with its property. The
safety config takes about 40 s, niced with two workers; the others under a minute.

- `Lifecycle.cfg` (`ForkRace = TRUE`): the fork of the fork is created during the behavior, its
  hold and its first manifest two steps, racing an operator's `deleteCheckpoint`.
- `LifecycleRemovers.cfg` (`SecondRemover = TRUE`): a second removal of the fork of the fork, and
  `create` split into its clear and its first manifest.
- `LifecycleServed.cfg`: `NoOldIncarnationServed` alone, for `recreateAtOnce`.
- `LifecycleLiveness.cfg`, `LifecyclePrefix.cfg`: one temporal property each, with the fork of the
  fork created from the start.
- `LifecycleRetry.cfg`, `LifecycleRetryLiveness.cfg` (`ForkRetry = TRUE`, D136): after
  `forkRange` of the fork of the fork returned, it is called again. Before the retry the fork may
  flush (replacing its first manifest), its GC may delete that first manifest, and it may release
  its hold. Holds have ids (`HoldIds = Forks × {1,2}`), so the hold a call created is told apart
  from one it found.

After the model checks, run `node tools/formal-verification/src/error-branches.ts` (D137). Every
error branch in the functions this model covers needs a row in `docs/coverage-matrix.md`'s
"Error branches in model-covered code" that names one of the actions below, or says why no model
covers it. A branch with no action is a gap in this model: add the action, or record the reason.

## Deeper search

Not part of `verify.ts`. Run each alone, for up to 20 minutes:

```sh
# from tools/formal-verification/specs
nice -n 15 java -XX:+UseParallelGC -cp ../../../.tlc/downloads/tla2tools.jar tlc2.TLC -workers 2 \
  -config LifecycleDeep.cfg Lifecycle.tla          # the fork race and the second remover together
# and LifecycleDeepLiveness.cfg, LifecycleDeepPrefix.cfg: the liveness properties with the fork race
```

## Action table

| Action | Code |
| --- | --- |
| `Rewrite(c)` | the fork's compaction rewriting its owner's SSTs into its own (the replaced manifest stays readable for `gcMinAge`) |
| `Expire(d)` | `gcMinAge` passing: replaced manifests may be collected, and no reader holds them (D67) |
| `Release(c)` | `Compactor`'s collection: `releasableExternalRefs` over the newest, the retained, and the pinned manifests, then `removeCheckpoint` on the owner (D77) |
| `Collect(d)` | `GarbageCollector.collect`: an owned SST no kept manifest (newest, recent, pinned by a hold) references |
| `DeleteHold(d)` | `deleteCheckpoint` of a fork's hold (refused while the fork exists, D91) |
| `ForkHold`, `ForkInit`, `ForkDies` | `forkRange` of the fork of the fork: the hold committed on its source, then (after awaits) its first manifest; or its process dies between them, leaving an orphan hold |
| `DeleteHoldRead`, `DeleteHoldCommit` | `deleteCheckpoint` of that hold: it reads the holder's head (an await), then removes the checkpoint only if the holder was removed, or has no manifest and the hold is `gcMinAge` old (D116). A refused operator retries while the fork does not exist |
| `Remove2` | a second removal of the fork of the fork (a retried `remove`, or the sweep), step by step beside the first, possibly past a recreation; its deletes take objects written no later than its tombstone |
| `RecreateClear`, `Recreate(d)` | `create` of a removed id: `clearRemovedIncarnation` (the old objects, its head among them), then `initialize` of manifest 1 |
| `RemoveStep(d)` | `DatabaseLifecycle.remove`, step by step: `markRemoved` (refused while holds exist), destroy the writer's and compactor's state, release this fork's hold, delete the objects keeping the Removed head (D91), `markRemovalCleared` |
| `StaleCommit(d)` | a flush, compaction, collection, checkpoint, or seal committed from a view loaded before the removal |
| `TombstoneAges(d)` | `gcMinAge` after the removal; readers' caches of the old incarnation have ended |
| `Recreate(d)` | `DatabaseLifecycle.create` of a removed id (refused while the tombstone is young, D90) |
| `ForkFlush`, `CollectFirst` | the fork of the fork committing a newer manifest that still reads its hold; `gcMinAge` later, its GC deleting its first manifest (D136) |
| `RetryAcquire`, `RetryInit` | `forkRange` called again: `acquireForkHold` finds the fork's hold (`Existing`) or commits one (`Created`); then `initialize` fails with `DatabaseAlreadyExistsError`, and the call returns the fork as its own when the fork's newest manifest (`findLatest`) has an external ref with that hold's id, or else undoes only a hold it `Created` (`removeCheckpointFor(UnusedHold)`) |
| `ReaderLoad`, `ReaderServe`, `ReaderForget` | `ManifestCache` loading, serving (a covering hit or within the refresh interval), and dropping a manifest |

## Bounds and assumptions

- **Size.** Three databases, one SST each, one recreation. 132,984 states.
- **Abstraction.** Manifests are summarized per database as the SSTs its newest manifest, its
  recently replaced manifests, and each hold's pinned manifest reference. `GcSafety` checks GC's
  manifest-level steps in detail.
- **Fairness.** Compactions, collections, and time run, and a stopped removal is retried (by the
  operator or the maintenance sweep).
- **Not modeled:**
  - two collections of one database at once (D83). D60's argument and the collection's boundary
    re-read cover it, with tests;
  - the boundary arithmetic of D90's clearing.

- **D91's assumption, explicit.** No committer's view outlives `gcMinAge`: `StaleCommit` is
  enabled only while the tombstone is younger (`staleViewOutlives` drops it, and a stale commit
  between `create`'s clear and its first manifest revives the old incarnation).
- **Orphan holds.** A hold whose `forkRange` died before the fork's first manifest pins its
  manifest until an operator deletes it, and so blocks its parent's own release. Nothing reaps it
  automatically; `HoldsReleased` assumes the operator does.

## Broken variants

| Variant | Re-introduces | Fails |
| --- | --- | --- |
| `releaseNewestOnly` | a fork releasing its hold once its newest manifest stops reading the owner (D77) | `NoReaderNeedsDeleted` |
| `releaseIgnoresPins` | a fork releasing its hold while its own fork's pin still reads the owner (D77, the chain) | `NoReaderNeedsDeleted` |
| `deleteHold` | deleting a fork's hold (D91) | `NoReaderNeedsDeleted` |
| `deleteHead` | removal deleting its Removed head, so a stale committer reopens the database (D91) | `RemovedNeverReopens` |
| `recreateAtOnce` | recreating a removed id at once (D90) | `RemovedNeverReopens` (`Lifecycle.cfg`), `NoOldIncarnationServed` (`LifecycleServed.cfg`) |
| `beforeD116` | `deleteCheckpoint` deleting a hold whose holder has no manifest yet | `NoReaderNeedsDeleted` |
| `deleteByPrefix` | a removal deleting everything under the prefix, not only what its tombstone covers | `NewIncarnationIntact` |
| `staleViewOutlives` | D91's assumption dropped | `RemovedNeverReopens` |
| `compensateAnyHold` | the code before D136: a retried `forkRange` that finds the fork not its own undoes the hold it found, not only one it created (issue #2) | `NoReaderNeedsDeleted` (`LifecycleRetry.cfg`) |
| `retryTrustsFirstManifest` | a retry that decides the fork is its own from the fork's first manifest, which GC may have deleted | `NoReaderNeedsDeleted` (`LifecycleRetry.cfg`) |

The liveness properties have no broken variant: no defect found to date was a hold or a removal
that never finished.

## Maintenance notes

- D116: the fork of the fork is created during the behavior and races `deleteCheckpoint`. The
  model found an unplanted defect: `deleteCheckpoint` read "holder not found" while `forkRange`
  was between its hold and the fork's first manifest, then deleted the hold, and the fork's source
  collected what the fork reads. Fixed in `database-lifecycle.ts`: a holder with no manifest counts
  as gone only once the hold is `gcMinAge` old. Tests: "deleteCheckpoint between forkRange's hold
  and its first manifest cannot leave the fork without a hold", "a hold whose fork was never
  created is deletable only once it is gcMinAge old".
- D116: removal has no lock. Its steps are ordered and idempotent, and `markRemoved` against a new
  hold is ordered by the manifest CAS. `LifecycleRemovers.cfg` checks a second, stalled remover.

- D136: before it, holds were sets of fork names and `forkRange` had no retry, so a call that
  undid a hold it had not created could not be written down. Issue #2 found it in the code;
  `compensateAnyHold` reproduces it. Test: `database-lifecycle-retry.test.ts`.

## Keeping it in step with the code

Sync commit: the D136 commit.
