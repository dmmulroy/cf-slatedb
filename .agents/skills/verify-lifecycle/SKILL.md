---
name: verify-lifecycle
description: "Model-check shaledb's database lifecycle across three databases (TLA+, TLC): a source, a fork, and a fork of the fork; each fork's hold, rewrite and release (D77); GC on every database; deleteCheckpoint of a hold (D91); removal interrupted at any step and resumed, keeping its Removed head (D91); a stale committer after a removal; recreation of a removed id with readers that cached the old incarnation (D90). Use after any change to forkManifest/forkRange, releasableExternalRefs, deleteCheckpoint, DatabaseLifecycle.remove or create, or GC's protection of pinned manifests."
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
- **HoldsReleased** (liveness, own config). Every fork's hold on its owner is eventually released.
- **PrefixEmptied** (liveness, own config). A removed database's prefix is eventually empty.

## Run it

```sh
nice -n 15 node tools/formal-verification/src/verify.ts lifecycle
```

Expect eight `✓` lines: the safety config passes, each of five variants fails, and the two
liveness configs pass. Each run takes under 30 s.

## Action table

| Action | Code |
| --- | --- |
| `Rewrite(c)` | the fork's compaction rewriting its owner's SSTs into its own (the replaced manifest stays readable for `gcMinAge`) |
| `Expire(d)` | `gcMinAge` passing: replaced manifests may be collected, and no reader holds them (D67) |
| `Release(c)` | `Compactor`'s collection: `releasableExternalRefs` over the newest, the retained, and the pinned manifests, then `removeCheckpoint` on the owner (D77) |
| `Collect(d)` | `GarbageCollector.collect`: an owned SST no kept manifest (newest, recent, pinned by a hold) references |
| `DeleteHold(d)` | `deleteCheckpoint` of a fork's hold (refused while the fork exists, D91) |
| `RemoveStep(d)` | `DatabaseLifecycle.remove`, step by step: `markRemoved` (refused while holds exist), destroy the writer's and compactor's state, release this fork's hold, delete the objects keeping the Removed head (D91), `markRemovalCleared` |
| `StaleCommit(d)` | a flush, compaction, collection, checkpoint, or seal committed from a view loaded before the removal |
| `TombstoneAges(d)` | `gcMinAge` after the removal; readers' caches of the old incarnation have ended |
| `Recreate(d)` | `DatabaseLifecycle.create` of a removed id (refused while the tombstone is young, D90) |
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

## Broken variants

| Variant | Re-introduces | Fails |
| --- | --- | --- |
| `releaseNewestOnly` | a fork releasing its hold once its newest manifest stops reading the owner (D77) | `NoReaderNeedsDeleted` |
| `releaseIgnoresPins` | a fork releasing its hold while its own fork's pin still reads the owner (D77, the chain) | `NoReaderNeedsDeleted` |
| `deleteHold` | deleting a fork's hold (D91) | `NoReaderNeedsDeleted` |
| `deleteHead` | removal deleting its Removed head, so a stale committer reopens the database (D91) | `RemovedNeverReopens` |
| `recreateAtOnce` | recreating a removed id at once (D90) | `NoOldIncarnationServed` |

The liveness properties have no broken variant: no defect found to date was a hold or a removal
that never finished.

## Keeping it in step with the code

Sync commit: the D114 commit.
