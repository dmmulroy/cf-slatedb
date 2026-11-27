---
name: verify-shard-split
description: "Model-check a shaledb shard split end to end (TLA+, TLC): the directory's pending split, the seal with its holds, both forks in either order, the publish, a crash after any step and its resumption by a waiting request (D101), a router writing through a stale map, a Flushed read through a stale map (D89), a seal naming other successors (D95), and a change-feed consumer following the split (D80). Use after any change to shard-split.ts, the router's routing wait or map refresh, DbWriter.seal's successor rule, or the change feed's fork point."
---

# Verify: a shard split end to end

## What is verified, in plain words

- **EveryShardWriteInItsSuccessor.** Once the split is published, every write the shard
  acknowledged is in the successor that owns its key. (`SealSplit` checks the seal and fork
  steps in detail.)
- **HoldsMatchSeal.** Every hold on the shard belongs to a successor its seal names (D95).
- **ConsumerMissesNothing.** A change-feed consumer that moved to the successors was given every
  write the shard acknowledged (D80's procedure).
- **NoStaleBeyondBound.** A `Flushed` read through a map older than the bound never returns an
  older value than its successor holds (D89).
- **WaitingWritesLand** (liveness, own config). A write that waits on the split eventually lands,
  even when the split's process died after the seal (D101).

## Run it

```sh
nice -n 15 node tools/formal-verification/src/verify.ts shard-split
```

Expect six `✓` lines, each in a few seconds.

## Action table

| Action | Code |
| --- | --- |
| `Begin` | `ShardDirectory.beginSplit` (the pending split, with its two database ids) |
| `Seal` | `DatabaseLifecycle.seal(shard, [left, right], true)` → `DbWriter.seal`: flush and seal in one commit, taking both holds; a repeat finds it done |
| `Fork(f)` | `forkRange` from the half's hold (the two run concurrently) |
| `Publish` | `ShardDirectory.completeSplit` |
| `Crash` | the split's request ends or its Worker is evicted after a step |
| `ClientWrite`, `ClientRefresh` | `ShardRouter.routed`: a write by the cached map. A sealed shard (`DatabaseSealed`) or an absent fork (`DatabaseNotFound`) waits on `awaitMap`; a newer map retries |
| `ClientResume` | `withAbandonedSplitFinished` / `ShardSplit.resumePending` after the routing wait (D101) |
| `ResealOthers` | a seal of the sealed shard naming other successors (refused, D95) |
| `ReaderTick`, `ReadFlushed` | the router's map aging past `SHARD_MAP_MAX_AGE_FOR_UNPROBED_READS_MS`, and a `Flushed` get that refreshes it first (D89) |
| `ConsumeSource`, `SwitchToSuccessors`, `ConsumeSuccessors` | a change-feed consumer: the sealed source to an empty page after the seal, then each successor from the source's `lastSeq` (D80) |

## Bounds and assumptions

- **Bounds:** two keys (one per half), 4 writes, one crash; 24,118 states. An exploration run at 6
  writes and 2 crashes (228,918 states) also passes.
- **Simplifications:**
  - a `Flushed` read sees every acknowledged write: the staleness modeled is the router's map,
    not the manifest refresh;
  - the feed consumer reads whole databases.
- **Not modeled:**
  - a second split of a successor before its rewrite (the fork-of-a-fork hold chain is in
    `Lifecycle`);
  - routers two generations behind (a second split);
  - a split's step running twice concurrently (tested in `shard-split-faults.test.ts`).

## Broken variants

| Variant | Re-introduces | Fails |
| --- | --- | --- |
| `noFlushedRefresh` | before D89 | `NoStaleBeyondBound` |
| `resealOthers` | before D95 | `HoldsMatchSeal` |
| `feedGap` | the consumer leaving the source at the seal, with a fork's feed silent below its fork point (before D80) | `ConsumerMissesNothing` |
| `noResume` | before D101 | `WaitingWritesLand` |

## Keeping it in step with the code

Sync commit: the D112 commit. Rerun after any change to a covered function, and show each
property still fails on its variant.
