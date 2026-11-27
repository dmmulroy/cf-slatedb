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
- **PublishedIsSealed.** The published map names the successors the shard's seal named, which the
  forks created. A second request to split the same shard, beginning at the same time, cannot
  replace the pending split's successor ids (the directory's lock, D116).
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

Expect seven `✓` lines, each in a few seconds.

## Deeper search

Not part of `verify.ts`. Run it after a change to the directory or the split's steps, alone, for up
to 20 minutes:

```sh
nice -n 15 java -XX:+UseParallelGC -cp .tlc/downloads/tla2tools.jar tlc2.TLC -workers 2 \
  -config ShardSplitDeep.cfg ShardSplit.tla   # from tools/formal-verification/specs
```

`ShardSplitDeep.cfg` has six writes and two crashes of the split's process.

## Action table

| Action | Code |
| --- | --- |
| `Begin` | `ShardDirectory.beginSplit` (the pending split, with its two database ids) under the directory's lock: `editMap`'s `cell.get`, `beginShardSplit`, `cell.set` in one step. A pending split of the same shard and key is returned as it is |
| `BeginRead`, `BeginWrite` | the same without the lock (`noDirectoryLock`): the read and the write are two awaits |
| `Begin2`, `Begin2Read`, `Begin2Write` | a second request to split the same shard at the same key, with its own fresh ids; its request then ends |
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

- **Bounds:** two keys (one per half), 4 writes, one crash, one second split request; 71,900
  states, 2 s.
- **The directory's lock and input gates.** `editMap`'s read and write are two Durable Object
  storage calls with only pure code between. Input gates already make them atomic unless the
  fiber's 2048-operation budget runs out between them; the lock is the guarantee.
- **Simplifications:**
  - a `Flushed` read sees every acknowledged write: the staleness modeled is the router's map,
    not the manifest refresh;
  - the feed consumer reads whole databases.
- **Not modeled:**
  - a second split of a successor before its rewrite (the fork-of-a-fork hold chain is in
    `Lifecycle`);
  - routers two generations behind (a second split). `shard-split-faults.test.ts` tests a
    two-generation stale router; the model's directory has two versions;
  - the second request's own seal and forks: under the lock it adopts the pending split, so it
    repeats the first's steps, which are idempotent.

## Broken variants

| Variant | Re-introduces | Fails |
| --- | --- | --- |
| `noFlushedRefresh` | before D89 | `NoStaleBeyondBound` |
| `resealOthers` | before D95 | `HoldsMatchSeal` |
| `feedGap` | the consumer leaving the source at the seal, with a fork's feed silent below its fork point (before D80) | `ConsumerMissesNothing` |
| `noResume` | before D101 | `WaitingWritesLand` |
| `noDirectoryLock` | the directory's `editMap` without its lock | `PublishedIsSealed` |

`noDirectoryLock`'s unit test: `shard-directory-lock.test.ts` (the lock with two permits fails it).

## Keeping it in step with the code

Sync commit: the D116 commit. Rerun after any change to a covered function, and show each
property still fails on its variant.
