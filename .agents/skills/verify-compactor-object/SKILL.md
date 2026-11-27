---
name: verify-compactor-object
description: "Model-check shaledb's compactor Durable Object at await granularity (TLA+, TLC): one fence-in per start, a stored epoch that never moves back, every job reported truly, every recorded job eventually leaving the record, and L0 at its limit eventually compacted, a dropped SST's rows never coming back, and every recorded job staying recorded, under two concurrent wakes, the alarm's poll, a collection beside it, an operator's dropSst, Workflow jobs whose commit may run twice, a one-slot lease, GC, and eviction. Each of the compactor's locks is a broken variant. Use after any change to compactor.ts (ensureState, poll, runAlarm, runCollection, dropSst, updateState, its locks, progress handling), compaction-launcher start or progress, the commit's job record, or GC's running-job cap."
---

# Verify: the compactor object

## What is verified, in plain words

- **EpochNeverRegresses** (action property). The compactor's stored epoch never moves back. If it
  did, the poll could write an older epoch over the one a second fence-in stored, which left the
  compactor fenced by itself for good (D78).
- **OutcomeTrue.** A job whose commit changed the manifest reports `Committed`, even when its
  commit step ran twice (D103).
- **LeaseCapacity.** At most one compaction holds the budget's slot.
- **DroppedStaysDropped.** An SST an operator dropped (D62) never comes back through a compaction
  that read it. `commitCompaction` checks a sorted-run input by its run id, which a drop keeps, so
  the drop must see every job that may still commit: `planLock`, and the job recorded before it is
  launched (D116).
- **RunningStaysRecorded.** No state write puts back running jobs that a poll replaced
  (`stateLock`, D116). D96's cap and the lease renewal both read the recorded jobs.
- **L0Drains** (liveness, own config). L0 at its write limit is eventually compacted. If not,
  writes stall for good (D78's live symptom, and D96's job that never commits).
- **RunningJobsLeave** (liveness, own config). A job recorded as running eventually leaves the
  record. If not, its lease and sources stay reserved for ever (D94).

## Run it

```sh
nice -n 15 node tools/formal-verification/src/verify.ts compactor-object
```

Expect twelve `✓` lines: four configs pass, and each broken variant fails with its property.

- `CompactorObject.cfg` checks the safety properties with the collection beside the poll
  (`Ops = {"collect"}`).
- `CompactorObjectDrop.cfg` checks them with `dropSst` instead (`Ops = {"drop"}`).
- `CompactorObjectL0.cfg` and `CompactorObjectJobs.cfg` each hold one temporal property, with
  neither process, because TLC does not name a violated temporal property. `verify.ts` reads the
  name from a config with exactly one.

## Deeper search

Not part of `verify.ts`. Run it after a change to the poll, the locks, or `dropSst`, alone, for up
to 20 minutes:

```sh
nice -n 15 java -XX:+UseParallelGC -cp .tlc/downloads/tla2tools.jar tlc2.TLC -workers 2 \
  -config CompactorObjectDeep.cfg CompactorObject.tla   # from tools/formal-verification/specs
```

`CompactorObjectDeep.cfg` runs the collection and `dropSst` together, with two evictions, three
epochs, and five flushes. It does not finish in 20 minutes; add `-simulate -depth 80` to sample
long behaviors instead.

## Action table

| Action | Code |
| --- | --- |
| `WakeBegin`, `WakeRead`, `WakeFence`, `WakeWrite` | `Compactor.ensureState` under `startLock`: `cell.get`, then `ManifestStore.update(fenceCompactor)` (an R2 await), then `updateState` with the new epoch, releasing the old epoch's leases. Entered from `wake`, `requestCollection`, `dropSst` |
| `AlarmFire` | `runAlarm`: a fenced state fences in again first (D78); otherwise the poll reads the cell under `planLock` |
| `PollLoad` | `poll`: `loadLatest`; a newer manifest epoch marks the state fenced (epoch-guarded `updateExisting`); otherwise each running job's `launcher.progress`. A job is dropped when its read says finished, when its instance is missing or was never launched (D103), or when no read has seen it running for `compactionLeaseMs` (D94). Then `planCompactionsAround`, `budget.acquire`, and the job's record: an epoch-guarded `updateExisting` before the launch (D116) |
| `PollLaunch` | `launcher.start` of the recorded job. A start that failed keeps the job recorded (D116) |
| `PollWrite` | `poll`'s final `updateExisting`: running jobs written onto the current state only if its epoch is still the poll's (D78); otherwise the new job's lease is released |
| `Upload`, `Commit`, `RetryCommit` | `CompactionWorkflow` steps: an output written, then `commitCompaction` (epoch, exact inputs, outputs above `gcSstBoundary`), whose retry is recognized by `recentCompactionJobIds` (D103) |
| `Vanish`, `Unseen` | A finished job's instance purged before it was read; `compactionLeaseMs` passing with no read seeing it |
| `Age`, `GcDelete` | `gcMinAge` passing; `GarbageCollector.collect` deleting an unreferenced output, capped at the oldest running job's start (D96), as the collection read it |
| `CollStart`, `CollEnd`, `CollWrite`, `CollSet` | `runCollection` under `collectionLock` (the alarm's, or an operator's `collect`): `cell.get` for D96's cap, `collector.collect`, then `updateExisting(collectionDueAt)`. Under `stateLock` its `cell.get` and `cell.set` are one step; `noStateLock` splits them |
| `DropCheck`, `DropCommit` | `dropSst` under `planLock`: the state's running jobs (refused while one reads the SST's run), then `ManifestStore.update(dropSstFromManifest)` |
| `LeaseExpire` | `CompactionBudget` lease expiry |
| `Evict` | Durable Object eviction: pcs and locks lost; the cell, the alarm, the manifest, and Workflow instances kept; an unfinished alarm handler runs again |
| `Flush` | The writer's flush into L0, up to `l0MaxSsts` |

## Bounds and assumptions

- **Bounds:** L0 limit 3, 4 flushes, epochs up to 2, one eviction, two job slots that are reused,
  two concurrent wakes. `CompactorObject.cfg` has 4,753,760 distinct states (88 s niced with two
  workers), `CompactorObjectDrop.cfg` 881,804 (17 s), and the liveness configs 132,024 each
  (23 s and 52 s). Both processes together exceed 31M states.
- **Merged awaits.** `PollLoad` merges the poll's awaits between the load and the job's record
  (progress reads, `budget.acquire`). Another process can run at each of them, but none of them
  writes what another process reads, so the merge loses no interleaving that matters to these
  properties. The record, the launch, and the final write are separate steps.
- **Locks and Durable Object input gates.** A Durable Object delivers no event while a storage
  call runs, and Effect resumes a fiber in the promise callback. So `updateState`'s `cell.get` and
  `cell.set` (two storage calls with only pure code between) are already atomic in production,
  unless the fiber's 2048-operation budget runs out between them and Effect yields through
  `setImmediate`. `stateLock` is the guarantee. `planLock` and `collectionLock` span R2 and Workflows
  requests, which do let other events in.
- **Assumptions behind the fairness.**
  - The runtime fires a set alarm.
  - Workflows runs a job's steps.
  - Time passes: leases expire, and `compactionLeaseMs` passes.
  - The writer's stalled-write wake comes back while L0 can be compacted.
  - A live Workflow instance whose reads fail for ever is an outage outside the model; only
    finished instances vanish.
- **Unreachable variants.** The stale write and the terminal fence are unreachable once
  `startLock` is held, because only this object fences in. They are checked together with the
  missing lock (`beforeD78`).
- **`collectionLock`.** Two collections at once break no safety property: `GcSafety`'s
  `GcSafetyTwoCollectors.cfg` checks two interleaved collections (D83), and the collection's
  due-time write keeps a time it did not see (D78's test). The lock saves duplicate work; it has
  no broken variant here.
- **Not modeled:**
  - `dropSst`'s `NotReportedCorrupted` refusal and corrupted SSTs (D53): a corrupted SST fails the
    job that reads it, so it cannot bring rows back; the drop of a healthy SST needs `force`.
    `compactor.test.ts` covers the refusals;
  - the budget's fair shares across groups: one database's compactor cannot express them. The
    budget's ledger is checked by `compaction-budget-ledger` property tests.

## Broken variants

| Variant | Re-introduces | Fails |
| --- | --- | --- |
| `twoFenceIns` | `ensureState` without `startLock` (D78, first half) | `EpochNeverRegresses` |
| `beforeD78` | no lock, the poll's stale write, and a fenced compactor that never fences in again | `L0Drains` |
| `noJobRecord` | a retried commit not recognized (D103) | `OutcomeTrue` |
| `gcIgnoresRunning` | GC deleting a running job's outputs (D96) | `L0Drains` |
| `noUnseenBound` | an unreadable job counted as running for ever (D94) | `RunningJobsLeave` |
| `noStateLock` | `updateState` without `stateLock` | `RunningStaysRecorded` |
| `noPlanLock` | `dropSst` without `planLock` (D62) | `DroppedStaysDropped` |
| `launchBeforeRecord` | the code before D116: the job launched before it is recorded, so an eviction between leaves it running unrecorded | `DroppedStaysDropped` |

Each lock's unit test: `compactor.test.ts` "a collection's state write never puts back the running
jobs a poll replaced meanwhile (D116, stateLock)", "an SST drop waits for a poll that is starting a
compaction of it, and is refused (D116, planLock)", and the D78 tests for `startLock`.

## Maintenance notes

- D116: the collection, `dropSst`, the job record before the launch, `Ops`, three variants, and
  two invariants. The model found an unplanted defect with every lock held. An eviction between
  `launcher.start` and the poll's state write left a running job unrecorded, and a drop of an SST
  of the run it read then committed, and the job brought the rows back. Fixed in `compactor.ts`:
  the job is recorded before it is launched, and a failed start keeps it recorded. Test: "a job
  launched by a poll that was evicted before recording it still blocks a drop of its sources".

- D110: written. Building it showed three things the code relies on that the model must say.
  Without them, TLC finds lassos that are not defects:
  - the runtime retries an alarm handler that an eviction cut off;
  - a lease expires on time whatever the polls do;
  - `gcMinAge` is far longer than the gap between a job's launch and its record.

## Keeping it in step with the code

Sync commit: the D116 commit. When a covered function changes after it, update the model so each
action is still one await in the code, rerun, and show each property still fails on its variant.
