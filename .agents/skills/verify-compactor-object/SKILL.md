---
name: verify-compactor-object
description: "Model-check shaledb's compactor Durable Object at await granularity (TLA+, TLC): one fence-in per start, a stored epoch that never moves back, every job reported truly, every recorded job eventually leaving the record, and L0 at its limit eventually compacted, under two concurrent wakes, the alarm's poll, Workflow jobs whose commit may run twice, a one-slot lease, GC, and eviction. Use after any change to compactor.ts (ensureState, poll, runAlarm, updateState, progress handling), compaction-launcher progress, the commit's job record, or GC's running-job cap."
---

# Verify: the compactor object

## What is verified, in plain words

- **EpochNeverRegresses** (action property). The compactor's stored epoch never moves back. If it
  did, the poll could write an older epoch over the one a second fence-in stored, which left the
  compactor fenced by itself for good (D78).
- **OutcomeTrue.** A job whose commit changed the manifest reports `Committed`, even when its
  commit step ran twice (D103).
- **LeaseCapacity.** At most one compaction holds the budget's slot.
- **L0Drains** (liveness, own config). L0 at its write limit is eventually compacted. If not,
  writes stall for good (D78's live symptom, and D96's job that never commits).
- **RunningJobsLeave** (liveness, own config). A job recorded as running eventually leaves the
  record. If not, its lease and sources stay reserved for ever (D94).

## Run it

```sh
nice -n 15 node tools/formal-verification/src/verify.ts compactor-object
```

Expect eight `✓` lines: three configs pass, and each broken variant fails with its property.
`CompactorObject.cfg` checks the safety properties. `CompactorObjectL0.cfg` and
`CompactorObjectJobs.cfg` each hold one temporal property, because TLC does not name a violated
temporal property. `verify.ts` reads the name from a config with exactly one.

## Action table

| Action | Code |
| --- | --- |
| `WakeBegin`, `WakeRead`, `WakeFence`, `WakeWrite` | `Compactor.ensureState` under `startLock`: `cell.get`, then `ManifestStore.update(fenceCompactor)` (an R2 await), then `updateState` with the new epoch, releasing the old epoch's leases. Entered from `wake`, `requestCollection`, `dropSst` |
| `AlarmFire` | `runAlarm`: a fenced state fences in again first (D78); otherwise the poll reads the cell under `planLock` |
| `PollLoad` | `poll`: `loadLatest`; a newer manifest epoch marks the state fenced (epoch-guarded `updateExisting`); otherwise each running job's `launcher.progress`. A job is dropped when its read says finished, when its instance is missing (D103), or when no read has seen it running for `compactionLeaseMs` (D94). Then `planCompactionsAround`, `budget.acquire`, `launcher.start` |
| `PollWrite` | `poll`'s final `updateExisting`: running jobs written onto the current state only if its epoch is still the poll's (D78); otherwise the new job's lease is released |
| `Upload`, `Commit`, `RetryCommit` | `CompactionWorkflow` steps: an output written, then `commitCompaction` (epoch, exact inputs, outputs above `gcSstBoundary`), whose retry is recognized by `recentCompactionJobIds` (D103) |
| `Vanish`, `Unseen` | A finished job's instance purged before it was read; `compactionLeaseMs` passing with no read seeing it |
| `Age`, `GcDelete` | `gcMinAge` passing; `GarbageCollector.collect` deleting an unreferenced output, capped at the oldest running job's start (D96) |
| `LeaseExpire` | `CompactionBudget` lease expiry |
| `Evict` | Durable Object eviction: pcs and locks lost; the cell, the alarm, the manifest, and Workflow instances kept; an unfinished alarm handler runs again |
| `Flush` | The writer's flush into L0, up to `l0MaxSsts` |

## Bounds and assumptions

- **Bounds:** L0 limit 3, 4 flushes, epochs up to 2, one eviction, two job slots that are reused,
  two concurrent wakes. About 270,000 distinct states per config; under two minutes each, niced,
  with two workers.
- **Merged awaits.** `PollLoad` merges the poll's awaits between the load and the launch. Only the
  final write's view of the cell matters for D78.
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
- **Not modeled:**
  - `dropSst`'s refusal rules;
  - corrupted SSTs (D53);
  - the budget's fair shares across groups;
  - two collections at once (D83; `GcSafety` and `Lifecycle` cover GC safety).

## Broken variants

| Variant | Re-introduces | Fails |
| --- | --- | --- |
| `twoFenceIns` | `ensureState` without `startLock` (D78, first half) | `EpochNeverRegresses` |
| `beforeD78` | no lock, the poll's stale write, and a fenced compactor that never fences in again | `L0Drains` |
| `noJobRecord` | a retried commit not recognized (D103) | `OutcomeTrue` |
| `gcIgnoresRunning` | GC deleting a running job's outputs (D96) | `L0Drains` |
| `noUnseenBound` | an unreadable job counted as running for ever (D94) | `RunningJobsLeave` |

## Maintenance notes

- D110: written. Building it showed three things the code relies on that the model must say.
  Without them, TLC finds lassos that are not defects:
  - the runtime retries an alarm handler that an eviction cut off;
  - a lease expires on time whatever the polls do;
  - `gcMinAge` is far longer than the gap between a job's launch and its record.

## Keeping it in step with the code

Sync commit: the D110 commit. When a covered function changes after it, update the model so each
action is still one await in the code, rerun, and show each property still fails on its variant.
