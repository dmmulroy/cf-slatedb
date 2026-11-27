---
name: verify-writer-object
description: "Model-check the shaledb writer Durable Object at await granularity: concurrent writes, transaction commits, flushes (request and alarm), seals, and probes interleaved with eviction, interruption at any await, and R2 failing before or after a commit lands. Use after any change to db-writer.ts (ensureReady/startWriter, commitBatch, commitTransaction, flushThen, refreshUnflushedCounters, scheduleFlush, seal, runAlarm, markFenced, any write of `phase`), mem-table.ts markFlushed/trim/stats, or durable-object-alarm.ts."
---

# Verify: the writer Durable Object (WriterObject)

The other writer models (`verify-no-acked-write-lost`, `verify-transactions`, `verify-seal-split`)
make each code step atomic. This one splits every entry point at each point where Effect may run
another fiber inside the object (D109): an R2 call, a lock, or two SQLite statements not in one
`MemTable.consistently`. It is written in PlusCal; the TLA+ translation is in the same file.

## What is verified, in plain words

- **NoAckedWriteLost**: every acknowledged write is in SQLite or in the newest manifest's SSTs.
- **LatestSeesAcked**: a probe and the manifest read after it return every write acknowledged
  before the probe.
- **Serializable**: a transaction commits only when no commit after its `readSeq` wrote its key
  (the check, a trim, and the conflict query interleaved: D84).
- **NoAckAfterSeal**: every acknowledged write is at or below the sealed manifest's `lastL0Seq`.
- **SealNotHalf**: once no seal is running, a sealed manifest never has an open `Ready` writer
  (D85).
- **NeverSelfFenced**: the object never marks itself `Fenced`. One object runs one writer, so a
  fence-out can only come from its own restart; a fenced object refuses every request until it is
  evicted (D115).
- **CountersCover**: while the writer is `Ready`, the unflushed counters hold every unflushed row,
  except inside one commit between its SQLite commit and its counters' update (D74b).
- **AlarmCovers**: with nothing running, an unflushed row has an alarm, whatever was interrupted
  or evicted (D74b, D102, D131). A commit makes sure an alarm exists after its mark and before
  its rows reach SQLite, and a scheduler that sees a commit under way keeps an alarm.
- **AckedFlushed** (liveness, `WriterObjectLive.cfg`): every acknowledged write reaches the
  manifest.
- **AlarmQuiets** (liveness, `WriterObjectAlarmLoop.cfg`): the alarm eventually stops firing: no
  alarm that fires forever without flushing (D74's loop).

## Run it

```sh
node tools/formal-verification/src/verify.ts writer-object   # about 4 minutes with 2 workers
TLC_WORKERS=2 nice -n 15 node tools/formal-verification/src/verify.ts writer-object
```

Expected: sixteen `✓` lines.

| Config | `Broken` | Expected |
| --- | --- | --- |
| `WriterObjectSeal.cfg` | `none` | pass (4,949,012 states, 72 s with 2 workers) |
| `WriterObjectSeal.cfg` | `alarmAfterCommit` (before D131) | `AlarmCovers` |
| `WriterObjectSeal.cfg` | `alarmBeforeMark` | `AlarmCovers` |
| `WriterObjectSeal.cfg` | `schedulerIgnoresCommit` | `AlarmCovers` |
| `WriterObjectSeal.cfg` | `sealKeepsPhase` (before D85) | `SealNotHalf` |
| `WriterObjectSeal.cfg` | `beforeD115` | `NeverSelfFenced` |
| `WriterObjectSeal.cfg` | `markFencedAnyPhase` | `NeverSelfFenced` |
| `WriterObjectSeal.cfg` | `noAlarmRetry` (before D102) | `AlarmCovers` |
| `WriterObjectTxn.cfg` | `none` | pass (2,312,359 states, about 35 s) |
| `WriterObjectTxn.cfg` | `noHorizonReread` (before D84) | `Serializable` |
| `WriterObjectTxn.cfg` | `noGeneration` (before D74b) | `CountersCover` |
| `WriterObjectTxn.cfg` | `interruptedWrite` (before D74b's second half) | `CountersCover` |
| `WriterObjectLive.cfg` | `none` | pass (97,769 states, about 20 s) |
| `WriterObjectLive.cfg` | `noAlarmRetry` | `AckedFlushed` |
| `WriterObjectAlarmLoop.cfg` | `none` | pass |
| `WriterObjectAlarmLoop.cfg` | `noSettle` (D74's loop) | `AlarmQuiets` |

`sealForgetsUntried` (a seal forgets its phase before applying its edit, with D115's `markFenced`
rule) passes `WriterObjectSeal.cfg`; it is not registered. It shows that the `markFenced` rule
alone keeps the writer correct (D115).

TLC reports a liveness failure without the property's name, so each config with a broken
liveness run lists one property; `verify.ts` reports that one.

After the model checks, run `node tools/formal-verification/src/error-branches.ts` (D137). Every
error branch in the functions this model covers needs a row in `docs/coverage-matrix.md`'s
"Error branches in model-covered code" that names one of the actions below, or says why no model
covers it. A branch with no action is a gap in this model: add the action, or record the reason.

## Edit it

The algorithm is PlusCal inside the comment `(* --algorithm WriterObject`. After changing it,
translate in place, then run:

```sh
.tlc/jdk/*/Contents/Home/bin/java -cp .tlc/downloads/tla2tools.jar pcal.trans -nocfg \
  tools/formal-verification/specs/WriterObject.tla
```

Eviction (`Evict`), interruption (`Interrupt`), `FullSpec`, `LiveSpec`, and the properties are
hand-written TLA+ after `\* END TRANSLATION`; the translator keeps them. Delete the
`WriterObject.old` it may leave.

## Read a result

Read results with the pstack `/statechart` skill on a scratch copy. Each state names every
process's label (`pc`); replay it against the code:

| Label | Code (`db-writer.ts` unless named) |
| --- | --- |
| `ErCheck` | `ensureReady`: the phase from memory; not `Ready`: `initLock` taken (`startWriter`) |
| `StFence` | `manifests.update(fenceWriter)` (sealed: unchanged), then `setIdentity`, `seed`, `markFlushed(lastL0Seq)`; may fail before or after the manifest lands |
| `StRefresh`, `StReady` | `refreshUnflushedCounters`; phase `Ready`; `scheduleFlush` unless sealed; `initLock` released |
| `RfScan`, `RfInstall` | `refreshUnflushedCounters`: `Ref.get` + `memTable.stats`; `Ref.modify` installing only if `generation` is unchanged and no commit runs; otherwise again or give up |
| `WLock`, `WReady`, `WCheck` | `commitBatch`: `commitLock`, `writableReady` |
| `THorizon`, `TQuery`, `THorizonAgain` | `commitTransaction`'s check: `retainedFromSeq`, `writtenAfter`, `retainedFromSeq` again (D84) |
| `WAlarmEarly` | only for `alarmBeforeMark`: the alarm made sure of before the mark |
| `WMark`, `WAlarm`, `WSql`, `WEnd` | the uninterruptible region: mark, `ensureAlarmBeforeCommit` (D131), `memTable.commit`, counters' update |
| `WDone` | `commitLock` released, `ensuring(scheduleFlush)`, the acknowledgement |
| `FlLock`, `FlReady`, `FlMeta` | `flushThen`: `flushLock`, `ensureReady` (the `ready` it keeps), `memTable.meta` for the cut; nothing to flush: `settleCountersWithNothingToFlush` |
| `FlUpload` | `ssts.write` (R2) |
| `FlCommit` | `manifests.updateFrom(commitL0Flush, then sealEdit)`; refused fenced: `markFenced` over its own epoch only (D115); refused stale (`FlushedSeqStaleError`, D113): `markFlushed(lastL0Seq)` and back to `FlMeta`; may land and lose its response |
| `FlMark`, `FlTrim`, `FlRefresh`, `FlDone` | `markFlushed`, `trim`, the view install, `refreshUnflushedCounters`, `flushLock` released |
| `SLock` … `SSet` | `seal`: `commitLock`, `ensureReady`, `flushThen(sealEdit)`, or its own `updateFrom(sealEdit)`, then phase `Ready` sealed |
| `PReady`, `PRead`, `PManifest` | `probe`: `consistently` (snapshot, `flushedSeq`, rows), then the reader's manifest (`flushedSeq <= lastL0Seq <= snapshot`, else probe again) |
| `Cleanup` | finalizers: locks; the seal's `onExit` (only inside its lock, only after its edit: D115); the write's `ensuring(scheduleFlush)` |
| `AIdle` … `AEnd` | `runAlarm`: the alarm fires (cleared); `memTable.meta` (failure: `retryAlarm`); `ensureReady`; flush when the counters are due; `scheduleFlush`; on failure `retryAlarm` unless fenced |
| `Evict` | the isolate goes: memory, locks, and requests lost; SQLite, R2, and the alarm kept; an alarm run cut short runs again |
| `Interrupt(p)` | the caller hangs up: the request's fiber goes to its finalizers, except inside the uninterruptible region |

If a step cannot happen in the code, fix the model. If every step can, it is a bug: report it in
the code's words.

## Bounds and assumptions

- `WriterObjectSeal.cfg`: two requests and the alarm; operations write, flush, seal; 3 requests in
  all (`MaxOps`), 2 sequence numbers, one key; 1 eviction, 1 interruption, 1 R2 failure, 1 seal;
  symmetry over the requests. Four operations take over 30 M states.
- `WriterObjectTxn.cfg`: write and transaction commits, flushes by the alarm only; 3 requests, 3
  sequence numbers, two keys (transactions read and write the first); 1 interruption; no eviction
  and no R2 failure (both are in the seal config).
- `WriterObjectLive.cfg`, `WriterObjectAlarmLoop.cfg`: one request process and the alarm; write
  and flush; 2 requests, 2 sequence numbers; 1 eviction, 1 interruption, 1 R2 failure. Fairness:
  every step of a started request is strongly fair (lock waits), and so is the alarm; starting a
  request is never forced.
- **Time is abstract.** `scheduleFlush` sets the alarm whenever the counters show an unflushed
  row, and the alarm fires when due. The two-schedulers race of D102 (a later time set last) is
  about times and is not here; it is covered by `db-writer.test.ts`'s gated test. A retry alarm
  is the same alarm.
- **Not modeled:** backpressure (`maxUnflushedBytes`, the L0 limit and its manifest reload),
  `probeAt`, `scan`, `changes`, `status`, `destroy`, the changelog export, compaction, GC,
  keeping only reachable versions in an SST (`NoAckedWriteLost` checks it), the manifest view's
  content, and other writer instances (removal's epoch bump). SQLite statements fail only where
  D102 retries them (`runAlarm`'s `meta`).
- **Abstract time and D131.** The alarm a commit sets is at `maxL0Age` from now; the model lets
  it fire at once, so `alarmBeforeMark`'s counterexample (an alarm run between the alarm check
  and the mark) stands for an alarm that was already scheduled and fires then. A SQLite commit
  that fails is not modeled; `db-writer.test.ts` shows its alarm fires once and is not set
  again.
- R2 is CAS-atomic at the head (`verify-manifest-cas-fencing` checks it); a commit may fail
  before landing or land and lose its response.

## Deeper search

Not registered in `formal-checks.ts` (too long for the gate). Run one at a time, niced:

```sh
cd "$(mktemp -d)" && cp <repo>/tools/formal-verification/specs/WriterObject.tla . && \
  cp <repo>/tools/formal-verification/specs/WriterObjectSearch.cfg run.cfg && \
  nice -n 15 <repo>/.tlc/jdk/*/Contents/Home/bin/java -XX:+UseParallelGC -cp \
  <repo>/.tlc/downloads/tla2tools.jar tlc2.TLC -workers 1 -config run.cfg WriterObject.tla
# random traces where the state space is too large: WriterObjectSimulate.cfg (6 requests,
# 4 sequence numbers, every fault twice), with `-simulate -depth 200` added
```

- `WriterObjectSearch.cfg`: all five operations, 3 requests, 3 sequence numbers, 2 keys, every
  fault once, symmetry.
- `WriterObjectSimulate.cfg`: 6 requests, 4 sequence numbers, 2 evictions, 2 interruptions,
  2 R2 failures, no symmetry; simulation only.

Results (2026-11-26, the D131 model, one worker, niced):

- `WriterObjectSearch.cfg`, breadth-first for 30 minutes: no violation; 34,464,583 distinct
  states to depth 72, 1,195,912 still queued (not complete).
- `WriterObjectSimulate.cfg`, `-simulate -depth 200` for 30 minutes: no violation; 227,199,679
  states checked.

## Maintenance notes

- D131: a commit makes sure an alarm exists before its rows reach SQLite
  (`ensureAlarmBeforeCommit`, after the mark), and `scheduleFlush` keeps an alarm when a commit
  is under way and no row is counted. `AlarmCovers` lost its `orphans` exception. `alarmAfterCommit`
  (the order before), `alarmBeforeMark` (the check before the mark), and
  `schedulerIgnoresCommit` (no alarm kept by a scheduler during a commit) fail it.

- D113: `FlCommit` gains the stale refusal: a flush whose `flushed_seq` is below the manifest's
  `lastL0Seq` (an earlier commit landed and lost its response) records it and flushes again.
  Reran: all 13 runs as expected; `WriterObjectSeal.cfg` grew from 3,406,245 to 3,855,683 states.
  The changelog itself is `verify-change-feed`'s.

- D109 (this model) found D115: a flush refused at an older epoch marked the restarted writer
  `Fenced`. Two paths, both through D85's forget: a seal interrupted while it waited for the
  flush lock, and a seal with nothing to flush whose create failed while a flush that took its
  `ready` before read `writer_meta` after a restart. Fixed in `db-writer.ts`; the tests are in
  `db-writer.test.ts` ("DbWriter seal interrupted while a flush is in flight (D115)", "DbWriter
  flush fenced by its own writer's restart (D115)").

## Keeping it in step with the code

Sync commit: the D131 fix (`fix(lsm-cloudflare): a commit makes sure an alarm exists before its
rows reach SQLite (D131)`); before it the D113 fix, and the D115 fix (`fix(lsm-cloudflare): a refused flush marks only its own phase fenced;
a seal forgets its phase only after applying its edit (D115)`). When a covered function changes,
move or add the labels so that each is still one step between two fiber switches, translate,
rerun, and show each property still fails on its broken variant before moving the sync commit.
For drift that skipped this skill, use
`.agents/skills/maintain-formal-verification-skill/SKILL.md`.
