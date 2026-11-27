---
name: verify-transactions
description: "Model-check that shaledb's SerializableSnapshot transactions admit only serializable histories: every committed transaction read exactly the values the database held when it committed. Use after any change to DbWriter.commitTransaction, MemTable.writtenAfter, DbClient.beginTransaction/getAt/scanAt/transaction, the reader's maxLastL0Seq check, or how flushes advance flushed_seq."
---

# Verify: transactions

## What is verified, in plain words

- **Serializable**: with `SerializableSnapshot`, every committed transaction read exactly the
  values the database held at the moment it committed. Commits happen one at a time under the
  writer's commit lock, so committing in that order is a serial order: the history is serializable.
  Harm if broken: two transactions each act on a state the other changed (write skew, lost
  update), and the database ends in a state no serial order produces.

## Run it

```sh
node tools/formal-verification/src/verify.ts transactions   # about 30 seconds
vp run verify                                               # every formal check
```

Expected: five `✓` lines. The code as written passes. `snapshotOnly` (the commit checks written
keys only, which is `Snapshot` isolation) and `trimWithoutAbort` (the commit skips the
`retained_from_seq` check, so a trim past an open transaction hides its writes) fail
`Serializable`. `noReadHorizonCheck` (a read accepts a manifest below `retained_from_seq`) and
`noReadUpperBound` (a read accepts a manifest flushed past `readSeq`) fail `ReadsAtBegin`.

## Read a result

Read results with the pstack `/statechart` skill (`.agents/skills/statechart/SKILL.md`, opened by
path), on a scratch copy of `tools/formal-verification/specs/Transactions.tla` and its config, with
`JAVA` and `TLA2TOOLS` pointing at `.tlc/`. Replay a reported path against the code, one action per
step:

| Action | Code |
| --- | --- |
| `Begin(t)` | `DbClient.beginTransaction`: `readSeq` = the writer's `last_seq` (`WriterClient.status`) |
| `Read(t, k)` | `DbClient.getAt`/`scanAt`: the writer's rows in `(retained_from_seq, readSeq]` (`probeAt`), merged by `DbReader.manifestFor` with the manifest the reader holds (its cached one: any committed manifest, so any `lastL0Seq` up to `seq`) when `retained_from_seq <= lastL0Seq <= readSeq` (`Fits`), else the begin manifest when it is stored and fits, else `SnapshotTooOld`. The model reads with any fitting `m \in 0 .. seq` and aborts whenever the begin manifest does not fit (D76, D83) |
| `Write(t, k)` | `DbTransaction.put` (buffered in the client) |
| `Commit(t)` | `DbWriter.commitTransaction` under `commitLock`: `flushed_seq > readSeq` aborts; `MemTable.writtenAfter(written keys, and for SerializableSnapshot read keys and ranges)` aborts; else `commitBatch` |
| `DirectWrite(k)` | `DbWriter.write` (a plain put) |
| `Flush` | `DbWriter.flushNow`: rows up to the cut leave the memtable and `flushed_seq` advances |

If a step cannot happen in the code, fix the model. If every step can, it is a bug: report it in
the code's words.

## Coverage record

Files (D58): `packages/lsm-cloudflare/src/db-writer.ts` (`commitTransaction`: `SnapshotTooOld`
only when `retained_from_seq > readSeq`; `probeAt`; `scan` with `includeRetained`; `status` with
`flushedManifestId`), `mem-table.ts` (`writtenAfter` over retained and unflushed rows,
`markFlushed`, `trim`, `retained_from_seq`), `db-client.ts` (`beginTransaction` returning the
flushed manifest, `getAt`/`scanAt` from it, `transaction`), `db-reader.ts` (the pinned manifest's
`minLastL0Seq`/`maxLastL0Seq` check). Code-level tests that bind the steps to the code:
`db-transaction.test.ts`: a property test interleaving three transactions with direct writes,
flushes, compactions, and the clock moving (3 s retention, a ~120-byte bound, so trims happen while
transactions are open) through the real writer and executor, checking every committed
`SerializableSnapshot` transaction read the values current at its commit and that `Snapshot` loses no
update; exact reads and commits across flushes and a compaction; `SnapshotTooOld` only once trimmed
(and at the first flush for callers without a manifest id); write skew allowed under `Snapshot` and
refused under `SerializableSnapshot`. `mem-table.test.ts`: flushes keep rows, trims keep the newest
flush. Mutations caught: the old flushed-seq check at commit, conflict checks or probes that ignore
retained rows, the pinned read without its horizon check, a trim of the newest flush, a flush that
deletes rows. `writer.workerd.ts` refuses write skew through the public API.

## Bounds and assumptions

- `Transactions.cfg`: 2 keys, 2 transactions (symmetric), 5 commits (~37M distinct states, ~2 min).
  Two transactions are enough for write skew, lost updates, and trims racing a transaction; 6 commits
  exceeds the time limit with the retained horizon added.
- `Trim` raises `retained_from_seq` to any cut at or below `flushed_seq` (a superset of the retention
  policy, which keeps the newest flush).
- A pinned manifest is its `lastL0Seq`: a read takes from it the newest version at or below it, which
  any compaction of that manifest keeps; pinned manifests are never rewritten, so compaction of the
  newest manifest changes nothing a transaction observes and is not an action here.
- Range reads (`readRanges`) are modeled as their keys: `writtenAfter` checks a range by the same
  "row newer than readSeq" query.
- Callers without a manifest id (before D58) keep the old reads (a flush past `readSeq` aborts); the
  model checks the D58 path, and the commit-time check is the same for both.
- One writer instance (fencing is `verify-manifest-cas-fencing`).
- Broken variants: `snapshotOnly` (written keys only), `trimWithoutAbort` (commit skips the
  horizon check, so a trim past an open transaction hides writes), `noReadHorizonCheck` (reads skip
  the pinned manifest's horizon check); each fails `Serializable`.

## Maintenance notes

- D84: `Commit(t)` is one atomic step in the model: the horizon check and the conflict queries see
  the same SQLite state. In the code they were separate statements outside any SQLite
  transaction, and a flush's trim (which takes no commit lock) could run between them and delete
  rows above `readSeq`, so the conflict query missed a write: a lost update. Now
  `commitTransaction` re-reads `retained_from_seq` after the queries and aborts with
  `SnapshotTooOld` if it passed `readSeq`.
  - **Why the atomic step matches the code again.** A trim only raises the horizon. If the horizon
    after the queries is still at or below `readSeq`, no trim in between deleted a row they looked
    for, so they saw what one atomic read would have seen. If it passed `readSeq`, the code aborts,
    as the model's step does when the trim comes first.
  - **Why not `MemTable.consistently`.** It is atomic in production (`transactionSync`), but the
    test storage runs it as a savepoint that another fiber can interleave, so no unit test could
    show it; the re-read is proven by `db-writer.test.ts` ("a trim between the commit's horizon
    check and its conflict query …") and two mutations.
  - No model step changed. Reran: pass in 239.8 s of the 240 s limit (36,915,507 states), and
    every broken variant fails. The limit needs raising before the model grows.

- D83 (review of D76): `Read` chose only between the begin manifest and `flushed`, but the
  reader's "newest" is its cached manifest, which may be older than the begin manifest, between
  it and the newest, or a flush's manifest committed before `flushed_seq` recorded it. `Read` now
  reads with any `m \in 0 .. seq` that `Fits`, and aborts only when the begin manifest does not
  fit (the code refuses only when neither the held manifest nor the begin manifest fits; refusing
  more often is safe, so the model over-approximates the refusals). The header comment now says
  the held manifest is tried first, as the code does. 36.9M states, 167 s niced on a loaded
  machine (an earlier draft that aborted whenever any chosen `m` did not fit timed out at 420 s);
  the time limit stays 420 s. All four broken variants still fail.

- D76: a transaction's read no longer requires its begin manifest. `DbReader.manifestFor` uses
  the newest manifest when it is between the read's bounds
  (`retained_from_seq <= lastL0Seq <= readSeq`), otherwise the begin manifest when it is stored
  and between them, otherwise `SnapshotTooOld`. The defect behind it: GC deletes the begin
  manifest `gcMinAge` after a compaction replaced it while the writer still names it, so every
  transaction on a warm database with no flush for that long failed, on every retry. The model's
  `Read` now takes either manifest (`\E m \in {pinned[t], flushed}`) and aborts when the one it
  finds does not fit (`Fits`). New invariant `ReadsAtBegin` (every read, committed or not,
  returned the value at the transaction's begin), which `noReadHorizonCheck` and the new
  `noReadUpperBound` fail; `snapshotOnly` and `trimWithoutAbort` still fail `Serializable`.
  40.6M states, about 170 s: the run's time limit is raised to 420 s. `DbClient.snapshot` now
  loads the newest manifest; snapshots are not in this model.

- D74: `DbClient.beginTransaction` still reads `lastSeq` and the flushed manifest from
  `DbWriter.status`, in one read of the writer's bookkeeping (`memTable.meta`); `status` no longer
  scans the memtable for the unflushed size, which `begin` never used. No model step changed.

- D70: (1) Fixed: `DbClient.scanAt` without a begin manifest (a caller from before D58) built its
  view with `minLastL0Seq = 0`, so within `manifestRefreshInterval` of a flush it could read a
  cached manifest from before that flush while the writer's pages no longer held the flushed
  rows. It now requires the largest `flushedSeq` its pages reported, as `getAt` always did.
  Test: `db-transaction.test.ts` "a scan by a caller that sends no begin manifest reads rows
  flushed since the reader cached its manifest", which fails on the old code. The model's reads
  already took flushed rows from a manifest covering them, so no step changed. (2) A flush now
  writes only the versions a read can reach. A transaction's reads take every row above its begin
  manifest's `lastL0Seq` from the writer's kept rows, and the begin manifest's SSTs for the rest
  at a snapshot at or after their newest rows; the model already represents a pinned manifest by
  its `lastL0Seq` and lets `Compact` collapse versions, which covers a collapsing flush. Reran;
  all broken variants still fail.

- D67 (v1 workstream 10): `ReadView` gained `freshness` and `manifestHint`. `DbClient.getAt` and
  `scanAt` set `freshness: "Recent"` with no hint on every path (pinned and unpinned), so
  transaction reads find their manifest exactly as before: the pinned begin manifest, or for old
  callers the refresh-interval manifest with the `maxLastL0Seq` check. `commitTransaction` and
  `writtenAfter` are unchanged. No model step changed; reran, all broken variants still fail.

- D58 (v1 workstream 4): the model now has the retained horizon, trims, and reads from the begin
  manifest plus rows above the horizon; `noFlushCheck` became `trimWithoutAbort`, and
  `noReadHorizonCheck` is new. Supersedes the D50 notes below.
- Checked against `c4ea735` (Phase 10). `db-client.ts` and `db-reader.ts` gained the change feed
  (`changes`, `changelogRows`) and the reader now reads changelog SSTs through `ObjectReader`; the
  covered functions did not change.
- Checked again for hand-off item 6 (D50, superseded by D58): the limit was kept then.

## Keeping it in step with the code

Sync commit: the D84/D85 commit (`fix(writer)`: the conflict check re-reads the horizon, a seal that does not return forgets the writer's phase). When a covered function changes after it, update the model
so each action is still one awaited effect in the code, rerun, and show each invariant still fails on
a broken variant before moving the sync commit. For drift that skipped this skill, use
`.agents/skills/maintain-formal-verification-skill/SKILL.md`.
