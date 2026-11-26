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

Expected: three `✓` lines. The code as written passes; `snapshotOnly` (the commit checks written
keys only, which is `Snapshot` isolation) and `noFlushCheck` (the commit skips the `flushed_seq`
check, so writes already flushed out of the memtable go unseen) fail `Serializable`.

## Read a result

Read results with the pstack `/statechart` skill (`.agents/skills/statechart/SKILL.md`, opened by
path), on a scratch copy of `tools/formal-verification/specs/Transactions.tla` and its config, with
`JAVA` and `TLA2TOOLS` pointing at `.tlc/`. Replay a reported path against the code, one action per
step:

| Action | Code |
| --- | --- |
| `Begin(t)` | `DbClient.beginTransaction`: `readSeq` = the writer's `last_seq` (`WriterClient.status`) |
| `Read(t, k)` | `DbClient.getAt`: the probe's `flushedSeq` (and the manifest's `lastL0Seq`) above `readSeq` aborts; else the value at `readSeq` |
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

Sync commit: the D70–D74 commit (`fix(read)`, `fix(writer)`, and `perf`: the snapshot bound, the newest-version flush, the read and flush path changes, the writer's counters). When a covered function changes after it, update the model
so each action is still one awaited effect in the code, rerun, and show each invariant still fails on
a broken variant before moving the sync commit. For drift that skipped this skill, use
`.agents/skills/maintain-formal-verification-skill/SKILL.md`.
