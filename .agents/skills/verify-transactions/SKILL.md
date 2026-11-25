---
name: verify-transactions
description: "Model-check that cf-slatedb's SerializableSnapshot transactions admit only serializable histories: every committed transaction read exactly the values the database held when it committed. Use after any change to DbWriter.commitTransaction, MemTable.writtenAfter, DbClient.beginTransaction/getAt/scanAt/transaction, the reader's maxLastL0Seq check, or how flushes advance flushed_seq."
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

Files: `packages/lsm-cloudflare/src/db-writer.ts` (`commitTransaction`, `commitBatch`),
`mem-table.ts` (`writtenAfter`), `db-client.ts` (`beginTransaction`, `getAt`, `scanAt`,
`transaction`), `db-reader.ts` (`maxLastL0Seq`). Code-level tests that bind the steps to the code:
`db-transaction.test.ts`: a property test interleaving three transactions with direct writes and
flushes through the real writer, checking every committed `SerializableSnapshot` transaction read
the values current at its commit and that `Snapshot` loses no update (shown failing without the
read-key check, the write-key check, or the flush check); write skew allowed under `Snapshot` and
refused under `SerializableSnapshot`; `SnapshotTooOld` at read and at commit. `writer.workerd.ts`
refuses write skew through the public API.

## Bounds and assumptions

- `Transactions.cfg`: 2 keys, 2 transactions (symmetric), 6 commits (~8M distinct states). Two
  transactions are enough for write skew, lost updates, and the flush race.
- Range reads (`readRanges`) are modeled as their keys: `writtenAfter` checks a range by the same
  "row newer than readSeq" query.
- Reads at `readSeq` are exact here. In the code a read that would need a manifest with data
  flushed after `readSeq` (which compaction may have collapsed) aborts instead
  (`maxLastL0Seq`, `SnapshotTooOld`), which is what `Read` models when `flushed > readSeq`.
- One writer instance (fencing is `verify-manifest-cas-fencing`).

## Maintenance notes

- Checked against `c4ea735` (Phase 10). `db-client.ts` and `db-reader.ts` gained the change feed
  (`changes`, `changelogRows`) and the reader now reads changelog SSTs through `ObjectReader`; the
  covered functions (`beginTransaction`, `getAt`, `scanAt`, `transaction`, the `maxLastL0Seq` check)
  did not change, and `vp run verify` passes with both broken variants still failing.

- Checked again for hand-off item 6 (D50): the limit is kept and no covered function changed;
  `Flush` in the model already interleaves anywhere between `Begin` and `Commit`, which includes the
  only window that aborts live (a transaction outlasting a whole flush). `vp run verify` passes with
  both broken variants failing.

## Keeping it in step with the code

Sync commit: `c4ea735`. When a covered function changes after it, update the model
so each action is still one awaited effect in the code, rerun, and show each invariant still fails on
a broken variant before moving the sync commit. For drift that skipped this skill, use
`.agents/skills/maintain-formal-verification-skill/SKILL.md`.
