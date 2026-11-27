---
name: verify-paged-scan
description: "Model-check shaledb's paged scans: a Latest scan in segments with resumeFrom (D93), pages that end on their work budget (D125), and a transaction scan read in several pages at one readSeq whose commit checks the whole scanned range (D125's phantom rule), with writes, flushes, trims, and GC between pages. Use after any change to DbClient.scanUnflushed, scanWithBudget, scanAtPage, the scan's manifest bounds (ReadView minLastL0Seq/maxLastL0Seq), ScanPageBudget, or commitTransaction's readRanges check."
---

# Verify: paged scans (PagedScan)

## What is verified, in plain words

- **LatestPageAtSnapshot** (`PagedScanLatest.cfg`): every `Latest` page holds exactly the live
  rows of `[its start, resumeFrom)` at the one snapshot it fixed. This holds when it ends on its
  budget, at the range's end, or overtaken by a flush after returning rows. The next page starts
  at `resumeFrom`, so no row is skipped or repeated. Harm if broken: a page mixes two moments,
  or repeats or misses keys.
- **TxnPagesAtReadSeq** and **TxnPagesComplete** (`PagedScanTxn.cfg`): every row of a
  transaction's pages is the database at `readSeq`, and together they hold every live key of the
  range. Writes, flushes, trims, and manifest GC may happen between any two pages. Harm if
  broken: a transaction reads a value that is not its snapshot's.
- **PhantomRefused** (`PagedScanTxn.cfg`): a commit is admitted only if no write after `readSeq`
  touched a key in the range scanned, including keys absent at `readSeq`. Harm if broken: a
  phantom inside a scanned range commits, which is not serializable.

## Run it

```sh
node tools/formal-verification/src/verify.ts paged-scan
```

Expected: four `✓` lines.

| Config | `Broken` | Expected |
| --- | --- | --- |
| `PagedScanLatest.cfg` | `none` | pass (2,045,175 states) |
| `PagedScanLatest.cfg` | `restartAfterRows` (a page overtaken after rows starts again) | `LatestPageAtSnapshot` |
| `PagedScanTxn.cfg` | `none` | pass (2,579,572 states) |
| `PagedScanTxn.cfg` | `pageRangesOnly` (the commit names only the returned keys) | `PhantomRefused` |

## Read a result

| Action | Code (`packages/lsm-cloudflare/src/`) |
| --- | --- |
| `Write`, `Flush`, `Trim`, `Collect` | a put or delete; a flush (commit and `markFlushed` at once); `MemTable.trim`; GC deleting a manifest that is not the newest |
| `LPageStart` | a page (`scan` with a limit) with its `ScanPageBudget`: here, keys it may examine |
| `LProbe` | `DbWriter.scan`: one writer page with `flushed_seq`; the first fixes the snapshot |
| `LMerge` | `reader.scan` for the segment with a stored manifest, `max flushed_seq <= lastL0Seq <= snapshot`; with none, `restart()` from the range's start before any row was emitted, else `ReaderUnavailableError` with `resumeFrom` (`OvertakenByFlush`) |
| `LPageEnd` | the page answered; the caller asks again from `resumeFrom` |
| `TBegin` | `beginTransaction`: `readSeq = last_seq` |
| `TPageStart`, `TProbe`, `TMerge` | `scanAtPage`: a page with its budget; `probeAt` rows above `retained_from_seq` with it; a manifest with `retained_from_seq <= lastL0Seq <= readSeq`, else `SnapshotTooOld` |
| `TCommit` | `commitTransaction`: `SnapshotTooOld` when `retained_from_seq > readSeq`; `ReadConflict` when `writtenAfter(readRanges, readSeq)` |

## Bounds and assumptions

- 3 keys, 3 sequence numbers (each a put or a delete of one key), budget 1 or 2 keys per page, 1
  manifest collection. One scanner per config: both in one model passed 48M states in 7 minutes
  without finishing.
- A segment is one key, and the budget counts keys examined, not bytes. The settled-key skip
  and merge operands (D125) are left to `scan-bounds.test.ts`.
- A flush commits and records `flushed_seq` in one step (`NoAckedWriteLost` and `WriterObject`
  check the steps between). Compaction is not a step: under D70's bounds a manifest answers the
  same compacted or not.
- The transaction's own writes are not modeled; the commit is a check.
- The code-level oracle tests (`scan-oracle.property.test.ts`, `shard-scan-oracle.property.test.ts`)
  check the real pages against an oracle; this model checks the protocol under every
  interleaving at these bounds.

## Maintenance notes

- D116 (this model). Writing it showed one thing about the code, recorded in the model: a restart
  before any row was emitted goes back to the range's start, not to the segment that was
  overtaken (`restart()` sets `start = range.start`). A first draft that resumed at the
  overtaken segment failed `LatestPageAtSnapshot`: an earlier key examined at the old snapshot
  had found nothing. So the code's choice is required.

## Keeping it in step with the code

Sync commit: the D116 commit (`test(formal): PagedScan, paged Latest and transaction scans
(D116)`). When a covered function changes, update the action it maps to, rerun, and show each
property still fails on its broken variant. For drift that skipped this skill, use
`.agents/skills/maintain-formal-verification-skill/SKILL.md`.
