---
name: verify-change-feed
description: "Model-check that shaledb's change feed delivers every committed change exactly once, in order, or tells the consumer Expired, across flush commits whose response is lost, writer restarts, changelog pruning, and paging. Use after any change to DbClient.changes, DbReader.changelogRows, DbWriter.flushThen's changelog export, commitL0Flush (afterSeq, the changelog entry), pruneChangelog, or changelogHoldsChangesAfter."
---

# Verify: the change feed (ChangeFeed)

## What is verified, in plain words

- **ExactlyOnce**: a consumer following `DbClient.changes` from its cursor receives changes 1, 2,
  3, … in order, each once, none skipped, until it is told `Expired` (D80). Harm if broken: a CDC
  consumer applies a change twice, or misses one.
- **NoOverlappingEntries** (`ChangeFeed.cfg`): no two changelog entries hold the same change
  (D113). Harm if broken: duplicate changelog SSTs; the feed reads them once only because of the
  reader's skip.
- **FlushedInManifest**: the writer's `flushed_seq` is never past the manifest's `lastL0Seq`.

## Run it

```sh
node tools/formal-verification/src/verify.ts change-feed   # about 8 seconds
```

Expected: six `✓` lines.

| Config | `Broken` | Expected |
| --- | --- | --- |
| `ChangeFeed.cfg` | `none` | pass (31,024 states) |
| `ChangeFeed.cfg` | `lostResponseReexport` (no stale refusal) | `NoOverlappingEntries` |
| `ChangeFeed.cfg` | `noExpiredCheck` (before D80) | `ExactlyOnce` |
| `ChangeFeed.cfg` | `appendAfterCut` (before D93's page rule) | `ExactlyOnce` |
| `ChangeFeedOldData.cfg` | `beforeD113` (no stale refusal, no reader skip) | `ExactlyOnce` |
| `ChangeFeedOldData.cfg` | `lostResponseReexport` | pass: the reader alone reads overlapping entries once, so manifests written before D113 are safe |

## Read a result

| Action | Code |
| --- | --- |
| `Write` | `DbWriter.write`: one batch, one sequence number (one row per batch here) |
| `FlushStart` | `flushThen`: `memTable.meta` (`afterSeq = flushed_seq`, `cut = last_seq`), `exportChangelog(afterSeq, cut)` uploaded |
| `FlushCommit` | `updateFrom(commitL0Flush)`: refused as stale (`FlushedSeqStaleError`; the writer runs `markFlushed(lastL0Seq)` and flushes again), or the entry `[afterSeq + 1, cut]` appended; then `markFlushed(cut)`, or the response lost (`attemptCreate`'s boundary read fails) and nothing recorded |
| `Restart` | eviction, then `startWriter`: `flushed_seq` raised to `lastL0Seq` |
| `Prune` | `pruneChangelog` (retention or D99's cap): the oldest entry dropped, `changelogAfterSeq` raised to its `lastSeq` |
| `ConsumeProbe` | `writer.changes`: `flushed_seq` and the rows above `max(afterSeq, flushed_seq)`, in one `consistently` |
| `ConsumeRead` | `changelogRows` from the newest manifest: `Expired` unless `changelogAfterSeq <= afterSeq`; entries in `(firstSeq, lastSeq)` order, rows at or below an earlier entry's `lastSeq` skipped; the flushed part cut at its byte bound or `limit` ends the page; else the unflushed rows follow, the page cut at `limit` |

## Bounds and assumptions

- Both configs: 4 sequence numbers, 2 lost responses, 1 restart, 2 prunes, `limit` 2, one
  consumer. The byte bound is any number of rows from 1 to `limit`.
- One row per batch: `cutAtSequenceBoundary`'s whole-batch rule and the one-batch-larger-than-a-page
  case are in `change-feed.test.ts`, not here.
- The reader uses the newest manifest. An older cached manifest that covers `flushed_seq` holds
  a subset of the entries and a lower `changelogAfterSeq`, so it answers the same or `Expired`.
- Not modeled: forks and split successors (their feed starts at the fork point, D80;
  `database-lifecycle.test.ts`), the changelog option disabled, and SSTs pruned between the
  manifest read and the SST read (a `ReaderUnavailableError`, not a gap).

## Deeper search

Not registered in `formal-checks.ts`. Run one at a time, niced, as for any spec (see
`verify-writer-object`'s "Deeper search" for the command):

- `ChangeFeedSearch.cfg`: 6 sequence numbers, 3 lost responses, 2 restarts, 3 prunes, `limit` 3.
  No error: 715,221 distinct states, depth 33, 17 s with one worker (2026-11-26).
- `ChangeFeedSearchLarge.cfg`: 8 sequence numbers, 4 lost responses, 3 restarts, 4 prunes.
  No error: 9,153,728 distinct states, depth 44, 306 s with one worker (2026-11-26).

## Maintenance notes

- D113 (this model): found while placing its labels, as a failing test first. A flush commit
  that landed and lost its response made the next flush export the same changes again. Fixed
  in `commitL0Flush` (`afterSeq`, `FlushedSeqStaleError`), `flushThen` (records the manifest's
  state and flushes again), and `changelogRows` (each change once).

## Keeping it in step with the code

Sync commit: the D113 fix (`fix(lsm-cloudflare): a flush is exact about where it starts; the
change feed reads each change once (D113)`). When a covered function changes, update the action
it maps to, rerun, and show each property still fails on its broken variant. For drift that
skipped this skill, use `.agents/skills/maintain-formal-verification-skill/SKILL.md`.
