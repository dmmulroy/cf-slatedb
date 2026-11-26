---
name: verify-no-acked-write-lost
description: "Model-check that shaledb never loses or hides an acknowledged write across writer eviction, flush, a crash between L0 upload and manifest commit, and the delete-after-commit rule, and that a Latest read sees every acknowledged write. Use after any change to DbWriter write/flushNow/probe/scan/ensureReady, MemTable commit/truncate/consistently, or the Latest read path."
---

# Verify: no acknowledged write is lost or hidden

## What is verified, in plain words

- **NoAckedWriteLost**: a key's newest acknowledged write is durable either in the memtable (the
  writer Durable Object's SQLite table) or in the SSTs of the newest manifest, whatever crash or
  eviction happened and whenever. Flushes and compactions drop the versions it superseded on
  purpose. Harm if broken: an acknowledged write disappears.
- **LatestSeesAcked**: a `Latest` read returns the newest write acknowledged before it started.
  The writer's probe gives it a snapshot (`last_seq`), `flushedSeq`, and the rows above
  `flushedSeq`. It reads those rows plus the SSTs of a manifest `M` with
  `flushedSeq <= M.lastL0Seq <= snapshot`, which may be a stale cached one, and takes the newest
  version at or below the snapshot. A manifest flushed past the snapshot is refused (the read
  asks the writer again), because a flush or a compaction after the snapshot may have dropped the
  version the snapshot needs. Harm if broken: a read misses a write the caller was told was
  committed.
- **SeqOracleMonotonic**: the memtable never holds a sequence number above `last_seq`, so the
  sequence oracle never reuses one, including after a restart seeded from the manifest.

## Run it

```sh
node tools/formal-verification/src/verify.ts no-acked-write-lost   # about 3 seconds
vp run verify                                                      # every formal check
```

Expected: five `✓` lines. The code as written passes; `truncateBeforeCommit` and `trimUnflushed`
fail `NoAckedWriteLost`; `nonAtomicProbe` and `noSnapshotUpperBound` fail `LatestSeesAcked`.

## Read a result

Read results with the pstack `/statechart` skill (`.agents/skills/statechart/SKILL.md`, opened by
path), on a scratch copy of `tools/formal-verification/specs/NoAckedWriteLost.tla` and its config,
with `JAVA` and `TLA2TOOLS` pointing at `.tlc/`. Replay a reported path against the code, one action
per step:

| Action | Code |
| --- | --- |
| `Write` | `DbWriter.write` → `MemTable.commit` (one SQLite transaction), then the receipt |
| `FlushUpload` | `flushNow`: `cut = meta.lastSeq`, `SstStore.write(keepReachableVersions(memTable.rowsUpTo(flushedSeq, cut)))`: of the rows in the window, the versions a read can reach (for one key, the newest) |
| `FlushCommit` | `ManifestStore.update(commitL0Flush)`: the new manifest has the SST, `lastL0Seq = cut` |
| `Compact` | `CompactionExecutor.commit` of `compactRowStreams`' output: a new manifest whose SSTs hold each key's newest version only |
| `FlushTruncate` | `MemTable.markFlushed(cut)`: sets `flushed_seq`; the rows stay until a `Trim` (D58) |
| `Crash` | Durable Object eviction or crash at any await: SQLite and R2 survive, nothing else does |
| `Restart` | `ensureReady`: `seed` from the manifest, `truncate(lastL0Seq)` |
| `Probe` | `DbWriter.probe` → `MemTable.consistently(meta + versions)`: the snapshot (`last_seq`), `flushed_seq`, and the rows above it |
| `ReadManifest` | `DbReader.manifestFor` with the view `DbClient.pointView` builds (`minLastL0Seq = flushedSeq`, `maxLastL0Seq = snapshotSeq`): any manifest between the two (the cached one, or a load when it is older) |
| `ReadAgain` | `DbClient.getRow`/`scan`: on `SnapshotExpired` (only a manifest flushed past the snapshot was found) a new probe, up to three times |

If a step cannot happen in the code, fix the model. If every step can, it is a bug: report it in
the code's words.

## Coverage record

Files: `packages/lsm-cloudflare/src/db-writer.ts` (`write`, `flushNow`, `probe`, `scan`,
`ensureReady`), `mem-table.ts` (`commit`, `truncate`, `consistently`, `seed`), `manifest.ts` in
lsm-core (`commitL0Flush`). Code-level tests that bind the steps to the code:
`packages/lsm-cloudflare/src/db-writer.test.ts` ("a crash between the manifest commit and the
memtable truncate loses and duplicates nothing", "flushes to an L0 SST, commits lastL0Seq, and only
then empties the memtable"; each fails when the restart truncate or the fence is removed),
`mem-table.test.ts` (truncate exactly at the cut). `db-client.test.ts`: "a Latest read that finds
only a manifest flushed past its snapshot asks the writer again, never missing the key (D70)" and
"a Latest scan overtaken by a flush between its pages of unflushed rows starts again (D70)", each
of which fails without the snapshot bound; the client-versus-oracle property, now over histories
of 20 to 60 steps. `row-resolution.test.ts`: `keepReachableVersions` against the oracle for any
snapshot at or after the window's newest row.

The model has found two real bugs. While it was first written: `probe` read `flushed_seq` and the
rows in two steps, so a flush could truncate between them (`nonAtomicProbe` reproduces it). When
compaction was added to it (D70): a `Latest` read that loaded a manifest flushed and compacted
past its snapshot filtered out the newer version and no longer found the older one
(`noSnapshotUpperBound` reproduces it, with the flush as it was then or as it is now).

## Bounds and assumptions

- `NoAckedWriteLost.cfg`: 4 writes, 2 crashes, 2 reads (~282k states). One key, whose writes are
  its versions: a later write supersedes an earlier one, which is what lets a flush or a
  compaction drop a version, and what a read's snapshot bound protects. More keys add nothing.
- Assumed: Durable Object SQLite transactions are atomic and durable before the response (output
  gates); manifest CAS and fencing are verified separately (`verify-manifest-cas-fencing`); a
  single live writer instance at a time owns the SQLite storage (Cloudflare's guarantee; fencing
  covers late R2 writes from a stale instance).
- Not modeled: TTL and merge operands (a flush keeps every operand down to the first value or
  delete; `row-resolution.test.ts` checks that against the oracle), and multi-page scans. A scan's
  view takes the largest `flushedSeq` across its pages as the lower bound and its snapshot as the
  upper one, so a flush between two pages leaves no manifest that fits and the scan starts again;
  `db-client.test.ts` covers it.

## Maintenance notes

- D74: `DbWriter.write` now advances three in-memory counters (unflushed bytes with the formula
  SQLite sums, unflushed rows, the oldest commit time), and `runAlarm` and `status` read them
  instead of scanning the memtable. That changes when a flush is scheduled and what a status
  call costs, not what a flush, a commit, or a probe does: `flushThen` still takes its cut from
  `memTable.meta` and reads every row above `flushed_seq`. No model step changed; reran, all
  broken variants still fail.

- D70: two changes, modeled together. (1) The model gained `Compact` (it had left compaction to
  the GC and CAS models), the read's snapshot `rSnap`, and `ReadAgain`; `ReadManifest` now needs
  `lastL0Seq <= rSnap` and returns the newest version at or below it; the invariants are stated
  for a key's newest version. That exposed the bug above in the code as it was: fixed by setting
  `maxLastL0Seq` on every view built from a probe and asking the writer again on
  `SnapshotExpired`. (2) `FlushUpload` keeps the newest version of the window
  (`keepReachableVersions`), which the same bound makes safe. Also fixed, outside this model: a
  transaction scan without a begin manifest used `minLastL0Seq = 0` (see `verify-transactions`).
  `DbWriter.flushThen` hands the compactor wake to `BackgroundWork` (D73): no step. Reran; all
  four broken variants fail.

- D67 (v1 workstream 10): the `Latest` read path now uses the reader's cached manifest whenever
  its `lastL0Seq` is at least the probe's `flushedSeq`, however long ago it was refreshed
  (`ManifestCache.covering`, `ReadView.freshness: "Covering"`), instead of only within
  `manifestRefreshInterval`. That is exactly `ReadManifest`, which always chose *any* manifest with
  `lastL0Seq >= rFs`, so no model step changed. The probe and the memtable scan also return
  `flushedManifestId`, read in the same `consistently` block as `flushed_seq`; it only tells the
  reader where to start looking for the newest manifest (`loadLatestFrom`, covered by
  `verify-manifest-cas-fencing`). `AtLeast` reads that skip the writer keep the refresh-interval
  bound and are not `Latest` reads. `DbWriter.ensureReady` answers from memory once ready (the
  same phase it returned under the lock). Mutation caught by `manifest-store.test.ts`: a covering
  read ignoring `flushedSeq`. Reran; all broken variants still fail.

- D62 (v1 workstream 7): `Compactor.dropSst` removes one SST from the manifest at an operator's
  explicit, confirmed request, after a compaction reported it corrupted (or with `force`). Its rows
  are lost by design and the manifest records the loss (`droppedSsts`), like removal (D39); the
  model's property covers the engine, not an operator's repair, so no step changed. Upload integrity
  (SHA-256 on single-part puts, MD5-checked multipart parts) only makes a flush fail and retry where
  it would have stored damaged bytes.
- Checked against the v1 workstream 6 commit (D61). `DbWriter.write` no longer scans the memtable
  to schedule the flush alarm: it reads in-memory counters (oldest unflushed commit time, unflushed
  bytes), refreshed from SQLite at start and after each flush. That only decides *when* a flush
  runs; a flush still reads every row above `flushed_seq`. The flush page queries now resume from a
  `(key, seq)` cursor (`resumeAfterCursor`, shared with `MemTable.scan`) with `+seq` bounds; the
  paged-scan property test in `mem-table.test.ts` (pages of 3 rows, the same cursor condition) shows
  every version is read exactly once, in order. No model step changed.

- D58 (v1 workstream 4): a flush no longer deletes rows. `MemTable.markFlushed` records
  `flushed_seq` after the manifest commit (the model's `FlushTruncate`), and a separate `Trim`
  deletes flushed rows up to a horizon at or below `flushed_seq` at any time. The `Latest` probe
  reads only rows above `flushed_seq`, and a restart records the manifest's `lastL0Seq` as flushed
  without deleting. New broken variant `trimUnflushed` fails `NoAckedWriteLost`;
  `truncateBeforeCommit` (advance and trim before the commit) and `nonAtomicProbe` still fail.

- Checked against the v1 workstream 2 commit (D56). `DbWriter.flushNow` and `DbWriter.seal` commit
  through `ManifestStore.updateFrom` from the writer's own view: still one manifest commit between
  the upload and the truncate, which may internally reload and retry (as `update` did); a refused
  or conflicting first attempt writes nothing the reader sees. The `LATEST` hint is now written in
  the background, and `loadLatest` probes from above GC's boundary; the `Latest` read path's probe of
  `flushed_seq` is unchanged. No model step changed; reran, both broken variants still fail.
- Checked against `c4ea735` (Phases 9–10). `DbWriter.write` now commits through `commitBatch`
  (the same commit lock and SQLite commit; the ack after it); `commitTransaction` runs its conflict
  check inside that lock before the same commit, so it is one more kind of `Commit`/`Write`.
  `MemTable.clear` runs only on removal. The change feed only reads.
- Checked against `3226177` (Phases 5–7). A flush whose commit is refused below GC's boundary
  fails before the truncate, leaving the rows in SQLite (the same as a crash between upload and
  commit). Flushes now wake the compactor after the truncate (no state change). `DbWriter.destroy`
  deletes the memtable and `ensureReady` reports a removed database as not found: both run only
  after `markRemoved` fenced the database for removal, whose purpose is to delete its data (design
  §12, decision D39), so removal is outside this check.

## Keeping it in step with the code

Sync commit: the D70–D74 commit (`fix(read)`, `fix(writer)`, and `perf`: the snapshot bound, the newest-version flush, the read and flush path changes, the writer's counters). When a covered function changes after it, update the model so
each action is still one awaited effect in the code, rerun, and show each invariant still fails on a
broken variant before moving the sync commit. For drift that skipped this skill, use
`.agents/skills/maintain-formal-verification-skill/SKILL.md`.
