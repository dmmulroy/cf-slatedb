---
name: verify-no-acked-write-lost
description: "Model-check that shaledb never loses or hides an acknowledged write across writer eviction, flush, a crash between L0 upload and manifest commit, and the delete-after-commit rule, and that a Latest read sees every acknowledged write. Use after any change to DbWriter write/flushNow/probe/scan/ensureReady, MemTable commit/truncate/consistently, or the Latest read path."
---

# Verify: no acknowledged write is lost or hidden

## What is verified, in plain words

- **NoAckedWriteLost**: every write the writer acknowledged is durable either in the memtable
  (the writer Durable Object's SQLite table) or in the SSTs of the newest manifest, whatever crash
  or eviction happened and whenever. Harm if broken: an acknowledged write disappears.
- **LatestSeesAcked**: a `Latest` read sees every write acknowledged before it started. It reads
  the memtable through the writer's probe, plus the SSTs of any manifest at least as new as the
  probe's `flushedSeq`, which may be a stale cached manifest. Harm if broken: a read misses a write
  the caller was told was committed.
- **SeqOracleMonotonic**: the memtable never holds a sequence number above `last_seq`, so the
  sequence oracle never reuses one, including after a restart seeded from the manifest.

## Run it

```sh
node tools/formal-verification/src/verify.ts no-acked-write-lost   # about 3 seconds
vp run verify                                                      # every formal check
```

Expected: three `✓` lines. The code as written passes, `truncateBeforeCommit` fails
`NoAckedWriteLost`, and `nonAtomicProbe` fails `LatestSeesAcked`.

## Read a result

Read results with the pstack `/statechart` skill (`.agents/skills/statechart/SKILL.md`, opened by
path), on a scratch copy of `tools/formal-verification/specs/NoAckedWriteLost.tla` and its config,
with `JAVA` and `TLA2TOOLS` pointing at `.tlc/`. Replay a reported path against the code, one action
per step:

| Action | Code |
| --- | --- |
| `Write` | `DbWriter.write` → `MemTable.commit` (one SQLite transaction), then the receipt |
| `FlushUpload` | `flushNow`: `cut = meta.lastSeq`, `SstStore.write(memTable.rowsUpTo(cut))` |
| `FlushCommit` | `ManifestStore.update(commitL0Flush)`: the new manifest has the SST, `lastL0Seq = cut` |
| `FlushTruncate` | `MemTable.truncate(cut)`: deletes rows `<= cut` and sets `flushed_seq`, one transaction |
| `Crash` | Durable Object eviction or crash at any await: SQLite and R2 survive, nothing else does |
| `Restart` | `ensureReady`: `seed` from the manifest, `truncate(lastL0Seq)` |
| `Probe` | `DbWriter.probe` → `MemTable.consistently(meta + versions)` |
| `ReadManifest` | the reader's manifest: any manifest with `lastL0Seq >= flushedSeq` (refresh when older) |

If a step cannot happen in the code, fix the model. If every step can, it is a bug: report it in
the code's words.

## Coverage record

Files: `packages/lsm-cloudflare/src/db-writer.ts` (`write`, `flushNow`, `probe`, `scan`,
`ensureReady`), `mem-table.ts` (`commit`, `truncate`, `consistently`, `seed`), `manifest.ts` in
lsm-core (`commitL0Flush`). Code-level tests that bind the steps to the code:
`packages/lsm-cloudflare/src/db-writer.test.ts` ("a crash between the manifest commit and the
memtable truncate loses and duplicates nothing", "flushes to an L0 SST, commits lastL0Seq, and only
then empties the memtable"; each fails when the restart truncate or the fence is removed),
`mem-table.test.ts` (truncate exactly at the cut). The model found a real bug while it was
written: `probe` read `flushed_seq` and the rows in two steps, so a flush could truncate between
them. The `nonAtomicProbe` variant reproduces it.

## Bounds and assumptions

- `NoAckedWriteLost.cfg`: 4 writes, 2 crashes, 2 reads (~32k states). One key: a write can be
  hidden only if its row is in neither place a read looks, so more keys add nothing.
- Assumed: Durable Object SQLite transactions are atomic and durable before the response (output
  gates); manifest CAS and fencing are verified separately (`verify-manifest-cas-fencing`); a
  single live writer instance at a time owns the SQLite storage (Cloudflare's guarantee; fencing
  covers late R2 writes from a stale instance).
- Not modeled: TTL and merge resolution (property tests in lsm-core), compaction (GC and CAS
  models), multi-page scans (the reader uses the largest `flushedSeq` across pages; same argument
  per page).

## Maintenance notes

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

Sync commit: the D67 commit (`perf(read)`: the covering manifest cache, `loadLatestFrom`, read-ahead, 16 KiB blocks). When a covered function changes after it, update the model so
each action is still one awaited effect in the code, rerun, and show each invariant still fails on a
broken variant before moving the sync commit. For drift that skipped this skill, use
`.agents/skills/maintain-formal-verification-skill/SKILL.md`.
