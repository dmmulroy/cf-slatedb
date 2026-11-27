# Invariants

What shaledb promises, in the order a row lives: acknowledged, flushed, read, reclaimed. Each
promise names the code that enforces it and the tests and models that check it. Read this before
changing the write path, a read path, or garbage collection: the features are safe one at a time,
and the risk is in how they meet.

Three structural rules carry everything below.

1. **One acknowledgement point.** A write is acknowledged by one SQLite transaction in the
   database's writer Durable Object, and by nothing else.
2. **Objects in R2 are immutable, and published by create-if-absent.** An SST is written once
   under a new id. A manifest is created once under the next id. Nothing is overwritten.
3. **Manifest edits are pure functions.** Every change to what a database consists of is a function
   from one manifest to the next (`packages/lsm-core/src/manifest.ts`), committed by creating the
   next manifest. The writer, the compactor, GC, and the lifecycle never edit shared state in
   place.

Defaults quoted below are `DEFAULT_LSM_SETTINGS` (`packages/lsm-core/src/lsm-settings.ts`).

## A write is acknowledged

A write is **acknowledged** when `DbWriter.write` returns its receipt. Before that, one SQLite
transaction (`MemTable.commit`) has inserted every row of the batch and advanced `last_seq`. The
batch gets one sequence number; all of it is visible, or none.

- An acknowledged write survives the writer's eviction or crash: Durable Object SQLite holds it.
- It is not yet in R2. It reaches R2 at the next flush: when `l0SstSize` (64 MiB) is unflushed,
  when the oldest unflushed row is `maxL0Age` (60 s) old, or on request.
- Only one writer flushes for a database. A writer fences in with a new epoch when it starts
  (`fenceWriter`), and a flush from an older epoch is refused at its manifest commit
  (`commitL0Flush`, `WriterFencedError`). A refusal marks only the phase it was made from as
  fenced: a writer that started again in the same object meanwhile keeps serving (D115).
- An acknowledged write has a flush alarm, or a flush running, whatever request is interrupted
  and whenever the object is evicted (`Effect.ensuring(scheduleFlush)`, D74b, D102). One gap: a
  write evicted between its SQLite commit and its `scheduleFlush` was never acknowledged, and its
  row waits for the next request to the object (D109).

| Enforced by                                                                      | Checked by                                                                                                                                                                                                                                           |
| -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DbWriter.commitBatch` under the commit lock; `MemTable.commit` (`mem-table.ts`) | `db-writer.test.ts`, `mem-table.test.ts`; `NoAckedWriteLost.tla` (`Write`, `Crash`, `Restart`); `WriterObject.tla` at await granularity (`NoAckedWriteLost`, `AlarmCovers`, `AckedFlushed`)                                                          |
| `fenceWriter`, `commitL0Flush`'s epoch check (`lsm-core/src/manifest.ts`)        | `db-writer.test.ts` ("a stale writer cannot commit after a newer writer starts, and its SST stays unreferenced"); `ManifestCasFencing.tla` (`NoStaleCommit`; `noWriterEpochCheck` fails); `WriterObject.tla` (`NeverSelfFenced`; `beforeD115` fails) |

## A write is flushed

A write is **flushed** when a manifest with `lastL0Seq` at or above its sequence number exists.
`DbWriter.flushThen` does three things in this order, and the order is the invariant:

1. **Upload** an SST of the rows in `(flushed_seq, cut]` under a new id (`SstStore.write`). A flush
   writes, per key, only the versions a read can still reach (`keepReachableVersions`).
2. **Commit** a manifest that names the SST and sets `lastL0Seq = cut`
   (`ManifestStore.updateFrom(commitL0Flush)`, a create-if-absent of the next manifest id).
3. **Record** `flushed_seq = cut` in SQLite (`MemTable.markFlushed`). The rows stay, for
   transactions, until `MemTable.trim`, a separate SQLite transaction run after each flush.

A crash between any two steps loses nothing: before step 2 the rows are still unflushed and the SST
is an orphan GC removes; before step 3 the next start records the manifest's `lastL0Seq` as
`flushed_seq`. A commit that lands and loses its response leaves the writer running with a stale
`flushed_seq`: its next commit is refused (`FlushedSeqStaleError`), and the writer records the
manifest's `lastL0Seq` and flushes again from it, so no change is exported twice (D113,
`ChangeFeed.tla`).

| Enforced by                                                                                       | Checked by                                                                                                                                                                                                                                                                                                                                                           |
| ------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DbWriter.flushThen` (`db-writer.ts`); `ManifestStore.updateFrom`, `create` (`manifest-store.ts`) | `db-writer.test.ts` ("a crash between the manifest commit and the memtable truncate loses and duplicates nothing"); `fault-injection.test.ts` ("a crash between the L0 upload and the manifest commit leaves an orphan GC removes, and loses nothing"); `NoAckedWriteLost.tla` (`truncateBeforeCommit` fails); `ManifestCasFencing.tla` (`CreateOnce`, `CommitOnce`) |
| `keepReachableVersions` (`lsm-core/src/row-resolution.ts`)                                        | `row-resolution.test.ts` ("drops only versions no read at or after the window's newest row can reach")                                                                                                                                                                                                                                                               |

## What each read may see

Every read resolves a key from two places: rows the writer holds, and the SSTs of one manifest.
What differs is which rows it asks the writer for and which manifest it may use.

| Read             | Sees                                                                                | May miss                                                                                                     | Calls the writer                                                           |
| ---------------- | ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------- |
| `Latest`         | Every write acknowledged before the read started                                    | Nothing acknowledged before it started                                                                       | Always (a probe)                                                           |
| `AtLeast(seq)`   | At least the write with sequence number `seq`                                       | Writes after `seq`, as a `Flushed` read may                                                                  | Only when the manifest a `Flushed` read would use now does not cover `seq` |
| `Flushed`        | Every write flushed more than about `manifestRefreshInterval` (1 s) before the read | Unflushed writes (normally at most `maxL0Age`, 60 s, of them), and newer flushes within the refresh interval | Never                                                                      |
| At a snapshot    | Exactly the database as of the snapshot's manifest and sequence number              | Everything after it, by design                                                                               | Never                                                                      |
| In a transaction | Exactly the database as of the transaction's begin (`readSeq`), plus its own writes | Everything committed after its begin, by design                                                              | Always                                                                     |

**`Latest`.** The writer's probe returns, from one SQLite transaction (`MemTable.consistently`), a
snapshot (`last_seq`), `flushed_seq`, and the key's rows above `flushed_seq`. The read then uses a
manifest `M` with

> `flushed_seq <= M.lastL0Seq <= snapshot`

and takes the newest version at or below the snapshot. The lower bound means `M` holds every row
the writer no longer returned. The upper bound means no flush or compaction after the snapshot
shaped `M`. A cached manifest that fits is used however long ago it was confirmed, up to half of
`gcMinAge` after it was loaded (`ManifestCache.covering`). When only a manifest past the snapshot
can be found, the read asks the writer again, at most `LATEST_READ_ATTEMPTS` (3) times in all, then
fails with the retryable `StorageUnavailable`. A scan reads the writer's rows in pages at one
snapshot and takes the largest `flushed_seq` of its pages as the lower bound, so a flush between
pages leaves no manifest that fits, and the scan starts again.

**`AtLeast(seq)`** is `Flushed` when the manifest a `Flushed` read would use right now already
covers `seq`, and `Latest` otherwise. It never loads a manifest to find out.

**`Flushed`** reads the manifest at its own `lastL0Seq` (`ManifestCache.current`): the cached one
while it was confirmed to hold every flushed row less than `manifestRefreshInterval` ago (by its
load, or by a `Latest` read's covering hit), otherwise a fresh load. One `Flushed` read after
another in the same Worker isolate never sees an older manifest: the cache never replaces a manifest
with an older one. Through different isolates they may go backwards, by about the refresh interval.

**A snapshot** is a manifest id and that manifest's `lastL0Seq`. `DbClient.snapshot` takes the
manifest a `Flushed` read would use and writes nothing. It is readable while GC keeps that manifest:
`gcMinAge` (1 h) after a newer manifest replaced it. The cache may serve a manifest a compaction
replaced up to half of `gcMinAge` earlier, so from the moment it is taken an unpinned snapshot is
guaranteed only half of `gcMinAge`. After that a read may fail with `SnapshotExpired`.
`DatabaseLifecycle.pinSnapshot` flushes, then adds a checkpoint on the resulting manifest that
expires after the given lifetime; the snapshot stays readable until it expires or is deleted.

**A transaction** reads at `readSeq`, the writer's `last_seq` at its begin, from the manifest the
writer's last flush committed (`flushed_manifest_id`) plus the rows the writer keeps in
`(retained_from_seq, readSeq]` (`DbWriter.probeAt`). That is exact while the writer keeps every row
above `readSeq`. A trim keeps a flush's rows at least `transactionRetentionMs` (60 s) after that
flush, less only when more than `maxRetainedBytes` (64 MiB) of flushed rows are kept, and never
removes the newest flush. Once a trim passes `readSeq`, the commit fails with `SnapshotTooOld`, and
so does a read whose begin manifest no longer covers `retained_from_seq` or has been collected by
GC. Every commit checks, under the commit lock, that no key it writes was written after `readSeq`
(`MemTable.writtenAfter`). A `SerializableSnapshot` commit also checks every key and range it read.

| Enforced by                                                                                                                             | Checked by                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| --------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DbClient.pointView`, `scanView`, `coveredByManifest`, `LATEST_READ_ATTEMPTS` (`db-client.ts`); `DbReader.manifestFor` (`db-reader.ts`) | `db-client.test.ts` ("Latest and AtLeast reads see every acknowledged write; Flushed reads see the last flush; scans agree", against a `Map` oracle; the two tests named in the worked example); `NoAckedWriteLost.tla` (`LatestSeesAcked`; `nonAtomicProbe` and `noSnapshotUpperBound` fail)                                                                                                                                                 |
| `ManifestCache.covering`, `recent`, `current` (`manifest-cache.ts`); `ManifestReader.loadLatestFrom`                                    | `manifest-store.test.ts` ("a covering hit vouches for the cached manifest: Flushed reads use it for another refresh interval, not longer (D67)", "loads the newest manifest from the writer's last flush, never that flush's superseded manifest (D67)"); `ManifestCasFencing.tla` (`LoadFindsHead`, `LoadFromKnownFindsHead`)                                                                                                                |
| `DbClient.beginTransaction`, `getAt`, `scanAt`; `DbWriter.commitTransaction`; `MemTable.writtenAfter`, `trim`                           | `db-transaction.test.ts` ("SerializableSnapshot: every committed transaction read what the database held at its commit", over random interleavings with flushes, compactions, and trims; "ends a transaction only once its rows are trimmed, and old callers at the first flush"); `mem-table.test.ts` ("trims flushed rows past retention or the byte bound, never the newest flush or unflushed rows"); `Transactions.tla` (`Serializable`) |

## When GC may reclaim an SST

`GarbageCollector.collect` deletes an SST of the database's own prefix only when all four hold. (A
fork's SSTs that live under its source's prefix are never its candidates.)

1. The newest manifest does not reference it.
2. No manifest that was replaced less than `gcMinAge` (1 h) ago references it. Age is measured
   from when the **next** manifest was written, not from the manifest's own write: a manifest that
   stayed newest for a day and was replaced a minute ago is one minute old.
3. No manifest a checkpoint pins references it. A fork's hold on its source, a pinned snapshot,
   and an operator's checkpoint are all checkpoints.
4. Its id (a ULID) was created before both `now - gcMinAge` and the `gcSstBoundary` of the
   manifest GC committed. GC advances that boundary to `now - gcMinAge` in a manifest commit before
   it lists the manifests it keeps and deletes, and only when some old SST is unreferenced by the
   newest manifest. A flush or compaction whose SST was created before the boundary is refused at
   commit (`SstBelowGcBoundaryError`), so an upload that stalled past `gcMinAge` cannot commit an
   SST GC may have removed.

Readers keep their side of rule 2: a Worker isolate uses a cached manifest for at most half of
`gcMinAge` after loading it (`maxHeldMs`), and caches a manifest only if it was the newest when
loaded. Snapshots and transactions name a manifest by id instead; when GC has collected it, their
reads fail (`SnapshotExpired`, `SnapshotTooOld`) rather than read a partial manifest.

| Enforced by                                                                                                                                                                                              | Checked by                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GarbageCollector.collect` (`garbage-collector.ts`); `planManifestCollection`, `planSstCollection`, `hasSstCollectionCandidates`, `prepareGarbageCollection` (`lsm-core/src/garbage-collection-plan.ts`) | `garbage-collector.test.ts` ("keeps young manifests and their SSTs, then collects them once older than gcMinAge", "keeps the SSTs of a checkpointed manifest however old it is"); `garbage-collection-plan.test.ts` ("measures a manifest's age from when it was superseded, not when it was written"); `GcSafety.tla` (`ReadableManifestsComplete`, `PinnedManifestsComplete`; `noBoundaryCheck`, `ownUploadAge`, `ignorePins`, `dropDeletesObject` fail) |
| `belowGcBoundary` in `commitL0Flush` and `commitCompaction` (`lsm-core/src/manifest.ts`); `ManifestStore.create`'s re-read of the manifest boundary                                                      | `manifest.test.ts` ("rejects new SSTs created before GC's boundary, which only moves forward"); `garbage-collector.test.ts` ("collects an old orphan, and a late commit of it then fails instead of referencing a deleted SST"); `manifest-store.test.ts` ("does not commit a stale create that reuses an id GC collected (RFC 0026 boundary)"); `ManifestCasFencing.tla` (`CommitOnce`; `noBoundaryReread` fails)                                         |
| `ManifestCache`'s hold (`maxHeldMs`), `forget` on a missing SST                                                                                                                                          | `manifest-store.test.ts` ("serves a covering manifest from cache past the refresh interval, never one missing flushed rows, and never past half of gcMinAge (D67)"); `db-client.test.ts` ("a read that finds an SST of its cached manifest missing drops the manifest, so the retry succeeds (D67)")                                                                                                                                                       |

## Worked example: the stale manifest after a compaction

Two bugs found while v1 was finished came from the same place: a read and a compaction meeting
across a manifest. Neither showed in a test or a soak; a model found one and a review the other.
They show how to use the rules above.

**A manifest too new for the read (D70).** A `Latest` read had the lower bound above and no upper
bound.

1. Key `k` is written with sequence number 1 and flushed.
2. A read asks the writer: snapshot 1, `flushed_seq` 1, no unflushed rows.
3. `k` is written again (sequence number 2) and flushed, and a compaction merges both SSTs. A
   compaction keeps each key's newest version, so the newest manifest holds only version 2.
4. The read loads that manifest. Version 2 is past its snapshot and is skipped. Version 1 is
   gone. The read answers "not found" for a key that had a value at every moment.

Each step is correct alone. The fix is the upper bound: the read refuses a manifest with
`lastL0Seq` above its snapshot and asks the writer again. `NoAckedWriteLost.tla` did not model
compaction until then; with it, the variant `noSnapshotUpperBound` reproduces the four steps.
Tests: `db-client.test.ts`, "a Latest read that finds only a manifest flushed past its snapshot
asks the writer again, never missing the key" and "a Latest scan overtaken by a flush between its
pages of unflushed rows starts again".

**A manifest too old for GC (D67, caught in review).** To save a round trip, a reader was given
the id of the manifest the writer's last flush committed, loaded it, and cached it for half of
`gcMinAge`. That manifest holds every flushed row, so reads through it were correct. But a
compaction replaces a flush's manifest within seconds, and GC's rule 2 counts from the
replacement. A database written, compacted, and then idle would hand a new isolate a manifest
replaced up to an hour earlier, and the isolate would keep reading it for another 30 minutes while
GC deleted its SSTs. The fix: the writer's manifest is only where the search for the newest
starts (`ManifestReader.loadLatestFrom`), and the cache holds only a manifest that was the newest
when loaded. `ManifestCasFencing.tla`'s `LoadFromKnownFindsHead` checks that the search finds the
newest from any stored manifest (`knownIgnoresBoundary` fails it).

**The check to apply to a change.** For anything that makes a reader keep or reuse a manifest,
answer both:

- Which rows may the manifest be missing, or have dropped, relative to what the read asked the
  writer for? (The two bounds.)
- For how long after it was replaced can the reader still be using it, and does GC wait longer?
  (Rule 2.)

Applied to transactions, the second question finds an open gap. A transaction's begin manifest is
the writer's last flush manifest, which a compaction replaces within seconds. On a database idle
for `gcMinAge` after a compaction, while its writer stays up, GC has collected that manifest, so
every read of every new transaction fails with `SnapshotTooOld` until the next flush. Nothing is
read wrongly; the retry just never succeeds. It is reported and not yet fixed.

## Where the detail is

- Each formal model's skill (`.agents/skills/verify-*`) lists the actions it models, the code each
  maps to, its bounds, and its maintenance notes. `vp run verify` runs all 28 checks.
- [`design-decisions.md`](design-decisions.md) records why each rule is as it is: D21 (the atomic
  probe), D29 (`Flushed` lag), D33 (GC boundaries and `gcMinAge` for readers), D56 (manifest commits), D58 (transactions),
  D67 and D70 (the read path), D74 (the writer's counters).
- [`coverage-matrix.md`](coverage-matrix.md) maps every design item to code, tests, trace
  assertions, and models.
