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
- Only one writer acknowledges for a database. A writer fences in with a new epoch when it starts
  (`fenceWriter`), and a commit from an older epoch is refused (`WriterFencedError`).

| Enforced by                                                                      | Checked by                                                                                                                   |
| -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `DbWriter.commitBatch` under the commit lock; `MemTable.commit` (`mem-table.ts`) | `db-writer.test.ts`, `mem-table.test.ts`; `NoAckedWriteLost.tla` (`Write`, `Crash`, `Restart`)                               |
| `fenceWriter`, `commitL0Flush`'s epoch check (`lsm-core/src/manifest.ts`)        | `db-writer.test.ts` ("a stale writer cannot commit after a newer writer starts"); `ManifestCasFencing.tla` (`NoStaleCommit`) |

## A write is flushed

A write is **flushed** when a manifest with `lastL0Seq` at or above its sequence number exists.
`DbWriter.flushThen` does three things in this order, and the order is the invariant:

1. **Upload** an SST of the rows in `(flushed_seq, cut]` under a new id (`SstStore.write`). A flush
   writes, per key, only the versions a read can still reach (`keepReachableVersions`).
2. **Commit** a manifest that names the SST and sets `lastL0Seq = cut`
   (`ManifestStore.updateFrom(commitL0Flush)`, a create-if-absent of the next manifest id).
3. **Record** `flushed_seq = cut` in SQLite (`MemTable.markFlushed`). The rows stay, for
   transactions, until a separate trim.

A crash between any two steps loses nothing: before step 2 the rows are still unflushed and the SST
is an orphan GC removes; before step 3 the next start reads `lastL0Seq` from the manifest.

| Enforced by                                                                         | Checked by                                                                                                                                                                                                                                                  |
| ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DbWriter.flushThen` (`db-writer.ts`); `ManifestStore.create` (`manifest-store.ts`) | `db-writer.test.ts` ("a crash between the manifest commit and the memtable truncate loses and duplicates nothing"); `fault-injection.test.ts`; `NoAckedWriteLost.tla` (`truncateBeforeCommit` fails); `ManifestCasFencing.tla` (`CreateOnce`, `CommitOnce`) |
| `keepReachableVersions` (`lsm-core/src/row-resolution.ts`)                          | `row-resolution.test.ts` (against the oracle for any flush window and any later snapshot)                                                                                                                                                                   |

## What each read may see

Every read resolves a key from two places: rows the writer holds, and the SSTs of one manifest.
What differs is which rows it asks the writer for and which manifest it may use.

| Read             | Sees                                                                                | May miss                                                                     | Calls the writer                                         |
| ---------------- | ----------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | -------------------------------------------------------- |
| `Latest`         | Every write acknowledged before the read started                                    | Nothing acknowledged before it started                                       | Always (a probe)                                         |
| `AtLeast(seq)`   | At least the write with sequence number `seq`                                       | Writes after `seq`, as a `Flushed` read may                                  | Only when the manifest it would use does not cover `seq` |
| `Flushed`        | Every write flushed more than `manifestRefreshInterval` (1 s) ago                   | Unflushed writes: up to `maxL0Age` (60 s) of them, plus the refresh interval | Never                                                    |
| At a snapshot    | Exactly the database as of the snapshot's manifest and sequence number              | Everything after it, by design                                               | Never                                                    |
| In a transaction | Exactly the database as of the transaction's begin (`readSeq`), plus its own writes | Everything committed after its begin, by design                              | Always                                                   |

**`Latest`.** The writer's probe returns, from one SQLite read, a snapshot (`last_seq`),
`flushed_seq`, and the key's rows above `flushed_seq`. The read then uses a manifest `M` with

> `flushed_seq <= M.lastL0Seq <= snapshot`

and takes the newest version at or below the snapshot. The lower bound means `M` holds every row
the writer no longer returned. The upper bound means no flush or compaction after the snapshot
shaped `M`. A cached manifest that fits is used however long ago it was loaded, up to half of
`gcMinAge`. When only a manifest past the snapshot can be found, the read asks the writer again.

**`AtLeast(seq)`** is `Flushed` when the manifest a `Flushed` read would use right now already
covers `seq`, and `Latest` otherwise. It never loads a manifest to find out.

**`Flushed`** reads the manifest at its own `lastL0Seq`. Two `Flushed` reads through one Worker
isolate never go backwards; through different isolates they may, by up to the refresh interval.

**A snapshot** is a manifest id and a sequence number. An unpinned snapshot is readable while GC
keeps its manifest: at least `gcMinAge` (1 h) after a newer manifest replaced it. After that the
read fails with `SnapshotExpired`. A pinned snapshot (`DatabaseLifecycle.pinSnapshot`) holds a
checkpoint, and stays readable until the checkpoint expires or is deleted.

**A transaction** reads at the sequence number of its begin: the manifest that was flushed then,
plus the rows the writer keeps above it. That is exact while the writer still keeps those rows:
at least `transactionRetentionMs` (60 s), less only if `maxRetainedBytes` (64 MiB) of newer
flushed rows pushed them out. After that, reads and the commit fail with `SnapshotTooOld`. A
`SerializableSnapshot` commit succeeds only if no key or range it read was written since its begin,
checked under the commit lock against the writer's rows.

| Enforced by                                                                                                     | Checked by                                                                                                                                                                                         |
| --------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DbClient.pointView`, `scanView`, `coveredByManifest` (`db-client.ts`); `DbReader.manifestFor` (`db-reader.ts`) | `db-client.test.ts` (a `Map` oracle at every consistency; the two tests named in the worked example); `NoAckedWriteLost.tla` (`LatestSeesAcked`; `nonAtomicProbe` and `noSnapshotUpperBound` fail) |
| `ManifestCache.covering`, `recent`, `current` (`manifest-cache.ts`); `ManifestReader.loadLatestFrom`            | `manifest-store.test.ts`; `ManifestCasFencing.tla` (`LoadFindsHead`, `LoadFromKnownFindsHead`)                                                                                                     |
| `DbClient.beginTransaction`, `getAt`, `scanAt`; `DbWriter.commitTransaction`; `MemTable.writtenAfter`, `trim`   | `db-transaction.test.ts` (serializability over random interleavings with flushes and compactions); `Transactions.tla` (`Serializable`)                                                             |

## When GC may reclaim an SST

GC deletes an SST only when all four hold:

1. The newest manifest does not reference it.
2. No manifest that was replaced less than `gcMinAge` (1 h) ago references it. Age is measured
   from when the **next** manifest was written, not from the manifest's own write: a manifest that
   stayed newest for a day and was replaced a minute ago is one minute old.
3. No manifest a checkpoint pins references it. A fork's hold on its source, a pinned snapshot,
   and an operator's checkpoint are all checkpoints.
4. Its id is older than the manifest's `gcSstBoundary`, which GC advances to `now - gcMinAge` in
   a manifest commit before it deletes. A flush or compaction whose SST is below the boundary is
   refused at commit (`SstBelowGcBoundaryError`), so an upload that stalled for an hour cannot
   commit an SST GC has already removed.

Readers keep their side of rule 2: a Worker isolate uses a cached manifest for at most half of
`gcMinAge` after loading it, and caches a manifest only if it was the newest when loaded.

| Enforced by                                                                                                                               | Checked by                                                                                                                                                         |
| ----------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `GarbageCollector.collect` (`garbage-collector.ts`); `planManifestCollection`, `planSstCollection`, `prepareGarbageCollection` (lsm-core) | `garbage-collector.test.ts`, `garbage-collection-plan.test.ts`; `GcSafety.tla` (`ReadableManifestsComplete`, `PinnedManifestsComplete`; four broken variants fail) |
| The boundary check in `commitL0Flush` and `commitCompaction`; `ManifestStore.create`'s re-read of the manifest boundary                   | `manifest.test.ts`, `manifest-store.test.ts`; `ManifestCasFencing.tla` (`CommitOnce`; `noBoundaryReread` fails)                                                    |
| `ManifestCache`'s hold (`maxHeldMs`), `forget` on a missing SST                                                                           | `manifest-store.test.ts` ("...never past half of gcMinAge"), `db-client.test.ts` ("a read that finds an SST of its cached manifest missing...")                    |

## Worked example: the stale manifest after a compaction

Two bugs found while v1 was finished came from the same place: a read and a compaction meeting
across a manifest. Both passed every test and soak until the interaction was modeled or reviewed.
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
newest from any stored manifest.

**The check to apply to a change.** For anything that makes a reader keep or reuse a manifest,
answer both:

- Which rows may the manifest be missing, or have dropped, relative to what the read asked the
  writer for? (The two bounds.)
- For how long after it was replaced can the reader still be using it, and does GC wait longer?
  (Rule 2.)

## Where the detail is

- Each formal model's skill (`.agents/skills/verify-*`) lists the actions it models, the code each
  maps to, its bounds, and its maintenance notes. `vp run verify` runs all 28 checks.
- [`design-decisions.md`](design-decisions.md) records why each rule is as it is: D21 (the atomic
  probe), D29 (readers and GC), D33 (GC boundaries), D56 (manifest commits), D58 (transactions),
  D67 and D70 (the read path), D74 (the writer's counters).
- [`coverage-matrix.md`](coverage-matrix.md) maps every design item to code, tests, trace
  assertions, and models.
