---
name: verify-manifest-cas-fencing
description: "Model-check shaledb's manifest CAS and writer/compactor epoch fencing with TLA+ (TLC), including optimistic commits from a committer's own view, the background LATEST hint, and GC deleting manifests. Use after any change to ManifestStore.update/updateFrom/create, ManifestReader.loadLatest, the LATEST hint, ObjectStore.putIfAbsent, GC's manifest boundary, or the manifest edits fenceWriter, fenceCompactor, commitL0Flush, commitCompaction, and before trusting a fencing or CAS change."
---

# Verify manifest CAS and fencing

## What is verified, in plain words

- **CreateOnce**: nothing ever overwrites an existing manifest.
  Harm if broken: two writers both believe they committed, and one's flush silently disappears.
- **CommitOnce**: no commit is ever reported at an id that was committed before, even after GC
  deleted that manifest and create-if-absent no longer remembers it (the RFC 0026 boundary
  re-read). Harm if broken: a committer with an old view "commits" behind the newest manifest and
  its edit is never seen.
- **LoadFindsHead**: `loadLatest` finds the newest manifest however far the `LATEST` hint lags (it
  is written in the background and may land late, out of order, or never) and whichever manifests
  GC deleted. Harm if broken: readers and committers act on an old manifest.
- **LoadFromKnownFindsHead**: `loadLatestFrom(k)` finds the newest manifest from any manifest `k`
  a commit was reported at (the writer's last flush, which a reader learns from the writer's
  probe, D67), however long ago, whatever was committed since and whichever manifests GC deleted.
  Harm if broken: a reader caches a superseded manifest as if it were the newest and keeps
  reading it after GC deleted its SSTs.
- **NoStaleCommit**: every committed edit was made by the writer or compactor epoch its base
  manifest names, so after a newer epoch commits, a stale writer or compactor commits nothing.
  Harm if broken: a fenced writer keeps writing next to its successor (split brain).
- **FencedWriterOrphansUnreferenced**: an SST uploaded by a writer that was then fenced never
  appears in the newest manifest; it stays an orphan for GC.
- **AckedDataVisible** (manifest CAS linearizability): every flush the code reported as
  committed is readable from the newest manifest, under any interleaving of writers, compactors,
  conflicting creates, and retries, including compactions whose inputs changed.
  Harm if broken: an acknowledged write is lost.
- **NoDoubleReference**: a compaction replaces exactly its inputs, never leaving data both in L0
  and in its output.

## Run it

```sh
vp run verify                                     # every formal check
node tools/formal-verification/src/verify.ts manifest-cas-fencing   # just this one
```

The first run downloads a pinned JDK 21 and `tla2tools.jar` 1.7.4 into `.tlc/` (checksummed).
Each TLC run has a hard time limit; a run that times out fails the verification. The whole check
takes about 10 seconds.

Expected output: nine `✓` lines. The code as written passes all three configs, and each broken
variant fails with its invariant. Any `✗` is a finding: read the next section.

## Read a result

Read every result with the pstack `/statechart` skill (`.agents/skills/statechart/SKILL.md`; open
it by path, it is not invoked automatically). Point it at a scratch copy of
`tools/formal-verification/specs/ManifestCasFencing.tla` and the config of the failing run, with
`JAVA` and `TLA2TOOLS` set to the binaries under `.tlc/` so it does not install its own. It
reports each broken rule as its shortest path. The state space is far above its 500-state chart
budget, so it draws only bug paths. Use a small config when you want a chart.

Telling a real bug from a modeling error: replay the path against the code, one action per step.
`WriterUpload`, `JobUpload` are SST uploads; `*Load` steps are `ManifestReader.loadLatest` (hint and
boundary read, probe from `max(hint, boundary + 1)`); `LoadLatestFrom(k)` is
`ManifestReader.loadLatestFrom` (the boundary, `k`, and `k + 1` read together; probe forward from
`k` when it is stored and above the boundary, else `loadLatest`); `*Create` steps are one
`ManifestStore.create` (`ObjectStore.putIfAbsent` of `manifest/<view + 1>` and the boundary re-read)
inside `ManifestStore.update` or `updateFrom`, with the edit (`fenceWriter`, `commitL0Flush`,
`fenceCompactor`, `commitCompaction`) applied to the view: the loaded head, or for a writer's flush
its own last committed manifest. `GcAdvanceBoundary`/`GcDeleteManifest` are `advanceManifestBoundary`
and GC's manifest deletes; `HintLands`/`HintLost` are the background `advanceLatestHint`. If a step cannot happen in the code, the model is wrong; fix the model and say why. If every
step can happen, it is a bug in the code: report the path in the code's words, and do not weaken
an invariant to make it pass.

## Coverage record

| Model element | Code |
| --- | --- |
| `Create`, `CanCreate` | `ObjectStore.putIfAbsent` (`packages/lsm-cloudflare/src/object-store.ts`), `ManifestStore.create` (`manifest-store.ts`) |
| `*Load` + `*Create` retry on conflict | `ManifestStore.update` CAS loop (`commitEdit`, `manifest-store.ts`) |
| `WriterUpload` → `WriterFlushCreate` without a load; `wOpt` | `ManifestStore.updateFrom` from the writer's own view (`DbWriter.flushNow`, `DbWriter.seal`); a non-write outcome from that view reloads |
| `CreateOutcome` (`Exists` / `Collected` / `Created`) | `ManifestStore.create`: `putIfAbsent`, then `loadManifestBoundary`; at or below it is a conflict |
| `LoadLatest`, `Start`, `ProbeFrom` | `ManifestReader.loadLatest` (`manifest-reader.ts`): probe from `max(hint, boundary + 1)`; a missing start lists |
| `LoadLatestFrom(k)` for `k` in `reported` | `ManifestReader.loadLatestFrom` (`manifest-reader.ts`), called by `ManifestCache.covering` with the writer's `flushedManifestId` (`manifest-cache.ts`, D67) |
| `HintLands`, `HintLost` | `advanceLatestHint` run by `BackgroundWork` (`background-work.ts`) |
| `GcAdvanceBoundary`, `GcDeleteManifest` | `ManifestStore.advanceManifestBoundary`, GC's manifest deletes (`garbage-collector.ts`) |
| `Head` | the largest existing manifest id |
| `WriterFence*` | `fenceWriter` (`packages/lsm-core/src/manifest.ts`) via `ManifestStore.update` (writer init, Phase 4) |
| `WriterFlushCreate` | `commitL0Flush`: already-applied check, writer-epoch check, prepend to L0 |
| `CompactorFence*` | `fenceCompactor` via `ManifestStore.update` (compactor init, Phase 6) |
| `JobPlan` | `planCompactions` picks L0 inputs and runs from the head |
| `JobCommitCreate` | `commitCompaction`: compactor-epoch check, inputs-present check, replace inputs |

Code-level tests that bind the model's steps to the code (each fails when the matching check is
removed from the real code): `packages/lsm-core/src/manifest.test.ts` ("fences a stale writer",
"fails when they changed") and `packages/lsm-cloudflare/src/manifest-store.test.ts` ("fences a
stale writer", "serializes concurrent edits", "writes nothing for an already-applied edit",
"commits from its own view with one create and no load", "reloads instead of committing when its
own view's next id was collected by GC", "trusts only a written commit from its own view",
"advances the LATEST hint in the background and never moves it back", "finds the newest manifest
when the hint lags below manifests GC deleted", "loads the newest manifest from the writer's last
flush, never that flush's superseded manifest (D67)"). Mutations shown caught (D67): `loadLatestFrom`
returning the known manifest without probing forward, and ignoring the boundary. Mutations shown
caught (D56): the boundary
re-read removed, a non-write outcome trusted from the own view, the probe started at the hint
alone, the hint written unconditionally, `updateFrom` always loading. The `flush` scenario asserts
a flush commits from its own view with one create and no `ManifestReader.loadLatest` span.

## Bounds and assumptions

- `ManifestCasFencing.cfg`: 2 writers, 2 compactors, 1 job each, 2 flushes, 9 manifest ids, no GC
  (~750k states). `ManifestCasFencingRace.cfg`: 1 writer, 1 compactor with 2 concurrent jobs,
  2 flushes, 8 ids, no GC (~4k states). The race config is the only one where two live jobs race;
  with one job per compactor, a second compactor fences the first before they can.
  `ManifestCasFencingGc.cfg`: 1 writer, 1 compactor, 1 job, 2 flushes, 7 ids, with GC advancing the
  boundary and deleting any manifest at or below it (a superset of what GC's age and pin rules
  allow) and background hint writes that land in any order or never (~370k states).
- The id bound must cover init + every fence + every flush + every compaction commit; with fewer
  ids, later commits cannot happen and a check can pass vacuously. Raise `MaxId` with the other
  bounds.
- Assumed, not modeled: R2 create-if-absent is atomic (verified against local R2 by
  `test/object-store/object-store.workerd.ts` and on real R2 by the `create-race` scenario);
  `loadLatest` is one step (a probe racing a commit returns a slightly older head, which the next
  create's conflict corrects); memtable truncation (the write-loss check covers it). The hint is
  modeled as able to move back, a weaker guarantee than the code's conditional write, so safety
  does not depend on that.
- Broken variants (`Broken` constant): `noWriterEpochCheck` → `NoStaleCommit`; `blindCreate` →
  `CreateOnce`; `noInputsCheck` → `AckedDataVisible` (race config); `noBoundaryReread` →
  `CommitOnce`, `hintOnlyProbe` → `LoadFindsHead`, and `knownIgnoresBoundary` →
  `LoadFromKnownFindsHead` (GC config). `hintOnlyProbe` is the reader as it was before D56: with a
  lagging hint and a checkpoint keeping manifest `h` while GC deleted `h + 1`, it stopped at `h`.
  `knownIgnoresBoundary` makes the same mistake from the writer's last flush.
- A compactor that fences in again after being fenced (D78) is one more `Compactors` element:
  each fence-in gets a fresh epoch, and an older epoch's jobs fail `commitCompaction`. That is the
  case the two-compactor config already checks.
- Not modeled: the compactor's own Durable Object state. The defect D78 fixed was an older epoch
  written back over a newer one in that state, plus two fence-ins for one start. It made the
  compactor stop for good, a liveness failure, and it never let a stale compaction commit, because
  every commit carries its job's epoch. The model checks the manifest, where nothing was wrong,
  so no broken variant here can express it. `compactor.test.ts` checks the state instead: one
  fence-in for overlapping first wakes, no write over a newer epoch, and a fenced compactor that
  fences in again, each shown failing under a mutation.
- Not a broken variant: trusting a failure or no-op from the writer's own view. In this model the
  writer's view can never give a wrong non-write answer (its view holds its own epoch and lacks its
  new SST), so such a variant would pass; the code rechecks them for generic edits (a checkpoint
  removal against an old view), which `manifest-store.test.ts` covers.

## Maintenance notes

- D103: `commitCompaction` records the job's id (`recentCompactionJobIds`, the last 16) and
  treats a commit of a recorded id as already applied, returning the manifest unchanged, whatever
  the job's outputs. Before, only a job with outputs could be recognized. The model has no retried
  commit after success: `JobCommitCreate` runs once per job. The already-applied path writes
  nothing, so widening it cannot break `AckedDataVisible`, `NoDoubleReference`, or the fencing
  invariants. No model step changed; not rerun.

- D78: `Compactor.ensureState` is single-flight (`startLock`), and a fenced compactor fences in
  again on its next start (wake, requested collection, SST drop, or alarm). Every compactor state
  write is a read-modify-write (`updateState`). The fence-in is still one `ManifestStore.update`
  of `fenceCompactor`, now possibly repeated by one Durable Object, which the model already covers
  as further compactors. No step changed. Reran
  `node tools/formal-verification/src/verify.ts manifest-cas-fencing`: nine `✓`, and every broken
  variant fails with its invariant.

- D72: `loadLatestFrom` now also reads the `LATEST` hint in its first round and, when the known
  manifest was superseded, continues from the hint if that names a stored manifest further along
  (`KnownStart`), so a known manifest far behind costs no more than `loadLatest`. The manifest
  cache's `refresh` calls it with the manifest it holds. `LoadFromKnownFindsHead` is therefore
  checked from every stored manifest, not only reported commits. `loadLatest` split into reading
  the hint and boundary and `newestFromHint`; its steps are unchanged. Mutations caught by
  `manifest-store.test.ts`: no jump to the hint, a jump to a hint behind the known manifest,
  trusting a hint whose manifest is not stored, a refresh that always searches from the hint, and
  a superseded known manifest returned as the newest. Reran; every broken variant fails.

- D67 (v1 workstream 10): readers gained `ManifestReader.loadLatestFrom(knownId)`, which starts
  from the manifest the writer's last flush committed instead of the `LATEST` hint. Modeled as
  `LoadLatestFrom(k)` over every reported commit `k` (a superset of the writer's flushes), with the
  invariant `LoadFromKnownFindsHead` in all three configs and the broken variant
  `knownIgnoresBoundary` in the GC config. Like `loadLatest`, it is one step: its three reads are
  concurrent, and a read racing a commit returns a slightly older head. `loadLatest`'s forward
  probe moved into `probeForward`, shared by both; its steps are unchanged. Reran; every broken
  variant fails. The first, unmodeled version returned the known manifest itself, which is what
  `LoadFromKnownFindsHead` now rules out.

- D64 (v1 workstream 9): `decodeStoredManifest`, the reader's single decode point, refuses a
  manifest whose `formatVersion` is newer than the code's (`StoredManifestInvalid`). That is one
  more way a load fails before any write; the CAS and fencing steps are unchanged. No model step
  changed.

- D62 (v1 workstream 7): one more manifest edit, `dropSstFromManifest`, committed by
  `ManifestStore.update` (reload and reapply on conflict) from the compactor, under its planning
  lock; it changes no epoch, so fencing is unaffected. A compaction that read a dropped L0 SST fails
  `commitCompaction`'s exact-inputs check; one reading a dropped sorted run SST cannot be running
  (the compactor refuses the drop, and plans nothing while it holds the lock). No model step
  changed.
- D56 (v1 workstream 2): optimistic `updateFrom`, the background monotonic hint, `loadLatest` from
  `max(hint, boundary + 1)`, GC and hint actions, `CommitOnce`, `LoadFindsHead`, the GC config and
  two broken variants. Every step still maps to one awaited effect.
- Checked against `3226177` (Phases 4–7). `commitL0Flush` and `commitCompaction` gained refusals
  (`SstBelowGcBoundaryError`, sealed, removed) that only turn commits into failures; seal and removal
  increment `writerEpoch` like a fence. `ManifestStore.create` re-reads `gc/manifest.boundary` after
  create-if-absent and reports a conflict at or below it: that path needs GC to have deleted
  manifests, which this model does not include (no manifest is ever deleted here); it is checked by
  `manifest-store.test.ts` and assumed by `verify-gc-safety`. Checkpoint, GC-boundary, and
  fork-release edits change no field this model reads.

## Keeping it in step with the code

Sync commit: the D78 commit (`fix(compactor)`: one fence-in per start, state written by read-modify-write, a fenced compactor fences in again) (the model was last
confirmed to describe the code at this commit). When a covered file changed after it, diff it, update the model so each step still maps
to one awaited effect in the code, rerun, and show the affected invariant still fails on a broken
variant. Move the sync commit forward only after confirming the model describes the code at the
new commit. For drift that skipped this skill, use
`.agents/skills/maintain-formal-verification-skill/SKILL.md`.
