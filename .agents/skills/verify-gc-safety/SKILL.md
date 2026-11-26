---
name: verify-gc-safety
description: "Model-check that shaledb garbage collection never deletes an SST that the newest manifest, a recently superseded manifest a reader may still hold, or a checkpoint (including a fork's hold on its source) references, and never deletes an uploaded SST whose commit can still succeed. Use after any change to GarbageCollector.collect, the GC planning functions, gcSstBoundary handling in commitL0Flush/commitCompaction, checkpoints, or how readers hold manifests."
---

# Verify: GC safety

## What is verified, in plain words

- **ReadableManifestsComplete**: every SST referenced by the newest manifest, or by a manifest
  superseded less than `gcMinAge` ago (one a reader may still hold, design decision D29), is still
  stored. This covers uploads that were not yet committed: a flush or compaction can stall for any
  time between uploading its SST and committing it, and once committed the SST must exist. Harm if
  broken: reads (and the writer's next flush or the next compaction) hit a missing SST.
- **PinnedManifestsComplete**: every manifest a checkpoint in the newest manifest pins, which
  includes the checkpoint a fork holds on its source, is still stored with all its SSTs. Harm if
  broken: a fork or a backup loses data its source no longer references.
- **ForkReadsComplete** (`GcSafetyFork.cfg`, D77): every SST of the source manifest a fork read
  is still stored while the fork reads it and for `gcMinAge` after the fork's rewrite replaced its
  manifest, the time a reader of the fork (a snapshot, a cached manifest) may still hold that
  manifest. A fork releases its hold only when its own collection finds no kept manifest naming
  the source's SSTs. Harm if broken: a fork's snapshot or cached manifest fails with
  `StorageUnavailable` after the source collects or is removed.

## Run it

```sh
node tools/formal-verification/src/verify.ts gc-safety   # about 45 seconds
vp run verify                                            # every formal check
```

Expected: seven `✓` lines. The code as written passes both configs; `noBoundaryCheck`,
`ownUploadAge`, and `dropDeletesObject` fail `ReadableManifestsComplete`; `ignorePins` fails
`PinnedManifestsComplete`; `earlyForkRelease` (`GcSafetyFork.cfg`) fails `ForkReadsComplete`.

## Read a result

Read results with the pstack `/statechart` skill (`.agents/skills/statechart/SKILL.md`, opened by
path), on a scratch copy of `tools/formal-verification/specs/GcSafety.tla` and its config, with
`JAVA` and `TLA2TOOLS` pointing at `.tlc/`. Replay a reported path against the code, one action per
step:

| Action | Code |
| --- | --- |
| `Upload(s)` | `SstStore.write` in a flush (`DbWriter.flushNow`) or a compaction step (`CompactionExecutor.writeOutput`): a new ULID, the object uploaded |
| `CommitFlush(s)` | `ManifestStore.update(commitL0Flush)`; refused below `gcSstBoundary` (`SstBelowGcBoundaryError`) |
| `CommitCompaction(s, inputs)` | `CompactionExecutor.commit` → `commitCompaction` (exact inputs, boundary check) |
| `DropSst(s)` | `Compactor.dropSst` → `ManifestStore.update(dropSstFromManifest)` (D62): removes the reference only; the object is left to GC |
| `Discard(s)` | a refused commit: the SST stays an orphan |
| `Pin` / `Unpin(p)` | `addCheckpoint` / `removeCheckpoint` for checkpoints other than a fork's hold (`GcSafety.cfg` only) |
| `ForkStart` | `forkRange` / `holdForSuccessors`: the fork's hold, a checkpoint held by the fork (`hold` here), pinning the newest manifest; the fork's first manifest (`forkManifest`) reads its SSTs |
| `ForkRewrite` | the fork's full compaction commits in the fork's own manifest (`commitCompaction`): its newest manifest names none of the source's SSTs; the one before is superseded now |
| `ForkRelease` | the fork's `GarbageCollector.collect`: `releasableExternalRefs` over `planManifestCollection`'s retained manifests is non-empty only once the pre-rewrite manifest was superseded `gcMinAge` ago; then `ManifestStore.update(removeCheckpoint)` on the source (this step), then `removeExternalRef` on the fork |
| `GcListSsts` | `GarbageCollector.collect`: `now` read, `compacted/` and `changelog/` listed |
| `GcPrecheck` | `hasSstCollectionCandidates(latest, ssts, cutoff)` |
| `GcCommit` | `ManifestStore.update(prepareGarbageCollection)`: a new manifest only when the boundary moves |
| `GcListManifests` | `manifest/` listed after the commit |
| `GcDeleteSsts` | `planManifestCollection` + `planSstCollection`, then `ObjectStore.delete` |
| `GcDeleteManifests` | `advanceManifestBoundary` (RFC 0026), LATEST hint, then manifest deletes |

If a step cannot happen in the code, fix the model. If every step can, it is a bug: report it in
the code's words.

## Coverage record

Files: `packages/lsm-cloudflare/src/garbage-collector.ts` (`collect`, including the fork's
release), `packages/lsm-core/src/garbage-collection-plan.ts` (`planManifestCollection`,
`planSstCollection`, `hasSstCollectionCandidates`, `prepareGarbageCollection`),
`packages/lsm-core/src/manifest.ts` (`commitL0Flush`, `commitCompaction`, `advanceGcSstBoundary`,
checkpoints, `releasableExternalRefs`), `packages/lsm-cloudflare/src/maintenance-sweep.ts` (asks a
rewritten fork that still holds its source to collect). Code-level tests that
bind the steps to the code: `garbage-collector.test.ts` (old orphan collected and its late commit
refused; checkpointed SSTs kept; young manifests kept; idle database writes nothing),
`garbage-collection-plan.test.ts` (age from the successor, pins, owned SSTs only), `manifest.test.ts`
(boundary rejects and only moves forward). Writing the model exposed a real gap: manifest age was
measured from its own write time, so a manifest that stayed newest for hours and was superseded a
moment ago counted as old and its SSTs could be deleted under a reader. `ownUploadAge` reproduces it;
`planManifestCollection` now uses the next manifest's write time.

## Bounds and assumptions

- `GcSafety.cfg`: 2 SSTs (symmetric), clock 0–3, `MinAge` 1, at most 5 manifests (~27M distinct
  states, 65–80 s). Three SSTs or a longer clock exceed the time limit. `WithFork = FALSE`.
- `GcSafetyFork.cfg` (`WithFork = TRUE`): one source database and one fork of it, 1 SST, clock
  0–3, `MinAge` 1, at most 6 manifests (~550k states, about 2 s). The early-release path needs six
  manifests: a flush, the hold, a drop, the release, and GC's boundary commit. With 2 SSTs it takes
  about 72 s, over the limit, so compaction of the source (which needs a second SST) is not in this
  config: a drop is how the source's newest manifest stops naming the SST here. The fork's hold is
  the only checkpoint in this config (other pins are covered by `GcSafety.cfg`).
- The fork is abstract: its own manifests, compaction, and GC are not modeled step by step. Its
  first manifest reads the held manifest's SSTs; `ForkRewrite` is the moment its successor is
  written; `ForkRelease` is enabled once that was `MinAge` before the fork's collection, which is
  what `releasableExternalRefs` over GC's retained set decides (the fork's own pins and retries
  only delay it). The fork's readers' window is `MayBeHeld` in the fork's chain, written into
  `ForkReadsComplete`.
- Not modeled: a fork of a fork (D13; covered by `database-lifecycle.test.ts`, "keeps a fork of a
  fork's holds down the chain…", and `manifest.test.ts`), and removal of the source, which deletes
  every object but is refused while the hold exists (`markRemoved`; covered by
  `database-lifecycle.test.ts`).
- One GC run at a time; its steps interleave with every other action. Manifest CAS is atomic here
  (`verify-manifest-cas-fencing` checks it); stale manifest creates over collected ids are refused by
  `gc/manifest.boundary`, which `ManifestCasFencingGc.cfg` model-checks (`CommitOnce`, broken variant
  `noBoundaryReread`) together with optimistic commits from a committer's own view and `loadLatest`
  starting above the boundary (D56), and `manifest-store.test.ts` tests.
- The `noMinBoundary` variant (delete below the cutoff rather than below `min(boundary, cutoff)`)
  passes within these bounds: the pre-check already advances the boundary whenever an old SST is
  unreferenced. The code keeps the `min` as defense for overlapping GC runs, which the model does not
  cover.
- Clocks: GC's clock and the writers' ULID clocks are one clock here; skew beyond `gcMinAge` is not
  modeled (as in SlateDB RFC 0029).

## Maintenance notes

- D77: a fork's hold on its source is released by the fork's own collection
  (`GarbageCollector.collect`), once no manifest it keeps (the newest, the pinned ones, and those
  superseded less than `gcMinAge` ago) names another database's SST; the compactor's idle poll no
  longer releases it. Modeled with `WithFork` (`ForkStart`, `ForkRewrite`, `ForkRelease`) in
  `GcSafetyFork.cfg`, with the invariant `ForkReadsComplete` and the broken variant
  `earlyForkRelease` (release as soon as the newest fork manifest stops reading the source, the
  rule before D77), which fails it. `GcSafety.cfg`'s state count is unchanged (27,408,658). Reran;
  all broken variants still fail. Two code paths are left to tests:
  - nothing is released when a kept manifest cannot be loaded;
  - a crash between the two release steps leaves the fork's reference.

  Both are in `database-lifecycle.test.ts`, which also covers a split half split again before its
  rewrite. No model step changed.

- D70–D73: no covered function changed. The manifest cache's `refresh` now finds the newest
  manifest from the one it holds (`loadLatestFrom`, D72), and still caches only the newest; a
  flush writes fewer rows into its SST (D70), which changes no reference. Reran; all broken
  variants still fail.

- D67 (v1 workstream 10): a reader now holds a cached manifest for up to half of `gcMinAge`
  after loading it (`ManifestCache.covering`), where it used to hold one for
  `manifestRefreshInterval`. The manifest was the newest when its load began
  (`loadLatest`/`loadLatestFrom` always return the newest; `verify-manifest-cas-fencing` checks
  both), so it is read less than `gcMinAge` after it was superseded: inside `MayBeHeld`. `collect`
  and the planning functions are unchanged, so no model step changed. Two things keep the bound
  true in the code, each with a test that fails without it (`manifest-store.test.ts`): the hold is
  measured from the load's start and capped at half of `gcMinAge`, and a manifest the writer
  merely names (its last flush) is never cached unless it is still the newest. Reads longer than
  half of `gcMinAge`, and clock skew of that size between an isolate and the compactor, are
  outside the model, as before. Reran; all broken variants still fail.

- D62 (v1 workstream 7): an operator's repair, `dropSstFromManifest`, is a commit that removes one
  SST reference and adds none. Added as `DropSst(s)`, with the broken variant `dropDeletesObject`
  (the drop also deletes the object at once, as a naive repair would), which fails
  `ReadableManifestsComplete`: a reader may still hold the manifest before the drop. The code never
  deletes on drop. The state space grew to ~27M states (about 55 s).
- D60 (v1 workstream 5): collection now runs on each database's compactor (its alarm, or the
  operator's collect-now through `Compactor.collect`), serialized by the compactor's lock, so the
  model's one-GC-at-a-time assumption holds by construction; `collect`'s steps are unchanged, and it
  still interleaves with compaction commits, which the model covers. The sweep's leftover sweep only
  touches prefixes without a manifest (outside this model). Reran; all broken variants fail.

- Checked against the v1 workstream 2 commit (D56). `collect` is unchanged; GC's manifest boundary
  is now read by `ManifestReader.loadManifestBoundary` for both `create`'s re-read and
  `loadLatest`'s starting point, and the commit path may start from a committer's own view. Neither
  changes a step here: commits stay atomic in this model, and `ManifestCasFencingGc.cfg` checks the
  boundary re-read and the reader against GC deletes. Reran; all broken variants still fail.
- Checked against `3226177` (Phase 7). `collect` now sweeps a `db/<id>/` prefix that has no
  manifest (objects older than `gcMinAge`): outside this one-database model. It relies on creation
  writing the first manifest before any other object and removal deleting manifests last, so such a
  prefix is only a removed database's leftovers; `garbage-collector.test.ts` covers it. Fork holds
  are pins here (`Pin`/`Unpin`); a fork releases its hold only once no manifest it keeps reads the
  source (`releasableExternalRefs`), and removal is refused while holds exist (`markRemoved`).

## Keeping it in step with the code

Sync commit: the D77 commit (`fix(lsm-cloudflare): a fork releases its hold only when no manifest GC keeps names another database's SST`); before it the D70–D74 commit. When a covered function changes after it, update the model
so each action is still one awaited effect in the code, rerun, and show each invariant still fails on
a broken variant before moving the sync commit. For drift that skipped this skill, use
`.agents/skills/maintain-formal-verification-skill/SKILL.md`.
