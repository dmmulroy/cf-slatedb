---
name: verify-gc-safety
description: "Model-check that cf-slatedb garbage collection never deletes an SST that the newest manifest, a recently superseded manifest a reader may still hold, or a checkpoint (including a fork's hold on its source) references, and never deletes an uploaded SST whose commit can still succeed. Use after any change to GarbageCollector.collect, the GC planning functions, gcSstBoundary handling in commitL0Flush/commitCompaction, checkpoints, or how readers hold manifests."
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

## Run it

```sh
node tools/formal-verification/src/verify.ts gc-safety   # about 45 seconds
vp run verify                                            # every formal check
```

Expected: four `✓` lines. The code as written passes; `noBoundaryCheck` and `ownUploadAge` fail
`ReadableManifestsComplete`; `ignorePins` fails `PinnedManifestsComplete`.

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
| `Discard(s)` | a refused commit: the SST stays an orphan |
| `Pin` / `Unpin(p)` | `addCheckpoint` / `removeCheckpoint` (a fork's checkpoint is added by `forkRange` and released when its compaction no longer reads the source) |
| `GcListSsts` | `GarbageCollector.collect`: `now` read, `compacted/` and `changelog/` listed |
| `GcPrecheck` | `hasSstCollectionCandidates(latest, ssts, cutoff)` |
| `GcCommit` | `ManifestStore.update(prepareGarbageCollection)`: a new manifest only when the boundary moves |
| `GcListManifests` | `manifest/` listed after the commit |
| `GcDeleteSsts` | `planManifestCollection` + `planSstCollection`, then `ObjectStore.delete` |
| `GcDeleteManifests` | `advanceManifestBoundary` (RFC 0026), LATEST hint, then manifest deletes |

If a step cannot happen in the code, fix the model. If every step can, it is a bug: report it in
the code's words.

## Coverage record

Files: `packages/lsm-cloudflare/src/garbage-collector.ts` (`collect`), `packages/lsm-core/src/
garbage-collection-plan.ts` (`planManifestCollection`, `planSstCollection`,
`hasSstCollectionCandidates`, `prepareGarbageCollection`), `packages/lsm-core/src/manifest.ts`
(`commitL0Flush`, `commitCompaction`, `advanceGcSstBoundary`, checkpoints). Code-level tests that
bind the steps to the code: `garbage-collector.test.ts` (old orphan collected and its late commit
refused; checkpointed SSTs kept; young manifests kept; idle database writes nothing),
`garbage-collection-plan.test.ts` (age from the successor, pins, owned SSTs only), `manifest.test.ts`
(boundary rejects and only moves forward). Writing the model exposed a real gap: manifest age was
measured from its own write time, so a manifest that stayed newest for hours and was superseded a
moment ago counted as old and its SSTs could be deleted under a reader. `ownUploadAge` reproduces it;
`planManifestCollection` now uses the next manifest's write time.

## Bounds and assumptions

- `GcSafety.cfg`: 2 SSTs (symmetric), clock 0–3, `MinAge` 1, at most 5 manifests (~21M distinct
  states, about 40 s). Three SSTs or a longer clock exceed the time limit.
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

Sync commit: the v1 workstream 2 commit (`fast manifest commits`, D56). When a covered function changes after it, update the model
so each action is still one awaited effect in the code, rerun, and show each invariant still fails on
a broken variant before moving the sync commit. For drift that skipped this skill, use
`.agents/skills/maintain-formal-verification-skill/SKILL.md`.
