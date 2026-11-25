---
name: verify-manifest-cas-fencing
description: "Model-check cf-slatedb's manifest CAS and writer/compactor epoch fencing with TLA+ (TLC). Use after any change to ManifestStore.update/create, ManifestReader.loadLatest, ObjectStore.putIfAbsent, or the manifest edits fenceWriter, fenceCompactor, commitL0Flush, commitCompaction, and before trusting a fencing or CAS change."
---

# Verify manifest CAS and fencing

## What is verified, in plain words

- **CreateOnce**: a manifest id is written exactly once; nothing ever overwrites a manifest.
  Harm if broken: two writers both believe they committed, and one's flush silently disappears.
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

Expected output: five `✓` lines. The code as written passes both configs, and each broken variant
fails with its invariant. Any `✗` is a finding: read the next section.

## Read a result

Read every result with the pstack `/statechart` skill (`.agents/skills/statechart/SKILL.md`; open
it by path, it is not invoked automatically). Point it at a scratch copy of
`tools/formal-verification/specs/ManifestCasFencing.tla` and the config of the failing run, with
`JAVA` and `TLA2TOOLS` set to the binaries under `.tlc/` so it does not install its own. It
reports each broken rule as its shortest path. The state space is far above its 500-state chart
budget, so it draws only bug paths. Use a small config when you want a chart.

Telling a real bug from a modeling error: replay the path against the code, one action per step.
`WriterUpload`, `JobUpload` are SST uploads; `*Load` steps are `ManifestReader.loadLatest`; `*Create`
steps are one `ObjectStore.putIfAbsent` of `manifest/<head + 1>` inside `ManifestStore.update`, with
the edit (`fenceWriter`, `commitL0Flush`, `fenceCompactor`, `commitCompaction`) applied to the loaded
head. If a step cannot happen in the code, the model is wrong; fix the model and say why. If every
step can happen, it is a bug in the code: report the path in the code's words, and do not weaken
an invariant to make it pass.

## Coverage record

| Model element | Code |
| --- | --- |
| `Create`, `CanCreate` | `ObjectStore.putIfAbsent` (`packages/lsm-cloudflare/src/object-store.ts`), `ManifestStore.create` (`manifest-store.ts`) |
| `*Load` + `*Create` retry on conflict | `ManifestStore.update` CAS loop (`manifest-store.ts`) |
| `Head` | `ManifestReader.loadLatest` (`manifest-reader.ts`): newest existing id |
| `WriterFence*` | `fenceWriter` (`packages/lsm-core/src/manifest.ts`) via `ManifestStore.update` (writer init, Phase 4) |
| `WriterFlushCreate` | `commitL0Flush`: already-applied check, writer-epoch check, prepend to L0 |
| `CompactorFence*` | `fenceCompactor` via `ManifestStore.update` (compactor init, Phase 6) |
| `JobPlan` | `planCompactions` picks L0 inputs and runs from the head |
| `JobCommitCreate` | `commitCompaction`: compactor-epoch check, inputs-present check, replace inputs |

Code-level tests that bind the model's steps to the code (each fails when the matching check is
removed from the real code): `packages/lsm-core/src/manifest.test.ts` ("fences a stale writer",
"fails when they changed") and `packages/lsm-cloudflare/src/manifest-store.test.ts` ("fences a
stale writer", "serializes concurrent edits", "writes nothing for an already-applied edit").

## Bounds and assumptions

- `ManifestCasFencing.cfg`: 2 writers, 2 compactors, 1 job each, 2 flushes, 9 manifest ids
  (~780k states). `ManifestCasFencingRace.cfg`: 1 writer, 1 compactor with 2 concurrent jobs,
  2 flushes, 8 ids (~4k states). The race config is the only one where two live jobs race; with one
  job per compactor, a second compactor fences the first before they can.
- The id bound must cover init + every fence + every flush + every compaction commit; with fewer
  ids, later commits cannot happen and a check can pass vacuously. Raise `MaxId` with the other
  bounds.
- Assumed, not modeled: R2 create-if-absent is atomic (verified against local R2 by
  `test/object-store/object-store.workerd.ts`; real R2 needs an approved live check); the `LATEST`
  hint (it only shortens the probe); GC deleting manifests; memtable truncation (the write-loss
  check covers it).
- Broken variants (`Broken` constant): `noWriterEpochCheck` → `NoStaleCommit`; `blindCreate` →
  `CreateOnce`; `noInputsCheck` → `AckedDataVisible` (race config).

## Keeping it in step with the code

Sync commit: `5985e4b` (the model was last confirmed to describe the code at this
commit). When a covered file changed after it, diff it, update the model so each step still maps
to one awaited effect in the code, rerun, and show the affected invariant still fails on a broken
variant. Move the sync commit forward only after confirming the model describes the code at the
new commit. For drift that skipped this skill, use
`.agents/skills/maintain-formal-verification-skill/SKILL.md`.
