import { Arbitrary, Bytes, Option, Result, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  CheckpointId,
  CompactorEpoch,
  DatabaseId,
  ManifestId,
  SeqNum,
  SortedRunId,
  SstId,
  Timestamp,
  WriterEpoch,
} from "./ids.ts";
import { FULL_KEY_RANGE, keyRangeContains } from "./key-range.ts";
import { assertProperty } from "./lsm-core-arbitraries.ts";
import {
  addCheckpoint,
  advanceGcSstBoundary,
  changelogAfterSeqOf,
  commitCompaction,
  commitL0Flush,
  DEFAULT_DATABASE_OPTIONS,
  dropSstFromManifest,
  fenceCompactor,
  fenceWriter,
  forkManifest,
  holdForSuccessors,
  initialManifest,
  type Manifest,
  manifestSstHandles,
  markRemoved,
  pruneChangelog,
  removeCheckpoint,
  removeExpiredCheckpoints,
  removeExternalRef,
  sealManifest,
  type SstHandle,
} from "./manifest.ts";
import { encodeUlid } from "./ulid.ts";

/**
 * The manifest edits as a state machine (D91): random sequences of every edit, applied the way
 * the system applies them (each to the newest manifest, a refused edit leaving it unchanged), with
 * the invariants other parts of the system rely on checked after every step.
 */

const DB = DatabaseId.make("orders");
const compareKeys = Bytes.Order;
const smallInt = (minimum: number, maximum: number) =>
  Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum, maximum })));

type Step =
  | {
      readonly _tag: "Flush";
      readonly rows: number;
      readonly staleEpoch: boolean;
      readonly first: number;
      readonly span: number;
    }
  | { readonly _tag: "Compact"; readonly runs: number; readonly staleEpoch: boolean }
  | { readonly _tag: "FenceWriter" }
  | { readonly _tag: "FenceCompactor" }
  | { readonly _tag: "Seal" }
  | { readonly _tag: "Checkpoint"; readonly lifetime: number; readonly retry: boolean }
  | { readonly _tag: "RemoveCheckpoint"; readonly pick: number }
  | { readonly _tag: "ExpireCheckpoints" }
  | { readonly _tag: "Hold" }
  | { readonly _tag: "Drop"; readonly pick: number }
  | { readonly _tag: "Prune"; readonly age: number }
  | { readonly _tag: "AdvanceBoundary"; readonly back: number }
  | { readonly _tag: "Remove" }
  | { readonly _tag: "RemoveRef" }
  | { readonly _tag: "Tick"; readonly ms: number };

const stepArbitrary: Arbitrary.Arbitrary<Step> = Arbitrary.all({
  kind: smallInt(0, 19),
  a: smallInt(0, 6),
  b: smallInt(0, 200),
  flag: smallInt(0, 5),
}).pipe(
  Arbitrary.map(({ kind, a, b, flag }): Step => {
    if (kind <= 4) return { _tag: "Flush", rows: a + 1, staleEpoch: flag === 0, first: b, span: a };
    if (kind <= 6) return { _tag: "Compact", runs: a <= 3 ? 0 : a - 3, staleEpoch: flag === 0 };
    if (kind === 7) return { _tag: "FenceWriter" };
    if (kind === 8) return { _tag: "FenceCompactor" };
    if (kind === 9) return flag === 0 ? { _tag: "Seal" } : { _tag: "Tick", ms: b * 10 };
    if (kind === 10) return { _tag: "Checkpoint", lifetime: b * 100, retry: flag <= 1 };
    if (kind === 11) return { _tag: "RemoveCheckpoint", pick: a };
    if (kind === 12) return { _tag: "ExpireCheckpoints" };
    if (kind === 13) return flag <= 1 ? { _tag: "Hold" } : { _tag: "RemoveRef" };
    if (kind === 14) return { _tag: "Drop", pick: a };
    if (kind === 15) return { _tag: "Prune", age: b * 10 };
    if (kind === 16) return { _tag: "AdvanceBoundary", back: b * 10 };
    if (kind === 17) return flag === 0 ? { _tag: "Remove" } : { _tag: "Tick", ms: b * 10 };
    return { _tag: "Tick", ms: b * 10 };
  }),
);

/** The system around the manifest: the chain of committed manifests and a clock. */
interface World {
  chain: Array<Manifest>;
  now: number;
  counter: number;
  /** Every SST id ever committed, so a later commit never reuses one. */
  everNamed: Set<SstId>;
  /** The epoch the writer fenced in with (`fenceWriter`), if any: what its flushes carry. */
  writerEpoch: Option.Option<WriterEpoch>;
  /** The epoch the compactor fenced in with (`fenceCompactor`), if any. */
  compactorEpoch: Option.Option<CompactorEpoch>;
}

const handleOf = (id: SstId, first: number, last: number): SstHandle => ({
  id,
  firstKey: Uint8Array.of(first),
  lastKey: Uint8Array.of(last),
  sizeBytes: 1000,
  rowCount: 1,
  metaOffset: 900,
  metaLength: 100,
  owner: Option.none(),
  view: Option.none(),
});

/** A ULID at the world's time, unique within the run. */
const newUlid = (world: World) => {
  world.counter += 1;
  return encodeUlid(
    world.now,
    String(world.counter)
      .padStart(16, "0")
      .split("")
      .map((digit) => Number(digit)),
  );
};

const newSstId = (world: World) => SstId.make(newUlid(world));

const latest = (world: World): Manifest => {
  const newest = world.chain.at(-1);
  if (newest === undefined) throw new Error("the chain always holds its first manifest");
  return newest;
};

/** Commits `next` as the chain's next manifest unless it is the same manifest (D14). */
const commit = (world: World, next: Manifest) => {
  if (next !== latest(world)) world.chain.push(next);
};

const apply = (world: World, step: Step) => {
  const manifest = latest(world);
  switch (step._tag) {
    case "Tick":
      world.now += step.ms;
      return;
    case "Flush": {
      if (Option.isNone(world.writerEpoch)) return;
      const held = world.writerEpoch.value;
      const lastSeq = SeqNum.make(manifest.lastL0Seq + step.rows);
      const id = newSstId(world);
      const sst = handleOf(id, step.first % 250, (step.first % 250) + step.span);
      const changelogId = newSstId(world);
      const result = commitL0Flush(manifest, {
        writerEpoch: step.staleEpoch && held > 0 ? WriterEpoch.make(held - 1) : held,
        sst,
        afterSeq: manifest.lastL0Seq,
        lastSeq,
        clockTick: Timestamp.make(world.now),
        changelog: Option.some({
          sst: handleOf(changelogId, sst.firstKey[0] ?? 0, sst.lastKey[0] ?? 0),
          firstSeq: SeqNum.make(manifest.lastL0Seq + 1),
          lastSeq,
          createdAt: Timestamp.make(world.now),
        }),
      });
      if (Result.isSuccess(result)) commit(world, result.success);
      return;
    }
    case "Compact": {
      if (Option.isNone(world.compactorEpoch)) return;
      const heldEpoch = world.compactorEpoch.value;
      // The oldest L0 SSTs, or nothing, plus the newest `runs` sorted runs, into the lowest run id
      // among them (or a new one): the shapes the scheduler plans.
      const runs = manifest.sortedRuns.slice(0, step.runs);
      const l0 = manifest.l0.slice(Math.max(0, manifest.l0.length - 2));
      if (l0.length === 0 && runs.length === 0) return;
      const inputs = [...l0, ...runs.flatMap((run) => run.ssts)];
      const first = Math.min(...inputs.map((sst) => sst.firstKey[0] ?? 0));
      const last = Math.max(...inputs.map((sst) => sst.lastKey[0] ?? 0));
      const destination =
        runs.length > 0
          ? Math.min(...runs.map((run) => run.id))
          : manifest.sortedRuns.reduce((max, run) => Math.max(max, run.id + 1), 0);
      // Outputs: two disjoint SSTs covering the inputs' keys, in key order.
      const middle = Math.floor((first + last) / 2);
      const outputs =
        middle > first
          ? [handleOf(newSstId(world), first, middle - 1), handleOf(newSstId(world), middle, last)]
          : [handleOf(newSstId(world), first, last)];
      const result = commitCompaction(manifest, {
        compactorEpoch:
          step.staleEpoch && heldEpoch > 0 ? CompactorEpoch.make(heldEpoch - 1) : heldEpoch,
        job: {
          sources: [
            ...l0.map((sst) => ({ _tag: "L0" as const, sstId: sst.id })),
            ...runs.map((run) => ({ _tag: "SortedRun" as const, id: run.id })),
          ],
          destination: SortedRunId.make(destination),
          isBottom: runs.length === manifest.sortedRuns.length,
        },
        outputs,
        // Each compaction is its own job: a repeated job id is a retried commit (D103).
        jobId: `job-${newSstId(world)}`,
      });
      if (Result.isSuccess(result)) commit(world, result.success);
      return;
    }
    case "FenceWriter": {
      const result = fenceWriter(manifest);
      if (Result.isSuccess(result)) {
        commit(world, result.success);
        world.writerEpoch = Option.some(result.success.writerEpoch);
      }
      return;
    }
    case "FenceCompactor": {
      const fenced = fenceCompactor(manifest);
      commit(world, fenced);
      world.compactorEpoch = Option.some(fenced.compactorEpoch);
      return;
    }
    case "Seal":
      if (manifest.state._tag === "Open")
        commit(world, sealManifest(manifest, [DatabaseId.make("orders-a")]));
      return;
    case "Checkpoint": {
      // A retried checkpoint commit names an id already added (D14).
      const retried = step.retry ? manifest.checkpoints.at(-1)?.id : undefined;
      const result = addCheckpoint(manifest, {
        id: retried ?? CheckpointId.make(newUlid(world)),
        manifestId: ManifestId.make(world.chain.length),
        createdAt: Timestamp.make(world.now),
        expireAt:
          step.lifetime === 0
            ? Option.none()
            : Option.some(Timestamp.make(world.now + step.lifetime)),
        holder: Option.none(),
        name: Option.none(),
      });
      if (Result.isSuccess(result)) commit(world, result.success);
      return;
    }
    case "RemoveCheckpoint": {
      const checkpoint = manifest.checkpoints[step.pick % Math.max(1, manifest.checkpoints.length)];
      if (checkpoint !== undefined) commit(world, removeCheckpoint(manifest, checkpoint.id));
      return;
    }
    case "ExpireCheckpoints":
      commit(world, removeExpiredCheckpoints(manifest, Timestamp.make(world.now)));
      return;
    case "Hold": {
      const checkpointId = CheckpointId.make(newUlid(world));
      const result = holdForSuccessors(manifest, {
        pinnedManifestId: ManifestId.make(world.chain.length),
        proposed: [
          {
            successor: DatabaseId.make(`orders-${world.counter}`),
            checkpointId,
          },
        ],
        createdAt: Timestamp.make(world.now),
      });
      if (Result.isSuccess(result)) commit(world, result.success);
      return;
    }
    case "Drop": {
      const handles = [...manifest.l0, ...manifest.sortedRuns.flatMap((run) => run.ssts)];
      const target = handles[step.pick % Math.max(1, handles.length)];
      if (target === undefined) return;
      const result = dropSstFromManifest(manifest, {
        sstId: target.id,
        reason: "Forced",
        droppedAt: Timestamp.make(world.now),
      });
      if (Result.isSuccess(result)) commit(world, result.success);
      return;
    }
    case "Prune":
      commit(world, pruneChangelog(manifest, Timestamp.make(Math.max(0, world.now - step.age))));
      return;
    case "AdvanceBoundary":
      commit(
        world,
        advanceGcSstBoundary(manifest, Timestamp.make(Math.max(0, world.now - step.back))),
      );
      return;
    case "Remove": {
      const result = markRemoved(manifest, Timestamp.make(world.now));
      if (Result.isSuccess(result)) commit(world, result.success);
      return;
    }
    case "RemoveRef": {
      const ref = manifest.externalRefs[0];
      if (ref !== undefined) commit(world, removeExternalRef(manifest, ref));
      return;
    }
  }
};

const STATE_ORDER = { Open: 0, Sealed: 1, Removed: 2 } as const;

/**
 * The invariants, each checked between the previous and the newest manifest; the first one broken
 * is returned. Several are relied on and written nowhere else (D91).
 */
const brokenInvariant = (
  previous: Manifest,
  next: Manifest,
  world: World,
): Option.Option<string> => {
  const handles = manifestSstHandles(next);
  const ids = handles.map((sst) => sst.id);
  const checks: ReadonlyArray<readonly [string, boolean]> = [
    ["lastL0Seq never decreases", next.lastL0Seq >= previous.lastL0Seq],
    ["lastL0ClockTick never decreases", next.lastL0ClockTick >= previous.lastL0ClockTick],
    ["writerEpoch never decreases", next.writerEpoch >= previous.writerEpoch],
    ["compactorEpoch never decreases", next.compactorEpoch >= previous.compactorEpoch],
    ["gcSstBoundary never decreases", next.gcSstBoundary >= previous.gcSstBoundary],
    ["no SST is named twice", new Set(ids).size === ids.length],
    [
      "a manifest names only SSTs never named before or named by its predecessor",
      ids.every(
        (id) =>
          manifestSstHandles(previous).some((sst) => sst.id === id) || !world.everNamed.has(id),
      ),
    ],
    [
      "sorted run ids are unique and newest first",
      next.sortedRuns.every(
        (run, index) => index === 0 || (next.sortedRuns[index - 1]?.id ?? 0) > run.id,
      ),
    ],
    [
      "the SSTs of a sorted run are key-ordered and disjoint",
      next.sortedRuns.every((run) =>
        run.ssts.every(
          (sst, index) =>
            compareKeys(sst.firstKey, sst.lastKey) <= 0 &&
            (index === 0 ||
              compareKeys(run.ssts[index - 1]?.lastKey ?? sst.firstKey, sst.firstKey) < 0),
        ),
      ),
    ],
    [
      "state only moves forward: Open, Sealed, Removed",
      STATE_ORDER[next.state._tag] >= STATE_ORDER[previous.state._tag],
    ],
    [
      "sealing or removal fences the writer",
      next.state._tag === previous.state._tag || next.writerEpoch > previous.writerEpoch,
    ],
    [
      "no flush commits after a seal or removal",
      previous.state._tag === "Open" || next.lastL0Seq === previous.lastL0Seq,
    ],
    [
      "checkpoint ids are unique",
      new Set(next.checkpoints.map((cp) => cp.id)).size === next.checkpoints.length,
    ],
    [
      "a checkpoint pins a manifest of this chain at or before the newest",
      next.checkpoints.every((cp) => cp.manifestId >= 1 && cp.manifestId <= world.chain.length),
    ],
    [
      "a dropped SST is not in the manifest",
      next.droppedSsts.every((dropped) => !ids.includes(dropped.sstId)),
    ],
    ["changelogAfterSeq is at most lastL0Seq", changelogAfterSeqOf(next) <= next.lastL0Seq],
    [
      "changelogAfterSeq never decreases",
      changelogAfterSeqOf(next) >= changelogAfterSeqOf(previous),
    ],
    [
      "the changelog holds every change after changelogAfterSeq, contiguously",
      next.changelog.every(
        (entry, index) =>
          entry.firstSeq <= entry.lastSeq &&
          entry.firstSeq ===
            (index === 0 ? changelogAfterSeqOf(next) : (next.changelog[index - 1]?.lastSeq ?? 0)) +
              1,
      ) && (next.changelog.at(-1)?.lastSeq ?? changelogAfterSeqOf(next)) === next.lastL0Seq,
    ],
    ["the database id never changes", next.databaseId === previous.databaseId],
  ];
  const broken = checks.find(([, holds]) => !holds);
  return broken === undefined ? Option.none() : Option.some(broken[0]);
};

/** A fork of `source` over `range`: its range lies inside the source's, each view inside it. */
const brokenForkInvariant = (
  source: Manifest,
  start: number,
  end: number,
): Option.Option<string> => {
  const range = { start: Uint8Array.of(start), end: Option.some(Uint8Array.of(end)) };
  const fork = forkManifest(source, {
    databaseId: DatabaseId.make("orders-f"),
    checkpointId: CheckpointId.make("01JD0000000000000000000FK1"),
    range,
  });
  const forkRange = Option.getOrElse(fork.keyRange, () => FULL_KEY_RANGE);
  const inside = (key: Uint8Array) =>
    keyRangeContains(range, key) &&
    Option.match(source.keyRange, {
      onNone: () => true,
      onSome: (outer) => keyRangeContains(outer, key),
    });
  const checks: ReadonlyArray<readonly [string, boolean]> = [
    [
      "a fork's range starts inside its source's",
      inside(forkRange.start) || source.l0.length === 0,
    ],
    [
      "every view of a fork lies inside the fork's range",
      manifestSstHandles(fork).every((sst) =>
        Option.match(sst.view, {
          onNone: () => false,
          onSome: (view) =>
            keyRangeContains(forkRange, view.start) &&
            Option.match(view.end, {
              onNone: () => Option.isNone(forkRange.end),
              onSome: (viewEnd) =>
                Option.match(forkRange.end, {
                  onNone: () => true,
                  onSome: (forkEnd) => compareKeys(viewEnd, forkEnd) <= 0,
                }),
            }),
        }),
      ),
    ],
    [
      "a fork's SSTs belong to its source",
      manifestSstHandles(fork).every((sst) => Option.isSome(sst.owner)),
    ],
    ["a fork continues its source's sequence numbers", fork.lastL0Seq === source.lastL0Seq],
    [
      "a fork starts open with epochs 0",
      fork.state._tag === "Open" && fork.writerEpoch === 0 && fork.compactorEpoch === 0,
    ],
    [
      "a fork names exactly one hold, on its source",
      fork.externalRefs.length === 1 && fork.externalRefs[0]?.owner === source.databaseId,
    ],
  ];
  const broken = checks.find(([, holds]) => !holds);
  return broken === undefined ? Option.none() : Option.some(broken[0]);
};

describe("manifest edits as a state machine (D91)", () => {
  it("keeps every invariant across random sequences of every edit", () => {
    let failure = "";
    assertProperty(
      Arbitrary.all({
        steps: Arbitrary.array(stepArbitrary, { minLength: 30, maxLength: 80 }),
        forkAt: Arbitrary.all({ start: smallInt(0, 120), width: smallInt(1, 120) }),
      }),
      ({ steps, forkAt }) => {
        const world: World = {
          chain: [
            initialManifest(DB, {
              keyRange: Option.none(),
              databaseOptions: DEFAULT_DATABASE_OPTIONS,
            }),
          ],
          now: 1_000_000,
          counter: 0,
          everNamed: new Set(),
          writerEpoch: Option.none(),
          compactorEpoch: Option.none(),
        };
        for (const [index, step] of steps.entries()) {
          const previous = latest(world);
          apply(world, step);
          const next = latest(world);
          const broken = brokenInvariant(previous, next, world);
          if (Option.isSome(broken)) {
            failure = `step ${index} (${step._tag}): ${broken.value}`;
            return false;
          }
          for (const sst of manifestSstHandles(next)) world.everNamed.add(sst.id);
          const fork = brokenForkInvariant(next, forkAt.start, forkAt.start + forkAt.width);
          if (Option.isSome(fork)) {
            failure = `fork after step ${index} (${step._tag}): ${fork.value}`;
            return false;
          }
        }
        return true;
      },
      { runs: 3000 },
    );
    expect(failure).toBe("");
    // 3,000 runs take 2 to 4 s alone and more beside the other test files: past the default 5 s.
  }, 60_000);
});
