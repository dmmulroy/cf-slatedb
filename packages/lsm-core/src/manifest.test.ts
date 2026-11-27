import { readFileSync } from "node:fs";

import { Arbitrary, Option, Result, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  CheckpointId,
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
import { encodeUlid } from "./ulid.ts";
import {
  addCheckpoint,
  advanceGcSstBoundary,
  changelogAfterSeqOf,
  changelogHoldsChangesAfter,
  commitCompaction,
  commitL0Flush,
  type CompactionJob,
  DEFAULT_DATABASE_OPTIONS,
  dropSstFromManifest,
  fenceCompactor,
  fenceWriter,
  forkManifest,
  holdForSuccessors,
  markRemoved,
  releasableExternalRefs,
  initialManifest,
  Manifest,
  MANIFEST_FORMAT_VERSION,
  MAX_DROPPED_SST_HISTORY,
  ManifestJson,
  pruneChangelog,
  removeCheckpoint,
  replaceManifestFields,
  sealManifest,
  type SstHandle,
  successorHoldsOf,
} from "./manifest.ts";

const DB = DatabaseId.make("orders");
const ulid = (n: number) => SstId.make(`01JD${String(n).padStart(22, "0")}`);

const handle = (n: number, first: number, last: number, sizeBytes = 1000): SstHandle => ({
  id: ulid(n),
  firstKey: Uint8Array.of(first),
  lastKey: Uint8Array.of(last),
  sizeBytes,
  rowCount: 10,
  metaOffset: sizeBytes - 100,
  metaLength: 100,
  owner: Option.none(),
  view: Option.none(),
});

const empty = initialManifest(DB, {
  keyRange: Option.none(),
  databaseOptions: DEFAULT_DATABASE_OPTIONS,
});

const manifestWith = replaceManifestFields;

const flush = (manifest: Manifest, n: number, writerEpoch = manifest.writerEpoch) =>
  commitL0Flush(manifest, {
    writerEpoch,
    sst: handle(n, n, n + 1),
    lastSeq: SeqNum.make(n * 10),
    clockTick: Timestamp.make(n),
    changelog: Option.none(),
  });

describe("Manifest JSON", () => {
  it("round-trips a manifest with every v1 field", () => {
    const manifest = manifestWith(Result.getOrThrow(flush(empty, 1)), {
      checkpoints: [
        {
          id: CheckpointId.make("01JD0000000000000000000CP1"),
          manifestId: ManifestId.make(3),
          createdAt: Timestamp.make(5),
          expireAt: Option.none(),
          holder: Option.some(DatabaseId.make("orders-a")),
          name: Option.some("split"),
        },
      ],
      keyRange: Option.some({ start: Uint8Array.of(1), end: Option.some(Uint8Array.of(9)) }),
      state: { _tag: "Sealed", successors: [DatabaseId.make("orders-a")] },
      externalRefs: [
        {
          owner: DatabaseId.make("root"),
          checkpointId: CheckpointId.make("01JD0000000000000000000CP2"),
        },
      ],
    });
    const decoded = Schema.decodeSync(ManifestJson)(Schema.encodeSync(ManifestJson)(manifest));
    expect(Schema.encodeSync(ManifestJson)(decoded)).toBe(
      Schema.encodeSync(ManifestJson)(manifest),
    );
  });

  it("decodes a manifest older code wrote, as format 1 with no dropped SSTs (D64)", () => {
    // Written by shaledb at v1 workstream 1 (before format versions and droppedSsts), read back
    // from preview-1's bucket.
    const written = readFileSync(
      new URL("./fixtures/manifest-written-by-workstream-1.json", import.meta.url),
      "utf8",
    );
    const decoded = Schema.decodeSync(ManifestJson)(written);
    expect([decoded.formatVersion, decoded.droppedSsts]).toEqual([1, []]);
    expect([decoded.l0.length, decoded.sortedRuns.length]).toEqual([1, 1]);
    // Writing it again records the format; everything it held is kept.
    const rewritten = JSON.parse(Schema.encodeSync(ManifestJson)(decoded));
    expect(rewritten.formatVersion).toBe(MANIFEST_FORMAT_VERSION);
    const original = JSON.parse(written);
    for (const key of Object.keys(original)) expect(rewritten[key]).toEqual(original[key]);
  });

  it("decodes manifests written with the per-database jurisdiction option, ignoring it (D49)", () => {
    const current = Schema.encodeSync(ManifestJson)(empty);
    for (const jurisdiction of [null, "eu", "fedramp"]) {
      // A manifest as written before residency moved to the Stack: the option was in `options`.
      const written = JSON.parse(current);
      written.options = { ...written.options, jurisdiction };
      const decoded = Schema.decodeSync(ManifestJson)(JSON.stringify(written));
      expect(decoded.options).toEqual(empty.options);
      // The next manifest written from it no longer carries the field.
      expect(Schema.encodeSync(ManifestJson)(decoded)).toBe(current);
    }
  });
});

describe("dropSstFromManifest (D62)", () => {
  const at = Timestamp.make(99);
  const withRuns = manifestWith(Result.getOrThrow(flush(Result.getOrThrow(flush(empty, 1)), 2)), {
    sortedRuns: [
      { id: SortedRunId.make(2), ssts: [handle(10, 10, 19), handle(11, 20, 29)] },
      { id: SortedRunId.make(1), ssts: [handle(12, 0, 50)] },
    ],
  });

  it("drops an L0 SST and records what was lost", () => {
    const dropped = Result.getOrThrow(
      dropSstFromManifest(withRuns, { sstId: ulid(1), reason: "Corrupted", droppedAt: at }),
    );
    expect(dropped.l0.map((sst) => sst.id)).toEqual([ulid(2)]);
    expect(dropped.sortedRuns).toEqual(withRuns.sortedRuns);
    expect(dropped.droppedSsts).toEqual([
      {
        sstId: ulid(1),
        owner: Option.none(),
        from: { _tag: "L0", sstId: ulid(1) },
        firstKey: Uint8Array.of(1),
        lastKey: Uint8Array.of(2),
        view: Option.none(),
        rowCount: 10,
        reason: "Corrupted",
        droppedAt: at,
      },
    ]);
    // Nothing else changes: epochs, sequence numbers, and the GC boundary stay.
    expect([dropped.writerEpoch, dropped.compactorEpoch, dropped.lastL0Seq]).toEqual([
      withRuns.writerEpoch,
      withRuns.compactorEpoch,
      withRuns.lastL0Seq,
    ]);
  });

  it("drops a sorted run's SST, removing the run when it was the last, and is idempotent", () => {
    const fromRun = Result.getOrThrow(
      dropSstFromManifest(withRuns, { sstId: ulid(11), reason: "Forced", droppedAt: at }),
    );
    expect(fromRun.sortedRuns.map((run) => [run.id, run.ssts.map((sst) => sst.id)])).toEqual([
      [2, [ulid(10)]],
      [1, [ulid(12)]],
    ]);
    expect(fromRun.droppedSsts[0]?.from).toEqual({ _tag: "SortedRun", id: SortedRunId.make(2) });
    const lastOfRun = Result.getOrThrow(
      dropSstFromManifest(fromRun, { sstId: ulid(12), reason: "Forced", droppedAt: at }),
    );
    expect(lastOfRun.sortedRuns.map((run) => run.id)).toEqual([2]);
    // Applying the same drop again writes nothing.
    expect(
      Result.getOrThrow(
        dropSstFromManifest(lastOfRun, { sstId: ulid(12), reason: "Forced", droppedAt: at }),
      ),
    ).toBe(lastOfRun);
  });

  it("refuses an SST the manifest never held, and a database being removed", () => {
    const missing = dropSstFromManifest(withRuns, {
      sstId: ulid(77),
      reason: "Forced",
      droppedAt: at,
    });
    expect(Result.isFailure(missing) && missing.failure._tag).toBe("SstNotInManifestError");
    const removed = dropSstFromManifest(
      Result.getOrThrow(markRemoved(withRuns, Timestamp.make(1))),
      {
        sstId: ulid(1),
        reason: "Corrupted",
        droppedAt: at,
      },
    );
    expect(Result.isFailure(removed) && removed.failure._tag).toBe("DatabaseRemovedError");
  });

  it("keeps the newest drops only, round-trips them, and decodes older manifests with none", () => {
    let manifest = empty;
    for (let n = 1; n <= MAX_DROPPED_SST_HISTORY + 3; n++) {
      manifest = Result.getOrThrow(
        dropSstFromManifest(Result.getOrThrow(flush(manifest, n)), {
          sstId: ulid(n),
          reason: "Corrupted",
          droppedAt: at,
        }),
      );
    }
    expect(manifest.droppedSsts.length).toBe(MAX_DROPPED_SST_HISTORY);
    expect(manifest.droppedSsts[0]?.sstId).toBe(ulid(4));
    const json = Schema.encodeSync(ManifestJson)(manifest);
    expect(Schema.decodeSync(ManifestJson)(json).droppedSsts).toEqual(manifest.droppedSsts);
    const older = JSON.parse(Schema.encodeSync(ManifestJson)(empty));
    delete older.droppedSsts;
    expect(Schema.decodeSync(ManifestJson)(JSON.stringify(older)).droppedSsts).toEqual([]);
  });
});

describe("manifest edits", () => {
  it("commits a flush, treats a repeated commit as applied, and fences a stale writer", () => {
    const fenced = Result.getOrThrow(fenceWriter(empty));
    const flushed = Result.getOrThrow(flush(fenced, 1));
    expect(flushed.l0.map((sst) => sst.id)).toEqual([ulid(1)]);
    expect(flushed.lastL0Seq).toBe(10);
    expect(Result.getOrThrow(flush(flushed, 1))).toBe(flushed);

    const stale = flush(Result.getOrThrow(fenceWriter(flushed)), 2, WriterEpoch.make(1));
    expect(Result.isFailure(stale) && stale.failure._tag).toBe("WriterFencedError");
  });

  it("rejects writers and flushes once sealed, and fences the sealed writer", () => {
    const sealed = sealManifest(empty, [DatabaseId.make("orders-a")]);
    expect(sealed.writerEpoch).toBe(1);
    expect(sealManifest(sealed, [DatabaseId.make("orders-a")])).toBe(sealed);
    const start = fenceWriter(sealed);
    expect(Result.isFailure(start) && start.failure._tag).toBe("DatabaseSealedError");
    const late = flush(sealed, 1);
    expect(Result.isFailure(late) && late.failure._tag).toBe("DatabaseSealedError");
  });

  it("replaces exactly a compaction's inputs, fails when they changed, and is idempotent", () => {
    let manifest = empty;
    for (const n of [1, 2, 3, 4, 5]) manifest = Result.getOrThrow(flush(manifest, n));
    const job: CompactionJob = {
      sources: [ulid(1), ulid(2), ulid(3)].map((sstId) => ({ _tag: "L0" as const, sstId })),
      destination: SortedRunId.make(0),
      isBottom: true,
    };
    const commit = {
      compactorEpoch: manifest.compactorEpoch,
      jobId: "job-1",
      job,
      outputs: [handle(10, 1, 4)],
    };
    const compacted = Result.getOrThrow(commitCompaction(manifest, commit));
    expect(compacted.l0.map((sst) => sst.id)).toEqual([ulid(5), ulid(4)]);
    expect(compacted.sortedRuns.map((run) => [run.id, run.ssts.map((sst) => sst.id)])).toEqual([
      [0, [ulid(10)]],
    ]);
    expect(Result.getOrThrow(commitCompaction(compacted, commit))).toBe(compacted);

    // Another job over the same inputs, which this one already replaced.
    const changed = commitCompaction(compacted, {
      ...commit,
      jobId: "job-2",
      outputs: [handle(11, 1, 4)],
    });
    expect(Result.isFailure(changed) && changed.failure._tag).toBe("CompactionInputsChangedError");
    const fenced = commitCompaction(fenceCompactor(manifest), commit);
    expect(Result.isFailure(fenced) && fenced.failure._tag).toBe("CompactorFencedError");
  });

  it("recognizes a retried commit of a job with no outputs by its recorded id, across later edits, for the last 16 jobs (D103)", () => {
    let manifest = empty;
    for (const n of [1, 2, 3]) manifest = Result.getOrThrow(flush(manifest, n));
    const job: CompactionJob = {
      sources: [ulid(1), ulid(2)].map((sstId) => ({ _tag: "L0" as const, sstId })),
      destination: SortedRunId.make(0),
      isBottom: true,
    };
    // Every row was dropped: the commit removes the inputs and adds nothing.
    const commit = { compactorEpoch: manifest.compactorEpoch, jobId: "dropped", job, outputs: [] };
    const compacted = Result.getOrThrow(commitCompaction(manifest, commit));
    expect(compacted.l0.map((sst) => sst.id)).toEqual([ulid(3)]);
    // A flush in between keeps the record; the retry then changes nothing.
    const flushedAfter = Result.getOrThrow(flush(compacted, 4));
    expect(Result.getOrThrow(commitCompaction(flushedAfter, commit))).toBe(flushedAfter);
    // Another job over the same inputs is still refused.
    const other = commitCompaction(flushedAfter, { ...commit, jobId: "other" });
    expect(Result.isFailure(other) && other.failure._tag).toBe("CompactionInputsChangedError");
    // A commit keeps only the newest 16 ids.
    const full = replaceManifestFields(flushedAfter, {
      recentCompactionJobIds: Array.from({ length: 16 }, (_, index) => `earlier-${index}`),
    });
    const next = Result.getOrThrow(
      commitCompaction(full, {
        ...commit,
        jobId: "next",
        job: { ...job, sources: [{ _tag: "L0", sstId: ulid(3) }] },
      }),
    );
    expect(next.recentCompactionJobIds).toHaveLength(16);
    expect(next.recentCompactionJobIds.at(-1)).toBe("next");
    expect(next.recentCompactionJobIds.includes("earlier-0")).toBe(false);
    // A manifest written before D103 has no such key and decodes with none recorded.
    const encoded: Record<string, unknown> = JSON.parse(Schema.encodeSync(ManifestJson)(compacted));
    delete encoded["recentCompactionJobIds"];
    expect(Schema.decodeSync(ManifestJson)(JSON.stringify(encoded)).recentCompactionJobIds).toEqual(
      [],
    );
  });

  it("rejects new SSTs created before GC's boundary, which only moves forward", () => {
    const at = (timeMillis: number, n: number): SstHandle => ({
      ...handle(n, n, n + 1),
      id: SstId.make(
        encodeUlid(
          timeMillis,
          Array.from({ length: 16 }, () => n),
        ),
      ),
    });
    const bounded = advanceGcSstBoundary(empty, Timestamp.make(5_000));
    expect(bounded.gcSstBoundary).toBe(5_000);
    expect(advanceGcSstBoundary(bounded, Timestamp.make(4_000))).toBe(bounded);
    expect(advanceGcSstBoundary(bounded, Timestamp.make(5_000))).toBe(bounded);

    const commitAt = (sst: SstHandle) =>
      commitL0Flush(bounded, {
        writerEpoch: bounded.writerEpoch,
        sst,
        lastSeq: SeqNum.make(1),
        clockTick: Timestamp.make(1),
        changelog: Option.none(),
      });
    const late = commitAt(at(4_999, 1));
    expect(Result.isFailure(late) && late.failure._tag).toBe("SstBelowGcBoundaryError");
    // GC deletes only SSTs created strictly before the boundary, so one created at it is safe.
    const flushed = Result.getOrThrow(commitAt(at(5_000, 2)));
    expect(flushed.l0.map((sst) => sst.id)).toEqual([at(5_000, 2).id]);

    const job: CompactionJob = {
      sources: [{ _tag: "L0", sstId: at(5_000, 2).id }],
      destination: SortedRunId.make(0),
      isBottom: true,
    };
    const compaction = (output: SstHandle) =>
      commitCompaction(flushed, {
        compactorEpoch: flushed.compactorEpoch,
        jobId: "job-1",
        job,
        outputs: [output],
      });
    const staleOutput = compaction(at(1_000, 3));
    expect(Result.isFailure(staleOutput) && staleOutput.failure._tag).toBe(
      "SstBelowGcBoundaryError",
    );
    expect(Result.isSuccess(compaction(at(6_000, 4)))).toBe(true);
  });

  it("adds and removes checkpoints idempotently", () => {
    const checkpoint = {
      id: CheckpointId.make("01JD0000000000000000000CP1"),
      manifestId: ManifestId.make(1),
      createdAt: Timestamp.make(1),
      expireAt: Option.none(),
      holder: Option.none(),
      name: Option.none(),
    };
    const added = Result.getOrThrow(addCheckpoint(empty, checkpoint));
    expect(Result.getOrThrow(addCheckpoint(added, checkpoint))).toBe(added);
    expect(removeCheckpoint(added, checkpoint.id).checkpoints).toEqual([]);
    expect(removeCheckpoint(empty, checkpoint.id)).toBe(empty);
  });

  it("forks a range as views of the source's SSTs, holding the source checkpoint", () => {
    let source = empty;
    source = Result.getOrThrow(flush(source, 1));
    source = Result.getOrThrow(flush(source, 5));
    const checkpointId = CheckpointId.make("01JD0000000000000000000CP1");
    const range = { start: Uint8Array.of(3), end: Option.some(Uint8Array.of(6)) };
    const fork = forkManifest(source, {
      databaseId: DatabaseId.make("orders-b"),
      checkpointId,
      range,
    });
    expect(fork.l0.map((sst) => sst.id)).toEqual([ulid(5)]);
    expect(fork.l0[0]?.owner).toEqual(Option.some(DB));
    expect(fork.externalRefs).toEqual([{ owner: DB, checkpointId }]);
    expect(fork.lastL0Seq).toBe(source.lastL0Seq);
    expect(
      Option.map(fork.keyRange, (keyRange) => keyRangeContains(keyRange, Uint8Array.of(6))),
    ).toEqual(Option.some(false));
  });

  it("refuses removal while a fork holds a checkpoint, then fences writers, checkpoints, and forks", () => {
    const checkpoint = {
      id: CheckpointId.make("01JD0000000000000000000CP2"),
      manifestId: ManifestId.make(1),
      createdAt: Timestamp.make(1),
      expireAt: Option.none(),
      holder: Option.some(DatabaseId.make("orders-b")),
      name: Option.none(),
    };
    const held = Result.getOrThrow(addCheckpoint(empty, checkpoint));
    const refused = markRemoved(held, Timestamp.make(1));
    expect(Result.isFailure(refused) && refused.failure.dependents).toEqual(["orders-b"]);
    const removed = Result.getOrThrow(
      markRemoved(removeCheckpoint(held, checkpoint.id), Timestamp.make(1)),
    );
    expect(removed.state._tag).toBe("Removed");
    expect(removed.writerEpoch).toBe(empty.writerEpoch + 1);
    expect(Result.getOrThrow(markRemoved(removed, Timestamp.make(2)))).toBe(removed);
    const start = fenceWriter(removed);
    expect(Result.isFailure(start) && start.failure._tag).toBe("DatabaseRemovedError");
    const pin = addCheckpoint(removed, {
      ...checkpoint,
      id: CheckpointId.make("01JD0000000000000000000CP3"),
    });
    expect(Result.isFailure(pin) && pin.failure._tag).toBe("DatabaseRemovedError");
    // A writer from before the removal is fenced.
    const late = flush(removed, 1, empty.writerEpoch);
    expect(Result.isFailure(late) && late.failure._tag).toBe("WriterFencedError");
  });

  it("releases an external reference only once no kept manifest reads the owner's SSTs", () => {
    let source = empty;
    source = Result.getOrThrow(flush(source, 1));
    const checkpointId = CheckpointId.make("01JD0000000000000000000CP4");
    const fork = forkManifest(source, {
      databaseId: DatabaseId.make("orders-b"),
      checkpointId,
      range: FULL_KEY_RANGE,
    });
    expect(releasableExternalRefs(fork, [])).toEqual([]);
    // Compaction rewrote the external SST into the fork's own.
    const rewritten = manifestWith(fork, {
      l0: [],
      sortedRuns: [{ id: SortedRunId.make(0), ssts: [handle(9, 1, 2)] }],
    });
    expect(releasableExternalRefs(rewritten, [])).toEqual([{ owner: DB, checkpointId }]);
    // But a manifest GC keeps (one a checkpoint pins, D13, or one superseded less than gcMinAge
    // ago, D77) still reads the owner's SST.
    expect(releasableExternalRefs(rewritten, [fork])).toEqual([]);
  });

  it("keeps a fork of a fork's hold on its source while it reads the original owner's SSTs", () => {
    const source = Result.getOrThrow(flush(empty, 1));
    const middleId = DatabaseId.make("orders-b");
    const middle = forkManifest(source, {
      databaseId: middleId,
      checkpointId: CheckpointId.make("01JD0000000000000000000CP5"),
      range: FULL_KEY_RANGE,
    });
    const holdOnMiddle = CheckpointId.make("01JD0000000000000000000CP6");
    const grandchild = forkManifest(middle, {
      databaseId: DatabaseId.make("orders-c"),
      checkpointId: holdOnMiddle,
      range: FULL_KEY_RANGE,
    });
    // Its SSTs carry the original owner's id, but its hold is on the middle database, whose
    // checkpoint is what keeps the middle database's own hold on the original owner.
    expect(grandchild.l0[0]?.owner).toEqual(Option.some(DB));
    expect(grandchild.externalRefs).toEqual([{ owner: middleId, checkpointId: holdOnMiddle }]);
    expect(releasableExternalRefs(grandchild, [])).toEqual([]);
    const rewritten = manifestWith(grandchild, {
      l0: [],
      sortedRuns: [{ id: SortedRunId.make(0), ssts: [handle(9, 1, 2)] }],
    });
    expect(releasableExternalRefs(rewritten, [])).toEqual(grandchild.externalRefs);
  });
});

describe("holdForSuccessors", () => {
  it("takes one hold per successor pinning the given manifest, once, and refuses a removed database", () => {
    const left = DatabaseId.make("orders-a");
    const right = DatabaseId.make("orders-b");
    const proposed = [
      {
        successor: left,
        checkpointId: CheckpointId.make(
          encodeUlid(
            1,
            Array.from({ length: 16 }, () => 0),
          ),
        ),
      },
      {
        successor: right,
        checkpointId: CheckpointId.make(
          encodeUlid(
            2,
            Array.from({ length: 16 }, () => 0),
          ),
        ),
      },
    ];
    const sealed = sealManifest(empty, [left, right]);
    const options = {
      pinnedManifestId: ManifestId.make(7),
      proposed,
      createdAt: Timestamp.make(5),
    };
    const held = Result.getOrThrow(holdForSuccessors(sealed, options));
    expect(
      held.checkpoints.map((checkpoint) => [checkpoint.holder, checkpoint.manifestId]),
    ).toEqual([
      [Option.some(left), 7],
      [Option.some(right), 7],
    ]);
    expect(successorHoldsOf(held, [right, left])).toEqual([proposed[1], proposed[0]]);
    // Repeating it (a resumed split with new proposed ids) keeps the holds already taken.
    const again = holdForSuccessors(held, {
      ...options,
      pinnedManifestId: ManifestId.make(9),
      proposed: proposed.map((hold, index) => ({
        ...hold,
        checkpointId: CheckpointId.make(
          encodeUlid(
            10 + index,
            Array.from({ length: 16 }, () => 0),
          ),
        ),
      })),
    });
    expect(Result.getOrThrow(again)).toBe(held);
    const removed = Result.getOrThrow(markRemoved(empty, Timestamp.make(1)));
    expect(Result.isFailure(holdForSuccessors(removed, options))).toBe(true);
  });
});

describe("changelog coverage (D80)", () => {
  /** A flush at `cut` whose changelog SST holds rows `firstSeq..lastSeq`, created at `at`. */
  const flushWithChangelog = (
    manifest: Manifest,
    n: number,
    rows: { readonly firstSeq: number; readonly lastSeq: number; readonly cut: number },
  ) =>
    Result.getOrThrow(
      commitL0Flush(manifest, {
        writerEpoch: manifest.writerEpoch,
        sst: handle(n, n, n + 1),
        lastSeq: SeqNum.make(rows.cut),
        clockTick: Timestamp.make(n),
        changelog: Option.some({
          sst: handle(1000 + n, 0, 1),
          firstSeq: SeqNum.make(rows.firstSeq),
          lastSeq: SeqNum.make(rows.lastSeq),
          createdAt: Timestamp.make(n),
        }),
      }),
    );

  it("starts at 0, rises past what retention prunes, and starts a fork at its fork point", () => {
    const first = flushWithChangelog(empty, 1, { firstSeq: 1, lastSeq: 3, cut: 3 });
    const second = flushWithChangelog(first, 2, { firstSeq: 4, lastSeq: 5, cut: 6 });
    const prunedFirst = pruneChangelog(second, Timestamp.make(2));
    const prunedAll = pruneChangelog(second, Timestamp.make(3));
    const fork = forkManifest(second, {
      databaseId: DatabaseId.make("orders-a"),
      checkpointId: CheckpointId.make("01JD0000000000000000000CP1"),
      range: FULL_KEY_RANGE,
    });
    expect(
      [empty, second, prunedFirst, prunedAll, fork].map((manifest) =>
        changelogAfterSeqOf(manifest),
      ),
    ).toEqual([0, 0, 3, 5, 6]);
    // The boundary itself is held; one below it is not.
    expect(changelogHoldsChangesAfter(prunedFirst, SeqNum.make(3))).toBe(true);
    expect(changelogHoldsChangesAfter(prunedFirst, SeqNum.make(2))).toBe(false);
  });

  it("reads a manifest without the field (older code, or a rollback's rewrite) on the safe side", () => {
    const written = readFileSync(
      new URL("./fixtures/manifest-written-by-workstream-1.json", import.meta.url),
      "utf8",
    );
    const decoded = Schema.decodeSync(ManifestJson)(written);
    expect(decoded.changelogAfterSeq).toEqual(Option.none());
    // No entry kept: nothing below lastL0Seq is provably held.
    expect(changelogAfterSeqOf(decoded)).toBe(decoded.lastL0Seq);
    assertProperty(
      Arbitrary.array(
        Arbitrary.all({
          rows: Arbitrary.schema(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 3 }))),
          gap: Arbitrary.schema(Schema.Boolean),
          prune: Arbitrary.schema(Schema.Boolean),
        }),
        { maxLength: 12 },
      ),
      (steps) => {
        let manifest = empty;
        let cut = 0;
        for (const [n, step] of steps.entries()) {
          // A flush of `rows` sequence numbers, the first possibly without a row.
          const firstSeq = cut + 1 + (step.gap ? 1 : 0);
          const nextCut = cut + 1 + (step.gap ? 1 : 0) + step.rows;
          manifest =
            step.rows === 0
              ? Result.getOrThrow(
                  commitL0Flush(manifest, {
                    writerEpoch: manifest.writerEpoch,
                    sst: handle(n + 1, 0, 1),
                    lastSeq: SeqNum.make(nextCut),
                    clockTick: Timestamp.make(n + 1),
                    changelog: Option.none(),
                  }),
                )
              : flushWithChangelog(manifest, n + 1, {
                  firstSeq,
                  lastSeq: nextCut,
                  cut: nextCut,
                });
          cut = nextCut;
          if (step.prune) manifest = pruneChangelog(manifest, Timestamp.make(n + 1));
          const explicit = changelogAfterSeqOf(manifest);
          const derived = changelogAfterSeqOf(
            replaceManifestFields(manifest, { changelogAfterSeq: Option.none() }),
          );
          // The default is never below what the field says, and the field never claims a
          // change no kept entry holds.
          const claimsTooMuch = manifest.changelog.some((entry) => entry.firstSeq <= explicit);
          if (derived < explicit || claimsTooMuch) return false;
        }
        return true;
      },
    );
  });
});
