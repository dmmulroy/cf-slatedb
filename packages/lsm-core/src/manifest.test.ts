import { Option, Result, Schema } from "effect";
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
import { encodeUlid } from "./ulid.ts";
import {
  addCheckpoint,
  advanceGcSstBoundary,
  commitCompaction,
  commitL0Flush,
  type CompactionJob,
  DEFAULT_DATABASE_OPTIONS,
  fenceCompactor,
  fenceWriter,
  forkManifest,
  holdForSuccessors,
  markRemoved,
  releasableExternalRefs,
  initialManifest,
  Manifest,
  ManifestJson,
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
    const commit = { compactorEpoch: manifest.compactorEpoch, job, outputs: [handle(10, 1, 4)] };
    const compacted = Result.getOrThrow(commitCompaction(manifest, commit));
    expect(compacted.l0.map((sst) => sst.id)).toEqual([ulid(5), ulid(4)]);
    expect(compacted.sortedRuns.map((run) => [run.id, run.ssts.map((sst) => sst.id)])).toEqual([
      [0, [ulid(10)]],
    ]);
    expect(Result.getOrThrow(commitCompaction(compacted, commit))).toBe(compacted);

    const changed = commitCompaction(compacted, { ...commit, outputs: [handle(11, 1, 4)] });
    expect(Result.isFailure(changed) && changed.failure._tag).toBe("CompactionInputsChangedError");
    const fenced = commitCompaction(fenceCompactor(manifest), commit);
    expect(Result.isFailure(fenced) && fenced.failure._tag).toBe("CompactorFencedError");
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
      commitCompaction(flushed, { compactorEpoch: flushed.compactorEpoch, job, outputs: [output] });
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
    const refused = markRemoved(held);
    expect(Result.isFailure(refused) && refused.failure.dependents).toEqual(["orders-b"]);
    const removed = Result.getOrThrow(markRemoved(removeCheckpoint(held, checkpoint.id)));
    expect(removed.state._tag).toBe("Removed");
    expect(removed.writerEpoch).toBe(empty.writerEpoch + 1);
    expect(Result.getOrThrow(markRemoved(removed))).toBe(removed);
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
    // But a checkpoint the fork holds pins a manifest that still reads the owner's SST (D13).
    expect(releasableExternalRefs(rewritten, [fork])).toEqual([]);
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
    const removed = Result.getOrThrow(markRemoved(empty));
    expect(Result.isFailure(holdForSuccessors(removed, options))).toBe(true);
  });
});
