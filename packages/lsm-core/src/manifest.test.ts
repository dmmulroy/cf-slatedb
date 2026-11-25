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
import { keyRangeContains } from "./key-range.ts";
import {
  addCheckpoint,
  commitCompaction,
  commitL0Flush,
  type CompactionJob,
  DEFAULT_DATABASE_OPTIONS,
  fenceCompactor,
  fenceWriter,
  forkManifest,
  initialManifest,
  Manifest,
  ManifestJson,
  removeCheckpoint,
  sealManifest,
  type SstHandle,
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

/** A copy of `base` with some fields replaced (class instances cannot be spread). */
const manifestWith = (base: Manifest, fields: Partial<Manifest>): Manifest =>
  new Manifest({
    databaseId: fields.databaseId ?? base.databaseId,
    writerEpoch: fields.writerEpoch ?? base.writerEpoch,
    compactorEpoch: fields.compactorEpoch ?? base.compactorEpoch,
    lastL0Seq: fields.lastL0Seq ?? base.lastL0Seq,
    lastL0ClockTick: fields.lastL0ClockTick ?? base.lastL0ClockTick,
    l0: fields.l0 ?? base.l0,
    sortedRuns: fields.sortedRuns ?? base.sortedRuns,
    checkpoints: fields.checkpoints ?? base.checkpoints,
    keyRange: fields.keyRange ?? base.keyRange,
    state: fields.state ?? base.state,
    externalRefs: fields.externalRefs ?? base.externalRefs,
    changelog: fields.changelog ?? base.changelog,
    options: fields.options ?? base.options,
  });

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

  it("adds and removes checkpoints idempotently", () => {
    const checkpoint = {
      id: CheckpointId.make("01JD0000000000000000000CP1"),
      manifestId: ManifestId.make(1),
      createdAt: Timestamp.make(1),
      expireAt: Option.none(),
      holder: Option.none(),
      name: Option.none(),
    };
    const added = addCheckpoint(empty, checkpoint);
    expect(addCheckpoint(added, checkpoint)).toBe(added);
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
});
