import { Effect, Option, Result, Schema } from "effect";

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
import {
  intersectKeyRanges,
  KeyRange,
  keyRangeOverlapsClosed,
  FULL_KEY_RANGE,
} from "./key-range.ts";
import { MergeOperatorName } from "./row-resolution.ts";
import type { SstInfo } from "./sst-codec.ts";
import { ulidTimeMillis } from "./ulid.ts";

/**
 * Schema for a reference to one SST as the manifest records it.
 *
 * **Details**
 *
 * `metaOffset` and `metaLength` locate the metadata section, so one cacheable request opens the SST.
 * After a fork, `owner` names the database whose `db/<owner>/` prefix stores the SST and `view`
 * limits the keys this database reads from it; both are none for the database's own SSTs.
 */
export const SstHandle = Schema.Struct({
  id: SstId,
  firstKey: Schema.Uint8ArrayFromBase64,
  lastKey: Schema.Uint8ArrayFromBase64,
  sizeBytes: Schema.Int,
  rowCount: Schema.Int,
  metaOffset: Schema.Int,
  metaLength: Schema.Int,
  owner: Schema.OptionFromNullOr(DatabaseId),
  view: Schema.OptionFromNullOr(KeyRange),
});

/** A reference to one SST. */
export type SstHandle = typeof SstHandle.Type;

/** Builds the handle of a database's own new SST from its id and {@link SstInfo}. */
export const sstHandleFromInfo = (id: SstId, info: SstInfo): SstHandle => ({
  id,
  firstKey: info.footer.firstKey,
  lastKey: info.footer.lastKey,
  sizeBytes: info.sizeBytes,
  rowCount: info.footer.rowCount,
  metaOffset: info.metaOffset,
  metaLength: info.metaLength,
  owner: Option.none(),
  view: Option.none(),
});

/** The key range a handle exposes: its view, or every key. */
export const sstHandleVisibleRange = (handle: SstHandle): KeyRange =>
  Option.getOrElse(handle.view, () => FULL_KEY_RANGE);

/** The database that stores an SST: its owner after a fork, else `self`. */
export const sstHandleOwner = (handle: SstHandle, self: DatabaseId): DatabaseId =>
  Option.getOrElse(handle.owner, () => self);

/** Schema for a sorted run: non-overlapping SSTs in key order. Higher ids hold newer data. */
export const SortedRun = Schema.Struct({ id: SortedRunId, ssts: Schema.Array(SstHandle) });

/** A sorted run. */
export type SortedRun = typeof SortedRun.Type;

/**
 * Schema for a checkpoint: a pin on one manifest so GC keeps every SST it references.
 *
 * **Details**
 *
 * `holder` names the fork that holds the checkpoint on this (its source) database; such checkpoints
 * have no expiry and are released by the fork once compaction has rewritten every SST it reads from
 * here.
 */
export const Checkpoint = Schema.Struct({
  id: CheckpointId,
  manifestId: ManifestId,
  createdAt: Timestamp,
  expireAt: Schema.OptionFromNullOr(Timestamp),
  holder: Schema.OptionFromNullOr(DatabaseId),
  name: Schema.OptionFromNullOr(Schema.String),
});

/** A checkpoint. */
export type Checkpoint = typeof Checkpoint.Type;

/** Schema for a fork's dependency on a checkpoint it holds in its source database. */
export const ExternalRef = Schema.Struct({ owner: DatabaseId, checkpointId: CheckpointId });

/** A fork's dependency on a checkpoint in its source database. */
export type ExternalRef = typeof ExternalRef.Type;

/**
 * Schema for a database's lifecycle state: open, sealed with the databases that replace it, or
 * being removed (its objects are deleted next; nothing may write to it or fork from it).
 */
export const DatabaseState = Schema.Union([
  Schema.TaggedStruct("Open", {}),
  Schema.TaggedStruct("Sealed", { successors: Schema.Array(DatabaseId) }),
  Schema.TaggedStruct("Removed", {}),
]);

/** A database's lifecycle state. */
export type DatabaseState = typeof DatabaseState.Type;

/** Schema for one changelog SST exported by a flush (change data capture). */
export const ChangelogEntry = Schema.Struct({
  sst: SstHandle,
  firstSeq: SeqNum,
  lastSeq: SeqNum,
  createdAt: Timestamp,
});

/** One changelog SST. */
export type ChangelogEntry = typeof ChangelogEntry.Type;

/**
 * Schema for the options a database is created with; readers, writers, and compactors all read
 * them from the manifest.
 *
 * **Details**
 *
 * Data residency is not a database option: a whole Stack runs in one jurisdiction (D49). Manifests
 * written before that carry a `jurisdiction` field; decoding ignores it and the next manifest
 * written drops it.
 */
export const DatabaseOptions = Schema.Struct({
  /** Merge operator that resolves merge operands; none makes merge rows unreadable. */
  mergeOperator: Schema.OptionFromNullOr(MergeOperatorName),
  /** Group for fair compaction and cache budgets; defaults to the database id. */
  budgetGroup: Schema.OptionFromNullOr(Schema.String),
  /** Changelog export: none disables it; retention is how long GC keeps changelog SSTs. */
  changelog: Schema.OptionFromNullOr(Schema.Struct({ retentionMs: Schema.Int })),
});

/** The options a database is created with. */
export type DatabaseOptions = typeof DatabaseOptions.Type;

/** Default options: no merge operator, budget group = database id, no changelog. */
export const DEFAULT_DATABASE_OPTIONS: DatabaseOptions = {
  mergeOperator: Option.none(),
  budgetGroup: Option.none(),
  changelog: Option.none(),
};

/** Schema for one compaction input: an L0 SST or a sorted run. */
export const CompactionSource = Schema.Union([
  Schema.TaggedStruct("L0", { sstId: SstId }),
  Schema.TaggedStruct("SortedRun", { id: SortedRunId }),
]);

/** One compaction input. */
export type CompactionSource = typeof CompactionSource.Type;

/**
 * Schema for an SST an operator removed from the manifest (D62): which SST, where it was, the keys
 * it covered (its versions of those keys are gone; older versions below it show again), why, and
 * when.
 */
export const DroppedSst = Schema.Struct({
  sstId: SstId,
  /** The database whose prefix stores it: none for the database's own SSTs. */
  owner: Schema.OptionFromNullOr(DatabaseId),
  /** The L0 SST or the sorted run that held it. */
  from: CompactionSource,
  /** The keys it held, [firstKey, lastKey]; the database read only those inside `view` (if any). */
  firstKey: Schema.Uint8ArrayFromBase64,
  lastKey: Schema.Uint8ArrayFromBase64,
  view: Schema.OptionFromNullOr(KeyRange),
  rowCount: Schema.Int,
  /** `Corrupted`: the compactor had reported it; `Forced`: dropped without that report. */
  reason: Schema.Literals(["Corrupted", "Forced"]),
  droppedAt: Timestamp,
});

/** An SST an operator removed from the manifest. */
export type DroppedSst = typeof DroppedSst.Type;

/**
 * The newest manifest format this code reads and writes (D64). A change older code cannot read
 * (a field whose absence would change what it does) raises it; additive fields that older code may
 * ignore do not. Manifests written before format versions decode as 1.
 */
export const MANIFEST_FORMAT_VERSION = 1;

/** How many dropped SSTs a manifest remembers, newest last; older entries fall off. */
export const MAX_DROPPED_SST_HISTORY = 32;

/**
 * A database's manifest: the complete, immutable description of one version of the LSM.
 *
 * **Details**
 *
 * Stored as `db/<id>/manifest/<manifestId>` with create-if-absent, so each id is written once.
 * `l0` is newest first and may overlap; `sortedRuns` is ordered by id descending (newest first).
 * `lastL0Seq` is the newest sequence number in L0 or below; `lastL0ClockTick` seeds the writer's
 * monotonic clock. Lifecycle primitives (`keyRange`, `state`, `externalRefs`, fork `checkpoints`) are
 * part of v1.
 *
 * `gcSstBoundary` is GC's SST deletion boundary: GC advances it (by CAS) before deleting unreferenced
 * SSTs created before it, and commits reject new SSTs created before it (`SstBelowGcBoundaryError`),
 * so an SST that is uploaded but not yet committed is never deleted out from under its commit.
 *
 * `droppedSsts` records SSTs an operator removed (D62), newest last; manifests written before it
 * decode with none.
 *
 * `changelogAfterSeq` says what the change feed can prove it holds (D80): `changelog` has every
 * change with a sequence number above it, up to `lastL0Seq`. Retention raises it past what it
 * prunes, and a fork starts it at the fork point. Manifests written before it decode with none;
 * read it through {@link changelogAfterSeqOf}. `formatVersion` is the format it was written in ({@link MANIFEST_FORMAT_VERSION});
 * code refuses to read, and so to write over, a newer one (D64).
 */
export class Manifest extends Schema.Class<Manifest>("Manifest")({
  formatVersion: Schema.Int.pipe(Schema.withDecodingDefaultKey(Effect.succeed(1))),
  databaseId: DatabaseId,
  writerEpoch: WriterEpoch,
  compactorEpoch: CompactorEpoch,
  lastL0Seq: SeqNum,
  lastL0ClockTick: Timestamp,
  l0: Schema.Array(SstHandle),
  sortedRuns: Schema.Array(SortedRun),
  checkpoints: Schema.Array(Checkpoint),
  keyRange: Schema.OptionFromNullOr(KeyRange),
  state: DatabaseState,
  externalRefs: Schema.Array(ExternalRef),
  changelog: Schema.Array(ChangelogEntry),
  options: DatabaseOptions,
  gcSstBoundary: Timestamp,
  droppedSsts: Schema.Array(DroppedSst).pipe(Schema.withDecodingDefaultKey(Effect.succeed([]))),
  changelogAfterSeq: Schema.OptionFromOptionalKey(SeqNum),
}) {}

/**
 * The sequence number after which the manifest's changelog holds every change, up to `lastL0Seq`
 * (D80).
 *
 * **Details**
 *
 * A manifest written before D80, or rewritten by older code that did not know the field, has
 * none. It then reads as the oldest kept entry's `firstSeq - 1`, or `lastL0Seq` when no entry is
 * kept. That is never below the true value: every change at or below it that the changelog lacks
 * was pruned, or belongs to a fork's source. So the default only answers `Expired` more often,
 * never with a silent gap.
 */
export const changelogAfterSeqOf = (manifest: Manifest): SeqNum =>
  Option.getOrElse(manifest.changelogAfterSeq, () => {
    const firstSeqs = manifest.changelog.map((entry) => entry.firstSeq);
    return firstSeqs.length === 0
      ? manifest.lastL0Seq
      : SeqNum.make(Math.max(0, Math.min(...firstSeqs) - 1));
  });

/**
 * Whether the manifest's changelog holds every change after `afterSeq` up to its `lastL0Seq`: the
 * one rule the change feed answers by (D80). When it does not, the feed answers `Expired`.
 */
export const changelogHoldsChangesAfter = (manifest: Manifest, afterSeq: SeqNum): boolean =>
  afterSeq >= changelogAfterSeqOf(manifest);

/** Codec for a manifest object's body: JSON via `Schema.fromJsonString(Manifest)`. */
export const ManifestJson = Schema.fromJsonString(Manifest);

/**
 * Builds the first manifest of a new database: empty LSM, epochs 0, open.
 */
export const initialManifest = (
  databaseId: DatabaseId,
  options: {
    readonly keyRange: Option.Option<KeyRange>;
    readonly databaseOptions: DatabaseOptions;
  },
): Manifest =>
  new Manifest({
    formatVersion: MANIFEST_FORMAT_VERSION,
    databaseId,
    writerEpoch: WriterEpoch.make(0),
    compactorEpoch: CompactorEpoch.make(0),
    lastL0Seq: SeqNum.make(0),
    lastL0ClockTick: Timestamp.make(0),
    l0: [],
    sortedRuns: [],
    checkpoints: [],
    keyRange: options.keyRange,
    state: { _tag: "Open" },
    externalRefs: [],
    changelog: [],
    options: options.databaseOptions,
    gcSstBoundary: Timestamp.make(0),
    droppedSsts: [],
    changelogAfterSeq: Option.some(SeqNum.make(0)),
  });

/** Every SST handle the manifest references: L0 newest first, then each sorted run. */
export const manifestSstHandles = (manifest: Manifest): ReadonlyArray<SstHandle> => [
  ...manifest.l0,
  ...manifest.sortedRuns.flatMap((run) => run.ssts),
];

// ---------------------------------------------------------------------------------------------
// Edit errors
// ---------------------------------------------------------------------------------------------

/**
 * A writer tried to commit after a newer writer took over the database.
 *
 * **Details**
 *
 * Not retryable for this writer: it must stop. Its uploaded SSTs are orphans that GC removes.
 */
export class WriterFencedError extends Schema.TaggedError<WriterFencedError>()(
  "WriterFencedError",
  {
    databaseId: DatabaseId,
    writerEpoch: WriterEpoch,
    currentWriterEpoch: WriterEpoch,
    message: Schema.String,
  },
) {}

/** A compactor tried to commit after a newer compactor took over the database. Not retryable. */
export class CompactorFencedError extends Schema.TaggedError<CompactorFencedError>()(
  "CompactorFencedError",
  {
    databaseId: DatabaseId,
    compactorEpoch: CompactorEpoch,
    currentCompactorEpoch: CompactorEpoch,
    message: Schema.String,
  },
) {}

/**
 * The database is sealed: it accepts no more writes. `successors` are the databases that replace it.
 *
 * **Details**
 *
 * Not retryable against this database; route the write to the successor whose key range holds it.
 */
export class DatabaseSealedError extends Schema.TaggedError<DatabaseSealedError>()(
  "DatabaseSealedError",
  {
    databaseId: DatabaseId,
    successors: Schema.Array(DatabaseId),
    message: Schema.String,
  },
) {}

/**
 * A compaction's inputs are no longer what the manifest holds (another edit replaced them), so its
 * output must not be committed. Discard the output; GC removes it.
 */
export class CompactionInputsChangedError extends Schema.TaggedError<CompactionInputsChangedError>()(
  "CompactionInputsChangedError",
  { databaseId: DatabaseId, message: Schema.String },
) {}

/**
 * A flush or compaction tried to commit an SST created before the manifest's `gcSstBoundary`: GC may
 * already have deleted it as an orphan. Not retryable with this SST; write the data to a new SST
 * (the writer re-flushes; the compactor discards the job and plans again).
 */
export class SstBelowGcBoundaryError extends Schema.TaggedError<SstBelowGcBoundaryError>()(
  "SstBelowGcBoundaryError",
  {
    databaseId: DatabaseId,
    sstId: SstId,
    gcSstBoundary: Timestamp,
    message: Schema.String,
  },
) {}

/** The creation time GC compares with its boundary: the time in the SST's ULID. */
export const sstCreatedAt = (sstId: SstId): Timestamp => Timestamp.make(ulidTimeMillis(sstId));

/** The first new SST created before the manifest's GC boundary, as an error; none when all are newer. */
const belowGcBoundary = (
  manifest: Manifest,
  sstIds: ReadonlyArray<SstId>,
): Option.Option<SstBelowGcBoundaryError> => {
  const stale = sstIds.find((sstId) => sstCreatedAt(sstId) < manifest.gcSstBoundary);
  return stale === undefined
    ? Option.none()
    : Option.some(
        new SstBelowGcBoundaryError({
          databaseId: manifest.databaseId,
          sstId: stale,
          gcSstBoundary: manifest.gcSstBoundary,
          message: `SST ${stale} of database ${manifest.databaseId} was created before GC boundary ${manifest.gcSstBoundary}, so GC may have deleted it. Write the data to a new SST and commit that instead.`,
        }),
      );
};

/**
 * The database is being removed: it accepts no writes, checkpoints, or forks. Not retryable; the
 * database is gone once removal finishes.
 */
export class DatabaseRemovedError extends Schema.TaggedError<DatabaseRemovedError>()(
  "DatabaseRemovedError",
  { databaseId: DatabaseId, message: Schema.String },
) {}

/**
 * The database cannot be removed while forks still read its SSTs through checkpoints they hold on
 * it. Not retryable now: remove the forks first, or wait until their compactions rewrite every SST
 * they read from it (they then release their checkpoints).
 */
export class DatabaseHasDependentsError extends Schema.TaggedError<DatabaseHasDependentsError>()(
  "DatabaseHasDependentsError",
  { databaseId: DatabaseId, dependents: Schema.Array(DatabaseId), message: Schema.String },
) {}

/**
 * The SST is not in the database's newest manifest, so it cannot be dropped. Not retryable: check
 * the SST id against `GET /v1/databases/:id/compaction`.
 */
export class SstNotInManifestError extends Schema.TaggedError<SstNotInManifestError>()(
  "SstNotInManifestError",
  { databaseId: DatabaseId, sstId: SstId, message: Schema.String },
) {}

/**
 * Most checkpoints a caller may hold on one database at once (D99): checkpoints and pinned
 * snapshots, not the holds forks and split successors take. Each is about 130 bytes of the
 * manifest, which every commit rewrites and every cold read decodes, so 1,000 is about 127 KiB, 3%
 * of the manifest's 4 MiB budget.
 */
export const MAX_CHECKPOINTS_PER_DATABASE = 1_000;

/**
 * The database already holds {@link MAX_CHECKPOINTS_PER_DATABASE} checkpoints created by callers.
 * Not retryable as it is: delete a checkpoint, or let a pinned snapshot expire, then retry.
 */
export class CheckpointLimitError extends Schema.TaggedError<CheckpointLimitError>()(
  "CheckpointLimitError",
  { databaseId: DatabaseId, checkpoints: Schema.Int, limit: Schema.Int, message: Schema.String },
) {}

const databaseRemoved = (manifest: Manifest) =>
  new DatabaseRemovedError({
    databaseId: manifest.databaseId,
    message: `Database ${manifest.databaseId} is being removed. It accepts no writes, checkpoints, or forks.`,
  });

const writerFenced = (manifest: Manifest, writerEpoch: WriterEpoch) =>
  new WriterFencedError({
    databaseId: manifest.databaseId,
    writerEpoch,
    currentWriterEpoch: manifest.writerEpoch,
    message: `Writer epoch ${writerEpoch} of database ${manifest.databaseId} was fenced by epoch ${manifest.writerEpoch}. This writer must stop; the newer writer serves the database.`,
  });

const databaseSealed = (manifest: Manifest, successors: ReadonlyArray<DatabaseId>) =>
  new DatabaseSealedError({
    databaseId: manifest.databaseId,
    successors,
    message: `Database ${manifest.databaseId} is sealed and accepts no writes${
      successors.length > 0 ? `; its successors are ${successors.join(", ")}` : ""
    }. Send the write to the successor whose key range holds the key.`,
  });

/** The successors of a sealed database, or none when it is open. */
export const sealedSuccessors = (manifest: Manifest): Option.Option<ReadonlyArray<DatabaseId>> =>
  manifest.state._tag === "Sealed" ? Option.some(manifest.state.successors) : Option.none();

/** A copy of `manifest` with some fields replaced (`Schema.Class` instances cannot be spread). */
export const replaceManifestFields = (manifest: Manifest, fields: Partial<Manifest>): Manifest =>
  new Manifest({
    formatVersion: fields.formatVersion ?? manifest.formatVersion,
    databaseId: fields.databaseId ?? manifest.databaseId,
    writerEpoch: fields.writerEpoch ?? manifest.writerEpoch,
    compactorEpoch: fields.compactorEpoch ?? manifest.compactorEpoch,
    lastL0Seq: fields.lastL0Seq ?? manifest.lastL0Seq,
    lastL0ClockTick: fields.lastL0ClockTick ?? manifest.lastL0ClockTick,
    l0: fields.l0 ?? manifest.l0,
    sortedRuns: fields.sortedRuns ?? manifest.sortedRuns,
    checkpoints: fields.checkpoints ?? manifest.checkpoints,
    keyRange: fields.keyRange ?? manifest.keyRange,
    state: fields.state ?? manifest.state,
    externalRefs: fields.externalRefs ?? manifest.externalRefs,
    changelog: fields.changelog ?? manifest.changelog,
    options: fields.options ?? manifest.options,
    gcSstBoundary: fields.gcSstBoundary ?? manifest.gcSstBoundary,
    droppedSsts: fields.droppedSsts ?? manifest.droppedSsts,
    changelogAfterSeq: fields.changelogAfterSeq ?? manifest.changelogAfterSeq,
  });

// ---------------------------------------------------------------------------------------------
// Edits: each is pure; returning the same `Manifest` instance means "already applied, write nothing".
// ---------------------------------------------------------------------------------------------

/**
 * A new writer takes over: increments `writerEpoch`.
 *
 * @returns The fenced manifest, `DatabaseSealedError` when the database is sealed, or
 *   `DatabaseRemovedError` when it is being removed (no writer may start).
 */
export const fenceWriter = (
  manifest: Manifest,
): Result.Result<Manifest, DatabaseSealedError | DatabaseRemovedError> => {
  switch (manifest.state._tag) {
    case "Sealed":
      return Result.fail(databaseSealed(manifest, manifest.state.successors));
    case "Removed":
      return Result.fail(databaseRemoved(manifest));
    case "Open":
      return Result.succeed(
        replaceManifestFields(manifest, {
          writerEpoch: WriterEpoch.make(manifest.writerEpoch + 1),
        }),
      );
  }
};

/** A new compactor takes over: increments `compactorEpoch`. */
export const fenceCompactor = (manifest: Manifest): Manifest =>
  replaceManifestFields(manifest, {
    compactorEpoch: CompactorEpoch.make(manifest.compactorEpoch + 1),
  });

/** One memtable flush to commit. */
export interface L0Flush {
  readonly writerEpoch: WriterEpoch;
  readonly sst: SstHandle;
  /** Newest sequence number in the flushed SST (the flush cut). */
  readonly lastSeq: SeqNum;
  /** Newest clock tick of the flushed rows. */
  readonly clockTick: Timestamp;
  readonly changelog: Option.Option<ChangelogEntry>;
}

/**
 * Commits a memtable flush: prepends the SST to L0 and advances `lastL0Seq` (and appends the
 * changelog entry, if any).
 *
 * @returns The new manifest; the same manifest when this SST is already in L0 (a retried commit
 *   that had succeeded); `WriterFencedError` when another writer took over;
 *   `DatabaseSealedError` when the database was sealed; or `SstBelowGcBoundaryError` when the SST
 *   (or changelog SST) predates GC's boundary.
 */
export const commitL0Flush = (
  manifest: Manifest,
  flush: L0Flush,
): Result.Result<Manifest, WriterFencedError | DatabaseSealedError | SstBelowGcBoundaryError> => {
  if (manifest.l0.some((sst) => sst.id === flush.sst.id)) return Result.succeed(manifest);
  // Sealing and removal both increment writerEpoch, so a writer from before either is fenced here.
  if (manifest.writerEpoch !== flush.writerEpoch)
    return Result.fail(writerFenced(manifest, flush.writerEpoch));
  if (manifest.state._tag === "Sealed")
    return Result.fail(databaseSealed(manifest, manifest.state.successors));
  const stale = belowGcBoundary(manifest, [
    flush.sst.id,
    ...Option.toArray(flush.changelog).map((entry) => entry.sst.id),
  ]);
  if (Option.isSome(stale)) return Result.fail(stale.value);
  return Result.succeed(
    capChangelog(
      replaceManifestFields(manifest, {
        l0: [flush.sst, ...manifest.l0],
        lastL0Seq: flush.lastSeq > manifest.lastL0Seq ? flush.lastSeq : manifest.lastL0Seq,
        lastL0ClockTick:
          flush.clockTick > manifest.lastL0ClockTick ? flush.clockTick : manifest.lastL0ClockTick,
        changelog: [...manifest.changelog, ...Option.toArray(flush.changelog)],
      }),
    ),
  );
};

/**
 * Schema for a compaction job: merge `sources` into sorted run `destination`.
 *
 * **Details**
 *
 * `isBottom` is true when no data older than the sources exists, so deletes can be dropped.
 */
export const CompactionJob = Schema.Struct({
  sources: Schema.Array(CompactionSource),
  destination: SortedRunId,
  isBottom: Schema.Boolean,
});

/** A compaction job. */
export type CompactionJob = typeof CompactionJob.Type;

/** A finished compaction to commit. */
export interface CompactionCommit {
  readonly compactorEpoch: CompactorEpoch;
  readonly job: CompactionJob;
  /** Output SSTs in key order; may be empty when every row was dropped. */
  readonly outputs: ReadonlyArray<SstHandle>;
}

/**
 * Commits a compaction: replaces exactly its input L0 SSTs and sorted runs with the destination
 * sorted run.
 *
 * @returns The new manifest; the same manifest when the commit already happened; or
 *   `CompactorFencedError` / `CompactionInputsChangedError` / `SstBelowGcBoundaryError` when the
 *   compactor was fenced, an input is no longer in the manifest, or an output predates GC's
 *   boundary. The edit never merges into changed inputs.
 */
export const commitCompaction = (
  manifest: Manifest,
  commit: CompactionCommit,
): Result.Result<
  Manifest,
  CompactorFencedError | CompactionInputsChangedError | SstBelowGcBoundaryError
> => {
  const l0Inputs = new Set(
    commit.job.sources.flatMap((source) => (source._tag === "L0" ? [source.sstId] : [])),
  );
  const runInputs = new Set(
    commit.job.sources.flatMap((source) => (source._tag === "SortedRun" ? [source.id] : [])),
  );
  const destination = manifest.sortedRuns.find((run) => run.id === commit.job.destination);
  const alreadyApplied =
    commit.outputs.length > 0 &&
    destination !== undefined &&
    destination.ssts.length === commit.outputs.length &&
    destination.ssts.every((sst, index) => sst.id === commit.outputs[index]?.id) &&
    !manifest.l0.some((sst) => l0Inputs.has(sst.id));
  if (alreadyApplied) return Result.succeed(manifest);
  if (manifest.compactorEpoch !== commit.compactorEpoch) {
    return Result.fail(
      new CompactorFencedError({
        databaseId: manifest.databaseId,
        compactorEpoch: commit.compactorEpoch,
        currentCompactorEpoch: manifest.compactorEpoch,
        message: `Compactor epoch ${commit.compactorEpoch} of database ${manifest.databaseId} was fenced by epoch ${manifest.compactorEpoch}. Discard this compaction's output; the newer compactor continues.`,
      }),
    );
  }
  const presentL0 = manifest.l0.filter((sst) => l0Inputs.has(sst.id)).length;
  const presentRuns = manifest.sortedRuns.filter((run) => runInputs.has(run.id)).length;
  if (presentL0 !== l0Inputs.size || presentRuns !== runInputs.size) {
    return Result.fail(
      new CompactionInputsChangedError({
        databaseId: manifest.databaseId,
        message: `A compaction of database ${manifest.databaseId} cannot commit because its inputs are no longer in the manifest. Its output is discarded; the scheduler plans again from the current manifest.`,
      }),
    );
  }
  const stale = belowGcBoundary(
    manifest,
    commit.outputs.map((output) => output.id),
  );
  if (Option.isSome(stale)) return Result.fail(stale.value);
  const remainingRuns = manifest.sortedRuns.filter((run) => !runInputs.has(run.id));
  const sortedRuns =
    commit.outputs.length === 0
      ? remainingRuns
      : [...remainingRuns, { id: commit.job.destination, ssts: commit.outputs }].sort(
          (left, right) => right.id - left.id,
        );
  return Result.succeed(
    replaceManifestFields(manifest, {
      l0: manifest.l0.filter((sst) => !l0Inputs.has(sst.id)),
      sortedRuns,
    }),
  );
};

/**
 * Seals the database: no more writes; `successors` replace it. Also increments `writerEpoch`, which
 * fences the current writer.
 *
 * @returns The sealed manifest, or the same manifest when it is already sealed with these successors.
 */
export const sealManifest = (
  manifest: Manifest,
  successors: ReadonlyArray<DatabaseId>,
): Manifest =>
  manifest.state._tag === "Sealed" &&
  manifest.state.successors.length === successors.length &&
  manifest.state.successors.every((id, index) => id === successors[index])
    ? manifest
    : replaceManifestFields(manifest, {
        state: { _tag: "Sealed", successors },
        writerEpoch: WriterEpoch.make(manifest.writerEpoch + 1),
      });

/**
 * Advances GC's SST boundary to `boundary`; the same manifest when it is already there or later (the
 * boundary never moves back).
 */
export const advanceGcSstBoundary = (manifest: Manifest, boundary: Timestamp): Manifest =>
  boundary > manifest.gcSstBoundary
    ? replaceManifestFields(manifest, { gcSstBoundary: boundary })
    : manifest;

/**
 * Adds a checkpoint; the same manifest when a checkpoint with its id exists.
 *
 * @returns The manifest, or `DatabaseRemovedError` when the database is being removed (a fork
 *   could otherwise pin files about to be deleted).
 */
export const addCheckpoint = (
  manifest: Manifest,
  checkpoint: Checkpoint,
): Result.Result<Manifest, DatabaseRemovedError> => {
  if (manifest.checkpoints.some((existing) => existing.id === checkpoint.id))
    return Result.succeed(manifest);
  if (manifest.state._tag === "Removed") return Result.fail(databaseRemoved(manifest));
  return Result.succeed(
    replaceManifestFields(manifest, { checkpoints: [...manifest.checkpoints, checkpoint] }),
  );
};

/** A successor's hold on a sealed database: the checkpoint its fork reads through. */
export interface SuccessorHold {
  readonly successor: DatabaseId;
  readonly checkpointId: CheckpointId;
}

/**
 * Takes a hold for each successor that has none yet: a checkpoint held by the successor, pinning
 * `pinnedManifestId` until its fork's compactor releases it (as `forkRange`'s own hold does). A
 * split takes them in its seal commit, pinning the sealed manifest itself, so no separate checkpoint
 * or hold commit stands between the seal and the forks (D57).
 *
 * @param proposed - A new checkpoint id per successor, used only for successors without a hold.
 * @returns The manifest with every successor held (the same manifest when all already are), or
 *   `DatabaseRemovedError`.
 */
export const holdForSuccessors = (
  manifest: Manifest,
  options: {
    readonly pinnedManifestId: ManifestId;
    readonly proposed: ReadonlyArray<SuccessorHold>;
    readonly createdAt: Timestamp;
  },
): Result.Result<Manifest, DatabaseRemovedError> => {
  let held = manifest;
  for (const hold of options.proposed) {
    if (held.checkpoints.some((existing) => Option.contains(existing.holder, hold.successor))) {
      continue;
    }
    const added = addCheckpoint(held, {
      id: hold.checkpointId,
      manifestId: options.pinnedManifestId,
      createdAt: options.createdAt,
      expireAt: Option.none(),
      holder: Option.some(hold.successor),
      name: Option.some(`successor ${hold.successor}`),
    });
    if (Result.isFailure(added)) return added;
    held = added.success;
  }
  return Result.succeed(held);
};

/** Each successor's hold in `manifest`, in `successors` order; successors without one are left out. */
export const successorHoldsOf = (
  manifest: Manifest,
  successors: ReadonlyArray<DatabaseId>,
): ReadonlyArray<SuccessorHold> =>
  successors.flatMap((successor) =>
    Option.toArray(
      Option.map(
        Option.fromUndefinedOr(
          manifest.checkpoints.find((checkpoint) => Option.contains(checkpoint.holder, successor)),
        ),
        (checkpoint) => ({ successor, checkpointId: checkpoint.id }),
      ),
    ),
  );

/** The forks holding checkpoints on this database. */
export const dependentForks = (manifest: Manifest): ReadonlyArray<DatabaseId> => [
  ...new Set(manifest.checkpoints.flatMap((checkpoint) => Option.toArray(checkpoint.holder))),
];

/**
 * Marks the database as being removed and fences its writer (`writerEpoch + 1`), so no write,
 * checkpoint, or fork can commit after it.
 *
 * @returns The manifest; the same manifest when it is already marked; or
 *   `DatabaseHasDependentsError` while forks hold checkpoints on it.
 */
export const markRemoved = (
  manifest: Manifest,
): Result.Result<Manifest, DatabaseHasDependentsError> => {
  if (manifest.state._tag === "Removed") return Result.succeed(manifest);
  const dependents = dependentForks(manifest);
  if (dependents.length > 0) {
    return Result.fail(
      new DatabaseHasDependentsError({
        databaseId: manifest.databaseId,
        dependents,
        message: `Database ${manifest.databaseId} cannot be removed: forks ${dependents.join(", ")} still read its SSTs. Remove them first, or wait until their compactions release their checkpoints.`,
      }),
    );
  }
  return Result.succeed(
    replaceManifestFields(manifest, {
      state: { _tag: "Removed" },
      writerEpoch: WriterEpoch.make(manifest.writerEpoch + 1),
    }),
  );
};

/** Removes a checkpoint; the same manifest when it is absent. */
export const removeCheckpoint = (manifest: Manifest, checkpointId: CheckpointId): Manifest =>
  manifest.checkpoints.some((existing) => existing.id === checkpointId)
    ? replaceManifestFields(manifest, {
        checkpoints: manifest.checkpoints.filter((existing) => existing.id !== checkpointId),
      })
    : manifest;

/**
 * {@link addCheckpoint} for a checkpoint a caller asks for (no `holder`), refused with
 * {@link CheckpointLimitError} once the database holds {@link MAX_CHECKPOINTS_PER_DATABASE} of them
 * (D99). Repeating an existing checkpoint is never refused. Forks' and successors' holds are not
 * counted: a split or fork must not fail because of a caller's checkpoints.
 */
export const addCallerCheckpoint = (
  manifest: Manifest,
  checkpoint: Checkpoint,
): Result.Result<Manifest, DatabaseRemovedError | CheckpointLimitError> => {
  if (manifest.checkpoints.some((existing) => existing.id === checkpoint.id)) {
    return Result.succeed(manifest);
  }
  const held = manifest.checkpoints.filter((existing) => Option.isNone(existing.holder)).length;
  if (Option.isNone(checkpoint.holder) && held >= MAX_CHECKPOINTS_PER_DATABASE) {
    return Result.fail(
      new CheckpointLimitError({
        databaseId: manifest.databaseId,
        checkpoints: held,
        limit: MAX_CHECKPOINTS_PER_DATABASE,
        message: `Database ${manifest.databaseId} already has ${held} checkpoints and pinned snapshots, the most it may hold (${MAX_CHECKPOINTS_PER_DATABASE}). Delete one, or wait for a pinned snapshot to expire, then retry.`,
      }),
    );
  }
  return addCheckpoint(manifest, checkpoint);
};

/** Removes expired checkpoints (fork checkpoints never expire); the same manifest when none expired. */
export const removeExpiredCheckpoints = (manifest: Manifest, now: Timestamp): Manifest => {
  const live = manifest.checkpoints.filter(
    (checkpoint) => Option.isNone(checkpoint.expireAt) || checkpoint.expireAt.value > now,
  );
  return live.length === manifest.checkpoints.length
    ? manifest
    : replaceManifestFields(manifest, { checkpoints: live });
};

/** Removes a fork's external reference to `owner`'s checkpoint; the same manifest when absent. */
export const removeExternalRef = (manifest: Manifest, ref: ExternalRef): Manifest =>
  manifest.externalRefs.some(
    (existing) => existing.owner === ref.owner && existing.checkpointId === ref.checkpointId,
  )
    ? replaceManifestFields(manifest, {
        externalRefs: manifest.externalRefs.filter(
          (existing) =>
            !(existing.owner === ref.owner && existing.checkpointId === ref.checkpointId),
        ),
      })
    : manifest;

/**
 * Most changelog entries one manifest keeps, however often the database flushes (D99). Each is
 * about 290 bytes of the manifest, so 2,048 is about 570 KiB, 14% of its 4 MiB budget. At one flush
 * a minute (`maxL0Age`) that is about 34 hours of changes; a database that flushes more often keeps
 * fewer hours.
 */
export const MAX_CHANGELOG_ENTRIES = 2_048;

/**
 * Keeps the newest {@link MAX_CHANGELOG_ENTRIES} changelog entries, dropping the oldest, and raises
 * `changelogAfterSeq` past what it dropped (D80, D99), so a consumer behind them gets a clean
 * `Expired` and GC collects the dropped SSTs. The same manifest when nothing is over the bound.
 */
export const capChangelog = (manifest: Manifest): Manifest => {
  const over = manifest.changelog.length - MAX_CHANGELOG_ENTRIES;
  if (over <= 0) return manifest;
  // Entries are appended by flushes, so the oldest come first.
  const dropped = manifest.changelog.slice(0, over);
  const afterSeq = SeqNum.make(
    Math.max(changelogAfterSeqOf(manifest), ...dropped.map((entry) => entry.lastSeq)),
  );
  return replaceManifestFields(manifest, {
    changelog: manifest.changelog.slice(over),
    changelogAfterSeq: Option.some(afterSeq),
  });
};

/**
 * Drops changelog entries created before `cutoff`, and raises `changelogAfterSeq` past the newest
 * change they held, so the feed answers `Expired` for what they held (D80); the same manifest when
 * none are that old.
 */
export const pruneChangelog = (manifest: Manifest, cutoff: Timestamp): Manifest => {
  const kept = manifest.changelog.filter((entry) => entry.createdAt >= cutoff);
  if (kept.length === manifest.changelog.length) return manifest;
  const dropped = manifest.changelog.filter((entry) => entry.createdAt < cutoff);
  const afterSeq = SeqNum.make(
    Math.max(changelogAfterSeqOf(manifest), ...dropped.map((entry) => entry.lastSeq)),
  );
  return replaceManifestFields(manifest, {
    changelog: kept,
    changelogAfterSeq: Option.some(afterSeq),
  });
};

/**
 * Restricts a source handle to `range` for a fork: the fork reads it under the owner's prefix and
 * only inside the intersection.
 *
 * @returns The fork's handle, or none when the SST has no key in `range`.
 */
const forkHandle = (
  handle: SstHandle,
  source: DatabaseId,
  range: KeyRange,
): Option.Option<SstHandle> => {
  if (!keyRangeOverlapsClosed(range, handle.firstKey, handle.lastKey)) return Option.none();
  return Option.map(intersectKeyRanges(sstHandleVisibleRange(handle), range), (view) => ({
    ...handle,
    owner: Option.some(sstHandleOwner(handle, source)),
    view: Option.some(view),
  }));
};

/**
 * Builds a fork's first manifest: the SSTs of `checkpointManifest` that overlap `range`, each as a
 * view of the source's file, bounded to `range`, holding `checkpointId` in the source.
 *
 * **Details**
 *
 * Zero-copy: no SST is rewritten. The fork inherits the source's options and `lastL0Seq` (its
 * sequence numbers continue from the source's), starts at epoch 0, and is open. A fork of a fork
 * keeps each handle's original owner, but holds a checkpoint only on its direct source; the source
 * keeps its own checkpoints on the original owners while any of its checkpoints (including the one
 * this fork holds) still references their files (`docs/design-decisions.md`).
 */
export const forkManifest = (
  checkpointManifest: Manifest,
  fork: {
    readonly databaseId: DatabaseId;
    readonly checkpointId: CheckpointId;
    readonly range: KeyRange;
  },
): Manifest => {
  const source = checkpointManifest.databaseId;
  const range = Option.match(checkpointManifest.keyRange, {
    onNone: () => Option.some(fork.range),
    onSome: (sourceRange) => intersectKeyRanges(sourceRange, fork.range),
  });
  const forkRange = Option.getOrElse(range, () => fork.range);
  const restrict = (handles: ReadonlyArray<SstHandle>) =>
    handles.flatMap((handle) => Option.toArray(forkHandle(handle, source, forkRange)));
  return new Manifest({
    formatVersion: MANIFEST_FORMAT_VERSION,
    databaseId: fork.databaseId,
    writerEpoch: WriterEpoch.make(0),
    compactorEpoch: CompactorEpoch.make(0),
    lastL0Seq: checkpointManifest.lastL0Seq,
    lastL0ClockTick: checkpointManifest.lastL0ClockTick,
    l0: restrict(checkpointManifest.l0),
    sortedRuns: checkpointManifest.sortedRuns
      .map((run) => ({ id: run.id, ssts: restrict(run.ssts) }))
      .filter((run) => run.ssts.length > 0),
    checkpoints: [],
    keyRange: Option.some(forkRange),
    state: { _tag: "Open" },
    externalRefs: [{ owner: source, checkpointId: fork.checkpointId }],
    changelog: [],
    options: checkpointManifest.options,
    gcSstBoundary: Timestamp.make(0),
    // A fork starts its own repair history.
    droppedSsts: [],
    // Changes up to the fork point belong to the source's feed (D80).
    changelogAfterSeq: Option.some(checkpointManifest.lastL0Seq),
  });
};

/**
 * The external references a fork can release (a pure function): all of them once no manifest a
 * reader may still hold names another database's SST, none before (D77).
 *
 * **Details**
 *
 * `kept` must be every manifest GC keeps (`planManifestCollection`'s retained set): the newest,
 * every manifest a checkpoint pins, and every manifest superseded less than `gcMinAge` ago. A reader
 * holding any of them reads the SSTs it names, so the hold that protects those SSTs stays.
 *
 * A database has at most one external reference, the hold `forkManifest` names on its direct
 * source, and every other database's SST it reads came through that source's checkpoint. A fork of
 * a fork reads the original owner's files under the original owner's id (D13) through its hold on
 * the middle database, so the hold is needed while any kept manifest names any other database's
 * SST, not only the holder's own.
 *
 * A database's manifests only ever lose other databases' SSTs (flushes and compactions add its own,
 * drops remove, and only `forkManifest` creates a manifest naming another's), so a manifest newer
 * than the ones in `kept` names none that they do not.
 *
 * @param kept - Every manifest of `latest`'s database a reader may still hold, `latest` included or
 *   not.
 */
export const releasableExternalRefs = (
  latest: Manifest,
  kept: ReadonlyArray<Manifest>,
): ReadonlyArray<ExternalRef> =>
  [latest, ...kept].some((manifest) => externalOwners(manifest).size > 0)
    ? []
    : latest.externalRefs;

/** Owners other than the database itself whose SSTs the manifest still reads. */
export const externalOwners = (manifest: Manifest): ReadonlySet<DatabaseId> => {
  const owners = new Set<DatabaseId>();
  for (const handle of manifestSstHandles(manifest)) {
    if (Option.isSome(handle.owner) && handle.owner.value !== manifest.databaseId)
      owners.add(handle.owner.value);
  }
  return owners;
};

/** How to record an SST being dropped (see {@link dropSstFromManifest}). */
export interface SstDrop {
  readonly sstId: SstId;
  readonly reason: DroppedSst["reason"];
  readonly droppedAt: Timestamp;
}

/**
 * Removes one SST from the manifest and records it in `droppedSsts` (D62): an operator's repair of
 * a corrupted SST. Its versions of the keys it covered are lost; older versions of those keys in
 * lower levels show again. A sorted run left empty is removed.
 *
 * **Details**
 *
 * The SST's object stays until GC finds it unreferenced, like any replaced SST, so readers holding
 * an older manifest are unaffected. The edit does not touch epochs: a running compaction that read
 * a dropped L0 SST fails to commit (its inputs changed); the compactor refuses to drop from a source
 * a running job reads, which covers sorted runs.
 *
 * @returns The manifest without the SST; the same manifest when it was already dropped;
 *   `SstNotInManifestError` when the manifest never held it (or forgot the drop); or
 *   `DatabaseRemovedError` when the database is being removed.
 */
export const dropSstFromManifest = (
  manifest: Manifest,
  drop: SstDrop,
): Result.Result<Manifest, SstNotInManifestError | DatabaseRemovedError> => {
  if (manifest.state._tag === "Removed") return Result.fail(databaseRemoved(manifest));
  const inL0 = manifest.l0.find((sst) => sst.id === drop.sstId);
  const run = manifest.sortedRuns.find((candidate) =>
    candidate.ssts.some((sst) => sst.id === drop.sstId),
  );
  const handle = inL0 ?? run?.ssts.find((sst) => sst.id === drop.sstId);
  if (handle === undefined) {
    return manifest.droppedSsts.some((dropped) => dropped.sstId === drop.sstId)
      ? Result.succeed(manifest)
      : Result.fail(
          new SstNotInManifestError({
            databaseId: manifest.databaseId,
            sstId: drop.sstId,
            message: `SST ${drop.sstId} is not in database ${manifest.databaseId}'s manifest, so there is nothing to drop. Check the SST id in the compaction status.`,
          }),
        );
  }
  const dropped: DroppedSst = {
    sstId: drop.sstId,
    owner: handle.owner,
    from:
      inL0 !== undefined || run === undefined
        ? { _tag: "L0", sstId: drop.sstId }
        : { _tag: "SortedRun", id: run.id },
    firstKey: handle.firstKey,
    lastKey: handle.lastKey,
    view: handle.view,
    rowCount: handle.rowCount,
    reason: drop.reason,
    droppedAt: drop.droppedAt,
  };
  return Result.succeed(
    replaceManifestFields(manifest, {
      l0: manifest.l0.filter((sst) => sst.id !== drop.sstId),
      sortedRuns: manifest.sortedRuns
        .map((candidate) =>
          candidate === run
            ? { id: candidate.id, ssts: candidate.ssts.filter((sst) => sst.id !== drop.sstId) }
            : candidate,
        )
        .filter((candidate) => candidate.ssts.length > 0),
      droppedSsts: [...manifest.droppedSsts, dropped].slice(-MAX_DROPPED_SST_HISTORY),
    }),
  );
};
