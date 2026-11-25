import { Option } from "effect";

import { type ManifestId, type SstId, Timestamp } from "./ids.ts";
import {
  advanceGcSstBoundary,
  type Manifest,
  manifestSstHandles,
  pruneChangelog,
  removeExpiredCheckpoints,
  sstCreatedAt,
} from "./manifest.ts";

/** A stored manifest object: its id and when it was written. */
export interface StoredManifest {
  readonly id: ManifestId;
  readonly uploadedAt: Timestamp;
}

/** A stored SST object (compacted or changelog): its key and id. */
export interface StoredSst {
  readonly key: string;
  readonly sstId: SstId;
}

/** What one GC pass may delete from a database's manifests. */
export interface ManifestCollectionPlan {
  /** Manifests to delete: older than `minAge`, not the latest, and pinned by no checkpoint. */
  readonly deletable: ReadonlyArray<ManifestId>;
  /** Every other stored manifest; each SST they reference stays. */
  readonly retained: ReadonlyArray<ManifestId>;
}

/**
 * Splits a database's stored manifests into those GC may delete and those it keeps (a pure
 * function).
 *
 * **Details**
 *
 * The latest manifest, every manifest a checkpoint in it pins, and every manifest younger than
 * `minAge` are kept, so a reader holding a manifest for less than `minAge` (a cached manifest or a
 * snapshot) finds every SST it references.
 */
export const planManifestCollection = (input: {
  readonly latestId: ManifestId;
  readonly latest: Manifest;
  readonly stored: ReadonlyArray<StoredManifest>;
  readonly now: Timestamp;
  readonly minAge: number;
}): ManifestCollectionPlan => {
  const pinned = new Set(input.latest.checkpoints.map((checkpoint) => checkpoint.manifestId));
  const deletable: Array<ManifestId> = [];
  const retained: Array<ManifestId> = [];
  for (const manifest of input.stored) {
    const old = manifest.uploadedAt + input.minAge <= input.now;
    if (manifest.id < input.latestId && old && !pinned.has(manifest.id))
      deletable.push(manifest.id);
    else retained.push(manifest.id);
  }
  return { deletable, retained };
};

/** Every SST a manifest references that its own database stores (its SSTs and changelog SSTs). */
export const ownedSstIds = (manifest: Manifest): ReadonlySet<SstId> => {
  const owned = new Set<SstId>();
  for (const handle of manifestSstHandles(manifest)) {
    if (Option.isNone(handle.owner) || handle.owner.value === manifest.databaseId)
      owned.add(handle.id);
  }
  for (const entry of manifest.changelog) owned.add(entry.sst.id);
  return owned;
};

/**
 * The stored SST objects GC may delete (a pure function): those no retained manifest references
 * and that were created before `boundary`.
 *
 * **Details**
 *
 * `boundary` must be a value of `gcSstBoundary` that a committed manifest holds: commits after that
 * manifest reject SSTs created before it, so an SST that is uploaded but not yet committed is
 * either newer than `boundary` or will never be committed. Only `databaseId`'s own objects are
 * listed, so SSTs a fork reads from its source are never candidates here.
 */
export const planSstCollection = (input: {
  readonly retained: ReadonlyArray<Manifest>;
  readonly stored: ReadonlyArray<StoredSst>;
  readonly boundary: Timestamp;
}): ReadonlyArray<StoredSst> => {
  const live = new Set<SstId>();
  for (const manifest of input.retained) for (const id of ownedSstIds(manifest)) live.add(id);
  return input.stored.filter(
    (object) => !live.has(object.sstId) && sstCreatedAt(object.sstId) < input.boundary,
  );
};

/**
 * Whether any stored SST could be collected at `cutoff` judging by the latest manifest alone (a
 * superset of what {@link planSstCollection} deletes), so GC advances the boundary — one manifest
 * write — only when there may be something to delete. An idle database costs no writes.
 */
export const hasSstCollectionCandidates = (
  latest: Manifest,
  stored: ReadonlyArray<StoredSst>,
  cutoff: Timestamp,
): boolean => {
  const live = ownedSstIds(latest);
  return stored.some((object) => !live.has(object.sstId) && sstCreatedAt(object.sstId) < cutoff);
};

/**
 * GC's manifest edit: drops expired checkpoints and changelog entries past retention, and advances
 * `gcSstBoundary` to `cutoff` when `advanceBoundary` (the same manifest when nothing changes).
 */
export const prepareGarbageCollection = (
  manifest: Manifest,
  options: {
    readonly now: Timestamp;
    readonly cutoff: Timestamp;
    readonly advanceBoundary: boolean;
  },
): Manifest => {
  const withoutExpired = removeExpiredCheckpoints(manifest, options.now);
  const pruned = Option.match(withoutExpired.options.changelog, {
    onNone: () => withoutExpired,
    onSome: ({ retentionMs }) =>
      pruneChangelog(withoutExpired, Timestamp.make(options.now - retentionMs)),
  });
  return options.advanceBoundary ? advanceGcSstBoundary(pruned, options.cutoff) : pruned;
};
