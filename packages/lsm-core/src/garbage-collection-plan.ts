import { Option } from "effect";

import { type ManifestId, type SstId, Timestamp } from "./ids.ts";
import {
  advanceGcSstBoundary,
  externalOwners,
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
  /**
   * Manifests to delete: superseded more than `minAge` ago, not the latest, and pinned by no
   * checkpoint.
   */
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
 * The latest manifest, every manifest a checkpoint in it pins, and every manifest superseded less
 * than `minAge` ago are kept, so a reader holding a manifest for less than `minAge` after it stopped
 * being the newest (a cached manifest or a snapshot) finds every SST it references. A manifest is
 * superseded when the next one is written, so its age is the next stored manifest's, not its own: a
 * manifest written long ago may have been the newest until a moment ago.
 */
export const planManifestCollection = (input: {
  readonly latestId: ManifestId;
  readonly latest: Manifest;
  readonly stored: ReadonlyArray<StoredManifest>;
  readonly now: Timestamp;
  readonly minAge: number;
}): ManifestCollectionPlan => {
  const pinned = new Set(input.latest.checkpoints.map((checkpoint) => checkpoint.manifestId));
  const ordered = [...input.stored].sort((left, right) => left.id - right.id);
  const deletable: Array<ManifestId> = [];
  const retained: Array<ManifestId> = [];
  for (const [index, manifest] of ordered.entries()) {
    // Superseded when the next stored manifest was written (later still if some were deleted).
    const successor = ordered[index + 1];
    const supersededLongAgo =
      successor !== undefined && successor.uploadedAt + input.minAge <= input.now;
    if (manifest.id < input.latestId && supersededLongAgo && !pinned.has(manifest.id))
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
  /** Every SST a retained manifest references ({@link RetainedReferences.live}). */
  readonly live: ReadonlySet<SstId>;
  readonly stored: ReadonlyArray<StoredSst>;
  readonly boundary: Timestamp;
}): ReadonlyArray<StoredSst> =>
  input.stored.filter(
    (object) => !input.live.has(object.sstId) && sstCreatedAt(object.sstId) < input.boundary,
  );

/**
 * What a collection's retained manifests reference, folded in as each is read, so the manifests
 * themselves are not held (D98).
 */
export interface RetainedReferences {
  /** The SSTs of the database's own that some retained manifest references. */
  readonly live: Set<SstId>;
  /** Whether some retained manifest reads another database's SST (a fork's, D77). */
  readsAnotherDatabase: boolean;
}

/** References of no manifest yet. */
export const emptyRetainedReferences = (): RetainedReferences => ({
  live: new Set(),
  readsAnotherDatabase: false,
});

/** Folds one retained manifest's references into `references`. */
export const addRetainedManifest = (references: RetainedReferences, manifest: Manifest): void => {
  for (const id of ownedSstIds(manifest)) references.live.add(id);
  if (externalOwners(manifest).size > 0) references.readsAnotherDatabase = true;
};

/** The references of `manifests`, as {@link addRetainedManifest} folds them. */
export const retainedReferencesOf = (manifests: ReadonlyArray<Manifest>): RetainedReferences => {
  const references = emptyRetainedReferences();
  for (const manifest of manifests) addRetainedManifest(references, manifest);
  return references;
};

/**
 * When collecting again can delete something this collection had to keep: an unreferenced SST or a
 * superseded, unpinned manifest that was too young (old enough `minAge` from now), or a checkpoint
 * that expires. None when nothing kept will become collectable, so an idle database schedules no
 * further collection (D60).
 */
export const nextCollectionTime = (input: {
  readonly latestId: ManifestId;
  readonly latest: Manifest;
  /** Manifests the collection kept, by id. */
  readonly retainedIds: ReadonlyArray<ManifestId>;
  /** Every SST the manifests the collection kept reference ({@link RetainedReferences.live}). */
  readonly live: ReadonlySet<SstId>;
  readonly stored: ReadonlyArray<StoredSst>;
  /** SSTs created before it could be deleted this time. */
  readonly boundary: Timestamp;
  readonly now: Timestamp;
  readonly minAge: number;
}): Option.Option<Timestamp> => {
  const pinned = new Set(input.latest.checkpoints.map((checkpoint) => checkpoint.manifestId));
  const youngSst = input.stored.some(
    (object) => !input.live.has(object.sstId) && sstCreatedAt(object.sstId) >= input.boundary,
  );
  const youngManifest = input.retainedIds.some((id) => id !== input.latestId && !pinned.has(id));
  const times: Array<number> = [];
  if (youngSst || youngManifest) times.push(input.now + input.minAge);
  for (const checkpoint of input.latest.checkpoints) {
    if (Option.isSome(checkpoint.expireAt))
      times.push(Math.max(input.now, checkpoint.expireAt.value));
  }
  return times.length === 0 ? Option.none() : Option.some(Timestamp.make(Math.min(...times)));
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
  // A removed database's head is its tombstone (D91): collection changes nothing in it.
  if (manifest.state._tag === "Removed") return manifest;
  const withoutExpired = removeExpiredCheckpoints(manifest, options.now);
  const pruned = Option.match(withoutExpired.options.changelog, {
    onNone: () => withoutExpired,
    onSome: ({ retentionMs }) =>
      pruneChangelog(withoutExpired, Timestamp.make(options.now - retentionMs)),
  });
  return options.advanceBoundary ? advanceGcSstBoundary(pruned, options.cutoff) : pruned;
};
