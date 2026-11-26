import type { DatabaseId, ManifestId, SeqNum, SstId } from "@cf-slatedb/lsm-core";

/**
 * Object key layout in the shared bucket: everything a database owns is under `db/<id>/`.
 *
 * - `db/<id>/manifest/<20-digit manifest id>`: one manifest per id, written once.
 * - `db/<id>/manifest/LATEST`: hint holding the newest known manifest id; may lag.
 * - `db/<id>/compacted/<ulid>.sst`: L0 and sorted-run SSTs.
 * - `db/<id>/changelog/<20-digit last seq>-<ulid>.sst`: changelog SSTs (change data capture).
 * - `db/<id>/gc/`: GC boundary files.
 */
export const databasePrefix = (databaseId: DatabaseId): string => `db/${databaseId}/`;

/** Prefix of a database's manifests. */
export const manifestPrefix = (databaseId: DatabaseId): string =>
  `${databasePrefix(databaseId)}manifest/`;

/** Key of one manifest; ids are zero-padded so keys sort by id. */
export const manifestKey = (databaseId: DatabaseId, manifestId: ManifestId): string =>
  `${manifestPrefix(databaseId)}${String(manifestId).padStart(20, "0")}`;

/** Key of the `manifest/LATEST` hint. */
export const latestManifestKey = (databaseId: DatabaseId): string =>
  `${manifestPrefix(databaseId)}LATEST`;

/** Prefix of a database's L0 and sorted-run SSTs. */
export const compactedPrefix = (databaseId: DatabaseId): string =>
  `${databasePrefix(databaseId)}compacted/`;

/** Key of an SST under its owning database. */
export const sstKey = (owner: DatabaseId, sstId: SstId): string =>
  `${compactedPrefix(owner)}${sstId}.sst`;

/** Prefix of a database's changelog SSTs. */
export const changelogPrefix = (databaseId: DatabaseId): string =>
  `${databasePrefix(databaseId)}changelog/`;

/** Key of a changelog SST, sortable by its last sequence number. */
export const changelogKey = (databaseId: DatabaseId, lastSeq: SeqNum, sstId: SstId): string =>
  `${changelogPrefix(databaseId)}${String(lastSeq).padStart(20, "0")}-${sstId}.sst`;

/** Prefix of a database's GC boundary files. */
export const gcPrefix = (databaseId: DatabaseId): string => `${databasePrefix(databaseId)}gc/`;

/**
 * Key of a database's manifest GC boundary (SlateDB RFC 0026): an ASCII manifest id at or below
 * which ids may have been deleted, so a manifest created there does not count as committed.
 */
export const manifestGcBoundaryKey = (databaseId: DatabaseId): string =>
  `${gcPrefix(databaseId)}manifest.boundary`;

/** What an object key holds, by the key layout above: a safe span attribute (`object.kind`). */
export type ObjectKind =
  | "Manifest"
  | "LatestHint"
  | "ManifestBoundary"
  | "Sst"
  | "Changelog"
  | "Gc"
  | "Other";

/** The {@link ObjectKind} of `key`. */
export const objectKindOfKey = (key: string): ObjectKind => {
  const match =
    /^db\/[^/]+\/(manifest\/(LATEST|\d+)|compacted\/|changelog\/|gc\/(manifest\.boundary)?)/.exec(
      key,
    );
  if (match === null) return "Other";
  if (match[2] === "LATEST") return "LatestHint";
  if (match[2] !== undefined) return "Manifest";
  if (match[3] !== undefined) return "ManifestBoundary";
  if (match[1]?.startsWith("compacted/")) return "Sst";
  if (match[1]?.startsWith("changelog/")) return "Changelog";
  return "Gc";
};
