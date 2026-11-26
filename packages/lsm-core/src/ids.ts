import { Schema } from "effect";

/** Largest sequence number, timestamp, or counter shaledb stores: 2^53 - 1. */
export const MAX_SAFE_COUNTER = Number.MAX_SAFE_INTEGER;

const NonNegativeSafeInt = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: MAX_SAFE_COUNTER }),
);

/**
 * Schema for a database's opaque identifier.
 *
 * **Details**
 *
 * The core attaches no tenant meaning. The writer Durable Object name, the R2 prefix `db/<id>/`,
 * and the cache tag `db:<id>` all derive from it, so it is limited to 1–63 lowercase letters,
 * digits, `-`, `_`, and `.`, starting with a letter or digit.
 */
export const DatabaseId = Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9._-]{0,62}$/)).pipe(
  Schema.brand("DatabaseId"),
);

/** A database's opaque identifier. */
export type DatabaseId = typeof DatabaseId.Type;

/**
 * Schema for a write's sequence number. Sequence numbers decide visibility; one write batch shares
 * one. Stored as u64 on disk and limited to 2^53 - 1 in memory.
 */
export const SeqNum = NonNegativeSafeInt.pipe(Schema.brand("SeqNum"));

/** A write's sequence number. */
export type SeqNum = typeof SeqNum.Type;

/** Schema for an SST's identifier: a ULID, so names sort by creation time and never collide. */
export const SstId = Schema.String.check(Schema.isULID()).pipe(Schema.brand("SstId"));

/** An SST's identifier. */
export type SstId = typeof SstId.Type;

/** Schema for a manifest's identifier: its position in the database's manifest chain, from 1. */
export const ManifestId = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: MAX_SAFE_COUNTER }),
).pipe(Schema.brand("ManifestId"));

/** A manifest's position in its database's manifest chain. */
export type ManifestId = typeof ManifestId.Type;

/** Schema for the writer epoch: incremented by each writer that takes over the database. */
export const WriterEpoch = NonNegativeSafeInt.pipe(Schema.brand("WriterEpoch"));

/** The writer epoch that fences stale writers. */
export type WriterEpoch = typeof WriterEpoch.Type;

/** Schema for the compactor epoch: incremented by each compactor that takes over the database. */
export const CompactorEpoch = NonNegativeSafeInt.pipe(Schema.brand("CompactorEpoch"));

/** The compactor epoch that fences stale compactors. */
export type CompactorEpoch = typeof CompactorEpoch.Type;

/** Schema for a sorted run's identifier; higher ids hold newer data. */
export const SortedRunId = NonNegativeSafeInt.pipe(Schema.brand("SortedRunId"));

/** A sorted run's identifier. */
export type SortedRunId = typeof SortedRunId.Type;

/** Schema for a checkpoint's identifier: a ULID. */
export const CheckpointId = Schema.String.check(Schema.isULID()).pipe(Schema.brand("CheckpointId"));

/** A checkpoint's identifier. */
export type CheckpointId = typeof CheckpointId.Type;

/** Schema for a wall-clock time in milliseconds since the Unix epoch (metadata, never ordering). */
export const Timestamp = NonNegativeSafeInt.pipe(Schema.brand("Timestamp"));

/** A wall-clock time in milliseconds since the Unix epoch. */
export type Timestamp = typeof Timestamp.Type;
