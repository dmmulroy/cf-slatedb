import { Schema } from "effect";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const PositiveNumber = Schema.Number.check(Schema.isGreaterThan(0));

/**
 * Schema for the engine's tuning settings. Sizes are bytes; durations are milliseconds.
 *
 * **Details**
 *
 * - `blockSize`: target data block size.
 * - `filterBitsPerKey`, `minFilterKeys`: Bloom filter sizing; SSTs with fewer distinct keys get none.
 * - `l0SstSize`: flush once the memtable holds this many bytes (8–16 MiB for hot databases).
 * - `maxL0Age`: flush once the oldest unflushed write is this old; bounds how long data lives only
 *   in Durable Object storage.
 * - `maxUnflushedBytes`: refuse writes (`WriteStalledError`) above this many unflushed bytes.
 * - `l0MaxSsts`: refuse writes while L0 holds this many SSTs (compaction is behind).
 * - `minCompactionSources`, `maxCompactionSources`, `includeSizeThreshold`,
 *   `maxConcurrentCompactions`: the size-tiered scheduler.
 * - `compactionOutputSstSize`: target size of each compaction output SST.
 * - `multipartPartSize`: multipart upload part size (R2 needs at least 5 MiB except the last).
 * - `manifestRefreshInterval`: how often a reader's cached manifest refreshes.
 * - `compactorPollInterval`: how often the compactor's alarm polls the manifest while it has work.
 * - `compactionBudgetCapacity`: most compactions running at once across every database sharing the
 *   compaction budget; groups split it fairly.
 * - `compactionLeaseMs`: how long a compaction's budget lease lasts without renewal (the compactor
 *   renews it on every poll), so a lost compaction frees its slot.
 * - `gcMinAge`: GC deletes an unreferenced object only once it is at least this old.
 * - `gcInterval`: how often the GC cron collects each database.
 */
export const LsmSettings = Schema.Struct({
  blockSize: PositiveInt,
  filterBitsPerKey: PositiveNumber,
  minFilterKeys: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  l0SstSize: PositiveInt,
  maxL0Age: PositiveInt,
  maxUnflushedBytes: PositiveInt,
  l0MaxSsts: PositiveInt,
  minCompactionSources: Schema.Int.check(Schema.isGreaterThanOrEqualTo(2)),
  maxCompactionSources: Schema.Int.check(Schema.isGreaterThanOrEqualTo(2)),
  includeSizeThreshold: PositiveNumber,
  maxConcurrentCompactions: PositiveInt,
  compactionOutputSstSize: PositiveInt,
  multipartPartSize: Schema.Int.check(Schema.isGreaterThanOrEqualTo(5 * 1024 * 1024)),
  manifestRefreshInterval: PositiveInt,
  compactorPollInterval: PositiveInt,
  compactionBudgetCapacity: PositiveInt,
  compactionLeaseMs: PositiveInt,
  gcMinAge: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  gcInterval: PositiveInt,
});

/** The engine's tuning settings. */
export type LsmSettings = typeof LsmSettings.Type;

const MiB = 1024 * 1024;

/** Default settings (design §11: 64 MiB L0, 256 MiB backpressure; SlateDB's compaction defaults). */
export const DEFAULT_LSM_SETTINGS: LsmSettings = {
  blockSize: 4096,
  filterBitsPerKey: 10,
  minFilterKeys: 0,
  l0SstSize: 64 * MiB,
  maxL0Age: 60_000,
  maxUnflushedBytes: 256 * MiB,
  l0MaxSsts: 16,
  minCompactionSources: 4,
  maxCompactionSources: 8,
  includeSizeThreshold: 4,
  maxConcurrentCompactions: 1,
  compactionOutputSstSize: 64 * MiB,
  multipartPartSize: 8 * MiB,
  manifestRefreshInterval: 1_000,
  compactorPollInterval: 5_000,
  compactionBudgetCapacity: 32,
  compactionLeaseMs: 900_000,
  gcMinAge: 3_600_000,
  gcInterval: 300_000,
};
