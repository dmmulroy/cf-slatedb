import { Schema } from "effect";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const PositiveNumber = Schema.Number.check(Schema.isGreaterThan(0));

const LsmSettingsFields = Schema.Struct({
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
  l1DataCacheBytes: PositiveInt,
  l1MetaCacheBytes: PositiveInt,
  l1GroupCacheShare: Schema.Number.check(Schema.isBetween({ minimum: 0.01, maximum: 1 })),
  transactionRetentionMs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxRetainedBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});

/** Every setting, each valid on its own. */
type LsmSettingsFields = typeof LsmSettingsFields.Type;

/** One rule between settings: the fields it relates, and what must hold between them. */
interface LsmSettingsCombinationRule {
  /** The field the issue is reported at. */
  readonly field: string;
  readonly holds: (settings: LsmSettingsFields) => boolean;
  /** Why the combination is refused, and how to fix it. */
  readonly message: string;
}

/**
 * Rules between settings that every valid {@link LsmSettings} satisfies.
 *
 * **Details**
 *
 * - A compaction needs at least as many sources allowed as it requires.
 * - A memtable must be able to reach `l0SstSize` before backpressure refuses writes, or writes stall
 *   until `maxL0Age` flushes.
 * - A compaction's budget lease must outlive the compactor's poll interval, which renews it.
 * - Flushed rows kept for transactions must not outlive the manifests those transactions read.
 */
const LSM_SETTINGS_COMBINATION_RULES: ReadonlyArray<LsmSettingsCombinationRule> = [
  {
    field: "maxCompactionSources",
    holds: (settings) => settings.minCompactionSources <= settings.maxCompactionSources,
    message: "maxCompactionSources must be at least minCompactionSources",
  },
  {
    field: "maxUnflushedBytes",
    holds: (settings) => settings.l0SstSize <= settings.maxUnflushedBytes,
    message:
      "maxUnflushedBytes must be at least l0SstSize, or writes are refused before a flush starts",
  },
  {
    field: "transactionRetentionMs",
    holds: (settings) =>
      settings.transactionRetentionMs === 0 || settings.transactionRetentionMs < settings.gcMinAge,
    message:
      "transactionRetentionMs must be shorter than gcMinAge: a transaction reads the manifest it began at, which GC keeps only for gcMinAge",
  },
  {
    field: "compactionLeaseMs",
    holds: (settings) => settings.compactorPollInterval < settings.compactionLeaseMs,
    message:
      "compactionLeaseMs must be longer than compactorPollInterval, which renews the lease on every poll",
  },
];

const lsmSettingsCombinationIssues = (settings: LsmSettingsFields) =>
  LSM_SETTINGS_COMBINATION_RULES.flatMap((rule) =>
    rule.holds(settings) ? [] : [{ path: [rule.field], issue: rule.message }],
  );

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
 * - `l1DataCacheBytes`, `l1MetaCacheBytes`: bytes of data blocks and of SST metadata (index, filter,
 *   stats) a reader isolate keeps in its L1 cache, counted as stored on R2.
 * - `l1GroupCacheShare`: the fraction of each L1 cache one `budgetGroup` may hold; under pressure the
 *   group holding the most gives up its least recently used entries first.
 * - `transactionRetentionMs`, `maxRetainedBytes`: how long, and up to how many bytes, a writer keeps
 *   flushed rows so transactions that span flushes can still be checked exactly; the newest flush's
 *   rows always stay (D58). Must be shorter than `gcMinAge` (or 0).
 *
 * Settings that only make sense together are checked together (`LSM_SETTINGS_COMBINATION_RULES`), so
 * a deployment with a contradictory combination fails to decode instead of misbehaving later.
 */
export const LsmSettings = LsmSettingsFields.check(
  Schema.makeFilter(lsmSettingsCombinationIssues, { identifier: "LsmSettingsCombination" }),
);

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
  // About a third of a 128 MB isolate for L1, the rest for decoding, merging, and requests.
  l1DataCacheBytes: 32 * MiB,
  l1MetaCacheBytes: 16 * MiB,
  l1GroupCacheShare: 0.5,
  transactionRetentionMs: 60_000,
  maxRetainedBytes: 64 * MiB,
};
