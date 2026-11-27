import { Schema } from "effect";

import { MAX_BATCH_BYTES } from "./write-batch.ts";

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
  blockReadHedgeAfterMs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  compactorPollInterval: PositiveInt,
  compactionBudgetCapacity: PositiveInt,
  compactionLeaseMs: PositiveInt,
  gcMinAge: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  gcInterval: PositiveInt,
  gcMaxRetainedManifestLoads: PositiveInt,
  l1DataCacheBytes: PositiveInt,
  l1MetaCacheBytes: PositiveInt,
  l1GroupCacheShare: Schema.Number.check(Schema.isBetween({ minimum: 0.01, maximum: 1 })),
  transactionRetentionMs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxRetainedBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});

/** Every setting, each valid on its own. */
type LsmSettingsFields = typeof LsmSettingsFields.Type;

/**
 * Cloudflare platform limits the settings are checked against (D92). Each is the documented value
 * on the date given; a limit that changes is changed here.
 */
export const LSM_PLATFORM_LIMITS = {
  /** R2: most parts in one multipart upload (developers.cloudflare.com/r2/platform/limits, 2026-10-30). */
  r2MaxUploadParts: 10_000,
  /** R2: largest single upload or multipart part, 5 GiB less 5 MiB (same page). */
  r2MaxPartBytes: 5 * 1024 ** 3 - 5 * 1024 ** 2,
  /** Workers: memory per isolate, 128 MB (developers.cloudflare.com/workers/platform/limits, 2026-11-25). */
  isolateMemoryBytes: 128_000_000,
  /** Durable Objects: SQLite storage per object on Workers Paid, 10 GB (developers.cloudflare.com/durable-objects/platform/limits, 2026-11-13). */
  durableObjectStorageBytes: 10_000_000_000,
  /** Workflows: concurrent instances per account on Workers Paid (developers.cloudflare.com/workflows/reference/limits, 2026-11-10). */
  workflowConcurrentInstances: 50_000,
} as const;

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
 * - Writes refused for L0 backlog must leave L0 enough SSTs to compact, or they stall for good.
 * - A memtable must be able to reach `l0SstSize` before backpressure refuses writes, or writes stall
 *   until `maxL0Age` flushes.
 * - A compaction's budget lease must outlive the compactor's poll interval, which renews it.
 * - Flushed rows kept for transactions must not outlive the manifests those transactions read.
 * - Platform limits ({@link LSM_PLATFORM_LIMITS}, D92): the largest flush and every compaction
 *   output fit in R2's parts; a part fits in one R2 upload; the L1 caches fit in an isolate; a
 *   writer's SQLite (unflushed rows, at most one batch over the limit, the newest flush kept for
 *   transactions, and the retained rows) fits in a Durable Object; the compaction budget fits the
 *   account's concurrent Workflow instances.
 */
const LSM_SETTINGS_COMBINATION_RULES: ReadonlyArray<LsmSettingsCombinationRule> = [
  {
    field: "maxCompactionSources",
    holds: (settings) => settings.minCompactionSources <= settings.maxCompactionSources,
    message: "maxCompactionSources must be at least minCompactionSources",
  },
  {
    field: "l0MaxSsts",
    holds: (settings) => settings.minCompactionSources <= settings.l0MaxSsts,
    message:
      "l0MaxSsts must be at least minCompactionSources: writes stop at l0MaxSsts L0 SSTs, and L0 is compacted only once it holds minCompactionSources, so writes would stall for good",
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
    field: "multipartPartSize",
    holds: (settings) =>
      Math.max(settings.maxUnflushedBytes, settings.compactionOutputSstSize) <=
      LSM_PLATFORM_LIMITS.r2MaxUploadParts * settings.multipartPartSize,
    message: `multipartPartSize is too small: the largest flush (maxUnflushedBytes) and each compaction output (compactionOutputSstSize) must fit in ${LSM_PLATFORM_LIMITS.r2MaxUploadParts} parts, R2's limit for one multipart upload`,
  },
  {
    field: "multipartPartSize",
    holds: (settings) => settings.multipartPartSize <= LSM_PLATFORM_LIMITS.r2MaxPartBytes,
    message: `multipartPartSize must be at most ${LSM_PLATFORM_LIMITS.r2MaxPartBytes} bytes (5 GiB less 5 MiB), R2's largest part`,
  },
  {
    field: "l1DataCacheBytes",
    holds: (settings) =>
      settings.l1DataCacheBytes + settings.l1MetaCacheBytes <
      LSM_PLATFORM_LIMITS.isolateMemoryBytes,
    message: `l1DataCacheBytes and l1MetaCacheBytes together must be below ${LSM_PLATFORM_LIMITS.isolateMemoryBytes} bytes, the memory of one isolate, which holds both caches and every request it serves`,
  },
  {
    field: "maxRetainedBytes",
    holds: (settings) =>
      settings.maxRetainedBytes + 2 * settings.maxUnflushedBytes + MAX_BATCH_BYTES <=
      LSM_PLATFORM_LIMITS.durableObjectStorageBytes,
    message: `maxRetainedBytes plus twice maxUnflushedBytes plus one batch must fit in ${LSM_PLATFORM_LIMITS.durableObjectStorageBytes} bytes, a Durable Object's storage: the writer's SQLite holds the unflushed rows (up to one batch over the limit), the newest flush (up to maxUnflushedBytes after a stall), and the rows retained for transactions`,
  },
  {
    field: "compactionBudgetCapacity",
    holds: (settings) =>
      settings.compactionBudgetCapacity <= LSM_PLATFORM_LIMITS.workflowConcurrentInstances,
    message: `compactionBudgetCapacity must be at most ${LSM_PLATFORM_LIMITS.workflowConcurrentInstances}, the concurrent Workflow instances an account may run`,
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
 * - `blockSize`: target data block size. 16 KiB: each block a scan or compaction reads is one
 *   request, and a point read fetches one block whatever its size, so larger blocks cut a scan's
 *   requests fourfold against 4 KiB at no cost to gets (D67). Readers follow each SST's own index,
 *   so SSTs written with another size stay readable.
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
 * - `blockReadHedgeAfterMs`: once a block or metadata read through the `BlockServer` has taken
 *   this long, the reader also reads the bytes straight from the bucket and takes whichever
 *   answers first. 0: never. 50 ms: Workers Cache answers nine reads in ten faster than that, so
 *   about a fifth of reads are hedged, and the slowest take 100 ms instead of 450 (D72). No effect
 *   in a Stack restricted to a jurisdiction: its `BlockServer` has no Workers Cache, so both reads
 *   take the same road to the same bucket, and readers there never hedge.
 * - `compactorPollInterval`: how often the compactor's alarm polls the manifest while it has work.
 * - `compactionBudgetCapacity`: most compactions running at once across every database sharing the
 *   compaction budget; groups split it fairly.
 * - `compactionLeaseMs`: how long a compaction's budget lease lasts without renewal (the compactor
 *   renews it on every poll), so a lost compaction frees its slot.
 * - `gcMinAge`: GC deletes an unreferenced object only once it is at least this old.
 * - `gcInterval`: how often the GC cron collects each database.
 * - `gcMaxRetainedManifestLoads`: most retained manifests one collection reads. Above it the
 *   collection still deletes old manifests but defers SST deletion (D98). 5,000 is half of an
 *   invocation's 10,000 subrequests, and a database reaches it at about 1.4 commits a second
 *   sustained over `gcMinAge` (one hour).
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
  blockSize: 16_384,
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
  blockReadHedgeAfterMs: 50,
  compactorPollInterval: 5_000,
  compactionBudgetCapacity: 32,
  compactionLeaseMs: 900_000,
  gcMinAge: 3_600_000,
  gcInterval: 300_000,
  gcMaxRetainedManifestLoads: 5_000,
  // About a third of a 128 MB isolate for L1, the rest for decoding, merging, and requests.
  l1DataCacheBytes: 32 * MiB,
  l1MetaCacheBytes: 16 * MiB,
  l1GroupCacheShare: 0.5,
  transactionRetentionMs: 60_000,
  maxRetainedBytes: 64 * MiB,
};
