import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { DEFAULT_LSM_SETTINGS, LSM_PLATFORM_LIMITS, LsmSettings } from "./lsm-settings.ts";
import { MAX_BATCH_BYTES } from "./write-batch.ts";

const isValid = Schema.is(LsmSettings);

describe("LsmSettings combinations", () => {
  it("accepts the defaults and refuses each contradictory combination", () => {
    expect(isValid(DEFAULT_LSM_SETTINGS)).toBe(true);
    expect(isValid({ ...DEFAULT_LSM_SETTINGS, minCompactionSources: 9 })).toBe(false);
    expect(
      isValid({ ...DEFAULT_LSM_SETTINGS, l0SstSize: DEFAULT_LSM_SETTINGS.maxUnflushedBytes + 1 }),
    ).toBe(false);
    expect(
      isValid({
        ...DEFAULT_LSM_SETTINGS,
        compactorPollInterval: DEFAULT_LSM_SETTINGS.compactionLeaseMs,
      }),
    ).toBe(false);
  });

  it("refuses settings Cloudflare's limits cannot hold, at each limit's edge (D92)", () => {
    const MiB = 1024 * 1024;
    const part = 5 * MiB;
    const withPart = { ...DEFAULT_LSM_SETTINGS, multipartPartSize: part, l0SstSize: part };
    // R2: one upload has at most 10,000 parts, for the largest flush and each compaction output.
    const parts = LSM_PLATFORM_LIMITS.r2MaxUploadParts;
    // (The Durable Object's storage bounds maxUnflushedBytes far below that; outputs are not.)
    expect(isValid({ ...withPart, compactionOutputSstSize: parts * part })).toBe(true);
    expect(isValid({ ...withPart, compactionOutputSstSize: parts * part + 1 })).toBe(false);
    // R2: a part is at most 5 GiB less 5 MiB.
    const largest = LSM_PLATFORM_LIMITS.r2MaxPartBytes;
    expect(isValid({ ...DEFAULT_LSM_SETTINGS, multipartPartSize: largest })).toBe(true);
    expect(isValid({ ...DEFAULT_LSM_SETTINGS, multipartPartSize: largest + 1 })).toBe(false);
    // Workers: both L1 caches live in one 128 MB isolate.
    const isolate = LSM_PLATFORM_LIMITS.isolateMemoryBytes;
    expect(
      isValid({
        ...DEFAULT_LSM_SETTINGS,
        l1DataCacheBytes: isolate - 1 - DEFAULT_LSM_SETTINGS.l1MetaCacheBytes,
      }),
    ).toBe(true);
    expect(
      isValid({
        ...DEFAULT_LSM_SETTINGS,
        l1DataCacheBytes: isolate - DEFAULT_LSM_SETTINGS.l1MetaCacheBytes,
      }),
    ).toBe(false);
    // Durable Objects: the writer's SQLite holds unflushed rows, the newest flush, and retained rows.
    const room =
      LSM_PLATFORM_LIMITS.durableObjectStorageBytes -
      2 * DEFAULT_LSM_SETTINGS.maxUnflushedBytes -
      MAX_BATCH_BYTES;
    expect(isValid({ ...DEFAULT_LSM_SETTINGS, maxRetainedBytes: room })).toBe(true);
    expect(isValid({ ...DEFAULT_LSM_SETTINGS, maxRetainedBytes: room + 1 })).toBe(false);
    // Workflows: the compaction budget fits the account's concurrent instances.
    const instances = LSM_PLATFORM_LIMITS.workflowConcurrentInstances;
    expect(isValid({ ...DEFAULT_LSM_SETTINGS, compactionBudgetCapacity: instances })).toBe(true);
    expect(isValid({ ...DEFAULT_LSM_SETTINGS, compactionBudgetCapacity: instances + 1 })).toBe(
      false,
    );
  });
});

describe("LsmSettings at each rule's edge (mutation sweep, D117)", () => {
  const valid = (overrides: Partial<LsmSettings>) =>
    Schema.is(LsmSettings)({ ...DEFAULT_LSM_SETTINGS, ...overrides });

  it("accepts each combination rule exactly at equality, and refuses one step past it", () => {
    expect(valid({ minCompactionSources: 8, maxCompactionSources: 8 })).toBe(true);
    expect(valid({ minCompactionSources: 9, maxCompactionSources: 8 })).toBe(false);
    expect(valid({ minCompactionSources: 16, maxCompactionSources: 16, l0MaxSsts: 16 })).toBe(true);
    expect(valid({ minCompactionSources: 16, maxCompactionSources: 16, l0MaxSsts: 15 })).toBe(
      false,
    );
    expect(valid({ l0SstSize: 256 * 1024 * 1024 })).toBe(true);
    expect(valid({ l0SstSize: 256 * 1024 * 1024 + 1 })).toBe(false);
    // Transaction retention: off (0) always, otherwise strictly shorter than gcMinAge.
    expect(valid({ transactionRetentionMs: 0 })).toBe(true);
    expect(valid({ transactionRetentionMs: DEFAULT_LSM_SETTINGS.gcMinAge - 1 })).toBe(true);
    expect(valid({ transactionRetentionMs: DEFAULT_LSM_SETTINGS.gcMinAge })).toBe(false);
    // A compaction merges at least two sources.
    expect(valid({ minCompactionSources: 2, maxCompactionSources: 2 })).toBe(true);
    expect(valid({ minCompactionSources: 1 })).toBe(false);
    expect(valid({ minCompactionSources: 2, maxCompactionSources: 1 })).toBe(false);
  });

  it("pins the defaults and the platform limits the docs name", () => {
    const MiB = 1024 * 1024;
    expect(DEFAULT_LSM_SETTINGS).toEqual({
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
      l1DataCacheBytes: 32 * MiB,
      l1MetaCacheBytes: 16 * MiB,
      l1GroupCacheShare: 0.5,
      transactionRetentionMs: 60_000,
      maxRetainedBytes: 64 * MiB,
    });
    expect(LSM_PLATFORM_LIMITS).toEqual({
      r2MaxUploadParts: 10_000,
      r2MaxPartBytes: 5_363_466_240,
      isolateMemoryBytes: 128_000_000,
      durableObjectStorageBytes: 10_000_000_000,
      workflowConcurrentInstances: 50_000,
    });
  });
});
