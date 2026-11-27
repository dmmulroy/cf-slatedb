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
