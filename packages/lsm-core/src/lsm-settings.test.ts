import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { DEFAULT_LSM_SETTINGS, LsmSettings } from "./lsm-settings.ts";

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
});
