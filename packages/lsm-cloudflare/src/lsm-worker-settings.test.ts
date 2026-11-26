import { DEFAULT_LSM_SETTINGS } from "@shaledb/lsm-core";
import { ConfigProvider, Effect, Option } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { makeLsmConfiguration } from "./lsm-configuration.ts";
import { lsmBucketLocation } from "./lsm-bucket.ts";
import { lsmJurisdiction } from "./lsm-jurisdiction.ts";
import { lsmTraceSampleRate } from "./lsm-trace-policy.ts";
import { lsmWorkerSettingsBindings } from "./lsm-worker-settings.ts";

/** What a Worker reads back from `bindings` through `Config` at runtime. */
const readBack = (bindings: Readonly<Record<string, string>>) =>
  Effect.all({
    settings: makeLsmConfiguration,
    jurisdiction: lsmJurisdiction,
    bucketLocation: lsmBucketLocation,
    traceSampleRate: lsmTraceSampleRate,
  }).pipe(
    Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown(bindings)),
    Effect.runPromise,
  );

describe("LsmWorkerSettings bindings", () => {
  it("round-trips the complete settings and the jurisdiction through a Worker's Config reads", async () => {
    const lsmSettings = {
      ...DEFAULT_LSM_SETTINGS,
      gcMinAge: 10_000,
      transactionRetentionMs: 5_000,
      l0SstSize: 8 * 1024 * 1024,
    };
    const read = await readBack(
      lsmWorkerSettingsBindings({
        lsmSettings,
        jurisdiction: Option.some("eu"),
        bucketLocation: Option.none(),
        traceSampleRate: 0.1,
      }),
    );
    expect(read).toEqual({
      settings: lsmSettings,
      jurisdiction: Option.some("eu"),
      bucketLocation: Option.none(),
      traceSampleRate: 0.1,
    });
    const unrestricted = await readBack(
      lsmWorkerSettingsBindings({
        lsmSettings,
        jurisdiction: Option.none(),
        bucketLocation: Option.some("weur"),
        traceSampleRate: 1,
      }),
    );
    expect(unrestricted.jurisdiction).toEqual(Option.none());
    expect(unrestricted.bucketLocation).toEqual(Option.some("weur"));
  });

  it("still reads the partial overrides that deploys before D55 bound", async () => {
    const read = await readBack({ SHALEDB_LSM_SETTINGS: '{"compactorPollInterval":2000}' });
    expect(read.settings).toEqual({ ...DEFAULT_LSM_SETTINGS, compactorPollInterval: 2000 });
    // Deploys before D63 bound no sample rate: every trace is kept.
    expect(read.traceSampleRate).toBe(1);
    expect((await readBack({ SHALEDB_LSM_SETTINGS: "{}" })).settings).toEqual(DEFAULT_LSM_SETTINGS);
  });
});
