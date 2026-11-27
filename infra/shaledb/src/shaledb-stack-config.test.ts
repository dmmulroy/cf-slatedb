import { DEFAULT_LSM_SETTINGS } from "@shaledb/lsm-core";
import { lsmWorkerSettingsBindings } from "@shaledb/lsm-cloudflare";
import { Effect, Option } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  type ShaledbDeployEnvironment,
  shaledbWorkerSettingsOf,
  resolveShaledbStackConfig,
} from "./shaledb-stack-config.ts";

const resolve = (stage: string, environment: ShaledbDeployEnvironment) =>
  resolveShaledbStackConfig(stage, environment).pipe(Effect.runPromise);

const refusedVariable = (stage: string, environment: ShaledbDeployEnvironment) =>
  resolveShaledbStackConfig(stage, environment).pipe(
    Effect.flip,
    Effect.map((error) => error.variable),
    Effect.runPromise,
  );

describe("ShaledbStackConfig", () => {
  it("resolves complete defaults from an empty environment, by stage", async () => {
    const preview = await resolve("preview-1", {});
    expect(preview).toEqual({
      stage: "preview-1",
      jurisdiction: "none",
      bucketLocation: "auto",
      lsmSettings: DEFAULT_LSM_SETTINGS,
      telemetry: { traceSampleRate: 1, traceRetentionDays: 14 },
      access: { mode: "BearerToken" },
      limits: { apiCpuMs: 30_000, compactorCpuMs: 60_000, compactorSubrequests: 10_000_000 },
    });
    expect((await resolve("dev", {})).access.mode).toBe("Open");
    // Production's decided defaults (P2–P5): no jurisdiction, R2 places the bucket, the default
    // engine settings, 10% of traces kept for 14 days, bearer tokens, measured CPU limits.
    expect(await resolve("production", {})).toEqual({
      stage: "production",
      jurisdiction: "none",
      bucketLocation: "auto",
      lsmSettings: DEFAULT_LSM_SETTINGS,
      telemetry: { traceSampleRate: 0.1, traceRetentionDays: 14 },
      access: { mode: "BearerToken" },
      limits: { apiCpuMs: 30_000, compactorCpuMs: 60_000, compactorSubrequests: 10_000_000 },
    });
    // Blank is unset: a shell's `VAR=` must not bind an empty value.
    expect((await resolve("preview-1", { SHALEDB_JURISDICTION: " " })).jurisdiction).toBe("none");
  });

  it("applies each variable, merging setting overrides into the defaults", async () => {
    expect((await resolve("preview-3", { SHALEDB_BUCKET_LOCATION: "weur" })).bucketLocation).toBe(
      "weur",
    );
    const config = await resolve("preview-2", {
      SHALEDB_JURISDICTION: "eu",
      SHALEDB_LSM_SETTINGS:
        '{"gcMinAge":10000,"compactorPollInterval":1000,"transactionRetentionMs":5000}',
      SHALEDB_TRACE_SAMPLE_RATE: "0.25",
      SHALEDB_TRACE_RETENTION_DAYS: "7",
    });
    expect(config.jurisdiction).toBe("eu");
    expect(config.lsmSettings).toEqual({
      ...DEFAULT_LSM_SETTINGS,
      gcMinAge: 10_000,
      compactorPollInterval: 1_000,
      transactionRetentionMs: 5_000,
    });
    expect(config.telemetry).toEqual({ traceSampleRate: 0.25, traceRetentionDays: 7 });
    expect((await resolve("preview-1", { SHALEDB_API_CPU_MS: "45000" })).limits.apiCpuMs).toBe(
      45_000,
    );
    expect(
      (await resolve("preview-1", { SHALEDB_COMPACTOR_SUBREQUESTS: "50000" })).limits
        .compactorSubrequests,
    ).toBe(50_000);
  });

  it("refuses an invalid variable or combination, naming the variable", async () => {
    expect(await refusedVariable("preview-1", { SHALEDB_JURISDICTION: "mars" })).toBe(
      "SHALEDB_JURISDICTION",
    );
    expect(await refusedVariable("preview-1", { SHALEDB_LSM_SETTINGS: "gcMinAge=1" })).toBe(
      "SHALEDB_LSM_SETTINGS",
    );
    // A misspelled setting would otherwise be dropped silently.
    expect(await refusedVariable("preview-1", { SHALEDB_LSM_SETTINGS: '{"gcMinAg":1}' })).toBe(
      "SHALEDB_LSM_SETTINGS",
    );
    expect(
      await refusedVariable("preview-1", { SHALEDB_LSM_SETTINGS: '{"minCompactionSources":9}' }),
    ).toBe("SHALEDB_LSM_SETTINGS");
    expect(await refusedVariable("preview-1", { SHALEDB_BUCKET_LOCATION: "mars" })).toBe(
      "SHALEDB_BUCKET_LOCATION",
    );
    // R2 places a jurisdiction's bucket itself.
    expect(
      await refusedVariable("preview-2", {
        SHALEDB_JURISDICTION: "eu",
        SHALEDB_BUCKET_LOCATION: "weur",
      }),
    ).toBe("SHALEDB_BUCKET_LOCATION");
    expect(await refusedVariable("preview-1", { SHALEDB_COMPACTOR_CPU_MS: "900000" })).toBe(
      "SHALEDB_COMPACTOR_CPU_MS",
    );
    // Workers Paid allows at most 10 million subrequests per invocation (D99).
    expect(await refusedVariable("preview-1", { SHALEDB_COMPACTOR_SUBREQUESTS: "10000001" })).toBe(
      "SHALEDB_COMPACTOR_SUBREQUESTS",
    );
    expect(await refusedVariable("preview-1", { SHALEDB_TRACE_SAMPLE_RATE: "2" })).toBe(
      "SHALEDB_TRACE_SAMPLE_RATE",
    );
    expect(await refusedVariable("preview-1", { SHALEDB_TRACE_RETENTION_DAYS: "0" })).toBe(
      "SHALEDB_TRACE_RETENTION_DAYS",
    );
  });

  it("binds every Worker the complete settings and an explicit jurisdiction, never an absence", async () => {
    const unrestricted = lsmWorkerSettingsBindings(
      shaledbWorkerSettingsOf(await resolve("preview-1", {})),
    );
    expect(unrestricted["SHALEDB_JURISDICTION"]).toBe("none");
    expect(unrestricted["SHALEDB_BUCKET_LOCATION"]).toBe("auto");
    expect(JSON.parse(unrestricted["SHALEDB_LSM_SETTINGS"] ?? "{}")).toEqual(DEFAULT_LSM_SETTINGS);
    const eu = shaledbWorkerSettingsOf(await resolve("preview-3", { SHALEDB_JURISDICTION: "eu" }));
    expect(eu.jurisdiction).toEqual(Option.some("eu"));
    expect(lsmWorkerSettingsBindings(eu)["SHALEDB_JURISDICTION"]).toBe("eu");
  });

  it("binds each setting within a Worker variable's 5 KB, with the largest values settings allow (D92)", async () => {
    // Workers: a variable or secret is at most 5 KB (developers.cloudflare.com/workers/platform/limits,
    // 2026-11-25). The engine settings are one JSON variable; its numbers grow with their values.
    const largest = Object.fromEntries(
      Object.entries(DEFAULT_LSM_SETTINGS).map(([name, value]) => [
        name,
        Number.isInteger(value) ? Number.MAX_SAFE_INTEGER : 0.123456789012345,
      ]),
    );
    // The settings' JSON is plain numbers, so its longest form is every field at its longest number.
    expect(new TextEncoder().encode(JSON.stringify(largest)).length).toBeLessThanOrEqual(5_000);
    const bindings = lsmWorkerSettingsBindings(
      shaledbWorkerSettingsOf(await resolve("production", {})),
    );
    for (const [name, value] of Object.entries(bindings)) {
      expect([name, new TextEncoder().encode(value).length <= 5_000]).toEqual([name, true]);
    }
  });
});
