import { DEFAULT_LSM_SETTINGS } from "@cf-slatedb/lsm-core";
import { lsmWorkerSettingsBindings } from "@cf-slatedb/lsm-cloudflare";
import { Effect, Option } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  type CfSlatedbDeployEnvironment,
  cfSlatedbWorkerSettingsOf,
  resolveCfSlatedbStackConfig,
} from "./cf-slatedb-stack-config.ts";

const resolve = (stage: string, environment: CfSlatedbDeployEnvironment) =>
  resolveCfSlatedbStackConfig(stage, environment).pipe(Effect.runPromise);

const refusedVariable = (stage: string, environment: CfSlatedbDeployEnvironment) =>
  resolveCfSlatedbStackConfig(stage, environment).pipe(
    Effect.flip,
    Effect.map((error) => error.variable),
    Effect.runPromise,
  );

describe("CfSlatedbStackConfig", () => {
  it("resolves complete defaults from an empty environment, by stage", async () => {
    const preview = await resolve("preview-1", {});
    expect(preview).toEqual({
      stage: "preview-1",
      jurisdiction: "none",
      bucketLocation: "auto",
      lsmSettings: DEFAULT_LSM_SETTINGS,
      telemetry: { traceSampleRate: 1, traceRetentionDays: 14 },
      access: { mode: "BearerToken" },
      limits: { apiCpuMs: 30_000, compactorCpuMs: 60_000 },
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
      limits: { apiCpuMs: 30_000, compactorCpuMs: 60_000 },
    });
    // Blank is unset: a shell's `VAR=` must not bind an empty value.
    expect((await resolve("preview-1", { CF_SLATEDB_JURISDICTION: " " })).jurisdiction).toBe(
      "none",
    );
  });

  it("applies each variable, merging setting overrides into the defaults", async () => {
    expect(
      (await resolve("preview-3", { CF_SLATEDB_BUCKET_LOCATION: "weur" })).bucketLocation,
    ).toBe("weur");
    const config = await resolve("preview-2", {
      CF_SLATEDB_JURISDICTION: "eu",
      CF_SLATEDB_LSM_SETTINGS:
        '{"gcMinAge":10000,"compactorPollInterval":1000,"transactionRetentionMs":5000}',
      CF_SLATEDB_TRACE_SAMPLE_RATE: "0.25",
      CF_SLATEDB_TRACE_RETENTION_DAYS: "7",
    });
    expect(config.jurisdiction).toBe("eu");
    expect(config.lsmSettings).toEqual({
      ...DEFAULT_LSM_SETTINGS,
      gcMinAge: 10_000,
      compactorPollInterval: 1_000,
      transactionRetentionMs: 5_000,
    });
    expect(config.telemetry).toEqual({ traceSampleRate: 0.25, traceRetentionDays: 7 });
    expect((await resolve("preview-1", { CF_SLATEDB_API_CPU_MS: "45000" })).limits.apiCpuMs).toBe(
      45_000,
    );
  });

  it("refuses an invalid variable or combination, naming the variable", async () => {
    expect(await refusedVariable("preview-1", { CF_SLATEDB_JURISDICTION: "mars" })).toBe(
      "CF_SLATEDB_JURISDICTION",
    );
    expect(await refusedVariable("preview-1", { CF_SLATEDB_LSM_SETTINGS: "gcMinAge=1" })).toBe(
      "CF_SLATEDB_LSM_SETTINGS",
    );
    // A misspelled setting would otherwise be dropped silently.
    expect(await refusedVariable("preview-1", { CF_SLATEDB_LSM_SETTINGS: '{"gcMinAg":1}' })).toBe(
      "CF_SLATEDB_LSM_SETTINGS",
    );
    expect(
      await refusedVariable("preview-1", { CF_SLATEDB_LSM_SETTINGS: '{"minCompactionSources":9}' }),
    ).toBe("CF_SLATEDB_LSM_SETTINGS");
    expect(await refusedVariable("preview-1", { CF_SLATEDB_BUCKET_LOCATION: "mars" })).toBe(
      "CF_SLATEDB_BUCKET_LOCATION",
    );
    // R2 places a jurisdiction's bucket itself.
    expect(
      await refusedVariable("preview-2", {
        CF_SLATEDB_JURISDICTION: "eu",
        CF_SLATEDB_BUCKET_LOCATION: "weur",
      }),
    ).toBe("CF_SLATEDB_BUCKET_LOCATION");
    expect(await refusedVariable("preview-1", { CF_SLATEDB_COMPACTOR_CPU_MS: "900000" })).toBe(
      "CF_SLATEDB_COMPACTOR_CPU_MS",
    );
    expect(await refusedVariable("preview-1", { CF_SLATEDB_TRACE_SAMPLE_RATE: "2" })).toBe(
      "CF_SLATEDB_TRACE_SAMPLE_RATE",
    );
    expect(await refusedVariable("preview-1", { CF_SLATEDB_TRACE_RETENTION_DAYS: "0" })).toBe(
      "CF_SLATEDB_TRACE_RETENTION_DAYS",
    );
  });

  it("binds every Worker the complete settings and an explicit jurisdiction, never an absence", async () => {
    const unrestricted = lsmWorkerSettingsBindings(
      cfSlatedbWorkerSettingsOf(await resolve("preview-1", {})),
    );
    expect(unrestricted["CF_SLATEDB_JURISDICTION"]).toBe("none");
    expect(unrestricted["CF_SLATEDB_BUCKET_LOCATION"]).toBe("auto");
    expect(JSON.parse(unrestricted["CF_SLATEDB_LSM_SETTINGS"] ?? "{}")).toEqual(
      DEFAULT_LSM_SETTINGS,
    );
    const eu = cfSlatedbWorkerSettingsOf(
      await resolve("preview-3", { CF_SLATEDB_JURISDICTION: "eu" }),
    );
    expect(eu.jurisdiction).toEqual(Option.some("eu"));
    expect(lsmWorkerSettingsBindings(eu)["CF_SLATEDB_JURISDICTION"]).toBe("eu");
  });
});
