import { describe, expect, it } from "vite-plus/test";

import { accessTokenSecretName, isCfSlatedbPreviewStage } from "./cf-slatedb-access-tokens.ts";
import { deploysTelemetryProbe } from "./telemetry-probe-worker.ts";

describe("access tokens and stage-only resources (D62, D64)", () => {
  it("names one Secrets Store secret per stage and slot", () => {
    expect(accessTokenSecretName("production", "A")).toBe("CF_SLATEDB_PRODUCTION_ACCESS_TOKEN_A");
    expect(accessTokenSecretName("preview-1", "B")).toBe("CF_SLATEDB_PREVIEW_1_ACCESS_TOKEN_B");
  });

  it("keeps the state-held token and the probe Worker out of production", () => {
    expect(isCfSlatedbPreviewStage("production")).toBe(false);
    expect(isCfSlatedbPreviewStage("preview-2")).toBe(true);
    expect(deploysTelemetryProbe("production")).toBe(false);
    expect(deploysTelemetryProbe("staging")).toBe(false);
    expect(deploysTelemetryProbe("preview-3")).toBe(true);
    expect(deploysTelemetryProbe("dev")).toBe(true);
  });
});
