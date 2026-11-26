import { describe, expect, it } from "vite-plus/test";

import { accessTokenSecretName, isShaledbPreviewStage } from "./shaledb-access-tokens.ts";
import { deploysTelemetryProbe } from "./telemetry-probe-worker.ts";

describe("access tokens and stage-only resources (D62, D64)", () => {
  it("names one Secrets Store secret per stage and slot", () => {
    expect(accessTokenSecretName("production", "A")).toBe("SHALEDB_PRODUCTION_ACCESS_TOKEN_A");
    expect(accessTokenSecretName("preview-1", "B")).toBe("SHALEDB_PREVIEW_1_ACCESS_TOKEN_B");
  });

  it("keeps the state-held token and the probe Worker out of production", () => {
    expect(isShaledbPreviewStage("production")).toBe(false);
    expect(isShaledbPreviewStage("preview-2")).toBe(true);
    expect(deploysTelemetryProbe("production")).toBe(false);
    expect(deploysTelemetryProbe("staging")).toBe(false);
    expect(deploysTelemetryProbe("preview-3")).toBe(true);
    expect(deploysTelemetryProbe("dev")).toBe(true);
  });
});
