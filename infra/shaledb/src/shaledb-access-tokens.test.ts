import { describe, expect, it } from "vite-plus/test";

import shaledbViteConfig from "../vite.config.ts";
import { missingAccessTokenSlots } from "./access-token-store.ts";
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

describe("the production token check (D104)", () => {
  it("names the slots whose own secret is missing, never satisfied by another stage's", () => {
    const production = (slot: "A" | "B") => accessTokenSecretName("production", slot);
    expect(missingAccessTokenSlots("production", new Set())).toEqual(["A", "B"]);
    expect(missingAccessTokenSlots("production", new Set([production("A")]))).toEqual(["B"]);
    expect(
      missingAccessTokenSlots("production", new Set([production("A"), production("B")])),
    ).toEqual([]);
    // A preview's secrets, or a longer name, never stand in for production's.
    expect(
      missingAccessTokenSlots(
        "production",
        new Set([
          accessTokenSecretName("preview-1", "A"),
          accessTokenSecretName("preview-1", "B"),
          `${production("A")}_OLD`,
        ]),
      ),
    ).toEqual(["A", "B"]);
  });

  it("runs the token check before the production deploy, and the deploy only if it passes", () => {
    const tasks = shaledbViteConfig.run?.tasks ?? {};
    expect(JSON.stringify(tasks["deploy:production"])).toContain(
      JSON.stringify(
        "node src/access-tokens-cli.ts check production && alchemy deploy --stage production --yes",
      ),
    );
  });
});
