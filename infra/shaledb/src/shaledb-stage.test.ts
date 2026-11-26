import { describe, expect, it } from "vite-plus/test";

import {
  SHALEDB_PREVIEW_STAGES,
  SHALEDB_PRODUCTION_STAGE,
  shaledbStateBackendForStage,
} from "./shaledb-stage.ts";

describe("shaledbStateBackendForStage", () => {
  it("keeps local stages' state on this machine and live stages' state in Cloudflare", () => {
    const local = ["dev", "test", "local", "dev-dillon", "test-writer"];
    const live = [...SHALEDB_PREVIEW_STAGES, SHALEDB_PRODUCTION_STAGE];
    expect(local.map(shaledbStateBackendForStage)).toEqual(local.map(() => "Local"));
    expect(live.map(shaledbStateBackendForStage)).toEqual(live.map(() => "Cloudflare"));
  });
});
