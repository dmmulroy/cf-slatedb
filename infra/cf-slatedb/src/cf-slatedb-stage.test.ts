import { describe, expect, it } from "vite-plus/test";

import {
  CF_SLATEDB_PREVIEW_STAGES,
  CF_SLATEDB_PRODUCTION_STAGE,
  cfSlatedbStateBackendForStage,
} from "./cf-slatedb-stage.ts";

describe("cfSlatedbStateBackendForStage", () => {
  it("keeps local stages' state on this machine and live stages' state in Cloudflare", () => {
    const local = ["dev", "test", "local", "dev-dillon", "test-writer"];
    const live = [...CF_SLATEDB_PREVIEW_STAGES, CF_SLATEDB_PRODUCTION_STAGE];
    expect(local.map(cfSlatedbStateBackendForStage)).toEqual(local.map(() => "Local"));
    expect(live.map(cfSlatedbStateBackendForStage)).toEqual(live.map(() => "Cloudflare"));
  });
});
