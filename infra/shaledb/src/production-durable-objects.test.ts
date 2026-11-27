import {
  CompactionBudgetServer,
  CompactionWorkflow,
  CompactorServer,
  WriterServer,
} from "@shaledb/lsm-cloudflare";
import { ShardDirectoryServer } from "@shaledb/sharding";
import { describe, expect, it } from "vite-plus/test";

import { deploysTelemetryProbe } from "./telemetry-probe-worker.ts";

/**
 * The Durable Object classes and the Workflow a production Stack hosts (D104). Alchemy derives a
 * Worker's Durable Object migrations from the classes it hosts: a class that disappears from a
 * deploy is a `deleted_classes` migration, which destroys every instance's storage (a writer's
 * unflushed, acknowledged writes; a compactor's state; a shard directory). Renaming or removing one
 * of these is never a refactor; see docs/operations.md, "Durable Object classes".
 */
describe("production's Durable Object classes and Workflow (D104)", () => {
  it("keep their names, which are permanent once production is deployed", () => {
    expect(
      [
        WriterServer,
        CompactorServer,
        CompactionBudgetServer,
        ShardDirectoryServer,
        CompactionWorkflow,
      ].map((declared) => declared.name),
    ).toEqual([
      "WriterServer",
      "CompactorServer",
      "CompactionBudgetServer",
      "ShardDirectoryServer",
      "CompactionWorkflow",
    ]);
  });

  it("do not include the probe's classes, which only previews and local stages deploy", () => {
    expect(deploysTelemetryProbe("production")).toBe(false);
  });
});
