import { ManifestId } from "@shaledb/lsm-core";
import { Cause, Option, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CompactionJobOutcome } from "./compaction-job.ts";
import { compactionJobProgressOf, isWorkflowInstanceNotFound } from "./compaction-workflow.ts";

const encode = Schema.encodeSync(CompactionJobOutcome);

describe("compactionJobProgressOf", () => {
  it("is running while the instance can still commit, and finished with its outcome otherwise", () => {
    for (const status of ["queued", "running", "paused", "waiting", "waitingForPause", "unknown"]) {
      expect(compactionJobProgressOf(status, undefined)).toEqual({ _tag: "Running" });
    }
    const committed = { _tag: "Committed" as const, manifestId: ManifestId.make(7) };
    expect(compactionJobProgressOf("complete", encode(committed))).toEqual({
      _tag: "Finished",
      outcome: Option.some(committed),
    });
    // A step that never finished: its retries ran out and the instance errored. The compactor
    // releases the lease and plans again; an errored or terminated job has no outcome.
    for (const status of ["errored", "terminated"]) {
      expect(compactionJobProgressOf(status, undefined)).toEqual({
        _tag: "Finished",
        outcome: Option.none(),
      });
    }
    // An output this version cannot read finishes the job without an outcome rather than hanging.
    expect(compactionJobProgressOf("complete", { _tag: "Later" })).toEqual({
      _tag: "Finished",
      outcome: Option.none(),
    });
  });
});

describe("isWorkflowInstanceNotFound", () => {
  it("recognizes Workflows' missing-instance error however Alchemy wraps it, and nothing else", () => {
    const notFound = new Error("instance.not_found");
    expect(isWorkflowInstanceNotFound(Cause.die(notFound))).toBe(true);
    expect(isWorkflowInstanceNotFound(Cause.die(new Cause.UnknownError(notFound)))).toBe(true);
    expect(isWorkflowInstanceNotFound(Cause.fail(new Cause.UnknownError(notFound)))).toBe(true);
    // A transient failure keeps the job running until the next poll.
    expect(isWorkflowInstanceNotFound(Cause.die(new Error("network connection lost")))).toBe(false);
    expect(isWorkflowInstanceNotFound(Cause.fail("instance.not_found"))).toBe(false);
  });

  it("does not count a status this code does not know as running", () => {
    expect(compactionJobProgressOf("someFutureStatus", undefined)).toEqual({
      _tag: "Finished",
      outcome: Option.none(),
    });
  });
});
