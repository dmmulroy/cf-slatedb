import { ManifestId } from "@shaledb/lsm-core";
import { Option, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CompactionJobOutcome } from "./compaction-job.ts";
import { compactionJobProgressOf } from "./compaction-workflow.ts";

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
