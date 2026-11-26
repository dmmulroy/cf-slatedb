import { describe, expect, it } from "vite-plus/test";

import { keepsInvocationRecord, tailSampleRateOf } from "./cpu-tail-worker.ts";

describe("the Tail Worker's sampling", () => {
  it("keeps every invocation that did not end ok, and the sampled fraction of the rest", () => {
    for (const outcome of ["exceededCpu", "exceededMemory", "exception", "canceled"]) {
      expect(keepsInvocationRecord(outcome, 0, 0.999)).toBe(true);
    }
    expect(keepsInvocationRecord("ok", 0.1, 0.09)).toBe(true);
    expect(keepsInvocationRecord("ok", 0.1, 0.1)).toBe(false);
    expect(keepsInvocationRecord("ok", 1, 0.999)).toBe(true);
    expect(keepsInvocationRecord("ok", 0, 0)).toBe(false);
    // About a tenth of a thousand ok invocations at 10%.
    const kept = Array.from({ length: 1000 }, (_, index) =>
      keepsInvocationRecord("ok", 0.1, index / 1000),
    ).filter(Boolean).length;
    expect(kept).toBe(100);
  });

  it("reads the bound rate, and keeps everything when it is unset or not a fraction", () => {
    expect(tailSampleRateOf("0.1")).toBe(0.1);
    expect(tailSampleRateOf("1")).toBe(1);
    expect(tailSampleRateOf("0")).toBe(0);
    expect(tailSampleRateOf(undefined)).toBe(1);
    expect(tailSampleRateOf("ten percent")).toBe(1);
    expect(tailSampleRateOf("7")).toBe(1);
    expect(tailSampleRateOf("-0.5")).toBe(1);
  });
});
