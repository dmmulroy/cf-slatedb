import { utimesSync } from "node:fs";
import { join } from "node:path";

import { NodeServices } from "@effect/platform-node";
import { Effect, Option } from "effect";
import { describe, expect, it } from "vite-plus/test";

import type { CollectedSpan } from "./collected-span.ts";
import { TraceStore, traceStoreTestLayer } from "./trace-store.ts";

const span = (traceId: string, startMs: number): CollectedSpan => ({
  traceId,
  spanId: "1".repeat(16),
  parentSpanId: Option.none(),
  name: "Span",
  kind: 1,
  serviceName: "test",
  startTimeUnixNano: String(BigInt(startMs) * 1_000_000n),
  endTimeUnixNano: String(BigInt(startMs + 1) * 1_000_000n),
  durationMs: 1,
  attributes: {},
  status: { code: "Unset", message: Option.none() },
  events: [],
});

describe("TraceStore.listTraceIdsWrittenSince (D132)", () => {
  it("lists only the traces whose files were written since a time, reading no file", async () => {
    const now = Date.now();
    const listed = await Effect.gen(function* () {
      const store = yield* TraceStore;
      yield* store.append([span("a".repeat(32), now - 3_600_000), span("b".repeat(32), now)]);
      // Trace a's file was last written an hour ago.
      const hourAgo = new Date(now - 3_600_000);
      utimesSync(join(store.directory, "spans", `${"a".repeat(32)}.ndjson`), hourAgo, hourAgo);
      return {
        recent: yield* store.listTraceIdsWrittenSince(now - 60_000),
        all: yield* store.listTraceIds,
      };
    }).pipe(Effect.provide([traceStoreTestLayer, NodeServices.layer]), Effect.runPromise);
    expect(listed.recent).toEqual(["b".repeat(32)]);
    expect([...listed.all].sort()).toEqual(["a".repeat(32), "b".repeat(32)]);
  });
});
