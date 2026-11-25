import { Stage } from "alchemy/Stage";
import { Effect, Layer } from "effect";
import { OtlpExporter } from "effect/observability";
import { describe, expect, it } from "vite-plus/test";

import {
  eventTelemetryFlushForStage,
  eventTelemetryFlushPolicyForStage,
  type EventTelemetryFlushPolicy,
  withEventTelemetryFlush,
} from "./lsm-telemetry.ts";

/**
 * Runs a handler under a policy with a `Flusher` that records each flush, and the order of the
 * handler's completion and the flush relative to the moment the wrapped Effect returns.
 */
const runHandler = (policy: EventTelemetryFlushPolicy) => {
  const events: Array<string> = [];
  const recordingFlusher = Layer.succeed(OtlpExporter.Flusher, {
    flush: Effect.sync(() => events.push("flush")),
    register: () => Effect.void,
  });
  const handler = Effect.sync(() => events.push("handler")).pipe(Effect.withSpan("Handler.fetch"));
  return withEventTelemetryFlush(policy)(handler)
    .pipe(
      Effect.tap(() => Effect.sync(() => events.push("returned"))),
      Effect.provide(recordingFlusher),
      Effect.runPromise,
    )
    .then(() => events);
};

describe("withEventTelemetryFlush", () => {
  it("BeforeResponse exports the handler's spans before its result is returned", async () => {
    expect(await runHandler("BeforeResponse")).toEqual(["handler", "flush", "returned"]);
  });

  it("AfterResponse adds no export to the handler, leaving it to the bridge's waitUntil flush", async () => {
    expect(await runHandler("AfterResponse")).toEqual(["handler", "returned"]);
  });

  it("BeforeResponse flushes even when the handler fails, and keeps the failure", async () => {
    const flushes: Array<string> = [];
    const exit = await withEventTelemetryFlush("BeforeResponse")(Effect.fail("boom")).pipe(
      Effect.provide(
        Layer.succeed(OtlpExporter.Flusher, {
          flush: Effect.sync(() => flushes.push("flush")),
          register: () => Effect.void,
        }),
      ),
      Effect.exit,
      Effect.runPromise,
    );
    expect(exit._tag).toBe("Failure");
    expect(flushes).toEqual(["flush"]);
  });
});

describe("eventTelemetryFlushPolicyForStage", () => {
  it("flushes before the response only on local stages", () => {
    const local = ["dev", "test", "local", "dev-dillon", "test-telemetry-smoke", "test-run-42"];
    const live = ["production", "preview", "preview-pr-12", "staging", "developer", "testing"];
    expect(local.map(eventTelemetryFlushPolicyForStage)).toEqual(local.map(() => "BeforeResponse"));
    expect(live.map(eventTelemetryFlushPolicyForStage)).toEqual(live.map(() => "AfterResponse"));
  });

  it("selects the wrapper from the Alchemy Stage service", async () => {
    const policyFor = (stage: string) =>
      eventTelemetryFlushForStage.pipe(
        Effect.flatMap((wrap) => {
          const flushed: Array<string> = [];
          return wrap(Effect.void).pipe(
            Effect.provide(
              Layer.succeed(OtlpExporter.Flusher, {
                flush: Effect.sync(() => flushed.push("flush")),
                register: () => Effect.void,
              }),
            ),
            Effect.map(() => (flushed.length > 0 ? "BeforeResponse" : "AfterResponse")),
          );
        }),
        Effect.provideService(Stage, stage),
        Effect.runPromise,
      );
    expect(await policyFor("dev")).toBe("BeforeResponse");
    expect(await policyFor("production")).toBe("AfterResponse");
  });
});
