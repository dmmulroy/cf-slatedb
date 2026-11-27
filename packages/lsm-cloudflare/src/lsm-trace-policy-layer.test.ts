import { RuntimeContext } from "alchemy/RuntimeContext";
import * as Telemetry from "alchemy/Telemetry";
import { ConfigProvider, Context, Effect, Layer, Option, Tracer } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { withLsmTracePolicy } from "./lsm-trace-policy.ts";

/**
 * `withLsmTracePolicy` registers the policy over a Worker's bound exporters when the Worker runs,
 * and only binds at plan time (D63). Without the registration every span would reach the exporter
 * unredacted, so the runtime half is pinned here (mutation sweep, D128).
 */
const recorded: Array<string> = [];
const boundExporter: Telemetry.TelemetryLayer = Layer.succeed(
  Tracer.Tracer,
  Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      span.attribute = (key) => {
        recorded.push(key);
      };
      return span;
    },
  }),
);

/** Builds the Layer in `phase` on a fresh host, returning the telemetry the host registered. */
const registered = (phase: "plan" | "runtime") =>
  Effect.gen(function* () {
    const host: RuntimeContext["Service"] = {
      Type: "Test",
      id: "shaledb-test",
      env: {},
      get: () => Effect.succeed(undefined),
      set: (id) => Effect.succeed(id),
    };
    yield* Layer.build(withLsmTracePolicy(Layer.empty)).pipe(
      Effect.provideService(RuntimeContext, host),
      Effect.provideService(
        ConfigProvider.ConfigProvider,
        ConfigProvider.fromUnknown({ ALCHEMY_PHASE: phase }),
      ),
    );
    return host.telemetry;
  }).pipe(Effect.scoped, Effect.runPromise);

describe("withLsmTracePolicy (D63)", () => {
  it("registers the policy's tracer over the bound exporters at runtime", async () => {
    const telemetry = await registered("runtime");
    expect(telemetry).toBeDefined();
    if (telemetry === undefined) return;
    recorded.length = 0;
    // As the runtime bridge builds an event's telemetry: the bound exporters, the registered Layer over them.
    await Effect.gen(function* () {
      const scope = yield* Effect.scope;
      const context = yield* Telemetry.buildEventTelemetry(
        Context.empty(),
        scope,
        telemetry,
        boundExporter,
      );
      const span = Context.get(context, Tracer.Tracer).span({
        name: "s",
        parent: Option.none(),
        annotations: Context.empty(),
        links: [],
        startTime: 0n,
        kind: "internal",
        root: true,
        sampled: true,
      });
      span.attribute("database.id", "orders");
      span.attribute("key", "MKSECRET");
    }).pipe(
      Effect.provideService(Telemetry.Telemetry, boundExporter),
      Effect.scoped,
      Effect.runPromise,
    );
    expect(recorded).toEqual(["database.id"]);
  });

  it("only binds at plan time", async () => {
    expect(await registered("plan")).toBeUndefined();
  });
});
