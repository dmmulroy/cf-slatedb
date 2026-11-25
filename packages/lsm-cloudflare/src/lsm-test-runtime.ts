import { RuntimeContext } from "alchemy/RuntimeContext";
import { Effect, Layer } from "effect";

/**
 * The smallest host `RuntimeContext` the in-memory test Layers accept: they need no Worker
 * environment or bindings. For tests only; production code always runs inside an Alchemy host.
 */
export const lsmTestRuntimeContextLayer = Layer.succeed(
  RuntimeContext,
  RuntimeContext.of({
    Type: "Test",
    id: "cf-slatedb-test",
    env: {},
    get: () => Effect.succeed(undefined),
    set: (id) => Effect.succeed(id),
  }),
);
