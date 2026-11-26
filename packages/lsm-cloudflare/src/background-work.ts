import * as Cloudflare from "@alchemy.run/cloudflare";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { Context, Effect, FiberSet, Layer } from "effect";

/**
 * Work that runs after the current request or event returns, without delaying it.
 *
 * **Details**
 *
 * Only for work whose outcome nobody waits for and whose loss is harmless, such as the
 * `manifest/LATEST` hint: the runtime may end before it settles (a Durable Object evicted, a
 * Worker torn down). Its failures and defects are logged as warnings, never raised.
 */
export interface IBackgroundWork {
  /**
   * Starts `work` and returns at once. `name` identifies it in logs and traces.
   *
   * @returns Nothing; it never fails.
   */
  readonly run: (
    name: string,
    work: Effect.Effect<void, never, RuntimeContext>,
  ) => Effect.Effect<void, never, RuntimeContext>;
}

/** The contextual background work runner of the current Worker, Durable Object, or Workflow. */
export class BackgroundWork extends Context.Service<BackgroundWork, IBackgroundWork>()(
  "@shaledb/BackgroundWork",
) {}

/** `work` under its span, with any failure or defect logged instead of raised. */
const loggedBackgroundWork = (name: string, work: Effect.Effect<void, never, RuntimeContext>) =>
  work.pipe(
    Effect.withSpan(`BackgroundWork.${name}`),
    Effect.catchCause((cause) =>
      Effect.logWarning(`Background work ${name} did not finish; nothing waits for it.`, cause),
    ),
  );

/**
 * Background work in a Worker's event (fetch, cron): registered with the event's `waitUntil`, so the
 * Worker stays alive until it settles.
 */
export const workerBackgroundWorkLayer = Layer.succeed(
  BackgroundWork,
  BackgroundWork.of({
    run: (name, work) => Cloudflare.waitUntil(loggedBackgroundWork(name, work)),
  }),
);

/**
 * Background work in a Durable Object: registered with the object's `waitUntil`, so the object stays
 * alive until it settles. Provide it where the object's state is in context (its init).
 */
export const durableObjectBackgroundWorkLayer = Layer.effect(
  BackgroundWork,
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    return BackgroundWork.of({
      run: (name, work) => state.waitUntil(loggedBackgroundWork(name, work)),
    });
  }),
);

/**
 * "Background" work that runs before `run` returns, for runtimes with nothing to keep alive past
 * the current call: a Workflow step has no `waitUntil`, and its result is durable only once the step
 * returns, so the work finishes first. Failures are still only logged.
 */
export const inlineBackgroundWorkLayer = Layer.succeed(
  BackgroundWork,
  BackgroundWork.of({ run: loggedBackgroundWork }),
);

/**
 * Background work for tests: each run is forked and returns at once, like the deployed runtimes, so
 * tests see work that lands later. Forked work is interrupted when the Layer's scope closes.
 */
export const backgroundWorkTestLayer = Layer.effect(
  BackgroundWork,
  Effect.gen(function* () {
    const running = yield* FiberSet.make<void, never>();
    return BackgroundWork.of({
      run: (name, work) =>
        FiberSet.run(running, loggedBackgroundWork(name, work)).pipe(Effect.asVoid),
    });
  }),
);
